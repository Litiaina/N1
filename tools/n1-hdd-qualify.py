#!/usr/bin/env python3
"""Sequential HDD qualification gates. No deployment, formatting, or automatic retries.

Build outputs, logs and generated fixtures stay on the selected HDDs. A passed
smaller object gate is not a million-object qualification. Failed gates stop
the pipeline; all evidence and partial acknowledgement ledgers are retained.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback

if not __debug__:
    raise RuntimeError('Run without -O: fixture safety assertions must remain enabled')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--fixture', required=True, type=Path)
    parser.add_argument('--baseline', required=True, type=Path)
    parser.add_argument('--cargo-home', required=True, type=Path)
    parser.add_argument('--member-parent', action='append', required=True, type=Path)
    parser.add_argument('--objects', type=int, default=1000000)
    parser.add_argument('--churn-mutations', type=int, default=1000000)
    parser.add_argument('--probes', type=int, default=1024)
    parser.add_argument('--deadline-seconds', type=int, default=604800)
    parser.add_argument('--unit-log', type=Path, help='Completed full engine suite on an up-to-date test binary')
    parser.add_argument('--forensic-result', type=Path, help='Previous failed-write reconciliation must pass first')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    parents = [path.resolve(strict=True) for path in args.member_parent]
    root = args.root.resolve()
    assert len(parents) == 2 and all(os.path.ismount(path) for path in parents)
    assert len({path.stat().st_dev for path in parents}) == 2
    assert repo.is_relative_to(parents[0]) and root.is_relative_to(parents[0])
    assert not root.exists(), 'Never overwrite a previous qualification'
    assert args.objects >= 1000000 and args.churn_mutations >= 1000000, 'This qualification must reach the million-object/mutation target'
    assert args.probes >= 256, 'A 256-client churn phase needs at least 256 actors'
    if args.forensic_result:
        forensic = json.loads(args.forensic_result.read_text())
        assert forensic['status'] == 'PASS' and forensic['original_reports_and_ledgers_unchanged']
    baseline = json.loads((args.baseline / 'results.json').read_text())
    assert baseline['status'] == 'PASS' and baseline['original_reports_and_ledgers_unchanged']
    os.umask(0o077)
    root.mkdir(mode=0o700)
    (root / 'tmp').mkdir()
    env = dict(os.environ, CARGO_HOME=str(args.cargo_home.resolve(strict=True)),
               CARGO_TARGET_DIR=str(repo / 'target'), TMPDIR=str(root / 'tmp'),
               N1_TEST_SECOND_FILESYSTEM=str(parents[1]), RUSTFLAGS='-Dwarnings')
    report = dict(status='RUNNING', phase='unit-regressions', completed=[],
                  qualification_population=args.objects, target_churn_mutations=args.churn_mutations,
                  million_object_qualification=False)
    source_paths = (list((repo / 'engine/src').rglob('*.rs'))
                    + list((repo / 'tools/n1-release-proof/src').rglob('*.rs'))
                    + list((repo / 'tools').glob('n1*hdd*.py'))
                    + list((repo / 'tools').glob('n1-rc*.py'))
                    + list((repo / 'tools').glob('test_n1*.py'))
                    + [repo / name for name in ('Cargo.toml', 'Cargo.lock', 'engine/Cargo.toml',
                                               'tools/n1-release-proof/Cargo.toml', 'shell/web/n1_config.default.ini')])
    def source_manifest():
        return {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(set(source_paths))}
    initial_sources = source_manifest()
    (root / 'source-manifest.json').write_text(json.dumps(initial_sources, indent=2))
    report['source_manifest_sha256'] = hashlib.sha256((root / 'source-manifest.json').read_bytes()).hexdigest()

    def save():
        temporary = root / 'qualification.json.tmp'
        temporary.write_text(json.dumps(report, indent=2))
        temporary.replace(root / 'qualification.json')

    def command(label, argv, timeout=7200):
        assert source_manifest() == initial_sources, 'Candidate or harness source changed during qualification; stop instead of mixing builds'
        report['phase'] = label
        save()
        started = time.monotonic()
        with (root / (label + '.log')).open('wb') as log:
            result = subprocess.run(argv, cwd=repo, env=env, stdout=log, stderr=log, timeout=timeout)
        assert result.returncode == 0, f'{label}: exit {result.returncode}; inspect its retained log'
        assert source_manifest() == initial_sources, 'Candidate or harness changed while gate was running'
        return round(time.monotonic() - started, 3)

    members = [value for parent in parents for value in ('--member-parent', str(parent))]
    python = [sys.executable, '-B']

    def gate(label, tool, extra, timeout=7200):
        elapsed = command(label, python + [str(repo / 'tools' / tool), '--binary', str(binary),
                          '--root', str(root / label)] + members + extra, timeout)
        result = json.loads((root / label / 'results.json').read_text())
        assert result['status'] == 'PASS', f'{label}: {result.get("error", result["status"])}'
        report['completed'].append(dict(phase=label, status='PASS', elapsed_seconds=elapsed))
        save()
        return result

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Operator/systemd signal {signum}')

    signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.unit_log:
            unit_text = args.unit_log.read_text()
            executable = re.search(r'Running unittests src/lib.rs \(([^)]+)\)', unit_text)
            assert executable, 'Completed engine-unit executable must be identified'
            unit_binary = (repo / executable[1]).resolve(strict=True)
            sources = list((repo / 'engine/src').rglob('*.rs')) + [repo / 'Cargo.lock', repo / 'engine/Cargo.toml', repo / 'Cargo.toml']
            assert all(path.stat().st_mtime_ns <= unit_binary.stat().st_mtime_ns for path in sources), 'Engine changed after the tested binary was built'
            shutil.copy2(args.unit_log, root / 'unit-regressions.log')
            elapsed = None
        else:
            elapsed = command('unit-regressions', ['cargo', 'test', '--locked', '--offline', '-p', 'n1',
                              '--lib', '--', '--test-threads=1'])
        text = (root / 'unit-regressions.log').read_text()
        match = re.search(r'test result: ok\. (\d+) passed; 0 failed; 0 ignored; 0 measured; 0 filtered out', text)
        assert match and int(match[1]) >= 344, 'The complete current engine library suite must pass, including read-bitrot and TTL regressions'
        for regression in ('streaming_detects_same_length_bitrot_and_refuses_invalid_survivors',
                           'read_integrity_rejects_missing_and_malformed_checksums',
                           'repair_rejects_reconstruction_that_disagrees_with_committed_checksum',
                           'one_read_repairs_both_corrupt_data_targets_after_rejecting_a_peer',
                           'read_repair_respects_whole_fragment_deletion_admission',
                           'unexpired_trash_does_not_read_version_metadata'):
            assert re.search(re.escape(regression) + r' \.\.\. ok', text), f'Required regression not passed: {regression}'
        report['completed'].append(dict(phase='unit-regressions', status='PASS',
                                       tests=int(match[1]), elapsed_seconds=elapsed))
        command('harness-regressions', python + ['-m', 'unittest', 'discover', '-s', 'tools', '-p', 'test_n1*hdd*.py'])
        command('rc-harness-regressions', python + ['-m', 'unittest', 'discover', '-s', 'tools', '-p', 'test_n1_rc_stress.py'])
        command('proof-regressions', ['cargo', 'test', '--locked', '--offline', '-p', 'n1-release-proof'])
        command('release-build', ['cargo', 'build', '--release', '--locked', '--offline', '-p', 'n1', '-p', 'n1-release-proof'])
        binary = root / 'n1-candidate'
        shutil.copy2(repo / 'target/release/litiaina-n1', binary)
        report['engine_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        proof_binary = root / 'n1-release-proof'
        shutil.copy2(repo / 'target/release/n1-release-proof', proof_binary)
        report['proof_sha256'] = hashlib.sha256(proof_binary.read_bytes()).hexdigest()
        current = gate('matched-admission-clone', 'n1-hdd-admission.py',
                       ['--fixture', str(args.fixture), '--objects', '2048', '--port', '57501'])
        old_phase = next(phase for phase in baseline['phases'] if phase['name'] == 'strict-fixed-load-at-existing-population')
        new_phase = next(phase for phase in current['phases'] if phase['name'] == old_phase['name'])
        ratio = new_phase['elapsed_seconds'] / old_phase['elapsed_seconds']
        report['matched_ingest_elapsed_ratio'] = round(ratio, 4)
        health_old = json.loads((args.baseline / 'health.json').read_text())
        health_new = json.loads((root / 'matched-admission-clone/health.json').read_text())
        def queue_p99(health):
            return next(item['p99_us'] for item in health['io_control']['latency']['classes']
                        if item['label'] == 'disk.queue.foreground_write')
        report['matched_queue_p99_ms'] = dict(before=queue_p99(health_old)/1000, after=queue_p99(health_new)/1000)
        save()
        assert ratio <= 1.10, 'Strict ingest slowed by more than 10%; investigate before scaling'
        assert queue_p99(health_new) < queue_p99(health_old), 'Admission tail did not improve; investigate before scaling'
        gate('overload-and-crash', 'n1-rc-stress.py', ['--port', '57511'])
        gate('recovery', 'n1-hdd-recovery.py', ['--proof-binary', str(proof_binary),
             '--port', '57521', '--gc-workload-timeout-seconds', '7200',
             '--wait-for', str(root / 'overload-and-crash/results.json')], timeout=14400)
        gate('extreme-overload', 'n1-rc-overload.py', ['--port', '57525'])
        gate('population-million', 'n1-hdd-scale.py', ['--objects', str(args.objects), '--probes', str(args.probes),
             '--churn-mutations', str(args.churn_mutations), '--port', '57531', '--deadline-seconds', str(args.deadline_seconds),
             '--request-timeout-seconds', '300', '--wait-for', str(root / 'recovery/results.json')], timeout=args.deadline_seconds+600)
        report.update(status='PASS', phase='million-object-and-churn-gates-complete', million_object_qualification=True)
    except KeyboardInterrupt as error:
        report.update(status='CANCELLED', error=str(error))
        raise
    except BaseException as error:
        report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
