#!/usr/bin/env python3
"""Bounded write-admission diagnosis on a disposable reflink clone of a failed fixture.

Never writes to the original members or acknowledgement ledger. The exact
source population is cloned on each member's existing XFS filesystem. This is
not a million-object qualification or a physical-drive replacement test.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import traceback

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('recheck', Path(__file__).with_name('n1-hdd-recheck.py'))
recheck = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recheck)
scale = recheck.scale


def source_population(report, reader):
    """Keep reconciled commits distinct from client acknowledgements."""
    known, uncertain, uncertain_bytes = reader.execute('''SELECT COUNT(*),
        COALESCE(SUM(category='ambiguous_commit_not_acknowledged'),0),
        COALESCE(SUM(CASE WHEN category='ambiguous_commit_not_acknowledged'
                          THEN size ELSE 0 END),0) FROM acks''').fetchone()
    if 'expected_acknowledged_versions' in report:
        assert report['status'] == 'PASS' and report.get('original_reports_and_ledgers_unchanged') is True
        phases = {phase['name']: phase for phase in report['phases'] if phase['status'] == 'PASS'}
        assert set(phases) >= {'all-acknowledged-version-hashes-after-restart',
            'ambiguous-write-outcomes', 'complete-namespace-ledger-comparison'}
        expected = report['expected_acknowledged_versions']
        assert phases['all-acknowledged-version-hashes-after-restart'][
            'every_acknowledged_version_downloaded_and_hashed'] == expected
        assert phases['ambiguous-write-outcomes'].get('committed_and_hashed', 0) == uncertain
    else:
        expected = report['counts']['acknowledged_versions']
        assert uncertain == 0, 'Unverified ambiguous commits require a completed reconciliation'
    assert known - uncertain == expected, 'Source acknowledgement/ledger count mismatch'
    return dict(acknowledged_versions=expected, known_versions=known,
                unacknowledged_commits=uncertain, unacknowledged_bytes=uncertain_bytes)


class Admission(scale.Scale):
    def __init__(self, args):
        original = args.fixture.resolve(strict=True)
        source_config = recheck.configparser.ConfigParser(interpolation=None)
        source_config.read(original / 'n1_config.ini')
        assert [name for name in source_config if name.startswith('storage_pool.')] == ['storage_pool.A']
        source_roots = source_config['storage_pool.A']['roots'].split(',')
        assert len(source_roots) == len(args.member_parent) == 2
        sources = [recheck.fixture_member(Path(path), parent)
                   for path, parent in zip(source_roots, args.member_parent)]
        assert original.is_relative_to(args.member_parent[0].resolve(strict=True))
        control = Path(source_config['disk_config']['fs_storage_path']).resolve(strict=True)
        assert control == original / 'control', 'Unexpected source control root'
        source_report = json.loads((original / 'results.json').read_text())
        evidence = ['results.json', 'acknowledgements.sqlite']
        if 'expected_acknowledged_versions' in source_report:
            evidence.append('resolved-ambiguous.jsonl')
        else:
            evidence.append('acknowledgements.jsonl')
        self.original_evidence = {original / name: scale.base.digest((original / name).read_bytes())
                                  for name in evidence}
        reader = sqlite3.connect((original / 'acknowledgements.sqlite').as_uri() + '?mode=ro', uri=True)
        try:
            population = source_population(source_report, reader)
        finally:
            reader.close()
        super().__init__(args)
        for source, destination in zip(sources + [control], self.roots + [self.root / 'control']):
            assert source != destination and source.stat().st_dev == destination.stat().st_dev
            subprocess.run(['cp', '-a', '--reflink=always', str(source) + '/.', str(destination)], check=True)
        self.fragment = recheck.fixture_fragment(self.roots)
        # Keep source tuning, durability and maintenance settings unchanged.
        # Only the disposable root/control paths and listening ports differ.
        for section in ('upload_config', 'auto_healing', 'io_control', 'garbage_collector'):
            self.config[section].update(source_config[section])
        self.write_config()
        reader = sqlite3.connect((original / 'acknowledgements.sqlite').as_uri() + '?mode=ro', uri=True)
        try:
            cursor = reader.execute('SELECT object_key,version_id,sha256,size,category FROM acks')
            while rows := cursor.fetchmany(128):
                self.record_acks(rows)
        finally:
            reader.close()
        assert self.counts['acknowledged_versions'] == population['known_versions']
        self.counts['acknowledged_versions'] -= population['unacknowledged_commits']
        self.counts['acknowledged_bytes'] -= population['unacknowledged_bytes']
        self.counts['reconciled_unacknowledged_commits'] = population['unacknowledged_commits']
        self.report.update(scope='isolated same-filesystem reflink clone; original evidence unchanged; admission diagnosis only',
                           source_engine_sha256=source_report['engine_sha256'],
                           initial_acknowledged_versions=self.counts['acknowledged_versions'],
                           initial_population=population,
                           diagnostic_objects=args.objects, target_objects=None, target_churn_mutations=None)

    def batch_object_key(self, index):
        return 'admission-diagnostic/' + scale.source_key(index)

    def authenticate(self):
        if not self.last_auth:
            reply = self.api('POST', '/auth/fragment', dict(fragment=self.fragment,
                secret=self.master, read=True, write=True, modify=True, delete=True,
                expires_in=21600), token='')
            self.token = reply['access_token']
            self.last_auth = scale.time.monotonic()

    def verify_all(self):
        verified = sum(1 for _ in scale.bounded_map(self.verify_row, self.ack_rows(), 32))
        return dict(every_known_version_downloaded_and_hashed=verified,
                    acknowledged_versions=self.counts['acknowledged_versions'],
                    reconciled_unacknowledged_commits=self.counts['reconciled_unacknowledged_commits'])

    def ingest_sampled(self):
        """Observe the unchanged scheduler; never set targets or stimulate pressure."""
        done = scale.threading.Event()
        errors, targets = [], scale.Counter()
        def observe():
            try:
                with (self.root / 'scheduler-samples.jsonl').open('x') as stream:
                    while not done.is_set():
                        health = self.api('GET', '/maintenance/health')
                        scheduler = health['io_scheduler']
                        targets[scheduler['foreground_target']] += 1
                        stream.write(json.dumps(dict(elapsed_seconds=scale.time.monotonic() - self.started,
                            system=health['system'], io_scheduler=scheduler,
                            process=health['io_control']['process'])) + '\n')
                        stream.flush()
                        if done.wait(10):
                            break
            except BaseException as error:
                errors.append(error)
        observer = scale.threading.Thread(target=observe)
        observer.start()
        before = self.counts['source_objects']
        try:
            intervals = ((index, min(index + self.args.batch_size, self.args.objects))
                         for index in range(0, self.args.objects, self.args.batch_size))
            for _ in scale.bounded_map(self.upload_batch, intervals, self.args.batch_clients):
                pass
        finally:
            done.set()
            observer.join(self.args.request_timeout_seconds + 5)
        assert not observer.is_alive(), 'Scheduler observer failed to terminate'
        assert not errors, f'Scheduler diagnostics failed: {errors}'
        assert targets, 'No scheduler observations'
        assert self.counts['source_objects'] - before == self.args.objects
        return dict(new_acknowledged_objects=self.args.objects,
                    observed_foreground_targets=dict(sorted(targets.items())))

    def run(self):
        self.start('admission-diagnostic')
        self.authenticate()
        self.phase_run('cloned-namespace-matches-original-ledger', self.snapshot_verify)
        ingestion_error = None
        try:
            self.phase_run('strict-fixed-load-at-existing-population', self.ingest_sampled)
        except Exception as error:
            ingestion_error = error
            self.report['ingestion_error'] = f'{type(error).__name__}: {error}'
        # Capture existing in-memory latency classes after work drains. The
        # engine logs its active permit owners at each admission timeout.
        self.phase = 'final-health-diagnostics'
        (self.root / 'health.json').write_text(json.dumps(
            self.api('GET', '/maintenance/health?refresh=true'), indent=2))
        self.phase_run('every-known-acknowledged-version-hashed', self.verify_all)
        self.phase_run('complete-quiescent-namespace-ledger', self.snapshot_verify)
        assert not self.fault, self.fault
        if ingestion_error:
            raise ingestion_error
        self.report['status'] = 'PASS'

    def finish(self):
        self.stop()
        unchanged = all(scale.base.digest(path.read_bytes()) == digest
                        for path, digest in self.original_evidence.items())
        self.report['original_reports_and_ledgers_unchanged'] = unchanged
        if not unchanged:
            self.report.update(status='FAIL', error='Original fixture evidence changed during diagnosis')
        super().finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--objects', type=int, default=2048)
    parser.add_argument('--batch-size', type=int, default=64)
    parser.add_argument('--batch-clients', type=int, default=8)
    parser.add_argument('--batch-concurrency', type=int, default=16)
    parser.add_argument('--port', type=int, default=57401)
    parser.add_argument('--deadline-seconds', type=int, default=5400)
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--free-floor-gib', type=int, default=128)
    parser.add_argument('--wait-for', type=Path, help='Prior completed diagnostic smoke gate must say PASS')
    args = parser.parse_args()
    assert 1 <= args.objects <= 100000 and 1 <= args.batch_size <= 256
    assert 1 <= args.batch_clients <= 32 and 1 <= args.batch_concurrency <= 128
    args.data_shards, args.parity_shards, args.probes, args.churn_mutations = 1, 1, 1, 0
    args.binary = args.binary.resolve(strict=True)
    if args.wait_for:
        previous = scale.wait_for_result(args.wait_for)
        assert previous.get('status') == 'PASS', 'Prior gate failed; no diagnostic writes started'
    os.umask(0o077)
    test = Admission(args)
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Operator signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
