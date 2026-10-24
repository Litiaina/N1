#!/usr/bin/env python3
"""Run existing Rust recovery/batch proofs on independent member filesystems.

Defaults to EC 1+1; use --data-shards 3 --parity-shards 1 and four member
parents for EC 3+1. Never contacts or stops an existing engine.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback


def require_data_volume(path):
    """Allow an owned child of a data mount, never an OS-filesystem fallback."""
    path = path.resolve(strict=True)
    mount = next((candidate for candidate in (path, *path.parents) if os.path.ismount(candidate)), None)
    assert mount is not None and mount != Path('/'), 'A mounted data filesystem, not the OS filesystem, is required'
    assert path.stat().st_dev == mount.stat().st_dev, 'Unexpected nested member device'
    return path


def backing_device(path):
    device = path.stat().st_dev
    link = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}'
    assert link.exists(), 'This proof needs an identifiable real block filesystem'
    block = link.resolve(strict=True)
    return block.parent if (block / 'partition').exists() else block


def validate_topology(data_shards, parity_shards, member_count):
    assert 1 <= data_shards <= 32 and 1 <= parity_shards <= 32, 'Recovery proofs need positive data and parity shard counts'
    assert member_count == data_shards + parity_shards, 'Need exactly K+P independent member filesystems'


def proof_command(args, source, roots, out, mode):
    """Keep the requested EC geometry and established workload dimensions."""
    command = [str(args.proof_binary), '--binary', str(args.binary), '--source-file', str(source),
               '--disk-roots', ','.join(map(str, roots)), '--out-dir', str(out),
               '--data-shards', str(args.data_shards), '--parity-shards', str(args.parity_shards),
               '--port', str(args.port), '--native-port', str(args.port+1),
               '--mixed-users', '4', '--'+mode+'-proof']
    if mode == 'concurrent-upload':
        # This is a focused integrity gate, not a million-object qualification.
        command += ['--mixed-files-per-user', '64', '--mixed-background-uploads', '128']
    if mode == 'gc-safety':
        command += ['--gc-workload-timeout-seconds', str(args.gc_workload_timeout_seconds)]
    return command


def finalize_owned_process(process, result, save):
    """Reap only our proof group without losing an earlier failure witness."""
    original_error = sys.exc_info()[1]
    cleanup_error = None

    def failed(stage, error, exit_not_confirmed=False):
        nonlocal cleanup_error
        cleanup_error = cleanup_error or error
        result['status'] = 'FAIL'
        result.setdefault('error', f'{type(error).__name__}: {error}')
        result.setdefault('cleanup_errors', []).append(dict(
            stage=stage, error=f'{type(error).__name__}: {error}',
            owned_proof_pid=getattr(process, 'pid', None),
            wait_timeout_seconds=getattr(error, 'timeout', None),
            process_exit_not_confirmed=exit_not_confirmed))

    try:
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except Exception as error:
                failed('terminate-owned-proof-group', error, True)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except Exception as error:
                    failed('kill-owned-proof-group', error, True)
                try:
                    process.wait(timeout=15)
                except Exception as error:
                    failed('reap-owned-proof-group', error, True)
            except Exception as error:
                failed('reap-owned-proof-group', error, True)
    finally:
        try:
            save()
        except Exception as error:
            failed('write-results', error)
            print(f'Recovery proof evidence finalization failed: {error}', file=sys.stderr, flush=True)
    if cleanup_error is not None and original_error is None:
        raise cleanup_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--proof-binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--data-shards', type=int, default=1)
    parser.add_argument('--parity-shards', type=int, default=1)
    parser.add_argument('--wait-for', type=Path, required=True)
    parser.add_argument('--port', type=int, default=56861)
    parser.add_argument('--gc-workload-timeout-seconds', type=int, default=300,
                        help='Aggregate GC mixed-workload budget; does not change request deadlines or workload size')
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('Do not disable fixture safety assertions with -O')
    validate_topology(args.data_shards, args.parity_shards, len(args.member_parent))
    os.umask(0o077)
    deadline = time.monotonic() + 15000
    while not args.wait_for.exists():
        assert time.monotonic() < deadline, 'Previous gate deadline exceeded'
        time.sleep(5)
    previous = json.loads(args.wait_for.read_text())
    assert not args.root.exists(), 'Never overwrite an existing fixture'
    parents = [require_data_volume(p) for p in args.member_parent]
    require_data_volume(args.root.parent)
    assert len({p.stat().st_dev for p in parents}) == len(parents), 'Need distinct member filesystems'
    physical_devices = [backing_device(parent) for parent in parents]
    assert len(set(physical_devices)) == len(parents), 'Separate partitions of one drive are not independent physical members'
    assert all(shutil.disk_usage(p).free > 128 * 1024**3 for p in parents)
    for parent in parents + [args.root.parent]:
        assert not any((p / '.n1-pool.json').exists() for p in [parent] + list(parent.parents))
    for port in (args.port, args.port+1):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', port))
    args.root.mkdir(mode=0o700)
    result = {'status': 'RUNNING', 'phases': [],
              'physical_topology': f'{args.data_shards}+{args.parity_shards}, {len(parents)} mounted filesystems',
              'topology': {'data_shards': args.data_shards, 'parity_shards': args.parity_shards,
                           'member_count': len(parents)},
              'physical_block_devices': list(map(str, physical_devices)),
              'engine_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'proof_tool_sha256': hashlib.sha256(args.proof_binary.read_bytes()).hexdigest(),
              'rust_proof_payload_default': 'relaxed; NCX publication is strict',
              'gc_mixed_workload_budget_seconds': args.gc_workload_timeout_seconds,
              'scope': 'isolated loopback HTTPS/native proof fixtures, not physical power loss'}
    def save():
        encoded = json.dumps(result, indent=2)
        temporary = args.root / 'progress.json.tmp'
        temporary.write_text(encoded)
        temporary.replace(args.root / 'progress.json')
        if result['status'] != 'RUNNING':
            temporary = args.root / 'results.json.tmp'
            temporary.write_text(encoded)
            temporary.replace(args.root / 'results.json')
    if previous.get('status') != 'PASS':
        result.update(status='BLOCKED', error='Prior overload gate failed')
        save()
        raise SystemExit('Prior gate failed; no recovery engine started')
    devices = {p: p.stat().st_dev for p in parents + [args.root]}
    nonce = secrets.token_hex(6)
    members = [p / ('n1-hdd-recovery-' + nonce) for p in parents]
    for member in members:
        member.mkdir(mode=0o700)
    result['member_directories'] = list(map(str, members))
    source = args.root / 'generated-source.bin'
    source.write_bytes(hashlib.sha256(nonce.encode()).digest() * 2048)
    process = None
    started = time.monotonic()
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Operator/systemd signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        for mode in ('auto-healing', 'trash-maintenance', 'gc-safety', 'maintenance-coverage', 'concurrent-upload'):
            out = args.root / mode
            out.mkdir(mode=0o700)
            roots = [p / mode for p in members]
            for root in roots:
                root.mkdir()
            command = proof_command(args, source, roots, out, mode)
            result['current_phase'] = mode
            save()
            print('START', mode, flush=True)
            began = time.monotonic()
            environment = {key: value for key, value in os.environ.items() if not key.startswith('N1_')}
            environment.update(TMPDIR=str(args.root.parent/'tmp'), TMP=str(args.root.parent/'tmp'),
                               TEMP=str(args.root.parent/'tmp'), RUST_BACKTRACE='1')
            with (out / 'runner.log').open('wb') as log:
                process = subprocess.Popen(command, cwd=out, env=environment, stdout=log, stderr=log, start_new_session=True)
                while process.poll() is None:
                    for path, device in devices.items():
                        assert path.stat().st_dev == device, f'Fixture filesystem changed: {path}'
                    assert all(shutil.disk_usage(p).free > 128*1024**3 for p in parents), 'Free-space floor crossed'
                    mode_budget = max(7200, args.gc_workload_timeout_seconds + 600) if mode == 'gc-safety' else 7200
                    assert time.monotonic()-began < mode_budget, f'{mode} exceeded its {mode_budget}-second deadline'
                    time.sleep(.5)
            summary_path = out / 'summary.json'
            summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
            phase = {'name': mode, 'exit_code': process.returncode, 'summary': summary,
                     'elapsed_seconds': round(time.monotonic()-began, 3)}
            result['phases'].append(phase)
            save()
            assert process.returncode == 0 and summary.get('status') == 'PASS', phase
            print('DONE', mode, 'PASS', flush=True)
        result.update(status='PASS', elapsed_seconds=round(time.monotonic()-started, 3))
    except BaseException as error:
        result.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        finalize_owned_process(process, result, save)


if __name__ == '__main__':
    main()
