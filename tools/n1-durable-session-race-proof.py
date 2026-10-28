#!/usr/bin/env python3
"""Isolated durable-upload init/read versus finalize/cancel race regression.

Uses NEW child roots on real mounted data drives and the supplied engine only.
Never contacts production, clones an existing store, retries failed requests,
changes committed files, or formats a device. Repeated successful init/part
requests are explicit idempotency checks, not error recovery. Preserves failures.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time
import traceback
import urllib.parse

sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location('durable_race_base', Path(__file__).with_name('n1-rc-stress.py'))
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)

ROUTE = '/upload/multipart/durable/'
LOAD_PROFILE = dict(enable_rate_limiting='true', view_limit_per_second='1000',
                    view_limit_burst_size='3000', global_limit_per_second='2000',
                    global_limit_burst_size='5000')


def apply_load_profile(config):
    # Keep admission enabled. These are the incident's explicit disposable
    # view/global rates; unlike that run, retain the normal 30-second I/O wait.
    # Any admission refusal still fails this regression; no request is retried.
    config['rate_limiter_config'].update(LOAD_PROFILE)
    return {key: config['rate_limiter_config'][key] for key in LOAD_PROFILE}


def data_device(path):
    path = path.resolve(strict=True)
    mount = next((parent for parent in (path, *path.parents) if os.path.ismount(parent)), None)
    assert mount is not None and mount != Path('/'), 'Tests require mounted data drives, not the OS filesystem'
    device = path.stat().st_dev
    link = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}'
    assert link.exists(), 'A real block filesystem is required'
    block = link.resolve(strict=True)
    return block.parent if (block / 'partition').exists() else block


def validate_workload(args):
    assert 1 <= args.concurrency <= 256, 'Concurrency must be between 1 and 256'
    assert args.concurrency <= args.objects <= 10000, 'Object count must cover every actor and remain bounded (10000)'
    assert 4096 <= args.part_size <= 1024 * 1024, 'Part size must be between 4 KiB and 1 MiB'
    assert 1 <= args.deadline_seconds <= 6 * 3600, 'Deadline must be bounded to six hours'
    assert 1 <= args.request_timeout_seconds <= args.deadline_seconds


class DurableRaceProof(base.Stress):
    def __init__(self, args):
        validate_workload(args)
        data_device(args.root.parent)
        devices = [data_device(path) for path in args.member_parent]
        assert len(set(devices)) == len(devices), 'Each member must use an independent physical drive'
        super().__init__(args)
        load_profile = apply_load_profile(self.config)
        self.write_config()
        self.counters = Counter()
        self.health_observations = []
        self.report.update(
            proof='HTTP durable default-reuse initialization overlapping unrelated finalize/cancel',
            concurrency=args.concurrency, publish_objects=args.objects,
            cancelled_objects=args.objects, part_size_bytes=args.part_size,
            harness_sha256=base.digest(Path(__file__).read_bytes()),
            fixture_helper_sha256=base.digest(Path(__file__).with_name('n1-rc-stress.py').read_bytes()),
            physical_block_devices=list(map(str, devices)),
            rate_limiter_profile=load_profile,
            foreground_wait_timeout_ms=int(self.config['io_control']['foreground_wait_timeout_ms']),
            full_object_bodies_retained=False,
            bounded_workers=args.concurrency,
            scope='Isolated authenticated loopback HTTPS; real member filesystems; no native-protocol or physical power-loss claim')

    def params(self):
        return dict(content_type='application/octet-stream', total_size_bytes=2 * self.args.part_size,
                    expected_parts=2, part_size_bytes=self.args.part_size)

    def init(self, key):
        # Deliberately omit fresh=true: this is the path used by the incident.
        result = self.api('POST', ROUTE + 'init/' + urllib.parse.quote(key, safe='/'), self.params())
        assert result.get('version_id') and result.get('session_id'), result
        with self.lock:
            self.counters['successful_init_calls'] += 1
        return result

    def part(self, key, session, index):
        body = base.payload(f'{key}:part:{index}', self.args.part_size)
        status, response = self.call('PUT', ROUTE + session['version_id'] + f'/{index}?durability=strict', body)
        assert status in (200, 201), f'Part upload failed: HTTP {status}: {response[:350]!r}'
        with self.lock:
            self.counters['successful_part_calls'] += 1

    def status(self, key, session):
        result = self.api('GET', ROUTE + session['version_id'])
        assert result.get('session_id') == session['session_id'], result
        assert result.get('version_id') == session['version_id'], result
        assert result.get('object_key') == key, result
        return result

    def resume(self, key, session):
        repeated = self.init(key)
        assert repeated == session, f'Init did not reuse the existing identity for {key}: {session} -> {repeated}'
        with self.lock:
            self.counters['identical_init_resumptions'] += 1

    def finalize(self, key, session):
        result = self.api('POST', ROUTE + session['version_id'] + '/' + session['session_id'] +
                          '/finalize?' + urllib.parse.urlencode({'object_key': key}), self.params())
        assert result['version_id'] == session['version_id'] and result['object_key'] == key, result
        body = b''.join(base.payload(f'{key}:part:{index}', self.args.part_size) for index in range(2))
        ack = (self.token, key, session['version_id'], base.digest(body), len(body))
        with self.lock:
            self.acknowledged.append(ack)
            self.counters['successful_finalizations'] += 1
            with (self.root / 'acknowledgements.jsonl').open('a') as stream:
                stream.write(json.dumps(dict(object_key=key, version_id=ack[2], sha256=ack[3], size_bytes=ack[4])) + '\n')
        self.read_ack(ack)
        return ack

    def cancel(self, key, session):
        route = ROUTE + session['version_id'] + '?' + urllib.parse.urlencode({'object_key': key})
        # A second successful DELETE explicitly verifies idempotent cleanup.
        for _ in range(2):
            status, response = self.call('DELETE', route)
            assert status in (200, 204), f'Cancel failed: HTTP {status}: {response[:350]!r}'
        state = self.api('GET', ROUTE + session['version_id'])
        assert not state.get('session_id') and not state.get('uploaded_parts'), state
        status, response = self.call('GET', '/objects/download?' + urllib.parse.urlencode({'object_key': key}))
        assert status == 404, f'Cancelled upload became visible: HTTP {status}: {response[:350]!r}'
        with self.lock:
            self.counters['cancelled_sessions_verified_invisible'] += 1

    def health(self):
        health = self.api('GET', '/maintenance/health?' + urllib.parse.urlencode({'refresh': 'true', 'fragment': self.fragment}))
        base.verify_io_backend(health, self.args)
        assert health['io_control']['process']['pid'] == self.process.pid, 'Telemetry is not from the owned engine'
        self.health_observations.append(health)
        return health

    def overlap(self):
        barrier = threading.Barrier(self.args.concurrency)

        def actor(actor_id):
            try:
                count = 0
                for index in range(actor_id, self.args.objects, self.args.concurrency):
                    key = f'publish/object-{index:06}.bin'
                    cancelled_key = f'cancel/object-{index:06}.bin'
                    cancelled = self.init(cancelled_key)
                    self.part(cancelled_key, cancelled, 0)
                    session = self.init(key)
                    if count == 0:
                        # All actors have real live pending sessions before any
                        # actor may finalize or cancel. No unbounded task queue.
                        barrier.wait(timeout=self.args.request_timeout_seconds)
                    self.resume(key, session)
                    if index % 3 == 0:
                        self.cancel(cancelled_key, cancelled)
                    self.part(key, session, 0)
                    self.part(key, session, 0)
                    state = self.status(key, session)
                    assert state['uploaded_parts'] == [0] and state['committed_bytes'] == self.args.part_size, state
                    self.resume(key, session)
                    if index % 3 == 1:
                        self.cancel(cancelled_key, cancelled)
                    self.part(key, session, 1)
                    self.finalize(key, session)
                    if index % 3 == 2:
                        self.cancel(cancelled_key, cancelled)
                    count += 1
                return count
            except BaseException:
                barrier.abort()
                raise

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.args.concurrency) as workers:
            counts = list(workers.map(actor, range(self.args.concurrency)))
        assert sum(counts) == self.args.objects
        return dict(actors=self.args.concurrency, finalized_objects=sum(counts),
                    objects_per_second=round(sum(counts) / (time.monotonic() - started), 3))

    def verify(self):
        with self.lock:
            acknowledgements = list(self.acknowledged)
        self.parallel(self.read_ack, acknowledgements, min(self.args.concurrency, 32))
        expected = {ack[1] for ack in acknowledgements}
        actual = set(self.list_all('publish', limit=37))
        assert actual == expected, f'Published listing differs: missing={len(expected - actual)}, extra={len(actual - expected)}'
        assert not self.list_all('cancel', limit=37), 'Cancelled uploads leaked into the namespace'
        return dict(acknowledged_historical_versions_hashed=len(acknowledgements), complete_namespace_verified=True)

    def run(self):
        self.start('initial')
        self.fragment, self.token = self.create('durable-race-' + secrets.token_hex(5))
        self.health()
        self.phase_run('concurrent_default_reuse_init_finalize_cancel', self.overlap)
        assert len(self.acknowledged) == self.args.objects
        self.phase_run('all_acknowledged_hashes_before_restart', self.verify)

        self.phase = 'prepare_partial_durable_upload'
        key = 'publish/resume-after-sigkill.bin'
        partial = self.init(key)
        self.part(key, partial, 0)
        self.resume(key, partial)
        self.health()
        self.stop()  # SIGKILL only the owned disposable engine.
        self.start('after-sigkill')

        def finish_partial():
            state = self.status(key, partial)
            assert state['uploaded_parts'] == [0] and state['committed_bytes'] == self.args.part_size, state
            self.resume(key, partial)
            self.part(key, partial, 0)
            self.part(key, partial, 1)
            self.finalize(key, partial)
            return dict(persisted_identity_and_receipt_preserved=True)

        self.phase_run('partial_durable_resume_after_sigkill', finish_partial)
        self.phase_run('all_acknowledged_hashes_after_sigkill', self.verify)
        self.health()
        assert not self.fault, self.fault
        logs = '\n'.join(path.read_text(errors='replace') for path in self.root.glob('engine-*.log'))
        assert 'panicked at' not in logs and 'memory allocation of' not in logs, 'Engine panic or allocation failure'
        assert self.counters['cancelled_sessions_verified_invisible'] == self.args.objects
        self.report.update(status='PASS', counters=dict(self.counters),
                           acknowledged_versions=len(self.acknowledged), peak_active_requests=self.peak_active,
                           elapsed_seconds=round(time.monotonic() - self.started, 3))

    def finish(self):
        self.report['counters'] = dict(self.counters)
        self.report['acknowledged_versions'] = len(self.acknowledged)
        (self.root / 'health-observations.json').write_text(json.dumps(self.health_observations, indent=2))
        super().finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--data-shards', type=int, default=1)
    parser.add_argument('--parity-shards', type=int, default=1)
    parser.add_argument('--io-backend', choices=('auto', 'uring', 'blocking'), default='uring')
    parser.add_argument('--objects', type=int, default=300)
    parser.add_argument('--concurrency', type=int, default=100)
    parser.add_argument('--part-size', type=int, default=65536)
    parser.add_argument('--port', type=int, default=57601)
    parser.add_argument('--deadline-seconds', type=int, default=1800)
    parser.add_argument('--request-timeout-seconds', type=int, default=120)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    test = DurableRaceProof(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
