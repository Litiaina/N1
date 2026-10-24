#!/usr/bin/env python3
"""Isolated strict small-file group qualification on real enrolled test members.

Never contacts an existing engine, formats a device, or removes fixture data.
Uses the established RC fixture's verified loopback TLS and owned-process kill.
This is a bounded correctness gate, NOT a million-object or power-cut result.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.parse

sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location('small_batch_rc_fixture', Path(__file__).with_name('n1-rc-stress.py'))
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)

SYNC_LABELS = tuple('upload.batch.group_sync_' + name for name in ('payload', 'ndx', 'ncx'))
STARTUP_TIMEOUT_SECONDS = 120
STOP_TIMEOUT_SECONDS = 120
ULID = re.compile(r'[0-7][0-9A-HJKMNP-TV-Z]{25}')
CORE_PHASES = (
    'mixed-boundaries', 'object-count-group-split', 'byte-budget-group-split',
    'duplicate-and-canonical-key-order', 'partial-invalid-fields',
    'new-version-preserves-ncx-and-old-immutables', 'same-key-batch-one-shot-and-mutations',
    'complete-integrity-before-sigkill', 'complete-integrity-after-sigkill',
    'ordinary-layout-and-group-evidence',
)
FAULT_PHASES = tuple(f'fault-{kind}-round-{number}' for kind in ('eio', 'sigkill') for number in (1, 2, 3))


def canonical(key):
    parts = [part for part in key.strip().split('/') if part]
    assert parts and not any(part in ('.', '..') for part in parts), 'Invalid test object key'
    return '/'.join(parts)


def field(key, size=512, seed=None, **options):
    return dict(key=key, data=base.payload(seed or key, size), **options)


def multipart(fields, boundary):
    assert re.fullmatch(r'[A-Za-z0-9_-]+', boundary)
    pieces = []
    for item in fields:
        name = item.get('name', 'object')
        assert not any(character in name for character in '\r\n"')
        disposition = f'Content-Disposition: form-data; name="{name}"'
        if not item.get('missing_filename', False):
            disposition += '; filename="' + urllib.parse.quote(item['key'], safe='') + '"'
        pieces.extend([
            ('--' + boundary + '\r\n' + disposition +
             '\r\nContent-Type: application/octet-stream\r\n\r\n').encode(),
            item['data'], b'\r\n',
        ])
    pieces.append(('--' + boundary + '--\r\n').encode())
    return b''.join(pieces)


def validate_reply(status, body, fields, allow_conflicts=False):
    reply = json.loads(body)
    assert set(reply) == {'requested', 'succeeded', 'failed', 'results'}, 'Batch response schema changed'
    assert all(type(reply[key]) is int for key in ('requested', 'succeeded', 'failed'))
    assert reply['requested'] == len(fields) == len(reply['results']) > 0
    succeeded = 0
    for item, result in zip(fields, reply['results']):
        expected = item.get('status', 201)
        allowed = (201, 409) if allow_conflicts and expected == 201 else (expected,)
        assert type(result['status']) is int and result['status'] in allowed, (item['key'], result)
        assert result['object_key'] == item.get('response_key', item['key']), (item['key'], result)
        if result['status'] == 201:
            assert set(result) == {'object_key', 'status', 'version_id', 'size_bytes'}
            assert type(result['size_bytes']) is int and result['size_bytes'] == len(item['data'])
            assert isinstance(result['version_id'], str) and ULID.fullmatch(result['version_id'])
            succeeded += 1
        else:
            assert set(result) <= {'object_key', 'status', 'size_bytes', 'error'}
            assert isinstance(result.get('error'), str) and result['error']
            assert 'version_id' not in result, 'Failed field must not claim a version acknowledgement'
    assert reply['succeeded'] == succeeded and reply['failed'] == len(fields) - succeeded
    assert status == (201 if succeeded == len(fields) else 207), (status, reply)
    return reply


def group_counts(health):
    classes = {entry['label']: entry for entry in health['io_control']['latency']['classes']}
    return {label: (classes.get(label, {}).get('observed', 0), classes.get(label, {}).get('failures', 0))
            for label in SYNC_LABELS}


def group_delta(before, after, expected=None):
    deltas = {}
    for label in SYNC_LABELS:
        old, old_failures = before[label]
        new, new_failures = after[label]
        assert all(type(number) is int and number >= 0 for number in (old, new, old_failures, new_failures))
        assert new > old and new_failures == old_failures, f'Missing/failed actual group execution: {label}'
        deltas[label] = new - old
    assert len(set(deltas.values())) == 1, f'Incomplete persistence boundaries: {deltas}'
    if expected is not None:
        assert set(deltas.values()) == {expected}, (expected, deltas)
    return deltas


def retain_inventory(before, after):
    assert before, 'No old immutable files were captured'
    assert all(after.get(name) == value for name, value in before.items()), 'Old immutable file changed/disappeared'
    return len(before)


def validate_fault_evidence(marker, trace, number, width, kind, expected_devices):
    assert kind in ('eio', 'sigkill')
    assert all(type(marker.get(key)) is int for key in ('round', 'width', 'completed'))
    assert marker.get('after_real_syncfs') is True
    assert marker == dict(round=number, width=width, completed=width, after_real_syncfs=True)
    expected_calls = number * width
    assert len(trace) == expected_calls, 'Missing or extra scoped syncfs calls'
    assert sorted(row['call'] for row in trace) == list(range(1, expected_calls+1))
    expected_devices = set(expected_devices)
    assert len(expected_devices) == width and all(type(device) is int and device > 0 for device in expected_devices)
    initial_fds = None
    for round_number in range(1, number+1):
        rows = [row for row in trace if row['round'] == round_number]
        assert len(rows) == width, 'Incomplete filesystem-member barrier'
        devices = {row.get('device') for row in rows}
        fds = {row.get('fd') for row in rows}
        assert devices == expected_devices, 'Barrier did not synchronize each enrolled filesystem exactly once'
        assert len(fds) == width and all(type(fd) is int and fd >= 0 for fd in fds)
        if initial_fds is None:
            initial_fds = fds
        assert fds == initial_fds, 'Filesystem barrier descriptors changed across publication boundaries'
        for row in rows:
            assert row['event'] == 'syncfs' and row['actual_result'] == 0 and row['actual_errno'] == 0
            failed = kind == 'eio' and round_number == number
            assert row['returned_result'] == (-1 if failed else 0)
            assert row['errno'] == (5 if failed else 0)
    return dict(scoped_syncfs_calls=expected_calls, all_actual_syncfs_completed=True,
                distinct_member_filesystems_verified=width)


def require_terminal(report):
    phases = report['phases']
    assert report.get('status') == 'PASS' and report.get('fault') is None
    assert [phase['name'] for phase in phases] == list(CORE_PHASES + FAULT_PHASES)
    assert all(phase.get('status') == 'PASS' for phase in phases)
    assert type(report.get('acknowledged_versions')) is int and report['acknowledged_versions'] >= 100
    assert report.get('strict_uploads') is True and report.get('automatic_write_retries') is False
    assert report.get('configured_pool_verified') is True
    assert type(report.get('historical_hash_verifications')) is int
    assert report['historical_hash_verifications'] >= 2 * report['acknowledged_versions']
    assert report.get('group_sync_execution_verified') is True
    assert report.get('fault_injection') == dict(
        eio_rounds=[1, 2, 3], sigkill_rounds=[1, 2, 3],
        successful_acknowledgements=0, all_prior_acks_verified=True, test_only_ld_preload=True)
    for phase in phases[:5]:
        deltas = phase.get('group_sync_deltas')
        assert isinstance(deltas, dict) and set(deltas) == set(SYNC_LABELS)
        assert all(type(value) is int and value > 0 for value in deltas.values())
    for phase in phases[-6:]:
        assert type(phase.get('injected_round_observed')) is int and phase['injected_round_observed'] in (1, 2, 3)
        assert phase.get('successful_acknowledgements') == 0
        assert phase.get('prior_acknowledgements_verified') == report['acknowledged_versions']
        assert phase.get('all_actual_syncfs_completed') is True
        assert phase.get('objects_per_fault_group') == 64 and phase.get('existing_update_exercised') is True
        assert phase.get('every_visible_attempt_verified') is True
    return report


def require_volume(parent):
    parent = parent.resolve(strict=True)
    mount = next(path for path in (parent, *parent.parents) if os.path.ismount(path))
    assert mount != Path('/') and parent.stat().st_dev == mount.stat().st_dev, 'Use mounted data volumes only'
    device = parent.stat().st_dev
    block = (Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}').resolve(strict=True)
    return block.parent if (block / 'partition').exists() else block


class SmallBatchProof(base.Stress):
    def __init__(self, args):
        require_volume(args.root.parent)
        devices = [require_volume(parent) for parent in args.member_parent]
        assert len(set(devices)) == len(devices), 'One independent physical drive per EC member required'
        self.shim_environment = {}
        super().__init__(args)
        self.config['garbage_collector']['cleanup_interval_seconds'] = '3600'
        self.write_config()
        self.locations = {}
        self.hash_checks = 0
        self.report.update(
            proof='bounded strict small-file groups and all three persistence boundaries',
            harness_sha256=base.digest(Path(__file__).read_bytes()),
            fixture_helper_sha256=base.digest(Path(__file__).with_name('n1-rc-stress.py').read_bytes()),
            fault_shim_sha256=base.digest(args.fault_shim.read_bytes()),
            physical_block_devices=list(map(str, devices)),
            client_body_note='Bounded correctness requests are materialized by this fixture; browser/million-file discovery is not qualified here',
            asserted_server_group_bounds=dict(objects=64, bytes=8*1024**2, object_bytes=1024**2),
            automatic_write_retries=False,
            startup_timeout_seconds=args.startup_timeout_seconds,
            stop_timeout_seconds=args.stop_timeout_seconds,
            startup_timings=[],
            fault=None,
        )

    def start(self, label):
        assert not self.storage_problem(), self.storage_problem()
        environment = {key: value for key, value in os.environ.items() if not key.startswith('N1_')}
        environment.update(RUST_LOG='info', RUST_BACKTRACE='1',
                           N1_IO_BACKEND=self.io_backend,
                           N1_REFRESH_STORE_ROOT=str(self.root/'refresh'), **self.shim_environment)
        log = (self.root/f'engine-{label}.log').open('wb')
        try:
            self.process = subprocess.Popen([str(self.args.binary)], cwd=self.root, env=environment, stdout=log, stderr=log)
        finally:
            log.close()
        self.generation += 1
        started = time.monotonic()
        deadline = started + self.args.startup_timeout_seconds
        timing = dict(label=label, owned_engine_pid=self.process.pid,
                      timeout_seconds=self.args.startup_timeout_seconds, status='STARTING')
        self.report['startup_timings'].append(timing)
        try:
            while time.monotonic() < deadline:
                assert not self.fault, self.fault
                assert self.process.poll() is None, 'Owned test engine exited during startup'
                connection = http.client.HTTPSConnection('127.0.0.1', self.args.port,
                    context=self.tls, timeout=max(.01, min(2, deadline-time.monotonic())))
                try:
                    connection.request('GET', '/noa/v1/ping')
                    response = connection.getresponse()
                    response.read()
                    if response.status == 200:
                        timing['status'] = 'PASS'
                        return
                except (OSError, http.client.HTTPException):
                    pass
                finally:
                    connection.close()
                time.sleep(.2)
            raise AssertionError(f'Owned test engine startup exceeded {self.args.startup_timeout_seconds} seconds')
        except BaseException as error:
            timing.update(status='FAIL', error=f'{type(error).__name__}: {error}')
            raise
        finally:
            timing['elapsed_seconds'] = round(time.monotonic()-started, 3)

    def stop(self):
        process = self.process
        self.process = None
        if process is not None:
            if process.poll() is None:
                process.kill()
            try:
                process.wait(timeout=self.args.stop_timeout_seconds)
            except BaseException:
                # Retain ownership so final cleanup can still reap this exact
                # process; never substitute broad process-name termination.
                self.process = process
                raise

    def finish(self):
        original = sys.exc_info()[1]
        cleanup_error = None
        self.stop_monitor.set()
        self.monitor.join(timeout=3)
        try:
            self.stop()
        except BaseException as error:
            cleanup_error = error
            self.report.update(status='FAIL')
            if not self.report.get('fault'):
                self.report['fault'] = f'{type(error).__name__}: {error}'
            self.report['cleanup_error'] = f'{type(error).__name__}: {error}'
            self.report['owned_engine_exit_unconfirmed'] = True
            self.report['owned_engine_pid'] = getattr(self.process, 'pid', None)
        if original is not None:
            self.report['status'] = 'FAIL'
            self.report['original_error'] = f'{type(original).__name__}: {original}'
        self.report['resources'] = dict(samples=len(self.samples),
            peak_rss_kib=max((sample['rss_kib'] for sample in self.samples), default=0),
            sampled_process_hwm_kib=max((sample['hwm_kib'] for sample in self.samples), default=0),
            peak_fds=max((sample['fds'] for sample in self.samples), default=0))
        self.report['elapsed_seconds'] = round(time.monotonic()-self.started, 3)
        try:
            (self.root/'resource-samples.json').write_text(json.dumps(self.samples))
        except OSError as error:
            cleanup_error = cleanup_error or error
            self.report.update(status='FAIL', resource_evidence_error=f'{type(error).__name__}: {error}')
        try:
            (self.root/'results.json').write_text(json.dumps(self.report, indent=2))
        except OSError as error:
            cleanup_error = cleanup_error or error
            print(f'Cannot preserve small-batch result: {error}', file=sys.stderr, flush=True)
        print(json.dumps(self.report, indent=2), flush=True)
        if cleanup_error is not None and original is None:
            raise cleanup_error

    def health(self):
        health = self.api('GET', '/maintenance/health?' + urllib.parse.urlencode({'fragment': self.fragment, 'refresh': 'true'}))
        base.verify_io_backend(health, self.args)
        assert health['io_control']['process']['pid'] == self.process.pid, 'Wrong process health'
        assert health['io_control']['backend']['publication_backend'] == 'posix'
        with (self.root/'health-observations.jsonl').open('a') as stream:
            stream.write(json.dumps(health) + '\n')
        return health

    def phase_run(self, name, function):
        self.phase = name
        self.report['current_phase'] = name
        (self.root/'progress.json').write_text(json.dumps(self.report, indent=2))
        print('START', name, flush=True)
        start = time.monotonic()
        details = function() or {}
        self.phases.append(dict(name=name, status='PASS', elapsed_seconds=round(time.monotonic()-start, 3), **details))
        (self.root/'progress.json').write_text(json.dumps(self.report, indent=2))
        print('DONE', json.dumps(self.phases[-1]), flush=True)

    def acknowledge(self, key, version, data, locations=None):
        assert not any(ack[2] == version for ack in self.acknowledged), 'Duplicate acknowledgement version'
        ack = (self.token, canonical(key), version, base.digest(data), len(data))
        with self.lock:
            self.acknowledged.append(ack)
            if locations:
                self.locations[version] = locations
            with (self.root/'acknowledgements.jsonl').open('a') as stream:
                stream.write(json.dumps(dict(object_key=ack[1], version_id=version, sha256=ack[3], size=ack[4])) + '\n')

    def raw_batch(self, fields):
        boundary = 'n1-small-batch-' + secrets.token_hex(12)
        return self.call('POST', '/upload/batch?durability=strict&concurrency=16', multipart(fields, boundary),
                         headers={'Content-Type': 'multipart/form-data; boundary=' + boundary})

    def batch(self, fields, groups=None, locations=None, evidence=True, allow_conflicts=False):
        before = group_counts(self.health()) if evidence else None
        reply = validate_reply(*self.raw_batch(fields), fields, allow_conflicts=allow_conflicts)
        for item, entry in zip(fields, reply['results']):
            if entry['status'] == 201:
                self.acknowledge(item['key'], entry['version_id'], item['data'], locations)
        details = dict(requested=reply['requested'], succeeded=reply['succeeded'], failed=reply['failed'])
        if evidence:
            details['group_sync_deltas'] = group_delta(before, group_counts(self.health()), groups)
        return reply, details

    def one_shot(self, key, data, locations=None):
        reply = self.api('POST', '/upload/one-shot/' + urllib.parse.quote(key, safe='/') + '?durability=strict', data)
        self.acknowledge(key, reply['version_id'], data, locations)
        return reply

    def read_ack(self, ack):
        token, key, version, expected, size = ack
        for location in self.locations.get(version, [key]):
            status, data = self.call('GET', '/objects/download?' + urllib.parse.urlencode(
                dict(object_key=location, version_id=version)), token=token)
            if status == 404:
                continue
            assert status == 200 and len(data) == size and base.digest(data) == expected, f'ACK HASH MISMATCH {key}@{version}'
            with self.lock:
                self.hash_checks += 1
            return
        raise AssertionError(f'Acknowledged version disappeared from all test names: {key}@{version}')

    def verify_all(self):
        self.parallel(self.read_ack, list(self.acknowledged), 8)
        return dict(acknowledged_versions_hashed=len(self.acknowledged))

    def ncx_inventory(self, key):
        parts = canonical(key).split('/')
        relative = Path(*(part + ('.ncx' if index == len(parts)-1 else '.ncp') for index, part in enumerate(parts)))
        result = {}
        for slot, root in enumerate(self.roots):
            for fragment in root.glob('@f_*'):
                path = fragment/'.namespace'/relative
                if path.exists():
                    assert not path.is_symlink()
                    stat = path.stat()
                    data = path.read_bytes()
                    assert data[:7] == b'N1NCX4\0' and hashlib.sha256(data[39:]).digest() == data[7:39]
                    result[f'{slot}/{path.relative_to(root)}'] = (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, base.digest(data))
        assert len(result) == min(self.args.parity_shards+1, len(self.roots)), 'Wrong NCX holder count'
        return result

    def check_max_ulid(self, key, expected=None, attempted_payloads=None):
        versions = self.versions(key)
        assert versions, 'Empty version inventory'
        if expected is not None:
            assert {entry['version_id'] for entry in versions} == set(expected)
        version = max(entry['version_id'] for entry in versions)
        ack = next((ack for ack in self.acknowledged if ack[2] == version), None)
        if ack is None:
            assert attempted_payloads is not None, 'Latest version has no acknowledged or attempted payload witness'
            status, historical = self.call('GET', '/objects/download?' + urllib.parse.urlencode(
                dict(object_key=key, version_id=version)))
            assert status == 200 and (len(historical), base.digest(historical)) in attempted_payloads
            expected_hash = base.digest(historical)
        else:
            expected_hash = ack[3]
        status, data = self.call('GET', '/objects/download?' + urllib.parse.urlencode({'object_key': key}))
        assert status == 200 and base.digest(data) == expected_hash, 'Latest is not MAX committed ULID'

    def configured_pool(self):
        for slot, root in enumerate(self.roots):
            descriptor = json.loads((root/'.n1-pool.json').read_text())
            assert descriptor['member_format'] == 'n1-pool-member-v1' and descriptor['member_slot'] == slot
            assert descriptor['data_shards'] == self.args.data_shards
            assert descriptor['parity_shards'] == self.args.parity_shards
        self.report['configured_pool_verified'] = True

    def boundaries(self):
        sizes = (0, 1, 4097, 65536, 1048576)
        _, details = self.batch([field(f'boundaries/{size}.bin', size) for size in sizes], groups=1)
        return dict(sizes=list(sizes), **details)

    def count_split(self):
        _, details = self.batch([field(f'count-split/{index}.bin', 1) for index in range(65)], groups=2)
        return dict(group_object_bound=64, **details)

    def byte_split(self):
        _, details = self.batch([field(f'byte-split/{index}.bin', 1048576) for index in range(17)], groups=3)
        return dict(group_byte_bound=8388608, total_body_bytes=17*1048576, **details)

    def repeated(self):
        values = [field('duplicates/same.bin', 4097, seed=f'version-{index}') for index in range(3)]
        values += [field('aliases//same.bin', 1, seed='alias-1'), field('aliases/same.bin', 4097, seed='alias-2')]
        reply, details = self.batch(values, groups=4)
        duplicate_versions = [entry['version_id'] for entry in reply['results'][:3]]
        alias_versions = [entry['version_id'] for entry in reply['results'][3:]]
        assert duplicate_versions == sorted(duplicate_versions) and len(set(duplicate_versions)) == 3
        assert alias_versions == sorted(alias_versions) and len(set(alias_versions)) == 2
        self.check_max_ulid('duplicates/same.bin', duplicate_versions)
        self.check_max_ulid('aliases/same.bin', alias_versions)
        return dict(duplicate_versions=duplicate_versions, canonical_alias_versions=alias_versions, **details)

    def invalid_fields(self):
        values = [
            field('partial/before.bin', 0),
            field('partial/wrong-name.bin', 1, name='file', status=400, response_key='<invalid>'),
            field('partial/no-filename.bin', 1, missing_filename=True, status=400, response_key='<invalid>'),
            field('.trash/forbidden.bin', 1, status=400),
            field('partial/oversized.bin', 1048577, status=413),
            field('partial/after.bin', 4097),
        ]
        _, details = self.batch(values, groups=1)
        for key in ('partial/wrong-name.bin', 'partial/no-filename.bin', 'partial/oversized.bin'):
            assert self.call('GET', '/objects/versions/' + key)[0] == 404, f'Invalid field became visible: {key}'
        return details

    def version_immutability(self):
        key = 'immutability/versioned.bin'
        self.one_shot(key, base.payload('original', 4097))
        ncx_before = self.ncx_inventory(key)
        before = self.immutable_inventory()
        _, details = self.batch([field(key, 65536, seed='batch-revision')], groups=1)
        assert self.ncx_inventory(key) == ncx_before, 'New version rewrote stable NCX'
        self.one_shot(key, base.payload('one-shot-revision', 1))
        assert self.ncx_inventory(key) == ncx_before, 'One-shot revision rewrote stable NCX'
        retained = retain_inventory(before, self.immutable_inventory())
        self.check_max_ulid(key)
        return dict(old_immutable_files_unchanged=retained, ncx_holders_unchanged=len(ncx_before), **details)

    def contention(self):
        source, destination = 'contention/shared.bin', 'contention/moved.bin'
        locations = [source, destination]
        attempted_payloads = set()

        def register(data):
            with self.lock:
                attempted_payloads.add((len(data), base.digest(data)))
            return data

        self.one_shot(source, register(base.payload('contention-initial', 4097)), locations)
        old = self.immutable_inventory()
        start = threading.Barrier(3)
        outcomes = []
        batch_ready = threading.Event()
        one_shot_ready = threading.Event()

        def batch_writer():
            start.wait(timeout=20)
            # Establish real progress from BOTH writers before namespace moves
            # begin. Further identity changes can legitimately refuse a field
            # with 409; those attempts are recorded, never automatically retried.
            first = field(source, 4097, seed='concurrent-batch-0')
            register(first['data'])
            self.batch([first], locations=locations, evidence=False)
            batch_ready.set()
            assert one_shot_ready.wait(timeout=self.args.request_timeout_seconds)
            values = [field(source, 4097, seed=f'concurrent-batch-{index}') for index in range(1, 4)]
            for item in values:
                register(item['data'])
            reply, _ = self.batch(values, locations=locations, evidence=False, allow_conflicts=True)
            return dict(acknowledged=1+reply['succeeded'], conflicts=reply['failed'])

        def one_shot_writer():
            start.wait(timeout=20)
            self.one_shot(source, register(base.payload('concurrent-one-shot-0', 4097)), locations)
            one_shot_ready.set()
            assert batch_ready.wait(timeout=self.args.request_timeout_seconds)
            acknowledgements, conflicts = 1, 0
            for index in range(1, 4):
                data = register(base.payload(f'concurrent-one-shot-{index}', 4097))
                status, body = self.call('POST', '/upload/one-shot/' + source + '?durability=strict', data)
                assert status in (201, 409), f'Unexpected concurrent one-shot response {status}: {body[:200]!r}'
                if status == 201:
                    reply = json.loads(body)
                    assert reply['object_key'] == source and ULID.fullmatch(reply['version_id'])
                    self.acknowledge(source, reply['version_id'], data, locations)
                    acknowledgements += 1
                else:
                    conflicts += 1
            return dict(acknowledged=acknowledgements, conflicts=conflicts)

        def mutator():
            start.wait(timeout=20)
            assert batch_ready.wait(timeout=self.args.request_timeout_seconds)
            assert one_shot_ready.wait(timeout=self.args.request_timeout_seconds)
            for index in range(8):
                src, dst = (source, destination) if index % 2 == 0 else (destination, source)
                status, body = self.call('POST', '/posix/rename', {'source_path': src, 'destination_path': dst})
                assert status in (200, 404, 409), f'Unexpected concurrent mutation response {status}'
                affected = json.loads(body)['affected'] if status == 200 else 0
                assert type(affected) is int and affected >= 0
                outcomes.append(dict(status=status, affected=affected))
            return 8

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(worker) for worker in (batch_writer, one_shot_writer, mutator)]
            batch_result, one_shot_result, mutation_count = [future.result() for future in futures]
        assert mutation_count == 8
        for result in (batch_result, one_shot_result):
            assert result['acknowledged'] >= 1 and result['acknowledged'] + result['conflicts'] == 4
        expected = {ack[2] for ack in self.acknowledged if ack[2] in self.locations}
        observed = set()
        observed_locations = {}
        for key in locations:
            status, body = self.call('GET', '/objects/versions/' + key)
            assert status in (200, 404)
            if status == 200:
                for entry in json.loads(body):
                    observed.add(entry['version_id'])
                    observed_locations.setdefault(entry['version_id'], []).append(key)
                self.check_max_ulid(key, attempted_payloads=attempted_payloads)
        assert expected <= observed, 'Concurrent mutation/publication lost an acknowledged version'
        unacknowledged = []
        for version in sorted(observed - expected):
            location = observed_locations[version][0]
            status, data = self.call('GET', '/objects/download?' + urllib.parse.urlencode(
                dict(object_key=location, version_id=version)))
            assert status == 200 and (len(data), base.digest(data)) in attempted_payloads, (
                'Unacknowledged version must contain the COMPLETE bytes of an actually attempted contender')
            unacknowledged.append(dict(object_key=location, version_id=version,
                size=len(data), sha256=base.digest(data)))
        (self.root/'unacknowledged-complete-attempts.json').write_text(json.dumps(unacknowledged, indent=2))
        self.parallel(self.read_ack, [ack for ack in self.acknowledged if ack[2] in expected], 8)
        successful_mutations = sum(outcome['affected'] for outcome in outcomes)
        assert successful_mutations > 0, 'No successful concurrent mutation was exercised'
        return dict(batch_acknowledgements=batch_result['acknowledged'], batch_explicit_conflicts=batch_result['conflicts'],
                    one_shot_acknowledgements=one_shot_result['acknowledged'], one_shot_explicit_conflicts=one_shot_result['conflicts'],
                    offered_writes=8, write_retries=0,
                    namespace_mutation_attempts=8, successful_namespace_mutations=successful_mutations,
                    namespace_mutation_statuses={str(code): sum(outcome['status'] == code for outcome in outcomes)
                        for code in {outcome['status'] for outcome in outcomes}},
                    old_immutable_files_unchanged=retain_inventory(old, self.immutable_inventory()),
                    historical_acknowledged_versions_reconciled_across_both_names=len(expected),
                    unacknowledged_complete_attempts_verified=len(unacknowledged))

    def layout(self):
        private = []
        ndx_count = 0
        shard_count = 0
        for root in self.roots:
            for fragment in root.glob('@f_*'):
                assert not fragment.is_symlink()
                for path in (fragment/'.objects').rglob('*'):
                    assert not path.is_symlink(), 'Unexpected redirected object layout'
                    if path.is_file():
                        if path.name.startswith('.n1-batch-'):
                            private.append(str(path.relative_to(root)))
                        elif path.suffix == '.ndx':
                            assert ULID.fullmatch(path.name.split('.')[0]), 'Changed version-summary filename'
                            ndx_count += 1
                        elif path.name.startswith('part_0_shard_'):
                            assert path.suffix in ('.shard', '.parity'), 'Changed shard filename'
                            shard_count += 1
        assert ndx_count > 0 and shard_count > 0 and not private, private
        health = self.health()
        counts = group_counts(health)
        assert all(value[0] >= 1 and value[1] == 0 for value in counts.values()), counts
        backend = health['io_control']['backend']
        if self.args.io_backend == 'uring':
            assert backend['submitted'] > 0 and backend['completed'] > 0
        return dict(ordinary_ndx_files=ndx_count, ordinary_shard_files=shard_count,
                    private_batch_files_remaining=0, group_sync_counters=counts, io_backend=backend)

    def fault_case(self, kind, number):
        old_immutables = self.immutable_inventory()
        existing_key = 'immutability/versioned.bin'
        prior_existing_versions = {entry['version_id'] for entry in self.versions(existing_key)}
        self.stop()
        directory = self.root/f'fault-{kind}-{number}'
        directory.mkdir(mode=0o700)
        marker = directory/'barrier'
        trigger = directory/'trigger'
        self.shim_environment = dict(
            LD_PRELOAD=str(self.args.fault_shim),
            N1_TEST_BATCH_TRIGGER=str(trigger),
            N1_TEST_BATCH_ROOTS=':'.join(map(str, self.roots)),
            N1_TEST_BATCH_WIDTH=str(len({root.stat().st_dev for root in self.roots})),
            N1_TEST_BATCH_MARKER_BASE=str(marker),
            N1_TEST_BATCH_RELEASE=str(directory/'release'),
            N1_TEST_BATCH_TRACE=str(directory/'syscalls.jsonl'),
            **{('N1_TEST_BATCH_FAIL_ROUND' if kind == 'eio' else 'N1_TEST_BATCH_PAUSE_ROUND'): str(number)},
        )
        self.start(f'fault-{kind}-{number}')
        self.health()
        trigger.touch(exist_ok=False)
        values = [field(f'fault-candidates/{kind}-{number}-{index:02}.bin', 4097)
                  for index in range(63)]
        values.append(field(existing_key, 4097, seed=f'fault-update-{kind}-{number}'))
        if not hasattr(self, 'fault_existing_attempt_payloads'):
            self.fault_existing_attempt_payloads = set()
        self.fault_existing_attempt_payloads.add((len(values[-1]['data']), base.digest(values[-1]['data'])))
        if kind == 'eio':
            status, body = self.raw_batch(values)
            reply = validate_reply(status, body, [dict(value, status=500) for value in values])
            assert reply['succeeded'] == 0
            observed = directory/f'barrier.round-{number}.json'
            assert observed.is_file(), 'Requested syncfs error boundary was not observed'
            self.stop()
        else:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.raw_batch, values)
                observed = directory/f'barrier.paused-round-{number}.json'
                deadline = time.monotonic() + self.args.request_timeout_seconds
                while not observed.exists() and time.monotonic() < deadline:
                    assert not future.done(), 'Batch finished without reaching requested pause boundary'
                    assert self.process.poll() is None
                    time.sleep(.01)
                assert observed.is_file(), 'Requested syncfs pause boundary was not observed'
                self.stop()
                try:
                    response = future.result(timeout=20)
                except (OSError, http.client.HTTPException):
                    response = None
                assert response is None, f'Paused incomplete group unexpectedly received response: {response}'
        marker_evidence = json.loads(observed.read_text())
        trace = [json.loads(line) for line in (directory/'syscalls.jsonl').read_text().splitlines()]
        trace_evidence = validate_fault_evidence(marker_evidence, trace, number,
            len({root.stat().st_dev for root in self.roots}), kind,
            {root.stat().st_dev for root in self.roots})
        self.shim_environment = {}
        self.start(f'after-fault-{kind}-{number}')
        self.health()
        self.verify_all()
        visible_new = 0
        for value in values[:-1]:
            status, body = self.call('GET', '/objects/download?' + urllib.parse.urlencode({'object_key': value['key']}))
            assert status in (200, 404)
            if status == 200:
                assert body == value['data'], 'A visible unacknowledged version contains incomplete data'
                visible_new += 1
            if number < 3:
                assert status == 404, 'New namespace became visible before its publication boundary'
        current_existing = {entry['version_id'] for entry in self.versions(existing_key)}
        assert prior_existing_versions <= current_existing, 'Fault erased an existing immutable version'
        unacknowledged_updates = current_existing - prior_existing_versions
        assert len(unacknowledged_updates) <= 1, 'Fault attempt created more than one existing-object version'
        if number == 1:
            assert not unacknowledged_updates, 'NDX became visible before payload persistence boundary'
        for version in unacknowledged_updates:
            status, body = self.call('GET', '/objects/download?' + urllib.parse.urlencode(
                dict(object_key=existing_key, version_id=version)))
            assert status == 200 and body == values[-1]['data'], 'Fault exposed incomplete existing-object version'
        self.check_max_ulid(existing_key, current_existing,
            attempted_payloads=self.fault_existing_attempt_payloads)
        old_unchanged = retain_inventory(old_immutables, self.immutable_inventory())
        return dict(injected_round_observed=number, marker=marker_evidence,
                    successful_acknowledgements=0, prior_acknowledgements_verified=len(self.acknowledged),
                    **trace_evidence,
                    objects_per_fault_group=64, existing_update_exercised=True,
                    every_visible_attempt_verified=True, visible_unacknowledged_new_objects=visible_new,
                    visible_unacknowledged_existing_updates=len(unacknowledged_updates),
                    old_immutable_files_unchanged=old_unchanged,
                    interruption='injected EIO after actual syncfs' if kind == 'eio' else 'SIGKILL at observed completed syncfs boundary')

    def run(self):
        self.start('initial')
        self.fragment, self.token = self.create('small-batch-proof-' + secrets.token_hex(5))
        self.configured_pool()
        self.health()
        for name, function in zip(CORE_PHASES[:7], (
            self.boundaries, self.count_split, self.byte_split, self.repeated,
            self.invalid_fields, self.version_immutability, self.contention,
        )):
            self.phase_run(name, function)
        self.phase_run(CORE_PHASES[7], self.verify_all)
        self.stop()
        self.start('after-acknowledged-sigkill')
        self.phase_run(CORE_PHASES[8], self.verify_all)
        # Metrics reset at restart; preserve positive live execution evidence
        # with one further strict group before evaluating current PID counters.
        self.batch([field('post-restart/native.bin', 1)], groups=1)
        self.verify_all()
        self.phase_run(CORE_PHASES[9], self.layout)
        for kind in ('eio', 'sigkill'):
            for number in (1, 2, 3):
                self.phase_run(f'fault-{kind}-round-{number}', lambda kind=kind, number=number: self.fault_case(kind, number))
        assert not self.fault, self.fault
        logs = '\n'.join(path.read_text(errors='replace') for path in self.root.glob('engine-*.log'))
        assert 'panicked at' not in logs and 'memory allocation of' not in logs
        self.report.update(status='PASS', fault=None, acknowledged_versions=len(self.acknowledged),
                           historical_hash_verifications=self.hash_checks,
                           group_sync_execution_verified=True,
                           fault_injection=dict(eio_rounds=[1, 2, 3], sigkill_rounds=[1, 2, 3],
                               successful_acknowledgements=0, all_prior_acks_verified=True, test_only_ld_preload=True),
                           elapsed_seconds=round(time.monotonic()-self.started, 3))
        require_terminal(self.report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--data-shards', type=int, default=3)
    parser.add_argument('--parity-shards', type=int, default=1)
    parser.add_argument('--port', type=int, default=58901)
    parser.add_argument('--io-backend', choices=('uring', 'blocking'), default='uring')
    parser.add_argument('--fault-shim', type=Path, required=True)
    parser.add_argument('--deadline-seconds', type=int, default=1800)
    parser.add_argument('--request-timeout-seconds', type=int, default=180)
    parser.add_argument('--startup-timeout-seconds', type=int, default=STARTUP_TIMEOUT_SECONDS)
    parser.add_argument('--stop-timeout-seconds', type=int, default=STOP_TIMEOUT_SECONDS)
    args = parser.parse_args()
    assert 1 <= args.startup_timeout_seconds <= 600 and 1 <= args.stop_timeout_seconds <= 600
    args.binary = args.binary.resolve(strict=True)
    args.fault_shim = args.fault_shim.resolve(strict=True)
    assert args.binary.is_file() and args.fault_shim.is_file()
    os.umask(0o077)
    test = SmallBatchProof(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', fault=f'{type(error).__name__}: {error}')
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
