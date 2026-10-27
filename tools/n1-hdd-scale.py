#!/usr/bin/env python3
"""Disposable, bounded-client million-file/churn gate on real member filesystems.

No connection to existing engines; no formatting; no personal-file traversal.
The acknowledgement ledger and real source folder live beside the fixture.
All accepted historical versions are downloaded and SHA256 checked. A deadline,
refusal, ambiguous write, or resource stop is not a million-object PASS.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import http.client
import importlib.util
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import signal
import sqlite3
import sys
import threading
import time
import traceback
import urllib.parse

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('rc_stress', Path(__file__).with_name('n1-rc-stress.py'))
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


class Histogram:
    """Fixed 1024-bin log histogram; quantiles are explicitly upper bounds."""
    def __init__(self):
        self.bins = [0] * 1024
        self.count, self.total, self.maximum = 0, 0.0, 0.0

    def append(self, value):
        index = max(0, min(1023, math.floor(math.log2(max(value, 0.001)) * 32) + 320))
        self.bins[index] += 1
        self.count += 1
        self.total += value
        self.maximum = max(self.maximum, value)

    def summary(self):
        result = {'count': self.count, 'max_ms': round(self.maximum, 3)}
        if not self.count:
            return result
        result['mean_ms'] = round(self.total / self.count, 3)
        for name, q in [('median', .5), ('p95', .95), ('p99', .99)]:
            target, seen = math.ceil(self.count * q), 0
            for index, count in enumerate(self.bins):
                seen += count
                if seen >= target:
                    result[name + '_upper_bound_ms'] = round(min(self.maximum, 2 ** ((index - 319) / 32)), 3)
                    break
        return result


def bounded_map(function, iterable, concurrency):
    """At most 2*C pending futures, even for million-entry generators."""
    iterator = iter(iterable)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        pending = set()
        exhausted = False
        while pending or not exhausted:
            while len(pending) < concurrency * 2 and not exhausted:
                try:
                    pending.add(pool.submit(function, next(iterator)))
                except StopIteration:
                    exhausted = True
            if pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    yield future.result()


def source_key(index, flat_folder=False):
    # Repeatable project-like tree, with a useful 1000-object listing prefix.
    if flat_folder:
        return f'projects/million/file-{index:09}.bin'
    return f'projects/project-{index // 1000:06}/file-{index:09}.bin'


def listing_prefixes(count, flat_folder=False):
    """At most three <=1000-name windows, even inside one million-name folder.

    Flat samples are lexical filename prefixes, not physical subdirectories.
    The independent streamed namespace/SQLite audit still verifies every name.
    """
    for index in sorted({0, max(0, count//2//1000), max(0, (count-1)//1000)}):
        start, end = index * 1000, min(count, (index+1)*1000)
        prefix = (f'projects/million/file-{start:09}'[:-3] if flat_folder else
                  f'projects/project-{index:06}')
        yield prefix, start, end


def source_size(index):
    return (0, 512, 1024, 4096, 16384, 65536)[index % 6]


def consume_snapshot(response, database, report_progress=lambda count, scanned: None):
    """Bounded NDJSON audit; progress is never a substitute for completion."""
    count, complete, scanned, progress_frames = 0, None, 0, 0
    read_seconds, ledger_seconds = 0.0, 0.0
    database.execute('DELETE FROM seen')
    while True:
        began = time.monotonic()
        line = response.readline(65537)
        read_seconds += time.monotonic() - began
        if not line:
            break
        assert len(line) <= 65536 and line.endswith(b'\n'), 'Oversized or truncated snapshot frame'
        frame = json.loads(line)
        assert complete is None, 'Frame after snapshot completion'
        if frame['kind'] == 'complete':
            complete = frame['entry_count']
            assert type(complete) is int and complete >= 0, 'Invalid snapshot completion count'
        elif frame['kind'] == 'progress':
            following = frame['scanned_entries']
            assert type(following) is int and following > scanned, 'Snapshot scan did not advance'
            assert type(frame['entry_count']) is int and frame['entry_count'] == count, 'Snapshot progress count mismatch'
            scanned = following
            progress_frames += 1
            report_progress(count, scanned)
        else:
            assert frame['kind'] == 'entry', frame
            began = time.monotonic()
            database.execute('INSERT INTO seen VALUES(?,?)', (frame['object_key'], frame['version_id']))
            count += 1
            if count % 512 == 0:
                database.commit()
                report_progress(count, scanned)
            ledger_seconds += time.monotonic() - began
    began = time.monotonic()
    database.commit()
    ledger_seconds += time.monotonic() - began
    report_progress(count, scanned)
    assert complete == count, f'Incomplete snapshot: count={count}, terminal={complete}'
    return dict(entries=count, progress_frames=progress_frames, scanned_entries=scanned,
                stream_read_seconds=round(read_seconds, 3), ledger_seconds=round(ledger_seconds, 3))


def population_checkpoints(objects):
    """Logarithmic checkpoint plan, including the exact requested ceiling."""
    assert objects >= 1
    checkpoints = {objects, *(min(objects, count) for count in (1000, 10000, 100000, 500000))}
    decade = 1000000
    while decade <= objects:
        checkpoints.update(multiplier * decade for multiplier in (1, 2, 5)
                           if multiplier * decade <= objects)
        decade *= 10
    return sorted(checkpoints)


def require_storage_filesystem(parent):
    """Allow an isolated child on a mounted data volume, never the OS filesystem."""
    parent = parent.resolve(strict=True)
    mount = next((path for path in (parent, *parent.parents) if os.path.ismount(path)), None)
    assert mount is not None and mount != Path('/'), 'Member parent must be on a mounted data filesystem'
    assert parent.stat().st_dev == mount.stat().st_dev, 'Member filesystem changed'


def validate_topology(args):
    assert 1 <= args.data_shards <= 32 and 0 <= args.parity_shards <= 32
    assert len(args.member_parent) == args.data_shards + args.parity_shards, 'One distinct filesystem per EC member required'


def backing_device(path):
    device = path.stat().st_dev
    link = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}'
    assert link.exists(), 'This proof needs an identifiable real block filesystem'
    block = link.resolve(strict=True)
    return block.parent if (block / 'partition').exists() else block


def independent_member_devices(parents):
    devices = [backing_device(parent) for parent in parents]
    assert len(set(devices)) == len(parents), (
        'Separate partitions of one drive are not independent physical members')
    return devices


def storage_budget_problem(roots, free_floor_gib, initial_free_bytes, max_growth_gib):
    for root in roots:
        free = shutil.disk_usage(root).free
        if free < free_floor_gib * 1024**3:
            return 'Free-space safety floor crossed; qualification incomplete'
        if max_growth_gib and initial_free_bytes[root] - free > max_growth_gib * 1024**3:
            return 'Filesystem growth budget crossed (includes other writers); qualification incomplete'
    return None


def process_usage(pid):
    """Optional /proc telemetry is not fixture-storage authority."""
    sample, unavailable = {}, {}
    try:
        status = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in line)
        if 'VmRSS' in status:
            sample['rss_kib'] = int(status['VmRSS'].split()[0])
    except (OSError, ValueError) as error:
        unavailable['rss_kib'] = type(error).__name__
    try:
        sample['fds'] = sum(1 for _ in Path(f'/proc/{pid}/fd').iterdir())
    except OSError as error:
        unavailable['fds'] = type(error).__name__
    if unavailable:
        sample['unavailable'] = unavailable
    return sample


def record_unexpected_engine_exit(test, process):
    """An intentional stop unpublishes ownership before killing the child.

    Poll a stable handle, then decide under the same lock as start/stop. An old
    generation's exit must not fault its replacement or dereference None.
    """
    returncode = process.poll() if process is not None else None
    with test.lock:
        if (returncode is not None and test.process is process
                and not test.stop_monitor.is_set()):
            test.fault = test.fault or f'Engine exited unexpectedly with code {returncode}'


def wait_for_result(path, timeout_seconds=15000, poll_seconds=5):
    """Only a terminal result opens a gate; a visible RUNNING file does not."""
    started = time.monotonic()
    while time.monotonic() - started < timeout_seconds:
        try:
            result = json.loads(path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            result = None
        if result and result.get('status') in ('PASS', 'FAIL', 'BLOCKED', 'INCOMPLETE', 'CANCELLED'):
            return result
        time.sleep(poll_seconds)
    raise AssertionError('Prior gate did not reach a terminal result before the deadline')


class Scale(base.Stress):
    def __init__(self, args):
        self.ready = False
        for parent in args.member_parent:
            require_storage_filesystem(parent)
        devices = independent_member_devices(args.member_parent)
        super().__init__(args)
        # Keep the legacy setting enabled to prove it cannot silently restore
        # periodic scans. The engine's policy is verified read-triggered repair;
        # explicit repair/GC coexistence is exercised by the recovery matrix.
        self.config['auto_healing']['enabled'] = 'true'
        self.write_config()
        self.latency = defaultdict(Histogram)
        self.batch_timings = defaultdict(lambda: defaultdict(Histogram))
        self.counts = Counter()
        self.peaks = Counter()
        self.by_phase = defaultdict(Counter)
        self.db_lock = threading.Lock()
        self.auth_lock = threading.Lock()
        self.db = sqlite3.connect(self.root / 'acknowledgements.sqlite', check_same_thread=False)
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            PRAGMA cache_size=-8192;
            PRAGMA temp_store=FILE;
            CREATE TABLE acks(object_key TEXT, version_id TEXT, sha256 TEXT,
                              size INTEGER, category TEXT,
                              PRIMARY KEY(object_key, version_id)) WITHOUT ROWID;
            CREATE TABLE seen(object_key TEXT PRIMARY KEY, version_id TEXT) WITHOUT ROWID;
        ''')
        self.ledger = (self.root / 'acknowledgements.jsonl').open('a', buffering=1)
        self.source = self.root / 'source-folder'
        self.source.mkdir()
        if getattr(args, 'flat_folder', False):
            (self.source / 'projects' / 'million').mkdir(parents=True)
        self.last_auth = 0
        try:
            group = Path('/proc/self/cgroup').read_text().strip().split('::')[-1]
            self.cgroup = Path('/sys/fs/cgroup') / group.lstrip('/')
        except OSError:
            self.cgroup = None
        self.disk_stats = {}
        self.initial_free_bytes = {root: shutil.disk_usage(root).free for root in self.roots}
        for root in self.roots:
            device = root.stat().st_dev
            block = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}'
            if block.exists():
                resolved = block.resolve()
                if (resolved/'partition').exists():
                    resolved = resolved.parent
                self.disk_stats[str(root)] = resolved/'stat'
        sequential = getattr(args, 'churn_after_upload', False)
        pure_ingest = getattr(args, 'upload_only', False) or sequential
        self.report.update(target_objects=args.objects, target_churn_mutations=args.churn_mutations,
                           workload='upload-then-churn' if sequential else (
                               'upload-only' if getattr(args, 'upload_only', False) else 'upload-plus-churn'),
                           concurrent_namespace_clients=0 if pure_ingest else min(32, args.probes),
                           physical_block_devices=list(map(str, devices)),
                           source_folder=str(self.source), transport='loopback HTTPS, CA validated',
                           source_layout='flat-folder' if getattr(args, 'flat_folder', False) else 'project-tree',
                           client_bounded_pending_batches=2 * args.batch_clients,
                           batch_size=args.batch_size, batch_clients=args.batch_clients,
                           batch_server_concurrency=args.batch_concurrency,
                           automatic_full_scans_enabled=False, read_triggered_repair_enabled=True,
                           max_filesystem_growth_gib=getattr(args, 'max_filesystem_growth_gib', 0),
                           note='Filesystem cache is not disabled; no Accel/application cache. '
                                'This is not a browser/Console or physical power-cut qualification.')
        self.ready = True
        self.progress()

    def monitor_loop(self):
        last_progress = 0
        while not self.stop_monitor.wait(1):
            if not self.ready:
                continue
            try:
                with self.lock:
                    process = self.process
                self.fault = self.fault or self.storage_problem()
                if time.monotonic() - self.started > self.args.deadline_seconds:
                    self.fault = 'Overall test deadline exceeded; qualification incomplete'
                self.fault = self.fault or storage_budget_problem(self.roots, self.args.free_floor_gib,
                    self.initial_free_bytes, getattr(self.args, 'max_filesystem_growth_gib', 0))
                sample = {'elapsed_seconds': round(time.monotonic()-self.started, 3), 'phase': self.phase}
                if self.cgroup:
                    for name in ('memory.current', 'memory.events', 'memory.pressure', 'io.pressure'):
                        path = self.cgroup/name
                        if path.exists():
                            sample[name] = path.read_text().strip()
                sample['member_device_aggregate_stats'] = {
                    root: list(map(int, path.read_text().split())) for root, path in self.disk_stats.items() if path.exists()
                }
                for label, pid in [('runner', os.getpid()), ('engine', getattr(process, 'pid', None))]:
                    if pid is None:
                        continue
                    usage = process_usage(pid)
                    if 'unavailable' in usage:
                        sample[label + '_telemetry_unavailable'] = usage['unavailable']
                    for metric in ('rss_kib', 'fds'):
                        if metric not in usage:
                            continue
                        value = usage[metric]
                        sample[label + '_' + metric] = value
                        with self.lock:
                            self.peaks[label+'_'+metric] = max(self.peaks[label+'_'+metric], value)
                            self.by_phase[self.phase][label+'_'+metric] = max(self.by_phase[self.phase][label+'_'+metric], value)
                    if usage.get('rss_kib', 0) > (2*1024*1024 if label == 'engine' else 768*1024):
                        self.fault = f'{label} RSS safety ceiling exceeded; inspect resource samples'
                record_unexpected_engine_exit(self, process)
                with (self.root / 'resources.jsonl').open('a') as stream:
                    stream.write(json.dumps(sample) + '\n')
                if time.monotonic() - last_progress > 10:
                    self.progress()
                    last_progress = time.monotonic()
                if self.fault:
                    with self.lock:
                        current = self.process
                    if current is not None and current.poll() is None:
                        current.kill()
                    return
            except OSError as error:
                self.fault = f'Fixture storage I/O failure: {error}'
                with self.lock:
                    current = self.process
                if current is not None and current.poll() is None:
                    current.kill()
                return

    def progress(self):
        with self.lock:
            self.report.update(current_phase=self.phase, counts=dict(self.counts),
                               elapsed_seconds=round(time.monotonic()-self.started, 3),
                               peak_resources=dict(self.peaks), fault=self.fault,
                               batch_client_timings_ms={phase: {name: values.summary()
                                   for name, values in timings.items()}
                                   for phase, timings in self.batch_timings.items()})
            encoded = json.dumps(self.report, indent=2)
        temporary = self.root / 'progress.json.tmp'
        # Serialise writers (main + monitor); never fall back to the OS disk.
        with self.auth_lock:
            temporary.write_text(encoded)
            temporary.replace(self.root / 'progress.json')

    def phase_run(self, name, function):
        self.phase = name
        self.progress()
        print('START', name, flush=True)
        started = time.monotonic()
        try:
            details = function() or {}
        except BaseException:
            self.phases.append({'name': name, 'status': 'FAIL',
                                'elapsed_seconds': round(time.monotonic()-started, 3)})
            raise
        with self.lock:
            self.phases.append({'name': name, 'status': 'PASS',
                'elapsed_seconds': round(time.monotonic()-started, 3),
                'http_statuses': dict(self.calls[name]),
                'request_latency_histogram': self.latency[name].summary(),
                'batch_client_timings_ms': {component: values.summary()
                    for component, values in self.batch_timings.get(name, {}).items()}, **details})
        self.progress()
        print('DONE', json.dumps(self.phases[-1]), flush=True)

    def authenticate(self):
        if getattr(self, 'token', None) and time.monotonic() - self.last_auth < 600:
            return
        with self.auth_lock:
            if getattr(self, 'token', None) and time.monotonic() - self.last_auth < 600:
                return
            reply = self.api('POST', '/auth/fragment', {
                'fragment': self.fragment, 'secret': self.fragment_key, 'read': True,
                'write': True, 'modify': True, 'delete': True, 'expires_in': 21600}, token='')
            self.token = reply['access_token']
            self.last_auth = time.monotonic()

    def record_acks(self, records):
        with self.db_lock:
            with self.db:
                self.db.executemany('INSERT INTO acks VALUES(?,?,?,?,?)', records)
            for key, version, sha, size, category in records:
                self.ledger.write(json.dumps({'object_key': key, 'version_id': version,
                    'sha256': sha, 'size': size, 'category': category}) + '\n')
            with self.lock:
                self.counts['acknowledged_versions'] += len(records)
                self.counts['acknowledged_bytes'] += sum(row[3] for row in records)
                self.counts['source_objects'] += sum(row[4] == 'source' for row in records)

    def ack_rows(self, category=None):
        reader = sqlite3.connect(self.root / 'acknowledgements.sqlite')
        reader.execute('PRAGMA cache_size=-4096')
        try:
            cursor = reader.execute('SELECT object_key,version_id,sha256,size,category FROM acks' +
                                    (' WHERE category=?' if category else ''), (category,) if category else ())
            while rows := cursor.fetchmany(128):
                yield from rows
        finally:
            reader.close()

    def upload_one(self, item):
        key, seed, size, category = item
        self.authenticate()
        data = base.payload(seed, size)
        status, body = self.call('POST', '/upload/one-shot/' + urllib.parse.quote(key, safe='/') + '?durability=strict', data)
        assert status == 201, f'One-shot {key}: HTTP {status}: {body[:300]!r}'
        reply = json.loads(body)
        assert reply['object_key'] == key and reply['version_id']
        self.record_acks([(key, reply['version_id'], base.digest(data), size, category)])

    def batch_object_key(self, index):
        return source_key(index, getattr(self.args, 'flat_folder', False))

    def record_batch_timing(self, phase, component, started):
        elapsed_ms = (time.monotonic() - started) * 1000
        with self.lock:
            self.batch_timings[phase][component].append(elapsed_ms)

    def upload_batch(self, interval):
        self.authenticate()
        problem = self.storage_problem()
        assert not problem, problem
        start, end = interval
        phase = self.phase
        prepared_at = time.monotonic()
        boundary = 'N1Scale' + secrets.token_hex(16)
        pieces, expected = [], {}
        try:
            for index in range(start, end):
                key, size = self.batch_object_key(index), source_size(index)
                data = base.payload(key, size)
                path = self.source / key
                if not getattr(self.args, 'flat_folder', False):
                    path.parent.mkdir(parents=True, exist_ok=True)
                with path.open('xb') as stream:
                    stream.write(data)
                # Actual source files, not a million in-memory browser metadata mocks.
                data = path.read_bytes()
                expected[key] = (base.digest(data), len(data))
                header = (f'--{boundary}\r\nContent-Disposition: form-data; name="object"; '
                          f'filename="{urllib.parse.quote(key, safe="")}"\r\n'
                          'Content-Type: application/octet-stream\r\n\r\n')
                pieces.extend([header.encode(), data, b'\r\n'])
            pieces.append(f'--{boundary}--\r\n'.encode())
            request_body = b''.join(pieces)
        finally:
            self.record_batch_timing(phase, 'source_preparation', prepared_at)
        requested_at = time.monotonic()
        try:
            try:
                status, body = self.call('POST', '/upload/batch?durability=strict&concurrency=' + str(self.args.batch_concurrency),
                    request_body, headers={'Content-Type': 'multipart/form-data; boundary=' + boundary})
            finally:
                self.record_batch_timing(phase, 'http_request', requested_at)
        except (OSError, http.client.HTTPException) as error:
            with self.lock:
                self.counts['transport_ambiguous_batches'] += 1
            with self.db_lock:
                with (self.root / 'ambiguous-batches.jsonl').open('a') as stream:
                    stream.write(json.dumps({'start': start, 'end': end,
                        'failed_at_unix': time.time(), 'elapsed_seconds': time.monotonic() - self.started,
                        'error': type(error).__name__, 'message': str(error)}) + '\n')
            raise
        assert status in (201, 207), f'Batch HTTP {status}: {body[:400]!r}'
        reply = json.loads(body)
        records, seen, failures = [], set(), []
        for result in reply['results']:
            key = result['object_key']
            assert key in expected and key not in seen, f'Wrong/duplicate batch result: {key}'
            seen.add(key)
            if result['status'] == 201:
                sha, size = expected[key]
                assert result['version_id'], result
                records.append((key, result['version_id'], sha, size, 'source'))
            else:
                failures.append(result)
        acknowledged_at = time.monotonic()
        try:
            self.record_acks(records)  # Preserve partial success before reporting any refusal.
        finally:
            self.record_batch_timing(phase, 'acknowledgement_ledger', acknowledged_at)
        assert len(seen) == len(expected), 'Batch response omitted source files'
        if failures:
            self.capture_batch_failure(interval, failures)
        if failures and getattr(self.args, 'accept_admission_refusals', False):
            assert all(result['status'] == 503 for result in failures), failures
            with self.lock:
                self.counts['admission_refusals'] += len(failures)
                if records:
                    self.counts['partial_batches_reported'] += 1
        else:
            assert not failures, f'Batch refused {len(failures)} objects: {failures[:3]}'
        assert reply['succeeded'] == len(records) and reply['failed'] == len(failures), reply

    def capture_batch_failure(self, interval, failures):
        """Best-effort, bounded diagnostics; never retry or mask a refused write.

        Keep safety sampling independent of these API reads. A fresh connection
        has a two-second socket deadline, bounded bodies, and no refresh/deep
        scan. Successful benchmark requests incur none of this extra work.
        """
        evidence = dict(elapsed_seconds=round(time.monotonic()-self.started, 3),
                        interval=list(interval), refused_objects=len(failures),
                        failure_samples=failures[:3], automatic_write_retries=False)
        connection = None
        try:
            connection = http.client.HTTPSConnection('127.0.0.1', self.args.port,
                                                     context=self.tls, timeout=2)
            headers = {'Authorization': 'Bearer ' + self.token}
            for label, route in (('health', '/maintenance/health'),
                                 ('gc', '/maintenance/gc/status')):
                connection.request('GET', '/noa/v1' + route, headers=headers)
                response = connection.getresponse()
                body = response.read(1024 * 1024 + 1)
                assert response.status == 200 and len(body) <= 1024 * 1024, (
                    f'{label} diagnostic HTTP {response.status} or oversized response')
                data = json.loads(body)
                if label == 'health':
                    assert data['io_control']['process']['pid'] == self.process.pid, (
                        'Diagnostic response belongs to another engine')
                    evidence[label] = {key: data[key] for key in
                                       ('system', 'io_scheduler', 'io_control') if key in data}
                else:
                    evidence[label] = data
        except Exception as error:
            evidence['diagnostic_error'] = f'{type(error).__name__}: {error}'
        finally:
            if connection is not None:
                try:
                    connection.close()
                except OSError as error:
                    evidence.setdefault('diagnostic_error', f'{type(error).__name__}: {error}')
        try:
            path = self.root / ('batch-failure-' + secrets.token_hex(8) + '.json')
            with path.open('x') as stream:
                json.dump(evidence, stream, indent=2)
        except Exception as error:
            print(f'Unable to retain batch-failure diagnostics: {error}', file=sys.stderr, flush=True)

    def verify_row(self, row):
        key, version, expected, size, _ = row
        self.authenticate()
        status, data = self.call('GET', '/objects/download?' + urllib.parse.urlencode({'object_key': key, 'version_id': version}))
        assert status == 200 and len(data) == size and base.digest(data) == expected, f'ACK DATA MISMATCH {key}@{version}: HTTP {status}'
        with self.lock:
            self.counts['historical_downloads_verified'] += 1

    def verify_all(self):
        verified = sum(1 for _ in bounded_map(self.verify_row, self.ack_rows(), 32))
        return {'every_acknowledged_version_downloaded_and_hashed': verified}

    def resource_checkpoint(self):
        # The engine deliberately restricts external /proc/<pid>/fd access.
        # Ask its existing API for self-observed descriptors at quiescent
        # checkpoints; do not weaken that security or invent zero samples.
        self.authenticate()
        health = self.api('GET', '/maintenance/health?' + urllib.parse.urlencode(
            {'refresh': 'true', 'fragment': self.fragment}))
        backend = base.verify_io_backend(health, self.args)
        usage = health['io_control']['process']
        assert usage['pid'] == self.process.pid, 'Resource sample belongs to another engine'
        assert isinstance(usage['open_fds'], int) and 0 < usage['open_fds'] <= 2048, usage
        assert 0 < usage['rss_bytes'] <= 2 * 1024**3, usage
        healer = health['maintenance']['auto_healing']
        assert healer['last_availability_scan_at'] is None and healer['last_checksum_scrub_at'] is None, (
            'An unsolicited full-object scan ran during the scale workload')
        latency, scheduler = health['io_control']['latency'], health['io_scheduler']
        gc = self.api('GET', '/maintenance/gc/status')
        assert isinstance(gc, dict) and isinstance(gc.get('is_running'), bool), 'Invalid GC telemetry'
        sample = dict(phase=self.phase, elapsed_seconds=time.monotonic()-self.started,
                      io_latency=latency, io_scheduler=scheduler, io_backend=backend,
                      gc=gc, **usage)
        with (self.root / 'engine-resource-checkpoints.jsonl').open('a') as stream:
            stream.write(json.dumps(sample) + '\n')
        return {'engine_reported_resources': usage, 'quiescent_fd_budget': 2048,
                'no_automatic_object_scans': True, 'engine_io_latency': latency,
                'engine_io_scheduler': scheduler, 'engine_io_backend': backend,
                'engine_gc_status': gc}

    def prefix_keys(self, prefix, limit=73):
        """Stream zero-decode object-prefix pages with O(page) client memory."""
        self.authenticate()
        cursor, previous = None, None
        while True:
            query = {'prefix': prefix, 'delimiter': '', 'limit': limit}
            if cursor is not None:
                query['continuation_token'] = cursor
            page = self.api('GET', '/objects?' + urllib.parse.urlencode(query))
            entries = page['entries']
            assert len(entries) <= limit, 'Oversized object-prefix LIST page'
            for entry in entries:
                assert entry['kind'] == 'file', 'Unexpected virtual directory in flat prefix LIST'
                key = prefix + entry['record']
                assert previous is None or key > previous, 'Duplicate or non-progressing LIST key'
                previous = key
                yield key
            next_cursor = page.get('continuation_token')
            if next_cursor is None:
                break
            assert entries and next_cursor == previous, 'LIST cursor skipped the last returned key'
            assert cursor is None or next_cursor > cursor, 'Non-progressing object-prefix LIST cursor'
            cursor = next_cursor

    def listing_probe(self, count):
        samples = []
        flat_folder = getattr(self.args, 'flat_folder', False)
        for prefix, start, end in listing_prefixes(count, flat_folder):
            expected = {self.batch_object_key(i) for i in range(start, end)}
            started = time.monotonic()
            if flat_folder:
                matched = 0
                for key in self.prefix_keys(prefix, 73):
                    assert key in expected, f'Unexpected paginated LIST key under {prefix}: {key}'
                    expected.remove(key)
                    matched += 1
                assert not expected, f'Missing paginated LIST keys under {prefix}'
            else:
                actual = set(self.list_all(prefix, 73))
                assert actual == expected, f'Paginated LIST mismatch under {prefix}'
                matched = len(actual)
            samples.append({'path': prefix, 'objects': matched, 'elapsed_seconds': round(time.monotonic()-started, 3)})
        return {'population': count, 'sampled_prefixes_all_pages': samples,
                'scope': 'lexical filename prefixes within one flat folder' if flat_folder else 'project subdirectories',
                'full_inventory': 'separate streamed namespace snapshot compared with SQLite ledger',
                'warning': 'Pagination bounds result memory, not necessarily scan work.'}

    def immutable_probe_inventory(self):
        inventory = {}
        for directory in self.probe_directories:
            for path in directory.rglob('*'):
                if path.is_file():
                    st = path.stat()
                    inventory[str(path)] = (st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, base.digest(path.read_bytes()))
        return inventory

    def setup_probes(self):
        for _ in bounded_map(self.upload_one,
                ((f'churn/probe-{i:04}', f'churn/probe-{i:04}', 4096, 'probe')
                 for i in range(self.args.probes)), 16):
            pass
        versions = {row[1] for row in self.ack_rows('probe')}
        assert len(versions) == self.args.probes, 'Probe acknowledgement inventory is incomplete'
        self.probe_directories = set()
        # After a million-source ingestion, retain only the bounded probe set,
        # never a million-entry physical inventory. This is an explicit setup
        # traversal; neither NDX bodies nor payload bytes are decoded here.
        for root in self.roots:
            found = set()
            for directory in root.glob('@f_*/.objects/*/*/@o_*'):
                if directory.is_symlink() or not directory.is_dir():
                    continue
                for child in directory.iterdir():
                    if child.name in versions and not child.is_symlink() and child.is_dir():
                        found.add(child.name)
                        self.probe_directories.add(directory)
                if found == versions:
                    break
            assert found == versions, f'Probe physical versions were not found on member {root}'
        return {'probe_objects': len(versions), 'immutable_probe_directories': len(self.probe_directories),
                'discovery': 'streamed physical names; only acknowledged probe directories retained'}

    def churn(self, cycles, concurrency):
        before = self.immutable_probe_inventory()
        def actor(index):
            key = f'churn/probe-{index:04}'
            destination = f'moved/probe-{index:04}'
            for _ in range(cycles):
                self.authenticate()
                for route, data in [('/posix/rename', {'source_path': key, 'destination_path': destination}),
                                    ('/posix/trash', {'path': destination}),
                                    ('/posix/recover', {'path': destination}),
                                    ('/posix/rename', {'source_path': destination, 'destination_path': key})]:
                    result = self.api('POST', route, data)
                    assert not result.get('failed_paths') and not result.get('conflicted_paths'), result
                    assert result.get('affected') == 1, (
                        f'Churn must change exactly one object: {route}: {result}')
                    with self.lock:
                        self.counts['namespace_mutations'] += 1
                status, data = self.call('GET', '/objects/download?' + urllib.parse.urlencode({'object_key': key}))
                assert status == 200 and data == base.payload(key, 4096), f'Churn changed payload {key}'
        for _ in bounded_map(actor, range(self.args.probes), concurrency):
            pass
        assert before == self.immutable_probe_inventory(), 'NCX churn modified immutable NDX/payload files'
        return {'actors': self.args.probes, 'maximum_client_concurrency': min(concurrency, self.args.probes), 'cycles_per_actor': cycles,
                'namespace_mutations': self.args.probes * cycles * 4, 'immutable_files_unchanged': len(before)}

    def ingest_only(self, start, target):
        """Identical strict batches, with no mutation clients or probe work."""
        started = time.monotonic()
        before = self.counts['source_objects']
        intervals = ((i, min(target, i+self.args.batch_size))
                     for i in range(start, target, self.args.batch_size))
        for _ in bounded_map(self.upload_batch, intervals, self.args.batch_clients):
            pass
        assert self.counts['source_objects'] - before == target - start
        return {'new_source_objects': target-start, 'source_objects_total': target,
                'concurrent_namespace_clients': 0,
                'objects_per_second_including_source_creation':
                    round((target-start)/(time.monotonic()-started), 3)}

    def ingest_with_churn(self, start, target):
        """Keep 32 namespace clients active alongside all eight batch clients.

        No retries, growing work queue, or result-sized in-memory inventory.
        Churn uses independent probe keys; the source acknowledgement ledger
        remains a complete authority for post-phase hash/list verification.
        """
        done = threading.Event()
        started = time.monotonic()
        rounds = 0
        def mutations():
            nonlocal rounds
            while not done.is_set():
                self.churn(1, 32)
                rounds += 1
        with ThreadPoolExecutor(max_workers=1) as workers:
            mutation = workers.submit(mutations)
            try:
                intervals = ((i, min(target, i+self.args.batch_size))
                             for i in range(start, target, self.args.batch_size))
                for _ in bounded_map(self.upload_batch, intervals, self.args.batch_clients):
                    if mutation.done():
                        mutation.result()  # Do not hide a concurrent mutation failure.
                        raise AssertionError('Concurrent churn stopped before ingestion finished')
            finally:
                done.set()
                mutation.result()
        return {'new_source_objects': target-start, 'source_objects_total': target,
                'concurrent_namespace_clients': min(32, self.args.probes),
                'concurrent_churn_rounds': rounds,
                'objects_per_second_including_source_creation_and_concurrent_churn':
                    round((target-start)/(time.monotonic()-started), 3)}

    def snapshot_verify(self):
        self.authenticate()
        connection = http.client.HTTPSConnection('127.0.0.1', self.args.port, context=self.tls, timeout=120)
        def progress(count, scanned):
            with self.lock:
                self.counts['inventory_entries_received'] = count
                self.counts['inventory_physical_entries_scanned'] = scanned
        progress(0, 0)
        try:
            connection.request('GET', '/noa/v1/namespace/snapshot?progress=true', headers={'Authorization': 'Bearer ' + self.token})
            response = connection.getresponse()
            assert response.status == 200, f'Snapshot HTTP {response.status}'
            inventory = consume_snapshot(response, self.db, progress)
            mismatch = self.db.execute('''SELECT a.object_key FROM
                (SELECT object_key, MAX(version_id) version_id FROM acks GROUP BY object_key) a
                LEFT JOIN seen s USING(object_key) WHERE s.version_id IS NULL OR a.version_id != s.version_id LIMIT 1''').fetchone()
            extra = self.db.execute('SELECT object_key FROM seen EXCEPT SELECT object_key FROM acks LIMIT 1').fetchone()
            assert not mismatch and not extra, f'Quiescent namespace/ledger mismatch: {mismatch}, extra={extra}'
            return {'all_live_names_and_max_ulid_versions_verified': inventory['entries'], 'completion_frame_verified': True,
                    'inventory_stream': inventory,
                    'note': 'Streamed inventory verification; not a replacement for paginated LIST benchmarks.'}
        finally:
            connection.close()

    def run(self):
        self.phase = 'startup'
        self.start('scale-initial')
        self.fragment_key = secrets.token_hex(32)
        self.fragment = self.api('POST', '/fragments', {'name': 'hdd-scale-' + secrets.token_hex(5),
            'key': self.fragment_key, 'secret': self.master}, token='')['fragment_hash']
        self.authenticate()
        upload_only = getattr(self.args, 'upload_only', False)
        sequential = getattr(self.args, 'churn_after_upload', False)
        if not upload_only and not sequential:
            self.phase_run('probe-object-setup', self.setup_probes)
            self.phase_run('ncx-churn-preflight', lambda: self.churn(1, 32))
            self.phase_run('all-probe-hashes-preflight', self.verify_all)
            self.stop()
            self.start('preflight-after-sigkill')
            self.phase_run('all-probe-hashes-after-sigkill', self.verify_all)
        self.phase_run('resources-before-population', self.resource_checkpoint)
        count = 0
        checkpoints = population_checkpoints(self.args.objects)
        for target in checkpoints:
            start = count
            ingest = self.ingest_only if upload_only or sequential else self.ingest_with_churn
            self.phase_run(f'folder-upload-to-{target}', lambda start=start, target=target: ingest(start, target))
            count = target
            self.phase_run(f'pagination-at-{target}', lambda count=count: self.listing_probe(count))
            # Every byte is checked before accepting a scale milestone.
            self.phase_run(f'complete-version-integrity-at-{target}', self.verify_all)
            self.phase_run(f'resources-at-{target}', self.resource_checkpoint)
        self.phase_run('complete-quiescent-namespace-inventory', self.snapshot_verify)
        if sequential:
            assert self.counts['source_objects'] == self.args.objects, 'Finish the source population before hardening'
            assert self.counts['namespace_mutations'] == 0, 'Population stage must not contain mutation clients'
            self.phase_run('post-population-probe-object-setup', self.setup_probes)
            self.phase_run('post-population-probe-integrity', self.verify_all)
        if not upload_only:
            cycles = max(1, math.ceil(self.args.churn_mutations / (self.args.probes * 4 * 4)))
            for concurrency in (8, 32, 128, 256):
                self.phase_run(f'high-churn-c{concurrency}', lambda c=concurrency: self.churn(cycles, c))
                self.phase_run(f'resources-after-churn-c{concurrency}', self.resource_checkpoint)
            self.phase_run('all-version-integrity-after-churn', self.verify_all)
        self.stop()
        self.start('million-scale-after-sigkill')
        self.phase_run('all-version-integrity-after-final-sigkill', self.verify_all)
        self.phase_run('quiescent-namespace-after-final-sigkill', self.snapshot_verify)
        self.phase_run('resources-after-final-sigkill', self.resource_checkpoint)
        assert self.counts['source_objects'] == self.args.objects
        if upload_only:
            assert self.counts['namespace_mutations'] == 0
        else:
            assert self.counts['namespace_mutations'] >= self.args.churn_mutations
        assert not self.fault, self.fault
        self.report['status'] = 'PASS'

    def finish(self):
        original_error = sys.exc_info()[1]
        process = self.process
        cleanup_error = None

        def record_cleanup_error(stage, error):
            nonlocal cleanup_error
            cleanup_error = cleanup_error or error
            self.report['status'] = 'FAIL'
            self.report.setdefault('error', f'{type(error).__name__}: {error}')
            self.report.setdefault('cleanup_errors', []).append(dict(
                stage=stage, error=f'{type(error).__name__}: {error}',
                owned_engine_pid=getattr(process, 'pid', None),
                wait_timeout_seconds=getattr(error, 'timeout', None),
                process_exit_not_confirmed=stage == 'stop-owned-engine'))

        self.stop_monitor.set()
        try:
            self.monitor.join(timeout=3)
            self.stop()
        except Exception as error:
            record_cleanup_error('stop-owned-engine', error)
        try:
            if self.cgroup:
                for name in ('memory.current', 'memory.events', 'memory.pressure', 'io.pressure'):
                    path = self.cgroup/name
                    if path.exists():
                        self.report['final_' + name] = path.read_text().strip()
        except Exception as error:
            record_cleanup_error('read-final-resources', error)
        try:
            self.ledger.flush()
            os.fsync(self.ledger.fileno())
        except Exception as error:
            record_cleanup_error('flush-acknowledgement-ledger', error)
        finally:
            try:
                self.ledger.close()
            except Exception as error:
                record_cleanup_error('close-acknowledgement-ledger', error)
        with self.db_lock:
            try:
                self.db.commit()
                self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            except Exception as error:
                record_cleanup_error('checkpoint-acknowledgement-database', error)
            finally:
                try:
                    self.db.close()
                except Exception as error:
                    record_cleanup_error('close-acknowledgement-database', error)
        self.report['phase_peak_resources'] = {key: dict(value) for key, value in self.by_phase.items()}
        try:
            self.progress()
        except Exception as error:
            record_cleanup_error('write-progress', error)
        try:
            (self.root/'results.json').write_text(json.dumps(self.report, indent=2))
        except Exception as error:
            record_cleanup_error('write-results', error)
            print(f'Scale evidence finalization failed: {error}', file=sys.stderr, flush=True)
        print(json.dumps(self.report, indent=2), flush=True)
        if cleanup_error is not None and original_error is None:
            raise cleanup_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--io-backend', choices=('auto', 'uring', 'blocking'), default='auto')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--data-shards', type=int, default=1)
    parser.add_argument('--parity-shards', type=int, default=1)
    parser.add_argument('--objects', type=int, default=1000000)
    parser.add_argument('--flat-folder', action='store_true',
                        help='Place all source objects in projects/million/; LIST samples lexical prefixes and snapshot verifies every name')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--upload-only', action='store_true', help='No probes or churn; requires --probes 0 --churn-mutations 0')
    modes.add_argument('--churn-after-upload', action='store_true',
                       help='No probes or mutations during population; harden the same complete population afterwards')
    parser.add_argument('--churn-mutations', type=int, default=1000000)
    parser.add_argument('--probes', type=int, default=1000)
    parser.add_argument('--batch-size', type=int, default=64)
    parser.add_argument('--batch-clients', type=int, default=8)
    parser.add_argument('--batch-concurrency', type=int, default=16)
    parser.add_argument('--port', type=int, default=56841)
    parser.add_argument('--deadline-seconds', type=int, default=172800)
    parser.add_argument('--free-floor-gib', type=int, default=128)
    parser.add_argument('--max-filesystem-growth-gib', type=int, default=0,
                        help='Stop on this much filesystem growth, including other writers; zero disables the extra guard')
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--wait-for', type=Path, help='Prior gate results.json, which must say PASS')
    args = parser.parse_args()
    assert args.objects >= 1 and args.churn_mutations >= 0 and args.probes >= 0
    if args.upload_only:
        assert args.probes == 0 and args.churn_mutations == 0, 'Upload-only requires --probes 0 --churn-mutations 0'
    else:
        assert args.probes >= 1
    assert 1 <= args.batch_size <= 256 and 1 <= args.batch_clients <= 32 and 1 <= args.batch_concurrency <= 128
    validate_topology(args)
    assert args.max_filesystem_growth_gib >= 0
    args.binary = args.binary.resolve(strict=True)
    if args.wait_for:
        print('WAITING FOR', str(args.wait_for), flush=True)
        previous = wait_for_result(args.wait_for)
        if previous.get('status') != 'PASS':
            assert not args.root.exists(), 'Never overwrite an existing fixture'
            args.root.mkdir(mode=0o700)
            (args.root / 'results.json').write_text(json.dumps({'status': 'BLOCKED',
                'error': 'Prior gate failed; investigate before escalating load', 'prior_gate': str(args.wait_for)}, indent=2))
            raise SystemExit('Prior gate failed; scale stage BLOCKED, not PASS')
    os.umask(0o077)
    test = Scale(args)
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Operator/systemd signal {signum}')
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
