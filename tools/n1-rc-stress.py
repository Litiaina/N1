#!/usr/bin/env python3
"""Isolated Linux RC stress gate. Never connects to or stops a live service.

Creates a NEW child directory, starts only the supplied engine binary, uses
strict uploads, hashes every acknowledgement after SIGKILL/restart, and records
overload refusals rather than hiding them with automatic write retries.
Requires one distinct filesystem per member; never bypasses pool enrollment.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import configparser
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import subprocess
import threading
import time
import urllib.parse

if not __debug__:
    raise RuntimeError('Run without -O: fixture safety assertions must remain enabled')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def payload(seed, size=16384):
    block = hashlib.sha256(seed.encode()).digest()
    return (block * math.ceil(size / len(block)))[:size]


def distribution(values):
    values = sorted(values)
    return {name: round(values[min(len(values)-1, math.ceil(len(values)*q)-1)], 3)
            for name, q in [('median_ms', .5), ('p95_ms', .95), ('p99_ms', .99), ('max_ms', 1)]} if values else {}


def fixture_certificate_days(deadline_seconds):
    # Trust must outlive the complete run, including fresh TLS connections at
    # later checkpoints. Never turn certificate validation off for a long soak.
    return max(2, math.ceil(deadline_seconds / 86400) + 1)


def requested_io_backend(args):
    mode = getattr(args, 'io_backend', 'auto')
    assert mode in ('auto', 'uring', 'blocking'), 'Invalid requested I/O backend'
    return mode


def verify_io_backend(health, args):
    mode = requested_io_backend(args)
    backend = health['io_control'].get('backend')
    assert isinstance(backend, dict), 'Engine did not report its I/O backend'
    assert backend.get('requested') == mode, f'Engine ignored requested I/O backend: {backend}'
    selected = backend.get('backend')
    if mode == 'uring':
        assert selected == 'io_uring', f'Required io_uring was not selected: {backend}'
    elif mode == 'blocking':
        assert selected == 'blocking', f'Required blocking backend was not selected: {backend}'
    else:
        assert selected in ('io_uring', 'blocking'), f'Unavailable I/O backend: {backend}'
    if mode == 'auto' and selected == 'blocking':
        assert isinstance(backend.get('fallback_reason'), str) and backend['fallback_reason'], (
            f'Automatic fallback must report its initialization reason: {backend}')
    else:
        assert backend.get('fallback_reason') is None, f'Unexpected I/O fallback: {backend}'
    return backend


class Stress:
    def __init__(self, args):
        self.io_backend = requested_io_backend(args)
        self.args = args
        self.root = args.root.resolve()
        assert not self.root.exists(), 'Use a new, nonexistent fixture directory'
        assert self.root.parent.is_dir()
        assert shutil.disk_usage(self.root.parent).free > 12 * 1024**3
        parents = [parent.resolve(strict=True) for parent in args.member_parent]
        assert 1 <= args.data_shards <= 32 and 0 <= args.parity_shards <= 32
        assert len(parents) == args.data_shards + args.parity_shards
        assert len({p.stat().st_dev for p in parents}) == len(parents), 'Members need distinct filesystems'
        assert all(shutil.disk_usage(p).free > 12*1024**3 for p in parents)
        for parent in [self.root.parent]+parents:
            assert not any((p/'.n1-pool.json').exists() for p in [parent]+list(parent.parents)), 'Never place tests inside an enrolled storage root'
        nonce = secrets.token_hex(5)
        roots = [p / f'n1-rc-stress-{nonce}-member-{i}' for i, p in enumerate(parents)]
        assert not any(p.exists() for p in roots)
        for port in (args.port, args.port+1):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
        self.root.mkdir(mode=0o700)
        (self.root/'control').mkdir()
        self.roots = roots
        for root in self.roots:
            root.mkdir(mode=0o700)
        # A removable member must never silently become a directory on the OS
        # filesystem after unplug/reconnect. Stop only our disposable engine.
        self.storage_devices = {path: path.stat().st_dev for path in [self.root] + self.roots}
        self.config = configparser.ConfigParser(interpolation=None)
        self.config.read(Path(__file__).resolve().parents[1] / 'shell/web/n1_config.default.ini')
        self.config['server'].update(host='127.0.0.1', https_port=str(args.port), native_port=str(args.port+1),
                                     cert_path=str(self.root/'cert.pem'), key_path=str(self.root/'key.pem'))
        self.config['disk_config']['fs_storage_path'] = str(self.root/'control')
        self.config['storage_pool.A'].update(roots=','.join(map(str, self.roots)), data_shards=str(args.data_shards), parity_shards=str(args.parity_shards))
        # Separate recovery proof exercises healing. This isolates foreground overload.
        self.config['auto_healing']['enabled'] = 'false'
        self.config['auto_healing']['topology_recovery_stability_seconds'] = '0'
        self.config['upload_config']['upload_part_strict_durability_default'] = 'true'
        self.config['rate_limiter_config']['enable_rate_limiting'] = 'false'
        self.write_config()
        self.master = secrets.token_hex(32)
        (self.root/'n1.env').write_text(f'MASTER_KEY={self.master}\nAUTH_KEY={secrets.token_hex(32)}\nADMIN_KEY={secrets.token_hex(32)}\nJWT_SECRET={secrets.token_hex(32)}\nN1_IO_BACKEND={self.io_backend}\n')
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1',
                        '-nodes', '-days', str(fixture_certificate_days(args.deadline_seconds)),
                        '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1',
                        '-keyout', str(self.root/'key.pem'), '-out', str(self.root/'cert.pem')],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.tls = ssl.create_default_context(cafile=str(self.root/'cert.pem'))
        self.process = None
        self.generation = 0
        self.local = threading.local()
        self.lock = threading.Lock()
        self.stop_monitor = threading.Event()
        self.fault = None
        self.phase = 'setup'
        self.samples = []
        self.phases = []
        self.calls = defaultdict(Counter)
        self.latency = defaultdict(list)
        self.active = 0
        self.peak_active = 0
        self.acknowledged = []
        self.ambiguous = []
        self.started = time.monotonic()
        self.report = {'status': 'RUNNING', 'engine_sha256': digest(args.binary.read_bytes()),
                       'requested_io_backend': self.io_backend,
                       'scope': 'isolated loopback HTTPS; distinct member filesystems; no physical power-cut qualification',
                       'topology': {'data_shards': args.data_shards, 'parity_shards': args.parity_shards, 'roots': list(map(str, self.roots))},
                       'strict_uploads': True, 'automatic_write_retries': False, 'phases': self.phases}
        self.monitor = threading.Thread(target=self.monitor_loop, daemon=True)
        self.monitor.start()

    def write_config(self):
        with (self.root/'n1_config.ini').open('w') as stream:
            self.config.write(stream)

    def monitor_loop(self):
        while not self.stop_monitor.wait(.5):
            process = self.process
            if process is None or process.poll() is not None:
                continue
            problem = self.storage_problem()
            if problem:
                self.fault = problem
                process.kill()
                return
            try:
                status = dict(line.split(':', 1) for line in Path(f'/proc/{process.pid}/status').read_text().splitlines() if ':' in line)
                sample = {'seconds': round(time.monotonic()-self.started, 3), 'phase': self.phase,
                          'rss_kib': int(status['VmRSS'].split()[0]), 'hwm_kib': int(status['VmHWM'].split()[0]),
                          'fds': len(list(Path(f'/proc/{process.pid}/fd').iterdir()))}
                self.samples.append(sample)
                if sample['rss_kib'] > 2*1024*1024 or sample['fds'] > 12000:
                    self.fault = 'Resource safety ceiling exceeded (2 GiB RSS / 12000 FD)'
                if any(shutil.disk_usage(p).free < 8*1024**3 for p in [self.root]+self.roots):
                    self.fault = 'Free-space safety floor crossed (8 GiB)'
                if time.monotonic()-self.started > self.args.deadline_seconds:
                    self.fault = 'Overall stress deadline exceeded'
                if self.fault:
                    process.kill()
                    return
            except (FileNotFoundError, ProcessLookupError, KeyError):
                pass

    def storage_problem(self):
        for path, device in self.storage_devices.items():
            try:
                if path.stat().st_dev != device:
                    return f'Test filesystem changed or unmounted: {path}'
            except OSError as error:
                return f'Test filesystem unavailable: {path}: {error}'
        return None

    def start(self, label):
        assert not self.storage_problem(), self.storage_problem()
        log = (self.root/f'engine-{label}.log').open('wb')
        environment = {key: value for key, value in os.environ.items() if not key.startswith('N1_')}
        environment.update(RUST_LOG='info', RUST_BACKTRACE='1', N1_REFRESH_STORE_ROOT=str(self.root/'refresh'))
        process = subprocess.Popen([str(self.args.binary)], cwd=self.root, env=environment, stdout=log, stderr=log)
        log.close()
        with self.lock:
            self.process = process
            self.generation += 1
        deadline = time.monotonic()+45
        while time.monotonic() < deadline:
            assert self.process.poll() is None, 'Owned engine exited during startup'
            try:
                if self.call('GET', '/ping', token='', record=False)[0] == 200:
                    return
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(.2)
        raise AssertionError('Owned engine startup timeout')

    def stop(self):
        # The sampler must not mistake our intentional SIGKILL/restart for an
        # unexpected engine exit while wait() reaps this owned child.
        with self.lock:
            process = self.process
            self.process = None
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=15)

    def call(self, method, route, data=None, token=None, record=True, headers=None):
        if self.fault:
            raise AssertionError(self.fault)
        connection, generation = getattr(self.local, 'connection', (None, -1))
        if connection is None or generation != self.generation:
            if connection:
                connection.close()
            connection = http.client.HTTPSConnection('127.0.0.1', self.args.port, context=self.tls,
                                                     timeout=getattr(self.args, 'request_timeout_seconds', 90))
            self.local.connection = (connection, self.generation)
        headers = dict(headers or {})
        if token is None:
            token = self.token
        if token:
            headers['Authorization'] = 'Bearer '+token
        if isinstance(data, (dict, list)):
            data = json.dumps(data).encode()
            headers['Content-Type'] = 'application/json'
        elif data is not None:
            headers.setdefault('Content-Type', 'application/octet-stream')
        started = time.monotonic()
        phase = self.phase
        with self.lock:
            self.active += 1
            self.peak_active = max(self.active, self.peak_active)
        status = 'transport_error'
        try:
            connection.request(method, '/noa/v1'+route, data, headers)
            response = connection.getresponse()
            status = response.status
            body = response.read()
            return status, body
        except (OSError, http.client.HTTPException):
            connection.close()
            self.local.connection = (None, -1)
            raise
        finally:
            with self.lock:
                self.active -= 1
                if record:
                    self.calls[phase][str(status)] += 1
                    self.latency[phase].append((time.monotonic()-started)*1000)

    def api(self, method, route, data=None, **options):
        status, body = self.call(method, route, data, **options)
        assert 200 <= status < 300, f'{method} {route}: HTTP {status}: {body[:350]!r}'
        return json.loads(body) if body else None

    def create(self, name):
        key = secrets.token_hex(32)
        fragment = self.api('POST', '/fragments', {'name': name, 'key': key, 'secret': self.master}, token='')['fragment_hash']
        token = self.api('POST', '/auth/fragment', {'fragment': fragment, 'secret': key, 'read': True, 'write': True,
                                                 'modify': True, 'delete': True, 'expires_in': 21600}, token='')['access_token']
        return fragment, token

    def upload(self, item):
        token, key, seed, size = item
        body = payload(seed, size)
        try:
            status, response = self.call('POST', '/upload/one-shot/'+urllib.parse.quote(key, safe='/')+'?durability=strict', body, token=token)
        except (OSError, http.client.HTTPException) as error:
            with self.lock:
                self.ambiguous.append((item, type(error).__name__))
            return 'transport_error'
        if status == 201:
            result = json.loads(response)
            assert result['object_key'] == key and result['version_id']
            with self.lock:
                self.acknowledged.append((token, key, result['version_id'], digest(body), len(body)))
        else:
            assert status in (429, 503), f'Unexpected write error {status}: {response[:350]!r}'
        return status

    def parallel(self, function, items, concurrency):
        with ThreadPoolExecutor(max_workers=concurrency) as workers:
            return list(workers.map(function, items))

    def phase_run(self, name, function):
        self.phase = name
        print('START', name, flush=True)
        started = time.monotonic()
        details = function() or {}
        report = {'name': name, 'elapsed_seconds': round(time.monotonic()-started, 3),
                  'http_statuses': dict(self.calls[name]), 'latency': distribution(self.latency[name]), **details}
        self.phases.append(report)
        (self.root/'progress.json').write_text(json.dumps(self.report, indent=2))
        print('DONE', json.dumps(report), flush=True)

    def versions(self, key, token=None):
        return self.api('GET', '/objects/versions/'+urllib.parse.quote(key, safe='/'), token=token)

    def read_ack(self, ack):
        token, key, version, expected, size = ack
        status, body = self.call('GET', '/objects/download?'+urllib.parse.urlencode({'object_key': key, 'version_id': version}), token=token)
        assert status == 200 and len(body) == size and digest(body) == expected, f'ACKNOWLEDGED DATA MISMATCH {key}@{version}'

    def list_all(self, path, limit=73, token=None):
        keys, seen, cursor = [], set(), None
        while True:
            query = {'path': path, 'limit': limit}
            if cursor:
                query['continuation_token'] = cursor
            page = self.api('GET', '/posix/list?'+urllib.parse.urlencode(query), token=token)
            keys.extend(entry['path'] for entry in page['entries'])
            cursor = page.get('continuation_token')
            if not cursor:
                break
            assert cursor not in seen, 'Non-progressing LIST cursor'
            seen.add(cursor)
        assert len(keys) == len(set(keys)), 'Duplicate LIST keys in quiescent namespace'
        return keys

    def immutable_inventory(self):
        records = {}
        for root in self.roots:
            for objects in root.glob('@f_*/.objects'):
                for path in objects.rglob('*'):
                    if path.is_file():
                        stat = path.stat()
                        records[f'{self.roots.index(root)}/{path.relative_to(root)}'] = (
                            stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                            digest(path.read_bytes()))
        assert records, 'No immutable files found to verify'
        return records

    def run(self):
        self.start('initial')
        self.fragment, self.token = self.create('rc-stress-'+secrets.token_hex(5))
        self.fragments = [self.create('rc-isolation-'+secrets.token_hex(5)) for _ in range(3)]
        for concurrency in (16, 64, 128, 256, 512):
            def ramp(concurrency=concurrency):
                items = [(self.token, f'ramp-{concurrency}/item-{i:05}.bin', f'ramp-{concurrency}-{i}', 16384) for i in range(concurrency*2)]
                started = time.monotonic()
                outcomes = self.parallel(self.upload, items, concurrency)
                return {'offered_requests': len(items), 'concurrency': concurrency,
                        'accepted': outcomes.count(201), 'objects_per_second': round(outcomes.count(201)/(time.monotonic()-started), 3)}
            self.phase_run(f'upload_ramp_{concurrency}', ramp)

        def shared():
            outcomes = self.parallel(self.upload, [(self.token, 'shared.bin', f'contention-{i}', 32768) for i in range(256)], 128)
            versions = self.versions('shared.bin')
            acknowledged = [ack for ack in self.acknowledged if ack[1] == 'shared.bin']
            assert {v['version_id'] for v in versions} == {ack[2] for ack in acknowledged}
            self.parallel(self.read_ack, acknowledged, 32)
            newest = max(acknowledged, key=lambda ack: ack[2])
            status, body = self.call('GET', '/objects/download?object_key=shared.bin')
            assert status == 200 and digest(body) == newest[3], 'Latest is not maximum committed ULID'
            return {'offered_versions': len(outcomes), 'committed_versions': len(versions), 'every_historical_hash_verified': True}
        self.phase_run('same_key_256_publishers', shared)

        def mixed():
            def actor(index):
                token = ([self.token]+[t for _, t in self.fragments])[index % 4]
                for item in range(6):
                    key = f'actor-{index:03}/item-{item:02}'
                    outcome = self.upload((token, key, key, 8192))
                    assert outcome == 201, f'Moderate mutation setup failed: {outcome}'
                    destination = key+'-moved'
                    self.api('POST', '/posix/rename', {'source_path': key, 'destination_path': destination}, token=token)
                    self.api('POST', '/posix/trash', {'path': destination}, token=token)
                    self.api('POST', '/posix/recover', {'path': destination}, token=token)
                    self.api('POST', '/posix/rename', {'source_path': destination, 'destination_path': key}, token=token)
                    status, body = self.call('GET', '/objects/download?'+urllib.parse.urlencode({'object_key': key}), token=token)
                    assert status == 200 and body == payload(key, 8192)
                    self.list_all(f'actor-{index:03}', 3, token)
                return index
            self.parallel(actor, range(32), 32)
            return {'actors': 32, 'fragments': 4, 'objects': 192, 'namespace_mutations': 768}
        self.phase_run('mixed_mutation_and_fragment_isolation', mixed)

        def mutations():
            before = self.immutable_inventory()
            def mutate(ack):
                token, key, *_ = ack
                dest = 'moved/'+key
                self.api('POST', '/posix/rename', {'source_path': key, 'destination_path': dest}, token=token)
                self.api('POST', '/posix/trash', {'path': dest}, token=token)
                self.api('POST', '/posix/recover', {'path': dest}, token=token)
                self.api('POST', '/posix/rename', {'source_path': dest, 'destination_path': key}, token=token)
            candidates = [ack for ack in self.acknowledged if ack[1].startswith('ramp-16/')]
            self.parallel(mutate, candidates, 32)
            assert before == self.immutable_inventory(), 'NCX operations altered NDX or payload files'
            return {'objects': len(candidates), 'immutable_files_hashed_before_and_after': len(before)}
        self.phase_run('ncx_only_immutable_inventory', mutations)

        def interrupt():
            before = self.immutable_inventory()
            candidates = [ack for ack in self.acknowledged if ack[1].startswith('ramp-128/')]
            expected = {ack[1] for ack in candidates}
            def rename():
                try:
                    return self.call('POST', '/posix/rename', {'source_path': 'ramp-128/', 'destination_path': 'interrupted/'})[0]
                except (OSError, http.client.HTTPException):
                    return 'expected_connection_loss_at_sigkill'
            with ThreadPoolExecutor(max_workers=1) as workers:
                future = workers.submit(rename)
                deadline = time.monotonic()+20
                observed = False
                while time.monotonic() < deadline:
                    observed = any(next(root.glob('@f_*/.namespace/interrupted.ncp/*.ncx'), None) for root in self.roots)
                    if observed:
                        self.stop()
                        break
                    assert not future.done(), 'Rename finished before interrupt; this is not a mid-operation proof'
                    time.sleep(.001)
                assert observed, 'No destination publication observed for interruption'
                outcome = future.result(timeout=10)
            self.start('interrupted-prefix-rename')
            source = set(self.list_all('ramp-128'))
            destination = {key.replace('interrupted/', 'ramp-128/', 1) for key in self.list_all('interrupted')}
            assert source | destination == expected, 'Crash lost a committed namespace reference'
            duplicates = len(source & destination)
            self.api('POST', '/posix/rename', {'source_path': 'ramp-128/', 'destination_path': 'interrupted/'})
            assert not self.list_all('ramp-128')
            assert {key.replace('interrupted/', 'ramp-128/', 1) for key in self.list_all('interrupted')} == expected
            self.api('POST', '/posix/rename', {'source_path': 'interrupted/', 'destination_path': 'ramp-128/'})
            self.parallel(self.read_ack, candidates, 32)
            assert before == self.immutable_inventory(), 'Interrupted NCX mutation changed NDX/payload'
            return {'objects': len(candidates), 'source_names_after_crash': len(source),
                    'destination_names_after_crash': len(destination), 'duplicate_names_after_crash': duplicates,
                    'request_outcome': outcome, 'retry_completed_and_all_hashes_verified': True}
        self.phase_run('sigkill_during_prefix_copy_delete', interrupt)

        def verify():
            self.parallel(self.read_ack, list(self.acknowledged), 32)
            for prefix in (16, 64, 128, 256, 512):
                expected = {ack[1] for ack in self.acknowledged if ack[1].startswith(f'ramp-{prefix}/')}
                assert set(self.list_all(f'ramp-{prefix}')) == expected
            return {'acknowledged_versions_hashed': len(self.acknowledged), 'all_ramp_listing_pages_verified': True}
        self.phase_run('complete_integrity_before_restart', verify)
        self.stop()
        self.start('after-sigkill')
        self.phase_run('complete_integrity_after_sigkill', verify)

        def limited():
            self.stop()
            self.config['rate_limiter_config']['enable_rate_limiting'] = 'true'
            self.write_config()
            self.start('production-rate-limits')
            outcomes = self.parallel(lambda _: self.call('GET', '/objects/versions/shared.bin')[0], range(256), 64)
            assert 200 in outcomes and 429 in outcomes, Counter(outcomes)
            assert not (set(outcomes) - {200, 429}), Counter(outcomes)
            time.sleep(1)
            recovered = self.call('GET', '/objects/versions/shared.bin')[0]
            assert recovered == 200, 'View limiter did not replenish after one second'
            return {'offered_requests': 256, 'statuses': dict(Counter(outcomes)), 'one_second_refill': recovered}
        self.phase_run('production_rate_limits_and_recovery', limited)
        self.stop()
        self.config['rate_limiter_config']['enable_rate_limiting'] = 'false'
        self.write_config()
        self.start('drained')
        self.phase = 'drain'
        time.sleep(5)
        self.phase_run('final_integrity_after_overload_and_refill', verify)
        self.stop()
        logs = '\n'.join(p.read_text(errors='replace') for p in self.root.glob('engine-*.log'))
        assert 'panicked at' not in logs and 'memory allocation of' not in logs
        assert not self.ambiguous, f'{len(self.ambiguous)} transport-ambiguous writes require investigation'
        assert not self.fault, self.fault
        self.report.update(status='PASS', acknowledged_versions=len(self.acknowledged),
                           peak_active_requests=self.peak_active, elapsed_seconds=round(time.monotonic()-self.started, 3))

    def finish(self):
        self.stop()
        self.stop_monitor.set()
        self.monitor.join(timeout=3)
        self.report['resources'] = {'samples': len(self.samples),
            'peak_rss_kib': max((s['rss_kib'] for s in self.samples), default=0),
            'sampled_process_hwm_kib': max((s['hwm_kib'] for s in self.samples), default=0),
            'peak_fds': max((s['fds'] for s in self.samples), default=0),
            'by_phase': {phase: {'peak_rss_kib': max(s['rss_kib'] for s in self.samples if s['phase']==phase),
                                 'peak_fds': max(s['fds'] for s in self.samples if s['phase']==phase)}
                         for phase in {s['phase'] for s in self.samples}}}
        (self.root/'resource-samples.json').write_text(json.dumps(self.samples))
        (self.root/'results.json').write_text(json.dumps(self.report, indent=2))
        print(json.dumps(self.report, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--io-backend', choices=('auto', 'uring', 'blocking'), default='auto')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--data-shards', type=int, default=1)
    parser.add_argument('--parity-shards', type=int, default=1)
    parser.add_argument('--port', type=int, default=56801)
    parser.add_argument('--deadline-seconds', type=int, default=1800)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    test = Stress(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}')
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
