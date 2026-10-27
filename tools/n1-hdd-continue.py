#!/usr/bin/env python3
"""Complete the failed million-object gate IN PLACE; never copy member storage.

The original FAIL and upload evidence remain immutable. A new, independently
identified build must pass pinned correctness gates before it may enumerate the
existing population. Only after a complete inventory passes are bounded probe
objects added and churn permitted. Run headlessly in a 4 GiB/no-swap unit.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import threading
import time
import traceback

spec = importlib.util.spec_from_file_location('continuation_recheck', Path(__file__).with_name('n1-hdd-recheck.py'))
recheck = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recheck)
scale, base = recheck.scale, recheck.base
REQUIRED_GATES = {'library-regression', 'inventory-focused', 'harness-regression', 'required-uring'}


def pinned_json(path, expected):
    assert re.fullmatch(r'[0-9a-f]{64}', expected), 'Invalid SHA-256 pin'
    assert recheck.file_digest(path) == expected, f'Pinned evidence changed: {path.name}'
    return json.loads(path.read_text())


def validate_gate(gate, candidate, source):
    assert gate.get('status') == 'PASS' and gate.get('engine_sha256') == candidate, 'Candidate gate/build mismatch'
    assert gate.get('source_upload_engine_sha256') == source, 'Candidate gate/source build mismatch'
    phases = gate.get('phases')
    assert isinstance(phases, list) and all(isinstance(item, dict) for item in phases), 'Missing correctness gate phases'
    names = [item.get('name') for item in phases]
    assert all(isinstance(name, str) for name in names) and len(names) == len(set(names)), 'Duplicate/invalid gate names'
    assert REQUIRED_GATES <= set(names) and all(item.get('status') == 'PASS' for item in phases), 'Incomplete correctness gates'


def validate_population(report, objects):
    assert report.get('status') == 'FAIL', 'Continuation requires the preserved failed result'
    assert report.get('source_layout') == 'flat-folder', 'Only the qualified flat million population is supported'
    assert report.get('workload') == 'upload-then-churn', 'Wrong source workload'
    assert report.get('strict_uploads') is True and report.get('automatic_write_retries') is False, 'Wrong source durability policy'
    counts = report.get('counts', {})
    assert counts.get('source_objects') == counts.get('acknowledged_versions') == objects, 'Incomplete source acknowledgements'
    assert counts.get('namespace_mutations', 0) == 0, 'Source already entered churn'
    phases = report.get('phases', [])
    hashes = [phase for phase in phases if phase.get('name') == f'complete-version-integrity-at-{objects}']
    assert len(hashes) == 1 and hashes[0].get('status') == 'PASS', 'Missing complete million-version integrity gate'
    assert hashes[0].get('every_acknowledged_version_downloaded_and_hashed') == objects, 'Partial source hash checkpoint'
    assert phases[-1].get('name') == 'complete-quiescent-namespace-inventory' and phases[-1].get('status') == 'FAIL', 'Unexpected failed phase'


def renew_fixture_certificate(test):
    """Only the new evidence directory gets a fresh, verified loopback CA."""
    certificate, key = test.root / 'cert.pem', test.root / 'key.pem'
    assert not certificate.exists() and not key.exists(), 'Never replace an existing certificate'
    recheck.subprocess.run(['openssl', 'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1',
        '-nodes', '-days', '7', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost',
        '-keyout', str(key), '-out', str(certificate)], check=True,
        stdout=recheck.subprocess.DEVNULL, stderr=recheck.subprocess.DEVNULL)
    test.config['server'].update(cert_path=str(certificate), key_path=str(key))
    test.write_config()
    test.tls = recheck.ssl.create_default_context(cafile=str(certificate))


def decode_probe_pointer(data):
    """Strict reader for the existing three-field NCX map; never writes format.

    Supports only the integer/string encodings used by these fields, not a
    general MessagePack implementation. A future format must update this test.
    """
    assert 39 <= len(data) <= 4096 and data[:7] == b'N1NCX4\0', 'Invalid probe NCX header/size'
    body = data[39:]
    assert hashlib.sha256(body).digest() == data[7:39], 'Probe NCX checksum mismatch'
    assert body and body[0] == 0x83, 'Expected three-field NCX map'
    offset = 1
    def value():
        nonlocal offset
        assert offset < len(body), 'Truncated probe NCX'
        tag = body[offset]
        offset += 1
        if tag <= 0x7f:
            return tag
        if 0xa0 <= tag <= 0xbf:
            length = tag & 0x1f
        elif tag in (0xd9, 0xda):
            width = 1 if tag == 0xd9 else 2
            assert offset + width <= len(body), 'Truncated probe NCX string length'
            length = int.from_bytes(body[offset:offset+width], 'big')
            offset += width
        elif tag in (0xcc, 0xcd):
            width = 1 if tag == 0xcc else 2
            assert offset + width <= len(body), 'Truncated probe NCX integer'
            result = int.from_bytes(body[offset:offset+width], 'big')
            offset += width
            return result
        else:
            raise AssertionError('Unsupported probe NCX value type')
        assert length <= 128 and offset + length <= len(body), 'Invalid probe NCX string size'
        result = body[offset:offset+length].decode('utf-8')
        offset += length
        return result
    pointer = {}
    for _ in range(3):
        key, item = value(), value()
        assert isinstance(key, str) and key not in pointer, 'Duplicate/invalid probe NCX field'
        pointer[key] = item
    assert offset == len(body) and set(pointer) == {'format', 'pool_id', 'object_id'}, 'Invalid probe NCX fields'
    assert pointer['format'] == 4 and type(pointer['format']) is int and pointer['pool_id'] == 'A', 'Probe NCX pool/format mismatch'
    assert isinstance(pointer['object_id'], str) and re.fullmatch(r'[0-9A-HJKMNP-TV-Z]{26}', pointer['object_id']), 'Invalid probe object ULID'
    return pointer


def probe_object_dirs(roots, fragment, key, version, expected_replicas):
    parts = key.split('/')
    assert len(parts) == 2 and parts[0] == 'churn' and re.fullmatch(r'probe-[0-9]{4}', parts[1]), 'Unexpected probe key'
    assert re.fullmatch(r'[0-9A-HJKMNP-TV-Z]{26}', version), 'Invalid acknowledged probe version ULID'
    pointers = []
    fragment_names = set()
    for root in roots:
        physical = (root / '.fragment' / fragment).resolve(strict=True)
        assert physical.parent == root and physical.name.startswith('@f_') and physical.is_dir(), 'Probe fragment escaped member'
        fragment_names.add(physical.name)
        ncx = physical / '.namespace' / (parts[0] + '.ncp') / (parts[1] + '.ncx')
        assert all(not parent.is_symlink() for parent in (physical / '.namespace', ncx.parent)), 'Probe namespace parent must not be a symlink'
        assert not ncx.is_symlink(), 'Probe NCX must not be a symlink'
        if ncx.exists():
            assert ncx.is_file() and ncx.stat().st_size <= 4096, 'Invalid probe NCX type/size'
            with ncx.open('rb') as stream:
                pointers.append(decode_probe_pointer(stream.read(4097)))
    assert len(fragment_names) == 1 and len(pointers) == expected_replicas, 'Incomplete probe NCX replicas'
    assert all(pointer == pointers[0] for pointer in pointers), 'Conflicting probe NCX replicas'
    object_id = pointers[0]['object_id']
    fanout = hashlib.sha256(object_id.encode()).hexdigest()
    directories = []
    for root in roots:
        directory = root / next(iter(fragment_names)) / '.objects' / fanout[:2] / fanout[2:4] / ('@o_' + object_id)
        assert all(not parent.is_symlink() for parent in directory.parents if parent != root and parent.is_relative_to(root)), 'Probe object parent must not be a symlink'
        assert directory.is_dir() and not directory.is_symlink(), 'Probe object directory missing'
        version_dir = directory / version
        assert version_dir.is_dir() and not version_dir.is_symlink(), 'Acknowledged probe version missing'
        directories.append(directory)
    return directories


class Continue(recheck.Recheck):
    phase_run = scale.Scale.phase_run
    progress = scale.Scale.progress
    monitor_loop = scale.Scale.monitor_loop
    record_acks = scale.Scale.record_acks
    upload_one = scale.Scale.upload_one
    immutable_probe_inventory = scale.Scale.immutable_probe_inventory
    churn = scale.Scale.churn
    resource_checkpoint = scale.Scale.resource_checkpoint

    def validate_binary(self, checksum, original):
        assert re.fullmatch(r'[0-9a-f]{64}', self.args.source_engine_sha256)
        assert re.fullmatch(r'[0-9a-f]{64}', self.args.candidate_sha256)
        assert original['engine_sha256'] == self.args.source_engine_sha256, 'Source engine pin mismatch'
        assert checksum == self.args.candidate_sha256, 'Candidate binary pin mismatch'
        validate_population(original, self.args.objects)
        self.correctness_gate = pinned_json(self.args.correctness_result, self.args.correctness_result_sha256)
        validate_gate(self.correctness_gate, checksum, original['engine_sha256'])

    def __init__(self, args):
        original = pinned_json(args.fixture / 'results.json', args.source_result_sha256)
        assert args.clone is False, 'Continuation never copies storage members'
        super().__init__(args)
        renew_fixture_certificate(self)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA temp_store=FILE')
        self.db.execute('PRAGMA cache_size=-8192')
        unique = self.db.execute('SELECT COUNT(DISTINCT object_key),COUNT(*) FROM acks').fetchone()
        assert unique == (args.objects, args.objects), 'Source ledger must have one acknowledged version per object'
        seen = self.db.execute('SELECT COUNT(*) FROM seen').fetchone()[0]
        self.phases = self.report['phases']
        self.db_lock, self.auth_lock = threading.Lock(), threading.Lock()
        self.last_auth = 0
        self.ledger = (self.root / 'new-probe-acknowledgements.jsonl').open('x', buffering=1)
        self.batch_timings = defaultdict(lambda: defaultdict(scale.Histogram))
        self.peaks, self.by_phase = Counter(), defaultdict(Counter)
        self.counts.update(acknowledged_versions=args.objects, source_objects=args.objects,
                           acknowledged_bytes=original['counts']['acknowledged_bytes'])
        self.initial_free_bytes = {root: shutil.disk_usage(root).free for root in self.roots}
        self.disk_stats = {}
        for root in self.roots:
            device = root.stat().st_dev
            link = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}'
            if link.exists():
                block = link.resolve()
                if (block / 'partition').exists():
                    block = block.parent
                self.disk_stats[str(root)] = block / 'stat'
        group = Path('/proc/self/cgroup').read_text().strip().split('::')[-1]
        self.cgroup = Path('/sys/fs/cgroup') / group.lstrip('/')
        memory_max = (self.cgroup / 'memory.max').read_text().strip()
        assert memory_max.isdigit() and int(memory_max) <= 4 * 1024**3, 'Use a <=4 GiB bounded systemd unit'
        assert (self.cgroup / 'memory.swap.max').read_text().strip() == '0', 'Use a no-swap test unit'
        self.report.update(scope='In-place post-upload continuation; no member/control cloning or source reupload',
            source_result_sha256=args.source_result_sha256, source_upload_engine_sha256=original['engine_sha256'],
            candidate_sha256=args.candidate_sha256, correctness_result_sha256=args.correctness_result_sha256,
            source_acknowledged_objects=args.objects, source_seen_rows_without_completion_frame=seen,
            original_status=original['status'], member_storage_copied=False, source_objects_reuploaded=False,
            target_objects=args.objects, target_churn_mutations=args.churn_mutations,
            automatic_write_retries=False, strict_uploads=True, max_filesystem_growth_gib=args.max_filesystem_growth_gib,
            physical_block_devices=list(map(str, scale.independent_member_devices(args.member_parent))),
            source_phases_reused=original['phases'][:-1], original_report_replaced=False,
            note='This is a separately built continuation, not an unchanged-build million-upload PASS.')
        self.original_evidence[args.binary] = args.candidate_sha256
        self.original_evidence[args.correctness_result] = args.correctness_result_sha256
        self.stop_monitor = threading.Event()
        self.ready = True
        self.monitor = threading.Thread(target=self.monitor_loop, daemon=True)
        self.monitor.start()
        self.progress()

    def authenticate(self):
        if self.token and time.monotonic() - self.last_auth < 600:
            return
        with self.auth_lock:
            if self.token and time.monotonic() - self.last_auth < 600:
                return
            self.token = self.api('POST', '/auth/fragment', dict(fragment=self.fragment, secret=self.master,
                read=True, write=True, modify=True, delete=True, expires_in=21600), token='')['access_token']
            self.last_auth = time.monotonic()

    def setup_probes(self):
        for _ in scale.bounded_map(self.upload_one,
                ((f'churn/probe-{i:04}', f'churn/probe-{i:04}', 4096, 'probe') for i in range(self.args.probes)), 16):
            pass
        self.probe_directories = set()
        rows = 0
        replicas = min(int(self.config['storage_pool.A']['parity_shards']) + 1, len(self.roots))
        for key, version, _, _, _ in self.ack_rows('probe'):
            self.probe_directories.update(probe_object_dirs(self.roots, self.fragment, key, version, replicas))
            rows += 1
        assert rows == self.args.probes and len(self.probe_directories) == rows * len(self.roots), 'Incomplete probe inventory'
        return dict(probe_objects=rows, immutable_probe_directories=len(self.probe_directories),
                    discovery='Exact bounded probe NCX identities and existing SHA256 object fanout; no population walk')

    def run(self):
        self.start('in-place-inventory-candidate')
        self.phase_run('resources-before-continuation', self.resource_checkpoint)
        self.phase_run('complete-quiescent-namespace-inventory', self.snapshot_verify)
        self.phase_run('post-population-probe-object-setup', self.setup_probes)
        self.phase_run('post-population-probe-integrity', self.verify_all)
        cycles = max(1, math.ceil(self.args.churn_mutations / (self.args.probes * 16)))
        for concurrency in (8, 32, 128, 256):
            self.phase_run(f'high-churn-c{concurrency}', lambda c=concurrency: self.churn(cycles, c))
            self.phase_run(f'resources-after-churn-c{concurrency}', self.resource_checkpoint)
        self.phase_run('all-version-integrity-after-churn', self.verify_all)
        self.stop()
        self.start('continuation-after-final-sigkill')
        self.phase_run('all-version-integrity-after-final-sigkill', self.verify_all)
        self.phase_run('quiescent-namespace-after-final-sigkill', self.snapshot_verify)
        self.phase_run('resources-after-final-sigkill', self.resource_checkpoint)
        assert self.counts['source_objects'] == self.args.objects
        assert self.counts['namespace_mutations'] >= self.args.churn_mutations
        assert not self.fault, self.fault
        self.report['status'] = 'PASS'

    def finish(self):
        self.stop_monitor.set()
        self.monitor.join(timeout=3)
        try:
            self.report['original_reports_and_ledgers_unchanged'] = all(
                recheck.file_digest(path) == expected for path, expected in self.original_evidence.items())
            assert self.report['original_reports_and_ledgers_unchanged'], 'Original failed evidence changed'
        except BaseException as error:
            self.report.update(status='FAIL', error=f'{type(error).__name__}: {error}')
            raise
        finally:
            scale.Scale.finish(self)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('fixture', 'root', 'binary', 'correctness-result'):
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ('source-result-sha256', 'source-engine-sha256', 'candidate-sha256', 'correctness-result-sha256'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--port', type=int, default=59241)
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--objects', type=int, default=1000000)
    parser.add_argument('--probes', type=int, default=1000)
    parser.add_argument('--churn-mutations', type=int, default=1000000)
    parser.add_argument('--deadline-seconds', type=int, default=172800)
    parser.add_argument('--free-floor-gib', type=int, default=140)
    parser.add_argument('--max-filesystem-growth-gib', type=int, default=64)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('Do not disable safety assertions with -O')
    assert args.objects >= 1000000 and 256 <= args.probes <= 4096 and args.churn_mutations >= 1000000
    assert 1 <= args.deadline_seconds <= 172800 and args.free_floor_gib >= 140
    assert 1 <= args.max_filesystem_growth_gib <= 64
    args.clone, args.clone_member_parent = False, None
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    test = Continue(args)
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
