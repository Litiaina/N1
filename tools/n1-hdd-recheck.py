#!/usr/bin/env python3
"""Restart and hash an existing disposable HDD fixture without replacing its evidence.

Only named n1-rc-stress member directories are accepted. The original reports,
configuration, and acknowledgement ledger are never overwritten. Engine startup
and recover-on-read retain their normal repair behavior; no uploads are issued.
"""
import argparse
from collections import Counter, defaultdict
import configparser
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import traceback
import urllib.parse

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('hdd_scale', Path(__file__).with_name('n1-hdd-scale.py'))
scale = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scale)
base = scale.base


def fixture_member(root, parent):
    assert not root.is_symlink(), 'Fixture member must not be a symlink'
    root, parent = root.resolve(strict=True), parent.resolve(strict=True)
    assert root.parent == parent and root.name.startswith('n1-rc-stress-'), 'Not a disposable fixture member'
    assert (root / '.n1-pool.json').is_file(), 'Enrolled fixture member required'
    assert root.stat().st_dev == parent.stat().st_dev, 'Member filesystem mismatch'
    return root


def fixture_fragment(roots):
    # Public authentication takes the fragment alias, not its physical ULID.
    aliases, identities, metadata = set(), set(), set()
    for root in roots:
        local = set()
        directory = root / '.fragment'
        assert directory.is_dir() and not directory.is_symlink(), 'Expected a real fragment alias directory'
        for path in directory.iterdir():
            target = path.resolve(strict=True)
            assert target.parent == root and target.name.startswith('@f_'), 'Unexpected alias target'
            record = target / '.fragment.ndx'
            assert path.is_symlink() and record.is_file() and not record.is_symlink()
            assert 0 <= record.stat().st_size <= 1024 * 1024, 'Oversized fragment metadata'
            identities.add(target.name)
            metadata.add(base.digest(record.read_bytes()))
            local.add(path.name)
        assert len(local) == 1, 'Expected exactly one isolated test fragment alias'
        aliases.update(local)
    assert len(aliases) == 1, 'Member aliases disagree'
    assert len(identities) == 1, 'Member stable fragment identities disagree'
    assert len(metadata) == 1, 'Member fragment metadata differs'
    return aliases.pop()


def source_description(fixture, parents):
    """Read exact enrolled topology; never reinterpret a four-member pool as two."""
    fixture = fixture.resolve(strict=True)
    parents = [Path(path).resolve(strict=True) for path in parents]
    config = configparser.ConfigParser(interpolation=None)
    assert config.read(fixture / 'n1_config.ini'), 'Missing source configuration'
    assert [name for name in config if name.startswith('storage_pool.')] == ['storage_pool.A']
    pool = config['storage_pool.A']
    data, parity = int(pool['data_shards']), int(pool['parity_shards'])
    assert 1 <= data <= 32 and 0 <= parity <= 32 and int(pool['generation']) > 0, 'Unsupported source EC geometry'
    configured = [value.strip() for value in pool['roots'].split(',')]
    assert len(configured) == len(parents) == data + parity, 'Source EC/member count mismatch'
    assert all(Path(value).is_absolute() for value in configured), 'Absolute source member paths required'
    for parent in parents:
        scale.require_storage_filesystem(parent)
    assert len({path.stat().st_dev for path in parents}) == len(parents), 'Distinct member filesystems required'
    scale.independent_member_devices(parents)
    roots = [fixture_member(Path(value), parent) for value, parent in zip(configured, parents)]
    assert len(set(roots)) == len(roots), 'Duplicate source member'
    control = Path(config['disk_config']['fs_storage_path']).resolve(strict=True)
    assert control == fixture / 'control', 'Unexpected source control root'
    scale.require_storage_filesystem(fixture)
    assert fixture.stat().st_dev == roots[0].stat().st_dev, 'Source control must remain on its fixture filesystem'

    identities, member_ids = None, set()
    for slot, root in enumerate(roots):
        path = root / '.n1-pool.json'
        assert not path.is_symlink() and path.stat().st_size <= 1024 * 1024, 'Invalid pool descriptor'
        descriptor = json.loads(path.read_text())
        assert descriptor['member_format'] == 'n1-pool-member-v1', 'Unknown source member format'
        assert all(type(descriptor[key]) is int for key in ('generation', 'data_shards', 'parity_shards', 'member_slot'))
        assert (descriptor['id'], descriptor['generation'], descriptor['data_shards'], descriptor['parity_shards']) == (
            'A', int(pool['generation']), data, parity), 'Source member/config geometry mismatch'
        assert descriptor['member_slot'] == slot, 'Source member slot/order mismatch'
        members = descriptor['members']
        assert isinstance(members, list) and len(members) == len(roots) and len(set(members)) == len(roots), 'Invalid source member identities'
        assert all(isinstance(value, str) and value for value in members), 'Empty source member identity'
        assert descriptor['member_id'] == members[slot], 'Source member identity/slot mismatch'
        addresses, legacy = descriptor['roots'], descriptor.get('legacy_roots', [])
        assert isinstance(addresses, list) and len(addresses) == len(roots), 'Invalid source address geometry'
        assert isinstance(legacy, list) and (not legacy or len(legacy) == len(roots)), 'Invalid source legacy address geometry'
        assert all(isinstance(value, str) and Path(value).is_absolute() for value in addresses + legacy)
        identity = (tuple(members), tuple(descriptor['roots']), tuple(descriptor.get('legacy_roots', [])))
        assert identities is None or identity == identities, 'Source member catalogs disagree'
        identities = identity
        marker = root / '.n1-root-id'
        assert marker.is_file() and not marker.is_symlink() and marker.stat().st_size <= 1024 * 1024
        marker_data = json.loads(marker.read_text())
        assert marker_data['engine_format'] == 'n1-4-root-identity-v1', 'Unknown source root format'
        assert marker_data['root_id'] == descriptor['member_id'], 'Source root/member identity mismatch'
        member_ids.add(descriptor['member_id'])
    assert len(member_ids) == len(roots)
    report = json.loads((fixture / 'results.json').read_text())
    if 'topology' in report:
        topology = report['topology']
        assert all(type(topology[key]) is int for key in ('data_shards', 'parity_shards'))
        assert (topology['data_shards'], topology['parity_shards']) == (data, parity), 'Source report/config geometry mismatch'
        assert [Path(value).resolve(strict=True) for value in topology['roots']] == roots, 'Source report/config roots mismatch'
    fixture_fragment(roots)
    return config, roots, control


def source_io_backend(fixture, report):
    values = dict(line.split('=', 1) for line in (fixture / 'n1.env').read_text().splitlines() if '=' in line)
    configured = values.get('N1_IO_BACKEND', 'auto')
    requested = report.get('requested_io_backend', configured)
    assert requested in ('auto', 'uring', 'blocking'), 'Unknown source requested I/O backend'
    assert configured == requested, 'Source report/environment I/O backend mismatch'
    return requested


def source_ledger(fixture):
    path = fixture / 'acknowledgements.sqlite'
    assert path.is_file() and not path.is_symlink(), 'Expected a real frozen source ledger'
    for suffix in ('-wal', '-journal'):
        journal = path.with_name(path.name + suffix)
        assert not journal.exists() or journal.stat().st_size == 0, 'Source ledger must be checkpointed before immutable verification'
    # mode=ro alone may create WAL/SHM sidecars. Immutable snapshots make the
    # original failed run strictly read-only, including its directory entries.
    return sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)


class Recheck(base.Stress):
    ack_rows = scale.Scale.ack_rows
    verify_row = scale.Scale.verify_row
    verify_all = scale.Scale.verify_all
    snapshot_verify = scale.Scale.snapshot_verify

    def __init__(self, args):
        self.args, self.root = args, args.root.resolve()
        fixture = args.fixture.resolve(strict=True)
        parents = [path.resolve(strict=True) for path in args.member_parent]
        assert not self.root.exists(), 'Never overwrite a previous verification'
        self.config, self.roots, control = source_description(fixture, parents)
        scale.require_storage_filesystem(self.root.parent)
        assert self.root.parent.stat().st_dev == parents[0].stat().st_dev, 'Verification must remain on the first member filesystem'
        assert not self.root.is_relative_to(fixture), 'Never place verification inside source evidence'
        assert all(not self.root.is_relative_to(root) for root in self.roots), 'Never place verification inside a source member'
        for port in (args.port, args.port + 1):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
        original = json.loads((fixture / 'results.json').read_text())
        self.io_backend = args.io_backend = source_io_backend(fixture, original)
        checksum = base.digest(args.binary.read_bytes())
        assert checksum == original['engine_sha256'], 'Recheck the exact failed candidate first'
        self.fragment = fixture_fragment(self.roots)
        destination_parents = getattr(args, 'clone_member_parent', None)
        assert args.clone or not destination_parents, 'Clone destinations require --clone'
        destination_parents = [path.resolve(strict=True) for path in destination_parents] if destination_parents else parents
        assert len(destination_parents) == len(self.roots), 'Clone destination/member count mismatch'
        for source, parent in zip(self.roots, destination_parents):
            scale.require_storage_filesystem(parent)
            assert source.stat().st_dev == parent.stat().st_dev, 'Clone must remain on its source member filesystem'
            assert not parent.is_relative_to(fixture), 'Never clone inside source evidence'
            assert all(not parent.is_relative_to(root) for root in self.roots), 'Never clone inside a source member'
            assert not any((path / '.n1-pool.json').exists() for path in (parent, *parent.parents)), (
                'Clone parent must not be inside an enrolled root')
        self.root.mkdir(mode=0o700)
        self.original_evidence = {fixture / name: base.digest((fixture / name).read_bytes())
            for name in ('results.json', 'acknowledgements.sqlite', 'acknowledgements.jsonl',
                         'n1_config.ini', 'n1.env', 'ambiguous-batches.jsonl') if (fixture / name).exists()}
        if args.clone:
            suffix = base.secrets.token_hex(6)
            cloned = [parent / f'n1-rc-stress-recheck-{suffix}-member-{index}'
                      for index, parent in enumerate(destination_parents)]
            for source, destination in zip(self.roots + [control], cloned + [self.root / 'control']):
                assert not destination.exists()
                destination.mkdir(mode=0o700)
                assert source.stat().st_dev == destination.stat().st_dev
                subprocess.run(['cp', '-a', '--reflink=auto', str(source) + '/.', str(destination)], check=True)
            self.roots = cloned
            self.config['storage_pool.A']['roots'] = ','.join(map(str, cloned))
            self.config['disk_config']['fs_storage_path'] = str(self.root / 'control')
        self.config['server'].update(host='127.0.0.1', https_port=str(args.port), native_port=str(args.port + 1))
        self.write_config()
        shutil.copyfile(fixture / 'n1.env', self.root / 'n1.env')
        values = dict(line.split('=', 1) for line in (fixture / 'n1.env').read_text().splitlines() if '=' in line)
        self.master = values['MASTER_KEY']
        self.tls = ssl.create_default_context(cafile=self.config['server']['cert_path'])
        self.process, self.generation = None, 0
        self.local, self.lock = threading.local(), threading.Lock()
        self.calls, self.latency = defaultdict(Counter), defaultdict(scale.Histogram)
        self.counts = Counter()
        self.active, self.peak_active = 0, 0
        self.phase, self.fault, self.token = 'restart', None, ''
        self.started = time.monotonic()
        self.storage_devices = {path: path.stat().st_dev for path in [self.root] + self.roots}
        source = source_ledger(fixture)
        self.db = sqlite3.connect(self.root / 'acknowledgements.sqlite')
        try:
            source.backup(self.db)
        finally:
            source.close()
        self.db.execute('PRAGMA cache_size=-4096')
        expected = self.db.execute('SELECT COUNT(*) FROM acks').fetchone()[0]
        assert expected == original['counts']['acknowledged_versions'], 'Ledger/report count mismatch'
        self.report = dict(status='RUNNING', expected_acknowledged_versions=expected,
                           engine_sha256=checksum, original_status=original['status'],
                           requested_io_backend=self.io_backend,
                           scope='existing isolated fixture; original evidence preserved; no new uploads',
                           phases=[])
        pool = self.config['storage_pool.A']
        self.report['topology'] = dict(data_shards=int(pool['data_shards']), parity_shards=int(pool['parity_shards']),
                                       roots=list(map(str, self.roots)))
        self.report['source_layout'] = original.get('source_layout', 'project-tree')
        assert self.report['source_layout'] in ('project-tree', 'flat-folder'), 'Unknown source key layout'
        self.source_flat_folder = self.report['source_layout'] == 'flat-folder'
        self.args.flat_folder = self.source_flat_folder
        self.fixture = fixture

    def resolve_ambiguous(self):
        path = self.fixture / 'ambiguous-batches.jsonl'
        intervals = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        counts = Counter()
        with (self.root / 'resolved-ambiguous.jsonl').open('x') as evidence:
            for interval in intervals:
                for index in range(interval['start'], interval['end']):
                    key = scale.source_key(index, getattr(self, 'source_flat_folder', getattr(self.args, 'flat_folder', False)))
                    status, body = self.call('GET', '/objects/versions/' + urllib.parse.quote(key, safe='/'))
                    assert status in (200, 404), f'Cannot resolve {key}: HTTP {status}'
                    versions = json.loads(body) if status == 200 else []
                    assert isinstance(versions, list) and len(versions) <= 1, f'Unexpected version history for unique source {key}'
                    for metadata in versions:
                        row = (key, metadata['version_id'], base.digest(base.payload(key, scale.source_size(index))),
                               scale.source_size(index), 'ambiguous_commit_not_acknowledged')
                        self.verify_row(row)
                        self.db.execute('INSERT OR IGNORE INTO acks VALUES(?,?,?,?,?)', row)
                    outcome = 'committed_and_hashed' if versions else 'not_published'
                    counts[outcome] += 1
                    evidence.write(json.dumps(dict(object_key=key, outcome=outcome,
                        versions=[item['version_id'] for item in versions])) + '\n')
        self.db.commit()
        return dict(batches=len(intervals), **dict(counts), automatic_write_retries=False)

    def authenticate(self):
        if self.token:
            return
        self.token = self.api('POST', '/auth/fragment', dict(fragment=self.fragment,
            secret=self.master, read=True, expires_in=21600), token='')['access_token']

    def run(self):
        self.start('acknowledgement-recheck')
        self.authenticate()
        operations = [('all-acknowledged-version-hashes-after-restart', self.verify_all)]
        if self.args.resolve_ambiguous:
            operations.append(('ambiguous-write-outcomes', self.resolve_ambiguous))
        operations.append(('complete-namespace-ledger-comparison', self.snapshot_verify))
        for name, operation in operations:
            self.phase = name
            started = time.monotonic()
            details = operation()
            self.report['phases'].append(dict(name=name, status='PASS', **details,
                elapsed_seconds=round(time.monotonic() - started, 3)))
            print(json.dumps(self.report['phases'][-1]), flush=True)
        assert self.counts['historical_downloads_verified'] >= self.report['expected_acknowledged_versions']
        self.phase = 'health-after-verification'
        (self.root / 'health.json').write_text(json.dumps(self.api('GET', '/maintenance/health'), indent=2))
        self.report['status'] = 'PASS'

    def finish(self):
        self.stop()
        self.db.close()
        self.report.update(elapsed_seconds=round(time.monotonic() - self.started, 3), counts=dict(self.counts),
                           latencies={name: histogram.summary() for name, histogram in self.latency.items()})
        self.report['original_reports_and_ledgers_unchanged'] = all(base.digest(path.read_bytes()) == checksum
            for path, checksum in self.original_evidence.items())
        if not self.report['original_reports_and_ledgers_unchanged']:
            self.report.update(status='FAIL', error='Original evidence changed')
        (self.root / 'results.json').write_text(json.dumps(self.report, indent=2))
        print(json.dumps(self.report, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--clone-member-parent', type=Path, action='append',
                        help='Optional separate owned destination parent per source member; requires --clone')
    parser.add_argument('--port', type=int, default=57311)
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--clone', action='store_true', help='Verify same-filesystem copies; never start the original members')
    parser.add_argument('--resolve-ambiguous', action='store_true', help='Read and hash uncertain outcomes; never retry writes')
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('Do not disable fixture safety checks with -O')
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    test = Recheck(args)
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
