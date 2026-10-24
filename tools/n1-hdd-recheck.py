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
    root, parent = root.resolve(strict=True), parent.resolve(strict=True)
    assert root.parent == parent and root.name.startswith('n1-rc-stress-'), 'Not a disposable fixture member'
    assert (root / '.n1-pool.json').is_file(), 'Enrolled fixture member required'
    assert root.stat().st_dev == parent.stat().st_dev, 'Member filesystem mismatch'
    return root


def fixture_fragment(roots):
    # Public authentication takes the fragment alias, not its physical ULID.
    aliases = set()
    for root in roots:
        local = set()
        for path in (root / '.fragment').iterdir():
            target = path.resolve(strict=True)
            assert target.parent == root and target.name.startswith('@f_'), 'Unexpected alias target'
            assert path.is_symlink() and (target / '.fragment.ndx').is_file()
            local.add(path.name)
        assert len(local) == 1, 'Expected exactly one isolated test fragment alias'
        aliases.update(local)
    assert len(aliases) == 1, 'Member aliases disagree'
    return aliases.pop()


class Recheck(base.Stress):
    ack_rows = scale.Scale.ack_rows
    verify_row = scale.Scale.verify_row
    verify_all = scale.Scale.verify_all
    snapshot_verify = scale.Scale.snapshot_verify

    def __init__(self, args):
        self.args, self.root = args, args.root.resolve()
        fixture = args.fixture.resolve(strict=True)
        parents = [path.resolve(strict=True) for path in args.member_parent]
        assert len(parents) == 2 and all(os.path.ismount(path) for path in parents)
        assert len({path.stat().st_dev for path in parents}) == 2, 'Two distinct HDD filesystems required'
        assert fixture.is_relative_to(parents[0]) and self.root.is_relative_to(parents[0])
        assert not self.root.exists(), 'Never overwrite a previous verification'
        self.config = configparser.ConfigParser(interpolation=None)
        self.config.read(fixture / 'n1_config.ini')
        pools = [name for name in self.config if name.startswith('storage_pool.')]
        assert pools == ['storage_pool.A'], 'Only the isolated one-pool fixture is supported'
        configured = self.config['storage_pool.A']['roots'].split(',')
        assert len(configured) == len(parents)
        self.roots = [fixture_member(Path(path), parent) for path, parent in zip(configured, parents)]
        control = Path(self.config['disk_config']['fs_storage_path']).resolve(strict=True)
        assert control == fixture / 'control', 'Unexpected fixture control root'
        for port in (args.port, args.port + 1):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(('127.0.0.1', port))
        original = json.loads((fixture / 'results.json').read_text())
        checksum = base.digest(args.binary.read_bytes())
        assert checksum == original['engine_sha256'], 'Recheck the exact failed candidate first'
        self.fragment = fixture_fragment(self.roots)
        self.root.mkdir(mode=0o700)
        self.original_evidence = {fixture / name: base.digest((fixture / name).read_bytes())
            for name in ('results.json', 'acknowledgements.sqlite', 'acknowledgements.jsonl')}
        if args.clone:
            suffix = base.secrets.token_hex(6)
            cloned = [parent / f'n1-rc-stress-recheck-{suffix}-member-{index}'
                      for index, parent in enumerate(parents)]
            for source, destination in zip(self.roots + [control], cloned + [self.root / 'control']):
                assert not destination.exists()
                destination.mkdir(mode=0o700)
                assert source.stat().st_dev == destination.stat().st_dev
                subprocess.run(['cp', '-a', '--reflink=always', str(source) + '/.', str(destination)], check=True)
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
        source = sqlite3.connect((fixture / 'acknowledgements.sqlite').as_uri() + '?mode=ro', uri=True)
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
                           scope='existing isolated fixture; original evidence preserved; no new uploads',
                           phases=[])
        self.fixture = fixture

    def resolve_ambiguous(self):
        path = self.fixture / 'ambiguous-batches.jsonl'
        intervals = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        counts = Counter()
        with (self.root / 'resolved-ambiguous.jsonl').open('x') as evidence:
            for interval in intervals:
                for index in range(interval['start'], interval['end']):
                    key = scale.source_key(index)
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
    parser.add_argument('--port', type=int, default=57311)
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--clone', action='store_true', help='Verify reflink copies; never start the original members')
    parser.add_argument('--resolve-ambiguous', action='store_true', help='Read and hash uncertain outcomes; never retry writes')
    args = parser.parse_args()
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
