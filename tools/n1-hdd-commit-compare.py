#!/usr/bin/env python3
"""Compare strict commits on independent copies of one reconciled HDD population.

Never starts the failed fixture. Copies preserve original reports and members.
Both builds get the same committed population, settings, TTL and fixed load.
Scheduled GC overlap waits for the normal timer; no manual shortened TTL is used.
This is a focused regression comparison, not a million-object qualification.
"""
import argparse
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('admission', Path(__file__).with_name('n1-hdd-admission.py'))
admission = importlib.util.module_from_spec(spec)
spec.loader.exec_module(admission)
recheck, scale = admission.recheck, admission.scale


def evidence_hashes(fixture):
    names = ('results.json', 'acknowledgements.sqlite', 'acknowledgements.jsonl',
             'ambiguous-batches.jsonl', 'n1_config.ini', 'n1.env', 'resolved-ambiguous.jsonl')
    return {path: scale.base.digest(path.read_bytes())
            for name in names if (path := fixture / name).exists()}


def unchanged(evidence):
    return all(path.is_file() and scale.base.digest(path.read_bytes()) == checksum
               for path, checksum in evidence.items())


def stage_witness(result, checksum, backend, data, parity):
    witness = json.loads((result / 'results.json').read_text())
    assert witness['status'] == 'PASS', 'Stage result is not a completed PASS'
    assert witness['engine_sha256'] == checksum, 'Stage engine hash differs from the selected build'
    assert witness['requested_io_backend'] == backend, 'Stage I/O backend differs from the source contract'
    assert (witness['topology']['data_shards'], witness['topology']['parity_shards']) == (data, parity), (
        'Stage EC geometry differs from the source contract')
    assert len(witness['topology']['roots']) == data + parity, 'Stage member count differs from the source contract'
    return witness


def copy_member(source, destination):
    assert not source.is_symlink() and source.is_dir(), 'Source must be a real fixture directory'
    assert not destination.is_symlink() and destination.is_dir(), 'New destination must be a real directory'
    assert not any(destination.iterdir()), 'Never overwrite a populated destination'
    assert source.resolve() != destination.resolve()
    assert source.stat().st_dev == destination.stat().st_dev, 'Clone must remain on its member filesystem'
    # Reflink on XFS, ordinary file copy on ext4; never require reflink support.
    subprocess.run(['cp', '-a', '--reflink=auto', str(source) + '/.', str(destination)], check=True)


def source_description(fixture, parents):
    return recheck.source_description(fixture, parents)


def read_population(fixture, report):
    reader = recheck.source_ledger(fixture)
    try:
        return admission.source_population(report, reader)
    finally:
        reader.close()


class Clone(scale.Scale):
    resolve_ambiguous = recheck.Recheck.resolve_ambiguous

    def authenticate(self):
        if self.token and time.monotonic() - self.last_auth < 600:
            return
        with self.auth_lock:
            if self.token and time.monotonic() - self.last_auth < 600:
                return
            self.token = self.api('POST', '/auth/fragment', dict(fragment=self.fragment,
                secret=self.master, read=True, write=True, modify=True, delete=True,
                expires_in=21600), token='')['access_token']
            self.last_auth = time.monotonic()

    def __init__(self, args, fixture, reconcile=False):
        fixture = fixture.resolve(strict=True)
        source_config, sources, control = source_description(fixture, args.source_member_parent)
        source_report = json.loads((fixture / 'results.json').read_text())
        args.io_backend = recheck.source_io_backend(fixture, source_report)
        pool = source_config['storage_pool.A']
        args.data_shards, args.parity_shards = int(pool['data_shards']), int(pool['parity_shards'])
        assert len(args.member_parent) == len(sources), 'Destination/source EC member count mismatch'
        source_layout = source_report.get('source_layout', 'project-tree')
        assert source_layout in ('project-tree', 'flat-folder'), 'Unknown source key layout'
        self.source_flat_folder = source_layout == 'flat-folder'
        # The fixed comparison workload has its own prefixed project tree.
        # Retain the original layout separately for ambiguous-key recovery.
        args.flat_folder = False
        if reconcile:
            assert source_report['engine_sha256'] == scale.base.digest(args.binary.read_bytes()), (
                'Reconcile using the exact failed build first')
        population = read_population(fixture, source_report)
        self.source_evidence = evidence_hashes(fixture)
        super().__init__(args)
        for source, destination in zip(sources + [control], self.roots + [self.root / 'control']):
            copy_member(source, destination)
        current_server = dict(self.config['server'])
        self.config = source_config
        self.config['server'].update(current_server)
        self.config['storage_pool.A']['roots'] = ','.join(map(str, self.roots))
        self.config['disk_config']['fs_storage_path'] = str(self.root / 'control')
        self.write_config()
        shutil.copyfile(fixture / 'n1.env', self.root / 'n1.env')
        self.master = dict(line.split('=', 1) for line in (fixture / 'n1.env').read_text().splitlines()
                           if '=' in line)['MASTER_KEY']
        reader = recheck.source_ledger(fixture)
        try:
            reader.backup(self.db)
        finally:
            reader.close()
        row = self.db.execute("SELECT COUNT(*),COALESCE(SUM(size),0),"
                              "COALESCE(SUM(category='source'),0) FROM acks").fetchone()
        self.counts.update(acknowledged_versions=population['acknowledged_versions'],
                           acknowledged_bytes=row[1] - population['unacknowledged_bytes'],
                           source_objects=row[2],
                           reconciled_unacknowledged_commits=population['unacknowledged_commits'])
        self.fragment = recheck.fixture_fragment(self.roots)
        self.token = ''
        self.fixture = fixture
        self.report.update(initial_population=population, source_fixture=str(fixture),
                           source_layout=source_layout, comparison_upload_layout='commit-comparison/project-tree',
                           expected_acknowledged_versions=population['acknowledged_versions'],
                           original_reports_and_ledgers_unchanged=True,
                           clone_method='cp -a --reflink=auto; same-filesystem XFS reflink / ext4 copy',
                           scope='matched disposable clone; strict durability; no automatic write retries',
                           normal_gc_settings=dict(self.config['garbage_collector']))

    def batch_object_key(self, index):
        return 'commit-comparison/' + scale.source_key(index)

    def finish(self):
        if not unchanged(self.source_evidence):
            self.report['status'] = 'FAIL'
            self.report.setdefault('error', 'Source fixture evidence changed')
        self.report['original_reports_and_ledgers_unchanged'] = unchanged(self.source_evidence)
        super().finish()

    def reconcile(self):
        self.start('reconcile-existing-population')
        self.authenticate()
        self.phase_run('all-acknowledged-version-hashes-after-restart', self.verify_all)
        self.phase_run('ambiguous-write-outcomes', self.resolve_ambiguous)
        self.phase_run('complete-namespace-ledger-comparison', self.snapshot_verify)
        assert not self.fault, self.fault
        self.report['status'] = 'PASS'

    def telemetry(self):
        self.authenticate()
        status, body = self.call('GET', '/maintenance/health', record=False)
        assert status == 200, f'Health telemetry HTTP {status}'
        health = json.loads(body)
        status, body = self.call('GET', '/maintenance/gc/status', record=False)
        assert status == 200, f'GC telemetry HTTP {status}'
        result = dict(elapsed_seconds=time.monotonic() - self.started, phase=self.phase,
                      health=health, gc=json.loads(body))
        if self.cgroup:
            result['cgroup'] = {name: (self.cgroup / name).read_text().strip()
                                for name in ('memory.current', 'memory.stat', 'memory.events',
                                             'memory.pressure', 'io.pressure')
                                if (self.cgroup / name).exists()}
        return result

    def wait_for_scheduled_gc(self):
        deadline = time.monotonic() + self.args.gc_wait_seconds
        with (self.root / 'gc-wait-samples.jsonl').open('x') as stream:
            while time.monotonic() < deadline:
                sample = self.telemetry()
                stream.write(json.dumps(sample) + '\n')
                stream.flush()
                if sample['gc']['is_running']:
                    assert not sample['gc']['is_manual_run'], 'Unexpected manual GC'
                    return sample
                time.sleep(2)
        raise AssertionError('Normal scheduled GC did not start before the bounded wait deadline')

    def ingest(self, start, count, require_gc=False):
        before = self.wait_for_scheduled_gc() if require_gc else self.telemetry()
        stopped, errors, observations = threading.Event(), [], []
        def observe():
            try:
                with (self.root / f'health-{start}.jsonl').open('x') as stream:
                    while not stopped.is_set():
                        sample = self.telemetry()
                        observations.append((sample['gc']['is_running'], sample['gc']['start_timestamp']))
                        stream.write(json.dumps(sample) + '\n')
                        stream.flush()
                        if stopped.wait(5):
                            return
            except BaseException as error:
                errors.append(error)
        observer = threading.Thread(target=observe)
        observer.start()
        began = time.monotonic()
        acknowledged_before = self.counts['source_objects']
        try:
            intervals = ((index, min(start + count, index + self.args.batch_size))
                         for index in range(start, start + count, self.args.batch_size))
            for _ in scale.bounded_map(self.upload_batch, intervals, self.args.batch_clients):
                pass
        finally:
            stopped.set()
            observer.join(self.args.request_timeout_seconds + 5)
        assert not observer.is_alive(), 'Telemetry observer did not terminate'
        assert not errors, f'Telemetry failed: {errors}'
        assert self.counts['source_objects'] - acknowledged_before == count, 'Fixed-load acknowledgement count mismatch'
        elapsed = time.monotonic() - began
        if require_gc:
            assert any(running for running, _ in observations), 'No scheduled GC overlapped upload observations'
            deadline = time.monotonic() + self.args.gc_wait_seconds
            while time.monotonic() < deadline:
                after = self.telemetry()
                if not after['gc']['is_running']:
                    assert after['gc']['start_timestamp'] == before['gc']['start_timestamp'], 'Different GC cycle observed'
                    assert after['gc']['total_errors'] == 0 and not after['gc'].get('last_error'), after['gc']
                    break
                time.sleep(2)
            else:
                raise AssertionError('Overlapped scheduled GC did not complete')
        return dict(new_objects=count, upload_elapsed_seconds=round(elapsed, 3),
                    objects_per_second=round(count / elapsed, 3), strict=True,
                    scheduled_gc_required=require_gc,
                    scheduled_gc_overlap=any(running for running, _ in observations),
                    scheduled_gc_overlap_samples=sum(running for running, _ in observations),
                    telemetry_samples=len(observations), automatic_write_retries=False)

    def compare(self):
        self.start('matched-population')
        self.authenticate()
        self.phase_run('cloned-namespace-matches-reconciled-population', self.snapshot_verify)
        self.phase_run('strict-initial-load', lambda: self.ingest(0, self.args.objects))
        self.phase_run('strict-with-normal-scheduled-gc', lambda: self.ingest(self.args.objects, self.args.objects, True))
        self.phase_run('every-known-version-hashed', lambda: admission.Admission.verify_all(self))
        self.phase_run('complete-quiescent-namespace-ledger', self.snapshot_verify)
        self.stop()
        self.start('matched-population-after-sigkill')
        self.phase_run('every-known-version-hashed-after-sigkill', lambda: admission.Admission.verify_all(self))
        self.phase_run('complete-namespace-ledger-after-sigkill', self.snapshot_verify)
        assert not self.fault, self.fault
        self.report['status'] = 'PASS'


def run_stage(args, fixture, reconcile=False):
    test = Clone(args, fixture, reconcile)
    try:
        (test.reconcile if reconcile else test.compare)()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()
    assert test.report['status'] == 'PASS'
    return test.root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('fixture', 'root', 'baseline', 'candidate'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--source-member-parent', type=Path, action='append', required=True,
                        help='Owned parents of the original fixture; never used as copy destinations')
    parser.add_argument('--objects', type=int, default=2048, help='New objects per phase per build')
    parser.add_argument('--port', type=int, default=57601)
    parser.add_argument('--deadline-seconds', type=int, default=21600)
    parser.add_argument('--gc-wait-seconds', type=int, default=7200)
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--free-floor-gib', type=int, default=128)
    parser.add_argument('--max-filesystem-growth-gib', type=int, default=96)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('Do not disable fixture safety checks with -O')
    assert 1 <= args.objects <= 100000
    assert args.request_timeout_seconds == 300, 'Keep the original HTTP deadline'
    assert args.free_floor_gib >= 128, 'Do not weaken the reserved-space floor'
    assert 1 <= args.max_filesystem_growth_gib <= 128, 'Bound disposable clone growth'
    args.fixture, args.root = args.fixture.resolve(strict=True), args.root.resolve()
    args.baseline, args.candidate = args.baseline.resolve(strict=True), args.candidate.resolve(strict=True)
    assert not args.root.exists(), 'Never overwrite prior evidence'
    for parent in args.member_parent:
        scale.require_storage_filesystem(parent)
    source_config, source_roots, _ = source_description(args.fixture, args.source_member_parent)
    source_report = json.loads((args.fixture / 'results.json').read_text())
    args.io_backend = recheck.source_io_backend(args.fixture, source_report)
    engine_hashes = dict(baseline=scale.base.digest(args.baseline.read_bytes()),
                         candidate=scale.base.digest(args.candidate.read_bytes()))
    assert engine_hashes['baseline'] == source_report['engine_sha256'], 'Reconcile using the exact failed build first'
    pool = source_config['storage_pool.A']
    args.data_shards, args.parity_shards = int(pool['data_shards']), int(pool['parity_shards'])
    assert len(args.member_parent) == len(source_roots), 'Destination/source EC member count mismatch'
    for parent in args.source_member_parent:
        scale.require_storage_filesystem(parent)
    scale.require_storage_filesystem(args.root.parent)
    assert args.root.parent.stat().st_dev == args.member_parent[0].resolve(strict=True).stat().st_dev, (
        'Comparison evidence must remain on the first destination member filesystem')
    assert not args.root.is_relative_to(args.fixture), 'Never place comparisons inside source evidence'
    scale.independent_member_devices(args.member_parent)
    original = evidence_hashes(args.fixture)
    os.umask(0o077)
    args.root.mkdir(mode=0o700)
    summary = dict(status='RUNNING', objects_per_phase=args.objects,
                   engine_sha256=engine_hashes, requested_io_backend=args.io_backend,
                   topology=dict(data_shards=args.data_shards, parity_shards=args.parity_shards,
                                 members=len(source_roots)),
                   baseline_first=True, cold_cache_control=False,
                   note='Fixed-order same-population comparison; filesystem caches are not disabled. '
                        'Both builds run 2048 objects per phase by default, then full integrity/restart gates.', stages=[])
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Operator signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        reconciled = None
        for index, (name, binary) in enumerate((('reconciled', args.baseline), ('baseline', args.baseline), ('candidate', args.candidate))):
            stage = deepcopy(args)
            stage.root, stage.binary, stage.port = args.root / name, binary, args.port + index * 2
            stage.probes, stage.churn_mutations = 1, 0
            stage.batch_size, stage.batch_clients, stage.batch_concurrency = 64, 8, 16
            source = args.fixture if reconciled is None else reconciled
            # Later clone roots can be children of owned member parents, not mount roots.
            if reconciled:
                config = recheck.configparser.ConfigParser(interpolation=None)
                config.read(reconciled / 'n1_config.ini')
                stage.source_member_parent = [Path(path).parent for path in config['storage_pool.A']['roots'].split(',')]
            result = run_stage(stage, source, index == 0)
            expected_hash = engine_hashes['candidate' if name == 'candidate' else 'baseline']
            witness = stage_witness(result, expected_hash, args.io_backend, args.data_shards, args.parity_shards)
            if index == 0:
                reconciled = result
            summary['stages'].append(dict(name=name, results=str(result / 'results.json'),
                                         engine_sha256=expected_hash, requested_io_backend=args.io_backend,
                                         topology=witness['topology']))
        summary['status'] = 'PASS'
    except BaseException as error:
        summary.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        summary['original_evidence_unchanged'] = unchanged(original)
        if not summary['original_evidence_unchanged']:
            summary.update(status='FAIL', error='Original evidence changed')
        (args.root / 'results.json').write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
