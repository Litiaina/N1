#!/usr/bin/env python3
"""Isolated EC proof of SCHEDULED reclamation, never manual gc/start.

Defaults to EC 1+1 on two independent physical drives; EC 3+1 is also supported.
Creates only fresh child directories on independent mounted data volumes.
Keeps all evidence, including failed runs. Never contacts an existing engine,
formats a device, retries an upload, or follows a fixture symlink outside its
own member. Synthetic candidates are explicitly distinguished from real public
API uploads and a real standard-multipart SIGKILL interruption.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sys
import time
import traceback
import urllib.parse

sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location('rc_stress_gc', Path(__file__).with_name('n1-rc-stress.py'))
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)

_CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
_GENERATION = re.compile(r'^(\d+)\.d(\d+)\.([0-9A-HJKMNP-TV-Z]{26})$')
FILE_TTL_SECONDS = 900
TRASH_TTL_SECONDS = 900
OLD_SECONDS = 3600


def new_ulid():
    value = (int(time.time() * 1000) << 80) | secrets.randbits(80)
    result = ''
    for _ in range(26):
        result = _CROCKFORD[value & 31] + result
        value >>= 5
    return result


def require_data_volume(path):
    path = path.resolve(strict=True)
    mount = next((candidate for candidate in (path, *path.parents) if os.path.ismount(candidate)), None)
    assert mount is not None and mount != Path('/'), 'A mounted data filesystem, not the OS filesystem, is required'
    assert mount.stat().st_dev == path.stat().st_dev, 'Unexpected nested member device'
    return path


def backing_device(path):
    device = path.stat().st_dev
    link = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}'
    assert link.exists(), 'This proof needs an identifiable real block filesystem'
    block = link.resolve(strict=True)
    return block.parent if (block / 'partition').exists() else block


def require_inside(path, member):
    """Do not let even an injected child symlink escape a disposable member."""
    resolved = path.resolve(strict=True)
    member = member.resolve(strict=True)
    assert resolved != member and member in resolved.parents, 'Fixture path escapes its owned member'
    return resolved


def age_trash_generation(path, member, old_unix_seconds):
    require_inside(path, member)
    assert not path.is_symlink() and path.is_dir(), 'Trash generation must be a real owned directory'
    match = _GENERATION.fullmatch(path.name)
    assert match is not None, 'Unknown trash generation layout; preserve rather than guess'
    destination = path.with_name(f'{old_unix_seconds * 1_000_000_000}.d{match[2]}.{match[3]}')
    assert not destination.exists(), 'Do not overwrite a trash generation'
    path.rename(destination)
    return destination


def validate_automatic_status(status):
    assert status.get('is_manual_run') is False, 'A manually started GC cannot satisfy this proof'
    assert status.get('total_errors', 0) == 0, f'Scheduled collector reported errors: {status}'


def topology_counts(data_shards, parity_shards, member_count):
    assert 1 <= data_shards <= 32 and 0 <= parity_shards <= 32, 'Invalid EC shard counts'
    assert member_count == data_shards + parity_shards, 'Exactly K+P independent member filesystems are required'
    return member_count, min(parity_shards + 1, member_count)


class ScheduledGcProof(base.Stress):
    def __init__(self, args):
        self.member_count, self.namespace_replica_count = topology_counts(
            args.data_shards, args.parity_shards, len(args.member_parent))
        for parent in [args.root.parent, *args.member_parent]:
            require_data_volume(parent)
        devices = [backing_device(parent) for parent in args.member_parent]
        assert len(set(devices)) == self.member_count, (
            'Separate partitions of one drive are not independent physical members')
        super().__init__(args)
        self.config['garbage_collector'].update(
            cleanup_interval_seconds=str(args.cleanup_interval_seconds),
            file_ttl_seconds=str(FILE_TTL_SECONDS),
            trash_retention_seconds=str(TRASH_TTL_SECONDS))
        # Legacy settings are deliberately enabled/short: neither may silently
        # reintroduce an automatic whole-object health/healing traversal.
        self.config['auto_healing'].update(enabled='true', initial_scan_delay_seconds='1',
                                           availability_scan_interval_seconds='1',
                                           checksum_scrub_interval_seconds='1',
                                           topology_recovery_stability_seconds='0')
        self.write_config()
        self.report.update(
            proof='scheduled GC: expired trash AND orphan versions',
            no_manual_gc_requests=True,
            file_ttl_seconds=FILE_TTL_SECONDS,
            trash_retention_seconds=TRASH_TTL_SECONDS,
            cleanup_interval_seconds=args.cleanup_interval_seconds,
            harness_sha256=base.digest(Path(__file__).read_bytes()),
            fixture_helper_sha256=base.digest(Path(__file__).with_name('n1-rc-stress.py').read_bytes()),
            physical_block_devices=list(map(str, devices)),
            expected_namespace_replicas=self.namespace_replica_count,
            synthetic_age_injection='Owned version-directory mtimes (including referenced history) and one expired trash generation; no committed NDX/payload edits',
            scope=f'Isolated {self.member_count}-filesystem EC {args.data_shards}+{args.parity_shards} loopback HTTPS; no production endpoints or physical power-loss claim')
        self.gc_observations = []
        self.health_observations = []
        self.live_acks = []

    def call(self, method, route, *args, **kwargs):
        assert not (method != 'GET' and '/maintenance/' in route), (
            'This scheduled proof must never start/stop GC, health, or repair manually')
        return super().call(method, route, *args, **kwargs)

    def add_ack(self, key, version, payload):
        ack = (self.token, key, version, base.digest(payload), len(payload))
        self.live_acks.append(ack)
        # Tokens are deliberately excluded from the durable test witness.
        with (self.root / 'acknowledgements.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(object_key=key, version_id=version,
                                         sha256=ack[3], size=ack[4])) + '\n')
        return ack

    def put_object(self, key, seed, size=16384, retain=True):
        body = base.payload(seed, size)
        reply = self.api('POST', '/upload/one-shot/' + urllib.parse.quote(key, safe='/') + '?durability=strict', body)
        assert reply['object_key'] == key and reply['version_id']
        if retain:
            self.add_ack(key, reply['version_id'], body)
        return reply['version_id'], body

    def physical_fragments(self):
        result = []
        for member in self.roots:
            candidates = [path for path in member.glob('@f_*') if path.is_dir() and not path.is_symlink()]
            assert len(candidates) == 1, 'Fixture must have exactly one physical fragment on every member'
            result.append(require_inside(candidates[0], member))
        return result

    def version_directories(self, version):
        found = []
        for member, fragment in zip(self.roots, self.physical_fragments()):
            for path in (fragment / '.objects').glob(f'*/*/@o_*/{version}'):
                assert path.is_dir() and not path.is_symlink()
                found.append(require_inside(path, member))
        return found

    def version_summaries(self, version):
        return [require_inside(path, member)
                for member, fragment in zip(self.roots, self.physical_fragments())
                for path in (fragment / '.objects').glob(f'*/*/@o_*/{version}.*.ndx')
                if path.is_file() and not path.is_symlink()]

    def trash_generations(self):
        return {path for fragment in self.physical_fragments()
                for path in (fragment / '.trash').glob('*') if path.is_dir() and not path.is_symlink()}

    def multipart(self, key, durable):
        part_size = 1024 * 1024
        params = dict(content_type='application/octet-stream', total_size_bytes=2 * part_size,
                      expected_parts=2, part_size_bytes=part_size)
        route = '/upload/multipart/' + ('durable/' if durable else '')
        init = self.api('POST', route + 'init/' + urllib.parse.quote(key, safe='/'), params)
        first = base.payload(key + '-part0', part_size)
        status, response = self.call('PUT', route + init['version_id'] + '/0?durability=strict', first)
        assert status in (200, 201), (status, response[:350])
        directories = self.version_directories(init['version_id'])
        assert len(directories) == self.member_count and all(any(path.iterdir()) for path in directories), (
            'Need real persisted shard files on every independent member')
        return dict(key=key, durable=durable, route=route, init=init, params=params,
                    first=first, directories=directories)

    def finalize(self, upload):
        second = base.payload(upload['key'] + '-part1', upload['params']['part_size_bytes'])
        version = upload['init']['version_id']
        status, response = self.call('PUT', upload['route'] + version + '/1?durability=strict', second)
        assert status in (200, 201), (status, response[:350])
        reply = self.api('POST', upload['route'] + version + '/' + upload['init']['session_id'] +
                         '/finalize?' + urllib.parse.urlencode({'object_key': upload['key']}), upload['params'])
        assert reply['version_id'] == version
        self.add_ack(upload['key'], version, upload['first'] + second)

    def health(self):
        health = self.api('GET', '/maintenance/health?' + urllib.parse.urlencode(
            {'refresh': 'true', 'fragment': self.fragment}))
        base.verify_io_backend(health, self.args)
        assert health['io_control']['process']['pid'] == self.process.pid, 'Wrong engine telemetry'
        healer = health['maintenance']['auto_healing']
        assert healer['last_availability_scan_at'] is None and healer['last_checksum_scrub_at'] is None, (
            'Unsolicited whole-object healing scan')
        assert not health['maintenance']['repair_running'], 'Unsolicited explicit repair job'
        # The monitor documents absence of a full consistency walk in its result.
        assert health['consistency']['total_index_records_scanned'] == 0, (
            'A full object consistency scan ran without deep_scan=true')
        self.health_observations.append(health)
        return health

    def observe(self):
        status = self.api('GET', '/maintenance/gc/status')
        validate_automatic_status(status)
        self.gc_observations.append(status)
        with (self.root / 'gc-observations.jsonl').open('a') as stream:
            stream.write(json.dumps(status) + '\n')
        return status

    def wait_for_automatic(self, predicate, label, minimum_completed_cycles=1):
        start = time.monotonic()
        baseline = self.health()
        before = next(row['completed'] for row in baseline['io_scheduler']['classes']
                      if row['class'] == 'background_gc')
        while time.monotonic() - start < self.args.reclaim_timeout_seconds:
            self.observe()
            health = self.health()
            completed = next(row['completed'] for row in health['io_scheduler']['classes']
                             if row['class'] == 'background_gc')
            if predicate() and completed - before >= minimum_completed_cycles:
                return {'label': label, 'scheduled_background_gc_completions': completed - before,
                        'elapsed_seconds': round(time.monotonic() - start, 3)}
            time.sleep(.25)
        raise AssertionError(f'Scheduled GC did not finish {label}; manual gc/start was never used')

    def synthetic_orphans(self):
        now = time.time()
        entries = []
        for label, modified in [('expired', now - OLD_SECONDS), ('young', now),
                                ('future', now + OLD_SECONDS), ('unknown-version', now - OLD_SECONDS)]:
            object_id = new_ulid()
            digest = base.digest(object_id.encode())
            version = new_ulid() if label != 'unknown-version' else 'preserve-unknown-entry'
            for member, fragment in zip(self.roots, self.physical_fragments()):
                parent = fragment / '.objects' / digest[:2] / digest[2:4] / ('@o_' + object_id)
                directory = parent / version
                directory.mkdir(parents=True)
                require_inside(directory, member)
                marker = directory / 'part_0_shard_0.shard'
                marker.write_bytes(base.payload(label, 4096))
                os.utime(directory, (modified, modified))
                os.utime(parent, (now - OLD_SECONDS, now - OLD_SECONDS))
                entries.append(dict(label=label, directory=directory, marker=marker,
                                    sha256=base.digest(marker.read_bytes())))
        return entries

    @staticmethod
    def assert_protected(entries):
        for entry in entries:
            assert entry['marker'].is_file() and base.digest(entry['marker'].read_bytes()) == entry['sha256'], (
                f"Protected candidate changed: {entry['label']}")

    def run(self):
        self.phase = 'prepare-public-api-data'
        self.start('initial')
        self.fragment, self.token = self.create('scheduled-gc-' + secrets.token_hex(5))
        for index in range(4):
            for version in range(3):
                self.put_object(f'live/object-{index}.bin', f'live-{index}-{version}')
        expired_versions = [self.put_object('expired.bin', f'expired-{i}', retain=False)[0] for i in range(2)]
        before = self.trash_generations()
        self.api('POST', '/posix/trash', {'path': 'expired.bin'})
        expired_generations = self.trash_generations() - before
        assert len(expired_generations) == self.namespace_replica_count, (
            'Expected P+1 real NCX trash replicas for this pool')
        self.put_object('retained.bin', 'retained-0')
        self.put_object('retained.bin', 'retained-1')
        self.api('POST', '/posix/trash', {'path': 'retained.bin'})
        interrupted = self.multipart('interrupted-standard.bin', False)
        interrupted_live = self.multipart('live/object-0.bin', False)
        # An orphan beneath an object that still has committed historical
        # versions must also be reclaimed; a live object is not a blanket pin.
        committed_live_dirs = self.version_directories(self.live_acks[0][2])
        assert {path.parent for path in interrupted_live['directories']} == {
            path.parent for path in committed_live_dirs}, 'New version must share the real live object directory'
        interrupted_uploads = [interrupted, interrupted_live]
        self.report['ordinary_upload_sigkill'] = [dict(
            object_key=upload['key'], version_id=upload['init']['version_id'], acknowledged_part=0,
            version_never_published=True, abandoned_shard_directories=self.member_count,
            existing_live_object=upload is interrupted_live) for upload in interrupted_uploads]
        expired_payloads = [path for version in expired_versions for path in self.version_directories(version)]
        assert len(expired_payloads) == len(expired_versions) * self.member_count
        expired_summaries = [path for version in expired_versions for path in self.version_summaries(version)]
        assert len(expired_summaries) == len(expired_versions) * self.member_count
        self.stop()  # Real SIGKILL; memory-only standard session must be lost.
        old = time.time() - OLD_SECONDS
        for path in [directory for upload in interrupted_uploads for directory in upload['directories']]:
            os.utime(path, (old, old))
        expired_trash_paths = []
        for path in expired_generations:
            member = next(member for member in self.roots if member in path.parents)
            expired_trash_paths.append(age_trash_generation(path, member, int(old)))
        synthetic = self.synthetic_orphans()
        expired = [entry for entry in synthetic if entry['label'] == 'expired']
        protected = [entry for entry in synthetic if entry['label'] != 'expired']
        # Hold references to the small subset of committed immutable files, not
        # an engine-sized inventory. This fixture has only 16 committed versions.
        live_versions = {ack[2] for ack in self.live_acks}
        retained_payloads = [path for version in live_versions for path in self.version_directories(version)]
        assert len(retained_payloads) == len(live_versions) * self.member_count
        for directory in retained_payloads:
            # Preservation must be due to live/trash reachability, not simply
            # because these committed versions are younger than the TTL.
            os.utime(directory, (old, old))
        immutable = {}
        for member, fragment in zip(self.roots, self.physical_fragments()):
            for path in (fragment / '.objects').glob('*/*/@o_*/*'):
                if path.is_file() and not path.is_symlink() and path.name.split('.')[0] in live_versions:
                    require_inside(path, member)
                    immutable[path] = (path.stat().st_ino, path.stat().st_mtime_ns, base.digest(path.read_bytes()))
                elif path.is_dir() and not path.is_symlink() and path.name in live_versions:
                    for payload in path.rglob('*'):
                        if payload.is_file() and not payload.is_symlink():
                            require_inside(payload, member)
                            immutable[payload] = (payload.stat().st_ino, payload.stat().st_mtime_ns,
                                                  base.digest(payload.read_bytes()))
        assert immutable, 'No committed immutable inventory was recorded'
        (self.root / 'immutable-reference-inventory.json').write_text(json.dumps([
            dict(path=str(path), inode=expected[0], mtime_ns=expected[1], sha256=expected[2])
            for path, expected in immutable.items()], indent=2))
        self.phase = 'automatic-reclamation-after-sigkill'
        self.start('after-real-sigkill')
        active = [self.multipart('active-standard.bin', False), self.multipart('active-durable.bin', True)]
        active_markers = []
        for upload in active:
            for directory in upload['directories']:
                for path in directory.iterdir():
                    if path.is_file() and not path.is_symlink():
                        active_markers.append(dict(label=upload['key'], marker=path,
                                                   sha256=base.digest(path.read_bytes())))
                # Even an old shard-directory mtime cannot override an active
                # standard session or persisted durable manifest/receipt.
                os.utime(directory, (old, old))
        assert active_markers, 'Active upload preservation must include real shard bytes'
        paths_to_remove = ([entry['directory'] for entry in expired] +
                           [directory for upload in interrupted_uploads for directory in upload['directories']] +
                           expired_payloads + expired_summaries + expired_trash_paths)
        def all_reclaimed():
            self.report['reclamation_paths_remaining'] = [str(path) for path in paths_to_remove if path.exists()]
            return not self.report['reclamation_paths_remaining']
        phase = self.wait_for_automatic(all_reclaimed,
                                        'expired trash + synthetic orphans + SIGKILL new-object and live-object version orphans', 2)
        self.phases.append(phase)
        self.assert_protected(protected + active_markers)
        assert set(self.list_all('live')) == {f'live/object-{index}.bin' for index in range(4)}
        assert not self.list_all('interrupted-standard.bin'), 'Interrupted standard upload was published'
        for upload in interrupted_uploads:
            status = self.api('GET', '/upload/multipart/' + upload['init']['version_id'])
            assert not status['uploaded_parts'], 'Volatile standard session survived SIGKILL'
        assert {version['version_id'] for version in self.versions('live/object-0.bin')} == {
            ack[2] for ack in self.live_acks if ack[1] == 'live/object-0.bin'}, (
                'Interrupted overwrite changed the committed history of the live object')
        for path, expected in immutable.items():
            assert path.is_file(), f'Collector lost referenced immutable file: {path}'
            actual = (path.stat().st_ino, path.stat().st_mtime_ns, base.digest(path.read_bytes()))
            assert actual == expected, f'GC mutated a referenced committed NDX/payload: {path}'
        self.api('POST', '/posix/recover', {'path': 'retained.bin'})
        self.parallel(self.read_ack, list(self.live_acks), 4)
        for upload in active:
            status = self.api('GET', upload['route'] + upload['init']['version_id'])
            assert status['uploaded_parts'] == [0], status
            self.finalize(upload)
        self.parallel(self.read_ack, list(self.live_acks), 4)
        self.phase = 'final-restart-and-all-historical-hashes'
        self.stop()
        self.start('final-restart')
        self.parallel(self.read_ack, list(self.live_acks), 4)
        self.wait_for_automatic(lambda: True, 'scheduled collector still runs after restart')
        self.assert_protected(protected)
        self.observe()
        for member in self.roots:
            for directory, dirs, files in os.walk(member, followlinks=False):
                for name in dirs + files:
                    path = Path(directory) / name
                    if path.is_symlink():
                        require_inside(path, member)
        logs = '\n'.join(path.read_text(errors='replace') for path in self.root.glob('engine-*.log'))
        assert 'panicked at' not in logs and 'memory allocation of' not in logs
        assert not self.fault, self.fault
        self.report.update(status='PASS', elapsed_seconds=round(time.monotonic() - self.started, 3),
                           acknowledged_live_versions=len(self.live_acks),
                           all_live_historical_versions_sha256_verified=True,
                           expired_trash_versions_reclaimed=len(expired_versions),
                           expired_unreferenced_ndx_summaries_reclaimed=len(expired_summaries),
                           synthetic_expired_orphan_directories_reclaimed=len(expired),
                           real_interrupted_standard_orphan_directories_reclaimed=len(interrupted_uploads) * self.member_count,
                           live_object_interrupted_version_orphans_reclaimed=self.member_count,
                           protected_young_future_unknown_candidates=len(protected),
                           protected_standard_and_durable_uploads_finalized_and_verified=2,
                           immutable_reference_files_unchanged=len(immutable),
                           referenced_version_directories_aged_past_orphan_ttl=len(retained_payloads),
                           background_gc_accounting_observed=True,
                           unsolicited_full_health_or_healing_scans=False)

    def finish(self):
        # A killed child may remain stuck in kernel I/O. Failure to reap it
        # within the existing 15-second limit must not discard the original
        # failure or skip the durable test witness.
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

        try:
            self.stop()  # Stress.stop clears self.process before its wait().
        except Exception as error:
            record_cleanup_error('stop-owned-engine', error)
        self.stop_monitor.set()
        try:
            with (self.root / 'health-observations.jsonl').open('w') as stream:
                for health in self.health_observations:
                    stream.write(json.dumps(health) + '\n')
        except Exception as error:
            record_cleanup_error('write-health-observations', error)
        try:
            super().finish()
        except Exception as error:
            record_cleanup_error('write-results-and-resources', error)
            print(f'Proof evidence finalization failed: {error}', file=sys.stderr, flush=True)
        finally:
            self.stop_monitor.set()
            self.monitor.join(timeout=3)
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
    parser.add_argument('--port', type=int, default=57961)
    parser.add_argument('--deadline-seconds', type=int, default=600)
    parser.add_argument('--request-timeout-seconds', type=int, default=90)
    parser.add_argument('--reclaim-timeout-seconds', type=int, default=90)
    parser.add_argument('--cleanup-interval-seconds', type=int, default=3)
    args = parser.parse_args()
    assert 1 <= args.cleanup_interval_seconds <= 10
    assert args.deadline_seconds < FILE_TTL_SECONDS, 'Protected young candidates must outlive this whole bounded proof'
    topology_counts(args.data_shards, args.parity_shards, len(args.member_parent))
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'Operator signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    test = ScheduledGcProof(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
