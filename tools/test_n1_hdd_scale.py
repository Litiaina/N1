"""Harness-only safety/boundedness checks. No engine or existing data touched."""
from collections import Counter, defaultdict
from contextlib import closing
from email import policy
from email.parser import BytesParser
import importlib.util
import io
import json
import sqlite3
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
import urllib.parse

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('hdd_scale', Path(__file__).with_name('n1-hdd-scale.py'))
scale = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scale)
benchmark_spec = importlib.util.spec_from_file_location('hdd_benchmark', Path(__file__).with_name('n1-hdd-benchmark.py'))
benchmark = importlib.util.module_from_spec(benchmark_spec)
benchmark_spec.loader.exec_module(benchmark)
recheck_spec = importlib.util.spec_from_file_location('hdd_recheck', Path(__file__).with_name('n1-hdd-recheck.py'))
recheck = importlib.util.module_from_spec(recheck_spec)
recheck_spec.loader.exec_module(recheck)
admission_spec = importlib.util.spec_from_file_location('hdd_admission', Path(__file__).with_name('n1-hdd-admission.py'))
admission = importlib.util.module_from_spec(admission_spec)
admission_spec.loader.exec_module(admission)


class HarnessChecks(unittest.TestCase):
    def test_snapshot_progress_is_not_completion_and_counts_are_exact(self):
        frames = [dict(kind='entry', object_key='a', version_id='v1'),
                  dict(kind='progress', scanned_entries=1024, entry_count=1),
                  dict(kind='entry', object_key='b', version_id='v2'),
                  dict(kind='progress', scanned_entries=2048, entry_count=2),
                  dict(kind='complete', entry_count=2)]
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('CREATE TABLE seen(object_key TEXT PRIMARY KEY, version_id TEXT) WITHOUT ROWID')
            def consume(values):
                body = io.BytesIO(b''.join(json.dumps(frame).encode() + b'\n' for frame in values))
                return scale.consume_snapshot(body, db)
            report = consume(frames)
            self.assertEqual(report['entries'], 2)
            self.assertEqual(report['progress_frames'], 2)
            self.assertEqual(report['scanned_entries'], 2048)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM seen').fetchone()[0], 2)
            self.assertEqual(consume([frames[0], dict(kind='complete', entry_count=1)])['progress_frames'], 0)
            for invalid in (frames[:-1], frames + [frames[1]],
                            [frames[0], frames[1], frames[1]],
                            [frames[0], dict(kind='progress', scanned_entries=1024, entry_count=0)],
                            [frames[0], dict(kind='complete', entry_count=2)],
                            [dict(kind='complete', entry_count=False)],
                            [dict(kind='unexpected')]):
                with self.subTest(invalid=invalid), self.assertRaises(AssertionError):
                    consume(invalid)
            with self.assertRaises(sqlite3.IntegrityError):
                consume([frames[0], frames[0]])
            with self.assertRaisesRegex(AssertionError, 'truncated'):
                scale.consume_snapshot(io.BytesIO(b'{"kind":"complete","entry_count":0}'), db)
            with self.assertRaisesRegex(AssertionError, 'Oversized'):
                scale.consume_snapshot(io.BytesIO(b'x' * 65537), db)

    def test_flat_source_keys_keep_the_million_names_in_one_directory(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(flat_folder=True)
        for index in (0, 999, 1000, 500000, 999999, 1000000):
            key = test.batch_object_key(index)
            self.assertEqual(key, f'projects/million/file-{index:09}.bin')
            self.assertEqual(Path(key).parent, Path('projects/million'))
            self.assertEqual(scale.source_key(index),
                             f'projects/project-{index // 1000:06}/file-{index:09}.bin')

    def test_flat_checkpoint_windows_are_lexical_prefixes_not_fake_subfolders(self):
        self.assertEqual(list(scale.listing_prefixes(1000000, True)), [
            ('projects/million/file-000000', 0, 1000),
            ('projects/million/file-000500', 500000, 501000),
            ('projects/million/file-000999', 999000, 1000000)])
        self.assertEqual(list(scale.listing_prefixes(1001, True)), [
            ('projects/million/file-000000', 0, 1000),
            ('projects/million/file-000001', 1000, 1001)])
        self.assertEqual(list(scale.listing_prefixes(2)), [('projects/project-000000', 0, 2)])
        self.assertEqual(scale.population_checkpoints(1000000), [1000, 10000, 100000, 500000, 1000000])

    def test_flat_prefix_pagination_streams_and_rejects_bad_order_or_cursor(self):
        test = scale.Scale.__new__(scale.Scale)
        test.authenticate = Mock()
        prefix = 'projects/million/file-000000'
        def page(method, route):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(route).query, keep_blank_values=True)
            self.assertEqual((method, urllib.parse.urlsplit(route).path), ('GET', '/objects'))
            self.assertEqual(query['delimiter'], [''])
            self.assertEqual(query['prefix'], [prefix])
            limit = int(query['limit'][0])
            self.assertEqual(limit, 73)
            cursor = query.get('continuation_token', [None])[0]
            start = 0 if cursor is None else int(Path(cursor).stem.split('-')[-1]) + 1
            end = min(start + limit, 151)
            return {'entries': [{'record': scale.source_key(i, True)[len(prefix):], 'kind': 'file'}
                                for i in range(start, end)],
                    'continuation_token': scale.source_key(end-1, True) if end < 151 else None}
        test.api = Mock(side_effect=page)
        stream = test.prefix_keys(prefix)
        self.assertEqual(next(stream), scale.source_key(0, True))
        self.assertEqual(test.api.call_count, 1, 'must not prefetch the entire prefix')
        self.assertEqual(list(stream), [scale.source_key(i, True) for i in range(1, 151)])
        self.assertEqual(test.api.call_count, 3)
        for broken in ({'entries': [{'record': '000.bin', 'kind': 'file'}] * 2, 'continuation_token': None},
                       {'entries': [{'record': '000.bin', 'kind': 'file'}], 'continuation_token': 'skipped'}):
            test.api = Mock(return_value=broken)
            with self.assertRaises(AssertionError):
                list(test.prefix_keys(prefix))

    def test_flat_listing_probe_checks_bounded_windows_and_reports_scope(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(flat_folder=True)
        windows = {prefix: (start, end) for prefix, start, end in scale.listing_prefixes(1000000, True)}
        test.list_all = Mock(side_effect=AssertionError('Do not collect the million-object directory'))
        test.prefix_keys = Mock(side_effect=lambda prefix, limit: (
            scale.source_key(index, True) for index in range(*windows[prefix])))
        report = test.listing_probe(1000000)
        self.assertEqual(report['population'], 1000000)
        self.assertEqual(report['scope'], 'lexical filename prefixes within one flat folder')
        self.assertEqual([sample['objects'] for sample in report['sampled_prefixes_all_pages']], [1000] * 3)
        self.assertEqual(test.prefix_keys.call_count, 3)
        test.list_all.assert_not_called()
        def unexpected(prefix, limit):
            yield 'projects/million/unexpected.bin'
            raise AssertionError('must stop on the first unexpected key, without collecting the rest')
        test.prefix_keys = unexpected
        with self.assertRaisesRegex(AssertionError, 'Unexpected paginated LIST key'):
            test.listing_probe(1000000)

    def test_population_checkpoint_plan_is_logarithmic_beyond_a_million(self):
        self.assertEqual(scale.population_checkpoints(2), [2])
        self.assertEqual(scale.population_checkpoints(300000), [1000, 10000, 100000, 300000])
        expected = [1000, 10000, 100000, 500000, 1000000, 2000000, 5000000,
                    10000000, 20000000, 50000000, 100000000, 200000000, 500000000]
        self.assertEqual(scale.population_checkpoints(500000000), expected)
        checkpoints = scale.population_checkpoints(10**30 + 7)
        self.assertEqual(checkpoints[-1], 10**30 + 7)
        self.assertEqual(checkpoints, sorted(set(checkpoints)))
        self.assertLess(len(checkpoints), 100)

    def test_shared_physical_drive_is_rejected_before_fixture_creation(self):
        args = SimpleNamespace(member_parent=[Path('/partition-a'), Path('/partition-b')])
        with patch.object(scale, 'require_storage_filesystem'), \
                patch.object(scale, 'backing_device', return_value=Path('/sys/block/sda')), \
                patch.object(scale.base.Stress, '__init__') as create_fixture:
            with self.assertRaisesRegex(AssertionError, 'not independent physical members'):
                scale.Scale(args)
        create_fixture.assert_not_called()

    def test_four_distinct_physical_devices_are_retained_as_topology_evidence(self):
        devices = [Path('/sys/block') / name for name in ('sda', 'sdb', 'sdc', 'sdd')]
        with patch.object(scale, 'backing_device', side_effect=devices):
            self.assertEqual(scale.independent_member_devices(range(4)), devices)

    def test_scale_topology_accepts_four_independent_members_for_three_plus_one(self):
        scale.validate_topology(SimpleNamespace(data_shards=3, parity_shards=1, member_parent=[1, 2, 3, 4]))
        scale.validate_topology(SimpleNamespace(data_shards=1, parity_shards=1, member_parent=[1, 2]))
        for data, parity, count in ((3, 1, 2), (0, 1, 1), (1, -1, 1), (33, 0, 33)):
            with self.assertRaises(AssertionError):
                scale.validate_topology(SimpleNamespace(data_shards=data, parity_shards=parity,
                                                       member_parent=list(range(count))))

    def test_member_parent_rejects_os_filesystem(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(scale.os.path, 'ismount', side_effect=lambda path: path == Path('/')):
                with self.assertRaisesRegex(AssertionError, 'mounted data filesystem'):
                    scale.require_storage_filesystem(Path(temporary))

    def test_member_parent_allows_an_isolated_child_on_a_mounted_volume(self):
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary)
            child = mount / 'isolated-test'
            child.mkdir()
            with patch.object(scale.os.path, 'ismount', side_effect=lambda path: path == mount):
                scale.require_storage_filesystem(child)

    def test_space_budget_stops_for_either_reserved_space_or_filesystem_growth(self):
        root = Path('/fixture')
        gib = 1024**3
        initial = {root: 900 * gib}
        for free, floor, growth, expected in ((850, 768, 80, None),
                                             (819, 768, 80, 'growth budget'),
                                             (767, 768, 0, 'Free-space'),
                                             (819, 768, 0, None)):
            with patch.object(scale.shutil, 'disk_usage', return_value=SimpleNamespace(free=free * gib)):
                problem = scale.storage_budget_problem([root], floor, initial, growth)
                if expected is None:
                    self.assertIsNone(problem)
                else:
                    self.assertIn(expected, problem)

    def test_benchmark_keeps_reconciled_commits_separate_from_acknowledgements(self):
        reader = sqlite3.connect(':memory:')
        self.addCleanup(reader.close)
        reader.executescript('CREATE TABLE acks(size INTEGER, category TEXT);'
                            "INSERT INTO acks VALUES(512,'source'),(1024,'ambiguous_commit_not_acknowledged');")
        report = dict(status='PASS', expected_acknowledged_versions=1,
                      original_reports_and_ledgers_unchanged=True,
                      phases=[dict(name='all-acknowledged-version-hashes-after-restart', status='PASS',
                                   every_acknowledged_version_downloaded_and_hashed=1),
                              dict(name='ambiguous-write-outcomes', status='PASS', committed_and_hashed=1),
                              dict(name='complete-namespace-ledger-comparison', status='PASS')])
        self.assertEqual(admission.source_population(report, reader),
                         dict(acknowledged_versions=1, known_versions=2,
                              unacknowledged_commits=1, unacknowledged_bytes=1024))
        for field, value in [('status', 'FAIL'), ('original_reports_and_ledgers_unchanged', False),
                             ('expected_acknowledged_versions', 2)]:
            with self.assertRaises(AssertionError):
                admission.source_population(dict(report, **{field: value}), reader)
        with self.assertRaises(AssertionError):
            admission.source_population(dict(counts=dict(acknowledged_versions=2)), reader)

    def test_upload_only_run_never_starts_probe_or_churn_work(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(upload_only=True, objects=2, probes=0, churn_mutations=0)
        test.counts = Counter()
        test.report = {}
        test.master = 'fixture'
        test.roots = []
        test.fault = None
        test.start = Mock()
        test.stop = Mock()
        test.authenticate = Mock()
        test.api = Mock(return_value={'fragment_hash': 'fixture-fragment'})
        test.resource_checkpoint = Mock(return_value={})
        test.verify_all = Mock(return_value={})
        test.snapshot_verify = Mock(return_value={})
        test.listing_probe = Mock(return_value={})
        test.churn = Mock(side_effect=AssertionError('Upload-only must never mutate'))
        test.ingest_with_churn = Mock(side_effect=AssertionError('Mixed ingestion was selected'))
        test.upload_one = Mock(side_effect=AssertionError('Unexpected probe upload'))
        phases = []
        def phase(name, function):
            phases.append(name)
            return function()
        test.phase_run = phase
        def ingest(start, target):
            test.counts['source_objects'] += target - start
            return {}
        test.ingest_only = Mock(side_effect=ingest)
        test.run()
        test.ingest_only.assert_called_once_with(0, 2)
        test.ingest_with_churn.assert_not_called()
        test.churn.assert_not_called()
        test.upload_one.assert_not_called()
        self.assertFalse(any('churn' in name or 'probe' in name for name in phases))
        self.assertIn('all-version-integrity-after-final-sigkill', phases)
        self.assertEqual(test.counts['namespace_mutations'], 0)
        self.assertEqual(test.report['status'], 'PASS')

    def test_sequential_churn_starts_only_after_full_population_hashes_and_inventory(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(upload_only=False, churn_after_upload=True,
                                    objects=2, probes=256, churn_mutations=1024)
        test.counts, test.report = Counter(), {}
        test.master, test.fault = 'fixture', None
        test.start, test.stop, test.authenticate = Mock(), Mock(), Mock()
        test.api = Mock(return_value={'fragment_hash': 'fixture-fragment'})
        test.resource_checkpoint = Mock(return_value={})
        test.verify_all, test.snapshot_verify = Mock(return_value={}), Mock(return_value={})
        test.listing_probe = Mock(return_value={})
        test.ingest_with_churn = Mock(side_effect=AssertionError('Population must not run churn'))
        phases = []
        def phase(name, function):
            phases.append(name)
            return function()
        test.phase_run = phase
        def ingest(start, target):
            self.assertFalse(test.setup_probes.called)
            self.assertEqual(test.counts['namespace_mutations'], 0)
            test.counts['source_objects'] += target - start
            return {}
        test.ingest_only = Mock(side_effect=ingest)
        def probes():
            self.assertEqual(test.counts['source_objects'], 2)
            self.assertIn('complete-version-integrity-at-2', phases)
            self.assertIn('complete-quiescent-namespace-inventory', phases)
            self.assertEqual(test.counts['namespace_mutations'], 0)
            return {}
        test.setup_probes = Mock(side_effect=probes)
        def churn(cycles, concurrency):
            self.assertTrue(test.setup_probes.called)
            self.assertIn('post-population-probe-integrity', phases)
            test.counts['namespace_mutations'] += test.args.probes * cycles * 4
            return {}
        test.churn = Mock(side_effect=churn)
        test.run()
        test.setup_probes.assert_called_once()
        test.ingest_only.assert_called_once_with(0, 2)
        test.ingest_with_churn.assert_not_called()
        self.assertEqual(test.churn.call_count, 4)
        self.assertIn('all-version-integrity-after-final-sigkill', phases)
        self.assertEqual(test.report['status'], 'PASS')

    def test_upload_only_and_sequential_churn_flags_are_mutually_exclusive(self):
        argv = ['n1-hdd-scale.py', '--binary', '/unused', '--root', '/unused',
                '--member-parent', '/unused', '--upload-only', '--churn-after-upload']
        with patch.object(scale.sys, 'argv', argv), patch.object(scale.sys, 'stderr', MagicMock()):
            with self.assertRaises(SystemExit) as caught:
                scale.main()
        self.assertEqual(caught.exception.code, 2)

    def test_probe_discovery_keeps_only_bounded_acknowledged_probe_directories(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(probes=2)
        test.upload_one = Mock()
        test.ack_rows = Mock(return_value=[('probe-a', 'version-a', '', 4096, 'probe'),
                                           ('probe-b', 'version-b', '', 4096, 'probe')])
        with tempfile.TemporaryDirectory() as temporary:
            test.roots = [Path(temporary) / str(index) for index in range(2)]
            expected = set()
            for root in test.roots:
                fanout = root / '@f_fixture' / '.objects' / 'aa' / 'bb'
                for index in range(20):
                    (fanout / f'@o_source-{index}' / f'source-version-{index}').mkdir(parents=True)
                for index, version in enumerate(('version-a', 'version-b')):
                    directory = fanout / f'@o_probe-{index}'
                    (directory / version).mkdir(parents=True)
                    expected.add(directory)
            result = test.setup_probes()
            self.assertEqual(test.probe_directories, expected)
            self.assertEqual(result['immutable_probe_directories'], 4)
        test.ack_rows.assert_called_once_with('probe')

    def cleanup_fixture(self, root, status='FAIL'):
        test = scale.Scale.__new__(scale.Scale)
        test.root, test.cgroup = root, None
        test.process = MagicMock(pid=12345)
        test.process.poll.return_value = None
        test.process.wait.side_effect = subprocess.TimeoutExpired('owned-engine', 15)
        test.stop_monitor, test.monitor, test.progress = Mock(), Mock(), Mock()
        test.report = dict(status=status)
        if status == 'FAIL':
            test.report['error'] = 'AssertionError: original integrity failure'
        test.ledger = (root / 'ledger.jsonl').open('a')
        test.db = sqlite3.connect(root / 'ledger.sqlite')
        test.db_lock, test.by_phase, test.lock = threading.Lock(), {}, threading.Lock()
        return test

    def test_scale_cleanup_timeout_preserves_original_error_and_failed_witness(self):
        with tempfile.TemporaryDirectory() as temporary:
            test = self.cleanup_fixture(Path(temporary))
            process = test.process
            original = AssertionError('original integrity failure')
            with patch('builtins.print'), self.assertRaises(AssertionError) as caught:
                try:
                    raise original
                finally:
                    test.finish()
            self.assertIs(caught.exception, original)
            result = json.loads((test.root / 'results.json').read_text())
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['error'], 'AssertionError: original integrity failure')
            self.assertEqual(result['cleanup_errors'][0]['owned_engine_pid'], 12345)
            self.assertEqual(result['cleanup_errors'][0]['wait_timeout_seconds'], 15)
            self.assertTrue(result['cleanup_errors'][0]['process_exit_not_confirmed'])
            self.assertIsNone(test.process)
            process.kill.assert_called_once()
            self.assertTrue(test.ledger.closed)

    def test_scale_cleanup_timeout_cannot_leave_nominal_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            test = self.cleanup_fixture(Path(temporary), status='PASS')
            with patch('builtins.print'), self.assertRaises(subprocess.TimeoutExpired):
                test.finish()
            self.assertEqual(json.loads((test.root / 'results.json').read_text())['status'], 'FAIL')

    def test_result_write_error_does_not_mask_original_integrity_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            test = self.cleanup_fixture(Path(temporary))
            test.process.wait.side_effect = None
            original = AssertionError('original integrity failure')
            with patch.object(Path, 'write_text', side_effect=OSError('witness disk unavailable')), \
                    patch('builtins.print'), self.assertRaises(AssertionError) as caught:
                try:
                    raise original
                finally:
                    test.finish()
            self.assertIs(caught.exception, original)
            self.assertEqual(test.report['status'], 'FAIL')
            self.assertEqual(test.report['error'], 'AssertionError: original integrity failure')
            self.assertEqual(test.report['cleanup_errors'][0]['stage'], 'write-results')

    def test_comparison_observer_preserves_the_offered_workload(self):
        test = admission.Admission.__new__(admission.Admission)
        test.args = SimpleNamespace(objects=5, batch_size=2, batch_clients=3,
                                    request_timeout_seconds=2)
        test.counts = Counter()
        test.started = time.monotonic()
        observed = threading.Event()
        intervals = []
        def health(*args):
            observed.set()
            return dict(system={}, io_scheduler=dict(foreground_target=4),
                        io_control=dict(process={}))
        def upload(interval):
            self.assertTrue(observed.wait(2))
            intervals.append(interval)
            test.counts['source_objects'] += interval[1] - interval[0]
        test.api = health
        test.upload_batch = upload
        with tempfile.TemporaryDirectory() as temporary:
            test.root = Path(temporary)
            result = test.ingest_sampled()
            self.assertTrue((test.root / 'scheduler-samples.jsonl').is_file())
        self.assertEqual(sorted(intervals), [(0, 2), (2, 4), (4, 5)])
        self.assertEqual(result['new_acknowledged_objects'], 5)
        self.assertEqual(result['observed_foreground_targets'], {4: 1})

    def test_upload_only_ingestion_keeps_batch_dimensions_and_counts(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(batch_size=2, batch_clients=3)
        test.counts = Counter(source_objects=7)
        intervals = []
        def upload(interval):
            intervals.append(interval)
            test.counts['source_objects'] += interval[1] - interval[0]
        test.upload_batch = upload
        result = test.ingest_only(7, 12)
        self.assertEqual(sorted(intervals), [(7, 9), (9, 11), (11, 12)])
        self.assertEqual(result['new_source_objects'], 5)
        self.assertEqual(result['source_objects_total'], 12)
        self.assertEqual(result['concurrent_namespace_clients'], 0)
        self.assertEqual(test.counts['namespace_mutations'], 0)

    def authentication_fixture(self):
        test = scale.Scale.__new__(scale.Scale)
        test.last_auth = 0
        test.auth_lock = threading.Lock()
        test.fragment = 'fixture'
        test.fragment_key = 'fixture-secret'
        test.api = Mock(return_value={'access_token': 'fixture-token'})
        return test

    def test_initial_authentication_is_not_skipped_after_reboot(self):
        test = self.authentication_fixture()
        with patch.object(scale.time, 'monotonic', return_value=42):
            test.authenticate()
            test.api.assert_called_once()
            self.assertEqual(test.token, 'fixture-token')
            self.assertEqual(test.last_auth, 42)

    def test_concurrent_initial_authentication_is_single_flight(self):
        test = self.authentication_fixture()
        with patch.object(scale.time, 'monotonic', return_value=42):
            list(scale.bounded_map(lambda _: test.authenticate(), range(64), 16))
        test.api.assert_called_once()
        self.assertEqual(test.token, 'fixture-token')

    def test_authentication_refresh_keeps_existing_interval(self):
        test = self.authentication_fixture()
        test.last_auth = 42
        test.token = 'old-token'
        with patch.object(scale.time, 'monotonic', return_value=641):
            test.authenticate()
        test.api.assert_not_called()
        with patch.object(scale.time, 'monotonic', return_value=642):
            test.authenticate()
        test.api.assert_called_once()
        self.assertEqual(test.token, 'fixture-token')
        self.assertEqual(test.last_auth, 642)

    def resource_fixture(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(io_backend='uring')
        test.process = SimpleNamespace(pid=123)
        test.authenticate = lambda: None
        test.fragment = 'fixture'
        test.phase = 'resources'
        test.started = time.monotonic()
        health = {'io_control': {'process': {'pid': 123, 'open_fds': 45, 'rss_bytes': 65536},
                                 'backend': {'requested': 'uring', 'backend': 'io_uring', 'fallback_reason': None},
                                 'latency': {'classes': {'upload.part.setup': {'count': 2}}}},
                  'io_scheduler': {'foreground_target': 6},
                  'maintenance': {'auto_healing': {'last_availability_scan_at': None, 'last_checksum_scrub_at': None}}}
        test.api = lambda method, route: ({'is_running': False, 'total_errors': 0}
                                         if route == '/maintenance/gc/status' else health)
        return test, health

    def test_resource_checkpoint_rejects_foreign_pid_and_missing_fd_authority(self):
        test, health = self.resource_fixture()
        health['io_control']['process']['pid'] = 456
        with self.assertRaisesRegex(AssertionError, 'another engine'):
            test.resource_checkpoint()
        health['io_control']['process'].update(pid=123, open_fds=None)
        with self.assertRaises(AssertionError):
            test.resource_checkpoint()

    def test_resource_checkpoint_records_engine_self_observed_fds(self):
        test, health = self.resource_fixture()
        with tempfile.TemporaryDirectory() as temporary:
            test.root = Path(temporary)
            result = test.resource_checkpoint()
            self.assertEqual(result['engine_reported_resources']['open_fds'], 45)
            sample = json.loads((test.root / 'engine-resource-checkpoints.jsonl').read_text())
            self.assertEqual(sample['pid'], 123)
            self.assertEqual(sample['io_latency'], health['io_control']['latency'])
            self.assertEqual(sample['io_scheduler'], health['io_scheduler'])
            self.assertEqual(result['engine_io_latency'], health['io_control']['latency'])
            self.assertEqual(result['engine_io_scheduler'], health['io_scheduler'])
            self.assertEqual(result['engine_io_backend'], health['io_control']['backend'])
            self.assertEqual(sample['io_backend'], health['io_control']['backend'])
            health['maintenance']['auto_healing']['last_availability_scan_at'] = 1
            with self.assertRaisesRegex(AssertionError, 'unsolicited'):
                test.resource_checkpoint()

    def test_resource_checkpoint_rejects_silently_changed_backend(self):
        test, health = self.resource_fixture()
        health['io_control']['backend']['backend'] = 'blocking'
        with self.assertRaisesRegex(AssertionError, 'Required io_uring'):
            test.resource_checkpoint()

    def test_fd_permission_error_is_telemetry_not_disk_failure(self):
        with patch.object(Path, 'read_text', return_value='VmRSS:\t12345 kB\n'), patch.object(Path, 'iterdir', side_effect=PermissionError('denied')):
            usage = scale.process_usage(123)
        self.assertEqual(usage['rss_kib'], 12345)
        self.assertNotIn('fds', usage, 'missing telemetry must not be reported as zero')
        self.assertEqual(usage['unavailable']['fds'], 'PermissionError')

    def test_departed_process_metrics_are_unavailable_not_zero(self):
        with patch.object(Path, 'read_text', side_effect=ProcessLookupError()), patch.object(Path, 'iterdir', side_effect=FileNotFoundError()):
            usage = scale.process_usage(123)
        self.assertNotIn('rss_kib', usage)
        self.assertNotIn('fds', usage)
        self.assertEqual(set(usage['unavailable']), {'rss_kib', 'fds'})

    def test_admission_diagnosis_does_not_overwrite_original_object_keys(self):
        test = admission.Admission.__new__(admission.Admission)
        for index in (0, 1000, 1000000):
            self.assertNotEqual(test.batch_object_key(index), scale.source_key(index))
            self.assertEqual(test.batch_object_key(index), 'admission-diagnostic/' + scale.source_key(index))

    def test_recheck_uses_public_alias_not_physical_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            physical = root / '@f_stable-id'
            physical.mkdir()
            (physical / '.fragment.ndx').touch()
            (root / '.fragment').mkdir()
            (root / '.fragment' / 'public-alias').symlink_to('../@f_stable-id')
            self.assertEqual(recheck.fixture_fragment([root]), 'public-alias')

    def test_recheck_refuses_alias_outside_member(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root, physical = parent / 'member', parent / '@f_other-member'
            root.mkdir()
            physical.mkdir()
            (physical / '.fragment.ndx').touch()
            (root / '.fragment').mkdir()
            (root / '.fragment' / 'alias').symlink_to('../../@f_other-member')
            with self.assertRaisesRegex(AssertionError, 'Unexpected alias target'):
                recheck.fixture_fragment([root])

    def test_recheck_refuses_nonfixture_member(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            root = parent / 'personal-data'
            root.mkdir()
            with self.assertRaisesRegex(AssertionError, 'Not a disposable'):
                recheck.fixture_member(root, parent)

    def maintenance_fixture(self):
        test = benchmark.Benchmark.__new__(benchmark.Benchmark)
        test.counts = Counter(acknowledged_versions=512)
        def call(method, route, data=None):
            self.assertEqual(method, 'POST')
            self.assertIsNone(data, 'Query-only maintenance endpoint must not receive a body')
            return 202, b''
        test.call = call
        result = dict(is_running=False, phase='Completed', total_scanned=512,
                      failed_repairs=0, corrupt_found=0)
        test.api = lambda *args: result
        return test

    def test_benchmark_maintenance_requires_bodyless_request_and_complete_scan(self):
        test = self.maintenance_fixture()
        self.assertEqual(test.repair(), {'scanned': 512, 'failed_repairs': 0})
        test.counts['acknowledged_versions'] = 513
        with self.assertRaises(AssertionError):
            test.repair()

    def test_completed_phase_does_not_hide_failed_maintenance(self):
        test = self.maintenance_fixture()
        test.api = lambda *args: dict(is_running=False, phase='Completed', total_scanned=512,
                                     failed_repairs=1, corrupt_found=0)
        with self.assertRaises(AssertionError):
            test.repair()

    def test_running_report_cannot_open_or_fail_a_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'gate.json'
            path.write_text('{"status":"RUNNING"}')
            def finish():
                time.sleep(.06)
                path.write_text('{"status":"PASS"}')
            worker = threading.Thread(target=finish)
            worker.start()
            try:
                self.assertEqual(scale.wait_for_result(path, 2, .01)['status'], 'PASS')
            finally:
                worker.join()

    def test_incomplete_json_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'gate.json'
            path.write_text('{"status":')
            with self.assertRaisesRegex(AssertionError, 'terminal result'):
                scale.wait_for_result(path, .03, .01)

    def test_histogram_storage_does_not_grow_with_request_count(self):
        histogram = scale.Histogram()
        for i in range(1000000):
            histogram.append(i % 100 + 1)
        self.assertEqual(len(histogram.bins), 1024)
        self.assertEqual(histogram.summary()['count'], 1000000)
        self.assertGreaterEqual(histogram.summary()['p95_upper_bound_ms'], 95)
        self.assertLessEqual(histogram.summary()['p95_upper_bound_ms'], 100)

    def test_submission_window_is_bounded(self):
        gate = threading.Event()
        consumed = []
        produced_four = threading.Event()
        def source():
            for i in range(100):
                consumed.append(i)
                if len(consumed) == 4:
                    produced_four.set()
                yield i
        results = []
        def work(i):
            gate.wait(5)
            return i
        worker = threading.Thread(target=lambda: results.extend(scale.bounded_map(work, source(), 2)))
        worker.start()
        try:
            self.assertTrue(produced_four.wait(3))
            time.sleep(.05)
            self.assertEqual(len(consumed), 4)
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(sorted(results), list(range(100)))

    def test_missing_or_replaced_member_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            test = scale.base.Stress.__new__(scale.base.Stress)
            test.storage_devices = {path: path.stat().st_dev + 1}
            self.assertIn('changed or unmounted', test.storage_problem())
            test.storage_devices = {path / 'gone': path.stat().st_dev}
            self.assertIn('unavailable', test.storage_problem())

    def batch_fixture(self, path, reject_last=False, flat_folder=False):
        test = scale.Scale.__new__(scale.Scale)
        test.source = path
        test.args = SimpleNamespace(batch_concurrency=16, flat_folder=flat_folder)
        if flat_folder:
            (path / 'projects' / 'million').mkdir(parents=True)
        test.authenticate = lambda: None
        test.storage_problem = lambda: None
        test.counts = Counter()
        test.lock = threading.Lock()
        test.db_lock = threading.Lock()
        test.batch_timings = defaultdict(lambda: defaultdict(scale.Histogram))
        test.phase, test.started, test.root = 'batch-upload', 0, path
        records = []
        test.record_acks = lambda rows: records.extend(rows)
        def call(method, route, data, headers):
            message = BytesParser(policy=policy.default).parsebytes(
                ('Content-Type: ' + headers['Content-Type'] + '\r\nMIME-Version: 1.0\r\n\r\n').encode() + data)
            replies = []
            for i, part in enumerate(message.iter_parts()):
                key = urllib.parse.unquote(part.get_filename())
                self.assertEqual(key, scale.source_key(i, flat_folder))
                body = part.get_payload(decode=True)
                self.assertEqual(body, scale.base.payload(key, scale.source_size(i)))
                replies.append({'object_key': key, 'version_id': 'version-' + str(i),
                                'status': 503 if reject_last and i == 5 else 201})
            self.assertEqual(len(replies), 6)
            return (207 if reject_last else 201), json.dumps({'results': replies,
                'succeeded': 5 if reject_last else 6, 'failed': 1 if reject_last else 0}).encode()
        test.call = call
        return test, records

    def test_wire_format_and_actual_folder_payloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, records = self.batch_fixture(Path(temporary))
            test.upload_batch((0, 6))
            self.assertEqual(len(records), 6)
            self.assertEqual(len(list(Path(temporary).rglob('*.bin'))), 6)
            self.assertEqual(records[0][3], 0)

    def test_flat_source_directory_is_prepared_once_not_created_per_object(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            test, records = self.batch_fixture(root, flat_folder=True)
            with patch.object(Path, 'mkdir', side_effect=AssertionError('Repeated flat-folder mkdir')):
                test.upload_batch((0, 6))
            files = list(root.rglob('*.bin'))
            self.assertEqual(len(files), 6)
            self.assertEqual({path.parent for path in files}, {root / 'projects' / 'million'})
            self.assertEqual(len(records), 6)

    def test_batch_timers_separate_source_http_and_ack_ledger_without_changing_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, records = self.batch_fixture(Path(temporary))
            test.call = Mock(wraps=test.call)
            with patch.object(scale.time, 'monotonic', side_effect=[10, 12, 20, 25, 30, 33]):
                test.upload_batch((0, 6))
            timings = test.batch_timings['batch-upload']
            for name, elapsed in (('source_preparation', 2000), ('http_request', 5000),
                                  ('acknowledgement_ledger', 3000)):
                self.assertEqual(timings[name].summary()['count'], 1)
                self.assertEqual(timings[name].summary()['mean_ms'], elapsed)
                self.assertEqual(len(timings[name].bins), 1024)
            self.assertEqual(len(records), 6)
            test.call.assert_called_once()
            self.assertEqual(test.call.call_args.args[:2],
                             ('POST', '/upload/batch?durability=strict&concurrency=16'))

    def test_batch_http_failure_is_timed_preserved_and_never_retried(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, records = self.batch_fixture(Path(temporary))
            test.call = Mock(side_effect=OSError('interrupted transport'))
            with self.assertRaisesRegex(OSError, 'interrupted transport'):
                test.upload_batch((0, 6))
            test.call.assert_called_once()
            self.assertEqual(records, [])
            self.assertEqual(test.counts['transport_ambiguous_batches'], 1)
            self.assertEqual(test.batch_timings['batch-upload']['http_request'].summary()['count'], 1)
            self.assertNotIn('acknowledgement_ledger', test.batch_timings['batch-upload'])
            evidence = json.loads((test.root / 'ambiguous-batches.jsonl').read_text())
            self.assertEqual((evidence['start'], evidence['end']), (0, 6))

    def test_batch_source_and_ack_failures_are_not_hidden_by_timing(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, _ = self.batch_fixture(Path(temporary))
            test.call = Mock(wraps=test.call)
            test.record_acks = Mock(side_effect=OSError('ledger failure'))
            with self.assertRaisesRegex(OSError, 'ledger failure'):
                test.upload_batch((0, 6))
            self.assertEqual(test.batch_timings['batch-upload']['acknowledgement_ledger'].summary()['count'], 1)
            with self.assertRaises(FileExistsError):
                test.upload_batch((0, 6))
            test.call.assert_called_once()
            self.assertEqual(test.batch_timings['batch-upload']['source_preparation'].summary()['count'], 2)
            self.assertEqual(test.batch_timings['batch-upload']['http_request'].summary()['count'], 1)

    def test_batch_timing_summaries_are_present_in_progress_and_completed_phases(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, _ = self.batch_fixture(Path(temporary))
            test.report, test.peaks, test.fault = {}, Counter(), None
            test.auth_lock = threading.Lock()
            test.phases, test.calls, test.latency = [], defaultdict(Counter), defaultdict(scale.Histogram)
            test.phase_run('batch-upload', lambda: test.upload_batch((0, 6)))
            progress = json.loads((test.root / 'progress.json').read_text())
            expected = {name: histogram.summary()
                        for name, histogram in test.batch_timings['batch-upload'].items()}
            self.assertEqual(progress['batch_client_timings_ms']['batch-upload'], expected)
            self.assertEqual(test.phases[-1]['batch_client_timings_ms'], expected)

    def test_partial_batch_success_is_preserved_before_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, records = self.batch_fixture(Path(temporary), reject_last=True)
            with self.assertRaisesRegex(AssertionError, 'refused'):
                test.upload_batch((0, 6))
            self.assertEqual(len(records), 5)

    def test_failed_batch_captures_only_bounded_read_only_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, _ = self.batch_fixture(Path(temporary))
            test.args.port, test.tls, test.token = 59001, object(), 'never-retain-this-token'
            test.process = SimpleNamespace(pid=123)
            connection = MagicMock()
            health = {'system': {'cpu_usage_percent': 12}, 'io_scheduler': {'active_foreground': 4},
                      'io_control': {'process': {'pid': 123}}, 'unrelated': 'not retained'}
            connection.getresponse.side_effect = [
                SimpleNamespace(status=200, read=Mock(return_value=json.dumps(health).encode())),
                SimpleNamespace(status=200, read=Mock(return_value=b'{"is_running":true}'))]
            with patch.object(scale.http.client, 'HTTPSConnection', return_value=connection) as create:
                test.capture_batch_failure((0, 64), [{'status': 503}] * 64)
            create.assert_called_once_with('127.0.0.1', 59001, context=test.tls, timeout=2)
            self.assertEqual([call.args[:2] for call in connection.request.call_args_list], [
                ('GET', '/noa/v1/maintenance/health'), ('GET', '/noa/v1/maintenance/gc/status')])
            connection.close.assert_called_once()
            paths = list(test.root.glob('batch-failure-*.json'))
            self.assertEqual(len(paths), 1)
            text = paths[0].read_text()
            self.assertNotIn(test.token, text)
            evidence = json.loads(text)
            self.assertEqual(len(evidence['failure_samples']), 3)
            self.assertEqual(evidence['refused_objects'], 64)
            self.assertEqual(evidence['gc'], {'is_running': True})
            self.assertNotIn('unrelated', evidence['health'])

    def test_failed_batch_diagnostics_do_not_mask_refusal_or_trust_foreign_pid(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, _ = self.batch_fixture(Path(temporary))
            test.args.port, test.tls, test.token = 59001, object(), 'token'
            test.process = SimpleNamespace(pid=123)
            connection = MagicMock()
            connection.getresponse.return_value = SimpleNamespace(status=200, read=Mock(
                return_value=b'{"io_control":{"process":{"pid":999}}}'))
            with patch.object(scale.http.client, 'HTTPSConnection', return_value=connection):
                test.capture_batch_failure((0, 64), [{'status': 503}])
            evidence = json.loads(next(test.root.glob('batch-failure-*.json')).read_text())
            self.assertIn('another engine', evidence['diagnostic_error'])
            self.assertNotIn('health', evidence)
            self.assertEqual(connection.request.call_count, 1, 'Never continue querying a foreign engine')

    def test_explicit_admission_probe_never_counts_rejections_as_writes(self):
        with tempfile.TemporaryDirectory() as temporary:
            test, records = self.batch_fixture(Path(temporary), reject_last=True)
            test.args.accept_admission_refusals = True
            test.upload_batch((0, 6))
            self.assertEqual(len(records), 5)
            self.assertEqual(test.counts['admission_refusals'], 1)
            self.assertEqual(test.counts['partial_batches_reported'], 1)

    def test_noop_mutations_are_not_counted_as_churn(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(probes=1)
        test.lock = threading.Lock()
        test.counts = Counter()
        test.authenticate = lambda: None
        test.immutable_probe_inventory = lambda: {'fixture': ('unchanged',)}
        test.api = lambda *a, **kw: {'operation': 'rename', 'kind': 'file', 'affected': 0,
                                   'failed_paths': [], 'conflicted_paths': []}
        test.call = lambda *a, **kw: (200, scale.base.payload('churn/probe-0000', 4096))
        with self.assertRaisesRegex(AssertionError, 'exactly one object'):
            test.churn(1, 1)
        self.assertEqual(test.counts['namespace_mutations'], 0)

    def test_public_posix_response_counts_real_mutations(self):
        test = scale.Scale.__new__(scale.Scale)
        test.args = SimpleNamespace(probes=1)
        test.lock = threading.Lock()
        test.counts = Counter()
        test.authenticate = lambda: None
        test.immutable_probe_inventory = lambda: {'fixture': ('unchanged',)}
        test.api = lambda method, route, data: {'operation': route.rsplit('/', 1)[-1],
                                              'kind': 'file', 'affected': 1}
        test.call = lambda *a, **kw: (200, scale.base.payload('churn/probe-0000', 4096))
        result = test.churn(1, 256)
        self.assertEqual(test.counts['namespace_mutations'], 4)
        self.assertEqual(result['maximum_client_concurrency'], 1)


if __name__ == '__main__':
    unittest.main()
