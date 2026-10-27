"""Safety checks for the scheduled-only proof; no engine/device operations."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location('scheduled_gc', Path(__file__).with_name('n1-gc-scheduled-proof.py'))
proof = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(proof)


class ScheduledProofSafety(unittest.TestCase):
    def test_health_observation_checks_the_explicit_backend_without_an_extra_request(self):
        test = proof.ScheduledGcProof.__new__(proof.ScheduledGcProof)
        test.args = SimpleNamespace(io_backend='uring')
        test.process = SimpleNamespace(pid=123)
        test.fragment = 'owned-fragment'
        test.health_observations = []
        health = dict(io_control=dict(process=dict(pid=123),
                                      backend=dict(requested='uring', backend='io_uring', fallback_reason=None)),
                      maintenance=dict(auto_healing=dict(last_availability_scan_at=None,
                                                        last_checksum_scrub_at=None), repair_running=False),
                      consistency=dict(total_index_records_scanned=0))
        test.api = MagicMock(return_value=health)
        self.assertIs(test.health(), health)
        test.api.assert_called_once()
        health['io_control']['backend']['backend'] = 'blocking'
        with self.assertRaisesRegex(AssertionError, 'Required io_uring'):
            test.health()
        self.assertEqual(len(test.health_observations), 1)

    def timeout_cleanup_fixture(self, status='FAIL'):
        test = proof.ScheduledGcProof.__new__(proof.ScheduledGcProof)
        process = MagicMock(pid=12345)
        process.poll.return_value = None
        process.wait.side_effect = subprocess.TimeoutExpired('owned-engine', 15)
        test.process = process
        test.lock = threading.Lock()
        test.stop_monitor = MagicMock()
        test.monitor = MagicMock()
        test.health_observations = [dict(observed=True)]
        test.samples = [dict(phase='setup', rss_kib=12, hwm_kib=14, fds=5)]
        test.report = dict(status=status)
        if status == 'FAIL':
            test.report['error'] = 'AssertionError: original startup failure'
        written = {}
        test.root = MagicMock()

        def output_file(name):
            target = MagicMock()
            target.write_text.side_effect = lambda content: written.update({name: content})
            return target

        test.root.__truediv__.side_effect = output_file
        return test, process, written

    def test_cleanup_timeout_persists_failure_and_does_not_mask_original(self):
        test, process, written = self.timeout_cleanup_fixture()
        original = AssertionError('original startup failure')
        with patch('builtins.print'), self.assertRaises(AssertionError) as caught:
            try:
                raise original
            finally:
                test.finish()
        self.assertIs(caught.exception, original)
        result = json.loads(written['results.json'])
        self.assertEqual(result['status'], 'FAIL')
        self.assertEqual(result['error'], 'AssertionError: original startup failure')
        self.assertEqual(result['cleanup_errors'][0]['owned_engine_pid'], 12345)
        self.assertEqual(result['cleanup_errors'][0]['wait_timeout_seconds'], 15)
        self.assertTrue(result['cleanup_errors'][0]['process_exit_not_confirmed'])
        self.assertEqual(result['resources']['peak_fds'], 5)
        self.assertEqual(len(json.loads(written['resource-samples.json'])), 1)
        self.assertIsNone(test.process)
        process.kill.assert_called_once()
        process.wait.assert_called_once_with(timeout=15)
        test.stop_monitor.set.assert_called()
        test.monitor.join.assert_called_with(timeout=3)

    def test_cleanup_timeout_turns_nominal_pass_into_failed_result(self):
        test, _, written = self.timeout_cleanup_fixture(status='PASS')
        with patch('builtins.print'), self.assertRaises(subprocess.TimeoutExpired):
            test.finish()
        self.assertEqual(json.loads(written['results.json'])['status'], 'FAIL')

    def test_topology_counts_preserve_two_drive_and_four_drive_coverage(self):
        self.assertEqual(proof.topology_counts(1, 1, 2), (2, 2))
        self.assertEqual(proof.topology_counts(3, 1, 4), (4, 2))
        self.assertEqual(proof.topology_counts(4, 2, 6), (6, 3))
        self.assertEqual(proof.topology_counts(1, 0, 1), (1, 1))
        for topology in ((1, 1, 4), (3, 1, 2), (0, 1, 1), (1, -1, 0), (33, 0, 33)):
            with self.subTest(topology=topology), self.assertRaises(AssertionError):
                proof.topology_counts(*topology)

    def test_shared_physical_drive_is_rejected_before_fixture_creation(self):
        args = SimpleNamespace(data_shards=1, parity_shards=1,
                               member_parent=[Path('/first-partition'), Path('/second-partition')],
                               root=Path('/results/fresh-fixture'))
        with patch.object(proof, 'require_data_volume'), \
                patch.object(proof, 'backing_device', return_value=Path('/sys/same-drive')), \
                patch.object(proof.base.Stress, '__init__') as create_fixture:
            with self.assertRaisesRegex(AssertionError, 'independent physical'):
                proof.ScheduledGcProof(args)
            create_fixture.assert_not_called()

    def test_manual_or_error_gc_status_is_never_a_pass(self):
        proof.validate_automatic_status(dict(is_manual_run=False, total_errors=0))
        for status in (dict(is_manual_run=True, total_errors=0), {},
                       dict(is_manual_run=False, total_errors=1)):
            with self.assertRaises(AssertionError):
                proof.validate_automatic_status(status)

    def test_manual_maintenance_start_is_forbidden_before_any_request(self):
        test = proof.ScheduledGcProof.__new__(proof.ScheduledGcProof)
        for route in ('/maintenance/gc/start', '/maintenance/fragments/test/gc/start',
                      '/maintenance/repair/start', '/maintenance/gc/stop'):
            with self.assertRaisesRegex(AssertionError, 'must never'):
                test.call('POST', route)

    def test_os_volume_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(proof.os.path, 'ismount', side_effect=lambda path: path == Path('/')):
                with self.assertRaisesRegex(AssertionError, 'OS filesystem'):
                    proof.require_data_volume(Path(temporary))

    def test_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            member, outside = base / 'member', base / 'outside'
            member.mkdir()
            outside.mkdir()
            (member / 'escape').symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(AssertionError, 'escapes'):
                proof.require_inside(member / 'escape', member)

    def test_backdating_changes_only_valid_owned_trash_generation_name(self):
        with tempfile.TemporaryDirectory() as temporary:
            member = Path(temporary)
            generation = member / '.trash' / ('123456789.d2.' + proof.new_ulid())
            generation.mkdir(parents=True)
            (generation / 'reference.ncx').write_bytes(b'unchanged')
            destination = proof.age_trash_generation(generation, member, 123)
            self.assertEqual(destination.name.split('.')[0], '123000000000')
            self.assertEqual((destination / 'reference.ncx').read_bytes(), b'unchanged')
            unknown = member / 'unknown-format'
            unknown.mkdir()
            with self.assertRaisesRegex(AssertionError, 'Unknown trash'):
                proof.age_trash_generation(unknown, member, 123)
            self.assertTrue(unknown.exists())

    def test_ulids_have_canonical_parseable_shape(self):
        values = [proof.new_ulid() for _ in range(100)]
        self.assertEqual(len(set(values)), 100)
        for value in values:
            self.assertRegex(value, r'^[0-7][0-9A-HJKMNP-TV-Z]{25}$')


if __name__ == '__main__':
    unittest.main()
