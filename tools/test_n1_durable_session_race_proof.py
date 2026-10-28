"""Regression/safety tests for the isolated HTTP session race proof; no engine."""
from collections import Counter
import configparser
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location('durable_session_race', Path(__file__).with_name('n1-durable-session-race-proof.py'))
proof = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(proof)


class DurableSessionProofTests(unittest.TestCase):
    def fixture(self):
        test = proof.DurableRaceProof.__new__(proof.DurableRaceProof)
        test.args = SimpleNamespace(part_size=4096, concurrency=2, objects=2, request_timeout_seconds=2)
        test.lock = threading.Lock()
        test.counters = Counter()
        test.acknowledged = []
        test.token = 'test-token'
        test.api = MagicMock()
        test.call = MagicMock()
        return test

    def test_load_profile_keeps_admission_enabled_at_explicit_incident_rates(self):
        config = configparser.ConfigParser()
        config['rate_limiter_config'] = dict(enable_rate_limiting='false',
                                            upload_limit_per_second='300', upload_limit_burst_size='3000')
        config['io_control'] = dict(foreground_wait_timeout_ms='30000')
        applied = proof.apply_load_profile(config)
        self.assertTrue(config['rate_limiter_config'].getboolean('enable_rate_limiting'))
        self.assertEqual(applied['view_limit_per_second'], '1000')
        self.assertEqual(applied['view_limit_burst_size'], '3000')
        self.assertEqual(applied['global_limit_per_second'], '2000')
        self.assertEqual(applied['global_limit_burst_size'], '5000')
        self.assertEqual(config['rate_limiter_config']['upload_limit_per_second'], '300')
        self.assertEqual(config['rate_limiter_config']['upload_limit_burst_size'], '3000')
        self.assertEqual(config['io_control']['foreground_wait_timeout_ms'], '30000')

    def test_workload_bounds_reject_unbounded_or_empty_workers(self):
        args = SimpleNamespace(concurrency=100, objects=300, part_size=65536,
                               deadline_seconds=1800, request_timeout_seconds=120)
        proof.validate_workload(args)
        for key, value in (('concurrency', 0), ('concurrency', 257), ('objects', 99),
                           ('objects', 10001), ('part_size', 4095), ('part_size', 1048577),
                           ('deadline_seconds', 21601), ('deadline_seconds', 0),
                           ('request_timeout_seconds', 1801)):
            bad = SimpleNamespace(**vars(args))
            setattr(bad, key, value)
            with self.subTest(key=key, value=value), self.assertRaises(AssertionError):
                proof.validate_workload(bad)

    def test_os_root_is_rejected_before_fixture_creation(self):
        with patch.object(proof.os.path, 'ismount', side_effect=lambda path: path == Path('/')):
            with self.assertRaisesRegex(AssertionError, 'not the OS filesystem'):
                proof.data_device(Path('/'))

    def test_init_uses_actual_incident_default_reuse_path_no_fresh_bypass(self):
        test = self.fixture()
        session = dict(session_id='session', version_id='version')
        test.api.return_value = session
        self.assertEqual(test.init('folder/name with space.bin'), session)
        args = test.api.call_args.args
        self.assertEqual(args[0], 'POST')
        self.assertEqual(args[1], '/upload/multipart/durable/init/folder/name%20with%20space.bin')
        self.assertNotIn('fresh', args[1])
        self.assertEqual(args[2], dict(content_type='application/octet-stream', expected_parts=2,
                                       total_size_bytes=8192, part_size_bytes=4096))
        self.assertEqual(test.counters['successful_init_calls'], 1)

    def test_resume_must_preserve_both_identifiers(self):
        test = self.fixture()
        original = dict(session_id='session', version_id='version')
        for changed in (dict(session_id='other', version_id='version'),
                        dict(session_id='session', version_id='other')):
            test.api.return_value = changed
            with self.subTest(changed=changed), self.assertRaisesRegex(AssertionError, 'did not reuse'):
                test.resume('same.bin', original)
        test.api.return_value = original
        test.resume('same.bin', original)
        self.assertEqual(test.counters['identical_init_resumptions'], 1)

    def test_init_error_is_not_retried_or_swallowed(self):
        test = self.fixture()
        test.api.side_effect = AssertionError('HTTP 500: pending binding read')
        with self.assertRaisesRegex(AssertionError, 'HTTP 500'):
            test.init('path.bin')
        test.api.assert_called_once()
        self.assertFalse(test.counters)

    def test_same_part_repeat_is_identical_and_strict(self):
        test = self.fixture()
        test.call.return_value = (200, b'')
        session = dict(session_id='session', version_id='version')
        test.part('same.bin', session, 0)
        test.part('same.bin', session, 0)
        first, second = test.call.call_args_list
        self.assertEqual(first, second)
        self.assertEqual(first.args[1], '/upload/multipart/durable/version/0?durability=strict')
        self.assertEqual(len(first.args[2]), 4096)

    def test_part_error_is_not_counted_as_success_or_retried(self):
        test = self.fixture()
        test.call.return_value = (503, b'overload')
        with self.assertRaisesRegex(AssertionError, 'HTTP 503'):
            test.part('same.bin', dict(version_id='version'), 0)
        test.call.assert_called_once()
        self.assertFalse(test.counters)

    def test_cancel_repeats_delete_and_proves_no_namespace_publication(self):
        test = self.fixture()
        test.call.side_effect = [(200, b''), (204, b''), (404, b'not found')]
        test.api.return_value = dict(uploaded_parts=[])
        test.cancel('cancel/name.bin', dict(version_id='version'))
        self.assertEqual([call.args[0] for call in test.call.call_args_list], ['DELETE', 'DELETE', 'GET'])
        self.assertEqual(test.call.call_args_list[0], test.call.call_args_list[1])
        self.assertEqual(test.counters['cancelled_sessions_verified_invisible'], 1)

    def test_cancel_visibility_error_fails_the_gate(self):
        test = self.fixture()
        test.call.side_effect = [(200, b''), (200, b''), (200, b'bad published data')]
        test.api.return_value = dict(uploaded_parts=[])
        with self.assertRaisesRegex(AssertionError, 'became visible'):
            test.cancel('cancel/name.bin', dict(version_id='version'))
        self.assertFalse(test.counters)

    def test_ack_witness_excludes_token_and_records_exact_bytes(self):
        test = self.fixture()
        session = dict(session_id='session', version_id='version')
        test.api.return_value = dict(version_id='version', object_key='publish/name.bin')
        test.read_ack = MagicMock()
        with tempfile.TemporaryDirectory() as temporary:
            test.root = Path(temporary)
            ack = test.finalize('publish/name.bin', session)
            records = (test.root / 'acknowledgements.jsonl').read_text()
            self.assertNotIn('test-token', records)
            witness = json.loads(records)
        body = b''.join(proof.base.payload(f'publish/name.bin:part:{index}', 4096) for index in range(2))
        self.assertEqual(witness['sha256'], proof.base.digest(body))
        self.assertEqual(witness['size_bytes'], len(body))
        self.assertEqual(test.acknowledged, [ack])
        test.read_ack.assert_called_once_with(ack)

    def test_historical_verification_failure_cannot_be_hidden_by_good_listing(self):
        test = self.fixture()
        test.acknowledged = [('token', 'publish/name.bin', 'version', 'hash', 1)]
        test.parallel = MagicMock(side_effect=AssertionError('ACKNOWLEDGED DATA MISMATCH'))
        test.list_all = MagicMock(return_value=['publish/name.bin'])
        with self.assertRaisesRegex(AssertionError, 'DATA MISMATCH'):
            test.verify()
        test.list_all.assert_not_called()

    def test_complete_listing_cannot_omit_or_add_objects(self):
        for actual in ([], ['publish/name.bin', 'publish/extra.bin']):
            test = self.fixture()
            test.acknowledged = [('token', 'publish/name.bin', 'version', 'hash', 1)]
            test.parallel = MagicMock()
            test.list_all = MagicMock(return_value=actual)
            with self.subTest(actual=actual), self.assertRaisesRegex(AssertionError, 'Published listing differs'):
                test.verify()


if __name__ == '__main__':
    unittest.main()
