"""Safety checks for the disposable RC harness; never starts an engine."""
import importlib.util
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
SCRIPT = Path(__file__).with_name('n1-rc-stress.py')
spec = importlib.util.spec_from_file_location('rc_stress', SCRIPT)
stress = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stress)


class FixtureSafety(unittest.TestCase):
    def test_backend_selection_verifies_required_mode_and_honest_auto_fallback(self):
        for mode, selected, reason in (('uring', 'io_uring', None),
                                       ('blocking', 'blocking', None),
                                       ('auto', 'io_uring', None),
                                       ('auto', 'blocking', 'io_uring_setup denied')):
            with self.subTest(mode=mode, selected=selected):
                backend = dict(requested=mode, backend=selected, fallback_reason=reason)
                health = dict(io_control=dict(backend=backend))
                self.assertIs(stress.verify_io_backend(health, SimpleNamespace(io_backend=mode)), backend)
        for mode, backend in (
                ('uring', dict(requested='uring', backend='blocking', fallback_reason=None)),
                ('uring', dict(requested='auto', backend='io_uring', fallback_reason=None)),
                ('auto', dict(requested='auto', backend='blocking', fallback_reason=None)),
                ('auto', dict(requested='auto', backend='unavailable', fallback_reason='denied')),
                ('blocking', dict(requested='blocking', backend='blocking', fallback_reason='ignored'))):
            with self.subTest(mode=mode, backend=backend), self.assertRaises(AssertionError):
                stress.verify_io_backend(dict(io_control=dict(backend=backend)), SimpleNamespace(io_backend=mode))
        with self.assertRaisesRegex(AssertionError, 'did not report'):
            stress.verify_io_backend(dict(io_control={}), SimpleNamespace(io_backend='uring'))
        self.assertEqual(stress.requested_io_backend(SimpleNamespace()), 'auto')

    def test_tls_trust_outlives_long_soak_deadline(self):
        self.assertEqual(stress.fixture_certificate_days(1800), 2)
        self.assertEqual(stress.fixture_certificate_days(72 * 3600), 4)
        for seconds in (1, 86400, 172800, 259200, 604800):
            self.assertGreater(stress.fixture_certificate_days(seconds) * 86400, seconds)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.parent = self.base/'members'
        self.parent.mkdir()
        self.args = SimpleNamespace(root=self.base/'new-fixture', member_parent=[self.parent],
                                    data_shards=1, parity_shards=0, port=56801)
        self.space = patch.object(shutil, 'disk_usage', return_value=SimpleNamespace(free=80*1024**3))
        self.space.start()
        self.addCleanup(self.space.stop)

    def test_existing_fixture_is_never_overwritten(self):
        self.args.root.mkdir()
        sentinel = self.args.root/'sentinel'
        sentinel.write_bytes(b'keep')
        with self.assertRaisesRegex(AssertionError, 'nonexistent'):
            stress.Stress(self.args)
        self.assertEqual(sentinel.read_bytes(), b'keep')

    def test_invalid_backend_is_rejected_before_creating_any_fixture(self):
        self.args.io_backend = 'typo'
        with self.assertRaisesRegex(AssertionError, 'Invalid requested I/O backend'):
            stress.Stress(self.args)
        self.assertFalse(self.args.root.exists())
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_explicit_backend_is_persisted_despite_inherited_environment_filter(self):
        self.args.io_backend = 'uring'
        self.args.deadline_seconds = 600
        self.args.binary = self.base / 'candidate'
        self.args.binary.write_bytes(b'fixture-only-not-an-engine')
        with patch.object(stress.subprocess, 'run'), \
                patch.object(stress.ssl, 'create_default_context'), \
                patch.object(stress.threading, 'Thread'):
            fixture = stress.Stress(self.args)
        self.assertIn('N1_IO_BACKEND=uring\n', (fixture.root / 'n1.env').read_text())
        self.assertEqual(fixture.report['requested_io_backend'], 'uring')
        fixture.call = lambda *args, **kwargs: (200, b'')
        with patch.object(stress.os, 'environ', {'N1_IO_BACKEND': 'blocking'}), \
                patch.object(stress.subprocess, 'Popen') as start:
            start.return_value.poll.return_value = None
            fixture.start('backend-selection')
        environment = start.call_args.kwargs['env']
        self.assertNotIn('N1_IO_BACKEND', environment)
        self.assertEqual(environment['N1_REFRESH_STORE_ROOT'], str(fixture.root / 'refresh'))
        self.assertIn('N1_IO_BACKEND=uring\n', (fixture.root / 'n1.env').read_text())

    def test_same_filesystem_members_are_rejected_before_creation(self):
        self.args.member_parent.append(self.base)
        self.args.parity_shards = 1
        with self.assertRaisesRegex(AssertionError, 'distinct filesystems'):
            stress.Stress(self.args)
        self.assertFalse(self.args.root.exists())
        self.assertEqual(list(self.parent.iterdir()), [])

    def test_enrolled_ancestor_is_rejected_before_creation(self):
        (self.parent/'.n1-pool.json').write_text('{}')
        with self.assertRaisesRegex(AssertionError, 'enrolled storage root'):
            stress.Stress(self.args)
        self.assertFalse(self.args.root.exists())

    def test_invalid_member_count_is_rejected_before_creation(self):
        self.args.parity_shards = 1
        with self.assertRaises(AssertionError):
            stress.Stress(self.args)
        self.assertFalse(self.args.root.exists())

    def test_occupied_port_is_rejected_before_creation(self):
        with socket.socket() as owner:
            owner.bind(('127.0.0.1', 0))
            self.args.port = owner.getsockname()[1]
            with self.assertRaises(OSError):
                stress.Stress(self.args)
        self.assertFalse(self.args.root.exists())

    def test_optimized_python_cannot_disable_safety_checks(self):
        result = subprocess.run([sys.executable, '-O', str(SCRIPT), '--help'], capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'safety assertions must remain enabled', result.stderr)


if __name__ == '__main__':
    unittest.main()
