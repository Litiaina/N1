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
