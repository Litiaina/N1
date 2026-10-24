"""Recovery runner cleanup checks; no engine, devices, or real signals."""
import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, call, patch

_spec = importlib.util.spec_from_file_location('hdd_recovery', Path(__file__).with_name('n1-hdd-recovery.py'))
recovery = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(recovery)


class RecoveryFinalization(unittest.TestCase):
    def test_recovery_accepts_three_plus_one_without_changing_one_plus_one(self):
        recovery.validate_topology(3, 1, 4)
        recovery.validate_topology(1, 1, 2)
        for data, parity, members in ((3, 1, 2), (1, 0, 1), (0, 1, 1), (33, 1, 34)):
            with self.assertRaises(AssertionError):
                recovery.validate_topology(data, parity, members)

    def test_proof_command_preserves_three_plus_one_for_every_mode(self):
        args = SimpleNamespace(proof_binary=Path('/fixture/proof'), binary=Path('/fixture/engine'),
                               data_shards=3, parity_shards=1, port=57521,
                               gc_workload_timeout_seconds=7200)
        roots = [Path(f'/member-{index}/isolated') for index in range(4)]
        for mode in ('auto-healing', 'trash-maintenance', 'gc-safety', 'maintenance-coverage', 'concurrent-upload'):
            command = recovery.proof_command(args, Path('/fixture/source'), roots, Path('/fixture/out'), mode)
            option = lambda name: command[command.index(name) + 1]
            self.assertEqual(option('--data-shards'), '3')
            self.assertEqual(option('--parity-shards'), '1')
            self.assertEqual(option('--disk-roots'), ','.join(map(str, roots)))
            self.assertEqual(option('--mixed-users'), '4')
            self.assertIn('--'+mode+'-proof', command)
            if mode == 'gc-safety':
                self.assertEqual(option('--gc-workload-timeout-seconds'), '7200')
            else:
                self.assertNotIn('--gc-workload-timeout-seconds', command)
            if mode == 'concurrent-upload':
                self.assertEqual(option('--mixed-files-per-user'), '64')
                self.assertEqual(option('--mixed-background-uploads'), '128')
            else:
                self.assertNotIn('--mixed-files-per-user', command)

    def test_owned_nested_directory_on_data_mount_is_allowed(self):
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary)
            nested = mount / 'owned-fixture-parent'
            nested.mkdir()
            with patch.object(recovery.os.path, 'ismount', side_effect=lambda path: path == mount):
                self.assertEqual(recovery.require_data_volume(nested), nested.resolve())

    def test_os_filesystem_fallback_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(recovery.os.path, 'ismount', side_effect=lambda path: path == Path('/')):
                with self.assertRaisesRegex(AssertionError, 'OS filesystem'):
                    recovery.require_data_volume(Path(temporary))

    def test_timeout_keeps_original_error_and_saves_failed_witness(self):
        process = MagicMock(pid=12345)
        process.wait.side_effect = subprocess.TimeoutExpired('owned-proof', 15)
        result = dict(status='FAIL', error='AssertionError: original integrity failure')
        saved = []
        original = AssertionError('original integrity failure')
        with patch.object(recovery.os, 'killpg') as killpg, self.assertRaises(AssertionError) as caught:
            try:
                raise original
            finally:
                recovery.finalize_owned_process(process, result,
                                                lambda: saved.append(json.dumps(result)))
        self.assertIs(caught.exception, original)
        witness = json.loads(saved[0])
        self.assertEqual(witness['status'], 'FAIL')
        self.assertEqual(witness['error'], 'AssertionError: original integrity failure')
        self.assertEqual(witness['cleanup_errors'][0]['owned_proof_pid'], 12345)
        self.assertEqual(witness['cleanup_errors'][0]['wait_timeout_seconds'], 15)
        self.assertTrue(witness['cleanup_errors'][0]['process_exit_not_confirmed'])
        self.assertEqual(process.wait.call_args_list, [call(timeout=15), call(timeout=15)])
        self.assertEqual(killpg.call_args_list, [call(12345, signal.SIGTERM), call(12345, signal.SIGKILL)])

    def test_timeout_changes_pass_to_fail_before_saving_and_raises(self):
        process = MagicMock(pid=12345)
        process.wait.side_effect = subprocess.TimeoutExpired('owned-proof', 15)
        result = dict(status='PASS')
        saved = []
        with patch.object(recovery.os, 'killpg'), self.assertRaises(subprocess.TimeoutExpired):
            recovery.finalize_owned_process(process, result, lambda: saved.append(json.dumps(result)))
        self.assertEqual(json.loads(saved[0])['status'], 'FAIL')

    def test_successful_kill_escalation_keeps_pass(self):
        process = MagicMock(pid=12345)
        process.wait.side_effect = [subprocess.TimeoutExpired('owned-proof', 15), -signal.SIGKILL]
        result = dict(status='PASS')
        save = MagicMock()
        with patch.object(recovery.os, 'killpg') as killpg:
            recovery.finalize_owned_process(process, result, save)
        self.assertEqual(result, dict(status='PASS'))
        save.assert_called_once()
        self.assertEqual(killpg.call_args_list, [call(12345, signal.SIGTERM), call(12345, signal.SIGKILL)])

    def test_already_gone_group_is_reaped_and_evidence_saved(self):
        process = MagicMock(pid=12345)
        process.wait.return_value = 0
        result = dict(status='PASS')
        save = MagicMock()
        with patch.object(recovery.os, 'killpg', side_effect=ProcessLookupError):
            recovery.finalize_owned_process(process, result, save)
        self.assertEqual(result['status'], 'PASS')
        save.assert_called_once()
        process.wait.assert_called_once_with(timeout=15)

    def test_no_process_still_saves_results(self):
        result = dict(status='FAIL', error='setup failure')
        save = MagicMock()
        with patch.object(recovery.os, 'killpg') as killpg:
            recovery.finalize_owned_process(None, result, save)
        killpg.assert_not_called()
        save.assert_called_once()

    def test_save_error_does_not_mask_integrity_failure(self):
        result = dict(status='FAIL', error='AssertionError: integrity failure')
        original = AssertionError('integrity failure')
        with patch('builtins.print'), self.assertRaises(AssertionError) as caught:
            try:
                raise original
            finally:
                recovery.finalize_owned_process(None, result, MagicMock(side_effect=OSError('write failure')))
        self.assertIs(caught.exception, original)
        self.assertEqual(result['error'], 'AssertionError: integrity failure')
        self.assertEqual(result['cleanup_errors'][0]['stage'], 'write-results')


if __name__ == '__main__':
    unittest.main()
