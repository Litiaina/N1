"""Matched comparison safety checks; never starts an engine or a disk workload."""
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('commit_compare', Path(__file__).with_name('n1-hdd-commit-compare.py'))
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class CommitCompareSafety(unittest.TestCase):
    def test_copy_uses_optional_reflink_not_a_requirement_on_ext4(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, destination = Path(temporary) / 'source', Path(temporary) / 'destination'
            source.mkdir()
            destination.mkdir()
            with patch.object(compare.subprocess, 'run') as run:
                compare.copy_member(source, destination)
            run.assert_called_once_with(['cp', '-a', '--reflink=auto', str(source) + '/.', str(destination)], check=True)

    def test_copy_refuses_existing_contents_or_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, destination = Path(temporary) / 'source', Path(temporary) / 'destination'
            source.mkdir()
            destination.mkdir()
            (destination / 'preserve').touch()
            with patch.object(compare.subprocess, 'run') as run:
                with self.assertRaises(AssertionError):
                    compare.copy_member(source, destination)
                link = Path(temporary) / 'link'
                link.symlink_to(source)
                with self.assertRaises(AssertionError):
                    compare.copy_member(link, destination)
                run.assert_not_called()

    def test_evidence_detects_changed_original_without_mutating_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            report = fixture / 'results.json'
            report.write_text(json.dumps({'status': 'FAIL'}))
            evidence = compare.evidence_hashes(fixture)
            self.assertTrue(compare.unchanged(evidence))
            report.write_text(json.dumps({'status': 'PASS'}))
            self.assertFalse(compare.unchanged(evidence))

    def test_authentication_refreshes_without_write_retry(self):
        fixture = compare.Clone.__new__(compare.Clone)
        fixture.token, fixture.last_auth = '', 0
        fixture.auth_lock = threading.Lock()
        fixture.fragment, fixture.master = 'isolated', 'not-a-real-key'
        fixture.api = Mock(return_value={'access_token': 'first'})
        with patch.object(compare.time, 'monotonic', return_value=10):
            fixture.authenticate()
        self.assertEqual(fixture.token, 'first')
        with patch.object(compare.time, 'monotonic', return_value=609):
            fixture.authenticate()
        self.assertEqual(fixture.api.call_count, 1)
        fixture.api.return_value = {'access_token': 'second'}
        with patch.object(compare.time, 'monotonic', return_value=610):
            fixture.authenticate()
        self.assertEqual(fixture.api.call_count, 2)
        self.assertEqual(fixture.token, 'second')

    def test_reconciliation_precedes_comparison_and_never_uploads(self):
        fixture = compare.Clone.__new__(compare.Clone)
        fixture.start = Mock()
        fixture.authenticate = Mock()
        fixture.verify_all, fixture.resolve_ambiguous, fixture.snapshot_verify = Mock(), Mock(), Mock()
        fixture.report, fixture.fault = {}, None
        phases = []
        fixture.phase_run = lambda name, action: (phases.append(name), action())
        fixture.reconcile()
        self.assertEqual(phases, ['all-acknowledged-version-hashes-after-restart',
                                  'ambiguous-write-outcomes', 'complete-namespace-ledger-comparison'])
        self.assertEqual(fixture.report['status'], 'PASS')

    def test_initial_comparison_label_does_not_claim_gc_is_absent(self):
        fixture = compare.Clone.__new__(compare.Clone)
        fixture.start, fixture.stop, fixture.authenticate = Mock(), Mock(), Mock()
        fixture.snapshot_verify = Mock()
        fixture.report, fixture.fault = {}, None
        fixture.args = SimpleNamespace(objects=2)
        phases = []
        fixture.phase_run = lambda name, action: phases.append(name)
        fixture.compare()
        self.assertIn('strict-initial-load', phases)
        self.assertNotIn('strict-foreground-only', phases)
        self.assertIn('strict-with-normal-scheduled-gc', phases)

    def test_initial_load_reports_observed_gc_independently_of_requirement(self):
        for running in (False, True):
            with self.subTest(running=running), tempfile.TemporaryDirectory() as temporary:
                fixture = compare.Clone.__new__(compare.Clone)
                fixture.root = Path(temporary)
                fixture.args = SimpleNamespace(batch_size=2, batch_clients=1, request_timeout_seconds=1)
                fixture.counts = {'source_objects': 0}
                observed = threading.Event()
                def telemetry():
                    if threading.current_thread() is not threading.main_thread():
                        observed.set()
                    return dict(gc=dict(is_running=running, start_timestamp=123))
                fixture.telemetry = telemetry
                def upload(interval):
                    self.assertTrue(observed.wait(2), 'Missing upload-time GC observation')
                    fixture.counts['source_objects'] += interval[1] - interval[0]
                fixture.upload_batch = upload
                result = fixture.ingest(0, 2)
                self.assertFalse(result['scheduled_gc_required'])
                self.assertEqual(result['scheduled_gc_overlap'], running)
                self.assertGreaterEqual(result['telemetry_samples'], 1)
                self.assertEqual(result['scheduled_gc_overlap_samples'], result['telemetry_samples'] if running else 0)

    def test_required_gc_phase_needs_upload_observation_and_cycle_completion(self):
        for overlap in (False, True):
            with self.subTest(overlap=overlap), tempfile.TemporaryDirectory() as temporary:
                fixture = compare.Clone.__new__(compare.Clone)
                fixture.root = Path(temporary)
                fixture.args = SimpleNamespace(batch_size=2, batch_clients=1,
                                               request_timeout_seconds=1, gc_wait_seconds=1)
                fixture.counts = {'source_objects': 0}
                fixture.wait_for_scheduled_gc = Mock(return_value=dict(gc=dict(is_running=True, start_timestamp=123)))
                observed = threading.Event()
                def telemetry():
                    if threading.current_thread() is not threading.main_thread():
                        observed.set()
                        return dict(gc=dict(is_running=overlap, start_timestamp=123))
                    return dict(gc=dict(is_running=False, start_timestamp=123, total_errors=0, last_error=None))
                fixture.telemetry = telemetry
                def upload(interval):
                    self.assertTrue(observed.wait(2))
                    fixture.counts['source_objects'] += interval[1] - interval[0]
                fixture.upload_batch = upload
                if overlap:
                    result = fixture.ingest(0, 2, require_gc=True)
                    self.assertTrue(result['scheduled_gc_required'])
                    self.assertTrue(result['scheduled_gc_overlap'])
                    self.assertGreaterEqual(result['scheduled_gc_overlap_samples'], 1)
                else:
                    with self.assertRaisesRegex(AssertionError, 'No scheduled GC overlapped'):
                        fixture.ingest(0, 2, require_gc=True)
                fixture.wait_for_scheduled_gc.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
