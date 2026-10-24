#!/usr/bin/env python3
"""Pure fixture/parser tests; these do not start or qualify an N1 engine."""
import copy
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location('small_batch_proof_tests', Path(__file__).with_name('n1-small-batch-proof.py'))
PROOF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROOF)
VERSION = '01ARZ3NDEKTSV4RRFFQ69G5FAV'


class LifecycleTests(unittest.TestCase):
    def test_startup_and_owned_stop_have_explicit_bounded_defaults(self):
        self.assertEqual(PROOF.STARTUP_TIMEOUT_SECONDS, 120)
        self.assertEqual(PROOF.STOP_TIMEOUT_SECONDS, 120)

    def test_stop_timeout_retains_exact_process_ownership(self):
        fixture = object.__new__(PROOF.SmallBatchProof)
        fixture.args = SimpleNamespace(stop_timeout_seconds=120)
        fixture.process = process = Mock(pid=123)
        process.poll.return_value = None
        process.wait.side_effect = subprocess.TimeoutExpired('owned-engine', 120)
        with self.assertRaises(subprocess.TimeoutExpired):
            fixture.stop()
        self.assertIs(fixture.process, process)
        process.kill.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=120)

    def test_finish_preserves_original_failure_and_cleanup_timeout(self):
        with tempfile.TemporaryDirectory(prefix='n1-proof-finalize-') as directory:
            fixture = object.__new__(PROOF.SmallBatchProof)
            fixture.root = Path(directory)
            fixture.process = Mock(pid=123)
            fixture.stop_monitor = threading.Event()
            fixture.monitor = Mock()
            fixture.stop = Mock(side_effect=subprocess.TimeoutExpired('owned-engine', 120))
            fixture.samples = []
            fixture.started = time.monotonic()
            fixture.report = dict(status='FAIL', fault='original startup failure')
            try:
                raise AssertionError('original startup failure')
            except AssertionError:
                with contextlib.redirect_stdout(io.StringIO()):
                    fixture.finish()
            report = json.loads((fixture.root/'results.json').read_text())
            self.assertEqual(report['status'], 'FAIL')
            self.assertEqual(report['fault'], 'original startup failure')
            self.assertIn('original startup failure', report['original_error'])
            self.assertIn('TimeoutExpired', report['cleanup_error'])
            self.assertTrue(report['owned_engine_exit_unconfirmed'])
            self.assertEqual(report['owned_engine_pid'], 123)
            self.assertTrue((fixture.root/'resource-samples.json').is_file())


def success_reply(fields):
    results = [dict(object_key=item['key'], status=201, version_id=VERSION,
                    size_bytes=len(item['data'])) for item in fields]
    return dict(requested=len(fields), succeeded=len(fields), failed=0, results=results)


def synthetic_report():
    phases = [dict(name=name, status='PASS') for name in PROOF.CORE_PHASES]
    for phase in phases[:5]:
        phase['group_sync_deltas'] = {label: 1 for label in PROOF.SYNC_LABELS}
    for name in PROOF.FAULT_PHASES:
        phases.append(dict(name=name, status='PASS', injected_round_observed=int(name[-1]),
                           successful_acknowledgements=0, prior_acknowledgements_verified=107,
                           all_actual_syncfs_completed=True, objects_per_fault_group=64,
                           existing_update_exercised=True, every_visible_attempt_verified=True))
    return dict(status='PASS', fault=None, strict_uploads=True, automatic_write_retries=False,
                configured_pool_verified=True, acknowledged_versions=107,
                group_sync_execution_verified=True,
                fault_injection=dict(eio_rounds=[1, 2, 3], sigkill_rounds=[1, 2, 3],
                    successful_acknowledgements=0, all_prior_acks_verified=True, test_only_ld_preload=True),
                historical_hash_verifications=214, phases=phases)


def fault_trace(number, width, kind):
    return [dict(event='syncfs', call=call, round=(call-1)//width+1,
                 fd=(call-1)%width+10, device=(call-1)%width+100,
                 actual_result=0, actual_errno=0,
                 returned_result=-1 if kind == 'eio' and (call-1)//width+1 == number else 0,
                 errno=5 if kind == 'eio' and (call-1)//width+1 == number else 0)
            for call in range(1, number*width+1)]


class BodyTests(unittest.TestCase):
    def test_explicit_percent_encoded_object_keys_and_input_order(self):
        fields = [PROOF.field('folder/space name.bin', 1), PROOF.field('unicode/ñ.bin', 0)]
        body = PROOF.multipart(fields, 'safe-boundary')
        self.assertIn(b'name="object"; filename="folder%2Fspace%20name.bin"', body)
        self.assertIn(b'filename="unicode%2F%C3%B1.bin"', body)
        self.assertLess(body.index(b'folder%2F'), body.index(b'unicode%2F'))
        self.assertTrue(body.endswith(b'--safe-boundary--\r\n'))

    def test_invalid_field_name_and_absent_filename_can_be_exercised(self):
        body = PROOF.multipart([PROOF.field('missing', 0, name='wrong', missing_filename=True)], 'boundary')
        self.assertIn(b'name="wrong"', body)
        self.assertNotIn(b'filename=', body)

    def test_injected_body_headers_are_rejected(self):
        with self.assertRaises(AssertionError):
            PROOF.multipart([PROOF.field('x', 0, name='object\r\nInjected: true')], 'boundary')
        with self.assertRaises(AssertionError):
            PROOF.multipart([PROOF.field('x', 0)], 'bad\r\n')

    def test_canonical_aliases_and_parent_escape(self):
        self.assertEqual(PROOF.canonical('a//b/file.bin'), 'a/b/file.bin')
        for key in ('/', '', '../x', 'a/./b'):
            with self.assertRaises(AssertionError):
                PROOF.canonical(key)


class ReplyTests(unittest.TestCase):
    def setUp(self):
        self.fields = [PROOF.field('one.bin', 0), PROOF.field('two.bin', 4097)]
        self.reply = success_reply(self.fields)

    def validate(self, status=201):
        return PROOF.validate_reply(status, json.dumps(self.reply).encode(), self.fields)

    def test_successful_wire_schema_and_zero_length(self):
        self.assertEqual(self.validate()['succeeded'], 2)

    def test_partial_failure_is_not_hidden_by_overall_http_success(self):
        self.fields[1]['status'] = 413
        self.reply.update(succeeded=1, failed=1)
        self.reply['results'][1] = dict(object_key='two.bin', status=413, error='too large')
        self.validate(207)
        with self.assertRaises(AssertionError):
            self.validate(201)

    def test_empty_workload_cannot_pass(self):
        with self.assertRaises(AssertionError):
            PROOF.validate_reply(201, b'{"requested":0,"succeeded":0,"failed":0,"results":[]}', [])

    def test_missing_or_extra_schema_fields_reject(self):
        for key in ('requested', 'results'):
            changed = copy.deepcopy(self.reply)
            del changed[key]
            with self.assertRaises(AssertionError):
                PROOF.validate_reply(201, json.dumps(changed), self.fields)
        self.reply['new_schema'] = True
        with self.assertRaises(AssertionError):
            self.validate()

    def test_incorrect_requested_and_success_counts_reject(self):
        for key in ('requested', 'succeeded', 'failed'):
            changed = copy.deepcopy(self.reply)
            changed[key] += 1
            with self.assertRaises(AssertionError):
                PROOF.validate_reply(201, json.dumps(changed), self.fields)

    def test_boolean_counts_do_not_satisfy_integer_contract(self):
        self.reply['failed'] = False
        with self.assertRaises(AssertionError):
            self.validate()

    def test_wrong_order_size_version_or_status_reject(self):
        for key, value in (('object_key', 'elsewhere'), ('size_bytes', 7), ('version_id', 'not-a-version'), ('status', 200)):
            changed = copy.deepcopy(self.reply)
            changed['results'][0][key] = value
            with self.assertRaises(AssertionError):
                PROOF.validate_reply(201, json.dumps(changed), self.fields)

    def test_failed_field_cannot_claim_a_success_version(self):
        self.fields[1]['status'] = 413
        self.reply.update(succeeded=1, failed=1)
        self.reply['results'][1].update(status=413, error='too large')
        with self.assertRaises(AssertionError):
            self.validate(207)

    def test_explicit_conflict_allowed_only_for_deliberate_namespace_race(self):
        self.reply.update(succeeded=1, failed=1)
        self.reply['results'][1] = dict(object_key='two.bin', status=409, error='identity changed')
        body = json.dumps(self.reply)
        with self.assertRaises(AssertionError):
            PROOF.validate_reply(207, body, self.fields)
        parsed = PROOF.validate_reply(207, body, self.fields, allow_conflicts=True)
        self.assertEqual(parsed['succeeded'], 1)
        self.assertEqual(parsed['failed'], 1)
        self.assertNotIn('version_id', parsed['results'][1])

    def test_namespace_race_does_not_hide_unexpected_errors_or_fake_acknowledgements(self):
        self.reply.update(succeeded=1, failed=1)
        for status in (400, 404, 429, 500, 503):
            self.reply['results'][1] = dict(object_key='two.bin', status=status, error='unexpected')
            with self.assertRaises(AssertionError):
                PROOF.validate_reply(207, json.dumps(self.reply), self.fields, allow_conflicts=True)
        self.reply['results'][1] = dict(object_key='two.bin', status=409, error='conflict', version_id=VERSION)
        with self.assertRaises(AssertionError):
            PROOF.validate_reply(207, json.dumps(self.reply), self.fields, allow_conflicts=True)


class EvidenceTests(unittest.TestCase):
    def test_real_group_counter_deltas_are_required(self):
        old = {label: (0, 0) for label in PROOF.SYNC_LABELS}
        new = {label: (2, 0) for label in PROOF.SYNC_LABELS}
        self.assertTrue(PROOF.group_delta(old, new, expected=2))
        for changed in (old, dict(new, **{PROOF.SYNC_LABELS[0]: (2, 1)}),
                        dict(new, **{PROOF.SYNC_LABELS[0]: (1, 0)})):
            with self.assertRaises(AssertionError):
                PROOF.group_delta(old, changed, expected=2)

    def test_old_immutable_bytes_and_inode_identity_must_remain(self):
        before = {'file': (1, 2, 3, 4, 'sha')}
        self.assertEqual(PROOF.retain_inventory(before, dict(before, new='okay')), 1)
        for after in ({}, {'file': (9, 2, 3, 4, 'sha')}, {'file': (1, 2, 3, 4, 'changed')}):
            with self.assertRaises(AssertionError):
                PROOF.retain_inventory(before, after)

    def test_fault_markers_and_all_member_real_sync_calls_are_required(self):
        for kind in ('eio', 'sigkill'):
            for number in (1, 2, 3):
                marker = dict(round=number, width=4, completed=4, after_real_syncfs=True)
                trace = fault_trace(number, 4, kind)
                self.assertTrue(PROOF.validate_fault_evidence(marker, trace, number, 4, kind, {100, 101, 102, 103}))
                with self.assertRaises(AssertionError):
                    PROOF.validate_fault_evidence(marker, trace[:-1], number, 4, kind, {100, 101, 102, 103})
                changed = copy.deepcopy(trace)
                changed[-1]['actual_result'] = -1
                with self.assertRaises(AssertionError):
                    PROOF.validate_fault_evidence(marker, changed, number, 4, kind, {100, 101, 102, 103})

    def test_mock_marker_alone_without_syscalls_is_not_a_proof(self):
        marker = dict(round=1, width=4, completed=4, after_real_syncfs=True)
        with self.assertRaises(AssertionError):
            PROOF.validate_fault_evidence(marker, [], 1, 4, 'eio', {100, 101, 102, 103})

    def test_repeated_missing_or_changed_filesystem_descriptors_reject(self):
        marker = dict(round=3, width=4, completed=4, after_real_syncfs=True)
        for field, value in (('device', 100), ('fd', 10), ('device', 999)):
            changed = fault_trace(3, 4, 'eio')
            changed[-1][field] = value
            with self.assertRaises(AssertionError):
                PROOF.validate_fault_evidence(marker, changed, 3, 4, 'eio', {100, 101, 102, 103})
        changed = fault_trace(3, 4, 'eio')
        for row in changed[-4:]:
            row['fd'] += 100
        with self.assertRaises(AssertionError):
            PROOF.validate_fault_evidence(marker, changed, 3, 4, 'eio', {100, 101, 102, 103})

    def test_synthetic_complete_record_parser_only(self):
        self.assertEqual(PROOF.require_terminal(synthetic_report())['status'], 'PASS')

    def test_incomplete_ignored_reordered_or_zero_ack_record_reject(self):
        for change in ('missing', 'ignored', 'reordered', 'zero-ack', 'missing-fault', 'running', 'zero-hashes'):
            report = synthetic_report()
            if change == 'missing':
                report['phases'].pop()
            elif change == 'ignored':
                report['phases'][0]['status'] = 'IGNORED'
            elif change == 'reordered':
                report['phases'][0], report['phases'][1] = report['phases'][1], report['phases'][0]
            elif change == 'zero-ack':
                report['acknowledged_versions'] = 0
            elif change == 'missing-fault':
                report['fault'] = 'fault marker missing'
            elif change == 'running':
                report['status'] = 'RUNNING'
            else:
                report['historical_hash_verifications'] = 0
            with self.assertRaises(AssertionError, msg=change):
                PROOF.require_terminal(report)

    def test_missing_group_or_injected_failure_evidence_reject(self):
        for change in ('zero-group', 'missing-label', 'false-native', 'acked-failed-group'):
            report = synthetic_report()
            if change == 'zero-group':
                report['phases'][0]['group_sync_deltas'][PROOF.SYNC_LABELS[0]] = 0
            elif change == 'missing-label':
                report['phases'][0]['group_sync_deltas'].pop(PROOF.SYNC_LABELS[0])
            elif change == 'false-native':
                report['phases'][-1]['all_actual_syncfs_completed'] = False
            else:
                report['phases'][-1]['successful_acknowledgements'] = 1
            with self.assertRaises(AssertionError, msg=change):
                PROOF.require_terminal(report)


if __name__ == '__main__':
    unittest.main()
