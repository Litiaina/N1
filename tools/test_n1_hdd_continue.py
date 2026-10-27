"""Continuation safety/protocol tests; no engine or physical disk I/O."""
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('hdd_continue', Path(__file__).with_name('n1-hdd-continue.py'))
continuation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(continuation)
topology_spec = importlib.util.spec_from_file_location('continue_topology', Path(__file__).with_name('test_n1_hdd_recheck_topology.py'))
topology = importlib.util.module_from_spec(topology_spec)
topology_spec.loader.exec_module(topology)

OBJECT = '01ARZ3NDEKTSV4RRFFQ69G5FAV'
VERSION = '01ARZ3NDEKTSV4RRFFQ69G5FAW'


def pointer_bytes(object_id=OBJECT, pool='A', format_value=4):
    def string(value):
        encoded = value.encode()
        return (bytes([0xa0 + len(encoded)]) if len(encoded) < 32 else bytes([0xd9, len(encoded)])) + encoded
    body = b'\x83' + string('format') + bytes([format_value]) + string('pool_id') + string(pool) + string('object_id') + string(object_id)
    return b'N1NCX4\0' + hashlib.sha256(body).digest() + body


def source_report(objects=1000000):
    return dict(status='FAIL', workload='upload-then-churn', source_layout='flat-folder',
                strict_uploads=True, automatic_write_retries=False,
                counts=dict(source_objects=objects, acknowledged_versions=objects, acknowledged_bytes=3),
                phases=[dict(name=f'complete-version-integrity-at-{objects}', status='PASS',
                             every_acknowledged_version_downloaded_and_hashed=objects),
                        dict(name='complete-quiescent-namespace-inventory', status='FAIL')])


class CandidateGate(unittest.TestCase):
    def gate(self):
        return dict(status='PASS', engine_sha256='c' * 64, source_upload_engine_sha256='a' * 64,
                    phases=[dict(name=name, status='PASS') for name in sorted(continuation.REQUIRED_GATES)])

    def test_all_four_exact_candidate_gate_types_required(self):
        gate = self.gate()
        continuation.validate_gate(gate, 'c' * 64, 'a' * 64)
        for broken in [gate | dict(status='RUNNING'), gate | dict(engine_sha256='b' * 64),
                       gate | dict(source_upload_engine_sha256='b' * 64),
                       gate | dict(phases=gate['phases'][:-1]),
                       gate | dict(phases=gate['phases'] + gate['phases'][:1]),
                       gate | dict(phases=[item | dict(status='FAIL') for item in gate['phases']])]:
            with self.subTest(broken=broken), self.assertRaises(AssertionError):
                continuation.validate_gate(broken, 'c' * 64, 'a' * 64)

    def test_source_must_be_complete_integrity_pass_followed_by_inventory_failure(self):
        report = source_report()
        continuation.validate_population(report, 1000000)
        for broken in [report | dict(status='PASS'), report | dict(workload='upload-only'),
                       report | dict(source_layout='project-tree'), report | dict(strict_uploads=False),
                       report | dict(automatic_write_retries=True),
                       report | dict(counts=report['counts'] | dict(source_objects=999999)),
                       report | dict(counts=report['counts'] | dict(namespace_mutations=1)),
                       report | dict(phases=report['phases'][:-1]),
                       report | dict(phases=[report['phases'][0] | dict(every_acknowledged_version_downloaded_and_hashed=500000), report['phases'][1]])]:
            with self.subTest(broken=broken), self.assertRaises((AssertionError, IndexError)):
                continuation.validate_population(broken, 1000000)

    def test_pin_detects_changed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'gate.json'
            path.write_text('{"status":"PASS"}')
            pin = continuation.recheck.file_digest(path)
            self.assertEqual(continuation.pinned_json(path, pin)['status'], 'PASS')
            path.write_text('{"status":"FAIL"}')
            with self.assertRaisesRegex(AssertionError, 'Pinned evidence changed'):
                continuation.pinned_json(path, pin)

    def test_fresh_seven_day_certificate_is_created_only_in_new_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            test = SimpleNamespace(root=root, config={'server': {'cert_path': '/old/source-cert'}}, write_config=Mock())
            with patch.object(continuation.recheck.subprocess, 'run') as run, \
                    patch.object(continuation.recheck.ssl, 'create_default_context') as context:
                continuation.renew_fixture_certificate(test)
            command = run.call_args.args[0]
            self.assertEqual(command[command.index('-days') + 1], '7')
            self.assertEqual(command[command.index('-keyout') + 1], str(root / 'key.pem'))
            self.assertEqual(command[command.index('-out') + 1], str(root / 'cert.pem'))
            self.assertIn('subjectAltName=IP:127.0.0.1,DNS:localhost', command)
            context.assert_called_once_with(cafile=str(root / 'cert.pem'))
            test.write_config.assert_called_once_with()
            (root / 'cert.pem').touch()
            with patch.object(continuation.recheck.subprocess, 'run') as run, \
                    self.assertRaisesRegex(AssertionError, 'Never replace'):
                continuation.renew_fixture_certificate(test)
            run.assert_not_called()


class ExactProbeIdentity(unittest.TestCase):
    def test_fixed_pointer_map_and_string8_fields_are_decoded(self):
        self.assertEqual(continuation.decode_probe_pointer(pointer_bytes()), dict(format=4, pool_id='A', object_id=OBJECT))
        data = pointer_bytes()
        body = data[39:].replace(b'\xba' + OBJECT.encode(), b'\xd9\x1a' + OBJECT.encode())
        encoded = b'N1NCX4\0' + hashlib.sha256(body).digest() + body
        self.assertEqual(continuation.decode_probe_pointer(encoded)['object_id'], OBJECT)

    def test_corrupt_oversized_truncated_wrong_pool_and_identity_fail_closed(self):
        valid = pointer_bytes()
        for data in [b'', valid[:-1], valid[:45] + bytes([valid[45] ^ 1]) + valid[46:],
                     valid + b'\0', b'x' * 4097, pointer_bytes(pool='B'), pointer_bytes(format_value=3),
                     pointer_bytes(object_id='../escaped'), pointer_bytes(object_id='U' * 26)]:
            with self.subTest(data=data[:45]), self.assertRaises((AssertionError, UnicodeDecodeError)):
                continuation.decode_probe_pointer(data)

    def fixture(self, base):
        roots = [base / str(i) for i in range(4)]
        digest = hashlib.sha256(OBJECT.encode()).hexdigest()
        for slot, root in enumerate(roots):
            fragment = root / '@f_stable'
            fragment.mkdir(parents=True)
            (root / '.fragment').mkdir()
            (root / '.fragment' / 'alias').symlink_to('../@f_stable')
            namespace = fragment / '.namespace' / 'churn.ncp'
            namespace.mkdir(parents=True)
            if slot < 2:
                (namespace / 'probe-0000.ncx').write_bytes(pointer_bytes())
            directory = fragment / '.objects' / digest[:2] / digest[2:4] / ('@o_' + OBJECT)
            (directory / VERSION).mkdir(parents=True)
        return roots

    def test_exact_fanout_matches_engine_without_any_recursive_population_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            roots = self.fixture(Path(directory))
            with patch.object(Path, 'glob', side_effect=AssertionError('No population discovery')):
                found = continuation.probe_object_dirs(roots, 'alias', 'churn/probe-0000', VERSION, 2)
            self.assertEqual(len(found), 4)
            fanout = hashlib.sha256(OBJECT.encode()).hexdigest()
            self.assertTrue(all(path.relative_to(root).parts == ('@f_stable', '.objects', fanout[:2], fanout[2:4], '@o_' + OBJECT)
                                for path, root in zip(found, roots)))

    def test_replica_conflict_missing_version_or_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            roots = self.fixture(Path(directory))
            ncx = roots[1] / '@f_stable/.namespace/churn.ncp/probe-0000.ncx'
            ncx.write_bytes(pointer_bytes(object_id=VERSION))
            with self.assertRaisesRegex(AssertionError, 'Conflicting'):
                continuation.probe_object_dirs(roots, 'alias', 'churn/probe-0000', VERSION, 2)
            ncx.write_bytes(pointer_bytes())
            digest = hashlib.sha256(OBJECT.encode()).hexdigest()
            version = roots[-1] / '@f_stable/.objects' / digest[:2] / digest[2:4] / ('@o_' + OBJECT) / VERSION
            version.rmdir()
            with self.assertRaisesRegex(AssertionError, 'version missing'):
                continuation.probe_object_dirs(roots, 'alias', 'churn/probe-0000', VERSION, 2)
            version.symlink_to(version.parent, target_is_directory=True)
            with self.assertRaisesRegex(AssertionError, 'version missing'):
                continuation.probe_object_dirs(roots, 'alias', 'churn/probe-0000', VERSION, 2)


class ContinuationOrder(unittest.TestCase):
    def test_no_probe_upload_or_mutation_until_complete_inventory_passes(self):
        test = continuation.Continue.__new__(continuation.Continue)
        test.args = SimpleNamespace(probes=1000, objects=1000000, churn_mutations=1000000)
        test.start = Mock()
        test.resource_checkpoint = Mock(return_value={})
        test.snapshot_verify = Mock(side_effect=AssertionError('Incomplete snapshot'))
        test.setup_probes = Mock()
        test.churn = Mock()
        test.phases = []
        test.phase_run = lambda _, operation: operation()
        with self.assertRaisesRegex(AssertionError, 'Incomplete snapshot'):
            test.run()
        test.setup_probes.assert_not_called()
        test.churn.assert_not_called()

    def test_complete_tail_includes_all_churn_levels_and_hashes_after_sigkill(self):
        test = continuation.Continue.__new__(continuation.Continue)
        test.args = SimpleNamespace(probes=1000, objects=1000000, churn_mutations=1000000)
        calls = []
        test.start = lambda name: calls.append('start:' + name)
        test.stop = lambda: calls.append('SIGKILL')
        test.resource_checkpoint = Mock(return_value={})
        test.snapshot_verify = Mock(return_value={})
        test.setup_probes = Mock(return_value={})
        test.verify_all = Mock(return_value={})
        test.churn = lambda cycles, concurrency: test.counts.update(namespace_mutations=1000 * cycles * 4)
        test.phase_run = lambda name, operation: (calls.append(name), operation())[-1]
        test.counts, test.fault, test.report = Counter(source_objects=1000000), None, {}
        test.run()
        self.assertEqual(test.report['status'], 'PASS')
        self.assertEqual(test.counts['namespace_mutations'], 1008000)
        self.assertEqual([name for name in calls if name.startswith('high-churn')],
                         ['high-churn-c8', 'high-churn-c32', 'high-churn-c128', 'high-churn-c256'])
        self.assertLess(calls.index('complete-quiescent-namespace-inventory'), calls.index('post-population-probe-object-setup'))
        self.assertLess(calls.index('SIGKILL'), calls.index('all-version-integrity-after-final-sigkill'))
        self.assertLess(calls.index('all-version-integrity-after-final-sigkill'), calls.index('quiescent-namespace-after-final-sigkill'))

    def test_authentication_is_scoped_write_enabled_and_refreshes(self):
        test = continuation.Continue.__new__(continuation.Continue)
        test.token, test.last_auth, test.fragment, test.master = '', 0, 'test-fragment', 'fixture-secret'
        test.auth_lock = __import__('threading').Lock()
        test.api = Mock(return_value={'access_token': 'scoped-token'})
        with patch.object(continuation.time, 'monotonic', return_value=1000):
            test.authenticate()
            test.authenticate()
        self.assertEqual(test.api.call_count, 1)
        _, route, payload = test.api.call_args.args
        self.assertEqual(route, '/auth/fragment')
        self.assertTrue(all(payload[key] for key in ('read', 'write', 'modify', 'delete')))
        self.assertEqual(payload['fragment'], 'test-fragment')
        with patch.object(continuation.time, 'monotonic', return_value=1601):
            test.authenticate()
        self.assertEqual(test.api.call_count, 2)


class OwnedEngineSampling(unittest.TestCase):
    def fixture(self):
        test = continuation.scale.Scale.__new__(continuation.scale.Scale)
        test.lock = __import__('threading').Lock()
        test.stop_monitor = __import__('threading').Event()
        test.fault = None
        test.process = Mock(pid=123)
        test.process.poll.return_value = None
        return test

    def test_intentional_stop_during_poll_cannot_be_reported_as_an_engine_crash(self):
        test = self.fixture()
        process = test.process
        def stopped():
            with test.lock:
                test.process = None
            return -9
        process.poll.side_effect = stopped
        continuation.scale.record_unexpected_engine_exit(test, process)
        self.assertIsNone(test.fault)
        self.assertIsNone(test.process)

    def test_old_generation_exit_does_not_fault_a_new_engine(self):
        test = self.fixture()
        previous = test.process
        previous.poll.return_value = -9
        test.process = Mock(pid=456)
        continuation.scale.record_unexpected_engine_exit(test, previous)
        self.assertIsNone(test.fault)

    def test_actual_owned_exit_fails_without_overwriting_an_existing_failure(self):
        test = self.fixture()
        test.process.poll.return_value = 17
        continuation.scale.record_unexpected_engine_exit(test, test.process)
        self.assertEqual(test.fault, 'Engine exited unexpectedly with code 17')
        test.fault = 'Original integrity failure'
        continuation.scale.record_unexpected_engine_exit(test, test.process)
        self.assertEqual(test.fault, 'Original integrity failure')

    def test_stop_unpublishes_under_lock_before_the_child_is_killed(self):
        test = self.fixture()
        process = test.process
        def killed():
            self.assertIsNone(test.process)
            self.assertTrue(test.lock.acquire(blocking=False), 'Do not hold state lock while waiting on I/O')
            test.lock.release()
        process.kill.side_effect = killed
        test.stop()
        process.kill.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=15)
        self.assertIsNone(test.process)

    def test_no_engine_and_stopped_sampler_are_not_crashes(self):
        test = self.fixture()
        test.process = None
        continuation.scale.record_unexpected_engine_exit(test, None)
        test.process = process = Mock()
        process.poll.return_value = -9
        test.stop_monitor.set()
        continuation.scale.record_unexpected_engine_exit(test, process)
        self.assertIsNone(test.fault)


class InPlaceSafety(topology.FixtureTopology):
    def test_continuation_reuses_exact_roots_never_copies_or_changes_source_evidence(self):
        fixture, parents, roots, original_binary = self.fixture()
        path = fixture / 'results.json'
        report = json.loads(path.read_text()) | source_report(2)
        path.write_text(json.dumps(report))
        candidate = parents[0] / 'candidate'
        candidate.write_bytes(b'new-qualified-binary')
        gate = parents[0] / 'correctness.json'
        gate.write_text(json.dumps(dict(status='PASS', engine_sha256=continuation.recheck.file_digest(candidate),
            source_upload_engine_sha256=report['engine_sha256'],
            phases=[dict(name=name, status='PASS') for name in sorted(continuation.REQUIRED_GATES)])))
        args = SimpleNamespace(root=parents[0] / 'continued-evidence', fixture=fixture, member_parent=parents,
            binary=candidate, source_engine_sha256=report['engine_sha256'], candidate_sha256=continuation.recheck.file_digest(candidate),
            source_result_sha256=continuation.recheck.file_digest(path), correctness_result=gate,
            correctness_result_sha256=continuation.recheck.file_digest(gate), clone=False, clone_member_parent=None,
            objects=2, probes=1000, churn_mutations=1000000, port=59241, free_floor_gib=140,
            max_filesystem_growth_gib=64, deadline_seconds=172800)
        before = {path: continuation.recheck.file_digest(path) for path in fixture.iterdir() if path.is_file()}
        original_read_text = Path.read_text
        def read_text(path, *args, **kwargs):
            if str(path).startswith('/sys/fs/cgroup'):
                return '4294967296' if path.name == 'memory.max' else '0'
            return original_read_text(path, *args, **kwargs)
        with self.authority(parents), patch.object(continuation.scale, 'require_storage_filesystem'), \
                patch.object(continuation.scale, 'independent_member_devices', return_value=parents), \
                patch.object(continuation.recheck.socket, 'socket'), patch.object(continuation.recheck.ssl, 'create_default_context'), \
                patch.object(continuation.recheck.subprocess, 'run') as copy, patch.object(continuation.threading, 'Thread'), \
                patch.object(Path, 'read_text', read_text):
            test = continuation.Continue(args)
        try:
            self.assertEqual(copy.call_count, 1)
            self.assertEqual(copy.call_args.args[0][0], 'openssl', 'Only a fresh certificate, never a member/control copy')
            self.assertEqual(test.roots, roots)
            self.assertEqual(test.config['disk_config']['fs_storage_path'], str(fixture / 'control'))
            self.assertEqual(test.config['server']['cert_path'], str(args.root / 'cert.pem'))
            self.assertFalse(test.report['member_storage_copied'])
            self.assertFalse(test.report['source_objects_reuploaded'])
            self.assertFalse(test.report['original_report_replaced'])
            self.assertEqual(test.report['source_upload_engine_sha256'], report['engine_sha256'])
            self.assertEqual(test.db.execute('SELECT COUNT(*) FROM acks').fetchone()[0], 2)
            self.assertEqual(test.counts['source_objects'], 2)
            self.assertTrue(all(continuation.recheck.file_digest(path) == pin for path, pin in before.items()))
        finally:
            test.db.close()
            test.ledger.close()


if __name__ == '__main__':
    unittest.main()
