"""Pure fixture safety checks: no engine, block-device mutation, or server calls."""
from collections import Counter
import configparser
from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('topology_compare', Path(__file__).with_name('n1-hdd-commit-compare.py'))
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)
recheck = compare.recheck


class FixtureTopology(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)

    def fixture(self, data=3, parity=1):
        parents = [self.base / f'device-{slot}' for slot in range(data + parity)]
        for parent in parents:
            parent.mkdir()
        fixture = parents[0] / 'failed-evidence'
        fixture.mkdir()
        control = fixture / 'control'
        control.mkdir()
        roots = [parent / f'n1-rc-stress-source-member-{slot}' for slot, parent in enumerate(parents)]
        members = [f'identity-{slot}' for slot in range(len(roots))]
        for slot, root in enumerate(roots):
            root.mkdir()
            descriptor = dict(member_format='n1-pool-member-v1', member_slot=slot, member_id=members[slot],
                              id='A', generation=1, data_shards=data, parity_shards=parity,
                              roots=list(map(str, roots)), members=members, sealed=False)
            (root / '.n1-pool.json').write_text(json.dumps(descriptor))
            (root / '.n1-root-id').write_text(json.dumps(dict(engine_format='n1-4-root-identity-v1', root_id=members[slot])))
            (root / '@f_stable').mkdir()
            (root / '@f_stable' / '.fragment.ndx').write_bytes(b'fixed-metadata')
            (root / '.fragment').mkdir()
            (root / '.fragment' / 'public-alias').symlink_to('../@f_stable')
        config = configparser.ConfigParser(interpolation=None)
        config['storage_pool.A'] = dict(generation='1', data_shards=str(data), parity_shards=str(parity),
                                         roots=','.join(map(str, roots)))
        config['disk_config'] = dict(fs_storage_path=str(control))
        config['server'] = dict(cert_path=str(fixture / 'cert.pem'), host='127.0.0.1', https_port='57211', native_port='57212')
        config['garbage_collector'] = dict(enabled='true', interval_hours='1')
        with (fixture / 'n1_config.ini').open('w') as stream:
            config.write(stream)
        binary = fixture / 'failed-engine'
        binary.write_bytes(b'exact-failed-binary')
        report = dict(status='FAIL', engine_sha256=recheck.base.digest(binary.read_bytes()),
                      requested_io_backend='uring',
                      topology=dict(data_shards=data, parity_shards=parity, roots=list(map(str, roots))),
                      counts=dict(acknowledged_versions=2), source_layout='flat-folder')
        (fixture / 'results.json').write_text(json.dumps(report))
        (fixture / 'n1.env').write_text('MASTER_KEY=not-a-production-key\nN1_IO_BACKEND=uring\n')
        (fixture / 'acknowledgements.jsonl').write_text('preserve-original-ledger\n')
        with sqlite3.connect(fixture / 'acknowledgements.sqlite') as connection:
            connection.executescript('CREATE TABLE acks(object_key TEXT,version_id TEXT,sha256 TEXT,size INTEGER,category TEXT);'
                                     'CREATE TABLE seen(object_key TEXT PRIMARY KEY,version_id TEXT);')
            connection.executemany('INSERT INTO acks VALUES(?,?,?,?,?)',
                                   [('key-a', 'version-a', 'hash-a', 1, 'source'),
                                    ('key-b', 'version-b', 'hash-b', 2, 'source')])
        connection.close()
        return fixture, parents, roots, binary

    def authority(self, parents):
        """Only device identity is mocked; all fixture paths/files are real."""
        original_stat = Path.stat
        def member_stat(path, *args, **kwargs):
            original = original_stat(path, *args, **kwargs)
            values = list(original)
            for slot, parent in enumerate(parents):
                if path == parent or path.is_relative_to(parent):
                    values[2] = 100 + slot
                    break
            return os.stat_result(values)
        stack = ExitStack()
        stack.enter_context(patch.object(Path, 'stat', member_stat))
        stack.enter_context(patch.object(recheck.scale, 'require_storage_filesystem'))
        stack.enter_context(patch.object(recheck.scale, 'independent_member_devices', return_value=list(parents)))
        return stack

    @staticmethod
    def copy(command, check):
        assert check and command[:3] == ['cp', '-a', '--reflink=auto']
        shutil.copytree(Path(command[3]), Path(command[4]), symlinks=True, dirs_exist_ok=True)

    def test_exact_three_plus_one_and_existing_one_plus_one_are_supported(self):
        for data, parity in [(3, 1), (1, 1)]:
            with self.subTest(data=data, parity=parity), tempfile.TemporaryDirectory(dir=self.base) as nested:
                previous, self.base = self.base, Path(nested)
                fixture, parents, roots, _ = self.fixture(data, parity)
                with self.authority(parents):
                    config, found, control = recheck.source_description(fixture, parents)
                self.assertEqual(found, roots)
                self.assertEqual(control, fixture / 'control')
                self.assertEqual(int(config['storage_pool.A']['data_shards']), data)
                self.base = previous

    def test_wrong_count_geometry_slots_and_member_formats_are_rejected(self):
        fixture, parents, roots, _ = self.fixture()
        with self.authority(parents):
            with self.assertRaisesRegex(AssertionError, 'EC/member count'):
                recheck.source_description(fixture, parents[:2])
            path = roots[2] / '.n1-pool.json'
            original = json.loads(path.read_text())
            for changes in [dict(data_shards=1), dict(member_slot=0), dict(member_format='unknown'),
                            dict(member_id='foreign'), dict(members=['duplicate'] * 4)]:
                path.write_text(json.dumps(original | changes))
                with self.subTest(changes=changes), self.assertRaises(AssertionError):
                    recheck.source_description(fixture, parents)
            path.write_text(json.dumps(original))
            marker = roots[2] / '.n1-root-id'
            marker.write_text(json.dumps(dict(engine_format='foreign', root_id='identity-2')))
            with self.assertRaisesRegex(AssertionError, 'Unknown source root format'):
                recheck.source_description(fixture, parents)

    def test_report_geometry_and_root_order_must_match_the_source_config(self):
        fixture, parents, roots, _ = self.fixture()
        report_path = fixture / 'results.json'
        report = json.loads(report_path.read_text())
        with self.authority(parents):
            for topology in [report['topology'] | dict(data_shards=1),
                             report['topology'] | dict(roots=list(map(str, reversed(roots))))]:
                report_path.write_text(json.dumps(report | dict(topology=topology)))
                with self.assertRaisesRegex(AssertionError, 'Source report/config'):
                    recheck.source_description(fixture, parents)

    def test_all_fragment_stable_identities_and_metadata_must_match(self):
        _, _, roots, _ = self.fixture()
        (roots[-1] / '@f_stable' / '.fragment.ndx').write_bytes(b'different')
        with self.assertRaisesRegex(AssertionError, 'fragment metadata differs'):
            recheck.fixture_fragment(roots)
        (roots[-1] / '@f_stable' / '.fragment.ndx').write_bytes(b'fixed-metadata')
        (roots[-1] / '@f_stable').rename(roots[-1] / '@f_other')
        alias = roots[-1] / '.fragment' / 'public-alias'
        alias.unlink()
        alias.symlink_to('../@f_other')
        with self.assertRaisesRegex(AssertionError, 'stable fragment identities disagree'):
            recheck.fixture_fragment(roots)

    def test_recheck_clone_uses_all_members_and_never_repoints_to_originals(self):
        fixture, parents, roots, binary = self.fixture()
        destinations = [parent / 'new-run' / 'members' for parent in parents]
        for parent in destinations:
            parent.mkdir(parents=True)
        args = SimpleNamespace(root=parents[0] / 'new-run' / 'recheck-evidence', fixture=fixture, member_parent=parents,
                               clone_member_parent=destinations, binary=binary, port=57231, clone=True)
        originals = compare.evidence_hashes(fixture)
        with self.authority(parents), patch.object(recheck.subprocess, 'run', side_effect=self.copy) as copy, \
                patch.object(recheck.socket, 'socket'), patch.object(recheck.ssl, 'create_default_context'):
            test = recheck.Recheck(args)
        try:
            self.assertEqual(copy.call_count, 5, 'Four members plus one control copy')
            self.assertEqual(len(test.roots), 4)
            self.assertTrue(all(root.parent == parent for root, parent in zip(test.roots, destinations)))
            self.assertTrue(set(test.roots).isdisjoint(roots))
            self.assertEqual(test.config['storage_pool.A']['roots'], ','.join(map(str, test.roots)))
            self.assertEqual(test.report['topology']['data_shards'], 3)
            self.assertEqual(test.report['topology']['parity_shards'], 1)
            self.assertTrue(test.args.flat_folder)
            self.assertEqual(test.report['requested_io_backend'], 'uring')
            self.assertEqual(test.args.io_backend, 'uring')
            self.assertIsNone(test.process)
            self.assertTrue(compare.unchanged(originals))
        finally:
            test.db.close()

    def test_recheck_rejects_a_different_binary_before_any_copy(self):
        fixture, parents, _, binary = self.fixture()
        binary.write_bytes(b'candidate-not-original')
        args = SimpleNamespace(root=parents[0] / 'recheck-evidence', fixture=fixture, member_parent=parents,
                               binary=binary, port=57231, clone=True)
        with self.authority(parents), patch.object(recheck.subprocess, 'run') as copy, \
                patch.object(recheck.socket, 'socket'):
            with self.assertRaisesRegex(AssertionError, 'exact failed candidate'):
                recheck.Recheck(args)
            copy.assert_not_called()
        self.assertFalse(args.root.exists())

    def test_recheck_accepts_sibling_evidence_and_owned_members_on_first_filesystem(self):
        fixture, devices, roots, binary = self.fixture()
        parents = [device / 'members' for device in devices]
        moved = []
        for parent, root in zip(parents, roots):
            parent.mkdir()
            destination = parent / root.name
            root.rename(destination)
            moved.append(destination)
        config = configparser.ConfigParser(interpolation=None)
        config.read(fixture / 'n1_config.ini')
        config['storage_pool.A']['roots'] = ','.join(map(str, moved))
        with (fixture / 'n1_config.ini').open('w') as stream:
            config.write(stream)
        report = json.loads((fixture / 'results.json').read_text())
        report['topology']['roots'] = list(map(str, moved))
        (fixture / 'results.json').write_text(json.dumps(report))
        output = devices[0] / 'new-run' / 'reconcile'
        output.parent.mkdir()
        args = SimpleNamespace(root=output, fixture=fixture, member_parent=parents,
                               binary=binary, port=57231, clone=True)
        self.assertFalse(fixture.is_relative_to(parents[0]))
        self.assertFalse(output.is_relative_to(parents[0]))
        with self.authority(devices), patch.object(recheck.subprocess, 'run', side_effect=self.copy), \
                patch.object(recheck.socket, 'socket'), patch.object(recheck.ssl, 'create_default_context'):
            test = recheck.Recheck(args)
        try:
            self.assertEqual(len(test.roots), 4)
            self.assertTrue(all(root.parent == parent for root, parent in zip(test.roots, parents)))
            self.assertTrue(set(test.roots).isdisjoint(moved))
            self.assertEqual(test.root, output)
        finally:
            test.db.close()

    def test_copy_refuses_a_different_member_filesystem(self):
        _, parents, roots, _ = self.fixture()
        destination = parents[1] / 'n1-rc-stress-cross-device-copy'
        destination.mkdir()
        with self.authority(parents), patch.object(compare.subprocess, 'run') as copy:
            with self.assertRaisesRegex(AssertionError, 'member filesystem'):
                compare.copy_member(roots[0], destination)
            copy.assert_not_called()

    def test_clone_destination_refuses_wrong_filesystem_or_enrolled_parent(self):
        fixture, parents, roots, binary = self.fixture()
        args = SimpleNamespace(root=parents[0] / 'recheck-evidence', fixture=fixture, member_parent=parents,
                               clone_member_parent=list(reversed(parents)), binary=binary, port=57231, clone=True)
        with self.authority(parents), patch.object(recheck.subprocess, 'run') as copy, patch.object(recheck.socket, 'socket'):
            with self.assertRaisesRegex(AssertionError, 'source member filesystem'):
                recheck.Recheck(args)
            args.clone_member_parent = roots
            with self.assertRaisesRegex(AssertionError, 'inside a source member'):
                recheck.Recheck(args)
            copy.assert_not_called()
        self.assertFalse(args.root.exists())

    def test_backend_mismatch_is_rejected_before_cloning(self):
        fixture, parents, _, binary = self.fixture()
        (fixture / 'n1.env').write_text('MASTER_KEY=not-real\nN1_IO_BACKEND=blocking\n')
        args = SimpleNamespace(root=parents[0] / 'recheck-evidence', fixture=fixture, member_parent=parents,
                               binary=binary, port=57231, clone=True)
        with self.authority(parents), patch.object(recheck.subprocess, 'run') as copy, patch.object(recheck.socket, 'socket'):
            with self.assertRaisesRegex(AssertionError, 'I/O backend mismatch'):
                recheck.Recheck(args)
            copy.assert_not_called()
        self.assertFalse(args.root.exists())

    def test_source_ledger_is_immutable_and_rejects_uncheckpointed_wal(self):
        fixture, _, _, _ = self.fixture()
        before = set(fixture.iterdir())
        reader = recheck.source_ledger(fixture)
        try:
            self.assertEqual(reader.execute('SELECT COUNT(*) FROM acks').fetchone()[0], 2)
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute('DELETE FROM acks')
        finally:
            reader.close()
        self.assertEqual(set(fixture.iterdir()), before, 'Read-only verification created source sidecars')
        (fixture / 'acknowledgements.sqlite-wal').write_bytes(b'uncheckpointed')
        with self.assertRaisesRegex(AssertionError, 'checkpointed'):
            recheck.source_ledger(fixture)

    def test_stage_witness_matches_selected_hash_backend_and_geometry(self):
        witness = dict(status='PASS', engine_sha256='selected-engine', requested_io_backend='uring',
                       topology=dict(data_shards=3, parity_shards=1, roots=['a', 'b', 'c', 'd']))
        report = self.base / 'results.json'
        report.write_text(json.dumps(witness))
        self.assertEqual(compare.stage_witness(self.base, 'selected-engine', 'uring', 3, 1), witness)
        for changes in [dict(status='RUNNING'), dict(engine_sha256='other-engine'),
                        dict(requested_io_backend='auto'),
                        dict(topology=dict(data_shards=1, parity_shards=1, roots=['a', 'b']))]:
            report.write_text(json.dumps(witness | changes))
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                compare.stage_witness(self.base, 'selected-engine', 'uring', 3, 1)

    def test_ambiguous_resolution_uses_the_original_flat_key_layout(self):
        fixture, _, _, _ = self.fixture()
        (fixture / 'ambiguous-batches.jsonl').write_text(json.dumps(dict(start=0, end=1)) + '\n')
        test = recheck.Recheck.__new__(recheck.Recheck)
        test.fixture, test.root = fixture, fixture / 'resolved'
        test.root.mkdir()
        test.args, test.source_flat_folder = SimpleNamespace(flat_folder=False), True
        test.call = Mock(return_value=(200, json.dumps([dict(version_id='confirmed-version')]).encode()))
        test.verify_row = Mock()
        test.db = sqlite3.connect(test.root / 'acknowledgements.sqlite')
        test.db.execute('CREATE TABLE acks(object_key TEXT,version_id TEXT,sha256 TEXT,size INTEGER,category TEXT)')
        try:
            result = test.resolve_ambiguous()
            self.assertEqual(result['committed_and_hashed'], 1)
            self.assertEqual(test.call.call_args.args, ('GET', '/objects/versions/projects/million/file-000000000.bin'))
            self.assertEqual(test.verify_row.call_args.args[0][0], 'projects/million/file-000000000.bin')
            self.assertEqual(test.db.execute('SELECT category FROM acks').fetchone()[0],
                             'ambiguous_commit_not_acknowledged')
        finally:
            test.db.close()

    def test_comparison_derives_four_member_geometry_before_creating_copies(self):
        fixture, parents, roots, binary = self.fixture()
        args = SimpleNamespace(root=parents[0] / 'comparison-evidence', member_parent=parents,
                               source_member_parent=parents, binary=binary, data_shards=1, parity_shards=1)
        generated = []
        def initialize(test, settings):
            generated.append((settings.data_shards, settings.parity_shards, settings.flat_folder, settings.io_backend))
            test.args, test.root = settings, settings.root
            test.root.mkdir()
            (test.root / 'control').mkdir()
            test.roots = [parent / f'n1-rc-stress-new-member-{slot}' for slot, parent in enumerate(parents)]
            for root in test.roots:
                root.mkdir()
            test.config = configparser.ConfigParser(interpolation=None)
            test.config['server'] = dict(host='127.0.0.1', https_port='57231')
            test.report = dict(source_layout='flat-folder')
            test.counts = Counter()
            test.db = sqlite3.connect(test.root / 'acknowledgements.sqlite')
        originals = compare.evidence_hashes(fixture)
        with self.authority(parents), patch.object(compare.scale.Scale, '__init__', initialize), \
                patch.object(compare.subprocess, 'run', side_effect=self.copy) as copy:
            test = compare.Clone(args, fixture, reconcile=True)
        try:
            self.assertEqual(generated, [(3, 1, False, 'uring')])
            self.assertTrue(test.source_flat_folder)
            self.assertEqual(copy.call_count, 5)
            self.assertEqual(len(test.roots), 4)
            self.assertTrue(set(test.roots).isdisjoint(roots))
            self.assertTrue(compare.unchanged(originals))
            self.assertEqual(test.counts['acknowledged_versions'], 2)
            self.assertEqual(test.report['initial_population']['known_versions'], 2)
        finally:
            test.db.close()


if __name__ == '__main__':
    unittest.main()
