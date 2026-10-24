#!/usr/bin/env python3
"""Synthetic parser regressions only: these never launch or qualify N1."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
SCRIPT = Path(__file__).with_name('n1-scale-result-check.py')
SPEC = importlib.util.spec_from_file_location('scale_result_check', SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
PIN = 'a' * 64


def fixture(objects=1000000, flat_folder=False):
    """A small fabricated record, not one million fabricated data files."""
    probes, mutations, phases, counter = 1000, 1000000, [], 0

    def phase(name, **fields):
        phases.append(dict(name=name, status='PASS', elapsed_seconds=1.0, **fields))

    def resources(name, restart=False):
        nonlocal counter
        counter += 100
        initial = name == 'resources-before-population'
        value = 1 if restart else (0 if initial else counter)
        phase(name, engine_reported_resources=dict(pid=102 if restart else 101,
              open_fds=32, rss_bytes=32 * 1024**2), quiescent_fd_budget=2048,
              no_automatic_object_scans=True, engine_io_backend=dict(requested='uring',
              backend='io_uring', publication_backend='posix', fallback_reason=None,
              queue_capacity=128, queued=0, in_flight=0, submitted=value, completed=value))

    resources('resources-before-population')
    previous = 0
    for n in CHECK.checkpoints(objects):
        phase(f'folder-upload-to-{n}', new_source_objects=n-previous, source_objects_total=n,
              concurrent_namespace_clients=0)
        indexes = sorted({0, max(0, n//2//1000), max(0, (n-1)//1000)})
        phase(f'pagination-at-{n}', population=n, sampled_prefixes_all_pages=[
              dict(path=(f'projects/million/file-{i*1000:09}'[:-3] if flat_folder else
                         f'projects/project-{i:06}'), objects=min(1000, n-i*1000)) for i in indexes],
              **({'scope': 'lexical filename prefixes within one flat folder'} if flat_folder else {}))
        phase(f'complete-version-integrity-at-{n}', every_acknowledged_version_downloaded_and_hashed=n)
        resources(f'resources-at-{n}')
        previous = n
    phase('complete-quiescent-namespace-inventory', all_live_names_and_max_ulid_versions_verified=objects,
          completion_frame_verified=True)
    phase('post-population-probe-object-setup', probe_objects=probes, immutable_probe_directories=probes*4)
    phase('post-population-probe-integrity', every_acknowledged_version_downloaded_and_hashed=objects+probes)
    cycles = (mutations + probes*16-1)//(probes*16)
    for c in (8, 32, 128, 256):
        phase(f'high-churn-c{c}', actors=probes, cycles_per_actor=cycles,
              maximum_client_concurrency=min(c, probes), namespace_mutations=probes*cycles*4,
              immutable_files_unchanged=probes*8)
        resources(f'resources-after-churn-c{c}')
    for name in ('all-version-integrity-after-churn', 'all-version-integrity-after-final-sigkill'):
        phase(name, every_acknowledged_version_downloaded_and_hashed=objects+probes)
    phase('quiescent-namespace-after-final-sigkill', all_live_names_and_max_ulid_versions_verified=objects+probes,
          completion_frame_verified=True)
    resources('resources-after-final-sigkill', restart=True)
    return dict(status='PASS', fault=None, engine_sha256=PIN, strict_uploads=True,
                automatic_write_retries=False, automatic_full_scans_enabled=False,
                read_triggered_repair_enabled=True, requested_io_backend='uring',
                workload='upload-then-churn', concurrent_namespace_clients=0,
                **({'source_layout': 'flat-folder'} if flat_folder else {}),
                batch_size=64, batch_clients=8, batch_server_concurrency=16,
                max_filesystem_growth_gib=64,
                topology=dict(data_shards=3, parity_shards=1, roots=[f'/mnt/member-{i}/owned' for i in range(4)]),
                physical_block_devices=[f'/sys/devices/test/block/sd{letter}' for letter in 'abcd'],
                target_objects=objects, target_churn_mutations=mutations, phases=phases,
                counts=dict(source_objects=objects, acknowledged_versions=objects+probes,
                            namespace_mutations=probes*cycles*16,
                            historical_downloads_verified=sum(CHECK.checkpoints(objects))+3*(objects+probes)))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.data = fixture()

    def phase(self, name):
        return next(p for p in self.data['phases'] if p['name'] == name)

    def rejects(self):
        with self.assertRaises(CHECK.EvidenceError):
            CHECK.check_result(self.data, PIN)

    def test_complete_record(self):
        checked = CHECK.check_result(self.data, PIN)
        self.assertEqual(checked['status'], 'PASS')
        self.assertEqual(checked['source_objects'], 1000000)

    def test_beyond_million_schedule(self):
        self.assertEqual(CHECK.check_result(fixture(2000000), PIN)['source_objects'], 2000000)

    def test_flat_folder_layout_qualifies_only_sampled_prefixes_and_complete_snapshots(self):
        self.data = fixture(flat_folder=True)
        checked = CHECK.check_result(self.data, PIN)
        self.assertEqual(checked['source_layout'], 'flat-folder')
        self.assertIn('full-directory paginated LIST in flat-folder layout', checked['not_qualified'])
        self.assertEqual(CHECK.check_result(fixture(2000000, flat_folder=True), PIN)['source_objects'], 2000000)
        self.phase('complete-quiescent-namespace-inventory')['all_live_names_and_max_ulid_versions_verified'] -= 1
        self.rejects()

    def test_layout_and_flat_listing_scope_cannot_be_silently_changed(self):
        self.assertEqual(CHECK.check_result(self.data, PIN)['source_layout'], 'project-tree')
        self.data['source_layout'] = 'project-tree'
        self.assertEqual(CHECK.check_result(self.data, PIN)['status'], 'PASS')
        self.data['source_layout'] = 'unknown'
        self.rejects()
        self.data = fixture(flat_folder=True)
        del self.phase('pagination-at-1000')['scope']
        self.rejects()
        self.data = fixture(flat_folder=True)
        self.phase('pagination-at-1000000')['sampled_prefixes_all_pages'][0]['path'] = 'projects/million'
        self.rejects()

    def test_wrong_pin(self):
        self.data['engine_sha256'] = 'b' * 64
        self.rejects()

    def test_nonterminal_statuses(self):
        for status in ('RUNNING', 'CANCELLED', 'FAIL', 'BLOCKED', 'INCOMPLETE'):
            self.data['status'] = status
            self.rejects()

    def test_fault(self):
        self.data['fault'] = 'deadline'
        self.rejects()

    def test_missing_fault(self):
        del self.data['fault']
        self.rejects()

    def test_wrong_geometry(self):
        for key, value in (('data_shards', 1), ('parity_shards', True)):
            self.data = fixture()
            self.data['topology'][key] = value
            self.rejects()

    def test_duplicate_physical_device(self):
        self.data['physical_block_devices'][3] = self.data['physical_block_devices'][0]
        self.rejects()

    def test_reduced_offered_load_or_unbounded_growth_is_rejected(self):
        for key, value in (('batch_size', 16), ('batch_clients', 1),
                           ('batch_server_concurrency', 1), ('max_filesystem_growth_gib', 0),
                           ('max_filesystem_growth_gib', 65), ('max_filesystem_growth_gib', True)):
            self.data = fixture()
            self.data[key] = value
            self.rejects()

    def test_incomplete_or_changed_immutable_inventory_is_rejected(self):
        self.phase('post-population-probe-object-setup')['immutable_probe_directories'] -= 1
        self.rejects()
        self.data = fixture()
        self.phase('high-churn-c256')['immutable_files_unchanged'] -= 1
        self.rejects()

    def test_boolean_and_string_counts(self):
        for value in (True, '1000000', 1000000.0, -1, None):
            self.data = fixture()
            self.data['counts']['source_objects'] = value
            self.rejects()

    def test_partial_population_or_acknowledgements(self):
        for key in ('source_objects', 'acknowledged_versions', 'historical_downloads_verified'):
            self.data = fixture()
            self.data['counts'][key] -= 1
            self.rejects()

    def test_missing_or_ignored_or_duplicate_phase(self):
        original = fixture()
        for change in ('missing', 'ignored', 'duplicate', 'reordered', 'extra'):
            self.data = copy.deepcopy(original)
            if change == 'missing':
                self.data['phases'].pop(3)
            elif change == 'ignored':
                self.data['phases'][3]['status'] = 'IGNORED'
            elif change == 'duplicate':
                self.data['phases'].append(copy.deepcopy(self.data['phases'][3]))
            elif change == 'extra':
                self.data['phases'].append(dict(name='unqualified-gate', status='PASS', elapsed_seconds=1))
            else:
                self.data['phases'][0], self.data['phases'][1] = self.data['phases'][1], self.data['phases'][0]
            self.rejects()

    def test_no_snapshot_terminal(self):
        self.phase('quiescent-namespace-after-final-sigkill')['completion_frame_verified'] = False
        self.rejects()

    def test_wrong_snapshot_population(self):
        self.phase('complete-quiescent-namespace-inventory')['all_live_names_and_max_ulid_versions_verified'] -= 1
        self.rejects()

    def test_missing_or_false_paginated_list_evidence(self):
        for samples in ([], ['not evidence'], [dict(path='elsewhere', objects=1000)]):
            self.data = fixture()
            self.phase('pagination-at-1000')['sampled_prefixes_all_pages'] = samples
            self.rejects()
        self.data = fixture()
        self.phase('pagination-at-1000')['sampled_prefixes_all_pages'][0]['objects'] = True
        self.rejects()

    def test_no_immutable_or_churn_evidence(self):
        for key in ('namespace_mutations', 'immutable_files_unchanged', 'maximum_client_concurrency'):
            self.data = fixture()
            self.phase('high-churn-c256')[key] = 0
            self.rejects()

    def test_mutation_clients_during_population(self):
        self.phase('folder-upload-to-1000000')['concurrent_namespace_clients'] = 32
        self.rejects()

    def test_wrong_or_fallback_backend(self):
        for key, value in (('backend', 'blocking'), ('requested', 'auto'),
                           ('fallback_reason', 'denied'), ('publication_backend', 'uring')):
            self.data = fixture()
            self.phase('resources-at-1000')['engine_io_backend'][key] = value
            self.rejects()

    def test_native_counters_all_zero(self):
        for p in self.data['phases']:
            if 'engine_io_backend' in p:
                p['engine_io_backend'].update(submitted=0, completed=0)
        self.rejects()

    def test_native_same_pid_reset(self):
        self.phase('resources-at-10000')['engine_io_backend'].update(submitted=1, completed=1)
        self.rejects()

    def test_native_counter_order(self):
        self.phase('resources-at-1000')['engine_io_backend']['completed'] = 999999
        self.rejects()

    def test_unexpected_pid_change(self):
        self.phase('resources-at-10000')['engine_reported_resources']['pid'] = 999
        self.rejects()

    def test_restart_is_required_but_counters_are_not_cross_compared(self):
        self.assertEqual(CHECK.check_result(self.data, PIN)['status'], 'PASS')
        self.phase('resources-after-final-sigkill')['engine_reported_resources']['pid'] = 101
        self.rejects()

    def test_restarted_engine_requires_native_work(self):
        self.phase('resources-after-final-sigkill')['engine_io_backend'].update(submitted=0, completed=0)
        self.rejects()

    def test_resource_or_queue_budget(self):
        for key, value in (('open_fds', 2049), ('rss_bytes', 2*1024**3+1), ('pid', True)):
            self.data = fixture()
            self.phase('resources-at-1000')['engine_reported_resources'][key] = value
            self.rejects()
        self.data = fixture()
        self.phase('resources-at-1000')['engine_io_backend']['queued'] = 129
        self.rejects()

    def test_duplicate_json_keys(self):
        with self.assertRaises(CHECK.EvidenceError):
            json.loads('{"status":"FAIL","status":"PASS"}', object_pairs_hook=CHECK.unique_object)

    def test_cli_read_only_hash_and_optimized_python_rejection(self):
        # Caller must put TMPDIR on its approved test filesystem.
        self.assertEqual(Path(tempfile.gettempdir()), Path(os.environ['TMPDIR']).resolve(strict=True))
        with tempfile.TemporaryDirectory(prefix='n1-scale-parser-') as directory:
            path = Path(directory)/'result.json'
            original = json.dumps(self.data).encode()
            path.write_bytes(original)
            command = [sys.executable, '-B', str(SCRIPT), '--result', str(path), '--expected-engine-sha256', PIN]
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            output = json.loads(result.stdout)
            self.assertEqual(output['status'], 'PASS')
            self.assertEqual(output['input_sha256'], CHECK.hashlib.sha256(original).hexdigest())
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ['result.json'])
            optimized = subprocess.run(command[:1]+['-O']+command[1:], capture_output=True, text=True)
            self.assertNotEqual(optimized.returncode, 0)
            self.assertIn('Python -O', optimized.stderr)


if __name__ == '__main__':
    unittest.main()
