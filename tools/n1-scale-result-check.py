#!/usr/bin/env python3
"""Read-only consistency gate for the staged four-drive EC 3+1 million-object result.

This validates recorded evidence, not hardware identity or a new engine run.
It never connects to servers, launches processes, or modifies an input file.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys


class EvidenceError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise EvidenceError(message)


def integer(value, label, minimum=0):
    require(type(value) is int and value >= minimum, f'{label}: invalid integer')
    return value


def count(record, key, expected):
    require(integer(record.get(key), key) == expected, f'{key}: expected {expected}')


def checkpoints(objects):
    """Same logarithmic population schedule as n1-hdd-scale.py; no imports."""
    values = {objects, *(min(objects, n) for n in (1000, 10000, 100000, 500000))}
    decade = 1000000
    while decade <= objects:
        values.update(n * decade for n in (1, 2, 5) if n * decade <= objects)
        decade *= 10
    return sorted(values)


def check_result(result, expected_engine_sha256):
    require(isinstance(result, dict), 'Result must be an object')
    require(isinstance(expected_engine_sha256, str) and
            re.fullmatch(r'[0-9a-f]{64}', expected_engine_sha256), 'Invalid engine SHA-256 pin')
    require(result.get('engine_sha256') == expected_engine_sha256, 'Engine SHA-256 mismatch')
    require(result.get('status') == 'PASS' and 'fault' in result and result['fault'] is None,
            'Run is not a complete, fault-free PASS')
    for key, expected in (('strict_uploads', True), ('automatic_write_retries', False),
                          ('automatic_full_scans_enabled', False), ('read_triggered_repair_enabled', True)):
        require(result.get(key) is expected, f'{key}: wrong or missing policy')
    require(result.get('workload') == 'upload-then-churn', 'Wrong staged workload')
    require(result.get('requested_io_backend') == 'uring', 'Required native backend not requested')
    layout = result.get('source_layout', 'project-tree')
    require(layout in ('project-tree', 'flat-folder'), 'Unknown source layout')
    count(result, 'concurrent_namespace_clients', 0)
    for key, value in (('batch_size', 64), ('batch_clients', 8), ('batch_server_concurrency', 16)):
        count(result, key, value)
    require(integer(result.get('max_filesystem_growth_gib'), 'max_filesystem_growth_gib', 1) <= 64,
            'Missing or excessive approved filesystem growth cap')
    topology = result.get('topology', {})
    require(isinstance(topology, dict), 'Missing topology')
    count(topology, 'data_shards', 3)
    count(topology, 'parity_shards', 1)
    for key, values in (('roots', topology.get('roots')),
                        ('physical_block_devices', result.get('physical_block_devices'))):
        require(isinstance(values, list) and len(values) == 4 and
                all(isinstance(p, str) and p.startswith('/') and '..' not in Path(p).parts for p in values)
                and len(set(values)) == 4, f'{key}: need four distinct absolute identities')
    objects = integer(result.get('target_objects'), 'target_objects', 1000000)
    mutations = integer(result.get('target_churn_mutations'), 'target_churn_mutations', 1000000)
    phases = result.get('phases')
    require(isinstance(phases, list) and all(isinstance(p, dict) for p in phases), 'Invalid phases')
    by_name = {}
    for phase in phases:
        name = phase.get('name')
        require(isinstance(name, str) and name not in by_name, 'Invalid or duplicate phase name')
        require(phase.get('status') == 'PASS', f'{name}: incomplete or failed phase')
        duration = phase.get('elapsed_seconds')
        require(type(duration) in (int, float) and duration >= 0 and
                (type(duration) is int or math.isfinite(duration)),
                f'{name}: invalid elapsed time')
        by_name[name] = phase
    milestones = checkpoints(objects)
    expected_names = ['resources-before-population']
    for n in milestones:
        expected_names.extend([f'folder-upload-to-{n}', f'pagination-at-{n}',
                               f'complete-version-integrity-at-{n}', f'resources-at-{n}'])
    expected_names.extend(['complete-quiescent-namespace-inventory', 'post-population-probe-object-setup',
                           'post-population-probe-integrity'])
    for c in (8, 32, 128, 256):
        expected_names.extend([f'high-churn-c{c}', f'resources-after-churn-c{c}'])
    expected_names.extend(['all-version-integrity-after-churn', 'all-version-integrity-after-final-sigkill',
                           'quiescent-namespace-after-final-sigkill', 'resources-after-final-sigkill'])
    require([p['name'] for p in phases] == expected_names, 'Missing, extra or reordered gate phases')
    probes = integer(by_name['post-population-probe-object-setup'].get('probe_objects'), 'probe_objects', 256)
    count(by_name['post-population-probe-object-setup'], 'immutable_probe_directories', probes * 4)
    acknowledgements = objects + probes
    previous = 0
    for n in milestones:
        upload = by_name[f'folder-upload-to-{n}']
        count(upload, 'new_source_objects', n - previous)
        count(upload, 'source_objects_total', n)
        count(upload, 'concurrent_namespace_clients', 0)
        count(by_name[f'pagination-at-{n}'], 'population', n)
        samples = by_name[f'pagination-at-{n}'].get('sampled_prefixes_all_pages')
        indexes = sorted({0, max(0, n//2//1000), max(0, (n-1)//1000)})
        prefixes = [(f'projects/million/file-{i*1000:09}'[:-3] if layout == 'flat-folder' else
                     f'projects/project-{i:06}') for i in indexes]
        require(isinstance(samples, list) and all(isinstance(s, dict) for s in samples) and
                [s.get('path') for s in samples] == prefixes,
                'Missing or incorrect paginated LIST prefixes')
        if layout == 'flat-folder':
            require(by_name[f'pagination-at-{n}'].get('scope') ==
                    'lexical filename prefixes within one flat folder',
                    'Flat LIST evidence must distinguish sampled prefixes from the full directory')
        for sample, index in zip(samples, indexes):
            count(sample, 'objects', min(1000, n-index*1000))
        count(by_name[f'complete-version-integrity-at-{n}'], 'every_acknowledged_version_downloaded_and_hashed', n)
        previous = n
    for name in ('post-population-probe-integrity', 'all-version-integrity-after-churn',
                 'all-version-integrity-after-final-sigkill'):
        count(by_name[name], 'every_acknowledged_version_downloaded_and_hashed', acknowledgements)
    for name, total in (('complete-quiescent-namespace-inventory', objects),
                        ('quiescent-namespace-after-final-sigkill', acknowledgements)):
        count(by_name[name], 'all_live_names_and_max_ulid_versions_verified', total)
        require(by_name[name].get('completion_frame_verified') is True, 'Missing snapshot completion frame')
    cycles = (mutations + probes * 16 - 1) // (probes * 16)
    immutable_files = None
    for c in (8, 32, 128, 256):
        churn = by_name[f'high-churn-c{c}']
        for key, value in (('actors', probes), ('cycles_per_actor', cycles),
                           ('maximum_client_concurrency', min(c, probes)), ('namespace_mutations', probes * cycles * 4)):
            count(churn, key, value)
        observed_files = integer(churn.get('immutable_files_unchanged'), 'immutable_files_unchanged', 1)
        if immutable_files is None:
            immutable_files = observed_files
        require(observed_files == immutable_files, 'Immutable probe inventory changed between churn stages')
    counts = result.get('counts')
    require(isinstance(counts, dict), 'Missing final counts')
    for key, value in counts.items():
        integer(value, key)
    for key, value in (('source_objects', objects), ('acknowledged_versions', acknowledgements),
                       ('namespace_mutations', probes * cycles * 16),
                       ('historical_downloads_verified', sum(milestones) + 3 * acknowledgements)):
        count(counts, key, value)
    require(counts.get('transport_ambiguous_batches', 0) == 0 and counts.get('admission_refusals', 0) == 0,
            'Ambiguous or refused scale uploads')
    initial_pid, last_submitted, last_completed = None, 0, 0
    for name in expected_names:
        if not name.startswith('resources-'):
            continue
        phase = by_name[name]
        usage, backend = phase.get('engine_reported_resources', {}), phase.get('engine_io_backend', {})
        require(isinstance(usage, dict) and isinstance(backend, dict), 'Missing engine resource/backend snapshot')
        pid = integer(usage.get('pid'), 'pid', 1)
        require(0 < integer(usage.get('open_fds'), 'open_fds') <= 2048, 'FD budget exceeded')
        require(0 < integer(usage.get('rss_bytes'), 'rss_bytes') <= 2 * 1024**3, 'RSS budget exceeded')
        count(phase, 'quiescent_fd_budget', 2048)
        require(phase.get('no_automatic_object_scans') is True, 'Unexpected automatic object scan')
        require(backend.get('requested') == 'uring' and backend.get('backend') == 'io_uring' and
                backend.get('publication_backend') == 'posix' and 'fallback_reason' in backend and
                backend['fallback_reason'] is None, 'Wrong native/publication policy or fallback')
        count(backend, 'queue_capacity', 128)
        require(integer(backend.get('queued'), 'queued') <= 128 and
                integer(backend.get('in_flight'), 'in_flight') <= 128, 'Unbounded native queue')
        submitted, completed = (integer(backend.get(k), k) for k in ('submitted', 'completed'))
        require(completed <= submitted, 'Native completions exceed submissions')
        if initial_pid is None:
            initial_pid = pid
        elif name == 'resources-after-final-sigkill':
            require(pid != initial_pid and submitted > 0 and completed > 0, 'Missing restarted native engine evidence')
            continue  # Counters are process-local: never compare across restart.
        else:
            require(pid == initial_pid and submitted > last_submitted and completed > last_completed,
                    'Native counters did not grow within the original engine PID')
        last_submitted, last_completed = submitted, completed
    return {'status': 'PASS', 'scope': 'Recorded-result consistency, not hardware attestation or a new engine test',
            'engine_sha256': expected_engine_sha256, 'source_objects': objects, 'acknowledged_versions': acknowledgements,
            'source_layout': layout,
            'namespace_mutations': counts['namespace_mutations'], 'initial_engine_pid': initial_pid,
            'restarted_engine_pid': pid, 'native_evidence': 'Same-PID growth before restart; independently positive counters after restart',
            'not_qualified': ['cold-cache speed', 'LAN or Console scale', 'GC at million objects',
                              'every source object mutated', 'physical power loss', 'hundred-million scale'] +
                             (['full-directory paginated LIST in flat-folder layout'] if layout == 'flat-folder' else [])}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f'Duplicate JSON field: {key}')
        result[key] = value
    return result


def main():
    if not __debug__:
        raise SystemExit('Do not run the evidence gate with Python -O')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--result', type=Path, required=True)
    parser.add_argument('--expected-engine-sha256', required=True)
    args = parser.parse_args()
    raw = args.result.read_bytes()
    try:
        checked = check_result(json.loads(raw, object_pairs_hook=unique_object), args.expected_engine_sha256)
    except (EvidenceError, ValueError, TypeError, KeyError) as error:
        checked = {'status': 'FAIL', 'error': str(error)}
    checked['input_sha256'] = hashlib.sha256(raw).hexdigest()
    print(json.dumps(checked, indent=2))
    return 0 if checked['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
