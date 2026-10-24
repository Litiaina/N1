#!/usr/bin/env python3
"""Bounded strict-upload/maintenance benchmark on two NEW HDD member roots.

Use the identical command with before/after binaries and separate --root paths.
All source files, ledgers, certificates and result files stay on those HDDs.
This is loopback HTTPS, not a million-object or physical power-cut qualification.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import sys
import time
import traceback

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('hdd_scale', Path(__file__).with_name('n1-hdd-scale.py'))
scale = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scale)


class Benchmark(scale.Scale):
    def __init__(self, args):
        super().__init__(args)
        # Isolate foreground cost; explicit availability scans below test the
        # very same maintenance algorithm without uncontrollable timer overlap.
        self.config['auto_healing']['enabled'] = 'false'
        self.config['upload_config']['monitor_interval_seconds'] = '86400'
        self.config['upload_config']['monitor_deep_scan_interval_seconds'] = '86400'
        if args.admission_probe:
            self.config['upload_config']['max_concurrent_io'] = '1'
            self.config['io_control']['foreground_wait_timeout_ms'] = '200'
        self.write_config()
        self.report.update(automatic_healing_enabled=False, explicit_maintenance_scans=True,
                           admission_probe=args.admission_probe,
                           note='Strict payload, NDX and NCX; actual source creation and ledger I/O included. '
                                'Admission probe accepts explicit per-object 503, never missing results or whole-request errors.')

    def ingest(self, start, count, batch_size):
        began = time.monotonic()
        intervals = ((i, min(start+count, i+batch_size)) for i in range(start, start+count, batch_size))
        for _ in scale.bounded_map(self.upload_batch, intervals, self.args.batch_clients):
            pass
        return {'offered_objects': count, 'batch_size': batch_size, 'clients': self.args.batch_clients,
                'offered_objects_per_second': round(count/(time.monotonic()-began), 3)}

    def repair(self):
        # This endpoint takes query parameters, not JSON. Sending an unread
        # request body can close HTTP/1 keep-alive before the status poll.
        status, body = self.call('POST', '/maintenance/repair/start?mode=availability&dry_run=true&batch=128&meta_concurrency=1&repair_concurrency=1')
        assert status == 202, (status, body[:200])
        end = time.monotonic()+600
        while time.monotonic() < end:
            result = self.api('GET', '/maintenance/repair/status')
            if not result['is_running']:
                assert result['phase'] == 'Completed' and not result.get('last_error'), result
                assert result['failed_repairs'] == 0 and result['corrupt_found'] == 0, result
                assert result['total_scanned'] == self.counts['acknowledged_versions'], result
                return {'scanned': result['total_scanned'], 'failed_repairs': result['failed_repairs']}
            time.sleep(.1)
        raise AssertionError('Explicit maintenance scan did not finish')

    def run(self):
        self.start('benchmark')
        self.fragment_key = secrets.token_hex(32)
        self.fragment = self.api('POST', '/fragments', {'name': 'hdd-benchmark-'+secrets.token_hex(4),
            'key': self.fragment_key, 'secret': self.master}, token='')['fragment_hash']
        self.authenticate()
        if self.args.admission_probe:
            self.phase_run('partial-batch-admission-results', lambda: self.ingest(0, self.args.objects, 64))
            assert self.counts['admission_refusals'] > 0, 'Probe did not exercise backpressure'
            assert self.counts['partial_batches_reported'] > 0, 'No partially committed batch was exercised'
        else:
            self.phase_run('strict-batch-64', lambda: self.ingest(0, self.args.objects, 64))
            self.phase_run('maintenance-at-first-population', self.repair)
            self.phase_run('strict-batch-16', lambda: self.ingest(self.args.objects, self.args.objects, 16))
            self.phase_run('maintenance-at-double-population', self.repair)
        self.phase_run('every-acknowledged-version-hashed', self.verify_all)
        self.stop()
        self.start('benchmark-after-sigkill')
        self.phase_run('every-acknowledged-version-hashed-after-sigkill', self.verify_all)
        assert not self.fault, self.fault
        self.report['status'] = 'PASS'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--objects', type=int, default=512, help='Objects per foreground phase')
    parser.add_argument('--batch-clients', type=int, default=8)
    parser.add_argument('--batch-concurrency', type=int, default=16)
    parser.add_argument('--port', type=int, default=56921)
    parser.add_argument('--deadline-seconds', type=int, default=3600)
    parser.add_argument('--request-timeout-seconds', type=int, default=300)
    parser.add_argument('--free-floor-gib', type=int, default=128)
    parser.add_argument('--admission-probe', action='store_true')
    args = parser.parse_args()
    assert 1 <= args.objects <= 100000 and 1 <= args.batch_clients <= 32
    args.data_shards, args.parity_shards = 1, 1
    args.batch_size, args.probes, args.churn_mutations = 64, 1, 0
    args.accept_admission_refusals = args.admission_probe
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    test = Benchmark(args)
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'Operator signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
