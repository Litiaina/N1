#!/usr/bin/env python3
"""Owned HDD proof: opt-in online scrub repairs cold corruption during HTTP I/O.

Uses fresh isolated fixtures only. Corrupts one owned shard deliberately, never
production or NDX. Preserves failures and reports; no automatic request retries.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time
import traceback
import urllib.parse

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('scrub_base', Path(__file__).with_name('n1-rc-stress.py'))
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
spec = importlib.util.spec_from_file_location('scrub_gc', Path(__file__).with_name('n1-gc-scheduled-proof.py'))
gc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gc)


class ScrubProof(base.Stress):
    def __init__(self, args):
        for parent in [args.root.parent, *args.member_parent]:
            gc.require_data_volume(parent)
        devices = [gc.backing_device(parent) for parent in args.member_parent]
        assert len(set(devices)) == len(devices), 'Need independent physical drives'
        super().__init__(args)
        self.config['auto_healing'].update(enabled='true', scrub_schedule_utc='',
            initial_scan_delay_seconds='0', topology_check_interval_seconds='1',
            availability_scan_interval_seconds='1', checksum_scrub_interval_seconds='1',
            scheduled_scrub_pause_millis='100')
        self.config['garbage_collector']['cleanup_interval_seconds'] = '3600'
        self.write_config()
        self.report.update(proof=__doc__, harness_sha256=base.digest(Path(__file__).read_bytes()),
                           cold_objects=64, object_bytes=262144,
                           physical_block_devices=list(map(str, devices)))
        self.health_samples = []

    def health(self):
        value = self.api('GET', '/maintenance/health?refresh=true&fragment=' + self.fragment)
        base.verify_io_backend(value, self.args)
        assert value['maintenance']['access_mode'] == 'availability', value['maintenance']
        assert value['maintenance']['auto_healing']['write_safe'], value['maintenance']
        self.health_samples.append(value)
        return value['maintenance']['auto_healing']['scheduled_scrub']

    def put(self, key, size=262144):
        payload = base.payload(key, size)
        value = self.api('POST', '/upload/one-shot/' + key + '?durability=strict', payload)
        ack = (self.token, key, value['version_id'], base.digest(payload), size)
        with self.lock:
            self.acknowledged.append(ack)
        return ack

    def run(self):
        self.phase = 'disabled-legacy-intervals'
        self.start('disabled')
        self.fragment, self.token = self.create('scrub-proof-' + secrets.token_hex(5))
        with ThreadPoolExecutor(max_workers=4) as workers:
            cold = list(workers.map(self.put, [f'cold-{index:03}.bin' for index in range(64)]))
        hot = self.put('hot.bin', 16384)
        time.sleep(2)
        assert self.health()['phase'] == 'disabled', 'Legacy interval settings must not opt in'
        inventory = self.immutable_inventory()
        self.stop()
        version = cold[0][2]
        shards = [path for member in self.roots for path in member.glob(f'@f_*/.objects/*/*/@o_*/{version}/*')
                  if path.is_file() and not path.is_symlink()]
        assert len(shards) == self.args.data_shards + self.args.parity_shards
        damaged = shards[0]
        original = damaged.read_bytes()
        changed = bytearray(original)
        changed[0] ^= 0xff
        with damaged.open('r+b') as stream:
            stream.write(changed)
            stream.flush()
            os.fsync(stream.fileno())
        assert base.digest(damaged.read_bytes()) != base.digest(original)
        slot = (int(time.time()) // 60 + 2) * 60
        schedule = datetime.fromtimestamp(slot, timezone.utc).strftime('%a %H:%M')
        self.config['auto_healing']['scrub_schedule_utc'] = schedule
        self.write_config()
        self.phase = 'scheduled-online-scrub-with-foreground-io'
        self.start('scheduled')
        stop = threading.Event()
        running_event = threading.Event()
        counts = dict(reads=0, writes=0)
        observations = []

        def foreground():
            index = 0
            while not stop.is_set():
                self.read_ack(hot)
                counts['reads'] += 1
                if index < 4 or (running_event.is_set() and index < 24):
                    self.put(f'foreground-{index:05}.bin', 16384)
                    counts['writes'] += 1
                    index += 1
                stop.wait(.1)

        overlap = False
        with ThreadPoolExecutor(max_workers=1) as workers:
            task = workers.submit(foreground)
            try:
                deadline = time.monotonic() + 240
                while time.monotonic() < deadline:
                    if task.done():
                        task.result()
                    status = self.health()
                    observations.append(status)
                    if status['phase'] == 'running' and not overlap:
                        running_event.set()
                        code, body = self.call('POST', '/maintenance/repair/start?check_mode=availability')
                        assert code == 409, (code, body[:500])
                        overlap = True
                    if status['phase'] == 'incomplete':
                        raise AssertionError(f'Scheduled scrub incomplete: {status}')
                    if status['last_completed_at']:
                        completed = status
                        break
                    time.sleep(.25)
                else:
                    raise AssertionError('Scheduled scrub did not complete within fixture deadline')
            finally:
                stop.set()
            task.result()
        assert overlap and counts['reads'] and counts['writes'], (overlap, counts)
        assert damaged.read_bytes() == original, 'Cold shard was not healed before any cold GET'
        # All original immutable NDX and healthy shard contents remain unchanged.
        for relative, expected in inventory.items():
            member, relative = relative.split('/', 1)
            path = self.roots[int(member)] / relative
            if path != damaged:
                stat = path.stat()
                assert (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                        base.digest(path.read_bytes())) == expected, str(path)
        for ack in self.acknowledged:
            self.read_ack(ack)
        self.stop()
        self.start('restart-after-completion')
        time.sleep(3)
        restored = self.health()
        assert restored['last_started_at'] == completed['last_started_at']
        assert restored['last_completed_at'] == completed['last_completed_at']
        assert restored['phase'] != 'running', 'Restart duplicated the scheduled slot'
        for ack in self.acknowledged:
            self.read_ack(ack)
        # Stop a second scheduled run through the existing operator API. Its
        # incomplete coverage must not erase success history or block clients.
        self.stop()
        stop_slot = (int(time.time()) // 60 + 1) * 60
        self.config['auto_healing']['scrub_schedule_utc'] = datetime.fromtimestamp(
            stop_slot, timezone.utc).strftime('%a %H:%M')
        self.config['auto_healing']['scheduled_scrub_pause_millis'] = '1000'
        self.write_config()
        self.phase = 'stopped-scheduled-scrub-remains-online'
        self.start('scheduled-stop')
        operator = self.api('POST', '/auth/fragment', dict(fragment=self.fragment,
            secret=self.master, read=True, write=True, modify=True, delete=True,
            expires_in=3600), token='')['access_token']
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            status = self.health()
            if status['phase'] == 'running' and status['last_started_at'] > completed['last_started_at']:
                code, body = self.call('POST', '/maintenance/repair/stop', token=operator)
                assert code == 200, (code, body[:500])
                break
            time.sleep(.1)
        else:
            raise AssertionError('Second calendar slot was not admitted')
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            stopped = self.health()
            if stopped['phase'] == 'incomplete':
                break
            time.sleep(.1)
        else:
            raise AssertionError('Stopped scrub did not report incomplete coverage')
        assert stopped['last_completed_at'] == completed['last_completed_at']
        assert stopped['last_error'], stopped
        self.read_ack(self.put('after-stopped-scrub.bin', 16384))
        code, body = self.call('POST', '/maintenance/repair/start?check_mode=availability')
        assert code == 202, ('Stopped scrub retained the running-job guard', code, body[:500])
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            repair = self.api('GET', '/maintenance/repair/status')
            if not repair['is_running']:
                assert repair['phase'] == 'Completed' and not repair['last_error'], repair
                break
            time.sleep(.1)
        else:
            raise AssertionError('Online repair after scheduled stop did not finish')
        self.report.update(status='PASS', configured_schedule_utc=schedule,
            admitted_slot=slot, foreground_counts=counts, overlap_refused=True,
            cold_shard_repaired_before_read=True, original_immutable_files_checked=len(inventory),
            acknowledgements_hashed_before_and_after_restart=len(self.acknowledged),
            scheduled_observations=observations, completion=completed, restored_schedule=restored,
            stopped_schedule=stopped, stopped_run_keeps_write_readiness=True,
            manual_repair_after_stopped_schedule=repair)

    def finish(self):
        (self.root / 'health-observations.json').write_text(json.dumps(self.health_samples))
        super().finish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--data-shards', type=int, default=3)
    parser.add_argument('--parity-shards', type=int, default=1)
    parser.add_argument('--port', type=int, default=58741)
    parser.add_argument('--io-backend', choices=('uring', 'blocking'), default='uring')
    parser.add_argument('--deadline-seconds', type=int, default=600)
    parser.add_argument('--request-timeout-seconds', type=int, default=90)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    os.umask(0o077)
    test = ScrubProof(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
