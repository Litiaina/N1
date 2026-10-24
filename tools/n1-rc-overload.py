#!/usr/bin/env python3
"""Extreme offered-load/crash extension to n1-rc-stress.py (stdlib only)."""
import argparse
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import secrets
import sys
import time
import urllib.parse

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('rc_stress', Path(__file__).with_name('n1-rc-stress.py'))
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


class Overload(base.Stress):
    async def raw(self, method, route, data=None, token=None):
        if getattr(self.args,'force_io_gate',False) and method=='POST' and route.startswith('/upload/'):
            route += '&io_gate=true'
        writer = None
        started = time.monotonic()
        phase, status = self.phase, 'transport_error'
        headers = {'Host': '127.0.0.1', 'Connection': 'close'}
        if token is None:
            token = self.token
        if token:
            headers['Authorization'] = 'Bearer '+token
        if data is not None:
            headers['Content-Type'] = 'application/octet-stream'
            headers['Content-Length'] = str(len(data))
        with self.lock:
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
        try:
            async with asyncio.timeout(120):
                reader, writer = await asyncio.open_connection('127.0.0.1', self.args.port, ssl=self.tls,
                                                               server_hostname='127.0.0.1', ssl_handshake_timeout=60)
                header = f'{method} /noa/v1{route} HTTP/1.1\r\n'+''.join(f'{k}: {v}\r\n' for k,v in headers.items())+'\r\n'
                writer.write(header.encode())
                if data:
                    writer.write(data)
                await writer.drain()
                response_header = (await reader.readuntil(b'\r\n\r\n')).decode('latin1').split('\r\n')
                status = int(response_header[0].split()[1])
                response_headers = {k.lower():v.strip() for line in response_header[1:] if ':' in line for k,v in [line.split(':',1)]}
                if 'content-length' in response_headers:
                    body = await reader.readexactly(int(response_headers['content-length']))
                elif response_headers.get('transfer-encoding','').lower() == 'chunked':
                    parts = []
                    while True:
                        length = int((await reader.readline()).split(b';',1)[0],16)
                        if not length:
                            while (await reader.readline()) not in (b'\r\n',b''):
                                pass
                            break
                        parts.append(await reader.readexactly(length))
                        assert await reader.readexactly(2) == b'\r\n'
                    body = b''.join(parts)
                else:
                    body = await reader.read()
                return status, body
        finally:
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except (OSError, asyncio.TimeoutError):
                    pass
            with self.lock:
                self.active -= 1
                self.calls[phase][str(status)] += 1
                self.latency[phase].append((time.monotonic()-started)*1000)

    async def wave(self, name, concurrency, requests, crash=False, shared=False):
        permit = asyncio.Semaphore(concurrency)
        failures, expected_disconnects = [], 0
        accepted_latencies, read_latencies = [], []
        accepted_before = len(self.acknowledged)
        async def worker(index):
            nonlocal expected_disconnects
            async with permit:
                started = time.monotonic()
                seed = f'{name}-{index}'
                key = 'contended.bin' if shared else f'{name}/object-{index:05}.bin'
                # During normal load, mix writers with live reads and LIST calls.
                write = crash or shared or index % 8 < 6
                body = base.payload(seed, 65536 if crash else 8192)
                try:
                    if write:
                        status, result = await self.raw('POST', '/upload/one-shot/'+urllib.parse.quote(key,safe='/')+'?durability=strict', body)
                        if status == 201:
                            reply = json.loads(result)
                            assert reply['version_id'] and reply['object_key'] == key
                            self.acknowledged.append((self.token,key,reply['version_id'],base.digest(body),len(body)))
                            accepted_latencies.append((time.monotonic()-started)*1000)
                        elif status != 503 or not result.startswith(b'disk scheduler admission timed out for foreground '):
                            failures.append(f'{key}: upload HTTP {status}: {result[:180]!r}')
                    else:
                        route = '/objects/download?object_key=baseline.bin' if index%8==6 else '/posix/list?path='+name+'&limit=73'
                        status, result = await self.raw('GET', route)
                        if status == 200:
                            if index%8==6:
                                assert result == base.payload('baseline')
                            else:
                                assert isinstance(json.loads(result)['entries'],list)
                            read_latencies.append((time.monotonic()-started)*1000)
                        elif status != 503 or not result.startswith(b'disk scheduler admission timed out for foreground '):
                            failures.append(f'{key}: read/LIST HTTP {status}: {result[:180]!r}')
                except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError) as error:
                    if crash and self.crash_fired:
                        expected_disconnects += 1
                    else:
                        failures.append(f'{key}: '+type(error).__name__+': '+str(error))

        self.crash_fired = False
        tasks = [asyncio.create_task(worker(i)) for i in range(requests)]
        async def interrupter():
            deadline = time.monotonic()+60
            while len(self.acknowledged)-accepted_before < 16:
                assert time.monotonic() < deadline, 'No committed prefix before crash deadline'
                await asyncio.sleep(.005)
            self.crash_fired = True
            self.stop()
        killer = asyncio.create_task(interrupter()) if crash else None
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if killer:
            await killer
        failures.extend(str(result) for result in results if isinstance(result,BaseException))
        # Preserve all acknowledged versions even if the phase exposed a failure.
        (self.root/'acknowledgements.json').write_text(json.dumps([
            {'key':key,'version_id':version,'sha256':sha,'bytes':size} for _,key,version,sha,size in self.acknowledged]))
        details = {'offered_requests':requests,'client_concurrency':concurrency,
                   'new_acknowledged_versions':len(self.acknowledged)-accepted_before,
                   'accepted_write_latency':base.distribution(accepted_latencies),
                   'successful_read_list_latency':base.distribution(read_latencies),
                   'expected_crash_disconnects':expected_disconnects,'unexpected_failures':len(failures),
                   'failure_examples':failures[:8]}
        if failures:
            self.errors.extend(failures)
        return details

    def run(self):
        self.errors = []
        self.mixed_prefixes = []
        self.start('extreme-initial')
        self.fragment,self.token = self.create('rc-overload-'+secrets.token_hex(5))
        assert self.upload((self.token,'baseline.bin','baseline',16384)) == 201
        for concurrency in ((2048,) if self.args.single_wave else (1024,2048,2048)):
            name = f'wave-{len(self.phases)}-c{concurrency}'
            self.phase_run(name, lambda name=name,c=concurrency: asyncio.run(self.wave(name,c,4096)))
            self.mixed_prefixes.append(name)
        self.phase_run('pagination-and-refused-write-absence', self.verify_listing)
        if self.args.single_wave:
            self.phase_run('all-acknowledgements-after-overload',self.verify_all)
            self.phase_run('quiescent-recovery',self.quiescent)
            self.report.update(acknowledged_versions=len(self.acknowledged),peak_active_requests=self.peak_active,
                               elapsed_seconds=round(time.monotonic()-self.started,3),unexpected_failures=len(self.errors),
                               failure_examples=self.errors[:10],force_io_gate=self.args.force_io_gate)
            assert not self.errors, f'{len(self.errors)} unexpected overload errors'
            self.report['status']='PASS'
            return
        self.phase_run('same_key_1024_concurrent', lambda: asyncio.run(self.wave('contended',1024,2048,shared=True)))
        versions = self.versions('contended.bin')
        ack_ids = {ack[2] for ack in self.acknowledged if ack[1]=='contended.bin'}
        version_ids = {version['version_id'] for version in versions}
        assert ack_ids <= version_ids, 'Acknowledged shared-key version is missing'
        # A client timeout can leave a valid unacknowledged commit; it is not
        # evidence of corruption or lost lineage. Verify every extra version.
        expected_hashes = {base.digest(base.payload(f'contended-{i}',8192)) for i in range(2048)}
        for version in version_ids-ack_ids:
            status, data = self.call('GET','/objects/download?'+urllib.parse.urlencode({'object_key':'contended.bin','version_id':version}))
            assert status==200 and base.digest(data) in expected_hashes, 'Unacknowledged shared-key version contains unknown bytes'
        self.report['reconciled_unacknowledged_shared_versions'] = len(version_ids-ack_ids)
        for cycle in range(3):
            name = f'inflight-kill-{cycle}'
            self.phase_run(name,lambda name=name:asyncio.run(self.wave(name,2048,2048,crash=True)))
            self.start(f'after-inflight-kill-{cycle}')
            self.phase_run(f'integrity-after-kill-{cycle}',self.verify_all)
        self.phase_run('all-version-integrity-after-final-drain',self.verify_all)
        # Direct namespace mutation proof after overload, without moving object bytes.
        before = self.immutable_inventory()
        self.api('POST','/posix/rename',{'source_path':'baseline.bin','destination_path':'moved.bin'})
        self.api('POST','/posix/trash',{'path':'moved.bin'})
        self.api('POST','/posix/recover',{'path':'moved.bin'})
        self.api('POST','/posix/rename',{'source_path':'moved.bin','destination_path':'baseline.bin'})
        assert before == self.immutable_inventory(), 'Post-overload NCX mutation rewrote immutable data'
        self.phase_run('quiescent-recovery',lambda:self.quiescent())
        self.report.update(acknowledged_versions=len(self.acknowledged),peak_active_requests=self.peak_active,
                           elapsed_seconds=round(time.monotonic()-self.started,3),unexpected_failures=len(self.errors),
                           failure_examples=self.errors[:10],inflight_sigkill_cycles=3)
        assert not self.errors, f'{len(self.errors)} unexpected overload errors; inspect results'
        self.report['status']='PASS'

    def verify_all(self):
        self.parallel(self.read_ack,list(self.acknowledged),32)
        return {'every_acknowledged_version_sha256_verified':len(self.acknowledged)}

    def verify_listing(self):
        verified = 0
        for prefix in self.mixed_prefixes:
            actual = set(self.list_all(prefix))
            acknowledged = {key for _,key,*_ in self.acknowledged if key.startswith(prefix+'/')}
            assert acknowledged <= actual, 'Paginated LIST omitted an acknowledged object'
            if not self.errors:
                assert actual == acknowledged, 'An admission-refused unique write became visible'
            verified += len(acknowledged)
        return {'acknowledged_names_verified':verified,'admission_refused_writes_absent':not self.errors}

    def quiescent(self):
        time.sleep(10)
        self.verify_all()
        return {'all_acknowledged_versions_still_readable':True}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--member-parent',type=Path,action='append',required=True)
    parser.add_argument('--data-shards',type=int,default=1)
    parser.add_argument('--parity-shards',type=int,default=1)
    parser.add_argument('--port',type=int,default=56821)
    parser.add_argument('--deadline-seconds',type=int,default=2400)
    parser.add_argument('--wait-for',type=Path)
    parser.add_argument('--force-io-gate',action='store_true')
    parser.add_argument('--single-wave',action='store_true')
    args=parser.parse_args()
    if args.wait_for:
        deadline=time.monotonic()+1800
        while not args.wait_for.exists():
            assert time.monotonic()<deadline,'Previous gate did not complete'
            time.sleep(2)
        previous = json.loads(args.wait_for.read_text())
        assert previous.get('status') == 'PASS', 'Previous gate failed; investigate before escalating load'
    args.binary=args.binary.resolve(strict=True)
    os.umask(0o077)
    test=Overload(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL',error=f'{type(error).__name__}: {error}')
        raise
    finally:
        test.finish()
