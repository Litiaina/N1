#!/usr/bin/env python3
"""Paced duplicate-part HTTP/1.1 RED/GREEN proof using new owned data roots.

Never retries a failed write, contacts production, or changes an existing store.
The GREEN gate requires streamed request-body consumption, connection reuse and
an exact committed-object hash. The RED gate preserves an early-close failure.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import select
import socket
import sys
import time
import traceback
import urllib.parse

sys.dont_write_bytecode = True
_spec = importlib.util.spec_from_file_location('session_race_proof', Path(__file__).with_name('n1-durable-session-race-proof.py'))
session_proof = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(session_proof)
base = session_proof.base
PART_BYTES = 65536


def early_response(connection):
    ready, _, _ = select.select([connection.sock], [], [], .5)
    if not ready:
        return b''
    connection.sock.settimeout(.5)
    try:
        # Read at most one small response; a TLS control record alone is not an
        # HTTP response and must not turn a correctly waiting server into FAIL.
        return connection.sock.recv(8192)
    except socket.timeout:
        return b''
    finally:
        connection.sock.settimeout(10)


def paced_duplicate(connection, route, token, body, expectation):
    assert len(body) == PART_BYTES
    assert expectation in ('closed', 'drained')
    connection.putrequest('PUT', '/noa/v1' + route)
    connection.putheader('Authorization', 'Bearer ' + token)
    connection.putheader('Content-Type', 'application/octet-stream')
    connection.putheader('Content-Length', str(len(body)))
    connection.endheaders()
    sent = 1024
    connection.send(body[:sent])
    response = early_response(connection)
    result = dict(declared_body_bytes=len(body), initial_body_bytes_sent=sent,
                  early_response_bytes=response.decode('iso-8859-1'))
    if expectation == 'closed':
        assert response.startswith(b'HTTP/1.1 200'), 'RED did not reproduce early duplicate success'
        assert b'connection: close' in response.lower(), 'RED did not advertise premature connection closure'
        time.sleep(.2)
        try:
            while sent < len(body):
                chunk = body[sent:sent + 4096]
                connection.send(chunk)
                sent += len(chunk)
                time.sleep(.02)
        except OSError as error:
            result['remaining_body_error'] = f'{type(error).__name__}: {error}'
        result['total_body_bytes_sent'] = sent
        assert sent < len(body) and 'remaining_body_error' in result, 'RED must reproduce the failed body send'
        result['early_success_then_failed_send_reproduced'] = True
        return result

    assert not response, 'GREEN returned an HTTP response before receiving the declared duplicate body'
    while sent < len(body):
        chunk = body[sent:sent + 4096]
        connection.send(chunk)
        sent += len(chunk)
        time.sleep(.02)
    reply = connection.getresponse()
    response_body = reply.read()
    assert reply.status == 200 and response_body == b'Part already uploaded', (reply.status, response_body)
    assert (reply.getheader('Connection') or '').lower() != 'close', 'Duplicate response disabled keepalive'
    assert connection.sock is not None, 'Server closed the duplicate connection'
    endpoint = connection.sock.getsockname()
    connection.request('GET', '/noa/v1/ping')
    ping = connection.getresponse()
    ping.read()
    assert ping.status == 200 and connection.sock is not None
    assert connection.sock.getsockname() == endpoint, 'Ping silently reconnected instead of reusing HTTP/1.1'
    result.update(total_body_bytes_sent=sent, duplicate_status=reply.status,
                  same_connection_ping_status=ping.status, same_connection_reused=True)
    return result


class DuplicateBodyProof(base.Stress):
    def __init__(self, args):
        assert len(args.member_parent) == 1, 'This focused HTTP gate uses a single 1+0 member'
        session_proof.data_device(args.root.parent)
        session_proof.data_device(args.member_parent[0])
        super().__init__(args)
        self.report.update(proof='Paced duplicate-part body lifetime', expectation=args.expectation,
                           rate_limiter_profile=session_proof.apply_load_profile(self.config),
                           harness_sha256=base.digest(Path(__file__).read_bytes()))
        self.write_config()

    def run(self):
        self.start('paced-duplicate')
        self.fragment, self.token = self.create('paced-duplicate-body')
        key = 'paced/duplicate.bin'
        params = dict(content_type='application/octet-stream', total_size_bytes=2 * PART_BYTES,
                      expected_parts=2, part_size_bytes=PART_BYTES)
        session = self.api('POST', '/upload/multipart/durable/init/' + key, params)
        route = '/upload/multipart/durable/' + session['version_id'] + '/0?durability=strict'
        body = base.payload('paced-body-part-0', PART_BYTES)
        status, response = self.call('PUT', route, body)
        assert status == 200, (status, response)
        connection = base.http.client.HTTPSConnection('127.0.0.1', self.args.port, context=self.tls, timeout=10)
        try:
            self.report['transport_observation'] = paced_duplicate(connection, route, self.token, body, self.args.expectation)
        finally:
            connection.close()
        if self.args.expectation == 'closed':
            self.report['status'] = 'EXPECTED_FAILURE_REPRODUCED'
            return

        second = base.payload('paced-body-part-1', PART_BYTES)
        status, response = self.call('PUT', route.replace('/0?', '/1?'), second)
        assert status == 200, (status, response)
        result = self.api('POST', '/upload/multipart/durable/' + session['version_id'] + '/' + session['session_id'] +
                          '/finalize?' + urllib.parse.urlencode({'object_key': key}), params)
        assert result['version_id'] == session['version_id'] and result['object_key'] == key
        expected = base.digest(body + second)
        self.read_ack((self.token, key, session['version_id'], expected, 2 * PART_BYTES))
        assert not self.fault, self.fault
        self.report.update(status='PASS', committed_object_sha256=expected,
                           committed_bytes_verified=2 * PART_BYTES,
                           elapsed_seconds=round(time.monotonic() - self.started, 3))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--member-parent', type=Path, action='append', required=True)
    parser.add_argument('--expectation', choices=('closed', 'drained'), required=True)
    parser.add_argument('--port', type=int, default=57641)
    parser.add_argument('--io-backend', choices=('auto', 'uring', 'blocking'), default='uring')
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    args.data_shards, args.parity_shards = 1, 0
    args.deadline_seconds, args.request_timeout_seconds = 180, 10
    os.umask(0o077)
    test = DuplicateBodyProof(args)
    try:
        test.run()
    except BaseException as error:
        test.report.update(status='FAIL', error=f'{type(error).__name__}: {error}', traceback=traceback.format_exc())
        raise
    finally:
        test.finish()


if __name__ == '__main__':
    main()
