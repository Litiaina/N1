"""Focused proof safety/protocol checks without starting any engine."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

_spec = importlib.util.spec_from_file_location('duplicate_body_proof', Path(__file__).with_name('n1-durable-duplicate-body-proof.py'))
proof = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(proof)


class DuplicateBodyProofTests(unittest.TestCase):
    def connection(self):
        connection = MagicMock()
        connection.sock.getsockname.return_value = ('127.0.0.1', 12345)
        response = MagicMock(status=200)
        response.read.return_value = b'Part already uploaded'
        response.getheader.return_value = None
        ping = MagicMock(status=200)
        connection.getresponse.side_effect = [response, ping]
        return connection

    def test_green_streams_remaining_body_and_reuses_exact_connection(self):
        connection = self.connection()
        with patch.object(proof, 'early_response', return_value=b''), patch.object(proof.time, 'sleep'):
            result = proof.paced_duplicate(connection, '/upload/0?durability=strict', 'secret', b'x' * proof.PART_BYTES, 'drained')
        self.assertEqual(sum(len(call.args[0]) for call in connection.send.call_args_list), proof.PART_BYTES)
        self.assertLessEqual(max(len(call.args[0]) for call in connection.send.call_args_list), 4096)
        self.assertTrue(result['same_connection_reused'])
        self.assertNotIn('secret', str(result))
        connection.request.assert_called_once_with('GET', '/noa/v1/ping')

    def test_green_rejects_early_response_without_retry(self):
        connection = self.connection()
        with patch.object(proof, 'early_response', return_value=b'HTTP/1.1 200 OK\r\n'), self.assertRaisesRegex(AssertionError, 'before receiving'):
            proof.paced_duplicate(connection, '/route', 'secret', b'x' * proof.PART_BYTES, 'drained')
        self.assertEqual(connection.send.call_count, 1)

    def test_red_requires_real_failed_remaining_send(self):
        connection = self.connection()
        connection.send.side_effect = [None, BrokenPipeError(32, 'Broken pipe')]
        with patch.object(proof, 'early_response', return_value=b'HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n'), patch.object(proof.time, 'sleep'):
            result = proof.paced_duplicate(connection, '/route', 'secret', b'x' * proof.PART_BYTES, 'closed')
        self.assertTrue(result['early_success_then_failed_send_reproduced'])
        self.assertEqual(result['total_body_bytes_sent'], 1024)
        self.assertEqual(connection.send.call_count, 2)

    def test_green_transport_error_is_not_retried(self):
        connection = self.connection()
        connection.send.side_effect = [None, BrokenPipeError(32, 'Broken pipe')]
        with patch.object(proof, 'early_response', return_value=b''), self.assertRaises(BrokenPipeError):
            proof.paced_duplicate(connection, '/route', 'secret', b'x' * proof.PART_BYTES, 'drained')
        self.assertEqual(connection.send.call_count, 2)

    def test_green_rejects_reconnection(self):
        connection = self.connection()
        connection.sock.getsockname.side_effect = [('127.0.0.1', 12345), ('127.0.0.1', 12346)]
        with patch.object(proof, 'early_response', return_value=b''), patch.object(proof.time, 'sleep'), self.assertRaisesRegex(AssertionError, 'silently reconnected'):
            proof.paced_duplicate(connection, '/route', 'secret', b'x' * proof.PART_BYTES, 'drained')


if __name__ == '__main__':
    unittest.main()
