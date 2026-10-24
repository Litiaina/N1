#!/usr/bin/env python3
"""Shim ABI/counting regressions; fake downstream syncfs performs NO disk flush.

These tests never launch N1 and are not storage durability qualification.
"""
import errno
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest


HERE = Path(__file__).resolve().parent
CHILD = r'''
import concurrent.futures, ctypes, json, os
from pathlib import Path
root = Path(os.environ['FIXTURE'])
library = ctypes.CDLL(None, use_errno=True)
library.syncfs.argtypes = [ctypes.c_int]
library.syncfs.restype = ctypes.c_int
fds = [os.open(root / ('member-' + str(i)), os.O_RDONLY | os.O_DIRECTORY) for i in range(4)]
def call(fd):
    result = library.syncfs(fd)
    return [result, ctypes.get_errno() if result < 0 else 0]
mode = os.environ['MODE']
if mode == 'disabled':
    print(json.dumps(call(fds[0])))
elif mode == 'scope':
    Path(os.environ['N1_TEST_BATCH_TRIGGER']).touch()
    outside = os.open(root / 'member-0-not-owned', os.O_RDONLY | os.O_DIRECTORY)
    print(json.dumps(call(outside)))
elif mode == 'rounds':
    Path(os.environ['N1_TEST_BATCH_TRIGGER']).touch()
    output = []
    for _ in range(3):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
            output.append(list(workers.map(call, fds)))
    print(json.dumps(output))
elif mode == 'pause':
    Path(os.environ['N1_TEST_BATCH_TRIGGER']).touch()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
        print(json.dumps(list(workers.map(call, fds))))
else:
    raise AssertionError(mode)
'''


class ShimTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compiler = shutil.which('cc')
        if not compiler:
            raise unittest.SkipTest('C compiler is needed for the test-only shim')
        cls.directory = tempfile.TemporaryDirectory(prefix='n1-batch-shim-')
        cls.base = Path(cls.directory.name)
        cls.library = cls.base / 'batch-faults.so'
        cls.fake = cls.base / 'fake-syncfs.so'
        subprocess.run([compiler, '-shared', '-fPIC', '-pthread', '-Wall', '-Wextra', '-Werror',
                        '-o', str(cls.library), str(HERE / 'small-batch-syscalls.c'), '-ldl'],
                       check=True, capture_output=True, text=True)
        # The fake is after the shim in LD_PRELOAD, so RTLD_NEXT resolves here.
        subprocess.run([compiler, '-x', 'c', '-shared', '-fPIC', '-Wall', '-Wextra', '-Werror',
                        '-o', str(cls.fake), '-'], input='int syncfs(int fd) { (void)fd; return 0; }\n',
                       check=True, capture_output=True, text=True)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def environment(self, mode, **settings):
        work = self.base / self._testMethodName
        work.mkdir()
        for name in [*(f'member-{i}' for i in range(4)), 'member-0-not-owned']:
            (work / name).mkdir()
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith('N1_TEST_BATCH_') and key != 'LD_PRELOAD'}
        environment.update(
            LD_PRELOAD=f'{self.library}:{self.fake}', FIXTURE=str(work), MODE=mode,
            N1_TEST_BATCH_TRIGGER=str(work / 'trigger'),
            N1_TEST_BATCH_ROOTS=':'.join(str(work / f'member-{i}') for i in range(4)),
            N1_TEST_BATCH_WIDTH='4', N1_TEST_BATCH_MARKER_BASE=str(work / 'barrier'),
            N1_TEST_BATCH_TRACE=str(work / 'trace.jsonl'),
            N1_TEST_BATCH_RELEASE=str(work / 'release'), **settings)
        return work, environment

    def run_child(self, environment):
        result = subprocess.run([sys.executable, '-B', '-c', CHILD], env=environment,
                                text=True, capture_output=True, timeout=10, check=True)
        return json.loads(result.stdout)

    def test_absent_trigger_never_changes_or_counts_calls(self):
        work, environment = self.environment('disabled', N1_TEST_BATCH_FAIL_ROUND='1')
        self.assertEqual(self.run_child(environment), [0, 0])
        self.assertFalse((work / 'trace.jsonl').exists())

    def test_similar_but_unowned_root_is_not_affected(self):
        work, environment = self.environment('scope', N1_TEST_BATCH_FAIL_ROUND='1')
        self.assertEqual(self.run_child(environment), [0, 0])
        self.assertFalse((work / 'trace.jsonl').exists())

    def test_round_two_error_is_complete_scoped_and_does_not_retry(self):
        work, environment = self.environment('rounds', N1_TEST_BATCH_FAIL_ROUND='2')
        output = self.run_child(environment)
        self.assertEqual(output[0], [[0, 0]] * 4)
        self.assertEqual(output[1], [[-1, errno.EIO]] * 4)
        self.assertEqual(output[2], [[0, 0]] * 4)
        records = [json.loads(line) for line in (work / 'trace.jsonl').read_text().splitlines()]
        self.assertEqual(len(records), 12)
        self.assertEqual(sorted(row['call'] for row in records), list(range(1, 13)))
        self.assertTrue(all(row['actual_result'] == 0 for row in records))
        self.assertEqual({row['device'] for row in records}, {work.stat().st_dev})
        self.assertTrue(all(type(row['device']) is int and row['device'] > 0 for row in records))
        for round_number in (1, 2, 3):
            marker = json.loads((work / f'barrier.round-{round_number}.json').read_text())
            self.assertEqual(marker, dict(round=round_number, width=4, completed=4,
                                          after_real_syncfs=True))

    def test_pause_is_after_all_real_calls_and_can_be_released(self):
        work, environment = self.environment('pause', N1_TEST_BATCH_PAUSE_ROUND='1')
        process = subprocess.Popen([sys.executable, '-B', '-c', CHILD], env=environment,
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            end = time.monotonic() + 5
            marker = work / 'barrier.paused-round-1.json'
            while not marker.exists() and time.monotonic() < end:
                time.sleep(.01)
            self.assertTrue(marker.exists(), 'did not reach deterministic pause')
            self.assertIsNone(process.poll())
            self.assertEqual(json.loads(marker.read_text())['completed'], 4)
            (work / 'release').touch()
            output, errors = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, errors)
            self.assertEqual(json.loads(output), [[0, 0]] * 4)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == '__main__':
    unittest.main()
