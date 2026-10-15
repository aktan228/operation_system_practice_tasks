import concurrent.futures
import json
import signal
import socket
import tempfile
import unittest
from pathlib import Path

import fifo_service
import unix_service
from bench_memory import benchmark
from calculator import calculate, response
from run_experiments import server


class IPCTests(unittest.TestCase):
    def test_arithmetic(self):
        for expression, expected in (("2+3*4", 14), ("(2+3)*4", 20), ("-8//3", -3),
                                     ("2**10", 1024), ("7/2", 3.5), ("7%3", 1)):
            self.assertEqual(calculate(expression), expected)

    def test_rejects_code_and_bad_requests(self):
        for expression in ("__import__('os')", "(1).__class__", "True", "2**100000", "1/0", "[1,2]", ""):
            self.assertFalse(response({"id": "test", "expression": expression})["ok"])
        self.assertFalse(response([])["ok"])

    def test_fifo_sequential_and_concurrent_callers(self):
        with tempfile.TemporaryDirectory(prefix="pr7-test-") as temp:
            directory = Path(temp) / "fifo"
            with server("fifo", directory):
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(fifo_service.request, f"{i}+10", directory) for i in range(10)]
                    for i, future in enumerate(futures):
                        self.assertEqual(future.result(timeout=15)["response"]["result"], i + 10)
                self.assertFalse(fifo_service.request("1/0", directory)["response"]["ok"])
            self.assertFalse(directory.exists())

    def test_unix_independent_worker_processes(self):
        with tempfile.TemporaryDirectory(prefix="pr7-test-") as temp:
            directory = Path(temp) / "unix"
            with server("unix", directory):
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(unix_service.request, f"{i}*2", directory) for i in range(5)]
                    rows = [future.result(timeout=15) for future in futures]
                self.assertEqual([row["response"]["result"] for row in rows], [0, 2, 4, 6, 8])
                self.assertEqual(len({row["response"]["worker_pid"] for row in rows}), 5)
                self.assertFalse(unix_service.request("1/0", directory)["response"]["ok"])
            self.assertFalse(directory.exists())

    def test_unix_fragmented_message(self):
        with tempfile.TemporaryDirectory(prefix="pr7-test-") as temp:
            directory = Path(temp) / "unix"
            with server("unix", directory):
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.settimeout(10)
                    sock.connect(str(directory / "service.sock"))
                    sock.sendall(b'{"id":"test",')
                    sock.sendall(b'"expression":"6*7"}\n')
                    self.assertEqual(unix_service.read_line(sock)["result"], 42)

    def test_sigint_cleanup(self):
        for kind in ("fifo", "unix"):
            with tempfile.TemporaryDirectory(prefix="pr7-test-") as temp:
                directory = Path(temp) / kind
                with server(kind, directory) as process:
                    process.send_signal(signal.SIGINT)
                    self.assertEqual(process.wait(timeout=10), 0)
                self.assertFalse(directory.exists())

    def test_shared_memory_pipe_integrity_and_cleanup(self):
        report = benchmark(count=100_000, repeats=2)
        self.assertTrue(report["cleanup_ok"])
        self.assertEqual(len(report["runs"]), 4)
        self.assertTrue(all(row["correct"] for row in report["runs"]))
        self.assertEqual(len({row["sha256"] for row in report["runs"]}), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
