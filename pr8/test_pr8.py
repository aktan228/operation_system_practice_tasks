import copy
import itertools
import json
import subprocess
import sys
import threading
import unittest
from pathlib import Path

from banker import Banker, load
from tracked_lock import DeadlockDetected, LockTracker, TrackedLock
from wait_graph import WaitGraph, detect_file, find_cycle

ROOT = Path(__file__).resolve().parent


class PracticeTests(unittest.TestCase):
    def test_bank_reference_states(self):
        bank = load(ROOT / "bank1.txt")
        self.assertEqual(bank.need("P0"), [7, 4, 3])
        self.assertEqual(bank.safety()["sequence"], ["P1", "P3", "P4", "P2", "P0"])
        self.assertTrue(bank.safety()["safe"])
        self.assertFalse(load(ROOT / "bank2.txt").safety()["safe"])

    def test_bank_requests_and_exact_rollback(self):
        bank = load(ROOT / "bank1.txt")
        before = copy.deepcopy(bank.state)
        self.assertEqual(bank.request("P4", [3, 3, 0])["status"], "DENIED_UNSAFE")
        self.assertEqual(bank.state, before)
        self.assertEqual(bank.request("P0", [0, 0, 3])["status"], "WAIT")
        self.assertEqual(bank.state, before)
        self.assertEqual(bank.request("P1", [2, 0, 0])["status"], "REJECTED")
        self.assertEqual(bank.state, before)
        self.assertEqual(bank.request("P1", [1, 0, 2])["status"], "GRANTED")
        self.assertEqual(bank.available, [2, 3, 0])
        self.assertEqual(bank.need("P1"), [0, 2, 0])
        self.assertTrue(bank.safety()["safe"])

    def test_banker_against_exhaustive_search(self):
        for available in (0, 1, 2):
            for needs in itertools.product(range(3), repeat=3):
                state = {"resources": ["A"], "available": [available], "processes": [
                    {"pid": f"P{i}", "allocation": [i % 2], "max": [i % 2 + needs[i]]}
                    for i in range(3)]}
                bank = Banker(state)
                possible = False
                for order in itertools.permutations(range(3)):
                    work = available
                    for index in order:
                        if needs[index] > work:
                            break
                        work += index % 2
                    else:
                        possible = True
                self.assertEqual(bank.safety()["safe"], possible)

    def test_invalid_requests(self):
        bank = load(ROOT / "bank1.txt")
        before = copy.deepcopy(bank.state)
        for pid, vector in (("P9", [0, 0, 0]), ("P0", [-1, 0, 0]), ("P0", [1, 2])):
            with self.assertRaises(ValueError):
                bank.request(pid, vector)
            self.assertEqual(bank.state, before)

    def test_wait_graph_logs_and_cycle_participants(self):
        rows = detect_file(ROOT / "events_deadlock.txt")
        self.assertEqual(rows[0]["participants"], ["T1", "T2"])
        self.assertEqual(detect_file(ROOT / "events_safe.txt"), [])
        self.assertEqual(find_cycle({"A": {"A"}}), ["A", "A"])
        self.assertIsNone(find_cycle({"A": {"B"}, "B": {"C"}}))

    def test_wait_graph_rebuilds_edges_after_resource_changes(self):
        state = WaitGraph()
        state.event("acquire", "T1", "A")
        state.event("wait", "T2", "A")
        self.assertEqual(state.graph(), {"T2": {"T1"}})
        state.event("release", "T1", "A")
        self.assertEqual(state.graph(), {})
        state.event("acquire", "T3", "A")
        self.assertEqual(state.graph(), {"T2": {"T3"}})
        with self.assertRaises(ValueError):
            state.event("acquire", "T4", "A")

    def test_raw_deadlock_is_real_and_dumped(self):
        process = subprocess.Popen([sys.executable, str(ROOT / "deadlock.py"), "--dump-after", "0.1"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                process.communicate(timeout=1)
        finally:
            process.kill()
            stdout, stderr = process.communicate()
        self.assertEqual(stdout.count("first lock acquired"), 2)
        self.assertIn("Timeout", stderr)
        self.assertGreaterEqual(stderr.count("in worker"), 2)

    def test_ordered_and_tracked_finish(self):
        for mode in ("ordered", "tracked"):
            result = subprocess.run([sys.executable, str(ROOT / "deadlock.py"), "--mode", mode],
                                    capture_output=True, text=True, timeout=5, check=True)
            self.assertIn("RECOVERED" if mode == "tracked" else "both threads finished", result.stdout)

    def test_tracked_lock_timeout_nonblocking_and_owner(self):
        tracker = LockTracker()
        lock = TrackedLock("A", tracker)
        self.assertTrue(lock.acquire())
        self.assertFalse(lock.acquire(blocking=False))
        with self.assertRaises(DeadlockDetected):
            lock.acquire(timeout=0.01)  # a non-reentrant self-cycle
        results = []

        def contender():
            results.append(lock.acquire(timeout=0.02))
            try:
                lock.release()
            except RuntimeError:
                results.append("owner_checked")

        thread = threading.Thread(target=contender)
        thread.start()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [False, "owner_checked"])
        self.assertFalse(tracker.state.waiting)
        lock.release()
        self.assertFalse(lock.locked())
        self.assertFalse(tracker.state.owners)

    def test_philosophers_all_modes(self):
        for mode in ("naive", "ordered", "waiter"):
            result = subprocess.run([sys.executable, str(ROOT / "philosophers.py"),
                                     "--mode", mode, "--seconds", "0.3", "--json"],
                                    capture_output=True, text=True, timeout=7, check=True)
            row = json.loads(result.stdout)
            if mode == "naive":
                self.assertEqual(row["meals"], [0] * 5)
                self.assertEqual(row["alive_at_end"], 5)
            else:
                self.assertTrue(all(row["meals"]))
                self.assertEqual(row["alive_at_end"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
