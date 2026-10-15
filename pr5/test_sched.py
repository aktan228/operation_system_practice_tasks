import unittest
from sched import Process, metrics, read_processes, simulate
from pathlib import Path


class SchedulingTests(unittest.TestCase):
    def test_references(self):
        processes = read_processes(Path(__file__).with_name("set1.txt"))
        # FCFS reference in the assignment is inconsistent with the supplied set1.
        for algo, wait, turn in (("fcfs", 8.75, 15.25), ("srtf", 6.5, 13), ("rr", 13.5, 20)):
            with self.subTest(algorithm=algo):
                _, avg = metrics(processes, simulate(processes, algo, quantum=3))
                self.assertEqual(avg["waiting"], wait)
                self.assertEqual(avg["turnaround"], turn)

    def test_rr_boundary(self):
        processes = [Process("A", 0, 6), Process("B", 3, 1)]
        self.assertEqual(simulate(processes, "rr", quantum=3),
                         [(0, 3, "A"), (3, 4, "B"), (4, 7, "A")])

    def test_srtf_preemption(self):
        self.assertEqual(simulate([Process("A", 0, 8), Process("B", 2, 1)], "srtf"),
                         [(0, 2, "A"), (2, 3, "B"), (3, 9, "A")])

    def test_idle(self):
        processes = [Process("A", 2, 2), Process("B", 7, 1)]
        for algo in ("fcfs", "sjf", "srtf", "priority", "rr"):
            rows, avg = metrics(processes, simulate(processes, algo))
            self.assertEqual(avg["utilization"], 37.5)
            self.assertTrue(all(r["waiting"] == r["response"] == 0 for r in rows))

    def test_priority_and_aging(self):
        processes = [Process("LOW", 0, 20, 10)] + [Process(f"H{i}", 2 * i, 2, 0) for i in range(100)]
        plain, _ = metrics(processes, simulate(processes, "priority"))
        aged, _ = metrics(processes, simulate(processes, "priority", aging=2))
        self.assertEqual(plain[0]["first"], 200)
        self.assertLess(aged[0]["first"], 200)
        self.assertEqual(simulate([Process("A", 0, 4, 5), Process("B", 1, 1, 0)], "priority"),
                         [(0, 1, "A"), (1, 2, "B"), (2, 5, "A")])

    def test_conservation(self):
        for name in ("set1.txt", "set2.txt", "set3.txt"):
            processes = read_processes(Path(__file__).with_name(name))
            for algo in ("fcfs", "sjf", "srtf", "priority", "rr"):
                intervals = simulate(processes, algo)
                self.assertEqual(intervals[0][0], 0)
                for left, right in zip(intervals, intervals[1:]):
                    self.assertEqual(left[1], right[0])
                for p in processes:
                    self.assertEqual(sum(b - a for a, b, pid in intervals if pid == p.pid), p.burst)
                    self.assertTrue(all(a >= p.arrival for a, _, pid in intervals if pid == p.pid))
                rows, _ = metrics(processes, intervals)
                self.assertTrue(all(r["waiting"] >= 0 and r["response"] >= 0 for r in rows))


if __name__ == "__main__":
    unittest.main(verbosity=2)
