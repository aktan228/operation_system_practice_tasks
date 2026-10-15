import threading
import unittest

import broken_wait
import fixed_wait
from sync_demo import ConditionBarrier, RingBuffer, run_barrier, run_buffer


class SynchronizationTests(unittest.TestCase):
    def test_ring_fifo_and_wraparound(self):
        ring = RingBuffer(5)
        for block in range(20):
            for item in range(block * 5, block * 5 + 5):
                ring.put(item)
            for item in range(block * 5, block * 5 + 5):
                self.assertEqual(ring.get(), item)
        self.assertEqual(ring.max_size, 5)
        self.assertEqual(ring.size, 0)

    def test_ring_rejects_overflow_and_underflow(self):
        ring = RingBuffer(1)
        with self.assertRaises(AssertionError):
            ring.get()
        ring.put(1)
        with self.assertRaises(AssertionError):
            ring.put(2)
        self.assertEqual(ring.get(), 1)

    def test_producer_consumer_exactly_once(self):
        for kind in ("semaphore", "condition"):
            for count in (1, 7, 1000):
                with self.subTest(kind=kind, items=count):
                    row = run_buffer(kind, total=count)
                    self.assertTrue(row["exactly_once"])
                    self.assertLessEqual(row["max_size"], 5)
                    self.assertEqual(sum(row["produced_per_thread"]), count)
                    self.assertEqual(sum(row["consumed_per_thread"]), count)

    def test_barrier_three_phases(self):
        self.assertTrue(run_barrier()["all_phases_valid"])

    def test_barrier_many_generations(self):
        barrier = ConditionBarrier(4)
        completed = [set() for _ in range(30)]
        mutex = threading.Lock()
        errors = []

        def worker(index):
            try:
                for phase in range(30):
                    with mutex:
                        completed[phase].add(index)
                    self.assertEqual(barrier.wait(), phase)
                    with mutex:
                        self.assertEqual(len(completed[phase]), 4)
            except BaseException as error:
                with mutex:
                    errors.append(repr(error))

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(12)
            self.assertFalse(thread.is_alive())
        self.assertFalse(errors, errors)
        self.assertEqual(barrier.generation, 30)

    def test_barrier_timeout_breaks_barrier(self):
        barrier = ConditionBarrier(2)
        with self.assertRaises(TimeoutError):
            barrier.wait(timeout=0.01)
        with self.assertRaises(RuntimeError):
            barrier.wait()

    def test_if_bug_and_fix(self):
        self.assertEqual(len(broken_wait.if_demo()["violations"]), 1)
        self.assertEqual(fixed_wait.if_demo()["violations"], [])

    def test_lost_notification_and_fix(self):
        self.assertFalse(broken_wait.lost_demo()["received_notification"])
        self.assertEqual(fixed_wait.lost_demo()["violations"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
