#!/usr/bin/env python3
"""Variant B: bounded ring buffers and a reusable condition-variable barrier."""
import argparse
import json
import logging
import threading
import time
from collections import Counter
from pathlib import Path

EMPTY = object()
STOP = object()


class Cancelled(RuntimeError):
    pass


class RingBuffer:
    """Storage only. The caller MUST hold its external mutex."""

    def __init__(self, capacity=5):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.data = [EMPTY] * capacity
        self.head = self.tail = self.size = self.max_size = 0
        self.puts = self.gets = 0

    def check(self):
        assert 0 <= self.size <= self.capacity, "buffer size outside bounds"
        assert 0 <= self.head < self.capacity and 0 <= self.tail < self.capacity
        assert self.tail == (self.head + self.size) % self.capacity
        assert sum(item is not EMPTY for item in self.data) == self.size
        occupied = {(self.head + offset) % self.capacity for offset in range(self.size)}
        assert all((item is not EMPTY) == (i in occupied)
                   for i, item in enumerate(self.data)), "broken ring layout"

    def put(self, item):
        self.check()
        assert self.size < self.capacity, "buffer overflow"
        assert item is not EMPTY
        self.data[self.tail] = item
        self.tail = (self.tail + 1) % self.capacity
        self.size += 1
        self.puts += 1
        self.max_size = max(self.max_size, self.size)
        self.check()

    def get(self):
        self.check()
        assert self.size > 0, "buffer underflow"
        item = self.data[self.head]
        self.data[self.head] = EMPTY
        self.head = (self.head + 1) % self.capacity
        self.size -= 1
        self.gets += 1
        self.check()
        return item


class SemaphoreBuffer:
    def __init__(self, capacity, cancel):
        self.ring = RingBuffer(capacity)
        self.mutex = threading.Lock()
        self.slots = threading.Semaphore(capacity)
        self.items = threading.Semaphore(0)
        self.cancel = cancel

    def acquire(self, semaphore):
        while not self.cancel.is_set():
            if semaphore.acquire(timeout=0.1):
                return
        raise Cancelled("run cancelled")

    def put(self, item):
        self.acquire(self.slots)
        try:
            with self.mutex:
                self.ring.put(item)
        except BaseException:
            self.slots.release()
            raise
        self.items.release()

    def get(self):
        self.acquire(self.items)
        try:
            with self.mutex:
                item = self.ring.get()
        except BaseException:
            self.items.release()
            raise
        self.slots.release()
        return item


class ConditionBuffer:
    def __init__(self, capacity, cancel):
        self.ring = RingBuffer(capacity)
        self.mutex = threading.Lock()
        self.changed = threading.Condition(self.mutex)
        self.cancel = cancel

    def put(self, item):
        with self.changed:
            while self.ring.size == self.ring.capacity:
                if self.cancel.is_set():
                    raise Cancelled("run cancelled")
                self.changed.wait(timeout=0.1)
            if self.cancel.is_set():
                raise Cancelled("run cancelled")
            self.ring.put(item)
            self.changed.notify_all()

    def get(self):
        with self.changed:
            while self.ring.size == 0:
                if self.cancel.is_set():
                    raise Cancelled("run cancelled")
                self.changed.wait(timeout=0.1)
            if self.cancel.is_set():
                raise Cancelled("run cancelled")
            item = self.ring.get()
            self.changed.notify_all()
            return item


def join_until(threads, deadline):
    for thread in threads:
        thread.join(max(0, deadline - time.monotonic()))
    alive = [thread.name for thread in threads if thread.is_alive()]
    if alive:
        raise TimeoutError(f"threads did not finish: {alive}")


def run_buffer(kind, total=1000, timeout=20):
    """1000 is the TOTAL across 3 producers, not 1000 per producer."""
    if total <= 0:
        raise ValueError("total must be positive")
    cancel = threading.Event()
    start = threading.Event()
    audit = threading.Lock()
    buffer_type = {"semaphore": SemaphoreBuffer, "condition": ConditionBuffer}[kind]
    buffer = buffer_type(5, cancel)
    seen = Counter()
    produced = [0] * 3
    consumed = [0] * 2
    errors = []

    def guarded(action):
        try:
            start.wait()
            action()
        except Cancelled:
            pass
        except BaseException as error:
            with audit:
                errors.append(repr(error))
            logging.exception("VIOLATION in %s", threading.current_thread().name)
            cancel.set()

    def producer(index):
        # Disjoint sequences: 0,3,6,...; 1,4,7,...; 2,5,8,...
        for item in range(index, total, 3):
            buffer.put(item)
            produced[index] += 1
            if item % 17 == 0:
                time.sleep(0)  # encourage different interleavings outside the lock

    def consumer(index):
        while True:
            item = buffer.get()
            if item is STOP:
                return
            with audit:
                assert isinstance(item, int) and 0 <= item < total, "invalid item"
                seen[item] += 1
                assert seen[item] == 1, f"duplicate item: {item}"
                consumed[index] += 1
            if item % 13 == 0:
                time.sleep(0)

    producers = [threading.Thread(target=guarded, args=(lambda i=i: producer(i),),
                                 name=f"producer-{i}", daemon=True) for i in range(3)]
    consumers = [threading.Thread(target=guarded, args=(lambda i=i: consumer(i),),
                                 name=f"consumer-{i}", daemon=True) for i in range(2)]
    threads = producers + consumers
    begin = time.monotonic()
    deadline = begin + timeout
    try:
        for thread in threads:
            thread.start()
        start.set()
        join_until(producers, deadline)
        if errors:
            raise AssertionError(errors)
        # One stop token per consumer, AFTER all producers have finished.
        for _ in consumers:
            buffer.put(STOP)
        join_until(consumers, deadline)
        assert not errors, errors
        assert seen == Counter(range(total)), "missing or duplicated elements"
        assert sum(produced) == sum(consumed) == total
        buffer.ring.check()
        assert buffer.ring.size == 0
        assert buffer.ring.puts == buffer.ring.gets == total + 2
        return {"algorithm": kind, "items": total, "producers": 3, "consumers": 2,
                "capacity": 5, "max_size": buffer.ring.max_size,
                "produced_per_thread": produced, "consumed_per_thread": consumed,
                "exactly_once": True, "empty_at_end": True, "violations": 0,
                "seconds": round(time.monotonic() - begin, 6)}
    except BaseException:
        logging.exception("VIOLATION: %s run failed", kind)
        raise
    finally:
        cancel.set()
        start.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=0.3)


class ConditionBarrier:
    """Reusable barrier with generations; deliberately no threading.Barrier."""

    def __init__(self, parties):
        if parties <= 0:
            raise ValueError("parties must be positive")
        self.parties = parties
        self.arrived = 0
        self.generation = 0
        self.broken = False
        self.condition = threading.Condition(threading.Lock())

    def wait(self, timeout=10):
        deadline = time.monotonic() + timeout
        with self.condition:
            if self.broken:
                raise RuntimeError("barrier is broken")
            generation = self.generation
            self.arrived += 1
            assert 1 <= self.arrived <= self.parties
            if self.arrived == self.parties:
                self.arrived = 0
                self.generation += 1
                self.condition.notify_all()
            else:
                while generation == self.generation and not self.broken:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.broken = True
                        self.condition.notify_all()
                        raise TimeoutError("barrier timeout")
                    self.condition.wait(remaining)
                if self.broken:
                    raise RuntimeError("barrier is broken")
            return generation


def run_barrier():
    barrier = ConditionBarrier(4)
    audit = threading.Lock()
    completed = [set() for _ in range(3)]
    events = []
    errors = []

    def worker(index):
        try:
            for phase in range(3):
                with audit:
                    if phase:
                        assert len(completed[phase - 1]) == 4, "phase started too early"
                    events.append(f"T{index}: start phase {phase + 1}")
                # Work outside the critical section; different durations.
                time.sleep(0.001 * ((index + phase) % 4))
                with audit:
                    assert index not in completed[phase]
                    completed[phase].add(index)
                    events.append(f"T{index}: finish phase {phase + 1}")
                assert barrier.wait() == phase
                with audit:
                    assert len(completed[phase]) == 4, "barrier released too early"
        except BaseException as error:
            with audit:
                errors.append(repr(error))
            logging.exception("VIOLATION in barrier worker")

    threads = [threading.Thread(target=worker, args=(i,), name=f"T{i}", daemon=True)
               for i in range(4)]
    for thread in threads:
        thread.start()
    join_until(threads, time.monotonic() + 15)
    assert not errors, errors
    assert all(group == set(range(4)) for group in completed)
    assert barrier.generation == 3 and barrier.arrived == 0
    return {"threads": 4, "phases": 3, "generations": barrier.generation,
            "all_phases_valid": True, "events": events}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("all", "semaphore", "condition", "barrier"), default="all")
    parser.add_argument("--items", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--out", default="results")
    args = parser.parse_args()
    if args.items <= 0 or args.repeat <= 0:
        parser.error("--items and --repeat must be positive")
    if not __debug__:
        parser.error("run without -O: invariant assertions must stay enabled")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=out / "violations.log", filemode="w", level=logging.ERROR,
                        format="%(asctime)s %(threadName)s %(message)s", force=True)
    report = {"runs": []}
    try:
        for _ in range(args.repeat):
            for kind in ("semaphore", "condition"):
                if args.mode in ("all", kind):
                    row = run_buffer(kind, args.items)
                    report["runs"].append(row)
                    print(f"{kind:10} items={row['items']} max={row['max_size']}/5 "
                          f"exactly_once=OK empty=OK violations=0 time={row['seconds']:.6f}s")
        if args.mode in ("all", "barrier"):
            report["barrier"] = run_barrier()
            print("\n".join(report["barrier"]["events"]))
            print("barrier: 4 threads, 3 phases, all invariants OK")
    except BaseException:
        logging.exception("VIOLATION: experiment failed")
        raise
    (out / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Saved {out / 'summary.json'}; violations: {out / 'violations.log'}")


if __name__ == "__main__":
    main()
