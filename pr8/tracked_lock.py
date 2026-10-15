"""Real threading.Lock objects with atomic wait-graph bookkeeping in a monitor."""
import threading
import time
from wait_graph import WaitGraph, find_cycle


class DeadlockDetected(RuntimeError):
    def __init__(self, cycle):
        self.cycle = cycle
        super().__init__("DEADLOCK: " + " -> ".join(cycle))


class LockTracker:
    def __init__(self):
        self.condition = threading.Condition(threading.Lock())
        self.state = WaitGraph()
        self.resources = set()
        self.events = []

    @staticmethod
    def identity():
        thread = threading.current_thread()
        return f"{thread.name}@{thread.ident}"

    def record(self, action, process, resource):
        self.events.append((action, process, resource))


class TrackedLock:
    def __init__(self, name, tracker):
        self.name = name
        self.tracker = tracker
        self.lock = threading.Lock()
        with tracker.condition:
            if name in tracker.resources:
                raise ValueError("lock names must be unique within a tracker")
            tracker.resources.add(name)

    def acquire(self, blocking=True, timeout=-1):
        if timeout < 0 and timeout != -1:
            raise ValueError("timeout must be nonnegative or -1")
        if not blocking and timeout != -1:
            raise ValueError("cannot specify timeout for nonblocking acquisition")
        tracker = self.tracker
        process = tracker.identity()
        deadline = None if timeout == -1 else time.monotonic() + timeout
        acquired = False
        with tracker.condition:
            try:
                while True:
                    if self.lock.acquire(blocking=False):
                        tracker.state.waiting.pop(process, None)
                        tracker.state.owners[self.name] = process
                        tracker.record("acquire", process, self.name)
                        acquired = True
                        return True
                    if not blocking:
                        return False
                    if process not in tracker.state.waiting:
                        tracker.state.waiting[process] = self.name
                        tracker.record("wait", process, self.name)
                    cycle = find_cycle(tracker.state.graph())
                    if cycle:
                        tracker.record("cycle", process, self.name)
                        raise DeadlockDetected(cycle)
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        return False
                    # Wait on the monitor, never block on the raw lock while holding it.
                    tracker.condition.wait(remaining)
            finally:
                if not acquired and process in tracker.state.waiting:
                    tracker.state.waiting.pop(process)
                    tracker.record("cancel_wait", process, self.name)

    def release(self):
        tracker = self.tracker
        process = tracker.identity()
        with tracker.condition:
            if tracker.state.owners.get(self.name) != process:
                raise RuntimeError("TrackedLock must be released by its owner")
            self.lock.release()
            del tracker.state.owners[self.name]
            tracker.record("release", process, self.name)
            tracker.condition.notify_all()

    def locked(self):
        with self.tracker.condition:
            return self.lock.locked()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
