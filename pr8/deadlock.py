#!/usr/bin/env python3
"""Default mode intentionally hangs; run under timeout. Other modes terminate."""
import argparse
import faulthandler
import threading
from tracked_lock import DeadlockDetected, LockTracker, TrackedLock


def run(mode, dump_after=1):
    tracker = LockTracker()
    if mode == "tracked":
        lock_a, lock_b = TrackedLock("A", tracker), TrackedLock("B", tracker)
    else:
        lock_a, lock_b = threading.Lock(), threading.Lock()
    gate = threading.Barrier(2)
    outcomes = []
    audit = threading.Lock()

    def worker(reverse):
        try:
            if mode == "ordered":
                gate.wait(timeout=5)  # rendezvous BEFORE holding any lock
                first, second = lock_a, lock_b
                with first:
                    with second:
                        print(threading.current_thread().name + ": finished", flush=True)
            else:
                first, second = (lock_b, lock_a) if reverse else (lock_a, lock_b)
                with first:
                    print(threading.current_thread().name + ": first lock acquired", flush=True)
                    gate.wait(timeout=5)  # both first locks are held before requesting second
                    with second:
                        print(threading.current_thread().name + ": finished", flush=True)
            with audit:
                outcomes.append("finished")
        except DeadlockDetected as error:
            print(str(error), flush=True)
            with audit:
                outcomes.append("detected")
        except BaseException as error:
            with audit:
                outcomes.append(f"ERROR: {error!r}")
            raise

    faulthandler.enable()
    if dump_after > 0:
        faulthandler.dump_traceback_later(dump_after, repeat=False)
    threads = [threading.Thread(target=worker, args=(False,), name="T1"),
               threading.Thread(target=worker, args=(True,), name="T2")]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()  # raw mode intentionally blocks here forever
    finally:
        faulthandler.cancel_dump_traceback_later()
    if mode == "tracked":
        assert sorted(outcomes) == ["detected", "finished"], outcomes
        assert not tracker.state.owners and not tracker.state.waiting
        print("RECOVERED: exception released held locks through with; graph empty", flush=True)
    elif mode == "ordered":
        assert outcomes == ["finished", "finished"]
        print("ORDERED: both threads finished", flush=True)
    return outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("raw", "ordered", "tracked"), default="raw")
    parser.add_argument("--dump-after", type=float, default=1)
    args = parser.parse_args()
    run(args.mode, args.dump_after)


if __name__ == "__main__":
    main()
