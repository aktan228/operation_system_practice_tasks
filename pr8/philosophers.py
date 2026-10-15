#!/usr/bin/env python3
"""Five philosophers: reproducible deadlock, total resource order, or N-1 seats."""
import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path


def simulate(mode, seconds=2):
    if mode not in ("naive", "ordered", "waiter") or seconds <= 0:
        raise ValueError("invalid mode or duration")
    count = 5
    forks = [threading.Lock() for _ in range(count)]
    seats = threading.Semaphore(count - 1)
    rendezvous = threading.Barrier(count)
    stop = threading.Event()
    audit = threading.Lock()
    meals = [0] * count
    max_wait = [0.0] * count
    errors = []
    start = threading.Event()

    def philosopher(index):
        try:
            start.wait()
            while not stop.is_set():
                time.sleep(0.001)  # thinking, outside the critical section
                begin = time.monotonic()
                left, right = index, (index + 1) % count
                if mode == "naive":
                    with forks[left]:
                        rendezvous.wait(timeout=5)
                        with forks[right]:  # all five deterministically block here
                            with audit:
                                meals[index] += 1
                else:
                    if mode == "waiter":
                        seats.acquire()
                    try:
                        first, second = sorted((left, right)) if mode == "ordered" else (left, right)
                        with forks[first]:
                            with forks[second]:
                                waited = time.monotonic() - begin
                                time.sleep(0.001)  # eating while holding both forks
                                with audit:
                                    meals[index] += 1
                                    max_wait[index] = max(max_wait[index], waited)
                    finally:
                        if mode == "waiter":
                            seats.release()
        except BaseException as error:
            with audit:
                errors.append(repr(error))
            stop.set()

    # Daemon threads only for the intentionally deadlocked experiment. The command
    # is run in a separate process so these threads disappear when that process exits.
    threads = [threading.Thread(target=philosopher, args=(i,), name=f"P{i}", daemon=mode == "naive")
               for i in range(count)]
    for thread in threads:
        thread.start()
    begin = time.monotonic()
    start.set()
    stop.wait(seconds)
    stop.set()
    if mode != "naive":
        deadline = time.monotonic() + 5
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
    with audit:
        row = {"mode": mode, "requested_seconds": seconds,
               "actual_seconds": round(time.monotonic() - begin, 6),
               "meals": meals.copy(), "max_wait_seconds": max_wait.copy(),
               "alive_at_end": sum(thread.is_alive() for thread in threads), "errors": errors.copy()}
    assert not row["errors"], row
    if mode == "naive":
        assert row["meals"] == [0] * count and row["alive_at_end"] == count, row
        assert all(fork.locked() for fork in forks)
        row["deadlock_reproduced"] = True
    else:
        assert row["alive_at_end"] == 0, row
        assert all(meals), "some philosopher made no progress during this run"
        assert not any(fork.locked() for fork in forks)
        row["deadlock_reproduced"] = False
    return row


def experiment(seconds=2):
    rows = []
    # Isolate each run, especially daemon threads in the intentionally broken case.
    for mode in ("naive", "ordered", "waiter"):
        completed = subprocess.run([sys.executable, str(Path(__file__).resolve()),
                                    "--mode", mode, "--seconds", str(seconds), "--json"],
                                   capture_output=True, text=True, timeout=seconds + 10, check=True)
        rows.append(json.loads(completed.stdout))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("all", "naive", "ordered", "waiter"), default="all")
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", default="results/philosophers.json")
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("duration must be positive")
    if args.mode == "all":
        rows = experiment(args.seconds)
    else:
        rows = [simulate(args.mode, args.seconds)]
    if args.json:
        print(json.dumps(rows[0] if args.mode != "all" else rows))
    else:
        print(f"{'Mode':<10} {'P0':>6} {'P1':>6} {'P2':>6} {'P3':>6} {'P4':>6} {'Alive':>7}")
        for row in rows:
            print(f"{row['mode']:<10}" + "".join(f"{x:>7}" for x in row["meals"]) + f"{row['alive_at_end']:>8}")
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
        print(f"Saved {path}; naive worker process exits with its intentionally blocked daemon threads.")


if __name__ == "__main__":
    main()
