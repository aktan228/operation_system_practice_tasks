#!/usr/bin/env python3
"""Correct counterparts: predicate loop and atomic check/wait under a mutex."""
import argparse
import threading
import time


def if_demo():
    condition = threading.Condition()
    buffer = []
    ready = [threading.Event(), threading.Event()]
    first_consumed = threading.Event()
    waiting_again = threading.Event()
    consumed = []
    errors = []

    def consumer(index):
        try:
            with condition:
                deadline = time.monotonic() + 5
                waits = 0
                while not buffer:  # FIX: recheck after every wakeup
                    ready[index].set()
                    if waits:
                        waiting_again.set()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("no item arrived")
                    condition.wait(remaining)
                    waits += 1
                consumed.append(buffer.pop(0))
                first_consumed.set()
        except BaseException as error:
            with condition:
                errors.append(repr(error))

    threads = [threading.Thread(target=consumer, args=(i,), daemon=True) for i in range(2)]
    for thread in threads:
        thread.start()
    for event in ready:
        assert event.wait(5)
    with condition:
        buffer.append(42)
        condition.notify_all()
    assert first_consumed.wait(5)
    assert waiting_again.wait(5), "second consumer did not recheck the predicate"
    # Only supply the second item after the other consumer actually waits again.
    with condition:
        buffer.append(43)
        condition.notify_all()
    for thread in threads:
        thread.join(6)
        assert not thread.is_alive()
    assert not errors, errors
    assert sorted(consumed) == [42, 43] and not buffer
    return {"case": "if", "consumed": consumed, "predicate_rechecked": True,
            "violations": []}


def lost_demo():
    condition = threading.Condition()
    buffer = []
    checked = threading.Event()
    consumed = []
    errors = []

    def consumer():
        try:
            with condition:
                deadline = time.monotonic() + 5
                while not buffer:
                    checked.set()
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("notification lost")
                    condition.wait(remaining)
                consumed.append(buffer.pop(0))
        except BaseException as error:
            with condition:
                errors.append(repr(error))

    thread = threading.Thread(target=consumer, daemon=True)
    thread.start()
    assert checked.wait(5)
    # This mutex becomes available only when wait atomically releases it.
    with condition:
        buffer.append(42)
        condition.notify()
    thread.join(6)
    assert not thread.is_alive()
    assert not errors, errors
    assert consumed == [42] and not buffer
    return {"case": "lost", "consumed": consumed, "violations": []}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("if", "lost", "all"), default="all")
    args = parser.parse_args()
    for name, demo in (("if", if_demo), ("lost", lost_demo)):
        if args.case in (name, "all"):
            print(demo())
    print("Both examples completed without violations.")


if __name__ == "__main__":
    main()
