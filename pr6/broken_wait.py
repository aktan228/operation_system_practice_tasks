#!/usr/bin/env python3
"""Intentionally incorrect condition-variable examples. Do not use in real code."""
import argparse
import threading


def if_demo():
    condition = threading.Condition()
    buffer = []
    ready = [threading.Event(), threading.Event()]
    consumed, violations = [], []

    def consumer(index):
        with condition:
            if not buffer:  # BUG: no predicate recheck after waking
                ready[index].set()
                condition.wait(timeout=5)
            try:
                consumed.append(buffer.pop(0))
            except IndexError:
                violations.append("UNDERFLOW: awakened consumer found an empty buffer")

    threads = [threading.Thread(target=consumer, args=(i,), daemon=True) for i in range(2)]
    for thread in threads:
        thread.start()
    for event in ready:
        assert event.wait(5), "consumer did not start"
    with condition:
        # Both consumers have released the mutex in wait().
        buffer.append(42)
        condition.notify_all()  # one element, two awakened consumers
    for thread in threads:
        thread.join(6)
        assert not thread.is_alive(), "demo timed out"
    assert consumed == [42] and len(violations) == 1
    return {"case": "if", "consumed": consumed, "violations": violations}


def lost_demo():
    condition = threading.Condition()
    buffer = []
    checked = threading.Event()
    notified = threading.Event()
    result = {}

    def consumer():
        with condition:
            empty = not buffer
        # BUG: gap between checking the predicate and entering wait().
        checked.set()
        assert notified.wait(5)
        if empty:
            with condition:
                result["received_notification"] = condition.wait(timeout=0.2)
                result["item_present_after_timeout"] = bool(buffer)

    thread = threading.Thread(target=consumer, daemon=True)
    thread.start()
    assert checked.wait(5)
    with condition:
        buffer.append(42)
        condition.notify()  # consumer is not waiting yet: notification is lost
    notified.set()
    thread.join(6)
    assert not thread.is_alive()
    assert result == {"received_notification": False, "item_present_after_timeout": True}
    return {"case": "lost", **result,
            "violations": ["LOST WAKEUP: item exists, but consumer waited until timeout"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("if", "lost", "all"), default="all")
    args = parser.parse_args()
    for name, demo in (("if", if_demo), ("lost", lost_demo)):
        if args.case in (name, "all"):
            print(demo())
    print("Expected bugs reproduced. These are intentional failures of the algorithm.")


if __name__ == "__main__":
    main()
