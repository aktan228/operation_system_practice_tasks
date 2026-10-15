#!/usr/bin/env python3
"""Banker's safety algorithm and transactional resource requests from stdin."""
import argparse
import copy
import json
import sys
from pathlib import Path


class Banker:
    def __init__(self, state):
        self.state = copy.deepcopy(state)
        self.resources = self.state["resources"]
        self.available = self.state["available"]
        self.processes = self.state["processes"]
        self.pids = [row["pid"] for row in self.processes]
        self.order = self.state.get("scan_order", self.pids)
        if not self.resources or len(set(self.resources)) != len(self.resources):
            raise ValueError("resources must be nonempty and unique")
        if not self.pids or len(set(self.pids)) != len(self.pids):
            raise ValueError("PIDs must be nonempty and unique")
        if len(self.order) != len(self.pids) or set(self.order) != set(self.pids):
            raise ValueError("scan_order must be a permutation of PIDs")
        self.by_pid = {row["pid"]: row for row in self.processes}
        self.vector(self.available)
        for row in self.processes:
            self.vector(row["allocation"])
            self.vector(row["max"])
            if any(a > m for a, m in zip(row["allocation"], row["max"])):
                raise ValueError("Allocation exceeds Max")
        self.total = [a + sum(row["allocation"][i] for row in self.processes)
                      for i, a in enumerate(self.available)]
        self.check()

    def vector(self, values):
        if len(values) != len(self.resources) or any(type(x) is not int or x < 0 for x in values):
            raise ValueError("vector must have one nonnegative integer per resource")

    def need(self, pid):
        row = self.by_pid[pid]
        return [m - a for m, a in zip(row["max"], row["allocation"])]

    def check(self):
        assert all(x >= 0 for x in self.available)
        for row in self.processes:
            assert all(0 <= a <= m for a, m in zip(row["allocation"], row["max"]))
        assert self.total == [a + sum(row["allocation"][i] for row in self.processes)
                              for i, a in enumerate(self.available)], "resource conservation failed"

    def safety(self, order=None):
        order = self.order if order is None else list(order)
        if len(order) != len(self.pids) or set(order) != set(self.pids):
            raise ValueError("order must include each PID exactly once")
        work = self.available.copy()
        finished = set()
        sequence, trace = [], []
        while len(finished) < len(self.pids):
            progress = False
            for pid in order:
                if pid not in finished and all(n <= w for n, w in zip(self.need(pid), work)):
                    before = work.copy()
                    work = [w + a for w, a in zip(work, self.by_pid[pid]["allocation"])]
                    finished.add(pid)
                    sequence.append(pid)
                    trace.append({"pid": pid, "need": self.need(pid), "work_before": before,
                                  "work_after": work.copy()})
                    progress = True
            if not progress:
                break
        return {"safe": len(finished) == len(self.pids), "sequence": sequence,
                "unfinished": [pid for pid in self.pids if pid not in finished], "trace": trace}

    def request(self, pid, vector):
        if pid not in self.by_pid:
            raise ValueError(f"unknown PID: {pid}")
        self.vector(vector)
        if any(r > n for r, n in zip(vector, self.need(pid))):
            return {"status": "REJECTED", "reason": "request exceeds Need"}
        if any(r > a for r, a in zip(vector, self.available)):
            return {"status": "WAIT", "reason": "not enough Available; state unchanged"}
        row = self.by_pid[pid]
        old_available = self.available.copy()
        old_allocation = row["allocation"].copy()
        granted = False
        try:
            self.available[:] = [a - r for a, r in zip(self.available, vector)]
            row["allocation"][:] = [a + r for a, r in zip(row["allocation"], vector)]
            self.check()
            result = self.safety()
            if not result["safe"]:
                return {"status": "DENIED_UNSAFE", "reason": "tentative state unsafe; rolled back"}
            granted = True
            return {"status": "GRANTED", "sequence": result["sequence"]}
        finally:
            if not granted:
                self.available[:] = old_available
                row["allocation"][:] = old_allocation
            self.check()

    def show(self):
        print("Available:", self.available)
        print("PID  Allocation  Max        Need")
        for row in self.processes:
            print(f"{row['pid']:<4} {str(row['allocation']):<11} {str(row['max']):<10} {self.need(row['pid'])}")


def load(path):
    return Banker(json.loads(Path(path).read_text(encoding="utf-8")))


def print_safety(bank):
    result = bank.safety()
    print("SAFE: " + " -> ".join(result["sequence"]) if result["safe"] else "UNSAFE")
    for row in result["trace"]:
        print(f"  {row['pid']}: Need={row['need']} Work={row['work_before']} -> {row['work_after']}")
    if result["unfinished"]:
        print("Cannot finish:", ", ".join(result["unfinished"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file")
    args = parser.parse_args()
    bank = load(args.file)
    bank.show()
    print_safety(bank)
    if sys.stdin.isatty():
        print("Commands: P1 1 0 2 | show | safe | quit; Ctrl+D ends input")
    for line in sys.stdin:
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        if fields == ["quit"]:
            break
        if fields == ["show"]:
            bank.show()
        elif fields == ["safe"]:
            print_safety(bank)
        else:
            try:
                result = bank.request(fields[0], [int(x) for x in fields[1:]])
                print(json.dumps({"pid": fields[0], "request": fields[1:], **result}))
            except ValueError as error:
                print("ERROR:", error)


if __name__ == "__main__":
    main()
