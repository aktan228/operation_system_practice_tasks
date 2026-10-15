#!/usr/bin/env python3
"""CPU scheduling simulator. All times are nonnegative integer ticks."""
import argparse
import csv
from collections import deque
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Process:
    pid: str
    arrival: int
    burst: int
    priority: int = 0


def read_processes(filename):
    processes = []
    for line_no, line in enumerate(Path(filename).read_text(encoding="utf-8").splitlines(), 1):
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        if len(fields) != 4:
            raise ValueError(f"{filename}:{line_no}: expected PID arrival burst priority")
        pid, arrival, burst, priority = fields
        processes.append(Process(pid, int(arrival), int(burst), int(priority)))
    validate(processes)
    return processes


def validate(processes):
    if not processes:
        raise ValueError("Process list is empty")
    if len({p.pid for p in processes}) != len(processes):
        raise ValueError("PIDs must be unique")
    for p in processes:
        if p.pid == "IDLE" or p.arrival < 0 or p.burst <= 0:
            raise ValueError("PID must not be IDLE; arrival >= 0; burst > 0")
        if not all(isinstance(v, int) for v in (p.arrival, p.burst, p.priority)):
            raise ValueError("Times and priorities must be integers")


def simulate(processes, algorithm, **params):
    """Return [(start, end, pid), ...]; intervals are half-open [start, end).

    Smaller priority number means higher priority. Ties use input order after
    arrival order. A running process wins preemptive ties to avoid useless
    switches. RR preserves quantum boundaries even for consecutive equal PIDs.
    """
    validate(processes)
    algorithm = algorithm.lower()
    if algorithm not in ("fcfs", "sjf", "srtf", "priority", "rr"):
        raise ValueError(f"Unknown algorithm: {algorithm}")
    quantum = params.get("quantum", 3)
    aging = params.get("aging", 0)
    if not isinstance(quantum, int) or quantum <= 0:
        raise ValueError("quantum must be a positive integer")
    if not isinstance(aging, int) or aging < 0:
        raise ValueError("aging must be a nonnegative integer")
    ordered = sorted(enumerate(processes), key=lambda item: (item[1].arrival, item[0]))
    sequence = {p.pid: n for n, p in enumerate(processes)}
    remaining = {p.pid: p.burst for p in processes}
    intervals = []
    t = 0

    def add(start, end, pid, merge=True):
        if merge and intervals and intervals[-1][2] == pid and intervals[-1][1] == start:
            intervals[-1] = (intervals[-1][0], end, pid)
        else:
            intervals.append((start, end, pid))

    if algorithm == "rr":
        ready = deque()
        next_index = 0
        while next_index < len(ordered) or ready:
            if not ready and next_index < len(ordered) and t < ordered[next_index][1].arrival:
                end = ordered[next_index][1].arrival
                add(t, end, "IDLE")
                t = end
            while next_index < len(ordered) and ordered[next_index][1].arrival <= t:
                ready.append(ordered[next_index][1])
                next_index += 1
            p = ready.popleft()
            duration = min(quantum, remaining[p.pid])
            add(t, t + duration, p.pid, merge=False)
            t += duration
            remaining[p.pid] -= duration
            # Include arrivals AT the end of the quantum BEFORE requeueing p.
            while next_index < len(ordered) and ordered[next_index][1].arrival <= t:
                ready.append(ordered[next_index][1])
                next_index += 1
            if remaining[p.pid]:
                ready.append(p)
        return intervals

    current = None
    while any(remaining.values()):
        ready = [p for p in processes if p.arrival <= t and remaining[p.pid] > 0]
        if not ready:
            end = min(p.arrival for p in processes if remaining[p.pid] > 0)
            add(t, end, "IDLE")
            t = end
            current = None
            continue
        if algorithm == "fcfs":
            chosen = min(ready, key=lambda p: (p.arrival, sequence[p.pid]))
        elif algorithm == "sjf":
            chosen = min(ready, key=lambda p: (p.burst, p.arrival, sequence[p.pid]))
        else:
            def rank(p):
                if algorithm == "srtf":
                    value = remaining[p.pid]
                else:
                    executed = p.burst - remaining[p.pid]
                    waited = t - p.arrival - executed
                    value = p.priority - (waited // aging if aging else 0)
                return (value, 0 if p.pid == current else 1, p.arrival, sequence[p.pid])
            chosen = min(ready, key=rank)
        duration = remaining[chosen.pid] if algorithm in ("fcfs", "sjf") else 1
        add(t, t + duration, chosen.pid)
        remaining[chosen.pid] -= duration
        t += duration
        current = chosen.pid if remaining[chosen.pid] else None
    return intervals


def metrics(processes, intervals):
    first, completion = {}, {}
    busy = 0
    switches = 0
    previous = None
    for start, end, pid in intervals:
        if pid == "IDLE":
            previous = None
            continue
        first.setdefault(pid, start)
        completion[pid] = end
        busy += end - start
        if previous is not None and previous != pid:
            switches += 1
        previous = pid
    rows = []
    for p in processes:
        turnaround = completion[p.pid] - p.arrival
        rows.append({"pid": p.pid, "arrival": p.arrival, "burst": p.burst,
                     "priority": p.priority, "first": first[p.pid],
                     "completion": completion[p.pid], "waiting": turnaround - p.burst,
alidate                     "turnaround": turnaround, "response": first[p.pid] - p.arrival})
    n = len(rows)
    return rows, {"waiting": sum(r["waiting"] for r in rows) / n,
                  "turnaround": sum(r["turnaround"] for r in rows) / n,
                  "response": sum(r["response"] for r in rows) / n,
                  "utilization": 100 * busy / intervals[-1][1], "switches": switches}


def print_run(processes, intervals):
    print("Gantt: " + " | ".join(f"{a}-{b}:{pid}" for a, b, pid in intervals))
    rows, avg = metrics(processes, intervals)
    print(f"{'PID':<8}{'Arrival':>8}{'Burst':>8}{'Prio':>8}{'First':>8}{'Finish':>8}{'Wait':>8}{'Turn':>8}{'Resp':>8}")
    for r in rows:
        print(f"{r['pid']:<8}" + "".join(f"{r[k]:>8}" for k in
              ("arrival", "burst", "priority", "first", "completion", "waiting", "turnaround", "response")))
    print(f"Average waiting: {avg['waiting']:.2f}")
    print(f"Average turnaround: {avg['turnaround']:.2f}")
    print(f"Average response: {avg['response']:.2f}")
    print(f"CPU utilization: {avg['utilization']:.2f}%")
    print(f"Context switches: {avg['switches']}")


def sweep(processes, folder):
    """Save data and a two-panel SVG graph without external dependencies."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    rows = []
    for q in range(1, 11):
        _, avg = metrics(processes, simulate(processes, "rr", quantum=q))
        rows.append((q, avg["waiting"], avg["switches"]))
    with (folder / "rr_quantum.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(("quantum", "average_waiting", "context_switches"))
        writer.writerows(rows)
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="960" height="700" viewBox="0 0 960 700">',
           '<rect width="960" height="700" fill="white"/>',
           '<g font-family="sans-serif" font-size="14" fill="#222">',
           '<text x="480" y="30" text-anchor="middle" font-size="22">Round Robin: quantum 1–10</text>']
    for panel, column, title, color in ((0, 1, "Average waiting (ticks)", "#2563eb"),
                                       (1, 2, "Context switches", "#dc2626")):
        top = 75 + panel * 310
        height = 220
        maximum = max(row[column] for row in rows) * 1.1 or 1
        svg.append(f'<text x="80" y="{top - 15}" font-size="18">{title}</text>')
        for i in range(5):
            value = maximum * i / 4
            y = top + height - height * i / 4
            svg.append(f'<path d="M80 {y} H890" stroke="#ddd"/>')
            svg.append(f'<text x="70" y="{y + 5}" text-anchor="end">{value:.1f}</text>')
        points = []
        for q, wait, switches in rows:
            value = wait if column == 1 else switches
            x, y = 80 + (q - 1) * 90, top + height - value / maximum * height
            points.append(f"{x},{y}")
            svg.append(f'<circle cx="{x}" cy="{y}" r="4" fill="{color}"/>')
            svg.append(f'<text x="{x}" y="{top + height + 22}" text-anchor="middle">{q}</text>')
        svg.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="2"/>')
        svg.append(f'<text x="480" y="{top + height + 47}" text-anchor="middle">Quantum</text>')
    svg.append('</g></svg>')
    (folder / "rr_quantum.svg").write_text("\n".join(svg), encoding="utf-8")
    print("quantum average_waiting context_switches")
    for q, wait, switches in rows:
        print(f"{q:>7} {wait:>15.2f} {switches:>16}")
    print(f"Saved: {folder / 'rr_quantum.csv'} and {folder / 'rr_quantum.svg'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", help="PID arrival burst priority input file")
    parser.add_argument("--algo", choices=("fcfs", "sjf", "srtf", "priority", "rr"), default="fcfs")
    parser.add_argument("--quantum", type=int, default=3)
    parser.add_argument("--aging", type=int, default=0, help="priority improves by 1 per N waiting ticks; 0 disables aging")
    parser.add_argument("--compare", action="store_true", help="compare all five algorithms")
    parser.add_argument("--sweep", action="store_true", help="RR quantum 1..10 experiment")
    parser.add_argument("--out", default="results", help="sweep output directory")
    args = parser.parse_args()
    try:
        processes = read_processes(args.file)
        if args.sweep:
            sweep(processes, args.out)
        elif args.compare:
            print(f"{'Algorithm':<12}{'Wait':>10}{'Turn':>10}{'Response':>10}{'CPU %':>10}{'Switches':>10}")
            for algorithm in ("fcfs", "sjf", "srtf", "priority", "rr"):
                intervals = simulate(processes, algorithm, quantum=args.quantum, aging=args.aging)
                _, avg = metrics(processes, intervals)
                print(f"{algorithm:<12}{avg['waiting']:>10.2f}{avg['turnaround']:>10.2f}"
                      f"{avg['response']:>10.2f}{avg['utilization']:>10.2f}{avg['switches']:>10}")
        else:
            print_run(processes, simulate(processes, args.algo, quantum=args.quantum, aging=args.aging))
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
