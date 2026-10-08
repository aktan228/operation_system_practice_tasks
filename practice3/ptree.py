#!/usr/bin/env python3

import os
from collections import defaultdict
from pathlib import Path


def read_process(pid, current_uid):
    try:
        text = Path(f"/proc/{pid}/status").read_text()
    except (OSError, UnicodeError):
        # Процесс мог завершиться или оказаться недоступным.
        return None

    fields = {}

    for line in text.splitlines():
        key, separator, value = line.partition(":")
        if separator:
            fields[key] = value.strip()

    try:
        # Первое число в Uid — реальный UID.
        uid = int(fields["Uid"].split()[0])

        if uid != current_uid:
            return None

        return {
            "pid": pid,
            "ppid": int(fields["PPid"]),
            "name": fields["Name"],
            "state": fields["State"].split()[0],
            "rss_kb": int(fields.get("VmRSS", "0 kB").split()[0]),
        }
    except (KeyError, ValueError, IndexError):
        return None


def main():
    current_uid = os.getuid()
    processes = {}

    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue

        pid = int(entry.name)
        process = read_process(pid, current_uid)

        if process is not None:
            processes[pid] = process

    children = defaultdict(list)
    roots = []

    for pid, process in processes.items():
        ppid = process["ppid"]

        if ppid in processes:
            children[ppid].append(pid)
        else:
            roots.append(pid)

    def print_tree(pid, depth=0):
        process = processes[pid]
        indent = "  " * depth

        print(
            f"{indent}PID={pid} "
            f"PPID={process['ppid']} "
            f"STATE={process['state']} "
            f"RSS={process['rss_kb']} kB "
            f"NAME={process['name']}"
        )

        for child_pid in sorted(children[pid]):
            print_tree(child_pid, depth + 1)

    print(f"Процессы пользователя UID={current_uid}")

    for pid in sorted(roots):
        print_tree(pid)


if __name__ == "__main__":
    main()
