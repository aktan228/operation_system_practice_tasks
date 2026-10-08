#!/usr/bin/env python3

import os
import time


def main():
    pid = os.fork()

    if pid == 0:
        print(
            f"Ребёнок: PID={os.getpid()}, PPID={os.getppid()}. "
            "Сейчас завершаюсь.",
            flush=True,
        )
        os._exit(0)

    print(
        f"Родитель: PID={os.getpid()}, ребёнок PID={pid}.",
        flush=True,
    )
    print(
        f"В другом терминале: ps -o pid,ppid,stat,cmd -p {pid}",
        flush=True,
    )
    print("30 секунд не вызываю wait: ребёнок станет зомби.", flush=True)

    try:
        time.sleep(30)
    finally:
        waited_pid, status = os.waitpid(pid, 0)
        code = os.waitstatus_to_exitcode(status)
        print(
            f"Забрал статус PID={waited_pid}, код={code}. Зомби убран.",
            flush=True,
        )


if __name__ == "__main__":
    main()
