#!/usr/bin/env python3

import os
import time


def main():
    read_fd, write_fd = os.pipe()
    parent_pid = os.fork()

    if parent_pid == 0:
        os.close(read_fd)

        child_pid = os.fork()

        if child_pid == 0:
            original_parent = os.getppid()

            print(
                f"Ребёнок: PID={os.getpid()}, "
                f"исходный PPID={original_parent}",
                flush=True,
            )

            # Ждём, пока ядро назначит нового родителя.
            while os.getppid() == original_parent:
                time.sleep(0.05)

            print(
                f"Я сирота: PID={os.getpid()}, "
                f"новый PPID={os.getppid()}",
                flush=True,
            )
            print(
                f"Проверка: ps -o pid,ppid,stat,cmd "
                f"-p {os.getpid()},{os.getppid()}",
                flush=True,
            )

            time.sleep(30)

            print("Ребёнок завершает работу.", flush=True)
            os.close(write_fd)
            os._exit(0)

        print(
            f"Родитель: PID={os.getpid()}, ребёнок PID={child_pid}. "
            "Завершусь через 2 секунды.",
            flush=True,
        )
        time.sleep(2)
        os.close(write_fd)
        os._exit(0)

    os.close(write_fd)

    try:
        os.waitpid(parent_pid, 0)

        # EOF появится, когда ребёнок закроет канал при завершении.
        while os.read(read_fd, 1024):
            pass
    finally:
        os.close(read_fd)

    print("Наблюдатель: демонстрация закончилась.", flush=True)


if __name__ == "__main__":
    main()
