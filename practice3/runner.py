#!/usr/bin/env python3

import os
import signal
import sys
import time


def main():
    if len(sys.argv) < 3:
        print(
            f"Использование: {sys.argv[0]} N команда [аргументы...]",
            file=sys.stderr,
        )
        return 2

    try:
        count = int(sys.argv[1])
        if count <= 0:
            raise ValueError
    except ValueError:
        print("N должно быть положительным целым числом.", file=sys.stderr)
        return 2

    command = sys.argv[2:]
    running = {}
    failed = False

    def terminate_children():
        for child_pid in list(running):
            try:
                os.kill(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def handle_interrupt(signum, frame):
        print("\nПолучен сигнал: завершаем дочерние процессы.", flush=True)
        terminate_children()

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    try:
        for number in range(1, count + 1):
            sys.stdout.flush()
            sys.stderr.flush()

            started = time.monotonic()

            try:
                pid = os.fork()
            except OSError as error:
                print(f"Ошибка fork: {error}", file=sys.stderr)
                failed = True
                terminate_children()
                break

            if pid == 0:
                # В ребёнке восстанавливаем обычную обработку сигналов.
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)

                try:
                    os.execvp(command[0], command)
                except OSError as error:
                    print(
                        f"PID={os.getpid()}: ошибка exec: {error}",
                        file=sys.stderr,
                        flush=True,
                    )
                    os._exit(127)

            running[pid] = (number, started)
            print(f"Запущен ребёнок №{number}, PID={pid}", flush=True)

        while running:
            try:
                pid, status = os.wait()
            except InterruptedError:
                continue

            finished = time.monotonic()
            number, started = running.pop(pid)
            return_code = os.waitstatus_to_exitcode(status)

            if return_code < 0:
                result = f"завершён сигналом {-return_code}"
            else:
                result = f"код возврата={return_code}"

            print(
                f"Ребёнок №{number}, PID={pid}: "
                f"{result}, время={finished - started:.3f} с",
                flush=True,
            )

    finally:
        # При неожиданной ошибке убираем оставшихся прямых детей.
        terminate_children()

        for pid in list(running):
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
