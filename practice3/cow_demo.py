#!/usr/bin/env python3

import os
import sys
from pathlib import Path


def read_kb_fields(path):
    result = {}

    for line in Path(path).read_text().splitlines():
        key, separator, value = line.partition(":")
        parts = value.split()

        if separator and parts and parts[0].isdigit():
            result[key] = int(parts[0])

    return result


def show_memory(label):
    status = read_kb_fields("/proc/self/status")

    try:
        rollup = read_kb_fields("/proc/self/smaps_rollup")
    except OSError:
        rollup = {}

    print(
        f"{label}: PID={os.getpid()}, "
        f"RSS={status.get('VmRSS', 0)} kB, "
        f"PSS={rollup.get('Pss', 'недоступно')} kB, "
        f"Private_Dirty={rollup.get('Private_Dirty', 'недоступно')} kB",
        flush=True,
    )


def main():
    try:
        size = int(sys.argv[1]) if len(sys.argv) > 1 else 4_000_000
        if size <= 0:
            raise ValueError
    except ValueError:
        print("Размер списка должен быть положительным числом.", file=sys.stderr)
        return 2

    # Все элементы ссылаются на один объект None.
    # Большую память занимает массив указателей самого списка.
    data = [None] * size

    print(f"Создан список из {size} элементов.", flush=True)
    show_memory("Родитель до fork")

    sys.stdout.flush()
    pid = os.fork()

    if pid == 0:
        try:
            show_memory("Ребёнок до изменения")

            # Перезаписываем массив указателей без создания
            # миллионов новых объектов.
            for index in range(size):
                data[index] = True

            show_memory("Ребёнок после изменения")
            print(f"Первый элемент у ребёнка: {data[0]}", flush=True)
        except BaseException as error:
            print(f"Ошибка ребёнка: {error}", file=sys.stderr, flush=True)
            os._exit(1)

        os._exit(0)

    _, status = os.waitpid(pid, 0)

    print(f"Первый элемент у родителя: {data[0]}", flush=True)
    show_memory("Родитель после завершения ребёнка")

    return os.waitstatus_to_exitcode(status)


if __name__ == "__main__":
    sys.exit(main())
