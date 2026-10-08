from array import array
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from statistics import median
from time import perf_counter
from urllib.request import urlopen

import multiprocessing as mp
import os
import subprocess
import sys
import threading


ROOT = Path(__file__).resolve().parent

COUNT = 10_000_000
WORKERS = 4
REPEATS = 3
FILE_BYTES = 65_536

NUMBERS = array("q")
ITEMS = []


def generate():
    """Подготовка данных. Её время не входит в измерения."""
    print("Создаю numbers.txt: числа от 1 до 10 000 000...")

    with (ROOT / "numbers.txt").open("w", encoding="utf-8") as file:
        for number in range(1, COUNT + 1):
            file.write(f"{number}\n")

    web = ROOT / "web"
    web.mkdir(exist_ok=True)

    payload = bytes(range(256)) * 256
    urls = []

    for index in range(20):
        filename = f"file_{index:02d}.bin"
        (web / filename).write_bytes(payload)
        urls.append(f"http://127.0.0.1:8000/{filename}")

    (ROOT / "urls.txt").write_text(
        "\n".join(urls) + "\n",
        encoding="utf-8",
    )

    print(f"Создано чисел: {COUNT}")
    print(f"Создано URL: {len(urls)}")
    print(f"Размер каждого сетевого файла: {len(payload)} байт")


def sum_part(bounds):
    """Суммирует указанную часть массива Python-циклом."""
    start, stop = bounds
    data = NUMBERS
    total = 0

    for index in range(start, stop):
        total += data[index]

    return total


def make_parts():
    size = len(NUMBERS)
    return [
        (index * size // WORKERS, (index + 1) * size // WORKERS)
        for index in range(WORKERS)
    ]


def cpu_one():
    return sum_part((0, len(NUMBERS)))


def cpu_threads():
    parts = make_parts()
    results = [None] * WORKERS

    def worker(index, bounds):
        results[index] = sum_part(bounds)

    threads = [
        threading.Thread(target=worker, args=(index, bounds))
        for index, bounds in enumerate(parts)
    ]

    # Сначала запускаем ВСЕ потоки.
    for thread in threads:
        thread.start()

    # Потом ожидаем завершения ВСЕХ потоков.
    for thread in threads:
        thread.join()

    if any(value is None for value in results):
        raise RuntimeError("Один из потоков не вернул результат")

    return sum(results)


def cpu_processes():
    # Явно выбираем fork для Linux/WSL.
    # Дочерние процессы наследуют уже загруженный массив.
    context = mp.get_context("fork")

    with context.Pool(processes=WORKERS) as pool:
        results = pool.map(sum_part, make_parts())

    return sum(results)


def download(url):
    """Полностью загружает ответ в память."""
    with urlopen(url, timeout=15) as response:
        data = response.read()

    if len(data) != FILE_BYTES:
        raise ValueError(
            f"Неверный размер ответа: {url}, {len(data)} байт"
        )

    return len(data)


def network_one(urls):
    return sum(download(url) for url in urls)


def network_threads(urls):
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return sum(pool.map(download, urls))


def network_processes(urls):
    # Здесь данные передаются явно: каждому работнику нужен только URL.
    context = mp.get_context("spawn")

    with ProcessPoolExecutor(
        max_workers=WORKERS,
        mp_context=context,
    ) as pool:
        return sum(pool.map(download, urls))


def benchmark(title, methods, expected, filename):
    measurements = {name: [] for name in methods}
    names = list(methods)

    for repeat in range(REPEATS):
        print(f"\nПовтор {repeat + 1}/{REPEATS}")

        # Меняем порядок способов между повторами.
        order = names[repeat:] + names[:repeat]

        for name in order:
            started = perf_counter()
            result = methods[name]()
            elapsed = perf_counter() - started

            # Проверка и печать выполняются ПОСЛЕ остановки секундомера.
            if result != expected:
                raise ValueError(
                    f"{name}: результат {result}, ожидался {expected}"
                )

            measurements[name].append(elapsed)
            print(f"{name}: {elapsed:.6f} с; результат = {result}")

    cpus = int(subprocess.check_output(["nproc"], text=True).strip())
    baseline = median(measurements[names[0]])

    lines = [
        f"## {title}",
        "",
        f"Python: {sys.version.split()[0]}; "
        f"nproc: {cpus}; повторов каждого способа: {REPEATS}.",
        "",
        f"Проверенный результат каждого запуска: {expected}.",
        "",
        "| Способ | nproc | Повторов | Запуск 1, с | Запуск 2, с | "
        "Запуск 3, с | Медиана, с | Ускорение |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for name in names:
        times = measurements[name]
        middle = median(times)
        speedup = baseline / middle

        lines.append(
            f"| {name} | {cpus} | {REPEATS} | "
            f"{times[0]:.6f} | {times[1]:.6f} | {times[2]:.6f} | "
            f"{middle:.6f} | {speedup:.2f} |"
        )

    lines.append("")
    report = "\n".join(lines) + "\n"
    (ROOT / filename).write_text(report, encoding="utf-8")

    print("\n" + report)
    print(f"Таблица сохранена в {filename}")


def run_cpu():
    global NUMBERS

    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    print(f"Интерпретатор: {sys.implementation.name}")
    print(f"GIL включён: {gil_enabled}")

    if sys.implementation.name != "cpython" or not gil_enabled:
        raise RuntimeError(
            "Этот опыт рассчитан на обычный CPython с включённым GIL"
        )

    print("Читаю numbers.txt. Секундомер пока НЕ запущен.")

    with (ROOT / "numbers.txt").open(encoding="utf-8") as file:
        NUMBERS = array("q", (int(line) for line in file))

    if len(NUMBERS) != COUNT:
        raise ValueError(f"Ожидалось {COUNT} чисел, получено {len(NUMBERS)}")

    print(f"Загружено чисел: {len(NUMBERS)}")

    expected = COUNT * (COUNT + 1) // 2

    benchmark(
        "Суммирование 10 млн чисел",
        {
            "Один поток": cpu_one,
            "4 потока, threading": cpu_threads,
            "4 процесса, multiprocessing": cpu_processes,
        },
        expected,
        "cpu_results.md",
    )


def run_network():
    urls = [
        line.strip()
        for line in (ROOT / "urls.txt").read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]

    if len(urls) != 20:
        raise ValueError("В urls.txt должно быть 20 URL")

    print("Проверяю доступность сервера вне измерений...")
    download(urls[0])

    benchmark(
        "Загрузка 20 URL",
        {
            "Последовательно": lambda: network_one(urls),
            "4 потока, ThreadPoolExecutor": lambda: network_threads(urls),
            "4 процесса, ProcessPoolExecutor": lambda: network_processes(urls),
        },
        len(urls) * FILE_BYTES,
        "network_results.md",
    )


def change_items(label):
    ITEMS.append(label)
    print(
        f"Внутри {label}: PID={os.getpid()}, список={ITEMS}",
        flush=True,
    )


def run_memory():
    print(f"PID родителя: {os.getpid()}")

    ITEMS.clear()
    print(f"До потока: {ITEMS}")

    thread = threading.Thread(target=change_items, args=("thread",))
    thread.start()
    thread.join()

    print(f"После потока в родителе: {ITEMS}")

    ITEMS.clear()
    print(f"\nДо процесса: {ITEMS}")

    context = mp.get_context("fork")
    process = context.Process(target=change_items, args=("process",))
    process.start()
    process.join()

    if process.exitcode != 0:
        raise RuntimeError("Дочерний процесс завершился с ошибкой")

    print(f"После процесса в родителе: {ITEMS}")


if __name__ == "__main__":
    commands = {
        "generate": generate,
        "cpu": run_cpu,
        "net": run_network,
        "memory": run_memory,
    }

    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        print("Использование: python3 lab.py generate|cpu|net|memory")
        raise SystemExit(1)

    commands[sys.argv[1]]()
