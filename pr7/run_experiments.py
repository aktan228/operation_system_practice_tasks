#!/usr/bin/env python3
"""Reproducible service and memory experiments; creates results/table.md."""
import argparse
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import fifo_service
import unix_service
from bench_memory import benchmark

ROOT = Path(__file__).resolve().parent


@contextmanager
def server(kind, directory):
    command = [sys.executable, str(ROOT / f"{kind}_service.py"), "server", "--directory", str(directory)]
    with tempfile.TemporaryFile(mode="w+t") as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
        try:
            ready_file = directory / "ready"
            deadline = time.monotonic() + 10
            while not ready_file.exists():
                if process.poll() is not None or time.monotonic() > deadline:
                    log.seek(0)
                    raise RuntimeError("server did not start: " + log.read())
                time.sleep(0.01)
            yield process
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise AssertionError("server did not shut down gracefully")
            log.seek(0)
            output = log.read()
            if process.returncode != 0:
                raise AssertionError(f"server exit {process.returncode}: {output}")
            assert not directory.exists(), f"IPC resources remain: {directory}"


def service_experiments(samples=10):
    report = {}
    with tempfile.TemporaryDirectory(prefix="pr7-check-") as temp:
        temp = Path(temp)
        with server("fifo", temp / "fifo"):
            rows = []
            for _ in range(samples):
                completed = subprocess.run([sys.executable, str(ROOT / "fifo_service.py"),
                                            "client", "(12 + 8) * 3", "--directory", str(temp / "fifo")],
                                           capture_output=True, text=True, timeout=20, check=True)
                rows.append(json.loads(completed.stdout))
            assert all(row["response"]["result"] == 60 for row in rows)
            assert len({row["client_pid"] for row in rows}) == samples
            error = fifo_service.request("1 / 0", temp / "fifo")
            assert error["response"]["ok"] is False
            report["fifo"] = rows
        with server("unix", temp / "unix"):
            # These client processes are all launched before waiting for any result.
            clients = [subprocess.Popen([sys.executable, str(ROOT / "unix_service.py"),
                                         "client", f"{i} * {i} + 1", "--directory", str(temp / "unix")],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                       for i in range(samples)]
            try:
                rows = []
                for i, client in enumerate(clients):
                    stdout, stderr = client.communicate(timeout=30)
                    assert client.returncode == 0, stderr
                    row = json.loads(stdout)
                    assert row["response"]["result"] == i * i + 1
                    rows.append(row)
                assert len({row["response"]["worker_pid"] for row in rows}) == samples
                assert len({row["client_pid"] for row in rows}) == samples
                report["unix"] = rows
            finally:
                for client in clients:
                    if client.poll() is None:
                        client.kill()
                    client.communicate()
    report["cleanup_ok"] = True
    return report


def write_table(services, memory, out):
    lines = ["# Измерения IPC", "", f"Среда: {platform.platform()}, Python {platform.python_version()}.", "",
             "| Механизм | Синхронизация программистом | Объём | Медианное время, с | Что измерено |",
             "|---|---|---:|---:|---|"]
    for kind, label, sync in (("fifo", "Два FIFO", "Да: flock на полный запрос/ответ"),
                              ("unix", "Unix socket", "Границы сообщений; отдельное соединение на клиента")):
        rows = services[kind]
        sizes = [row["wire_bytes"] for row in rows]
        lines.append(f"| {label} | {sync} | {min(sizes)}–{max(sizes)} байт запрос+ответ | "
                     f"{statistics.median(row['seconds'] for row in rows):.6f} | "
                     f"{'Последовательный' if kind == 'fifo' else 'Конкурентный'} RPC, {len(rows)} клиентов/запросов |")
    for row in memory["medians"]:
        sync = "Да: Event публикации + подтверждение чтения" if row["mechanism"] == "shared_memory" else "Протокол Pipe + подтверждение; один писатель"
        lines.append(f"| {row['mechanism']} | {sync} | {row['bytes']:,} байт | "
                     f"{row['handoff_seconds']:.6f} | Доставка массива, без SHA256 |")
    lines += ["", "Массив: uint32, значения 0..N−1, 4 байта на число.", "",
              "## Проверка массива целиком", "",
              "| Механизм | Доставка + чтение всех байт для SHA256, с |", "|---|---:|"]
    for row in memory["medians"]:
        lines.append(f"| {row['mechanism']} | {row['verified_seconds']:.6f} |")
    lines += ["", "Маленький RPC и передача массива имеют разные объёмы и границы измерения;",
              "сравнивать абсолютные времена всех четырёх строк как рейтинг скорости нельзя.",
              "Объём RPC включает JSON, ID, перевод строки и результат; у сокета также PID обработчика.",
              "Конкурентные сокеты измерены под нагрузкой, FIFO последовательно: это разные режимы.",
              "RPC включает ожидание ответа и для сокетов запуск отдельного серверного процесса (spawn).",
              "Запуск процессов, создание сегмента и генерация исходного массива исключены из времён массива.",
              "Для shared_memory включена копия исходного массива в сегмент и Event/подтверждение;",
              "receiver подключается до старта замера. Для Pipe включены send_bytes/recv_bytes и подтверждение.",
              "Передача имени сегмента не считается передачей всего массива; отдельно приведено время",
              "с полным чтением данных для одинаковой SHA256-проверки в обоих механизмах.",
              "SHA256 совпал для каждого прогона. Ресурсы после штатного завершения удалены.", ""]
    (out / "table.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10_000_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--out", default="results")
    args = parser.parse_args()
    if args.clients <= 0:
        parser.error("--clients must be positive")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    services = service_experiments(args.clients)
    memory = benchmark(args.count, args.repeats)
    (out / "services.json").write_text(json.dumps(services, indent=2) + "\n", encoding="utf-8")
    (out / "memory.json").write_text(json.dumps(memory, indent=2) + "\n", encoding="utf-8")
    write_table(services, memory, out)
    print((out / "table.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
