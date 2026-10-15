#!/usr/bin/env python3
"""Save all variant-B experiments, including bounded observation of a real hang."""
import json
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(args, stdin="", timeout=15):
    result = subprocess.run([sys.executable, *args], input=stdin, cwd=ROOT,
                            capture_output=True, text=True, timeout=timeout, check=True)
    return result.stdout + result.stderr


def main():
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    reports = {
        "bank1.txt": run(["banker.py", "bank1.txt"]),
        "bank2.txt": run(["banker.py", "bank2.txt"]),
        "bank_request_granted.txt": run(["banker.py", "bank1.txt"], "P1 1 0 2\nshow\nsafe\n"),
        "bank_request_rollback.txt": run(["banker.py", "bank1.txt"], "show\nP4 3 3 0\nshow\n"),
        "graph_deadlock.txt": run(["wait_graph.py", "events_deadlock.txt"]),
        "graph_safe.txt": run(["wait_graph.py", "events_safe.txt"]),
        "tracked.txt": run(["deadlock.py", "--mode", "tracked"]),
        "ordered.txt": run(["deadlock.py", "--mode", "ordered"]),
    }
    process = subprocess.Popen([sys.executable, str(ROOT / "deadlock.py"), "--dump-after", "0.3"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    hung = False
    try:
        stdout, stderr = process.communicate(timeout=1.2)
    except subprocess.TimeoutExpired:
        hung = True
        process.kill()
        stdout, stderr = process.communicate()
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert hung and stdout.count("first lock acquired") == 2 and stderr.count("in worker") >= 2
    reports["raw_deadlock.txt"] = stdout + stderr + "\nOBSERVED: still blocked at timeout; wrapper killed the process.\n"
    reports["philosophers.txt"] = run(["philosophers.py", "--seconds", "2"], timeout=20)
    for name, content in reports.items():
        (out / name).write_text(content, encoding="utf-8")
    rows = json.loads((out / "philosophers.json").read_text())
    lines = ["# Результаты: обедающие философы", "",
             "| Вариант | P0 | P1 | P2 | P3 | P4 | Живых потоков после наблюдения |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        lines.append("| " + " | ".join(map(str, [row["mode"], *row["meals"], row["alive_at_end"]])) + " |")
    lines += ["", "Интервал наблюдения каждого варианта — 2 секунды; числа зависят от планировщика.",
              "В naive все потоки удерживают левую вилку и ждут правую; отдельный процесс",
              "демонстрации завершается после снимка, унося свои заблокированные daemon-потоки.",
              "В ordered и waiter потоки завершаются и освобождают все вилки.",
              "Положительные счётчики не являются доказательством отсутствия голодания при любом расписании.", ""]
    (out / "table.md").write_text("\n".join(lines), encoding="utf-8")
    (out / "environment.txt").write_text(f"{platform.platform()}\nPython {platform.python_version()}\n", encoding="utf-8")
    print(reports["bank1.txt"])
    print(reports["bank2.txt"])
    print(reports["philosophers.txt"])
    print("All experiments saved in results/; intentional deadlock process was terminated.")


if __name__ == "__main__":
    main()
