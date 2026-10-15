from pathlib import Path

# H_i arrive every 2 ticks and need 2 ticks, keeping the high-priority queue busy.
lines = ["# PID arrival burst priority", "LOW 0 20 10"]
lines.extend(f"H{i + 1} {2 * i} 2 0" for i in range(100))
Path(__file__).with_name("set_aging.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
print("Created set_aging.txt: LOW and 100 high-priority processes")
