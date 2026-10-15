import json
import os
import signal
import time
from pathlib import Path

MAX_MESSAGE = 4096


def encode(value):
    data = json.dumps(value, ensure_ascii=True, allow_nan=False).encode("ascii") + b"\n"
    if len(data) > MAX_MESSAGE:
        raise ValueError("message too large")
    return data


def default_directory(kind):
    return Path(f"/tmp/pr7-{kind}-{os.getuid()}")


def install_stop_handler():
    state = {"stop": False}

    def stop(signum, frame):
        state["stop"] = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    return state


def remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("IPC operation timed out; check that the server is running")
    return value
