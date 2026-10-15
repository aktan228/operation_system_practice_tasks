#!/usr/bin/env python3
"""Two FIFO files. A client lock serializes the complete request/reply exchange."""
import argparse
import fcntl
import json
import os
import select
import sys
import time
import uuid
from pathlib import Path

from calculator import response
from ipc_common import MAX_MESSAGE, default_directory, encode, install_stop_handler, remaining


def read_reply(fd, deadline, data):
    while b"\n" not in data:
        select.select([fd], [], [], min(0.1, remaining(deadline)))
        try:
            part = os.read(fd, MAX_MESSAGE)
        except BlockingIOError:
            continue
        if not part:
            time.sleep(0.005)  # writer has not opened the FIFO yet
            continue
        data.extend(part)
        if len(data) > MAX_MESSAGE:
            raise ValueError("reply too large")
    line, _, rest = data.partition(b"\n")
    data[:] = rest
    return json.loads(line)


def request(expression, directory=None, timeout=10):
    directory = Path(directory) if directory else default_directory("fifo")
    deadline = time.monotonic() + timeout
    message = {"id": uuid.uuid4().hex, "expression": expression}
    payload = encode(message)
    begin = time.perf_counter()
    # The server creates this lock; clients never recreate a stopped server's files.
    with (directory / "client.lock").open("r+b") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.01, remaining(deadline)))
        read_fd = write_fd = None
        try:
            read_fd = os.open(directory / "reply.fifo", os.O_RDONLY | os.O_NONBLOCK)
            write_fd = os.open(directory / "request.fifo", os.O_WRONLY | os.O_NONBLOCK)
            assert len(payload) <= os.fpathconf(write_fd, "PC_PIPE_BUF")
            assert os.write(write_fd, payload) == len(payload)
            # Ignore a stale response from an earlier timed-out client.
            pending = bytearray()
            while True:
                answer = read_reply(read_fd, deadline, pending)
                if answer.get("id") == message["id"]:
                    break
        finally:
            if write_fd is not None:
                os.close(write_fd)
            if read_fd is not None:
                os.close(read_fd)
            fcntl.flock(lock, fcntl.LOCK_UN)
    return {"client_pid": os.getpid(), "expression": expression, "response": answer,
            "seconds": time.perf_counter() - begin,
            "wire_bytes": len(payload) + len(encode(answer))}


def serve(directory=None):
    directory = Path(directory) if directory else default_directory("fifo")
    state = install_stop_handler()
    directory.mkdir(mode=0o700)  # refuse to overwrite an existing server or its files
    created = []
    fd = None
    try:
        for name in ("request.fifo", "reply.fifo"):
            path = directory / name
            os.mkfifo(path, mode=0o600)
            created.append(path)
        lock = directory / "client.lock"
        lock.touch(mode=0o600)
        created.append(lock)
        # Linux O_RDWR keeps the request FIFO open without requiring a client at startup.
        fd = os.open(directory / "request.fifo", os.O_RDWR | os.O_NONBLOCK)
        ready = directory / "ready"
        ready.touch(mode=0o600)
        created.append(ready)
        print(f"READY FIFO directory={directory} pid={os.getpid()}", flush=True)
        pending = bytearray()
        while not state["stop"]:
            if not select.select([fd], [], [], 0.1)[0]:
                continue
            pending.extend(os.read(fd, MAX_MESSAGE))
            if len(pending) > MAX_MESSAGE and b"\n" not in pending:
                pending.clear()
                print("Rejected oversized request", file=sys.stderr, flush=True)
            while b"\n" in pending:
                line, _, pending = pending.partition(b"\n")
                try:
                    answer = response(json.loads(line))
                    data = encode(answer)
                    reply_fd = os.open(directory / "reply.fifo", os.O_WRONLY | os.O_NONBLOCK)
                    try:
                        assert os.write(reply_fd, data) == len(data)
                    finally:
                        os.close(reply_fd)
                except (ValueError, OSError) as error:
                    print(f"Client exchange failed: {error}", file=sys.stderr, flush=True)
    finally:
        if fd is not None:
            os.close(fd)
        for path in reversed(created):
            path.unlink(missing_ok=True)
        directory.rmdir()
        print("CLEAN FIFO", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("server", "client"))
    parser.add_argument("expression", nargs="?")
    parser.add_argument("--directory")
    args = parser.parse_args()
    try:
        if args.mode == "server":
            serve(args.directory)
        else:
            if args.expression is None:
                parser.error("client requires a quoted expression")
            print(json.dumps(request(args.expression, args.directory), ensure_ascii=False))
    except (OSError, ValueError, TimeoutError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
