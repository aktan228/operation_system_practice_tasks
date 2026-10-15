#!/usr/bin/env python3
"""AF_UNIX stream service: one child process per connected client."""
import argparse
import json
import multiprocessing as mp
import os
import socket
import sys
import time
import uuid
from pathlib import Path

from calculator import response
from ipc_common import MAX_MESSAGE, default_directory, encode, install_stop_handler


def read_line(sock):
    data = bytearray()
    while b"\n" not in data:
        part = sock.recv(MAX_MESSAGE)
        if not part:
            raise EOFError("peer closed before the complete message")
        data.extend(part)
        if len(data) > MAX_MESSAGE:
            raise ValueError("message too large")
    return json.loads(bytes(data).split(b"\n", 1)[0])


def request(expression, directory=None, timeout=10):
    directory = Path(directory) if directory else default_directory("unix")
    message = {"id": uuid.uuid4().hex, "expression": expression}
    payload = encode(message)
    begin = time.perf_counter()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(directory / "service.sock"))
        sock.sendall(payload)
        answer = read_line(sock)
        assert answer["id"] == message["id"], "wrong response id"
    return {"client_pid": os.getpid(), "expression": expression, "response": answer,
            "seconds": time.perf_counter() - begin,
            "wire_bytes": len(payload) + len(encode(answer))}


def handle_client(connection):
    try:
        with connection:
            connection.settimeout(5)
            answer = response(read_line(connection))
            answer["worker_pid"] = os.getpid()
            connection.sendall(encode(answer))
    except (OSError, EOFError, ValueError) as error:
        print(f"Client disconnected: {error}", file=sys.stderr, flush=True)


def serve(directory=None):
    directory = Path(directory) if directory else default_directory("unix")
    state = install_stop_handler()
    directory.mkdir(mode=0o700)
    path = directory / "service.sock"
    children = []
    context = mp.get_context("spawn")  # children inherit neither listener nor signal handlers
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(path))
            listener.listen(32)
            listener.settimeout(0.1)
            (directory / "ready").touch(mode=0o600)
            print(f"READY UNIX directory={directory} pid={os.getpid()}", flush=True)
            while not state["stop"]:
                for child in children[:]:
                    if not child.is_alive():
                        child.join()
                        child.close()
                        children.remove(child)
                try:
                    connection, _ = listener.accept()
                except socket.timeout:
                    continue
                child = context.Process(target=handle_client, args=(connection,))
                try:
                    child.start()
                    children.append(child)
                finally:
                    connection.close()
    finally:
        deadline = time.monotonic() + 6
        for child in children:
            child.join(max(0, deadline - time.monotonic()))
            if child.is_alive():
                child.terminate()
                child.join(2)
            child.close()
        path.unlink(missing_ok=True)
        (directory / "ready").unlink(missing_ok=True)
        directory.rmdir()
        print("CLEAN UNIX", flush=True)


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
    except (OSError, ValueError, EOFError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
