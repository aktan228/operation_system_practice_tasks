#!/usr/bin/env python3
"""Compare shared-memory publication with Pipe.send_bytes for identical uint32 data."""
import argparse
import hashlib
import json
import multiprocessing as mp
import os
import platform
import signal
import statistics
import time
from array import array
from multiprocessing import shared_memory
from pathlib import Path


def receiver(kind, count, control, data_rx, shm_name, published):
    segment = view = None
    try:
        if kind == "shared_memory":
            segment = shared_memory.SharedMemory(name=shm_name)
            view = segment.buf[:count * 4]
        control.send({"stage": "ready"})
        if kind == "shared_memory":
            if not published.wait(60):
                raise TimeoutError("data publication timeout")
            payload = view
        else:
            payload = data_rx.recv_bytes()
        # Delivery acknowledgement is deliberately before the full checksum scan.
        control.send({"stage": "received", "bytes": len(payload)})
        digest = hashlib.sha256(payload).hexdigest()
        control.send({"stage": "verified", "sha256": digest, "bytes": len(payload)})
    except BaseException as error:
        try:
            control.send({"stage": "error", "error": repr(error)})
        except (OSError, EOFError):
            pass
        raise
    finally:
        if view is not None:
            view.release()
        if segment is not None:
            segment.close()
        if data_rx is not None:
            data_rx.close()
        control.close()


def receive_stage(control, stage):
    if not control.poll(60):
        raise TimeoutError(f"receiver did not reach {stage}")
    message = control.recv()
    if message.get("stage") != stage:
        raise RuntimeError(f"expected {stage}, got {message}")
    return message


def transfer(kind, data, expected_digest):
    context = mp.get_context("spawn")
    parent, child = context.Pipe()
    data_rx = data_tx = None
    segment = None
    process = None
    name = None
    published = context.Event()
    try:
        if kind == "shared_memory":
            segment = shared_memory.SharedMemory(create=True, size=len(data) * 4)
            name = segment.name
        else:
            data_rx, data_tx = context.Pipe(duplex=False)
        process = context.Process(target=receiver,
                                  args=(kind, len(data), child, data_rx, name, published))
        process.start()
        child.close()
        if data_rx is not None:
            data_rx.close()
        receive_stage(parent, "ready")
        begin = time.perf_counter()
        if kind == "shared_memory":
            source = memoryview(data).cast("B")
            try:
                segment.buf[:] = source
            finally:
                source.release()
            published.set()  # publish only after the entire copy has finished
        else:
            data_tx.send_bytes(data)
        delivered = receive_stage(parent, "received")
        handoff = time.perf_counter() - begin
        verified = receive_stage(parent, "verified")
        complete = time.perf_counter() - begin
        assert delivered["bytes"] == verified["bytes"] == len(data) * 4
        assert verified["sha256"] == expected_digest, "data corruption"
        process.join(10)
        assert process.exitcode == 0, f"receiver failed: {process.exitcode}"
        return {"mechanism": kind, "count": len(data), "bytes": len(data) * 4,
                "handoff_seconds": handoff, "verified_seconds": complete,
                "sha256": expected_digest, "correct": True, "shm_name": name}
    finally:
        if process is not None and process.pid is not None:
            if process.is_alive():
                process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join()
            process.close()
        parent.close()
        child.close()
        if data_rx is not None:
            data_rx.close()
        if data_tx is not None:
            data_tx.close()
        if segment is not None:
            segment.close()
            segment.unlink()  # creator owns unlink; receiver only closes its mapping


def benchmark(count=10_000_000, repeats=3):
    if not 0 < count <= 2**32 or repeats <= 0:
        raise ValueError("count must be in 1..2**32; repeats must be positive")
    data = array("I", range(count))
    assert data.itemsize == 4, "this benchmark requires 4-byte unsigned integers"
    digest = hashlib.sha256(data).hexdigest()
    if os.name == "posix" and Path("/dev/shm").exists():
        free = os.statvfs("/dev/shm")
        if free.f_bavail * free.f_frsize < len(data) * 4:
            raise RuntimeError("not enough free space in /dev/shm")
    runs = []
    for iteration in range(repeats):
        order = ("shared_memory", "pipe") if iteration % 2 == 0 else ("pipe", "shared_memory")
        for kind in order:
            row = transfer(kind, data, digest)
            if row["shm_name"]:
                try:
                    leftover = shared_memory.SharedMemory(name=row["shm_name"])
                except FileNotFoundError:
                    pass
                else:
                    leftover.close()
                    raise AssertionError("shared-memory segment was not unlinked")
            runs.append(row)
    medians = []
    for kind in ("shared_memory", "pipe"):
        selected = [row for row in runs if row["mechanism"] == kind]
        medians.append({"mechanism": kind, "bytes": len(data) * 4,
                        "handoff_seconds": statistics.median(row["handoff_seconds"] for row in selected),
                        "verified_seconds": statistics.median(row["verified_seconds"] for row in selected)})
    return {"platform": platform.platform(), "python": platform.python_version(),
            "count": count, "bytes": len(data) * 4, "repeats": repeats,
            "runs": runs, "medians": medians, "cleanup_ok": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10_000_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--out", default="results/memory.json")
    args = parser.parse_args()

    def stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    report = benchmark(args.count, args.repeats)
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"{report['count']} uint32 = {report['bytes']} bytes; medians of {report['repeats']} runs")
    print(f"{'Mechanism':<18} {'Handoff, s':>12} {'With SHA256, s':>16}")
    for row in report["medians"]:
        print(f"{row['mechanism']:<18} {row['handoff_seconds']:>12.6f} {row['verified_seconds']:>16.6f}")
    print(f"SHA256: OK; cleanup: OK; saved {path}")


if __name__ == "__main__":
    main()
