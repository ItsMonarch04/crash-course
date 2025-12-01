#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import math
import os
import platform
import random
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WARMUP = 16


def percentile(values: list[int], p: int) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]


def buckets(values: list[int], count: int = 16) -> list[int]:
    ordered = sorted(values)
    return [ordered[min(len(ordered) - 1, (i * len(ordered)) // count)] for i in range(count)]


def timed(callable_) -> int:
    before = time.perf_counter_ns()
    callable_()
    return time.perf_counter_ns() - before


def filesystem_details(path: Path) -> tuple[str, str]:
    try:
        fs = subprocess.check_output(
            ["stat", "-f", "%T", str(path)], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        fs = "unknown"
    try:
        mounts = subprocess.check_output(["mount"], text=True, stderr=subprocess.DEVNULL)
        options = next((line for line in mounts.splitlines() if " on / " in line), "unknown")
    except (OSError, subprocess.CalledProcessError):
        options = "unknown"
    return fs, options


def disk_measurements(root: Path, samples: int) -> dict[str, list[int]]:
    results: dict[str, list[int]] = {
        "wal_append": [],
        "wal_fsync": [],
        "sst_random_read": [],
        "sst_sequential_read": [],
        "atomic_publish": [],
    }
    wal = root / "wal.bin"
    with wal.open("w+b", buffering=0) as handle:
        for _ in range(WARMUP + samples * 2):
            payload = b"w" * 4096
            append = timed(lambda: handle.write(payload))
            fsync = timed(lambda: os.fsync(handle.fileno()))
            if _ >= WARMUP:
                results["wal_append"].append(append)
                results["wal_fsync"].append(fsync)

    sst = root / "table.sst"
    sst.write_bytes(bytes(range(256)) * (16 * 1024 * 1024 // 256))
    rng = random.Random(0xCCDB)
    with sst.open("rb", buffering=0) as handle:
        offsets = [rng.randrange(0, (16 * 1024 * 1024) // 4096) * 4096 for _ in range(WARMUP + samples * 2)]
        for index, offset in enumerate(offsets):
            duration = timed(lambda offset=offset: os.pread(handle.fileno(), 4096, offset))
            if index >= WARMUP:
                results["sst_random_read"].append(duration)
        for index in range(WARMUP + samples * 2):
            offset = (index * 4096) % (16 * 1024 * 1024)
            duration = timed(lambda offset=offset: os.pread(handle.fileno(), 4096, offset))
            if index >= WARMUP:
                results["sst_sequential_read"].append(duration)

    directory_fd = os.open(root, os.O_RDONLY)
    try:
        for index in range(WARMUP + samples * 2):
            temporary = root / f"publish.{index}.tmp"
            published = root / f"publish.{index}.dat"

            def publish() -> None:
                with temporary.open("wb", buffering=0) as handle:
                    handle.write(b"p" * 4096)
                    os.fsync(handle.fileno())
                os.replace(temporary, published)
                os.fsync(directory_fd)

            duration = timed(publish)
            if index >= WARMUP:
                results["atomic_publish"].append(duration)
    finally:
        os.close(directory_fd)
    return results


def loopback_measurements(samples: int) -> list[int]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def echo() -> None:
        connection, _ = listener.accept()
        with connection:
            while True:
                data = connection.recv(1)
                if not data:
                    break
                connection.sendall(data)

    thread = threading.Thread(target=echo, daemon=True)
    thread.start()
    values: list[int] = []
    with socket.create_connection(("127.0.0.1", port)) as connection:
        for index in range(WARMUP + samples * 2):
            duration = timed(lambda: (connection.sendall(b"x"), connection.recv(1)))
            if index >= WARMUP:
                values.append(duration)
    listener.close()
    thread.join(timeout=1)
    return values


def resp(parts: list[str]) -> bytes:
    output = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        encoded = part.encode()
        output.extend((f"${len(encoded)}\r\n".encode(), encoded, b"\r\n"))
    return b"".join(output)


def commit_measurements(root: Path, samples: int) -> dict[str, list[int]]:
    subprocess.run(["cargo", "build", "--locked", "-p", "cc-node", "--quiet"], cwd=ROOT, check=True)
    binary = ROOT / "target/debug/ccdb"
    cluster_root = root / "cluster"
    data = cluster_root / "n1"
    subprocess.run(
        [
            str(binary),
            "init",
            "--cluster",
            "calibration",
            "--cluster-id",
            "abcdefabcdefabcdefabcdefabcdefab",
            "--nodes",
            "1",
            "--base-dir",
            str(cluster_root),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    reserved = [socket.socket() for _ in range(3)]
    for item in reserved:
        item.bind(("127.0.0.1", 0))
    ports = [item.getsockname()[1] for item in reserved]
    for item in reserved:
        item.close()
    config = data / "ccdb.toml"
    text = config.read_text()
    text = text.replace(":7101", f":{ports[0]}").replace(":7201", f":{ports[1]}").replace(":7301", f":{ports[2]}")
    config.write_text(text)
    process = subprocess.Popen(
        [str(binary), "run", "--config", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=None if os.environ.get("CC_CALIBRATION_VERBOSE") else subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", ports[0]), timeout=0.1):
                    break
            except OSError:
                time.sleep(0.02)
        else:
            raise RuntimeError("calibration ccdb did not start")

        next_client = 10_000
        client_lock = threading.Lock()

        def one_write() -> int:
            nonlocal next_client
            with client_lock:
                client = next_client
                next_client += 1
            request = resp(["CC.REQUEST", str(client), "1", "SET", f"cal:{client}", "value"])

            def exchange() -> None:
                last: object = b""
                for _ in range(8):
                    try:
                        with socket.create_connection(("127.0.0.1", ports[0]), timeout=4) as stream:
                            stream.sendall(request)
                            stream.shutdown(socket.SHUT_WR)
                            reply = b""
                            while True:
                                chunk = stream.recv(1024)
                                if not chunk:
                                    break
                                reply += chunk
                        if reply == b"+OK\r\n":
                            return
                        last = reply
                    except OSError as error:
                        last = error
                    time.sleep(0.01)
                raise RuntimeError(f"commit reply {last!r}")

            return timed(exchange)

        for _ in range(WARMUP):
            try:
                one_write()
            except RuntimeError:
                time.sleep(0.05)
        output: dict[str, list[int]] = {}
        for concurrency in (1, 8, 64):
            values: list[int] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
                for _ in range(samples):
                    values.extend(pool.map(lambda _: one_write(), range(concurrency)))
            output[f"commit_c{concurrency}"] = values
        return output
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--samples", type=int, default=4)
    args = parser.parse_args()
    if not args.profile or any(not (c.isalnum() or c in "-_") for c in args.profile):
        raise SystemExit("profile must be an ASCII name")
    if args.samples < 1:
        raise SystemExit("samples must be positive")

    with tempfile.TemporaryDirectory(prefix="cc-calibration-") as temporary:
        temp = Path(temporary)
        measurements = disk_measurements(temp, args.samples)
        measurements["loopback_rtt"] = loopback_measurements(args.samples)
        measurements.update(commit_measurements(temp, args.samples))

    # Odd samples fit the profile; even samples are the independent validation set.
    fit = {name: values[1::2] for name, values in measurements.items()}
    validation = {name: values[::2] for name, values in measurements.items()}
    read_values = fit["sst_random_read"] + fit["sst_sequential_read"]
    model_buckets = {
        "read": buckets(read_values),
        "write": buckets(fit["wal_append"]),
        "fsync": buckets(fit["wal_fsync"]),
        "rename": buckets(fit["atomic_publish"]),
        "dirsync": buckets(fit["atomic_publish"]),
    }
    modeled = {
        "wal_append": model_buckets["write"],
        "wal_fsync": model_buckets["fsync"],
        "sst_random_read": model_buckets["read"],
        "sst_sequential_read": model_buckets["read"],
        "atomic_publish": [model_buckets["rename"][i] + model_buckets["dirsync"][i] for i in range(16)],
        "loopback_rtt": [1_000_000],
    }
    for concurrency in (1, 8, 64):
        modeled[f"commit_c{concurrency}"] = [
            model_buckets["write"][i] + model_buckets["fsync"][i] + 1_000_000
            for i in range(16)
        ]

    fs_name, mount_options = filesystem_details(ROOT)
    binary_version = subprocess.check_output([str(ROOT / "target/debug/ccdb"), "--version"], text=True).splitlines()[0]
    environment = {
        "os": platform.platform(),
        "kernel": platform.release(),
        "cpu": platform.processor() or platform.machine(),
        "filesystem": fs_name,
        "mount_options": mount_options,
        "storage": os.environ.get("CC_CALIBRATION_STORAGE", "not-disclosed"),
        "build": binary_version,
        "command": f"scripts/calibrate.sh --profile {args.profile} --samples {args.samples}",
        "warmup": WARMUP,
        "fit_samples": sum(len(values) for values in fit.values()),
        "validation_samples": sum(len(values) for values in validation.values()),
    }
    environment_id = hashlib.sha256(json.dumps(environment, sort_keys=True).encode()).hexdigest()[:16]

    profile_dir = ROOT / "sim/profiles/calibrated"
    profile_dir.mkdir(parents=True, exist_ok=True)
    profile_path = profile_dir / f"{args.profile}.toml"
    def toml_string(value: object) -> str:
        return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'

    lines = [
        "schema = 1",
        f"name = {toml_string(args.profile)}",
        f"environment_id = {toml_string(environment_id)}",
        f"os = {toml_string(environment['os'])}",
        f"kernel = {toml_string(environment['kernel'])}",
        f"cpu = {toml_string(environment['cpu'])}",
        f"filesystem = {toml_string(environment['filesystem'])}",
        f"mount_options = {toml_string(environment['mount_options'])}",
        f"storage = {toml_string(environment['storage'])}",
        f"build = {toml_string(environment['build'])}",
        f"command = {toml_string(environment['command'])}",
        f"warmup = {WARMUP}",
        f"fit_samples = {environment['fit_samples']}",
        f"validation_samples = {environment['validation_samples']}",
    ]
    for operation in ("read", "write", "fsync", "rename", "dirsync"):
        values = ", ".join(str(value) for value in model_buckets[operation])
        lines.append(f"{operation}_buckets_ns = [{values}]")
    profile_path.write_text("\n".join(lines) + "\n")

    results_dir = ROOT / "bench/results"
    results_dir.mkdir(parents=True, exist_ok=True)
    raw_path = results_dir / f"{args.profile}.raw.json"
    raw_path.write_text(json.dumps({"environment": environment, "measurements_ns": measurements}, indent=2, sort_keys=True) + "\n")
    csv_path = results_dir / f"{args.profile}.csv"
    with csv_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["profile", "environment_id", "workload", "concurrency", "stat", "measured_ns", "modeled_ns", "residual_ns", "samples", "tail_count"])
        for workload, measured_values in validation.items():
            concurrency = int(workload.removeprefix("commit_c")) if workload.startswith("commit_c") else 1
            model_values = modeled[workload]
            measured_p99 = percentile(measured_values, 99)
            tail_count = sum(value > measured_p99 for value in measured_values)
            for stat in (50, 95, 99):
                measured_value = percentile(measured_values, stat)
                modeled_value = percentile(model_values, stat)
                writer.writerow([args.profile, environment_id, workload, concurrency, f"p{stat}", measured_value, modeled_value, modeled_value - measured_value, len(measured_values), tail_count])
    print(f"calibration: PASS profile={args.profile} profile_config={profile_path.relative_to(ROOT)} normalized_csv={csv_path.relative_to(ROOT)} raw_json={raw_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
