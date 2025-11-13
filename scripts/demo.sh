#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

demo_dir="$(mktemp -d "${TMPDIR:-/tmp}/ccdb-demo.XXXXXX")"
declare -a node_pids=()
cleanup() {
  for pid in "${node_pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${node_pids[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$demo_dir"
}
trap cleanup EXIT INT TERM

cargo build --quiet -p cc-node --bin ccdb
ccdb_bin="$repo_root/target/debug/ccdb"
ccdb() {
  "$ccdb_bin" "$@"
}

echo "[1/7] initialize an isolated three-node cluster"
ccdb init --cluster demo --nodes 3 --base-dir "$demo_dir"

wait_for_port() {
  local port="$1"
  local attempts="${2:-80}"
  # bash's /dev/tcp needs no external binary and no Python: the point of this
  # harness is the database, not its scaffolding.
  for _ in $(seq 1 "$attempts"); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      return 0
    fi
    sleep 0.05
  done
  echo "port $port did not become ready" >&2
  return 1
}

start_node() {
  local node="$1"
  local config="$demo_dir/n${node}/ccdb.toml"
  "$ccdb_bin" run --config "$config" >"$demo_dir/n${node}.log" 2>&1 &
  node_pids[node]="$!"
  wait_for_port "$((7100 + node))"
}

start_node 1
start_node 2
start_node 3

echo "[2/7] verify bounded peer frames on every node"
for node in 1 2 3; do
  ccdb peer --addr "127.0.0.1:$((7200 + node))" --retries 5
done

echo "[3/7] exercise SET/GET and twenty acknowledged INCR operations"
python3 - "$demo_dir" <<'PY'
import socket
import sys
import time


def frame(*parts):
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = str(part).encode()
        encoded.extend([f"${len(value)}\r\n".encode(), value, b"\r\n"])
    return b"".join(encoded)


def command(port, *parts):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
        sock.sendall(frame(*parts))
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            data = sock.recv(4096)
            if not data:
                return b"".join(chunks)
            chunks.append(data)


def ensure_ok(reply, label):
    if reply.startswith(b"-"):
        raise SystemExit(f"{label} failed: {reply!r}")


ensure_ok(command(7101, "SET", "counter", "0"), "SET")
for index in range(20):
    reply = command(7101, "INCR", "counter")
    ensure_ok(reply, f"INCR {index + 1}")
    time.sleep(0.05)
reply = command(7101, "GET", "counter")
ensure_ok(reply, "GET")
if b"20" not in reply:
    raise SystemExit(f"counter did not reach 20: {reply!r}")
print("acknowledged=20 counter=20")
PY

echo "[4/7] kill the current leader and verify sub-two-second failover"
kill -9 "${node_pids[1]}"
node_pids[1]=""
python3 <<'PY'
import socket
import time


def frame(*parts):
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = str(part).encode()
        encoded.extend([f"${len(value)}\r\n".encode(), value, b"\r\n"])
    return b"".join(encoded)


started = time.monotonic()
last = b""
while time.monotonic() - started < 2:
    try:
        with socket.create_connection(("127.0.0.1", 7102), timeout=0.2) as sock:
            sock.sendall(frame("INCR", "counter"))
            sock.shutdown(socket.SHUT_WR)
            last = sock.recv(4096)
        if not last.startswith(b"-"):
            print(f"failover_ms={(time.monotonic() - started) * 1000:.1f} reply={last!r}")
            raise SystemExit(0)
    except OSError:
        pass
    time.sleep(0.05)
raise SystemExit(f"failover did not produce an acknowledgement: {last!r}")
PY

echo "[5/7] restart the killed node and verify TCP snapshot catch-up"
rm -f "$demo_dir/n1/commands.log" "$demo_dir/n1/trace.log" "$demo_dir/n1/metrics.prom"
echo "wiped n1 journal and trace; identity marker remains"
start_node 1
python3 <<'PY'
import socket
import time


def frame(*parts):
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = str(part).encode()
        encoded.extend([f"${len(value)}\r\n".encode(), value, b"\r\n"])
    return b"".join(encoded)


started = time.monotonic()
while time.monotonic() - started < 2:
    try:
        with socket.create_connection(("127.0.0.1", 7101), timeout=0.2) as sock:
            sock.sendall(frame("GET", "counter"))
            sock.shutdown(socket.SHUT_WR)
            reply = sock.recv(4096)
        if b"21" in reply:
            print(f"catchup_ms={(time.monotonic() - started) * 1000:.1f} counter=21")
            raise SystemExit(0)
    except OSError:
        pass
    time.sleep(0.05)
raise SystemExit("restarted node did not catch up to counter=21")
PY

echo "[6/7] inspect live admin status and durable journals"
ccdb admin --addr 127.0.0.1:7102 status
ccdb admin --config "$demo_dir/n1/ccdb.toml" members
for node in 1 2 3; do
  ccdb selfcheck --data-dir "$demo_dir/n${node}"
done

echo "[7/7] demo: PASS (three-node replication, failover, restart, snapshot catch-up)"
