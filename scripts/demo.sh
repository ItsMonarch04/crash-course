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
  if [[ "${CCDB_DEMO_KEEP:-0}" == "1" ]]; then
    echo "retained demo directory: $demo_dir" >&2
  else
    rm -rf "$demo_dir"
  fi
}
trap cleanup EXIT INT TERM

cargo build --locked --quiet -p cc-node --bin ccdb -p cc-swarm --bin cc-swarm
ccdb_bin="$repo_root/target/debug/ccdb"
swarm_bin="$repo_root/target/debug/cc-swarm"
ccdb() {
  "$ccdb_bin" "$@"
}

echo "[1/8] initialize an isolated three-node cluster"
ccdb init --cluster demo --cluster-id 00112233445566778899aabbccddeeff --nodes 3 --base-dir "$demo_dir"

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
  local record="${2:-}"
  local config="$demo_dir/n${node}/ccdb.toml"
  if [[ "$record" == "record" ]]; then
    "$ccdb_bin" run --config "$config" --record "$demo_dir/n${node}.ccij" >"$demo_dir/n${node}.log" 2>&1 &
  else
    "$ccdb_bin" run --config "$config" >"$demo_dir/n${node}.log" 2>&1 &
  fi
  node_pids[node]="$!"
  wait_for_port "$((7100 + node))"
}

start_node 1 record
start_node 2 record
start_node 3 record

echo "[2/8] verify bounded peer frames on every node"
for node in 1 2 3; do
  probe_node=$((node % 3 + 1))
  ccdb peer --config "$demo_dir/n${probe_node}/ccdb.toml" --addr "127.0.0.1:$((7200 + node))" --retries 5
done

echo "[3/8] exercise SET/GET and twenty acknowledged CC.REQUEST INCR operations"
python3 - "$demo_dir" <<'PY'
from pathlib import Path
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


def command_any(*parts):
    deadline = time.monotonic() + 3
    last = b""
    while time.monotonic() < deadline:
        for port in (7101, 7102, 7103):
            try:
                reply = command(port, *parts)
            except OSError:
                continue
            last = reply
            if not reply.startswith(b"-"):
                return reply, port
        time.sleep(0.025)
    raise SystemExit(f"no elected node acknowledged {parts!r}: {last!r}")


def ensure_ok(reply, label):
    if reply.startswith(b"-"):
        raise SystemExit(f"{label} failed: {reply!r}")


reply, leader_port = command_any("SET", "counter", "0")
ensure_ok(reply, "SET")
for index in range(1, 21):
    reply, leader_port = command_any("CC.REQUEST", "9001", index, "INCR", "counter")
    if reply != f":{index}\r\n".encode():
        raise SystemExit(f"CC.REQUEST {index} failed: {reply!r}")
    time.sleep(0.05)
reply, leader_port = command_any("GET", "counter")
ensure_ok(reply, "GET")
if b"20" not in reply:
    raise SystemExit(f"counter did not reach 20: {reply!r}")
Path(sys.argv[1], "leader-node").write_text(f"{leader_port - 7100}\n", encoding="ascii")
print(f"acknowledged_cc_requests=20 counter=20 leader=n{leader_port - 7100}")
PY

echo "[4/8] kill the current leader and verify sub-two-second failover"
leader_node="$(<"$demo_dir/leader-node")"
leader_port=$((7100 + leader_node))
kill -9 "${node_pids[leader_node]}"
node_pids[leader_node]=""
python3 - "$leader_port" <<'PY'
import socket
import sys
import time


def frame(*parts):
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = str(part).encode()
        encoded.extend([f"${len(value)}\r\n".encode(), value, b"\r\n"])
    return b"".join(encoded)


started = time.monotonic()
last = b""
former_leader = int(sys.argv[1])
while time.monotonic() - started < 2:
    for port in (7101, 7102, 7103):
        if port == former_leader:
            continue
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2) as sock:
                sock.settimeout(0.4)
                sock.sendall(frame("CC.REQUEST", "9001", "21", "INCR", "counter"))
                sock.shutdown(socket.SHUT_WR)
                last = sock.recv(4096)
            if last == b":21\r\n":
                print(f"failover_ms={(time.monotonic() - started) * 1000:.1f} reply={last!r}")
                raise SystemExit(0)
        except OSError:
            pass
    time.sleep(0.05)
raise SystemExit(f"failover did not produce an acknowledgement: {last!r}")
PY

echo "[5/8] restart the killed node and verify its retained durable WAL"
rm -f "$demo_dir/n${leader_node}/trace.log" "$demo_dir/n${leader_node}/metrics.prom"
start_node "$leader_node"
ccdb selfcheck --data-dir "$demo_dir/n${leader_node}"
echo "restarted n${leader_node}; durable WAL prefix verified"
python3 - <<'PY'
import socket
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


def command_any(*parts):
    deadline = time.monotonic() + 3
    last = b""
    while time.monotonic() < deadline:
        for port in (7101, 7102, 7103):
            try:
                reply = command(port, *parts)
            except OSError:
                continue
            last = reply
            if not reply.startswith(b"-"):
                return reply
        time.sleep(0.025)
    raise SystemExit(f"no elected node acknowledged {parts!r}: {last!r}")


# Retry the write acknowledged after failover. The same durable request must
# return its cached reply and never increment a second time, including after
# the killed node has restarted from its WAL prefix.
retry = command_any("CC.REQUEST", "9001", "21", "INCR", "counter")
if retry != b":21\r\n":
    raise SystemExit(f"durable retry diverged: {retry!r}")
counter = command_any("GET", "counter")
if counter != b"$2\r\n21\r\n":
    raise SystemExit(f"acknowledged CC.REQUEST write was lost or reapplied: {counter!r}")
print("durable_cc_request_retry=PASS counter=21")
PY

echo "[6/8] inspect configured membership and durable WAL prefixes"
ccdb admin --config "$demo_dir/n1/ccdb.toml" members
for node in 1 2 3; do
  ccdb selfcheck --data-dir "$demo_dir/n${node}"
done

echo "[7/8] replay every recorded node receipt through the shared Driver"
for node in 1 2 3; do
  "$swarm_bin" replay --file "$demo_dir/n${node}.ccij" --assert-effects
done

echo "[8/8] demo: PASS (three-node Raft replication, failover, retained-log restart, replay)"
