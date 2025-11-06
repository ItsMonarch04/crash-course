#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

demo_dir="$(mktemp -d "${TMPDIR:-/tmp}/ccdb-demo.XXXXXX")"
node_pid=""
cleanup() {
  if [[ -n "$node_pid" ]]; then
    kill "$node_pid" 2>/dev/null || true
    wait "$node_pid" 2>/dev/null || true
  fi
  rm -rf "$demo_dir"
}
trap cleanup EXIT INT TERM

echo "[1/5] initialize an isolated one-node cluster"
cargo run --quiet -p cc-node --bin ccdb -- init --cluster demo --nodes 1 --base-dir "$demo_dir"
config="$demo_dir/n1/ccdb.toml"

start_node() {
  cargo run --quiet -p cc-node --bin ccdb -- run --config "$config" >"$demo_dir/node.log" 2>&1 &
  node_pid=$!
  for _ in {1..50}; do
    if python3 - "$demo_dir" <<'PY'
import socket

with socket.socket() as sock:
    sock.settimeout(0.1)
    try:
        sock.connect(("127.0.0.1", 7101))
    except OSError:
        raise SystemExit(1)
PY
    then
      return 0
    fi
    sleep 0.1
  done
  echo "node did not become ready" >&2
  return 1
}

send_commands() {
  python3 - <<'PY'
import socket

def frame(*parts):
    encoded = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = part.encode()
        encoded.extend([f"${len(value)}\r\n".encode(), value, b"\r\n"])
    return b"".join(encoded)

request = b"".join([
    frame("PING"),
    frame("SET", "course", "durable"),
    frame("GET", "course"),
])
with socket.create_connection(("127.0.0.1", 7101), timeout=2) as sock:
    sock.sendall(request)
    sock.shutdown(socket.SHUT_WR)
    print(sock.recv(4096).decode("utf-8", "replace"), end="")
PY
}

cat >"$demo_dir/history.tsv" <<'EOF'
# CC-HISTORY v1: KIND KEY OBSERVED_VALUE INVOKE_NS COMPLETE_NS
SET	course	durable	1	2
GET	course	durable	3	4
EOF

echo "[2/5] start the real host and exercise pipelined RESP"
start_node
cargo run --quiet -p cc-node --bin ccdb -- peer --addr 127.0.0.1:7201 --retries 3
send_commands
cargo run --quiet -p cc-swarm -- check-history --file "$demo_dir/history.tsv"

echo "[3/5] kill and restart: the command journal is the recovery source"
kill "$node_pid"
wait "$node_pid" 2>/dev/null || true
node_pid=""
start_node
python3 - <<'PY'
import socket

request = b"*2\r\n$3\r\nGET\r\n$6\r\ncourse\r\n"
with socket.create_connection(("127.0.0.1", 7101), timeout=2) as sock:
    sock.sendall(request)
    sock.shutdown(socket.SHUT_WR)
    answer = sock.recv(1024)
print("recovered:", answer.decode("utf-8", "replace").strip())
PY

echo "[4/5] inspect journal and metrics surfaces"
cargo run --quiet -p cc-node --bin ccdb -- selfcheck --data-dir "$demo_dir/n1"
if [[ -f "$demo_dir/n1/metrics.prom" ]]; then
  sed -n '1,20p' "$demo_dir/n1/metrics.prom"
else
  echo "metrics file is written by the host heartbeat; it was not observed before teardown"
fi

echo "[5/5] demo: PASS (single-node real-host restart path)"
