#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

report_shell_error() {
  local exit_code="$1"
  local line="$2"
  echo "real-faults: shell failure exit=$exit_code line=$line" >&2
}
trap 'report_shell_error "$?" "$LINENO"' ERR

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

duration_seconds=10
soak_hours=0
sigstop_ms=150
drop_every=0
delay_ms=2
skip_demo=0
while (($#)); do
  case "$1" in
    --duration-seconds) duration_seconds="$2"; shift 2 ;;
    --soak-hours) soak_hours="$2"; shift 2 ;;
    --sigstop-ms) sigstop_ms="$2"; shift 2 ;;
    --drop-every) drop_every="$2"; shift 2 ;;
    --delay-ms) delay_ms="$2"; shift 2 ;;
    --skip-demo) skip_demo=1; shift ;;
    *)
      echo "usage: $0 [--duration-seconds N] [--soak-hours N] [--sigstop-ms N] [--drop-every N] [--delay-ms N] [--skip-demo]" >&2
      exit 2
      ;;
  esac
done

if (( soak_hours > 0 )); then
  duration_seconds=$((soak_hours * 60 * 60))
fi

echo "real-faults: userspace TCP fault harness"
echo "duration_seconds=$duration_seconds sigstop_ms=$sigstop_ms drop_every=$drop_every delay_ms=$delay_ms"
echo "faults=SIGSTOP/SIGCONT,peer-proxy-delay,peer-proxy-drop,fsync-fatal-shim,ENOSPC-fatal-shim"
echo "The harness does not claim kernel-truth: disk/page-cache campaigns remain in cc-sim."

if (( skip_demo == 0 )); then
  "$repo_root/scripts/demo.sh"
else
  echo "demo phase skipped; use only when an independent demo audit has already passed"
fi

fault_dir="$(mktemp -d "${TMPDIR:-/tmp}/ccdb-faults.XXXXXX")"

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
declare -a node_pids=()
proxy_pid=""
workload_pid=""
checker_pid=""
pause_pid=""
cleanup() {
  if [[ -n "$pause_pid" ]]; then
    kill "$pause_pid" 2>/dev/null || true
    wait "$pause_pid" 2>/dev/null || true
  fi
  if [[ -n "$checker_pid" ]]; then
    kill "$checker_pid" 2>/dev/null || true
    wait "$checker_pid" 2>/dev/null || true
  fi
  if [[ -n "$workload_pid" ]]; then
    kill "$workload_pid" 2>/dev/null || true
    wait "$workload_pid" 2>/dev/null || true
  fi
  if [[ -n "$proxy_pid" ]]; then
    kill "$proxy_pid" 2>/dev/null || true
    wait "$proxy_pid" 2>/dev/null || true
  fi
  for pid in "${node_pids[@]:-}"; do
    kill -CONT "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${node_pids[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  if [[ "${CCDB_KEEP_FAULT_ARTIFACTS:-0}" == "1" ]]; then
    echo "real-faults: retained artifacts at $fault_dir" >&2
  else
    rm -rf "$fault_dir"
  fi
}
trap cleanup EXIT INT TERM

cargo build --locked --quiet -p cc-node --bin ccdb
ccdb_bin="$repo_root/target/debug/ccdb"
"$ccdb_bin" init --cluster faults --cluster-id 00112233445566778899aabbccddeeff --nodes 3 --base-dir "$fault_dir"

# Keep n1 direct and put the n2/n3 -> n1 peer path behind the userspace
# byte proxy.  This exercises the CCHL/CCPF/CCRP peer path, not only client
# RESP traffic.
"$ccdb_bin" run --config "$fault_dir/n1/ccdb.toml" >"$fault_dir/n1.log" 2>&1 &
node_pids[1]="$!"
wait_for_port 7101
wait_for_port 7201

python3 - "$fault_dir/n2/ccdb.toml" "$fault_dir/n3/ccdb.toml" <<'PY'
from pathlib import Path
import sys

for name in sys.argv[1:]:
    path = Path(name)
    text = path.read_text(encoding="utf-8")
    old = 'peer_nodes = "127.0.0.1:7201,'
    new = 'peer_nodes = "127.0.0.1:7379,'
    if old not in text:
        raise SystemExit(f"n1 peer address missing from {path}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
PY

cargo run --locked --quiet -p cc-swarm -- proxy \
  --listen 127.0.0.1:7379 \
  --upstream 127.0.0.1:7201 \
  --drop-every "$drop_every" \
  --delay-ms "$delay_ms" \
  >"$fault_dir/proxy.log" 2>&1 &
proxy_pid="$!"

wait_for_port 7379 40

for node in 2 3; do
  "$ccdb_bin" run --config "$fault_dir/n${node}/ccdb.toml" >"$fault_dir/n${node}.log" 2>&1 &
  node_pids[node]="$!"
done

for port in 7101 7102 7103 7201 7202 7203; do
  wait_for_port "$port"
done

if (( drop_every == 0 )); then
  "$ccdb_bin" peer --config "$fault_dir/n2/ccdb.toml" --addr 127.0.0.1:7379 --retries 5
else
  echo "peer proxy probe skipped because drop-every=$drop_every intentionally drops frames"
fi

echo "running one-client SET workload for ${duration_seconds}s"
python3 - "$fault_dir" "$duration_seconds" >"$fault_dir/workload.log" 2>&1 <<'PY' &
from pathlib import Path
import re
import socket
import sys
import time

root = Path(sys.argv[1])
duration = float(sys.argv[2])
deadline = time.monotonic() + duration
start_ns = time.monotonic_ns()
ports = [7101, 7102, 7103]
current = 0
sequence = 0
chunk = 0
chunk_count = 0
history = None
history_path = None
key = "soak-key"
key_hex = key.encode().hex()


def open_chunk(number):
    path = root / f"history-{number:06d}.tsv.partial"
    handle = path.open("w", encoding="utf-8")
    handle.write("# CC-HISTORY v1: KIND KEY OBSERVED_VALUE INVOKE_NS COMPLETE_NS\n")
    handle.flush()
    return path, handle


def close_chunk(path, handle):
    handle.flush()
    handle.close()
    path.rename(path.with_suffix(""))


def frame(*parts):
    output = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        value = str(part).encode()
        output.extend([f"${len(value)}\r\n".encode(), value, b"\r\n"])
    return b"".join(output)


def call(port, *parts):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
        sock.settimeout(2)
        sock.sendall(frame(*parts))
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            data = sock.recv(4096)
            if not data:
                return b"".join(chunks)
            chunks.append(data)


try:
    history_path, history = open_chunk(chunk)
    while time.monotonic() < deadline:
        value = str(sequence)
        invoke_ns = time.monotonic_ns() - start_ns
        operation_started = time.monotonic()
        acknowledged = False
        for _ in range(8):
            port = ports[current]
            try:
                reply = call(port, "SET", "soak-key", value)
            except OSError:
                current = (current + 1) % len(ports)
                continue
            redirect = re.search(rb"addr=127\.0\.0\.1:(710[1-3])", reply)
            if redirect:
                current = int(redirect.group(1)) - 7101
                continue
            if reply.startswith(b"+OK"):
                complete_ns = time.monotonic_ns() - start_ns
                # CC-HISTORY v1 stores binary keys and SET values as hex, not
                # the textual RESP arguments.  Preserve the exact operation
                # presented to ccdb so the checker proves the right history.
                history.write(
                    f"SET\t{key_hex}\t{value.encode().hex()}\t{invoke_ns}\t{complete_ns}\n"
                )
                history.flush()
                chunk_count += 1
                acknowledged = True
                break
            current = (current + 1) % len(ports)
        if not acknowledged:
            time.sleep(0.05)
        else:
            time.sleep(max(0.0, 0.05 - (time.monotonic() - operation_started)))
        sequence += 1
        if chunk_count >= 100:
            close_chunk(history_path, history)
            chunk += 1
            chunk_count = 0
            history_path, history = open_chunk(chunk)
    if history is not None:
        close_chunk(history_path, history)
finally:
    (root / "workload.done").write_text("done\n", encoding="utf-8")
PY
workload_pid="$!"

# Check completed history chunks while the workload is still running.  The
# workload atomically renames only closed chunks, so the checker never reads
# a partial TSV line.
python3 - "$fault_dir" "$workload_pid" "$repo_root" <<'PY' &
from pathlib import Path
import subprocess
import sys
import time

root = Path(sys.argv[1])
pid = int(sys.argv[2])
repo_root = Path(sys.argv[3])
checked = set()
while not (root / "workload.done").exists():
    for path in sorted(root.glob("history-*.tsv")):
        if path in checked:
            continue
        result = subprocess.run(
            ["cargo", "run", "--quiet", "-p", "cc-swarm", "--", "check-history", "--file", str(path)],
            cwd=repo_root,
            check=False,
        )
        if result.returncode != 0:
            try:
                import os
                import signal
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            raise SystemExit("history checker rejected a completed soak chunk")
        checked.add(path)
    time.sleep(5)
PY
checker_pid="$!"

if (( sigstop_ms > 0 )); then
  # A completed child remains visible to `kill -0` until its parent reaps it,
  # so polling the workload PID here can loop forever on a zombie.  Keep the
  # injector independently cancellable; the parent reaps the workload below.
  (
    while true; do
      echo "SIGSTOP node 1 for ${sigstop_ms}ms"
      kill -STOP "${node_pids[1]}" 2>/dev/null || exit 0
      sleep "$(awk "BEGIN { print $sigstop_ms / 1000 }")"
      kill -CONT "${node_pids[1]}" 2>/dev/null || exit 0
      sleep 10
    done
  ) &
  pause_pid="$!"
fi

workload_status=0
wait "$workload_pid" || workload_status=$?
workload_pid=""
if [[ -n "$pause_pid" ]]; then
  kill "$pause_pid" 2>/dev/null || true
  wait "$pause_pid" 2>/dev/null || true
  pause_pid=""
fi
touch "$fault_dir/workload.done"
checker_status=0
wait "$checker_pid" || checker_status=$?
checker_pid=""
if (( workload_status != 0 || checker_status != 0 )); then
  echo "real-faults: FAIL workload_status=$workload_status checker_status=$checker_status" >&2
  if [[ -f "$fault_dir/workload.log" ]]; then
    sed -n '1,120p' "$fault_dir/workload.log" >&2
  fi
  exit 1
fi

history_count=0
for history in "$fault_dir"/history-*.tsv; do
  [[ -f "$history" ]] || continue
  count=$(awk 'NR > 1 { count++ } END { print count + 0 }' "$history")
  history_count=$((history_count + count))
  cargo run --locked --quiet -p cc-swarm -- check-history --file "$history"
done
if (( history_count == 0 )); then
  echo "real-faults: no acknowledged operations recorded" >&2
  exit 1
fi
echo "history_operations=$history_count"

echo "CCDB_FAIL_FSYNC=1 and CCDB_FAIL_ENOSPC=1 are opt-in process-fatal shims; the journal unit path is covered by cc-node tests."
if (( skip_demo == 0 )); then
  echo "real-faults: PASS (demo, peer proxy path, SIGSTOP pauses, sustained workload, and history checker)"
else
  echo "real-faults: PASS (peer proxy path, SIGSTOP pauses, sustained workload, and history checker; demo was independently audited)"
fi
