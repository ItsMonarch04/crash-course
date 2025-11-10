#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

duration_seconds=10
soak_hours=0
sigstop_ms=150
drop_every=0
delay_ms=2
while (($#)); do
  case "$1" in
    --duration-seconds) duration_seconds="$2"; shift 2 ;;
    --soak-hours) soak_hours="$2"; shift 2 ;;
    --sigstop-ms) sigstop_ms="$2"; shift 2 ;;
    --drop-every) drop_every="$2"; shift 2 ;;
    --delay-ms) delay_ms="$2"; shift 2 ;;
    *)
      echo "usage: $0 [--duration-seconds N] [--soak-hours N] [--sigstop-ms N] [--drop-every N] [--delay-ms N]" >&2
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

"$repo_root/scripts/demo.sh"

fault_dir="$(mktemp -d "${TMPDIR:-/tmp}/ccdb-faults.XXXXXX")"
declare -a node_pids=()
proxy_pid=""
workload_pid=""
checker_pid=""
cleanup() {
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
  rm -rf "$fault_dir"
}
trap cleanup EXIT INT TERM

cargo build --quiet -p cc-node --bin ccdb
ccdb_bin="$repo_root/target/debug/ccdb"
"$ccdb_bin" init --cluster faults --nodes 3 --base-dir "$fault_dir"

# Keep n1 direct and put the n2/n3 -> n1 peer path behind the userspace
# byte proxy.  This exercises CCREPL1 frames, not only client RESP traffic.
"$ccdb_bin" run --config "$fault_dir/n1/ccdb.toml" >"$fault_dir/n1.log" 2>&1 &
node_pids[1]="$!"
python3 <<'PY'
import socket
import time

for port in (7101, 7201):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.05)
    else:
        raise SystemExit(f"node 1 port {port} did not become ready")
PY

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

python3 scripts/resp-proxy.py \
  --listen 127.0.0.1:7379 \
  --upstream 127.0.0.1:7201 \
  --drop-every "$drop_every" \
  --delay-ms "$delay_ms" \
  >"$fault_dir/proxy.log" 2>&1 &
proxy_pid="$!"

python3 <<'PY'
import socket
import time

deadline = time.monotonic() + 2
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", 7379), timeout=0.1):
            break
    except OSError:
        time.sleep(0.05)
else:
    raise SystemExit("peer proxy did not become ready")
PY

for node in 2 3; do
  "$ccdb_bin" run --config "$fault_dir/n${node}/ccdb.toml" >"$fault_dir/n${node}.log" 2>&1 &
  node_pids[node]="$!"
done

python3 <<'PY'
import socket
import time

for port in (7101, 7102, 7103, 7201, 7202, 7203):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.05)
    else:
        raise SystemExit(f"port {port} did not become ready")
PY

if (( drop_every == 0 )); then
  "$ccdb_bin" peer --addr 127.0.0.1:7379 --retries 5
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
                history.write(f"SET\tsoak-key\t{value}\t{invoke_ns}\t{complete_ns}\n")
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

workload_status=0
while kill -0 "$workload_pid" 2>/dev/null; do
  if (( sigstop_ms > 0 )); then
    echo "SIGSTOP node 1 for ${sigstop_ms}ms"
    kill -STOP "${node_pids[1]}" 2>/dev/null || true
    sleep "$(awk "BEGIN { print $sigstop_ms / 1000 }")"
    kill -CONT "${node_pids[1]}" 2>/dev/null || true
  fi
  sleep 10
done
wait "$workload_pid" || workload_status=$?
workload_pid=""
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
  cargo run --quiet -p cc-swarm -- check-history --file "$history"
done
if (( history_count == 0 )); then
  echo "real-faults: no acknowledged operations recorded" >&2
  exit 1
fi
echo "history_operations=$history_count"

echo "CCDB_FAIL_FSYNC=1 and CCDB_FAIL_ENOSPC=1 are opt-in process-fatal shims; the journal unit path is covered by cc-node tests."
echo "real-faults: PASS (demo, peer proxy path, SIGSTOP pauses, sustained workload, and history checker)"
