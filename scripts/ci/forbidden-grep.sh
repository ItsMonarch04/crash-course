#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cargo run --quiet -p cc-detlint -- check \
  "$root_dir/crates/cc-core" \
  "$root_dir/crates/cc-env" \
  "$root_dir/crates/cc-sim" \
  "$root_dir/crates/cc-wal" \
  "$root_dir/crates/cc-store" \
  "$root_dir/crates/cc-raft" \
  "$root_dir/crates/cc-kv" \
  "$root_dir/crates/cc-log" \
  "$root_dir/crates/cc-cluster" \
  "$root_dir/crates/cc-host" \
  "$root_dir/crates/cc-resp" \
  "$root_dir/crates/cc-checker" \
  "$root_dir/crates/cc-wasm"

# N1 permits a literal `commands.log` only in the migration-boundary refusal;
# it must not retain an executable second replication/state-machine path.
if rg -n \
  'CCREPL|DurableJournal|struct HostState|fn (run_node|apply_durable|apply_replica|replicate)\b|thread::spawn' \
  "$root_dir/crates/cc-node/src" -g '*.rs'; then
  echo "forbidden-grep: cc-node contains a legacy replication path or raw thread spawn" >&2
  exit 1
fi

# Simulated durable recovery is intentionally the same framed cc-log stream as
# the real adapter. A private hard-state/entry decoder would create a second
# recovery vocabulary and invalidate that shared receipt.
if rg -n \
  'WAL_HARD_STATE|fn decode_wal\b|fn encode_entry\b|fn prepare_(append|truncate)\b' \
  "$root_dir/crates/cc-swarm/src/lib.rs"; then
  echo "forbidden-grep: cc-swarm contains a synthetic Raft WAL codec" >&2
  exit 1
fi

# The shared discrete-event queue owns the ordering semantics. The cluster
# host must not grow a separate heap/tie-sequence implementation.
if rg -n '\bBinaryHeap\b|struct ClusterEvent\b|next_tie_seq' \
  "$root_dir/crates/cc-swarm/src/lib.rs"; then
  echo "forbidden-grep: cc-swarm contains a private event scheduler" >&2
  exit 1
fi

# SimCluster is an adapter over the shared Driver. It may orchestrate virtual
# time, faults, and CCPF transport, but it must not call the private core input
# or effect vocabulary or restore a wiped node by copying leader state.
if rg -n '\b(NodeInput|NodeEffect)\b|install_leader_snapshot' \
  "$root_dir/crates/cc-swarm/src/lib.rs"; then
  echo "forbidden-grep: cc-swarm bypasses the shared Driver boundary" >&2
  exit 1
fi

if rg -n 'next_message_token|messages:\s*BTreeMap' \
  "$root_dir/crates/cc-swarm/src/lib.rs"; then
  echo "forbidden-grep: cc-swarm contains a peer-message side channel" >&2
  exit 1
fi

# The generic deterministic scheduler stays below the consensus/KV layer.
if rg -n 'cc-(cluster|kv)' "$root_dir/crates/cc-sim/Cargo.toml"; then
  echo "forbidden-grep: cc-sim depends on consensus or KV" >&2
  exit 1
fi

# The wasm bridge consumes snapshots from SimCluster; it cannot reach into the
# composition, KV, or store crates to construct an alternate execution path.
if rg -n 'cc-(cluster|kv|store)' "$root_dir/crates/cc-wasm/Cargo.toml"; then
  echo "forbidden-grep: cc-wasm can construct a second simulator core" >&2
  exit 1
fi
