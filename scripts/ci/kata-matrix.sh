#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

cargo check --workspace
cargo test -p cc-swarm trap_default_build_enables_no_kata
cargo test -p cc-swarm trap_kata_features_are_mutually_exclusive

pair_log="$repo_root/target/kata-mutual-exclusion.log"
mkdir -p "$(dirname "$pair_log")"
if cargo check -p cc-swarm --features kata01,kata02 >"$pair_log" 2>&1; then
  echo "kata01,kata02 unexpectedly compiled together" >&2
  exit 1
fi
if ! rg -qF "kata features are mutually exclusive" "$pair_log"; then
  echo "kata pair failed for an unexpected reason" >&2
  cat "$pair_log" >&2
  exit 1
fi

# Each kata build is linted too. A kata arm that only compiles under `cargo
# test` can rot unnoticed: the default build never sees it, so nothing else in
# CI would catch a warning or a type error introduced by a refactor.
for pair in cc-raft:kata01 cc-raft:kata02 cc-host:kata03 cc-store:kata04 cc-cluster:kata05; do
  cargo clippy -p "${pair%%:*}" --all-targets --features "${pair##*:}" -- -D warnings
done

cargo test -p cc-raft --features kata01 trap_kata_01_commit_quorum_is_found_within_budget
cargo test -p cc-raft --features kata02 trap_kata_02_wrong_timer_reset_is_found_within_budget
cargo test -p cc-host --features kata03 trap_kata_03_ack_before_fsync_is_found_within_budget
cargo test -p cc-store --features kata04 trap_kata_04_tombstone_gc_is_found_within_budget
cargo test -p cc-cluster --features kata05 trap_kata_05_session_dedup_is_found_within_budget

for kata in kata01 kata02 kata03 kata04 kata05; do
  cargo test -p cc-swarm --features "$kata" trap_active_kata_is_visible_in_trace_and_artifact_type
done

echo "kata matrix: PASS"
