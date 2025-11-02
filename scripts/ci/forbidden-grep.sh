#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
core_dirs=(
  "$root_dir/crates/cc-core"
  "$root_dir/crates/cc-env"
  "$root_dir/crates/cc-sim"
  "$root_dir/crates/cc-wal"
  "$root_dir/crates/cc-store"
  "$root_dir/crates/cc-raft"
  "$root_dir/crates/cc-kv"
  "$root_dir/crates/cc-cluster"
  "$root_dir/crates/cc-resp"
  "$root_dir/crates/cc-checker"
)

pattern='std::time::(SystemTime|Instant)|std::thread|tokio::|async[[:space:]]+fn|rand::|getrandom|thread_rng|HashMap|HashSet|f32|f64|ptr[[:space:]]+as[[:space:]]+usize'
if rg -n --glob '*.rs' "$pattern" "${core_dirs[@]}"; then
  echo "forbidden API scan: FAIL" >&2
  exit 1
fi
echo "forbidden API scan: PASS"
