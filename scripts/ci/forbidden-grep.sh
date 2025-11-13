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
  "$root_dir/crates/cc-cluster" \
  "$root_dir/crates/cc-resp" \
  "$root_dir/crates/cc-checker"
