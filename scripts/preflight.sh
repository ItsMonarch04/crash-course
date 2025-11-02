#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[1/7] version coherence"
node "$repo_root/scripts/check-version-coherence.mjs"
echo "[2/7] cargo fmt"
cargo fmt --all -- --check
echo "[3/7] cargo clippy"
cargo clippy --workspace --all-targets -- -D warnings
echo "[4/7] forbidden API scan"
"$repo_root/scripts/ci/forbidden-grep.sh"
echo "[5/7] tests"
cargo test --workspace --all-targets
echo "[6/7] determinism double-run"
preflight_dir="$repo_root/target/preflight"
mkdir -p "$preflight_dir"
cargo run --quiet -p cc-swarm -- --determinism > "$preflight_dir/trace-1.cctrace"
cargo run --quiet -p cc-swarm -- --determinism > "$preflight_dir/trace-2.cctrace"
cmp "$preflight_dir/trace-1.cctrace" "$preflight_dir/trace-2.cctrace"
echo "[7/7] deterministic self-check"
cargo run --quiet -p cc-swarm -- --selfcheck
echo "preflight: PASS"
