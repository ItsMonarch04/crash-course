#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[1/13] version coherence"
node "$repo_root/scripts/check-version-coherence.mjs"
echo "[2/13] documentation coherence"
node "$repo_root/scripts/ci/doc-coherence.mjs"
echo "[3/13] golden manifest schema"
bash "$repo_root/scripts/ci/golden-manifest.sh" --check
echo "[4/13] fuzz inventory and corpus"
bash "$repo_root/scripts/ci/fuzz-inventory.sh" --check
echo "[5/13] theater control contract"
node "$repo_root/scripts/ci/control-contract.mjs"
echo "[6/13] theater contrast"
node "$repo_root/scripts/ci/contrast.mjs"
node "$repo_root/scripts/ci/contrast-fixture.mjs"
echo "[7/13] cargo fmt"
cargo fmt --all -- --check
echo "[8/13] cargo clippy"
cargo clippy --workspace --all-targets -- -D warnings
echo "[9/13] test register"
bash "$repo_root/scripts/ci/test-register.sh"
echo "[10/13] forbidden API scan"
"$repo_root/scripts/ci/forbidden-grep.sh"
echo "[11/13] tests"
cargo test --workspace --all-targets
echo "[12/13] determinism double-run"
preflight_dir="$repo_root/target/preflight"
mkdir -p "$preflight_dir"
cargo run --quiet -p cc-swarm -- --determinism > "$preflight_dir/trace-1.cctrace"
cargo run --quiet -p cc-swarm -- --determinism > "$preflight_dir/trace-2.cctrace"
cmp "$preflight_dir/trace-1.cctrace" "$preflight_dir/trace-2.cctrace"
echo "[13/13] deterministic self-check"
cargo run --quiet -p cc-swarm -- --selfcheck
echo "preflight: PASS"
