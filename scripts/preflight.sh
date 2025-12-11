#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[1/15] version coherence"
node "$repo_root/scripts/check-version-coherence.mjs"
echo "[2/15] documentation coherence"
node "$repo_root/scripts/ci/doc-coherence.mjs"
echo "[3/15] campaign profile coverage"
node "$repo_root/scripts/ci/campaign-coverage.mjs"
echo "[4/15] golden manifest schema"
bash "$repo_root/scripts/ci/golden-manifest.sh" --check
echo "[5/15] fuzz inventory and corpus"
bash "$repo_root/scripts/ci/fuzz-inventory.sh" --check
echo "[6/15] theater control contract"
node "$repo_root/scripts/ci/control-contract.mjs"
echo "[7/15] theater contrast"
node "$repo_root/scripts/ci/contrast.mjs"
node "$repo_root/scripts/ci/contrast-fixture.mjs"
echo "[8/15] cargo fmt"
cargo fmt --all -- --check
echo "[9/15] cargo clippy"
cargo clippy --workspace --all-targets -- -D warnings
echo "[10/15] test register"
bash "$repo_root/scripts/ci/test-register.sh"
echo "[11/15] kata feature matrix"
bash "$repo_root/scripts/ci/kata-matrix.sh"
echo "[12/15] forbidden API scan"
"$repo_root/scripts/ci/forbidden-grep.sh"
echo "[13/15] tests"
cargo test --workspace --all-targets
echo "[14/15] determinism double-run"
preflight_dir="$repo_root/target/preflight"
mkdir -p "$preflight_dir"
cargo run --quiet -p cc-swarm -- --determinism > "$preflight_dir/trace-1.cctrace"
cargo run --quiet -p cc-swarm -- --determinism > "$preflight_dir/trace-2.cctrace"
cmp "$preflight_dir/trace-1.cctrace" "$preflight_dir/trace-2.cctrace"
echo "[15/15] deterministic self-check"
cargo run --quiet -p cc-swarm -- --selfcheck
echo "preflight: PASS"
