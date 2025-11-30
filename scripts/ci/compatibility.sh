#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compat_commit="2154f1688231ecfc4de7a50a7899a90c327f844b"
compat_source="$(mktemp -d "${TMPDIR:-/tmp}/cc-compat-source.XXXXXX")"
cleanup() {
  rm -rf "$compat_source"
}
trap cleanup EXIT HUP INT TERM

cd "$repo_root"
bash scripts/ci/golden-manifest.sh --check
node scripts/ci/golden-mutation.mjs

git archive "$compat_commit" | tar -x -C "$compat_source"
CARGO_TARGET_DIR="$repo_root/target/compat-base" \
  cargo build --locked --manifest-path "$compat_source/Cargo.toml" -p cc-node --quiet
compat_binary="$repo_root/target/compat-base/debug/ccdb"
test -x "$compat_binary"

cargo test --locked -p cc-node --bin ccdb --quiet
cargo test --locked -p cc-env trap_mixed_build_negotiates_semantic_v2 --quiet
CC_COMPAT_CCDB="$compat_binary" \
  cargo test --locked -p cc-node --test rolling_upgrade \
    trap_real_rolling_upgrade_keeps_every_ack -- --ignored

echo "compatibility: PASS cut=$compat_commit"
