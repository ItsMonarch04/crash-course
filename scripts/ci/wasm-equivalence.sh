#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

"$repo_root/scripts/build-wasm.sh"
seed_count="${WASM_EQUIVALENCE_SEEDS:-100}"
equivalence_dir="$repo_root/target/wasm-equivalence"
mkdir -p "$equivalence_dir"
for ((seed = 0; seed < seed_count; seed += 1)); do
  seed_text="$(printf '0x%016x' "$seed")"
  cargo run --locked --quiet -p cc-wasm --example equivalence -- \
    --seed "$seed_text" --profile calm >"$equivalence_dir/native-${seed}.json"
  node "$repo_root/scripts/ci/run-wasm-equivalence.mjs" "$seed_text" calm \
    >"$equivalence_dir/browser-${seed}.json"
  cmp "$equivalence_dir/native-${seed}.json" "$equivalence_dir/browser-${seed}.json"
done
echo "wasm equivalence: PASS seeds=$seed_count"
