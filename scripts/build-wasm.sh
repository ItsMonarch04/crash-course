#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

command -v wasm-bindgen >/dev/null 2>&1 || {
  echo "build-wasm: wasm-bindgen CLI is required" >&2
  exit 2
}

cargo build --release -p cc-wasm --target wasm32-unknown-unknown
wasm_bindgen_dir="$repo_root/theater/public/wasm"
mkdir -p "$wasm_bindgen_dir"
wasm-bindgen \
  "target/wasm32-unknown-unknown/release/cc_wasm.wasm" \
  --target web \
  --out-dir "$wasm_bindgen_dir"
mkdir -p "$repo_root/theater/src/wasm"
cp "$wasm_bindgen_dir/cc_wasm.js" "$repo_root/theater/src/wasm/cc_wasm.js"
cp "$wasm_bindgen_dir/cc_wasm.d.ts" "$repo_root/theater/src/wasm/cc_wasm.d.ts"

wasm_bytes="$(wc -c < "$wasm_bindgen_dir/cc_wasm_bg.wasm" | tr -d ' ')"
gzip_bytes="$(gzip -c "$wasm_bindgen_dir/cc_wasm_bg.wasm" | wc -c | tr -d ' ')"
echo "wasm fixture: bytes=$wasm_bytes gzip_bytes=$gzip_bytes output=$wasm_bindgen_dir"
