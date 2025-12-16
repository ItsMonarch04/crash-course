#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

# Runs after `build-wasm.sh` and `npm run build`; both artifacts are mandatory
# inputs to the size calculation.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
assets_dir="$repo_root/theater/dist/assets"
if [[ ! -d "$assets_dir" ]]; then
  echo "theater size budget: FAIL $assets_dir does not exist; run npm run build" >&2
  exit 1
fi

# `cat | wc -c` sums every matching chunk without relying on batched subtotals.
bundle_bytes="$(find "$assets_dir" -type f \( -name '*.js' -o -name '*.css' \) -exec cat {} + | wc -c | tr -d ' ')"
if (( bundle_bytes == 0 )); then
  echo "theater size budget: FAIL no .js or .css assets under $assets_dir" >&2
  exit 1
fi
if (( bundle_bytes > 300000 )); then
  echo "theater size budget: FAIL ${bundle_bytes} bytes > 300000" >&2
  exit 1
fi
echo "theater size budget: PASS ${bundle_bytes} bytes <= 300000"

wasm_path="$repo_root/theater/public/wasm/cc_wasm_bg.wasm"
if [[ ! -s "$wasm_path" ]]; then
  echo "wasm size budget: FAIL $wasm_path is missing or empty; run scripts/build-wasm.sh" >&2
  exit 1
fi
wasm_gzip_bytes="$(gzip -c "$wasm_path" | wc -c | tr -d ' ')"
if (( wasm_gzip_bytes > 2500000 )); then
  echo "wasm size budget: FAIL ${wasm_gzip_bytes} gzip bytes > 2500000" >&2
  exit 1
fi
echo "wasm size budget: PASS ${wasm_gzip_bytes} gzip bytes <= 2500000"
