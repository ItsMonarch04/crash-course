#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
bundle_bytes="$(find "$repo_root/theater/dist/assets" -type f \( -name '*.js' -o -name '*.css' \) -print0 | xargs -0 wc -c | tail -1 | awk '{print $1}')"
if (( bundle_bytes > 300000 )); then
  echo "theater size budget: FAIL ${bundle_bytes} bytes > 300000" >&2
  exit 1
fi
echo "theater size budget: PASS ${bundle_bytes} bytes <= 300000"
