#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "${1:-}" != "--profile" || -z "${2:-}" || $# -gt 4 ]]; then
  echo "usage: scripts/calibrate.sh --profile NAME [--samples N]" >&2
  exit 64
fi
profile="$2"
samples=4
if [[ $# -eq 4 ]]; then
  if [[ "$3" != "--samples" || ! "$4" =~ ^[1-9][0-9]*$ ]]; then
    echo "--samples must be a positive integer" >&2
    exit 64
  fi
  samples="$4"
fi
cd "$repo_root"
python3 scripts/calibrate.py --profile "$profile" --samples "$samples"
