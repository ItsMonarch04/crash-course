#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

minimum_runs_per_second="${CCDB_MIN_SIM_RUNS_PER_SECOND:-3.34}"
output="$(cargo run --quiet --release -p cc-swarm -- run --profile rough --seeds 200 --jobs 1)"
printf '%s\n' "$output"
actual="$(printf '%s\n' "$output" | sed -n 's/.*runs_per_sec=\([0-9.]*\).*/\1/p')"
if [[ -z "$actual" ]]; then
  echo "sim-throughput: missing runs_per_sec" >&2
  exit 1
fi
awk -v actual="$actual" -v minimum="$minimum_runs_per_second" 'BEGIN { if (actual + 0 < minimum + 0) exit 1 }'
echo "sim-throughput: PASS actual=${actual}/s minimum=${minimum_runs_per_second}/s (200 runs/min/core)"
