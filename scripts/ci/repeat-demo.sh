#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
runs=50

if [[ $# -gt 0 ]]; then
  if [[ $# -ne 2 || "$1" != "--runs" || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
    echo "usage: $0 [--runs POSITIVE_COUNT]" >&2
    exit 64
  fi
  runs="$2"
fi

last_log="$(mktemp "${TMPDIR:-/tmp}/ccdb-demo-repeat.XXXXXX")"
cleanup() {
  rm -f "$last_log"
}
trap cleanup EXIT INT TERM

for ((run = 1; run <= runs; run += 1)); do
  if ! "$repo_root/scripts/demo.sh" >"$last_log" 2>&1; then
    cat "$last_log"
    echo "repeat-demo: FAIL run=${run}/${runs}" >&2
    exit 1
  fi
  printf 'repeat-demo: PASS run=%s/%s\n' "$run" "$runs"
done

printf 'repeat-demo: PASS all_runs=%s\n' "$runs"
