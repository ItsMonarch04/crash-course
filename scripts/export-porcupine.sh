#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ "${1:-}" != "--file" || -z "${2:-}" ]]; then
  echo "usage: $0 --file HISTORY.tsv [--output PORCUPINE.json]" >&2
  exit 2
fi

history_file="$2"
shift 2
args=(export-porcupine --file "$history_file")
if [[ "${1:-}" == "--output" && -n "${2:-}" ]]; then
  args+=(--output "$2")
  shift 2
fi
if (($#)); then
  echo "usage: $0 --file HISTORY.tsv [--output PORCUPINE.json]" >&2
  exit 2
fi
cargo run --quiet -p cc-swarm -- "${args[@]}"
