#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ "${1:-}" != "--file" || -z "${2:-}" ]]; then
  echo "usage: $0 --file HISTORY.tsv" >&2
  exit 2
fi
cargo run --quiet -p cc-swarm -- check-history --file "$2"
