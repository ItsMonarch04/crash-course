#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

mkdir -p bench/results
cargo run --release -p cc-bench -- --workload A --clients 1 --ops 10000 --output bench/results/perf-smoke.json
test -s bench/results/perf-smoke.json
echo "perf-smoke: PASS"
