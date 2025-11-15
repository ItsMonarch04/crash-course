#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

# Sweep client concurrency and append one row per point to the checked-in trend
# CSV. The file shipped with a header and no producer for its whole life, which
# made it look like a record of measurements that had never been taken.
#
# The numbers are only meaningful with the environment caveat that
# docs/LIMITATIONS.md states: this is a closed-loop local model, not a
# production benchmark, and loopback replication latency is not represented.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

workload="${WORKLOAD:-A}"
ops="${OPS:-20000}"
concurrencies=(1 2 4 8 16)
trend="bench/results/perf-trend.csv"
stamp="${SOURCE_DATE:-$(date -u +%Y-%m-%d)}"

mkdir -p bench/results
if [[ ! -s "$trend" ]]; then
  echo "date,config_hash,workload,clients,throughput_ops_per_sec,p50_ns,p95_ns,p99_ns,p999_ns,max_ns" >"$trend"
fi

field() { # field <json> <dotted-key>
  node -e '
    const report = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
    const value = process.argv[2].split(".").reduce((node, key) => node?.[key], report);
    process.stdout.write(String(value));
  ' "$1" "$2"
}

echo "group-commit curve: workload=$workload ops=$ops points=${#concurrencies[@]}"
for clients in "${concurrencies[@]}"; do
  report="bench/results/curve-c${clients}.json"
  cargo run --release --quiet -p cc-bench -- \
    --workload "$workload" --clients "$clients" --ops "$ops" --output "$report"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$stamp" \
    "$(field "$report" config_hash)" \
    "$(field "$report" workload)" \
    "$clients" \
    "$(field "$report" throughput_ops_per_sec)" \
    "$(field "$report" latency_ns.p50)" \
    "$(field "$report" latency_ns.p95)" \
    "$(field "$report" latency_ns.p99)" \
    "$(field "$report" latency_ns.p999)" \
    "$(field "$report" latency_ns.max)" \
    >>"$trend"
  echo "  clients=${clients} throughput=$(field "$report" throughput_ops_per_sec) p50=$(field "$report" latency_ns.p50)ns p99=$(field "$report" latency_ns.p99)ns"
done

echo "group-commit curve: PASS rows appended to $trend"
