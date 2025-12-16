#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

# Sweep client concurrency and append one fully attributed row per point to the
# checked-in trend CSV. Each row retains the workload shape and environment
# caveat from the source report.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

workload="${WORKLOAD:-A}"
ops="${OPS:-20000}"
concurrencies=(1 2 4 8 16)
trend="bench/results/perf-trend.csv"
stamp="${SOURCE_DATE:-$(date -u +%Y-%m-%d)}"
header="date,config_hash,workload,clients,ops,repetitions,seed,value_bytes,environment_os,environment_arch,environment_note,throughput_ops_per_sec,p50_ns,p95_ns,p99_ns,p999_ns,max_ns"

mkdir -p bench/results
if [[ ! -s "$trend" ]]; then
  echo "$header" >"$trend"
elif [[ "$(head -n 1 "$trend")" != "$header" ]]; then
  echo "group-commit curve: incompatible CSV header in $trend" >&2
  exit 1
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
  cargo run --locked --release --quiet -p cc-bench -- \
    --workload "$workload" --clients "$clients" --ops "$ops" --output "$report"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$stamp" \
    "$(field "$report" config_hash)" \
    "$(field "$report" workload)" \
    "$clients" \
    "$(field "$report" ops)" \
    "$(field "$report" repetitions)" \
    "$(field "$report" seed)" \
    "$(field "$report" value_bytes)" \
    "$(field "$report" environment.os)" \
    "$(field "$report" environment.arch)" \
    "$(field "$report" environment.note)" \
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
