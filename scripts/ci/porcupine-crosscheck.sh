#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

cross_dir="$(mktemp -d "${TMPDIR:-/tmp}/ccdb-porcupine.XXXXXX")"
cleanup() { rm -rf "$cross_dir"; }
trap cleanup EXIT INT TERM

# Cross-validation only means something on histories the simulator actually
# produced, so export real runs across several profiles rather than checking a
# hand-written sample.
cargo build --quiet --release -p cc-swarm

total_operations=0
for spec in "0x0000000000000005:calm" "0x0000000000000071:membership" "0x00000000000000a3:rough"; do
  seed="${spec%%:*}"
  profile="${spec##*:}"
  tsv="$cross_dir/$profile-$seed.tsv"
  json="$cross_dir/$profile-$seed.json"

  ./target/release/cc-swarm one --seed "$seed" --profile "$profile" --export-history "$tsv" >/dev/null
  ./target/release/cc-swarm check-history --file "$tsv"
  scripts/export-porcupine.sh --file "$tsv" --output "$json"

  operations="$(node - "$json" <<'NODE'
const fs = require("node:fs");
const events = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
if (!Array.isArray(events) || events.length === 0) throw new Error("empty Porcupine export");
if (events.length % 2 !== 0) throw new Error("every operation needs an invoke and a completion");
const open = new Map();
for (const event of events) {
  if (!Number.isInteger(event.process) || !["invoke", "ok", "fail"].includes(event.type)) {
    throw new Error(`invalid Porcupine event: ${JSON.stringify(event)}`);
  }
  if (!Number.isInteger(event.time) || !Object.hasOwn(event, "value")) {
    throw new Error(`incomplete Porcupine event: ${JSON.stringify(event)}`);
  }
  // Porcupine pairs each invoke with the next completion on the same process.
  if (event.type === "invoke") {
    if (open.get(event.process)) throw new Error(`process ${event.process} invoked twice`);
    open.set(event.process, true);
  } else {
    if (!open.get(event.process)) throw new Error(`process ${event.process} completed without invoke`);
    open.set(event.process, false);
  }
}
for (const [process, pending] of open) {
  if (pending) throw new Error(`process ${process} never completed`);
}
console.log(events.length / 2);
NODE
)"
  total_operations=$((total_operations + operations))
  echo "porcupine shape: PASS profile=$profile seed=$seed operations=$operations"
done

if [[ "$total_operations" -lt 1 ]]; then
  echo "porcupine cross-validation: FAIL no real operations were exported" >&2
  exit 1
fi

if [[ -n "${PORCUPINE_COMMAND:-}" ]]; then
  # The owner may point this gate at a pinned Porcupine binary in CI. Any
  # disagreement between the two checkers is release-blocking for one of them.
  for json in "$cross_dir"/*.json; do
    sh -c "$PORCUPINE_COMMAND '$json'"
  done
  echo "porcupine cross-validation: PASS external command operations=$total_operations"
else
  echo "porcupine cross-validation: PENDING external Porcupine command (set PORCUPINE_COMMAND in CI); real-history shape gate PASS operations=$total_operations"
fi
