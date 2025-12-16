#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

usage() {
  echo "usage: $0 --legacy-root DIR --out DIR --source-dir DIR" >&2
  exit 64
}

legacy_root=""
out=""
source_dir=""
while (($#)); do
  case "$1" in
    --legacy-root)
      (($# >= 2)) || usage
      legacy_root="$2"
      shift 2
      ;;
    --out)
      (($# >= 2)) || usage
      out="$2"
      shift 2
      ;;
    --source-dir)
      (($# >= 2)) || usage
      source_dir="$2"
      shift 2
      ;;
    *) usage ;;
  esac
done
[[ -n "$legacy_root" && -n "$out" && -n "$source_dir" ]] || usage
[[ ! -e "$out" ]] || { echo "refusing to overwrite fixture output: $out" >&2; exit 1; }
swarm="$legacy_root/target/debug/cc-swarm"
ccdb="$legacy_root/target/debug/ccdb"
[[ -x "$swarm" && -x "$ccdb" ]] || { echo "legacy binaries are absent beneath $legacy_root" >&2; exit 1; }

mkdir -p "$out"
(
  cd "$legacy_root"
  cargo run --offline --locked --quiet -p cc-swarm --example c0_legacy_fixtures -- --out "$out"
)
"$swarm" --determinism > "$out/cctr-v1.bin"
"$swarm" one --seed 0x51 --profile calm --export-history "$out/cchy-v1.bin" >/dev/null
if [[ ! -e "$source_dir" ]]; then
  "$ccdb" init --cluster legacy-c0 --nodes 1 --base-dir "$source_dir" >/dev/null
fi
[[ -f "$source_dir/n1/node.json" && -f "$source_dir/n1/ccdb.toml" ]] || { echo "invalid legacy source directory: $source_dir" >&2; exit 1; }
"$ccdb" admin backup --data-dir "$source_dir/n1" --output "$out/ccbk-v1.bin" >/dev/null
printf '%s\n' \
  'reader_test=trap_trace_reads_legacy_cctr_fixture' \
  'format=CCTR' \
  'semantic=legacy deterministic trace emitted by 2c733f3' > "$out/cctr-v1.txt"
printf '%s\n' \
  'reader_test=trap_history_reads_legacy_v1_fixture' \
  'format=CCHY' \
  'semantic=legacy CC-HISTORY v1 text receipt emitted by 2c733f3' > "$out/cchy-v1.txt"
printf '%s\n' \
  'reader_test=trap_legacy_backup_is_explicitly_refused' \
  'format=CCBK' \
  'semantic=legacy node-clone archive is refused without target mutation' > "$out/ccbk-v1.txt"
