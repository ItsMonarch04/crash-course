#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
usage() {
  echo "usage: $0 --producer-root DIR --out DIR --source-dir DIR [--ccdb PATH]" >&2
  exit 64
}

out=""
source_dir=""
producer_root=""
ccdb_bin=""
while (($#)); do
  case "$1" in
    --producer-root)
      (($# >= 2)) || usage
      producer_root="$2"
      shift 2
      ;;
    --out)
      (($# >= 2)) || usage
      out="$2"
      shift 2
      ;;
    --ccdb)
      (($# >= 2)) || usage
      ccdb_bin="$2"
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
[[ -n "$out" ]] || usage
[[ -n "$source_dir" ]] || usage
[[ -n "$producer_root" ]] || usage
[[ -d "$producer_root" ]] || { echo "producer source tree is absent: $producer_root" >&2; exit 1; }
[[ -f "$producer_root/crates/cc-swarm/examples/c0_fixtures.rs" ]] || {
  echo "producer tree lacks the C0 encoder harness; copy it from $repo_root before running" >&2
  exit 1
}
[[ -f "$producer_root/crates/cc-node/src/c0_identity_fixtures.rs" ]] || {
  echo "producer tree lacks the C0 identity harness; copy it from $repo_root before running" >&2
  exit 1
}
grep -q '^mod c0_identity_fixtures;' "$producer_root/crates/cc-node/src/main.rs" || {
  echo "producer main.rs has not enabled the test-only C0 identity harness" >&2
  exit 1
}
[[ -n "$ccdb_bin" ]] || ccdb_bin="$producer_root/target/release/ccdb"
[[ ! -e "$out" ]] || { echo "refusing to overwrite fixture output: $out" >&2; exit 1; }
[[ ! -e "$source_dir" ]] || { echo "refusing to reuse C0 source directory: $source_dir" >&2; exit 1; }
[[ -x "$ccdb_bin" ]] || { echo "ccdb binary is not executable: $ccdb_bin" >&2; exit 1; }

mkdir -p "$out"
(
  cd "$producer_root"
  cargo run --offline --locked --quiet -p cc-swarm --example c0_fixtures -- --out "$out"
  CC_C0_CCID_OUT="$out" cargo test --offline --locked --quiet -p cc-node emit_c0_identity_fixtures_when_requested
)

backup="$out/ccbk-v1.bin"
cluster_id="31313131313131313131313131313131"
"$ccdb_bin" init --cluster c0 --cluster-id "$cluster_id" --node-id 1 --data-dir "$source_dir" >/dev/null
# CCBK v1 embeds ccdb.toml.  A relative logical data directory keeps the
# compatibility fixture independent of the temporary generation path.
printf '[node]\nid = 1\ncluster_id = "%s"\ndata_dir = "."\nlisten_client = "127.0.0.1:7101"\nlisten_peer = "127.0.0.1:7201"\nlisten_metrics = "127.0.0.1:7301"\npeer_nodes = "127.0.0.1:7201"\n\n[storage]\nfsync = "always"\n' \
  "$cluster_id" > "$source_dir/ccdb.toml"
[[ -f "$source_dir/identity.ccid" && -f "$source_dir/ccdb.toml" ]] || { echo "invalid C0 source directory: $source_dir" >&2; exit 1; }
"$ccdb_bin" admin backup --data-dir "$source_dir" --output "$backup" >/dev/null
printf '%s\n' \
  'reader_test=trap_legacy_backup_is_explicitly_refused' \
  'format=CCBK' \
  'semantic=compatibility-cut node-clone archive retained for the explicit v1 importer' > "$out/ccbk-v1.txt"
