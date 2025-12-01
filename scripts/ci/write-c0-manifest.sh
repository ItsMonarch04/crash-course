#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
usage() {
  echo "usage: $0 --producer-commit HASH --fixture-dir tests/golden/DIR" >&2
  exit 64
}

producer_commit=""
fixture_dir=""
while (($#)); do
  case "$1" in
    --producer-commit)
      (($# >= 2)) || usage
      producer_commit="$2"
      shift 2
      ;;
    --fixture-dir)
      (($# >= 2)) || usage
      fixture_dir="$2"
      shift 2
      ;;
    *) usage ;;
  esac
done
[[ "$producer_commit" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$fixture_dir" == tests/golden/* ]] || usage
[[ -d "$repo_root/$fixture_dir" ]] || { echo "fixture directory is absent: $fixture_dir" >&2; exit 1; }

manifest="$repo_root/tests/golden/manifest.tsv"
producer_build="$(sed -n 's/^version = "\([0-9][^"]*\)"/\1/p' "$repo_root/Cargo.toml" | head -n 1)"
tmp_manifest="$(mktemp "$repo_root/tests/golden/.manifest.XXXXXX")"
trap 'rm -f "$tmp_manifest"' EXIT

printf '%s\n' $'format\tnamespace\tversion\tproducer_commit\tproducer_build\tfixture\texpected_value\tsha256\tcurrent_reader\told_reader_policy\tmigration' > "$tmp_manifest"
while IFS=$'\t' read -r format namespace version name current_reader policy migration; do
  fixture="$fixture_dir/$name.bin"
  sidecar="$fixture_dir/$name.txt"
  [[ -f "$repo_root/$fixture" && -f "$repo_root/$sidecar" ]] || { echo "missing generated C0 fixture: $name" >&2; exit 1; }
  sha256="$(shasum -a 256 "$repo_root/$fixture" | awk '{print $1}')"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$format" "$namespace" "$version" "$producer_commit" "$producer_build" "$fixture" "$sidecar" "$sha256" "$current_reader" "$policy" "$migration" >> "$tmp_manifest"
done <<'ROWS'
CCAR	semantic	1	ccar-v1	must-read	self-read	none
CCAP	semantic	1	ccap-v1	must-read	self-read	none
CCBK	storage	1	ccbk-v1	must-reject	legacy-import-required	legacy-node-backup
CCCF	semantic	1	cccf-v1	must-read	self-read	none
CCEI	diagnostic	1	ccei-v1	must-read	self-read	none
CCEO	diagnostic	1	cceo-v1	must-read	self-read	none
CCHL	transport	1	cchl-v1	must-read	self-read	none
CCHY	semantic	2	cchy-v2	must-read	self-read	none
CCID	storage	1	ccid-v1	must-read	self-read	fresh-node-identity-only
CCID	storage	1	ccid-joining-v1	must-read	self-read	fresh-node-identity-only
CCID	storage	1	ccid-removed-v1	must-read	self-read	fresh-node-identity-only
CCIJ	diagnostic	1	ccij-v1	must-read	self-read	none
CCKR	semantic	1	cckr-v1	must-read	self-read	none
CCKV	semantic	1	cckv-v1	must-read	self-read	none
CCLR	storage	1	cclr-v1	must-read	self-read	none
CCLR	storage	1	cclr-hard-v1	must-read	self-read	none
CCLR	storage	1	cclr-append-v1	must-read	self-read	none
CCLR	storage	1	cclr-truncate-v1	must-read	self-read	none
CCLR	storage	1	cclr-snapshot-mark-v1	must-read	self-read	none
CCMT	storage	1	ccmt-v1	must-read	self-read	legacy-store-reader
CCMS	semantic	1	ccms-v1	must-read	self-read	none
CCPF	transport	1	ccpf-v1	must-read	self-read	none
CCPL	semantic	1	ccpl-v1	must-read	self-read	none
CCRP	transport	1	ccrp-v1	must-read	self-read	none
CCST	storage	1	ccst-v1	must-read	self-read	legacy-store-reader
CCTR	diagnostic	1	cctr-v1	must-read	self-read	none
CCWL	storage	1	ccwl-v1	must-read	self-read	legacy-wal-reader
ROWS
if [[ -d "$repo_root/tests/golden/legacy" ]]; then
  legacy_commit=2c733f3d8765fe12d02b2af8fbbe67afc19f898b
  while IFS=$'\t' read -r format namespace version name current_reader policy migration; do
    fixture="tests/golden/legacy/$name.bin"
    sidecar="tests/golden/legacy/$name.txt"
    [[ -f "$repo_root/$fixture" && -f "$repo_root/$sidecar" ]] || { echo "missing generated legacy fixture: $name" >&2; exit 1; }
    sha256="$(shasum -a 256 "$repo_root/$fixture" | awk '{print $1}')"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$format" "$namespace" "$version" "$legacy_commit" "0.11.0" "$fixture" "$sidecar" "$sha256" "$current_reader" "$policy" "$migration" >> "$tmp_manifest"
  done <<'LEGACY_ROWS'
CCBK	storage	1	ccbk-v1	must-reject	must-read	pre-n1-node-clone-refused
CCHY	semantic	1	cchy-v1	must-read	must-read	legacy-history-v1-reader
CCKV	semantic	1	cckv-v1	must-read	must-read	legacy-command-v1-reader
CCMT	storage	1	ccmt-v1	must-read	must-read	legacy-store-meta-v1-reader
CCPF	transport	1	ccpf-v1	must-read	must-read	legacy-peer-frame-v1-reader
CCST	storage	1	ccst-v1	must-read	must-read	legacy-store-table-v1-reader
CCTR	diagnostic	1	cctr-v1	must-read	must-read	legacy-trace-reader
CCWL	storage	1	ccwl-v1	must-read	must-read	legacy-wal-v1-reader
LEGACY_ROWS
fi
{
  head -n 1 "$tmp_manifest"
  tail -n +2 "$tmp_manifest" | LC_ALL=C sort -t $'\t' -k1,1 -k2,2 -k3,3n -k6,6
} > "$manifest"
