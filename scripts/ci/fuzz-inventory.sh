#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
inventory="$repo_root/fuzz/inventory.tsv"
manifest="$repo_root/fuzz/corpus/manifest.tsv"

[[ "${1:-}" == "--check" ]] || {
  echo "usage: scripts/ci/fuzz-inventory.sh --check" >&2
  exit 2
}

expected_inventory=$'format\towner\tdecoder\tversion\tmax_input_bytes\tmax_declared_count\tallocation_budget_bytes\twork_budget'
expected_manifest=$'format\tpath\tcontent_hash\texpected\tsignature\tbudget'
[[ "$(head -n 1 "$inventory")" == "$expected_inventory" ]] || {
  echo "invalid fuzz inventory header" >&2
  exit 1
}
[[ "$(head -n 1 "$manifest")" == "$expected_manifest" ]] || {
  echo "invalid fuzz corpus manifest header" >&2
  exit 1
}

while IFS=$'\t' read -r format owner decoder version max_input max_count allocation work; do
  [[ -n "$format" && -n "$owner" && -n "$decoder" && -n "$version" ]] || exit 1
  [[ "$max_input" =~ ^[0-9]+$ && "$max_count" =~ ^[0-9]+$ && "$allocation" =~ ^[0-9]+$ && "$work" =~ ^[0-9]+$ ]] || {
    echo "invalid numeric fuzz budget for $format" >&2
    exit 1
  }
  directory="$repo_root/fuzz/corpus/$format"
  [[ -d "$directory" ]] || { echo "missing corpus directory for $format" >&2; exit 1; }
  count=$(find "$directory" -type f -name '*.bin' | wc -l | tr -d ' ')
  (( count >= 1 && count <= 64 )) || { echo "invalid corpus count for $format: $count" >&2; exit 1; }
  while IFS= read -r case_path; do
    size=$(wc -c < "$case_path" | tr -d ' ')
    (( size <= 262144 && size <= max_input && size <= allocation )) || {
      echo "corpus case exceeds budget: $case_path" >&2
      exit 1
    }
    expected_name=$(basename "$case_path" .bin)
    actual_name=$(shasum -a 256 "$case_path" | awk '{print $1}')
    relative_path=${case_path#"$repo_root/"}
    manifest_hash=$(awk -F '\t' -v path="$relative_path" '$2 == path { print $3; exit }' "$manifest")
    [[ "$expected_name" == "$actual_name" || ( ${#expected_name} -eq 16 && "$expected_name" == "$manifest_hash" ) ]] || {
      echo "corpus filename hash mismatch: $case_path" >&2
      exit 1
    }
  done < <(find "$directory" -type f -name '*.bin' | sort)
  grep -q "^${format}"$'\t' "$manifest" || {
    echo "missing corpus manifest row for $format" >&2
    exit 1
  }
done < <(tail -n +2 "$inventory")

while IFS=$'\t' read -r format path content_hash expected signature budget; do
  [[ "$expected" == "ok" || "$expected" == "typed-error" ]] || exit 1
  [[ "$signature" == "ok" || "$signature" == "typed-error" ]] || exit 1
  [[ "$content_hash" =~ ^[0-9a-f]{16}$ && "$budget" =~ ^[0-9]+$ ]] || exit 1
  [[ -f "$repo_root/$path" ]] || { echo "missing manifest case: $path" >&2; exit 1; }
done < <(tail -n +2 "$manifest")

echo "fuzz inventory: PASS formats=$(($(wc -l < "$inventory") - 1))"
