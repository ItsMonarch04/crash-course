#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
manifest="$repo_root/tests/golden/manifest.tsv"
register="$repo_root/tests/test-register.tsv"
expected_header=$'format\tnamespace\tversion\tproducer_commit\tproducer_build\tfixture\texpected_value\tsha256\tcurrent_reader\told_reader_policy\tmigration'

allow_empty=false
if [[ "${1:-}" != "--check" || $# -gt 2 || ( $# -eq 2 && "${2:-}" != "--allow-empty" ) ]]; then
  echo "usage: $0 --check [--allow-empty]" >&2
  exit 64
fi
if [[ "${2:-}" == "--allow-empty" ]]; then
  allow_empty=true
fi
if [[ ! -f "$manifest" ]]; then
  echo "golden manifest is missing: $manifest" >&2
  exit 1
fi
if [[ "$(head -n 1 "$manifest")" != "$expected_header" ]]; then
  echo "golden manifest has an invalid header" >&2
  exit 1
fi
if rg -n $'\r' "$manifest"; then
  echo "golden manifest must use LF line endings" >&2
  exit 1
fi

previous_key=""
row_count=0
while IFS=$'\t' read -r format namespace version producer_commit producer_build fixture expected_value sha256 current_reader old_reader_policy migration extra; do
  row_count=$((row_count + 1))
  if [[ -n "${extra:-}" || -z "$format" || -z "$namespace" || -z "$version" || -z "$producer_commit" || -z "$producer_build" || -z "$fixture" || -z "$expected_value" || -z "$sha256" || -z "$current_reader" || -z "$old_reader_policy" || -z "$migration" ]]; then
    echo "malformed golden manifest row: $row_count" >&2
    exit 1
  fi
  if [[ ! "$format" =~ ^[A-Z][A-Z0-9-]*$ || ! "$namespace" =~ ^(storage|transport|semantic|diagnostic)$ || ! "$version" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid format identity at golden manifest row: $row_count" >&2
    exit 1
  fi
  if [[ ! "$producer_commit" =~ ^[0-9a-f]{40}$ || ! "$sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid immutable hash at golden manifest row: $row_count" >&2
    exit 1
  fi
  if [[ "$current_reader" != "must-read" && "$current_reader" != "must-reject" ]]; then
    echo "invalid current-reader policy at golden manifest row: $row_count" >&2
    exit 1
  fi
  if [[ "$fixture" != tests/golden/* || "$expected_value" != tests/golden/* ]]; then
    echo "golden manifest paths must stay beneath tests/golden at row: $row_count" >&2
    exit 1
  fi
  key="$format"$'\t'"$namespace"$'\t'"$version"$'\t'"$fixture"
  if [[ -n "$previous_key" && "$key" < "$previous_key" ]]; then
    echo "golden manifest rows are not sorted at row: $row_count" >&2
    exit 1
  fi
  if [[ "$key" == "$previous_key" ]]; then
    echo "duplicate golden manifest fixture at row: $row_count" >&2
    exit 1
  fi
  previous_key="$key"
  if [[ ! -f "$repo_root/$fixture" || ! -f "$repo_root/$expected_value" ]]; then
    echo "golden manifest fixture or sidecar is missing at row: $row_count" >&2
    exit 1
  fi
  actual_sha256="$(shasum -a 256 "$repo_root/$fixture" | awk '{print $1}')"
  if [[ "$actual_sha256" != "$sha256" ]]; then
    echo "golden manifest fixture hash mismatch at row: $row_count" >&2
    exit 1
  fi
  reader_test="$(sed -n 's/^reader_test=//p' "$repo_root/$expected_value" | head -n 1)"
  if [[ -z "$reader_test" || ! "$reader_test" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "golden sidecar has no named reader_test at row: $row_count" >&2
    exit 1
  fi
  if ! rg -q --glob '*.rs' "fn ${reader_test}\\b" "$repo_root/crates" \
    && ! rg -qF --glob '*.ts' --glob '*.tsx' "test(\"${reader_test}\"" "$repo_root/theater/tests"; then
    echo "golden sidecar reader_test is not present at row: $row_count" >&2
    exit 1
  fi
  if ! rg -qF "$reader_test" "$register"; then
    echo "golden sidecar reader_test is not owned by the test register at row: $row_count" >&2
    exit 1
  fi
done < <(tail -n +2 "$manifest")

if (( row_count == 0 )); then
  if "$allow_empty"; then
    echo "golden manifest: PASS schema-only rows=0"
    exit 0
  fi
  echo "golden manifest has no compatibility rows; use --allow-empty only before the compatibility cut" >&2
  exit 1
fi

# A closed compatibility cut must provide one row for every format currently
# documented as versioned. The manifest may have additional legacy rows, but
# it cannot silently omit a format merely because its current writer/reader
# happens to round-trip in one source tree.
while IFS= read -r documented_format; do
  if ! rg -q "^${documented_format}"$'\t' "$manifest"; then
    echo "documented format has no golden manifest row: $documented_format" >&2
    exit 1
  fi
done < <(
  awk -F '|' '
    $3 ~ /^ `[A-Z][A-Z0-9-]*` $/ && $4 ~ /^ [1-9][0-9]* $/ {
      value = $3
      gsub(/^ `|` $/, "", value)
      print value
    }
  ' "$repo_root/docs/formats.md"
)

echo "golden manifest: PASS rows=$row_count"
