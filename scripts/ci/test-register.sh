#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
register="$repo_root/tests/test-register.tsv"

if [[ ! -f "$register" ]]; then
  echo "test register is missing: $register" >&2
  exit 1
fi

expected_header=$'id\tstatus\trequirement\ttest'
if [[ "$(head -n 1 "$register")" != "$expected_header" ]]; then
  echo "test register has an invalid header" >&2
  exit 1
fi

while IFS=$'\t' read -r id status requirement test extra; do
  [[ -z "$id" ]] && continue
  if [[ -n "${extra:-}" || -z "$status" || -z "$requirement" || -z "$test" ]]; then
    echo "malformed test-register row: $id" >&2
    exit 1
  fi
  case "$status" in
    planned) ;;
    implemented)
      if ! rg -q --glob '*.rs' "fn ${test}\\b" "$repo_root/crates" \
        && ! rg -qF --glob '*.ts' --glob '*.tsx' "test(\"${test}\"" "$repo_root/theater/tests"; then
        echo "implemented test is not present: $test" >&2
        exit 1
      fi
      ;;
    *)
      echo "unknown test-register status for $id: $status" >&2
      exit 1
      ;;
  esac
done < <(tail -n +2 "$register")

echo "test register: PASS"
