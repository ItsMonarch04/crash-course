#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runs="${CC_MEMBERSHIP_RUNS:-20}"
if [[ ! "$runs" =~ ^([1-9]|1[0-9]|20)$ ]]; then
  echo "CC_MEMBERSHIP_RUNS must be an integer from 1 through 20" >&2
  exit 64
fi

cd "$repo_root"
CC_MEMBERSHIP_RUNS="$runs" cargo test --locked -p cc-node --test real_cluster \
  trap_real_membership_demo_3_to_5 -- --ignored --nocapture
