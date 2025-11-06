#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

duration_seconds=10
soak_hours=0
while (($#)); do
  case "$1" in
    --duration-seconds) duration_seconds="$2"; shift 2 ;;
    --soak-hours) soak_hours="$2"; shift 2 ;;
    *) echo "usage: $0 [--duration-seconds N] [--soak-hours N]" >&2; exit 2 ;;
  esac
done

if (( soak_hours > 0 )); then
  duration_seconds=$((soak_hours * 60 * 60))
fi

echo "real-faults: userspace restart harness"
echo "duration_seconds=$duration_seconds"
echo "faults=process-kill,restart,resp-replay,peer-frame-checksum"
echo "proxy=python3 scripts/resp-proxy.py --drop-every N --delay-ms N"
echo "The harness does not claim kernel-truth: disk/page-cache campaigns remain in cc-sim and the WAL gate."
if (( duration_seconds > 300 )); then
  echo "long soak requested; run this command under the owner-controlled campaign environment"
  exit 0
fi

"$repo_root/scripts/demo.sh"
echo "real-faults: PASS (bounded restart/recovery smoke); long soak is a pending gate"
