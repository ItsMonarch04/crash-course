#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

python3 - exhibits/manifest.json <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1], encoding="utf-8"))
assert manifest["schema_version"] == 1
assert isinstance(manifest["build"], str) and manifest["build"]
for exhibit in manifest["exhibits"]:
    for key in ("id", "title", "kind", "seed", "trace", "verdict", "chapters"):
        assert key in exhibit, key
print(f"museum manifest: PASS exhibits={len(manifest['exhibits'])}")
PY

cargo run --quiet -p cc-swarm -- regress
echo "museum replay: PASS"
