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
# The required-key list is owned by exhibits/schema.json. Deriving it here keeps the
# gate and the schema from drifting apart; they did, and `anomaly` went unchecked.
schema = json.load(open("exhibits/schema.json", encoding="utf-8"))
required = schema["properties"]["exhibits"]["items"]["required"]
assert manifest["schema_version"] == 2
assert manifest.get("synthetic") is not True
assert isinstance(manifest["build"], str) and manifest["build"]
for exhibit in manifest["exhibits"]:
    assert exhibit.get("synthetic") is not True
    for key in required:
        assert key in exhibit, key
print(f"museum manifest: PASS exhibits={len(manifest['exhibits'])}")
PY

cargo run --quiet -p cc-swarm -- regress
echo "museum replay: PASS"
