#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

python3 - exhibits/manifest.json theater/public/exhibits/manifest.json <<'PY'
import json
import sys

# The required-key list is owned by exhibits/schema.json. Deriving it here keeps the
# gate and the schema from drifting apart; they did, and `anomaly` went unchecked.
schema = json.load(open("exhibits/schema.json", encoding="utf-8"))
required = schema["properties"]["exhibits"]["items"]["required"]


def check(path):
    manifest = json.load(open(path, encoding="utf-8"))
    assert manifest["schema_version"] == 2, path
    assert manifest.get("synthetic") is not True, path
    assert isinstance(manifest["build"], str) and manifest["build"], path
    for exhibit in manifest["exhibits"]:
        assert exhibit.get("synthetic") is not True, path
        for key in required:
            assert key in exhibit, f"{path}: {key}"
    return manifest


# Both copies, not just the source one. `theater/public/exhibits/manifest.json`
# is the file the published site actually fetches, so validating only the root
# copy would leave the visitor-facing catalogue ungated — and an exhibit added
# to one and not the other would either ship unchecked or never appear.
source, served = (check(path) for path in sys.argv[1:3])
assert source["exhibits"] == served["exhibits"], (
    "exhibits/manifest.json and theater/public/exhibits/manifest.json disagree"
)
print(f"museum manifest: PASS exhibits={len(source['exhibits'])} copies=2")
PY

cargo run --quiet -p cc-swarm -- regress
echo "museum replay: PASS"
