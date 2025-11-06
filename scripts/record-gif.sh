#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "record-gif: ffmpeg is required; no placeholder GIF is generated" >&2
  exit 2
fi
if [[ ! -d theater/node_modules ]]; then
  echo "record-gif: install theater dependencies first with npm ci" >&2
  exit 2
fi

npm --prefix theater run build
npm --prefix theater run test:e2e
echo "record-gif: Playwright capture and ffmpeg assembly are intentionally operator-driven"
echo "record-gif: supply a capture directory and encode frames with ffmpeg -framerate 12"
