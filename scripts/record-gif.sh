#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

output_path="${1:-$repo_root/theater/public/crash-course.gif}"
if [[ ! -d theater/node_modules ]]; then
  echo "record-gif: install theater dependencies first with npm ci" >&2
  exit 2
fi

capture_dir="$(mktemp -d "${TMPDIR:-/tmp}/ccdb-gif.XXXXXX")"
cleanup() { rm -rf "$capture_dir"; }
trap cleanup EXIT INT TERM

npm --prefix theater run build
node "$repo_root/theater/scripts/capture-gif.mjs" "$capture_dir"
mkdir -p "$(dirname "$output_path")"

# ffmpeg gives a better GIF when it is installed, but the capture also writes
# PNG frames so the stdlib encoder can assemble the same animation anywhere.
video_path="$(find "$capture_dir" -type f -name '*.webm' -print -quit)"
if command -v ffmpeg >/dev/null 2>&1 && [[ -n "$video_path" ]]; then
  ffmpeg -y -loglevel error -i "$video_path" \
    -vf 'fps=12,scale=1280:-2:flags=lanczos' -loop 0 "$output_path"
  echo "record-gif: PASS encoder=ffmpeg output=$output_path"
else
  python3 "$repo_root/scripts/ci/frames-to-gif.py" "$capture_dir" "$output_path" --fps 12 --scale 2
  echo "record-gif: PASS encoder=stdlib output=$output_path"
fi
