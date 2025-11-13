#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
};
const artifacts = resolve(flag("--artifacts", "artifacts"));
const output = resolve(flag("--output", "target/find-of-night.md"));
const baseUrl = flag("--base-url", "./theater/");
const candidates = [];
if (existsSync(artifacts)) {
  for (const name of readdirSync(artifacts).filter((entry) => entry.endsWith(".json")).sort()) {
    try {
      const artifact = JSON.parse(readFileSync(resolve(artifacts, name), "utf8"));
      const failed = artifact.error
        || artifact.trace_invariants_ok === false
        || artifact.liveness_ok === false
        || ["not-linearizable", "undecided"].includes(artifact.verdict);
      if (failed) candidates.push({ name, artifact });
    } catch {
      // A malformed artifact is not promoted as a finding.
    }
  }
}
candidates.sort((left, right) => (right.artifact.events ?? 0) - (left.artifact.events ?? 0) || left.name.localeCompare(right.name));
let markdown = "## Find of the night\n\nNo real failing artifact was produced. The slot stays empty.\n";
if (candidates.length > 0) {
  const { name, artifact } = candidates[0];
  const seed = artifact.seed ?? artifact.run_spec?.seed;
  const profile = artifact.profile ?? artifact.run_spec?.profile ?? "rough";
  const runSpec = encodeURIComponent(JSON.stringify(artifact.run_spec ?? { seed, profile, faults: [] }));
  const url = `${baseUrl}#seed=${encodeURIComponent(seed)}&profile=${encodeURIComponent(profile)}&run_spec=${runSpec}`;
  markdown = `## Find of the night\n\n[Replay ${name} in the theater](${url}) — verdict=${artifact.verdict ?? "runner-error"}, events=${artifact.events ?? 0}.\n`;
}
writeFileSync(output, markdown);
process.stdout.write(markdown);
