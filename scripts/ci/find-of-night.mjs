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
  const names = readdirSync(artifacts)
    .filter((entry) => entry.endsWith(".json") && !entry.endsWith(".shrunk.json"))
    .sort();
  for (const name of names) {
    try {
      const artifact = JSON.parse(readFileSync(resolve(artifacts, name), "utf8"));
      // Name the reason, not just the fact. A trace-invariant or liveness
      // failure can carry `verdict: "linearizable"`, and the summary used to
      // print only the verdict — so the one finding of the night read as
      // though nothing had gone wrong.
      const reasons = [];
      if (artifact.error) reasons.push(`runner error: ${artifact.error}`);
      if (artifact.trace_invariants_ok === false) reasons.push("trace invariants violated");
      if (artifact.liveness_ok === false) reasons.push("liveness not reached");
      if (["not-linearizable", "undecided"].includes(artifact.verdict)) {
        reasons.push(`verdict=${artifact.verdict}`);
      }
      if (reasons.length === 0) continue;
      artifact.find_reasons = reasons;
      // A finding has to be reduced before it is worth anyone's attention. An
      // unshrunk trace is a thousand actions of noise around the one that
      // mattered, so it is counted but never promoted.
      let receipt = null;
      const receiptPath = resolve(artifacts, `${name.replace(/\.json$/, "")}.shrunk.json`);
      if (existsSync(receiptPath)) {
        try {
          const parsed = JSON.parse(readFileSync(receiptPath, "utf8"));
          if (parsed.reproduces === true) receipt = parsed;
        } catch {
          // An unreadable receipt is no receipt.
        }
      }
      candidates.push({ name, artifact, receipt });
    } catch {
      // A malformed artifact is not promoted as a finding.
    }
  }
}
candidates.sort((left, right) => (right.artifact.events ?? 0) - (left.artifact.events ?? 0) || left.name.localeCompare(right.name));
const shrunk = candidates.filter((candidate) => candidate.receipt !== null);
// Prefer the most reduced finding: a 3-action repro teaches more than a 900-action one.
shrunk.sort((left, right) => left.receipt.shrunk_actions - right.receipt.shrunk_actions || left.name.localeCompare(right.name));

let markdown = "## Find of the night\n\nNo real failing artifact was produced. The slot stays empty.\n";
if (candidates.length > 0 && shrunk.length === 0) {
  markdown = `## Find of the night\n\n${candidates.length} failing artifact(s) were produced but none has a shrink receipt, so nothing is promoted. Reduce one with \`cc-swarm shrink --failure artifacts/<seed>.json\` and it becomes publishable.\n`;
}
if (shrunk.length > 0) {
  const { name, artifact, receipt } = shrunk[0];
  const seed = artifact.seed ?? artifact.run_spec?.seed;
  const profile = artifact.profile ?? artifact.run_spec?.profile ?? "rough";
  const runSpec = encodeURIComponent(JSON.stringify(artifact.run_spec ?? { seed, profile, faults: [] }));
  const url = `${baseUrl}#seed=${encodeURIComponent(seed)}&profile=${encodeURIComponent(profile)}&run_spec=${runSpec}`;
  markdown = `## Find of the night\n\n[Replay ${name} in the theater](${url}) — ${artifact.find_reasons.join("; ")}, verdict=${artifact.verdict ?? "runner-error"}, events=${artifact.events ?? 0}, shrunk from ${receipt.canonical_actions} fault actions to ${receipt.shrunk_actions} and re-verified to still reproduce.\n`;
}
writeFileSync(output, markdown);
process.stdout.write(markdown);
