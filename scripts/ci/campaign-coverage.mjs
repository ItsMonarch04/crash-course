#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

/**
 * Every fault profile must be run by the campaign workflow.
 *
 * A selectable profile with no gate rots without anyone noticing. `corruption`
 * reached the point where every seed aborted before recording a single event
 * and stayed that way across releases, because it was the one transport
 * profile no workflow executed. This check reads the profile set out of
 * `cc-sim` — the enum, its `ALL` table, and its `as_str` names — and fails if a
 * profile is missing from any of them or from `campaigns.yml`.
 */
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const source = readFileSync(resolve(root, "crates/cc-sim/src/lib.rs"), "utf8");
const workflow = readFileSync(resolve(root, ".github/workflows/campaigns.yml"), "utf8");
const errors = [];

function section(pattern, label) {
  const match = pattern.exec(source);
  if (!match) errors.push(`cannot find ${label} in crates/cc-sim/src/lib.rs`);
  return match?.[1] ?? "";
}

const variantBlock = section(
  /#\[derive\([^)]*\)\]\s*\npub enum FaultProfile \{([\s\S]*?)\n\}/,
  "the FaultProfile enum",
);
const allBlock = section(/pub const ALL: \[Self; \d+\] = \[([\s\S]*?)\n {4}\];/, "FaultProfile::ALL");
const asStrBlock = section(
  /pub const fn as_str\(self\) -> &'static str \{\s*\n\s*match self \{([\s\S]*?)\n {8}\}/,
  "FaultProfile::as_str",
);

const variants = [...variantBlock.matchAll(/^\s{4}([A-Z][A-Za-z0-9]*),$/gm)].map((m) => m[1]);
const listed = new Set([...allBlock.matchAll(/Self::([A-Za-z0-9]+),/g)].map((m) => m[1]));
const names = new Map(
  [...asStrBlock.matchAll(/Self::([A-Za-z0-9]+) => "([a-z0-9-]+)",/g)].map((m) => [m[1], m[2]]),
);

if (variants.length === 0) errors.push("no FaultProfile variants were parsed");

const runs = new Set([...workflow.matchAll(/--profile ([a-z0-9-]+)/g)].map((m) => m[1]));

for (const variant of variants) {
  if (!listed.has(variant)) errors.push(`FaultProfile::ALL is missing ${variant}`);
  const name = names.get(variant);
  if (!name) {
    errors.push(`FaultProfile::as_str is missing ${variant}`);
    continue;
  }
  if (!runs.has(name)) {
    errors.push(`campaigns.yml never runs --profile ${name} (${variant})`);
  }
}
for (const variant of listed) {
  if (!variants.includes(variant)) errors.push(`FaultProfile::ALL lists unknown ${variant}`);
}

if (errors.length > 0) {
  for (const error of errors) console.error(`campaign coverage: ${error}`);
  process.exit(1);
}
console.log(`campaign coverage: PASS profiles=${variants.length}`);
