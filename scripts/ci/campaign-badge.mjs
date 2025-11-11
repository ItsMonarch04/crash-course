#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

import fs from "node:fs";
import path from "node:path";

function flag(name, fallback) {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function positiveInteger(name, fallback) {
  const value = Number(flag(name, fallback));
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer`);
  }
  return value;
}

const input = flag("--input", "");
const output = flag("--output", "target/campaign/campaign-badge.json");
const seeds = positiveInteger("--seeds", 0);
const failures = positiveInteger("--failures", 0);
const runId = flag("--run-id", "local");
const profile = flag("--profile", "rough");
let previous = { cumulative_seeds: 0, runs: 0 };
if (input && fs.existsSync(input)) {
  previous = JSON.parse(fs.readFileSync(input, "utf8"));
}

const cumulativeSeeds = positiveInteger(
  "--cumulative-seeds",
  Number(previous.cumulative_seeds || 0) + seeds,
);
const runs = positiveInteger("--runs", Number(previous.runs || 0) + 1);
const status = failures === 0 ? "passing" : "failing";
const badge = {
  schemaVersion: 1,
  label: "sim campaign",
  message: `${cumulativeSeeds.toLocaleString("en-US")} seeds`,
  color: failures === 0 ? "brightgreen" : "red",
  status,
  profile,
  seedsThisRun: seeds,
  cumulativeSeeds,
  failures,
  runs,
  runId,
};
fs.mkdirSync(path.dirname(output), { recursive: true });
fs.writeFileSync(output, `${JSON.stringify(badge, null, 2)}\n`);
console.log(`campaign badge: ${output} cumulative_seeds=${cumulativeSeeds} status=${status}`);
