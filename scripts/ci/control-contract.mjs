// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../..", import.meta.url));
const contractPath = `${root}/theater/tests/control-contract.tsv`;
const sourcePath = `${root}/theater/src/main.tsx`;
const rows = readFileSync(contractPath, "utf8").trimEnd().split("\n");
const header = "control\twasm input or host action\texpected observable state\tPlaywright coverage";

if (rows.shift() !== header) {
  throw new Error("control contract has an invalid header");
}

const declared = new Set();
for (const row of rows) {
  const fields = row.split("\t");
  if (fields.length !== 4 || fields.some((field) => field.length === 0)) {
    throw new Error(`control contract row is malformed: ${row}`);
  }
  if (declared.has(fields[0])) {
    throw new Error(`control contract repeats ${fields[0]}`);
  }
  declared.add(fields[0]);
}

const rendered = new Set();
const source = readFileSync(sourcePath, "utf8");
for (const match of source.matchAll(/data-control="([^"]+)"/g)) rendered.add(match[1]);

const missing = [...rendered].filter((control) => !declared.has(control));
const stale = [...declared].filter((control) => !rendered.has(control));
if (missing.length > 0 || stale.length > 0) {
  const parts = [];
  if (missing.length > 0) parts.push(`rendered without contract: ${missing.join(", ")}`);
  if (stale.length > 0) parts.push(`contract without rendered control: ${stale.join(", ")}`);
  throw new Error(parts.join("; "));
}

console.log(`control contract: PASS ${declared.size} controls`);
