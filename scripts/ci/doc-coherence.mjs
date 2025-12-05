#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

/**
 * Small, dependency-free documentation consistency check.  It intentionally
 * validates mechanical claims (local links, the ADR sequence, and the test
 * register shape); semantic claims remain a human gate and are recorded in
 * BUILDLOG outside the repository.
 */
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, extname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const errors = [];

const requiredDocs = [
  "README.md",
  "docs/consistency.md",
  "docs/LIMITATIONS.md",
  "docs/formats.md",
  "docs/compatibility.md",
  "docs/calibration.md",
  "docs/ops.md",
  "docs/sim.md",
  "docs/threat-model.md",
];
for (const path of requiredDocs) {
  if (!existsSync(resolve(root, path))) errors.push(`missing required documentation: ${path}`);
}

function markdownFiles(path) {
  return readdirSync(path, { withFileTypes: true }).flatMap((entry) => {
    const target = resolve(path, entry.name);
    if (entry.isDirectory()) return markdownFiles(target);
    return extname(entry.name) === ".md" ? [target] : [];
  });
}

function localLinkTarget(raw) {
  const target = raw.trim().replace(/^<|>$/g, "");
  if (
    !target ||
    target.startsWith("#") ||
    target.startsWith("/") ||
    /^[a-z][a-z0-9+.-]*:/i.test(target)
  ) {
    return null;
  }
  return target.split(/[?#]/, 1)[0];
}

for (const file of [resolve(root, "README.md"), ...markdownFiles(resolve(root, "docs"))]) {
  const source = readFileSync(file, "utf8");
  const links = source.matchAll(/\[[^\]]*\]\(([^)]+)\)/g);
  for (const match of links) {
    const target = localLinkTarget(match[1]);
    if (!target) continue;
    const resolved = resolve(dirname(file), target);
    if (!existsSync(resolved)) {
      errors.push(`${relative(root, file)}: missing local link ${target}`);
    }
  }
}

const adrDir = resolve(root, "docs/adr");
const adrNumbers = readdirSync(adrDir)
  .map((name) => /^(\d{4})-.*\.md$/.exec(name))
  .filter(Boolean)
  .map((match) => Number(match[1]))
  .filter((number) => number !== 0)
  .sort((left, right) => left - right);
for (let expected = 1; expected <= adrNumbers.at(-1); expected += 1) {
  if (adrNumbers[expected - 1] !== expected) {
    errors.push(`ADR sequence is not append-only at ${String(expected).padStart(4, "0")}`);
    break;
  }
}

const register = resolve(root, "tests/test-register.tsv");
const [header, ...rows] = readFileSync(register, "utf8").trimEnd().split("\n");
if (header !== "phase\tgate\tharness\tpackage\tname\trequirement\tstatus") {
  errors.push("test register header differs from the checked-in schema");
}
for (const [index, row] of rows.entries()) {
  const columns = row.split("\t");
  if (columns.length !== 7 || columns.some((column) => column.length === 0)) {
    errors.push(`test register row ${index + 2} is not a complete seven-column record`);
  }
}

if (errors.length > 0) {
  for (const error of errors) console.error(`doc coherence: ${error}`);
  process.exitCode = 1;
} else {
  console.log(
    `doc coherence: PASS docs=${requiredDocs.length} markdown=${markdownFiles(resolve(root, "docs")).length + 1} adrs=${adrNumbers.length} register_rows=${rows.length}`,
  );
}
