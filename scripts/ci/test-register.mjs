#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

/**
 * The checked-in test register is the repository's own inventory of every
 * named receipt. It is self-contained on purpose: this script never reads an
 * external planning document, only the sources it governs.
 *
 * `--check` (the default) fails on a registered test that no longer exists, on
 * an implemented test that nobody registered, on a name that two owners claim,
 * and on a malformed row. `--write` regenerates the file, preserving the
 * reviewed requirement text of every row that already exists.
 */

import { readFileSync, writeFileSync, readdirSync, statSync } from "node:fs";
import { resolve, dirname, basename, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const registerPath = resolve(root, "tests/test-register.tsv");
const HEADER = "phase\tgate\tharness\tpackage\tname\trequirement\tstatus";

const GATES = {
  C0: "C0",
  N0: "NG0",
  N1: "NG1",
  N2: "NG2",
  N3: "NG3",
  N4: "NG4",
  N5: "NG5",
  N6: "NG6",
  N7: "NG7",
  N8: "NG8",
  N9: "NG9",
  N10: "NG10",
  N11: "NG11",
};

function walk(directory, out = []) {
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) {
      if (entry === "target" || entry === "node_modules") continue;
      walk(path, out);
    } else if (path.endsWith(".rs")) {
      out.push(path);
    }
  }
  return out;
}

/** Every `#[test]` function, with the crate and harness that owns it. */
function rustTests() {
  const found = [];
  const cratesDir = resolve(root, "crates");
  for (const crate of readdirSync(cratesDir)) {
    const crateDir = join(cratesDir, crate);
    if (!statSync(crateDir).isDirectory()) continue;
    for (const file of walk(crateDir)) {
      const relative = file.slice(crateDir.length + 1);
      let harness = "cargo-lib";
      if (relative.startsWith("tests/")) harness = "cargo-test";
      else if (relative === "src/main.rs" || relative.startsWith("src/bin/")) {
        harness = "cargo-bin";
      } else if (relative.startsWith("examples/")) continue;
      const text = readFileSync(file, "utf8");
      const lines = text.split("\n");
      for (const [index, line] of lines.entries()) {
        if (line.trim() !== "#[test]") continue;
        // libtest also accepts attributes between `#[test]` and the item.
        for (let cursor = index + 1; cursor < lines.length; cursor += 1) {
          const candidate = lines[cursor].trim();
          if (candidate.startsWith("#[") || candidate.startsWith("//")) continue;
          const match = /^(?:pub\s+)?fn\s+([a-z0-9_]+)\s*\(/.exec(candidate);
          if (match) found.push({ name: match[1], package: crate, harness });
          break;
        }
      }
    }
  }
  return found;
}

/** Every Playwright spec, which owns the browser-facing receipts. */
function playwrightTests() {
  const specDir = resolve(root, "theater/tests");
  const found = [];
  for (const entry of readdirSync(specDir)) {
    if (!entry.endsWith(".spec.ts")) continue;
    const text = readFileSync(join(specDir, entry), "utf8");
    for (const match of text.matchAll(/^\s*test\(\s*"([^"]+)"/gm)) {
      found.push({ name: match[1], package: "theater", harness: "playwright" });
    }
  }
  return found;
}

function readRegister() {
  const text = readFileSync(registerPath, "utf8").trimEnd();
  const [header, ...rows] = text.split("\n");
  if (header !== HEADER) {
    return { header, rows: [], invalid: "header differs from the checked-in schema" };
  }
  const parsed = [];
  for (const [index, row] of rows.entries()) {
    const columns = row.split("\t");
    if (columns.length !== 7 || columns.some((column) => column.length === 0)) {
      return { header, rows: [], invalid: `row ${index + 2} is not a complete seven-column record` };
    }
    const [phase, gate, harness, pkg, name, requirement, status] = columns;
    parsed.push({ phase, gate, harness, package: pkg, name, requirement, status });
  }
  return { header, rows: parsed };
}

function humanize(name) {
  const words = name.replace(/^(trap_|golden_)/, "").split("_");
  const sentence = words.join(" ");
  return sentence.charAt(0).toUpperCase() + sentence.slice(1);
}

function discovered() {
  const all = [...rustTests(), ...playwrightTests()];
  all.sort(
    (left, right) =>
      left.package.localeCompare(right.package) || left.name.localeCompare(right.name),
  );
  return all;
}

function check() {
  const { rows, invalid } = readRegister();
  const errors = [];
  if (invalid) {
    console.error(`test register: ${invalid}`);
    process.exit(1);
  }
  const found = discovered();
  const foundByName = new Map();
  for (const test of found) {
    const existing = foundByName.get(test.name);
    if (existing && existing.package !== test.package) {
      errors.push(
        `test ${test.name} is claimed by both ${existing.package} and ${test.package}`,
      );
    }
    foundByName.set(test.name, test);
  }
  const registeredNames = new Set();
  for (const row of rows) {
    if (registeredNames.has(row.name)) {
      errors.push(`duplicate register row for ${row.name}`);
    }
    registeredNames.add(row.name);
    if (!["planned", "red", "green"].includes(row.status)) {
      errors.push(`unknown status for ${row.name}: ${row.status}`);
    }
    const test = foundByName.get(row.name);
    if (!test) {
      if (row.status !== "planned") {
        errors.push(`registered test is missing or renamed: ${row.name}`);
      }
      continue;
    }
    if (test.package !== row.package || test.harness !== row.harness) {
      errors.push(
        `${row.name} moved to ${test.package}/${test.harness}; the register still says ${row.package}/${row.harness}`,
      );
    }
  }
  for (const test of found) {
    if (!registeredNames.has(test.name)) {
      errors.push(`implemented test is not registered: ${test.package}::${test.name}`);
    }
  }
  if (errors.length > 0) {
    for (const error of errors) console.error(`test register: ${error}`);
    console.error("test register: run scripts/ci/test-register.sh --write to regenerate");
    process.exit(1);
  }
  console.log(`test register: PASS rows=${rows.length} discovered=${found.length}`);
}

function write() {
  let existing = new Map();
  try {
    for (const row of readRegister().rows) existing.set(row.name, row);
  } catch {
    existing = new Map();
  }
  const lines = [HEADER];
  for (const test of discovered()) {
    const prior = existing.get(test.name);
    const phase = prior?.phase ?? "-";
    lines.push(
      [
        phase,
        prior?.gate ?? GATES[phase] ?? "-",
        test.harness,
        test.package,
        test.name,
        prior?.requirement ?? humanize(test.name),
        "green",
      ].join("\t"),
    );
  }
  writeFileSync(registerPath, `${lines.join("\n")}\n`);
  console.log(`test register: wrote ${lines.length - 1} rows`);
}

if (process.argv.includes("--write")) {
  write();
} else {
  check();
}
