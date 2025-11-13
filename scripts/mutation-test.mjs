#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { cpSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const listOnly = args.includes("--list");
const limitIndex = args.indexOf("--limit");
const limit = limitIndex >= 0 ? Number(args[limitIndex + 1]) : 20;
const targets = ["cc-raft", "cc-checker", "cc-kv", "cc-store", "cc-wal"];
const swaps = [
  ["<=", "<"],
  [">=", ">"],
  ["==", "!="],
  ["saturating_add(1)", "saturating_sub(1)"],
  [" + 1", " + 2"],
];

const mutants = [];
for (const packageName of targets) {
  const path = join(root, "crates", packageName, "src", "lib.rs");
  const source = readFileSync(path, "utf8");
  const production = source.split("#[cfg(test)]", 1)[0];
  for (const [from, to] of swaps) {
    let offset = 0;
    while ((offset = production.indexOf(from, offset)) >= 0) {
      const line = production.slice(0, offset).split("\n").length;
      mutants.push({ packageName, path, offset, from, to, line });
      offset += from.length;
    }
  }
}
mutants.sort((left, right) => left.path.localeCompare(right.path) || left.offset - right.offset || left.to.localeCompare(right.to));

if (listOnly) {
  for (const mutant of mutants.slice(0, limit)) {
    console.log(`${relative(root, mutant.path)}:${mutant.line} ${mutant.from} -> ${mutant.to}`);
  }
  console.log(`mutation-test: candidates=${mutants.length} listed=${Math.min(limit, mutants.length)}`);
  process.exit(0);
}

const sandbox = mkdtempSync(join(tmpdir(), "cc-mutation-"));
try {
  cpSync(root, sandbox, {
    recursive: true,
    filter: (source) => ![".git", "target", "node_modules", "dist", "artifacts"].includes(basename(source)),
  });
  const selected = mutants.slice(0, limit);
  const results = [];
  for (const [index, mutant] of selected.entries()) {
    const copied = join(sandbox, relative(root, mutant.path));
    const original = readFileSync(copied, "utf8");
    const changed = `${original.slice(0, mutant.offset)}${mutant.to}${original.slice(mutant.offset + mutant.from.length)}`;
    writeFileSync(copied, changed);
    const run = spawnSync("cargo", ["test", "--quiet", "-p", mutant.packageName], {
      cwd: sandbox,
      encoding: "utf8",
      env: { ...process.env, CARGO_TARGET_DIR: join(sandbox, "target") },
    });
    writeFileSync(copied, original);
    const killed = run.status !== 0;
    results.push({
      id: index + 1,
      file: relative(root, mutant.path),
      line: mutant.line,
      mutation: `${mutant.from} -> ${mutant.to}`,
      killed,
    });
    console.log(`mutant ${index + 1}/${selected.length} ${killed ? "KILLED" : "SURVIVED"} ${results.at(-1).file}:${mutant.line}`);
  }
  const killed = results.filter((result) => result.killed).length;
  const report = {
    schema_version: 1,
    selected: results.length,
    killed,
    survived: results.length - killed,
    kill_score_percent: results.length === 0 ? 0 : Math.round((killed * 10_000) / results.length) / 100,
    results,
  };
  mkdirSync(join(root, "artifacts"), { recursive: true });
  writeFileSync(join(root, "artifacts", "mutation-score.json"), `${JSON.stringify(report, null, 2)}\n`);
  console.log(`mutation-test: kill_score=${report.kill_score_percent}% killed=${killed} selected=${results.length}`);
} finally {
  rmSync(sandbox, { recursive: true, force: true });
}
