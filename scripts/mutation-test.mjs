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

// Take an evenly-spaced slice from every target crate rather than the first N
// by path. A prefix sample is alphabetical, not representative: at --limit 25
// it never reached cc-store or cc-wal at all, while the report's single
// kill_score read like a suite-wide number. Spacing is deterministic — the
// score has to be reproducible for the same tree.
function stratify(all, take) {
  if (take >= all.length) return all;
  const byCrate = new Map();
  for (const mutant of all) {
    if (!byCrate.has(mutant.packageName)) byCrate.set(mutant.packageName, []);
    byCrate.get(mutant.packageName).push(mutant);
  }
  const crates = [...byCrate.keys()].sort();
  // Largest remainder, so the quotas sum to exactly `take`.
  const quotas = crates.map((name) => {
    const exact = (byCrate.get(name).length * take) / all.length;
    return { name, floor: Math.floor(exact), remainder: exact - Math.floor(exact) };
  });
  // Every crate with any candidate contributes at least one mutant. A crate
  // silently contributing zero is the failure this function exists to stop, so
  // the floor of 1 wins even when it pushes the sample above `take`.
  for (const quota of quotas) quota.floor = Math.max(quota.floor, 1);
  let assigned = quotas.reduce((sum, quota) => sum + quota.floor, 0);
  const byRemainder = [...quotas].sort(
    (a, b) => b.remainder - a.remainder || a.name.localeCompare(b.name),
  );
  while (assigned < take) {
    const quota = byRemainder.find((candidate) => candidate.floor < byCrate.get(candidate.name).length);
    if (!quota) break;
    quota.floor += 1;
    assigned += 1;
  }
  // Trim the largest quotas first if the per-crate floors overshot, never
  // below one.
  while (assigned > take) {
    const quota = [...quotas].sort((a, b) => b.floor - a.floor || a.name.localeCompare(b.name))[0];
    if (!quota || quota.floor <= 1) break;
    quota.floor -= 1;
    assigned -= 1;
  }
  const picked = [];
  for (const { name, floor } of quotas) {
    const pool = byCrate.get(name);
    const count = Math.min(pool.length, floor);
    const stride = pool.length / count;
    for (let index = 0; index < count; index += 1) {
      picked.push(pool[Math.floor(index * stride)]);
    }
  }
  return picked;
}

const selected = stratify(mutants, limit);

function perCrate(items) {
  const counts = {};
  for (const item of items) counts[item.packageName] = (counts[item.packageName] ?? 0) + 1;
  return counts;
}

if (listOnly) {
  for (const mutant of selected) {
    console.log(`${relative(root, mutant.path)}:${mutant.line} ${mutant.from} -> ${mutant.to}`);
  }
  console.log(`mutation-test: candidates=${mutants.length} listed=${selected.length}`);
  console.log(`mutation-test: per_crate=${JSON.stringify(perCrate(selected))}`);
  process.exit(0);
}

const sandbox = mkdtempSync(join(tmpdir(), "cc-mutation-"));
try {
  cpSync(root, sandbox, {
    recursive: true,
    filter: (source) => ![".git", "target", "node_modules", "dist", "artifacts"].includes(basename(source)),
  });
  const results = [];
  for (const [index, mutant] of selected.entries()) {
    const copied = join(sandbox, relative(root, mutant.path));
    const original = readFileSync(copied, "utf8");
    const changed = `${original.slice(0, mutant.offset)}${mutant.to}${original.slice(mutant.offset + mutant.from.length)}`;
    writeFileSync(copied, changed);
    const run = spawnSync("cargo", ["test", "--locked", "--quiet", "-p", mutant.packageName], {
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
    schema_version: 2,
    candidates: mutants.length,
    selected: results.length,
    // The score describes this sample, not the whole suite. Publishing the
    // sample's shape alongside it is what keeps that distinction visible.
    sampling: "stratified-by-crate, evenly spaced, deterministic",
    per_crate: perCrate(selected),
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
