#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (file) => readFileSync(path.join(root, file), "utf8");
const errors = [];
const semver = "([0-9]+\\.[0-9]+\\.[0-9]+)";

const readmeVersion = read("README.md").match(
  new RegExp(`\\*\\*Version:\\*\\*\\s+v${semver}(?:\\s|$)`),
)?.[1];
if (!readmeVersion) errors.push("README.md missing **Version:** vX.Y.Z marker");

let version = readmeVersion;
if (existsSync(path.join(root, "Cargo.toml"))) {
  const cargoVersion = read("Cargo.toml").match(
    new RegExp(`\\[workspace\\.package\\][\\s\\S]*?^version\\s*=\\s*"${semver}"`, "m"),
  )?.[1];
  if (!cargoVersion) errors.push("Cargo.toml missing workspace package version");
  if (version && cargoVersion !== version) {
    errors.push(`Cargo.toml version ${cargoVersion ?? "missing"} != README.md ${version}`);
  }
  version = cargoVersion ?? version;
}

if (version && existsSync(path.join(root, "Cargo.lock"))) {
  for (const block of read("Cargo.lock").split("[[package]]").slice(1)) {
    const name = block.match(/^\s*name\s*=\s*"([^"]+)"/m)?.[1];
    const packageVersion = block.match(/^\s*version\s*=\s*"([^"]+)"/m)?.[1];
    if (name?.startsWith("cc-") && packageVersion !== version) {
      errors.push(`Cargo.lock ${name} version ${packageVersion ?? "missing"} != ${version}`);
    }
  }
}

if (version && existsSync(path.join(root, "theater/package.json"))) {
  const pkg = JSON.parse(read("theater/package.json"));
  const lock = JSON.parse(read("theater/package-lock.json"));
  if (pkg.version !== version) {
    errors.push(`theater/package.json version ${pkg.version} != ${version}`);
  }
  if (lock.version !== version) {
    errors.push(`theater/package-lock.json version ${lock.version} != ${version}`);
  }
  if (lock.packages?.[""]?.version !== version) {
    errors.push(`theater lock root version ${lock.packages?.[""]?.version} != ${version}`);
  }
}

// The service worker names its cache after the build. If that name stops
// tracking the version, `activate` no longer evicts the previous build and
// returning visitors are pinned to a stale `index.html`.
if (version && existsSync(path.join(root, "theater/public/sw.js"))) {
  const cache = read("theater/public/sw.js").match(
    new RegExp(`const CACHE = "crash-course-theater-v${semver}"`),
  )?.[1];
  if (cache !== version) {
    errors.push(`theater/public/sw.js cache ${cache ?? "missing"} != ${version}`);
  }
}

for (const file of ["exhibits/manifest.json", "theater/public/exhibits/manifest.json"]) {
  if (!version || !existsSync(path.join(root, file))) continue;
  const build = JSON.parse(read(file)).build;
  if (build !== version) errors.push(`${file} build ${build ?? "missing"} != ${version}`);
}

for (const file of ["crates/cc-swarm/src/main.rs", "crates/cc-node/src/main.rs"]) {
  if (!existsSync(path.join(root, file))) continue;
  if (!read(file).includes('env!("CARGO_PKG_VERSION")')) {
    errors.push(`${file} must derive runtime versions from CARGO_PKG_VERSION`);
  }
}

if (errors.length) {
  console.error("version-coherence FAILED:");
  for (const error of errors) console.error(` - ${error}`);
  process.exit(1);
}

console.log(`version-coherence OK — ${version}`);
