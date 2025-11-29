#!/usr/bin/env node
// SPDX-License-Identifier: AGPL-3.0-only

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "../..");
const rows = readFileSync(resolve(root, "tests/golden/manifest.tsv"), "utf8")
  .trimEnd()
  .split("\n")
  .slice(1)
  .map((line) => line.split("\t"));
const row = rows.find(
  ([format, , version, , build]) =>
    format === "CCPL" && version === "1" && build === "0.11.14",
);
if (!row) throw new Error("immutable CCPL v1 golden row is absent");

const expectedHash = row[7];
const bytes = Buffer.from(readFileSync(resolve(root, row[5])));
const versionBefore = bytes.readUInt16LE(4);
const members = bytes.readUInt32LE(6);
bytes.writeUInt32LE(members + 1, 6); // a real CCPL field, not padding
bytes.writeUInt32LE(0, bytes.length - 4);

function crc32c(input) {
  let crc = 0xffff_ffff;
  for (const byte of input) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      crc = (crc >>> 1) ^ (crc & 1 ? 0x82f6_3b78 : 0);
    }
  }
  return (~crc) >>> 0;
}

bytes.writeUInt32LE(crc32c(bytes), bytes.length - 4);
if (bytes.readUInt16LE(4) !== versionBefore || versionBefore !== 1) {
  throw new Error("mutation changed the format version");
}
const actualHash = createHash("sha256").update(bytes).digest("hex");
if (actualHash === expectedHash) {
  throw new Error("unversioned CCPL field mutation escaped the golden hash");
}
console.log(
  `golden mutation: PASS format=CCPL version=1 field=max_members before=${members} after=${members + 1}`,
);

