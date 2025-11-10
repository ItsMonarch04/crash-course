// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { readFileSync } from "node:fs";
import { init, initSync, state, step } from "../../theater/src/wasm/cc_wasm.js";

initSync({ module: readFileSync("theater/public/wasm/cc_wasm_bg.wasm") });
const seed = process.argv[2] ?? "0x2a";
const profile = process.argv[3] ?? "calm";
const handle = init(JSON.stringify({ seed, profile }));
let output = state(handle);
for (let index = 0; index < 120; index += 1) {
  output = step(handle, 500_000_000n);
}
process.stdout.write(output);
handle.free();
