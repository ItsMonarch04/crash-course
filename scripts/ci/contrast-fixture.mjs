// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

import assert from "node:assert/strict";
import { contrast, tokenMap, verifyTheme } from "./contrast.mjs";

assert.equal(contrast("#000", "#fff"), 21);
assert.throws(() => verifyTheme("fixture", tokenMap(":root { --text:#777; --bg:#fff; --panel:#fff; --muted:#777; --teal:#000; --amber:#000; --red:#000; --blue:#000; }")), /contrast/);
console.log("contrast fixture: PASS");
