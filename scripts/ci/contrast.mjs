// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export function luminance(hex) {
  const value = hex.slice(1);
  const channels = value.length === 3
    ? [...value].map((channel) => Number.parseInt(channel + channel, 16))
    : [value.slice(0, 2), value.slice(2, 4), value.slice(4, 6)].map((channel) => Number.parseInt(channel, 16));
  if (channels.length !== 3 || channels.some(Number.isNaN)) throw new Error(`invalid color ${hex}`);
  const linear = channels.map((channel) => {
    const srgb = channel / 255;
    return srgb <= 0.04045 ? srgb / 12.92 : ((srgb + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
}

export function contrast(left, right) {
  const [high, low] = [luminance(left), luminance(right)].sort((a, b) => b - a);
  return (high + 0.05) / (low + 0.05);
}

export function tokenMap(source) {
  return new Map([...source.matchAll(/--([a-z-]+):\s*(#[0-9a-fA-F]{3,6})\b/g)].map((match) => [match[1], match[2]]));
}

export function verifyTheme(name, tokens) {
  const pairs = [
    ["text", "bg", 4.5, "text"],
    ["text", "panel", 4.5, "text"],
    ["muted", "bg", 4.5, "text"],
    ["muted", "panel", 4.5, "text"],
    ["teal", "bg", 3, "indicator"],
    ["amber", "bg", 3, "indicator"],
    ["red", "bg", 3, "indicator"],
    ["blue", "bg", 3, "indicator"],
  ];
  const results = pairs.map(([foreground, background, minimum, kind]) => {
    const left = tokens.get(foreground);
    const right = tokens.get(background);
    if (!left || !right) throw new Error(`${name}: missing --${foreground} or --${background}`);
    return { foreground, background, minimum, kind, ratio: contrast(left, right) };
  });
  const failure = results.find((result) => result.ratio < result.minimum);
  if (failure) {
    throw new Error(`${name}: ${failure.kind} --${failure.foreground}/--${failure.background} contrast ${failure.ratio.toFixed(2)} < ${failure.minimum}`);
  }
  return results;
}

function main() {
  const root = fileURLToPath(new URL("../..", import.meta.url));
  const source = readFileSync(`${root}/theater/src/styles/tokens.css`, "utf8");
  const lightMarker = "@media (prefers-color-scheme: light)";
  const split = source.indexOf(lightMarker);
  if (split < 0) throw new Error("missing light theme tokens");
  const dark = verifyTheme("dark", tokenMap(source.slice(0, split)));
  const light = verifyTheme("light", tokenMap(source.slice(split)));
  console.log(`contrast: PASS ${dark.length + light.length} documented pairs`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) main();
