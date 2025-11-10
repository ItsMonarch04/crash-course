// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { spawn } from "node:child_process";
import { mkdirSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import { chromium } from "@playwright/test";

const outputDir = process.argv[2];
if (!outputDir) throw new Error("usage: capture-gif.mjs OUTPUT_DIR");
mkdirSync(outputDir, { recursive: true });

const port = "4174";
const server = spawn("npm", ["run", "dev", "--", "--host", "127.0.0.1", "--port", port], {
  cwd: new URL("..", import.meta.url),
  stdio: "ignore",
});

async function waitForServer() {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/`);
      if (response.ok) return;
    } catch {}
    await delay(100);
  }
  throw new Error("theater dev server did not become ready");
}

try {
  await waitForServer();
  const browser = await chromium.launch();
  const context = await browser.newContext({
    recordVideo: { dir: outputDir, size: { width: 1280, height: 720 } },
    viewport: { width: 1280, height: 720 },
  });
  const page = await context.newPage();
  await page.goto(`http://127.0.0.1:${port}/`);
  await page.getByTestId("engine-state").filter({ hasText: "LIVE SIM" }).waitFor({ timeout: 30_000 });

  // Frames are captured alongside the video so the GIF can be assembled with
  // or without ffmpeg. `frames-to-gif.py` consumes exactly this naming.
  let frame = 0;
  const shoot = async () => {
    await page.screenshot({
      path: `${outputDir}/frame-${String(frame).padStart(4, "0")}.png`,
      animations: "disabled",
    });
    frame += 1;
  };

  await page.getByRole("button", { name: "PLAY" }).click();
  for (let index = 0; index < 18; index += 1) {
    await shoot();
    await delay(90);
  }
  await page.getByRole("button", { name: /KILL LEADER/ }).click();
  for (let index = 0; index < 30; index += 1) {
    await shoot();
    await delay(90);
  }
  console.log(`capture-gif: frames=${frame}`);
  await context.close();
  await browser.close();
} finally {
  server.kill("SIGTERM");
  await new Promise((resolve) => server.once("exit", resolve));
}
