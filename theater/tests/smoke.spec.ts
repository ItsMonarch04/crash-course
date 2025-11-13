// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { expect, test } from "@playwright/test";

test("hero scenario elects, survives a leader kill, and shares its run", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("CRASH COURSE")).toBeVisible();

  // The engine must actually be live. Without this the whole suite passes
  // against the recorded fixture when the WASM blob fails to load.
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });

  // Run until a leader exists, rather than asserting a role string that any
  // node satisfies at any time.
  await page.getByRole("button", { name: "PLAY" }).click();
  await expect(page.getByTestId("leader-id")).not.toHaveText("none", { timeout: 15_000 });
  const firstLeader = await page.getByTestId("leader-id").textContent();
  await expect(page.getByTestId("virtual-time")).not.toHaveText("0s / 60s");

  await page.getByTestId("determinism-proof").click();
  await expect(page.getByTestId("determinism-proof")).toContainText("MATCH", { timeout: 30_000 });

  // Killing the leader must produce a *different* leader within bounded
  // virtual time — the §1.5 hero claim, actually asserted.
  await page.getByRole("button", { name: /KILL LEADER/ }).click();
  await expect
    .poll(async () => page.getByTestId("leader-id").textContent(), { timeout: 30_000 })
    .not.toBe(firstLeader);
  await expect(page.getByTestId("leader-id")).not.toHaveText("none");

  await expect(page.getByText("No verified failure exhibits are published yet.")).toBeVisible();

  // The share link must carry the injected fault, not just the seed.
  await page.getByRole("button", { name: /Share this scenario/ }).click();
  const sharedUrl = page.url();
  expect(sharedUrl).toContain("#seed=");
  expect(decodeURIComponent(sharedUrl)).toContain("crash");

  await page.goto(sharedUrl);
  await expect(page.getByLabel("Seed")).toHaveValue(/0x/);
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
});

test("guided lesson hands control back and embed mode is chromeless", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await page.getByLabel("Lesson").selectOption("asymmetric");
  await expect(page.getByText("Asymmetric election")).toBeVisible();
  await page.getByRole("button", { name: "TAKE THE CONTROLS" }).click();
  await expect(page.getByText("Asymmetric election")).not.toBeVisible();

  await page.goto("/#embed=1");
  await expect(page.getByLabel("Cluster topology")).toBeVisible();
  await expect(page.getByText("CRASH COURSE")).not.toBeVisible();
});
