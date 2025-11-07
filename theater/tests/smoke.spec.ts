// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { expect, test } from "@playwright/test";

test("flight recorder shell exposes the core controls", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("CRASH COURSE")).toBeVisible();
  await expect(page.getByRole("button", { name: /KILL LEADER/ })).toBeVisible();
  await page.getByRole("button", { name: /KILL LEADER/ }).click();
  await expect(page.getByText("SAFE")).toBeVisible();
});

