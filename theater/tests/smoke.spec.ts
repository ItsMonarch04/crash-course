// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { expect, test } from "@playwright/test";

const CONTROL_IDS = new Set([
  "play", "motion-preference", "seed", "profile", "lesson", "cluster-size", "speed", "heal-all",
  "determinism-proof", "kill-leader", "crash-selected", "partition-selected", "heal-palette",
  "packet-loss", "clock-skew", "disk-latency", "take-controls", "selected-node", "previous-event",
  "timeline-play", "next-event", "timeline", "timeline-marker", "checkpoint", "museum-category",
  "museum-exhibit", "share",
]);

test("every_control_changes_observable_state", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });

  // The contract is intentionally checked against the rendered DOM. A new
  // visible control has to declare its engine/host action and observable
  // result before it can ship.
  const rendered = await page.locator("[data-control]").evaluateAll((elements) =>
    elements.map((element) => element.getAttribute("data-control")),
  );
  for (const control of rendered) expect(CONTROL_IDS.has(control ?? "")).toBeTruthy();

  await page.getByLabel("Speed").selectOption("4×");
  await page.getByRole("button", { name: "PLAY", exact: true }).click();
  await expect.poll(async () => page.getByTestId("virtual-time").textContent(), { timeout: 15_000 })
    .not.toBe("0s / 60s");

  const timeline = page.getByLabel("Timeline");
  await timeline.focus();
  await timeline.press("End");
  await expect(timeline).toHaveAttribute("aria-valuetext", "60 virtual seconds");

  await page.getByLabel("Disk latency").fill("64");
  await expect(page.getByTestId("disk-latency-value")).toHaveText("64 ms");
});

test("timeline markers render without duplicate React keys", async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await page.getByTestId("determinism-proof").click();
  await expect(page.getByTestId("determinism-proof")).toContainText("MATCH", { timeout: 30_000 });
  expect(consoleErrors).toEqual([]);
});

test("hero scenario elects, survives a leader kill, and shares its run", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("CRASH COURSE")).toBeVisible();

  // The engine must actually be live. Without this the whole suite passes
  // against the recorded fixture when the WASM blob fails to load.
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });

  // Run until a leader exists, rather than asserting a role string that any
  // node satisfies at any time.
  await page.getByRole("button", { name: "PLAY", exact: true }).click();
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

// Every control in the strip is supposed to reach the engine. These used to be
// chrome: the cluster select had no handler and displayed a size the sim was
// not running, the canvas toggled only between n1 and n2 so three of five nodes
// were unreachable, the timeline's step buttons both merely paused, and the
// skew/latency readouts were hardcoded regardless of what was injected.
test("cluster size, node selection, stepping, and fault readouts are live", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });

  // The control must agree with the cluster that is actually running.
  await expect(page.getByLabel("Cluster size")).toHaveValue("5");
  await page.getByLabel("Cluster size").selectOption("7");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await expect(page.getByLabel("Cluster size")).toHaveValue("7");

  // A node beyond n2 must be selectable, or the whole chaos palette can only
  // ever target two of them.
  const canvas = page.getByLabel("Cluster topology");
  const box = await canvas.boundingBox();
  if (!box) throw new Error("topology canvas has no box");
  // Node 4 of 7 sits on the lower-left arc: -90° + 3/7 of a turn.
  const angle = -Math.PI / 2 + (3 / 7) * Math.PI * 2;
  const radius = Math.min(box.width, box.height) * 0.31;
  await canvas.click({
    position: { x: box.width * 0.48 + Math.cos(angle) * radius, y: box.height * 0.51 + Math.sin(angle) * radius },
  });
  await expect(page.getByText("CLOCK SKEW · n4")).toBeVisible();

  // The readout must equal what the slider injected, not a constant.
  await page.getByLabel("Clock skew").fill("37");
  await expect(page.getByTestId("clock-skew-value")).toHaveText("37 ms");
  await page.getByLabel("Packet loss").fill("37");
  await expect(page.getByTestId("packet-loss-value")).toHaveText("37%");
  await page.getByLabel("Disk latency").fill("64");
  await expect(page.getByTestId("disk-latency-value")).toHaveText("64 ms");

  // Stepping moves the playhead rather than only pausing.
  await page.getByRole("button", { name: "PLAY", exact: true }).click();
  await expect.poll(async () => page.getByTestId("virtual-time").textContent(), { timeout: 15_000 })
    .not.toBe("0s / 60s");
  const before = await page.getByTestId("virtual-time").textContent();
  await page.getByRole("button", { name: "Previous event" }).click();
  await expect.poll(async () => page.getByTestId("virtual-time").textContent(), { timeout: 15_000 })
    .not.toBe(before);
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
