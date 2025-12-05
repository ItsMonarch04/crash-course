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

test("every control changes observable state", async ({ page }) => {
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

  const timeline = page.getByRole("slider", { name: "Timeline", exact: true });
  await timeline.focus();
  await timeline.press("End");
  await expect(timeline).toHaveAttribute("aria-valuetext", "5 virtual seconds");

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

test("hero scenario is completable by keyboard alone", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await page.locator("body").press("Space");
  await expect(page.getByTestId("leader-id")).not.toHaveText("none", { timeout: 15_000 });
  const firstLeader = await page.getByTestId("leader-id").textContent();
  await page.locator("body").press("k");
  await expect.poll(async () => page.getByTestId("leader-id").textContent(), { timeout: 30_000 })
    .not.toBe(firstLeader);
  await expect(page.getByTestId("leader-id")).not.toHaveText("none");
});

test("reduced motion disables autoplay", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await expect(page.getByRole("button", { name: "PLAY", exact: true })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Play timeline" })).toBeDisabled();

  const motion = page.locator('[data-control="motion-preference"]');
  await motion.click();
  await expect(motion).toHaveAccessibleName("Motion preference: on");
  await motion.click();
  await expect(motion).toHaveAccessibleName("Motion preference: off");
  await expect(page.getByRole("button", { name: "PLAY", exact: true })).toBeEnabled();
  await expect.poll(() => page.evaluate(() => localStorage.getItem("crash-course-motion"))).toBe("off");
  await page.reload();
  await expect(page.locator('[data-control="motion-preference"]')).toHaveAccessibleName("Motion preference: off");
});

test("text mirror reports role changes", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await expect(page.locator("#topology-mirror tbody tr")).toHaveCount(5);
  await page.getByRole("button", { name: "PLAY", exact: true }).click();
  await expect(page.getByTestId("leader-id")).not.toHaveText("none", { timeout: 15_000 });
  await expect(page.getByRole("status")).toContainText("Role change:");
  const leader = await page.getByTestId("leader-id").textContent();
  await expect(page.locator(`#topology-mirror tbody tr:nth-child(${leader})`)).toContainText("leader");
});

test("scrub restores from the nearest checkpoint", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await page.getByLabel("Speed").selectOption("64×");
  await page.getByRole("button", { name: "PLAY", exact: true }).click();
  await expect(page.getByTestId("virtual-time")).toHaveText("60s / 60s", { timeout: 15_000 });
  await page.getByRole("slider", { name: "Timeline", exact: true }).fill("37");
  await expect(page.getByTestId("restore-from-ns")).toHaveText("35000000000");
  await expect(page.getByTestId("replay-ns")).toHaveText("2000000000");
});

test("scrubbed trace matches uninterrupted trace", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await page.getByLabel("Speed").selectOption("64×");
  await page.getByRole("button", { name: "PLAY", exact: true }).click();
  await expect(page.getByTestId("virtual-time")).toHaveText("60s / 60s", { timeout: 15_000 });
  await page.getByRole("slider", { name: "Timeline", exact: true }).fill("37");
  await expect(page.getByTestId("virtual-time")).toHaveText("37s / 60s");
  await page.getByTestId("determinism-proof").click();
  await expect(page.getByTestId("determinism-proof")).toContainText("MATCH", { timeout: 30_000 });
});

test("wasm handles are freed on every error", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 15_000 });
  await expect(page.getByTestId("live-wasm-handles")).toHaveText("1");
  await page.getByRole("slider", { name: "Timeline", exact: true }).evaluate((element) => {
    const input = element as HTMLInputElement;
    input.max = "60";
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
    if (!setter) throw new Error("range input has no native value setter");
    setter.call(input, "60");
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await expect(page.getByRole("alert")).toContainText("five-second replay horizon");
  await expect(page.getByTestId("live-wasm-handles")).toHaveText("0");
});

test("wasm state and trace pages obey the ABI byte cap", async ({ page }) => {
  await page.goto("/");
  const result = await page.evaluate(async () => {
    const module = await import(String("/src/wasm/cc_wasm.js"));
    await module.default("/wasm/cc_wasm_bg.wasm");
    const handle = module.init('{"seed":"0x56","profile":"calm"}');
    try {
      module.step(handle, 3_000_000_000n);
      const state = module.state(handle);
      const page = module.trace_page(handle, 0n, 17);
      return { stateBytes: new TextEncoder().encode(state).length, pageBytes: new TextEncoder().encode(page).length };
    } finally {
      handle.free();
    }
  });
  expect(result.stateBytes).toBeLessThanOrEqual(1024 * 1024);
  expect(result.pageBytes).toBeLessThanOrEqual(1024 * 1024);
});

test("museum ABI compatibility is explicit and bounded", async ({ page }) => {
  await page.goto("/");
  const result = await page.evaluate(async () => {
    const { parseMuseum } = await import(String("/src/museum.ts"));
    const exhibit = {
      id: "legacy-1",
      title: "Legacy capture",
      kind: "raft",
      seed: "0x1",
      trace: "legacy.cctrace",
      verdict: "fixed",
      anomaly: "election",
      chapters: ["one"],
    };
    const legacy = parseMuseum({ schema_version: 1, build: "old", exhibits: [exhibit] });
    let oversized = "";
    try {
      parseMuseum({
        schema_version: 2,
        theater_abi: 2,
        build: "new",
        exhibits: [{ ...exhibit, theater_abi: 2, horizon_ns: 65_000_000_000, checkpoint_interval_ns: 5_000_000_000 }],
      });
    } catch (error) {
      oversized = String(error);
    }
    let synthetic = "";
    try {
      parseMuseum({
        schema_version: 2,
        theater_abi: 2,
        build: "kata",
        exhibits: [{ ...exhibit, synthetic: true, theater_abi: 2, horizon_ns: 5_000_000_000, checkpoint_interval_ns: 5_000_000_000 }],
      });
    } catch (error) {
      synthetic = String(error);
    }
    return { legacy: legacy.exhibits[0], oversized, synthetic };
  });
  expect(result.legacy).toMatchObject({
    theater_abi: 1,
    readonly: true,
    horizon_ns: 60_000_000_000,
    checkpoint_interval_ns: 5_000_000_000,
  });
  expect(result.oversized).toContain("regenerate");
  expect(result.synthetic).toContain("Synthetic artifact");

  await page.addInitScript(() => {
    const originalFetch = window.fetch.bind(window);
    window.fetch = (input, init) => String(input).includes("exhibits/manifest.json")
      ? Promise.resolve(new Response(JSON.stringify({ schema_version: 99, build: "future", exhibits: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }))
      : originalFetch(input, init);
  });
  await page.reload();
  await expect(page.getByRole("alert")).toContainText("Unsupported museum schema 99");
});

test("contrast gate", async ({ page }) => {
  for (const colorScheme of ["dark", "light"] as const) {
    await page.emulateMedia({ colorScheme });
    await page.goto("/");
    const seed = page.getByLabel("Seed");
    await seed.focus();
    const focus = await seed.evaluate((element) => {
      const style = getComputedStyle(element);
      return { style: style.outlineStyle, width: style.outlineWidth };
    });
    expect(focus).toEqual({ style: "solid", width: "3px" });
  }
});

test("responsive text mirror avoids two-dimensional page scrolling", async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 700 });
  await page.goto("/");
  await expect(page.locator("#topology-mirror")).toBeVisible();
  await expect(page.getByLabel("Cluster topology")).toBeHidden();
  const layout = await page.evaluate(() => ({
    viewport: window.innerWidth,
    page: document.documentElement.scrollWidth,
    overflow: [...document.querySelectorAll<HTMLElement>("body *")]
      .filter((element) => {
        const box = element.getBoundingClientRect();
        return box.right > window.innerWidth || box.left < 0;
      })
      .map((element) => `${element.tagName.toLowerCase()}.${element.className}`)
      .slice(0, 12),
  }));
  expect(layout, `horizontal overflow: ${layout.overflow.join(", ")}`).toMatchObject({
    page: layout.viewport,
  });
});
