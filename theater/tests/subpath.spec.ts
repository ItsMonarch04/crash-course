// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { expect, test } from "@playwright/test";
import { SUBPATH_BASE, SUBPATH_ORIGIN } from "../playwright.config";

// `npm run dev` and the rest of the suite serve the theater from the origin
// root, where a root-absolute URL is indistinguishable from a relative one.
// The published site is a GitHub Pages project page under `/crash-course/`,
// so every runtime fetch has to resolve against the document, not the origin.
// This is the only test that would have caught `fetch("/wasm/...")` shipping
// a theater whose engine could never load.
const SUBPATH_URL = `${SUBPATH_ORIGIN}${SUBPATH_BASE}`;

test("built site loads its engine when served from a deployment subpath", async ({ page }) => {
	const offBase: string[] = [];
	page.on("requestfinished", async (request) => {
		const url = new URL(request.url());
		if (url.origin !== SUBPATH_ORIGIN) return;
		if (!url.pathname.startsWith(SUBPATH_BASE)) offBase.push(url.pathname);
	});
	const failed: string[] = [];
	page.on("response", (response) => {
		if (response.status() >= 400) failed.push(`${response.status()} ${response.url()}`);
	});

	await page.goto(SUBPATH_URL);

	// The engine is the whole artifact. `RECORDED TRACE` means the wasm never
	// loaded and the page fell back to a fixture.
	await expect(page.getByTestId("engine-state")).toHaveText("LIVE SIM", { timeout: 30_000 });
	expect(failed, "no request may fail under a subpath deployment").toEqual([]);
	expect(offBase, "every request must stay under the deployment base").toEqual([]);
});
