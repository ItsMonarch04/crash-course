// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
import { defineConfig, devices } from "@playwright/test";

// The deployed site lives under a GitHub Pages project subpath, not at the
// origin root. Serving only the dev server at `/` hid a root-absolute wasm URL
// that resolved off the deployment and left the published theater with no
// engine, so the built bundle is also served under a subpath here.
export const SUBPATH_ORIGIN = "http://127.0.0.1:4174";
export const SUBPATH_BASE = "/crash-course/";

export default defineConfig({
	testDir: "./tests",
	use: { baseURL: "http://127.0.0.1:4173", ...devices["Desktop Chrome"] },
	webServer: [
		{
			command: "npm run dev -- --host 127.0.0.1 --port 4173",
			port: 4173,
			reuseExistingServer: true,
		},
		{
			command: `npm run build && npm run preview -- --base ${SUBPATH_BASE} --host 127.0.0.1 --port 4174`,
			url: `${SUBPATH_ORIGIN}${SUBPATH_BASE}`,
			reuseExistingServer: true,
			timeout: 180_000,
		},
	],
});
