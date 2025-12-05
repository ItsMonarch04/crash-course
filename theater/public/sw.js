// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
// The cache name carries the build version, and `check-version-coherence.mjs`
// asserts it matches. That coupling is load-bearing: the previous constant
// never changed, so a returning visitor kept the old `index.html` forever and
// with it the old hashed asset and wasm URLs. A deploy could not reach anyone
// who had already loaded the theater once.
const CACHE = "crash-course-theater-v0.15.8-abi2";

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE).then(() => self.skipWaiting()));
});

// Drop every cache from an earlier build, then take over open tabs.
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE).map((key) => caches.delete(key))))
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  // Navigations go to the network first so a new deploy is picked up on the
  // next visit; the cache is the offline fallback, not the source of truth.
  if (request.mode === "navigate") {
    event.respondWith(
      fetch(request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE).then((cache) => cache.put(request, copy));
          return response;
        })
        .catch(() => caches.match(request).then((cached) => cached || Promise.reject(new Error("offline")))),
    );
    return;
  }

  // Assets are content-hashed by Vite, so cache-first is safe for them.
  event.respondWith(
    caches.match(request).then(
      (cached) =>
        cached ||
        fetch(request).then((response) => {
          if (response.ok) {
            const copy = response.clone();
            caches.open(CACHE).then((cache) => cache.put(request, copy));
          }
          return response;
        }),
    ),
  );
});
