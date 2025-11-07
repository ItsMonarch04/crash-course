// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh
const CACHE = "crash-course-theater-v1";
self.addEventListener("install", (event) => event.waitUntil(caches.open(CACHE)));
self.addEventListener("fetch", (event) => event.respondWith(
  caches.match(event.request).then((cached) => cached || fetch(event.request).then((response) => {
    const copy = response.clone();
    caches.open(CACHE).then((cache) => cache.put(event.request, copy));
    return response;
  }))
));
