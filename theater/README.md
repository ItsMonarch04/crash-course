# Theater

The theater is a static Vite application. It keeps the topology and timeline
hot paths on Canvas2D, uses React for controls and inspectors, loads versioned
trace fixtures from `public/fixtures/`, and validates the pinned museum
manifest from `public/exhibits/`. Theater ABI 2 keeps complete, bounded
simulator checkpoints at five-second virtual-time intervals, restores the
nearest earlier image, and replays no more than five seconds. `state()` is a
bounded summary; trace events cross the wasm boundary through capped
`trace_page()` responses. Serializable seed/profile URLs remain the scenario
surface, and native↔wasm equivalence remains a CI gate. ABI-1 museum exhibits
remain read-only inputs through the museum compatibility adapter. ABI-2
exhibits declare positive, finite `horizon_ns` and `checkpoint_interval_ns`;
imports needing more than 13 checkpoints are rejected with a regeneration
instruction. Unknown schema or Theater ABI values are visible import errors,
never an apparently empty wing.

The generated web bridge is rebuilt with `./scripts/build-wasm.sh` using the
repository-pinned `wasm-bindgen-cli 0.2.105`. Source imports require the
checked-in `src/wasm/cc_wasm.{js,d.ts}` pair. The wasm binary and `dist/` stay
ignored; CI regenerates the bridge and treats a JS/type-definition diff as a
failure. Generated bridge files are never hand-edited.

The published site is a GitHub Pages project page under `/crash-course/`, so
`base` is `./` and every runtime fetch resolves against the document rather
than the origin. `npm run dev` serves from the origin root, where the two are
indistinguishable, so `tests/subpath.spec.ts` builds the bundle and serves it
under a subpath: a root-absolute URL leaves the deployed theater with no
engine, and only that test sees it.

Every visible interactive control has a row in
`tests/control-contract.tsv`; `scripts/ci/control-contract.mjs` rejects either
a rendered control without a contract row or a stale row. The disk-latency
slider maps to the simulator's persistent `SlowDisk` service-time fields;
zero clears the selected node's added delay. WAL, file-backed reads, rename,
and directory-sync paths consume the corresponding modeled service fields.
The older one-shot EIO fault remains a
separate scenario injection and is never presented as latency.

## Accessibility

The canvas has a live, semantic node table with node id, role, term, commit,
applied index, and durable bytes. The table is visually exposed below 768 CSS
px and for print, while remaining available to assistive technology at wider
viewports. Use the **Selected node** control rather than the canvas when using
a keyboard.

Keyboard map:

- `Tab` moves through every control, including timeline and node selection.
- `Space` toggles play/pause when focus is not in a text field or select.
- `K` crashes the current leader when focus is not in a text field or select.
- Timeline range keys, including `Home` and `End`, scrub virtual time.

The **MOTION** button cycles `system`, `on`, and `off`, persisting the choice
in local storage. System mode follows `prefers-reduced-motion`; light/dark,
forced-colors, narrow-screen, and print styles preserve the text mirror.
`scripts/ci/contrast.mjs` verifies the documented text and indicator token
pairs for both themes, and `contrast-fixture.mjs` verifies its failing fixture.

```sh
npm ci
npm run dev
npm run build
npm run test:e2e
```
