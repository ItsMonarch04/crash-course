# Theater

The theater is a static Vite application. It keeps the topology and timeline
hot paths on Canvas2D, uses React for controls and inspectors, loads versioned
trace fixtures from `public/fixtures/`, and validates the pinned museum
manifest from `public/exhibits/`. The current app exposes the ABI-1 scenario
shape through serializable seed/profile URLs; native↔wasm equivalence remains a
CI gate.

Every visible interactive control has a row in
`tests/control-contract.tsv`; `scripts/ci/control-contract.mjs` rejects either
a rendered control without a contract row or a stale row. The disk-latency
slider maps to the simulator's persistent `SlowDisk` service-time fields;
zero clears the selected node's added delay. Current WAL write/fsync paths
consume that delay, while N3 file-backed storage will consume its remaining
read/rename/directory-sync fields. The older one-shot EIO fault remains a
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
