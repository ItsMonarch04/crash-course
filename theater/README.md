# Theater

The theater is a static Vite application. It keeps the topology and timeline
hot paths on Canvas2D, uses React for controls and inspectors, honors reduced
motion, loads versioned trace fixtures from `public/fixtures/`, and validates
the pinned museum manifest from `public/exhibits/`. The current app exposes
the ABI-1 scenario shape through serializable seed/profile URLs; I run
native↔wasm equivalence as a CI gate until a browser toolchain is available.

```sh
npm ci
npm run dev
npm run build
npm run test:e2e
```
