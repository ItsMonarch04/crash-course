# ADR-0003: Use wasm-bindgen for the theater bridge

- Status: accepted
- Date: 2025-11-10

## Context

The simulator and cluster core must become a real WebAssembly surface for the
theater. The committed shell currently has a JSON-shaped facade, but it does
not compile the persistent simulator or expose the state and fault controls
needed by the browser. The bridge must preserve deterministic state-machine
semantics while keeping JavaScript glue out of the consensus and storage
crates.

## Decision

Use `wasm-bindgen` for the `cc-wasm` boundary, as permitted by the dependency
policy. The bridge will expose a small ABI-versioned surface owned by
`cc-wasm`; the simulator, cluster, Raft, KV, and storage crates remain
host-independent and dependency-light. The generated JavaScript glue is a
theater build artifact, not a second execution model.

The bridge will own persistent handles and JSON conversion only. A browser
`step` advances one simulator instance by a virtual-time budget, and `inject`
adds fault-plan data to that same instance. Native and WebAssembly traces must
remain byte-identical for the equivalence gate before the theater claims live
cluster behavior.

## Consequences

The theater gets a conventional, maintainable boundary and does not require a
hand-written ABI encoder. `wasm-bindgen` and its build tooling become an
explicit exception to the core's standard-library-only rule. The generated
glue must be pinned and tested, and ABI changes require an explicit version
bump plus adapters for museum builds.

## Alternatives considered

- Hand-rolled `wasm32-unknown-unknown` exports and JavaScript glue would keep
  the dependency surface smaller but would add a second fragile ABI
  implementation to the project.
- Running a separate browser-only model would be simpler but would violate the
  project's one-core thesis and make native/browser equivalence meaningless.

## Supersedes / superseded by

None.
