# ADR-0004: Keep the core dependency-light

- Status: Accepted
- Date: 2025-11-10

## Context

The initial workspace was built in an offline environment, so its
standard-library-only core was partly a constraint of the build environment.
The network is now available, which makes it necessary to decide whether the
dependency-free shape was merely temporary or part of the artifact's design.

## Decision

Keep `cc-core`, `cc-env`, `cc-sim`, `cc-wal`, `cc-store`, `cc-raft`, `cc-kv`,
`cc-cluster`, `cc-resp`, and `cc-checker` standard-library-only permanently.
The deterministic core's small dependency surface is now a teaching and
replayability feature, not an accident.

`wasm-bindgen` is admitted only at the `cc-wasm` host boundary under
ADR-0003. `tokio` remains out of the core and the current dependency-light
real host remains the accepted implementation under ADR-0002. New testing
libraries are not added merely for convenience; the existing deterministic
generators and focused property-style tests remain the default. Any future
dev-dependency must be justified by a measurable fixture gap and recorded in a
new decision or an amendment to this one.

## Consequences

Core builds stay portable and inspectable, with no dependency-resolution drift
between native simulation and WebAssembly. Some test infrastructure remains
hand-rolled, and the theater build carries the explicit exception and its
toolchain cost. Dependency additions outside the stated exception require a
new decision before implementation.

## Alternatives considered

- Admit a general property-testing stack to every crate. Rejected because the
  current mini-PBT approach is sufficient for the core and keeps reproducible
  builds narrow.
- Move the core to an async runtime. Rejected; host scheduling must not enter
  the synchronous state machines.
- Keep the zero-dependency rule as an accidental offline workaround. Rejected;
  the constraint is now an intentional project property.

## Supersedes / superseded by

None.
