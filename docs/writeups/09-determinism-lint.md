# 09 — Determinism lint: exporting the perimeter

`cc-detlint` turns Crash Course's determinism constitution into a reusable,
zero-dependency command. `check` scans an explicit set of Rust paths for
ambient clocks, random sources, scheduler dependencies, randomized maps, and
floating-point types. `double-run` executes any trace-producing command twice
and compares stdout byte for byte; stderr remains available for host/compiler
diagnostics and is not part of the trace contract.

The perimeter is explicit because host adapters legitimately use clocks,
threads, and sockets. A repository-wide ban would either reject the real host
or, worse, encourage exceptions broad enough to hide core mistakes. CI passes
only the deterministic state-machine crates to the scanner:

```sh
./scripts/ci/forbidden-grep.sh
cargo run --locked -p cc-detlint -- double-run -- \
  cargo run --locked --quiet -p cc-swarm -- --determinism
```

The tool is intentionally syntactic. It is fast, auditable, and catches the
common accidental imports; the double-run gate remains the semantic backstop.
