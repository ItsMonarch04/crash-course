# Crash Course

Crash Course is a from-scratch distributed-database laboratory. The database
is the excuse; the deterministic simulator, checker, and browser theater are
the proof that the failure cases are understood.

The project is deliberately honest about its status: it is an educational lab
tool under active construction, not a production database and not a benchmark
claim.

The consensus core is written as synchronous, host-independent state machines,
and the same core runs in three places: inside the deterministic simulator,
compiled to WebAssembly inside the browser theater, and behind the real
`ccdb` socket/filesystem adapter. The real adapter drives `cc-host::Driver`,
uses the same Raft message codec and durability continuations, and performs a
checked peer hello before it accepts a peer frame. Its storage and snapshot
work are still intentionally incomplete; the exact boundary is spelled out in
[limitations](docs/LIMITATIONS.md).

![The browser theater running a five-node cluster, killing the leader, and
watching the survivors elect a new one without losing an acknowledged write.](theater/public/crash-course.gif)

The animation above is generated, not staged: `./scripts/record-gif.sh` drives
the real theater with Playwright and encodes the captured frames.

**[Open the theater →](https://itsmonarch04.github.io/crash-course/)** The same
WebAssembly engine runs in your browser: resize the cluster, kill the leader,
step the timeline, inject faults, and share the resulting run as a URL. No
toolchain required.

## Try the current workspace

```sh
cargo run -p cc-swarm -- --selfcheck
cargo run -p cc-node --bin ccdb -- --help
./scripts/preflight.sh
./scripts/demo.sh
cargo run --release -p cc-bench -- --workload A --clients 1 --ops 10000
```

The repository currently contains:

- `ccdb`, a bounded RESP lab host that adapts the shared Raft driver to TCP,
  CCHL/CCPF peer connections, a framed durable Raft WAL, leader redirects, and
  fail-closed durable-write/fsync shims;
- a deterministic simulator that drives real `cc-cluster::Node` instances
  only through `cc-host::Driver`,
  drives workload actors, captures client histories, checks invariants, runs
  campaigns, catches wiped nodes up through ordinary Raft replication without
  an out-of-band state copy, shrinks reproduced
  failures, searches trace n-gram coverage, gates on reachability beacons, and
  explains first semantic trace divergence;
- a bounded model checker that exhaustively explores the reachable `cc-raft`
  state space within printed log, term, message, and depth bounds;
- a persistent `wasm-bindgen` bridge and browser theater with a resizable
  cluster, live topology, node inspector, timeline stepping and scrubbing,
  fault injection, verdict, and share-URL panels, guided lessons, embeddable
  mode, and a clickable double-run trace proof; traces can also be exported as
  standalone SVG sequence diagrams;
- a Redis-shaped single-key RMW family, deep self-check and environment doctor,
  Prometheus/dashboard endpoint, Rust fault proxy, and three-node Compose lab;
- `cc-detlint`, the reusable zero-dependency determinism scanner and double-run
  harness;
- an empty-by-default museum manifest that accepts only pinned real traces; and
- normative format notes, named safety traps, campaign tooling, limitations,
  and disclosed local-model benchmark reports.

Correctness work comes before performance claims. Supported development targets
are Linux x86_64, macOS arm64, and WebAssembly; Windows is not a supported CI
target. The real listener is a lab tool and does not provide authentication,
authorization, encryption, or TLS. See [limitations](docs/LIMITATIONS.md)
before interpreting any result.

## Going deeper

The write-ups are the point of the project — each one names a failure mode and
shows the machinery that catches it.

- [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) — what this does and does not
  prove; read it before interpreting any result
- [`docs/writeups/`](docs/writeups/) — the nine essays:
  [determinism as a boundary](docs/writeups/01-determinism.md),
  [fsync and the page cache](docs/writeups/02-fsync-page-cache.md),
  [Raft traps worth naming](docs/writeups/03-raft-traps.md),
  [what the simulator cannot promise](docs/writeups/04-real-faults.md),
  [histories as proof](docs/writeups/05-linearizability.md),
  [membership as a two-quorum story](docs/writeups/06-membership.md),
  [benchmarking without theater](docs/writeups/07-benchmark-honesty.md),
  [the flight recorder](docs/writeups/08-theater.md), and
  [exporting the determinism perimeter](docs/writeups/09-determinism-lint.md)
- [`docs/consistency.md`](docs/consistency.md) — what a successful write and a
  leader read actually guarantee, and the ReadIndex fixture behind them
- [`docs/sim.md`](docs/sim.md) — scenarios as data: seeds, profiles, fault
  plans, and the replay/shrink/diff commands
- [`docs/formats.md`](docs/formats.md) — every persisted and wire format, with
  magic values, versions, and bounds
- [`docs/compatibility.md`](docs/compatibility.md) — the current compatibility
  boundary and fixture-manifest contract
- [`docs/calibration.md`](docs/calibration.md) — named simulator calibration
  profiles, validation results, and residuals
- [`docs/ops.md`](docs/ops.md) — running the real `ccdb` host: shared-driver
  WAL layout, peer handshake, and current recovery limits
- [`docs/adr/`](docs/adr/) — numbered decisions, append-only
- [`docs/talk-kit.md`](docs/talk-kit.md) — a fifteen-minute run of show, with
  [slides](docs/crash-course-talk-kit.pptx)
- [`theater/README.md`](theater/README.md) — the browser theater
- [`bench/README.md`](bench/README.md) — the benchmark harness and what it
  refuses to claim
- [`exhibits/README.md`](exhibits/README.md) — the museum, empty by default and
  deliberately so

## Contributing

See [the contribution guide](docs/contributing.md), the numbered decisions in
[`docs/adr/`](docs/adr/), and the pull-request checklist. Every change needs
tests and a clean local preflight. Persisted or wire-format changes require a
versioned decision record.

## License

Copyright (c) 2025 Sidakpreet Singh.

Crash Course is free software: you may redistribute it and modify it under the
terms of the GNU Affero General Public License, **version 3 only** — not any
later version. The complete license text is in [LICENSE](LICENSE); the SPDX
identifier is `AGPL-3.0-only`.

The Affero clause is the point rather than an accident. The browser theater is
this project's main surface, and section 13 means anyone who serves a modified
theater over a network owes its users the corresponding source. A fork that
publishes an altered simulator as a website cannot keep those changes closed.

---

**Version:** v0.15.4
