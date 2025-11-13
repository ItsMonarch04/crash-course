# Crash Course

Crash Course is a from-scratch distributed-database laboratory. The database
is the excuse; the deterministic simulator, checker, and browser theater are
the proof that the failure cases are understood.

The project is deliberately honest about its status: it is an educational lab
tool under active construction, not a production database and not a benchmark
claim. The core is designed as synchronous, host-independent state machines
that can run in a simulator, a real host, or WebAssembly.

![The browser theater running a five-node cluster, killing the leader, and
watching the survivors elect a new one without losing an acknowledged write.](theater/public/crash-course.gif)

The animation above is generated, not staged: `./scripts/record-gif.sh` drives
the real theater with Playwright and encodes the captured frames.

## Try the current workspace

```sh
cargo run -p cc-swarm -- --selfcheck
cargo run -p cc-node --bin ccdb -- --help
./scripts/preflight.sh
./scripts/demo.sh
cargo run --release -p cc-bench -- --workload A --clients 1 --ops 10000
```

The repository currently contains:

- `ccdb`, a bounded RESP lab host with a CRC-checked restart journal, three-node
  TCP replication, leader redirects, snapshot catch-up, and durable fsync
  failure shims;
- a deterministic simulator that composes real `cc-cluster::Node` instances,
  drives workload actors, captures client histories, checks invariants, runs
  campaigns, shrinks reproduced failures, searches trace n-gram coverage,
  reports reachability beacons, and explains first semantic trace divergence;
- a persistent `wasm-bindgen` bridge and browser theater with live topology,
  inspector, timeline/scrubbing, fault injection, verdict, and share-URL
  panels, guided lessons, embeddable mode, and a clickable double-run trace
  proof; traces can also be exported as standalone SVG sequence diagrams;
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

## Contributing

See [the contribution guide](docs/contributing.md), the numbered decisions in
[`docs/adr/`](docs/adr/), and the pull-request checklist. Every change needs
tests and a clean local preflight. Persisted or wire-format changes require a
versioned decision record.

## License

AGPL-3.0-only. See [LICENSE](LICENSE).

---

**Version:** v0.9.2
