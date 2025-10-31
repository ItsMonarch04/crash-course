# Crash Course

Crash Course is a from-scratch distributed-database laboratory. The database
is the excuse; the deterministic simulator, checker, and browser theater are
the proof that the failure cases are understood.

The project is deliberately honest about its status: it is an educational lab
tool under active construction, not a production database and not a benchmark
claim. The core is designed as synchronous, host-independent state machines
that can run in a simulator, a real host, or WebAssembly.

## Try the current workspace

```sh
cargo run -p cc-swarm -- --selfcheck
cargo run -p cc-node --bin ccdb -- --help
./scripts/preflight.sh
./scripts/demo.sh
cargo run --release -p cc-bench -- --workload A --clients 1 --ops 10000
```

The repository currently contains:

- `ccdb`, a bounded RESP lab host with a CRC-checked restart journal;
- a deterministic simulator with fault plans, traces, checkers, and a shrinker;
- a static browser theater with the ABI-1 JSON facade, topology canvas,
  re-execution timeline, shareable scenarios, and museum loader;
- an empty-by-default museum manifest that accepts only pinned real traces; and
- writeups, limitations, and disclosed local-model benchmark reports.

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

**Version:** v0.0.2
