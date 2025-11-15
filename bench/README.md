# Benchmarks

`cc-bench` is a deterministic, closed-loop local harness. It publishes the
workload, client count, value size, seed, environment, config hash, and
integer latency percentiles in JSON. The current driver exercises the local
state-machine model; it does not claim a real replicated-cluster number.

Examples:

```sh
cargo run --release -p cc-bench -- --workload A --clients 1 --ops 10000 \
  --output bench/results/local.json
cargo run --release -p cc-bench -- repro bench/results/local.json
```

`scripts/ci/group-commit-curve.sh` sweeps 1, 2, 4, 8, and 16 clients and appends
one row per point to the checked-in `results/perf-trend.csv`. What that data
currently shows — and, more usefully, what it does not — is written up in
[writeup 07](../docs/writeups/07-benchmark-honesty.md).

The checked-in `bench.html` page accepts a report JSON file in the browser.
Real hardware reports must include the filesystem, durability mode, topology,
and the caveat that all-local loopback replication understates network cost.
