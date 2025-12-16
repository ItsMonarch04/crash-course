# 07 — Benchmarking without theater

The benchmark harness reports a config hash beside every percentile. Workload
names, client count, value size, seed, repetitions, operating system, and
architecture are part of the report, while the local-model caveat is printed
into the JSON itself. `cc-bench repro <report.json>` regenerates the workload
shape rather than asking a reader to trust a copied number.

The first performance question is whether a change regresses this project on
the same machine and durability configuration. External systems are orientation
points only when their durability model and feature set are stated beside the
comparison.

## The concurrency sweep, and what it does not show

`scripts/ci/group-commit-curve.sh` runs the bench driver at 1, 2, 4, 8, and 16
clients and appends one row per point to `bench/results/perf-trend.csv`. Every
row carries its workload size, repetitions, seed, value size, operating system,
architecture, model caveat, and configuration hash.

The interesting result is a negative one. In the checked 2025-12-16 run,
median latency moves between 91 ns and 126 ns with no monotonic trend, and
throughput does not climb monotonically with concurrency. That is not a
group-commit curve. It is what a
closed-loop, in-process model looks like when the thing being measured is
faster than the noise floor of measuring it: there is no socket, no scheduler
contention, and no real `fsync` in the path, so added clients mostly add
sampling variance.

So the batching is deliberately left alone. Tuning a self-clocking batch
against numbers that cannot distinguish a real improvement from run-to-run
scatter would be tuning against noise, and shipping a "we improved the latency
floor" claim on this data would be exactly the theater this page exists to
refuse. A meaningful curve needs the real host under a real network with real
durability, and that measurement does not exist yet.
