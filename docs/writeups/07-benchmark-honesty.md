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
