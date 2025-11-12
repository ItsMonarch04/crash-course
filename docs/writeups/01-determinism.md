# 01 — Determinism is a boundary

The core does not read a wall clock, spawn a task, or pull ambient randomness.
Hosts translate those effects into `Input` values and preserve the resulting
`Effect` values. `cc-swarm` now composes real `cc-cluster::Node` instances in
that boundary: the seed, materialized fault plan, workload replies, and trace
bytes are enough to replay a cluster run.

The practical consequence is slightly unusual: APIs are designed around what
must be recorded before they are designed around convenience. A test that
needs the current time receives a `Time`; a retry that needs randomness receives
a domain-separated stream. The theater advances one persistent WASM-backed
simulation, and scrub replays the same fault inputs from deterministic
checkpoints instead of trying to reverse a live system.
