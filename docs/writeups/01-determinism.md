# 01 — Determinism is a boundary

The core does not read a wall clock, spawn a task, or pull ambient randomness.
Hosts translate those effects into `Input` values and preserve the resulting
`Effect` values. This makes a failing schedule a data file: seed, input order,
trace bytes, and the smallest retained fault plan are enough to replay it.

The practical consequence is slightly unusual: APIs are designed around what
must be recorded before they are designed around convenience. A test that
needs the current time receives a `Time`; a retry that needs randomness receives
a domain-separated stream. The theater can therefore scrub by re-executing
the same inputs instead of trying to reverse a live system.
