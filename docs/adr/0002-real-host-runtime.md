# ADR-0002: Keep the real-host adapter dependency-light

- Status: accepted for the 0.1 lab host
- Date: 2025-11-05

## Context

The deterministic crates must remain host-independent, while the real mode
needs TCP listeners, a journal fsync boundary, peer framing, restart recovery,
and metrics. A runtime dependency would add a second resolution surface to a
plain-directory educational build and was not available in the offline build
environment.

## Decision

`cc-node` uses the standard-library TCP and thread primitives. Each accepted
client or peer gets a bounded worker, the journal calls `sync_data` before the
state-machine apply, and the metrics heartbeat is a dedicated host thread.
`cc-env` owns the versioned peer-frame codec so a future async adapter can
replace the host loop without changing the core vocabulary.

## Consequences

This keeps the workspace dependency-free outside its local crates and makes
the host easy to inspect. It is not a claim that the thread-per-connection
adapter is production scheduling. A future runtime migration must preserve the
frame bounds, journal ordering, restart semantics, and real-fault coverage,
then add a new decision record if it changes those surfaces.
