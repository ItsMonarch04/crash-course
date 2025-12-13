# ADR 0014: Durable continuations (D05)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

Dependent sends and client replies wait for the matching durable write and
fsync completion. A failed critical write discards the remaining continuation,
emits no dependent external effect, and fail-stops the node. `cc-log` is the
single owner of hard state, Raft entries, truncation, and snapshot marks.

This decision intentionally rejects effect-list ordering as a persistence
receipt. The Driver continuation and fault-injection matrix now enforce the
boundary, as consolidated in ADR-0017.
