# ADR 0014: Durable continuations (D05)

Status: Accepted — implementation pending

Dependent sends and client replies wait for the matching durable write and
fsync completion. A failed critical write discards the remaining continuation,
emits no dependent external effect, and fail-stops the node. `cc-log` is the
single owner of hard state, Raft entries, truncation, and snapshot marks.

This decision intentionally rejects effect-list ordering as a persistence
receipt; the continuation and fault-injection matrix remains to be built.
