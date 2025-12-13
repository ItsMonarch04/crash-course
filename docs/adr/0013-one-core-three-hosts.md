# ADR 0013: One core and three hosts (D04)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

`cc-cluster::Node` is the sole consensus/state-machine composition used by the
simulator, wasm wrapper, and TCP adapter. Its host boundary is `cc-env` values
and `BlockSource`; scheduling belongs to a standard-library-only host driver.
`cc-log` owns durable Raft state. The legacy replication protocol,
`commands.log`, duplicate command vocabularies, and out-of-band state copies
are absent; the architecture receipt is consolidated in ADR-0017.
