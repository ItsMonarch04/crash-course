# ADR 0013: One core and three hosts (D04)

Status: Accepted — implementation pending

`cc-cluster::Node` is the sole consensus/state-machine composition used by the
simulator, wasm wrapper, and TCP adapter. Its host boundary is `cc-env` values
and `BlockSource`; scheduling belongs to a standard-library-only host driver.
`cc-log` will own durable Raft state. The legacy replication protocol,
`commands.log`, duplicate command vocabularies, and out-of-band state copies
must be removed before this decision becomes implemented.
