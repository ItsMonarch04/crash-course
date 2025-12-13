# ADR 0009: Replicated policy and configuration

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

Crash Course encodes immutable deterministic limits as canonical `CCPL` bytes.
The policy is part of a cluster's identity: a hash can reject an obviously
wrong disk quickly, but authoritative boundaries compare the entire canonical
record. Host-local scheduling and admission limits remain outside this value.

Membership changes are canonical `CCCF` configuration entries. Voters,
learners, joint old/new quorums, and peer addresses are reconstructed from the
surviving log projection; unknown and learner acknowledgements never count
toward election, commit, or ReadIndex quorum. A CheckQuorum window steps down
an isolated leader.

The original decision did not claim an admin surface or streamed durable
checkpoint transport. ADR-0017 records their implemented receipts.
