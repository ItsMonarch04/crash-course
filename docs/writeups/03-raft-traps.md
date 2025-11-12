# 03 — Raft traps worth naming

The consensus core is intentionally sans-IO. A host persists hard state before
returning a vote, treats a same-term append as a step-down, commits only an
entry from the current term, and sends the leader no-op before starting a
quorum-confirmed ReadIndex round. Each rule has a named `trap_*` test because
the failure mode is more memorable than a generic “Raft works” test.

Snapshots are also a protocol, not a large byte slice: chunks carry an offset,
stale snapshots are ignored, an offset mismatch is visible in the trace, and
the state machine is restored only after the final chunk. Membership changes
use learners and a joint configuration so both majorities are explicit. The
named trap suite covers timer reset discipline, snapshot ordering, session and
applied-index atomicity, current-term commit, and membership transitions.
