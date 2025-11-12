# Consistency and failure semantics

Crash Course exposes a single replication group. A successful write is
linearized when the Raft leader has a current-term entry committed by a voter
majority and the state machine applies it. A leader read uses the full local
ReadIndex fixture: a current-term no-op is required, a quorum heartbeat round
confirms the read index, and the request waits until the state machine has
applied that index. The confirmation round is tagged: each read increments a
round counter that the leader stamps on its appends and followers echo back,
so only acknowledgements raised *after* the read index was fixed count toward
the quorum. An acknowledgement already in flight when the read arrived proves
nothing about current leadership and is ignored. Followers return an explicit
not-leader result rather than pretending to provide a local read.

Client retries carry `(ClientId, RequestSeq)`. The replicated session table
caches the last reply, so a duplicate sequence returns the original reply and
an older sequence is rejected. The exactly-once window ends when the replicated
session expires after log-time inactivity; clients must use a fresh identity
after that point.

TTL deadlines are derived from the leader timestamp in the replicated entry.
Replica wall clocks and clock skew do not affect visibility. Scan results are
ordered and snapshot-consistent within one call; cursor pages are independent.
Multi-key commands are a sequence of independent entries, not an atomic
transaction. Authentication, authorization, TLS, sharding, and cross-region
replication are outside this laboratory's core promise.

The linearizability checker in `cc-checker` tests completed operations and
branches open timeouts as either taking effect or not taking effect. The
simulator's invariant checker separately enforces trace order and bounded
liveness, so a checker `undecided` result is never silently reported as safe.
