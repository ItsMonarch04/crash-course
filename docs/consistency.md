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

Explicit replicated retries carry a `(namespace, ClientId, RequestSeq)`.
`cc-cluster::SessionTable` caches both canonical CCKV command bytes and the
canonical CCKR reply, so a duplicate is replayed without reapplying its
mutation; a same sequence with different bytes is rejected and cannot become a
cached success. The exactly-once window ends after policy-defined log-time
inactivity; clients must use a fresh identity after that point. Ordinary host
RESP commands are still adapter-local and must not be described as
reconnect-stable exactly-once requests.

The real-host spelling is `CC.REQUEST <client-u64> <sequence-u64> <write-command>
[args...]`. Both identity numbers must be nonzero. It accepts exactly one
state-changing command; reads and a multi-key `DEL` are rejected rather than
being split into several hidden requests. A reconnect may use another socket,
but the durable identity remains the caller's values, never the host's
connection route counter.

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

## Claim receipts

| Claim | Enforcing receipt |
|---|---|
| One leader per term and committed-prefix safety | [`cc-raft` invariant evaluators and `message_soup_campaign_100k_schedules`](../crates/cc-raft/src/lib.rs) |
| ReadIndex requires a fresh quorum round and current-term no-op | [`trap_readindex_noop` and stale-round tests](../crates/cc-raft/src/lib.rs) |
| Followers reject writes instead of serving them locally | [`node_starts_as_follower_and_rejects_writes_until_leader`](../crates/cc-cluster/src/lib.rs) |
| Duplicate explicit requests return the cached result | [`SessionTable` and cluster receipts](../crates/cc-cluster/src/lib.rs) |
| Applied index changes atomically with state | [`trap_applied_index_atomicity`](../crates/cc-kv/src/lib.rs) |
| Replica clocks do not change TTL visibility | [`trap_ttl_replica_clock_uses_leader_time_only`](../crates/cc-kv/src/lib.rs) |
| Scans are checked as snapshot-legal within the call window | [`scan_is_checked_as_a_snapshot_legal_operation`](../crates/cc-checker/src/lib.rs) |
| Open timeouts branch both ways | [`trap_open_op_semantics_allows_timeout_to_take_effect` and `trap_open_op_can_be_dropped_in_the_other_direction`](../crates/cc-checker/src/lib.rs) |
| Checker budget exhaustion is reported as undecided | [`budget_exhaustion_is_explicitly_undecided`](../crates/cc-checker/src/lib.rs) |
| Client histories are checked, not synthesized | [`calm_five_node_cluster_elects_and_captures_real_history`](../crates/cc-swarm/src/lib.rs) |
| Crash/restart rebuilds from durable simulated bytes | [`scripted_leader_crash_restart_catches_up_from_surviving_cluster`](../crates/cc-swarm/src/lib.rs) |
