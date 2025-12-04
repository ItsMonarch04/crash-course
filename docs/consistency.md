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

`MULTI` queues a closed set of batchable RESP commands on one connection;
`EXEC` submits them as one CCKV v3 batch only after the replicated
`ATOMIC_BATCH` feature has committed. Every subcommand observes its
predecessors using the one replicated leader timestamp, and either every
mutation becomes visible or the complete batch returns a deterministic
failing-index error without publishing any child mutation. `DISCARD` and a
connection close drop only the local queue. An empty `EXEC` returns an empty
array without proposing an entry. There are no interactive transactions, no
cross-group atomicity, no snapshot isolation, and no `WATCH` optimistic
concurrency. A plain `MULTI`/`EXEC` exchange has the documented lost-reply
at-least-once edge. A one-shot `BATCH` accepts one nested RESP array of
subcommand arrays; wrapping that exact form as `CC.REQUEST <client> <sequence>
BATCH <nested-array>` makes the complete batch one reconnect-stable dedup unit.

`READ STALE GET key` is an explicit local observation, returned as
`["STALE", reply, applied_index, applied_term, read_time, last_contact_ms]`.
It makes no linearizability or time-bounded freshness claim. `READ FOLLOWER
GET|TTL` is separately tagged as `["FOLLOWER", reply, read_index,
applied_index, applied_term, read_time]`. It is admitted only on a current
v3 `FOLLOWER_READ` CCHL connection; the leader issues its grant after a fresh
ReadIndex quorum, and the follower waits until that index is locally applied
before evaluating the command at the leader-supplied timestamp. A missing
leader, v2/featureless connection, term/config change, or timeout returns
`TRYAGAIN` with a leader hint instead of labelling local state linearizable.

Authentication, authorization, TLS, sharding, and cross-region replication
are outside this laboratory's core promise.

The linearizability checker in `cc-checker` tests completed operations and
branches open timeouts as either taking effect or not taking effect. The
simulator's invariant checker separately enforces trace order and bounded
liveness, so a checker `undecided` result is never silently reported as safe.

## Client session guarantees

The checker also reports four client-scoped guarantees over completed,
sequential, strong-mode `UserRequest` operations. A retry with the same
`(client, sequence)` is one logical request; failed commands do not establish
a write, a successful delete establishes nil, and a nil read is an ordinary
observation. Open/time-out operations, diagnostic final or replica probes,
stale-local reads, and AdminRequest workflows are outside this report.

- **Read-your-writes:** after a client's acknowledged mutation, its later
  strong read cannot return the pre-mutation state when no other successful
  mutation overlaps or intervenes.
- **Monotonic reads:** two sequential strong reads by one client cannot move
  between states without an intervening successful mutation that could
  explain the change.
- **Monotonic writes:** the request sequence of sequential client mutations
  cannot regress. An equal sequence is a retry, not a second write.
- **Writes-follow-reads:** a sequential mutation following a strong read
  cannot carry a request sequence older than that read.

For a well-formed client whose completed operations are all strong,
non-diagnostic, sequential in certain real-time order, and included in the
same history, a `Linearizable` verdict must satisfy all four predicates. The
campaign runner treats a contradiction as a checker invariant failure. Mixed
consistency histories are deliberately excluded from that implication rather
than being falsely condemned.

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
