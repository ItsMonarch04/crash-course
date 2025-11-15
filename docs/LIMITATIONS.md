# Limitations

This page is part of the product, not an apology. The project is an
educational, deterministic database lab and its guarantees are deliberately
narrow.

- **The real host does not run the consensus core.** `ccdb` is a separate,
  simpler implementation. It has no elections, no votes, and no terms: every
  journal record is written at term 1, and "the leader" is whichever configured
  node has the lowest id and answers a TCP probe. When `scripts/demo.sh` kills
  the leader, the next node takes over because it is now the lowest reachable
  id, not because it won an election. Raft — Pre-Vote, CheckQuorum, joint
  consensus, ReadIndex, snapshot install — runs in `cc-cluster`/`cc-raft` under
  the deterministic simulator and in the WebAssembly theater, which is where the
  safety claims on this page come from. The real host exists to catch
  integration mistakes at the host boundary and to give the RESP surface
  somewhere to live.
- We have one Raft group. Write throughput is ultimately bounded by the
  leader's durable-log path; sharding is a future expansion track.
- There are no follower reads. Leader reads use the quorum-confirmed ReadIndex
  fixture, but a follower redirects rather than serving a local read. See
  [consistency](consistency.md).
- Group commit creates a latency floor at low concurrency. The page-cache and
  `fsync` model is explained in [writeup 02](writeups/02-fsync-page-cache.md).
- The LSM store has read amplification as table depth grows. Compaction is
  deterministic and chunked, but restart intervals, per-block CRCs, bloom
  filters, manifest flip thresholds, and checkpoint pin/release remain
  explicitly deferred by [ADR-0005](adr/0005-store-format-audit.md).
- Exactly-once client results are bounded by the session idle TTL. A client
  that disappears longer than the TTL must reconcile its application state.
- TTLs use leader-stamped logical time. They are not wall-clock leases and
  their granularity follows the log's application cadence.
- There is no authentication, authorization, encryption, TLS, ACL, or secure
  multi-tenant boundary. The real host is for local experiments.
- The theater simulator is not kernel truth. Its disk and network behavior is
  a model; the real-host restart and fault harness exists to catch integration
  mistakes, not to replace the model or a production fault lab. The browser
  bridge is the same deterministic fixture compiled to WASM, not a second
  kernel implementation.
- Each simulated run captures a bounded client history (32 operations) so that
  campaigns stay in the hundreds-of-runs-per-second range. Linearizability
  verdicts are therefore statements about short histories under many seeds, not
  about long-running sessions.
- Uniform campaign fault plans have a fixed shape per profile. The seed varies
  timing, workload, network delays, and membership target. The separate
  coverage-guided search mutates typed plan timing toward novel trace n-grams;
  it does not turn uniform campaign counts into coverage claims.
- The current `cc-bench` driver is a reproducible local-state harness. It does
  not publish production performance claims, and all-local loopback replication
  would make network latency unrealistically low.
- The wasm/theater surface is a bounded educational bridge. Its state is
  JSON-oriented, and the native/WASM equivalence gate compares the exported
  state for the checked fixture inputs; it is not a general browser-hosting
  or performance guarantee.
- The bounded model checker exhausts only the explicit node/log/term/message,
  transition-depth, and state-count bounds printed in its report. A completed
  tiny model is evidence inside that state space, not a proof of unbounded
  Raft.
- Real-host backups are offline `CCBK` archives of the identity, config, and
  durable command journal. They are not yet SSTable checkpoints; the command
  rejects a journal that changes during capture and restore never merges data.

- The simulator's snapshot install is a modelled state transfer, not a chunked
  one. A wiped node is caught up by installing the leader's `create_snapshot`
  output directly; `cc-raft` can frame `SnapshotChunk` messages, but routing
  them through the network model would move raft's indices without the state
  machine bytes, so the direct transfer is the honest level of detail. The real
  host's TCP snapshot catch-up is a separate, genuinely chunked path.
- Permanent non-goals, recorded in [ADR-0008](adr/0008-scope-boundaries.md): a
  second host on an async runtime, an external Jepsen/Elle audit harness,
  keyspace notifications, and an LSM block cache. Each was considered and
  declined for a stated reason, not merely left undone.

Performance language is intentionally absent from the claims above. Any future
comparison must carry its environment, durability mode, topology, config hash,
and regeneration command.
