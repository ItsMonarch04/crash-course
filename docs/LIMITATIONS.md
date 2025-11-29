# Limitations

This page is part of the product, not an apology. The project is an
educational, deterministic database lab and its guarantees are deliberately
narrow.

- **The real host is now a strict shared-driver adapter, but it is not a
  complete Storage v2 host.** `ccdb` runs `cc_host::Driver` over
  `cc_cluster::Node`, including election traffic, Raft terms, CCHL/CCPF/CCRP
  peer bytes, and a write-plus-fsync durability continuation. A fatal WAL I/O
  error terminates the process. Its v2 SST codec plus CCMF/CCMT codec and
  host-neutral publication plan are separately verified, but ccdb does not
  execute that plan yet; there is still no store-WAL authority or prefix
  reclamation. The shared Driver creates a bounded, record-streamed
  CCSN checkpoint, fsyncs and atomically publishes it, then fsyncs the
  matching Raft snapshot mark before the file becomes transfer or recovery
  authority. It stages/fsyncs each received chunk, decodes it from the staged
  file, publishes the file, fsyncs an installed-snapshot mark, and only then
  installs and acknowledges it. Boot accepts only the exact marked file and
  checksum; an unmarked checkpoint is not recovery authority. Checkpoint
  trigger, publication, and transfer are implemented, but the missing
  manifest/store checkpoint edit and safe durable-prefix reclamation mean this
  is not yet the complete N3/N4 storage lifecycle.
- We have one Raft group. Write throughput is ultimately bounded by the
  leader's durable-log path; sharding is a future expansion track.
- Default reads remain leader reads. `READ FOLLOWER GET|TTL` is available only
  between peers that negotiate semantic v3 plus `FOLLOWER_READ`; it waits for
  a leader ReadIndex grant and local apply, and fails closed with `TRYAGAIN`
  for an unknown/v2/featureless leader or a changed term/configuration.
  `READ STALE GET` is intentionally separate and tagged with its local applied
  index/term and read time; it has no bounded-staleness or linearizability
  claim. See [consistency](consistency.md).
- `MULTI`/`EXEC` has an all-or-nothing CCKV v3 implementation. Atomic-batch
  admission is fenced by a replicated feature bit that changes only on commit
  and currently requires every observed member capability. The full
  mixed-build rolling-upgrade fence—semantic-v3 log-entry metadata and a
  per-entry CCID reader-floor barrier—is still incomplete. Do not treat the
  feature as rolling-upgrade-safe yet.
- `CC.ADMIN ADDLEARNER <id>`, `PROMOTE <id>`, and `LEAVEJOINT` are live
  operator controls and append the matching replicated config transition;
  `ccdb admin add-learner|promote-learner --node-id <id>` follows the current
  leader. This prototype has no authentication or address-discovery workflow,
  and promotion may only be requested after the learner has caught up.
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
- Real-host `CCBK` v2 backups contain only the exact CCSN file named by a
  durable snapshot mark. Restore validates it and writes a fresh one-node
  CCID/Restore-origin WAL/checkpoint; it never clones source identity. The
  capture is offline and can therefore be behind the current leader; online
  linearizable backup, store-manifest validation, and the promised legacy v1
  logical importer remain incomplete.

- Wiped simulator nodes re-enter through a Join-origin durable prefix and
  ordinary Raft traffic; no leader snapshot is copied directly into a target.
  The host has streamed durable checkpoint transfer, but still lacks the
  storage-manifest and prefix-reclamation portions of N4, so a wiped-node
  claim remains narrower than full storage recovery.
- Permanent non-goals, recorded in [ADR-0008](adr/0008-scope-boundaries.md): a
  second host on an async runtime, an external Jepsen/Elle audit harness,
  keyspace notifications, and an LSM block cache. Each was considered and
  declined for a stated reason, not merely left undone.

Performance language is intentionally absent from the claims above. Any future
comparison must carry its environment, durability mode, topology, config hash,
and regeneration command.
