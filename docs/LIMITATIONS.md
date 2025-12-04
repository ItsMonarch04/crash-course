# Limitations

This page is part of the product, not an apology. The project is an
educational, deterministic database lab and its guarantees are deliberately
narrow.

- `ccdb`, the simulator, and WASM all drive the same `cc_cluster::Node` through
  `cc_host::Driver`. Storage v2 uses store-WAL authority, file-backed SST
  reads, manifest publication, streamed checkpoints, installed-snapshot marks,
  and durable prefix reclamation. Snapshot transfer deliberately resumes from
  byte zero after a lost acknowledgement; bounded duplicate chunks are safe,
  but mid-file resume is not implemented.
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
- The LSM store has read amplification as table depth grows. Deterministic
  levelled compaction, per-block/table integrity, bloom/index reads, manifest
  generations, and checkpoint pins are implemented; there is intentionally no
  block cache, as recorded in [ADR-0008](adr/0008-scope-boundaries.md).
- Exactly-once client results are bounded by the session idle TTL. A client
  that disappears longer than the TTL must reconcile its application state.
- TTLs use leader-stamped logical time. They are not wall-clock leases and
  their granularity follows the log's application cadence. A checker read whose
  invocation/completion interval truly straddles the deadline may legally
  observe either side of expiry.
- There is no authentication, authorization, encryption, TLS, ACL, or secure
  multi-tenant boundary. The real host is for local experiments.
- The theater simulator is not kernel truth. Its disk and network behavior is
  a model; the real-host restart and fault harness exists to catch integration
  mistakes, not to replace the model or a production fault lab. The browser
  bridge is the same deterministic fixture compiled to WASM, not a second
  kernel implementation.
- Theater checkpoints and scrub replay cover only the declared finite event
  horizon and byte caps; they are not an unbounded execution archive.
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
- A minimized counterexample is a budgeted, one-deletion-minimal witness
  candidate. The complete history remains authoritative when the minimization
  budget is exhausted or several independent failures exist.
- Real-host `CCBK` v2 backups contain only the exact CCSN file named by a
  durable snapshot mark. Restore validates it and writes a fresh one-node
  CCID/Restore-origin WAL/checkpoint; it never clones source identity. This is
  fresh-cluster recovery, not an in-place old-binary downgrade. Legacy v1 is
  accepted only through the explicit legacy-node importer and never counts as
  cluster-complete provenance. Offline capture can be behind the current
  leader; online linearizable backup is not claimed.
- Storage and semantic reader floors are monotonic. Crossing a floor is an
  intentionally irreversible in-place compatibility fence; recovery into a
  fresh compatible cluster remains the escape hatch.
- User-space queues, histories, snapshots, codecs, and logical state expose
  exact count/byte caps. Kernel socket memory is not included in that
  accounting; it is bounded indirectly by connection caps and operating-system
  limits.
- Calibration profiles describe one measured environment and publish their
  residuals. They do not retune universal defaults or predict production
  latency.
- Permanent non-goals, recorded in [ADR-0008](adr/0008-scope-boundaries.md): a
  second host on an async runtime, an external Jepsen/Elle audit harness,
  keyspace notifications, and an LSM block cache. Each was considered and
  declined for a stated reason, not merely left undone.

Performance language is intentionally absent from the claims above. Any future
comparison must carry its environment, durability mode, topology, config hash,
and regeneration command.
