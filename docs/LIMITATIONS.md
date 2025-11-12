# Limitations

This page is part of the product, not an apology. The project is an
educational, deterministic database lab and its guarantees are deliberately
narrow.

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
- Generated fault plans have a fixed shape per profile. The seed varies fault
  timing, the workload, network delays, and which voter a membership change
  removes, but not the set of faults itself. Broader fault search is an
  enhancement track, not a property of the current campaigns.
- The current `cc-bench` driver is a reproducible local-state harness. It does
  not publish production performance claims, and all-local loopback replication
  would make network latency unrealistically low.
- The wasm/theater surface is a bounded educational bridge. Its state is
  JSON-oriented, and the native/WASM equivalence gate compares the exported
  state for the checked fixture inputs; it is not a general browser-hosting
  or performance guarantee.

Performance language is intentionally absent from the claims above. Any future
comparison must carry its environment, durability mode, topology, config hash,
and regeneration command.
