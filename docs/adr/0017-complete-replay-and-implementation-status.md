# ADR 0017: Complete replay boot images and implementation status

- Status: Accepted
- Date: 2025-12-13

## Context

The shared Driver now starts from three kinds of durable authority: the framed
Raft WAL, the store-apply WAL, and an optional logical checkpoint named by the
Raft snapshot mark. CCBI v3 recorded only the Raft WAL. A recording that began
after earlier writes could therefore replay from a different logical state,
and a recording begun after snapshot compaction lacked the state covered by
the discarded prefix.

Several earlier ADRs also retained implementation-pending status after their
mechanical receipts had landed. That made the decision index disagree with the
host, simulator, and public documentation.

## Decision

CCBI v4 embeds the verified Raft-WAL and store-WAL prefixes plus the exact CCSN
checkpoint named by a durable snapshot mark, when present. Replay validates
the cluster, policy, membership, WAL prefixes, snapshot position, and snapshot
checksum before it reconstructs the Driver. It restores durable store applies
and registers the published checkpoint before consuming the first recorded
input. CCBI v2 and v3 remain readable; new recordings emit v4.

The following earlier decisions are implemented in the current architecture:

- replicated policy and membership quorum projection (ADR-0009/0010);
- atomic committed apply through the store-WAL continuation (ADR-0011/0014);
- binary histories and real-host replay receipts (ADR-0012);
- one `cc-cluster::Node` driven through `cc-host::Driver` by the simulator,
  WebAssembly bridge, and socket/filesystem host (ADR-0013);
- explicit aggregate resource bounds (ADR-0015); and
- replicated learner promotion, joint-consensus administration, voter
  removal, and leadership transfer (ADR-0016).

Storage v2, logical CCSN checkpoints, and CCBK v2 fresh-cluster restore
supersede the v1 implementation boundaries in ADR-0005 and ADR-0007. The
follower-read deferral in ADR-0008 is also superseded: negotiated semantic-v3
`FOLLOWER_READ` requests now use a leader ReadIndex grant and the follower's
durable KV apply watermark.

## Consequences

A CCIJ recording can begin after durable writes or snapshot compaction without
silently replaying from an empty logical store. The boot image remains bounded;
recording refuses an image that exceeds the CCIJ boot-image limit rather than
producing an incomplete receipt. Adding the store and checkpoint fields is a
diagnostic format change, so the golden compatibility manifest retains the v3
fixture and adds a v4 fixture.

The older ADRs remain useful history, but their status lines point here where
their former pending or deferred statements no longer describe the code.
