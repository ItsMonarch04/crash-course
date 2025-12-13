# ADR-0005: Store format audit boundary for v1

- Status: Superseded by [ADR-0017](0017-complete-replay-and-implementation-status.md)
- Date: 2025-11-10

## Decision

The v1 store fixture documents and tests the byte layouts that actually exist:
versioned SSTables with a table-level CRC, versioned META with a CRC, a
deterministic seven-probe bloom filter, in-memory manifest edits, and
checkpoint pin/release semantics. The golden vectors live in
`cc-store::tests::golden_byte_layout_vectors` and `docs/formats.md`.

The following production-scale deltas were explicitly outside that fixture
release: restart-interval block
indexes, per-block CRCs, thresholded on-disk manifest rewrites with an atomic
META flip, and chunked compaction driven by simulator ticks. The current
single-table flush and whole-job compaction at that boundary were deterministic
and covered by focused tests.

## Consequences

Readers can verify every persisted byte emitted by the fixture and can reject
corruption before values are exposed. ADR-0017 records the versioned successor;
any further layout change still requires an explicit compatible extension,
refreshed golden vectors, and crash-ordering tests.
