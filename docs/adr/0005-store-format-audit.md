# ADR-0005: Store format audit boundary for v1

## Status

Accepted for the fixture release.

## Decision

The v1 store fixture documents and tests the byte layouts that actually exist:
versioned SSTables with a table-level CRC, versioned META with a CRC, a
deterministic seven-probe bloom filter, in-memory manifest edits, and
checkpoint pin/release semantics. The golden vectors live in
`cc-store::tests::golden_byte_layout_vectors` and `docs/formats.md`.

The following production-scale deltas are explicitly outside this fixture
release and must not be described as implemented: restart-interval block
indexes, per-block CRCs, thresholded on-disk manifest rewrites with an atomic
META flip, and chunked compaction driven by simulator ticks. The current
single-table flush and whole-job compaction remain deterministic and are
covered by focused tests.

## Consequences

Readers can verify every persisted byte emitted by the fixture and can reject
corruption before values are exposed. A future implementation of any deferred
delta must add a new format version or an explicit compatible extension,
refresh the golden vectors, and add crash-ordering tests before the claim can
be promoted.
