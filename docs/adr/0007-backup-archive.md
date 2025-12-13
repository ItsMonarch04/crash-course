# ADR-0007: Offline checkpoint archive

- Status: Superseded by [ADR-0017](0017-complete-replay-and-implementation-status.md)
- Date: 2025-11-13

## Decision

`ccdb admin backup` emits a bounded `CCBK` version-1 archive containing the
node identity, configuration, and CRC-validated framed Raft WAL. The command
rejects a torn WAL during capture, so operators must stop writes and retry
rather than accept a fuzzy backup.

Restore accepts only the three known relative paths, verifies every checksum
before writing, stages files under a sibling directory, fsyncs files and the
directory, then atomically renames into a target that must not already exist.
It rewrites only `data_dir` in the restored configuration. Repair remains
report-only; restore never merges with existing data.

## Consequences

This archive followed the real host's v1 `cc-log` durable truth. ADR-0017
records the CCSN-based CCBK v2 format that superseded it.
