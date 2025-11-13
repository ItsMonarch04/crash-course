# ADR-0007: Offline checkpoint archive

- Status: Accepted
- Date: 2025-11-13

## Decision

`ccdb admin backup` emits a bounded `CCBK` version-1 archive containing the
node identity, configuration, and CRC-validated command journal. The command
replays the journal and rejects a length change during capture, so operators
must stop writes and retry rather than accept a fuzzy backup.

Restore accepts only the three known relative paths, verifies every checksum
before writing, stages files under a sibling directory, fsyncs files and the
directory, then atomically renames into a target that must not already exist.
It rewrites only `data_dir` in the restored configuration. Repair remains
report-only; restore never merges with existing data.

## Consequences

This archive follows the real host's current durable-journal truth. It is not
an SSTable checkpoint and must evolve when the real host adopts the simulator
store's file set.
