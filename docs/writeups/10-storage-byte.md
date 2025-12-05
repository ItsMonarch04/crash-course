# 10 — The storage engine that finally wrote a byte

The first store could encode SST and metadata fixtures, but its authoritative
read path remained an in-memory Rust value. Storage v2 closes that gap with a
specific chain: a committed state-machine transition prepares one store-WAL
batch, the host writes and fsyncs it, manifest generations name checksum-
verified SST blocks, and reads cross `BlockSource` before a reply is released.

The receipts are deliberately about boundaries, not type names:

```sh
cargo test -p cc-store trap_file_backed_v2_boot_retains_only_metadata_and_reads_blocks
cargo test -p cc-store trap_store_wal_replays_only_after_manifest_watermark
cargo test -p cc-host trap_file_backed_scans_and_compaction_respect_open_file_cap
```

The first test reboots from persisted v2 metadata and serves through block
reads rather than retained entries. The second proves the store WAL replays
only beyond the manifest watermark. The host receipt keeps scans and
compaction inside the configured file-descriptor cap. Together they show which
bytes are authority, when they become authority, and which resource owns the
read.

This does not make every byte irreplaceable. SST and compaction output remain
derived data while the durable log/checkpoint and store watermark define what
may be reconstructed. A checksum failure is reported and fails closed; it is
never converted into a silent empty value.
