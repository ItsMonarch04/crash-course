# Versioned formats

Every persisted and wire format begins with a magic value and version, and all
length fields are checked before allocation. The current foundational formats
are:

| Format | Magic | Version | Owner |
|---|---:|---:|---|
| Trace binary | `CCTR` | 1 | `cc-core::Trace` |
| WAL segment | `CCWL` | 1 | `cc-wal` |
| SSTable | `CCST` | 1 | `cc-store` |
| Raft frame | `CCRP` | 1 | `cc-raft` |
| Peer stream frame | `CCPF` | 1 | `cc-env` |
| Real-host command journal payload | `CCKV` | 1 | `cc-node` |
| KV snapshot | `CCKV` | 1 | `cc-kv` |

The byte layouts are intentionally implemented by hand. A layout change needs
a new decision record, a version bump, a compatibility note, and round-trip,
malformed-input, and corruption tests.

The museum manifest is JSON rather than a consensus format. It still carries a
schema version and a pinned build string; a missing or malformed manifest
loads as an empty wing instead of silently inventing an exhibit.
