# Versioned formats

Every persisted and wire format begins with a magic value and version, and all
length fields are checked before allocation. The current foundational formats
are:

| Format | Magic | Version | Owner |
|---|---:|---:|---|
| Trace binary | `CCTR` | 1 | `cc-core::Trace` |
| WAL segment | `CCWL` | 1 | `cc-wal` |
| SSTable | `CCST` | 1 | `cc-store` |
| Peer stream frame | `CCPF` | 1 | `cc-env` |
| Real-host command journal payload | `CCKV` | 1 | `cc-node` |
| KV snapshot | `CCKV` | 1 | `cc-kv` |
| Offline backup archive | `CCBK` | 1 | `cc-node` |

The byte layouts are intentionally implemented by hand. A layout change needs
a new decision record, a version bump, a compatibility note, and round-trip,
malformed-input, and corruption tests.

`CCKV` command tags 1–11 are the original command family. ADR-0006 adds the
backward-compatible tags `12=Append`, `13=GetSet`, `14=GetDel`,
`15=ExpireAt`, and `16=Ttl`; existing tag bytes and fields are unchanged.

`CCBK` is `magic:u32`, `version:u16`, `file_count:u32`, then sorted entries of
`path_len:u16`, UTF-8 path bytes, `data_len:u64`, `crc32c:u32`, and file bytes.
Version 1 permits exactly `node.json`, `ccdb.toml`, and `commands.log`.

Raft messages are deliberately absent from that table. `cc-raft` is sans-IO:
its `Message` values never become bytes. Inside the simulator they are moved
between nodes as Rust values, and the real host replicates committed journal
records over `CCPF` peer frames rather than shipping raft frames. The
`proto_version` field carried on each `Message` is an in-process compatibility
check (`cc_raft::PROTOCOL_VERSION`, currently `2` since append requests and
responses gained `read_round`), not a serialized header. When a real raft frame
format is introduced it gets its own magic, version, ADR, and round-trip tests.

The simulated WAL used by `cc-swarm`'s `SimCluster` is a host-side fixture, not
a `cc-wal` segment. It is a 24-byte hard-state header (`version:u64`,
`term:u64`, `voted_for:u64`, where `0` means "no vote") followed by appended
records of `term:u64`, `index:u64`, `kind:u8`, `payload_len:u64`, `payload`.
Recovery decodes forward and stops at the first short or malformed record, so a
torn tail is simply not part of the recovered log. It is documented here
because restart genuinely rebuilds a node from these bytes.

The museum manifest is JSON rather than a consensus format. It still carries a
schema version and a pinned build string; a missing or malformed manifest
loads as an empty wing instead of silently inventing an exhibit.

## Store fixture layouts

All integer fields below are little-endian. `bytes` means a `u32` byte length
followed by exactly that many bytes. The final four bytes of both SST and META
are CRC-32C over the preceding body.

- SSTable: `CCST`, format `1`, `file_no:u64`, `entry_count:u32`, then each
  entry as `user_key:bytes`, `sequence:u64`, `kind:u8` (`1=Put`, `2=Delete`),
  and `value:bytes`, followed by the CRC.
- META: `CCMT`, format `1`, `manifest_generation:u64`, followed by the CRC.
- WAL segment: `CCWL`, format `1`, `segment_sequence:u64`; records carry a
  `length:u32`, `kind:u8`, payload, and CRC, with segment padding and seal
  records defined by `cc-wal`.
- Peer frame: `CCPF`, transport version `1`, `body_length:u32`, `crc32c:u32`,
  then `protocol_version:u16` and the bounded payload.

Golden SST and META vectors (the SST contains `a=one` at sequence 1 and a
delete tombstone for `b` at sequence 2) are:

```text
SST 4343535401000700000000000000020000000100000061010000000000000001030000006f6e65010000006202000000000000000200000000360c63d6
META 43434d54010007000000000000008b275acd
```

The audit boundary and deferred deltas are recorded in ADR-0005; this page
describes the fixture bytes rather than an aspirational LSM implementation.
