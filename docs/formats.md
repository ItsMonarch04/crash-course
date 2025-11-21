# Versioned formats

Every persisted and wire format begins with a magic value and version, and all
length fields are checked before allocation. The current foundational formats
are:

| Format | Magic | Version | Owner |
|---|---:|---:|---|
| Trace binary | `CCTR` | 1 | `cc-core::Trace` |
| WAL segment | `CCWL` | 1 | `cc-wal` |
| SSTable | `CCST` | 1 | `cc-store` |
| Peer hello | `CCHL` | 1 | `cc-env` |
| Peer stream frame | `CCPF` | 1 | `cc-env` |
| Replicated KV command | `CCKV` | 1 | `cc-kv` |
| Replicated KV reply | `CCKR` | 1 | `cc-kv` |
| Cluster policy | `CCPL` | 1 | `cc-core` |
| Cluster identity | `CCID` | 1 | `cc-node` |
| Configuration envelope | `CCCF` | 1 | `cc-core` |
| Admin reply | `CCAR` | 1 | `cc-core` |
| Raft peer message | `CCRP` | 1 | `cc-raft::codec` |
| Host input value | `CCEI` | 1 | `cc-env` |
| Host effect value | `CCEO` | 1 | `cc-env` |
| Paired input/effect journal | `CCIJ` | 1 | `cc-host::journal` |
| Raft durability record | `CCLR` | 1 | `cc-log` |
| History receipt | `CCHY` | 2 | `cc-checker` |
| Offline backup archive | `CCBK` | 1 | `cc-node` |

The byte layouts are intentionally implemented by hand. A layout change needs
a new decision record, a version bump, a compatibility note, and round-trip,
malformed-input, and corruption tests.

`CCKV` command tags 1–11 are the original command family. ADR-0006 adds the
backward-compatible tags `12=Append`, `13=GetSet`, `14=GetDel`,
`15=ExpireAt`, and `16=Ttl`; `17=ConditionalSet` encodes NX/XX as one
replicated transition. Existing tag bytes and fields are unchanged.

`CCID` is `magic:u32`, `format_version:u16`, `cluster_id:bytes16`,
`node_id:u64`, `lifecycle:u8`, `cluster_policy_hash:u64`,
`min_storage_reader:u16`, `min_semantic_reader:u16`, `migration_epoch:u64`,
and `crc32c:u32`. It is exactly 55 bytes, uses little-endian integers, and
checksums the whole record with its final checksum field zeroed. Lifecycle
values are `1=Active`, `2=Joining`, and `3=Removed`; a removed directory is
terminal. The cluster ID is a nonzero 16-byte identifier supplied as exactly
32 lowercase hexadecimal characters in configuration.

`CCBK` is `magic:u32`, `version:u16`, `file_count:u32`, then sorted entries of
`path_len:u16`, UTF-8 path bytes, `data_len:u64`, `crc32c:u32`, and file bytes.
Version 1 permits exactly `identity.ccid`, `ccdb.toml`, and `raft/wal.0`.
The archive validates an untorn framed WAL before capture and before restore;
it is an offline operator copy, not the authority for shared-driver recovery
or a substitute for a future storage/snapshot checkpoint.

`CCHL` is the pre-frame peer negotiation record. It carries raw 16-byte
cluster identity, node id, exact bounded `CCPL` bytes, semantic-version range,
supported and required feature bits, and maximum peer-frame size. Its final
CRC-32C is computed over the complete record with its checksum field zeroed.
The decoder can consume a fragmented hello or return its consumed length when
the first `CCPF` frame is coalesced in the same TCP read. Peers reject a
cluster/policy mismatch, invalid range, unknown required bit, or missing
required capability before accepting a frame.

`CCRP` is a canonical Raft-message payload and is transported inside `CCPF`.
Its format version is `1`; its embedded semantic protocol version is currently
`2` (the version that introduced `read_round`). `CCHL` negotiation, `CCPF`
transport framing, and `CCRP` encoding are separate version namespaces. The
simulator now performs `Message → CCRP → CCPF → Network → CCPF → CCRP →
Message`; malformed frames do not reach Raft.

Peer trace events use a fixed nine-byte diagnostic fingerprint rather than a
Rust display/debug string: `fingerprint_version:u16=1 |
semantic_version:u16 | ccrp_tag:u8 | final_ccpf_crc32c:u32`. A malformed outer
frame uses tag zero and the expected semantic-version hint. This is trace
evidence only; it is neither a peer decoder nor a routing format.

`CCPL` is a fixed little-endian record plus CRC-32C. It carries every limit
that can alter deterministic command/apply behavior. Its FNV-1a hash is an
early storage fence, not authentication; authoritative peers compare the full
canonical bytes. `CCCF` and `CCAR` similarly carry a trailing CRC and reject
non-canonical absent admin-session fields.

`CCHY` v2 is a bounded binary-safe history container. It retains open
operations, operation identities, binary keys/values, an explicit initial
model, and outcomes. The CLI still reads legacy `CC-HISTORY v1` text files and
decodes their hex fields as bytes; new exports emit v2.

`CCIJ` v1 is a host replay receipt. Its header is `magic:u32`, `version:u16`,
and `boot_image:bytes32`. It then has zero or more
`record_len:u32 | record_crc32c:u32 | record` values. A record contains
`ordinal:u64`, `now_ns:u64`, one complete `CCEI` input, a bounded request-order
vector of block observations, and a bounded vector of complete `CCEO` effects.
A block observation is `file_id:bytes32 | offset:u64 | len:u32 |
result_tag:u8 | bytes:bytes32 | service_ns:u64`. Successful exact reads have
`result_tag=Ok` and exactly `len` bytes; error tags have empty bytes but retain
their measured service time. Replay
rejects a different request, an exhausted observation vector, or unused
observations instead of substituting a local read. Record CRC covers the record
body. Recovery retains only complete CRC-checked records, discarding a torn
final prefix and failing closed on a complete corrupt record, nonmonotonic
ordinal, or backward time.

An optional final control frame uses the high bit of its length word and has a
CRC-protected `CCIF` v1 body: `termination:u8 | last_ordinal:u64`. Termination
tags are `Complete`, `Capped`, `HostError`, and `FatalIo`. No footer means an
interrupted prefix, never an implicit complete run. In the current host,
`--record-max-bytes` writes the `Capped` footer before disabling recording.
By default the node then continues unrecorded; `--record-required` instead
exits nonzero on a cap or recorder write failure. The real host also makes a
best-effort synced `FatalIo` footer before an injected or real WAL write/fsync
failure aborts, and makes the same best effort with `HostError` for a surfaced
Driver error. A recorder write failure never fabricates one of those footers.
An explicit bounded `ccdb run --run-for-ms N` closes admission, waits for any
already-admitted Driver transition to record, then syncs `Complete`; external
interruption remains an interrupted prefix.

For `CCEI` client inputs, the volatile reply `(client, req)` is followed by a
canonical optional durable retry identity. An absent identity uses a zero flag
and zero fields; a present identity carries nonzero caller-owned client and
sequence values. It is how `CC.REQUEST` reaches the replicated CCAP session
envelope without turning a connection counter into durable state.

The current `boot_image` is `CCBI` v2. It contains the cluster id; complete
effective `NodeConfig`/host limits; committed membership; boot epoch; bounded
build label; and the verified Raft WAL. Replay checks the copied identity,
policy, and membership against WAL Genesis before rebuilding the shared Driver,
then supplies the recorded block observations and compares each transition's
complete effect vector. The real host's current WAL-only store path does not
yet issue SST reads, but the receipt field is present and exact rather than
silently omitted. Snapshot images remain future work.
The replay tool accepts a different captured build label but emits a warning;
it never consults a repository checkout or ambient node configuration.

`CCLR` v1 is the semantic Raft durability record owned by `cc-log`. The real
adapter stores a sequence of `record_len:u32 | CCLR-record` frames in
`raft/wal.0`; the length is little-endian and bounds the record before decode.
Only an incomplete final length/body is a torn tail. A complete malformed or
corrupt record fails recovery. Genesis is the first and only record, followed
by hard-state, append, truncation, and snapshot-mark records.

The simulated WAL used by `cc-swarm`'s `SimCluster` is a `SimDisk` fixture,
not a `cc-wal` segment, but its contents are the same framed `CCLR` durable
record stream as the shared Driver: a sealed Genesis followed by append-only
hard-state, append, and truncate records. `cc-log` recovers its durable prefix;
the simulator does not retain a private hard-state/entry decoder.

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
