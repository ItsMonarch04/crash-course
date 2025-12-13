# Versioned formats

Every persisted and wire format begins with a magic value and version, and all
length fields are checked before allocation. The current foundational formats
are:

| Format | Magic | Version | Owner |
|---|---:|---:|---|
| Trace binary | `CCTR` | 1 | `cc-core::Trace` |
| WAL segment | `CCWL` | 1 | `cc-wal` |
| SSTable | `CCST` | 1 | `cc-store` |
| Block-structured SSTable | `CCST` | 2 | `cc-store::SstV2Table` |
| Peer hello | `CCHL` | 1 | `cc-env` |
| Peer stream frame | `CCPF` | 1 | `cc-env` |
| Replicated KV command | `CCKV` | 1 | `cc-kv` |
| Transactional KV batch command | `CCKV` | 3 | `cc-kv` |
| Replicated KV reply | `CCKR` | 1 | `cc-kv` |
| Transactional KV batch reply | `CCKR` | 3 | `cc-kv` |
| Cluster policy | `CCPL` | 1 | `cc-core` |
| Versioned membership | `CCMS` | 2 (v1 read) | `cc-core` |
| Cluster identity | `CCID` | 1 | `cc-node` |
| Application envelope | `CCAP` | 1 | `cc-cluster` |
| Configuration envelope | `CCCF` | 1 | `cc-core` |
| Admin reply | `CCAR` | 1 | `cc-core` |
| Raft peer message | `CCRP` | 1 | `cc-raft::codec` |
| Host input value | `CCEI` | 1 | `cc-env` |
| Host effect value | `CCEO` | 1 | `cc-env` |
| Recorded Driver boot image | `CCBI` | 4 (v2/v3 read) | `cc-host::journal` |
| Paired input/effect journal | `CCIJ` | 1 | `cc-host::journal` |
| Raft durability record | `CCLR` | 1 | `cc-log` |
| Store apply WAL | `CCSW` | 1 | `cc-store` |
| Logical checkpoint | `CCSN` | 1 | `cc-cluster` |
| Store manifest | `CCMF` | 1 | `cc-store` |
| History receipt | `CCHY` | 2 | `cc-checker` |
| Offline logical backup archive | `CCBK` | 2 (v1 reject) | `cc-cluster::backup` |
| Store metadata | `CCMT` | 2 (v1 read) | `cc-store` |

The byte layouts are intentionally implemented by hand. A layout change needs
a new decision record, a version bump, a compatibility note, and round-trip,
malformed-input, and corruption tests.

`CCKV` command tags 1–11 are the original command family. ADR-0006 adds the
backward-compatible tags `12=Append`, `13=GetSet`, `14=GetDel`,
`15=ExpireAt`, and `16=Ttl`; `17=ConditionalSet` encodes NX/XX as one
replicated transition. Existing tag bytes and fields are unchanged.

`CCKV` v3 is intentionally narrow: it emits only `tag=18 Batch`, followed by
`count:u32` and exactly that many canonical v1 CCKV child records encoded as
`bytes32`. Count is nonzero, policy-bounded, and nested batches are rejected.
`CCKR` v3 emits only `tag=7 BatchReply`. Its payload begins with
`success:u8`. Success is `count:u32` followed by canonical v1 CCKR children as
`bytes32`. Failure is `has_failing_index:u8 | failing_index:u32 |
error_cckr:bytes32`; the error child is one canonical v1 CCKR error envelope.
An indexed error names the first failed subcommand, while a whole-batch limit
error uses `has=0,index=0`. A batch error publishes none of its child state
transitions. Both v3 decoders reject non-canonical flags, v1-only tags, trailing bytes,
nested batch values, invalid checksums, and oversize lengths; their v1 readers
remain unchanged for compatibility fixtures.

`CCID` is `magic:u32`, `format_version:u16`, `cluster_id:bytes16`,
`node_id:u64`, `lifecycle:u8`, `cluster_policy_hash:u64`,
`min_storage_reader:u16`, `min_semantic_reader:u16`, `migration_epoch:u64`,
and `crc32c:u32`. It is exactly 55 bytes, uses little-endian integers, and
checksums the whole record with its final checksum field zeroed. Lifecycle
values are `1=Active`, `2=Joining`, and `3=Removed`; a removed directory is
terminal. The cluster ID is a nonzero 16-byte identifier supplied as exactly
32 lowercase hexadecimal characters in configuration.

`CCMS` v2 retains the v1 membership/address layout and appends
`active_features:u64` before its final CRC-32C. The only currently defined
bit is `ATOMIC_BATCH=1<<1`; unknown bits fail closed. The decoder retains the
v1 reader and maps its absent field to zero, while new writers emit v2.

`CCBK` v2 is `magic:u32="CCBK" | version:u16 | source_cluster_id:bytes16 |
source_index:u64 | source_term:u64 | source_last_leader_time:u64 |
source_cluster_policy_hash:u64 | source_min_semantic:u16 |
source_active_features:u64 | checkpoint_len:u64 | checkpoint_crc32c:u32 |
header_crc32c:u32 | CCSN[checkpoint_len] | bundle_crc32c:u32 |
magic:u32="CBKE"`. The header and bundle checksums zero only their own
checksum field. Capture requires the exact CCSN named by a durable CCLR
snapshot mark; restore validates the inner checkpoint then creates a new
one-node CCID, Restore-origin Genesis, synthetic `(index=1, term=1)` snapshot,
and fresh WAL. Source node/config identity is never copied. The legacy v1
node-clone envelope is rejected by default. The explicit
`--accept-legacy-node-backup` importer validates its checkpoint, discards its
node and configuration identity, and restores only into a caller-supplied
fresh cluster identity.

`CCMF` v1 is an append-only manifest: `magic:u32="CCMF" | version:u16 |
generation:u64 | header_crc32c:u32`, then independently checksummed bounded
Snapshot/EditBatch records. `CCMT` v2 is the atomic pointer
`magic:u32="CCMT" | version:u16 | generation:u64 |
manifest_header_crc32c:u32 | meta_crc32c:u32`. The codecs validate table
ranges, footer checksums, monotonic file/watermark edits, and a torn final
record prefix. The same codecs drive Storage v2 publication in `ccdb` and the
simulator: atomic META install, file-backed SST reads, and store-WAL
authority.

`CCSN` v1 is a logical checkpoint: `magic:u32="CCSN" | version:u16 |
cluster_id:bytes16 | cluster_policy_hash:u64 | index:u64 | term:u64 |
last_leader_time:u64 | store_sequence:u64 | record_count:u64 |
header_crc32c:u32`. It then contains bounded records
`body_len:u32 | body_crc32c:u32 | tag:u8 | body`, followed by
`total_len:u64 | records_crc32c:u32 | file_crc32c:u32 | magic:u32="CSNE"`.
The header checksum zeros its final field; the per-record checksum covers the
tag and body; the records checksum covers the exact record sequence; and the
file checksum zeros only its own footer field. The canonical records are one
membership record, one exact policy record, byte-sorted live key/value records
with MVCC sequence and optional deadline, then `(namespace, client)`-ordered
live session and tombstone records. An optional tag-6 record preserves a
committed leadership-transfer intent as `intent_index`, target, deadline,
finishing flag, and its exact AdminRequest identity; absent identity fields
must be canonical zeroes. Installing that checkpoint restores the transfer
so TimeoutNow and Finish still apply after the Begin entry has been compacted
out of the log. It rejects duplicate, out-of-order,
expired, noncanonical, wrong-cluster, and checksum-invalid state before the
core installs it.

The shared Driver sends CCSN in non-empty CCRP `SnapshotChunk` records no
larger than the Raft chunk limit. It keeps one chunk outstanding per peer and
retransmits that exact offset until a matching `SnapshotAck`; `Gap` and
`RestartFromZero` reset the sender only to the acknowledged offset. Receiver
chunks are written and fsynced to staging before incremental decode, then the
completed file is renamed and directory-synced. A local `CCLR` snapshot mark
or an installed-snapshot mark is written and fsynced before the checkpoint is
recovery authority or the final acknowledgement is sent. Boot validates the
marked generation, index, term, and CCSN file checksum together; orphaned
unmarked files are not authority.
New checkpoint publication is deferred while joint consensus is active and
while a reachable removed peer has not acknowledged the terminal LeaveJoint.
The host may still send an older marked checkpoint followed by the retained
suffix. A checkpoint that already covers a committed BeginLeaderTransfer
carries that workflow in its tag-6 record, and install restores it instead of
relying on the compacted-away log entry.

`CCLR` v1 remains a CRC-protected framed Raft durability record. Tag 5 is a
locally-created snapshot mark and requires the marked position to exist in the
retained log. Tag 6 is an installed-snapshot mark for a follower whose
covered prefix may already be absent; it is valid only when the host verifies
the named checkpoint file and checksum during recovery. Both tags carry
`index:u64 | term:u64 | generation:u64 | crc32c:u32`.

`CCHL` is the pre-frame peer negotiation record. It carries raw 16-byte
cluster identity, node id, exact bounded `CCPL` bytes, semantic-version range,
supported and required feature bits, and maximum peer-frame size. Its final
CRC-32C is computed over the complete record with its checksum field zeroed.
The decoder can consume a fragmented hello or return its consumed length when
the first `CCPF` frame is coalesced in the same TCP read. Peers reject a
cluster/policy mismatch, invalid range, unknown required bit, or missing
required capability before accepting a frame.

`CCRP` is a canonical Raft-message payload and is transported inside `CCPF`.
Its format version is `1`. Semantic v2 is frozen for ordinary Raft traffic
(`read_round`), while semantic v3 adds only tags `10=FollowerReadRequest`
(`request_id:u64 | command_hash:u64`) and `11=FollowerReadGrant`
(`request_id:u64 | command_hash:u64 | read_index:u64 | read_time:u64`). Both
ids are nonzero; the hash is FNV-1a-64 of canonical CCKV bytes and is a
correlation value, not authentication. Tags 10/11 reject v2 frames and a
connection may send them only when CCHL selected both semantic v3 and feature
bit `FOLLOWER_READ`. CCHL negotiation, CCPF transport framing, and CCRP
encoding are separate version namespaces. The simulator now performs
`Message → CCRP → CCPF → Network → CCPF → CCRP → Message`; malformed frames
do not reach Raft.

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

The current `boot_image` is `CCBI` v4; the reader retains v2 and v3 support.
It contains the cluster id; complete effective `NodeConfig`/host limits;
committed bootstrap membership; boot epoch; bounded build label; verified
Raft and store-WAL prefixes; and the exact logical CCSN checkpoint named by a
snapshot mark, when one exists. Replay checks the copied identity, policy,
membership, snapshot metadata, and durability prefixes before rebuilding the
shared Driver. It then supplies recorded block observations and compares each
transition's complete effect vector. This lets a recording begin after prior
writes or snapshot compaction without silently reconstructing an empty store.
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
- SSTable v2: independently checksummed prefix-compressed data blocks,
  an index block, a seven-probe FNV-1a bloom block, and a fixed 42-byte footer.
  Internal keys sort by user key ascending, sequence descending, then kind;
  each sixteenth data entry is a restart. Its footer is
  `index_offset:u64 | index_length:u32 | bloom_offset:u64 | bloom_length:u32 |
  entry_count:u64 | format_version:u16=2 | footer_crc32c:u32 | "CCST"`.
  V2 is the current durable node-store table format. Its separate reader and
  writer retain every C0 v1 byte and reader unchanged.
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

The frozen v1 fixture boundary is recorded in ADR-0005. ADR-0017 records the
implemented Storage v2 boundary described above.
