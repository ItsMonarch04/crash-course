// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The composition boundary: one Raft node, one KV state machine, value-only effects."]

pub mod backup;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use cc_core::{
    AdminReply, AdminResultTag, Bytes, ClientId, ClusterPolicy, ConfigEnvelope, ConfigOperation,
    Crc32c, Dec, Duration, Enc, HostLimits, LogIndex, MembershipState, NodeId, Seed, SessionKey,
    SessionNamespace, Term, Time, TransferResult, crc32c, crc32c_zeroed_tail,
};
use cc_kv::{
    BatchLimits, Kv, KvCommand, KvError, KvReply, KvSnapshot, LogicalKvEntry, LogicalKvSnapshot,
    decode_command, decode_reply, encode_command, encode_reply, validate_batch,
};
use cc_raft::{Entry, HardState, LeadershipTransferState, RaftEffect, RaftError, RaftNode};
use cc_store::{StoreApplyBatch, StoreConfig, StoreEntryKind, StoreMetadataEdit, StoreWatermark};

pub use cc_raft::{
    Message, MessageKind, PROTOCOL_VERSION, RaftConfig, Role, SEMANTIC_VERSION_V3,
    SNAPSHOT_CHUNK_BYTES, SnapshotRejectReason, TimerKind,
};
pub const FOLLOWER_READ_FEATURE: u64 = cc_env::FEATURE_FOLLOWER_READ;

pub const CLUSTER_VERSION: u16 = 1;
pub const APP_ENVELOPE_MAGIC: u32 = u32::from_le_bytes(*b"CCAP");
pub const APP_ENVELOPE_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    pub id: NodeId,
    /// Fixed cluster identity from CCID/genesis. Snapshot bytes carry this
    /// exact value so a valid checkpoint can never cross cluster boundaries.
    pub cluster_id: [u8; 16],
    pub seed: Seed,
    pub raft: RaftConfig,
    pub store: StoreConfig,
    pub policy: ClusterPolicy,
    pub host_limits: HostLimits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeInput {
    Tick {
        now: Time,
    },
    Message(Message),
    MessageAt {
        now: Time,
        message: Message,
    },
    Timer {
        now: Time,
        kind: cc_raft::TimerKind,
    },
    /// Host acknowledgement for the one outstanding logical durability
    /// barrier.  A real host maps this to its write/fsync continuation; the
    /// deterministic simulator supplies the same result explicitly.
    Persisted {
        success: bool,
    },
    ClientRequest {
        client: ClientId,
        sequence: u64,
        command: KvCommand,
        leader_time: Time,
    },
    /// A byte-oriented request with a volatile reply route.  Unlike the
    /// legacy typed request above, `route_client`/`route_req` are never put
    /// into CCAP; only an explicitly supplied SessionKey is replicated.
    ClientBytes {
        route_client: ClientId,
        route_req: u64,
        session: Option<(SessionKey, u64)>,
        command: Bytes,
        leader_time: Time,
    },
    Read {
        client: ClientId,
        sequence: u64,
        command: KvCommand,
        at: Time,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeEffect {
    Send(Message),
    /// A validated snapshot chunk awaiting host-owned staging. No Raft index
    /// changes until the host reports a completed logical installation.
    ReceiveSnapshotChunk(Message),
    /// A validated peer acknowledgement for a host-owned snapshot sender.
    ReceiveSnapshotAck(Message),
    PersistHard(cc_raft::HardState),
    PersistEntries(Vec<Entry>),
    TruncateSuffix(LogIndex),
    /// One complete committed state-machine transition framed for the
    /// append-only store WAL. The host must write+fsync it before delivering
    /// `Persisted`; until then the tentative KV/session state and reply remain
    /// invisible.
    PersistStore {
        bytes: Bytes,
    },
    ClientReply {
        client: ClientId,
        sequence: u64,
        reply: KvReply,
    },
    ReadReply {
        client: ClientId,
        sequence: u64,
        reply: KvReply,
    },
    AdminReply {
        client: ClientId,
        sequence: u64,
        reply: AdminReply,
    },
    ArmTimer {
        id: cc_core::TimerId,
        at: Time,
        kind: cc_raft::TimerKind,
    },
    Trace(&'static str),
}

/// The only canonical mapping from a Raft durability boundary to bytes a host
/// may write. Keeping it in the composition crate prevents adapters from
/// growing their own durable-log vocabulary.
pub fn encode_durability_effect(effect: &NodeEffect) -> Result<Option<Bytes>, NodeError> {
    let bytes = match effect {
        NodeEffect::PersistHard(hard) => {
            cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Hard(*hard))
                .map_err(|_| NodeError::Durability)?
        }
        NodeEffect::PersistEntries(entries) => {
            let mut bytes = Vec::new();
            for entry in entries {
                bytes.extend(
                    cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Append(
                        entry.clone(),
                    ))
                    .map_err(|_| NodeError::Durability)?,
                );
            }
            bytes
        }
        NodeEffect::TruncateSuffix(from) => {
            cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Truncate { from: *from })
                .map_err(|_| NodeError::Durability)?
        }
        _ => return Ok(None),
    };
    Ok(Some(bytes))
}

/// Canonically frame one outbound Raft message for the value host boundary.
pub fn encode_peer_effect(message: &Message) -> Result<cc_env::WireMsg, NodeError> {
    let payload =
        cc_raft::codec::encode(message).map_err(|_| NodeError::Environment("CCRP encode"))?;
    Ok(cc_env::WireMsg::new(message.proto_version, payload))
}

#[must_use]
pub fn encode_client_reply(reply: &KvReply) -> Bytes {
    encode_reply(reply)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    Raft(RaftError),
    Kv(KvError),
    NotLeader,
    Durability,
    UnexpectedPersistenceCompletion,
    PersistencePending,
    FeatureDisabled,
    Environment(&'static str),
    MalformedCommittedEntry(LogIndex),
}

/// Result of delivering one value-boundary input to a node.  The host owns
/// when effects become externally visible; the service time belongs to this
/// input and is never charged to the next one.
#[derive(Debug)]
pub struct NodeStep {
    pub synchronous_service: Duration,
    pub outcome: Result<Vec<NodeEffect>, NodeError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSnapshot {
    pub kv: KvSnapshot,
    pub sessions: SessionTable,
    pub membership: MembershipState,
    pub leadership_transfer: Option<LeadershipTransferState>,
    pub cluster_policy: ClusterPolicy,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}

/// Convert a fully decoded logical CCSN checkpoint into the recovery value
/// consumed by [`Node::restore`].  Keeping this conversion in the composition
/// crate prevents a host adapter from reconstructing KV/session state or
/// inventing a second snapshot representation during boot.
pub fn node_snapshot_from_ccsn(
    snapshot: CcsnSnapshot,
    store: StoreConfig,
) -> Result<NodeSnapshot, NodeError> {
    if snapshot.kv.applied_index.get() == 0
        || snapshot.kv.applied_term.get() == 0
        || snapshot.membership.validate().is_err()
    {
        return Err(NodeError::MalformedCommittedEntry(
            snapshot.kv.applied_index,
        ));
    }
    let index = snapshot.kv.applied_index;
    let term = snapshot.kv.applied_term;
    let mut kv = Kv::restore_logical(snapshot.kv, store)?;
    Ok(NodeSnapshot {
        kv: kv.snapshot()?,
        sessions: snapshot.sessions,
        membership: snapshot.membership,
        leadership_transfer: snapshot.leadership_transfer,
        cluster_policy: snapshot.cluster_policy,
        last_included_index: index,
        last_included_term: term,
    })
}

/// CCSN is the portable, logical checkpoint format.  It intentionally has no
/// SST, WAL, host-path, or Rust-serialization vocabulary.
pub const CCSN_MAGIC: u32 = u32::from_le_bytes(*b"CCSN");
pub const CCSN_END_MAGIC: u32 = u32::from_le_bytes(*b"CSNE");
pub const CCSN_VERSION: u16 = 1;
const CCSN_HEADER_LEN: usize = 74;
const CCSN_FOOTER_LEN: usize = 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CcsnSnapshot {
    pub cluster_id: [u8; 16],
    pub cluster_policy: ClusterPolicy,
    pub membership: MembershipState,
    pub kv: LogicalKvSnapshot,
    pub sessions: SessionTable,
    pub leadership_transfer: Option<LeadershipTransferState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotCodecError {
    Invalid(&'static str),
    TooLarge,
}

impl fmt::Display for SnapshotCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid CCSN: {reason}"),
            Self::TooLarge => f.write_str("CCSN exceeds configured maximum"),
        }
    }
}

impl std::error::Error for SnapshotCodecError {}

/// The largest single record buffer accepted by the streaming CCSN decoder.
/// A checkpoint may be much larger; only one independently checksummed record
/// is ever materialised by the decoder before it becomes logical state.
pub const MAX_CCSN_RECORD_BYTES: u64 = 16 * 1024 * 1024;

/// Return the canonical whole-file checksum embedded in a complete CCSN
/// footer. This is the checksum carried in CCRP snapshot chunks; calculating a
/// CRC over the final file bytes would incorrectly include the checksum field
/// itself.
pub fn ccsn_file_crc(bytes: &[u8]) -> Result<u32, SnapshotCodecError> {
    if bytes.len() < CCSN_HEADER_LEN + CCSN_FOOTER_LEN {
        return Err(SnapshotCodecError::Invalid("truncated"));
    }
    let footer = bytes.len() - CCSN_FOOTER_LEN;
    if read_u64(&bytes[footer..footer + 8])?
        != u64::try_from(bytes.len()).map_err(|_| SnapshotCodecError::TooLarge)?
        || read_u32(&bytes[footer + 16..])? != CCSN_END_MAGIC
    {
        return Err(SnapshotCodecError::Invalid("footer"));
    }
    read_u32(&bytes[footer + 12..footer + 16])
}

#[derive(Clone, Copy, Debug)]
struct CcsnHeader {
    policy_hash: u64,
    index: LogIndex,
    term: Term,
    last_leader_time: Time,
    store_sequence: u64,
    record_count: u64,
}

/// Incremental validator for a CCSN file. It retains only a bounded current
/// record plus the logical state being reconstructed; it never retains the raw
/// checkpoint image. Hosts feed it bytes read back from a staged, fsynced file.
#[derive(Clone, Debug)]
pub struct CcsnStreamDecoder {
    expected_cluster_id: [u8; 16],
    max_total_bytes: u64,
    received: u64,
    pending: Vec<u8>,
    header: Option<CcsnHeader>,
    seen: u64,
    complete: bool,
    records_crc: Crc32c,
    file_crc: Crc32c,
    membership: Option<MembershipState>,
    policy: Option<ClusterPolicy>,
    entries: Vec<LogicalKvEntry>,
    sessions: SessionTable,
    leadership_transfer: Option<LeadershipTransferState>,
    phase: u8,
    prior_key: Option<Vec<u8>>,
    prior_session: Option<SessionKey>,
    prior_tombstone: Option<SessionKey>,
    total_session_bytes: u64,
}

impl CcsnStreamDecoder {
    #[must_use]
    pub fn new(expected_cluster_id: [u8; 16], max_total_bytes: u64) -> Self {
        Self {
            expected_cluster_id,
            max_total_bytes,
            received: 0,
            pending: Vec::new(),
            header: None,
            seen: 0,
            complete: false,
            records_crc: Crc32c::new(),
            file_crc: Crc32c::new(),
            membership: None,
            policy: None,
            entries: Vec::new(),
            sessions: SessionTable::default(),
            leadership_transfer: None,
            phase: 0,
            prior_key: None,
            prior_session: None,
            prior_tombstone: None,
            total_session_bytes: 0,
        }
    }

    /// Feed exactly the next persisted byte range. Gaps, overlaps, and a
    /// record that would exceed the bounded builder are rejected before a
    /// large allocation occurs.
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), SnapshotCodecError> {
        if self.complete {
            return Err(SnapshotCodecError::Invalid("trailing bytes"));
        }
        self.received = self
            .received
            .checked_add(u64::try_from(bytes.len()).map_err(|_| SnapshotCodecError::TooLarge)?)
            .ok_or(SnapshotCodecError::TooLarge)?;
        if self.received > self.max_total_bytes {
            return Err(SnapshotCodecError::TooLarge);
        }
        self.pending.extend_from_slice(bytes);
        self.consume()
    }

    /// Finish a completed stream and return logical state plus the exact
    /// whole-file checksum used by CCRP's `snapshot_crc32c` field.
    pub fn finish(self) -> Result<(CcsnSnapshot, u32), SnapshotCodecError> {
        if !self.complete || !self.pending.is_empty() {
            return Err(SnapshotCodecError::Invalid("truncated"));
        }
        let header = self
            .header
            .ok_or(SnapshotCodecError::Invalid("missing header"))?;
        let membership = self
            .membership
            .ok_or(SnapshotCodecError::Invalid("missing membership"))?;
        let policy = self
            .policy
            .ok_or(SnapshotCodecError::Invalid("missing policy"))?;
        if self.seen != header.record_count || membership.validate().is_err() {
            return Err(SnapshotCodecError::Invalid("record count or membership"));
        }
        if self
            .leadership_transfer
            .is_some_and(|transfer| !membership.voters.contains(&transfer.target))
        {
            return Err(SnapshotCodecError::Invalid("leadership transfer"));
        }
        for entry in &self.entries {
            if entry.key.len() as u64 > policy.max_key_bytes
                || entry.value.len() as u64 > policy.max_value_bytes
            {
                return Err(SnapshotCodecError::TooLarge);
            }
        }
        if self.sessions.records.len() as u64 > policy.max_sessions
            || self.sessions.tombstones.len() as u64 > policy.max_session_tombstones
            || self.total_session_bytes > policy.max_session_bytes
        {
            return Err(SnapshotCodecError::TooLarge);
        }
        let file_crc = self.file_crc.finish();
        Ok((
            CcsnSnapshot {
                cluster_id: self.expected_cluster_id,
                cluster_policy: policy,
                membership,
                kv: LogicalKvSnapshot {
                    entries: self.entries,
                    store_sequence: header.store_sequence,
                    applied_index: header.index,
                    applied_term: header.term,
                    last_leader_time: header.last_leader_time,
                },
                sessions: self.sessions,
                leadership_transfer: self.leadership_transfer,
            },
            file_crc,
        ))
    }

    fn consume(&mut self) -> Result<(), SnapshotCodecError> {
        loop {
            if self.header.is_none() {
                if self.pending.len() < CCSN_HEADER_LEN {
                    return Ok(());
                }
                let header = self.take_prefix(CCSN_HEADER_LEN);
                self.consume_header(&header)?;
                continue;
            }
            let record_count = self.header.expect("header checked").record_count;
            if self.seen < record_count {
                if self.pending.len() < 9 {
                    return Ok(());
                }
                let body_len = u64::from(read_u32(&self.pending[..4])?);
                if body_len > MAX_CCSN_RECORD_BYTES {
                    return Err(SnapshotCodecError::TooLarge);
                }
                let total = usize::try_from(body_len)
                    .ok()
                    .and_then(|length| length.checked_add(9))
                    .ok_or(SnapshotCodecError::TooLarge)?;
                if self.pending.len() < total {
                    return Ok(());
                }
                let record = self.take_prefix(total);
                self.consume_record(&record)?;
                continue;
            }
            if self.pending.len() < CCSN_FOOTER_LEN {
                return Ok(());
            }
            let footer = self.take_prefix(CCSN_FOOTER_LEN);
            self.consume_footer(&footer)?;
            self.complete = true;
            if !self.pending.is_empty() {
                return Err(SnapshotCodecError::Invalid("trailing bytes"));
            }
            return Ok(());
        }
    }

    fn take_prefix(&mut self, len: usize) -> Vec<u8> {
        self.pending.drain(..len).collect()
    }

    fn consume_header(&mut self, bytes: &[u8]) -> Result<(), SnapshotCodecError> {
        if read_u32(&bytes[..4])? != CCSN_MAGIC
            || u16::from_le_bytes(bytes[4..6].try_into().expect("fixed version")) != CCSN_VERSION
        {
            return Err(SnapshotCodecError::Invalid("magic or version"));
        }
        let cluster_id: [u8; 16] = bytes[6..22].try_into().expect("fixed cluster id");
        let header = CcsnHeader {
            policy_hash: read_u64(&bytes[22..30])?,
            index: LogIndex::new(read_u64(&bytes[30..38])?),
            term: Term::new(read_u64(&bytes[38..46])?),
            last_leader_time: Time::from_nanos(read_u64(&bytes[46..54])?),
            store_sequence: read_u64(&bytes[54..62])?,
            record_count: read_u64(&bytes[62..70])?,
        };
        let header_crc = read_u32(&bytes[70..74])?;
        if cluster_id != self.expected_cluster_id
            || crc32c_zeroed_tail(bytes) != header_crc
            || header.index.get() == 0
            || header.term.get() == 0
        {
            return Err(SnapshotCodecError::Invalid("header identity or checksum"));
        }
        self.file_crc.update(bytes);
        self.header = Some(header);
        Ok(())
    }

    fn consume_record(&mut self, record: &[u8]) -> Result<(), SnapshotCodecError> {
        let body_len =
            usize::try_from(read_u32(&record[..4])?).map_err(|_| SnapshotCodecError::TooLarge)?;
        if record.len() != body_len.saturating_add(9) {
            return Err(SnapshotCodecError::Invalid("record length"));
        }
        let body_crc = read_u32(&record[4..8])?;
        let tag = record[8];
        let body = &record[9..];
        if crc32c(&record[8..]) != body_crc {
            return Err(SnapshotCodecError::Invalid("record checksum"));
        }
        let header = self.header.expect("header checked");
        match tag {
            1 if self.phase == 0 && self.membership.is_none() => {
                self.membership = Some(
                    MembershipState::decode(body)
                        .map_err(|_| SnapshotCodecError::Invalid("membership"))?,
                );
                self.phase = 1;
            }
            2 if self.phase == 1 && self.policy.is_none() => {
                let policy = ClusterPolicy::decode(body)
                    .map_err(|_| SnapshotCodecError::Invalid("policy"))?;
                if policy.hash() != header.policy_hash || policy.encode() != body {
                    return Err(SnapshotCodecError::Invalid("policy hash"));
                }
                self.policy = Some(policy);
                self.phase = 2;
            }
            6 if self.phase == 2 && self.leadership_transfer.is_none() => {
                self.leadership_transfer = Some(decode_ccsn_transfer(body, header.index)?);
            }
            3 if self.phase == 2 => {
                let entry = decode_ccsn_key(body, header.store_sequence, header.last_leader_time)?;
                if self.prior_key.as_ref().is_some_and(|key| key >= &entry.key) {
                    return Err(SnapshotCodecError::Invalid("key order"));
                }
                self.prior_key = Some(entry.key.clone());
                self.entries.push(entry);
            }
            4 if self.phase == 2 || self.phase == 3 => {
                self.phase = 3;
                let (key, record) = decode_ccsn_session(body)?;
                if self.prior_session.is_some_and(|prior| prior >= key)
                    || self.sessions.records.contains_key(&key)
                    || self.sessions.tombstones.contains_key(&key)
                    || record.last_active > header.last_leader_time
                {
                    return Err(SnapshotCodecError::Invalid("session order"));
                }
                self.total_session_bytes = self.total_session_bytes.saturating_add(
                    u64::try_from(
                        record
                            .canonical_command
                            .len()
                            .saturating_add(record.cached_reply.len()),
                    )
                    .unwrap_or(u64::MAX),
                );
                self.prior_session = Some(key);
                self.sessions.records.insert(key, record);
            }
            5 if self.phase == 2 || self.phase == 3 || self.phase == 4 => {
                self.phase = 4;
                let (key, max_seq, expires_at) = decode_ccsn_tombstone(body)?;
                if max_seq == 0
                    || expires_at <= header.last_leader_time
                    || self.prior_tombstone.is_some_and(|prior| prior >= key)
                    || self.sessions.records.contains_key(&key)
                    || self
                        .sessions
                        .tombstones
                        .insert(
                            key,
                            SessionTombstone {
                                max_seq,
                                expires_at,
                            },
                        )
                        .is_some()
                {
                    return Err(SnapshotCodecError::Invalid("tombstone order"));
                }
                self.prior_tombstone = Some(key);
            }
            _ => return Err(SnapshotCodecError::Invalid("record tag or order")),
        }
        self.records_crc.update(record);
        self.file_crc.update(record);
        self.seen = self.seen.saturating_add(1);
        Ok(())
    }

    fn consume_footer(&mut self, footer: &[u8]) -> Result<(), SnapshotCodecError> {
        let total_len = read_u64(&footer[..8])?;
        let records_crc = read_u32(&footer[8..12])?;
        let file_crc = read_u32(&footer[12..16])?;
        if total_len != self.received
            || read_u32(&footer[16..])? != CCSN_END_MAGIC
            || self.records_crc.finish() != records_crc
        {
            return Err(SnapshotCodecError::Invalid("footer"));
        }
        self.file_crc.update(&footer[..12]);
        self.file_crc.update(&[0; 4]);
        self.file_crc.update(&footer[16..]);
        if self.file_crc.finish() != file_crc {
            return Err(SnapshotCodecError::Invalid("file checksum"));
        }
        Ok(())
    }
}

/// Bounded canonical CCSN writer. The preflight pass calculates checksums and
/// lengths record-by-record, then the write pass emits no more than the
/// requested chunk size. It deliberately owns logical state, not a complete
/// encoded checkpoint image.
#[derive(Clone, Debug)]
pub struct CcsnStreamEncoder {
    snapshot: CcsnSnapshot,
    header: Bytes,
    footer: Bytes,
    record_count: u64,
    next_record: u64,
    current: Option<Bytes>,
    current_offset: usize,
    done: bool,
    total_len: u64,
    file_crc: u32,
}

impl CcsnStreamEncoder {
    pub fn new(snapshot: CcsnSnapshot) -> Result<Self, SnapshotCodecError> {
        validate_ccsn_encode_snapshot(&snapshot)?;
        let record_count = 2_u64
            .checked_add(u64::from(snapshot.leadership_transfer.is_some()))
            .ok_or(SnapshotCodecError::TooLarge)?
            .checked_add(
                u64::try_from(snapshot.kv.entries.len())
                    .map_err(|_| SnapshotCodecError::TooLarge)?,
            )
            .and_then(|count| {
                count.checked_add(
                    u64::try_from(snapshot.sessions.records.len())
                        .map_err(|_| SnapshotCodecError::TooLarge)
                        .ok()?,
                )
            })
            .and_then(|count| {
                count.checked_add(
                    u64::try_from(snapshot.sessions.tombstones.len())
                        .map_err(|_| SnapshotCodecError::TooLarge)
                        .ok()?,
                )
            })
            .ok_or(SnapshotCodecError::TooLarge)?;
        let mut records_crc = Crc32c::new();
        let mut records_len = 0_u64;
        for ordinal in 0..record_count {
            let record = ccsn_record_by_ordinal(&snapshot, ordinal)?;
            records_crc.update(&record);
            records_len = records_len
                .checked_add(u64::try_from(record.len()).map_err(|_| SnapshotCodecError::TooLarge)?)
                .ok_or(SnapshotCodecError::TooLarge)?;
        }
        let header = ccsn_header(&snapshot, record_count);
        let total_len = u64::try_from(CCSN_HEADER_LEN)
            .map_err(|_| SnapshotCodecError::TooLarge)?
            .checked_add(records_len)
            .and_then(|length| length.checked_add(u64::try_from(CCSN_FOOTER_LEN).ok()?))
            .ok_or(SnapshotCodecError::TooLarge)?;
        let mut footer = Vec::with_capacity(CCSN_FOOTER_LEN);
        footer.extend_from_slice(&total_len.to_le_bytes());
        footer.extend_from_slice(&records_crc.finish().to_le_bytes());
        footer.extend_from_slice(&0_u32.to_le_bytes());
        footer.extend_from_slice(&CCSN_END_MAGIC.to_le_bytes());
        let mut file_crc = Crc32c::new();
        file_crc.update(&header);
        for ordinal in 0..record_count {
            file_crc.update(&ccsn_record_by_ordinal(&snapshot, ordinal)?);
        }
        file_crc.update(&footer);
        let file_crc = file_crc.finish();
        footer[12..16].copy_from_slice(&file_crc.to_le_bytes());
        Ok(Self {
            snapshot,
            header,
            footer,
            record_count,
            next_record: 0,
            current: None,
            current_offset: 0,
            done: false,
            total_len,
            file_crc,
        })
    }

    #[must_use]
    pub const fn total_len(&self) -> u64 {
        self.total_len
    }

    #[must_use]
    pub const fn file_crc(&self) -> u32 {
        self.file_crc
    }

    /// Produce the next bounded byte slice. A zero-sized request is rejected
    /// so a host cannot spin forever while it owns a checkpoint pin.
    pub fn next_chunk(&mut self, max_bytes: usize) -> Result<Option<Bytes>, SnapshotCodecError> {
        if max_bytes == 0 {
            return Err(SnapshotCodecError::TooLarge);
        }
        if self.done {
            return Ok(None);
        }
        let mut output = Vec::with_capacity(max_bytes);
        while output.len() < max_bytes {
            if self.current.is_none() {
                self.current = if self.next_record == 0 {
                    self.next_record = 1;
                    Some(self.header.clone())
                } else if self.next_record <= self.record_count {
                    let ordinal = self.next_record - 1;
                    self.next_record = self.next_record.saturating_add(1);
                    Some(ccsn_record_by_ordinal(&self.snapshot, ordinal)?)
                } else if self.next_record == self.record_count.saturating_add(1) {
                    self.next_record = self.next_record.saturating_add(1);
                    Some(self.footer.clone())
                } else {
                    self.done = true;
                    None
                };
                self.current_offset = 0;
            }
            let Some(current) = self.current.as_ref() else {
                break;
            };
            let available = current.len().saturating_sub(self.current_offset);
            let wanted = max_bytes.saturating_sub(output.len()).min(available);
            output.extend_from_slice(&current[self.current_offset..self.current_offset + wanted]);
            self.current_offset = self.current_offset.saturating_add(wanted);
            if self.current_offset == current.len() {
                self.current = None;
            }
        }
        if output.is_empty() {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}

fn ccsn_header(snapshot: &CcsnSnapshot, record_count: u64) -> Bytes {
    let kv = &snapshot.kv;
    let mut header = Vec::with_capacity(CCSN_HEADER_LEN);
    header.extend_from_slice(&CCSN_MAGIC.to_le_bytes());
    header.extend_from_slice(&CCSN_VERSION.to_le_bytes());
    header.extend_from_slice(&snapshot.cluster_id);
    header.extend_from_slice(&snapshot.cluster_policy.hash().to_le_bytes());
    header.extend_from_slice(&kv.applied_index.get().to_le_bytes());
    header.extend_from_slice(&kv.applied_term.get().to_le_bytes());
    header.extend_from_slice(&kv.last_leader_time.as_nanos().to_le_bytes());
    header.extend_from_slice(&kv.store_sequence.to_le_bytes());
    header.extend_from_slice(&record_count.to_le_bytes());
    header.extend_from_slice(&0_u32.to_le_bytes());
    let header_crc = crc32c_zeroed_tail(&header);
    header[CCSN_HEADER_LEN - 4..].copy_from_slice(&header_crc.to_le_bytes());
    header
}

fn validate_ccsn_encode_snapshot(snapshot: &CcsnSnapshot) -> Result<(), SnapshotCodecError> {
    if snapshot.cluster_id.iter().all(|byte| *byte == 0) {
        return Err(SnapshotCodecError::Invalid("zero cluster id"));
    }
    if snapshot.membership.validate().is_err() {
        return Err(SnapshotCodecError::Invalid("membership"));
    }
    let kv = &snapshot.kv;
    if kv.applied_index.get() == 0 || kv.applied_term.get() == 0 {
        return Err(SnapshotCodecError::Invalid("snapshot base"));
    }
    if let Some(transfer) = snapshot.leadership_transfer
        && (transfer.intent_index.get() == 0
            || transfer.intent_index > kv.applied_index
            || !snapshot.membership.voters.contains(&transfer.target)
            || transfer.admin_session.is_some_and(|(key, sequence)| {
                key.namespace != SessionNamespace::AdminRequest as u8 || sequence == 0
            }))
    {
        return Err(SnapshotCodecError::Invalid("leadership transfer"));
    }
    let mut previous_key: Option<&[u8]> = None;
    for entry in &kv.entries {
        if entry.key.is_empty()
            || entry.sequence == 0
            || entry.sequence > kv.store_sequence
            || entry.key.len() as u64 > snapshot.cluster_policy.max_key_bytes
            || entry.value.len() as u64 > snapshot.cluster_policy.max_value_bytes
            || previous_key.is_some_and(|key| key >= entry.key.as_slice())
            || entry
                .deadline
                .is_some_and(|deadline| deadline <= kv.last_leader_time)
        {
            return Err(SnapshotCodecError::Invalid("key order or field"));
        }
        previous_key = Some(&entry.key);
    }
    let mut previous_session = None;
    let mut total_session_bytes = 0_u64;
    for (key, record) in &snapshot.sessions.records {
        if key.namespace > SessionNamespace::AdminRequest as u8
            || record.max_seq == 0
            || record.last_active > kv.last_leader_time
            || previous_session.is_some_and(|prior| prior >= *key)
            || !canonical_session_payload(
                key.namespace,
                &record.canonical_command,
                &record.cached_reply,
            )
        {
            return Err(SnapshotCodecError::Invalid("session record"));
        }
        total_session_bytes = total_session_bytes.saturating_add(
            u64::try_from(
                record
                    .canonical_command
                    .len()
                    .saturating_add(record.cached_reply.len()),
            )
            .unwrap_or(u64::MAX),
        );
        if total_session_bytes > snapshot.cluster_policy.max_session_bytes {
            return Err(SnapshotCodecError::TooLarge);
        }
        previous_session = Some(*key);
    }
    let mut previous_tombstone = None;
    for (key, tombstone) in &snapshot.sessions.tombstones {
        if key.namespace > SessionNamespace::AdminRequest as u8
            || tombstone.max_seq == 0
            || tombstone.expires_at <= kv.last_leader_time
            || snapshot.sessions.records.contains_key(key)
            || previous_tombstone.is_some_and(|prior| prior >= *key)
        {
            return Err(SnapshotCodecError::Invalid("session tombstone"));
        }
        previous_tombstone = Some(*key);
    }
    Ok(())
}

fn ccsn_record_by_ordinal(
    snapshot: &CcsnSnapshot,
    ordinal: u64,
) -> Result<Bytes, SnapshotCodecError> {
    if ordinal == 0 {
        return ccsn_record(
            1,
            snapshot
                .membership
                .encode()
                .map_err(|_| SnapshotCodecError::Invalid("membership"))?,
        );
    }
    if ordinal == 1 {
        return ccsn_record(2, snapshot.cluster_policy.encode());
    }
    let transfer_count = u64::from(snapshot.leadership_transfer.is_some());
    if ordinal == 2
        && let Some(transfer) = snapshot.leadership_transfer
    {
        return ccsn_record(6, encode_ccsn_transfer(transfer));
    }
    let entry_count =
        u64::try_from(snapshot.kv.entries.len()).map_err(|_| SnapshotCodecError::TooLarge)?;
    let after_prefix = ordinal.saturating_sub(2 + transfer_count);
    if after_prefix < entry_count {
        let entry = &snapshot.kv.entries
            [usize::try_from(after_prefix).map_err(|_| SnapshotCodecError::TooLarge)?];
        let mut body = Enc::new();
        body.bytes(&entry.key);
        body.u64(entry.sequence);
        body.bytes(&entry.value);
        match entry.deadline {
            Some(deadline) => {
                body.u8(1);
                body.u64(deadline.as_nanos());
            }
            None => {
                body.u8(0);
                body.u64(0);
            }
        }
        return ccsn_record(3, body.finish());
    }
    let session_ordinal = after_prefix.saturating_sub(entry_count);
    let session_count =
        u64::try_from(snapshot.sessions.records.len()).map_err(|_| SnapshotCodecError::TooLarge)?;
    if session_ordinal < session_count {
        let (key, record) = snapshot
            .sessions
            .records
            .iter()
            .nth(usize::try_from(session_ordinal).map_err(|_| SnapshotCodecError::TooLarge)?)
            .ok_or(SnapshotCodecError::Invalid("session ordinal"))?;
        let mut body = Enc::new();
        body.u8(key.namespace);
        body.u64(key.client.get());
        body.u64(record.max_seq);
        body.u64(record.last_active.as_nanos());
        body.bytes(&record.canonical_command);
        body.bytes(&record.cached_reply);
        return ccsn_record(4, body.finish());
    }
    let tombstone_ordinal = session_ordinal.saturating_sub(session_count);
    let (key, tombstone) = snapshot
        .sessions
        .tombstones
        .iter()
        .nth(usize::try_from(tombstone_ordinal).map_err(|_| SnapshotCodecError::TooLarge)?)
        .ok_or(SnapshotCodecError::Invalid("tombstone ordinal"))?;
    let mut body = Enc::new();
    body.u8(key.namespace);
    body.u64(key.client.get());
    body.u64(tombstone.max_seq);
    body.u64(tombstone.expires_at.as_nanos());
    ccsn_record(5, body.finish())
}

/// Encode the one canonical CCSN v1 representation. It exports logical state
/// rather than serializing a store image; [`CcsnStreamEncoder`] emits the same
/// record sequence without inheriting table/WAL details.
pub fn encode_ccsn(snapshot: &CcsnSnapshot) -> Result<Bytes, SnapshotCodecError> {
    if snapshot.cluster_id.iter().all(|byte| *byte == 0) {
        return Err(SnapshotCodecError::Invalid("zero cluster id"));
    }
    if snapshot.membership.validate().is_err() {
        return Err(SnapshotCodecError::Invalid("membership"));
    }
    let kv = &snapshot.kv;
    if kv.applied_index.get() == 0 || kv.applied_term.get() == 0 {
        return Err(SnapshotCodecError::Invalid("snapshot base"));
    }
    let mut records = Vec::new();
    records.push(ccsn_record(
        1,
        snapshot
            .membership
            .encode()
            .map_err(|_| SnapshotCodecError::Invalid("membership"))?,
    )?);
    records.push(ccsn_record(2, snapshot.cluster_policy.encode())?);
    if let Some(transfer) = snapshot.leadership_transfer {
        if transfer.intent_index.get() == 0
            || transfer.intent_index > kv.applied_index
            || !snapshot.membership.voters.contains(&transfer.target)
            || transfer.admin_session.is_some_and(|(key, sequence)| {
                key.namespace != SessionNamespace::AdminRequest as u8 || sequence == 0
            })
        {
            return Err(SnapshotCodecError::Invalid("leadership transfer"));
        }
        records.push(ccsn_record(6, encode_ccsn_transfer(transfer))?);
    }
    let mut previous_key: Option<&[u8]> = None;
    for entry in &kv.entries {
        if entry.key.is_empty()
            || entry.sequence == 0
            || entry.sequence > kv.store_sequence
            || entry.key.len() as u64 > snapshot.cluster_policy.max_key_bytes
            || entry.value.len() as u64 > snapshot.cluster_policy.max_value_bytes
            || previous_key.is_some_and(|key| key >= entry.key.as_slice())
            || entry
                .deadline
                .is_some_and(|deadline| deadline <= kv.last_leader_time)
        {
            return Err(SnapshotCodecError::Invalid("key order or field"));
        }
        previous_key = Some(&entry.key);
        let mut body = Enc::new();
        body.bytes(&entry.key);
        body.u64(entry.sequence);
        body.bytes(&entry.value);
        match entry.deadline {
            Some(deadline) => {
                body.u8(1);
                body.u64(deadline.as_nanos());
            }
            None => {
                body.u8(0);
                body.u64(0);
            }
        }
        records.push(ccsn_record(3, body.finish())?);
    }
    let mut previous_session: Option<SessionKey> = None;
    let mut total_session_bytes = 0_u64;
    for (key, record) in &snapshot.sessions.records {
        if key.namespace > SessionNamespace::AdminRequest as u8
            || record.max_seq == 0
            || record.last_active > kv.last_leader_time
            || previous_session.is_some_and(|prior| prior >= *key)
            || !canonical_session_payload(
                key.namespace,
                &record.canonical_command,
                &record.cached_reply,
            )
        {
            return Err(SnapshotCodecError::Invalid("session record"));
        }
        let session_bytes = record
            .canonical_command
            .len()
            .saturating_add(record.cached_reply.len());
        total_session_bytes = total_session_bytes.saturating_add(session_bytes as u64);
        if total_session_bytes > snapshot.cluster_policy.max_session_bytes {
            return Err(SnapshotCodecError::TooLarge);
        }
        previous_session = Some(*key);
        let mut body = Enc::new();
        body.u8(key.namespace);
        body.u64(key.client.get());
        body.u64(record.max_seq);
        body.u64(record.last_active.as_nanos());
        body.bytes(&record.canonical_command);
        body.bytes(&record.cached_reply);
        records.push(ccsn_record(4, body.finish())?);
    }
    let mut previous_tombstone: Option<SessionKey> = None;
    for (key, tombstone) in &snapshot.sessions.tombstones {
        if key.namespace > SessionNamespace::AdminRequest as u8
            || tombstone.max_seq == 0
            || tombstone.expires_at <= kv.last_leader_time
            || snapshot.sessions.records.contains_key(key)
            || previous_tombstone.is_some_and(|prior| prior >= *key)
        {
            return Err(SnapshotCodecError::Invalid("session tombstone"));
        }
        previous_tombstone = Some(*key);
        let mut body = Enc::new();
        body.u8(key.namespace);
        body.u64(key.client.get());
        body.u64(tombstone.max_seq);
        body.u64(tombstone.expires_at.as_nanos());
        records.push(ccsn_record(5, body.finish())?);
    }
    let record_count = u64::try_from(records.len()).map_err(|_| SnapshotCodecError::TooLarge)?;
    let mut output = Vec::new();
    output.extend_from_slice(&CCSN_MAGIC.to_le_bytes());
    output.extend_from_slice(&CCSN_VERSION.to_le_bytes());
    output.extend_from_slice(&snapshot.cluster_id);
    output.extend_from_slice(&snapshot.cluster_policy.hash().to_le_bytes());
    output.extend_from_slice(&kv.applied_index.get().to_le_bytes());
    output.extend_from_slice(&kv.applied_term.get().to_le_bytes());
    output.extend_from_slice(&kv.last_leader_time.as_nanos().to_le_bytes());
    output.extend_from_slice(&kv.store_sequence.to_le_bytes());
    output.extend_from_slice(&record_count.to_le_bytes());
    output.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(output.len(), CCSN_HEADER_LEN);
    let header_crc = crc32c_zeroed_tail(&output);
    output[CCSN_HEADER_LEN - 4..].copy_from_slice(&header_crc.to_le_bytes());
    for record in records {
        output.extend_from_slice(&record);
    }
    let records_crc = crc32c(&output[CCSN_HEADER_LEN..]);
    let total_len = u64::try_from(output.len().saturating_add(CCSN_FOOTER_LEN))
        .map_err(|_| SnapshotCodecError::TooLarge)?;
    output.extend_from_slice(&total_len.to_le_bytes());
    output.extend_from_slice(&records_crc.to_le_bytes());
    let file_crc_offset = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    output.extend_from_slice(&CCSN_END_MAGIC.to_le_bytes());
    let file_crc = crc32c(&output);
    output[file_crc_offset..file_crc_offset + 4].copy_from_slice(&file_crc.to_le_bytes());
    Ok(output)
}

/// Decode and validate a complete CCSN v1 file. Hosts verify the same whole
/// checksum while staging chunks; this parser validates the independent
/// logical invariants before the core swaps any state.
pub fn decode_ccsn(
    bytes: &[u8],
    expected_cluster_id: [u8; 16],
    max_total_bytes: u64,
) -> Result<CcsnSnapshot, SnapshotCodecError> {
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_total_bytes {
        return Err(SnapshotCodecError::TooLarge);
    }
    if bytes.len() < CCSN_HEADER_LEN + CCSN_FOOTER_LEN {
        return Err(SnapshotCodecError::Invalid("truncated"));
    }
    if read_u32(&bytes[..4])? != CCSN_MAGIC
        || u16::from_le_bytes(bytes[4..6].try_into().expect("fixed version")) != CCSN_VERSION
    {
        return Err(SnapshotCodecError::Invalid("magic or version"));
    }
    let cluster_id: [u8; 16] = bytes[6..22].try_into().expect("fixed cluster id");
    let policy_hash = read_u64(&bytes[22..30])?;
    let index = LogIndex::new(read_u64(&bytes[30..38])?);
    let term = Term::new(read_u64(&bytes[38..46])?);
    let last_leader_time = Time::from_nanos(read_u64(&bytes[46..54])?);
    let store_sequence = read_u64(&bytes[54..62])?;
    let record_count = read_u64(&bytes[62..70])?;
    let header_crc = read_u32(&bytes[70..74])?;
    if cluster_id != expected_cluster_id
        || crc32c_zeroed_tail(&bytes[..CCSN_HEADER_LEN]) != header_crc
        || index.get() == 0
        || term.get() == 0
    {
        return Err(SnapshotCodecError::Invalid("header identity or checksum"));
    }
    let footer = bytes.len() - CCSN_FOOTER_LEN;
    let total_len = read_u64(&bytes[footer..footer + 8])?;
    let records_crc = read_u32(&bytes[footer + 8..footer + 12])?;
    let file_crc = read_u32(&bytes[footer + 12..footer + 16])?;
    if total_len != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
        || read_u32(&bytes[footer + 16..])? != CCSN_END_MAGIC
        || crc32c(&bytes[CCSN_HEADER_LEN..footer]) != records_crc
    {
        return Err(SnapshotCodecError::Invalid("footer"));
    }
    let mut file_copy = bytes.to_vec();
    file_copy[footer + 12..footer + 16].fill(0);
    if crc32c(&file_copy) != file_crc {
        return Err(SnapshotCodecError::Invalid("file checksum"));
    }
    let mut offset = CCSN_HEADER_LEN;
    let mut seen = 0_u64;
    let mut membership = None;
    let mut policy = None;
    let mut entries = Vec::new();
    let mut sessions = SessionTable::default();
    let mut leadership_transfer = None;
    let mut phase = 0_u8;
    let mut prior_key: Option<Vec<u8>> = None;
    let mut prior_session: Option<SessionKey> = None;
    let mut prior_tombstone: Option<SessionKey> = None;
    let mut total_session_bytes = 0_u64;
    while offset < footer {
        if seen >= record_count || footer.saturating_sub(offset) < 9 {
            return Err(SnapshotCodecError::Invalid("record count or framing"));
        }
        let body_len = usize::try_from(read_u32(&bytes[offset..offset + 4])?)
            .map_err(|_| SnapshotCodecError::TooLarge)?;
        let body_crc = read_u32(&bytes[offset + 4..offset + 8])?;
        let tag = bytes[offset + 8];
        let body_start = offset + 9;
        let body_end = body_start
            .checked_add(body_len)
            .filter(|end| *end <= footer)
            .ok_or(SnapshotCodecError::Invalid("record length"))?;
        if crc32c(&bytes[offset + 8..body_end]) != body_crc {
            return Err(SnapshotCodecError::Invalid("record checksum"));
        }
        let body = &bytes[body_start..body_end];
        match tag {
            1 if phase == 0 && membership.is_none() => {
                membership = Some(
                    MembershipState::decode(body)
                        .map_err(|_| SnapshotCodecError::Invalid("membership"))?,
                );
                phase = 1;
            }
            2 if phase == 1 && policy.is_none() => {
                let decoded = ClusterPolicy::decode(body)
                    .map_err(|_| SnapshotCodecError::Invalid("policy"))?;
                if decoded.hash() != policy_hash || decoded.encode() != body {
                    return Err(SnapshotCodecError::Invalid("policy hash"));
                }
                policy = Some(decoded);
                phase = 2;
            }
            6 if phase == 2 && leadership_transfer.is_none() => {
                leadership_transfer = Some(decode_ccsn_transfer(body, index)?);
            }
            3 if phase == 2 => {
                let entry = decode_ccsn_key(body, store_sequence, last_leader_time)?;
                if prior_key.as_ref().is_some_and(|key| key >= &entry.key) {
                    return Err(SnapshotCodecError::Invalid("key order"));
                }
                prior_key = Some(entry.key.clone());
                entries.push(entry);
            }
            4 if phase == 2 || phase == 3 => {
                phase = 3;
                let (key, record) = decode_ccsn_session(body)?;
                if prior_session.is_some_and(|prior| prior >= key)
                    || sessions.records.contains_key(&key)
                    || sessions.tombstones.contains_key(&key)
                    || record.last_active > last_leader_time
                {
                    return Err(SnapshotCodecError::Invalid("session order"));
                }
                total_session_bytes = total_session_bytes.saturating_add(
                    u64::try_from(
                        record
                            .canonical_command
                            .len()
                            .saturating_add(record.cached_reply.len()),
                    )
                    .unwrap_or(u64::MAX),
                );
                prior_session = Some(key);
                sessions.records.insert(key, record);
            }
            5 if phase == 2 || phase == 3 || phase == 4 => {
                phase = 4;
                let (key, max_seq, expires_at) = decode_ccsn_tombstone(body)?;
                if max_seq == 0
                    || expires_at <= last_leader_time
                    || prior_tombstone.is_some_and(|prior| prior >= key)
                    || sessions.records.contains_key(&key)
                    || sessions
                        .tombstones
                        .insert(
                            key,
                            SessionTombstone {
                                max_seq,
                                expires_at,
                            },
                        )
                        .is_some()
                {
                    return Err(SnapshotCodecError::Invalid("tombstone order"));
                }
                prior_tombstone = Some(key);
            }
            _ => return Err(SnapshotCodecError::Invalid("record tag or order")),
        }
        seen = seen.saturating_add(1);
        offset = body_end;
    }
    let membership = membership.ok_or(SnapshotCodecError::Invalid("missing membership"))?;
    let policy = policy.ok_or(SnapshotCodecError::Invalid("missing policy"))?;
    if seen != record_count || membership.validate().is_err() {
        return Err(SnapshotCodecError::Invalid("record count or membership"));
    }
    if leadership_transfer.is_some_and(|transfer| !membership.voters.contains(&transfer.target)) {
        return Err(SnapshotCodecError::Invalid("leadership transfer"));
    }
    for entry in &entries {
        if entry.key.len() as u64 > policy.max_key_bytes
            || entry.value.len() as u64 > policy.max_value_bytes
        {
            return Err(SnapshotCodecError::TooLarge);
        }
    }
    if sessions.records.len() as u64 > policy.max_sessions
        || sessions.tombstones.len() as u64 > policy.max_session_tombstones
        || total_session_bytes > policy.max_session_bytes
    {
        return Err(SnapshotCodecError::TooLarge);
    }
    Ok(CcsnSnapshot {
        cluster_id,
        cluster_policy: policy,
        membership,
        kv: LogicalKvSnapshot {
            entries,
            store_sequence,
            applied_index: index,
            applied_term: term,
            last_leader_time,
        },
        sessions,
        leadership_transfer,
    })
}

fn ccsn_record(tag: u8, body: Bytes) -> Result<Bytes, SnapshotCodecError> {
    let len = u32::try_from(body.len()).map_err(|_| SnapshotCodecError::TooLarge)?;
    let mut record = Vec::with_capacity(body.len().saturating_add(9));
    record.extend_from_slice(&len.to_le_bytes());
    record.extend_from_slice(&0_u32.to_le_bytes());
    record.push(tag);
    record.extend_from_slice(&body);
    let crc = crc32c(&record[8..]);
    record[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(record)
}

fn decode_ccsn_key(
    body: &[u8],
    store_sequence: u64,
    last_leader_time: Time,
) -> Result<LogicalKvEntry, SnapshotCodecError> {
    let mut dec = Dec::new(body);
    let key = dec
        .bytes()
        .map_err(|_| SnapshotCodecError::Invalid("key"))?;
    let sequence = dec.u64().map_err(|_| SnapshotCodecError::Invalid("key"))?;
    let value = dec
        .bytes()
        .map_err(|_| SnapshotCodecError::Invalid("key"))?;
    let has_deadline = dec.u8().map_err(|_| SnapshotCodecError::Invalid("key"))?;
    let deadline_raw = dec.u64().map_err(|_| SnapshotCodecError::Invalid("key"))?;
    dec.finish()
        .map_err(|_| SnapshotCodecError::Invalid("key"))?;
    let deadline = match has_deadline {
        0 if deadline_raw == 0 => None,
        1 if deadline_raw > last_leader_time.as_nanos() => Some(Time::from_nanos(deadline_raw)),
        _ => return Err(SnapshotCodecError::Invalid("deadline")),
    };
    if key.is_empty() || sequence == 0 || sequence > store_sequence {
        return Err(SnapshotCodecError::Invalid("key sequence"));
    }
    Ok(LogicalKvEntry {
        key,
        sequence,
        value,
        deadline,
    })
}

fn decode_ccsn_session(body: &[u8]) -> Result<(SessionKey, SessionRecord), SnapshotCodecError> {
    let mut dec = Dec::new(body);
    let namespace = dec
        .u8()
        .map_err(|_| SnapshotCodecError::Invalid("session"))?;
    let key = SessionKey::new(
        namespace,
        ClientId::new(
            dec.u64()
                .map_err(|_| SnapshotCodecError::Invalid("session"))?,
        ),
    )
    .map_err(|_| SnapshotCodecError::Invalid("session identity"))?;
    let max_seq = dec
        .u64()
        .map_err(|_| SnapshotCodecError::Invalid("session"))?;
    let last_active = Time::from_nanos(
        dec.u64()
            .map_err(|_| SnapshotCodecError::Invalid("session"))?,
    );
    let canonical_command = dec
        .bytes()
        .map_err(|_| SnapshotCodecError::Invalid("session"))?;
    let cached_reply = dec
        .bytes()
        .map_err(|_| SnapshotCodecError::Invalid("session"))?;
    dec.finish()
        .map_err(|_| SnapshotCodecError::Invalid("session"))?;
    if max_seq == 0 || !canonical_session_payload(namespace, &canonical_command, &cached_reply) {
        return Err(SnapshotCodecError::Invalid("session payload"));
    }
    Ok((
        key,
        SessionRecord {
            max_seq,
            canonical_command,
            cached_reply,
            last_active,
        },
    ))
}

fn decode_ccsn_tombstone(body: &[u8]) -> Result<(SessionKey, u64, Time), SnapshotCodecError> {
    let mut dec = Dec::new(body);
    let namespace = dec
        .u8()
        .map_err(|_| SnapshotCodecError::Invalid("tombstone"))?;
    let key = SessionKey::new(
        namespace,
        ClientId::new(
            dec.u64()
                .map_err(|_| SnapshotCodecError::Invalid("tombstone"))?,
        ),
    )
    .map_err(|_| SnapshotCodecError::Invalid("tombstone identity"))?;
    let max_seq = dec
        .u64()
        .map_err(|_| SnapshotCodecError::Invalid("tombstone"))?;
    let expires_at = Time::from_nanos(
        dec.u64()
            .map_err(|_| SnapshotCodecError::Invalid("tombstone"))?,
    );
    dec.finish()
        .map_err(|_| SnapshotCodecError::Invalid("tombstone"))?;
    Ok((key, max_seq, expires_at))
}

fn encode_ccsn_transfer(transfer: LeadershipTransferState) -> Bytes {
    let mut body = Enc::new();
    body.u64(transfer.intent_index.get());
    body.u64(transfer.target.get());
    body.u64(transfer.deadline.as_nanos());
    body.u8(u8::from(transfer.finishing));
    match transfer.admin_session {
        Some((key, sequence)) => {
            body.u8(1);
            body.u8(key.namespace);
            body.u64(key.client.get());
            body.u64(sequence);
        }
        None => {
            body.u8(0);
            body.u8(0);
            body.u64(0);
            body.u64(0);
        }
    }
    body.finish()
}

fn decode_ccsn_transfer(
    body: &[u8],
    snapshot_index: LogIndex,
) -> Result<LeadershipTransferState, SnapshotCodecError> {
    let mut dec = Dec::new(body);
    let intent_index = LogIndex::new(
        dec.u64()
            .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?,
    );
    let target = NodeId::new(
        dec.u64()
            .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?,
    );
    let deadline = Time::from_nanos(
        dec.u64()
            .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?,
    );
    let finishing = match dec
        .u8()
        .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?
    {
        0 => false,
        1 => true,
        _ => return Err(SnapshotCodecError::Invalid("leadership transfer")),
    };
    let has_admin = dec
        .u8()
        .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?;
    let namespace = dec
        .u8()
        .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?;
    let client = dec
        .u64()
        .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?;
    let sequence = dec
        .u64()
        .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?;
    dec.finish()
        .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?;
    let admin_session = match has_admin {
        0 if namespace == 0 && client == 0 && sequence == 0 => None,
        1 if namespace == SessionNamespace::AdminRequest as u8 && sequence != 0 => Some((
            SessionKey::new(namespace, ClientId::new(client))
                .map_err(|_| SnapshotCodecError::Invalid("leadership transfer"))?,
            sequence,
        )),
        _ => return Err(SnapshotCodecError::Invalid("leadership transfer")),
    };
    if intent_index.get() == 0 || intent_index > snapshot_index || target.get() == 0 {
        return Err(SnapshotCodecError::Invalid("leadership transfer"));
    }
    Ok(LeadershipTransferState {
        intent_index,
        target,
        deadline,
        finishing,
        admin_session,
    })
}

fn canonical_session_payload(namespace: u8, command: &[u8], reply: &[u8]) -> bool {
    match namespace {
        value if value == SessionNamespace::UserRequest as u8 => {
            decode_command(command).is_ok_and(|parsed| encode_command(&parsed) == command)
                && decode_reply(reply).is_ok_and(|parsed| encode_reply(&parsed) == reply)
        }
        value if value == SessionNamespace::AdminRequest as u8 => {
            ConfigEnvelope::decode(command).is_ok_and(|parsed| parsed.encode() == command)
                && AdminReply::decode(reply).is_ok_and(|parsed| parsed.encode() == reply)
        }
        _ => false,
    }
}

fn read_u32(bytes: &[u8]) -> Result<u32, SnapshotCodecError> {
    bytes
        .try_into()
        .map(u32::from_le_bytes)
        .map_err(|_| SnapshotCodecError::Invalid("integer"))
}

fn read_u64(bytes: &[u8]) -> Result<u64, SnapshotCodecError> {
    bytes
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| SnapshotCodecError::Invalid("integer"))
}

/// The recovered durable value passed into the deterministic composition.  It
/// is owned below the host layer, so an adapter cannot smuggle back volatile
/// Raft or session state after a crash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredNode {
    pub hard_state: HardState,
    pub log_base: (LogIndex, Term),
    pub entries: Vec<Entry>,
    pub membership: MembershipState,
    pub cluster_policy: ClusterPolicy,
    pub snapshot: Option<NodeSnapshot>,
    pub durable_applied: (LogIndex, Term),
}

impl RecoveredNode {
    /// Combine the durable Raft-log recovery result with the independently
    /// decoded logical snapshot.  `cc-log` proves log byte continuity while
    /// this layer keeps membership/session/KV authority explicit.
    #[must_use]
    pub fn from_log(
        recovered: cc_log::RecoveredLog,
        membership: MembershipState,
        snapshot: Option<NodeSnapshot>,
        durable_applied: (LogIndex, Term),
    ) -> Self {
        Self {
            hard_state: recovered.state.hard_state,
            log_base: (recovered.state.base_index, recovered.state.base_term),
            entries: recovered.state.entries,
            membership,
            cluster_policy: recovered.state.genesis.policy,
            snapshot,
            durable_applied,
        }
    }
}

/// Canonical application-log envelope. Host route ids are intentionally absent:
/// only an explicit SessionKey is replicated and retained across restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppEnvelope {
    pub session: Option<(SessionKey, u64)>,
    pub leader_time: Time,
    pub command: Bytes,
}

impl AppEnvelope {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.header(APP_ENVELOPE_MAGIC, APP_ENVELOPE_VERSION);
        match self.session {
            Some((key, sequence)) => {
                enc.u8(1);
                enc.u8(key.namespace);
                enc.u64(key.client.get());
                enc.u64(sequence);
            }
            None => {
                enc.u8(0);
                enc.u8(0);
                enc.u64(0);
                enc.u64(0);
            }
        }
        enc.u64(self.leader_time.as_nanos());
        enc.bytes(&self.command);
        let mut bytes = enc.finish();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c_zeroed_tail(&bytes);
        let checksum_start = bytes.len() - 4;
        bytes[checksum_start..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, KvError> {
        if bytes.len() < 4 {
            return Err(KvError::InvalidInput);
        }
        let body_len = bytes.len() - 4;
        let expected = u32::from_le_bytes(bytes[body_len..].try_into().expect("CRC field"));
        if crc32c_zeroed_tail(bytes) != expected {
            return Err(KvError::InvalidInput);
        }
        let mut dec = Dec::new(&bytes[..body_len]);
        dec.header(APP_ENVELOPE_MAGIC, APP_ENVELOPE_VERSION)?;
        let has_session = dec.u8()?;
        let namespace = dec.u8()?;
        let client = ClientId::new(dec.u64()?);
        let sequence = dec.u64()?;
        let session = match has_session {
            0 if namespace == 0 && client.get() == 0 && sequence == 0 => None,
            1 if sequence != 0 => Some((
                SessionKey::new(namespace, client).map_err(KvError::from)?,
                sequence,
            )),
            _ => return Err(KvError::InvalidInput),
        };
        let leader_time = Time::from_nanos(dec.u64()?);
        let command = dec.bytes()?;
        dec.finish()?;
        decode_command(&command)?;
        Ok(Self {
            session,
            leader_time,
            command,
        })
    }
}

/// Generic replicated retry state. It sits above the key/value machine so all
/// replicated command families share one canonical request identity.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct SessionTable {
    records: BTreeMap<SessionKey, SessionRecord>,
    tombstones: BTreeMap<SessionKey, SessionTombstone>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRecord {
    pub max_seq: u64,
    pub canonical_command: Bytes,
    pub cached_reply: Bytes,
    pub last_active: Time,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionTombstone {
    pub max_seq: u64,
    pub expires_at: Time,
}

struct AdminApplyContext {
    policy: ClusterPolicy,
    key: SessionKey,
    sequence: u64,
    canonical_command: Bytes,
    operation_tag: u8,
    source_index: LogIndex,
    at: Time,
}

impl SessionTable {
    pub fn from_snapshot_parts(
        records: BTreeMap<SessionKey, SessionRecord>,
        tombstones: BTreeMap<SessionKey, SessionTombstone>,
    ) -> Result<Self, SnapshotCodecError> {
        if records.keys().any(|key| tombstones.contains_key(key))
            || records.values().any(|record| record.max_seq == 0)
            || tombstones.values().any(|tombstone| tombstone.max_seq == 0)
        {
            return Err(SnapshotCodecError::Invalid("session snapshot parts"));
        }
        Ok(Self {
            records,
            tombstones,
        })
    }

    #[must_use]
    pub fn contains(&self, key: SessionKey) -> bool {
        self.records.contains_key(&key) || self.tombstones.contains_key(&key)
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    fn encoded_bytes(&self) -> u64 {
        self.records.values().fold(0_u64, |total, record| {
            total.saturating_add(
                u64::try_from(
                    record
                        .canonical_command
                        .len()
                        .saturating_add(record.cached_reply.len()),
                )
                .unwrap_or(u64::MAX),
            )
        })
    }

    fn reservation_for(
        &self,
        policy: ClusterPolicy,
        key: SessionKey,
        sequence: u64,
        canonical_command: &[u8],
    ) -> Result<SessionReservation, KvError> {
        if sequence == 0 {
            return Err(KvError::InvalidInput);
        }
        if let Some(tombstone) = self.tombstones.get(&key)
            && sequence <= tombstone.max_seq
        {
            return Ok(SessionReservation::default());
        }
        if let Some(record) = self.records.get(&key)
            && sequence <= record.max_seq
        {
            return Ok(SessionReservation::default());
        }
        let old_bytes = self.records.get(&key).map_or(0, |record| {
            u64::try_from(
                record
                    .canonical_command
                    .len()
                    .saturating_add(record.cached_reply.len()),
            )
            .unwrap_or(u64::MAX)
        });
        let bytes = u64::try_from(canonical_command.len())
            .unwrap_or(u64::MAX)
            .checked_add(policy.max_reply_bytes)
            .ok_or(KvError::Busy)?;
        Ok(SessionReservation {
            bytes: bytes.saturating_sub(old_bytes.min(bytes)),
            new_session: !self.records.contains_key(&key),
        })
    }

    fn first_expiry(
        &self,
        policy: ClusterPolicy,
        pinned: Option<SessionKey>,
    ) -> Option<(Time, SessionKey)> {
        self.records
            .iter()
            .filter(|(key, _)| Some(**key) != pinned)
            .map(|(key, record)| {
                (
                    Time::from_nanos(
                        record
                            .last_active
                            .as_nanos()
                            .saturating_add(policy.session_idle_ns),
                    ),
                    *key,
                )
            })
            .chain(
                self.tombstones
                    .iter()
                    .filter(|(key, _)| Some(**key) != pinned)
                    .map(|(key, tombstone)| (tombstone.expires_at, *key)),
            )
            .min()
    }

    fn expire_due(
        &mut self,
        policy: ClusterPolicy,
        up_to: Time,
        max_items: u32,
        pinned: Option<SessionKey>,
    ) -> u32 {
        let mut deadlines = self
            .records
            .iter()
            .filter(|(key, _)| Some(**key) != pinned)
            .map(|(key, record)| {
                (
                    Time::from_nanos(
                        record
                            .last_active
                            .as_nanos()
                            .saturating_add(policy.session_idle_ns),
                    ),
                    *key,
                    false,
                )
            })
            .chain(
                self.tombstones
                    .iter()
                    .filter(|(key, _)| Some(**key) != pinned)
                    .map(|(key, tombstone)| (tombstone.expires_at, *key, true)),
            )
            .filter(|(deadline, _, _)| *deadline <= up_to)
            .collect::<Vec<_>>();
        deadlines.sort_unstable();
        deadlines.truncate(max_items as usize);
        for (idle_deadline, key, tombstone) in &deadlines {
            if *tombstone {
                self.tombstones.remove(key);
                continue;
            }
            let Some(record) = self.records.remove(key) else {
                continue;
            };
            let grace_end = Time::from_nanos(
                idle_deadline
                    .as_nanos()
                    .saturating_add(policy.session_retry_grace_ns),
            );
            if grace_end > up_to && (self.tombstones.len() as u64) < policy.max_session_tombstones {
                self.tombstones.insert(
                    *key,
                    SessionTombstone {
                        max_seq: record.max_seq,
                        expires_at: grace_end,
                    },
                );
            }
        }
        u32::try_from(deadlines.len()).unwrap_or(u32::MAX)
    }

    /// Filter replicated retry state for a fresh-cluster logical restore.
    /// Administrative requests describe membership workflows from the source
    /// cluster and therefore cannot survive a new identity. User sessions
    /// retain their exact retry bytes while live; idle records become only a
    /// bounded retry-grace tombstone, so restore never wildcard-matches a
    /// reply that was not carried by the checkpoint.
    pub fn for_fresh_cluster_restore(
        &self,
        policy: ClusterPolicy,
        at: Time,
    ) -> Result<Self, SnapshotCodecError> {
        let mut records = BTreeMap::new();
        let mut tombstones = BTreeMap::new();
        for (key, record) in &self.records {
            if key.namespace != SessionNamespace::UserRequest as u8 {
                continue;
            }
            let idle_until = record
                .last_active
                .as_nanos()
                .saturating_add(policy.session_idle_ns);
            if at.as_nanos() <= idle_until {
                records.insert(*key, record.clone());
            } else {
                let expires_at =
                    Time::from_nanos(idle_until.saturating_add(policy.session_retry_grace_ns));
                if expires_at > at {
                    tombstones.insert(
                        *key,
                        SessionTombstone {
                            max_seq: record.max_seq,
                            expires_at,
                        },
                    );
                }
            }
        }
        for (key, tombstone) in &self.tombstones {
            if key.namespace == SessionNamespace::UserRequest as u8
                && tombstone.expires_at > at
                && !records.contains_key(key)
            {
                tombstones
                    .entry(*key)
                    .and_modify(|current| current.max_seq = current.max_seq.max(tombstone.max_seq))
                    .or_insert(*tombstone);
            }
        }
        let bytes = records.values().try_fold(0_u64, |total, record| {
            total
                .checked_add(
                    u64::try_from(
                        record
                            .canonical_command
                            .len()
                            .saturating_add(record.cached_reply.len()),
                    )
                    .unwrap_or(u64::MAX),
                )
                .ok_or(SnapshotCodecError::TooLarge)
        })?;
        if records.len() as u64 > policy.max_sessions
            || tombstones.len() as u64 > policy.max_session_tombstones
            || bytes > policy.max_session_bytes
        {
            return Err(SnapshotCodecError::TooLarge);
        }
        Ok(Self {
            records,
            tombstones,
        })
    }

    fn apply_user(
        &mut self,
        policy: ClusterPolicy,
        key: SessionKey,
        sequence: u64,
        canonical_command: Bytes,
        at: Time,
        mutate: impl FnOnce() -> KvReply,
    ) -> KvReply {
        if sequence == 0 {
            return mutate();
        }
        self.tombstones
            .retain(|_, tombstone| tombstone.expires_at > at);
        if self.tombstones.contains_key(&key) {
            return KvReply::Error(KvError::SessionExpired);
        }
        if let Some(record) = self.records.get(&key) {
            if at.as_nanos().saturating_sub(record.last_active.as_nanos()) > policy.session_idle_ns
            {
                let max_seq = record.max_seq;
                if self.tombstones.len() as u64 >= policy.max_session_tombstones {
                    return KvReply::Error(KvError::TooLarge);
                }
                self.records.remove(&key);
                self.tombstones.insert(
                    key,
                    SessionTombstone {
                        max_seq,
                        expires_at: Time::from_nanos(
                            at.as_nanos().saturating_add(policy.session_retry_grace_ns),
                        ),
                    },
                );
                return KvReply::Error(KvError::SessionExpired);
            }
            if sequence < record.max_seq {
                return KvReply::Error(KvError::StaleSequence);
            }
            if sequence == record.max_seq {
                #[cfg(feature = "kata05")]
                let same = true;
                #[cfg(not(feature = "kata05"))]
                let same = record.canonical_command == canonical_command;
                let cached_reply = record.cached_reply.clone();
                if let Some(record) = self.records.get_mut(&key) {
                    record.last_active = at;
                }
                return if same {
                    decode_reply(&cached_reply).unwrap_or(KvReply::Error(KvError::InvalidInput))
                } else {
                    KvReply::Error(KvError::SequenceConflict)
                };
            }
        }
        let existing_bytes = self.records.get(&key).map_or(0, |record| {
            u64::try_from(
                record
                    .canonical_command
                    .len()
                    .saturating_add(record.cached_reply.len()),
            )
            .unwrap_or(u64::MAX)
        });
        if !self.records.contains_key(&key) && self.records.len() as u64 >= policy.max_sessions {
            return KvReply::Error(KvError::Busy);
        }
        let worst_case = self
            .encoded_bytes()
            .saturating_sub(existing_bytes)
            .saturating_add(u64::try_from(canonical_command.len()).unwrap_or(u64::MAX))
            .saturating_add(policy.max_reply_bytes);
        if worst_case > policy.max_session_bytes {
            return KvReply::Error(KvError::Busy);
        }
        let reply = mutate();
        let cached_reply = encode_reply(&reply);
        let bytes = u64::try_from(canonical_command.len().saturating_add(cached_reply.len()))
            .unwrap_or(u64::MAX);
        if self
            .encoded_bytes()
            .saturating_sub(existing_bytes)
            .saturating_add(bytes)
            > policy.max_session_bytes
        {
            return KvReply::Error(KvError::Busy);
        }
        self.records.insert(
            key,
            SessionRecord {
                max_seq: sequence,
                canonical_command,
                cached_reply,
                last_active: at,
            },
        );
        reply
    }

    fn preview_admin(
        &self,
        policy: ClusterPolicy,
        key: SessionKey,
        sequence: u64,
        canonical_command: &[u8],
        operation_tag: u8,
        at: Time,
    ) -> Result<Option<AdminReply>, KvError> {
        if key.namespace != SessionNamespace::AdminRequest as u8 || sequence == 0 {
            return Err(KvError::InvalidInput);
        }
        let reply = |result, detail: &[u8]| AdminReply {
            operation_tag,
            result,
            source_index: LogIndex::new(0),
            detail: detail.to_vec(),
        };
        if self
            .tombstones
            .get(&key)
            .is_some_and(|tombstone| sequence <= tombstone.max_seq && tombstone.expires_at > at)
        {
            return Ok(Some(reply(AdminResultTag::RequestExpired, b"retry-grace")));
        }
        if let Some(record) = self.records.get(&key) {
            if at.as_nanos().saturating_sub(record.last_active.as_nanos()) > policy.session_idle_ns
            {
                return Ok(Some(reply(AdminResultTag::RequestExpired, b"idle-expired")));
            }
            if sequence < record.max_seq {
                return Ok(Some(reply(AdminResultTag::Rejected, b"stale-sequence")));
            }
            if sequence == record.max_seq {
                return if same_admin_operation(&record.canonical_command, canonical_command) {
                    AdminReply::decode(&record.cached_reply)
                        .map(Some)
                        .map_err(|_| KvError::InvalidInput)
                } else {
                    Ok(Some(reply(
                        AdminResultTag::RequestConflict,
                        b"same-id-different-operation",
                    )))
                };
            }
        }
        self.reservation_for(policy, key, sequence, canonical_command)?;
        Ok(None)
    }

    fn apply_admin(
        &mut self,
        context: AdminApplyContext,
        mutate: impl FnOnce() -> Result<(AdminReply, Vec<RaftEffect>), RaftError>,
    ) -> Result<(AdminReply, Vec<RaftEffect>), RaftError> {
        let AdminApplyContext {
            policy,
            key,
            sequence,
            canonical_command,
            operation_tag,
            source_index,
            at,
        } = context;
        let fallback = |result, detail: &[u8]| AdminReply {
            operation_tag,
            result,
            source_index,
            detail: detail.to_vec(),
        };
        self.tombstones
            .retain(|_, tombstone| tombstone.expires_at > at);
        if self.tombstones.contains_key(&key) {
            return Ok((
                fallback(AdminResultTag::RequestExpired, b"retry-grace"),
                Vec::new(),
            ));
        }
        if let Some(record) = self.records.get(&key) {
            if at.as_nanos().saturating_sub(record.last_active.as_nanos()) > policy.session_idle_ns
            {
                return Ok((
                    fallback(AdminResultTag::RequestExpired, b"idle-expired"),
                    Vec::new(),
                ));
            }
            if sequence < record.max_seq {
                return Ok((
                    fallback(AdminResultTag::Rejected, b"stale-sequence"),
                    Vec::new(),
                ));
            }
            if sequence == record.max_seq {
                let reply = if same_admin_operation(&record.canonical_command, &canonical_command) {
                    AdminReply::decode(&record.cached_reply)
                        .map_err(|_| RaftError::InvalidMessage)?
                } else {
                    fallback(
                        AdminResultTag::RequestConflict,
                        b"same-id-different-operation",
                    )
                };
                if let Some(record) = self.records.get_mut(&key) {
                    record.last_active = at;
                }
                return Ok((reply, Vec::new()));
            }
        }
        let existing_bytes = self.records.get(&key).map_or(0, |record| {
            u64::try_from(
                record
                    .canonical_command
                    .len()
                    .saturating_add(record.cached_reply.len()),
            )
            .unwrap_or(u64::MAX)
        });
        if !self.records.contains_key(&key) && self.records.len() as u64 >= policy.max_sessions {
            return Err(RaftError::Busy);
        }
        let (reply, effects) = mutate()?;
        let cached_reply = reply.encode();
        let bytes = u64::try_from(canonical_command.len().saturating_add(cached_reply.len()))
            .unwrap_or(u64::MAX);
        if self
            .encoded_bytes()
            .saturating_sub(existing_bytes)
            .saturating_add(bytes)
            > policy.max_session_bytes
        {
            return Err(RaftError::Busy);
        }
        self.records.insert(
            key,
            SessionRecord {
                max_seq: sequence,
                canonical_command,
                cached_reply,
                last_active: at,
            },
        );
        Ok((reply, effects))
    }
}

fn same_admin_operation(left: &[u8], right: &[u8]) -> bool {
    if left == right {
        return true;
    }
    matches!(
        (ConfigEnvelope::decode(left), ConfigEnvelope::decode(right)),
        (Ok(left), Ok(right))
            if left.admin_session == right.admin_session && left.operation == right.operation
    )
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raft(error) => write!(f, "raft: {error}"),
            Self::Kv(error) => write!(f, "kv: {error}"),
            Self::NotLeader => write!(f, "not leader"),
            Self::Durability => write!(f, "durability barrier failed"),
            Self::UnexpectedPersistenceCompletion => {
                write!(f, "unexpected persistence completion")
            }
            Self::PersistencePending => write!(f, "persistence continuation is pending"),
            Self::FeatureDisabled => write!(f, "required cluster feature is not active"),
            Self::Environment(reason) => write!(f, "invalid environment input: {reason}"),
            Self::MalformedCommittedEntry(index) => {
                write!(f, "malformed committed entry at {index}")
            }
        }
    }
}

impl std::error::Error for NodeError {}

impl From<RaftError> for NodeError {
    fn from(error: RaftError) -> Self {
        Self::Raft(error)
    }
}

impl From<KvError> for NodeError {
    fn from(error: KvError) -> Self {
        Self::Kv(error)
    }
}

#[derive(Clone)]
pub struct Node {
    pub raft: RaftNode,
    pub kv: Kv,
    pub sessions: SessionTable,
    config: NodeConfig,
    pending_reads: Vec<PendingRead>,
    pending_follower_reads: BTreeMap<(ClientId, u64), PendingFollowerRead>,
    pending_follower_grants: BTreeMap<(NodeId, u64), PendingFollowerGrant>,
    completed_follower_reads: BTreeMap<(ClientId, u64), FollowerReadMetadata>,
    peer_capabilities: BTreeMap<NodeId, PeerCapability>,
    follower_read_round_active: bool,
    read_barrier_ready: Option<LogIndex>,
    client_routes: BTreeMap<LogIndex, (ClientId, u64)>,
    admin_routes: BTreeMap<SessionKey, (ClientId, u64)>,
    completed_file_reads: BTreeMap<(ClientId, u64), (KvCommand, Time)>,
    session_reservations: BTreeMap<LogIndex, SessionReservation>,
    logical_reservations: BTreeMap<LogIndex, u64>,
    expiry_sweep_inflight: bool,
    session_expiry_sweep_inflight: bool,
    metrics: CoreMetrics,
    continuation: Option<NodeContinuation>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CoreMetrics {
    expiry_proposals: u64,
    expiry_keys: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CoreMetricsSnapshot {
    pub expiry_proposals: u64,
    pub expiry_keys: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SessionReservation {
    bytes: u64,
    new_session: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CoreResourceUsage {
    pub log_bytes: u64,
    pub snapshot_staging_bytes: u64,
    pub session_bytes: u64,
    pub session_tombstone_bytes: u64,
    pub pending_read_bytes: u64,
    pub pending_client_route_bytes: u64,
    pub memtable_bytes: u64,
    pub sst_metadata_bytes: u64,
}

#[derive(Clone)]
enum NodeContinuation {
    Raft(Vec<RaftEffect>),
    Store(Box<StoreApplyContinuation>),
}

#[derive(Clone)]
struct PreparedCommittedEntry {
    kv: Kv,
    sessions: SessionTable,
    raft: RaftNode,
    wal_frame: Bytes,
    reply: Option<PreparedReply>,
    finished_admin_session: Option<SessionKey>,
    post_effects: Vec<RaftEffect>,
    finishes_expiry_sweep: bool,
    finishes_session_expiry_sweep: bool,
    expired_keys: u64,
}

#[derive(Clone)]
enum PreparedReply {
    Kv(ClientId, u64, KvReply),
    Admin(ClientId, u64, AdminReply),
}

#[derive(Clone)]
struct StoreApplyContinuation {
    current: PreparedCommittedEntry,
    pending: VecDeque<PreparedCommittedEntry>,
    deferred: Vec<RaftEffect>,
    remaining: Vec<RaftEffect>,
}

#[derive(Clone)]
struct PendingRead {
    client: ClientId,
    sequence: u64,
    command: KvCommand,
    at: Time,
    index: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerCapability {
    semantic_version: u16,
    features: u64,
}

#[derive(Clone, Debug)]
struct PendingFollowerRead {
    command: KvCommand,
    request_id: u64,
    command_hash: u64,
    leader: NodeId,
    term: Term,
    grant: Option<(LogIndex, Time)>,
}

#[derive(Clone, Copy, Debug)]
struct PendingFollowerGrant {
    follower: NodeId,
    request_id: u64,
    command_hash: u64,
    request_time: Time,
    term: Term,
}

/// Volatile route metadata for a completed semantic-v3 follower read. The
/// client adapter consumes this together with the canonical CCKR reply; none
/// of these route ids or timestamps enter the replicated log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FollowerReadMetadata {
    pub read_index: LogIndex,
    pub applied_index: LogIndex,
    pub applied_term: Term,
    pub read_time: Time,
}

impl Node {
    pub fn new(mut config: NodeConfig, voters: BTreeSet<NodeId>) -> Result<Self, NodeError> {
        if config.cluster_id.iter().all(|byte| *byte == 0) {
            return Err(NodeError::Environment("zero cluster id"));
        }
        config.store.max_key_bytes = usize::try_from(config.policy.max_key_bytes)
            .map_err(|_| NodeError::Kv(KvError::TooLarge))?;
        config.store.max_value_bytes = usize::try_from(config.policy.max_value_bytes)
            .map_err(|_| NodeError::Kv(KvError::TooLarge))?;
        let mut raft_config = config.raft;
        raft_config.leader_transfer_timeout =
            cc_core::Duration::from_nanos(config.policy.leader_transfer_timeout_ns);
        Ok(Self {
            raft: RaftNode::new(config.id, voters, config.seed, raft_config),
            kv: Kv::new(config.store)?,
            sessions: SessionTable::default(),
            config,
            pending_reads: Vec::new(),
            pending_follower_reads: BTreeMap::new(),
            pending_follower_grants: BTreeMap::new(),
            completed_follower_reads: BTreeMap::new(),
            peer_capabilities: BTreeMap::new(),
            follower_read_round_active: false,
            read_barrier_ready: None,
            client_routes: BTreeMap::new(),
            admin_routes: BTreeMap::new(),
            completed_file_reads: BTreeMap::new(),
            session_reservations: BTreeMap::new(),
            logical_reservations: BTreeMap::new(),
            expiry_sweep_inflight: false,
            session_expiry_sweep_inflight: false,
            metrics: CoreMetrics::default(),
            continuation: None,
        })
    }

    pub fn fresh(config: NodeConfig, bootstrap: MembershipState) -> Result<Self, NodeError> {
        if !config.host_limits.is_valid() {
            return Err(NodeError::Environment("invalid host limits"));
        }
        bootstrap
            .validate()
            .map_err(|_| NodeError::MalformedCommittedEntry(LogIndex::new(0)))?;
        if bootstrap.voters.len() as u32 > config.policy.max_members {
            return Err(NodeError::Kv(KvError::TooLarge));
        }
        let mut node = Self::new(config, bootstrap.voters.clone())?;
        node.raft
            .restore_membership_state(bootstrap)
            .map_err(NodeError::Raft)?;
        Ok(node)
    }

    pub fn restore(config: NodeConfig, recovered: RecoveredNode) -> Result<Self, NodeError> {
        if !config.host_limits.is_valid() {
            return Err(NodeError::Environment("invalid host limits"));
        }
        if config.policy.encode() != recovered.cluster_policy.encode() {
            return Err(NodeError::Kv(KvError::InvalidInput));
        }
        recovered
            .membership
            .validate()
            .map_err(|_| NodeError::MalformedCommittedEntry(recovered.log_base.0))?;
        // Before N3's store-WAL watermark exists, the only durable proof of
        // a state-machine application is the installed snapshot itself.  A
        // log append is not a commit proof, so accepting an arbitrary later
        // `durable_applied` would serve a KV/session image that was never
        // recovered.  Keep the exact pair check (including term) rather than
        // comparing indexes alone.
        if recovered.durable_applied != recovered.log_base {
            return Err(NodeError::MalformedCommittedEntry(
                recovered.durable_applied.0,
            ));
        }
        if recovered.snapshot.is_none() && recovered.log_base != (LogIndex::new(0), Term::new(0)) {
            return Err(NodeError::MalformedCommittedEntry(recovered.log_base.0));
        }
        let mut node = Self::new(config, recovered.membership.voters.clone())?;
        node.raft.hard_state = recovered.hard_state;
        node.raft
            .install_snapshot_state(recovered.log_base.0, recovered.log_base.1);
        node.raft.log = recovered.entries;
        node.raft.commit_index = recovered.durable_applied.0;
        node.raft.applied_index = recovered.durable_applied.0;
        node.raft
            .restore_membership_state(recovered.membership)
            .map_err(NodeError::Raft)?;
        if let Some(snapshot) = recovered.snapshot {
            if snapshot.cluster_policy.encode() != node.config.policy.encode()
                || snapshot.last_included_index != recovered.log_base.0
                || snapshot.last_included_term != recovered.log_base.1
                || snapshot.membership != node.raft.membership_state()
            {
                return Err(NodeError::MalformedCommittedEntry(recovered.log_base.0));
            }
            node.kv = Kv::restore(snapshot.kv, node.config.store)?;
            node.sessions = snapshot.sessions;
            node.raft
                .restore_leadership_transfer(snapshot.leadership_transfer)
                .map_err(NodeError::Raft)?;
        }
        node.raft.replay_retained_membership_suffix();
        Ok(node)
    }

    /// Rebuild a node from durable state alone, as a restarting process does.
    ///
    /// Only the hard state and the recovered log survive. Role, commit index,
    /// applied index, the state machine, and session table all start empty and
    /// are re-derived by replaying the log once a leader re-establishes the
    /// commit index. A caller that hands back volatile state here is modelling
    /// a pause, not a crash.
    pub fn recover(
        config: NodeConfig,
        voters: BTreeSet<NodeId>,
        hard_state: HardState,
        log: Vec<Entry>,
        now: Time,
    ) -> Result<Self, NodeError> {
        let mut node = Self::new(config, voters)?;
        node.raft.hard_state = hard_state;
        node.raft.log = log;
        node.raft.rearm_election(now);
        Ok(node)
    }

    /// Reconstruct committed KV/session state from a verified store-WAL
    /// prefix and the independently recovered Raft suffix. Each durable CCSW
    /// record is recomputed from the canonical log entry and compared byte-
    /// semantically before its tentative state is published.
    pub fn recover_durable_applies(
        &mut self,
        recovered: &cc_store::RecoveredStoreWal,
    ) -> Result<(), NodeError> {
        if self.continuation.is_some() {
            return Err(NodeError::PersistencePending);
        }
        for durable in &recovered.records {
            if durable.watermark.index <= self.kv.applied_index {
                if durable.watermark.index == self.kv.applied_index
                    && (durable.watermark.term != self.kv.applied_term
                        || durable.watermark.last_leader_time != self.kv.last_leader_time())
                {
                    return Err(NodeError::MalformedCommittedEntry(durable.watermark.index));
                }
                continue;
            }
            let expected = self
                .kv
                .applied_index
                .get()
                .checked_add(1)
                .ok_or(NodeError::MalformedCommittedEntry(durable.watermark.index))?;
            if durable.watermark.index.get() != expected {
                return Err(NodeError::MalformedCommittedEntry(durable.watermark.index));
            }
            let entry = self
                .raft
                .log
                .iter()
                .find(|entry| entry.index == durable.watermark.index)
                .cloned()
                .ok_or(NodeError::MalformedCommittedEntry(durable.watermark.index))?;
            if entry.term != durable.watermark.term {
                return Err(NodeError::MalformedCommittedEntry(entry.index));
            }
            self.raft.commit_index = entry.index;
            self.raft.applied_index = entry.index;
            let mut prepared = self.prepare_committed_entries(vec![entry.clone()])?;
            let current = prepared
                .pop_front()
                .ok_or(NodeError::MalformedCommittedEntry(entry.index))?;
            let planned = cc_store::recover_store_wal(&current.wal_frame)
                .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?;
            if planned.records.len() != 1 || &planned.records[0] != durable {
                return Err(NodeError::MalformedCommittedEntry(entry.index));
            }
            self.kv = current.kv;
            self.sessions = current.sessions;
            self.raft = current.raft;
        }
        Ok(())
    }

    #[must_use]
    pub fn id(&self) -> NodeId {
        self.config.id
    }

    #[must_use]
    pub fn role(&self) -> Role {
        self.raft.role
    }

    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.raft.leader_id
    }

    /// Register the semantic version and feature intersection selected by a
    /// fresh CCHL connection. The host must call this before delivering a
    /// v3-only message. A later v2 connection replaces the old capability,
    /// so a stale observation can never authorize a follower read.
    pub fn observe_peer_capability(
        &mut self,
        peer: NodeId,
        semantic_version: u16,
        features: u64,
    ) -> Result<(), NodeError> {
        if peer.get() == 0
            || peer == self.id()
            || !cc_raft::supports_protocol_version(semantic_version)
        {
            return Err(NodeError::Environment("peer capability"));
        }
        self.peer_capabilities.insert(
            peer,
            PeerCapability {
                semantic_version,
                features,
            },
        );
        Ok(())
    }

    /// Invalidate the proof attached to one negotiated connection
    /// generation. Capability is volatile evidence: a closed connection must
    /// never continue authorizing follower reads or feature activation.
    pub fn forget_peer_capability(&mut self, peer: NodeId) {
        self.peer_capabilities.remove(&peer);
        self.pending_follower_reads
            .retain(|_, pending| pending.leader != peer);
        self.pending_follower_grants
            .retain(|_, pending| pending.follower != peer);
        if self.pending_follower_grants.is_empty() {
            self.follower_read_round_active = false;
        }
    }

    /// Bounded point-in-time copy for host metrics. Membership policy bounds
    /// the map, and node ids are the only labels exposed by the adapter.
    #[must_use]
    pub fn peer_capabilities(&self) -> Vec<(NodeId, u16, u64)> {
        self.peer_capabilities
            .iter()
            .map(|(id, capability)| (*id, capability.semantic_version, capability.features))
            .collect()
    }

    #[must_use]
    pub const fn metrics(&self) -> CoreMetricsSnapshot {
        CoreMetricsSnapshot {
            expiry_proposals: self.metrics.expiry_proposals,
            expiry_keys: self.metrics.expiry_keys,
        }
    }

    /// Start a bounded semantic-v3 follower read. Completion arrives on the
    /// ordinary volatile client route, and its tagged metadata is retrieved
    /// with [`Self::take_follower_read_metadata`].
    pub fn request_follower_read(
        &mut self,
        client: ClientId,
        sequence: u64,
        command: KvCommand,
        _at: Time,
    ) -> Result<Vec<NodeEffect>, NodeError> {
        if !matches!(command, KvCommand::Get { .. } | KvCommand::Ttl { .. }) {
            return Err(NodeError::Kv(KvError::InvalidInput));
        }
        if sequence == 0
            || self.raft.role == Role::Leader
            || (!self.raft.voters.contains(&self.id()) && !self.raft.learners.contains(&self.id()))
            || self.pending_follower_reads.len() >= self.config.host_limits.max_pending_client
        {
            return Err(NodeError::Environment("follower read unavailable"));
        }
        let leader = self
            .raft
            .leader_id
            .ok_or(NodeError::Environment("follower read leader unknown"))?;
        let capability = self
            .peer_capabilities
            .get(&leader)
            .copied()
            .ok_or(NodeError::Environment("follower read capability unknown"))?;
        if capability.semantic_version != SEMANTIC_VERSION_V3
            || capability.features & FOLLOWER_READ_FEATURE == 0
        {
            return Err(NodeError::Environment("follower read feature unavailable"));
        }
        let canonical = encode_command(&command);
        let command_hash = cc_core::fnv1a(&canonical);
        let route = (client, sequence);
        if self.pending_follower_reads.contains_key(&route) {
            return Err(NodeError::Environment("duplicate follower read route"));
        }
        self.pending_follower_reads.insert(
            route,
            PendingFollowerRead {
                command,
                request_id: sequence,
                command_hash,
                leader,
                term: self.raft.hard_state.term,
                grant: None,
            },
        );
        Ok(vec![NodeEffect::Send(Message {
            proto_version: SEMANTIC_VERSION_V3,
            from: self.id(),
            to: leader,
            term: self.raft.hard_state.term,
            kind: MessageKind::FollowerReadRequest {
                request_id: sequence,
                command_hash,
            },
        })])
    }

    pub fn take_follower_read_metadata(
        &mut self,
        client: ClientId,
        sequence: u64,
    ) -> Option<FollowerReadMetadata> {
        self.completed_follower_reads.remove(&(client, sequence))
    }

    pub fn add_learner(&mut self, node: NodeId) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.add_learner(node)?;
        self.map_effects(effects, None)
    }

    pub fn promote_learner(&mut self, node: NodeId) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.promote_learner(node)?;
        self.map_effects(effects, None)
    }

    pub fn replicate_peer(&self, node: NodeId) -> Result<Vec<NodeEffect>, NodeError> {
        self.raft
            .replicate_peer(node)?
            .into_iter()
            .map(|effect| match effect {
                RaftEffect::Send(message) => Ok(NodeEffect::Send(message)),
                _ => Err(NodeError::Environment("replicate peer effect")),
            })
            .collect()
    }

    pub fn enter_joint(&mut self, voters: BTreeSet<NodeId>) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.enter_joint(voters)?;
        self.map_effects(effects, None)
    }

    pub fn leave_joint(&mut self) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.leave_joint()?;
        self.map_effects(effects, None)
    }

    /// Submit one durable, idempotent membership/feature workflow. The
    /// caller-owned AdminRequest identity is replicated in CCCF; the volatile
    /// `(client, route)` pair exists only to deliver this attempt's reply.
    pub fn admin_request(
        &mut self,
        now: Time,
        client: ClientId,
        route: u64,
        session: SessionKey,
        sequence: u64,
        operation: ConfigOperation,
    ) -> Result<Vec<NodeEffect>, NodeError> {
        if self.raft.role != Role::Leader {
            return Err(NodeError::NotLeader);
        }
        if session.namespace != SessionNamespace::AdminRequest as u8 || sequence == 0 {
            return Err(NodeError::Environment("invalid admin request identity"));
        }
        if !self.admin_routes.contains_key(&session)
            && self.admin_routes.len() as u64 >= self.config.host_limits.max_pending_client_routes
        {
            return Err(NodeError::Kv(KvError::Busy));
        }
        let operation_tag = operation.tag();
        let envelope = ConfigEnvelope {
            admin_session: Some((session, sequence)),
            leader_time: now,
            operation,
        };
        let canonical = envelope.encode();

        if let Some(reply) = self.sessions.preview_admin(
            self.config.policy,
            session,
            sequence,
            &canonical,
            operation_tag,
            now,
        )? {
            return Ok(vec![NodeEffect::AdminReply {
                client,
                sequence: route,
                reply,
            }]);
        }

        if let Some(pending) = self
            .raft
            .log
            .iter()
            .filter(|entry| entry.kind.is_config())
            .filter_map(|entry| {
                ConfigEnvelope::decode(&entry.payload)
                    .ok()
                    .map(|value| (entry, value))
            })
            .find(|(_, value)| value.admin_session == Some((session, sequence)))
        {
            let same = same_admin_operation(&pending.0.payload, &canonical);
            let reply = AdminReply {
                operation_tag,
                result: if same {
                    AdminResultTag::InProgress
                } else {
                    AdminResultTag::RequestConflict
                },
                source_index: pending.0.index,
                detail: if same {
                    b"workflow-in-progress".to_vec()
                } else {
                    b"same-id-different-operation".to_vec()
                },
            };
            return Ok(vec![NodeEffect::AdminReply {
                client,
                sequence: route,
                reply,
            }]);
        }

        if let Some(transfer) = self.raft.leadership_transfer_state()
            && transfer.admin_session == Some((session, sequence))
        {
            let same = matches!(
                envelope.operation,
                ConfigOperation::BeginLeaderTransfer { target } if target == transfer.target
            );
            return Ok(vec![NodeEffect::AdminReply {
                client,
                sequence: route,
                reply: AdminReply {
                    operation_tag,
                    result: if same {
                        AdminResultTag::InProgress
                    } else {
                        AdminResultTag::RequestConflict
                    },
                    source_index: transfer.intent_index,
                    detail: if same {
                        b"workflow-in-progress".to_vec()
                    } else {
                        b"same-id-different-operation".to_vec()
                    },
                },
            }]);
        }

        match &envelope.operation {
            ConfigOperation::AddLearner { id, .. } => {
                if self
                    .raft
                    .voters
                    .len()
                    .saturating_add(self.raft.learners.len())
                    >= self.config.policy.max_members as usize
                {
                    return Err(NodeError::Kv(KvError::Busy));
                }
                if self.raft.active_features() & cc_core::ATOMIC_BATCH_FEATURE != 0
                    && !self.peer_capabilities.get(id).is_some_and(|capability| {
                        capability.semantic_version == SEMANTIC_VERSION_V3
                            && capability.features & cc_env::FEATURE_ATOMIC_BATCH != 0
                    })
                {
                    return Err(NodeError::FeatureDisabled);
                }
            }
            ConfigOperation::EnterJoint { new_voters } => {
                const MIN_VOTERS: usize = 3;
                if new_voters.len() < MIN_VOTERS
                    || (!new_voters.contains(&self.id()) && self.raft.role == Role::Leader)
                {
                    return Err(NodeError::Environment(
                        "membership preflight would lose quorum",
                    ));
                }
            }
            ConfigOperation::ActivateFeature { feature }
                if *feature == cc_core::ATOMIC_BATCH_FEATURE =>
            {
                let all_capable = self
                    .raft
                    .voters
                    .iter()
                    .chain(self.raft.learners.iter())
                    .filter(|peer| **peer != self.id())
                    .all(|peer| {
                        self.peer_capabilities.get(peer).is_some_and(|capability| {
                            capability.semantic_version == SEMANTIC_VERSION_V3
                                && capability.features & cc_env::FEATURE_ATOMIC_BATCH != 0
                        })
                    });
                if !all_capable {
                    return Err(NodeError::FeatureDisabled);
                }
            }
            _ => {}
        }

        let effects = self.raft.propose_admin_config(envelope)?;
        self.admin_routes.insert(session, (client, route));
        self.map_effects(effects, None)
    }

    #[must_use]
    pub fn membership(&self) -> (BTreeSet<NodeId>, BTreeSet<NodeId>, bool) {
        (
            self.raft.voters.clone(),
            self.raft.learners.clone(),
            self.raft.joint_active(),
        )
    }

    #[must_use]
    pub const fn active_features(&self) -> u64 {
        self.raft.active_features()
    }

    #[must_use]
    pub fn resource_usage(&self) -> CoreResourceUsage {
        let log_bytes = self.raft.log.iter().fold(0_u64, |total, entry| {
            total.saturating_add(
                u64::try_from(entry.payload.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(25),
            )
        });
        let pending_read_bytes = self
            .pending_reads
            .iter()
            .map(|read| encode_command(&read.command).len())
            .chain(
                self.pending_follower_reads
                    .values()
                    .map(|read| encode_command(&read.command).len()),
            )
            .fold(0_u64, |total, bytes| {
                total.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX).saturating_add(40))
            });
        let (memtable_bytes, sst_metadata_bytes) = self.kv.store.memory_footprint();
        CoreResourceUsage {
            log_bytes,
            snapshot_staging_bytes: 0,
            session_bytes: self.sessions.encoded_bytes(),
            session_tombstone_bytes: u64::try_from(self.sessions.tombstones.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(33),
            pending_read_bytes,
            pending_client_route_bytes: u64::try_from(self.client_routes.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(self.admin_routes.len()).unwrap_or(u64::MAX))
                .saturating_mul(32),
            memtable_bytes,
            sst_metadata_bytes,
        }
    }

    #[must_use]
    pub const fn cluster_policy(&self) -> ClusterPolicy {
        self.config.policy
    }

    /// Activate the atomic-batch semantic only after every current member has
    /// a fresh v3 CCHL observation advertising the feature. The resulting
    /// Config entry still has to commit before [`Self::active_features`] and
    /// client admission change.
    pub fn activate_atomic_batch(&mut self, now: Time) -> Result<Vec<NodeEffect>, NodeError> {
        let feature = cc_core::ATOMIC_BATCH_FEATURE;
        let all_capable = self
            .raft
            .voters
            .iter()
            .chain(self.raft.learners.iter())
            .filter(|peer| **peer != self.id())
            .all(|peer| {
                self.peer_capabilities.get(peer).is_some_and(|capability| {
                    capability.semantic_version == SEMANTIC_VERSION_V3
                        && capability.features & cc_env::FEATURE_ATOMIC_BATCH != 0
                })
            });
        if !all_capable {
            return Err(NodeError::FeatureDisabled);
        }
        let effects = self.raft.activate_feature(feature, now)?;
        self.map_effects(effects, None)
    }

    pub fn on_input(&mut self, input: NodeInput) -> Result<Vec<NodeEffect>, NodeError> {
        if self.continuation.is_some() && !matches!(input, NodeInput::Persisted { .. }) {
            return Err(NodeError::PersistencePending);
        }
        let output = match input {
            NodeInput::Persisted { success } => {
                let Some(continuation) = self.continuation.take() else {
                    return Err(NodeError::UnexpectedPersistenceCompletion);
                };
                if !success {
                    return Err(NodeError::Durability);
                }
                match continuation {
                    NodeContinuation::Raft(effects) => self.map_effects(effects, None),
                    NodeContinuation::Store(mut state) => {
                        self.session_reservations
                            .remove(&state.current.raft.applied_index);
                        self.logical_reservations
                            .remove(&state.current.raft.applied_index);
                        if state.current.finishes_expiry_sweep {
                            self.expiry_sweep_inflight = false;
                            self.metrics.expiry_keys = self
                                .metrics
                                .expiry_keys
                                .saturating_add(state.current.expired_keys);
                        }
                        if state.current.finishes_session_expiry_sweep {
                            self.session_expiry_sweep_inflight = false;
                        }
                        self.kv = state.current.kv;
                        self.sessions = state.current.sessions;
                        self.raft = state.current.raft;
                        if let Some(session) = state.current.finished_admin_session {
                            self.admin_routes.remove(&session);
                        }
                        let mut output = state
                            .current
                            .reply
                            .map(|reply| match reply {
                                PreparedReply::Kv(client, sequence, reply) => {
                                    NodeEffect::ClientReply {
                                        client,
                                        sequence,
                                        reply,
                                    }
                                }
                                PreparedReply::Admin(client, sequence, reply) => {
                                    NodeEffect::AdminReply {
                                        client,
                                        sequence,
                                        reply,
                                    }
                                }
                            })
                            .into_iter()
                            .collect::<Vec<_>>();
                        state.deferred.extend(state.current.post_effects);
                        if let Some(next) = state.pending.pop_front() {
                            let bytes = next.wal_frame.clone();
                            state.current = next;
                            self.continuation = Some(NodeContinuation::Store(state));
                            output.push(NodeEffect::PersistStore { bytes });
                        } else {
                            state.deferred.extend(state.remaining);
                            output.extend(self.map_effects(state.deferred, None)?);
                        }
                        Ok(output)
                    }
                }
            }
            NodeInput::Tick { now } => {
                let effects = self.raft.tick(now);
                let mut output = self.map_effects(effects, None)?;
                // These flags are a cache, never authority. Rebuild them from
                // the durable log on every scheduling point so restart,
                // step-down, and suffix truncation cannot either duplicate a
                // sweep or suppress all future reclamation.
                self.expiry_sweep_inflight = self.pending_internal_sweep(false);
                self.session_expiry_sweep_inflight = self.pending_internal_sweep(true);
                if self.continuation.is_none()
                    && self.raft.role == Role::Leader
                    && !self.expiry_sweep_inflight
                    && self
                        .kv
                        .first_deadline()
                        .is_some_and(|(deadline, _)| deadline <= now)
                {
                    let command = encode_command(&KvCommand::PurgeExpired { up_to: now });
                    let effects = self.raft.propose(
                        AppEnvelope {
                            session: None,
                            leader_time: now,
                            command,
                        }
                        .encode(),
                    )?;
                    self.expiry_sweep_inflight = true;
                    self.metrics.expiry_proposals = self.metrics.expiry_proposals.saturating_add(1);
                    output.extend(self.map_effects(effects, None)?);
                } else if self.continuation.is_none()
                    && self.raft.role == Role::Leader
                    && !self.session_expiry_sweep_inflight
                    && self
                        .sessions
                        .first_expiry(self.config.policy, self.pinned_admin_session())
                        .is_some_and(|(deadline, _)| deadline <= now)
                {
                    let command = encode_command(&KvCommand::ExpireSessions { up_to: now });
                    let effects = self.raft.propose(
                        AppEnvelope {
                            session: None,
                            leader_time: now,
                            command,
                        }
                        .encode(),
                    )?;
                    self.session_expiry_sweep_inflight = true;
                    output.extend(self.map_effects(effects, None)?);
                }
                Ok(output)
            }
            NodeInput::Message(message) => {
                let effects = self.raft.on_message(message);
                self.map_effects(effects, None)
            }
            NodeInput::MessageAt { now, message } => {
                let effects = self.raft.on_message_at(message, now);
                self.map_effects(effects, None)
            }
            NodeInput::Timer { now, kind } => {
                let effects = self.raft.on_timer(now, kind);
                self.map_effects(effects, None)
            }
            NodeInput::ClientRequest {
                client,
                sequence,
                command,
                leader_time,
            } => self.submit_client(
                client,
                sequence,
                Some((
                    SessionKey::new(SessionNamespace::UserRequest as u8, client)
                        .map_err(|_| NodeError::Kv(KvError::InvalidInput))?,
                    sequence,
                )),
                encode_command(&command),
                leader_time,
            ),
            NodeInput::ClientBytes {
                route_client,
                route_req,
                session,
                command,
                leader_time,
            } => self.submit_client(route_client, route_req, session, command, leader_time),
            NodeInput::Read {
                client,
                sequence,
                command,
                at,
            } => {
                let read_bytes = u64::try_from(encode_command(&command).len()).unwrap_or(u64::MAX);
                let pending_bytes = self.pending_reads.iter().fold(0_u64, |total, pending| {
                    total.saturating_add(
                        u64::try_from(encode_command(&pending.command).len()).unwrap_or(u64::MAX),
                    )
                });
                if self.pending_reads.len() as u64 >= self.config.host_limits.max_pending_reads
                    || pending_bytes.saturating_add(read_bytes)
                        > self.config.host_limits.max_pending_read_bytes
                {
                    return Err(NodeError::Kv(KvError::Busy));
                }
                let effects = self.raft.request_read()?;
                self.read_barrier_ready = None;
                self.pending_reads.push(PendingRead {
                    client,
                    sequence,
                    command,
                    at,
                    index: self.raft.commit_index,
                });
                self.map_effects(effects, None)
            }
        }?;
        if self.raft.role != Role::Leader {
            // Route ids name live host connections.  They are never safe to
            // retain through a leadership change, even if an old log entry is
            // later applied by this follower.
            self.client_routes.clear();
            self.session_reservations.clear();
            self.logical_reservations.clear();
        }
        Ok(output)
    }

    /// Value-boundary entry point used by new hosts.  It deliberately takes
    /// an explicit timestamp and block-read seam so neither the core nor a
    /// simulator-only global clock can affect deterministic state. File-
    /// backed replies are recomputed through `blocks` after their ReadIndex
    /// barrier and carry this input's exact accumulated service duration.
    pub fn on_env_input(
        &mut self,
        now: Time,
        input: cc_env::Input,
        blocks: &mut dyn cc_store::BlockSource,
    ) -> NodeStep {
        let outcome = match input {
            cc_env::Input::Recv { from, msg } => {
                if !cc_raft::supports_protocol_version(msg.proto_version) {
                    Err(NodeError::Environment("peer semantic version"))
                } else {
                    match cc_raft::codec::decode(&msg.payload) {
                        Err(_) => Err(NodeError::Environment("peer CCRP")),
                        Ok(message)
                            if message.proto_version != msg.proto_version
                                || message.from != from
                                || message.to != self.id() =>
                        {
                            Err(NodeError::Environment("peer CCRP identity"))
                        }
                        Ok(message) => self.on_input(NodeInput::MessageAt { now, message }),
                    }
                }
            }
            cc_env::Input::ClientRequest {
                client,
                req,
                session,
                command,
            } => match decode_command(&command) {
                Ok(parsed) if session.is_some() && !parsed.is_write() => {
                    Err(NodeError::Kv(KvError::InvalidInput))
                }
                Ok(parsed)
                    if session.is_none()
                        && matches!(
                            parsed,
                            KvCommand::Get { .. } | KvCommand::Ttl { .. } | KvCommand::Scan { .. }
                        ) =>
                {
                    self.on_input(NodeInput::Read {
                        client,
                        sequence: req.get(),
                        command: parsed,
                        at: now,
                    })
                }
                // Keep follower fencing ahead of leader-side write decoding.
                // A malformed write is never a follower-local validation
                // result, while a leader still validates it in `submit_client`.
                Ok(_) | Err(_) => {
                    let session = match session {
                        None => Ok(None),
                        Some((session_client, sequence)) => {
                            SessionKey::new(SessionNamespace::UserRequest as u8, session_client)
                                .map(|key| Some((key, sequence.get())))
                                .map_err(|_| NodeError::Kv(KvError::InvalidInput))
                        }
                    };
                    session.and_then(|session| {
                        self.on_input(NodeInput::ClientBytes {
                            route_client: client,
                            route_req: req.get(),
                            session,
                            command,
                            leader_time: now,
                        })
                    })
                }
            },
            cc_env::Input::Tick => self.on_input(NodeInput::Tick { now }),
            // The Driver owns timer generations and logical I/O ids.  Passing
            // either completion directly to a node would make a stale host
            // event look like a valid Raft transition, so reject it here.
            cc_env::Input::TimerFired { .. } => Err(NodeError::Environment("unrouted timer")),
            cc_env::Input::IoDone { .. } => Err(NodeError::Environment("unrouted I/O completion")),
        };
        let mut synchronous_service = Duration::from_nanos(0);
        let outcome = outcome.and_then(|mut effects| {
            for effect in &mut effects {
                let NodeEffect::ReadReply {
                    client,
                    sequence,
                    reply,
                } = effect
                else {
                    continue;
                };
                let Some((command, at)) = self.completed_file_reads.remove(&(*client, *sequence))
                else {
                    continue;
                };
                let read = self.kv.read_with_source(command, at, blocks);
                synchronous_service = Duration::from_nanos(
                    synchronous_service
                        .as_nanos()
                        .saturating_add(read.service.as_nanos()),
                );
                *reply = read.outcome.map_err(NodeError::Kv)?;
            }
            Ok(effects)
        });
        NodeStep {
            synchronous_service,
            outcome,
        }
    }

    fn submit_client(
        &mut self,
        route_client: ClientId,
        route_req: u64,
        session: Option<(SessionKey, u64)>,
        command: Bytes,
        leader_time: Time,
    ) -> Result<Vec<NodeEffect>, NodeError> {
        if self.raft.role != Role::Leader {
            return Err(NodeError::NotLeader);
        }
        if command.len() as u64 > self.config.policy.max_command_bytes {
            return Err(NodeError::Kv(KvError::TooLarge));
        }
        let parsed = decode_command(&command)?;
        self.require_active_feature(&parsed)?;
        validate_command_policy(&parsed, self.config.policy)?;
        if let Some((key, sequence)) = session
            && (key.namespace != SessionNamespace::UserRequest as u8 || sequence == 0)
        {
            return Err(NodeError::Kv(KvError::InvalidInput));
        }
        if self.client_routes.len() as u64 >= self.config.host_limits.max_pending_client_routes {
            return Err(NodeError::Kv(KvError::Busy));
        }
        let uncommitted = self
            .raft
            .log
            .iter()
            .filter(|entry| entry.index > self.raft.commit_index)
            .collect::<Vec<_>>();
        let uncommitted_bytes = uncommitted.iter().fold(0_u64, |total, entry| {
            total.saturating_add(u64::try_from(entry.payload.len() + 25).unwrap_or(u64::MAX))
        });
        let proposed_bytes = u64::try_from(command.len() + 48).unwrap_or(u64::MAX);
        if uncommitted.len() as u64 >= self.config.host_limits.max_uncommitted_entries
            || uncommitted_bytes.saturating_add(proposed_bytes)
                > self.config.host_limits.max_uncommitted_bytes
        {
            return Err(NodeError::Kv(KvError::Busy));
        }
        let reservation = session
            .map(|(key, sequence)| {
                self.sessions
                    .reservation_for(self.config.policy, key, sequence, &command)
            })
            .transpose()?
            .unwrap_or_default();
        let reserved_bytes = self
            .session_reservations
            .values()
            .fold(0_u64, |total, reservation| {
                total.saturating_add(reservation.bytes)
            });
        let reserved_sessions = self
            .session_reservations
            .values()
            .filter(|reservation| reservation.new_session)
            .count() as u64;
        if self
            .sessions
            .encoded_bytes()
            .saturating_add(reserved_bytes)
            .saturating_add(reservation.bytes)
            > self.config.policy.max_session_bytes
            || (reservation.new_session
                && self.sessions.records.len() as u64 + reserved_sessions
                    >= self.config.policy.max_sessions)
        {
            return Err(NodeError::Kv(KvError::Busy));
        }
        let current_charge = self.logical_state_charge().unwrap_or(0);
        let projected_charge =
            self.projected_logical_state_charge(&parsed, session, &command, leader_time)?;
        let positive_delta = projected_charge.saturating_sub(current_charge);
        let reserved_logical = self
            .logical_reservations
            .values()
            .fold(0_u64, |total, bytes| total.saturating_add(*bytes));
        if current_charge
            .saturating_add(reserved_logical)
            .saturating_add(positive_delta)
            > self.config.policy.max_live_logical_bytes
        {
            return Err(NodeError::Kv(KvError::Busy));
        }
        let effects = self.raft.propose(
            AppEnvelope {
                session,
                leader_time,
                command,
            }
            .encode(),
        )?;
        let index = self.raft.last_index();
        self.client_routes.insert(index, (route_client, route_req));
        if reservation != SessionReservation::default() {
            self.session_reservations.insert(index, reservation);
        }
        if positive_delta != 0 {
            self.logical_reservations.insert(index, positive_delta);
        }
        self.map_effects(effects, None)
    }

    fn projected_logical_state_charge(
        &self,
        command: &KvCommand,
        session: Option<(SessionKey, u64)>,
        canonical_command: &[u8],
        leader_time: Time,
    ) -> Result<u64, NodeError> {
        let policy = self.config.policy;
        let mut kv = self.kv.clone();
        let mut sessions = self.sessions.clone();
        let index = LogIndex::new(self.raft.last_index().get().saturating_add(1));
        let term = self.raft.hard_state.term;
        let mut apply = || {
            kv.apply_command_only_with_batch_limits(
                index,
                term,
                command.clone(),
                leader_time,
                BatchLimits {
                    max_commands: policy.max_batch_commands,
                    max_bytes: policy.max_batch_bytes,
                    max_reply_bytes: policy.max_batch_reply_bytes,
                    max_expiry_items: policy.max_keys_per_expiry_sweep,
                },
            )
        };
        match session {
            Some((key, sequence)) => {
                sessions.apply_user(
                    policy,
                    key,
                    sequence,
                    canonical_command.to_vec(),
                    leader_time,
                    apply,
                );
            }
            None => {
                apply();
            }
        }
        if kv.applied_index < index {
            kv.mark_applied(index, term, leader_time);
        }
        CcsnStreamEncoder::new(CcsnSnapshot {
            cluster_id: self.config.cluster_id,
            cluster_policy: policy,
            membership: self.raft.membership_state_at(self.kv.applied_index),
            kv: kv.logical_snapshot(kv.last_leader_time()),
            sessions,
            leadership_transfer: self.raft.leadership_transfer_state(),
        })
        .map(|encoder| encoder.total_len())
        .map_err(|_| NodeError::Kv(KvError::TooLarge))
    }

    fn pinned_admin_session(&self) -> Option<SessionKey> {
        self.raft
            .log
            .iter()
            .filter(|entry| {
                entry.index > self.raft.applied_index && entry.kind.is_config()
            })
            .filter_map(|entry| ConfigEnvelope::decode(&entry.payload).ok())
            .find_map(|envelope| envelope.admin_session.map(|(key, _)| key))
    }

    fn pending_internal_sweep(&self, sessions: bool) -> bool {
        self.raft
            .log
            .iter()
            .filter(|entry| {
                entry.index > self.raft.applied_index && entry.kind.is_app()
            })
            .filter_map(|entry| decode_proposal(entry).ok().flatten())
            .filter_map(|envelope| decode_command(&envelope.command).ok())
            .any(|command| {
                if sessions {
                    matches!(command, KvCommand::ExpireSessions { .. })
                } else {
                    matches!(command, KvCommand::PurgeExpired { .. })
                }
            })
    }

    fn map_effects(
        &mut self,
        effects: Vec<RaftEffect>,
        _proposal: Option<(ClientId, u64)>,
    ) -> Result<Vec<NodeEffect>, NodeError> {
        if self.raft.role != Role::Leader {
            self.client_routes.clear();
            self.admin_routes.clear();
        }
        let mut output = Vec::new();
        let mut remaining = effects.into_iter();
        while let Some(effect) = remaining.next() {
            match effect {
                RaftEffect::Send(message) => output.push(NodeEffect::Send(message)),
                RaftEffect::ReceiveSnapshotChunk(message) => {
                    output.push(NodeEffect::ReceiveSnapshotChunk(message))
                }
                RaftEffect::ReceiveSnapshotAck(message) => {
                    output.push(NodeEffect::ReceiveSnapshotAck(message))
                }
                RaftEffect::PersistHard(hard) => {
                    output.push(NodeEffect::PersistHard(hard));
                    self.continuation = Some(NodeContinuation::Raft(remaining.collect()));
                    break;
                }
                RaftEffect::PersistEntries(entries) => {
                    output.push(NodeEffect::PersistEntries(entries));
                    self.continuation = Some(NodeContinuation::Raft(remaining.collect()));
                    break;
                }
                RaftEffect::TruncateSuffix(index) => {
                    self.client_routes
                        .retain(|route_index, _| *route_index < index);
                    self.session_reservations
                        .retain(|route_index, _| *route_index < index);
                    self.logical_reservations
                        .retain(|route_index, _| *route_index < index);
                    self.admin_routes.clear();
                    output.push(NodeEffect::TruncateSuffix(index));
                    self.continuation = Some(NodeContinuation::Raft(remaining.collect()));
                    break;
                }
                RaftEffect::Apply(entries) => {
                    let mut prepared = self.prepare_committed_entries(entries)?;
                    let Some(current) = prepared.pop_front() else {
                        continue;
                    };
                    let bytes = current.wal_frame.clone();
                    self.continuation =
                        Some(NodeContinuation::Store(Box::new(StoreApplyContinuation {
                            current,
                            pending: prepared,
                            deferred: Vec::new(),
                            remaining: remaining.collect(),
                        })));
                    output.push(NodeEffect::PersistStore { bytes });
                    break;
                }
                RaftEffect::ArmTimer { id, at, kind } => {
                    output.push(NodeEffect::ArmTimer { id, at, kind })
                }
                RaftEffect::ReadBarrier { .. } => {}
                RaftEffect::ReadBarrierReady { index } => {
                    self.read_barrier_ready = Some(index);
                    output.extend(self.finish_follower_read_round(index));
                }
                RaftEffect::FollowerReadRequest {
                    from,
                    request_id,
                    command_hash,
                    at,
                } => output.extend(self.accept_follower_read_request(
                    from,
                    request_id,
                    command_hash,
                    at,
                )?),
                RaftEffect::FollowerReadGrant {
                    from,
                    term,
                    request_id,
                    command_hash,
                    read_index,
                    read_time,
                } => self.accept_follower_read_grant(
                    from,
                    term,
                    request_id,
                    command_hash,
                    read_index,
                    read_time,
                ),
                RaftEffect::Trace { name, .. } => output.push(NodeEffect::Trace(name)),
            }
        }
        output.extend(self.drain_pending_reads());
        output.extend(self.drain_pending_follower_reads());
        Ok(output)
    }

    fn prepare_committed_entries(
        &mut self,
        entries: Vec<Entry>,
    ) -> Result<VecDeque<PreparedCommittedEntry>, NodeError> {
        let mut working_kv = self.kv.clone();
        let mut working_sessions = self.sessions.clone();
        let mut working_raft = self.raft.clone();
        let mut prepared = VecDeque::new();
        for entry in entries {
            let base_kv = working_kv.clone();
            let base_sessions = working_sessions.clone();
            let mut canonical_command = Vec::new();
            let mut cached_reply = Vec::new();
            let mut reply = None;
            let mut finished_admin_session = None;
            let mut post_effects = Vec::new();
            let mut finishes_expiry_sweep = false;
            let mut finishes_session_expiry_sweep = false;
            let mut expired_keys = 0_u64;
            let entry_kind = match entry.kind {
                cc_raft::EntryKind::App | cc_raft::EntryKind::AppV3 => {
                    let envelope = decode_proposal(&entry)
                        .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?
                        .ok_or(NodeError::MalformedCommittedEntry(entry.index))?;
                    if envelope.command.len() as u64 > self.config.policy.max_command_bytes {
                        return Err(NodeError::MalformedCommittedEntry(entry.index));
                    }
                    let command = decode_command(&envelope.command)
                        .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?;
                    if matches!(entry.kind, cc_raft::EntryKind::AppV3)
                        != matches!(command, KvCommand::Batch { .. })
                    {
                        return Err(NodeError::MalformedCommittedEntry(entry.index));
                    }
                    finishes_expiry_sweep = matches!(command, KvCommand::PurgeExpired { .. });
                    finishes_session_expiry_sweep =
                        matches!(command, KvCommand::ExpireSessions { .. });
                    if matches!(command, KvCommand::Batch { .. })
                        && working_raft.active_features() & cc_core::ATOMIC_BATCH_FEATURE == 0
                    {
                        return Err(NodeError::MalformedCommittedEntry(entry.index));
                    }
                    let policy = self.config.policy;
                    validate_command_policy(&command, policy)
                        .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?;
                    canonical_command.clone_from(&envelope.command);
                    let result = match envelope.session {
                        Some((session_key, sequence)) => {
                            if session_key.namespace != SessionNamespace::UserRequest as u8
                                || sequence == 0
                            {
                                return Err(NodeError::MalformedCommittedEntry(entry.index));
                            }
                            working_sessions.apply_user(
                                policy,
                                session_key,
                                sequence,
                                envelope.command,
                                envelope.leader_time,
                                || {
                                    working_kv.apply_command_only_with_batch_limits(
                                        entry.index,
                                        entry.term,
                                        command.clone(),
                                        envelope.leader_time,
                                        BatchLimits {
                                            max_commands: policy.max_batch_commands,
                                            max_bytes: policy.max_batch_bytes,
                                            max_reply_bytes: policy.max_batch_reply_bytes,
                                            max_expiry_items: policy.max_keys_per_expiry_sweep,
                                        },
                                    )
                                },
                            )
                        }
                        None => working_kv.apply_command_only_with_batch_limits(
                            entry.index,
                            entry.term,
                            command.clone(),
                            envelope.leader_time,
                            BatchLimits {
                                max_commands: policy.max_batch_commands,
                                max_bytes: policy.max_batch_bytes,
                                max_reply_bytes: policy.max_batch_reply_bytes,
                                max_expiry_items: policy.max_keys_per_expiry_sweep,
                            },
                        ),
                    };
                    if finishes_expiry_sweep
                        && let KvReply::Integer(count) = &result
                        && *count > 0
                    {
                        expired_keys = u64::try_from(*count).unwrap_or(u64::MAX);
                    }
                    if let KvCommand::ExpireSessions { up_to } = command {
                        let pinned = working_raft
                            .log
                            .iter()
                            .filter(|candidate| {
                                candidate.index > working_raft.applied_index
                                    && candidate.kind.is_config()
                            })
                            .filter_map(|candidate| ConfigEnvelope::decode(&candidate.payload).ok())
                            .find_map(|config| config.admin_session.map(|(key, _)| key));
                        working_sessions.expire_due(
                            policy,
                            up_to,
                            policy.max_keys_per_expiry_sweep,
                            pinned,
                        );
                    }
                    if working_kv.applied_index < entry.index {
                        working_kv.mark_applied(entry.index, entry.term, envelope.leader_time);
                    }
                    cached_reply = encode_reply(&result);
                    reply = self
                        .client_routes
                        .remove(&entry.index)
                        .map(|(client, sequence)| PreparedReply::Kv(client, sequence, result));
                    StoreEntryKind::App
                }
                cc_raft::EntryKind::Config | cc_raft::EntryKind::ConfigV3 => {
                    let envelope = ConfigEnvelope::decode(&entry.payload)
                        .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?;
                    if matches!(entry.kind, cc_raft::EntryKind::ConfigV3)
                        != matches!(
                            envelope.operation,
                            ConfigOperation::ActivateFeature { .. }
                        )
                    {
                        return Err(NodeError::MalformedCommittedEntry(entry.index));
                    }
                    working_kv.mark_applied(entry.index, entry.term, envelope.leader_time);
                    canonical_command.clone_from(&entry.payload);
                    match (envelope.admin_session, &envelope.operation) {
                        (Some((key, sequence)), ConfigOperation::EnterJoint { .. }) => {
                            // EnterJoint is only the durable first half of an
                            // operator workflow.  Do not cache or report
                            // success yet: on the leader, append the matching
                            // LeaveJoint with the *same* AdminRequest identity.
                            // Followers merely apply the projection; the
                            // resulting entry is replicated through Raft.
                            post_effects = working_raft
                                .apply_committed_config(&entry)
                                .map_err(NodeError::Raft)?;
                            if working_raft.role == Role::Leader {
                                post_effects.extend(
                                    working_raft
                                        .propose_admin_config(ConfigEnvelope {
                                            admin_session: Some((key, sequence)),
                                            leader_time: envelope.leader_time,
                                            operation: ConfigOperation::LeaveJoint {
                                                enter_index: entry.index,
                                            },
                                        })
                                        .map_err(NodeError::Raft)?,
                                );
                            }
                        }
                        (Some((key, sequence)), ConfigOperation::LeaveJoint { enter_index }) => {
                            // The canonical request being deduplicated is the
                            // original EnterJoint, not this internal second
                            // half.  A retry of promote/remove therefore finds
                            // one byte-identical terminal result.
                            let original = working_raft
                                .log
                                .iter()
                                .find(|candidate| candidate.index == *enter_index)
                                .and_then(|candidate| {
                                    ConfigEnvelope::decode(&candidate.payload)
                                        .ok()
                                        .filter(|begin| {
                                            begin.admin_session == Some((key, sequence))
                                                && matches!(
                                                    begin.operation,
                                                    ConfigOperation::EnterJoint { .. }
                                                )
                                        })
                                        .map(|_| candidate.payload.clone())
                                })
                                .ok_or(NodeError::MalformedCommittedEntry(entry.index))?;
                            let operation_tag = ConfigOperation::EnterJoint {
                                new_voters: BTreeSet::new(),
                            }
                            .tag();
                            let (admin_reply, effects) = working_sessions
                                .apply_admin(
                                    AdminApplyContext {
                                        policy: self.config.policy,
                                        key,
                                        sequence,
                                        canonical_command: original,
                                        operation_tag,
                                        source_index: entry.index,
                                        at: envelope.leader_time,
                                    },
                                    || {
                                        let effects =
                                            working_raft.apply_committed_config(&entry)?;
                                        Ok((
                                            AdminReply {
                                                operation_tag,
                                                result: AdminResultTag::Applied,
                                                source_index: entry.index,
                                                detail: Vec::new(),
                                            },
                                            effects,
                                        ))
                                    },
                                )
                                .map_err(NodeError::Raft)?;
                            cached_reply = admin_reply.encode();
                            post_effects = effects;
                            reply = self.admin_routes.get(&key).map(|(client, route)| {
                                PreparedReply::Admin(*client, *route, admin_reply)
                            });
                            finished_admin_session = Some(key);
                        }
                        (Some(_), ConfigOperation::BeginLeaderTransfer { .. }) => {
                            post_effects = working_raft
                                .apply_committed_config(&entry)
                                .map_err(NodeError::Raft)?;
                        }
                        (
                            Some((key, sequence)),
                            ConfigOperation::FinishLeaderTransfer {
                                intent_index,
                                result,
                            },
                        ) => {
                            let original = working_raft
                                .log
                                .iter()
                                .find(|candidate| candidate.index == *intent_index)
                                .and_then(|candidate| {
                                    ConfigEnvelope::decode(&candidate.payload)
                                        .ok()
                                        .filter(|begin| {
                                            begin.admin_session == Some((key, sequence))
                                                && matches!(
                                                    begin.operation,
                                                    ConfigOperation::BeginLeaderTransfer { .. }
                                                )
                                        })
                                        .map(|_| candidate.payload.clone())
                                })
                                .ok_or(NodeError::MalformedCommittedEntry(entry.index))?;
                            let result_tag = match result {
                                TransferResult::Success => AdminResultTag::TransferSuccess,
                                TransferResult::Timeout => AdminResultTag::TransferTimeout,
                                TransferResult::Superseded => AdminResultTag::TransferSuperseded,
                            };
                            let (admin_reply, effects) = working_sessions
                                .apply_admin(
                                    AdminApplyContext {
                                        policy: self.config.policy,
                                        key,
                                        sequence,
                                        canonical_command: original,
                                        operation_tag: ConfigOperation::BeginLeaderTransfer {
                                            target: working_raft
                                                .leadership_transfer_state()
                                                .map(|state| state.target)
                                                .ok_or(NodeError::MalformedCommittedEntry(
                                                    entry.index,
                                                ))?,
                                        }
                                        .tag(),
                                        source_index: entry.index,
                                        at: envelope.leader_time,
                                    },
                                    || {
                                        let effects =
                                            working_raft.apply_committed_config(&entry)?;
                                        Ok((
                                            AdminReply {
                                                operation_tag: 6,
                                                result: result_tag,
                                                source_index: entry.index,
                                                detail: Vec::new(),
                                            },
                                            effects,
                                        ))
                                    },
                                )
                                .map_err(NodeError::Raft)?;
                            cached_reply = admin_reply.encode();
                            post_effects = effects;
                            reply = self.admin_routes.get(&key).map(|(client, route)| {
                                PreparedReply::Admin(*client, *route, admin_reply)
                            });
                            finished_admin_session = Some(key);
                        }
                        (Some((key, sequence)), _) => {
                            let operation_tag = envelope.operation.tag();
                            let (admin_reply, effects) = working_sessions
                                .apply_admin(
                                    AdminApplyContext {
                                        policy: self.config.policy,
                                        key,
                                        sequence,
                                        canonical_command: entry.payload.clone(),
                                        operation_tag,
                                        source_index: entry.index,
                                        at: envelope.leader_time,
                                    },
                                    || {
                                        let effects =
                                            working_raft.apply_committed_config(&entry)?;
                                        Ok((
                                            AdminReply {
                                                operation_tag,
                                                result: AdminResultTag::Applied,
                                                source_index: entry.index,
                                                detail: Vec::new(),
                                            },
                                            effects,
                                        ))
                                    },
                                )
                                .map_err(NodeError::Raft)?;
                            cached_reply = admin_reply.encode();
                            post_effects = effects;
                            reply = self.admin_routes.get(&key).map(|(client, route)| {
                                PreparedReply::Admin(*client, *route, admin_reply)
                            });
                            finished_admin_session = Some(key);
                        }
                        (None, _) => {
                            post_effects = working_raft
                                .apply_committed_config(&entry)
                                .map_err(NodeError::Raft)?;
                        }
                    }
                    StoreEntryKind::Config
                }
                cc_raft::EntryKind::Noop => {
                    working_kv.mark_applied(entry.index, entry.term, Time::from_nanos(0));
                    StoreEntryKind::Noop
                }
            };
            let (mutations, mut metadata) = base_kv.store_delta(&working_kv);
            metadata.extend(session_table_delta(&base_sessions, &working_sessions));
            let batch = StoreApplyBatch {
                entry_kind,
                watermark: StoreWatermark {
                    index: entry.index,
                    term: entry.term,
                    last_leader_time: working_kv.last_leader_time(),
                },
                mutations,
                metadata,
                canonical_command,
                cached_reply,
            };
            let store_apply = base_kv.store.prepare_apply(batch).map_err(KvError::Store)?;
            let wal_frame = store_apply.wal_frame().to_vec();
            working_kv.store = store_apply.into_store();
            prepared.push_back(PreparedCommittedEntry {
                kv: working_kv.clone(),
                sessions: working_sessions.clone(),
                raft: working_raft.clone(),
                wal_frame,
                reply,
                finished_admin_session,
                post_effects,
                finishes_expiry_sweep,
                finishes_session_expiry_sweep,
                expired_keys,
            });
        }
        Ok(prepared)
    }

    fn drain_pending_reads(&mut self) -> Vec<NodeEffect> {
        let Some(ready) = self.read_barrier_ready else {
            return Vec::new();
        };
        let applied = self.raft.applied_index;
        let pending = std::mem::take(&mut self.pending_reads);
        let mut output = Vec::new();
        for read in pending {
            if ready >= read.index && applied >= read.index {
                if self.kv.store.is_file_backed() {
                    self.completed_file_reads.insert(
                        (read.client, read.sequence),
                        (read.command.clone(), read.at),
                    );
                }
                let reply = self
                    .kv
                    .read(read.command, read.at)
                    .unwrap_or_else(KvReply::Error);
                output.push(NodeEffect::ReadReply {
                    client: read.client,
                    sequence: read.sequence,
                    reply,
                });
            } else {
                self.pending_reads.push(read);
            }
        }
        output
    }

    fn accept_follower_read_request(
        &mut self,
        from: NodeId,
        request_id: u64,
        command_hash: u64,
        at: Time,
    ) -> Result<Vec<NodeEffect>, NodeError> {
        let supported = self.peer_capabilities.get(&from).is_some_and(|capability| {
            capability.semantic_version == SEMANTIC_VERSION_V3
                && capability.features & FOLLOWER_READ_FEATURE != 0
        });
        if !supported
            || self.raft.role != Role::Leader
            || self.raft.hard_state.term.get() == 0
            || (!self.raft.voters.contains(&from) && !self.raft.learners.contains(&from))
            || self.pending_follower_grants.len() >= self.config.host_limits.max_pending_peer
        {
            return Ok(vec![NodeEffect::Trace("follower_read_request_rejected")]);
        }
        let key = (from, request_id);
        if self.pending_follower_grants.contains_key(&key) {
            return Ok(Vec::new());
        }
        self.pending_follower_grants.insert(
            key,
            PendingFollowerGrant {
                follower: from,
                request_id,
                command_hash,
                request_time: at,
                term: self.raft.hard_state.term,
            },
        );
        if self.follower_read_round_active {
            return Ok(Vec::new());
        }
        self.follower_read_round_active = true;
        match self.raft.request_read() {
            Ok(effects) => self.map_effects(effects, None),
            Err(_) => {
                self.pending_follower_grants.clear();
                self.follower_read_round_active = false;
                Ok(vec![NodeEffect::Trace("follower_read_barrier_unavailable")])
            }
        }
    }

    fn finish_follower_read_round(&mut self, read_index: LogIndex) -> Vec<NodeEffect> {
        if !self.follower_read_round_active {
            return Vec::new();
        }
        self.follower_read_round_active = false;
        let grants = std::mem::take(&mut self.pending_follower_grants);
        grants
            .into_values()
            .filter(|grant| {
                self.raft.role == Role::Leader
                    && self.raft.hard_state.term == grant.term
                    && self
                        .peer_capabilities
                        .get(&grant.follower)
                        .is_some_and(|capability| {
                            capability.semantic_version == SEMANTIC_VERSION_V3
                                && capability.features & FOLLOWER_READ_FEATURE != 0
                        })
            })
            .map(|grant| {
                NodeEffect::Send(Message {
                    proto_version: SEMANTIC_VERSION_V3,
                    from: self.id(),
                    to: grant.follower,
                    term: self.raft.hard_state.term,
                    kind: MessageKind::FollowerReadGrant {
                        request_id: grant.request_id,
                        command_hash: grant.command_hash,
                        read_index,
                        read_time: grant.request_time,
                    },
                })
            })
            .collect()
    }

    fn accept_follower_read_grant(
        &mut self,
        from: NodeId,
        term: Term,
        request_id: u64,
        command_hash: u64,
        read_index: LogIndex,
        read_time: Time,
    ) {
        let supported = self.peer_capabilities.get(&from).is_some_and(|capability| {
            capability.semantic_version == SEMANTIC_VERSION_V3
                && capability.features & FOLLOWER_READ_FEATURE != 0
        });
        if !supported || self.raft.leader_id != Some(from) || self.raft.hard_state.term != term {
            return;
        }
        for pending in self.pending_follower_reads.values_mut() {
            if pending.leader == from
                && pending.term == term
                && pending.request_id == request_id
                && pending.command_hash == command_hash
            {
                pending.grant = Some((read_index, read_time));
                return;
            }
        }
    }

    fn drain_pending_follower_reads(&mut self) -> Vec<NodeEffect> {
        let pending = std::mem::take(&mut self.pending_follower_reads);
        let mut output = Vec::new();
        for ((client, sequence), read) in pending {
            let current = self.raft.leader_id == Some(read.leader)
                && self.raft.hard_state.term == read.term
                && (self.raft.voters.contains(&self.id())
                    || self.raft.learners.contains(&self.id()));
            let Some((read_index, read_time)) = read.grant else {
                if current {
                    self.pending_follower_reads.insert((client, sequence), read);
                } else {
                    output.push(NodeEffect::ReadReply {
                        client,
                        sequence,
                        reply: KvReply::Error(KvError::InvalidInput),
                    });
                }
                continue;
            };
            if !current {
                output.push(NodeEffect::ReadReply {
                    client,
                    sequence,
                    reply: KvReply::Error(KvError::InvalidInput),
                });
            } else if self.raft.applied_index >= read_index {
                if self.kv.store.is_file_backed() {
                    self.completed_file_reads
                        .insert((client, sequence), (read.command.clone(), read_time));
                }
                let reply = self
                    .kv
                    .read(read.command, read_time)
                    .unwrap_or_else(KvReply::Error);
                self.completed_follower_reads.insert(
                    (client, sequence),
                    FollowerReadMetadata {
                        read_index,
                        applied_index: self.raft.applied_index,
                        applied_term: self.kv.applied_term,
                        read_time,
                    },
                );
                output.push(NodeEffect::ReadReply {
                    client,
                    sequence,
                    reply,
                });
            } else {
                self.pending_follower_reads.insert((client, sequence), read);
            }
        }
        output
    }

    #[must_use]
    pub fn proposal_bytes(
        client: ClientId,
        sequence: u64,
        command: &KvCommand,
        leader_time: Time,
    ) -> Vec<u8> {
        encode_proposal(client, sequence, command.clone(), leader_time)
    }

    pub fn create_snapshot(&mut self) -> Result<NodeSnapshot, NodeError> {
        if self
            .raft
            .membership_state_at(self.kv.applied_index)
            .joint
            .is_some()
        {
            return Err(NodeError::Environment(
                "checkpoint deferred during joint configuration",
            ));
        }
        let kv = self.kv.snapshot()?;
        Ok(NodeSnapshot {
            kv,
            sessions: self.sessions.clone(),
            membership: self.raft.membership_state_at(self.kv.applied_index),
            leadership_transfer: self.raft.leadership_transfer_state(),
            cluster_policy: self.config.policy,
            last_included_index: self.raft.applied_index,
            last_included_term: self
                .raft
                .term_at(self.raft.applied_index)
                .unwrap_or(cc_core::Term::new(0)),
        })
    }

    pub fn install_snapshot(&mut self, snapshot: NodeSnapshot) -> Result<(), NodeError> {
        if snapshot.cluster_policy.encode() != self.config.policy.encode() {
            return Err(NodeError::MalformedCommittedEntry(
                snapshot.last_included_index,
            ));
        }
        self.kv = Kv::restore(snapshot.kv, self.config.store)?;
        self.sessions = snapshot.sessions;
        self.raft
            .restore_membership_state(snapshot.membership)
            .map_err(NodeError::Raft)?;
        self.raft
            .restore_leadership_transfer(snapshot.leadership_transfer)
            .map_err(NodeError::Raft)?;
        self.raft
            .install_snapshot_state(snapshot.last_included_index, snapshot.last_included_term);
        self.raft.replay_retained_membership_suffix();
        Ok(())
    }

    /// Materialise a canonical logical checkpoint for compatibility tests and
    /// small in-process callers. Hosts use [`Self::begin_ccsn_encode`] so the
    /// durable checkpoint path does not allocate this complete image.
    pub fn encode_ccsn_snapshot(&self) -> Result<Bytes, SnapshotCodecError> {
        encode_ccsn(&CcsnSnapshot {
            cluster_id: self.config.cluster_id,
            cluster_policy: self.config.policy,
            membership: self.raft.membership_state_at(self.kv.applied_index),
            kv: self.kv.logical_snapshot(self.kv.last_leader_time()),
            sessions: self.sessions.clone(),
            leadership_transfer: self.raft.leadership_transfer_state(),
        })
    }

    /// Capture a bounded CCSN writer. The writer owns only logical snapshot
    /// state and emits checkpoint bytes in caller-selected chunks; it never
    /// materialises a complete transport image in the host driver.
    pub fn begin_ccsn_encode(&self) -> Result<CcsnStreamEncoder, SnapshotCodecError> {
        CcsnStreamEncoder::new(CcsnSnapshot {
            cluster_id: self.config.cluster_id,
            cluster_policy: self.config.policy,
            membership: self.raft.membership_state_at(self.kv.applied_index),
            kv: self.kv.logical_snapshot(self.kv.last_leader_time()),
            sessions: self.sessions.clone(),
            leadership_transfer: self.raft.leadership_transfer_state(),
        })
    }

    /// Exact canonical charge used by `max_live_logical_bytes`.  This is the
    /// encoded CCSN length, including its header, membership/policy records,
    /// key/session/tombstone framing, and footer.  It is deliberately not an
    /// allocator-dependent estimate of the live Rust values.
    pub fn logical_state_charge(&self) -> Result<u64, SnapshotCodecError> {
        self.begin_ccsn_encode().map(|encoder| encoder.total_len())
    }

    /// Decode and atomically replace state from a validated logical
    /// checkpoint.  Callers must only invoke this after their staging file is
    /// complete and durable; a decode failure leaves the current node intact.
    pub fn install_ccsn_snapshot(&mut self, bytes: &[u8]) -> Result<(), NodeError> {
        let snapshot = self.decode_ccsn_snapshot(bytes)?;
        self.install_decoded_ccsn_snapshot(snapshot)
    }

    /// Create an incremental checkpoint decoder configured for this exact
    /// cluster and its host-side total-size fence.  Drivers feed it only
    /// bytes read back from durable staging files.
    #[must_use]
    pub fn begin_ccsn_decode(&self) -> CcsnStreamDecoder {
        CcsnStreamDecoder::new(
            self.config.cluster_id,
            self.config.host_limits.max_snapshot_bytes,
        )
    }

    /// Validate a logically decoded checkpoint before its host publishes the
    /// staging path.  This leaves the current state untouched.
    pub fn validate_decoded_ccsn_snapshot(&self, snapshot: &CcsnSnapshot) -> Result<(), NodeError> {
        if snapshot.cluster_id != self.config.cluster_id
            || snapshot.cluster_policy.encode() != self.config.policy.encode()
            || snapshot.kv.applied_index < self.raft.applied_index
        {
            return Err(NodeError::MalformedCommittedEntry(
                snapshot.kv.applied_index,
            ));
        }
        Ok(())
    }

    /// Atomically replace in-core logical state with a checkpoint previously
    /// validated by [`Self::validate_decoded_ccsn_snapshot`].  The caller is
    /// responsible for durable file publication before this transition.
    pub fn install_decoded_ccsn_snapshot(
        &mut self,
        snapshot: CcsnSnapshot,
    ) -> Result<(), NodeError> {
        self.validate_decoded_ccsn_snapshot(&snapshot)?;
        let index = snapshot.kv.applied_index;
        let term = snapshot.kv.applied_term;
        let kv = Kv::restore_logical(snapshot.kv, self.config.store)
            .map_err(|_| NodeError::MalformedCommittedEntry(index))?;
        self.raft
            .restore_membership_state(snapshot.membership)
            .map_err(NodeError::Raft)?;
        self.raft
            .restore_leadership_transfer(None)
            .map_err(NodeError::Raft)?;
        self.kv = kv;
        self.sessions = snapshot.sessions;
        self.raft.install_snapshot_state(index, term);
        self.raft.replay_retained_membership_suffix();
        Ok(())
    }

    /// Verify a complete CCSN image against this node without changing any
    /// state. Hosts use this before atomically publishing a staged checkpoint;
    /// installation remains a separate transition after publication succeeds.
    pub fn validate_ccsn_snapshot(&self, bytes: &[u8]) -> Result<(), NodeError> {
        self.decode_ccsn_snapshot(bytes).map(|_| ())
    }

    fn decode_ccsn_snapshot(&self, bytes: &[u8]) -> Result<CcsnSnapshot, NodeError> {
        let snapshot = decode_ccsn(
            bytes,
            self.config.cluster_id,
            self.config.host_limits.max_snapshot_bytes,
        )
        .map_err(|_| NodeError::MalformedCommittedEntry(self.raft.applied_index))?;
        self.validate_decoded_ccsn_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    fn require_active_feature(&self, command: &KvCommand) -> Result<(), NodeError> {
        if matches!(command, KvCommand::Batch { .. })
            && self.raft.active_features() & cc_core::ATOMIC_BATCH_FEATURE == 0
        {
            return Err(NodeError::FeatureDisabled);
        }
        Ok(())
    }
}

fn validate_command_policy(command: &KvCommand, policy: ClusterPolicy) -> Result<(), KvError> {
    if let KvCommand::Batch { commands } = command {
        validate_batch(commands, policy.max_batch_commands, policy.max_batch_bytes)?;
    }
    Ok(())
}

fn session_table_delta(before: &SessionTable, after: &SessionTable) -> Vec<StoreMetadataEdit> {
    const SESSION_NAMESPACE: u8 = 3;
    const TOMBSTONE_NAMESPACE: u8 = 4;
    let key_bytes = |key: SessionKey| {
        let mut bytes = Vec::with_capacity(9);
        bytes.push(key.namespace);
        bytes.extend_from_slice(&key.client.get().to_le_bytes());
        bytes
    };
    let mut edits = Vec::new();
    let record_keys = before
        .records
        .keys()
        .chain(after.records.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for key in record_keys {
        match (before.records.get(&key), after.records.get(&key)) {
            (Some(left), Some(right)) if left == right => {}
            (_, Some(record)) => {
                let mut enc = Enc::new();
                enc.u64(record.max_seq);
                enc.u64(record.last_active.as_nanos());
                enc.bytes(&record.canonical_command);
                enc.bytes(&record.cached_reply);
                edits.push(StoreMetadataEdit::Upsert {
                    namespace: SESSION_NAMESPACE,
                    key: key_bytes(key),
                    value: enc.finish(),
                });
            }
            (Some(_), None) => edits.push(StoreMetadataEdit::Delete {
                namespace: SESSION_NAMESPACE,
                key: key_bytes(key),
            }),
            (None, None) => unreachable!("session key came from map union"),
        }
    }
    let tombstone_keys = before
        .tombstones
        .keys()
        .chain(after.tombstones.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for key in tombstone_keys {
        match (before.tombstones.get(&key), after.tombstones.get(&key)) {
            (Some(left), Some(right)) if left == right => {}
            (_, Some(tombstone)) => {
                let mut value = Vec::with_capacity(16);
                value.extend_from_slice(&tombstone.max_seq.to_le_bytes());
                value.extend_from_slice(&tombstone.expires_at.as_nanos().to_le_bytes());
                edits.push(StoreMetadataEdit::Upsert {
                    namespace: TOMBSTONE_NAMESPACE,
                    key: key_bytes(key),
                    value,
                });
            }
            (Some(_), None) => edits.push(StoreMetadataEdit::Delete {
                namespace: TOMBSTONE_NAMESPACE,
                key: key_bytes(key),
            }),
            (None, None) => unreachable!("tombstone key came from map union"),
        }
    }
    edits
}

fn encode_proposal(
    client: ClientId,
    sequence: u64,
    command: KvCommand,
    leader_time: Time,
) -> Vec<u8> {
    AppEnvelope {
        session: Some((
            SessionKey::new(SessionNamespace::UserRequest as u8, client)
                .expect("public proposal needs nonzero client id"),
            sequence,
        )),
        leader_time,
        command: encode_command(&command),
    }
    .encode()
}

fn decode_proposal(entry: &Entry) -> Result<Option<AppEnvelope>, KvError> {
    if !entry.kind.is_app() || entry.payload.is_empty() {
        return Ok(None);
    }
    let envelope = AppEnvelope::decode(&entry.payload)?;
    Ok(Some(envelope))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_core::Term;
    use cc_raft::{AppendResponse, MessageKind, PROTOCOL_VERSION};

    fn config(id: u64) -> NodeConfig {
        NodeConfig {
            id: NodeId::new(id),
            cluster_id: [7; 16],
            seed: Seed::new(id),
            raft: RaftConfig::default(),
            store: StoreConfig::default(),
            policy: ClusterPolicy::default(),
            host_limits: HostLimits::default(),
        }
    }

    fn restore_atomic_batch_feature(node: &mut Node) {
        let mut membership = node.raft.membership_state();
        membership.active_features = cc_core::ATOMIC_BATCH_FEATURE;
        node.raft
            .restore_membership_state(membership)
            .expect("committed feature snapshot");
    }

    #[test]
    fn trap_membership_retry_is_idempotent() {
        let session = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(77))
            .expect("admin session");
        let envelope = |time, target| {
            ConfigEnvelope {
                admin_session: Some((session, 9)),
                leader_time: Time::from_nanos(time),
                operation: ConfigOperation::BeginLeaderTransfer {
                    target: NodeId::new(target),
                },
            }
            .encode()
        };
        assert!(same_admin_operation(&envelope(10, 4), &envelope(20, 4)));
        assert!(!same_admin_operation(&envelope(10, 4), &envelope(20, 5)));
    }

    #[test]
    fn trap_membership_request_id_cannot_alias_another_operation() {
        let session = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(88))
            .expect("admin session");
        let canonical = |operation| {
            ConfigEnvelope {
                admin_session: Some((session, 3)),
                leader_time: Time::from_nanos(10),
                operation,
            }
            .encode()
        };
        assert!(!same_admin_operation(
            &canonical(ConfigOperation::BeginLeaderTransfer {
                target: NodeId::new(2),
            }),
            &canonical(ConfigOperation::BeginLeaderTransfer {
                target: NodeId::new(3),
            }),
        ));
    }

    #[test]
    fn trap_membership_preflight_rejects_quorum_loss() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(2);
        let session = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(91))
            .expect("admin session");
        assert!(matches!(
            node.admin_request(
                Time::from_nanos(10),
                ClientId::new(1),
                1,
                session,
                1,
                ConfigOperation::EnterJoint {
                    new_voters: [NodeId::new(1), NodeId::new(2)].into_iter().collect(),
                },
            ),
            Err(NodeError::Environment(
                "membership preflight would lose quorum"
            ))
        ));
        assert!(!node.raft.joint_active());
    }

    #[test]
    fn proposal_envelope_round_trips_through_public_builder() {
        let bytes = Node::proposal_bytes(
            ClientId::new(3),
            4,
            &KvCommand::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
                ttl: None,
            },
            Time::from_nanos(9),
        );
        let entry = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: cc_raft::EntryKind::App,
            payload: bytes,
        };
        let decoded = decode_proposal(&entry).expect("decode").expect("app");
        assert_eq!(
            decoded.session,
            Some((
                SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(3))
                    .expect("session"),
                4,
            ))
        );
    }

    #[test]
    fn trap_single_voter_election_still_waits_for_hard_state_fsync() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut node = Node::new(config(1), voters).expect("node");
        let first = node
            .on_input(NodeInput::Tick {
                now: Time::from_nanos(1_000_000_000),
            })
            .expect("single voter campaign");
        assert!(matches!(first.as_slice(), [NodeEffect::PersistHard(_)]));
        assert_eq!(node.role(), Role::Leader);
        let second = node
            .on_input(NodeInput::Persisted { success: true })
            .expect("hard state durable");
        assert!(matches!(second.as_slice(), [NodeEffect::PersistEntries(_)]));
    }

    #[test]
    fn trap_ccap_session_absence_is_canonical() {
        let envelope = AppEnvelope {
            session: None,
            leader_time: Time::from_nanos(3),
            command: encode_command(&KvCommand::Ping),
        };
        let mut encoded = envelope.encode();
        // C C A P + version + has-session; an absent session cannot hide a
        // namespace value in its otherwise zero identity fields.
        encoded[7] = SessionNamespace::AdminRequest as u8;
        let last = encoded.len() - 4;
        let crc = crc32c_zeroed_tail(&encoded);
        encoded[last..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(AppEnvelope::decode(&encoded), Err(KvError::InvalidInput));
    }

    #[test]
    fn trap_plain_route_ids_never_enter_raft_payload() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        let command = encode_command(&KvCommand::Ping);
        node.on_input(NodeInput::ClientBytes {
            route_client: ClientId::new(99),
            route_req: 77,
            session: None,
            command: command.clone(),
            leader_time: Time::from_nanos(5),
        })
        .expect("plain request");
        let entry = node.raft.log.last().expect("assigned entry");
        assert_eq!(
            entry.payload,
            AppEnvelope {
                session: None,
                leader_time: Time::from_nanos(5),
                command,
            }
            .encode()
        );
        assert_eq!(
            node.client_routes.get(&entry.index),
            Some(&(ClientId::new(99), 77))
        );
    }

    #[test]
    fn trap_follower_read_waits_for_readindex() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let mut leader = Node::new(config(1), voters.clone()).expect("leader");
        let mut follower = Node::new(config(2), voters).expect("follower");
        let noop = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: cc_raft::EntryKind::Noop,
            payload: Vec::new(),
        };
        for node in [&mut leader, &mut follower] {
            node.raft.hard_state.term = Term::new(1);
            node.raft.log.push(noop.clone());
            node.raft.commit_index = LogIndex::new(1);
            node.raft.applied_index = LogIndex::new(1);
            node.kv
                .mark_applied(LogIndex::new(1), Term::new(1), Time::from_nanos(0));
        }
        leader.raft.role = Role::Leader;
        leader.raft.leader_id = Some(NodeId::new(1));
        follower.raft.role = Role::Follower;
        follower.raft.leader_id = Some(NodeId::new(1));
        leader
            .observe_peer_capability(NodeId::new(2), SEMANTIC_VERSION_V3, FOLLOWER_READ_FEATURE)
            .expect("leader capability");
        follower
            .observe_peer_capability(NodeId::new(1), SEMANTIC_VERSION_V3, FOLLOWER_READ_FEATURE)
            .expect("follower capability");

        let request = follower
            .request_follower_read(
                ClientId::new(7),
                9,
                KvCommand::Get {
                    key: b"missing".to_vec(),
                },
                Time::from_nanos(20),
            )
            .expect("request");
        let NodeEffect::Send(request) = request.into_iter().next().expect("request send") else {
            panic!("follower read must send one v3 request");
        };
        let effects = leader
            .on_input(NodeInput::MessageAt {
                now: Time::from_nanos(30),
                message: request,
            })
            .expect("leader request");
        assert!(effects.iter().any(|effect| matches!(
            effect,
            NodeEffect::Send(Message {
                kind: MessageKind::AppendReq(_),
                ..
            })
        )));
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                NodeEffect::Send(Message {
                    kind: MessageKind::FollowerReadGrant { .. },
                    ..
                })
            )),
            "the grant must wait for a fresh quorum ReadIndex acknowledgement"
        );
        let effects = leader
            .on_input(NodeInput::Message(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(2),
                to: NodeId::new(1),
                term: Term::new(1),
                kind: MessageKind::AppendResp(AppendResponse {
                    success: true,
                    match_index: LogIndex::new(1),
                    conflict_term: None,
                    conflict_index: LogIndex::new(0),
                    read_round: 1,
                }),
            }))
            .expect("read-index acknowledgement");
        let grant = effects
            .into_iter()
            .find_map(|effect| match effect {
                NodeEffect::Send(
                    message @ Message {
                        kind: MessageKind::FollowerReadGrant { .. },
                        ..
                    },
                ) => Some(message),
                _ => None,
            })
            .expect("grant after quorum");
        let reply = follower
            .on_input(NodeInput::MessageAt {
                now: Time::from_nanos(40),
                message: grant,
            })
            .expect("grant delivery");
        assert!(matches!(
            reply.as_slice(),
            [NodeEffect::ReadReply {
                client,
                sequence: 9,
                reply: KvReply::Value(None),
            }] if *client == ClientId::new(7)
        ));
        assert_eq!(
            follower.take_follower_read_metadata(ClientId::new(7), 9),
            Some(FollowerReadMetadata {
                read_index: LogIndex::new(1),
                applied_index: LogIndex::new(1),
                applied_term: Term::new(1),
                read_time: Time::from_nanos(30),
            })
        );
    }

    #[test]
    fn trap_follower_read_tags_require_negotiated_feature_bit() {
        let voters = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let mut follower = Node::new(config(2), voters).expect("follower");
        follower.raft.role = Role::Follower;
        follower.raft.hard_state.term = Term::new(2);
        follower.raft.leader_id = Some(NodeId::new(1));
        follower
            .observe_peer_capability(NodeId::new(1), SEMANTIC_VERSION_V3, 0)
            .expect("featureless v3 connection");
        assert_eq!(
            follower.request_follower_read(
                ClientId::new(7),
                1,
                KvCommand::Get { key: b"k".to_vec() },
                Time::from_nanos(1),
            ),
            Err(NodeError::Environment("follower read feature unavailable"))
        );
        follower
            .observe_peer_capability(NodeId::new(1), SEMANTIC_VERSION_V3, FOLLOWER_READ_FEATURE)
            .expect("feature-bearing connection");
        assert!(
            follower
                .request_follower_read(
                    ClientId::new(7),
                    2,
                    KvCommand::Get { key: b"k".to_vec() },
                    Time::from_nanos(2),
                )
                .is_ok()
        );
        follower.forget_peer_capability(NodeId::new(1));
        assert!(follower.pending_follower_reads.is_empty());
        assert_eq!(
            follower.request_follower_read(
                ClientId::new(7),
                3,
                KvCommand::Get { key: b"k".to_vec() },
                Time::from_nanos(3),
            ),
            Err(NodeError::Environment("follower read capability unknown"))
        );
    }

    #[test]
    fn trap_follower_read_grant_requires_current_term() {
        let voters = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let mut follower = Node::new(config(2), voters).expect("follower");
        follower.raft.role = Role::Follower;
        follower.raft.hard_state.term = Term::new(2);
        follower.raft.leader_id = Some(NodeId::new(1));
        follower
            .observe_peer_capability(NodeId::new(1), SEMANTIC_VERSION_V3, FOLLOWER_READ_FEATURE)
            .expect("capability");
        follower
            .request_follower_read(
                ClientId::new(7),
                9,
                KvCommand::Get { key: b"k".to_vec() },
                Time::from_nanos(1),
            )
            .expect("request");
        let hash = follower
            .pending_follower_reads
            .get(&(ClientId::new(7), 9))
            .expect("pending")
            .command_hash;
        follower.accept_follower_read_grant(
            NodeId::new(1),
            Term::new(1),
            9,
            hash,
            LogIndex::new(0),
            Time::from_nanos(5),
        );
        assert_eq!(
            follower
                .pending_follower_reads
                .get(&(ClientId::new(7), 9))
                .expect("pending")
                .grant,
            None
        );
    }

    #[test]
    fn trap_follower_read_uses_leader_time_for_ttl() {
        let voters = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let mut follower = Node::new(config(2), voters).expect("follower");
        follower.raft.role = Role::Follower;
        follower.raft.hard_state.term = Term::new(2);
        follower.raft.leader_id = Some(NodeId::new(1));
        follower.raft.applied_index = LogIndex::new(1);
        follower.kv.apply_command_only(
            LogIndex::new(1),
            Term::new(2),
            KvCommand::Set {
                key: b"ttl".to_vec(),
                value: b"v".to_vec(),
                ttl: Some(Duration::from_secs(10)),
            },
            Time::from_nanos(0),
        );
        follower
            .observe_peer_capability(NodeId::new(1), SEMANTIC_VERSION_V3, FOLLOWER_READ_FEATURE)
            .expect("capability");
        follower
            .request_follower_read(
                ClientId::new(7),
                9,
                KvCommand::Ttl {
                    key: b"ttl".to_vec(),
                },
                Time::from_nanos(99_000_000_000),
            )
            .expect("request");
        let hash = follower
            .pending_follower_reads
            .get(&(ClientId::new(7), 9))
            .expect("pending")
            .command_hash;
        follower.accept_follower_read_grant(
            NodeId::new(1),
            Term::new(2),
            9,
            hash,
            LogIndex::new(1),
            Time::from_nanos(5_000_000_000),
        );
        assert!(matches!(
            follower.drain_pending_follower_reads().as_slice(),
            [NodeEffect::ReadReply {
                reply: KvReply::Integer(5),
                ..
            }]
        ));
    }

    #[test]
    fn trap_default_read_is_leader_read() {
        let voters = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let mut follower = Node::new(config(2), voters).expect("follower");
        follower.raft.role = Role::Follower;
        follower.raft.hard_state.term = Term::new(1);
        follower.raft.leader_id = Some(NodeId::new(1));
        assert_eq!(
            follower.on_input(NodeInput::Read {
                client: ClientId::new(7),
                sequence: 1,
                command: KvCommand::Get { key: b"k".to_vec() },
                at: Time::from_nanos(1),
            }),
            Err(NodeError::Raft(RaftError::NotLeader))
        );
    }

    #[test]
    fn trap_follower_apply_emits_no_client_route_reply() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        let effects = append_committed(
            &mut follower,
            LogIndex::new(0),
            vec![Entry {
                term: Term::new(1),
                index: LogIndex::new(1),
                kind: cc_raft::EntryKind::App,
                payload: AppEnvelope {
                    session: None,
                    leader_time: Time::from_nanos(1),
                    command: encode_command(&KvCommand::Ping),
                }
                .encode(),
            }],
        );
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, NodeEffect::ClientReply { .. }))
        );
    }

    #[test]
    fn trap_route_is_dropped_on_leadership_loss_or_truncation() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        node.on_input(NodeInput::ClientBytes {
            route_client: ClientId::new(7),
            route_req: 8,
            session: None,
            command: encode_command(&KvCommand::Ping),
            leader_time: Time::from_nanos(1),
        })
        .expect("request");
        complete_persistence(&mut node);
        let entry = node.raft.log.last().expect("entry").clone();
        assert_eq!(node.client_routes.len(), 1);
        let effects = node
            .on_input(NodeInput::Message(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(2),
                to: NodeId::new(1),
                term: Term::new(2),
                kind: MessageKind::AppendReq(cc_raft::AppendRequest {
                    prev_index: LogIndex::new(0),
                    prev_term: Term::new(0),
                    entries: vec![entry],
                    leader_commit: LogIndex::new(1),
                    read_round: 0,
                }),
            }))
            .expect("leadership loss");
        assert_eq!(node.role(), Role::Follower);
        assert!(node.client_routes.is_empty());
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, NodeEffect::ClientReply { .. }))
        );
    }

    fn vote_request_for(node: NodeId, term: Term) -> Message {
        Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: node,
            term,
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        }
    }

    #[test]
    fn trap_vote_reply_waits_for_successful_hard_state_fsync() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        let first = follower
            .on_input(NodeInput::Message(vote_request_for(
                NodeId::new(2),
                Term::new(1),
            )))
            .expect("vote request");
        assert!(matches!(first.as_slice(), [NodeEffect::PersistHard(_)]));
        assert_eq!(
            follower.on_input(NodeInput::Tick {
                now: Time::from_nanos(1),
            }),
            Err(NodeError::PersistencePending)
        );
        let second = follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("term durable");
        assert!(matches!(second.as_slice(), [NodeEffect::PersistHard(_)]));
        let final_effects = follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("vote durable");
        assert!(matches!(
            final_effects.as_slice(),
            [NodeEffect::Send(Message {
                kind: MessageKind::VoteResp { granted: true },
                ..
            })]
        ));
    }

    #[test]
    fn trap_append_reply_waits_for_successful_log_fsync() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        follower.raft.hard_state.term = Term::new(1);
        let first = follower
            .on_input(NodeInput::Message(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: Term::new(1),
                kind: MessageKind::AppendReq(cc_raft::AppendRequest {
                    prev_index: LogIndex::new(0),
                    prev_term: Term::new(0),
                    entries: vec![Entry {
                        term: Term::new(1),
                        index: LogIndex::new(1),
                        kind: cc_raft::EntryKind::Noop,
                        payload: Vec::new(),
                    }],
                    leader_commit: LogIndex::new(0),
                    read_round: 0,
                }),
            }))
            .expect("append request");
        assert!(matches!(first.as_slice(), [NodeEffect::PersistEntries(_)]));
        let final_effects = follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("append durable");
        assert!(matches!(
            final_effects.as_slice(),
            [NodeEffect::Send(Message {
                kind: MessageKind::AppendResp(_),
                ..
            })]
        ));
    }

    #[test]
    fn trap_failed_fsync_suppresses_continuation() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        follower
            .on_input(NodeInput::Message(vote_request_for(
                NodeId::new(2),
                Term::new(1),
            )))
            .expect("vote request");
        assert_eq!(
            follower.on_input(NodeInput::Persisted { success: false }),
            Err(NodeError::Durability)
        );
        assert_eq!(
            follower.on_input(NodeInput::Persisted { success: true }),
            Err(NodeError::UnexpectedPersistenceCompletion)
        );
    }

    #[test]
    fn trap_unknown_or_duplicate_io_completion_fails_closed() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        assert_eq!(
            follower.on_input(NodeInput::Persisted { success: true }),
            Err(NodeError::UnexpectedPersistenceCompletion)
        );
        follower
            .on_input(NodeInput::Message(vote_request_for(
                NodeId::new(2),
                Term::new(1),
            )))
            .expect("vote request");
        follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("first completion");
        follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("second completion");
        assert_eq!(
            follower.on_input(NodeInput::Persisted { success: true }),
            Err(NodeError::UnexpectedPersistenceCompletion)
        );
    }

    #[test]
    fn trap_node_processes_no_input_while_sync_io_is_blocked() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        follower
            .on_input(NodeInput::Message(vote_request_for(
                NodeId::new(2),
                Term::new(1),
            )))
            .expect("vote request");
        assert_eq!(
            follower.on_input(NodeInput::Timer {
                now: Time::from_nanos(1),
                kind: cc_raft::TimerKind::Election,
            }),
            Err(NodeError::PersistencePending)
        );
    }

    #[test]
    fn trap_effect_order_survives_two_barriers() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut follower = Node::new(config(2), voters).expect("node");
        let first = follower
            .on_input(NodeInput::Message(vote_request_for(
                NodeId::new(2),
                Term::new(1),
            )))
            .expect("vote request");
        let second = follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("term barrier");
        let third = follower
            .on_input(NodeInput::Persisted { success: true })
            .expect("vote barrier");
        assert!(matches!(first.as_slice(), [NodeEffect::PersistHard(_)]));
        assert!(matches!(second.as_slice(), [NodeEffect::PersistHard(_)]));
        assert!(matches!(third.as_slice(), [NodeEffect::Send(_)]));
    }

    #[test]
    fn node_starts_as_follower_and_rejects_writes_until_leader() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        assert_eq!(node.role(), Role::Follower);
        assert!(matches!(
            node.on_input(NodeInput::ClientRequest {
                client: ClientId::new(1),
                sequence: 1,
                command: KvCommand::Ping,
                leader_time: Time::from_nanos(0),
            }),
            Err(NodeError::NotLeader)
        ));
    }

    #[test]
    fn checkpoint_snapshot_restores_kv_and_applied_index() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut source = Node::new(config(1), voters.clone()).expect("node");
        source
            .kv
            .apply(
                LogIndex::new(5),
                Term::new(2),
                ClientId::new(1),
                1,
                KvCommand::Set {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                    ttl: None,
                },
                Time::from_nanos(1),
            )
            .expect("apply");
        source.raft.applied_index = LogIndex::new(5);
        let snapshot = source.create_snapshot().expect("snapshot");
        let mut restored = Node::new(config(2), voters).expect("node");
        restored.install_snapshot(snapshot).expect("install");
        assert_eq!(restored.kv.applied_index, LogIndex::new(5));
        assert_eq!(restored.kv.store.get(b"a", None), Some(b"one".to_vec()));
    }

    #[test]
    fn trap_ccsn_v1_round_trips_logical_state_and_rejects_corruption() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut source = Node::new(config(1), voters.clone()).expect("source");
        source.kv.apply_command_only(
            LogIndex::new(3),
            Term::new(2),
            KvCommand::Set {
                key: b"ccsn-key".to_vec(),
                value: b"ccsn-value".to_vec(),
                ttl: Some(Duration::from_secs(60)),
            },
            Time::from_nanos(10),
        );
        source.raft.applied_index = LogIndex::new(3);
        let session_key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(9))
            .expect("session key");
        assert_eq!(
            source.sessions.apply_user(
                source.config.policy,
                session_key,
                1,
                encode_command(&KvCommand::Ping),
                Time::from_nanos(10),
                || KvReply::Ok,
            ),
            KvReply::Ok
        );
        let bytes = source.encode_ccsn_snapshot().expect("encode CCSN");
        let decoded = decode_ccsn(&bytes, [7; 16], u64::MAX).expect("decode CCSN");
        assert_eq!(decoded.kv.entries.len(), 1);
        assert_eq!(decoded.sessions.records.len(), 1);
        let mut target = Node::new(config(2), voters).expect("target");
        target.install_ccsn_snapshot(&bytes).expect("install CCSN");
        assert_eq!(
            target.kv.store.get(b"ccsn-key", None),
            Some(b"ccsn-value".to_vec())
        );
        assert_eq!(
            target.kv.read(
                KvCommand::Ttl {
                    key: b"ccsn-key".to_vec()
                },
                Time::from_nanos(10)
            ),
            Ok(KvReply::Integer(60))
        );
        let mut corrupt = bytes;
        corrupt[CCSN_HEADER_LEN + 10] ^= 1;
        assert!(decode_ccsn(&corrupt, [7; 16], u64::MAX).is_err());
    }

    #[test]
    fn trap_decoded_ccsn_constructs_the_single_recovery_snapshot_value() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut source = Node::new(config(1), voters.clone()).expect("source");
        source.kv.apply_command_only(
            LogIndex::new(3),
            Term::new(2),
            KvCommand::Set {
                key: b"recovery-key".to_vec(),
                value: b"recovery-value".to_vec(),
                ttl: None,
            },
            Time::from_nanos(10),
        );
        source.raft.applied_index = LogIndex::new(3);
        let bytes = source.encode_ccsn_snapshot().expect("encode CCSN");
        let decoded = decode_ccsn(&bytes, [7; 16], u64::MAX).expect("decode CCSN");
        let snapshot = node_snapshot_from_ccsn(decoded, config(1).store).expect("recovery value");
        let restored = Node::restore(
            config(2),
            RecoveredNode {
                hard_state: HardState {
                    term: Term::new(0),
                    voted_for: None,
                },
                log_base: (snapshot.last_included_index, snapshot.last_included_term),
                entries: Vec::new(),
                membership: snapshot.membership.clone(),
                cluster_policy: snapshot.cluster_policy,
                snapshot: Some(snapshot),
                durable_applied: (LogIndex::new(3), Term::new(2)),
            },
        )
        .expect("restore CCSN recovery value");
        assert_eq!(
            restored.kv.store.get(b"recovery-key", None),
            Some(b"recovery-value".to_vec())
        );
    }

    #[test]
    fn trap_leadership_transfer_identity_survives_ccsn_compaction() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let mut source = Node::new(config(1), voters).expect("source");
        source.kv.apply_command_only(
            LogIndex::new(7),
            Term::new(2),
            KvCommand::Ping,
            Time::from_nanos(10),
        );
        source.raft.applied_index = LogIndex::new(7);
        let admin = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(77))
            .expect("admin");
        let transfer = LeadershipTransferState {
            intent_index: LogIndex::new(7),
            target: NodeId::new(2),
            deadline: Time::from_nanos(100),
            finishing: false,
            admin_session: Some((admin, 9)),
        };
        source
            .raft
            .restore_leadership_transfer(Some(transfer))
            .expect("transfer state");
        let bytes = source.encode_ccsn_snapshot().expect("checkpoint");
        let decoded = decode_ccsn(&bytes, [7; 16], u64::MAX).expect("decode");
        assert_eq!(decoded.leadership_transfer, Some(transfer));
        let restored = node_snapshot_from_ccsn(decoded, config(1).store).expect("node snapshot");
        assert_eq!(restored.leadership_transfer, Some(transfer));
    }

    #[test]
    fn trap_ccsn_stream_decoder_matches_complete_decoder_without_raw_image_buffer() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut source = Node::new(config(1), voters).expect("source");
        source.kv.apply_command_only(
            LogIndex::new(3),
            Term::new(2),
            KvCommand::Set {
                key: b"stream-key".to_vec(),
                value: vec![7; 8192],
                ttl: None,
            },
            Time::from_nanos(10),
        );
        source.raft.applied_index = LogIndex::new(3);
        let bytes = source.encode_ccsn_snapshot().expect("encode CCSN");
        let expected = decode_ccsn(&bytes, [7; 16], u64::MAX).expect("complete decode");
        let mut stream = source.begin_ccsn_decode();
        for chunk in bytes.chunks(31) {
            stream.push(chunk).expect("stream chunk");
        }
        let (actual, checksum) = stream.finish().expect("stream decode");
        assert_eq!(actual, expected);
        assert_eq!(checksum, ccsn_file_crc(&bytes).expect("CCSN checksum"));
    }

    #[test]
    fn trap_ccsn_stream_encoder_matches_the_frozen_canonical_bytes() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut source = Node::new(config(1), voters).expect("source");
        source.kv.apply_command_only(
            LogIndex::new(3),
            Term::new(2),
            KvCommand::Set {
                key: b"writer-key".to_vec(),
                value: vec![3; 8192],
                ttl: None,
            },
            Time::from_nanos(10),
        );
        source.raft.applied_index = LogIndex::new(3);
        let expected = source.encode_ccsn_snapshot().expect("complete encoding");
        let mut encoder = source.begin_ccsn_encode().expect("stream encoder");
        let mut streamed = Vec::new();
        while let Some(chunk) = encoder.next_chunk(37).expect("stream chunk") {
            assert!(chunk.len() <= 37);
            streamed.extend_from_slice(&chunk);
        }
        assert_eq!(streamed, expected);
        assert_eq!(encoder.total_len() as usize, streamed.len());
        assert_eq!(
            encoder.file_crc(),
            ccsn_file_crc(&streamed).expect("CCSN checksum")
        );
    }

    #[test]
    fn read_waits_for_quorum_barrier_and_applied_index() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        node.raft.log.push(Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: cc_raft::EntryKind::Noop,
            payload: Vec::new(),
        });
        node.raft.commit_index = LogIndex::new(1);
        node.raft.applied_index = LogIndex::new(1);
        let effects = node
            .on_input(NodeInput::Read {
                sequence: 1,
                client: ClientId::new(8),
                command: KvCommand::Get { key: b"a".to_vec() },
                at: Time::from_nanos(1),
            })
            .expect("read request");
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, NodeEffect::ReadReply { .. }))
        );
        // An ack that predates the read round is not evidence of current
        // leadership, so it must not release the barrier.
        let stale = node
            .on_input(NodeInput::Message(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(2),
                to: NodeId::new(1),
                term: Term::new(1),
                kind: MessageKind::AppendResp(AppendResponse {
                    success: true,
                    match_index: LogIndex::new(1),
                    conflict_term: None,
                    conflict_index: LogIndex::new(0),
                    read_round: 0,
                }),
            }))
            .expect("stale response");
        assert!(
            !stale
                .iter()
                .any(|effect| matches!(effect, NodeEffect::ReadReply { .. })),
            "a pre-read ack must not confirm the read quorum"
        );

        let effects = node
            .on_input(NodeInput::Message(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(2),
                to: NodeId::new(1),
                term: Term::new(1),
                kind: MessageKind::AppendResp(AppendResponse {
                    success: true,
                    match_index: LogIndex::new(1),
                    conflict_term: None,
                    conflict_index: LogIndex::new(0),
                    read_round: 1,
                }),
            }))
            .expect("quorum response");
        assert!(effects
            .iter()
            .any(|effect| matches!(effect, NodeEffect::ReadReply { client, .. } if *client == ClientId::new(8))));
    }

    #[cfg(not(feature = "kata05"))]
    #[test]
    fn trap_same_sequence_different_command_never_mutates() {
        let mut sessions = SessionTable::default();
        let key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(7))
            .expect("session key");
        let mut mutations = 0;
        let first = sessions.apply_user(
            ClusterPolicy::default(),
            key,
            1,
            b"first".to_vec(),
            Time::from_nanos(1),
            || {
                mutations += 1;
                KvReply::Ok
            },
        );
        assert_eq!(first, KvReply::Ok);
        let conflict = sessions.apply_user(
            ClusterPolicy::default(),
            key,
            1,
            b"other".to_vec(),
            Time::from_nanos(2),
            || {
                mutations += 1;
                KvReply::Ok
            },
        );
        assert_eq!(conflict, KvReply::Error(KvError::SequenceConflict));
        assert_eq!(mutations, 1);
    }

    #[cfg(feature = "kata05")]
    #[test]
    fn trap_kata_05_session_dedup_is_found_within_budget() {
        let mut sessions = SessionTable::default();
        let key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(7))
            .expect("session key");
        let first = sessions.apply_user(
            ClusterPolicy::default(),
            key,
            1,
            b"first".to_vec(),
            Time::from_nanos(1),
            || KvReply::Ok,
        );
        assert_eq!(first, KvReply::Ok);

        let replay = sessions.apply_user(
            ClusterPolicy::default(),
            key,
            1,
            b"different".to_vec(),
            Time::from_nanos(2),
            || KvReply::Error(KvError::InvalidInput),
        );
        assert_eq!(
            replay,
            KvReply::Ok,
            "the synthetic defect accepts a different command as a retry"
        );
    }

    fn expiry_leader(with_deadline: bool) -> Node {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        if with_deadline {
            let _ = node.kv.apply_command_only(
                LogIndex::new(1),
                Term::new(1),
                KvCommand::Set {
                    key: b"expiry".to_vec(),
                    value: b"value".to_vec(),
                    ttl: Some(Duration::from_nanos(1)),
                },
                Time::from_nanos(1),
            );
        }
        node
    }

    #[test]
    fn trap_expiry_is_replicated_not_local() {
        let mut node = expiry_leader(true);
        node.on_input(NodeInput::Tick {
            now: Time::from_nanos(2),
        })
        .expect("expiry tick");
        let entry = node.raft.log.last().expect("replicated expiry entry");
        let envelope = decode_proposal(entry).expect("envelope").expect("app");
        assert_eq!(
            decode_command(&envelope.command),
            Ok(KvCommand::PurgeExpired {
                up_to: Time::from_nanos(2)
            })
        );
        assert_eq!(node.kv.store.get(b"expiry", None), Some(b"value".to_vec()));
    }

    #[test]
    fn trap_only_one_expiry_sweep_is_inflight() {
        let mut node = expiry_leader(true);
        node.on_input(NodeInput::Tick {
            now: Time::from_nanos(2),
        })
        .expect("first expiry tick");
        let log_len = node.raft.log.len();
        assert!(node.expiry_sweep_inflight);
        assert_eq!(
            node.on_input(NodeInput::Tick {
                now: Time::from_nanos(3)
            }),
            Err(NodeError::PersistencePending)
        );
        assert_eq!(node.raft.log.len(), log_len);
    }

    #[test]
    fn trap_idle_cluster_proposes_no_sweeps() {
        let mut node = expiry_leader(false);
        node.on_input(NodeInput::Tick {
            now: Time::from_nanos(2),
        })
        .expect("idle tick");
        assert!(node.raft.log.is_empty());
        assert!(!node.expiry_sweep_inflight);
    }

    #[test]
    fn trap_expired_session_retry_does_not_apply() {
        let policy = ClusterPolicy {
            session_idle_ns: 1,
            session_retry_grace_ns: 10,
            ..ClusterPolicy::default()
        };
        let mut sessions = SessionTable::default();
        let key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(7))
            .expect("session key");
        let mut mutations = 0;
        let apply = |table: &mut SessionTable, sequence, at, mutations: &mut u64| {
            table.apply_user(policy, key, sequence, b"cmd".to_vec(), at, || {
                *mutations += 1;
                KvReply::Ok
            })
        };
        assert_eq!(
            apply(&mut sessions, 1, Time::from_nanos(0), &mut mutations),
            KvReply::Ok
        );
        assert_eq!(
            apply(&mut sessions, 1, Time::from_nanos(2), &mut mutations),
            KvReply::Error(KvError::SessionExpired)
        );
        assert_eq!(
            apply(&mut sessions, 2, Time::from_nanos(5), &mut mutations),
            KvReply::Error(KvError::SessionExpired)
        );
        assert_eq!(mutations, 1);
        assert_eq!(
            apply(&mut sessions, 2, Time::from_nanos(13), &mut mutations),
            KvReply::Ok
        );
        assert_eq!(mutations, 2);
    }

    #[test]
    fn trap_malformed_committed_proposal_fails_closed() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        let index = LogIndex::new(4);
        let error = node
            .map_effects(
                vec![RaftEffect::Apply(vec![Entry {
                    term: Term::new(1),
                    index,
                    kind: cc_raft::EntryKind::App,
                    payload: b"not-ccap".to_vec(),
                }])],
                None,
            )
            .expect_err("malformed committed app");
        assert_eq!(error, NodeError::MalformedCommittedEntry(index));
    }

    #[test]
    fn recovered_node_requires_exact_policy_and_snapshot_membership() {
        let membership = MembershipState::new(
            [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                .into_iter()
                .collect(),
        )
        .expect("membership");
        let source = Node::fresh(config(1), membership.clone()).expect("fresh");
        let mut altered = config(1).policy;
        altered.max_sessions = altered.max_sessions.saturating_sub(1);
        let result = Node::restore(
            NodeConfig {
                policy: altered,
                ..config(1)
            },
            RecoveredNode {
                hard_state: source.raft.hard_state,
                log_base: (LogIndex::new(0), Term::new(0)),
                entries: Vec::new(),
                membership,
                cluster_policy: config(1).policy,
                snapshot: None,
                durable_applied: (LogIndex::new(0), Term::new(0)),
            },
        );
        let Err(error) = result else {
            panic!("policy mismatch must fail");
        };
        assert_eq!(error, NodeError::Kv(KvError::InvalidInput));
    }

    #[test]
    fn trap_restore_refuses_unproven_applied_watermark() {
        let membership = MembershipState::new(
            [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                .into_iter()
                .collect(),
        )
        .expect("membership");
        let result = Node::restore(
            config(1),
            RecoveredNode {
                hard_state: HardState {
                    term: Term::new(1),
                    voted_for: None,
                },
                log_base: (LogIndex::new(0), Term::new(0)),
                entries: vec![Entry {
                    term: Term::new(1),
                    index: LogIndex::new(1),
                    kind: cc_raft::EntryKind::App,
                    payload: Vec::new(),
                }],
                membership,
                cluster_policy: config(1).policy,
                snapshot: None,
                durable_applied: (LogIndex::new(1), Term::new(1)),
            },
        );
        assert!(matches!(
            result,
            Err(NodeError::MalformedCommittedEntry(index)) if index == LogIndex::new(1)
        ));
    }

    fn append_committed(
        node: &mut Node,
        prev_index: LogIndex,
        entries: Vec<Entry>,
    ) -> Vec<NodeEffect> {
        let term = entries.first().map_or(Term::new(1), |entry| entry.term);
        let commit = entries.last().map_or(prev_index, |entry| entry.index);
        let mut output = node
            .on_input(NodeInput::MessageAt {
                now: Time::from_nanos(10),
                message: Message {
                    proto_version: PROTOCOL_VERSION,
                    from: NodeId::new(1),
                    to: node.id(),
                    term,
                    kind: MessageKind::AppendReq(cc_raft::AppendRequest {
                        prev_index,
                        prev_term: node.raft.term_at(prev_index).unwrap_or(Term::new(0)),
                        entries,
                        leader_commit: commit,
                        read_round: 0,
                    }),
                },
            })
            .expect("committed append");
        while node.continuation.is_some() {
            output.extend(
                node.on_input(NodeInput::Persisted { success: true })
                    .expect("durability completion"),
            );
        }
        output
    }

    fn complete_persistence(node: &mut Node) {
        while node.continuation.is_some() {
            node.on_input(NodeInput::Persisted { success: true })
                .expect("durability completion");
        }
    }

    #[test]
    fn trap_every_committed_entry_advances_both_applied_indices() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(2), voters).expect("node");
        let entry = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: cc_raft::EntryKind::App,
            payload: Node::proposal_bytes(
                ClientId::new(7),
                1,
                &KvCommand::Ping,
                Time::from_nanos(1),
            ),
        };
        append_committed(&mut node, LogIndex::new(0), vec![entry]);
        assert_eq!(node.raft.applied_index, LogIndex::new(1));
        assert_eq!(node.kv.applied_index, LogIndex::new(1));
        assert_eq!(node.raft.applied_index, node.kv.applied_index);
    }

    #[test]
    fn trap_duplicate_advances_index_without_reapplying() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(2), voters).expect("node");
        let payload = Node::proposal_bytes(
            ClientId::new(7),
            1,
            &KvCommand::Set {
                key: b"once".to_vec(),
                value: b"value".to_vec(),
                ttl: None,
            },
            Time::from_nanos(1),
        );
        append_committed(
            &mut node,
            LogIndex::new(0),
            vec![Entry {
                term: Term::new(1),
                index: LogIndex::new(1),
                kind: cc_raft::EntryKind::App,
                payload: payload.clone(),
            }],
        );
        let writes = node.kv.store.image().sequence;
        append_committed(
            &mut node,
            LogIndex::new(1),
            vec![Entry {
                term: Term::new(1),
                index: LogIndex::new(2),
                kind: cc_raft::EntryKind::App,
                payload,
            }],
        );
        assert_eq!(node.kv.store.image().sequence, writes);
        assert_eq!(node.raft.applied_index, LogIndex::new(2));
        assert_eq!(node.kv.applied_index, LogIndex::new(2));
    }

    #[test]
    fn trap_command_error_is_cached_and_applied() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(2), voters).expect("node");
        let set = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: cc_raft::EntryKind::App,
            payload: Node::proposal_bytes(
                ClientId::new(7),
                1,
                &KvCommand::Set {
                    key: b"counter".to_vec(),
                    value: b"not-a-number".to_vec(),
                    ttl: None,
                },
                Time::from_nanos(1),
            ),
        };
        append_committed(&mut node, LogIndex::new(0), vec![set]);
        let failing_payload = Node::proposal_bytes(
            ClientId::new(7),
            2,
            &KvCommand::Incr {
                key: b"counter".to_vec(),
                delta: 1,
            },
            Time::from_nanos(2),
        );
        append_committed(
            &mut node,
            LogIndex::new(1),
            vec![Entry {
                term: Term::new(1),
                index: LogIndex::new(2),
                kind: cc_raft::EntryKind::App,
                payload: failing_payload.clone(),
            }],
        );
        let session = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(7))
            .expect("session");
        assert_eq!(
            decode_reply(&node.sessions.records[&session].cached_reply),
            Ok(KvReply::Error(KvError::NotNumeric))
        );
        append_committed(
            &mut node,
            LogIndex::new(2),
            vec![Entry {
                term: Term::new(1),
                index: LogIndex::new(3),
                kind: cc_raft::EntryKind::App,
                payload: failing_payload,
            }],
        );
        assert_eq!(
            decode_reply(&node.sessions.records[&session].cached_reply),
            Ok(KvReply::Error(KvError::NotNumeric))
        );
        assert_eq!(node.raft.applied_index, LogIndex::new(3));
        assert_eq!(node.kv.applied_index, LogIndex::new(3));
    }

    #[test]
    fn trap_batch_dedup_is_one_unit() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(2), voters).expect("node");
        restore_atomic_batch_feature(&mut node);
        append_committed(
            &mut node,
            LogIndex::new(0),
            vec![Entry {
                term: Term::new(1),
                index: LogIndex::new(1),
                kind: cc_raft::EntryKind::App,
                payload: Node::proposal_bytes(
                    ClientId::new(7),
                    1,
                    &KvCommand::Set {
                        key: b"counter".to_vec(),
                        value: b"not-a-number".to_vec(),
                        ttl: None,
                    },
                    Time::from_nanos(1),
                ),
            }],
        );
        let payload = Node::proposal_bytes(
            ClientId::new(7),
            2,
            &KvCommand::Batch {
                commands: vec![
                    KvCommand::Set {
                        key: b"must-not-publish".to_vec(),
                        value: b"x".to_vec(),
                        ttl: None,
                    },
                    KvCommand::Incr {
                        key: b"counter".to_vec(),
                        delta: 1,
                    },
                ],
            },
            Time::from_nanos(2),
        );
        for index in [2, 3] {
            append_committed(
                &mut node,
                LogIndex::new(index - 1),
                vec![Entry {
                    term: Term::new(1),
                    index: LogIndex::new(index),
                    kind: cc_raft::EntryKind::App,
                    payload: payload.clone(),
                }],
            );
        }
        assert_eq!(node.kv.store.get(b"must-not-publish", None), None);
        let session = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(7))
            .expect("session");
        assert_eq!(
            decode_reply(&node.sessions.records[&session].cached_reply),
            Ok(KvReply::BatchError {
                failed_index: Some(1),
                error: KvError::NotNumeric,
            })
        );
        assert_eq!(node.raft.applied_index, LogIndex::new(3));
        assert_eq!(node.kv.applied_index, LogIndex::new(3));
    }

    #[test]
    fn trap_batch_respects_size_caps() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut limited = config(1);
        limited.policy.max_batch_commands = 1;
        let mut node = Node::new(limited, voters).expect("node");
        restore_atomic_batch_feature(&mut node);
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        let result = node.on_input(NodeInput::ClientRequest {
            client: ClientId::new(8),
            sequence: 1,
            command: KvCommand::Batch {
                commands: vec![KvCommand::Ping, KvCommand::Ping],
            },
            leader_time: Time::from_nanos(1),
        });
        assert_eq!(result, Err(NodeError::Kv(KvError::TooLarge)));
        assert!(node.raft.log.is_empty());
    }

    #[test]
    fn trap_empty_batch_does_not_consume_session_sequence() {
        let voters = [NodeId::new(1)].into_iter().collect();
        let mut node = Node::new(config(1), voters).expect("node");
        restore_atomic_batch_feature(&mut node);
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        let result = node.on_input(NodeInput::ClientRequest {
            client: ClientId::new(8),
            sequence: 7,
            command: KvCommand::Batch {
                commands: Vec::new(),
            },
            leader_time: Time::from_nanos(1),
        });
        assert_eq!(result, Err(NodeError::Kv(KvError::InvalidInput)));
        assert!(node.raft.log.is_empty());
        let session = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(8))
            .expect("session");
        assert!(!node.sessions.records.contains_key(&session));
    }

    #[test]
    fn trap_batch_activation_requires_every_learner_capability() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        node.raft.learners.insert(NodeId::new(4));
        for peer in [2, 3] {
            node.observe_peer_capability(
                NodeId::new(peer),
                SEMANTIC_VERSION_V3,
                cc_env::FEATURE_ATOMIC_BATCH,
            )
            .expect("voter capability");
        }
        let session = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(90))
            .expect("admin session");
        let activate = || ConfigOperation::ActivateFeature {
            feature: cc_core::ATOMIC_BATCH_FEATURE,
        };
        assert_eq!(
            node.admin_request(
                Time::from_nanos(1),
                ClientId::new(9),
                1,
                session,
                1,
                activate(),
            ),
            Err(NodeError::FeatureDisabled)
        );
        assert!(node.raft.log.is_empty());
        node.observe_peer_capability(
            NodeId::new(4),
            SEMANTIC_VERSION_V3,
            cc_env::FEATURE_ATOMIC_BATCH,
        )
        .expect("learner capability");
        assert!(
            node.admin_request(
                Time::from_nanos(2),
                ClientId::new(9),
                2,
                session,
                1,
                activate(),
            )
            .is_ok()
        );
    }

    #[test]
    fn trap_batch_requires_cluster_feature_activation() {
        let voters = [NodeId::new(1)].into_iter().collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        assert_eq!(
            node.on_input(NodeInput::ClientRequest {
                client: ClientId::new(8),
                sequence: 1,
                command: KvCommand::Batch {
                    commands: vec![KvCommand::Ping],
                },
                leader_time: Time::from_nanos(1),
            }),
            Err(NodeError::FeatureDisabled)
        );
        assert!(node.raft.log.is_empty());
    }

    #[test]
    fn trap_uncommitted_log_is_bounded_under_partition() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut limited = config(1);
        limited.host_limits.max_uncommitted_entries = 1;
        let mut node = Node::new(limited, voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        node.on_input(NodeInput::ClientRequest {
            client: ClientId::new(8),
            sequence: 1,
            command: KvCommand::Ping,
            leader_time: Time::from_nanos(1),
        })
        .expect("first proposal");
        complete_persistence(&mut node);
        assert_eq!(node.raft.commit_index, LogIndex::new(0));
        assert_eq!(
            node.on_input(NodeInput::ClientRequest {
                client: ClientId::new(8),
                sequence: 2,
                command: KvCommand::Ping,
                leader_time: Time::from_nanos(2),
            }),
            Err(NodeError::Kv(KvError::Busy))
        );
        assert_eq!(node.raft.log.len(), 1);
    }

    #[test]
    fn trap_busy_is_returned_before_memory_grows() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut limited = config(1);
        limited.host_limits.max_uncommitted_bytes = 64;
        let mut node = Node::new(limited, voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        let before = node.resource_usage();
        let result = node.on_input(NodeInput::ClientRequest {
            client: ClientId::new(8),
            sequence: 1,
            command: KvCommand::Set {
                key: b"key".to_vec(),
                value: vec![1; 128],
                ttl: None,
            },
            leader_time: Time::from_nanos(1),
        });
        assert_eq!(result, Err(NodeError::Kv(KvError::Busy)));
        assert_eq!(node.resource_usage(), before);
        assert!(node.raft.log.is_empty());
        assert!(node.client_routes.is_empty());
    }

    #[test]
    fn trap_pending_client_routes_are_bounded_and_dropped_on_stepdown() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut limited = config(1);
        limited.host_limits.max_pending_client_routes = 1;
        let mut node = Node::new(limited, voters).expect("node");
        node.raft.role = Role::Leader;
        node.raft.hard_state.term = Term::new(1);
        node.on_input(NodeInput::ClientRequest {
            client: ClientId::new(8),
            sequence: 1,
            command: KvCommand::Ping,
            leader_time: Time::from_nanos(1),
        })
        .expect("first route");
        complete_persistence(&mut node);
        assert_eq!(node.client_routes.len(), 1);
        assert_eq!(
            node.on_input(NodeInput::ClientRequest {
                client: ClientId::new(9),
                sequence: 1,
                command: KvCommand::Ping,
                leader_time: Time::from_nanos(2),
            }),
            Err(NodeError::Kv(KvError::Busy))
        );
        node.on_input(NodeInput::Message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: Term::new(2),
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        }))
        .expect("higher term");
        assert!(node.client_routes.is_empty());
    }

    #[test]
    fn trap_full_session_table_still_answers_duplicate() {
        let policy = ClusterPolicy {
            max_sessions: 1,
            ..ClusterPolicy::default()
        };
        let mut sessions = SessionTable::default();
        let key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(9))
            .expect("session");
        assert_eq!(
            sessions.apply_user(
                policy,
                key,
                1,
                b"same".to_vec(),
                Time::from_nanos(1),
                || KvReply::Integer(7),
            ),
            KvReply::Integer(7)
        );
        let mut reapplied = false;
        assert_eq!(
            sessions.apply_user(
                policy,
                key,
                1,
                b"same".to_vec(),
                Time::from_nanos(2),
                || {
                    reapplied = true;
                    KvReply::Integer(8)
                },
            ),
            KvReply::Integer(7)
        );
        assert!(!reapplied);
    }

    #[test]
    fn trap_active_session_is_never_evicted() {
        let policy = ClusterPolicy {
            max_sessions: 1,
            ..ClusterPolicy::default()
        };
        let mut sessions = SessionTable::default();
        let first =
            SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(1)).expect("first");
        let second =
            SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(2)).expect("second");
        assert_eq!(
            sessions.apply_user(
                policy,
                first,
                1,
                b"one".to_vec(),
                Time::from_nanos(1),
                || KvReply::Ok,
            ),
            KvReply::Ok
        );
        assert_eq!(
            sessions.apply_user(
                policy,
                second,
                1,
                b"two".to_vec(),
                Time::from_nanos(2),
                || KvReply::Ok,
            ),
            KvReply::Error(KvError::Busy)
        );
        assert!(sessions.records.contains_key(&first));
        assert!(!sessions.records.contains_key(&second));
    }

    #[test]
    fn trap_session_byte_limit_counts_request_and_reply() {
        let policy = ClusterPolicy {
            max_session_bytes: 7,
            max_reply_bytes: 4,
            ..ClusterPolicy::default()
        };
        let mut sessions = SessionTable::default();
        let key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(1))
            .expect("session");
        // The four-byte canonical request and the encoded reply are both
        // charged, so request bytes alone fitting is insufficient.
        assert_eq!(
            sessions.apply_user(
                policy,
                key,
                1,
                b"four".to_vec(),
                Time::from_nanos(1),
                || KvReply::Ok,
            ),
            KvReply::Error(KvError::Busy)
        );
        assert!(sessions.records.is_empty());
    }

    #[test]
    fn trap_session_table_is_bounded() {
        let policy = ClusterPolicy {
            max_sessions: 2,
            ..ClusterPolicy::default()
        };
        let mut sessions = SessionTable::default();
        for client in 1..=3 {
            let key = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(client))
                .expect("session");
            let reply = sessions.apply_user(
                policy,
                key,
                1,
                vec![client as u8],
                Time::from_nanos(1),
                || KvReply::Ok,
            );
            assert_eq!(
                reply,
                if client <= 2 {
                    KvReply::Ok
                } else {
                    KvReply::Error(KvError::Busy)
                }
            );
        }
        assert_eq!(sessions.records.len(), 2);
    }

    #[test]
    fn trap_pending_admin_workflow_session_cannot_expire() {
        let policy = ClusterPolicy {
            session_idle_ns: 10,
            session_retry_grace_ns: 5,
            ..ClusterPolicy::default()
        };
        let mut sessions = SessionTable::default();
        let key = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(11))
            .expect("admin session");
        assert_eq!(
            sessions.apply_user(
                policy,
                key,
                1,
                b"workflow".to_vec(),
                Time::from_nanos(1),
                || KvReply::Ok,
            ),
            KvReply::Ok
        );
        assert_eq!(
            sessions.expire_due(policy, Time::from_nanos(20), 10, Some(key)),
            0
        );
        assert!(sessions.records.contains_key(&key));
        assert_eq!(
            sessions.expire_due(policy, Time::from_nanos(20), 10, None),
            1
        );
        assert!(!sessions.records.contains_key(&key));
    }

    #[test]
    fn trap_expired_membership_request_cannot_restart_transition() {
        let policy = ClusterPolicy {
            session_idle_ns: 10,
            ..ClusterPolicy::default()
        };
        let key = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(12))
            .expect("admin session");
        let command = ConfigEnvelope {
            admin_session: Some((key, 1)),
            leader_time: Time::from_nanos(1),
            operation: ConfigOperation::BeginLeaderTransfer {
                target: NodeId::new(2),
            },
        }
        .encode();
        let cached = AdminReply {
            operation_tag: 6,
            result: AdminResultTag::TransferSuccess,
            source_index: LogIndex::new(7),
            detail: Vec::new(),
        }
        .encode();
        let mut sessions = SessionTable::default();
        sessions.records.insert(
            key,
            SessionRecord {
                max_seq: 1,
                canonical_command: command.clone(),
                cached_reply: cached,
                last_active: Time::from_nanos(1),
            },
        );
        let reply = sessions
            .preview_admin(policy, key, 1, &command, 6, Time::from_nanos(12))
            .expect("preview")
            .expect("terminal expiry reply");
        assert_eq!(reply.result, AdminResultTag::RequestExpired);
        assert_eq!(sessions.records.len(), 1, "preview must not re-execute");
    }

    #[test]
    fn trap_logical_state_charge_matches_encoded_ccsn_length() {
        let voters = [NodeId::new(1)].into_iter().collect();
        let mut node = Node::new(config(1), voters).expect("node");
        node.kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"charge".to_vec(),
                value: vec![9; 73],
                ttl: Some(Duration::from_nanos(10)),
            },
            Time::from_nanos(1),
        );
        node.raft.applied_index = LogIndex::new(1);
        assert_eq!(
            node.logical_state_charge().expect("charge"),
            node.encode_ccsn_snapshot().expect("CCSN").len() as u64
        );
    }

    #[test]
    fn trap_host_limits_cannot_change_committed_apply_result() {
        let voters: BTreeSet<NodeId> = [NodeId::new(1)].into_iter().collect();
        let mut left = Node::new(config(1), voters.clone()).expect("left");
        let mut right_config = config(1);
        right_config.host_limits.max_pending_reads = 1;
        right_config.host_limits.max_pending_read_bytes = 64;
        let mut right = Node::new(right_config, voters).expect("right");
        let entry = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: cc_raft::EntryKind::App,
            payload: Node::proposal_bytes(
                ClientId::new(7),
                1,
                &KvCommand::Set {
                    key: b"same".to_vec(),
                    value: b"answer".to_vec(),
                    ttl: None,
                },
                Time::from_nanos(1),
            ),
        };
        append_committed(&mut left, LogIndex::new(0), vec![entry.clone()]);
        append_committed(&mut right, LogIndex::new(0), vec![entry]);
        assert_eq!(
            left.kv.logical_snapshot(Time::from_nanos(1)),
            right.kv.logical_snapshot(Time::from_nanos(1))
        );
        assert_eq!(left.sessions, right.sessions);
    }

    #[test]
    fn trap_user_and_admin_session_namespaces_cannot_alias() {
        let mut sessions = SessionTable::default();
        let user = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(9))
            .expect("user key");
        let admin = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(9))
            .expect("admin key");
        for key in [user, admin] {
            assert_eq!(
                sessions.apply_user(
                    ClusterPolicy::default(),
                    key,
                    1,
                    b"same-number-different-protocol".to_vec(),
                    Time::from_nanos(1),
                    || KvReply::Ok,
                ),
                KvReply::Ok
            );
        }
        assert_eq!(sessions.records.len(), 2);
    }

    #[test]
    fn trap_user_and_admin_entries_share_one_atomic_session_table() {
        let mut sessions = SessionTable::default();
        let user = SessionKey::new(SessionNamespace::UserRequest as u8, ClientId::new(9))
            .expect("user key");
        let admin = SessionKey::new(SessionNamespace::AdminRequest as u8, ClientId::new(9))
            .expect("admin key");
        let policy = ClusterPolicy {
            max_sessions: 1,
            ..ClusterPolicy::default()
        };
        assert_eq!(
            sessions.apply_user(
                policy,
                user,
                1,
                b"user".to_vec(),
                Time::from_nanos(1),
                || KvReply::Ok,
            ),
            KvReply::Ok
        );
        assert_eq!(
            sessions.apply_user(
                policy,
                admin,
                1,
                b"admin".to_vec(),
                Time::from_nanos(1),
                || KvReply::Ok,
            ),
            KvReply::Error(KvError::Busy),
            "the one global table, not namespace-local limits, admits sessions"
        );
    }

    #[test]
    fn trap_forged_matching_policy_hash_cannot_bypass_exact_bytes() {
        let membership = MembershipState::new(
            [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                .into_iter()
                .collect(),
        )
        .expect("membership");
        let stored = config(1).policy;
        let forged_disk_hash = stored.hash();
        let mut different = stored;
        different.max_sessions = different.max_sessions.saturating_sub(1);
        assert_ne!(different.encode(), stored.encode());
        // Recovery intentionally has no hash-only admission path. A stale or
        // forged fast-fence value therefore cannot make different bytes pass.
        let result = Node::restore(
            NodeConfig {
                policy: different,
                ..config(1)
            },
            RecoveredNode {
                hard_state: HardState {
                    term: Term::new(0),
                    voted_for: None,
                },
                log_base: (LogIndex::new(0), Term::new(0)),
                entries: Vec::new(),
                membership,
                cluster_policy: stored,
                snapshot: None,
                durable_applied: (LogIndex::new(0), Term::new(0)),
            },
        );
        assert_eq!(forged_disk_hash, stored.hash());
        assert!(matches!(result, Err(NodeError::Kv(KvError::InvalidInput))));
    }
}
