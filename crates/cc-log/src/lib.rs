// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The single durable Raft-log vocabulary, independent of ambient I/O."]

use std::collections::BTreeMap;
use std::fmt;

use cc_core::{
    ClusterPolicy, Dec, DecodeError, Enc, LogIndex, MembershipState, Term, crc32c_zeroed_tail,
};
use cc_raft::{Entry, EntryKind, HardState};
use cc_wal::{LsnRange, RecordType, SegmentImage, Wal, WalConfig, WalError, recover};

pub const LOG_RECORD_MAGIC: u32 = u32::from_le_bytes(*b"CCLR");
pub const LOG_RECORD_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Origin {
    Bootstrap = 1,
    Join = 2,
    Restore = 3,
}

impl Origin {
    fn decode(value: u8) -> Result<Self, LogError> {
        match value {
            1 => Ok(Self::Bootstrap),
            2 => Ok(Self::Join),
            3 => Ok(Self::Restore),
            tag => Err(LogError::InvalidTag(tag)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Genesis {
    pub origin: Origin,
    pub cluster_id: [u8; 16],
    pub policy: ClusterPolicy,
    pub membership: MembershipState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotMark {
    pub index: LogIndex,
    pub term: Term,
    pub generation: u64,
    pub crc32c: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableRecord {
    Genesis(Box<Genesis>),
    Hard(HardState),
    Append(Entry),
    Truncate {
        from: LogIndex,
    },
    /// A locally-created checkpoint. Its position must already exist in the
    /// durable Raft log before the mark can retire that prefix.
    SnapshotMark(SnapshotMark),
    /// A checkpoint installed from a leader. A follower may not retain the
    /// covered log prefix, so the verified checkpoint itself is the durable
    /// proof of the supplied base position. Hosts validate the referenced
    /// file before accepting recovery from this record.
    InstalledSnapshotMark(SnapshotMark),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogState {
    pub genesis: Genesis,
    pub hard_state: HardState,
    pub base_index: LogIndex,
    pub base_term: Term,
    pub entries: Vec<Entry>,
    pub snapshot: Option<SnapshotMark>,
}

impl LogState {
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(self.base_index, |entry| entry.index)
    }

    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.base_term, |entry| entry.term)
    }

    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.base_index {
            return Some(self.base_term);
        }
        self.entries
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogPlan {
    pub lsn: LsnRange,
    pub record: DurableRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredLog {
    pub state: LogState,
    pub segments: Vec<SegmentImage>,
    pub torn_tail_truncated: bool,
}

/// Recovery result for the bounded length-prefixed record stream used at the
/// shared host boundary.  The stream has the same semantic records and
/// validation as [`Log::recover`]; it merely lets a host persist one Driver
/// write at a time without inventing a second record vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredRecordStream {
    pub state: LogState,
    pub bytes_consumed: u64,
    pub torn_tail_truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogError {
    Wal(WalError),
    Decode(DecodeError),
    InvalidTag(u8),
    Invalid(&'static str),
}

/// Canonically encode one semantic WAL record for a host-issued write.  The
/// `Log` planner remains responsible for validating transition order; this
/// helper lets the shared Driver persist the exact same vocabulary rather
/// than inventing a second host journal format.
pub fn encode_durable_record(record: &DurableRecord) -> Result<Vec<u8>, LogError> {
    encode_record(record)
}

/// Encode exactly one durable record for a host file.  A fixed little-endian
/// length prefix makes consecutive semantic records independently recoverable
/// and turns a short final write into a clearly bounded torn tail.
pub fn encode_framed_durable_record(record: &DurableRecord) -> Result<Vec<u8>, LogError> {
    let record = encode_record(record)?;
    let length = u32::try_from(record.len()).map_err(|_| LogError::Invalid("record too large"))?;
    let mut framed = Vec::with_capacity(4 + record.len());
    framed.extend_from_slice(&length.to_le_bytes());
    framed.extend_from_slice(&record);
    Ok(framed)
}

/// Recover a complete prefix of framed durable records.  A malformed record
/// inside the prefix is corruption; only an incomplete final length or body is
/// considered a torn tail.  This is deliberately public so a real host can
/// recover through `cc-log` rather than replaying an adapter-owned journal.
pub fn recover_framed_record_stream(bytes: &[u8]) -> Result<RecoveredRecordStream, LogError> {
    let mut cursor = 0_usize;
    let mut state: Option<LogState> = None;
    let mut torn_tail_truncated = false;
    while cursor < bytes.len() {
        let Some(length_bytes) = bytes.get(cursor..cursor.saturating_add(4)) else {
            torn_tail_truncated = true;
            break;
        };
        let length = usize::try_from(u32::from_le_bytes(
            length_bytes.try_into().expect("four-byte record length"),
        ))
        .map_err(|_| LogError::Invalid("record length overflow"))?;
        if length == 0 || length > cc_core::MAX_CODEC_BYTES {
            return Err(LogError::Invalid("record length"));
        }
        let body_start = cursor
            .checked_add(4)
            .ok_or(LogError::Invalid("record offset"))?;
        let body_end = body_start
            .checked_add(length)
            .ok_or(LogError::Invalid("record length overflow"))?;
        let Some(body) = bytes.get(body_start..body_end) else {
            torn_tail_truncated = true;
            break;
        };
        let record = decode_record(body)?;
        match (&mut state, record) {
            (None, DurableRecord::Genesis(genesis)) => {
                validate_genesis(&genesis)?;
                state = Some(LogState {
                    genesis: *genesis,
                    hard_state: HardState {
                        term: Term::new(0),
                        voted_for: None,
                    },
                    base_index: LogIndex::new(0),
                    base_term: Term::new(0),
                    entries: Vec::new(),
                    snapshot: None,
                });
            }
            (None, _) => return Err(LogError::Invalid("genesis must be first")),
            (Some(_), DurableRecord::Genesis(_)) => {
                return Err(LogError::Invalid("multiple genesis records"));
            }
            (Some(state), record) => apply_record(state, &record)?,
        }
        cursor = body_end;
    }
    let state = state.ok_or(LogError::Invalid("missing genesis"))?;
    Ok(RecoveredRecordStream {
        state,
        bytes_consumed: u64::try_from(cursor).unwrap_or(u64::MAX),
        torn_tail_truncated,
    })
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal(error) => write!(f, "WAL: {error}"),
            Self::Decode(error) => write!(f, "log decode: {error}"),
            Self::InvalidTag(tag) => write!(f, "unknown durable log record tag {tag}"),
            Self::Invalid(reason) => write!(f, "invalid durable log history: {reason}"),
        }
    }
}

impl std::error::Error for LogError {}

impl From<WalError> for LogError {
    fn from(value: WalError) -> Self {
        Self::Wal(value)
    }
}

impl From<DecodeError> for LogError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

/// Durable-log planner.  It owns semantic record validation and WAL bytes;
/// callers map the returned plans to their own `cc_env` write/fsync effects.
pub struct Log {
    wal: Wal,
    /// State known to be behind a completed WAL fsync.
    state: LogState,
    /// Tentative state reflected by queued WAL writes.  The owner may use it
    /// to plan a subsequent write, but it is never returned as recovered
    /// durable state before `commit` publishes the matching barrier.
    staged: LogState,
}

impl Log {
    pub fn fresh(config: WalConfig, genesis: Genesis) -> Result<(Self, LogPlan), LogError> {
        validate_genesis(&genesis)?;
        let state = LogState {
            genesis: genesis.clone(),
            hard_state: HardState {
                term: Term::new(0),
                voted_for: None,
            },
            base_index: LogIndex::new(0),
            base_term: Term::new(0),
            entries: Vec::new(),
            snapshot: None,
        };
        let mut log = Self {
            wal: Wal::new(config)?,
            staged: state.clone(),
            state,
        };
        let plan = log.append_record(DurableRecord::Genesis(Box::new(genesis)))?;
        Ok((log, plan))
    }

    pub fn recover(images: &[SegmentImage], config: WalConfig) -> Result<RecoveredLog, LogError> {
        // Hosts enumerate directories in filesystem order, which is not a
        // recovery order. Segment sequence is authoritative; duplicates and
        // gaps still fail in `cc-wal` after this canonical ordering step.
        let mut ordered = images.to_vec();
        ordered.sort_by_key(|image| image.sequence);
        let recovered = recover(&ordered, config)?;
        let mut state: Option<LogState> = None;
        for raw in recovered.records {
            if raw.kind != RecordType::Data {
                continue;
            }
            let record = decode_record(&raw.payload)?;
            match (&mut state, record) {
                (None, DurableRecord::Genesis(genesis)) => {
                    validate_genesis(&genesis)?;
                    state = Some(LogState {
                        genesis: *genesis,
                        hard_state: HardState {
                            term: Term::new(0),
                            voted_for: None,
                        },
                        base_index: LogIndex::new(0),
                        base_term: Term::new(0),
                        entries: Vec::new(),
                        snapshot: None,
                    });
                }
                (None, _) => return Err(LogError::Invalid("genesis must be first")),
                (Some(_), DurableRecord::Genesis(_)) => {
                    return Err(LogError::Invalid("multiple genesis records"));
                }
                (Some(state), record) => apply_record(state, &record)?,
            }
        }
        let state = state.ok_or(LogError::Invalid("missing genesis"))?;
        Ok(RecoveredLog {
            state,
            segments: recovered.segments,
            torn_tail_truncated: recovered.truncated_at.is_some(),
        })
    }

    /// Recover only when the sealed genesis exactly matches the locally
    /// configured identity.  A CCID hash is a fast fence, not authority: the
    /// complete policy and initial membership bytes must agree before the
    /// host can expose a recovered node.
    pub fn recover_expected(
        images: &[SegmentImage],
        config: WalConfig,
        expected: &Genesis,
    ) -> Result<RecoveredLog, LogError> {
        validate_genesis(expected)?;
        let recovered = Self::recover(images, config)?;
        if recovered.state.genesis != *expected {
            return Err(LogError::Invalid(
                "genesis does not match recovery configuration",
            ));
        }
        Ok(recovered)
    }

    /// Recovery-side proof that an on-log snapshot mark refers to an already
    /// durable snapshot artifact.  Hosts obtain this bounded generation/CRC
    /// map while validating their logical snapshot files before constructing
    /// a recovered node.
    pub fn recover_with_snapshots(
        images: &[SegmentImage],
        config: WalConfig,
        snapshots: &BTreeMap<u64, u32>,
    ) -> Result<RecoveredLog, LogError> {
        let recovered = Self::recover(images, config)?;
        if let Some(mark) = recovered.state.snapshot
            && snapshots.get(&mark.generation) != Some(&mark.crc32c)
        {
            return Err(LogError::Invalid("snapshot mark lacks durable snapshot"));
        }
        Ok(recovered)
    }

    #[must_use]
    pub fn state(&self) -> &LogState {
        &self.state
    }

    #[must_use]
    pub fn images(&self) -> Vec<SegmentImage> {
        self.wal.segment_images()
    }

    #[must_use]
    pub fn durable_images(&self) -> Vec<SegmentImage> {
        self.wal.durable_images()
    }

    pub fn set_hard(&mut self, hard: HardState) -> Result<LogPlan, LogError> {
        self.append_record(DurableRecord::Hard(hard))
    }

    pub fn append(&mut self, entry: Entry) -> Result<LogPlan, LogError> {
        self.append_record(DurableRecord::Append(entry))
    }

    pub fn truncate_suffix(&mut self, from: LogIndex) -> Result<LogPlan, LogError> {
        self.append_record(DurableRecord::Truncate { from })
    }

    pub fn mark_snapshot(&mut self, mark: SnapshotMark) -> Result<LogPlan, LogError> {
        self.append_record(DurableRecord::SnapshotMark(mark))
    }

    pub fn commit(&mut self) -> Option<LsnRange> {
        let committed = self.wal.commit()?;
        self.state.clone_from(&self.staged);
        Some(committed)
    }

    fn append_record(&mut self, record: DurableRecord) -> Result<LogPlan, LogError> {
        if !matches!(record, DurableRecord::Genesis(_)) {
            apply_record(&mut self.staged, &record)?;
        }
        let bytes = encode_record(&record)?;
        let lsn = self.wal.append(RecordType::Data, &bytes)?;
        Ok(LogPlan { lsn, record })
    }
}

fn validate_genesis(genesis: &Genesis) -> Result<(), LogError> {
    if genesis.cluster_id.iter().all(|byte| *byte == 0) {
        return Err(LogError::Invalid("zero cluster id"));
    }
    genesis.policy.validate()?;
    genesis.membership.validate()?;
    Ok(())
}

fn apply_record(state: &mut LogState, record: &DurableRecord) -> Result<(), LogError> {
    match record {
        DurableRecord::Genesis(_) => return Err(LogError::Invalid("duplicate genesis")),
        DurableRecord::Hard(hard) => {
            if hard.term < state.hard_state.term {
                return Err(LogError::Invalid("hard-state term regressed"));
            }
            if hard.term == state.hard_state.term
                && state.hard_state.voted_for.is_some()
                && hard.voted_for.is_some()
                && state.hard_state.voted_for != hard.voted_for
            {
                return Err(LogError::Invalid("vote changed in one term"));
            }
            state.hard_state = *hard;
        }
        DurableRecord::Append(entry) => {
            if entry.index.get() == 0
                || entry.index != LogIndex::new(state.last_index().get().saturating_add(1))
            {
                return Err(LogError::Invalid("append index is not contiguous"));
            }
            state.entries.push(entry.clone());
        }
        DurableRecord::Truncate { from } => {
            if *from <= state.base_index {
                return Err(LogError::Invalid("truncate crosses log base"));
            }
            state.entries.retain(|entry| entry.index < *from);
        }
        DurableRecord::SnapshotMark(mark) | DurableRecord::InstalledSnapshotMark(mark) => {
            let installed = matches!(record, DurableRecord::InstalledSnapshotMark(_));
            if mark.index.get() == 0
                || mark.index < state.base_index
                || state
                    .snapshot
                    .is_some_and(|prior| mark.index <= prior.index)
                || (!installed && state.term_at(mark.index) != Some(mark.term))
                || (installed
                    && state
                        .term_at(mark.index)
                        .is_some_and(|term| term != mark.term))
            {
                return Err(LogError::Invalid("invalid snapshot mark"));
            }
            state.entries.retain(|entry| entry.index > mark.index);
            state.base_index = mark.index;
            state.base_term = mark.term;
            state.snapshot = Some(*mark);
        }
    }
    Ok(())
}

fn encode_record(record: &DurableRecord) -> Result<Vec<u8>, LogError> {
    let mut enc = Enc::new();
    enc.header(LOG_RECORD_MAGIC, LOG_RECORD_VERSION);
    match record {
        DurableRecord::Genesis(genesis) => {
            validate_genesis(genesis)?;
            enc.u8(1);
            enc.u8(genesis.origin as u8);
            for byte in genesis.cluster_id {
                enc.u8(byte);
            }
            enc.bytes(&genesis.policy.encode());
            enc.bytes(&genesis.membership.encode()?);
        }
        DurableRecord::Hard(hard) => {
            enc.u8(2);
            enc.u64(hard.term.get());
            enc.u64(hard.voted_for.map_or(0, cc_core::NodeId::get));
        }
        DurableRecord::Append(entry) => {
            if entry.index.get() == 0 {
                return Err(LogError::Invalid("zero append index"));
            }
            if matches!(entry.kind, EntryKind::AppV3 | EntryKind::ConfigV3) {
                enc.u8(7);
                encode_entry_v3(&mut enc, entry)?;
            } else {
                enc.u8(3);
                encode_entry(&mut enc, entry)?;
            }
        }
        DurableRecord::Truncate { from } => {
            if from.get() == 0 {
                return Err(LogError::Invalid("zero truncate index"));
            }
            enc.u8(4);
            enc.u64(from.get());
        }
        DurableRecord::SnapshotMark(mark) => {
            if mark.index.get() == 0 {
                return Err(LogError::Invalid("zero snapshot index"));
            }
            enc.u8(5);
            enc.u64(mark.index.get());
            enc.u64(mark.term.get());
            enc.u64(mark.generation);
            enc.u32(mark.crc32c);
        }
        DurableRecord::InstalledSnapshotMark(mark) => {
            if mark.index.get() == 0 {
                return Err(LogError::Invalid("zero snapshot index"));
            }
            enc.u8(6);
            enc.u64(mark.index.get());
            enc.u64(mark.term.get());
            enc.u64(mark.generation);
            enc.u32(mark.crc32c);
        }
    }
    let mut bytes = enc.finish();
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c_zeroed_tail(&bytes);
    let start = bytes.len() - 4;
    bytes[start..].copy_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn decode_record(bytes: &[u8]) -> Result<DurableRecord, LogError> {
    if bytes.len() < 4 {
        return Err(LogError::Invalid("truncated record checksum"));
    }
    let body_len = bytes.len() - 4;
    let expected = u32::from_le_bytes(bytes[body_len..].try_into().expect("record CRC"));
    if crc32c_zeroed_tail(bytes) != expected {
        return Err(LogError::Invalid("record checksum"));
    }
    let mut dec = Dec::new(&bytes[..body_len]);
    dec.header(LOG_RECORD_MAGIC, LOG_RECORD_VERSION)?;
    let record = match dec.u8()? {
        1 => {
            let origin = Origin::decode(dec.u8()?)?;
            let mut cluster_id = [0_u8; 16];
            for byte in &mut cluster_id {
                *byte = dec.u8()?;
            }
            let policy = ClusterPolicy::decode(&dec.bytes()?)?;
            let membership = MembershipState::decode(&dec.bytes()?)?;
            DurableRecord::Genesis(Box::new(Genesis {
                origin,
                cluster_id,
                policy,
                membership,
            }))
        }
        2 => {
            let term = Term::new(dec.u64()?);
            let voter = dec.u64()?;
            DurableRecord::Hard(HardState {
                term,
                voted_for: (voter != 0).then(|| cc_core::NodeId::new(voter)),
            })
        }
        3 => DurableRecord::Append(decode_entry(&mut dec)?),
        4 => {
            let from = LogIndex::new(dec.u64()?);
            if from.get() == 0 {
                return Err(LogError::Invalid("zero truncate index"));
            }
            DurableRecord::Truncate { from }
        }
        5 => {
            let index = LogIndex::new(dec.u64()?);
            if index.get() == 0 {
                return Err(LogError::Invalid("zero snapshot index"));
            }
            DurableRecord::SnapshotMark(SnapshotMark {
                index,
                term: Term::new(dec.u64()?),
                generation: dec.u64()?,
                crc32c: dec.u32()?,
            })
        }
        6 => {
            let index = LogIndex::new(dec.u64()?);
            if index.get() == 0 {
                return Err(LogError::Invalid("zero snapshot index"));
            }
            DurableRecord::InstalledSnapshotMark(SnapshotMark {
                index,
                term: Term::new(dec.u64()?),
                generation: dec.u64()?,
                crc32c: dec.u32()?,
            })
        }
        7 => DurableRecord::Append(decode_entry_v3(&mut dec)?),
        tag => return Err(LogError::InvalidTag(tag)),
    };
    dec.finish()?;
    Ok(record)
}

fn encode_entry(enc: &mut Enc, entry: &Entry) -> Result<(), LogError> {
    if entry.payload.len() > cc_core::MAX_CODEC_BYTES {
        return Err(LogError::Invalid("entry payload too large"));
    }
    enc.u64(entry.term.get());
    enc.u64(entry.index.get());
    enc.u8(entry.kind as u8);
    enc.bytes(&entry.payload);
    Ok(())
}

fn decode_entry(dec: &mut Dec<'_>) -> Result<Entry, LogError> {
    let term = Term::new(dec.u64()?);
    let index = LogIndex::new(dec.u64()?);
    if index.get() == 0 {
        return Err(LogError::Invalid("zero entry index"));
    }
    let kind = match dec.u8()? {
        1 => EntryKind::App,
        2 => EntryKind::Noop,
        3 => EntryKind::Config,
        tag => return Err(LogError::InvalidTag(tag)),
    };
    Ok(Entry {
        term,
        index,
        kind,
        payload: dec.bytes()?,
    })
}

fn encode_entry_v3(enc: &mut Enc, entry: &Entry) -> Result<(), LogError> {
    if entry.payload.len() > cc_core::MAX_CODEC_BYTES {
        return Err(LogError::Invalid("entry payload too large"));
    }
    let (required_features, kind) = match entry.kind {
        EntryKind::AppV3 => (cc_core::ATOMIC_BATCH_FEATURE, EntryKind::App),
        EntryKind::ConfigV3 => (0, EntryKind::Config),
        _ => return Err(LogError::Invalid("v3 entry kind")),
    };
    enc.u64(entry.term.get());
    enc.u64(entry.index.get());
    enc.u16(cc_raft::SEMANTIC_VERSION_V3);
    enc.u64(required_features);
    enc.u8(kind as u8);
    enc.bytes(&entry.payload);
    Ok(())
}

fn decode_entry_v3(dec: &mut Dec<'_>) -> Result<Entry, LogError> {
    let term = Term::new(dec.u64()?);
    let index = LogIndex::new(dec.u64()?);
    if index.get() == 0 || dec.u16()? != cc_raft::SEMANTIC_VERSION_V3 {
        return Err(LogError::Invalid("v3 entry header"));
    }
    let required_features = dec.u64()?;
    let kind = match (dec.u8()?, required_features) {
        (1, cc_core::ATOMIC_BATCH_FEATURE) => EntryKind::AppV3,
        (3, 0) => EntryKind::ConfigV3,
        _ => return Err(LogError::Invalid("v3 entry requirements")),
    };
    Ok(Entry {
        term,
        index,
        kind,
        payload: dec.bytes()?,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn genesis() -> Genesis {
        Genesis {
            origin: Origin::Bootstrap,
            cluster_id: [7; 16],
            policy: ClusterPolicy::default(),
            membership: MembershipState::new(
                [
                    cc_core::NodeId::new(1),
                    cc_core::NodeId::new(2),
                    cc_core::NodeId::new(3),
                ]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            )
            .expect("membership"),
        }
    }

    fn config() -> WalConfig {
        WalConfig {
            segment_size: 512,
            max_record_size: 256,
        }
    }

    #[test]
    fn trap_log_recovery_is_idempotent() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.set_hard(HardState {
            term: Term::new(2),
            voted_for: Some(cc_core::NodeId::new(2)),
        })
        .expect("hard");
        log.append(Entry {
            term: Term::new(2),
            index: LogIndex::new(1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        })
        .expect("append");
        log.commit().expect("commit");
        let once = Log::recover(&log.durable_images(), config()).expect("recover");
        let twice = Log::recover(&once.segments, config()).expect("recover again");
        assert_eq!(once.state, twice.state);
        assert_eq!(once.segments, twice.segments);
    }

    #[test]
    fn trap_log_recovery_sorts_enumerated_segments_by_sequence() {
        let mut small_config = config();
        small_config.segment_size = 256;
        let (mut log, _) = Log::fresh(small_config, genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        for index in 1..=4 {
            log.append(Entry {
                term: Term::new(1),
                index: LogIndex::new(index),
                kind: EntryKind::App,
                payload: vec![index as u8; 100],
            })
            .expect("append");
        }
        log.commit().expect("append commit");
        let ordered = log.durable_images();
        assert!(ordered.len() > 1, "fixture must roll WAL segments");
        let mut enumerated = ordered.clone();
        enumerated.reverse();
        let canonical = Log::recover(&ordered, small_config).expect("ordered recovery");
        let recovered = Log::recover(&enumerated, small_config).expect("sorted recovery");
        assert_eq!(recovered.state, canonical.state);
        assert_eq!(recovered.segments, canonical.segments);
    }

    #[test]
    fn trap_log_write_is_not_published_before_fsync() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        log.append(Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        })
        .expect("append");
        assert_eq!(log.state().last_index(), LogIndex::new(0));
        log.commit().expect("append fsync");
        assert_eq!(log.state().last_index(), LogIndex::new(1));
    }

    #[test]
    fn trap_log_torn_tail_is_prefix_safe() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        log.append(Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        })
        .expect("append");
        let mut images = log.images();
        images.last_mut().expect("segment").bytes.pop();
        let recovered = Log::recover(&images, config()).expect("tail recovery");
        assert_eq!(recovered.state.last_index(), LogIndex::new(0));
        assert!(recovered.torn_tail_truncated);
    }

    #[test]
    fn trap_log_midsegment_corruption_fails_closed() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        for (index, payload) in [(1, 0xa5_u8), (2, 0xb6_u8)] {
            log.append(Entry {
                term: Term::new(1),
                index: LogIndex::new(index),
                kind: EntryKind::App,
                payload: vec![payload],
            })
            .expect("append");
        }
        log.commit().expect("append commit");
        let mut images = log.durable_images();
        let position = images[0]
            .bytes
            .iter()
            .position(|byte| *byte == 0xa5)
            .expect("unique first payload");
        images[0].bytes[position] ^= 1;
        assert!(matches!(
            Log::recover(&images, config()),
            Err(LogError::Wal(WalError::MidLogCorruption { .. }))
        ));
    }

    #[test]
    fn trap_framed_host_stream_recovers_a_prefix_after_torn_tail() {
        let (mut log, genesis_plan) = Log::fresh(config(), genesis()).expect("fresh");
        let hard_plan = log
            .set_hard(HardState {
                term: Term::new(2),
                voted_for: Some(cc_core::NodeId::new(2)),
            })
            .expect("hard state");
        let append_plan = log
            .append(Entry {
                term: Term::new(2),
                index: LogIndex::new(1),
                kind: EntryKind::Noop,
                payload: Vec::new(),
            })
            .expect("append");
        let mut bytes = Vec::new();
        for plan in [genesis_plan, hard_plan, append_plan] {
            bytes.extend(encode_framed_durable_record(&plan.record).expect("frame record"));
        }
        let complete_len = bytes.len();
        bytes.extend_from_slice(&[9, 0, 0]);

        let recovered = recover_framed_record_stream(&bytes).expect("recover framed prefix");
        assert_eq!(recovered.bytes_consumed, complete_len as u64);
        assert!(recovered.torn_tail_truncated);
        assert_eq!(recovered.state.hard_state.term, Term::new(2));
        assert_eq!(recovered.state.last_index(), LogIndex::new(1));
    }

    #[test]
    fn trap_framed_host_stream_rejects_complete_corruption() {
        let (_, genesis_plan) = Log::fresh(config(), genesis()).expect("fresh");
        let mut bytes = encode_framed_durable_record(&genesis_plan.record).expect("frame record");
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(recover_framed_record_stream(&bytes).is_err());
    }

    #[test]
    fn trap_lower_hard_term_is_corruption() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.set_hard(HardState {
            term: Term::new(2),
            voted_for: None,
        })
        .expect("term two");
        assert!(matches!(
            log.set_hard(HardState {
                term: Term::new(1),
                voted_for: None,
            }),
            Err(LogError::Invalid("hard-state term regressed"))
        ));
    }

    #[test]
    fn trap_vote_change_in_one_term_is_corruption() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.set_hard(HardState {
            term: Term::new(2),
            voted_for: Some(cc_core::NodeId::new(1)),
        })
        .expect("first vote");
        assert!(matches!(
            log.set_hard(HardState {
                term: Term::new(2),
                voted_for: Some(cc_core::NodeId::new(2)),
            }),
            Err(LogError::Invalid("vote changed in one term"))
        ));
    }

    #[test]
    fn trap_snapshot_mark_requires_durable_log_position() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        assert!(matches!(
            log.mark_snapshot(SnapshotMark {
                index: LogIndex::new(1),
                term: Term::new(1),
                generation: 1,
                crc32c: 0,
            }),
            Err(LogError::Invalid("invalid snapshot mark"))
        ));
    }

    #[test]
    fn trap_snapshot_mark_requires_durable_snapshot() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        log.append(Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        })
        .expect("append");
        log.commit().expect("append commit");
        log.mark_snapshot(SnapshotMark {
            index: LogIndex::new(1),
            term: Term::new(1),
            generation: 9,
            crc32c: 0x55aa_1234,
        })
        .expect("mark plan");
        log.commit().expect("mark commit");
        assert!(matches!(
            Log::recover_with_snapshots(&log.durable_images(), config(), &BTreeMap::new()),
            Err(LogError::Invalid("snapshot mark lacks durable snapshot"))
        ));
        let snapshots = [(9, 0x55aa_1234)].into_iter().collect();
        assert!(Log::recover_with_snapshots(&log.durable_images(), config(), &snapshots).is_ok());
    }

    #[test]
    fn trap_installed_snapshot_mark_can_rebase_a_follower_without_old_entries() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        let mark = SnapshotMark {
            index: LogIndex::new(7),
            term: Term::new(3),
            generation: 7,
            crc32c: 0x55aa_1234,
        };
        log.append_record(DurableRecord::InstalledSnapshotMark(mark))
            .expect("installed mark");
        log.commit().expect("mark commit");
        let snapshots = [(7, 0x55aa_1234)].into_iter().collect();
        let recovered = Log::recover_with_snapshots(&log.durable_images(), config(), &snapshots)
            .expect("marked snapshot recovery");
        assert_eq!(recovered.state.base_index, mark.index);
        assert_eq!(recovered.state.base_term, mark.term);
        assert_eq!(recovered.state.snapshot, Some(mark));
    }

    #[test]
    fn trap_genesis_policy_must_match_ccid_and_recovery_config() {
        let (mut log, _) = Log::fresh(config(), genesis()).expect("fresh");
        log.commit().expect("genesis commit");
        let mut expected = genesis();
        expected.cluster_id[0] = 8;
        assert!(matches!(
            Log::recover_expected(&log.durable_images(), config(), &expected),
            Err(LogError::Invalid(
                "genesis does not match recovery configuration"
            ))
        ));
    }
}
