// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! CCIJ v1: paired host input/effect records for deterministic replay.

use std::collections::VecDeque;
use std::fmt;

use crate::{BootState, Driver, HostError};

use cc_cluster::{NodeConfig, RaftConfig, RecoveredNode};
use cc_core::{
    ClusterPolicy, Dec, DecodeError, Duration, Enc, HostLimits, MembershipState, NodeId, Seed,
    Time, crc32c,
};
use cc_env::{
    BoundaryCodecError, Effect, FileId, Input, decode_effect, decode_file_id, decode_input,
    encode_effect, encode_file_id, encode_input,
};
use cc_log::{LogError, recover_framed_record_stream};
use cc_store::{BlockRead, BlockReadError, BlockSource, StoreConfig, StoreError};

pub const INPUT_JOURNAL_MAGIC: u32 = u32::from_le_bytes(*b"CCIJ");
pub const INPUT_JOURNAL_VERSION: u16 = 1;
pub const MAX_JOURNAL_RECORD_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_EFFECTS_PER_RECORD: usize = 4_096;
pub const MAX_BLOCK_OBSERVATIONS_PER_RECORD: usize = 4_096;
pub const MAX_BOOT_IMAGE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_BOOT_BUILD_LABEL_BYTES: usize = 256;
pub const DRIVER_BOOT_MAGIC: u32 = u32::from_le_bytes(*b"CCBI");
pub const DRIVER_BOOT_VERSION: u16 = 3;
const FOOTER_FRAME_BIT: u32 = 1 << 31;
const FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"CCIF");
const FOOTER_VERSION: u16 = 1;
const MAX_FOOTER_BYTES: usize = 64;

/// The host-neutral image required to rebuild the current real Driver without
/// consulting an ambient config file. The embedded WAL remains authoritative
/// for durable Raft state; the copied identity/configuration/membership fields
/// fence that recovery and retain its exact effective host inputs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedBootImage {
    pub config: NodeConfig,
    pub cluster_id: [u8; 16],
    pub membership: MembershipState,
    pub boot_epoch: Time,
    pub build_label: String,
    pub wal: Vec<u8>,
}

impl RecordedBootImage {
    pub fn encode(&self) -> Result<Vec<u8>, JournalError> {
        if self.wal.len() > MAX_BOOT_IMAGE_BYTES {
            return Err(JournalError::TooLarge("boot WAL"));
        }
        if self.build_label.len() > MAX_BOOT_BUILD_LABEL_BYTES {
            return Err(JournalError::TooLarge("build label"));
        }
        self.membership
            .validate()
            .map_err(|_| JournalError::Invalid("boot membership"))?;
        if self.config.id.get() == 0
            || self.config.cluster_id != self.cluster_id
            || self.cluster_id.iter().all(|byte| *byte == 0)
            || !self.config.host_limits.is_valid()
        {
            return Err(JournalError::Invalid("boot node configuration"));
        }
        let mut enc = Enc::new();
        enc.header(DRIVER_BOOT_MAGIC, DRIVER_BOOT_VERSION);
        enc.bytes(&self.cluster_id);
        encode_node_config(&mut enc, self.config)?;
        enc.bytes(
            &self
                .membership
                .encode()
                .map_err(|_| JournalError::Invalid("boot membership"))?,
        );
        enc.u64(self.boot_epoch.as_nanos());
        enc.string(&self.build_label);
        enc.bytes(&self.wal);
        Ok(enc.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, JournalError> {
        let mut dec = Dec::new(bytes);
        let magic = dec.u32()?;
        let boot_version = dec.u16()?;
        if magic != DRIVER_BOOT_MAGIC {
            return Err(JournalError::Decode(DecodeError::InvalidMagic {
                expected: DRIVER_BOOT_MAGIC,
                actual: magic,
            }));
        }
        if !matches!(boot_version, 2 | DRIVER_BOOT_VERSION) {
            return Err(JournalError::Decode(DecodeError::InvalidVersion {
                expected: DRIVER_BOOT_VERSION,
                actual: boot_version,
            }));
        }
        let cluster_id: [u8; 16] = dec
            .bytes()?
            .try_into()
            .map_err(|_| JournalError::Invalid("boot cluster id"))?;
        let config = decode_node_config(&mut dec, cluster_id, boot_version)?;
        let membership = MembershipState::decode(&dec.bytes()?)
            .map_err(|_| JournalError::Invalid("boot membership"))?;
        let boot_epoch = Time::from_nanos(dec.u64()?);
        let build_label = dec.string()?;
        if build_label.len() > MAX_BOOT_BUILD_LABEL_BYTES {
            return Err(JournalError::TooLarge("build label"));
        }
        let wal = dec.bytes()?;
        if wal.len() > MAX_BOOT_IMAGE_BYTES {
            return Err(JournalError::TooLarge("boot WAL"));
        }
        dec.finish()?;
        Ok(Self {
            config,
            cluster_id,
            membership,
            boot_epoch,
            build_label,
            wal,
        })
    }
}

fn encode_node_config(enc: &mut Enc, config: NodeConfig) -> Result<(), JournalError> {
    enc.u64(config.id.get());
    enc.u64(config.seed.0);
    enc.u64(config.raft.election_min.as_nanos());
    enc.u64(config.raft.election_max.as_nanos());
    enc.u64(config.raft.heartbeat.as_nanos());
    enc.u64(u64::try_from(config.raft.max_entries_per_append).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(config.raft.pipeline_window).unwrap_or(u64::MAX));
    enc.u64(config.raft.leader_transfer_timeout.as_nanos());
    enc.u64(u64::try_from(config.store.memtable_bytes).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(config.store.max_key_bytes).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(config.store.max_value_bytes).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(config.store.wal.segment_size).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(config.store.wal.max_record_size).unwrap_or(u64::MAX));
    enc.bytes(&config.policy.encode());
    let limits = config.host_limits;
    enc.u64(u64::try_from(limits.max_pending_peer).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(limits.max_pending_timer).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(limits.max_pending_io).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(limits.max_pending_client).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(limits.max_driver_pending_inputs).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(limits.max_driver_pending_input_bytes).unwrap_or(u64::MAX));
    enc.u64(limits.max_events);
    enc.u64(limits.max_events_per_instant);
    enc.u64(u64::try_from(limits.max_trace_bytes).unwrap_or(u64::MAX));
    enc.u64(limits.max_snapshot_bytes);
    enc.u32(limits.max_block_read_bytes);
    enc.u32(limits.max_manifest_record_bytes);
    enc.u64(u64::try_from(limits.max_threads).unwrap_or(u64::MAX));
    enc.u64(u64::try_from(limits.thread_stack_bytes).unwrap_or(u64::MAX));
    for value in [
        limits.max_peer_frame_bytes,
        limits.max_uncommitted_entries,
        limits.max_uncommitted_bytes,
        limits.max_log_bytes_before_snapshot,
        limits.max_raft_log_bytes,
        limits.max_store_wal_bytes,
        limits.max_data_dir_bytes,
        limits.maintenance_reserve_bytes,
        limits.max_snapshot_chunk_bytes,
        limits.max_snapshot_staging_bytes,
        limits.max_snapshot_pins,
        limits.max_checkpoint_builder_bytes,
        limits.max_pending_reads,
        limits.max_pending_read_bytes,
        limits.max_pending_client_routes,
        limits.max_host_connections,
        limits.max_open_files,
        limits.max_host_thread_stack_bytes,
        limits.max_host_input_bytes,
        limits.max_host_total_input_bytes,
        limits.max_host_output_bytes,
        limits.max_host_total_output_bytes,
        limits.max_host_queued_requests,
        limits.max_host_total_queued_requests,
        limits.max_driver_pending_effects,
        limits.max_driver_pending_effect_bytes,
        limits.max_network_inflight_bytes,
        limits.max_fault_replay_bytes,
        limits.max_memtable_bytes,
        limits.max_frozen_memtables,
        limits.max_sst_files,
        limits.max_referenced_sst_bytes,
        limits.max_sst_metadata_bytes,
        limits.max_manifest_generations,
        limits.max_compaction_builder_bytes,
        limits.max_history_operations,
        limits.max_history_bytes,
        limits.max_failure_artifact_bytes,
    ] {
        enc.u64(value);
    }
    if config.id.get() == 0 || !limits.is_valid() {
        return Err(JournalError::Invalid("boot node configuration"));
    }
    Ok(())
}

fn decode_node_config(
    dec: &mut Dec<'_>,
    cluster_id: [u8; 16],
    boot_version: u16,
) -> Result<NodeConfig, JournalError> {
    let id = NodeId::new(dec.u64()?);
    if id.get() == 0 {
        return Err(JournalError::Invalid("zero boot node id"));
    }
    let seed = Seed::new(dec.u64()?);
    let raft = RaftConfig {
        election_min: cc_core::Duration::from_nanos(dec.u64()?),
        election_max: cc_core::Duration::from_nanos(dec.u64()?),
        heartbeat: cc_core::Duration::from_nanos(dec.u64()?),
        max_entries_per_append: decode_usize(dec, "raft append limit")?,
        pipeline_window: decode_usize(dec, "raft pipeline limit")?,
        leader_transfer_timeout: cc_core::Duration::from_nanos(dec.u64()?),
    };
    let store = StoreConfig::from_parts(
        decode_usize(dec, "store memtable limit")?,
        decode_usize(dec, "store key limit")?,
        decode_usize(dec, "store value limit")?,
        decode_usize(dec, "WAL segment limit")?,
        decode_usize(dec, "WAL record limit")?,
    );
    let policy = ClusterPolicy::decode(&dec.bytes()?)
        .map_err(|_| JournalError::Invalid("boot cluster policy"))?;
    let mut host_limits = HostLimits {
        max_pending_peer: decode_usize(dec, "peer queue limit")?,
        max_pending_timer: decode_usize(dec, "timer queue limit")?,
        max_pending_io: decode_usize(dec, "I/O queue limit")?,
        max_pending_client: decode_usize(dec, "client queue limit")?,
        max_driver_pending_inputs: decode_usize(dec, "driver input limit")?,
        max_driver_pending_input_bytes: decode_usize(dec, "driver byte limit")?,
        max_events: dec.u64()?,
        max_events_per_instant: dec.u64()?,
        max_trace_bytes: decode_usize(dec, "trace byte limit")?,
        max_snapshot_bytes: dec.u64()?,
        max_block_read_bytes: dec.u32()?,
        max_manifest_record_bytes: dec.u32()?,
        max_threads: decode_usize(dec, "thread limit")?,
        thread_stack_bytes: decode_usize(dec, "thread stack bytes")?,
        ..HostLimits::default()
    };
    if boot_version >= 3 {
        macro_rules! take_limits {
            ($($field:ident),+ $(,)?) => { $(host_limits.$field = dec.u64()?;)+ };
        }
        take_limits!(
            max_peer_frame_bytes,
            max_uncommitted_entries,
            max_uncommitted_bytes,
            max_log_bytes_before_snapshot,
            max_raft_log_bytes,
            max_store_wal_bytes,
            max_data_dir_bytes,
            maintenance_reserve_bytes,
            max_snapshot_chunk_bytes,
            max_snapshot_staging_bytes,
            max_snapshot_pins,
            max_checkpoint_builder_bytes,
            max_pending_reads,
            max_pending_read_bytes,
            max_pending_client_routes,
            max_host_connections,
            max_open_files,
            max_host_thread_stack_bytes,
            max_host_input_bytes,
            max_host_total_input_bytes,
            max_host_output_bytes,
            max_host_total_output_bytes,
            max_host_queued_requests,
            max_host_total_queued_requests,
            max_driver_pending_effects,
            max_driver_pending_effect_bytes,
            max_network_inflight_bytes,
            max_fault_replay_bytes,
            max_memtable_bytes,
            max_frozen_memtables,
            max_sst_files,
            max_referenced_sst_bytes,
            max_sst_metadata_bytes,
            max_manifest_generations,
            max_compaction_builder_bytes,
            max_history_operations,
            max_history_bytes,
            max_failure_artifact_bytes,
        );
    }
    if !host_limits.is_valid() {
        return Err(JournalError::Invalid("boot host limits"));
    }
    Ok(NodeConfig {
        id,
        cluster_id,
        seed,
        raft,
        store,
        policy,
        host_limits,
    })
}

fn decode_usize(dec: &mut Dec<'_>, what: &'static str) -> Result<usize, JournalError> {
    usize::try_from(dec.u64()?).map_err(|_| JournalError::Invalid(what))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalRecord {
    pub ordinal: u64,
    pub now: Time,
    pub input: Input,
    /// Exact synchronous block reads issued while handling this input.
    /// They are paired with the input rather than accumulated across the run
    /// so replay can reject the first request-order divergence.
    pub block_observations: Vec<BlockObservation>,
    pub effects: Vec<Effect>,
}

/// The outcome tag for one observed block read.  Error observations contain
/// no bytes; their tag is enough to replay the core's typed failure boundary
/// without borrowing local storage during replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum BlockResultTag {
    Ok = 1,
    MissingTable = 2,
    InvalidInput = 3,
    TooLarge = 4,
    Busy = 5,
    Corrupt = 6,
    MetaMismatch = 7,
    Wal = 8,
    Decode = 9,
}

impl BlockResultTag {
    fn from_store_error(error: &BlockReadError) -> Self {
        match &error.error {
            StoreError::MissingTable { .. } => Self::MissingTable,
            StoreError::InvalidInput(_) => Self::InvalidInput,
            StoreError::TooLarge { .. } => Self::TooLarge,
            StoreError::Busy => Self::Busy,
            StoreError::Corrupt(_) => Self::Corrupt,
            StoreError::MetaMismatch => Self::MetaMismatch,
            StoreError::Wal(_) => Self::Wal,
            StoreError::Decode(_) => Self::Decode,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, JournalError> {
        match tag {
            1 => Ok(Self::Ok),
            2 => Ok(Self::MissingTable),
            3 => Ok(Self::InvalidInput),
            4 => Ok(Self::TooLarge),
            5 => Ok(Self::Busy),
            6 => Ok(Self::Corrupt),
            7 => Ok(Self::MetaMismatch),
            8 => Ok(Self::Wal),
            9 => Ok(Self::Decode),
            _ => Err(JournalError::Invalid("block result tag")),
        }
    }

    fn replay_error(self, file: FileId) -> StoreError {
        match self {
            Self::Ok => StoreError::InvalidInput("recorded successful block read has no bytes"),
            Self::MissingTable => StoreError::MissingTable {
                file_no: file_number(file),
            },
            Self::InvalidInput => StoreError::InvalidInput("recorded block read error"),
            Self::TooLarge => StoreError::TooLarge {
                what: "recorded block read",
                size: usize::MAX,
                max: 0,
            },
            Self::Busy => StoreError::Busy,
            Self::Corrupt => StoreError::Corrupt("recorded block read error"),
            Self::MetaMismatch => StoreError::MetaMismatch,
            // The core distinguishes read success from failure at this seam;
            // the backing WAL/decoder diagnostic does not alter the replayed
            // input bytes, so retain a typed fail-closed storage error.
            Self::Wal | Self::Decode => StoreError::Corrupt("recorded block read error"),
        }
    }
}

/// One exact `BlockSource` request and outcome.  `len` is the requested
/// length, `bytes` is present only for successful exact reads, and `service`
/// is charged to the same driver input on replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockObservation {
    pub file: FileId,
    pub offset: u64,
    pub len: u32,
    pub result: BlockResultTag,
    pub bytes: Vec<u8>,
    pub service: Duration,
}

impl BlockObservation {
    fn from_result(
        file: FileId,
        offset: u64,
        len: u32,
        result: Result<&BlockRead, &BlockReadError>,
    ) -> Self {
        match result {
            Ok(read) => Self {
                file,
                offset,
                len,
                result: BlockResultTag::Ok,
                bytes: read.bytes.clone(),
                service: read.service,
            },
            Err(error) => Self {
                file,
                offset,
                len,
                result: BlockResultTag::from_store_error(error),
                bytes: Vec::new(),
                service: error.service,
            },
        }
    }

    fn validate(&self) -> Result<(), JournalError> {
        if self.result == BlockResultTag::Ok {
            if self.bytes.len() != usize::try_from(self.len).unwrap_or(usize::MAX) {
                return Err(JournalError::Invalid("successful block read length"));
            }
        } else if !self.bytes.is_empty() {
            return Err(JournalError::Invalid("failed block read bytes"));
        }
        Ok(())
    }

    fn replay_result(&self) -> Result<BlockRead, BlockReadError> {
        if self.result == BlockResultTag::Ok {
            Ok(BlockRead {
                bytes: self.bytes.clone(),
                service: self.service,
            })
        } else {
            Err(BlockReadError {
                error: self.result.replay_error(self.file),
                service: self.service,
            })
        }
    }
}

/// Delegates real reads and retains the exact outcomes until the surrounding
/// host transition has been durably appended to CCIJ.
#[derive(Debug)]
pub struct RecordingBlockSource<B> {
    inner: B,
    observations: Vec<BlockObservation>,
}

impl<B> RecordingBlockSource<B> {
    #[must_use]
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            observations: Vec::new(),
        }
    }

    #[must_use]
    pub fn take_observations(&mut self) -> Vec<BlockObservation> {
        std::mem::take(&mut self.observations)
    }
}

impl<B: BlockSource> BlockSource for RecordingBlockSource<B> {
    fn read_block(
        &mut self,
        file: FileId,
        offset: u64,
        len: u32,
    ) -> Result<BlockRead, BlockReadError> {
        let result = self.inner.read_block(file, offset, len);
        self.observations.push(BlockObservation::from_result(
            file,
            offset,
            len,
            result.as_ref(),
        ));
        result
    }
}

/// A replay-only block source. It consumes observations in request order and
/// records a diagnostic at the exact point a live read differs from CCIJ.
#[derive(Clone, Debug)]
pub struct ReplayBlockSource {
    observations: VecDeque<BlockObservation>,
    divergence: Option<String>,
}

impl ReplayBlockSource {
    #[must_use]
    pub fn new(observations: Vec<BlockObservation>) -> Self {
        Self {
            observations: observations.into(),
            divergence: None,
        }
    }

    pub fn finish(self) -> Result<(), String> {
        if let Some(divergence) = self.divergence {
            return Err(divergence);
        }
        if self.observations.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "unused recorded block observation count={}",
                self.observations.len()
            ))
        }
    }
}

impl BlockSource for ReplayBlockSource {
    fn read_block(
        &mut self,
        file: FileId,
        offset: u64,
        len: u32,
    ) -> Result<BlockRead, BlockReadError> {
        if self.divergence.is_some() {
            return Err(StoreError::InvalidInput("block replay already diverged").into());
        }
        let Some(observation) = self.observations.pop_front() else {
            self.divergence = Some(format!(
                "recorded block observations exhausted request={file:?}@{offset}+{len}"
            ));
            return Err(StoreError::InvalidInput("recorded block observations exhausted").into());
        };
        if (observation.file, observation.offset, observation.len) != (file, offset, len) {
            self.divergence = Some(format!(
                "block request mismatch expected={:?}@{}+{} actual={file:?}@{offset}+{len}",
                observation.file, observation.offset, observation.len
            ));
            return Err(StoreError::InvalidInput("recorded block request mismatch").into());
        }
        observation.replay_result()
    }
}

/// The explicit end state of a completed recording stream. A missing footer
/// is an interrupted prefix, not an implicit success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalTermination {
    Complete,
    Capped,
    HostError,
    FatalIo,
}

impl JournalTermination {
    const fn tag(self) -> u8 {
        match self {
            Self::Complete => 1,
            Self::Capped => 2,
            Self::HostError => 3,
            Self::FatalIo => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, JournalError> {
        match tag {
            1 => Ok(Self::Complete),
            2 => Ok(Self::Capped),
            3 => Ok(Self::HostError),
            4 => Ok(Self::FatalIo),
            _ => Err(JournalError::Invalid("termination tag")),
        }
    }
}

/// A control frame is distinguished from ordinary record frames by the high
/// bit of its length prefix. Normal record limits are far below that bit, so
/// this extends CCIJ v1 without making a control trailer parse as an input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalFooter {
    pub termination: JournalTermination,
    pub last_ordinal: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputJournal {
    /// A bounded, caller-owned boot image. The host is responsible for using
    /// this instead of ambient state when it replays a journal.
    pub boot_image: Vec<u8>,
    pub records: Vec<JournalRecord>,
    pub footer: Option<JournalFooter>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalError {
    Decode(DecodeError),
    Boundary(BoundaryCodecError),
    Invalid(&'static str),
    CorruptRecord { ordinal: u64 },
    CorruptFooter,
    TooLarge(&'static str),
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(f, "CCIJ decode: {error}"),
            Self::Boundary(error) => write!(f, "CCIJ boundary value: {error}"),
            Self::Invalid(reason) => write!(f, "invalid CCIJ: {reason}"),
            Self::CorruptRecord { ordinal } => write!(f, "corrupt CCIJ record {ordinal}"),
            Self::CorruptFooter => f.write_str("corrupt CCIJ footer"),
            Self::TooLarge(what) => write!(f, "oversized CCIJ {what}"),
        }
    }
}

impl std::error::Error for JournalError {}
impl From<DecodeError> for JournalError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}
impl From<BoundaryCodecError> for JournalError {
    fn from(value: BoundaryCodecError) -> Self {
        Self::Boundary(value)
    }
}

/// The bounded result of replaying one CCIJ input/effect receipt.  A journal
/// without a footer is still a valid durable prefix; only `Complete` proves
/// that it covers a deliberately terminated whole run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalReplay {
    pub records: usize,
    pub termination: Option<JournalTermination>,
}

/// Replay failures identify the durable boundary at which the recording no
/// longer reproduces.  They are deliberately not collapsed into an I/O error:
/// callers need the record ordinal to preserve the first divergence.
#[derive(Debug)]
pub enum JournalReplayError {
    Journal(JournalError),
    Log(LogError),
    IncompleteBootWal,
    BootMetadataMismatch,
    DriverBoot(HostError),
    BlockDivergence {
        ordinal: u64,
        detail: String,
    },
    Driver {
        ordinal: u64,
        error: HostError,
    },
    Effects {
        ordinal: u64,
        expected: usize,
        actual: usize,
    },
}

impl fmt::Display for JournalReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => write!(f, "CCIJ: {error}"),
            Self::Log(error) => write!(f, "recorded boot WAL: {error}"),
            Self::IncompleteBootWal => {
                f.write_str("recorded boot WAL is not a complete durable prefix")
            }
            Self::BootMetadataMismatch => {
                f.write_str("recorded boot metadata disagrees with embedded WAL genesis")
            }
            Self::DriverBoot(error) => write!(f, "replay Driver boot: {error}"),
            Self::BlockDivergence { ordinal, detail } => {
                write!(f, "replay block divergence ordinal={ordinal}: {detail}")
            }
            Self::Driver { ordinal, error } => {
                write!(f, "replay Driver divergence ordinal={ordinal}: {error}")
            }
            Self::Effects {
                ordinal,
                expected,
                actual,
            } => write!(
                f,
                "replay divergence ordinal={ordinal} expected_effects={expected} actual_effects={actual}"
            ),
        }
    }
}

impl std::error::Error for JournalReplayError {}

/// Replay a CCIJ receipt through the same `Driver` boundary as a real host.
/// The embedded boot image is the sole source of config, identity, membership
/// and durable log bytes: replay deliberately consults neither a local data
/// directory nor ambient configuration.
pub fn replay_journal(journal: &InputJournal) -> Result<JournalReplay, JournalReplayError> {
    let boot =
        RecordedBootImage::decode(&journal.boot_image).map_err(JournalReplayError::Journal)?;
    let recovered = recover_framed_record_stream(&boot.wal).map_err(JournalReplayError::Log)?;
    if recovered.torn_tail_truncated || recovered.bytes_consumed != boot.wal.len() as u64 {
        return Err(JournalReplayError::IncompleteBootWal);
    }
    let state = recovered.state;
    if state.genesis.cluster_id != boot.cluster_id
        || state.genesis.policy != boot.config.policy
        || state.genesis.membership != boot.membership
    {
        return Err(JournalReplayError::BootMetadataMismatch);
    }
    let node = RecoveredNode {
        hard_state: state.hard_state,
        log_base: (state.base_index, state.base_term),
        entries: state.entries,
        membership: boot.membership.clone(),
        cluster_policy: boot.config.policy,
        snapshot: None,
        durable_applied: (state.base_index, state.base_term),
    };
    let mut driver = Driver::boot_with_wal_offset(
        boot.config,
        BootState::Recovered(Box::new(node)),
        recovered.bytes_consumed,
    )
    .map_err(JournalReplayError::DriverBoot)?;

    for record in &journal.records {
        let mut blocks = ReplayBlockSource::new(record.block_observations.clone());
        // Every journalled transition was one the host had already admitted,
        // including the ones it took off its own queue after a durability
        // barrier. Replaying through the plain `deliver` door refused those,
        // so an ordinary queued client request that found the node no longer
        // leader made the whole journal unreplayable.
        let (_poll, actual) = driver
            .deliver_admitted(record.now, record.input.clone(), &mut blocks)
            .map_err(|error| JournalReplayError::Driver {
                ordinal: record.ordinal,
                error,
            })?;
        blocks
            .finish()
            .map_err(|detail| JournalReplayError::BlockDivergence {
                ordinal: record.ordinal,
                detail,
            })?;
        if actual != record.effects {
            return Err(JournalReplayError::Effects {
                ordinal: record.ordinal,
                expected: record.effects.len(),
                actual: actual.len(),
            });
        }
    }

    Ok(JournalReplay {
        records: journal.records.len(),
        termination: journal.footer.map(|footer| footer.termination),
    })
}

impl InputJournal {
    #[must_use]
    pub fn new(boot_image: Vec<u8>) -> Self {
        Self {
            boot_image,
            records: Vec::new(),
            footer: None,
        }
    }

    pub fn push(&mut self, record: JournalRecord) -> Result<(), JournalError> {
        if self.footer.is_some() {
            return Err(JournalError::Invalid("record after footer"));
        }
        if record.block_observations.len() > MAX_BLOCK_OBSERVATIONS_PER_RECORD {
            return Err(JournalError::TooLarge("block observation count"));
        }
        for observation in &record.block_observations {
            observation.validate()?;
        }
        if record.effects.len() > MAX_EFFECTS_PER_RECORD {
            return Err(JournalError::TooLarge("effect count"));
        }
        if let Some(previous) = self.records.last()
            && (record.ordinal <= previous.ordinal || record.now < previous.now)
        {
            return Err(JournalError::Invalid("nonmonotonic ordinal or time"));
        }
        self.records.push(record);
        Ok(())
    }

    /// Finish the in-memory journal with an explicit status. The footer
    /// records the last durable ordinal so a decoder cannot call a capped or
    /// errored prefix complete by accident.
    pub fn finish(&mut self, footer: JournalFooter) -> Result<(), JournalError> {
        if self.footer.is_some() {
            return Err(JournalError::Invalid("multiple footers"));
        }
        let last_ordinal = self.records.last().map_or(0, |record| record.ordinal);
        if footer.last_ordinal != last_ordinal {
            return Err(JournalError::Invalid("footer ordinal"));
        }
        self.footer = Some(footer);
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, JournalError> {
        let mut bytes = Self::encode_header(&self.boot_image)?;
        for record in &self.records {
            bytes.extend_from_slice(&Self::encode_record_frame(record)?);
        }
        if let Some(footer) = self.footer {
            bytes.extend_from_slice(&Self::encode_footer_frame(footer)?);
        }
        Ok(bytes)
    }

    /// Encodes the durable CCIJ prefix written before the first delivered
    /// input.  Hosts append frames from [`Self::encode_record_frame`] and fsync
    /// each completed frame, so a crash is always a valid journal prefix.
    pub fn encode_header(boot_image: &[u8]) -> Result<Vec<u8>, JournalError> {
        if boot_image.len() > MAX_BOOT_IMAGE_BYTES {
            return Err(JournalError::TooLarge("boot image"));
        }
        let mut enc = Enc::new();
        enc.header(INPUT_JOURNAL_MAGIC, INPUT_JOURNAL_VERSION);
        enc.bytes(boot_image);
        Ok(enc.finish())
    }

    /// Encodes one independently CRC-protected append frame. This does not
    /// mutate an [`InputJournal`], making it suitable for a host-owned file
    /// sink that cannot accumulate an unbounded recording in memory.
    pub fn encode_record_frame(record: &JournalRecord) -> Result<Vec<u8>, JournalError> {
        let body = encode_record(record)?;
        let length = u32::try_from(body.len()).map_err(|_| JournalError::TooLarge("record"))?;
        let mut frame = Vec::with_capacity(8 + body.len());
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(&crc32c(&body).to_le_bytes());
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    /// Encodes one terminal control frame. It is reserved for an explicit
    /// recording status; a normal input/effect record can never use this
    /// namespace because its maximum size is far below the high-bit marker.
    pub fn encode_footer_frame(footer: JournalFooter) -> Result<Vec<u8>, JournalError> {
        let body = encode_footer(footer);
        let length = u32::try_from(body.len()).map_err(|_| JournalError::TooLarge("footer"))?;
        if length as usize > MAX_FOOTER_BYTES {
            return Err(JournalError::TooLarge("footer"));
        }
        let mut frame = Vec::with_capacity(8 + body.len());
        frame.extend_from_slice(&(length | FOOTER_FRAME_BIT).to_le_bytes());
        frame.extend_from_slice(&crc32c(&body).to_le_bytes());
        frame.extend_from_slice(&body);
        Ok(frame)
    }

    /// Recovers every complete CRC-checked record. A trailing partial record
    /// is a crash prefix and is discarded; malformed or CRC-bad complete
    /// records fail closed.
    pub fn decode(bytes: &[u8]) -> Result<Self, JournalError> {
        let mut dec = Dec::new(bytes);
        dec.header(INPUT_JOURNAL_MAGIC, INPUT_JOURNAL_VERSION)?;
        let boot_image = dec.bytes()?;
        if boot_image.len() > MAX_BOOT_IMAGE_BYTES {
            return Err(JournalError::TooLarge("boot image"));
        }
        let mut offset = dec.position();
        let mut journal = Self::new(boot_image);
        while offset < bytes.len() {
            let Some(prefix) = bytes.get(offset..offset.saturating_add(8)) else {
                break;
            };
            let encoded_length = u32::from_le_bytes(prefix[..4].try_into().expect("u32"));
            let is_footer = encoded_length & FOOTER_FRAME_BIT != 0;
            let length = usize::try_from(encoded_length & !FOOTER_FRAME_BIT)
                .map_err(|_| JournalError::TooLarge("record"))?;
            let expected_crc = u32::from_le_bytes(prefix[4..8].try_into().expect("CRC"));
            if if is_footer {
                length > MAX_FOOTER_BYTES
            } else {
                length > MAX_JOURNAL_RECORD_BYTES
            } {
                return Err(JournalError::TooLarge(if is_footer {
                    "footer"
                } else {
                    "record"
                }));
            }
            let body_start = offset.saturating_add(8);
            let Some(body) = bytes.get(body_start..body_start.saturating_add(length)) else {
                break;
            };
            if crc32c(body) != expected_crc {
                if is_footer {
                    return Err(JournalError::CorruptFooter);
                }
                let ordinal = body
                    .get(..8)
                    .and_then(|bytes| bytes.try_into().ok())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0);
                return Err(JournalError::CorruptRecord { ordinal });
            }
            offset = body_start.saturating_add(length);
            if is_footer {
                let footer = decode_footer(body)?;
                journal.finish(footer)?;
                if offset != bytes.len() {
                    return Err(JournalError::Invalid("records after footer"));
                }
                return Ok(journal);
            }
            let record = decode_record(body)?;
            journal.push(record)?;
        }
        Ok(journal)
    }
}

fn encode_footer(footer: JournalFooter) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.header(FOOTER_MAGIC, FOOTER_VERSION);
    enc.u8(footer.termination.tag());
    enc.u64(footer.last_ordinal);
    enc.finish()
}

fn decode_footer(bytes: &[u8]) -> Result<JournalFooter, JournalError> {
    let mut dec = Dec::new(bytes);
    dec.header(FOOTER_MAGIC, FOOTER_VERSION)?;
    let termination = JournalTermination::from_tag(dec.u8()?)?;
    let last_ordinal = dec.u64()?;
    dec.finish()?;
    Ok(JournalFooter {
        termination,
        last_ordinal,
    })
}

fn encode_record(record: &JournalRecord) -> Result<Vec<u8>, JournalError> {
    if record.block_observations.len() > MAX_BLOCK_OBSERVATIONS_PER_RECORD {
        return Err(JournalError::TooLarge("block observation count"));
    }
    if record.effects.len() > MAX_EFFECTS_PER_RECORD {
        return Err(JournalError::TooLarge("effect count"));
    }
    let mut enc = Enc::new();
    enc.u64(record.ordinal);
    enc.u64(record.now.as_nanos());
    enc.bytes(&encode_input(&record.input)?);
    enc.u32(
        u32::try_from(record.block_observations.len())
            .map_err(|_| JournalError::TooLarge("block observation count"))?,
    );
    for observation in &record.block_observations {
        observation.validate()?;
        enc.bytes(&encode_file_id(observation.file));
        enc.u64(observation.offset);
        enc.u32(observation.len);
        enc.u8(observation.result as u8);
        enc.bytes(&observation.bytes);
        enc.u64(observation.service.as_nanos());
    }
    enc.u32(
        u32::try_from(record.effects.len()).map_err(|_| JournalError::TooLarge("effect count"))?,
    );
    for effect in &record.effects {
        enc.bytes(&encode_effect(effect)?);
    }
    let bytes = enc.finish();
    if bytes.len() > MAX_JOURNAL_RECORD_BYTES {
        return Err(JournalError::TooLarge("record"));
    }
    Ok(bytes)
}

fn decode_record(bytes: &[u8]) -> Result<JournalRecord, JournalError> {
    let mut dec = Dec::new(bytes);
    let ordinal = dec.u64()?;
    let now = Time::from_nanos(dec.u64()?);
    let input = decode_input(&dec.bytes()?)?;
    let observation_count = usize::try_from(dec.u32()?)
        .map_err(|_| JournalError::TooLarge("block observation count"))?;
    if observation_count > MAX_BLOCK_OBSERVATIONS_PER_RECORD
        || observation_count > dec.remaining() / 22
    {
        return Err(JournalError::TooLarge("block observation count"));
    }
    let mut block_observations = Vec::with_capacity(observation_count);
    for _ in 0..observation_count {
        let file = decode_file_id(&dec.bytes()?)?;
        let offset = dec.u64()?;
        let len = dec.u32()?;
        let result = BlockResultTag::from_tag(dec.u8()?)?;
        let bytes = dec.bytes()?;
        let service = Duration::from_nanos(dec.u64()?);
        let observation = BlockObservation {
            file,
            offset,
            len,
            result,
            bytes,
            service,
        };
        observation.validate()?;
        block_observations.push(observation);
    }
    let count = usize::try_from(dec.u32()?).map_err(|_| JournalError::TooLarge("effect count"))?;
    if count > MAX_EFFECTS_PER_RECORD || count > dec.remaining() / 5 {
        return Err(JournalError::TooLarge("effect count"));
    }
    let mut effects = Vec::with_capacity(count);
    for _ in 0..count {
        effects.push(decode_effect(&dec.bytes()?)?);
    }
    dec.finish()?;
    Ok(JournalRecord {
        ordinal,
        now,
        input,
        block_observations,
        effects,
    })
}

fn file_number(file: FileId) -> u64 {
    match file {
        FileId::Wal { segment } | FileId::StoreWal { segment } => segment,
        FileId::Sst { file_no } => file_no,
        FileId::Manifest { generation } | FileId::Snapshot { generation } => generation,
        FileId::Meta => 0,
        FileId::Temp { sequence } => sequence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_core::{ClientId, RequestSeq};

    struct BusyBlockSource;

    impl BlockSource for BusyBlockSource {
        fn read_block(
            &mut self,
            _file: FileId,
            _offset: u64,
            _len: u32,
        ) -> Result<BlockRead, BlockReadError> {
            Err(BlockReadError {
                error: StoreError::Busy,
                service: Duration::from_nanos(23),
            })
        }
    }

    fn record(ordinal: u64) -> JournalRecord {
        JournalRecord {
            ordinal,
            now: Time::from_nanos(ordinal),
            input: Input::ClientRequest {
                client: ClientId::new(1),
                req: RequestSeq::new(ordinal),
                session: None,
                command: b"command".to_vec(),
            },
            block_observations: vec![BlockObservation {
                file: FileId::Sst { file_no: ordinal },
                offset: 0,
                len: 5,
                result: BlockResultTag::Ok,
                bytes: b"block".to_vec(),
                service: Duration::from_nanos(ordinal),
            }],
            effects: vec![Effect::ClientReply {
                client: ClientId::new(1),
                req: RequestSeq::new(ordinal),
                reply: b"reply".to_vec(),
            }],
        }
    }

    #[test]
    fn trap_journal_record_pairs_input_and_effects() {
        let mut journal = InputJournal::new(b"boot".to_vec());
        journal.push(record(1)).expect("record");
        assert_eq!(
            InputJournal::decode(&journal.encode().expect("encode")),
            Ok(journal)
        );
    }

    #[test]
    fn trap_streamed_journal_header_and_frames_match_complete_encoding() {
        let mut journal = InputJournal::new(b"boot".to_vec());
        journal.push(record(1)).expect("first record");
        journal.push(record(2)).expect("second record");
        let mut streamed = InputJournal::encode_header(&journal.boot_image).expect("header");
        for record in &journal.records {
            streamed.extend_from_slice(
                &InputJournal::encode_record_frame(record).expect("record frame"),
            );
        }
        assert_eq!(streamed, journal.encode().expect("complete encoding"));
        assert_eq!(InputJournal::decode(&streamed), Ok(journal));
    }

    #[test]
    fn trap_recorded_boot_image_is_bounded_and_canonical() {
        let membership =
            MembershipState::new([NodeId::new(7)].into_iter().collect()).expect("membership");
        let image = RecordedBootImage {
            config: NodeConfig {
                id: NodeId::new(7),
                cluster_id: [7; 16],
                seed: Seed::new(9),
                raft: RaftConfig::default(),
                store: StoreConfig::default(),
                policy: ClusterPolicy::default(),
                host_limits: HostLimits::default(),
            },
            cluster_id: [7; 16],
            membership,
            boot_epoch: Time::from_nanos(11),
            build_label: String::from("test"),
            wal: vec![1, 2, 3],
        };
        assert_eq!(
            RecordedBootImage::decode(&image.encode().expect("encode")),
            Ok(image)
        );
    }

    #[test]
    fn trap_input_journal_is_prefix_durable() {
        let mut journal = InputJournal::new(b"boot".to_vec());
        journal.push(record(1)).expect("first");
        journal.push(record(2)).expect("second");
        let bytes = journal.encode().expect("encode");
        let torn = &bytes[..bytes.len().saturating_sub(3)];
        let recovered = InputJournal::decode(torn).expect("torn tail");
        assert_eq!(recovered.records, vec![record(1)]);
    }

    #[test]
    fn trap_input_journal_rejects_complete_crc_corruption() {
        let mut journal = InputJournal::new(b"boot".to_vec());
        journal.push(record(1)).expect("record");
        let mut bytes = journal.encode().expect("encode");
        let last = bytes.len().saturating_sub(1);
        bytes[last] ^= 1;
        assert!(matches!(
            InputJournal::decode(&bytes),
            Err(JournalError::CorruptRecord { ordinal: 1 })
        ));
    }

    #[test]
    fn trap_capped_journal_never_claims_complete() {
        let mut journal = InputJournal::new(b"boot".to_vec());
        journal.push(record(1)).expect("record");
        journal
            .finish(JournalFooter {
                termination: JournalTermination::Capped,
                last_ordinal: 1,
            })
            .expect("capped footer");
        assert_eq!(
            InputJournal::decode(&journal.encode().expect("encode")),
            Ok(journal)
        );
    }

    #[test]
    fn trap_replay_consumes_recorded_block_reads_and_service_time() {
        let observed = BlockObservation {
            file: FileId::Sst { file_no: 9 },
            offset: 2,
            len: 3,
            result: BlockResultTag::Ok,
            bytes: b"abc".to_vec(),
            service: Duration::from_nanos(17),
        };
        let mut replay = ReplayBlockSource::new(vec![observed]);
        assert_eq!(
            replay
                .read_block(FileId::Sst { file_no: 9 }, 2, 3)
                .expect("recorded read"),
            BlockRead {
                bytes: b"abc".to_vec(),
                service: Duration::from_nanos(17),
            }
        );
        replay.finish().expect("all observed reads consumed");
    }

    #[test]
    fn trap_replay_rejects_block_read_request_order_divergence() {
        let mut replay = ReplayBlockSource::new(vec![BlockObservation {
            file: FileId::Sst { file_no: 9 },
            offset: 2,
            len: 3,
            result: BlockResultTag::Ok,
            bytes: b"abc".to_vec(),
            service: Duration::from_nanos(0),
        }]);
        assert!(replay.read_block(FileId::Sst { file_no: 9 }, 3, 3).is_err());
        assert!(replay.finish().is_err());
    }

    #[test]
    fn trap_failed_block_read_keeps_recorded_service_time() {
        let mut recording = RecordingBlockSource::new(BusyBlockSource);
        assert!(
            recording
                .read_block(FileId::Sst { file_no: 5 }, 0, 4)
                .is_err()
        );
        let observations = recording.take_observations();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].result, BlockResultTag::Busy);
        assert!(observations[0].bytes.is_empty());
        assert_eq!(observations[0].service, Duration::from_nanos(23));

        let mut replay = ReplayBlockSource::new(observations);
        let error = replay
            .read_block(FileId::Sst { file_no: 5 }, 0, 4)
            .expect_err("recorded failure");
        assert!(matches!(error.error, StoreError::Busy));
        assert_eq!(error.service, Duration::from_nanos(23));
        replay.finish().expect("failed observation consumed");
    }
}
