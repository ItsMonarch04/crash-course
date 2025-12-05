// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Host-neutral node driver: boundary translation, timer generations, and I/O correlation."]

pub mod journal;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Instant; // cc-detlint: allow host-boundary

use cc_cluster::{
    CcsnSnapshot, CcsnStreamDecoder, CcsnStreamEncoder, Message, MessageKind, Node, NodeConfig,
    NodeEffect, NodeError, RecoveredNode, SnapshotRejectReason, TimerKind, encode_client_reply,
    encode_durability_effect, encode_peer_effect,
};
use cc_core::{
    ClientId, ConfigOperation, Duration, IoId, LogIndex, NodeId, RequestSeq, SessionKey, Time,
    TimerId,
};
use cc_env::{Effect, FileId, Input, IoResult};
use cc_log::{DurableRecord, Genesis, SnapshotMark, encode_framed_durable_record};
use cc_store::{
    BlockRead, BlockReadError, BlockSource, ManifestCheckpoint, ManifestEditV2, ManifestV2,
    StoreError, StoreWatermark, encode_manifest_v2,
};

pub const DEFAULT_MAX_PENDING_INPUTS: usize = 16_384;

/// Real positioned-read implementation. The deterministic store sees only a
/// logical file id, returned bytes, and measured service duration; paths,
/// descriptors, and the blocking clock remain host-owned.
#[derive(Debug)]
pub struct FileBlockSource {
    root: PathBuf,
    max_open_files: usize,
    open_files: usize,
}

impl FileBlockSource {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::with_limit(root, 128)
    }

    pub fn with_limit(root: impl AsRef<Path>, max_open_files: usize) -> Result<Self, StoreError> {
        let root = root.as_ref();
        if max_open_files == 0 || !root.is_dir() {
            return Err(StoreError::InvalidInput("block source root/limit"));
        }
        Ok(Self {
            root: root.to_path_buf(),
            max_open_files,
            open_files: 0,
        })
    }
}

struct FileLease<'a> {
    count: &'a mut usize,
}

impl Drop for FileLease<'_> {
    fn drop(&mut self) {
        *self.count = self.count.saturating_sub(1);
    }
}

impl BlockSource for FileBlockSource {
    fn read_block(
        &mut self,
        file: FileId,
        offset: u64,
        len: u32,
    ) -> Result<BlockRead, BlockReadError> {
        let started = Instant::now(); // cc-detlint: allow host-boundary
        let result = (|| {
            let FileId::Sst { file_no } = file else {
                return Err(StoreError::InvalidInput("block source file class"));
            };
            let length = usize::try_from(len).unwrap_or(usize::MAX);
            if length > cc_core::MAX_CODEC_BYTES {
                return Err(StoreError::TooLarge {
                    what: "block read",
                    size: length,
                    max: cc_core::MAX_CODEC_BYTES,
                });
            }
            if self.open_files >= self.max_open_files {
                return Err(StoreError::Busy);
            }
            let path = self.root.join(format!("sst-{file_no}"));
            self.open_files += 1;
            let _lease = FileLease {
                count: &mut self.open_files,
            };
            let metadata =
                fs::symlink_metadata(&path).map_err(|_| StoreError::MissingTable { file_no })?;
            if !metadata.file_type().is_file() {
                return Err(StoreError::InvalidInput(
                    "block source requires regular file",
                ));
            }
            let end = offset
                .checked_add(u64::from(len))
                .ok_or(StoreError::InvalidInput("block range"))?;
            if end > metadata.len() {
                return Err(StoreError::InvalidInput("block range"));
            }
            let mut handle = File::open(path).map_err(|_| StoreError::MissingTable { file_no })?;
            let mut bytes = vec![0; length];
            handle
                .seek(SeekFrom::Start(offset))
                .and_then(|_| handle.read_exact(&mut bytes))
                .map_err(|_| StoreError::Corrupt("short block read"))?;
            Ok(bytes)
        })();
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let service = Duration::from_nanos(nanos);
        match result {
            Ok(bytes) => Ok(BlockRead { bytes, service }),
            Err(error) => Err(BlockReadError { error, service }),
        }
    }
}

/// Host-side admission classes. These are intentionally not part of the
/// deterministic node: they only decide which untrusted input is allowed to
/// wait at the adapter boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputClass {
    Peer,
    Timer,
    Io,
    Client,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostError {
    Node(NodeError),
    Durability { id: IoId, file: FileId },
    UnknownIo(IoId),
    InvalidIoCompletion(IoId),
    QueueFull(InputClass),
    ResourceLimit(&'static str),
    IoIdExhausted,
    TimeOverflow,
    Recorder(String),
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(error) => write!(f, "node: {error}"),
            Self::Durability { id, file } => {
                write!(f, "critical durability operation {id} failed for {file:?}")
            }
            Self::UnknownIo(id) => write!(f, "unknown or duplicate I/O completion {id}"),
            Self::InvalidIoCompletion(id) => write!(f, "invalid I/O completion for {id}"),
            Self::QueueFull(class) => write!(f, "host {class:?} input queue is full"),
            Self::ResourceLimit(field) => write!(f, "host resource limit reached: {field}"),
            Self::IoIdExhausted => f.write_str("logical I/O id space exhausted"),
            Self::TimeOverflow => f.write_str("synchronous service time overflow"),
            Self::Recorder(error) => write!(f, "recording: {error}"),
        }
    }
}

impl std::error::Error for HostError {}

impl From<NodeError> for HostError {
    fn from(value: NodeError) -> Self {
        Self::Node(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootState {
    Fresh { bootstrap: cc_core::MembershipState },
    Recovered(Box<RecoveredNode>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverPoll {
    Ready,
    BlockedUntil(Time),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Usage {
    /// Deterministically charged current bytes, not allocator usage.
    pub current: u64,
    /// Highest charged byte count seen since boot.
    pub peak: u64,
    /// The admission limit that the current charge is checked against.
    pub limit: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeFootprint {
    pub log: Usage,
    pub snapshot_staging: Usage,
    pub sessions: Usage,
    pub session_tombstones: Usage,
    pub pending_reads: Usage,
    pub pending_client_routes: Usage,
    pub memtables: Usage,
    pub sst_metadata: Usage,
    pub driver_effects: Usage,
    pub outbound_frames: Usage,
    pub checkpoint_builder: Usage,
    pub compaction_builder: Usage,
    pub armed_timers: usize,
    pub pending_io: usize,
    pub pending_peer_inputs: usize,
    pub pending_timer_inputs: usize,
    pub pending_io_inputs: usize,
    pub pending_client_inputs: usize,
    pub pending_input_bytes: usize,
    /// Aggregate, encoded input charge across all driver queues.  The charge
    /// is the explicit header allowance plus payload bytes, so it remains
    /// stable across allocators and platforms.
    pub driver_inputs: Usage,
    pub blocked: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct FootprintPeaks {
    log: u64,
    snapshot_staging: u64,
    sessions: u64,
    session_tombstones: u64,
    pending_reads: u64,
    pending_client_routes: u64,
    memtables: u64,
    sst_metadata: u64,
    driver_effects: u64,
    outbound_frames: u64,
    checkpoint_builder: u64,
    compaction_builder: u64,
}

#[derive(Clone, Copy, Debug)]
struct TimerState {
    at: Time,
    generation: u64,
    kind: TimerKind,
}

#[derive(Clone, Debug)]
enum IoStage {
    Write {
        file: FileId,
        len: u32,
    },
    #[cfg_attr(feature = "kata03", allow(dead_code))]
    Fsync {
        file: FileId,
    },
    SnapshotWrite {
        file: FileId,
        len: u32,
        message: Message,
        end: u64,
    },
    SnapshotFsync {
        file: FileId,
        message: Message,
        end: u64,
    },
    /// Read back one fsynced received chunk before it enters the incremental
    /// CCSN decoder. This proves the decoded bytes are the durable staging
    /// bytes, rather than a network-buffer copy.
    SnapshotChunkRead {
        file: FileId,
        message: Message,
        end: u64,
    },
    SnapshotDuplicateRead {
        file: FileId,
        message: Message,
        expected: Vec<u8>,
        next_offset: u64,
    },
    SnapshotRename {
        from: FileId,
        to: FileId,
        message: Message,
        end: u64,
    },
    SnapshotDirectorySync {
        file: FileId,
        message: Message,
        end: u64,
    },
    LocalSnapshotWrite {
        file: FileId,
        published: PublishedSnapshot,
        at: u64,
        len: u32,
        encoder: Box<CcsnStreamEncoder>,
    },
    LocalSnapshotFsync {
        file: FileId,
        published: PublishedSnapshot,
    },
    LocalSnapshotRename {
        from: FileId,
        published: PublishedSnapshot,
    },
    LocalSnapshotDirectorySync {
        published: PublishedSnapshot,
    },
    SnapshotManifestWrite {
        published: PublishedSnapshot,
        installed: Option<(Message, u64)>,
        len: u32,
    },
    SnapshotManifestFsync {
        published: PublishedSnapshot,
        installed: Option<(Message, u64)>,
    },
    SnapshotManifestDirectorySync {
        published: PublishedSnapshot,
        installed: Option<(Message, u64)>,
    },
    /// The local checkpoint file and containing directory are durable.  The
    /// next barrier records the matching log mark before the checkpoint can
    /// become recovery or transfer authority.
    LocalSnapshotMarkWrite {
        published: PublishedSnapshot,
        len: u32,
    },
    LocalSnapshotMarkFsync {
        published: PublishedSnapshot,
    },
    /// A received checkpoint needs a distinct mark: its covered prefix may
    /// not exist on this follower, so recovery treats the verified checkpoint
    /// as the source of the supplied base position.
    InstalledSnapshotMarkWrite {
        published: PublishedSnapshot,
        message: Message,
        end: u64,
        len: u32,
    },
    InstalledSnapshotMarkFsync {
        published: PublishedSnapshot,
        message: Message,
        end: u64,
    },
    WalCompactWrite {
        file: FileId,
        len: u32,
        new_len: u64,
        post_effects: Vec<Effect>,
    },
    WalCompactFsync {
        file: FileId,
        new_len: u64,
        post_effects: Vec<Effect>,
    },
    WalCompactRename {
        from: FileId,
        new_len: u64,
        post_effects: Vec<Effect>,
    },
    WalCompactDirectorySync {
        new_len: u64,
        post_effects: Vec<Effect>,
    },
    StoreWalCompactWrite {
        file: FileId,
        post_effects: Vec<Effect>,
    },
    StoreWalCompactFsync {
        file: FileId,
        post_effects: Vec<Effect>,
    },
    StoreWalCompactRename {
        from: FileId,
        post_effects: Vec<Effect>,
    },
    StoreWalCompactDirectorySync {
        post_effects: Vec<Effect>,
    },
    SnapshotSendRead {
        file: FileId,
        peer: NodeId,
        offset: u64,
        end: u64,
    },
}

impl IoStage {
    fn file(&self) -> FileId {
        match self {
            Self::Write { file, .. }
            | Self::Fsync { file }
            | Self::SnapshotWrite { file, .. }
            | Self::SnapshotFsync { file, .. }
            | Self::SnapshotChunkRead { file, .. }
            | Self::SnapshotDuplicateRead { file, .. }
            | Self::SnapshotDirectorySync { file, .. }
            | Self::LocalSnapshotWrite { file, .. }
            | Self::LocalSnapshotFsync { file, .. } => *file,
            Self::SnapshotRename { from, .. } | Self::LocalSnapshotRename { from, .. } => *from,
            Self::LocalSnapshotDirectorySync { published } => published.file,
            Self::SnapshotManifestWrite { published, .. }
            | Self::SnapshotManifestFsync { published, .. }
            | Self::SnapshotManifestDirectorySync { published, .. } => FileId::Manifest {
                generation: published.index.get(),
            },
            Self::LocalSnapshotMarkWrite { .. }
            | Self::LocalSnapshotMarkFsync { .. }
            | Self::InstalledSnapshotMarkWrite { .. }
            | Self::InstalledSnapshotMarkFsync { .. } => FileId::Wal { segment: 0 },
            Self::WalCompactWrite { file, .. } | Self::WalCompactFsync { file, .. } => *file,
            Self::WalCompactRename { from, .. } => *from,
            Self::WalCompactDirectorySync { .. } => FileId::Wal { segment: 0 },
            Self::StoreWalCompactWrite { file, .. } | Self::StoreWalCompactFsync { file, .. } => {
                *file
            }
            Self::StoreWalCompactRename { from, .. } => *from,
            Self::StoreWalCompactDirectorySync { .. } => FileId::StoreWal { segment: 0 },
            Self::SnapshotSendRead { file, .. } => *file,
        }
    }
}

#[derive(Clone, Debug)]
struct IncomingSnapshot {
    source: NodeId,
    leader_term: cc_core::Term,
    transfer_id: u64,
    index: cc_core::LogIndex,
    snapshot_term: cc_core::Term,
    total_len: u64,
    crc32c: u32,
    file: FileId,
    next_offset: u64,
    inflight: bool,
    decoder: Option<CcsnStreamDecoder>,
    decoded: Option<CcsnSnapshot>,
}

#[derive(Clone, Debug)]
struct OutgoingSnapshot {
    peer: NodeId,
    leader_term: cc_core::Term,
    transfer_id: u64,
    index: cc_core::LogIndex,
    snapshot_term: cc_core::Term,
    file: FileId,
    total_len: u64,
    crc32c: u32,
    next_offset: u64,
    inflight_end: u64,
}

#[derive(Clone, Copy, Debug)]
struct PublishedSnapshot {
    file: FileId,
    index: cc_core::LogIndex,
    snapshot_term: cc_core::Term,
    total_len: u64,
    crc32c: u32,
    store_sequence: u64,
}

#[derive(Clone, Copy, Debug)]
struct CompletedSnapshot {
    source: NodeId,
    leader_term: cc_core::Term,
    transfer_id: u64,
    index: cc_core::LogIndex,
    snapshot_term: cc_core::Term,
    total_len: u64,
    crc32c: u32,
}

#[derive(Clone, Debug)]
struct BlockedStep {
    until: Time,
    outcome: Result<Vec<NodeEffect>, NodeError>,
}

#[derive(Clone, Debug)]
struct PendingInput {
    input: Input,
    bytes: usize,
}

/// One owner of the environment boundary.  The driver assigns logical I/O
/// ids, keeps timer generations out of Raft, and only releases a continuation
/// after the write *and* the matching fsync completion have succeeded.
#[derive(Clone)]
pub struct Driver {
    node: Node,
    limits: cc_core::HostLimits,
    timers: BTreeMap<TimerId, TimerState>,
    pending_io: BTreeMap<IoId, IoStage>,
    pending_peer_inputs: VecDeque<PendingInput>,
    pending_timer_inputs: VecDeque<PendingInput>,
    pending_io_inputs: VecDeque<PendingInput>,
    pending_client_inputs: VecDeque<PendingInput>,
    next_io: u64,
    next_wal_offset: u64,
    next_store_wal_offset: u64,
    blocked: Option<BlockedStep>,
    peak_pending_input_bytes: usize,
    incoming_snapshot: Option<IncomingSnapshot>,
    completed_snapshot: Option<CompletedSnapshot>,
    outgoing_snapshots: BTreeMap<NodeId, OutgoingSnapshot>,
    published_snapshot: Option<PublishedSnapshot>,
    pending_snapshot_peers: std::collections::BTreeSet<NodeId>,
    next_temp_file: u64,
    wal_genesis: Option<Genesis>,
    footprint_peaks: RefCell<FootprintPeaks>,
}

impl Driver {
    pub fn boot(config: NodeConfig, state: BootState) -> Result<Self, HostError> {
        Self::boot_with_wal_offset(config, state, 0)
    }

    /// Boot from a durable prefix already present in the host WAL file.
    /// Recovery owns semantic replay; the driver needs only the exact byte
    /// boundary so each subsequent write appends to verified records.
    pub fn boot_with_wal_offset(
        config: NodeConfig,
        state: BootState,
        next_wal_offset: u64,
    ) -> Result<Self, HostError> {
        Self::boot_with_offsets(config, state, next_wal_offset, 0)
    }

    pub fn boot_with_offsets(
        config: NodeConfig,
        state: BootState,
        next_wal_offset: u64,
        next_store_wal_offset: u64,
    ) -> Result<Self, HostError> {
        Self::boot_with_optional_genesis(
            config,
            state,
            next_wal_offset,
            next_store_wal_offset,
            None,
        )
    }

    pub fn boot_with_offsets_and_genesis(
        config: NodeConfig,
        state: BootState,
        next_wal_offset: u64,
        next_store_wal_offset: u64,
        genesis: Genesis,
    ) -> Result<Self, HostError> {
        if genesis.cluster_id != config.cluster_id
            || genesis.policy.encode() != config.policy.encode()
        {
            return Err(HostError::Node(NodeError::Environment(
                "WAL genesis/config mismatch",
            )));
        }
        Self::boot_with_optional_genesis(
            config,
            state,
            next_wal_offset,
            next_store_wal_offset,
            Some(genesis),
        )
    }

    fn boot_with_optional_genesis(
        config: NodeConfig,
        state: BootState,
        next_wal_offset: u64,
        next_store_wal_offset: u64,
        wal_genesis: Option<Genesis>,
    ) -> Result<Self, HostError> {
        let limits = config.host_limits;
        let node = match state {
            BootState::Fresh { bootstrap } => Node::fresh(config, bootstrap),
            BootState::Recovered(recovered) => Node::restore(config, *recovered),
        }?;
        Ok(Self {
            node,
            limits,
            timers: BTreeMap::new(),
            pending_io: BTreeMap::new(),
            pending_peer_inputs: VecDeque::new(),
            pending_timer_inputs: VecDeque::new(),
            pending_io_inputs: VecDeque::new(),
            pending_client_inputs: VecDeque::new(),
            next_io: 1,
            next_wal_offset,
            next_store_wal_offset,
            blocked: None,
            peak_pending_input_bytes: 0,
            incoming_snapshot: None,
            completed_snapshot: None,
            outgoing_snapshots: BTreeMap::new(),
            published_snapshot: None,
            pending_snapshot_peers: std::collections::BTreeSet::new(),
            next_temp_file: 1,
            wal_genesis,
            footprint_peaks: RefCell::new(FootprintPeaks::default()),
        })
    }

    #[must_use]
    pub fn node(&self) -> &Node {
        &self.node
    }

    pub fn node_mut(&mut self) -> &mut Node {
        &mut self.node
    }

    #[must_use]
    pub fn role(&self) -> cc_cluster::Role {
        self.node.role()
    }

    #[must_use]
    pub fn membership(
        &self,
    ) -> (
        std::collections::BTreeSet<NodeId>,
        std::collections::BTreeSet<NodeId>,
        bool,
    ) {
        self.node.membership()
    }

    #[must_use]
    pub fn membership_state(&self) -> cc_core::MembershipState {
        self.node.raft.membership_state()
    }

    #[must_use]
    pub fn genesis(&self) -> Option<&Genesis> {
        self.wal_genesis.as_ref()
    }

    /// Host-facing configuration transition entry point.  Keeping this
    /// translation here prevents adapters from reintroducing the private
    /// `NodeEffect` vocabulary merely to schedule an already replicated
    /// membership operation.
    pub fn enter_joint(
        &mut self,
        now: Time,
        voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects = self.node.enter_joint(voters)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    /// Host-facing learner admission. It uses the same durable effect path as
    /// every other configuration entry so adapters cannot update membership
    /// behind the replicated log.
    pub fn add_learner(
        &mut self,
        now: Time,
        learner: NodeId,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects = self.node.add_learner(learner)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    /// Begin a joint-consensus promotion through the durable host boundary.
    pub fn promote_learner(
        &mut self,
        now: Time,
        learner: NodeId,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects = self.node.promote_learner(learner)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    /// Replicate the atomic-batch semantic feature fence. The node validates
    /// current member capability observations before appending; admission of
    /// `BATCH` changes only when the Config entry later applies.
    pub fn activate_atomic_batch(
        &mut self,
        now: Time,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects = self.node.activate_atomic_batch(now)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    /// Completes a previously committed joint-consensus transition through
    /// the same host boundary as ordinary inputs.
    pub fn leave_joint(&mut self, now: Time) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects = self.node.leave_joint()?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    pub fn admin_request(
        &mut self,
        now: Time,
        client: ClientId,
        request: RequestSeq,
        session: SessionKey,
        sequence: u64,
        operation: ConfigOperation,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects =
            self.node
                .admin_request(now, client, request.get(), session, sequence, operation)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    /// Start a semantic-v3 follower read without exposing `NodeEffect` to an
    /// adapter. The resulting `ClientReply` follows the same volatile route
    /// as a leader read; metadata remains in the node until the adapter takes
    /// it after receiving that reply.
    pub fn follower_read(
        &mut self,
        now: Time,
        client: ClientId,
        request: RequestSeq,
        command: cc_kv::KvCommand,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        self.require_idle_durability()?;
        let effects = self
            .node
            .request_follower_read(client, request.get(), command, now)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    fn require_idle_durability(&self) -> Result<(), HostError> {
        if self.pending_io.is_empty() {
            Ok(())
        } else {
            Err(HostError::Node(NodeError::PersistencePending))
        }
    }

    /// Begin one bounded, stop-and-wait snapshot transfer from a locally
    /// published checkpoint. A new local checkpoint is made durable before
    /// any peer chunk is read, so the sender never owns a per-peer full image.
    pub fn begin_snapshot_transfer(&mut self, peer: NodeId) -> Result<Vec<Effect>, HostError> {
        if peer.get() == 0 || peer == self.node.id() || self.node.role() != cc_cluster::Role::Leader
        {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot sender unavailable",
            )));
        }
        if self.outgoing_snapshots.contains_key(&peer) || !self.pending_snapshot_peers.insert(peer)
        {
            return Err(HostError::QueueFull(InputClass::Peer));
        }
        if self.published_snapshot_matches_applied() {
            return self.start_pending_snapshot_transfers();
        }
        if self.checkpoint_covers_joint_membership() || self.node.raft.has_retiring_peers() {
            // The current joint workflow's original AdminRequest identity is
            // anchored by its EnterJoint entry. Use an older checkpoint plus
            // the retained suffix until LeaveJoint commits; never compact the
            // anchor out from underneath a safe retry.
            return if self.published_snapshot.is_some() {
                self.start_pending_snapshot_transfers()
            } else {
                Ok(Vec::new())
            };
        }
        if self.pending_io.values().any(|stage| {
            matches!(
                stage,
                IoStage::LocalSnapshotWrite { .. }
                    | IoStage::LocalSnapshotFsync { .. }
                    | IoStage::LocalSnapshotRename { .. }
                    | IoStage::LocalSnapshotDirectorySync { .. }
            )
        }) {
            return Ok(Vec::new());
        }
        self.begin_local_snapshot_publication()
    }

    fn begin_local_snapshot_publication(&mut self) -> Result<Vec<Effect>, HostError> {
        let mut encoder = self
            .node
            .begin_ccsn_encode()
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot encode")))?;
        if encoder.total_len() > self.limits.max_snapshot_bytes {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot source too large",
            )));
        }
        let index = self.node.kv.applied_index;
        let snapshot_term = self.node.kv.applied_term;
        if index.get() == 0 || snapshot_term.get() == 0 {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot base unavailable",
            )));
        }
        let total_len = encoder.total_len();
        let file = FileId::Temp {
            sequence: self.allocate_temp_file(),
        };
        let published = PublishedSnapshot {
            file: FileId::Snapshot {
                generation: index.get(),
            },
            index,
            snapshot_term,
            total_len,
            crc32c: encoder.file_crc(),
            store_sequence: self.node.kv.store.last_sequence(),
        };
        let bytes = encoder
            .next_chunk(self.snapshot_chunk_bytes())
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot chunk")))?
            .ok_or(HostError::Node(NodeError::Environment("empty snapshot")))?;
        let len = u32::try_from(bytes.len())
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot chunk length")))?;
        let id = self.allocate_io()?;
        self.pending_io.insert(
            id,
            IoStage::LocalSnapshotWrite {
                file,
                published,
                at: 0,
                len,
                encoder: Box::new(encoder),
            },
        );
        Ok(vec![Effect::DiskWrite {
            file,
            at: 0,
            bytes,
            id,
        }])
    }

    fn published_snapshot_matches_applied(&self) -> bool {
        self.published_snapshot.is_some_and(|snapshot| {
            snapshot.index == self.node.kv.applied_index
                && snapshot.snapshot_term.get() != 0
                && snapshot.total_len > 0
        })
    }

    fn queue_snapshot_mark(
        &mut self,
        published: PublishedSnapshot,
        installed: Option<(Message, u64)>,
    ) -> Result<Vec<Effect>, HostError> {
        let mark = SnapshotMark {
            index: published.index,
            term: published.snapshot_term,
            generation: published.index.get(),
            crc32c: published.crc32c,
        };
        let record = if installed.is_some() {
            DurableRecord::InstalledSnapshotMark(mark)
        } else {
            DurableRecord::SnapshotMark(mark)
        };
        let bytes = encode_framed_durable_record(&record)
            .map_err(|_| HostError::Node(NodeError::Durability))?;
        let len = u32::try_from(bytes.len()).map_err(|_| HostError::Node(NodeError::Durability))?;
        let at = self.next_wal_offset;
        let end = self
            .next_wal_offset
            .checked_add(u64::from(len))
            .ok_or(HostError::IoIdExhausted)?;
        if end > self.limits.max_raft_log_bytes {
            return Err(HostError::ResourceLimit("max_raft_log_bytes"));
        }
        self.next_wal_offset = end;
        let id = self.allocate_io()?;
        let stage = match installed {
            Some((message, end)) => IoStage::InstalledSnapshotMarkWrite {
                published,
                message,
                end,
                len,
            },
            None => IoStage::LocalSnapshotMarkWrite { published, len },
        };
        self.pending_io.insert(id, stage);
        Ok(vec![Effect::DiskWrite {
            file: FileId::Wal { segment: 0 },
            at,
            bytes,
            id,
        }])
    }

    fn queue_snapshot_manifest(
        &mut self,
        published: PublishedSnapshot,
        installed: Option<(Message, u64)>,
    ) -> Result<Vec<Effect>, HostError> {
        let checkpoint = ManifestCheckpoint {
            index: published.index,
            term: published.snapshot_term,
            generation: published.index.get(),
            crc32c: published.crc32c,
        };
        let mut manifest = ManifestV2::empty(published.index.get());
        manifest
            .append_edit_batch(vec![
                ManifestEditV2::AppliedWatermark {
                    watermark: StoreWatermark {
                        index: published.index,
                        term: published.snapshot_term,
                        last_leader_time: self.node.kv.last_leader_time(),
                    },
                    store_sequence: published.store_sequence,
                },
                ManifestEditV2::Checkpoint(Some(checkpoint)),
            ])
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot manifest")))?;
        let bytes = encode_manifest_v2(&manifest)
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot manifest encode")))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
            > u64::from(self.limits.max_manifest_record_bytes)
        {
            return Err(HostError::ResourceLimit("max_manifest_record_bytes"));
        }
        let len = u32::try_from(bytes.len())
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot manifest length")))?;
        let id = self.allocate_io()?;
        self.pending_io.insert(
            id,
            IoStage::SnapshotManifestWrite {
                published,
                installed,
                len,
            },
        );
        Ok(vec![Effect::DiskWrite {
            file: FileId::Manifest {
                generation: published.index.get(),
            },
            at: 0,
            bytes,
            id,
        }])
    }

    fn finish_checkpoint_publication(
        &mut self,
        published: PublishedSnapshot,
        mut post_effects: Vec<Effect>,
    ) -> Result<Vec<Effect>, HostError> {
        self.published_snapshot = Some(published);
        let Some(genesis) = self.wal_genesis.clone() else {
            post_effects.extend(self.start_pending_snapshot_transfers()?);
            return Ok(post_effects);
        };
        let mark = SnapshotMark {
            index: published.index,
            term: published.snapshot_term,
            generation: published.index.get(),
            crc32c: published.crc32c,
        };
        let mut bytes =
            encode_framed_durable_record(&DurableRecord::Genesis(Box::new(genesis.clone())))
                .map_err(|_| HostError::Node(NodeError::Durability))?;
        let hard = self.node.raft.hard_state;
        if hard.term.get() != 0 || hard.voted_for.is_some() {
            bytes.extend_from_slice(
                &encode_framed_durable_record(&DurableRecord::Hard(hard))
                    .map_err(|_| HostError::Node(NodeError::Durability))?,
            );
        }
        bytes.extend_from_slice(
            &encode_framed_durable_record(&DurableRecord::InstalledSnapshotMark(mark))
                .map_err(|_| HostError::Node(NodeError::Durability))?,
        );
        for entry in &self.node.raft.log {
            bytes.extend_from_slice(
                &encode_framed_durable_record(&DurableRecord::Append(entry.clone()))
                    .map_err(|_| HostError::Node(NodeError::Durability))?,
            );
        }
        let recovered = cc_log::recover_framed_record_stream(&bytes)
            .map_err(|_| HostError::Node(NodeError::Durability))?;
        if recovered.torn_tail_truncated
            || recovered.state.genesis != genesis
            || recovered.state.hard_state != hard
            || recovered.state.base_index != published.index
            || recovered.state.base_term != published.snapshot_term
            || recovered.state.snapshot != Some(mark)
            || recovered.state.entries != self.node.raft.log
        {
            return Err(HostError::Node(NodeError::Durability));
        }
        let new_len =
            u64::try_from(bytes.len()).map_err(|_| HostError::Node(NodeError::Durability))?;
        if new_len > self.limits.max_raft_log_bytes {
            return Err(HostError::ResourceLimit("max_raft_log_bytes"));
        }
        let len = u32::try_from(bytes.len()).map_err(|_| HostError::Node(NodeError::Durability))?;
        let file = FileId::Temp {
            sequence: self.allocate_temp_file(),
        };
        let id = self.allocate_io()?;
        self.pending_io.insert(
            id,
            IoStage::WalCompactWrite {
                file,
                len,
                new_len,
                post_effects,
            },
        );
        Ok(vec![Effect::DiskWrite {
            file,
            at: 0,
            bytes,
            id,
        }])
    }

    fn begin_store_wal_compaction(
        &mut self,
        post_effects: Vec<Effect>,
    ) -> Result<Vec<Effect>, HostError> {
        let file = FileId::Temp {
            sequence: self.allocate_temp_file(),
        };
        let id = self.allocate_io()?;
        self.pending_io
            .insert(id, IoStage::StoreWalCompactWrite { file, post_effects });
        Ok(vec![Effect::DiskWrite {
            file,
            at: 0,
            bytes: Vec::new(),
            id,
        }])
    }

    fn start_pending_snapshot_transfers(&mut self) -> Result<Vec<Effect>, HostError> {
        let snapshot = self
            .published_snapshot
            .ok_or(HostError::Node(NodeError::Environment(
                "published snapshot unavailable",
            )))?;
        let peers = std::mem::take(&mut self.pending_snapshot_peers);
        let mut output = Vec::new();
        for peer in peers {
            if self.outgoing_snapshots.contains_key(&peer) {
                continue;
            }
            let leader_term = self.node.raft.hard_state.term;
            let transfer_id = self.node.raft.allocate_snapshot_transfer_id();
            self.outgoing_snapshots.insert(
                peer,
                OutgoingSnapshot {
                    peer,
                    leader_term,
                    transfer_id,
                    index: snapshot.index,
                    snapshot_term: snapshot.snapshot_term,
                    file: snapshot.file,
                    total_len: snapshot.total_len,
                    crc32c: snapshot.crc32c,
                    next_offset: 0,
                    inflight_end: 0,
                },
            );
            output.extend(self.send_snapshot_chunk(peer)?);
        }
        Ok(output)
    }

    fn finish_snapshot_send_read(
        &mut self,
        peer: NodeId,
        offset: u64,
        end: u64,
        data: Vec<u8>,
    ) -> Result<Vec<Effect>, HostError> {
        let sender =
            self.outgoing_snapshots
                .get(&peer)
                .ok_or(HostError::Node(NodeError::Environment(
                    "snapshot sender missing",
                )))?;
        if sender.next_offset != offset
            || sender.inflight_end != end
            || u64::try_from(data.len()).unwrap_or(u64::MAX) != end.saturating_sub(offset)
        {
            self.outgoing_snapshots.remove(&peer);
            return Err(HostError::Node(NodeError::Environment(
                "snapshot source read range",
            )));
        }
        let message = Message {
            proto_version: cc_cluster::PROTOCOL_VERSION,
            from: self.node.id(),
            to: sender.peer,
            term: sender.leader_term,
            kind: MessageKind::SnapshotChunk {
                transfer_id: sender.transfer_id,
                last_included_index: sender.index,
                last_included_term: sender.snapshot_term,
                total_len: sender.total_len,
                snapshot_crc32c: sender.crc32c,
                offset,
                data,
                done: end == sender.total_len,
            },
        };
        Ok(vec![Effect::Send {
            to: peer,
            msg: encode_peer_effect(&message)?,
        }])
    }

    fn allocate_temp_file(&mut self) -> u64 {
        let file = self.next_temp_file.max(1);
        self.next_temp_file = self.next_temp_file.saturating_add(1).max(1);
        file
    }

    fn snapshot_chunk_bytes(&self) -> usize {
        usize::try_from(
            self.limits
                .max_snapshot_chunk_bytes
                .min(u64::try_from(cc_cluster::SNAPSHOT_CHUNK_BYTES).expect("chunk fits")),
        )
        .expect("validated snapshot chunk fits usize")
    }

    /// Re-send the one currently outstanding chunk. Hosts call this from a
    /// transfer timeout; it never advances the offset and therefore cannot
    /// skip a durable receiver acknowledgement.
    pub fn retry_snapshot_transfer(&mut self, peer: NodeId) -> Result<Vec<Effect>, HostError> {
        if !self.outgoing_snapshots.contains_key(&peer) {
            return Ok(Vec::new());
        }
        self.send_snapshot_chunk(peer)
    }

    #[must_use]
    pub fn snapshot_transfer_active(&self, peer: NodeId) -> bool {
        self.pending_snapshot_peers.contains(&peer) || self.outgoing_snapshots.contains_key(&peer)
    }

    /// Record the CCHL generation selected for a peer before the first frame
    /// from that generation reaches the core.
    pub fn observe_peer_capability(
        &mut self,
        peer: NodeId,
        semantic_version: u16,
        features: u64,
    ) -> Result<(), HostError> {
        self.node
            .observe_peer_capability(peer, semantic_version, features)?;
        Ok(())
    }

    /// Remove capability evidence when the owning CCHL connection generation
    /// closes or fails.
    pub fn forget_peer_capability(&mut self, peer: NodeId) {
        self.node.forget_peer_capability(peer);
    }

    pub fn take_follower_read_metadata(
        &mut self,
        client: ClientId,
        request: RequestSeq,
    ) -> Option<cc_cluster::FollowerReadMetadata> {
        self.node.take_follower_read_metadata(client, request.get())
    }

    /// Register a checkpoint that the adapter recovered from a durable
    /// published file before the driver started accepting inputs.  This lets
    /// a later leader stream that exact file without regenerating or
    /// overwriting it.
    pub fn register_published_snapshot(
        &mut self,
        file: FileId,
        index: LogIndex,
        snapshot_term: cc_core::Term,
        store_sequence: u64,
        total_len: u64,
        crc32c: u32,
    ) -> Result<(), HostError> {
        let FileId::Snapshot { generation } = file else {
            return Err(HostError::Node(NodeError::Environment("snapshot file id")));
        };
        if generation != index.get()
            || index.get() == 0
            || index > self.node.kv.applied_index
            || snapshot_term.get() == 0
            || store_sequence > self.node.kv.store.last_sequence()
            || total_len == 0
        {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot registration",
            )));
        }
        self.published_snapshot = Some(PublishedSnapshot {
            file,
            index,
            snapshot_term,
            total_len,
            crc32c,
            store_sequence,
        });
        Ok(())
    }

    pub fn deliver(
        &mut self,
        now: Time,
        input: Input,
        blocks: &mut dyn BlockSource,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        if let Some(blocked) = &self.blocked {
            return Ok((DriverPoll::BlockedUntil(blocked.until), Vec::new()));
        }
        if !self.pending_io.is_empty() && !matches!(&input, Input::IoDone { .. }) {
            // Filesystem effects are one ordered transaction chain. In
            // particular, a peer append must not enter the in-memory Raft log
            // while a snapshot image built from the prior log is waiting to
            // replace the WAL. Real hosts enqueue this error at the serialized
            // admission boundary and replay the input after the final fsync.
            return Err(HostError::Node(NodeError::PersistencePending));
        }
        match input {
            Input::IoDone { id, result } => self.complete_io(id, result),
            Input::TimerFired { id, generation } => self.fire_timer(now, id, generation),
            other => {
                let step = self.node.on_env_input(now, other, blocks);
                self.defer_or_translate(now, step.synchronous_service, step.outcome)
            }
        }
    }

    /// Admit an input for deterministic, class-aware delivery. I/O completion
    /// is drained first, then timer and peer work, and finally client work.
    /// A full class queue refuses only that class; it never lets a client flood
    /// consume consensus-completion capacity.
    pub fn enqueue(&mut self, input: Input) -> Result<(), HostError> {
        let class = input_class(&input);
        let bytes = input_charge(&input);
        if self.pending_input_count() >= self.limits.max_driver_pending_inputs
            || self
                .pending_input_bytes()
                .checked_add(bytes)
                .is_none_or(|total| total > self.limits.max_driver_pending_input_bytes)
        {
            return Err(HostError::QueueFull(class));
        }
        let queue = match class {
            InputClass::Peer => &mut self.pending_peer_inputs,
            InputClass::Timer => &mut self.pending_timer_inputs,
            InputClass::Io => &mut self.pending_io_inputs,
            InputClass::Client => &mut self.pending_client_inputs,
        };
        let limit = match class {
            InputClass::Peer => self.limits.max_pending_peer,
            InputClass::Timer => self.limits.max_pending_timer,
            InputClass::Io => self.limits.max_pending_io,
            InputClass::Client => self.limits.max_pending_client,
        };
        if queue.len() >= limit {
            return Err(HostError::QueueFull(class));
        }
        queue.push_back(PendingInput { input, bytes });
        self.peak_pending_input_bytes = self
            .peak_pending_input_bytes
            .max(self.pending_input_bytes());
        Ok(())
    }

    /// Deliver exactly one previously admitted input. The caller chooses the
    /// delivery time; queueing is host-local and must not sneak wall-clock time
    /// into the deterministic state machine.
    pub fn deliver_next(
        &mut self,
        now: Time,
        blocks: &mut dyn BlockSource,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        let (_input, poll, effects) = self.deliver_next_with_input(now, blocks)?;
        Ok((poll, effects))
    }

    /// Like [`Self::deliver_next`], but returns the admitted input alongside
    /// its result. Real recorders need that exact association to emit one CCIJ
    /// record per Driver transition without reimplementing host scheduling.
    pub fn deliver_next_with_input(
        &mut self,
        now: Time,
        blocks: &mut dyn BlockSource,
    ) -> Result<(Option<Input>, DriverPoll, Vec<Effect>), HostError> {
        if let Some(blocked) = &self.blocked {
            return Ok((None, DriverPoll::BlockedUntil(blocked.until), Vec::new()));
        }
        let input = self
            .pending_io_inputs
            .pop_front()
            .or_else(|| self.pending_timer_inputs.pop_front())
            .or_else(|| self.pending_peer_inputs.pop_front())
            .or_else(|| self.pending_client_inputs.pop_front())
            .map(|pending| pending.input);
        match input {
            Some(input) => match self.deliver(now, input.clone(), blocks) {
                Ok((poll, effects)) => Ok((Some(input), poll, effects)),
                Err(
                    HostError::Node(NodeError::NotLeader | NodeError::FeatureDisabled)
                    | HostError::Node(NodeError::Kv(cc_kv::KvError::Busy)),
                ) if matches!(&input, Input::ClientRequest { .. }) => {
                    let (client, req) = match &input {
                        Input::ClientRequest { client, req, .. } => (*client, *req),
                        _ => unreachable!("guarded client request"),
                    };
                    Ok((
                        Some(input),
                        DriverPoll::Ready,
                        vec![Effect::ClientReply {
                            client,
                            req,
                            reply: encode_client_reply(&cc_kv::KvReply::Error(
                                cc_kv::KvError::Busy,
                            )),
                        }],
                    ))
                }
                Err(error) => Err(error),
            },
            None => Ok((None, DriverPoll::Ready, Vec::new())),
        }
    }

    pub fn release_ready(&mut self, now: Time) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        let Some(blocked) = &self.blocked else {
            return Ok((DriverPoll::Ready, Vec::new()));
        };
        if now < blocked.until {
            return Ok((DriverPoll::BlockedUntil(blocked.until), Vec::new()));
        }
        let blocked = self.blocked.take().expect("checked blocked step");
        let effects = self.translate(blocked.outcome?)?;
        Ok((DriverPoll::Ready, effects))
    }

    pub fn armed_timers(&self) -> impl Iterator<Item = (TimerId, Time, u64)> + '_ {
        self.timers
            .iter()
            .map(|(id, state)| (*id, state.at, state.generation))
    }

    #[must_use]
    pub fn footprint(&self) -> NodeFootprint {
        let core = self.node.resource_usage();
        let policy = self.node.cluster_policy();
        let mut peaks = self.footprint_peaks.borrow_mut();
        let usage = |current: u64, limit: u64, peak: &mut u64| {
            *peak = (*peak).max(current);
            Usage {
                current,
                peak: *peak,
                limit,
            }
        };
        let snapshot_staging = self
            .incoming_snapshot
            .as_ref()
            .map_or(core.snapshot_staging_bytes, |snapshot| snapshot.next_offset);
        let driver_effects = u64::try_from(self.pending_io.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(64);
        let checkpoint_builder = if self
            .pending_io
            .values()
            .any(|stage| matches!(stage, IoStage::LocalSnapshotWrite { .. }))
        {
            u64::try_from(self.snapshot_chunk_bytes()).unwrap_or(u64::MAX)
        } else {
            0
        };
        NodeFootprint {
            log: usage(
                core.log_bytes,
                self.limits.max_raft_log_bytes,
                &mut peaks.log,
            ),
            snapshot_staging: usage(
                snapshot_staging,
                self.limits.max_snapshot_staging_bytes,
                &mut peaks.snapshot_staging,
            ),
            sessions: usage(
                core.session_bytes,
                policy.max_session_bytes,
                &mut peaks.sessions,
            ),
            session_tombstones: usage(
                core.session_tombstone_bytes,
                policy.max_session_tombstones.saturating_mul(33),
                &mut peaks.session_tombstones,
            ),
            pending_reads: usage(
                core.pending_read_bytes,
                self.limits.max_pending_read_bytes,
                &mut peaks.pending_reads,
            ),
            pending_client_routes: usage(
                core.pending_client_route_bytes,
                self.limits.max_pending_client_routes.saturating_mul(32),
                &mut peaks.pending_client_routes,
            ),
            memtables: usage(
                core.memtable_bytes,
                self.limits.max_memtable_bytes,
                &mut peaks.memtables,
            ),
            sst_metadata: usage(
                core.sst_metadata_bytes,
                self.limits.max_sst_metadata_bytes,
                &mut peaks.sst_metadata,
            ),
            driver_effects: usage(
                driver_effects,
                self.limits.max_driver_pending_effect_bytes,
                &mut peaks.driver_effects,
            ),
            outbound_frames: usage(
                0,
                self.limits.max_network_inflight_bytes,
                &mut peaks.outbound_frames,
            ),
            checkpoint_builder: usage(
                checkpoint_builder,
                self.limits.max_checkpoint_builder_bytes,
                &mut peaks.checkpoint_builder,
            ),
            compaction_builder: usage(
                0,
                self.limits.max_compaction_builder_bytes,
                &mut peaks.compaction_builder,
            ),
            armed_timers: self.timers.len(),
            pending_io: self.pending_io.len(),
            pending_peer_inputs: self.pending_peer_inputs.len(),
            pending_timer_inputs: self.pending_timer_inputs.len(),
            pending_io_inputs: self.pending_io_inputs.len(),
            pending_client_inputs: self.pending_client_inputs.len(),
            pending_input_bytes: self.pending_input_bytes(),
            driver_inputs: Usage {
                current: u64::try_from(self.pending_input_bytes()).unwrap_or(u64::MAX),
                peak: u64::try_from(self.peak_pending_input_bytes).unwrap_or(u64::MAX),
                limit: u64::try_from(self.limits.max_driver_pending_input_bytes)
                    .unwrap_or(u64::MAX),
            },
            blocked: self.blocked.is_some(),
        }
    }

    fn fire_timer(
        &mut self,
        now: Time,
        id: TimerId,
        generation: u64,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        let Some(timer) = self.timers.get(&id).copied() else {
            // Stale wakeups are a normal host race.  They cannot reach Raft.
            return Ok((DriverPoll::Ready, Vec::new()));
        };
        if timer.generation != generation || now < timer.at {
            return Ok((DriverPoll::Ready, Vec::new()));
        }
        self.timers.remove(&id);
        let outcome = self.node.on_input(cc_cluster::NodeInput::Timer {
            now,
            kind: timer.kind,
        });
        self.defer_or_translate(now, Duration::from_nanos(0), outcome)
    }

    fn defer_or_translate(
        &mut self,
        now: Time,
        service: Duration,
        outcome: Result<Vec<NodeEffect>, NodeError>,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        if service.as_nanos() == 0 {
            return Ok((DriverPoll::Ready, self.translate(outcome?)?));
        }
        let until = now.checked_add(service).ok_or(HostError::TimeOverflow)?;
        self.blocked = Some(BlockedStep { until, outcome });
        Ok((DriverPoll::BlockedUntil(until), Vec::new()))
    }

    fn complete_io(
        &mut self,
        id: IoId,
        result: IoResult,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        let Some(stage) = self.pending_io.remove(&id) else {
            return Err(HostError::UnknownIo(id));
        };
        match (stage, result) {
            (IoStage::Write { file, len }, IoResult::Written { len: actual }) if len == actual => {
                #[cfg(feature = "kata03")]
                {
                    let _ = file;
                    // Synthetic teaching defect: publish the continuation
                    // after write completion without an fsync acknowledgement.
                    let effects = self
                        .node
                        .on_input(cc_cluster::NodeInput::Persisted { success: true })?;
                    return Ok((DriverPoll::Ready, self.translate(effects)?));
                }
                #[cfg(not(feature = "kata03"))]
                {
                    let fsync_id = self.allocate_io()?;
                    self.pending_io.insert(fsync_id, IoStage::Fsync { file });
                    Ok((
                        DriverPoll::Ready,
                        vec![Effect::DiskFsync { file, id: fsync_id }],
                    ))
                }
            }
            (IoStage::Fsync { file }, IoResult::Fsynced) => {
                let _ = file;
                let effects = self
                    .node
                    .on_input(cc_cluster::NodeInput::Persisted { success: true })?;
                Ok((DriverPoll::Ready, self.translate(effects)?))
            }
            (
                IoStage::SnapshotWrite {
                    file,
                    len: expected,
                    message,
                    end,
                    ..
                },
                IoResult::Written { len: actual },
            ) if actual == expected => {
                if !self.snapshot_message_is_current(&message) {
                    return Ok((DriverPoll::Ready, Vec::new()));
                }
                let fsync_id = self.allocate_io()?;
                self.pending_io
                    .insert(fsync_id, IoStage::SnapshotFsync { file, message, end });
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync { file, id: fsync_id }],
                ))
            }
            (IoStage::SnapshotFsync { file, message, end }, IoResult::Fsynced) => {
                if !self.snapshot_message_is_current(&message) {
                    return Ok((DriverPoll::Ready, Vec::new()));
                }
                let (at, len) = match &message.kind {
                    MessageKind::SnapshotChunk { offset, data, .. } => (
                        *offset,
                        u32::try_from(data.len())
                            .map_err(|_| HostError::InvalidIoCompletion(id))?,
                    ),
                    _ => return Err(HostError::InvalidIoCompletion(id)),
                };
                let read_id = self.allocate_io()?;
                self.pending_io
                    .insert(read_id, IoStage::SnapshotChunkRead { file, message, end });
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskRead {
                        file,
                        at,
                        len,
                        id: read_id,
                    }],
                ))
            }
            (IoStage::SnapshotChunkRead { message, end, .. }, IoResult::Read(actual)) => Ok((
                DriverPoll::Ready,
                if self.snapshot_message_is_current(&message) {
                    self.finish_snapshot_chunk_read(message, end, actual)?
                } else {
                    Vec::new()
                },
            )),
            (
                IoStage::SnapshotDuplicateRead {
                    message,
                    expected,
                    next_offset,
                    ..
                },
                IoResult::Read(actual),
            ) => Ok((
                DriverPoll::Ready,
                self.finish_snapshot_duplicate(message, expected, actual, next_offset)?,
            )),
            (
                IoStage::SnapshotRename {
                    to, message, end, ..
                },
                IoResult::Fsynced,
            ) => {
                if !self.snapshot_message_is_current(&message) {
                    return Ok((DriverPoll::Ready, Vec::new()));
                }
                let sync_id = self.allocate_io()?;
                self.pending_io.insert(
                    sync_id,
                    IoStage::SnapshotDirectorySync {
                        file: to,
                        message,
                        end,
                    },
                );
                Ok((DriverPoll::Ready, vec![Effect::DiskSyncDir { id: sync_id }]))
            }
            (IoStage::SnapshotDirectorySync { message, end, .. }, IoResult::Fsynced) => {
                if !self.snapshot_message_is_current(&message) {
                    return Ok((DriverPoll::Ready, Vec::new()));
                }
                let Some(current) = self.incoming_snapshot.as_ref() else {
                    return Err(HostError::Node(NodeError::Environment(
                        "snapshot marker without staging",
                    )));
                };
                if current.next_offset != end || current.decoded.is_none() {
                    return Err(HostError::Node(NodeError::Environment(
                        "snapshot marker state",
                    )));
                }
                let published = PublishedSnapshot {
                    file: FileId::Snapshot {
                        generation: current.index.get(),
                    },
                    index: current.index,
                    snapshot_term: current.snapshot_term,
                    total_len: current.total_len,
                    crc32c: current.crc32c,
                    store_sequence: current
                        .decoded
                        .as_ref()
                        .expect("checked decoded checkpoint")
                        .kv
                        .store_sequence,
                };
                Ok((
                    DriverPoll::Ready,
                    self.queue_snapshot_manifest(published, Some((message, end)))?,
                ))
            }
            (
                IoStage::LocalSnapshotWrite {
                    file,
                    published,
                    at,
                    len: expected,
                    mut encoder,
                },
                IoResult::Written { len },
            ) if len == expected => {
                let next_at = at
                    .checked_add(u64::from(expected))
                    .ok_or(HostError::InvalidIoCompletion(id))?;
                if let Some(bytes) = encoder
                    .next_chunk(self.snapshot_chunk_bytes())
                    .map_err(|_| HostError::InvalidIoCompletion(id))?
                {
                    let len = u32::try_from(bytes.len())
                        .map_err(|_| HostError::InvalidIoCompletion(id))?;
                    let write_id = self.allocate_io()?;
                    self.pending_io.insert(
                        write_id,
                        IoStage::LocalSnapshotWrite {
                            file,
                            published,
                            at: next_at,
                            len,
                            encoder,
                        },
                    );
                    Ok((
                        DriverPoll::Ready,
                        vec![Effect::DiskWrite {
                            file,
                            at: next_at,
                            bytes,
                            id: write_id,
                        }],
                    ))
                } else if next_at == published.total_len {
                    let fsync_id = self.allocate_io()?;
                    self.pending_io
                        .insert(fsync_id, IoStage::LocalSnapshotFsync { file, published });
                    Ok((
                        DriverPoll::Ready,
                        vec![Effect::DiskFsync { file, id: fsync_id }],
                    ))
                } else {
                    Err(HostError::InvalidIoCompletion(id))
                }
            }
            (IoStage::LocalSnapshotFsync { file, published }, IoResult::Fsynced) => {
                let rename_id = self.allocate_io()?;
                self.pending_io.insert(
                    rename_id,
                    IoStage::LocalSnapshotRename {
                        from: file,
                        published,
                    },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskRename {
                        from: file,
                        to: published.file,
                        id: rename_id,
                    }],
                ))
            }
            (IoStage::LocalSnapshotRename { published, .. }, IoResult::Fsynced) => {
                let sync_id = self.allocate_io()?;
                self.pending_io
                    .insert(sync_id, IoStage::LocalSnapshotDirectorySync { published });
                Ok((DriverPoll::Ready, vec![Effect::DiskSyncDir { id: sync_id }]))
            }
            (IoStage::LocalSnapshotDirectorySync { published }, IoResult::Fsynced) => Ok((
                DriverPoll::Ready,
                self.queue_snapshot_manifest(published, None)?,
            )),
            (
                IoStage::SnapshotManifestWrite {
                    published,
                    installed,
                    len: expected,
                },
                IoResult::Written { len },
            ) if len == expected => {
                let fsync_id = self.allocate_io()?;
                self.pending_io.insert(
                    fsync_id,
                    IoStage::SnapshotManifestFsync {
                        published,
                        installed,
                    },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync {
                        file: FileId::Manifest {
                            generation: published.index.get(),
                        },
                        id: fsync_id,
                    }],
                ))
            }
            (
                IoStage::SnapshotManifestFsync {
                    published,
                    installed,
                },
                IoResult::Fsynced,
            ) => {
                let sync_id = self.allocate_io()?;
                self.pending_io.insert(
                    sync_id,
                    IoStage::SnapshotManifestDirectorySync {
                        published,
                        installed,
                    },
                );
                Ok((DriverPoll::Ready, vec![Effect::DiskSyncDir { id: sync_id }]))
            }
            (
                IoStage::SnapshotManifestDirectorySync {
                    published,
                    installed,
                },
                IoResult::Fsynced,
            ) => Ok((
                DriverPoll::Ready,
                self.queue_snapshot_mark(published, installed)?,
            )),
            (
                IoStage::LocalSnapshotMarkWrite {
                    published,
                    len: expected,
                },
                IoResult::Written { len },
            ) if len == expected => {
                let fsync_id = self.allocate_io()?;
                self.pending_io
                    .insert(fsync_id, IoStage::LocalSnapshotMarkFsync { published });
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync {
                        file: FileId::Wal { segment: 0 },
                        id: fsync_id,
                    }],
                ))
            }
            (IoStage::LocalSnapshotMarkFsync { published }, IoResult::Fsynced) => {
                let snapshot_membership = self.node.raft.membership_state_at(published.index);
                self.node
                    .raft
                    .install_snapshot_state(published.index, published.snapshot_term);
                self.node
                    .raft
                    .restore_membership_state(snapshot_membership)
                    .map_err(NodeError::Raft)?;
                self.node.raft.replay_retained_membership_suffix();
                Ok((
                    DriverPoll::Ready,
                    self.finish_checkpoint_publication(published, Vec::new())?,
                ))
            }
            (
                IoStage::InstalledSnapshotMarkWrite {
                    published,
                    message,
                    end,
                    len: expected,
                },
                IoResult::Written { len },
            ) if len == expected => {
                let fsync_id = self.allocate_io()?;
                self.pending_io.insert(
                    fsync_id,
                    IoStage::InstalledSnapshotMarkFsync {
                        published,
                        message,
                        end,
                    },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync {
                        file: FileId::Wal { segment: 0 },
                        id: fsync_id,
                    }],
                ))
            }
            (
                IoStage::InstalledSnapshotMarkFsync {
                    published,
                    message,
                    end,
                },
                IoResult::Fsynced,
            ) => {
                let effects = self.finish_snapshot_install(message, end)?;
                Ok((
                    DriverPoll::Ready,
                    self.finish_checkpoint_publication(published, effects)?,
                ))
            }
            (
                IoStage::WalCompactWrite {
                    file,
                    len: expected,
                    new_len,
                    post_effects,
                },
                IoResult::Written { len },
            ) if len == expected => {
                let fsync_id = self.allocate_io()?;
                self.pending_io.insert(
                    fsync_id,
                    IoStage::WalCompactFsync {
                        file,
                        new_len,
                        post_effects,
                    },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync { file, id: fsync_id }],
                ))
            }
            (
                IoStage::WalCompactFsync {
                    file,
                    new_len,
                    post_effects,
                },
                IoResult::Fsynced,
            ) => {
                let rename_id = self.allocate_io()?;
                self.pending_io.insert(
                    rename_id,
                    IoStage::WalCompactRename {
                        from: file,
                        new_len,
                        post_effects,
                    },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskRename {
                        from: file,
                        to: FileId::Wal { segment: 0 },
                        id: rename_id,
                    }],
                ))
            }
            (
                IoStage::WalCompactRename {
                    new_len,
                    post_effects,
                    ..
                },
                IoResult::Fsynced,
            ) => {
                let sync_id = self.allocate_io()?;
                self.pending_io.insert(
                    sync_id,
                    IoStage::WalCompactDirectorySync {
                        new_len,
                        post_effects,
                    },
                );
                Ok((DriverPoll::Ready, vec![Effect::DiskSyncDir { id: sync_id }]))
            }
            (
                IoStage::WalCompactDirectorySync {
                    new_len,
                    post_effects,
                    ..
                },
                IoResult::Fsynced,
            ) => {
                self.next_wal_offset = new_len;
                Ok((
                    DriverPoll::Ready,
                    self.begin_store_wal_compaction(post_effects)?,
                ))
            }
            (
                IoStage::StoreWalCompactWrite { file, post_effects },
                IoResult::Written { len: 0 },
            ) => {
                let fsync_id = self.allocate_io()?;
                self.pending_io.insert(
                    fsync_id,
                    IoStage::StoreWalCompactFsync { file, post_effects },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync { file, id: fsync_id }],
                ))
            }
            (IoStage::StoreWalCompactFsync { file, post_effects }, IoResult::Fsynced) => {
                // The rename is the linearization point for the live store-WAL
                // image. Reset the logical append cursor before exposing the
                // rename effect. A real host may execute effects on a different
                // worker from the one that delivered the transition; keeping
                // the old cursor until the following directory sync allowed an
                // already-admitted apply to target the pre-compaction offset
                // after the zero-length image was installed.
                self.next_store_wal_offset = 0;
                let rename_id = self.allocate_io()?;
                self.pending_io.insert(
                    rename_id,
                    IoStage::StoreWalCompactRename {
                        from: file,
                        post_effects,
                    },
                );
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskRename {
                        from: file,
                        to: FileId::StoreWal { segment: 0 },
                        id: rename_id,
                    }],
                ))
            }
            (IoStage::StoreWalCompactRename { post_effects, .. }, IoResult::Fsynced) => {
                let sync_id = self.allocate_io()?;
                self.pending_io.insert(
                    sync_id,
                    IoStage::StoreWalCompactDirectorySync { post_effects },
                );
                Ok((DriverPoll::Ready, vec![Effect::DiskSyncDir { id: sync_id }]))
            }
            (IoStage::StoreWalCompactDirectorySync { mut post_effects }, IoResult::Fsynced) => {
                post_effects.extend(self.start_pending_snapshot_transfers()?);
                Ok((DriverPoll::Ready, post_effects))
            }
            (
                IoStage::SnapshotSendRead {
                    peer, offset, end, ..
                },
                IoResult::Read(data),
            ) => Ok((
                DriverPoll::Ready,
                self.finish_snapshot_send_read(peer, offset, end, data)?,
            )),
            (
                stage @ (IoStage::SnapshotWrite { .. }
                | IoStage::SnapshotFsync { .. }
                | IoStage::SnapshotChunkRead { .. }
                | IoStage::SnapshotDuplicateRead { .. }
                | IoStage::SnapshotRename { .. }
                | IoStage::SnapshotDirectorySync { .. }
                | IoStage::LocalSnapshotWrite { .. }
                | IoStage::LocalSnapshotFsync { .. }
                | IoStage::LocalSnapshotRename { .. }
                | IoStage::LocalSnapshotDirectorySync { .. }
                | IoStage::SnapshotManifestWrite { .. }
                | IoStage::SnapshotManifestFsync { .. }
                | IoStage::SnapshotManifestDirectorySync { .. }
                | IoStage::LocalSnapshotMarkWrite { .. }
                | IoStage::LocalSnapshotMarkFsync { .. }
                | IoStage::InstalledSnapshotMarkWrite { .. }
                | IoStage::InstalledSnapshotMarkFsync { .. }
                | IoStage::WalCompactWrite { .. }
                | IoStage::WalCompactFsync { .. }
                | IoStage::WalCompactRename { .. }
                | IoStage::WalCompactDirectorySync { .. }
                | IoStage::StoreWalCompactWrite { .. }
                | IoStage::StoreWalCompactFsync { .. }
                | IoStage::StoreWalCompactRename { .. }
                | IoStage::StoreWalCompactDirectorySync { .. }
                | IoStage::SnapshotSendRead { .. }),
                IoResult::Failed(_),
            ) => {
                self.incoming_snapshot = None;
                Err(HostError::Durability {
                    id,
                    file: stage.file(),
                })
            }
            (
                _stage @ (IoStage::SnapshotWrite { .. }
                | IoStage::SnapshotFsync { .. }
                | IoStage::SnapshotChunkRead { .. }
                | IoStage::SnapshotDuplicateRead { .. }
                | IoStage::SnapshotRename { .. }
                | IoStage::SnapshotDirectorySync { .. }
                | IoStage::LocalSnapshotWrite { .. }
                | IoStage::LocalSnapshotFsync { .. }
                | IoStage::LocalSnapshotRename { .. }
                | IoStage::LocalSnapshotDirectorySync { .. }
                | IoStage::SnapshotManifestWrite { .. }
                | IoStage::SnapshotManifestFsync { .. }
                | IoStage::SnapshotManifestDirectorySync { .. }
                | IoStage::LocalSnapshotMarkWrite { .. }
                | IoStage::LocalSnapshotMarkFsync { .. }
                | IoStage::InstalledSnapshotMarkWrite { .. }
                | IoStage::InstalledSnapshotMarkFsync { .. }
                | IoStage::WalCompactWrite { .. }
                | IoStage::WalCompactFsync { .. }
                | IoStage::WalCompactRename { .. }
                | IoStage::WalCompactDirectorySync { .. }
                | IoStage::StoreWalCompactWrite { .. }
                | IoStage::StoreWalCompactFsync { .. }
                | IoStage::StoreWalCompactRename { .. }
                | IoStage::StoreWalCompactDirectorySync { .. }
                | IoStage::SnapshotSendRead { .. }),
                _,
            ) => {
                self.incoming_snapshot = None;
                Err(HostError::InvalidIoCompletion(id))
            }
            (stage, IoResult::Failed(_)) => {
                let file = stage.file();
                let _ = self
                    .node
                    .on_input(cc_cluster::NodeInput::Persisted { success: false });
                Err(HostError::Durability { id, file })
            }
            _ => {
                let _ = self
                    .node
                    .on_input(cc_cluster::NodeInput::Persisted { success: false });
                Err(HostError::InvalidIoCompletion(id))
            }
        }
    }

    fn translate(&mut self, source: Vec<NodeEffect>) -> Result<Vec<Effect>, HostError> {
        self.prune_stale_snapshot_senders();
        let mut output = Vec::new();
        for effect in source {
            match effect {
                NodeEffect::Send(message) => {
                    output.push(Effect::Send {
                        to: message.to,
                        msg: encode_peer_effect(&message)?,
                    });
                }
                NodeEffect::ReceiveSnapshotChunk(message) => {
                    output.extend(self.stage_snapshot_chunk(message)?);
                }
                NodeEffect::ReceiveSnapshotAck(message) => {
                    output.extend(self.advance_snapshot_transfer(message)?);
                }
                effect @ (NodeEffect::PersistHard(_)
                | NodeEffect::PersistEntries(_)
                | NodeEffect::TruncateSuffix(_)) => {
                    let bytes = encode_durability_effect(&effect)?
                        .ok_or(HostError::Node(NodeError::Durability))?;
                    output.push(self.issue_raw_wal_write(bytes)?);
                }
                NodeEffect::PersistStore { bytes } => {
                    output.push(self.issue_store_wal_write(bytes)?);
                }
                NodeEffect::ClientReply {
                    client,
                    sequence,
                    reply,
                }
                | NodeEffect::ReadReply {
                    client,
                    sequence,
                    reply,
                } => output.push(Effect::ClientReply {
                    client,
                    req: RequestSeq::new(sequence),
                    reply: encode_client_reply(&reply),
                }),
                NodeEffect::AdminReply {
                    client,
                    sequence,
                    reply,
                } => output.push(Effect::ClientReply {
                    client,
                    req: RequestSeq::new(sequence),
                    reply: reply.encode(),
                }),
                NodeEffect::ArmTimer { id, at, kind } => {
                    let generation = self
                        .timers
                        .get(&id)
                        .map_or(1, |prior| prior.generation.saturating_add(1));
                    self.timers.insert(
                        id,
                        TimerState {
                            at,
                            generation,
                            kind,
                        },
                    );
                    output.push(Effect::SetTimer { id, fire_at: at });
                    if kind == TimerKind::Heartbeat {
                        // Snapshot chunks are stop-and-wait, so the sender
                        // cannot infer delivery from a socket write. Couple
                        // retransmission to the already-bounded Raft heartbeat
                        // cadence: a lost final ACK must not leave a learner
                        // permanently installed at the checkpoint base while
                        // ordinary suffix replication is suppressed.
                        let peers = self.outgoing_snapshots.keys().copied().collect::<Vec<_>>();
                        for peer in peers {
                            output.extend(self.retry_snapshot_transfer(peer)?);
                        }
                    }
                }
                // Detailed trace payloads are produced by the recorder.  This
                // legacy marker deliberately has no lossy synthetic Event.
                NodeEffect::Trace(_) => {}
            }
        }
        output.extend(self.maybe_begin_local_checkpoint()?);
        output.extend(self.maybe_begin_snapshot_transfers()?);
        Ok(output)
    }

    fn prune_stale_snapshot_senders(&mut self) {
        if self.node.role() != cc_cluster::Role::Leader {
            self.outgoing_snapshots.clear();
            self.pending_snapshot_peers.clear();
            return;
        }
        let term = self.node.raft.hard_state.term;
        let membership = self.node.raft.membership_state();
        self.outgoing_snapshots.retain(|peer, sender| {
            sender.leader_term == term
                && (membership.voters.contains(peer) || membership.learners.contains(peer))
        });
        self.pending_snapshot_peers
            .retain(|peer| membership.voters.contains(peer) || membership.learners.contains(peer));
    }

    fn issue_raw_wal_write(&mut self, bytes: Vec<u8>) -> Result<Effect, HostError> {
        if self.pending_io.len() >= self.limits.max_pending_io {
            return Err(HostError::QueueFull(InputClass::Io));
        }
        let len = u32::try_from(bytes.len()).map_err(|_| HostError::Node(NodeError::Durability))?;
        let at = self.next_wal_offset;
        let end = self
            .next_wal_offset
            .checked_add(u64::from(len))
            .ok_or(HostError::IoIdExhausted)?;
        if end > self.limits.max_raft_log_bytes {
            return Err(HostError::ResourceLimit("max_raft_log_bytes"));
        }
        self.next_wal_offset = end;
        let id = self.allocate_io()?;
        let file = FileId::Wal { segment: 0 };
        self.pending_io.insert(id, IoStage::Write { file, len });
        Ok(Effect::DiskWrite {
            file,
            at,
            bytes,
            id,
        })
    }

    fn issue_store_wal_write(&mut self, bytes: Vec<u8>) -> Result<Effect, HostError> {
        if self.pending_io.len() >= self.limits.max_pending_io {
            return Err(HostError::QueueFull(InputClass::Io));
        }
        let len = u32::try_from(bytes.len()).map_err(|_| HostError::Node(NodeError::Durability))?;
        let at = self.next_store_wal_offset;
        let end = self
            .next_store_wal_offset
            .checked_add(u64::from(len))
            .ok_or(HostError::IoIdExhausted)?;
        if end > self.limits.max_store_wal_bytes {
            return Err(HostError::ResourceLimit("max_store_wal_bytes"));
        }
        self.next_store_wal_offset = end;
        let id = self.allocate_io()?;
        let file = FileId::StoreWal { segment: 0 };
        self.pending_io.insert(id, IoStage::Write { file, len });
        Ok(Effect::DiskWrite {
            file,
            at,
            bytes,
            id,
        })
    }

    fn send_snapshot_chunk(&mut self, peer: NodeId) -> Result<Vec<Effect>, HostError> {
        let chunk_bytes = self.snapshot_chunk_bytes();
        let sender = self
            .outgoing_snapshots
            .get_mut(&peer)
            .ok_or(HostError::Node(NodeError::Environment(
                "snapshot sender missing",
            )))?;
        let offset = sender.next_offset;
        if offset >= sender.total_len {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot source exhausted",
            )));
        }
        let end_offset = offset
            .checked_add(u64::try_from(chunk_bytes).expect("chunk fits"))
            .map(|end| end.min(sender.total_len))
            .ok_or(HostError::Node(NodeError::Environment(
                "snapshot chunk range",
            )))?;
        sender.inflight_end = end_offset;
        let len = u32::try_from(end_offset.saturating_sub(offset))
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot chunk length")))?;
        let file = sender.file;
        let id = self.allocate_io()?;
        self.pending_io.insert(
            id,
            IoStage::SnapshotSendRead {
                file,
                peer,
                offset,
                end: end_offset,
            },
        );
        Ok(vec![Effect::DiskRead {
            file,
            at: offset,
            len,
            id,
        }])
    }

    fn advance_snapshot_transfer(&mut self, message: Message) -> Result<Vec<Effect>, HostError> {
        let MessageKind::SnapshotAck {
            transfer_id,
            next_offset,
            accepted,
            reason,
        } = message.kind
        else {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot acknowledgement",
            )));
        };
        let peer = message.from;
        let Some(sender) = self.outgoing_snapshots.get(&peer) else {
            return Ok(Vec::new());
        };
        if sender.transfer_id != transfer_id
            || sender.leader_term != message.term
            || message.to != self.node.id()
        {
            return Ok(Vec::new());
        }
        let total_len = sender.total_len;
        if accepted {
            if next_offset != sender.inflight_end || next_offset > total_len {
                self.outgoing_snapshots.remove(&peer);
                return Ok(Vec::new());
            }
            if next_offset == total_len {
                if let Some(sender) = self.outgoing_snapshots.get(&peer) {
                    self.node.raft.match_index.insert(peer, sender.index);
                    self.node
                        .raft
                        .next_index
                        .insert(peer, LogIndex::new(sender.index.get().saturating_add(1)));
                }
                self.outgoing_snapshots.remove(&peer);
                let membership = self.node.raft.membership_state();
                if self.node.role() != cc_cluster::Role::Leader
                    || (!membership.voters.contains(&peer) && !membership.learners.contains(&peer))
                {
                    // The final acknowledgement can race a leader step-down
                    // or committed removal. The checkpoint remains installed;
                    // only the old leader's optional suffix nudge is stale.
                    return Ok(Vec::new());
                }
                let effects = self.node.replicate_peer(peer)?;
                return self.translate(effects);
            }
            if let Some(sender) = self.outgoing_snapshots.get_mut(&peer) {
                sender.next_offset = next_offset;
                sender.inflight_end = next_offset;
            }
            return self.send_snapshot_chunk(peer);
        }
        match reason {
            Some(SnapshotRejectReason::RestartFromZero) | Some(SnapshotRejectReason::Gap)
                if next_offset <= total_len =>
            {
                if let Some(sender) = self.outgoing_snapshots.get_mut(&peer) {
                    sender.next_offset = next_offset;
                    sender.inflight_end = next_offset;
                }
                self.send_snapshot_chunk(peer)
            }
            _ => {
                self.outgoing_snapshots.remove(&peer);
                Ok(Vec::new())
            }
        }
    }

    fn maybe_begin_snapshot_transfers(&mut self) -> Result<Vec<Effect>, HostError> {
        if self.node.role() != cc_cluster::Role::Leader
            || self.node.kv.applied_index.get() == 0
            || self.node.kv.applied_term.get() == 0
        {
            return Ok(Vec::new());
        }
        let peers = self
            .node
            .raft
            .voters
            .iter()
            .chain(self.node.raft.learners.iter())
            .copied()
            .filter(|peer| *peer != self.node.id())
            .filter(|peer| {
                let snapshot_index = self.node.raft.snapshot_base().0;
                snapshot_index.get() != 0
                    && self
                        .node
                        .raft
                        .next_index
                        .get(peer)
                        .is_some_and(|next| *next <= snapshot_index)
                    && !self.outgoing_snapshots.contains_key(peer)
                    && !self.pending_snapshot_peers.contains(peer)
            })
            .collect::<Vec<_>>();
        let mut output = Vec::new();
        for peer in peers {
            output.extend(self.begin_snapshot_transfer(peer)?);
        }
        Ok(output)
    }

    /// Every node checkpoints its own applied state after the durable Raft
    /// prefix crosses the configured trigger.  The task is coalesced with an
    /// active checkpoint and starts only when no unrelated durability barrier
    /// could make the captured state ambiguous.
    fn maybe_begin_local_checkpoint(&mut self) -> Result<Vec<Effect>, HostError> {
        if !self.pending_io.is_empty()
            || self.node.kv.applied_index.get() == 0
            || self.node.kv.applied_term.get() == 0
            || self.published_snapshot_matches_applied()
            || self.next_wal_offset < self.limits.max_log_bytes_before_snapshot
            || self.checkpoint_covers_joint_membership()
            || self.node.raft.has_retiring_peers()
        {
            return Ok(Vec::new());
        }
        self.begin_local_snapshot_publication()
    }

    fn checkpoint_covers_joint_membership(&self) -> bool {
        self.node
            .raft
            .membership_state_at(self.node.kv.applied_index)
            .joint
            .is_some()
    }

    fn stage_snapshot_chunk(&mut self, message: Message) -> Result<Vec<Effect>, HostError> {
        let MessageKind::SnapshotChunk {
            transfer_id,
            last_included_index,
            last_included_term,
            total_len,
            snapshot_crc32c,
            offset,
            data,
            done: _,
        } = &message.kind
        else {
            return Err(HostError::Node(NodeError::Environment("snapshot effect")));
        };
        let end = offset
            .checked_add(u64::try_from(data.len()).unwrap_or(u64::MAX))
            .ok_or(HostError::Node(NodeError::Environment("snapshot range")))?;
        if *total_len > self.limits.max_snapshot_bytes
            || u64::try_from(data.len()).unwrap_or(u64::MAX) > self.limits.max_snapshot_chunk_bytes
        {
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::TooLarge),
            );
        }
        if let Some(receipt) = self.completed_snapshot
            && receipt.source == message.from
            && receipt.leader_term == message.term
            && receipt.transfer_id == *transfer_id
            && receipt.index == *last_included_index
            && receipt.snapshot_term == *last_included_term
            && receipt.total_len == *total_len
            && receipt.crc32c == *snapshot_crc32c
        {
            return self.snapshot_ack(&message, *transfer_id, *total_len, true, None);
        }
        let reset = match self.incoming_snapshot.as_ref() {
            None => {
                if *offset != 0 {
                    return self.snapshot_ack(
                        &message,
                        *transfer_id,
                        0,
                        false,
                        Some(SnapshotRejectReason::RestartFromZero),
                    );
                }
                true
            }
            Some(current)
                if current.source == message.from
                    && current.leader_term == message.term
                    && current.transfer_id == *transfer_id
                    && current.index == *last_included_index
                    && current.snapshot_term == *last_included_term
                    && current.total_len == *total_len
                    && current.crc32c == *snapshot_crc32c =>
            {
                false
            }
            Some(current)
                if current.source == message.from
                    && *transfer_id > current.transfer_id
                    && *offset == 0 =>
            {
                true
            }
            Some(current) if message.term > current.leader_term && *offset == 0 => true,
            Some(_current) => {
                return self.snapshot_ack(
                    &message,
                    *transfer_id,
                    0,
                    false,
                    Some(SnapshotRejectReason::Conflict),
                );
            }
        };
        if reset {
            // Reuse the one bounded staging file when a newer-term leader
            // supersedes an interrupted transfer. The first offset-zero write
            // truncates it, so repeated elections cannot leak one file each.
            let file = self.incoming_snapshot.as_ref().map_or_else(
                || FileId::Temp {
                    sequence: message
                        .from
                        .get()
                        .rotate_left(23)
                        .wrapping_add(*transfer_id)
                        .max(1),
                },
                |current| current.file,
            );
            self.incoming_snapshot = Some(IncomingSnapshot {
                source: message.from,
                leader_term: message.term,
                transfer_id: *transfer_id,
                index: *last_included_index,
                snapshot_term: *last_included_term,
                total_len: *total_len,
                crc32c: *snapshot_crc32c,
                file,
                next_offset: 0,
                inflight: false,
                decoder: Some(self.node.begin_ccsn_decode()),
                decoded: None,
            });
        }
        let gap = {
            let current = self
                .incoming_snapshot
                .as_ref()
                .expect("reset creates snapshot staging");
            (current.inflight || *offset > current.next_offset).then_some(current.next_offset)
        };
        if let Some(next_offset) = gap {
            return self.snapshot_ack(
                &message,
                *transfer_id,
                next_offset,
                false,
                Some(SnapshotRejectReason::Gap),
            );
        }
        let duplicate_file = {
            let current = self
                .incoming_snapshot
                .as_ref()
                .expect("reset creates snapshot staging");
            if *offset < current.next_offset {
                (end <= current.next_offset).then_some((current.file, current.next_offset))
            } else {
                None
            }
        };
        if let Some((file, next_offset)) = duplicate_file {
            let id = self.allocate_io()?;
            self.pending_io.insert(
                id,
                IoStage::SnapshotDuplicateRead {
                    file,
                    message: message.clone(),
                    expected: data.clone(),
                    next_offset,
                },
            );
            if let Some(current) = self.incoming_snapshot.as_mut() {
                current.inflight = true;
            }
            return Ok(vec![Effect::DiskRead {
                file,
                at: *offset,
                len: u32::try_from(data.len()).map_err(|_| {
                    HostError::Node(NodeError::Environment("snapshot duplicate length"))
                })?,
                id,
            }]);
        }
        if *offset
            < self
                .incoming_snapshot
                .as_ref()
                .expect("snapshot exists")
                .next_offset
        {
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Conflict),
            );
        }
        let length = u32::try_from(data.len())
            .map_err(|_| HostError::Node(NodeError::Environment("snapshot chunk length")))?;
        let file = {
            let current = self
                .incoming_snapshot
                .as_mut()
                .expect("reset creates snapshot staging");
            current.inflight = true;
            current.file
        };
        let id = self.allocate_io()?;
        self.pending_io.insert(
            id,
            IoStage::SnapshotWrite {
                file,
                len: length,
                message: message.clone(),
                end,
            },
        );
        Ok(vec![Effect::DiskWrite {
            file,
            at: *offset,
            bytes: data.clone(),
            id,
        }])
    }

    fn snapshot_message_is_current(&self, message: &Message) -> bool {
        let MessageKind::SnapshotChunk {
            transfer_id,
            last_included_index,
            last_included_term,
            total_len,
            snapshot_crc32c,
            ..
        } = &message.kind
        else {
            return false;
        };
        self.incoming_snapshot.as_ref().is_some_and(|current| {
            current.source == message.from
                && current.leader_term == message.term
                && current.transfer_id == *transfer_id
                && current.index == *last_included_index
                && current.snapshot_term == *last_included_term
                && current.total_len == *total_len
                && current.crc32c == *snapshot_crc32c
        })
    }

    fn finish_snapshot_chunk_read(
        &mut self,
        message: Message,
        end: u64,
        actual: Vec<u8>,
    ) -> Result<Vec<Effect>, HostError> {
        let MessageKind::SnapshotChunk {
            transfer_id,
            total_len,
            snapshot_crc32c,
            data,
            done,
            ..
        } = &message.kind
        else {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot completion",
            )));
        };
        if actual != *data {
            self.incoming_snapshot = None;
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            );
        }
        let decoder = {
            let Some(current) = self.incoming_snapshot.as_mut() else {
                return self.snapshot_ack(
                    &message,
                    *transfer_id,
                    0,
                    false,
                    Some(SnapshotRejectReason::RestartFromZero),
                );
            };
            current.inflight = false;
            current.next_offset = end;
            let result = current
                .decoder
                .as_mut()
                .ok_or(HostError::Node(NodeError::Environment("snapshot decoder")))?
                .push(&actual);
            if result.is_err() {
                None
            } else if *done {
                current.decoder.take()
            } else {
                return self.snapshot_ack(&message, *transfer_id, end, true, None);
            }
        };
        let Some(decoder) = decoder else {
            self.incoming_snapshot = None;
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            );
        };
        if end != *total_len {
            self.incoming_snapshot = None;
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            );
        }
        let Ok((snapshot, file_crc)) = decoder.finish() else {
            self.incoming_snapshot = None;
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            );
        };
        if file_crc != *snapshot_crc32c
            || self.node.validate_decoded_ccsn_snapshot(&snapshot).is_err()
        {
            self.incoming_snapshot = None;
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            );
        }
        let Some(current) = self.incoming_snapshot.as_mut() else {
            return self.snapshot_ack(
                &message,
                *transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::RestartFromZero),
            );
        };
        current.decoded = Some(snapshot);
        let from = current.file;
        let snapshot = FileId::Snapshot {
            generation: current.index.get(),
        };
        let rename_id = self.allocate_io()?;
        self.pending_io.insert(
            rename_id,
            IoStage::SnapshotRename {
                from,
                to: snapshot,
                message,
                end,
            },
        );
        Ok(vec![Effect::DiskRename {
            from,
            to: snapshot,
            id: rename_id,
        }])
    }

    fn finish_snapshot_duplicate(
        &mut self,
        message: Message,
        expected: Vec<u8>,
        actual: Vec<u8>,
        next_offset: u64,
    ) -> Result<Vec<Effect>, HostError> {
        let MessageKind::SnapshotChunk { transfer_id, .. } = message.kind else {
            return Err(HostError::Node(NodeError::Environment(
                "snapshot duplicate",
            )));
        };
        let Some(current) = self.incoming_snapshot.as_mut() else {
            return self.snapshot_ack(
                &message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::RestartFromZero),
            );
        };
        current.inflight = false;
        if actual == expected {
            return self.snapshot_ack(&message, transfer_id, next_offset, true, None);
        }
        self.incoming_snapshot = None;
        self.snapshot_ack(
            &message,
            transfer_id,
            0,
            false,
            Some(SnapshotRejectReason::Conflict),
        )
    }

    fn finish_snapshot_install(
        &mut self,
        message: Message,
        end: u64,
    ) -> Result<Vec<Effect>, HostError> {
        let MessageKind::SnapshotChunk {
            transfer_id,
            total_len,
            snapshot_crc32c,
            ..
        } = message.kind
        else {
            return Err(HostError::Node(NodeError::Environment("snapshot install")));
        };
        let Some(current) = self.incoming_snapshot.as_mut() else {
            return self.snapshot_ack(
                &message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::RestartFromZero),
            );
        };
        let valid = end == total_len;
        let completed = CompletedSnapshot {
            source: current.source,
            leader_term: current.leader_term,
            transfer_id,
            index: current.index,
            snapshot_term: current.snapshot_term,
            total_len,
            crc32c: snapshot_crc32c,
        };
        let decoded = current.decoded.take();
        if !valid
            || decoded.is_none()
            || self
                .node
                .install_decoded_ccsn_snapshot(decoded.expect("checked some"))
                .is_err()
        {
            self.incoming_snapshot = None;
            return self.snapshot_ack(
                &message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            );
        }
        self.completed_snapshot = Some(completed);
        self.incoming_snapshot = None;
        self.snapshot_ack(&message, transfer_id, end, true, None)
    }

    fn snapshot_ack(
        &self,
        message: &Message,
        transfer_id: u64,
        next_offset: u64,
        accepted: bool,
        reason: Option<SnapshotRejectReason>,
    ) -> Result<Vec<Effect>, HostError> {
        Ok(vec![Effect::Send {
            to: message.from,
            msg: encode_peer_effect(&Message {
                proto_version: cc_cluster::PROTOCOL_VERSION,
                from: self.node.id(),
                to: message.from,
                term: self.node.raft.hard_state.term,
                kind: MessageKind::SnapshotAck {
                    transfer_id,
                    next_offset,
                    accepted,
                    reason,
                },
            })?,
        }])
    }

    fn allocate_io(&mut self) -> Result<IoId, HostError> {
        let id = self.next_io;
        self.next_io = self
            .next_io
            .checked_add(1)
            .ok_or(HostError::IoIdExhausted)?;
        Ok(IoId::new(id))
    }

    fn pending_input_count(&self) -> usize {
        self.pending_peer_inputs
            .len()
            .saturating_add(self.pending_timer_inputs.len())
            .saturating_add(self.pending_io_inputs.len())
            .saturating_add(self.pending_client_inputs.len())
    }

    fn pending_input_bytes(&self) -> usize {
        self.pending_peer_inputs
            .iter()
            .chain(self.pending_timer_inputs.iter())
            .chain(self.pending_io_inputs.iter())
            .chain(self.pending_client_inputs.iter())
            .fold(0_usize, |total, pending| {
                total.saturating_add(pending.bytes)
            })
    }
}

const fn input_class(input: &Input) -> InputClass {
    match input {
        Input::Recv { .. } => InputClass::Peer,
        Input::IoDone { .. } => InputClass::Io,
        Input::TimerFired { .. } | Input::Tick => InputClass::Timer,
        Input::ClientRequest { .. } => InputClass::Client,
    }
}

fn input_charge(input: &Input) -> usize {
    const HEADER_BYTES: usize = 32;
    match input {
        Input::Recv { msg, .. } => HEADER_BYTES.saturating_add(msg.payload.len()),
        Input::ClientRequest { command, .. } => HEADER_BYTES.saturating_add(command.len()),
        Input::IoDone { result, .. } => match result {
            IoResult::Read(bytes) => HEADER_BYTES.saturating_add(bytes.len()),
            _ => HEADER_BYTES,
        },
        Input::TimerFired { .. } | Input::Tick => HEADER_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;

    use super::*;
    use cc_cluster::{Message, MessageKind, NodeConfig, PROTOCOL_VERSION, RaftConfig};
    use cc_core::{ClusterPolicy, HostLimits, NodeId, Seed};
    use cc_env::IoError;
    use cc_store::{MemoryBlockSource, StoreConfig};

    fn config() -> NodeConfig {
        NodeConfig {
            id: NodeId::new(1),
            cluster_id: [7; 16],
            seed: Seed::new(1),
            raft: RaftConfig::default(),
            store: StoreConfig::default(),
            policy: ClusterPolicy::default(),
            host_limits: HostLimits::default(),
        }
    }

    fn driver() -> Driver {
        Driver::boot(
            config(),
            BootState::Fresh {
                bootstrap: cc_core::MembershipState::new(
                    [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                )
                .expect("membership"),
            },
        )
        .expect("driver")
    }

    #[cfg(feature = "kata03")]
    #[test]
    fn trap_kata_03_ack_before_fsync_is_found_within_budget() {
        let mut driver = driver();
        let mut blocks = MemoryBlockSource::default();
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(1);
        let request = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let wire = encode_peer_effect(&request).expect("CCRP");
        let (_, write) = driver
            .deliver(
                Time::from_nanos(1),
                Input::Recv {
                    from: NodeId::new(2),
                    msg: wire,
                },
                &mut blocks,
            )
            .expect("vote request");
        let [Effect::DiskWrite { id, bytes, .. }] = write.as_slice() else {
            panic!("hard state must write first");
        };

        let (_, released) = driver
            .deliver(
                Time::from_nanos(1),
                Input::IoDone {
                    id: *id,
                    result: IoResult::Written {
                        len: u32::try_from(bytes.len()).expect("write length"),
                    },
                },
                &mut blocks,
            )
            .expect("write completion");

        assert!(matches!(released.as_slice(), [Effect::Send { .. }]));
        assert!(
            !released
                .iter()
                .any(|effect| matches!(effect, Effect::DiskFsync { .. })),
            "the synthetic continuation must escape before fsync"
        );
    }

    #[test]
    fn trap_driver_correlates_write_and_fsync_before_continuation() {
        let mut driver = driver();
        let mut blocks = MemoryBlockSource::default();
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(1);
        let request = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let wire = encode_peer_effect(&request).expect("CCRP");
        let (_, write) = driver
            .deliver(
                Time::from_nanos(1),
                Input::Recv {
                    from: NodeId::new(2),
                    msg: wire,
                },
                &mut blocks,
            )
            .expect("vote request");
        let [Effect::DiskWrite { id, bytes, .. }] = write.as_slice() else {
            panic!("hard state must write first");
        };
        let (_, fsync) = driver
            .deliver(
                Time::from_nanos(1),
                Input::IoDone {
                    id: *id,
                    result: IoResult::Written {
                        len: u32::try_from(bytes.len()).expect("write length"),
                    },
                },
                &mut blocks,
            )
            .expect("write completion");
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("write must be followed by fsync");
        };
        let (_, next) = driver
            .deliver(
                Time::from_nanos(1),
                Input::IoDone {
                    id: *id,
                    result: IoResult::Fsynced,
                },
                &mut blocks,
            )
            .expect("fsync completion");
        assert!(matches!(next.as_slice(), [Effect::Send { .. }]));
    }

    #[test]
    fn trap_lost_final_snapshot_ack_does_not_reinstall() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut source_config = config();
        source_config.id = NodeId::new(2);
        let mut source = Node::new(source_config, voters).expect("source");
        source
            .kv
            .store
            .put(b"snapshot-key", b"snapshot-value")
            .expect("source value");
        source.kv.applied_index = cc_core::LogIndex::new(4);
        source.kv.applied_term = cc_core::Term::new(1);
        source.raft.applied_index = cc_core::LogIndex::new(4);
        let bytes = source.encode_ccsn_snapshot().expect("CCSN");
        let message = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: MessageKind::SnapshotChunk {
                transfer_id: 7,
                last_included_index: cc_core::LogIndex::new(4),
                last_included_term: cc_core::Term::new(1),
                total_len: u64::try_from(bytes.len()).expect("snapshot length"),
                snapshot_crc32c: cc_cluster::ccsn_file_crc(&bytes).expect("CCSN checksum"),
                offset: 0,
                data: bytes,
                done: true,
            },
        };
        let mut driver = driver();
        let staged = driver
            .translate(vec![NodeEffect::ReceiveSnapshotChunk(message.clone())])
            .expect("stage snapshot");
        let [Effect::DiskWrite { id, bytes, .. }] = staged.as_slice() else {
            panic!("snapshot must stage before ack");
        };
        let staged_bytes = bytes.clone();
        let (_, fsync) = driver
            .complete_io(
                *id,
                IoResult::Written {
                    len: u32::try_from(bytes.len()).expect("chunk length"),
                },
            )
            .expect("stage write");
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("snapshot must fsync before install");
        };
        let (_, read) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("snapshot fsync");
        let [Effect::DiskRead { id, .. }] = read.as_slice() else {
            panic!("snapshot must be read from durable staging before install");
        };
        let (_, rename) = driver
            .complete_io(*id, IoResult::Read(staged_bytes.clone()))
            .expect("snapshot staged read");
        let [Effect::DiskRename { id, .. }] = rename.as_slice() else {
            panic!("validated snapshot must publish by atomic rename");
        };
        let (_, sync_dir) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("snapshot rename");
        let [Effect::DiskSyncDir { id }] = sync_dir.as_slice() else {
            panic!("published snapshot must sync its directory");
        };
        let (_, manifest_write) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("snapshot directory sync");
        let mark_write = complete_checkpoint_manifest(&mut driver, manifest_write);
        let [Effect::DiskWrite { id, bytes, .. }] = mark_write.as_slice() else {
            panic!("published snapshot must record a WAL mark before install");
        };
        let (_, mark_fsync) = driver
            .complete_io(
                *id,
                IoResult::Written {
                    len: u32::try_from(bytes.len()).expect("mark length"),
                },
            )
            .expect("snapshot mark write");
        let [Effect::DiskFsync { id, .. }] = mark_fsync.as_slice() else {
            panic!("snapshot mark must fsync before install acknowledgement");
        };
        let (_, ack) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("snapshot mark fsync");
        assert!(matches!(ack.as_slice(), [Effect::Send { to, .. }] if *to == NodeId::new(2)));
        assert_eq!(
            driver.node().kv.store.get(b"snapshot-key", None),
            Some(b"snapshot-value".to_vec())
        );
        let applied = driver.node().raft.applied_index;
        let retry = driver
            .stage_snapshot_chunk(message)
            .expect("lost final Ack retry");
        assert!(matches!(retry.as_slice(), [Effect::Send { .. }]));
        assert_eq!(driver.node().raft.applied_index, applied);
        assert!(driver.incoming_snapshot.is_none());
    }

    fn test_snapshot_chunk(
        transfer_id: u64,
        offset: u64,
        data: &[u8],
        total_len: u64,
        checksum: u32,
        done: bool,
    ) -> Message {
        Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: MessageKind::SnapshotChunk {
                transfer_id,
                last_included_index: cc_core::LogIndex::new(4),
                last_included_term: cc_core::Term::new(1),
                total_len,
                snapshot_crc32c: checksum,
                offset,
                data: data.to_vec(),
                done,
            },
        }
    }

    fn snapshot_ack_fields(effects: &[Effect]) -> (bool, Option<SnapshotRejectReason>, u64) {
        let [Effect::Send { msg, .. }] = effects else {
            panic!("expected one snapshot Ack, got {effects:?}");
        };
        let decoded = cc_raft::codec::decode(&msg.payload).expect("CCRP Ack");
        let MessageKind::SnapshotAck {
            accepted,
            reason,
            next_offset,
            ..
        } = decoded.kind
        else {
            panic!("expected SnapshotAck");
        };
        (accepted, reason, next_offset)
    }

    fn drive_local_checkpoint(driver: &mut Driver, mut effects: Vec<Effect>) {
        let mut files = BTreeMap::<FileId, Vec<u8>>::new();
        while let Some(effect) = effects.pop() {
            let (id, result) = match effect {
                Effect::DiskWrite {
                    file,
                    at,
                    bytes,
                    id,
                } => {
                    let at = usize::try_from(at).expect("offset");
                    let image = files.entry(file).or_default();
                    if image.len() < at + bytes.len() {
                        image.resize(at + bytes.len(), 0);
                    }
                    image[at..at + bytes.len()].copy_from_slice(&bytes);
                    (
                        id,
                        IoResult::Written {
                            len: u32::try_from(bytes.len()).expect("write length"),
                        },
                    )
                }
                Effect::DiskFsync { id, .. } | Effect::DiskSyncDir { id } => {
                    (id, IoResult::Fsynced)
                }
                Effect::DiskRename { from, to, id } => {
                    let image = files.remove(&from).expect("rename source");
                    files.insert(to, image);
                    (id, IoResult::Fsynced)
                }
                other => panic!("unexpected local checkpoint effect {other:?}"),
            };
            let (_, next) = driver.complete_io(id, result).expect("checkpoint I/O");
            effects.extend(next.into_iter().rev());
        }
    }

    fn complete_checkpoint_manifest(driver: &mut Driver, effects: Vec<Effect>) -> Vec<Effect> {
        let [
            Effect::DiskWrite {
                file: FileId::Manifest { .. },
                id,
                bytes,
                ..
            },
        ] = effects.as_slice()
        else {
            panic!("checkpoint must publish its manifest before its WAL mark");
        };
        let (_, fsync) = driver
            .complete_io(
                *id,
                IoResult::Written {
                    len: u32::try_from(bytes.len()).expect("manifest length"),
                },
            )
            .expect("manifest write");
        let [
            Effect::DiskFsync {
                file: FileId::Manifest { .. },
                id,
            },
        ] = fsync.as_slice()
        else {
            panic!("checkpoint manifest must fsync");
        };
        let (_, sync) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("manifest fsync");
        let [Effect::DiskSyncDir { id }] = sync.as_slice() else {
            panic!("checkpoint manifest directory must fsync");
        };
        driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("manifest directory sync")
            .1
    }

    #[test]
    fn trap_snapshot_duplicate_chunk_is_idempotent() {
        let mut driver = driver();
        let message = test_snapshot_chunk(7, 0, b"abc", 10, 9, false);
        let write = driver
            .stage_snapshot_chunk(message.clone())
            .expect("first chunk");
        let [Effect::DiskWrite { id, .. }] = write.as_slice() else {
            panic!("first chunk write");
        };
        let (_, fsync) = driver
            .complete_io(*id, IoResult::Written { len: 3 })
            .expect("write");
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("first chunk fsync");
        };
        let (_, read) = driver.complete_io(*id, IoResult::Fsynced).expect("fsync");
        let [Effect::DiskRead { id, .. }] = read.as_slice() else {
            panic!("read staged chunk");
        };
        let (_, ack) = driver
            .complete_io(*id, IoResult::Read(b"abc".to_vec()))
            .expect("read");
        assert_eq!(snapshot_ack_fields(&ack), (true, None, 3));

        let duplicate = driver
            .stage_snapshot_chunk(message)
            .expect("duplicate chunk");
        let [Effect::DiskRead { id, .. }] = duplicate.as_slice() else {
            panic!("duplicate must compare durable staging bytes");
        };
        let (_, duplicate_ack) = driver
            .complete_io(*id, IoResult::Read(b"abc".to_vec()))
            .expect("duplicate read");
        assert_eq!(snapshot_ack_fields(&duplicate_ack), (true, None, 3));
        assert_eq!(
            driver
                .incoming_snapshot
                .as_ref()
                .expect("transfer")
                .next_offset,
            3
        );
    }

    #[test]
    fn trap_competing_snapshot_rejection_is_canonical_after_progress() {
        let mut driver = driver();
        let first = test_snapshot_chunk(7, 0, b"abc", 10, 9, false);
        let write = driver
            .stage_snapshot_chunk(first)
            .expect("first transfer chunk");
        let [Effect::DiskWrite { id, .. }] = write.as_slice() else {
            panic!("first transfer writes staging");
        };
        let (_, fsync) = driver
            .complete_io(*id, IoResult::Written { len: 3 })
            .expect("staging write");
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("staging write fsyncs");
        };
        let (_, read) = driver.complete_io(*id, IoResult::Fsynced).expect("fsync");
        let [Effect::DiskRead { id, .. }] = read.as_slice() else {
            panic!("staged chunk is verified before acknowledgement");
        };
        let (_, ack) = driver
            .complete_io(*id, IoResult::Read(b"abc".to_vec()))
            .expect("staged chunk verification");
        assert_eq!(snapshot_ack_fields(&ack), (true, None, 3));

        let competing = driver
            .stage_snapshot_chunk(test_snapshot_chunk(6, 0, b"old", 10, 9, false))
            .expect("older competing transfer gets a canonical response");
        assert_eq!(
            snapshot_ack_fields(&competing),
            (false, Some(SnapshotRejectReason::Conflict), 0)
        );
    }

    #[test]
    fn trap_snapshot_gap_is_rejected() {
        let mut driver = driver();
        driver
            .stage_snapshot_chunk(test_snapshot_chunk(7, 0, b"abc", 10, 9, false))
            .expect("first chunk");
        let gap = driver
            .stage_snapshot_chunk(test_snapshot_chunk(7, 4, b"x", 10, 9, false))
            .expect("gap Ack");
        assert_eq!(
            snapshot_ack_fields(&gap),
            (false, Some(SnapshotRejectReason::Gap), 0)
        );
        assert_eq!(
            driver
                .incoming_snapshot
                .as_ref()
                .expect("transfer")
                .next_offset,
            0
        );
    }

    #[test]
    fn trap_delayed_old_transfer_cannot_evict_new_staging() {
        let mut driver = driver();
        let old = driver
            .stage_snapshot_chunk(test_snapshot_chunk(7, 0, b"old", 10, 9, false))
            .expect("old transfer");
        let [Effect::DiskWrite { id: old_write, .. }] = old.as_slice() else {
            panic!("old write");
        };
        driver
            .stage_snapshot_chunk(test_snapshot_chunk(8, 0, b"new", 10, 9, false))
            .expect("new transfer");
        assert_eq!(
            driver
                .incoming_snapshot
                .as_ref()
                .expect("new staging")
                .transfer_id,
            8
        );
        let (_, stale) = driver
            .complete_io(*old_write, IoResult::Written { len: 3 })
            .expect("stale completion is harmless");
        assert!(stale.is_empty());
        assert_eq!(
            driver
                .incoming_snapshot
                .as_ref()
                .expect("new staging")
                .transfer_id,
            8
        );
    }

    #[test]
    fn trap_snapshot_staging_is_bounded() {
        let mut driver = driver();
        let total = driver.limits.max_snapshot_bytes.saturating_add(1);
        let effects = driver
            .stage_snapshot_chunk(test_snapshot_chunk(7, 0, b"x", total, 9, false))
            .expect("bounded refusal");
        assert_eq!(
            snapshot_ack_fields(&effects),
            (false, Some(SnapshotRejectReason::TooLarge), 0)
        );
        assert!(driver.incoming_snapshot.is_none());
    }

    #[test]
    fn trap_snapshot_buffer_cannot_exceed_limit() {
        let mut driver = driver();
        driver
            .stage_snapshot_chunk(test_snapshot_chunk(7, 0, b"bounded", 20, 9, false))
            .expect("bounded chunk");
        let footprint = driver.footprint();
        assert!(footprint.snapshot_staging.current <= footprint.snapshot_staging.limit);
        assert!(
            driver
                .incoming_snapshot
                .as_ref()
                .expect("staging")
                .next_offset
                <= driver.limits.max_snapshot_staging_bytes
        );
    }

    #[test]
    fn trap_snapshot_restart_from_zero_after_lost_ack_is_safe() {
        let mut restarted = driver();
        let retry = restarted
            .stage_snapshot_chunk(test_snapshot_chunk(9, 3, b"later", 10, 9, false))
            .expect("restart response");
        assert_eq!(
            snapshot_ack_fields(&retry),
            (false, Some(SnapshotRejectReason::RestartFromZero), 0)
        );
        assert!(restarted.incoming_snapshot.is_none());
    }

    #[test]
    fn trap_snapshot_reads_never_observe_staging_state() {
        let mut driver = driver();
        driver
            .node_mut()
            .kv
            .store
            .put(b"visible", b"old")
            .expect("old state");
        driver
            .stage_snapshot_chunk(test_snapshot_chunk(7, 0, b"partial", 100, 9, false))
            .expect("partial staging");
        assert_eq!(
            driver.node().kv.store.get(b"visible", None),
            Some(b"old".to_vec())
        );
        assert_eq!(driver.node().raft.applied_index, cc_core::LogIndex::new(0));
    }

    #[test]
    fn trap_snapshot_crc_failure_does_not_advance_apply() {
        let voters = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect();
        let mut source_config = config();
        source_config.id = NodeId::new(2);
        let mut source = Node::new(source_config, voters).expect("source");
        source.kv.applied_index = cc_core::LogIndex::new(4);
        source.kv.applied_term = cc_core::Term::new(1);
        source.raft.applied_index = cc_core::LogIndex::new(4);
        let bytes = source.encode_ccsn_snapshot().expect("CCSN");
        let message = test_snapshot_chunk(
            7,
            0,
            &bytes,
            bytes.len() as u64,
            cc_cluster::ccsn_file_crc(&bytes).expect("CRC") ^ 1,
            true,
        );
        let mut driver = driver();
        let before = driver.node().raft.applied_index;
        let write = driver.stage_snapshot_chunk(message).expect("stage");
        let [Effect::DiskWrite { id, bytes, .. }] = write.as_slice() else {
            panic!("write");
        };
        let staged = bytes.clone();
        let (_, fsync) = driver
            .complete_io(
                *id,
                IoResult::Written {
                    len: bytes.len() as u32,
                },
            )
            .expect("write completion");
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("fsync");
        };
        let (_, read) = driver.complete_io(*id, IoResult::Fsynced).expect("fsync");
        let [Effect::DiskRead { id, .. }] = read.as_slice() else {
            panic!("read");
        };
        let (_, ack) = driver
            .complete_io(*id, IoResult::Read(staged))
            .expect("validation");
        assert_eq!(
            snapshot_ack_fields(&ack),
            (false, Some(SnapshotRejectReason::Corrupt), 0)
        );
        assert_eq!(driver.node().raft.applied_index, before);
    }

    #[test]
    fn trap_follower_checkpoint_reclaims_its_own_log_prefix() {
        let mut driver = driver();
        driver.node_mut().kv.mark_applied(
            cc_core::LogIndex::new(2),
            cc_core::Term::new(1),
            Time::from_nanos(1),
        );
        driver.node_mut().raft.applied_index = cc_core::LogIndex::new(2);
        driver.node_mut().raft.log = vec![
            cc_raft::Entry {
                term: cc_core::Term::new(1),
                index: cc_core::LogIndex::new(1),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
            cc_raft::Entry {
                term: cc_core::Term::new(1),
                index: cc_core::LogIndex::new(2),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
        ];
        driver.limits.max_log_bytes_before_snapshot = 1;
        driver.next_wal_offset = 1;
        let effects = driver.translate(Vec::new()).expect("local checkpoint");
        drive_local_checkpoint(&mut driver, effects);
        assert_eq!(
            driver.node().raft.snapshot_base(),
            (cc_core::LogIndex::new(2), cc_core::Term::new(1))
        );
        assert!(driver.node().raft.log.is_empty());
        assert_eq!(driver.node().role(), cc_cluster::Role::Follower);
    }

    #[test]
    fn trap_log_prefix_discard_follows_snapshot_mark_and_dir_fsync() {
        let mut driver = driver();
        driver.node_mut().kv.mark_applied(
            cc_core::LogIndex::new(2),
            cc_core::Term::new(1),
            Time::from_nanos(1),
        );
        driver.node_mut().raft.applied_index = cc_core::LogIndex::new(2);
        driver.node_mut().raft.log = vec![
            cc_raft::Entry {
                term: cc_core::Term::new(1),
                index: cc_core::LogIndex::new(1),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
            cc_raft::Entry {
                term: cc_core::Term::new(1),
                index: cc_core::LogIndex::new(2),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
        ];
        driver.limits.max_log_bytes_before_snapshot = 1;
        driver.next_wal_offset = 1;
        let mut effects = driver.translate(Vec::new()).expect("local checkpoint");
        assert!(
            !driver.node().raft.log.is_empty(),
            "the prefix was released before any byte was durable"
        );

        // Reclaiming the log prefix is only safe once the snapshot file, its
        // mark record, and the directory entry that names them are all
        // durable. Until then the entries are the only recovery authority.
        let mut files = BTreeMap::<FileId, Vec<u8>>::new();
        let mut fsyncs = 0_u32;
        let mut directory_synced = false;
        while let Some(effect) = effects.pop() {
            let (id, result) = match effect {
                Effect::DiskWrite {
                    file,
                    at,
                    bytes,
                    id,
                } => {
                    let at = usize::try_from(at).expect("offset");
                    let image = files.entry(file).or_default();
                    if image.len() < at + bytes.len() {
                        image.resize(at + bytes.len(), 0);
                    }
                    image[at..at + bytes.len()].copy_from_slice(&bytes);
                    assert!(
                        !driver.node().raft.log.is_empty(),
                        "a write alone released the prefix"
                    );
                    (
                        id,
                        IoResult::Written {
                            len: u32::try_from(bytes.len()).expect("write length"),
                        },
                    )
                }
                Effect::DiskFsync { id, .. } => {
                    fsyncs += 1;
                    (id, IoResult::Fsynced)
                }
                Effect::DiskSyncDir { id } => {
                    directory_synced = true;
                    (id, IoResult::Fsynced)
                }
                Effect::DiskRename { from, to, id } => {
                    let image = files.remove(&from).expect("rename source");
                    files.insert(to, image);
                    (id, IoResult::Fsynced)
                }
                other => panic!("unexpected local checkpoint effect {other:?}"),
            };
            if driver.node().raft.log.is_empty() {
                assert!(
                    fsyncs > 0 && directory_synced,
                    "the prefix was discarded before the mark and directory were durable"
                );
            }
            let (_, next) = driver.complete_io(id, result).expect("checkpoint I/O");
            effects.extend(next.into_iter().rev());
        }
        assert!(fsyncs > 0 && directory_synced);
        assert!(driver.node().raft.log.is_empty());
        assert_eq!(
            driver.node().raft.snapshot_base(),
            (cc_core::LogIndex::new(2), cc_core::Term::new(1))
        );
    }

    #[test]
    fn trap_leader_sends_snapshot_when_log_prefix_is_gone() {
        let mut driver = driver();
        driver.node_mut().raft.role = cc_cluster::Role::Leader;
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(2);
        driver.node_mut().raft.leader_id = Some(NodeId::new(1));
        driver.node_mut().kv.mark_applied(
            cc_core::LogIndex::new(4),
            cc_core::Term::new(2),
            Time::from_nanos(1),
        );
        driver.node_mut().raft.applied_index = cc_core::LogIndex::new(4);
        driver
            .node_mut()
            .raft
            .install_snapshot_state(cc_core::LogIndex::new(4), cc_core::Term::new(2));
        driver
            .node_mut()
            .raft
            .next_index
            .insert(NodeId::new(2), cc_core::LogIndex::new(1));
        driver
            .register_published_snapshot(
                FileId::Snapshot { generation: 4 },
                cc_core::LogIndex::new(4),
                cc_core::Term::new(2),
                driver.node().kv.store.last_sequence(),
                99,
                7,
            )
            .expect("published snapshot");
        let effects = driver.translate(Vec::new()).expect("snapshot send");
        assert!(matches!(
            effects.as_slice(),
            [Effect::DiskRead {
                file: FileId::Snapshot { generation: 4 },
                at: 0,
                ..
            }]
        ));
    }

    #[test]
    fn trap_snapshot_install_serializes_with_apply_and_local_checkpoint() {
        let mut driver = driver();
        driver.node_mut().raft.role = cc_cluster::Role::Leader;
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(1);
        let mut blocks = MemoryBlockSource::default();
        let (_, persistence) = driver
            .deliver(
                Time::from_nanos(1),
                Input::ClientRequest {
                    client: ClientId::new(9),
                    req: RequestSeq::new(1),
                    session: Some((ClientId::new(9), RequestSeq::new(1))),
                    command: cc_kv::encode_command(&cc_kv::KvCommand::Set {
                        key: b"apply".to_vec(),
                        value: b"pending".to_vec(),
                        ttl: None,
                    }),
                },
                &mut blocks,
            )
            .expect("proposal");
        assert!(matches!(persistence.as_slice(), [Effect::DiskWrite { .. }]));
        let snapshot = test_snapshot_chunk(7, 0, b"not-installed", 20, 9, false);
        let staged = driver.deliver(
            Time::from_nanos(2),
            Input::Recv {
                from: NodeId::new(2),
                msg: encode_peer_effect(&snapshot).expect("snapshot CCRP"),
            },
            &mut blocks,
        );
        assert_eq!(
            staged,
            Err(HostError::Node(NodeError::PersistencePending)),
            "snapshot admission must wait for the apply durability chain"
        );
        assert_eq!(driver.node().raft.applied_index, cc_core::LogIndex::new(0));
        assert_eq!(driver.pending_io.len(), 1);
        assert!(
            driver
                .translate(Vec::new())
                .expect("checkpoint probe")
                .is_empty(),
            "local checkpoint must coalesce behind the same barrier"
        );
    }

    #[test]
    fn trap_streamed_snapshot_lifecycle_is_bounded() {
        let mut driver = driver();
        driver.node_mut().raft.role = cc_cluster::Role::Leader;
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(1);
        driver.node_mut().raft.leader_id = Some(NodeId::new(1));
        for number in 0..70_u16 {
            let key = format!("snapshot-{number:03}").into_bytes();
            driver
                .node_mut()
                .kv
                .store
                .put(&key, &vec![u8::try_from(number).expect("byte"); 4 * 1024])
                .expect("source value");
        }
        driver.node_mut().kv.applied_index = cc_core::LogIndex::new(4);
        driver.node_mut().kv.applied_term = cc_core::Term::new(1);
        driver.node_mut().raft.applied_index = cc_core::LogIndex::new(4);

        let mut next = driver
            .begin_snapshot_transfer(NodeId::new(2))
            .expect("begin transfer");
        let mut checkpoint = Vec::new();
        let fsync = loop {
            let [Effect::DiskWrite { id, bytes, .. }] = next.as_slice() else {
                break next;
            };
            checkpoint.extend_from_slice(bytes);
            let (_, after_write) = driver
                .complete_io(
                    *id,
                    IoResult::Written {
                        len: u32::try_from(bytes.len()).expect("checkpoint chunk length"),
                    },
                )
                .expect("checkpoint chunk write");
            next = after_write;
        };
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("source checkpoint must fsync");
        };
        let (_, rename) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("checkpoint fsync");
        let [Effect::DiskRename { id, .. }] = rename.as_slice() else {
            panic!("source checkpoint must publish by rename");
        };
        let (_, sync_dir) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("checkpoint rename");
        let [Effect::DiskSyncDir { id }] = sync_dir.as_slice() else {
            panic!("source checkpoint directory must sync");
        };
        let (_, manifest_write) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("checkpoint directory sync");
        let mark_write = complete_checkpoint_manifest(&mut driver, manifest_write);
        let [Effect::DiskWrite { id, bytes, .. }] = mark_write.as_slice() else {
            panic!("source checkpoint must persist its WAL mark before transfer");
        };
        let (_, mark_fsync) = driver
            .complete_io(
                *id,
                IoResult::Written {
                    len: u32::try_from(bytes.len()).expect("mark length"),
                },
            )
            .expect("checkpoint mark write");
        let [Effect::DiskFsync { id, .. }] = mark_fsync.as_slice() else {
            panic!("source checkpoint mark must fsync before transfer");
        };
        let (_, first_read) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("checkpoint mark fsync");
        let [Effect::DiskRead { id, len, .. }] = first_read.as_slice() else {
            panic!("source chunk must be read from the published checkpoint");
        };
        let first_len = usize::try_from(*len).expect("chunk length");
        let (_, first_send) = driver
            .complete_io(*id, IoResult::Read(checkpoint[..first_len].to_vec()))
            .expect("first source chunk read");
        assert!(
            matches!(first_send.as_slice(), [Effect::Send { to, .. }] if *to == NodeId::new(2))
        );
        let sender = driver
            .outgoing_snapshots
            .get(&NodeId::new(2))
            .expect("one outgoing transfer");
        assert!(sender.total_len as usize > cc_cluster::SNAPSHOT_CHUNK_BYTES);
        assert_eq!(sender.next_offset, 0);
        assert_eq!(
            sender.inflight_end,
            u64::try_from(cc_cluster::SNAPSHOT_CHUNK_BYTES).expect("chunk size")
        );
        let transfer_id = sender.transfer_id;
        let total = sender.total_len;

        let heartbeat_retry = driver
            .translate(vec![NodeEffect::ArmTimer {
                id: TimerId::new(3),
                at: Time::from_nanos(50),
                kind: TimerKind::Heartbeat,
            }])
            .expect("heartbeat retries the outstanding chunk");
        let [Effect::SetTimer { .. }, Effect::DiskRead { id, .. }] = heartbeat_retry.as_slice()
        else {
            panic!("heartbeat must schedule the timer and re-read the outstanding chunk");
        };
        let (_, heartbeat_send) = driver
            .complete_io(*id, IoResult::Read(checkpoint[..first_len].to_vec()))
            .expect("heartbeat retry read");
        assert!(matches!(heartbeat_send.as_slice(), [Effect::Send { .. }]));

        let retry_read = driver
            .retry_snapshot_transfer(NodeId::new(2))
            .expect("retry keeps exact outstanding chunk");
        let [Effect::DiskRead { id, .. }] = retry_read.as_slice() else {
            panic!("retry must re-read the exact outstanding durable chunk");
        };
        let (_, retry) = driver
            .complete_io(*id, IoResult::Read(checkpoint[..first_len].to_vec()))
            .expect("retry read");
        assert!(matches!(retry.as_slice(), [Effect::Send { .. }]));
        assert_eq!(
            driver
                .outgoing_snapshots
                .get(&NodeId::new(2))
                .expect("sender")
                .next_offset,
            0,
            "a retry does not advance before an acknowledgement"
        );

        let second = driver
            .translate(vec![NodeEffect::ReceiveSnapshotAck(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(2),
                to: NodeId::new(1),
                term: cc_core::Term::new(1),
                kind: MessageKind::SnapshotAck {
                    transfer_id,
                    next_offset: u64::try_from(cc_cluster::SNAPSHOT_CHUNK_BYTES)
                        .expect("chunk size"),
                    accepted: true,
                    reason: None,
                },
            })])
            .expect("first acknowledgement");
        let [Effect::DiskRead { id, len, .. }] = second.as_slice() else {
            panic!("next chunk must be read from the durable checkpoint");
        };
        let second_len = usize::try_from(*len).expect("second length");
        let (_, second_send) = driver
            .complete_io(
                *id,
                IoResult::Read(checkpoint[first_len..first_len + second_len].to_vec()),
            )
            .expect("second source chunk read");
        assert!(matches!(second_send.as_slice(), [Effect::Send { .. }]));
        let sender = driver
            .outgoing_snapshots
            .get(&NodeId::new(2))
            .expect("second chunk sender");
        assert_eq!(
            sender.next_offset,
            u64::try_from(cc_cluster::SNAPSHOT_CHUNK_BYTES).expect("chunk size")
        );
        assert_eq!(sender.inflight_end, total);

        let complete = driver
            .translate(vec![NodeEffect::ReceiveSnapshotAck(Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(2),
                to: NodeId::new(1),
                term: cc_core::Term::new(1),
                kind: MessageKind::SnapshotAck {
                    transfer_id,
                    next_offset: total,
                    accepted: true,
                    reason: None,
                },
            })])
            .expect("final acknowledgement");
        assert!(matches!(complete.as_slice(), [Effect::Send { to, .. }] if *to == NodeId::new(2)));
        assert!(driver.outgoing_snapshots.is_empty());
    }

    #[test]
    fn trap_snapshot_sender_is_discarded_after_leader_term_ends() {
        let mut driver = driver();
        driver.outgoing_snapshots.insert(
            NodeId::new(2),
            OutgoingSnapshot {
                peer: NodeId::new(2),
                leader_term: cc_core::Term::new(1),
                transfer_id: 7,
                index: LogIndex::new(4),
                snapshot_term: cc_core::Term::new(1),
                file: FileId::Snapshot { generation: 4 },
                total_len: 10,
                crc32c: 9,
                next_offset: 0,
                inflight_end: 3,
            },
        );
        driver.pending_snapshot_peers.insert(NodeId::new(3));

        assert!(driver.translate(Vec::new()).expect("prune").is_empty());
        assert!(driver.outgoing_snapshots.is_empty());
        assert!(driver.pending_snapshot_peers.is_empty());
    }

    #[test]
    fn trap_durable_log_trigger_starts_one_coalesced_local_checkpoint() {
        let mut driver = driver();
        driver.node_mut().kv.applied_index = cc_core::LogIndex::new(1);
        driver.node_mut().kv.applied_term = cc_core::Term::new(1);
        driver.node_mut().raft.applied_index = cc_core::LogIndex::new(1);
        driver.limits.max_log_bytes_before_snapshot = 8 * 1024 * 1024;
        driver.next_wal_offset = 8 * 1024 * 1024;
        let first = driver.translate(Vec::new()).expect("snapshot trigger");
        assert!(matches!(
            first.as_slice(),
            [Effect::DiskWrite {
                file: FileId::Temp { .. },
                ..
            }]
        ));
        let duplicate = driver.translate(Vec::new()).expect("coalesced trigger");
        assert!(
            duplicate.is_empty(),
            "an active checkpoint absorbs a new trigger"
        );
    }

    #[test]
    fn trap_store_wal_cursor_switches_before_compaction_rename_is_exposed() {
        let mut driver = driver();
        driver.next_store_wal_offset = 230_958;
        let write = driver
            .begin_store_wal_compaction(Vec::new())
            .expect("begin store WAL compaction");
        let [Effect::DiskWrite { id, .. }] = write.as_slice() else {
            panic!("compaction must create an empty replacement");
        };
        let (_, fsync) = driver
            .complete_io(*id, IoResult::Written { len: 0 })
            .expect("replacement write");
        let [Effect::DiskFsync { id, .. }] = fsync.as_slice() else {
            panic!("replacement must fsync");
        };
        let (_, rename) = driver
            .complete_io(*id, IoResult::Fsynced)
            .expect("replacement fsync");
        assert!(matches!(
            rename.as_slice(),
            [Effect::DiskRename {
                to: FileId::StoreWal { segment: 0 },
                ..
            }]
        ));
        assert_eq!(
            driver.next_store_wal_offset, 0,
            "no append may retain the superseded image's cursor once rename is executable"
        );
    }

    #[test]
    fn trap_maintenance_reserve_serializes_space_amplifying_jobs() {
        let mut driver = driver();
        driver.node_mut().kv.applied_index = cc_core::LogIndex::new(1);
        driver.node_mut().kv.applied_term = cc_core::Term::new(1);
        driver.node_mut().raft.applied_index = cc_core::LogIndex::new(1);
        driver.limits.max_log_bytes_before_snapshot = 1;
        driver.next_wal_offset = 1;
        let first = driver.translate(Vec::new()).expect("checkpoint publisher");
        assert!(matches!(first.as_slice(), [Effect::DiskWrite { .. }]));
        assert!(
            driver
                .translate(Vec::new())
                .expect("coalesced publisher")
                .is_empty(),
            "one pending publisher owns the maintenance reserve"
        );
        assert_eq!(
            driver.begin_snapshot_transfer(NodeId::new(2)),
            Err(HostError::Node(NodeError::Environment(
                "snapshot sender unavailable"
            ))),
            "a follower cannot start a competing publisher"
        );
    }

    #[test]
    fn trap_recovered_driver_appends_after_verified_wal_prefix() {
        let bootstrap = cc_core::MembershipState::new(
            [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        )
        .expect("membership");
        let mut driver = Driver::boot_with_wal_offset(config(), BootState::Fresh { bootstrap }, 73)
            .expect("driver");
        let mut blocks = MemoryBlockSource::default();
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(1);
        let request = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let (_, effects) = driver
            .deliver(
                Time::from_nanos(1),
                Input::Recv {
                    from: NodeId::new(2),
                    msg: encode_peer_effect(&request).expect("CCRP"),
                },
                &mut blocks,
            )
            .expect("vote request");
        assert!(matches!(
            effects.as_slice(),
            [Effect::DiskWrite { at: 73, .. }]
        ));
    }

    #[test]
    fn trap_driver_rejects_unknown_or_failed_io_completion() {
        let mut driver = driver();
        let mut blocks = MemoryBlockSource::default();
        assert_eq!(
            driver.deliver(
                Time::from_nanos(0),
                Input::IoDone {
                    id: IoId::new(9),
                    result: IoResult::Failed(IoError::Eio),
                },
                &mut blocks,
            ),
            Err(HostError::UnknownIo(IoId::new(9)))
        );
    }

    #[test]
    fn trap_driver_failed_critical_write_is_a_durability_error() {
        let mut driver = driver();
        let mut blocks = MemoryBlockSource::default();
        driver.node_mut().raft.hard_state.term = cc_core::Term::new(1);
        let request = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let (_, effects) = driver
            .deliver(
                Time::from_nanos(1),
                Input::Recv {
                    from: NodeId::new(2),
                    msg: encode_peer_effect(&request).expect("CCRP"),
                },
                &mut blocks,
            )
            .expect("vote request");
        let [Effect::DiskWrite { id, file, .. }] = effects.as_slice() else {
            panic!("vote must issue one write");
        };
        assert_eq!(
            driver.deliver(
                Time::from_nanos(1),
                Input::IoDone {
                    id: *id,
                    result: IoResult::Failed(IoError::Eio),
                },
                &mut blocks,
            ),
            Err(HostError::Durability {
                id: *id,
                file: *file,
            })
        );
    }

    #[test]
    fn trap_raft_and_store_wal_hard_caps_fail_closed() {
        let mut driver = driver();
        driver.limits.max_raft_log_bytes = 3;
        driver.limits.max_store_wal_bytes = 4;
        assert_eq!(
            driver.issue_raw_wal_write(vec![0; 4]),
            Err(HostError::ResourceLimit("max_raft_log_bytes"))
        );
        assert_eq!(driver.next_wal_offset, 0);
        assert_eq!(
            driver.issue_store_wal_write(vec![0; 5]),
            Err(HostError::ResourceLimit("max_store_wal_bytes"))
        );
        assert_eq!(driver.next_store_wal_offset, 0);
        assert!(driver.pending_io.is_empty());
    }

    #[test]
    fn trap_driver_ignores_stale_timer_generation() {
        let mut driver = driver();
        let mut blocks = MemoryBlockSource::default();
        let id = TimerId::new(7);
        driver.timers.insert(
            id,
            TimerState {
                at: Time::from_nanos(5),
                generation: 2,
                kind: TimerKind::Election,
            },
        );
        let (_, effects) = driver
            .deliver(
                Time::from_nanos(5),
                Input::TimerFired { id, generation: 1 },
                &mut blocks,
            )
            .expect("stale timer");
        assert!(effects.is_empty());
        assert_eq!(
            driver.armed_timers().collect::<Vec<_>>(),
            vec![(id, Time::from_nanos(5), 2)]
        );
    }

    #[test]
    fn trap_driver_release_is_exactly_once() {
        let mut driver = driver();
        let timer = TimerId::new(11);
        let (poll, effects) = driver
            .defer_or_translate(
                Time::from_nanos(10),
                Duration::from_nanos(7),
                Ok(vec![NodeEffect::ArmTimer {
                    id: timer,
                    at: Time::from_nanos(20),
                    kind: TimerKind::Election,
                }]),
            )
            .expect("defer");
        assert_eq!(poll, DriverPoll::BlockedUntil(Time::from_nanos(17)));
        assert!(effects.is_empty());
        assert_eq!(
            driver
                .release_ready(Time::from_nanos(16))
                .expect("not ready"),
            (DriverPoll::BlockedUntil(Time::from_nanos(17)), Vec::new())
        );
        let (_, released) = driver.release_ready(Time::from_nanos(17)).expect("ready");
        assert!(matches!(released.as_slice(), [Effect::SetTimer { id, .. }] if *id == timer));
        assert_eq!(
            driver
                .release_ready(Time::from_nanos(17))
                .expect("released"),
            (DriverPoll::Ready, Vec::new())
        );
    }

    #[test]
    fn trap_driver_retains_effects_until_sync_service_elapses() {
        let mut driver = driver();
        let reply = NodeEffect::ReadReply {
            client: cc_core::ClientId::new(4),
            sequence: 1,
            reply: cc_kv::KvReply::Value(Some(b"served-from-a-block".to_vec())),
        };
        let (poll, effects) = driver
            .defer_or_translate(
                Time::from_nanos(100),
                Duration::from_nanos(25),
                Ok(vec![reply]),
            )
            .expect("defer a synchronous read");
        // The reply belongs to the read that paid for it, so nothing escapes
        // before the modelled service duration has elapsed.
        assert_eq!(poll, DriverPoll::BlockedUntil(Time::from_nanos(125)));
        assert!(effects.is_empty(), "a zero-latency reply escaped");
        for early in [100, 101, 124] {
            assert_eq!(
                driver
                    .release_ready(Time::from_nanos(early))
                    .expect("still blocked"),
                (DriverPoll::BlockedUntil(Time::from_nanos(125)), Vec::new()),
                "the deadline moved at {early}"
            );
        }
        let (poll, released) = driver
            .release_ready(Time::from_nanos(125))
            .expect("service elapsed");
        assert_eq!(poll, DriverPoll::Ready);
        assert!(matches!(
            released.as_slice(),
            [Effect::ClientReply { client, .. }] if client.get() == 4
        ));
    }

    #[test]
    fn trap_failed_block_read_delays_its_own_error() {
        let mut driver = driver();
        let (poll, effects) = driver
            .defer_or_translate(
                Time::from_nanos(40),
                Duration::from_nanos(9),
                Err(NodeError::Kv(cc_kv::KvError::Busy)),
            )
            .expect("defer a failed read");
        // A failure pays the same service time as a success; releasing it
        // early would report an error the storage stack has not yet produced.
        assert_eq!(poll, DriverPoll::BlockedUntil(Time::from_nanos(49)));
        assert!(effects.is_empty());
        assert_eq!(
            driver
                .release_ready(Time::from_nanos(48))
                .expect("still blocked"),
            (DriverPoll::BlockedUntil(Time::from_nanos(49)), Vec::new())
        );
        assert_eq!(
            driver.release_ready(Time::from_nanos(49)),
            Err(HostError::Node(NodeError::Kv(cc_kv::KvError::Busy))),
            "the stored error surfaces at its own deadline"
        );
        // Exactly once: the error is not replayed on the next release.
        assert_eq!(
            driver
                .release_ready(Time::from_nanos(50))
                .expect("error was consumed"),
            (DriverPoll::Ready, Vec::new())
        );
    }

    #[test]
    fn trap_sync_read_blocks_same_node_inputs() {
        let mut driver = driver();
        let mut blocks = MemoryBlockSource::default();
        let (_poll, effects) = driver
            .defer_or_translate(
                Time::from_nanos(10),
                Duration::from_nanos(7),
                Ok(vec![NodeEffect::ReadReply {
                    client: cc_core::ClientId::new(9),
                    sequence: 1,
                    reply: cc_kv::KvReply::Ok,
                }]),
            )
            .expect("start synchronous read");
        assert!(effects.is_empty());
        let (poll, effects) = driver
            .deliver(Time::from_nanos(11), Input::Tick, &mut blocks)
            .expect("blocked input");
        assert_eq!(poll, DriverPoll::BlockedUntil(Time::from_nanos(17)));
        assert!(effects.is_empty());
        driver.enqueue(Input::Tick).expect("bounded admission");
        assert_eq!(driver.footprint().pending_timer_inputs, 1);
        let (_, released) = driver.release_ready(Time::from_nanos(17)).expect("release");
        assert_eq!(released.len(), 1);
    }

    #[test]
    fn trap_every_host_queue_has_a_limit() {
        let mut config = config();
        config.host_limits.max_pending_client = 1;
        config.host_limits.max_pending_io = 1;
        let mut driver = Driver::boot(
            config,
            BootState::Fresh {
                bootstrap: cc_core::MembershipState::new(
                    [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                )
                .expect("membership"),
            },
        )
        .expect("driver");
        driver
            .enqueue(Input::ClientRequest {
                client: cc_core::ClientId::new(1),
                req: RequestSeq::new(1),
                session: None,
                command: vec![1],
            })
            .expect("first client request");
        assert_eq!(
            driver.enqueue(Input::ClientRequest {
                client: cc_core::ClientId::new(2),
                req: RequestSeq::new(1),
                session: None,
                command: vec![2],
            }),
            Err(HostError::QueueFull(InputClass::Client))
        );
        driver
            .enqueue(Input::IoDone {
                id: IoId::new(999),
                result: IoResult::Failed(IoError::Eio),
            })
            .expect("I/O completion has a separate queue");
        let mut blocks = MemoryBlockSource::default();
        assert_eq!(
            driver.deliver_next(Time::from_nanos(1), &mut blocks),
            Err(HostError::UnknownIo(IoId::new(999))),
            "I/O is delivered before the queued client request"
        );
        assert_eq!(driver.footprint().pending_client_inputs, 1);
    }

    #[test]
    fn trap_bounded_peer_queue_drops_instead_of_blocking_consensus() {
        let mut config = config();
        config.host_limits.max_pending_peer = 1;
        let mut driver = Driver::boot(
            config,
            BootState::Fresh {
                bootstrap: cc_core::MembershipState::new(
                    [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                )
                .expect("membership"),
            },
        )
        .expect("driver");
        let peer_input = |from: u64| Input::Recv {
            from: NodeId::new(from),
            msg: cc_env::WireMsg::new(cc_raft::PROTOCOL_VERSION, vec![0; 8]),
        };
        driver.enqueue(peer_input(2)).expect("first peer datagram");
        // The second datagram is refused rather than queued without bound.
        // Raft retries its own appends, so dropping here costs a round trip
        // while blocking the Driver would stall every other node's consensus.
        assert_eq!(
            driver.enqueue(peer_input(3)),
            Err(HostError::QueueFull(InputClass::Peer))
        );
        assert_eq!(driver.footprint().pending_peer_inputs, 1);

        // A full peer queue never closes admission for other classes: client
        // work and I/O completions still make progress.
        driver
            .enqueue(Input::ClientRequest {
                client: cc_core::ClientId::new(1),
                req: RequestSeq::new(1),
                session: None,
                command: vec![1],
            })
            .expect("client admission survives a full peer queue");
        assert_eq!(driver.footprint().pending_client_inputs, 1);

        // Draining the peer queue restores peer admission.
        let mut blocks = MemoryBlockSource::default();
        let _ = driver.deliver_next(Time::from_nanos(1), &mut blocks);
        driver
            .enqueue(peer_input(3))
            .expect("peer admission resumes after the queue drains");
    }

    #[test]
    fn trap_driver_queue_byte_limit_is_charged_before_admission() {
        let mut config = config();
        config.host_limits.max_driver_pending_inputs = 8;
        config.host_limits.max_driver_pending_input_bytes = 35;
        let mut driver = Driver::boot(
            config,
            BootState::Fresh {
                bootstrap: cc_core::MembershipState::new(
                    [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                )
                .expect("membership"),
            },
        )
        .expect("driver");
        assert_eq!(
            driver.enqueue(Input::ClientRequest {
                client: cc_core::ClientId::new(1),
                req: RequestSeq::new(1),
                session: None,
                command: vec![7; 4],
            }),
            Err(HostError::QueueFull(InputClass::Client)),
            "the 32-byte route envelope plus command must be reserved first"
        );
        assert_eq!(driver.footprint().pending_input_bytes, 0);
    }

    #[test]
    fn trap_footprint_counts_encoded_queue_bytes() {
        let mut config = config();
        config.host_limits.max_driver_pending_inputs = 8;
        config.host_limits.max_driver_pending_input_bytes = 64;
        let mut driver = Driver::boot(
            config,
            BootState::Fresh {
                bootstrap: cc_core::MembershipState::new(
                    [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                )
                .expect("membership"),
            },
        )
        .expect("driver");
        driver
            .enqueue(Input::ClientRequest {
                client: cc_core::ClientId::new(1),
                req: RequestSeq::new(1),
                session: None,
                command: vec![7; 4],
            })
            .expect("admission");
        assert_eq!(
            driver.footprint().driver_inputs,
            Usage {
                current: 36,
                peak: 36,
                limit: 64,
            }
        );

        let mut blocks = MemoryBlockSource::default();
        let (_, effects) = driver
            .deliver_next(Time::from_nanos(1), &mut blocks)
            .expect("queued follower request resolves without poisoning its drainer");
        assert!(matches!(
            effects.as_slice(),
            [Effect::ClientReply { client, req, .. }]
                if *client == ClientId::new(1) && *req == RequestSeq::new(1)
        ));
        assert_eq!(
            driver.footprint().driver_inputs,
            Usage {
                current: 0,
                peak: 36,
                limit: 64,
            },
            "the current charge is released but the observed peak remains useful"
        );
    }

    #[test]
    fn trap_failed_sync_read_charges_service_before_error() {
        let root = std::env::temp_dir().join(format!(
            "cc-host-block-source-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test") // cc-detlint: allow host-boundary
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).expect("temp block root");
        fs::write(root.join("sst-1"), b"four").expect("table bytes");
        let mut source = FileBlockSource::with_limit(&root, 1).expect("source");
        let read = source
            .read_block(FileId::Sst { file_no: 1 }, 0, 4)
            .expect("positioned read");
        assert_eq!(read.bytes, b"four");
        let failure = source
            .read_block(FileId::Sst { file_no: 1 }, 0, 5)
            .expect_err("short range must fail");
        assert!(matches!(
            failure.error,
            StoreError::InvalidInput("block range")
        ));
        assert_eq!(source.open_files, 0, "all scoped leases were released");
        fs::remove_dir_all(root).expect("clean block root");
    }

    #[test]
    fn trap_file_backed_scans_and_compaction_respect_open_file_cap() {
        let root = std::env::temp_dir().join(format!(
            "cc-host-open-file-cap-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test") // cc-detlint: allow host-boundary
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).expect("temp block root");
        fs::write(root.join("sst-1"), b"block").expect("table bytes");
        let mut source = FileBlockSource::with_limit(&root, 1).expect("source");

        // Model a scan/compaction lease that already owns the one descriptor.
        // A concurrent block consumer gets a typed retry result; after the
        // scoped owner releases it, the same read succeeds and the count
        // returns exactly to zero.
        source.open_files = 1;
        let busy = source
            .read_block(FileId::Sst { file_no: 1 }, 0, 5)
            .expect_err("file cap");
        assert!(matches!(busy.error, StoreError::Busy));
        source.open_files = 0;
        assert_eq!(
            source
                .read_block(FileId::Sst { file_no: 1 }, 0, 5)
                .expect("lease released")
                .bytes,
            b"block"
        );
        assert_eq!(source.open_files, 0);
        fs::remove_dir_all(root).expect("clean block root");
    }
}
