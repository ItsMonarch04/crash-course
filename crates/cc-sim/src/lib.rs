// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "A small deterministic discrete-event host used by tests and later cluster work."]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use cc_core::{
    DelayDist, Duration, EventKind, NodeId, P16, Seed, Time, TimerId, Trace, Xoshiro256pp, crc32c,
};
use cc_env::{FileId, IoError, IoResult};

pub const DEFAULT_MAX_EVENTS: u64 = 10_000_000;
pub const DEFAULT_MAX_EVENTS_PER_INSTANT: u64 = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimConfig {
    pub end_time: Time,
    pub max_events: u64,
    pub max_events_per_instant: u64,
    pub node_count: u64,
    pub disk_model: DiskModel,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            end_time: Time::from_nanos(60_000_000_000),
            max_events: DEFAULT_MAX_EVENTS,
            max_events_per_instant: DEFAULT_MAX_EVENTS_PER_INSTANT,
            node_count: 5,
            disk_model: DiskModel::universal(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Target {
    Node(NodeId),
    Actor(u64),
    Model,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SimEvent {
    Tick { target: Target },
    Timer { target: Target, id: TimerId },
    Message { target: Target, bytes: Vec<u8> },
}

#[derive(Clone, Debug)]
struct QueueItem<E> {
    at: Time,
    tie_seq: u64,
    event: E,
}

impl<E> PartialEq for QueueItem<E> {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.tie_seq == other.tie_seq
    }
}

impl<E> Eq for QueueItem<E> {}

impl<E> Ord for QueueItem<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.tie_seq.cmp(&self.tie_seq))
    }
}

impl<E> PartialOrd for QueueItem<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Shared deterministic scheduler. It orders events by virtual time and then
/// insertion sequence, so hosts can share exact same-instant FIFO behavior
/// without retaining private heap implementations.
#[derive(Clone)]
pub struct EventQueue<E> {
    queue: BinaryHeap<QueueItem<E>>,
    next_tie_seq: u64,
    peak_len: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledEvent<E> {
    pub at: Time,
    pub event: E,
}

impl<E> EventQueue<E> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
            next_tie_seq: 0,
            peak_len: 0,
        }
    }

    pub fn schedule(&mut self, at: Time, event: E) {
        let tie_seq = self.next_tie_seq;
        self.next_tie_seq = self
            .next_tie_seq
            .checked_add(1)
            .expect("invariant: scheduler tie sequence overflow");
        self.queue.push(QueueItem { at, tie_seq, event });
        self.peak_len = self.peak_len.max(self.queue.len());
    }

    #[must_use]
    pub fn peek_time(&self) -> Option<Time> {
        self.queue.peek().map(|item| item.at)
    }

    pub fn pop(&mut self) -> Option<ScheduledEvent<E>> {
        self.queue.pop().map(|item| ScheduledEvent {
            at: item.at,
            event: item.event,
        })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[must_use]
    pub const fn peak_len(&self) -> usize {
        self.peak_len
    }
}

impl<E> Default for EventQueue<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecorderLevel {
    Gate,
    Campaign,
    Theater,
}

#[derive(Clone)]
pub struct Recorder {
    level: RecorderLevel,
    trace: Trace,
}

impl Recorder {
    #[must_use]
    pub fn new(seed: Seed, level: RecorderLevel) -> Self {
        Self {
            level,
            trace: Trace::new(seed, 0),
        }
    }

    pub fn record(&mut self, time: Time, node: Option<NodeId>, kind: EventKind, payload: Vec<u8>) {
        let payload = match self.level {
            RecorderLevel::Gate | RecorderLevel::Theater => payload,
            RecorderLevel::Campaign => Vec::new(),
        };
        self.trace.push(time, node, kind, payload);
    }

    #[must_use]
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    #[must_use]
    pub fn finish(self) -> Trace {
        self.trace
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToyNode {
    pub id: NodeId,
    pub ticks: u64,
}

impl ToyNode {
    #[must_use]
    pub const fn new(id: NodeId) -> Self {
        Self { id, ticks: 0 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunError {
    EventLimit { limit: u64 },
    InstantLimit { at: Time, limit: u64 },
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EventLimit { limit } => write!(f, "runaway: event limit {limit} exceeded"),
            Self::InstantLimit { at, limit } => {
                write!(f, "runaway: {limit} events at {at}")
            }
        }
    }
}

impl std::error::Error for RunError {}

pub struct Sim {
    pub seed: Seed,
    pub now: Time,
    pub config: SimConfig,
    queue: EventQueue<SimEvent>,
    pub recorder: Recorder,
    pub nodes: Vec<ToyNode>,
    rng: Xoshiro256pp,
}

impl Sim {
    #[must_use]
    pub fn new(seed: Seed, config: SimConfig, level: RecorderLevel) -> Self {
        let nodes = (0..config.node_count)
            .map(|id| ToyNode::new(NodeId::new(id + 1)))
            .collect();
        Self {
            seed,
            now: Time::from_nanos(0),
            config,
            queue: EventQueue::new(),
            recorder: Recorder::new(seed, level),
            nodes,
            rng: Xoshiro256pp::stream(seed, "sim", 0),
        }
    }

    pub fn schedule(&mut self, at: Time, event: SimEvent) {
        self.queue.schedule(at, event);
    }

    pub fn seed_toy_ticks(&mut self) {
        for id in 0..self.nodes.len() {
            self.schedule(
                Time::from_nanos(0),
                SimEvent::Tick {
                    target: Target::Node(self.nodes[id].id),
                },
            );
        }
    }

    pub fn run_toy(&mut self) -> Result<Trace, RunError> {
        let mut processed = 0;
        let mut instant = Time::from_nanos(0);
        let mut at_instant = 0;
        while let Some(item) = self.queue.pop() {
            if item.at > self.config.end_time {
                break;
            }
            if item.at != instant {
                instant = item.at;
                at_instant = 0;
            }
            at_instant += 1;
            if at_instant > self.config.max_events_per_instant {
                return Err(RunError::InstantLimit {
                    at: instant,
                    limit: self.config.max_events_per_instant,
                });
            }
            processed += 1;
            if processed > self.config.max_events {
                return Err(RunError::EventLimit {
                    limit: self.config.max_events,
                });
            }
            self.now = item.at;
            if let SimEvent::Tick {
                target: Target::Node(node_id),
            } = item.event
            {
                let mut reschedule = None;
                if let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) {
                    node.ticks += 1;
                    let node_id = node.id;
                    self.recorder.record(
                        self.now,
                        Some(node_id),
                        EventKind::TimerFire,
                        node.ticks.to_le_bytes().to_vec(),
                    );
                    if node.ticks < 3 {
                        let delay = 1_000_000 + self.rng.range_u64(0, 1_000_001);
                        reschedule = Some((node_id, delay));
                    }
                }
                if let Some((node_id, delay)) = reschedule {
                    self.schedule(
                        self.now + cc_core::Duration::from_nanos(delay),
                        SimEvent::Tick {
                            target: Target::Node(node_id),
                        },
                    );
                }
            }
        }
        Ok(self.recorder.trace().clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelfcheckError {
    Run(RunError),
    Diverged { event: usize },
}

impl std::fmt::Display for SelfcheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Run(error) => error.fmt(f),
            Self::Diverged { event } => write!(f, "determinism divergence at event {event}"),
        }
    }
}

impl std::error::Error for SelfcheckError {}

#[must_use]
pub fn deterministic_trace(seed: Seed) -> Vec<u8> {
    let mut sim = Sim::new(seed, SimConfig::default(), RecorderLevel::Gate);
    sim.seed_toy_ticks();
    sim.run_toy()
        .expect("invariant: default toy simulation must finish")
        .encode()
}

pub fn selfcheck(seed: Seed) -> Result<(), SelfcheckError> {
    let first = deterministic_trace(seed);
    let second = deterministic_trace(seed);
    if first == second {
        Ok(())
    } else {
        let event = first
            .iter()
            .zip(second.iter())
            .position(|(left, right)| left != right)
            .unwrap_or(first.len().min(second.len()));
        Err(SelfcheckError::Diverged { event })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskFault {
    EioNextWrite,
    EioNextFsync,
    TornNextWrite { prefix_len: usize },
}

/// Persistent deterministic service time added to disk operations.  This is
/// deliberately separate from one-shot faults: a latency experiment must not
/// silently turn into an EIO injection after its first write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlowDisk {
    pub read_extra: Duration,
    pub write_extra: Duration,
    pub fsync_extra: Duration,
    pub rename_extra: Duration,
    pub dirsync_extra: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskOperation {
    Read,
    Write,
    Fsync,
    Rename,
    SyncDir,
}

/// Baseline service distributions for each modeled disk operation. Faults
/// such as [`SlowDisk`] add to these samples; they never replace the named
/// environment model or turn latency into an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiskModel {
    pub read: DelayDist,
    pub write: DelayDist,
    pub fsync: DelayDist,
    pub rename: DelayDist,
    pub dirsync: DelayDist,
}

impl DiskModel {
    /// Small deterministic defaults used by universal tests and examples.
    #[must_use]
    pub const fn universal() -> Self {
        Self {
            read: DelayDist::Fixed(Duration::from_nanos(0)),
            write: DelayDist::Fixed(Duration::from_nanos(0)),
            fsync: DelayDist::Fixed(Duration::from_nanos(0)),
            rename: DelayDist::Fixed(Duration::from_nanos(0)),
            dirsync: DelayDist::Fixed(Duration::from_nanos(0)),
        }
    }

    #[must_use]
    pub const fn distribution(self, operation: DiskOperation) -> DelayDist {
        match operation {
            DiskOperation::Read => self.read,
            DiskOperation::Write => self.write,
            DiskOperation::Fsync => self.fsync,
            DiskOperation::Rename => self.rename,
            DiskOperation::SyncDir => self.dirsync,
        }
    }
}

impl Default for DiskModel {
    fn default() -> Self {
        Self::universal()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SimFile {
    /// Page-cache namespace. A rename updates this immediately, just as it is
    /// visible to a live process before the containing directory is synced.
    id: FileId,
    /// Directory-durable namespace. Crash recovery exposes this name, not a
    /// rename that was never followed by `sync_dir`.
    durable_id: FileId,
    visible: Vec<u8>,
    durable: Vec<u8>,
    /// Checksum recorded by the last successful fsync.  Keeping this
    /// separately lets the simulator model an at-rest bit flip as corruption
    /// that a recovery/read boundary must notice, rather than silently
    /// handing the altered byte to the caller.
    durable_crc32c: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BitRot {
    file: FileId,
    offset: u64,
}

/// A deterministic page-cache disk. Writes are visible before fsync; crash
/// discards every visible byte not copied to the durable image.
#[derive(Clone, Debug)]
pub struct SimDisk {
    files: Vec<SimFile>,
    fault: Option<DiskFault>,
    slow: SlowDisk,
    enospc: bool,
    quota: Option<u64>,
    bitrot: Option<BitRot>,
    model: DiskModel,
    service_rng: Xoshiro256pp,
}

impl SimDisk {
    #[must_use]
    pub fn new() -> Self {
        Self {
            files: Vec::new(),
            fault: None,
            slow: SlowDisk {
                read_extra: Duration::from_nanos(0),
                write_extra: Duration::from_nanos(0),
                fsync_extra: Duration::from_nanos(0),
                rename_extra: Duration::from_nanos(0),
                dirsync_extra: Duration::from_nanos(0),
            },
            enospc: false,
            quota: None,
            bitrot: None,
            model: DiskModel::universal(),
            service_rng: Xoshiro256pp::stream(Seed::new(0), "sim-disk-service", 0),
        }
    }

    #[must_use]
    pub fn with_model(model: DiskModel, seed: Seed, node: NodeId) -> Self {
        Self {
            model,
            service_rng: Xoshiro256pp::stream(seed, "sim-disk-service", node.get()),
            ..Self::new()
        }
    }

    pub fn inject(&mut self, fault: DiskFault) {
        self.fault = Some(fault);
    }

    pub fn set_slow_disk(&mut self, slow: SlowDisk) {
        self.slow = slow;
    }

    pub fn set_enospc(&mut self, enabled: bool) {
        self.enospc = enabled;
    }

    pub fn set_quota(&mut self, quota: Option<u64>) {
        self.quota = quota;
    }

    pub fn inject_bitrot(&mut self, file: FileId, offset: u64) {
        self.bitrot = Some(BitRot { file, offset });
    }

    #[must_use]
    pub const fn slow_disk(&self) -> SlowDisk {
        self.slow
    }

    #[must_use]
    pub fn service_time(&mut self, operation: DiskOperation) -> Duration {
        let extra = match operation {
            DiskOperation::Read => self.slow.read_extra,
            DiskOperation::Write => self.slow.write_extra,
            DiskOperation::Fsync => self.slow.fsync_extra,
            DiskOperation::Rename => self.slow.rename_extra,
            DiskOperation::SyncDir => self.slow.dirsync_extra,
        };
        self.service_rng
            .sample_delay(self.model.distribution(operation))
            .checked_add(extra)
            .unwrap_or(Duration::from_nanos(u64::MAX))
    }

    pub fn write(&mut self, file: FileId, at: u64, bytes: &[u8]) -> Result<IoResult, IoError> {
        if self.enospc {
            return Err(IoError::Enospc);
        }
        if matches!(self.fault, Some(DiskFault::EioNextWrite)) {
            self.fault = None;
            return Err(IoError::Eio);
        }
        let mut bytes_to_write = bytes.to_vec();
        if let Some(DiskFault::TornNextWrite { prefix_len }) = self.fault.take() {
            bytes_to_write.truncate(prefix_len.min(bytes_to_write.len()));
        }
        let at = usize::try_from(at).map_err(|_| IoError::InvalidRange)?;
        let end = at
            .checked_add(bytes_to_write.len())
            .ok_or(IoError::InvalidRange)?;
        let current_len = self
            .files
            .iter()
            .find(|entry| entry.id == file)
            .map_or(0, |entry| entry.visible.len());
        let new_len = current_len.max(end);
        if let Some(quota) = self.quota {
            let allocated = self.visible_bytes();
            let projected = allocated
                .saturating_sub(u64::try_from(current_len).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(new_len).unwrap_or(u64::MAX));
            if projected > quota {
                return Err(IoError::Enospc);
            }
        }
        let file_state = self.file_mut(file);
        if file_state.visible.len() < end {
            file_state.visible.resize(end, 0);
        }
        file_state.visible[at..end].copy_from_slice(&bytes_to_write);
        Ok(IoResult::Written {
            len: u32::try_from(bytes_to_write.len()).map_err(|_| IoError::InvalidRange)?,
        })
    }

    pub fn read(&self, file: FileId, at: u64, len: u32) -> Result<IoResult, IoError> {
        let file_state = self
            .files
            .iter()
            .find(|entry| entry.id == file)
            .ok_or(IoError::NotFound)?;
        self.verify_file(file_state)?;
        let at = usize::try_from(at).map_err(|_| IoError::InvalidRange)?;
        let len = usize::try_from(len).map_err(|_| IoError::InvalidRange)?;
        let end = at.checked_add(len).ok_or(IoError::InvalidRange)?;
        if end > file_state.visible.len() {
            return Err(IoError::InvalidRange);
        }
        Ok(IoResult::Read(file_state.visible[at..end].to_vec()))
    }

    pub fn fsync(&mut self, file: FileId) -> Result<IoResult, IoError> {
        if matches!(self.fault, Some(DiskFault::EioNextFsync)) {
            self.fault = None;
            return Err(IoError::Eio);
        }
        let bitrot = if self.bitrot.is_some_and(|fault| fault.file == file) {
            self.bitrot.take()
        } else {
            None
        };
        if let Some(fault) = bitrot {
            let offset = usize::try_from(fault.offset).map_err(|_| IoError::InvalidRange)?;
            let visible_len = self
                .files
                .iter()
                .find(|entry| entry.id == file)
                .map_or(0, |entry| entry.visible.len());
            if offset >= visible_len {
                return Err(IoError::InvalidRange);
            }
            let file_state = self.file_mut(file);
            file_state.durable.clone_from(&file_state.visible);
            file_state.durable_crc32c = crc32c(&file_state.durable);
            file_state.durable[offset] ^= 1;
        } else {
            let file_state = self.file_mut(file);
            file_state.durable.clone_from(&file_state.visible);
            file_state.durable_crc32c = crc32c(&file_state.durable);
        }
        Ok(IoResult::Fsynced)
    }

    pub fn truncate(&mut self, file: FileId, len: u64) -> Result<IoResult, IoError> {
        let len = usize::try_from(len).map_err(|_| IoError::InvalidRange)?;
        let file_state = self.file_mut(file);
        file_state.visible.truncate(len);
        Ok(IoResult::Truncated { len: len as u64 })
    }

    /// Complete a host delete effect. Space is released at this transition,
    /// not when deletion is merely scheduled.
    pub fn delete(&mut self, file: FileId) -> Result<IoResult, IoError> {
        let index = self
            .files
            .iter()
            .position(|entry| entry.id == file)
            .ok_or(IoError::NotFound)?;
        self.files.remove(index);
        Ok(IoResult::Fsynced)
    }

    /// Rename one logical file without copying its page-cache or durable
    /// image. Hosts pair this with a directory sync before treating a newly
    /// published snapshot/manifest name as durable.
    pub fn rename(&mut self, from: FileId, to: FileId) -> Result<IoResult, IoError> {
        if from == to {
            return Err(IoError::AlreadyExists);
        }
        if let Some(target) = self.files.iter().position(|entry| entry.id == to) {
            if !matches!(
                to,
                FileId::Wal { segment: 0 } | FileId::StoreWal { segment: 0 }
            ) {
                return Err(IoError::AlreadyExists);
            }
            // A rename-over-fsynced-WAL is the atomic prefix-reclamation
            // point. Choosing the new complete name as the crash outcome is
            // one of the two filesystem-permitted results; sync_dir still
            // orders subsequent deletion/publication effects.
            self.files.remove(target);
        }
        let file = self
            .files
            .iter_mut()
            .find(|entry| entry.id == from)
            .ok_or(IoError::NotFound)?;
        file.id = to;
        if matches!(
            to,
            FileId::Wal { segment: 0 } | FileId::StoreWal { segment: 0 }
        ) {
            file.durable_id = to;
        }
        if let Some(bitrot) = self.bitrot.as_mut()
            && bitrot.file == from
        {
            bitrot.file = to;
        }
        Ok(IoResult::Fsynced)
    }

    pub fn sync_dir(&mut self) -> Result<IoResult, IoError> {
        if matches!(self.fault, Some(DiskFault::EioNextFsync)) {
            self.fault = None;
            return Err(IoError::Eio);
        }
        for file in &mut self.files {
            file.durable_id = file.id;
        }
        Ok(IoResult::Fsynced)
    }

    pub fn crash(&mut self) {
        for file in &mut self.files {
            file.visible.clone_from(&file.durable);
            file.id = file.durable_id;
        }
    }

    #[must_use]
    pub fn durable(&self, file: FileId) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|entry| entry.durable_id == file)
            .map(|entry| entry.durable.as_slice())
    }

    #[must_use]
    pub fn visible(&self, file: FileId) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|entry| entry.id == file)
            .map(|entry| entry.visible.as_slice())
    }

    /// Verify the durable image before a recovery boundary.  Page-cache bytes
    /// are intentionally not substituted here: after a crash only this image
    /// is authoritative.
    pub fn verify_durable(&self, file: FileId) -> Result<(), IoError> {
        let file_state = self
            .files
            .iter()
            .find(|entry| entry.durable_id == file)
            .ok_or(IoError::NotFound)?;
        self.verify_file(file_state)
    }

    fn verify_file(&self, file: &SimFile) -> Result<(), IoError> {
        if crc32c(&file.durable) == file.durable_crc32c {
            Ok(())
        } else {
            Err(IoError::Corrupt(
                "simulated durable checksum mismatch".to_owned(),
            ))
        }
    }

    fn file_mut(&mut self, file: FileId) -> &mut SimFile {
        if let Some(index) = self.files.iter().position(|entry| entry.id == file) {
            return &mut self.files[index];
        }
        self.files.push(SimFile {
            id: file,
            durable_id: file,
            visible: Vec::new(),
            durable: Vec::new(),
            durable_crc32c: crc32c(&[]),
        });
        self.files
            .last_mut()
            .expect("invariant: file inserted into disk")
    }

    pub fn visible_bytes(&self) -> u64 {
        self.files.iter().fold(0_u64, |total, file| {
            total.saturating_add(u64::try_from(file.visible.len()).unwrap_or(u64::MAX))
        })
    }
}

impl Default for SimDisk {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkConfig {
    pub base_delay: Duration,
    pub jitter: DelayDist,
    pub drop: P16,
    pub duplicate: P16,
    pub max_inflight: u64,
    /// Reservations are per scheduled datagram copy, including duplicates.
    /// This prevents a small count of very large frames from escaping the
    /// simulator's resource model.
    pub max_inflight_bytes: u64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(1),
            jitter: DelayDist::Fixed(Duration::default()),
            drop: P16::ZERO,
            duplicate: P16::ZERO,
            max_inflight: 4_096,
            max_inflight_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
struct LinkState {
    config: LinkConfig,
    injected_delay: Duration,
    blocked: bool,
    inflight: u64,
    inflight_bytes: u64,
    rng: Xoshiro256pp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub at: Time,
    pub from: NodeId,
    pub to: NodeId,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkDecision {
    Delivered(Delivery),
    Dropped,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetworkError {
    UnknownLink { from: NodeId, to: NodeId },
}

/// Directional unreliable-datagram network model.
#[derive(Clone)]
pub struct Network {
    links: BTreeMap<(NodeId, NodeId), LinkState>,
    peak_inflight_bytes: u64,
}

impl Network {
    #[must_use]
    pub fn new(nodes: &[NodeId], seed: Seed, config: LinkConfig) -> Self {
        let mut links = BTreeMap::new();
        for from in nodes {
            for to in nodes {
                if from != to {
                    let index = from.get().wrapping_mul(1_000_003).wrapping_add(to.get());
                    links.insert(
                        (*from, *to),
                        LinkState {
                            config,
                            injected_delay: Duration::default(),
                            blocked: false,
                            inflight: 0,
                            inflight_bytes: 0,
                            rng: Xoshiro256pp::stream(seed, "link", index),
                        },
                    );
                }
            }
        }
        Self {
            links,
            peak_inflight_bytes: 0,
        }
    }

    pub fn set_blocked(
        &mut self,
        from: NodeId,
        to: NodeId,
        blocked: bool,
    ) -> Result<(), NetworkError> {
        self.links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?
            .blocked = blocked;
        Ok(())
    }

    pub fn configure(
        &mut self,
        from: NodeId,
        to: NodeId,
        config: LinkConfig,
    ) -> Result<(), NetworkError> {
        self.links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?
            .config = config;
        Ok(())
    }

    /// Return the complete effective configuration of one directed link. The
    /// theater uses this to display the simulator's value rather than a stale
    /// UI-local slider value.
    pub fn config(&self, from: NodeId, to: NodeId) -> Result<LinkConfig, NetworkError> {
        self.links
            .get(&(from, to))
            .map(|link| link.config)
            .ok_or(NetworkError::UnknownLink { from, to })
    }

    pub fn set_injected_delay(
        &mut self,
        from: NodeId,
        to: NodeId,
        extra: Duration,
    ) -> Result<(), NetworkError> {
        self.links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?
            .injected_delay = extra;
        Ok(())
    }

    pub fn clear_injected_delays(&mut self) {
        for link in self.links.values_mut() {
            link.injected_delay = Duration::default();
        }
    }

    pub fn send(
        &mut self,
        now: Time,
        from: NodeId,
        to: NodeId,
        payload: Vec<u8>,
    ) -> Result<Vec<NetworkDecision>, NetworkError> {
        let total_inflight_before = self.total_inflight_bytes();
        let link = self
            .links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?;
        if link.blocked || link.rng.chance(link.config.drop) {
            return Ok(vec![NetworkDecision::Dropped]);
        }
        let duplicated = link.rng.chance(link.config.duplicate);
        let copies = if duplicated { 2_u64 } else { 1 };
        let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let reserve_bytes = bytes.saturating_mul(copies);
        if link.inflight.saturating_add(copies) > link.config.max_inflight
            || link.inflight_bytes.saturating_add(reserve_bytes) > link.config.max_inflight_bytes
        {
            return Ok(vec![NetworkDecision::Dropped]);
        }
        let delay = link
            .config
            .base_delay
            .checked_add(link.rng.sample_delay(link.config.jitter))
            .and_then(|delay| delay.checked_add(link.injected_delay))
            .expect("invariant: network delay must not overflow virtual time");
        link.inflight += copies;
        link.inflight_bytes = link.inflight_bytes.saturating_add(reserve_bytes);
        self.peak_inflight_bytes = self
            .peak_inflight_bytes
            .max(total_inflight_before.saturating_add(reserve_bytes));
        let mut decisions = vec![NetworkDecision::Delivered(Delivery {
            at: now + delay,
            from,
            to,
            payload: payload.clone(),
        })];
        if duplicated {
            let duplicate_delay = link
                .config
                .base_delay
                .checked_add(link.rng.sample_delay(link.config.jitter))
                .and_then(|delay| delay.checked_add(link.injected_delay))
                .expect("invariant: network delay must not overflow virtual time");
            decisions.push(NetworkDecision::Delivered(Delivery {
                at: now + duplicate_delay,
                from,
                to,
                payload,
            }));
        }
        Ok(decisions)
    }

    pub fn complete(&mut self, from: NodeId, to: NodeId, bytes: usize) -> Result<(), NetworkError> {
        let link = self
            .links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?;
        link.inflight = link.inflight.saturating_sub(1);
        link.inflight_bytes = link
            .inflight_bytes
            .saturating_sub(u64::try_from(bytes).unwrap_or(u64::MAX));
        Ok(())
    }

    pub fn inflight(&self, from: NodeId, to: NodeId) -> Result<(u64, u64), NetworkError> {
        let link = self
            .links
            .get(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?;
        Ok((link.inflight, link.inflight_bytes))
    }

    #[must_use]
    pub fn total_inflight_bytes(&self) -> u64 {
        self.links.values().fold(0_u64, |total, link| {
            total.saturating_add(link.inflight_bytes)
        })
    }

    #[must_use]
    pub const fn peak_inflight_bytes(&self) -> u64 {
        self.peak_inflight_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultProfile {
    Calm,
    Gentle,
    Rough,
    Brutal,
    Membership,
    Corruption,
    Wipe,
    Starve,
    Ttl,
    Batch,
    FollowerRead,
    FollowerReadV2,
    StaleRead,
}

impl FaultProfile {
    /// Every profile, in declaration order. Callers use this to print the
    /// accepted set when a name fails to parse, so a typo names its
    /// alternatives instead of silently selecting a default profile.
    pub const ALL: [Self; 13] = [
        Self::Calm,
        Self::Gentle,
        Self::Rough,
        Self::Brutal,
        Self::Membership,
        Self::Corruption,
        Self::Wipe,
        Self::Starve,
        Self::Ttl,
        Self::Batch,
        Self::FollowerRead,
        Self::FollowerReadV2,
        Self::StaleRead,
    ];

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "calm" => Self::Calm,
            "gentle" => Self::Gentle,
            "rough" => Self::Rough,
            "brutal" => Self::Brutal,
            "membership" => Self::Membership,
            "corruption" => Self::Corruption,
            "wipe" => Self::Wipe,
            "starve" => Self::Starve,
            "ttl" => Self::Ttl,
            "batch" => Self::Batch,
            "follower-read" => Self::FollowerRead,
            "follower-read-v2" => Self::FollowerReadV2,
            "stale-read" => Self::StaleRead,
            _ => return None,
        })
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Calm => "calm",
            Self::Gentle => "gentle",
            Self::Rough => "rough",
            Self::Brutal => "brutal",
            Self::Membership => "membership",
            Self::Corruption => "corruption",
            Self::Wipe => "wipe",
            Self::Starve => "starve",
            Self::Ttl => "ttl",
            Self::Batch => "batch",
            Self::FollowerRead => "follower-read",
            Self::FollowerReadV2 => "follower-read-v2",
            Self::StaleRead => "stale-read",
        }
    }

    #[must_use]
    pub const fn workload_kind(self) -> WorkloadKind {
        match self {
            Self::Batch => WorkloadKind::Batch,
            Self::FollowerRead | Self::FollowerReadV2 => WorkloadKind::FollowerRead,
            Self::StaleRead => WorkloadKind::StaleRead,
            Self::Calm
            | Self::Gentle
            | Self::Rough
            | Self::Brutal
            | Self::Membership
            | Self::Corruption
            | Self::Wipe
            | Self::Starve
            | Self::Ttl => WorkloadKind::Mixed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FaultAction {
    Partition {
        left: Vec<NodeId>,
        right: Vec<NodeId>,
    },
    Heal,
    Crash {
        node: NodeId,
    },
    Restart {
        node: NodeId,
    },
    Wipe {
        node: NodeId,
    },
    ClockSkew {
        node: NodeId,
        offset: Duration,
    },
    DiskDegrade {
        node: NodeId,
        write_latency: Duration,
    },
    /// Persistently delay selected node disk operations. A zero value clears
    /// that operation's additional service time.
    SlowDisk {
        node: NodeId,
        slow: SlowDisk,
    },
    /// Every growth write fails from this point until an operator clears it.
    EnospcFrom {
        node: NodeId,
    },
    /// Flip one durable byte after the selected fsync and before recovery.
    BitRotAtRest {
        node: NodeId,
        file: FileId,
        offset: u64,
    },
    /// Lower the node's data-directory allocation cap. This is a persistent
    /// model constraint, not an ambient host filesystem limit.
    DiskQuota {
        node: NodeId,
        bytes: u64,
    },
    LinkDegrade {
        from: NodeId,
        to: NodeId,
        config: LinkConfig,
    },
    /// Flip one bit after CCPF framing on a deterministic per-link outbound
    /// ordinal. This exercises the transport CRC boundary, not Raft.
    CorruptFrame {
        from: NodeId,
        to: NodeId,
        nth: u64,
        byte: usize,
        bit: u8,
    },
    TruncateFrame {
        from: NodeId,
        to: NodeId,
        nth: u64,
        keep: usize,
    },
    /// Deliver the previously sent complete frame on this link a second time
    /// when the selected outbound ordinal is observed.
    ReplayFrame {
        from: NodeId,
        to: NodeId,
        nth: u64,
        at: Time,
    },
    /// Add deterministic latency to every datagram on one directed link until
    /// `Heal` clears the injected delay.
    DelayLink {
        from: NodeId,
        to: NodeId,
        extra: Duration,
    },
    /// Change the decoded CCRP value bytes and then recompute the enclosing
    /// CCPF checksum. This deliberately reaches the CCRP decoder.
    MutateRaftAndRechecksum {
        from: NodeId,
        to: NodeId,
        nth: u64,
        mutation: CcrpMutation,
    },
    /// Move the voting set to `voters` via one joint-consensus transition. The
    /// simulator only carries the node ids; what a joint config means is the
    /// host's business, so `cc-sim` stays generic per the crate DAG rule.
    Reconfigure {
        voters: Vec<NodeId>,
    },
}

/// Bounded, location-aware malformed CCRP mutations used only by the
/// deterministic transport fault profile. They are values instead of raw byte
/// offsets so campaigns remain stable if the outer framing changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CcrpMutation {
    MessageTag(u8),
    AppendEntryCount(u32),
    EntryPayloadLength(u32),
    OptionFlag(u8),
    FromNodeId(u64),
    Truncate(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultAt {
    pub at: Time,
    pub action: FaultAction,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    pub actions: Vec<FaultAt>,
}

impl FaultPlan {
    pub fn push(&mut self, action: FaultAt) {
        self.actions.push(action);
        self.actions.sort_by_key(|action| action.at);
    }

    /// Liveness is only required of plans that leave a quorum standing, and a
    /// reconfigure changes which set that quorum is counted over.
    #[must_use]
    pub fn is_survivable(&self, nodes: &[NodeId]) -> bool {
        let wiped: BTreeSet<NodeId> = self
            .actions
            .iter()
            .filter_map(|entry| match &entry.action {
                FaultAction::Wipe { node } => Some(*node),
                _ => None,
            })
            .collect();
        let voters: Vec<NodeId> = self
            .actions
            .iter()
            .rev()
            .find_map(|entry| match &entry.action {
                FaultAction::Reconfigure { voters } if !voters.is_empty() => Some(voters.clone()),
                _ => None,
            })
            .unwrap_or_else(|| nodes.to_vec());
        voters.iter().filter(|node| !wiped.contains(node)).count() * 2 > voters.len()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WorkloadKind {
    #[default]
    Mixed,
    Batch,
    FollowerRead,
    StaleRead,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadSpec {
    pub clients: u64,
    pub ops_per_second: u64,
    pub keyspace: u64,
    /// Relative expiry attached to generated SETs. A TTL workload uses only
    /// SET/GET so expiry visibility is isolated from numeric semantics.
    pub set_ttl: Option<Duration>,
    pub kind: WorkloadKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadOperation {
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    Del {
        key: Vec<u8>,
    },
    Incr {
        key: Vec<u8>,
    },
    Batch {
        commands: Vec<WorkloadOperation>,
    },
    ReadFollower {
        key: Vec<u8>,
    },
    ReadStale {
        key: Vec<u8>,
    },
}

#[derive(Clone)]
pub struct WorkloadActor {
    pub client: u64,
    pub next_sequence: u64,
    spec: WorkloadSpec,
    rng: Xoshiro256pp,
}

impl WorkloadActor {
    #[must_use]
    pub fn new(client: u64, seed: Seed, spec: WorkloadSpec) -> Self {
        Self {
            client,
            next_sequence: 1,
            rng: Xoshiro256pp::stream(seed, "workload", client),
            spec,
        }
    }

    #[must_use]
    pub fn next_operation(&mut self) -> (u64, WorkloadOperation) {
        let key = self.next_key();
        let operation = match self.spec.kind {
            WorkloadKind::Batch => self.next_batch_mix(key),
            WorkloadKind::FollowerRead => match self.rng.range_u64(0, 100) {
                0..=39 => self.next_set(key),
                _ => WorkloadOperation::ReadFollower { key },
            },
            WorkloadKind::StaleRead => match self.rng.range_u64(0, 100) {
                0..=39 => self.next_set(key),
                _ => WorkloadOperation::ReadStale { key },
            },
            WorkloadKind::Mixed if self.spec.set_ttl.is_some() => {
                match self.rng.range_u64(0, 100) {
                    0..=59 => WorkloadOperation::Get { key },
                    _ => self.next_set(key),
                }
            }
            WorkloadKind::Mixed => match self.rng.range_u64(0, 100) {
                0..=49 => WorkloadOperation::Get { key },
                50..=79 => self.next_set(key),
                80..=89 => WorkloadOperation::Del { key },
                _ => WorkloadOperation::Incr { key },
            },
        };
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        (sequence, operation)
    }

    fn next_key(&mut self) -> Vec<u8> {
        let key_number = self.rng.range_u64(0, self.spec.keyspace.max(1));
        format!("key-{key_number}").into_bytes()
    }

    fn next_set(&mut self, key: Vec<u8>) -> WorkloadOperation {
        WorkloadOperation::Set {
            key,
            value: self.rng.u64().to_le_bytes().to_vec(),
            ttl: self.spec.set_ttl,
        }
    }

    fn next_simple(&mut self, key: Vec<u8>) -> WorkloadOperation {
        match self.rng.range_u64(0, 4) {
            0 => WorkloadOperation::Get { key },
            1 => self.next_set(key),
            2 => WorkloadOperation::Del { key },
            _ => WorkloadOperation::Incr { key },
        }
    }

    fn next_batch_mix(&mut self, key: Vec<u8>) -> WorkloadOperation {
        match self.rng.range_u64(0, 100) {
            0..=19 => WorkloadOperation::Get { key },
            20..=39 => self.next_set(key),
            _ => {
                let count = 2_u64.saturating_add(self.rng.range_u64(0, 3));
                let mut commands = Vec::new();
                for _ in 0..count {
                    let key = self.next_key();
                    commands.push(self.next_simple(key));
                }
                WorkloadOperation::Batch { commands }
            }
        }
    }
}

impl Default for WorkloadSpec {
    fn default() -> Self {
        Self {
            clients: 4,
            ops_per_second: 50,
            keyspace: 512,
            set_ttl: None,
            kind: WorkloadKind::Mixed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSpec {
    pub seed: Seed,
    pub config: SimConfig,
    pub profile: FaultProfile,
    pub plan: FaultPlan,
    pub workload: WorkloadSpec,
    pub end_time: Time,
    pub host_limits: cc_core::HostLimits,
    /// Human-facing calibration identity. `None` means universal defaults;
    /// named profiles are optional and never rewrite those defaults.
    pub disk_profile: Option<String>,
}

impl RunSpec {
    #[must_use]
    pub fn standard(seed: Seed, profile: FaultProfile) -> Self {
        let config = SimConfig::default();
        let nodes: Vec<NodeId> = (1..=config.node_count).map(NodeId::new).collect();
        let end_time = config.end_time;
        let plan = materialize_fault_plan(seed, profile, &nodes, end_time);
        Self {
            seed,
            config,
            profile,
            plan,
            workload: WorkloadSpec::default(),
            end_time,
            host_limits: cc_core::HostLimits::default(),
            disk_profile: None,
        }
    }
}

#[must_use]
pub fn materialize_fault_plan(
    seed: Seed,
    profile: FaultProfile,
    nodes: &[NodeId],
    end_time: Time,
) -> FaultPlan {
    let mut plan = FaultPlan::default();
    let span = end_time.as_nanos();
    if !matches!(profile, FaultProfile::Calm) && nodes.len() >= 3 && span > 0 {
        let mut rng = Xoshiro256pp::stream(seed, "fault-plan", 0);
        let first = nodes[0];
        let second = nodes[1];
        // Vary the cut so the campaign is not forever testing "node 1 alone
        // versus the rest": some seeds isolate one node, some split two off.
        let minority: Vec<NodeId> = if rng.range_u64(0, 2) == 0 {
            vec![first]
        } else {
            nodes.iter().copied().take(2).collect()
        };
        let directional = rng.range_u64(0, 2) == 0;
        plan.push(FaultAt {
            at: at_percent(span, 20, &mut rng),
            action: FaultAction::Partition {
                left: minority.clone(),
                right: nodes
                    .iter()
                    .copied()
                    .filter(|id| !minority.contains(id))
                    .collect(),
            },
        });
        // A one-way link failure is not the same fault as a partition: the
        // leader still hears acks while its appends vanish. Seeds that pick a
        // directional cut exercise the asymmetric case §3.4 calls for.
        if directional {
            plan.push(FaultAt {
                at: at_percent(span, 22, &mut rng),
                action: FaultAction::LinkDegrade {
                    from: first,
                    to: second,
                    config: LinkConfig {
                        base_delay: Duration::from_millis(40),
                        drop: P16::new(24_576),
                        ..LinkConfig::default()
                    },
                },
            });
        }
        if matches!(
            profile,
            FaultProfile::Rough
                | FaultProfile::Brutal
                | FaultProfile::Membership
                | FaultProfile::Wipe
                | FaultProfile::Starve
                | FaultProfile::Batch
                | FaultProfile::FollowerRead
                | FaultProfile::StaleRead
        ) {
            plan.push(FaultAt {
                at: at_percent(span, 30, &mut rng),
                action: FaultAction::Crash { node: second },
            });
            plan.push(FaultAt {
                at: at_percent(span, 50, &mut rng),
                action: FaultAction::Restart { node: second },
            });
        }
        if matches!(profile, FaultProfile::Membership) {
            // Shrink the voting set while a node is already down, then restore
            // it. Which node leaves varies by seed, so the leader is sometimes
            // the one being removed.
            let removed = nodes[(rng.range_u64(0, nodes.len() as u64)) as usize];
            let shrunk: Vec<NodeId> = nodes.iter().copied().filter(|id| *id != removed).collect();
            plan.push(FaultAt {
                at: at_percent(span, 40, &mut rng),
                action: FaultAction::Reconfigure { voters: shrunk },
            });
            plan.push(FaultAt {
                at: at_percent(span, 60, &mut rng),
                action: FaultAction::Reconfigure {
                    voters: nodes.to_vec(),
                },
            });
        }
        if matches!(profile, FaultProfile::Wipe) {
            plan.push(FaultAt {
                at: at_percent(span, 25, &mut rng),
                action: FaultAction::Wipe { node: first },
            });
            // The wiped node has to come back, or the profile only proves the
            // cluster survives losing a disk — never that the owner of that
            // disk rejoins. Recovery from nothing is the whole point of the
            // wipe wing, and it is what forces state transfer rather than log
            // replay.
            plan.push(FaultAt {
                at: at_percent(span, 45, &mut rng),
                action: FaultAction::Restart { node: first },
            });
        }
        if matches!(profile, FaultProfile::Brutal) {
            plan.push(FaultAt {
                at: at_percent(span, 35, &mut rng),
                action: FaultAction::ClockSkew {
                    node: first,
                    offset: Duration::from_millis(25),
                },
            });
            plan.push(FaultAt {
                at: at_percent(span, 45, &mut rng),
                action: FaultAction::DiskDegrade {
                    node: second,
                    write_latency: Duration::from_millis(5),
                },
            });
        }
        if matches!(profile, FaultProfile::Starve) {
            plan.push(FaultAt {
                at: at_percent(span, 34, &mut rng),
                action: FaultAction::SlowDisk {
                    node: second,
                    slow: SlowDisk {
                        read_extra: Duration::from_millis(2),
                        write_extra: Duration::from_millis(8),
                        fsync_extra: Duration::from_millis(12),
                        rename_extra: Duration::from_millis(8),
                        dirsync_extra: Duration::from_millis(12),
                    },
                },
            });
            plan.push(FaultAt {
                at: at_percent(span, 42, &mut rng),
                action: FaultAction::DiskQuota {
                    node: second,
                    bytes: 256 * 1024,
                },
            });
            if seed.0.is_multiple_of(2) {
                plan.push(FaultAt {
                    at: at_percent(span, 46, &mut rng),
                    action: FaultAction::EnospcFrom { node: second },
                });
            }
        }
        if matches!(profile, FaultProfile::Corruption) {
            // Install all transport faults before the initial tick. Their
            // ordinals are global per-link counters, so selecting the first
            // four outbound frames makes the profile reach all decoder paths
            // without depending on timing or a random draw at delivery time.
            plan.push(FaultAt {
                at: Time::from_nanos(0),
                action: FaultAction::CorruptFrame {
                    from: first,
                    to: second,
                    nth: 1,
                    byte: 14,
                    bit: 0,
                },
            });
            plan.push(FaultAt {
                at: Time::from_nanos(0),
                action: FaultAction::TruncateFrame {
                    from: first,
                    to: second,
                    nth: 2,
                    keep: 4,
                },
            });
            plan.push(FaultAt {
                at: Time::from_nanos(0),
                action: FaultAction::MutateRaftAndRechecksum {
                    from: first,
                    to: second,
                    nth: 3,
                    mutation: CcrpMutation::MessageTag(255),
                },
            });
            plan.push(FaultAt {
                at: Time::from_nanos(0),
                action: FaultAction::ReplayFrame {
                    from: first,
                    to: second,
                    nth: 4,
                    at: Time::from_nanos(span / 2),
                },
            });
            plan.push(FaultAt {
                at: Time::from_nanos(0),
                action: FaultAction::DelayLink {
                    from: first,
                    to: second,
                    extra: Duration::from_millis(20),
                },
            });
        }
    }
    // §7.3's rule is `end_time − 30× election timeout`, but a short campaign run
    // is shorter than that margin. Clamping to a fraction of the run keeps the
    // heal late and, crucially, still after every fault above.
    let heal_margin = cc_raft::DEFAULT_ELECTION_MAX
        .as_nanos()
        .saturating_mul(30)
        .min(span.saturating_mul(30) / 100);
    plan.push(FaultAt {
        at: Time::from_nanos(span.saturating_sub(heal_margin)),
        action: FaultAction::Heal,
    });
    plan
}

#[cfg(test)]
mod workload_tests {
    use super::*;

    #[test]
    fn trap_batch_workload_emits_multi_command_batches() {
        let spec = WorkloadSpec {
            kind: WorkloadKind::Batch,
            ..WorkloadSpec::default()
        };
        let mut actor = WorkloadActor::new(1, Seed::new(7), spec);
        let mut saw_batch = false;
        for _ in 0..64 {
            match actor.next_operation().1 {
                WorkloadOperation::Batch { commands } => {
                    assert!(commands.len() >= 2);
                    assert!(commands.iter().all(|command| {
                        !matches!(
                            command,
                            WorkloadOperation::Batch { .. }
                                | WorkloadOperation::ReadFollower { .. }
                                | WorkloadOperation::ReadStale { .. }
                        )
                    }));
                    saw_batch = true;
                }
                WorkloadOperation::Get { .. }
                | WorkloadOperation::Set { .. }
                | WorkloadOperation::Del { .. }
                | WorkloadOperation::Incr { .. } => {}
                WorkloadOperation::ReadFollower { .. } | WorkloadOperation::ReadStale { .. } => {
                    panic!("batch mix must not emit follower or stale reads")
                }
            }
        }
        assert!(saw_batch, "batch mix must emit at least one atomic batch");
    }

    #[test]
    fn trap_follower_and_stale_workloads_route_reads_explicitly() {
        let mut follower = WorkloadActor::new(
            1,
            Seed::new(11),
            WorkloadSpec {
                kind: WorkloadKind::FollowerRead,
                ..WorkloadSpec::default()
            },
        );
        let mut stale = WorkloadActor::new(
            1,
            Seed::new(11),
            WorkloadSpec {
                kind: WorkloadKind::StaleRead,
                ..WorkloadSpec::default()
            },
        );
        assert!((0..32).any(|_| {
            matches!(
                follower.next_operation().1,
                WorkloadOperation::ReadFollower { .. }
            )
        }));
        assert!((0..32).any(|_| {
            matches!(
                stale.next_operation().1,
                WorkloadOperation::ReadStale { .. }
            )
        }));
    }

    #[test]
    fn trap_new_profiles_parse_and_select_workload_kind() {
        assert_eq!(FaultProfile::parse("batch"), Some(FaultProfile::Batch));
        assert_eq!(
            FaultProfile::parse("follower-read"),
            Some(FaultProfile::FollowerRead)
        );
        assert_eq!(
            FaultProfile::parse("follower-read-v2"),
            Some(FaultProfile::FollowerReadV2)
        );
        assert_eq!(
            FaultProfile::parse("stale-read"),
            Some(FaultProfile::StaleRead)
        );
        assert_eq!(FaultProfile::Batch.workload_kind(), WorkloadKind::Batch);
        assert_eq!(
            FaultProfile::FollowerReadV2.workload_kind(),
            WorkloadKind::FollowerRead
        );
        assert_eq!(
            FaultProfile::StaleRead.workload_kind(),
            WorkloadKind::StaleRead
        );
    }

    #[test]
    fn trap_profile_all_round_trips_every_variant() {
        // `ALL` is what the CLI prints when a profile name fails to parse. A
        // profile missing from it would be undiscoverable from the error
        // message, and a name that does not round-trip would be unusable.
        for profile in FaultProfile::ALL {
            assert_eq!(
                FaultProfile::parse(profile.as_str()),
                Some(profile),
                "{} does not round-trip through parse/as_str",
                profile.as_str()
            );
        }
        let names: BTreeSet<&str> = FaultProfile::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(
            names.len(),
            FaultProfile::ALL.len(),
            "two profiles share a name"
        );
        assert_eq!(FaultProfile::parse("no-such-profile"), None);
    }
}

/// Place a fault at a fraction of the run, with jitter of one percent, so a
/// three-second campaign run and a sixty-second soak exercise the same shape.
fn at_percent(span: u64, percent: u64, rng: &mut Xoshiro256pp) -> Time {
    let base = span.saturating_mul(percent) / 100;
    let jitter = (span / 100).max(1);
    Time::from_nanos(base.saturating_add(rng.range_u64(0, jitter)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeStatus {
    Up,
    Crashed,
    Wiped,
    StorageFault,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLifecycle {
    pub node: NodeId,
    pub status: NodeStatus,
    pub clock_offset: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lifecycle {
    pub nodes: Vec<NodeLifecycle>,
}

/// Deterministic delta-debugging over materialized actions.
pub fn shrink_fault_plan<F>(plan: &FaultPlan, mut still_fails: F) -> FaultPlan
where
    F: FnMut(&FaultPlan) -> bool,
{
    let mut candidate = plan.clone();
    let mut index = 0;
    while index < candidate.actions.len() {
        let mut trial = candidate.clone();
        trial.actions.remove(index);
        if still_fails(&trial) {
            candidate = trial;
        } else {
            index += 1;
        }
    }
    candidate
}

#[must_use]
pub fn canonicalize_fault_plan(plan: &FaultPlan) -> FaultPlan {
    let mut result = plan.clone();
    result.actions.sort_by_key(|action| action.at);
    result
}

impl Lifecycle {
    #[must_use]
    pub fn new(nodes: &[NodeId]) -> Self {
        Self {
            nodes: nodes
                .iter()
                .map(|node| NodeLifecycle {
                    node: *node,
                    status: NodeStatus::Up,
                    clock_offset: Duration::default(),
                })
                .collect(),
        }
    }

    pub fn apply(&mut self, action: &FaultAction) {
        match action {
            FaultAction::Crash { node } => self.set_status(*node, NodeStatus::Crashed),
            FaultAction::Restart { node } => self.set_status(*node, NodeStatus::Up),
            FaultAction::Wipe { node } => self.set_status(*node, NodeStatus::Wiped),
            FaultAction::ClockSkew { node, offset } => {
                if let Some(state) = self.nodes.iter_mut().find(|state| state.node == *node) {
                    state.clock_offset = *offset;
                }
            }
            FaultAction::Partition { .. }
            | FaultAction::Heal
            | FaultAction::DiskDegrade { .. }
            | FaultAction::SlowDisk { .. }
            | FaultAction::EnospcFrom { .. }
            | FaultAction::BitRotAtRest { .. }
            | FaultAction::DiskQuota { .. }
            | FaultAction::LinkDegrade { .. }
            | FaultAction::CorruptFrame { .. }
            | FaultAction::TruncateFrame { .. }
            | FaultAction::ReplayFrame { .. }
            | FaultAction::DelayLink { .. }
            | FaultAction::MutateRaftAndRechecksum { .. }
            | FaultAction::Reconfigure { .. } => {}
        }
    }

    fn set_status(&mut self, node: NodeId, status: NodeStatus) {
        if let Some(state) = self.nodes.iter_mut().find(|state| state.node == node) {
            state.status = status;
        }
    }
}

#[cfg(test)]
mod plan_coverage_tests {
    use super::*;

    fn kind_name(action: &FaultAction) -> &'static str {
        match action {
            FaultAction::Partition { .. } => "partition",
            FaultAction::Heal => "heal",
            FaultAction::Crash { .. } => "crash",
            FaultAction::Restart { .. } => "restart",
            FaultAction::Wipe { .. } => "wipe",
            FaultAction::ClockSkew { .. } => "clock-skew",
            FaultAction::DiskDegrade { .. } => "disk-degrade",
            FaultAction::SlowDisk { .. } => "slow-disk",
            FaultAction::EnospcFrom { .. } => "enospc-from",
            FaultAction::BitRotAtRest { .. } => "bitrot-at-rest",
            FaultAction::DiskQuota { .. } => "disk-quota",
            FaultAction::LinkDegrade { .. } => "link-degrade",
            FaultAction::CorruptFrame { .. } => "corrupt-frame",
            FaultAction::TruncateFrame { .. } => "truncate-frame",
            FaultAction::ReplayFrame { .. } => "replay-frame",
            FaultAction::DelayLink { .. } => "delay-link",
            FaultAction::MutateRaftAndRechecksum { .. } => "mutate-raft-and-rechecksum",
            FaultAction::Reconfigure { .. } => "reconfigure",
        }
    }

    /// Every fault a plan generates must land inside the run and before the
    /// closing heal. A campaign horizon shorter than the fault schedule used to
    /// silently drop crash and restart from every profile.
    #[test]
    fn every_generated_fault_lands_inside_the_run_and_before_the_heal() {
        let nodes: Vec<NodeId> = (1..=5).map(NodeId::new).collect();
        for span_ns in [1_000_000_000_u64, 3_000_000_000, 60_000_000_000] {
            let end = Time::from_nanos(span_ns);
            for profile in [
                FaultProfile::Rough,
                FaultProfile::Brutal,
                FaultProfile::Membership,
                FaultProfile::Corruption,
                FaultProfile::Wipe,
            ] {
                for seed in 0..64_u64 {
                    let plan = materialize_fault_plan(Seed::new(seed), profile, &nodes, end);
                    let heal = plan
                        .actions
                        .iter()
                        .find(|fault| matches!(fault.action, FaultAction::Heal))
                        .expect("every plan closes with a heal");
                    for fault in &plan.actions {
                        assert!(
                            fault.at <= end,
                            "{} at {} escapes the {span_ns}ns run",
                            kind_name(&fault.action),
                            fault.at.as_nanos()
                        );
                        if !matches!(fault.action, FaultAction::Heal) {
                            assert!(
                                fault.at < heal.at,
                                "{} must precede the closing heal",
                                kind_name(&fault.action)
                            );
                        }
                    }
                }
            }
        }
    }

    /// The palette in `docs` has to be the palette the campaigns actually
    /// generate — including the directional link cut and both partition shapes.
    #[test]
    fn generated_plans_cover_the_whole_fault_palette() {
        let nodes: Vec<NodeId> = (1..=5).map(NodeId::new).collect();
        let end = Time::from_nanos(3_000_000_000);
        let mut kinds = BTreeSet::new();
        let mut minority_sizes = BTreeSet::new();
        for profile in [
            FaultProfile::Rough,
            FaultProfile::Brutal,
            FaultProfile::Corruption,
            FaultProfile::Membership,
            FaultProfile::Wipe,
        ] {
            for seed in 0..256_u64 {
                for fault in materialize_fault_plan(Seed::new(seed), profile, &nodes, end).actions {
                    kinds.insert(kind_name(&fault.action));
                    if let FaultAction::Partition { left, .. } = &fault.action {
                        minority_sizes.insert(left.len());
                    }
                }
            }
        }
        for expected in [
            "partition",
            "heal",
            "crash",
            "restart",
            "wipe",
            "clock-skew",
            "disk-degrade",
            "link-degrade",
            "corrupt-frame",
            "truncate-frame",
            "replay-frame",
            "delay-link",
            "mutate-raft-and-rechecksum",
            "reconfigure",
        ] {
            assert!(kinds.contains(expected), "{expected} is never generated");
        }
        assert_eq!(
            minority_sizes,
            BTreeSet::from([1, 2]),
            "partitions must vary the cut, not always isolate node 1"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_time_events_use_insertion_order() {
        let mut events = EventQueue::new();
        events.schedule(Time::from_nanos(5), "first");
        events.schedule(Time::from_nanos(5), "second");
        events.schedule(Time::from_nanos(5), "third");
        events.schedule(Time::from_nanos(6), "later");
        assert_eq!(
            [
                events.pop().expect("first event").event,
                events.pop().expect("second event").event,
                events.pop().expect("third event").event,
                events.pop().expect("fourth event").event,
            ],
            ["first", "second", "third", "later"]
        );
    }

    #[test]
    fn selfcheck_is_repeatable() {
        selfcheck(Seed::new(9)).expect("same seed must produce same trace");
    }

    #[test]
    fn disk_models_page_cache_loss() {
        let mut disk = SimDisk::new();
        let file = cc_env::FileId::Wal { segment: 0 };
        disk.write(file, 0, b"dirty").expect("write");
        disk.fsync(file).expect("fsync");
        disk.write(file, 5, b"lost").expect("write");
        disk.crash();
        assert_eq!(disk.durable(file), Some(b"dirty".as_slice()));
    }

    #[test]
    fn trap_rename_is_not_durable_until_the_directory_is_synced() {
        let mut disk = SimDisk::new();
        let staging = FileId::Temp { sequence: 7 };
        let published = FileId::Snapshot { generation: 4 };
        disk.write(staging, 0, b"checkpoint").expect("stage write");
        disk.fsync(staging).expect("stage fsync");
        disk.rename(staging, published).expect("rename");
        disk.crash();
        assert_eq!(disk.durable(staging), Some(b"checkpoint".as_slice()));
        assert_eq!(disk.durable(published), None);

        disk.rename(staging, published).expect("replay rename");
        disk.sync_dir().expect("directory sync");
        disk.crash();
        assert_eq!(disk.durable(staging), None);
        assert_eq!(disk.durable(published), Some(b"checkpoint".as_slice()));
    }

    #[test]
    fn disk_faults_are_one_shot_and_file_local() {
        let mut disk = SimDisk::new();
        let first = cc_env::FileId::Wal { segment: 0 };
        let second = cc_env::FileId::Wal { segment: 1 };
        disk.inject(DiskFault::EioNextWrite);
        assert_eq!(disk.write(first, 0, b"x"), Err(IoError::Eio));
        disk.write(first, 0, b"x").expect("fault consumed");
        disk.fsync(first).expect("fsync first");
        disk.write(second, 0, b"y").expect("write second");
        disk.crash();
        assert_eq!(disk.durable(first), Some(b"x".as_slice()));
        assert_eq!(disk.durable(second), Some([].as_slice()));
    }

    #[test]
    fn slow_disk_is_persistent_and_operation_specific() {
        let mut disk = SimDisk::new();
        let slow = SlowDisk {
            read_extra: Duration::from_millis(1),
            write_extra: Duration::from_millis(2),
            fsync_extra: Duration::from_millis(3),
            rename_extra: Duration::from_millis(4),
            dirsync_extra: Duration::from_millis(5),
        };
        disk.set_slow_disk(slow);
        assert_eq!(
            disk.service_time(DiskOperation::Read),
            Duration::from_millis(1)
        );
        assert_eq!(
            disk.service_time(DiskOperation::Write),
            Duration::from_millis(2)
        );
        assert_eq!(
            disk.service_time(DiskOperation::Fsync),
            Duration::from_millis(3)
        );
        assert_eq!(
            disk.service_time(DiskOperation::Rename),
            Duration::from_millis(4)
        );
        assert_eq!(
            disk.service_time(DiskOperation::SyncDir),
            Duration::from_millis(5)
        );
        let file = cc_env::FileId::Wal { segment: 0 };
        disk.write(file, 0, b"durable").expect("write");
        disk.fsync(file).expect("fsync");
        assert_eq!(
            disk.slow_disk(),
            slow,
            "ordinary I/O must not consume latency"
        );
    }

    #[test]
    fn calibrated_disk_model_samples_the_current_operation_only() {
        let mut read_buckets = [Duration::from_nanos(0); 16];
        read_buckets[..2].copy_from_slice(&[Duration::from_nanos(7), Duration::from_nanos(11)]);
        let model = DiskModel {
            read: DelayDist::Empirical {
                buckets: read_buckets,
                count: 2,
            },
            write: DelayDist::Fixed(Duration::from_nanos(13)),
            fsync: DelayDist::Fixed(Duration::from_nanos(17)),
            rename: DelayDist::Fixed(Duration::from_nanos(19)),
            dirsync: DelayDist::Fixed(Duration::from_nanos(23)),
        };
        let mut first = SimDisk::with_model(model, Seed::new(9), NodeId::new(1));
        let mut second = SimDisk::with_model(model, Seed::new(9), NodeId::new(1));
        let observed = [
            first.service_time(DiskOperation::Read),
            first.service_time(DiskOperation::Write),
            first.service_time(DiskOperation::Fsync),
            first.service_time(DiskOperation::Rename),
            first.service_time(DiskOperation::SyncDir),
        ];
        let repeated = [
            second.service_time(DiskOperation::Read),
            second.service_time(DiskOperation::Write),
            second.service_time(DiskOperation::Fsync),
            second.service_time(DiskOperation::Rename),
            second.service_time(DiskOperation::SyncDir),
        ];
        assert_eq!(observed, repeated, "named profiles remain replayable");
        assert!(matches!(observed[0].as_nanos(), 7 | 11));
        assert_eq!(observed[1], Duration::from_nanos(13));
        assert_eq!(observed[2], Duration::from_nanos(17));
        assert_eq!(observed[3], Duration::from_nanos(19));
        assert_eq!(observed[4], Duration::from_nanos(23));
    }

    #[test]
    fn disk_quota_and_enospc_reject_growth_without_corrupting_existing_bytes() {
        let mut disk = SimDisk::new();
        let file = cc_env::FileId::Wal { segment: 0 };
        disk.set_quota(Some(3));
        disk.write(file, 0, b"abc").expect("within quota");
        assert_eq!(disk.write(file, 3, b"d"), Err(IoError::Enospc));
        disk.write(file, 1, b"Z").expect("overwrite is not growth");
        disk.fsync(file).expect("fsync");
        assert_eq!(disk.durable(file), Some(b"aZc".as_slice()));
        disk.set_enospc(true);
        assert_eq!(disk.write(file, 0, b"x"), Err(IoError::Enospc));
        assert_eq!(disk.durable(file), Some(b"aZc".as_slice()));
    }

    #[test]
    fn trap_data_directory_quota_counts_live_temp_and_staging_bytes() {
        let mut disk = SimDisk::new();
        disk.set_quota(Some(8));
        let live = cc_env::FileId::Wal { segment: 0 };
        let temp = cc_env::FileId::Temp { sequence: 1 };
        disk.write(live, 0, b"live").expect("live bytes");
        disk.write(temp, 0, b"temp").expect("temporary bytes");
        assert_eq!(disk.visible_bytes(), 8);
        assert_eq!(disk.write(temp, 4, b"x"), Err(IoError::Enospc));
        disk.delete(temp).expect("delete temp");
        assert_eq!(disk.visible_bytes(), 4);
        disk.write(live, 4, b"room").expect("released quota");
    }

    #[test]
    fn trap_disk_quota_counts_snapshot_staging() {
        let mut disk = SimDisk::new();
        disk.set_quota(Some(6));
        let staging = cc_env::FileId::Temp { sequence: 44 };
        disk.write(staging, 0, b"chunk1")
            .expect("first staged chunk");
        assert_eq!(disk.write(staging, 6, b"2"), Err(IoError::Enospc));
        assert_eq!(disk.visible(staging), Some(b"chunk1".as_slice()));
    }

    #[test]
    fn trap_snapshot_install_is_atomic_across_crash() {
        let staging = cc_env::FileId::Temp { sequence: 8 };
        let published = cc_env::FileId::Snapshot { generation: 4 };

        let mut before_publish = SimDisk::new();
        before_publish
            .write(staging, 0, b"new-checkpoint")
            .expect("stage");
        before_publish.fsync(staging).expect("stage fsync");
        before_publish.crash();
        assert_eq!(before_publish.durable(published), None);
        assert_eq!(
            before_publish.durable(staging),
            Some(b"new-checkpoint".as_slice())
        );

        let mut after_publish = SimDisk::new();
        after_publish
            .write(staging, 0, b"new-checkpoint")
            .expect("stage");
        after_publish.fsync(staging).expect("stage fsync");
        after_publish.rename(staging, published).expect("rename");
        after_publish.sync_dir().expect("directory fsync");
        after_publish.crash();
        assert_eq!(
            after_publish.durable(published),
            Some(b"new-checkpoint".as_slice())
        );
        assert_eq!(after_publish.durable(staging), None);
    }

    #[test]
    fn bitrot_flips_one_durable_byte_after_the_selected_fsync() {
        let mut disk = SimDisk::new();
        let file = cc_env::FileId::Wal { segment: 0 };
        disk.write(file, 0, b"stable").expect("write");
        disk.inject_bitrot(file, 2);
        disk.fsync(file).expect("fsync with bit rot");
        assert_eq!(disk.durable(file), Some(b"st`ble".as_slice()));
        disk.crash();
        assert_eq!(
            disk.read(file, 0, 6),
            Err(IoError::Corrupt(
                "simulated durable checksum mismatch".to_owned()
            )),
            "an at-rest corruption must fail closed rather than be served"
        );
        assert_eq!(
            disk.verify_durable(file),
            Err(IoError::Corrupt(
                "simulated durable checksum mismatch".to_owned()
            ))
        );
    }

    #[test]
    fn disk_can_make_a_torn_prefix() {
        let mut disk = SimDisk::new();
        let file = cc_env::FileId::Wal { segment: 0 };
        disk.inject(DiskFault::TornNextWrite { prefix_len: 2 });
        disk.write(file, 0, b"whole").expect("torn write completes");
        disk.fsync(file).expect("fsync");
        assert_eq!(disk.durable(file), Some(b"wh".as_slice()));
    }

    #[test]
    fn network_partition_is_directional_and_delivery_is_seeded() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let mut network = Network::new(&nodes, Seed::new(4), LinkConfig::default());
        network.set_blocked(nodes[0], nodes[1], true).expect("link");
        assert_eq!(
            network
                .send(Time::from_nanos(0), nodes[0], nodes[1], vec![1])
                .expect("send"),
            vec![NetworkDecision::Dropped]
        );
        let deliveries = network
            .send(Time::from_nanos(0), nodes[1], nodes[0], vec![2])
            .expect("send");
        assert!(matches!(deliveries[0], NetworkDecision::Delivered(_)));
    }

    #[test]
    fn network_duplicate_delivery_is_explicit() {
        let nodes = [NodeId::new(1), NodeId::new(2)];
        let config = LinkConfig {
            duplicate: P16::MAX,
            ..LinkConfig::default()
        };
        let mut network = Network::new(&nodes, Seed::new(5), config);
        let decisions = network
            .send(Time::from_nanos(0), nodes[0], nodes[1], vec![7])
            .expect("send");
        assert!(
            decisions
                .iter()
                .filter(|decision| matches!(decision, NetworkDecision::Delivered(_)))
                .count()
                >= 1
        );
        assert_eq!(network.inflight(nodes[0], nodes[1]), Ok((2, 2)));
        for decision in decisions {
            if let NetworkDecision::Delivered(delivery) = decision {
                network
                    .complete(nodes[0], nodes[1], delivery.payload.len())
                    .expect("complete");
            }
        }
        assert_eq!(network.inflight(nodes[0], nodes[1]), Ok((0, 0)));
    }

    #[test]
    fn network_byte_cap_applies_to_each_duplicate_copy() {
        let nodes = [NodeId::new(1), NodeId::new(2)];
        let config = LinkConfig {
            duplicate: P16::MAX,
            max_inflight_bytes: 7,
            ..LinkConfig::default()
        };
        let mut network = Network::new(&nodes, Seed::new(5), config);
        assert_eq!(
            network
                .send(Time::from_nanos(0), nodes[0], nodes[1], vec![7; 4])
                .expect("send"),
            vec![NetworkDecision::Dropped]
        );
        assert_eq!(network.inflight(nodes[0], nodes[1]), Ok((0, 0)));
    }

    #[test]
    fn network_max_inflight_drops_until_completion() {
        let nodes = [NodeId::new(1), NodeId::new(2)];
        let config = LinkConfig {
            max_inflight: 1,
            ..LinkConfig::default()
        };
        let mut network = Network::new(&nodes, Seed::new(6), config);
        let first = network
            .send(Time::from_nanos(0), nodes[0], nodes[1], vec![1])
            .expect("first send");
        assert!(matches!(first[0], NetworkDecision::Delivered(_)));
        let second = network
            .send(Time::from_nanos(0), nodes[0], nodes[1], vec![2])
            .expect("second send");
        assert_eq!(second, vec![NetworkDecision::Dropped]);
        network.complete(nodes[0], nodes[1], 1).expect("complete");
        assert!(matches!(
            network
                .send(Time::from_nanos(0), nodes[0], nodes[1], vec![3])
                .expect("retry")[0],
            NetworkDecision::Delivered(_)
        ));
    }

    #[test]
    fn disk_cross_file_reordering_is_lost_without_each_fsync() {
        let mut disk = SimDisk::new();
        let wal = cc_env::FileId::Wal { segment: 0 };
        let sst = cc_env::FileId::Sst { file_no: 1 };
        disk.write(wal, 0, b"wal").expect("wal");
        disk.fsync(wal).expect("wal fsync");
        disk.write(sst, 0, b"sst").expect("sst");
        disk.crash();
        assert_eq!(disk.durable(wal), Some(b"wal".as_slice()));
        assert_eq!(disk.durable(sst), Some([].as_slice()));
    }

    #[test]
    fn clock_skew_changes_lifecycle_clock_not_node_membership() {
        let node = NodeId::new(1);
        let mut lifecycle = Lifecycle::new(&[node]);
        lifecycle.apply(&FaultAction::ClockSkew {
            node,
            offset: Duration::from_millis(25),
        });
        assert_eq!(lifecycle.nodes[0].status, NodeStatus::Up);
        assert_eq!(lifecycle.nodes[0].clock_offset, Duration::from_millis(25));
    }

    #[test]
    fn fault_plan_is_materialized_and_survivability_is_explicit() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let left = materialize_fault_plan(
            Seed::new(8),
            FaultProfile::Rough,
            &nodes,
            Time::from_nanos(10_000_000_000),
        );
        let right = materialize_fault_plan(
            Seed::new(8),
            FaultProfile::Rough,
            &nodes,
            Time::from_nanos(10_000_000_000),
        );
        assert_eq!(left, right);
        assert!(
            left.actions
                .iter()
                .any(|entry| entry.action == FaultAction::Heal)
        );
        assert!(left.is_survivable(&nodes));
    }

    #[test]
    fn generated_plans_heal_thirty_election_timeouts_before_the_end() {
        let nodes = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let end = Time::from_nanos(60_000_000_000);
        let plan = materialize_fault_plan(Seed::new(9), FaultProfile::Rough, &nodes, end);
        let heal = plan
            .actions
            .last()
            .expect("generated plan has a terminal heal");
        assert_eq!(heal.action, FaultAction::Heal);
        assert_eq!(
            heal.at,
            Time::from_nanos(end.as_nanos() - cc_raft::DEFAULT_ELECTION_MAX.as_nanos() * 30)
        );
    }

    #[test]
    fn lifecycle_applies_crash_restart_and_wipe() {
        let node = NodeId::new(1);
        let mut lifecycle = Lifecycle::new(&[node]);
        lifecycle.apply(&FaultAction::Crash { node });
        assert_eq!(lifecycle.nodes[0].status, NodeStatus::Crashed);
        lifecycle.apply(&FaultAction::Restart { node });
        lifecycle.apply(&FaultAction::Wipe { node });
        assert_eq!(lifecycle.nodes[0].status, NodeStatus::Wiped);
    }

    #[test]
    fn workload_actor_is_repeatable_and_sequence_addressable() {
        let spec = WorkloadSpec::default();
        let mut first = WorkloadActor::new(7, Seed::new(12), spec.clone());
        let mut second = WorkloadActor::new(7, Seed::new(12), spec);
        for expected_sequence in 1..=32 {
            let left = first.next_operation();
            let right = second.next_operation();
            assert_eq!(left, right);
            assert_eq!(left.0, expected_sequence);
            assert_eq!(first.next_sequence, expected_sequence + 1);
        }
    }

    /// Plan-shape coverage only: this asserts that every generated wipe plan
    /// leaves a quorum standing. It builds no cluster and applies no wipe. The
    /// behavioural gate is
    /// `cc_swarm::tests::wiped_node_rejoins_by_installing_the_leader_snapshot`
    /// plus the `wipe` campaign profile, which requires the `snapshot-install`
    /// beacon.
    #[test]
    #[ignore = "G6 wipe plan-survivability sweep; run explicitly in release mode"]
    fn wipe_profile_plans_are_survivable_500k() {
        let nodes = [
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
            NodeId::new(5),
        ];
        for seed in 0..500_000_u64 {
            let plan = materialize_fault_plan(
                Seed::new(seed),
                FaultProfile::Wipe,
                &nodes,
                Time::from_nanos(60_000_000_000),
            );
            assert!(plan.is_survivable(&nodes));
        }
    }

    #[test]
    fn shrinker_deletes_irrelevant_actions_and_keeps_failure() {
        let plan = FaultPlan {
            actions: vec![
                FaultAt {
                    at: Time::from_nanos(1),
                    action: FaultAction::Crash {
                        node: NodeId::new(1),
                    },
                },
                FaultAt {
                    at: Time::from_nanos(2),
                    action: FaultAction::Heal,
                },
            ],
        };
        let shrunk = shrink_fault_plan(&plan, |candidate| {
            candidate.actions.iter().any(|entry| {
                matches!(&entry.action, FaultAction::Crash { node } if *node == NodeId::new(1))
            })
        });
        assert_eq!(shrunk.actions.len(), 1);
    }
}
