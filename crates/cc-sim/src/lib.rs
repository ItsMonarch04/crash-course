// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "A small deterministic discrete-event host used by tests and later cluster work."]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use cc_core::{
    DelayDist, Duration, EventKind, NodeId, P16, Seed, Time, TimerId, Trace, Xoshiro256pp,
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
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            end_time: Time::from_nanos(60_000_000_000),
            max_events: DEFAULT_MAX_EVENTS,
            max_events_per_instant: DEFAULT_MAX_EVENTS_PER_INSTANT,
            node_count: 5,
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueItem {
    at: Time,
    tie_seq: u64,
    event: SimEvent,
}

impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.tie_seq.cmp(&self.tie_seq))
    }
}

impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecorderLevel {
    Gate,
    Campaign,
    Theater,
}

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
    queue: BinaryHeap<QueueItem>,
    next_tie_seq: u64,
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
            queue: BinaryHeap::new(),
            next_tie_seq: 0,
            recorder: Recorder::new(seed, level),
            nodes,
            rng: Xoshiro256pp::stream(seed, "sim", 0),
        }
    }

    pub fn schedule(&mut self, at: Time, event: SimEvent) {
        let tie_seq = self.next_tie_seq;
        self.next_tie_seq = self
            .next_tie_seq
            .checked_add(1)
            .expect("invariant: scheduler tie sequence overflow");
        self.queue.push(QueueItem { at, tie_seq, event });
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct SimFile {
    id: FileId,
    visible: Vec<u8>,
    durable: Vec<u8>,
}

/// A deterministic page-cache disk. Writes are visible before fsync; crash
/// discards every visible byte not copied to the durable image.
#[derive(Clone, Debug, Default)]
pub struct SimDisk {
    files: Vec<SimFile>,
    fault: Option<DiskFault>,
}

impl SimDisk {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            files: Vec::new(),
            fault: None,
        }
    }

    pub fn inject(&mut self, fault: DiskFault) {
        self.fault = Some(fault);
    }

    pub fn write(&mut self, file: FileId, at: u64, bytes: &[u8]) -> Result<IoResult, IoError> {
        if matches!(self.fault, Some(DiskFault::EioNextWrite)) {
            self.fault = None;
            return Err(IoError::Eio);
        }
        let mut bytes_to_write = bytes.to_vec();
        if let Some(DiskFault::TornNextWrite { prefix_len }) = self.fault.take() {
            bytes_to_write.truncate(prefix_len.min(bytes_to_write.len()));
        }
        let file_state = self.file_mut(file);
        let at = usize::try_from(at).map_err(|_| IoError::InvalidRange)?;
        let end = at
            .checked_add(bytes_to_write.len())
            .ok_or(IoError::InvalidRange)?;
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
        let file_state = self.file_mut(file);
        file_state.durable.clone_from(&file_state.visible);
        Ok(IoResult::Fsynced)
    }

    pub fn truncate(&mut self, file: FileId, len: u64) -> Result<IoResult, IoError> {
        let len = usize::try_from(len).map_err(|_| IoError::InvalidRange)?;
        let file_state = self.file_mut(file);
        file_state.visible.truncate(len);
        Ok(IoResult::Truncated { len: len as u64 })
    }

    pub fn crash(&mut self) {
        for file in &mut self.files {
            file.visible.clone_from(&file.durable);
        }
    }

    #[must_use]
    pub fn durable(&self, file: FileId) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|entry| entry.id == file)
            .map(|entry| entry.durable.as_slice())
    }

    fn file_mut(&mut self, file: FileId) -> &mut SimFile {
        if let Some(index) = self.files.iter().position(|entry| entry.id == file) {
            return &mut self.files[index];
        }
        self.files.push(SimFile {
            id: file,
            visible: Vec::new(),
            durable: Vec::new(),
        });
        self.files
            .last_mut()
            .expect("invariant: file inserted into disk")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkConfig {
    pub base_delay: Duration,
    pub jitter: DelayDist,
    pub drop: P16,
    pub duplicate: P16,
    pub max_inflight: u64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(1),
            jitter: DelayDist::Fixed(Duration::default()),
            drop: P16::ZERO,
            duplicate: P16::ZERO,
            max_inflight: 4_096,
        }
    }
}

#[derive(Clone, Debug)]
struct LinkState {
    config: LinkConfig,
    blocked: bool,
    inflight: u64,
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
pub struct Network {
    links: BTreeMap<(NodeId, NodeId), LinkState>,
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
                            blocked: false,
                            inflight: 0,
                            rng: Xoshiro256pp::stream(seed, "link", index),
                        },
                    );
                }
            }
        }
        Self { links }
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

    pub fn send(
        &mut self,
        now: Time,
        from: NodeId,
        to: NodeId,
        payload: Vec<u8>,
    ) -> Result<Vec<NetworkDecision>, NetworkError> {
        let link = self
            .links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?;
        if link.blocked
            || link.inflight >= link.config.max_inflight
            || link.rng.chance(link.config.drop)
        {
            return Ok(vec![NetworkDecision::Dropped]);
        }
        let delay = link
            .config
            .base_delay
            .checked_add(link.rng.sample_delay(link.config.jitter))
            .expect("invariant: network delay must not overflow virtual time");
        link.inflight += 1;
        let mut decisions = vec![NetworkDecision::Delivered(Delivery {
            at: now + delay,
            from,
            to,
            payload: payload.clone(),
        })];
        if link.rng.chance(link.config.duplicate) {
            let duplicate_delay = link
                .config
                .base_delay
                .checked_add(link.rng.sample_delay(link.config.jitter))
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

    pub fn complete(&mut self, from: NodeId, to: NodeId) -> Result<(), NetworkError> {
        let link = self
            .links
            .get_mut(&(from, to))
            .ok_or(NetworkError::UnknownLink { from, to })?;
        link.inflight = link.inflight.saturating_sub(1);
        Ok(())
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
}

impl FaultProfile {
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
    LinkDegrade {
        from: NodeId,
        to: NodeId,
        config: LinkConfig,
    },
    /// Move the voting set to `voters` via one joint-consensus transition. The
    /// simulator only carries the node ids; what a joint config means is the
    /// host's business, so `cc-sim` stays generic per the crate DAG rule.
    Reconfigure {
        voters: Vec<NodeId>,
    },
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
        self.actions.sort_by(|left, right| left.at.cmp(&right.at));
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadSpec {
    pub clients: u64,
    pub ops_per_second: u64,
    pub keyspace: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadOperation {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    Del { key: Vec<u8> },
    Incr { key: Vec<u8> },
}

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
    pub fn next(&mut self) -> (u64, WorkloadOperation) {
        let key_number = self.rng.range_u64(0, self.spec.keyspace.max(1));
        let key = format!("key-{key_number}").into_bytes();
        let operation = match self.rng.range_u64(0, 100) {
            0..=49 => WorkloadOperation::Get { key },
            50..=79 => WorkloadOperation::Set {
                key,
                value: self.rng.u64().to_le_bytes().to_vec(),
            },
            80..=89 => WorkloadOperation::Del { key },
            _ => WorkloadOperation::Incr { key },
        };
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        (sequence, operation)
    }
}

impl Default for WorkloadSpec {
    fn default() -> Self {
        Self {
            clients: 4,
            ops_per_second: 50,
            keyspace: 512,
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
    result.actions.sort_by(|left, right| left.at.cmp(&right.at));
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
            | FaultAction::LinkDegrade { .. }
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
            FaultAction::LinkDegrade { .. } => "link-degrade",
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
        let mut sim = Sim::new(Seed::new(1), SimConfig::default(), RecorderLevel::Gate);
        sim.schedule(
            Time::from_nanos(5),
            SimEvent::Tick {
                target: Target::Node(NodeId::new(2)),
            },
        );
        sim.schedule(
            Time::from_nanos(5),
            SimEvent::Tick {
                target: Target::Node(NodeId::new(1)),
            },
        );
        sim.schedule(
            Time::from_nanos(5),
            SimEvent::Tick {
                target: Target::Node(NodeId::new(2)),
            },
        );
        let trace = sim.run_toy().expect("run completes");
        assert_eq!(trace.events[0].node, Some(NodeId::new(2)));
        assert_eq!(trace.events[1].node, Some(NodeId::new(1)));
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
        let mut config = LinkConfig::default();
        config.duplicate = P16::MAX;
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
    }

    #[test]
    fn network_max_inflight_drops_until_completion() {
        let nodes = [NodeId::new(1), NodeId::new(2)];
        let mut config = LinkConfig::default();
        config.max_inflight = 1;
        let mut network = Network::new(&nodes, Seed::new(6), config);
        let first = network
            .send(Time::from_nanos(0), nodes[0], nodes[1], vec![1])
            .expect("first send");
        assert!(matches!(first[0], NetworkDecision::Delivered(_)));
        let second = network
            .send(Time::from_nanos(0), nodes[0], nodes[1], vec![2])
            .expect("second send");
        assert_eq!(second, vec![NetworkDecision::Dropped]);
        network.complete(nodes[0], nodes[1]).expect("complete");
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
            let left = first.next();
            let right = second.next();
            assert_eq!(left, right);
            assert_eq!(left.0, expected_sequence);
            assert_eq!(first.next_sequence, expected_sequence + 1);
        }
    }

    #[test]
    #[ignore = "G6 wipe campaign; run explicitly in release mode"]
    fn wipe_profile_campaign_500k() {
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
