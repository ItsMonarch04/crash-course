// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Host-neutral node driver: boundary translation, timer generations, and I/O correlation."]

pub mod journal;

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use cc_cluster::{
    Node, NodeConfig, NodeEffect, NodeError, RecoveredNode, TimerKind, encode_client_reply,
    encode_durability_effect, encode_peer_effect,
};
use cc_core::{Duration, IoId, NodeId, RequestSeq, Time, TimerId};
use cc_env::{Effect, FileId, Input, IoResult};
use cc_store::BlockSource;

pub const DEFAULT_MAX_PENDING_INPUTS: usize = 16_384;

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

#[derive(Clone, Copy, Debug)]
struct TimerState {
    at: Time,
    generation: u64,
    kind: TimerKind,
}

#[derive(Clone, Copy, Debug)]
enum IoStage {
    Write { file: FileId, len: u32 },
    Fsync { file: FileId },
}

impl IoStage {
    const fn file(self) -> FileId {
        match self {
            Self::Write { file, .. } | Self::Fsync { file } => file,
        }
    }
}

#[derive(Debug)]
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
    blocked: Option<BlockedStep>,
    peak_pending_input_bytes: usize,
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
            blocked: None,
            peak_pending_input_bytes: 0,
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

    /// Host-facing configuration transition entry point.  Keeping this
    /// translation here prevents adapters from reintroducing the private
    /// `NodeEffect` vocabulary merely to schedule an already replicated
    /// membership operation.
    pub fn enter_joint(
        &mut self,
        now: Time,
        voters: std::collections::BTreeSet<NodeId>,
    ) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        let effects = self.node.enter_joint(voters)?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
    }

    /// Completes a previously committed joint-consensus transition through
    /// the same host boundary as ordinary inputs.
    pub fn leave_joint(&mut self, now: Time) -> Result<(DriverPoll, Vec<Effect>), HostError> {
        let effects = self.node.leave_joint()?;
        self.defer_or_translate(now, Duration::from_nanos(0), Ok(effects))
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
            Some(input) => {
                let (poll, effects) = self.deliver(now, input.clone(), blocks)?;
                Ok((Some(input), poll, effects))
            }
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
        NodeFootprint {
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
                let fsync_id = self.allocate_io()?;
                self.pending_io.insert(fsync_id, IoStage::Fsync { file });
                Ok((
                    DriverPoll::Ready,
                    vec![Effect::DiskFsync { file, id: fsync_id }],
                ))
            }
            (IoStage::Fsync { file }, IoResult::Fsynced) => {
                let _ = file;
                let effects = self
                    .node
                    .on_input(cc_cluster::NodeInput::Persisted { success: true })?;
                Ok((DriverPoll::Ready, self.translate(effects)?))
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
        let mut output = Vec::new();
        for effect in source {
            match effect {
                NodeEffect::Send(message) => {
                    output.push(Effect::Send {
                        to: message.to,
                        msg: encode_peer_effect(&message)?,
                    });
                }
                effect @ (NodeEffect::PersistHard(_)
                | NodeEffect::PersistEntries(_)
                | NodeEffect::TruncateSuffix(_)) => {
                    let bytes = encode_durability_effect(&effect)?
                        .ok_or(HostError::Node(NodeError::Durability))?;
                    output.push(self.issue_raw_wal_write(bytes)?);
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
                }
                // Detailed trace payloads are produced by the recorder.  This
                // legacy marker deliberately has no lossy synthetic Event.
                NodeEffect::Trace(_) => {}
            }
        }
        Ok(output)
    }

    fn issue_raw_wal_write(&mut self, bytes: Vec<u8>) -> Result<Effect, HostError> {
        if self.pending_io.len() >= self.limits.max_pending_io {
            return Err(HostError::QueueFull(InputClass::Io));
        }
        let len = u32::try_from(bytes.len()).map_err(|_| HostError::Node(NodeError::Durability))?;
        let at = self.next_wal_offset;
        self.next_wal_offset = self
            .next_wal_offset
            .checked_add(u64::from(len))
            .ok_or(HostError::IoIdExhausted)?;
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

    use super::*;
    use cc_cluster::{Message, MessageKind, NodeConfig, PROTOCOL_VERSION, RaftConfig};
    use cc_core::{ClusterPolicy, HostLimits, NodeId, Seed};
    use cc_env::IoError;
    use cc_store::{MemoryBlockSource, StoreConfig};

    fn config() -> NodeConfig {
        NodeConfig {
            id: NodeId::new(1),
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
        assert!(matches!(
            driver.deliver_next(Time::from_nanos(1), &mut blocks),
            Err(HostError::Node(NodeError::NotLeader))
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
}
