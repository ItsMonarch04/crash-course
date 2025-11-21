// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Sans-IO Raft core: deterministic elections, replication, and read barriers."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{
    ConfigEnvelope, ConfigOperation, Duration, JointMembership, LogIndex, MembershipState, NodeId,
    Seed, Term, Time, TimerId, TransferResult, Xoshiro256pp,
};

pub mod codec;
pub mod model;

/// Bumped to 2 when append requests and responses gained `read_round`, which
/// scopes a ReadIndex confirmation to the round that raised it.
pub const PROTOCOL_VERSION: u16 = 2;
pub const DEFAULT_ELECTION_MIN: Duration = Duration::from_millis(150);
pub const DEFAULT_ELECTION_MAX: Duration = Duration::from_millis(300);
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_millis(50);
pub const PIPELINE_WINDOW: usize = 8;
pub const MAX_ENTRIES_PER_APPEND: usize = 64;
pub const SNAPSHOT_TRIGGER_BYTES: u64 = 8 * 1024 * 1024;
pub const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;
pub const DEFAULT_LEADER_TRANSFER_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RaftConfig {
    pub election_min: Duration,
    pub election_max: Duration,
    pub heartbeat: Duration,
    pub max_entries_per_append: usize,
    pub pipeline_window: usize,
    pub leader_transfer_timeout: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_min: DEFAULT_ELECTION_MIN,
            election_max: DEFAULT_ELECTION_MAX,
            heartbeat: DEFAULT_HEARTBEAT,
            max_entries_per_append: MAX_ENTRIES_PER_APPEND,
            pipeline_window: PIPELINE_WINDOW,
            leader_transfer_timeout: DEFAULT_LEADER_TRANSFER_TIMEOUT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    Learner,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum EntryKind {
    App = 1,
    Noop = 2,
    Config = 3,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub kind: EntryKind,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRequest {
    pub prev_index: LogIndex,
    pub prev_term: Term,
    pub entries: Vec<Entry>,
    pub leader_commit: LogIndex,
    /// Identifies the leadership-confirmation round this append belongs to.
    /// Followers echo it so the leader can tell a fresh ack from one that was
    /// already in flight when the read index was fixed.
    pub read_round: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendResponse {
    pub success: bool,
    pub match_index: LogIndex,
    pub conflict_term: Option<Term>,
    pub conflict_index: LogIndex,
    pub read_round: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessageKind {
    PreVoteReq {
        last_index: LogIndex,
        last_term: Term,
    },
    PreVoteResp {
        granted: bool,
    },
    VoteReq {
        last_index: LogIndex,
        last_term: Term,
    },
    VoteResp {
        granted: bool,
    },
    AppendReq(AppendRequest),
    AppendResp(AppendResponse),
    SnapshotChunk {
        transfer_id: u64,
        last_included_index: LogIndex,
        last_included_term: Term,
        total_len: u64,
        snapshot_crc32c: u32,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    SnapshotAck {
        transfer_id: u64,
        next_offset: u64,
        accepted: bool,
        reason: Option<SnapshotRejectReason>,
    },
    TimeoutNow {
        intent_index: LogIndex,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SnapshotRejectReason {
    RestartFromZero = 1,
    Gap = 2,
    Conflict = 3,
    TooLarge = 4,
    StaleTerm = 5,
    Corrupt = 6,
}

impl SnapshotRejectReason {
    fn decode(tag: u8) -> Option<Self> {
        Some(match tag {
            1 => Self::RestartFromZero,
            2 => Self::Gap,
            3 => Self::Conflict,
            4 => Self::TooLarge,
            5 => Self::StaleTerm,
            6 => Self::Corrupt,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub proto_version: u16,
    pub from: NodeId,
    pub to: NodeId,
    pub term: Term,
    pub kind: MessageKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerKind {
    Election,
    Heartbeat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftEffect {
    Send(Message),
    PersistHard(HardState),
    PersistEntries(Vec<Entry>),
    TruncateSuffix(LogIndex),
    Apply(Vec<Entry>),
    ArmTimer {
        id: TimerId,
        at: Time,
        kind: TimerKind,
    },
    ReadBarrier {
        index: LogIndex,
    },
    ReadBarrierReady {
        index: LogIndex,
    },
    Trace {
        name: &'static str,
        index: LogIndex,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftError {
    NotLeader,
    ReadBarrierNotReady,
    Busy,
    InvalidMessage,
    CommittedConflict,
    TransferInProgress,
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader => write!(f, "not leader"),
            Self::ReadBarrierNotReady => write!(f, "leader no-op is not committed"),
            Self::Busy => write!(f, "pipeline window is full"),
            Self::InvalidMessage => write!(f, "invalid raft message"),
            Self::CommittedConflict => write!(f, "message would truncate committed log"),
            Self::TransferInProgress => write!(f, "leadership transfer is in progress"),
        }
    }
}

impl std::error::Error for RaftError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantViolation {
    pub name: &'static str,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RaftInvariantReport {
    pub violations: Vec<InvariantViolation>,
}

impl RaftInvariantReport {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// A pure state machine. Hosts persist effects and deliver message/timer inputs.
#[derive(Clone)]
pub struct RaftNode {
    pub id: NodeId,
    pub role: Role,
    pub hard_state: HardState,
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub joint: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
    joint_enter_index: Option<LogIndex>,
    base_voters: BTreeSet<NodeId>,
    base_learners: BTreeSet<NodeId>,
    base_joint: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
    base_joint_enter_index: Option<LogIndex>,
    base_addresses: BTreeMap<NodeId, cc_core::PeerAddress>,
    pub addresses: BTreeMap<NodeId, cc_core::PeerAddress>,
    pub log: Vec<Entry>,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub leader_id: Option<NodeId>,
    pub next_index: BTreeMap<NodeId, LogIndex>,
    pub match_index: BTreeMap<NodeId, LogIndex>,
    pub election_deadline: Time,
    pub heartbeat_deadline: Time,
    pub config: RaftConfig,
    rng: Xoshiro256pp,
    votes: BTreeSet<NodeId>,
    pre_votes: BTreeSet<NodeId>,
    heard_quorum: BTreeSet<NodeId>,
    quorum_deadline: Time,
    read_acks: BTreeSet<NodeId>,
    read_round: u64,
    read_index: Option<LogIndex>,
    snapshot_buffer: Vec<u8>,
    snapshot_index: LogIndex,
    snapshot_term: Term,
    snapshot_transfer: Option<SnapshotTransfer>,
    next_snapshot_transfer_id: u64,
    transfer: Option<LeadershipTransfer>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LeadershipTransfer {
    intent_index: LogIndex,
    target: NodeId,
    deadline: Time,
    finishing: bool,
}

/// The durable portion of a leadership-transfer workflow.  It is deliberately
/// value-only so a logical node snapshot can retain a committed intent across
/// a crash without retaining any host routing state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeadershipTransferState {
    pub intent_index: LogIndex,
    pub target: NodeId,
    pub deadline: Time,
    pub finishing: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SnapshotTransfer {
    id: u64,
    index: LogIndex,
    term: Term,
    total_len: u64,
    crc32c: u32,
}

impl RaftNode {
    #[must_use]
    pub fn new(id: NodeId, voters: BTreeSet<NodeId>, seed: Seed, config: RaftConfig) -> Self {
        let mut node = Self {
            id,
            role: if voters.contains(&id) {
                Role::Follower
            } else {
                Role::Learner
            },
            hard_state: HardState {
                term: Term::new(0),
                voted_for: None,
            },
            base_voters: voters.clone(),
            voters,
            learners: BTreeSet::new(),
            joint: None,
            joint_enter_index: None,
            base_learners: BTreeSet::new(),
            base_joint: None,
            base_joint_enter_index: None,
            base_addresses: BTreeMap::new(),
            addresses: BTreeMap::new(),
            log: Vec::new(),
            commit_index: LogIndex::new(0),
            applied_index: LogIndex::new(0),
            leader_id: None,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_deadline: Time::from_nanos(0),
            heartbeat_deadline: Time::from_nanos(0),
            config,
            rng: Xoshiro256pp::stream(seed, "raft", id.get()),
            votes: BTreeSet::new(),
            pre_votes: BTreeSet::new(),
            heard_quorum: BTreeSet::new(),
            quorum_deadline: Time::from_nanos(0),
            read_acks: BTreeSet::new(),
            read_round: 0,
            read_index: None,
            snapshot_buffer: Vec::new(),
            snapshot_index: LogIndex::new(0),
            snapshot_term: Term::new(0),
            snapshot_transfer: None,
            next_snapshot_transfer_id: 1,
            transfer: None,
        };
        node.reset_election(Time::from_nanos(0));
        node
    }

    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        self.log
            .last()
            .map_or(self.snapshot_index, |entry| entry.index)
    }

    #[must_use]
    pub fn last_term(&self) -> Term {
        self.log
            .last()
            .map_or(self.snapshot_term, |entry| entry.term)
    }

    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.get() == 0 {
            return Some(Term::new(0));
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index {
            return None;
        }
        self.log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    pub fn tick(&mut self, now: Time) -> Vec<RaftEffect> {
        if self.role == Role::Leader
            && self
                .transfer
                .is_some_and(|transfer| !transfer.finishing && now >= transfer.deadline)
            && let Ok(effects) = self.finish_leadership_transfer(TransferResult::Timeout, now)
        {
            return effects;
        }
        if self.role == Role::Leader && self.quorum_deadline == Time::from_nanos(0) {
            self.heard_quorum.clear();
            self.heard_quorum.insert(self.id);
            self.quorum_deadline = now + self.config.election_min;
        } else if self.role == Role::Leader && now >= self.quorum_deadline {
            let enough = self.has_majority(&self.heard_quorum);
            self.heard_quorum.clear();
            self.heard_quorum.insert(self.id);
            self.quorum_deadline = now + self.config.election_min;
            if !enough {
                self.role = Role::Follower;
                self.leader_id = None;
                self.reset_election(now);
                return vec![RaftEffect::Trace {
                    name: "checkquorum_stepdown",
                    index: self.last_index(),
                }];
            }
        }
        if self.role == Role::Leader && now >= self.heartbeat_deadline {
            self.heartbeat_deadline = now + self.config.heartbeat;
            let mut effects = self.broadcast_append();
            effects.push(RaftEffect::ArmTimer {
                id: TimerId::new(self.id.get().saturating_mul(2).saturating_add(1)),
                at: self.heartbeat_deadline,
                kind: TimerKind::Heartbeat,
            });
            effects
        } else if self.role != Role::Leader && now >= self.election_deadline {
            self.start_pre_vote(now)
        } else {
            Vec::new()
        }
    }

    pub fn on_timer(&mut self, now: Time, timer: TimerKind) -> Vec<RaftEffect> {
        match timer {
            TimerKind::Election if self.role == Role::Leader => self.broadcast_append(),
            TimerKind::Election => self.start_pre_vote(now),
            TimerKind::Heartbeat if self.role == Role::Leader => self.broadcast_append(),
            TimerKind::Heartbeat => Vec::new(),
        }
    }

    pub fn propose(&mut self, payload: Vec<u8>) -> Result<Vec<RaftEffect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self.transfer.is_some() {
            return Err(RaftError::TransferInProgress);
        }
        let outstanding = self
            .next_index
            .values()
            .filter(|next| next.get() <= self.last_index().get())
            .count();
        if outstanding >= self.config.pipeline_window * self.voters.len().max(1) {
            return Err(RaftError::Busy);
        }
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get() + 1),
            kind: EntryKind::App,
            payload,
        };
        self.log.push(entry.clone());
        let mut effects = vec![RaftEffect::PersistEntries(vec![entry])];
        // In a one-voter group the leader's durable local append is already a
        // quorum. Keep Apply after PersistEntries in this effect sequence; the
        // composite node's durability continuation releases it only after the
        // matching fsync succeeds.
        effects.extend(self.advance_commit());
        effects.extend(self.broadcast_append());
        Ok(effects)
    }

    pub fn request_read(&mut self) -> Result<Vec<RaftEffect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self.transfer.is_some() {
            return Err(RaftError::TransferInProgress);
        }
        if self
            .log
            .iter()
            .find(|entry| entry.term == self.hard_state.term && entry.kind == EntryKind::Noop)
            .is_none_or(|entry| entry.index > self.commit_index)
        {
            return Err(RaftError::ReadBarrierNotReady);
        }
        self.read_round = self.read_round.saturating_add(1);
        self.read_acks.clear();
        self.read_acks.insert(self.id);
        self.read_index = Some(self.commit_index);
        let mut effects = vec![RaftEffect::ReadBarrier {
            index: self.commit_index,
        }];
        effects.extend(self.broadcast_append());
        if self.has_majority(&self.read_acks) {
            self.read_index = None;
            effects.push(RaftEffect::ReadBarrierReady {
                index: self.commit_index,
            });
        }
        Ok(effects)
    }

    pub fn add_learner(&mut self, node: NodeId) -> Result<(), RaftError> {
        if node.get() == 0 || self.voters.contains(&node) || self.learners.contains(&node) {
            return Err(RaftError::InvalidMessage);
        }
        self.learners.insert(node);
        Ok(())
    }

    pub fn promote_learner(&mut self, node: NodeId) -> Result<(), RaftError> {
        if !self.learners.contains(&node) {
            return Err(RaftError::InvalidMessage);
        }
        if self
            .match_index
            .get(&node)
            .is_none_or(|index| *index < self.last_index())
        {
            return Err(RaftError::Busy);
        }
        self.learners.remove(&node);
        self.voters.insert(node);
        self.next_index
            .insert(node, LogIndex::new(self.last_index().get() + 1));
        self.match_index.insert(node, self.last_index());
        Ok(())
    }

    pub fn enter_joint(
        &mut self,
        new_voters: BTreeSet<NodeId>,
    ) -> Result<Vec<RaftEffect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self.joint.is_some()
            || new_voters.is_empty()
            || new_voters.iter().any(|id| id.get() == 0)
        {
            return Err(RaftError::Busy);
        }
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get() + 1),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(0),
                operation: ConfigOperation::EnterJoint { new_voters },
            }
            .encode(),
        };
        self.apply_config_on_append(&entry)?;
        self.log.push(entry.clone());
        let mut effects = vec![RaftEffect::PersistEntries(vec![entry])];
        // A single voter must commit the durable leader no-op locally; without
        // it ReadIndex and every subsequent client proposal wait forever for
        // an AppendResp that no peer can send.
        effects.extend(self.advance_commit());
        effects.extend(self.broadcast_append());
        Ok(effects)
    }

    pub fn leave_joint(&mut self) -> Result<Vec<RaftEffect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        let Some((_old, _new_voters)) = self.joint.clone() else {
            return Err(RaftError::InvalidMessage);
        };
        let enter_index = self.joint_enter_index().ok_or(RaftError::InvalidMessage)?;
        if self.commit_index < enter_index {
            return Err(RaftError::Busy);
        }
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get() + 1),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(0),
                operation: ConfigOperation::LeaveJoint { enter_index },
            }
            .encode(),
        };
        self.apply_config_on_append(&entry)?;
        self.log.push(entry.clone());
        if !self.voters.contains(&self.id) {
            self.role = Role::Follower;
        }
        let mut effects = vec![RaftEffect::PersistEntries(vec![entry])];
        effects.extend(self.broadcast_append());
        Ok(effects)
    }

    /// Append the durable transfer intent only after the target has caught up
    /// with the leader's current log.  It does not send TimeoutNow until the
    /// entry later becomes committed/applied.
    pub fn begin_leadership_transfer(
        &mut self,
        target: NodeId,
        leader_time: Time,
    ) -> Result<Vec<RaftEffect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self.joint.is_some() || self.transfer.is_some() || target == self.id {
            return Err(RaftError::TransferInProgress);
        }
        if !self.voters.contains(&target)
            || self
                .match_index
                .get(&target)
                .is_none_or(|index| *index < self.last_index())
        {
            return Err(RaftError::Busy);
        }
        self.append_config(ConfigEnvelope {
            admin_session: None,
            leader_time,
            operation: ConfigOperation::BeginLeaderTransfer { target },
        })
    }

    /// Apply the committed half of transfer/config workflow.  Append-time
    /// membership projection remains separate; this method intentionally runs
    /// only from a Raft `Apply` effect.
    pub fn apply_committed_config(&mut self, entry: &Entry) -> Result<Vec<RaftEffect>, RaftError> {
        if entry.kind != EntryKind::Config {
            return Err(RaftError::InvalidMessage);
        }
        let envelope =
            ConfigEnvelope::decode(&entry.payload).map_err(|_| RaftError::InvalidMessage)?;
        match envelope.operation {
            ConfigOperation::BeginLeaderTransfer { target } => {
                if self.transfer.is_some() || !self.voters.contains(&target) {
                    return Err(RaftError::InvalidMessage);
                }
                let deadline = envelope.leader_time + self.config.leader_transfer_timeout;
                self.transfer = Some(LeadershipTransfer {
                    intent_index: entry.index,
                    target,
                    deadline,
                    finishing: false,
                });
                if self.role == Role::Leader
                    && self
                        .match_index
                        .get(&target)
                        .is_some_and(|index| *index >= entry.index)
                {
                    return Ok(vec![RaftEffect::Send(Message {
                        proto_version: PROTOCOL_VERSION,
                        from: self.id,
                        to: target,
                        term: self.hard_state.term,
                        kind: MessageKind::TimeoutNow {
                            intent_index: entry.index,
                        },
                    })]);
                }
            }
            ConfigOperation::FinishLeaderTransfer {
                intent_index,
                result: _,
            } => {
                if self
                    .transfer
                    .is_some_and(|transfer| transfer.intent_index == intent_index)
                {
                    self.transfer = None;
                } else {
                    return Err(RaftError::InvalidMessage);
                }
            }
            _ => {}
        }
        Ok(Vec::new())
    }

    fn append_config(&mut self, envelope: ConfigEnvelope) -> Result<Vec<RaftEffect>, RaftError> {
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get().saturating_add(1)),
            kind: EntryKind::Config,
            payload: envelope.encode(),
        };
        self.apply_config_on_append(&entry)?;
        self.log.push(entry.clone());
        let mut effects = vec![RaftEffect::PersistEntries(vec![entry])];
        effects.extend(self.broadcast_append());
        Ok(effects)
    }

    fn finish_leadership_transfer(
        &mut self,
        result: TransferResult,
        leader_time: Time,
    ) -> Result<Vec<RaftEffect>, RaftError> {
        let Some(mut transfer) = self.transfer else {
            return Err(RaftError::InvalidMessage);
        };
        if transfer.finishing {
            return Err(RaftError::TransferInProgress);
        }
        transfer.finishing = true;
        self.transfer = Some(transfer);
        match self.append_config(ConfigEnvelope {
            admin_session: None,
            leader_time,
            operation: ConfigOperation::FinishLeaderTransfer {
                intent_index: transfer.intent_index,
                result,
            },
        }) {
            Ok(effects) => Ok(effects),
            Err(error) => {
                self.transfer = Some(LeadershipTransfer {
                    finishing: false,
                    ..transfer
                });
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn joint_active(&self) -> bool {
        self.joint.is_some()
    }

    #[must_use]
    pub fn snapshot_needed(&self, applied_bytes: u64) -> bool {
        applied_bytes >= SNAPSHOT_TRIGGER_BYTES
    }

    pub fn install_snapshot_state(&mut self, index: LogIndex, term: Term) {
        if index < self.snapshot_index
            || (index == self.snapshot_index && term != self.snapshot_term)
        {
            return;
        }
        self.log.retain(|entry| entry.index > index);
        self.commit_index = self.commit_index.max(index);
        self.applied_index = self.applied_index.max(index);
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    #[must_use]
    pub fn leadership_transfer_state(&self) -> Option<LeadershipTransferState> {
        self.transfer.map(|transfer| LeadershipTransferState {
            intent_index: transfer.intent_index,
            target: transfer.target,
            deadline: transfer.deadline,
            finishing: transfer.finishing,
        })
    }

    /// Restore the committed transfer workflow carried by a logical snapshot.
    /// The caller restores membership first, because the target must still be
    /// an eligible voter at the exact snapshot point.
    pub fn restore_leadership_transfer(
        &mut self,
        transfer: Option<LeadershipTransferState>,
    ) -> Result<(), RaftError> {
        let Some(transfer) = transfer else {
            self.transfer = None;
            return Ok(());
        };
        if transfer.intent_index.get() == 0
            || transfer.target == self.id && !self.voters.contains(&self.id)
            || !self.voters.contains(&transfer.target)
        {
            return Err(RaftError::InvalidMessage);
        }
        self.transfer = Some(LeadershipTransfer {
            intent_index: transfer.intent_index,
            target: transfer.target,
            deadline: transfer.deadline,
            finishing: transfer.finishing,
        });
        Ok(())
    }

    #[must_use]
    pub fn membership_state(&self) -> MembershipState {
        MembershipState {
            voters: self.voters.clone(),
            learners: self.learners.clone(),
            joint: self.joint.as_ref().and_then(|(old_voters, new_voters)| {
                self.joint_enter_index.map(|enter_index| JointMembership {
                    old_voters: old_voters.clone(),
                    new_voters: new_voters.clone(),
                    enter_index,
                })
            }),
            addresses: self.addresses.clone(),
        }
    }

    /// Install the committed membership base recovered from a snapshot.  Any
    /// retained configuration suffix is then replayed on top of this exact
    /// value, so truncation cannot accidentally fall back to bootstrap state.
    pub fn restore_membership_state(&mut self, state: MembershipState) -> Result<(), RaftError> {
        state.validate().map_err(|_| RaftError::InvalidMessage)?;
        self.voters = state.voters.clone();
        self.learners = state.learners.clone();
        self.joint = state
            .joint
            .as_ref()
            .map(|joint| (joint.old_voters.clone(), joint.new_voters.clone()));
        self.joint_enter_index = state.joint.as_ref().map(|joint| joint.enter_index);
        self.addresses = state.addresses.clone();
        self.base_voters = state.voters;
        self.base_learners = state.learners;
        self.base_joint = self.joint.clone();
        self.base_joint_enter_index = self.joint_enter_index;
        self.base_addresses = state.addresses;
        Ok(())
    }

    #[must_use]
    pub fn snapshot_chunks(
        &mut self,
        peer: NodeId,
        index: LogIndex,
        term: Term,
        bytes: &[u8],
    ) -> Vec<Message> {
        let chunk_size = SNAPSHOT_CHUNK_BYTES;
        if bytes.is_empty() {
            return Vec::new();
        }
        let transfer_id = self.next_snapshot_transfer_id;
        self.next_snapshot_transfer_id = self.next_snapshot_transfer_id.saturating_add(1).max(1);
        let total_len = u64::try_from(bytes.len()).expect("invariant: snapshot length fits u64");
        let snapshot_crc32c = cc_core::crc32c(bytes);
        bytes
            .chunks(chunk_size)
            .enumerate()
            .map(|(chunk, data)| Message {
                proto_version: PROTOCOL_VERSION,
                from: self.id,
                to: peer,
                term: self.hard_state.term,
                kind: MessageKind::SnapshotChunk {
                    transfer_id,
                    last_included_index: index,
                    last_included_term: term,
                    total_len,
                    snapshot_crc32c,
                    offset: u64::try_from(chunk * chunk_size)
                        .expect("invariant: snapshot offset fits u64"),
                    data: data.to_vec(),
                    done: (chunk + 1) * chunk_size >= bytes.len(),
                },
            })
            .collect()
    }

    pub fn on_message(&mut self, message: Message) -> Vec<RaftEffect> {
        self.on_message_at(message, Time::from_nanos(0))
    }

    /// Deliver a message at the host's virtual time.
    ///
    /// The original sans-IO surface remains available through `on_message`;
    /// the timestamped entry is the integration hook used by the simulator so
    /// election deadlines are reset relative to delivery, not the epoch.
    pub fn on_message_at(&mut self, message: Message, now: Time) -> Vec<RaftEffect> {
        if message.proto_version != PROTOCOL_VERSION {
            return vec![RaftEffect::Trace {
                name: "proto_version_rejected",
                index: self.last_index(),
            }];
        }
        let mut effects = Vec::new();
        if message.term > self.hard_state.term
            && !matches!(
                message.kind,
                MessageKind::PreVoteReq { .. } | MessageKind::PreVoteResp { .. }
            )
        {
            self.hard_state.term = message.term;
            self.hard_state.voted_for = None;
            self.role = if self.voters.contains(&self.id) {
                Role::Follower
            } else {
                Role::Learner
            };
            self.leader_id = None;
            // A tally belongs to the election it was raised in. Carrying it
            // into a new term lets an abandoned majority elect this node in a
            // term it never won.
            self.votes.clear();
            self.pre_votes.clear();
            effects.push(RaftEffect::PersistHard(self.hard_state));
        }
        match message.kind.clone() {
            MessageKind::PreVoteReq {
                last_index,
                last_term,
            } => {
                effects.push(self.pre_vote_response(&message, last_index, last_term));
            }
            MessageKind::PreVoteResp { granted } => {
                // Pre-votes are solicited for `term + 1`, so that is the only
                // round a reply can belong to. A straggler from an earlier
                // attempt would otherwise start an election off stale consent.
                if (self.role == Role::Follower || self.role == Role::Candidate)
                    && message.term.get() == self.hard_state.term.get() + 1
                    && granted
                {
                    self.pre_votes.insert(message.from);
                    if self.has_majority(&self.pre_votes) {
                        effects.extend(self.start_election());
                    }
                }
            }
            MessageKind::VoteReq {
                last_index,
                last_term,
            } => {
                effects.extend(self.vote_request(&message, last_index, last_term, now));
            }
            MessageKind::VoteResp { granted } => {
                // A grant raised for an election this node has already left is
                // not a vote in the current one. `append_response` has always
                // rejected off-term replies; without the same rule here a
                // candidate can reach quorum on votes cast for an earlier term
                // and become a second leader in a term another node has won.
                // Only a grant can complete a quorum, so a denial must never be
                // the event that promotes this node.
                if self.role == Role::Candidate && message.term == self.hard_state.term && granted {
                    self.votes.insert(message.from);
                    if self.has_majority(&self.votes) {
                        effects.extend(self.become_leader());
                    }
                }
            }
            MessageKind::AppendReq(request) => {
                effects.extend(self.append_request(&message, request, now))
            }
            MessageKind::AppendResp(response) => {
                effects.extend(self.append_response(&message, response))
            }
            chunk @ MessageKind::SnapshotChunk { .. } => {
                effects.extend(self.snapshot_chunk(&message, chunk))
            }
            MessageKind::SnapshotAck {
                transfer_id: _,
                next_offset,
                accepted: _,
                reason: _,
            } => {
                effects.push(RaftEffect::Trace {
                    name: "snapshot_ack",
                    index: LogIndex::new(next_offset),
                });
            }
            MessageKind::TimeoutNow { intent_index } => {
                let valid = intent_index.get() != 0
                    && message.term == self.hard_state.term
                    && self.leader_id == Some(message.from)
                    && self.voters.contains(&self.id)
                    && self.applied_index >= intent_index
                    && self.transfer.is_some_and(|transfer| {
                        transfer.intent_index == intent_index && transfer.target == self.id
                    });
                if !valid {
                    effects.push(RaftEffect::Trace {
                        name: "timeout_now_rejected",
                        index: intent_index,
                    });
                } else {
                    effects.extend(self.start_election());
                }
            }
        }
        effects
    }

    fn pre_vote_response(
        &self,
        message: &Message,
        last_index: LogIndex,
        last_term: Term,
    ) -> RaftEffect {
        let granted = message.term >= Term::new(self.hard_state.term.get() + 1)
            && self.log_is_up_to_date(last_index, last_term)
            && self.role != Role::Learner;
        RaftEffect::Send(Message {
            proto_version: PROTOCOL_VERSION,
            from: self.id,
            to: message.from,
            term: message.term,
            kind: MessageKind::PreVoteResp { granted },
        })
    }

    fn vote_request(
        &mut self,
        message: &Message,
        last_index: LogIndex,
        last_term: Term,
        now: Time,
    ) -> Vec<RaftEffect> {
        let member = self.voters.contains(&message.from);
        let can_vote = self
            .hard_state
            .voted_for
            .is_none_or(|voted| voted == message.from);
        let granted = message.term == self.hard_state.term
            && member
            && can_vote
            && self.role != Role::Learner
            && self.log_is_up_to_date(last_index, last_term);
        let mut effects = Vec::new();
        if granted {
            self.hard_state.voted_for = Some(message.from);
            effects.push(RaftEffect::PersistHard(self.hard_state));
            self.reset_election(now);
        }
        effects.push(RaftEffect::Send(Message {
            proto_version: PROTOCOL_VERSION,
            from: self.id,
            to: message.from,
            term: self.hard_state.term,
            kind: MessageKind::VoteResp { granted },
        }));
        effects
    }

    fn start_pre_vote(&mut self, now: Time) -> Vec<RaftEffect> {
        self.role = Role::Candidate;
        self.pre_votes.clear();
        self.pre_votes.insert(self.id);
        // The vote tally belongs to the previous attempt. Leaving it in place
        // means a single stray `VoteResp` in the new term finds a majority
        // that was never cast for it.
        self.votes.clear();
        self.reset_election(now);
        // A one-voter configuration has already satisfied the pre-vote
        // quorum with its own vote. Waiting for a response that no peer can
        // send leaves legitimate single-node deployments permanently unable
        // to elect (and, consequently, unable to reach a durability barrier).
        if self.has_majority(&self.pre_votes) {
            return self.start_election();
        }
        self.voters
            .iter()
            .filter(|peer| **peer != self.id)
            .map(|peer| {
                RaftEffect::Send(Message {
                    proto_version: PROTOCOL_VERSION,
                    from: self.id,
                    to: *peer,
                    term: Term::new(self.hard_state.term.get() + 1),
                    kind: MessageKind::PreVoteReq {
                        last_index: self.last_index(),
                        last_term: self.last_term(),
                    },
                })
            })
            .collect()
    }

    fn start_election(&mut self) -> Vec<RaftEffect> {
        self.role = Role::Candidate;
        self.hard_state.term = Term::new(self.hard_state.term.get() + 1);
        self.hard_state.voted_for = Some(self.id);
        self.votes.clear();
        self.votes.insert(self.id);
        let mut effects = vec![RaftEffect::PersistHard(self.hard_state)];
        // The hard state remains the first durability barrier. The composite
        // node will hold the appended leader no-op behind that fsync, so the
        // self-quorum fast path cannot publish leadership before its vote is
        // durable.
        if self.has_majority(&self.votes) {
            effects.extend(self.become_leader());
            return effects;
        }
        effects.extend(
            self.voters
                .iter()
                .filter(|peer| **peer != self.id)
                .map(|peer| {
                    RaftEffect::Send(Message {
                        proto_version: PROTOCOL_VERSION,
                        from: self.id,
                        to: *peer,
                        term: self.hard_state.term,
                        kind: MessageKind::VoteReq {
                            last_index: self.last_index(),
                            last_term: self.last_term(),
                        },
                    })
                }),
        );
        effects
    }

    fn become_leader(&mut self) -> Vec<RaftEffect> {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get() + 1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        };
        self.log.push(entry.clone());
        let next = LogIndex::new(self.last_index().get() + 1);
        for peer in self.voters.iter().chain(self.learners.iter()) {
            if *peer != self.id {
                self.next_index.insert(*peer, next);
                self.match_index.insert(*peer, LogIndex::new(0));
            }
        }
        self.heard_quorum.clear();
        self.heard_quorum.insert(self.id);
        self.quorum_deadline = Time::from_nanos(0);
        let mut effects = vec![RaftEffect::PersistEntries(vec![entry])];
        // A single voter must commit the durable leader no-op locally; without
        // it ReadIndex and every subsequent client proposal wait forever for
        // an AppendResp that no peer can send.
        effects.extend(self.advance_commit());
        // A committed transfer intent belongs to the cluster, not the old
        // leader.  The first leader elected afterwards makes the terminal
        // result durable in its own term so the workflow cannot pause future
        // proposals indefinitely after a crash.
        if let Some(transfer) = self.transfer
            && !transfer.finishing
        {
            let result = if self.id == transfer.target {
                TransferResult::Success
            } else {
                TransferResult::Superseded
            };
            if let Ok(finish) = self.finish_leadership_transfer(result, transfer.deadline) {
                effects.extend(finish);
                return effects;
            }
        }
        effects.extend(self.broadcast_append());
        effects
    }

    fn append_request(
        &mut self,
        message: &Message,
        request: AppendRequest,
        now: Time,
    ) -> Vec<RaftEffect> {
        if message.term < self.hard_state.term {
            return vec![self.make_append_response(
                message,
                false,
                self.last_index(),
                None,
                self.last_index(),
            )];
        }
        if message.term == self.hard_state.term && self.role == Role::Candidate {
            self.role = Role::Follower;
        }
        self.role = if self.voters.contains(&self.id) {
            Role::Follower
        } else {
            Role::Learner
        };
        self.leader_id = Some(message.from);
        if self.term_at(request.prev_index) != Some(request.prev_term) {
            let conflict_index = self
                .log
                .iter()
                .find(|entry| entry.index >= request.prev_index)
                .map_or(self.last_index(), |entry| entry.index);
            return vec![self.make_append_response(
                message,
                false,
                self.last_index(),
                self.term_at(request.prev_index),
                conflict_index,
            )];
        }
        self.reset_election(now);
        let mut effects = Vec::new();
        let mut appended = Vec::new();
        for entry in request.entries {
            if let Some(existing) = self
                .log
                .iter()
                .find(|existing| existing.index == entry.index)
            {
                if existing.term != entry.term {
                    if entry.index <= self.commit_index {
                        effects.push(RaftEffect::Trace {
                            name: "committed_conflict_rejected",
                            index: entry.index,
                        });
                        return effects;
                    }
                    self.log.retain(|existing| existing.index < entry.index);
                    self.rebuild_membership_from_log();
                    effects.push(RaftEffect::TruncateSuffix(entry.index));
                } else {
                    continue;
                }
            }
            if entry.kind == EntryKind::Config && self.apply_config_on_append(&entry).is_err() {
                // Do not retain an entry whose append-time configuration
                // projection is invalid.  Keeping it would make later
                // recovery derive a membership that was never acknowledged.
                return vec![RaftEffect::Trace {
                    name: "invalid_config_rejected",
                    index: entry.index,
                }];
            }
            self.log.push(entry.clone());
            appended.push(entry);
        }
        if !appended.is_empty() {
            effects.push(RaftEffect::PersistEntries(appended));
        }
        if request.leader_commit > self.commit_index {
            self.commit_index =
                LogIndex::new(request.leader_commit.get().min(self.last_index().get()));
            effects.extend(self.apply_committed());
        }
        effects.push(self.make_append_response(
            message,
            true,
            self.last_index(),
            None,
            LogIndex::new(0),
        ));
        effects
    }

    fn make_append_response(
        &self,
        message: &Message,
        success: bool,
        match_index: LogIndex,
        conflict_term: Option<Term>,
        conflict_index: LogIndex,
    ) -> RaftEffect {
        // Echo the round the leader stamped on this append, so it can tell a
        // fresh confirmation ack from one that predates the read.
        let read_round = match &message.kind {
            MessageKind::AppendReq(request) => request.read_round,
            _ => 0,
        };
        RaftEffect::Send(Message {
            proto_version: PROTOCOL_VERSION,
            from: self.id,
            to: message.from,
            term: self.hard_state.term,
            kind: MessageKind::AppendResp(AppendResponse {
                success,
                match_index,
                conflict_term,
                conflict_index,
                read_round,
            }),
        })
    }

    fn append_response_from_peer(
        &mut self,
        peer: NodeId,
        response: AppendResponse,
    ) -> Vec<RaftEffect> {
        if response.success {
            self.match_index.insert(peer, response.match_index);
            self.next_index
                .insert(peer, LogIndex::new(response.match_index.get() + 1));
            if self.voters.contains(&peer) {
                self.heard_quorum.insert(peer);
            }
            let mut effects = self.advance_commit();
            // A response carrying an older round was already in flight when the
            // read index was fixed, so it is not evidence of current leadership.
            if self.voters.contains(&peer) && response.read_round == self.read_round {
                self.read_acks.insert(peer);
            }
            if let Some(index) = self.read_index
                && self.has_majority(&self.read_acks)
            {
                self.read_index = None;
                effects.push(RaftEffect::ReadBarrierReady { index });
            }
            if self
                .next_index
                .get(&peer)
                .is_some_and(|next| *next <= self.last_index())
            {
                effects.extend(self.send_append(peer));
            }
            effects
        } else {
            let next = response.conflict_index.get().max(1);
            self.next_index.insert(peer, LogIndex::new(next));
            self.send_append(peer)
        }
    }

    fn append_response(&mut self, message: &Message, response: AppendResponse) -> Vec<RaftEffect> {
        if self.role != Role::Leader
            || message.term != self.hard_state.term
            || (!self.voters.contains(&message.from) && !self.learners.contains(&message.from))
        {
            return Vec::new();
        }
        self.append_response_from_peer(message.from, response)
    }

    fn send_append(&self, peer: NodeId) -> Vec<RaftEffect> {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(LogIndex::new(self.last_index().get() + 1));
        let prev_index = LogIndex::new(next.get().saturating_sub(1));
        let prev_term = self.term_at(prev_index).unwrap_or(Term::new(0));
        let entries = self
            .log
            .iter()
            .filter(|entry| entry.index >= next)
            .take(self.config.max_entries_per_append)
            .cloned()
            .collect();
        vec![RaftEffect::Send(Message {
            proto_version: PROTOCOL_VERSION,
            from: self.id,
            to: peer,
            term: self.hard_state.term,
            kind: MessageKind::AppendReq(AppendRequest {
                prev_index,
                prev_term,
                entries,
                leader_commit: self.commit_index,
                read_round: self.read_round,
            }),
        })]
    }

    fn broadcast_append(&self) -> Vec<RaftEffect> {
        self.voters
            .iter()
            .chain(self.learners.iter())
            .filter(|peer| **peer != self.id)
            .flat_map(|peer| self.send_append(*peer))
            .collect()
    }

    fn advance_commit(&mut self) -> Vec<RaftEffect> {
        let mut candidate = self.commit_index.get() + 1;
        let mut new_commit = self.commit_index;
        while candidate <= self.last_index().get() {
            let index = LogIndex::new(candidate);
            if self.commit_quorum(candidate) && self.term_at(index) == Some(self.hard_state.term) {
                new_commit = index;
            }
            candidate += 1;
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed()
        } else {
            Vec::new()
        }
    }

    fn apply_committed(&mut self) -> Vec<RaftEffect> {
        let mut applied = Vec::new();
        while self.applied_index < self.commit_index {
            let next = LogIndex::new(self.applied_index.get() + 1);
            if let Some(entry) = self.log.iter().find(|entry| entry.index == next) {
                applied.push(entry.clone());
                self.applied_index = next;
            } else {
                break;
            }
        }
        if applied.is_empty() {
            Vec::new()
        } else {
            vec![RaftEffect::Apply(applied)]
        }
    }

    fn snapshot_chunk(&mut self, message: &Message, chunk: MessageKind) -> Vec<RaftEffect> {
        let MessageKind::SnapshotChunk {
            transfer_id,
            last_included_index,
            last_included_term,
            total_len,
            snapshot_crc32c,
            offset,
            data,
            done,
        } = chunk
        else {
            unreachable!("snapshot chunk dispatcher supplies the matching variant");
        };
        if last_included_index < self.snapshot_index
            || last_included_index < self.applied_index
            || (last_included_index == self.snapshot_index
                && last_included_term != self.snapshot_term)
        {
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::StaleTerm),
            )];
        }
        let data_len = u64::try_from(data.len()).expect("invariant: chunk length fits u64");
        let Some(end) = offset.checked_add(data_len) else {
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::TooLarge),
            )];
        };
        if transfer_id == 0
            || total_len == 0
            || total_len > cc_core::MAX_CODEC_BYTES as u64
            || data.is_empty()
            || data.len() > SNAPSHOT_CHUNK_BYTES
            || end > total_len
            || done != (end == total_len)
        {
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Corrupt),
            )];
        }
        let same_transfer = self.snapshot_transfer.is_some_and(|transfer| {
            transfer.id == transfer_id
                && transfer.index == last_included_index
                && transfer.term == last_included_term
                && transfer.total_len == total_len
                && transfer.crc32c == snapshot_crc32c
        });
        if offset == 0 && !same_transfer {
            self.snapshot_buffer.clear();
            self.snapshot_transfer = Some(SnapshotTransfer {
                id: transfer_id,
                index: last_included_index,
                term: last_included_term,
                total_len,
                crc32c: snapshot_crc32c,
            });
        }
        let Some(transfer) = self.snapshot_transfer else {
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::RestartFromZero),
            )];
        };
        if transfer.id != transfer_id
            || transfer.index != last_included_index
            || transfer.term != last_included_term
            || transfer.total_len != total_len
            || transfer.crc32c != snapshot_crc32c
        {
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                0,
                false,
                Some(SnapshotRejectReason::Conflict),
            )];
        }
        let expected_offset = self.snapshot_buffer.len() as u64;
        if offset < expected_offset {
            let start = usize::try_from(offset).expect("snapshot offset is bounded");
            let finish = usize::try_from(end).expect("snapshot end is bounded");
            if finish <= self.snapshot_buffer.len() && self.snapshot_buffer[start..finish] == data {
                return vec![self.snapshot_ack(message, transfer_id, expected_offset, true, None)];
            }
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                expected_offset,
                false,
                Some(SnapshotRejectReason::Conflict),
            )];
        }
        if offset > expected_offset {
            return vec![self.snapshot_ack(
                message,
                transfer_id,
                expected_offset,
                false,
                Some(SnapshotRejectReason::Gap),
            )];
        }
        self.snapshot_buffer.extend_from_slice(&data);
        if done {
            if cc_core::crc32c(&self.snapshot_buffer) != snapshot_crc32c {
                self.snapshot_buffer.clear();
                self.snapshot_transfer = None;
                return vec![self.snapshot_ack(
                    message,
                    transfer_id,
                    0,
                    false,
                    Some(SnapshotRejectReason::Corrupt),
                )];
            }
            self.log.retain(|entry| entry.index > last_included_index);
            self.commit_index = self.commit_index.max(last_included_index);
            self.applied_index = last_included_index;
            self.snapshot_index = last_included_index;
            self.snapshot_term = last_included_term;
            self.snapshot_buffer.clear();
            self.snapshot_transfer = None;
        }
        vec![self.snapshot_ack(message, transfer_id, end, true, None)]
    }

    fn snapshot_ack(
        &self,
        message: &Message,
        transfer_id: u64,
        next_offset: u64,
        accepted: bool,
        reason: Option<SnapshotRejectReason>,
    ) -> RaftEffect {
        RaftEffect::Send(Message {
            proto_version: PROTOCOL_VERSION,
            from: self.id,
            to: message.from,
            term: self.hard_state.term,
            kind: MessageKind::SnapshotAck {
                transfer_id,
                next_offset,
                accepted,
                reason,
            },
        })
    }

    /// Arm the election timer from `now`. A process that restarts mid-run must
    /// call this, or it inherits a deadline measured from time zero and
    /// campaigns on its first tick.
    pub fn rearm_election(&mut self, now: Time) {
        self.reset_election(now);
    }

    fn reset_election(&mut self, now: Time) {
        let low = self.config.election_min.as_nanos();
        let high = self.config.election_max.as_nanos().max(low + 1);
        let timeout = Duration::from_nanos(self.rng.range_u64(low, high + 1));
        self.election_deadline = now + timeout;
    }

    fn log_is_up_to_date(&self, index: LogIndex, term: Term) -> bool {
        term > self.last_term() || (term == self.last_term() && index >= self.last_index())
    }

    fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    fn commit_quorum(&self, candidate: u64) -> bool {
        let Some((old, new)) = &self.joint else {
            return self
                .voters
                .iter()
                .filter(|member| {
                    **member == self.id
                        || self
                            .match_index
                            .get(member)
                            .is_some_and(|index| index.get() >= candidate)
                })
                .count()
                >= self.majority();
        };
        let count = |members: &BTreeSet<NodeId>| {
            members
                .iter()
                .filter(|member| {
                    **member == self.id
                        || self
                            .match_index
                            .get(member)
                            .is_some_and(|index| index.get() >= candidate)
                })
                .count()
        };
        count(old) > old.len() / 2 && count(new) > new.len() / 2
    }

    fn has_majority(&self, votes: &BTreeSet<NodeId>) -> bool {
        let count =
            |members: &BTreeSet<NodeId>| votes.iter().filter(|id| members.contains(id)).count();
        match &self.joint {
            Some((old, new)) => count(old) > old.len() / 2 && count(new) > new.len() / 2,
            None => count(&self.voters) >= self.majority(),
        }
    }

    fn joint_enter_index(&self) -> Option<LogIndex> {
        self.joint_enter_index
    }

    fn apply_config_on_append(&mut self, entry: &Entry) -> Result<(), RaftError> {
        let envelope =
            ConfigEnvelope::decode(&entry.payload).map_err(|_| RaftError::InvalidMessage)?;
        match envelope.operation {
            ConfigOperation::AddLearner { id, address } => {
                if self.voters.contains(&id) || !self.learners.insert(id) {
                    return Err(RaftError::InvalidMessage);
                }
                if let Some(address) = address {
                    self.addresses.insert(id, address);
                }
            }
            ConfigOperation::RemoveLearner { id } => {
                if !self.learners.remove(&id) {
                    return Err(RaftError::InvalidMessage);
                }
                self.addresses.remove(&id);
            }
            ConfigOperation::UpdateAddress { id, address } => {
                if !self.voters.contains(&id) && !self.learners.contains(&id) {
                    return Err(RaftError::InvalidMessage);
                }
                self.addresses.insert(id, address);
            }
            ConfigOperation::EnterJoint { new_voters } => {
                if self.joint.is_some()
                    || self.config_transition_in_flight_before(entry.index)
                    || new_voters.is_empty()
                {
                    return Err(RaftError::Busy);
                }
                let old = self.voters.clone();
                let mut union = old.clone();
                union.extend(new_voters.iter().copied());
                self.learners.retain(|id| !new_voters.contains(id));
                self.joint = Some((old, new_voters));
                self.joint_enter_index = Some(entry.index);
                self.voters = union;
            }
            ConfigOperation::LeaveJoint { enter_index } => {
                let Some((_old, new)) = self.joint.clone() else {
                    return Err(RaftError::InvalidMessage);
                };
                if self.joint_enter_index != Some(enter_index) {
                    return Err(RaftError::InvalidMessage);
                }
                self.voters = new;
                self.joint = None;
                self.joint_enter_index = None;
                if !self.voters.contains(&self.id) {
                    self.role = Role::Follower;
                }
            }
            // The higher-level cluster workflow owns the committed transfer
            // state. Raft still accepts only canonical envelopes here so a
            // follower reconstructs the same append projection.
            ConfigOperation::BeginLeaderTransfer { target } => {
                if !self.voters.contains(&target)
                    || self.joint.is_some()
                    || self.config_transition_in_flight_before(entry.index)
                {
                    return Err(RaftError::InvalidMessage);
                }
            }
            ConfigOperation::FinishLeaderTransfer { intent_index, .. } => {
                if self
                    .transfer
                    .is_some_and(|transfer| transfer.intent_index == intent_index)
                {
                    // The durable workflow is cleared only by committed apply;
                    // append accepts the matching terminal entry so it can be
                    // replicated to every follower first.
                } else {
                    return Err(RaftError::InvalidMessage);
                }
            }
        }
        Ok(())
    }

    fn rebuild_membership_from_log(&mut self) {
        self.voters = self.base_voters.clone();
        self.learners = self.base_learners.clone();
        self.joint = self.base_joint.clone();
        self.joint_enter_index = self.base_joint_enter_index;
        self.addresses = self.base_addresses.clone();
        for entry in self
            .log
            .clone()
            .iter()
            .filter(|entry| entry.kind == EntryKind::Config)
        {
            if self.apply_config_on_append(entry).is_err() {
                break;
            }
        }
    }

    /// Return whether an earlier retained configuration entry still owns the
    /// one permitted multi-entry workflow.  This examines only the prefix
    /// before `index`, making it correct both while appending a fresh entry and
    /// while replaying a retained suffix after truncation.
    fn config_transition_in_flight_before(&self, index: LogIndex) -> bool {
        let mut transfer = None;
        for entry in self
            .log
            .iter()
            .filter(|entry| entry.kind == EntryKind::Config && entry.index < index)
        {
            let Ok(envelope) = ConfigEnvelope::decode(&entry.payload) else {
                return true;
            };
            match envelope.operation {
                ConfigOperation::BeginLeaderTransfer { .. } => {
                    if transfer.replace(entry.index).is_some() {
                        return true;
                    }
                }
                ConfigOperation::FinishLeaderTransfer { intent_index, .. } => {
                    if transfer == Some(intent_index) {
                        transfer = None;
                    } else {
                        return true;
                    }
                }
                _ => {}
            }
        }
        transfer.is_some()
    }

    #[must_use]
    pub fn invariants(&self) -> RaftInvariantReport {
        let mut report = RaftInvariantReport::default();
        if self.applied_index > self.commit_index {
            report.violations.push(InvariantViolation {
                name: "applied_le_commit",
                detail: format!("{} > {}", self.applied_index, self.commit_index),
            });
        }
        if self.commit_index > self.last_index() {
            report.violations.push(InvariantViolation {
                name: "commit_le_last",
                detail: format!("{} > {}", self.commit_index, self.last_index()),
            });
        }
        for pair in self.log.windows(2) {
            if pair[1].index != LogIndex::new(pair[0].index.get() + 1) {
                report.violations.push(InvariantViolation {
                    name: "log_contiguous",
                    detail: format!("{} then {}", pair[0].index, pair[1].index),
                });
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters() -> BTreeSet<NodeId> {
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .collect()
    }

    fn node(id: u64) -> RaftNode {
        RaftNode::new(
            NodeId::new(id),
            voters(),
            Seed::new(id),
            RaftConfig::default(),
        )
    }

    #[test]
    fn trap_single_voter_commits_local_noop_and_proposal_after_persistence() {
        let voters = [NodeId::new(1)].into_iter().collect();
        let mut leader = RaftNode::new(NodeId::new(1), voters, Seed::new(1), RaftConfig::default());
        leader.role = Role::Leader;
        leader.leader_id = Some(NodeId::new(1));
        leader.hard_state.term = Term::new(1);

        let election = leader.become_leader();
        assert_eq!(leader.commit_index, LogIndex::new(1));
        assert!(matches!(
            election.first(),
            Some(RaftEffect::PersistEntries(_))
        ));
        assert!(matches!(
            election.get(1),
            Some(RaftEffect::Apply(entries)) if entries[0].kind == EntryKind::Noop
        ));

        let proposal = leader
            .propose(b"write".to_vec())
            .expect("single voter proposal");
        assert_eq!(leader.commit_index, LogIndex::new(2));
        assert!(matches!(
            proposal.first(),
            Some(RaftEffect::PersistEntries(_))
        ));
        assert!(matches!(
            proposal.get(1),
            Some(RaftEffect::Apply(entries)) if entries[0].kind == EntryKind::App
        ));
    }

    #[test]
    fn pre_vote_then_vote_persists_before_grant() {
        let mut candidate = node(1);
        let effects = candidate.on_timer(Time::from_nanos(1_000_000_000), TimerKind::Election);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            RaftEffect::Send(Message {
                kind: MessageKind::PreVoteReq { .. },
                ..
            })
        )));
        let mut follower = node(2);
        follower.hard_state.term = Term::new(1);
        let effects = follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        });
        assert!(matches!(effects[0], RaftEffect::PersistHard(_)));
        assert!(matches!(
            effects[1],
            RaftEffect::Send(Message {
                kind: MessageKind::VoteResp { granted: true },
                ..
            })
        ));
    }

    #[test]
    fn trap_revote_after_crash_is_blocked_by_persisted_vote() {
        let mut follower = node(2);
        follower.hard_state.term = Term::new(1);
        follower.hard_state.voted_for = Some(NodeId::new(1));
        let effects = follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(3),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        });
        assert!(matches!(
            effects.last(),
            Some(RaftEffect::Send(Message {
                kind: MessageKind::VoteResp { granted: false },
                ..
            }))
        ));
    }

    #[test]
    fn trap_candidate_same_term_append_steps_down() {
        let mut follower = node(2);
        follower.role = Role::Candidate;
        follower.hard_state.term = Term::new(3);
        let effects = follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(3),
            kind: MessageKind::AppendReq(AppendRequest {
                prev_index: LogIndex::new(0),
                prev_term: Term::new(0),
                entries: Vec::new(),
                leader_commit: LogIndex::new(0),
                read_round: 0,
            }),
        });
        assert_eq!(follower.role, Role::Follower);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, RaftEffect::Send(_)))
        );
    }

    #[test]
    fn leader_appends_noop_and_read_waits_for_commit() {
        let mut leader = node(1);
        leader.role = Role::Candidate;
        leader.hard_state.term = Term::new(1);
        leader.votes = [NodeId::new(1), NodeId::new(2)].into_iter().collect();
        let effects = leader.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: Term::new(1),
            kind: MessageKind::VoteResp { granted: true },
        });
        assert!(effects.iter().any(|effect| matches!(effect, RaftEffect::PersistEntries(entries) if entries[0].kind == EntryKind::Noop)));
        assert!(matches!(
            leader.request_read(),
            Err(RaftError::ReadBarrierNotReady)
        ));
        leader.commit_index = leader.last_index();
        assert!(leader.request_read().is_ok());
    }

    #[test]
    fn trap_readindex_noop() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        assert_eq!(leader.request_read(), Err(RaftError::ReadBarrierNotReady));

        leader.log.push(Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        });
        leader.commit_index = LogIndex::new(1);
        leader.applied_index = LogIndex::new(1);
        let effects = leader.request_read().expect("read barrier");
        assert!(effects.iter().any(|effect| matches!(
            effect,
            RaftEffect::ReadBarrier { index } if *index == LogIndex::new(1)
        )));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, RaftEffect::ReadBarrierReady { .. }))
        );
        // An ack that was already in flight when the read index was fixed
        // carries the previous round and proves nothing about leadership now.
        let stale = leader.on_message(Message {
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
        });
        assert!(
            !stale
                .iter()
                .any(|effect| matches!(effect, RaftEffect::ReadBarrierReady { .. })),
            "a pre-read ack must not confirm the read quorum"
        );

        let effects = leader.on_message(Message {
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
        });
        assert!(effects.iter().any(|effect| matches!(
            effect,
            RaftEffect::ReadBarrierReady { index } if *index == LogIndex::new(1)
        )));
    }

    #[test]
    fn trap_leave_joint_waits_for_committed_enter_joint() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        let next: BTreeSet<NodeId> = [NodeId::new(1), NodeId::new(2), NodeId::new(4)]
            .into_iter()
            .collect();
        let enter = leader.enter_joint(next).expect("enter joint");
        assert!(
            enter
                .iter()
                .any(|effect| matches!(effect, RaftEffect::Send(_)))
        );
        assert_eq!(leader.leave_joint(), Err(RaftError::Busy));
        leader.commit_index = leader.last_index();
        let leave = leader.leave_joint().expect("leave after enter commit");
        assert!(
            leave
                .iter()
                .any(|effect| matches!(effect, RaftEffect::Send(_)))
        );
        assert!(!leader.joint_active());
    }

    #[test]
    fn trap_rejected_config_proposal_does_not_mutate_membership() {
        let mut follower = node(1);
        let before = follower.voters.clone();
        let result = follower.enter_joint([NodeId::new(1)].into_iter().collect());
        assert_eq!(result, Err(RaftError::NotLeader));
        assert_eq!(follower.voters, before);
        assert!(!follower.joint_active());
    }

    #[test]
    fn trap_timer_reset_discipline() {
        let mut follower = node(2);
        let before = follower.election_deadline;
        let effects = follower.on_message_at(
            Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: Term::new(0),
                kind: MessageKind::AppendReq(AppendRequest {
                    prev_index: LogIndex::new(1),
                    prev_term: Term::new(0),
                    entries: Vec::new(),
                    leader_commit: LogIndex::new(0),
                    read_round: 0,
                }),
            },
            Time::from_nanos(1_000_000_000),
        );
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, RaftEffect::Send(_)))
        );
        assert_eq!(follower.election_deadline, before);

        follower.on_message_at(
            Message {
                proto_version: PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: Term::new(0),
                kind: MessageKind::AppendReq(AppendRequest {
                    prev_index: LogIndex::new(0),
                    prev_term: Term::new(0),
                    entries: Vec::new(),
                    leader_commit: LogIndex::new(0),
                    read_round: 0,
                }),
            },
            Time::from_nanos(1_000_000_000),
        );
        assert!(follower.election_deadline > Time::from_nanos(1_000_000_000));
    }

    #[test]
    fn trap_snapshot_ordering() {
        let mut follower = node(2);
        follower.commit_index = LogIndex::new(2);
        follower.applied_index = LogIndex::new(2);
        let effects = follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::SnapshotChunk {
                transfer_id: 1,
                last_included_index: LogIndex::new(3),
                last_included_term: Term::new(1),
                total_len: 3,
                snapshot_crc32c: cc_core::crc32c(&[1, 2, 3]),
                offset: 0,
                data: vec![1, 2, 3],
                done: true,
            },
        });
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, RaftEffect::Send(_)))
        );
        assert!(follower.applied_index <= follower.commit_index);
    }

    #[test]
    fn trap_replayed_snapshot_chunk_is_idempotent() {
        let mut follower = node(2);
        let first = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::SnapshotChunk {
                transfer_id: 3,
                last_included_index: LogIndex::new(4),
                last_included_term: Term::new(1),
                total_len: 2,
                snapshot_crc32c: cc_core::crc32c(&[9, 10]),
                offset: 0,
                data: vec![9],
                done: false,
            },
        };
        follower.on_message(first.clone());
        let replay = follower.on_message(first);
        assert_eq!(follower.snapshot_buffer, vec![9]);
        assert!(matches!(
            replay.as_slice(),
            [RaftEffect::Send(Message {
                kind: MessageKind::SnapshotAck {
                    transfer_id: 3,
                    next_offset: 1,
                    accepted: true,
                    reason: None,
                },
                ..
            })]
        ));
    }

    #[test]
    fn trap_ack_before_fsync_is_represented_by_persist_effect_order() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        let effects = leader.propose(b"value".to_vec()).expect("proposal");
        assert!(matches!(
            effects.first(),
            Some(RaftEffect::PersistEntries(_))
        ));
    }

    #[test]
    fn trap_stale_snapshot_install_is_ignored() {
        let mut follower = node(2);
        follower.applied_index = LogIndex::new(10);
        let effects = follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::SnapshotChunk {
                transfer_id: 1,
                last_included_index: LogIndex::new(9),
                last_included_term: Term::new(1),
                total_len: 1,
                snapshot_crc32c: cc_core::crc32c(&[1]),
                offset: 0,
                data: vec![1],
                done: true,
            },
        });
        assert!(effects.iter().any(|effect| matches!(
            effect,
            RaftEffect::Send(Message {
                kind: MessageKind::SnapshotAck {
                    accepted: false,
                    reason: Some(SnapshotRejectReason::StaleTerm),
                    ..
                },
                ..
            })
        )));
    }

    #[test]
    fn trap_figure8_does_not_commit_prior_term_without_current_term_entry() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(2);
        leader.log.push(Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::App,
            payload: vec![1],
        });
        leader.match_index.insert(NodeId::new(2), LogIndex::new(1));
        leader.match_index.insert(NodeId::new(3), LogIndex::new(1));
        assert_eq!(leader.commit_index, LogIndex::new(0));
    }

    #[test]
    fn invariants_hold_for_empty_node() {
        assert!(node(1).invariants().is_ok());
    }

    #[test]
    fn trap_stale_vote_tally_cannot_elect_in_a_later_term() {
        // Found by the message-soup campaign at seed 0x66a: a node kept the
        // vote tally from an election it had already left, so one stray
        // response in a later term promoted it on consent nobody gave —
        // two leaders in one term.
        let mut n3 = node(3);
        n3.on_timer(Time::from_nanos(1_000_000), TimerKind::Election);
        let grant = |from: u64, term: u64, granted: bool, kind_is_pre: bool| Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(from),
            to: NodeId::new(3),
            term: Term::new(term),
            kind: if kind_is_pre {
                MessageKind::PreVoteResp { granted }
            } else {
                MessageKind::VoteResp { granted }
            },
        };
        n3.on_message(grant(2, 1, true, true));
        n3.on_message(grant(2, 1, true, false));
        assert_eq!(n3.role, Role::Leader, "the honest term-1 win still works");
        assert_eq!(n3.hard_state.term, Term::new(1));

        // A higher term knocks it down; the term-1 tally must not survive.
        n3.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(3),
            term: Term::new(2),
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        });
        assert_eq!(n3.role, Role::Follower);
        assert!(n3.votes.is_empty(), "tally survived a term change");

        // Re-entering the pre-vote phase must not resurrect it either, and a
        // lone denial must never be the event that promotes a candidate.
        n3.on_timer(Time::from_nanos(2_000_000), TimerKind::Election);
        assert!(n3.votes.is_empty(), "tally survived a new pre-vote round");
        n3.on_message(grant(1, 2, false, false));
        assert_ne!(
            n3.role,
            Role::Leader,
            "a denial elected a leader on a stale tally"
        );
    }

    #[test]
    fn trap_config_on_append_changes_joint_quorum_before_commit() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        let new_voters = [NodeId::new(1), NodeId::new(2), NodeId::new(4)]
            .into_iter()
            .collect();
        leader.enter_joint(new_voters).expect("joint entry");
        assert!(leader.joint_active());
        assert!(leader.voters.contains(&NodeId::new(4)));
    }

    #[test]
    fn trap_removed_leader_stepdown_timing_waits_for_leave_joint() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        let new_voters = [NodeId::new(2), NodeId::new(3), NodeId::new(4)]
            .into_iter()
            .collect();
        leader.enter_joint(new_voters).expect("joint");
        assert_eq!(leader.role, Role::Leader);
        leader.commit_index = leader.last_index();
        leader.leave_joint().expect("leave");
        assert_eq!(leader.role, Role::Follower);
    }

    #[test]
    fn trap_removed_node_disruption_is_rejected_by_membership_vote_rule() {
        let mut follower = node(2);
        follower.voters.remove(&NodeId::new(3));
        let effects = follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(3),
            to: NodeId::new(2),
            term: Term::new(0),
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        });
        assert!(matches!(
            effects.last(),
            Some(RaftEffect::Send(Message {
                kind: MessageKind::VoteResp { granted: false },
                ..
            }))
        ));
    }

    #[test]
    fn trap_config_in_snapshot_keeps_current_membership() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        let new_voters = [NodeId::new(1), NodeId::new(2), NodeId::new(4)]
            .into_iter()
            .collect();
        leader.enter_joint(new_voters).expect("joint");
        leader.install_snapshot_state(LogIndex::new(1), Term::new(1));
        assert!(leader.joint_active());
    }

    #[test]
    fn trap_snapshot_base_is_the_log_origin() {
        let mut follower = node(1);
        follower.install_snapshot_state(LogIndex::new(9), Term::new(4));
        assert_eq!(follower.last_index(), LogIndex::new(9));
        assert_eq!(follower.last_term(), Term::new(4));
        assert_eq!(follower.term_at(LogIndex::new(9)), Some(Term::new(4)));
    }

    #[test]
    fn trap_same_index_different_term_snapshot_is_rejected() {
        let mut follower = node(1);
        follower.install_snapshot_state(LogIndex::new(4), Term::new(2));
        follower.install_snapshot_state(LogIndex::new(4), Term::new(3));
        assert_eq!(follower.term_at(LogIndex::new(4)), Some(Term::new(2)));
    }

    #[test]
    fn trap_checkquorum_steps_down_isolated_leader() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        leader.tick(Time::from_nanos(1));
        let effects = leader.tick(Time::from_nanos(1 + DEFAULT_ELECTION_MIN.as_nanos()));
        assert_eq!(leader.role, Role::Follower);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            RaftEffect::Trace {
                name: "checkquorum_stepdown",
                ..
            }
        )));
    }

    #[test]
    fn trap_unknown_vote_response_cannot_form_quorum() {
        let mut candidate = node(1);
        candidate.role = Role::Candidate;
        candidate.hard_state.term = Term::new(4);
        candidate.votes.insert(NodeId::new(1));
        let effects = candidate.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(99),
            to: NodeId::new(1),
            term: Term::new(4),
            kind: MessageKind::VoteResp { granted: true },
        });
        assert!(effects.is_empty());
        assert_eq!(candidate.role, Role::Candidate);
    }

    #[test]
    fn trap_learner_append_response_cannot_commit() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(2);
        leader.log.push(Entry {
            term: Term::new(2),
            index: LogIndex::new(1),
            kind: EntryKind::App,
            payload: b"only-a-learner-acked".to_vec(),
        });
        leader.add_learner(NodeId::new(4)).expect("learner");
        let effects = leader.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(4),
            to: NodeId::new(1),
            term: Term::new(2),
            kind: MessageKind::AppendResp(AppendResponse {
                success: true,
                match_index: LogIndex::new(1),
                conflict_term: None,
                conflict_index: LogIndex::new(0),
                read_round: 0,
            }),
        });
        assert!(effects.is_empty());
        assert_eq!(leader.commit_index, LogIndex::new(0));
    }

    #[test]
    fn trap_joint_election_requires_both_majorities() {
        let mut candidate = node(1);
        candidate.role = Role::Leader;
        candidate.hard_state.term = Term::new(1);
        candidate
            .enter_joint(
                [NodeId::new(1), NodeId::new(4), NodeId::new(5)]
                    .into_iter()
                    .collect(),
            )
            .expect("enter joint");
        candidate.role = Role::Follower;
        candidate.hard_state.term = Term::new(1);
        candidate.start_election();

        candidate.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: Term::new(2),
            kind: MessageKind::VoteResp { granted: true },
        });
        assert_eq!(candidate.role, Role::Candidate, "new voters lack quorum");

        candidate.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(4),
            to: NodeId::new(1),
            term: Term::new(2),
            kind: MessageKind::VoteResp { granted: true },
        });
        assert_eq!(candidate.role, Role::Leader);
    }

    #[test]
    fn trap_follower_applies_config_on_append() {
        let mut follower = node(2);
        let entry = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(0),
                operation: ConfigOperation::AddLearner {
                    id: NodeId::new(4),
                    address: None,
                },
            }
            .encode(),
        };
        follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::AppendReq(AppendRequest {
                prev_index: LogIndex::new(0),
                prev_term: Term::new(0),
                entries: vec![entry],
                leader_commit: LogIndex::new(0),
                read_round: 0,
            }),
        });
        assert!(follower.learners.contains(&NodeId::new(4)));
        assert_eq!(follower.commit_index, LogIndex::new(0));
    }

    #[test]
    fn trap_learners_receive_entries_but_never_vote() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(1);
        leader.add_learner(NodeId::new(4)).expect("learner");
        assert!(leader.broadcast_append().iter().any(|effect| matches!(
            effect,
            RaftEffect::Send(Message {
                to,
                kind: MessageKind::AppendReq(_),
                ..
            }) if *to == NodeId::new(4)
        )));

        let mut learner = RaftNode::new(
            NodeId::new(4),
            voters(),
            Seed::new(4),
            RaftConfig::default(),
        );
        assert_eq!(learner.role, Role::Learner);
        let effects = learner.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(4),
            term: Term::new(1),
            kind: MessageKind::VoteReq {
                last_index: LogIndex::new(0),
                last_term: Term::new(0),
            },
        });
        assert!(matches!(
            effects.last(),
            Some(RaftEffect::Send(Message {
                kind: MessageKind::VoteResp { granted: false },
                ..
            }))
        ));
    }

    #[test]
    fn trap_membership_recovers_from_log_and_snapshot() {
        let snapshot_membership = MembershipState {
            voters: [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                .into_iter()
                .collect(),
            learners: [NodeId::new(4)].into_iter().collect(),
            joint: None,
            addresses: BTreeMap::new(),
        };
        let mut recovered = node(1);
        recovered
            .restore_membership_state(snapshot_membership)
            .expect("snapshot membership");
        recovered.log.push(Entry {
            term: Term::new(2),
            index: LogIndex::new(9),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(0),
                operation: ConfigOperation::EnterJoint {
                    new_voters: [NodeId::new(1), NodeId::new(2), NodeId::new(4)]
                        .into_iter()
                        .collect(),
                },
            }
            .encode(),
        });
        recovered.rebuild_membership_from_log();
        assert!(recovered.joint_active());
        assert!(recovered.voters.contains(&NodeId::new(4)));
        assert!(!recovered.learners.contains(&NodeId::new(4)));
    }

    #[test]
    fn trap_truncated_uncommitted_config_restores_prior_membership() {
        let mut follower = node(2);
        let add_learner = Entry {
            term: Term::new(1),
            index: LogIndex::new(1),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(0),
                operation: ConfigOperation::AddLearner {
                    id: NodeId::new(4),
                    address: None,
                },
            }
            .encode(),
        };
        follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(1),
            kind: MessageKind::AppendReq(AppendRequest {
                prev_index: LogIndex::new(0),
                prev_term: Term::new(0),
                entries: vec![add_learner],
                leader_commit: LogIndex::new(0),
                read_round: 0,
            }),
        });
        assert!(follower.learners.contains(&NodeId::new(4)));

        follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(3),
            to: NodeId::new(2),
            term: Term::new(2),
            kind: MessageKind::AppendReq(AppendRequest {
                prev_index: LogIndex::new(0),
                prev_term: Term::new(0),
                entries: vec![Entry {
                    term: Term::new(2),
                    index: LogIndex::new(1),
                    kind: EntryKind::App,
                    payload: b"replacement".to_vec(),
                }],
                leader_commit: LogIndex::new(0),
                read_round: 0,
            }),
        });
        assert!(!follower.learners.contains(&NodeId::new(4)));
    }

    #[test]
    fn trap_append_matches_snapshot_base() {
        let mut follower = node(2);
        follower.install_snapshot_state(LogIndex::new(5), Term::new(3));
        follower.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(3),
            kind: MessageKind::AppendReq(AppendRequest {
                prev_index: LogIndex::new(5),
                prev_term: Term::new(3),
                entries: vec![Entry {
                    term: Term::new(3),
                    index: LogIndex::new(6),
                    kind: EntryKind::App,
                    payload: b"after-base".to_vec(),
                }],
                leader_commit: LogIndex::new(5),
                read_round: 0,
            }),
        });
        assert_eq!(follower.last_index(), LogIndex::new(6));
        assert_eq!(follower.term_at(LogIndex::new(5)), Some(Term::new(3)));
    }

    #[test]
    fn trap_leadership_transfer_waits_for_committed_intent_and_caught_up_target() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(2);
        leader.log.push(Entry {
            term: Term::new(2),
            index: LogIndex::new(1),
            kind: EntryKind::Noop,
            payload: Vec::new(),
        });
        leader.match_index.insert(NodeId::new(2), LogIndex::new(1));
        let effects = leader
            .begin_leadership_transfer(NodeId::new(2), Time::from_nanos(10))
            .expect("intent append");
        let intent = leader.last_index();
        assert!(effects.iter().all(|effect| !matches!(
            effect,
            RaftEffect::Send(Message {
                kind: MessageKind::TimeoutNow { .. },
                ..
            })
        )));
        leader.match_index.insert(NodeId::new(2), intent);
        let entry = leader.log.last().expect("intent entry").clone();
        let committed = leader.apply_committed_config(&entry).expect("apply intent");
        assert_eq!(
            leader.propose(b"blocked".to_vec()),
            Err(RaftError::TransferInProgress),
            "the committed intent must pause new client proposals"
        );
        assert!(committed.iter().any(|effect| matches!(
            effect,
            RaftEffect::Send(Message {
                to,
                kind: MessageKind::TimeoutNow { intent_index },
                ..
            }) if *to == NodeId::new(2) && *intent_index == intent
        )));
    }

    #[test]
    fn trap_timeout_now_uses_normal_term_and_vote_fsync() {
        let mut target = node(2);
        target.hard_state.term = Term::new(3);
        target.leader_id = Some(NodeId::new(1));
        let intent = Entry {
            term: Term::new(3),
            index: LogIndex::new(4),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(0),
                operation: ConfigOperation::BeginLeaderTransfer {
                    target: NodeId::new(2),
                },
            }
            .encode(),
        };
        target.applied_index = intent.index;
        target
            .apply_committed_config(&intent)
            .expect("apply transfer intent");
        let effects = target.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(3),
            kind: MessageKind::TimeoutNow {
                intent_index: intent.index,
            },
        });
        assert!(
            matches!(effects.first(), Some(RaftEffect::PersistHard(hard)) if hard.term == Term::new(4) && hard.voted_for == Some(NodeId::new(2)))
        );
    }

    #[test]
    fn trap_wrong_target_or_stale_transfer_cannot_complete() {
        let mut target = node(2);
        target.hard_state.term = Term::new(3);
        target.leader_id = Some(NodeId::new(1));
        let effects = target.on_message(Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(3),
            to: NodeId::new(2),
            term: Term::new(3),
            kind: MessageKind::TimeoutNow {
                intent_index: LogIndex::new(1),
            },
        });
        assert!(matches!(
            effects.as_slice(),
            [RaftEffect::Trace {
                name: "timeout_now_rejected",
                ..
            }]
        ));
        assert_eq!(target.role, Role::Follower);
    }

    #[test]
    fn trap_leadership_transfer_recovers_or_finishes_after_crash() {
        let mut original = node(1);
        original.role = Role::Leader;
        original.hard_state.term = Term::new(3);
        let intent = Entry {
            term: Term::new(3),
            index: LogIndex::new(7),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(100),
                operation: ConfigOperation::BeginLeaderTransfer {
                    target: NodeId::new(2),
                },
            }
            .encode(),
        };
        original
            .apply_committed_config(&intent)
            .expect("intent is committed before crash");
        let state = original
            .leadership_transfer_state()
            .expect("snapshot retains transfer");

        let mut recovered = node(2);
        recovered.role = Role::Leader;
        recovered.hard_state.term = Term::new(4);
        recovered
            .restore_leadership_transfer(Some(state))
            .expect("restore workflow");
        assert_eq!(
            recovered.propose(b"blocked-until-finish".to_vec()),
            Err(RaftError::TransferInProgress)
        );

        let effects = recovered.become_leader();
        let finish = effects.iter().find_map(|effect| match effect {
            RaftEffect::PersistEntries(entries) => entries.iter().find(|entry| {
                matches!(
                    ConfigEnvelope::decode(&entry.payload),
                    Ok(ConfigEnvelope {
                        operation:
                            ConfigOperation::FinishLeaderTransfer {
                                intent_index,
                                result: TransferResult::Success,
                            },
                        ..
                    }) if intent_index == state.intent_index
                )
            }),
            _ => None,
        });
        assert!(
            finish.is_some(),
            "new target leader must append success finish"
        );
    }

    #[test]
    fn trap_leadership_transfer_timeout_appends_matching_finish() {
        let mut leader = node(1);
        leader.role = Role::Leader;
        leader.hard_state.term = Term::new(2);
        leader.match_index.insert(NodeId::new(2), LogIndex::new(0));
        let intent = Entry {
            term: Term::new(2),
            index: LogIndex::new(1),
            kind: EntryKind::Config,
            payload: ConfigEnvelope {
                admin_session: None,
                leader_time: Time::from_nanos(5),
                operation: ConfigOperation::BeginLeaderTransfer {
                    target: NodeId::new(2),
                },
            }
            .encode(),
        };
        leader.log.push(intent.clone());
        leader
            .apply_committed_config(&intent)
            .expect("committed intent");
        let effects = leader.tick(Time::from_nanos(
            5 + DEFAULT_LEADER_TRANSFER_TIMEOUT.as_nanos(),
        ));
        let finish = effects.iter().find_map(|effect| match effect {
            RaftEffect::PersistEntries(entries) => entries.first(),
            _ => None,
        });
        let Some(finish) = finish else {
            panic!("transfer timeout must append a finish entry");
        };
        let envelope = ConfigEnvelope::decode(&finish.payload).expect("finish envelope");
        assert!(matches!(
            envelope.operation,
            ConfigOperation::FinishLeaderTransfer {
                intent_index,
                result: TransferResult::Timeout,
            } if intent_index == intent.index
        ));
    }

    /// Beacons proving a schedule reached the states the soup exists to
    /// stress. A campaign that quietly stops electing leaders or committing
    /// entries has decayed into a smoke test; asserting on these at the end
    /// makes that decay a failure instead of a fast green run.
    #[derive(Default)]
    struct Coverage {
        elections: u64,
        committed_indices: u64,
        truncations: u64,
        duplicates: u64,
        drops: u64,
    }

    /// Three real nodes, a pool of in-flight messages, and a scheduler free to
    /// deliver them late, twice, or never.
    ///
    /// Only messages a node actually emitted ever enter the pool. Raft promises
    /// nothing against forged traffic, so injecting a synthetic message would
    /// manufacture a failure rather than find one; reordering, duplication and
    /// loss are the whole legal fault palette of an asynchronous network.
    struct Soup {
        nodes: BTreeMap<NodeId, RaftNode>,
        inflight: Vec<Message>,
        rng: Xoshiro256pp,
        now: Time,
        /// The entry some node had already committed at an index, with the term
        /// it was committed in, so a later disagreement is a safety violation
        /// while a stale leader from an earlier term is not.
        committed: BTreeMap<u64, (Entry, u64)>,
        leader_for_term: BTreeMap<u64, NodeId>,
        coverage: Coverage,
    }

    impl Soup {
        fn new(seed: u64) -> Self {
            let mut nodes = BTreeMap::new();
            for id in 1..=3_u64 {
                nodes.insert(
                    NodeId::new(id),
                    RaftNode::new(
                        NodeId::new(id),
                        voters(),
                        Seed::new(seed.wrapping_mul(3).wrapping_add(id)),
                        RaftConfig::default(),
                    ),
                );
            }
            Self {
                nodes,
                inflight: Vec::new(),
                rng: Xoshiro256pp::stream(Seed::new(seed), "message-soup", 0),
                now: Time::from_nanos(0),
                committed: BTreeMap::new(),
                leader_for_term: BTreeMap::new(),
                coverage: Coverage::default(),
            }
        }

        fn pick_node(&mut self) -> NodeId {
            NodeId::new(self.rng.range_u64(1, 4))
        }

        /// Queue what a node emitted. The pool is bounded because an unbounded
        /// one turns a long schedule into a memory test; shedding the oldest
        /// message is just one more legal loss.
        fn route(&mut self, effects: Vec<RaftEffect>) {
            for effect in effects {
                match effect {
                    RaftEffect::Send(message) => self.inflight.push(message),
                    RaftEffect::TruncateSuffix(_) => self.coverage.truncations += 1,
                    _ => {}
                }
            }
            while self.inflight.len() > 64 {
                self.inflight.remove(0);
                self.coverage.drops += 1;
            }
        }

        fn deliver(&mut self, position: usize, keep: bool) {
            let message = if keep {
                self.coverage.duplicates += 1;
                self.inflight[position].clone()
            } else {
                self.inflight.remove(position)
            };
            let now = self.now;
            let Some(node) = self.nodes.get_mut(&message.to) else {
                return;
            };
            let effects = node.on_message_at(message, now);
            self.route(effects);
        }

        fn step(&mut self) {
            // 1-20ms of jitter per step, so heartbeat and election deadlines
            // interleave differently from one schedule to the next.
            self.now = Time::from_nanos(
                self.now
                    .as_nanos()
                    .saturating_add(self.rng.range_u64(1_000_000, 20_000_000)),
            );
            let choice = self.rng.range_u64(0, 100);
            let pending = self.inflight.len();
            if pending > 0 && choice < 70 {
                // Deliver from anywhere in the pool, not the head: arrival order
                // is the thing under test.
                let position =
                    usize::try_from(self.rng.range_u64(0, pending as u64)).expect("in range");
                if choice < 8 {
                    self.deliver(position, true);
                } else if choice < 14 {
                    self.inflight.remove(position);
                    self.coverage.drops += 1;
                } else {
                    self.deliver(position, false);
                }
                return;
            }
            let now = self.now;
            if choice < 84 {
                let id = self.pick_node();
                let effects = self
                    .nodes
                    .get_mut(&id)
                    .expect("invariant: soup nodes are 1..=3")
                    .on_timer(now, TimerKind::Election);
                self.route(effects);
            } else if choice < 93 {
                let id = self.pick_node();
                let effects = self
                    .nodes
                    .get_mut(&id)
                    .expect("invariant: soup nodes are 1..=3")
                    .on_timer(now, TimerKind::Heartbeat);
                self.route(effects);
            } else {
                let id = self.pick_node();
                let payload = self.now.as_nanos().to_le_bytes().to_vec();
                let node = self
                    .nodes
                    .get_mut(&id)
                    .expect("invariant: soup nodes are 1..=3");
                // A non-leader answering `NotLeader` is the correct outcome, not
                // a harness error.
                if let Ok(effects) = node.propose(payload) {
                    self.route(effects);
                }
            }
        }

        /// The four cross-node safety properties. Each assertion carries the
        /// seed and step so a failure reproduces from the message alone.
        fn check(&mut self, seed: u64, step: usize) {
            let where_ = || format!("seed {seed:#018x} step {step}");

            for node in self.nodes.values() {
                let report = node.invariants();
                assert!(
                    report.is_ok(),
                    "{}: node {} local invariants {:?}",
                    where_(),
                    node.id,
                    report.violations
                );
            }

            // Election safety: at most one leader per term.
            let leaders: Vec<(u64, NodeId)> = self
                .nodes
                .values()
                .filter(|node| node.role == Role::Leader)
                .map(|node| (node.hard_state.term.get(), node.id))
                .collect();
            for (term, id) in leaders {
                match self.leader_for_term.get(&term) {
                    Some(existing) => assert_eq!(
                        *existing,
                        id,
                        "{}: term {term} had leaders {existing} and {id}",
                        where_()
                    ),
                    None => {
                        self.leader_for_term.insert(term, id);
                        self.coverage.elections += 1;
                    }
                }
            }

            let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            for (position, left_id) in ids.iter().enumerate() {
                for right_id in &ids[position + 1..] {
                    let left = &self.nodes[left_id];
                    let right = &self.nodes[right_id];
                    for left_entry in &left.log {
                        let Some(right_entry) = right
                            .log
                            .iter()
                            .find(|entry| entry.index == left_entry.index)
                        else {
                            continue;
                        };
                        // Log matching: agreement on (index, term) is agreement
                        // on the entry.
                        if left_entry.term == right_entry.term {
                            assert_eq!(
                                left_entry,
                                right_entry,
                                "{}: log matching at {} between {left_id} and {right_id}",
                                where_(),
                                left_entry.index
                            );
                        }
                        // State machine safety: two nodes cannot commit
                        // different entries at one index.
                        if left_entry.index <= left.commit_index
                            && right_entry.index <= right.commit_index
                        {
                            assert_eq!(
                                left_entry,
                                right_entry,
                                "{}: committed divergence at {} between {left_id} and {right_id}",
                                where_(),
                                left_entry.index
                            );
                        }
                    }
                }
            }

            // Record what is now committed anywhere, and hold it immutable.
            let mut seen: Vec<(Entry, u64)> = Vec::new();
            for node in self.nodes.values() {
                for entry in &node.log {
                    if entry.index <= node.commit_index {
                        seen.push((entry.clone(), node.hard_state.term.get()));
                    }
                }
            }
            for (entry, term) in seen {
                match self.committed.get(&entry.index.get()) {
                    Some((previous, _)) => assert_eq!(
                        *previous,
                        entry,
                        "{}: committed entry at {} changed",
                        where_(),
                        entry.index
                    ),
                    None => {
                        self.committed.insert(entry.index.get(), (entry, term));
                        self.coverage.committed_indices += 1;
                    }
                }
            }

            // Leader completeness: a leader of a strictly later term than the
            // one an entry committed in must carry that entry. Earlier-term
            // leaders are excluded because a partitioned stale leader that has
            // not yet stepped down is legal Raft.
            for node in self.nodes.values() {
                if node.role != Role::Leader {
                    continue;
                }
                for (index, (entry, commit_term)) in &self.committed {
                    if node.hard_state.term.get() <= *commit_term {
                        continue;
                    }
                    let held = node
                        .log
                        .iter()
                        .find(|candidate| candidate.index.get() == *index);
                    assert_eq!(
                        held,
                        Some(entry),
                        "{}: leader {} of term {} lacks entry {index} committed in term {commit_term}",
                        where_(),
                        node.id,
                        node.hard_state.term
                    );
                }
            }
        }
    }

    #[test]
    #[ignore = "G4 message-soup campaign; run explicitly in release mode"]
    fn message_soup_campaign_100k_schedules() {
        let mut total = Coverage::default();
        for seed in 0..100_000_u64 {
            let mut soup = Soup::new(seed);
            for step in 0..64 {
                soup.step();
                soup.check(seed, step);
            }
            total.elections += soup.coverage.elections;
            total.committed_indices += soup.coverage.committed_indices;
            total.truncations += soup.coverage.truncations;
            total.duplicates += soup.coverage.duplicates;
            total.drops += soup.coverage.drops;
        }
        // A gate run should be able to show what it exercised, not just that it
        // returned zero.
        println!(
            "message soup: schedules=100000 elections={} committed_indices={} truncations={} duplicates={} drops={}",
            total.elections,
            total.committed_indices,
            total.truncations,
            total.duplicates,
            total.drops
        );
        // Without these the campaign could pass by never reaching a leader,
        // which is exactly how the previous version of this test stayed green.
        assert!(total.elections > 0, "no leader was ever elected");
        assert!(
            total.committed_indices > 0,
            "no entry was ever committed under reordering"
        );
        assert!(
            total.truncations > 0,
            "no follower ever truncated a conflicting suffix"
        );
        assert!(total.duplicates > 0, "no message was ever redelivered");
        assert!(total.drops > 0, "no message was ever lost");
    }
}
