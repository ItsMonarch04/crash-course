// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Sans-IO Raft core: deterministic elections, replication, and read barriers."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{Duration, LogIndex, NodeId, Seed, Term, Time, TimerId, Xoshiro256pp};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RaftConfig {
    pub election_min: Duration,
    pub election_max: Duration,
    pub heartbeat: Duration,
    pub max_entries_per_append: usize,
    pub pipeline_window: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_min: DEFAULT_ELECTION_MIN,
            election_max: DEFAULT_ELECTION_MAX,
            heartbeat: DEFAULT_HEARTBEAT,
            max_entries_per_append: MAX_ENTRIES_PER_APPEND,
            pipeline_window: PIPELINE_WINDOW,
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
        last_included_index: LogIndex,
        last_included_term: Term,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    SnapshotAck {
        offset: u64,
    },
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
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader => write!(f, "not leader"),
            Self::ReadBarrierNotReady => write!(f, "leader no-op is not committed"),
            Self::Busy => write!(f, "pipeline window is full"),
            Self::InvalidMessage => write!(f, "invalid raft message"),
            Self::CommittedConflict => write!(f, "message would truncate committed log"),
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
    read_acks: BTreeSet<NodeId>,
    read_round: u64,
    read_index: Option<LogIndex>,
    snapshot_buffer: Vec<u8>,
    snapshot_index: LogIndex,
    snapshot_term: Term,
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
            voters,
            learners: BTreeSet::new(),
            joint: None,
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
            read_acks: BTreeSet::new(),
            read_round: 0,
            read_index: None,
            snapshot_buffer: Vec::new(),
            snapshot_index: LogIndex::new(0),
            snapshot_term: Term::new(0),
        };
        node.reset_election(Time::from_nanos(0));
        node
    }

    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        self.log
            .last()
            .map_or(LogIndex::new(0), |entry| entry.index)
    }

    #[must_use]
    pub fn last_term(&self) -> Term {
        self.log.last().map_or(Term::new(0), |entry| entry.term)
    }

    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.get() == 0 {
            return Some(Term::new(0));
        }
        self.log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    pub fn tick(&mut self, now: Time) -> Vec<RaftEffect> {
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
        effects.extend(self.broadcast_append());
        Ok(effects)
    }

    pub fn request_read(&mut self) -> Result<Vec<RaftEffect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
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
        if self.voters.contains(&node) {
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
        if self.joint.is_some() || new_voters.is_empty() {
            return Err(RaftError::Busy);
        }
        let old = self.voters.clone();
        let mut union = old.clone();
        union.extend(new_voters.iter().copied());
        self.joint = Some((old, new_voters.clone()));
        self.voters = union;
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get() + 1),
            kind: EntryKind::Config,
            payload: encode_membership(&new_voters),
        };
        self.log.push(entry.clone());
        Ok(vec![RaftEffect::PersistEntries(vec![entry])])
    }

    pub fn leave_joint(&mut self) -> Result<Vec<RaftEffect>, RaftError> {
        let Some((_, new_voters)) = self.joint.take() else {
            return Err(RaftError::InvalidMessage);
        };
        self.voters = new_voters.clone();
        let entry = Entry {
            term: self.hard_state.term,
            index: LogIndex::new(self.last_index().get() + 1),
            kind: EntryKind::Config,
            payload: encode_membership(&new_voters),
        };
        self.log.push(entry.clone());
        if !self.voters.contains(&self.id) {
            self.role = Role::Follower;
        }
        Ok(vec![RaftEffect::PersistEntries(vec![entry])])
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
        self.log.retain(|entry| entry.index > index);
        self.commit_index = self.commit_index.max(index);
        self.applied_index = self.applied_index.max(index);
        self.snapshot_index = index;
        self.snapshot_term = term;
    }

    #[must_use]
    pub fn snapshot_chunks(
        &self,
        peer: NodeId,
        index: LogIndex,
        term: Term,
        bytes: &[u8],
    ) -> Vec<Message> {
        let chunk_size = SNAPSHOT_CHUNK_BYTES;
        if bytes.is_empty() {
            return vec![Message {
                proto_version: PROTOCOL_VERSION,
                from: self.id,
                to: peer,
                term: self.hard_state.term,
                kind: MessageKind::SnapshotChunk {
                    last_included_index: index,
                    last_included_term: term,
                    offset: 0,
                    data: Vec::new(),
                    done: true,
                },
            }];
        }
        bytes
            .chunks(chunk_size)
            .enumerate()
            .map(|(chunk, data)| Message {
                proto_version: PROTOCOL_VERSION,
                from: self.id,
                to: peer,
                term: self.hard_state.term,
                kind: MessageKind::SnapshotChunk {
                    last_included_index: index,
                    last_included_term: term,
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
            self.role = Role::Follower;
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
            MessageKind::SnapshotChunk {
                last_included_index,
                last_included_term,
                offset,
                data,
                done,
            } => effects.extend(self.snapshot_chunk(
                &message,
                last_included_index,
                last_included_term,
                offset,
                data,
                done,
            )),
            MessageKind::SnapshotAck { offset } => {
                effects.push(RaftEffect::Trace {
                    name: "snapshot_ack",
                    index: LogIndex::new(offset),
                });
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
        for peer in &self.voters {
            if *peer != self.id {
                self.next_index.insert(*peer, next);
                self.match_index.insert(*peer, LogIndex::new(0));
            }
        }
        let mut effects = vec![RaftEffect::PersistEntries(vec![entry])];
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
                    effects.push(RaftEffect::TruncateSuffix(entry.index));
                } else {
                    continue;
                }
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
            self.heard_quorum.insert(peer);
            let mut effects = self.advance_commit();
            // A response carrying an older round was already in flight when the
            // read index was fixed, so it is not evidence of current leadership.
            if response.read_round == self.read_round {
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
        if self.role != Role::Leader || message.term != self.hard_state.term {
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
            .filter(|peer| **peer != self.id)
            .flat_map(|peer| self.send_append(*peer))
            .collect()
    }

    fn advance_commit(&mut self) -> Vec<RaftEffect> {
        let mut candidate = self.commit_index.get() + 1;
        let mut new_commit = self.commit_index;
        while candidate <= self.last_index().get() {
            let index = LogIndex::new(candidate);
            let replicated = 1 + self
                .match_index
                .values()
                .filter(|match_index| match_index.get() >= candidate)
                .count();
            if self.commit_quorum(candidate, replicated)
                && self.term_at(index) == Some(self.hard_state.term)
            {
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

    fn snapshot_chunk(
        &mut self,
        message: &Message,
        last_included_index: LogIndex,
        last_included_term: Term,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    ) -> Vec<RaftEffect> {
        if last_included_index <= self.applied_index {
            return vec![RaftEffect::Trace {
                name: "stale_snapshot_ignored",
                index: last_included_index,
            }];
        }
        if offset == 0 {
            self.snapshot_buffer.clear();
            self.snapshot_index = last_included_index;
            self.snapshot_term = last_included_term;
        }
        if offset != self.snapshot_buffer.len() as u64 {
            return vec![RaftEffect::Trace {
                name: "snapshot_offset_rejected",
                index: last_included_index,
            }];
        }
        self.snapshot_buffer.extend_from_slice(&data);
        if done {
            self.log.retain(|entry| entry.index > last_included_index);
            self.commit_index = self.commit_index.max(last_included_index);
            self.applied_index = last_included_index;
            self.snapshot_buffer.clear();
        }
        vec![RaftEffect::Send(Message {
            proto_version: PROTOCOL_VERSION,
            from: self.id,
            to: message.from,
            term: self.hard_state.term,
            kind: MessageKind::SnapshotAck {
                offset: offset
                    + u64::try_from(data.len()).expect("invariant: snapshot chunk length fits u64"),
            },
        })]
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

    fn commit_quorum(&self, candidate: u64, union_replicated: usize) -> bool {
        let Some((old, new)) = &self.joint else {
            return union_replicated >= self.majority();
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
        count(old) >= old.len() / 2 + 1 && count(new) >= new.len() / 2 + 1
    }

    fn has_majority(&self, votes: &BTreeSet<NodeId>) -> bool {
        votes.len() >= self.majority()
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

fn encode_membership(voters: &BTreeSet<NodeId>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 * voters.len());
    for voter in voters {
        bytes.extend_from_slice(&voter.get().to_le_bytes());
    }
    bytes
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
                last_included_index: LogIndex::new(3),
                last_included_term: Term::new(1),
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
                last_included_index: LogIndex::new(9),
                last_included_term: Term::new(1),
                offset: 0,
                data: vec![1],
                done: true,
            },
        });
        assert!(effects.iter().any(|effect| matches!(
            effect,
            RaftEffect::Trace {
                name: "stale_snapshot_ignored",
                ..
            }
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
        let new_voters = [NodeId::new(1), NodeId::new(2), NodeId::new(4)]
            .into_iter()
            .collect();
        leader.enter_joint(new_voters).expect("joint");
        leader.install_snapshot_state(LogIndex::new(1), Term::new(1));
        assert!(leader.joint_active());
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
