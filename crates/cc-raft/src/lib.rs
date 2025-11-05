// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Sans-IO Raft core: deterministic elections, replication, and read barriers."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{Duration, LogIndex, NodeId, Seed, Term, Time, TimerId, Xoshiro256pp};

pub const PROTOCOL_VERSION: u16 = 1;
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendResponse {
    pub success: bool,
    pub match_index: LogIndex,
    pub conflict_term: Option<Term>,
    pub conflict_index: LogIndex,
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

    pub fn request_read(&self) -> Result<Vec<RaftEffect>, RaftError> {
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
        Ok(vec![RaftEffect::ReadBarrier {
            index: self.commit_index,
        }])
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
        if message.proto_version != PROTOCOL_VERSION {
            return vec![RaftEffect::Trace {
                name: "proto_version_rejected",
                index: self.last_index(),
            }];
        }
        let mut effects = Vec::new();
        if message.term > self.hard_state.term {
            self.hard_state.term = message.term;
            self.hard_state.voted_for = None;
            self.role = Role::Follower;
            self.leader_id = None;
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
                if self.role == Role::Follower || self.role == Role::Candidate {
                    if granted {
                        self.pre_votes.insert(message.from);
                    }
                    if self.has_majority(&self.pre_votes) {
                        effects.extend(self.start_election());
                    }
                }
            }
            MessageKind::VoteReq {
                last_index,
                last_term,
            } => {
                effects.extend(self.vote_request(&message, last_index, last_term));
            }
            MessageKind::VoteResp { granted } => {
                if self.role == Role::Candidate {
                    if granted {
                        self.votes.insert(message.from);
                    }
                    if self.has_majority(&self.votes) {
                        effects.extend(self.become_leader());
                    }
                }
            }
            MessageKind::AppendReq(request) => {
                effects.extend(self.append_request(&message, request))
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
            self.reset_election(Time::from_nanos(0));
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

    fn append_request(&mut self, message: &Message, request: AppendRequest) -> Vec<RaftEffect> {
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
        self.reset_election(Time::from_nanos(0));
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
            effects.extend(self.send_append(peer));
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

    #[test]
    #[ignore = "G4 message-soup campaign; run explicitly in release mode"]
    fn message_soup_campaign_100k_schedules() {
        for seed in 0..100_000_u64 {
            let mut candidate = RaftNode::new(
                NodeId::new(1),
                voters(),
                Seed::new(seed),
                RaftConfig::default(),
            );
            let _ = candidate.on_timer(Time::from_nanos(1_000_000_000), TimerKind::Election);
            assert!(candidate.invariants().is_ok());
        }
    }
}
