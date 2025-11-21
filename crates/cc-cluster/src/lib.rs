// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The composition boundary: one Raft node, one KV state machine, value-only effects."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{
    Bytes, ClientId, ClusterPolicy, Dec, Duration, Enc, HostLimits, LogIndex, MembershipState,
    NodeId, Seed, SessionKey, SessionNamespace, Term, Time, crc32c_zeroed_tail,
};
use cc_kv::{
    Kv, KvCommand, KvError, KvReply, KvSnapshot, decode_command, decode_reply, encode_command,
    encode_reply,
};
use cc_raft::{Entry, HardState, LeadershipTransferState, RaftEffect, RaftError, RaftNode};
use cc_store::StoreConfig;

pub use cc_raft::{Message, MessageKind, PROTOCOL_VERSION, RaftConfig, Role, TimerKind};

pub const CLUSTER_VERSION: u16 = 1;
pub const APP_ENVELOPE_MAGIC: u32 = u32::from_le_bytes(*b"CCAP");
pub const APP_ENVELOPE_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    pub id: NodeId,
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
    PersistHard(cc_raft::HardState),
    PersistEntries(Vec<Entry>),
    TruncateSuffix(LogIndex),
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
    tombstones: BTreeMap<SessionKey, Time>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRecord {
    pub max_seq: u64,
    pub canonical_command: Bytes,
    pub cached_reply: Bytes,
    pub last_active: Time,
}

impl SessionTable {
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
        self.tombstones.retain(|_, until| *until > at);
        if self.tombstones.contains_key(&key) {
            return KvReply::Error(KvError::SessionExpired);
        }
        if let Some(record) = self.records.get(&key) {
            if at.as_nanos().saturating_sub(record.last_active.as_nanos()) > policy.session_idle_ns
            {
                if self.tombstones.len() as u64 >= policy.max_session_tombstones {
                    return KvReply::Error(KvError::TooLarge);
                }
                self.records.remove(&key);
                self.tombstones.insert(
                    key,
                    Time::from_nanos(at.as_nanos().saturating_add(policy.session_retry_grace_ns)),
                );
                return KvReply::Error(KvError::SessionExpired);
            }
            if sequence < record.max_seq {
                return KvReply::Error(KvError::StaleSequence);
            }
            if sequence == record.max_seq {
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
        if self.records.len() as u64 >= policy.max_sessions {
            return KvReply::Error(KvError::TooLarge);
        }
        let reply = mutate();
        let cached_reply = encode_reply(&reply);
        let bytes = u64::try_from(canonical_command.len().saturating_add(cached_reply.len()))
            .unwrap_or(u64::MAX);
        if bytes > policy.max_session_bytes {
            return KvReply::Error(KvError::TooLarge);
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

pub struct Node {
    pub raft: RaftNode,
    pub kv: Kv,
    pub sessions: SessionTable,
    config: NodeConfig,
    pending_reads: Vec<PendingRead>,
    read_barrier_ready: Option<LogIndex>,
    client_routes: BTreeMap<LogIndex, (ClientId, u64)>,
    continuation: Option<Vec<RaftEffect>>,
}

struct PendingRead {
    client: ClientId,
    sequence: u64,
    command: KvCommand,
    at: Time,
    index: LogIndex,
}

impl Node {
    pub fn new(mut config: NodeConfig, voters: BTreeSet<NodeId>) -> Result<Self, NodeError> {
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
            read_barrier_ready: None,
            client_routes: BTreeMap::new(),
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

    pub fn add_learner(&mut self, node: NodeId) -> Result<(), NodeError> {
        self.raft.add_learner(node)?;
        Ok(())
    }

    pub fn promote_learner(&mut self, node: NodeId) -> Result<(), NodeError> {
        self.raft.promote_learner(node)?;
        Ok(())
    }

    pub fn enter_joint(&mut self, voters: BTreeSet<NodeId>) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.enter_joint(voters)?;
        self.map_effects(effects, None)
    }

    pub fn leave_joint(&mut self) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.leave_joint()?;
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
                self.map_effects(continuation, None)
            }
            NodeInput::Tick { now } => {
                let effects = self.raft.tick(now);
                self.map_effects(effects, None)
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
        }
        Ok(output)
    }

    /// Value-boundary entry point used by new hosts.  It deliberately takes
    /// an explicit timestamp and block-read seam so neither the core nor a
    /// simulator-only global clock can affect deterministic state.  Current
    /// in-memory KV reads do not issue block reads yet, so their accounted
    /// service is zero; N3 routes real table reads through `blocks`.
    pub fn on_env_input(
        &mut self,
        now: Time,
        input: cc_env::Input,
        _blocks: &mut dyn cc_store::BlockSource,
    ) -> NodeStep {
        let outcome = match input {
            cc_env::Input::Recv { from, msg } => {
                if msg.proto_version != cc_raft::PROTOCOL_VERSION {
                    Err(NodeError::Environment("peer semantic version"))
                } else {
                    match cc_raft::codec::decode(&msg.payload) {
                        Err(_) => Err(NodeError::Environment("peer CCRP")),
                        Ok(message) if message.from != from || message.to != self.id() => {
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
        NodeStep {
            synchronous_service: Duration::from_nanos(0),
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
        decode_command(&command)?;
        if let Some((key, sequence)) = session
            && (key.namespace != SessionNamespace::UserRequest as u8 || sequence == 0)
        {
            return Err(NodeError::Kv(KvError::InvalidInput));
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
        self.map_effects(effects, None)
    }

    fn map_effects(
        &mut self,
        effects: Vec<RaftEffect>,
        _proposal: Option<(ClientId, u64)>,
    ) -> Result<Vec<NodeEffect>, NodeError> {
        if self.raft.role != Role::Leader {
            self.client_routes.clear();
        }
        let mut output = Vec::new();
        let mut remaining = effects.into_iter();
        while let Some(effect) = remaining.next() {
            match effect {
                RaftEffect::Send(message) => output.push(NodeEffect::Send(message)),
                RaftEffect::PersistHard(hard) => {
                    output.push(NodeEffect::PersistHard(hard));
                    self.continuation = Some(remaining.collect());
                    break;
                }
                RaftEffect::PersistEntries(entries) => {
                    output.push(NodeEffect::PersistEntries(entries));
                    self.continuation = Some(remaining.collect());
                    break;
                }
                RaftEffect::TruncateSuffix(index) => {
                    self.client_routes
                        .retain(|route_index, _| *route_index < index);
                    output.push(NodeEffect::TruncateSuffix(index));
                    self.continuation = Some(remaining.collect());
                    break;
                }
                RaftEffect::Apply(entries) => {
                    for entry in entries {
                        if entry.kind == cc_raft::EntryKind::App {
                            let envelope = decode_proposal(&entry)
                                .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?
                                .ok_or(NodeError::MalformedCommittedEntry(entry.index))?;
                            if envelope.command.len() as u64 > self.config.policy.max_command_bytes
                            {
                                return Err(NodeError::MalformedCommittedEntry(entry.index));
                            }
                            let command = decode_command(&envelope.command)
                                .map_err(|_| NodeError::MalformedCommittedEntry(entry.index))?;
                            let policy = self.config.policy;
                            let reply = match envelope.session {
                                Some((session_key, sequence)) => {
                                    if session_key.namespace != SessionNamespace::UserRequest as u8
                                        || sequence == 0
                                    {
                                        return Err(NodeError::MalformedCommittedEntry(
                                            entry.index,
                                        ));
                                    }
                                    self.sessions.apply_user(
                                        policy,
                                        session_key,
                                        sequence,
                                        envelope.command,
                                        envelope.leader_time,
                                        || {
                                            self.kv.apply_command_only(
                                                entry.index,
                                                entry.term,
                                                command,
                                                envelope.leader_time,
                                            )
                                        },
                                    )
                                }
                                None => self.kv.apply_command_only(
                                    entry.index,
                                    entry.term,
                                    command,
                                    envelope.leader_time,
                                ),
                            };
                            if self.kv.applied_index < entry.index {
                                self.kv
                                    .mark_applied(entry.index, entry.term, envelope.leader_time);
                            }
                            if let Some((client, sequence)) =
                                self.client_routes.remove(&entry.index)
                            {
                                output.push(NodeEffect::ClientReply {
                                    client,
                                    sequence,
                                    reply,
                                });
                            }
                        } else if entry.kind == cc_raft::EntryKind::Config {
                            self.kv
                                .mark_applied(entry.index, entry.term, Time::from_nanos(0));
                            let workflow_effects = self
                                .raft
                                .apply_committed_config(&entry)
                                .map_err(NodeError::Raft)?;
                            output.extend(self.map_effects(workflow_effects, None)?);
                        } else {
                            self.kv
                                .mark_applied(entry.index, entry.term, Time::from_nanos(0));
                        }
                    }
                }
                RaftEffect::ArmTimer { id, at, kind } => {
                    output.push(NodeEffect::ArmTimer { id, at, kind })
                }
                RaftEffect::ReadBarrier { .. } => {}
                RaftEffect::ReadBarrierReady { index } => {
                    self.read_barrier_ready = Some(index);
                }
                RaftEffect::Trace { name, .. } => output.push(NodeEffect::Trace(name)),
            }
        }
        output.extend(self.drain_pending_reads());
        Ok(output)
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
        let kv = self.kv.snapshot()?;
        Ok(NodeSnapshot {
            kv,
            sessions: self.sessions.clone(),
            membership: self.raft.membership_state(),
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
        Ok(())
    }
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
    if entry.kind != cc_raft::EntryKind::App || entry.payload.is_empty() {
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
            seed: Seed::new(id),
            raft: RaftConfig::default(),
            store: StoreConfig::default(),
            policy: ClusterPolicy::default(),
            host_limits: HostLimits::default(),
        }
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
        let voters = [NodeId::new(1)].into_iter().collect();
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
            KvReply::Error(KvError::TooLarge),
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
