// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The composition boundary: one Raft node, one KV state machine, value-only effects."]

use std::collections::BTreeSet;
use std::fmt;

use cc_core::{ClientId, Dec, Enc, LogIndex, NodeId, Seed, Time};
use cc_kv::{Kv, KvCommand, KvError, KvReply, KvSnapshot, decode_command, encode_command};
use cc_raft::{Entry, Message, RaftConfig, RaftEffect, RaftError, RaftNode, Role};
use cc_store::StoreConfig;

pub const CLUSTER_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    pub id: NodeId,
    pub seed: Seed,
    pub raft: RaftConfig,
    pub store: StoreConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeInput {
    Tick {
        now: Time,
    },
    Message(Message),
    ClientRequest {
        client: ClientId,
        sequence: u64,
        command: KvCommand,
        leader_time: Time,
    },
    Read {
        client: ClientId,
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
        reply: KvReply,
    },
    ReadReply {
        client: ClientId,
        reply: KvReply,
    },
    ArmTimer {
        id: cc_core::TimerId,
        at: Time,
        kind: cc_raft::TimerKind,
    },
    Trace(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeError {
    Raft(RaftError),
    Kv(KvError),
    NotLeader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSnapshot {
    pub kv: KvSnapshot,
    pub last_included_index: LogIndex,
    pub last_included_term: cc_core::Term,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raft(error) => write!(f, "raft: {error}"),
            Self::Kv(error) => write!(f, "kv: {error}"),
            Self::NotLeader => write!(f, "not leader"),
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
    config: NodeConfig,
}

impl Node {
    pub fn new(config: NodeConfig, voters: BTreeSet<NodeId>) -> Result<Self, NodeError> {
        Ok(Self {
            raft: RaftNode::new(config.id, voters, config.seed, config.raft),
            kv: Kv::new(config.store)?,
            config,
        })
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
        Ok(self.map_effects(effects, None))
    }

    pub fn leave_joint(&mut self) -> Result<Vec<NodeEffect>, NodeError> {
        let effects = self.raft.leave_joint()?;
        Ok(self.map_effects(effects, None))
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
        match input {
            NodeInput::Tick { now } => {
                let effects = self.raft.tick(now);
                Ok(self.map_effects(effects, None))
            }
            NodeInput::Message(message) => {
                let effects = self.raft.on_message(message);
                Ok(self.map_effects(effects, None))
            }
            NodeInput::ClientRequest {
                client,
                sequence,
                command,
                leader_time,
            } => {
                if self.raft.role != Role::Leader {
                    return Err(NodeError::NotLeader);
                }
                let effects =
                    self.raft
                        .propose(encode_proposal(client, sequence, command, leader_time))?;
                Ok(self.map_effects(effects, Some((client, sequence))))
            }
            NodeInput::Read {
                client,
                command,
                at,
            } => {
                self.raft.request_read()?;
                Ok(vec![NodeEffect::ReadReply {
                    client,
                    reply: self.kv.read(command, at)?,
                }])
            }
        }
    }

    fn map_effects(
        &mut self,
        effects: Vec<RaftEffect>,
        _proposal: Option<(ClientId, u64)>,
    ) -> Vec<NodeEffect> {
        let mut output = Vec::new();
        for effect in effects {
            match effect {
                RaftEffect::Send(message) => output.push(NodeEffect::Send(message)),
                RaftEffect::PersistHard(hard) => output.push(NodeEffect::PersistHard(hard)),
                RaftEffect::PersistEntries(entries) => {
                    output.push(NodeEffect::PersistEntries(entries))
                }
                RaftEffect::TruncateSuffix(index) => output.push(NodeEffect::TruncateSuffix(index)),
                RaftEffect::Apply(entries) => {
                    for entry in entries {
                        if let Ok(Some((client, sequence, command, time))) = decode_proposal(&entry)
                        {
                            let reply = self
                                .kv
                                .apply(entry.index, entry.term, client, sequence, command, time)
                                .unwrap_or_else(KvReply::Error);
                            output.push(NodeEffect::ClientReply { client, reply });
                        }
                    }
                }
                RaftEffect::ArmTimer { id, at, kind } => {
                    output.push(NodeEffect::ArmTimer { id, at, kind })
                }
                RaftEffect::ReadBarrier { .. } => {}
                RaftEffect::Trace { name, .. } => output.push(NodeEffect::Trace(name)),
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
            last_included_index: self.raft.applied_index,
            last_included_term: self
                .raft
                .term_at(self.raft.applied_index)
                .unwrap_or(cc_core::Term::new(0)),
        })
    }

    pub fn install_snapshot(&mut self, snapshot: NodeSnapshot) -> Result<(), NodeError> {
        self.kv = Kv::restore(snapshot.kv, self.config.store)?;
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
    let mut enc = Enc::new();
    enc.u64(client.get());
    enc.u64(sequence);
    enc.u64(leader_time.as_nanos());
    enc.bytes(&encode_command(&command));
    enc.finish()
}

fn decode_proposal(entry: &Entry) -> Result<Option<(ClientId, u64, KvCommand, Time)>, KvError> {
    if entry.kind != cc_raft::EntryKind::App || entry.payload.is_empty() {
        return Ok(None);
    }
    let mut dec = Dec::new(&entry.payload);
    let client = ClientId::new(dec.u64()?);
    let sequence = dec.u64()?;
    let leader_time = Time::from_nanos(dec.u64()?);
    let command = decode_command(&dec.bytes()?)?;
    dec.finish()?;
    Ok(Some((client, sequence, command, leader_time)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_core::Term;

    fn config(id: u64) -> NodeConfig {
        NodeConfig {
            id: NodeId::new(id),
            seed: Seed::new(id),
            raft: RaftConfig::default(),
            store: StoreConfig::default(),
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
        assert_eq!(decoded.0, ClientId::new(3));
        assert_eq!(decoded.1, 4);
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
}
