// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Bounded exhaustive exploration of the production `RaftNode` transition surface.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use cc_core::{NodeId, Seed, Time};

use super::{Entry, Message, RaftConfig, RaftEffect, RaftNode, Role, TimerKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelConfig {
    pub max_log: usize,
    pub max_term: u64,
    pub max_messages: usize,
    pub max_depth: usize,
    pub max_states: usize,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            max_log: 4,
            max_term: 3,
            max_messages: 8,
            max_depth: 16,
            max_states: 2_000_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelReport {
    pub explored_states: usize,
    pub explored_transitions: u64,
    pub max_frontier: usize,
    pub max_depth: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelError {
    InvalidConfig,
    StateLimit { explored: usize, limit: usize },
    Invariant { state: usize, reason: String },
}

impl fmt::Display for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => write!(formatter, "invalid model-check bounds"),
            Self::StateLimit { explored, limit } => {
                write!(
                    formatter,
                    "model state limit {limit} reached after {explored} states"
                )
            }
            Self::Invariant { state, reason } => {
                write!(
                    formatter,
                    "model invariant failed at state {state}: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ModelError {}

#[derive(Clone)]
struct ModelState {
    nodes: BTreeMap<NodeId, RaftNode>,
    soup: Vec<Message>,
    leaders_seen: BTreeMap<u64, NodeId>,
    committed: BTreeMap<u64, Entry>,
}

/// Enumerate every timer, proposal, delivery, duplication, and loss transition
/// reachable inside the configured bounds. Nodes are production `RaftNode`s;
/// only the host scheduler is the tiny model.
pub fn check(config: ModelConfig) -> Result<ModelReport, ModelError> {
    if config.max_log == 0
        || config.max_term == 0
        || config.max_messages == 0
        || config.max_depth == 0
        || config.max_states == 0
    {
        return Err(ModelError::InvalidConfig);
    }
    let voters: BTreeSet<NodeId> = (1..=3).map(NodeId::new).collect();
    let raft_config = RaftConfig {
        election_min: cc_core::Duration::from_millis(100),
        election_max: cc_core::Duration::from_millis(101),
        heartbeat: cc_core::Duration::from_millis(25),
        ..RaftConfig::default()
    };
    let nodes = voters
        .iter()
        .map(|id| {
            (
                *id,
                RaftNode::new(*id, voters.clone(), Seed::new(id.get()), raft_config),
            )
        })
        .collect();
    let initial = ModelState {
        nodes,
        soup: Vec::new(),
        leaders_seen: BTreeMap::new(),
        committed: BTreeMap::new(),
    };
    let mut seen = BTreeSet::from([fingerprint(&initial)]);
    let mut queue = VecDeque::from([(initial, 0_usize)]);
    let mut explored = 0_usize;
    let mut transitions = 0_u64;
    let mut max_frontier = 1_usize;
    let mut explored_depth = 0_usize;
    while let Some((mut state, depth)) = queue.pop_front() {
        explored = explored.saturating_add(1);
        explored_depth = explored_depth.max(depth);
        check_invariants(&mut state).map_err(|reason| ModelError::Invariant {
            state: explored,
            reason,
        })?;
        if depth == config.max_depth {
            continue;
        }
        for next in successors(&state, config) {
            transitions = transitions.saturating_add(1);
            if seen.insert(fingerprint(&next)) {
                if seen.len() > config.max_states {
                    return Err(ModelError::StateLimit {
                        explored,
                        limit: config.max_states,
                    });
                }
                queue.push_back((next, depth.saturating_add(1)));
            }
        }
        max_frontier = max_frontier.max(queue.len());
    }
    Ok(ModelReport {
        explored_states: explored,
        explored_transitions: transitions,
        max_frontier,
        max_depth: explored_depth,
    })
}

fn successors(state: &ModelState, config: ModelConfig) -> Vec<ModelState> {
    let mut output = Vec::new();
    let now = Time::from_nanos(1_000_000);
    for id in state.nodes.keys().copied() {
        if state.nodes[&id].hard_state.term.get() <= config.max_term {
            let mut next = state.clone();
            let effects = next
                .nodes
                .get_mut(&id)
                .expect("model node")
                .on_timer(now, TimerKind::Election);
            route(&mut next, effects, config.max_messages);
            if in_bounds(&next, config) {
                output.push(next);
            }
        }
        if state.nodes[&id].role == Role::Leader {
            let mut heartbeat = state.clone();
            let effects = heartbeat
                .nodes
                .get_mut(&id)
                .expect("model node")
                .on_timer(now, TimerKind::Heartbeat);
            route(&mut heartbeat, effects, config.max_messages);
            if in_bounds(&heartbeat, config) {
                output.push(heartbeat);
            }
            if state.nodes[&id].log.len() < config.max_log {
                let mut proposed = state.clone();
                let payload = state.nodes[&id]
                    .last_index()
                    .get()
                    .saturating_add(1)
                    .to_le_bytes()
                    .to_vec();
                if let Ok(effects) = proposed
                    .nodes
                    .get_mut(&id)
                    .expect("model node")
                    .propose(payload)
                {
                    route(&mut proposed, effects, config.max_messages);
                    if in_bounds(&proposed, config) {
                        output.push(proposed);
                    }
                }
            }
        }
    }
    for index in 0..state.soup.len() {
        let mut dropped = state.clone();
        dropped.soup.remove(index);
        output.push(dropped);

        let mut delivered = state.clone();
        let message = delivered.soup.remove(index);
        let effects = delivered
            .nodes
            .get_mut(&message.to)
            .expect("message recipient")
            .on_message_at(message, now);
        route(&mut delivered, effects, config.max_messages);
        if in_bounds(&delivered, config) {
            output.push(delivered);
        }

        if state.soup.len() < config.max_messages {
            let mut duplicated = state.clone();
            let message = duplicated.soup[index].clone();
            let effects = duplicated
                .nodes
                .get_mut(&message.to)
                .expect("message recipient")
                .on_message_at(message, now);
            route(&mut duplicated, effects, config.max_messages);
            if in_bounds(&duplicated, config) {
                output.push(duplicated);
            }
        }
    }
    output
}

fn route(state: &mut ModelState, effects: Vec<RaftEffect>, max_messages: usize) {
    for effect in effects {
        if let RaftEffect::Send(message) = effect
            && state.soup.len() < max_messages
            && !state.soup.contains(&message)
        {
            state.soup.push(message);
        }
    }
}

fn in_bounds(state: &ModelState, config: ModelConfig) -> bool {
    state.soup.len() <= config.max_messages
        && state.nodes.values().all(|node| {
            node.log.len() <= config.max_log && node.hard_state.term.get() <= config.max_term
        })
}

fn check_invariants(state: &mut ModelState) -> Result<(), String> {
    for node in state.nodes.values() {
        let report = node.invariants();
        if !report.is_ok() {
            return Err(format!("node {}: {:?}", node.id, report.violations));
        }
        if node.role == Role::Leader {
            let term = node.hard_state.term.get();
            if let Some(existing) = state.leaders_seen.insert(term, node.id)
                && existing != node.id
            {
                return Err(format!(
                    "election safety: term {term} leaders {existing} and {}",
                    node.id
                ));
            }
        }
        for entry in node
            .log
            .iter()
            .filter(|entry| entry.index <= node.commit_index)
        {
            if let Some(existing) = state.committed.insert(entry.index.get(), entry.clone())
                && existing != *entry
            {
                return Err(format!(
                    "state-machine safety: conflicting committed index {}",
                    entry.index
                ));
            }
        }
    }
    let nodes: Vec<&RaftNode> = state.nodes.values().collect();
    for left in 0..nodes.len() {
        for right in left + 1..nodes.len() {
            for left_entry in &nodes[left].log {
                if let Some(right_entry) = nodes[right]
                    .log
                    .iter()
                    .find(|entry| entry.index == left_entry.index && entry.term == left_entry.term)
                {
                    let prefix = left_entry.index.get() as usize;
                    if nodes[left].log[..prefix] != nodes[right].log[..prefix]
                        || right_entry != left_entry
                    {
                        return Err(format!("log matching failed at index {}", left_entry.index));
                    }
                }
            }
        }
    }
    for leader in state
        .nodes
        .values()
        .filter(|node| node.role == Role::Leader)
    {
        for entry in state
            .committed
            .values()
            .filter(|entry| entry.term < leader.hard_state.term)
        {
            if leader.log.get(entry.index.get().saturating_sub(1) as usize) != Some(entry) {
                return Err(format!(
                    "leader completeness failed at index {}",
                    entry.index
                ));
            }
        }
    }
    Ok(())
}

fn fingerprint(state: &ModelState) -> String {
    let mut messages = state
        .soup
        .iter()
        .map(|message| format!("{message:?}"))
        .collect::<Vec<_>>();
    messages.sort();
    let nodes = state
        .nodes
        .values()
        .map(|node| {
            format!(
                "{}:{:?}:{:?}:{:?}:{}:{}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{:?}:{}:{:?}:{}:{}:{:?}:{}:{}:{:?}",
                node.id,
                node.role,
                node.hard_state,
                node.log,
                node.commit_index.get(),
                node.applied_index.get(),
                node.next_index,
                node.match_index,
                node.votes,
                node.pre_votes,
                node.heard_quorum,
                node.read_acks,
                node.leader_id,
                node.joint,
                node.read_round,
                node.read_index,
                node.snapshot_buffer.len(),
                node.snapshot_index.get(),
                node.snapshot_term,
                node.election_deadline.as_nanos(),
                node.heartbeat_deadline.as_nanos(),
                node.rng,
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    format!(
        "{nodes}#{}#{:?}#{:?}",
        messages.join("|"),
        state.leaders_seen,
        state.committed
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_real_raft_state_space_is_safe() {
        let report = check(ModelConfig {
            max_log: 1,
            max_term: 1,
            max_messages: 1,
            max_depth: 6,
            max_states: 250_000,
        })
        .expect("bounded model");
        assert!(report.explored_states > 10);
        assert!(report.explored_transitions >= report.explored_states as u64);
    }
}
