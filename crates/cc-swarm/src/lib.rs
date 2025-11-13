// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The real deterministic cluster fixture used by cc-swarm and later theater work."]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt;

use cc_checker::{
    CheckerConfig, History, LivenessReport, Operation, OperationKind, Outcome, Verdict, check,
    check_liveness,
};
use cc_cluster::{Node, NodeConfig, NodeEffect, NodeError, NodeInput};
use cc_core::{
    ClientId, Duration, EventKind, NodeId, Seed, Time, TimerId, Trace, Xoshiro256pp, fnv1a,
};
use cc_env::FileId;
use cc_kv::{KvCommand, KvReply};
use cc_raft::{Entry, RaftConfig, Role, TimerKind};
use cc_sim::{
    DiskFault, FaultAction, FaultAt, FaultPlan, FaultProfile, LinkConfig, Network, NetworkDecision,
    Recorder, RecorderLevel, RunError, RunSpec, SimConfig, SimDisk, WorkloadActor,
    WorkloadOperation, canonicalize_fault_plan, shrink_fault_plan,
};
use cc_store::StoreConfig;

pub const MAX_OPERATIONS_PER_RUN: u64 = 32;
pub const REACHABILITY_BEACONS: [&str; 6] = [
    "leader-elected",
    "network-drop",
    "client-timeout",
    "snapshot-install",
    "membership-change",
    "disk-loss",
];
/// The same list, pre-joined for `--help`. Kept next to the array so the two
/// cannot drift.
pub const REACHABILITY_BEACONS_HELP: &str =
    "leader-elected, network-drop, client-timeout, snapshot-install, membership-change, disk-loss";
const TICK_INTERVAL: Duration = Duration::from_millis(25);
const CLIENT_RETRY: Duration = Duration::from_millis(25);
const CLIENT_TIMEOUT: Duration = Duration::from_millis(750);
/// How long a joint config is left open before the host closes it.
const JOINT_SETTLE: Duration = Duration::from_millis(500);
const FIRST_CLIENT_TIME: Duration = Duration::from_secs(1);
const WAL_FILE: FileId = FileId::Wal { segment: 0 };
/// `version u64 | term u64 | voted_for u64`, rewritten in place on every
/// `PersistHard`. Log records are appended after it.
const WAL_HARD_STATE_LEN: u64 = 24;
const WAL_HARD_STATE_VERSION: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ClusterEventKind {
    Tick(NodeId),
    Timer {
        node: NodeId,
        id: TimerId,
        kind: TimerKind,
    },
    Message {
        token: u64,
        from: NodeId,
        to: NodeId,
    },
    ClientIssue {
        client: u64,
        sequence: u64,
        operation: WorkloadOperation,
    },
    ClientTimeout(u64),
    Fault(FaultAction),
    /// Second half of a joint-consensus transition, scheduled by the host once
    /// the joint config has had time to replicate.
    LeaveJoint(NodeId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClusterEvent {
    at: Time,
    tie_seq: u64,
    kind: ClusterEventKind,
}

impl Ord for ClusterEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.tie_seq.cmp(&self.tie_seq))
    }
}

impl PartialOrd for ClusterEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct NodeSlot {
    config: NodeConfig,
    node: Option<Node>,
    status: cc_sim::NodeStatus,
    clock_offset: Duration,
    disk: SimDisk,
    armed_timers: BTreeMap<TimerId, Time>,
    /// Byte offset of each log index inside `WAL_FILE`, so a conflicting append
    /// or a `TruncateSuffix` can rewind the log to a real byte boundary.
    entry_offsets: BTreeMap<u64, u64>,
    wal_end: u64,
}

impl NodeSlot {
    fn reset_wal(&mut self) {
        self.entry_offsets.clear();
        self.wal_end = WAL_HARD_STATE_LEN;
    }
}

struct PendingOperation {
    operation: Operation,
    client: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRun {
    pub spec: RunSpec,
    pub seed: Seed,
    pub trace: Trace,
    pub history: History,
    pub verdict: Verdict,
    pub trace_invariants_ok: bool,
    pub had_leader: bool,
    pub completed_operations: u64,
    pub event_count: u64,
    pub final_log_indices: Vec<(u64, u64, u64)>,
    pub liveness_ok: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterNodeSnapshot {
    pub id: u64,
    pub status: cc_sim::NodeStatus,
    pub role: Role,
    pub term: u64,
    pub commit: u64,
    pub applied: u64,
    pub durable_bytes: u64,
    pub log_tail: Vec<u64>,
    pub voters: Vec<u64>,
    pub joint: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterSnapshot {
    pub virtual_time: Time,
    pub trace: Trace,
    pub nodes: Vec<ClusterNodeSnapshot>,
    pub history_len: usize,
    pub completed_operations: u64,
    pub had_leader: bool,
    pub verdict: Verdict,
    pub liveness_ok: bool,
}

/// The first semantic difference between two traces. Sequence numbers are
/// deliberately omitted from the comparison: position is the sequence, while
/// the fields below explain the state-machine-visible divergence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceDifference {
    pub event_index: usize,
    pub left_len: usize,
    pub right_len: usize,
    pub field: &'static str,
    pub left: String,
    pub right: String,
}

/// Find the first meaningful difference between two traces.
#[must_use]
pub fn semantic_trace_diff(left: &Trace, right: &Trace) -> Option<TraceDifference> {
    for (index, (left_event, right_event)) in left.events.iter().zip(&right.events).enumerate() {
        let difference = if left_event.time != right_event.time {
            Some((
                "time_ns",
                left_event.time.as_nanos().to_string(),
                right_event.time.as_nanos().to_string(),
            ))
        } else if left_event.node != right_event.node {
            Some((
                "node",
                left_event
                    .node
                    .map_or_else(|| String::from("none"), |node| node.get().to_string()),
                right_event
                    .node
                    .map_or_else(|| String::from("none"), |node| node.get().to_string()),
            ))
        } else if left_event.kind != right_event.kind {
            Some((
                "kind",
                left_event.kind.as_str().to_owned(),
                right_event.kind.as_str().to_owned(),
            ))
        } else if left_event.payload != right_event.payload {
            Some((
                "payload_hex",
                hex(&left_event.payload),
                hex(&right_event.payload),
            ))
        } else {
            None
        };
        if let Some((field, left_value, right_value)) = difference {
            return Some(TraceDifference {
                event_index: index,
                left_len: left.events.len(),
                right_len: right.events.len(),
                field,
                left: left_value,
                right: right_value,
            });
        }
    }
    (left.events.len() != right.events.len()).then(|| TraceDifference {
        event_index: left.events.len().min(right.events.len()),
        left_len: left.events.len(),
        right_len: right.events.len(),
        field: "length",
        left: left.events.len().to_string(),
        right: right.events.len().to_string(),
    })
}

/// Render a trace as a dependency-free SVG sequence diagram suitable for
/// issue reports, writeups, and museum exhibits.
#[must_use]
pub fn sequence_diagram_svg(trace: &Trace) -> String {
    let node_count = trace
        .events
        .iter()
        .filter_map(|event| event.node)
        .map(NodeId::get)
        .max()
        .unwrap_or(1);
    let visible: Vec<_> = trace
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                EventKind::NetSend
                    | EventKind::RoleChange
                    | EventKind::Commit
                    | EventKind::Fault
                    | EventKind::ClientOk
                    | EventKind::ClientTimeout
            )
        })
        .take(500)
        .collect();
    let width = 120_u64.saturating_add(node_count.saturating_mul(140));
    let height = 100_usize.saturating_add(visible.len().saturating_mul(24));
    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\"><style>text{{font:11px ui-monospace,monospace;fill:#dce7f2}}.lane{{stroke:#334354}}.msg{{stroke:#58d6b2;marker-end:url(#a)}}.event{{fill:#f2b84b}}</style><rect width=\"100%\" height=\"100%\" fill=\"#0d131c\"/><defs><marker id=\"a\" markerWidth=\"7\" markerHeight=\"7\" refX=\"6\" refY=\"3.5\" orient=\"auto\"><path d=\"M0,0 L7,3.5 L0,7z\" fill=\"#58d6b2\"/></marker></defs>"
    );
    for node in 1..=node_count {
        let x = 80_u64.saturating_add(node.saturating_sub(1).saturating_mul(140));
        svg.push_str(&format!("<text x=\"{x}\" y=\"28\" text-anchor=\"middle\">n{node}</text><line class=\"lane\" x1=\"{x}\" y1=\"40\" x2=\"{x}\" y2=\"{}\"/>", height.saturating_sub(20)));
    }
    for (row, event) in visible.iter().enumerate() {
        let y = 58_usize.saturating_add(row.saturating_mul(24));
        let from = event.node.map_or(1, NodeId::get);
        let from_x = 80_u64.saturating_add(from.saturating_sub(1).saturating_mul(140));
        if event.kind == EventKind::NetSend {
            let payload = String::from_utf8_lossy(&event.payload);
            let to = payload
                .split_once('>')
                .and_then(|(_, tail)| tail.split(':').next())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(from);
            let to_x = 80_u64.saturating_add(to.saturating_sub(1).saturating_mul(140));
            svg.push_str(&format!("<line class=\"msg\" x1=\"{from_x}\" y1=\"{y}\" x2=\"{to_x}\" y2=\"{y}\"/><text x=\"{}\" y=\"{}\" text-anchor=\"middle\">send #{}</text>", (from_x + to_x) / 2, y.saturating_sub(4), event.seq));
        } else {
            svg.push_str(&format!("<circle class=\"event\" cx=\"{from_x}\" cy=\"{y}\" r=\"4\"/><text x=\"{}\" y=\"{}\">{} #{}</text>", from_x.saturating_add(8), y.saturating_add(4), event.kind.as_str(), event.seq));
        }
    }
    svg.push_str("</svg>");
    svg
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Antithesis-style reachability beacon. A beacon records an expected rare
/// state without turning its absence in one short run into a safety failure.
#[macro_export]
macro_rules! sometimes {
    ($hits:expr, $index:expr, $condition:expr) => {
        if $condition {
            $hits[$index] = $hits[$index].saturating_add(1);
        }
    };
}

#[must_use]
pub fn reachability_beacons(trace: &Trace, had_leader: bool) -> [u64; REACHABILITY_BEACONS.len()] {
    let mut hits = [0_u64; REACHABILITY_BEACONS.len()];
    sometimes!(hits, 0, had_leader);
    for event in &trace.events {
        sometimes!(hits, 1, event.kind == EventKind::NetDrop);
        sometimes!(hits, 2, event.kind == EventKind::ClientTimeout);
        sometimes!(hits, 3, event.kind == EventKind::SnapshotInstall);
        sometimes!(hits, 4, event.kind == EventKind::ConfChange);
        sometimes!(hits, 5, event.kind == EventKind::IoLost);
    }
    hits
}

/// Stable event n-grams used as feedback for coverage-guided fault-plan
/// search. Payload hashes distinguish messages of the same broad event kind.
#[must_use]
pub fn trace_coverage(trace: &Trace) -> BTreeSet<u64> {
    let atoms: Vec<u64> = trace
        .events
        .iter()
        .map(|event| {
            let mut bytes = event.kind.as_str().as_bytes().to_vec();
            bytes.extend_from_slice(&fnv1a(&event.payload).to_le_bytes());
            fnv1a(&bytes)
        })
        .collect();
    let mut coverage = BTreeSet::new();
    for width in 1..=3 {
        for window in atoms.windows(width) {
            let mut bytes = Vec::with_capacity(window.len() * 8);
            for atom in window {
                bytes.extend_from_slice(&atom.to_le_bytes());
            }
            coverage.insert(fnv1a(&bytes));
        }
    }
    coverage
}

/// Deterministically mutate plan timing while retaining typed actions. This is
/// intentionally small: feedback decides which plans survive, and mutation
/// never invents an unrepresentable fault.
#[must_use]
pub fn mutate_fault_plan(plan: &FaultPlan, seed: Seed, end_time: Time) -> FaultPlan {
    if plan.actions.is_empty() {
        return plan.clone();
    }
    let mut rng = Xoshiro256pp::stream(seed, "coverage-guided-fault-search", 0);
    let mut mutated = plan.clone();
    let index = usize::try_from(rng.range_u64(0, mutated.actions.len() as u64)).unwrap_or(0);
    let horizon = end_time.as_nanos().max(1);
    let delta = rng.range_u64(0, (horizon / 8).max(1));
    let old = mutated.actions[index].at.as_nanos();
    let moved = if rng.range_u64(0, 2) == 0 {
        old.saturating_sub(delta)
    } else {
        old.saturating_add(delta).min(horizon.saturating_sub(1))
    };
    mutated.actions[index].at = Time::from_nanos(moved);
    canonicalize_fault_plan(&mutated)
}

impl ClusterRun {
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.trace_invariants_ok && self.had_leader && self.liveness_ok
    }

    #[must_use]
    pub fn artifact_json(&self, profile: FaultProfile) -> String {
        let verdict = match self.verdict {
            Verdict::Linearizable { .. } => "linearizable",
            Verdict::NotLinearizable { .. } => "not-linearizable",
            Verdict::Undecided { .. } => "undecided",
        };
        let checker_report = verdict_json(&self.verdict);
        format!(
            "{{\"fixture_version\":1,\"run_spec\":{},\"seed\":\"{}\",\"profile\":\"{}\",\"events\":{},\"completed_operations\":{},\"had_leader\":{},\"trace_invariants_ok\":{},\"liveness_ok\":{},\"verdict\":\"{}\",\"checker_report\":{},\"trace\":{}}}",
            run_spec_json(&self.spec),
            self.seed,
            profile.as_str(),
            self.event_count,
            self.completed_operations,
            self.had_leader,
            self.trace_invariants_ok,
            self.liveness_ok,
            verdict,
            checker_report,
            self.trace.to_json()
        )
    }
}

fn run_spec_json(spec: &RunSpec) -> String {
    let faults = spec
        .plan
        .actions
        .iter()
        .map(|fault| {
            format!(
                "{{\"at_ns\":{},\"action\":{}}}",
                fault.at.as_nanos(),
                fault_action_json(&fault.action)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"seed\":\"{}\",\"profile\":\"{}\",\"node_count\":{},\"end_time_ns\":{},\"workload\":{{\"clients\":{},\"ops_per_second\":{},\"keyspace\":{}}},\"faults\":[{}]}}",
        spec.seed,
        spec.profile.as_str(),
        spec.config.node_count,
        spec.end_time.as_nanos(),
        spec.workload.clients,
        spec.workload.ops_per_second,
        spec.workload.keyspace,
        faults
    )
}

fn fault_action_json(action: &FaultAction) -> String {
    match action {
        FaultAction::Partition { left, right } => format!(
            "{{\"kind\":\"partition\",\"left\":{},\"right\":{}}}",
            node_ids_json(left),
            node_ids_json(right)
        ),
        FaultAction::Heal => String::from("{\"kind\":\"heal\"}"),
        FaultAction::Crash { node } => format!("{{\"kind\":\"crash\",\"node\":{}}}", node.get()),
        FaultAction::Restart { node } => {
            format!("{{\"kind\":\"restart\",\"node\":{}}}", node.get())
        }
        FaultAction::Wipe { node } => format!("{{\"kind\":\"wipe\",\"node\":{}}}", node.get()),
        FaultAction::ClockSkew { node, offset } => format!(
            "{{\"kind\":\"clock-skew\",\"node\":{},\"offset_ns\":{}}}",
            node.get(),
            offset.as_nanos()
        ),
        FaultAction::DiskDegrade {
            node,
            write_latency,
        } => format!(
            "{{\"kind\":\"disk-degrade\",\"node\":{},\"write_latency_ns\":{}}}",
            node.get(),
            write_latency.as_nanos()
        ),
        FaultAction::LinkDegrade { from, to, .. } => format!(
            "{{\"kind\":\"link-degrade\",\"from\":{},\"to\":{}}}",
            from.get(),
            to.get()
        ),
        FaultAction::Reconfigure { voters } => format!(
            "{{\"kind\":\"reconfigure\",\"voters\":{}}}",
            node_ids_json(voters)
        ),
    }
}

fn node_ids_json(nodes: &[NodeId]) -> String {
    format!(
        "[{}]",
        nodes
            .iter()
            .map(|node| node.get().to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn verdict_json(verdict: &Verdict) -> String {
    match verdict {
        Verdict::Linearizable { visited } => {
            format!("{{\"kind\":\"linearizable\",\"visited\":{visited}}}")
        }
        Verdict::Undecided { visited } => {
            format!("{{\"kind\":\"undecided\",\"visited\":{visited}}}")
        }
        Verdict::NotLinearizable {
            operation_ids,
            visited,
        } => format!(
            "{{\"kind\":\"not-linearizable\",\"visited\":{},\"operation_ids\":[{}]}}",
            visited,
            operation_ids
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

#[derive(Debug)]
pub enum ClusterError {
    Run(RunError),
    Node { node: NodeId, error: NodeError },
    Network { from: NodeId, to: NodeId },
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(error) => error.fmt(f),
            Self::Node { node, error } => write!(f, "node {node}: {error}"),
            Self::Network { from, to } => write!(f, "network link missing {from}->{to}"),
        }
    }
}

impl std::error::Error for ClusterError {}

impl From<RunError> for ClusterError {
    fn from(error: RunError) -> Self {
        Self::Run(error)
    }
}

/// A deterministic host for the real `cc-cluster::Node` composition.
pub struct SimCluster {
    spec: RunSpec,
    now: Time,
    events: BinaryHeap<ClusterEvent>,
    next_tie_seq: u64,
    next_message_token: u64,
    next_operation_id: u64,
    processed_events: u64,
    total_issued: u64,
    had_leader: bool,
    recorder: Recorder,
    network: Network,
    nodes: BTreeMap<NodeId, NodeSlot>,
    voters: BTreeSet<NodeId>,
    messages: BTreeMap<u64, cc_raft::Message>,
    pending: BTreeMap<u64, PendingOperation>,
    actors: BTreeMap<u64, WorkloadActor>,
    history: History,
}

impl SimCluster {
    pub fn new(spec: RunSpec, level: RecorderLevel) -> Result<Self, ClusterError> {
        let node_count = usize::try_from(spec.config.node_count).unwrap_or(0);
        let node_ids: Vec<NodeId> = (1..=node_count as u64).map(NodeId::new).collect();
        let voters: BTreeSet<NodeId> = node_ids.iter().copied().collect();
        let network = Network::new(&node_ids, spec.seed, link_config(spec.profile));
        let mut nodes = BTreeMap::new();
        for id in &node_ids {
            let election_min =
                150_u64.saturating_add(id.get().saturating_sub(1).saturating_mul(35));
            let config = NodeConfig {
                id: *id,
                seed: Seed::new(spec.seed.0 ^ id.get().rotate_left(17)),
                raft: RaftConfig {
                    election_min: Duration::from_millis(election_min),
                    election_max: Duration::from_millis(election_min.saturating_add(20)),
                    ..RaftConfig::default()
                },
                store: StoreConfig::default(),
            };
            let node = Node::new(config, voters.clone())
                .map_err(|error| ClusterError::Node { node: *id, error })?;
            nodes.insert(
                *id,
                NodeSlot {
                    config,
                    node: Some(node),
                    status: cc_sim::NodeStatus::Up,
                    clock_offset: Duration::default(),
                    disk: SimDisk::new(),
                    armed_timers: BTreeMap::new(),
                    entry_offsets: BTreeMap::new(),
                    wal_end: WAL_HARD_STATE_LEN,
                },
            );
        }
        let mut cluster = Self {
            recorder: Recorder::new(spec.seed, level),
            spec,
            now: Time::from_nanos(0),
            events: BinaryHeap::new(),
            next_tie_seq: 0,
            next_message_token: 1,
            next_operation_id: 1,
            processed_events: 0,
            total_issued: 0,
            had_leader: false,
            network,
            nodes,
            voters,
            messages: BTreeMap::new(),
            pending: BTreeMap::new(),
            actors: BTreeMap::new(),
            history: History::default(),
        };
        cluster.seed_events();
        Ok(cluster)
    }

    pub fn run(mut self) -> Result<ClusterRun, ClusterError> {
        self.process_until(self.spec.end_time)?;
        self.finish_pending();
        self.finish_result()
    }

    /// Advance one persistent cluster by a virtual-time budget.
    pub fn advance(&mut self, budget: Duration) -> Result<ClusterSnapshot, ClusterError> {
        let target = (self.now + budget).min(self.spec.end_time);
        self.process_until(target)?;
        Ok(self.snapshot())
    }

    /// Append a fault to the persistent run specification at the current time.
    pub fn inject(&mut self, action: FaultAction) {
        self.spec.plan.push(FaultAt {
            at: self.now,
            action: action.clone(),
        });
        self.schedule(self.now, ClusterEventKind::Fault(action));
    }

    fn liveness_ok(&self) -> bool {
        let nodes: Vec<NodeId> = (1..=self.spec.config.node_count).map(NodeId::new).collect();
        let probe_committed = self
            .history
            .operations
            .iter()
            .any(|operation| operation.complete.is_some());
        check_liveness(LivenessReport {
            leader_seen: self.had_leader,
            probe_committed,
            survivable: self.spec.plan.is_survivable(&nodes),
        })
        .is_ok()
    }

    #[must_use]
    pub fn spec(&self) -> &RunSpec {
        &self.spec
    }

    #[must_use]
    pub fn snapshot(&self) -> ClusterSnapshot {
        let nodes = self
            .nodes
            .iter()
            .map(|(id, slot)| {
                let (role, term, commit, applied, log_tail, voters, joint) =
                    slot.node.as_ref().map_or(
                        (Role::Follower, 0, 0, 0, Vec::new(), Vec::new(), false),
                        |node| {
                            let (voters, _, joint) = node.membership();
                            (
                                node.role(),
                                node.raft.hard_state.term.get(),
                                node.raft.commit_index.get(),
                                node.raft.applied_index.get(),
                                node.raft
                                    .log
                                    .iter()
                                    .rev()
                                    .take(8)
                                    .map(|entry| entry.index.get())
                                    .collect(),
                                voters.iter().copied().map(NodeId::get).collect(),
                                joint,
                            )
                        },
                    );
                let durable_bytes = slot
                    .disk
                    .durable(WAL_FILE)
                    .map_or(0, |bytes| bytes.len() as u64);
                ClusterNodeSnapshot {
                    id: id.get(),
                    status: slot.status,
                    role,
                    term,
                    commit,
                    applied,
                    durable_bytes,
                    log_tail,
                    voters,
                    joint,
                }
            })
            .collect();
        let completed_operations = self
            .history
            .operations
            .iter()
            .filter(|operation| operation.complete.is_some())
            .count() as u64;
        ClusterSnapshot {
            virtual_time: self.now,
            trace: self.recorder.trace().clone(),
            nodes,
            history_len: self.history.operations.len(),
            completed_operations,
            had_leader: self.had_leader,
            liveness_ok: self.liveness_ok(),
            verdict: check(
                &self.history,
                CheckerConfig {
                    max_states: 100_000,
                },
            ),
        }
    }

    fn process_until(&mut self, limit: Time) -> Result<(), ClusterError> {
        while self.events.peek().is_some_and(|event| event.at <= limit) {
            let event = self
                .events
                .pop()
                .expect("invariant: peeked scheduler event exists");
            self.processed_events = self.processed_events.saturating_add(1);
            if self.processed_events > self.spec.config.max_events {
                return Err(ClusterError::Run(RunError::EventLimit {
                    limit: self.spec.config.max_events,
                }));
            }
            self.now = event.at;
            self.handle(event.kind)?;
        }
        self.now = limit;
        Ok(())
    }

    fn finish_result(self) -> Result<ClusterRun, ClusterError> {
        let trace = self.recorder.finish();
        let trace_invariants_ok = cc_checker::check_trace_invariants(&trace).is_ok()
            && self
                .nodes
                .values()
                .filter_map(|slot| slot.node.as_ref())
                .all(|node| node.raft.invariants().is_ok());
        let verdict = check(
            &self.history,
            CheckerConfig {
                max_states: 100_000,
            },
        );
        let completed_operations = self
            .history
            .operations
            .iter()
            .filter(|operation| operation.complete.is_some())
            .count() as u64;
        let nodes: Vec<NodeId> = (1..=self.spec.config.node_count).map(NodeId::new).collect();
        let liveness_ok = check_liveness(LivenessReport {
            leader_seen: self.had_leader,
            probe_committed: completed_operations > 0,
            survivable: self.spec.plan.is_survivable(&nodes),
        })
        .is_ok();
        let final_log_indices = self
            .nodes
            .iter()
            .map(|(id, slot)| {
                let (last, applied) = slot.node.as_ref().map_or((0, 0), |node| {
                    (node.raft.last_index().get(), node.raft.applied_index.get())
                });
                (id.get(), last, applied)
            })
            .collect();
        let spec = self.spec;
        let seed = spec.seed;
        Ok(ClusterRun {
            spec,
            seed,
            trace,
            history: self.history,
            verdict,
            trace_invariants_ok,
            had_leader: self.had_leader,
            completed_operations,
            event_count: self.processed_events,
            final_log_indices,
            liveness_ok,
        })
    }

    fn seed_events(&mut self) {
        for fault in self.spec.plan.actions.clone() {
            self.schedule(fault.at, ClusterEventKind::Fault(fault.action));
        }
        let mut at = Time::from_nanos(0);
        while at <= self.spec.end_time {
            for node in self.nodes.keys().copied().collect::<Vec<_>>() {
                self.schedule(at, ClusterEventKind::Tick(node));
            }
            at = at + TICK_INTERVAL;
        }
        for client in 1..=self.spec.workload.clients {
            self.actors.insert(
                client,
                WorkloadActor::new(client, self.spec.seed, self.spec.workload.clone()),
            );
            if let Some(actor) = self.actors.get_mut(&client) {
                let (sequence, operation) = actor.next_operation();
                let at = Time::from_nanos(
                    FIRST_CLIENT_TIME
                        .as_nanos()
                        .saturating_add(client.saturating_mul(1_000_000)),
                );
                self.schedule(
                    at,
                    ClusterEventKind::ClientIssue {
                        client,
                        sequence,
                        operation,
                    },
                );
            }
        }
    }

    fn schedule(&mut self, at: Time, kind: ClusterEventKind) {
        if at <= self.spec.end_time {
            let tie_seq = self.next_tie_seq;
            self.next_tie_seq = self.next_tie_seq.saturating_add(1);
            self.events.push(ClusterEvent { at, tie_seq, kind });
        }
    }

    fn handle(&mut self, event: ClusterEventKind) -> Result<(), ClusterError> {
        match event {
            ClusterEventKind::Tick(node) => self.handle_tick(node),
            ClusterEventKind::Timer { node, id, kind } => self.handle_timer(node, id, kind),
            ClusterEventKind::Message { token, from, to } => self.handle_message(token, from, to),
            ClusterEventKind::ClientIssue {
                client,
                sequence,
                operation,
            } => self.handle_client_issue(client, sequence, operation),
            ClusterEventKind::ClientTimeout(id) => {
                self.handle_timeout(id);
                Ok(())
            }
            ClusterEventKind::Fault(action) => {
                self.handle_fault(action)?;
                Ok(())
            }
            ClusterEventKind::LeaveJoint(node) => self.handle_leave_joint(node),
        }
    }

    /// Close a joint transition on the node that opened it, provided it is
    /// still up and still leading. If it is not, the joint config stays open
    /// and the next leader inherits it through the log.
    fn handle_leave_joint(&mut self, node: NodeId) -> Result<(), ClusterError> {
        if !self.is_up(node) {
            return Ok(());
        }
        let leading_in_joint = self
            .nodes
            .get(&node)
            .and_then(|slot| slot.node.as_ref())
            .is_some_and(|node| node.role() == Role::Leader && node.membership().2);
        if !leading_in_joint {
            return Ok(());
        }
        let effects = {
            let slot = self
                .nodes
                .get_mut(&node)
                .expect("invariant: node slot exists");
            let composition = slot
                .node
                .as_mut()
                .expect("invariant: up node has a composition");
            composition
                .leave_joint()
                .map_err(|error| ClusterError::Node { node, error })?
        };
        self.record(self.now, Some(node), EventKind::ConfChange, Vec::new());
        self.consume_effects(node, effects)
    }

    fn handle_tick(&mut self, id: NodeId) -> Result<(), ClusterError> {
        if self.is_up(id) {
            let input_time = self.host_time(id);
            self.drive_node(id, NodeInput::Tick { now: input_time })?;
        }
        Ok(())
    }

    fn handle_timer(
        &mut self,
        id: NodeId,
        timer_id: TimerId,
        kind: TimerKind,
    ) -> Result<(), ClusterError> {
        let armed = self
            .nodes
            .get(&id)
            .and_then(|slot| slot.armed_timers.get(&timer_id).copied());
        if armed != Some(self.now) || !self.is_up(id) {
            return Ok(());
        }
        let input_time = self.host_time(id);
        self.drive_node(
            id,
            NodeInput::Timer {
                now: input_time,
                kind,
            },
        )
    }

    fn handle_message(&mut self, token: u64, from: NodeId, to: NodeId) -> Result<(), ClusterError> {
        let Some(message) = self.messages.get(&token).cloned() else {
            return Ok(());
        };
        self.messages.remove(&token);
        self.network
            .complete(from, to)
            .map_err(|_| ClusterError::Network { from, to })?;
        if !self.is_up(to) {
            self.record(
                self.now,
                Some(to),
                EventKind::NetDrop,
                token.to_le_bytes().to_vec(),
            );
            return Ok(());
        }
        self.record(
            self.now,
            Some(to),
            EventKind::NetRecv,
            message_fingerprint(&message),
        );
        self.drive_node(
            to,
            NodeInput::MessageAt {
                now: self.host_time(to),
                message,
            },
        )
    }

    fn handle_client_issue(
        &mut self,
        client: u64,
        sequence: u64,
        operation: WorkloadOperation,
    ) -> Result<(), ClusterError> {
        if self.total_issued >= MAX_OPERATIONS_PER_RUN {
            return Ok(());
        }
        let Some(leader) = self.leader() else {
            self.schedule(
                self.now + CLIENT_RETRY,
                ClusterEventKind::ClientIssue {
                    client,
                    sequence,
                    operation,
                },
            );
            return Ok(());
        };
        let command = command_for(&operation);
        let kind = operation_kind(&operation);
        let id = self.next_operation_id;
        self.next_operation_id = self.next_operation_id.saturating_add(1);
        let mut history_operation = Operation::open(id, kind, self.now);
        history_operation.client = client;
        history_operation.sequence = sequence;
        self.pending.insert(
            id,
            PendingOperation {
                operation: history_operation,
                client,
            },
        );
        self.total_issued = self.total_issued.saturating_add(1);
        self.record(
            self.now,
            Some(leader),
            EventKind::ClientInvoke,
            id.to_le_bytes().to_vec(),
        );
        // Reads go through the ReadIndex barrier rather than the log, so the
        // quorum-confirmation round is exercised by every campaign instead of
        // living only in unit tests.
        let input = if matches!(operation, WorkloadOperation::Get { .. }) {
            NodeInput::Read {
                client: ClientId::new(client),
                sequence,
                command,
                at: self.host_time(leader),
            }
        } else {
            NodeInput::ClientRequest {
                client: ClientId::new(client),
                sequence,
                command,
                leader_time: self.host_time(leader),
            }
        };
        match self.drive_node(leader, input) {
            Ok(()) => {
                self.schedule(
                    self.now + CLIENT_TIMEOUT,
                    ClusterEventKind::ClientTimeout(id),
                );
            }
            Err(ClusterError::Node {
                error:
                    NodeError::NotLeader
                    | NodeError::Raft(
                        cc_raft::RaftError::Busy
                        | cc_raft::RaftError::NotLeader
                        | cc_raft::RaftError::ReadBarrierNotReady,
                    ),
                ..
            }) => {
                self.pending.remove(&id);
                self.schedule(
                    self.now + CLIENT_RETRY,
                    ClusterEventKind::ClientIssue {
                        client,
                        sequence,
                        operation,
                    },
                );
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    fn handle_timeout(&mut self, id: u64) {
        let Some(mut pending) = self.pending.remove(&id) else {
            return;
        };
        pending.operation.complete = None;
        pending.operation.outcome = Outcome::Timeout;
        self.history.push(pending.operation);
        self.record(
            self.now,
            None,
            EventKind::ClientTimeout,
            id.to_le_bytes().to_vec(),
        );
        self.schedule_next(pending.client);
    }

    fn handle_fault(&mut self, action: FaultAction) -> Result<(), ClusterError> {
        self.record(
            self.now,
            None,
            EventKind::Fault,
            format!("{action:?}").into_bytes(),
        );
        match action {
            FaultAction::Partition { left, right } => {
                for from in &left {
                    for to in &right {
                        self.set_blocked(*from, *to, true)?;
                        self.set_blocked(*to, *from, true)?;
                    }
                }
            }
            FaultAction::Heal => {
                let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                for from in &ids {
                    for to in &ids {
                        if from != to {
                            self.set_blocked(*from, *to, false)?;
                        }
                    }
                }
            }
            FaultAction::Crash { node } => {
                // A crash is a process death: every byte of volatile state goes
                // with it. Dropping the composition is what makes the restart
                // path exercise recovery instead of resuming a paused node.
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.status = cc_sim::NodeStatus::Crashed;
                    slot.node = None;
                    slot.armed_timers.clear();
                    slot.disk.crash();
                }
            }
            FaultAction::Restart { node } => {
                if self
                    .nodes
                    .get(&node)
                    .is_some_and(|slot| slot.status == cc_sim::NodeStatus::Up)
                {
                    return Ok(());
                }
                // A wiped node has no durable bytes to recover from, so
                // `recover_node` rebuilds it empty. Catching it up is the
                // leader's job, and it needs state transfer rather than log
                // replay — the same path the real host uses when it installs a
                // journal snapshot into a cleared data directory.
                let was_wiped = self
                    .nodes
                    .get(&node)
                    .is_some_and(|slot| slot.status == cc_sim::NodeStatus::Wiped);
                self.recover_node(node)?;
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.status = cc_sim::NodeStatus::Up;
                }
                if was_wiped {
                    self.install_leader_snapshot(node)?;
                }
                self.schedule(self.now, ClusterEventKind::Tick(node));
            }
            FaultAction::Wipe { node } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.status = cc_sim::NodeStatus::Wiped;
                    slot.node = None;
                    slot.armed_timers.clear();
                    slot.disk = SimDisk::new();
                    slot.reset_wal();
                }
                // A wipe destroys durable state. That is disk loss, and the
                // trace has to say so or the `disk-loss` beacon reports a
                // profile whose entire purpose is losing a disk as never having
                // lost one.
                self.record(self.now, Some(node), EventKind::IoLost, Vec::new());
            }
            FaultAction::ClockSkew { node, offset } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.clock_offset = offset;
                }
            }
            FaultAction::DiskDegrade { node, .. } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.disk.inject(DiskFault::EioNextWrite);
                }
            }
            FaultAction::LinkDegrade { from, to, config } => {
                self.network
                    .configure(from, to, config)
                    .map_err(|_| ClusterError::Network { from, to })?;
            }
            FaultAction::Reconfigure { voters } => {
                let Some(leader) = self.leader() else {
                    return Ok(());
                };
                let target: BTreeSet<NodeId> = voters.into_iter().collect();
                if target.is_empty() {
                    return Ok(());
                }
                let effects = {
                    let slot = self
                        .nodes
                        .get_mut(&leader)
                        .expect("invariant: leader slot exists");
                    let node = slot
                        .node
                        .as_mut()
                        .expect("invariant: leader has a composition");
                    if node.membership().0 == target || node.membership().2 {
                        // Already there, or a transition is still open.
                        return Ok(());
                    }
                    match node.enter_joint(target) {
                        Ok(effects) => effects,
                        // A leader that cannot open a transition right now is a
                        // legitimate outcome, not a host error.
                        Err(NodeError::Raft(_)) => return Ok(()),
                        Err(error) => {
                            return Err(ClusterError::Node {
                                node: leader,
                                error,
                            });
                        }
                    }
                };
                self.record(self.now, Some(leader), EventKind::ConfChange, Vec::new());
                self.consume_effects(leader, effects)?;
                self.schedule(
                    self.now + JOINT_SETTLE,
                    ClusterEventKind::LeaveJoint(leader),
                );
            }
        }
        Ok(())
    }

    fn drive_node(&mut self, id: NodeId, input: NodeInput) -> Result<(), ClusterError> {
        let (before_role, effects) = {
            let slot = self
                .nodes
                .get_mut(&id)
                .expect("invariant: node slot exists");
            let node = slot
                .node
                .as_mut()
                .expect("invariant: up node has a composition");
            let before = node.role();
            let effects = node
                .on_input(input)
                .map_err(|error| ClusterError::Node { node: id, error })?;
            (before, effects)
        };
        let after_role = self
            .nodes
            .get(&id)
            .and_then(|slot| slot.node.as_ref())
            .map(Node::role)
            .unwrap_or(Role::Follower);
        if before_role != after_role {
            self.record(
                self.now,
                Some(id),
                EventKind::RoleChange,
                format!("{}>{}", role_name(before_role), role_name(after_role)).into_bytes(),
            );
            if after_role == Role::Leader {
                self.had_leader = true;
            }
        }
        self.consume_effects(id, effects)
    }

    fn consume_effects(
        &mut self,
        source: NodeId,
        effects: Vec<NodeEffect>,
    ) -> Result<(), ClusterError> {
        for effect in effects {
            match effect {
                NodeEffect::Send(message) => self.send_message(message)?,
                NodeEffect::PersistHard(hard) => {
                    let mut bytes = Vec::with_capacity(WAL_HARD_STATE_LEN as usize);
                    bytes.extend_from_slice(&WAL_HARD_STATE_VERSION.to_le_bytes());
                    bytes.extend_from_slice(&hard.term.get().to_le_bytes());
                    bytes.extend_from_slice(&hard.voted_for.map_or(0, NodeId::get).to_le_bytes());
                    self.persist(source, 0, &bytes);
                }
                NodeEffect::PersistEntries(entries) => self.append_entries(source, &entries),
                NodeEffect::TruncateSuffix(index) => self.truncate_from(source, index.get()),
                NodeEffect::ClientReply {
                    client,
                    sequence,
                    reply,
                } => {
                    if self
                        .nodes
                        .get(&source)
                        .and_then(|slot| slot.node.as_ref())
                        .is_some_and(|node| node.role() == Role::Leader)
                    {
                        self.complete_client(source, client.get(), sequence, reply);
                    }
                }
                NodeEffect::ReadReply {
                    client,
                    sequence,
                    reply,
                } => {
                    // The sequence matters: a read whose barrier completes after
                    // its client already timed out must not be matched against
                    // whatever that client issued next.
                    self.complete_client(source, client.get(), sequence, reply);
                }
                NodeEffect::ArmTimer { id, at, kind } => {
                    if let Some(slot) = self.nodes.get_mut(&source) {
                        slot.armed_timers.insert(id, at);
                    }
                    self.record(
                        self.now,
                        Some(source),
                        EventKind::TimerSet,
                        at.as_nanos().to_le_bytes().to_vec(),
                    );
                    self.schedule(
                        at,
                        ClusterEventKind::Timer {
                            node: source,
                            id,
                            kind,
                        },
                    );
                }
                NodeEffect::Trace(name) => {
                    self.record(
                        self.now,
                        Some(source),
                        EventKind::CheckerNote,
                        name.as_bytes().to_vec(),
                    );
                }
            }
        }
        Ok(())
    }

    fn send_message(&mut self, message: cc_raft::Message) -> Result<(), ClusterError> {
        let from = message.from;
        let to = message.to;
        let token = self.next_message_token;
        self.next_message_token = self.next_message_token.saturating_add(1);
        self.messages.insert(token, message.clone());
        if !self.is_up(from) || !self.is_up(to) {
            self.messages.remove(&token);
            self.record(
                self.now,
                Some(from),
                EventKind::NetDrop,
                message_fingerprint(&message),
            );
            return Ok(());
        }
        let decisions = self
            .network
            .send(self.now, from, to, token.to_le_bytes().to_vec())
            .map_err(|_| ClusterError::Network { from, to })?;
        let mut delivered = false;
        for decision in decisions {
            match decision {
                NetworkDecision::Delivered(delivery) => {
                    delivered = true;
                    self.record(
                        self.now,
                        Some(from),
                        EventKind::NetSend,
                        message_fingerprint(&message),
                    );
                    self.schedule(delivery.at, ClusterEventKind::Message { token, from, to });
                }
                NetworkDecision::Dropped => {
                    self.record(
                        self.now,
                        Some(from),
                        EventKind::NetDrop,
                        message_fingerprint(&message),
                    );
                }
            }
        }
        if !delivered {
            self.messages.remove(&token);
        }
        Ok(())
    }

    fn persist(&mut self, node: NodeId, offset: u64, bytes: &[u8]) {
        self.record(self.now, Some(node), EventKind::IoIssue, bytes.to_vec());
        let result = self
            .nodes
            .get_mut(&node)
            .map(|slot| slot.disk.write(WAL_FILE, offset, bytes));
        if result.is_some_and(|result| result.is_ok()) {
            let synced = self
                .nodes
                .get_mut(&node)
                .map(|slot| slot.disk.fsync(WAL_FILE));
            if synced.is_some_and(|result| result.is_ok()) {
                self.record(self.now, Some(node), EventKind::IoDone, Vec::new());
                self.record(self.now, Some(node), EventKind::Flush, Vec::new());
            } else {
                self.record(self.now, Some(node), EventKind::IoLost, Vec::new());
            }
        } else {
            self.record(self.now, Some(node), EventKind::IoLost, Vec::new());
        }
    }

    /// Append log records at the byte offset the first entry index maps to. An
    /// index already on disk means this append conflicts with a stale suffix,
    /// so the log rewinds to that offset and overwrites from there.
    fn append_entries(&mut self, node: NodeId, entries: &[Entry]) {
        let Some(first) = entries.first() else {
            return;
        };
        let Some(slot) = self.nodes.get_mut(&node) else {
            return;
        };
        let start = slot
            .entry_offsets
            .get(&first.index.get())
            .copied()
            .unwrap_or(slot.wal_end);
        slot.entry_offsets
            .retain(|index, _| *index < first.index.get());
        let mut offset = start;
        let mut bytes = Vec::new();
        for entry in entries {
            slot.entry_offsets.insert(entry.index.get(), offset);
            let encoded = encode_entry(entry);
            offset = offset.saturating_add(encoded.len() as u64);
            bytes.extend_from_slice(&encoded);
        }
        slot.wal_end = offset;
        self.persist(node, start, &bytes);
    }

    /// Drop every log record at or after `index`, as Raft's conflict path asks.
    fn truncate_from(&mut self, node: NodeId, index: u64) {
        let Some(slot) = self.nodes.get_mut(&node) else {
            return;
        };
        let end = slot
            .entry_offsets
            .get(&index)
            .copied()
            .unwrap_or(slot.wal_end);
        slot.entry_offsets.retain(|existing, _| *existing < index);
        slot.wal_end = end;
        let result = slot.disk.truncate(WAL_FILE, end);
        self.record(
            self.now,
            Some(node),
            EventKind::IoIssue,
            index.to_le_bytes().to_vec(),
        );
        if result.is_ok() {
            self.record(self.now, Some(node), EventKind::IoDone, Vec::new());
        } else {
            self.record(self.now, Some(node), EventKind::IoLost, Vec::new());
        }
    }

    /// Rebuild a node from whatever survived on its disk. Everything volatile
    /// is gone; the log is replayed by Raft once a leader re-establishes commit.
    /// Transfer the leader's applied state to a node that came back with an
    /// empty disk.
    ///
    /// This is a real state transfer: the leader's own `create_snapshot` output
    /// is installed into the target's `Node`, carrying the KV image and the
    /// `last_included` index/term that `install_snapshot_state` uses to retire
    /// the log prefix. It is *modelled* rather than chunked over the network —
    /// `cc-raft` can frame `SnapshotChunk` messages, but `cc-cluster::Node`
    /// does not intercept them, so routing chunks would move raft's indices
    /// without the state machine bytes and leave the follower confidently
    /// wrong. `docs/LIMITATIONS.md` records that boundary.
    fn install_leader_snapshot(&mut self, target: NodeId) -> Result<(), ClusterError> {
        let Some(leader) = self.leader() else {
            return Ok(());
        };
        if leader == target {
            return Ok(());
        }
        let snapshot = {
            let Some(slot) = self.nodes.get_mut(&leader) else {
                return Ok(());
            };
            let Some(node) = slot.node.as_mut() else {
                return Ok(());
            };
            node.create_snapshot().map_err(|error| ClusterError::Node {
                node: leader,
                error,
            })?
        };
        let last_included = snapshot.last_included_index;
        let Some(slot) = self.nodes.get_mut(&target) else {
            return Ok(());
        };
        let Some(node) = slot.node.as_mut() else {
            return Ok(());
        };
        node.install_snapshot(snapshot)
            .map_err(|error| ClusterError::Node {
                node: target,
                error,
            })?;
        self.record(
            self.now,
            Some(target),
            EventKind::SnapshotInstall,
            last_included.get().to_le_bytes().to_vec(),
        );
        Ok(())
    }

    fn recover_node(&mut self, id: NodeId) -> Result<(), ClusterError> {
        let voters = self.voters.clone();
        let now = self.host_time(id);
        let Some(slot) = self.nodes.get_mut(&id) else {
            return Ok(());
        };
        let durable = slot.disk.durable(WAL_FILE).unwrap_or_default().to_vec();
        let (hard_state, entries, offsets, wal_end) = decode_wal(&durable);
        slot.entry_offsets = offsets;
        slot.wal_end = wal_end;
        slot.node = Some(
            Node::recover(slot.config, voters, hard_state, entries, now)
                .map_err(|error| ClusterError::Node { node: id, error })?,
        );
        self.record(
            self.now,
            Some(id),
            EventKind::WalRecover,
            durable.len().to_le_bytes().to_vec(),
        );
        Ok(())
    }

    fn complete_client(&mut self, source: NodeId, client: u64, sequence: u64, reply: KvReply) {
        let id = self
            .pending
            .iter()
            .find(|(_, pending)| pending.client == client && pending.operation.sequence == sequence)
            .map(|(id, _)| *id);
        let Some(id) = id else {
            return;
        };
        let Some(mut pending) = self.pending.remove(&id) else {
            return;
        };
        pending.operation.complete = Some(self.now);
        pending.operation.outcome = reply_to_outcome(&pending.operation.kind, &reply);
        self.history.push(pending.operation);
        self.record(
            self.now,
            Some(source),
            EventKind::ClientOk,
            id.to_le_bytes().to_vec(),
        );
        self.schedule_next(client);
    }

    fn schedule_next(&mut self, client: u64) {
        if self.total_issued >= MAX_OPERATIONS_PER_RUN {
            return;
        }
        let Some(actor) = self.actors.get_mut(&client) else {
            return;
        };
        let (sequence, operation) = actor.next_operation();
        self.schedule(
            self.now + CLIENT_RETRY,
            ClusterEventKind::ClientIssue {
                client,
                sequence,
                operation,
            },
        );
    }

    fn finish_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        for (_, mut operation) in pending {
            operation.operation.complete = None;
            operation.operation.outcome = Outcome::Timeout;
            self.history.push(operation.operation);
        }
    }

    fn set_blocked(&mut self, from: NodeId, to: NodeId, blocked: bool) -> Result<(), ClusterError> {
        self.network
            .set_blocked(from, to, blocked)
            .map_err(|_| ClusterError::Network { from, to })
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .filter_map(|(id, slot)| {
                if slot.status != cc_sim::NodeStatus::Up {
                    return None;
                }
                let node = slot.node.as_ref()?;
                (node.role() == Role::Leader).then_some((*id, node.raft.hard_state.term.get()))
            })
            .max_by_key(|(id, term)| (*term, *id))
            .map(|(id, _)| id)
    }

    fn is_up(&self, id: NodeId) -> bool {
        self.nodes
            .get(&id)
            .is_some_and(|slot| slot.status == cc_sim::NodeStatus::Up && slot.node.is_some())
    }

    fn host_time(&self, id: NodeId) -> Time {
        self.nodes
            .get(&id)
            .map_or(self.now, |slot| self.now + slot.clock_offset)
    }

    fn record(&mut self, time: Time, node: Option<NodeId>, kind: EventKind, payload: Vec<u8>) {
        self.recorder.record(time, node, kind, payload);
    }
}

pub fn run_spec(spec: RunSpec, level: RecorderLevel) -> Result<ClusterRun, ClusterError> {
    SimCluster::new(spec, level)?.run()
}

/// Profiles the determinism gate sweeps. A fault-free run is the weakest
/// possible determinism check: crash, restart, wipe and reconfigure are where
/// ordering bugs actually hide, so every one of them is covered.
pub const DETERMINISM_PROFILES: [FaultProfile; 5] = [
    FaultProfile::Calm,
    FaultProfile::Rough,
    FaultProfile::Membership,
    FaultProfile::Wipe,
    FaultProfile::Brutal,
];

#[must_use]
pub fn deterministic_cluster_trace(seed: Seed) -> Vec<u8> {
    deterministic_cluster_trace_for(seed, FaultProfile::Rough)
}

#[must_use]
pub fn deterministic_cluster_trace_for(seed: Seed, profile: FaultProfile) -> Vec<u8> {
    let config = SimConfig {
        end_time: Time::from_nanos(3_000_000_000),
        max_events: 250_000,
        max_events_per_instant: 10_000,
        node_count: 5,
    };
    let nodes: Vec<NodeId> = (1..=config.node_count).map(NodeId::new).collect();
    let mut spec = RunSpec::standard(seed, profile);
    spec.end_time = config.end_time;
    spec.plan = cc_sim::materialize_fault_plan(seed, profile, &nodes, config.end_time);
    spec.config = config;
    run_spec(spec, RecorderLevel::Gate)
        .expect("invariant: deterministic cluster fixture must finish")
        .trace
        .encode()
}

/// Apply the real run oracle while retaining the existing simulator reducer.
#[must_use]
pub fn reproduces_failure(spec: &RunSpec) -> bool {
    run_spec(spec.clone(), RecorderLevel::Campaign)
        .map(|run| !run.healthy() || matches!(run.verdict, Verdict::NotLinearizable { .. }))
        .unwrap_or(true)
}

/// Canonicalize, ddmin, truncate, thin, then canonicalize three more times.
#[must_use]
pub fn shrink_cluster_plan(spec: &RunSpec) -> FaultPlan {
    let mut candidate = canonicalize_fault_plan(&spec.plan);
    candidate = shrink_fault_plan(&candidate, |plan| {
        let mut trial = spec.clone();
        trial.plan = plan.clone();
        reproduces_failure(&trial)
    });
    let mut index = candidate.actions.len().saturating_sub(1);
    while index > 0 {
        let trial = FaultPlan {
            actions: candidate.actions[..index].to_vec(),
        };
        let mut spec_trial = spec.clone();
        spec_trial.plan = trial.clone();
        if reproduces_failure(&spec_trial) {
            candidate = trial;
            index = candidate.actions.len();
        } else {
            index -= 1;
        }
    }
    let mut thin = candidate.clone();
    let mut cursor = 0;
    while cursor < thin.actions.len() {
        let mut trial = thin.clone();
        trial.actions.remove(cursor);
        let mut spec_trial = spec.clone();
        spec_trial.plan = trial.clone();
        if reproduces_failure(&spec_trial) {
            thin = trial;
        } else {
            cursor += 1;
        }
    }
    for _ in 0..3 {
        thin = canonicalize_fault_plan(&thin);
    }
    thin
}

fn link_config(profile: FaultProfile) -> LinkConfig {
    let mut config = LinkConfig::default();
    match profile {
        FaultProfile::Calm => {}
        FaultProfile::Gentle => {
            config.base_delay = Duration::from_millis(2);
            config.drop = cc_core::P16::new(128);
            config.duplicate = cc_core::P16::new(64);
        }
        FaultProfile::Rough | FaultProfile::Membership | FaultProfile::Corruption => {
            config.base_delay = Duration::from_millis(2);
            config.jitter = cc_core::DelayDist::Uniform {
                low: Duration::default(),
                high: Duration::from_millis(3),
            };
            config.drop = cc_core::P16::new(256);
            config.duplicate = cc_core::P16::new(128);
        }
        FaultProfile::Brutal | FaultProfile::Wipe => {
            config.base_delay = Duration::from_millis(3);
            config.drop = cc_core::P16::new(512);
            config.duplicate = cc_core::P16::new(256);
        }
    }
    config
}

fn command_for(operation: &WorkloadOperation) -> KvCommand {
    match operation {
        WorkloadOperation::Get { key } => KvCommand::Get { key: key.clone() },
        WorkloadOperation::Set { key, value } => KvCommand::Set {
            key: key.clone(),
            value: value.clone(),
            ttl: None,
        },
        WorkloadOperation::Del { key } => KvCommand::Del { key: key.clone() },
        WorkloadOperation::Incr { key } => KvCommand::Incr {
            key: key.clone(),
            delta: 1,
        },
    }
}

fn operation_kind(operation: &WorkloadOperation) -> OperationKind {
    match operation {
        WorkloadOperation::Get { key } => OperationKind::Get { key: key.clone() },
        WorkloadOperation::Set { key, value } => OperationKind::Set {
            key: key.clone(),
            value: value.clone(),
        },
        WorkloadOperation::Del { key } => OperationKind::Del { key: key.clone() },
        WorkloadOperation::Incr { key } => OperationKind::Incr { key: key.clone() },
    }
}

fn reply_to_outcome(kind: &OperationKind, reply: &KvReply) -> Outcome {
    match (kind, reply) {
        (_, KvReply::Error(_)) => Outcome::Error,
        (OperationKind::Set { .. }, KvReply::Ok) => Outcome::Ok,
        (OperationKind::Del { .. }, KvReply::Integer(_)) => Outcome::Ok,
        (OperationKind::Get { .. }, KvReply::Value(value)) => Outcome::Value(value.clone()),
        (OperationKind::Incr { .. }, KvReply::Integer(value)) => Outcome::Integer(*value),
        (_, KvReply::Ok) => Outcome::Ok,
        (_, KvReply::Value(value)) => Outcome::Value(value.clone()),
        (_, KvReply::Integer(value)) => Outcome::Integer(*value),
        (_, KvReply::Cas(value)) => Outcome::Cas(*value),
        (_, KvReply::Scan(_)) => Outcome::Error,
    }
}

fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(25 + entry.payload.len());
    bytes.extend_from_slice(&entry.term.get().to_le_bytes());
    bytes.extend_from_slice(&entry.index.get().to_le_bytes());
    bytes.push(entry.kind as u8);
    bytes.extend_from_slice(&(entry.payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&entry.payload);
    bytes
}

/// Decode durable WAL bytes back into hard state and a log prefix.
///
/// Decoding stops at the first record that is short or malformed, which is the
/// prefix-durability rule: a torn tail is simply not part of the recovered log.
fn decode_wal(durable: &[u8]) -> (cc_raft::HardState, Vec<Entry>, BTreeMap<u64, u64>, u64) {
    let mut hard = cc_raft::HardState {
        term: cc_core::Term::new(0),
        voted_for: None,
    };
    let header = WAL_HARD_STATE_LEN as usize;
    if durable.len() >= header && read_u64(durable, 0) == WAL_HARD_STATE_VERSION {
        hard.term = cc_core::Term::new(read_u64(durable, 8));
        let voted = read_u64(durable, 16);
        hard.voted_for = (voted != 0).then(|| NodeId::new(voted));
    }
    let mut entries = Vec::new();
    let mut offsets = BTreeMap::new();
    let mut cursor = header.min(durable.len());
    let mut end = WAL_HARD_STATE_LEN;
    while cursor + 25 <= durable.len() {
        let term = read_u64(durable, cursor);
        let index = read_u64(durable, cursor + 8);
        let Some(kind) = entry_kind(durable[cursor + 16]) else {
            break;
        };
        let len = read_u64(durable, cursor + 17) as usize;
        let start = cursor + 25;
        let Some(payload) = durable.get(start..start.saturating_add(len)) else {
            break;
        };
        offsets.insert(index, cursor as u64);
        entries.push(Entry {
            term: cc_core::Term::new(term),
            index: cc_core::LogIndex::new(index),
            kind,
            payload: payload.to_vec(),
        });
        cursor = start.saturating_add(len);
        end = cursor as u64;
    }
    (hard, entries, offsets, end)
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    bytes
        .get(at..at + 8)
        .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
        .map_or(0, u64::from_le_bytes)
}

const fn entry_kind(tag: u8) -> Option<cc_raft::EntryKind> {
    match tag {
        1 => Some(cc_raft::EntryKind::App),
        2 => Some(cc_raft::EntryKind::Noop),
        3 => Some(cc_raft::EntryKind::Config),
        _ => None,
    }
}

fn message_fingerprint(message: &cc_raft::Message) -> Vec<u8> {
    format!(
        "{}>{}:{}:{:?}",
        message.from.get(),
        message.to.get(),
        message.term.get(),
        message.kind
    )
    .into_bytes()
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
        Role::Learner => "learner",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calm_five_node_cluster_elects_and_captures_real_history() {
        let mut spec = RunSpec::standard(Seed::new(0x51), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(3_000_000_000);
        spec.end_time = spec.config.end_time;
        let run = run_spec(spec, RecorderLevel::Gate).expect("cluster");
        assert!(run.had_leader);
        assert!(run.trace_invariants_ok);
        assert!(run.liveness_ok);
        assert!(run.healthy());
        assert!(!run.history.operations.is_empty());
        assert!(!run.trace.events.is_empty());
    }

    #[test]
    fn semantic_diff_reports_the_first_changed_field() {
        let mut left = Trace::new(Seed::new(1), 0);
        left.push(
            Time::from_nanos(7),
            Some(NodeId::new(1)),
            EventKind::Commit,
            vec![1],
        );
        let mut right = left.clone();
        right.events[0].payload = vec![2];
        let difference = semantic_trace_diff(&left, &right).expect("difference");
        assert_eq!(difference.event_index, 0);
        assert_eq!(difference.field, "payload_hex");
        assert_eq!(difference.left, "01");
        assert_eq!(difference.right, "02");
        assert!(semantic_trace_diff(&left, &left).is_none());
    }

    #[test]
    fn sequence_diagram_is_a_standalone_svg() {
        let mut trace = Trace::new(Seed::new(1), 0);
        trace.push(
            Time::from_nanos(1),
            Some(NodeId::new(1)),
            EventKind::NetSend,
            b"1>2:1:append".to_vec(),
        );
        let svg = sequence_diagram_svg(&trace);
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("n1"));
        assert!(svg.contains("class=\"msg\""));
        assert!(svg.ends_with("</svg>"));
    }

    #[test]
    fn sometimes_beacons_count_reached_states() {
        let mut trace = Trace::new(Seed::new(1), 0);
        trace.push(
            Time::from_nanos(1),
            Some(NodeId::new(1)),
            EventKind::RoleChange,
            b"candidate>leader".to_vec(),
        );
        trace.push(Time::from_nanos(2), None, EventKind::NetDrop, Vec::new());
        let hits = reachability_beacons(&trace, true);
        assert_eq!(hits[0], 1);
        assert_eq!(hits[1], 1);
        assert_eq!(hits[2], 0);
    }

    #[test]
    fn coverage_and_plan_mutation_are_deterministic() {
        let mut trace = Trace::new(Seed::new(1), 0);
        trace.push(
            Time::from_nanos(1),
            None,
            EventKind::Fault,
            b"crash".to_vec(),
        );
        trace.push(
            Time::from_nanos(2),
            None,
            EventKind::RoleChange,
            b"leader".to_vec(),
        );
        assert_eq!(trace_coverage(&trace).len(), 3);
        let plan = FaultPlan {
            actions: vec![FaultAt {
                at: Time::from_nanos(50),
                action: FaultAction::Heal,
            }],
        };
        assert_eq!(
            mutate_fault_plan(&plan, Seed::new(7), Time::from_nanos(100)),
            mutate_fault_plan(&plan, Seed::new(7), Time::from_nanos(100))
        );
    }

    #[test]
    fn scripted_leader_crash_restart_catches_up_from_surviving_cluster() {
        let mut spec = RunSpec::standard(Seed::new(0x54), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(6_000_000_000);
        spec.end_time = spec.config.end_time;
        spec.plan = FaultPlan {
            actions: vec![
                cc_sim::FaultAt {
                    at: Time::from_nanos(2_000_000_000),
                    action: FaultAction::Crash {
                        node: NodeId::new(1),
                    },
                },
                cc_sim::FaultAt {
                    at: Time::from_nanos(4_000_000_000),
                    action: FaultAction::Restart {
                        node: NodeId::new(1),
                    },
                },
            ],
        };
        let run = run_spec(spec, RecorderLevel::Gate).expect("cluster");
        assert!(run.had_leader);
        assert!(run.trace_invariants_ok);
        assert!(run.completed_operations > 0);
        let max_last = run
            .final_log_indices
            .iter()
            .map(|(_, last, _)| *last)
            .max()
            .unwrap_or(0);
        let restarted = run
            .final_log_indices
            .iter()
            .find(|(id, _, _)| *id == 1)
            .expect("restarted node remains in cluster");
        assert_eq!(restarted.1, max_last);
        assert_eq!(restarted.2, restarted.1);
    }

    /// The wipe wing's actual claim. A wiped node loses every durable byte, so
    /// there is no log to replay and no prefix for the leader to append onto:
    /// the only way back is state transfer. Before this was wired the `wipe`
    /// profile wiped node 1 and never restarted it, so the profile proved the
    /// cluster survived losing a disk and nothing about the node rejoining —
    /// and `EventKind::SnapshotInstall` was emitted nowhere in the workspace.
    #[test]
    fn wiped_node_rejoins_by_installing_the_leader_snapshot() {
        let mut spec = RunSpec::standard(Seed::new(0x9c), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(10_000_000_000);
        spec.end_time = spec.config.end_time;
        spec.plan = FaultPlan {
            actions: vec![
                cc_sim::FaultAt {
                    at: Time::from_nanos(3_000_000_000),
                    action: FaultAction::Wipe {
                        node: NodeId::new(2),
                    },
                },
                cc_sim::FaultAt {
                    at: Time::from_nanos(6_000_000_000),
                    action: FaultAction::Restart {
                        node: NodeId::new(2),
                    },
                },
            ],
        };
        let run = run_spec(spec, RecorderLevel::Theater).expect("cluster");

        assert!(run.had_leader);
        assert!(run.trace_invariants_ok);
        assert!(matches!(run.verdict, Verdict::Linearizable { .. }));

        let wiped_disk = run
            .trace
            .events
            .iter()
            .any(|event| event.kind == EventKind::IoLost && event.node == Some(NodeId::new(2)));
        assert!(wiped_disk, "a wipe is durable-state loss and must say so");

        let installed = run.trace.events.iter().any(|event| {
            event.kind == EventKind::SnapshotInstall && event.node == Some(NodeId::new(2))
        });
        assert!(
            installed,
            "the rejoining node was caught up by state transfer"
        );

        // Catching up means catching up: the rejoined node ends at the same
        // last index as the rest of the cluster, with everything applied.
        let max_last = run
            .final_log_indices
            .iter()
            .map(|(_, last, _)| *last)
            .max()
            .unwrap_or(0);
        let rejoined = run
            .final_log_indices
            .iter()
            .find(|(id, _, _)| *id == 2)
            .expect("wiped node remains in cluster");
        assert_eq!(rejoined.1, max_last);
        assert_eq!(rejoined.2, rejoined.1);
    }

    /// Reads run through the ReadIndex quorum round. Before this was wired the
    /// `ReadReply` handler matched on sequence zero, which no operation ever
    /// carries, so every read silently timed out.
    #[test]
    fn reads_are_served_through_the_read_index_barrier() {
        let mut spec = RunSpec::standard(Seed::new(0x81), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(6_000_000_000);
        spec.end_time = spec.config.end_time;
        let run = run_spec(spec, RecorderLevel::Gate).expect("cluster");
        let served = run
            .history
            .operations
            .iter()
            .filter(|operation| {
                matches!(operation.kind, OperationKind::Get { .. }) && operation.complete.is_some()
            })
            .count();
        assert!(served > 0, "the read barrier served real reads");
        assert!(matches!(run.verdict, Verdict::Linearizable { .. }));
    }

    /// The crash/pause distinction, pinned. Under pause semantics the restarted
    /// node keeps its applied index and this fails.
    #[test]
    fn crash_drops_volatile_state_and_restart_recovers_only_durable_state() {
        let mut spec = RunSpec::standard(Seed::new(0x61), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(10_000_000_000);
        spec.end_time = spec.config.end_time;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .advance(Duration::from_secs(3))
            .expect("warmup advances");

        let before = cluster.snapshot();
        let victim = before
            .nodes
            .iter()
            .find(|node| node.role != Role::Leader && node.applied > 0)
            .expect("a follower has applied entries before the crash");
        let victim_id = NodeId::new(victim.id);
        assert!(victim.durable_bytes > 0, "victim persisted a real log");

        // Cut the victim off first, so anything it holds after the restart can
        // only have come from its own disk.
        let peers: Vec<NodeId> = before
            .nodes
            .iter()
            .map(|node| NodeId::new(node.id))
            .filter(|id| *id != victim_id)
            .collect();
        cluster.inject(FaultAction::Partition {
            left: vec![victim_id],
            right: peers,
        });
        cluster.inject(FaultAction::Crash { node: victim_id });
        cluster
            .advance(Duration::from_millis(100))
            .expect("crash settles");
        cluster.inject(FaultAction::Restart { node: victim_id });
        cluster
            .advance(Duration::from_millis(100))
            .expect("restart settles");

        let after = cluster.snapshot();
        let recovered = after
            .nodes
            .iter()
            .find(|node| node.id == victim.id)
            .expect("victim is still in the cluster");
        assert!(
            !recovered.log_tail.is_empty(),
            "durable log survived the crash"
        );
        assert_eq!(recovered.applied, 0, "volatile applied index was lost");
        assert_eq!(recovered.commit, 0, "volatile commit index was lost");
        assert_eq!(
            recovered.role,
            Role::Follower,
            "restarts come back as followers"
        );
    }

    /// The old `index * 64` slotting silently overlapped entries once a payload
    /// exceeded 64 bytes; a round-trip over a large payload pins the fix.
    #[test]
    fn wal_round_trips_entries_larger_than_the_old_fixed_slot() {
        let entries: Vec<Entry> = (1..=4)
            .map(|index| Entry {
                term: cc_core::Term::new(7),
                index: cc_core::LogIndex::new(index),
                kind: cc_raft::EntryKind::App,
                payload: vec![index as u8; 200],
            })
            .collect();
        let mut durable = vec![0_u8; WAL_HARD_STATE_LEN as usize];
        durable[0..8].copy_from_slice(&WAL_HARD_STATE_VERSION.to_le_bytes());
        durable[8..16].copy_from_slice(&9_u64.to_le_bytes());
        durable[16..24].copy_from_slice(&3_u64.to_le_bytes());
        for entry in &entries {
            durable.extend_from_slice(&encode_entry(entry));
        }

        let (hard, decoded, offsets, end) = decode_wal(&durable);
        assert_eq!(hard.term.get(), 9);
        assert_eq!(hard.voted_for, Some(NodeId::new(3)));
        assert_eq!(decoded, entries);
        assert_eq!(offsets.len(), 4);
        assert_eq!(end, durable.len() as u64);

        // A torn tail is not part of the recovered log.
        durable.truncate(durable.len() - 40);
        let (_, torn, _, _) = decode_wal(&durable);
        assert_eq!(torn.len(), 3, "the partial final record is dropped");
    }

    /// The membership profile used to be byte-identical to `rough`. It must now
    /// generate real reconfigure actions and land them as committed config.
    #[test]
    fn membership_profile_moves_the_voting_set() {
        let end_time = Time::from_nanos(6_000_000_000);
        let nodes: Vec<NodeId> = (1..=5).map(NodeId::new).collect();
        let mut spec = RunSpec::standard(Seed::new(0x71), FaultProfile::Membership);
        spec.config.end_time = end_time;
        spec.end_time = end_time;
        spec.plan =
            cc_sim::materialize_fault_plan(spec.seed, FaultProfile::Membership, &nodes, end_time);
        let reconfigures = spec
            .plan
            .actions
            .iter()
            .filter(|fault| matches!(fault.action, FaultAction::Reconfigure { .. }))
            .count();
        assert_eq!(reconfigures, 2, "the profile plans a shrink and a restore");

        let rough =
            cc_sim::materialize_fault_plan(spec.seed, FaultProfile::Rough, &nodes, end_time);
        assert_ne!(
            spec.plan, rough,
            "membership is no longer a relabelled rough run"
        );

        // `enter_joint` widens the voter set to the union and only `leave_joint`
        // narrows it, so sample across the run rather than at one instant.
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        let mut saw_joint = false;
        let mut smallest = usize::MAX;
        for _ in 0..60 {
            let snapshot = cluster
                .advance(Duration::from_millis(100))
                .expect("advance succeeds");
            for node in &snapshot.nodes {
                if node.status != cc_sim::NodeStatus::Up || node.voters.is_empty() {
                    continue;
                }
                saw_joint |= node.joint;
                smallest = smallest.min(node.voters.len());
            }
        }
        assert!(saw_joint, "a joint configuration was actually open");
        assert_eq!(smallest, 4, "a voter left the configuration");

        let run = cluster.run().expect("run completes");
        assert!(run.trace_invariants_ok);
        assert!(
            run.trace
                .events
                .iter()
                .any(|event| event.kind == EventKind::ConfChange),
            "joint transitions reached the trace"
        );
    }

    /// Determinism has to hold on the fault-bearing profiles, not just a calm
    /// run — and the profiles must not collapse into the same trace.
    #[test]
    fn cluster_traces_are_byte_deterministic_under_every_fault_profile() {
        let seed = Seed::new(0x52);
        let mut traces = Vec::new();
        for profile in DETERMINISM_PROFILES {
            let first = deterministic_cluster_trace_for(seed, profile);
            let second = deterministic_cluster_trace_for(seed, profile);
            assert_eq!(first, second, "{} is not deterministic", profile.as_str());
            traces.push(first);
        }
        let distinct: BTreeSet<&Vec<u8>> = traces.iter().collect();
        assert_eq!(
            distinct.len(),
            traces.len(),
            "each profile must actually exercise a different schedule"
        );
    }

    #[test]
    fn planted_all_wipe_failure_shrinks_past_irrelevant_faults() {
        let nodes: Vec<NodeId> = (1..=3).map(NodeId::new).collect();
        let mut plan = FaultPlan::default();
        for node in &nodes {
            plan.push(cc_sim::FaultAt {
                at: Time::from_nanos(0),
                action: FaultAction::Wipe { node: *node },
            });
        }
        for index in 0..20 {
            plan.push(cc_sim::FaultAt {
                at: Time::from_nanos(1_000_000_000),
                action: FaultAction::ClockSkew {
                    node: nodes[index % nodes.len()],
                    offset: Duration::from_millis(1),
                },
            });
        }
        // The plan's acceptance bar is a >=80% median reduction over planted
        // config failures, so measure the median rather than one sample.
        let mut reductions = Vec::new();
        for seed in 0..9_u64 {
            let mut spec = RunSpec::standard(Seed::new(0x53 + seed), FaultProfile::Wipe);
            spec.config.node_count = 3;
            spec.config.end_time = Time::from_nanos(500_000_000);
            spec.end_time = spec.config.end_time;
            spec.plan = plan.clone();
            let before = spec.plan.actions.len();
            let shrunk = shrink_cluster_plan(&spec);
            let after = shrunk.actions.len();
            assert!(
                reproduces_failure(&RunSpec {
                    plan: shrunk,
                    ..spec
                }),
                "the shrunk plan must still reproduce the failure"
            );
            reductions.push(100 - (after * 100 / before));
        }
        reductions.sort_unstable();
        let median = reductions[reductions.len() / 2];
        assert!(
            median >= 80,
            "median reduction {median}% is below the plan's 80% bar (samples {reductions:?})"
        );
    }
}
