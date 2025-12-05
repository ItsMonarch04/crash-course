// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The real deterministic cluster fixture used by cc-swarm and later theater work."]

#[cfg(any(
    all(feature = "kata01", feature = "kata02"),
    all(feature = "kata01", feature = "kata03"),
    all(feature = "kata01", feature = "kata04"),
    all(feature = "kata01", feature = "kata05"),
    all(feature = "kata02", feature = "kata03"),
    all(feature = "kata02", feature = "kata04"),
    all(feature = "kata02", feature = "kata05"),
    all(feature = "kata03", feature = "kata04"),
    all(feature = "kata03", feature = "kata05"),
    all(feature = "kata04", feature = "kata05"),
))]
compile_error!("kata features are mutually exclusive");

#[cfg(feature = "kata01")]
pub const ACTIVE_KATA: Option<&str> = Some("kata01");
#[cfg(all(not(feature = "kata01"), feature = "kata02"))]
pub const ACTIVE_KATA: Option<&str> = Some("kata02");
#[cfg(all(not(any(feature = "kata01", feature = "kata02")), feature = "kata03"))]
pub const ACTIVE_KATA: Option<&str> = Some("kata03");
#[cfg(all(
    not(any(feature = "kata01", feature = "kata02", feature = "kata03")),
    feature = "kata04"
))]
pub const ACTIVE_KATA: Option<&str> = Some("kata04");
#[cfg(all(
    not(any(
        feature = "kata01",
        feature = "kata02",
        feature = "kata03",
        feature = "kata04"
    )),
    feature = "kata05"
))]
pub const ACTIVE_KATA: Option<&str> = Some("kata05");
#[cfg(not(any(
    feature = "kata01",
    feature = "kata02",
    feature = "kata03",
    feature = "kata04",
    feature = "kata05",
)))]
pub const ACTIVE_KATA: Option<&str> = None;

mod fuzzing;
mod ledger;
pub use fuzzing::{FuzzOutcome, MAX_FUZZ_INPUT_BYTES, fuzz_decode, minimize_case, mutate_case};
pub use ledger::{
    KATA_LEDGER_HEADER, LEDGER_COLUMNS, LEDGER_HEADER, LedgerError, LedgerKey, LedgerKind,
    LedgerRow, LedgerVerdict, SeedLedger, Shard, ShardError, encode_ledger_row,
    validate_sharded_coverage,
};

#[cfg(all(
    test,
    not(any(
        feature = "kata01",
        feature = "kata02",
        feature = "kata03",
        feature = "kata04",
        feature = "kata05",
    ))
))]
mod default_kata_tests {
    use super::ACTIVE_KATA;

    #[test]
    fn trap_default_build_enables_no_kata() {
        assert_eq!(ACTIVE_KATA, None);
    }

    #[test]
    fn trap_kata_features_are_mutually_exclusive() {
        let source = include_str!("lib.rs");
        assert!(source.contains("compile_error!(\"kata features are mutually exclusive\")"));
        assert_eq!(source.matches("all(feature = \"kata").count(), 10);
    }
}

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_checker::{
    CheckerConfig, History, LivenessReport, Operation, OperationKind, Outcome, Verdict, check,
    check_liveness, cross_check_linearizable_sessions,
};
use cc_cluster::{NodeConfig, NodeError, RecoveredNode};
use cc_core::{
    ClientId, ClusterPolicy, Duration, EventKind, NodeId, Seed, Time, TimerId, Trace, Xoshiro256pp,
    fnv1a,
};
use cc_env::{Effect, FileId, Input, IoResult, WireMsg, decode_peer_frame, encode_peer_frame};
use cc_host::{BootState, Driver, DriverPoll, HostError, Usage};
use cc_kv::{KvCommand, KvReply, decode_reply, encode_command};
use cc_raft::{RaftConfig, Role};
use cc_sim::{
    CcrpMutation, DiskFault, DiskOperation, EventQueue, FaultAction, FaultAt, FaultPlan,
    FaultProfile, LinkConfig, Network, NetworkDecision, Recorder, RecorderLevel, RunError, RunSpec,
    SimConfig, SimDisk, WorkloadActor, WorkloadOperation, canonicalize_fault_plan,
    shrink_fault_plan,
};
use cc_store::{BlockRead, BlockReadError, BlockSource, StoreConfig, StoreError};

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
const JOINT_RETRY: Duration = Duration::from_millis(50);
const FIRST_CLIENT_TIME: Duration = Duration::from_secs(1);
const WAL_FILE: FileId = FileId::Wal { segment: 0 };

#[derive(Clone, Debug, Eq, PartialEq)]
enum ClusterEventKind {
    Tick(NodeId),
    Timer {
        node: NodeId,
        id: TimerId,
        generation: u64,
    },
    Message {
        from: NodeId,
        to: NodeId,
        frame: Vec<u8>,
        charged: bool,
        expected_rejection: bool,
    },
    ClientIssue {
        client: u64,
        sequence: u64,
        operation: WorkloadOperation,
    },
    ClientTimeout(u64),
    Fault(FaultAction),
    /// A node durability barrier has completed its write half. The fsync is
    /// scheduled separately so a persistent SlowDisk delay belongs to the
    /// operation being serviced, never to the next unrelated input.
    DiskWriteComplete {
        node: NodeId,
        file: FileId,
        at: u64,
        bytes: Vec<u8>,
        id: cc_core::IoId,
    },
    DiskFsyncComplete {
        node: NodeId,
        file: FileId,
        id: cc_core::IoId,
    },
    DiskReadComplete {
        node: NodeId,
        file: FileId,
        at: u64,
        len: u32,
        id: cc_core::IoId,
    },
    DiskRenameComplete {
        node: NodeId,
        from: FileId,
        to: FileId,
        id: cc_core::IoId,
    },
    DiskSyncDirComplete {
        node: NodeId,
        id: cc_core::IoId,
    },
    DeferredInput {
        node: NodeId,
        input: Input,
    },
    DriverServiceComplete {
        node: NodeId,
    },
    /// Second half of a joint-consensus transition, scheduled by the host once
    /// the joint config has had time to replicate.
    LeaveJoint(NodeId),
}

#[derive(Clone)]
struct NodeSlot {
    config: NodeConfig,
    genesis: cc_log::Genesis,
    driver: Option<Driver>,
    status: cc_sim::NodeStatus,
    clock_offset: Duration,
    disk: SimDisk,
    /// End of the verified `cc-log` framed durable stream.  Every simulator
    /// persistence operation appends one canonical CCLR record or record
    /// batch; recovery owns the semantic replay of that prefix.
    wal_end: u64,
    /// The Driver owns the continuation; this host-only deadline prevents a
    /// later simulated arrival from calling into that Driver before its
    /// scheduled write/fsync completion is delivered.
    persistence_ready_at: Option<Time>,
}

/// Simulator counterpart of the real positioned file reader. It observes a
/// live process's page-cache namespace, while `SimDisk::crash` first discards
/// all bytes that were not fsynced. Every result, including an error, carries
/// the deterministic service charged to the initiating operation.
struct SimBlockSource<'a> {
    disk: &'a mut SimDisk,
}

impl BlockSource for SimBlockSource<'_> {
    fn read_block(
        &mut self,
        file: FileId,
        offset: u64,
        len: u32,
    ) -> Result<BlockRead, BlockReadError> {
        let service = self.disk.service_time(DiskOperation::Read);
        match self.disk.read(file, offset, len) {
            Ok(IoResult::Read(bytes)) => Ok(BlockRead { bytes, service }),
            Ok(_) => Err(BlockReadError {
                error: StoreError::Corrupt("sim block read result"),
                service,
            }),
            Err(cc_env::IoError::NotFound) => Err(BlockReadError {
                error: StoreError::MissingTable {
                    file_no: match file {
                        FileId::Sst { file_no } => file_no,
                        _ => 0,
                    },
                },
                service,
            }),
            Err(cc_env::IoError::Corrupt(_)) => Err(BlockReadError {
                error: StoreError::Corrupt("simulated block corruption"),
                service,
            }),
            Err(_) => Err(BlockReadError {
                error: StoreError::InvalidInput("simulated block read"),
                service,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CorruptFrameFault {
    nth: u64,
    byte: usize,
    bit: u8,
}

#[derive(Clone, Copy, Debug)]
enum FrameFault {
    Corrupt(CorruptFrameFault),
    Truncate { nth: u64, keep: usize },
    Mutate { nth: u64, mutation: CcrpMutation },
}

impl FrameFault {
    const fn nth(self) -> u64 {
        match self {
            Self::Corrupt(fault) => fault.nth,
            Self::Truncate { nth, .. } => nth,
            Self::Mutate { nth, .. } => nth,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ReplayFrameFault {
    nth: u64,
    at: Time,
}

impl NodeSlot {
    fn reset_wal(&mut self) {
        self.wal_end = 0;
    }
}

#[derive(Clone)]
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
    pub invariant_violations: Vec<String>,
    pub run_footprint: RunFootprint,
    pub had_leader: bool,
    pub completed_operations: u64,
    pub event_count: u64,
    pub peak_total_bytes: u64,
    pub final_log_indices: Vec<(u64, u64, u64)>,
    pub liveness_ok: bool,
}

/// Run-owned resources that cannot truthfully be attributed to one Driver.
/// Counts and encoded bytes use the same `Usage` shape as node gauges.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RunFootprint {
    pub network_inflight: Usage,
    pub fault_replay: Usage,
    pub scheduled_events: Usage,
    pub checker_history: Usage,
    pub failure_artifact_buffer: Usage,
    pub theater_checkpoints: Usage,
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
    pub disk_service_delay_ns: u64,
    pub clock_offset_ns: u64,
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
    const MAX_LIFELINES: u64 = 32;
    const MAX_EVENTS: usize = 500;
    const MAX_SVG_BYTES: usize = 2 * 1024 * 1024;
    let actual_node_count = trace
        .events
        .iter()
        .filter_map(|event| event.node)
        .map(NodeId::get)
        .max()
        .unwrap_or(1);
    let node_count = actual_node_count.min(MAX_LIFELINES);
    let candidates: Vec<_> = trace
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
        .collect();
    let omitted = actual_node_count > node_count || candidates.len() > MAX_EVENTS;
    let visible = candidates.into_iter().take(MAX_EVENTS).collect::<Vec<_>>();
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
        let from = event.node.map_or(1, NodeId::get).min(node_count);
        let from_x = 80_u64.saturating_add(from.saturating_sub(1).saturating_mul(140));
        if event.kind == EventKind::NetSend {
            // Transport trace payloads are binary fingerprints, not a display
            // string protocol. Keep the visual anchored at the sender rather
            // than parsing the former `from>to:{kind:?}` debug payload.
            let to_x = from_x;
            svg.push_str(&format!("<line class=\"msg\" x1=\"{from_x}\" y1=\"{y}\" x2=\"{to_x}\" y2=\"{y}\"/><text x=\"{}\" y=\"{}\" text-anchor=\"middle\">send #{}</text>", (from_x + to_x) / 2, y.saturating_sub(4), event.seq));
        } else {
            let payload = svg_event_payload(&event.payload);
            let suffix = if payload.is_empty() {
                String::new()
            } else {
                format!(" {payload}")
            };
            svg.push_str(&format!("<circle class=\"event\" cx=\"{from_x}\" cy=\"{y}\" r=\"4\"/><text x=\"{}\" y=\"{}\">{} #{}{}</text>", from_x.saturating_add(8), y.saturating_add(4), event.kind.as_str(), event.seq, suffix));
        }
    }
    if omitted {
        let y = height.saturating_sub(8);
        svg.push_str(&format!(
            "<text x=\"12\" y=\"{y}\">… omitted by bounded SVG renderer …</text>"
        ));
    }
    svg.push_str("</svg>");
    if svg.len() > MAX_SVG_BYTES {
        return String::from(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"640\" height=\"80\"><text x=\"12\" y=\"40\">diagram omitted: output byte cap reached</text></svg>",
        );
    }
    svg
}

fn svg_event_payload(payload: &[u8]) -> String {
    const MAX_LABEL_BYTES: usize = 64;
    let mut label = payload
        .iter()
        .take(MAX_LABEL_BYTES)
        .map(|byte| {
            if byte.is_ascii_graphic() || *byte == b' ' {
                char::from(*byte)
            } else {
                '�'
            }
        })
        .collect::<String>();
    if payload.len() > MAX_LABEL_BYTES {
        label.push('…');
    }
    xml_escape(&label)
}

fn xml_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '&' => "&amp;".chars().collect::<Vec<_>>(),
            '<' => "&lt;".chars().collect(),
            '>' => "&gt;".chars().collect(),
            '"' => "&quot;".chars().collect(),
            '\'' => "&apos;".chars().collect(),
            other => vec![other],
        })
        .collect()
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
        let invariant_violations = self
            .invariant_violations
            .iter()
            .map(|violation| format!("\"{}\"", json_escape(violation)))
            .collect::<Vec<_>>()
            .join(",");
        let run_footprint = run_footprint_json(self.run_footprint);
        format!(
            "{{\"fixture_version\":1,\"synthetic\":{},\"run_spec\":{},\"seed\":\"{}\",\"profile\":\"{}\",\"events\":{},\"completed_operations\":{},\"peak_total_bytes\":{},\"had_leader\":{},\"trace_invariants_ok\":{},\"invariant_violations\":[{}],\"run_footprint\":{},\"liveness_ok\":{},\"verdict\":\"{}\",\"checker_report\":{},\"trace\":{}}}",
            ACTIVE_KATA.is_some(),
            canonical_run_spec_json(&self.spec),
            self.seed,
            profile.as_str(),
            self.event_count,
            self.completed_operations,
            self.peak_total_bytes,
            self.had_leader,
            self.trace_invariants_ok,
            invariant_violations,
            run_footprint,
            self.liveness_ok,
            verdict,
            checker_report,
            self.trace.to_json()
        )
    }
}

fn run_footprint_json(footprint: RunFootprint) -> String {
    let usage = |usage: Usage| {
        format!(
            "{{\"current\":{},\"peak\":{},\"limit\":{}}}",
            usage.current, usage.peak, usage.limit
        )
    };
    format!(
        "{{\"network_inflight\":{},\"fault_replay\":{},\"scheduled_events\":{},\"checker_history\":{},\"failure_artifact_buffer\":{},\"theater_checkpoints\":{}}}",
        usage(footprint.network_inflight),
        usage(footprint.fault_replay),
        usage(footprint.scheduled_events),
        usage(footprint.checker_history),
        usage(footprint.failure_artifact_buffer),
        usage(footprint.theater_checkpoints),
    )
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

/// Claims ledgers, summaries, and badges accept only artifacts produced by a
/// default build. The scan is intentionally dependency-free and whitespace-
/// insensitive; escaped content remains inside strings and cannot spell an
/// unescaped JSON field token.
#[must_use]
pub fn artifact_is_claim_eligible(bytes: &[u8]) -> bool {
    let mut compact = Vec::with_capacity(bytes.len());
    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes {
        if in_string {
            compact.push(*byte);
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
        } else if *byte == b'"' {
            in_string = true;
            compact.push(*byte);
        } else if !byte.is_ascii_whitespace() {
            compact.push(*byte);
        }
    }
    !compact
        .windows(b"\"synthetic\":true".len())
        .any(|window| window == b"\"synthetic\":true")
}

#[must_use]
pub fn canonical_run_spec_json(spec: &RunSpec) -> String {
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
    let disk_profile = spec.disk_profile.as_ref().map_or_else(
        || String::from("null"),
        |name| format!("\"{}\"", json_escape(name)),
    );
    let disk = spec.config.disk_model;
    format!(
        "{{\"seed\":\"{}\",\"profile\":\"{}\",\"node_count\":{},\"end_time_ns\":{},\"disk_profile\":{},\"disk_model\":{{\"read\":{},\"write\":{},\"fsync\":{},\"rename\":{},\"dirsync\":{}}},\"workload\":{{\"clients\":{},\"ops_per_second\":{},\"keyspace\":{},\"set_ttl_ns\":{}}},\"faults\":[{}]}}",
        spec.seed,
        spec.profile.as_str(),
        spec.config.node_count,
        spec.end_time.as_nanos(),
        disk_profile,
        delay_dist_json(disk.read),
        delay_dist_json(disk.write),
        delay_dist_json(disk.fsync),
        delay_dist_json(disk.rename),
        delay_dist_json(disk.dirsync),
        spec.workload.clients,
        spec.workload.ops_per_second,
        spec.workload.keyspace,
        spec.workload
            .set_ttl
            .map_or_else(|| String::from("null"), |ttl| ttl.as_nanos().to_string()),
        faults
    )
}

fn delay_dist_json(distribution: cc_core::DelayDist) -> String {
    match distribution {
        cc_core::DelayDist::Fixed(value) => {
            format!("{{\"kind\":\"fixed\",\"ns\":{}}}", value.as_nanos())
        }
        cc_core::DelayDist::Uniform { low, high } => format!(
            "{{\"kind\":\"uniform\",\"low_ns\":{},\"high_ns\":{}}}",
            low.as_nanos(),
            high.as_nanos()
        ),
        cc_core::DelayDist::TwoPoint {
            short,
            long,
            long_chance,
        } => format!(
            "{{\"kind\":\"two-point\",\"short_ns\":{},\"long_ns\":{},\"long_p16\":{}}}",
            short.as_nanos(),
            long.as_nanos(),
            long_chance.numerator()
        ),
        cc_core::DelayDist::Empirical { buckets, count } => {
            let count = usize::from(count.min(16));
            let values = buckets[..count]
                .iter()
                .map(|value| value.as_nanos().to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("{{\"kind\":\"empirical\",\"buckets_ns\":[{values}]}}")
        }
    }
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
        FaultAction::SlowDisk { node, slow } => format!(
            "{{\"kind\":\"slow-disk\",\"node\":{},\"read_extra_ns\":{},\"write_extra_ns\":{},\"fsync_extra_ns\":{},\"rename_extra_ns\":{},\"dirsync_extra_ns\":{}}}",
            node.get(),
            slow.read_extra.as_nanos(),
            slow.write_extra.as_nanos(),
            slow.fsync_extra.as_nanos(),
            slow.rename_extra.as_nanos(),
            slow.dirsync_extra.as_nanos(),
        ),
        FaultAction::EnospcFrom { node } => {
            format!("{{\"kind\":\"enospc-from\",\"node\":{}}}", node.get())
        }
        FaultAction::BitRotAtRest { node, file, offset } => {
            let (kind, number) = file_id_json(*file);
            format!(
                "{{\"kind\":\"bitrot-at-rest\",\"node\":{},\"file_kind\":\"{kind}\",\"file_no\":{number},\"offset\":{}}}",
                node.get(),
                offset,
            )
        }
        FaultAction::DiskQuota { node, bytes } => format!(
            "{{\"kind\":\"disk-quota\",\"node\":{},\"bytes\":{}}}",
            node.get(),
            bytes,
        ),
        FaultAction::LinkDegrade { from, to, .. } => format!(
            "{{\"kind\":\"link-degrade\",\"from\":{},\"to\":{}}}",
            from.get(),
            to.get()
        ),
        FaultAction::CorruptFrame {
            from,
            to,
            nth,
            byte,
            bit,
        } => format!(
            "{{\"kind\":\"corrupt-frame\",\"from\":{},\"to\":{},\"nth\":{},\"byte\":{},\"bit\":{}}}",
            from.get(),
            to.get(),
            nth,
            byte,
            bit,
        ),
        FaultAction::TruncateFrame {
            from,
            to,
            nth,
            keep,
        } => format!(
            "{{\"kind\":\"truncate-frame\",\"from\":{},\"to\":{},\"nth\":{},\"keep\":{}}}",
            from.get(),
            to.get(),
            nth,
            keep,
        ),
        FaultAction::ReplayFrame { from, to, nth, at } => format!(
            "{{\"kind\":\"replay-frame\",\"from\":{},\"to\":{},\"nth\":{},\"at_ns\":{}}}",
            from.get(),
            to.get(),
            nth,
            at.as_nanos(),
        ),
        FaultAction::DelayLink { from, to, extra } => format!(
            "{{\"kind\":\"delay-link\",\"from\":{},\"to\":{},\"extra_ns\":{}}}",
            from.get(),
            to.get(),
            extra.as_nanos(),
        ),
        FaultAction::MutateRaftAndRechecksum {
            from,
            to,
            nth,
            mutation,
        } => format!(
            "{{\"kind\":\"mutate-raft-and-rechecksum\",\"from\":{},\"to\":{},\"nth\":{},\"mutation\":\"{}\"}}",
            from.get(),
            to.get(),
            nth,
            ccrp_mutation_json(*mutation),
        ),
        FaultAction::Reconfigure { voters } => format!(
            "{{\"kind\":\"reconfigure\",\"voters\":{}}}",
            node_ids_json(voters)
        ),
    }
}

fn ccrp_mutation_json(mutation: CcrpMutation) -> String {
    match mutation {
        CcrpMutation::MessageTag(value) => format!("message-tag:{value}"),
        CcrpMutation::AppendEntryCount(value) => format!("append-entry-count:{value}"),
        CcrpMutation::EntryPayloadLength(value) => format!("entry-payload-length:{value}"),
        CcrpMutation::OptionFlag(value) => format!("option-flag:{value}"),
        CcrpMutation::FromNodeId(value) => format!("from-node-id:{value}"),
        CcrpMutation::Truncate(value) => format!("truncate:{value}"),
    }
}

const fn file_id_json(file: FileId) -> (&'static str, u64) {
    match file {
        FileId::Wal { segment } => ("wal", segment),
        FileId::StoreWal { segment } => ("store-wal", segment),
        FileId::Sst { file_no } => ("sst", file_no),
        FileId::Manifest { generation } => ("manifest", generation),
        FileId::Snapshot { generation } => ("snapshot", generation),
        FileId::Meta => ("meta", 0),
        FileId::Temp { sequence } => ("temp", sequence),
    }
}

/// Change a deliberately selected CCRP field and rebuild CCPF around it. The
/// CCPF envelope therefore remains valid and the CCRP decoder is the component
/// under test. Offsets are the fixed v1 CCRP layout and are checked before
/// every write; a nonsensical mutation is a bad fault plan, never a panic.
fn mutate_and_rechecksum(frame: &mut Vec<u8>, mutation: CcrpMutation) -> Result<(), ()> {
    let (wire, used) = decode_peer_frame(frame).map_err(|_| ())?;
    if used != frame.len() {
        return Err(());
    }
    let mut payload = wire.payload;
    const TAG: usize = 32;
    const APPEND_COUNT: usize = 65;
    const ENTRY_PAYLOAD_LENGTH: usize = 86;
    const APPEND_RESPONSE_OPTION: usize = 42;
    const FROM_NODE_ID: usize = 8;
    match mutation {
        CcrpMutation::MessageTag(tag) => *payload.get_mut(TAG).ok_or(())? = tag,
        CcrpMutation::AppendEntryCount(count) => {
            if payload.get(TAG).copied() != Some(5) || payload.len() < APPEND_COUNT + 4 {
                return Err(());
            }
            payload[APPEND_COUNT..APPEND_COUNT + 4].copy_from_slice(&count.to_le_bytes());
        }
        CcrpMutation::EntryPayloadLength(length) => {
            if payload.get(TAG).copied() != Some(5) || payload.len() < ENTRY_PAYLOAD_LENGTH + 4 {
                return Err(());
            }
            payload[ENTRY_PAYLOAD_LENGTH..ENTRY_PAYLOAD_LENGTH + 4]
                .copy_from_slice(&length.to_le_bytes());
        }
        CcrpMutation::OptionFlag(flag) => {
            if payload.get(TAG).copied() != Some(6) || payload.len() <= APPEND_RESPONSE_OPTION {
                return Err(());
            }
            payload[APPEND_RESPONSE_OPTION] = flag;
        }
        CcrpMutation::FromNodeId(id) => {
            if payload.len() < FROM_NODE_ID + 8 {
                return Err(());
            }
            payload[FROM_NODE_ID..FROM_NODE_ID + 8].copy_from_slice(&id.to_le_bytes());
        }
        CcrpMutation::Truncate(keep) => {
            if keep >= payload.len() {
                return Err(());
            }
            payload.truncate(keep);
        }
    }
    *frame = encode_peer_frame(&WireMsg::new(wire.proto_version, payload)).map_err(|_| ())?;
    Ok(())
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
        Verdict::NotLinearizable { witness, visited } => format!(
            "{{\"kind\":\"not-linearizable\",\"visited\":{},\"witness\":{{\"operation_ids\":[{}],\"oracle_calls\":{},\"budget_exhausted\":{},\"one_minimal\":{}}}}}",
            visited,
            witness
                .operation_ids
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(","),
            witness.oracle_calls,
            witness.budget_exhausted,
            witness.one_minimal,
        ),
    }
}

#[derive(Debug)]
pub enum ClusterError {
    Run(RunError),
    Node { node: NodeId, error: NodeError },
    Host { node: NodeId, error: HostError },
    Network { from: NodeId, to: NodeId },
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(error) => error.fmt(f),
            Self::Node { node, error } => write!(f, "node {node}: {error}"),
            Self::Host { node, error } => write!(f, "host {node}: {error}"),
            Self::Network { from, to } => write!(f, "network link missing {from}->{to}"),
        }
    }
}

fn cluster_host_error(node: NodeId, error: HostError) -> ClusterError {
    ClusterError::Host { node, error }
}

impl std::error::Error for ClusterError {}

impl From<RunError> for ClusterError {
    fn from(error: RunError) -> Self {
        Self::Run(error)
    }
}

/// A deterministic host for the real `cc-cluster::Node` composition.
#[derive(Clone)]
pub struct SimCluster {
    spec: RunSpec,
    now: Time,
    events: EventQueue<ClusterEventKind>,
    next_operation_id: u64,
    processed_events: u64,
    current_instant: Option<Time>,
    events_at_instant: u64,
    total_issued: u64,
    had_leader: bool,
    recorder: Recorder,
    network: Network,
    frame_ordinals: BTreeMap<(NodeId, NodeId), u64>,
    frame_faults: BTreeMap<(NodeId, NodeId), Vec<FrameFault>>,
    replay_faults: BTreeMap<(NodeId, NodeId), Vec<ReplayFrameFault>>,
    replay_frames: BTreeMap<(NodeId, NodeId), Vec<u8>>,
    peak_replay_bytes: u64,
    nodes: BTreeMap<NodeId, NodeSlot>,
    pending: BTreeMap<u64, PendingOperation>,
    actors: BTreeMap<u64, WorkloadActor>,
    history: History,
}

impl SimCluster {
    pub fn new(spec: RunSpec, level: RecorderLevel) -> Result<Self, ClusterError> {
        let node_count = usize::try_from(spec.config.node_count).unwrap_or(0);
        let node_ids: Vec<NodeId> = (1..=node_count as u64).map(NodeId::new).collect();
        let voters: BTreeSet<NodeId> = node_ids.iter().copied().collect();
        let bootstrap_membership =
            cc_core::MembershipState::new(voters.clone()).map_err(|_| ClusterError::Node {
                node: NodeId::new(1),
                error: NodeError::Environment("invalid simulator membership"),
            })?;
        let cluster_id = simulated_cluster_id(spec.seed);
        let network = Network::new(&node_ids, spec.seed, link_config(spec.profile));
        let mut nodes = BTreeMap::new();
        for id in &node_ids {
            let election_min =
                150_u64.saturating_add(id.get().saturating_sub(1).saturating_mul(35));
            let config = NodeConfig {
                id: *id,
                cluster_id,
                seed: Seed::new(spec.seed.0 ^ id.get().rotate_left(17)),
                raft: RaftConfig {
                    election_min: Duration::from_millis(election_min),
                    election_max: Duration::from_millis(election_min.saturating_add(20)),
                    ..RaftConfig::default()
                },
                store: StoreConfig::default(),
                policy: ClusterPolicy::default(),
                host_limits: spec.host_limits,
            };
            let genesis = cc_log::Genesis {
                origin: cc_log::Origin::Bootstrap,
                cluster_id,
                policy: config.policy,
                membership: bootstrap_membership.clone(),
            };
            let genesis_bytes = cc_log::encode_framed_durable_record(
                &cc_log::DurableRecord::Genesis(Box::new(genesis.clone())),
            )
            .map_err(|_| ClusterError::Node {
                node: *id,
                error: NodeError::Durability,
            })?;
            let mut disk = SimDisk::with_model(spec.config.disk_model, spec.seed, *id);
            disk.write(WAL_FILE, 0, &genesis_bytes)
                .and_then(|_| disk.fsync(WAL_FILE))
                .map_err(|_| ClusterError::Node {
                    node: *id,
                    error: NodeError::Durability,
                })?;
            let driver = Driver::boot_with_offsets_and_genesis(
                config,
                BootState::Fresh {
                    bootstrap: bootstrap_membership.clone(),
                },
                u64::try_from(genesis_bytes.len()).unwrap_or(u64::MAX),
                0,
                genesis.clone(),
            )
            .map_err(|error| cluster_host_error(*id, error))?;
            nodes.insert(
                *id,
                NodeSlot {
                    config,
                    genesis,
                    driver: Some(driver),
                    status: cc_sim::NodeStatus::Up,
                    clock_offset: Duration::default(),
                    disk,
                    wal_end: u64::try_from(genesis_bytes.len()).unwrap_or(u64::MAX),
                    persistence_ready_at: None,
                },
            );
        }
        let mut cluster = Self {
            recorder: Recorder::new(spec.seed, level),
            spec,
            now: Time::from_nanos(0),
            events: EventQueue::new(),
            next_operation_id: 1,
            processed_events: 0,
            current_instant: None,
            events_at_instant: 0,
            total_issued: 0,
            had_leader: false,
            network,
            frame_ordinals: BTreeMap::new(),
            frame_faults: BTreeMap::new(),
            replay_faults: BTreeMap::new(),
            replay_frames: BTreeMap::new(),
            peak_replay_bytes: 0,
            nodes,
            pending: BTreeMap::new(),
            actors: BTreeMap::new(),
            history: History::default(),
        };
        if let Some(kata) = ACTIVE_KATA {
            cluster.record(
                Time::from_nanos(0),
                None,
                EventKind::SyntheticKataEnabled,
                format!("synthetic=true kata={kata}").into_bytes(),
            );
        }
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

    /// Expose the applied transport configuration without exposing the
    /// mutable network. Hosts can render the effective value after a typed
    /// fault action instead of remembering what they asked for.
    #[must_use]
    pub fn link_config(&self, from: NodeId, to: NodeId) -> Option<LinkConfig> {
        self.network.config(from, to).ok()
    }

    #[must_use]
    pub fn snapshot(&self) -> ClusterSnapshot {
        let nodes = self
            .nodes
            .iter()
            .map(|(id, slot)| {
                let (role, term, commit, applied, log_tail, voters, joint) =
                    slot.driver.as_ref().map_or(
                        (Role::Follower, 0, 0, 0, Vec::new(), Vec::new(), false),
                        |driver| {
                            let node = driver.node();
                            let (voters, _, joint) = driver.membership();
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
                let disk_service_delay_ns = slot.disk.slow_disk().write_extra.as_nanos();
                ClusterNodeSnapshot {
                    id: id.get(),
                    status: slot.status,
                    role,
                    term,
                    commit,
                    applied,
                    durable_bytes,
                    disk_service_delay_ns,
                    clock_offset_ns: slot.clock_offset.as_nanos(),
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
                    ..CheckerConfig::default()
                },
            ),
        }
    }

    fn process_until(&mut self, limit: Time) -> Result<(), ClusterError> {
        while self.events.peek_time().is_some_and(|at| at <= limit) {
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
            if self.current_instant == Some(event.at) {
                self.events_at_instant = self.events_at_instant.saturating_add(1);
            } else {
                self.current_instant = Some(event.at);
                self.events_at_instant = 1;
            }
            if self.events_at_instant > self.spec.config.max_events_per_instant {
                return Err(ClusterError::Run(RunError::InstantLimit {
                    at: event.at,
                    limit: self.spec.config.max_events_per_instant,
                }));
            }
            self.now = event.at;
            self.handle(event.event)?;
        }
        self.now = limit;
        Ok(())
    }

    fn finish_result(self) -> Result<ClusterRun, ClusterError> {
        let trace = self.recorder.finish();
        let verdict = check(
            &self.history,
            CheckerConfig {
                max_states: 100_000,
                ..CheckerConfig::default()
            },
        );
        let mut invariant_violations = cc_checker::check_trace_invariants(&trace)
            .violations
            .into_iter()
            .map(|violation| format!("trace/{}: {}", violation.name, violation.detail))
            .collect::<Vec<_>>();
        let mut peak_total_bytes = 0_u64;
        for (node, slot) in &self.nodes {
            let Some(driver) = slot.driver.as_ref() else {
                continue;
            };
            invariant_violations.extend(
                driver
                    .node()
                    .raft
                    .invariants()
                    .violations
                    .into_iter()
                    .map(|violation| {
                        format!(
                            "raft/node-{}/{}: {}",
                            node.get(),
                            violation.name,
                            violation.detail
                        )
                    }),
            );
            let footprint = driver.footprint();
            for (owner, usage) in [
                ("log", footprint.log),
                ("snapshot-staging", footprint.snapshot_staging),
                ("sessions", footprint.sessions),
                ("session-tombstones", footprint.session_tombstones),
                ("pending-reads", footprint.pending_reads),
                ("pending-client-routes", footprint.pending_client_routes),
                ("memtables", footprint.memtables),
                ("sst-metadata", footprint.sst_metadata),
                ("driver-inputs", footprint.driver_inputs),
                ("driver-effects", footprint.driver_effects),
                ("outbound-frames", footprint.outbound_frames),
                ("checkpoint-builder", footprint.checkpoint_builder),
                ("compaction-builder", footprint.compaction_builder),
            ] {
                peak_total_bytes = peak_total_bytes.saturating_add(usage.peak);
                if usage.current > usage.limit || usage.peak > usage.limit {
                    invariant_violations.push(format!(
                        "footprint/node-{}/{owner}: current={} peak={} limit={}",
                        node.get(),
                        usage.current,
                        usage.peak,
                        usage.limit
                    ));
                }
            }
        }
        let history_bytes = u64::try_from(
            cc_checker::HistoryDocument {
                build_label: String::new(),
                config_hash: 0,
                initial: BTreeMap::new(),
                retain_open: true,
                history: self.history.clone(),
            }
            .encode()
            .len(),
        )
        .unwrap_or(u64::MAX);
        let replay_bytes = self.replay_frames.values().fold(0_u64, |total, frame| {
            total.saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX))
        });
        let run_footprint = RunFootprint {
            network_inflight: Usage {
                current: self.network.total_inflight_bytes(),
                peak: self.network.peak_inflight_bytes(),
                limit: self.spec.host_limits.max_network_inflight_bytes,
            },
            fault_replay: Usage {
                current: replay_bytes,
                peak: self.peak_replay_bytes,
                limit: self.spec.host_limits.max_fault_replay_bytes,
            },
            scheduled_events: Usage {
                current: u64::try_from(self.events.len()).unwrap_or(u64::MAX),
                peak: u64::try_from(self.events.peak_len()).unwrap_or(u64::MAX),
                limit: self.spec.config.max_events,
            },
            checker_history: Usage {
                current: history_bytes,
                peak: history_bytes,
                limit: self.spec.host_limits.max_history_bytes,
            },
            failure_artifact_buffer: Usage {
                current: 0,
                peak: 0,
                limit: self.spec.host_limits.max_failure_artifact_bytes,
            },
            theater_checkpoints: Usage {
                current: 0,
                peak: 0,
                limit: self.spec.host_limits.max_trace_bytes as u64,
            },
        };
        for (owner, usage) in [
            ("network-inflight", run_footprint.network_inflight),
            ("fault-replay", run_footprint.fault_replay),
            ("scheduled-events", run_footprint.scheduled_events),
            ("checker-history", run_footprint.checker_history),
            (
                "failure-artifact-buffer",
                run_footprint.failure_artifact_buffer,
            ),
            ("theater-checkpoints", run_footprint.theater_checkpoints),
        ] {
            peak_total_bytes = peak_total_bytes.saturating_add(usage.peak);
            if usage.current > usage.limit || usage.peak > usage.limit {
                invariant_violations.push(format!(
                    "footprint/run/{owner}: current={} peak={} limit={}",
                    usage.current, usage.peak, usage.limit
                ));
            }
        }
        invariant_violations.extend(
            cross_check_linearizable_sessions(&self.history, &verdict)
                .violations
                .into_iter()
                .map(|violation| format!("session/{}: {}", violation.name, violation.detail)),
        );
        let trace_invariants_ok = invariant_violations.is_empty();
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
                let (last, applied) = slot.driver.as_ref().map_or((0, 0), |driver| {
                    let node = driver.node();
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
            invariant_violations,
            run_footprint,
            had_leader: self.had_leader,
            completed_operations,
            event_count: self.processed_events,
            peak_total_bytes,
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
            self.events.schedule(at, kind);
        }
    }

    fn handle(&mut self, event: ClusterEventKind) -> Result<(), ClusterError> {
        match event {
            ClusterEventKind::Tick(node) => self.handle_tick(node),
            ClusterEventKind::Timer {
                node,
                id,
                generation,
            } => self.handle_timer(node, id, generation),
            ClusterEventKind::Message {
                from,
                to,
                frame,
                charged,
                expected_rejection,
            } => self.handle_delivery(from, to, frame, charged, expected_rejection),
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
            ClusterEventKind::DiskWriteComplete {
                node,
                file,
                at,
                bytes,
                id,
            } => self.handle_disk_write_complete(node, file, at, bytes, id),
            ClusterEventKind::DiskFsyncComplete { node, file, id } => {
                self.handle_disk_fsync_complete(node, file, id)
            }
            ClusterEventKind::DiskReadComplete {
                node,
                file,
                at,
                len,
                id,
            } => self.handle_disk_read_complete(node, file, at, len, id),
            ClusterEventKind::DiskRenameComplete { node, from, to, id } => {
                self.handle_disk_rename_complete(node, from, to, id)
            }
            ClusterEventKind::DiskSyncDirComplete { node, id } => {
                self.handle_disk_sync_dir_complete(node, id)
            }
            ClusterEventKind::DeferredInput { node, input } => {
                if self.is_up(node) {
                    self.drive_node(node, input)
                } else {
                    Ok(())
                }
            }
            ClusterEventKind::DriverServiceComplete { node } => self.complete_driver_service(node),
            ClusterEventKind::LeaveJoint(node) => self.handle_leave_joint(node),
        }
    }

    /// Close a joint transition only through its current leader.  A leadership
    /// change or an uncommitted EnterJoint is retried instead of letting an
    /// arbitrary follower manufacture the paired configuration entry.
    fn handle_leave_joint(&mut self, scheduled_node: NodeId) -> Result<(), ClusterError> {
        let Some(node) = self.leader().filter(|id| {
            self.nodes
                .get(id)
                .and_then(|slot| slot.driver.as_ref())
                .is_some_and(|driver| driver.membership().2)
        }) else {
            self.schedule(
                self.now + JOINT_RETRY,
                ClusterEventKind::LeaveJoint(scheduled_node),
            );
            return Ok(());
        };
        let host_time = self.host_time(node);
        let effects = {
            let slot = self
                .nodes
                .get_mut(&node)
                .expect("invariant: node slot exists");
            let driver = slot
                .driver
                .as_mut()
                .expect("invariant: up node has a driver");
            match driver.leave_joint(host_time) {
                Ok((_, effects)) => effects,
                Err(HostError::Node(NodeError::Raft(cc_raft::RaftError::Busy))) => {
                    self.schedule(
                        self.now + JOINT_RETRY,
                        ClusterEventKind::LeaveJoint(scheduled_node),
                    );
                    return Ok(());
                }
                Err(error) => return Err(cluster_host_error(node, error)),
            }
        };
        self.record(self.now, Some(node), EventKind::ConfChange, Vec::new());
        self.consume_effects(node, effects)
    }

    fn handle_tick(&mut self, id: NodeId) -> Result<(), ClusterError> {
        if self.is_up(id) {
            self.drive_node(id, Input::Tick)?;
        }
        Ok(())
    }

    fn handle_timer(
        &mut self,
        id: NodeId,
        timer_id: TimerId,
        generation: u64,
    ) -> Result<(), ClusterError> {
        let armed = self.nodes.get(&id).and_then(|slot| {
            slot.driver.as_ref().and_then(|driver| {
                driver
                    .armed_timers()
                    .find(|(armed_id, _, armed_generation)| {
                        *armed_id == timer_id && *armed_generation == generation
                    })
                    .map(|(_, at, _)| at)
            })
        });
        if armed != Some(self.now) || !self.is_up(id) {
            return Ok(());
        }
        self.drive_node(
            id,
            Input::TimerFired {
                id: timer_id,
                generation,
            },
        )
    }

    #[cfg(test)]
    fn handle_message(
        &mut self,
        from: NodeId,
        to: NodeId,
        frame: Vec<u8>,
    ) -> Result<(), ClusterError> {
        self.handle_delivery(from, to, frame, false, false)
    }

    fn handle_delivery(
        &mut self,
        from: NodeId,
        to: NodeId,
        frame: Vec<u8>,
        charged: bool,
        expected_rejection: bool,
    ) -> Result<(), ClusterError> {
        if charged {
            self.network
                .complete(from, to, frame.len())
                .map_err(|_| ClusterError::Network { from, to })?;
        }
        if !self.is_up(to) {
            self.record(
                self.now,
                Some(to),
                EventKind::NetDrop,
                transport_fingerprint_from_frame(&frame, cc_raft::PROTOCOL_VERSION, &[]),
            );
            return Ok(());
        }
        let (wire, used) = match decode_peer_frame(&frame) {
            Ok(decoded) => decoded,
            Err(_) if expected_rejection => {
                self.record(
                    self.now,
                    Some(to),
                    EventKind::NetDrop,
                    transport_fingerprint_from_frame(&frame, cc_raft::PROTOCOL_VERSION, &[]),
                );
                return Ok(());
            }
            Err(_) => return Err(ClusterError::Network { from, to }),
        };
        if used != frame.len() || wire.proto_version != cc_raft::PROTOCOL_VERSION {
            return Err(ClusterError::Network { from, to });
        }
        self.record(
            self.now,
            Some(to),
            EventKind::NetRecv,
            transport_fingerprint_from_frame(&frame, wire.proto_version, &wire.payload),
        );
        match self.drive_node(to, Input::Recv { from, msg: wire }) {
            // A malformed inner CCRP frame is an untrusted datagram, not a
            // fatal host condition. The Driver owns the decoder; the network
            // host records the drop without duplicating it.
            Err(ClusterError::Host {
                error: HostError::Node(NodeError::Environment("peer CCRP")),
                ..
            }) if expected_rejection => {
                self.record(
                    self.now,
                    Some(to),
                    EventKind::NetDrop,
                    transport_fingerprint_from_frame(&frame, cc_raft::PROTOCOL_VERSION, &[]),
                );
                Ok(())
            }
            other => other,
        }
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
        let command = encode_command(&command_for(&operation));
        let kind = operation_kind(&operation);
        let id = self.next_operation_id;
        self.next_operation_id = self.next_operation_id.saturating_add(1);
        let mut history_operation = Operation::open(id, kind, self.now);
        history_operation.client = client;
        history_operation.sequence = sequence;
        history_operation.deadline = match &operation {
            WorkloadOperation::Set { ttl: Some(ttl), .. } => Some(self.host_time(leader) + *ttl),
            _ => None,
        };
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
        // Reads pass through Driver as ordinary unsessioned commands; the
        // core chooses its ReadIndex path from the canonical command bytes.
        let input = Input::ClientRequest {
            client: ClientId::new(client),
            req: cc_core::RequestSeq::new(sequence),
            session: None,
            command,
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
            })
            | Err(ClusterError::Host {
                error:
                    HostError::Node(
                        NodeError::NotLeader
                        | NodeError::Raft(
                            cc_raft::RaftError::Busy
                            | cc_raft::RaftError::NotLeader
                            | cc_raft::RaftError::ReadBarrierNotReady,
                        ),
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
            fault_action_json(&action).into_bytes(),
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
                self.frame_faults.clear();
                self.replay_faults.clear();
                self.replay_frames.clear();
                self.network.clear_injected_delays();
            }
            FaultAction::Crash { node } => {
                // A crash is a process death: every byte of volatile state goes
                // with it. Dropping the composition is what makes the restart
                // path exercise recovery instead of resuming a paused node.
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.status = cc_sim::NodeStatus::Crashed;
                    slot.driver = None;
                    slot.persistence_ready_at = None;
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
                self.recover_node(node)?;
                let recovered = self.nodes.get(&node).is_some_and(|slot| {
                    slot.status != cc_sim::NodeStatus::StorageFault && slot.driver.is_some()
                });
                if recovered {
                    if let Some(slot) = self.nodes.get_mut(&node) {
                        slot.status = cc_sim::NodeStatus::Up;
                    }
                    self.schedule(self.now, ClusterEventKind::Tick(node));
                }
            }
            FaultAction::Wipe { node } => {
                let disk_model = self.spec.config.disk_model;
                let seed = self.spec.seed;
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.status = cc_sim::NodeStatus::Wiped;
                    slot.driver = None;
                    slot.persistence_ready_at = None;
                    slot.disk = SimDisk::with_model(disk_model, seed, node);
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
            FaultAction::SlowDisk { node, slow } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.disk.set_slow_disk(slow);
                }
            }
            FaultAction::EnospcFrom { node } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.disk.set_enospc(true);
                }
            }
            FaultAction::BitRotAtRest { node, file, offset } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.disk.inject_bitrot(file, offset);
                }
            }
            FaultAction::DiskQuota { node, bytes } => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.disk.set_quota(Some(bytes));
                }
            }
            FaultAction::LinkDegrade { from, to, config } => {
                self.network
                    .configure(from, to, config)
                    .map_err(|_| ClusterError::Network { from, to })?;
            }
            FaultAction::CorruptFrame {
                from,
                to,
                nth,
                byte,
                bit,
            } => {
                if nth == 0 || bit >= 8 {
                    return Err(ClusterError::Network { from, to });
                }
                self.frame_faults
                    .entry((from, to))
                    .or_default()
                    .push(FrameFault::Corrupt(CorruptFrameFault { nth, byte, bit }));
            }
            FaultAction::TruncateFrame {
                from,
                to,
                nth,
                keep,
            } => {
                if nth == 0 {
                    return Err(ClusterError::Network { from, to });
                }
                self.frame_faults
                    .entry((from, to))
                    .or_default()
                    .push(FrameFault::Truncate { nth, keep });
            }
            FaultAction::ReplayFrame { from, to, nth, at } => {
                if nth == 0 || at < self.now {
                    return Err(ClusterError::Network { from, to });
                }
                self.replay_faults
                    .entry((from, to))
                    .or_default()
                    .push(ReplayFrameFault { nth, at });
            }
            FaultAction::DelayLink { from, to, extra } => {
                self.network
                    .set_injected_delay(from, to, extra)
                    .map_err(|_| ClusterError::Network { from, to })?;
            }
            FaultAction::MutateRaftAndRechecksum {
                from,
                to,
                nth,
                mutation,
            } => {
                if nth == 0 {
                    return Err(ClusterError::Network { from, to });
                }
                self.frame_faults
                    .entry((from, to))
                    .or_default()
                    .push(FrameFault::Mutate { nth, mutation });
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
                    let now = self.host_time(leader);
                    let slot = self
                        .nodes
                        .get_mut(&leader)
                        .expect("invariant: leader slot exists");
                    let driver = slot
                        .driver
                        .as_mut()
                        .expect("invariant: leader has a driver");
                    if driver.membership().0 == target || driver.membership().2 {
                        // Already there, or a transition is still open.
                        return Ok(());
                    }
                    match driver.enter_joint(now, target) {
                        Ok((_, effects)) => effects,
                        // A leader that cannot open a transition right now is a
                        // legitimate outcome, not a host error.
                        Err(HostError::Node(NodeError::Raft(_))) => return Ok(()),
                        Err(error) => return Err(cluster_host_error(leader, error)),
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

    fn drive_node(&mut self, id: NodeId, input: Input) -> Result<(), ClusterError> {
        if !self.is_up(id) {
            return Ok(());
        }
        if let Some(ready_at) = self
            .nodes
            .get(&id)
            .and_then(|slot| slot.persistence_ready_at)
        {
            self.schedule(
                ready_at + Duration::from_nanos(1),
                ClusterEventKind::DeferredInput { node: id, input },
            );
            return Ok(());
        }
        let now = self.host_time(id);
        let (before_role, was_blocked, poll, effects) = {
            let slot = self
                .nodes
                .get_mut(&id)
                .expect("invariant: node slot exists");
            let NodeSlot { driver, disk, .. } = slot;
            let driver = driver.as_mut().expect("invariant: up node has a driver");
            let before = driver.role();
            let was_blocked = driver.footprint().blocked;
            let mut blocks = SimBlockSource { disk };
            let (poll, effects) = driver
                .deliver(now, input.clone(), &mut blocks)
                .map_err(|error| cluster_host_error(id, error))?;
            let _ = driver.footprint();
            (before, was_blocked, poll, effects)
        };
        if let DriverPoll::BlockedUntil(until) = poll {
            if was_blocked {
                let slot = self
                    .nodes
                    .get_mut(&id)
                    .expect("invariant: node slot exists");
                slot.driver
                    .as_mut()
                    .expect("invariant: up node has a driver")
                    .enqueue(input)
                    .map_err(|error| cluster_host_error(id, error))?;
                let _ = slot
                    .driver
                    .as_ref()
                    .expect("invariant: up node has a driver")
                    .footprint();
            } else {
                self.schedule(until, ClusterEventKind::DriverServiceComplete { node: id });
            }
            return Ok(());
        }
        let after_role = self
            .nodes
            .get(&id)
            .and_then(|slot| slot.driver.as_ref())
            .map(Driver::role)
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

    fn complete_driver_service(&mut self, node: NodeId) -> Result<(), ClusterError> {
        if !self.is_up(node) {
            return Ok(());
        }
        let now = self.host_time(node);
        let (poll, effects) = {
            let slot = self
                .nodes
                .get_mut(&node)
                .expect("invariant: node slot exists");
            let result = slot
                .driver
                .as_mut()
                .expect("invariant: up node has a driver")
                .release_ready(now)
                .map_err(|error| cluster_host_error(node, error))?;
            let _ = slot
                .driver
                .as_ref()
                .expect("invariant: up node has a driver")
                .footprint();
            result
        };
        if let DriverPoll::BlockedUntil(until) = poll {
            self.schedule(until, ClusterEventKind::DriverServiceComplete { node });
            return Ok(());
        }
        self.consume_effects(node, effects)?;
        // Release queued inputs in the Driver's I/O/timer/peer/client order.
        loop {
            let (input, poll, effects) = {
                let slot = self
                    .nodes
                    .get_mut(&node)
                    .expect("invariant: node slot exists");
                let NodeSlot { driver, disk, .. } = slot;
                let mut blocks = SimBlockSource { disk };
                let result = driver
                    .as_mut()
                    .expect("invariant: up node has a driver")
                    .deliver_next_with_input(now, &mut blocks)
                    .map_err(|error| cluster_host_error(node, error))?;
                let _ = driver
                    .as_ref()
                    .expect("invariant: up node has a driver")
                    .footprint();
                result
            };
            self.consume_effects(node, effects)?;
            if let DriverPoll::BlockedUntil(until) = poll {
                self.schedule(until, ClusterEventKind::DriverServiceComplete { node });
                break;
            }
            if input.is_none() {
                break;
            }
        }
        Ok(())
    }

    fn consume_effects(
        &mut self,
        source: NodeId,
        effects: Vec<Effect>,
    ) -> Result<(), ClusterError> {
        for effect in effects {
            match effect {
                Effect::Send { to, msg } => self.send_wire(source, to, msg)?,
                Effect::DiskWrite {
                    file,
                    at,
                    bytes,
                    id,
                } => self.begin_persistence(source, file, at, bytes, id)?,
                Effect::DiskFsync { file, id } => self.begin_fsync(source, file, id)?,
                Effect::DiskRead { file, at, len, id } => {
                    self.begin_read(source, file, at, len, id)?
                }
                Effect::DiskRename { from, to, id } => self.begin_rename(source, from, to, id)?,
                Effect::DiskSyncDir { id } => self.begin_sync_dir(source, id)?,
                Effect::ClientReply { client, req, reply } => {
                    let reply = decode_reply(&reply).map_err(|_| ClusterError::Node {
                        node: source,
                        error: NodeError::Environment("CCKR reply"),
                    })?;
                    self.complete_client(source, client.get(), req.get(), reply);
                }
                Effect::SetTimer { id, fire_at } => {
                    let generation = self
                        .nodes
                        .get(&source)
                        .and_then(|slot| slot.driver.as_ref())
                        .and_then(|driver| {
                            driver
                                .armed_timers()
                                .find(|(timer, _, _)| *timer == id)
                                .map(|(_, _, generation)| generation)
                        })
                        .ok_or(ClusterError::Node {
                            node: source,
                            error: NodeError::Environment("unarmed driver timer"),
                        })?;
                    self.record(
                        self.now,
                        Some(source),
                        EventKind::TimerSet,
                        fire_at.as_nanos().to_le_bytes().to_vec(),
                    );
                    self.schedule(
                        fire_at,
                        ClusterEventKind::Timer {
                            node: source,
                            id,
                            generation,
                        },
                    );
                }
                Effect::CancelTimer { .. } => {}
                Effect::Trace(event) => self.record(
                    self.now,
                    Some(source),
                    EventKind::CheckerNote,
                    event.payload,
                ),
                Effect::DiskTruncate { .. }
                | Effect::DiskCreateTemp { .. }
                | Effect::DiskDelete { .. } => {
                    return Err(ClusterError::Node {
                        node: source,
                        error: NodeError::Environment("unsupported simulator storage effect"),
                    });
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn send_message(&mut self, message: cc_raft::Message) -> Result<(), ClusterError> {
        let from = message.from;
        let to = message.to;
        let wire = cc_cluster::encode_peer_effect(&message)
            .map_err(|_| ClusterError::Network { from, to })?;
        self.send_wire(from, to, wire)
    }

    fn send_wire(&mut self, from: NodeId, to: NodeId, wire: WireMsg) -> Result<(), ClusterError> {
        let mut frame = encode_peer_frame(&wire).map_err(|_| ClusterError::Network { from, to })?;
        let mut expected_rejection = false;
        let ordinal = *self
            .frame_ordinals
            .entry((from, to))
            .and_modify(|value| *value = value.saturating_add(1))
            .or_insert(1);
        if let Some(faults) = self.frame_faults.get(&(from, to)) {
            for fault in faults
                .iter()
                .copied()
                .filter(|fault| fault.nth() == ordinal)
            {
                match fault {
                    FrameFault::Corrupt(fault) => {
                        if fault.bit >= 8 || fault.byte >= frame.len() {
                            return Err(ClusterError::Network { from, to });
                        }
                        frame[fault.byte] ^= 1_u8 << fault.bit;
                        expected_rejection = true;
                    }
                    FrameFault::Truncate { keep, .. } => {
                        if keep >= frame.len() {
                            return Err(ClusterError::Network { from, to });
                        }
                        frame.truncate(keep);
                        expected_rejection = true;
                    }
                    FrameFault::Mutate { mutation, .. } => {
                        mutate_and_rechecksum(&mut frame, mutation)
                            .map_err(|_| ClusterError::Network { from, to })?;
                        expected_rejection = true;
                    }
                }
            }
        }
        let fingerprint =
            transport_fingerprint_from_frame(&frame, wire.proto_version, &wire.payload);
        let replay_at: Vec<Time> = self
            .replay_faults
            .get(&(from, to))
            .into_iter()
            .flatten()
            .filter(|fault| fault.nth == ordinal)
            .map(|fault| fault.at)
            .collect();
        if let Some(previous) = self.replay_frames.get(&(from, to)).cloned() {
            for at in replay_at {
                self.schedule(
                    at.max(self.now),
                    ClusterEventKind::Message {
                        from,
                        to,
                        frame: previous.clone(),
                        charged: false,
                        expected_rejection: false,
                    },
                );
            }
        }
        self.replay_frames.insert((from, to), frame.clone());
        let replay_bytes = self.replay_frames.values().fold(0_u64, |total, frame| {
            total.saturating_add(u64::try_from(frame.len()).unwrap_or(u64::MAX))
        });
        self.peak_replay_bytes = self.peak_replay_bytes.max(replay_bytes);
        if !self.is_up(from) || !self.is_up(to) {
            self.record(
                self.now,
                Some(from),
                EventKind::NetDrop,
                fingerprint.clone(),
            );
            return Ok(());
        }
        let decisions = self
            .network
            .send(self.now, from, to, frame)
            .map_err(|_| ClusterError::Network { from, to })?;
        let mut delivered = false;
        for decision in decisions {
            match decision {
                NetworkDecision::Delivered(delivery) => {
                    delivered = true;
                    if delivery.at <= self.spec.end_time {
                        self.record(
                            self.now,
                            Some(from),
                            EventKind::NetSend,
                            fingerprint.clone(),
                        );
                        self.schedule(
                            delivery.at,
                            ClusterEventKind::Message {
                                from,
                                to,
                                frame: delivery.payload,
                                charged: true,
                                expected_rejection,
                            },
                        );
                    } else {
                        // Scheduling stops at the declared horizon, but the
                        // network already reserved this delivery. Releasing
                        // it here is the modeled cancellation outcome rather
                        // than leaking capacity into a later run.
                        self.network
                            .complete(from, to, delivery.payload.len())
                            .map_err(|_| ClusterError::Network { from, to })?;
                        self.record(
                            self.now,
                            Some(from),
                            EventKind::NetDrop,
                            fingerprint.clone(),
                        );
                    }
                }
                NetworkDecision::Dropped => {
                    self.record(
                        self.now,
                        Some(from),
                        EventKind::NetDrop,
                        fingerprint.clone(),
                    );
                }
            }
        }
        let _ = delivered;
        Ok(())
    }

    fn begin_persistence(
        &mut self,
        node: NodeId,
        file: FileId,
        at: u64,
        bytes: Vec<u8>,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let write_delay = self
            .nodes
            .get_mut(&node)
            .map(|slot| slot.disk.service_time(DiskOperation::Write))
            .ok_or(ClusterError::Node {
                node,
                error: NodeError::Durability,
            })?;
        if let Some(slot) = self.nodes.get_mut(&node) {
            slot.persistence_ready_at = Some(self.now + write_delay);
            if file == WAL_FILE {
                slot.wal_end = at.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            }
        }
        self.record(self.now, Some(node), EventKind::IoIssue, bytes.clone());
        self.schedule(
            self.now + write_delay,
            ClusterEventKind::DiskWriteComplete {
                node,
                file,
                at,
                bytes,
                id,
            },
        );
        Ok(())
    }

    fn handle_disk_write_complete(
        &mut self,
        node: NodeId,
        file: FileId,
        at: u64,
        bytes: Vec<u8>,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let result = {
            let Some(slot) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if slot.status != cc_sim::NodeStatus::Up || slot.driver.is_none() {
                return Ok(());
            }
            slot.disk.write(file, at, &bytes)
        };
        match result {
            Ok(_) => self.complete_driver_io(
                node,
                id,
                IoResult::Written {
                    len: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
                },
            ),
            Err(error) => self.complete_driver_io(node, id, IoResult::Failed(error)),
        }
    }

    fn begin_fsync(
        &mut self,
        node: NodeId,
        file: FileId,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let delay = self
            .nodes
            .get_mut(&node)
            .map(|slot| slot.disk.service_time(DiskOperation::Fsync))
            .ok_or(ClusterError::Node {
                node,
                error: NodeError::Durability,
            })?;
        if let Some(slot) = self.nodes.get_mut(&node) {
            slot.persistence_ready_at = Some(self.now + delay);
        }
        self.schedule(
            self.now + delay,
            ClusterEventKind::DiskFsyncComplete { node, file, id },
        );
        Ok(())
    }

    fn begin_read(
        &mut self,
        node: NodeId,
        file: FileId,
        at: u64,
        len: u32,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let delay = self
            .nodes
            .get_mut(&node)
            .map(|slot| slot.disk.service_time(DiskOperation::Read))
            .ok_or(ClusterError::Node {
                node,
                error: NodeError::Durability,
            })?;
        self.schedule(
            self.now + delay,
            ClusterEventKind::DiskReadComplete {
                node,
                file,
                at,
                len,
                id,
            },
        );
        Ok(())
    }

    fn begin_rename(
        &mut self,
        node: NodeId,
        from: FileId,
        to: FileId,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let delay = self
            .nodes
            .get_mut(&node)
            .map(|slot| slot.disk.service_time(DiskOperation::Rename))
            .ok_or(ClusterError::Node {
                node,
                error: NodeError::Durability,
            })?;
        self.schedule(
            self.now + delay,
            ClusterEventKind::DiskRenameComplete { node, from, to, id },
        );
        Ok(())
    }

    fn begin_sync_dir(&mut self, node: NodeId, id: cc_core::IoId) -> Result<(), ClusterError> {
        let delay = self
            .nodes
            .get_mut(&node)
            .map(|slot| slot.disk.service_time(DiskOperation::SyncDir))
            .ok_or(ClusterError::Node {
                node,
                error: NodeError::Durability,
            })?;
        self.schedule(
            self.now + delay,
            ClusterEventKind::DiskSyncDirComplete { node, id },
        );
        Ok(())
    }

    fn handle_disk_fsync_complete(
        &mut self,
        node: NodeId,
        file: FileId,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let result = {
            let Some(slot) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if slot.status != cc_sim::NodeStatus::Up || slot.driver.is_none() {
                return Ok(());
            }
            slot.disk.fsync(file)
        };
        match result {
            Ok(_) => self.complete_driver_io(node, id, IoResult::Fsynced),
            Err(error) => self.complete_driver_io(node, id, IoResult::Failed(error)),
        }
    }

    fn handle_disk_read_complete(
        &mut self,
        node: NodeId,
        file: FileId,
        at: u64,
        len: u32,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let result = {
            let Some(slot) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if slot.status != cc_sim::NodeStatus::Up || slot.driver.is_none() {
                return Ok(());
            }
            slot.disk.read(file, at, len)
        };
        match result {
            Ok(result) => self.complete_driver_io(node, id, result),
            Err(error) => self.complete_driver_io(node, id, IoResult::Failed(error)),
        }
    }

    fn handle_disk_rename_complete(
        &mut self,
        node: NodeId,
        from: FileId,
        to: FileId,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let result = {
            let Some(slot) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if slot.status != cc_sim::NodeStatus::Up || slot.driver.is_none() {
                return Ok(());
            }
            let result = slot.disk.rename(from, to);
            if result.is_ok() && to == WAL_FILE {
                slot.wal_end = slot
                    .disk
                    .visible(WAL_FILE)
                    .map_or(0, |bytes| bytes.len() as u64);
            }
            result
        };
        match result {
            Ok(result) => self.complete_driver_io(node, id, result),
            Err(error) => self.complete_driver_io(node, id, IoResult::Failed(error)),
        }
    }

    fn handle_disk_sync_dir_complete(
        &mut self,
        node: NodeId,
        id: cc_core::IoId,
    ) -> Result<(), ClusterError> {
        let result = {
            let Some(slot) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            if slot.status != cc_sim::NodeStatus::Up || slot.driver.is_none() {
                return Ok(());
            }
            slot.disk.sync_dir()
        };
        match result {
            Ok(result) => self.complete_driver_io(node, id, result),
            Err(error) => self.complete_driver_io(node, id, IoResult::Failed(error)),
        }
    }

    fn complete_driver_io(
        &mut self,
        node: NodeId,
        id: cc_core::IoId,
        result: IoResult,
    ) -> Result<(), ClusterError> {
        let was_fsync = matches!(result, IoResult::Fsynced);
        let now = self.host_time(node);
        let (before_snapshot, outcome, after_snapshot) = {
            let Some(slot) = self.nodes.get_mut(&node) else {
                return Ok(());
            };
            let NodeSlot { driver, disk, .. } = slot;
            let Some(driver) = driver.as_mut() else {
                return Ok(());
            };
            let before = driver.node().raft.snapshot_base().0;
            let mut blocks = SimBlockSource { disk };
            let outcome = driver.deliver(now, Input::IoDone { id, result }, &mut blocks);
            let after = driver.node().raft.snapshot_base().0;
            (before, outcome, after)
        };
        if after_snapshot > before_snapshot {
            self.record(
                self.now,
                Some(node),
                EventKind::SnapshotInstall,
                after_snapshot.get().to_le_bytes().to_vec(),
            );
        }
        match outcome {
            Ok((DriverPoll::Ready, effects)) => {
                if was_fsync {
                    if let Some(slot) = self.nodes.get_mut(&node) {
                        slot.persistence_ready_at = None;
                    }
                    self.record(self.now, Some(node), EventKind::IoDone, Vec::new());
                    self.record(self.now, Some(node), EventKind::Flush, Vec::new());
                }
                self.consume_effects(node, effects)
            }
            Ok((DriverPoll::BlockedUntil(until), _)) => {
                self.schedule(until, ClusterEventKind::DriverServiceComplete { node });
                Ok(())
            }
            Err(error) => {
                if let Some(slot) = self.nodes.get_mut(&node) {
                    slot.status = cc_sim::NodeStatus::StorageFault;
                    slot.driver = None;
                    slot.persistence_ready_at = None;
                }
                self.record(self.now, Some(node), EventKind::IoLost, Vec::new());
                let _ = error;
                Ok(())
            }
        }
    }

    fn recover_node(&mut self, id: NodeId) -> Result<(), ClusterError> {
        let now = self.host_time(id);
        let Some(slot) = self.nodes.get_mut(&id) else {
            return Ok(());
        };
        if slot.disk.durable(WAL_FILE).is_some() && slot.disk.verify_durable(WAL_FILE).is_err() {
            // A durable checksum mismatch is not a torn tail.  Recovery has no
            // authority to guess which bytes were intended, so preserve the
            // image for inspection and fail-stop this node before it can vote,
            // answer a read, or emit a reply.
            slot.status = cc_sim::NodeStatus::StorageFault;
            slot.driver = None;
            slot.persistence_ready_at = None;
            self.record(
                self.now,
                Some(id),
                EventKind::IoLost,
                b"corrupt-wal".to_vec(),
            );
            return Ok(());
        }
        let durable = slot.disk.durable(WAL_FILE).unwrap_or_default().to_vec();
        let store_wal_file = FileId::StoreWal { segment: 0 };
        let store_wal_bytes = slot
            .disk
            .durable(store_wal_file)
            .unwrap_or_default()
            .to_vec();
        let recovered_store =
            cc_store::recover_store_wal(&store_wal_bytes).map_err(|_| ClusterError::Node {
                node: id,
                error: NodeError::Durability,
            })?;
        if recovered_store.torn_tail_truncated {
            slot.disk
                .truncate(store_wal_file, recovered_store.bytes_consumed)
                .and_then(|_| slot.disk.fsync(store_wal_file))
                .map_err(|_| ClusterError::Node {
                    node: id,
                    error: NodeError::Durability,
                })?;
        }
        if durable.is_empty() {
            // A wiped disk starts from a sealed Join-origin Genesis. It does
            // not receive an out-of-band state copy: ordinary peer traffic
            // backtracks and replicates the surviving log prefix.
            let mut genesis = slot.genesis.clone();
            genesis.origin = cc_log::Origin::Join;
            let bytes = cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(
                Box::new(genesis.clone()),
            ))
            .map_err(|_| ClusterError::Node {
                node: id,
                error: NodeError::Durability,
            })?;
            slot.disk
                .write(WAL_FILE, 0, &bytes)
                .and_then(|_| slot.disk.fsync(WAL_FILE))
                .map_err(|_| ClusterError::Node {
                    node: id,
                    error: NodeError::Durability,
                })?;
            let bootstrap = genesis.membership.clone();
            slot.genesis = genesis;
            slot.wal_end = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            let mut driver = Driver::boot_with_offsets_and_genesis(
                slot.config,
                BootState::Fresh { bootstrap },
                slot.wal_end,
                recovered_store.bytes_consumed,
                slot.genesis.clone(),
            )
            .map_err(|error| cluster_host_error(id, error))?;
            driver
                .node_mut()
                .recover_durable_applies(&recovered_store)
                .map_err(|error| ClusterError::Node { node: id, error })?;
            driver.node_mut().raft.rearm_election(now);
            slot.driver = Some(driver);
            slot.persistence_ready_at = None;
        } else {
            let recovered =
                cc_log::recover_framed_record_stream(&durable).map_err(|_| ClusterError::Node {
                    node: id,
                    error: NodeError::Durability,
                })?;
            if recovered.torn_tail_truncated {
                slot.disk
                    .truncate(WAL_FILE, recovered.bytes_consumed)
                    .and_then(|_| slot.disk.fsync(WAL_FILE))
                    .map_err(|_| ClusterError::Node {
                        node: id,
                        error: NodeError::Durability,
                    })?;
            }
            let state = recovered.state;
            slot.wal_end = recovered.bytes_consumed;
            slot.genesis = state.genesis.clone();
            let mut driver = Driver::boot_with_offsets_and_genesis(
                slot.config,
                BootState::Recovered(Box::new(RecoveredNode {
                    hard_state: state.hard_state,
                    log_base: (state.base_index, state.base_term),
                    entries: state.entries,
                    membership: state.genesis.membership,
                    cluster_policy: state.genesis.policy,
                    snapshot: None,
                    durable_applied: (state.base_index, state.base_term),
                })),
                slot.wal_end,
                recovered_store.bytes_consumed,
                slot.genesis.clone(),
            )
            .map_err(|error| cluster_host_error(id, error))?;
            driver
                .node_mut()
                .recover_durable_applies(&recovered_store)
                .map_err(|error| ClusterError::Node { node: id, error })?;
            driver.node_mut().raft.rearm_election(now);
            slot.driver = Some(driver);
            slot.persistence_ready_at = None;
        }
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
                let driver = slot.driver.as_ref()?;
                (driver.role() == Role::Leader)
                    .then_some((*id, driver.node().raft.hard_state.term.get()))
            })
            .max_by_key(|(id, term)| (*term, *id))
            .map(|(id, _)| id)
    }

    fn is_up(&self, id: NodeId) -> bool {
        self.nodes
            .get(&id)
            .is_some_and(|slot| slot.status == cc_sim::NodeStatus::Up && slot.driver.is_some())
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
        disk_model: cc_sim::DiskModel::universal(),
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
        FaultProfile::Rough
        | FaultProfile::Membership
        | FaultProfile::Corruption
        | FaultProfile::Ttl => {
            config.base_delay = Duration::from_millis(2);
            config.jitter = cc_core::DelayDist::Uniform {
                low: Duration::default(),
                high: Duration::from_millis(3),
            };
            config.drop = cc_core::P16::new(256);
            config.duplicate = cc_core::P16::new(128);
        }
        FaultProfile::Brutal | FaultProfile::Wipe | FaultProfile::Starve => {
            config.base_delay = Duration::from_millis(3);
            config.drop = cc_core::P16::new(512);
            config.duplicate = cc_core::P16::new(256);
        }
    }
    config
}

fn simulated_cluster_id(seed: Seed) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&seed.0.to_le_bytes());
    id[8..].copy_from_slice(&(seed.0 ^ 0x4343_4c52_5349_4d31).to_le_bytes());
    if id.iter().all(|byte| *byte == 0) {
        id[0] = 1;
    }
    id
}

fn command_for(operation: &WorkloadOperation) -> KvCommand {
    match operation {
        WorkloadOperation::Get { key } => KvCommand::Get { key: key.clone() },
        WorkloadOperation::Set { key, value, ttl } => KvCommand::Set {
            key: key.clone(),
            value: value.clone(),
            ttl: *ttl,
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
        WorkloadOperation::Set { key, value, .. } => OperationKind::Set {
            key: key.clone(),
            value: value.clone(),
        },
        WorkloadOperation::Del { key } => OperationKind::Del { key: key.clone() },
        WorkloadOperation::Incr { key } => OperationKind::Incr { key: key.clone() },
    }
}

fn reply_to_outcome(kind: &OperationKind, reply: &KvReply) -> Outcome {
    match (kind, reply) {
        (_, KvReply::Error(_) | KvReply::BatchError { .. }) => Outcome::Error,
        (OperationKind::Set { .. }, KvReply::Ok) => Outcome::Ok,
        (OperationKind::Del { .. }, KvReply::Integer(_)) => Outcome::Ok,
        (OperationKind::Get { .. }, KvReply::Value(value)) => Outcome::Value(value.clone()),
        (OperationKind::Incr { .. }, KvReply::Integer(value)) => Outcome::Integer(*value),
        (_, KvReply::Ok) => Outcome::Ok,
        (_, KvReply::Value(value)) => Outcome::Value(value.clone()),
        (_, KvReply::Integer(value)) => Outcome::Integer(*value),
        (_, KvReply::Cas(value)) => Outcome::Cas(*value),
        (_, KvReply::Conditional(value)) => Outcome::Cas(*value),
        (_, KvReply::Scan(_)) => Outcome::Error,
        (_, KvReply::Batch(_)) => Outcome::Error,
    }
}

/// Stable trace vocabulary for a peer frame: local fingerprint format,
/// negotiated semantic version, raw CCRP tag (zero if the CCPF frame is not
/// decodable), and the final outer-frame CRC. This is trace evidence, not a
/// second peer codec.
fn transport_fingerprint_from_frame(
    frame: &[u8],
    semantic_hint: u16,
    payload_hint: &[u8],
) -> Vec<u8> {
    let (semantic, tag) = decode_peer_frame(frame).ok().map_or(
        (semantic_hint, payload_hint.get(32).copied().unwrap_or(0)),
        |(wire, _)| {
            (
                wire.proto_version,
                wire.payload.get(32).copied().unwrap_or(0),
            )
        },
    );
    let mut bytes = Vec::with_capacity(9);
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&semantic.to_le_bytes());
    bytes.push(tag);
    bytes.extend_from_slice(&cc_core::crc32c(frame).to_le_bytes());
    bytes
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

    #[cfg(any(
        feature = "kata01",
        feature = "kata02",
        feature = "kata03",
        feature = "kata04",
        feature = "kata05",
    ))]
    #[test]
    fn trap_active_kata_is_visible_in_trace_and_artifact_type() {
        let cluster = SimCluster::new(
            RunSpec::standard(Seed::new(0), FaultProfile::Calm),
            RecorderLevel::Gate,
        )
        .expect("synthetic cluster");
        let event = cluster
            .recorder
            .trace()
            .events
            .as_slice()
            .first()
            .expect("synthetic marker");
        assert_eq!(event.kind, EventKind::SyntheticKataEnabled);
        assert!(event.payload.starts_with(b"synthetic=true kata="));
        assert!(ACTIVE_KATA.is_some());
    }

    #[test]
    fn trap_synthetic_artifact_is_rejected_by_claims_and_museum() {
        assert!(artifact_is_claim_eligible(
            br#"{"fixture_version":1,"synthetic":false}"#
        ));
        assert!(!artifact_is_claim_eligible(
            br#"{ "fixture_version": 1, "synthetic" : true }"#
        ));
        let schema = include_str!("../../../exhibits/schema.json");
        let museum = include_str!("../../../theater/src/museum.ts");
        assert!(schema.contains("\"synthetic\": { \"const\": false }"));
        assert!(museum.contains("Synthetic artifact"));
    }
    use cc_sim::SlowDisk;

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
    fn trap_sim_live_read_sees_page_cache_but_restart_sees_durable_bytes() {
        let file = FileId::Sst { file_no: 7 };
        let mut disk = SimDisk::new();
        disk.set_slow_disk(SlowDisk {
            read_extra: Duration::from_nanos(9),
            ..SlowDisk::default()
        });
        disk.write(file, 0, b"old").expect("old page-cache write");
        disk.fsync(file).expect("old durable image");
        disk.write(file, 0, b"new").expect("new page-cache write");
        let live = SimBlockSource { disk: &mut disk }
            .read_block(file, 0, 3)
            .expect("live read");
        assert_eq!(live.bytes, b"new");
        assert_eq!(live.service, Duration::from_nanos(9));
        disk.crash();
        let recovered = SimBlockSource { disk: &mut disk }
            .read_block(file, 0, 3)
            .expect("recovery read");
        assert_eq!(recovered.bytes, b"old");
    }

    #[test]
    fn trap_ccpf_and_ccrp_semantic_versions_must_match() {
        let spec = RunSpec::standard(Seed::new(0x77), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        let message = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::PreVoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let frame = encode_peer_frame(&WireMsg::new(
            cc_raft::PROTOCOL_VERSION.saturating_sub(1),
            cc_raft::codec::encode(&message).expect("CCRP"),
        ))
        .expect("CCPF");
        assert!(matches!(
            cluster.handle_message(NodeId::new(1), NodeId::new(2), frame),
            Err(ClusterError::Network { .. })
        ));
    }

    #[test]
    fn trap_slow_disk_preserves_election_safety() {
        let mut spec = RunSpec::standard(Seed::new(0x7d), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(250_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::SlowDisk {
                node: NodeId::new(1),
                slow: SlowDisk {
                    write_extra: Duration::from_millis(50),
                    fsync_extra: Duration::from_millis(50),
                    ..SlowDisk::default()
                },
            })
            .expect("slow disk");
        let request = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let frame = encode_peer_frame(&WireMsg::new(
            cc_raft::PROTOCOL_VERSION,
            cc_raft::codec::encode(&request).expect("CCRP"),
        ))
        .expect("CCPF");
        cluster
            .handle_message(NodeId::new(2), NodeId::new(1), frame)
            .expect("vote request");

        cluster
            .process_until(Time::from_nanos(99_999_999))
            .expect("delayed write and fsync");
        assert!(
            cluster
                .recorder
                .trace()
                .events
                .iter()
                .all(|event| !(event.node == Some(NodeId::new(1))
                    && event.kind == EventKind::IoDone))
        );

        cluster
            .process_until(Time::from_nanos(101_000_000))
            .expect("delayed fsync completion");
        let trace = cluster.recorder.trace();
        let fsync_at = trace
            .events
            .iter()
            .find(|event| event.node == Some(NodeId::new(1)) && event.kind == EventKind::IoDone)
            .map(|event| event.time)
            .expect("node one fsync completion");
        assert_eq!(fsync_at, Time::from_nanos(100_000_000));
        assert!(
            trace
                .events
                .iter()
                .filter(|event| {
                    event.node == Some(NodeId::new(1)) && event.kind == EventKind::NetSend
                })
                .all(|event| event.time >= fsync_at)
        );
        assert_eq!(
            cluster
                .snapshot()
                .nodes
                .iter()
                .find(|node| node.id == 1)
                .expect("node one")
                .disk_service_delay_ns,
            Duration::from_millis(50).as_nanos()
        );
    }

    #[test]
    fn trap_crash_discards_a_delayed_durability_continuation() {
        let mut spec = RunSpec::standard(Seed::new(0x7e), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(250_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::SlowDisk {
                node: NodeId::new(1),
                slow: SlowDisk {
                    write_extra: Duration::from_millis(50),
                    fsync_extra: Duration::from_millis(50),
                    ..SlowDisk::default()
                },
            })
            .expect("slow disk");
        let request = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let frame = encode_peer_frame(&WireMsg::new(
            cc_raft::PROTOCOL_VERSION,
            cc_raft::codec::encode(&request).expect("CCRP"),
        ))
        .expect("CCPF");
        cluster
            .handle_message(NodeId::new(2), NodeId::new(1), frame)
            .expect("vote request");
        cluster.inject(FaultAction::Crash {
            node: NodeId::new(1),
        });
        cluster
            .advance(Duration::from_millis(150))
            .expect("stale completion is ignored after crash");
        let node = cluster
            .snapshot()
            .nodes
            .into_iter()
            .find(|node| node.id == 1)
            .expect("node one");
        assert_eq!(node.status, cc_sim::NodeStatus::Crashed);
        assert!(cluster.recorder.trace().events.iter().all(|event| {
            !(event.node == Some(NodeId::new(1))
                && matches!(event.kind, EventKind::IoDone | EventKind::Flush))
        }));
    }

    #[test]
    fn trap_enospc_fails_closed_before_a_vote_reply() {
        let mut spec = RunSpec::standard(Seed::new(0x7f), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(250_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::EnospcFrom {
                node: NodeId::new(1),
            })
            .expect("ENOSPC fault");
        let request = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let frame = encode_peer_frame(&WireMsg::new(
            cc_raft::PROTOCOL_VERSION,
            cc_raft::codec::encode(&request).expect("CCRP"),
        ))
        .expect("CCPF");
        cluster
            .handle_message(NodeId::new(2), NodeId::new(1), frame)
            .expect("vote request");
        cluster
            .advance(Duration::from_nanos(0))
            .expect("failed durability is contained");
        assert_eq!(
            cluster
                .snapshot()
                .nodes
                .into_iter()
                .find(|node| node.id == 1)
                .expect("node one")
                .status,
            cc_sim::NodeStatus::StorageFault
        );
        assert!(cluster.recorder.trace().events.iter().all(|event| {
            !(event.node == Some(NodeId::new(1)) && event.kind == EventKind::NetSend)
        }));
    }

    #[test]
    fn trap_bitrot_is_detected_not_served() {
        let mut spec = RunSpec::standard(Seed::new(0x80), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(250_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        let node = NodeId::new(1);
        {
            let slot = cluster.nodes.get_mut(&node).expect("node one");
            slot.disk.inject_bitrot(WAL_FILE, 0);
            slot.disk.fsync(WAL_FILE).expect("inject at-rest bit rot");
        }

        cluster
            .handle_fault(FaultAction::Crash { node })
            .expect("crash");
        cluster
            .handle_fault(FaultAction::Restart { node })
            .expect("recovery contains corruption");

        assert_eq!(
            cluster
                .snapshot()
                .nodes
                .into_iter()
                .find(|snapshot| snapshot.id == node.get())
                .expect("node one")
                .status,
            cc_sim::NodeStatus::StorageFault
        );
        assert!(cluster.recorder.trace().events.iter().any(|event| {
            event.node == Some(node)
                && event.kind == EventKind::IoLost
                && event.payload == b"corrupt-wal"
        }));
    }

    #[test]
    fn trap_enospc_never_loses_acked_writes() {
        let mut spec = RunSpec::standard(Seed::new(0x8a), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(4_000_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .advance(Duration::from_millis(800))
            .expect("elect leader");
        cluster
            .handle_client_issue(
                91,
                1,
                WorkloadOperation::Set {
                    key: b"durable".to_vec(),
                    value: b"acknowledged".to_vec(),
                    ttl: None,
                },
            )
            .expect("first write");
        cluster
            .advance(Duration::from_millis(500))
            .expect("commit first write");
        assert!(
            cluster.history.operations.iter().any(|operation| {
                operation.complete.is_some() && operation.outcome == Outcome::Ok
            })
        );
        let leader = cluster.leader().expect("leader");
        let faulted = [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
            .into_iter()
            .find(|node| *node != leader)
            .expect("follower");
        cluster
            .handle_fault(FaultAction::EnospcFrom { node: faulted })
            .expect("ENOSPC");
        cluster
            .handle_client_issue(
                91,
                2,
                WorkloadOperation::Set {
                    key: b"after-fault".to_vec(),
                    value: b"value".to_vec(),
                    ttl: None,
                },
            )
            .expect("write that reaches faulted follower");
        cluster
            .advance(Duration::from_millis(700))
            .expect("contain fault");
        for slot in cluster
            .nodes
            .values()
            .filter(|slot| slot.status == cc_sim::NodeStatus::Up)
        {
            assert_eq!(
                slot.driver
                    .as_ref()
                    .expect("healthy driver")
                    .node()
                    .kv
                    .store
                    .get(b"durable", None),
                Some(b"acknowledged".to_vec())
            );
        }
    }

    #[test]
    fn trap_storage_fault_serves_no_unproven_read() {
        let mut spec = RunSpec::standard(Seed::new(0x8b), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(2_000_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::EnospcFrom {
                node: NodeId::new(1),
            })
            .expect("ENOSPC");
        let request = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        let frame = encode_peer_frame(&WireMsg::new(
            cc_raft::PROTOCOL_VERSION,
            cc_raft::codec::encode(&request).expect("CCRP"),
        ))
        .expect("CCPF");
        cluster
            .handle_message(NodeId::new(2), NodeId::new(1), frame)
            .expect("vote request");
        cluster.advance(Duration::from_nanos(0)).expect("fail stop");
        let slot = cluster.nodes.get(&NodeId::new(1)).expect("node");
        assert_eq!(slot.status, cc_sim::NodeStatus::StorageFault);
        assert!(slot.driver.is_none());
        let replies_before = cluster
            .recorder
            .trace()
            .events
            .iter()
            .filter(|event| event.node == Some(NodeId::new(1)) && event.kind == EventKind::ClientOk)
            .count();
        cluster
            .drive_node(
                NodeId::new(1),
                Input::ClientRequest {
                    client: ClientId::new(1),
                    req: cc_core::RequestSeq::new(1),
                    session: None,
                    command: encode_command(&KvCommand::Get {
                        key: b"unproven".to_vec(),
                    }),
                },
            )
            .expect("faulted node ignores input");
        let replies_after = cluster
            .recorder
            .trace()
            .events
            .iter()
            .filter(|event| event.node == Some(NodeId::new(1)) && event.kind == EventKind::ClientOk)
            .count();
        assert_eq!(replies_before, replies_after);
    }

    #[test]
    fn trap_wiped_node_rejoins_only_via_snapshot() {
        let mut spec = RunSpec::standard(Seed::new(0x8c), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(6_000_000_000);
        spec.end_time = spec.config.end_time;
        spec.host_limits.max_log_bytes_before_snapshot = 1024;
        spec.workload = cc_sim::WorkloadSpec {
            clients: 2,
            ops_per_second: 30,
            keyspace: 8,
            set_ttl: None,
        };
        spec.plan = FaultPlan::default();
        spec.plan.push(FaultAt {
            at: Time::from_nanos(3_000_000_000),
            action: FaultAction::Wipe {
                node: NodeId::new(3),
            },
        });
        spec.plan.push(FaultAt {
            at: Time::from_nanos(3_100_000_000),
            action: FaultAction::Restart {
                node: NodeId::new(3),
            },
        });
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .advance(Duration::from_nanos(6_000_000_000))
            .expect("snapshot recovery run");
        assert!(cluster.recorder.trace().events.iter().any(|event| {
            event.node == Some(NodeId::new(3)) && event.kind == EventKind::SnapshotInstall
        }));
        assert!(cluster.recorder.trace().events.iter().all(|event| {
            !(event.node == Some(NodeId::new(3))
                && event.kind == EventKind::SnapshotInstall
                && event.payload == b"out-of-band")
        }));
        assert!(
            cluster
                .snapshot()
                .nodes
                .iter()
                .find(|node| node.id == 3)
                .is_some_and(|node| node.status == cc_sim::NodeStatus::Up)
        );
        for slot in cluster
            .nodes
            .values()
            .filter(|slot| slot.status == cc_sim::NodeStatus::Up)
        {
            let final_log_bytes = slot
                .disk
                .durable(WAL_FILE)
                .map_or(0, |bytes| bytes.len() as u64);
            assert!(
                final_log_bytes < cluster.spec.host_limits.max_log_bytes_before_snapshot,
                "published checkpoint must reclaim the physical WAL prefix"
            );
        }
    }

    #[test]
    fn trap_footprint_returns_to_baseline_after_heal() {
        let mut spec = RunSpec::standard(Seed::new(0x8d), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(2_500_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        spec.plan = FaultPlan::default();
        spec.plan.push(FaultAt {
            at: Time::from_nanos(500_000_000),
            action: FaultAction::Partition {
                left: vec![NodeId::new(1)],
                right: vec![NodeId::new(2), NodeId::new(3)],
            },
        });
        spec.plan.push(FaultAt {
            at: Time::from_nanos(1_000_000_000),
            action: FaultAction::Heal,
        });
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .advance(Duration::from_nanos(2_500_000_000))
            .expect("healed run");
        for slot in cluster
            .nodes
            .values()
            .filter_map(|slot| slot.driver.as_ref())
        {
            let footprint = slot.footprint();
            assert_eq!(footprint.driver_inputs.current, 0);
            assert_eq!(footprint.snapshot_staging.current, 0);
            for usage in [
                footprint.log,
                footprint.sessions,
                footprint.pending_reads,
                footprint.pending_client_routes,
                footprint.driver_inputs,
                footprint.driver_effects,
                footprint.outbound_frames,
            ] {
                assert!(usage.current <= usage.limit);
            }
        }
    }

    #[test]
    fn trap_corrupt_frame_never_reaches_raft_decoder() {
        let spec = RunSpec::standard(Seed::new(0x78), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::CorruptFrame {
                from: NodeId::new(1),
                to: NodeId::new(2),
                nth: 1,
                byte: 14,
                bit: 0,
            })
            .expect("corruption fault");
        cluster
            .send_message(cc_raft::Message {
                proto_version: cc_raft::PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: cc_core::Term::new(1),
                kind: cc_raft::MessageKind::VoteReq {
                    last_index: cc_core::LogIndex::new(0),
                    last_term: cc_core::Term::new(0),
                },
            })
            .expect("send");
        cluster
            .process_until(Time::from_nanos(5_000_000))
            .expect("delivery");
        let term = cluster
            .nodes
            .get(&NodeId::new(2))
            .and_then(|slot| slot.driver.as_ref())
            .expect("receiver")
            .node()
            .raft
            .hard_state
            .term;
        assert_eq!(term, cc_core::Term::new(0));
        assert!(cluster.recorder.trace().events.iter().any(|event| {
            event.kind == EventKind::NetDrop && event.node == Some(NodeId::new(2))
        }));
    }

    #[test]
    fn trap_truncated_frame_is_rejected_before_allocation() {
        let spec = RunSpec::standard(Seed::new(0x79), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::TruncateFrame {
                from: NodeId::new(1),
                to: NodeId::new(2),
                nth: 1,
                keep: 4,
            })
            .expect("truncation fault");
        cluster
            .send_message(cc_raft::Message {
                proto_version: cc_raft::PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: cc_core::Term::new(1),
                kind: cc_raft::MessageKind::VoteReq {
                    last_index: cc_core::LogIndex::new(0),
                    last_term: cc_core::Term::new(0),
                },
            })
            .expect("send");
        cluster
            .process_until(Time::from_nanos(5_000_000))
            .expect("delivery");
        let term = cluster
            .nodes
            .get(&NodeId::new(2))
            .and_then(|slot| slot.driver.as_ref())
            .expect("receiver")
            .node()
            .raft
            .hard_state
            .term;
        assert_eq!(term, cc_core::Term::new(0));
        assert!(cluster.recorder.trace().events.iter().any(|event| {
            event.kind == EventKind::NetDrop && event.node == Some(NodeId::new(2))
        }));
    }

    #[test]
    fn trap_valid_frame_decode_failure_is_an_invariant() {
        let spec = RunSpec::standard(Seed::new(0x7a1), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        let frame = encode_peer_frame(&WireMsg::new(
            cc_raft::PROTOCOL_VERSION,
            b"not-a-ccrp".to_vec(),
        ))
        .expect("valid outer frame");
        assert!(matches!(
            cluster.handle_message(NodeId::new(1), NodeId::new(2), frame),
            Err(ClusterError::Host {
                error: HostError::Node(NodeError::Environment("peer CCRP")),
                ..
            })
        ));
    }

    #[test]
    fn trap_rechecksummed_malformed_ccrp_never_reaches_state_machine() {
        let spec = RunSpec::standard(Seed::new(0x7a), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::MutateRaftAndRechecksum {
                from: NodeId::new(1),
                to: NodeId::new(2),
                nth: 1,
                mutation: CcrpMutation::MessageTag(99),
            })
            .expect("mutation fault");
        cluster
            .send_message(cc_raft::Message {
                proto_version: cc_raft::PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: cc_core::Term::new(1),
                kind: cc_raft::MessageKind::VoteReq {
                    last_index: cc_core::LogIndex::new(0),
                    last_term: cc_core::Term::new(0),
                },
            })
            .expect("send");
        cluster
            .process_until(Time::from_nanos(5_000_000))
            .expect("delivery");
        let term = cluster
            .nodes
            .get(&NodeId::new(2))
            .and_then(|slot| slot.driver.as_ref())
            .expect("receiver")
            .node()
            .raft
            .hard_state
            .term;
        assert_eq!(term, cc_core::Term::new(0));
        assert!(cluster.recorder.trace().events.iter().any(|event| {
            event.kind == EventKind::NetDrop && event.node == Some(NodeId::new(2))
        }));
    }

    #[test]
    fn trap_replayed_append_is_idempotent() {
        let spec = RunSpec::standard(Seed::new(0x7c), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::ReplayFrame {
                from: NodeId::new(1),
                to: NodeId::new(2),
                nth: 2,
                at: Time::from_nanos(3_000_000),
            })
            .expect("replay fault");
        let append = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::AppendReq(cc_raft::AppendRequest {
                prev_index: cc_core::LogIndex::new(0),
                prev_term: cc_core::Term::new(0),
                entries: vec![cc_raft::Entry {
                    term: cc_core::Term::new(1),
                    index: cc_core::LogIndex::new(1),
                    kind: cc_raft::EntryKind::Noop,
                    payload: Vec::new(),
                }],
                leader_commit: cc_core::LogIndex::new(0),
                read_round: 0,
            }),
        };
        cluster.send_message(append.clone()).expect("first send");
        cluster.send_message(append).expect("second send");
        cluster
            .process_until(Time::from_nanos(10_000_000))
            .expect("delivery");
        let receiver = cluster
            .nodes
            .get(&NodeId::new(2))
            .and_then(|slot| slot.driver.as_ref())
            .expect("receiver");
        let receiver = receiver.node();
        assert_eq!(receiver.raft.log.len(), 1);
        assert_eq!(receiver.raft.log[0].index, cc_core::LogIndex::new(1));
    }

    #[test]
    fn trap_network_byte_charge_releases_for_every_delivery_outcome() {
        let mut spec = RunSpec::standard(Seed::new(0x82), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(1);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .send_message(cc_raft::Message {
                proto_version: cc_raft::PROTOCOL_VERSION,
                from: NodeId::new(1),
                to: NodeId::new(2),
                term: cc_core::Term::new(1),
                kind: cc_raft::MessageKind::VoteReq {
                    last_index: cc_core::LogIndex::new(0),
                    last_term: cc_core::Term::new(0),
                },
            })
            .expect("the horizon cancels the modeled delivery");
        assert_eq!(
            cluster.network.inflight(NodeId::new(1), NodeId::new(2)),
            Ok((0, 0)),
            "an unscheduled beyond-horizon frame must release both reservations"
        );

        let mut cluster = SimCluster::new(
            RunSpec::standard(Seed::new(0x83), FaultProfile::Rough),
            RecorderLevel::Gate,
        )
        .expect("cluster");
        let end = cluster.spec.end_time;
        cluster.process_until(end).expect("run through horizon");
        let ids: Vec<_> = cluster.nodes.keys().copied().collect();
        for from in &ids {
            for to in &ids {
                if from != to {
                    assert_eq!(
                        cluster.network.inflight(*from, *to),
                        Ok((0, 0)),
                        "link {from}->{to} leaked an in-flight reservation"
                    );
                }
            }
        }
    }

    #[test]
    fn trap_simulator_has_no_message_side_channel() {
        let mut spec = RunSpec::standard(Seed::new(0x84), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(10_000_000);
        spec.end_time = spec.config.end_time;
        spec.workload.clients = 0;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        let message = cc_raft::Message {
            proto_version: cc_raft::PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: cc_core::Term::new(1),
            kind: cc_raft::MessageKind::VoteReq {
                last_index: cc_core::LogIndex::new(0),
                last_term: cc_core::Term::new(0),
            },
        };
        cluster.send_message(message).expect("CCRP through CCPF");
        let frame = cluster
            .replay_frames
            .get(&(NodeId::new(1), NodeId::new(2)))
            .expect("the transport retains only frame bytes");
        let (outer, used) = decode_peer_frame(frame).expect("CCPF frame");
        assert_eq!(used, frame.len());
        assert!(cc_raft::codec::decode(&outer.payload).is_ok());
        cluster
            .process_until(Time::from_nanos(10_000_000))
            .expect("frame delivery");
    }

    #[test]
    fn trap_trace_payloads_do_not_use_debug_format() {
        let run = run_spec(
            RunSpec::standard(Seed::new(0x85), FaultProfile::Calm),
            RecorderLevel::Gate,
        )
        .expect("cluster");
        let frames: Vec<_> = run
            .trace
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    EventKind::NetSend | EventKind::NetRecv | EventKind::NetDrop
                )
            })
            .collect();
        assert!(!frames.is_empty());
        for event in frames {
            assert_eq!(event.payload.len(), 9);
            assert_eq!(&event.payload[..2], &1_u16.to_le_bytes());
        }
    }

    #[test]
    fn trap_heal_clears_sustained_link_fault_without_leaking_bytes() {
        let spec = RunSpec::standard(Seed::new(0x7b), FaultProfile::Calm);
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .handle_fault(FaultAction::DelayLink {
                from: NodeId::new(1),
                to: NodeId::new(2),
                extra: Duration::from_millis(20),
            })
            .expect("delay fault");
        cluster
            .replay_frames
            .insert((NodeId::new(1), NodeId::new(2)), vec![1, 2]);
        cluster.handle_fault(FaultAction::Heal).expect("heal fault");
        assert!(cluster.frame_faults.is_empty());
        assert!(cluster.replay_faults.is_empty());
        assert!(cluster.replay_frames.is_empty());
        let delayed = cluster
            .network
            .send(cluster.now, NodeId::new(1), NodeId::new(2), vec![1])
            .expect("network send");
        let [NetworkDecision::Delivered(delivery)] = delayed.as_slice() else {
            panic!("healed link must deliver one datagram");
        };
        assert_eq!(delivery.at, cluster.now + Duration::from_millis(1));
    }

    #[test]
    fn trap_explain_emits_parseable_svg() {
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
    fn trap_explain_xml_escapes_labels() {
        let mut trace = Trace::new(Seed::new(1), 0);
        trace.push(
            Time::from_nanos(1),
            Some(NodeId::new(1)),
            EventKind::Fault,
            br#"<fault source="x">&'"#.to_vec(),
        );
        let svg = sequence_diagram_svg(&trace);
        assert!(svg.contains("&lt;fault source=&quot;x&quot;&gt;&amp;&apos;"));
        assert!(!svg.contains("<fault"));
    }

    #[test]
    fn trap_explain_obeys_output_cap() {
        let mut trace = Trace::new(Seed::new(1), 0);
        for index in 0..10_000_u64 {
            trace.push(
                Time::from_nanos(index),
                Some(NodeId::new(index % 80 + 1)),
                EventKind::Fault,
                vec![b'x'; 4_096],
            );
        }
        let svg = sequence_diagram_svg(&trace);
        assert!(svg.len() <= 2 * 1024 * 1024);
        assert!(svg.contains("omitted"));
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

    /// A wiped node loses every durable byte, then re-enters only through a
    /// Join-origin durable prefix and ordinary Raft traffic.  The simulator is
    /// expressly forbidden from copying a leader's state into that node.
    #[test]
    fn trap_wipe_has_no_out_of_band_state_copy() {
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
            !installed,
            "a wiped node must not receive an out-of-band state copy"
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

    fn isolate_crash_and_restart(
        cluster: &mut SimCluster,
    ) -> (ClusterNodeSnapshot, ClusterNodeSnapshot) {
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
        assert_eq!(
            recovered.applied, victim.applied,
            "store-WAL proof restores the exact durable applied index"
        );
        assert_eq!(
            recovered.commit, victim.applied,
            "recovery serves only the store-proven commit prefix"
        );
        assert_eq!(
            recovered.role,
            Role::Follower,
            "restarts come back as followers"
        );
        (victim.clone(), recovered.clone())
    }

    /// A crash drops volatile role/routes/timers while N3 restores only the
    /// state-machine prefix proven by the durable store WAL.
    #[test]
    fn trap_crash_discards_volatile_state() {
        let mut spec = RunSpec::standard(Seed::new(0x61), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(10_000_000_000);
        spec.end_time = spec.config.end_time;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .advance(Duration::from_secs(3))
            .expect("warmup advances");
        let (before, after) = isolate_crash_and_restart(&mut cluster);
        assert!(before.applied > 0, "test requires live volatile state");
        assert_eq!(after.applied, before.applied);
        assert_eq!(after.commit, before.applied);
        assert_eq!(after.role, Role::Follower);
    }

    #[test]
    fn trap_restart_recovers_hard_state_log_and_membership_from_disk() {
        let mut spec = RunSpec::standard(Seed::new(0x62), FaultProfile::Calm);
        spec.config.end_time = Time::from_nanos(10_000_000_000);
        spec.end_time = spec.config.end_time;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        cluster
            .advance(Duration::from_secs(3))
            .expect("warmup advances");
        let (before, after) = isolate_crash_and_restart(&mut cluster);
        assert!(after.durable_bytes > 0, "durable prefix survived restart");
        assert!(
            !after.log_tail.is_empty(),
            "recovered Driver exposes log prefix"
        );
        assert!(
            after.term >= before.term,
            "recovered hard state cannot regress to a fresh term"
        );
        assert_eq!(
            after.voters, before.voters,
            "membership must come from the durable prefix, not discovery"
        );
    }

    /// Simulator durability uses the same CCLR record stream as the shared
    /// Driver. A long append and torn final record are recovered by `cc-log`,
    /// not a host-owned slot decoder.
    #[test]
    fn trap_simulator_recovers_framed_cc_log_prefix() {
        let entries: Vec<cc_raft::Entry> = (1..=4)
            .map(|index| cc_raft::Entry {
                term: cc_core::Term::new(7),
                index: cc_core::LogIndex::new(index),
                kind: cc_raft::EntryKind::App,
                payload: vec![index as u8; 200],
            })
            .collect();
        let membership = cc_core::MembershipState::new([NodeId::new(1)].into_iter().collect())
            .expect("membership");
        let mut durable = cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(
            Box::new(cc_log::Genesis {
                origin: cc_log::Origin::Bootstrap,
                cluster_id: [7; 16],
                policy: ClusterPolicy::default(),
                membership,
            }),
        ))
        .expect("genesis");
        durable.extend(
            cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Hard(
                cc_raft::HardState {
                    term: cc_core::Term::new(9),
                    voted_for: Some(NodeId::new(3)),
                },
            ))
            .expect("hard state"),
        );
        for entry in &entries {
            durable.extend(
                cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Append(entry.clone()))
                    .expect("append"),
            );
        }

        let recovered = cc_log::recover_framed_record_stream(&durable).expect("recover");
        assert_eq!(recovered.state.hard_state.term.get(), 9);
        assert_eq!(recovered.state.hard_state.voted_for, Some(NodeId::new(3)));
        assert_eq!(recovered.state.entries, entries);
        assert_eq!(recovered.bytes_consumed, durable.len() as u64);

        // A torn tail is not part of the recovered log.
        durable.truncate(durable.len() - 40);
        let torn = cc_log::recover_framed_record_stream(&durable).expect("torn recovery");
        assert!(torn.torn_tail_truncated);
        assert_eq!(
            torn.state.entries.len(),
            3,
            "the partial final record is dropped"
        );
    }

    #[test]
    fn trap_architecture_has_one_host_boundary_and_one_raft_log() {
        let spec = RunSpec::standard(Seed::new(0x63), FaultProfile::Calm);
        let cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        for slot in cluster.nodes.values() {
            assert!(slot.driver.is_some(), "every simulator node owns a Driver");
            let durable = slot.disk.durable(WAL_FILE).expect("genesis durable prefix");
            let recovered = cc_log::recover_framed_record_stream(durable).expect("cc-log recovery");
            assert_eq!(recovered.bytes_consumed as usize, durable.len());
            assert!(!recovered.torn_tail_truncated);
            assert_eq!(recovered.state.genesis, slot.genesis);
        }
    }

    #[test]
    fn trap_single_scheduler_enforces_per_instant_limit() {
        let mut spec = RunSpec::standard(Seed::new(0x1a), FaultProfile::Calm);
        spec.config.max_events = 100;
        spec.config.max_events_per_instant = 1;
        spec.config.end_time = Time::from_nanos(1);
        spec.end_time = spec.config.end_time;
        let mut cluster = SimCluster::new(spec, RecorderLevel::Gate).expect("cluster");
        assert!(matches!(
            cluster.advance(Duration::from_nanos(0)),
            Err(ClusterError::Run(RunError::InstantLimit {
                at: Time { .. },
                limit: 1,
            }))
        ));
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
