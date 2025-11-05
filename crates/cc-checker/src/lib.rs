// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "History, linearizability, and lightweight trace invariant checking."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{Time, Trace};

pub const CHECKER_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        key: Vec<u8>,
    },
    Del {
        key: Vec<u8>,
    },
    Incr {
        key: Vec<u8>,
    },
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    },
}

impl OperationKind {
    fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. }
            | Self::Get { key }
            | Self::Del { key }
            | Self::Incr { key }
            | Self::Cas { key, .. } => key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ok,
    Value(Option<Vec<u8>>),
    Integer(i64),
    Cas(bool),
    Error,
    Timeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    pub id: u64,
    pub client: u64,
    pub sequence: u64,
    pub invoke: Time,
    pub complete: Option<Time>,
    pub kind: OperationKind,
    pub outcome: Outcome,
}

impl Operation {
    #[must_use]
    pub fn open(id: u64, kind: OperationKind, invoke: Time) -> Self {
        Self {
            id,
            client: 0,
            sequence: id,
            invoke,
            complete: None,
            kind,
            outcome: Outcome::Timeout,
        }
    }

    #[must_use]
    pub fn completed(
        id: u64,
        kind: OperationKind,
        invoke: Time,
        complete: Time,
        outcome: Outcome,
    ) -> Self {
        Self {
            id,
            client: 0,
            sequence: id,
            invoke,
            complete: Some(complete),
            kind,
            outcome,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct History {
    pub operations: Vec<Operation>,
}

impl History {
    pub fn push(&mut self, operation: Operation) {
        self.operations.push(operation);
    }

    #[must_use]
    pub fn by_key(&self, key: &[u8]) -> Vec<&Operation> {
        self.operations
            .iter()
            .filter(|operation| operation.kind.key() == key)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckerConfig {
    pub max_states: u64,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            max_states: 10_000_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict {
    Linearizable {
        visited: u64,
    },
    NotLinearizable {
        operation_ids: Vec<u64>,
        visited: u64,
    },
    Undecided {
        visited: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MemoKey {
    remaining: Vec<usize>,
    state: Vec<(Vec<u8>, Vec<u8>)>,
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

enum SearchResult {
    Found,
    NotFound,
    Undecided,
}

/// Check a per-key register history with an open-operation branch.
#[must_use]
pub fn check(history: &History, config: CheckerConfig) -> Verdict {
    if history.operations.is_empty() {
        return Verdict::Linearizable { visited: 0 };
    }
    let remaining: Vec<usize> = (0..history.operations.len()).collect();
    let mut visited = 0;
    let mut memo = BTreeSet::new();
    let mut path = Vec::new();
    let result = search(
        history,
        remaining,
        BTreeMap::new(),
        config,
        &mut visited,
        &mut memo,
        &mut path,
    );
    match result {
        SearchResult::Found => Verdict::Linearizable { visited },
        SearchResult::Undecided => Verdict::Undecided { visited },
        SearchResult::NotFound => Verdict::NotLinearizable {
            operation_ids: history
                .operations
                .iter()
                .map(|operation| operation.id)
                .collect(),
            visited,
        },
    }
}

fn search(
    history: &History,
    remaining: Vec<usize>,
    model: Model,
    config: CheckerConfig,
    visited: &mut u64,
    memo: &mut BTreeSet<MemoKey>,
    path: &mut Vec<u64>,
) -> SearchResult {
    if remaining.is_empty() {
        return SearchResult::Found;
    }
    *visited += 1;
    if *visited > config.max_states {
        return SearchResult::Undecided;
    }
    let memo_key = MemoKey {
        remaining: remaining.clone(),
        state: model
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    };
    if !memo.insert(memo_key) {
        return SearchResult::NotFound;
    }
    let mut saw_undecided = false;
    for (position, index) in remaining.iter().enumerate() {
        let operation = &history.operations[*index];
        if !eligible(*index, &remaining, history) {
            continue;
        }
        let mut next_remaining = remaining.clone();
        next_remaining.remove(position);
        path.push(operation.id);
        if operation.outcome == Outcome::Timeout {
            match search(
                history,
                next_remaining.clone(),
                model.clone(),
                config,
                visited,
                memo,
                path,
            ) {
                SearchResult::Found => return SearchResult::Found,
                SearchResult::Undecided => saw_undecided = true,
                SearchResult::NotFound => {}
            }
            if let Some(next_model) = apply_without_observation(&model, &operation.kind) {
                match search(
                    history,
                    next_remaining,
                    next_model,
                    config,
                    visited,
                    memo,
                    path,
                ) {
                    SearchResult::Found => return SearchResult::Found,
                    SearchResult::Undecided => saw_undecided = true,
                    SearchResult::NotFound => {}
                }
            }
        } else if let Some(next_model) = apply_observed(&model, &operation.kind, &operation.outcome)
        {
            match search(
                history,
                next_remaining,
                next_model,
                config,
                visited,
                memo,
                path,
            ) {
                SearchResult::Found => return SearchResult::Found,
                SearchResult::Undecided => saw_undecided = true,
                SearchResult::NotFound => {}
            }
        }
        path.pop();
    }
    if saw_undecided {
        SearchResult::Undecided
    } else {
        SearchResult::NotFound
    }
}

fn eligible(index: usize, remaining: &[usize], history: &History) -> bool {
    let candidate = &history.operations[index];
    history
        .operations
        .iter()
        .enumerate()
        .all(|(other_index, other)| {
            if other_index == index {
                return true;
            }
            match other.complete {
                Some(completion) if completion <= candidate.invoke => {
                    !remaining.contains(&other_index)
                }
                _ => true,
            }
        })
}

fn apply_without_observation(model: &Model, operation: &OperationKind) -> Option<Model> {
    apply_operation(model, operation).map(|(next, _)| next)
}

fn apply_observed(model: &Model, operation: &OperationKind, observed: &Outcome) -> Option<Model> {
    if matches!(observed, Outcome::Error) {
        return Some(model.clone());
    }
    let (next, expected) = apply_operation(model, operation)?;
    if &expected == observed {
        Some(next)
    } else {
        None
    }
}

fn apply_operation(model: &Model, operation: &OperationKind) -> Option<(Model, Outcome)> {
    let mut next = model.clone();
    let outcome = match operation {
        OperationKind::Set { key, value } => {
            next.insert(key.clone(), value.clone());
            Outcome::Ok
        }
        OperationKind::Get { key } => Outcome::Value(next.get(key).cloned()),
        OperationKind::Del { key } => {
            next.remove(key);
            Outcome::Ok
        }
        OperationKind::Incr { key } => {
            let old = next
                .get(key)
                .and_then(|value| std::str::from_utf8(value).ok())
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            let new = old.checked_add(1)?;
            next.insert(key.clone(), new.to_string().into_bytes());
            Outcome::Integer(new)
        }
        OperationKind::Cas {
            key,
            expected,
            value,
        } => {
            if next.get(key) == expected.as_ref() {
                next.insert(key.clone(), value.clone());
                Outcome::Cas(true)
            } else {
                Outcome::Cas(false)
            }
        }
    };
    Some((next, outcome))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantViolation {
    pub name: &'static str,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InvariantReport {
    pub violations: Vec<InvariantViolation>,
}

impl InvariantReport {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

#[must_use]
pub fn check_trace_invariants(trace: &Trace) -> InvariantReport {
    let mut report = InvariantReport::default();
    for pair in trace.events.windows(2) {
        if pair[1].seq != pair[0].seq + 1 {
            report.violations.push(InvariantViolation {
                name: "trace_sequence",
                detail: format!("{} followed by {}", pair[0].seq, pair[1].seq),
            });
        }
        if pair[1].time < pair[0].time {
            report.violations.push(InvariantViolation {
                name: "trace_time_order",
                detail: format!(
                    "{}ns followed by {}ns",
                    pair[0].time.as_nanos(),
                    pair[1].time.as_nanos()
                ),
            });
        }
    }
    report
}

#[must_use]
pub fn check_no_resurrection(history: &History) -> InvariantReport {
    let mut report = InvariantReport::default();
    let mut deleted: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut operations = history.operations.clone();
    operations.sort_by_key(|operation| operation.complete.unwrap_or(operation.invoke));
    for operation in operations {
        match &operation.kind {
            OperationKind::Del { key } if operation.outcome == Outcome::Ok => {
                deleted.insert(key.clone());
            }
            OperationKind::Set { key, .. } if operation.outcome == Outcome::Ok => {
                deleted.remove(key);
            }
            OperationKind::Get { key } => {
                if deleted.contains(key) && operation.outcome == Outcome::Value(Some(Vec::new())) {
                    report.violations.push(InvariantViolation {
                        name: "no_resurrection",
                        detail: format!("key {:?} returned after acknowledged delete", key),
                    });
                }
            }
            _ => {}
        }
    }
    report
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LivenessReport {
    pub leader_seen: bool,
    pub probe_committed: bool,
    pub survivable: bool,
}

impl LivenessReport {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        !self.survivable || (self.leader_seen && self.probe_committed)
    }
}

#[must_use]
pub fn check_liveness(report: LivenessReport) -> InvariantReport {
    if report.is_ok() {
        InvariantReport::default()
    } else {
        InvariantReport {
            violations: vec![InvariantViolation {
                name: "bounded_liveness",
                detail: String::from("survivable plan did not produce a leader and probe commit"),
            }],
        }
    }
}

#[must_use]
pub fn export_porcupine_json(history: &History) -> String {
    let mut output = String::from("[");
    for (index, operation) in history.operations.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(&format!(
            "{{\"process\":{},\"id\":{},\"time\":{},\"type\":\"{}\"}}",
            operation.client,
            operation.id,
            operation.invoke.as_nanos(),
            operation_name(&operation.kind)
        ));
    }
    output.push(']');
    output
}

fn operation_name(operation: &OperationKind) -> &'static str {
    match operation {
        OperationKind::Set { .. } => "set",
        OperationKind::Get { .. } => "get",
        OperationKind::Del { .. } => "del",
        OperationKind::Incr { .. } => "incr",
        OperationKind::Cas { .. } => "cas",
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Linearizable { visited } => write!(f, "linearizable (visited {visited})"),
            Self::NotLinearizable { visited, .. } => {
                write!(f, "not linearizable (visited {visited})")
            }
            Self::Undecided { visited } => write!(f, "undecided (visited {visited})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_core::NodeId;

    #[test]
    fn legal_set_get_history_is_linearizable() {
        let mut history = History::default();
        history.push(Operation::completed(
            1,
            OperationKind::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
            },
            Time::from_nanos(0),
            Time::from_nanos(1),
            Outcome::Ok,
        ));
        history.push(Operation::completed(
            2,
            OperationKind::Get { key: b"a".to_vec() },
            Time::from_nanos(2),
            Time::from_nanos(3),
            Outcome::Value(Some(b"one".to_vec())),
        ));
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn stale_read_is_rejected() {
        let mut history = History::default();
        history.push(Operation::completed(
            1,
            OperationKind::Set {
                key: b"a".to_vec(),
                value: b"new".to_vec(),
            },
            Time::from_nanos(0),
            Time::from_nanos(2),
            Outcome::Ok,
        ));
        history.push(Operation::completed(
            2,
            OperationKind::Get { key: b"a".to_vec() },
            Time::from_nanos(3),
            Time::from_nanos(4),
            Outcome::Value(None),
        ));
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
    }

    #[test]
    fn trap_open_op_semantics_allows_timeout_to_take_effect() {
        let mut history = History::default();
        history.push(Operation::open(
            1,
            OperationKind::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
            },
            Time::from_nanos(0),
        ));
        history.push(Operation::completed(
            2,
            OperationKind::Get { key: b"a".to_vec() },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Value(Some(b"one".to_vec())),
        ));
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn increment_lost_update_is_rejected() {
        let mut history = History::default();
        for id in 1..=2 {
            history.push(Operation::completed(
                id,
                OperationKind::Incr {
                    key: b"counter".to_vec(),
                },
                Time::from_nanos(0),
                Time::from_nanos(2),
                Outcome::Integer(1),
            ));
        }
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
    }

    #[test]
    fn trace_and_liveness_reports_are_actionable() {
        let mut trace = Trace::new(cc_core::Seed::new(1), 0);
        trace.push(
            Time::from_nanos(1),
            Some(NodeId::new(1)),
            cc_core::EventKind::Apply,
            vec![],
        );
        trace.push(
            Time::from_nanos(0),
            Some(NodeId::new(1)),
            cc_core::EventKind::Commit,
            vec![],
        );
        assert!(!check_trace_invariants(&trace).is_ok());
        assert!(
            !check_liveness(LivenessReport {
                leader_seen: false,
                probe_committed: false,
                survivable: true,
            })
            .is_ok()
        );
    }
}
