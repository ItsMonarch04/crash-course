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
    Scan {
        prefix: Option<Vec<u8>>,
        limit: usize,
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
            Self::Scan { prefix, .. } => prefix.as_deref().unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ok,
    Value(Option<Vec<u8>>),
    Integer(i64),
    Cas(bool),
    Scan(Vec<(Vec<u8>, Vec<u8>)>),
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
    if history
        .operations
        .iter()
        .any(|operation| matches!(operation.kind, OperationKind::Scan { .. }))
    {
        return check_single(history, config);
    }
    let mut per_key = BTreeMap::<Vec<u8>, History>::new();
    for operation in &history.operations {
        per_key
            .entry(operation.kind.key().to_vec())
            .or_default()
            .push(operation.clone());
    }
    if per_key.len() > 1 {
        let mut visited = 0_u64;
        let mut undecided = false;
        for key_history in per_key.values() {
            match check_single(key_history, config) {
                Verdict::Linearizable { visited: count } => visited = visited.saturating_add(count),
                Verdict::Undecided { visited: count } => {
                    visited = visited.saturating_add(count);
                    undecided = true;
                }
                Verdict::NotLinearizable {
                    operation_ids,
                    visited: count,
                } => {
                    return Verdict::NotLinearizable {
                        operation_ids,
                        visited: visited.saturating_add(count),
                    };
                }
            }
        }
        return if undecided {
            Verdict::Undecided { visited }
        } else {
            Verdict::Linearizable { visited }
        };
    }
    check_single(history, config)
}

fn check_single(history: &History, config: CheckerConfig) -> Verdict {
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
            // Real-time precedence must be strict. With `<=`, two operations
            // that share an instant each require the other to be linearized
            // first, which no order can satisfy — the search then reports a
            // perfectly legal history as non-linearizable. Operations that
            // merely touch at the same timestamp are concurrent.
            match other.complete {
                Some(completion) if completion < candidate.invoke => {
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
        OperationKind::Scan { prefix, limit } => {
            let mut values = next
                .iter()
                .filter(|(key, _)| prefix.as_ref().is_none_or(|prefix| key.starts_with(prefix)))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            values.truncate(*limit);
            Outcome::Scan(values)
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
            OperationKind::Get { key }
                if deleted.contains(key)
                    && operation.outcome == Outcome::Value(Some(Vec::new())) =>
            {
                report.violations.push(InvariantViolation {
                    name: "no_resurrection",
                    detail: format!("key {key:?} returned after acknowledged delete"),
                });
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

#[cfg(test)]
mod same_instant_tests {
    use super::*;

    /// Two zero-duration reads at the same instant are concurrent, not mutually
    /// preceding. Under the old `<=` precedence rule this history was reported
    /// as non-linearizable.
    #[test]
    fn operations_sharing_an_instant_are_concurrent() {
        let key = b"k".to_vec();
        let mut history = History::default();

        let mut write = Operation::open(
            1,
            OperationKind::Set {
                key: key.clone(),
                value: vec![7],
            },
            Time::from_nanos(10),
        );
        write.complete = Some(Time::from_nanos(20));
        write.outcome = Outcome::Ok;
        history.push(write);

        for id in 2..=3 {
            let mut read = Operation::open(
                id,
                OperationKind::Get { key: key.clone() },
                Time::from_nanos(30),
            );
            read.complete = Some(Time::from_nanos(30));
            read.outcome = Outcome::Value(Some(vec![7]));
            history.push(read);
        }

        assert!(matches!(
            check(
                &history,
                CheckerConfig {
                    max_states: 100_000
                }
            ),
            Verdict::Linearizable { .. }
        ));
    }

    /// The strict rule must still enforce genuine precedence: a read that
    /// completes before a write starts cannot observe that write.
    #[test]
    fn genuine_precedence_is_still_enforced() {
        let key = b"k".to_vec();
        let mut history = History::default();

        let mut read = Operation::open(
            1,
            OperationKind::Get { key: key.clone() },
            Time::from_nanos(10),
        );
        read.complete = Some(Time::from_nanos(20));
        read.outcome = Outcome::Value(Some(vec![7]));
        history.push(read);

        let mut write = Operation::open(
            2,
            OperationKind::Set {
                key,
                value: vec![7],
            },
            Time::from_nanos(30),
        );
        write.complete = Some(Time::from_nanos(40));
        write.outcome = Outcome::Ok;
        history.push(write);

        assert!(matches!(
            check(
                &history,
                CheckerConfig {
                    max_states: 100_000
                }
            ),
            Verdict::NotLinearizable { .. }
        ));
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
    let mut event_index = 0_usize;
    for operation in &history.operations {
        if event_index != 0 {
            output.push(',');
        }
        output.push_str(&format!(
            "{{\"process\":{},\"type\":\"invoke\",\"value\":{},\"time\":{}}}",
            operation.client,
            operation_value(&operation.kind),
            operation.invoke.as_nanos()
        ));
        event_index += 1;
        if let Some(complete) = operation.complete {
            output.push(',');
            output.push_str(&format!(
                "{{\"process\":{},\"type\":\"{}\",\"value\":{},\"time\":{}}}",
                operation.client,
                if operation.outcome == Outcome::Timeout {
                    "fail"
                } else {
                    "ok"
                },
                outcome_value(&operation.outcome),
                complete.as_nanos()
            ));
            event_index += 1;
        }
    }
    output.push(']');
    output
}

fn operation_value(operation: &OperationKind) -> String {
    match operation {
        OperationKind::Set { key, value } => {
            format!("[\"set\",{},{}]", json_bytes(key), json_bytes(value))
        }
        OperationKind::Get { key } => format!("[\"get\",{}]", json_bytes(key)),
        OperationKind::Del { key } => format!("[\"del\",{}]", json_bytes(key)),
        OperationKind::Incr { key } => format!("[\"incr\",{}]", json_bytes(key)),
        OperationKind::Cas {
            key,
            expected,
            value,
        } => format!(
            "[\"cas\",{},{},{}]",
            json_bytes(key),
            expected
                .as_deref()
                .map_or_else(|| String::from("null"), json_bytes),
            json_bytes(value)
        ),
        OperationKind::Scan { prefix, limit } => format!(
            "[\"scan\",{},{}]",
            prefix
                .as_deref()
                .map_or_else(|| String::from("null"), json_bytes),
            limit
        ),
    }
}

fn outcome_value(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Ok => String::from("\"ok\""),
        Outcome::Value(value) => value
            .as_deref()
            .map_or_else(|| String::from("null"), json_bytes),
        Outcome::Integer(value) => value.to_string(),
        Outcome::Cas(value) => value.to_string(),
        Outcome::Scan(values) => format!(
            "[{}]",
            values
                .iter()
                .map(|(key, value)| format!("[{},{}]", json_bytes(key), json_bytes(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Outcome::Error => String::from("\"error\""),
        Outcome::Timeout => String::from("\"timeout\""),
    }
}

fn json_bytes(value: &[u8]) -> String {
    let text = String::from_utf8_lossy(value);
    format!("\"{}\"", json_escape(&text))
}

fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '"' => ['\\', '"'].into_iter().collect::<Vec<_>>(),
            '\\' => ['\\', '\\'].into_iter().collect::<Vec<_>>(),
            '\n' => ['\\', 'n'].into_iter().collect::<Vec<_>>(),
            '\r' => ['\\', 'r'].into_iter().collect::<Vec<_>>(),
            '\t' => ['\\', 't'].into_iter().collect::<Vec<_>>(),
            other => [other].into_iter().collect::<Vec<_>>(),
        })
        .collect()
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
    fn trap_open_op_can_be_dropped_in_the_other_direction() {
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
            Outcome::Value(None),
        ));
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn scan_is_checked_as_a_snapshot_legal_operation() {
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
            OperationKind::Set {
                key: b"b".to_vec(),
                value: b"two".to_vec(),
            },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Ok,
        ));
        history.push(Operation::completed(
            3,
            OperationKind::Scan {
                prefix: None,
                limit: 8,
            },
            Time::from_nanos(3),
            Time::from_nanos(4),
            Outcome::Scan(vec![
                (b"a".to_vec(), b"one".to_vec()),
                (b"b".to_vec(), b"two".to_vec()),
            ]),
        ));
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn porcupine_export_contains_invoke_and_completion_events() {
        let mut history = History::default();
        history.push(Operation::completed(
            7,
            OperationKind::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
            },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Ok,
        ));
        let exported = export_porcupine_json(&history);
        assert!(exported.contains("\"type\":\"invoke\""));
        assert!(exported.contains("\"type\":\"ok\""));
        assert!(exported.contains("[\"set\",\"a\",\"one\"]"));
    }

    #[test]
    fn independent_keys_are_partitioned_before_search() {
        let mut history = History::default();
        for (id, key, value) in [(1, b"a".as_slice(), b"one".as_slice()), (2, b"b", b"two")] {
            history.push(Operation::completed(
                id,
                OperationKind::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                Time::from_nanos(0),
                Time::from_nanos(1),
                Outcome::Ok,
            ));
        }
        assert!(matches!(
            check(&history, CheckerConfig { max_states: 1 }),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn budget_exhaustion_is_explicitly_undecided() {
        let mut history = History::default();
        for id in 1..=8 {
            history.push(Operation::completed(
                id,
                OperationKind::Set {
                    key: b"hot".to_vec(),
                    value: id.to_string().into_bytes(),
                },
                Time::from_nanos(0),
                Time::from_nanos(1),
                Outcome::Ok,
            ));
        }
        assert!(matches!(
            check(&history, CheckerConfig { max_states: 1 }),
            Verdict::Undecided { .. }
        ));
    }

    #[test]
    fn wing_gong_search_matches_a_brute_force_eight_operation_oracle() {
        let mut legal = History::default();
        legal.push(Operation::completed(
            1,
            OperationKind::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Ok,
        ));
        legal.push(Operation::completed(
            2,
            OperationKind::Get { key: b"a".to_vec() },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Value(Some(b"1".to_vec())),
        ));
        legal.push(Operation::completed(
            3,
            OperationKind::Incr { key: b"a".to_vec() },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Integer(2),
        ));
        legal.push(Operation::completed(
            4,
            OperationKind::Get { key: b"a".to_vec() },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Value(Some(b"2".to_vec())),
        ));
        legal.push(Operation::completed(
            5,
            OperationKind::Set {
                key: b"b".to_vec(),
                value: b"x".to_vec(),
            },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Ok,
        ));
        legal.push(Operation::completed(
            6,
            OperationKind::Get { key: b"b".to_vec() },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Value(Some(b"x".to_vec())),
        ));
        legal.push(Operation::completed(
            7,
            OperationKind::Del { key: b"b".to_vec() },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Ok,
        ));
        legal.push(Operation::completed(
            8,
            OperationKind::Get { key: b"b".to_vec() },
            Time::from_nanos(0),
            Time::from_nanos(8),
            Outcome::Value(None),
        ));
        let checker_is_legal = matches!(
            check(&legal, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        );
        assert_eq!(checker_is_legal, brute_force(&legal));

        let mut illegal = legal.clone();
        illegal.operations[1].outcome = Outcome::Value(None);
        let checker_is_legal = matches!(
            check(&illegal, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        );
        assert_eq!(checker_is_legal, brute_force(&illegal));
    }

    fn brute_force(history: &History) -> bool {
        fn search(history: &History, remaining: Vec<usize>, model: Model) -> bool {
            if remaining.is_empty() {
                return true;
            }
            for (position, index) in remaining.iter().enumerate() {
                if !eligible(*index, &remaining, history) {
                    continue;
                }
                let operation = &history.operations[*index];
                let mut next_remaining = remaining.clone();
                next_remaining.remove(position);
                if operation.outcome == Outcome::Timeout {
                    if search(history, next_remaining.clone(), model.clone()) {
                        return true;
                    }
                    if let Some((next, _)) = apply_operation(&model, &operation.kind)
                        && search(history, next_remaining, next)
                    {
                        return true;
                    }
                } else if let Some((next, expected)) = apply_operation(&model, &operation.kind)
                    && expected == operation.outcome
                    && search(history, next_remaining, next)
                {
                    return true;
                }
            }
            false
        }
        search(
            history,
            (0..history.operations.len()).collect(),
            BTreeMap::new(),
        )
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
