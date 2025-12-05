// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "History, linearizability, and lightweight trace invariant checking."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{Dec, DecodeError, Enc, Time, Trace};

pub const CHECKER_VERSION: u16 = 1;
pub const HISTORY_MAGIC: u32 = u32::from_le_bytes(*b"CCHY");
pub const HISTORY_VERSION: u16 = 4;
pub const HISTORY_V3_VERSION: u16 = 3;
pub const HISTORY_V2_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsistencyMode {
    Strong,
    StaleLocal,
    Diagnostic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiagnosticKind {
    None,
    FinalProbe,
    ReplicaState {
        replica: u64,
        committed_index: u64,
        state_hash: u64,
    },
}

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
    Batch {
        commands: Vec<OperationKind>,
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
            Self::Batch { .. } => &[],
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
    Batch { replies: Vec<Outcome> },
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
    /// Replicated expiry attached to the decisive mutation, when present.
    /// Diagnostic persistence labels require their observation interval to
    /// lie wholly before this time.
    pub deadline: Option<Time>,
    pub consistency: ConsistencyMode,
    pub diagnostic: DiagnosticKind,
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
            deadline: None,
            consistency: ConsistencyMode::Strong,
            diagnostic: DiagnosticKind::None,
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
            deadline: None,
            consistency: ConsistencyMode::Strong,
            diagnostic: DiagnosticKind::None,
        }
    }

    #[must_use]
    pub fn final_probe(mut self) -> Self {
        self.client = 0;
        self.sequence = 0;
        self.consistency = ConsistencyMode::Diagnostic;
        self.diagnostic = DiagnosticKind::FinalProbe;
        self
    }

    #[must_use]
    pub fn replica_probe(mut self, replica: u64, committed_index: u64, state_hash: u64) -> Self {
        self.client = 0;
        self.sequence = 0;
        self.consistency = ConsistencyMode::Diagnostic;
        self.diagnostic = DiagnosticKind::ReplicaState {
            replica,
            committed_index,
            state_hash,
        };
        self
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

/// The binary-safe CC-HISTORY v2 container used by real-run export, checking,
/// and external adapters. It retains open operations instead of turning a
/// timeout into an accidental absence from the proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryDocument {
    pub build_label: String,
    pub config_hash: u64,
    pub initial: BTreeMap<Vec<u8>, Vec<u8>>,
    pub retain_open: bool,
    pub history: History,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryCodecError {
    Decode(DecodeError),
    Invalid(&'static str),
    DuplicateId(u64),
}
impl std::fmt::Display for HistoryCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(error) => write!(f, "decode: {error}"),
            Self::Invalid(reason) => write!(f, "invalid history: {reason}"),
            Self::DuplicateId(id) => write!(f, "duplicate operation id {id}"),
        }
    }
}
impl std::error::Error for HistoryCodecError {}
impl From<DecodeError> for HistoryCodecError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

impl HistoryDocument {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.header(HISTORY_MAGIC, HISTORY_VERSION);
        enc.string(&self.build_label);
        enc.u64(self.config_hash);
        enc.u8(u8::from(self.retain_open));
        enc.u32(u32::try_from(self.initial.len()).expect("initial count fits"));
        for (key, value) in &self.initial {
            enc.bytes(key);
            enc.bytes(value);
        }
        enc.u32(u32::try_from(self.history.operations.len()).expect("operation count fits"));
        for operation in &self.history.operations {
            encode_operation(&mut enc, operation);
        }
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, HistoryCodecError> {
        let version = bytes
            .get(4..6)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            .ok_or(HistoryCodecError::Invalid("history header"))?;
        if !matches!(
            version,
            HISTORY_V2_VERSION | HISTORY_V3_VERSION | HISTORY_VERSION
        ) {
            return Err(HistoryCodecError::Invalid("history version"));
        }
        let mut dec = Dec::new(bytes);
        dec.header(HISTORY_MAGIC, version)?;
        let build_label = dec.string()?;
        let config_hash = dec.u64()?;
        let retain_open = match dec.u8()? {
            0 => false,
            1 => true,
            _ => return Err(HistoryCodecError::Invalid("open flag")),
        };
        let initial_count = bounded_count(&mut dec, bytes.len(), 1_000_000)?;
        let mut initial = BTreeMap::new();
        for _ in 0..initial_count {
            let key = dec.bytes()?;
            let value = dec.bytes()?;
            if initial.insert(key, value).is_some() {
                return Err(HistoryCodecError::Invalid("duplicate initial key"));
            }
        }
        let count = bounded_count(&mut dec, bytes.len(), 1_000_000)?;
        let mut history = History::default();
        let mut ids = BTreeSet::new();
        for _ in 0..count {
            let operation = decode_operation(&mut dec, version)?;
            if !ids.insert(operation.id) {
                return Err(HistoryCodecError::DuplicateId(operation.id));
            }
            if operation.complete.is_none() && !retain_open {
                return Err(HistoryCodecError::Invalid("open operation not retained"));
            }
            history.push(operation);
        }
        dec.finish()?;
        Ok(Self {
            build_label,
            config_hash,
            initial,
            retain_open,
            history,
        })
    }
}

/// Decode the captured tab-separated CC-HISTORY v1 receipt format.  The
/// legacy writer rendered binary arguments as hexadecimal; decoding those
/// fields as their ASCII text would silently prove a different history.  New
/// receipts use [`HistoryDocument`], but this reader keeps old artifacts
/// checkable with their original byte meaning.
pub fn decode_history_v1_tsv(text: &str) -> Result<History, HistoryCodecError> {
    let mut history = History::default();
    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() != 5 {
            return Err(HistoryCodecError::Invalid("v1 field count"));
        }
        let invoke = fields[3]
            .parse::<u64>()
            .map_err(|_| HistoryCodecError::Invalid("v1 invoke"))?;
        let complete = fields[4]
            .parse::<u64>()
            .map_err(|_| HistoryCodecError::Invalid("v1 completion"))?;
        let key = decode_history_v1_hex(fields[1])?;
        let kind = match fields[0] {
            "SET" => OperationKind::Set {
                key,
                value: decode_history_v1_hex(fields[2])?,
            },
            "GET" => OperationKind::Get { key },
            "DEL" => OperationKind::Del { key },
            "INCR" => OperationKind::Incr { key },
            "CAS" => OperationKind::Cas {
                key,
                expected: if fields[2] == "-" {
                    None
                } else {
                    Some(decode_history_v1_hex(fields[2])?)
                },
                value: b"cas".to_vec(),
            },
            _ => return Err(HistoryCodecError::Invalid("v1 operation")),
        };
        let outcome = match fields[0] {
            "SET" | "DEL" => Outcome::Ok,
            "GET" => Outcome::Value(if fields[2] == "-" {
                None
            } else {
                Some(decode_history_v1_hex(fields[2])?)
            }),
            "INCR" => Outcome::Integer(
                fields[2]
                    .parse()
                    .map_err(|_| HistoryCodecError::Invalid("v1 increment"))?,
            ),
            "CAS" => Outcome::Cas(fields[2] != "-"),
            _ => return Err(HistoryCodecError::Invalid("v1 operation")),
        };
        history.push(Operation::completed(
            complete,
            kind,
            Time::from_nanos(invoke),
            Time::from_nanos(complete),
            outcome,
        ));
    }
    Ok(history)
}

fn decode_history_v1_hex(text: &str) -> Result<Vec<u8>, HistoryCodecError> {
    if !text.len().is_multiple_of(2) {
        return Err(HistoryCodecError::Invalid("v1 odd hex"));
    }
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    let mut bytes = Vec::with_capacity(text.len() / 2);
    for pair in text.as_bytes().chunks_exact(2) {
        let high = digit(pair[0]).ok_or(HistoryCodecError::Invalid("v1 hex"))?;
        let low = digit(pair[1]).ok_or(HistoryCodecError::Invalid("v1 hex"))?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn bounded_count(dec: &mut Dec<'_>, total: usize, max: u32) -> Result<u32, HistoryCodecError> {
    let count = dec.u32()?;
    if count > max
        || usize::try_from(count).unwrap_or(usize::MAX) > total.saturating_sub(dec.position())
    {
        Err(HistoryCodecError::Invalid("count"))
    } else {
        Ok(count)
    }
}
fn encode_operation(enc: &mut Enc, operation: &Operation) {
    enc.u64(operation.id);
    enc.u64(operation.client);
    enc.u64(operation.sequence);
    enc.u64(operation.invoke.as_nanos());
    match operation.complete {
        Some(time) => {
            enc.u8(1);
            enc.u64(time.as_nanos());
        }
        None => enc.u8(0),
    }
    encode_operation_kind(enc, &operation.kind);
    encode_outcome(enc, &operation.outcome);
    match operation.deadline {
        Some(deadline) => {
            enc.u8(1);
            enc.u64(deadline.as_nanos());
        }
        None => enc.u8(0),
    }
    enc.u8(match operation.consistency {
        ConsistencyMode::Strong => 1,
        ConsistencyMode::StaleLocal => 2,
        ConsistencyMode::Diagnostic => 3,
    });
    match operation.diagnostic {
        DiagnosticKind::None => enc.u8(0),
        DiagnosticKind::FinalProbe => enc.u8(1),
        DiagnosticKind::ReplicaState {
            replica,
            committed_index,
            state_hash,
        } => {
            enc.u8(2);
            enc.u64(replica);
            enc.u64(committed_index);
            enc.u64(state_hash);
        }
    }
}

fn encode_operation_kind(enc: &mut Enc, kind: &OperationKind) {
    match kind {
        OperationKind::Set { key, value } => {
            enc.u8(1);
            enc.bytes(key);
            enc.bytes(value);
        }
        OperationKind::Get { key } => {
            enc.u8(2);
            enc.bytes(key);
        }
        OperationKind::Del { key } => {
            enc.u8(3);
            enc.bytes(key);
        }
        OperationKind::Incr { key } => {
            enc.u8(4);
            enc.bytes(key);
        }
        OperationKind::Cas {
            key,
            expected,
            value,
        } => {
            enc.u8(5);
            enc.bytes(key);
            opt_bytes(enc, expected);
            enc.bytes(value);
        }
        OperationKind::Scan { prefix, limit } => {
            enc.u8(6);
            opt_bytes(enc, prefix);
            enc.u32(u32::try_from(*limit).unwrap_or(u32::MAX));
        }
        OperationKind::Batch { commands } => {
            enc.u8(7);
            enc.u32(u32::try_from(commands.len()).unwrap_or(u32::MAX));
            for command in commands {
                encode_operation_kind(enc, command);
            }
        }
    }
}
fn decode_operation(dec: &mut Dec<'_>, version: u16) -> Result<Operation, HistoryCodecError> {
    let id = dec.u64()?;
    let client = dec.u64()?;
    let sequence = dec.u64()?;
    let invoke = Time::from_nanos(dec.u64()?);
    let complete = match dec.u8()? {
        0 => None,
        1 => Some(Time::from_nanos(dec.u64()?)),
        _ => return Err(HistoryCodecError::Invalid("completion flag")),
    };
    let kind = decode_operation_kind(dec, version, true)?;
    let outcome = decode_outcome(dec)?;
    let (deadline, consistency, diagnostic) = if version >= HISTORY_V3_VERSION {
        let deadline = match dec.u8()? {
            0 => None,
            1 => Some(Time::from_nanos(dec.u64()?)),
            _ => return Err(HistoryCodecError::Invalid("deadline flag")),
        };
        let consistency = match dec.u8()? {
            1 => ConsistencyMode::Strong,
            2 => ConsistencyMode::StaleLocal,
            3 => ConsistencyMode::Diagnostic,
            _ => return Err(HistoryCodecError::Invalid("consistency mode")),
        };
        let diagnostic = match dec.u8()? {
            0 => DiagnosticKind::None,
            1 => DiagnosticKind::FinalProbe,
            2 => DiagnosticKind::ReplicaState {
                replica: dec.u64()?,
                committed_index: dec.u64()?,
                state_hash: dec.u64()?,
            },
            _ => return Err(HistoryCodecError::Invalid("diagnostic kind")),
        };
        (deadline, consistency, diagnostic)
    } else {
        (None, ConsistencyMode::Strong, DiagnosticKind::None)
    };
    Ok(Operation {
        id,
        client,
        sequence,
        invoke,
        complete,
        kind,
        outcome,
        deadline,
        consistency,
        diagnostic,
    })
}

fn decode_operation_kind(
    dec: &mut Dec<'_>,
    version: u16,
    allow_batch: bool,
) -> Result<OperationKind, HistoryCodecError> {
    Ok(match dec.u8()? {
        1 => OperationKind::Set {
            key: dec.bytes()?,
            value: dec.bytes()?,
        },
        2 => OperationKind::Get { key: dec.bytes()? },
        3 => OperationKind::Del { key: dec.bytes()? },
        4 => OperationKind::Incr { key: dec.bytes()? },
        5 => OperationKind::Cas {
            key: dec.bytes()?,
            expected: decode_opt_bytes(dec)?,
            value: dec.bytes()?,
        },
        6 => OperationKind::Scan {
            prefix: decode_opt_bytes(dec)?,
            limit: usize::try_from(dec.u32()?)
                .map_err(|_| HistoryCodecError::Invalid("scan limit"))?,
        },
        7 if version >= HISTORY_VERSION && allow_batch => {
            let count = bounded_count(dec, cc_core::MAX_CODEC_BYTES, 4096)?;
            let mut commands = Vec::with_capacity(count as usize);
            for _ in 0..count {
                commands.push(decode_operation_kind(dec, version, false)?);
            }
            OperationKind::Batch { commands }
        }
        _ => return Err(HistoryCodecError::Invalid("operation tag")),
    })
}
fn opt_bytes(enc: &mut Enc, value: &Option<Vec<u8>>) {
    match value {
        Some(value) => {
            enc.u8(1);
            enc.bytes(value);
        }
        None => enc.u8(0),
    }
}
fn decode_opt_bytes(dec: &mut Dec<'_>) -> Result<Option<Vec<u8>>, HistoryCodecError> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(dec.bytes()?)),
        _ => Err(HistoryCodecError::Invalid("option flag")),
    }
}
fn encode_outcome(enc: &mut Enc, outcome: &Outcome) {
    match outcome {
        Outcome::Ok => enc.u8(1),
        Outcome::Value(value) => {
            enc.u8(2);
            opt_bytes(enc, value);
        }
        Outcome::Integer(value) => {
            enc.u8(3);
            enc.u64(*value as u64);
        }
        Outcome::Cas(value) => {
            enc.u8(4);
            enc.u8(u8::from(*value));
        }
        Outcome::Scan(values) => {
            enc.u8(5);
            enc.u32(u32::try_from(values.len()).unwrap_or(u32::MAX));
            for (key, value) in values {
                enc.bytes(key);
                enc.bytes(value);
            }
        }
        Outcome::Batch { replies } => {
            enc.u8(8);
            enc.u32(u32::try_from(replies.len()).unwrap_or(u32::MAX));
            for reply in replies {
                encode_outcome(enc, reply);
            }
        }
        Outcome::Error => enc.u8(6),
        Outcome::Timeout => enc.u8(7),
    }
}
fn decode_outcome(dec: &mut Dec<'_>) -> Result<Outcome, HistoryCodecError> {
    Ok(match dec.u8()? {
        1 => Outcome::Ok,
        2 => Outcome::Value(decode_opt_bytes(dec)?),
        3 => Outcome::Integer(dec.u64()? as i64),
        4 => Outcome::Cas(match dec.u8()? {
            0 => false,
            1 => true,
            _ => return Err(HistoryCodecError::Invalid("boolean")),
        }),
        5 => {
            let count = bounded_count(dec, cc_core::MAX_CODEC_BYTES, 4096)?;
            let mut values = Vec::with_capacity(count as usize);
            for _ in 0..count {
                values.push((dec.bytes()?, dec.bytes()?));
            }
            Outcome::Scan(values)
        }
        6 => Outcome::Error,
        7 => Outcome::Timeout,
        8 => {
            let count = bounded_count(dec, cc_core::MAX_CODEC_BYTES, 4096)?;
            let mut replies = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let reply = decode_outcome(dec)?;
                if matches!(reply, Outcome::Batch { .. }) {
                    return Err(HistoryCodecError::Invalid("nested batch outcome"));
                }
                replies.push(reply);
            }
            Outcome::Batch { replies }
        }
        _ => return Err(HistoryCodecError::Invalid("outcome tag")),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckerConfig {
    pub max_states: u64,
    pub witness_budget: u32,
}

impl Default for CheckerConfig {
    fn default() -> Self {
        Self {
            max_states: 10_000_000,
            witness_budget: 200,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict {
    Linearizable { visited: u64 },
    NotLinearizable { witness: Witness, visited: u64 },
    Undecided { visited: u64 },
}

/// Trace evidence attached to one explicitly stale local read. The reported
/// watermark and the traced state watermark are deliberately separate so a
/// future-state observation cannot validate an older claimed index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaleReadEvidence {
    pub operation_id: u64,
    pub applied_index: u64,
    pub applied_term: u64,
    pub traced_index: u64,
    pub traced_term: u64,
    pub read_time: Time,
    pub value_at_traced_index: Option<Vec<u8>>,
    pub deadline: Option<Time>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StaleReadVerdict {
    Valid {
        checked: usize,
    },
    Invalid {
        operation_id: u64,
        reason: &'static str,
    },
    Undecided {
        operation_id: u64,
        reason: &'static str,
    },
}

/// Check only the narrow claim made by `READ STALE`: the returned value is
/// exactly the traced local value at the reported applied watermark after
/// filtering expiry at the reported read time. No contact-age or wall-time
/// freshness bound is inferred.
#[must_use]
pub fn check_stale_reads(history: &History, evidence: &[StaleReadEvidence]) -> StaleReadVerdict {
    let mut checked = 0_usize;
    for operation in history
        .operations
        .iter()
        .filter(|operation| operation.consistency == ConsistencyMode::StaleLocal)
    {
        let Some(observation) = evidence
            .iter()
            .find(|observation| observation.operation_id == operation.id)
        else {
            return StaleReadVerdict::Undecided {
                operation_id: operation.id,
                reason: "missing traced state at reported applied index",
            };
        };
        if !matches!(operation.kind, OperationKind::Get { .. })
            || !matches!(operation.outcome, Outcome::Value(_))
        {
            return StaleReadVerdict::Invalid {
                operation_id: operation.id,
                reason: "stale evidence is defined only for completed GET values",
            };
        }
        if observation.applied_index != observation.traced_index
            || observation.applied_term != observation.traced_term
        {
            return StaleReadVerdict::Invalid {
                operation_id: operation.id,
                reason: "trace watermark differs from the reported applied watermark",
            };
        }
        let visible = if observation
            .deadline
            .is_some_and(|deadline| deadline <= observation.read_time)
        {
            None
        } else {
            observation.value_at_traced_index.clone()
        };
        if operation.outcome != Outcome::Value(visible) {
            return StaleReadVerdict::Invalid {
                operation_id: operation.id,
                reason: "reply differs from traced state after TTL filtering",
            };
        }
        checked = checked.saturating_add(1);
    }
    StaleReadVerdict::Valid { checked }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Witness {
    pub operation_ids: Vec<u64>,
    pub oracle_calls: u32,
    pub budget_exhausted: bool,
    pub one_minimal: bool,
}

/// Produce a bounded one-deletion-minimal witness for a completed failing
/// history. A timeout/undecided oracle is never accepted as evidence.
#[must_use]
pub fn minimize_witness(history: &History, config: CheckerConfig, budget: u32) -> Option<Witness> {
    minimize_witness_with_initial(history, initial_model(BTreeMap::new()), config, budget)
}

/// Minimize a failed v2 receipt while retaining the exact initial state image
/// that made the history meaningful.  This is the window-safe counterpart to
/// [`minimize_witness`].
#[must_use]
pub fn minimize_document_witness(
    document: &HistoryDocument,
    config: CheckerConfig,
    budget: u32,
) -> Option<Witness> {
    minimize_witness_with_initial(
        &document.history,
        initial_model(document.initial.clone()),
        config,
        budget,
    )
}

fn minimize_witness_with_initial(
    history: &History,
    initial: Model,
    config: CheckerConfig,
    budget: u32,
) -> Option<Witness> {
    let mut cache = BTreeMap::new();
    let mut calls = 0_u32;
    if !witness_oracle(
        &history.operations,
        &initial,
        config,
        budget,
        &mut calls,
        &mut cache,
    )? {
        return None;
    }
    let mut candidate = history.operations.clone();
    let mut granularity = 2_usize;
    while candidate.len() >= 2 && calls < budget {
        let chunk = candidate.len().div_ceil(granularity);
        let mut removed = false;
        for start in (0..candidate.len()).step_by(chunk.max(1)) {
            if calls >= budget {
                break;
            }
            let end = (start + chunk).min(candidate.len());
            let reduced = candidate
                .iter()
                .enumerate()
                .filter(|(index, _)| *index < start || *index >= end)
                .map(|(_, operation)| operation.clone())
                .collect::<Vec<_>>();
            if reduced.is_empty() {
                continue;
            }
            if witness_oracle(&reduced, &initial, config, budget, &mut calls, &mut cache)
                == Some(true)
            {
                candidate = reduced;
                granularity = 2;
                removed = true;
                break;
            }
        }
        if !removed {
            if granularity >= candidate.len() {
                break;
            }
            granularity = (granularity.saturating_mul(2)).min(candidate.len());
        }
    }
    let one_minimal = 'deletion: loop {
        for index in (0..candidate.len()).rev() {
            if calls >= budget {
                break 'deletion false;
            }
            let reduced = candidate
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .map(|(_, operation)| operation.clone())
                .collect::<Vec<_>>();
            if witness_oracle(&reduced, &initial, config, budget, &mut calls, &mut cache)
                == Some(true)
            {
                candidate = reduced;
                continue 'deletion;
            }
        }
        break true;
    };
    Some(Witness {
        operation_ids: candidate.iter().map(|operation| operation.id).collect(),
        oracle_calls: calls,
        budget_exhausted: calls >= budget,
        one_minimal,
    })
}

fn witness_oracle(
    operations: &[Operation],
    initial: &Model,
    config: CheckerConfig,
    budget: u32,
    calls: &mut u32,
    cache: &mut BTreeMap<Vec<u64>, bool>,
) -> Option<bool> {
    let mut ids = operations
        .iter()
        .map(|operation| operation.id)
        .collect::<Vec<_>>();
    ids.sort_unstable();
    if let Some(result) = cache.get(&ids) {
        return Some(*result);
    }
    if *calls >= budget {
        return None;
    }
    *calls = calls.saturating_add(1);
    let result = matches!(
        check_with_initial_raw(
            &History {
                operations: operations.to_vec(),
            },
            initial.clone(),
            config
        ),
        Verdict::NotLinearizable { .. }
    );
    cache.insert(ids, result);
    Some(result)
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MemoKey {
    remaining: Vec<usize>,
    state: Vec<(Vec<u8>, Vec<u8>, Option<Time>)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ModelValue {
    value: Vec<u8>,
    deadline: Option<Time>,
}

type Model = BTreeMap<Vec<u8>, ModelValue>;

fn initial_model(initial: BTreeMap<Vec<u8>, Vec<u8>>) -> Model {
    initial
        .into_iter()
        .map(|(key, value)| {
            (
                key,
                ModelValue {
                    value,
                    deadline: None,
                },
            )
        })
        .collect()
}

enum SearchResult {
    Found,
    NotFound,
    Undecided,
}

/// Check a per-key register history with an open-operation branch.
#[must_use]
pub fn check(history: &History, config: CheckerConfig) -> Verdict {
    check_with_initial(history, BTreeMap::new(), config)
}

/// Check a complete v2 receipt using the explicit state image carried in its
/// header.  A caller cannot accidentally treat a bounded trace window as if
/// it began from an empty database.
#[must_use]
pub fn check_document(document: &HistoryDocument, config: CheckerConfig) -> Verdict {
    check_with_initial(&document.history, document.initial.clone(), config)
}

/// Window checking is deliberately opt-in and requires a supplied initial
/// state receipt. `Some(empty)` is a valid assertion that the window began
/// empty; `None` is rejected rather than silently assuming it.
pub fn check_window(
    history: &History,
    initial: Option<BTreeMap<Vec<u8>, Vec<u8>>>,
    config: CheckerConfig,
) -> Result<Verdict, HistoryCodecError> {
    let initial = initial.ok_or(HistoryCodecError::Invalid("window initial receipt"))?;
    Ok(check_with_initial(history, initial, config))
}

fn check_with_initial(
    history: &History,
    initial: BTreeMap<Vec<u8>, Vec<u8>>,
    config: CheckerConfig,
) -> Verdict {
    let initial = initial_model(initial);
    let raw = check_with_initial_raw(history, initial.clone(), config);
    let Verdict::NotLinearizable { visited, .. } = raw else {
        return raw;
    };
    let witness = minimize_witness_with_initial(history, initial, config, config.witness_budget)
        .unwrap_or_else(|| Witness {
            operation_ids: history
                .operations
                .iter()
                .map(|operation| operation.id)
                .collect(),
            oracle_calls: 0,
            budget_exhausted: config.witness_budget == 0,
            one_minimal: false,
        });
    Verdict::NotLinearizable { witness, visited }
}

fn check_with_initial_raw(history: &History, initial: Model, config: CheckerConfig) -> Verdict {
    // Local stale observations and diagnostic probes make different claims;
    // neither is an operation in the linearizable history.
    let history = History {
        operations: history
            .operations
            .iter()
            .filter(|operation| {
                operation.consistency == ConsistencyMode::Strong
                    && operation.diagnostic == DiagnosticKind::None
            })
            .cloned()
            .collect(),
    };
    let history = &history;
    if history.operations.iter().any(|operation| {
        matches!(
            operation.kind,
            OperationKind::Scan { .. } | OperationKind::Batch { .. }
        )
    }) {
        return check_single(history, initial, config);
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
        for (key, key_history) in &per_key {
            let key_initial = initial
                .get(key)
                .map(|value| [(key.clone(), value.clone())].into_iter().collect())
                .unwrap_or_default();
            match check_single(key_history, key_initial, config) {
                Verdict::Linearizable { visited: count } => visited = visited.saturating_add(count),
                Verdict::Undecided { visited: count } => {
                    visited = visited.saturating_add(count);
                    undecided = true;
                }
                Verdict::NotLinearizable {
                    witness,
                    visited: count,
                } => {
                    return Verdict::NotLinearizable {
                        witness,
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
    check_single(history, initial, config)
}

fn check_single(history: &History, initial: Model, config: CheckerConfig) -> Verdict {
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
        initial,
        config,
        &mut visited,
        &mut memo,
        &mut path,
    );
    match result {
        SearchResult::Found => Verdict::Linearizable { visited },
        SearchResult::Undecided => Verdict::Undecided { visited },
        SearchResult::NotFound => Verdict::NotLinearizable {
            witness: Witness {
                operation_ids: history
                    .operations
                    .iter()
                    .map(|operation| operation.id)
                    .collect(),
                oracle_calls: 0,
                budget_exhausted: false,
                one_minimal: false,
            },
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
            .map(|(key, value)| (key.clone(), value.value.clone(), value.deadline))
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
            if let Some(next_model) = apply_without_observation(&model, operation) {
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
        } else if let Some(next_model) = apply_observed(&model, operation) {
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

fn apply_without_observation(model: &Model, operation: &Operation) -> Option<Model> {
    apply_operation(model, &operation.kind, operation.deadline).map(|(next, _)| next)
}

fn apply_observed(model: &Model, operation: &Operation) -> Option<Model> {
    if matches!(operation.outcome, Outcome::Error) {
        return Some(model.clone());
    }
    if let OperationKind::Get { key } = &operation.kind {
        let stored = model.get(key);
        let visible_allowed = stored.is_some_and(|stored| {
            stored
                .deadline
                .is_none_or(|deadline| operation.invoke < deadline)
        });
        let absent_allowed = stored.is_none_or(|stored| {
            stored.deadline.is_some_and(|deadline| {
                operation
                    .complete
                    .is_some_and(|complete| complete >= deadline)
            })
        });
        return match &operation.outcome {
            Outcome::Value(Some(observed))
                if visible_allowed && stored.is_some_and(|stored| stored.value == *observed) =>
            {
                Some(model.clone())
            }
            Outcome::Value(None) if absent_allowed => Some(model.clone()),
            _ => None,
        };
    }
    let (next, expected) = apply_operation(model, &operation.kind, operation.deadline)?;
    if expected == operation.outcome {
        Some(next)
    } else {
        None
    }
}

fn apply_operation(
    model: &Model,
    operation: &OperationKind,
    deadline: Option<Time>,
) -> Option<(Model, Outcome)> {
    let mut next = model.clone();
    let outcome = match operation {
        OperationKind::Set { key, value } => {
            next.insert(
                key.clone(),
                ModelValue {
                    value: value.clone(),
                    deadline,
                },
            );
            Outcome::Ok
        }
        OperationKind::Get { key } => {
            Outcome::Value(next.get(key).map(|value| value.value.clone()))
        }
        OperationKind::Del { key } => {
            next.remove(key);
            Outcome::Ok
        }
        OperationKind::Incr { key } => {
            let old = next
                .get(key)
                .and_then(|value| std::str::from_utf8(&value.value).ok())
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0);
            let new = old.checked_add(1)?;
            next.insert(
                key.clone(),
                ModelValue {
                    value: new.to_string().into_bytes(),
                    deadline: None,
                },
            );
            Outcome::Integer(new)
        }
        OperationKind::Cas {
            key,
            expected,
            value,
        } => {
            if next.get(key).map(|stored| &stored.value) == expected.as_ref() {
                next.insert(
                    key.clone(),
                    ModelValue {
                        value: value.clone(),
                        deadline: None,
                    },
                );
                Outcome::Cas(true)
            } else {
                Outcome::Cas(false)
            }
        }
        OperationKind::Scan { prefix, limit } => {
            let mut values = next
                .iter()
                .filter(|(key, _)| prefix.as_ref().is_none_or(|prefix| key.starts_with(prefix)))
                .map(|(key, value)| (key.clone(), value.value.clone()))
                .collect::<Vec<_>>();
            values.truncate(*limit);
            Outcome::Scan(values)
        }
        OperationKind::Batch { commands } => {
            if commands.is_empty()
                || commands
                    .iter()
                    .any(|command| matches!(command, OperationKind::Batch { .. }))
            {
                return None;
            }
            let mut replies = Vec::with_capacity(commands.len());
            for command in commands {
                let (updated, reply) = apply_operation(&next, command, deadline)?;
                next = updated;
                replies.push(reply);
            }
            Outcome::Batch { replies }
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
                    && matches!(operation.outcome, Outcome::Value(Some(_))) =>
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

/// Conservative, evidence-based labels for an already captured history.
/// These labels are diagnostic only: they do not weaken or replace the
/// linearizability verdict, and overlapping operations never manufacture an
/// order that the history did not observe.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AnomalyClass {
    DirtyRead,
    StaleRead,
    Resurrection,
    NonMonotonicIncr,
    DuplicateApply,
    AckedWriteLost,
    LostUpdate,
    ReplicaDivergence,
    Unclassified,
}

impl AnomalyClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirtyRead => "dirty-read",
            Self::StaleRead => "stale-read",
            Self::Resurrection => "resurrection",
            Self::NonMonotonicIncr => "non-monotonic-incr",
            Self::DuplicateApply => "duplicate-apply",
            Self::AckedWriteLost => "acked-write-lost",
            Self::LostUpdate => "lost-update",
            Self::ReplicaDivergence => "replica-divergence",
            Self::Unclassified => "unclassified",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Anomaly {
    pub class: AnomalyClass,
    pub operation_ids: Vec<u64>,
    pub predicate: &'static str,
}

/// Classify only facts that are certain from real-time intervals.  The
#[must_use]
pub fn classify_anomalies(history: &History, initial: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();
    for read in &history.operations {
        let (OperationKind::Get { key }, Outcome::Value(value)) = (&read.kind, &read.outcome)
        else {
            continue;
        };
        let Some(read_complete) = read.complete else {
            continue;
        };
        if read.consistency != ConsistencyMode::Strong || read.diagnostic != DiagnosticKind::None {
            continue;
        }

        for deleted in &history.operations {
            if !matches!(&deleted.kind, OperationKind::Del { key: deleted_key } if deleted_key == key)
                || deleted.outcome != Outcome::Ok
                || !happens_before(deleted, read)
                || has_intervening_mutation(history, key, deleted.invoke, read_complete, deleted.id)
                || value.is_none()
            {
                continue;
            }
            anomalies.push(Anomaly {
                class: AnomalyClass::Resurrection,
                operation_ids: vec![deleted.id, read.id],
                predicate: "acknowledged delete precedes nonnil read without an intervening mutation",
            });
        }

        if let Some(observed) = value {
            for written in &history.operations {
                let Some(expected) = successful_write_value(written) else {
                    continue;
                };
                if written.kind.key() != key
                    || !happens_before(written, read)
                    || expected == observed
                    || !known_value_before(history, initial, key, observed, read)
                    || !persistence_interval_is_before_deadline(written, read_complete)
                    || has_intervening_mutation(
                        history,
                        key,
                        written.invoke,
                        read_complete,
                        written.id,
                    )
                {
                    continue;
                }
                anomalies.push(Anomaly {
                    class: AnomalyClass::StaleRead,
                    operation_ids: vec![written.id, read.id],
                    predicate: "acknowledged write precedes a different read value without an intervening mutation",
                });
            }

            let known_prior_value = initial.get(key) == Some(observed)
                || history.operations.iter().any(|operation| {
                    successful_write_value(operation).is_some_and(|written| {
                        operation.kind.key() == key
                            && written == observed
                            && operation.invoke <= read_complete
                    })
                });
            if !known_prior_value {
                anomalies.push(Anomaly {
                    class: AnomalyClass::DirtyRead,
                    operation_ids: vec![read.id],
                    predicate: "read value is absent from initial state and all certainly preceding writes",
                });
            }
        }
    }
    classify_non_monotonic_increments(history, &mut anomalies);
    classify_final_probe_failures(history, initial, &mut anomalies);
    classify_lost_updates(history, &mut anomalies);
    classify_replica_divergence(history, &mut anomalies);
    anomalies.sort_by(|left, right| {
        left.class
            .cmp(&right.class)
            .then_with(|| left.operation_ids.cmp(&right.operation_ids))
            .then_with(|| left.predicate.cmp(right.predicate))
    });
    anomalies.dedup();
    if anomalies.is_empty() {
        anomalies.push(Anomaly {
            class: AnomalyClass::Unclassified,
            operation_ids: Vec::new(),
            predicate: "no conservative anomaly predicate matched",
        });
    }
    anomalies
}

fn known_value_before(
    history: &History,
    initial: &BTreeMap<Vec<u8>, Vec<u8>>,
    key: &[u8],
    value: &[u8],
    read: &Operation,
) -> bool {
    initial.get(key).is_some_and(|initial| initial == value)
        || history.operations.iter().any(|operation| {
            successful_write_value(operation).is_some_and(|written| {
                operation.kind.key() == key && written == value && happens_before(operation, read)
            })
        })
}

fn persistence_interval_is_before_deadline(operation: &Operation, end: Time) -> bool {
    operation.deadline.is_none_or(|deadline| end < deadline)
}

fn classify_non_monotonic_increments(history: &History, anomalies: &mut Vec<Anomaly>) {
    for earlier in &history.operations {
        let (OperationKind::Incr { key }, Outcome::Integer(left)) =
            (&earlier.kind, &earlier.outcome)
        else {
            continue;
        };
        for later in &history.operations {
            let (OperationKind::Incr { key: later_key }, Outcome::Integer(right)) =
                (&later.kind, &later.outcome)
            else {
                continue;
            };
            if key == later_key
                && happens_before(earlier, later)
                && right < left
                && !has_intervening_reset(history, key, earlier, later)
            {
                anomalies.push(Anomaly {
                    class: AnomalyClass::NonMonotonicIncr,
                    operation_ids: vec![earlier.id, later.id],
                    predicate: "real-time-ordered positive increments returned a decreasing value without an intervening reset",
                });
            }
        }
    }
}

fn has_intervening_reset(
    history: &History,
    key: &[u8],
    earlier: &Operation,
    later: &Operation,
) -> bool {
    history.operations.iter().any(|operation| {
        operation.id != earlier.id
            && operation.id != later.id
            && operation.kind.key() == key
            && matches!(
                (&operation.kind, &operation.outcome),
                (
                    OperationKind::Set { .. } | OperationKind::Del { .. },
                    Outcome::Ok
                ) | (OperationKind::Cas { .. }, Outcome::Cas(true))
            )
            && operation.invoke < later.invoke
            && operation
                .complete
                .is_some_and(|complete| complete > earlier.complete.unwrap_or(earlier.invoke))
    })
}

fn classify_final_probe_failures(
    history: &History,
    initial: &BTreeMap<Vec<u8>, Vec<u8>>,
    anomalies: &mut Vec<Anomaly>,
) {
    for probe in &history.operations {
        let (OperationKind::Get { key }, DiagnosticKind::FinalProbe) =
            (&probe.kind, &probe.diagnostic)
        else {
            continue;
        };
        let Some(probe_complete) = probe.complete else {
            continue;
        };
        for mutation in &history.operations {
            if mutation.kind.key() != key
                || !is_successful_mutation(mutation)
                || !happens_before(mutation, probe)
                || !persistence_interval_is_before_deadline(mutation, probe_complete)
                || has_intervening_mutation(
                    history,
                    key,
                    mutation.invoke,
                    probe_complete,
                    mutation.id,
                )
            {
                continue;
            }
            if expected_mutation_observation(mutation)
                .is_some_and(|expected| !observation_matches(&expected, &probe.outcome))
            {
                anomalies.push(Anomaly {
                    class: AnomalyClass::AckedWriteLost,
                    operation_ids: vec![mutation.id, probe.id],
                    predicate: "acknowledged final mutation precedes a quiescent strong final probe returning another state",
                });
            }
        }

        let successful = history
            .operations
            .iter()
            .filter(|operation| operation.kind.key() == key && happens_before(operation, probe))
            .filter(|operation| is_successful_mutation(operation))
            .collect::<Vec<_>>();
        if !successful.is_empty()
            && successful
                .iter()
                .all(|operation| matches!(operation.kind, OperationKind::Incr { .. }))
        {
            let unique = successful
                .iter()
                .map(|operation| (operation.client, operation.sequence))
                .collect::<BTreeSet<_>>();
            if unique.len() < successful.len() {
                let initial_value = initial
                    .get(key)
                    .and_then(|value| parse_numeric_bytes(value))
                    .unwrap_or(0);
                let expected =
                    initial_value.saturating_add(i64::try_from(unique.len()).unwrap_or(i64::MAX));
                if outcome_numeric(&probe.outcome).is_some_and(|actual| actual > expected) {
                    let mut ids = successful
                        .iter()
                        .map(|operation| operation.id)
                        .collect::<Vec<_>>();
                    ids.push(probe.id);
                    anomalies.push(Anomaly {
                        class: AnomalyClass::DuplicateApply,
                        operation_ids: ids,
                        predicate: "final numeric probe exceeds the sum of unique positive increments for repeated client sequences",
                    });
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedObservation {
    Value(Option<Vec<u8>>),
    Integer(i64),
}

fn expected_mutation_observation(operation: &Operation) -> Option<ExpectedObservation> {
    match (&operation.kind, &operation.outcome) {
        (OperationKind::Set { value, .. }, Outcome::Ok)
        | (OperationKind::Cas { value, .. }, Outcome::Cas(true)) => {
            Some(ExpectedObservation::Value(Some(value.clone())))
        }
        (OperationKind::Del { .. }, Outcome::Ok) => Some(ExpectedObservation::Value(None)),
        (OperationKind::Incr { .. }, Outcome::Integer(value)) => {
            Some(ExpectedObservation::Integer(*value))
        }
        _ => None,
    }
}

fn observation_matches(expected: &ExpectedObservation, outcome: &Outcome) -> bool {
    match (expected, outcome) {
        (ExpectedObservation::Value(expected), Outcome::Value(actual)) => expected == actual,
        (ExpectedObservation::Integer(expected), actual) => {
            outcome_numeric(actual) == Some(*expected)
        }
        _ => false,
    }
}

fn outcome_numeric(outcome: &Outcome) -> Option<i64> {
    match outcome {
        Outcome::Integer(value) => Some(*value),
        Outcome::Value(Some(value)) => parse_numeric_bytes(value),
        _ => None,
    }
}

fn parse_numeric_bytes(value: &[u8]) -> Option<i64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn classify_lost_updates(history: &History, anomalies: &mut Vec<Anomaly>) {
    for left in &history.operations {
        let (
            OperationKind::Cas {
                key,
                expected,
                value,
            },
            Outcome::Cas(true),
        ) = (&left.kind, &left.outcome)
        else {
            continue;
        };
        for right in &history.operations {
            let (
                OperationKind::Cas {
                    key: right_key,
                    expected: right_expected,
                    value: right_value,
                },
                Outcome::Cas(true),
            ) = (&right.kind, &right.outcome)
            else {
                continue;
            };
            if left.id >= right.id
                || key != right_key
                || expected != right_expected
                || value == right_value
                || !(happens_before(left, right) || happens_before(right, left))
            {
                continue;
            }
            for probe in &history.operations {
                if !matches!(probe.diagnostic, DiagnosticKind::FinalProbe)
                    || probe.kind.key() != key
                    || !happens_before(left, probe)
                    || !happens_before(right, probe)
                {
                    continue;
                }
                let observed = match &probe.outcome {
                    Outcome::Value(Some(value)) => value,
                    _ => continue,
                };
                if observed == value || observed == right_value {
                    anomalies.push(Anomaly {
                        class: AnomalyClass::LostUpdate,
                        operation_ids: vec![left.id, right.id, probe.id],
                        predicate: "two ordered compare-and-set transitions from one predecessor both acknowledged while the final probe contains only one result",
                    });
                }
            }
        }
    }
}

fn classify_replica_divergence(history: &History, anomalies: &mut Vec<Anomaly>) {
    for (index, left) in history.operations.iter().enumerate() {
        let DiagnosticKind::ReplicaState {
            replica,
            committed_index,
            state_hash,
        } = left.diagnostic
        else {
            continue;
        };
        for right in history.operations.iter().skip(index + 1) {
            let DiagnosticKind::ReplicaState {
                replica: right_replica,
                committed_index: right_index,
                state_hash: right_hash,
            } = right.diagnostic
            else {
                continue;
            };
            if replica != right_replica
                && committed_index == right_index
                && state_hash != right_hash
            {
                anomalies.push(Anomaly {
                    class: AnomalyClass::ReplicaDivergence,
                    operation_ids: vec![left.id, right.id],
                    predicate: "diagnostic replica probes at one committed index returned different state hashes",
                });
            }
        }
    }
}

fn happens_before(left: &Operation, right: &Operation) -> bool {
    left.complete
        .is_some_and(|complete| complete < right.invoke)
}

fn has_intervening_mutation(
    history: &History,
    key: &[u8],
    left_invoke: Time,
    right_complete: Time,
    excluded_id: u64,
) -> bool {
    history.operations.iter().any(|operation| {
        operation.id != excluded_id
            && operation.kind.key() == key
            && is_successful_mutation(operation)
            // Equal endpoints are concurrent under this history's strict
            // happens-before relation and can therefore linearize between the
            // two observations.  Exclude a mutation only when real time forces
            // it wholly before the left operation or wholly after the right.
            && operation.invoke <= right_complete
            && operation
                .complete
                .is_some_and(|complete| complete >= left_invoke)
    })
}

fn is_successful_mutation(operation: &Operation) -> bool {
    matches!(
        (&operation.kind, &operation.outcome),
        (
            OperationKind::Set { .. } | OperationKind::Del { .. },
            Outcome::Ok
        ) | (OperationKind::Cas { .. }, Outcome::Cas(true))
            | (OperationKind::Incr { .. }, Outcome::Integer(_))
    )
}

fn successful_write_value(operation: &Operation) -> Option<&[u8]> {
    match (&operation.kind, &operation.outcome) {
        (OperationKind::Set { value, .. }, Outcome::Ok) => Some(value),
        (OperationKind::Cas { value, .. }, Outcome::Cas(true)) => Some(value),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SessionGuarantee {
    ReadYourWrites,
    MonotonicReads,
    MonotonicWrites,
    WritesFollowReads,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionViolation {
    pub client: u64,
    pub guarantee: SessionGuarantee,
    pub operation_ids: Vec<u64>,
    pub predicate: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientSessionReport {
    pub client: u64,
    pub eligible_for_linearizability_cross_check: bool,
    pub operation_ids: Vec<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionReport {
    pub clients: Vec<ClientSessionReport>,
    pub violations: Vec<SessionViolation>,
}

impl SessionReport {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Check client-scoped guarantees without reusing the linearizability search
/// or model transition code. Diagnostics, stale-local reads, admin/client 0,
/// timeouts, and unknown outcomes are outside this report's proof domain.
#[must_use]
pub fn check_session_guarantees(history: &History) -> SessionReport {
    let mut by_client = BTreeMap::<u64, Vec<&Operation>>::new();
    for operation in &history.operations {
        if operation.client != 0 {
            by_client
                .entry(operation.client)
                .or_default()
                .push(operation);
        }
    }
    let mut report = SessionReport::default();
    for (client, mut operations) in by_client {
        operations.sort_by_key(|operation| (operation.invoke, operation.id));
        let in_domain = operations.iter().all(|operation| {
            operation.complete.is_some()
                && operation.consistency == ConsistencyMode::Strong
                && operation.diagnostic == DiagnosticKind::None
                && !matches!(operation.outcome, Outcome::Timeout | Outcome::Error)
        });
        let sequential = operations.windows(2).all(|pair| {
            pair[0]
                .complete
                .is_some_and(|complete| complete < pair[1].invoke)
        });
        report.clients.push(ClientSessionReport {
            client,
            eligible_for_linearizability_cross_check: in_domain && sequential,
            operation_ids: operations.iter().map(|operation| operation.id).collect(),
        });
        if !in_domain || !sequential {
            continue;
        }
        for pair in operations.windows(2) {
            let earlier = pair[0];
            let later = pair[1];
            if earlier.sequence > later.sequence {
                let guarantee = if matches!(earlier.kind, OperationKind::Get { .. })
                    && is_successful_mutation(later)
                {
                    SessionGuarantee::WritesFollowReads
                } else {
                    SessionGuarantee::MonotonicWrites
                };
                report.violations.push(SessionViolation {
                    client,
                    guarantee,
                    operation_ids: vec![earlier.id, later.id],
                    predicate: "a sequential client request sequence regressed",
                });
            }
        }
        for write in &operations {
            let Some(expected) = expected_mutation_observation(write) else {
                continue;
            };
            for read in &operations {
                if !matches!(read.kind, OperationKind::Get { .. })
                    || read.kind.key() != write.kind.key()
                    || !happens_before(write, read)
                    || write.deadline.is_some_and(|deadline| {
                        read.complete.is_none_or(|complete| complete >= deadline)
                    })
                    || has_intervening_mutation(
                        history,
                        write.kind.key(),
                        write.invoke,
                        read.complete.unwrap_or(read.invoke),
                        write.id,
                    )
                {
                    continue;
                }
                if !observation_matches(&expected, &read.outcome) {
                    report.violations.push(SessionViolation {
                        client,
                        guarantee: SessionGuarantee::ReadYourWrites,
                        operation_ids: vec![write.id, read.id],
                        predicate: "a later sequential strong read did not observe the client's quiescent acknowledged write",
                    });
                }
            }
        }
        let reads = operations
            .iter()
            .copied()
            .filter(|operation| matches!(operation.kind, OperationKind::Get { .. }))
            .collect::<Vec<_>>();
        for pair in reads.windows(2) {
            let earlier = pair[0];
            let later = pair[1];
            if earlier.kind.key() == later.kind.key()
                && earlier.outcome != later.outcome
                && !has_possible_expiry_between(history, earlier, later)
                && !has_intervening_mutation(
                    history,
                    earlier.kind.key(),
                    earlier.invoke,
                    later.complete.unwrap_or(later.invoke),
                    earlier.id,
                )
            {
                report.violations.push(SessionViolation {
                    client,
                    guarantee: SessionGuarantee::MonotonicReads,
                    operation_ids: vec![earlier.id, later.id],
                    predicate: "two sequential strong reads changed without any intervening mutation",
                });
            }
        }
    }
    report
}

fn has_possible_expiry_between(history: &History, left: &Operation, right: &Operation) -> bool {
    history.operations.iter().any(|operation| {
        operation.kind.key() == left.kind.key()
            && operation.deadline.is_some_and(|deadline| {
                deadline >= left.invoke
                    && right.complete.is_some_and(|complete| deadline <= complete)
            })
    })
}

/// Release-blocking implication: a linearizable verdict and an eligible
/// well-formed strong client history cannot contradict the session checker.
#[must_use]
pub fn cross_check_linearizable_sessions(history: &History, verdict: &Verdict) -> InvariantReport {
    if !matches!(verdict, Verdict::Linearizable { .. }) {
        return InvariantReport::default();
    }
    let report = check_session_guarantees(history);
    let eligible = report
        .clients
        .iter()
        .filter(|client| client.eligible_for_linearizability_cross_check)
        .map(|client| client.client)
        .collect::<BTreeSet<_>>();
    InvariantReport {
        violations: report
            .violations
            .into_iter()
            .filter(|violation| eligible.contains(&violation.client))
            .map(|violation| InvariantViolation {
                name: "linearizable_session_implication",
                detail: format!(
                    "client {} {:?} contradiction at {:?}",
                    violation.client, violation.guarantee, violation.operation_ids
                ),
            })
            .collect(),
    }
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
                    max_states: 100_000,
                    ..CheckerConfig::default()
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
                    max_states: 100_000,
                    ..CheckerConfig::default()
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
        OperationKind::Batch { commands } => format!(
            "[\"batch\",[{}]]",
            commands
                .iter()
                .map(operation_value)
                .collect::<Vec<_>>()
                .join(",")
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
        Outcome::Batch { replies } => format!(
            "[{}]",
            replies
                .iter()
                .map(outcome_value)
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

    #[test]
    fn trap_batch_is_one_atomic_checker_transition() {
        let batch = Operation::completed(
            1,
            OperationKind::Batch {
                commands: vec![
                    OperationKind::Set {
                        key: b"a".to_vec(),
                        value: b"new-a".to_vec(),
                    },
                    OperationKind::Set {
                        key: b"b".to_vec(),
                        value: b"new-b".to_vec(),
                    },
                    OperationKind::Get { key: b"a".to_vec() },
                ],
            },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Batch {
                replies: vec![
                    Outcome::Ok,
                    Outcome::Ok,
                    Outcome::Value(Some(b"new-a".to_vec())),
                ],
            },
        );
        let document = HistoryDocument {
            build_label: String::from("batch-checker"),
            config_hash: 7,
            initial: BTreeMap::from([
                (b"a".to_vec(), b"old-a".to_vec()),
                (b"b".to_vec(), b"old-b".to_vec()),
            ]),
            retain_open: true,
            history: History {
                operations: vec![batch.clone()],
            },
        };
        assert_eq!(
            HistoryDocument::decode(&document.encode()).expect("batch history"),
            document
        );
        assert!(matches!(
            check(
                &History {
                    operations: vec![batch]
                },
                CheckerConfig::default()
            ),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn trap_failed_batch_cannot_publish_partial_state() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Batch {
                        commands: vec![OperationKind::Set {
                            key: b"a".to_vec(),
                            value: b"published".to_vec(),
                        }],
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Error,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"a".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(Some(b"published".to_vec())),
                ),
            ],
        };
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
    }
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
    fn trap_history_v2_round_trips_binary_keys_and_values() {
        let mut history = History::default();
        history.push(Operation::completed(
            1,
            OperationKind::Set {
                key: vec![0, 0xff],
                value: vec![0, 9],
            },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Ok,
        ));
        let document = HistoryDocument {
            build_label: String::from("test"),
            config_hash: 7,
            initial: BTreeMap::new(),
            retain_open: true,
            history,
        };
        assert_eq!(HistoryDocument::decode(&document.encode()), Ok(document));
    }

    #[test]
    fn trap_history_v2_preserves_open_operations() {
        let document = HistoryDocument {
            build_label: String::from("test"),
            config_hash: 7,
            initial: BTreeMap::new(),
            retain_open: true,
            history: History {
                operations: vec![Operation::open(
                    3,
                    OperationKind::Get { key: vec![1] },
                    Time::from_nanos(1),
                )],
            },
        };
        assert!(
            HistoryDocument::decode(&document.encode())
                .expect("decode")
                .history
                .operations[0]
                .complete
                .is_none()
        );
    }

    #[test]
    fn trap_history_rejects_invalid_hex_and_duplicate_ids() {
        assert!(matches!(
            decode_history_v1_tsv("SET\t0g\t00\t1\t2\n"),
            Err(HistoryCodecError::Invalid("v1 hex"))
        ));
        let document = HistoryDocument {
            build_label: String::from("duplicate"),
            config_hash: 0,
            initial: BTreeMap::new(),
            retain_open: true,
            history: History {
                operations: vec![
                    Operation::completed(
                        7,
                        OperationKind::Get { key: b"k".to_vec() },
                        Time::from_nanos(1),
                        Time::from_nanos(2),
                        Outcome::Value(None),
                    ),
                    Operation::completed(
                        7,
                        OperationKind::Get { key: b"k".to_vec() },
                        Time::from_nanos(3),
                        Time::from_nanos(4),
                        Outcome::Value(None),
                    ),
                ],
            },
        };
        assert_eq!(
            HistoryDocument::decode(&document.encode()),
            Err(HistoryCodecError::DuplicateId(7))
        );
    }

    #[test]
    fn trap_history_reads_legacy_v1_fixture() {
        let text = std::str::from_utf8(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/legacy/cchy-v1.bin"
        )))
        .expect("legacy CCHY fixture is text");
        let history = decode_history_v1_tsv(text).expect("legacy CCHY fixture");
        assert!(
            !history.operations.is_empty(),
            "legacy history must retain operations"
        );
        assert!(
            history
                .operations
                .iter()
                .all(|operation| operation.complete.is_some())
        );
    }

    #[test]
    fn trap_chunk_cannot_claim_an_empty_initial_state() {
        let history = History {
            operations: vec![Operation::completed(
                1,
                OperationKind::Get {
                    key: b"before-window".to_vec(),
                },
                Time::from_nanos(1),
                Time::from_nanos(2),
                Outcome::Value(Some(b"present".to_vec())),
            )],
        };
        assert_eq!(
            check_window(&history, None, CheckerConfig::default()),
            Err(HistoryCodecError::Invalid("window initial receipt"))
        );
        let initial = [(b"before-window".to_vec(), b"present".to_vec())]
            .into_iter()
            .collect();
        assert!(matches!(
            check_window(&history, Some(initial), CheckerConfig::default()),
            Ok(Verdict::Linearizable { .. })
        ));
    }

    #[test]
    fn trap_real_history_detects_planted_lost_ack() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"acknowledged".to_vec(),
                        value: b"write".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                // Harness-only fault: the write reply was observed, but the
                // final state receipt reports that its mutation vanished.
                Operation::completed(
                    2,
                    OperationKind::Get {
                        key: b"acknowledged".to_vec(),
                    },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                ),
            ],
        };
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
    }

    #[test]
    fn trap_witness_is_smaller_than_the_history() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"k".to_vec(),
                        value: b"written".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                ),
                Operation::completed(
                    3,
                    OperationKind::Set {
                        key: b"irrelevant".to_vec(),
                        value: b"value".to_vec(),
                    },
                    Time::from_nanos(5),
                    Time::from_nanos(6),
                    Outcome::Ok,
                ),
            ],
        };
        let witness = minimize_witness(&history, CheckerConfig::default(), 50).expect("witness");
        assert!(witness.operation_ids.len() < history.operations.len());
        let reduced = History {
            operations: history
                .operations
                .iter()
                .filter(|operation| witness.operation_ids.contains(&operation.id))
                .cloned()
                .collect(),
        };
        assert!(matches!(
            check(&reduced, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
    }

    #[test]
    fn trap_witness_still_fails_the_checker() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"w".to_vec(),
                        value: b"value".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"w".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                ),
            ],
        };
        let Verdict::NotLinearizable { witness, .. } = check(&history, CheckerConfig::default())
        else {
            panic!("planted history must fail");
        };
        let reduced = History {
            operations: history
                .operations
                .iter()
                .filter(|operation| witness.operation_ids.contains(&operation.id))
                .cloned()
                .collect(),
        };
        assert!(matches!(
            check_with_initial_raw(
                &reduced,
                initial_model(BTreeMap::new()),
                CheckerConfig::default(),
            ),
            Verdict::NotLinearizable { .. }
        ));
    }

    #[test]
    fn trap_witness_preserves_initial_state() {
        let document = HistoryDocument {
            build_label: String::from("window"),
            config_hash: 0,
            initial: [(b"k".to_vec(), b"present".to_vec())].into_iter().collect(),
            retain_open: true,
            history: History {
                operations: vec![Operation::completed(
                    1,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )],
            },
        };
        assert!(matches!(
            check_document(&document, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
        assert_eq!(
            minimize_document_witness(&document, CheckerConfig::default(), 10)
                .expect("initial-state witness")
                .operation_ids,
            vec![1]
        );
        assert!(
            minimize_witness(&document.history, CheckerConfig::default(), 10).is_none(),
            "an empty initial model would incorrectly accept this window"
        );
    }

    #[test]
    fn trap_witness_budget_never_returns_a_passing_subset() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"k".to_vec(),
                        value: b"written".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                ),
            ],
        };
        let witness = minimize_witness(&history, CheckerConfig::default(), 1).expect("witness");
        assert!(witness.budget_exhausted);
        let reduced = History {
            operations: history
                .operations
                .iter()
                .filter(|operation| witness.operation_ids.contains(&operation.id))
                .cloned()
                .collect(),
        };
        assert!(matches!(
            check(&reduced, CheckerConfig::default()),
            Verdict::NotLinearizable { .. }
        ));
    }

    #[test]
    fn trap_minimal_witness_is_deterministic() {
        let history = History {
            operations: (1..=2)
                .map(|id| {
                    Operation::completed(
                        id,
                        OperationKind::Incr {
                            key: b"counter".to_vec(),
                        },
                        Time::from_nanos(1),
                        Time::from_nanos(2),
                        Outcome::Integer(1),
                    )
                })
                .collect(),
        };
        let first = minimize_witness(&history, CheckerConfig::default(), 20).expect("witness");
        let second = minimize_witness(&history, CheckerConfig::default(), 20).expect("witness");
        assert_eq!(first, second);
    }

    #[test]
    fn history_document_initial_state_is_checked() {
        let document = HistoryDocument {
            build_label: String::from("window"),
            config_hash: 0,
            initial: [(b"k".to_vec(), b"before".to_vec())].into_iter().collect(),
            retain_open: true,
            history: History {
                operations: vec![Operation::completed(
                    1,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(Some(b"before".to_vec())),
                )],
            },
        };
        assert!(matches!(
            check_document(&document, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn trap_classifies_resurrection() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Del { key: b"k".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(Some(b"resurrected".to_vec())),
                ),
            ],
        };
        assert!(!check_no_resurrection(&history).is_ok());
        assert!(anomaly_classes(&history).contains(&AnomalyClass::Resurrection));
    }

    #[test]
    fn trap_resurrection_checker_flags_any_value_not_only_empty_bytes() {
        // An earlier checker only noticed a resurrected empty byte string, so
        // the loudest possible failure — a real value returning after an
        // acknowledged delete — went unreported.
        for resurrected in [
            Vec::new(),
            b"v".to_vec(),
            b"a much longer value".to_vec(),
            vec![0_u8; 64],
        ] {
            let history = History {
                operations: vec![
                    Operation::completed(
                        1,
                        OperationKind::Del { key: b"k".to_vec() },
                        Time::from_nanos(1),
                        Time::from_nanos(2),
                        Outcome::Ok,
                    ),
                    Operation::completed(
                        2,
                        OperationKind::Get { key: b"k".to_vec() },
                        Time::from_nanos(3),
                        Time::from_nanos(4),
                        Outcome::Value(Some(resurrected.clone())),
                    ),
                ],
            };
            let report = check_no_resurrection(&history);
            assert!(
                !report.is_ok(),
                "a {}-byte resurrected value went unreported",
                resurrected.len()
            );
            assert!(
                report
                    .violations
                    .iter()
                    .all(|violation| violation.name == "no_resurrection")
            );
        }

        // A absent read after the delete is the correct observation.
        let clean = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Del { key: b"k".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                ),
            ],
        };
        assert!(check_no_resurrection(&clean).is_ok());
    }

    #[test]
    fn trap_classifier_does_not_order_concurrent_operations() {
        let resurrection = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Del { key: b"k".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(Some(b"returned".to_vec())),
                ),
            ],
        };
        assert!(
            classify_anomalies(&resurrection, &BTreeMap::new())
                .iter()
                .any(|anomaly| anomaly.class == AnomalyClass::Resurrection)
        );

        let concurrent_write = History {
            operations: vec![
                resurrection.operations[0].clone(),
                Operation::completed(
                    3,
                    OperationKind::Set {
                        key: b"k".to_vec(),
                        value: b"new".to_vec(),
                    },
                    Time::from_nanos(3),
                    Time::from_nanos(6),
                    Outcome::Ok,
                ),
                resurrection.operations[1].clone(),
            ],
        };
        assert!(
            !classify_anomalies(&concurrent_write, &BTreeMap::new())
                .iter()
                .any(|anomaly| anomaly.class == AnomalyClass::Resurrection)
        );
    }

    #[test]
    fn trap_classifies_stale_read() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"k".to_vec(),
                        value: b"new".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(Some(b"old".to_vec())),
                ),
            ],
        };
        let initial = [(b"k".to_vec(), b"old".to_vec())].into_iter().collect();
        assert!(
            classify_anomalies(&history, &initial)
                .iter()
                .any(|anomaly| anomaly.class == AnomalyClass::StaleRead)
        );
    }

    fn anomaly_classes(history: &History) -> BTreeSet<AnomalyClass> {
        classify_anomalies(history, &BTreeMap::new())
            .into_iter()
            .map(|anomaly| anomaly.class)
            .collect()
    }

    #[test]
    fn trap_classifies_dirty_read() {
        let history = History {
            operations: vec![Operation::completed(
                1,
                OperationKind::Get { key: b"k".to_vec() },
                Time::from_nanos(1),
                Time::from_nanos(2),
                Outcome::Value(Some(b"never-written".to_vec())),
            )],
        };
        assert!(anomaly_classes(&history).contains(&AnomalyClass::DirtyRead));
    }

    #[test]
    fn trap_classifies_non_monotonic_incr() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Incr { key: b"n".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Integer(4),
                ),
                Operation::completed(
                    2,
                    OperationKind::Incr { key: b"n".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Integer(3),
                ),
            ],
        };
        assert!(anomaly_classes(&history).contains(&AnomalyClass::NonMonotonicIncr));
    }

    #[test]
    fn trap_classifies_duplicate_apply() {
        let mut first = Operation::completed(
            1,
            OperationKind::Incr { key: b"d".to_vec() },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Integer(1),
        );
        first.client = 7;
        first.sequence = 9;
        let mut retry = first.clone();
        retry.id = 2;
        retry.invoke = Time::from_nanos(3);
        retry.complete = Some(Time::from_nanos(4));
        let history = History {
            operations: vec![
                first,
                retry,
                Operation::completed(
                    3,
                    OperationKind::Get { key: b"d".to_vec() },
                    Time::from_nanos(5),
                    Time::from_nanos(6),
                    Outcome::Value(Some(b"2".to_vec())),
                )
                .final_probe(),
            ],
        };
        assert!(anomaly_classes(&history).contains(&AnomalyClass::DuplicateApply));
    }

    #[test]
    fn trap_classifies_acked_write_lost() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"a".to_vec(),
                        value: b"kept".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"a".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                )
                .final_probe(),
            ],
        };
        assert!(anomaly_classes(&history).contains(&AnomalyClass::AckedWriteLost));
    }

    #[test]
    fn trap_classifies_lost_update() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Cas {
                        key: b"c".to_vec(),
                        expected: Some(b"base".to_vec()),
                        value: b"left".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Cas(true),
                ),
                Operation::completed(
                    2,
                    OperationKind::Cas {
                        key: b"c".to_vec(),
                        expected: Some(b"base".to_vec()),
                        value: b"right".to_vec(),
                    },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Cas(true),
                ),
                Operation::completed(
                    3,
                    OperationKind::Get { key: b"c".to_vec() },
                    Time::from_nanos(5),
                    Time::from_nanos(6),
                    Outcome::Value(Some(b"right".to_vec())),
                )
                .final_probe(),
            ],
        };
        assert!(anomaly_classes(&history).contains(&AnomalyClass::LostUpdate));
    }

    #[test]
    fn trap_classifies_replica_divergence() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Get { key: Vec::new() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )
                .replica_probe(1, 42, 100),
                Operation::completed(
                    2,
                    OperationKind::Get { key: Vec::new() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )
                .replica_probe(2, 42, 200),
            ],
        };
        assert!(anomaly_classes(&history).contains(&AnomalyClass::ReplicaDivergence));
    }

    #[test]
    fn trap_extended_anomaly_predicates_require_explicit_evidence() {
        let non_monotonic = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Incr { key: b"n".to_vec() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Integer(4),
                ),
                Operation::completed(
                    2,
                    OperationKind::Incr { key: b"n".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Integer(3),
                ),
            ],
        };
        assert!(anomaly_classes(&non_monotonic).contains(&AnomalyClass::NonMonotonicIncr));

        let mut first = Operation::completed(
            3,
            OperationKind::Incr { key: b"d".to_vec() },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Integer(1),
        );
        first.client = 7;
        first.sequence = 9;
        let mut retry = first.clone();
        retry.id = 4;
        retry.invoke = Time::from_nanos(3);
        retry.complete = Some(Time::from_nanos(4));
        let duplicate = History {
            operations: vec![
                first,
                retry,
                Operation::completed(
                    5,
                    OperationKind::Get { key: b"d".to_vec() },
                    Time::from_nanos(5),
                    Time::from_nanos(6),
                    Outcome::Value(Some(b"2".to_vec())),
                )
                .final_probe(),
            ],
        };
        assert!(anomaly_classes(&duplicate).contains(&AnomalyClass::DuplicateApply));

        let acked_lost = History {
            operations: vec![
                Operation::completed(
                    6,
                    OperationKind::Set {
                        key: b"a".to_vec(),
                        value: b"kept".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    7,
                    OperationKind::Get { key: b"a".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                )
                .final_probe(),
            ],
        };
        assert!(anomaly_classes(&acked_lost).contains(&AnomalyClass::AckedWriteLost));

        let lost_update = History {
            operations: vec![
                Operation::completed(
                    8,
                    OperationKind::Cas {
                        key: b"c".to_vec(),
                        expected: Some(b"base".to_vec()),
                        value: b"left".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Cas(true),
                ),
                Operation::completed(
                    9,
                    OperationKind::Cas {
                        key: b"c".to_vec(),
                        expected: Some(b"base".to_vec()),
                        value: b"right".to_vec(),
                    },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Cas(true),
                ),
                Operation::completed(
                    10,
                    OperationKind::Get { key: b"c".to_vec() },
                    Time::from_nanos(5),
                    Time::from_nanos(6),
                    Outcome::Value(Some(b"right".to_vec())),
                )
                .final_probe(),
            ],
        };
        assert!(anomaly_classes(&lost_update).contains(&AnomalyClass::LostUpdate));

        let divergence = History {
            operations: vec![
                Operation::completed(
                    11,
                    OperationKind::Get { key: Vec::new() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )
                .replica_probe(1, 42, 100),
                Operation::completed(
                    12,
                    OperationKind::Get { key: Vec::new() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )
                .replica_probe(2, 42, 200),
            ],
        };
        assert!(anomaly_classes(&divergence).contains(&AnomalyClass::ReplicaDivergence));
    }

    #[test]
    fn trap_anomaly_near_misses_remain_unclassified() {
        let ordinary_probe = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                Operation::completed(
                    2,
                    OperationKind::Get { key: b"k".to_vec() },
                    Time::from_nanos(3),
                    Time::from_nanos(4),
                    Outcome::Value(None),
                ),
            ],
        };
        assert!(!anomaly_classes(&ordinary_probe).contains(&AnomalyClass::AckedWriteLost));

        let mut expiring = ordinary_probe.operations[0].clone();
        expiring.deadline = Some(Time::from_nanos(3));
        let expired_probe = History {
            operations: vec![expiring, ordinary_probe.operations[1].clone().final_probe()],
        };
        assert!(!anomaly_classes(&expired_probe).contains(&AnomalyClass::AckedWriteLost));

        let replica_near_miss = History {
            operations: vec![
                Operation::completed(
                    3,
                    OperationKind::Get { key: Vec::new() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )
                .replica_probe(1, 9, 1),
                Operation::completed(
                    4,
                    OperationKind::Get { key: Vec::new() },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Value(None),
                )
                .replica_probe(2, 10, 2),
            ],
        };
        assert!(!anomaly_classes(&replica_near_miss).contains(&AnomalyClass::ReplicaDivergence));
    }

    #[test]
    fn trap_linearizable_implies_session_guarantees() {
        let mut write = Operation::completed(
            1,
            OperationKind::Set {
                key: b"session".to_vec(),
                value: b"value".to_vec(),
            },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Ok,
        );
        write.client = 9;
        write.sequence = 1;
        let mut read = Operation::completed(
            2,
            OperationKind::Get {
                key: b"session".to_vec(),
            },
            Time::from_nanos(3),
            Time::from_nanos(4),
            Outcome::Value(Some(b"value".to_vec())),
        );
        read.client = 9;
        read.sequence = 2;
        let history = History {
            operations: vec![write, read],
        };
        let verdict = check(&history, CheckerConfig::default());
        assert!(matches!(verdict, Verdict::Linearizable { .. }));
        assert!(check_session_guarantees(&history).is_ok());
        assert!(cross_check_linearizable_sessions(&history, &verdict).is_ok());
    }

    #[test]
    fn trap_session_crosscheck_excludes_stale_reads() {
        let mut write = Operation::completed(
            1,
            OperationKind::Set {
                key: b"mixed".to_vec(),
                value: b"value".to_vec(),
            },
            Time::from_nanos(1),
            Time::from_nanos(2),
            Outcome::Ok,
        );
        write.client = 4;
        write.sequence = 1;
        let mut stale = Operation::completed(
            2,
            OperationKind::Get {
                key: b"mixed".to_vec(),
            },
            Time::from_nanos(3),
            Time::from_nanos(4),
            Outcome::Value(None),
        );
        stale.client = 4;
        stale.sequence = 2;
        stale.consistency = ConsistencyMode::StaleLocal;
        let history = History {
            operations: vec![write, stale],
        };
        let report = check_session_guarantees(&history);
        assert!(
            report
                .clients
                .iter()
                .all(|client| { !client.eligible_for_linearizability_cross_check })
        );
        assert!(
            cross_check_linearizable_sessions(&history, &Verdict::Linearizable { visited: 1 })
                .is_ok()
        );
    }

    fn stale_get(id: u64, value: Option<&[u8]>) -> Operation {
        let mut operation = Operation::completed(
            id,
            OperationKind::Get { key: b"k".to_vec() },
            Time::from_nanos(10),
            Time::from_nanos(20),
            Outcome::Value(value.map(<[u8]>::to_vec)),
        );
        operation.consistency = ConsistencyMode::StaleLocal;
        operation
    }

    #[test]
    fn trap_stale_read_is_excluded_from_the_linearizable_history() {
        let history = History {
            operations: vec![
                Operation::completed(
                    1,
                    OperationKind::Set {
                        key: b"k".to_vec(),
                        value: b"new".to_vec(),
                    },
                    Time::from_nanos(1),
                    Time::from_nanos(2),
                    Outcome::Ok,
                ),
                stale_get(2, Some(b"old")),
            ],
        };
        assert!(matches!(
            check(&history, CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ));
    }

    #[test]
    fn trap_stale_read_matches_reported_applied_index() {
        let history = History {
            operations: vec![stale_get(7, Some(b"v"))],
        };
        let mut evidence = StaleReadEvidence {
            operation_id: 7,
            applied_index: 4,
            applied_term: 2,
            traced_index: 4,
            traced_term: 2,
            read_time: Time::from_nanos(50),
            value_at_traced_index: Some(b"v".to_vec()),
            deadline: None,
        };
        assert_eq!(
            check_stale_reads(&history, &[evidence.clone()]),
            StaleReadVerdict::Valid { checked: 1 }
        );
        evidence.traced_index = 5;
        assert!(matches!(
            check_stale_reads(&history, &[evidence]),
            StaleReadVerdict::Invalid {
                operation_id: 7,
                ..
            }
        ));
        assert!(matches!(
            check_stale_reads(&history, &[]),
            StaleReadVerdict::Undecided {
                operation_id: 7,
                ..
            }
        ));
    }

    #[test]
    fn trap_stale_read_ttl_uses_reported_read_time() {
        let history = History {
            operations: vec![stale_get(7, None)],
        };
        let evidence = StaleReadEvidence {
            operation_id: 7,
            applied_index: 4,
            applied_term: 2,
            traced_index: 4,
            traced_term: 2,
            read_time: Time::from_nanos(100),
            value_at_traced_index: Some(b"expired".to_vec()),
            deadline: Some(Time::from_nanos(99)),
        };
        assert_eq!(
            check_stale_reads(&history, &[evidence]),
            StaleReadVerdict::Valid { checked: 1 }
        );
    }

    #[test]
    fn trap_stale_read_makes_no_time_bound() {
        let history = History {
            operations: vec![stale_get(7, Some(b"old"))],
        };
        let evidence = StaleReadEvidence {
            operation_id: 7,
            applied_index: 1,
            applied_term: 1,
            traced_index: 1,
            traced_term: 1,
            read_time: Time::from_nanos(9_000_000_000_000),
            value_at_traced_index: Some(b"old".to_vec()),
            deadline: None,
        };
        assert_eq!(
            check_stale_reads(&history, &[evidence]),
            StaleReadVerdict::Valid { checked: 1 }
        );
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
            check(
                &history,
                CheckerConfig {
                    max_states: 1,
                    ..CheckerConfig::default()
                },
            ),
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
            check(
                &history,
                CheckerConfig {
                    max_states: 1,
                    ..CheckerConfig::default()
                },
            ),
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
                    if let Some((next, _)) =
                        apply_operation(&model, &operation.kind, operation.deadline)
                        && search(history, next_remaining, next)
                    {
                        return true;
                    }
                } else if let Some((next, expected)) =
                    apply_operation(&model, &operation.kind, operation.deadline)
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
    fn trap_ttl_checker_allows_only_a_true_deadline_straddle() {
        let set = |id| {
            let mut operation = Operation::completed(
                id,
                OperationKind::Set {
                    key: b"ttl".to_vec(),
                    value: b"v".to_vec(),
                },
                Time::from_nanos(1),
                Time::from_nanos(2),
                Outcome::Ok,
            );
            operation.deadline = Some(Time::from_nanos(10));
            operation
        };
        let read = |id, invoke, complete, outcome| {
            Operation::completed(
                id,
                OperationKind::Get {
                    key: b"ttl".to_vec(),
                },
                Time::from_nanos(invoke),
                Time::from_nanos(complete),
                outcome,
            )
        };
        for outcome in [Outcome::Value(Some(b"v".to_vec())), Outcome::Value(None)] {
            let history = History {
                operations: vec![set(1), read(2, 9, 11, outcome)],
            };
            assert!(matches!(
                check(&history, CheckerConfig::default()),
                Verdict::Linearizable { .. }
            ));
        }
        let after_deadline = History {
            operations: vec![set(1), read(2, 11, 12, Outcome::Value(Some(b"v".to_vec())))],
        };
        assert!(matches!(
            check(&after_deadline, CheckerConfig::default()),
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
