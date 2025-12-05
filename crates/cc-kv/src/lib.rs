// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Replicated key/value state machine with log-time TTL and sessions."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{ClientId, Dec, DecodeError, Duration, Enc, LogIndex, Term, Time};
use cc_store::{
    BlockSource, Checkpoint, LogicalEntry, Store, StoreConfig, StoreError, StoreMetadataEdit,
    StoreMutation, StoreRead,
};

pub const SNAPSHOT_VERSION: u16 = 1;
/// CCKV/CCKR v3 is reserved for the one-entry transactional batch envelope.
/// Single commands deliberately retain their v1 encoding so mixed storage
/// readers continue to see the cut-time bytes until semantic negotiation is
/// enabled at the peer boundary.
pub const BATCH_VERSION: u16 = 3;
pub const KV_MAGIC: u32 = u32::from_le_bytes(*b"CCKV");
pub const REPLY_MAGIC: u32 = u32::from_le_bytes(*b"CCKR");
pub const SESSION_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_SCAN: usize = 4_096;
const MAX_BATCH_COMMANDS: usize = 65_536;

/// Limits imposed by the replicated cluster policy when applying a batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchLimits {
    pub max_commands: u32,
    pub max_bytes: u64,
    pub max_reply_bytes: u64,
    pub max_expiry_items: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetCondition {
    Nx,
    Xx,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommand {
    /// One replicated, all-or-nothing sequence of ordinary KV commands.
    /// Batches cannot contain another batch; the outer entry owns retry and
    /// apply atomicity.
    Batch {
        commands: Vec<KvCommand>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    /// A condition and its mutation are one replicated state-machine action.
    /// It exists specifically so an adapter cannot race a local read with a
    /// later replicated write when implementing SET NX/XX.
    ConditionalSet {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        condition: SetCondition,
    },
    Del {
        key: Vec<u8>,
    },
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
    },
    Incr {
        key: Vec<u8>,
        delta: i64,
    },
    Append {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    GetSet {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    GetDel {
        key: Vec<u8>,
    },
    Expire {
        key: Vec<u8>,
        ttl: Duration,
    },
    ExpireAt {
        key: Vec<u8>,
        at: Time,
    },
    Ttl {
        key: Vec<u8>,
    },
    Persist {
        key: Vec<u8>,
    },
    PurgeExpired {
        up_to: Time,
    },
    ExpireSessions {
        up_to: Time,
    },
    Get {
        key: Vec<u8>,
    },
    Scan {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        limit: usize,
    },
    Ping,
}

impl KvCommand {
    #[must_use]
    pub fn is_write(&self) -> bool {
        if matches!(self, Self::Batch { .. }) {
            return true;
        }
        !matches!(
            self,
            Self::Get { .. } | Self::Ttl { .. } | Self::Scan { .. } | Self::Ping
        )
    }

    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Batch { .. } => None,
            Self::Set { key, .. }
            | Self::ConditionalSet { key, .. }
            | Self::Del { key }
            | Self::Cas { key, .. }
            | Self::Incr { key, .. }
            | Self::Append { key, .. }
            | Self::GetSet { key, .. }
            | Self::GetDel { key }
            | Self::Expire { key, .. }
            | Self::ExpireAt { key, .. }
            | Self::Ttl { key }
            | Self::Persist { key }
            | Self::Get { key } => Some(key),
            Self::PurgeExpired { .. }
            | Self::ExpireSessions { .. }
            | Self::Scan { .. }
            | Self::Ping => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvError {
    Store(StoreError),
    Decode(DecodeError),
    StaleSequence,
    SequenceConflict,
    SessionExpired,
    NotNumeric,
    CasMismatch,
    Busy,
    TooLarge,
    InvalidInput,
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "store: {error}"),
            Self::Decode(error) => write!(f, "decode: {error}"),
            Self::StaleSequence => write!(f, "stale-seq"),
            Self::SequenceConflict => write!(f, "sequence-conflict"),
            Self::SessionExpired => write!(f, "session-expired"),
            Self::NotNumeric => write!(f, "not-numeric"),
            Self::CasMismatch => write!(f, "cas-mismatch"),
            Self::Busy => write!(f, "busy"),
            Self::TooLarge => write!(f, "too-large"),
            Self::InvalidInput => write!(f, "invalid-input"),
        }
    }
}

impl std::error::Error for KvError {}

impl From<StoreError> for KvError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<DecodeError> for KvError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvReply {
    /// Successful batch replies retain subcommand order and are encoded as a
    /// CCKR v3 frame.
    Batch(Vec<KvReply>),
    /// A failed batch publishes nothing; the zero-based index identifies the
    /// subcommand that made the complete transition abort.
    BatchError {
        failed_index: Option<u32>,
        error: KvError,
    },
    Ok,
    Value(Option<Vec<u8>>),
    Integer(i64),
    Cas(bool),
    Conditional(bool),
    Scan(Vec<(Vec<u8>, Vec<u8>)>),
    Error(KvError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Session {
    last_seq: u64,
    cached: KvReply,
    last_active: Time,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvSnapshot {
    pub checkpoint: Checkpoint,
    pub sessions: BTreeMap<ClientId, (u64, KvReply, Time)>,
    pub ttl: BTreeMap<Vec<u8>, Time>,
    pub applied_index: LogIndex,
    pub applied_term: Term,
    pub last_leader_time: Time,
}

/// Portable state-machine data used by CCSN.  It is deliberately independent
/// of the store's derived tables, WAL, and historical KV session cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalKvSnapshot {
    pub entries: Vec<LogicalKvEntry>,
    pub store_sequence: u64,
    pub applied_index: LogIndex,
    pub applied_term: Term,
    pub last_leader_time: Time,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalKvEntry {
    pub key: Vec<u8>,
    pub sequence: u64,
    pub value: Vec<u8>,
    pub deadline: Option<Time>,
}

#[derive(Clone)]
pub struct Kv {
    pub store: Store,
    sessions: BTreeMap<ClientId, Session>,
    ttl: BTreeMap<Vec<u8>, Time>,
    deadlines: BTreeSet<(Time, Vec<u8>)>,
    pub applied_index: LogIndex,
    pub applied_term: Term,
    last_leader_time: Time,
}

impl Kv {
    pub fn new(config: StoreConfig) -> Result<Self, KvError> {
        Ok(Self {
            store: Store::new(config)?,
            sessions: BTreeMap::new(),
            ttl: BTreeMap::new(),
            deadlines: BTreeSet::new(),
            applied_index: LogIndex::new(0),
            applied_term: Term::new(0),
            last_leader_time: Time::from_nanos(0),
        })
    }

    #[must_use]
    pub const fn last_leader_time(&self) -> Time {
        self.last_leader_time
    }

    /// Compute the exact derived-store changes between two tentative KV
    /// states. The caller adds the cluster-wide generic-session edits before
    /// preparing one atomic store-WAL record.
    #[must_use]
    pub fn store_delta(&self, next: &Self) -> (Vec<StoreMutation>, Vec<StoreMetadataEdit>) {
        const TTL_NAMESPACE: u8 = 1;
        const LEGACY_SESSION_NAMESPACE: u8 = 2;

        let before = self
            .store
            .logical_entries()
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<BTreeMap<_, _>>();
        let after = next
            .store
            .logical_entries()
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<BTreeMap<_, _>>();
        let mut keys = before
            .keys()
            .chain(after.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut mutations = Vec::new();
        for key in std::mem::take(&mut keys) {
            match (before.get(&key), after.get(&key)) {
                (Some(left), Some(right)) if left == right => {}
                (_, Some(value)) => mutations.push(StoreMutation::Put {
                    key,
                    value: value.clone(),
                }),
                (Some(_), None) => mutations.push(StoreMutation::Delete { key }),
                (None, None) => unreachable!("key came from map union"),
            }
        }

        let mut metadata = Vec::new();
        let ttl_keys = self
            .ttl
            .keys()
            .chain(next.ttl.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for key in ttl_keys {
            match (self.ttl.get(&key), next.ttl.get(&key)) {
                (Some(left), Some(right)) if left == right => {}
                (_, Some(deadline)) => metadata.push(StoreMetadataEdit::Upsert {
                    namespace: TTL_NAMESPACE,
                    key,
                    value: deadline.as_nanos().to_le_bytes().to_vec(),
                }),
                (Some(_), None) => metadata.push(StoreMetadataEdit::Delete {
                    namespace: TTL_NAMESPACE,
                    key,
                }),
                (None, None) => unreachable!("key came from map union"),
            }
        }

        let session_keys = self
            .sessions
            .keys()
            .chain(next.sessions.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for client in session_keys {
            match (self.sessions.get(&client), next.sessions.get(&client)) {
                (Some(left), Some(right)) if left == right => {}
                (_, Some(session)) => {
                    let mut enc = Enc::new();
                    enc.u64(session.last_seq);
                    enc.u64(session.last_active.as_nanos());
                    enc.bytes(&encode_reply(&session.cached));
                    metadata.push(StoreMetadataEdit::Upsert {
                        namespace: LEGACY_SESSION_NAMESPACE,
                        key: client.get().to_le_bytes().to_vec(),
                        value: enc.finish(),
                    });
                }
                (Some(_), None) => metadata.push(StoreMetadataEdit::Delete {
                    namespace: LEGACY_SESSION_NAMESPACE,
                    key: client.get().to_le_bytes().to_vec(),
                }),
                (None, None) => unreachable!("key came from map union"),
            }
        }
        (mutations, metadata)
    }

    pub fn apply(
        &mut self,
        index: LogIndex,
        term: Term,
        client: ClientId,
        sequence: u64,
        command: KvCommand,
        leader_time: Time,
    ) -> Result<KvReply, KvError> {
        let leader_time = self.monotonic_time(leader_time);
        if let Some(session) = self.sessions.get(&client) {
            if leader_time
                .as_nanos()
                .saturating_sub(session.last_active.as_nanos())
                > SESSION_IDLE_TTL.as_nanos()
            {
                self.sessions.remove(&client);
                let reply = KvReply::Error(KvError::SessionExpired);
                self.advance_applied(index, term);
                return Ok(reply);
            }
            if sequence < session.last_seq {
                let reply = KvReply::Error(KvError::StaleSequence);
                self.advance_applied(index, term);
                return Ok(reply);
            }
            if sequence == session.last_seq {
                let reply = session.cached.clone();
                self.advance_applied(index, term);
                return Ok(reply);
            }
        }
        let reply = self.apply_command(command, leader_time)?;
        if sequence > 0 {
            self.sessions.insert(
                client,
                Session {
                    last_seq: sequence,
                    cached: reply.clone(),
                    last_active: leader_time,
                },
            );
        }
        self.advance_applied(index, term);
        Ok(reply)
    }

    /// Command-only apply surface used by the composite state machine.  Retry
    /// ownership belongs above `Kv`; this method deliberately knows nothing
    /// about client identities or request sequences.
    pub fn apply_command_only(
        &mut self,
        index: LogIndex,
        term: Term,
        command: KvCommand,
        leader_time: Time,
    ) -> KvReply {
        self.apply_command_only_with_batch_limits(
            index,
            term,
            command,
            leader_time,
            BatchLimits {
                max_commands: u32::try_from(MAX_BATCH_COMMANDS)
                    .expect("batch command cap fits u32"),
                max_bytes: cc_core::MAX_CODEC_BYTES as u64,
                max_reply_bytes: cc_core::MAX_CODEC_BYTES as u64,
                max_expiry_items: MAX_SCAN as u32,
            },
        )
    }

    /// Apply one committed command using the replicated batch limits owned by
    /// the cluster policy.  The limits are repeated at apply time: a corrupt
    /// or stale log must not turn a policy violation into a partially applied
    /// state transition.
    pub fn apply_command_only_with_batch_limits(
        &mut self,
        index: LogIndex,
        term: Term,
        command: KvCommand,
        leader_time: Time,
        limits: BatchLimits,
    ) -> KvReply {
        let time = self.monotonic_time(leader_time);
        let reply = self
            .apply_command_with_batch_limits(
                command,
                time,
                limits.max_commands,
                limits.max_bytes,
                limits.max_reply_bytes,
                limits.max_expiry_items,
            )
            .unwrap_or_else(KvReply::Error);
        self.advance_applied(index, term);
        reply
    }

    fn advance_applied(&mut self, index: LogIndex, term: Term) {
        self.applied_index = index;
        self.applied_term = term;
    }

    /// Advance the composite-state watermark for a committed no-op or config
    /// entry.  Such entries have no KV mutation, but they are still a state
    /// machine transition and must never leave Raft ahead of the snapshot.
    pub fn mark_applied(&mut self, index: LogIndex, term: Term, leader_time: Time) {
        self.monotonic_time(leader_time);
        self.advance_applied(index, term);
    }

    pub fn read(&self, command: KvCommand, at: Time) -> Result<KvReply, KvError> {
        match command {
            KvCommand::Get { key } => Ok(KvReply::Value(self.visible_get(&key, at))),
            KvCommand::Ttl { key } => Ok(KvReply::Integer(self.ttl_seconds(&key, at))),
            KvCommand::Scan { start, end, limit } => Ok(KvReply::Scan(self.visible_scan(
                start.as_deref(),
                end.as_deref(),
                at,
                limit,
            ))),
            KvCommand::Ping => Ok(KvReply::Ok),
            _ => Err(KvError::InvalidInput),
        }
    }

    pub fn read_with_source(
        &self,
        command: KvCommand,
        at: Time,
        source: &mut dyn BlockSource,
    ) -> StoreRead<KvReply, KvError> {
        let read = match command {
            KvCommand::Get { key } => {
                let mut read = self.store.get_with_source(source, &key, None);
                if self.ttl.get(&key).is_some_and(|deadline| *deadline <= at) {
                    read.outcome = Ok(None);
                }
                StoreRead {
                    service: read.service,
                    outcome: read.outcome.map(KvReply::Value),
                }
            }
            KvCommand::Ttl { key } => {
                let read = self.store.get_with_source(source, &key, None);
                let outcome = read.outcome.map(|value| {
                    KvReply::Integer(
                        if value.is_none()
                            || self.ttl.get(&key).is_some_and(|deadline| *deadline <= at)
                        {
                            -2
                        } else {
                            self.ttl.get(&key).map_or(-1, |deadline| {
                                i64::try_from(
                                    deadline.as_nanos().saturating_sub(at.as_nanos())
                                        / Duration::from_secs(1).as_nanos(),
                                )
                                .unwrap_or(i64::MAX)
                            })
                        },
                    )
                });
                StoreRead {
                    service: read.service,
                    outcome,
                }
            }
            KvCommand::Scan { start, end, limit } => {
                let read = self.store.scan_with_source(
                    source,
                    start.as_deref(),
                    end.as_deref(),
                    None,
                    limit.min(MAX_SCAN),
                );
                StoreRead {
                    service: read.service,
                    outcome: read.outcome.map(|items| {
                        KvReply::Scan(
                            items
                                .into_iter()
                                .filter(|(key, _)| {
                                    self.ttl.get(key).is_none_or(|deadline| *deadline > at)
                                })
                                .take(limit.min(MAX_SCAN))
                                .collect(),
                        )
                    }),
                }
            }
            KvCommand::Ping => StoreRead {
                service: Duration::from_nanos(0),
                outcome: Ok(KvReply::Ok),
            },
            _ => StoreRead {
                service: Duration::from_nanos(0),
                outcome: Err(StoreError::InvalidInput("file-backed read command")),
            },
        };
        StoreRead {
            service: read.service,
            outcome: read.outcome.map_err(KvError::Store),
        }
    }

    fn apply_command(&mut self, command: KvCommand, now: Time) -> Result<KvReply, KvError> {
        self.apply_command_with_batch_limits(
            command,
            now,
            u32::try_from(MAX_BATCH_COMMANDS).expect("batch command cap fits u32"),
            cc_core::MAX_CODEC_BYTES as u64,
            cc_core::MAX_CODEC_BYTES as u64,
            MAX_SCAN as u32,
        )
    }

    fn apply_command_with_batch_limits(
        &mut self,
        command: KvCommand,
        now: Time,
        max_batch_commands: u32,
        max_batch_bytes: u64,
        max_batch_reply_bytes: u64,
        max_expiry_items: u32,
    ) -> Result<KvReply, KvError> {
        match command {
            KvCommand::Batch { commands } => self.apply_batch(
                commands,
                now,
                max_batch_commands,
                max_batch_bytes,
                max_batch_reply_bytes,
                max_expiry_items,
            ),
            KvCommand::Set { key, value, ttl } => {
                self.store.put(&key, &value)?;
                self.replace_deadline(key, ttl.map(|ttl| now + ttl));
                Ok(KvReply::Ok)
            }
            KvCommand::ConditionalSet {
                key,
                value,
                ttl,
                condition,
            } => {
                let exists = self.visible_get(&key, now).is_some();
                let matches = match condition {
                    SetCondition::Nx => !exists,
                    SetCondition::Xx => exists,
                };
                if !matches {
                    return Ok(KvReply::Conditional(false));
                }
                self.store.put(&key, &value)?;
                self.replace_deadline(key, ttl.map(|ttl| now + ttl));
                Ok(KvReply::Conditional(true))
            }
            KvCommand::Del { key } => {
                self.store.delete(&key)?;
                self.replace_deadline(key, None);
                Ok(KvReply::Integer(1))
            }
            KvCommand::Cas {
                key,
                expected,
                value,
            } => {
                let current = self.visible_get(&key, now);
                if current != expected {
                    return Ok(KvReply::Cas(false));
                }
                match value {
                    Some(value) => {
                        self.store.put(&key, &value)?;
                    }
                    None => {
                        self.store.delete(&key)?;
                    }
                }
                self.replace_deadline(key, None);
                Ok(KvReply::Cas(true))
            }
            KvCommand::Incr { key, delta } => {
                let current = self.visible_get(&key, now).unwrap_or_else(|| b"0".to_vec());
                let text = std::str::from_utf8(&current).map_err(|_| KvError::NotNumeric)?;
                let old = text.parse::<i64>().map_err(|_| KvError::NotNumeric)?;
                let value = old.checked_add(delta).ok_or(KvError::NotNumeric)?;
                self.store.put(&key, value.to_string().as_bytes())?;
                Ok(KvReply::Integer(value))
            }
            KvCommand::Append { key, value } => {
                let mut combined = self.visible_get(&key, now).unwrap_or_default();
                combined.extend_from_slice(&value);
                self.store.put(&key, &combined)?;
                Ok(KvReply::Integer(
                    i64::try_from(combined.len()).unwrap_or(i64::MAX),
                ))
            }
            KvCommand::GetSet { key, value } => {
                let previous = self.visible_get(&key, now);
                self.store.put(&key, &value)?;
                self.replace_deadline(key, None);
                Ok(KvReply::Value(previous))
            }
            KvCommand::GetDel { key } => {
                let previous = self.visible_get(&key, now);
                if previous.is_some() {
                    self.store.delete(&key)?;
                }
                self.replace_deadline(key, None);
                Ok(KvReply::Value(previous))
            }
            KvCommand::Expire { key, ttl } => {
                if self.visible_get(&key, now).is_some() {
                    self.replace_deadline(key, Some(now + ttl));
                    Ok(KvReply::Integer(1))
                } else {
                    Ok(KvReply::Integer(0))
                }
            }
            KvCommand::ExpireAt { key, at } => {
                if self.visible_get(&key, now).is_some() {
                    self.replace_deadline(key, Some(at));
                    Ok(KvReply::Integer(1))
                } else {
                    Ok(KvReply::Integer(0))
                }
            }
            KvCommand::Ttl { key } => Ok(KvReply::Integer(self.ttl_seconds(&key, now))),
            KvCommand::Persist { key } => {
                let existed = self.ttl.contains_key(&key);
                self.replace_deadline(key, None);
                Ok(KvReply::Integer(if existed { 1 } else { 0 }))
            }
            KvCommand::PurgeExpired { up_to } => {
                let expired: Vec<Vec<u8>> = self
                    .deadlines
                    .iter()
                    .take_while(|(deadline, _)| *deadline <= up_to)
                    .take(usize::try_from(max_expiry_items).unwrap_or(usize::MAX))
                    .map(|(_, key)| key.clone())
                    .collect();
                for key in &expired {
                    self.store.delete(key)?;
                    self.replace_deadline(key.clone(), None);
                }
                Ok(KvReply::Integer(
                    i64::try_from(expired.len()).unwrap_or(i64::MAX),
                ))
            }
            KvCommand::ExpireSessions { up_to } => {
                self.sessions.retain(|_, session| {
                    up_to
                        .as_nanos()
                        .saturating_sub(session.last_active.as_nanos())
                        <= SESSION_IDLE_TTL.as_nanos()
                });
                Ok(KvReply::Ok)
            }
            KvCommand::Get { key } => Ok(KvReply::Value(self.visible_get(&key, now))),
            KvCommand::Scan { start, end, limit } => Ok(KvReply::Scan(self.visible_scan(
                start.as_deref(),
                end.as_deref(),
                now,
                limit,
            ))),
            KvCommand::Ping => Ok(KvReply::Ok),
        }
    }

    fn apply_batch(
        &mut self,
        commands: Vec<KvCommand>,
        now: Time,
        max_batch_commands: u32,
        max_batch_bytes: u64,
        max_batch_reply_bytes: u64,
        max_expiry_items: u32,
    ) -> Result<KvReply, KvError> {
        validate_batch(&commands, max_batch_commands, max_batch_bytes)?;

        // `Store`, the TTL index, sessions, and the applied KV cursor are all
        // cloneable deterministic state.  Work against a tentative image, so
        // a later command failure cannot expose an earlier mutation.  The
        // outer committed entry advances the actual watermark exactly once.
        let mut tentative = self.clone();
        let mut replies = Vec::with_capacity(commands.len());
        for (index, command) in commands.into_iter().enumerate() {
            let result = tentative.apply_command_with_batch_limits(
                command,
                now,
                max_batch_commands,
                max_batch_bytes,
                max_batch_reply_bytes,
                max_expiry_items,
            );
            match result {
                Ok(reply) => replies.push(reply),
                Err(error) => {
                    return Ok(KvReply::BatchError {
                        failed_index: Some(u32::try_from(index).expect("bounded batch index")),
                        error,
                    });
                }
            }
        }
        let reply = KvReply::Batch(replies);
        if encode_reply(&reply).len() as u64 > max_batch_reply_bytes {
            return Ok(KvReply::BatchError {
                failed_index: None,
                error: KvError::TooLarge,
            });
        }
        *self = tentative;
        Ok(reply)
    }

    fn replace_deadline(&mut self, key: Vec<u8>, deadline: Option<Time>) {
        if let Some(previous) = self.ttl.remove(&key) {
            self.deadlines.remove(&(previous, key.clone()));
        }
        if let Some(deadline) = deadline {
            self.ttl.insert(key.clone(), deadline);
            self.deadlines.insert((deadline, key));
        }
    }

    #[must_use]
    pub fn first_deadline(&self) -> Option<(Time, &[u8])> {
        self.deadlines
            .iter()
            .next()
            .map(|(deadline, key)| (*deadline, key.as_slice()))
    }

    fn monotonic_time(&mut self, time: Time) -> Time {
        if time < self.last_leader_time {
            self.last_leader_time
        } else {
            self.last_leader_time = time;
            time
        }
    }

    fn visible_get(&self, key: &[u8], now: Time) -> Option<Vec<u8>> {
        if self.ttl.get(key).is_some_and(|deadline| *deadline <= now) {
            return None;
        }
        self.store.get(key, None)
    }

    fn ttl_seconds(&self, key: &[u8], now: Time) -> i64 {
        if self.visible_get(key, now).is_none() {
            return -2;
        }
        self.ttl.get(key).map_or(-1, |deadline| {
            i64::try_from(
                deadline.as_nanos().saturating_sub(now.as_nanos())
                    / Duration::from_secs(1).as_nanos(),
            )
            .unwrap_or(i64::MAX)
        })
    }

    fn visible_scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        now: Time,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .scan(start, end, None, limit.min(MAX_SCAN))
            .into_iter()
            .filter(|(key, _)| self.ttl.get(key).is_none_or(|deadline| *deadline > now))
            .collect()
    }

    pub fn snapshot(&mut self) -> Result<KvSnapshot, KvError> {
        let checkpoint = self.store.checkpoint()?;
        let sessions = self
            .sessions
            .iter()
            .map(|(client, session)| {
                (
                    *client,
                    (
                        session.last_seq,
                        session.cached.clone(),
                        session.last_active,
                    ),
                )
            })
            .collect();
        Ok(KvSnapshot {
            checkpoint,
            sessions,
            ttl: self.ttl.clone(),
            applied_index: self.applied_index,
            applied_term: self.applied_term,
            last_leader_time: self.last_leader_time,
        })
    }

    /// Capture only canonical live key/value state.  Generic retry sessions
    /// belong to `cc-cluster::SessionTable`; the historical direct-KV session
    /// cache is intentionally not part of replicated node snapshots.
    #[must_use]
    pub fn logical_snapshot(&self, at: Time) -> LogicalKvSnapshot {
        let entries = self
            .store
            .logical_entries()
            .into_iter()
            .filter_map(|entry| {
                let deadline = self.ttl.get(&entry.key).copied();
                (deadline.is_none_or(|value| value > at)).then_some(LogicalKvEntry {
                    key: entry.key,
                    sequence: entry.sequence,
                    value: entry.value,
                    deadline,
                })
            })
            .collect();
        LogicalKvSnapshot {
            entries,
            store_sequence: self.store.last_sequence(),
            applied_index: self.applied_index,
            applied_term: self.applied_term,
            last_leader_time: self.last_leader_time.max(at),
        }
    }

    pub fn restore_logical(
        snapshot: LogicalKvSnapshot,
        config: StoreConfig,
    ) -> Result<Self, KvError> {
        let mut ttl = BTreeMap::new();
        let entries = snapshot
            .entries
            .into_iter()
            .map(|entry| {
                if let Some(deadline) = entry.deadline
                    && (deadline <= snapshot.last_leader_time
                        || ttl.insert(entry.key.clone(), deadline).is_some())
                {
                    return Err(KvError::InvalidInput);
                }
                Ok(LogicalEntry {
                    key: entry.key,
                    sequence: entry.sequence,
                    value: entry.value,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let store = if snapshot.applied_index.get() == 0 {
            Store::from_logical(config, snapshot.store_sequence, entries)?
        } else {
            Store::from_logical_at(
                config,
                snapshot.store_sequence,
                entries,
                cc_store::StoreWatermark {
                    index: snapshot.applied_index,
                    term: snapshot.applied_term,
                    last_leader_time: snapshot.last_leader_time,
                },
            )?
        };
        Ok(Self {
            store,
            sessions: BTreeMap::new(),
            deadlines: ttl
                .iter()
                .map(|(key, deadline)| (*deadline, key.clone()))
                .collect(),
            ttl,
            applied_index: snapshot.applied_index,
            applied_term: snapshot.applied_term,
            last_leader_time: snapshot.last_leader_time,
        })
    }

    pub fn restore(snapshot: KvSnapshot, config: StoreConfig) -> Result<Self, KvError> {
        let sessions = snapshot
            .sessions
            .into_iter()
            .map(|(client, (last_seq, cached, last_active))| {
                (
                    client,
                    Session {
                        last_seq,
                        cached,
                        last_active,
                    },
                )
            })
            .collect();
        let ttl = snapshot.ttl;
        let deadlines = ttl
            .iter()
            .map(|(key, deadline)| (*deadline, key.clone()))
            .collect();
        Ok(Self {
            store: Store::restore(snapshot.checkpoint, config)?,
            sessions,
            ttl,
            deadlines,
            applied_index: snapshot.applied_index,
            applied_term: snapshot.applied_term,
            last_leader_time: snapshot.last_leader_time,
        })
    }
}

pub fn encode_command(command: &KvCommand) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.header(
        KV_MAGIC,
        if matches!(command, KvCommand::Batch { .. }) {
            BATCH_VERSION
        } else {
            SNAPSHOT_VERSION
        },
    );
    match command {
        KvCommand::Batch { commands } => {
            enc.u8(18);
            enc.u32(u32::try_from(commands.len()).expect("batch count fits u32"));
            for command in commands {
                enc.bytes(&encode_command(command));
            }
        }
        KvCommand::Set { key, value, ttl } => {
            enc.u8(1);
            enc.bytes(key);
            enc.bytes(value);
            encode_optional_duration(&mut enc, *ttl);
        }
        KvCommand::ConditionalSet {
            key,
            value,
            ttl,
            condition,
        } => {
            enc.u8(17);
            enc.bytes(key);
            enc.bytes(value);
            encode_optional_duration(&mut enc, *ttl);
            enc.u8(match condition {
                SetCondition::Nx => 1,
                SetCondition::Xx => 2,
            });
        }
        KvCommand::Del { key } => {
            enc.u8(2);
            enc.bytes(key);
        }
        KvCommand::Cas {
            key,
            expected,
            value,
        } => {
            enc.u8(3);
            enc.bytes(key);
            encode_optional_bytes(&mut enc, expected);
            encode_optional_bytes(&mut enc, value);
        }
        KvCommand::Incr { key, delta } => {
            enc.u8(4);
            enc.bytes(key);
            enc.u64(*delta as u64);
        }
        KvCommand::Append { key, value } => {
            enc.u8(12);
            enc.bytes(key);
            enc.bytes(value);
        }
        KvCommand::GetSet { key, value } => {
            enc.u8(13);
            enc.bytes(key);
            enc.bytes(value);
        }
        KvCommand::GetDel { key } => {
            enc.u8(14);
            enc.bytes(key);
        }
        KvCommand::Expire { key, ttl } => {
            enc.u8(5);
            enc.bytes(key);
            enc.u64(ttl.as_nanos());
        }
        KvCommand::ExpireAt { key, at } => {
            enc.u8(15);
            enc.bytes(key);
            enc.u64(at.as_nanos());
        }
        KvCommand::Ttl { key } => {
            enc.u8(16);
            enc.bytes(key);
        }
        KvCommand::Persist { key } => {
            enc.u8(6);
            enc.bytes(key);
        }
        KvCommand::PurgeExpired { up_to } => {
            enc.u8(7);
            enc.u64(up_to.as_nanos());
        }
        KvCommand::ExpireSessions { up_to } => {
            enc.u8(8);
            enc.u64(up_to.as_nanos());
        }
        KvCommand::Get { key } => {
            enc.u8(9);
            enc.bytes(key);
        }
        KvCommand::Scan { start, end, limit } => {
            enc.u8(10);
            encode_optional_bytes(&mut enc, start);
            encode_optional_bytes(&mut enc, end);
            enc.u32(u32::try_from(*limit).unwrap_or(u32::MAX));
        }
        KvCommand::Ping => enc.u8(11),
    }
    enc.finish()
}

pub fn decode_command(bytes: &[u8]) -> Result<KvCommand, KvError> {
    let mut dec = Dec::new(bytes);
    let version = decode_version(&mut dec, KV_MAGIC)?;
    let tag = dec.u8()?;
    if (version == SNAPSHOT_VERSION && tag == 18) || (version == BATCH_VERSION && tag != 18) {
        return Err(KvError::Decode(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        }));
    }
    let command = match tag {
        1 => KvCommand::Set {
            key: dec.bytes()?,
            value: dec.bytes()?,
            ttl: decode_optional_duration(&mut dec)?,
        },
        2 => KvCommand::Del { key: dec.bytes()? },
        17 => {
            let key = dec.bytes()?;
            let value = dec.bytes()?;
            let ttl = decode_optional_duration(&mut dec)?;
            let condition = match dec.u8()? {
                1 => SetCondition::Nx,
                2 => SetCondition::Xx,
                tag => {
                    return Err(KvError::Decode(DecodeError::InvalidTag {
                        offset: dec.position().saturating_sub(1),
                        tag,
                    }));
                }
            };
            KvCommand::ConditionalSet {
                key,
                value,
                ttl,
                condition,
            }
        }
        18 if version == BATCH_VERSION => {
            let count = dec.u32()?;
            if count == 0 {
                return Err(KvError::InvalidInput);
            }
            let max_by_remaining = dec.remaining() / 4;
            if usize::try_from(count).unwrap_or(usize::MAX) > MAX_BATCH_COMMANDS
                || usize::try_from(count).unwrap_or(usize::MAX) > max_by_remaining
            {
                return Err(KvError::Decode(DecodeError::LengthTooLarge {
                    offset: dec.position().saturating_sub(4),
                    length: count,
                    max: MAX_BATCH_COMMANDS.min(max_by_remaining),
                }));
            }
            let mut commands = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let child = decode_command(&dec.bytes()?)?;
                if matches!(child, KvCommand::Batch { .. }) {
                    return Err(KvError::InvalidInput);
                }
                commands.push(child);
            }
            KvCommand::Batch { commands }
        }
        3 => KvCommand::Cas {
            key: dec.bytes()?,
            expected: decode_optional_bytes(&mut dec)?,
            value: decode_optional_bytes(&mut dec)?,
        },
        4 => KvCommand::Incr {
            key: dec.bytes()?,
            delta: dec.u64()? as i64,
        },
        5 => KvCommand::Expire {
            key: dec.bytes()?,
            ttl: Duration::from_nanos(dec.u64()?),
        },
        6 => KvCommand::Persist { key: dec.bytes()? },
        7 => KvCommand::PurgeExpired {
            up_to: Time::from_nanos(dec.u64()?),
        },
        8 => KvCommand::ExpireSessions {
            up_to: Time::from_nanos(dec.u64()?),
        },
        9 => KvCommand::Get { key: dec.bytes()? },
        10 => KvCommand::Scan {
            start: decode_optional_bytes(&mut dec)?,
            end: decode_optional_bytes(&mut dec)?,
            limit: usize::try_from(dec.u32()?).unwrap_or(usize::MAX),
        },
        11 => KvCommand::Ping,
        12 => KvCommand::Append {
            key: dec.bytes()?,
            value: dec.bytes()?,
        },
        13 => KvCommand::GetSet {
            key: dec.bytes()?,
            value: dec.bytes()?,
        },
        14 => KvCommand::GetDel { key: dec.bytes()? },
        15 => KvCommand::ExpireAt {
            key: dec.bytes()?,
            at: Time::from_nanos(dec.u64()?),
        },
        16 => KvCommand::Ttl { key: dec.bytes()? },
        tag => {
            return Err(KvError::Decode(DecodeError::InvalidTag {
                offset: dec.position().saturating_sub(1),
                tag,
            }));
        }
    };
    dec.finish()?;
    Ok(command)
}

/// Stable reply bytes used by the cluster-wide session cache. Infrastructure
/// errors are intentionally not encodable: they fail the node instead of
/// becoming a successful deterministic response.
#[must_use]
pub fn encode_reply(reply: &KvReply) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.header(
        REPLY_MAGIC,
        if matches!(reply, KvReply::Batch(_) | KvReply::BatchError { .. }) {
            BATCH_VERSION
        } else {
            SNAPSHOT_VERSION
        },
    );
    match reply {
        KvReply::Batch(replies) => {
            enc.u8(7);
            enc.u8(1);
            enc.u32(u32::try_from(replies.len()).expect("batch reply count fits u32"));
            for reply in replies {
                enc.bytes(&encode_reply(reply));
            }
        }
        KvReply::BatchError {
            failed_index,
            error,
        } => {
            enc.u8(7);
            enc.u8(0);
            match failed_index {
                Some(index) => {
                    enc.u8(1);
                    enc.u32(*index);
                }
                None => {
                    enc.u8(0);
                    enc.u32(0);
                }
            }
            enc.bytes(&encode_reply(&KvReply::Error(error.clone())));
        }
        KvReply::Ok => enc.u8(1),
        KvReply::Value(value) => {
            enc.u8(2);
            encode_optional_bytes(&mut enc, value);
        }
        KvReply::Integer(value) => {
            enc.u8(3);
            enc.u64(*value as u64);
        }
        KvReply::Cas(value) => {
            enc.u8(4);
            enc.u8(u8::from(*value));
        }
        KvReply::Conditional(value) => {
            enc.u8(5);
            enc.u8(u8::from(*value));
        }
        KvReply::Scan(items) => {
            enc.u8(6);
            enc.u32(u32::try_from(items.len()).expect("scan count fits"));
            for (key, value) in items {
                enc.bytes(key);
                enc.bytes(value);
            }
        }
        KvReply::Error(error) => {
            enc.u8(7);
            enc.u8(error_tag(error));
        }
    }
    let mut bytes = enc.finish();
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    let crc = cc_core::crc32c_zeroed_tail(&bytes);
    let checksum_start = bytes.len() - 4;
    bytes[checksum_start..].copy_from_slice(&crc.to_le_bytes());
    bytes
}

pub fn decode_reply(bytes: &[u8]) -> Result<KvReply, KvError> {
    if bytes.len() < 4 {
        return Err(KvError::InvalidInput);
    }
    let body_len = bytes.len() - 4;
    if cc_core::crc32c_zeroed_tail(bytes)
        != u32::from_le_bytes(bytes[body_len..].try_into().expect("reply CRC"))
    {
        return Err(KvError::InvalidInput);
    }
    let mut dec = Dec::new(&bytes[..body_len]);
    let version = decode_version(&mut dec, REPLY_MAGIC)?;
    let tag = dec.u8()?;
    if version == BATCH_VERSION && tag != 7 {
        return Err(KvError::Decode(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        }));
    }
    let reply = match tag {
        1 => KvReply::Ok,
        2 => KvReply::Value(decode_optional_bytes(&mut dec)?),
        3 => KvReply::Integer(dec.u64()? as i64),
        4 => KvReply::Cas(decode_bool(&mut dec)?),
        5 => KvReply::Conditional(decode_bool(&mut dec)?),
        6 => {
            let count = dec.u32()?;
            let max_by_remaining = dec.remaining() / 8;
            if count > MAX_SCAN as u32 || count as usize > max_by_remaining {
                return Err(KvError::Decode(DecodeError::LengthTooLarge {
                    offset: dec.position().saturating_sub(4),
                    length: count,
                    max: MAX_SCAN.min(max_by_remaining),
                }));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push((dec.bytes()?, dec.bytes()?));
            }
            KvReply::Scan(items)
        }
        7 if version == SNAPSHOT_VERSION => KvReply::Error(error_from_tag(dec.u8()?)?),
        7 if version == BATCH_VERSION => decode_batch_reply(&mut dec)?,
        tag => {
            return Err(KvError::Decode(DecodeError::InvalidTag {
                offset: dec.position().saturating_sub(1),
                tag,
            }));
        }
    };
    dec.finish()?;
    Ok(reply)
}

fn decode_batch_reply(dec: &mut Dec<'_>) -> Result<KvReply, KvError> {
    match dec.u8()? {
        1 => {
            let count = dec.u32()?;
            let max_by_remaining = dec.remaining() / 4;
            if count == 0
                || usize::try_from(count).unwrap_or(usize::MAX) > MAX_BATCH_COMMANDS
                || usize::try_from(count).unwrap_or(usize::MAX) > max_by_remaining
            {
                return Err(KvError::Decode(DecodeError::LengthTooLarge {
                    offset: dec.position().saturating_sub(4),
                    length: count,
                    max: MAX_BATCH_COMMANDS.min(max_by_remaining),
                }));
            }
            let mut replies = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let reply = decode_reply(&dec.bytes()?)?;
                if matches!(reply, KvReply::Batch(_) | KvReply::BatchError { .. }) {
                    return Err(KvError::InvalidInput);
                }
                replies.push(reply);
            }
            Ok(KvReply::Batch(replies))
        }
        0 => {
            let has_index = decode_bool(dec)?;
            let index = dec.u32()?;
            if (!has_index && index != 0)
                || (has_index && usize::try_from(index).unwrap_or(usize::MAX) >= MAX_BATCH_COMMANDS)
            {
                return Err(KvError::InvalidInput);
            }
            let error = match decode_reply(&dec.bytes()?)? {
                KvReply::Error(error) => error,
                _ => return Err(KvError::InvalidInput),
            };
            Ok(KvReply::BatchError {
                failed_index: has_index.then_some(index),
                error,
            })
        }
        _ => Err(KvError::InvalidInput),
    }
}

fn decode_version(dec: &mut Dec<'_>, expected_magic: u32) -> Result<u16, KvError> {
    let actual_magic = dec.u32()?;
    if actual_magic != expected_magic {
        return Err(KvError::Decode(DecodeError::InvalidMagic {
            expected: expected_magic,
            actual: actual_magic,
        }));
    }
    let version = dec.u16()?;
    if version != SNAPSHOT_VERSION && version != BATCH_VERSION {
        return Err(KvError::Decode(DecodeError::InvalidVersion {
            expected: BATCH_VERSION,
            actual: version,
        }));
    }
    Ok(version)
}

/// Validate the part of a batch that must be known before replication. The
/// cluster calls this with immutable policy values both at proposal and when
/// replaying an entry; a corrupt log cannot bypass the active policy.
pub fn validate_batch(
    commands: &[KvCommand],
    max_commands: u32,
    max_bytes: u64,
) -> Result<(), KvError> {
    if commands.is_empty() {
        return Err(KvError::InvalidInput);
    }
    if commands.len() > usize::try_from(max_commands).unwrap_or(usize::MAX) {
        return Err(KvError::TooLarge);
    }
    // CCKV header, tag, and count are part of the policy charge too. Each
    // child is framed as bytes32 and therefore pays four more bytes.
    let mut bytes = 11_u64;
    for command in commands {
        if matches!(command, KvCommand::Batch { .. }) {
            return Err(KvError::InvalidInput);
        }
        let child = u64::try_from(encode_command(command).len()).unwrap_or(u64::MAX);
        bytes = bytes
            .checked_add(4_u64.checked_add(child).ok_or(KvError::TooLarge)?)
            .ok_or(KvError::TooLarge)?;
        if bytes > max_bytes {
            return Err(KvError::TooLarge);
        }
    }
    Ok(())
}

fn decode_bool(dec: &mut Dec<'_>) -> Result<bool, KvError> {
    match dec.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        tag => Err(KvError::Decode(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        })),
    }
}

fn error_tag(error: &KvError) -> u8 {
    match error {
        KvError::StaleSequence => 1,
        KvError::SequenceConflict => 2,
        KvError::SessionExpired => 3,
        KvError::NotNumeric => 4,
        KvError::CasMismatch => 5,
        KvError::TooLarge => 6,
        KvError::InvalidInput => 7,
        KvError::Store(_) | KvError::Decode(_) => 8,
        KvError::Busy => 9,
    }
}

fn error_from_tag(tag: u8) -> Result<KvError, KvError> {
    match tag {
        1 => Ok(KvError::StaleSequence),
        2 => Ok(KvError::SequenceConflict),
        3 => Ok(KvError::SessionExpired),
        4 => Ok(KvError::NotNumeric),
        5 => Ok(KvError::CasMismatch),
        6 => Ok(KvError::TooLarge),
        7 => Ok(KvError::InvalidInput),
        9 => Ok(KvError::Busy),
        _ => Err(KvError::Decode(DecodeError::InvalidTag { offset: 0, tag })),
    }
}

fn encode_optional_bytes(enc: &mut Enc, value: &Option<Vec<u8>>) {
    match value {
        Some(value) => {
            enc.u8(1);
            enc.bytes(value);
        }
        None => enc.u8(0),
    }
}

fn decode_optional_bytes(dec: &mut Dec<'_>) -> Result<Option<Vec<u8>>, DecodeError> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(dec.bytes()?)),
        tag => Err(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        }),
    }
}

fn encode_optional_duration(enc: &mut Enc, value: Option<Duration>) {
    match value {
        Some(value) => {
            enc.u8(1);
            enc.u64(value.as_nanos());
        }
        None => enc.u8(0),
    }
}

fn decode_optional_duration(dec: &mut Dec<'_>) -> Result<Option<Duration>, DecodeError> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(Duration::from_nanos(dec.u64()?))),
        tag => Err(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trap_legacy_command_fixture_is_readable() {
        assert_eq!(
            decode_command(include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/golden/legacy/cckv-v1.bin"
            )))
            .expect("legacy CCKV fixture"),
            KvCommand::Set {
                key: b"legacy-c0-key".to_vec(),
                value: b"legacy-c0-value".to_vec(),
                ttl: None,
            }
        );
    }

    fn kv() -> Kv {
        Kv::new(StoreConfig {
            memtable_bytes: 1024,
            ..StoreConfig::default()
        })
        .expect("kv")
    }

    #[test]
    fn rmw_family_is_atomic_and_preserves_documented_ttl_rules() {
        let mut kv = kv();
        let client = ClientId::new(4);
        let now = Time::from_nanos(100_000_000_000);
        kv.apply(
            LogIndex::new(1),
            Term::new(1),
            client,
            1,
            KvCommand::Set {
                key: b"k".to_vec(),
                value: b"a".to_vec(),
                ttl: Some(Duration::from_secs(20)),
            },
            now,
        )
        .expect("set");
        assert_eq!(
            kv.apply(
                LogIndex::new(2),
                Term::new(1),
                client,
                2,
                KvCommand::Append {
                    key: b"k".to_vec(),
                    value: b"bc".to_vec()
                },
                now,
            ),
            Ok(KvReply::Integer(3))
        );
        assert_eq!(
            kv.read(KvCommand::Ttl { key: b"k".to_vec() }, now),
            Ok(KvReply::Integer(20))
        );
        assert_eq!(
            kv.apply(
                LogIndex::new(3),
                Term::new(1),
                client,
                3,
                KvCommand::GetSet {
                    key: b"k".to_vec(),
                    value: b"new".to_vec()
                },
                now,
            ),
            Ok(KvReply::Value(Some(b"abc".to_vec())))
        );
        assert_eq!(
            kv.read(KvCommand::Ttl { key: b"k".to_vec() }, now),
            Ok(KvReply::Integer(-1))
        );
        assert_eq!(
            kv.apply(
                LogIndex::new(4),
                Term::new(1),
                client,
                4,
                KvCommand::GetDel { key: b"k".to_vec() },
                now,
            ),
            Ok(KvReply::Value(Some(b"new".to_vec())))
        );
        assert_eq!(
            kv.read(KvCommand::Ttl { key: b"k".to_vec() }, now),
            Ok(KvReply::Integer(-2))
        );
    }

    #[test]
    fn extended_command_codec_round_trips() {
        let commands = [
            KvCommand::Append {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            KvCommand::GetSet {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            KvCommand::GetDel { key: b"k".to_vec() },
            KvCommand::ExpireAt {
                key: b"k".to_vec(),
                at: Time::from_nanos(7),
            },
            KvCommand::Ttl { key: b"k".to_vec() },
        ];
        for command in commands {
            assert_eq!(decode_command(&encode_command(&command)), Ok(command));
        }
    }

    #[test]
    fn command_codec_round_trip() {
        let command = KvCommand::Set {
            key: b"a".to_vec(),
            value: b"one".to_vec(),
            ttl: Some(Duration::from_secs(3)),
        };
        assert_eq!(
            decode_command(&encode_command(&command)).expect("decode"),
            command
        );
    }

    #[test]
    fn sessions_deduplicate_and_reject_stale_sequences() {
        let mut kv = kv();
        let first = kv
            .apply(
                LogIndex::new(1),
                Term::new(1),
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
        let duplicate = kv
            .apply(
                LogIndex::new(2),
                Term::new(1),
                ClientId::new(1),
                1,
                KvCommand::Set {
                    key: b"a".to_vec(),
                    value: b"two".to_vec(),
                    ttl: None,
                },
                Time::from_nanos(2),
            )
            .expect("duplicate");
        assert_eq!(first, duplicate);
        let stale = kv
            .apply(
                LogIndex::new(3),
                Term::new(1),
                ClientId::new(1),
                0,
                KvCommand::Ping,
                Time::from_nanos(3),
            )
            .expect("stale reply");
        assert_eq!(stale, KvReply::Error(KvError::StaleSequence));
    }

    #[test]
    fn trap_ttl_replica_clock_uses_leader_time_only() {
        let mut first = kv();
        let mut second = kv();
        let command = KvCommand::Set {
            key: b"a".to_vec(),
            value: b"one".to_vec(),
            ttl: Some(Duration::from_secs(10)),
        };
        first
            .apply(
                LogIndex::new(1),
                Term::new(1),
                ClientId::new(1),
                1,
                command.clone(),
                Time::from_nanos(10_000_000_000),
            )
            .expect("apply");
        second
            .apply(
                LogIndex::new(1),
                Term::new(1),
                ClientId::new(1),
                1,
                command,
                Time::from_nanos(10_000_000_000),
            )
            .expect("apply");
        assert_eq!(
            first.read(
                KvCommand::Get { key: b"a".to_vec() },
                Time::from_nanos(19_000_000_000)
            ),
            second.read(
                KvCommand::Get { key: b"a".to_vec() },
                Time::from_nanos(19_000_000_000)
            )
        );
        assert_eq!(
            first.read(
                KvCommand::Get { key: b"a".to_vec() },
                Time::from_nanos(20_000_000_000)
            ),
            second.read(
                KvCommand::Get { key: b"a".to_vec() },
                Time::from_nanos(20_000_000_000)
            )
        );
    }

    #[test]
    fn trap_sessions_in_snapshot() {
        let mut kv = kv();
        kv.apply(
            LogIndex::new(4),
            Term::new(2),
            ClientId::new(7),
            1,
            KvCommand::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
                ttl: None,
            },
            Time::from_nanos(1),
        )
        .expect("apply");
        let snapshot = kv.snapshot().expect("snapshot");
        let restored = Kv::restore(snapshot, StoreConfig::default()).expect("restore");
        assert_eq!(restored.applied_index, LogIndex::new(4));
        assert_eq!(
            restored.read(KvCommand::Get { key: b"a".to_vec() }, Time::from_nanos(1)),
            Ok(KvReply::Value(Some(b"one".to_vec())))
        );
    }

    #[test]
    fn trap_applied_index_atomicity() {
        let mut kv = kv();
        kv.apply(
            LogIndex::new(4),
            Term::new(2),
            ClientId::new(7),
            1,
            KvCommand::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
                ttl: None,
            },
            Time::from_nanos(1),
        )
        .expect("apply");
        assert_eq!(kv.applied_index, LogIndex::new(4));
        assert_eq!(kv.applied_term, Term::new(2));
    }

    #[test]
    fn trap_conditional_set_is_one_replicated_apply() {
        let mut kv = kv();
        let reply = kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::ConditionalSet {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl: None,
                condition: SetCondition::Nx,
            },
            Time::from_nanos(1),
        );
        assert_eq!(reply, KvReply::Conditional(true));
        assert_eq!(kv.store.get(b"k", None), Some(b"v".to_vec()));
        assert_eq!(kv.store.image().sequence, 1);
    }

    #[test]
    fn trap_failed_conditional_set_preserves_value_and_ttl() {
        let mut kv = kv();
        let at = Time::from_nanos(1);
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"k".to_vec(),
                value: b"old".to_vec(),
                ttl: Some(Duration::from_secs(4)),
            },
            at,
        );
        let sequence = kv.store.image().sequence;
        assert_eq!(
            kv.apply_command_only(
                LogIndex::new(2),
                Term::new(1),
                KvCommand::ConditionalSet {
                    key: b"k".to_vec(),
                    value: b"new".to_vec(),
                    ttl: None,
                    condition: SetCondition::Nx
                },
                at
            ),
            KvReply::Conditional(false)
        );
        assert_eq!(kv.store.get(b"k", None), Some(b"old".to_vec()));
        assert_eq!(kv.store.image().sequence, sequence);
        assert_eq!(
            kv.read(KvCommand::Ttl { key: b"k".to_vec() }, at),
            Ok(KvReply::Integer(4))
        );
    }

    #[test]
    fn trap_conditional_set_treats_expired_key_as_absent() {
        let mut kv = kv();
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"k".to_vec(),
                value: b"expired".to_vec(),
                ttl: Some(Duration::from_nanos(1)),
            },
            Time::from_nanos(10),
        );
        assert_eq!(
            kv.apply_command_only(
                LogIndex::new(2),
                Term::new(1),
                KvCommand::ConditionalSet {
                    key: b"k".to_vec(),
                    value: b"replacement".to_vec(),
                    ttl: None,
                    condition: SetCondition::Nx,
                },
                Time::from_nanos(12),
            ),
            KvReply::Conditional(true)
        );
        assert_eq!(kv.store.get(b"k", None), Some(b"replacement".to_vec()));
    }

    #[test]
    fn trap_cas_writes_once() {
        let mut kv = kv();
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"k".to_vec(),
                value: b"old".to_vec(),
                ttl: None,
            },
            Time::from_nanos(1),
        );
        let before = kv.store.image().sequence;
        assert_eq!(
            kv.apply_command_only(
                LogIndex::new(2),
                Term::new(1),
                KvCommand::Cas {
                    key: b"k".to_vec(),
                    expected: Some(b"old".to_vec()),
                    value: Some(b"new".to_vec()),
                },
                Time::from_nanos(2),
            ),
            KvReply::Cas(true)
        );
        assert_eq!(kv.store.image().sequence, before + 1);
        assert_eq!(kv.store.get(b"k", None), Some(b"new".to_vec()));
    }

    #[test]
    fn trap_committed_apply_never_returns_busy() {
        let config = StoreConfig {
            memtable_bytes: 1,
            ..StoreConfig::default()
        };
        let mut kv = Kv::new(config).expect("small store");
        for index in 1..=4 {
            let reply = kv.apply_command_only(
                LogIndex::new(index),
                Term::new(1),
                KvCommand::Set {
                    key: format!("k{index}").into_bytes(),
                    value: b"v".to_vec(),
                    ttl: None,
                },
                Time::from_nanos(index),
            );
            assert!(
                !matches!(reply, KvReply::Error(KvError::Store(StoreError::Busy))),
                "a frozen derived memtable must flush before a committed mutation"
            );
        }
    }

    #[test]
    fn trap_last_leader_time_survives_snapshot_and_restore() {
        let mut kv = kv();
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Ping,
            Time::from_nanos(90),
        );
        let restored =
            Kv::restore(kv.snapshot().expect("snapshot"), StoreConfig::default()).expect("restore");
        assert_eq!(restored.last_leader_time, Time::from_nanos(90));
    }

    #[test]
    fn trap_logical_snapshot_preserves_visible_sequences_and_deadlines() {
        let mut kv = kv();
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"kept".to_vec(),
                value: b"value".to_vec(),
                ttl: Some(Duration::from_secs(30)),
            },
            Time::from_nanos(10),
        );
        kv.apply_command_only(
            LogIndex::new(2),
            Term::new(1),
            KvCommand::Set {
                key: b"deleted".to_vec(),
                value: b"old".to_vec(),
                ttl: None,
            },
            Time::from_nanos(10),
        );
        kv.apply_command_only(
            LogIndex::new(3),
            Term::new(1),
            KvCommand::Del {
                key: b"deleted".to_vec(),
            },
            Time::from_nanos(10),
        );
        let snapshot = kv.logical_snapshot(Time::from_nanos(10));
        assert_eq!(snapshot.store_sequence, 3);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].sequence, 1);
        let restored = Kv::restore_logical(snapshot, StoreConfig::default()).expect("restore");
        assert_eq!(restored.store.last_sequence(), 3);
        assert_eq!(
            restored.read(
                KvCommand::Ttl {
                    key: b"kept".to_vec(),
                },
                Time::from_nanos(10),
            ),
            Ok(KvReply::Integer(30))
        );
        assert_eq!(restored.store.get(b"deleted", None), None);
    }

    #[test]
    fn trap_batch_applies_atomically_or_not_at_all() {
        let mut kv = kv();
        let at = Time::from_nanos(10);
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"stable".to_vec(),
                value: b"before".to_vec(),
                ttl: None,
            },
            at,
        );
        kv.apply_command_only(
            LogIndex::new(2),
            Term::new(1),
            KvCommand::Set {
                key: b"not-a-number".to_vec(),
                value: b"x".to_vec(),
                ttl: None,
            },
            at,
        );
        let sequence = kv.store.image().sequence;

        let reply = kv.apply_command_only(
            LogIndex::new(3),
            Term::new(1),
            KvCommand::Batch {
                commands: vec![
                    KvCommand::Set {
                        key: b"stable".to_vec(),
                        value: b"after".to_vec(),
                        ttl: None,
                    },
                    KvCommand::Incr {
                        key: b"not-a-number".to_vec(),
                        delta: 1,
                    },
                ],
            },
            at,
        );
        assert_eq!(
            reply,
            KvReply::BatchError {
                failed_index: Some(1),
                error: KvError::NotNumeric,
            }
        );
        assert_eq!(kv.store.get(b"stable", None), Some(b"before".to_vec()));
        assert_eq!(kv.store.image().sequence, sequence);
        assert_eq!(kv.applied_index, LogIndex::new(3));
    }

    #[test]
    fn trap_batch_reads_see_prior_subcommands() {
        let mut kv = kv();
        let command = KvCommand::Batch {
            commands: vec![
                KvCommand::Set {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ttl: Some(Duration::from_secs(2)),
                },
                KvCommand::Get { key: b"k".to_vec() },
                KvCommand::Ttl { key: b"k".to_vec() },
            ],
        };
        let encoded = encode_command(&command);
        assert_eq!(&encoded[4..6], &BATCH_VERSION.to_le_bytes());
        assert_eq!(decode_command(&encoded), Ok(command.clone()));
        let reply = kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            command,
            Time::from_nanos(20),
        );
        assert_eq!(
            reply,
            KvReply::Batch(vec![
                KvReply::Ok,
                KvReply::Value(Some(b"v".to_vec())),
                KvReply::Integer(2),
            ])
        );
        let reply_bytes = encode_reply(&reply);
        assert_eq!(&reply_bytes[4..6], &BATCH_VERSION.to_le_bytes());
        assert_eq!(decode_reply(&reply_bytes), Ok(reply));
    }

    #[test]
    fn trap_batch_failure_reports_the_failing_index() {
        let mut kv = kv();
        let at = Time::from_nanos(50);
        let reply = kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Batch {
                commands: vec![
                    KvCommand::Set {
                        key: b"n".to_vec(),
                        value: b"not-numeric".to_vec(),
                        ttl: None,
                    },
                    KvCommand::Get { key: b"n".to_vec() },
                    KvCommand::Incr {
                        key: b"n".to_vec(),
                        delta: 1,
                    },
                ],
            },
            at,
        );
        assert_eq!(
            reply,
            KvReply::BatchError {
                failed_index: Some(2),
                error: KvError::NotNumeric,
            }
        );
        assert_eq!(kv.store.get(b"n", None), None);
    }

    #[test]
    fn trap_batch_ttl_uses_one_timestamp() {
        let mut kv = kv();
        let at = Time::from_nanos(5_000_000_000);
        let reply = kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Batch {
                commands: vec![
                    KvCommand::Set {
                        key: b"a".to_vec(),
                        value: b"v".to_vec(),
                        ttl: Some(Duration::from_secs(3)),
                    },
                    KvCommand::Set {
                        key: b"b".to_vec(),
                        value: b"v".to_vec(),
                        ttl: Some(Duration::from_secs(3)),
                    },
                    KvCommand::Ttl { key: b"a".to_vec() },
                    KvCommand::Ttl { key: b"b".to_vec() },
                ],
            },
            at,
        );
        assert_eq!(
            reply,
            KvReply::Batch(vec![
                KvReply::Ok,
                KvReply::Ok,
                KvReply::Integer(3),
                KvReply::Integer(3),
            ])
        );
        assert_eq!(kv.ttl.get(b"a".as_slice()), kv.ttl.get(b"b".as_slice()));
    }

    #[test]
    fn golden_cckv_batch_v3() {
        let command = KvCommand::Batch {
            commands: vec![
                KvCommand::Set {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                    ttl: None,
                },
                KvCommand::Get { key: b"a".to_vec() },
            ],
        };
        let bytes = encode_command(&command);
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            hex,
            "43434b56030012020000001200000043434b5601000101000000610100000031000c00000043434b560100090100000061"
        );
        assert_eq!(decode_command(&bytes), Ok(command));
    }

    #[test]
    fn golden_cckr_batch_reply_v3() {
        let reply = KvReply::Batch(vec![KvReply::Ok, KvReply::Integer(7)]);
        let bytes = encode_reply(&reply);
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            hex,
            "43434b5203000701020000000b00000043434b520100010ca81a2c1300000043434b520100030700000000000000e990ebdbc43ba1c2"
        );
        assert_eq!(&bytes[..4], b"CCKR");
        assert_eq!(&bytes[4..6], &BATCH_VERSION.to_le_bytes());
        assert_eq!(bytes[6], 7);
        assert_eq!(decode_reply(&bytes), Ok(reply));
    }

    #[test]
    fn trap_batch_reply_flags_are_canonical() {
        let mut invalid_success = encode_reply(&KvReply::Batch(vec![KvReply::Ok]));
        invalid_success[7] = 2;
        let crc = cc_core::crc32c_zeroed_tail(&invalid_success);
        let tail = invalid_success.len() - 4;
        invalid_success[tail..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_reply(&invalid_success), Err(KvError::InvalidInput));

        let mut hidden_index = encode_reply(&KvReply::BatchError {
            failed_index: None,
            error: KvError::TooLarge,
        });
        hidden_index[9..13].copy_from_slice(&1_u32.to_le_bytes());
        let crc = cc_core::crc32c_zeroed_tail(&hidden_index);
        let tail = hidden_index.len() - 4;
        hidden_index[tail..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_reply(&hidden_index), Err(KvError::InvalidInput));
    }

    #[test]
    fn trap_nested_batch_is_rejected() {
        let nested = KvCommand::Batch {
            commands: vec![KvCommand::Batch {
                commands: vec![KvCommand::Ping],
            }],
        };
        assert_eq!(
            decode_command(&encode_command(&nested)),
            Err(KvError::InvalidInput)
        );
    }

    #[test]
    fn trap_oversized_batch_reply_aborts_before_publish() {
        let mut kv = kv();
        let reply = kv.apply_command_only_with_batch_limits(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Batch {
                commands: vec![KvCommand::Set {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ttl: None,
                }],
            },
            Time::from_nanos(1),
            BatchLimits {
                max_commands: 1,
                max_bytes: cc_core::MAX_CODEC_BYTES as u64,
                max_reply_bytes: 1,
                max_expiry_items: 1,
            },
        );
        assert_eq!(
            reply,
            KvReply::BatchError {
                failed_index: None,
                error: KvError::TooLarge,
            }
        );
        assert_eq!(kv.store.get(b"k", None), None);
    }

    #[test]
    fn golden_cckr_v1() {
        // The checked-in compatibility-base vector is the authority for CCKR
        // v1 bytes: this build must decode it and re-encode it exactly.
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let bytes = std::fs::read(root.join("tests/golden/compat-base/cckr-v1.bin"))
            .expect("read CCKR v1 golden");
        let reply = decode_reply(&bytes).expect("decode CCKR v1 golden");
        assert_eq!(
            encode_reply(&reply),
            bytes,
            "CCKR v1 encoding is not canonical"
        );
        assert!(decode_reply(&bytes[..bytes.len() - 1]).is_err());
        let mut corrupt = bytes;
        corrupt[0] ^= 0xff;
        assert!(decode_reply(&corrupt).is_err());
    }

    #[test]
    fn golden_cckr_v1_round_trips_and_rejects_corruption() {
        let reply = KvReply::Scan(vec![(vec![0], vec![0xff])]);
        let encoded = encode_reply(&reply);
        assert_eq!(decode_reply(&encoded), Ok(reply));
        let mut corrupt = encoded;
        corrupt[8] ^= 1;
        assert!(decode_reply(&corrupt).is_err());
    }

    #[test]
    fn trap_cckr_scan_count_is_bounded_by_remaining_bytes() {
        let mut encoded = encode_reply(&KvReply::Scan(vec![(b"k".to_vec(), b"v".to_vec())]));
        // CCKR header (6), tag (1), then scan count (4).
        encoded[7..11].copy_from_slice(&2_u32.to_le_bytes());
        let last = encoded.len() - 4;
        let crc = cc_core::crc32c_zeroed_tail(&encoded);
        encoded[last..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_reply(&encoded),
            Err(KvError::Decode(DecodeError::LengthTooLarge { .. }))
        ));
    }

    #[test]
    fn trap_sweep_is_bounded_per_entry() {
        let mut kv = kv();
        for (index, key) in [b"a", b"b", b"c"].into_iter().enumerate() {
            kv.apply_command_only(
                LogIndex::new(index as u64 + 1),
                Term::new(1),
                KvCommand::Set {
                    key: key.to_vec(),
                    value: b"v".to_vec(),
                    ttl: Some(Duration::from_nanos(1)),
                },
                Time::from_nanos(1),
            );
        }
        let reply = kv.apply_command_only_with_batch_limits(
            LogIndex::new(4),
            Term::new(1),
            KvCommand::PurgeExpired {
                up_to: Time::from_nanos(2),
            },
            Time::from_nanos(2),
            BatchLimits {
                max_commands: 1,
                max_bytes: cc_core::MAX_CODEC_BYTES as u64,
                max_reply_bytes: cc_core::MAX_CODEC_BYTES as u64,
                max_expiry_items: 2,
            },
        );
        assert_eq!(reply, KvReply::Integer(2));
        assert_eq!(
            kv.first_deadline(),
            Some((Time::from_nanos(2), b"c".as_slice()))
        );
    }

    #[test]
    fn trap_expired_key_is_invisible_before_sweep() {
        let mut kv = kv();
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ttl: Some(Duration::from_nanos(1)),
            },
            Time::from_nanos(10),
        );
        assert_eq!(
            kv.read(
                KvCommand::Get {
                    key: b"key".to_vec()
                },
                Time::from_nanos(11)
            ),
            Ok(KvReply::Value(None))
        );
        assert_eq!(kv.store.get(b"key", None), Some(b"value".to_vec()));
    }

    #[test]
    fn trap_deadline_index_survives_snapshot_and_restore() {
        let mut kv = kv();
        kv.apply_command_only(
            LogIndex::new(1),
            Term::new(1),
            KvCommand::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                ttl: Some(Duration::from_nanos(50)),
            },
            Time::from_nanos(10),
        );
        let snapshot = kv.logical_snapshot(Time::from_nanos(10));
        let restored = Kv::restore_logical(snapshot, StoreConfig::default()).expect("restore");
        assert_eq!(
            restored.first_deadline(),
            Some((Time::from_nanos(60), b"key".as_slice()))
        );
    }
}
