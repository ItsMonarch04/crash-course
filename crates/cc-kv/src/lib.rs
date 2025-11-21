// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Replicated key/value state machine with log-time TTL and sessions."]

use std::collections::BTreeMap;
use std::fmt;

use cc_core::{ClientId, Dec, DecodeError, Duration, Enc, LogIndex, Term, Time};
use cc_store::{Checkpoint, Store, StoreConfig, StoreError};

pub const SNAPSHOT_VERSION: u16 = 1;
pub const KV_MAGIC: u32 = u32::from_le_bytes(*b"CCKV");
pub const REPLY_MAGIC: u32 = u32::from_le_bytes(*b"CCKR");
pub const SESSION_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_SCAN: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetCondition {
    Nx,
    Xx,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommand {
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
        !matches!(
            self,
            Self::Get { .. } | Self::Ttl { .. } | Self::Scan { .. } | Self::Ping
        )
    }

    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        match self {
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

pub struct Kv {
    pub store: Store,
    sessions: BTreeMap<ClientId, Session>,
    ttl: BTreeMap<Vec<u8>, Time>,
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
            applied_index: LogIndex::new(0),
            applied_term: Term::new(0),
            last_leader_time: Time::from_nanos(0),
        })
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
        let time = self.monotonic_time(leader_time);
        let reply = self
            .apply_command(command, time)
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

    fn apply_command(&mut self, command: KvCommand, now: Time) -> Result<KvReply, KvError> {
        match command {
            KvCommand::Set { key, value, ttl } => {
                self.store.put(&key, &value)?;
                if let Some(ttl) = ttl {
                    self.ttl.insert(key, now + ttl);
                } else {
                    self.ttl.remove(&key);
                }
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
                if let Some(ttl) = ttl {
                    self.ttl.insert(key, now + ttl);
                } else {
                    self.ttl.remove(&key);
                }
                Ok(KvReply::Conditional(true))
            }
            KvCommand::Del { key } => {
                self.store.delete(&key)?;
                self.ttl.remove(&key);
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
                self.ttl.remove(&key);
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
                self.ttl.remove(&key);
                Ok(KvReply::Value(previous))
            }
            KvCommand::GetDel { key } => {
                let previous = self.visible_get(&key, now);
                if previous.is_some() {
                    self.store.delete(&key)?;
                }
                self.ttl.remove(&key);
                Ok(KvReply::Value(previous))
            }
            KvCommand::Expire { key, ttl } => {
                if self.visible_get(&key, now).is_some() {
                    self.ttl.insert(key, now + ttl);
                    Ok(KvReply::Integer(1))
                } else {
                    Ok(KvReply::Integer(0))
                }
            }
            KvCommand::ExpireAt { key, at } => {
                if self.visible_get(&key, now).is_some() {
                    self.ttl.insert(key, at);
                    Ok(KvReply::Integer(1))
                } else {
                    Ok(KvReply::Integer(0))
                }
            }
            KvCommand::Ttl { key } => Ok(KvReply::Integer(self.ttl_seconds(&key, now))),
            KvCommand::Persist { key } => {
                Ok(KvReply::Integer(if self.ttl.remove(&key).is_some() {
                    1
                } else {
                    0
                }))
            }
            KvCommand::PurgeExpired { up_to } => {
                let expired: Vec<Vec<u8>> = self
                    .ttl
                    .iter()
                    .filter(|(_, deadline)| **deadline <= up_to)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in &expired {
                    self.store.delete(key)?;
                    self.ttl.remove(key);
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
        Ok(Self {
            store: Store::restore(snapshot.checkpoint, config)?,
            sessions,
            ttl: snapshot.ttl,
            applied_index: snapshot.applied_index,
            applied_term: snapshot.applied_term,
            last_leader_time: snapshot.last_leader_time,
        })
    }
}

pub fn encode_command(command: &KvCommand) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.header(KV_MAGIC, SNAPSHOT_VERSION);
    match command {
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
    dec.header(KV_MAGIC, SNAPSHOT_VERSION)?;
    let command = match dec.u8()? {
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
                offset: dec.position(),
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
    enc.header(REPLY_MAGIC, SNAPSHOT_VERSION);
    match reply {
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
    dec.header(REPLY_MAGIC, SNAPSHOT_VERSION)?;
    let reply = match dec.u8()? {
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
        7 => KvReply::Error(error_from_tag(dec.u8()?)?),
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
}
