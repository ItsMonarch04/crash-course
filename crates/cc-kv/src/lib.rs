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
pub const SESSION_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_SCAN: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvCommand {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
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
    Expire {
        key: Vec<u8>,
        ttl: Duration,
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
        !matches!(self, Self::Get { .. } | Self::Scan { .. } | Self::Ping)
    }

    #[must_use]
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Set { key, .. }
            | Self::Del { key }
            | Self::Cas { key, .. }
            | Self::Incr { key, .. }
            | Self::Expire { key, .. }
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
                return Ok(KvReply::Error(KvError::SessionExpired));
            }
            if sequence < session.last_seq {
                return Ok(KvReply::Error(KvError::StaleSequence));
            }
            if sequence == session.last_seq {
                return Ok(session.cached.clone());
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
        self.applied_index = index;
        self.applied_term = term;
        Ok(reply)
    }

    pub fn read(&self, command: KvCommand, at: Time) -> Result<KvReply, KvError> {
        match command {
            KvCommand::Get { key } => Ok(KvReply::Value(self.visible_get(&key, at))),
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
            KvCommand::Expire { key, ttl } => {
                if self.visible_get(&key, now).is_some() {
                    self.ttl.insert(key, now + ttl);
                    Ok(KvReply::Integer(1))
                } else {
                    Ok(KvReply::Integer(0))
                }
            }
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
            .filter(|(key, _)| !self.ttl.get(key).is_some_and(|deadline| *deadline <= now))
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
            last_leader_time: Time::from_nanos(0),
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
        KvCommand::Expire { key, ttl } => {
            enc.u8(5);
            enc.bytes(key);
            enc.u64(ttl.as_nanos());
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
    fn trap_sessions_in_snapshot_and_applied_index_atomicity() {
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
}
