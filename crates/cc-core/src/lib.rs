// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Deterministic, host-independent vocabulary shared by Crash Course crates."]

use std::fmt;
use std::ops::{Add, Sub};

pub type Bytes = Vec<u8>;

macro_rules! id_type {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
        pub struct $name(pub u64);

        impl $name {
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }
    };
}

id_type!(NodeId, "n");
id_type!(ClientId, "c");
id_type!(RequestSeq, "q");
id_type!(Term, "t");
id_type!(LogIndex, "i");
id_type!(IoId, "io");
id_type!(TimerId, "timer");

/// Nanoseconds from the host-defined epoch. Core code never reads a wall clock.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Time(u64);

impl Time {
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn checked_add(self, duration: Duration) -> Option<Self> {
        match self.0.checked_add(duration.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn checked_sub(self, duration: Duration) -> Option<Self> {
        match self.0.checked_sub(duration.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

/// A non-negative duration expressed in nanoseconds.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Duration(u64);

impl Duration {
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl Add<Duration> for Time {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self.checked_add(rhs)
            .expect("invariant: virtual time overflow")
    }
}

impl Sub<Time> for Time {
    type Output = Duration;

    fn sub(self, rhs: Time) -> Self::Output {
        Duration::from_nanos(
            self.0
                .checked_sub(rhs.0)
                .expect("invariant: time subtraction must be ordered"),
        )
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

/// A run seed. The hexadecimal form is the stable human-facing form.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Seed(pub u64);

impl Seed {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:016x}", self.0)
    }
}

/// A probability represented as numerator / 2^16.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct P16(u16);

impl P16 {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u16::MAX);

    #[must_use]
    pub const fn new(numerator: u16) -> Self {
        Self(numerator)
    }

    #[must_use]
    pub const fn numerator(self) -> u16 {
        self.0
    }
}

/// Integer-only delay distributions used by the simulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayDist {
    Fixed(Duration),
    Uniform {
        low: Duration,
        high: Duration,
    },
    TwoPoint {
        short: Duration,
        long: Duration,
        long_chance: P16,
    },
}

impl Default for DelayDist {
    fn default() -> Self {
        Self::Fixed(Duration::default())
    }
}

/// SplitMix64 is used only to derive stable component streams.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// Deterministic xoshiro256++ stream with domain-separated construction.
#[derive(Clone, Debug)]
pub struct Xoshiro256pp {
    state: [u64; 4],
}

impl Xoshiro256pp {
    #[must_use]
    pub fn stream(seed: Seed, domain: &'static str, index: u64) -> Self {
        let domain_hash = fnv1a(domain.as_bytes());
        let mut splitter = SplitMix64::new(seed.0 ^ domain_hash ^ index.rotate_left(17));
        let mut state = [0; 4];
        for slot in &mut state {
            *slot = splitter.next_u64();
        }
        if state == [0; 4] {
            state[0] = 1;
        }
        Self { state }
    }

    #[must_use]
    pub fn u64(&mut self) -> u64 {
        let result = self.state[0]
            .wrapping_add(self.state[3])
            .rotate_left(23)
            .wrapping_add(self.state[0]);
        let t = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    #[must_use]
    pub fn range_u64(&mut self, low: u64, high: u64) -> u64 {
        assert!(low < high, "invariant: RNG range must be non-empty");
        let span = high - low;
        low + self.u64().wrapping_rem(span)
    }

    #[must_use]
    pub fn chance(&mut self, probability: P16) -> bool {
        (self.u64() >> 48) < u64::from(probability.0)
    }

    #[must_use]
    pub fn sample_delay(&mut self, distribution: DelayDist) -> Duration {
        match distribution {
            DelayDist::Fixed(value) => value,
            DelayDist::Uniform { low, high } => {
                if low >= high {
                    low
                } else {
                    Duration::from_nanos(self.range_u64(low.0, high.0.saturating_add(1)))
                }
            }
            DelayDist::TwoPoint {
                short,
                long,
                long_chance,
            } => {
                if self.chance(long_chance) {
                    long
                } else {
                    short
                }
            }
        }
    }
}

#[must_use]
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

pub const MAX_CODEC_BYTES: usize = 4 * 1024 * 1024;

/// Error returned by total, bounds-checked decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    UnexpectedEof {
        offset: usize,
        needed: usize,
    },
    LengthTooLarge {
        offset: usize,
        length: u32,
        max: usize,
    },
    InvalidMagic {
        expected: u32,
        actual: u32,
    },
    InvalidVersion {
        expected: u16,
        actual: u16,
    },
    InvalidTag {
        offset: usize,
        tag: u8,
    },
    TrailingBytes {
        offset: usize,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { offset, needed } => {
                write!(f, "decode EOF at {offset}, need {needed} bytes")
            }
            Self::LengthTooLarge {
                offset,
                length,
                max,
            } => {
                write!(f, "decode length {length} at {offset} exceeds {max}")
            }
            Self::InvalidMagic { expected, actual } => {
                write!(f, "invalid magic {actual:#x}, expected {expected:#x}")
            }
            Self::InvalidVersion { expected, actual } => {
                write!(f, "invalid version {actual}, expected {expected}")
            }
            Self::InvalidTag { offset, tag } => write!(f, "invalid tag {tag} at {offset}"),
            Self::TrailingBytes { offset } => write!(f, "trailing bytes at {offset}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Enc {
    bytes: Vec<u8>,
}

impl Enc {
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    pub fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn bytes(&mut self, value: &[u8]) {
        assert!(
            value.len() <= MAX_CODEC_BYTES,
            "invariant: encoded value cap"
        );
        self.u32(u32::try_from(value.len()).expect("invariant: codec length fits u32"));
        self.bytes.extend_from_slice(value);
    }

    pub fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    pub fn header(&mut self, magic: u32, version: u16) {
        self.u32(magic);
        self.u16(version);
    }

    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub struct Dec<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Dec<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(DecodeError::UnexpectedEof {
                offset: self.offset,
                needed: length,
            })?;
        if end > self.bytes.len() {
            return Err(DecodeError::UnexpectedEof {
                offset: self.offset,
                needed: length,
            });
        }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("invariant: length is 2"),
        ))
    }

    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("invariant: length is 4"),
        ))
    }

    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("invariant: length is 8"),
        ))
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let length = self.u32()?;
        if usize::try_from(length).unwrap_or(usize::MAX) > MAX_CODEC_BYTES {
            return Err(DecodeError::LengthTooLarge {
                offset: self.offset.saturating_sub(4),
                length,
                max: MAX_CODEC_BYTES,
            });
        }
        Ok(self.take(length as usize)?.to_vec())
    }

    pub fn string(&mut self) -> Result<String, DecodeError> {
        String::from_utf8(self.bytes()?).map_err(|_| DecodeError::InvalidTag {
            offset: self.offset,
            tag: 0xff,
        })
    }

    pub fn header(
        &mut self,
        expected_magic: u32,
        expected_version: u16,
    ) -> Result<(), DecodeError> {
        let actual_magic = self.u32()?;
        if actual_magic != expected_magic {
            return Err(DecodeError::InvalidMagic {
                expected: expected_magic,
                actual: actual_magic,
            });
        }
        let actual_version = self.u16()?;
        if actual_version != expected_version {
            return Err(DecodeError::InvalidVersion {
                expected: expected_version,
                actual: actual_version,
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn position(&self) -> usize {
        self.offset
    }

    pub fn finish(self) -> Result<(), DecodeError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes {
                offset: self.offset,
            })
        }
    }
}

/// CRC-32C (Castagnoli), kept dependency-free for the core vocabulary crate.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum EventKind {
    NetSend = 1,
    NetRecv = 2,
    NetDrop = 3,
    IoIssue = 4,
    IoDone = 5,
    IoLost = 6,
    TimerSet = 7,
    TimerFire = 8,
    RoleChange = 9,
    VoteReq = 10,
    VoteGrant = 11,
    VoteDeny = 12,
    AppendSent = 13,
    AppendAck = 14,
    Commit = 15,
    Apply = 16,
    SnapshotStart = 17,
    SnapshotChunk = 18,
    SnapshotInstall = 19,
    ConfChange = 20,
    ClientInvoke = 21,
    ClientOk = 22,
    ClientFail = 23,
    ClientTimeout = 24,
    WalRecover = 25,
    Flush = 26,
    Compact = 27,
    Fault = 28,
    CheckerNote = 29,
}

impl EventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NetSend => "NetSend",
            Self::NetRecv => "NetRecv",
            Self::NetDrop => "NetDrop",
            Self::IoIssue => "IoIssue",
            Self::IoDone => "IoDone",
            Self::IoLost => "IoLost",
            Self::TimerSet => "TimerSet",
            Self::TimerFire => "TimerFire",
            Self::RoleChange => "RoleChange",
            Self::VoteReq => "VoteReq",
            Self::VoteGrant => "VoteGrant",
            Self::VoteDeny => "VoteDeny",
            Self::AppendSent => "AppendSent",
            Self::AppendAck => "AppendAck",
            Self::Commit => "Commit",
            Self::Apply => "Apply",
            Self::SnapshotStart => "SnapshotStart",
            Self::SnapshotChunk => "SnapshotChunk",
            Self::SnapshotInstall => "SnapshotInstall",
            Self::ConfChange => "ConfChange",
            Self::ClientInvoke => "ClientInvoke",
            Self::ClientOk => "ClientOk",
            Self::ClientFail => "ClientFail",
            Self::ClientTimeout => "ClientTimeout",
            Self::WalRecover => "WalRecover",
            Self::Flush => "Flush",
            Self::Compact => "Compact",
            Self::Fault => "Fault",
            Self::CheckerNote => "CheckerNote",
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::NetSend,
            2 => Self::NetRecv,
            3 => Self::NetDrop,
            4 => Self::IoIssue,
            5 => Self::IoDone,
            6 => Self::IoLost,
            7 => Self::TimerSet,
            8 => Self::TimerFire,
            9 => Self::RoleChange,
            10 => Self::VoteReq,
            11 => Self::VoteGrant,
            12 => Self::VoteDeny,
            13 => Self::AppendSent,
            14 => Self::AppendAck,
            15 => Self::Commit,
            16 => Self::Apply,
            17 => Self::SnapshotStart,
            18 => Self::SnapshotChunk,
            19 => Self::SnapshotInstall,
            20 => Self::ConfChange,
            21 => Self::ClientInvoke,
            22 => Self::ClientOk,
            23 => Self::ClientFail,
            24 => Self::ClientTimeout,
            25 => Self::WalRecover,
            26 => Self::Flush,
            27 => Self::Compact,
            28 => Self::Fault,
            29 => Self::CheckerNote,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub time: Time,
    pub node: Option<NodeId>,
    pub kind: EventKind,
    pub payload: Bytes,
}

impl Event {
    #[must_use]
    pub fn new(
        seq: u64,
        time: Time,
        node: Option<NodeId>,
        kind: EventKind,
        payload: Bytes,
    ) -> Self {
        Self {
            seq,
            time,
            node,
            kind,
            payload,
        }
    }

    fn encode(&self, enc: &mut Enc) {
        enc.u64(self.seq);
        enc.u64(self.time.as_nanos());
        match self.node {
            Some(node) => {
                enc.u8(1);
                enc.u64(node.0);
            }
            None => enc.u8(0),
        }
        enc.u8(self.kind as u8);
        enc.bytes(&self.payload);
    }

    fn decode(dec: &mut Dec<'_>) -> Result<Self, DecodeError> {
        let seq = dec.u64()?;
        let time = Time::from_nanos(dec.u64()?);
        let node = match dec.u8()? {
            0 => None,
            1 => Some(NodeId::new(dec.u64()?)),
            tag => {
                return Err(DecodeError::InvalidTag {
                    offset: dec.position(),
                    tag,
                });
            }
        };
        let tag = dec.u8()?;
        let kind = EventKind::from_code(tag).ok_or(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        })?;
        let payload = dec.bytes()?;
        Ok(Self {
            seq,
            time,
            node,
            kind,
            payload,
        })
    }
}

pub const TRACE_MAGIC: u32 = u32::from_le_bytes(*b"CCTR");
pub const TRACE_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trace {
    pub seed: Seed,
    pub config_hash: u32,
    pub build: String,
    pub events: Vec<Event>,
}

impl Trace {
    #[must_use]
    pub fn new(seed: Seed, config_hash: u32) -> Self {
        Self {
            seed,
            config_hash,
            build: String::from("local"),
            events: Vec::new(),
        }
    }

    pub fn push(&mut self, time: Time, node: Option<NodeId>, kind: EventKind, payload: Bytes) {
        let seq = self.events.len() as u64;
        self.events.push(Event::new(seq, time, node, kind, payload));
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(32 + self.events.len() * 24);
        enc.header(TRACE_MAGIC, TRACE_VERSION);
        enc.u64(self.seed.0);
        enc.u32(self.config_hash);
        enc.string(&self.build);
        enc.u32(u32::try_from(self.events.len()).expect("invariant: trace event count fits u32"));
        for event in &self.events {
            event.encode(&mut enc);
        }
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut dec = Dec::new(bytes);
        dec.header(TRACE_MAGIC, TRACE_VERSION)?;
        let seed = Seed::new(dec.u64()?);
        let config_hash = dec.u32()?;
        let build = dec.string()?;
        let count = dec.u32()?;
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        if count > MAX_CODEC_BYTES {
            return Err(DecodeError::LengthTooLarge {
                offset: dec.position().saturating_sub(4),
                length: count as u32,
                max: MAX_CODEC_BYTES,
            });
        }
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(Event::decode(&mut dec)?);
        }
        dec.finish()?;
        Ok(Self {
            seed,
            config_hash,
            build,
            events,
        })
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let mut json = format!(
            "{{\"trace_version\":{},\"seed\":\"{}\",\"config_hash\":{},\"build\":\"{}\",\"events\":[",
            TRACE_VERSION,
            self.seed,
            self.config_hash,
            json_escape(&self.build)
        );
        for (index, event) in self.events.iter().enumerate() {
            if index != 0 {
                json.push(',');
            }
            let node = event
                .node
                .map_or_else(|| String::from("null"), |value| value.0.to_string());
            json.push_str(&format!(
                "{{\"seq\":{},\"time_ns\":{},\"node\":{},\"kind\":\"{}\",\"payload_hex\":\"{}\"}}",
                event.seq,
                event.time.as_nanos(),
                node,
                event.kind.as_str(),
                hex(&event.payload)
            ));
        }
        json.push_str("]}");
        json
    }
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

fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        result.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    result
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Set {
        key: Bytes,
        value: Bytes,
    },
    Get {
        key: Bytes,
    },
    Del {
        key: Bytes,
    },
    Incr {
        key: Bytes,
    },
    Cas {
        key: Bytes,
        expected: Option<Bytes>,
        value: Bytes,
    },
    Expire {
        key: Bytes,
        ttl: Duration,
    },
    Ping,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    NotLeader,
    Busy,
    StaleSequence,
    SessionExpired,
    CasMismatch,
    NotNumeric,
    TooLarge,
    UnknownCommand,
    ReadOnlyDuringTransfer,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Response {
    Ok,
    Value(Option<Bytes>),
    Integer(i64),
    Error(ErrorCode),
    Redirect(NodeId),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_published_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn ids_have_stable_display() {
        assert_eq!(NodeId::new(3).to_string(), "n3");
        assert_eq!(Term::new(7).to_string(), "t7");
        assert_eq!(LogIndex::new(412).to_string(), "i412");
        assert_eq!(Seed::new(1).to_string(), "0x0000000000000001");
    }

    #[test]
    fn stream_is_domain_separated_and_repeatable() {
        let mut left = Xoshiro256pp::stream(Seed::new(9), "node", 1);
        let mut right = Xoshiro256pp::stream(Seed::new(9), "node", 1);
        assert_eq!(left.u64(), right.u64());
        assert_ne!(
            left.u64(),
            Xoshiro256pp::stream(Seed::new(9), "disk", 1).u64()
        );
    }

    #[test]
    fn trace_round_trip_is_byte_stable() {
        let mut trace = Trace::new(Seed::new(42), 7);
        trace.push(
            Time::from_nanos(2),
            Some(NodeId::new(1)),
            EventKind::Apply,
            vec![1, 2, 3],
        );
        let bytes = trace.encode();
        assert_eq!(Trace::decode(&bytes).expect("valid trace").encode(), bytes);
    }

    #[test]
    fn decoder_rejects_oversized_length() {
        let mut enc = Enc::new();
        enc.u32(0xffff_ffff);
        let error = Dec::new(&enc.finish())
            .bytes()
            .expect_err("oversized length");
        assert!(matches!(error, DecodeError::LengthTooLarge { .. }));
    }

    #[test]
    fn malformed_trace_inputs_are_total() {
        let mut trace = Trace::new(Seed::new(17), 11);
        trace.push(
            Time::from_nanos(4),
            None,
            EventKind::CheckerNote,
            vec![9, 8, 7],
        );
        let encoded = trace.encode();
        for end in 0..encoded.len() {
            let _ = Trace::decode(&encoded[..end]);
        }
        for byte in 0..=u8::MAX {
            let input = [byte; 7];
            let _ = Trace::decode(&input);
        }
    }

    #[test]
    fn time_arithmetic_is_checked() {
        let now = Time::from_nanos(10);
        assert_eq!(now + Duration::from_nanos(2), Time::from_nanos(12));
        assert_eq!(now.checked_sub(Duration::from_nanos(11)), None);
        assert_eq!(now - Time::from_nanos(3), Duration::from_nanos(7));
    }

    #[test]
    fn trace_json_is_stable_and_escaped() {
        let mut trace = Trace::new(Seed::new(2), 3);
        trace.build = String::from("build\n1");
        trace.push(Time::from_nanos(1), None, EventKind::Fault, vec![0xab]);
        assert!(trace.to_json().contains("build\\n1"));
        assert!(trace.to_json().contains("ab"));
    }
}
