// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The value-only boundary between deterministic cores and their hosts."]

use std::fmt;

use cc_core::{
    Bytes, ClientId, ClusterPolicy, Dec, DecodeError, Enc, Event, IoId, MAX_CODEC_BYTES, NodeId,
    RequestSeq, Time, TimerId, crc32c, crc32c_zeroed_tail,
};

pub const PEER_FRAME_MAGIC: u32 = u32::from_le_bytes(*b"CCPF");
pub const PEER_FRAME_VERSION: u16 = 1;
pub const MAX_PEER_FRAME: usize = 4 * 1024 * 1024;
pub const PEER_HELLO_MAGIC: u32 = u32::from_le_bytes(*b"CCHL");
pub const PEER_HELLO_VERSION: u16 = 1;
pub const MAX_PEER_HELLO_POLICY_BYTES: usize = 1024;
pub const FEATURE_FOLLOWER_READ: u64 = 1 << 0;
pub const FEATURE_ATOMIC_BATCH: u64 = 1 << 1;
pub const KNOWN_PEER_FEATURES: u64 = FEATURE_FOLLOWER_READ | FEATURE_ATOMIC_BATCH;

/// The peer-connection preamble.  It is deliberately separate from CCPF:
/// the transport frame version, Raft message format version, and negotiated
/// semantic version are independent compatibility namespaces.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerHello {
    pub cluster_id: [u8; 16],
    pub node_id: NodeId,
    pub cluster_policy: Bytes,
    pub semantic_min: u16,
    pub semantic_max: u16,
    pub supported_features: u64,
    pub required_features: u64,
    pub max_peer_frame: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiatedPeer {
    pub semantic_version: u16,
    pub features: u64,
    pub max_peer_frame: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HelloError {
    Incomplete,
    Decode(DecodeError),
    Checksum { expected: u32, actual: u32 },
    Invalid(&'static str),
    Policy,
    ClusterMismatch,
    PolicyMismatch,
    VersionOverlap,
    RequiredFeature(u64),
}

impl fmt::Display for HelloError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => f.write_str("incomplete peer hello"),
            Self::Decode(error) => write!(f, "invalid peer hello: {error}"),
            Self::Checksum { expected, actual } => {
                write!(
                    f,
                    "peer hello checksum mismatch: {actual:#x} != {expected:#x}"
                )
            }
            Self::Invalid(reason) => write!(f, "invalid peer hello: {reason}"),
            Self::Policy => f.write_str("invalid peer hello cluster policy"),
            Self::ClusterMismatch => f.write_str("peer hello cluster id mismatch"),
            Self::PolicyMismatch => f.write_str("peer hello cluster policy mismatch"),
            Self::VersionOverlap => f.write_str("peer hello has no semantic version overlap"),
            Self::RequiredFeature(feature) => {
                write!(f, "peer hello lacks required feature bits {feature:#x}")
            }
        }
    }
}

impl std::error::Error for HelloError {}

impl From<DecodeError> for HelloError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

impl PeerHello {
    /// Canonically encode one pre-frame CCHL record.
    pub fn encode(&self) -> Result<Vec<u8>, HelloError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(62 + self.cluster_policy.len());
        bytes.extend_from_slice(&PEER_HELLO_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&PEER_HELLO_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.cluster_id);
        bytes.extend_from_slice(&self.node_id.get().to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.cluster_policy.len())
                .map_err(|_| HelloError::Invalid("policy length"))?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&self.cluster_policy);
        bytes.extend_from_slice(&self.semantic_min.to_le_bytes());
        bytes.extend_from_slice(&self.semantic_max.to_le_bytes());
        bytes.extend_from_slice(&self.supported_features.to_le_bytes());
        bytes.extend_from_slice(&self.required_features.to_le_bytes());
        bytes.extend_from_slice(&self.max_peer_frame.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c_zeroed_tail(&bytes);
        let start = bytes.len() - 4;
        bytes[start..].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    /// Decode exactly one hello and return the unconsumed stream suffix offset.
    /// This lets a TCP reader retain a legal coalesced first CCPF frame.
    pub fn decode(input: &[u8]) -> Result<(Self, usize), HelloError> {
        const PREFIX: usize = 34;
        const FIXED: usize = 62;
        if input.len() < PREFIX {
            return Err(HelloError::Incomplete);
        }
        let policy_len = u32::from_le_bytes(input[30..34].try_into().expect("hello prefix"));
        let policy_len = usize::try_from(policy_len)
            .map_err(|_| HelloError::Invalid("policy length overflow"))?;
        if policy_len > MAX_PEER_HELLO_POLICY_BYTES {
            return Err(HelloError::Invalid("policy too large"));
        }
        let total = FIXED
            .checked_add(policy_len)
            .ok_or(HelloError::Invalid("hello length overflow"))?;
        if input.len() < total {
            return Err(HelloError::Incomplete);
        }
        let bytes = &input[..total];
        let expected = u32::from_le_bytes(bytes[total - 4..].try_into().expect("hello CRC"));
        let actual = crc32c_zeroed_tail(bytes);
        if actual != expected {
            return Err(HelloError::Checksum { expected, actual });
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("hello magic"));
        if magic != PEER_HELLO_MAGIC {
            return Err(HelloError::Invalid("magic"));
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("hello version"));
        if version != PEER_HELLO_VERSION {
            return Err(HelloError::Invalid("format version"));
        }
        let mut cluster_id = [0_u8; 16];
        cluster_id.copy_from_slice(&bytes[6..22]);
        let node_id = NodeId::new(u64::from_le_bytes(
            bytes[22..30].try_into().expect("hello node"),
        ));
        let policy_end = PREFIX + policy_len;
        let hello = Self {
            cluster_id,
            node_id,
            cluster_policy: bytes[PREFIX..policy_end].to_vec(),
            semantic_min: u16::from_le_bytes(
                bytes[policy_end..policy_end + 2]
                    .try_into()
                    .expect("hello semantic min"),
            ),
            semantic_max: u16::from_le_bytes(
                bytes[policy_end + 2..policy_end + 4]
                    .try_into()
                    .expect("hello semantic max"),
            ),
            supported_features: u64::from_le_bytes(
                bytes[policy_end + 4..policy_end + 12]
                    .try_into()
                    .expect("hello features"),
            ),
            required_features: u64::from_le_bytes(
                bytes[policy_end + 12..policy_end + 20]
                    .try_into()
                    .expect("hello required features"),
            ),
            max_peer_frame: u32::from_le_bytes(
                bytes[policy_end + 20..policy_end + 24]
                    .try_into()
                    .expect("hello maximum frame"),
            ),
        };
        hello.validate()?;
        Ok((hello, total))
    }

    pub fn negotiate(&self, peer: &Self) -> Result<NegotiatedPeer, HelloError> {
        self.validate()?;
        peer.validate()?;
        if self.cluster_id != peer.cluster_id {
            return Err(HelloError::ClusterMismatch);
        }
        if self.cluster_policy != peer.cluster_policy {
            return Err(HelloError::PolicyMismatch);
        }
        if self.node_id == peer.node_id {
            return Err(HelloError::Invalid("same node id"));
        }
        let low = self.semantic_min.max(peer.semantic_min);
        let high = self.semantic_max.min(peer.semantic_max);
        if low > high {
            return Err(HelloError::VersionOverlap);
        }
        if self.required_features & !peer.supported_features != 0 {
            return Err(HelloError::RequiredFeature(
                self.required_features & !peer.supported_features,
            ));
        }
        if peer.required_features & !self.supported_features != 0 {
            return Err(HelloError::RequiredFeature(
                peer.required_features & !self.supported_features,
            ));
        }
        Ok(NegotiatedPeer {
            semantic_version: high,
            features: self.supported_features & peer.supported_features,
            max_peer_frame: self.max_peer_frame.min(peer.max_peer_frame),
        })
    }

    fn validate(&self) -> Result<(), HelloError> {
        if self.node_id.get() == 0 {
            return Err(HelloError::Invalid("zero node id"));
        }
        if self.cluster_policy.is_empty()
            || self.cluster_policy.len() > MAX_PEER_HELLO_POLICY_BYTES
            || !ClusterPolicy::decode(&self.cluster_policy)
                .map(|policy| policy.encode() == self.cluster_policy)
                .unwrap_or(false)
        {
            return Err(HelloError::Policy);
        }
        if self.semantic_min == 0 || self.semantic_min > self.semantic_max {
            return Err(HelloError::Invalid("semantic range"));
        }
        if self.required_features & !KNOWN_PEER_FEATURES != 0
            || self.supported_features & !KNOWN_PEER_FEATURES != 0
            || self.required_features & !self.supported_features != 0
        {
            return Err(HelloError::Invalid("feature bits"));
        }
        if self.max_peer_frame == 0 || self.max_peer_frame as usize > MAX_PEER_FRAME {
            return Err(HelloError::Invalid("max peer frame"));
        }
        Ok(())
    }
}

/// A versioned datagram payload. The core does not assume TCP stream semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireMsg {
    pub proto_version: u16,
    pub payload: Bytes,
}

impl WireMsg {
    #[must_use]
    pub const fn new(proto_version: u16, payload: Bytes) -> Self {
        Self {
            proto_version,
            payload,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameError {
    Incomplete,
    TooLarge(usize),
    InvalidMagic(u32),
    InvalidVersion(u16),
    Checksum { expected: u32, actual: u32 },
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => f.write_str("incomplete peer frame"),
            Self::TooLarge(length) => write!(f, "peer frame is too large: {length} bytes"),
            Self::InvalidMagic(magic) => write!(f, "invalid peer frame magic {magic:#x}"),
            Self::InvalidVersion(version) => write!(f, "invalid peer frame version {version}"),
            Self::Checksum { expected, actual } => {
                write!(
                    f,
                    "peer frame checksum mismatch: {actual:#x} != {expected:#x}"
                )
            }
        }
    }
}

impl std::error::Error for FrameError {}

/// Encode one length-delimited peer frame. The checksum covers the protocol
/// version and payload, so a peer never hands corrupted bytes to the core.
pub fn encode_peer_frame(message: &WireMsg) -> Result<Vec<u8>, FrameError> {
    if message.payload.len() > MAX_PEER_FRAME {
        return Err(FrameError::TooLarge(message.payload.len()));
    }
    let mut body = Vec::with_capacity(2 + message.payload.len());
    body.extend_from_slice(&message.proto_version.to_le_bytes());
    body.extend_from_slice(&message.payload);
    let mut frame = Vec::with_capacity(16 + message.payload.len());
    frame.extend_from_slice(&PEER_FRAME_MAGIC.to_le_bytes());
    frame.extend_from_slice(&PEER_FRAME_VERSION.to_le_bytes());
    frame.extend_from_slice(
        &(u32::try_from(body.len()).expect("peer frame length fits u32")).to_le_bytes(),
    );
    frame.extend_from_slice(&crc32c(&body).to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub fn decode_peer_frame(input: &[u8]) -> Result<(WireMsg, usize), FrameError> {
    const HEADER: usize = 14;
    if input.len() < HEADER {
        return Err(FrameError::Incomplete);
    }
    let magic = u32::from_le_bytes(input[0..4].try_into().expect("peer header"));
    if magic != PEER_FRAME_MAGIC {
        return Err(FrameError::InvalidMagic(magic));
    }
    let version = u16::from_le_bytes(input[4..6].try_into().expect("peer header"));
    if version != PEER_FRAME_VERSION {
        return Err(FrameError::InvalidVersion(version));
    }
    let body_len = u32::from_le_bytes(input[6..10].try_into().expect("peer header")) as usize;
    if !(2..=MAX_PEER_FRAME + 2).contains(&body_len) {
        return Err(FrameError::TooLarge(body_len));
    }
    let total = HEADER
        .checked_add(body_len)
        .ok_or(FrameError::TooLarge(body_len))?;
    if input.len() < total {
        return Err(FrameError::Incomplete);
    }
    let expected = u32::from_le_bytes(input[10..14].try_into().expect("peer header"));
    let body = &input[14..total];
    let actual = crc32c(body);
    if actual != expected {
        return Err(FrameError::Checksum { expected, actual });
    }
    let proto_version = u16::from_le_bytes(body[0..2].try_into().expect("peer body"));
    Ok((WireMsg::new(proto_version, body[2..].to_vec()), total))
}

/// Files are logical objects; only hosts know their filesystem paths.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum FileId {
    Wal { segment: u64 },
    Sst { file_no: u64 },
    Manifest { generation: u64 },
    Snapshot { generation: u64 },
    Meta,
    Temp { sequence: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IoError {
    Eio,
    Enospc,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    InvalidData,
    InvalidRange,
    Corrupt(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IoResult {
    Written { len: u32 },
    Read(Bytes),
    Fsynced,
    Truncated { len: u64 },
    Failed(IoError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    Send {
        to: NodeId,
        msg: WireMsg,
    },
    DiskWrite {
        file: FileId,
        at: u64,
        bytes: Bytes,
        id: IoId,
    },
    DiskFsync {
        file: FileId,
        id: IoId,
    },
    DiskRead {
        file: FileId,
        at: u64,
        len: u32,
        id: IoId,
    },
    DiskTruncate {
        file: FileId,
        to_len: u64,
        id: IoId,
    },
    DiskCreateTemp {
        file: FileId,
        id: IoId,
    },
    DiskRename {
        from: FileId,
        to: FileId,
        id: IoId,
    },
    DiskDelete {
        file: FileId,
        id: IoId,
    },
    DiskSyncDir {
        id: IoId,
    },
    SetTimer {
        id: TimerId,
        fire_at: Time,
    },
    CancelTimer {
        id: TimerId,
    },
    ClientReply {
        client: ClientId,
        req: RequestSeq,
        reply: Bytes,
    },
    Trace(Event),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Input {
    Recv {
        from: NodeId,
        msg: WireMsg,
    },
    IoDone {
        id: IoId,
        result: IoResult,
    },
    TimerFired {
        id: TimerId,
        /// Monotonically increasing host-side arm generation.  A stale
        /// wakeup never reaches the deterministic node after a re-arm.
        generation: u64,
    },
    ClientRequest {
        /// Volatile reply route allocated by the host connection.
        client: ClientId,
        req: RequestSeq,
        /// Optional durable retry identity supplied only by `CC.REQUEST`.
        /// Its client id and sequence are never derived from the volatile
        /// route above, so a restarted host cannot alias an old session.
        session: Option<(ClientId, RequestSeq)>,
        command: Bytes,
    },
    Tick,
}

/// Independent diagnostic/journal codecs for the one host boundary.  They are
/// intentionally not CCPF or CCRP: a journal records host inputs/effects,
/// while peer framing has its own compatibility namespace.
pub const INPUT_MAGIC: u32 = u32::from_le_bytes(*b"CCEI");
pub const EFFECT_MAGIC: u32 = u32::from_le_bytes(*b"CCEO");
pub const BOUNDARY_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoundaryCodecError {
    Decode(DecodeError),
    Invalid(&'static str),
    TooLarge(&'static str),
}

impl fmt::Display for BoundaryCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(f, "boundary decode: {error}"),
            Self::Invalid(reason) => write!(f, "invalid boundary value: {reason}"),
            Self::TooLarge(what) => write!(f, "oversized boundary {what}"),
        }
    }
}

impl std::error::Error for BoundaryCodecError {}
impl From<DecodeError> for BoundaryCodecError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

pub fn encode_input(input: &Input) -> Result<Bytes, BoundaryCodecError> {
    let mut enc = Enc::new();
    enc.header(INPUT_MAGIC, BOUNDARY_FORMAT_VERSION);
    match input {
        Input::Recv { from, msg } => {
            enc.u8(1);
            nonzero_node(&mut enc, *from)?;
            encode_wire(&mut enc, msg)?;
        }
        Input::IoDone { id, result } => {
            enc.u8(2);
            enc.u64(id.get());
            encode_io_result(&mut enc, result)?;
        }
        Input::TimerFired { id, generation } => {
            enc.u8(3);
            enc.u64(id.get());
            enc.u64(*generation);
        }
        Input::ClientRequest {
            client,
            req,
            session,
            command,
        } => {
            enc.u8(4);
            nonzero_client(&mut enc, *client)?;
            enc.u64(req.get());
            match session {
                None => {
                    enc.u8(0);
                    enc.u64(0);
                    enc.u64(0);
                }
                Some((session_client, sequence)) => {
                    if sequence.get() == 0 {
                        return Err(BoundaryCodecError::Invalid("zero session sequence"));
                    }
                    enc.u8(1);
                    nonzero_client(&mut enc, *session_client)?;
                    enc.u64(sequence.get());
                }
            }
            bounded_bytes(&mut enc, command, "client command")?;
        }
        Input::Tick => enc.u8(5),
    }
    finish_boundary(enc)
}

pub fn decode_input(bytes: &[u8]) -> Result<Input, BoundaryCodecError> {
    let mut dec = begin_boundary(bytes, INPUT_MAGIC)?;
    let input = match dec.u8()? {
        1 => Input::Recv {
            from: decode_node(&mut dec)?,
            msg: decode_wire(&mut dec)?,
        },
        2 => Input::IoDone {
            id: IoId::new(dec.u64()?),
            result: decode_io_result(&mut dec)?,
        },
        3 => Input::TimerFired {
            id: TimerId::new(dec.u64()?),
            generation: dec.u64()?,
        },
        4 => Input::ClientRequest {
            client: decode_client(&mut dec)?,
            req: RequestSeq::new(dec.u64()?),
            session: match dec.u8()? {
                0 => {
                    if dec.u64()? != 0 || dec.u64()? != 0 {
                        return Err(BoundaryCodecError::Invalid("noncanonical absent session"));
                    }
                    None
                }
                1 => {
                    let session_client = decode_client(&mut dec)?;
                    let sequence = RequestSeq::new(dec.u64()?);
                    if sequence.get() == 0 {
                        return Err(BoundaryCodecError::Invalid("zero session sequence"));
                    }
                    Some((session_client, sequence))
                }
                _ => return Err(BoundaryCodecError::Invalid("client session flag")),
            },
            command: dec.bytes()?,
        },
        5 => Input::Tick,
        _ => return Err(BoundaryCodecError::Invalid("input tag")),
    };
    dec.finish()?;
    Ok(input)
}

pub fn encode_effect(effect: &Effect) -> Result<Bytes, BoundaryCodecError> {
    let mut enc = Enc::new();
    enc.header(EFFECT_MAGIC, BOUNDARY_FORMAT_VERSION);
    match effect {
        Effect::Send { to, msg } => {
            enc.u8(1);
            nonzero_node(&mut enc, *to)?;
            encode_wire(&mut enc, msg)?;
        }
        Effect::DiskWrite {
            file,
            at,
            bytes,
            id,
        } => {
            enc.u8(2);
            encode_file(&mut enc, *file);
            enc.u64(*at);
            bounded_bytes(&mut enc, bytes, "disk write")?;
            enc.u64(id.get());
        }
        Effect::DiskFsync { file, id } => {
            enc.u8(3);
            encode_file(&mut enc, *file);
            enc.u64(id.get());
        }
        Effect::DiskRead { file, at, len, id } => {
            enc.u8(4);
            encode_file(&mut enc, *file);
            enc.u64(*at);
            enc.u32(*len);
            enc.u64(id.get());
        }
        Effect::DiskTruncate { file, to_len, id } => {
            enc.u8(5);
            encode_file(&mut enc, *file);
            enc.u64(*to_len);
            enc.u64(id.get());
        }
        Effect::DiskCreateTemp { file, id } => {
            enc.u8(6);
            encode_file(&mut enc, *file);
            enc.u64(id.get());
        }
        Effect::DiskRename { from, to, id } => {
            enc.u8(7);
            encode_file(&mut enc, *from);
            encode_file(&mut enc, *to);
            enc.u64(id.get());
        }
        Effect::DiskDelete { file, id } => {
            enc.u8(8);
            encode_file(&mut enc, *file);
            enc.u64(id.get());
        }
        Effect::DiskSyncDir { id } => {
            enc.u8(9);
            enc.u64(id.get());
        }
        Effect::SetTimer { id, fire_at } => {
            enc.u8(10);
            enc.u64(id.get());
            enc.u64(fire_at.as_nanos());
        }
        Effect::CancelTimer { id } => {
            enc.u8(11);
            enc.u64(id.get());
        }
        Effect::ClientReply { client, req, reply } => {
            enc.u8(12);
            nonzero_client(&mut enc, *client)?;
            enc.u64(req.get());
            bounded_bytes(&mut enc, reply, "client reply")?;
        }
        Effect::Trace(event) => {
            enc.u8(13);
            bounded_bytes(&mut enc, &event.encode_value(), "trace event")?;
        }
    }
    finish_boundary(enc)
}

pub fn decode_effect(bytes: &[u8]) -> Result<Effect, BoundaryCodecError> {
    let mut dec = begin_boundary(bytes, EFFECT_MAGIC)?;
    let effect = match dec.u8()? {
        1 => Effect::Send {
            to: decode_node(&mut dec)?,
            msg: decode_wire(&mut dec)?,
        },
        2 => Effect::DiskWrite {
            file: decode_file(&mut dec)?,
            at: dec.u64()?,
            bytes: dec.bytes()?,
            id: IoId::new(dec.u64()?),
        },
        3 => Effect::DiskFsync {
            file: decode_file(&mut dec)?,
            id: IoId::new(dec.u64()?),
        },
        4 => Effect::DiskRead {
            file: decode_file(&mut dec)?,
            at: dec.u64()?,
            len: dec.u32()?,
            id: IoId::new(dec.u64()?),
        },
        5 => Effect::DiskTruncate {
            file: decode_file(&mut dec)?,
            to_len: dec.u64()?,
            id: IoId::new(dec.u64()?),
        },
        6 => Effect::DiskCreateTemp {
            file: decode_file(&mut dec)?,
            id: IoId::new(dec.u64()?),
        },
        7 => Effect::DiskRename {
            from: decode_file(&mut dec)?,
            to: decode_file(&mut dec)?,
            id: IoId::new(dec.u64()?),
        },
        8 => Effect::DiskDelete {
            file: decode_file(&mut dec)?,
            id: IoId::new(dec.u64()?),
        },
        9 => Effect::DiskSyncDir {
            id: IoId::new(dec.u64()?),
        },
        10 => Effect::SetTimer {
            id: TimerId::new(dec.u64()?),
            fire_at: Time::from_nanos(dec.u64()?),
        },
        11 => Effect::CancelTimer {
            id: TimerId::new(dec.u64()?),
        },
        12 => Effect::ClientReply {
            client: decode_client(&mut dec)?,
            req: RequestSeq::new(dec.u64()?),
            reply: dec.bytes()?,
        },
        13 => Effect::Trace(Event::decode_value(&dec.bytes()?)?),
        _ => return Err(BoundaryCodecError::Invalid("effect tag")),
    };
    dec.finish()?;
    Ok(effect)
}

fn finish_boundary(enc: Enc) -> Result<Bytes, BoundaryCodecError> {
    let mut bytes = enc.finish();
    if bytes.len() > MAX_CODEC_BYTES {
        return Err(BoundaryCodecError::TooLarge("record"));
    }
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c_zeroed_tail(&bytes);
    let at = bytes.len() - 4;
    bytes[at..].copy_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn begin_boundary<'a>(bytes: &'a [u8], magic: u32) -> Result<Dec<'a>, BoundaryCodecError> {
    if bytes.len() < 4 {
        return Err(BoundaryCodecError::Invalid("checksum"));
    }
    let body_len = bytes.len() - 4;
    if crc32c_zeroed_tail(bytes)
        != u32::from_le_bytes(bytes[body_len..].try_into().expect("boundary CRC"))
    {
        return Err(BoundaryCodecError::Invalid("checksum"));
    }
    let mut dec = Dec::new(&bytes[..body_len]);
    dec.header(magic, BOUNDARY_FORMAT_VERSION)?;
    Ok(dec)
}

fn bounded_bytes(
    enc: &mut Enc,
    bytes: &[u8],
    what: &'static str,
) -> Result<(), BoundaryCodecError> {
    if bytes.len() > MAX_CODEC_BYTES {
        return Err(BoundaryCodecError::TooLarge(what));
    }
    enc.bytes(bytes);
    Ok(())
}

fn nonzero_node(enc: &mut Enc, node: NodeId) -> Result<(), BoundaryCodecError> {
    if node.get() == 0 {
        return Err(BoundaryCodecError::Invalid("zero node"));
    }
    enc.u64(node.get());
    Ok(())
}
fn nonzero_client(enc: &mut Enc, client: ClientId) -> Result<(), BoundaryCodecError> {
    if client.get() == 0 {
        return Err(BoundaryCodecError::Invalid("zero client"));
    }
    enc.u64(client.get());
    Ok(())
}
fn decode_node(dec: &mut Dec<'_>) -> Result<NodeId, BoundaryCodecError> {
    let node = NodeId::new(dec.u64()?);
    if node.get() == 0 {
        Err(BoundaryCodecError::Invalid("zero node"))
    } else {
        Ok(node)
    }
}
fn decode_client(dec: &mut Dec<'_>) -> Result<ClientId, BoundaryCodecError> {
    let client = ClientId::new(dec.u64()?);
    if client.get() == 0 {
        Err(BoundaryCodecError::Invalid("zero client"))
    } else {
        Ok(client)
    }
}
fn encode_wire(enc: &mut Enc, wire: &WireMsg) -> Result<(), BoundaryCodecError> {
    enc.u16(wire.proto_version);
    bounded_bytes(enc, &wire.payload, "wire message")
}
fn decode_wire(dec: &mut Dec<'_>) -> Result<WireMsg, BoundaryCodecError> {
    Ok(WireMsg::new(dec.u16()?, dec.bytes()?))
}
/// Encode a logical file identifier for another bounded diagnostic format.
/// Hosts must still map this value to a validated local path; the bytes carry
/// no ambient filesystem authority.
pub fn encode_file_id(file: FileId) -> Vec<u8> {
    let mut enc = Enc::new();
    encode_file(&mut enc, file);
    enc.finish()
}

/// Decode the exact bounded form emitted by [`encode_file_id`].
pub fn decode_file_id(bytes: &[u8]) -> Result<FileId, BoundaryCodecError> {
    let mut dec = Dec::new(bytes);
    let file = decode_file(&mut dec)?;
    dec.finish()?;
    Ok(file)
}

fn encode_file(enc: &mut Enc, file: FileId) {
    match file {
        FileId::Wal { segment } => {
            enc.u8(1);
            enc.u64(segment);
        }
        FileId::Sst { file_no } => {
            enc.u8(2);
            enc.u64(file_no);
        }
        FileId::Manifest { generation } => {
            enc.u8(3);
            enc.u64(generation);
        }
        FileId::Snapshot { generation } => {
            enc.u8(4);
            enc.u64(generation);
        }
        FileId::Meta => enc.u8(5),
        FileId::Temp { sequence } => {
            enc.u8(6);
            enc.u64(sequence);
        }
    }
}
fn decode_file(dec: &mut Dec<'_>) -> Result<FileId, BoundaryCodecError> {
    Ok(match dec.u8()? {
        1 => FileId::Wal {
            segment: dec.u64()?,
        },
        2 => FileId::Sst {
            file_no: dec.u64()?,
        },
        3 => FileId::Manifest {
            generation: dec.u64()?,
        },
        4 => FileId::Snapshot {
            generation: dec.u64()?,
        },
        5 => FileId::Meta,
        6 => FileId::Temp {
            sequence: dec.u64()?,
        },
        _ => return Err(BoundaryCodecError::Invalid("file id")),
    })
}
fn encode_io_error(enc: &mut Enc, error: &IoError) -> Result<(), BoundaryCodecError> {
    match error {
        IoError::Eio => enc.u8(1),
        IoError::Enospc => enc.u8(2),
        IoError::NotFound => enc.u8(3),
        IoError::AlreadyExists => enc.u8(4),
        IoError::PermissionDenied => enc.u8(5),
        IoError::InvalidData => enc.u8(6),
        IoError::InvalidRange => enc.u8(7),
        IoError::Corrupt(detail) => {
            enc.u8(8);
            bounded_bytes(enc, detail.as_bytes(), "I/O detail")?;
        }
    }
    Ok(())
}
fn decode_io_error(dec: &mut Dec<'_>) -> Result<IoError, BoundaryCodecError> {
    Ok(match dec.u8()? {
        1 => IoError::Eio,
        2 => IoError::Enospc,
        3 => IoError::NotFound,
        4 => IoError::AlreadyExists,
        5 => IoError::PermissionDenied,
        6 => IoError::InvalidData,
        7 => IoError::InvalidRange,
        8 => IoError::Corrupt(
            String::from_utf8(dec.bytes()?)
                .map_err(|_| BoundaryCodecError::Invalid("I/O detail"))?,
        ),
        _ => return Err(BoundaryCodecError::Invalid("I/O error")),
    })
}
fn encode_io_result(enc: &mut Enc, result: &IoResult) -> Result<(), BoundaryCodecError> {
    match result {
        IoResult::Written { len } => {
            enc.u8(1);
            enc.u32(*len);
        }
        IoResult::Read(bytes) => {
            enc.u8(2);
            bounded_bytes(enc, bytes, "I/O read")?;
        }
        IoResult::Fsynced => enc.u8(3),
        IoResult::Truncated { len } => {
            enc.u8(4);
            enc.u64(*len);
        }
        IoResult::Failed(error) => {
            enc.u8(5);
            encode_io_error(enc, error)?;
        }
    }
    Ok(())
}
fn decode_io_result(dec: &mut Dec<'_>) -> Result<IoResult, BoundaryCodecError> {
    Ok(match dec.u8()? {
        1 => IoResult::Written { len: dec.u32()? },
        2 => IoResult::Read(dec.bytes()?),
        3 => IoResult::Fsynced,
        4 => IoResult::Truncated { len: dec.u64()? },
        5 => IoResult::Failed(decode_io_error(dec)?),
        _ => return Err(BoundaryCodecError::Invalid("I/O result")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_ids_have_ordered_stable_variants() {
        assert!(FileId::Meta > FileId::Wal { segment: 1 });
        assert_eq!(WireMsg::new(1, vec![4]).payload, vec![4]);
    }

    #[test]
    fn peer_frame_round_trip_and_stream_prefix_are_bounded() {
        let message = WireMsg::new(7, b"append-request".to_vec());
        let mut stream = encode_peer_frame(&message).expect("frame");
        stream.extend_from_slice(b"next");
        let (decoded, used) = decode_peer_frame(&stream).expect("frame");
        assert_eq!(decoded, message);
        assert_eq!(&stream[used..], b"next");
        assert_eq!(decode_peer_frame(&stream[..5]), Err(FrameError::Incomplete));
    }

    #[test]
    fn peer_frame_checksum_rejects_corruption() {
        let mut frame = encode_peer_frame(&WireMsg::new(1, vec![1, 2, 3])).expect("frame");
        let last = frame.len() - 1;
        frame[last] ^= 1;
        assert!(matches!(
            decode_peer_frame(&frame),
            Err(FrameError::Checksum { .. })
        ));
    }

    #[test]
    fn boundary_input_and_effect_codecs_round_trip() {
        let input = Input::IoDone {
            id: IoId::new(7),
            result: IoResult::Failed(IoError::Corrupt(String::from("torn"))),
        };
        assert_eq!(
            decode_input(&encode_input(&input).expect("encode")),
            Ok(input)
        );
        let timer = Input::TimerFired {
            id: TimerId::new(9),
            generation: 3,
        };
        assert_eq!(
            decode_input(&encode_input(&timer).expect("timer encode")),
            Ok(timer)
        );
        assert_eq!(
            decode_input(&encode_input(&Input::Tick).expect("tick encode")),
            Ok(Input::Tick)
        );
        let request = Input::ClientRequest {
            client: ClientId::new(9),
            req: RequestSeq::new(3),
            session: Some((ClientId::new(77), RequestSeq::new(4))),
            command: b"CCKV".to_vec(),
        };
        assert_eq!(
            decode_input(&encode_input(&request).expect("session request encode")),
            Ok(request)
        );

        let effect = Effect::DiskRename {
            from: FileId::Temp { sequence: 9 },
            to: FileId::Snapshot { generation: 3 },
            id: IoId::new(8),
        };
        assert_eq!(
            decode_effect(&encode_effect(&effect).expect("encode")),
            Ok(effect)
        );
        let reply = Effect::ClientReply {
            client: ClientId::new(1),
            req: RequestSeq::new(2),
            reply: vec![0, 0xff],
        };
        assert_eq!(
            decode_effect(&encode_effect(&reply).expect("encode")),
            Ok(reply)
        );
    }

    fn hello(id: u64) -> PeerHello {
        PeerHello {
            cluster_id: [7; 16],
            node_id: NodeId::new(id),
            cluster_policy: ClusterPolicy::default().encode(),
            semantic_min: 2,
            semantic_max: 2,
            supported_features: 0,
            required_features: 0,
            max_peer_frame: MAX_PEER_FRAME as u32,
        }
    }

    #[test]
    fn golden_cchl_vectors() {
        let hello = hello(1);
        let encoded = hello.encode().expect("encode hello");
        assert_eq!(&encoded[..4], b"CCHL");
        assert_eq!(PeerHello::decode(&encoded), Ok((hello, encoded.len())));
    }

    #[test]
    fn trap_fragmented_or_coalesced_hello_preserves_first_frame() {
        let hello = hello(1).encode().expect("hello");
        for split in 0..hello.len() {
            assert_eq!(
                PeerHello::decode(&hello[..split]),
                Err(HelloError::Incomplete)
            );
        }
        let frame = encode_peer_frame(&WireMsg::new(2, vec![9])).expect("frame");
        let mut stream = hello;
        stream.extend_from_slice(&frame);
        let (decoded, used) = PeerHello::decode(&stream).expect("coalesced hello");
        assert_eq!(decoded.node_id, NodeId::new(1));
        assert_eq!(&stream[used..], frame);
    }

    #[test]
    fn trap_peer_hello_requires_version_overlap() {
        let left = hello(1);
        let mut right = hello(2);
        right.semantic_min = 3;
        right.semantic_max = 3;
        assert_eq!(left.negotiate(&right), Err(HelloError::VersionOverlap));
    }

    #[test]
    fn trap_peer_hello_rejects_cluster_policy_mismatch() {
        let left = hello(1);
        let mut right = hello(2);
        let policy = ClusterPolicy {
            max_sessions: 99,
            ..ClusterPolicy::default()
        };
        right.cluster_policy = policy.encode();
        assert_eq!(left.negotiate(&right), Err(HelloError::PolicyMismatch));
    }

    #[test]
    fn trap_peer_hello_rejects_missing_required_feature() {
        let left = hello(1);
        let mut right = hello(2);
        right.required_features = FEATURE_FOLLOWER_READ;
        right.supported_features = FEATURE_FOLLOWER_READ;
        assert_eq!(
            left.negotiate(&right),
            Err(HelloError::RequiredFeature(FEATURE_FOLLOWER_READ))
        );
    }
}
