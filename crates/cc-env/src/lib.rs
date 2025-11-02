// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "The value-only boundary between deterministic cores and their hosts."]

use std::fmt;

use cc_core::{Bytes, ClientId, Event, IoId, NodeId, RequestSeq, Response, Time, TimerId, crc32c};

pub const PEER_FRAME_MAGIC: u32 = u32::from_le_bytes(*b"CCPF");
pub const PEER_FRAME_VERSION: u16 = 1;
pub const MAX_PEER_FRAME: usize = 4 * 1024 * 1024;

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
#[must_use]
pub fn encode_peer_frame(message: &WireMsg) -> Vec<u8> {
    assert!(
        message.payload.len() <= MAX_PEER_FRAME,
        "peer frame payload cap"
    );
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
    frame
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
    if body_len < 2 || body_len > MAX_PEER_FRAME + 2 {
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IoError {
    Eio,
    Enospc,
    NotFound,
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
        resp: Response,
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
    },
    ClientRequest {
        client: ClientId,
        req: RequestSeq,
        cmd: cc_core::Command,
    },
    Tick,
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
        let mut stream = encode_peer_frame(&message);
        stream.extend_from_slice(b"next");
        let (decoded, used) = decode_peer_frame(&stream).expect("frame");
        assert_eq!(decoded, message);
        assert_eq!(&stream[used..], b"next");
        assert_eq!(decode_peer_frame(&stream[..5]), Err(FrameError::Incomplete));
    }

    #[test]
    fn peer_frame_checksum_rejects_corruption() {
        let mut frame = encode_peer_frame(&WireMsg::new(1, vec![1, 2, 3]));
        let last = frame.len() - 1;
        frame[last] ^= 1;
        assert!(matches!(
            decode_peer_frame(&frame),
            Err(FrameError::Checksum { .. })
        ));
    }
}
