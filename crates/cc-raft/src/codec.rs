// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Canonical CCRP v1 encoding.  CCRP's format version is intentionally
//! independent from the Raft semantic protocol version carried in every
//! message and from CCPF's transport framing version.

use cc_core::{Dec, DecodeError, Enc, LogIndex, NodeId, Term};

use crate::{
    AppendRequest, AppendResponse, Entry, EntryKind, Message, MessageKind, SEMANTIC_VERSION_V3,
    SNAPSHOT_CHUNK_BYTES, SnapshotRejectReason, supports_protocol_version,
};

pub const CCRP_MAGIC: u32 = u32::from_le_bytes(*b"CCRP");
pub const CCRP_FORMAT_VERSION: u16 = 1;
const MAX_ENTRIES: usize = 64;
const MIN_ENTRY_BYTES: usize = 8 + 8 + 1 + 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    Decode(DecodeError),
    UnsupportedSemantic(u16),
    Invalid(&'static str),
    TooLarge(&'static str),
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(error) => write!(f, "decode: {error}"),
            Self::UnsupportedSemantic(version) => {
                write!(f, "unsupported semantic version {version}")
            }
            Self::Invalid(reason) => write!(f, "invalid CCRP: {reason}"),
            Self::TooLarge(what) => write!(f, "oversized CCRP {what}"),
        }
    }
}

impl std::error::Error for CodecError {}
impl From<DecodeError> for CodecError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

pub fn encode(message: &Message) -> Result<Vec<u8>, CodecError> {
    if !supports_protocol_version(message.proto_version)
        || message.from.get() == 0
        || message.to.get() == 0
        || message.from == message.to
    {
        return Err(CodecError::Invalid("message identity or semantic version"));
    }
    let mut enc = Enc::new();
    enc.header(CCRP_MAGIC, CCRP_FORMAT_VERSION);
    enc.u16(message.proto_version);
    enc.u64(message.from.get());
    enc.u64(message.to.get());
    enc.u64(message.term.get());
    match &message.kind {
        MessageKind::PreVoteReq {
            last_index,
            last_term,
        } => {
            enc.u8(1);
            index_term(&mut enc, *last_index, *last_term);
        }
        MessageKind::PreVoteResp { granted } => {
            enc.u8(2);
            enc.u8(u8::from(*granted));
        }
        MessageKind::VoteReq {
            last_index,
            last_term,
        } => {
            enc.u8(3);
            index_term(&mut enc, *last_index, *last_term);
        }
        MessageKind::VoteResp { granted } => {
            enc.u8(4);
            enc.u8(u8::from(*granted));
        }
        MessageKind::AppendReq(request) => {
            if request.entries.len() > MAX_ENTRIES {
                return Err(CodecError::TooLarge("entry count"));
            }
            enc.u8(5);
            enc.u64(request.prev_index.get());
            enc.u64(request.prev_term.get());
            enc.u64(request.leader_commit.get());
            enc.u64(request.read_round);
            enc.u32(
                u32::try_from(request.entries.len())
                    .map_err(|_| CodecError::TooLarge("entry count"))?,
            );
            for entry in &request.entries {
                encode_entry(&mut enc, entry)?;
            }
        }
        MessageKind::AppendResp(response) => {
            enc.u8(6);
            enc.u8(u8::from(response.success));
            enc.u64(response.match_index.get());
            match response.conflict_term {
                Some(term) if !response.success => {
                    enc.u8(1);
                    enc.u64(term.get());
                }
                None => {
                    enc.u8(0);
                    enc.u64(0);
                }
                Some(_) => return Err(CodecError::Invalid("successful append conflict")),
            }
            if response.success && response.conflict_index.get() != 0 {
                return Err(CodecError::Invalid("successful append conflict index"));
            }
            enc.u64(response.conflict_index.get());
            enc.u64(response.read_round);
        }
        MessageKind::SnapshotChunk {
            transfer_id,
            last_included_index,
            last_included_term,
            total_len,
            snapshot_crc32c,
            offset,
            data,
            done,
        } => {
            if *transfer_id == 0
                || *total_len == 0
                || data.is_empty()
                || data.len() > SNAPSHOT_CHUNK_BYTES
            {
                return Err(CodecError::TooLarge("snapshot chunk"));
            }
            let end = offset
                .checked_add(
                    u64::try_from(data.len())
                        .map_err(|_| CodecError::TooLarge("snapshot chunk"))?,
                )
                .ok_or(CodecError::Invalid("snapshot size overflow"))?;
            if end > *total_len || *done != (end == *total_len) {
                return Err(CodecError::Invalid("snapshot chunk final offset"));
            }
            enc.u8(7);
            enc.u64(*transfer_id);
            enc.u64(last_included_index.get());
            enc.u64(last_included_term.get());
            enc.u64(*total_len);
            enc.u32(*snapshot_crc32c);
            enc.u64(*offset);
            enc.u8(u8::from(*done));
            enc.u32(cc_core::crc32c(data));
            enc.bytes(data);
        }
        MessageKind::SnapshotAck {
            transfer_id,
            next_offset,
            accepted,
            reason,
        } => {
            if *transfer_id == 0
                || (*accepted && reason.is_some())
                || (!*accepted && reason.is_none())
            {
                return Err(CodecError::Invalid("snapshot ack canonicality"));
            }
            let reason_tag = reason.map_or(0, |reason| reason as u8);
            if !*accepted
                && !matches!(
                    reason,
                    Some(SnapshotRejectReason::RestartFromZero | SnapshotRejectReason::Gap)
                )
                && *next_offset != 0
            {
                return Err(CodecError::Invalid("snapshot rejection offset"));
            }
            enc.u8(8);
            enc.u64(*transfer_id);
            enc.u64(*next_offset);
            enc.u8(u8::from(*accepted));
            enc.u8(reason_tag);
        }
        MessageKind::TimeoutNow { intent_index } => {
            if intent_index.get() == 0 {
                return Err(CodecError::Invalid("zero transfer intent"));
            }
            enc.u8(9);
            enc.u64(intent_index.get());
        }
        MessageKind::FollowerReadRequest {
            request_id,
            command_hash,
        } => {
            if message.proto_version != SEMANTIC_VERSION_V3 || *request_id == 0 {
                return Err(CodecError::Invalid("follower read request semantic or id"));
            }
            enc.u8(10);
            enc.u64(*request_id);
            enc.u64(*command_hash);
        }
        MessageKind::FollowerReadGrant {
            request_id,
            command_hash,
            read_index,
            read_time,
        } => {
            if message.proto_version != SEMANTIC_VERSION_V3 || *request_id == 0 {
                return Err(CodecError::Invalid("follower read grant semantic or id"));
            }
            enc.u8(11);
            enc.u64(*request_id);
            enc.u64(*command_hash);
            enc.u64(read_index.get());
            enc.u64(read_time.as_nanos());
        }
    }
    Ok(enc.finish())
}

pub fn decode(bytes: &[u8]) -> Result<Message, CodecError> {
    let mut dec = Dec::new(bytes);
    dec.header(CCRP_MAGIC, CCRP_FORMAT_VERSION)?;
    let proto_version = dec.u16()?;
    if !supports_protocol_version(proto_version) {
        return Err(CodecError::UnsupportedSemantic(proto_version));
    }
    let from = nonzero_node(&mut dec)?;
    let to = nonzero_node(&mut dec)?;
    if from == to {
        return Err(CodecError::Invalid("self-addressed message"));
    }
    let term = Term::new(dec.u64()?);
    let kind = match dec.u8()? {
        1 => {
            let (last_index, last_term) = decode_index_term(&mut dec)?;
            MessageKind::PreVoteReq {
                last_index,
                last_term,
            }
        }
        2 => MessageKind::PreVoteResp {
            granted: bool(&mut dec)?,
        },
        3 => {
            let (last_index, last_term) = decode_index_term(&mut dec)?;
            MessageKind::VoteReq {
                last_index,
                last_term,
            }
        }
        4 => MessageKind::VoteResp {
            granted: bool(&mut dec)?,
        },
        5 => {
            let prev_index = LogIndex::new(dec.u64()?);
            let prev_term = Term::new(dec.u64()?);
            let leader_commit = LogIndex::new(dec.u64()?);
            let read_round = dec.u64()?;
            let count =
                usize::try_from(dec.u32()?).map_err(|_| CodecError::TooLarge("entry count"))?;
            if count > MAX_ENTRIES
                || count > bytes.len().saturating_sub(dec.position()) / MIN_ENTRY_BYTES
            {
                return Err(CodecError::TooLarge("entry count"));
            }
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push(decode_entry(&mut dec)?);
            }
            MessageKind::AppendReq(AppendRequest {
                prev_index,
                prev_term,
                entries,
                leader_commit,
                read_round,
            })
        }
        6 => {
            let success = bool(&mut dec)?;
            let match_index = LogIndex::new(dec.u64()?);
            let has_term = bool(&mut dec)?;
            let term_value = Term::new(dec.u64()?);
            let conflict_index = LogIndex::new(dec.u64()?);
            let read_round = dec.u64()?;
            if success && (has_term || term_value.get() != 0 || conflict_index.get() != 0) {
                return Err(CodecError::Invalid("successful append conflict"));
            }
            if !has_term && term_value.get() != 0 {
                return Err(CodecError::Invalid("noncanonical absent conflict term"));
            }
            MessageKind::AppendResp(AppendResponse {
                success,
                match_index,
                conflict_term: has_term.then_some(term_value),
                conflict_index,
                read_round,
            })
        }
        7 => {
            let transfer_id = dec.u64()?;
            let last_included_index = LogIndex::new(dec.u64()?);
            let last_included_term = Term::new(dec.u64()?);
            let total = dec.u64()?;
            let snapshot_crc = dec.u32()?;
            let offset = dec.u64()?;
            let done = bool(&mut dec)?;
            let chunk_crc = dec.u32()?;
            let data = dec.bytes()?;
            let end = offset
                .checked_add(
                    u64::try_from(data.len())
                        .map_err(|_| CodecError::TooLarge("snapshot chunk"))?,
                )
                .ok_or(CodecError::Invalid("snapshot offset overflow"))?;
            if transfer_id == 0
                || total == 0
                || data.is_empty()
                || data.len() > SNAPSHOT_CHUNK_BYTES
                || end > total
                || done != (end == total)
                || chunk_crc != cc_core::crc32c(&data)
            {
                return Err(CodecError::Invalid("snapshot chunk canonicality"));
            }
            MessageKind::SnapshotChunk {
                transfer_id,
                last_included_index,
                last_included_term,
                total_len: total,
                snapshot_crc32c: snapshot_crc,
                offset,
                data,
                done,
            }
        }
        8 => {
            let transfer_id = dec.u64()?;
            let next_offset = dec.u64()?;
            let accepted = bool(&mut dec)?;
            let reason_tag = dec.u8()?;
            let reason = match reason_tag {
                0 => None,
                tag => Some(
                    SnapshotRejectReason::decode(tag)
                        .ok_or(CodecError::Invalid("unknown snapshot rejection reason"))?,
                ),
            };
            if transfer_id == 0
                || (accepted && reason.is_some())
                || (!accepted && reason.is_none())
                || (!accepted
                    && !matches!(
                        reason,
                        Some(SnapshotRejectReason::RestartFromZero | SnapshotRejectReason::Gap)
                    )
                    && next_offset != 0)
            {
                return Err(CodecError::Invalid("snapshot ack canonicality"));
            }
            MessageKind::SnapshotAck {
                transfer_id,
                next_offset,
                accepted,
                reason,
            }
        }
        9 => {
            let intent_index = LogIndex::new(dec.u64()?);
            if intent_index.get() == 0 {
                return Err(CodecError::Invalid("zero transfer intent"));
            }
            MessageKind::TimeoutNow { intent_index }
        }
        10 => {
            if proto_version != SEMANTIC_VERSION_V3 {
                return Err(CodecError::Invalid("follower read request requires v3"));
            }
            let request_id = dec.u64()?;
            if request_id == 0 {
                return Err(CodecError::Invalid("zero follower read request id"));
            }
            MessageKind::FollowerReadRequest {
                request_id,
                command_hash: dec.u64()?,
            }
        }
        11 => {
            if proto_version != SEMANTIC_VERSION_V3 {
                return Err(CodecError::Invalid("follower read grant requires v3"));
            }
            let request_id = dec.u64()?;
            if request_id == 0 {
                return Err(CodecError::Invalid("zero follower read grant id"));
            }
            MessageKind::FollowerReadGrant {
                request_id,
                command_hash: dec.u64()?,
                read_index: LogIndex::new(dec.u64()?),
                read_time: cc_core::Time::from_nanos(dec.u64()?),
            }
        }
        tag => {
            return Err(CodecError::Decode(DecodeError::InvalidTag {
                offset: dec.position().saturating_sub(1),
                tag,
            }));
        }
    };
    dec.finish()?;
    Ok(Message {
        proto_version,
        from,
        to,
        term,
        kind,
    })
}

fn index_term(enc: &mut Enc, index: LogIndex, term: Term) {
    enc.u64(index.get());
    enc.u64(term.get());
}
fn decode_index_term(dec: &mut Dec<'_>) -> Result<(LogIndex, Term), CodecError> {
    Ok((LogIndex::new(dec.u64()?), Term::new(dec.u64()?)))
}
fn nonzero_node(dec: &mut Dec<'_>) -> Result<NodeId, CodecError> {
    let id = NodeId::new(dec.u64()?);
    if id.get() == 0 {
        Err(CodecError::Invalid("zero node id"))
    } else {
        Ok(id)
    }
}
fn bool(dec: &mut Dec<'_>) -> Result<bool, CodecError> {
    match dec.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::Invalid("noncanonical boolean")),
    }
}
fn encode_entry(enc: &mut Enc, entry: &Entry) -> Result<(), CodecError> {
    if entry.index.get() == 0 || entry.payload.len() > cc_core::MAX_CODEC_BYTES {
        return Err(CodecError::TooLarge("entry"));
    }
    enc.u64(entry.term.get());
    enc.u64(entry.index.get());
    enc.u8(entry.kind as u8);
    enc.bytes(&entry.payload);
    Ok(())
}
fn decode_entry(dec: &mut Dec<'_>) -> Result<Entry, CodecError> {
    let term = Term::new(dec.u64()?);
    let index = LogIndex::new(dec.u64()?);
    if index.get() == 0 {
        return Err(CodecError::Invalid("zero entry index"));
    }
    let kind = match dec.u8()? {
        1 => EntryKind::App,
        2 => EntryKind::Noop,
        3 => EntryKind::Config,
        tag => {
            return Err(CodecError::Decode(DecodeError::InvalidTag {
                offset: dec.position().saturating_sub(1),
                tag,
            }));
        }
    };
    Ok(Entry {
        term,
        index,
        kind,
        payload: dec.bytes()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PROTOCOL_VERSION;
    fn message(kind: MessageKind) -> Message {
        Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(3),
            kind,
        }
    }
    #[test]
    fn golden_ccrp_vectors() {
        let messages = [
            message(MessageKind::PreVoteReq {
                last_index: LogIndex::new(2),
                last_term: Term::new(1),
            }),
            message(MessageKind::PreVoteResp { granted: true }),
            message(MessageKind::VoteReq {
                last_index: LogIndex::new(2),
                last_term: Term::new(1),
            }),
            message(MessageKind::VoteResp { granted: false }),
            message(MessageKind::AppendReq(AppendRequest {
                prev_index: LogIndex::new(0),
                prev_term: Term::new(0),
                entries: Vec::new(),
                leader_commit: LogIndex::new(0),
                read_round: 4,
            })),
            message(MessageKind::AppendResp(AppendResponse {
                success: true,
                match_index: LogIndex::new(2),
                conflict_term: None,
                conflict_index: LogIndex::new(0),
                read_round: 4,
            })),
            message(MessageKind::SnapshotChunk {
                transfer_id: 1,
                last_included_index: LogIndex::new(1),
                last_included_term: Term::new(1),
                total_len: 1,
                snapshot_crc32c: cc_core::crc32c(&[7]),
                offset: 0,
                data: vec![7],
                done: true,
            }),
            message(MessageKind::SnapshotAck {
                transfer_id: 1,
                next_offset: 1,
                accepted: true,
                reason: None,
            }),
            message(MessageKind::TimeoutNow {
                intent_index: LogIndex::new(1),
            }),
        ];
        for value in messages {
            assert_eq!(
                decode(&encode(&value).expect("encode")).expect("decode"),
                value
            );
        }
    }
    #[test]
    fn trap_ccrp_preserves_read_round() {
        let value = message(MessageKind::AppendReq(AppendRequest {
            prev_index: LogIndex::new(0),
            prev_term: Term::new(0),
            entries: Vec::new(),
            leader_commit: LogIndex::new(0),
            read_round: 55,
        }));
        assert_eq!(
            decode(&encode(&value).expect("encode")).expect("decode"),
            value
        );
    }
    #[test]
    fn trap_timeout_now_codec_requires_nonzero_intent() {
        assert!(
            encode(&message(MessageKind::TimeoutNow {
                intent_index: LogIndex::new(0)
            }))
            .is_err()
        );
    }

    #[test]
    fn trap_follower_read_tags_require_semantic_v3() {
        let request = Message {
            proto_version: SEMANTIC_VERSION_V3,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(3),
            kind: MessageKind::FollowerReadRequest {
                request_id: 9,
                command_hash: 0xfeed,
            },
        };
        let grant = Message {
            kind: MessageKind::FollowerReadGrant {
                request_id: 9,
                command_hash: 0xfeed,
                read_index: LogIndex::new(7),
                read_time: cc_core::Time::from_nanos(8),
            },
            ..request.clone()
        };
        for message in [request, grant] {
            assert_eq!(
                decode(&encode(&message).expect("v3 encode")).expect("v3 decode"),
                message
            );
        }
        let invalid = Message {
            proto_version: PROTOCOL_VERSION,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: Term::new(3),
            kind: MessageKind::FollowerReadRequest {
                request_id: 1,
                command_hash: 1,
            },
        };
        assert!(encode(&invalid).is_err());
    }

    #[test]
    fn trap_snapshot_chunk_done_and_ack_offsets_are_canonical() {
        let snapshot = message(MessageKind::SnapshotChunk {
            transfer_id: 9,
            last_included_index: LogIndex::new(4),
            last_included_term: Term::new(2),
            total_len: 2,
            snapshot_crc32c: cc_core::crc32c(&[7, 8]),
            offset: 0,
            data: vec![7],
            done: false,
        });
        assert_eq!(
            decode(&encode(&snapshot).expect("encode")).expect("decode"),
            snapshot
        );

        let overshoot = message(MessageKind::SnapshotChunk {
            transfer_id: 9,
            last_included_index: LogIndex::new(4),
            last_included_term: Term::new(2),
            total_len: 1,
            snapshot_crc32c: cc_core::crc32c(&[7, 8]),
            offset: 0,
            data: vec![7, 8],
            done: false,
        });
        assert!(encode(&overshoot).is_err());

        let rejected = message(MessageKind::SnapshotAck {
            transfer_id: 9,
            next_offset: 1,
            accepted: false,
            reason: Some(SnapshotRejectReason::Gap),
        });
        assert_eq!(
            decode(&encode(&rejected).expect("encode")).expect("decode"),
            rejected
        );
        let invalid_ack = message(MessageKind::SnapshotAck {
            transfer_id: 9,
            next_offset: 1,
            accepted: true,
            reason: Some(SnapshotRejectReason::Gap),
        });
        assert!(encode(&invalid_ack).is_err());
    }

    #[test]
    fn trap_ccrp_count_is_bounded_by_remaining_bytes() {
        let mut encoded = encode(&message(MessageKind::AppendReq(AppendRequest {
            prev_index: LogIndex::new(0),
            prev_term: Term::new(0),
            entries: Vec::new(),
            leader_commit: LogIndex::new(0),
            read_round: 0,
        })))
        .expect("append frame");
        // CCRP fixed header + semantic/from/to/term/tag + append prefix.
        encoded[65..69].copy_from_slice(&1_u32.to_le_bytes());
        assert_eq!(decode(&encoded), Err(CodecError::TooLarge("entry count")));
    }

    #[test]
    fn trap_ccrp_rejects_noncanonical_options() {
        let mut encoded = encode(&message(MessageKind::AppendResp(AppendResponse {
            success: true,
            match_index: LogIndex::new(2),
            conflict_term: None,
            conflict_index: LogIndex::new(0),
            read_round: 0,
        })))
        .expect("append response");
        // A successful response must encode an absent conflict term as both
        // flag=0 and a zero value.
        encoded[42] = 1;
        encoded[43..51].copy_from_slice(&3_u64.to_le_bytes());
        assert_eq!(
            decode(&encoded),
            Err(CodecError::Invalid("successful append conflict"))
        );
    }

    #[test]
    fn trap_unknown_message_tag_is_a_typed_error() {
        let mut encoded =
            encode(&message(MessageKind::VoteResp { granted: true })).expect("vote response");
        encoded[32] = 99;
        assert!(matches!(
            decode(&encoded),
            Err(CodecError::Decode(DecodeError::InvalidTag { tag: 99, .. }))
        ));
    }
}
