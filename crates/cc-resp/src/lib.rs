// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Bounded RESP2 parser/encoder and the closed client command table."]

use std::fmt;

use cc_core::Duration;

pub const RESP_VERSION: u16 = 2;
pub const MAX_FRAME: usize = 4 * 1024 * 1024;
pub const MAX_ARRAY_ITEMS: usize = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RespError {
    Incomplete,
    FrameTooLarge,
    InvalidUtf8,
    InvalidInteger,
    InvalidLength,
    InvalidType(u8),
    InlineUnsupported,
    TooManyItems,
    Protocol(&'static str),
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete RESP frame"),
            Self::FrameTooLarge => write!(f, "RESP frame too large"),
            Self::InvalidUtf8 => write!(f, "RESP simple string is not UTF-8"),
            Self::InvalidInteger => write!(f, "invalid RESP integer"),
            Self::InvalidLength => write!(f, "invalid RESP length"),
            Self::InvalidType(tag) => write!(f, "invalid RESP type {tag:#x}"),
            Self::InlineUnsupported => write!(f, "inline commands are unsupported"),
            Self::TooManyItems => write!(f, "too many RESP array items"),
            Self::Protocol(reason) => write!(f, "RESP protocol error: {reason}"),
        }
    }
}

impl std::error::Error for RespError {}

pub fn parse(input: &[u8]) -> Result<(RespValue, usize), RespError> {
    let parsed = parse_at(input, 0, 0)?;
    if parsed.1 > MAX_FRAME {
        return Err(RespError::FrameTooLarge);
    }
    Ok(parsed)
}

fn parse_at(input: &[u8], offset: usize, depth: usize) -> Result<(RespValue, usize), RespError> {
    if depth > 64 || offset >= input.len() {
        return Err(RespError::Incomplete);
    }
    let tag = input[offset];
    match tag {
        b'+' | b'-' => {
            let (line, end) = line(input, offset + 1)?;
            let text = std::str::from_utf8(line)
                .map_err(|_| RespError::InvalidUtf8)?
                .to_owned();
            if tag == b'+' {
                Ok((RespValue::Simple(text), end))
            } else {
                Ok((RespValue::Error(text), end))
            }
        }
        b':' => {
            let (line, end) = line(input, offset + 1)?;
            let text = std::str::from_utf8(line).map_err(|_| RespError::InvalidUtf8)?;
            let integer = text.parse().map_err(|_| RespError::InvalidInteger)?;
            Ok((RespValue::Integer(integer), end))
        }
        b'$' => {
            let (line, mut cursor) = line(input, offset + 1)?;
            let length_text = std::str::from_utf8(line).map_err(|_| RespError::InvalidUtf8)?;
            let length: i64 = length_text.parse().map_err(|_| RespError::InvalidLength)?;
            if length == -1 {
                return Ok((RespValue::Bulk(None), cursor));
            }
            if length < -1 {
                return Err(RespError::InvalidLength);
            }
            let length = usize::try_from(length).map_err(|_| RespError::InvalidLength)?;
            if length > MAX_FRAME {
                return Err(RespError::FrameTooLarge);
            }
            let end = cursor
                .checked_add(length)
                .and_then(|value| value.checked_add(2))
                .ok_or(RespError::FrameTooLarge)?;
            if end > input.len() {
                return Err(RespError::Incomplete);
            }
            if &input[cursor + length..end] != b"\r\n" {
                return Err(RespError::Protocol("bulk string missing CRLF"));
            }
            let bytes = input[cursor..cursor + length].to_vec();
            cursor = end;
            Ok((RespValue::Bulk(Some(bytes)), cursor))
        }
        b'*' => {
            let (line, mut cursor) = line(input, offset + 1)?;
            let count_text = std::str::from_utf8(line).map_err(|_| RespError::InvalidUtf8)?;
            let count: i64 = count_text.parse().map_err(|_| RespError::InvalidLength)?;
            if count < 0 {
                return Ok((RespValue::Array(Vec::new()), cursor));
            }
            let count = usize::try_from(count).map_err(|_| RespError::InvalidLength)?;
            if count > MAX_ARRAY_ITEMS {
                return Err(RespError::TooManyItems);
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let (value, end) = parse_at(input, cursor, depth + 1)?;
                items.push(value);
                cursor = end;
            }
            Ok((RespValue::Array(items), cursor))
        }
        b'\r' | b'\n' => Err(RespError::InlineUnsupported),
        other => Err(RespError::InlineUnsupported.or_type(other)),
    }
}

trait RespErrorExt {
    fn or_type(self, tag: u8) -> RespError;
}

impl RespErrorExt for RespError {
    fn or_type(self, tag: u8) -> RespError {
        match self {
            RespError::InlineUnsupported => RespError::InvalidType(tag),
            other => other,
        }
    }
}

fn line(input: &[u8], start: usize) -> Result<(&[u8], usize), RespError> {
    let remaining = input.get(start..).ok_or(RespError::Incomplete)?;
    let relative = remaining
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or(RespError::Incomplete)?;
    Ok((&remaining[..relative], start + relative + 2))
}

#[must_use]
pub fn encode(value: &RespValue) -> Vec<u8> {
    let mut output = Vec::new();
    encode_into(value, &mut output);
    output
}

fn encode_into(value: &RespValue, output: &mut Vec<u8>) {
    match value {
        RespValue::Simple(text) => {
            output.extend_from_slice(b"+");
            output.extend_from_slice(text.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Error(text) => {
            output.extend_from_slice(b"-");
            output.extend_from_slice(text.as_bytes());
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(value) => output.extend_from_slice(format!(":{value}\r\n").as_bytes()),
        RespValue::Bulk(None) => output.extend_from_slice(b"$-1\r\n"),
        RespValue::Bulk(Some(bytes)) => {
            output.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
            output.extend_from_slice(bytes);
            output.extend_from_slice(b"\r\n");
        }
        RespValue::Array(values) => {
            output.extend_from_slice(format!("*{}\r\n", values.len()).as_bytes());
            for value in values {
                encode_into(value, output);
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientCommand {
    Ping,
    Echo(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
        nx: bool,
        xx: bool,
    },
    Get(Vec<u8>),
    Del(Vec<Vec<u8>>),
    Exists(Vec<u8>),
    IncrBy {
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
    GetDel(Vec<u8>),
    SetNx {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Expire {
        key: Vec<u8>,
        ttl: Duration,
    },
    ExpireAt {
        key: Vec<u8>,
        at_seconds: u64,
    },
    Ttl(Vec<u8>),
    Persist(Vec<u8>),
    Scan {
        cursor: u64,
        prefix: Option<Vec<u8>>,
        count: usize,
    },
    /// A caller-owned durable retry identity around exactly one mutating
    /// command. The adapter keeps the connection route separate from these
    /// values before constructing the replicated session envelope.
    Request {
        client: u64,
        sequence: u64,
        command: Box<ClientCommand>,
    },
    Info,
    Unknown(Vec<u8>),
}

pub fn parse_command(value: RespValue) -> Result<ClientCommand, RespError> {
    let RespValue::Array(values) = value else {
        return Err(RespError::Protocol("command must be an array"));
    };
    let args: Vec<Vec<u8>> = values
        .into_iter()
        .map(|value| match value {
            RespValue::Bulk(Some(bytes)) => Ok(bytes),
            RespValue::Simple(text) => Ok(text.into_bytes()),
            _ => Err(RespError::Protocol(
                "command arguments must be bulk strings",
            )),
        })
        .collect::<Result<_, _>>()?;
    parse_args(args)
}

fn parse_args(args: Vec<Vec<u8>>) -> Result<ClientCommand, RespError> {
    let command = args.first().map_or(&b""[..], Vec::as_slice);
    let upper = command
        .iter()
        .map(u8::to_ascii_uppercase)
        .collect::<Vec<_>>();
    match upper.as_slice() {
        b"PING" if args.len() == 1 => Ok(ClientCommand::Ping),
        b"ECHO" if args.len() == 2 => Ok(ClientCommand::Echo(args[1].clone())),
        b"SET" if args.len() >= 3 => parse_set(args),
        b"GET" if args.len() == 2 => Ok(ClientCommand::Get(args[1].clone())),
        b"DEL" if args.len() >= 2 => Ok(ClientCommand::Del(args[1..].to_vec())),
        b"EXISTS" if args.len() == 2 => Ok(ClientCommand::Exists(args[1].clone())),
        b"INCR" if args.len() == 2 => Ok(ClientCommand::IncrBy {
            key: args[1].clone(),
            delta: 1,
        }),
        b"DECR" if args.len() == 2 => Ok(ClientCommand::IncrBy {
            key: args[1].clone(),
            delta: -1,
        }),
        b"INCRBY" if args.len() == 3 => Ok(ClientCommand::IncrBy {
            key: args[1].clone(),
            delta: parse_i64(&args[2])?,
        }),
        b"APPEND" if args.len() == 3 => Ok(ClientCommand::Append {
            key: args[1].clone(),
            value: args[2].clone(),
        }),
        b"GETSET" if args.len() == 3 => Ok(ClientCommand::GetSet {
            key: args[1].clone(),
            value: args[2].clone(),
        }),
        b"GETDEL" if args.len() == 2 => Ok(ClientCommand::GetDel(args[1].clone())),
        b"SETNX" if args.len() == 3 => Ok(ClientCommand::SetNx {
            key: args[1].clone(),
            value: args[2].clone(),
        }),
        b"EXPIRE" if args.len() == 3 => Ok(ClientCommand::Expire {
            key: args[1].clone(),
            ttl: Duration::from_secs(parse_u64(&args[2])?),
        }),
        b"EXPIREAT" if args.len() == 3 => Ok(ClientCommand::ExpireAt {
            key: args[1].clone(),
            at_seconds: parse_u64(&args[2])?,
        }),
        b"TTL" if args.len() == 2 => Ok(ClientCommand::Ttl(args[1].clone())),
        b"PERSIST" if args.len() == 2 => Ok(ClientCommand::Persist(args[1].clone())),
        b"SCAN" if args.len() >= 2 => parse_scan(args),
        b"CC.REQUEST" if args.len() >= 4 => Ok(ClientCommand::Request {
            client: parse_u64(&args[1])?,
            sequence: parse_u64(&args[2])?,
            command: Box::new(parse_args(args[3..].to_vec())?),
        }),
        b"INFO" if args.len() == 1 => Ok(ClientCommand::Info),
        _ => Ok(ClientCommand::Unknown(
            args.first().cloned().unwrap_or_default(),
        )),
    }
}

fn parse_set(args: Vec<Vec<u8>>) -> Result<ClientCommand, RespError> {
    let mut ttl = None;
    let mut nx = false;
    let mut xx = false;
    let mut index = 3;
    while index < args.len() {
        let flag = args[index]
            .iter()
            .map(u8::to_ascii_uppercase)
            .collect::<Vec<_>>();
        match flag.as_slice() {
            b"EX" if index + 1 < args.len() => {
                ttl = Some(Duration::from_secs(parse_u64(&args[index + 1])?));
                index += 2;
            }
            b"PX" if index + 1 < args.len() => {
                ttl = Some(Duration::from_millis(parse_u64(&args[index + 1])?));
                index += 2;
            }
            b"NX" => {
                nx = true;
                index += 1;
            }
            b"XX" => {
                xx = true;
                index += 1;
            }
            _ => return Err(RespError::Protocol("invalid SET option")),
        }
    }
    if nx && xx {
        return Err(RespError::Protocol("SET NX and XX are exclusive"));
    }
    Ok(ClientCommand::Set {
        key: args[1].clone(),
        value: args[2].clone(),
        ttl,
        nx,
        xx,
    })
}

fn parse_scan(args: Vec<Vec<u8>>) -> Result<ClientCommand, RespError> {
    let cursor = parse_u64(&args[1])?;
    let mut prefix = None;
    let mut count = 10;
    let mut index = 2;
    while index < args.len() {
        let flag = args[index]
            .iter()
            .map(u8::to_ascii_uppercase)
            .collect::<Vec<_>>();
        match flag.as_slice() {
            b"MATCH" if index + 1 < args.len() => {
                let pattern = &args[index + 1];
                prefix = pattern.strip_suffix(b"*").map(ToOwned::to_owned);
                index += 2;
            }
            b"COUNT" if index + 1 < args.len() => {
                count = usize::try_from(parse_u64(&args[index + 1])?).unwrap_or(usize::MAX);
                index += 2;
            }
            _ => return Err(RespError::Protocol("invalid SCAN option")),
        }
    }
    Ok(ClientCommand::Scan {
        cursor,
        prefix,
        count,
    })
}

fn parse_u64(value: &[u8]) -> Result<u64, RespError> {
    std::str::from_utf8(value)
        .map_err(|_| RespError::InvalidUtf8)?
        .parse()
        .map_err(|_| RespError::InvalidInteger)
}

fn parse_i64(value: &[u8]) -> Result<i64, RespError> {
    std::str::from_utf8(value)
        .map_err(|_| RespError::InvalidUtf8)?
        .parse()
        .map_err(|_| RespError::InvalidInteger)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp_round_trip_nested_values() {
        let value = RespValue::Array(vec![
            RespValue::Bulk(Some(b"SET".to_vec())),
            RespValue::Bulk(Some(b"key".to_vec())),
            RespValue::Integer(3),
        ]);
        let encoded = encode(&value);
        let (decoded, used) = parse(&encoded).expect("parse");
        assert_eq!(decoded, value);
        assert_eq!(used, encoded.len());
    }

    #[test]
    fn command_table_maps_ttl_and_nx() {
        let value = RespValue::Array(vec![
            RespValue::Bulk(Some(b"set".to_vec())),
            RespValue::Bulk(Some(b"a".to_vec())),
            RespValue::Bulk(Some(b"one".to_vec())),
            RespValue::Bulk(Some(b"PX".to_vec())),
            RespValue::Bulk(Some(b"10".to_vec())),
            RespValue::Bulk(Some(b"NX".to_vec())),
        ]);
        assert_eq!(
            parse_command(value).expect("command"),
            ClientCommand::Set {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
                ttl: Some(Duration::from_millis(10)),
                nx: true,
                xx: false,
            }
        );
    }

    #[test]
    fn command_table_maps_rmw_and_ttl_family() {
        let command = |parts: &[&[u8]]| {
            RespValue::Array(
                parts
                    .iter()
                    .map(|part| RespValue::Bulk(Some(part.to_vec())))
                    .collect(),
            )
        };
        assert_eq!(
            parse_command(command(&[b"APPEND", b"k", b"v"])).expect("append"),
            ClientCommand::Append {
                key: b"k".to_vec(),
                value: b"v".to_vec()
            }
        );
        assert_eq!(
            parse_command(command(&[b"GETDEL", b"k"])).expect("getdel"),
            ClientCommand::GetDel(b"k".to_vec())
        );
        assert_eq!(
            parse_command(command(&[b"EXPIREAT", b"k", b"42"])).expect("expireat"),
            ClientCommand::ExpireAt {
                key: b"k".to_vec(),
                at_seconds: 42
            }
        );
        assert_eq!(
            parse_command(command(&[b"TTL", b"k"])).expect("ttl"),
            ClientCommand::Ttl(b"k".to_vec())
        );
    }

    #[test]
    fn trap_cc_request_keeps_the_caller_identity_outside_the_inner_command() {
        let parts: &[&[u8]] = &[b"CC.REQUEST", b"77", b"4", b"INCRBY", b"counter", b"2"];
        let command = RespValue::Array(
            parts
                .iter()
                .map(|part| RespValue::Bulk(Some(part.to_vec())))
                .collect(),
        );
        assert_eq!(
            parse_command(command).expect("CC.REQUEST"),
            ClientCommand::Request {
                client: 77,
                sequence: 4,
                command: Box::new(ClientCommand::IncrBy {
                    key: b"counter".to_vec(),
                    delta: 2,
                }),
            }
        );
    }

    #[test]
    fn trap_inline_commands_are_rejected() {
        assert!(matches!(
            parse(b"GET a\r\n"),
            Err(RespError::InlineUnsupported | RespError::InvalidType(_))
        ));
    }

    #[test]
    fn malformed_bulk_is_total() {
        assert!(matches!(
            parse(b"$4\r\na\r\n"),
            Err(RespError::Incomplete | RespError::Protocol(_))
        ));
    }

    #[test]
    fn pipelined_frames_parse_without_dropping_the_suffix() {
        let first = encode(&RespValue::Simple(String::from("PONG")));
        let second = encode(&RespValue::Integer(2));
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);
        let (value, used) = parse(&bytes).expect("first frame");
        assert_eq!(value, RespValue::Simple(String::from("PONG")));
        assert_eq!(used, first.len());
        assert_eq!(
            parse(&bytes[used..]).expect("second frame").0,
            RespValue::Integer(2)
        );
    }
}
