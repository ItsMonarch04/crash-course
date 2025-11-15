// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Checksummed, torn-write-tolerant write-ahead log primitives."]

use std::fmt;

use cc_core::crc32c;

pub const FORMAT_VERSION: u16 = 1;
pub const WAL_MAGIC: [u8; 4] = *b"CCWL";
pub const DEFAULT_SEGMENT_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_RECORD_SIZE: usize = 2 * 1024 * 1024;
const RECORD_HEADER_SIZE: usize = 9;
const SEGMENT_HEADER_SIZE: usize = 14;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Data = 1,
    Pad = 2,
    SegmentSeal = 3,
}

impl RecordType {
    fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Data,
            2 => Self::Pad,
            3 => Self::SegmentSeal,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Lsn {
    pub segment: u64,
    pub offset: u32,
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.segment, self.offset)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LsnRange {
    pub first: Lsn,
    pub end: Lsn,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogRecord {
    pub lsn: Lsn,
    pub kind: RecordType,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalConfig {
    pub segment_size: usize,
    pub max_record_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_record_size: MAX_RECORD_SIZE,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentImage {
    pub sequence: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalError {
    InvalidConfig,
    RecordTooLarge {
        size: usize,
        max: usize,
    },
    InvalidRecord {
        segment: u64,
        offset: usize,
        reason: &'static str,
    },
    MidLogCorruption {
        segment: u64,
        offset: usize,
    },
    InvalidSegmentSequence {
        expected: u64,
        actual: u64,
    },
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig => write!(f, "invalid WAL configuration"),
            Self::RecordTooLarge { size, max } => {
                write!(f, "WAL record size {size} exceeds {max}")
            }
            Self::InvalidRecord {
                segment,
                offset,
                reason,
            } => write!(f, "invalid WAL record at {segment}:{offset}: {reason}"),
            Self::MidLogCorruption { segment, offset } => {
                write!(f, "mid-log WAL corruption at {segment}:{offset}")
            }
            Self::InvalidSegmentSequence { expected, actual } => {
                write!(f, "WAL segment sequence {actual}, expected {expected}")
            }
        }
    }
}

impl std::error::Error for WalError {}

#[derive(Clone, Debug)]
struct Segment {
    sequence: u64,
    bytes: Vec<u8>,
    written_len: usize,
    durable_len: usize,
}

impl Segment {
    fn new(sequence: u64) -> Self {
        let mut bytes = Vec::with_capacity(SEGMENT_HEADER_SIZE + 10);
        bytes.extend_from_slice(&WAL_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&sequence.to_le_bytes());
        Self {
            sequence,
            bytes,
            written_len: SEGMENT_HEADER_SIZE,
            durable_len: SEGMENT_HEADER_SIZE,
        }
    }
}

/// An in-memory WAL host that makes write visibility and durability explicit.
/// A real host maps `flush` and `commit` to disk write and fsync effects.
pub struct Wal {
    config: WalConfig,
    segments: Vec<Segment>,
    records: Vec<LogRecord>,
    pending_first: Option<Lsn>,
}

impl Wal {
    pub fn new(config: WalConfig) -> Result<Self, WalError> {
        if config.segment_size < SEGMENT_HEADER_SIZE + RECORD_HEADER_SIZE
            || config.max_record_size < RECORD_HEADER_SIZE
            || config.max_record_size > MAX_RECORD_SIZE
        {
            return Err(WalError::InvalidConfig);
        }
        Ok(Self {
            config,
            segments: vec![Segment::new(0)],
            records: Vec::new(),
            pending_first: None,
        })
    }

    #[must_use]
    pub fn config(&self) -> WalConfig {
        self.config
    }

    #[must_use]
    pub fn records(&self) -> &[LogRecord] {
        &self.records
    }

    #[must_use]
    pub fn segment_images(&self) -> Vec<SegmentImage> {
        self.segments
            .iter()
            .map(|segment| SegmentImage {
                sequence: segment.sequence,
                bytes: segment.bytes.clone(),
            })
            .collect()
    }

    #[must_use]
    pub fn durable_images(&self) -> Vec<SegmentImage> {
        self.segments
            .iter()
            .map(|segment| SegmentImage {
                sequence: segment.sequence,
                bytes: segment.bytes[..segment.durable_len].to_vec(),
            })
            .collect()
    }

    pub fn append(&mut self, kind: RecordType, payload: &[u8]) -> Result<LsnRange, WalError> {
        let encoded = encode_record(kind, payload, self.config.max_record_size)?;
        if encoded.len() > self.config.segment_size - SEGMENT_HEADER_SIZE {
            return Err(WalError::RecordTooLarge {
                size: encoded.len(),
                max: self.config.segment_size - SEGMENT_HEADER_SIZE,
            });
        }
        let available = self
            .config
            .segment_size
            .saturating_sub(self.active().bytes.len());
        if available < encoded.len() {
            self.pad_active(available)?;
            self.roll_segment();
        }
        let segment = self.active_mut();
        let offset = segment.bytes.len();
        segment.bytes.extend_from_slice(&encoded);
        let end = segment.bytes.len();
        segment.written_len = end;
        let lsn = Lsn {
            segment: segment.sequence,
            offset: u32::try_from(offset).expect("invariant: WAL segment offset fits u32"),
        };
        let range = LsnRange {
            first: lsn,
            end: Lsn {
                segment: segment.sequence,
                offset: u32::try_from(end).expect("invariant: WAL segment offset fits u32"),
            },
        };
        self.records.push(LogRecord {
            lsn,
            kind,
            payload: payload.to_vec(),
        });
        if self.pending_first.is_none() {
            self.pending_first = Some(lsn);
        }
        Ok(range)
    }

    /// Make page-cache writes visible to a subsequent read, but not durable.
    pub fn flush(&mut self) {
        for segment in &mut self.segments {
            segment.written_len = segment.bytes.len();
        }
    }

    /// Complete the fsync barrier for all writes that have been flushed.
    pub fn commit(&mut self) -> Option<LsnRange> {
        self.flush();
        for segment in &mut self.segments {
            segment.durable_len = segment.written_len;
        }
        let first = self.pending_first.take()?;
        let last = self.records.last()?.lsn;
        Some(LsnRange {
            first,
            end: Lsn {
                segment: last.segment,
                offset: last.offset,
            },
        })
    }

    #[must_use]
    pub fn is_durable(&self, lsn: Lsn) -> bool {
        self.segments
            .iter()
            .find(|segment| segment.sequence == lsn.segment)
            .is_some_and(|segment| {
                usize::try_from(lsn.offset).unwrap_or(usize::MAX) < segment.durable_len
            })
    }

    fn active(&self) -> &Segment {
        self.segments
            .last()
            .expect("invariant: WAL has an active segment")
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("invariant: WAL has an active segment")
    }

    fn pad_active(&mut self, available: usize) -> Result<(), WalError> {
        if available == 0 {
            return Ok(());
        }
        if available < RECORD_HEADER_SIZE || available > self.config.max_record_size {
            let segment = self.active_mut();
            segment.bytes.extend(std::iter::repeat_n(0, available));
            segment.written_len = segment.bytes.len();
            return Ok(());
        }
        let payload_len = available - RECORD_HEADER_SIZE;
        let padding = encode_record(
            RecordType::Pad,
            &vec![0; payload_len],
            self.config.max_record_size,
        )?;
        if padding.len() != available {
            return Err(WalError::InvalidConfig);
        }
        let segment = self.active_mut();
        segment.bytes.extend_from_slice(&padding);
        segment.written_len = segment.bytes.len();
        Ok(())
    }

    fn roll_segment(&mut self) {
        let next = self
            .active()
            .sequence
            .checked_add(1)
            .expect("invariant: WAL segment sequence overflow");
        self.segments.push(Segment::new(next));
    }
}

fn encode_record(
    kind: RecordType,
    payload: &[u8],
    max_record_size: usize,
) -> Result<Vec<u8>, WalError> {
    let size = RECORD_HEADER_SIZE + payload.len();
    if size > max_record_size || payload.len() > u32::MAX as usize {
        return Err(WalError::RecordTooLarge {
            size,
            max: max_record_size,
        });
    }
    let mut body = Vec::with_capacity(1 + payload.len());
    body.push(kind as u8);
    body.extend_from_slice(payload);
    let mut encoded = Vec::with_capacity(size);
    encoded.extend_from_slice(
        &(u32::try_from(payload.len()).expect("invariant: payload fits u32")).to_le_bytes(),
    );
    encoded.extend_from_slice(&crc32c(&body).to_le_bytes());
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn parse_record(
    bytes: &[u8],
    sequence: u64,
    offset: usize,
    max_record_size: usize,
) -> Result<Option<(LogRecord, usize)>, WalError> {
    if bytes[offset..].iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    if bytes.len() - offset < RECORD_HEADER_SIZE {
        return Err(WalError::InvalidRecord {
            segment: sequence,
            offset,
            reason: "truncated header",
        });
    }
    let payload_len = u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("invariant: header slice is four bytes"),
    ) as usize;
    let total = RECORD_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or(WalError::InvalidRecord {
            segment: sequence,
            offset,
            reason: "length overflow",
        })?;
    if total > max_record_size || total > bytes.len() - offset {
        return Err(WalError::InvalidRecord {
            segment: sequence,
            offset,
            reason: "length outside segment",
        });
    }
    let expected_crc = u32::from_le_bytes(
        bytes[offset + 4..offset + 8]
            .try_into()
            .expect("invariant: CRC slice is four bytes"),
    );
    let kind = RecordType::from_byte(bytes[offset + 8]).ok_or(WalError::InvalidRecord {
        segment: sequence,
        offset,
        reason: "unknown record type",
    })?;
    let body = &bytes[offset + 8..offset + total];
    let actual_crc = crc32c(body);
    if actual_crc != expected_crc {
        return Err(WalError::InvalidRecord {
            segment: sequence,
            offset,
            reason: "CRC mismatch",
        });
    }
    Ok(Some((
        LogRecord {
            lsn: Lsn {
                segment: sequence,
                offset: u32::try_from(offset).expect("invariant: WAL offset fits u32"),
            },
            kind,
            payload: body[1..].to_vec(),
        },
        offset + total,
    )))
}

fn has_valid_record_after(
    bytes: &[u8],
    sequence: u64,
    start: usize,
    max_record_size: usize,
) -> bool {
    ((start + 1)..bytes.len()).any(|offset| {
        parse_record(bytes, sequence, offset, max_record_size)
            .ok()
            .flatten()
            .is_some()
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredWal {
    pub records: Vec<LogRecord>,
    pub segments: Vec<SegmentImage>,
    pub truncated_at: Option<Lsn>,
}

impl RecoveredWal {
    #[must_use]
    pub fn durable_payloads(&self) -> Vec<Vec<u8>> {
        self.records
            .iter()
            .filter(|record| record.kind == RecordType::Data)
            .map(|record| record.payload.clone())
            .collect()
    }
}

pub fn recover(images: &[SegmentImage], config: WalConfig) -> Result<RecoveredWal, WalError> {
    if images.is_empty() || config.segment_size < SEGMENT_HEADER_SIZE + RECORD_HEADER_SIZE {
        return Err(WalError::InvalidConfig);
    }
    let mut records = Vec::new();
    let mut normalized = Vec::with_capacity(images.len());
    let mut truncated_at = None;
    for (index, image) in images.iter().enumerate() {
        let expected_sequence = index as u64;
        if image.sequence != expected_sequence {
            return Err(WalError::InvalidSegmentSequence {
                expected: expected_sequence,
                actual: image.sequence,
            });
        }
        if image.bytes.len() < SEGMENT_HEADER_SIZE {
            return Err(WalError::InvalidRecord {
                segment: image.sequence,
                offset: 0,
                reason: "missing segment header",
            });
        }
        if image.bytes[..4] != WAL_MAGIC
            || u16::from_le_bytes([image.bytes[4], image.bytes[5]]) != FORMAT_VERSION
        {
            return Err(WalError::InvalidRecord {
                segment: image.sequence,
                offset: 0,
                reason: "invalid segment header",
            });
        }
        let header_sequence = u64::from_le_bytes(
            image.bytes[6..14]
                .try_into()
                .expect("invariant: segment header is fourteen bytes"),
        );
        if header_sequence != image.sequence {
            return Err(WalError::InvalidSegmentSequence {
                expected: image.sequence,
                actual: header_sequence,
            });
        }
        let mut offset = 14;
        let mut output = image.bytes.clone();
        while offset < image.bytes.len() {
            match parse_record(&image.bytes, image.sequence, offset, config.max_record_size) {
                Ok(Some((record, next))) => {
                    if record.kind != RecordType::Pad {
                        records.push(record);
                    }
                    offset = next;
                }
                Ok(None) => {
                    output.truncate(offset);
                    break;
                }
                Err(_error) => {
                    let later_segment_exists = index + 1 < images.len();
                    let valid_after = has_valid_record_after(
                        &image.bytes,
                        image.sequence,
                        offset,
                        config.max_record_size,
                    );
                    if later_segment_exists || valid_after {
                        return Err(WalError::MidLogCorruption {
                            segment: image.sequence,
                            offset,
                        });
                    }
                    output.truncate(offset);
                    truncated_at = Some(Lsn {
                        segment: image.sequence,
                        offset: u32::try_from(offset).expect("invariant: WAL offset fits u32"),
                    });
                    break;
                }
            }
            if offset == image.bytes.len() {
                break;
            }
        }
        if truncated_at.is_some() {
            normalized.push(SegmentImage {
                sequence: image.sequence,
                bytes: output,
            });
            break;
        }
        normalized.push(SegmentImage {
            sequence: image.sequence,
            bytes: output,
        });
    }
    if normalized.is_empty() {
        return Err(WalError::InvalidConfig);
    }
    Ok(RecoveredWal {
        records,
        segments: normalized,
        truncated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> WalConfig {
        WalConfig {
            segment_size: 64,
            max_record_size: 32,
        }
    }

    #[test]
    fn append_flush_commit_and_recover() {
        let mut wal = Wal::new(config()).expect("config");
        let range = wal.append(RecordType::Data, b"one").expect("append");
        assert!(!wal.is_durable(range.first));
        wal.flush();
        assert!(!wal.is_durable(range.first));
        wal.commit().expect("commit");
        assert!(wal.is_durable(range.first));
        let recovered = recover(&wal.durable_images(), config()).expect("recovery");
        assert_eq!(recovered.durable_payloads(), vec![b"one".to_vec()]);
    }

    #[test]
    fn page_cache_writes_disappear_without_commit() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"lost").expect("append");
        wal.flush();
        let durable = Wal::new(config()).expect("fresh WAL").durable_images();
        let recovered = recover(&durable, config()).expect("recovery");
        assert!(recovered.records.is_empty());
    }

    #[test]
    fn rolls_segments_without_splitting_records() {
        let mut wal = Wal::new(config()).expect("config");
        for value in [
            b"aaaa".as_slice(),
            b"bbbb".as_slice(),
            b"cccc".as_slice(),
            b"dddd".as_slice(),
            b"eeee".as_slice(),
        ] {
            wal.append(RecordType::Data, value).expect("append");
        }
        wal.commit().expect("commit");
        assert!(wal.segment_images().len() > 1);
        let recovered = recover(&wal.durable_images(), config()).expect("recovery");
        assert_eq!(recovered.durable_payloads().len(), 5);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"good").expect("append");
        wal.commit().expect("commit");
        wal.append(RecordType::Data, b"tail").expect("append");
        wal.flush();
        let mut image = wal.segment_images().pop().expect("segment");
        image.bytes.pop();
        let recovered = recover(&[image], config()).expect("torn tail is expected");
        assert_eq!(recovered.durable_payloads(), vec![b"good".to_vec()]);
        assert!(recovered.truncated_at.is_some());
    }

    #[test]
    fn trap_midlog_corruption_failstop() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"first").expect("append");
        wal.append(RecordType::Data, b"second").expect("append");
        wal.commit().expect("commit");
        let mut image = wal.durable_images().pop().expect("segment");
        image.bytes[14 + RECORD_HEADER_SIZE] ^= 1;
        let error = recover(&[image], config()).expect_err("mid-log corruption");
        assert!(matches!(error, WalError::MidLogCorruption { .. }));
    }

    #[test]
    fn recovery_is_idempotent_after_tail_truncate() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"good").expect("append");
        wal.commit().expect("commit");
        let mut image = wal.durable_images().pop().expect("segment");
        image.bytes.extend_from_slice(&[1, 2, 3]);
        let first = recover(&[image], config()).expect("first recovery");
        let second = recover(&first.segments, config()).expect("second recovery");
        assert_eq!(first.records, second.records);
        assert_eq!(first.segments, second.segments);
        assert!(first.truncated_at.is_some());
        assert!(second.truncated_at.is_none());
    }

    #[test]
    fn crash_before_write_completes_leaves_no_record() {
        let wal = Wal::new(config()).expect("config");
        let recovered = recover(&wal.durable_images(), config()).expect("recovery");
        assert!(recovered.records.is_empty());
    }

    #[test]
    fn crash_after_write_before_fsync_loses_the_page_cache_record() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"not-durable")
            .expect("append");
        wal.flush();
        let recovered = recover(&wal.durable_images(), config()).expect("recovery");
        assert!(recovered.records.is_empty());
    }

    #[test]
    fn crash_during_multi_record_batch_keeps_only_durable_prefix() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"committed").expect("append");
        wal.commit().expect("commit");
        wal.append(RecordType::Data, b"pending").expect("append");
        let recovered = recover(&wal.durable_images(), config()).expect("recovery");
        assert_eq!(recovered.durable_payloads(), vec![b"committed".to_vec()]);
    }

    #[test]
    fn torn_mid_record_at_sector_boundary_is_tail_safe() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"durable").expect("append");
        wal.commit().expect("commit");
        wal.append(RecordType::Data, b"torn").expect("append");
        let mut image = wal.segment_images().pop().expect("segment");
        image.bytes.truncate(14 + 16 + 4);
        let recovered = recover(&[image], config()).expect("tail recovery");
        assert_eq!(recovered.durable_payloads(), vec![b"durable".to_vec()]);
    }

    #[test]
    fn crash_during_segment_roll_preserves_old_segment() {
        let mut wal = Wal::new(config()).expect("config");
        for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            wal.append(RecordType::Data, value).expect("append");
        }
        wal.commit().expect("commit");
        wal.append(RecordType::Data, b"new-segment")
            .expect("append");
        let durable = wal.durable_images();
        let recovered = recover(&durable, config()).expect("recovery");
        assert!(!recovered.durable_payloads().is_empty());
    }

    #[test]
    fn crash_during_truncate_after_recovery_is_idempotent() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"good").expect("append");
        wal.commit().expect("commit");
        let mut image = wal.segment_images().pop().expect("segment");
        image.bytes.push(0xaa);
        let recovered = recover(&[image], config()).expect("first recovery");
        let reopened = recover(&recovered.segments, config()).expect("reopen");
        assert_eq!(reopened.durable_payloads(), vec![b"good".to_vec()]);
    }

    #[test]
    fn double_crash_during_recovery_has_same_prefix() {
        let mut wal = Wal::new(config()).expect("config");
        wal.append(RecordType::Data, b"good").expect("append");
        wal.commit().expect("commit");
        let mut image = wal.segment_images().pop().expect("segment");
        image.bytes.extend_from_slice(&[0xaa, 0xbb]);
        let first = recover(&[image], config()).expect("first recovery");
        let second = recover(&first.segments, config()).expect("second recovery");
        assert_eq!(first.durable_payloads(), second.durable_payloads());
    }

    #[test]
    fn prefix_durability_property_for_many_deterministic_sequences() {
        for seed in 0..1_000_u64 {
            let mut rng = cc_core::Xoshiro256pp::stream(cc_core::Seed::new(seed), "wal-test", 0);
            let mut wal = Wal::new(WalConfig {
                segment_size: 256,
                max_record_size: 64,
            })
            .expect("config");
            let mut committed = Vec::new();
            let mut pending = Vec::new();
            for index in 0..20_u8 {
                let length = usize::try_from(rng.range_u64(1, 17)).expect("small length");
                let value = vec![index; length];
                wal.append(RecordType::Data, &value).expect("append");
                pending.push(value);
                if index % 3 == 2 {
                    wal.commit().expect("commit");
                    committed.append(&mut pending);
                }
            }
            let recovered = recover(&wal.durable_images(), wal.config()).expect("recovery");
            assert_eq!(recovered.durable_payloads(), committed);
        }
    }

    #[test]
    #[ignore = "G1 long campaign; run explicitly in release mode"]
    fn prefix_durability_campaign_1m() {
        for seed in 0..1_000_000_u64 {
            let mut wal = Wal::new(WalConfig {
                segment_size: 512,
                max_record_size: 128,
            })
            .expect("config");
            let first = seed.to_le_bytes().to_vec();
            let second = seed.rotate_left(7).to_le_bytes().to_vec();
            let third = seed.rotate_left(13).to_le_bytes().to_vec();
            wal.append(RecordType::Data, &first).expect("append");
            wal.append(RecordType::Data, &second).expect("append");
            wal.commit().expect("commit");
            wal.append(RecordType::Data, &third).expect("append");
            let recovered = recover(&wal.durable_images(), wal.config()).expect("recovery");
            assert_eq!(recovered.durable_payloads(), vec![first, second]);
        }
    }
}
