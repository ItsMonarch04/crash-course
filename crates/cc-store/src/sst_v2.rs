// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! The bounded, block-structured `CCST` v2 table codec.
//!
//! The pre-N3 `SstTable` remains the `CCST` v1 reader.  This module does not
//! reinterpret those bytes: v2 has an independently checksummed index, bloom,
//! and footer, and an exact EOF rule before it exposes a single entry.

use cc_core::{Duration, crc32c};
use cc_env::FileId;

use crate::{BlockSource, InternalKey, StoreError, StoreRead, ValueKind};

pub const SST_V2_VERSION: u16 = 2;
pub const SST_V2_TARGET_BLOCK_BYTES: usize = 4 * 1024;
pub const SST_V2_FOOTER_BYTES: usize = 42;
const BLOCK_TRAILER_FIXED_BYTES: usize = 9;
const RESTART_INTERVAL: usize = 16;
const BLOOM_PROBES: u8 = 7;

/// Limits used before a v2 decoder allocates or a writer accepts an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SstV2Limits {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_file_bytes: usize,
    pub max_entries: usize,
}

impl Default for SstV2Limits {
    fn default() -> Self {
        Self {
            max_key_bytes: 4 * 1024,
            max_value_bytes: 1024 * 1024,
            max_file_bytes: 64 * 1024 * 1024,
            max_entries: 1_000_000,
        }
    }
}

impl SstV2Limits {
    fn valid(self) -> bool {
        self.max_key_bytes > 0
            && self.max_value_bytes > 0
            && self.max_file_bytes >= SST_V2_FOOTER_BYTES
            && self.max_entries > 0
    }
}

/// Footer/index metadata retained by a file-backed reader.  The table bytes
/// and decoded data blocks do not need to remain in memory after boot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstV2Meta {
    pub index_offset: u64,
    pub index_length: u32,
    pub bloom_offset: u64,
    pub bloom_length: u32,
    pub entry_count: u64,
}

/// A fully verified v2 table.  `entries` is provided for deterministic codec
/// tests and migration tooling; a production file reader retains only `meta`
/// and reads data blocks through the host-owned block source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstV2Table {
    pub entries: Vec<(InternalKey, Vec<u8>)>,
    pub meta: SstV2Meta,
    bloom_bits: Vec<u8>,
}

/// File-backed v2 table metadata.  It deliberately retains only index and
/// bloom bytes; a point read requests the selected data block from the host
/// boundary instead of retaining table contents in the deterministic core.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstV2Reader {
    file: FileId,
    file_size: u64,
    meta: SstV2Meta,
    footer_crc32c: u32,
    index: Vec<IndexRecord>,
    bloom_bits: Vec<u8>,
    limits: SstV2Limits,
}

type VisibleEntry = (Vec<u8>, ValueKind, Vec<u8>, u64);

impl SstV2Reader {
    pub fn open(
        source: &mut dyn BlockSource,
        file: FileId,
        file_size: u64,
        limits: SstV2Limits,
    ) -> StoreRead<Self> {
        let mut service = Duration::from_nanos(0);
        if !limits.valid() || file_size < SST_V2_FOOTER_BYTES as u64 {
            return store_read_error(service, StoreError::InvalidInput("v2 SST file size"));
        }
        if file_size > limits.max_file_bytes as u64 {
            return store_read_error(
                service,
                StoreError::TooLarge {
                    what: "v2 SST file",
                    size: usize::try_from(file_size).unwrap_or(usize::MAX),
                    max: limits.max_file_bytes,
                },
            );
        }
        let footer_offset = file_size - SST_V2_FOOTER_BYTES as u64;
        let footer = match source.read_block(file, footer_offset, SST_V2_FOOTER_BYTES as u32) {
            Ok(read) => {
                service = add_service(service, read.service);
                read.bytes
            }
            Err(error) => return store_read_block_error(service, error),
        };
        let meta = match decode_footer(&footer) {
            Ok(value) => value,
            Err(error) => return store_read_error(service, error),
        };
        let footer_crc32c = match sst_v2_footer_crc32c(&footer) {
            Ok(value) => value,
            Err(error) => return store_read_error(service, error),
        };
        let index_end = match meta.index_offset.checked_add(u64::from(meta.index_length)) {
            Some(value) => value,
            None => return store_read_error(service, StoreError::Corrupt("v2 SST index range")),
        };
        let bloom_end = match meta.bloom_offset.checked_add(u64::from(meta.bloom_length)) {
            Some(value) => value,
            None => return store_read_error(service, StoreError::Corrupt("v2 SST bloom range")),
        };
        if meta.index_offset == 0
            || index_end > footer_offset
            || meta.bloom_offset < index_end
            || bloom_end != footer_offset
        {
            return store_read_error(service, StoreError::Corrupt("v2 SST footer ranges"));
        }
        let index_bytes = match source.read_block(file, meta.index_offset, meta.index_length) {
            Ok(read) => {
                service = add_service(service, read.service);
                read.bytes
            }
            Err(error) => return store_read_block_error(service, error),
        };
        let index = match decode_index_block(&index_bytes, limits) {
            Ok(value) if !value.is_empty() => value,
            Ok(_) => {
                return store_read_error(
                    service,
                    StoreError::Corrupt("v2 SST has no index records"),
                );
            }
            Err(error) => return store_read_error(service, error),
        };
        let bloom_bytes = match source.read_block(file, meta.bloom_offset, meta.bloom_length) {
            Ok(read) => {
                service = add_service(service, read.service);
                read.bytes
            }
            Err(error) => return store_read_block_error(service, error),
        };
        let bloom_bits = match decode_bloom_block(&bloom_bytes, meta.entry_count, limits) {
            Ok(value) => value,
            Err(error) => return store_read_error(service, error),
        };
        StoreRead {
            service,
            outcome: Ok(Self {
                file,
                file_size,
                meta,
                footer_crc32c,
                index,
                bloom_bits,
                limits,
            }),
        }
    }

    #[must_use]
    pub fn metadata(&self) -> &SstV2Meta {
        &self.meta
    }

    #[must_use]
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    #[must_use]
    pub const fn footer_crc32c(&self) -> u32 {
        self.footer_crc32c
    }

    #[must_use]
    pub fn retained_metadata_bytes(&self) -> u64 {
        let index = self.index.iter().fold(0_u64, |total, record| {
            total.saturating_add(
                u64::try_from(record.last.user_key.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(29),
            )
        });
        index
            .saturating_add(u64::try_from(self.bloom_bits.len()).unwrap_or(u64::MAX))
            .saturating_add(SST_V2_FOOTER_BYTES as u64)
    }

    /// Consult the retained bloom filter without issuing a data-block read.
    /// The store uses this exact decision to expose positive/negative
    /// operator counters without duplicating the filter implementation.
    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        bloom_may_contain(&self.bloom_bits, key)
    }

    pub fn get(
        &self,
        source: &mut dyn BlockSource,
        key: &[u8],
        snapshot: u64,
    ) -> StoreRead<Option<(ValueKind, Vec<u8>, u64)>> {
        if !bloom_may_contain(&self.bloom_bits, key) {
            return StoreRead {
                service: Duration::from_nanos(0),
                outcome: Ok(None),
            };
        }
        let Some(record) = self
            .index
            .iter()
            .find(|record| record.last.user_key.as_slice() >= key)
        else {
            return StoreRead {
                service: Duration::from_nanos(0),
                outcome: Ok(None),
            };
        };
        let read = match source.read_block(self.file, record.offset, record.length) {
            Ok(value) => value,
            Err(error) => return store_read_block_error(Duration::from_nanos(0), error),
        };
        let service = read.service;
        let entries = match decode_data_block(&read.bytes, self.limits) {
            Ok(value) => value,
            Err(error) => return store_read_error(service, error),
        };
        let value = entries
            .iter()
            .filter(|(internal, _)| internal.user_key == key && internal.sequence <= snapshot)
            .max_by_key(|(internal, _)| internal.sequence)
            .map(|(internal, value)| (internal.kind, value.clone(), internal.sequence));
        StoreRead {
            service,
            outcome: Ok(value),
        }
    }

    /// Read data blocks in index order and stop as soon as `limit` visible
    /// user keys have been produced. The iterator state is just the decoded
    /// index plus the current block; the table body is never retained.
    pub fn scan(
        &self,
        source: &mut dyn BlockSource,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot: u64,
        limit: usize,
    ) -> StoreRead<Vec<VisibleEntry>> {
        let mut service = Duration::from_nanos(0);
        let mut visible = std::collections::BTreeMap::<Vec<u8>, (ValueKind, Vec<u8>, u64)>::new();
        if limit == 0 {
            return StoreRead {
                service,
                outcome: Ok(Vec::new()),
            };
        }
        for record in &self.index {
            if start.is_some_and(|start| record.last.user_key.as_slice() < start) {
                continue;
            }
            let read = match source.read_block(self.file, record.offset, record.length) {
                Ok(value) => value,
                Err(error) => return store_read_block_error(service, error),
            };
            service = add_service(service, read.service);
            let entries = match decode_data_block(&read.bytes, self.limits) {
                Ok(value) => value,
                Err(error) => return store_read_error(service, error),
            };
            for (internal, value) in entries {
                if internal.sequence > snapshot
                    || start.is_some_and(|start| internal.user_key.as_slice() < start)
                    || end.is_some_and(|end| internal.user_key.as_slice() >= end)
                {
                    continue;
                }
                visible
                    .entry(internal.user_key)
                    .and_modify(|current| {
                        if internal.sequence > current.2 {
                            *current = (internal.kind, value.clone(), internal.sequence);
                        }
                    })
                    .or_insert((internal.kind, value, internal.sequence));
            }
            if visible.len() >= limit {
                break;
            }
        }
        StoreRead {
            service,
            outcome: Ok(visible
                .into_iter()
                .take(limit)
                .map(|(key, (kind, value, sequence))| (key, kind, value, sequence))
                .collect()),
        }
    }
}

fn add_service(left: Duration, right: Duration) -> Duration {
    Duration::from_nanos(left.as_nanos().saturating_add(right.as_nanos()))
}

fn store_read_error<T>(service: Duration, error: StoreError) -> StoreRead<T> {
    StoreRead {
        service,
        outcome: Err(error),
    }
}

fn store_read_block_error<T>(service: Duration, error: crate::BlockReadError) -> StoreRead<T> {
    store_read_error(add_service(service, error.service), error.error)
}

impl SstV2Table {
    pub fn encode(
        entries: Vec<(InternalKey, Vec<u8>)>,
        limits: SstV2Limits,
    ) -> Result<Vec<u8>, StoreError> {
        validate_entries(&entries, limits)?;
        let mut output = Vec::new();
        let mut blocks = Vec::new();
        let mut cursor = 0_usize;
        while cursor < entries.len() {
            let (block, used, last) = encode_data_block(&entries[cursor..], limits)?;
            let offset = u64::try_from(output.len()).map_err(|_| StoreError::TooLarge {
                what: "v2 SST offset",
                size: output.len(),
                max: u32::MAX as usize,
            })?;
            let length = u32::try_from(block.len()).map_err(|_| StoreError::TooLarge {
                what: "v2 SST data block",
                size: block.len(),
                max: u32::MAX as usize,
            })?;
            output.extend_from_slice(&block);
            blocks.push(IndexRecord {
                last: last.clone(),
                offset,
                length,
            });
            cursor = cursor
                .checked_add(used)
                .ok_or(StoreError::Corrupt("v2 SST entry count overflow"))?;
        }
        if blocks.is_empty() {
            return Err(StoreError::InvalidInput("v2 SST must not be empty"));
        }
        let index_offset = u64::try_from(output.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST index offset",
            size: output.len(),
            max: u32::MAX as usize,
        })?;
        let index = encode_index_block(&blocks, limits)?;
        let index_length = u32::try_from(index.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST index block",
            size: index.len(),
            max: u32::MAX as usize,
        })?;
        output.extend_from_slice(&index);
        let bloom_offset = u64::try_from(output.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST bloom offset",
            size: output.len(),
            max: u32::MAX as usize,
        })?;
        let bloom_bits = build_bloom(entries.iter().map(|(key, _)| key.user_key.as_slice()))?;
        let bloom = encode_bloom_block(&bloom_bits)?;
        let bloom_length = u32::try_from(bloom.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST bloom block",
            size: bloom.len(),
            max: u32::MAX as usize,
        })?;
        output.extend_from_slice(&bloom);
        let entry_count = u64::try_from(entries.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST entry count",
            size: entries.len(),
            max: u32::MAX as usize,
        })?;
        let footer = encode_footer(SstV2Meta {
            index_offset,
            index_length,
            bloom_offset,
            bloom_length,
            entry_count,
        });
        output.extend_from_slice(&footer);
        if output.len() > limits.max_file_bytes {
            return Err(StoreError::TooLarge {
                what: "v2 SST file",
                size: output.len(),
                max: limits.max_file_bytes,
            });
        }
        Ok(output)
    }

    pub fn decode(bytes: &[u8], limits: SstV2Limits) -> Result<Self, StoreError> {
        if !limits.valid() {
            return Err(StoreError::InvalidInput("invalid v2 SST limits"));
        }
        if bytes.len() > limits.max_file_bytes {
            return Err(StoreError::TooLarge {
                what: "v2 SST file",
                size: bytes.len(),
                max: limits.max_file_bytes,
            });
        }
        if bytes.len() < SST_V2_FOOTER_BYTES {
            return Err(StoreError::Corrupt("v2 SST footer is truncated"));
        }
        let footer_offset = bytes.len() - SST_V2_FOOTER_BYTES;
        let meta = decode_footer(&bytes[footer_offset..])?;
        let index_start = as_usize(meta.index_offset, "v2 SST index offset")?;
        let index_end = index_start
            .checked_add(usize::try_from(meta.index_length).unwrap_or(usize::MAX))
            .ok_or(StoreError::Corrupt("v2 SST index range"))?;
        let bloom_start = as_usize(meta.bloom_offset, "v2 SST bloom offset")?;
        let bloom_end = bloom_start
            .checked_add(usize::try_from(meta.bloom_length).unwrap_or(usize::MAX))
            .ok_or(StoreError::Corrupt("v2 SST bloom range"))?;
        if index_start == 0
            || index_end > footer_offset
            || bloom_start < index_end
            || bloom_end != footer_offset
        {
            return Err(StoreError::Corrupt("v2 SST footer ranges"));
        }
        let records = decode_index_block(&bytes[index_start..index_end], limits)?;
        if records.is_empty() {
            return Err(StoreError::Corrupt("v2 SST has no index records"));
        }
        let bloom_bits =
            decode_bloom_block(&bytes[bloom_start..bloom_end], meta.entry_count, limits)?;
        let expected_entries = usize::try_from(meta.entry_count)
            .map_err(|_| StoreError::Corrupt("v2 SST entry count"))?;
        if expected_entries == 0 || expected_entries > limits.max_entries {
            return Err(StoreError::Corrupt("v2 SST entry count"));
        }
        let mut next_offset = 0_usize;
        let mut entries = Vec::with_capacity(expected_entries);
        let mut previous_last: Option<InternalKey> = None;
        for record in &records {
            let offset = as_usize(record.offset, "v2 SST data offset")?;
            let end = offset
                .checked_add(usize::try_from(record.length).unwrap_or(usize::MAX))
                .ok_or(StoreError::Corrupt("v2 SST data range"))?;
            if offset != next_offset || end > index_start || record.length == 0 {
                return Err(StoreError::Corrupt("v2 SST data block range"));
            }
            let decoded = decode_data_block(&bytes[offset..end], limits)?;
            let Some((last, _)) = decoded.last() else {
                return Err(StoreError::Corrupt("v2 SST empty data block"));
            };
            let last = last.clone();
            if last != record.last {
                return Err(StoreError::Corrupt("v2 SST index separator"));
            }
            if previous_last
                .as_ref()
                .is_some_and(|previous| previous >= &last)
            {
                return Err(StoreError::Corrupt("v2 SST index order"));
            }
            previous_last = Some(last);
            if entries.len().saturating_add(decoded.len()) > expected_entries {
                return Err(StoreError::Corrupt("v2 SST entry count"));
            }
            entries.extend(decoded);
            next_offset = end;
        }
        if next_offset != index_start || entries.len() != expected_entries {
            return Err(StoreError::Corrupt("v2 SST entry count"));
        }
        validate_entries(&entries, limits)?;
        Ok(Self {
            entries,
            meta,
            bloom_bits,
        })
    }

    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        bloom_may_contain(&self.bloom_bits, key)
    }

    #[must_use]
    pub fn get(&self, key: &[u8], snapshot: u64) -> Option<(ValueKind, Vec<u8>, u64)> {
        if !self.may_contain(key) {
            return None;
        }
        self.entries
            .iter()
            .filter(|(internal, _)| internal.user_key == key && internal.sequence <= snapshot)
            .max_by_key(|(internal, _)| internal.sequence)
            .map(|(internal, value)| (internal.kind, value.clone(), internal.sequence))
    }
}

/// Return the independently validated footer checksum from one complete v2
/// table. Manifest metadata records this value so recovery can reject a file
/// whose identity/range looks plausible but whose table footer differs.
pub fn sst_v2_footer_crc32c(bytes: &[u8]) -> Result<u32, StoreError> {
    if bytes.len() < SST_V2_FOOTER_BYTES {
        return Err(StoreError::Corrupt("v2 SST footer is truncated"));
    }
    let footer = &bytes[bytes.len() - SST_V2_FOOTER_BYTES..];
    let _ = decode_footer(footer)?;
    Ok(u32::from_le_bytes(
        footer[34..38]
            .try_into()
            .map_err(|_| StoreError::Corrupt("v2 SST footer"))?,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IndexRecord {
    last: InternalKey,
    offset: u64,
    length: u32,
}

fn validate_entries(
    entries: &[(InternalKey, Vec<u8>)],
    limits: SstV2Limits,
) -> Result<(), StoreError> {
    if !limits.valid() {
        return Err(StoreError::InvalidInput("invalid v2 SST limits"));
    }
    if entries.is_empty() {
        return Err(StoreError::InvalidInput("v2 SST must not be empty"));
    }
    if entries.len() > limits.max_entries {
        return Err(StoreError::TooLarge {
            what: "v2 SST entry count",
            size: entries.len(),
            max: limits.max_entries,
        });
    }
    let mut previous: Option<&InternalKey> = None;
    for (key, value) in entries {
        if key.user_key.is_empty() || key.user_key.len() > limits.max_key_bytes {
            return Err(StoreError::InvalidInput("v2 SST key size"));
        }
        if value.len() > limits.max_value_bytes {
            return Err(StoreError::TooLarge {
                what: "v2 SST value",
                size: value.len(),
                max: limits.max_value_bytes,
            });
        }
        if previous.is_some_and(|prior| prior >= key) {
            return Err(StoreError::InvalidInput("v2 SST internal key order"));
        }
        previous = Some(key);
    }
    Ok(())
}

fn encode_data_block(
    entries: &[(InternalKey, Vec<u8>)],
    limits: SstV2Limits,
) -> Result<(Vec<u8>, usize, InternalKey), StoreError> {
    let mut body = Vec::new();
    let mut restarts = Vec::new();
    let mut previous = Vec::new();
    let mut used = 0_usize;
    for (key, value) in entries {
        let restart = used.is_multiple_of(RESTART_INTERVAL);
        let shared = if restart {
            0
        } else {
            common_prefix(&previous, &key.user_key)
        };
        let encoded = encode_data_entry(key, value, shared)?;
        let projected = body
            .len()
            .checked_add(encoded.len())
            .and_then(|size| size.checked_add(restarts.len().saturating_add(1).saturating_mul(4)))
            .and_then(|size| size.checked_add(BLOCK_TRAILER_FIXED_BYTES))
            .ok_or(StoreError::Corrupt("v2 SST block size"))?;
        if used > 0 && projected > SST_V2_TARGET_BLOCK_BYTES {
            break;
        }
        if restart {
            restarts.push(u32::try_from(body.len()).map_err(|_| StoreError::TooLarge {
                what: "v2 SST restart offset",
                size: body.len(),
                max: u32::MAX as usize,
            })?);
        }
        body.extend_from_slice(&encoded);
        previous = key.user_key.clone();
        used = used.saturating_add(1);
    }
    if used == 0 {
        return Err(StoreError::Corrupt("v2 SST cannot encode entry"));
    }
    let block = finish_block(body, &restarts)?;
    if block.len() > limits.max_file_bytes {
        return Err(StoreError::TooLarge {
            what: "v2 SST data block",
            size: block.len(),
            max: limits.max_file_bytes,
        });
    }
    Ok((block, used, entries[used - 1].0.clone()))
}

fn encode_data_entry(
    key: &InternalKey,
    value: &[u8],
    shared: usize,
) -> Result<Vec<u8>, StoreError> {
    let tail = key
        .user_key
        .get(shared..)
        .ok_or(StoreError::Corrupt("v2 SST prefix"))?;
    let mut out = Vec::with_capacity(21 + tail.len() + value.len());
    push_u32(
        &mut out,
        u32::try_from(shared).map_err(|_| StoreError::Corrupt("v2 SST prefix"))?,
    );
    push_u32(
        &mut out,
        u32::try_from(tail.len()).map_err(|_| StoreError::Corrupt("v2 SST key length"))?,
    );
    push_u32(
        &mut out,
        u32::try_from(value.len()).map_err(|_| StoreError::Corrupt("v2 SST value length"))?,
    );
    out.extend_from_slice(tail);
    push_u64(&mut out, key.sequence);
    out.push(encode_kind(key.kind));
    out.extend_from_slice(value);
    Ok(out)
}

fn encode_index_block(records: &[IndexRecord], limits: SstV2Limits) -> Result<Vec<u8>, StoreError> {
    let mut body = Vec::new();
    let mut previous: Option<&InternalKey> = None;
    for record in records {
        if record.last.user_key.len() > limits.max_key_bytes
            || previous.is_some_and(|prior| prior >= &record.last)
        {
            return Err(StoreError::InvalidInput("v2 SST index order"));
        }
        push_bytes(&mut body, &record.last.user_key)?;
        push_u64(&mut body, record.last.sequence);
        body.push(encode_kind(record.last.kind));
        push_u64(&mut body, record.offset);
        push_u32(&mut body, record.length);
        previous = Some(&record.last);
    }
    finish_block(body, &[])
}

fn decode_data_block(
    bytes: &[u8],
    limits: SstV2Limits,
) -> Result<Vec<(InternalKey, Vec<u8>)>, StoreError> {
    let (body_end, restarts) = parse_block_trailer(bytes)?;
    if restarts.is_empty() || restarts[0] != 0 {
        return Err(StoreError::Corrupt("v2 SST restart offsets"));
    }
    let body = &bytes[..body_end];
    let mut cursor = 0_usize;
    let mut previous_key = Vec::new();
    let mut entries = Vec::new();
    let mut restart_cursor = 0_usize;
    while cursor < body.len() {
        let entry_offset = cursor;
        while restart_cursor < restarts.len() && (restarts[restart_cursor] as usize) < entry_offset
        {
            restart_cursor += 1;
        }
        let restart = restarts
            .get(restart_cursor)
            .is_some_and(|offset| *offset as usize == entry_offset);
        let shared = usize::try_from(take_u32(body, &mut cursor)?).unwrap_or(usize::MAX);
        let unshared = usize::try_from(take_u32(body, &mut cursor)?).unwrap_or(usize::MAX);
        let value_len = usize::try_from(take_u32(body, &mut cursor)?).unwrap_or(usize::MAX);
        if shared > previous_key.len()
            || unshared > limits.max_key_bytes
            || value_len > limits.max_value_bytes
            || (restart && shared != 0)
            || restart != entries.len().is_multiple_of(RESTART_INTERVAL)
        {
            return Err(StoreError::Corrupt("v2 SST prefix entry"));
        }
        let tail = take_slice(body, &mut cursor, unshared)?;
        let mut user_key = previous_key[..shared].to_vec();
        user_key.extend_from_slice(tail);
        if user_key.is_empty() || user_key.len() > limits.max_key_bytes {
            return Err(StoreError::Corrupt("v2 SST key length"));
        }
        let sequence = take_u64(body, &mut cursor)?;
        let kind = decode_kind(take_u8(body, &mut cursor)?)?;
        let value = take_slice(body, &mut cursor, value_len)?.to_vec();
        let internal = InternalKey::new(user_key.clone(), sequence, kind);
        if entries
            .last()
            .is_some_and(|(previous, _): &(InternalKey, Vec<u8>)| previous >= &internal)
        {
            return Err(StoreError::Corrupt("v2 SST internal key order"));
        }
        entries.push((internal, value));
        if restart {
            restart_cursor = restart_cursor.saturating_add(1);
        }
        if entries.len() > limits.max_entries {
            return Err(StoreError::Corrupt("v2 SST entry count"));
        }
        previous_key = user_key;
    }
    if cursor != body.len() || restart_cursor != restarts.len() {
        return Err(StoreError::Corrupt("v2 SST restart offsets"));
    }
    Ok(entries)
}

fn decode_index_block(bytes: &[u8], limits: SstV2Limits) -> Result<Vec<IndexRecord>, StoreError> {
    let (body_end, restarts) = parse_block_trailer(bytes)?;
    if !restarts.is_empty() {
        return Err(StoreError::Corrupt("v2 SST index restart count"));
    }
    let body = &bytes[..body_end];
    let mut cursor = 0_usize;
    let mut records = Vec::new();
    let mut previous: Option<InternalKey> = None;
    while cursor < body.len() {
        let user_key = take_bytes(body, &mut cursor, limits.max_key_bytes)?;
        if user_key.is_empty() {
            return Err(StoreError::Corrupt("v2 SST index key"));
        }
        let sequence = take_u64(body, &mut cursor)?;
        let kind = decode_kind(take_u8(body, &mut cursor)?)?;
        let offset = take_u64(body, &mut cursor)?;
        let length = take_u32(body, &mut cursor)?;
        let last = InternalKey::new(user_key, sequence, kind);
        if previous.as_ref().is_some_and(|prior| prior >= &last) {
            return Err(StoreError::Corrupt("v2 SST index order"));
        }
        previous = Some(last.clone());
        records.push(IndexRecord {
            last,
            offset,
            length,
        });
        if records.len() > limits.max_entries {
            return Err(StoreError::Corrupt("v2 SST index count"));
        }
    }
    Ok(records)
}

fn build_bloom<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Result<Vec<u8>, StoreError> {
    let keys: Vec<&[u8]> = keys.collect();
    if keys.is_empty() {
        return Err(StoreError::InvalidInput("v2 SST bloom is empty"));
    }
    let bits = keys
        .len()
        .saturating_mul(10)
        .max(64)
        .checked_add(7)
        .ok_or(StoreError::Corrupt("v2 SST bloom size"))?
        / 8
        * 8;
    let mut bytes = vec![0; bits / 8];
    for key in keys {
        bloom_insert(&mut bytes, key);
    }
    Ok(bytes)
}

fn encode_bloom_block(bits: &[u8]) -> Result<Vec<u8>, StoreError> {
    let bit_len = bits
        .len()
        .checked_mul(8)
        .ok_or(StoreError::Corrupt("v2 SST bloom size"))?;
    let mut body = Vec::with_capacity(5 + bits.len());
    push_u32(
        &mut body,
        u32::try_from(bit_len).map_err(|_| StoreError::TooLarge {
            what: "v2 SST bloom bits",
            size: bit_len,
            max: u32::MAX as usize,
        })?,
    );
    body.push(BLOOM_PROBES);
    body.extend_from_slice(bits);
    finish_block(body, &[])
}

fn decode_bloom_block(
    bytes: &[u8],
    entry_count: u64,
    limits: SstV2Limits,
) -> Result<Vec<u8>, StoreError> {
    let (body_end, restarts) = parse_block_trailer(bytes)?;
    if !restarts.is_empty() || body_end < 5 {
        return Err(StoreError::Corrupt("v2 SST bloom trailer"));
    }
    let body = &bytes[..body_end];
    let bit_len = u32::from_le_bytes(body[..4].try_into().expect("bloom length")) as usize;
    if body[4] != BLOOM_PROBES || bit_len < 64 || !bit_len.is_multiple_of(8) {
        return Err(StoreError::Corrupt("v2 SST bloom parameters"));
    }
    let byte_len = bit_len / 8;
    if body.len() != 5_usize.saturating_add(byte_len) || byte_len > limits.max_file_bytes {
        return Err(StoreError::Corrupt("v2 SST bloom length"));
    }
    let count =
        usize::try_from(entry_count).map_err(|_| StoreError::Corrupt("v2 SST bloom count"))?;
    let expected = count
        .saturating_mul(10)
        .max(64)
        .checked_add(7)
        .ok_or(StoreError::Corrupt("v2 SST bloom count"))?
        / 8
        * 8;
    if bit_len != expected {
        return Err(StoreError::Corrupt("v2 SST bloom size"));
    }
    Ok(body[5..].to_vec())
}

fn finish_block(mut body: Vec<u8>, restarts: &[u32]) -> Result<Vec<u8>, StoreError> {
    for offset in restarts {
        push_u32(&mut body, *offset);
    }
    push_u32(
        &mut body,
        u32::try_from(restarts.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST restart count",
            size: restarts.len(),
            max: u32::MAX as usize,
        })?,
    );
    body.push(0); // only uncompressed blocks exist in v2.
    let checksum = crc32c(&body);
    push_u32(&mut body, checksum);
    Ok(body)
}

fn parse_block_trailer(bytes: &[u8]) -> Result<(usize, Vec<u32>), StoreError> {
    if bytes.len() < BLOCK_TRAILER_FIXED_BYTES {
        return Err(StoreError::Corrupt("v2 SST block trailer"));
    }
    let checksum_offset = bytes.len() - 4;
    let expected = u32::from_le_bytes(bytes[checksum_offset..].try_into().expect("block checksum"));
    if crc32c(&bytes[..checksum_offset]) != expected {
        return Err(StoreError::Corrupt("v2 SST block checksum"));
    }
    if bytes[checksum_offset - 1] != 0 {
        return Err(StoreError::Corrupt("v2 SST unsupported block type"));
    }
    let count_offset = checksum_offset - 5;
    let count = u32::from_le_bytes(
        bytes[count_offset..checksum_offset - 1]
            .try_into()
            .expect("restart count"),
    ) as usize;
    let restart_bytes = count
        .checked_mul(4)
        .ok_or(StoreError::Corrupt("v2 SST restart count"))?;
    let body_end = count_offset
        .checked_sub(restart_bytes)
        .ok_or(StoreError::Corrupt("v2 SST restart count"))?;
    let mut restarts = Vec::with_capacity(count);
    for chunk in bytes[body_end..count_offset].chunks_exact(4) {
        restarts.push(u32::from_le_bytes(
            chunk.try_into().expect("restart offset"),
        ));
    }
    if restarts.windows(2).any(|pair| pair[0] >= pair[1])
        || restarts.iter().any(|offset| (*offset as usize) >= body_end)
    {
        return Err(StoreError::Corrupt("v2 SST restart offsets"));
    }
    Ok((body_end, restarts))
}

fn encode_footer(meta: SstV2Meta) -> [u8; SST_V2_FOOTER_BYTES] {
    let mut footer = [0_u8; SST_V2_FOOTER_BYTES];
    footer[0..8].copy_from_slice(&meta.index_offset.to_le_bytes());
    footer[8..12].copy_from_slice(&meta.index_length.to_le_bytes());
    footer[12..20].copy_from_slice(&meta.bloom_offset.to_le_bytes());
    footer[20..24].copy_from_slice(&meta.bloom_length.to_le_bytes());
    footer[24..32].copy_from_slice(&meta.entry_count.to_le_bytes());
    footer[32..34].copy_from_slice(&SST_V2_VERSION.to_le_bytes());
    footer[38..42].copy_from_slice(b"CCST");
    let checksum = crc32c(&footer);
    footer[34..38].copy_from_slice(&checksum.to_le_bytes());
    footer
}

fn decode_footer(bytes: &[u8]) -> Result<SstV2Meta, StoreError> {
    if bytes.len() != SST_V2_FOOTER_BYTES || &bytes[38..42] != b"CCST" {
        return Err(StoreError::Corrupt("v2 SST footer"));
    }
    if u16::from_le_bytes(bytes[32..34].try_into().expect("footer version")) != SST_V2_VERSION {
        return Err(StoreError::Corrupt("v2 SST version"));
    }
    let expected = u32::from_le_bytes(bytes[34..38].try_into().expect("footer checksum"));
    let mut checked = bytes.to_vec();
    checked[34..38].fill(0);
    if crc32c(&checked) != expected {
        return Err(StoreError::Corrupt("v2 SST footer checksum"));
    }
    Ok(SstV2Meta {
        index_offset: u64::from_le_bytes(bytes[0..8].try_into().expect("index offset")),
        index_length: u32::from_le_bytes(bytes[8..12].try_into().expect("index length")),
        bloom_offset: u64::from_le_bytes(bytes[12..20].try_into().expect("bloom offset")),
        bloom_length: u32::from_le_bytes(bytes[20..24].try_into().expect("bloom length")),
        entry_count: u64::from_le_bytes(bytes[24..32].try_into().expect("entry count")),
    })
}

fn bloom_insert(bits: &mut [u8], key: &[u8]) {
    let bit_len = bits.len() as u64 * 8;
    let (first, second) = bloom_hashes(key);
    for probe in 0..u64::from(BLOOM_PROBES) {
        let bit = first.wrapping_add(probe.wrapping_mul(second)) % bit_len;
        bits[bit as usize / 8] |= 1 << (bit as usize % 8);
    }
}

fn bloom_may_contain(bits: &[u8], key: &[u8]) -> bool {
    if bits.is_empty() {
        return false;
    }
    let bit_len = bits.len() as u64 * 8;
    let (first, second) = bloom_hashes(key);
    (0..u64::from(BLOOM_PROBES)).all(|probe| {
        let bit = first.wrapping_add(probe.wrapping_mul(second)) % bit_len;
        bits[bit as usize / 8] & (1 << (bit as usize % 8)) != 0
    })
}

fn bloom_hashes(key: &[u8]) -> (u64, u64) {
    let first = cc_core::fnv1a(key);
    let mut prefixed = Vec::with_capacity(key.len().saturating_add(1));
    prefixed.push(0xff);
    prefixed.extend_from_slice(key);
    (first, cc_core::fnv1a(&prefixed) | 1)
}

fn decode_kind(tag: u8) -> Result<ValueKind, StoreError> {
    match tag {
        0 => Ok(ValueKind::Delete),
        1 => Ok(ValueKind::Put),
        _ => Err(StoreError::Corrupt("v2 SST value kind")),
    }
}

const fn encode_kind(kind: ValueKind) -> u8 {
    match kind {
        ValueKind::Delete => 0,
        ValueKind::Put => 1,
    }
}

fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(a, b)| a == b).count()
}

fn as_usize(value: u64, reason: &'static str) -> Result<usize, StoreError> {
    usize::try_from(value).map_err(|_| StoreError::Corrupt(reason))
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), StoreError> {
    push_u32(
        out,
        u32::try_from(bytes.len()).map_err(|_| StoreError::TooLarge {
            what: "v2 SST byte field",
            size: bytes.len(),
            max: u32::MAX as usize,
        })?,
    );
    out.extend_from_slice(bytes);
    Ok(())
}

fn take_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], StoreError> {
    let end = cursor
        .checked_add(length)
        .ok_or(StoreError::Corrupt("v2 SST truncated field"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(StoreError::Corrupt("v2 SST truncated field"))?;
    *cursor = end;
    Ok(value)
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, StoreError> {
    Ok(take_slice(bytes, cursor, 1)?[0])
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, StoreError> {
    Ok(u32::from_le_bytes(
        take_slice(bytes, cursor, 4)?
            .try_into()
            .expect("bounded u32"),
    ))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, StoreError> {
    Ok(u64::from_le_bytes(
        take_slice(bytes, cursor, 8)?
            .try_into()
            .expect("bounded u64"),
    ))
}

fn take_bytes(bytes: &[u8], cursor: &mut usize, max: usize) -> Result<Vec<u8>, StoreError> {
    let length = usize::try_from(take_u32(bytes, cursor)?).unwrap_or(usize::MAX);
    if length > max {
        return Err(StoreError::Corrupt("v2 SST byte field length"));
    }
    Ok(take_slice(bytes, cursor, length)?.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryBlockSource;

    fn entries() -> Vec<(InternalKey, Vec<u8>)> {
        vec![
            (
                InternalKey::new(b"apple".to_vec(), 3, ValueKind::Put),
                b"red".to_vec(),
            ),
            (
                InternalKey::new(b"apple".to_vec(), 2, ValueKind::Delete),
                Vec::new(),
            ),
            (
                InternalKey::new(b"banana".to_vec(), 1, ValueKind::Put),
                b"yellow".to_vec(),
            ),
        ]
    }

    #[test]
    fn v2_round_trip_preserves_internal_order_and_bloom() {
        let bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        let table = SstV2Table::decode(&bytes, SstV2Limits::default()).expect("decode");
        assert_eq!(table.entries, entries());
        assert!(table.may_contain(b"apple"));
        assert_eq!(
            table.get(b"apple", 3),
            Some((ValueKind::Put, b"red".to_vec(), 3))
        );
    }

    #[test]
    fn golden_sst_v2_vectors() {
        let bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join("");
        assert_eq!(
            hex,
            "0000000005000000030000006170706c6503000000000000000172656405000000000000000000000002000000000000000000000000060000000600000062616e616e6101000000000000000179656c6c6f7700000000010000000040cce5500600000062616e616e6101000000000000000100000000000000006000000000000000007e915c2940000000074204a100030844900000000000db935c8f600000000000000028000000880000000000000016000000030000000000000002001a80df0a43435354"
        );
    }

    #[test]
    fn trap_block_index_seek_matches_linear_scan() {
        let entries = (0_u16..160)
            .map(|index| {
                (
                    InternalKey::new(format!("key-{index:03}").into_bytes(), 1, ValueKind::Put),
                    vec![index as u8; 48],
                )
            })
            .collect::<Vec<_>>();
        let bytes = SstV2Table::encode(entries.clone(), SstV2Limits::default()).expect("encode");
        let table = SstV2Table::decode(&bytes, SstV2Limits::default()).expect("decode");
        for (internal, expected) in entries {
            assert_eq!(
                table.get(&internal.user_key, internal.sequence),
                Some((internal.kind, expected, internal.sequence))
            );
        }
    }

    #[test]
    fn trap_out_of_order_internal_keys_are_rejected() {
        let mut invalid = entries();
        invalid.swap(0, 1);
        assert!(matches!(
            SstV2Table::encode(invalid, SstV2Limits::default()),
            Err(StoreError::InvalidInput("v2 SST internal key order"))
        ));
    }

    #[test]
    fn trap_oversized_entry_gets_its_own_bounded_block() {
        let value = vec![7; SST_V2_TARGET_BLOCK_BYTES + 1];
        let entries = vec![(
            (InternalKey::new(b"large".to_vec(), 1, ValueKind::Put)),
            value.clone(),
        )];
        let bytes = SstV2Table::encode(entries.clone(), SstV2Limits::default()).expect("encode");
        let table = SstV2Table::decode(&bytes, SstV2Limits::default()).expect("decode");
        assert!(table.meta.index_offset as usize > SST_V2_TARGET_BLOCK_BYTES);
        assert_eq!(table.entries, entries);
    }

    #[test]
    fn trap_bloom_negative_avoids_data_read() {
        let table = SstV2Table::decode(
            &SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode"),
            SstV2Limits::default(),
        )
        .expect("decode");
        let missing = (0_u16..10_000)
            .map(|index| format!("missing-{index}").into_bytes())
            .find(|key| !table.may_contain(key))
            .expect("deterministic bloom negative");
        assert!(!table.may_contain(&missing));
    }

    #[test]
    fn trap_sync_read_delays_its_own_reply() {
        let bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        let mut source = MemoryBlockSource {
            service_per_read: Duration::from_nanos(7),
            ..MemoryBlockSource::default()
        };
        let file = FileId::Sst { file_no: 7 };
        source.insert(file, bytes.clone());
        let opened = SstV2Reader::open(
            &mut source,
            file,
            bytes.len() as u64,
            SstV2Limits::default(),
        );
        assert_eq!(opened.service, Duration::from_nanos(21));
        let reader = opened.outcome.expect("open reader");
        let read = reader.get(&mut source, b"apple", 3);
        assert_eq!(read.service, Duration::from_nanos(7));
        assert_eq!(
            read.outcome.expect("read"),
            Some((ValueKind::Put, b"red".to_vec(), 3))
        );
    }

    #[test]
    fn trap_short_block_read_fails_closed() {
        let bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        let mut source = MemoryBlockSource::default();
        let file = FileId::Sst { file_no: 8 };
        source.insert(file, bytes[..bytes.len() - 1].to_vec());
        let opened = SstV2Reader::open(
            &mut source,
            file,
            bytes.len() as u64,
            SstV2Limits::default(),
        );
        assert!(matches!(
            opened.outcome,
            Err(StoreError::InvalidInput("block range"))
        ));
    }

    #[test]
    fn trap_block_crc_detects_bit_flip() {
        let mut bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        bytes[3] ^= 1;
        assert!(matches!(
            SstV2Table::decode(&bytes, SstV2Limits::default()),
            Err(StoreError::Corrupt(_))
        ));
    }

    #[test]
    fn trap_footer_crc_and_offsets_are_checked() {
        let mut bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        let footer = bytes.len() - SST_V2_FOOTER_BYTES;
        bytes[footer] ^= 1;
        assert!(matches!(
            SstV2Table::decode(&bytes, SstV2Limits::default()),
            Err(StoreError::Corrupt(_))
        ));
    }

    #[test]
    fn trap_restart_prefix_cannot_escape_previous_key() {
        let mut bytes = SstV2Table::encode(entries(), SstV2Limits::default()).expect("encode");
        // The first entry's shared-prefix word lies at offset zero. Rebuild
        // the data-block CRC so this specifically tests prefix validation.
        bytes[0..4].copy_from_slice(&1_u32.to_le_bytes());
        let footer = bytes.len() - SST_V2_FOOTER_BYTES;
        let index_offset =
            u64::from_le_bytes(bytes[footer..footer + 8].try_into().expect("offset")) as usize;
        let checksum = index_offset - 4;
        let block_checksum = crc32c(&bytes[..checksum]);
        bytes[checksum..index_offset].copy_from_slice(&block_checksum.to_le_bytes());
        assert!(matches!(
            SstV2Table::decode(&bytes, SstV2Limits::default()),
            Err(StoreError::Corrupt(_))
        ));
    }
}
