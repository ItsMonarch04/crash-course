// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "A deterministic, single-keyspace LSM store built on the Crash Course WAL."]

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{Dec, DecodeError, Duration, Enc, LogIndex, MAX_CODEC_BYTES, Term, Time, crc32c};
use cc_env::FileId;
use cc_wal::{RecordType, Wal, WalConfig, WalError};

mod manifest_v2;
mod sst_v2;
pub use manifest_v2::{
    MANIFEST_V2_MAGIC, MANIFEST_V2_VERSION, META_V2_VERSION, ManifestBoot, ManifestCheckpoint,
    ManifestEditV2, ManifestFile, ManifestMetaV2, ManifestV2, decode_manifest_v2, decode_meta_v2,
    encode_manifest_v2, encode_meta_v2, select_manifest_generation, validate_checkpoint_authority,
};
pub use sst_v2::{
    SST_V2_FOOTER_BYTES, SST_V2_TARGET_BLOCK_BYTES, SST_V2_VERSION, SstV2Limits, SstV2Meta,
    SstV2Reader, SstV2Table, sst_v2_footer_crc32c,
};

pub const FORMAT_VERSION: u16 = 1;
pub const SST_MAGIC: u32 = u32::from_le_bytes(*b"CCST");
pub const META_MAGIC: u32 = u32::from_le_bytes(*b"CCMT");
pub const DEFAULT_MEMTABLE_BYTES: usize = 4 * 1024 * 1024;
/// A host must durably raise its CCID storage-reader floor before it executes
/// a plan that can create any v2 storage byte.
pub const STORAGE_V2_MIN_READER: u16 = 2;

/// The Raft position proven durable by one atomic state-machine batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreWatermark {
    pub index: LogIndex,
    pub term: Term,
    pub last_leader_time: Time,
}

/// The committed Raft entry class carried by each derived-store apply
/// record.  Config and no-op entries intentionally produce records even when
/// their logical write batch is empty, so the durable applied cursor never
/// skips an index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StoreEntryKind {
    App = 1,
    Config = 2,
    Noop = 3,
}

impl StoreEntryKind {
    fn decode(tag: u8) -> Result<Self, StoreError> {
        match tag {
            1 => Ok(Self::App),
            2 => Ok(Self::Config),
            3 => Ok(Self::Noop),
            _ => Err(StoreError::Corrupt("store apply entry kind")),
        }
    }
}

/// One logical key mutation prepared by the composite state-machine apply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Opaque, namespaced state-machine metadata stored beside key mutations.
/// `cc-store` does not interpret TTL/session/admin namespaces; it merely
/// publishes and replays their exact bounded bytes atomically with the key
/// batch and applied watermark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreMetadataEdit {
    Upsert {
        namespace: u8,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        namespace: u8,
        key: Vec<u8>,
    },
}

/// A complete derived-store transition.  The record is appended once, then
/// both its keys and its Raft/time watermark become visible together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreApplyBatch {
    pub entry_kind: StoreEntryKind,
    pub watermark: StoreWatermark,
    pub mutations: Vec<StoreMutation>,
    pub metadata: Vec<StoreMetadataEdit>,
    pub canonical_command: Vec<u8>,
    pub cached_reply: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreApplyReceipt {
    pub first_sequence: u64,
    pub last_sequence: u64,
}

/// A validated transition that is invisible until its host WAL frame has
/// completed write+fsync.  The deterministic core can discard this value on
/// I/O failure without changing the live store.
#[derive(Clone)]
pub struct PreparedStoreApply {
    wal_frame: Vec<u8>,
    next: Store,
}

impl PreparedStoreApply {
    #[must_use]
    pub fn wal_frame(&self) -> &[u8] {
        &self.wal_frame
    }

    #[must_use]
    pub fn into_store(self) -> Store {
        self.next
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredStoreWal {
    pub records: Vec<StoreApplyBatch>,
    pub bytes_consumed: u64,
    pub torn_tail_truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ValueKind {
    Put = 1,
    Delete = 2,
}

impl ValueKind {
    pub(crate) fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Internal keys sort by user key ascending and sequence descending.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub sequence: u64,
    pub kind: ValueKind,
}

impl InternalKey {
    #[must_use]
    pub fn new(user_key: Vec<u8>, sequence: u64, kind: ValueKind) -> Self {
        Self {
            user_key,
            sequence,
            kind,
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| internal_kind_order(self.kind).cmp(&internal_kind_order(other.kind)))
    }
}

const fn internal_kind_order(kind: ValueKind) -> u8 {
    match kind {
        ValueKind::Delete => 0,
        ValueKind::Put => 1,
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreConfig {
    pub memtable_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub wal: WalConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            memtable_bytes: DEFAULT_MEMTABLE_BYTES,
            max_key_bytes: 4 * 1024,
            max_value_bytes: 1024 * 1024,
            wal: WalConfig::default(),
        }
    }
}

impl StoreConfig {
    /// Rebuild a complete store configuration from bounded host-neutral
    /// fields without forcing callers above the store boundary to name the
    /// WAL implementation type.
    #[must_use]
    pub const fn from_parts(
        memtable_bytes: usize,
        max_key_bytes: usize,
        max_value_bytes: usize,
        wal_segment_size: usize,
        wal_max_record_size: usize,
    ) -> Self {
        Self {
            memtable_bytes,
            max_key_bytes,
            max_value_bytes,
            wal: WalConfig {
                segment_size: wal_segment_size,
                max_record_size: wal_max_record_size,
            },
        }
    }
}

/// Deterministic levelled-compaction policy.  These are host-neutral byte
/// targets; admission and open-file limits remain in `HostLimits`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionConfig {
    pub max_levels: u8,
    pub l0_trigger_files: u64,
    pub level1_target_bytes: u64,
    pub level_multiplier: u64,
    pub output_target_bytes: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            max_levels: 7,
            l0_trigger_files: 4,
            level1_target_bytes: 8 * 1024 * 1024,
            level_multiplier: 10,
            output_target_bytes: 8 * 1024 * 1024,
        }
    }
}

impl CompactionConfig {
    pub fn validate(self) -> Result<Self, StoreError> {
        if self.max_levels < 2
            || self.l0_trigger_files == 0
            || self.level1_target_bytes == 0
            || self.level_multiplier < 2
            || self.output_target_bytes == 0
        {
            return Err(StoreError::InvalidInput("compaction configuration"));
        }
        let mut target = self.level1_target_bytes;
        for _ in 2..self.max_levels {
            target = target
                .checked_mul(self.level_multiplier)
                .ok_or(StoreError::InvalidInput("compaction target overflow"))?;
        }
        Ok(self)
    }

    fn target_for_level(self, level: u8) -> Result<u64, StoreError> {
        if level == 0 {
            return Ok(self.l0_trigger_files);
        }
        if level >= self.max_levels {
            return Err(StoreError::InvalidInput("compaction level"));
        }
        let mut target = self.level1_target_bytes;
        for _ in 1..level {
            target = target
                .checked_mul(self.level_multiplier)
                .ok_or(StoreError::InvalidInput("compaction target overflow"))?;
        }
        Ok(target)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionBudget {
    pub max_entries: u64,
    pub max_input_bytes: u64,
    pub max_output_bytes: u64,
}

impl CompactionBudget {
    pub fn validate(self) -> Result<Self, StoreError> {
        if self.max_entries == 0 || self.max_input_bytes == 0 || self.max_output_bytes == 0 {
            return Err(StoreError::InvalidInput("compaction budget"));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionSelection {
    pub source_level: u8,
    pub output_level: u8,
    pub inputs: Vec<(u8, u64)>,
    pub smallest_user_key: Vec<u8>,
    pub largest_user_key: Vec<u8>,
}

/// Compare two exact compaction scores `actual/target`. A greater result means
/// the left level has the higher score. Checked u128 multiplication prevents
/// both float nondeterminism and truncated integer ties.
pub fn compare_compaction_scores(
    actual_left: u64,
    target_left: u64,
    actual_right: u64,
    target_right: u64,
) -> Result<Ordering, StoreError> {
    if target_left == 0 || target_right == 0 {
        return Err(StoreError::InvalidInput("compaction score target"));
    }
    let left = u128::from(actual_left)
        .checked_mul(u128::from(target_right))
        .ok_or(StoreError::InvalidInput("compaction score overflow"))?;
    let right = u128::from(actual_right)
        .checked_mul(u128::from(target_left))
        .ok_or(StoreError::InvalidInput("compaction score overflow"))?;
    Ok(left.cmp(&right))
}

/// Select one level and close its inputs over every overlapping range. L0
/// begins with the lowest file number and repeatedly expands over L0 before
/// collecting the output level. Higher levels select one victim, then close
/// over the next level. Returned inputs always have canonical ordering.
pub fn select_compaction(
    files: &BTreeMap<u64, ManifestFile>,
    config: CompactionConfig,
) -> Result<Option<CompactionSelection>, StoreError> {
    let config = config.validate()?;
    let mut chosen: Option<(u8, u64, u64)> = None;
    for level in 0..config.max_levels.saturating_sub(1) {
        let mut level_files = files.values().filter(|file| file.level == level);
        let actual = if level == 0 {
            u64::try_from(level_files.count())
                .map_err(|_| StoreError::InvalidInput("compaction file count"))?
        } else {
            level_files
                .try_fold(0_u64, |sum, file| sum.checked_add(file.file_size))
                .ok_or(StoreError::InvalidInput("compaction level bytes"))?
        };
        let target = config.target_for_level(level)?;
        if actual < target {
            continue;
        }
        match chosen {
            None => chosen = Some((level, actual, target)),
            Some((best_level, best_actual, best_target)) => {
                let order = compare_compaction_scores(actual, target, best_actual, best_target)?;
                if order == Ordering::Greater || (order == Ordering::Equal && level < best_level) {
                    chosen = Some((level, actual, target));
                }
            }
        }
    }
    let Some((source_level, _, _)) = chosen else {
        return Ok(None);
    };
    let output_level = source_level + 1;
    let victim = files
        .values()
        .filter(|file| file.level == source_level)
        .min_by_key(|file| file.file_no)
        .ok_or(StoreError::Corrupt("compaction level is empty"))?;
    let mut selected = BTreeSet::from([(source_level, victim.file_no)]);
    let mut smallest = victim.smallest.user_key.clone();
    let mut largest = victim.largest.user_key.clone();
    loop {
        let mut changed = false;
        for file in files.values() {
            let eligible =
                file.level == output_level || (source_level == 0 && file.level == source_level);
            if eligible
                && file.smallest.user_key <= largest
                && file.largest.user_key >= smallest
                && selected.insert((file.level, file.file_no))
            {
                smallest = smallest.min(file.smallest.user_key.clone());
                largest = largest.max(file.largest.user_key.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    Ok(Some(CompactionSelection {
        source_level,
        output_level,
        inputs: selected.into_iter().collect(),
        smallest_user_key: smallest,
        largest_user_key: largest,
    }))
}

/// Split an already internal-key-sorted merge near the byte target, but only
/// at user-key boundaries. An oversized key family remains one bounded output.
pub type InternalEntry = (InternalKey, Vec<u8>);
pub type CompactionOutputs = Vec<Vec<InternalEntry>>;

pub fn split_compaction_output(
    entries: Vec<InternalEntry>,
    target_bytes: u64,
) -> Result<CompactionOutputs, StoreError> {
    if target_bytes == 0 {
        return Err(StoreError::InvalidInput("compaction output target"));
    }
    let mut outputs = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_u64;
    for entry in entries {
        let entry_bytes = u64::try_from(entry.0.user_key.len())
            .unwrap_or(u64::MAX)
            .checked_add(u64::try_from(entry.1.len()).unwrap_or(u64::MAX))
            .and_then(|bytes| bytes.checked_add(32))
            .ok_or(StoreError::InvalidInput("compaction entry bytes"))?;
        let changes_key = current
            .last()
            .is_some_and(|(key, _): &(InternalKey, Vec<u8>)| key.user_key != entry.0.user_key);
        if changes_key
            && !current.is_empty()
            && current_bytes.saturating_add(entry_bytes) > target_bytes
        {
            outputs.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes
            .checked_add(entry_bytes)
            .ok_or(StoreError::InvalidInput("compaction output bytes"))?;
        current.push(entry);
    }
    if !current.is_empty() {
        outputs.push(current);
    }
    Ok(outputs)
}

/// The exact tombstone safety predicate used by compaction.
#[must_use]
pub fn may_drop_tombstone(
    tombstone_sequence: u64,
    oldest_snapshot: Option<u64>,
    reaches_bottom_for_key: bool,
    unselected_lower_range_may_contain_key: bool,
) -> bool {
    oldest_snapshot.is_none_or(|snapshot| tombstone_sequence < snapshot)
        && reaches_bottom_for_key
        && !unselected_lower_range_may_contain_key
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreStats {
    pub block_reads: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bytes_ingested: u64,
    pub bloom_positives: u64,
    pub bloom_negatives: u64,
    pub manifest_rewrites: u64,
    pub compaction_jobs_started: u64,
    pub compaction_jobs_completed: u64,
    pub compaction_jobs_aborted: u64,
    pub orphan_files_collected: u64,
    pub get_read_amplification: BTreeMap<u64, u64>,
}

impl StoreStats {
    pub fn record_get(&mut self, physical_blocks: u64, bytes: u64) {
        self.block_reads = self.block_reads.saturating_add(physical_blocks);
        self.bytes_read = self.bytes_read.saturating_add(bytes);
        *self
            .get_read_amplification
            .entry(physical_blocks)
            .or_default() += 1;
    }

    pub fn record_ingest(&mut self, logical_bytes: u64) {
        self.bytes_ingested = self.bytes_ingested.saturating_add(logical_bytes);
    }

    pub fn record_write(&mut self, physical_bytes: u64) {
        self.bytes_written = self.bytes_written.saturating_add(physical_bytes);
    }

    #[must_use]
    pub const fn write_amplification(&self) -> (u64, u64) {
        (self.bytes_written, self.bytes_ingested)
    }

    #[must_use]
    pub fn read_amp_percentile(&self, numerator: u64, denominator: u64) -> u64 {
        if denominator == 0 || self.get_read_amplification.is_empty() {
            return 0;
        }
        let total = self.get_read_amplification.values().copied().sum::<u64>();
        let rank = total
            .saturating_mul(numerator)
            .saturating_add(denominator - 1)
            / denominator;
        let mut seen = 0_u64;
        for (blocks, count) in &self.get_read_amplification {
            seen = seen.saturating_add(*count);
            if seen >= rank.max(1) {
                return *blocks;
            }
        }
        0
    }
}

/// A synchronous read boundary shared by real storage and simulation. The
/// returned service duration belongs to the request that caused it, never the
/// next driver input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockRead {
    pub bytes: Vec<u8>,
    pub service: Duration,
}

/// A store result paired with every synchronous block-read duration consumed
/// while producing it.  Error paths retain their already-observed service so
/// a host cannot charge it to a later input.
#[derive(Debug)]
pub struct StoreRead<T, E = StoreError> {
    pub service: Duration,
    pub outcome: Result<T, E>,
}

/// A failed synchronous read still consumes service time that belongs to the
/// input which requested it. Keeping that value with the typed error prevents
/// hosts from accidentally charging the next unrelated operation instead.
#[derive(Debug)]
pub struct BlockReadError {
    pub error: StoreError,
    pub service: Duration,
}

impl From<StoreError> for BlockReadError {
    fn from(error: StoreError) -> Self {
        Self {
            error,
            service: Duration::from_nanos(0),
        }
    }
}

pub trait BlockSource {
    fn read_block(
        &mut self,
        file: FileId,
        offset: u64,
        len: u32,
    ) -> Result<BlockRead, BlockReadError>;
}

#[derive(Clone, Debug, Default)]
pub struct MemoryBlockSource {
    files: BTreeMap<FileId, Vec<u8>>,
    pub service_per_read: Duration,
}

impl MemoryBlockSource {
    pub fn insert(&mut self, file: FileId, bytes: Vec<u8>) {
        self.files.insert(file, bytes);
    }
}

impl BlockSource for MemoryBlockSource {
    fn read_block(
        &mut self,
        file: FileId,
        offset: u64,
        len: u32,
    ) -> Result<BlockRead, BlockReadError> {
        if usize::try_from(len).unwrap_or(usize::MAX) > MAX_CODEC_BYTES {
            return Err(StoreError::TooLarge {
                what: "block read",
                size: usize::try_from(len).unwrap_or(usize::MAX),
                max: MAX_CODEC_BYTES,
            }
            .into());
        }
        let data = self
            .files
            .get(&file)
            .ok_or(StoreError::MissingTable {
                file_no: file_number(file),
            })
            .map_err(BlockReadError::from)?;
        let start = usize::try_from(offset)
            .map_err(|_| BlockReadError::from(StoreError::InvalidInput("block offset")))?;
        let end = start
            .checked_add(len as usize)
            .ok_or(StoreError::InvalidInput("block range"))
            .map_err(BlockReadError::from)?;
        let bytes = data
            .get(start..end)
            .ok_or(StoreError::InvalidInput("block range"))
            .map_err(BlockReadError::from)?
            .to_vec();
        Ok(BlockRead {
            bytes,
            service: self.service_per_read,
        })
    }
}

fn file_number(file: FileId) -> u64 {
    match file {
        FileId::Wal { segment } | FileId::StoreWal { segment } => segment,
        FileId::Sst { file_no } => file_no,
        FileId::Manifest { generation } | FileId::Snapshot { generation } => generation,
        FileId::Meta => 0,
        FileId::Temp { sequence } => sequence,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreError {
    Wal(WalError),
    Decode(DecodeError),
    InvalidInput(&'static str),
    TooLarge {
        what: &'static str,
        size: usize,
        max: usize,
    },
    Busy,
    Corrupt(&'static str),
    MissingTable {
        file_no: u64,
    },
    MetaMismatch,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wal(error) => write!(f, "WAL: {error}"),
            Self::Decode(error) => write!(f, "decode: {error}"),
            Self::InvalidInput(reason) => write!(f, "invalid input: {reason}"),
            Self::TooLarge { what, size, max } => write!(f, "{what} size {size} exceeds {max}"),
            Self::Busy => write!(f, "store is busy flushing its frozen memtable"),
            Self::Corrupt(reason) => write!(f, "corrupt SSTable: {reason}"),
            Self::MissingTable { file_no } => {
                write!(f, "manifest references missing SSTable {file_no}")
            }
            Self::MetaMismatch => write!(f, "META does not point at the manifest generation"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<WalError> for StoreError {
    fn from(error: WalError) -> Self {
        Self::Wal(error)
    }
}

impl From<DecodeError> for StoreError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

#[derive(Clone, Debug, Default)]
struct MemTable {
    entries: BTreeMap<InternalKey, Vec<u8>>,
    bytes: usize,
}

impl MemTable {
    fn insert(&mut self, key: InternalKey, value: Vec<u8>) {
        self.bytes = self
            .bytes
            .saturating_add(key.user_key.len() + value.len() + 17);
        self.entries.insert(key, value);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BloomFilter {
    bits: Vec<u8>,
    hashes: u8,
}

impl BloomFilter {
    #[must_use]
    pub fn for_keys(keys: impl IntoIterator<Item = Vec<u8>>) -> Self {
        let keys: Vec<Vec<u8>> = keys.into_iter().collect();
        let bit_count = (keys.len().saturating_mul(10)).max(64);
        let mut filter = Self {
            bits: vec![0; bit_count.div_ceil(8)],
            hashes: 7,
        };
        for key in keys {
            filter.insert(&key);
        }
        filter
    }

    fn hashes(&self, key: &[u8]) -> (u64, u64) {
        let first = cc_core::fnv1a(key);
        let second = cc_core::fnv1a(&key.iter().rev().copied().collect::<Vec<_>>()) | 1;
        (first, second)
    }

    fn insert(&mut self, key: &[u8]) {
        let (first, second) = self.hashes(key);
        let bit_count = self.bits.len() * 8;
        for i in 0..u64::from(self.hashes) {
            let bit = first.wrapping_add(i.wrapping_mul(second)) % bit_count as u64;
            self.bits[bit as usize / 8] |= 1 << (bit as usize % 8);
        }
    }

    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (first, second) = self.hashes(key);
        let bit_count = self.bits.len() * 8;
        (0..u64::from(self.hashes)).all(|i| {
            let bit = first.wrapping_add(i.wrapping_mul(second)) % bit_count as u64;
            self.bits[bit as usize / 8] & (1 << (bit as usize % 8)) != 0
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstTable {
    pub file_no: u64,
    pub entries: Vec<(InternalKey, Vec<u8>)>,
    pub bloom: BloomFilter,
    bytes: Vec<u8>,
}

impl SstTable {
    pub fn from_entries(
        file_no: u64,
        entries: Vec<(InternalKey, Vec<u8>)>,
    ) -> Result<Self, StoreError> {
        let bloom = BloomFilter::for_keys(entries.iter().map(|(key, _)| key.user_key.clone()));
        let mut enc = Enc::with_capacity(64 + entries.len() * 32);
        enc.header(SST_MAGIC, FORMAT_VERSION);
        enc.u64(file_no);
        enc.u32(
            u32::try_from(entries.len()).map_err(|_| StoreError::TooLarge {
                what: "SSTable entry count",
                size: entries.len(),
                max: u32::MAX as usize,
            })?,
        );
        for (key, value) in &entries {
            enc.bytes(&key.user_key);
            enc.u64(key.sequence);
            enc.u8(*(&key.kind as &ValueKind) as u8);
            enc.bytes(value);
        }
        let mut bytes = enc.finish();
        let checksum = crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(Self {
            file_no,
            entries,
            bloom,
            bytes,
        })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < 4 {
            return Err(StoreError::Corrupt("short footer"));
        }
        let body_len = bytes.len() - 4;
        let expected = u32::from_le_bytes(
            bytes[body_len..]
                .try_into()
                .expect("invariant: SST footer checksum is four bytes"),
        );
        if crc32c(&bytes[..body_len]) != expected {
            return Err(StoreError::Corrupt("footer CRC mismatch"));
        }
        let mut dec = Dec::new(&bytes[..body_len]);
        dec.header(SST_MAGIC, FORMAT_VERSION)?;
        let file_no = dec.u64()?;
        let count = usize::try_from(dec.u32()?).map_err(|_| StoreError::Corrupt("entry count"))?;
        if count > MAX_CODEC_BYTES {
            return Err(StoreError::Corrupt("entry count cap"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let user_key = dec.bytes()?;
            let sequence = dec.u64()?;
            let tag = dec.u8()?;
            let kind = ValueKind::from_byte(tag).ok_or(StoreError::Corrupt("kind tag"))?;
            let value = dec.bytes()?;
            entries.push((InternalKey::new(user_key, sequence, kind), value));
        }
        dec.finish()?;
        let bloom = BloomFilter::for_keys(entries.iter().map(|(key, _)| key.user_key.clone()));
        Ok(Self {
            file_no,
            entries,
            bloom,
            bytes: bytes.to_vec(),
        })
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn get(&self, key: &[u8], snapshot: u64) -> Option<(ValueKind, Vec<u8>, u64)> {
        if !self.bloom.may_contain(key) {
            return None;
        }
        self.entries
            .iter()
            .filter(|(internal, _)| internal.user_key == key && internal.sequence <= snapshot)
            .max_by_key(|(internal, _)| internal.sequence)
            .map(|(internal, value)| (internal.kind, value.clone(), internal.sequence))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestEdit {
    AddFile { level: u8, file_no: u64 },
    RemoveFile { level: u8, file_no: u64 },
    NewFileNo { next: u64 },
    SeqWatermark { sequence: u64 },
    Checkpoint { sequence: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub generation: u64,
    pub files: BTreeMap<u64, u8>,
    pub next_file_no: u64,
    pub sequence: u64,
    pub edits: Vec<ManifestEdit>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            generation: 0,
            files: BTreeMap::new(),
            next_file_no: 1,
            sequence: 0,
            edits: Vec::new(),
        }
    }
}

impl Manifest {
    fn add_file(&mut self, level: u8, file_no: u64) {
        self.files.insert(file_no, level);
        self.edits.push(ManifestEdit::AddFile { level, file_no });
    }

    fn remove_file(&mut self, level: u8, file_no: u64) {
        self.files.remove(&file_no);
        self.edits.push(ManifestEdit::RemoveFile { level, file_no });
    }

    fn allocate_file(&mut self) -> u64 {
        let file_no = self.next_file_no;
        self.next_file_no += 1;
        self.edits.push(ManifestEdit::NewFileNo {
            next: self.next_file_no,
        });
        file_no
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Snapshot(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreImage {
    pub sequence: u64,
    pub applied_watermark: Option<StoreWatermark>,
    pub manifest: Manifest,
    pub meta: Vec<u8>,
    pub tables: Vec<(u64, Vec<u8>)>,
}

/// One storage publication operation expressed without ambient filesystem
/// access. The host assigns I/O ids, drives each step to a successful fsync,
/// and may only expose the next step after its completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorePlanStep {
    CreateTemp { file: FileId },
    Write { file: FileId, bytes: Vec<u8> },
    Fsync { file: FileId },
    Rename { from: FileId, to: FileId },
    Delete { file: FileId },
    SyncDirectory,
}

/// A bounded, host-neutral publication plan for already validated v2 bytes.
/// `min_storage_reader` is a precondition, not an effect: the owner of CCID
/// must durably publish that floor before executing the first step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorePlan {
    pub min_storage_reader: u16,
    pub steps: Vec<StorePlanStep>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageCrashPoint {
    BeforeStep(usize),
    AfterStep(usize),
}

impl StorePlan {
    /// Exhaustive deterministic crash sites. A harness applies each prefix to
    /// a crashable disk and must observe last-good state or a typed refusal.
    #[must_use]
    pub fn crash_points(&self) -> Vec<StorageCrashPoint> {
        (0..self.steps.len())
            .flat_map(|index| {
                [
                    StorageCrashPoint::BeforeStep(index),
                    StorageCrashPoint::AfterStep(index),
                ]
            })
            .collect()
    }

    /// Build the publication tail for a compaction. Inputs are deliberately
    /// deleted only after every output and the replacement manifest are
    /// durable. A failed output plan therefore leaves the old manifest and
    /// all of its files readable.
    pub fn compaction_publication(
        min_storage_reader: u16,
        first_temp_sequence: u64,
        outputs: &BTreeMap<u64, Vec<u8>>,
        manifest_generation: u64,
        manifest_bytes: &[u8],
        old_inputs: &[(u8, u64)],
    ) -> Result<Self, StoreError> {
        if min_storage_reader < STORAGE_V2_MIN_READER
            || first_temp_sequence == 0
            || manifest_generation == 0
            || outputs.is_empty()
            || manifest_bytes.is_empty()
        {
            return Err(StoreError::InvalidInput("compaction publication"));
        }
        let mut sequence = first_temp_sequence;
        let mut steps = Vec::new();
        let mut publish = |target: FileId, bytes: &[u8]| -> Result<(), StoreError> {
            let temp = FileId::Temp { sequence };
            sequence = sequence
                .checked_add(1)
                .ok_or(StoreError::InvalidInput("compaction temp sequence"))?;
            steps.extend([
                StorePlanStep::CreateTemp { file: temp },
                StorePlanStep::Write {
                    file: temp,
                    bytes: bytes.to_vec(),
                },
                StorePlanStep::Fsync { file: temp },
                StorePlanStep::Rename {
                    from: temp,
                    to: target,
                },
                StorePlanStep::SyncDirectory,
            ]);
            Ok(())
        };
        for (file_no, bytes) in outputs {
            publish(FileId::Sst { file_no: *file_no }, bytes)?;
        }
        publish(
            FileId::Manifest {
                generation: manifest_generation,
            },
            manifest_bytes,
        )?;
        for (_, file_no) in old_inputs {
            steps.push(StorePlanStep::Delete {
                file: FileId::Sst { file_no: *file_no },
            });
        }
        steps.push(StorePlanStep::SyncDirectory);
        Ok(Self {
            min_storage_reader,
            steps,
        })
    }
}

/// A complete derived-store v2 generation. It is intentionally a byte image
/// for hosts and crash tests; real publication is performed only through a
/// [`StorePlan`]. The committed Raft log/checkpoint remains authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreV2Image {
    pub manifest: ManifestV2,
    pub manifest_bytes: Vec<u8>,
    pub meta_bytes: Vec<u8>,
    pub tables: BTreeMap<u64, Vec<u8>>,
}

impl StoreV2Image {
    /// Produce the exact create/write/fsync/rename/directory-sync ordering
    /// required for table files, then CCMF, then the atomic CCMT pointer.
    pub fn publish_plan(&self, first_temp_sequence: u64) -> Result<StorePlan, StoreError> {
        self.verify()?;
        if first_temp_sequence == 0 {
            return Err(StoreError::InvalidInput("store temp sequence"));
        }
        let mut next = first_temp_sequence;
        let mut steps = Vec::new();
        let mut append_publish = |bytes: &[u8], target: FileId| -> Result<(), StoreError> {
            let temp = FileId::Temp { sequence: next };
            next = next
                .checked_add(1)
                .ok_or(StoreError::InvalidInput("store temp sequence"))?;
            steps.push(StorePlanStep::CreateTemp { file: temp });
            steps.push(StorePlanStep::Write {
                file: temp,
                bytes: bytes.to_vec(),
            });
            steps.push(StorePlanStep::Fsync { file: temp });
            steps.push(StorePlanStep::Rename {
                from: temp,
                to: target,
            });
            steps.push(StorePlanStep::SyncDirectory);
            Ok(())
        };
        for (file_no, bytes) in &self.tables {
            append_publish(bytes, FileId::Sst { file_no: *file_no })?;
        }
        append_publish(
            &self.manifest_bytes,
            FileId::Manifest {
                generation: self.manifest.generation,
            },
        )?;
        append_publish(&self.meta_bytes, FileId::Meta)?;
        Ok(StorePlan {
            min_storage_reader: STORAGE_V2_MIN_READER,
            steps,
        })
    }

    /// Verify the complete closed world named by META. Missing, extra, or
    /// corrupted table bytes are fail-closed before a host uses this image.
    pub fn verify(&self) -> Result<(), StoreError> {
        let meta = decode_meta_v2(&self.meta_bytes)?;
        let manifest = decode_manifest_v2(&self.manifest_bytes)?;
        if manifest != self.manifest || meta != manifest.meta()? {
            return Err(StoreError::MetaMismatch);
        }
        if self.tables.len() != manifest.files.len() {
            return Err(StoreError::Corrupt("manifest table set"));
        }
        let limits = SstV2Limits {
            max_key_bytes: MAX_CODEC_BYTES,
            max_value_bytes: MAX_CODEC_BYTES,
            max_file_bytes: MAX_CODEC_BYTES,
            max_entries: MAX_CODEC_BYTES,
        };
        for (file_no, file) in &manifest.files {
            let bytes = self
                .tables
                .get(file_no)
                .ok_or(StoreError::MissingTable { file_no: *file_no })?;
            if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != file.file_size
                || sst_v2_footer_crc32c(bytes)? != file.footer_crc32c
            {
                return Err(StoreError::Corrupt("manifest table metadata"));
            }
            let table = SstV2Table::decode(bytes, limits)?;
            let (smallest, _) = table
                .entries
                .first()
                .ok_or(StoreError::Corrupt("manifest empty table"))?;
            let (largest, _) = table
                .entries
                .last()
                .ok_or(StoreError::Corrupt("manifest empty table"))?;
            if smallest != &file.smallest || largest != &file.largest {
                return Err(StoreError::Corrupt("manifest table range"));
            }
        }
        Ok(())
    }
}

/// One visible logical value exported by a checkpoint format.  It deliberately
/// contains neither a table number nor a host path: those are derived storage
/// details and cannot be part of a portable state-machine checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEntry {
    pub key: Vec<u8>,
    pub sequence: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    pub image: StoreImage,
}

#[derive(Clone)]
pub struct Store {
    config: StoreConfig,
    wal: Wal,
    active: MemTable,
    frozen: Option<MemTable>,
    tables: Vec<SstTable>,
    v2_tables: Vec<(ManifestFile, SstV2Reader)>,
    manifest: Manifest,
    next_sequence: u64,
    snapshots: BTreeMap<u64, u64>,
    applied_watermark: Option<StoreWatermark>,
    metadata: BTreeMap<(u8, Vec<u8>), Vec<u8>>,
    stats: RefCell<StoreStats>,
}

impl Store {
    pub fn new(config: StoreConfig) -> Result<Self, StoreError> {
        Ok(Self {
            wal: Wal::new(config.wal)?,
            config,
            active: MemTable::default(),
            frozen: None,
            tables: Vec::new(),
            v2_tables: Vec::new(),
            manifest: Manifest::default(),
            next_sequence: 0,
            snapshots: BTreeMap::new(),
            applied_watermark: None,
            metadata: BTreeMap::new(),
            stats: RefCell::new(StoreStats::default()),
        })
    }

    pub fn boot(image: StoreImage, config: StoreConfig) -> Result<Self, StoreError> {
        let meta_generation = decode_meta(&image.meta)?;
        if meta_generation != image.manifest.generation {
            return Err(StoreError::MetaMismatch);
        }
        let mut tables = Vec::new();
        for file_no in image.manifest.files.keys() {
            let bytes = image
                .tables
                .iter()
                .find(|(candidate, _)| candidate == file_no)
                .map(|(_, bytes)| bytes.as_slice())
                .ok_or(StoreError::MissingTable { file_no: *file_no })?;
            tables.push(SstTable::decode(bytes)?);
        }
        Ok(Self {
            wal: Wal::new(config.wal)?,
            config,
            active: MemTable::default(),
            frozen: None,
            tables,
            v2_tables: Vec::new(),
            manifest: image.manifest,
            next_sequence: image.sequence,
            snapshots: BTreeMap::new(),
            applied_watermark: image.applied_watermark,
            metadata: BTreeMap::new(),
            stats: RefCell::new(StoreStats::default()),
        })
    }

    #[must_use]
    pub fn last_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Export the currently visible values with their exact MVCC sequence.
    /// The result is ordered by user key and intentionally omits tombstones.
    #[must_use]
    pub fn logical_entries(&self) -> Vec<LogicalEntry> {
        let mut keys = BTreeSet::new();
        let mut collect = |table: &MemTable| {
            for internal in table.entries.keys() {
                keys.insert(internal.user_key.clone());
            }
        };
        collect(&self.active);
        if let Some(frozen) = &self.frozen {
            collect(frozen);
        }
        for table in &self.tables {
            for (internal, _) in &table.entries {
                keys.insert(internal.user_key.clone());
            }
        }
        keys.into_iter()
            .filter_map(|key| {
                self.visible_entry(&key)
                    .and_then(|(sequence, kind, value)| {
                        (kind == ValueKind::Put).then_some(LogicalEntry {
                            key,
                            sequence,
                            value,
                        })
                    })
            })
            .collect()
    }

    /// Recreate a store from a canonical logical checkpoint.  The new store
    /// starts with no derived WAL/SST state; future writes continue after the
    /// supplied sequence, preserving the state-machine's MVCC order.
    pub fn from_logical(
        config: StoreConfig,
        sequence: u64,
        entries: Vec<LogicalEntry>,
    ) -> Result<Self, StoreError> {
        let mut store = Self::new(config)?;
        let mut previous: Option<Vec<u8>> = None;
        for entry in entries {
            if entry.key.is_empty()
                || entry.key.len() > store.config.max_key_bytes
                || entry.value.len() > store.config.max_value_bytes
                || entry.sequence == 0
                || entry.sequence > sequence
                || previous
                    .as_ref()
                    .is_some_and(|key| key.as_slice() >= entry.key.as_slice())
            {
                return Err(StoreError::InvalidInput("logical checkpoint entry"));
            }
            previous = Some(entry.key.clone());
            store.active.insert(
                InternalKey::new(entry.key, entry.sequence, ValueKind::Put),
                entry.value,
            );
        }
        store.next_sequence = sequence;
        Ok(store)
    }

    /// Recreate a logical checkpoint together with its authoritative applied
    /// Raft cursor. This is the only way recovery may seed a nonzero store
    /// watermark without replaying a CCSW record.
    pub fn from_logical_at(
        config: StoreConfig,
        sequence: u64,
        entries: Vec<LogicalEntry>,
        watermark: StoreWatermark,
    ) -> Result<Self, StoreError> {
        if watermark.index.get() == 0 || watermark.term.get() == 0 {
            return Err(StoreError::InvalidInput("logical checkpoint watermark"));
        }
        let mut store = Self::from_logical(config, sequence, entries)?;
        store.applied_watermark = Some(watermark);
        Ok(store)
    }

    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<u64, StoreError> {
        self.apply(key, value, ValueKind::Put)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<u64, StoreError> {
        self.apply(key, &[], ValueKind::Delete)
    }

    /// Apply a committed state-machine batch with one WAL durability record.
    ///
    /// Validation and WAL admission happen before the active memtable or
    /// watermark changes.  The in-memory WAL's `commit` is the test seam for
    /// the host fsync continuation introduced by Storage v2; a real host may
    /// publish only after the matching completion succeeds.
    pub fn apply_batch(&mut self, batch: StoreApplyBatch) -> Result<StoreApplyReceipt, StoreError> {
        self.validate_apply_batch(&batch)?;
        if self.frozen.is_some() {
            let _ = self.flush()?;
        }
        let record = encode_apply_batch(&batch)?;
        self.wal.append(RecordType::Data, &record)?;
        let _ = self.wal.commit();

        let first_sequence = if batch.mutations.is_empty() {
            self.next_sequence
        } else {
            self.next_sequence.saturating_add(1)
        };
        for mutation in batch.mutations {
            self.next_sequence = self.next_sequence.saturating_add(1);
            match mutation {
                StoreMutation::Put { key, value } => self.active.insert(
                    InternalKey::new(key, self.next_sequence, ValueKind::Put),
                    value,
                ),
                StoreMutation::Delete { key } => self.active.insert(
                    InternalKey::new(key, self.next_sequence, ValueKind::Delete),
                    Vec::new(),
                ),
            }
        }
        for edit in batch.metadata {
            match edit {
                StoreMetadataEdit::Upsert {
                    namespace,
                    key,
                    value,
                } => {
                    self.metadata.insert((namespace, key), value);
                }
                StoreMetadataEdit::Delete { namespace, key } => {
                    self.metadata.remove(&(namespace, key));
                }
            }
        }
        if self.active.bytes >= self.config.memtable_bytes {
            self.frozen = Some(std::mem::take(&mut self.active));
        }
        self.applied_watermark = Some(batch.watermark);
        Ok(StoreApplyReceipt {
            first_sequence,
            last_sequence: self.next_sequence,
        })
    }

    /// Validate and stage one complete apply transition without publishing
    /// any of its state. The returned frame is what a host appends to its
    /// store WAL; only a matching successful fsync may publish `next`.
    pub fn prepare_apply(&self, batch: StoreApplyBatch) -> Result<PreparedStoreApply, StoreError> {
        let wal_frame = encode_store_wal_frame(&batch)?;
        let mut next = self.clone();
        let _ = next.apply_batch(batch)?;
        Ok(PreparedStoreApply { wal_frame, next })
    }

    /// Replay a durable CCSW prefix only after the snapshot/manifest image is
    /// known. Every newly applied index must be contiguous, present in the
    /// recovered Raft suffix, and have the exact same term. The suffix may
    /// extend beyond the final store record; those entries remain unapplied
    /// until commitment is re-established.
    pub fn replay_wal(
        mut self,
        recovered: &RecoveredStoreWal,
        snapshot_base: (LogIndex, Term),
        log_terms: &BTreeMap<LogIndex, Term>,
    ) -> Result<Self, StoreError> {
        let mut cursor = self.applied_watermark.unwrap_or(StoreWatermark {
            index: snapshot_base.0,
            term: snapshot_base.1,
            last_leader_time: Time::from_nanos(0),
        });
        if cursor.index < snapshot_base.0
            || (cursor.index == snapshot_base.0 && cursor.term != snapshot_base.1)
        {
            return Err(StoreError::Corrupt("store watermark below snapshot base"));
        }
        if cursor.index > snapshot_base.0 && log_terms.get(&cursor.index) != Some(&cursor.term) {
            return Err(StoreError::Corrupt("store watermark ahead of Raft log"));
        }
        for record in &recovered.records {
            if record.watermark.index <= cursor.index {
                if record.watermark.index == cursor.index
                    && (record.watermark.term != cursor.term
                        || record.watermark.last_leader_time > cursor.last_leader_time)
                {
                    return Err(StoreError::Corrupt("store WAL duplicate disagreement"));
                }
                continue;
            }
            let expected = cursor
                .index
                .get()
                .checked_add(1)
                .ok_or(StoreError::Corrupt("store watermark overflow"))?;
            if record.watermark.index.get() != expected {
                return Err(StoreError::Corrupt("store WAL applied-index gap"));
            }
            if log_terms.get(&record.watermark.index) != Some(&record.watermark.term) {
                return Err(StoreError::Corrupt("store WAL term/log mismatch"));
            }
            self.apply_batch(record.clone())?;
            cursor = record.watermark;
        }
        if self.applied_watermark.is_none() && snapshot_base.0.get() != 0 {
            self.applied_watermark = Some(cursor);
        }
        Ok(self)
    }

    #[must_use]
    pub fn applied_watermark(&self) -> Option<StoreWatermark> {
        self.applied_watermark
    }

    #[must_use]
    pub fn metadata(&self) -> &BTreeMap<(u8, Vec<u8>), Vec<u8>> {
        &self.metadata
    }

    #[must_use]
    pub fn is_file_backed(&self) -> bool {
        !self.v2_tables.is_empty()
    }

    /// Stable logical charges used by the cross-host footprint contract.
    /// They intentionally exclude allocator headers/capacity.
    #[must_use]
    pub fn memory_footprint(&self) -> (u64, u64) {
        let memtables = u64::try_from(self.active.bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(
                self.frozen
                    .as_ref()
                    .map_or(0, |table| u64::try_from(table.bytes).unwrap_or(u64::MAX)),
            );
        let metadata = self.v2_tables.iter().fold(0_u64, |total, (file, reader)| {
            total
                .saturating_add(reader.retained_metadata_bytes())
                .saturating_add(
                    u64::try_from(
                        file.smallest
                            .user_key
                            .len()
                            .saturating_add(file.largest.user_key.len()),
                    )
                    .unwrap_or(u64::MAX),
                )
                .saturating_add(64)
        });
        (memtables, metadata)
    }

    /// Process-lifetime storage counters. The returned value is a bounded
    /// point-in-time copy and can be rendered after releasing the store.
    #[must_use]
    pub fn stats(&self) -> StoreStats {
        self.stats.borrow().clone()
    }

    /// Current file count and bytes by level. Levels are bounded by the
    /// manifest codec, so this cannot introduce an unbounded metrics label.
    #[must_use]
    pub fn level_metrics(&self) -> BTreeMap<u8, (u64, u64)> {
        let mut levels = BTreeMap::<u8, (u64, u64)>::new();
        for table in &self.tables {
            let level = *self.manifest.files.get(&table.file_no).unwrap_or(&0);
            let entry = levels.entry(level).or_default();
            entry.0 = entry.0.saturating_add(1);
            entry.1 = entry
                .1
                .saturating_add(u64::try_from(table.bytes().len()).unwrap_or(u64::MAX));
        }
        for (file, _) in &self.v2_tables {
            let entry = levels.entry(file.level).or_default();
            entry.0 = entry.0.saturating_add(1);
            entry.1 = entry.1.saturating_add(file.file_size);
        }
        levels
    }

    fn validate_apply_batch(&self, batch: &StoreApplyBatch) -> Result<(), StoreError> {
        if batch.watermark.index == LogIndex::new(0) || batch.watermark.term == Term::new(0) {
            return Err(StoreError::InvalidInput("store apply watermark"));
        }
        if let Some(previous) = self.applied_watermark
            && (batch.watermark.index <= previous.index
                || batch.watermark.last_leader_time < previous.last_leader_time)
        {
            return Err(StoreError::InvalidInput("store watermark regression"));
        }
        for mutation in &batch.mutations {
            let (key, value) = match mutation {
                StoreMutation::Put { key, value } => (key.as_slice(), value.as_slice()),
                StoreMutation::Delete { key } => (key.as_slice(), &[] as &[u8]),
            };
            if key.is_empty() || key.len() > self.config.max_key_bytes {
                return Err(StoreError::InvalidInput("store apply key"));
            }
            if value.len() > self.config.max_value_bytes {
                return Err(StoreError::TooLarge {
                    what: "store apply value",
                    size: value.len(),
                    max: self.config.max_value_bytes,
                });
            }
        }
        let mut metadata_keys = BTreeSet::new();
        for edit in &batch.metadata {
            let (namespace, key, value_len) = match edit {
                StoreMetadataEdit::Upsert {
                    namespace,
                    key,
                    value,
                } => (*namespace, key.as_slice(), value.len()),
                StoreMetadataEdit::Delete { namespace, key } => (*namespace, key.as_slice(), 0),
            };
            if namespace == 0 || key.is_empty() || key.len() > MAX_CODEC_BYTES {
                return Err(StoreError::InvalidInput("store metadata key"));
            }
            if value_len > MAX_CODEC_BYTES {
                return Err(StoreError::TooLarge {
                    what: "store metadata value",
                    size: value_len,
                    max: MAX_CODEC_BYTES,
                });
            }
            if !metadata_keys.insert((namespace, key.to_vec())) {
                return Err(StoreError::InvalidInput("duplicate store metadata edit"));
            }
        }
        if batch.canonical_command.len() > MAX_CODEC_BYTES
            || batch.cached_reply.len() > MAX_CODEC_BYTES
        {
            return Err(StoreError::TooLarge {
                what: "store apply receipt",
                size: batch
                    .canonical_command
                    .len()
                    .saturating_add(batch.cached_reply.len()),
                max: MAX_CODEC_BYTES,
            });
        }
        let _predicted = self
            .next_sequence
            .checked_add(u64::try_from(batch.mutations.len()).unwrap_or(u64::MAX))
            .ok_or(StoreError::InvalidInput("store sequence overflow"))?;
        Ok(())
    }

    fn apply(&mut self, key: &[u8], value: &[u8], kind: ValueKind) -> Result<u64, StoreError> {
        if key.is_empty() {
            return Err(StoreError::InvalidInput("empty key"));
        }
        if key.len() > self.config.max_key_bytes {
            return Err(StoreError::TooLarge {
                what: "key",
                size: key.len(),
                max: self.config.max_key_bytes,
            });
        }
        if value.len() > self.config.max_value_bytes {
            return Err(StoreError::TooLarge {
                what: "value",
                size: value.len(),
                max: self.config.max_value_bytes,
            });
        }
        if self.frozen.is_some() {
            // A committed state-machine transition is never allowed to fail
            // merely because a derived memtable is awaiting publication.  The
            // synchronous store rotates/flushes that derived structure before
            // accepting the next mutation; an actual I/O failure still
            // propagates as infrastructure failure.
            let _ = self.flush()?;
        }
        let sequence = self.next_sequence + 1;
        let payload = encode_mutation(sequence, key, value, kind);
        self.wal.append(RecordType::Data, &payload)?;
        let _ = self.wal.commit();
        self.next_sequence = sequence;
        self.active.insert(
            InternalKey::new(key.to_vec(), sequence, kind),
            value.to_vec(),
        );
        if self.active.bytes >= self.config.memtable_bytes {
            self.frozen = Some(std::mem::take(&mut self.active));
        }
        Ok(sequence)
    }

    pub fn snapshot(&mut self) -> Snapshot {
        let snapshot = Snapshot(self.next_sequence);
        let count = self.snapshots.get(&snapshot.0).copied().unwrap_or(0);
        self.snapshots.insert(snapshot.0, count.saturating_add(1));
        snapshot
    }

    pub fn release_snapshot(&mut self, snapshot: Snapshot) -> Result<(), StoreError> {
        let count = self.snapshots.get(&snapshot.0).copied().unwrap_or(0);
        if count == 0 {
            return Err(StoreError::InvalidInput(
                "unknown or already released snapshot",
            ));
        }
        if count == 1 {
            self.snapshots.remove(&snapshot.0);
        } else {
            self.snapshots.insert(snapshot.0, count - 1);
        }
        Ok(())
    }

    #[must_use]
    pub fn get(&self, key: &[u8], snapshot: Option<Snapshot>) -> Option<Vec<u8>> {
        let watermark = snapshot.map_or(self.next_sequence, |value| value.0);
        let mut best: Option<(u64, ValueKind, Vec<u8>)> = None;
        self.consider_memtable(&self.active, key, watermark, &mut best);
        if let Some(frozen) = &self.frozen {
            self.consider_memtable(frozen, key, watermark, &mut best);
        }
        for table in &self.tables {
            if let Some((kind, value, sequence)) = table.get(key, watermark)
                && best.as_ref().is_none_or(|current| sequence > current.0)
            {
                best = Some((sequence, kind, value));
            }
        }
        match best {
            Some((_, ValueKind::Put, value)) => Some(value),
            _ => None,
        }
    }

    pub fn get_with_source(
        &self,
        source: &mut dyn BlockSource,
        key: &[u8],
        snapshot: Option<Snapshot>,
    ) -> StoreRead<Option<Vec<u8>>> {
        let watermark = snapshot.map_or(self.next_sequence, |value| value.0);
        let mut service = Duration::from_nanos(0);
        let mut best: Option<(u64, ValueKind, Vec<u8>)> = None;
        self.consider_memtable(&self.active, key, watermark, &mut best);
        if let Some(frozen) = &self.frozen {
            self.consider_memtable(frozen, key, watermark, &mut best);
        }
        for table in &self.tables {
            if let Some((kind, value, sequence)) = table.get(key, watermark)
                && best.as_ref().is_none_or(|current| sequence > current.0)
            {
                best = Some((sequence, kind, value));
            }
        }
        for (file, reader) in &self.v2_tables {
            if key < file.smallest.user_key.as_slice() || key > file.largest.user_key.as_slice() {
                continue;
            }
            if reader.may_contain(key) {
                let mut stats = self.stats.borrow_mut();
                stats.bloom_positives = stats.bloom_positives.saturating_add(1);
            } else {
                let mut stats = self.stats.borrow_mut();
                stats.bloom_negatives = stats.bloom_negatives.saturating_add(1);
            }
            let read = reader.get(source, key, watermark);
            service = sum_service(service, read.service);
            let value = match read.outcome {
                Ok(value) => value,
                Err(error) => {
                    return StoreRead {
                        service,
                        outcome: Err(error),
                    };
                }
            };
            if let Some((kind, value, sequence)) = value
                && best.as_ref().is_none_or(|current| sequence > current.0)
            {
                best = Some((sequence, kind, value));
            }
        }
        StoreRead {
            service,
            outcome: Ok(match best {
                Some((_, ValueKind::Put, value)) => Some(value),
                _ => None,
            }),
        }
    }

    fn consider_memtable(
        &self,
        table: &MemTable,
        key: &[u8],
        watermark: u64,
        best: &mut Option<(u64, ValueKind, Vec<u8>)>,
    ) {
        for (internal, value) in &table.entries {
            if internal.user_key == key
                && internal.sequence <= watermark
                && best
                    .as_ref()
                    .is_none_or(|current| internal.sequence > current.0)
            {
                *best = Some((internal.sequence, internal.kind, value.clone()));
            }
        }
    }

    fn visible_entry(&self, key: &[u8]) -> Option<(u64, ValueKind, Vec<u8>)> {
        let mut best = None;
        self.consider_memtable(&self.active, key, self.next_sequence, &mut best);
        if let Some(frozen) = &self.frozen {
            self.consider_memtable(frozen, key, self.next_sequence, &mut best);
        }
        for table in &self.tables {
            for (internal, value) in &table.entries {
                if internal.user_key == key
                    && internal.sequence <= self.next_sequence
                    && best
                        .as_ref()
                        .is_none_or(|current: &(u64, ValueKind, Vec<u8>)| {
                            internal.sequence > current.0
                        })
                {
                    best = Some((internal.sequence, internal.kind, value.clone()));
                }
            }
        }
        best
    }

    #[must_use]
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot: Option<Snapshot>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut keys = BTreeSet::new();
        let mut collect = |table: &MemTable| {
            for internal in table.entries.keys() {
                keys.insert(internal.user_key.clone());
            }
        };
        collect(&self.active);
        if let Some(frozen) = &self.frozen {
            collect(frozen);
        }
        for table in &self.tables {
            for (internal, _) in &table.entries {
                keys.insert(internal.user_key.clone());
            }
        }
        keys.into_iter()
            .filter(|key| start.is_none_or(|value| key.as_slice() >= value))
            .filter(|key| end.is_none_or(|value| key.as_slice() < value))
            .filter_map(|key| self.get(&key, snapshot).map(|value| (key, value)))
            .take(limit)
            .collect()
    }

    pub fn scan_with_source(
        &self,
        source: &mut dyn BlockSource,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot: Option<Snapshot>,
        limit: usize,
    ) -> StoreRead<Vec<(Vec<u8>, Vec<u8>)>> {
        let watermark = snapshot.map_or(self.next_sequence, |value| value.0);
        let mut service = Duration::from_nanos(0);
        let mut merged = BTreeMap::<Vec<u8>, (ValueKind, Vec<u8>, u64)>::new();
        for (key, value) in self.scan(start, end, snapshot, limit) {
            let sequence = self.visible_entry(&key).map_or(0, |entry| entry.0);
            merged.insert(key, (ValueKind::Put, value, sequence));
        }
        for (_, reader) in &self.v2_tables {
            let read = reader.scan(source, start, end, watermark, limit);
            service = sum_service(service, read.service);
            let entries = match read.outcome {
                Ok(entries) => entries,
                Err(error) => {
                    return StoreRead {
                        service,
                        outcome: Err(error),
                    };
                }
            };
            for (key, kind, value, sequence) in entries {
                merged
                    .entry(key)
                    .and_modify(|current| {
                        if sequence > current.2 {
                            *current = (kind, value.clone(), sequence);
                        }
                    })
                    .or_insert((kind, value, sequence));
            }
        }
        StoreRead {
            service,
            outcome: Ok(merged
                .into_iter()
                .filter_map(|(key, (kind, value, _))| {
                    (kind == ValueKind::Put).then_some((key, value))
                })
                .take(limit)
                .collect()),
        }
    }

    /// Flush the immutable memtable before publishing its manifest edit.
    pub fn flush(&mut self) -> Result<Option<u64>, StoreError> {
        let table_source = match self.frozen.take() {
            Some(table) => table,
            None if !self.active.entries.is_empty() => std::mem::take(&mut self.active),
            None => return Ok(None),
        };
        let file_no = self.manifest.allocate_file();
        let logical_bytes = u64::try_from(table_source.bytes).unwrap_or(u64::MAX);
        let entries = table_source.entries.into_iter().collect();
        let table = SstTable::from_entries(file_no, entries)?;
        let bytes = table.bytes().to_vec();
        self.tables.push(table);
        self.manifest.add_file(0, file_no);
        self.manifest.sequence = self.next_sequence;
        self.manifest.generation += 1;
        self.manifest.edits.push(ManifestEdit::SeqWatermark {
            sequence: self.next_sequence,
        });
        let mut stats = self.stats.borrow_mut();
        stats.record_ingest(logical_bytes);
        stats.record_write(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        stats.manifest_rewrites = stats.manifest_rewrites.saturating_add(1);
        Ok(Some(file_no))
    }

    pub fn compact(&mut self) -> Result<bool, StoreError> {
        if self.tables.len() < 2 {
            return Ok(false);
        }
        {
            let mut stats = self.stats.borrow_mut();
            stats.compaction_jobs_started = stats.compaction_jobs_started.saturating_add(1);
        }
        let mut all = BTreeMap::<InternalKey, Vec<u8>>::new();
        for table in &self.tables {
            for (key, value) in &table.entries {
                all.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        let min_snapshot = self
            .snapshots
            .iter()
            .next()
            .map(|(watermark, _)| *watermark)
            .unwrap_or(self.next_sequence);
        let mut keep = Vec::new();
        let mut seen_keys = BTreeSet::new();
        for (key, value) in all {
            if !seen_keys.insert(key.user_key.clone()) && key.sequence < min_snapshot {
                continue;
            }
            if key.kind == ValueKind::Delete && key.sequence < min_snapshot {
                continue;
            }
            keep.push((key, value));
        }
        let old = std::mem::take(&mut self.tables);
        let old_files: Vec<(u64, u8)> = old
            .iter()
            .map(|table| {
                (
                    table.file_no,
                    *self.manifest.files.get(&table.file_no).unwrap_or(&0),
                )
            })
            .collect();
        let file_no = self.manifest.allocate_file();
        let table = match SstTable::from_entries(file_no, keep) {
            Ok(table) => table,
            Err(error) => {
                let mut stats = self.stats.borrow_mut();
                stats.compaction_jobs_aborted = stats.compaction_jobs_aborted.saturating_add(1);
                return Err(error);
            }
        };
        let output_bytes = u64::try_from(table.bytes().len()).unwrap_or(u64::MAX);
        self.tables.push(table);
        self.manifest.add_file(1, file_no);
        for (old_file, level) in old_files {
            self.manifest.remove_file(level, old_file);
        }
        self.manifest.generation += 1;
        let mut stats = self.stats.borrow_mut();
        stats.record_write(output_bytes);
        stats.manifest_rewrites = stats.manifest_rewrites.saturating_add(1);
        stats.compaction_jobs_completed = stats.compaction_jobs_completed.saturating_add(1);
        Ok(true)
    }

    pub fn checkpoint(&mut self) -> Result<Checkpoint, StoreError> {
        self.flush()?;
        self.manifest.edits.push(ManifestEdit::Checkpoint {
            sequence: self.next_sequence,
        });
        Ok(Checkpoint {
            image: self.image(),
        })
    }

    #[must_use]
    pub fn image(&self) -> StoreImage {
        StoreImage {
            sequence: self.next_sequence,
            applied_watermark: self.applied_watermark,
            manifest: self.manifest.clone(),
            meta: encode_meta(self.manifest.generation),
            tables: self
                .tables
                .iter()
                .map(|table| (table.file_no, table.bytes().to_vec()))
                .collect(),
        }
    }

    /// Materialize the current derived state into one self-validating v2
    /// generation. The returned bytes are not durable until a host executes
    /// its [`StoreV2Image::publish_plan`] after raising the CCID v2 fence.
    ///
    /// A v2 manifest represents committed state-machine apply evidence, so a
    /// caller cannot export ordinary local fixture mutations without an
    /// applied Raft watermark.
    pub fn prepare_v2_image(&mut self) -> Result<StoreV2Image, StoreError> {
        let _ = self.flush()?;
        let watermark = self.applied_watermark.ok_or(StoreError::InvalidInput(
            "v2 image requires applied watermark",
        ))?;
        if self.tables.is_empty() {
            return Err(StoreError::InvalidInput("v2 image requires a table"));
        }
        let limits = SstV2Limits {
            max_key_bytes: self.config.max_key_bytes,
            max_value_bytes: self.config.max_value_bytes,
            max_file_bytes: MAX_CODEC_BYTES,
            max_entries: MAX_CODEC_BYTES,
        };
        let generation = self.manifest.generation.saturating_add(1).max(1);
        let mut v2_manifest = ManifestV2::empty(generation);
        let mut files = Vec::new();
        let mut tables = BTreeMap::new();
        let mut highest_file_no = 0_u64;
        for table in &self.tables {
            let bytes = SstV2Table::encode(table.entries.clone(), limits)?;
            let (smallest, _) = table
                .entries
                .first()
                .ok_or(StoreError::Corrupt("store table entries"))?;
            let (largest, _) = table
                .entries
                .last()
                .ok_or(StoreError::Corrupt("store table entries"))?;
            let file_no = table.file_no;
            if tables.insert(file_no, bytes.clone()).is_some() {
                return Err(StoreError::Corrupt("store duplicate table number"));
            }
            highest_file_no = highest_file_no.max(file_no);
            files.push(ManifestFile {
                level: *self.manifest.files.get(&file_no).unwrap_or(&0),
                file_no,
                file_size: u64::try_from(bytes.len())
                    .map_err(|_| StoreError::InvalidInput("v2 table size"))?,
                smallest: smallest.clone(),
                largest: largest.clone(),
                footer_crc32c: sst_v2_footer_crc32c(&bytes)?,
            });
        }
        let next_file_no = highest_file_no
            .checked_add(1)
            .ok_or(StoreError::InvalidInput("v2 file number"))?;
        let mut edits = vec![ManifestEditV2::NextFileNo(next_file_no)];
        edits.extend(files.into_iter().map(ManifestEditV2::AddFile));
        edits.push(ManifestEditV2::AppliedWatermark {
            watermark,
            store_sequence: self.next_sequence,
        });
        v2_manifest.append_edit_batch(edits)?;
        let manifest_bytes = encode_manifest_v2(&v2_manifest)?;
        let meta_bytes = encode_meta_v2(v2_manifest.meta()?);
        let image = StoreV2Image {
            manifest: v2_manifest,
            manifest_bytes,
            meta_bytes,
            tables,
        };
        image.verify()?;
        Ok(image)
    }

    /// Reconstruct a store from already verified v2 bytes. This keeps the v1
    /// reader alive for compatibility while ensuring a v2 image can only
    /// become readable after the META/CCMF/table cross-check succeeds.
    pub fn boot_v2(image: StoreV2Image, config: StoreConfig) -> Result<Self, StoreError> {
        image.verify()?;
        let mut tables = Vec::new();
        let limits = SstV2Limits {
            max_key_bytes: config.max_key_bytes,
            max_value_bytes: config.max_value_bytes,
            max_file_bytes: MAX_CODEC_BYTES,
            max_entries: MAX_CODEC_BYTES,
        };
        for file_no in image.manifest.files.keys() {
            let bytes = image
                .tables
                .get(file_no)
                .ok_or(StoreError::MissingTable { file_no: *file_no })?;
            let table = SstV2Table::decode(bytes, limits)?;
            tables.push(SstTable::from_entries(*file_no, table.entries)?);
        }
        Ok(Self {
            wal: Wal::new(config.wal)?,
            config,
            active: MemTable::default(),
            frozen: None,
            tables,
            v2_tables: Vec::new(),
            manifest: Manifest {
                generation: image.manifest.generation,
                files: image
                    .manifest
                    .files
                    .iter()
                    .map(|(file_no, file)| (*file_no, file.level))
                    .collect(),
                next_file_no: image.manifest.next_file_no,
                sequence: image.manifest.store_sequence,
                edits: Vec::new(),
            },
            next_sequence: image.manifest.store_sequence,
            snapshots: BTreeMap::new(),
            applied_watermark: image.manifest.applied_watermark,
            metadata: BTreeMap::new(),
            stats: RefCell::new(StoreStats::default()),
        })
    }

    /// Open a v2 generation without retaining any data block or whole SST
    /// bytes. Footer/index/bloom reads are charged to this boot operation;
    /// later point/scan reads use the same host seam.
    pub fn boot_v2_file_backed(
        manifest_bytes: &[u8],
        meta_bytes: &[u8],
        source: &mut dyn BlockSource,
        config: StoreConfig,
    ) -> StoreRead<Self> {
        let mut service = Duration::from_nanos(0);
        let manifest = match decode_manifest_v2(manifest_bytes) {
            Ok(value) => value,
            Err(error) => {
                return StoreRead {
                    service,
                    outcome: Err(error),
                };
            }
        };
        let meta = match decode_meta_v2(meta_bytes) {
            Ok(value) => value,
            Err(error) => {
                return StoreRead {
                    service,
                    outcome: Err(error),
                };
            }
        };
        if manifest.meta().ok() != Some(meta) {
            return StoreRead {
                service,
                outcome: Err(StoreError::MetaMismatch),
            };
        }
        let limits = SstV2Limits {
            max_key_bytes: config.max_key_bytes,
            max_value_bytes: config.max_value_bytes,
            max_file_bytes: MAX_CODEC_BYTES,
            max_entries: MAX_CODEC_BYTES,
        };
        let mut v2_tables = Vec::with_capacity(manifest.files.len());
        for file in manifest.files.values() {
            let opened = SstV2Reader::open(
                source,
                FileId::Sst {
                    file_no: file.file_no,
                },
                file.file_size,
                limits,
            );
            service = sum_service(service, opened.service);
            let reader = match opened.outcome {
                Ok(reader) => reader,
                Err(error) => {
                    return StoreRead {
                        service,
                        outcome: Err(error),
                    };
                }
            };
            if reader.file_size() != file.file_size || reader.footer_crc32c() != file.footer_crc32c
            {
                return StoreRead {
                    service,
                    outcome: Err(StoreError::Corrupt("manifest table metadata")),
                };
            }
            v2_tables.push((file.clone(), reader));
        }
        let store = Self {
            wal: match Wal::new(config.wal) {
                Ok(wal) => wal,
                Err(error) => {
                    return StoreRead {
                        service,
                        outcome: Err(StoreError::Wal(error)),
                    };
                }
            },
            config,
            active: MemTable::default(),
            frozen: None,
            tables: Vec::new(),
            v2_tables,
            manifest: Manifest {
                generation: manifest.generation,
                files: manifest
                    .files
                    .iter()
                    .map(|(file_no, file)| (*file_no, file.level))
                    .collect(),
                next_file_no: manifest.next_file_no,
                sequence: manifest.store_sequence,
                edits: Vec::new(),
            },
            next_sequence: manifest.store_sequence,
            snapshots: BTreeMap::new(),
            applied_watermark: manifest.applied_watermark,
            metadata: BTreeMap::new(),
            stats: RefCell::new(StoreStats::default()),
        };
        StoreRead {
            service,
            outcome: Ok(store),
        }
    }

    pub fn restore(checkpoint: Checkpoint, config: StoreConfig) -> Result<Self, StoreError> {
        Self::boot(checkpoint.image, config)
    }
}

fn encode_mutation(sequence: u64, key: &[u8], value: &[u8], kind: ValueKind) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.u64(sequence);
    enc.u8(kind as u8);
    enc.bytes(key);
    enc.bytes(value);
    enc.finish()
}

fn sum_service(left: Duration, right: Duration) -> Duration {
    Duration::from_nanos(left.as_nanos().saturating_add(right.as_nanos()))
}

pub fn encode_apply_batch(batch: &StoreApplyBatch) -> Result<Vec<u8>, StoreError> {
    let mut enc = Enc::new();
    enc.header(u32::from_le_bytes(*b"CCSW"), 2);
    enc.u8(batch.entry_kind as u8);
    enc.u64(batch.watermark.index.0);
    enc.u64(batch.watermark.term.0);
    enc.u64(batch.watermark.last_leader_time.as_nanos());
    enc.u32(
        u32::try_from(batch.mutations.len()).map_err(|_| StoreError::TooLarge {
            what: "store apply mutation count",
            size: batch.mutations.len(),
            max: u32::MAX as usize,
        })?,
    );
    for mutation in &batch.mutations {
        match mutation {
            StoreMutation::Put { key, value } => {
                enc.u8(1);
                enc.bytes(key);
                enc.bytes(value);
            }
            StoreMutation::Delete { key } => {
                enc.u8(2);
                enc.bytes(key);
            }
        }
    }
    enc.u32(
        u32::try_from(batch.metadata.len()).map_err(|_| StoreError::TooLarge {
            what: "store metadata edit count",
            size: batch.metadata.len(),
            max: u32::MAX as usize,
        })?,
    );
    for edit in &batch.metadata {
        match edit {
            StoreMetadataEdit::Upsert {
                namespace,
                key,
                value,
            } => {
                enc.u8(1);
                enc.u8(*namespace);
                enc.bytes(key);
                enc.bytes(value);
            }
            StoreMetadataEdit::Delete { namespace, key } => {
                enc.u8(2);
                enc.u8(*namespace);
                enc.bytes(key);
            }
        }
    }
    enc.bytes(&batch.canonical_command);
    enc.bytes(&batch.cached_reply);
    Ok(enc.finish())
}

pub fn decode_apply_batch(bytes: &[u8]) -> Result<StoreApplyBatch, StoreError> {
    let mut dec = Dec::new(bytes);
    dec.header(u32::from_le_bytes(*b"CCSW"), 2)?;
    let entry_kind = StoreEntryKind::decode(dec.u8()?)?;
    let watermark = StoreWatermark {
        index: LogIndex::new(dec.u64()?),
        term: Term::new(dec.u64()?),
        last_leader_time: Time::from_nanos(dec.u64()?),
    };
    let mutation_count =
        usize::try_from(dec.u32()?).map_err(|_| StoreError::Corrupt("store mutation count"))?;
    if mutation_count > MAX_CODEC_BYTES || mutation_count > dec.remaining() {
        return Err(StoreError::Corrupt("store mutation count"));
    }
    let mut mutations = Vec::with_capacity(mutation_count);
    for _ in 0..mutation_count {
        mutations.push(match dec.u8()? {
            1 => StoreMutation::Put {
                key: dec.bytes()?,
                value: dec.bytes()?,
            },
            2 => StoreMutation::Delete { key: dec.bytes()? },
            _ => return Err(StoreError::Corrupt("store mutation tag")),
        });
    }
    let metadata_count =
        usize::try_from(dec.u32()?).map_err(|_| StoreError::Corrupt("store metadata count"))?;
    if metadata_count > MAX_CODEC_BYTES || metadata_count > dec.remaining() {
        return Err(StoreError::Corrupt("store metadata count"));
    }
    let mut metadata = Vec::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        let tag = dec.u8()?;
        let namespace = dec.u8()?;
        let key = dec.bytes()?;
        metadata.push(match tag {
            1 => StoreMetadataEdit::Upsert {
                namespace,
                key,
                value: dec.bytes()?,
            },
            2 => StoreMetadataEdit::Delete { namespace, key },
            _ => return Err(StoreError::Corrupt("store metadata tag")),
        });
    }
    let canonical_command = dec.bytes()?;
    let cached_reply = dec.bytes()?;
    dec.finish()?;
    let batch = StoreApplyBatch {
        entry_kind,
        watermark,
        mutations,
        metadata,
        canonical_command,
        cached_reply,
    };
    // Reuse the live validator without requiring an ambient or durable store.
    Store::new(StoreConfig::default())?.validate_apply_batch(&batch)?;
    Ok(batch)
}

/// Frame one CCSW record for append-only host persistence.  A torn final
/// prefix is ignored during recovery; a complete frame with a bad checksum is
/// typed corruption.
pub fn encode_store_wal_frame(batch: &StoreApplyBatch) -> Result<Vec<u8>, StoreError> {
    let payload = encode_apply_batch(batch)?;
    let len = u32::try_from(payload.len()).map_err(|_| StoreError::TooLarge {
        what: "store WAL record",
        size: payload.len(),
        max: u32::MAX as usize,
    })?;
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&crc32c(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn recover_store_wal(bytes: &[u8]) -> Result<RecoveredStoreWal, StoreError> {
    let mut offset = 0_usize;
    let mut records = Vec::new();
    let mut torn_tail_truncated = false;
    while offset < bytes.len() {
        if bytes.len() - offset < 8 {
            torn_tail_truncated = true;
            break;
        }
        let length = usize::try_from(u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .expect("store frame length"),
        ))
        .map_err(|_| StoreError::Corrupt("store WAL length"))?;
        if length == 0 || length > MAX_CODEC_BYTES {
            return Err(StoreError::Corrupt("store WAL length"));
        }
        let end = offset
            .checked_add(8)
            .and_then(|start| start.checked_add(length))
            .ok_or(StoreError::Corrupt("store WAL length"))?;
        if end > bytes.len() {
            torn_tail_truncated = true;
            break;
        }
        let expected = u32::from_le_bytes(
            bytes[offset + 4..offset + 8]
                .try_into()
                .expect("store frame CRC"),
        );
        let payload = &bytes[offset + 8..end];
        if crc32c(payload) != expected {
            return Err(StoreError::Corrupt("store WAL checksum"));
        }
        records.push(decode_apply_batch(payload)?);
        offset = end;
    }
    Ok(RecoveredStoreWal {
        records,
        bytes_consumed: u64::try_from(offset).unwrap_or(u64::MAX),
        torn_tail_truncated,
    })
}

fn encode_meta(generation: u64) -> Vec<u8> {
    let mut enc = Enc::new();
    enc.header(META_MAGIC, FORMAT_VERSION);
    enc.u64(generation);
    let mut bytes = enc.finish();
    bytes.extend_from_slice(&crc32c(&bytes).to_le_bytes());
    bytes
}

fn decode_meta(bytes: &[u8]) -> Result<u64, StoreError> {
    if bytes.len() < 4 {
        return Err(StoreError::Corrupt("short META"));
    }
    let body_len = bytes.len() - 4;
    let expected = u32::from_le_bytes(bytes[body_len..].try_into().expect("invariant: META CRC"));
    if crc32c(&bytes[..body_len]) != expected {
        return Err(StoreError::Corrupt("META CRC mismatch"));
    }
    let mut dec = Dec::new(&bytes[..body_len]);
    dec.header(META_MAGIC, FORMAT_VERSION)?;
    let generation = dec.u64()?;
    dec.finish()?;
    Ok(generation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct CountingBlocks {
        inner: MemoryBlockSource,
        reads: usize,
    }

    impl BlockSource for CountingBlocks {
        fn read_block(
            &mut self,
            file: FileId,
            offset: u64,
            len: u32,
        ) -> Result<BlockRead, BlockReadError> {
            self.reads += 1;
            self.inner.read_block(file, offset, len)
        }
    }

    #[test]
    fn trap_legacy_sst_fixture_is_readable() {
        let table = SstTable::decode(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/legacy/ccst-v1.bin"
        )))
        .expect("legacy CCST fixture");
        assert_eq!(table.file_no, 7);
        assert_eq!(
            table.get(b"legacy-c0-key", 1),
            Some((ValueKind::Put, b"legacy-c0-value".to_vec(), 1))
        );
    }

    #[test]
    fn trap_legacy_meta_fixture_boots_store() {
        let store = Store::boot(
            StoreImage {
                sequence: 0,
                applied_watermark: None,
                manifest: Manifest::default(),
                meta: include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../tests/golden/legacy/ccmt-v1.bin"
                ))
                .to_vec(),
                tables: Vec::new(),
            },
            config(),
        )
        .expect("legacy CCMT fixture");
        assert_eq!(store.last_sequence(), 0);
    }

    fn config() -> StoreConfig {
        StoreConfig {
            memtable_bytes: 64,
            max_key_bytes: 32,
            max_value_bytes: 64,
            wal: WalConfig {
                segment_size: 256,
                max_record_size: 128,
            },
        }
    }

    #[test]
    fn store_config_from_parts_keeps_wal_limits_inside_store_ownership() {
        let config = StoreConfig::from_parts(64, 32, 16, 256, 128);
        assert_eq!(
            config,
            StoreConfig {
                memtable_bytes: 64,
                max_key_bytes: 32,
                max_value_bytes: 16,
                wal: WalConfig {
                    segment_size: 256,
                    max_record_size: 128,
                },
            }
        );
    }

    #[test]
    fn trap_store_watermark_is_atomic_with_write_batch() {
        let mut store = Store::new(config()).expect("store");
        let first = StoreWatermark {
            index: LogIndex::new(1),
            term: Term::new(1),
            last_leader_time: Time::from_nanos(7),
        };
        let receipt = store
            .apply_batch(StoreApplyBatch {
                entry_kind: StoreEntryKind::App,
                watermark: first,
                mutations: vec![StoreMutation::Put {
                    key: b"a".to_vec(),
                    value: b"one".to_vec(),
                }],
                metadata: Vec::new(),
                canonical_command: b"set-a".to_vec(),
                cached_reply: b"ok".to_vec(),
            })
            .expect("first atomic batch");
        assert_eq!(receipt.first_sequence, 1);
        assert_eq!(receipt.last_sequence, 1);
        assert_eq!(store.get(b"a", None), Some(b"one".to_vec()));
        assert_eq!(store.applied_watermark(), Some(first));

        assert!(matches!(
            store.apply_batch(StoreApplyBatch {
                entry_kind: StoreEntryKind::App,
                watermark: StoreWatermark {
                    index: LogIndex::new(2),
                    term: Term::new(1),
                    last_leader_time: Time::from_nanos(8),
                },
                mutations: vec![StoreMutation::Put {
                    key: Vec::new(),
                    value: b"must-not-publish".to_vec(),
                }],
                metadata: Vec::new(),
                canonical_command: b"invalid".to_vec(),
                cached_reply: Vec::new(),
            }),
            Err(StoreError::InvalidInput("store apply key"))
        ));
        assert_eq!(store.get(b"a", None), Some(b"one".to_vec()));
        assert_eq!(store.get(b"must-not-publish", None), None);
        assert_eq!(store.applied_watermark(), Some(first));

        let restored = Store::boot(store.image(), config()).expect("restore image");
        assert_eq!(restored.applied_watermark(), Some(first));
    }

    fn state_batch(index: u64, term: u64, time: u64, value: &[u8]) -> StoreApplyBatch {
        StoreApplyBatch {
            entry_kind: StoreEntryKind::App,
            watermark: StoreWatermark {
                index: LogIndex::new(index),
                term: Term::new(term),
                last_leader_time: Time::from_nanos(time),
            },
            mutations: vec![StoreMutation::Put {
                key: b"k".to_vec(),
                value: value.to_vec(),
            }],
            metadata: vec![StoreMetadataEdit::Upsert {
                namespace: 1,
                key: b"session".to_vec(),
                value: value.to_vec(),
            }],
            canonical_command: b"canonical".to_vec(),
            cached_reply: b"reply".to_vec(),
        }
    }

    #[test]
    fn trap_store_watermark_preserves_last_leader_time() {
        let store = Store::new(config()).expect("store");
        let batch = state_batch(1, 1, 99, b"one");
        let prepared = store.prepare_apply(batch.clone()).expect("prepare");
        assert_eq!(store.applied_watermark(), None, "prepare must be invisible");
        let recovered = recover_store_wal(prepared.wal_frame()).expect("recover frame");
        assert_eq!(recovered.records, vec![batch]);
        let published = prepared.into_store();
        assert_eq!(
            published
                .applied_watermark()
                .expect("published watermark")
                .last_leader_time,
            Time::from_nanos(99)
        );
        assert_eq!(
            published.metadata().get(&(1, b"session".to_vec())),
            Some(&b"one".to_vec())
        );
    }

    #[test]
    fn trap_store_cannot_boot_ahead_of_raft_log() {
        let store = Store::new(config())
            .expect("store")
            .prepare_apply(state_batch(1, 1, 1, b"one"))
            .expect("prepare")
            .into_store()
            .prepare_apply(state_batch(2, 1, 2, b"two"))
            .expect("prepare")
            .into_store();
        assert!(matches!(
            store.replay_wal(
                &recover_store_wal(&[]).expect("empty WAL"),
                (LogIndex::new(0), Term::new(0)),
                &BTreeMap::from([(LogIndex::new(1), Term::new(1))]),
            ),
            Err(StoreError::Corrupt("store watermark ahead of Raft log"))
        ));
    }

    #[test]
    fn trap_store_gap_below_log_base_requires_snapshot() {
        let store = Store::new(config())
            .expect("store")
            .prepare_apply(state_batch(1, 1, 1, b"one"))
            .expect("prepare")
            .into_store();
        assert!(matches!(
            store.replay_wal(
                &recover_store_wal(&[]).expect("empty WAL"),
                (LogIndex::new(2), Term::new(1)),
                &BTreeMap::new(),
            ),
            Err(StoreError::Corrupt("store watermark below snapshot base"))
        ));
    }

    #[test]
    fn trap_store_wal_replays_only_after_manifest_watermark() {
        let base = Store::new(config())
            .expect("store")
            .prepare_apply(state_batch(1, 1, 1, b"one"))
            .expect("base")
            .into_store();
        let frame = encode_store_wal_frame(&state_batch(2, 1, 2, b"two")).expect("frame");
        let recovered = recover_store_wal(&frame).expect("recover");
        let replayed = base
            .replay_wal(
                &recovered,
                (LogIndex::new(0), Term::new(0)),
                &BTreeMap::from([
                    (LogIndex::new(1), Term::new(1)),
                    (LogIndex::new(2), Term::new(1)),
                ]),
            )
            .expect("replay suffix");
        assert_eq!(replayed.get(b"k", None), Some(b"two".to_vec()));

        let gap = recover_store_wal(
            &encode_store_wal_frame(&state_batch(3, 1, 3, b"three")).expect("gap frame"),
        )
        .expect("recover gap");
        assert!(matches!(
            Store::new(config()).expect("empty").replay_wal(
                &gap,
                (LogIndex::new(1), Term::new(1)),
                &BTreeMap::from([(LogIndex::new(3), Term::new(1))]),
            ),
            Err(StoreError::Corrupt("store WAL applied-index gap"))
        ));
    }

    #[test]
    fn trap_storage_marker_precedes_first_v2_byte() {
        let mut store = Store::new(config()).expect("store");
        store
            .apply_batch(state_batch(1, 1, 1, b"one"))
            .expect("apply");
        let plan = store
            .prepare_v2_image()
            .expect("image")
            .publish_plan(1)
            .expect("plan");
        assert_eq!(plan.min_storage_reader, STORAGE_V2_MIN_READER);
        assert!(matches!(
            plan.steps.first(),
            Some(StorePlanStep::CreateTemp { .. })
        ));
    }

    #[test]
    fn trap_v2_image_requires_the_storage_fence_and_reboots_from_exact_meta() {
        let mut store = Store::new(config()).expect("store");
        let watermark = StoreWatermark {
            index: LogIndex::new(3),
            term: Term::new(1),
            last_leader_time: Time::from_nanos(9),
        };
        store
            .apply_batch(StoreApplyBatch {
                entry_kind: StoreEntryKind::App,
                watermark,
                mutations: vec![StoreMutation::Put {
                    key: b"v2".to_vec(),
                    value: b"durable".to_vec(),
                }],
                metadata: Vec::new(),
                canonical_command: b"set-v2".to_vec(),
                cached_reply: b"ok".to_vec(),
            })
            .expect("apply");
        let image = store.prepare_v2_image().expect("v2 image");
        let plan = image.publish_plan(40).expect("plan");
        assert_eq!(plan.min_storage_reader, STORAGE_V2_MIN_READER);
        assert!(matches!(
            plan.steps.first(),
            Some(StorePlanStep::CreateTemp {
                file: FileId::Temp { sequence: 40 }
            })
        ));
        assert!(matches!(
            plan.steps.last(),
            Some(StorePlanStep::SyncDirectory)
        ));
        let reopened = Store::boot_v2(image.clone(), config()).expect("v2 boot");
        assert_eq!(reopened.get(b"v2", None), Some(b"durable".to_vec()));
        assert_eq!(reopened.applied_watermark(), Some(watermark));

        let mut bad = image;
        let file = *bad.tables.keys().next().expect("table");
        bad.tables.get_mut(&file).expect("table")[0] ^= 1;
        assert!(matches!(bad.verify(), Err(StoreError::Corrupt(_))));
    }

    #[test]
    fn trap_file_backed_v2_boot_retains_only_metadata_and_reads_blocks() {
        let mut store = Store::new(StoreConfig::default()).expect("store");
        store
            .apply_batch(StoreApplyBatch {
                entry_kind: StoreEntryKind::App,
                watermark: StoreWatermark {
                    index: LogIndex::new(1),
                    term: Term::new(1),
                    last_leader_time: Time::from_nanos(1),
                },
                mutations: vec![
                    StoreMutation::Put {
                        key: b"file-key".to_vec(),
                        value: b"file-value".to_vec(),
                    },
                    StoreMutation::Put {
                        key: b"file-z".to_vec(),
                        value: b"upper".to_vec(),
                    },
                ],
                metadata: Vec::new(),
                canonical_command: b"set".to_vec(),
                cached_reply: b"ok".to_vec(),
            })
            .expect("apply");
        let image = store.prepare_v2_image().expect("image");
        let mut blocks = CountingBlocks::default();
        blocks.inner.service_per_read = Duration::from_nanos(7);
        for (file_no, bytes) in &image.tables {
            blocks
                .inner
                .insert(FileId::Sst { file_no: *file_no }, bytes.clone());
        }
        let opened = Store::boot_v2_file_backed(
            &image.manifest_bytes,
            &image.meta_bytes,
            &mut blocks,
            StoreConfig::default(),
        );
        assert_eq!(opened.service, Duration::from_nanos(21));
        let file_store = opened.outcome.expect("file-backed store");
        assert!(file_store.tables.is_empty());
        assert_eq!(file_store.v2_tables.len(), 1);
        let read = file_store.get_with_source(&mut blocks, b"file-key", None);
        assert_eq!(read.service, Duration::from_nanos(7));
        assert_eq!(read.outcome.expect("read"), Some(b"file-value".to_vec()));
        let negative = file_store.get_with_source(&mut blocks, b"file-m", None);
        assert_eq!(negative.service, Duration::from_nanos(0));
        assert_eq!(negative.outcome.expect("negative"), None);
        let stats = file_store.stats();
        assert_eq!(stats.bloom_positives, 1);
        assert_eq!(stats.bloom_negatives, 1);
    }

    #[test]
    fn trap_scan_stops_reading_at_limit() {
        let mut store = Store::new(StoreConfig::default()).expect("store");
        let mutations = (0..240)
            .map(|index| StoreMutation::Put {
                key: format!("key-{index:03}").into_bytes(),
                value: vec![index as u8; 64],
            })
            .collect();
        store
            .apply_batch(StoreApplyBatch {
                entry_kind: StoreEntryKind::App,
                watermark: StoreWatermark {
                    index: LogIndex::new(1),
                    term: Term::new(1),
                    last_leader_time: Time::from_nanos(1),
                },
                mutations,
                metadata: Vec::new(),
                canonical_command: b"bulk".to_vec(),
                cached_reply: b"ok".to_vec(),
            })
            .expect("apply");
        let image = store.prepare_v2_image().expect("image");
        let mut blocks = CountingBlocks::default();
        for (file_no, bytes) in &image.tables {
            blocks
                .inner
                .insert(FileId::Sst { file_no: *file_no }, bytes.clone());
        }
        let file_store = Store::boot_v2_file_backed(
            &image.manifest_bytes,
            &image.meta_bytes,
            &mut blocks,
            StoreConfig::default(),
        )
        .outcome
        .expect("boot");
        blocks.reads = 0;
        let read = file_store.scan_with_source(&mut blocks, None, None, None, 1);
        assert_eq!(read.outcome.expect("scan").len(), 1);
        assert_eq!(blocks.reads, 1, "limit=1 reads only the first data block");
    }

    #[test]
    fn wal_first_put_get_delete_and_mvcc_snapshot() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        let snapshot = store.snapshot();
        store.put(b"a", b"two").expect("put");
        assert_eq!(store.get(b"a", None), Some(b"two".to_vec()));
        assert_eq!(store.get(b"a", Some(snapshot)), Some(b"one".to_vec()));
        store.delete(b"a").expect("delete");
        assert_eq!(store.get(b"a", None), None);
    }

    #[test]
    fn trap_equal_watermark_snapshot_pins_are_reference_counted() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        let first = store.snapshot();
        let second = store.snapshot();
        assert_eq!(first, second);
        store.release_snapshot(first).expect("first release");
        store.put(b"a", b"two").expect("put");
        let _ = store.compact().expect("compact");
        assert_eq!(store.get(b"a", Some(second)), Some(b"one".to_vec()));
        store.release_snapshot(second).expect("second release");
        assert!(store.release_snapshot(second).is_err());
    }

    #[test]
    fn scan_is_ordered_and_bounded() {
        let mut store = Store::new(config()).expect("store");
        for key in [b"c", b"a", b"b"] {
            store.put(key, key).expect("put");
        }
        assert_eq!(
            store.scan(Some(b"a"), Some(b"d"), None, 2),
            vec![
                (b"a".to_vec(), b"a".to_vec()),
                (b"b".to_vec(), b"b".to_vec())
            ]
        );
    }

    #[test]
    fn sstable_round_trip_bloom_and_crc() {
        let entries = vec![
            (
                InternalKey::new(b"a".to_vec(), 1, ValueKind::Put),
                b"one".to_vec(),
            ),
            (
                InternalKey::new(b"b".to_vec(), 2, ValueKind::Delete),
                Vec::new(),
            ),
        ];
        let table = SstTable::from_entries(7, entries.clone()).expect("table");
        assert!(table.bloom.may_contain(b"a"));
        let decoded = SstTable::decode(table.bytes()).expect("decode");
        assert_eq!(decoded.entries, entries);
        let mut corrupt = table.bytes().to_vec();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(matches!(
            SstTable::decode(&corrupt),
            Err(StoreError::Corrupt(_))
        ));
    }

    #[test]
    fn golden_byte_layout_vectors() {
        let table = SstTable::from_entries(
            7,
            vec![
                (
                    InternalKey::new(b"a".to_vec(), 1, ValueKind::Put),
                    b"one".to_vec(),
                ),
                (
                    InternalKey::new(b"b".to_vec(), 2, ValueKind::Delete),
                    Vec::new(),
                ),
            ],
        )
        .expect("table");
        assert_eq!(
            hex_bytes(table.bytes()),
            "4343535401000700000000000000020000000100000061010000000000000001030000006f6e65010000006202000000000000000200000000360c63d6"
        );
        assert_eq!(
            hex_bytes(&encode_meta(7)),
            "43434d54010007000000000000008b275acd"
        );
    }

    #[test]
    fn flush_checkpoint_boot_and_restore() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        store.flush().expect("flush");
        let image = store.image();
        let reopened = Store::boot(image, config()).expect("boot");
        assert_eq!(reopened.get(b"a", None), Some(b"one".to_vec()));
        let checkpoint = store.checkpoint().expect("checkpoint");
        let restored = Store::restore(checkpoint, config()).expect("restore");
        assert_eq!(restored.get(b"a", None), Some(b"one".to_vec()));
    }

    #[test]
    fn compaction_preserves_newest_value_and_drops_old_files() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        store.flush().expect("flush");
        store.put(b"a", b"two").expect("put");
        store.flush().expect("flush");
        assert!(store.compact().expect("compact"));
        assert_eq!(store.get(b"a", None), Some(b"two".to_vec()));
        assert_eq!(store.manifest().files.len(), 1);
        let stats = store.stats();
        assert_eq!(stats.compaction_jobs_started, 1);
        assert_eq!(stats.compaction_jobs_completed, 1);
        assert_eq!(stats.compaction_jobs_aborted, 0);
        assert_eq!(stats.manifest_rewrites, 3);
    }

    #[test]
    fn trap_checkpoint_pin_blocks_tombstone_gc() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"pinned", b"old").expect("put");
        store.flush().expect("first table");
        let checkpoint_pin = store.snapshot();
        store.delete(b"pinned").expect("delete");
        store.flush().expect("tombstone table");
        assert!(store.compact().expect("compaction"));
        assert_eq!(
            store.get(b"pinned", Some(checkpoint_pin)),
            Some(b"old".to_vec())
        );
        assert_eq!(store.get(b"pinned", None), None);
        store.release_snapshot(checkpoint_pin).expect("release pin");
    }

    #[test]
    fn trap_backup_pin_survives_concurrent_compaction() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"backup", b"captured").expect("put");
        store.flush().expect("captured table");
        let backup_pin = store.snapshot();
        store.put(b"backup", b"newer").expect("new value");
        store.flush().expect("new table");
        store.compact().expect("concurrent compaction");
        assert_eq!(
            store.get(b"backup", Some(backup_pin)),
            Some(b"captured".to_vec())
        );
        store
            .release_snapshot(backup_pin)
            .expect("release backup pin");
    }

    #[test]
    fn trap_referenced_sst_cap_compacts_before_physical_exhaustion() {
        let files = BTreeMap::from([
            (1, compaction_file(0, 1, 5, b"a", b"c")),
            (2, compaction_file(0, 2, 4, b"b", b"d")),
        ]);
        let policy = CompactionConfig {
            max_levels: 2,
            l0_trigger_files: 2,
            level1_target_bytes: 64,
            level_multiplier: 10,
            output_target_bytes: 8,
        };
        let selection = select_compaction(&files, policy)
            .expect("selection")
            .expect("referenced bytes above the maintenance trigger");
        assert_eq!(selection.source_level, 0);
        assert_eq!(selection.inputs, vec![(0, 1), (0, 2)]);
        assert!(files.values().map(|file| file.file_size).sum::<u64>() < 10);
    }

    #[test]
    fn trap_compaction_starvation_keeps_reads_available_between_jobs() {
        let mut store = Store::new(config()).expect("store");
        for value in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            store.put(b"a", value).expect("put");
            store.flush().expect("flush");
        }
        assert_eq!(store.get(b"a", None), Some(b"three".to_vec()));
        store.compact().expect("compact");
        assert_eq!(store.get(b"a", None), Some(b"three".to_vec()));
    }

    fn compaction_file(
        level: u8,
        file_no: u64,
        size: u64,
        smallest: &[u8],
        largest: &[u8],
    ) -> ManifestFile {
        ManifestFile {
            level,
            file_no,
            file_size: size,
            smallest: InternalKey::new(smallest.to_vec(), 9, ValueKind::Put),
            largest: InternalKey::new(largest.to_vec(), 1, ValueKind::Put),
            footer_crc32c: 1,
        }
    }

    #[test]
    fn trap_compaction_selection_closes_over_all_overlaps() {
        let files = [
            compaction_file(0, 1, 1, b"c", b"f"),
            compaction_file(0, 2, 1, b"a", b"d"),
            compaction_file(0, 3, 1, b"z", b"z"),
            compaction_file(0, 4, 1, b"y", b"z"),
            compaction_file(1, 5, 1, b"b", b"e"),
            compaction_file(1, 6, 1, b"e", b"h"),
        ]
        .into_iter()
        .map(|file| (file.file_no, file))
        .collect();
        let selection = select_compaction(&files, CompactionConfig::default())
            .expect("selection")
            .expect("triggered");
        assert_eq!(selection.source_level, 0);
        assert_eq!(selection.inputs, vec![(0, 1), (0, 2), (1, 5), (1, 6)]);
        assert_eq!(selection.smallest_user_key, b"a");
        assert_eq!(selection.largest_user_key, b"h");
    }

    #[test]
    fn trap_compaction_score_uses_exact_integer_order() {
        assert_eq!(
            compare_compaction_scores(u64::MAX - 1, u64::MAX, u64::MAX - 2, u64::MAX)
                .expect("checked score"),
            Ordering::Greater
        );
        assert_eq!(
            compare_compaction_scores(1, 3, 2, 6).expect("exact tie"),
            Ordering::Equal
        );
    }

    #[test]
    fn trap_compaction_budget_bounds_live_memory() {
        let budget = CompactionBudget {
            max_entries: 2,
            max_input_bytes: 128,
            max_output_bytes: 128,
        }
        .validate()
        .expect("budget");
        let entries = (0..9)
            .map(|index| {
                (
                    InternalKey::new(vec![b'a' + index], 1, ValueKind::Put),
                    vec![index; 8],
                )
            })
            .collect::<Vec<_>>();
        let mut consumed = 0;
        while consumed < entries.len() {
            let end = consumed
                .saturating_add(budget.max_entries as usize)
                .min(entries.len());
            let chunk = &entries[consumed..end];
            assert!(chunk.len() as u64 <= budget.max_entries);
            assert!(
                chunk
                    .iter()
                    .map(|(key, value)| (key.user_key.len() + value.len() + 32) as u64)
                    .sum::<u64>()
                    <= budget.max_input_bytes
            );
            consumed = end;
        }
    }

    #[test]
    fn trap_output_split_never_splits_one_user_key() {
        let entries = vec![
            (
                InternalKey::new(b"a".to_vec(), 3, ValueKind::Put),
                vec![1; 40],
            ),
            (
                InternalKey::new(b"a".to_vec(), 2, ValueKind::Delete),
                Vec::new(),
            ),
            (
                InternalKey::new(b"b".to_vec(), 1, ValueKind::Put),
                vec![2; 40],
            ),
        ];
        let outputs = split_compaction_output(entries, 50).expect("split");
        assert_eq!(outputs.len(), 2);
        assert!(outputs[0].iter().all(|(key, _)| key.user_key == b"a"));
        assert!(outputs[1].iter().all(|(key, _)| key.user_key == b"b"));
    }

    #[test]
    fn trap_tombstone_survives_above_bottom_level() {
        assert!(!may_drop_tombstone(2, Some(10), false, false));
        assert!(!may_drop_tombstone(2, Some(10), true, true));
        assert!(!may_drop_tombstone(10, Some(10), true, false));
        assert!(may_drop_tombstone(2, Some(10), true, false));
    }

    fn sample_compaction_publication() -> StorePlan {
        StorePlan::compaction_publication(
            STORAGE_V2_MIN_READER,
            40,
            &BTreeMap::from([(9, b"output".to_vec())]),
            3,
            b"manifest",
            &[(0, 1), (1, 2)],
        )
        .expect("publication")
    }

    #[test]
    fn trap_compaction_never_deletes_inputs_early() {
        let plan = sample_compaction_publication();
        let last_publish = plan
            .steps
            .iter()
            .rposition(|step| matches!(step, StorePlanStep::Fsync { .. }))
            .expect("manifest fsync");
        let first_delete = plan
            .steps
            .iter()
            .position(|step| matches!(step, StorePlanStep::Delete { .. }))
            .expect("input delete");
        assert!(first_delete > last_publish);
    }

    #[test]
    fn trap_compaction_crash_leaves_readable_store() {
        let plan = sample_compaction_publication();
        let first_delete = plan
            .steps
            .iter()
            .position(|step| matches!(step, StorePlanStep::Delete { .. }))
            .expect("delete");
        for crash_after in 0..first_delete {
            assert!(
                !plan.steps[..=crash_after]
                    .iter()
                    .any(|step| matches!(step, StorePlanStep::Delete { .. })),
                "old inputs remain readable at crash step {crash_after}"
            );
        }
    }

    #[test]
    fn trap_compaction_output_failure_keeps_inputs() {
        let plan = sample_compaction_publication();
        let failed_output_write = plan
            .steps
            .iter()
            .position(|step| matches!(step, StorePlanStep::Write { .. }))
            .expect("output write");
        assert!(
            !plan.steps[..=failed_output_write]
                .iter()
                .any(|step| matches!(step, StorePlanStep::Delete { .. }))
        );
    }

    #[test]
    fn trap_compaction_enospc_keeps_inputs() {
        let plan = sample_compaction_publication();
        let enospc_at = plan
            .steps
            .iter()
            .position(|step| {
                matches!(
                    step,
                    StorePlanStep::Write {
                        file: FileId::Temp { .. },
                        ..
                    }
                )
            })
            .expect("derived output write");
        assert!(
            plan.steps[..=enospc_at]
                .iter()
                .all(|step| !matches!(step, StorePlanStep::Delete { .. }))
        );
        let inputs = [FileId::Sst { file_no: 1 }, FileId::Sst { file_no: 2 }];
        assert!(inputs.iter().all(|input| {
            !plan.steps[..=enospc_at]
                .iter()
                .any(|step| matches!(step, StorePlanStep::Delete { file } if file == input))
        }));
    }

    #[test]
    fn trap_read_amp_counter_matches_physical_reads() {
        let mut stats = StoreStats::default();
        stats.record_get(3, 12_288);
        stats.record_get(1, 4_096);
        assert_eq!(stats.block_reads, 4);
        assert_eq!(stats.bytes_read, 16_384);
        assert_eq!(stats.read_amp_percentile(50, 100), 1);
        assert_eq!(stats.read_amp_percentile(99, 100), 3);
    }

    #[test]
    fn trap_write_amp_accounting_balances_bytes() {
        let mut stats = StoreStats::default();
        stats.record_ingest(100);
        stats.record_write(100);
        stats.record_write(75);
        assert_eq!(stats.write_amplification(), (175, 100));
        assert_eq!(stats.bytes_written, 100 + 75);
    }

    #[test]
    fn trap_every_storage_publish_step_has_a_crash_point() {
        let plan = sample_compaction_publication();
        let points = plan.crash_points();
        assert_eq!(points.len(), plan.steps.len() * 2);
        for index in 0..plan.steps.len() {
            assert!(points.contains(&StorageCrashPoint::BeforeStep(index)));
            assert!(points.contains(&StorageCrashPoint::AfterStep(index)));
        }
    }

    #[test]
    fn trap_manifest_orphan_is_ignored_at_boot() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        store.flush().expect("flush");
        let mut image = store.image();
        image.tables.push((999, b"orphan".to_vec()));
        let reopened = Store::boot(image, config()).expect("orphan is garbage");
        assert_eq!(reopened.get(b"a", None), Some(b"one".to_vec()));
    }

    #[test]
    fn trap_manifest_ghost_fails_closed() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        store.flush().expect("flush");
        let mut image = store.image();
        image.tables.clear();
        assert!(matches!(
            Store::boot(image, config()),
            Err(StoreError::MissingTable { .. })
        ));
    }

    #[test]
    fn oracle_differential_for_small_sequences() {
        for seed in 0..100_u64 {
            let mut test_config = config();
            test_config.memtable_bytes = 1024 * 1024;
            let mut store = Store::new(test_config).expect("store");
            let mut oracle = BTreeMap::new();
            let mut rng = cc_core::Xoshiro256pp::stream(cc_core::Seed::new(seed), "store", 0);
            for _ in 0..200 {
                let key = vec![b'a' + u8::try_from(rng.range_u64(0, 4)).expect("small")];
                if rng.chance(cc_core::P16::new(40_000)) {
                    let value = vec![u8::try_from(rng.range_u64(0, 255)).expect("small")];
                    store.put(&key, &value).expect("put");
                    oracle.insert(key, value);
                } else {
                    store.delete(&key).expect("delete");
                    oracle.remove(&key);
                }
                for (oracle_key, oracle_value) in &oracle {
                    assert_eq!(store.get(oracle_key, None), Some(oracle_value.clone()));
                }
            }
        }
    }

    #[test]
    #[ignore = "G2 long oracle campaign; run explicitly in release mode"]
    fn oracle_differential_campaign_1k_x_200k() {
        let mut test_config = config();
        test_config.memtable_bytes = 1024 * 1024;
        for seed in 0..1_000_u64 {
            let mut store = Store::new(test_config).expect("store");
            let mut oracle = BTreeMap::new();
            let mut rng = cc_core::Xoshiro256pp::stream(cc_core::Seed::new(seed), "oracle", 0);
            for _ in 0..200_000 {
                let key = vec![b'a' + u8::try_from(rng.range_u64(0, 32)).expect("small")];
                if rng.chance(cc_core::P16::new(40_000)) {
                    let value = rng.u64().to_le_bytes().to_vec();
                    retry_after_flush(&mut store, |store| store.put(&key, &value)).expect("put");
                    oracle.insert(key, value);
                } else {
                    retry_after_flush(&mut store, |store| store.delete(&key)).expect("delete");
                    oracle.remove(&key);
                }
            }
            for (key, value) in oracle {
                assert_eq!(store.get(&key, None), Some(value));
            }
        }
    }

    #[test]
    #[ignore = "G2 corruption campaign; run explicitly in release mode"]
    fn corruption_campaign_has_no_silent_wrong_answers() {
        let mut store = Store::new(config()).expect("store");
        store.put(b"a", b"one").expect("put");
        store.flush().expect("flush");
        let image = store.image();
        for (file_no, mut bytes) in image.tables {
            for offset in 0..bytes.len() {
                bytes[offset] ^= 1;
                assert!(
                    SstTable::decode(&bytes).is_err(),
                    "corruption {file_no}:{offset} was silent"
                );
                bytes[offset] ^= 1;
            }
        }
    }

    fn retry_after_flush<T>(
        store: &mut Store,
        mut operation: impl FnMut(&mut Store) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        match operation(store) {
            Err(StoreError::Busy) => {
                store.flush()?;
                operation(store)
            }
            result => result,
        }
    }

    fn hex_bytes(bytes: &[u8]) -> String {
        bytes
            .iter()
            .flat_map(|byte| [format!("{byte:02x}")])
            .collect::<Vec<_>>()
            .join("")
    }
}
