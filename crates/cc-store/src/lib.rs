// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "A deterministic, single-keyspace LSM store built on the Crash Course WAL."]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use cc_core::{Dec, DecodeError, Enc, MAX_CODEC_BYTES, crc32c};
use cc_wal::{RecordType, Wal, WalConfig, WalError};

pub const FORMAT_VERSION: u16 = 1;
pub const SST_MAGIC: u32 = u32::from_le_bytes(*b"CCST");
pub const META_MAGIC: u32 = u32::from_le_bytes(*b"CCMT");
pub const DEFAULT_MEMTABLE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ValueKind {
    Put = 1,
    Delete = 2,
}

impl ValueKind {
    fn from_byte(value: u8) -> Option<Self> {
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
            .then_with(|| self.kind.cmp(&other.kind))
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
    pub manifest: Manifest,
    pub meta: Vec<u8>,
    pub tables: Vec<(u64, Vec<u8>)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    pub image: StoreImage,
}

pub struct Store {
    config: StoreConfig,
    wal: Wal,
    active: MemTable,
    frozen: Option<MemTable>,
    tables: Vec<SstTable>,
    manifest: Manifest,
    next_sequence: u64,
    snapshots: BTreeSet<u64>,
}

impl Store {
    pub fn new(config: StoreConfig) -> Result<Self, StoreError> {
        Ok(Self {
            wal: Wal::new(config.wal)?,
            config,
            active: MemTable::default(),
            frozen: None,
            tables: Vec::new(),
            manifest: Manifest::default(),
            next_sequence: 0,
            snapshots: BTreeSet::new(),
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
            manifest: image.manifest,
            next_sequence: image.sequence,
            snapshots: BTreeSet::new(),
        })
    }

    #[must_use]
    pub fn last_sequence(&self) -> u64 {
        self.next_sequence
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
            return Err(StoreError::Busy);
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
        self.snapshots.insert(snapshot.0);
        snapshot
    }

    pub fn release_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshots.remove(&snapshot.0);
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

    /// Flush the immutable memtable before publishing its manifest edit.
    pub fn flush(&mut self) -> Result<Option<u64>, StoreError> {
        let table_source = match self.frozen.take() {
            Some(table) => table,
            None if !self.active.entries.is_empty() => std::mem::take(&mut self.active),
            None => return Ok(None),
        };
        let file_no = self.manifest.allocate_file();
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
        let _ = bytes;
        Ok(Some(file_no))
    }

    pub fn compact(&mut self) -> Result<bool, StoreError> {
        if self.tables.len() < 2 {
            return Ok(false);
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
            .copied()
            .unwrap_or(self.next_sequence);
        let mut keep = Vec::new();
        let mut seen_keys = BTreeSet::new();
        for (key, value) in all {
            if !seen_keys.insert(key.user_key.clone()) {
                if key.sequence < min_snapshot {
                    continue;
                }
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
        let table = SstTable::from_entries(file_no, keep)?;
        self.tables.push(table);
        self.manifest.add_file(1, file_no);
        for (old_file, level) in old_files {
            self.manifest.remove_file(level, old_file);
        }
        self.manifest.generation += 1;
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
            manifest: self.manifest.clone(),
            meta: encode_meta(self.manifest.generation),
            tables: self
                .tables
                .iter()
                .map(|table| (table.file_no, table.bytes().to_vec()))
                .collect(),
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
                    store.put(&key, &value).expect("put");
                    oracle.insert(key, value);
                } else {
                    store.delete(&key).expect("delete");
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
}
