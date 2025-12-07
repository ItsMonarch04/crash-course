// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Strict `CCMF`/`CCMT` v2 metadata codecs.
//!
//! These types intentionally contain only logical file identities and bytes.
//! A host owns writes, fsync, rename, and directory durability; the store owns
//! the deterministic manifest transition and validates it before publication.

use std::collections::BTreeMap;

use cc_core::{LogIndex, MAX_CODEC_BYTES, Term, Time, crc32c, crc32c_zeroed_tail};

use crate::{
    InternalKey, SST_V2_FOOTER_BYTES, SstV2Limits, SstV2Table, StoreError, StoreWatermark,
    ValueKind, sst_v2_footer_crc32c,
};

pub const MANIFEST_V2_MAGIC: u32 = u32::from_le_bytes(*b"CCMF");
pub const MANIFEST_V2_VERSION: u16 = 1;
pub const META_V2_VERSION: u16 = 2;
const HEADER_BYTES: usize = 18;
const META_BYTES: usize = 22;
const MAX_LEVEL: u8 = 7;

/// The durable checkpoint named by the derived-store manifest.  It is not
/// independently authoritative: boot must also find the matching Raft
/// `SnapshotMark` and checkpoint bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestCheckpoint {
    pub index: LogIndex,
    pub term: Term,
    pub generation: u64,
    pub crc32c: u32,
}

/// Cross-check the derived-store checkpoint edit against the independently
/// durable Raft snapshot mark and the verified checkpoint file checksum.
/// Neither side is authority by itself.
pub fn validate_checkpoint_authority(
    manifest: Option<ManifestCheckpoint>,
    raft_mark: Option<ManifestCheckpoint>,
    verified_file_crc32c: Option<u32>,
) -> Result<ManifestCheckpoint, StoreError> {
    let manifest = manifest.ok_or(StoreError::Corrupt("missing manifest checkpoint"))?;
    let raft_mark = raft_mark.ok_or(StoreError::Corrupt("missing raft snapshot mark"))?;
    if manifest != raft_mark || verified_file_crc32c != Some(manifest.crc32c) {
        return Err(StoreError::Corrupt("checkpoint authority mismatch"));
    }
    Ok(manifest)
}

/// The exact bounded metadata the manifest retains for one v2 table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestFile {
    pub level: u8,
    pub file_no: u64,
    pub file_size: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
    pub footer_crc32c: u32,
}

/// One atomic manifest edit.  An `EditBatch` is applied all-or-nothing only
/// after its record checksum verifies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestEditV2 {
    AddFile(ManifestFile),
    RemoveFile {
        level: u8,
        file_no: u64,
    },
    NextFileNo(u64),
    AppliedWatermark {
        watermark: StoreWatermark,
        store_sequence: u64,
    },
    Checkpoint(Option<ManifestCheckpoint>),
}

/// Fully replayed one-generation manifest state.  `edits` are retained only
/// for deterministic re-encoding and test fixtures; callers may compact to a
/// fresh snapshot at any time without changing the represented state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestV2 {
    pub generation: u64,
    pub next_file_no: u64,
    pub applied_watermark: Option<StoreWatermark>,
    pub store_sequence: u64,
    pub checkpoint: Option<ManifestCheckpoint>,
    pub files: BTreeMap<u64, ManifestFile>,
    pub edits: Vec<Vec<ManifestEditV2>>,
    base: ManifestBase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestBase {
    next_file_no: u64,
    applied_watermark: Option<StoreWatermark>,
    store_sequence: u64,
    checkpoint: Option<ManifestCheckpoint>,
    files: BTreeMap<u64, ManifestFile>,
}

/// The atomic `CCMT` pointer.  The CRC names the target manifest header, not
/// its mutable tail, so a valid META can be checked before replaying records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManifestMetaV2 {
    pub generation: u64,
    pub manifest_header_crc32c: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestBoot {
    pub manifest: ManifestV2,
    pub used_meta_fallback: bool,
    pub orphan_tables: Vec<u64>,
}

/// Select the only manifest generation boot is allowed to expose. A valid
/// META is authoritative and never falls through to another generation; an
/// absent/torn META scans a bounded descending generation set and accepts the
/// first complete closed world.
pub fn select_manifest_generation(
    meta_bytes: Option<&[u8]>,
    generations: &BTreeMap<u64, Vec<u8>>,
    tables: &BTreeMap<u64, Vec<u8>>,
    max_generations: usize,
    limits: SstV2Limits,
) -> Result<ManifestBoot, StoreError> {
    if max_generations == 0 || generations.len() > max_generations {
        return Err(StoreError::TooLarge {
            what: "manifest generations",
            size: generations.len(),
            max: max_generations,
        });
    }
    let load = |generation: u64, expected_header: Option<u32>| {
        let bytes = generations
            .get(&generation)
            .ok_or(StoreError::Corrupt("META target manifest missing"))?;
        if expected_header.is_some_and(|expected| manifest_header_crc(bytes).ok() != Some(expected))
        {
            return Err(StoreError::Corrupt("META target manifest header"));
        }
        let (manifest, _) = decode_manifest_v2_prefix(bytes)?;
        if manifest.generation != generation {
            return Err(StoreError::Corrupt("manifest filename generation"));
        }
        verify_manifest_tables(&manifest, tables, limits)?;
        Ok(manifest)
    };

    if let Some(meta) = meta_bytes.and_then(|bytes| decode_meta_v2(bytes).ok()) {
        let manifest = load(meta.generation, Some(meta.manifest_header_crc32c))?;
        return Ok(ManifestBoot {
            orphan_tables: orphan_tables(&manifest, tables),
            manifest,
            used_meta_fallback: false,
        });
    }

    for generation in generations.keys().rev().copied() {
        if let Ok(manifest) = load(generation, None) {
            return Ok(ManifestBoot {
                orphan_tables: orphan_tables(&manifest, tables),
                manifest,
                used_meta_fallback: true,
            });
        }
    }
    Err(StoreError::Corrupt("no valid manifest generation"))
}

fn verify_manifest_tables(
    manifest: &ManifestV2,
    tables: &BTreeMap<u64, Vec<u8>>,
    limits: SstV2Limits,
) -> Result<(), StoreError> {
    for (file_no, file) in &manifest.files {
        let bytes = tables
            .get(file_no)
            .ok_or(StoreError::MissingTable { file_no: *file_no })?;
        if bytes.len() < SST_V2_FOOTER_BYTES
            || u64::try_from(bytes.len()).unwrap_or(u64::MAX) != file.file_size
            || sst_v2_footer_crc32c(bytes)? != file.footer_crc32c
        {
            return Err(StoreError::Corrupt("manifest table metadata"));
        }
        let decoded = SstV2Table::decode(bytes, limits)?;
        let (smallest, _) = decoded
            .entries
            .first()
            .ok_or(StoreError::Corrupt("manifest empty table"))?;
        let (largest, _) = decoded
            .entries
            .last()
            .ok_or(StoreError::Corrupt("manifest empty table"))?;
        if smallest != &file.smallest || largest != &file.largest {
            return Err(StoreError::Corrupt("manifest table range"));
        }
    }
    Ok(())
}

fn orphan_tables(manifest: &ManifestV2, tables: &BTreeMap<u64, Vec<u8>>) -> Vec<u64> {
    tables
        .keys()
        .filter(|file_no| !manifest.files.contains_key(file_no))
        .copied()
        .collect()
}

impl ManifestV2 {
    #[must_use]
    pub fn empty(generation: u64) -> Self {
        let base = ManifestBase {
            next_file_no: 1,
            applied_watermark: None,
            store_sequence: 0,
            checkpoint: None,
            files: BTreeMap::new(),
        };
        Self {
            generation,
            next_file_no: base.next_file_no,
            applied_watermark: base.applied_watermark,
            store_sequence: base.store_sequence,
            checkpoint: base.checkpoint,
            files: base.files.clone(),
            edits: Vec::new(),
            base,
        }
    }

    /// Start a compact new generation with one complete Snapshot record and
    /// no tail edits.  The caller publishes this bytestring before atomically
    /// replacing `META`; old generation files must remain until that swap is
    /// durable.
    pub fn compact_generation(&self, generation: u64) -> Result<Self, StoreError> {
        if generation <= self.generation {
            return Err(StoreError::InvalidInput("manifest generation regression"));
        }
        validate_state(self)?;
        let base = ManifestBase {
            next_file_no: self.next_file_no,
            applied_watermark: self.applied_watermark,
            store_sequence: self.store_sequence,
            checkpoint: self.checkpoint,
            files: self.files.clone(),
        };
        Ok(Self {
            generation,
            next_file_no: base.next_file_no,
            applied_watermark: base.applied_watermark,
            store_sequence: base.store_sequence,
            checkpoint: base.checkpoint,
            files: base.files.clone(),
            edits: Vec::new(),
            base,
        })
    }

    /// Validate and append one all-or-nothing manifest record.
    pub fn append_edit_batch(&mut self, edits: Vec<ManifestEditV2>) -> Result<(), StoreError> {
        let mut candidate = self.clone();
        candidate.apply_edits(&edits)?;
        candidate.edits.push(edits);
        *self = candidate;
        Ok(())
    }

    /// Return the pointer that atomically names this manifest generation.
    pub fn meta(&self) -> Result<ManifestMetaV2, StoreError> {
        let bytes = encode_manifest_v2(self)?;
        let crc = manifest_header_crc(&bytes)?;
        Ok(ManifestMetaV2 {
            generation: self.generation,
            manifest_header_crc32c: crc,
        })
    }

    fn apply_edits(&mut self, edits: &[ManifestEditV2]) -> Result<(), StoreError> {
        if edits.is_empty() {
            return Err(StoreError::InvalidInput("empty manifest edit batch"));
        }
        for edit in edits {
            match edit {
                ManifestEditV2::AddFile(file) => {
                    validate_file(file)?;
                    if self.files.contains_key(&file.file_no) || file.file_no >= self.next_file_no {
                        return Err(StoreError::Corrupt("manifest file number"));
                    }
                    if file.level > 0 {
                        for existing in self
                            .files
                            .values()
                            .filter(|value| value.level == file.level)
                        {
                            if ranges_overlap(file, existing) {
                                return Err(StoreError::Corrupt("manifest level range overlap"));
                            }
                        }
                    }
                    self.files.insert(file.file_no, file.clone());
                }
                ManifestEditV2::RemoveFile { level, file_no } => {
                    let existing = self
                        .files
                        .get(file_no)
                        .ok_or(StoreError::Corrupt("manifest removes missing file"))?;
                    if existing.level != *level {
                        return Err(StoreError::Corrupt("manifest remove level"));
                    }
                    self.files.remove(file_no);
                }
                ManifestEditV2::NextFileNo(next) => {
                    if *next <= self.next_file_no || *next == 0 {
                        return Err(StoreError::Corrupt("manifest next file number"));
                    }
                    self.next_file_no = *next;
                }
                ManifestEditV2::AppliedWatermark {
                    watermark,
                    store_sequence,
                } => {
                    validate_watermark(*watermark, *store_sequence)?;
                    if self.applied_watermark.is_some_and(|previous| {
                        watermark.index <= previous.index
                            || watermark.last_leader_time < previous.last_leader_time
                            || *store_sequence < self.store_sequence
                    }) {
                        return Err(StoreError::Corrupt("manifest watermark regression"));
                    }
                    self.applied_watermark = Some(*watermark);
                    self.store_sequence = *store_sequence;
                }
                ManifestEditV2::Checkpoint(checkpoint) => {
                    if let Some(checkpoint) = checkpoint {
                        validate_checkpoint(*checkpoint)?;
                        let watermark = self
                            .applied_watermark
                            .ok_or(StoreError::Corrupt("manifest checkpoint without watermark"))?;
                        if checkpoint.index > watermark.index || checkpoint.term.get() == 0 {
                            return Err(StoreError::Corrupt("manifest checkpoint watermark"));
                        }
                    }
                    self.checkpoint = *checkpoint;
                }
            }
        }
        Ok(())
    }
}

/// Canonically encode one generation beginning with its mandatory full
/// snapshot.  All appendable records are bounded before allocation.
pub fn encode_manifest_v2(manifest: &ManifestV2) -> Result<Vec<u8>, StoreError> {
    validate_state(manifest)?;
    // A generation starts from one full Snapshot.  `ManifestV2` keeps the
    // replayed state as well as its append-only tail, so reconstruct the base
    // before serializing rather than writing the final state and applying the
    // same edits a second time during recovery.
    let mut replayed = ManifestV2 {
        generation: manifest.generation,
        next_file_no: manifest.base.next_file_no,
        applied_watermark: manifest.base.applied_watermark,
        store_sequence: manifest.base.store_sequence,
        checkpoint: manifest.base.checkpoint,
        files: manifest.base.files.clone(),
        edits: Vec::new(),
        base: manifest.base.clone(),
    };
    for edits in &manifest.edits {
        replayed.append_edit_batch(edits.clone())?;
    }
    if replayed.next_file_no != manifest.next_file_no
        || replayed.applied_watermark != manifest.applied_watermark
        || replayed.store_sequence != manifest.store_sequence
        || replayed.checkpoint != manifest.checkpoint
        || replayed.files != manifest.files
    {
        return Err(StoreError::InvalidInput(
            "manifest state is not represented by its edit tail",
        ));
    }
    let mut out = Vec::new();
    out.extend_from_slice(&MANIFEST_V2_MAGIC.to_le_bytes());
    out.extend_from_slice(&MANIFEST_V2_VERSION.to_le_bytes());
    out.extend_from_slice(&manifest.generation.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    let header_crc = crc32c_zeroed_tail(&out);
    out[HEADER_BYTES - 4..].copy_from_slice(&header_crc.to_le_bytes());
    append_record(
        &mut out,
        1,
        &encode_snapshot(&ManifestV2 {
            generation: manifest.generation,
            next_file_no: manifest.base.next_file_no,
            applied_watermark: manifest.base.applied_watermark,
            store_sequence: manifest.base.store_sequence,
            checkpoint: manifest.base.checkpoint,
            files: manifest.base.files.clone(),
            edits: Vec::new(),
            base: manifest.base.clone(),
        })?,
    )?;
    for edits in &manifest.edits {
        append_record(&mut out, 2, &encode_edit_batch(edits)?)?;
    }
    Ok(out)
}

/// Decode a complete manifest.  A torn final record is intentionally not
/// accepted by this strict API; boot callers inside this crate that need
/// prefix recovery use the crate-internal `decode_manifest_v2_prefix`.
pub fn decode_manifest_v2(bytes: &[u8]) -> Result<ManifestV2, StoreError> {
    let (manifest, torn_tail) = decode_manifest_v2_prefix(bytes)?;
    if torn_tail {
        return Err(StoreError::Corrupt("manifest torn final record"));
    }
    Ok(manifest)
}

/// Replay a manifest's complete-record prefix.  Only a truncated final
/// record is ignored; checksum/tag/semantic failures in a complete record
/// remain corruption.
pub fn decode_manifest_v2_prefix(bytes: &[u8]) -> Result<(ManifestV2, bool), StoreError> {
    if bytes.len() < HEADER_BYTES {
        return Err(StoreError::Corrupt("manifest header"));
    }
    let magic = take_u32(bytes, 0)?;
    let version = take_u16(bytes, 4)?;
    if magic != MANIFEST_V2_MAGIC || version != MANIFEST_V2_VERSION {
        return Err(StoreError::Corrupt("manifest format"));
    }
    let expected = take_u32(bytes, HEADER_BYTES - 4)?;
    if crc32c_zeroed_tail(&bytes[..HEADER_BYTES]) != expected {
        return Err(StoreError::Corrupt("manifest header CRC"));
    }
    let generation = take_u64(bytes, 6)?;
    let mut cursor = HEADER_BYTES;
    let mut snapshot: Option<ManifestV2> = None;
    let mut torn_tail = false;
    while cursor < bytes.len() {
        let remaining = bytes.len() - cursor;
        if remaining < 9 {
            torn_tail = true;
            break;
        }
        let body_len = usize::try_from(take_u32(bytes, cursor)?).unwrap_or(usize::MAX);
        if body_len > MAX_CODEC_BYTES {
            return Err(StoreError::TooLarge {
                what: "manifest record",
                size: body_len,
                max: MAX_CODEC_BYTES,
            });
        }
        let total = 9_usize
            .checked_add(body_len)
            .ok_or(StoreError::Corrupt("manifest record size"))?;
        if remaining < total {
            torn_tail = true;
            break;
        }
        let record = &bytes[cursor..cursor + total];
        let expected_crc = take_u32(record, 4)?;
        let tag = record[8];
        let body = &record[9..];
        let mut checksummed = Vec::with_capacity(1 + body.len());
        checksummed.push(tag);
        checksummed.extend_from_slice(body);
        if crc32c(&checksummed) != expected_crc {
            return Err(StoreError::Corrupt("manifest record CRC"));
        }
        match tag {
            1 if snapshot.is_none() => snapshot = Some(decode_snapshot(generation, body)?),
            1 => return Err(StoreError::Corrupt("manifest duplicate snapshot")),
            2 => {
                let state = snapshot
                    .as_mut()
                    .ok_or(StoreError::Corrupt("manifest edit before snapshot"))?;
                let edits = decode_edit_batch(body)?;
                state.append_edit_batch(edits)?;
            }
            _ => return Err(StoreError::Corrupt("manifest record tag")),
        }
        cursor += total;
    }
    let manifest = snapshot.ok_or(StoreError::Corrupt("manifest missing snapshot"))?;
    validate_state(&manifest)?;
    Ok((manifest, torn_tail))
}

/// Encode the atomic `CCMT` pointer to one fully durable generation.
pub fn encode_meta_v2(meta: ManifestMetaV2) -> Vec<u8> {
    let mut out = Vec::with_capacity(META_BYTES);
    out.extend_from_slice(&crate::META_MAGIC.to_le_bytes());
    out.extend_from_slice(&META_V2_VERSION.to_le_bytes());
    out.extend_from_slice(&meta.generation.to_le_bytes());
    out.extend_from_slice(&meta.manifest_header_crc32c.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c_zeroed_tail(&out);
    out[META_BYTES - 4..].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_meta_v2(bytes: &[u8]) -> Result<ManifestMetaV2, StoreError> {
    if bytes.len() != META_BYTES
        || take_u32(bytes, 0)? != crate::META_MAGIC
        || take_u16(bytes, 4)? != META_V2_VERSION
    {
        return Err(StoreError::Corrupt("META v2 format"));
    }
    let expected = take_u32(bytes, META_BYTES - 4)?;
    if crc32c_zeroed_tail(bytes) != expected {
        return Err(StoreError::Corrupt("META v2 CRC"));
    }
    Ok(ManifestMetaV2 {
        generation: take_u64(bytes, 6)?,
        manifest_header_crc32c: take_u32(bytes, 14)?,
    })
}

pub(crate) fn manifest_header_crc(bytes: &[u8]) -> Result<u32, StoreError> {
    if bytes.len() < HEADER_BYTES {
        return Err(StoreError::Corrupt("manifest header"));
    }
    let crc = take_u32(bytes, HEADER_BYTES - 4)?;
    if crc32c_zeroed_tail(&bytes[..HEADER_BYTES]) != crc {
        return Err(StoreError::Corrupt("manifest header CRC"));
    }
    Ok(crc)
}

fn encode_snapshot(manifest: &ManifestV2) -> Result<Vec<u8>, StoreError> {
    let mut out = Vec::new();
    put_u64(&mut out, manifest.next_file_no);
    match manifest.applied_watermark {
        Some(watermark) => {
            validate_watermark(watermark, manifest.store_sequence)?;
            put_u64(&mut out, watermark.index.get());
            put_u64(&mut out, watermark.term.get());
            put_u64(&mut out, watermark.last_leader_time.as_nanos());
        }
        None => {
            put_u64(&mut out, 0);
            put_u64(&mut out, 0);
            put_u64(&mut out, 0);
        }
    }
    put_u64(&mut out, manifest.store_sequence);
    match manifest.checkpoint {
        Some(checkpoint) => {
            validate_checkpoint(checkpoint)?;
            out.push(1);
            put_u64(&mut out, checkpoint.index.get());
            put_u64(&mut out, checkpoint.term.get());
            put_u64(&mut out, checkpoint.generation);
            put_u32(&mut out, checkpoint.crc32c);
        }
        None => {
            out.push(0);
            out.extend_from_slice(&[0; 28]);
        }
    }
    put_u32(
        &mut out,
        u32::try_from(manifest.files.len()).map_err(|_| StoreError::TooLarge {
            what: "manifest files",
            size: manifest.files.len(),
            max: u32::MAX as usize,
        })?,
    );
    for file in manifest.files.values() {
        encode_file(&mut out, file)?;
    }
    bounded(&out, "manifest snapshot")
}

fn decode_snapshot(generation: u64, body: &[u8]) -> Result<ManifestV2, StoreError> {
    let mut cursor = 0_usize;
    let next_file_no = take_u64_cursor(body, &mut cursor)?;
    if next_file_no == 0 {
        return Err(StoreError::Corrupt("manifest next file number"));
    }
    let index = take_u64_cursor(body, &mut cursor)?;
    let term = take_u64_cursor(body, &mut cursor)?;
    let time = take_u64_cursor(body, &mut cursor)?;
    let store_sequence = take_u64_cursor(body, &mut cursor)?;
    let applied_watermark = match (index, term, time) {
        (0, 0, 0) => None,
        (index, term, time) if index > 0 && term > 0 => Some(StoreWatermark {
            index: LogIndex::new(index),
            term: Term::new(term),
            last_leader_time: Time::from_nanos(time),
        }),
        _ => return Err(StoreError::Corrupt("manifest watermark")),
    };
    let has_checkpoint = take_u8_cursor(body, &mut cursor)?;
    let checkpoint_index = take_u64_cursor(body, &mut cursor)?;
    let checkpoint_term = take_u64_cursor(body, &mut cursor)?;
    let checkpoint_generation = take_u64_cursor(body, &mut cursor)?;
    let checkpoint_crc32c = take_u32_cursor(body, &mut cursor)?;
    let checkpoint = match has_checkpoint {
        0 if checkpoint_index == 0
            && checkpoint_term == 0
            && checkpoint_generation == 0
            && checkpoint_crc32c == 0 =>
        {
            None
        }
        1 if checkpoint_index > 0 && checkpoint_term > 0 && checkpoint_generation > 0 => {
            Some(ManifestCheckpoint {
                index: LogIndex::new(checkpoint_index),
                term: Term::new(checkpoint_term),
                generation: checkpoint_generation,
                crc32c: checkpoint_crc32c,
            })
        }
        _ => return Err(StoreError::Corrupt("manifest checkpoint")),
    };
    let count = usize::try_from(take_u32_cursor(body, &mut cursor)?).unwrap_or(usize::MAX);
    if count > MAX_CODEC_BYTES {
        return Err(StoreError::TooLarge {
            what: "manifest files",
            size: count,
            max: MAX_CODEC_BYTES,
        });
    }
    let mut files = BTreeMap::new();
    for _ in 0..count {
        let file = decode_file(body, &mut cursor)?;
        if files.insert(file.file_no, file).is_some() {
            return Err(StoreError::Corrupt("manifest duplicate file"));
        }
    }
    if cursor != body.len() {
        return Err(StoreError::Corrupt("manifest snapshot trailing bytes"));
    }
    let base = ManifestBase {
        next_file_no,
        applied_watermark,
        store_sequence,
        checkpoint,
        files: files.clone(),
    };
    let manifest = ManifestV2 {
        generation,
        next_file_no: base.next_file_no,
        applied_watermark: base.applied_watermark,
        store_sequence: base.store_sequence,
        checkpoint: base.checkpoint,
        files,
        edits: Vec::new(),
        base,
    };
    validate_state(&manifest)?;
    Ok(manifest)
}

fn encode_edit_batch(edits: &[ManifestEditV2]) -> Result<Vec<u8>, StoreError> {
    if edits.is_empty() {
        return Err(StoreError::InvalidInput("empty manifest edit batch"));
    }
    let mut out = Vec::new();
    put_u32(
        &mut out,
        u32::try_from(edits.len()).map_err(|_| StoreError::TooLarge {
            what: "manifest edits",
            size: edits.len(),
            max: u32::MAX as usize,
        })?,
    );
    for edit in edits {
        let (tag, body) = match edit {
            ManifestEditV2::AddFile(file) => {
                let mut body = Vec::new();
                encode_file(&mut body, file)?;
                (1, body)
            }
            ManifestEditV2::RemoveFile { level, file_no } => {
                let mut body = vec![*level];
                put_u64(&mut body, *file_no);
                (2, body)
            }
            ManifestEditV2::NextFileNo(next) => (3, next.to_le_bytes().to_vec()),
            ManifestEditV2::AppliedWatermark {
                watermark,
                store_sequence,
            } => {
                validate_watermark(*watermark, *store_sequence)?;
                let mut body = Vec::new();
                put_u64(&mut body, watermark.index.get());
                put_u64(&mut body, watermark.term.get());
                put_u64(&mut body, watermark.last_leader_time.as_nanos());
                put_u64(&mut body, *store_sequence);
                (4, body)
            }
            ManifestEditV2::Checkpoint(checkpoint) => {
                let mut body = Vec::new();
                match checkpoint {
                    Some(checkpoint) => {
                        validate_checkpoint(*checkpoint)?;
                        body.push(1);
                        put_u64(&mut body, checkpoint.index.get());
                        put_u64(&mut body, checkpoint.term.get());
                        put_u64(&mut body, checkpoint.generation);
                        put_u32(&mut body, checkpoint.crc32c);
                    }
                    None => {
                        body.push(0);
                        body.extend_from_slice(&[0; 28]);
                    }
                }
                (5, body)
            }
        };
        bounded(&body, "manifest edit")?;
        out.push(tag);
        put_u32(
            &mut out,
            u32::try_from(body.len()).map_err(|_| StoreError::TooLarge {
                what: "manifest edit",
                size: body.len(),
                max: u32::MAX as usize,
            })?,
        );
        out.extend_from_slice(&body);
    }
    bounded(&out, "manifest edit batch")
}

fn decode_edit_batch(body: &[u8]) -> Result<Vec<ManifestEditV2>, StoreError> {
    let mut cursor = 0_usize;
    let count = usize::try_from(take_u32_cursor(body, &mut cursor)?).unwrap_or(usize::MAX);
    if count == 0 || count > MAX_CODEC_BYTES {
        return Err(StoreError::Corrupt("manifest edit count"));
    }
    let mut edits = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = take_u8_cursor(body, &mut cursor)?;
        let len = usize::try_from(take_u32_cursor(body, &mut cursor)?).unwrap_or(usize::MAX);
        if len > MAX_CODEC_BYTES {
            return Err(StoreError::TooLarge {
                what: "manifest edit",
                size: len,
                max: MAX_CODEC_BYTES,
            });
        }
        let edit_body = take_slice_cursor(body, &mut cursor, len)?;
        let edit = match tag {
            1 => {
                let mut body_cursor = 0;
                let file = decode_file(edit_body, &mut body_cursor)?;
                if body_cursor != edit_body.len() {
                    return Err(StoreError::Corrupt("manifest AddFile trailing bytes"));
                }
                ManifestEditV2::AddFile(file)
            }
            2 if edit_body.len() == 9 => ManifestEditV2::RemoveFile {
                level: edit_body[0],
                file_no: take_u64(edit_body, 1)?,
            },
            3 if edit_body.len() == 8 => ManifestEditV2::NextFileNo(take_u64(edit_body, 0)?),
            4 if edit_body.len() == 32 => ManifestEditV2::AppliedWatermark {
                watermark: StoreWatermark {
                    index: LogIndex::new(take_u64(edit_body, 0)?),
                    term: Term::new(take_u64(edit_body, 8)?),
                    last_leader_time: Time::from_nanos(take_u64(edit_body, 16)?),
                },
                store_sequence: take_u64(edit_body, 24)?,
            },
            5 if edit_body.len() == 29 => {
                let present = edit_body[0];
                let index = take_u64(edit_body, 1)?;
                let term = take_u64(edit_body, 9)?;
                let generation = take_u64(edit_body, 17)?;
                let crc32c = take_u32(edit_body, 25)?;
                let checkpoint = match present {
                    0 if index == 0 && term == 0 && generation == 0 && crc32c == 0 => None,
                    1 if index > 0 && term > 0 && generation > 0 => Some(ManifestCheckpoint {
                        index: LogIndex::new(index),
                        term: Term::new(term),
                        generation,
                        crc32c,
                    }),
                    _ => return Err(StoreError::Corrupt("manifest checkpoint edit")),
                };
                ManifestEditV2::Checkpoint(checkpoint)
            }
            _ => return Err(StoreError::Corrupt("manifest edit tag")),
        };
        edits.push(edit);
    }
    if cursor != body.len() {
        return Err(StoreError::Corrupt("manifest edit batch trailing bytes"));
    }
    Ok(edits)
}

fn append_record(out: &mut Vec<u8>, tag: u8, body: &[u8]) -> Result<(), StoreError> {
    bounded(body, "manifest record")?;
    put_u32(
        out,
        u32::try_from(body.len()).map_err(|_| StoreError::TooLarge {
            what: "manifest record",
            size: body.len(),
            max: u32::MAX as usize,
        })?,
    );
    let crc_start = out.len();
    put_u32(out, 0);
    out.push(tag);
    out.extend_from_slice(body);
    let mut checksummed = Vec::with_capacity(body.len() + 1);
    checksummed.push(tag);
    checksummed.extend_from_slice(body);
    out[crc_start..crc_start + 4].copy_from_slice(&crc32c(&checksummed).to_le_bytes());
    Ok(())
}

fn encode_file(out: &mut Vec<u8>, file: &ManifestFile) -> Result<(), StoreError> {
    validate_file(file)?;
    out.push(file.level);
    put_u64(out, file.file_no);
    put_u64(out, file.file_size);
    encode_internal_key(out, &file.smallest)?;
    encode_internal_key(out, &file.largest)?;
    put_u32(out, file.footer_crc32c);
    Ok(())
}

fn decode_file(bytes: &[u8], cursor: &mut usize) -> Result<ManifestFile, StoreError> {
    let file = ManifestFile {
        level: take_u8_cursor(bytes, cursor)?,
        file_no: take_u64_cursor(bytes, cursor)?,
        file_size: take_u64_cursor(bytes, cursor)?,
        smallest: decode_internal_key(bytes, cursor)?,
        largest: decode_internal_key(bytes, cursor)?,
        footer_crc32c: take_u32_cursor(bytes, cursor)?,
    };
    validate_file(&file)?;
    Ok(file)
}

fn encode_internal_key(out: &mut Vec<u8>, key: &InternalKey) -> Result<(), StoreError> {
    if key.user_key.is_empty() || key.user_key.len() > MAX_CODEC_BYTES || key.sequence == 0 {
        return Err(StoreError::InvalidInput("manifest internal key"));
    }
    put_u32(
        out,
        u32::try_from(key.user_key.len()).map_err(|_| StoreError::TooLarge {
            what: "manifest key",
            size: key.user_key.len(),
            max: u32::MAX as usize,
        })?,
    );
    out.extend_from_slice(&key.user_key);
    put_u64(out, key.sequence);
    out.push(key.kind as u8);
    Ok(())
}

fn decode_internal_key(bytes: &[u8], cursor: &mut usize) -> Result<InternalKey, StoreError> {
    let len = usize::try_from(take_u32_cursor(bytes, cursor)?).unwrap_or(usize::MAX);
    if len == 0 || len > MAX_CODEC_BYTES {
        return Err(StoreError::Corrupt("manifest key length"));
    }
    let key = take_slice_cursor(bytes, cursor, len)?.to_vec();
    let sequence = take_u64_cursor(bytes, cursor)?;
    let kind = ValueKind::from_byte(take_u8_cursor(bytes, cursor)?)
        .ok_or(StoreError::Corrupt("manifest key kind"))?;
    if sequence == 0 {
        return Err(StoreError::Corrupt("manifest key sequence"));
    }
    Ok(InternalKey::new(key, sequence, kind))
}

fn validate_state(manifest: &ManifestV2) -> Result<(), StoreError> {
    if manifest.generation == 0 || manifest.next_file_no == 0 {
        return Err(StoreError::InvalidInput("manifest next file number"));
    }
    if let Some(watermark) = manifest.applied_watermark {
        validate_watermark(watermark, manifest.store_sequence)?;
    } else if manifest.store_sequence != 0 || manifest.checkpoint.is_some() {
        return Err(StoreError::InvalidInput("manifest missing watermark"));
    }
    if let Some(checkpoint) = manifest.checkpoint {
        validate_checkpoint(checkpoint)?;
        let watermark = manifest
            .applied_watermark
            .ok_or(StoreError::InvalidInput("manifest checkpoint watermark"))?;
        if checkpoint.index > watermark.index {
            return Err(StoreError::InvalidInput("manifest checkpoint watermark"));
        }
    }
    let mut previous_by_level: BTreeMap<u8, &ManifestFile> = BTreeMap::new();
    for file in manifest.files.values() {
        validate_file(file)?;
        if file.file_no >= manifest.next_file_no {
            return Err(StoreError::InvalidInput("manifest next file number"));
        }
        if file.level > 0 {
            if let Some(previous) = previous_by_level.get(&file.level)
                && ranges_overlap(previous, file)
            {
                return Err(StoreError::InvalidInput("manifest level range overlap"));
            }
            previous_by_level.insert(file.level, file);
        }
    }
    Ok(())
}

fn validate_file(file: &ManifestFile) -> Result<(), StoreError> {
    if file.level > MAX_LEVEL
        || file.file_no == 0
        || file.file_size == 0
        || file.smallest > file.largest
    {
        return Err(StoreError::InvalidInput("manifest file metadata"));
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: ManifestCheckpoint) -> Result<(), StoreError> {
    if checkpoint.index.get() == 0 || checkpoint.term.get() == 0 || checkpoint.generation == 0 {
        return Err(StoreError::InvalidInput("manifest checkpoint"));
    }
    Ok(())
}

fn validate_watermark(watermark: StoreWatermark, _store_sequence: u64) -> Result<(), StoreError> {
    // A committed config/no-op prefix can have a nonzero applied Raft
    // watermark while the logical store has never allocated an MVCC
    // sequence. Such an empty checkpoint is valid and must still be
    // publishable for prefix reclamation.
    if watermark.index.get() == 0 || watermark.term.get() == 0 {
        return Err(StoreError::InvalidInput("manifest watermark"));
    }
    Ok(())
}

fn ranges_overlap(left: &ManifestFile, right: &ManifestFile) -> bool {
    left.smallest.user_key <= right.largest.user_key
        && right.smallest.user_key <= left.largest.user_key
}

fn bounded(bytes: &[u8], what: &'static str) -> Result<Vec<u8>, StoreError> {
    if bytes.len() > MAX_CODEC_BYTES {
        return Err(StoreError::TooLarge {
            what,
            size: bytes.len(),
            max: MAX_CODEC_BYTES,
        });
    }
    Ok(bytes.to_vec())
}

fn take_slice_cursor<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], StoreError> {
    let end = cursor
        .checked_add(len)
        .ok_or(StoreError::Corrupt("manifest range"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(StoreError::Corrupt("manifest truncated"))?;
    *cursor = end;
    Ok(value)
}

fn take_u8_cursor(bytes: &[u8], cursor: &mut usize) -> Result<u8, StoreError> {
    Ok(*take_slice_cursor(bytes, cursor, 1)?
        .first()
        .ok_or(StoreError::Corrupt("manifest truncated"))?)
}

fn take_u16(bytes: &[u8], offset: usize) -> Result<u16, StoreError> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or(StoreError::Corrupt("manifest truncated"))?
            .try_into()
            .map_err(|_| StoreError::Corrupt("manifest truncated"))?,
    ))
}

fn take_u32(bytes: &[u8], offset: usize) -> Result<u32, StoreError> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(StoreError::Corrupt("manifest truncated"))?
            .try_into()
            .map_err(|_| StoreError::Corrupt("manifest truncated"))?,
    ))
}

fn take_u64(bytes: &[u8], offset: usize) -> Result<u64, StoreError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(StoreError::Corrupt("manifest truncated"))?
            .try_into()
            .map_err(|_| StoreError::Corrupt("manifest truncated"))?,
    ))
}

fn take_u32_cursor(bytes: &[u8], cursor: &mut usize) -> Result<u32, StoreError> {
    let value = take_slice_cursor(bytes, cursor, 4)?;
    Ok(u32::from_le_bytes(
        value
            .try_into()
            .map_err(|_| StoreError::Corrupt("manifest truncated"))?,
    ))
}

fn take_u64_cursor(bytes: &[u8], cursor: &mut usize) -> Result<u64, StoreError> {
    let value = take_slice_cursor(bytes, cursor, 8)?;
    Ok(u64::from_le_bytes(
        value
            .try_into()
            .map_err(|_| StoreError::Corrupt("manifest truncated"))?,
    ))
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &[u8], sequence: u64) -> InternalKey {
        InternalKey::new(name.to_vec(), sequence, ValueKind::Put)
    }

    fn file(file_no: u64, smallest: &[u8], largest: &[u8]) -> ManifestFile {
        ManifestFile {
            level: 0,
            file_no,
            file_size: 99,
            smallest: key(smallest, 1),
            largest: key(largest, 1),
            footer_crc32c: file_no as u32,
        }
    }

    fn watermark(index: u64) -> StoreWatermark {
        StoreWatermark {
            index: LogIndex::new(index),
            term: Term::new(1),
            last_leader_time: Time::from_nanos(index),
        }
    }

    fn complete_generation(generation: u64) -> (ManifestV2, Vec<u8>, BTreeMap<u64, Vec<u8>>) {
        let limits = SstV2Limits::default();
        let entries = vec![(key(b"a", generation), b"value".to_vec())];
        let table = SstV2Table::encode(entries.clone(), limits).expect("table");
        let mut manifest = ManifestV2::empty(generation);
        manifest
            .append_edit_batch(vec![
                ManifestEditV2::NextFileNo(2),
                ManifestEditV2::AddFile(ManifestFile {
                    level: 0,
                    file_no: 1,
                    file_size: table.len() as u64,
                    smallest: entries[0].0.clone(),
                    largest: entries[0].0.clone(),
                    footer_crc32c: sst_v2_footer_crc32c(&table).expect("footer"),
                }),
                ManifestEditV2::AppliedWatermark {
                    watermark: watermark(generation),
                    store_sequence: generation,
                },
            ])
            .expect("generation");
        let bytes = encode_manifest_v2(&manifest).expect("manifest bytes");
        (manifest, bytes, BTreeMap::from([(1, table)]))
    }

    #[test]
    fn manifest_v2_round_trips_atomic_edits_and_meta_pointer() {
        let mut manifest = ManifestV2::empty(7);
        manifest
            .append_edit_batch(vec![
                ManifestEditV2::NextFileNo(2),
                ManifestEditV2::AddFile(file(1, b"a", b"z")),
                ManifestEditV2::AppliedWatermark {
                    watermark: watermark(3),
                    store_sequence: 5,
                },
            ])
            .expect("edit");
        let bytes = encode_manifest_v2(&manifest).expect("encode");
        let decoded = decode_manifest_v2(&bytes).expect("decode");
        assert_eq!(decoded, manifest);
        let meta = manifest.meta().expect("meta");
        assert_eq!(
            decode_meta_v2(&encode_meta_v2(meta)).expect("decode meta"),
            meta
        );
    }

    #[test]
    fn trap_manifest_torn_tail_replays_only_complete_edit_prefix() {
        let mut manifest = ManifestV2::empty(1);
        manifest
            .append_edit_batch(vec![ManifestEditV2::NextFileNo(2)])
            .expect("first edit");
        let mut bytes = encode_manifest_v2(&manifest).expect("encode");
        let beginning = bytes.len();
        append_record(
            &mut bytes,
            2,
            &encode_edit_batch(&[ManifestEditV2::NextFileNo(3)]).expect("second edit"),
        )
        .expect("append");
        bytes.truncate(beginning + 6);
        let (decoded, torn) = decode_manifest_v2_prefix(&bytes).expect("prefix recovery");
        assert!(torn);
        assert_eq!(decoded.next_file_no, 2);
        assert!(decode_manifest_v2(&bytes).is_err());
    }

    #[test]
    fn trap_manifest_rejects_complete_checksum_and_watermark_regressions() {
        let mut manifest = ManifestV2::empty(1);
        manifest
            .append_edit_batch(vec![ManifestEditV2::AppliedWatermark {
                watermark: watermark(2),
                store_sequence: 2,
            }])
            .expect("watermark");
        assert!(
            manifest
                .append_edit_batch(vec![ManifestEditV2::AppliedWatermark {
                    watermark: watermark(1),
                    store_sequence: 1,
                }])
                .is_err()
        );
        let mut bytes = encode_manifest_v2(&manifest).expect("encode");
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(decode_manifest_v2_prefix(&bytes).is_err());
    }

    #[test]
    fn trap_manifest_compaction_rewrites_a_complete_next_generation() {
        let mut manifest = ManifestV2::empty(3);
        manifest
            .append_edit_batch(vec![
                ManifestEditV2::NextFileNo(2),
                ManifestEditV2::AddFile(file(1, b"a", b"z")),
                ManifestEditV2::AppliedWatermark {
                    watermark: watermark(4),
                    store_sequence: 6,
                },
            ])
            .expect("edit");
        let compact = manifest.compact_generation(4).expect("compact generation");
        assert!(compact.edits.is_empty());
        assert_eq!(
            decode_manifest_v2(&encode_manifest_v2(&compact).expect("encode")).expect("decode"),
            compact
        );
        assert!(manifest.compact_generation(3).is_err());
    }

    #[test]
    fn trap_meta_torn_write_falls_back_to_fully_durable_generation() {
        let (manifest, bytes, tables) = complete_generation(2);
        let mut torn_meta = encode_meta_v2(manifest.meta().expect("meta"));
        torn_meta.truncate(torn_meta.len() - 1);
        let boot = select_manifest_generation(
            Some(&torn_meta),
            &BTreeMap::from([(2, bytes)]),
            &tables,
            4,
            SstV2Limits::default(),
        )
        .expect("fallback");
        assert!(boot.used_meta_fallback);
        assert_eq!(boot.manifest.generation, 2);
    }

    #[test]
    fn trap_valid_meta_with_bad_target_fails_closed() {
        let (old, old_bytes, tables) = complete_generation(1);
        let (new, mut new_bytes, _) = complete_generation(2);
        new_bytes[0] ^= 1;
        let generations = BTreeMap::from([(1, old_bytes), (2, new_bytes)]);
        assert!(
            select_manifest_generation(
                Some(&encode_meta_v2(new.meta().expect("new meta"))),
                &generations,
                &tables,
                4,
                SstV2Limits::default(),
            )
            .is_err()
        );
        assert_eq!(old.generation, 1);
    }

    #[test]
    fn trap_no_valid_manifest_fails_closed() {
        assert!(
            select_manifest_generation(
                None,
                &BTreeMap::from([(1, b"not-a-manifest".to_vec())]),
                &BTreeMap::new(),
                4,
                SstV2Limits::default(),
            )
            .is_err()
        );
    }

    #[test]
    fn trap_orphan_sst_is_gc_safe() {
        let (manifest, bytes, mut tables) = complete_generation(1);
        tables.insert(99, b"orphan".to_vec());
        let boot = select_manifest_generation(
            Some(&encode_meta_v2(manifest.meta().expect("meta"))),
            &BTreeMap::from([(1, bytes)]),
            &tables,
            4,
            SstV2Limits::default(),
        )
        .expect("boot ignores orphan");
        assert_eq!(boot.orphan_tables, vec![99]);
    }

    #[test]
    fn trap_ghost_sst_fails_closed() {
        let (manifest, bytes, _) = complete_generation(1);
        assert!(
            select_manifest_generation(
                Some(&encode_meta_v2(manifest.meta().expect("meta"))),
                &BTreeMap::from([(1, bytes)]),
                &BTreeMap::new(),
                4,
                SstV2Limits::default(),
            )
            .is_err()
        );
    }

    #[test]
    fn trap_manifest_rewrite_is_atomic_under_every_crash_point() {
        let (old, old_bytes, tables) = complete_generation(1);
        let new = old.compact_generation(2).expect("rewrite");
        let new_bytes = encode_manifest_v2(&new).expect("new bytes");
        let old_meta = encode_meta_v2(old.meta().expect("old meta"));
        let new_meta = encode_meta_v2(new.meta().expect("new meta"));

        let only_old = BTreeMap::from([(1, old_bytes.clone())]);
        assert_eq!(
            select_manifest_generation(
                Some(&old_meta),
                &only_old,
                &tables,
                4,
                SstV2Limits::default(),
            )
            .expect("old")
            .manifest
            .generation,
            1
        );
        let both = BTreeMap::from([(1, old_bytes), (2, new_bytes)]);
        assert_eq!(
            select_manifest_generation(Some(&old_meta), &both, &tables, 4, SstV2Limits::default(),)
                .expect("pre-swap")
                .manifest
                .generation,
            1
        );
        assert_eq!(
            select_manifest_generation(
                Some(&new_meta[..new_meta.len() - 1]),
                &both,
                &tables,
                4,
                SstV2Limits::default(),
            )
            .expect("torn swap fallback")
            .manifest
            .generation,
            2
        );
        assert_eq!(
            select_manifest_generation(Some(&new_meta), &both, &tables, 4, SstV2Limits::default(),)
                .expect("post-swap")
                .manifest
                .generation,
            2
        );
    }

    #[test]
    fn trap_manifest_rejects_file_number_and_watermark_regression() {
        trap_manifest_rejects_complete_checksum_and_watermark_regressions();
    }

    fn checkpoint_mark() -> ManifestCheckpoint {
        ManifestCheckpoint {
            index: LogIndex::new(7),
            term: Term::new(3),
            generation: 7,
            crc32c: 0x1234_5678,
        }
    }

    #[test]
    fn trap_manifest_only_checkpoint_is_not_log_authority() {
        assert_eq!(
            validate_checkpoint_authority(Some(checkpoint_mark()), None, Some(0x1234_5678)),
            Err(StoreError::Corrupt("missing raft snapshot mark"))
        );
    }

    #[test]
    fn trap_snapshot_mark_requires_matching_manifest_checkpoint() {
        let mut different = checkpoint_mark();
        different.generation = 8;
        assert_eq!(
            validate_checkpoint_authority(
                Some(checkpoint_mark()),
                Some(different),
                Some(0x1234_5678),
            ),
            Err(StoreError::Corrupt("checkpoint authority mismatch"))
        );
        assert_eq!(
            validate_checkpoint_authority(
                Some(checkpoint_mark()),
                Some(checkpoint_mark()),
                Some(0x1234_5678),
            ),
            Ok(checkpoint_mark())
        );
    }
}
