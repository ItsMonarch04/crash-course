// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Canonical CCBK logical-backup envelope shared by production and fixtures.

use std::io;

use cc_core::{ClusterId, LogIndex, Term, Time, crc32c};

use crate::decode_ccsn;

pub const BACKUP_MAGIC: &[u8; 4] = b"CCBK";
pub const BACKUP_VERSION: u16 = 2;
pub const LEGACY_BACKUP_VERSION: u16 = 1;
pub const BACKUP_V2_HEADER_BYTES: usize = 80;
pub const BACKUP_V2_FOOTER_BYTES: usize = 8;
pub const BACKUP_MAX_CHECKPOINT_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct BackupV2 {
    pub source_cluster_id: ClusterId,
    pub source_index: LogIndex,
    pub source_term: Term,
    pub source_last_leader_time: Time,
    pub source_policy_hash: u64,
    pub source_min_semantic: u16,
    pub source_active_features: u64,
    pub checkpoint: Vec<u8>,
    pub provenance: BackupProvenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackupProvenance {
    LogicalCluster,
    LegacyNode,
}

impl BackupV2 {
    #[must_use]
    pub const fn counts_as_cluster_complete(&self) -> bool {
        matches!(self.provenance, BackupProvenance::LogicalCluster)
    }
}

pub fn encode_backup_v2(backup: &BackupV2) -> io::Result<Vec<u8>> {
    if backup.source_cluster_id.is_zero()
        || backup.source_index.get() == 0
        || backup.source_term.get() == 0
        || backup.source_min_semantic == 0
        || backup.source_min_semantic > cc_raft::SEMANTIC_VERSION_V3
        || backup.checkpoint.is_empty()
        || backup.checkpoint.len() > BACKUP_MAX_CHECKPOINT_BYTES
        || backup.provenance != BackupProvenance::LogicalCluster
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup metadata",
        ));
    }
    let checkpoint_len = u64::try_from(backup.checkpoint.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup checkpoint length"))?;
    let checkpoint_crc = crc32c(&backup.checkpoint);
    let mut bytes = Vec::with_capacity(
        BACKUP_V2_HEADER_BYTES
            .saturating_add(backup.checkpoint.len())
            .saturating_add(BACKUP_V2_FOOTER_BYTES),
    );
    bytes.extend_from_slice(BACKUP_MAGIC);
    bytes.extend_from_slice(&BACKUP_VERSION.to_le_bytes());
    bytes.extend_from_slice(&backup.source_cluster_id.bytes());
    bytes.extend_from_slice(&backup.source_index.get().to_le_bytes());
    bytes.extend_from_slice(&backup.source_term.get().to_le_bytes());
    bytes.extend_from_slice(&backup.source_last_leader_time.as_nanos().to_le_bytes());
    bytes.extend_from_slice(&backup.source_policy_hash.to_le_bytes());
    bytes.extend_from_slice(&backup.source_min_semantic.to_le_bytes());
    bytes.extend_from_slice(&backup.source_active_features.to_le_bytes());
    bytes.extend_from_slice(&checkpoint_len.to_le_bytes());
    bytes.extend_from_slice(&checkpoint_crc.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    if bytes.len() != BACKUP_V2_HEADER_BYTES {
        return Err(io::Error::other("backup header layout"));
    }
    let header_crc = crc32c(&bytes);
    bytes[BACKUP_V2_HEADER_BYTES - 4..].copy_from_slice(&header_crc.to_le_bytes());
    bytes.extend_from_slice(&backup.checkpoint);
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(b"CBKE");
    let footer = bytes.len() - BACKUP_V2_FOOTER_BYTES;
    let bundle_crc = crc32c(&bytes);
    bytes[footer..footer + 4].copy_from_slice(&bundle_crc.to_le_bytes());
    Ok(bytes)
}

pub fn decode_backup_v2(bytes: &[u8]) -> io::Result<BackupV2> {
    if bytes.len() < BACKUP_V2_HEADER_BYTES + BACKUP_V2_FOOTER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated backup",
        ));
    }
    if &bytes[..4] != BACKUP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup magic",
        ));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("backup version"));
    if version == LEGACY_BACKUP_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy node backup requires an explicit legacy importer",
        ));
    }
    if version != BACKUP_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported backup version",
        ));
    }
    let header = &bytes[..BACKUP_V2_HEADER_BYTES];
    let expected_header = u32::from_le_bytes(
        header[BACKUP_V2_HEADER_BYTES - 4..]
            .try_into()
            .expect("backup header CRC"),
    );
    let mut header_copy = header.to_vec();
    header_copy[BACKUP_V2_HEADER_BYTES - 4..].fill(0);
    if crc32c(&header_copy) != expected_header {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup header checksum",
        ));
    }
    let footer = bytes.len() - BACKUP_V2_FOOTER_BYTES;
    if &bytes[footer + 4..] != b"CBKE" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup footer magic",
        ));
    }
    let expected_bundle =
        u32::from_le_bytes(bytes[footer..footer + 4].try_into().expect("bundle CRC"));
    let mut bundle_copy = bytes.to_vec();
    bundle_copy[footer..footer + 4].fill(0);
    if crc32c(&bundle_copy) != expected_bundle {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup bundle checksum",
        ));
    }
    let mut cluster = [0_u8; 16];
    cluster.copy_from_slice(&header[6..22]);
    let source_cluster_id = ClusterId::new(cluster);
    let source_index = LogIndex::new(u64::from_le_bytes(
        header[22..30].try_into().expect("index"),
    ));
    let source_term = Term::new(u64::from_le_bytes(header[30..38].try_into().expect("term")));
    let source_last_leader_time =
        Time::from_nanos(u64::from_le_bytes(header[38..46].try_into().expect("time")));
    let source_policy_hash = u64::from_le_bytes(header[46..54].try_into().expect("policy"));
    let source_min_semantic = u16::from_le_bytes(header[54..56].try_into().expect("semantic"));
    let source_active_features = u64::from_le_bytes(header[56..64].try_into().expect("features"));
    let checkpoint_len = usize::try_from(u64::from_le_bytes(
        header[64..72].try_into().expect("checkpoint length"),
    ))
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup checkpoint length"))?;
    if checkpoint_len == 0 || checkpoint_len > BACKUP_MAX_CHECKPOINT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup checkpoint limit",
        ));
    }
    let end = BACKUP_V2_HEADER_BYTES
        .checked_add(checkpoint_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "backup checkpoint length"))?;
    if end != footer {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup trailing bytes",
        ));
    }
    let checkpoint = bytes[BACKUP_V2_HEADER_BYTES..end].to_vec();
    let expected_checkpoint =
        u32::from_le_bytes(header[72..76].try_into().expect("checkpoint CRC"));
    if crc32c(&checkpoint) != expected_checkpoint
        || source_cluster_id.is_zero()
        || source_index.get() == 0
        || source_term.get() == 0
        || source_min_semantic == 0
        || source_min_semantic > cc_raft::SEMANTIC_VERSION_V3
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup checkpoint",
        ));
    }
    let snapshot = decode_ccsn(
        &checkpoint,
        source_cluster_id.bytes(),
        BACKUP_MAX_CHECKPOINT_BYTES as u64,
    )
    .map_err(io::Error::other)?;
    if snapshot.kv.applied_index != source_index
        || snapshot.kv.applied_term != source_term
        || snapshot.kv.last_leader_time != source_last_leader_time
        || snapshot.cluster_policy.hash() != source_policy_hash
        || snapshot.membership.active_features != source_active_features
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup checkpoint metadata",
        ));
    }
    Ok(BackupV2 {
        source_cluster_id,
        source_index,
        source_term,
        source_last_leader_time,
        source_policy_hash,
        source_min_semantic,
        source_active_features,
        checkpoint,
        provenance: BackupProvenance::LogicalCluster,
    })
}
