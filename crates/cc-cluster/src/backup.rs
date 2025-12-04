// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Canonical CCBK logical-backup envelope shared by production and fixtures.

use std::io;

use cc_core::{ClusterId, LogIndex, MembershipState, NodeId, Term, Time, crc32c};

use crate::{CcsnSnapshot, decode_ccsn};

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

/// Convert a validated logical backup into the state image for one new
/// single-node cluster. Source Raft/node identity is never copied; logical
/// data, TTL deadlines, the semantic floor, and committed feature state are
/// retained under the new cluster identity.
pub fn snapshot_for_fresh_cluster(
    backup: &BackupV2,
    new_cluster_id: ClusterId,
    new_node_id: NodeId,
    restore_time: Time,
) -> io::Result<CcsnSnapshot> {
    if new_cluster_id.is_zero()
        || new_cluster_id == backup.source_cluster_id
        || new_node_id.get() == 0
        || restore_time < backup.source_last_leader_time
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fresh-cluster restore identity or time",
        ));
    }
    let mut snapshot = decode_ccsn(
        &backup.checkpoint,
        backup.source_cluster_id.bytes(),
        BACKUP_MAX_CHECKPOINT_BYTES as u64,
    )
    .map_err(io::Error::other)?;
    if snapshot.kv.applied_index != backup.source_index
        || snapshot.kv.applied_term != backup.source_term
        || snapshot.kv.last_leader_time != backup.source_last_leader_time
        || snapshot.cluster_policy.hash() != backup.source_policy_hash
        || snapshot.membership.active_features != backup.source_active_features
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup checkpoint metadata",
        ));
    }
    snapshot.kv.entries.retain(|entry| {
        entry
            .deadline
            .is_none_or(|deadline| deadline > restore_time)
    });
    snapshot.sessions = snapshot
        .sessions
        .for_fresh_cluster_restore(snapshot.cluster_policy, restore_time)
        .map_err(io::Error::other)?;
    let mut membership =
        MembershipState::new([new_node_id].into_iter().collect()).map_err(io::Error::other)?;
    membership.active_features = backup.source_active_features;
    membership.validate().map_err(io::Error::other)?;
    snapshot.cluster_id = new_cluster_id.bytes();
    snapshot.membership = membership;
    snapshot.leadership_transfer = None;
    snapshot.kv.applied_index = LogIndex::new(1);
    snapshot.kv.applied_term = Term::new(1);
    snapshot.kv.last_leader_time = restore_time;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use cc_core::{ATOMIC_BATCH_FEATURE, ClusterPolicy, MembershipState, Seed};
    use cc_kv::{KvCommand, KvReply, LogicalKvEntry, LogicalKvSnapshot};
    use cc_store::StoreConfig;

    use super::*;
    use crate::{Node, NodeConfig, SessionTable, encode_ccsn};

    fn logical_backup() -> BackupV2 {
        let source_cluster_id = ClusterId::new([1; 16]);
        let mut membership = MembershipState::new(
            [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                .into_iter()
                .collect(),
        )
        .expect("membership");
        membership.active_features = ATOMIC_BATCH_FEATURE;
        let snapshot = CcsnSnapshot {
            cluster_id: source_cluster_id.bytes(),
            cluster_policy: ClusterPolicy::default(),
            membership,
            kv: LogicalKvSnapshot {
                entries: vec![
                    LogicalKvEntry {
                        key: b"expired".to_vec(),
                        sequence: 1,
                        value: b"old".to_vec(),
                        deadline: Some(Time::from_nanos(4_000_000_000)),
                    },
                    LogicalKvEntry {
                        key: b"kept".to_vec(),
                        sequence: 2,
                        value: b"value".to_vec(),
                        deadline: Some(Time::from_nanos(20_000_000_000)),
                    },
                ],
                store_sequence: 2,
                applied_index: LogIndex::new(9),
                applied_term: Term::new(3),
                last_leader_time: Time::from_nanos(3_000_000_000),
            },
            sessions: SessionTable::default(),
            leadership_transfer: None,
        };
        BackupV2 {
            source_cluster_id,
            source_index: LogIndex::new(9),
            source_term: Term::new(3),
            source_last_leader_time: Time::from_nanos(3_000_000_000),
            source_policy_hash: ClusterPolicy::default().hash(),
            source_min_semantic: cc_raft::SEMANTIC_VERSION_V3,
            source_active_features: ATOMIC_BATCH_FEATURE,
            checkpoint: encode_ccsn(&snapshot).expect("checkpoint"),
            provenance: BackupProvenance::LogicalCluster,
        }
    }

    #[test]
    fn trap_restored_cluster_grows_without_identity_clone() {
        let backup =
            decode_backup_v2(&encode_backup_v2(&logical_backup()).expect("backup envelope"))
                .expect("decoded backup");
        let fresh_id = ClusterId::new([7; 16]);
        let mut restored = snapshot_for_fresh_cluster(
            &backup,
            fresh_id,
            NodeId::new(7),
            Time::from_nanos(5_000_000_000),
        )
        .expect("fresh snapshot");
        assert_ne!(restored.cluster_id, backup.source_cluster_id.bytes());
        assert_eq!(
            restored.membership.voters,
            [NodeId::new(7)].into_iter().collect()
        );
        assert!(!restored.membership.voters.contains(&NodeId::new(1)));

        for id in 8..=11 {
            restored.membership.learners.insert(NodeId::new(id));
        }
        restored
            .membership
            .validate()
            .expect("five-member learner stage");
        restored.membership.voters = (7..=11).map(NodeId::new).collect::<BTreeSet<_>>();
        restored.membership.learners.clear();
        restored
            .membership
            .validate()
            .expect("five-voter final state");
    }

    #[test]
    fn trap_backup_restore_preserves_active_feature_floor() {
        let backup = logical_backup();
        let restored = snapshot_for_fresh_cluster(
            &backup,
            ClusterId::new([8; 16]),
            NodeId::new(8),
            Time::from_nanos(5_000_000_000),
        )
        .expect("fresh snapshot");
        assert_eq!(backup.source_min_semantic, cc_raft::SEMANTIC_VERSION_V3);
        assert_eq!(restored.membership.active_features, ATOMIC_BATCH_FEATURE);
    }

    #[test]
    fn trap_restored_cluster_matches_source_final_probes() {
        let backup = logical_backup();
        let cluster_id = ClusterId::new([9; 16]);
        let snapshot = snapshot_for_fresh_cluster(
            &backup,
            cluster_id,
            NodeId::new(9),
            Time::from_nanos(5_000_000_000),
        )
        .expect("fresh snapshot");
        let mut node = Node::new(
            NodeConfig {
                id: NodeId::new(9),
                cluster_id: cluster_id.bytes(),
                seed: Seed::new(9),
                raft: cc_raft::RaftConfig::default(),
                store: StoreConfig::default(),
                policy: ClusterPolicy::default(),
                host_limits: cc_core::HostLimits::default(),
            },
            [NodeId::new(9)].into_iter().collect(),
        )
        .expect("node");
        node.install_decoded_ccsn_snapshot(snapshot)
            .expect("install restored state");
        assert_eq!(
            node.kv.read(
                KvCommand::Get {
                    key: b"kept".to_vec()
                },
                Time::from_nanos(5_000_000_000),
            ),
            Ok(KvReply::Value(Some(b"value".to_vec())))
        );
        assert_eq!(
            node.kv.read(
                KvCommand::Ttl {
                    key: b"kept".to_vec()
                },
                Time::from_nanos(5_000_000_000),
            ),
            Ok(KvReply::Integer(15))
        );
        assert_eq!(node.kv.store.get(b"expired", None), None);
        assert_eq!(node.active_features(), ATOMIC_BATCH_FEATURE);
    }
}
