// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Emit current-format C0 fixture bytes through their owning encoders.
//!
//! This is intentionally an example, rather than a production command.  The
//! compatibility script invokes it from a pinned source tree twice and then
//! owns hashes, manifests, and reader receipts.  It has no version override:
//! every byte is emitted by the exact crate build selected by Cargo.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

use cc_checker::{History, HistoryDocument, Operation, OperationKind, Outcome};
use cc_cluster::backup::{BackupProvenance, BackupV2, encode_backup_v2};
use cc_cluster::{AppEnvelope, CcsnSnapshot, NodeConfig, RaftConfig, SessionTable, encode_ccsn};
use cc_core::{
    AdminReply, AdminResultTag, ClusterId, ClusterPolicy, ConfigEnvelope, ConfigOperation, Event,
    EventKind, HostLimits, LogIndex, MembershipState, NodeId, PeerAddress, Seed, Term, Time, Trace,
};
use cc_env::{Effect, Input, PeerHello, WireMsg, encode_effect, encode_input, encode_peer_frame};
use cc_host::journal::{InputJournal, RecordedBootImage};
use cc_kv::{KvCommand, KvReply, LogicalKvEntry, LogicalKvSnapshot, encode_command, encode_reply};
use cc_log::{
    DurableRecord, Genesis, Log as DurableLog, Origin, SnapshotMark, encode_durable_record,
};
use cc_raft::{Entry, EntryKind, HardState, Message, MessageKind, PROTOCOL_VERSION, codec};
use cc_resp::{RespValue, encode as encode_resp};
use cc_store::{
    InternalKey, ManifestEditV2, ManifestV2, SstTable, SstV2Limits, SstV2Table, Store,
    StoreApplyBatch, StoreConfig, StoreEntryKind, StoreMetadataEdit, StoreMutation, StoreWatermark,
    ValueKind, encode_manifest_v2, encode_meta_v2, encode_store_wal_frame,
};

struct Fixture<'a> {
    format: &'a str,
    name: &'a str,
    reader_test: &'a str,
    semantic: &'a str,
    bytes: Vec<u8>,
}

fn main() -> io::Result<()> {
    let out = parse_output()?;
    emit(out)
}

pub fn emit(out: PathBuf) -> io::Result<()> {
    if out.exists() && fs::read_dir(&out)?.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("fixture output is not empty: {}", out.display()),
        ));
    }
    fs::create_dir_all(&out)?;
    for fixture in fixtures()? {
        let binary = out.join(format!("{}.bin", fixture.name));
        let sidecar = out.join(format!("{}.txt", fixture.name));
        fs::write(&binary, fixture.bytes)?;
        fs::write(
            &sidecar,
            format!(
                "reader_test={}\nformat={}\nsemantic={}\n",
                fixture.reader_test, fixture.format, fixture.semantic
            ),
        )?;
        println!(
            "fixture format={} binary={} sidecar={}",
            fixture.format,
            binary.display(),
            sidecar.display()
        );
    }
    Ok(())
}

fn parse_output() -> io::Result<PathBuf> {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next(), args.next()) {
        (Some("--out"), Some(path), None) => Ok(PathBuf::from(path)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cargo run -p cc-swarm --example c0_fixtures -- --out DIR",
        )),
    }
}

fn fixtures() -> io::Result<Vec<Fixture<'static>>> {
    let policy = ClusterPolicy::default();
    let policy_bytes = policy.encode();
    let membership = MembershipState::new(BTreeSet::from([NodeId::new(1)]))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let genesis = Genesis {
        origin: Origin::Bootstrap,
        cluster_id: [0x31; 16],
        policy,
        membership: membership.clone(),
    };
    let hello = PeerHello {
        cluster_id: [0x31; 16],
        node_id: NodeId::new(1),
        cluster_policy: policy_bytes.clone(),
        semantic_min: PROTOCOL_VERSION,
        semantic_max: PROTOCOL_VERSION,
        supported_features: 0,
        required_features: 0,
        max_peer_frame: cc_env::MAX_PEER_FRAME as u32,
    };
    let peer = Message {
        proto_version: PROTOCOL_VERSION,
        from: NodeId::new(1),
        to: NodeId::new(2),
        term: Term::new(1),
        kind: MessageKind::PreVoteReq {
            last_index: LogIndex::new(0),
            last_term: Term::new(0),
        },
    };
    let ccrp = codec::encode(&peer).map_err(io::Error::other)?;
    let ccpf = encode_peer_frame(&WireMsg::new(PROTOCOL_VERSION, ccrp.clone()))
        .map_err(io::Error::other)?;
    let command = KvCommand::Set {
        key: b"c0-key".to_vec(),
        value: b"c0-value".to_vec(),
        ttl: None,
    };
    let reply = KvReply::Value(Some(b"c0-value".to_vec()));
    let config = ConfigEnvelope {
        admin_session: None,
        leader_time: Time::from_nanos(7),
        operation: ConfigOperation::AddLearner {
            id: NodeId::new(2),
            address: Some(PeerAddress::V4 {
                ip: [127, 0, 0, 1],
                port: 7202,
            }),
        },
    };
    let admin = AdminReply {
        operation_tag: 1,
        result: AdminResultTag::Applied,
        source_index: LogIndex::new(1),
        detail: b"learner-added".to_vec(),
    };
    let app = AppEnvelope {
        session: None,
        leader_time: Time::from_nanos(7),
        command: encode_command(&command),
    };
    let mut trace = Trace::new(Seed::new(0xc0), 1);
    trace.push(
        Time::from_nanos(1),
        Some(NodeId::new(1)),
        EventKind::ClientInvoke,
        b"c0".to_vec(),
    );
    let (mut durable_log, _) =
        DurableLog::fresh(Default::default(), genesis.clone()).map_err(io::Error::other)?;
    durable_log
        .commit()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "fixture WAL did not commit"))?;
    let ccwl = durable_log
        .durable_images()
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "fixture WAL has no image"))?
        .bytes;
    let sst = SstTable::from_entries(
        1,
        vec![(
            InternalKey::new(b"c0-key".to_vec(), 1, ValueKind::Put),
            b"c0-value".to_vec(),
        )],
    )
    .map_err(io::Error::other)?;
    let meta = Store::new(StoreConfig::default())
        .map_err(io::Error::other)?
        .image()
        .meta;
    let input = Input::Tick;
    let effect = Effect::Trace(Event::new(
        1,
        Time::from_nanos(1),
        Some(NodeId::new(1)),
        EventKind::ClientOk,
        b"c0".to_vec(),
    ));
    let journal = InputJournal::new(b"c0-fixture-header".to_vec())
        .encode()
        .map_err(io::Error::other)?;
    let cclr = encode_durable_record(&DurableRecord::Genesis(Box::new(genesis)))
        .map_err(io::Error::other)?;
    let cclr_hard = encode_durable_record(&DurableRecord::Hard(HardState {
        term: Term::new(1),
        voted_for: Some(NodeId::new(1)),
    }))
    .map_err(io::Error::other)?;
    let cclr_append = encode_durable_record(&DurableRecord::Append(Entry {
        term: Term::new(1),
        index: LogIndex::new(1),
        kind: EntryKind::App,
        payload: b"c0-entry".to_vec(),
    }))
    .map_err(io::Error::other)?;
    let cclr_truncate = encode_durable_record(&DurableRecord::Truncate {
        from: LogIndex::new(1),
    })
    .map_err(io::Error::other)?;
    let cclr_snapshot = encode_durable_record(&DurableRecord::SnapshotMark(SnapshotMark {
        index: LogIndex::new(1),
        term: Term::new(1),
        generation: 7,
        crc32c: 0xc0c0_c0c0,
    }))
    .map_err(io::Error::other)?;
    let history = HistoryDocument {
        build_label: String::from(env!("CARGO_PKG_VERSION")),
        config_hash: 0,
        initial: BTreeMap::new(),
        retain_open: false,
        history: History {
            operations: vec![Operation::completed(
                1,
                OperationKind::Set {
                    key: b"c0-key".to_vec(),
                    value: b"c0-value".to_vec(),
                },
                Time::from_nanos(1),
                Time::from_nanos(2),
                Outcome::Ok,
            )],
        },
    };
    let node_config = NodeConfig {
        id: NodeId::new(1),
        cluster_id: [0x31; 16],
        seed: Seed::new(0xc0),
        raft: RaftConfig::default(),
        store: StoreConfig::default(),
        policy,
        host_limits: HostLimits::default(),
    };
    let boot_image = RecordedBootImage {
        config: node_config,
        cluster_id: [0x31; 16],
        membership: membership.clone(),
        boot_epoch: Time::from_nanos(11),
        build_label: String::from(env!("CARGO_PKG_VERSION")),
        wal: encode_durable_record(&DurableRecord::Genesis(Box::new(Genesis {
            origin: Origin::Bootstrap,
            cluster_id: [0x31; 16],
            policy,
            membership: membership.clone(),
        })))
        .map_err(io::Error::other)?,
        store_wal: Vec::new(),
        snapshot: None,
    }
    .encode()
    .map_err(io::Error::other)?;
    let snapshot = encode_ccsn(&CcsnSnapshot {
        cluster_id: [0x31; 16],
        cluster_policy: policy,
        membership: membership.clone(),
        kv: LogicalKvSnapshot {
            entries: vec![LogicalKvEntry {
                key: b"c0-key".to_vec(),
                sequence: 1,
                value: b"c0-value".to_vec(),
                deadline: None,
            }],
            store_sequence: 1,
            applied_index: LogIndex::new(1),
            applied_term: Term::new(1),
            last_leader_time: Time::from_nanos(7),
        },
        sessions: SessionTable::default(),
        leadership_transfer: None,
    })
    .map_err(io::Error::other)?;
    let backup = encode_backup_v2(&BackupV2 {
        source_cluster_id: ClusterId::new([0x31; 16]),
        source_index: LogIndex::new(1),
        source_term: Term::new(1),
        source_last_leader_time: Time::from_nanos(7),
        source_policy_hash: policy.hash(),
        source_min_semantic: PROTOCOL_VERSION,
        source_active_features: membership.active_features,
        checkpoint: snapshot.clone(),
        provenance: BackupProvenance::LogicalCluster,
    })?;
    let v2_entries = vec![(
        InternalKey::new(b"c0-key".to_vec(), 1, ValueKind::Put),
        b"c0-value".to_vec(),
    )];
    let sst_v2 =
        SstV2Table::encode(v2_entries, SstV2Limits::default()).map_err(io::Error::other)?;
    let decoded_sst_v2 =
        SstV2Table::decode(&sst_v2, SstV2Limits::default()).map_err(io::Error::other)?;
    let data_block = sst_v2[..usize::try_from(decoded_sst_v2.meta.index_offset)
        .map_err(|_| io::Error::other("SST index offset"))?]
        .to_vec();
    let bloom_start = usize::try_from(decoded_sst_v2.meta.bloom_offset)
        .map_err(|_| io::Error::other("SST bloom offset"))?;
    let bloom_end = bloom_start
        .checked_add(usize::try_from(decoded_sst_v2.meta.bloom_length).unwrap_or(usize::MAX))
        .ok_or_else(|| io::Error::other("SST bloom range"))?;
    let bloom_block = sst_v2[bloom_start..bloom_end].to_vec();
    let mut manifest = ManifestV2::empty(1);
    manifest
        .append_edit_batch(vec![
            ManifestEditV2::NextFileNo(2),
            ManifestEditV2::AppliedWatermark {
                watermark: StoreWatermark {
                    index: LogIndex::new(1),
                    term: Term::new(1),
                    last_leader_time: Time::from_nanos(7),
                },
                store_sequence: 1,
            },
        ])
        .map_err(io::Error::other)?;
    let manifest_bytes = encode_manifest_v2(&manifest).map_err(io::Error::other)?;
    let meta_v2 = encode_meta_v2(manifest.meta().map_err(io::Error::other)?);
    let store_wal = encode_store_wal_frame(&StoreApplyBatch {
        entry_kind: StoreEntryKind::App,
        watermark: StoreWatermark {
            index: LogIndex::new(1),
            term: Term::new(1),
            last_leader_time: Time::from_nanos(7),
        },
        mutations: vec![StoreMutation::Put {
            key: b"c0-key".to_vec(),
            value: b"c0-value".to_vec(),
        }],
        metadata: vec![StoreMetadataEdit::Upsert {
            namespace: 1,
            key: b"fixture".to_vec(),
            value: b"yes".to_vec(),
        }],
        canonical_command: encode_command(&command),
        cached_reply: encode_reply(&reply),
    })
    .map_err(io::Error::other)?;
    let resp = encode_resp(&RespValue::Array(vec![RespValue::Bulk(Some(
        b"PING".to_vec(),
    ))]));
    Ok(vec![
        Fixture {
            format: "CCTR",
            name: "cctr-v1",
            reader_test: "trap_trace_reads_current_binary_and_json_exports",
            semantic: "one deterministic ClientInvoke trace event",
            bytes: trace.encode(),
        },
        Fixture {
            format: "CCWL",
            name: "ccwl-v1",
            reader_test: "trap_log_torn_tail_is_prefix_safe",
            semantic: "one committed data WAL record c0-wal",
            bytes: ccwl,
        },
        Fixture {
            format: "CCST",
            name: "ccst-v1",
            reader_test: "trap_equal_watermark_snapshot_pins_are_reference_counted",
            semantic: "one Put c0-key=c0-value at sequence 1",
            bytes: sst.bytes().to_vec(),
        },
        Fixture {
            format: "CCMT",
            name: "ccmt-v1",
            reader_test: "golden_byte_layout_vectors",
            semantic: "default store metadata record",
            bytes: meta,
        },
        Fixture {
            format: "CCHL",
            name: "cchl-v1",
            reader_test: "golden_cchl_vectors",
            semantic: "node 1 protocol-v2 hello for policy-default cluster",
            bytes: hello.encode().map_err(io::Error::other)?,
        },
        Fixture {
            format: "CCPF",
            name: "ccpf-v1",
            reader_test: "trap_corrupt_frame_never_reaches_raft_decoder",
            semantic: "one checksum-protected CCRP PreVoteReq frame",
            bytes: ccpf,
        },
        Fixture {
            format: "CCKV",
            name: "cckv-v1",
            reader_test: "trap_conditional_set_is_one_replicated_apply",
            semantic: "SET c0-key c0-value without TTL",
            bytes: encode_command(&command),
        },
        Fixture {
            format: "CCKR",
            name: "cckr-v1",
            reader_test: "golden_cckr_v1_round_trips_and_rejects_corruption",
            semantic: "Value(c0-value) reply",
            bytes: encode_reply(&reply),
        },
        Fixture {
            format: "CCPL",
            name: "ccpl-v1",
            reader_test: "golden_cluster_policy_v1",
            semantic: "default deterministic cluster policy",
            bytes: policy_bytes,
        },
        Fixture {
            format: "CCMS",
            name: "ccms-v2",
            reader_test: "trap_membership_recovers_from_log_and_snapshot",
            semantic: "stable one-voter membership containing node 1",
            bytes: membership.encode().map_err(io::Error::other)?,
        },
        Fixture {
            format: "CCAP",
            name: "ccap-v1",
            reader_test: "trap_plain_route_ids_never_enter_raft_payload",
            semantic: "sessionless application SET envelope at leader time 7",
            bytes: app.encode(),
        },
        Fixture {
            format: "CCCF",
            name: "cccf-v1",
            reader_test: "golden_cccf_v1_and_ccar_v1",
            semantic: "AddLearner node 2 at 127.0.0.1:7202",
            bytes: config.encode(),
        },
        Fixture {
            format: "CCAR",
            name: "ccar-v1",
            reader_test: "golden_cccf_v1_and_ccar_v1",
            semantic: "Applied admin reply at source index 1",
            bytes: admin.encode(),
        },
        Fixture {
            format: "CCRP",
            name: "ccrp-v1",
            reader_test: "golden_ccrp_vectors",
            semantic: "node 1 to node 2 PreVoteReq at term 1",
            bytes: ccrp,
        },
        Fixture {
            format: "CCEI",
            name: "ccei-v1",
            reader_test: "trap_journal_record_pairs_input_and_effects",
            semantic: "one host Tick input",
            bytes: encode_input(&input).map_err(io::Error::other)?,
        },
        Fixture {
            format: "CCEO",
            name: "cceo-v1",
            reader_test: "trap_journal_record_pairs_input_and_effects",
            semantic: "one ClientOk trace effect",
            bytes: encode_effect(&effect).map_err(io::Error::other)?,
        },
        Fixture {
            format: "CCIJ",
            name: "ccij-v1",
            reader_test: "trap_input_journal_is_prefix_durable",
            semantic: "header-only interrupted journal prefix",
            bytes: journal,
        },
        Fixture {
            format: "CCLR",
            name: "cclr-v1",
            reader_test: "trap_log_recovery_is_idempotent",
            semantic: "Bootstrap Genesis for one-node default-policy cluster",
            bytes: cclr,
        },
        Fixture {
            format: "CCLR",
            name: "cclr-hard-v1",
            reader_test: "trap_log_recovery_is_idempotent",
            semantic: "HardState term 1 voted for node 1",
            bytes: cclr_hard,
        },
        Fixture {
            format: "CCLR",
            name: "cclr-append-v1",
            reader_test: "trap_log_recovery_is_idempotent",
            semantic: "normal entry c0-entry at index 1 term 1",
            bytes: cclr_append,
        },
        Fixture {
            format: "CCLR",
            name: "cclr-truncate-v1",
            reader_test: "trap_log_recovery_is_idempotent",
            semantic: "truncate suffix beginning at index 1",
            bytes: cclr_truncate,
        },
        Fixture {
            format: "CCLR",
            name: "cclr-snapshot-mark-v1",
            reader_test: "trap_log_recovery_is_idempotent",
            semantic: "local snapshot mark at index 1 term 1 generation 7",
            bytes: cclr_snapshot,
        },
        Fixture {
            format: "CCHY",
            name: "cchy-v2",
            reader_test: "trap_history_v2_round_trips_binary_keys_and_values",
            semantic: "one completed SET c0-key=c0-value operation",
            bytes: history.encode(),
        },
        Fixture {
            format: "CCBI",
            name: "ccbi-v4",
            reader_test: "trap_replay_starts_from_captured_boot_image",
            semantic: "one-node complete Driver boot image with store durability fields",
            bytes: boot_image,
        },
        Fixture {
            format: "CCSN",
            name: "ccsn-v1",
            reader_test: "trap_snapshot_ordering",
            semantic: "one-key logical checkpoint at index 1 term 1",
            bytes: snapshot,
        },
        Fixture {
            format: "CCBK",
            name: "ccbk-v2",
            reader_test: "golden_ccbk_v2",
            semantic: "logical one-key checkpoint at index 1 term 1",
            bytes: backup,
        },
        Fixture {
            format: "CCST",
            name: "ccst-v2",
            reader_test: "golden_sst_v2_vectors",
            semantic: "one block-structured v2 table containing c0-key",
            bytes: sst_v2,
        },
        Fixture {
            format: "CCST-DATA",
            name: "ccst-v2-data-block",
            reader_test: "trap_block_crc_detects_bit_flip",
            semantic: "first checksummed SST v2 data block",
            bytes: data_block,
        },
        Fixture {
            format: "CCST-BLOOM",
            name: "ccst-v2-bloom-block",
            reader_test: "trap_bloom_negative_avoids_data_read",
            semantic: "SST v2 bloom block for c0-key",
            bytes: bloom_block,
        },
        Fixture {
            format: "CCMF",
            name: "ccmf-v1",
            reader_test: "manifest_v2_round_trips_atomic_edits_and_meta_pointer",
            semantic: "manifest generation 1 with watermark index 1",
            bytes: manifest_bytes,
        },
        Fixture {
            format: "CCMT",
            name: "ccmt-v2",
            reader_test: "manifest_v2_round_trips_atomic_edits_and_meta_pointer",
            semantic: "atomic pointer to manifest generation 1",
            bytes: meta_v2,
        },
        Fixture {
            format: "CCSW",
            name: "store-wal-v1",
            reader_test: "trap_store_wal_replays_only_after_manifest_watermark",
            semantic: "one atomic store apply for c0-key",
            bytes: store_wal,
        },
        Fixture {
            format: "RESP",
            name: "resp2",
            reader_test: "trap_inline_commands_are_rejected",
            semantic: "RESP2 array containing PING",
            bytes: resp,
        },
    ])
}
