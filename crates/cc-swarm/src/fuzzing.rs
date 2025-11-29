// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Dependency-free deterministic codec fuzz adapters. The inventory remains
//! the source of ownership/budget metadata; this module provides one bounded
//! total adapter per inventory format.

use std::panic::{AssertUnwindSafe, catch_unwind};

use cc_core::{AdminReply, ClusterPolicy, ConfigEnvelope, Trace, Xoshiro256pp, crc32c};
use cc_env::{PeerHello, decode_effect, decode_input, decode_peer_frame};

pub const MAX_FUZZ_INPUT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FuzzOutcome {
    Ok,
    TypedError,
    Panic,
    BudgetExceeded,
    UnknownFormat,
}

impl FuzzOutcome {
    #[must_use]
    pub const fn signature(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::TypedError => "typed-error",
            Self::Panic => "panic",
            Self::BudgetExceeded => "budget-exceeded",
            Self::UnknownFormat => "unknown-format",
        }
    }
}

#[must_use]
pub fn fuzz_decode(format: &str, bytes: &[u8], allocation_budget: usize) -> FuzzOutcome {
    if bytes.len() > MAX_FUZZ_INPUT_BYTES || bytes.len() > allocation_budget {
        return FuzzOutcome::BudgetExceeded;
    }
    match catch_unwind(AssertUnwindSafe(|| decode_inner(format, bytes))) {
        Ok(Some(true)) => FuzzOutcome::Ok,
        Ok(Some(false)) => FuzzOutcome::TypedError,
        Ok(None) => FuzzOutcome::UnknownFormat,
        Err(_) => FuzzOutcome::Panic,
    }
}

fn decode_inner(format: &str, bytes: &[u8]) -> Option<bool> {
    let accepted = match format {
        "resp" => cc_resp::parse(bytes).is_ok(),
        "ccpl" => ClusterPolicy::decode(bytes).is_ok(),
        "ccid" => decode_ccid_envelope(bytes),
        "cchl" => PeerHello::decode(bytes).is_ok(),
        "ccpf" => decode_peer_frame(bytes).is_ok(),
        "cc-input" => decode_input(bytes).is_ok(),
        "cc-effect" => decode_effect(bytes).is_ok(),
        "ccrp" => cc_raft::codec::decode(bytes).is_ok(),
        "cclr" => cc_log::recover_framed_record_stream(bytes).is_ok(),
        "ccwl" => cc_wal::recover(
            &[cc_wal::SegmentImage {
                sequence: 0,
                bytes: bytes.to_vec(),
            }],
            cc_wal::WalConfig {
                segment_size: bytes.len().max(64),
                max_record_size: cc_wal::MAX_RECORD_SIZE,
            },
        )
        .is_ok(),
        "sst-v1" => cc_store::SstTable::decode(bytes).is_ok(),
        "sst-v2" | "sst-v2-block" | "sst-v2-bloom" => {
            cc_store::SstV2Table::decode(bytes, cc_store::SstV2Limits::default()).is_ok()
        }
        "ccmf" => cc_store::decode_manifest_v2(bytes).is_ok(),
        "ccmt" => cc_store::decode_meta_v2(bytes).is_ok(),
        "ccsn" => {
            let mut cluster_id = [0_u8; 16];
            if let Some(encoded) = bytes.get(6..22) {
                cluster_id.copy_from_slice(encoded);
            }
            cc_cluster::decode_ccsn(bytes, cluster_id, MAX_FUZZ_INPUT_BYTES as u64).is_ok()
        }
        "cctr" => Trace::decode(bytes).is_ok(),
        "ccap" => cc_cluster::AppEnvelope::decode(bytes).is_ok(),
        "cccf" => ConfigEnvelope::decode(bytes).is_ok(),
        "ccar" => AdminReply::decode(bytes).is_ok(),
        "cckv" => cc_kv::decode_command(bytes).is_ok(),
        "cckr" => cc_kv::decode_reply(bytes).is_ok(),
        "ccij" => cc_host::journal::InputJournal::decode(bytes).is_ok(),
        "ccbi" => cc_host::journal::RecordedBootImage::decode(bytes).is_ok(),
        "ccbk-v1" | "ccbk-v2" => decode_backup_envelope(bytes),
        "cchy-v1" => std::str::from_utf8(bytes)
            .ok()
            .is_some_and(|text| cc_checker::decode_history_v1_tsv(text).is_ok()),
        "cchy-v2" => cc_checker::HistoryDocument::decode(bytes).is_ok(),
        "store-wal" => cc_store::recover_store_wal(bytes).is_ok(),
        _ => return None,
    };
    Some(accepted)
}

fn decode_ccid_envelope(bytes: &[u8]) -> bool {
    const LENGTH: usize = 55;
    if bytes.len() != LENGTH || bytes.get(..4) != Some(b"CCID") {
        return false;
    }
    let expected = u32::from_le_bytes(bytes[LENGTH - 4..].try_into().expect("CCID CRC"));
    let mut copy = bytes.to_vec();
    copy[LENGTH - 4..].fill(0);
    crc32c(&copy) == expected
}

fn decode_backup_envelope(bytes: &[u8]) -> bool {
    const MAX_BACKUP_BYTES: usize = 64 * 1024 * 1024;
    if bytes.len() < 6 || bytes.len() > MAX_BACKUP_BYTES || bytes.get(..4) != Some(b"CCBK") {
        return false;
    }
    match u16::from_le_bytes(bytes[4..6].try_into().expect("backup version")) {
        1 => bytes.len() >= 10,
        2 => {
            const CHECKPOINT_LEN_OFFSET: usize = 64;
            let Some(length_bytes) = bytes.get(CHECKPOINT_LEN_OFFSET..CHECKPOINT_LEN_OFFSET + 8)
            else {
                return false;
            };
            let length = usize::try_from(u64::from_le_bytes(
                length_bytes.try_into().expect("checkpoint length"),
            ))
            .unwrap_or(usize::MAX);
            const HEADER_LEN: usize = 80;
            const FOOTER_LEN: usize = 8;
            length <= MAX_BACKUP_BYTES
                && HEADER_LEN
                    .checked_add(length)
                    .and_then(|end| end.checked_add(FOOTER_LEN))
                    == Some(bytes.len())
        }
        _ => false,
    }
}

/// Deterministic mutation palette used by both CLI fuzzing and replay tests.
#[must_use]
pub fn mutate_case(format: &str, input: &[u8], rng: &mut Xoshiro256pp) -> Vec<u8> {
    let mut bytes = input.to_vec();
    match rng.range_u64(0, 6) {
        0 if !bytes.is_empty() => {
            let offset = usize::try_from(rng.range_u64(0, bytes.len() as u64)).unwrap_or(0);
            bytes[offset] ^= 1_u8 << rng.range_u64(0, 8);
        }
        1 => {
            let keep = rng.range_u64(0, bytes.len().saturating_add(1) as u64);
            bytes.truncate(usize::try_from(keep).unwrap_or(0));
        }
        2 if !bytes.is_empty() && bytes.len() < MAX_FUZZ_INPUT_BYTES / 2 => {
            let start = usize::try_from(rng.range_u64(0, bytes.len() as u64)).unwrap_or(0);
            let duplicate = bytes[start..].to_vec();
            bytes.extend_from_slice(&duplicate);
        }
        3 if bytes.len() >= 4 => bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes()),
        4 if bytes.len() < MAX_FUZZ_INPUT_BYTES => {
            let at = usize::try_from(rng.range_u64(0, bytes.len().saturating_add(1) as u64))
                .unwrap_or(bytes.len());
            bytes.insert(at, rng.u64() as u8);
        }
        5 if format == "ccpf" && bytes.len() >= 15 => {
            let body_len = usize::try_from(u32::from_le_bytes(
                bytes[6..10].try_into().expect("CCPF body length"),
            ))
            .unwrap_or(usize::MAX);
            if 14_usize.checked_add(body_len) == Some(bytes.len()) {
                bytes[14] ^= 1;
                let checksum = crc32c(&bytes[14..]);
                bytes[10..14].copy_from_slice(&checksum.to_le_bytes());
            }
        }
        _ => bytes.push(0),
    }
    bytes.truncate(MAX_FUZZ_INPUT_BYTES);
    bytes
}

/// Three-pass failure minimizer: suffix truncation then byte deletion until
/// one-deletion minimal. The predicate owns signature equality.
#[must_use]
pub fn minimize_case(mut bytes: Vec<u8>, mut same_failure: impl FnMut(&[u8]) -> bool) -> Vec<u8> {
    let mut length = bytes.len();
    while length > 0 {
        let trial = &bytes[..length - 1];
        if same_failure(trial) {
            bytes.truncate(length - 1);
            length -= 1;
        } else {
            break;
        }
    }
    let mut index = bytes.len();
    while index > 0 {
        index -= 1;
        let mut trial = bytes.clone();
        trial.remove(index);
        if same_failure(&trial) {
            bytes = trial;
        }
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::path::{Path, PathBuf};

    fn root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    fn inventory() -> Vec<Vec<String>> {
        let text = fs::read_to_string(root().join("fuzz/inventory.tsv")).expect("inventory");
        assert_eq!(
            text.lines().next(),
            Some(
                "format\towner\tdecoder\tversion\tmax_input_bytes\tmax_declared_count\tallocation_budget_bytes\twork_budget"
            )
        );
        text.lines()
            .skip(1)
            .map(|line| line.split('\t').map(str::to_owned).collect::<Vec<_>>())
            .collect()
    }

    #[test]
    fn trap_every_decoder_is_in_the_fuzz_inventory() {
        let rows = inventory();
        let actual = rows
            .iter()
            .map(|fields| fields[0].as_str())
            .collect::<BTreeSet<_>>();
        let required = [
            "resp",
            "ccpl",
            "ccid",
            "cchl",
            "ccpf",
            "cc-input",
            "cc-effect",
            "ccrp",
            "cclr",
            "ccwl",
            "sst-v1",
            "sst-v2",
            "sst-v2-block",
            "sst-v2-bloom",
            "ccmf",
            "ccmt",
            "ccsn",
            "cctr",
            "ccap",
            "cccf",
            "ccar",
            "cckv",
            "cckr",
            "ccij",
            "ccbi",
            "ccbk-v1",
            "ccbk-v2",
            "cchy-v1",
            "cchy-v2",
            "store-wal",
        ];
        assert_eq!(actual, required.into_iter().collect());
        for fields in rows {
            assert_eq!(fields.len(), 8, "inventory row {fields:?}");
            let owner = &fields[1];
            let source_root = root().join("crates").join(owner).join("src");
            let needle = fields[2].rsplit("::").next().expect("decoder identifier");
            let found = fs::read_dir(&source_root)
                .expect("owner source")
                .filter_map(Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rs"))
                .any(|entry| {
                    fs::read_to_string(entry.path()).is_ok_and(|source| source.contains(needle))
                });
            assert!(
                found,
                "{} does not expose {}",
                source_root.display(),
                fields[2]
            );
        }
    }

    fn manifest() -> BTreeMap<String, (PathBuf, u64, String, usize)> {
        let text = fs::read_to_string(root().join("fuzz/corpus/manifest.tsv")).expect("manifest");
        assert_eq!(
            text.lines().next(),
            Some("format\tpath\tcontent_hash\texpected\tsignature\tbudget")
        );
        text.lines()
            .skip(1)
            .map(|line| {
                let fields = line.split('\t').collect::<Vec<_>>();
                assert_eq!(fields.len(), 6);
                (
                    fields[0].to_owned(),
                    (
                        root().join(fields[1]),
                        u64::from_str_radix(fields[2], 16).expect("content hash"),
                        fields[4].to_owned(),
                        fields[5].parse().expect("budget"),
                    ),
                )
            })
            .collect()
    }

    #[test]
    fn trap_every_format_has_a_corpus() {
        let manifest = manifest();
        for fields in inventory() {
            let format = &fields[0];
            let directory = root().join("fuzz/corpus").join(format);
            let cases = fs::read_dir(&directory)
                .expect("format corpus")
                .filter_map(Result::ok)
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "bin"))
                .count();
            assert!((1..=64).contains(&cases), "{format} corpus count {cases}");
            assert!(
                manifest.contains_key(format),
                "missing {format} manifest row"
            );
        }
    }

    #[test]
    fn trap_corpus_replay_is_panic_free() {
        for (format, (path, hash, expected, budget)) in manifest() {
            let bytes = fs::read(&path).expect("corpus case");
            assert_eq!(cc_core::fnv1a(&bytes), hash, "{} hash", path.display());
            let outcome = fuzz_decode(&format, &bytes, budget);
            assert_eq!(outcome.signature(), expected, "{} replay", path.display());
            assert!(!matches!(outcome, FuzzOutcome::Panic));
        }
    }

    #[test]
    fn trap_declared_length_cannot_exceed_allocation_budget() {
        for fields in inventory() {
            let format = &fields[0];
            assert_eq!(
                fuzz_decode(format, &[0; 16], 8),
                FuzzOutcome::BudgetExceeded
            );
            let malicious = u32::MAX.to_le_bytes();
            assert!(!matches!(
                fuzz_decode(format, &malicious, MAX_FUZZ_INPUT_BYTES),
                FuzzOutcome::Panic | FuzzOutcome::BudgetExceeded | FuzzOutcome::UnknownFormat
            ));
        }
    }

    #[test]
    fn trap_minimizer_reduces_a_planted_crash() {
        let input = b"prefix-PLANTED-CRASH-suffix".to_vec();
        let minimized = minimize_case(input.clone(), |candidate| {
            candidate
                .windows(b"PLANTED-CRASH".len())
                .any(|window| window == b"PLANTED-CRASH")
        });
        assert_eq!(minimized, b"PLANTED-CRASH");
        assert!(minimized.len() < input.len());
    }
}
