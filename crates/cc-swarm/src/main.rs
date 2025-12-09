// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use cc_checker::{
    CheckerConfig, History, HistoryDocument, OperationKind, Outcome, Verdict, check_document,
    classify_anomalies, decode_history_v1_tsv, export_porcupine_json,
};
#[cfg(test)]
use cc_cluster::{NodeConfig, RaftConfig};
#[cfg(test)]
use cc_core::HostLimits;
use cc_core::{
    ClusterPolicy, Duration, Event, EventKind, NodeId, Seed, Time, Trace, Xoshiro256pp, fnv1a,
};
use cc_env::{FEATURE_ATOMIC_BATCH, FEATURE_FOLLOWER_READ, FileId, HelloError, PeerHello};
#[cfg(test)]
use cc_host::journal::JournalRecord;
use cc_host::journal::{InputJournal, RecordedBootImage, replay_journal};
use cc_sim::{
    CcrpMutation, DiskModel, FaultAction, FaultAt, FaultPlan, FaultProfile, LinkConfig,
    RecorderLevel, RunSpec, SlowDisk, WorkloadSpec, materialize_fault_plan,
};
#[cfg(test)]
use cc_store::StoreConfig;

use cc_swarm::{
    ClusterRun, DETERMINISM_PROFILES, LedgerError, LedgerKey, LedgerKind, LedgerRow, LedgerVerdict,
    REACHABILITY_BEACONS, REACHABILITY_BEACONS_HELP, SeedLedger, Shard, canonical_run_spec_json,
    deterministic_cluster_trace, deterministic_cluster_trace_for, encode_ledger_row, fuzz_decode,
    minimize_case, mutate_case, mutate_fault_plan, reachability_beacons, reproduces_failure,
    run_spec, semantic_trace_diff, sequence_diagram_svg, shrink_cluster_plan, trace_coverage,
    validate_sharded_coverage,
};

#[path = "../examples/c0_fixtures.rs"]
#[allow(dead_code)]
mod golden_fixtures;

struct CampaignWorkerSummary {
    failures: u64,
    history_failures: u64,
    events: u64,
    runs: u64,
    beacon_hits: [u64; REACHABILITY_BEACONS.len()],
    rows: Vec<LedgerRow>,
}

fn main() -> io::Result<()> {
    if let Some(kata) = cc_swarm::ACTIVE_KATA {
        eprintln!(
            "SYNTHETIC KATA ENABLED: {kata}; artifacts cannot support claims or museum exhibits"
        );
    }
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--determinism") => emit_determinism_trace(),
        Some("--determinism-seeds") => run_determinism_seeds(parse_u64(&args, 1, 1_000)?),
        Some("--selfcheck") => {
            selfcheck_cluster(Seed::new(0xcc)).map_err(io::Error::other)?;
            println!("selfcheck: PASS");
            Ok(())
        }
        Some("one") => run_one(&args[1..]),
        Some("run") => run_campaign(&args[1..]),
        Some("ledger") => run_ledger(&args[1..]),
        Some("regress") => run_regressions(),
        Some("shrink") => run_shrink(&args[1..]),
        Some("check-history") => check_history(&args[1..]),
        Some("replay") => replay_input_journal(&args[1..]),
        Some("export-porcupine") => export_porcupine(&args[1..]),
        Some("diff") => run_diff(&args[1..]),
        Some("sequence") => run_sequence(&args[1..]),
        Some("explain") => run_explain(&args[1..]),
        Some("trace") => run_trace(&args[1..]),
        Some("proxy") => run_proxy(&args[1..]),
        Some("search") => run_coverage_search(&args[1..]),
        Some("model-check") => run_model_check(&args[1..]),
        Some("fuzz") => run_fuzz(&args[1..]),
        Some("golden") => run_golden(&args[1..]),
        Some("capability-campaign") => run_capability_campaign(&args[1..]),
        _ => {
            print_help();
            Ok(())
        }
    }
}

fn run_capability_campaign(args: &[String]) -> io::Result<()> {
    let seeds = parse_u64_flag(args, "--seeds")?.unwrap_or(100_000);
    if seeds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--seeds must be nonzero",
        ));
    }
    let policy = ClusterPolicy::default();
    let policy_bytes = policy.encode();
    for seed in 0..seeds {
        let mut rng = Xoshiro256pp::stream(Seed::new(seed), "capability-campaign", 0);
        let left_id = 1 + rng.u64() % 1_000_000;
        let mut right_id = 1 + rng.u64() % 1_000_000;
        if right_id == left_id {
            right_id = right_id.saturating_add(1);
        }
        let mut left = PeerHello {
            cluster_id: [0x71; 16],
            node_id: NodeId::new(left_id),
            cluster_policy: policy_bytes.clone(),
            semantic_min: 2,
            semantic_max: 3,
            supported_features: FEATURE_FOLLOWER_READ | FEATURE_ATOMIC_BATCH,
            required_features: 0,
            max_peer_frame: 64 * 1024 + u32::try_from(rng.u64() % 65_536).unwrap_or(0),
        };
        let mut right = PeerHello {
            cluster_id: left.cluster_id,
            node_id: NodeId::new(right_id),
            cluster_policy: policy_bytes.clone(),
            semantic_min: 2,
            semantic_max: 3,
            supported_features: FEATURE_FOLLOWER_READ | FEATURE_ATOMIC_BATCH,
            required_features: 0,
            max_peer_frame: 64 * 1024 + u32::try_from(rng.u64() % 65_536).unwrap_or(0),
        };
        let expected = match seed % 8 {
            0 | 1 => {
                right.semantic_max = 2;
                right.supported_features = FEATURE_FOLLOWER_READ;
                Ok((
                    2,
                    FEATURE_FOLLOWER_READ,
                    left.max_peer_frame.min(right.max_peer_frame),
                ))
            }
            2 => Ok((
                3,
                FEATURE_FOLLOWER_READ | FEATURE_ATOMIC_BATCH,
                left.max_peer_frame.min(right.max_peer_frame),
            )),
            3 => {
                left.semantic_min = 3;
                right.semantic_max = 2;
                Err(HelloError::VersionOverlap)
            }
            4 => {
                left.required_features = FEATURE_ATOMIC_BATCH;
                right.supported_features = FEATURE_FOLLOWER_READ;
                Err(HelloError::RequiredFeature(FEATURE_ATOMIC_BATCH))
            }
            5 => {
                right.cluster_id[0] ^= 1;
                Err(HelloError::ClusterMismatch)
            }
            6 => {
                let mut drift = policy;
                drift.max_members = drift.max_members.saturating_sub(1);
                right.cluster_policy = drift.encode();
                Err(HelloError::PolicyMismatch)
            }
            _ => {
                let encoded = right.encode().map_err(io::Error::other)?;
                right = PeerHello::decode(&encoded).map_err(io::Error::other)?.0;
                Ok((
                    3,
                    FEATURE_FOLLOWER_READ | FEATURE_ATOMIC_BATCH,
                    left.max_peer_frame.min(right.max_peer_frame),
                ))
            }
        };
        let actual = left.negotiate(&right);
        match (expected, actual) {
            (Ok((version, features, frame)), Ok(negotiated))
                if negotiated.semantic_version == version
                    && negotiated.features == features
                    && negotiated.max_peer_frame == frame => {}
            (Err(expected), Err(actual)) if actual == expected => {}
            (expected, actual) => {
                return Err(io::Error::other(format!(
                    "negotiated-capabilities seed={seed} expected={expected:?} actual={actual:?}"
                )));
            }
        }
    }
    println!("capability-campaign: PASS profile=negotiated-capabilities seeds={seeds} failures=0");
    Ok(())
}

fn run_golden(args: &[String]) -> io::Result<()> {
    match args {
        [flag, output] if flag == "--out" => golden_fixtures::emit(output.into()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cc-swarm golden --out DIR (current versions only)",
        )),
    }
}

fn run_ledger(args: &[String]) -> io::Result<()> {
    match args.first().map(String::as_str) {
        Some("stats") if args.len() >= 2 => {
            let ledger = read_ledger(Path::new(&args[1]))?;
            let mut verdicts = BTreeMap::<&str, u64>::new();
            let mut profiles = BTreeMap::<&str, u64>::new();
            let mut builds = BTreeSet::new();
            for row in ledger.rows() {
                *verdicts.entry(row.verdict.as_str()).or_default() += 1;
                *profiles.entry(row.key.profile.as_str()).or_default() += 1;
                builds.insert(row.key.build_label.as_str());
            }
            let verdict_text = verdicts
                .iter()
                .map(|(verdict, count)| format!("{verdict}={count}"))
                .collect::<Vec<_>>()
                .join(",");
            let profile_text = profiles
                .iter()
                .map(|(profile, count)| format!("{profile}={count}"))
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "ledger rows={} builds={} verdicts={} profiles={}",
                ledger.rows().count(),
                builds.len(),
                verdict_text,
                profile_text
            );
            if let Some(summary_path) = parse_string_flag(args, "--summary") {
                let allow_dev = has_flag(args, "--allow-dev");
                let generator_build = parse_string_flag(args, "--generator-build")
                    .unwrap_or_else(|| String::from("dev"));
                let summary = coverage_summary(
                    Path::new(&args[1]),
                    &ledger,
                    &generator_build,
                    allow_dev,
                    unix_time_ms()?,
                )?;
                write_replace_atomic(Path::new(&summary_path), summary.as_bytes())?;
                println!("ledger summary={summary_path}");
                if let Some(badge_path) = parse_string_flag(args, "--badge") {
                    let rows = ledger
                        .rows()
                        .filter(|row| allow_dev || row.key.build_label != "dev")
                        .count();
                    let badge = format!(
                        "{{\"schemaVersion\":1,\"label\":\"campaign seeds\",\"message\":\"{}\",\"summary_hash\":\"{:016x}\",\"generator_build\":\"{}\"}}\n",
                        rows,
                        fnv1a(summary.as_bytes()),
                        json_escape(&generator_build),
                    );
                    write_replace_atomic(Path::new(&badge_path), badge.as_bytes())?;
                    println!("ledger badge={badge_path}");
                }
            } else if parse_string_flag(args, "--badge").is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--badge requires --summary so provenance is the only source",
                ));
            }
            Ok(())
        }
        Some("merge") => {
            let output = parse_string_flag(args, "--out").ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "usage: cc-swarm ledger merge --out <path> <ledger>...",
                )
            })?;
            let mut sources = Vec::new();
            let value_flags = [
                "--out",
                "--expect-range",
                "--expect-shards",
                "--expect-build",
                "--expect-config",
            ];
            let mut skip_value = false;
            for argument in args.iter().skip(1) {
                if skip_value {
                    skip_value = false;
                    continue;
                }
                if value_flags.contains(&argument.as_str()) {
                    skip_value = true;
                    continue;
                }
                if argument == "--allow-dev" {
                    continue;
                }
                sources.push(argument);
            }
            if sources.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ledger merge requires at least one input",
                ));
            }
            if sources.iter().any(|source| source.as_str() == output) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ledger output may not alias an input",
                ));
            }
            let ledgers = sources
                .iter()
                .map(|source| read_ledger(Path::new(source)))
                .collect::<io::Result<Vec<_>>>()?;
            if !has_flag(args, "--allow-dev")
                && ledgers
                    .iter()
                    .flat_map(SeedLedger::rows)
                    .any(|row| row.key.build_label == "dev")
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ledger merge refuses dev rows without --allow-dev",
                ));
            }
            let expected = (
                parse_string_flag(args, "--expect-range"),
                parse_u64_flag(args, "--expect-shards")?,
                parse_string_flag(args, "--expect-build"),
                parse_string_flag(args, "--expect-config"),
            );
            if [
                expected.0.is_some(),
                expected.1.is_some(),
                expected.2.is_some(),
                expected.3.is_some(),
            ]
            .into_iter()
            .any(|present| present)
            {
                let (Some(range), Some(shards), Some(build), Some(config)) = expected else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "coverage merge requires all --expect-* flags",
                    ));
                };
                let (start, end) = parse_seed_range(&range)?;
                let config = parse_fixed_hex(&config, "--expect-config")?;
                validate_sharded_coverage(&ledgers, start, end, shards, &build, config)
                    .map_err(io::Error::other)?;
            }
            let merged = match SeedLedger::merge(ledgers.iter()) {
                Ok(merged) => merged,
                Err(error @ LedgerError::Conflict { .. }) => {
                    let conflict_path = format!("{output}.conflicts.tsv");
                    write_ledger_conflict_artifact(Path::new(&conflict_path), &ledgers, &error)?;
                    return Err(io::Error::other(format!(
                        "{error}; preserved conflicting rows in {conflict_path}"
                    )));
                }
                Err(error) => return Err(io::Error::other(error)),
            };
            write_ledger_atomic(Path::new(&output), &merged.encode())?;
            println!(
                "ledger merge: rows={} output={output}",
                merged.rows().count()
            );
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cc-swarm ledger stats <ledger.tsv> | ledger merge --out <path> <ledger.tsv>...",
        )),
    }
}

fn read_ledger(path: &Path) -> io::Result<SeedLedger> {
    SeedLedger::parse(&fs::read_to_string(path)?).map_err(io::Error::other)
}

fn write_ledger_atomic(path: &Path, contents: &str) -> io::Result<()> {
    if path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "refusing to overwrite campaign ledger output",
        ));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid ledger output filename",
            )
        })?;
    let temporary = parent.join(format!(".{name}.tmp"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn unix_time_ms() -> io::Result<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(io::Error::other)
}

fn filename_component(value: &str) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .take(80)
        .collect::<String>();
    if value.is_empty() {
        String::from("run")
    } else {
        value
    }
}

fn write_create_new_synced(path: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    File::open(parent)?.sync_all()
}

fn write_replace_atomic(path: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid output filename"))?;
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

fn coverage_summary(
    source: &Path,
    ledger: &SeedLedger,
    generator_build: &str,
    allow_dev: bool,
    generated_at: u128,
) -> io::Result<String> {
    if ledger.kind() != LedgerKind::Production {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "coverage summaries reject synthetic kata ledgers",
        ));
    }
    if generator_build.is_empty() || generator_build.contains(['\t', '\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "summary generator build must be one TSV field",
        ));
    }
    let source_text = source.display().to_string();
    if source_text.contains(['\t', '\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "summary source path must be one TSV field",
        ));
    }
    let rows = ledger
        .rows()
        .filter(|row| allow_dev || row.key.build_label != "dev")
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "summary has no non-dev rows; pass --allow-dev only for debugging",
        ));
    }
    let config_hashes = rows
        .iter()
        .map(|row| format!("{:016x}", row.key.config_hash))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",");
    let mut verdicts = BTreeMap::<&str, u64>::new();
    let mut groups = BTreeMap::<String, Vec<u64>>::new();
    for row in &rows {
        *verdicts.entry(row.verdict.as_str()).or_default() += 1;
        groups
            .entry(format!(
                "{}:{:016x}:{}",
                row.key.build_label,
                row.key.config_hash,
                row.key.profile.as_str()
            ))
            .or_default()
            .push(row.key.seed);
    }
    let mut ranges = Vec::new();
    for (group, mut seeds) in groups {
        seeds.sort_unstable();
        seeds.dedup();
        let mut cursor = 0;
        while cursor < seeds.len() {
            let start = seeds[cursor];
            let mut end = start.saturating_add(1);
            cursor += 1;
            while cursor < seeds.len() && seeds[cursor] == end {
                end = end.saturating_add(1);
                cursor += 1;
            }
            ranges.push(format!("{group}:0x{start:016x}..0x{end:016x}"));
        }
    }
    let verdict_counts = verdicts
        .iter()
        .map(|(verdict, count)| format!("{verdict}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    let source_bytes = fs::read(source)?;
    Ok(format!(
        "schema\tgenerator_build\tconfig_hashes\tsource_ledger_hashes\tcovered_seed_ranges\tverdict_counts\tgenerated_at\ncc-summary-v1\t{generator_build}\t{config_hashes}\t{}:{:016x}\t{}\t{verdict_counts}\t{generated_at}\n",
        source_text,
        fnv1a(&source_bytes),
        ranges.join(","),
    ))
}

fn parse_seed_range(value: &str) -> io::Result<(u64, u64)> {
    let (start, end) = value.split_once("..").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "--expect-range must be start..end",
        )
    })?;
    let parse = |value: &str| {
        if let Some(value) = value.strip_prefix("0x") {
            u64::from_str_radix(value, 16)
        } else {
            value.parse::<u64>()
        }
    };
    let start = parse(start)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid range start"))?;
    let end =
        parse(end).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid range end"))?;
    Ok((start, end))
}

fn parse_fixed_hex(value: &str, field: &str) -> io::Result<u64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 16 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{field} must be 16 hexadecimal digits"),
        ));
    }
    u64::from_str_radix(value, 16)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {field}")))
}

fn write_ledger_conflict_artifact(
    path: &Path,
    ledgers: &[SeedLedger],
    error: &LedgerError,
) -> io::Result<()> {
    let LedgerError::Conflict { key } = error else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "conflict artifact requires a conflict key",
        ));
    };
    let mut contents = String::from("# cc-ledger-conflict-v1\n");
    contents.push_str(cc_swarm::LEDGER_COLUMNS);
    contents.push('\n');
    for row in ledgers
        .iter()
        .flat_map(SeedLedger::rows)
        .filter(|row| row.key == *key)
    {
        contents.push_str(&encode_ledger_row(row));
        contents.push('\n');
    }
    write_ledger_atomic(path, &contents)
}

fn append_ledger_rows(
    path: &Path,
    rows: &[LedgerRow],
    expected_kind: LedgerKind,
) -> io::Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    file.lock()?;
    file.rewind()?;
    let mut existing_bytes = Vec::new();
    file.read_to_end(&mut existing_bytes)?;
    let canonical_len = if existing_bytes.is_empty() || existing_bytes.ends_with(b"\n") {
        existing_bytes.len()
    } else {
        existing_bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |offset| offset + 1)
    };
    if canonical_len != existing_bytes.len() {
        file.set_len(u64::try_from(canonical_len).unwrap_or(u64::MAX))?;
        existing_bytes.truncate(canonical_len);
    }
    let mut existing = if existing_bytes.is_empty() {
        match expected_kind {
            LedgerKind::Production => SeedLedger::default(),
            LedgerKind::Kata => SeedLedger::kata(),
        }
    } else {
        SeedLedger::parse(
            std::str::from_utf8(&existing_bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
        )
        .map_err(io::Error::other)?
    };
    if existing.kind() != expected_kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "campaign ledger type does not match this build",
        ));
    }
    if existing_bytes.is_empty() {
        file.write_all(expected_kind.header().as_bytes())?;
        file.write_all(b"\n")?;
        file.write_all(cc_swarm::LEDGER_COLUMNS.as_bytes())?;
        file.write_all(b"\n")?;
    }
    let mut canonical = match expected_kind {
        LedgerKind::Production => SeedLedger::default(),
        LedgerKind::Kata => SeedLedger::kata(),
    };
    for row in rows {
        if existing.insert(row.clone()).map_err(io::Error::other)? {
            canonical.insert(row.clone()).map_err(io::Error::other)?;
        }
    }
    for (index, row) in canonical.rows().enumerate() {
        file.write_all(encode_ledger_row(row).as_bytes())?;
        file.write_all(b"\n")?;
        if (index + 1).is_multiple_of(1_000) {
            file.sync_all()?;
        }
    }
    file.sync_all()?;
    Ok(canonical.rows().count())
}

fn campaign_config_hash(profile: FaultProfile, disk_profile: Option<&(String, DiskModel)>) -> u64 {
    let mut spec = fixture_spec(Seed::new(0), profile, true);
    apply_loaded_disk_profile(&mut spec, disk_profile);
    fnv1a(canonical_run_spec_json(&spec).as_bytes())
}

fn run_model_check(args: &[String]) -> io::Result<()> {
    let config = cc_raft::model::ModelConfig {
        max_log: parse_usize_flag(args, "--max-log", 4)?,
        max_term: parse_u64_flag(args, "--max-term")?.unwrap_or(3),
        max_messages: parse_usize_flag(args, "--max-messages", 8)?,
        max_depth: parse_usize_flag(args, "--max-depth", 8)?,
        max_states: parse_usize_flag(args, "--max-states", 2_000_000)?,
    };
    let report = cc_raft::model::check(config).map_err(io::Error::other)?;
    println!(
        "model-check: PASS nodes=3 max_log={} max_term={} depth={} explored_states={} explored_transitions={} max_frontier={}",
        config.max_log,
        config.max_term,
        report.max_depth,
        report.explored_states,
        report.explored_transitions,
        report.max_frontier,
    );
    Ok(())
}

fn run_fuzz(args: &[String]) -> io::Result<()> {
    let format = parse_string_flag(args, "--format").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "fuzz requires --format <inventory-name>",
        )
    })?;
    let iterations = parse_u64_flag(args, "--iterations")?.unwrap_or(1_000);
    let (max_input, allocation_budget, work_budget) = fuzz_inventory_budget(&format)?;
    if iterations > work_budget {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("iterations {iterations} exceed inventory work budget {work_budget}"),
        ));
    }
    let corpus =
        parse_string_flag(args, "--corpus").unwrap_or_else(|| format!("fuzz/corpus/{format}"));
    let corpus_files = collect_corpus_files(Path::new(&corpus))?;
    if corpus_files.is_empty() || corpus_files.len() > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corpus must contain 1..=64 .bin files",
        ));
    }
    let mut cases = Vec::with_capacity(corpus_files.len());
    for path in &corpus_files {
        let bytes = fs::read(path)?;
        if bytes.len() > max_input || bytes.len() > allocation_budget {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corpus case {} exceeds inventory budget", path.display()),
            ));
        }
        let outcome = fuzz_decode(&format, &bytes, allocation_budget);
        if matches!(
            outcome,
            cc_swarm::FuzzOutcome::Panic
                | cc_swarm::FuzzOutcome::BudgetExceeded
                | cc_swarm::FuzzOutcome::UnknownFormat
        ) {
            return Err(io::Error::other(format!(
                "corpus replay {} returned {}",
                path.display(),
                outcome.signature()
            )));
        }
        cases.push(bytes);
    }
    let seed = parse_seed(args, 0)?;
    let mut rng = cc_core::Xoshiro256pp::stream(seed, "codec-fuzz", fnv1a(format.as_bytes()));
    let mut ok = 0_u64;
    let mut typed_errors = 0_u64;
    for iteration in 0..iterations {
        let case = usize::try_from(rng.range_u64(0, cases.len() as u64)).unwrap_or(0);
        let mutated = mutate_case(&format, &cases[case], &mut rng);
        match fuzz_decode(&format, &mutated, allocation_budget) {
            cc_swarm::FuzzOutcome::Ok => ok = ok.saturating_add(1),
            cc_swarm::FuzzOutcome::TypedError => typed_errors = typed_errors.saturating_add(1),
            failure => {
                let signature = failure.signature();
                let minimized = minimize_case(mutated, |candidate| {
                    fuzz_decode(&format, candidate, allocation_budget).signature() == signature
                });
                let hash = fnv1a(&minimized);
                let path = format!("fuzz-artifacts/crashes/{format}/{hash:016x}.bin");
                write_create_new_synced(Path::new(&path), &minimized)?;
                return Err(io::Error::other(format!(
                    "fuzz {signature} at iteration {iteration}; minimized artifact={path}"
                )));
            }
        }
    }
    if has_flag(args, "--update-corpus") {
        let proposed = mutate_case(&format, &cases[0], &mut rng);
        let hash = fnv1a(&proposed);
        let path = Path::new(&corpus).join(format!("{hash:016x}.bin"));
        if !path.exists() {
            write_create_new_synced(&path, &proposed)?;
            println!("fuzz corpus proposal={}", path.display());
        }
        regenerate_fuzz_manifest(Path::new("fuzz/corpus/manifest.tsv"))?;
    }
    println!(
        "fuzz format={format} seed={seed} iterations={iterations} corpus={} ok={ok} typed_error={typed_errors} budget={allocation_budget}",
        cases.len()
    );
    Ok(())
}

fn fuzz_inventory_budget(format: &str) -> io::Result<(usize, usize, u64)> {
    let text = fs::read_to_string("fuzz/inventory.tsv")?;
    let mut lines = text.lines();
    if lines.next()
        != Some(
            "format\towner\tdecoder\tversion\tmax_input_bytes\tmax_declared_count\tallocation_budget_bytes\twork_budget",
        )
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid fuzz inventory header",
        ));
    }
    for (offset, line) in lines.enumerate() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 8 || fields.iter().any(|field| field.is_empty()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid fuzz inventory line {}", offset + 2),
            ));
        }
        if fields[0] == format {
            let parse_usize = |value: &str| {
                value.parse::<usize>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid inventory byte budget")
                })
            };
            return Ok((
                parse_usize(fields[4])?,
                parse_usize(fields[6])?,
                fields[7].parse::<u64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid inventory work budget")
                })?,
            ));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("format {format} is absent from fuzz inventory"),
    ))
}

fn collect_corpus_files(root: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|extension| extension == "bin") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn regenerate_fuzz_manifest(path: &Path) -> io::Result<()> {
    let inventory = fs::read_to_string("fuzz/inventory.tsv")?;
    let mut output = String::from("format\tpath\tcontent_hash\texpected\tsignature\tbudget\n");
    for line in inventory.lines().skip(1) {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 8 {
            continue;
        }
        let format = fields[0];
        let allocation_budget = fields[6].parse::<usize>().map_err(io::Error::other)?;
        for case in collect_corpus_files(&Path::new("fuzz/corpus").join(format))? {
            let bytes = fs::read(&case)?;
            let signature = fuzz_decode(format, &bytes, allocation_budget).signature();
            output.push_str(&format!(
                "{format}\t{}\t{:016x}\t{}\t{signature}\t{allocation_budget}\n",
                case.display(),
                fnv1a(&bytes),
                if signature == "ok" {
                    "ok"
                } else {
                    "typed-error"
                },
            ));
        }
    }
    write_replace_atomic(path, output.as_bytes())
}

fn run_coverage_search(args: &[String]) -> io::Result<()> {
    let iterations = parse_u64_flag(args, "--iterations")?.unwrap_or(100);
    let profile = parse_profile(args, FaultProfile::Rough)?;
    let mut corpus = Vec::<RunSpec>::new();
    let mut guided_coverage = BTreeSet::new();
    let mut uniform_coverage = BTreeSet::new();
    let mut guided_failures = 0_u64;
    let mut uniform_failures = 0_u64;
    let guided_started = Instant::now();
    for iteration in 0..iterations {
        let seed = Seed::new(iteration);
        let mut spec = fixture_spec(seed, profile, true);
        if !corpus.is_empty() {
            let parent = &corpus[iteration as usize % corpus.len()];
            spec.plan = mutate_fault_plan(&parent.plan, seed, spec.end_time);
        }
        let run = run_spec(spec.clone(), RecorderLevel::Gate).map_err(io::Error::other)?;
        let coverage = trace_coverage(&run.trace);
        let novel = coverage.iter().any(|edge| !guided_coverage.contains(edge));
        guided_coverage.extend(coverage);
        if novel {
            corpus.push(spec);
        }
        if !run.healthy() || !matches!(run.verdict, Verdict::Linearizable { .. }) {
            guided_failures = guided_failures.saturating_add(1);
        }
    }
    let guided_seconds = guided_started.elapsed().as_secs_f64();
    let uniform_started = Instant::now();
    for iteration in 0..iterations {
        let run = run_spec(
            fixture_spec(Seed::new(iteration), profile, true),
            RecorderLevel::Gate,
        )
        .map_err(io::Error::other)?;
        uniform_coverage.extend(trace_coverage(&run.trace));
        if !run.healthy() || !matches!(run.verdict, Verdict::Linearizable { .. }) {
            uniform_failures = uniform_failures.saturating_add(1);
        }
    }
    let uniform_seconds = uniform_started.elapsed().as_secs_f64();
    let per_hour = |failures: u64, seconds: f64| {
        if seconds == 0.0 {
            0.0
        } else {
            failures as f64 * 3_600.0 / seconds
        }
    };
    println!(
        "coverage-search profile={} iterations={} corpus={} guided_edges={} uniform_edges={} guided_failures={} uniform_failures={} guided_bugs_per_cpu_hour={:.3} uniform_bugs_per_cpu_hour={:.3}",
        profile.as_str(),
        iterations,
        corpus.len(),
        guided_coverage.len(),
        uniform_coverage.len(),
        guided_failures,
        uniform_failures,
        per_hour(guided_failures, guided_seconds),
        per_hour(uniform_failures, uniform_seconds),
    );
    Ok(())
}

fn run_sequence(args: &[String]) -> io::Result<()> {
    let artifact = args.first().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cc-swarm sequence <artifact.json> --output diagram.svg",
        )
    })?;
    let output = parse_string_flag(args, "--output")
        .unwrap_or_else(|| String::from("artifacts/sequence.svg"));
    let text = fs::read_to_string(artifact)?;
    let seed = extract_seed(&text)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "artifact has no seed"))?;
    let profile = extract_profile(&text).unwrap_or(FaultProfile::Rough);
    let spec = extract_run_spec(&text, seed, profile);
    let run = run_spec(spec, RecorderLevel::Theater).map_err(io::Error::other)?;
    if let Some(parent) = Path::new(&output).parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, sequence_diagram_svg(&run.trace))?;
    println!(
        "sequence diagram: wrote {output} events={}",
        run.trace.events.len()
    );
    for violation in &run.invariant_violations {
        println!("invariant violation: {violation}");
    }
    Ok(())
}

fn run_explain(args: &[String]) -> io::Result<()> {
    let failure = parse_string_flag(args, "--failure").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cc-swarm explain --failure <artifact.json> [--svg <path>]",
        )
    })?;
    let text = fs::read_to_string(&failure)?;
    let seed = extract_seed(&text)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "artifact has no seed"))?;
    let profile = extract_profile(&text).unwrap_or(FaultProfile::Rough);
    let spec = extract_run_spec(&text, seed, profile);
    let run = run_spec(spec, RecorderLevel::Theater).map_err(io::Error::other)?;
    let anomalies = classify_anomalies(&run.history, &BTreeMap::new());
    let anomaly = anomalies
        .iter()
        .find(|anomaly| anomaly.class.as_str() != "unclassified")
        .unwrap_or_else(|| &anomalies[0]);
    let witness = match &run.verdict {
        Verdict::NotLinearizable { witness, .. } => Some(witness.clone()),
        _ => None,
    };
    let ids = witness
        .as_ref()
        .map_or_else(Vec::new, |witness| witness.operation_ids.clone());
    println!(
        "anomaly={} statement={}",
        anomaly.class.as_str(),
        anomaly.predicate
    );
    if let Some(witness) = &witness {
        println!(
            "witness operations={} oracle_calls={} one_minimal={} budget_exhausted={}",
            witness.operation_ids.len(),
            witness.oracle_calls,
            witness.one_minimal,
            witness.budget_exhausted
        );
    } else {
        println!("witness unavailable: verdict is not a completed non-linearizable result");
    }
    let selected: Vec<_> = if ids.is_empty() {
        run.history.operations.iter().take(16).collect()
    } else {
        run.history
            .operations
            .iter()
            .filter(|operation| ids.contains(&operation.id))
            .collect()
    };
    for operation in &selected {
        println!(
            "op={} client={} seq={} interval={}..{} key={} outcome={}",
            operation.id,
            operation.client,
            operation.sequence,
            operation.invoke.as_nanos(),
            operation
                .complete
                .map_or_else(|| String::from("open"), |time| time.as_nanos().to_string()),
            hex(operation_key(&operation.kind)),
            operation_outcome(&operation.outcome),
        );
    }
    let first = selected
        .iter()
        .map(|operation| operation.invoke)
        .min()
        .unwrap_or(Time::from_nanos(0));
    let last = selected
        .iter()
        .filter_map(|operation| operation.complete)
        .max()
        .unwrap_or(first);
    let event_kinds = [
        EventKind::RoleChange,
        EventKind::Commit,
        EventKind::Apply,
        EventKind::ConfChange,
        EventKind::SnapshotInstall,
        EventKind::Fault,
    ];
    let relevant = run
        .trace
        .events
        .iter()
        .filter(|event| {
            event.time >= first && event.time <= last && event_kinds.contains(&event.kind)
        })
        .collect::<Vec<_>>();
    for event in relevant.iter().take(128) {
        println!(
            "event={} time={} node={} kind={} payload={}",
            event.seq,
            event.time.as_nanos(),
            event.node.map_or(0, NodeId::get),
            event.kind.as_str(),
            trace_payload(&event.payload),
        );
    }
    let relevant_nodes = relevant
        .iter()
        .filter_map(|event| event.node)
        .map(NodeId::get)
        .collect::<BTreeSet<_>>();
    println!(
        "hypothesis=Terms and nodes {:?} appear in {} election/commit/apply/config/snapshot/fault events in the witness interval; this is diagnostic evidence, not a root-cause claim.",
        relevant_nodes,
        relevant.len(),
    );
    if let Some(path) = parse_string_flag(args, "--svg") {
        if let Some(parent) = Path::new(&path).parent() {
            fs::create_dir_all(parent)?;
        }
        let mut witness_trace = run.trace.clone();
        witness_trace
            .events
            .retain(|event| event.time >= first && event.time <= last);
        fs::write(&path, sequence_diagram_svg(&witness_trace))?;
        println!("svg={path}");
    }
    Ok(())
}

fn run_trace(args: &[String]) -> io::Result<()> {
    let path = args.first().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cc-swarm trace <trace.cctr|trace.json> [--node ID] [--kind K[,K]] [--since D] [--until D] [--grep HEX_OR_TEXT] [--tail N] [--stats]",
        )
    })?;
    let raw = fs::read(path)?;
    let events = if raw.starts_with(b"CCTR") {
        decode_trace_display_events(&raw)?
    } else {
        decode_trace_json(std::str::from_utf8(&raw).map_err(io::Error::other)?)?
            .events
            .into_iter()
            .map(|event| TraceDisplayEvent {
                seq: event.seq,
                time: event.time,
                node: event.node,
                kind: event.kind.as_str().to_owned(),
                payload: event.payload,
            })
            .collect()
    };
    let node = parse_u64_flag(args, "--node")?.map(NodeId::new);
    let kinds = parse_string_flag(args, "--kind")
        .map(|value| value.split(',').map(str::to_owned).collect::<BTreeSet<_>>())
        .unwrap_or_default();
    let since = parse_string_flag(args, "--since")
        .as_deref()
        .map(parse_trace_duration)
        .transpose()?;
    let until = parse_string_flag(args, "--until")
        .as_deref()
        .map(parse_trace_duration)
        .transpose()?;
    let needle = parse_string_flag(args, "--grep")
        .map(|value| parse_trace_needle(&value))
        .transpose()?;
    let tail =
        parse_u64_flag(args, "--tail")?.map(|value| usize::try_from(value).unwrap_or(usize::MAX));
    let mut rows = VecDeque::new();
    let mut row_bytes = 0_usize;
    const MAX_TAIL_BYTES: usize = 8 * 1024 * 1024;
    let mut kind_counts = BTreeMap::<String, u64>::new();
    let mut node_counts = BTreeMap::<String, u64>::new();
    let mut instant_counts = BTreeMap::<u64, u64>::new();
    for event in &events {
        if node.is_some_and(|node| event.node != Some(node))
            || !kinds.is_empty() && !kinds.contains(&event.kind)
            || since.is_some_and(|time| event.time < time)
            || until.is_some_and(|time| event.time > time)
            || needle.as_ref().is_some_and(|needle| {
                !event
                    .payload
                    .windows(needle.len().max(1))
                    .any(|part| part == needle)
            })
        {
            continue;
        }
        *kind_counts.entry(event.kind.clone()).or_default() += 1;
        *node_counts
            .entry(
                event
                    .node
                    .map_or_else(|| String::from("-"), |id| id.get().to_string()),
            )
            .or_default() += 1;
        *instant_counts.entry(event.time.as_nanos()).or_default() += 1;
        let row = format!(
            "{:>12} · {:>3} · {:<16} · {}",
            event.time.as_nanos(),
            event
                .node
                .map_or_else(|| String::from("-"), |id| id.get().to_string()),
            event.kind,
            trace_payload(&event.payload),
        );
        if let Some(limit) = tail {
            if limit != 0 {
                let bytes = row.len();
                if bytes > MAX_TAIL_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "one trace row exceeds the tail byte cap",
                    ));
                }
                rows.push_back(row);
                row_bytes = row_bytes.saturating_add(bytes);
                while rows.len() > limit || row_bytes > MAX_TAIL_BYTES {
                    if let Some(removed) = rows.pop_front() {
                        row_bytes = row_bytes.saturating_sub(removed.len());
                    }
                }
            }
        } else {
            println!("{row}");
        }
    }
    if tail.is_some() {
        for row in rows {
            println!("{row}");
        }
    }
    if has_flag(args, "--stats") {
        let busiest = instant_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map_or_else(
                || String::from("none"),
                |(time, count)| format!("{time}ns:{count}"),
            );
        println!(
            "stats kinds={} nodes={} busiest={busiest}",
            kind_counts
                .iter()
                .map(|(kind, count)| format!("{kind}:{count}"))
                .collect::<Vec<_>>()
                .join(","),
            node_counts
                .iter()
                .map(|(node, count)| format!("{node}:{count}"))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TraceDisplayEvent {
    seq: u64,
    time: Time,
    node: Option<NodeId>,
    kind: String,
    payload: Vec<u8>,
}

fn decode_trace_display_events(bytes: &[u8]) -> io::Result<Vec<TraceDisplayEvent>> {
    struct Cursor<'a> {
        bytes: &'a [u8],
        offset: usize,
    }
    impl<'a> Cursor<'a> {
        fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
            let start = self.offset;
            let end = start.checked_add(length).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("trace at byte offset {start}: length overflow"),
                )
            })?;
            let value = self.bytes.get(start..end).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("trace at byte offset {start}: need {length} bytes"),
                )
            })?;
            self.offset = end;
            Ok(value)
        }
        fn u8(&mut self) -> io::Result<u8> {
            Ok(self.take(1)?[0])
        }
        fn u16(&mut self) -> io::Result<u16> {
            Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("u16")))
        }
        fn u32(&mut self) -> io::Result<u32> {
            Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("u32")))
        }
        fn u64(&mut self) -> io::Result<u64> {
            Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("u64")))
        }
        fn bounded_bytes(&mut self, maximum: usize, field: &str) -> io::Result<Vec<u8>> {
            let length_offset = self.offset;
            let length = usize::try_from(self.u32()?).unwrap_or(usize::MAX);
            if length > maximum || length > self.bytes.len().saturating_sub(self.offset) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "trace at byte offset {length_offset}: {field} length {length} exceeds remaining/budget"
                    ),
                ));
            }
            Ok(self.take(length)?.to_vec())
        }
    }

    let mut cursor = Cursor { bytes, offset: 0 };
    if cursor.take(4)? != b"CCTR" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trace at byte offset 0: invalid magic",
        ));
    }
    let version_offset = cursor.offset;
    if cursor.u16()? != cc_core::TRACE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("trace at byte offset {version_offset}: unsupported version"),
        ));
    }
    let _seed = cursor.u64()?;
    let _config_hash = cursor.u32()?;
    let build = cursor.bounded_bytes(cc_core::MAX_CODEC_BYTES, "build")?;
    if std::str::from_utf8(&build).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trace build label is not UTF-8",
        ));
    }
    let count_offset = cursor.offset;
    let count = usize::try_from(cursor.u32()?).unwrap_or(usize::MAX);
    const MIN_EVENT_BYTES: usize = 8 + 8 + 1 + 1 + 4;
    let maximum_count = bytes.len().saturating_sub(cursor.offset) / MIN_EVENT_BYTES;
    if count > maximum_count || count > cc_core::MAX_CODEC_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "trace at byte offset {count_offset}: event count {count} exceeds remaining/budget"
            ),
        ));
    }
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let seq = cursor.u64()?;
        let time = Time::from_nanos(cursor.u64()?);
        let node = match cursor.u8()? {
            0 => None,
            1 => Some(NodeId::new(cursor.u64()?)),
            tag => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "trace at byte offset {}: invalid node tag {tag}",
                        cursor.offset.saturating_sub(1)
                    ),
                ));
            }
        };
        let tag = cursor.u8()?;
        let kind = EventKind::from_code(tag)
            .map_or_else(|| format!("unknown-{tag}"), |kind| kind.as_str().to_owned());
        let payload = cursor.bounded_bytes(cc_core::MAX_CODEC_BYTES, "event payload")?;
        events.push(TraceDisplayEvent {
            seq,
            time,
            node,
            kind,
            payload,
        });
    }
    if cursor.offset != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("trace at byte offset {}: trailing bytes", cursor.offset),
        ));
    }
    Ok(events)
}

fn operation_key(kind: &OperationKind) -> &[u8] {
    match kind {
        OperationKind::Set { key, .. }
        | OperationKind::Get { key }
        | OperationKind::Del { key }
        | OperationKind::Incr { key }
        | OperationKind::Cas { key, .. } => key,
        OperationKind::Scan { prefix, .. } => prefix.as_deref().unwrap_or_default(),
        OperationKind::Batch { .. } => &[],
    }
}

fn operation_outcome(outcome: &Outcome) -> String {
    match outcome {
        Outcome::Ok => String::from("ok"),
        Outcome::Value(Some(value)) => format!("value:{}", hex(value)),
        Outcome::Value(None) => String::from("nil"),
        Outcome::Integer(value) => format!("integer:{value}"),
        Outcome::Cas(value) => format!("cas:{value}"),
        Outcome::Scan(values) => format!("scan:{}", values.len()),
        Outcome::Batch { replies } => format!("batch:{}", replies.len()),
        Outcome::Error => String::from("error"),
        Outcome::Timeout => String::from("timeout"),
    }
}

fn parse_trace_duration(value: &str) -> io::Result<Time> {
    let (digits, multiplier) = if let Some(value) = value.strip_suffix("ns") {
        (value, 1)
    } else if let Some(value) = value.strip_suffix("ms") {
        (value, 1_000_000)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000_000_000)
    } else {
        (value, 1)
    };
    let nanos = digits
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(multiplier))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid trace duration"))?;
    Ok(Time::from_nanos(nanos))
}

fn parse_trace_needle(value: &str) -> io::Result<Vec<u8>> {
    if let Some(hex_value) = value.strip_prefix("0x") {
        return decode_hex(hex_value)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid trace hex grep"));
    }
    if value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "trace grep must not be empty",
        ));
    }
    Ok(value.as_bytes().to_vec())
}

fn trace_payload(payload: &[u8]) -> String {
    const MAX_DISPLAY: usize = 96;
    let shown = &payload[..payload.len().min(MAX_DISPLAY)];
    let mut value = if shown
        .iter()
        .all(|byte| byte.is_ascii_graphic() || *byte == b' ')
    {
        String::from_utf8_lossy(shown).into_owned()
    } else {
        format!("0x{}", hex(shown))
    };
    if payload.len() > shown.len() {
        value.push('…');
    }
    value
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Some((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

fn decode_trace_json(text: &str) -> io::Result<Trace> {
    let seed = extract_seed(text)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON has no seed"))?;
    let config_hash = extract_number(text, "\"config_hash\":")
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON config hash"))?;
    let mut trace = Trace::new(seed, config_hash);
    for fragment in text.split("{\"seq\":").skip(1) {
        let body = fragment.split('}').next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "trace JSON event terminator")
        })?;
        let seq = extract_number(body, "")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON sequence"))?;
        let time = extract_number(body, "\"time_ns\":")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON time"))?;
        let node_value = body
            .split("\"node\":")
            .nth(1)
            .and_then(|value| value.split(',').next())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON node"))?;
        let node = if node_value == "null" {
            None
        } else {
            Some(NodeId::new(node_value.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "trace JSON node id")
            })?))
        };
        let kind = event_kind_from_name(
            &extract_string(body, "\"kind\":\"")
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON kind"))?,
        )
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON event kind"))?;
        let payload = decode_hex(
            &extract_string(body, "\"payload_hex\":\"")
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON payload"))?,
        )
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "trace JSON payload hex"))?;
        trace
            .events
            .push(Event::new(seq, Time::from_nanos(time), node, kind, payload));
    }
    Ok(trace)
}

fn event_kind_from_name(value: &str) -> Option<EventKind> {
    [
        EventKind::NetSend,
        EventKind::NetRecv,
        EventKind::NetDrop,
        EventKind::IoIssue,
        EventKind::IoDone,
        EventKind::IoLost,
        EventKind::TimerSet,
        EventKind::TimerFire,
        EventKind::RoleChange,
        EventKind::VoteReq,
        EventKind::VoteGrant,
        EventKind::VoteDeny,
        EventKind::AppendSent,
        EventKind::AppendAck,
        EventKind::Commit,
        EventKind::Apply,
        EventKind::SnapshotStart,
        EventKind::SnapshotChunk,
        EventKind::SnapshotInstall,
        EventKind::ConfChange,
        EventKind::ClientInvoke,
        EventKind::ClientOk,
        EventKind::ClientFail,
        EventKind::ClientTimeout,
        EventKind::WalRecover,
        EventKind::Flush,
        EventKind::Compact,
        EventKind::Fault,
        EventKind::CheckerNote,
    ]
    .into_iter()
    .find(|kind| kind.as_str() == value)
}

fn run_diff(args: &[String]) -> io::Result<()> {
    if args.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: cc-swarm diff <artifact-a.json> <artifact-b.json>",
        ));
    }
    let mut runs = Vec::with_capacity(2);
    for path in args {
        let text = fs::read_to_string(path)?;
        let seed = extract_seed(&text)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "artifact has no seed"))?;
        let profile = extract_profile(&text).unwrap_or(FaultProfile::Rough);
        let spec = extract_run_spec(&text, seed, profile);
        runs.push(run_spec(spec, RecorderLevel::Theater).map_err(io::Error::other)?);
    }
    match semantic_trace_diff(&runs[0].trace, &runs[1].trace) {
        None => println!(
            "trace diff: identical events={}",
            runs[0].trace.events.len()
        ),
        Some(difference) => println!(
            "trace diff: event={} field={} left={} right={} left_events={} right_events={}",
            difference.event_index,
            difference.field,
            difference.left,
            difference.right,
            difference.left_len,
            difference.right_len,
        ),
    }
    Ok(())
}

fn run_proxy(args: &[String]) -> io::Result<()> {
    let listen =
        parse_string_flag(args, "--listen").unwrap_or_else(|| String::from("127.0.0.1:7379"));
    let upstream =
        parse_string_flag(args, "--upstream").unwrap_or_else(|| String::from("127.0.0.1:7101"));
    let drop_every = parse_u64_flag(args, "--drop-every")?.unwrap_or(0);
    let delay_ms = parse_u64_flag(args, "--delay-ms")?.unwrap_or(0);
    let listener = TcpListener::bind(&listen)?;
    println!(
        "cc-swarm proxy listening={listen} upstream={upstream} drop_every={drop_every} delay_ms={delay_ms}"
    );
    for client in listener.incoming() {
        let client = client?;
        let upstream_address = upstream.clone();
        thread::spawn(move || {
            if let Err(error) = proxy_connection(client, &upstream_address, drop_every, delay_ms) {
                eprintln!("proxy connection: {error}");
            }
        });
    }
    Ok(())
}

fn proxy_connection(
    client: TcpStream,
    upstream_address: &str,
    drop_every: u64,
    delay_ms: u64,
) -> io::Result<()> {
    let upstream = TcpStream::connect(upstream_address)?;
    let client_reply = client.try_clone()?;
    let upstream_request = upstream.try_clone()?;
    let first = thread::spawn(move || relay(client, upstream_request, drop_every, delay_ms));
    let second = thread::spawn(move || relay(upstream, client_reply, drop_every, delay_ms));
    first
        .join()
        .map_err(|_| io::Error::other("proxy request relay panicked"))??;
    second
        .join()
        .map_err(|_| io::Error::other("proxy reply relay panicked"))??;
    Ok(())
}

fn relay(
    mut source: TcpStream,
    mut target: TcpStream,
    drop_every: u64,
    delay_ms: u64,
) -> io::Result<()> {
    let mut buffer = [0_u8; 64 * 1024];
    let mut chunks = 0_u64;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            let _ = target.shutdown(Shutdown::Write);
            return Ok(());
        }
        chunks = chunks.saturating_add(1);
        if drop_every != 0 && chunks.is_multiple_of(drop_every) {
            continue;
        }
        if delay_ms != 0 {
            thread::sleep(std::time::Duration::from_millis(delay_ms));
        }
        target.write_all(&buffer[..read])?;
    }
}

fn emit_determinism_trace() -> io::Result<()> {
    io::stdout().write_all(&deterministic_cluster_trace(Seed::new(0xcc)))
}

fn run_determinism_seeds(count: u64) -> io::Result<()> {
    for seed in 0..count {
        selfcheck_cluster(Seed::new(seed)).map_err(io::Error::other)?;
    }
    println!("determinism: PASS seeds={count}");
    Ok(())
}

fn selfcheck_cluster(seed: Seed) -> Result<(), &'static str> {
    for profile in DETERMINISM_PROFILES {
        let first = deterministic_cluster_trace_for(seed, profile);
        let second = deterministic_cluster_trace_for(seed, profile);
        if first != second {
            return Err("cluster determinism divergence");
        }
    }
    let first = deterministic_cluster_trace(seed);
    let second = deterministic_cluster_trace(seed);
    if first == second {
        Ok(())
    } else {
        Err("cluster trace diverged on the second run")
    }
}

fn run_one(args: &[String]) -> io::Result<()> {
    let seed = parse_seed(args, 0)?;
    let profile = parse_profile(args, FaultProfile::Calm)?;
    let disk_profile = load_disk_profile(args)?;
    let mut spec = fixture_spec(seed, profile, false);
    apply_loaded_disk_profile(&mut spec, disk_profile.as_ref());
    let run = run_spec(spec, RecorderLevel::Theater).map_err(io::Error::other)?;
    print_run_summary(seed, profile, &run, "one");
    if has_flag(args, "--export-json") {
        fs::create_dir_all("artifacts")?;
        fs::write(format!("artifacts/{seed}.json"), run.artifact_json(profile))?;
    }
    if let Some(path) = parse_string_flag(args, "--export-history") {
        let written = write_history_tsv(&path, &run.history)?;
        println!("history export file={path} operations={written}");
    }
    if run.healthy() {
        Ok(())
    } else {
        Err(io::Error::other("cluster fixture found a failure"))
    }
}

fn run_campaign(args: &[String]) -> io::Result<()> {
    let started_unix_ms = unix_time_ms()?;
    let seeds = parse_u64_flag(args, "--seeds")?.unwrap_or(1);
    let profile = parse_profile(args, FaultProfile::Rough)?;
    let shard = parse_shard(args)?;
    let jobs = parse_u64_flag(args, "--jobs")?.unwrap_or(1).max(1);
    let ledger_path = parse_string_flag(args, "--ledger");
    let build_label = parse_string_flag(args, "--build").unwrap_or_else(|| String::from("dev"));
    let disk_profile = load_disk_profile(args)?;
    if build_label.is_empty() || build_label.contains(['\t', '\n', '\r']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--build must be a nonempty single ledger field",
        ));
    }
    let resume = has_flag(args, "--resume");
    let resume_failures = has_flag(args, "--resume-failures");
    if resume_failures && !resume {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--resume-failures requires --resume",
        ));
    }
    let config_hash = campaign_config_hash(profile, disk_profile.as_ref());
    let expected_ledger_kind = if cc_swarm::ACTIVE_KATA.is_some() {
        LedgerKind::Kata
    } else {
        LedgerKind::Production
    };
    let prior_ledger = match ledger_path.as_deref() {
        Some(path) if Path::new(path).exists() => read_ledger(Path::new(path))?,
        Some(_) | None => match expected_ledger_kind {
            LedgerKind::Production => SeedLedger::default(),
            LedgerKind::Kata => SeedLedger::kata(),
        },
    };
    if prior_ledger.kind() != expected_ledger_kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "campaign ledger type does not match this build",
        ));
    }
    if resume && ledger_path.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--resume requires --ledger",
        ));
    }
    let prior_rows = Arc::new(
        prior_ledger
            .rows()
            .filter(|row| {
                row.key.build_label == build_label
                    && row.key.config_hash == config_hash
                    && row.key.profile == profile
                    && (resume_failures || row.verdict == LedgerVerdict::Ok)
            })
            .map(|row| row.key.seed)
            .collect::<BTreeSet<_>>(),
    );
    let worker_count = jobs.min(seeds.max(1)) as usize;
    let next_seed = Arc::new(AtomicU64::new(0));
    let export_json = has_flag(args, "--export-json");
    fs::create_dir_all("artifacts")?;
    let started = Instant::now();
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let next_seed = Arc::clone(&next_seed);
        let prior_rows = Arc::clone(&prior_rows);
        let build_label = build_label.clone();
        let disk_profile = disk_profile.clone();
        workers.push(thread::spawn(move || -> io::Result<CampaignWorkerSummary> {
            let mut failures = 0_u64;
            let mut history_failures = 0_u64;
            let mut events = 0_u64;
            let mut runs = 0_u64;
            let mut beacon_hits = [0_u64; REACHABILITY_BEACONS.len()];
            let mut rows = Vec::new();
            loop {
                let seed = next_seed.fetch_add(1, Ordering::Relaxed);
                if seed >= seeds {
                    break;
                }
                if shard.is_some_and(|shard| !shard.contains(seed)) {
                    continue;
                }
                if resume && prior_rows.contains(&seed) {
                    continue;
                }
                runs = runs.saturating_add(1);
                let mut spec = fixture_spec(Seed::new(seed), profile, true);
                apply_loaded_disk_profile(&mut spec, disk_profile.as_ref());
                let result = run_spec(spec, RecorderLevel::Campaign)
                    .map_err(|error| error.to_string());
                match result {
                    Ok(run) => {
                        events = events.saturating_add(run.event_count);
                        for (total, hits) in
                            beacon_hits
                                .iter_mut()
                                .zip(reachability_beacons(&run.trace, run.had_leader))
                        {
                            *total = total.saturating_add(hits);
                        }
                        let failed = !run.healthy();
                        let history_failed = !matches!(run.verdict, Verdict::Linearizable { .. });
                        let (ledger_verdict, checker_states) = match &run.verdict {
                            Verdict::Linearizable { visited } => (LedgerVerdict::Ok, *visited),
                            Verdict::NotLinearizable { visited, .. } => {
                                (LedgerVerdict::NotLinearizable, *visited)
                            }
                            Verdict::Undecided { visited } => (LedgerVerdict::Undecided, *visited),
                        };
                        if failed {
                            failures = failures.saturating_add(1);
                        }
                        if history_failed {
                            history_failures = history_failures.saturating_add(1);
                        }
                        if failed || history_failed || export_json {
                            fs::write(format!("artifacts/{seed}.json"), run.artifact_json(profile))?;
                        }
                        rows.push(LedgerRow {
                            key: LedgerKey {
                                build_label: build_label.clone(),
                                config_hash,
                                profile,
                                seed,
                            },
                            verdict: if failed {
                                LedgerVerdict::Invariant
                            } else {
                                ledger_verdict
                            },
                            events: run.event_count,
                            checker_states,
                            peak_total_bytes: run.peak_total_bytes,
                            artifact_hash: (failed || history_failed)
                                .then(|| fnv1a(run.artifact_json(profile).as_bytes())),
                        });
                    }
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        fs::write(
                            format!("artifacts/{seed}.json"),
                            format!(
                                "{{\"fixture_version\":1,\"synthetic\":{},\"seed\":\"{seed}\",\"profile\":\"{}\",\"error\":\"{}\"}}",
                                cc_swarm::ACTIVE_KATA.is_some(),
                                profile.as_str(),
                                json_escape(&error)
                            ),
                        )?;
                        rows.push(LedgerRow {
                            key: LedgerKey {
                                build_label: build_label.clone(),
                                config_hash,
                                profile,
                                seed,
                            },
                            verdict: LedgerVerdict::Error,
                            events: 0,
                            checker_states: 0,
                            peak_total_bytes: 0,
                            artifact_hash: Some(fnv1a(error.as_bytes())),
                        });
                    }
                }
            }
            Ok(CampaignWorkerSummary {
                failures,
                history_failures,
                events,
                runs,
                beacon_hits,
                rows,
            })
        }));
    }
    let mut failures = 0_u64;
    let mut history_failures = 0_u64;
    let mut events = 0_u64;
    let mut runs = 0_u64;
    let mut beacon_hits = [0_u64; REACHABILITY_BEACONS.len()];
    let mut ledger_rows = Vec::new();
    for worker in workers {
        let worker = worker
            .join()
            .map_err(|_| io::Error::other("campaign worker panicked"))??;
        failures = failures.saturating_add(worker.failures);
        history_failures = history_failures.saturating_add(worker.history_failures);
        events = events.saturating_add(worker.events);
        runs = runs.saturating_add(worker.runs);
        ledger_rows.extend(worker.rows);
        for (total, hits) in beacon_hits.iter_mut().zip(worker.beacon_hits) {
            *total = total.saturating_add(hits);
        }
    }
    let elapsed = started.elapsed().as_secs_f64().max(f64::EPSILON);
    println!(
        "campaign profile={} seeds={} runs={} jobs={} failures={} history_failures={} events={} runs_per_sec={:.2}",
        profile.as_str(),
        seeds,
        runs,
        jobs,
        failures,
        history_failures,
        events,
        runs as f64 / elapsed
    );
    let beacon_report = REACHABILITY_BEACONS
        .iter()
        .zip(beacon_hits)
        .map(|(name, hits)| format!("{name}:{hits}"))
        .collect::<Vec<_>>()
        .join(",");
    let missing = REACHABILITY_BEACONS
        .iter()
        .zip(beacon_hits)
        .filter_map(|(name, hits)| (hits == 0).then_some(*name))
        .collect::<Vec<_>>()
        .join(",");
    println!("reachability beacons={beacon_report} missing={missing}");
    if let Some(path) = ledger_path.as_deref() {
        let mut ledger = prior_ledger;
        let mut appended = Vec::new();
        for row in ledger_rows {
            if ledger.insert(row.clone()).map_err(io::Error::other)? {
                appended.push(row);
            }
        }
        let appended_count = append_ledger_rows(Path::new(path), &appended, expected_ledger_kind)?;
        println!(
            "ledger path={path} appended={} config_hash={config_hash:016x} build={build_label}",
            appended_count
        );
        let ended_unix_ms = unix_time_ms()?;
        let ledger_bytes = fs::read(path)?;
        let receipt_path = parse_string_flag(args, "--receipt").unwrap_or_else(|| {
            let shard_label = shard.map_or_else(
                || String::from("all"),
                |shard| format!("{}-of-{}", shard.index, shard.total),
            );
            format!(
                "campaigns/receipts/{}-{}-{shard_label}-{started_unix_ms}-{}.json",
                filename_component(&build_label),
                profile.as_str(),
                std::process::id(),
            )
        });
        let receipt = format!(
            "{{\"schema_version\":1,\"synthetic\":{},\"source_ledger\":\"{}\",\"source_ledger_hash\":\"{:016x}\",\"build_label\":\"{}\",\"config_hash\":\"{config_hash:016x}\",\"profile\":\"{}\",\"shard\":\"{}\",\"range\":\"0..{seeds}\",\"host\":\"{}-{}\",\"started_unix_ms\":{started_unix_ms},\"ended_unix_ms\":{ended_unix_ms},\"wall_duration_ms\":{},\"runs\":{runs},\"events\":{events},\"runs_per_sec\":{:.6}}}\n",
            cc_swarm::ACTIVE_KATA.is_some(),
            json_escape(path),
            fnv1a(&ledger_bytes),
            json_escape(&build_label),
            profile.as_str(),
            shard.map_or_else(
                || String::from("all"),
                |shard| format!("{}/{}", shard.index, shard.total)
            ),
            std::env::consts::OS,
            std::env::consts::ARCH,
            ended_unix_ms.saturating_sub(started_unix_ms),
            runs as f64 / elapsed,
        );
        write_create_new_synced(Path::new(&receipt_path), receipt.as_bytes())?;
        println!("campaign receipt={receipt_path}");
    }
    if let Some(required) = parse_string_flag(args, "--require-beacon") {
        let Some(position) = REACHABILITY_BEACONS
            .iter()
            .position(|name| *name == required)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown reachability beacon",
            ));
        };
        if beacon_hits[position] == 0 {
            return Err(io::Error::other(format!(
                "reachability beacon never fired: {required}"
            )));
        }
    }
    if failures == 0 && history_failures == 0 {
        Ok(())
    } else {
        Err(io::Error::other("campaign found a failure"))
    }
}

fn parse_shard(args: &[String]) -> io::Result<Option<Shard>> {
    let Some(value) = parse_string_flag(args, "--shard") else {
        return Ok(None);
    };
    let (index, total) = value.split_once('/').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "--shard must have the form <index>/<total>",
        )
    })?;
    let index = index
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid --shard index"))?;
    let total = total
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid --shard total"))?;
    Shard::new(index, total).map(Some).map_err(io::Error::other)
}

fn run_regressions() -> io::Result<()> {
    let path = Path::new("exhibits/regressions.toml");
    if !path.exists() {
        println!("regressions: PASS entries=0");
        return Ok(());
    }
    let content = fs::read_to_string(path)?;
    let mut entries = Vec::new();
    let mut seed = None;
    let mut profile = FaultProfile::Rough;
    for line in content.lines().map(str::trim) {
        if line == "[[regression]]" {
            if let Some(seed) = seed.take() {
                entries.push((seed, profile));
            }
        } else if let Some(value) = line.strip_prefix("seed =") {
            seed = parse_seed_text(value.trim().trim_matches('"'));
        } else if let Some(value) = line.strip_prefix("profile =") {
            profile = FaultProfile::parse(value.trim().trim_matches('"')).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid regression profile")
            })?;
        }
    }
    if let Some(seed) = seed {
        entries.push((seed, profile));
    }
    // A corpus entry records a seed that once failed and has since been fixed,
    // so the assertion is that it stays clean. Requiring it to keep failing
    // would make the corpus impossible to populate after a fix.
    for (seed, profile) in &entries {
        let run = run_spec(fixture_spec(*seed, *profile, true), RecorderLevel::Campaign)
            .map_err(io::Error::other)?;
        if !run.healthy() {
            return Err(io::Error::other(format!(
                "regression {seed} ({}) reopened: invariants or liveness failed",
                profile.as_str()
            )));
        }
        if !matches!(run.verdict, Verdict::Linearizable { .. }) {
            return Err(io::Error::other(format!(
                "regression {seed} ({}) reopened: history is no longer linearizable",
                profile.as_str()
            )));
        }
    }
    println!("regressions: PASS entries={}", entries.len());
    Ok(())
}

fn run_shrink(args: &[String]) -> io::Result<()> {
    let failure = parse_string_flag(args, "--failure");
    let failure_text = failure.as_deref().map(fs::read_to_string).transpose()?;
    let seed = failure_text
        .as_deref()
        .and_then(extract_seed)
        .unwrap_or(Seed::new(0x2a));
    let profile = failure_text
        .as_deref()
        .and_then(extract_profile)
        .unwrap_or(FaultProfile::Rough);
    let spec = failure_text
        .as_deref()
        .map(|text| extract_run_spec(text, seed, profile))
        .unwrap_or_else(|| fixture_spec(seed, profile, true));
    let canonical = cc_sim::canonicalize_fault_plan(&spec.plan);
    let shrunk = shrink_cluster_plan(&RunSpec {
        plan: canonical.clone(),
        ..spec.clone()
    });
    let reproduces = reproduces_failure(&RunSpec {
        plan: shrunk.clone(),
        ..spec
    });
    println!(
        "shrinker: input={} canonical_actions={} shrunk_actions={} reproduces={}",
        failure.as_deref().unwrap_or("(standard fixture)"),
        canonical.actions.len(),
        shrunk.actions.len(),
        reproduces
    );
    if !reproduces {
        return Err(io::Error::other("shrinker lost the reproduce oracle"));
    }
    // Leave a receipt beside the artifact. Nightly's "find of the night" only
    // publishes a failure that carries one, so a raw thousand-action trace can
    // never be presented as a finding.
    if let Some(input) = failure.as_deref() {
        let receipt_path = parse_string_flag(args, "--receipt")
            .unwrap_or_else(|| format!("{}.shrunk.json", input.trim_end_matches(".json")));
        let receipt = format!(
            "{{\"schema_version\":1,\"artifact\":\"{}\",\"seed\":\"{}\",\"profile\":\"{}\",\
             \"canonical_actions\":{},\"shrunk_actions\":{},\"reproduces\":true}}\n",
            input.replace('"', "'"),
            seed,
            profile.as_str(),
            canonical.actions.len(),
            shrunk.actions.len(),
        );
        if let Some(parent) = Path::new(&receipt_path).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&receipt_path, receipt)?;
        println!("shrinker: receipt={receipt_path}");
    }
    Ok(())
}

/// Replay a CCIJ recording against the same Driver boundary used by the real
/// host. The boot image is self-contained for the current WAL-only adapter;
/// a mismatched input/effect pair reports the first ordinal rather than
/// silently accepting a locally reconstructed result.
fn replay_input_journal(args: &[String]) -> io::Result<()> {
    let path = parse_string_flag(args, "--file").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "replay requires --file JOURNAL",
        )
    })?;
    if !args.iter().any(|arg| arg == "--assert-effects") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "replay requires --assert-effects",
        ));
    }
    let journal = InputJournal::decode(&fs::read(&path)?).map_err(io::Error::other)?;
    let termination = match journal.footer.map(|footer| footer.termination) {
        Some(cc_host::journal::JournalTermination::Complete) => "complete",
        Some(cc_host::journal::JournalTermination::Capped) => "capped",
        Some(cc_host::journal::JournalTermination::HostError) => "host-error",
        Some(cc_host::journal::JournalTermination::FatalIo) => "fatal-io",
        None => "interrupted",
    };
    let boot = RecordedBootImage::decode(&journal.boot_image).map_err(io::Error::other)?;
    if boot.build_label != env!("CARGO_PKG_VERSION") {
        eprintln!(
            "replay warning recorded_build={} current_build={}",
            boot.build_label,
            env!("CARGO_PKG_VERSION")
        );
    }
    let replay = replay_journal(&journal).map_err(io::Error::other)?;
    let verdict = if termination == "complete" {
        "effects-match"
    } else {
        "effects-match-prefix"
    };
    println!(
        "replay file={path} records={} termination={termination} verdict={verdict}",
        replay.records
    );
    Ok(())
}

fn check_history(args: &[String]) -> io::Result<()> {
    let path = parse_string_flag(args, "--file").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "check-history requires --file PATH",
        )
    })?;
    let document = read_history(&path)?;
    let verdict = check_document(&document, CheckerConfig::default());
    println!("history file={path} verdict={}", verdict_name(&verdict));
    if matches!(verdict, Verdict::Linearizable { .. }) {
        Ok(())
    } else {
        Err(io::Error::other("real history is not linearizable"))
    }
}

fn export_porcupine(args: &[String]) -> io::Result<()> {
    let path = parse_string_flag(args, "--file").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "export-porcupine requires --file PATH",
        )
    })?;
    let document = read_history(&path)?;
    let json = export_porcupine_json(&document.history);
    if let Some(output) = parse_string_flag(args, "--output") {
        fs::write(&output, json)?;
        println!(
            "porcupine export file={output} operations={}",
            document.history.operations.len()
        );
    } else {
        println!("{json}");
    }
    Ok(())
}

/// Write the binary-safe CC-HISTORY v2 receipt. Existing v1 text files remain
/// readable below for compatibility, but no new run silently drops opens or
/// changes arbitrary bytes into ASCII hex.
fn write_history_tsv(path: &str, history: &History) -> io::Result<usize> {
    let document = HistoryDocument {
        build_label: String::from("dev"),
        config_hash: 0,
        initial: Default::default(),
        retain_open: true,
        history: history.clone(),
    };
    fs::write(path, document.encode())?;
    Ok(history.operations.len())
}

fn read_history(path: &str) -> io::Result<HistoryDocument> {
    let raw = fs::read(path)?;
    if raw.starts_with(b"CCHY") {
        return HistoryDocument::decode(&raw).map_err(io::Error::other);
    }
    let text = String::from_utf8(raw).map_err(io::Error::other)?;
    let history = decode_history_v1_tsv(&text).map_err(io::Error::other)?;
    Ok(HistoryDocument {
        build_label: String::from("legacy-v1"),
        config_hash: 0,
        initial: Default::default(),
        retain_open: true,
        history,
    })
}

fn fixture_spec(seed: Seed, profile: FaultProfile, campaign: bool) -> RunSpec {
    let end_time = if campaign {
        Time::from_nanos(3_000_000_000)
    } else {
        Time::from_nanos(6_000_000_000)
    };
    let node_count = 5;
    let nodes: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let mut spec = RunSpec::standard(seed, profile);
    spec.config.end_time = end_time;
    spec.end_time = end_time;
    spec.plan = materialize_fault_plan(seed, profile, &nodes, end_time);
    spec.workload = WorkloadSpec {
        clients: 2,
        ops_per_second: 10,
        keyspace: 16,
        set_ttl: matches!(profile, FaultProfile::Ttl).then_some(Duration::from_millis(125)),
        kind: profile.workload_kind(),
    };
    spec
}

fn print_run_summary(seed: Seed, profile: FaultProfile, run: &ClusterRun, command: &str) {
    println!(
        "{} seed={} profile={} events={} history={} completed={} healthy={} verdict={}",
        command,
        seed,
        profile.as_str(),
        run.event_count,
        run.history.operations.len(),
        run.completed_operations,
        run.healthy(),
        verdict_name(&run.verdict)
    );
    for violation in &run.invariant_violations {
        println!("invariant violation: {violation}");
    }
}

fn extract_seed(text: &str) -> Option<Seed> {
    text.split("\"seed\":\"")
        .nth(1)
        .and_then(|value| value.split('"').next())
        .and_then(parse_seed_text)
}

fn extract_profile(text: &str) -> Option<FaultProfile> {
    text.split("\"profile\":\"")
        .nth(1)
        .and_then(|value| value.split('"').next())
        .and_then(FaultProfile::parse)
}

fn parse_seed_text(value: &str) -> Option<Seed> {
    let value = value.trim_start_matches("0x");
    u64::from_str_radix(value, 16).ok().map(Seed::new)
}

fn extract_run_spec(text: &str, seed: Seed, profile: FaultProfile) -> RunSpec {
    let mut spec = fixture_spec(seed, profile, true);
    if let Some(node_count) = extract_number(text, "\"node_count\":") {
        spec.config.node_count = node_count;
    }
    if let Some(end_time) = extract_number(text, "\"end_time_ns\":") {
        spec.config.end_time = Time::from_nanos(end_time);
        spec.end_time = spec.config.end_time;
    }
    if let Some(clients) = extract_number(text, "\"clients\":") {
        spec.workload.clients = clients;
    }
    if let Some(ops_per_second) = extract_number(text, "\"ops_per_second\":") {
        spec.workload.ops_per_second = ops_per_second;
    }
    if let Some(keyspace) = extract_number(text, "\"keyspace\":") {
        spec.workload.keyspace = keyspace;
    }
    spec.workload.set_ttl = extract_number(text, "\"set_ttl_ns\":").map(Duration::from_nanos);
    if let Some(faults) = extract_faults(text) {
        spec.plan = faults;
    }
    spec
}

fn extract_faults(text: &str) -> Option<FaultPlan> {
    let mut plan = FaultPlan::default();
    let faults = text.split("\"faults\":[").nth(1)?;
    for fragment in faults.split("\"at_ns\":").skip(1) {
        let at = extract_number(fragment, "").unwrap_or(0);
        let action_text = fragment.split("\"action\":{").nth(1)?;
        let kind = extract_string(action_text, "\"kind\":\"")?;
        let action = match kind.as_str() {
            "partition" => FaultAction::Partition {
                left: extract_node_array(action_text, "\"left\":[")?,
                right: extract_node_array(action_text, "\"right\":[")?,
            },
            "heal" => FaultAction::Heal,
            "crash" => FaultAction::Crash {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
            },
            "restart" => FaultAction::Restart {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
            },
            "wipe" => FaultAction::Wipe {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
            },
            "clock-skew" => FaultAction::ClockSkew {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
                offset: Duration::from_nanos(extract_number(action_text, "\"offset_ns\":")?),
            },
            "disk-degrade" => FaultAction::DiskDegrade {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
                write_latency: Duration::from_nanos(extract_number(
                    action_text,
                    "\"write_latency_ns\":",
                )?),
            },
            "slow-disk" => FaultAction::SlowDisk {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
                slow: SlowDisk {
                    read_extra: Duration::from_nanos(extract_number(
                        action_text,
                        "\"read_extra_ns\":",
                    )?),
                    write_extra: Duration::from_nanos(extract_number(
                        action_text,
                        "\"write_extra_ns\":",
                    )?),
                    fsync_extra: Duration::from_nanos(extract_number(
                        action_text,
                        "\"fsync_extra_ns\":",
                    )?),
                    rename_extra: Duration::from_nanos(extract_number(
                        action_text,
                        "\"rename_extra_ns\":",
                    )?),
                    dirsync_extra: Duration::from_nanos(extract_number(
                        action_text,
                        "\"dirsync_extra_ns\":",
                    )?),
                },
            },
            "enospc-from" => FaultAction::EnospcFrom {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
            },
            "bitrot-at-rest" => FaultAction::BitRotAtRest {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
                file: extract_file_id(action_text)?,
                offset: extract_number(action_text, "\"offset\":")?,
            },
            "disk-quota" => FaultAction::DiskQuota {
                node: NodeId::new(extract_number(action_text, "\"node\":")?),
                bytes: extract_number(action_text, "\"bytes\":")?,
            },
            "link-degrade" => FaultAction::LinkDegrade {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                config: LinkConfig::default(),
            },
            "corrupt-frame" => FaultAction::CorruptFrame {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                nth: extract_number(action_text, "\"nth\":")?,
                byte: usize::try_from(extract_number(action_text, "\"byte\":")?).ok()?,
                bit: u8::try_from(extract_number(action_text, "\"bit\":")?).ok()?,
            },
            "truncate-frame" => FaultAction::TruncateFrame {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                nth: extract_number(action_text, "\"nth\":")?,
                keep: usize::try_from(extract_number(action_text, "\"keep\":")?).ok()?,
            },
            "replay-frame" => FaultAction::ReplayFrame {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                nth: extract_number(action_text, "\"nth\":")?,
                at: Time::from_nanos(extract_number(action_text, "\"at_ns\":")?),
            },
            "delay-link" => FaultAction::DelayLink {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                extra: Duration::from_nanos(extract_number(action_text, "\"extra_ns\":")?),
            },
            "mutate-raft-and-rechecksum" => FaultAction::MutateRaftAndRechecksum {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                nth: extract_number(action_text, "\"nth\":")?,
                mutation: extract_ccrp_mutation(action_text)?,
            },
            "reconfigure" => FaultAction::Reconfigure {
                voters: extract_node_array(action_text, "\"voters\":[")?,
            },
            _ => continue,
        };
        plan.push(FaultAt {
            at: Time::from_nanos(at),
            action,
        });
    }
    Some(plan)
}

fn extract_ccrp_mutation(text: &str) -> Option<CcrpMutation> {
    let value = extract_string(text, "\"mutation\":\"")?;
    let (kind, number) = value.split_once(':')?;
    let number = number.parse::<u64>().ok()?;
    match kind {
        "message-tag" => u8::try_from(number).ok().map(CcrpMutation::MessageTag),
        "append-entry-count" => u32::try_from(number)
            .ok()
            .map(CcrpMutation::AppendEntryCount),
        "entry-payload-length" => u32::try_from(number)
            .ok()
            .map(CcrpMutation::EntryPayloadLength),
        "option-flag" => u8::try_from(number).ok().map(CcrpMutation::OptionFlag),
        "from-node-id" => Some(CcrpMutation::FromNodeId(number)),
        "truncate" => usize::try_from(number).ok().map(CcrpMutation::Truncate),
        _ => None,
    }
}

fn extract_file_id(text: &str) -> Option<FileId> {
    let number = extract_number(text, "\"file_no\":")?;
    match extract_string(text, "\"file_kind\":\"")?.as_str() {
        "wal" => Some(FileId::Wal { segment: number }),
        "sst" => Some(FileId::Sst { file_no: number }),
        "manifest" => Some(FileId::Manifest { generation: number }),
        "snapshot" => Some(FileId::Snapshot { generation: number }),
        "meta" if number == 0 => Some(FileId::Meta),
        "temp" => Some(FileId::Temp { sequence: number }),
        _ => None,
    }
}

fn extract_node_array(text: &str, marker: &str) -> Option<Vec<NodeId>> {
    let body = text.split(marker).nth(1)?.split(']').next()?;
    body.split(',')
        .filter(|value| !value.is_empty())
        .map(|value| value.parse().ok().map(NodeId::new))
        .collect::<Option<Vec<_>>>()
}

fn extract_number(text: &str, marker: &str) -> Option<u64> {
    let value = if marker.is_empty() {
        text
    } else {
        text.split(marker).nth(1)?
    };
    value
        .trim_start_matches(|character: char| !character.is_ascii_digit())
        .split(|character: char| !character.is_ascii_digit())
        .next()
        .and_then(|number| number.parse().ok())
}

fn extract_string(text: &str, marker: &str) -> Option<String> {
    text.split(marker)
        .nth(1)
        .and_then(|value| value.split('"').next())
        .map(str::to_owned)
}

fn verdict_name(verdict: &Verdict) -> &'static str {
    match verdict {
        Verdict::Linearizable { .. } => "ok",
        Verdict::NotLinearizable { .. } => "failed",
        Verdict::Undecided { .. } => "undecided",
    }
}

// A malformed value is an error, never a silent fall back to the default. The
// whole harness sells reproducibility: `--seed 0xdeadbeef` typed as `--seed
// 0xdeadbeff` must not quietly run seed 0 and report a green verdict, and
// `--profile brutal` typed as `--profile brutl` must not quietly run `calm`.
// A flag that is absent still takes its default; only a flag that is present
// and unreadable stops the run.

fn parse_profile(args: &[String], default: FaultProfile) -> io::Result<FaultProfile> {
    let Some(value) = parse_string_flag(args, "--profile") else {
        return Ok(default);
    };
    FaultProfile::parse(&value).ok_or_else(|| {
        let names: Vec<&str> = FaultProfile::ALL.iter().map(|p| p.as_str()).collect();
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unknown --profile {value}; expected one of {}",
                names.join(", ")
            ),
        )
    })
}

fn parse_seed(args: &[String], default: u64) -> io::Result<Seed> {
    let Some(value) = parse_string_flag(args, "--seed") else {
        return Ok(Seed::new(default));
    };
    parse_seed_text(&value).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--seed {value} is not a hexadecimal u64"),
        )
    })
}

fn parse_u64(args: &[String], position: usize, default: u64) -> io::Result<u64> {
    let Some(value) = args.get(position) else {
        return Ok(default);
    };
    value.parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("expected an unsigned integer, got {value}"),
        )
    })
}

fn parse_u64_flag(args: &[String], flag: &str) -> io::Result<Option<u64>> {
    let Some(value) = parse_string_flag(args, flag) else {
        return Ok(None);
    };
    value.parse().map(Some).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} requires an unsigned integer, got {value}"),
        )
    })
}

fn parse_usize_flag(args: &[String], flag: &str, default: usize) -> io::Result<usize> {
    let Some(value) = parse_u64_flag(args, flag)? else {
        return Ok(default);
    };
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} value {value} does not fit this platform's usize"),
        )
    })
}

fn load_disk_profile(args: &[String]) -> io::Result<Option<(String, DiskModel)>> {
    let Some(name) = parse_string_flag(args, "--disk-profile") else {
        return Ok(None);
    };
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--disk-profile must be an ASCII name",
        ));
    }
    let relative = Path::new("sim/profiles/calibrated").join(format!("{name}.toml"));
    let path = if relative.exists() {
        relative
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(relative)
    };
    let text = fs::read_to_string(&path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot read calibrated profile {}: {error}", path.display()),
        )
    })?;
    if profile_scalar(&text, "schema") != Some("1")
        || profile_scalar(&text, "name") != Some(name.as_str())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "calibrated profile schema/name mismatch",
        ));
    }
    let model = DiskModel {
        read: profile_distribution(&text, "read_buckets_ns")?,
        write: profile_distribution(&text, "write_buckets_ns")?,
        fsync: profile_distribution(&text, "fsync_buckets_ns")?,
        rename: profile_distribution(&text, "rename_buckets_ns")?,
        dirsync: profile_distribution(&text, "dirsync_buckets_ns")?,
    };
    Ok(Some((name, model)))
}

fn profile_scalar<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines().map(str::trim).find_map(|line| {
        let (candidate, value) = line.split_once('=')?;
        (candidate.trim() == key).then(|| value.trim().trim_matches('"'))
    })
}

fn profile_distribution(text: &str, key: &str) -> io::Result<cc_core::DelayDist> {
    let value = profile_scalar(text, key)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "profile lacks disk buckets"))?;
    let value = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid disk bucket array"))?;
    let parsed = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.parse::<u64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid disk bucket"))?;
    if parsed.is_empty() || parsed.len() > 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "disk bucket count must be 1..=16",
        ));
    }
    let mut buckets = [Duration::from_nanos(0); 16];
    for (slot, value) in buckets.iter_mut().zip(parsed.iter()) {
        *slot = Duration::from_nanos(*value);
    }
    Ok(cc_core::DelayDist::Empirical {
        buckets,
        count: u8::try_from(parsed.len()).expect("bounded bucket count"),
    })
}

fn apply_loaded_disk_profile(spec: &mut RunSpec, profile: Option<&(String, DiskModel)>) {
    if let Some((name, model)) = profile {
        spec.disk_profile = Some(name.clone());
        spec.config.disk_model = *model;
    }
}

fn parse_string_flag(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|value| value == flag)
}

/// Escape one JSON string body. Receipts carry operator-supplied build labels
/// and paths and formatted error text, so every control character has to be
/// escaped, not just the three with short forms — one stray control byte in an
/// error message would otherwise make a receipt unparseable.
fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if control.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => escaped.push(other),
        }
    }
    escaped
}

fn print_help() {
    println!(concat!(
        "cc-swarm ",
        env!("CARGO_PKG_VERSION"),
        "\n\nCommands:\n  run --profile rough --seeds N --jobs N [--disk-profile NAME] [--shard i/N] [--ledger PATH] [--build LABEL] [--resume] [--require-beacon NAME]\n  capability-campaign --seeds N\n  one --seed 0x... --profile rough [--disk-profile NAME] [--export-json] [--export-history PATH]\n  ledger stats <ledger.tsv>\n  ledger merge --out <path> <ledger.tsv>...\n  model-check [--max-log N] [--max-term N] [--max-messages N] [--max-depth N] [--max-states N]\n  search --profile rough --iterations N\n  regress\n  shrink --failure PATH\n  diff <artifact-a.json> <artifact-b.json>\n  sequence <artifact.json> [--output diagram.svg]\n  explain --failure <artifact.json> [--svg diagram.svg]\n  trace <trace.cctr|trace.json> [--node ID] [--kind K[,K]] [--since D] [--until D] [--grep HEX_OR_TEXT] [--tail N] [--stats]\n  proxy [--listen ADDR] [--upstream ADDR] [--drop-every N] [--delay-ms N]\n  check-history --file PATH\n  replay --file JOURNAL --assert-effects\n  export-porcupine --file PATH [--output PATH]\n  --selfcheck\n  --determinism\n  --determinism-seeds N"
    ));
    println!("\nReachability beacons (for --require-beacon):\n  {REACHABILITY_BEACONS_HELP}");
    println!(
        "A beacon names a state the campaign is expected to reach. --require-beacon\n\
         exits non-zero if it never fired, so a code path that goes dark fails the\n\
         gate instead of passing quietly."
    );
}

#[cfg(test)]
mod trace_tests {
    use super::*;

    #[test]
    fn trap_kata_ledger_cannot_generate_production_summary() {
        let ledger = SeedLedger::kata();
        let error = coverage_summary(
            Path::new("not-read-for-kata-summary.tsv"),
            &ledger,
            "build",
            false,
            0,
        )
        .expect_err("synthetic summary must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("synthetic kata ledgers"));
    }

    #[test]
    fn trap_named_disk_profile_is_canonical_and_does_not_change_defaults() {
        let args = vec![
            String::from("--disk-profile"),
            String::from("reference-local"),
        ];
        let loaded = load_disk_profile(&args)
            .expect("profile")
            .expect("named profile");
        let defaults = RunSpec::standard(Seed::new(7), FaultProfile::Calm);
        let mut first = defaults.clone();
        let mut second = defaults.clone();
        apply_loaded_disk_profile(&mut first, Some(&loaded));
        apply_loaded_disk_profile(&mut second, Some(&loaded));
        assert_eq!(
            canonical_run_spec_json(&first),
            canonical_run_spec_json(&second)
        );
        assert_eq!(first.disk_profile.as_deref(), Some("reference-local"));
        assert_eq!(defaults.disk_profile, None);
        assert_eq!(defaults.config.disk_model, DiskModel::universal());
        assert_ne!(
            canonical_run_spec_json(&first),
            canonical_run_spec_json(&defaults)
        );
    }

    fn binary_trace_with_tag(tag: u8) -> Vec<u8> {
        let mut enc = cc_core::Enc::new();
        enc.header(cc_core::TRACE_MAGIC, cc_core::TRACE_VERSION);
        enc.u64(7);
        enc.u32(9);
        enc.string("future");
        enc.u32(1);
        enc.u64(0);
        enc.u64(3);
        enc.u8(0);
        enc.u8(tag);
        enc.bytes(b"future-payload");
        enc.finish()
    }

    #[test]
    fn trap_trace_reads_current_binary_and_json_exports() {
        let mut trace = Trace::new(Seed::new(7), 9);
        trace.push(
            Time::from_nanos(3),
            Some(NodeId::new(2)),
            EventKind::Fault,
            b"fault\x00payload".to_vec(),
        );
        assert_eq!(Trace::decode(&trace.encode()).expect("binary trace"), trace);
        assert_eq!(
            decode_trace_json(&trace.to_json()).expect("JSON trace"),
            trace
        );
        assert_eq!(
            trace_payload(b"fault\x00payload"),
            "0x6661756c74007061796c6f6164"
        );
    }

    #[test]
    fn trap_trace_reads_legacy_cctr_fixture() {
        let trace = Trace::decode(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/legacy/cctr-v1.bin"
        )))
        .expect("legacy CCTR fixture");
        assert_eq!(trace.seed, Seed::new(0xcc));
        assert!(!trace.events.is_empty(), "legacy trace must retain events");
    }

    #[test]
    fn trap_trace_unknown_event_is_skippable() {
        let events = decode_trace_display_events(&binary_trace_with_tag(0xfe))
            .expect("unknown registry code is still framed and skippable");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "unknown-254");
        assert_eq!(events[0].payload, b"future-payload");
    }

    #[test]
    fn trap_trace_rejects_malformed_length_at_offset() {
        let mut bytes = binary_trace_with_tag(EventKind::Fault as u8);
        let length_offset = bytes.len() - b"future-payload".len() - 4;
        bytes[length_offset..length_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let error = decode_trace_display_events(&bytes).expect_err("declared payload overflow");
        let message = error.to_string();
        assert!(message.contains(&format!("byte offset {length_offset}")));
        assert!(message.contains("event payload length"));
    }

    #[test]
    fn slow_disk_fault_spec_preserves_every_service_delay() {
        let text = "{\"faults\":[{\"at_ns\":7,\"action\":{\"kind\":\"slow-disk\",\"node\":3,\"read_extra_ns\":1,\"write_extra_ns\":2,\"fsync_extra_ns\":3,\"rename_extra_ns\":4,\"dirsync_extra_ns\":5}}]}";
        let plan = extract_faults(text).expect("fault plan");
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(
            plan.actions[0],
            FaultAt {
                at,
                action: FaultAction::SlowDisk { node, slow }
            } if at == Time::from_nanos(7)
                && node == NodeId::new(3)
                && slow.read_extra == Duration::from_nanos(1)
                && slow.write_extra == Duration::from_nanos(2)
                && slow.fsync_extra == Duration::from_nanos(3)
                && slow.rename_extra == Duration::from_nanos(4)
                && slow.dirsync_extra == Duration::from_nanos(5)
        ));
    }

    #[test]
    fn persistent_disk_fault_specs_round_trip_all_selected_fields() {
        let text = "{\"faults\":[{\"at_ns\":7,\"action\":{\"kind\":\"bitrot-at-rest\",\"node\":3,\"file_kind\":\"snapshot\",\"file_no\":9,\"offset\":12}},{\"at_ns\":8,\"action\":{\"kind\":\"disk-quota\",\"node\":4,\"bytes\":4096}},{\"at_ns\":9,\"action\":{\"kind\":\"enospc-from\",\"node\":5}}]}";
        let plan = extract_faults(text).expect("fault plan");
        assert_eq!(plan.actions.len(), 3);
        assert!(matches!(
            plan.actions[0],
            FaultAt {
                at,
                action: FaultAction::BitRotAtRest { node, file, offset }
            } if at == Time::from_nanos(7)
                && node == NodeId::new(3)
                && file == FileId::Snapshot { generation: 9 }
                && offset == 12
        ));
        assert!(matches!(
            plan.actions[1],
            FaultAt {
                at,
                action: FaultAction::DiskQuota { node, bytes }
            } if at == Time::from_nanos(8) && node == NodeId::new(4) && bytes == 4096
        ));
        assert!(matches!(
            plan.actions[2],
            FaultAt {
                at,
                action: FaultAction::EnospcFrom { node }
            } if at == Time::from_nanos(9) && node == NodeId::new(5)
        ));
    }

    #[test]
    fn trap_replay_starts_from_captured_boot_image() {
        let membership = cc_core::MembershipState::new([NodeId::new(1)].into_iter().collect())
            .expect("membership");
        let wal = cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(
            cc_log::Genesis {
                origin: cc_log::Origin::Bootstrap,
                cluster_id: [7; 16],
                policy: ClusterPolicy::default(),
                membership,
            },
        )))
        .expect("genesis");
        let image = RecordedBootImage {
            config: NodeConfig {
                id: NodeId::new(1),
                cluster_id: [7; 16],
                seed: Seed::new(1),
                raft: RaftConfig::default(),
                store: StoreConfig::default(),
                policy: ClusterPolicy::default(),
                host_limits: HostLimits::default(),
            },
            cluster_id: [7; 16],
            membership: cc_core::MembershipState::new([NodeId::new(1)].into_iter().collect())
                .expect("membership"),
            boot_epoch: Time::from_nanos(0),
            build_label: String::from("test"),
            wal,
        }
        .encode()
        .expect("boot image");
        let mut journal = InputJournal::new(image);
        journal
            .push(JournalRecord {
                ordinal: 1,
                now: Time::from_nanos(0),
                input: cc_env::Input::Tick,
                block_observations: Vec::new(),
                effects: Vec::new(),
            })
            .expect("record");
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("cc-swarm-replay-{unique}.ccij"));
        fs::write(&path, journal.encode().expect("journal encoding")).expect("journal file");
        replay_input_journal(&[
            String::from("--file"),
            path.display().to_string(),
            String::from("--assert-effects"),
        ])
        .expect("replay");
        fs::remove_file(path).expect("remove journal");
    }

    #[test]
    fn trap_replay_rejects_boot_metadata_mismatch() {
        let membership = cc_core::MembershipState::new([NodeId::new(1)].into_iter().collect())
            .expect("membership");
        let wal = cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(
            cc_log::Genesis {
                origin: cc_log::Origin::Bootstrap,
                cluster_id: [7; 16],
                policy: ClusterPolicy::default(),
                membership: membership.clone(),
            },
        )))
        .expect("genesis");
        let image = RecordedBootImage {
            config: NodeConfig {
                id: NodeId::new(1),
                cluster_id: [8; 16],
                seed: Seed::new(1),
                raft: RaftConfig::default(),
                store: StoreConfig::default(),
                policy: ClusterPolicy::default(),
                host_limits: HostLimits::default(),
            },
            cluster_id: [8; 16],
            membership,
            boot_epoch: Time::from_nanos(0),
            build_label: String::from("test"),
            wal,
        }
        .encode()
        .expect("mismatched boot image");
        let journal = InputJournal::new(image);
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("cc-swarm-mismatch-{unique}.ccij"));
        fs::write(&path, journal.encode().expect("journal encoding")).expect("journal file");
        let error = replay_input_journal(&[
            String::from("--file"),
            path.display().to_string(),
            String::from("--assert-effects"),
        ])
        .expect_err("metadata mismatch must fail before driver boot");
        assert!(error.to_string().contains("metadata disagrees"));
        fs::remove_file(path).expect("remove journal");
    }

    #[test]
    fn trap_unreadable_flag_values_stop_the_run_instead_of_defaulting() {
        let arg = |value: &str| String::from(value);

        // A misspelt profile must not silently select the default. Reporting
        // `profile=calm verdict=ok` for a run the operator asked to be
        // `brutal` would turn a typo into a false claim.
        let args = [arg("--profile"), arg("brutl")];
        let error = parse_profile(&args, FaultProfile::Calm).expect_err("typo must not default");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("brutl"));
        assert!(
            error.to_string().contains("brutal"),
            "names the alternatives"
        );

        // A seed is the whole reproduction handle; a value that does not parse
        // must never become seed 0.
        let args = [arg("--seed"), arg("nothex")];
        let error = parse_seed(&args, 0).expect_err("typo must not default");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        // A model-check bound that fails to parse would silently shrink the
        // explored state space and still print PASS.
        let args = [arg("--max-log"), arg("abc")];
        let error = parse_u64_flag(&args, "--max-log").expect_err("typo must not default");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let error = parse_usize_flag(&args, "--max-log", 4).expect_err("typo must not default");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let args = [arg("run"), arg("nine")];
        let error = parse_u64(&args, 1, 1_000).expect_err("typo must not default");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        // An absent flag still takes its default.
        let empty: [String; 0] = [];
        assert_eq!(
            parse_profile(&empty, FaultProfile::Rough).expect("absent flag"),
            FaultProfile::Rough
        );
        assert_eq!(parse_seed(&empty, 7).expect("absent flag"), Seed::new(7));
        assert_eq!(
            parse_u64_flag(&empty, "--max-log").expect("absent flag"),
            None
        );
        assert_eq!(
            parse_usize_flag(&empty, "--max-log", 4).expect("absent flag"),
            4
        );
        assert_eq!(parse_u64(&empty, 1, 1_000).expect("absent flag"), 1_000);
    }
}
