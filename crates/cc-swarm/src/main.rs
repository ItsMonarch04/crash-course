// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use cc_checker::{
    CheckerConfig, History, Operation, OperationKind, Outcome, Verdict, check,
    export_porcupine_json,
};
use cc_core::{Duration, NodeId, Seed, Time};
use cc_sim::{
    FaultAction, FaultAt, FaultPlan, FaultProfile, LinkConfig, RecorderLevel, RunSpec,
    WorkloadSpec, materialize_fault_plan,
};

use cc_swarm::{
    ClusterRun, DETERMINISM_PROFILES, REACHABILITY_BEACONS, deterministic_cluster_trace,
    deterministic_cluster_trace_for, mutate_fault_plan, reachability_beacons, reproduces_failure,
    run_spec, semantic_trace_diff, sequence_diagram_svg, shrink_cluster_plan, trace_coverage,
};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--determinism") => emit_determinism_trace(),
        Some("--determinism-seeds") => run_determinism_seeds(parse_u64(&args, 1, 1_000)),
        Some("--selfcheck") => {
            selfcheck_cluster(Seed::new(0xcc)).map_err(io::Error::other)?;
            println!("selfcheck: PASS");
            Ok(())
        }
        Some("one") => run_one(&args[1..]),
        Some("run") => run_campaign(&args[1..]),
        Some("regress") => run_regressions(),
        Some("shrink") => run_shrink(&args[1..]),
        Some("check-history") => check_history(&args[1..]),
        Some("export-porcupine") => export_porcupine(&args[1..]),
        Some("diff") => run_diff(&args[1..]),
        Some("sequence") => run_sequence(&args[1..]),
        Some("proxy") => run_proxy(&args[1..]),
        Some("search") => run_coverage_search(&args[1..]),
        Some("model-check") => run_model_check(&args[1..]),
        _ => {
            print_help();
            Ok(())
        }
    }
}

fn run_model_check(args: &[String]) -> io::Result<()> {
    let config = cc_raft::model::ModelConfig {
        max_log: usize::try_from(parse_u64_flag(args, "--max-log").unwrap_or(4))
            .unwrap_or(usize::MAX),
        max_term: parse_u64_flag(args, "--max-term").unwrap_or(3),
        max_messages: usize::try_from(parse_u64_flag(args, "--max-messages").unwrap_or(8))
            .unwrap_or(usize::MAX),
        max_depth: usize::try_from(parse_u64_flag(args, "--max-depth").unwrap_or(16))
            .unwrap_or(usize::MAX),
        max_states: usize::try_from(parse_u64_flag(args, "--max-states").unwrap_or(2_000_000))
            .unwrap_or(usize::MAX),
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

fn run_coverage_search(args: &[String]) -> io::Result<()> {
    let iterations = parse_u64_flag(args, "--iterations").unwrap_or(100);
    let profile = parse_profile(args, FaultProfile::Rough);
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
    Ok(())
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
    let drop_every = parse_u64_flag(args, "--drop-every").unwrap_or(0);
    let delay_ms = parse_u64_flag(args, "--delay-ms").unwrap_or(0);
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
    let seed = parse_seed(args, 0);
    let profile = parse_profile(args, FaultProfile::Calm);
    let spec = fixture_spec(seed, profile, false);
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
    let seeds = parse_u64_flag(args, "--seeds").unwrap_or(1);
    let profile = parse_profile(args, FaultProfile::Rough);
    let jobs = parse_u64_flag(args, "--jobs").unwrap_or(1).max(1);
    let worker_count = jobs.min(seeds.max(1)) as usize;
    let next_seed = Arc::new(AtomicU64::new(0));
    let export_json = has_flag(args, "--export-json");
    fs::create_dir_all("artifacts")?;
    let started = Instant::now();
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let next_seed = Arc::clone(&next_seed);
        workers.push(thread::spawn(move || -> io::Result<(
            u64,
            u64,
            u64,
            [u64; REACHABILITY_BEACONS.len()],
        )> {
            let mut failures = 0_u64;
            let mut history_failures = 0_u64;
            let mut events = 0_u64;
            let mut beacon_hits = [0_u64; REACHABILITY_BEACONS.len()];
            loop {
                let seed = next_seed.fetch_add(1, Ordering::Relaxed);
                if seed >= seeds {
                    break;
                }
                let result = run_spec(
                    fixture_spec(Seed::new(seed), profile, true),
                    RecorderLevel::Campaign,
                )
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
                        if failed {
                            failures = failures.saturating_add(1);
                        }
                        if history_failed {
                            history_failures = history_failures.saturating_add(1);
                        }
                        if failed || history_failed || export_json {
                            fs::write(format!("artifacts/{seed}.json"), run.artifact_json(profile))?;
                        }
                    }
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        fs::write(
                            format!("artifacts/{seed}.json"),
                            format!(
                                "{{\"fixture_version\":1,\"seed\":\"{seed}\",\"profile\":\"{}\",\"error\":\"{}\"}}",
                                profile.as_str(),
                                json_escape(&error)
                            ),
                        )?;
                    }
                }
            }
            Ok((failures, history_failures, events, beacon_hits))
        }));
    }
    let mut failures = 0_u64;
    let mut history_failures = 0_u64;
    let mut events = 0_u64;
    let mut beacon_hits = [0_u64; REACHABILITY_BEACONS.len()];
    for worker in workers {
        let (worker_failures, worker_history_failures, worker_events, worker_beacons) = worker
            .join()
            .map_err(|_| io::Error::other("campaign worker panicked"))??;
        failures = failures.saturating_add(worker_failures);
        history_failures = history_failures.saturating_add(worker_history_failures);
        events = events.saturating_add(worker_events);
        for (total, hits) in beacon_hits.iter_mut().zip(worker_beacons) {
            *total = total.saturating_add(hits);
        }
    }
    let elapsed = started.elapsed().as_secs_f64().max(f64::EPSILON);
    println!(
        "campaign profile={} seeds={} jobs={} failures={} history_failures={} events={} runs_per_sec={:.2}",
        profile.as_str(),
        seeds,
        jobs,
        failures,
        history_failures,
        events,
        f64::from(seeds as u32) / elapsed
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
    if reproduces {
        Ok(())
    } else {
        Err(io::Error::other("shrinker lost the reproduce oracle"))
    }
}

fn check_history(args: &[String]) -> io::Result<()> {
    let path = parse_string_flag(args, "--file").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "check-history requires --file PATH",
        )
    })?;
    let history = read_history(&path)?;
    let verdict = check(&history, CheckerConfig::default());
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
    let history = read_history(&path)?;
    let json = export_porcupine_json(&history);
    if let Some(output) = parse_string_flag(args, "--output") {
        fs::write(&output, json)?;
        println!(
            "porcupine export file={output} operations={}",
            history.operations.len()
        );
    } else {
        println!("{json}");
    }
    Ok(())
}

/// Render a produced history in the same TSV shape `read_history` parses, so a
/// real campaign history can feed `check-history` and the Porcupine export
/// instead of a hand-written toy.
fn write_history_tsv(path: &str, history: &History) -> io::Result<usize> {
    let mut out = String::from("# CC-HISTORY v1: KIND KEY OBSERVED_VALUE INVOKE_NS COMPLETE_NS\n");
    let mut written = 0_usize;
    for operation in &history.operations {
        // Only completed operations have an observed value; an ambiguous
        // timeout is not representable in this shape and is skipped.
        let Some(complete) = operation.complete else {
            continue;
        };
        let (kind, key) = match &operation.kind {
            OperationKind::Set { key, .. } => ("SET", key.clone()),
            OperationKind::Get { key } => ("GET", key.clone()),
            OperationKind::Del { key } => ("DEL", key.clone()),
            OperationKind::Incr { key } => ("INCR", key.clone()),
            OperationKind::Cas { key, .. } => ("CAS", key.clone()),
            OperationKind::Scan { .. } => continue,
        };
        let value = match (&operation.outcome, kind) {
            (Outcome::Value(Some(value)), _) => hex_value(value),
            (Outcome::Value(None), _) => String::from("-"),
            (Outcome::Integer(value), _) => value.to_string(),
            (Outcome::Ok, "SET") => match &operation.kind {
                OperationKind::Set { value, .. } => hex_value(value),
                _ => String::from("-"),
            },
            (Outcome::Ok, _) => String::from("-"),
            _ => continue,
        };
        out.push_str(&format!(
            "{kind}\t{}\t{value}\t{}\t{}\n",
            hex_value(&key),
            operation.invoke.as_nanos(),
            complete.as_nanos()
        ));
        written += 1;
    }
    fs::write(path, out)?;
    Ok(written)
}

/// Keys and values are arbitrary bytes; the TSV shape is tab-delimited text, so
/// render them as hex to keep the format unambiguous.
fn hex_value(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::from("-");
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn read_history(path: &str) -> io::Result<History> {
    let text = fs::read_to_string(path)?;
    let mut history = History::default();
    for (line_number, line) in text.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let operation = parse_history_operation(&fields).map_err(|reason| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("history line {}: {reason}", line_number + 1),
            )
        })?;
        history.push(operation);
    }
    Ok(history)
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
}

fn parse_history_operation(fields: &[&str]) -> Result<Operation, &'static str> {
    if fields.len() != 5 {
        return Err("expected KIND<TAB>KEY<TAB>VALUE<TAB>INVOKE<TAB>COMPLETE");
    }
    let invoke = fields[3].parse().map_err(|_| "invalid invoke time")?;
    let complete = fields[4].parse().map_err(|_| "invalid complete time")?;
    let key = fields[1].as_bytes().to_vec();
    let value = fields[2].as_bytes().to_vec();
    let kind = match fields[0] {
        "SET" => OperationKind::Set {
            key,
            value: value.clone(),
        },
        "GET" => OperationKind::Get { key },
        "DEL" => OperationKind::Del { key },
        "INCR" => OperationKind::Incr { key },
        "CAS" => OperationKind::Cas {
            key,
            expected: if fields[2] == "-" {
                None
            } else {
                Some(value.clone())
            },
            value: b"cas".to_vec(),
        },
        _ => return Err("unknown operation kind"),
    };
    let outcome = match fields[0] {
        "SET" | "DEL" => Outcome::Ok,
        "GET" => {
            if fields[2] == "-" {
                Outcome::Value(None)
            } else {
                Outcome::Value(Some(value))
            }
        }
        "INCR" => Outcome::Integer(fields[2].parse().map_err(|_| "invalid INCR result")?),
        "CAS" => Outcome::Cas(fields[2] != "-"),
        _ => return Err("unknown operation kind"),
    };
    Ok(Operation::completed(
        complete,
        kind,
        Time::from_nanos(invoke),
        Time::from_nanos(complete),
        outcome,
    ))
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
            "link-degrade" => FaultAction::LinkDegrade {
                from: NodeId::new(extract_number(action_text, "\"from\":")?),
                to: NodeId::new(extract_number(action_text, "\"to\":")?),
                config: LinkConfig::default(),
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

fn extract_node_array(text: &str, marker: &str) -> Option<Vec<NodeId>> {
    let body = text.split(marker).nth(1)?.split(']').next()?;
    Some(
        body.split(',')
            .filter(|value| !value.is_empty())
            .map(|value| value.parse().ok().map(NodeId::new))
            .collect::<Option<Vec<_>>>()?,
    )
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

fn parse_profile(args: &[String], default: FaultProfile) -> FaultProfile {
    parse_string_flag(args, "--profile")
        .as_deref()
        .and_then(FaultProfile::parse)
        .unwrap_or(default)
}

fn parse_seed(args: &[String], default: u64) -> Seed {
    parse_string_flag(args, "--seed")
        .and_then(|value| parse_seed_text(&value))
        .unwrap_or_else(|| Seed::new(default))
}

fn parse_u64(args: &[String], position: usize, default: u64) -> u64 {
    args.get(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn parse_u64_flag(args: &[String], flag: &str) -> Option<u64> {
    parse_string_flag(args, flag).and_then(|value| value.parse().ok())
}

fn parse_string_flag(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].clone())
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|value| value == flag)
}

fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '"' => ['\\', '"'].into_iter().collect::<Vec<_>>(),
            '\\' => ['\\', '\\'].into_iter().collect::<Vec<_>>(),
            '\n' => ['\\', 'n'].into_iter().collect::<Vec<_>>(),
            '\r' => ['\\', 'r'].into_iter().collect::<Vec<_>>(),
            '\t' => ['\\', 't'].into_iter().collect::<Vec<_>>(),
            other => [other].into_iter().collect::<Vec<_>>(),
        })
        .collect()
}

fn print_help() {
    println!(concat!(
        "cc-swarm ",
        env!("CARGO_PKG_VERSION"),
        "\n\nCommands:\n  run --profile rough --seeds N --jobs N\n  one --seed 0x... --profile rough [--export-json] [--export-history PATH]\n  model-check [--max-log N] [--max-term N] [--max-messages N] [--max-depth N] [--max-states N]\n  search --profile rough --iterations N\n  regress\n  shrink --failure PATH\n  diff <artifact-a.json> <artifact-b.json>\n  sequence <artifact.json> [--output diagram.svg]\n  proxy [--listen ADDR] [--upstream ADDR] [--drop-every N] [--delay-ms N]\n  check-history --file PATH\n  export-porcupine --file PATH [--output PATH]\n  --selfcheck\n  --determinism\n  --determinism-seeds N"
    ));
}
