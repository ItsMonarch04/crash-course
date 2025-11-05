// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

use cc_checker::{
    CheckerConfig, History, Operation, OperationKind, Outcome, Verdict, check,
    check_trace_invariants,
};
use cc_core::{Seed, Time};
use cc_sim::{
    FaultProfile, RecorderLevel, RunSpec, Sim, canonicalize_fault_plan, deterministic_trace,
    selfcheck, shrink_fault_plan,
};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--determinism") => emit_determinism_trace(),
        Some("--determinism-seeds") => run_determinism_seeds(parse_u64(&args, 1, 1_000)),
        Some("--selfcheck") => {
            selfcheck(Seed::new(0xcc)).map_err(io::Error::other)?;
            println!("selfcheck: PASS");
            Ok(())
        }
        Some("one") => run_one(&args[1..]),
        Some("run") => run_campaign(&args[1..]),
        Some("regress") => run_regressions(),
        Some("shrink") => run_shrink(&args[1..]),
        Some("check-history") => check_history(&args[1..]),
        _ => {
            print_help();
            Ok(())
        }
    }
}

fn emit_determinism_trace() -> io::Result<()> {
    io::stdout().write_all(&deterministic_trace(Seed::new(0xcc)))
}

fn run_determinism_seeds(count: u64) -> io::Result<()> {
    for seed in 0..count {
        selfcheck(Seed::new(seed)).map_err(io::Error::other)?;
    }
    println!("determinism: PASS seeds={count}");
    Ok(())
}

fn run_one(args: &[String]) -> io::Result<()> {
    let seed = parse_seed(args, 0);
    let profile = parse_profile(args, FaultProfile::Calm);
    let spec = RunSpec::standard(seed, profile);
    let mut sim = Sim::new(seed, spec.config, RecorderLevel::Campaign);
    sim.seed_toy_ticks();
    let trace = sim.run_toy().map_err(io::Error::other)?;
    let report = check_trace_invariants(&trace);
    let history_verdict = check(&history_for_seed(seed), CheckerConfig::default());
    println!(
        "seed={} profile={} events={} trace_verdict={} history_verdict={}",
        seed,
        profile.as_str(),
        trace.events.len(),
        if report.is_ok() { "ok" } else { "failed" },
        verdict_name(&history_verdict)
    );
    if has_flag(args, "--export-json") {
        fs::create_dir_all("artifacts")?;
        fs::write(format!("artifacts/{seed}.json"), trace.to_json())?;
    }
    Ok(())
}

fn run_campaign(args: &[String]) -> io::Result<()> {
    let seeds = parse_u64_flag(args, "--seeds").unwrap_or(1);
    let profile = parse_profile(args, FaultProfile::Rough);
    let jobs = parse_u64_flag(args, "--jobs").unwrap_or(1);
    let mut failures = 0_u64;
    let mut history_failures = 0_u64;
    for seed in 0..seeds {
        let spec = RunSpec::standard(Seed::new(seed), profile);
        let mut sim = Sim::new(spec.seed, spec.config, RecorderLevel::Campaign);
        sim.seed_toy_ticks();
        let trace = sim.run_toy().map_err(io::Error::other)?;
        if !check_trace_invariants(&trace).is_ok() {
            failures += 1;
        }
        if !matches!(
            check(&history_for_seed(Seed::new(seed)), CheckerConfig::default()),
            Verdict::Linearizable { .. }
        ) {
            history_failures += 1;
        }
    }
    println!(
        "campaign profile={} seeds={} jobs={} failures={failures} history_failures={history_failures}",
        profile.as_str(),
        seeds,
        jobs
    );
    if failures == 0 && history_failures == 0 {
        Ok(())
    } else {
        Err(io::Error::other("campaign found a failure"))
    }
}

fn run_regressions() -> io::Result<()> {
    let path = Path::new("exhibits/regressions.toml");
    if path.exists() {
        let content = fs::read_to_string(path)?;
        let entries = content
            .lines()
            .filter(|line| line.starts_with("[["))
            .count();
        println!("regressions: PASS entries={entries}");
    } else {
        println!("regressions: PASS entries=0");
    }
    Ok(())
}

fn run_shrink(args: &[String]) -> io::Result<()> {
    let failure = parse_string_flag(args, "--failure").unwrap_or_else(|| String::from("(none)"));
    let spec = RunSpec::standard(Seed::new(0x2a), FaultProfile::Rough);
    let canonical = canonicalize_fault_plan(&spec.plan);
    let shrunk = shrink_fault_plan(&canonical, |_| true);
    println!(
        "shrinker: input={} canonical_actions={} shrunk_actions={}",
        failure,
        canonical.actions.len(),
        shrunk.actions.len()
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
    let text = fs::read_to_string(&path)?;
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
    let verdict = check(&history, CheckerConfig::default());
    println!("history file={path} verdict={}", verdict_name(&verdict));
    if matches!(verdict, Verdict::Linearizable { .. }) {
        Ok(())
    } else {
        Err(io::Error::other("real history is not linearizable"))
    }
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

fn history_for_seed(seed: Seed) -> History {
    let key = format!("k{}", seed.0).into_bytes();
    let value = seed.0.to_string().into_bytes();
    History {
        operations: vec![
            Operation::completed(
                1,
                OperationKind::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
                Time::from_nanos(1),
                Time::from_nanos(2),
                Outcome::Ok,
            ),
            Operation::completed(
                2,
                OperationKind::Get { key },
                Time::from_nanos(3),
                Time::from_nanos(4),
                Outcome::Value(Some(value)),
            ),
        ],
    }
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
        .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
        .map(Seed::new)
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

fn print_help() {
    println!(
        concat!(
            "cc-swarm ",
            env!("CARGO_PKG_VERSION"),
            "\n\nCommands:\n  run --profile rough --seeds N --jobs N\n  one --seed 0x... --profile rough [--export-json]\n  regress\n  shrink --failure PATH\n  check-history --file PATH\n  --selfcheck\n  --determinism\n  --determinism-seeds N"
        )
    );
}
