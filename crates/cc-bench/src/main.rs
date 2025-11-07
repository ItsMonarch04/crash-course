// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Instant;

use cc_core::{Seed, Xoshiro256pp, fnv1a};

const DEFAULT_KEYS: u64 = 1_000_000;
const DEFAULT_VALUE_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Workload {
    A,
    B,
    C,
    W,
    Cas,
    Scan,
}

impl Workload {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_uppercase().as_str() {
            "A" | "50/50" => Some(Self::A),
            "B" | "95/5" => Some(Self::B),
            "C" | "READ" | "READ-ONLY" => Some(Self::C),
            "W" | "5/95" | "WRITE" => Some(Self::W),
            "CAS" | "CAS-CONTENTION" => Some(Self::Cas),
            "SCAN" | "SCAN-HEAVY" => Some(Self::Scan),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::W => "W",
            Self::Cas => "CAS-contention",
            Self::Scan => "scan-heavy",
        }
    }
}

#[derive(Debug)]
struct Options {
    workload: Workload,
    clients: u64,
    ops: u64,
    seed: u64,
    value_bytes: usize,
    repetitions: u64,
    output: Option<String>,
}

#[derive(Debug)]
struct Report {
    workload: Workload,
    clients: u64,
    ops: u64,
    repetitions: u64,
    seed: u64,
    value_bytes: usize,
    elapsed_ns: u128,
    samples: Vec<u64>,
    config_hash: u64,
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.first().is_some_and(|value| value == "repro") {
        let path = args.get(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "cc-bench repro requires a JSON path",
            )
        })?;
        let text = fs::read_to_string(path)?;
        let options = Options {
            workload: Workload::parse(
                &json_string(&text, "workload").unwrap_or_else(|| "A".into()),
            )
            .unwrap_or(Workload::A),
            clients: json_u64(&text, "clients").unwrap_or(1),
            ops: json_u64(&text, "ops").unwrap_or(10_000),
            seed: json_u64(&text, "seed").unwrap_or(0),
            value_bytes: usize::try_from(json_u64(&text, "value_bytes").unwrap_or(128))
                .unwrap_or(DEFAULT_VALUE_BYTES),
            repetitions: 1,
            output: None,
        };
        let report = run(&options);
        println!("{}", report_json(&report));
        return Ok(());
    }

    let options = parse_options(&args)?;
    let report = run(&options);
    let json = report_json(&report);
    if let Some(output) = options.output {
        if let Some(parent) = Path::new(&output).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&output, json)?;
        println!("cc-bench: wrote {output}");
    } else {
        println!("{json}");
    }
    Ok(())
}

fn parse_options(args: &[String]) -> io::Result<Options> {
    let workload = flag(args, "--workload")
        .as_deref()
        .and_then(Workload::parse)
        .unwrap_or(Workload::A);
    let clients = number_flag(args, "--clients", 1)?;
    let ops = number_flag(args, "--ops", 10_000)?;
    let seed = number_flag(args, "--seed", 0x_ccb0_01)?;
    let value_bytes = usize::try_from(number_flag(
        args,
        "--value-bytes",
        DEFAULT_VALUE_BYTES as u64,
    )?)
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value size is too large"))?;
    let repetitions = number_flag(args, "--repetitions", 3)?;
    if clients == 0 || ops == 0 || repetitions == 0 || value_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "clients, ops, repetitions, and value-bytes must be non-zero",
        ));
    }
    Ok(Options {
        workload,
        clients,
        ops,
        seed,
        value_bytes,
        repetitions,
        output: flag(args, "--output"),
    })
}

fn run(options: &Options) -> Report {
    let config = format!(
        "v1|workload={}|clients={}|ops={}|seed={}|value_bytes={}|repetitions={}",
        options.workload.as_str(),
        options.clients,
        options.ops,
        options.seed,
        options.value_bytes,
        options.repetitions
    );
    let config_hash = fnv1a(config.as_bytes());
    let mut samples = Vec::with_capacity((options.ops * options.repetitions) as usize);
    let overall = Instant::now();
    for repetition in 0..options.repetitions {
        let mut state = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut rng = Xoshiro256pp::stream(
            Seed::new(options.seed.wrapping_add(repetition)),
            "bench",
            options.clients,
        );
        for operation in 0..options.ops {
            let key = if options.workload == Workload::Cas {
                b"hot-key".to_vec()
            } else {
                format!("k{:07}", rng.range_u64(0, DEFAULT_KEYS)).into_bytes()
            };
            let started = Instant::now();
            match options.workload {
                Workload::A => {
                    if operation % 2 == 0 {
                        state.insert(key, value(options.value_bytes, operation));
                    } else {
                        let _ = state.get(&key);
                    }
                }
                Workload::B | Workload::C => {
                    if options.workload == Workload::B && operation % 20 == 0 {
                        state.insert(key, value(options.value_bytes, operation));
                    } else {
                        let _ = state.get(&key);
                    }
                }
                Workload::W => {
                    if operation % 20 != 0 {
                        state.insert(key, value(options.value_bytes, operation));
                    } else {
                        let _ = state.get(&key);
                    }
                }
                Workload::Cas => {
                    let expected = state.get(&key).cloned();
                    if operation % 2 == 0 || expected.is_none() {
                        state.insert(key, value(options.value_bytes, operation));
                    }
                }
                Workload::Scan => {
                    let _ = state.range(key..).take(32).count();
                }
            }
            let client_bias = operation % options.clients;
            let elapsed = started.elapsed().as_nanos() as u64;
            samples.push(elapsed.saturating_add(client_bias));
        }
    }
    Report {
        workload: options.workload,
        clients: options.clients,
        ops: options.ops,
        repetitions: options.repetitions,
        seed: options.seed,
        value_bytes: options.value_bytes,
        elapsed_ns: overall.elapsed().as_nanos(),
        samples,
        config_hash,
    }
}

fn value(size: usize, operation: u64) -> Vec<u8> {
    let byte = b'a'.saturating_add((operation % 26) as u8);
    vec![byte; size]
}

fn report_json(report: &Report) -> String {
    let mut samples = report.samples.clone();
    samples.sort_unstable();
    let total_ops = report.ops.saturating_mul(report.repetitions);
    let throughput = if report.elapsed_ns == 0 {
        0
    } else {
        u128::from(total_ops).saturating_mul(1_000_000_000) / report.elapsed_ns
    };
    format!(
        "{{\n  \"schema\": 1,\n  \"workload\": \"{}\",\n  \"clients\": {},\n  \"ops\": {},\n  \"repetitions\": {},\n  \"seed\": {},\n  \"value_bytes\": {},\n  \"config_hash\": \"{:016x}\",\n  \"environment\": {{\"os\": \"{}\", \"arch\": \"{}\", \"note\": \"closed-loop deterministic local model; loopback replication latency is not measured\"}},\n  \"throughput_ops_per_sec\": {},\n  \"latency_ns\": {{\"p50\": {}, \"p95\": {}, \"p99\": {}, \"p999\": {}, \"max\": {}}}\n}}",
        report.workload.as_str(),
        report.clients,
        report.ops,
        report.repetitions,
        report.seed,
        report.value_bytes,
        report.config_hash,
        env::consts::OS,
        env::consts::ARCH,
        throughput,
        percentile(&samples, 50, 1000),
        percentile(&samples, 95, 1000),
        percentile(&samples, 99, 1000),
        percentile(&samples, 999, 1000),
        samples.last().copied().unwrap_or(0),
    )
}

fn percentile(samples: &[u64], numerator: usize, denominator: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let index = (samples.len().saturating_sub(1) * numerator) / denominator;
    samples[index]
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn number_flag(args: &[String], name: &str, default: u64) -> io::Result<u64> {
    let value = flag(args, name).unwrap_or_else(|| default.to_string());
    value.parse().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} requires an unsigned integer"),
        )
    })
}

fn json_string(text: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\": \"");
    let start = text.find(&marker)? + marker.len();
    let end = text[start..].find('"')? + start;
    Some(text[start..end].to_owned())
}

fn json_u64(text: &str, key: &str) -> Option<u64> {
    let marker = format!("\"{key}\":");
    let start = text.find(&marker)? + marker.len();
    let number = text[start..]
        .trim_start()
        .split(|character: char| !character.is_ascii_digit())
        .next()?;
    number.parse().ok()
}
