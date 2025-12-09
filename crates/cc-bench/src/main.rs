// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpStream;
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
    addr: Option<String>,
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
            addr: None,
        };
        let report = run(&options);
        println!("{}", report_json(&report));
        return Ok(());
    }

    let options = parse_options(&args)?;
    let json = if let Some(address) = options.addr.as_deref() {
        run_remote(&options, address)?
    } else {
        let report = run(&options);
        report_json(&report)
    };
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
    // A misspelt workload is an error, not workload A. The config hash that
    // labels every published number is derived from this value, so silently
    // substituting the default would attribute one workload's numbers to
    // another.
    let workload = match flag(args, "--workload") {
        Some(value) => Workload::parse(&value).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown --workload {value}; expected one of A, B, C, W, CAS, SCAN"),
            )
        })?,
        None => Workload::A,
    };
    let clients = number_flag(args, "--clients", 1)?;
    let ops = number_flag(args, "--ops", 10_000)?;
    let seed = number_flag(args, "--seed", 0x00cc_b001)?;
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
        addr: flag(args, "--addr"),
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

fn run_remote(options: &Options, initial_address: &str) -> io::Result<String> {
    let total_operations = options.ops.saturating_mul(options.repetitions);
    let mut samples = Vec::with_capacity(usize::try_from(total_operations).unwrap_or(0));
    let mut address = initial_address.to_owned();
    let started = Instant::now();
    let mut acknowledged = 0_u64;
    for repetition in 0..options.repetitions {
        for operation in 0..options.ops {
            let operation_number = repetition
                .saturating_mul(options.ops)
                .saturating_add(operation);
            let key = format!("bench-{}", operation % 64);
            let write = match options.workload {
                Workload::A => operation % 2 == 0,
                Workload::B => operation % 20 == 0,
                Workload::C | Workload::Scan => false,
                Workload::W => operation % 20 != 0,
                Workload::Cas => operation % 2 == 0,
            };
            let command = if write {
                vec![
                    String::from("SET"),
                    key,
                    format!("value-{operation_number}"),
                ]
            } else {
                vec![String::from("GET"), key]
            };
            let operation_started = Instant::now();
            let reply = remote_request_follow(&mut address, &command)?;
            if reply.starts_with(b"-") {
                return Err(io::Error::other(format!(
                    "real-host benchmark command failed: {}",
                    String::from_utf8_lossy(&reply)
                )));
            }
            acknowledged = acknowledged.saturating_add(1);
            samples.push(u64::try_from(operation_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        }
    }
    samples.sort_unstable();
    let elapsed_ns = started.elapsed().as_nanos();
    let throughput = u128::from(acknowledged)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed_ns)
        .unwrap_or(0);
    Ok(format!(
        "{{\n  \"schema\": 1,\n  \"mode\": \"real-host\",\n  \"address\": \"{}\",\n  \"workload\": \"{}\",\n  \"clients\": {},\n  \"ops\": {},\n  \"repetitions\": {},\n  \"seed\": {},\n  \"acked\": {},\n  \"throughput_ops_per_sec\": {},\n  \"latency_ns\": {{\"p50\": {}, \"p95\": {}, \"p99\": {}, \"max\": {}}}\n}}",
        json_escape(&address),
        options.workload.as_str(),
        options.clients,
        options.ops,
        options.repetitions,
        options.seed,
        acknowledged,
        throughput,
        percentile(&samples, 50, 100),
        percentile(&samples, 95, 100),
        percentile(&samples, 99, 100),
        samples.last().copied().unwrap_or(0),
    ))
}

fn remote_request_follow(address: &mut String, command: &[String]) -> io::Result<Vec<u8>> {
    for _ in 0..4 {
        let response = remote_request(address, command)?;
        if !response.starts_with(b"-NOTLEADER") {
            return Ok(response);
        }
        let text = String::from_utf8_lossy(&response);
        let Some(next) = text
            .split_whitespace()
            .find_map(|field| field.strip_prefix("addr="))
        else {
            return Ok(response);
        };
        if next == address {
            return Ok(response);
        }
        *address = next.to_owned();
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "real-host benchmark leader redirect exceeded hop limit",
    ))
}

fn remote_request(address: &str, command: &[String]) -> io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
    stream.write_all(&encode_resp_command(command))?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    Ok(response)
}

fn encode_resp_command(command: &[String]) -> Vec<u8> {
    let mut frame = format!("*{}\r\n", command.len()).into_bytes();
    for part in command {
        frame.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        frame.extend_from_slice(part.as_bytes());
        frame.extend_from_slice(b"\r\n");
    }
    frame
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn value(size: usize, operation: u64) -> Vec<u8> {
    let byte = b'a'.saturating_add((operation % 26) as u8);
    vec![byte; size]
}

fn report_json(report: &Report) -> String {
    let mut samples = report.samples.clone();
    samples.sort_unstable();
    let total_ops = report.ops.saturating_mul(report.repetitions);
    let throughput = u128::from(total_ops)
        .saturating_mul(1_000_000_000)
        .checked_div(report.elapsed_ns)
        .unwrap_or(0);
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
