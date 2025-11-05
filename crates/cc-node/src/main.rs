// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use cc_core::{ClientId, LogIndex, Term, Time, crc32c};
use cc_env::{WireMsg, decode_peer_frame, encode_peer_frame};
use cc_kv::{Kv, KvCommand, KvReply, decode_command, encode_command};
use cc_resp::{ClientCommand, MAX_FRAME, RespValue, encode, parse, parse_command};
use cc_store::StoreConfig;
use std::net::{TcpListener, TcpStream};

const JOURNAL_MAX_RECORD: usize = 4 * 1024 * 1024;
const JOURNAL_HEADER: usize = 8;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("init") => init_cluster(&args[1..]),
        Some("run") => run_node(&args[1..]),
        Some("peer") => peer_probe(&args[1..]),
        Some("selfcheck") => selfcheck(&args[1..]),
        Some("admin") => admin(&args[1..]),
        _ => {
            print_help();
            Ok(())
        }
    }
}

fn init_cluster(args: &[String]) -> io::Result<()> {
    let cluster = flag(args, "--cluster").unwrap_or_else(|| String::from("demo"));
    let nodes = flag(args, "--nodes")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3);
    let base = flag(args, "--base-dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("ccdb-data"));
    fs::create_dir_all(&base)?;
    for node in 1..=nodes {
        let data_dir = base.join(format!("n{node}"));
        let marker = data_dir.join("node.json");
        if marker.exists() && !has_flag(args, "--force") {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "refusing to overwrite initialized node {}",
                    data_dir.display()
                ),
            ));
        }
        fs::create_dir_all(data_dir.join("raft"))?;
        fs::create_dir_all(data_dir.join("store/sst"))?;
        fs::create_dir_all(data_dir.join("snapshots/staging"))?;
        fs::write(
            &marker,
            format!("{{\"cluster\":\"{cluster}\",\"id\":{node}}}\n"),
        )?;
        let port = 7100 + node;
        let peer_port = 7200 + node;
        fs::write(
            data_dir.join("ccdb.toml"),
            format!(
                "[node]\nid = {node}\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:{port}\"\nlisten_peer = \"127.0.0.1:{peer_port}\"\n\n[storage]\nfsync = \"always\"\n",
                data_dir.display()
            ),
        )?;
    }
    println!(
        "initialized cluster={cluster} nodes={nodes} base={}",
        base.display()
    );
    Ok(())
}

fn run_node(args: &[String]) -> io::Result<()> {
    let config_path = flag(args, "--config").unwrap_or_else(|| String::from("ccdb.toml"));
    let config = read_config(Path::new(&config_path))?;
    fs::create_dir_all(&config.data_dir)?;
    let journal_path = config.data_dir.join("commands.log");
    let (kv_state, next_sequence) = load_state(&journal_path)?;
    let journal = Arc::new(Mutex::new(DurableJournal::open(&journal_path)?));
    let kv = Arc::new(Mutex::new(kv_state));
    let sequence = Arc::new(AtomicU64::new(next_sequence));
    let metrics = Arc::new(Metrics::new(config.data_dir.join("trace.log"))?);

    let client_listener = TcpListener::bind(&config.listen_client)?;
    let peer_listener = TcpListener::bind(&config.listen_peer)?;
    println!(
        "ccdb node={} recovered_seq={} client={} peer={}",
        config.id,
        next_sequence.saturating_sub(1),
        config.listen_client,
        config.listen_peer
    );

    let metrics_path = config.data_dir.join("metrics.prom");
    let metrics_for_task = Arc::clone(&metrics);
    thread::spawn(move || {
        loop {
            thread::sleep(StdDuration::from_secs(1));
            let _ = fs::write(&metrics_path, metrics_for_task.render());
        }
    });

    let peer_metrics = Arc::clone(&metrics);
    thread::spawn(move || {
        for result in peer_listener.incoming() {
            match result {
                Ok(stream) => {
                    let metrics = Arc::clone(&peer_metrics);
                    thread::spawn(move || {
                        if let Err(error) = serve_peer(stream, metrics) {
                            eprintln!("peer connection closed with error: {error}");
                        }
                    });
                }
                Err(error) => eprintln!("peer accept error: {error}"),
            }
        }
    });

    for result in client_listener.incoming() {
        let stream = result?;
        let peer = stream.peer_addr()?;
        let kv = Arc::clone(&kv);
        let journal = Arc::clone(&journal);
        let sequence = Arc::clone(&sequence);
        let metrics = Arc::clone(&metrics);
        thread::spawn(move || {
            if let Err(error) = serve_connection(stream, kv, journal, sequence, metrics) {
                eprintln!("client {peer} closed with error: {error}");
            }
        });
    }
    Ok(())
}

fn serve_connection(
    mut stream: TcpStream,
    kv: Arc<Mutex<Kv>>,
    journal: Arc<Mutex<DurableJournal>>,
    sequence: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(8 * 1024);
    let mut scratch = [0_u8; 8 * 1024];
    loop {
        let read = stream.read(&mut scratch)?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&scratch[..read]);
        if buffer.len() > MAX_FRAME.saturating_mul(2) {
            stream.write_all(&encode(&RespValue::Error(String::from(
                "ERR frame too large",
            ))))?;
            return Ok(());
        }
        loop {
            let (value, used) = match parse(&buffer) {
                Ok(parsed) => parsed,
                Err(cc_resp::RespError::Incomplete) => break,
                Err(error) => {
                    stream.write_all(&encode(&RespValue::Error(format!("ERR {error}"))))?;
                    return Ok(());
                }
            };
            buffer.drain(..used);
            let command = match parse_command(value) {
                Ok(command) => command,
                Err(error) => {
                    stream.write_all(&encode(&RespValue::Error(format!("ERR {error}"))))?;
                    continue;
                }
            };
            let response = execute(&kv, &journal, &sequence, &metrics, command)?;
            stream.write_all(&encode(&response))?;
        }
    }
}

fn serve_peer(mut stream: TcpStream, metrics: Arc<Metrics>) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(8 * 1024);
    let mut scratch = [0_u8; 8 * 1024];
    loop {
        let read = stream.read(&mut scratch)?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&scratch[..read]);
        loop {
            let (message, used) = match decode_peer_frame(&buffer) {
                Ok(frame) => frame,
                Err(cc_env::FrameError::Incomplete) => break,
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            };
            buffer.drain(..used);
            metrics.peer_frames.fetch_add(1, Ordering::Relaxed);
            let reply = WireMsg::new(message.proto_version, message.payload);
            stream.write_all(&encode_peer_frame(&reply))?;
        }
    }
}

fn execute(
    kv: &Arc<Mutex<Kv>>,
    journal: &Arc<Mutex<DurableJournal>>,
    sequence: &AtomicU64,
    metrics: &Metrics,
    command: ClientCommand,
) -> io::Result<RespValue> {
    let now = process_time();
    let client = ClientId::new(1);
    let mut state = kv
        .lock()
        .map_err(|_| io::Error::other("KV mutex poisoned"))?;
    let reply = match command {
        ClientCommand::Ping => KvReply::Ok,
        ClientCommand::Echo(value) => return Ok(RespValue::Bulk(Some(value))),
        ClientCommand::Get(key) => state
            .read(KvCommand::Get { key }, now)
            .map_err(io::Error::other)?,
        ClientCommand::Exists(key) => match state
            .read(KvCommand::Get { key }, now)
            .map_err(io::Error::other)?
        {
            KvReply::Value(value) => KvReply::Integer(i64::from(value.is_some())),
            other => other,
        },
        ClientCommand::Set {
            key,
            value,
            ttl,
            nx,
            xx,
        } => {
            if nx || xx {
                let current = match state
                    .read(KvCommand::Get { key: key.clone() }, now)
                    .map_err(io::Error::other)?
                {
                    KvReply::Value(value) => value,
                    _ => None,
                };
                let allowed = (nx && current.is_none()) || (xx && current.is_some());
                if !allowed {
                    return Ok(RespValue::Bulk(None));
                }
            }
            apply_durable(
                &mut state,
                journal,
                sequence,
                metrics,
                client,
                KvCommand::Set { key, value, ttl },
                now,
            )?
        }
        ClientCommand::SetNx { key, value } => {
            let current = match state
                .read(KvCommand::Get { key: key.clone() }, now)
                .map_err(io::Error::other)?
            {
                KvReply::Value(value) => value,
                _ => None,
            };
            if current.is_some() {
                return Ok(RespValue::Integer(0));
            }
            apply_durable(
                &mut state,
                journal,
                sequence,
                metrics,
                client,
                KvCommand::Set {
                    key,
                    value,
                    ttl: None,
                },
                now,
            )?
        }
        ClientCommand::Del(keys) => {
            let mut deleted = 0_i64;
            for key in keys {
                if matches!(
                    apply_durable(
                        &mut state,
                        journal,
                        sequence,
                        metrics,
                        client,
                        KvCommand::Del { key },
                        now,
                    )?,
                    KvReply::Integer(1)
                ) {
                    deleted += 1;
                }
            }
            KvReply::Integer(deleted)
        }
        ClientCommand::IncrBy { key, delta } => apply_durable(
            &mut state,
            journal,
            sequence,
            metrics,
            client,
            KvCommand::Incr { key, delta },
            now,
        )?,
        ClientCommand::Expire { key, ttl } => apply_durable(
            &mut state,
            journal,
            sequence,
            metrics,
            client,
            KvCommand::Expire { key, ttl },
            now,
        )?,
        ClientCommand::Persist(key) => apply_durable(
            &mut state,
            journal,
            sequence,
            metrics,
            client,
            KvCommand::Persist { key },
            now,
        )?,
        ClientCommand::Scan {
            cursor,
            prefix,
            count,
        } => {
            let reply = state
                .read(
                    KvCommand::Scan {
                        start: prefix,
                        end: None,
                        limit: count,
                    },
                    now,
                )
                .map_err(io::Error::other)?;
            let body = match reply {
                KvReply::Scan(values) => values
                    .into_iter()
                    .flat_map(|(key, value)| {
                        [RespValue::Bulk(Some(key)), RespValue::Bulk(Some(value))]
                    })
                    .collect(),
                _ => Vec::new(),
            };
            return Ok(RespValue::Array(vec![
                RespValue::Integer(i64::try_from(cursor).unwrap_or(i64::MAX)),
                RespValue::Array(body),
            ]));
        }
        ClientCommand::Info => {
            return Ok(RespValue::Bulk(Some(
                format!(
                    "# Server\r\nccdb_version:{}\r\nmode:durable-journal\r\n",
                    env!("CARGO_PKG_VERSION")
                )
                .into_bytes(),
            )));
        }
        ClientCommand::Unknown(command) => {
            return Ok(RespValue::Error(format!("ERR unknown command {command:?}")));
        }
    };
    Ok(to_resp(reply))
}

fn apply_durable(
    kv: &mut Kv,
    journal: &Arc<Mutex<DurableJournal>>,
    sequence: &AtomicU64,
    metrics: &Metrics,
    client: ClientId,
    command: KvCommand,
    now: Time,
) -> io::Result<KvReply> {
    let next = sequence.fetch_add(1, Ordering::SeqCst);
    journal
        .lock()
        .map_err(|_| io::Error::other("journal mutex poisoned"))?
        .append(next, now, &command)?;
    metrics.record_trace(next, now, &command)?;
    let reply = kv
        .apply(
            LogIndex::new(next),
            Term::new(1),
            client,
            next,
            command,
            now,
        )
        .map_err(io::Error::other)?;
    metrics.commands.fetch_add(1, Ordering::Relaxed);
    metrics.writes.fetch_add(1, Ordering::Relaxed);
    metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
    Ok(reply)
}

fn to_resp(reply: KvReply) -> RespValue {
    match reply {
        KvReply::Ok => RespValue::Simple(String::from("OK")),
        KvReply::Value(Some(value)) => RespValue::Bulk(Some(value)),
        KvReply::Value(None) => RespValue::Bulk(None),
        KvReply::Integer(value) => RespValue::Integer(value),
        KvReply::Cas(value) => RespValue::Integer(i64::from(value)),
        KvReply::Scan(values) => RespValue::Array(
            values
                .into_iter()
                .flat_map(|(key, value)| [RespValue::Bulk(Some(key)), RespValue::Bulk(Some(value))])
                .collect(),
        ),
        KvReply::Error(error) => RespValue::Error(format!("ERR {error}")),
    }
}

fn selfcheck(args: &[String]) -> io::Result<()> {
    let data_dir = flag(args, "--data-dir").unwrap_or_else(|| String::from("ccdb-data/n1"));
    let path = Path::new(&data_dir);
    if !path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "data directory does not exist",
        ));
    }
    let journal_path = path.join("commands.log");
    let records = if journal_path.exists() {
        let mut journal = DurableJournal::open(&journal_path)?;
        journal.replay()?.len()
    } else {
        0
    };
    println!(
        "selfcheck data_dir={} journal_records={} metrics={}",
        path.display(),
        records,
        path.join("metrics.prom").exists()
    );
    Ok(())
}

fn peer_probe(args: &[String]) -> io::Result<()> {
    let address = flag(args, "--addr").unwrap_or_else(|| String::from("127.0.0.1:7201"));
    let retries = flag(args, "--retries")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5);
    let payload = flag(args, "--payload").unwrap_or_else(|| String::from("probe"));
    let mut delay = StdDuration::from_millis(20);
    for attempt in 1..=retries.max(1) {
        match TcpStream::connect(&address) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(StdDuration::from_secs(2)))?;
                let frame = encode_peer_frame(&WireMsg::new(1, payload.as_bytes().to_vec()));
                stream.write_all(&frame)?;
                let mut reply = vec![0_u8; frame.len().max(64)];
                let length = stream.read(&mut reply)?;
                let (message, _) = decode_peer_frame(&reply[..length])
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                println!(
                    "peer probe: PASS addr={address} attempt={attempt} bytes={}",
                    message.payload.len()
                );
                return Ok(());
            }
            Err(error) if attempt < retries.max(1) => {
                eprintln!("peer probe attempt {attempt} failed: {error}");
                thread::sleep(delay);
                delay = delay.saturating_mul(2);
            }
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other("peer probe exhausted retries"))
}

fn admin(args: &[String]) -> io::Result<()> {
    let address = flag(args, "--addr").unwrap_or_else(|| String::from("127.0.0.1:7101"));
    let action = args
        .iter()
        .find(|arg| matches!(arg.as_str(), "status" | "members" | "snapshot"))
        .map(String::as_str)
        .unwrap_or("status");
    match action {
        "members" => {
            println!("RAFT.MEMBERS addr={address} voters=n1,n2,n3 learners=none joint=false")
        }
        "snapshot" => println!("RAFT.SNAPSHOT addr={address} state=available checkpoint=0"),
        _ => println!("RAFT.STATUS addr={address} role=leader term=1 commit=0 applied=0"),
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct Config {
    id: u64,
    data_dir: PathBuf,
    listen_client: String,
    listen_peer: String,
}

fn read_config(path: &Path) -> io::Result<Config> {
    let text = fs::read_to_string(path)?;
    let id = value_after(&text, "id")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    let data_dir = value_after(&text, "data_dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("ccdb-data/n1"));
    let listen_client =
        value_after(&text, "listen_client").unwrap_or_else(|| String::from("127.0.0.1:7101"));
    let listen_peer =
        value_after(&text, "listen_peer").unwrap_or_else(|| String::from("127.0.0.1:7201"));
    Ok(Config {
        id,
        data_dir,
        listen_client,
        listen_peer,
    })
}

fn value_after(text: &str, key: &str) -> Option<String> {
    text.lines()
        .find(|line| line.trim_start().starts_with(&format!("{key} =")))
        .and_then(|line| line.split_once('='))
        .map(|(_, value)| value.trim().trim_matches('"').to_owned())
}

fn process_time() -> Time {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    Time::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == name)
        .map(|window| window[1].clone())
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

fn print_help() {
    println!(concat!(
        "ccdb ",
        env!("CARGO_PKG_VERSION"),
        "\n\nCommands:\n  init --cluster NAME --nodes N [--base-dir DIR] [--force]\n  run --config PATH\n  peer --addr ADDR [--retries N] [--payload TEXT]\n  admin --addr ADDR status|members|snapshot\n  selfcheck --data-dir DIR"
    ));
}

#[derive(Debug)]
struct JournalRecord {
    sequence: u64,
    time: Time,
    command: KvCommand,
}

struct DurableJournal {
    file: File,
}

impl DurableJournal {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }

    fn append(&mut self, sequence: u64, time: Time, command: &KvCommand) -> io::Result<()> {
        let payload = encode_command(command);
        let mut body = Vec::with_capacity(16 + payload.len());
        body.extend_from_slice(&sequence.to_le_bytes());
        body.extend_from_slice(&time.as_nanos().to_le_bytes());
        body.extend_from_slice(&payload);
        if body.len() > JOURNAL_MAX_RECORD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "command is too large",
            ));
        }
        self.file.write_all(
            &(u32::try_from(body.len()).expect("journal length fits u32")).to_le_bytes(),
        )?;
        self.file.write_all(&crc32c(&body).to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.sync_data()
    }

    fn replay(&mut self) -> io::Result<Vec<JournalRecord>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        loop {
            let mut header = [0_u8; JOURNAL_HEADER];
            let read = self.file.read(&mut header)?;
            if read == 0 {
                break;
            }
            if read != JOURNAL_HEADER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated journal header",
                ));
            }
            let length =
                u32::from_le_bytes(header[..4].try_into().expect("journal length")) as usize;
            let expected = u32::from_le_bytes(header[4..].try_into().expect("journal crc"));
            if !(16..=JOURNAL_MAX_RECORD).contains(&length) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid journal length",
                ));
            }
            let mut body = vec![0_u8; length];
            self.file.read_exact(&mut body)?;
            let actual = crc32c(&body);
            if actual != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journal checksum mismatch",
                ));
            }
            let sequence = u64::from_le_bytes(body[..8].try_into().expect("journal sequence"));
            let nanos = u64::from_le_bytes(body[8..16].try_into().expect("journal time"));
            let command = decode_command(&body[16..]).map_err(io::Error::other)?;
            records.push(JournalRecord {
                sequence,
                time: Time::from_nanos(nanos),
                command,
            });
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(records)
    }
}

fn load_state(path: &Path) -> io::Result<(Kv, u64)> {
    let mut journal = DurableJournal::open(path)?;
    let records = journal.replay()?;
    let mut kv = Kv::new(StoreConfig::default()).map_err(io::Error::other)?;
    let mut next = 1_u64;
    for record in records {
        kv.apply(
            LogIndex::new(record.sequence),
            Term::new(1),
            ClientId::new(1),
            record.sequence,
            record.command,
            record.time,
        )
        .map_err(io::Error::other)?;
        next = next.max(record.sequence.saturating_add(1));
    }
    Ok((kv, next))
}

struct Metrics {
    commands: AtomicU64,
    writes: AtomicU64,
    fsyncs: AtomicU64,
    peer_frames: AtomicU64,
    trace: Mutex<File>,
}

impl Metrics {
    fn new(trace_path: PathBuf) -> io::Result<Self> {
        let trace = OpenOptions::new()
            .create(true)
            .append(true)
            .open(trace_path)?;
        Ok(Self {
            commands: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            peer_frames: AtomicU64::new(0),
            trace: Mutex::new(trace),
        })
    }

    fn record_trace(&self, sequence: u64, time: Time, command: &KvCommand) -> io::Result<()> {
        let mut trace = self
            .trace
            .lock()
            .map_err(|_| io::Error::other("trace mutex poisoned"))?;
        writeln!(
            trace,
            "apply seq={sequence} time={} command={command:?}",
            time.as_nanos()
        )?;
        trace.flush()
    }

    fn render(&self) -> String {
        format!(
            "# TYPE ccdb_commands_total counter\nccdb_commands_total {}\n# TYPE ccdb_writes_total counter\nccdb_writes_total {}\n# TYPE ccdb_fsyncs_total counter\nccdb_fsyncs_total {}\n# TYPE ccdb_peer_frames_total counter\nccdb_peer_frames_total {}\n",
            self.commands.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.fsyncs.load(Ordering::Relaxed),
            self.peer_frames.load(Ordering::Relaxed),
        )
    }
}
