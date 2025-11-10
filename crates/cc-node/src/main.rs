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
const REPLICATION_MAGIC: &[u8] = b"CCREPL1";
const REPLICATION_WRITE: u8 = b'W';
const REPLICATION_SYNC: u8 = b'S';
const REPLICATION_ACK: u8 = b'A';
const REPLICATION_SNAPSHOT: u8 = b'R';
const PEER_CONNECT_TIMEOUT: StdDuration = StdDuration::from_millis(250);
const PEER_IO_TIMEOUT: StdDuration = StdDuration::from_millis(500);

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
        let peer_nodes = (1..=nodes)
            .map(|peer| format!("127.0.0.1:{}", 7200 + peer))
            .collect::<Vec<_>>()
            .join(",");
        fs::write(
            data_dir.join("ccdb.toml"),
            format!(
                "[node]\nid = {node}\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:{port}\"\nlisten_peer = \"127.0.0.1:{peer_port}\"\npeer_nodes = \"{peer_nodes}\"\n\n[storage]\nfsync = \"always\"\n",
                data_dir.display()
            ),
        )?;
        sync_directory(&data_dir)?;
    }
    sync_directory(&base)?;
    println!(
        "initialized cluster={cluster} nodes={nodes} base={}",
        base.display()
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Peer {
    id: u64,
    address: String,
}

#[derive(Clone)]
struct HostState {
    config: Config,
    kv: Arc<Mutex<Kv>>,
    journal: Arc<Mutex<DurableJournal>>,
    sequence: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
}

fn run_node(args: &[String]) -> io::Result<()> {
    let config_path = flag(args, "--config").unwrap_or_else(|| String::from("ccdb.toml"));
    let config = read_config(Path::new(&config_path))?;
    validate_identity(&config)?;
    fs::create_dir_all(&config.data_dir)?;
    let journal_path = config.data_dir.join("commands.log");
    let (kv_state, next_sequence) = load_state(&journal_path)?;
    let journal = Arc::new(Mutex::new(DurableJournal::open(&journal_path)?));
    let kv = Arc::new(Mutex::new(kv_state));
    let sequence = Arc::new(AtomicU64::new(next_sequence));
    let metrics = Arc::new(Metrics::new(config.data_dir.join("trace.log"))?);
    let state = Arc::new(HostState {
        config: config.clone(),
        kv,
        journal,
        sequence,
        metrics,
    });

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
    let metrics_for_task = Arc::clone(&state.metrics);
    thread::spawn(move || {
        loop {
            thread::sleep(StdDuration::from_secs(1));
            let _ = fs::write(&metrics_path, metrics_for_task.render());
        }
    });

    let peer_state = Arc::clone(&state);
    thread::spawn(move || {
        for result in peer_listener.incoming() {
            match result {
                Ok(stream) => {
                    let state = Arc::clone(&peer_state);
                    thread::spawn(move || {
                        if let Err(error) = serve_peer(stream, state) {
                            eprintln!("peer connection closed with error: {error}");
                        }
                    });
                }
                Err(error) => eprintln!("peer accept error: {error}"),
            }
        }
    });

    sync_from_peers(&state)?;

    for result in client_listener.incoming() {
        let stream = result?;
        let peer = stream.peer_addr()?;
        let state = Arc::clone(&state);
        thread::spawn(move || {
            if let Err(error) = serve_connection(stream, state) {
                eprintln!("client {peer} closed with error: {error}");
            }
        });
    }
    Ok(())
}

fn serve_connection(mut stream: TcpStream, state: Arc<HostState>) -> io::Result<()> {
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
            let response = execute(&state, command)?;
            stream.write_all(&encode(&response))?;
        }
    }
}

fn serve_peer(mut stream: TcpStream, state: Arc<HostState>) -> io::Result<()> {
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
            state.metrics.peer_frames.fetch_add(1, Ordering::Relaxed);
            let reply = peer_request(&state, message)?;
            stream.write_all(&encode_peer_frame(&reply))?;
        }
    }
}

fn execute(state: &Arc<HostState>, command: ClientCommand) -> io::Result<RespValue> {
    let now = process_time();
    let client = ClientId::new(1);
    if is_write_command(&command) {
        let (leader, peer_address) = leader_info(&state.config);
        if leader != state.config.id {
            let address = client_address_for(&state.config, leader, &peer_address);
            return Ok(RespValue::Error(format!(
                "NOTLEADER leader=n{leader} addr={address}"
            )));
        }
    }
    let reply = match command {
        ClientCommand::Ping => KvReply::Ok,
        ClientCommand::Echo(value) => return Ok(RespValue::Bulk(Some(value))),
        ClientCommand::Get(key) => state
            .kv
            .lock()
            .map_err(|_| io::Error::other("KV mutex poisoned"))?
            .read(KvCommand::Get { key }, now)
            .map_err(io::Error::other)?,
        ClientCommand::Exists(key) => match state
            .kv
            .lock()
            .map_err(|_| io::Error::other("KV mutex poisoned"))?
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
                    .kv
                    .lock()
                    .map_err(|_| io::Error::other("KV mutex poisoned"))?
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
            match apply_durable(state, client, KvCommand::Set { key, value, ttl }, now) {
                Ok(reply) => reply,
                Err(error) => return Ok(RespValue::Error(format!("ERR {error}"))),
            }
        }
        ClientCommand::SetNx { key, value } => {
            let current = match state
                .kv
                .lock()
                .map_err(|_| io::Error::other("KV mutex poisoned"))?
                .read(KvCommand::Get { key: key.clone() }, now)
                .map_err(io::Error::other)?
            {
                KvReply::Value(value) => value,
                _ => None,
            };
            if current.is_some() {
                return Ok(RespValue::Integer(0));
            }
            match apply_durable(
                state,
                client,
                KvCommand::Set {
                    key,
                    value,
                    ttl: None,
                },
                now,
            ) {
                Ok(reply) => reply,
                Err(error) => return Ok(RespValue::Error(format!("ERR {error}"))),
            }
        }
        ClientCommand::Del(keys) => {
            let mut deleted = 0_i64;
            for key in keys {
                match apply_durable(state, client, KvCommand::Del { key }, now) {
                    Ok(KvReply::Integer(1)) => deleted += 1,
                    Ok(_) => {}
                    Err(error) => return Ok(RespValue::Error(format!("ERR {error}"))),
                }
            }
            KvReply::Integer(deleted)
        }
        ClientCommand::IncrBy { key, delta } => {
            match apply_durable(state, client, KvCommand::Incr { key, delta }, now) {
                Ok(reply) => reply,
                Err(error) => return Ok(RespValue::Error(format!("ERR {error}"))),
            }
        }
        ClientCommand::Expire { key, ttl } => {
            match apply_durable(state, client, KvCommand::Expire { key, ttl }, now) {
                Ok(reply) => reply,
                Err(error) => return Ok(RespValue::Error(format!("ERR {error}"))),
            }
        }
        ClientCommand::Persist(key) => {
            match apply_durable(state, client, KvCommand::Persist { key }, now) {
                Ok(reply) => reply,
                Err(error) => return Ok(RespValue::Error(format!("ERR {error}"))),
            }
        }
        ClientCommand::Scan {
            cursor,
            prefix,
            count,
        } => {
            let reply = state
                .kv
                .lock()
                .map_err(|_| io::Error::other("KV mutex poisoned"))?
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
            let (leader, peer_address) = leader_info(&state.config);
            let client_address = client_address_for(&state.config, leader, &peer_address);
            let role = if leader == state.config.id {
                "leader"
            } else {
                "follower"
            };
            return Ok(RespValue::Bulk(Some(
                format!(
                    "# Server\r\nccdb_version:{}\r\nmode:durable-journal\r\nrole:{role}\r\nleader:n{leader}\r\nleader_peer_addr:{peer_address}\r\nleader_client_addr:{client_address}\r\ncommit:{}\r\napplied:{}\r\n",
                    env!("CARGO_PKG_VERSION"),
                    state.sequence.load(Ordering::Acquire).saturating_sub(1),
                    state
                        .kv
                        .lock()
                        .map_err(|_| io::Error::other("KV mutex poisoned"))?
                        .applied_index
                        .get(),
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

fn is_write_command(command: &ClientCommand) -> bool {
    matches!(
        command,
        ClientCommand::Set { .. }
            | ClientCommand::SetNx { .. }
            | ClientCommand::Del(_)
            | ClientCommand::IncrBy { .. }
            | ClientCommand::Expire { .. }
            | ClientCommand::Persist(_)
    )
}

fn apply_durable(
    state: &HostState,
    client: ClientId,
    command: KvCommand,
    now: Time,
) -> io::Result<KvReply> {
    let next = state.sequence.fetch_add(1, Ordering::SeqCst);
    replicate(state, next, now, &command)?;
    state
        .journal
        .lock()
        .map_err(|_| io::Error::other("journal mutex poisoned"))?
        .append(next, now, &command)?;
    state.metrics.record_trace(next, now, &command)?;
    let reply = state
        .kv
        .lock()
        .map_err(|_| io::Error::other("KV mutex poisoned"))?
        .apply(
            LogIndex::new(next),
            Term::new(1),
            client,
            next,
            command,
            now,
        )
        .map_err(io::Error::other)?;
    state.metrics.commands.fetch_add(1, Ordering::Relaxed);
    state.metrics.writes.fetch_add(1, Ordering::Relaxed);
    state.metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
    Ok(reply)
}

fn leader_info(config: &Config) -> (u64, String) {
    let mut candidates = vec![(config.id, config.listen_peer.clone())];
    for peer in &config.peers {
        if peer.id == config.id {
            continue;
        }
        if TcpStream::connect_timeout(
            &peer
                .address
                .parse()
                .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
            PEER_CONNECT_TIMEOUT,
        )
        .is_ok()
        {
            candidates.push((peer.id, peer.address.clone()));
        }
    }
    candidates.sort_by_key(|(id, _)| *id);
    candidates
        .into_iter()
        .next()
        .unwrap_or((config.id, config.listen_peer.clone()))
}

fn client_address_for(config: &Config, node_id: u64, peer_address: &str) -> String {
    if node_id == config.id {
        return config.listen_client.clone();
    }
    if let Some((host, port)) = peer_address.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
        && (7201..=7299).contains(&port)
    {
        return format!("{host}:{}", port.saturating_sub(100));
    }
    peer_address.to_owned()
}

fn replicate(state: &HostState, sequence: u64, time: Time, command: &KvCommand) -> io::Result<()> {
    let total_nodes = state.config.peers.len().max(1);
    let quorum = total_nodes / 2 + 1;
    let mut acknowledgements = 1_usize;
    let mut last_error = None;
    for peer in &state.config.peers {
        if peer.id == state.config.id {
            continue;
        }
        match send_replication(peer, sequence, time, command) {
            Ok(()) => acknowledgements += 1,
            Err(error) => last_error = Some(error),
        }
    }
    if acknowledgements >= quorum {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "replication quorum unavailable: acknowledgements={acknowledgements}/{quorum}{}",
                last_error
                    .map(|error| format!(" last_error={error}"))
                    .unwrap_or_default()
            ),
        ))
    }
}

fn send_replication(peer: &Peer, sequence: u64, time: Time, command: &KvCommand) -> io::Result<()> {
    let payload = encode_replication_write(sequence, time, command)?;
    let mut delay = StdDuration::from_millis(10);
    let mut last_error = None;
    for attempt in 0..3 {
        match TcpStream::connect_timeout(
            &peer
                .address
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer address"))?,
            PEER_CONNECT_TIMEOUT,
        ) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(PEER_IO_TIMEOUT))?;
                stream.set_write_timeout(Some(PEER_IO_TIMEOUT))?;
                stream.write_all(&encode_peer_frame(&WireMsg::new(1, payload.clone())))?;
                let reply = read_wire_message(&mut stream)?;
                if is_ack(&reply.payload, sequence) {
                    return Ok(());
                }
                last_error = Some(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "peer returned a non-ack replication response",
                ));
            }
            Err(error) => last_error = Some(error),
        }
        if attempt < 2 {
            thread::sleep(delay);
            delay = delay.saturating_mul(2);
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("replication failed")))
}

fn encode_replication_write(sequence: u64, time: Time, command: &KvCommand) -> io::Result<Vec<u8>> {
    let command = encode_command(command);
    let length = u32::try_from(command.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "replication command too large")
    })?;
    let mut payload = Vec::with_capacity(REPLICATION_MAGIC.len() + 1 + 8 + 8 + 4 + command.len());
    payload.extend_from_slice(REPLICATION_MAGIC);
    payload.push(REPLICATION_WRITE);
    payload.extend_from_slice(&sequence.to_le_bytes());
    payload.extend_from_slice(&time.as_nanos().to_le_bytes());
    payload.extend_from_slice(&length.to_le_bytes());
    payload.extend_from_slice(&command);
    Ok(payload)
}

fn decode_replication_write(payload: &[u8]) -> io::Result<(u64, Time, KvCommand)> {
    let mut cursor = 0_usize;
    expect_bytes(payload, &mut cursor, REPLICATION_MAGIC)?;
    expect_byte(payload, &mut cursor, REPLICATION_WRITE)?;
    let sequence = take_u64(payload, &mut cursor)?;
    let time = Time::from_nanos(take_u64(payload, &mut cursor)?);
    let length = usize::try_from(take_u32(payload, &mut cursor)?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid replication length"))?;
    let command_bytes = take_bytes(payload, &mut cursor, length)?;
    if cursor != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "replication write has trailing bytes",
        ));
    }
    let command = decode_command(command_bytes).map_err(io::Error::other)?;
    Ok((sequence, time, command))
}

fn apply_replica(
    state: &HostState,
    sequence: u64,
    time: Time,
    command: KvCommand,
) -> io::Result<()> {
    if sequence < state.sequence.load(Ordering::Acquire) {
        return Ok(());
    }
    state
        .journal
        .lock()
        .map_err(|_| io::Error::other("journal mutex poisoned"))?
        .append(sequence, time, &command)?;
    state.metrics.record_trace(sequence, time, &command)?;
    state
        .kv
        .lock()
        .map_err(|_| io::Error::other("KV mutex poisoned"))?
        .apply(
            LogIndex::new(sequence),
            Term::new(1),
            ClientId::new(1),
            sequence,
            command,
            time,
        )
        .map_err(io::Error::other)?;
    state
        .sequence
        .fetch_max(sequence.saturating_add(1), Ordering::AcqRel);
    state.metrics.commands.fetch_add(1, Ordering::Relaxed);
    state.metrics.writes.fetch_add(1, Ordering::Relaxed);
    state.metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

fn peer_request(state: &HostState, message: WireMsg) -> io::Result<WireMsg> {
    let payload = &message.payload;
    if payload.starts_with(REPLICATION_MAGIC) {
        let tag = payload
            .get(REPLICATION_MAGIC.len())
            .copied()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "replication message has no tag")
            })?;
        match tag {
            REPLICATION_WRITE => {
                let (sequence, time, command) = decode_replication_write(payload)?;
                apply_replica(state, sequence, time, command)?;
                let mut reply = Vec::with_capacity(REPLICATION_MAGIC.len() + 1 + 8);
                reply.extend_from_slice(REPLICATION_MAGIC);
                reply.push(REPLICATION_ACK);
                reply.extend_from_slice(&sequence.to_le_bytes());
                return Ok(WireMsg::new(1, reply));
            }
            REPLICATION_SYNC => return Ok(WireMsg::new(1, snapshot_payload(state)?)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown replication message tag",
                ));
            }
        }
    }
    Ok(message)
}

fn is_ack(payload: &[u8], sequence: u64) -> bool {
    payload.len() == REPLICATION_MAGIC.len() + 1 + 8
        && payload.starts_with(REPLICATION_MAGIC)
        && payload[REPLICATION_MAGIC.len()] == REPLICATION_ACK
        && u64::from_le_bytes(
            payload[REPLICATION_MAGIC.len() + 1..]
                .try_into()
                .expect("ack sequence length"),
        ) == sequence
}

fn snapshot_payload(state: &HostState) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    payload.extend_from_slice(REPLICATION_MAGIC);
    payload.push(REPLICATION_SNAPSHOT);
    let records = state
        .journal
        .lock()
        .map_err(|_| io::Error::other("journal mutex poisoned"))?
        .replay()?;
    let count = u32::try_from(records.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many snapshot records"))?;
    payload.extend_from_slice(&count.to_le_bytes());
    for record in records {
        let command = encode_command(&record.command);
        let length = u32::try_from(command.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "snapshot command too large")
        })?;
        payload.extend_from_slice(&record.sequence.to_le_bytes());
        payload.extend_from_slice(&record.time.as_nanos().to_le_bytes());
        payload.extend_from_slice(&length.to_le_bytes());
        payload.extend_from_slice(&command);
    }
    if payload.len() > cc_env::MAX_PEER_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot exceeds peer frame limit",
        ));
    }
    Ok(payload)
}

fn sync_from_peers(state: &HostState) -> io::Result<()> {
    for peer in &state.config.peers {
        if peer.id == state.config.id {
            continue;
        }
        let mut stream = match TcpStream::connect_timeout(
            &peer
                .address
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer address"))?,
            PEER_CONNECT_TIMEOUT,
        ) {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        stream.set_read_timeout(Some(PEER_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(PEER_IO_TIMEOUT))?;
        let mut request = Vec::with_capacity(REPLICATION_MAGIC.len() + 1);
        request.extend_from_slice(REPLICATION_MAGIC);
        request.push(REPLICATION_SYNC);
        stream.write_all(&encode_peer_frame(&WireMsg::new(1, request)))?;
        let reply = read_wire_message(&mut stream)?;
        apply_snapshot_payload(state, &reply.payload)?;
    }
    Ok(())
}

fn apply_snapshot_payload(state: &HostState, payload: &[u8]) -> io::Result<()> {
    let mut cursor = 0_usize;
    expect_bytes(payload, &mut cursor, REPLICATION_MAGIC)?;
    expect_byte(payload, &mut cursor, REPLICATION_SNAPSHOT)?;
    let count = usize::try_from(take_u32(payload, &mut cursor)?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid snapshot count"))?;
    for _ in 0..count {
        let sequence = take_u64(payload, &mut cursor)?;
        let time = Time::from_nanos(take_u64(payload, &mut cursor)?);
        let length = usize::try_from(take_u32(payload, &mut cursor)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid snapshot length"))?;
        let command =
            decode_command(take_bytes(payload, &mut cursor, length)?).map_err(io::Error::other)?;
        apply_replica(state, sequence, time, command)?;
    }
    if cursor != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot has trailing bytes",
        ));
    }
    Ok(())
}

fn read_wire_message(stream: &mut TcpStream) -> io::Result<WireMsg> {
    let mut buffer = Vec::with_capacity(1024);
    let mut scratch = [0_u8; 8 * 1024];
    loop {
        match decode_peer_frame(&buffer) {
            Ok((message, _)) => return Ok(message),
            Err(cc_env::FrameError::Incomplete) => {}
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
        let read = stream.read(&mut scratch)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed before a complete frame",
            ));
        }
        buffer.extend_from_slice(&scratch[..read]);
        if buffer.len() > cc_env::MAX_PEER_FRAME + 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer frame exceeds host buffer limit",
            ));
        }
    }
}

fn expect_bytes(input: &[u8], cursor: &mut usize, expected: &[u8]) -> io::Result<()> {
    let actual = take_bytes(input, cursor, expected.len())?;
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication magic",
        ))
    }
}

fn expect_byte(input: &[u8], cursor: &mut usize, expected: u8) -> io::Result<()> {
    let actual = *take_bytes(input, cursor, 1)?
        .first()
        .expect("one-byte replication field");
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication message tag",
        ))
    }
}

fn take_bytes<'a>(input: &'a [u8], cursor: &mut usize, length: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "replication length overflow"))?;
    let bytes = input.get(*cursor..end).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated replication message",
        )
    })?;
    *cursor = end;
    Ok(bytes)
}

fn take_u32(input: &[u8], cursor: &mut usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(
        take_bytes(input, cursor, 4)?
            .try_into()
            .expect("four-byte replication field"),
    ))
}

fn take_u64(input: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        take_bytes(input, cursor, 8)?
            .try_into()
            .expect("eight-byte replication field"),
    ))
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
    let config = flag(args, "--config")
        .map(|path| read_config(Path::new(&path)))
        .transpose()?;
    let action = args
        .iter()
        .find(|arg| matches!(arg.as_str(), "status" | "members" | "snapshot"))
        .map(String::as_str)
        .unwrap_or("status");
    match action {
        "members" => {
            let members = config
                .as_ref()
                .map(|value| {
                    value
                        .peers
                        .iter()
                        .map(|peer| format!("n{}={}", peer.id, peer.address))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_else(|| String::from("unknown"));
            println!("RAFT.MEMBERS addr={address} voters={members} learners=none joint=false")
        }
        "snapshot" => println!("RAFT.SNAPSHOT addr={address} state=available checkpoint=0"),
        _ => {
            let (resolved, response) = request_info_follow(&address)?;
            println!("RAFT.STATUS requested={address} resolved={resolved} {response}");
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct Config {
    id: u64,
    data_dir: PathBuf,
    listen_client: String,
    listen_peer: String,
    peers: Vec<Peer>,
}

fn validate_identity(config: &Config) -> io::Result<()> {
    let marker = config.data_dir.join("node.json");
    let text = fs::read_to_string(&marker).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("data-dir identity marker {}: {error}", marker.display()),
        )
    })?;
    let marker_id = text
        .split_once("\"id\":")
        .and_then(|(_, rest)| rest.split(|byte: char| !byte.is_ascii_digit()).next())
        .and_then(|value| value.parse::<u64>().ok());
    if marker_id != Some(config.id) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "data-dir identity mismatch: config id={} marker id={marker_id:?}",
                config.id
            ),
        ));
    }
    if !text.contains(&format!("\"cluster\":\"")) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "data-dir identity marker has no cluster",
        ));
    }
    Ok(())
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
    let peer_addresses = value_after(&text, "peer_nodes")
        .unwrap_or_else(|| listen_peer.clone())
        .split(',')
        .filter(|address| !address.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let peers = peer_addresses
        .into_iter()
        .enumerate()
        .map(|(index, address)| Peer {
            id: u64::try_from(index + 1).unwrap_or(u64::MAX),
            address,
        })
        .collect();
    Ok(Config {
        id,
        data_dir,
        listen_client,
        listen_peer,
        peers,
    })
}

fn request_info(address: &str) -> io::Result<String> {
    let socket = address
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid client address"))?;
    let mut stream = TcpStream::connect_timeout(&socket, StdDuration::from_secs(2))?;
    stream.set_read_timeout(Some(StdDuration::from_secs(2)))?;
    stream.write_all(&encode(&RespValue::Array(vec![RespValue::Bulk(Some(
        b"INFO".to_vec(),
    ))])))?;
    let response = read_resp_value(&mut stream)?;
    Ok(match response {
        RespValue::Bulk(Some(value)) => String::from_utf8_lossy(&value)
            .replace('\r', "")
            .replace('\n', " "),
        RespValue::Simple(value) => value,
        RespValue::Error(value) => value,
        other => format!("{other:?}"),
    })
}

fn request_info_follow(address: &str) -> io::Result<(String, String)> {
    let mut current = address.to_owned();
    for _ in 0..4 {
        let response = request_info(&current)?;
        let role = info_field(&response, "role");
        let Some(next) = info_field(&response, "leader_client_addr") else {
            return Ok((current, response));
        };
        if role.as_deref() != Some("follower") || next == current {
            return Ok((current, response));
        }
        current = next;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "admin leader redirect exceeded hop limit",
    ))
}

fn info_field(response: &str, key: &str) -> Option<String> {
    let marker = format!("{key}:");
    response
        .split_whitespace()
        .find_map(|field| field.strip_prefix(&marker).map(str::to_owned))
}

fn read_resp_value(stream: &mut TcpStream) -> io::Result<RespValue> {
    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 4 * 1024];
    loop {
        match parse(&buffer) {
            Ok((value, _)) => return Ok(value),
            Err(cc_resp::RespError::Incomplete) => {}
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
        let read = stream.read(&mut scratch)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before a complete RESP value",
            ));
        }
        buffer.extend_from_slice(&scratch[..read]);
        if buffer.len() > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "admin response exceeds frame limit",
            ));
        }
    }
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

fn sync_directory(path: &Path) -> io::Result<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()
}

fn fatal_disk(reason: &str) -> ! {
    eprintln!("ccdb fatal disk error: {reason}");
    std::process::abort()
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
        if std::env::var_os("CCDB_FAIL_ENOSPC").is_some() {
            fatal_disk("ENOSPC fault shim");
        }
        if std::env::var_os("CCDB_FAIL_FSYNC").is_some() {
            fatal_disk("fsync fault shim");
        }
        self.file
            .sync_data()
            .unwrap_or_else(|error| fatal_disk(&format!("fsync failed: {error}")));
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_guard_rejects_a_mismatched_node_marker() {
        let directory = env::temp_dir().join(format!(
            "cc-node-identity-{}-{}",
            std::process::id(),
            process_time().as_nanos()
        ));
        fs::create_dir_all(&directory).expect("identity test directory");
        fs::write(
            directory.join("node.json"),
            b"{\"cluster\":\"test\",\"id\":1}\n",
        )
        .expect("identity marker");
        sync_directory(&directory).expect("directory fsync");
        let config = Config {
            id: 1,
            data_dir: directory.clone(),
            listen_client: String::from("127.0.0.1:7101"),
            listen_peer: String::from("127.0.0.1:7201"),
            peers: vec![Peer {
                id: 1,
                address: String::from("127.0.0.1:7201"),
            }],
        };
        assert!(validate_identity(&config).is_ok());
        let mut mismatched = config.clone();
        mismatched.id = 2;
        assert_eq!(
            validate_identity(&mismatched)
                .expect_err("mismatched identity must be rejected")
                .kind(),
            io::ErrorKind::InvalidData
        );
        fs::remove_dir_all(directory).expect("remove identity test directory");
    }

    #[test]
    fn replication_write_payload_round_trips_and_rejects_trailing_bytes() {
        let command = KvCommand::Set {
            key: b"course".to_vec(),
            value: b"fixture".to_vec(),
            ttl: None,
        };
        let payload = encode_replication_write(7, Time::from_nanos(11), &command)
            .expect("encode replication write");
        assert_eq!(
            decode_replication_write(&payload).expect("decode replication write"),
            (7, Time::from_nanos(11), command)
        );
        let mut malformed = payload;
        malformed.push(0);
        assert!(decode_replication_write(&malformed).is_err());
    }

    #[test]
    fn malformed_peer_and_resp_inputs_are_bounded() {
        let malformed_peer_inputs = [
            Vec::new(),
            vec![0xff; 14],
            vec![
                b'C', b'C', b'P', b'F', 1, 0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0,
            ],
        ];
        for input in malformed_peer_inputs {
            assert!(decode_peer_frame(&input).is_err());
        }
        let malformed_resp_inputs = [
            b"*1025\r\n".to_vec(),
            b"$4194305\r\n".to_vec(),
            b"?not-resp\r\n".to_vec(),
            vec![b'*', b'1', b'\r', b'\n', b'*', b'1', b'\r', b'\n'],
        ];
        for input in malformed_resp_inputs {
            assert!(parse(&input).is_err());
        }
    }

    #[test]
    fn parser_fuzz_corpus_is_total_and_bounded() {
        let mut state = 0xfeed_cafe_d15e_a5e5_u64;
        for round in 0..2_048 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let peer_len = (state as usize) % 1_024;
            let mut peer = Vec::with_capacity(peer_len);
            for _ in 0..peer_len {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                peer.push((state >> 32) as u8);
            }
            let peer_result = std::panic::catch_unwind(|| decode_peer_frame(&peer));
            assert!(peer_result.is_ok(), "peer parser panicked at round {round}");

            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let resp_len = (state as usize) % 1_024;
            let mut resp = Vec::with_capacity(resp_len);
            for _ in 0..resp_len {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                resp.push((state >> 32) as u8);
            }
            let resp_result = std::panic::catch_unwind(|| parse(&resp));
            assert!(resp_result.is_ok(), "RESP parser panicked at round {round}");
            if let Ok(Ok((_, used))) = resp_result {
                assert!(used <= resp.len(), "RESP parser overread at round {round}");
            }
        }
    }
}
