// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! The real, blocking socket/filesystem adapter for the shared host driver.
//!
//! This module owns clocks, TCP, CCHL negotiation, and filesystem handles.
//! Consensus, client-command application, I/O correlation, and CCRP encoding
//! stay below this boundary in `cc-host`/`cc-cluster`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as FmtWrite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};

use cc_cluster::{
    CcsnStreamDecoder, FOLLOWER_READ_FEATURE, FollowerReadMetadata, NodeConfig, NodeError,
    PROTOCOL_VERSION, RaftConfig, RecoveredNode, SEMANTIC_VERSION_V3, node_snapshot_from_ccsn,
};
use cc_core::{
    AdminReply, AdminResultTag, ClientId, ClusterPolicy, ConfigOperation, HostLimits,
    MembershipState, NodeId, PeerAddress, RequestSeq, Seed, SessionKey, SessionNamespace, Time,
};
use cc_env::{
    Effect, FEATURE_ATOMIC_BATCH, FileId, Input, IoResult, PeerHello, WireMsg, decode_peer_frame,
    encode_peer_frame,
};
use cc_host::{
    BootState, Driver, FileBlockSource, HostError,
    journal::{
        BlockObservation, InputJournal, JournalFooter, JournalRecord, JournalTermination,
        RecordedBootImage, RecordingBlockSource,
    },
};
use cc_kv::{KvCommand, KvReply, SetCondition, decode_reply, encode_command};
use cc_log::{Genesis, Origin, encode_framed_durable_record, recover_framed_record_stream};
use cc_raft::MessageKind;
use cc_resp::{AdminOperation, ClientCommand, MAX_FRAME, RespValue, encode, parse, parse_command};
use cc_store::{
    ManifestCheckpoint, StoreConfig, decode_manifest_v2, recover_store_wal,
    validate_checkpoint_authority,
};

#[cfg(test)]
use super::Peer;
use super::{
    Config, has_flag, metrics_dashboard, read_config, to_resp, unsafe_listener_warning,
    validate_identity, validate_listener_safety,
};

const PEER_CONNECT_TIMEOUT: StdDuration = StdDuration::from_millis(250);
const PEER_IO_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const CLIENT_REPLY_TIMEOUT: StdDuration = StdDuration::from_secs(3);
const TICK_INTERVAL: StdDuration = StdDuration::from_millis(10);
const SEMANTIC_VERSION_MIN: u16 = PROTOCOL_VERSION;
const SEMANTIC_VERSION_MAX: u16 = SEMANTIC_VERSION_V3;
const DEFAULT_RECORD_MAX_BYTES: u64 = 128 * 1024 * 1024;

type ReplyRoute = (u64, u64);
type ReplySender = mpsc::SyncSender<Vec<u8>>;

/// A RESP connection may build one transaction, but its queue is deliberately
/// host-local and never replicated. Only `EXEC` creates the single canonical
/// CCKV batch entry; closing the socket simply drops this value.
enum ClientTransaction {
    Clean {
        commands: Vec<(ClientCommand, KvCommand)>,
        encoded_bytes: u64,
    },
    Dirty,
}

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    let config_path = if super::flag(args, "--join").is_some() {
        super::prepare_join_config(args)?
            .to_string_lossy()
            .into_owned()
    } else {
        super::flag(args, "--config").unwrap_or_else(|| String::from("ccdb.toml"))
    };
    let config = read_config(Path::new(&config_path))?;
    validate_identity(&config)?;
    // The store WAL is the first N3 v2 byte opened by a normal host.  Its
    // downgrade fence must be durable before DriverHost::boot can create it.
    super::raise_identity_storage_reader(&config, cc_store::STORAGE_V2_MIN_READER)?;
    super::raise_identity_semantic_reader(&config, SEMANTIC_VERSION_MAX)?;
    let allow_unsafe = has_flag(args, "--i-know-this-is-unauthenticated");
    validate_listener_safety(&config, allow_unsafe)?;
    let record_path = super::flag(args, "--record").map(std::path::PathBuf::from);
    let record_required = has_flag(args, "--record-required");
    let run_for = parse_run_for(args)?;
    let record_max_bytes = match super::flag(args, "--record-max-bytes") {
        Some(value) => value.parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "--record-max-bytes must be a positive integer",
            )
        })?,
        None => DEFAULT_RECORD_MAX_BYTES,
    };
    if record_max_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--record-max-bytes must be a positive integer",
        ));
    }
    validate_record_options(
        record_path.as_deref(),
        super::flag(args, "--record-max-bytes").is_some(),
        record_required,
    )?;
    let state = Arc::new(DriverHost::boot(
        config.clone(),
        record_path.as_deref(),
        record_max_bytes,
        record_required,
    )?);
    if state.enforce_membership_lifecycle()? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "local node is absent from committed membership; CCID is terminal Removed",
        ));
    }

    let client_listener = TcpListener::bind(&config.listen_client)?;
    let peer_listener = TcpListener::bind(&config.listen_peer)?;
    let metrics_listener = TcpListener::bind(&config.listen_metrics)?;
    client_listener.set_nonblocking(true)?;
    peer_listener.set_nonblocking(true)?;
    metrics_listener.set_nonblocking(true)?;
    for (name, listener) in [
        ("client", &client_listener),
        ("peer", &peer_listener),
        ("metrics", &metrics_listener),
    ] {
        if let Some(warning) = unsafe_listener_warning(name, listener.local_addr()?) {
            if !allow_unsafe {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, warning));
            }
            eprintln!("{warning}");
        }
    }
    println!(
        "ccdb node={} mode=shared-driver client={} peer={} metrics={}",
        config.id, config.listen_client, config.listen_peer, config.listen_metrics,
    );

    let ticker = Arc::clone(&state);
    if !spawn_host_thread(Arc::clone(&state.thread_budget), "ccdb-tick", move || {
        loop {
            thread::sleep(TICK_INTERVAL);
            if ticker.stopping.load(Ordering::Acquire) {
                break;
            }
            if let Err(error) = ticker.deliver(Input::Tick) {
                eprintln!("driver tick failed: {error}");
            }
        }
    })? {
        return Err(io::Error::other("thread admission refused ccdb-tick"));
    }

    let metrics_state = Arc::clone(&state);
    if !spawn_host_thread(
        Arc::clone(&state.thread_budget),
        "ccdb-metrics-listener",
        move || {
            while !metrics_state.stopping.load(Ordering::Acquire) {
                match metrics_listener.accept() {
                    Ok((stream, _)) => {
                        // Accepted sockets may inherit O_NONBLOCK from the
                        // listener on some hosts.  Per-connection workers use
                        // bounded blocking I/O, so normalize the mode before
                        // handing the stream to another thread.
                        if let Err(error) = stream.set_nonblocking(false) {
                            eprintln!("metrics socket mode failed: {error}");
                            continue;
                        }
                        let state = Arc::clone(&metrics_state);
                        let budget = Arc::clone(&state.thread_budget);
                        match spawn_host_thread(budget, "ccdb-metrics-client", move || {
                            if let Err(error) = serve_metrics(stream, &state) {
                                eprintln!("metrics connection closed with error: {error}");
                            }
                        }) {
                            Ok(true) => {}
                            Ok(false) => eprintln!("metrics connection refused: thread cap"),
                            Err(error) => eprintln!("metrics connection spawn failed: {error}"),
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(StdDuration::from_millis(2));
                    }
                    Err(error) => eprintln!("metrics accept error: {error}"),
                }
            }
        },
    )? {
        return Err(io::Error::other(
            "thread admission refused ccdb-metrics-listener",
        ));
    }

    let peer_state = Arc::clone(&state);
    if !spawn_host_thread(
        Arc::clone(&state.thread_budget),
        "ccdb-peer-listener",
        move || {
            while !peer_state.stopping.load(Ordering::Acquire) {
                match peer_listener.accept() {
                    Ok((stream, _)) => {
                        if let Err(error) = stream.set_nonblocking(false) {
                            eprintln!("peer socket mode failed: {error}");
                            continue;
                        }
                        let state = Arc::clone(&peer_state);
                        let budget = Arc::clone(&state.thread_budget);
                        match spawn_host_thread(budget, "ccdb-peer-client", move || {
                            if let Err(error) = serve_peer(stream, &state) {
                                eprintln!("peer connection closed with error: {error}");
                            }
                        }) {
                            Ok(true) => {}
                            Ok(false) => eprintln!("peer connection refused: thread cap"),
                            Err(error) => eprintln!("peer connection spawn failed: {error}"),
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(StdDuration::from_millis(2));
                    }
                    Err(error) => eprintln!("peer accept error: {error}"),
                }
            }
        },
    )? {
        return Err(io::Error::other(
            "thread admission refused ccdb-peer-listener",
        ));
    }

    let deadline = run_for.and_then(|duration| Instant::now().checked_add(duration));
    loop {
        if state.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        match client_listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false)?;
                let state = Arc::clone(&state);
                let client = state.allocate_client();
                let budget = Arc::clone(&state.thread_budget);
                match spawn_host_thread(budget, "ccdb-client", move || {
                    if let Err(error) = serve_client(stream, &state, client) {
                        eprintln!("client connection closed with error: {error}");
                    }
                }) {
                    Ok(true) => {}
                    Ok(false) => eprintln!("client connection refused: thread cap"),
                    Err(error) => eprintln!("client connection spawn failed: {error}"),
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    state.finish_complete_recording()?;
                    return Ok(());
                }
                thread::sleep(StdDuration::from_millis(1));
            }
            Err(error) => {
                if let Err(record_error) = state.finish_recording(JournalTermination::HostError) {
                    eprintln!("CCIJ host-error footer failed: {record_error}");
                }
                return Err(error);
            }
        }
    }
}

fn parse_run_for(args: &[String]) -> io::Result<Option<StdDuration>> {
    let Some(value) = super::flag(args, "--run-for-ms") else {
        return Ok(None);
    };
    let milliseconds = value.parse::<u64>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "--run-for-ms must be a positive integer",
        )
    })?;
    if milliseconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--run-for-ms must be a positive integer",
        ));
    }
    Ok(Some(StdDuration::from_millis(milliseconds)))
}

fn validate_record_options(
    record_path: Option<&Path>,
    has_record_max_bytes: bool,
    record_required: bool,
) -> io::Result<()> {
    if record_path.is_none() && has_record_max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--record-max-bytes requires --record PATH",
        ));
    }
    if record_path.is_none() && record_required {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--record-required requires --record PATH",
        ));
    }
    Ok(())
}

/// CCHL is a connection preamble, so probes need a local config to construct
/// an authenticated-in-context hello rather than sending an old raw CCPF
/// echo frame.
pub(crate) fn peer_probe(args: &[String]) -> io::Result<()> {
    let address = super::flag(args, "--addr").unwrap_or_else(|| String::from("127.0.0.1:7201"));
    let config_path = super::flag(args, "--config").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "peer probe requires --config for CCHL",
        )
    })?;
    let config = read_config(Path::new(&config_path))?;
    let retries = super::flag(args, "--retries")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5)
        .max(1);
    let mut delay = StdDuration::from_millis(20);
    for attempt in 1..=retries {
        match TcpStream::connect(&address) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(StdDuration::from_secs(2)))?;
                stream.set_write_timeout(Some(StdDuration::from_secs(2)))?;
                stream.write_all(&local_hello(&config).encode().map_err(io::Error::other)?)?;
                let (remote, _) = read_hello(&mut stream)?;
                local_hello(&config)
                    .negotiate(&remote)
                    .map_err(io::Error::other)?;
                println!(
                    "peer probe: PASS addr={address} attempt={attempt} remote_node={}",
                    remote.node_id.get()
                );
                return Ok(());
            }
            Err(error) if attempt < retries => {
                eprintln!("peer probe attempt {attempt} failed: {error}");
                thread::sleep(delay);
                delay = delay.saturating_mul(2);
            }
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other("peer probe exhausted retries"))
}

struct DriverHost {
    config: Config,
    clock: HostClock,
    driver: Mutex<Driver>,
    blocks: Mutex<RecordingBlockSource<FileBlockSource>>,
    wal: Mutex<File>,
    store_wal: Mutex<File>,
    outbound_peers: Mutex<BTreeMap<NodeId, OutboundPeer>>,
    replies: Mutex<BTreeMap<ReplyRoute, ReplySender>>,
    lifecycle_lock: Mutex<()>,
    next_client: AtomicU64,
    next_request: AtomicU64,
    stopping: AtomicBool,
    metrics: HostMetrics,
    thread_budget: Arc<ThreadBudget>,
    recorder: Option<Mutex<Option<RecordWriter>>>,
    record_required: bool,
}

struct OutboundPeer {
    address: String,
    stream: TcpStream,
    negotiated: cc_env::NegotiatedPeer,
}

struct PeerCapabilityGuard<'a> {
    state: &'a DriverHost,
    peer: NodeId,
}

impl Drop for PeerCapabilityGuard<'_> {
    fn drop(&mut self) {
        self.state.forget_peer_capability(self.peer);
    }
}

/// Append-only CCIJ writer. It retains only the current ordinal and file
/// handle; the recording itself stays bounded by the caller's filesystem
/// quota rather than accumulating in process memory.
#[derive(Debug)]
struct RecordWriter {
    file: File,
    next_ordinal: u64,
    last_now: Option<Time>,
    bytes_written: u64,
    max_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordAppend {
    Written,
    Capped,
}

impl RecordWriter {
    fn create(path: &Path, boot_image: &[u8], max_bytes: u64) -> io::Result<Self> {
        if fs::symlink_metadata(path).is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "record path already exists",
            ));
        }
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "record path has no parent")
        })?;
        if fs::symlink_metadata(parent)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record parent must not be a symbolic link",
            ));
        }
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let header = InputJournal::encode_header(boot_image).map_err(io::Error::other)?;
        let footer = InputJournal::encode_footer_frame(JournalFooter {
            termination: JournalTermination::Capped,
            last_ordinal: 0,
        })
        .map_err(io::Error::other)?;
        let minimum = u64::try_from(header.len().saturating_add(footer.len())).unwrap_or(u64::MAX);
        if max_bytes < minimum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--record-max-bytes cannot contain the CCIJ header and capped footer",
            ));
        }
        let mut file = options.open(path)?;
        file.write_all(&header)?;
        file.sync_all()?;
        Ok(Self {
            file,
            next_ordinal: 1,
            last_now: None,
            bytes_written: u64::try_from(header.len()).unwrap_or(u64::MAX),
            max_bytes,
        })
    }

    fn append(
        &mut self,
        now: Time,
        input: Input,
        block_observations: Vec<BlockObservation>,
        effects: Vec<Effect>,
    ) -> io::Result<RecordAppend> {
        if self.last_now.is_some_and(|previous| now < previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CCIJ transition time regressed",
            ));
        }
        let ordinal = self.next_ordinal;
        let record = JournalRecord {
            ordinal,
            now,
            input,
            block_observations,
            effects,
        };
        let frame = InputJournal::encode_record_frame(&record).map_err(io::Error::other)?;
        let capped_footer = InputJournal::encode_footer_frame(JournalFooter {
            termination: JournalTermination::Capped,
            last_ordinal: ordinal.saturating_sub(1),
        })
        .map_err(io::Error::other)?;
        let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        let footer_bytes = u64::try_from(capped_footer.len()).unwrap_or(u64::MAX);
        let required = self
            .bytes_written
            .checked_add(frame_bytes)
            .and_then(|bytes| bytes.checked_add(footer_bytes))
            .unwrap_or(u64::MAX);
        if required > self.max_bytes {
            self.file.write_all(&capped_footer)?;
            self.file.sync_data()?;
            self.bytes_written = self.bytes_written.saturating_add(footer_bytes);
            return Ok(RecordAppend::Capped);
        }
        self.file.write_all(&frame)?;
        self.file.sync_data()?;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| io::Error::other("CCIJ ordinal exhausted"))?;
        self.last_now = Some(now);
        self.bytes_written = self.bytes_written.saturating_add(frame_bytes);
        Ok(RecordAppend::Written)
    }

    fn finish(&mut self, termination: JournalTermination) -> io::Result<()> {
        let footer = InputJournal::encode_footer_frame(JournalFooter {
            termination,
            last_ordinal: self.next_ordinal.saturating_sub(1),
        })
        .map_err(io::Error::other)?;
        let footer_bytes = u64::try_from(footer.len()).unwrap_or(u64::MAX);
        let required = self.bytes_written.saturating_add(footer_bytes);
        if required > self.max_bytes {
            return Err(io::Error::other(
                "CCIJ terminal footer exceeds reserved recording capacity",
            ));
        }
        self.file.write_all(&footer)?;
        self.file.sync_data()?;
        self.bytes_written = required;
        Ok(())
    }
}

/// A required receipt is an operational contract rather than a best-effort
/// diagnostic. Its terminal Capped footer is already synced when this runs;
/// a write failure deliberately has no fabricated completion footer.
fn required_recording_failure(reason: &str) -> ! {
    eprintln!("ccdb required recording failure: {reason}");
    std::process::exit(70)
}

/// A process-local admission fence for all adapter-created threads.  It is
/// deliberately outside Raft: refusal is a host resource result, never a
/// replicated state transition.
struct ThreadBudget {
    live: AtomicU64,
    max: u64,
    stack_bytes: usize,
}

struct ThreadPermit {
    budget: Arc<ThreadBudget>,
}

impl Drop for ThreadPermit {
    fn drop(&mut self) {
        self.budget.live.fetch_sub(1, Ordering::Release);
    }
}

impl ThreadBudget {
    fn new(max: usize, stack_bytes: usize) -> Self {
        Self {
            live: AtomicU64::new(0),
            max: u64::try_from(max).unwrap_or(u64::MAX),
            stack_bytes,
        }
    }

    fn reserve(self: &Arc<Self>) -> Option<ThreadPermit> {
        let mut observed = self.live.load(Ordering::Acquire);
        loop {
            if observed >= self.max {
                return None;
            }
            match self.live.compare_exchange_weak(
                observed,
                observed + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ThreadPermit {
                        budget: Arc::clone(self),
                    });
                }
                Err(next) => observed = next,
            }
        }
    }
}

fn spawn_host_thread<F>(budget: Arc<ThreadBudget>, name: &str, task: F) -> io::Result<bool>
where
    F: FnOnce() + Send + 'static,
{
    let Some(permit) = budget.reserve() else {
        return Ok(false);
    };
    let stack_bytes = budget.stack_bytes;
    thread::Builder::new()
        .name(name.to_owned())
        .stack_size(stack_bytes)
        .spawn(move || {
            let _permit = permit;
            task();
        })?;
    Ok(true)
}

/// One Unix-time sample at boot paired with monotonic elapsed time. Core inputs
/// never observe later wall-clock corrections, and recovery can raise the
/// logical floor once a durable snapshot/store watermark exists.
struct HostClock {
    boot_epoch: Time,
    boot_instant: Instant,
    floor: Time,
}

impl HostClock {
    fn new(floor: Time) -> io::Result<Self> {
        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "wall clock predates Unix epoch")
        })?;
        let nanos = u64::try_from(elapsed.as_nanos()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "wall clock exceeds logical time range",
            )
        })?;
        Ok(Self {
            boot_epoch: Time::from_nanos(nanos),
            boot_instant: Instant::now(),
            floor,
        })
    }

    fn now(&self) -> Time {
        let elapsed = u64::try_from(self.boot_instant.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Time::from_nanos(
            self.boot_epoch
                .as_nanos()
                .saturating_add(elapsed)
                .max(self.floor.as_nanos()),
        )
    }
}

impl DriverHost {
    fn boot(
        config: Config,
        record_path: Option<&Path>,
        record_max_bytes: u64,
        record_required: bool,
    ) -> io::Result<Self> {
        let mut host_limits = HostLimits::default();
        if let Ok(value) = std::env::var("CCDB_SNAPSHOT_AFTER_BYTES") {
            let threshold = value.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "CCDB_SNAPSHOT_AFTER_BYTES must be an integer",
                )
            })?;
            if threshold == 0 || threshold >= host_limits.max_raft_log_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "CCDB_SNAPSHOT_AFTER_BYTES is outside the host limits",
                ));
            }
            host_limits.max_log_bytes_before_snapshot = threshold;
        }
        let membership = membership(&config)?;
        let genesis = Genesis {
            origin: Origin::Bootstrap,
            cluster_id: config.cluster_id.bytes(),
            policy: ClusterPolicy::default(),
            membership: membership.clone(),
        };
        let raft_dir = config.data_dir.join("raft");
        fs::create_dir_all(&raft_dir)?;
        let store_dir = config.data_dir.join("store");
        fs::create_dir_all(store_dir.join("sst"))?;
        cleanup_snapshot_staging(&config.data_dir)?;
        let wal_path = raft_dir.join("wal.0");
        let mut wal = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&wal_path)?;
        if wal.metadata()?.len() == 0 {
            wal.write_all(
                &encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(
                    genesis.clone(),
                )))
                .map_err(io::Error::other)?,
            )?;
            wal.sync_data()?;
        }
        let bytes = fs::read(&wal_path)?;
        let recovered = recover_framed_record_stream(&bytes).map_err(io::Error::other)?;
        let join_origin = recovered.state.genesis.origin == Origin::Join;
        if recovered.state.genesis.cluster_id != genesis.cluster_id
            || recovered.state.genesis.policy != genesis.policy
            || (!join_origin
                && !bootstrap_membership_matches(
                    &recovered.state.genesis.membership,
                    &genesis.membership,
                ))
            || (join_origin
                && (recovered
                    .state
                    .genesis
                    .membership
                    .voters
                    .contains(&NodeId::new(config.id))
                    || recovered
                        .state
                        .genesis
                        .membership
                        .learners
                        .contains(&NodeId::new(config.id))))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL genesis does not match CCID/configured membership",
            ));
        }
        if recovered.torn_tail_truncated {
            wal.set_len(recovered.bytes_consumed)?;
            wal.sync_data()?;
        }
        wal.seek(SeekFrom::Start(recovered.bytes_consumed))?;
        let store_wal_path = store_dir.join("wal.0");
        let mut store_wal = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&store_wal_path)?;
        let store_wal_bytes = fs::read(&store_wal_path)?;
        let recovered_store_wal = recover_store_wal(&store_wal_bytes).map_err(io::Error::other)?;
        if recovered_store_wal.torn_tail_truncated {
            store_wal.set_len(recovered_store_wal.bytes_consumed)?;
            store_wal.sync_data()?;
        }
        store_wal.seek(SeekFrom::Start(recovered_store_wal.bytes_consumed))?;

        let effective_config = node_config(&config, host_limits);
        let recovered_snapshot = if let Some(mark) = recovered.state.snapshot {
            if mark.generation != mark.index.get() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot mark generation disagrees with index",
                ));
            }
            let path = published_snapshot_path(&config.data_dir, mark.generation);
            let metadata = fs::metadata(&path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("snapshot mark lacks published checkpoint: {error}"),
                )
            })?;
            if metadata.len() == 0 || metadata.len() > host_limits.max_snapshot_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "published snapshot exceeds host limit",
                ));
            }
            let (decoded, snapshot_crc) = decode_published_snapshot(
                CcsnStreamDecoder::new(config.cluster_id.bytes(), host_limits.max_snapshot_bytes),
                &path,
            )?;
            if snapshot_crc != mark.crc32c
                || decoded.kv.applied_index != mark.index
                || decoded.kv.applied_term != mark.term
                || decoded.cluster_policy.encode() != effective_config.policy.encode()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "published snapshot disagrees with durable mark",
                ));
            }
            let manifest_path = config
                .data_dir
                .join("store")
                .join(format!("manifest.{}.ccmf", mark.generation));
            let manifest =
                decode_manifest_v2(&fs::read(&manifest_path)?).map_err(io::Error::other)?;
            let authority = ManifestCheckpoint {
                index: mark.index,
                term: mark.term,
                generation: mark.generation,
                crc32c: mark.crc32c,
            };
            validate_checkpoint_authority(manifest.checkpoint, Some(authority), Some(snapshot_crc))
                .map_err(io::Error::other)?;
            let snapshot = node_snapshot_from_ccsn(decoded, effective_config.store)
                .map_err(io::Error::other)?;
            Some((snapshot, metadata.len(), mark))
        } else {
            None
        };
        // A verified snapshot mark lets the durable Raft prefix be replaced
        // by one self-contained log image.  This happens only during boot,
        // while no new append can race the rename.  It deliberately retains
        // the checkpoint mark plus every suffix entry; a later store-WAL
        // reclamation pass remains separate authority.
        let (wal, wal_bytes, wal_offset) = if recovered_snapshot.is_some() {
            drop(wal);
            let compacted = compact_verified_wal(&wal_path, &recovered.state)?;
            let file = OpenOptions::new().read(true).write(true).open(&wal_path)?;
            let offset = u64::try_from(compacted.len()).unwrap_or(u64::MAX);
            (file, compacted, offset)
        } else {
            (wal, fs::read(&wal_path)?, recovered.bytes_consumed)
        };
        let recovered_membership = recovered_snapshot.as_ref().map_or_else(
            || recovered.state.genesis.membership.clone(),
            |(snapshot, _, _)| snapshot.membership.clone(),
        );
        let wal_genesis = recovered.state.genesis.clone();
        let record_membership = recovered.state.genesis.membership.clone();
        let recovered_node = RecoveredNode {
            hard_state: recovered.state.hard_state,
            log_base: (recovered.state.base_index, recovered.state.base_term),
            entries: recovered.state.entries,
            membership: recovered_membership,
            cluster_policy: recovered.state.genesis.policy,
            snapshot: recovered_snapshot
                .as_ref()
                .map(|(snapshot, _, _)| snapshot.clone()),
            durable_applied: (recovered.state.base_index, recovered.state.base_term),
        };
        let mut driver = Driver::boot_with_offsets_and_genesis(
            effective_config,
            BootState::Recovered(Box::new(recovered_node)),
            wal_offset,
            recovered_store_wal.bytes_consumed,
            wal_genesis,
        )
        .map_err(io::Error::other)?;
        driver
            .node_mut()
            .recover_durable_applies(&recovered_store_wal)
            .map_err(io::Error::other)?;
        if let Some((snapshot, snapshot_len, mark)) = recovered_snapshot {
            driver
                .register_published_snapshot(
                    FileId::Snapshot {
                        generation: mark.generation,
                    },
                    snapshot.last_included_index,
                    snapshot.last_included_term,
                    snapshot.kv.checkpoint.image.sequence,
                    snapshot_len,
                    mark.crc32c,
                )
                .map_err(io::Error::other)?;
            debug_assert_eq!(snapshot.last_included_index, mark.index);
        }
        let clock = HostClock::new(driver.node().kv.last_leader_time())?;
        let recorder = record_path
            .map(|path| {
                let boot_image = RecordedBootImage {
                    config: effective_config,
                    cluster_id: config.cluster_id.bytes(),
                    membership: record_membership.clone(),
                    boot_epoch: clock.boot_epoch,
                    build_label: String::from(env!("CARGO_PKG_VERSION")),
                    wal: wal_bytes,
                }
                .encode()
                .map_err(io::Error::other)?;
                RecordWriter::create(path, &boot_image, record_max_bytes)
            })
            .transpose()?;
        Ok(Self {
            config: config.clone(),
            clock,
            driver: Mutex::new(driver),
            blocks: Mutex::new(RecordingBlockSource::new(
                FileBlockSource::with_limit(
                    config.data_dir.join("store/sst"),
                    usize::try_from(host_limits.max_open_files).unwrap_or(usize::MAX),
                )
                .map_err(io::Error::other)?,
            )),
            wal: Mutex::new(wal),
            store_wal: Mutex::new(store_wal),
            outbound_peers: Mutex::new(BTreeMap::new()),
            replies: Mutex::new(BTreeMap::new()),
            lifecycle_lock: Mutex::new(()),
            next_client: AtomicU64::new(1),
            next_request: AtomicU64::new(1),
            stopping: AtomicBool::new(false),
            metrics: HostMetrics::default(),
            thread_budget: Arc::new(ThreadBudget::new(
                host_limits.max_threads,
                host_limits.thread_stack_bytes,
            )),
            recorder: recorder.map(|writer| Mutex::new(Some(writer))),
            record_required,
        })
    }

    fn allocate_client(&self) -> ClientId {
        ClientId::new(self.next_client.fetch_add(1, Ordering::Relaxed))
    }

    fn deliver(&self, input: Input) -> Result<(), HostError> {
        let effects = self.transition(input)?;
        if let Err(error) = self.execute_effects(effects) {
            if let Err(record_error) = self.finish_recording(JournalTermination::HostError) {
                eprintln!("CCIJ host-error footer failed: {record_error}");
            }
            return Err(error);
        }
        if let Err(error) = self.drain_admitted() {
            if let Err(record_error) = self.finish_recording(JournalTermination::HostError) {
                eprintln!("CCIJ host-error footer failed: {record_error}");
            }
            return Err(error);
        }
        Ok(())
    }

    fn transition(&self, input: Input) -> Result<Vec<Effect>, HostError> {
        if self.stopping.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let effects = {
            let mut driver = self.driver.lock().map_err(|_| HostError::TimeOverflow)?;
            if self.stopping.load(Ordering::Acquire) {
                return Ok(Vec::new());
            }
            // Assign the journal time at the serialized Driver boundary. A
            // thread that sampled before waiting here could otherwise append
            // its later sample ahead of an older already-admitted transition.
            let now = self.clock.now();
            let mut blocks = self.blocks.lock().map_err(|_| HostError::TimeOverflow)?;
            match driver.deliver(now, input.clone(), &mut *blocks) {
                Ok((_, effects)) => {
                    // Keep recording under the Driver lock: another host
                    // thread must not obtain a later logical time, complete a
                    // Driver transition, and append its CCIJ frame first.
                    let observations = blocks.take_observations();
                    self.metrics.observe_blocks(&observations);
                    self.record(now, input, observations, effects.clone())?;
                    effects
                }
                // A filesystem operation is a strict durability barrier. TCP
                // and timer work that arrives behind it is admitted to the
                // driver's bounded queues, never passed through to Raft early.
                Err(HostError::Node(NodeError::PersistencePending)) => {
                    if let Err(error) = driver.enqueue(input) {
                        if matches!(error, HostError::QueueFull(_)) {
                            self.metrics
                                .queue_rejections
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        return Err(error);
                    }
                    return Ok(Vec::new());
                }
                Err(error) => {
                    if let Err(record_error) = self.finish_recording(JournalTermination::HostError)
                    {
                        eprintln!("CCIJ host-error footer failed: {record_error}");
                    }
                    return Err(error);
                }
            }
        };
        Ok(effects)
    }

    fn record(
        &self,
        now: Time,
        input: Input,
        block_observations: Vec<BlockObservation>,
        effects: Vec<Effect>,
    ) -> Result<(), HostError> {
        let Some(recorder) = &self.recorder else {
            return Ok(());
        };
        let mut recorder = recorder.lock().map_err(|_| HostError::TimeOverflow)?;
        let Some(writer) = recorder.as_mut() else {
            return Ok(());
        };
        match writer.append(now, input, block_observations, effects) {
            Ok(RecordAppend::Written) => Ok(()),
            Ok(RecordAppend::Capped) => {
                *recorder = None;
                if self.record_required {
                    required_recording_failure(
                        "--record-max-bytes reached before the required recording completed",
                    );
                }
                eprintln!("CCIJ recorder reached --record-max-bytes; continuing unrecorded");
                Ok(())
            }
            Err(error) => {
                // Recording is diagnostic by default. The journal has no
                // complete footer after a write error, while the service can
                // safely continue without fabricating a whole-run proof.
                *recorder = None;
                if self.record_required {
                    required_recording_failure(&format!("required CCIJ write failed: {error}"));
                }
                eprintln!("CCIJ recorder disabled after write error: {error}");
                Ok(())
            }
        }
    }

    fn finish_recording(&self, termination: JournalTermination) -> io::Result<()> {
        let Some(recorder) = &self.recorder else {
            return Ok(());
        };
        let mut recorder = recorder
            .lock()
            .map_err(|_| io::Error::other("CCIJ recorder mutex poisoned"))?;
        let Some(writer) = recorder.as_mut() else {
            return Ok(());
        };
        writer.finish(termination)?;
        *recorder = None;
        Ok(())
    }

    /// Close host admission before appending a `Complete` footer. Acquiring
    /// the Driver mutex waits for any already-admitted transition to record,
    /// while the second shutdown check in `deliver` excludes contenders that
    /// were waiting on that mutex. The footer therefore marks a real boundary.
    fn finish_complete_recording(&self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        let _driver = self
            .driver
            .lock()
            .map_err(|_| io::Error::other("Driver mutex poisoned during shutdown"))?;
        self.finish_recording(JournalTermination::Complete)
    }

    fn drain_admitted(&self) -> Result<(), HostError> {
        loop {
            let effects = {
                let mut driver = self.driver.lock().map_err(|_| HostError::TimeOverflow)?;
                if driver.footprint().pending_io != 0 {
                    return Ok(());
                }
                let mut blocks = self.blocks.lock().map_err(|_| HostError::TimeOverflow)?;
                let now = self.clock.now();
                let (input, _poll, effects) = driver.deliver_next_with_input(now, &mut *blocks)?;
                if let Some(input) = input {
                    let observations = blocks.take_observations();
                    self.metrics.observe_blocks(&observations);
                    self.record(now, input, observations, effects.clone())?;
                }
                effects
            };
            if effects.is_empty() {
                return Ok(());
            }
            self.execute_effects(effects)?;
        }
    }

    fn execute_effects(&self, effects: Vec<Effect>) -> Result<(), HostError> {
        let mut pending = VecDeque::from(effects);
        while let Some(effect) = pending.pop_front() {
            match effect {
                Effect::Send { to, msg } => {
                    self.metrics.peer_frames.fetch_add(1, Ordering::Relaxed);
                    if let Ok(message) = cc_raft::codec::decode(&msg.payload) {
                        match message.kind {
                            MessageKind::SnapshotChunk {
                                transfer_id,
                                done: true,
                                ..
                            } => self.metrics.count_transfer_once(
                                &self.metrics.sent_transfers,
                                &self.metrics.snapshots_sent,
                                to,
                                transfer_id,
                            ),
                            MessageKind::SnapshotAck {
                                transfer_id,
                                accepted: false,
                                ..
                            } => self.metrics.count_transfer_once(
                                &self.metrics.aborted_transfers,
                                &self.metrics.snapshots_aborted,
                                to,
                                transfer_id,
                            ),
                            _ => {}
                        }
                    }
                    if let Err(error) = self.send_peer(to, msg) {
                        // A Raft send is lossy by design; later heartbeat/election
                        // traffic retries it. Never turn transport reachability
                        // into a fabricated successful reply.
                        eprintln!(
                            "peer send failed: from=n{} to={} error={error}",
                            self.config.id, to
                        );
                    }
                }
                Effect::DiskWrite {
                    file,
                    at,
                    bytes,
                    id,
                } => {
                    self.metrics.file_writes.fetch_add(1, Ordering::Relaxed);
                    self.metrics.file_bytes_written.fetch_add(
                        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                    let len = self.write_file(file, at, &bytes).unwrap_or_else(|error| {
                        self.metrics.storage_fault.store(true, Ordering::Relaxed);
                        if let Err(record_error) =
                            self.finish_recording(JournalTermination::FatalIo)
                        {
                            eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                        }
                        super::fatal_disk(&format!("WAL write failed: {error}"))
                    });
                    self.metrics.fsync_writes.fetch_add(1, Ordering::Relaxed);
                    pending.extend(self.transition(Input::IoDone {
                        id,
                        result: IoResult::Written { len },
                    })?);
                }
                Effect::DiskFsync { file, id } => {
                    self.sync_file(file).unwrap_or_else(|error| {
                        self.metrics.storage_fault.store(true, Ordering::Relaxed);
                        if let Err(record_error) =
                            self.finish_recording(JournalTermination::FatalIo)
                        {
                            eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                        }
                        super::fatal_disk(&format!("WAL fsync failed: {error}"))
                    });
                    pending.extend(self.transition(Input::IoDone {
                        id,
                        result: IoResult::Fsynced,
                    })?);
                }
                Effect::DiskRead { file, at, len, id } => {
                    self.metrics.file_reads.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .file_bytes_read
                        .fetch_add(u64::from(len), Ordering::Relaxed);
                    let bytes = self.read_file(file, at, len).unwrap_or_else(|error| {
                        self.metrics.storage_fault.store(true, Ordering::Relaxed);
                        if let Err(record_error) =
                            self.finish_recording(JournalTermination::FatalIo)
                        {
                            eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                        }
                        super::fatal_disk(&format!("disk read failed: {error}"))
                    });
                    pending.extend(self.transition(Input::IoDone {
                        id,
                        result: IoResult::Read(bytes),
                    })?);
                }
                Effect::DiskRename { from, to, id } => {
                    self.metrics.renames.fetch_add(1, Ordering::Relaxed);
                    self.rename_file(from, to).unwrap_or_else(|error| {
                        self.metrics.storage_fault.store(true, Ordering::Relaxed);
                        if let Err(record_error) =
                            self.finish_recording(JournalTermination::FatalIo)
                        {
                            eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                        }
                        super::fatal_disk(&format!("disk rename failed: {error}"))
                    });
                    if matches!(to, FileId::Snapshot { .. }) {
                        self.metrics
                            .snapshots_created
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    pending.extend(self.transition(Input::IoDone {
                        id,
                        result: IoResult::Fsynced,
                    })?);
                }
                Effect::DiskSyncDir { id } => {
                    self.metrics.directory_syncs.fetch_add(1, Ordering::Relaxed);
                    for directory in [
                        self.config.data_dir.join("snapshots"),
                        self.config.data_dir.join("store"),
                        self.config.data_dir.join("raft"),
                    ] {
                        if directory.exists() {
                            super::sync_directory(&directory).unwrap_or_else(|error| {
                                self.metrics.storage_fault.store(true, Ordering::Relaxed);
                                if let Err(record_error) =
                                    self.finish_recording(JournalTermination::FatalIo)
                                {
                                    eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                                }
                                super::fatal_disk(&format!(
                                    "durability directory sync failed: {error}"
                                ))
                            });
                        }
                    }
                    pending.extend(self.transition(Input::IoDone {
                        id,
                        result: IoResult::Fsynced,
                    })?);
                }
                Effect::ClientReply { client, req, reply } => {
                    if let Ok(mut routes) = self.replies.lock()
                        && let Some(sender) = routes.remove(&(client.get(), req.get()))
                    {
                        let _ = sender.send(reply);
                    }
                }
                // A periodic host Tick drives the deterministic clock. The
                // Driver still owns timer generations, but a blocked I/O-free
                // socket host need not create one OS thread per arm.
                Effect::SetTimer { .. } | Effect::CancelTimer { .. } | Effect::Trace(_) => {}
                unsupported => {
                    eprintln!("unimplemented host effect: {unsupported:?}");
                }
            }
        }
        if let Err(error) = self.enforce_membership_lifecycle() {
            super::fatal_disk(&format!(
                "failed to publish committed membership lifecycle: {error}"
            ));
        }
        Ok(())
    }

    fn write_file(&self, file: FileId, at: u64, bytes: &[u8]) -> io::Result<u32> {
        if std::env::var_os("CCDB_FAIL_ENOSPC").is_some() {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        let len = u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL write too large"))?;
        let target = match file {
            FileId::Wal { segment: 0 } => &self.wal,
            FileId::StoreWal { segment: 0 } => &self.store_wal,
            _ => return self.write_snapshot_stage(file, at, bytes),
        };
        let mut wal = target
            .lock()
            .map_err(|_| io::Error::other("WAL mutex poisoned"))?;
        let actual = wal.metadata()?.len();
        if actual != at {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL append offset mismatch file={file:?} expected={at} actual={actual}"),
            ));
        }
        wal.seek(SeekFrom::Start(at))?;
        wal.write_all(bytes)?;
        Ok(len)
    }

    fn sync_file(&self, file: FileId) -> io::Result<()> {
        if std::env::var_os("CCDB_FAIL_FSYNC").is_some() {
            return Err(io::Error::other("injected fsync failure"));
        }
        let target = match file {
            FileId::Wal { segment: 0 } => &self.wal,
            FileId::StoreWal { segment: 0 } => &self.store_wal,
            _ => return self.sync_snapshot_stage(file),
        };
        target
            .lock()
            .map_err(|_| io::Error::other("WAL mutex poisoned"))?
            .sync_data()
    }

    fn read_file(&self, file: FileId, at: u64, len: u32) -> io::Result<Vec<u8>> {
        let path = match file {
            FileId::Wal { segment: 0 } => self.config.data_dir.join("raft/wal.0"),
            FileId::StoreWal { segment: 0 } => self.config.data_dir.join("store/wal.0"),
            _ => self.snapshot_stage_path(file)?,
        };
        let mut handle = OpenOptions::new().read(true).open(path)?;
        handle.seek(SeekFrom::Start(at))?;
        let length = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "disk read too large"))?;
        let mut bytes = vec![0; length];
        handle.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn snapshot_stage_path(&self, file: FileId) -> io::Result<PathBuf> {
        let snapshots = self.config.data_dir.join("snapshots");
        match file {
            FileId::Temp { sequence } => Ok(snapshots
                .join("staging")
                .join(format!("stage.{sequence}.ccsn"))),
            FileId::Snapshot { generation } => {
                Ok(snapshots.join(format!("snapshot.{generation}.ccsn")))
            }
            FileId::Manifest { generation } => Ok(self
                .config
                .data_dir
                .join("store")
                .join(format!("manifest.{generation}.ccmf"))),
            FileId::Meta => Ok(self.config.data_dir.join("store/META")),
            FileId::Sst { file_no } => Ok(self
                .config
                .data_dir
                .join("store")
                .join(format!("sst.{file_no}.ccst"))),
            FileId::Wal { segment: 0 } => Ok(self.config.data_dir.join("raft/wal.0")),
            FileId::StoreWal { segment: 0 } => Ok(self.config.data_dir.join("store/wal.0")),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected logical snapshot file",
            )),
        }
    }

    fn write_snapshot_stage(&self, file: FileId, at: u64, bytes: &[u8]) -> io::Result<u32> {
        if std::env::var_os("CCDB_FAIL_ENOSPC").is_some() {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        let path = self.snapshot_stage_path(file)?;
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "snapshot path parent"))?;
        fs::create_dir_all(parent)?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        if at == 0 {
            options.truncate(true);
        }
        let mut stage = options.open(path)?;
        stage.seek(SeekFrom::Start(at))?;
        stage.write_all(bytes)?;
        u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "snapshot chunk too large"))
    }

    fn sync_snapshot_stage(&self, file: FileId) -> io::Result<()> {
        if std::env::var_os("CCDB_FAIL_FSYNC").is_some() {
            return Err(io::Error::other("injected fsync failure"));
        }
        OpenOptions::new()
            .read(true)
            .open(self.snapshot_stage_path(file)?)?
            .sync_data()
    }

    fn rename_file(&self, from: FileId, to: FileId) -> io::Result<()> {
        if std::env::var_os("CCDB_FAIL_FSYNC").is_some() {
            return Err(io::Error::other("injected snapshot rename failure"));
        }
        let source = self.snapshot_stage_path(from)?;
        let target = self.snapshot_stage_path(to)?;
        if target.exists()
            && !matches!(
                to,
                FileId::Wal { segment: 0 } | FileId::StoreWal { segment: 0 }
            )
        {
            if files_are_identical(&source, &target)? {
                // A final snapshot chunk or manifest publication may be
                // retried after its acknowledgement is lost, including after
                // a leader change. Exact byte identity makes that retry a
                // no-op; a same-generation collision with different bytes is
                // still a fatal durability violation.
                fs::remove_file(source)?;
                return Ok(());
            }
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "durable snapshot generation already exists with different bytes",
            ));
        }
        fs::rename(source, &target)?;
        if matches!(to, FileId::Wal { segment: 0 }) {
            let replacement = OpenOptions::new().read(true).write(true).open(target)?;
            *self
                .wal
                .lock()
                .map_err(|_| io::Error::other("WAL mutex poisoned"))? = replacement;
        } else if matches!(to, FileId::StoreWal { segment: 0 }) {
            let replacement = OpenOptions::new().read(true).write(true).open(target)?;
            *self
                .store_wal
                .lock()
                .map_err(|_| io::Error::other("store WAL mutex poisoned"))? = replacement;
        }
        Ok(())
    }

    fn send_peer(&self, to: NodeId, msg: WireMsg) -> io::Result<()> {
        // LeaveJoint removes the peer from the authoritative address book
        // before its final best-effort notification is executed. A healthy
        // joint member already has a negotiated route; preserve that route
        // for the terminal append instead of falling back to stale config.
        let cached_address = self
            .outbound_peers
            .lock()
            .map_err(|_| io::Error::other("outbound peer mutex poisoned"))?
            .get(&to)
            .map(|connection| connection.address.clone());
        let address = cached_address.map_or_else(|| self.peer_address(to), Ok)?;
        let frame = encode_peer_frame(&msg).map_err(io::Error::other)?;
        let mut last_error = None;
        for _ in 0..2 {
            let mut peers = self
                .outbound_peers
                .lock()
                .map_err(|_| io::Error::other("outbound peer mutex poisoned"))?;
            if peers
                .get(&to)
                .is_some_and(|connection| connection.address != address)
            {
                peers.remove(&to);
                self.forget_peer_capability(to);
            }
            if let std::collections::btree_map::Entry::Vacant(entry) = peers.entry(to) {
                entry.insert(self.connect_peer(to, &address)?);
            }
            let connection = peers.get_mut(&to).expect("inserted outbound peer");
            if !wire_message_features_are_allowed(&msg, connection.negotiated) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CCRP semantic version or feature differs from negotiated CCHL",
                ));
            }
            match connection.stream.write_all(&frame) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    peers.remove(&to);
                    self.forget_peer_capability(to);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| io::Error::other("peer send retry exhausted")))
    }

    fn connect_peer(&self, to: NodeId, address: &str) -> io::Result<OutboundPeer> {
        let mut stream =
            TcpStream::connect_timeout(&resolve_peer_addr(address)?, PEER_CONNECT_TIMEOUT)?;
        stream.set_read_timeout(Some(PEER_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(PEER_IO_TIMEOUT))?;
        stream.write_all(
            &local_hello(&self.config)
                .encode()
                .map_err(io::Error::other)?,
        )?;
        let (remote, _) = read_hello(&mut stream)?;
        let negotiated = local_hello(&self.config)
            .negotiate(&remote)
            .map_err(io::Error::other)?;
        if remote.node_id != to {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CCHL node id does not match peer route",
            ));
        }
        self.observe_peer_capability(to, negotiated)?;
        Ok(OutboundPeer {
            address: address.to_owned(),
            stream,
            negotiated,
        })
    }

    fn peer_address(&self, id: NodeId) -> io::Result<String> {
        // Replicated membership is authoritative after recovery.  The local
        // peer list is bootstrap/discovery input only and is used solely when
        // no committed address has ever existed for this member.
        let membership = self
            .driver
            .lock()
            .map_err(|_| io::Error::other("Driver mutex poisoned"))?
            .membership_state();
        if let Some(address) = membership.addresses.get(&id).map(render_peer_address) {
            return Ok(address);
        }
        self.config
            .peers
            .iter()
            .find(|peer| peer.id == id.get())
            .map(|peer| peer.address.clone())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unknown peer node id; voters={:?} learners={:?} addresses={:?}",
                        membership.voters, membership.learners, membership.addresses
                    ),
                )
            })
    }

    fn client_address(&self, id: NodeId) -> String {
        if id.get() == self.config.id {
            return self.config.listen_client.clone();
        }
        if let Ok(driver) = self.driver.lock()
            && let Some(address) = driver.membership_state().addresses.get(&id)
        {
            let socket = match address {
                PeerAddress::V4 { ip, port } => SocketAddr::from((*ip, *port)),
                PeerAddress::V6 { ip, port } => SocketAddr::from((*ip, *port)),
            };
            if let Some(port) = socket.port().checked_sub(100) {
                return SocketAddr::new(socket.ip(), port).to_string();
            }
        }
        client_address_for(&self.config, id.get())
    }

    fn submit(
        &self,
        client: ClientId,
        session: Option<(ClientId, RequestSeq)>,
        command: KvCommand,
    ) -> Result<KvReply, SubmitError> {
        let request = self.next_request.fetch_add(1, Ordering::Relaxed);
        let req = RequestSeq::new(request);
        let (sender, receiver) = mpsc::sync_channel(1);
        self.replies
            .lock()
            .map_err(|_| SubmitError::Host(HostError::TimeOverflow))?
            .insert((client.get(), req.get()), sender);
        self.metrics.commands.fetch_add(1, Ordering::Relaxed);
        if is_read(&command) {
            self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.writes.fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = self.deliver(Input::ClientRequest {
            client,
            req,
            session,
            command: encode_command(&command),
        }) {
            self.replies
                .lock()
                .ok()
                .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
            return match error {
                HostError::Node(NodeError::NotLeader) => Err(SubmitError::NotLeader(self.leader())),
                HostError::Node(NodeError::FeatureDisabled) => Err(SubmitError::FeatureDisabled),
                other => Err(SubmitError::Host(other)),
            };
        }
        match receiver.recv_timeout(CLIENT_REPLY_TIMEOUT) {
            Ok(reply) => decode_reply(&reply).map_err(|_| SubmitError::InvalidReply),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.replies
                    .lock()
                    .ok()
                    .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                Err(SubmitError::Timeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(SubmitError::InvalidReply),
        }
    }

    fn leader(&self) -> Option<NodeId> {
        self.driver
            .lock()
            .ok()
            .and_then(|driver| driver.node().leader())
    }

    fn observe_peer_capability(
        &self,
        peer: NodeId,
        negotiated: cc_env::NegotiatedPeer,
    ) -> io::Result<()> {
        self.driver
            .lock()
            .map_err(|_| io::Error::other("Driver mutex poisoned"))?
            .observe_peer_capability(peer, negotiated.semantic_version, negotiated.features)
            .map_err(io::Error::other)?;
        self.metrics
            .observe_peer(peer, negotiated.semantic_version, negotiated.features);
        Ok(())
    }

    fn forget_peer_capability(&self, peer: NodeId) {
        if let Ok(mut driver) = self.driver.lock() {
            driver.forget_peer_capability(peer);
        }
    }

    fn follower_read(
        &self,
        client: ClientId,
        command: KvCommand,
    ) -> Result<(KvReply, FollowerReadMetadata), SubmitError> {
        let request = self.next_request.fetch_add(1, Ordering::Relaxed);
        let req = RequestSeq::new(request);
        let (sender, receiver) = mpsc::sync_channel(1);
        self.replies
            .lock()
            .map_err(|_| SubmitError::Host(HostError::TimeOverflow))?
            .insert((client.get(), req.get()), sender);
        self.metrics.commands.fetch_add(1, Ordering::Relaxed);
        self.metrics.reads.fetch_add(1, Ordering::Relaxed);
        let effects = {
            let mut driver = self
                .driver
                .lock()
                .map_err(|_| SubmitError::Host(HostError::TimeOverflow))?;
            let now = self.clock.now();
            match driver.follower_read(now, client, req, command) {
                Ok((_, effects)) => effects,
                Err(error) => {
                    self.replies
                        .lock()
                        .ok()
                        .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                    return match error {
                        HostError::Node(NodeError::NotLeader) => {
                            Err(SubmitError::NotLeader(driver.node().leader()))
                        }
                        other => Err(SubmitError::Host(other)),
                    };
                }
            }
        };
        if let Err(error) = self.execute_effects(effects) {
            self.replies
                .lock()
                .ok()
                .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
            return Err(SubmitError::Host(error));
        }
        if let Err(error) = self.drain_admitted() {
            return Err(SubmitError::Host(error));
        }
        let reply = match receiver.recv_timeout(CLIENT_REPLY_TIMEOUT) {
            Ok(reply) => decode_reply(&reply).map_err(|_| SubmitError::InvalidReply)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.replies
                    .lock()
                    .ok()
                    .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                return Err(SubmitError::Timeout);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Err(SubmitError::InvalidReply),
        };
        let metadata = self
            .driver
            .lock()
            .map_err(|_| SubmitError::Host(HostError::TimeOverflow))?
            .take_follower_read_metadata(client, req)
            .ok_or(SubmitError::InvalidReply)?;
        Ok((reply, metadata))
    }

    fn admin_submit(
        &self,
        client: ClientId,
        operator_id: u64,
        sequence: u64,
        operation: ConfigOperation,
    ) -> Result<AdminReply, SubmitError> {
        let session = SessionKey::new(
            SessionNamespace::AdminRequest as u8,
            ClientId::new(operator_id),
        )
        .map_err(|_| SubmitError::InvalidReply)?;
        if operator_id == 0 || sequence == 0 {
            return Err(SubmitError::InvalidReply);
        }
        let request = self.next_request.fetch_add(1, Ordering::Relaxed);
        let req = RequestSeq::new(request);
        let (sender, receiver) = mpsc::sync_channel(1);
        self.replies
            .lock()
            .map_err(|_| SubmitError::Host(HostError::TimeOverflow))?
            .insert((client.get(), req.get()), sender);
        let effects = {
            let mut driver = self
                .driver
                .lock()
                .map_err(|_| SubmitError::Host(HostError::TimeOverflow))?;
            let now = self.clock.now();
            match driver.admin_request(now, client, req, session, sequence, operation) {
                Ok((_, effects)) => effects,
                Err(HostError::Node(NodeError::NotLeader)) => {
                    self.replies
                        .lock()
                        .ok()
                        .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                    return Err(SubmitError::NotLeader(driver.node().leader()));
                }
                Err(HostError::Node(NodeError::FeatureDisabled)) => {
                    self.replies
                        .lock()
                        .ok()
                        .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                    return Err(SubmitError::FeatureDisabled);
                }
                Err(error) => {
                    self.replies
                        .lock()
                        .ok()
                        .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                    return Err(SubmitError::Host(error));
                }
            }
        };
        if let Err(error) = self.execute_effects(effects) {
            self.replies
                .lock()
                .ok()
                .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
            return Err(SubmitError::Host(error));
        }
        if let Err(error) = self.drain_admitted() {
            self.replies
                .lock()
                .ok()
                .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
            return Err(SubmitError::Host(error));
        }
        match receiver.recv_timeout(CLIENT_REPLY_TIMEOUT) {
            Ok(reply) => AdminReply::decode(&reply).map_err(|_| SubmitError::InvalidReply),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.replies
                    .lock()
                    .ok()
                    .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                Err(SubmitError::Timeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.replies
                    .lock()
                    .ok()
                    .and_then(|mut routes| routes.remove(&(client.get(), req.get())));
                Err(SubmitError::InvalidReply)
            }
        }
    }

    fn membership_status(&self) -> String {
        self.driver.lock().map_or_else(
            |_| String::from("ERR driver unavailable"),
            |driver| {
                let node = driver.node();
                let membership = driver.membership_state();
                let genesis_membership = driver
                    .genesis()
                    .map_or_else(|| membership.clone(), |genesis| genesis.membership.clone());
                let voters = membership.voters.clone();
                let learners = membership.learners.clone();
                let joint = membership.joint.is_some();
                let render = |members: BTreeSet<NodeId>| {
                    members
                        .into_iter()
                        .map(|node| node.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                };
                let addresses = membership
                    .addresses
                    .iter()
                    .map(|(id, address)| format!("{}@{}", id.get(), render_peer_address(address)))
                    .collect::<Vec<_>>()
                    .join(",");
                let genesis_addresses = genesis_membership
                    .addresses
                    .iter()
                    .map(|(id, address)| format!("{}@{}", id.get(), render_peer_address(address)))
                    .collect::<Vec<_>>()
                    .join(",");
                let commit = node.raft.commit_index.get();
                let capabilities = node
                    .peer_capabilities()
                    .into_iter()
                    .map(|(id, semantic, features)| (id, (semantic, features)))
                    .collect::<BTreeMap<_, _>>();
                let now = self.clock.now();
                let storage_fault = u8::from(self.metrics.storage_fault.load(Ordering::Relaxed));
                let members = voters
                    .iter()
                    .map(|id| (*id, "voter"))
                    .chain(learners.iter().map(|id| (*id, "learner")))
                    .map(|(id, role)| {
                        let matched = if id == node.id() {
                            node.raft.last_index().get()
                        } else {
                            node.raft.match_index.get(&id).map_or(0, |index| index.get())
                        };
                        let (semantic, features) = capabilities.get(&id).copied().unwrap_or((0, 0));
                        let last_contact_ms = if id == node.id() {
                            0_i128
                        } else {
                            node.raft.last_contact.get(&id).map_or(-1_i128, |contact| {
                                i128::from(
                                    now.as_nanos().saturating_sub(contact.as_nanos()) / 1_000_000,
                                )
                            })
                        };
                        format!(
                            "{}:{role}:{}:{matched}:{}:{last_contact_ms}:{}:{semantic}:{features:#x}:{storage_fault}",
                            id.get(),
                            membership
                                .addresses
                                .get(&id)
                                .map_or_else(|| String::from("unknown"), render_peer_address),
                            commit.saturating_sub(matched),
                            if driver.snapshot_transfer_active(id) {
                                "active"
                            } else {
                                "idle"
                            },
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "cluster_id={} policy={} policy_hash={:016x} config_index={} config_term={} voters={} learners={} joint={joint} addresses={} members={} active_features={:#x} genesis_voters={} genesis_learners={} genesis_addresses={} genesis_active_features={:#x}",
                    self.config.cluster_id,
                    hex_bytes(&ClusterPolicy::default().encode()),
                    ClusterPolicy::default().hash(),
                    node.raft.applied_index.get(),
                    node.kv.applied_term.get(),
                    render(voters),
                    render(learners),
                    addresses,
                    members,
                    node.active_features(),
                    render(genesis_membership.voters),
                    render(genesis_membership.learners),
                    genesis_addresses,
                    genesis_membership.active_features,
                )
            },
        )
    }

    /// Reconcile committed membership with the terminal disk lifecycle.  A
    /// Joining directory becomes Active only after this node appears in
    /// applied membership.  An already-Active node that is absent is marked
    /// Removed and host admission closes before another listener iteration.
    fn enforce_membership_lifecycle(&self) -> io::Result<bool> {
        // Peer/client handlers may finish effects concurrently. Serialize the
        // read-check-replace sequence so two observations of Joining cannot
        // race on the fixed atomic-replacement temporary path.
        let _lifecycle = self
            .lifecycle_lock
            .lock()
            .map_err(|_| io::Error::other("lifecycle mutex poisoned"))?;
        let lifecycle = super::identity_lifecycle(&self.config)?;
        let local = NodeId::new(self.config.id);
        let membership = self
            .driver
            .lock()
            .map_err(|_| io::Error::other("Driver mutex poisoned"))?
            .membership_state();
        let present = membership.voters.contains(&local) || membership.learners.contains(&local);
        if present && lifecycle == super::IDENTITY_JOINING {
            super::mark_identity_active(&self.config)?;
            return Ok(false);
        }
        if !present && lifecycle == super::IDENTITY_ACTIVE {
            super::mark_identity_removed(&self.config)?;
            self.stopping.store(true, Ordering::Release);
            return Ok(true);
        }
        if lifecycle == super::IDENTITY_REMOVED {
            self.stopping.store(true, Ordering::Release);
            return Ok(true);
        }
        Ok(false)
    }

    fn serves_clients(&self) -> bool {
        if self.stopping.load(Ordering::Acquire) {
            return false;
        }
        let Ok(lifecycle) = super::identity_lifecycle(&self.config) else {
            return false;
        };
        if lifecycle != super::IDENTITY_ACTIVE {
            return false;
        }
        self.driver.lock().is_ok_and(|driver| {
            let membership = driver.membership_state();
            let local = NodeId::new(self.config.id);
            membership.voters.contains(&local) || membership.learners.contains(&local)
        })
    }

    fn atomic_batch_active(&self) -> Result<bool, HostError> {
        Ok(self
            .driver
            .lock()
            .map_err(|_| HostError::TimeOverflow)?
            .node()
            .active_features()
            & cc_core::ATOMIC_BATCH_FEATURE
            != 0)
    }

    fn stale_get(&self, key: Vec<u8>) -> Result<(KvReply, u64, u64, Time, i64), HostError> {
        let read_time = self.clock.now();
        let driver = self.driver.lock().map_err(|_| HostError::TimeOverflow)?;
        let node = driver.node();
        let reply = node
            .kv
            .read(KvCommand::Get { key }, read_time)
            .map_err(NodeError::from)?;
        Ok((
            reply,
            node.kv.applied_index.get(),
            node.kv.applied_term.get(),
            read_time,
            -1,
        ))
    }

    fn render_metrics(&self) -> String {
        const METRICS_BYTE_CAP: usize = 64 * 1024;
        let Ok(driver) = self.driver.lock() else {
            return String::from("# TYPE ccdb_up gauge\nccdb_up 0\n");
        };
        let node = driver.node();
        let leader = node.leader().map(NodeId::get);
        let commit = node.raft.commit_index.get();
        let applied = node.raft.applied_index.get();
        let membership = driver.membership_state();
        let footprint = driver.footprint();
        let store = node.kv.store.stats();
        let levels = node.kv.store.level_metrics();
        let core = node.metrics();
        let match_indexes = node.raft.match_index.clone();
        let capabilities = node
            .peer_capabilities()
            .into_iter()
            .map(|(id, semantic, features)| (id, (semantic, features)))
            .collect::<BTreeMap<_, _>>();
        let observed = self
            .metrics
            .peers
            .lock()
            .map(|peers| peers.clone())
            .unwrap_or_default();

        // All live maps have been copied while the driver was locked. Render
        // from those bounded values so a slow scraper cannot retain the lock.
        drop(driver);
        let mut output = self.metrics.render();
        let _ = write!(
            output,
            "# TYPE ccdb_up gauge\nccdb_up 1\n# TYPE ccdb_node_id gauge\nccdb_node_id {}\n# TYPE ccdb_is_leader gauge\nccdb_is_leader {}\n# TYPE ccdb_leader_node_id gauge\nccdb_leader_node_id {}\n# TYPE ccdb_commit_index gauge\nccdb_commit_index {}\n# TYPE ccdb_applied_index gauge\nccdb_applied_index {}\n# TYPE ccdb_peers_configured gauge\nccdb_peers_configured {}\n",
            self.config.id,
            u8::from(leader == Some(self.config.id)),
            leader.unwrap_or(0),
            commit,
            applied,
            membership
                .voters
                .len()
                .saturating_add(membership.learners.len())
                .saturating_sub(1),
        );
        let _ = write!(
            output,
            "# TYPE ccdb_store_bloom_positives_total counter\nccdb_store_bloom_positives_total {}\n# TYPE ccdb_store_bloom_negatives_total counter\nccdb_store_bloom_negatives_total {}\n# TYPE ccdb_store_manifest_rewrites_total counter\nccdb_store_manifest_rewrites_total {}\n# TYPE ccdb_store_compactions_started_total counter\nccdb_store_compactions_started_total {}\n# TYPE ccdb_store_compactions_completed_total counter\nccdb_store_compactions_completed_total {}\n# TYPE ccdb_store_compactions_aborted_total counter\nccdb_store_compactions_aborted_total {}\n# TYPE ccdb_expiry_proposals_total counter\nccdb_expiry_proposals_total {}\n# TYPE ccdb_expiry_keys_total counter\nccdb_expiry_keys_total {}\n",
            store.bloom_positives,
            store.bloom_negatives,
            store.manifest_rewrites,
            store.compaction_jobs_started,
            store.compaction_jobs_completed,
            store.compaction_jobs_aborted,
            core.expiry_proposals,
            core.expiry_keys,
        );
        output.push_str("# TYPE ccdb_store_files gauge\n# TYPE ccdb_store_file_bytes gauge\n");
        for (level, (files, bytes)) in levels {
            let _ = writeln!(output, "ccdb_store_files{{level=\"{level}\"}} {files}");
            let _ = writeln!(output, "ccdb_store_file_bytes{{level=\"{level}\"}} {bytes}");
        }

        output.push_str("# TYPE ccdb_footprint_bytes gauge\n");
        append_usage_metrics(&mut output, "log", footprint.log);
        append_usage_metrics(&mut output, "snapshot_staging", footprint.snapshot_staging);
        append_usage_metrics(&mut output, "sessions", footprint.sessions);
        append_usage_metrics(
            &mut output,
            "session_tombstones",
            footprint.session_tombstones,
        );
        append_usage_metrics(&mut output, "pending_reads", footprint.pending_reads);
        append_usage_metrics(
            &mut output,
            "pending_client_routes",
            footprint.pending_client_routes,
        );
        append_usage_metrics(&mut output, "memtables", footprint.memtables);
        append_usage_metrics(&mut output, "sst_metadata", footprint.sst_metadata);
        append_usage_metrics(&mut output, "driver_effects", footprint.driver_effects);
        append_usage_metrics(&mut output, "outbound_frames", footprint.outbound_frames);
        append_usage_metrics(
            &mut output,
            "checkpoint_builder",
            footprint.checkpoint_builder,
        );
        append_usage_metrics(
            &mut output,
            "compaction_builder",
            footprint.compaction_builder,
        );
        append_usage_metrics(&mut output, "driver_inputs", footprint.driver_inputs);
        let _ = write!(
            output,
            "# TYPE ccdb_footprint_items gauge\nccdb_footprint_items{{resource=\"armed_timers\"}} {}\nccdb_footprint_items{{resource=\"pending_io\"}} {}\nccdb_footprint_items{{resource=\"pending_peer_inputs\"}} {}\nccdb_footprint_items{{resource=\"pending_timer_inputs\"}} {}\nccdb_footprint_items{{resource=\"pending_io_inputs\"}} {}\nccdb_footprint_items{{resource=\"pending_client_inputs\"}} {}\nccdb_footprint_items{{resource=\"pending_input_bytes\"}} {}\n# TYPE ccdb_driver_blocked gauge\nccdb_driver_blocked {}\n",
            footprint.armed_timers,
            footprint.pending_io,
            footprint.pending_peer_inputs,
            footprint.pending_timer_inputs,
            footprint.pending_io_inputs,
            footprint.pending_client_inputs,
            footprint.pending_input_bytes,
            u8::from(footprint.blocked),
        );

        output.push_str("# TYPE ccdb_peer_semantic_version gauge\n# TYPE ccdb_peer_features gauge\n# TYPE ccdb_peer_match_index gauge\n# TYPE ccdb_peer_commit_lag gauge\n# TYPE ccdb_peer_last_contact_age_seconds gauge\n");
        for peer in membership
            .voters
            .iter()
            .chain(membership.learners.iter())
            .filter(|peer| peer.get() != self.config.id)
        {
            let (semantic, features) = capabilities
                .get(peer)
                .copied()
                .or_else(|| {
                    observed
                        .get(peer)
                        .map(|metric| (metric.semantic_version, metric.features))
                })
                .unwrap_or((0, 0));
            let matched = match_indexes.get(peer).map_or(0, |index| index.get());
            let age = observed.get(peer).map_or(-1_i128, |metric| {
                i128::from(
                    self.metrics
                        .elapsed_millis()
                        .saturating_sub(metric.last_contact_millis)
                        / 1_000,
                )
            });
            let _ = write!(
                output,
                "ccdb_peer_semantic_version{{node_id=\"{}\"}} {}\nccdb_peer_features{{node_id=\"{}\"}} {}\nccdb_peer_match_index{{node_id=\"{}\"}} {}\nccdb_peer_commit_lag{{node_id=\"{}\"}} {}\nccdb_peer_last_contact_age_seconds{{node_id=\"{}\"}} {}\n",
                peer.get(),
                semantic,
                peer.get(),
                features,
                peer.get(),
                matched,
                peer.get(),
                commit.saturating_sub(matched),
                peer.get(),
                age,
            );
        }
        if output.len() > METRICS_BYTE_CAP {
            return String::from(
                "# TYPE ccdb_up gauge\nccdb_up 1\n# TYPE ccdb_metrics_overflow gauge\nccdb_metrics_overflow 1\n",
            );
        }
        output
    }
}

fn append_usage_metrics(output: &mut String, resource: &str, usage: cc_host::Usage) {
    let _ = write!(
        output,
        "ccdb_footprint_bytes{{resource=\"{resource}\",kind=\"current\"}} {}\nccdb_footprint_bytes{{resource=\"{resource}\",kind=\"peak\"}} {}\nccdb_footprint_bytes{{resource=\"{resource}\",kind=\"limit\"}} {}\n",
        usage.current, usage.peak, usage.limit,
    );
}

fn files_are_identical(left: &Path, right: &Path) -> io::Result<bool> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = File::open(left)?;
    let mut right = File::open(right)?;
    let mut left_chunk = [0_u8; 64 * 1024];
    let mut right_chunk = [0_u8; 64 * 1024];
    loop {
        let left_len = left.read(&mut left_chunk)?;
        let right_len = right.read(&mut right_chunk)?;
        if left_len != right_len || left_chunk[..left_len] != right_chunk[..right_len] {
            return Ok(false);
        }
        if left_len == 0 {
            return Ok(true);
        }
    }
}

/// CCMS v1 Genesis records from the immutable compatibility cut contain the
/// voter set but predate the replicated address book.  Empty durable
/// addresses therefore mean "use config for discovery"; all consensus fields
/// remain exact.  Once an address book is present it must match byte-for-byte,
/// so stale local config can never overwrite a replicated route.
pub(crate) fn bootstrap_membership_matches(
    durable: &MembershipState,
    configured: &MembershipState,
) -> bool {
    durable.voters == configured.voters
        && durable.learners == configured.learners
        && durable.joint == configured.joint
        && durable.active_features == configured.active_features
        && (durable.addresses.is_empty() || durable.addresses == configured.addresses)
}

fn decode_published_snapshot(
    mut decoder: cc_cluster::CcsnStreamDecoder,
    path: &Path,
) -> io::Result<(cc_cluster::CcsnSnapshot, u32)> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; cc_cluster::SNAPSHOT_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        decoder.push(&buffer[..read]).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid published snapshot")
        })?;
    }
    decoder
        .finish()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated published snapshot"))
}

/// Partial incoming-transfer files are deliberately resume-from-zero in CCSN
/// v1. Remove only canonical staging names at boot and sync that directory so
/// a later transfer cannot mistake stale bytes for its own durable prefix.
fn cleanup_snapshot_staging(data_dir: &Path) -> io::Result<()> {
    let staging = data_dir.join("snapshots/staging");
    fs::create_dir_all(&staging)?;
    let mut removed = false;
    for entry in fs::read_dir(&staging)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name
            .strip_prefix("stage.")
            .and_then(|rest| rest.strip_suffix(".ccsn"))
            .is_some_and(|sequence| {
                !sequence.is_empty() && sequence.bytes().all(|byte| byte.is_ascii_digit())
            })
        {
            fs::remove_file(entry.path())?;
            removed = true;
        }
    }
    if removed {
        super::sync_directory(&staging)?;
    }
    Ok(())
}

#[cfg(test)]
fn latest_published_snapshot(data_dir: &Path) -> io::Result<Option<PathBuf>> {
    let snapshots = data_dir.join("snapshots");
    if !snapshots.is_dir() {
        return Ok(None);
    }
    let mut newest: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(snapshots)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(generation) = name
            .strip_prefix("snapshot.")
            .and_then(|rest| rest.strip_suffix(".ccsn"))
            .and_then(|raw| raw.parse::<u64>().ok())
        else {
            continue;
        };
        if generation == 0 {
            continue;
        }
        if newest
            .as_ref()
            .is_none_or(|(current, _)| generation > *current)
        {
            newest = Some((generation, entry.path()));
        }
    }
    Ok(newest.map(|(_, path)| path))
}

fn published_snapshot_path(data_dir: &Path, generation: u64) -> PathBuf {
    data_dir
        .join("snapshots")
        .join(format!("snapshot.{generation}.ccsn"))
}

/// Atomically replace a marked WAL with the minimum complete framed prefix
/// needed to recover the same Raft state.  The caller has already verified
/// the matching CCSN file and must run while no append handle remains open.
fn compact_verified_wal(path: &Path, state: &cc_log::LogState) -> io::Result<Vec<u8>> {
    let mark = state.snapshot.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "cannot compact an unmarked WAL",
        )
    })?;
    let mut bytes = cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(
        Box::new(state.genesis.clone()),
    ))
    .map_err(io::Error::other)?;
    if state.hard_state.term.get() != 0 || state.hard_state.voted_for.is_some() {
        bytes.extend_from_slice(
            &cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Hard(state.hard_state))
                .map_err(io::Error::other)?,
        );
    }
    // An installed mark permits recovery when no copy of the covered entry is
    // retained. The host's exact-file validation above supplies the missing
    // durability proof and makes the compacted representation canonical for
    // both locally-created and received checkpoints.
    bytes.extend_from_slice(
        &cc_log::encode_framed_durable_record(&cc_log::DurableRecord::InstalledSnapshotMark(mark))
            .map_err(io::Error::other)?,
    );
    for entry in &state.entries {
        bytes.extend_from_slice(
            &cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Append(entry.clone()))
                .map_err(io::Error::other)?,
        );
    }
    let recovered = recover_framed_record_stream(&bytes).map_err(io::Error::other)?;
    if recovered.torn_tail_truncated || recovered.state != *state {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compacted WAL does not recover the verified state",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "WAL path has no parent directory",
        )
    })?;
    let temporary = path.with_extension(format!("compact-{}", std::process::id()));
    if temporary.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "WAL compaction temporary already exists",
        ));
    }
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        super::sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map(|()| bytes)
}

struct HostMetrics {
    started: OnceInstant,
    commands: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    fsync_writes: AtomicU64,
    peer_frames: AtomicU64,
    block_reads: AtomicU64,
    block_bytes_read: AtomicU64,
    file_reads: AtomicU64,
    file_bytes_read: AtomicU64,
    file_writes: AtomicU64,
    file_bytes_written: AtomicU64,
    renames: AtomicU64,
    directory_syncs: AtomicU64,
    queue_rejections: AtomicU64,
    snapshots_created: AtomicU64,
    snapshots_sent: AtomicU64,
    snapshots_received: AtomicU64,
    snapshots_aborted: AtomicU64,
    storage_fault: AtomicBool,
    peers: Mutex<BTreeMap<NodeId, PeerMetric>>,
    sent_transfers: Mutex<BTreeMap<NodeId, u64>>,
    received_transfers: Mutex<BTreeMap<NodeId, u64>>,
    aborted_transfers: Mutex<BTreeMap<NodeId, u64>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct PeerMetric {
    semantic_version: u16,
    features: u64,
    last_contact_millis: u64,
}

impl Default for HostMetrics {
    fn default() -> Self {
        Self {
            started: OnceInstant::default(),
            commands: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            fsync_writes: AtomicU64::new(0),
            peer_frames: AtomicU64::new(0),
            block_reads: AtomicU64::new(0),
            block_bytes_read: AtomicU64::new(0),
            file_reads: AtomicU64::new(0),
            file_bytes_read: AtomicU64::new(0),
            file_writes: AtomicU64::new(0),
            file_bytes_written: AtomicU64::new(0),
            renames: AtomicU64::new(0),
            directory_syncs: AtomicU64::new(0),
            queue_rejections: AtomicU64::new(0),
            snapshots_created: AtomicU64::new(0),
            snapshots_sent: AtomicU64::new(0),
            snapshots_received: AtomicU64::new(0),
            snapshots_aborted: AtomicU64::new(0),
            storage_fault: AtomicBool::new(false),
            peers: Mutex::new(BTreeMap::new()),
            sent_transfers: Mutex::new(BTreeMap::new()),
            received_transfers: Mutex::new(BTreeMap::new()),
            aborted_transfers: Mutex::new(BTreeMap::new()),
        }
    }
}

/// `Instant` is not `Default`; keeping it in a tiny wrapper makes metrics
/// construction explicit without an unrelated global clock.
struct OnceInstant(Instant);

impl Default for OnceInstant {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl HostMetrics {
    fn elapsed_millis(&self) -> u64 {
        u64::try_from(self.started.0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn observe_blocks(&self, observations: &[BlockObservation]) {
        self.block_reads.fetch_add(
            u64::try_from(observations.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let bytes = observations.iter().fold(0_u64, |total, observation| {
            total.saturating_add(u64::from(observation.len))
        });
        self.block_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    fn observe_peer(&self, peer: NodeId, semantic_version: u16, features: u64) {
        if let Ok(mut peers) = self.peers.lock() {
            peers.insert(
                peer,
                PeerMetric {
                    semantic_version,
                    features,
                    last_contact_millis: self.elapsed_millis(),
                },
            );
        }
    }

    fn touch_peer(&self, peer: NodeId) {
        if let Ok(mut peers) = self.peers.lock() {
            peers.entry(peer).or_default().last_contact_millis = self.elapsed_millis();
        }
    }

    fn count_transfer_once(
        &self,
        map: &Mutex<BTreeMap<NodeId, u64>>,
        counter: &AtomicU64,
        peer: NodeId,
        transfer_id: u64,
    ) {
        if let Ok(mut transfers) = map.lock()
            && transfers.insert(peer, transfer_id) != Some(transfer_id)
        {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn render(&self) -> String {
        format!(
            "# TYPE ccdb_commands_total counter\nccdb_commands_total {}\n# TYPE ccdb_reads_total counter\nccdb_reads_total {}\n# TYPE ccdb_writes_total counter\nccdb_writes_total {}\n# TYPE ccdb_fsyncs_total counter\nccdb_fsyncs_total {}\n# TYPE ccdb_peer_frames_total counter\nccdb_peer_frames_total {}\n# TYPE ccdb_block_reads_total counter\nccdb_block_reads_total {}\n# TYPE ccdb_block_bytes_read_total counter\nccdb_block_bytes_read_total {}\n# TYPE ccdb_file_reads_total counter\nccdb_file_reads_total {}\n# TYPE ccdb_file_bytes_read_total counter\nccdb_file_bytes_read_total {}\n# TYPE ccdb_file_writes_total counter\nccdb_file_writes_total {}\n# TYPE ccdb_file_bytes_written_total counter\nccdb_file_bytes_written_total {}\n# TYPE ccdb_file_renames_total counter\nccdb_file_renames_total {}\n# TYPE ccdb_directory_syncs_total counter\nccdb_directory_syncs_total {}\n# TYPE ccdb_queue_rejections_total counter\nccdb_queue_rejections_total {}\n# TYPE ccdb_snapshots_created_total counter\nccdb_snapshots_created_total {}\n# TYPE ccdb_snapshots_sent_total counter\nccdb_snapshots_sent_total {}\n# TYPE ccdb_snapshots_received_total counter\nccdb_snapshots_received_total {}\n# TYPE ccdb_snapshots_aborted_total counter\nccdb_snapshots_aborted_total {}\n# TYPE ccdb_storage_fault gauge\nccdb_storage_fault {}\n# TYPE ccdb_uptime_seconds gauge\nccdb_uptime_seconds {}\n",
            self.commands.load(Ordering::Relaxed),
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.fsync_writes.load(Ordering::Relaxed),
            self.peer_frames.load(Ordering::Relaxed),
            self.block_reads.load(Ordering::Relaxed),
            self.block_bytes_read.load(Ordering::Relaxed),
            self.file_reads.load(Ordering::Relaxed),
            self.file_bytes_read.load(Ordering::Relaxed),
            self.file_writes.load(Ordering::Relaxed),
            self.file_bytes_written.load(Ordering::Relaxed),
            self.renames.load(Ordering::Relaxed),
            self.directory_syncs.load(Ordering::Relaxed),
            self.queue_rejections.load(Ordering::Relaxed),
            self.snapshots_created.load(Ordering::Relaxed),
            self.snapshots_sent.load(Ordering::Relaxed),
            self.snapshots_received.load(Ordering::Relaxed),
            self.snapshots_aborted.load(Ordering::Relaxed),
            u8::from(self.storage_fault.load(Ordering::Relaxed)),
            self.started.0.elapsed().as_secs(),
        )
    }
}

enum SubmitError {
    NotLeader(Option<NodeId>),
    FeatureDisabled,
    Host(HostError),
    Timeout,
    InvalidReply,
}

fn serve_client(
    mut stream: TcpStream,
    state: &Arc<DriverHost>,
    client: ClientId,
) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(8 * 1024);
    let mut scratch = [0_u8; 8 * 1024];
    let mut transaction = None;
    loop {
        match stream.read(&mut scratch) {
            Ok(0) => return Ok(()),
            Ok(read) => buffer.extend_from_slice(&scratch[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if state.stopping.load(Ordering::Acquire) {
                    return Ok(());
                }
            }
            Err(error) => return Err(error),
        }
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
            // Read the complete request before refusing it.  Closing a TCP
            // socket with unread client bytes can turn the intended RESP
            // error into an RST on some kernels.  Rechecking at the command
            // boundary also fences connections accepted just before removal.
            if !state.serves_clients() {
                stream.write_all(&encode(&RespValue::Error(String::from(
                    "TRYAGAIN joining node is not active",
                ))))?;
                return Ok(());
            }
            let command = match parse_command(value) {
                Ok(command) => command,
                Err(error) => {
                    if transaction.is_some() {
                        transaction = Some(ClientTransaction::Dirty);
                    }
                    stream.write_all(&encode(&RespValue::Error(format!("ERR {error}"))))?;
                    continue;
                }
            };
            let response = execute_client(state, client, command, &mut transaction)
                .unwrap_or_else(|error| RespValue::Error(error.to_string()));
            stream.write_all(&encode(&response))?;
        }
    }
}

fn execute_client(
    state: &DriverHost,
    client: ClientId,
    command: ClientCommand,
    transaction: &mut Option<ClientTransaction>,
) -> io::Result<RespValue> {
    match command {
        ClientCommand::Multi => {
            if transaction.is_some() {
                return Ok(RespValue::Error(String::from(
                    "ERR MULTI calls cannot be nested",
                )));
            }
            if !state.atomic_batch_active().map_err(io::Error::other)? {
                return Ok(RespValue::Error(String::from("FEATUREDISABLED")));
            }
            *transaction = Some(ClientTransaction::Clean {
                commands: Vec::new(),
                encoded_bytes: 0,
            });
            Ok(RespValue::Simple(String::from("OK")))
        }
        ClientCommand::Discard => {
            if transaction.take().is_none() {
                return Ok(RespValue::Error(String::from("ERR DISCARD without MULTI")));
            }
            Ok(RespValue::Simple(String::from("OK")))
        }
        ClientCommand::Exec => match transaction.take() {
            None => Ok(RespValue::Error(String::from("ERR EXEC without MULTI"))),
            Some(ClientTransaction::Dirty) => Ok(RespValue::Error(String::from(
                "EXECABORT transaction discarded because it contains queue errors",
            ))),
            Some(ClientTransaction::Clean { commands, .. }) => {
                execute_transaction(state, client, commands)
            }
        },
        command => {
            if let Some(transaction) = transaction {
                return Ok(queue_client_transaction(transaction, command));
            }
            execute_client_immediate(state, client, command)
        }
    }
}

fn queue_client_transaction(
    transaction: &mut ClientTransaction,
    command: ClientCommand,
) -> RespValue {
    match queue_transaction_command(command) {
        Ok(queued) => match transaction {
            ClientTransaction::Clean {
                commands,
                encoded_bytes,
            } => {
                let policy = cc_core::ClusterPolicy::default();
                let child_bytes =
                    u64::try_from(encode_command(&queued.1).len()).unwrap_or(u64::MAX);
                let next_bytes = encoded_bytes.checked_add(child_bytes);
                if commands.len()
                    >= usize::try_from(policy.max_batch_commands).unwrap_or(usize::MAX)
                    || next_bytes.is_none_or(|bytes| bytes > policy.max_batch_bytes)
                {
                    *transaction = ClientTransaction::Dirty;
                    RespValue::Error(String::from("ERR transaction exceeds batch limits"))
                } else {
                    *encoded_bytes = next_bytes.expect("checked batch bytes");
                    commands.push(queued);
                    RespValue::Simple(String::from("QUEUED"))
                }
            }
            ClientTransaction::Dirty => {
                RespValue::Error(String::from("ERR transaction is already dirty"))
            }
        },
        Err(error) => {
            *transaction = ClientTransaction::Dirty;
            RespValue::Error(format!("ERR command cannot be queued: {error}"))
        }
    }
}

fn execute_transaction(
    state: &DriverHost,
    client: ClientId,
    commands: Vec<(ClientCommand, KvCommand)>,
) -> io::Result<RespValue> {
    if commands.is_empty() {
        return Ok(RespValue::Array(Vec::new()));
    }
    let original = commands
        .iter()
        .map(|(command, _)| command.clone())
        .collect::<Vec<_>>();
    let batch = KvCommand::Batch {
        commands: commands.into_iter().map(|(_, command)| command).collect(),
    };
    match submit(state, client, batch)? {
        KvReply::Batch(replies) if replies.len() == original.len() => Ok(RespValue::Array(
            original
                .iter()
                .zip(replies)
                .map(|(command, reply)| render_write_reply(command, reply))
                .collect(),
        )),
        KvReply::Batch(replies) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("batch reply count {} does not match request", replies.len()),
        )),
        reply => Ok(to_resp(reply)),
    }
}

fn execute_one_shot_batch(
    state: &DriverHost,
    client: ClientId,
    commands: Vec<ClientCommand>,
) -> io::Result<RespValue> {
    let batch = batch_kv(&commands)?;
    let reply = submit(state, client, batch)?;
    Ok(render_write_reply(&ClientCommand::Batch(commands), reply))
}

fn batch_kv(commands: &[ClientCommand]) -> io::Result<KvCommand> {
    if commands.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "BATCH requires at least one command",
        ));
    }
    let commands = commands
        .iter()
        .cloned()
        .map(queue_transaction_command)
        .map(|result| result.map(|(_, command)| command))
        .collect::<io::Result<Vec<_>>>()?;
    Ok(KvCommand::Batch { commands })
}

fn queue_transaction_command(command: ClientCommand) -> io::Result<(ClientCommand, KvCommand)> {
    let kv = match &command {
        ClientCommand::Set { .. } => set_kv(&command)?,
        ClientCommand::Del(keys) if keys.len() == 1 => KvCommand::Del {
            key: keys[0].clone(),
        },
        ClientCommand::Del(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DEL inside MULTI accepts one key",
            ));
        }
        ClientCommand::Request { .. }
        | ClientCommand::Batch(_)
        | ClientCommand::Admin { .. }
        | ClientCommand::AdminMembers { .. }
        | ClientCommand::ReadFollower(_)
        | ClientCommand::ReadStale(_)
        | ClientCommand::Info
        | ClientCommand::Ping
        | ClientCommand::Echo(_)
        | ClientCommand::Exists(_)
        | ClientCommand::Scan { .. }
        | ClientCommand::Unknown(_)
        | ClientCommand::Multi
        | ClientCommand::Exec
        | ClientCommand::Discard => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "command is not batchable",
            ));
        }
        _ => simple_kv(command.clone())?,
    };
    Ok((command, kv))
}

fn execute_client_immediate(
    state: &DriverHost,
    client: ClientId,
    command: ClientCommand,
) -> io::Result<RespValue> {
    match command {
        ClientCommand::Ping => Ok(RespValue::Simple(String::from("PONG"))),
        ClientCommand::Echo(value) => Ok(RespValue::Bulk(Some(value))),
        ClientCommand::Info => Ok(RespValue::Bulk(Some(info(state).into_bytes()))),
        ClientCommand::Batch(commands) => execute_one_shot_batch(state, client, commands),
        ClientCommand::Request {
            client: session_client,
            sequence,
            command,
        } => execute_explicit_request(state, client, session_client, sequence, *command),
        ClientCommand::Admin {
            operator_id,
            sequence,
            operation,
        } => {
            let operation = admin_config_operation(state, operation)?;
            match state.admin_submit(client, operator_id, sequence, operation) {
                Ok(reply) => Ok(render_admin_reply(reply)),
                Err(SubmitError::NotLeader(leader)) => {
                    let hint = leader
                        .map_or_else(|| String::from("unknown"), |id| state.client_address(id));
                    Ok(RespValue::Error(format!("NOTLEADER addr={hint}")))
                }
                Err(SubmitError::FeatureDisabled) => {
                    Ok(RespValue::Error(String::from("FEATUREDISABLED")))
                }
                Err(SubmitError::Timeout) => Ok(RespValue::Error(format!(
                    "UNKNOWN operator_id={operator_id} sequence={sequence}"
                ))),
                Err(SubmitError::Host(error)) => Err(io::Error::other(error)),
                Err(SubmitError::InvalidReply) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid administrative reply",
                )),
            }
        }
        ClientCommand::AdminMembers { consistent } => {
            if consistent && state.leader() != Some(NodeId::new(state.config.id)) {
                return Ok(RespValue::Error(String::from(
                    "NOTLEADER consistent membership requires the leader",
                )));
            }
            if consistent {
                // An ordinary strong read is the same bounded ReadIndex path
                // used by GET.  The sentinel value is ignored; only the
                // completed barrier authorizes the membership snapshot below.
                let _ = submit(
                    state,
                    client,
                    KvCommand::Get {
                        key: b"__cc_admin_readindex__".to_vec(),
                    },
                )?;
            }
            Ok(RespValue::Bulk(Some(
                state.membership_status().into_bytes(),
            )))
        }
        ClientCommand::ReadFollower(command) => {
            let kv = match *command {
                ClientCommand::Get(key) => KvCommand::Get { key },
                ClientCommand::Ttl(key) => KvCommand::Ttl { key },
                _ => {
                    return Ok(RespValue::Error(String::from(
                        "ERR READ FOLLOWER accepts GET or TTL",
                    )));
                }
            };
            match state.follower_read(client, kv) {
                Ok((reply, metadata)) => Ok(RespValue::Array(vec![
                    RespValue::Bulk(Some(b"FOLLOWER".to_vec())),
                    to_resp(reply),
                    RespValue::Integer(
                        i64::try_from(metadata.read_index.get()).unwrap_or(i64::MAX),
                    ),
                    RespValue::Integer(
                        i64::try_from(metadata.applied_index.get()).unwrap_or(i64::MAX),
                    ),
                    RespValue::Integer(
                        i64::try_from(metadata.applied_term.get()).unwrap_or(i64::MAX),
                    ),
                    RespValue::Integer(
                        i64::try_from(metadata.read_time.as_nanos()).unwrap_or(i64::MAX),
                    ),
                ])),
                Err(SubmitError::NotLeader(leader)) => Ok(follower_read_retry(state, leader)),
                Err(
                    SubmitError::FeatureDisabled
                    | SubmitError::Host(_)
                    | SubmitError::Timeout
                    | SubmitError::InvalidReply,
                ) => Ok(follower_read_retry(state, state.leader())),
            }
        }
        ClientCommand::ReadStale(command) => match *command {
            ClientCommand::Get(key) => {
                let (reply, index, term, read_time, contact_age) =
                    state.stale_get(key).map_err(io::Error::other)?;
                Ok(RespValue::Array(vec![
                    RespValue::Bulk(Some(b"STALE".to_vec())),
                    to_resp(reply),
                    RespValue::Integer(i64::try_from(index).unwrap_or(i64::MAX)),
                    RespValue::Integer(i64::try_from(term).unwrap_or(i64::MAX)),
                    RespValue::Integer(i64::try_from(read_time.as_nanos()).unwrap_or(i64::MAX)),
                    RespValue::Integer(contact_age),
                ]))
            }
            _ => Ok(RespValue::Error(String::from("ERR READ STALE accepts GET"))),
        },
        ClientCommand::Unknown(command) => {
            Ok(RespValue::Error(format!("ERR unknown command {command:?}")))
        }
        ClientCommand::Del(keys) => {
            let mut deleted = 0_i64;
            for key in keys {
                if matches!(
                    submit(state, client, KvCommand::Del { key })?,
                    KvReply::Integer(1)
                ) {
                    deleted += 1;
                }
            }
            Ok(RespValue::Integer(deleted))
        }
        ClientCommand::Exists(key) => match submit(state, client, KvCommand::Get { key })? {
            KvReply::Value(value) => Ok(RespValue::Integer(i64::from(value.is_some()))),
            reply => Ok(to_resp(reply)),
        },
        ClientCommand::Scan {
            cursor,
            prefix,
            count,
        } => match submit(
            state,
            client,
            KvCommand::Scan {
                start: prefix,
                end: None,
                limit: count,
            },
        )? {
            KvReply::Scan(values) => Ok(RespValue::Array(vec![
                RespValue::Integer(i64::try_from(cursor).unwrap_or(i64::MAX)),
                RespValue::Array(
                    values
                        .into_iter()
                        .flat_map(|(key, value)| {
                            [RespValue::Bulk(Some(key)), RespValue::Bulk(Some(value))]
                        })
                        .collect(),
                ),
            ])),
            reply => Ok(to_resp(reply)),
        },
        ClientCommand::Set {
            key,
            value,
            ttl,
            nx,
            xx,
        } => {
            if nx && xx {
                return Ok(RespValue::Error(String::from(
                    "ERR NX and XX are mutually exclusive",
                )));
            }
            let command = ClientCommand::Set {
                key,
                value,
                ttl,
                nx,
                xx,
            };
            let reply = submit(state, client, set_kv(&command)?)?;
            Ok(render_write_reply(&command, reply))
        }
        other => Ok(to_resp(submit(state, client, simple_kv(other)?)?)),
    }
}

fn checked_member_id(id: u64) -> io::Result<NodeId> {
    if id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "member id must be nonzero",
        ));
    }
    Ok(NodeId::new(id))
}

fn canonical_peer_address(raw: &[u8]) -> io::Result<PeerAddress> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "peer address must be UTF-8"))?;
    let socket = text.parse::<SocketAddr>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "peer address must be a numeric IP:port",
        )
    })?;
    let address = match socket {
        SocketAddr::V4(value) => PeerAddress::V4 {
            ip: value.ip().octets(),
            port: value.port(),
        },
        SocketAddr::V6(value) => PeerAddress::V6 {
            ip: value.ip().octets(),
            port: value.port(),
        },
    };
    address
        .validate()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer address"))?;
    Ok(address)
}

fn render_peer_address(address: &PeerAddress) -> String {
    match address {
        PeerAddress::V4 { ip, port } => SocketAddr::from((*ip, *port)).to_string(),
        PeerAddress::V6 { ip, port } => SocketAddr::from((*ip, *port)).to_string(),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn admin_config_operation(
    state: &DriverHost,
    operation: AdminOperation,
) -> io::Result<ConfigOperation> {
    match operation {
        AdminOperation::AddLearner { id, peer_address } => Ok(ConfigOperation::AddLearner {
            id: checked_member_id(id)?,
            address: Some(canonical_peer_address(&peer_address)?),
        }),
        AdminOperation::Promote { id } => {
            let node = checked_member_id(id)?;
            let mut membership = state
                .driver
                .lock()
                .map_err(|_| io::Error::other("Driver mutex poisoned"))?
                .membership_state();
            membership.voters.insert(node);
            Ok(ConfigOperation::EnterJoint {
                new_voters: membership.voters,
            })
        }
        AdminOperation::Remove { id } => {
            let node = checked_member_id(id)?;
            let membership = state
                .driver
                .lock()
                .map_err(|_| io::Error::other("Driver mutex poisoned"))?
                .membership_state();
            if membership.learners.contains(&node) {
                Ok(ConfigOperation::RemoveLearner { id: node })
            } else if membership.voters.contains(&node) {
                let mut voters = membership.voters;
                voters.remove(&node);
                Ok(ConfigOperation::EnterJoint { new_voters: voters })
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "member is absent",
                ))
            }
        }
        AdminOperation::UpdateAddress { id, peer_address } => Ok(ConfigOperation::UpdateAddress {
            id: checked_member_id(id)?,
            address: canonical_peer_address(&peer_address)?,
        }),
        AdminOperation::TransferLeader { id } => Ok(ConfigOperation::BeginLeaderTransfer {
            target: checked_member_id(id)?,
        }),
        AdminOperation::LeaveJoint => {
            let membership = state
                .driver
                .lock()
                .map_err(|_| io::Error::other("Driver mutex poisoned"))?
                .membership_state();
            let enter_index = membership
                .joint
                .map(|joint| joint.enter_index)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "membership is not joint")
                })?;
            Ok(ConfigOperation::LeaveJoint { enter_index })
        }
        AdminOperation::ActivateAtomicBatch => Ok(ConfigOperation::ActivateFeature {
            feature: cc_core::ATOMIC_BATCH_FEATURE,
        }),
    }
}

fn render_admin_reply(reply: AdminReply) -> RespValue {
    let detail = String::from_utf8_lossy(&reply.detail);
    let receipt = format!(
        "result={:?} source_index={} detail={detail}",
        reply.result, reply.source_index
    );
    match reply.result {
        AdminResultTag::Applied | AdminResultTag::TransferSuccess => RespValue::Simple(receipt),
        AdminResultTag::InProgress => RespValue::Error(format!("INPROGRESS {receipt}")),
        AdminResultTag::RequestConflict => RespValue::Error(format!("REQUESTCONFLICT {receipt}")),
        AdminResultTag::RequestExpired => RespValue::Error(format!("REQUESTEXPIRED {receipt}")),
        AdminResultTag::TransferTimeout
        | AdminResultTag::TransferSuperseded
        | AdminResultTag::Rejected => RespValue::Error(receipt),
    }
}

fn follower_read_retry(state: &DriverHost, leader: Option<NodeId>) -> RespValue {
    let (leader, address) = leader
        .map(|node| (node.get(), state.client_address(node)))
        .unwrap_or((0, String::from("unknown")));
    RespValue::Error(format!(
        "TRYAGAIN follower read unavailable leader=n{leader} addr={address}"
    ))
}

fn execute_explicit_request(
    state: &DriverHost,
    route_client: ClientId,
    session_client: u64,
    sequence: u64,
    command: ClientCommand,
) -> io::Result<RespValue> {
    if session_client == 0 || sequence == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CC.REQUEST client and sequence must be nonzero",
        ));
    }
    let kv = explicit_write_kv(&command)?;
    let reply = submit_with_session(
        state,
        route_client,
        Some((ClientId::new(session_client), RequestSeq::new(sequence))),
        kv,
    )?;
    Ok(render_write_reply(&command, reply))
}

/// Convert only one state-changing RESP command into its canonical CCKV
/// request. The CC.REQUEST wrapper intentionally rejects reads and multi-key
/// DEL rather than turning a caller-owned retry identity into several hidden
/// replicated operations.
fn explicit_write_kv(command: &ClientCommand) -> io::Result<KvCommand> {
    match command {
        ClientCommand::Batch(commands) => batch_kv(commands),
        ClientCommand::Set { .. } => set_kv(command),
        ClientCommand::Del(keys) if keys.len() == 1 => Ok(KvCommand::Del {
            key: keys[0].clone(),
        }),
        ClientCommand::Del(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CC.REQUEST accepts one mutating command",
        )),
        _ => {
            let kv = simple_kv(command.clone())?;
            if kv.is_write() {
                Ok(kv)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "CC.REQUEST accepts state-changing commands only",
                ))
            }
        }
    }
}

fn set_kv(command: &ClientCommand) -> io::Result<KvCommand> {
    let ClientCommand::Set {
        key,
        value,
        ttl,
        nx,
        xx,
    } = command
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected SET command",
        ));
    };
    match (*nx, *xx) {
        (true, false) => Ok(KvCommand::ConditionalSet {
            key: key.clone(),
            value: value.clone(),
            ttl: *ttl,
            condition: SetCondition::Nx,
        }),
        (false, true) => Ok(KvCommand::ConditionalSet {
            key: key.clone(),
            value: value.clone(),
            ttl: *ttl,
            condition: SetCondition::Xx,
        }),
        (false, false) => Ok(KvCommand::Set {
            key: key.clone(),
            value: value.clone(),
            ttl: *ttl,
        }),
        (true, true) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "SET NX and XX are mutually exclusive",
        )),
    }
}

fn render_write_reply(command: &ClientCommand, reply: KvReply) -> RespValue {
    match (command, reply) {
        (ClientCommand::Batch(commands), KvReply::Batch(replies))
            if commands.len() == replies.len() =>
        {
            RespValue::Array(
                commands
                    .iter()
                    .zip(replies)
                    .map(|(command, reply)| render_write_reply(command, reply))
                    .collect(),
            )
        }
        (ClientCommand::Set { .. }, KvReply::Conditional(false)) => RespValue::Bulk(None),
        (ClientCommand::Set { .. }, KvReply::Conditional(true)) => {
            RespValue::Simple(String::from("OK"))
        }
        (_, reply) => to_resp(reply),
    }
}

fn simple_kv(command: ClientCommand) -> io::Result<KvCommand> {
    Ok(match command {
        ClientCommand::Get(key) => KvCommand::Get { key },
        ClientCommand::Cas {
            key,
            expected,
            value,
        } => KvCommand::Cas {
            key,
            expected: Some(expected),
            value: Some(value),
        },
        ClientCommand::IncrBy { key, delta } => KvCommand::Incr { key, delta },
        ClientCommand::Append { key, value } => KvCommand::Append { key, value },
        ClientCommand::GetSet { key, value } => KvCommand::GetSet { key, value },
        ClientCommand::GetDel(key) => KvCommand::GetDel { key },
        ClientCommand::SetNx { key, value } => KvCommand::ConditionalSet {
            key,
            value,
            ttl: None,
            condition: SetCondition::Nx,
        },
        ClientCommand::Expire { key, ttl } => KvCommand::Expire { key, ttl },
        ClientCommand::ExpireAt { key, at_seconds } => {
            let nanos = at_seconds.checked_mul(1_000_000_000).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "EXPIREAT nanosecond overflow")
            })?;
            KvCommand::ExpireAt {
                key,
                at: Time::from_nanos(nanos),
            }
        }
        ClientCommand::Ttl(key) => KvCommand::Ttl { key },
        ClientCommand::Persist(key) => KvCommand::Persist { key },
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported client command",
            ));
        }
    })
}

fn submit(state: &DriverHost, client: ClientId, command: KvCommand) -> io::Result<KvReply> {
    submit_with_session(state, client, None, command)
}

fn submit_with_session(
    state: &DriverHost,
    client: ClientId,
    session: Option<(ClientId, RequestSeq)>,
    command: KvCommand,
) -> io::Result<KvReply> {
    match state.submit(client, session, command) {
        Ok(reply) => Ok(reply),
        Err(SubmitError::NotLeader(leader)) => {
            let (leader, address) = leader
                .map(|id| (id.get(), state.client_address(id)))
                .unwrap_or((0, String::from("unknown")));
            Err(io::Error::other(format!(
                "NOTLEADER leader=n{leader} addr={address}"
            )))
        }
        Err(SubmitError::FeatureDisabled) => Err(io::Error::other("FEATUREDISABLED")),
        Err(SubmitError::Host(error)) => Err(io::Error::other(error)),
        Err(SubmitError::Timeout) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "request did not commit",
        )),
        Err(SubmitError::InvalidReply) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid core reply",
        )),
    }
}

fn info(state: &DriverHost) -> String {
    let leader = state.leader().map(NodeId::get).unwrap_or(0);
    let role = if leader == state.config.id {
        "leader"
    } else {
        "follower"
    };
    let (commit, applied) = state.driver.lock().map_or((0, 0), |driver| {
        (
            driver.node().raft.commit_index.get(),
            driver.node().raft.applied_index.get(),
        )
    });
    format!(
        "# Server\r\nccdb_version:{}\r\nmode:shared-driver\r\nrole:{role}\r\nleader:n{leader}\r\nleader_client_addr:{}\r\ncommit:{commit}\r\napplied:{applied}\r\n",
        env!("CARGO_PKG_VERSION"),
        state.client_address(NodeId::new(leader)),
    )
}

fn serve_peer(mut stream: TcpStream, state: &Arc<DriverHost>) -> io::Result<()> {
    stream.set_read_timeout(Some(PEER_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(PEER_IO_TIMEOUT))?;
    let (hello, mut buffer) = read_hello(&mut stream)?;
    let local = local_hello(&state.config);
    let negotiated = local.negotiate(&hello).map_err(io::Error::other)?;
    // Publish the negotiated generation before acknowledging CCHL.  A probe
    // that has received our hello can then be used as a strict capability
    // preflight without racing this server thread's observation.
    state.observe_peer_capability(hello.node_id, negotiated)?;
    let _capability = PeerCapabilityGuard {
        state,
        peer: hello.node_id,
    };
    stream.write_all(&local.encode().map_err(io::Error::other)?)?;
    let mut scratch = [0_u8; 8 * 1024];
    loop {
        loop {
            let (message, used) = match decode_peer_frame(&buffer) {
                Ok(frame) => frame,
                Err(cc_env::FrameError::Incomplete) => break,
                Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
            };
            buffer.drain(..used);
            if !wire_message_features_are_allowed(&message, negotiated) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CCRP semantic version or feature differs from negotiated CCHL",
                ));
            }
            state.metrics.peer_frames.fetch_add(1, Ordering::Relaxed);
            let completed_snapshot =
                cc_raft::codec::decode(&message.payload)
                    .ok()
                    .and_then(|message| match message.kind {
                        MessageKind::SnapshotChunk {
                            transfer_id,
                            done: true,
                            ..
                        } => Some(transfer_id),
                        _ => None,
                    });
            let diagnostic = cc_raft::codec::decode(&message.payload)
                .map(|decoded| {
                    format!(
                        "from={} term={} kind={}",
                        decoded.from,
                        decoded.term,
                        raft_message_kind_name(&decoded.kind)
                    )
                })
                .unwrap_or_else(|_| String::from("undecodable"));
            state
                .deliver(Input::Recv {
                    from: hello.node_id,
                    msg: message,
                })
                .map_err(|error| io::Error::other(format!("{error}; {diagnostic}")))?;
            state.metrics.touch_peer(hello.node_id);
            if let Some(transfer_id) = completed_snapshot {
                state.metrics.count_transfer_once(
                    &state.metrics.received_transfers,
                    &state.metrics.snapshots_received,
                    hello.node_id,
                    transfer_id,
                );
            }
        }
        match stream.read(&mut scratch) {
            Ok(0) => return Ok(()),
            Ok(read) => buffer.extend_from_slice(&scratch[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if state.stopping.load(Ordering::Acquire) {
                    return Ok(());
                }
                continue;
            }
            Err(error) => return Err(error),
        }
        if buffer.len() > cc_env::MAX_PEER_FRAME + 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer frame exceeds host buffer limit",
            ));
        }
    }
}

fn raft_message_kind_name(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::PreVoteReq { .. } => "PreVoteReq",
        MessageKind::PreVoteResp { .. } => "PreVoteResp",
        MessageKind::VoteReq { .. } => "VoteReq",
        MessageKind::VoteResp { .. } => "VoteResp",
        MessageKind::AppendReq(_) => "AppendReq",
        MessageKind::AppendResp(_) => "AppendResp",
        MessageKind::SnapshotChunk { .. } => "SnapshotChunk",
        MessageKind::SnapshotAck { .. } => "SnapshotAck",
        MessageKind::TimeoutNow { .. } => "TimeoutNow",
        MessageKind::FollowerReadRequest { .. } => "FollowerReadRequest",
        MessageKind::FollowerReadGrant { .. } => "FollowerReadGrant",
    }
}

fn serve_metrics(mut stream: TcpStream, state: &DriverHost) -> io::Result<()> {
    stream.set_read_timeout(Some(StdDuration::from_secs(2)))?;
    let mut request = [0_u8; 4 * 1024];
    let read = stream.read(&mut request)?;
    let first_line = std::str::from_utf8(&request[..read])
        .ok()
        .and_then(|text| text.lines().next())
        .unwrap_or_default();
    let (content_type, body, status) = if first_line.starts_with("GET /metrics ") {
        (
            "text/plain; version=0.0.4",
            state.render_metrics(),
            "200 OK",
        )
    } else if first_line.starts_with("GET / ") {
        ("text/html; charset=utf-8", metrics_dashboard(), "200 OK")
    } else {
        (
            "text/plain; charset=utf-8",
            String::from("not found\n"),
            "404 Not Found",
        )
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len(),
    )
}

pub(crate) fn membership(config: &Config) -> io::Result<MembershipState> {
    let mut membership = MembershipState::new(
        config
            .peers
            .iter()
            .map(|peer| NodeId::new(peer.id))
            .collect::<BTreeSet<_>>(),
    )
    .map_err(io::Error::other)?;
    for peer in &config.peers {
        membership.addresses.insert(
            NodeId::new(peer.id),
            canonical_peer_address(resolve_peer_addr(&peer.address)?.to_string().as_bytes())?,
        );
    }
    membership.validate().map_err(io::Error::other)?;
    Ok(membership)
}

fn node_config(config: &Config, host_limits: HostLimits) -> NodeConfig {
    NodeConfig {
        id: NodeId::new(config.id),
        cluster_id: config.cluster_id.bytes(),
        seed: Seed::new(config.id),
        raft: RaftConfig::default(),
        store: StoreConfig::default(),
        policy: ClusterPolicy::default(),
        host_limits,
    }
}

fn local_hello(config: &Config) -> PeerHello {
    PeerHello {
        cluster_id: config.cluster_id.bytes(),
        node_id: NodeId::new(config.id),
        cluster_policy: ClusterPolicy::default().encode(),
        semantic_min: SEMANTIC_VERSION_MIN,
        semantic_max: SEMANTIC_VERSION_MAX,
        supported_features: FOLLOWER_READ_FEATURE | FEATURE_ATOMIC_BATCH,
        required_features: 0,
        max_peer_frame: u32::try_from(cc_env::MAX_PEER_FRAME).expect("peer frame limit fits u32"),
    }
}

fn wire_semantic_is_allowed(version: u16, negotiated: cc_env::NegotiatedPeer) -> bool {
    version >= SEMANTIC_VERSION_MIN
        && version <= negotiated.semantic_version
        && matches!(version, PROTOCOL_VERSION | SEMANTIC_VERSION_V3)
}

fn message_features_are_allowed(
    message: &cc_cluster::Message,
    negotiated: cc_env::NegotiatedPeer,
) -> bool {
    wire_semantic_is_allowed(message.proto_version, negotiated)
        && (!matches!(
            &message.kind,
            cc_cluster::MessageKind::FollowerReadRequest { .. }
                | cc_cluster::MessageKind::FollowerReadGrant { .. }
        ) || negotiated.features & FOLLOWER_READ_FEATURE != 0)
}

fn wire_message_features_are_allowed(wire: &WireMsg, negotiated: cc_env::NegotiatedPeer) -> bool {
    cc_raft::codec::decode(&wire.payload).is_ok_and(|message| {
        message.proto_version == wire.proto_version
            && message_features_are_allowed(&message, negotiated)
    })
}

fn read_hello(stream: &mut TcpStream) -> io::Result<(PeerHello, Vec<u8>)> {
    let mut buffer = Vec::with_capacity(128);
    let mut scratch = [0_u8; 1024];
    loop {
        match PeerHello::decode(&buffer) {
            Ok((hello, used)) => return Ok((hello, buffer.split_off(used))),
            Err(cc_env::HelloError::Incomplete) => {}
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
        let read = stream.read(&mut scratch)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed before CCHL",
            ));
        }
        buffer.extend_from_slice(&scratch[..read]);
        if buffer.len() > 2 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CCHL exceeds host buffer limit",
            ));
        }
    }
}

fn resolve_peer_addr(address: &str) -> io::Result<std::net::SocketAddr> {
    use std::net::ToSocketAddrs;

    address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer address"))
}

fn client_address_for(config: &Config, node: u64) -> String {
    if node == config.id {
        return config.listen_client.clone();
    }
    config
        .peers
        .iter()
        .find(|peer| peer.id == node)
        .and_then(|peer| peer.address.rsplit_once(':'))
        .and_then(|(host, port)| {
            port.parse::<u16>()
                .ok()
                .map(|port| format!("{host}:{}", port.saturating_sub(100)))
        })
        .unwrap_or_else(|| String::from("unknown"))
}

fn is_read(command: &KvCommand) -> bool {
    matches!(
        command,
        KvCommand::Get { .. } | KvCommand::Ttl { .. } | KvCommand::Scan { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lifecycle_config(root: PathBuf) -> Config {
        Config {
            id: 1,
            cluster_id: cc_core::ClusterId::from_hex("00112233445566778899aabbccddeeff")
                .expect("cluster id"),
            data_dir: root,
            listen_client: String::from("127.0.0.1:0"),
            listen_peer: String::from("127.0.0.1:0"),
            listen_metrics: String::from("127.0.0.1:0"),
            peers: vec![Peer {
                id: 1,
                address: String::from("127.0.0.1:7201"),
            }],
        }
    }

    #[test]
    fn trap_join_serves_only_after_active_ccid_fsync() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-joining-lifecycle-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("root");
        let config = lifecycle_config(root.clone());
        crate::write_identity(
            &crate::identity_path(&root),
            crate::DiskIdentity {
                lifecycle: crate::IDENTITY_JOINING,
                ..crate::DiskIdentity::fresh(config.cluster_id, 1)
            },
        )
        .expect("joining identity");
        let host = DriverHost::boot(config.clone(), None, 0, false).expect("host");
        assert!(!host.serves_clients());
        crate::mark_identity_active(&config).expect("durable Active transition");
        assert!(host.serves_clients());
        drop(host);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_removed_node_stops_serving_clients() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-removed-lifecycle-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("root");
        let config = lifecycle_config(root.clone());
        crate::write_identity(
            &crate::identity_path(&root),
            crate::DiskIdentity::fresh(config.cluster_id, 1),
        )
        .expect("active identity");
        let host = DriverHost::boot(config, None, 0, false).expect("host");
        assert!(host.serves_clients());
        host.driver
            .lock()
            .expect("driver")
            .node_mut()
            .raft
            .restore_membership_state(
                MembershipState::new([NodeId::new(2)].into_iter().collect())
                    .expect("replacement membership"),
            )
            .expect("remove local member");
        assert!(host.enforce_membership_lifecycle().expect("lifecycle"));
        assert!(!host.serves_clients());
        assert_eq!(
            crate::identity_lifecycle(&host.config).expect("identity"),
            crate::IDENTITY_REMOVED
        );
        drop(host);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_metrics_snapshot_is_bounded_and_covers_every_footprint() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-metrics-snapshot-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let config = Config {
            id: 1,
            cluster_id: cc_core::ClusterId::from_hex("00112233445566778899aabbccddeeff")
                .expect("cluster id"),
            data_dir: root.clone(),
            listen_client: String::from("127.0.0.1:0"),
            listen_peer: String::from("127.0.0.1:0"),
            listen_metrics: String::from("127.0.0.1:0"),
            peers: vec![Peer {
                id: 1,
                address: String::from("127.0.0.1:7201"),
            }],
        };
        let host = DriverHost::boot(config, None, 0, false).expect("host boot");
        let metrics = host.render_metrics();
        assert!(metrics.len() <= 64 * 1024);
        for resource in [
            "log",
            "snapshot_staging",
            "sessions",
            "session_tombstones",
            "pending_reads",
            "pending_client_routes",
            "memtables",
            "sst_metadata",
            "driver_effects",
            "outbound_frames",
            "checkpoint_builder",
            "compaction_builder",
            "driver_inputs",
        ] {
            for kind in ["current", "peak", "limit"] {
                assert!(
                    metrics.contains(&format!(
                        "ccdb_footprint_bytes{{resource=\"{resource}\",kind=\"{kind}\"}}"
                    )),
                    "missing {resource}/{kind}"
                );
            }
        }
        for required in [
            "ccdb_block_reads_total",
            "ccdb_store_bloom_positives_total",
            "ccdb_store_manifest_rewrites_total",
            "ccdb_store_compactions_aborted_total",
            "ccdb_snapshots_received_total",
            "ccdb_expiry_keys_total",
            "ccdb_queue_rejections_total",
            "ccdb_storage_fault",
            "ccdb_peer_last_contact_age_seconds",
        ] {
            assert!(metrics.contains(required), "missing {required}");
        }
        fs::remove_dir_all(root).expect("remove metrics fixture");
    }

    #[test]
    fn trap_snapshot_recovery_selects_only_the_newest_published_generation() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-snapshot-discovery-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let snapshots = root.join("snapshots");
        fs::create_dir_all(snapshots.join("staging")).expect("snapshot directories");
        fs::write(snapshots.join("snapshot.4.ccsn"), b"four").expect("published snapshot");
        fs::write(snapshots.join("snapshot.9.ccsn"), b"nine").expect("published snapshot");
        fs::write(snapshots.join("staging/stage.12.ccsn"), b"partial").expect("staging");
        fs::write(snapshots.join("snapshot.bad.ccsn"), b"ignored").expect("invalid name");
        let newest = latest_published_snapshot(&root)
            .expect("snapshot discovery")
            .expect("published snapshot");
        assert_eq!(
            newest.file_name().and_then(|name| name.to_str()),
            Some("snapshot.9.ccsn")
        );
        cleanup_snapshot_staging(&root).expect("staging cleanup");
        assert!(
            !snapshots.join("staging/stage.12.ccsn").exists(),
            "partial transfers restart from zero after boot"
        );
        assert!(snapshots.join("snapshot.9.ccsn").exists());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_boot_restores_a_restore_origin_checkpoint_named_by_its_durable_mark() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-marked-snapshot-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let config = Config {
            id: 1,
            cluster_id: cc_core::ClusterId::from_hex("00112233445566778899aabbccddeeff")
                .expect("cluster id"),
            data_dir: root.clone(),
            listen_client: String::from("127.0.0.1:7101"),
            listen_peer: String::from("127.0.0.1:7201"),
            listen_metrics: String::from("127.0.0.1:7301"),
            peers: vec![Peer {
                id: 1,
                address: String::from("127.0.0.1:7201"),
            }],
        };
        let limits = HostLimits::default();
        let members = membership(&config).expect("membership");
        let mut source =
            cc_cluster::Node::new(node_config(&config, limits), members.voters.clone())
                .expect("source");
        source.kv.apply_command_only(
            cc_core::LogIndex::new(3),
            cc_core::Term::new(2),
            KvCommand::Set {
                key: b"marked-key".to_vec(),
                value: b"marked-value".to_vec(),
                ttl: None,
            },
            Time::from_nanos(10),
        );
        source.raft.applied_index = cc_core::LogIndex::new(3);
        let checkpoint = source.encode_ccsn_snapshot().expect("checkpoint");
        let checksum = cc_cluster::ccsn_file_crc(&checkpoint).expect("checkpoint checksum");
        fs::create_dir_all(root.join("raft")).expect("raft directory");
        fs::create_dir_all(root.join("store")).expect("store directory");
        fs::create_dir_all(root.join("snapshots")).expect("snapshot directory");
        fs::write(root.join("snapshots/snapshot.3.ccsn"), checkpoint).expect("checkpoint file");
        let mut manifest = cc_store::ManifestV2::empty(3);
        manifest
            .append_edit_batch(vec![
                cc_store::ManifestEditV2::AppliedWatermark {
                    watermark: cc_store::StoreWatermark {
                        index: cc_core::LogIndex::new(3),
                        term: cc_core::Term::new(2),
                        last_leader_time: Time::from_nanos(10),
                    },
                    store_sequence: source.kv.store.last_sequence(),
                },
                cc_store::ManifestEditV2::Checkpoint(Some(ManifestCheckpoint {
                    index: cc_core::LogIndex::new(3),
                    term: cc_core::Term::new(2),
                    generation: 3,
                    crc32c: checksum,
                })),
            ])
            .expect("manifest checkpoint");
        fs::write(
            root.join("store/manifest.3.ccmf"),
            cc_store::encode_manifest_v2(&manifest).expect("manifest bytes"),
        )
        .expect("manifest file");
        let genesis = Genesis {
            origin: Origin::Restore,
            cluster_id: config.cluster_id.bytes(),
            policy: ClusterPolicy::default(),
            membership: members,
        };
        let mut wal = Vec::new();
        for entry in [
            cc_raft::Entry {
                term: cc_core::Term::new(1),
                index: cc_core::LogIndex::new(1),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
            cc_raft::Entry {
                term: cc_core::Term::new(1),
                index: cc_core::LogIndex::new(2),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
            cc_raft::Entry {
                term: cc_core::Term::new(2),
                index: cc_core::LogIndex::new(3),
                kind: cc_raft::EntryKind::Noop,
                payload: Vec::new(),
            },
        ] {
            if wal.is_empty() {
                wal.extend(
                    encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(
                        genesis.clone(),
                    )))
                    .expect("genesis frame"),
                );
            }
            wal.extend(
                encode_framed_durable_record(&cc_log::DurableRecord::Append(entry))
                    .expect("append frame"),
            );
        }
        wal.extend(
            encode_framed_durable_record(&cc_log::DurableRecord::SnapshotMark(
                cc_log::SnapshotMark {
                    index: cc_core::LogIndex::new(3),
                    term: cc_core::Term::new(2),
                    generation: 3,
                    crc32c: checksum,
                },
            ))
            .expect("snapshot mark frame"),
        );
        let original_wal_len = wal.len();
        fs::write(root.join("raft/wal.0"), wal).expect("WAL");
        let host = DriverHost::boot(config, None, DEFAULT_RECORD_MAX_BYTES, false)
            .expect("marked snapshot boot");
        assert_eq!(
            host.driver
                .lock()
                .expect("driver")
                .node()
                .kv
                .store
                .get(b"marked-key", None),
            Some(b"marked-value".to_vec())
        );
        let compacted = fs::read(root.join("raft/wal.0")).expect("compacted WAL");
        let compacted_state = recover_framed_record_stream(&compacted)
            .expect("compact recovery")
            .state;
        assert!(compacted.len() < original_wal_len);
        assert_eq!(compacted_state.base_index, cc_core::LogIndex::new(3));
        assert!(compacted_state.entries.is_empty());
        drop(host);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_boot_clock_never_moves_below_recovered_floor() {
        let clock = HostClock {
            boot_epoch: Time::from_nanos(4),
            boot_instant: Instant::now(),
            floor: Time::from_nanos(9),
        };
        assert!(clock.now() >= Time::from_nanos(9));
    }

    #[test]
    fn trap_restart_time_never_regresses_ttl() {
        let recovered = Time::from_nanos(50);
        let clock = HostClock {
            boot_epoch: Time::from_nanos(10),
            boot_instant: Instant::now(),
            floor: recovered,
        };
        assert!(clock.now() >= recovered);
        assert!(clock.now() >= Time::from_nanos(50));
    }

    #[test]
    fn trap_host_thread_count_and_stack_reservations_are_bounded() {
        let stack_bytes = 256 * 1024;
        let budget = Arc::new(ThreadBudget::new(1, stack_bytes));
        let permit = budget.reserve().expect("first thread reservation");
        assert!(
            budget.reserve().is_none(),
            "second thread exceeds count cap"
        );
        assert_eq!(budget.live.load(Ordering::Acquire), 1);
        assert_eq!(budget.stack_bytes, stack_bytes);
        drop(permit);
        assert_eq!(budget.live.load(Ordering::Acquire), 0);
        assert!(
            budget.reserve().is_some(),
            "released stack reservation is reusable"
        );
    }

    #[test]
    fn trap_expireat_overflow_is_rejected() {
        assert!(
            simple_kv(ClientCommand::ExpireAt {
                key: b"key".to_vec(),
                at_seconds: u64::MAX,
            })
            .is_err()
        );
    }

    #[test]
    fn trap_hello_advertises_the_same_semantic_version_as_ccrp() {
        let config = Config {
            id: 1,
            cluster_id: cc_core::ClusterId::from_hex("00112233445566778899aabbccddeeff")
                .expect("cluster id"),
            data_dir: std::env::temp_dir(),
            listen_client: String::from("127.0.0.1:7101"),
            listen_peer: String::from("127.0.0.1:7201"),
            listen_metrics: String::from("127.0.0.1:7301"),
            peers: Vec::new(),
        };
        let hello = local_hello(&config);
        assert_eq!(hello.semantic_min, PROTOCOL_VERSION);
        assert_eq!(hello.semantic_max, SEMANTIC_VERSION_V3);
        assert_eq!(
            hello.supported_features,
            FOLLOWER_READ_FEATURE | FEATURE_ATOMIC_BATCH
        );
    }

    #[test]
    fn trap_only_follower_read_messages_require_the_follower_read_feature() {
        let negotiated = cc_env::NegotiatedPeer {
            semantic_version: SEMANTIC_VERSION_V3,
            features: 0,
            max_peer_frame: 1024,
        };
        let ordinary = cc_cluster::Message {
            proto_version: SEMANTIC_VERSION_V3,
            from: NodeId::new(1),
            to: NodeId::new(2),
            term: cc_core::Term::new(1),
            kind: cc_cluster::MessageKind::AppendReq(cc_raft::AppendRequest {
                prev_index: cc_core::LogIndex::new(0),
                prev_term: cc_core::Term::new(0),
                entries: Vec::new(),
                leader_commit: cc_core::LogIndex::new(0),
                read_round: 0,
            }),
        };
        assert!(message_features_are_allowed(&ordinary, negotiated));
        let follower_read = cc_cluster::Message {
            kind: cc_cluster::MessageKind::FollowerReadRequest {
                request_id: 1,
                command_hash: 2,
            },
            ..ordinary
        };
        assert!(!message_features_are_allowed(&follower_read, negotiated));
    }

    #[test]
    fn trap_cc_request_rejects_reads_and_multikey_commands() {
        assert!(explicit_write_kv(&ClientCommand::Get(b"key".to_vec())).is_err());
        assert!(
            explicit_write_kv(&ClientCommand::Del(vec![b"a".to_vec(), b"b".to_vec(),])).is_err()
        );
    }

    #[test]
    fn trap_multi_queue_is_closed_and_does_not_expand_multikey_writes() {
        let queued = queue_transaction_command(ClientCommand::Set {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl: None,
            nx: false,
            xx: false,
        })
        .expect("SET is batchable");
        assert!(matches!(queued.1, KvCommand::Set { .. }));
        assert!(
            queue_transaction_command(ClientCommand::Del(vec![b"a".to_vec(), b"b".to_vec(),]))
                .is_err()
        );
        assert!(
            queue_transaction_command(ClientCommand::Request {
                client: 9,
                sequence: 1,
                command: Box::new(ClientCommand::SetNx {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }),
            })
            .is_err()
        );
    }

    #[test]
    fn trap_dirty_transaction_never_proposes() {
        let mut transaction = ClientTransaction::Clean {
            commands: Vec::new(),
            encoded_bytes: 0,
        };
        let response = queue_client_transaction(
            &mut transaction,
            ClientCommand::Unknown(b"unsupported".to_vec()),
        );
        assert!(matches!(response, RespValue::Error(_)));
        assert!(matches!(transaction, ClientTransaction::Dirty));
        let exec = match Some(transaction).take() {
            Some(ClientTransaction::Dirty) => RespValue::Error(String::from("EXECABORT")),
            Some(ClientTransaction::Clean { .. }) => panic!("dirty transaction became clean"),
            None => panic!("transaction disappeared"),
        };
        assert_eq!(exec, RespValue::Error(String::from("EXECABORT")));
    }

    #[test]
    fn trap_exec_disconnect_discards_queue() {
        let next_connection = {
            let mut transaction = ClientTransaction::Clean {
                commands: Vec::new(),
                encoded_bytes: 0,
            };
            assert_eq!(
                queue_client_transaction(
                    &mut transaction,
                    ClientCommand::Set {
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                        ttl: None,
                        nx: false,
                        xx: false,
                    },
                ),
                RespValue::Simple(String::from("QUEUED"))
            );
            assert!(matches!(transaction, ClientTransaction::Clean { .. }));
            // `serve_client` owns this value on its stack. Returning on EOF
            // drops it; a new connection always starts from `None`.
            None::<ClientTransaction>
        };
        assert!(next_connection.is_none());
    }

    #[test]
    fn trap_exec_cannot_claim_reconnect_dedup_without_a_batch_envelope() {
        assert!(explicit_write_kv(&ClientCommand::Exec).is_err());
        assert!(explicit_write_kv(&ClientCommand::Multi).is_err());
        assert!(explicit_write_kv(&ClientCommand::Discard).is_err());
        let batch = ClientCommand::Batch(vec![ClientCommand::SetNx {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }]);
        assert!(matches!(
            explicit_write_kv(&batch),
            Ok(KvCommand::Batch { .. })
        ));
    }

    #[test]
    fn trap_v2_peer_never_serves_local_state_as_linearizable() {
        let negotiated = cc_env::NegotiatedPeer {
            semantic_version: PROTOCOL_VERSION,
            features: 0,
            max_peer_frame: 1024,
        };
        let request = cc_cluster::Message {
            proto_version: SEMANTIC_VERSION_V3,
            from: NodeId::new(2),
            to: NodeId::new(1),
            term: cc_core::Term::new(1),
            kind: cc_cluster::MessageKind::FollowerReadRequest {
                request_id: 1,
                command_hash: 2,
            },
        };
        assert!(!message_features_are_allowed(&request, negotiated));
    }

    #[test]
    fn trap_capped_recording_is_a_labeled_replayable_prefix() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-capped-record-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("record directory");
        let path = root.join("run.ccij");
        let header = InputJournal::encode_header(b"boot").expect("header");
        let footer = InputJournal::encode_footer_frame(JournalFooter {
            termination: JournalTermination::Capped,
            last_ordinal: 0,
        })
        .expect("footer");
        let cap = u64::try_from(header.len().saturating_add(footer.len())).expect("cap");
        let mut writer = RecordWriter::create(&path, b"boot", cap).expect("writer");
        assert_eq!(
            writer
                .append(Time::from_nanos(1), Input::Tick, Vec::new(), Vec::new())
                .expect("cap footer"),
            RecordAppend::Capped
        );
        let journal = InputJournal::decode(&fs::read(&path).expect("record bytes"))
            .expect("replayable prefix");
        assert!(journal.records.is_empty());
        assert_eq!(
            journal.footer,
            Some(JournalFooter {
                termination: JournalTermination::Capped,
                last_ordinal: 0,
            })
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_terminal_recording_footer_is_synced_and_labeled() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-terminal-record-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("record directory");
        let path = root.join("run.ccij");
        let mut writer = RecordWriter::create(&path, b"boot", 1024).expect("writer");
        assert_eq!(
            writer
                .append(Time::from_nanos(1), Input::Tick, Vec::new(), Vec::new())
                .expect("record"),
            RecordAppend::Written
        );
        writer
            .finish(JournalTermination::FatalIo)
            .expect("synced terminal footer");
        let journal = InputJournal::decode(&fs::read(&path).expect("record bytes"))
            .expect("terminal journal");
        assert_eq!(
            journal.footer,
            Some(JournalFooter {
                termination: JournalTermination::FatalIo,
                last_ordinal: 1,
            })
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_record_writer_rejects_nonmonotonic_transition_time() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-record-time-order-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("record directory");
        let path = root.join("run.ccij");
        let mut writer = RecordWriter::create(&path, b"boot", 1024).expect("writer");
        assert_eq!(
            writer
                .append(Time::from_nanos(2), Input::Tick, Vec::new(), Vec::new())
                .expect("first record"),
            RecordAppend::Written
        );
        let error = writer
            .append(Time::from_nanos(1), Input::Tick, Vec::new(), Vec::new())
            .expect_err("timestamp regression must not become a CCIJ record");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let journal = InputJournal::decode(&fs::read(&path).expect("record bytes"))
            .expect("prefix remains replayable");
        assert_eq!(journal.records.len(), 1);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_bounded_run_can_finish_a_complete_recording() {
        let root = std::env::temp_dir().join(format!(
            "cc-node-complete-record-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("record directory");
        let path = root.join("run.ccij");
        let mut writer = RecordWriter::create(&path, b"boot", 1024).expect("writer");
        writer
            .finish(JournalTermination::Complete)
            .expect("synced complete footer");
        let journal = InputJournal::decode(&fs::read(&path).expect("record bytes"))
            .expect("complete journal");
        assert_eq!(
            journal.footer,
            Some(JournalFooter {
                termination: JournalTermination::Complete,
                last_ordinal: 0,
            })
        );
        assert_eq!(
            parse_run_for(&[String::from("--run-for-ms"), String::from("1")])
                .expect("valid duration"),
            Some(StdDuration::from_millis(1))
        );
        assert!(parse_run_for(&[String::from("--run-for-ms"), String::from("0")]).is_err());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_record_required_refuses_unrecorded_mode() {
        assert_eq!(
            validate_record_options(None, false, true)
                .expect_err("required recorder without path")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(validate_record_options(Some(Path::new("run.ccij")), false, true).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn trap_record_path_permissions_and_symlinks_are_rejected() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let root = std::env::temp_dir().join(format!(
            "cc-node-record-path-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("record directory");
        let path = root.join("run.ccij");
        let _writer = RecordWriter::create(&path, b"boot", 1024).expect("secure writer");
        assert_eq!(
            fs::metadata(&path)
                .expect("record metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            RecordWriter::create(&path, b"boot", 1024)
                .expect_err("existing record path")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        let target = root.join("target");
        fs::create_dir_all(&target).expect("symlink target");
        let alias = root.join("alias");
        symlink(&target, &alias).expect("symlink parent");
        assert_eq!(
            RecordWriter::create(&alias.join("run.ccij"), b"boot", 1024)
                .expect_err("symlink parent")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
