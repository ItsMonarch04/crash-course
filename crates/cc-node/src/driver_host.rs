// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! The real, blocking socket/filesystem adapter for the shared host driver.
//!
//! This module owns clocks, TCP, CCHL negotiation, and filesystem handles.
//! Consensus, client-command application, I/O correlation, and CCRP encoding
//! stay below this boundary in `cc-host`/`cc-cluster`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};

use cc_cluster::{NodeConfig, NodeError, PROTOCOL_VERSION, RaftConfig, RecoveredNode};
use cc_core::{
    ClientId, ClusterPolicy, HostLimits, MembershipState, NodeId, RequestSeq, Seed, Time,
};
use cc_env::{
    Effect, FileId, Input, IoResult, PeerHello, WireMsg, decode_peer_frame, encode_peer_frame,
};
use cc_host::{
    BootState, Driver, HostError,
    journal::{
        BlockObservation, InputJournal, JournalFooter, JournalRecord, JournalTermination,
        RecordedBootImage, RecordingBlockSource,
    },
};
use cc_kv::{KvCommand, KvReply, SetCondition, decode_reply, encode_command};
use cc_log::{Genesis, Origin, encode_framed_durable_record, recover_framed_record_stream};
use cc_resp::{ClientCommand, MAX_FRAME, RespValue, encode, parse, parse_command};
use cc_store::{FileBlockSource, StoreConfig};

use super::{
    Config, Peer, has_flag, metrics_dashboard, read_config, to_resp, unsafe_listener_warning,
    validate_identity, validate_listener_safety,
};

const PEER_CONNECT_TIMEOUT: StdDuration = StdDuration::from_millis(250);
const PEER_IO_TIMEOUT: StdDuration = StdDuration::from_millis(500);
const CLIENT_REPLY_TIMEOUT: StdDuration = StdDuration::from_secs(3);
const TICK_INTERVAL: StdDuration = StdDuration::from_millis(10);
const SEMANTIC_VERSION: u16 = PROTOCOL_VERSION;
const DEFAULT_RECORD_MAX_BYTES: u64 = 128 * 1024 * 1024;

type ReplyRoute = (u64, u64);
type ReplySender = mpsc::SyncSender<Vec<u8>>;

pub(crate) fn run(args: &[String]) -> io::Result<()> {
    let config_path = super::flag(args, "--config").unwrap_or_else(|| String::from("ccdb.toml"));
    let config = read_config(Path::new(&config_path))?;
    validate_identity(&config)?;
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

    let client_listener = TcpListener::bind(&config.listen_client)?;
    let peer_listener = TcpListener::bind(&config.listen_peer)?;
    let metrics_listener = TcpListener::bind(&config.listen_metrics)?;
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

    if run_for.is_some() {
        client_listener.set_nonblocking(true)?;
    }

    let ticker = Arc::clone(&state);
    if !spawn_host_thread(Arc::clone(&state.thread_budget), "ccdb-tick", move || {
        loop {
            thread::sleep(TICK_INTERVAL);
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
            for result in metrics_listener.incoming() {
                match result {
                    Ok(stream) => {
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
            for result in peer_listener.incoming() {
                match result {
                    Ok(stream) => {
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
        match client_listener.accept() {
            Ok((stream, _)) => {
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
    replies: Mutex<BTreeMap<ReplyRoute, ReplySender>>,
    next_client: AtomicU64,
    next_request: AtomicU64,
    stopping: AtomicBool,
    metrics: HostMetrics,
    thread_budget: Arc<ThreadBudget>,
    recorder: Option<Mutex<Option<RecordWriter>>>,
    record_required: bool,
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
        let host_limits = HostLimits::default();
        let membership = membership(&config)?;
        let genesis = Genesis {
            origin: Origin::Bootstrap,
            cluster_id: config.cluster_id.bytes(),
            policy: ClusterPolicy::default(),
            membership: membership.clone(),
        };
        let raft_dir = config.data_dir.join("raft");
        fs::create_dir_all(&raft_dir)?;
        fs::create_dir_all(config.data_dir.join("store/sst"))?;
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
        if recovered.state.genesis != genesis {
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

        let recovered_node = RecoveredNode {
            hard_state: recovered.state.hard_state,
            log_base: (recovered.state.base_index, recovered.state.base_term),
            entries: recovered.state.entries,
            membership: recovered.state.genesis.membership,
            cluster_policy: ClusterPolicy::default(),
            snapshot: None,
            durable_applied: (recovered.state.base_index, recovered.state.base_term),
        };
        let effective_config = node_config(&config, host_limits);
        let clock = HostClock::new(Time::from_nanos(0))?;
        let driver = Driver::boot_with_wal_offset(
            effective_config,
            BootState::Recovered(Box::new(recovered_node)),
            recovered.bytes_consumed,
        )
        .map_err(io::Error::other)?;
        let recorder = record_path
            .map(|path| {
                let boot_image = RecordedBootImage {
                    config: effective_config,
                    cluster_id: config.cluster_id.bytes(),
                    membership: membership.clone(),
                    boot_epoch: clock.boot_epoch,
                    build_label: String::from(env!("CARGO_PKG_VERSION")),
                    wal: bytes.clone(),
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
                FileBlockSource::new(config.data_dir.join("store/sst"))
                    .map_err(io::Error::other)?,
            )),
            wal: Mutex::new(wal),
            replies: Mutex::new(BTreeMap::new()),
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
        if self.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        let effects = {
            let mut driver = self.driver.lock().map_err(|_| HostError::TimeOverflow)?;
            if self.stopping.load(Ordering::Acquire) {
                return Ok(());
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
                    self.record(now, input, blocks.take_observations(), effects.clone())?;
                    effects
                }
                // A filesystem operation is a strict durability barrier. TCP
                // and timer work that arrives behind it is admitted to the
                // driver's bounded queues, never passed through to Raft early.
                Err(HostError::Node(NodeError::PersistencePending)) => {
                    driver.enqueue(input)?;
                    return Ok(());
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
                    self.record(now, input, blocks.take_observations(), effects.clone())?;
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
        for effect in effects {
            match effect {
                Effect::Send { to, msg } => {
                    self.metrics.peer_frames.fetch_add(1, Ordering::Relaxed);
                    if let Err(error) = self.send_peer(to, msg) {
                        // A Raft send is lossy by design; later heartbeat/election
                        // traffic retries it. Never turn transport reachability
                        // into a fabricated successful reply.
                        eprintln!("peer send failed: {error}");
                    }
                }
                Effect::DiskWrite {
                    file,
                    at,
                    bytes,
                    id,
                } => {
                    let len = self.write_wal(file, at, &bytes).unwrap_or_else(|error| {
                        if let Err(record_error) =
                            self.finish_recording(JournalTermination::FatalIo)
                        {
                            eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                        }
                        super::fatal_disk(&format!("WAL write failed: {error}"))
                    });
                    self.metrics.fsync_writes.fetch_add(1, Ordering::Relaxed);
                    self.deliver(Input::IoDone {
                        id,
                        result: IoResult::Written { len },
                    })?;
                }
                Effect::DiskFsync { file, id } => {
                    self.sync_wal(file).unwrap_or_else(|error| {
                        if let Err(record_error) =
                            self.finish_recording(JournalTermination::FatalIo)
                        {
                            eprintln!("CCIJ fatal-I/O footer failed: {record_error}");
                        }
                        super::fatal_disk(&format!("WAL fsync failed: {error}"))
                    });
                    self.deliver(Input::IoDone {
                        id,
                        result: IoResult::Fsynced,
                    })?;
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
        Ok(())
    }

    fn write_wal(&self, file: FileId, at: u64, bytes: &[u8]) -> io::Result<u32> {
        if file != (FileId::Wal { segment: 0 }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected disk file",
            ));
        }
        if std::env::var_os("CCDB_FAIL_ENOSPC").is_some() {
            return Err(io::Error::from(io::ErrorKind::StorageFull));
        }
        let len = u32::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL write too large"))?;
        let mut wal = self
            .wal
            .lock()
            .map_err(|_| io::Error::other("WAL mutex poisoned"))?;
        if wal.metadata()?.len() != at {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL append offset mismatch",
            ));
        }
        wal.seek(SeekFrom::Start(at))?;
        wal.write_all(bytes)?;
        Ok(len)
    }

    fn sync_wal(&self, file: FileId) -> io::Result<()> {
        if file != (FileId::Wal { segment: 0 }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unexpected disk file",
            ));
        }
        if std::env::var_os("CCDB_FAIL_FSYNC").is_some() {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.wal
            .lock()
            .map_err(|_| io::Error::other("WAL mutex poisoned"))?
            .sync_data()
    }

    fn send_peer(&self, to: NodeId, msg: WireMsg) -> io::Result<()> {
        let peer = self.peer(to)?;
        let mut stream =
            TcpStream::connect_timeout(&resolve_peer_addr(&peer.address)?, PEER_CONNECT_TIMEOUT)?;
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
        if msg.proto_version != negotiated.semantic_version {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CCRP semantic version differs from negotiated CCHL version",
            ));
        }
        stream.write_all(&encode_peer_frame(&msg).map_err(io::Error::other)?)
    }

    fn peer(&self, id: NodeId) -> io::Result<&Peer> {
        self.config
            .peers
            .iter()
            .find(|peer| peer.id == id.get())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unknown peer node id"))
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

    fn render_metrics(&self) -> String {
        let (leader, commit, applied) = self.driver.lock().map_or((None, 0, 0), |driver| {
            let node = driver.node();
            (
                node.leader().map(NodeId::get),
                node.raft.commit_index.get(),
                node.raft.applied_index.get(),
            )
        });
        format!(
            "{}# TYPE ccdb_up gauge\nccdb_up 1\n# TYPE ccdb_node_id gauge\nccdb_node_id {}\n# TYPE ccdb_is_leader gauge\nccdb_is_leader {}\n# TYPE ccdb_leader_node_id gauge\nccdb_leader_node_id {}\n# TYPE ccdb_commit_index gauge\nccdb_commit_index {}\n# TYPE ccdb_applied_index gauge\nccdb_applied_index {}\n# TYPE ccdb_peers_configured gauge\nccdb_peers_configured {}\n",
            self.metrics.render(),
            self.config.id,
            u8::from(leader == Some(self.config.id)),
            leader.unwrap_or(0),
            commit,
            applied,
            self.config.peers.len(),
        )
    }
}

#[derive(Default)]
struct HostMetrics {
    started: OnceInstant,
    commands: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    fsync_writes: AtomicU64,
    peer_frames: AtomicU64,
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
    fn render(&self) -> String {
        format!(
            "# TYPE ccdb_commands_total counter\nccdb_commands_total {}\n# TYPE ccdb_reads_total counter\nccdb_reads_total {}\n# TYPE ccdb_writes_total counter\nccdb_writes_total {}\n# TYPE ccdb_fsyncs_total counter\nccdb_fsyncs_total {}\n# TYPE ccdb_peer_frames_total counter\nccdb_peer_frames_total {}\n# TYPE ccdb_uptime_seconds gauge\nccdb_uptime_seconds {}\n",
            self.commands.load(Ordering::Relaxed),
            self.reads.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.fsync_writes.load(Ordering::Relaxed),
            self.peer_frames.load(Ordering::Relaxed),
            self.started.0.elapsed().as_secs(),
        )
    }
}

enum SubmitError {
    NotLeader(Option<NodeId>),
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
            let response = execute_client(state, client, command)
                .unwrap_or_else(|error| RespValue::Error(error.to_string()));
            stream.write_all(&encode(&response))?;
        }
    }
}

fn execute_client(
    state: &DriverHost,
    client: ClientId,
    command: ClientCommand,
) -> io::Result<RespValue> {
    match command {
        ClientCommand::Ping => Ok(RespValue::Simple(String::from("PONG"))),
        ClientCommand::Echo(value) => Ok(RespValue::Bulk(Some(value))),
        ClientCommand::Info => Ok(RespValue::Bulk(Some(info(state).into_bytes()))),
        ClientCommand::Request {
            client: session_client,
            sequence,
            command,
        } => execute_explicit_request(state, client, session_client, sequence, *command),
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
        ClientCommand::ExpireAt { key, at_seconds } => KvCommand::ExpireAt {
            key,
            at: Time::from_nanos(at_seconds.saturating_mul(1_000_000_000)),
        },
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
                .map(|id| (id.get(), client_address_for(&state.config, id.get())))
                .unwrap_or((0, String::from("unknown")));
            Err(io::Error::other(format!(
                "NOTLEADER leader=n{leader} addr={address}"
            )))
        }
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
        client_address_for(&state.config, leader),
    )
}

fn serve_peer(mut stream: TcpStream, state: &Arc<DriverHost>) -> io::Result<()> {
    stream.set_read_timeout(Some(PEER_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(PEER_IO_TIMEOUT))?;
    let (hello, mut buffer) = read_hello(&mut stream)?;
    let local = local_hello(&state.config);
    let negotiated = local.negotiate(&hello).map_err(io::Error::other)?;
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
            if message.proto_version != negotiated.semantic_version {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CCRP semantic version differs from negotiated CCHL version",
                ));
            }
            state.metrics.peer_frames.fetch_add(1, Ordering::Relaxed);
            state
                .deliver(Input::Recv {
                    from: hello.node_id,
                    msg: message,
                })
                .map_err(io::Error::other)?;
        }
        let read = stream.read(&mut scratch)?;
        if read == 0 {
            return Ok(());
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

fn membership(config: &Config) -> io::Result<MembershipState> {
    MembershipState::new(
        config
            .peers
            .iter()
            .map(|peer| NodeId::new(peer.id))
            .collect::<BTreeSet<_>>(),
    )
    .map_err(io::Error::other)
}

fn node_config(config: &Config, host_limits: HostLimits) -> NodeConfig {
    NodeConfig {
        id: NodeId::new(config.id),
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
        semantic_min: SEMANTIC_VERSION,
        semantic_max: SEMANTIC_VERSION,
        supported_features: 0,
        required_features: 0,
        max_peer_frame: u32::try_from(cc_env::MAX_PEER_FRAME).expect("peer frame limit fits u32"),
    }
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
        assert_eq!(hello.semantic_max, PROTOCOL_VERSION);
    }

    #[test]
    fn trap_cc_request_rejects_reads_and_multikey_commands() {
        assert!(explicit_write_kv(&ClientCommand::Get(b"key".to_vec())).is_err());
        assert!(
            explicit_write_kv(&ClientCommand::Del(vec![b"a".to_vec(), b"b".to_vec(),])).is_err()
        );
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
