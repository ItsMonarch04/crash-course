// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

//! `ccdb` is deliberately a thin operator/transport adapter.  Raft, durable
//! continuations, and application state all live below `driver_host`.

mod driver_host;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use cc_core::{ClusterId, ClusterPolicy, Time, crc32c};
use cc_kv::KvReply;
use cc_resp::{MAX_FRAME, RespValue, encode, parse};

const BACKUP_MAGIC: &[u8; 4] = b"CCBK";
const BACKUP_VERSION: u16 = 1;
const BACKUP_MAX_FILE: usize = 1024 * 1024 * 1024;
const IDENTITY_MAGIC: &[u8; 4] = b"CCID";
const IDENTITY_VERSION: u16 = 1;
const IDENTITY_LEN: usize = 55;
const IDENTITY_ACTIVE: u8 = 1;
const IDENTITY_JOINING: u8 = 2;
const IDENTITY_REMOVED: u8 = 3;
const MIN_STORAGE_READER: u16 = 1;
const MIN_SEMANTIC_READER: u16 = 2;

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("init") => init_cluster(&args[1..]),
        Some("run") => driver_host::run(&args[1..]),
        Some("peer") => driver_host::peer_probe(&args[1..]),
        Some("selfcheck") => selfcheck(&args[1..]),
        Some("doctor") => doctor(&args[1..]),
        Some("admin") => admin(&args[1..]),
        _ => {
            print_help();
            Ok(())
        }
    }
}

fn init_cluster(args: &[String]) -> io::Result<()> {
    let cluster = flag(args, "--cluster").unwrap_or_else(|| String::from("demo"));
    let cluster_id = flag(args, "--cluster-id")
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "init requires --cluster-id as 32 lowercase hexadecimal characters",
            )
        })
        .and_then(|value| parse_cluster_id(&value))?;
    if let Some(data_dir) = flag(args, "--data-dir") {
        let node = flag(args, "--node-id")
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "single-node initialization requires --node-id",
                )
            })?
            .parse::<u64>()
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "node id must be a nonzero u64")
            })?;
        initialize_data_dir(Path::new(&data_dir), cluster_id, node)?;
        println!(
            "initialized cluster={cluster} cluster_id={cluster_id} node={node} data_dir={data_dir}"
        );
        return Ok(());
    }
    let nodes = flag(args, "--nodes")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3);
    if nodes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "init --nodes must be nonzero",
        ));
    }
    let base = flag(args, "--base-dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("ccdb-data"));
    fs::create_dir_all(&base)?;
    for node in 1..=nodes {
        let data_dir = base.join(format!("n{node}"));
        initialize_data_dir(&data_dir, cluster_id, node)?;
        let port = 7100 + node;
        let peer_port = 7200 + node;
        let peer_nodes = (1..=nodes)
            .map(|peer| format!("127.0.0.1:{}", 7200 + peer))
            .collect::<Vec<_>>()
            .join(",");
        fs::write(
            data_dir.join("ccdb.toml"),
            format!(
                "[node]\nid = {node}\ncluster_id = \"{cluster_id}\"\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:{port}\"\nlisten_peer = \"127.0.0.1:{peer_port}\"\nlisten_metrics = \"127.0.0.1:{}\"\npeer_nodes = \"{peer_nodes}\"\n\n[storage]\nfsync = \"always\"\n",
                data_dir.display(),
                7300 + node
            ),
        )?;
        sync_directory(&data_dir)?;
    }
    sync_directory(&base)?;
    println!(
        "initialized cluster={cluster} cluster_id={cluster_id} nodes={nodes} base={}",
        base.display()
    );
    Ok(())
}

fn initialize_data_dir(data_dir: &Path, cluster_id: ClusterId, node: u64) -> io::Result<()> {
    if node == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node id must be nonzero",
        ));
    }
    reject_symlink(data_dir, "data directory")?;
    if data_dir.exists() && fs::read_dir(data_dir)?.next().transpose()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "refusing to initialize nonempty data directory {}",
                data_dir.display()
            ),
        ));
    }
    fs::create_dir_all(data_dir.join("raft"))?;
    fs::create_dir_all(data_dir.join("store/sst"))?;
    fs::create_dir_all(data_dir.join("snapshots/staging"))?;
    write_identity(
        &identity_path(data_dir),
        DiskIdentity::fresh(cluster_id, node),
    )?;
    sync_directory(data_dir)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Peer {
    pub(crate) id: u64,
    pub(crate) address: String,
}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) id: u64,
    pub(crate) cluster_id: ClusterId,
    pub(crate) data_dir: PathBuf,
    pub(crate) listen_client: String,
    pub(crate) listen_peer: String,
    pub(crate) listen_metrics: String,
    pub(crate) peers: Vec<Peer>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DiskIdentity {
    cluster_id: ClusterId,
    node_id: u64,
    lifecycle: u8,
    policy_hash: u64,
    min_storage_reader: u16,
    min_semantic_reader: u16,
    migration_epoch: u64,
}

impl DiskIdentity {
    fn fresh(cluster_id: ClusterId, node_id: u64) -> Self {
        Self {
            cluster_id,
            node_id,
            lifecycle: IDENTITY_ACTIVE,
            policy_hash: ClusterPolicy::default().hash(),
            min_storage_reader: MIN_STORAGE_READER,
            min_semantic_reader: MIN_SEMANTIC_READER,
            migration_epoch: 0,
        }
    }

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(IDENTITY_LEN);
        bytes.extend_from_slice(IDENTITY_MAGIC);
        bytes.extend_from_slice(&IDENTITY_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.cluster_id.bytes());
        bytes.extend_from_slice(&self.node_id.to_le_bytes());
        bytes.push(self.lifecycle);
        bytes.extend_from_slice(&self.policy_hash.to_le_bytes());
        bytes.extend_from_slice(&self.min_storage_reader.to_le_bytes());
        bytes.extend_from_slice(&self.min_semantic_reader.to_le_bytes());
        bytes.extend_from_slice(&self.migration_epoch.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let checksum = crc32c(&bytes);
        let checksum_start = bytes.len() - 4;
        bytes[checksum_start..].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != IDENTITY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("CCID must be exactly {IDENTITY_LEN} bytes"),
            ));
        }
        if &bytes[..4] != IDENTITY_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid CCID magic",
            ));
        }
        if u16::from_le_bytes(bytes[4..6].try_into().expect("CCID version")) != IDENTITY_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported CCID format version",
            ));
        }
        let expected = u32::from_le_bytes(bytes[IDENTITY_LEN - 4..].try_into().expect("CCID CRC"));
        let mut crc_bytes = bytes.to_vec();
        crc_bytes[IDENTITY_LEN - 4..].fill(0);
        if crc32c(&crc_bytes) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CCID checksum mismatch",
            ));
        }
        let mut cluster = [0_u8; 16];
        cluster.copy_from_slice(&bytes[6..22]);
        let identity = Self {
            cluster_id: ClusterId::new(cluster),
            node_id: u64::from_le_bytes(bytes[22..30].try_into().expect("CCID node")),
            lifecycle: bytes[30],
            policy_hash: u64::from_le_bytes(bytes[31..39].try_into().expect("CCID policy")),
            min_storage_reader: u16::from_le_bytes(bytes[39..41].try_into().expect("CCID storage")),
            min_semantic_reader: u16::from_le_bytes(
                bytes[41..43].try_into().expect("CCID semantic"),
            ),
            migration_epoch: u64::from_le_bytes(bytes[43..51].try_into().expect("CCID epoch")),
        };
        if identity.cluster_id.is_zero()
            || identity.node_id == 0
            || !matches!(
                identity.lifecycle,
                IDENTITY_ACTIVE | IDENTITY_JOINING | IDENTITY_REMOVED
            )
            || identity.min_storage_reader == 0
            || identity.min_semantic_reader == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid CCID fields",
            ));
        }
        Ok(identity)
    }
}

fn identity_path(data_dir: &Path) -> PathBuf {
    data_dir.join("identity.ccid")
}

fn parse_cluster_id(value: &str) -> io::Result<ClusterId> {
    ClusterId::from_hex(value).map_err(|reason| io::Error::new(io::ErrorKind::InvalidInput, reason))
}

fn write_identity(path: &Path, identity: DiskIdentity) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "CCID identity path has no parent",
        )
    })?;
    reject_symlink(parent, "CCID parent directory")?;
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(&identity.encode())?;
    file.sync_all()?;
    sync_directory(parent)
}

fn reject_symlink(path: &Path, what: &str) -> io::Result<()> {
    if fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} must not be a symbolic link: {}", path.display()),
        ));
    }
    Ok(())
}

pub(crate) fn validate_identity(config: &Config) -> io::Result<()> {
    reject_symlink(&config.data_dir, "data directory")?;
    let marker = identity_path(&config.data_dir);
    reject_symlink(&marker, "CCID identity")?;
    // This is deliberately a boundary refusal, rather than a compatibility
    // reader. The old journal has no proof compatible with the shared driver.
    if config.data_dir.join("commands.log").exists() && !config.data_dir.join("raft/wal.0").exists()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "pre-N1 data directory found (commands.log); migration is intentionally unsupported",
        ));
    }
    let bytes = fs::read(&marker).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("data-dir CCID identity {}: {error}", marker.display()),
        )
    })?;
    let identity = DiskIdentity::decode(&bytes)?;
    if identity.node_id != config.id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "data-dir identity mismatch: config id={} CCID node id={}",
                config.id, identity.node_id
            ),
        ));
    }
    if identity.cluster_id != config.cluster_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "data-dir cluster identity does not match node.cluster_id",
        ));
    }
    if identity.lifecycle == IDENTITY_REMOVED {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "CCID lifecycle Removed is terminal for this data directory",
        ));
    }
    if identity.policy_hash != ClusterPolicy::default().hash() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CCID policy hash does not match configured cluster policy",
        ));
    }
    if identity.min_storage_reader > MIN_STORAGE_READER
        || identity.min_semantic_reader > MIN_SEMANTIC_READER
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CCID requires an unsupported storage or semantic reader",
        ));
    }
    Ok(())
}

pub(crate) fn read_config(path: &Path) -> io::Result<Config> {
    parse_config(&fs::read_to_string(path)?)
}

fn parse_config(text: &str) -> io::Result<Config> {
    let mut values = BTreeMap::new();
    let mut section = String::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len().saturating_sub(1)].to_owned();
            if !matches!(section.as_str(), "node" | "storage") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown config section {section} at line {}", index + 1),
                ));
            }
            continue;
        }
        let (key, raw_value) = line.split_once('=').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid config assignment at line {}", index + 1),
            )
        })?;
        let key = key.trim();
        let value = raw_value.trim().trim_matches('"').to_owned();
        let allowed = match section.as_str() {
            "node" => matches!(
                key,
                "id" | "cluster_id"
                    | "data_dir"
                    | "listen_client"
                    | "listen_peer"
                    | "listen_metrics"
                    | "peer_nodes"
            ),
            "storage" => key == "fsync",
            _ => false,
        };
        if !allowed {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "unknown safety-critical config key {key} at line {}",
                    index + 1
                ),
            ));
        }
        let composite = format!("{section}.{key}");
        if values.insert(composite, value).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate config key {key} at line {}", index + 1),
            ));
        }
    }
    let required = |key: &str| {
        values.get(&format!("node.{key}")).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("missing required node.{key}"),
            )
        })
    };
    let id = required("id")?.parse::<u64>().map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "node.id must be a nonzero u64")
    })?;
    if id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node.id must be nonzero",
        ));
    }
    let cluster_id = parse_cluster_id(&required("cluster_id")?)?;
    let data_dir = PathBuf::from(required("data_dir")?);
    if data_dir.as_os_str().is_empty()
        || data_dir
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node.data_dir must not contain a parent traversal",
        ));
    }
    let listen_client = required("listen_client")?;
    let listen_peer = required("listen_peer")?;
    let listen_metrics = required("listen_metrics")?;
    for (name, address) in [
        ("listen_client", &listen_client),
        ("listen_peer", &listen_peer),
        ("listen_metrics", &listen_metrics),
    ] {
        if address.is_empty() || address.to_socket_addrs().ok().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("node.{name} is not a resolvable socket address"),
            ));
        }
    }
    let peer_addresses = required("peer_nodes")?
        .split(',')
        .filter(|address| !address.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if peer_addresses.is_empty()
        || peer_addresses
            .iter()
            .any(|address| address.to_socket_addrs().ok().is_none())
        || peer_addresses.iter().collect::<BTreeSet<_>>().len() != peer_addresses.len()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node.peer_nodes must be nonempty, resolvable, and unique",
        ));
    }
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
        cluster_id,
        data_dir,
        listen_client,
        listen_peer,
        listen_metrics,
        peers,
    })
}

pub(crate) fn validate_listener_safety(config: &Config, allow_unsafe: bool) -> io::Result<()> {
    for (name, address) in [
        ("client", &config.listen_client),
        ("peer", &config.listen_peer),
        ("metrics", &config.listen_metrics),
    ] {
        let parsed = address.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {name} listener"),
            )
        })?;
        if let Some(warning) = unsafe_listener_warning(name, parsed)
            && !allow_unsafe
        {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, warning));
        }
    }
    Ok(())
}

pub(crate) fn unsafe_listener_warning(name: &str, address: SocketAddr) -> Option<String> {
    (!address.ip().is_loopback())
        .then(|| format!("ccdb warning listener={name} address={address} unauthenticated=true"))
}

fn selfcheck(args: &[String]) -> io::Result<()> {
    let data_dir = flag(args, "--data-dir").unwrap_or_else(|| String::from("ccdb-data/n1"));
    let path = Path::new(&data_dir);
    if !path.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "data directory does not exist",
        ));
    }
    let config_path = path.join("ccdb.toml");
    let config = if config_path.exists() {
        let config = read_config(&config_path)?;
        validate_identity(&config)?;
        if fs::canonicalize(&config.data_dir)? != fs::canonicalize(path)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "config data_dir does not match checked directory",
            ));
        }
        Some(config)
    } else {
        DiskIdentity::decode(&fs::read(identity_path(path))?)?;
        None
    };
    let wal_path = path.join("raft/wal.0");
    let wal_records = if wal_path.exists() {
        let bytes = fs::read(&wal_path)?;
        if bytes.is_empty() {
            0
        } else {
            let recovered =
                cc_log::recover_framed_record_stream(&bytes).map_err(io::Error::other)?;
            if recovered.torn_tail_truncated {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "selfcheck refuses a WAL with an untruncated torn tail",
                ));
            }
            if let Some(config) = &config {
                let membership = cc_core::MembershipState::new(
                    config
                        .peers
                        .iter()
                        .map(|peer| cc_core::NodeId::new(peer.id))
                        .collect(),
                )
                .map_err(io::Error::other)?;
                if recovered.state.genesis.cluster_id != config.cluster_id.bytes()
                    || recovered.state.genesis.policy != ClusterPolicy::default()
                    || recovered.state.genesis.membership != membership
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "selfcheck WAL genesis disagrees with config/identity",
                    ));
                }
            }
            recovered.state.entries.len().saturating_add(1)
        }
    } else {
        0
    };
    let staging = path.join("snapshots/staging");
    if has_flag(args, "--deep")
        && staging.exists()
        && fs::read_dir(&staging)?.next().transpose()?.is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot staging contains an incomplete restore",
        ));
    }
    println!(
        "selfcheck{} data_dir={} wal_records={} snapshot_staging={}",
        if has_flag(args, "--deep") {
            " --deep"
        } else {
            ""
        },
        path.display(),
        wal_records,
        if staging.exists() {
            "present"
        } else {
            "absent"
        },
    );
    Ok(())
}

fn doctor(args: &[String]) -> io::Result<()> {
    let data_dir = flag(args, "--data-dir").unwrap_or_else(|| String::from("."));
    let path = Path::new(&data_dir);
    fs::create_dir_all(path)?;
    fsync_probe(path)?;
    let client = flag(args, "--client-addr").unwrap_or_else(|| String::from("127.0.0.1:7101"));
    let peer = flag(args, "--peer-addr").unwrap_or_else(|| String::from("127.0.0.1:7201"));
    println!(
        "doctor data_dir={} filesystem={} fsync=pass client_port={} peer_port={}",
        path.display(),
        filesystem_kind(path),
        port_probe(&client),
        port_probe(&peer),
    );
    Ok(())
}

fn fsync_probe(path: &Path) -> io::Result<()> {
    let nonce = process_time().as_nanos();
    let source = path.join(format!(".ccdb-doctor-{}-{nonce}.tmp", std::process::id()));
    let renamed = path.join(format!(".ccdb-doctor-{}-{nonce}.ok", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&source)?;
        file.write_all(b"ccdb-fsync-probe-v1")?;
        file.sync_all()?;
        fs::rename(&source, &renamed)?;
        sync_directory(path)?;
        if fs::read(&renamed)? != b"ccdb-fsync-probe-v1" {
            return Err(io::Error::other("fsync probe readback mismatch"));
        }
        fs::remove_file(&renamed)?;
        sync_directory(path)
    })();
    let _ = fs::remove_file(&source);
    let _ = fs::remove_file(&renamed);
    result
}

fn port_probe(address: &str) -> &'static str {
    address
        .parse::<SocketAddr>()
        .ok()
        .and_then(|socket| TcpStream::connect_timeout(&socket, StdDuration::from_millis(100)).ok())
        .map_or("closed", |_| "open")
}

fn filesystem_kind(path: &Path) -> String {
    path.metadata()
        .map(|metadata| {
            if metadata.is_dir() {
                "directory"
            } else {
                "file"
            }
        })
        .unwrap_or("unknown")
        .to_owned()
}

fn admin(args: &[String]) -> io::Result<()> {
    if args.iter().any(|arg| arg == "backup") {
        let data_dir = flag(args, "--data-dir").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "backup requires --data-dir")
        })?;
        let output = flag(args, "--output").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "backup requires --output")
        })?;
        let count = backup_data_dir(Path::new(&data_dir), Path::new(&output))?;
        println!("backup: PASS files={count} output={output}");
        return Ok(());
    }
    if args.iter().any(|arg| arg == "restore") {
        let input = flag(args, "--input").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "restore requires --input")
        })?;
        let data_dir = flag(args, "--data-dir").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "restore requires --data-dir")
        })?;
        let count = restore_backup(Path::new(&input), Path::new(&data_dir))?;
        println!("restore: PASS files={count} data_dir={data_dir}");
        return Ok(());
    }
    let address = flag(args, "--addr").unwrap_or_else(|| String::from("127.0.0.1:7101"));
    let config = flag(args, "--config")
        .map(|path| read_config(Path::new(&path)))
        .transpose()?;
    match args
        .iter()
        .find(|arg| matches!(arg.as_str(), "status" | "members" | "snapshot"))
        .map(String::as_str)
        .unwrap_or("status")
    {
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
        "snapshot" => println!("RAFT.SNAPSHOT addr={address} state=unavailable checkpoint=none"),
        _ => {
            let (resolved, response) = request_info_follow(&address)?;
            println!("RAFT.STATUS requested={address} resolved={resolved} {response}");
        }
    }
    Ok(())
}

/// CCBK is an operator archive of the strict-adapter directory.  It preserves
/// identity/configuration and the single cc-log WAL; snapshots and store files
/// receive their own consistency protocol in the storage/snapshot phases.
fn backup_data_dir(data_dir: &Path, output: &Path) -> io::Result<usize> {
    if !data_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "backup data directory is absent",
        ));
    }
    if output.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "backup output already exists",
        ));
    }
    let mut entries = Vec::new();
    for name in ["identity.ccid", "ccdb.toml", "raft/wal.0"] {
        let path = data_dir.join(name);
        let data = if path.exists() {
            fs::read(&path)?
        } else if name == "raft/wal.0" {
            Vec::new()
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("backup requires {}", path.display()),
            ));
        };
        if data.len() > BACKUP_MAX_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "backup file exceeds limit",
            ));
        }
        if name == "raft/wal.0" && !data.is_empty() {
            let recovered =
                cc_log::recover_framed_record_stream(&data).map_err(io::Error::other)?;
            if recovered.torn_tail_truncated || recovered.bytes_consumed != data.len() as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "backup refuses a torn WAL",
                ));
            }
        }
        entries.push((name.as_bytes().to_vec(), data));
    }
    let mut archive = Vec::new();
    archive.extend_from_slice(BACKUP_MAGIC);
    archive.extend_from_slice(&BACKUP_VERSION.to_le_bytes());
    archive.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (name, data) in &entries {
        archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
        archive.extend_from_slice(name);
        archive.extend_from_slice(&(data.len() as u64).to_le_bytes());
        archive.extend_from_slice(&crc32c(data).to_le_bytes());
        archive.extend_from_slice(data);
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temporary = output.with_extension(format!("tmp-{}", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&archive)?;
        file.sync_all()?;
        fs::rename(&temporary, output)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map(|()| entries.len())
}

fn restore_backup(input: &Path, data_dir: &Path) -> io::Result<usize> {
    if data_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "restore target must not exist",
        ));
    }
    let archive = fs::read(input)?;
    let mut cursor = 0_usize;
    if take_bytes(&archive, &mut cursor, 4)? != BACKUP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup magic",
        ));
    }
    if take_u16(&archive, &mut cursor)? != BACKUP_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported backup version",
        ));
    }
    let count = usize::try_from(take_u32(&archive, &mut cursor)?).unwrap_or(usize::MAX);
    let allowed = ["identity.ccid", "ccdb.toml", "raft/wal.0"];
    if count != allowed.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup file count",
        ));
    }
    let mut entries = BTreeMap::new();
    for _ in 0..count {
        let name_len = usize::from(take_u16(&archive, &mut cursor)?);
        let name = std::str::from_utf8(take_bytes(&archive, &mut cursor, name_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup path is not UTF-8"))?;
        if !allowed.contains(&name) || entries.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid backup path",
            ));
        }
        let length = usize::try_from(take_u64(&archive, &mut cursor)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup length overflow"))?;
        if length > BACKUP_MAX_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "backup file exceeds limit",
            ));
        }
        let expected = take_u32(&archive, &mut cursor)?;
        let data = take_bytes(&archive, &mut cursor, length)?.to_vec();
        if crc32c(&data) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "backup checksum mismatch",
            ));
        }
        entries.insert(name.to_owned(), data);
    }
    if cursor != archive.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing backup bytes",
        ));
    }
    let wal = entries.get("raft/wal.0").expect("validated allowed names");
    if !wal.is_empty() {
        let recovered = cc_log::recover_framed_record_stream(wal).map_err(io::Error::other)?;
        if recovered.torn_tail_truncated || recovered.bytes_consumed != wal.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "backup WAL is torn",
            ));
        }
    }
    let parent = data_dir.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = data_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ccdb");
    let staging = parent.join(format!(".{file_name}.restore-{}", std::process::id()));
    let result = (|| {
        fs::create_dir(&staging)?;
        fs::create_dir_all(staging.join("raft"))?;
        fs::create_dir_all(staging.join("store/sst"))?;
        fs::create_dir_all(staging.join("snapshots/staging"))?;
        for (name, mut data) in entries {
            if name == "ccdb.toml" {
                let text = String::from_utf8(data).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "backup config is not UTF-8")
                })?;
                data = text
                    .lines()
                    .map(|line| {
                        if line.trim_start().starts_with("data_dir =") {
                            format!("data_dir = \"{}\"", data_dir.display())
                        } else {
                            line.to_owned()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
                    .into_bytes();
                data.push(b'\n');
            }
            let path = staging.join(name);
            let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        sync_directory(&staging)?;
        fs::rename(&staging, data_dir)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result.map(|()| count)
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
    match read_resp_value(&mut stream)? {
        RespValue::Bulk(Some(value)) => Ok(String::from_utf8_lossy(&value)
            .replace('\r', "")
            .replace('\n', " ")),
        RespValue::Simple(value) | RespValue::Error(value) => Ok(value),
        other => Ok(format!("{other:?}")),
    }
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
    let mut scratch = [0_u8; 4096];
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

pub(crate) fn to_resp(reply: KvReply) -> RespValue {
    match reply {
        KvReply::Ok => RespValue::Simple(String::from("OK")),
        KvReply::Value(Some(value)) => RespValue::Bulk(Some(value)),
        KvReply::Value(None) => RespValue::Bulk(None),
        KvReply::Integer(value) => RespValue::Integer(value),
        KvReply::Cas(value) | KvReply::Conditional(value) => RespValue::Integer(i64::from(value)),
        KvReply::Scan(values) => RespValue::Array(
            values
                .into_iter()
                .flat_map(|(key, value)| [RespValue::Bulk(Some(key)), RespValue::Bulk(Some(value))])
                .collect(),
        ),
        KvReply::Error(error) => RespValue::Error(format!("ERR {error}")),
    }
}

pub(crate) fn metrics_dashboard() -> String {
    String::from(
        "<!doctype html><title>ccdb / metrics</title><pre id=metrics>loading…</pre><script>fetch('/metrics').then(r=>r.text()).then(t=>document.querySelector('#metrics').textContent=t)</script>",
    )
}

fn take_bytes<'a>(input: &'a [u8], cursor: &mut usize, length: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "backup length overflow"))?;
    let bytes = input
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated backup"))?;
    *cursor = end;
    Ok(bytes)
}

fn take_u16(input: &[u8], cursor: &mut usize) -> io::Result<u16> {
    Ok(u16::from_le_bytes(
        take_bytes(input, cursor, 2)?.try_into().expect("two bytes"),
    ))
}
fn take_u32(input: &[u8], cursor: &mut usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(
        take_bytes(input, cursor, 4)?
            .try_into()
            .expect("four bytes"),
    ))
}
fn take_u64(input: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        take_bytes(input, cursor, 8)?
            .try_into()
            .expect("eight bytes"),
    ))
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

pub(crate) fn fatal_disk(reason: &str) -> ! {
    eprintln!("ccdb fatal disk error: {reason}");
    std::process::abort()
}

pub(crate) fn flag(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|window| window[0] == name)
        .map(|window| window[1].clone())
}
pub(crate) fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

fn print_help() {
    println!(concat!(
        "ccdb ",
        env!("CARGO_PKG_VERSION"),
        "\n\nCommands:\n  init --cluster NAME --cluster-id HEX32 --nodes N [--base-dir DIR]\n  init --cluster NAME --cluster-id HEX32 --node-id ID --data-dir DIR\n  run --config PATH [--record PATH] [--record-max-bytes N] [--record-required] [--run-for-ms N] [--i-know-this-is-unauthenticated]\n  peer --config PATH --addr ADDR [--retries N]\n  admin --addr ADDR status|members|snapshot\n  admin backup --data-dir DIR --output FILE\n  admin restore --input FILE --data-dir DIR\n  selfcheck --data-dir DIR [--deep]\n  doctor [--data-dir DIR] [--client-addr ADDR] [--peer-addr ADDR]"
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CLUSTER_ID: &str = "00112233445566778899aabbccddeeff";
    fn test_cluster_id() -> ClusterId {
        ClusterId::from_hex(TEST_CLUSTER_ID).expect("test cluster id")
    }
    fn test_config(directory: PathBuf) -> Config {
        Config {
            id: 1,
            cluster_id: test_cluster_id(),
            data_dir: directory,
            listen_client: String::from("127.0.0.1:7101"),
            listen_peer: String::from("127.0.0.1:7201"),
            listen_metrics: String::from("127.0.0.1:7301"),
            peers: vec![Peer {
                id: 1,
                address: String::from("127.0.0.1:7201"),
            }],
        }
    }

    #[test]
    fn trap_ccid_rejects_mismatched_node_or_cluster_identity() {
        let directory =
            env::temp_dir().join(format!("cc-node-identity-{}", process_time().as_nanos()));
        fs::create_dir_all(&directory).expect("identity directory");
        let config = test_config(directory.clone());
        write_identity(
            &identity_path(&directory),
            DiskIdentity::fresh(test_cluster_id(), 1),
        )
        .expect("identity");
        assert!(validate_identity(&config).is_ok());
        let mut wrong_node = config.clone();
        wrong_node.id = 2;
        assert_eq!(
            validate_identity(&wrong_node)
                .expect_err("node mismatch")
                .kind(),
            io::ErrorKind::InvalidData
        );
        let mut wrong_cluster = config;
        wrong_cluster.cluster_id =
            ClusterId::from_hex("11112233445566778899aabbccddeeff").expect("cluster id");
        assert_eq!(
            validate_identity(&wrong_cluster)
                .expect_err("cluster mismatch")
                .kind(),
            io::ErrorKind::InvalidData
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn trap_cluster_id_has_one_nonzero_canonical_text_form() {
        assert!(ClusterId::from_hex(TEST_CLUSTER_ID).is_ok());
        assert!(ClusterId::from_hex("00112233445566778899AABBCCDDEEFF").is_err());
        assert!(ClusterId::from_hex("00112233445566778899aabbccddeef").is_err());
        assert!(ClusterId::from_hex("00000000000000000000000000000000").is_err());
    }

    #[test]
    fn trap_ccid_is_exact_checksum_fenced_and_removed_is_terminal() {
        let identity = DiskIdentity::fresh(test_cluster_id(), 7);
        let bytes = identity.encode();
        assert_eq!(bytes.len(), IDENTITY_LEN);
        assert_eq!(
            DiskIdentity::decode(&bytes).expect("decode identity"),
            identity
        );
        let mut corrupt = bytes;
        corrupt[31] ^= 1;
        assert!(DiskIdentity::decode(&corrupt).is_err());

        let directory =
            env::temp_dir().join(format!("cc-node-removed-{}", process_time().as_nanos()));
        fs::create_dir_all(&directory).expect("identity directory");
        write_identity(
            &identity_path(&directory),
            DiskIdentity {
                lifecycle: IDENTITY_REMOVED,
                ..DiskIdentity::fresh(test_cluster_id(), 1)
            },
        )
        .expect("removed identity");
        assert_eq!(
            validate_identity(&test_config(directory.clone()))
                .expect_err("removed is terminal")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[cfg(unix)]
    #[test]
    fn trap_ccid_refuses_a_symlinked_data_directory_before_opening_state() {
        use std::os::unix::fs::symlink;

        let root = env::temp_dir().join(format!("cc-node-symlink-{}", process_time().as_nanos()));
        let target = root.join("target");
        let alias = root.join("alias");
        fs::create_dir_all(&target).expect("target directory");
        write_identity(
            &identity_path(&target),
            DiskIdentity::fresh(test_cluster_id(), 1),
        )
        .expect("identity");
        symlink(&target, &alias).expect("symlink");
        assert_eq!(
            validate_identity(&test_config(alias))
                .expect_err("symlink must be refused")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_non_loopback_bind_requires_the_flag() {
        let mut config = test_config(PathBuf::from("/tmp/cc-node-listener-test"));
        config.listen_peer = String::from("0.0.0.0:7201");
        assert_eq!(
            validate_listener_safety(&config, false)
                .expect_err("unsafe listener must be refused")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(validate_listener_safety(&config, true).is_ok());
    }

    #[test]
    fn trap_unsafe_warning_names_actual_listener() {
        assert_eq!(
            unsafe_listener_warning("peer", "0.0.0.0:7201".parse().expect("address")),
            Some(String::from(
                "ccdb warning listener=peer address=0.0.0.0:7201 unauthenticated=true"
            ))
        );
        assert_eq!(
            unsafe_listener_warning("peer", "127.0.0.1:7201".parse().expect("address")),
            None
        );
    }

    #[test]
    fn trap_pre_upgrade_data_dir_is_refused_before_writes() {
        let directory =
            env::temp_dir().join(format!("cc-node-pre-n1-{}", process_time().as_nanos()));
        fs::create_dir_all(&directory).expect("legacy directory");
        fs::write(directory.join("commands.log"), b"legacy bytes").expect("legacy journal");
        let error = validate_identity(&test_config(directory.clone()))
            .expect_err("legacy directory must be refused");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            fs::read(directory.join("commands.log")).expect("legacy bytes"),
            b"legacy bytes"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn trap_strict_config_rejects_defaults_and_duplicate_identity() {
        assert!(parse_config("[node]\nid = 1\ndata_dir = \"/tmp/node\"\n").is_err());
        let duplicate = "[node]\nid = 1\nid = 2\ncluster_id = \"00112233445566778899aabbccddeeff\"\ndata_dir = \"/tmp/node\"\nlisten_client = \"127.0.0.1:7101\"\nlisten_peer = \"127.0.0.1:7201\"\nlisten_metrics = \"127.0.0.1:7301\"\npeer_nodes = \"127.0.0.1:7201\"\n";
        assert!(parse_config(duplicate).is_err());
    }

    #[test]
    fn trap_backup_round_trip_preserves_new_wal_layout() {
        let root = env::temp_dir().join(format!("cc-node-backup-{}", process_time().as_nanos()));
        let source = root.join("source");
        let restored = root.join("restored");
        initialize_data_dir(&source, test_cluster_id(), 1).expect("initialize");
        fs::write(source.join("ccdb.toml"), format!("[node]\nid = 1\ncluster_id = \"{TEST_CLUSTER_ID}\"\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:7101\"\nlisten_peer = \"127.0.0.1:7201\"\nlisten_metrics = \"127.0.0.1:7301\"\npeer_nodes = \"127.0.0.1:7201\"\n", source.display())).expect("config");
        let archive = root.join("backup.ccbk");
        assert_eq!(backup_data_dir(&source, &archive).expect("backup"), 3);
        assert_eq!(restore_backup(&archive, &restored).expect("restore"), 3);
        selfcheck(&[
            String::from("--data-dir"),
            restored.display().to_string(),
            String::from("--deep"),
        ])
        .expect("restored selfcheck");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_metrics_page_remains_local_and_dependency_free() {
        let dashboard = metrics_dashboard();
        assert!(dashboard.contains("fetch('/metrics')"));
        assert!(!dashboard.contains("https://"));
    }
}
