// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]

//! `ccdb` is deliberately a thin operator/transport adapter.  Raft, durable
//! continuations, and application state all live below `driver_host`.

#[cfg(test)]
mod c0_identity_fixtures;
mod driver_host;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use cc_cluster::backup::{
    BACKUP_MAGIC, BACKUP_V2_FOOTER_BYTES, BACKUP_V2_HEADER_BYTES, BACKUP_VERSION, BackupProvenance,
    BackupV2, LEGACY_BACKUP_VERSION,
};
use cc_cluster::{CcsnSnapshot, ccsn_file_crc, decode_ccsn, encode_ccsn};
use cc_core::{
    ClusterId, ClusterPolicy, HostLimits, LogIndex, MembershipState, NodeId, Term, Time, crc32c,
};
use cc_kv::KvReply;
use cc_resp::{MAX_FRAME, RespValue, encode, parse};
use cc_store::{
    ManifestCheckpoint, ManifestEditV2, ManifestV2, StoreWatermark, decode_manifest_v2,
    encode_manifest_v2, validate_checkpoint_authority,
};

const BACKUP_MAX_FILE: usize = 1024 * 1024 * 1024;
const IDENTITY_MAGIC: &[u8; 4] = b"CCID";
const IDENTITY_VERSION: u16 = 1;
const IDENTITY_LEN: usize = 55;
const IDENTITY_ACTIVE: u8 = 1;
const IDENTITY_JOINING: u8 = 2;
const IDENTITY_REMOVED: u8 = 3;
// This binary understands both the frozen v1 readers and the N3 derived-store
// formats.  A data directory remains at floor 1 until the first v2-capable
// host boot, which atomically raises the CCID before opening store/wal.0.
const MIN_STORAGE_READER: u16 = 2;
/// The current binary can read v2 and v3 traffic. Once it advertises v3 it
/// raises this durable floor before opening peer service, preventing a v2
/// binary from later serving the directory as though v3 had never existed.
const MIN_SEMANTIC_READER: u16 = 3;

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
            .map(|peer| format!("{peer}@127.0.0.1:{}", 7200 + peer))
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

/// Materialize the durable, non-voting discovery state used by `run --join`.
/// The seed response is authorized by a leader ReadIndex, but the joining
/// process is deliberately absent from that membership and therefore cannot
/// vote or serve until a replicated config/snapshot later includes it.
pub(crate) fn prepare_join_config(args: &[String]) -> io::Result<PathBuf> {
    let seed = flag(args, "--join").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "run --join requires a seed address",
        )
    })?;
    let node_id = flag(args, "--node-id")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "join requires --node-id"))?
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "join node id"))?;
    if node_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "join node id must be nonzero",
        ));
    }
    let peer_address = flag(args, "--peer-addr")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "join requires --peer-addr"))?;
    let peer_socket = peer_address.parse::<SocketAddr>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "join peer address must be a numeric IP:port",
        )
    })?;
    if peer_socket.port() == 0
        || peer_socket.ip().is_unspecified()
        || peer_socket.ip().is_multicast()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "join peer address must be a canonical unicast endpoint",
        ));
    }
    let data_dir =
        PathBuf::from(flag(args, "--data-dir").ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "join requires --data-dir")
        })?);
    let config_path = flag(args, "--config")
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("ccdb.toml"));

    if identity_path(&data_dir).exists() {
        let config = read_config(&config_path)?;
        let identity = DiskIdentity::decode(&fs::read(identity_path(&data_dir))?)?;
        if identity.lifecycle != IDENTITY_JOINING
            || identity.node_id != node_id
            || config.id != node_id
            || config.data_dir != data_dir
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "join directory is not a resumable Joining identity",
            ));
        }
        return Ok(config_path);
    }
    reject_symlink(&data_dir, "join data directory")?;
    if data_dir.exists() && fs::read_dir(&data_dir)?.next().transpose()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "join requires an empty data directory",
        ));
    }

    let status = request_membership_follow(&seed)?;
    let cluster_id = parse_cluster_id(status_field(&status, "cluster_id").ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "join response lacks cluster_id")
    })?)?;
    let policy_bytes = decode_hex(status_field(&status, "policy").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "join response lacks cluster policy",
        )
    })?)?;
    let policy = ClusterPolicy::decode(&policy_bytes).map_err(io::Error::other)?;
    validate_join_policy(policy)?;

    let voters = parse_member_ids(status_field(&status, "voters").unwrap_or(""))?;
    let learners = parse_member_ids(status_field(&status, "learners").unwrap_or(""))?;
    if voters.contains(&NodeId::new(node_id)) || learners.contains(&NodeId::new(node_id)) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "join node id already exists in membership",
        ));
    }
    let addresses = parse_member_addresses(status_field(&status, "addresses").unwrap_or(""))?;
    if addresses.values().any(|address| *address == peer_socket) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "join peer address already exists in membership",
        ));
    }
    let mut current_membership = MembershipState::new(voters).map_err(io::Error::other)?;
    current_membership.learners = learners;
    for (id, socket) in &addresses {
        let address = match socket {
            SocketAddr::V4(value) => cc_core::PeerAddress::V4 {
                ip: value.ip().octets(),
                port: value.port(),
            },
            SocketAddr::V6(value) => cc_core::PeerAddress::V6 {
                ip: value.ip().octets(),
                port: value.port(),
            },
        };
        current_membership.addresses.insert(*id, address);
    }
    current_membership.active_features = status_field(&status, "active_features")
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .unwrap_or(0);
    current_membership.validate().map_err(io::Error::other)?;

    // Join Genesis is a replay base, not a copy of today's membership.  A
    // current feature bit or membership transition placed in Genesis would
    // be applied a second time when the leader sends its retained log prefix.
    let genesis_voters =
        parse_member_ids(status_field(&status, "genesis_voters").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "join response lacks Genesis voters",
            )
        })?)?;
    let genesis_learners =
        parse_member_ids(status_field(&status, "genesis_learners").unwrap_or(""))?;
    let genesis_addresses =
        parse_member_addresses(status_field(&status, "genesis_addresses").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "join response lacks Genesis addresses",
            )
        })?)?;
    let mut genesis_membership = MembershipState::new(genesis_voters).map_err(io::Error::other)?;
    genesis_membership.learners = genesis_learners;
    for (id, socket) in genesis_addresses {
        let address = match socket {
            SocketAddr::V4(value) => cc_core::PeerAddress::V4 {
                ip: value.ip().octets(),
                port: value.port(),
            },
            SocketAddr::V6(value) => cc_core::PeerAddress::V6 {
                ip: value.ip().octets(),
                port: value.port(),
            },
        };
        genesis_membership.addresses.insert(id, address);
    }
    genesis_membership.active_features = status_field(&status, "genesis_active_features")
        .and_then(|value| value.strip_prefix("0x"))
        .and_then(|value| u64::from_str_radix(value, 16).ok())
        .unwrap_or(0);
    if genesis_membership.voters.contains(&NodeId::new(node_id))
        || genesis_membership.learners.contains(&NodeId::new(node_id))
    {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "join node id already exists in Genesis membership",
        ));
    }
    genesis_membership.validate().map_err(io::Error::other)?;

    fs::create_dir_all(data_dir.join("raft"))?;
    fs::create_dir_all(data_dir.join("store/sst"))?;
    fs::create_dir_all(data_dir.join("snapshots/staging"))?;
    let mut identity = DiskIdentity::fresh(cluster_id, node_id);
    identity.lifecycle = IDENTITY_JOINING;
    write_identity(&identity_path(&data_dir), identity)?;
    let genesis = cc_log::Genesis {
        origin: cc_log::Origin::Join,
        cluster_id: cluster_id.bytes(),
        policy,
        membership: genesis_membership,
    };
    write_synced_file(
        &data_dir.join("raft/wal.0"),
        &cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(genesis)))
            .map_err(io::Error::other)?,
    )?;
    let local_client = flag(args, "--client-addr").unwrap_or_else(|| {
        SocketAddr::new(peer_socket.ip(), peer_socket.port().saturating_sub(100)).to_string()
    });
    let local_metrics = flag(args, "--metrics-addr").unwrap_or_else(|| {
        SocketAddr::new(peer_socket.ip(), peer_socket.port().saturating_add(100)).to_string()
    });
    let peers = addresses
        .iter()
        .map(|(id, address)| format!("{}@{address}", id.get()))
        .collect::<Vec<_>>()
        .join(",");
    let config = format!(
        "[node]\nid = {node_id}\ncluster_id = \"{cluster_id}\"\ndata_dir = \"{}\"\nlisten_client = \"{local_client}\"\nlisten_peer = \"{peer_socket}\"\nlisten_metrics = \"{local_metrics}\"\npeer_nodes = \"{peers}\"\n\n[storage]\nfsync = \"always\"\n",
        data_dir.display(),
    );
    write_synced_file(&config_path, config.as_bytes())?;
    sync_directory(&data_dir)?;
    Ok(config_path)
}

fn validate_join_policy(policy: ClusterPolicy) -> io::Result<()> {
    if policy != ClusterPolicy::default() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "this build/HostLimits cannot honor the discovered cluster policy",
        ));
    }
    Ok(())
}

fn request_membership_follow(seed: &str) -> io::Result<String> {
    let mut current = seed.to_owned();
    let mut seen = BTreeSet::new();
    for _ in 0..4 {
        if !seen.insert(current.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "join leader redirect loop",
            ));
        }
        match request_admin_command(&current, &["CC.ADMIN", "MEMBERS", "CONSISTENT"]) {
            Ok(status) => return Ok(status),
            Err(error) => {
                let text = error.to_string();
                let Some(next) = text
                    .split_whitespace()
                    .find_map(|part| part.strip_prefix("addr="))
                    .filter(|value| *value != "unknown")
                else {
                    return Err(error);
                };
                current = next.to_owned();
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "join leader redirect exceeded hop limit",
    ))
}

fn status_field<'a>(status: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    status
        .split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
}

fn parse_member_ids(value: &str) -> io::Result<BTreeSet<NodeId>> {
    value
        .split(',')
        .filter(|value| !value.is_empty())
        .map(|value| {
            let id = value
                .strip_prefix('n')
                .unwrap_or(value)
                .parse::<u64>()
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid membership node id")
                })?;
            if id == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "zero membership node id",
                ));
            }
            Ok(NodeId::new(id))
        })
        .collect()
}

fn parse_member_addresses(value: &str) -> io::Result<BTreeMap<NodeId, SocketAddr>> {
    value
        .split(',')
        .filter(|value| !value.is_empty())
        .map(|value| {
            let (id, address) = value.split_once('@').ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid membership address")
            })?;
            let id = id.parse::<u64>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid membership address id")
            })?;
            let address = address.parse::<SocketAddr>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "non-numeric membership address")
            })?;
            Ok((NodeId::new(id), address))
        })
        .collect()
}

fn decode_hex(value: &str) -> io::Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "odd hex length"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let nibble = |byte| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid hex")),
            };
            Ok((nibble(pair[0])? << 4) | nibble(pair[1])?)
        })
        .collect()
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

fn replace_identity(path: &Path, identity: DiskIdentity) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "CCID identity path has no parent",
        )
    })?;
    reject_symlink(parent, "CCID parent directory")?;
    reject_symlink(path, "CCID identity")?;
    let temporary = path.with_extension("ccid.next");
    if temporary.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "CCID replacement temporary file already exists",
        ));
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&identity.encode())?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_directory(parent)
}

pub(crate) fn identity_lifecycle(config: &Config) -> io::Result<u8> {
    Ok(DiskIdentity::decode(&fs::read(identity_path(&config.data_dir))?)?.lifecycle)
}

pub(crate) fn mark_identity_active(config: &Config) -> io::Result<()> {
    mark_identity_lifecycle(config, IDENTITY_ACTIVE)
}

pub(crate) fn mark_identity_removed(config: &Config) -> io::Result<()> {
    mark_identity_lifecycle(config, IDENTITY_REMOVED)
}

fn mark_identity_lifecycle(config: &Config, lifecycle: u8) -> io::Result<()> {
    let marker = identity_path(&config.data_dir);
    let mut identity = DiskIdentity::decode(&fs::read(&marker)?)?;
    if identity.lifecycle == IDENTITY_REMOVED && lifecycle != IDENTITY_REMOVED {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "CCID lifecycle Removed is terminal",
        ));
    }
    if identity.lifecycle == lifecycle {
        return Ok(());
    }
    identity.lifecycle = lifecycle;
    identity.migration_epoch = identity
        .migration_epoch
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CCID epoch overflow"))?;
    replace_identity(&marker, identity)
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
    validate_identity_for_readers(config, MIN_STORAGE_READER, MIN_SEMANTIC_READER)
}

fn validate_identity_for_readers(
    config: &Config,
    max_storage_reader: u16,
    max_semantic_reader: u16,
) -> io::Result<()> {
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
    if identity.min_storage_reader > max_storage_reader
        || identity.min_semantic_reader > max_semantic_reader
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CCID requires an unsupported storage or semantic reader",
        ));
    }
    Ok(())
}

/// Durably raise (never lower) the local semantic reader floor. This is done
/// before CCHL can advertise v3 or its feature bits, so a crash cannot leave a
/// connection-generation capability claim without the downgrade fence.
pub(crate) fn raise_identity_semantic_reader(config: &Config, minimum: u16) -> io::Result<()> {
    raise_identity_reader_floor(config, None, Some(minimum))
}

/// Durably raise the storage reader floor before any v2 store file is opened
/// or created. The shared replacement path also proves both reader minima are
/// monotonic and advances one migration epoch for every actual transition.
pub(crate) fn raise_identity_storage_reader(config: &Config, minimum: u16) -> io::Result<()> {
    raise_identity_reader_floor(config, Some(minimum), None)
}

fn raise_identity_reader_floor(
    config: &Config,
    storage: Option<u16>,
    semantic: Option<u16>,
) -> io::Result<()> {
    if storage.is_some_and(|minimum| minimum == 0 || minimum > MIN_STORAGE_READER) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported storage reader floor",
        ));
    }
    let minimum = semantic.unwrap_or(1);
    if minimum == 0 || minimum > MIN_SEMANTIC_READER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported semantic reader floor",
        ));
    }
    let marker = identity_path(&config.data_dir);
    reject_symlink(&marker, "CCID identity")?;
    let mut identity = DiskIdentity::decode(&fs::read(&marker)?)?;
    let next_storage = storage.map_or(identity.min_storage_reader, |value| {
        identity.min_storage_reader.max(value)
    });
    let next_semantic = semantic.map_or(identity.min_semantic_reader, |value| {
        identity.min_semantic_reader.max(value)
    });
    if identity.min_storage_reader == next_storage && identity.min_semantic_reader == next_semantic
    {
        return Ok(());
    }
    identity.min_storage_reader = next_storage;
    identity.min_semantic_reader = next_semantic;
    identity.migration_epoch = identity
        .migration_epoch
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CCID epoch overflow"))?;
    replace_identity(&marker, identity)
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
    let peer_entries = required("peer_nodes")?
        .split(',')
        .filter(|address| !address.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if peer_entries.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node.peer_nodes must be nonempty",
        ));
    }
    let peer_count = peer_entries.len();
    let peers = peer_entries
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            let (peer_id, address) = match entry.split_once('@') {
                Some((raw_id, address)) => (
                    raw_id.parse::<u64>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "node.peer_nodes member id must be a nonzero u64",
                        )
                    })?,
                    address.to_owned(),
                ),
                None => (
                    if peer_count == 1 {
                        id
                    } else {
                        u64::try_from(index + 1).unwrap_or(u64::MAX)
                    },
                    entry,
                ),
            };
            if peer_id == 0 || address.to_socket_addrs().ok().is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "node.peer_nodes entries must use nonzero-id@resolvable-address",
                ));
            }
            Ok(Peer {
                id: peer_id,
                address,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    if peers
        .iter()
        .map(|peer| peer.id)
        .collect::<BTreeSet<_>>()
        .len()
        != peers.len()
        || peers
            .iter()
            .map(|peer| &peer.address)
            .collect::<BTreeSet<_>>()
            .len()
            != peers.len()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "node.peer_nodes member ids and addresses must be unique",
        ));
    }
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
        let parsed = address.to_socket_addrs()?.collect::<Vec<_>>();
        if parsed.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid {name} listener"),
            ));
        }
        validate_resolved_listener(name, parsed, allow_unsafe)?;
    }
    Ok(())
}

fn validate_resolved_listener(
    name: &str,
    addresses: impl IntoIterator<Item = SocketAddr>,
    allow_unsafe: bool,
) -> io::Result<()> {
    for parsed in addresses {
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
    let (config, expected_cluster_id) = if config_path.exists() {
        let config = read_config(&config_path)?;
        validate_identity(&config)?;
        if fs::canonicalize(&config.data_dir)? != fs::canonicalize(path)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "config data_dir does not match checked directory",
            ));
        }
        let cluster_id = config.cluster_id.bytes();
        (Some(config), cluster_id)
    } else {
        let identity = DiskIdentity::decode(&fs::read(identity_path(path))?)?;
        (None, identity.cluster_id.bytes())
    };
    let wal_path = path.join("raft/wal.0");
    let (wal_records, snapshot_mark) = if wal_path.exists() {
        let bytes = fs::read(&wal_path)?;
        if bytes.is_empty() {
            (0, None)
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
                let mut membership = driver_host::membership(config)?;
                // A Join-origin Genesis discovers the already-committed
                // feature fence; local bootstrap config intentionally has no
                // independent field that could override it.
                if recovered.state.genesis.origin == cc_log::Origin::Join {
                    membership.active_features = recovered.state.genesis.membership.active_features;
                }
                if recovered.state.genesis.cluster_id != config.cluster_id.bytes()
                    || recovered.state.genesis.policy != ClusterPolicy::default()
                    || !driver_host::bootstrap_membership_matches(
                        &recovered.state.genesis.membership,
                        &membership,
                    )
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "selfcheck WAL genesis disagrees with config/identity",
                    ));
                }
            }
            (
                recovered.state.entries.len().saturating_add(1),
                recovered.state.snapshot,
            )
        }
    } else {
        (0, None)
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
    let snapshot_state = match snapshot_mark {
        Some(mark) if has_flag(args, "--deep") => {
            let snapshot = path
                .join("snapshots")
                .join(format!("snapshot.{}.ccsn", mark.generation));
            let metadata = fs::metadata(&snapshot).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("snapshot mark lacks checkpoint: {error}"),
                )
            })?;
            if metadata.len() == 0 || metadata.len() > HostLimits::default().max_snapshot_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "marked checkpoint exceeds host limit",
                ));
            }
            let mut decoder = cc_cluster::CcsnStreamDecoder::new(
                expected_cluster_id,
                HostLimits::default().max_snapshot_bytes,
            );
            let mut file = File::open(&snapshot)?;
            let mut buffer = [0_u8; cc_cluster::SNAPSHOT_CHUNK_BYTES];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                decoder.push(&buffer[..read]).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid marked checkpoint")
                })?;
            }
            let (snapshot, checksum) = decoder.finish().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated marked checkpoint")
            })?;
            if mark.generation != mark.index.get()
                || snapshot.kv.applied_index != mark.index
                || snapshot.kv.applied_term != mark.term
                || checksum != mark.crc32c
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "marked checkpoint disagrees with WAL",
                ));
            }
            let manifest = decode_manifest_v2(&fs::read(
                path.join("store")
                    .join(format!("manifest.{}.ccmf", mark.generation)),
            )?)
            .map_err(io::Error::other)?;
            validate_checkpoint_authority(
                manifest.checkpoint,
                Some(ManifestCheckpoint {
                    index: mark.index,
                    term: mark.term,
                    generation: mark.generation,
                    crc32c: mark.crc32c,
                }),
                Some(checksum),
            )
            .map_err(io::Error::other)?;
            "verified"
        }
        Some(_) => "marked",
        None => "none",
    };
    println!(
        "selfcheck{} data_dir={} wal_records={} snapshot={} snapshot_staging={}",
        if has_flag(args, "--deep") {
            " --deep"
        } else {
            ""
        },
        path.display(),
        wal_records,
        snapshot_state,
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
        let new_cluster_id = flag(args, "--new-cluster-id")
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "restore requires --new-cluster-id",
                )
            })
            .and_then(|value| parse_cluster_id(&value))?;
        let new_node_id = flag(args, "--new-node-id")
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "restore requires --new-node-id",
                )
            })?
            .parse::<u64>()
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "restore new node id must be nonzero",
                )
            })?;
        let count = restore_backup(
            Path::new(&input),
            Path::new(&data_dir),
            new_cluster_id,
            new_node_id,
            has_flag(args, "--accept-legacy-node-backup"),
        )?;
        println!("restore: PASS files={count} data_dir={data_dir}");
        return Ok(());
    }
    let address = flag(args, "--addr").unwrap_or_else(|| String::from("127.0.0.1:7101"));
    if args.iter().any(|arg| arg == "feature")
        && args.iter().any(|arg| arg == "activate")
        && args.iter().any(|arg| arg == "atomic-batch")
    {
        let (operator, sequence) = required_admin_identity(args)?;
        let (resolved, _) = request_info_follow(&address)?;
        let response = request_admin_command(
            &resolved,
            &["CC.ADMIN", "ACTIVATE", "ATOMIC-BATCH", &operator, &sequence],
        )?;
        println!(
            "RAFT.ACTIVATE.ATOMIC-BATCH requested={address} resolved={resolved} result={response}"
        );
        return Ok(());
    }
    if let Some(action) = args.iter().find_map(|arg| match arg.as_str() {
        "add-learner" => Some("ADDLEARNER"),
        "promote" | "promote-learner" => Some("PROMOTE"),
        "remove" => Some("REMOVE"),
        "update-address" => Some("UPDATEADDRESS"),
        "transfer-leader" => Some("TRANSFER"),
        "leave-joint" => Some("LEAVEJOINT"),
        _ => None,
    }) {
        let (operator, sequence) = required_admin_identity(args)?;
        let (resolved, _) = request_info_follow(&address)?;
        let mut command = vec![String::from("CC.ADMIN"), String::from(action)];
        if action != "LEAVEJOINT" {
            let node_id = flag(args, "--node-id").ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "membership operation requires --node-id",
                )
            })?;
            if node_id.parse::<u64>().ok().filter(|id| *id != 0).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--node-id must be a nonzero integer",
                ));
            }
            command.push(node_id);
            if matches!(action, "ADDLEARNER" | "UPDATEADDRESS") {
                let peer_address = flag(args, "--peer-addr").ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "learner/address operation requires --peer-addr",
                    )
                })?;
                peer_address.parse::<SocketAddr>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--peer-addr must be a numeric IP:port",
                    )
                })?;
                command.push(peer_address);
            }
        }
        command.push(operator);
        command.push(sequence);
        let command_refs = command.iter().map(String::as_str).collect::<Vec<_>>();
        let response = request_admin_command(&resolved, &command_refs)?;
        println!("RAFT.{action} requested={address} resolved={resolved} result={response}");
        return Ok(());
    }
    match args
        .iter()
        .find(|arg| matches!(arg.as_str(), "status" | "members" | "list" | "snapshot"))
        .map(String::as_str)
        .unwrap_or("status")
    {
        "members" | "list" => {
            let (resolved, _) = request_info_follow(&address)?;
            let members = if has_flag(args, "--consistent") {
                request_admin_command(&resolved, &["CC.ADMIN", "MEMBERS", "CONSISTENT"])?
            } else {
                request_admin_command(&resolved, &["CC.ADMIN", "MEMBERS"])?
            };
            println!("RAFT.MEMBERS requested={address} resolved={resolved} {members}")
        }
        "snapshot" => println!("RAFT.SNAPSHOT addr={address} state=unavailable checkpoint=none"),
        _ => {
            let (resolved, response) = request_info_follow(&address)?;
            println!("RAFT.STATUS requested={address} resolved={resolved} {response}");
        }
    }
    Ok(())
}

fn required_admin_identity(args: &[String]) -> io::Result<(String, String)> {
    let operator = flag(args, "--operator-id").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "membership mutation requires --operator-id",
        )
    })?;
    let sequence = flag(args, "--sequence").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "membership mutation requires --sequence",
        )
    })?;
    for (name, value) in [("--operator-id", &operator), ("--sequence", &sequence)] {
        if value
            .parse::<u64>()
            .ok()
            .filter(|value| *value != 0)
            .is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a nonzero integer"),
            ));
        }
    }
    Ok((operator, sequence))
}

/// Historical CCBK v1 implementation retained only for fixture inspection.
/// It is deliberately not exposed by the operator command: copying this
/// archive would clone a node and cluster identity.
#[allow(dead_code)]
fn backup_legacy_node_clone(data_dir: &Path, output: &Path) -> io::Result<usize> {
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
    archive.extend_from_slice(&LEGACY_BACKUP_VERSION.to_le_bytes());
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

fn backup_data_dir(data_dir: &Path, output: &Path) -> io::Result<usize> {
    let (identity, mark, snapshot_bytes, snapshot) = marked_checkpoint(data_dir)?;
    let backup = BackupV2 {
        source_cluster_id: identity.cluster_id,
        source_index: mark.index,
        source_term: mark.term,
        source_last_leader_time: snapshot.kv.last_leader_time,
        source_policy_hash: snapshot.cluster_policy.hash(),
        source_min_semantic: identity.min_semantic_reader,
        source_active_features: snapshot.membership.active_features,
        checkpoint: snapshot_bytes,
        provenance: BackupProvenance::LogicalCluster,
    };
    if !backup.counts_as_cluster_complete() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "node-local provenance cannot be exported as a complete cluster backup",
        ));
    }
    write_new_backup(output, &encode_backup_v2(&backup)?)?;
    Ok(1)
}

fn marked_checkpoint(
    data_dir: &Path,
) -> io::Result<(DiskIdentity, cc_log::SnapshotMark, Vec<u8>, CcsnSnapshot)> {
    let config_path = data_dir.join("ccdb.toml");
    let config = read_config(&config_path)?;
    validate_identity(&config)?;
    let identity = DiskIdentity::decode(&fs::read(identity_path(data_dir))?)?;
    let wal_path = data_dir.join("raft/wal.0");
    let wal = fs::read(&wal_path)?;
    let recovered = cc_log::recover_framed_record_stream(&wal).map_err(io::Error::other)?;
    if recovered.torn_tail_truncated || recovered.bytes_consumed != wal.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup refuses a torn WAL",
        ));
    }
    let mark = recovered.state.snapshot.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "backup requires a durably marked logical checkpoint",
        )
    })?;
    if mark.generation != mark.index.get() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot mark generation disagrees with index",
        ));
    }
    let path = data_dir
        .join("snapshots")
        .join(format!("snapshot.{}.ccsn", mark.generation));
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > BACKUP_MAX_FILE as u64
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup checkpoint must be a bounded regular file",
        ));
    }
    let bytes = fs::read(path)?;
    let snapshot = decode_ccsn(&bytes, identity.cluster_id.bytes(), BACKUP_MAX_FILE as u64)
        .map_err(io::Error::other)?;
    let checkpoint_crc = ccsn_file_crc(&bytes).map_err(io::Error::other)?;
    let manifest = decode_manifest_v2(&fs::read(
        data_dir
            .join("store")
            .join(format!("manifest.{}.ccmf", mark.generation)),
    )?)
    .map_err(io::Error::other)?;
    validate_checkpoint_authority(
        manifest.checkpoint,
        Some(ManifestCheckpoint {
            index: mark.index,
            term: mark.term,
            generation: mark.generation,
            crc32c: mark.crc32c,
        }),
        Some(checkpoint_crc),
    )
    .map_err(io::Error::other)?;
    if checkpoint_crc != mark.crc32c
        || snapshot.kv.applied_index != mark.index
        || snapshot.kv.applied_term != mark.term
        || snapshot.cluster_policy.hash() != identity.policy_hash
        || snapshot.cluster_id != config.cluster_id.bytes()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "marked checkpoint disagrees with CCID/WAL",
        ));
    }
    Ok((identity, mark, bytes, snapshot))
}

fn encode_backup_v2(backup: &BackupV2) -> io::Result<Vec<u8>> {
    cc_cluster::backup::encode_backup_v2(backup)
}

#[allow(dead_code)]
fn encode_backup_v2_previous_local_copy(backup: &BackupV2) -> io::Result<Vec<u8>> {
    if backup.source_cluster_id.is_zero()
        || backup.source_index.get() == 0
        || backup.source_term.get() == 0
        || backup.source_min_semantic == 0
        || backup.source_min_semantic > MIN_SEMANTIC_READER
        || backup.checkpoint.is_empty()
        || backup.checkpoint.len() > BACKUP_MAX_FILE
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup metadata",
        ));
    }
    let checkpoint_len = u64::try_from(backup.checkpoint.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup checkpoint length"))?;
    let checkpoint_crc = crc32c(&backup.checkpoint);
    let mut bytes = Vec::with_capacity(
        BACKUP_V2_HEADER_BYTES
            .saturating_add(backup.checkpoint.len())
            .saturating_add(BACKUP_V2_FOOTER_BYTES),
    );
    bytes.extend_from_slice(BACKUP_MAGIC);
    bytes.extend_from_slice(&BACKUP_VERSION.to_le_bytes());
    bytes.extend_from_slice(&backup.source_cluster_id.bytes());
    bytes.extend_from_slice(&backup.source_index.get().to_le_bytes());
    bytes.extend_from_slice(&backup.source_term.get().to_le_bytes());
    bytes.extend_from_slice(&backup.source_last_leader_time.as_nanos().to_le_bytes());
    bytes.extend_from_slice(&backup.source_policy_hash.to_le_bytes());
    bytes.extend_from_slice(&backup.source_min_semantic.to_le_bytes());
    bytes.extend_from_slice(&backup.source_active_features.to_le_bytes());
    bytes.extend_from_slice(&checkpoint_len.to_le_bytes());
    bytes.extend_from_slice(&checkpoint_crc.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    if bytes.len() != BACKUP_V2_HEADER_BYTES {
        return Err(io::Error::other("backup header layout"));
    }
    let header_crc = crc32c(&bytes);
    bytes[BACKUP_V2_HEADER_BYTES - 4..].copy_from_slice(&header_crc.to_le_bytes());
    bytes.extend_from_slice(&backup.checkpoint);
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(b"CBKE");
    let footer = bytes.len() - BACKUP_V2_FOOTER_BYTES;
    let bundle_crc = crc32c(&bytes);
    bytes[footer..footer + 4].copy_from_slice(&bundle_crc.to_le_bytes());
    Ok(bytes)
}

fn decode_backup_v2(bytes: &[u8]) -> io::Result<BackupV2> {
    cc_cluster::backup::decode_backup_v2(bytes)
}

#[allow(dead_code)]
fn decode_backup_v2_previous_local_copy(bytes: &[u8]) -> io::Result<BackupV2> {
    if bytes.len() < BACKUP_V2_HEADER_BYTES + BACKUP_V2_FOOTER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated backup",
        ));
    }
    if &bytes[..4] != BACKUP_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup magic",
        ));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("backup version"));
    if version == LEGACY_BACKUP_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy node backup requires an explicit legacy importer",
        ));
    }
    if version != BACKUP_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported backup version",
        ));
    }
    let header = &bytes[..BACKUP_V2_HEADER_BYTES];
    let expected_header = u32::from_le_bytes(
        header[BACKUP_V2_HEADER_BYTES - 4..]
            .try_into()
            .expect("backup header CRC"),
    );
    let mut header_copy = header.to_vec();
    header_copy[BACKUP_V2_HEADER_BYTES - 4..].fill(0);
    if crc32c(&header_copy) != expected_header {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup header checksum",
        ));
    }
    let footer = bytes.len() - BACKUP_V2_FOOTER_BYTES;
    if &bytes[footer + 4..] != b"CBKE" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup footer magic",
        ));
    }
    let expected_bundle =
        u32::from_le_bytes(bytes[footer..footer + 4].try_into().expect("bundle CRC"));
    let mut bundle_copy = bytes.to_vec();
    bundle_copy[footer..footer + 4].fill(0);
    if crc32c(&bundle_copy) != expected_bundle {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup bundle checksum",
        ));
    }
    let mut cluster = [0_u8; 16];
    cluster.copy_from_slice(&header[6..22]);
    let source_cluster_id = ClusterId::new(cluster);
    let source_index = LogIndex::new(u64::from_le_bytes(
        header[22..30].try_into().expect("index"),
    ));
    let source_term = Term::new(u64::from_le_bytes(header[30..38].try_into().expect("term")));
    let source_last_leader_time =
        Time::from_nanos(u64::from_le_bytes(header[38..46].try_into().expect("time")));
    let source_policy_hash = u64::from_le_bytes(header[46..54].try_into().expect("policy"));
    let source_min_semantic = u16::from_le_bytes(header[54..56].try_into().expect("semantic"));
    let source_active_features = u64::from_le_bytes(header[56..64].try_into().expect("features"));
    let checkpoint_len = usize::try_from(u64::from_le_bytes(
        header[64..72].try_into().expect("checkpoint length"),
    ))
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "backup checkpoint length"))?;
    if checkpoint_len == 0 || checkpoint_len > BACKUP_MAX_FILE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup checkpoint limit",
        ));
    }
    let end = BACKUP_V2_HEADER_BYTES
        .checked_add(checkpoint_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "backup checkpoint length"))?;
    if end != footer {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup trailing bytes",
        ));
    }
    let checkpoint = bytes[BACKUP_V2_HEADER_BYTES..end].to_vec();
    let expected_checkpoint =
        u32::from_le_bytes(header[72..76].try_into().expect("checkpoint CRC"));
    if crc32c(&checkpoint) != expected_checkpoint
        || source_cluster_id.is_zero()
        || source_index.get() == 0
        || source_term.get() == 0
        || source_min_semantic == 0
        || source_min_semantic > MIN_SEMANTIC_READER
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid backup checkpoint",
        ));
    }
    let snapshot = decode_ccsn(
        &checkpoint,
        source_cluster_id.bytes(),
        BACKUP_MAX_FILE as u64,
    )
    .map_err(io::Error::other)?;
    if snapshot.kv.applied_index != source_index
        || snapshot.kv.applied_term != source_term
        || snapshot.kv.last_leader_time != source_last_leader_time
        || snapshot.cluster_policy.hash() != source_policy_hash
        || snapshot.membership.active_features != source_active_features
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup checkpoint metadata",
        ));
    }
    Ok(BackupV2 {
        source_cluster_id,
        source_index,
        source_term,
        source_last_leader_time,
        source_policy_hash,
        source_min_semantic,
        source_active_features,
        checkpoint,
        provenance: BackupProvenance::LogicalCluster,
    })
}

fn write_new_backup(output: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    reject_symlink(parent, "backup parent")?;
    if fs::symlink_metadata(output).is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "backup output already exists",
        ));
    }
    fs::create_dir_all(parent)?;
    let temporary = output.with_extension(format!("tmp-{}", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, output)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn restore_backup(
    input: &Path,
    data_dir: &Path,
    new_cluster_id: ClusterId,
    new_node_id: u64,
    accept_legacy: bool,
) -> io::Result<usize> {
    if new_cluster_id.is_zero() || new_node_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "restore identity",
        ));
    }
    if fs::symlink_metadata(data_dir).is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "restore target must not exist",
        ));
    }
    let input_metadata = fs::symlink_metadata(input)?;
    if !input_metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "backup input must be a regular file",
        ));
    }
    if input_metadata.len()
        > (BACKUP_MAX_FILE + BACKUP_V2_HEADER_BYTES + BACKUP_V2_FOOTER_BYTES) as u64
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "backup input exceeds limit",
        ));
    }
    let input_bytes = fs::read(input)?;
    let version = input_bytes
        .get(4..6)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated backup"))?;
    let backup = if version == LEGACY_BACKUP_VERSION {
        if !accept_legacy {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "legacy node backup requires --accept-legacy-node-backup",
            ));
        }
        import_legacy_backup(&input_bytes)?
    } else {
        decode_backup_v2(&input_bytes)?
    };
    if new_cluster_id == backup.source_cluster_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "restore must use a fresh cluster id",
        ));
    }
    let mut snapshot = decode_ccsn(
        &backup.checkpoint,
        backup.source_cluster_id.bytes(),
        BACKUP_MAX_FILE as u64,
    )
    .map_err(io::Error::other)?;
    validate_restore_capabilities(&backup, &snapshot)?;
    let restore_time = Time::from_nanos(
        process_time()
            .as_nanos()
            .max(snapshot.kv.last_leader_time.as_nanos()),
    );
    snapshot.kv.entries.retain(|entry| {
        entry
            .deadline
            .is_none_or(|deadline| deadline > restore_time)
    });
    snapshot.sessions = snapshot
        .sessions
        .for_fresh_cluster_restore(snapshot.cluster_policy, restore_time)
        .map_err(io::Error::other)?;
    let mut membership = MembershipState::new([NodeId::new(new_node_id)].into_iter().collect())
        .map_err(io::Error::other)?;
    membership.active_features = snapshot.membership.active_features;
    membership.validate().map_err(io::Error::other)?;
    snapshot.cluster_id = new_cluster_id.bytes();
    snapshot.membership = membership.clone();
    snapshot.kv.applied_index = LogIndex::new(1);
    snapshot.kv.applied_term = Term::new(1);
    snapshot.kv.last_leader_time = restore_time;
    let checkpoint = encode_ccsn(&snapshot).map_err(io::Error::other)?;
    let checkpoint_crc = ccsn_file_crc(&checkpoint).map_err(io::Error::other)?;
    let checkpoint_mark = ManifestCheckpoint {
        index: LogIndex::new(1),
        term: Term::new(1),
        generation: 1,
        crc32c: checkpoint_crc,
    };
    let mut manifest = ManifestV2::empty(1);
    manifest
        .append_edit_batch(vec![
            ManifestEditV2::AppliedWatermark {
                watermark: StoreWatermark {
                    index: LogIndex::new(1),
                    term: Term::new(1),
                    last_leader_time: restore_time,
                },
                store_sequence: snapshot.kv.store_sequence,
            },
            ManifestEditV2::Checkpoint(Some(checkpoint_mark)),
        ])
        .map_err(io::Error::other)?;
    let manifest_bytes = encode_manifest_v2(&manifest).map_err(io::Error::other)?;
    let genesis = cc_log::Genesis {
        origin: cc_log::Origin::Restore,
        cluster_id: new_cluster_id.bytes(),
        policy: snapshot.cluster_policy,
        membership,
    };
    let mut wal =
        cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(Box::new(genesis)))
            .map_err(io::Error::other)?;
    wal.extend_from_slice(
        &cc_log::encode_framed_durable_record(&cc_log::DurableRecord::InstalledSnapshotMark(
            cc_log::SnapshotMark {
                index: LogIndex::new(1),
                term: Term::new(1),
                generation: 1,
                crc32c: checkpoint_crc,
            },
        ))
        .map_err(io::Error::other)?,
    );
    let parent = data_dir.parent().unwrap_or_else(|| Path::new("."));
    reject_symlink(parent, "restore parent")?;
    fs::create_dir_all(parent)?;
    let name = data_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("ccdb");
    let staging = parent.join(format!(".{name}.restore-{}", std::process::id()));
    let result = (|| {
        fs::create_dir(&staging)?;
        fs::create_dir_all(staging.join("raft"))?;
        fs::create_dir_all(staging.join("store/sst"))?;
        fs::create_dir_all(staging.join("snapshots/staging"))?;
        let mut restored_identity = DiskIdentity::fresh(new_cluster_id, new_node_id);
        restored_identity.policy_hash = snapshot.cluster_policy.hash();
        restored_identity.min_storage_reader = cc_store::STORAGE_V2_MIN_READER;
        restored_identity.min_semantic_reader = backup.source_min_semantic;
        restored_identity.migration_epoch = 1;
        write_identity(&identity_path(&staging), restored_identity)?;
        write_synced_file(
            &staging.join("ccdb.toml"),
            fresh_restore_config(data_dir, new_cluster_id, new_node_id)?.as_bytes(),
        )?;
        write_synced_file(&staging.join("raft/wal.0"), &wal)?;
        write_synced_file(&staging.join("store/manifest.1.ccmf"), &manifest_bytes)?;
        write_synced_file(&staging.join("snapshots/snapshot.1.ccsn"), &checkpoint)?;
        sync_directory(&staging.join("raft"))?;
        sync_directory(&staging.join("store"))?;
        sync_directory(&staging.join("snapshots"))?;
        sync_directory(&staging)?;
        fs::rename(&staging, data_dir)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result.map(|()| 1)
}

fn validate_restore_capabilities(backup: &BackupV2, snapshot: &CcsnSnapshot) -> io::Result<()> {
    let policy = snapshot.cluster_policy;
    let host = HostLimits::default();
    let unsupported_feature = backup.source_active_features & !cc_core::ATOMIC_BATCH_FEATURE != 0;
    let semantic_too_old = backup.source_active_features != 0
        && backup.source_min_semantic < cc_cluster::SEMANTIC_VERSION_V3;
    let allocation_too_large = [
        policy.max_key_bytes,
        policy.max_value_bytes,
        policy.max_command_bytes,
        policy.max_reply_bytes,
        policy.max_batch_bytes,
        policy.max_batch_reply_bytes,
    ]
    .into_iter()
    .any(|bytes| bytes > cc_core::MAX_CODEC_BYTES as u64);
    if unsupported_feature
        || semantic_too_old
        || backup.source_policy_hash != policy.hash()
        || backup.source_active_features != snapshot.membership.active_features
        || policy.max_live_logical_bytes > host.max_snapshot_bytes
        || allocation_too_large
        || policy.validate().is_err()
        || host.validate().is_err()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "restore host cannot honor source policy/features",
        ));
    }
    Ok(())
}

/// Convert the explicitly limited v1 node archive into empty logical
/// provenance. The archive contains no cluster-consistent checkpoint or
/// store/session WAL, so only a genesis-only source can be imported without
/// fabricating acknowledged state. Identity/config bytes are validated and
/// then discarded; the common fresh-cluster restore path emits new authority.
fn import_legacy_backup(bytes: &[u8]) -> io::Result<BackupV2> {
    let mut cursor = 0_usize;
    if take_bytes(bytes, &mut cursor, 4)? != BACKUP_MAGIC
        || take_u16(bytes, &mut cursor)? != LEGACY_BACKUP_VERSION
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid legacy backup",
        ));
    }
    let count = usize::try_from(take_u32(bytes, &mut cursor)?).unwrap_or(usize::MAX);
    if count != 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy backup file count",
        ));
    }
    let allowed = ["identity.ccid", "ccdb.toml", "raft/wal.0"];
    let mut entries = BTreeMap::new();
    for _ in 0..count {
        let name_len = usize::from(take_u16(bytes, &mut cursor)?);
        let name = std::str::from_utf8(take_bytes(bytes, &mut cursor, name_len)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "legacy backup path"))?;
        let length = usize::try_from(take_u64(bytes, &mut cursor)?)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "legacy backup length"))?;
        if !allowed.contains(&name) || length > BACKUP_MAX_FILE || entries.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy backup entry",
            ));
        }
        let expected = take_u32(bytes, &mut cursor)?;
        let body = take_bytes(bytes, &mut cursor, length)?.to_vec();
        if crc32c(&body) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy backup entry checksum",
            ));
        }
        entries.insert(name.to_owned(), body);
    }
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy backup trailing bytes",
        ));
    }
    let identity = DiskIdentity::decode(
        entries
            .get("identity.ccid")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "legacy identity"))?,
    )?;
    let config_text = std::str::from_utf8(
        entries
            .get("ccdb.toml")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "legacy config"))?,
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "legacy config UTF-8"))?;
    let config = parse_config(config_text)?;
    if config.cluster_id != identity.cluster_id || config.id != identity.node_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy config identity mismatch",
        ));
    }
    let wal = entries
        .get("raft/wal.0")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "legacy WAL"))?;
    if wal.is_empty() {
        let membership =
            MembershipState::new([NodeId::new(identity.node_id)].into_iter().collect())
                .map_err(io::Error::other)?;
        let snapshot = CcsnSnapshot {
            cluster_id: identity.cluster_id.bytes(),
            cluster_policy: ClusterPolicy::default(),
            membership,
            kv: cc_kv::LogicalKvSnapshot {
                entries: Vec::new(),
                store_sequence: 0,
                applied_index: LogIndex::new(1),
                applied_term: Term::new(1),
                last_leader_time: Time::from_nanos(0),
            },
            sessions: cc_cluster::SessionTable::default(),
            leadership_transfer: None,
        };
        let checkpoint = encode_ccsn(&snapshot).map_err(io::Error::other)?;
        return Ok(BackupV2 {
            source_cluster_id: identity.cluster_id,
            source_index: LogIndex::new(1),
            source_term: Term::new(1),
            source_last_leader_time: Time::from_nanos(0),
            source_policy_hash: snapshot.cluster_policy.hash(),
            source_min_semantic: identity.min_semantic_reader,
            source_active_features: 0,
            checkpoint,
            provenance: BackupProvenance::LegacyNode,
        });
    }
    let recovered = cc_log::recover_framed_record_stream(wal).map_err(io::Error::other)?;
    if recovered.torn_tail_truncated
        || recovered.bytes_consumed != wal.len() as u64
        || !recovered.state.entries.is_empty()
        || recovered.state.snapshot.is_some()
        || recovered.state.genesis.cluster_id != identity.cluster_id.bytes()
        || recovered.state.genesis.policy.hash() != identity.policy_hash
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy backup lacks a complete logical checkpoint",
        ));
    }
    let snapshot = CcsnSnapshot {
        cluster_id: identity.cluster_id.bytes(),
        cluster_policy: recovered.state.genesis.policy,
        membership: recovered.state.genesis.membership,
        kv: cc_kv::LogicalKvSnapshot {
            entries: Vec::new(),
            store_sequence: 0,
            applied_index: LogIndex::new(1),
            applied_term: Term::new(1),
            last_leader_time: Time::from_nanos(0),
        },
        sessions: cc_cluster::SessionTable::default(),
        leadership_transfer: None,
    };
    let checkpoint = encode_ccsn(&snapshot).map_err(io::Error::other)?;
    Ok(BackupV2 {
        source_cluster_id: identity.cluster_id,
        source_index: LogIndex::new(1),
        source_term: Term::new(1),
        source_last_leader_time: Time::from_nanos(0),
        source_policy_hash: snapshot.cluster_policy.hash(),
        source_min_semantic: identity.min_semantic_reader,
        source_active_features: 0,
        checkpoint,
        provenance: BackupProvenance::LegacyNode,
    })
}

fn fresh_restore_config(
    data_dir: &Path,
    cluster_id: ClusterId,
    node_id: u64,
) -> io::Result<String> {
    let client = 7100_u64
        .checked_add(node_id)
        .filter(|port| *port <= u64::from(u16::MAX))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "restore node port"))?;
    let peer = 7200_u64
        .checked_add(node_id)
        .filter(|port| *port <= u64::from(u16::MAX))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "restore node port"))?;
    let metrics = 7300_u64
        .checked_add(node_id)
        .filter(|port| *port <= u64::from(u16::MAX))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "restore node port"))?;
    Ok(format!(
        "[node]\nid = {node_id}\ncluster_id = \"{cluster_id}\"\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:{client}\"\nlisten_peer = \"127.0.0.1:{peer}\"\nlisten_metrics = \"127.0.0.1:{metrics}\"\npeer_nodes = \"127.0.0.1:{peer}\"\n\n[storage]\nfsync = \"always\"\n",
        data_dir.display()
    ))
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
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

fn request_admin_command(address: &str, command: &[&str]) -> io::Result<String> {
    let socket = address
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid client address"))?;
    let mut stream = TcpStream::connect_timeout(&socket, StdDuration::from_secs(2))?;
    stream.set_read_timeout(Some(StdDuration::from_secs(2)))?;
    stream.write_all(&encode(&RespValue::Array(
        command
            .iter()
            .map(|part| RespValue::Bulk(Some(part.as_bytes().to_vec())))
            .collect(),
    )))?;
    match read_resp_value(&mut stream)? {
        RespValue::Bulk(Some(value)) => Ok(String::from_utf8_lossy(&value).into_owned()),
        RespValue::Simple(value) => Ok(value),
        RespValue::Error(value) => Err(io::Error::other(value)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected admin response {other:?}"),
        )),
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
        KvReply::Batch(replies) => RespValue::Array(replies.into_iter().map(to_resp).collect()),
        KvReply::BatchError {
            failed_index,
            error,
        } => RespValue::Error(format!("ERR batch failed at index {failed_index}: {error}")),
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
        KvReply::Error(cc_kv::KvError::Busy) => RespValue::Error(String::from("BUSY")),
        KvReply::Error(error) => RespValue::Error(format!("ERR {error}")),
    }
}

pub(crate) fn metrics_dashboard() -> String {
    String::from(
        "<!doctype html><title>ccdb / metrics</title><pre id=metrics>loading…</pre><script>fetch('/metrics').then(r=>r.text()).then(t=>document.querySelector('#metrics').textContent=t)</script>",
    )
}

#[allow(dead_code)]
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

#[allow(dead_code)]
fn take_u16(input: &[u8], cursor: &mut usize) -> io::Result<u16> {
    Ok(u16::from_le_bytes(
        take_bytes(input, cursor, 2)?.try_into().expect("two bytes"),
    ))
}
#[allow(dead_code)]
fn take_u32(input: &[u8], cursor: &mut usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(
        take_bytes(input, cursor, 4)?
            .try_into()
            .expect("four bytes"),
    ))
}
#[allow(dead_code)]
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
        "\n\nCommands:\n  init --cluster NAME --cluster-id HEX32 --nodes N [--base-dir DIR]\n  init --cluster NAME --cluster-id HEX32 --node-id ID --data-dir DIR\n  run --config PATH [--record PATH] [--record-max-bytes N] [--record-required] [--run-for-ms N] [--i-know-this-is-unauthenticated]\n  peer --config PATH --addr ADDR [--retries N]\n  admin --addr ADDR status|members|snapshot\n  admin --addr ADDR add-learner|promote-learner --node-id ID\n  admin --addr ADDR leave-joint\n  admin --addr ADDR feature activate atomic-batch\n  admin backup --data-dir DIR --output FILE\n  admin restore --input FILE --data-dir DIR\n  selfcheck --data-dir DIR [--deep]\n  doctor [--data-dir DIR] [--client-addr ADDR] [--peer-addr ADDR]"
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

    fn sample_logical_backup() -> BackupV2 {
        let membership =
            MembershipState::new([NodeId::new(1)].into_iter().collect()).expect("membership");
        let snapshot = CcsnSnapshot {
            cluster_id: test_cluster_id().bytes(),
            cluster_policy: ClusterPolicy::default(),
            membership,
            kv: cc_kv::LogicalKvSnapshot {
                entries: vec![cc_kv::LogicalKvEntry {
                    key: b"sample".to_vec(),
                    sequence: 1,
                    value: b"value".to_vec(),
                    deadline: Some(Time::from_nanos(u64::MAX)),
                }],
                store_sequence: 1,
                applied_index: LogIndex::new(3),
                applied_term: Term::new(2),
                last_leader_time: Time::from_nanos(9),
            },
            sessions: cc_cluster::SessionTable::default(),
            leadership_transfer: None,
        };
        BackupV2 {
            source_cluster_id: test_cluster_id(),
            source_index: LogIndex::new(3),
            source_term: Term::new(2),
            source_last_leader_time: Time::from_nanos(9),
            source_policy_hash: snapshot.cluster_policy.hash(),
            source_min_semantic: MIN_SEMANTIC_READER,
            source_active_features: 0,
            checkpoint: encode_ccsn(&snapshot).expect("checkpoint"),
            provenance: BackupProvenance::LogicalCluster,
        }
    }

    #[test]
    fn trap_join_rejects_foreign_cluster_data() {
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
    fn trap_join_rejects_unhonorable_cluster_policy_before_identity_publish() {
        let directory =
            env::temp_dir().join(format!("cc-node-join-policy-{}", process_time().as_nanos()));
        let mut foreign = ClusterPolicy::default();
        foreign.max_scan_items = foreign.max_scan_items.saturating_sub(1);
        assert_eq!(
            validate_join_policy(foreign)
                .expect_err("policy drift")
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(
            !identity_path(&directory).exists(),
            "policy refusal must precede CCID publication"
        );
    }

    #[test]
    fn trap_cluster_id_has_one_nonzero_canonical_text_form() {
        assert!(ClusterId::from_hex(TEST_CLUSTER_ID).is_ok());
        assert!(ClusterId::from_hex("00112233445566778899AABBCCDDEEFF").is_err());
        assert!(ClusterId::from_hex("00112233445566778899aabbccddeef").is_err());
        assert!(ClusterId::from_hex("00000000000000000000000000000000").is_err());
    }

    #[test]
    fn trap_removed_membership_forces_terminal_ccid_before_listen() {
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

    #[test]
    fn trap_ccid_refuses_future_reader_before_opening_state() {
        let directory = env::temp_dir().join(format!(
            "cc-node-future-reader-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(directory.join("raft")).expect("state directory");
        let state = directory.join("raft/wal.0");
        fs::write(&state, b"must-not-be-opened").expect("state sentinel");
        write_identity(
            &identity_path(&directory),
            DiskIdentity {
                min_storage_reader: MIN_STORAGE_READER.saturating_add(1),
                migration_epoch: 1,
                ..DiskIdentity::fresh(test_cluster_id(), 1)
            },
        )
        .expect("future-reader identity");

        let error = validate_identity(&test_config(directory.clone()))
            .expect_err("future storage reader must be refused");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            fs::read(&state).expect("state sentinel after refusal"),
            b"must-not-be-opened"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn trap_compatibility_matrix_is_enforced() {
        check_compatibility_matrix();
    }

    #[test]
    fn trap_every_supported_persisted_golden_decodes() {
        check_compatibility_matrix();
    }

    fn check_compatibility_matrix() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let status = std::process::Command::new("bash")
            .arg(root.join("scripts/ci/golden-manifest.sh"))
            .arg("--check")
            .current_dir(&root)
            .status()
            .expect("run compatibility matrix gate");
        assert!(
            status.success(),
            "compatibility matrix gate failed: {status}"
        );
    }

    #[test]
    fn doc_coherence() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let status = std::process::Command::new("node")
            .arg(root.join("scripts/ci/doc-coherence.mjs"))
            .current_dir(&root)
            .status()
            .expect("run documentation coherence gate");
        assert!(
            status.success(),
            "documentation coherence gate failed: {status}"
        );
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
    fn trap_hostname_with_any_nonloopback_answer_is_unsafe() {
        let resolved = [
            "127.0.0.1:7101".parse().expect("loopback"),
            "192.0.2.1:7101".parse().expect("non-loopback"),
        ];
        assert_eq!(
            validate_resolved_listener("client", resolved, false)
                .expect_err("one unsafe answer makes the hostname unsafe")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(validate_resolved_listener("client", resolved, true).is_ok());
    }

    #[test]
    fn trap_metrics_listener_obeys_bind_safety() {
        let mut config = test_config(PathBuf::from("/tmp/cc-node-metrics-safety"));
        config.listen_metrics = String::from("[::]:7301");
        assert_eq!(
            validate_listener_safety(&config, false)
                .expect_err("unsafe metrics listener")
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(validate_listener_safety(&config, true).is_ok());
    }

    #[test]
    fn trap_storage_marker_defines_downgrade_boundary() {
        let directory = env::temp_dir().join(format!(
            "cc-node-storage-marker-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(directory.join("raft")).expect("fixture directory");
        let config = test_config(directory.clone());
        let old_identity = DiskIdentity {
            min_storage_reader: 1,
            min_semantic_reader: 2,
            ..DiskIdentity::fresh(test_cluster_id(), 1)
        };
        write_identity(&identity_path(&directory), old_identity).expect("v1 identity");
        let sentinel = directory.join("raft/wal.0");
        fs::write(&sentinel, b"old-reader-sentinel").expect("sentinel");
        validate_identity_for_readers(&config, 1, 2).expect("old reader before marker");

        raise_identity_storage_reader(&config, 2).expect("publish storage fence");
        let raised =
            DiskIdentity::decode(&fs::read(identity_path(&directory)).expect("raised identity"))
                .expect("decode raised identity");
        assert_eq!(raised.min_storage_reader, 2);
        assert_eq!(raised.migration_epoch, old_identity.migration_epoch + 1);
        assert_eq!(
            validate_identity_for_readers(&config, 1, 2)
                .expect_err("old reader after marker")
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            fs::read(&sentinel).expect("sentinel after refusal"),
            b"old-reader-sentinel"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn trap_old_build_refuses_new_directory_before_mutation() {
        let directory = env::temp_dir().join(format!(
            "cc-node-old-reader-refusal-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&directory).expect("fixture directory");
        let config = test_config(directory.clone());
        write_identity(
            &identity_path(&directory),
            DiskIdentity {
                min_storage_reader: 2,
                min_semantic_reader: 3,
                migration_epoch: 7,
                ..DiskIdentity::fresh(test_cluster_id(), 1)
            },
        )
        .expect("new identity");
        let before = fs::read(identity_path(&directory)).expect("identity before");
        assert!(validate_identity_for_readers(&config, 1, 2).is_err());
        assert_eq!(
            fs::read(identity_path(&directory)).expect("identity after"),
            before
        );
        assert_eq!(fs::read_dir(&directory).expect("directory").count(), 1);
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[test]
    fn trap_rolling_upgrade_rejects_cluster_policy_drift() {
        let directory = env::temp_dir().join(format!(
            "cc-node-policy-drift-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&directory).expect("fixture directory");
        write_identity(
            &identity_path(&directory),
            DiskIdentity {
                policy_hash: ClusterPolicy {
                    max_sessions: ClusterPolicy::default().max_sessions + 1,
                    ..ClusterPolicy::default()
                }
                .hash(),
                ..DiskIdentity::fresh(test_cluster_id(), 1)
            },
        )
        .expect("drifted identity");
        assert_eq!(
            validate_identity(&test_config(directory.clone()))
                .expect_err("policy drift")
                .kind(),
            io::ErrorKind::InvalidData
        );
        fs::remove_dir_all(directory).expect("remove fixture");
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
    fn trap_restore_creates_fresh_cluster_identity() {
        let root = env::temp_dir().join(format!("cc-node-backup-{}", process_time().as_nanos()));
        let source = root.join("source");
        let restored = root.join("restored");
        initialize_data_dir(&source, test_cluster_id(), 1).expect("initialize");
        fs::write(source.join("ccdb.toml"), format!("[node]\nid = 1\ncluster_id = \"{TEST_CLUSTER_ID}\"\ndata_dir = \"{}\"\nlisten_client = \"127.0.0.1:7101\"\nlisten_peer = \"127.0.0.1:7201\"\nlisten_metrics = \"127.0.0.1:7301\"\npeer_nodes = \"127.0.0.1:7201\"\n", source.display())).expect("config");
        let membership =
            MembershipState::new([NodeId::new(1)].into_iter().collect()).expect("membership");
        let checkpoint = encode_ccsn(&CcsnSnapshot {
            cluster_id: test_cluster_id().bytes(),
            cluster_policy: ClusterPolicy::default(),
            membership: membership.clone(),
            kv: cc_kv::LogicalKvSnapshot {
                entries: vec![cc_kv::LogicalKvEntry {
                    key: b"backup-key".to_vec(),
                    sequence: 1,
                    value: b"backup-value".to_vec(),
                    deadline: None,
                }],
                store_sequence: 1,
                applied_index: LogIndex::new(3),
                applied_term: Term::new(2),
                last_leader_time: Time::from_nanos(9),
            },
            sessions: cc_cluster::SessionTable::default(),
            leadership_transfer: None,
        })
        .expect("checkpoint");
        let checkpoint_crc = ccsn_file_crc(&checkpoint).expect("checkpoint CRC");
        fs::create_dir_all(source.join("snapshots")).expect("snapshot directory");
        fs::write(source.join("snapshots/snapshot.3.ccsn"), &checkpoint).expect("checkpoint");
        let mut manifest = ManifestV2::empty(3);
        manifest
            .append_edit_batch(vec![
                ManifestEditV2::AppliedWatermark {
                    watermark: StoreWatermark {
                        index: LogIndex::new(3),
                        term: Term::new(2),
                        last_leader_time: Time::from_nanos(9),
                    },
                    store_sequence: 1,
                },
                ManifestEditV2::Checkpoint(Some(ManifestCheckpoint {
                    index: LogIndex::new(3),
                    term: Term::new(2),
                    generation: 3,
                    crc32c: checkpoint_crc,
                })),
            ])
            .expect("checkpoint manifest");
        fs::write(
            source.join("store/manifest.3.ccmf"),
            encode_manifest_v2(&manifest).expect("manifest bytes"),
        )
        .expect("checkpoint manifest");
        let genesis = cc_log::Genesis {
            origin: cc_log::Origin::Bootstrap,
            cluster_id: test_cluster_id().bytes(),
            policy: ClusterPolicy::default(),
            membership,
        };
        let mut wal = cc_log::encode_framed_durable_record(&cc_log::DurableRecord::Genesis(
            Box::new(genesis),
        ))
        .expect("genesis");
        wal.extend_from_slice(
            &cc_log::encode_framed_durable_record(&cc_log::DurableRecord::InstalledSnapshotMark(
                cc_log::SnapshotMark {
                    index: LogIndex::new(3),
                    term: Term::new(2),
                    generation: 3,
                    crc32c: checkpoint_crc,
                },
            ))
            .expect("mark"),
        );
        fs::write(source.join("raft/wal.0"), wal).expect("marked WAL");
        let archive = root.join("backup.ccbk");
        assert_eq!(backup_data_dir(&source, &archive).expect("backup"), 1);
        let fresh_cluster =
            ClusterId::from_hex("11112233445566778899aabbccddeeff").expect("fresh cluster");
        assert_eq!(
            restore_backup(&archive, &restored, fresh_cluster, 2, false).expect("restore"),
            1
        );
        selfcheck(&[
            String::from("--data-dir"),
            restored.display().to_string(),
            String::from("--deep"),
        ])
        .expect("restored selfcheck");
        let restored_identity = DiskIdentity::decode(
            &fs::read(restored.join("identity.ccid")).expect("restored identity"),
        )
        .expect("identity");
        assert_eq!(restored_identity.cluster_id, fresh_cluster);
        assert_eq!(restored_identity.node_id, 2);
        assert_ne!(
            fs::read(source.join("identity.ccid")).expect("source identity"),
            fs::read(restored.join("identity.ccid")).expect("restored identity")
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_backup_is_one_consistent_applied_index() {
        let backup = sample_logical_backup();
        let decoded =
            decode_backup_v2(&encode_backup_v2(&backup).expect("CCBK")).expect("decode CCBK");
        let checkpoint = decode_ccsn(
            &decoded.checkpoint,
            decoded.source_cluster_id.bytes(),
            BACKUP_MAX_FILE as u64,
        )
        .expect("CCSN");
        assert_eq!(decoded.source_index, checkpoint.kv.applied_index);
        assert_eq!(decoded.source_term, checkpoint.kv.applied_term);
        assert_eq!(
            decoded.source_last_leader_time,
            checkpoint.kv.last_leader_time
        );
    }

    #[test]
    fn trap_cluster_policy_hash_matches_genesis_ccid_and_snapshot() {
        let backup = sample_logical_backup();
        let snapshot = decode_ccsn(
            &backup.checkpoint,
            backup.source_cluster_id.bytes(),
            BACKUP_MAX_FILE as u64,
        )
        .expect("snapshot");
        let identity = DiskIdentity::fresh(backup.source_cluster_id, 1);
        let genesis = cc_log::Genesis {
            origin: cc_log::Origin::Bootstrap,
            cluster_id: backup.source_cluster_id.bytes(),
            policy: snapshot.cluster_policy,
            membership: snapshot.membership.clone(),
        };
        assert_eq!(identity.policy_hash, genesis.policy.hash());
        assert_eq!(identity.policy_hash, snapshot.cluster_policy.hash());
        assert_eq!(backup.source_policy_hash, identity.policy_hash);
    }

    #[test]
    fn trap_restore_validates_every_checkpoint_record_crc() {
        let mut backup = sample_logical_backup();
        // CCSN's 74-byte header is followed by a nine-byte record prefix;
        // mutate the first record body while recomputing every outer CCBK CRC.
        let record = 83.min(backup.checkpoint.len() - 1);
        backup.checkpoint[record] ^= 1;
        let bundle = encode_backup_v2(&backup).expect("outer checksums recomputed");
        assert!(decode_backup_v2(&bundle).is_err());
    }

    #[test]
    fn trap_restore_rejects_oversized_or_trailing_bundle() {
        let mut trailing = encode_backup_v2(&sample_logical_backup()).expect("CCBK");
        trailing.push(0);
        assert!(decode_backup_v2(&trailing).is_err());

        let mut oversized = encode_backup_v2(&sample_logical_backup()).expect("CCBK");
        oversized[64..72].copy_from_slice(&u64::MAX.to_le_bytes());
        let header_crc_at = BACKUP_V2_HEADER_BYTES - 4;
        oversized[header_crc_at..BACKUP_V2_HEADER_BYTES].fill(0);
        let checksum = crc32c(&oversized[..BACKUP_V2_HEADER_BYTES]);
        oversized[header_crc_at..BACKUP_V2_HEADER_BYTES].copy_from_slice(&checksum.to_le_bytes());
        assert!(decode_backup_v2(&oversized).is_err());
    }

    #[test]
    fn trap_restore_rejects_source_cluster_id_reuse() {
        let root = env::temp_dir().join(format!(
            "cc-node-backup-reuse-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&root).expect("root");
        let input = root.join("backup.ccbk");
        fs::write(
            &input,
            encode_backup_v2(&sample_logical_backup()).expect("CCBK"),
        )
        .expect("backup file");
        let target = root.join("target");
        let error = restore_backup(&input, &target, test_cluster_id(), 2, false)
            .expect_err("source identity reuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!target.exists());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_restore_rejects_unsupported_active_feature() {
        let mut backup = sample_logical_backup();
        backup.source_active_features = 1_u64 << 63;
        let snapshot = decode_ccsn(
            &backup.checkpoint,
            backup.source_cluster_id.bytes(),
            BACKUP_MAX_FILE as u64,
        )
        .expect("checkpoint");
        assert_eq!(
            validate_restore_capabilities(&backup, &snapshot)
                .expect_err("unknown feature")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn trap_restore_rejects_host_that_cannot_honor_cluster_policy() {
        let mut backup = sample_logical_backup();
        let mut snapshot = decode_ccsn(
            &backup.checkpoint,
            backup.source_cluster_id.bytes(),
            BACKUP_MAX_FILE as u64,
        )
        .expect("checkpoint");
        snapshot.cluster_policy.max_live_logical_bytes =
            HostLimits::default().max_snapshot_bytes.saturating_add(1);
        backup.source_policy_hash = snapshot.cluster_policy.hash();
        assert_eq!(
            validate_restore_capabilities(&backup, &snapshot)
                .expect_err("unhonorable policy")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(unix)]
    #[test]
    fn trap_backup_and_restore_paths_reject_symlinks_and_overwrite() {
        use std::os::unix::fs::symlink;

        let root = env::temp_dir().join(format!(
            "cc-node-backup-paths-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&root).expect("root");
        let existing = root.join("existing.ccbk");
        fs::write(&existing, b"sentinel").expect("existing output");
        assert_eq!(
            write_new_backup(&existing, b"replacement")
                .expect_err("overwrite")
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(&existing).expect("sentinel"), b"sentinel");

        let real = root.join("real.ccbk");
        fs::write(
            &real,
            encode_backup_v2(&sample_logical_backup()).expect("CCBK"),
        )
        .expect("real backup");
        let alias = root.join("alias.ccbk");
        symlink(&real, &alias).expect("input symlink");
        let target = root.join("restored");
        assert_eq!(
            restore_backup(
                &alias,
                &target,
                ClusterId::from_hex("33332233445566778899aabbccddeeff").expect("fresh"),
                3,
                false,
            )
            .expect_err("symlink input")
            .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(!target.exists());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_restore_preserves_ttl_time_floor() {
        let root =
            env::temp_dir().join(format!("cc-node-restore-ttl-{}", process_time().as_nanos()));
        fs::create_dir_all(&root).expect("root");
        let backup = sample_logical_backup();
        let input = root.join("backup.ccbk");
        fs::write(&input, encode_backup_v2(&backup).expect("CCBK")).expect("backup");
        let target = root.join("restored");
        let fresh = ClusterId::from_hex("44442233445566778899aabbccddeeff").expect("fresh");
        restore_backup(&input, &target, fresh, 4, false).expect("restore");
        let bytes = fs::read(target.join("snapshots/snapshot.1.ccsn")).expect("checkpoint");
        let snapshot = decode_ccsn(&bytes, fresh.bytes(), BACKUP_MAX_FILE as u64)
            .expect("restored checkpoint");
        assert!(snapshot.kv.last_leader_time >= backup.source_last_leader_time);
        assert_eq!(snapshot.kv.entries.len(), 1);
        assert_eq!(
            snapshot.kv.entries[0].deadline,
            Some(Time::from_nanos(u64::MAX))
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_restore_discards_source_admin_sessions_and_workflows() {
        let admin = cc_core::SessionKey::new(
            cc_core::SessionNamespace::AdminRequest as u8,
            cc_core::ClientId::new(9),
        )
        .expect("admin session");
        let command = cc_core::ConfigEnvelope {
            admin_session: Some((admin, 1)),
            leader_time: Time::from_nanos(1),
            operation: cc_core::ConfigOperation::RemoveLearner { id: NodeId::new(2) },
        }
        .encode();
        let reply = cc_core::AdminReply {
            operation_tag: 2,
            result: cc_core::AdminResultTag::Applied,
            source_index: LogIndex::new(3),
            detail: Vec::new(),
        }
        .encode();
        let sessions = cc_cluster::SessionTable::from_snapshot_parts(
            BTreeMap::from([(
                admin,
                cc_cluster::SessionRecord {
                    max_seq: 1,
                    canonical_command: command,
                    cached_reply: reply,
                    last_active: Time::from_nanos(1),
                },
            )]),
            BTreeMap::new(),
        )
        .expect("session table");
        let restored = sessions
            .for_fresh_cluster_restore(ClusterPolicy::default(), Time::from_nanos(2))
            .expect("fresh-cluster filter");
        assert!(!restored.contains(admin));
        assert_eq!(restored.record_count(), 0);
    }

    #[test]
    fn trap_restore_never_clones_node_identity() {
        let backup = sample_logical_backup();
        let fresh = ClusterId::from_hex("55552233445566778899aabbccddeeff").expect("fresh");
        let restored = DiskIdentity::fresh(fresh, 55);
        assert_ne!(restored.cluster_id, backup.source_cluster_id);
        assert_ne!(restored.node_id, 1);
        assert_eq!(restored.lifecycle, IDENTITY_ACTIVE);
    }

    #[test]
    fn golden_ccbk_v2() {
        let expected = sample_logical_backup();
        let bytes = encode_backup_v2(&expected).expect("encode logical backup");
        let decoded = decode_backup_v2(&bytes).expect("decode logical backup");
        assert_eq!(decoded.source_cluster_id, expected.source_cluster_id);
        assert_eq!(decoded.source_index, expected.source_index);
        assert_eq!(decoded.source_term, expected.source_term);
        assert_eq!(decoded.checkpoint, expected.checkpoint);
        assert!(decoded.counts_as_cluster_complete());
    }

    #[test]
    fn trap_backup_recovery_is_not_reported_as_old_binary_downgrade() {
        let root = env::temp_dir().join(format!(
            "cc-node-backup-recovery-boundary-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&root).expect("root");
        let archive = root.join("logical.ccbk");
        let archive_bytes = encode_backup_v2(&sample_logical_backup()).expect("CCBK v2");
        fs::write(&archive, &archive_bytes).expect("archive");
        assert_eq!(
            u16::from_le_bytes(archive_bytes[4..6].try_into().expect("version")),
            BACKUP_VERSION,
            "the recovery path is the current logical CCBK v2 format"
        );
        let decoded = decode_backup_v2(&archive_bytes).expect("logical backup");
        assert_eq!(decoded.provenance, BackupProvenance::LogicalCluster);

        let fresh_cluster =
            ClusterId::from_hex("77772233445566778899aabbccddeeff").expect("fresh cluster");
        let target = root.join("fresh");
        restore_backup(&archive, &target, fresh_cluster, 7, false).expect("fresh recovery");
        let restored = DiskIdentity::decode(
            &fs::read(target.join("identity.ccid")).expect("restored identity"),
        )
        .expect("restored CCID");
        assert_eq!(restored.cluster_id, fresh_cluster);
        assert_ne!(restored.cluster_id, decoded.source_cluster_id);
        assert_eq!(fs::read(&archive).expect("source archive"), archive_bytes);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_selfcheck_deep_is_read_only() {
        let root = env::temp_dir().join(format!(
            "cc-node-selfcheck-readonly-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&root).expect("root");
        let input = root.join("backup.ccbk");
        fs::write(
            &input,
            encode_backup_v2(&sample_logical_backup()).expect("CCBK"),
        )
        .expect("backup");
        let target = root.join("restored");
        let fresh = ClusterId::from_hex("66662233445566778899aabbccddeeff").expect("fresh");
        restore_backup(&input, &target, fresh, 6, false).expect("restore");
        let watched = [
            target.join("identity.ccid"),
            target.join("ccdb.toml"),
            target.join("raft/wal.0"),
            target.join("snapshots/snapshot.1.ccsn"),
        ];
        let before = watched
            .iter()
            .map(|path| fs::read(path).expect("before"))
            .collect::<Vec<_>>();
        selfcheck(&[
            String::from("--data-dir"),
            target.display().to_string(),
            String::from("--deep"),
        ])
        .expect("deep selfcheck");
        let after = watched
            .iter()
            .map(|path| fs::read(path).expect("after"))
            .collect::<Vec<_>>();
        assert_eq!(before, after);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_legacy_backup_is_explicitly_refused() {
        let root = env::temp_dir().join(format!(
            "cc-node-legacy-backup-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&root).expect("legacy backup root");
        let archive = root.join("legacy.ccbk");
        let restored = root.join("restored");
        fs::write(
            &archive,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/golden/legacy/ccbk-v1.bin"
            )),
        )
        .expect("legacy backup fixture");

        let error = decode_backup_v2(&fs::read(&archive).expect("legacy fixture"))
            .expect_err("legacy archive must be refused");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "legacy node backup requires an explicit legacy importer"
        );
        assert!(
            !restored.exists(),
            "a refused restore must not create a target"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_legacy_backup_import_never_reuses_node_identity() {
        let root = env::temp_dir().join(format!(
            "cc-node-legacy-import-{}",
            process_time().as_nanos()
        ));
        fs::create_dir_all(&root).expect("legacy import root");
        let archive = root.join("legacy.ccbk");
        fs::write(
            &archive,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/golden/compat-base/ccbk-v1.bin"
            )),
        )
        .expect("compatibility-cut legacy backup");
        let imported = import_legacy_backup(&fs::read(&archive).expect("legacy bytes"))
            .expect("explicit legacy import");
        let fresh_cluster =
            ClusterId::from_hex("22222233445566778899aabbccddeeff").expect("fresh cluster");
        let target = root.join("restored");
        restore_backup(&archive, &target, fresh_cluster, 77, true).expect("fresh restore");
        let identity = DiskIdentity::decode(
            &fs::read(target.join("identity.ccid")).expect("restored identity"),
        )
        .expect("CCID");
        assert_eq!(identity.cluster_id, fresh_cluster);
        assert_eq!(identity.node_id, 77);
        assert_ne!(identity.cluster_id, imported.source_cluster_id);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn trap_legacy_session_without_command_never_wildcard_matches() {
        let imported = import_legacy_backup(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/compat-base/ccbk-v1.bin"
        )))
        .expect("explicit legacy import");
        let snapshot = decode_ccsn(
            &imported.checkpoint,
            imported.source_cluster_id.bytes(),
            BACKUP_MAX_FILE as u64,
        )
        .expect("imported checkpoint");
        assert_eq!(
            snapshot.sessions,
            cc_cluster::SessionTable::default(),
            "a v1 archive has no canonical command/reply receipt, so it must not create a wildcard retry cache"
        );
    }

    #[test]
    fn trap_legacy_backup_is_not_counted_as_cluster_complete() {
        let imported = import_legacy_backup(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tests/golden/compat-base/ccbk-v1.bin"
        )))
        .expect("explicit legacy import");
        assert_eq!(imported.provenance, BackupProvenance::LegacyNode);
        assert!(!imported.counts_as_cluster_complete());
    }

    #[test]
    fn trap_metrics_page_remains_local_and_dependency_free() {
        let dashboard = metrics_dashboard();
        assert!(dashboard.contains("fetch('/metrics')"));
        assert!(!dashboard.contains("https://"));
    }
}
