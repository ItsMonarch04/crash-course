// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Persistent JSON-facing theater bridge over the real deterministic cluster fixture."]

use cc_checker::Verdict;
use std::collections::BTreeMap;

use cc_core::{Duration, NodeId, P16, Seed, Time, fnv1a};
use cc_raft::Role;
use cc_sim::{FaultAction, FaultProfile, RecorderLevel, RunSpec, SlowDisk, WorkloadSpec};
use cc_swarm::{ClusterSnapshot, SimCluster};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

pub const THEATER_ABI: u16 = 2;
pub const MAX_CHECKPOINT_COUNT: usize = 13;
pub const MAX_CHECKPOINT_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_CHECKPOINT_TOTAL_BYTES: u64 = 208 * 1024 * 1024;
pub const MAX_ABI_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_TRACE_PAGE_EVENTS: u32 = 4_096;
const MAX_ABI_INPUT_BYTES: usize = 4 * 1024;

#[derive(Clone)]
struct TheaterCheckpoint {
    spec: RunSpec,
    virtual_time: Time,
    cluster: SimCluster,
    last_snapshot: ClusterSnapshot,
    packet_loss_link: Option<(NodeId, NodeId)>,
    accounted_bytes: u64,
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub struct SimHandle {
    spec: RunSpec,
    virtual_time: Time,
    cluster: SimCluster,
    last_snapshot: ClusterSnapshot,
    packet_loss_link: Option<(NodeId, NodeId)>,
    checkpoints: BTreeMap<u64, TheaterCheckpoint>,
    next_checkpoint_id: u64,
    checkpoint_bytes: u64,
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn init(spec_json: &str) -> Result<SimHandle, String> {
    validate_json_input(spec_json)?;
    let seed = match json_string_value(spec_json, "seed") {
        Some(value) => {
            let value = value.trim_start_matches("0x").trim_start_matches("0X");
            u64::from_str_radix(value, 16).map_err(|_| abi_error("InvalidSeed"))?
        }
        None if json_has_key(spec_json, "seed") => return Err(abi_error("InvalidSeed")),
        None => 0,
    };
    let profile = match json_string_value(spec_json, "profile") {
        Some(value) => FaultProfile::parse(&value).ok_or_else(|| abi_error("InvalidProfile"))?,
        None if json_has_key(spec_json, "profile") => return Err(abi_error("InvalidProfile")),
        None => FaultProfile::Calm,
    };
    let nodes = json_number_value(spec_json, "nodes");
    if json_has_key(spec_json, "nodes")
        && !nodes.is_some_and(|count| THEATER_NODE_COUNTS.contains(&count))
    {
        return Err(abi_error("InvalidNodeCount"));
    }
    let spec = theater_spec(Seed::new(seed), profile, nodes);
    let cluster = SimCluster::new(spec.clone(), RecorderLevel::Theater)
        .map_err(|_| abi_error("SimulatorInitFailed"))?;
    let last_snapshot = cluster.snapshot();
    Ok(SimHandle {
        spec,
        virtual_time: Time::from_nanos(0),
        cluster,
        last_snapshot,
        packet_loss_link: None,
        checkpoints: BTreeMap::new(),
        next_checkpoint_id: 1,
        checkpoint_bytes: 0,
    })
}

/// Cluster sizes the theater offers. Odd sizes only — an even voter count buys
/// no extra failure tolerance and makes the quorum arithmetic on screen read
/// like a mistake.
const THEATER_NODE_COUNTS: [u64; 3] = [3, 5, 7];

fn theater_spec(seed: Seed, profile: FaultProfile, node_count: Option<u64>) -> RunSpec {
    let end_time = Time::from_nanos(60_000_000_000);
    let mut spec = RunSpec::standard(seed, profile);
    if let Some(count) = node_count.filter(|count| THEATER_NODE_COUNTS.contains(count)) {
        spec.config.node_count = count;
    }
    let nodes: Vec<NodeId> = (1..=spec.config.node_count).map(NodeId::new).collect();
    spec.config.end_time = end_time;
    spec.end_time = end_time;
    spec.plan = cc_sim::materialize_fault_plan(seed, profile, &nodes, end_time);
    spec.workload = WorkloadSpec {
        clients: 2,
        ops_per_second: 10,
        keyspace: 16,
        set_ttl: matches!(profile, FaultProfile::Ttl).then_some(Duration::from_millis(125)),
    };
    spec
}

fn json_number_value(spec_json: &str, key: &str) -> Option<u64> {
    let marker = format!("\"{key}\"");
    let tail = spec_json.split_once(&marker)?.1;
    let value = tail.split_once(':')?.1.trim_start();
    let digits: String = value.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

fn json_string_value(spec_json: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\"");
    let tail = spec_json.split_once(&marker)?.1;
    let value = tail.split_once(':')?.1.trim_start();
    let value = value.strip_prefix('"')?;
    Some(value.split('"').next()?.to_owned())
}

fn json_has_key(text: &str, key: &str) -> bool {
    text.contains(&format!("\"{key}\""))
}

fn validate_json_input(text: &str) -> Result<(), String> {
    if text.len() > MAX_ABI_INPUT_BYTES {
        return Err(abi_error("InputByteLimit"));
    }
    let trimmed = text.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(abi_error("InvalidJsonObject"));
    }
    Ok(())
}

fn abi_error(code: &str) -> String {
    debug_assert!(code.len() <= 64);
    code.to_owned()
}

/// Advance one persistent simulator by a virtual-time budget.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn step(handle: &mut SimHandle, virtual_ns: u64) -> Result<String, String> {
    if virtual_ns == 0 {
        return Err(abi_error("InvalidStepDuration"));
    }
    handle.last_snapshot = handle
        .cluster
        .advance(Duration::from_nanos(virtual_ns))
        .map_err(|_| abi_error("SimulatorStepFailed"))?;
    handle.virtual_time = handle.last_snapshot.virtual_time;
    Ok(state(handle))
}

/// Append a data-described fault to the same persistent run used by `step`.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn inject(handle: &mut SimHandle, action_json: &str) -> Result<(), String> {
    validate_json_input(action_json)?;
    for key in [
        "node",
        "drop",
        "to",
        "drop_percent",
        "offset_ms",
        "latency_ms",
    ] {
        if json_has_key(action_json, key) && json_u64_value(action_json, key).is_none() {
            return Err(abi_error("InvalidIntegerField"));
        }
    }
    let action_name =
        json_string_value(action_json, "action").ok_or_else(|| abi_error("InvalidFaultAction"))?;
    let node = json_u64_value(action_json, "node").unwrap_or(1);
    let node_id = |value: u64| {
        if value == 0 || value > handle.spec.config.node_count {
            Err(abi_error("InvalidNodeId"))
        } else {
            Ok(NodeId::new(value))
        }
    };
    let (action, packet_loss_link) = if action_name == "reconfigure" {
        let target = json_u64_value(action_json, "drop").unwrap_or(node);
        let target = node_id(target)?;
        (
            FaultAction::Reconfigure {
                voters: (1..=handle.spec.config.node_count)
                    .map(NodeId::new)
                    .filter(|peer| *peer != target)
                    .collect(),
            },
            None,
        )
    } else if action_name == "restart" {
        (
            FaultAction::Restart {
                node: node_id(node)?,
            },
            None,
        )
    } else if action_name == "heal" {
        (FaultAction::Heal, None)
    } else if action_name == "partition" {
        let isolated = node_id(node)?;
        let left = vec![isolated];
        let right = (1..=handle.spec.config.node_count)
            .map(NodeId::new)
            .filter(|peer| *peer != isolated)
            .collect();
        (FaultAction::Partition { left, right }, None)
    } else if action_name == "link-degrade" {
        let from = node_id(node)?;
        let fallback_to = if node >= handle.spec.config.node_count {
            1
        } else {
            node + 1
        };
        let to = node_id(json_u64_value(action_json, "to").unwrap_or(fallback_to))?;
        let percent = json_u64_value(action_json, "drop_percent").unwrap_or(0);
        if percent > 100 {
            return Err(abi_error("InvalidDropPercent"));
        }
        let mut config = handle.cluster.link_config(from, to).unwrap_or_default();
        config.drop = percent_to_p16(percent);
        (
            FaultAction::LinkDegrade { from, to, config },
            Some((from, to)),
        )
    } else if action_name == "clock-skew" {
        let offset_ms = json_u64_value(action_json, "offset_ms").unwrap_or(25);
        if offset_ms > 5_000 {
            return Err(abi_error("InvalidClockOffset"));
        }
        (
            FaultAction::ClockSkew {
                node: node_id(node)?,
                offset: Duration::from_millis(offset_ms),
            },
            None,
        )
    } else if action_name == "slow-disk" {
        let latency_ms = json_u64_value(action_json, "latency_ms").unwrap_or(0);
        if latency_ms > 5_000 {
            return Err(abi_error("InvalidDiskLatency"));
        }
        let extra = Duration::from_millis(latency_ms);
        (
            FaultAction::SlowDisk {
                node: node_id(node)?,
                slow: SlowDisk {
                    read_extra: extra,
                    write_extra: extra,
                    fsync_extra: extra,
                    rename_extra: extra,
                    dirsync_extra: extra,
                },
            },
            None,
        )
    } else if action_name == "disk-degrade" {
        (
            FaultAction::DiskDegrade {
                node: node_id(node)?,
                write_latency: Duration::from_millis(
                    json_u64_value(action_json, "latency_ms").unwrap_or(5),
                ),
            },
            None,
        )
    } else if action_name == "crash" {
        (
            FaultAction::Crash {
                node: node_id(node)?,
            },
            None,
        )
    } else {
        return Err(abi_error("UnknownFaultAction"));
    };
    handle.cluster.inject(action);
    handle.last_snapshot = handle
        .cluster
        .advance(Duration::from_nanos(0))
        .map_err(|_| abi_error("FaultInjectionFailed"))?;
    handle.spec = handle.cluster.spec().clone();
    if let Some(link) = packet_loss_link {
        handle.packet_loss_link = Some(link);
    }
    Ok(())
}

#[must_use]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn state(handle: &SimHandle) -> String {
    let link_drop_percent = handle.packet_loss_link.and_then(|(from, to)| {
        handle
            .cluster
            .link_config(from, to)
            .map(|config| p16_to_percent(config.drop))
    });
    let nodes = handle
        .last_snapshot
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{{\"id\":{},\"status\":\"{}\",\"role\":\"{}\",\"term\":{},\"commit\":{},\"applied\":{},\"durable_bytes\":{},\"disk_service_delay_ms\":{},\"clock_offset_ms\":{},\"log_tail\":[{}]}}",
                node.id,
                status_name(node.status),
                role_name(node.role),
                node.term,
                node.commit,
                node.applied,
                node.durable_bytes,
                node.disk_service_delay_ns / 1_000_000,
                node.clock_offset_ns / 1_000_000,
                node.log_tail
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let rendered = format!(
        "{{\"theater_abi\":{},\"seed\":\"{}\",\"virtual_time_ns\":{},\"history_len\":{},\"completed_operations\":{},\"had_leader\":{},\"link_drop_percent\":{},\"checkpoint_count\":{},\"checkpoint_bytes\":{},\"nodes\":[{}]}}",
        THEATER_ABI,
        handle.spec.seed,
        handle.virtual_time.as_nanos(),
        handle.last_snapshot.history_len,
        handle.last_snapshot.completed_operations,
        handle.last_snapshot.had_leader,
        link_drop_percent.unwrap_or(0),
        handle.checkpoints.len(),
        handle.checkpoint_bytes,
        nodes,
    );
    debug_assert!(rendered.len() <= MAX_ABI_RESPONSE_BYTES);
    rendered
}

/// Retain one complete in-memory simulator image.  `SimCluster: Clone`
/// deliberately includes scheduler/RNG/network/disk/driver volatile state;
/// this is not a replay-from-zero token disguised as a checkpoint.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn checkpoint(handle: &mut SimHandle) -> Result<u64, String> {
    if handle.checkpoints.len() >= MAX_CHECKPOINT_COUNT {
        return Err(String::from("CheckpointCountLimit"));
    }
    let accounted_bytes = checkpoint_accounted_bytes(handle);
    if accounted_bytes > MAX_CHECKPOINT_BYTES
        || handle.checkpoint_bytes.saturating_add(accounted_bytes) > MAX_CHECKPOINT_TOTAL_BYTES
    {
        return Err(String::from("CheckpointByteLimit"));
    }
    let id = handle.next_checkpoint_id;
    handle.next_checkpoint_id = handle
        .next_checkpoint_id
        .checked_add(1)
        .ok_or_else(|| String::from("CheckpointIdOverflow"))?;
    handle.checkpoints.insert(
        id,
        TheaterCheckpoint {
            spec: handle.spec.clone(),
            virtual_time: handle.virtual_time,
            cluster: handle.cluster.clone(),
            last_snapshot: handle.last_snapshot.clone(),
            packet_loss_link: handle.packet_loss_link,
            accounted_bytes,
        },
    );
    handle.checkpoint_bytes = handle.checkpoint_bytes.saturating_add(accounted_bytes);
    Ok(id)
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn restore(handle: &mut SimHandle, checkpoint_id: u64) -> Result<String, String> {
    let saved = handle
        .checkpoints
        .get(&checkpoint_id)
        .cloned()
        .ok_or_else(|| String::from("InvalidCheckpointId"))?;
    handle.spec = saved.spec;
    handle.virtual_time = saved.virtual_time;
    handle.cluster = saved.cluster;
    handle.last_snapshot = saved.last_snapshot;
    handle.packet_loss_link = saved.packet_loss_link;
    Ok(state(handle))
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn drop_checkpoint(handle: &mut SimHandle, checkpoint_id: u64) -> Result<(), String> {
    let saved = handle
        .checkpoints
        .remove(&checkpoint_id)
        .ok_or_else(|| String::from("InvalidCheckpointId"))?;
    handle.checkpoint_bytes = handle
        .checkpoint_bytes
        .saturating_sub(saved.accounted_bytes);
    Ok(())
}

#[must_use]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn trace_hash(handle: &SimHandle) -> String {
    format!("{:016x}", fnv1a(&handle.last_snapshot.trace.encode()))
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn trace_page(handle: &SimHandle, cursor: u64, max_events: u32) -> Result<String, String> {
    if max_events == 0 || max_events > MAX_TRACE_PAGE_EVENTS {
        return Err(String::from("InvalidTracePageLimit"));
    }
    let start = usize::try_from(cursor).map_err(|_| String::from("InvalidTraceCursor"))?;
    let events = &handle.last_snapshot.trace.events;
    if start > events.len() {
        return Err(String::from("StaleTraceCursor"));
    }
    let end = start
        .saturating_add(usize::try_from(max_events).unwrap_or(usize::MAX))
        .min(events.len());
    let mut encoded = String::from("{\"events\":[");
    let mut emitted = 0_usize;
    for event in &events[start..end] {
        let item = format!(
            "{{\"seq\":{},\"time_ns\":{},\"node\":{},\"kind\":\"{}\",\"payload_hex\":\"{}\"}}",
            event.seq,
            event.time.as_nanos(),
            event.node.map_or(0, NodeId::get),
            event.kind.as_str(),
            hex(&event.payload),
        );
        let footer_reserve = 96_usize;
        let required = encoded
            .len()
            .saturating_add(usize::from(emitted != 0))
            .saturating_add(item.len())
            .saturating_add(footer_reserve);
        if required > MAX_ABI_RESPONSE_BYTES {
            if emitted == 0 {
                return Err(String::from("TraceEventExceedsAbiByteCap"));
            }
            break;
        }
        if emitted != 0 {
            encoded.push(',');
        }
        encoded.push_str(&item);
        emitted += 1;
    }
    let next = start.saturating_add(emitted);
    encoded.push_str(&format!(
        "],\"next_cursor\":{next},\"done\":{}}}",
        next == events.len()
    ));
    if encoded.len() > MAX_ABI_RESPONSE_BYTES {
        return Err(String::from("AbiResponseByteCap"));
    }
    Ok(encoded)
}

fn checkpoint_accounted_bytes(handle: &SimHandle) -> u64 {
    let trace = u64::try_from(handle.last_snapshot.trace.encode().len()).unwrap_or(u64::MAX);
    let durable = handle
        .last_snapshot
        .nodes
        .iter()
        .fold(0_u64, |total, node| {
            total.saturating_add(node.durable_bytes)
        });
    let node_state = u64::try_from(handle.last_snapshot.nodes.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(512);
    let history = u64::try_from(handle.last_snapshot.history_len)
        .unwrap_or(u64::MAX)
        .saturating_mul(192);
    // Fixed/container charges are explicit so the accounting remains stable
    // across allocator and browser implementations.
    512_u64
        .saturating_add(trace)
        .saturating_add(durable)
        .saturating_add(node_state)
        .saturating_add(history)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn percent_to_p16(percent: u64) -> P16 {
    let scaled = percent.saturating_mul(u64::from(u16::MAX));
    let rounded = scaled.saturating_add(50) / 100;
    P16::new(u16::try_from(rounded).unwrap_or(u16::MAX))
}

fn p16_to_percent(value: P16) -> u64 {
    u64::from(value.numerator())
        .saturating_mul(100)
        .saturating_add(u64::from(u16::MAX) / 2)
        / u64::from(u16::MAX)
}

#[must_use]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn history_verdict(handle: &SimHandle) -> String {
    let verdict = match handle.last_snapshot.verdict {
        Verdict::Linearizable { .. } => "ok",
        Verdict::NotLinearizable { .. } => "failed",
        Verdict::Undecided { .. } => "undecided",
    };
    format!(
        "{{\"verdict\":\"{verdict}\",\"history_len\":{}}}",
        handle.last_snapshot.history_len
    )
}

fn json_u64_value(text: &str, key: &str) -> Option<u64> {
    let marker = format!("\"{key}\"");
    let tail = text.split_once(&marker)?.1;
    let value = tail.split_once(':')?.1;
    let digits: String = value
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn status_name(status: cc_sim::NodeStatus) -> &'static str {
    match status {
        cc_sim::NodeStatus::Up => "up",
        cc_sim::NodeStatus::Crashed => "crashed",
        cc_sim::NodeStatus::Wiped => "wiped",
        cc_sim::NodeStatus::StorageFault => "storage-fault",
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Follower => "follower",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
        Role::Learner => "learner",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_steps_one_persistent_cluster_and_exposes_real_nodes() {
        let mut handle = init("{\"seed\":\"0x2a\",\"profile\":\"calm\"}").expect("valid spec");
        let first = step(&mut handle, 400_000_000).expect("valid step");
        let second = step(&mut handle, 700_000_000).expect("valid step");
        assert_ne!(first, second);
        assert!(second.contains("\"nodes\":["));
        assert!(history_verdict(&handle).contains("verdict"));
    }

    /// The browser bridge owns JSON/handle translation only. Its progress is
    /// the same persistent, Driver-backed `SimCluster` used by native swarm
    /// runs; it does not build a second Raft/KV composition for wasm.
    #[test]
    fn trap_wasm_wraps_the_same_driver_backed_simcluster() {
        let mut handle =
            init("{\"seed\":\"0x91\",\"profile\":\"calm\",\"nodes\":3}").expect("valid spec");
        let before = handle.last_snapshot.clone();
        let rendered = step(&mut handle, 1_000_000_000).expect("valid step");
        assert!(handle.last_snapshot.virtual_time > before.virtual_time);
        assert!(
            handle
                .last_snapshot
                .nodes
                .iter()
                .any(|node| node.durable_bytes > 0),
            "the shared simulator path must include its durable Driver WAL"
        );
        assert!(rendered.contains("\"nodes\":["));
    }

    /// The theater's cluster-size control has to reach the engine. It used to
    /// be a `<select>` with no handler and a `defaultValue` matching none of
    /// its options, so it displayed "3 nodes" over a five-node cluster.
    #[test]
    fn init_honours_the_requested_cluster_size() {
        for count in THEATER_NODE_COUNTS {
            let handle = init(&format!(
                "{{\"seed\":\"0x2a\",\"profile\":\"calm\",\"nodes\":{count}}}"
            ))
            .expect("valid spec");
            assert_eq!(handle.spec.config.node_count, count);
            let reported = state(&handle).matches("\"id\":").count();
            assert_eq!(reported as u64, count, "state reports every node");
        }
    }

    /// Unsupported sizes are rejected instead of silently running a cluster
    /// different from the one the UI claimed to initialize.
    #[test]
    fn init_rejects_a_cluster_size_outside_the_offered_set() {
        let standard = init("{\"seed\":\"0x2a\",\"profile\":\"calm\"}")
            .expect("default spec")
            .spec
            .config
            .node_count;
        assert!(THEATER_NODE_COUNTS.contains(&standard));
        assert_eq!(
            init("{\"seed\":\"0x2a\",\"profile\":\"calm\",\"nodes\":4}").err(),
            Some(String::from("InvalidNodeCount"))
        );
    }

    #[test]
    fn inject_appends_fault_data_to_same_run() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}").expect("valid spec");
        inject(&mut handle, "{\"action\":\"crash\",\"node\":1}").expect("valid fault");
        assert!(handle.spec.plan.actions.iter().any(|fault| matches!(
            &fault.action,
            FaultAction::Crash { node } if *node == NodeId::new(1)
        )));
    }

    #[test]
    fn trap_abi_rejects_unknown_actions_and_invalid_ids_without_mutation() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}").expect("valid spec");
        let before = handle.spec.plan.actions.len();
        assert_eq!(
            inject(&mut handle, "{\"action\":\"typo\",\"node\":1}"),
            Err(String::from("UnknownFaultAction"))
        );
        assert_eq!(
            inject(&mut handle, "{\"action\":\"crash\",\"node\":0}"),
            Err(String::from("InvalidNodeId"))
        );
        assert_eq!(
            inject(&mut handle, "{\"action\":\"crash\",\"node\":-1}"),
            Err(String::from("InvalidIntegerField"))
        );
        assert_eq!(handle.spec.plan.actions.len(), before);
    }

    #[test]
    fn trap_packet_loss_uses_the_effective_link_p16_value() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}").expect("valid spec");
        inject(
            &mut handle,
            "{\"action\":\"link-degrade\",\"node\":1,\"to\":2,\"drop_percent\":37}",
        )
        .expect("valid fault");
        let config = handle
            .cluster
            .link_config(NodeId::new(1), NodeId::new(2))
            .expect("configured link");
        assert_eq!(config.drop, percent_to_p16(37));
        assert_eq!(p16_to_percent(config.drop), 37);
        assert!(state(&handle).contains("\"link_drop_percent\":37"));
    }

    #[test]
    fn trap_packet_loss_preserves_other_effective_link_fields() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}").expect("valid spec");
        let mut configured = handle
            .cluster
            .link_config(NodeId::new(1), NodeId::new(2))
            .expect("default link");
        configured.base_delay = Duration::from_millis(77);
        configured.duplicate = P16::new(1_234);
        handle.cluster.inject(FaultAction::LinkDegrade {
            from: NodeId::new(1),
            to: NodeId::new(2),
            config: configured,
        });
        handle
            .cluster
            .advance(Duration::default())
            .expect("apply setup");

        inject(
            &mut handle,
            "{\"action\":\"link-degrade\",\"node\":1,\"to\":2,\"drop_percent\":37}",
        )
        .expect("valid fault");
        let effective = handle
            .cluster
            .link_config(NodeId::new(1), NodeId::new(2))
            .expect("configured link");
        assert_eq!(effective.base_delay, configured.base_delay);
        assert_eq!(effective.duplicate, configured.duplicate);
        assert_eq!(effective.drop, percent_to_p16(37));
    }

    #[test]
    fn trap_slow_disk_reports_the_effective_persistent_delay() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}").expect("valid spec");
        inject(
            &mut handle,
            "{\"action\":\"slow-disk\",\"node\":3,\"latency_ms\":64}",
        )
        .expect("valid fault");
        let node = handle
            .last_snapshot
            .nodes
            .iter()
            .find(|node| node.id == 3)
            .expect("selected node");
        assert_eq!(
            node.disk_service_delay_ns,
            Duration::from_millis(64).as_nanos()
        );
        assert!(state(&handle).contains("\"disk_service_delay_ms\":64"));
    }

    #[test]
    fn trap_checkpoint_restore_replays_identical_complete_state() {
        let mut handle = init("{\"seed\":\"0x55\",\"profile\":\"brutal\"}").expect("valid spec");
        step(&mut handle, 2_000_000_000).expect("valid step");
        let checkpoint_id = checkpoint(&mut handle).expect("bounded checkpoint");
        step(&mut handle, 750_000_000).expect("valid step");
        let uninterrupted = trace_hash(&handle);
        restore(&mut handle, checkpoint_id).expect("restore complete image");
        step(&mut handle, 750_000_000).expect("valid step");
        assert_eq!(trace_hash(&handle), uninterrupted);
        drop_checkpoint(&mut handle, checkpoint_id).expect("drop checkpoint");
        assert_eq!(handle.checkpoint_bytes, 0);
    }

    #[test]
    fn trap_wasm_state_and_trace_pages_obey_the_abi_byte_cap() {
        let mut handle = init("{\"seed\":\"0x56\",\"profile\":\"calm\"}").expect("valid spec");
        step(&mut handle, 3_000_000_000).expect("valid step");
        let summary = state(&handle);
        assert!(summary.len() <= MAX_ABI_RESPONSE_BYTES);
        assert!(!summary.contains("\"trace\""));
        let mut cursor = 0_u64;
        loop {
            let page = trace_page(&handle, cursor, 17).expect("bounded trace page");
            assert!(page.len() <= MAX_ABI_RESPONSE_BYTES);
            let next = json_number_value(&page, "next_cursor").expect("next cursor");
            assert!(next >= cursor);
            if page.contains("\"done\":true") {
                break;
            }
            assert!(next > cursor);
            cursor = next;
        }
        assert_eq!(
            trace_page(
                &handle,
                u64::try_from(handle.last_snapshot.trace.events.len()).unwrap_or(u64::MAX) + 1,
                1,
            ),
            Err(String::from("StaleTraceCursor"))
        );
    }
}
