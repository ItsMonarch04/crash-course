// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Persistent JSON-facing theater bridge over the real deterministic cluster fixture."]

use cc_checker::Verdict;
use cc_core::{Duration, NodeId, P16, Seed, Time};
use cc_raft::Role;
use cc_sim::{
    FaultAction, FaultProfile, LinkConfig, RecorderLevel, RunSpec, SlowDisk, WorkloadSpec,
};
use cc_swarm::{ClusterSnapshot, SimCluster};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::wasm_bindgen;

pub const THEATER_ABI: u16 = 1;

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub struct SimHandle {
    spec: RunSpec,
    virtual_time: Time,
    cluster: SimCluster,
    last_snapshot: ClusterSnapshot,
    packet_loss_link: Option<(NodeId, NodeId)>,
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn init(spec_json: &str) -> SimHandle {
    let seed = json_string_value(spec_json, "seed")
        .and_then(|value| {
            let value = value.trim_start_matches("0x").trim_start_matches("0X");
            u64::from_str_radix(value, 16).ok()
        })
        .unwrap_or(0);
    let profile = json_string_value(spec_json, "profile")
        .and_then(|value| FaultProfile::parse(&value))
        .unwrap_or(FaultProfile::Calm);
    let nodes = json_number_value(spec_json, "nodes");
    let spec = theater_spec(Seed::new(seed), profile, nodes);
    let cluster = SimCluster::new(spec.clone(), RecorderLevel::Theater)
        .expect("invariant: theater cluster fixture initializes");
    let last_snapshot = cluster.snapshot();
    SimHandle {
        spec,
        virtual_time: Time::from_nanos(0),
        cluster,
        last_snapshot,
        packet_loss_link: None,
    }
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

/// Advance one persistent simulator by a virtual-time budget.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn step(handle: &mut SimHandle, virtual_ns: u64) -> String {
    handle.last_snapshot = handle
        .cluster
        .advance(Duration::from_nanos(virtual_ns))
        .expect("invariant: theater step stays within simulator guards");
    handle.virtual_time = handle.last_snapshot.virtual_time;
    state(handle)
}

/// Append a data-described fault to the same persistent run used by `step`.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn inject(handle: &mut SimHandle, action_json: &str) {
    let node = json_u64_value(action_json, "node").unwrap_or(1);
    let (action, packet_loss_link) = if action_json.contains("reconfigure") {
        let target = json_u64_value(action_json, "drop").unwrap_or(node);
        (
            FaultAction::Reconfigure {
                voters: (1..=handle.spec.config.node_count)
                    .map(NodeId::new)
                    .filter(|peer| *peer != NodeId::new(target))
                    .collect(),
            },
            None,
        )
    } else if action_json.contains("restart") {
        (
            FaultAction::Restart {
                node: NodeId::new(node),
            },
            None,
        )
    } else if action_json.contains("heal") {
        (FaultAction::Heal, None)
    } else if action_json.contains("partition") {
        let left = vec![NodeId::new(node)];
        let right = (1..=handle.spec.config.node_count)
            .map(NodeId::new)
            .filter(|peer| *peer != NodeId::new(node))
            .collect();
        (FaultAction::Partition { left, right }, None)
    } else if action_json.contains("link-degrade") {
        let from = NodeId::new(node);
        let fallback_to = if node >= handle.spec.config.node_count {
            1
        } else {
            node + 1
        };
        let to = NodeId::new(json_u64_value(action_json, "to").unwrap_or(fallback_to));
        let percent = json_u64_value(action_json, "drop_percent").unwrap_or(0);
        let percent = percent.min(100);
        (
            FaultAction::LinkDegrade {
                from,
                to,
                config: LinkConfig {
                    drop: percent_to_p16(percent),
                    ..LinkConfig::default()
                },
            },
            Some((from, to)),
        )
    } else if action_json.contains("clock") || action_json.contains("skew") {
        (
            FaultAction::ClockSkew {
                node: NodeId::new(node),
                offset: Duration::from_millis(
                    json_u64_value(action_json, "offset_ms").unwrap_or(25),
                ),
            },
            None,
        )
    } else if action_json.contains("slow-disk") {
        let extra = Duration::from_millis(
            json_u64_value(action_json, "latency_ms")
                .unwrap_or(0)
                .min(5_000),
        );
        (
            FaultAction::SlowDisk {
                node: NodeId::new(node),
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
    } else if action_json.contains("disk") {
        (
            FaultAction::DiskDegrade {
                node: NodeId::new(node),
                write_latency: Duration::from_millis(
                    json_u64_value(action_json, "latency_ms").unwrap_or(5),
                ),
            },
            None,
        )
    } else {
        (
            FaultAction::Crash {
                node: NodeId::new(node),
            },
            None,
        )
    };
    handle.cluster.inject(action);
    handle.last_snapshot = handle
        .cluster
        .advance(Duration::from_nanos(0))
        .expect("invariant: immediate theater fault applies within simulator guards");
    handle.spec = handle.cluster.spec().clone();
    if let Some(link) = packet_loss_link {
        handle.packet_loss_link = Some(link);
    }
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
                "{{\"id\":{},\"status\":\"{}\",\"role\":\"{}\",\"term\":{},\"commit\":{},\"applied\":{},\"durable_bytes\":{},\"disk_service_delay_ms\":{},\"log_tail\":[{}]}}",
                node.id,
                status_name(node.status),
                role_name(node.role),
                node.term,
                node.commit,
                node.applied,
                node.durable_bytes,
                node.disk_service_delay_ns / 1_000_000,
                node.log_tail
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"theater_abi\":{},\"seed\":\"{}\",\"virtual_time_ns\":{},\"history_len\":{},\"completed_operations\":{},\"had_leader\":{},\"link_drop_percent\":{},\"nodes\":[{}],\"trace\":{}}}",
        THEATER_ABI,
        handle.spec.seed,
        handle.virtual_time.as_nanos(),
        handle.last_snapshot.history_len,
        handle.last_snapshot.completed_operations,
        handle.last_snapshot.had_leader,
        link_drop_percent.unwrap_or(0),
        nodes,
        handle.last_snapshot.trace.to_json()
    )
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
    value
        .trim_start()
        .trim_matches(|character: char| !character.is_ascii_digit())
        .split(|character: char| !character.is_ascii_digit())
        .next()
        .and_then(|value| value.parse().ok())
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
        let mut handle = init("{\"seed\":\"0x2a\",\"profile\":\"calm\"}");
        let first = step(&mut handle, 400_000_000);
        let second = step(&mut handle, 700_000_000);
        assert_ne!(first, second);
        assert!(second.contains("\"nodes\":["));
        assert!(history_verdict(&handle).contains("verdict"));
    }

    /// The browser bridge owns JSON/handle translation only. Its progress is
    /// the same persistent, Driver-backed `SimCluster` used by native swarm
    /// runs; it does not build a second Raft/KV composition for wasm.
    #[test]
    fn trap_wasm_wraps_the_same_driver_backed_simcluster() {
        let mut handle = init("{\"seed\":\"0x91\",\"profile\":\"calm\",\"nodes\":3}");
        let before = handle.last_snapshot.clone();
        let rendered = step(&mut handle, 1_000_000_000);
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
            ));
            assert_eq!(handle.spec.config.node_count, count);
            let reported = state(&handle).matches("\"id\":").count();
            assert_eq!(reported as u64, count, "state reports every node");
        }
    }

    /// An unsupported size falls back to the standard cluster rather than
    /// building something the quorum arithmetic was never checked against.
    #[test]
    fn init_ignores_a_cluster_size_outside_the_offered_set() {
        let standard = init("{\"seed\":\"0x2a\",\"profile\":\"calm\"}")
            .spec
            .config
            .node_count;
        let handle = init("{\"seed\":\"0x2a\",\"profile\":\"calm\",\"nodes\":4}");
        assert_eq!(handle.spec.config.node_count, standard);
    }

    #[test]
    fn inject_appends_fault_data_to_same_run() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}");
        inject(&mut handle, "{\"action\":\"crash\",\"node\":1}");
        assert!(handle.spec.plan.actions.iter().any(|fault| matches!(
            &fault.action,
            FaultAction::Crash { node } if *node == NodeId::new(1)
        )));
    }

    #[test]
    fn trap_packet_loss_uses_the_effective_link_p16_value() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}");
        inject(
            &mut handle,
            "{\"action\":\"link-degrade\",\"node\":1,\"to\":2,\"drop_percent\":37}",
        );
        let config = handle
            .cluster
            .link_config(NodeId::new(1), NodeId::new(2))
            .expect("configured link");
        assert_eq!(config.drop, percent_to_p16(37));
        assert_eq!(p16_to_percent(config.drop), 37);
        assert!(state(&handle).contains("\"link_drop_percent\":37"));
    }

    #[test]
    fn trap_slow_disk_reports_the_effective_persistent_delay() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}");
        inject(
            &mut handle,
            "{\"action\":\"slow-disk\",\"node\":3,\"latency_ms\":64}",
        );
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
}
