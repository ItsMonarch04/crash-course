// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Persistent JSON-facing theater bridge over the real deterministic cluster fixture."]

use cc_checker::Verdict;
use cc_core::{Duration, NodeId, Seed, Time};
use cc_raft::Role;
use cc_sim::{FaultAction, FaultProfile, RecorderLevel, RunSpec, WorkloadSpec};
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
    let spec = theater_spec(Seed::new(seed), profile);
    let cluster = SimCluster::new(spec.clone(), RecorderLevel::Theater)
        .expect("invariant: theater cluster fixture initializes");
    let last_snapshot = cluster.snapshot();
    SimHandle {
        spec,
        virtual_time: Time::from_nanos(0),
        cluster,
        last_snapshot,
    }
}

fn theater_spec(seed: Seed, profile: FaultProfile) -> RunSpec {
    let end_time = Time::from_nanos(60_000_000_000);
    let mut spec = RunSpec::standard(seed, profile);
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
    let action = if action_json.contains("reconfigure") {
        let target = json_u64_value(action_json, "drop").unwrap_or(node);
        FaultAction::Reconfigure {
            voters: (1..=handle.spec.config.node_count)
                .map(NodeId::new)
                .filter(|peer| *peer != NodeId::new(target))
                .collect(),
        }
    } else if action_json.contains("restart") {
        FaultAction::Restart {
            node: NodeId::new(node),
        }
    } else if action_json.contains("heal") {
        FaultAction::Heal
    } else if action_json.contains("partition") {
        let left = vec![NodeId::new(node)];
        let right = (1..=handle.spec.config.node_count)
            .map(NodeId::new)
            .filter(|peer| *peer != NodeId::new(node))
            .collect();
        FaultAction::Partition { left, right }
    } else if action_json.contains("clock") || action_json.contains("skew") {
        FaultAction::ClockSkew {
            node: NodeId::new(node),
            offset: Duration::from_millis(json_u64_value(action_json, "offset_ms").unwrap_or(25)),
        }
    } else if action_json.contains("disk") {
        FaultAction::DiskDegrade {
            node: NodeId::new(node),
            write_latency: Duration::from_millis(
                json_u64_value(action_json, "latency_ms").unwrap_or(5),
            ),
        }
    } else {
        FaultAction::Crash {
            node: NodeId::new(node),
        }
    };
    handle.cluster.inject(action);
    handle.spec = handle.cluster.spec().clone();
}

#[must_use]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
pub fn state(handle: &SimHandle) -> String {
    let nodes = handle
        .last_snapshot
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{{\"id\":{},\"status\":\"{}\",\"role\":\"{}\",\"term\":{},\"commit\":{},\"applied\":{},\"durable_bytes\":{},\"log_tail\":[{}]}}",
                node.id,
                status_name(node.status),
                role_name(node.role),
                node.term,
                node.commit,
                node.applied,
                node.durable_bytes,
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
        "{{\"theater_abi\":{},\"seed\":\"{}\",\"virtual_time_ns\":{},\"history_len\":{},\"completed_operations\":{},\"had_leader\":{},\"nodes\":[{}],\"trace\":{}}}",
        THEATER_ABI,
        handle.spec.seed,
        handle.virtual_time.as_nanos(),
        handle.last_snapshot.history_len,
        handle.last_snapshot.completed_operations,
        handle.last_snapshot.had_leader,
        nodes,
        handle.last_snapshot.trace.to_json()
    )
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

    #[test]
    fn inject_appends_fault_data_to_same_run() {
        let mut handle = init("{\"seed\":\"0x2b\",\"profile\":\"calm\"}");
        inject(&mut handle, "{\"action\":\"crash\",\"node\":1}");
        assert!(handle.spec.plan.actions.iter().any(|fault| matches!(
            &fault.action,
            FaultAction::Crash { node } if *node == NodeId::new(1)
        )));
    }
}
