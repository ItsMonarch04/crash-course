// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Small JSON-facing theater facade over the deterministic simulator."]

use cc_checker::{InvariantReport, check_trace_invariants};
use cc_core::{Seed, Time};
use cc_sim::{FaultProfile, RecorderLevel, RunSpec, Sim};

pub const THEATER_ABI: u16 = 1;

pub struct SimHandle {
    pub spec: RunSpec,
    pub virtual_time: Time,
    last_trace_json: String,
}

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
    SimHandle {
        spec: RunSpec::standard(Seed::new(seed), profile),
        virtual_time: Time::from_nanos(0),
        last_trace_json: String::from("{\"events\":[]}"),
    }
}

fn json_string_value(spec_json: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\"");
    let tail = spec_json.split_once(&marker)?.1;
    let value = tail.split_once(':')?.1.trim_start();
    let value = value.strip_prefix('"')?;
    Some(value.split('"').next()?.to_owned())
}

pub fn step(handle: &mut SimHandle, virtual_ns: u64) -> String {
    handle.virtual_time = handle.virtual_time + cc_core::Duration::from_nanos(virtual_ns);
    let mut sim = Sim::new(handle.spec.seed, handle.spec.config, RecorderLevel::Theater);
    sim.seed_toy_ticks();
    if let Ok(trace) = sim.run_toy() {
        handle.last_trace_json = trace.to_json();
    }
    handle.last_trace_json.clone()
}

pub fn inject(handle: &mut SimHandle, action_json: &str) {
    if action_json.contains("heal") {
        handle
            .spec
            .plan
            .actions
            .retain(|action| !matches!(&action.action, cc_sim::FaultAction::Partition { .. }));
    }
}

#[must_use]
pub fn state(handle: &SimHandle) -> String {
    format!(
        "{{\"theater_abi\":{},\"seed\":\"{}\",\"virtual_time_ns\":{},\"events_json\":{}}}",
        THEATER_ABI,
        handle.spec.seed,
        handle.virtual_time.as_nanos(),
        handle.last_trace_json
    )
}

#[must_use]
pub fn history_verdict(handle: &SimHandle) -> String {
    let mut sim = Sim::new(handle.spec.seed, handle.spec.config, RecorderLevel::Gate);
    sim.seed_toy_ticks();
    let report: InvariantReport = sim
        .run_toy()
        .map(|trace| check_trace_invariants(&trace))
        .unwrap_or_else(|_| InvariantReport {
            violations: vec![cc_checker::InvariantViolation {
                name: "runaway",
                detail: String::from("simulator runaway"),
            }],
        });
    if report.is_ok() {
        String::from("{\"verdict\":\"ok\"}")
    } else {
        format!(
            "{{\"verdict\":\"failed\",\"violations\":{}}}",
            report.violations.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facade_is_batch_oriented_and_stable() {
        let mut handle = init("{\"seed\":\"0x2a\",\"profile\":\"rough\"}");
        assert_eq!(handle.spec.seed, Seed::new(42));
        let first = step(&mut handle, 1_000);
        let second = step(&mut handle, 1_000);
        assert_eq!(first, second);
        assert!(state(&handle).contains("theater_abi"));
        assert!(history_verdict(&handle).contains("verdict"));
    }
}
