// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

#![forbid(unsafe_code)]
#![doc = "Deterministic, host-independent vocabulary shared by Crash Course crates."]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::{Add, Sub};

pub type Bytes = Vec<u8>;

macro_rules! id_type {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
        pub struct $name(pub u64);

        impl $name {
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }
    };
}

id_type!(NodeId, "n");
id_type!(ClientId, "c");
id_type!(RequestSeq, "q");
id_type!(Term, "t");
id_type!(LogIndex, "i");
id_type!(IoId, "io");
id_type!(TimerId, "timer");

/// Immutable, non-secret identity for one logical cluster.  The text form is
/// deliberately fixed-width lowercase hexadecimal so configuration parsing
/// cannot admit aliases for the same on-disk/wire value.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ClusterId([u8; 16]);

impl ClusterId {
    #[must_use]
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        let mut index = 0;
        while index < self.0.len() {
            if self.0[index] != 0 {
                return false;
            }
            index += 1;
        }
        true
    }

    /// Parse exactly 32 lowercase hexadecimal characters.  Cluster IDs are
    /// identifiers, not secrets, but accepting alternative spellings makes
    /// audit and identity comparisons needlessly ambiguous.
    pub fn from_hex(value: &str) -> Result<Self, &'static str> {
        if value.len() != 32 {
            return Err("cluster id must be exactly 32 lowercase hexadecimal characters");
        }
        let mut bytes = [0_u8; 16];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            bytes[index] = (high << 4) | low;
        }
        let id = Self(bytes);
        if id.is_zero() {
            Err("cluster id must not be all zero")
        } else {
            Ok(id)
        }
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(32);
        for byte in self.0 {
            output.push(char::from(HEX[usize::from(byte >> 4)]));
            output.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        output
    }
}

fn hex_nibble(value: u8) -> Result<u8, &'static str> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("cluster id must be exactly 32 lowercase hexadecimal characters"),
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Nanoseconds from the host-defined epoch. Core code never reads a wall clock.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Time(u64);

impl Time {
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn checked_add(self, duration: Duration) -> Option<Self> {
        match self.0.checked_add(duration.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn checked_sub(self, duration: Duration) -> Option<Self> {
        match self.0.checked_sub(duration.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

/// A non-negative duration expressed in nanoseconds.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Duration(u64);

impl Duration {
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn from_micros(micros: u64) -> Self {
        Self(micros.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl Add<Duration> for Time {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self.checked_add(rhs)
            .expect("invariant: virtual time overflow")
    }
}

impl Sub<Time> for Time {
    type Output = Duration;

    fn sub(self, rhs: Time) -> Self::Output {
        Duration::from_nanos(
            self.0
                .checked_sub(rhs.0)
                .expect("invariant: time subtraction must be ordered"),
        )
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

/// A run seed. The hexadecimal form is the stable human-facing form.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Seed(pub u64);

impl Seed {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:016x}", self.0)
    }
}

/// A probability represented as numerator / 2^16.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct P16(u16);

impl P16 {
    pub const ZERO: Self = Self(0);
    pub const MAX: Self = Self(u16::MAX);

    #[must_use]
    pub const fn new(numerator: u16) -> Self {
        Self(numerator)
    }

    #[must_use]
    pub const fn numerator(self) -> u16 {
        self.0
    }
}

/// Integer-only delay distributions used by the simulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DelayDist {
    Fixed(Duration),
    Uniform {
        low: Duration,
        high: Duration,
    },
    TwoPoint {
        short: Duration,
        long: Duration,
        long_chance: P16,
    },
    /// An integer-only empirical CDF represented by repeated equiprobable
    /// buckets. `count` selects the canonical prefix; unused slots are zero.
    Empirical {
        buckets: [Duration; 16],
        count: u8,
    },
}

impl Default for DelayDist {
    fn default() -> Self {
        Self::Fixed(Duration::default())
    }
}

/// SplitMix64 is used only to derive stable component streams.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// Deterministic xoshiro256++ stream with domain-separated construction.
#[derive(Clone, Debug)]
pub struct Xoshiro256pp {
    state: [u64; 4],
}

impl Xoshiro256pp {
    #[must_use]
    pub fn stream(seed: Seed, domain: &'static str, index: u64) -> Self {
        let domain_hash = fnv1a(domain.as_bytes());
        let mut splitter = SplitMix64::new(seed.0 ^ domain_hash ^ index.rotate_left(17));
        let mut state = [0; 4];
        for slot in &mut state {
            *slot = splitter.next_u64();
        }
        if state == [0; 4] {
            state[0] = 1;
        }
        Self { state }
    }

    #[must_use]
    pub fn u64(&mut self) -> u64 {
        let result = self.state[0]
            .wrapping_add(self.state[3])
            .rotate_left(23)
            .wrapping_add(self.state[0]);
        let t = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    #[must_use]
    pub fn range_u64(&mut self, low: u64, high: u64) -> u64 {
        assert!(low < high, "invariant: RNG range must be non-empty");
        let span = high - low;
        low + self.u64().wrapping_rem(span)
    }

    #[must_use]
    pub fn chance(&mut self, probability: P16) -> bool {
        (self.u64() >> 48) < u64::from(probability.0)
    }

    #[must_use]
    pub fn sample_delay(&mut self, distribution: DelayDist) -> Duration {
        match distribution {
            DelayDist::Fixed(value) => value,
            DelayDist::Uniform { low, high } => {
                if low >= high {
                    low
                } else {
                    Duration::from_nanos(self.range_u64(low.0, high.0.saturating_add(1)))
                }
            }
            DelayDist::TwoPoint {
                short,
                long,
                long_chance,
            } => {
                if self.chance(long_chance) {
                    long
                } else {
                    short
                }
            }
            DelayDist::Empirical { buckets, count } => {
                let count = usize::from(count.clamp(1, 16));
                buckets[self.range_u64(0, count as u64) as usize]
            }
        }
    }
}

#[must_use]
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

pub const MAX_CODEC_BYTES: usize = 4 * 1024 * 1024;

/// Local, non-replicated resource limits. Unlike [`ClusterPolicy`], these
/// values cannot change the result of an accepted replicated command; they
/// bound host admission, queues, tracing, and file operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostLimits {
    pub max_pending_peer: usize,
    pub max_pending_timer: usize,
    pub max_pending_io: usize,
    pub max_pending_client: usize,
    /// Aggregate admission caps for Driver's class-separated input queues.
    /// These are host-only: overflowing them changes only whether a caller is
    /// admitted, never the result of an accepted replicated command.
    pub max_driver_pending_inputs: usize,
    pub max_driver_pending_input_bytes: usize,
    pub max_events: u64,
    pub max_events_per_instant: u64,
    pub max_trace_bytes: usize,
    pub max_snapshot_bytes: u64,
    pub max_block_read_bytes: u32,
    pub max_manifest_record_bytes: u32,
    pub max_threads: usize,
    pub thread_stack_bytes: usize,
    pub max_peer_frame_bytes: u64,
    pub max_uncommitted_entries: u64,
    pub max_uncommitted_bytes: u64,
    pub max_log_bytes_before_snapshot: u64,
    pub max_raft_log_bytes: u64,
    pub max_store_wal_bytes: u64,
    pub max_data_dir_bytes: u64,
    pub maintenance_reserve_bytes: u64,
    pub max_snapshot_chunk_bytes: u64,
    pub max_snapshot_staging_bytes: u64,
    pub max_snapshot_pins: u64,
    pub max_checkpoint_builder_bytes: u64,
    pub max_pending_reads: u64,
    pub max_pending_read_bytes: u64,
    pub max_pending_client_routes: u64,
    pub max_host_connections: u64,
    pub max_open_files: u64,
    pub max_host_thread_stack_bytes: u64,
    pub max_host_input_bytes: u64,
    pub max_host_total_input_bytes: u64,
    pub max_host_output_bytes: u64,
    pub max_host_total_output_bytes: u64,
    pub max_host_queued_requests: u64,
    pub max_host_total_queued_requests: u64,
    pub max_driver_pending_effects: u64,
    pub max_driver_pending_effect_bytes: u64,
    pub max_network_inflight_bytes: u64,
    pub max_fault_replay_bytes: u64,
    pub max_memtable_bytes: u64,
    pub max_frozen_memtables: u64,
    pub max_sst_files: u64,
    pub max_referenced_sst_bytes: u64,
    pub max_sst_metadata_bytes: u64,
    pub max_manifest_generations: u64,
    pub max_compaction_builder_bytes: u64,
    pub max_history_operations: u64,
    pub max_history_bytes: u64,
    pub max_failure_artifact_bytes: u64,
}

impl Default for HostLimits {
    fn default() -> Self {
        Self {
            max_pending_peer: 4_096,
            max_pending_timer: 1_024,
            max_pending_io: 1_024,
            max_pending_client: 4_096,
            max_driver_pending_inputs: 16_384,
            max_driver_pending_input_bytes: 128 * 1024 * 1024,
            max_events: 2_000_000,
            max_events_per_instant: 100_000,
            max_trace_bytes: 64 * 1024 * 1024,
            max_snapshot_bytes: 4 * 1024 * 1024 * 1024,
            max_block_read_bytes: 4 * 1024 * 1024,
            max_manifest_record_bytes: 256 * 1024 * 1024,
            max_threads: 2_100,
            thread_stack_bytes: 256 * 1024,
            max_peer_frame_bytes: 4 * 1024 * 1024,
            max_uncommitted_entries: 4_096,
            max_uncommitted_bytes: 64 * 1024 * 1024,
            max_log_bytes_before_snapshot: 64 * 1024 * 1024,
            max_raft_log_bytes: 256 * 1024 * 1024,
            max_store_wal_bytes: 256 * 1024 * 1024,
            max_data_dir_bytes: 16 * 1024 * 1024 * 1024,
            maintenance_reserve_bytes: 5 * 1024 * 1024 * 1024,
            max_snapshot_chunk_bytes: 1024 * 1024,
            max_snapshot_staging_bytes: 5 * 1024 * 1024 * 1024,
            max_snapshot_pins: 4,
            max_checkpoint_builder_bytes: 2 * 1024 * 1024,
            max_pending_reads: 1_024,
            max_pending_read_bytes: 64 * 1024 * 1024,
            max_pending_client_routes: 4_096,
            max_host_connections: 1_024,
            max_open_files: 4_096,
            max_host_thread_stack_bytes: 525 * 1024 * 1024,
            max_host_input_bytes: 16 * 1024 * 1024,
            max_host_total_input_bytes: 64 * 1024 * 1024,
            max_host_output_bytes: 16 * 1024 * 1024,
            max_host_total_output_bytes: 64 * 1024 * 1024,
            max_host_queued_requests: 1_024,
            max_host_total_queued_requests: 16_384,
            max_driver_pending_effects: 16_384,
            max_driver_pending_effect_bytes: 128 * 1024 * 1024,
            max_network_inflight_bytes: 256 * 1024 * 1024,
            max_fault_replay_bytes: 16 * 1024 * 1024,
            max_memtable_bytes: 64 * 1024 * 1024,
            max_frozen_memtables: 2,
            max_sst_files: 16_384,
            max_referenced_sst_bytes: 5 * 1024 * 1024 * 1024,
            max_sst_metadata_bytes: 240 * 1024 * 1024,
            max_manifest_generations: 64,
            max_compaction_builder_bytes: 16 * 1024 * 1024,
            max_history_operations: 100_000,
            max_history_bytes: 256 * 1024 * 1024,
            max_failure_artifact_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitError {
    pub field: &'static str,
}

impl HostLimits {
    #[must_use]
    pub fn is_valid(self) -> bool {
        self.validate().is_ok()
    }

    pub fn validate(self) -> Result<(), LimitError> {
        let nonzero = self.max_pending_peer > 0
            && self.max_pending_timer > 0
            && self.max_pending_io > 0
            && self.max_pending_client > 0
            && self.max_driver_pending_inputs > 0
            && self.max_driver_pending_input_bytes > 0
            && self.max_events > 0
            && self.max_events_per_instant > 0
            && self.max_trace_bytes > 0
            && self.max_snapshot_bytes > 0
            && self.max_block_read_bytes > 0
            && self.max_manifest_record_bytes > 0
            && self.max_threads > 0
            && self.thread_stack_bytes > 0;
        if !nonzero {
            return Err(LimitError {
                field: "host admission limit",
            });
        }
        macro_rules! require_nonzero {
            ($($field:ident),+ $(,)?) => {
                $(if self.$field == 0 { return Err(LimitError { field: stringify!($field) }); })+
            };
        }
        require_nonzero!(
            max_peer_frame_bytes,
            max_uncommitted_entries,
            max_uncommitted_bytes,
            max_log_bytes_before_snapshot,
            max_raft_log_bytes,
            max_store_wal_bytes,
            max_data_dir_bytes,
            maintenance_reserve_bytes,
            max_snapshot_chunk_bytes,
            max_snapshot_staging_bytes,
            max_snapshot_pins,
            max_checkpoint_builder_bytes,
            max_pending_reads,
            max_pending_read_bytes,
            max_pending_client_routes,
            max_host_connections,
            max_open_files,
            max_host_thread_stack_bytes,
            max_host_input_bytes,
            max_host_total_input_bytes,
            max_host_output_bytes,
            max_host_total_output_bytes,
            max_host_queued_requests,
            max_host_total_queued_requests,
            max_driver_pending_effects,
            max_driver_pending_effect_bytes,
            max_network_inflight_bytes,
            max_fault_replay_bytes,
            max_memtable_bytes,
            max_frozen_memtables,
            max_sst_files,
            max_referenced_sst_bytes,
            max_sst_metadata_bytes,
            max_manifest_generations,
            max_compaction_builder_bytes,
            max_history_operations,
            max_history_bytes,
            max_failure_artifact_bytes,
        );
        let relationships = [
            (
                self.max_snapshot_chunk_bytes <= self.max_peer_frame_bytes,
                "max_snapshot_chunk_bytes",
            ),
            (
                self.max_snapshot_chunk_bytes <= self.max_checkpoint_builder_bytes,
                "max_checkpoint_builder_bytes",
            ),
            (
                self.max_log_bytes_before_snapshot < self.max_raft_log_bytes,
                "max_log_bytes_before_snapshot",
            ),
            (
                self.max_snapshot_bytes <= self.max_snapshot_staging_bytes,
                "max_snapshot_staging_bytes",
            ),
            (
                self.max_host_input_bytes <= self.max_host_total_input_bytes,
                "max_host_total_input_bytes",
            ),
            (
                self.max_host_output_bytes <= self.max_host_total_output_bytes,
                "max_host_total_output_bytes",
            ),
            (
                self.max_host_queued_requests <= self.max_host_total_queued_requests,
                "max_host_total_queued_requests",
            ),
            (
                u64::try_from(self.max_threads)
                    .unwrap_or(u64::MAX)
                    .checked_mul(u64::try_from(self.thread_stack_bytes).unwrap_or(u64::MAX))
                    .is_some_and(|total| total <= self.max_host_thread_stack_bytes),
                "max_host_thread_stack_bytes",
            ),
            (
                self.max_data_dir_bytes
                    >= self
                        .max_referenced_sst_bytes
                        .saturating_mul(2)
                        .saturating_add(self.max_raft_log_bytes)
                        .saturating_add(self.max_store_wal_bytes)
                        .saturating_add(self.max_snapshot_staging_bytes),
                "max_data_dir_bytes",
            ),
        ];
        if let Some((_, field)) = relationships.iter().find(|(valid, _)| !valid) {
            return Err(LimitError { field });
        }
        Ok(())
    }
}

/// Canonical, cluster-wide limits.  These are deliberately values rather than
/// host configuration: changing one can change the answer a committed command
/// produces, so peers must agree on the exact encoded value before serving.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterPolicy {
    pub max_members: u32,
    pub max_key_bytes: u64,
    pub max_value_bytes: u64,
    pub max_live_logical_bytes: u64,
    pub max_command_bytes: u64,
    pub max_reply_bytes: u64,
    pub max_scan_items: u32,
    pub max_live_deadlines: u64,
    pub max_sessions: u64,
    pub max_session_bytes: u64,
    pub max_session_tombstones: u64,
    pub session_idle_ns: u64,
    pub session_retry_grace_ns: u64,
    pub leader_transfer_timeout_ns: u64,
    pub max_keys_per_expiry_sweep: u32,
    pub max_batch_commands: u32,
    pub max_batch_bytes: u64,
    pub max_batch_reply_bytes: u64,
}

pub const CLUSTER_POLICY_MAGIC: u32 = u32::from_le_bytes(*b"CCPL");
pub const CLUSTER_POLICY_VERSION: u16 = 1;
pub const MAX_POLICY_MEMBERS: u32 = 4_096;
pub const MAX_POLICY_LIVE_LOGICAL_BYTES: u64 = 1 << 40;
pub const MAX_POLICY_LIVE_DEADLINES: u64 = 10_000_000;
pub const MAX_POLICY_SESSIONS: u64 = 1_000_000;
pub const MAX_POLICY_SESSION_BYTES: u64 = 1 << 30;
pub const MAX_POLICY_SESSION_TOMBSTONES: u64 = 1_000_000;
pub const MAX_POLICY_DURATION_NS: u64 = 365 * 24 * 60 * 60 * 1_000_000_000;
pub const MAX_POLICY_EXPIRY_SWEEP: u32 = 1_000_000;

impl Default for ClusterPolicy {
    fn default() -> Self {
        Self {
            max_members: 64,
            max_key_bytes: 4 * 1024,
            max_value_bytes: 1024 * 1024,
            max_live_logical_bytes: 1024 * 1024 * 1024,
            max_command_bytes: 2 * 1024 * 1024,
            max_reply_bytes: 2 * 1024 * 1024,
            max_scan_items: 4_096,
            max_live_deadlines: 1_000_000,
            max_sessions: 100_000,
            max_session_bytes: 64 * 1024 * 1024,
            max_session_tombstones: 100_000,
            session_idle_ns: 30 * 60 * 1_000_000_000,
            session_retry_grace_ns: 5 * 60 * 1_000_000_000,
            leader_transfer_timeout_ns: 15 * 1_000_000_000,
            max_keys_per_expiry_sweep: 1_024,
            max_batch_commands: 256,
            max_batch_bytes: 2 * 1024 * 1024,
            max_batch_reply_bytes: 2 * 1024 * 1024,
        }
    }
}

impl ClusterPolicy {
    /// Encodes the exact little-endian CCPL v1 record, including its CRC.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(160);
        enc.header(CLUSTER_POLICY_MAGIC, CLUSTER_POLICY_VERSION);
        enc.u32(self.max_members);
        enc.u64(self.max_key_bytes);
        enc.u64(self.max_value_bytes);
        enc.u64(self.max_live_logical_bytes);
        enc.u64(self.max_command_bytes);
        enc.u64(self.max_reply_bytes);
        enc.u32(self.max_scan_items);
        enc.u64(self.max_live_deadlines);
        enc.u64(self.max_sessions);
        enc.u64(self.max_session_bytes);
        enc.u64(self.max_session_tombstones);
        enc.u64(self.session_idle_ns);
        enc.u64(self.session_retry_grace_ns);
        enc.u64(self.leader_transfer_timeout_ns);
        enc.u32(self.max_keys_per_expiry_sweep);
        enc.u32(self.max_batch_commands);
        enc.u64(self.max_batch_bytes);
        enc.u64(self.max_batch_reply_bytes);
        let mut bytes = enc.finish();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c_zeroed_tail(&bytes);
        let checksum_start = bytes.len() - 4;
        bytes[checksum_start..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        const RECORD_LEN: usize = 138;
        if bytes.len() != RECORD_LEN {
            return Err(if bytes.len() < RECORD_LEN {
                DecodeError::UnexpectedEof {
                    offset: bytes.len(),
                    needed: RECORD_LEN.saturating_sub(bytes.len()),
                }
            } else {
                DecodeError::TrailingBytes { offset: RECORD_LEN }
            });
        }
        let expected = u32::from_le_bytes(bytes[RECORD_LEN - 4..].try_into().expect("CRC"));
        if crc32c_zeroed_tail(bytes) != expected {
            return Err(DecodeError::InvalidTag {
                offset: RECORD_LEN - 4,
                tag: 0,
            });
        }
        let mut dec = Dec::new(&bytes[..RECORD_LEN - 4]);
        dec.header(CLUSTER_POLICY_MAGIC, CLUSTER_POLICY_VERSION)?;
        let policy = Self {
            max_members: dec.u32()?,
            max_key_bytes: dec.u64()?,
            max_value_bytes: dec.u64()?,
            max_live_logical_bytes: dec.u64()?,
            max_command_bytes: dec.u64()?,
            max_reply_bytes: dec.u64()?,
            max_scan_items: dec.u32()?,
            max_live_deadlines: dec.u64()?,
            max_sessions: dec.u64()?,
            max_session_bytes: dec.u64()?,
            max_session_tombstones: dec.u64()?,
            session_idle_ns: dec.u64()?,
            session_retry_grace_ns: dec.u64()?,
            leader_transfer_timeout_ns: dec.u64()?,
            max_keys_per_expiry_sweep: dec.u32()?,
            max_batch_commands: dec.u32()?,
            max_batch_bytes: dec.u64()?,
            max_batch_reply_bytes: dec.u64()?,
        };
        dec.finish()?;
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(self) -> Result<(), DecodeError> {
        let nonzero = self.max_members != 0
            && self.max_key_bytes != 0
            && self.max_value_bytes != 0
            && self.max_live_logical_bytes != 0
            && self.max_command_bytes != 0
            && self.max_reply_bytes != 0
            && self.max_scan_items != 0
            && self.max_live_deadlines != 0
            && self.max_sessions != 0
            && self.max_session_bytes != 0
            && self.max_session_tombstones != 0
            && self.session_idle_ns != 0
            && self.session_retry_grace_ns != 0
            && self.leader_transfer_timeout_ns != 0
            && self.max_keys_per_expiry_sweep != 0
            && self.max_batch_commands != 0
            && self.max_batch_bytes != 0
            && self.max_batch_reply_bytes != 0;
        let coherent = self.max_batch_bytes <= self.max_command_bytes
            && self.max_batch_reply_bytes <= self.max_reply_bytes
            && self.max_key_bytes <= self.max_command_bytes
            && self.max_value_bytes <= self.max_command_bytes
            && self.max_session_bytes <= self.max_live_logical_bytes;
        let hard_bounded = self.max_members <= MAX_POLICY_MEMBERS
            && self.max_key_bytes <= MAX_CODEC_BYTES as u64
            && self.max_value_bytes <= MAX_CODEC_BYTES as u64
            && self.max_command_bytes <= MAX_CODEC_BYTES as u64
            && self.max_reply_bytes <= MAX_CODEC_BYTES as u64
            && self.max_batch_bytes <= MAX_CODEC_BYTES as u64
            && self.max_batch_reply_bytes <= MAX_CODEC_BYTES as u64
            && self.max_live_logical_bytes <= MAX_POLICY_LIVE_LOGICAL_BYTES
            && self.max_scan_items <= 1_000_000
            && self.max_live_deadlines <= MAX_POLICY_LIVE_DEADLINES
            && self.max_sessions <= MAX_POLICY_SESSIONS
            && self.max_session_bytes <= MAX_POLICY_SESSION_BYTES
            && self.max_session_tombstones <= MAX_POLICY_SESSION_TOMBSTONES
            && self.session_idle_ns <= MAX_POLICY_DURATION_NS
            && self.session_retry_grace_ns <= MAX_POLICY_DURATION_NS
            && self.leader_transfer_timeout_ns <= MAX_POLICY_DURATION_NS
            && self.max_keys_per_expiry_sweep <= MAX_POLICY_EXPIRY_SWEEP
            && self.max_batch_commands <= 65_536;
        if nonzero && coherent && hard_bounded {
            Ok(())
        } else {
            Err(DecodeError::InvalidTag { offset: 0, tag: 0 })
        }
    }

    #[must_use]
    pub fn hash(self) -> u64 {
        fnv1a(&self.encode())
    }
}

/// A canonical peer endpoint.  We store address bytes instead of `SocketAddr`
/// so the replicated form cannot inherit platform formatting differences.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PeerAddress {
    V4 { ip: [u8; 4], port: u16 },
    V6 { ip: [u8; 16], port: u16 },
}

impl PeerAddress {
    pub fn validate(&self) -> Result<(), DecodeError> {
        let (bytes, port) = match self {
            Self::V4 { ip, port } => (ip.as_slice(), *port),
            Self::V6 { ip, port } => (ip.as_slice(), *port),
        };
        let unspecified = bytes.iter().all(|byte| *byte == 0);
        let multicast = match self {
            Self::V4 { ip, .. } => (ip[0] & 0xf0) == 0xe0,
            Self::V6 { ip, .. } => ip[0] == 0xff,
        };
        let v4_mapped_v6 = matches!(self, Self::V6 { ip, .. } if ip[..10].iter().all(|byte| *byte == 0) && ip[10] == 0xff && ip[11] == 0xff);
        if port == 0 || unspecified || multicast || v4_mapped_v6 {
            Err(DecodeError::InvalidTag { offset: 0, tag: 0 })
        } else {
            Ok(())
        }
    }

    fn encode(&self, enc: &mut Enc) {
        match self {
            Self::V4 { ip, port } => {
                enc.u8(1);
                for byte in ip {
                    enc.u8(*byte);
                }
                enc.u16(*port);
            }
            Self::V6 { ip, port } => {
                enc.u8(2);
                for byte in ip {
                    enc.u8(*byte);
                }
                enc.u16(*port);
            }
        }
    }

    fn decode(dec: &mut Dec<'_>) -> Result<Self, DecodeError> {
        let address = match dec.u8()? {
            1 => Self::V4 {
                ip: [dec.u8()?, dec.u8()?, dec.u8()?, dec.u8()?],
                port: dec.u16()?,
            },
            2 => Self::V6 {
                ip: [
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                    dec.u8()?,
                ],
                port: dec.u16()?,
            },
            tag => {
                return Err(DecodeError::InvalidTag {
                    offset: dec.position().saturating_sub(1),
                    tag,
                });
            }
        };
        address.validate()?;
        Ok(address)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JointMembership {
    pub old_voters: BTreeSet<NodeId>,
    pub new_voters: BTreeSet<NodeId>,
    pub enter_index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipState {
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub joint: Option<JointMembership>,
    pub addresses: BTreeMap<NodeId, PeerAddress>,
    /// Monotonic, replicated semantic capabilities. Feature activation is
    /// intentionally membership state so CCSN/genesis recovery cannot turn a
    /// committed semantic fence back into a local preference.
    pub active_features: u64,
}

pub const MEMBERSHIP_MAGIC: u32 = u32::from_le_bytes(*b"CCMS");
pub const MEMBERSHIP_FORMAT_VERSION: u16 = 2;
pub const ATOMIC_BATCH_FEATURE: u64 = 1 << 1;

impl MembershipState {
    pub fn new(voters: BTreeSet<NodeId>) -> Result<Self, DecodeError> {
        let state = Self {
            voters,
            learners: BTreeSet::new(),
            joint: None,
            addresses: BTreeMap::new(),
            active_features: 0,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.voters.is_empty()
            || self.voters.iter().any(|id| id.get() == 0)
            || self.learners.iter().any(|id| id.get() == 0)
            || !self.voters.is_disjoint(&self.learners)
            || self.voters.len() + self.learners.len() > 4_096
            || self.active_features & !ATOMIC_BATCH_FEATURE != 0
        {
            return Err(DecodeError::InvalidTag { offset: 0, tag: 0 });
        }
        for (id, address) in &self.addresses {
            if id.get() == 0 || (!self.voters.contains(id) && !self.learners.contains(id)) {
                return Err(DecodeError::InvalidTag { offset: 0, tag: 0 });
            }
            address.validate()?;
        }
        if let Some(joint) = &self.joint
            && (joint.old_voters.is_empty()
                || joint.new_voters.is_empty()
                || joint.enter_index.get() == 0
                || !joint.old_voters.is_subset(&self.voters)
                || !joint.new_voters.is_subset(&self.voters))
        {
            return Err(DecodeError::InvalidTag { offset: 0, tag: 0 });
        }
        Ok(())
    }

    /// A complete, versioned membership image for snapshots and durable
    /// genesis.  The append projection may still contain uncommitted config
    /// entries; this value is the committed base from which that suffix is
    /// replayed.
    pub fn encode(&self) -> Result<Vec<u8>, DecodeError> {
        self.validate()?;
        let mut enc = Enc::new();
        enc.header(MEMBERSHIP_MAGIC, MEMBERSHIP_FORMAT_VERSION);
        encode_member_set(&mut enc, &self.voters);
        encode_member_set(&mut enc, &self.learners);
        match &self.joint {
            None => enc.u8(0),
            Some(joint) => {
                enc.u8(1);
                encode_member_set(&mut enc, &joint.old_voters);
                encode_member_set(&mut enc, &joint.new_voters);
                enc.u64(joint.enter_index.get());
            }
        }
        enc.u32(
            u32::try_from(self.addresses.len()).map_err(|_| DecodeError::LengthTooLarge {
                offset: 0,
                length: u32::MAX,
                max: MAX_POLICY_MEMBERS as usize,
            })?,
        );
        for (id, address) in &self.addresses {
            enc.u64(id.get());
            address.encode(&mut enc);
        }
        enc.u64(self.active_features);
        let mut bytes = enc.finish();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c_zeroed_tail(&bytes);
        let checksum_start = bytes.len() - 4;
        bytes[checksum_start..].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 4 {
            return Err(DecodeError::UnexpectedEof {
                offset: bytes.len(),
                needed: 4 - bytes.len(),
            });
        }
        let body_len = bytes.len() - 4;
        let expected = u32::from_le_bytes(bytes[body_len..].try_into().expect("membership CRC"));
        if crc32c_zeroed_tail(bytes) != expected {
            return Err(DecodeError::InvalidTag {
                offset: body_len,
                tag: 0,
            });
        }
        let mut dec = Dec::new(&bytes[..body_len]);
        let magic = dec.u32()?;
        let version = dec.u16()?;
        if magic != MEMBERSHIP_MAGIC {
            return Err(DecodeError::InvalidMagic {
                expected: MEMBERSHIP_MAGIC,
                actual: magic,
            });
        }
        if !matches!(version, 1 | MEMBERSHIP_FORMAT_VERSION) {
            return Err(DecodeError::InvalidVersion {
                expected: MEMBERSHIP_FORMAT_VERSION,
                actual: version,
            });
        }
        let voters = decode_member_set(&mut dec, false)?;
        let learners = decode_member_set(&mut dec, true)?;
        let joint = match dec.u8()? {
            0 => None,
            1 => Some(JointMembership {
                old_voters: decode_member_set(&mut dec, false)?,
                new_voters: decode_member_set(&mut dec, false)?,
                enter_index: decode_nonzero_index(&mut dec)?,
            }),
            tag => {
                return Err(DecodeError::InvalidTag {
                    offset: dec.position().saturating_sub(1),
                    tag,
                });
            }
        };
        let address_count = dec.u32()?;
        if address_count > MAX_POLICY_MEMBERS {
            return Err(DecodeError::LengthTooLarge {
                offset: dec.position().saturating_sub(4),
                length: address_count,
                max: MAX_POLICY_MEMBERS as usize,
            });
        }
        let mut addresses = BTreeMap::new();
        for _ in 0..address_count {
            let id = decode_nonzero_id(&mut dec)?;
            let address = PeerAddress::decode(&mut dec)?;
            if addresses.insert(id, address).is_some() {
                return Err(DecodeError::InvalidTag {
                    offset: dec.position().saturating_sub(1),
                    tag: 0,
                });
            }
        }
        let active_features = if version == 1 { 0 } else { dec.u64()? };
        dec.finish()?;
        let state = Self {
            voters,
            learners,
            joint,
            addresses,
            active_features,
        };
        state.validate()?;
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigOperation {
    AddLearner {
        id: NodeId,
        address: Option<PeerAddress>,
    },
    RemoveLearner {
        id: NodeId,
    },
    UpdateAddress {
        id: NodeId,
        address: PeerAddress,
    },
    EnterJoint {
        new_voters: BTreeSet<NodeId>,
    },
    LeaveJoint {
        enter_index: LogIndex,
    },
    BeginLeaderTransfer {
        target: NodeId,
    },
    FinishLeaderTransfer {
        intent_index: LogIndex,
        result: TransferResult,
    },
    ActivateFeature {
        feature: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TransferResult {
    Success = 1,
    Timeout = 2,
    Superseded = 3,
}

impl TransferResult {
    fn decode(tag: u8) -> Result<Self, DecodeError> {
        match tag {
            1 => Ok(Self::Success),
            2 => Ok(Self::Timeout),
            3 => Ok(Self::Superseded),
            _ => Err(DecodeError::InvalidTag { offset: 0, tag }),
        }
    }
}

impl ConfigOperation {
    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::AddLearner { .. } => 1,
            Self::RemoveLearner { .. } => 2,
            Self::UpdateAddress { .. } => 3,
            Self::EnterJoint { .. } => 4,
            Self::LeaveJoint { .. } => 5,
            Self::BeginLeaderTransfer { .. } => 6,
            Self::FinishLeaderTransfer { .. } => 7,
            Self::ActivateFeature { .. } => 8,
        }
    }

    #[must_use]
    pub fn encode_body(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        match self {
            Self::AddLearner { id, address } => {
                enc.u64(id.get());
                encode_optional_address(&mut enc, address);
            }
            Self::RemoveLearner { id } | Self::BeginLeaderTransfer { target: id } => {
                enc.u64(id.get())
            }
            Self::UpdateAddress { id, address } => {
                enc.u64(id.get());
                address.encode(&mut enc);
            }
            Self::EnterJoint { new_voters } => encode_members(&mut enc, new_voters),
            Self::LeaveJoint { enter_index } => enc.u64(enter_index.get()),
            Self::FinishLeaderTransfer {
                intent_index,
                result,
            } => {
                enc.u64(intent_index.get());
                enc.u8(*result as u8);
            }
            Self::ActivateFeature { feature } => enc.u64(*feature),
        }
        enc.finish()
    }

    pub fn decode(tag: u8, body: &[u8]) -> Result<Self, DecodeError> {
        let mut dec = Dec::new(body);
        let operation = match tag {
            1 => Self::AddLearner {
                id: decode_nonzero_id(&mut dec)?,
                address: decode_optional_address(&mut dec)?,
            },
            2 => Self::RemoveLearner {
                id: decode_nonzero_id(&mut dec)?,
            },
            3 => Self::UpdateAddress {
                id: decode_nonzero_id(&mut dec)?,
                address: PeerAddress::decode(&mut dec)?,
            },
            4 => Self::EnterJoint {
                new_voters: decode_members(&mut dec)?,
            },
            5 => Self::LeaveJoint {
                enter_index: decode_nonzero_index(&mut dec)?,
            },
            6 => Self::BeginLeaderTransfer {
                target: decode_nonzero_id(&mut dec)?,
            },
            7 => Self::FinishLeaderTransfer {
                intent_index: decode_nonzero_index(&mut dec)?,
                result: TransferResult::decode(dec.u8()?)?,
            },
            8 => {
                let feature = dec.u64()?;
                if feature != ATOMIC_BATCH_FEATURE {
                    return Err(DecodeError::InvalidTag {
                        offset: dec.position().saturating_sub(8),
                        tag: 8,
                    });
                }
                Self::ActivateFeature { feature }
            }
            _ => return Err(DecodeError::InvalidTag { offset: 0, tag }),
        };
        dec.finish()?;
        Ok(operation)
    }
}

pub const CONFIG_ENVELOPE_MAGIC: u32 = u32::from_le_bytes(*b"CCCF");
pub const ADMIN_REPLY_MAGIC: u32 = u32::from_le_bytes(*b"CCAR");
pub const CONFIG_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionNamespace {
    UserRequest = 0,
    AdminRequest = 1,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionKey {
    pub namespace: u8,
    pub client: ClientId,
}

impl SessionKey {
    pub fn new(namespace: u8, client: ClientId) -> Result<Self, DecodeError> {
        if namespace > SessionNamespace::AdminRequest as u8 || client.get() == 0 {
            return Err(DecodeError::InvalidTag {
                offset: 0,
                tag: namespace,
            });
        }
        Ok(Self { namespace, client })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigEnvelope {
    pub admin_session: Option<(SessionKey, u64)>,
    pub leader_time: Time,
    pub operation: ConfigOperation,
}

impl ConfigEnvelope {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.header(CONFIG_ENVELOPE_MAGIC, CONFIG_FORMAT_VERSION);
        match self.admin_session {
            Some((key, sequence)) => {
                enc.u8(1);
                enc.u8(key.namespace);
                enc.u64(key.client.get());
                enc.u64(sequence);
            }
            None => {
                enc.u8(0);
                enc.u8(0);
                enc.u64(0);
                enc.u64(0);
            }
        }
        enc.u64(self.leader_time.as_nanos());
        enc.u8(self.operation.tag());
        enc.bytes(&self.operation.encode_body());
        let mut bytes = enc.finish();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c_zeroed_tail(&bytes);
        let checksum_start = bytes.len() - 4;
        bytes[checksum_start..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 4 {
            return Err(DecodeError::UnexpectedEof {
                offset: bytes.len(),
                needed: 4 - bytes.len(),
            });
        }
        let body_len = bytes.len() - 4;
        let crc = u32::from_le_bytes(bytes[body_len..].try_into().expect("CRC"));
        if crc32c_zeroed_tail(bytes) != crc {
            return Err(DecodeError::InvalidTag {
                offset: body_len,
                tag: 0,
            });
        }
        let mut dec = Dec::new(&bytes[..body_len]);
        dec.header(CONFIG_ENVELOPE_MAGIC, CONFIG_FORMAT_VERSION)?;
        let has_session = dec.u8()?;
        let namespace = dec.u8()?;
        let client = ClientId::new(dec.u64()?);
        let sequence = dec.u64()?;
        let admin_session = match has_session {
            0 if namespace == 0 && client.get() == 0 && sequence == 0 => None,
            1 if namespace == SessionNamespace::AdminRequest as u8 && sequence != 0 => {
                Some((SessionKey::new(namespace, client)?, sequence))
            }
            _ => {
                return Err(DecodeError::InvalidTag {
                    offset: 6,
                    tag: has_session,
                });
            }
        };
        let leader_time = Time::from_nanos(dec.u64()?);
        let tag = dec.u8()?;
        let operation = ConfigOperation::decode(tag, &dec.bytes()?)?;
        dec.finish()?;
        Ok(Self {
            admin_session,
            leader_time,
            operation,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AdminResultTag {
    Applied = 1,
    TransferSuccess = 2,
    TransferTimeout = 3,
    TransferSuperseded = 4,
    InProgress = 5,
    RequestConflict = 6,
    RequestExpired = 7,
    Rejected = 8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminReply {
    pub operation_tag: u8,
    pub result: AdminResultTag,
    pub source_index: LogIndex,
    pub detail: Bytes,
}

impl AdminReply {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        enc.header(ADMIN_REPLY_MAGIC, CONFIG_FORMAT_VERSION);
        enc.u8(self.operation_tag);
        enc.u8(self.result as u8);
        enc.u64(self.source_index.get());
        enc.bytes(&self.detail);
        let mut bytes = enc.finish();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c_zeroed_tail(&bytes);
        let checksum_start = bytes.len() - 4;
        bytes[checksum_start..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 4 {
            return Err(DecodeError::UnexpectedEof {
                offset: bytes.len(),
                needed: 4 - bytes.len(),
            });
        }
        let body_len = bytes.len() - 4;
        if crc32c_zeroed_tail(bytes)
            != u32::from_le_bytes(bytes[body_len..].try_into().expect("CRC"))
        {
            return Err(DecodeError::InvalidTag {
                offset: body_len,
                tag: 0,
            });
        }
        let mut dec = Dec::new(&bytes[..body_len]);
        dec.header(ADMIN_REPLY_MAGIC, CONFIG_FORMAT_VERSION)?;
        let operation_tag = dec.u8()?;
        if !(1..=8).contains(&operation_tag) {
            return Err(DecodeError::InvalidTag {
                offset: dec.position() - 1,
                tag: operation_tag,
            });
        }
        let result_tag = dec.u8()?;
        let result = match result_tag {
            1 => AdminResultTag::Applied,
            2 => AdminResultTag::TransferSuccess,
            3 => AdminResultTag::TransferTimeout,
            4 => AdminResultTag::TransferSuperseded,
            5 => AdminResultTag::InProgress,
            6 => AdminResultTag::RequestConflict,
            7 => AdminResultTag::RequestExpired,
            8 => AdminResultTag::Rejected,
            _ => {
                return Err(DecodeError::InvalidTag {
                    offset: dec.position() - 1,
                    tag: result_tag,
                });
            }
        };
        let source_index = LogIndex::new(dec.u64()?);
        if matches!(
            result,
            AdminResultTag::Applied
                | AdminResultTag::TransferSuccess
                | AdminResultTag::TransferTimeout
                | AdminResultTag::TransferSuperseded
        ) && source_index.get() == 0
        {
            return Err(DecodeError::InvalidTag {
                offset: dec.position() - 8,
                tag: 0,
            });
        }
        let detail = dec.bytes()?;
        dec.finish()?;
        Ok(Self {
            operation_tag,
            result,
            source_index,
            detail,
        })
    }
}

fn decode_nonzero_id(dec: &mut Dec<'_>) -> Result<NodeId, DecodeError> {
    let id = NodeId::new(dec.u64()?);
    if id.get() == 0 {
        Err(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(8),
            tag: 0,
        })
    } else {
        Ok(id)
    }
}

fn decode_nonzero_index(dec: &mut Dec<'_>) -> Result<LogIndex, DecodeError> {
    let index = LogIndex::new(dec.u64()?);
    if index.get() == 0 {
        Err(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(8),
            tag: 0,
        })
    } else {
        Ok(index)
    }
}

fn encode_optional_address(enc: &mut Enc, address: &Option<PeerAddress>) {
    match address {
        Some(address) => {
            enc.u8(1);
            address.encode(enc);
        }
        None => enc.u8(0),
    }
}

fn decode_optional_address(dec: &mut Dec<'_>) -> Result<Option<PeerAddress>, DecodeError> {
    match dec.u8()? {
        0 => Ok(None),
        1 => Ok(Some(PeerAddress::decode(dec)?)),
        tag => Err(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        }),
    }
}

fn encode_members(enc: &mut Enc, members: &BTreeSet<NodeId>) {
    encode_member_set(enc, members);
}

fn encode_member_set(enc: &mut Enc, members: &BTreeSet<NodeId>) {
    enc.u32(u32::try_from(members.len()).expect("member count fits"));
    for member in members {
        enc.u64(member.get());
    }
}

fn decode_members(dec: &mut Dec<'_>) -> Result<BTreeSet<NodeId>, DecodeError> {
    decode_member_set(dec, false)
}

fn decode_member_set(
    dec: &mut Dec<'_>,
    allow_empty: bool,
) -> Result<BTreeSet<NodeId>, DecodeError> {
    let count = dec.u32()?;
    if (!allow_empty && count == 0) || count > MAX_POLICY_MEMBERS {
        return Err(DecodeError::LengthTooLarge {
            offset: dec.position().saturating_sub(4),
            length: count,
            max: MAX_POLICY_MEMBERS as usize,
        });
    }
    let mut members = BTreeSet::new();
    for _ in 0..count {
        if !members.insert(decode_nonzero_id(dec)?) {
            return Err(DecodeError::InvalidTag {
                offset: dec.position().saturating_sub(8),
                tag: 0,
            });
        }
    }
    Ok(members)
}

/// Error returned by total, bounds-checked decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    UnexpectedEof {
        offset: usize,
        needed: usize,
    },
    LengthTooLarge {
        offset: usize,
        length: u32,
        max: usize,
    },
    InvalidMagic {
        expected: u32,
        actual: u32,
    },
    InvalidVersion {
        expected: u16,
        actual: u16,
    },
    InvalidTag {
        offset: usize,
        tag: u8,
    },
    TrailingBytes {
        offset: usize,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { offset, needed } => {
                write!(f, "decode EOF at {offset}, need {needed} bytes")
            }
            Self::LengthTooLarge {
                offset,
                length,
                max,
            } => {
                write!(f, "decode length {length} at {offset} exceeds {max}")
            }
            Self::InvalidMagic { expected, actual } => {
                write!(f, "invalid magic {actual:#x}, expected {expected:#x}")
            }
            Self::InvalidVersion { expected, actual } => {
                write!(f, "invalid version {actual}, expected {expected}")
            }
            Self::InvalidTag { offset, tag } => write!(f, "invalid tag {tag} at {offset}"),
            Self::TrailingBytes { offset } => write!(f, "trailing bytes at {offset}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Enc {
    bytes: Vec<u8>,
}

impl Enc {
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    pub fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn bytes(&mut self, value: &[u8]) {
        assert!(
            value.len() <= MAX_CODEC_BYTES,
            "invariant: encoded value cap"
        );
        self.u32(u32::try_from(value.len()).expect("invariant: codec length fits u32"));
        self.bytes.extend_from_slice(value);
    }

    pub fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    pub fn header(&mut self, magic: u32, version: u16) {
        self.u32(magic);
        self.u16(version);
    }

    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

pub struct Dec<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Dec<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(DecodeError::UnexpectedEof {
                offset: self.offset,
                needed: length,
            })?;
        if end > self.bytes.len() {
            return Err(DecodeError::UnexpectedEof {
                offset: self.offset,
                needed: length,
            });
        }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("invariant: length is 2"),
        ))
    }

    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("invariant: length is 4"),
        ))
    }

    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("invariant: length is 8"),
        ))
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let length = self.u32()?;
        if usize::try_from(length).unwrap_or(usize::MAX) > MAX_CODEC_BYTES {
            return Err(DecodeError::LengthTooLarge {
                offset: self.offset.saturating_sub(4),
                length,
                max: MAX_CODEC_BYTES,
            });
        }
        Ok(self.take(length as usize)?.to_vec())
    }

    pub fn string(&mut self) -> Result<String, DecodeError> {
        String::from_utf8(self.bytes()?).map_err(|_| DecodeError::InvalidTag {
            offset: self.offset,
            tag: 0xff,
        })
    }

    pub fn header(
        &mut self,
        expected_magic: u32,
        expected_version: u16,
    ) -> Result<(), DecodeError> {
        let actual_magic = self.u32()?;
        if actual_magic != expected_magic {
            return Err(DecodeError::InvalidMagic {
                expected: expected_magic,
                actual: actual_magic,
            });
        }
        let actual_version = self.u16()?;
        if actual_version != expected_version {
            return Err(DecodeError::InvalidVersion {
                expected: expected_version,
                actual: actual_version,
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn position(&self) -> usize {
        self.offset
    }

    /// Bytes still available to this bounded decoder.  Collection codecs use
    /// this for a cheap minimum-layout preflight before allocating from an
    /// attacker-controlled count field.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    pub fn finish(self) -> Result<(), DecodeError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes {
                offset: self.offset,
            })
        }
    }
}

/// CRC-32C (Castagnoli), kept dependency-free for the core vocabulary crate.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut state = Crc32c::new();
    state.update(bytes);
    state.finish()
}

/// Incremental CRC-32C (Castagnoli) state.  Streaming persisted formats use
/// this instead of retaining an entire file solely to calculate its checksum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Crc32c {
    state: u32,
}

impl Crc32c {
    #[must_use]
    pub const fn new() -> Self {
        Self { state: u32::MAX }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u32::from(*byte);
            for _ in 0..8 {
                let mask = 0u32.wrapping_sub(self.state & 1);
                self.state = (self.state >> 1) ^ (0x82f6_3b78 & mask);
            }
        }
    }

    #[must_use]
    pub const fn finish(self) -> u32 {
        !self.state
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

/// CRC-32C over a record whose final four-byte checksum field is treated as
/// zero.  Keeping this operation allocation-free makes it safe to use while
/// rejecting a bounded but malformed wire record.
#[must_use]
pub fn crc32c_zeroed_tail(bytes: &[u8]) -> u32 {
    crc32c_with_zeroed_tail(bytes, true)
}

fn crc32c_with_zeroed_tail(bytes: &[u8], zero_tail: bool) -> u32 {
    let mut crc = Crc32c::new();
    let zero_from = bytes.len().saturating_sub(4);
    for (index, original) in bytes.iter().enumerate() {
        let byte = if zero_tail && index >= zero_from {
            0
        } else {
            *original
        };
        crc.update(&[byte]);
    }
    crc.finish()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum EventKind {
    NetSend = 1,
    NetRecv = 2,
    NetDrop = 3,
    IoIssue = 4,
    IoDone = 5,
    IoLost = 6,
    TimerSet = 7,
    TimerFire = 8,
    RoleChange = 9,
    VoteReq = 10,
    VoteGrant = 11,
    VoteDeny = 12,
    AppendSent = 13,
    AppendAck = 14,
    Commit = 15,
    Apply = 16,
    SnapshotStart = 17,
    SnapshotChunk = 18,
    SnapshotInstall = 19,
    ConfChange = 20,
    ClientInvoke = 21,
    ClientOk = 22,
    ClientFail = 23,
    ClientTimeout = 24,
    WalRecover = 25,
    Flush = 26,
    Compact = 27,
    Fault = 28,
    CheckerNote = 29,
    SyntheticKataEnabled = 30,
}

impl EventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NetSend => "NetSend",
            Self::NetRecv => "NetRecv",
            Self::NetDrop => "NetDrop",
            Self::IoIssue => "IoIssue",
            Self::IoDone => "IoDone",
            Self::IoLost => "IoLost",
            Self::TimerSet => "TimerSet",
            Self::TimerFire => "TimerFire",
            Self::RoleChange => "RoleChange",
            Self::VoteReq => "VoteReq",
            Self::VoteGrant => "VoteGrant",
            Self::VoteDeny => "VoteDeny",
            Self::AppendSent => "AppendSent",
            Self::AppendAck => "AppendAck",
            Self::Commit => "Commit",
            Self::Apply => "Apply",
            Self::SnapshotStart => "SnapshotStart",
            Self::SnapshotChunk => "SnapshotChunk",
            Self::SnapshotInstall => "SnapshotInstall",
            Self::ConfChange => "ConfChange",
            Self::ClientInvoke => "ClientInvoke",
            Self::ClientOk => "ClientOk",
            Self::ClientFail => "ClientFail",
            Self::ClientTimeout => "ClientTimeout",
            Self::WalRecover => "WalRecover",
            Self::Flush => "Flush",
            Self::Compact => "Compact",
            Self::Fault => "Fault",
            Self::CheckerNote => "CheckerNote",
            Self::SyntheticKataEnabled => "SyntheticKataEnabled",
        }
    }

    /// Resolve a stable CCTR registry code. Readers that provide diagnostic
    /// forward compatibility may retain an unknown code rather than treating
    /// it as an event payload schema they understand.
    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::NetSend,
            2 => Self::NetRecv,
            3 => Self::NetDrop,
            4 => Self::IoIssue,
            5 => Self::IoDone,
            6 => Self::IoLost,
            7 => Self::TimerSet,
            8 => Self::TimerFire,
            9 => Self::RoleChange,
            10 => Self::VoteReq,
            11 => Self::VoteGrant,
            12 => Self::VoteDeny,
            13 => Self::AppendSent,
            14 => Self::AppendAck,
            15 => Self::Commit,
            16 => Self::Apply,
            17 => Self::SnapshotStart,
            18 => Self::SnapshotChunk,
            19 => Self::SnapshotInstall,
            20 => Self::ConfChange,
            21 => Self::ClientInvoke,
            22 => Self::ClientOk,
            23 => Self::ClientFail,
            24 => Self::ClientTimeout,
            25 => Self::WalRecover,
            26 => Self::Flush,
            27 => Self::Compact,
            28 => Self::Fault,
            29 => Self::CheckerNote,
            30 => Self::SyntheticKataEnabled,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub time: Time,
    pub node: Option<NodeId>,
    pub kind: EventKind,
    pub payload: Bytes,
}

impl Event {
    #[must_use]
    pub fn new(
        seq: u64,
        time: Time,
        node: Option<NodeId>,
        kind: EventKind,
        payload: Bytes,
    ) -> Self {
        Self {
            seq,
            time,
            node,
            kind,
            payload,
        }
    }

    fn encode(&self, enc: &mut Enc) {
        enc.u64(self.seq);
        enc.u64(self.time.as_nanos());
        match self.node {
            Some(node) => {
                enc.u8(1);
                enc.u64(node.0);
            }
            None => enc.u8(0),
        }
        enc.u8(self.kind as u8);
        enc.bytes(&self.payload);
    }

    fn decode(dec: &mut Dec<'_>) -> Result<Self, DecodeError> {
        let seq = dec.u64()?;
        let time = Time::from_nanos(dec.u64()?);
        let node = match dec.u8()? {
            0 => None,
            1 => Some(NodeId::new(dec.u64()?)),
            tag => {
                return Err(DecodeError::InvalidTag {
                    offset: dec.position(),
                    tag,
                });
            }
        };
        let tag = dec.u8()?;
        let kind = EventKind::from_code(tag).ok_or(DecodeError::InvalidTag {
            offset: dec.position().saturating_sub(1),
            tag,
        })?;
        let payload = dec.bytes()?;
        Ok(Self {
            seq,
            time,
            node,
            kind,
            payload,
        })
    }

    /// Canonical standalone encoding for value-boundary diagnostic codecs.
    #[must_use]
    pub fn encode_value(&self) -> Vec<u8> {
        let mut enc = Enc::new();
        self.encode(&mut enc);
        enc.finish()
    }

    /// Decode one exact standalone diagnostic event value.
    pub fn decode_value(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut dec = Dec::new(bytes);
        let event = Self::decode(&mut dec)?;
        dec.finish()?;
        Ok(event)
    }
}

pub const TRACE_MAGIC: u32 = u32::from_le_bytes(*b"CCTR");
pub const TRACE_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trace {
    pub seed: Seed,
    pub config_hash: u32,
    pub build: String,
    pub events: Vec<Event>,
}

impl Trace {
    #[must_use]
    pub fn new(seed: Seed, config_hash: u32) -> Self {
        Self {
            seed,
            config_hash,
            build: String::from("local"),
            events: Vec::new(),
        }
    }

    pub fn push(&mut self, time: Time, node: Option<NodeId>, kind: EventKind, payload: Bytes) {
        let seq = self.events.len() as u64;
        self.events.push(Event::new(seq, time, node, kind, payload));
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = Enc::with_capacity(32 + self.events.len() * 24);
        enc.header(TRACE_MAGIC, TRACE_VERSION);
        enc.u64(self.seed.0);
        enc.u32(self.config_hash);
        enc.string(&self.build);
        enc.u32(u32::try_from(self.events.len()).expect("invariant: trace event count fits u32"));
        for event in &self.events {
            event.encode(&mut enc);
        }
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut dec = Dec::new(bytes);
        dec.header(TRACE_MAGIC, TRACE_VERSION)?;
        let seed = Seed::new(dec.u64()?);
        let config_hash = dec.u32()?;
        let build = dec.string()?;
        let count = dec.u32()?;
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        if count > MAX_CODEC_BYTES {
            return Err(DecodeError::LengthTooLarge {
                offset: dec.position().saturating_sub(4),
                length: count as u32,
                max: MAX_CODEC_BYTES,
            });
        }
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(Event::decode(&mut dec)?);
        }
        dec.finish()?;
        Ok(Self {
            seed,
            config_hash,
            build,
            events,
        })
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let mut json = format!(
            "{{\"trace_version\":{},\"seed\":\"{}\",\"config_hash\":{},\"build\":\"{}\",\"events\":[",
            TRACE_VERSION,
            self.seed,
            self.config_hash,
            json_escape(&self.build)
        );
        for (index, event) in self.events.iter().enumerate() {
            if index != 0 {
                json.push(',');
            }
            let node = event
                .node
                .map_or_else(|| String::from("null"), |value| value.0.to_string());
            json.push_str(&format!(
                "{{\"seq\":{},\"time_ns\":{},\"node\":{},\"kind\":\"{}\",\"payload_hex\":\"{}\"}}",
                event.seq,
                event.time.as_nanos(),
                node,
                event.kind.as_str(),
                hex(&event.payload)
            ));
        }
        json.push_str("]}");
        json
    }
}

fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '"' => ['\\', '"'].into_iter().collect::<Vec<_>>(),
            '\\' => ['\\', '\\'].into_iter().collect::<Vec<_>>(),
            '\n' => ['\\', 'n'].into_iter().collect::<Vec<_>>(),
            '\r' => ['\\', 'r'].into_iter().collect::<Vec<_>>(),
            '\t' => ['\\', 't'].into_iter().collect::<Vec<_>>(),
            other => [other].into_iter().collect::<Vec<_>>(),
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        result.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_published_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn ids_have_stable_display() {
        assert_eq!(NodeId::new(3).to_string(), "n3");
        assert_eq!(Term::new(7).to_string(), "t7");
        assert_eq!(LogIndex::new(412).to_string(), "i412");
        assert_eq!(Seed::new(1).to_string(), "0x0000000000000001");
    }

    #[test]
    fn trap_cluster_id_has_one_nonzero_canonical_text_form() {
        let id = ClusterId::from_hex("00112233445566778899aabbccddeeff").expect("canonical id");
        assert_eq!(id.to_hex(), "00112233445566778899aabbccddeeff");
        assert_eq!(
            id.bytes(),
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]
        );
        for invalid in [
            "00112233445566778899AABBCCDDEEFF",
            "00112233445566778899aabbccddeef",
            "00112233445566778899aabbccddeeff0",
            "00000000000000000000000000000000",
            "00112233445566778899aabbccddeefg",
        ] {
            assert!(ClusterId::from_hex(invalid).is_err(), "accepted {invalid}");
        }
    }

    #[test]
    fn stream_is_domain_separated_and_repeatable() {
        let mut left = Xoshiro256pp::stream(Seed::new(9), "node", 1);
        let mut right = Xoshiro256pp::stream(Seed::new(9), "node", 1);
        assert_eq!(left.u64(), right.u64());
        assert_ne!(
            left.u64(),
            Xoshiro256pp::stream(Seed::new(9), "disk", 1).u64()
        );
    }

    #[test]
    fn trace_round_trip_is_byte_stable() {
        let mut trace = Trace::new(Seed::new(42), 7);
        trace.push(
            Time::from_nanos(2),
            Some(NodeId::new(1)),
            EventKind::Apply,
            vec![1, 2, 3],
        );
        let bytes = trace.encode();
        assert_eq!(Trace::decode(&bytes).expect("valid trace").encode(), bytes);
    }

    #[test]
    fn decoder_rejects_oversized_length() {
        let mut enc = Enc::new();
        enc.u32(0xffff_ffff);
        let error = Dec::new(&enc.finish())
            .bytes()
            .expect_err("oversized length");
        assert!(matches!(error, DecodeError::LengthTooLarge { .. }));
    }

    #[test]
    fn malformed_trace_inputs_are_total() {
        let mut trace = Trace::new(Seed::new(17), 11);
        trace.push(
            Time::from_nanos(4),
            None,
            EventKind::CheckerNote,
            vec![9, 8, 7],
        );
        let encoded = trace.encode();
        for end in 0..encoded.len() {
            let _ = Trace::decode(&encoded[..end]);
        }
        for byte in 0..=u8::MAX {
            let input = [byte; 7];
            let _ = Trace::decode(&input);
        }
    }

    #[test]
    fn time_arithmetic_is_checked() {
        let now = Time::from_nanos(10);
        assert_eq!(now + Duration::from_nanos(2), Time::from_nanos(12));
        assert_eq!(now.checked_sub(Duration::from_nanos(11)), None);
        assert_eq!(now - Time::from_nanos(3), Duration::from_nanos(7));
    }

    #[test]
    fn trace_json_is_stable_and_escaped() {
        let mut trace = Trace::new(Seed::new(2), 3);
        trace.build = String::from("build\n1");
        trace.push(Time::from_nanos(1), None, EventKind::Fault, vec![0xab]);
        assert!(trace.to_json().contains("build\\n1"));
        assert!(trace.to_json().contains("ab"));
    }

    #[test]
    fn golden_cluster_policy_v1() {
        let policy = ClusterPolicy::default();
        let bytes = policy.encode();
        assert_eq!(bytes.len(), 138);
        assert_eq!(&bytes[..4], b"CCPL");
        assert_eq!(ClusterPolicy::decode(&bytes), Ok(policy));
        assert_eq!(policy.hash(), fnv1a(&bytes));
    }

    #[test]
    fn trap_cluster_policy_codec_is_bounded_and_canonical() {
        let mut bytes = ClusterPolicy::default().encode();
        bytes.push(0);
        assert!(matches!(
            ClusterPolicy::decode(&bytes),
            Err(DecodeError::TrailingBytes { .. })
        ));
        let mut corrupt = ClusterPolicy::default().encode();
        corrupt[6..10].copy_from_slice(&0_u32.to_le_bytes());
        let last = corrupt.len() - 4;
        let checksum = crc32c_zeroed_tail(&corrupt);
        corrupt[last..].copy_from_slice(&checksum.to_le_bytes());
        assert!(ClusterPolicy::decode(&corrupt).is_err());
    }

    #[test]
    fn golden_cccf_v1_and_ccar_v1() {
        let envelope = ConfigEnvelope {
            admin_session: Some((SessionKey::new(1, ClientId::new(9)).expect("key"), 4)),
            leader_time: Time::from_nanos(12),
            operation: ConfigOperation::AddLearner {
                id: NodeId::new(3),
                address: Some(PeerAddress::V4 {
                    ip: [127, 0, 0, 1],
                    port: 9000,
                }),
            },
        };
        let encoded = envelope.encode();
        assert_eq!(ConfigEnvelope::decode(&encoded), Ok(envelope));
        let reply = AdminReply {
            operation_tag: 1,
            result: AdminResultTag::Applied,
            source_index: LogIndex::new(7),
            detail: vec![1, 2],
        };
        assert_eq!(AdminReply::decode(&reply.encode()), Ok(reply));
    }

    #[test]
    fn trap_config_envelope_admin_absence_is_canonical() {
        let envelope = ConfigEnvelope {
            admin_session: None,
            leader_time: Time::from_nanos(0),
            operation: ConfigOperation::RemoveLearner { id: NodeId::new(2) },
        };
        let mut bytes = envelope.encode();
        // `has_admin_session=0` permits no hidden identity fields.
        bytes[8] = 1;
        let last = bytes.len() - 4;
        let checksum = crc32c_zeroed_tail(&bytes);
        bytes[last..].copy_from_slice(&checksum.to_le_bytes());
        assert!(ConfigEnvelope::decode(&bytes).is_err());
    }

    #[test]
    fn trap_membership_address_codec_is_canonical() {
        assert!(
            PeerAddress::V4 {
                ip: [0, 0, 0, 0],
                port: 1
            }
            .validate()
            .is_err()
        );
        assert!(
            PeerAddress::V4 {
                ip: [224, 0, 0, 1],
                port: 1
            }
            .validate()
            .is_err()
        );
        assert!(
            PeerAddress::V4 {
                ip: [127, 0, 0, 1],
                port: 0
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn golden_membership_state_v1_round_trips_joint_projection() {
        let voters = [
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
            NodeId::new(4),
        ]
        .into_iter()
        .collect();
        let state = MembershipState {
            voters,
            learners: [NodeId::new(5)].into_iter().collect(),
            joint: Some(JointMembership {
                old_voters: [NodeId::new(1), NodeId::new(2), NodeId::new(3)]
                    .into_iter()
                    .collect(),
                new_voters: [NodeId::new(1), NodeId::new(2), NodeId::new(4)]
                    .into_iter()
                    .collect(),
                enter_index: LogIndex::new(9),
            }),
            addresses: [(
                NodeId::new(5),
                PeerAddress::V4 {
                    ip: [127, 0, 0, 1],
                    port: 7205,
                },
            )]
            .into_iter()
            .collect(),
            active_features: 0,
        };
        let bytes = state.encode().expect("membership encode");
        assert_eq!(&bytes[..4], b"CCMS");
        assert_eq!(MembershipState::decode(&bytes), Ok(state));
    }

    #[test]
    fn trap_unknown_config_tag_fails_closed() {
        assert!(ConfigOperation::decode(99, &[]).is_err());
    }

    #[test]
    fn trap_host_limits_reject_zero_admission_caps() {
        assert!(HostLimits::default().is_valid());
        let invalid = HostLimits {
            max_pending_io: 0,
            ..HostLimits::default()
        };
        assert!(!invalid.is_valid());
    }

    #[test]
    fn trap_invalid_limits_fail_boot() {
        let invalid = HostLimits {
            max_snapshot_chunk_bytes: HostLimits::default().max_peer_frame_bytes + 1,
            ..HostLimits::default()
        };
        assert_eq!(
            invalid.validate(),
            Err(LimitError {
                field: "max_snapshot_chunk_bytes"
            })
        );

        let invalid = HostLimits {
            max_host_output_bytes: HostLimits::default().max_host_total_output_bytes + 1,
            ..HostLimits::default()
        };
        assert_eq!(
            invalid.validate(),
            Err(LimitError {
                field: "max_host_total_output_bytes"
            })
        );
    }

    #[test]
    fn trap_wasm_accepts_stream_totals_larger_than_usize() {
        // Stream totals are retained as u64 and are never converted to one
        // allocation-sized usize at boot.  Four GiB therefore remains a
        // valid configuration even for a wasm32 host; only independently
        // bounded chunks and builders need to fit one allocation.
        let limits = HostLimits::default();
        assert_eq!(limits.max_snapshot_bytes, 4_u64 * 1024 * 1024 * 1024);
        assert!(limits.max_snapshot_bytes > u64::from(u32::MAX));
        assert!(limits.validate().is_ok());
        assert!(limits.max_snapshot_chunk_bytes <= u64::from(u32::MAX));
        assert!(limits.max_checkpoint_builder_bytes <= u64::from(u32::MAX));
    }
}
