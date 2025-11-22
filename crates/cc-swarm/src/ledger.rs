// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Sidakpreet Singh

//! Canonical, append-only campaign ledger values. File locking and fsync are
//! adapter concerns; this module owns the strict TSV grammar and conflict
//! semantics so shard merging is deterministic and independently testable.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use cc_sim::FaultProfile;

pub const LEDGER_HEADER: &str = "# cc-ledger-v1";
pub const LEDGER_COLUMNS: &str = "build_label\tconfig_hash\tprofile\tseed_hex\tverdict\tevents\tchecker_states\tpeak_total_bytes\tartifact_hash";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Shard {
    pub index: u64,
    pub total: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardError {
    ZeroTotal,
    IndexOutsideTotal { index: u64, total: u64 },
}

impl fmt::Display for ShardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTotal => f.write_str("campaign shard count must be nonzero"),
            Self::IndexOutsideTotal { index, total } => {
                write!(f, "campaign shard {index} is outside 0..{total}")
            }
        }
    }
}

impl std::error::Error for ShardError {}

impl Shard {
    pub fn new(index: u64, total: u64) -> Result<Self, ShardError> {
        if total == 0 {
            return Err(ShardError::ZeroTotal);
        }
        if index >= total {
            return Err(ShardError::IndexOutsideTotal { index, total });
        }
        Ok(Self { index, total })
    }

    #[must_use]
    pub const fn contains(self, seed: u64) -> bool {
        seed % self.total == self.index
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LedgerVerdict {
    Ok,
    Invariant,
    NotLinearizable,
    Undecided,
    Runaway,
    Error,
}

impl LedgerVerdict {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Invariant => "invariant",
            Self::NotLinearizable => "not-linearizable",
            Self::Undecided => "undecided",
            Self::Runaway => "runaway",
            Self::Error => "error",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "ok" => Self::Ok,
            "invariant" => Self::Invariant,
            "not-linearizable" => Self::NotLinearizable,
            "undecided" => Self::Undecided,
            "runaway" => Self::Runaway,
            "error" => Self::Error,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerKey {
    pub build_label: String,
    pub config_hash: u64,
    pub profile: FaultProfile,
    pub seed: u64,
}

impl Ord for LedgerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.build_label
            .cmp(&other.build_label)
            .then_with(|| self.config_hash.cmp(&other.config_hash))
            .then_with(|| self.profile.as_str().cmp(other.profile.as_str()))
            .then_with(|| self.seed.cmp(&other.seed))
    }
}

impl PartialOrd for LedgerKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerRow {
    pub key: LedgerKey,
    pub verdict: LedgerVerdict,
    pub events: u64,
    pub checker_states: u64,
    pub peak_total_bytes: u64,
    pub artifact_hash: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerError {
    Header,
    Line { line: usize, reason: &'static str },
    Conflict { key: LedgerKey },
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header => f.write_str("invalid campaign ledger header"),
            Self::Line { line, reason } => write!(f, "campaign ledger line {line}: {reason}"),
            Self::Conflict { key } => write!(
                f,
                "campaign ledger conflict for {} {:016x} {} 0x{:016x}",
                key.build_label,
                key.config_hash,
                key.profile.as_str(),
                key.seed
            ),
        }
    }
}

impl std::error::Error for LedgerError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SeedLedger {
    rows: BTreeMap<LedgerKey, LedgerRow>,
}

impl SeedLedger {
    pub fn rows(&self) -> impl Iterator<Item = &LedgerRow> {
        self.rows.values()
    }

    #[must_use]
    pub fn has_ok(&self, key: &LedgerKey) -> bool {
        self.rows
            .get(key)
            .is_some_and(|row| row.verdict == LedgerVerdict::Ok)
    }

    /// Insert one completed result. Identical retries deduplicate; any other
    /// row for the same reproducibility key is a conflict rather than a
    /// convenient choice of the happier outcome.
    pub fn insert(&mut self, row: LedgerRow) -> Result<bool, LedgerError> {
        match self.rows.get(&row.key) {
            None => {
                self.rows.insert(row.key.clone(), row);
                Ok(true)
            }
            Some(existing) if existing == &row => Ok(false),
            Some(_) => Err(LedgerError::Conflict { key: row.key }),
        }
    }

    pub fn parse(text: &str) -> Result<Self, LedgerError> {
        let mut lines = text.split_inclusive('\n');
        let header = lines.next().ok_or(LedgerError::Header)?;
        if header.trim_end_matches('\n') != LEDGER_HEADER {
            return Err(LedgerError::Header);
        }
        let columns = lines.next().ok_or(LedgerError::Header)?;
        if columns.trim_end_matches('\n') != LEDGER_COLUMNS {
            return Err(LedgerError::Header);
        }
        let mut ledger = Self::default();
        for (offset, raw) in lines.enumerate() {
            // A crash may tear only the last line.  It is safe to discard
            // exactly that suffix; any earlier malformed complete record is
            // evidence of corruption.
            if !raw.ends_with('\n') {
                break;
            }
            let line = offset + 3;
            let row = parse_row(raw.trim_end_matches('\n'), line)?;
            ledger.insert(row)?;
        }
        Ok(ledger)
    }

    pub fn merge<'a>(
        ledgers: impl IntoIterator<Item = &'a SeedLedger>,
    ) -> Result<Self, LedgerError> {
        let mut merged = Self::default();
        for ledger in ledgers {
            for row in ledger.rows() {
                merged.insert(row.clone())?;
            }
        }
        Ok(merged)
    }

    #[must_use]
    pub fn encode(&self) -> String {
        let mut result = String::from(LEDGER_HEADER);
        result.push('\n');
        result.push_str(LEDGER_COLUMNS);
        result.push('\n');
        for row in self.rows() {
            result.push_str(&encode_row(row));
            result.push('\n');
        }
        result
    }
}

fn parse_row(raw: &str, line: usize) -> Result<LedgerRow, LedgerError> {
    if raw.is_empty() || raw.contains('\r') {
        return Err(line_error(line, "empty or non-LF row"));
    }
    let fields: Vec<&str> = raw.split('\t').collect();
    if fields.len() != 9 || fields.iter().any(|field| field.is_empty()) {
        return Err(line_error(line, "field count"));
    }
    if fields[0].contains(['\t', '\n']) {
        return Err(line_error(line, "invalid build label"));
    }
    let config_hash = parse_hex16(fields[1]).ok_or_else(|| line_error(line, "config hash"))?;
    let profile = FaultProfile::parse(fields[2]).ok_or_else(|| line_error(line, "profile"))?;
    let seed = fields[3]
        .strip_prefix("0x")
        .and_then(parse_hex16)
        .ok_or_else(|| line_error(line, "seed"))?;
    let verdict = LedgerVerdict::parse(fields[4]).ok_or_else(|| line_error(line, "verdict"))?;
    let events = parse_decimal(fields[5]).ok_or_else(|| line_error(line, "events"))?;
    let checker_states =
        parse_decimal(fields[6]).ok_or_else(|| line_error(line, "checker states"))?;
    let peak_total_bytes =
        parse_decimal(fields[7]).ok_or_else(|| line_error(line, "peak total bytes"))?;
    let artifact_hash = if fields[8] == "-" {
        None
    } else {
        Some(parse_hex16(fields[8]).ok_or_else(|| line_error(line, "artifact hash"))?)
    };
    Ok(LedgerRow {
        key: LedgerKey {
            build_label: String::from(fields[0]),
            config_hash,
            profile,
            seed,
        },
        verdict,
        events,
        checker_states,
        peak_total_bytes,
        artifact_hash,
    })
}

fn line_error(line: usize, reason: &'static str) -> LedgerError {
    LedgerError::Line { line, reason }
}

fn parse_hex16(value: &str) -> Option<u64> {
    (value.len() == 16
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| u64::from_str_radix(value, 16).ok())
    .flatten()
}

fn parse_decimal(value: &str) -> Option<u64> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn encode_row(row: &LedgerRow) -> String {
    format!(
        "{}\t{:016x}\t{}\t0x{:016x}\t{}\t{}\t{}\t{}\t{}",
        row.key.build_label,
        row.key.config_hash,
        row.key.profile.as_str(),
        row.key.seed,
        row.verdict.as_str(),
        row.events,
        row.checker_states,
        row.peak_total_bytes,
        row.artifact_hash
            .map_or_else(|| String::from("-"), |hash| format!("{hash:016x}")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(seed: u64) -> LedgerRow {
        LedgerRow {
            key: LedgerKey {
                build_label: String::from("build-a"),
                config_hash: 7,
                profile: FaultProfile::Rough,
                seed,
            },
            verdict: LedgerVerdict::Ok,
            events: 12,
            checker_states: 34,
            peak_total_bytes: 56,
            artifact_hash: None,
        }
    }

    #[test]
    fn trap_seed_ledger_is_append_only() {
        let mut ledger = SeedLedger::default();
        assert!(ledger.insert(row(1)).expect("first row"));
        assert!(!ledger.insert(row(1)).expect("identical retry"));
        let mut conflict = row(1);
        conflict.events = 13;
        assert!(matches!(
            ledger.insert(conflict),
            Err(LedgerError::Conflict { .. })
        ));
        let encoded = ledger.encode();
        assert_eq!(SeedLedger::parse(&encoded), Ok(ledger));
    }

    #[test]
    fn trap_ledger_resume_only_skips_prior_ok_rows() {
        let mut ledger = SeedLedger::default();
        let ok = row(1);
        let key = ok.key.clone();
        ledger.insert(ok).expect("ok row");
        assert!(ledger.has_ok(&key));
        let failed_key = LedgerKey { seed: 2, ..key };
        let mut failed = row(2);
        failed.verdict = LedgerVerdict::Error;
        ledger.insert(failed).expect("failed row");
        assert!(!ledger.has_ok(&failed_key));
    }

    #[test]
    fn trap_ledger_only_discards_an_incomplete_final_line() {
        let mut ledger = SeedLedger::default();
        ledger.insert(row(1)).expect("first row");
        let mut torn = ledger.encode();
        torn.push_str("build-a\t0000000000000007");
        assert_eq!(SeedLedger::parse(&torn), Ok(ledger.clone()));
        let corrupt = format!("{}bad\n", ledger.encode());
        assert!(matches!(
            SeedLedger::parse(&corrupt),
            Err(LedgerError::Line { line: 4, .. })
        ));
    }

    #[test]
    fn trap_shards_partition_each_seed_once_and_validate_bounds() {
        let shards = (0..4)
            .map(|index| Shard::new(index, 4).expect("valid shard"))
            .collect::<Vec<_>>();
        for seed in 0..128 {
            assert_eq!(
                shards.iter().filter(|shard| shard.contains(seed)).count(),
                1,
                "seed {seed} must belong to exactly one shard"
            );
        }
        assert_eq!(Shard::new(0, 0), Err(ShardError::ZeroTotal));
        assert_eq!(
            Shard::new(4, 4),
            Err(ShardError::IndexOutsideTotal { index: 4, total: 4 })
        );
    }
}
