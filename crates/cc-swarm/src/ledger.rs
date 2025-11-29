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
    Coverage(String),
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
            Self::Coverage(reason) => write!(f, "campaign ledger coverage: {reason}"),
        }
    }
}

/// Validate an explicit sharded coverage claim. Coverage is proved against
/// the requested half-open range; it is never inferred from the rows that
/// happened to arrive.
pub fn validate_sharded_coverage(
    ledgers: &[SeedLedger],
    start: u64,
    end: u64,
    shard_count: u64,
    expected_build: &str,
    expected_config: u64,
) -> Result<(), LedgerError> {
    if start > end {
        return Err(LedgerError::Coverage(String::from(
            "range start exceeds end",
        )));
    }
    if shard_count == 0 || ledgers.len() != usize::try_from(shard_count).unwrap_or(usize::MAX) {
        return Err(LedgerError::Coverage(format!(
            "expected {shard_count} shard ledgers, received {}",
            ledgers.len()
        )));
    }
    let mut residue_owners = BTreeMap::<u64, usize>::new();
    let mut covered = BTreeMap::<u64, usize>::new();
    for (source, ledger) in ledgers.iter().enumerate() {
        let mut source_residue = None;
        for row in ledger.rows() {
            if row.key.build_label != expected_build {
                return Err(LedgerError::Coverage(format!(
                    "wrong build {} for seed 0x{:016x}",
                    row.key.build_label, row.key.seed
                )));
            }
            if row.key.config_hash != expected_config {
                return Err(LedgerError::Coverage(format!(
                    "wrong config {:016x} for seed 0x{:016x}",
                    row.key.config_hash, row.key.seed
                )));
            }
            if row.key.seed < start || row.key.seed >= end {
                return Err(LedgerError::Coverage(format!(
                    "seed 0x{:016x} is outside {start}..{end}",
                    row.key.seed
                )));
            }
            let residue = row.key.seed % shard_count;
            match source_residue {
                Some(prior) if prior != residue => {
                    return Err(LedgerError::Coverage(format!(
                        "source {} contains multiple shard residues",
                        source + 1
                    )));
                }
                Some(_) => {}
                None => {
                    source_residue = Some(residue);
                    if residue_owners.insert(residue, source).is_some() {
                        return Err(LedgerError::Coverage(format!(
                            "shard residue {residue} appears in multiple sources"
                        )));
                    }
                }
            }
            *covered.entry(row.key.seed).or_default() += 1;
        }
    }
    for seed in start..end {
        match covered.get(&seed).copied().unwrap_or(0) {
            1 => {}
            0 => {
                return Err(LedgerError::Coverage(format!("missing seed 0x{seed:016x}")));
            }
            count => {
                return Err(LedgerError::Coverage(format!(
                    "seed 0x{seed:016x} covered {count} times"
                )));
            }
        }
    }
    Ok(())
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
            result.push_str(&encode_ledger_row(row));
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

#[must_use]
pub fn encode_ledger_row(row: &LedgerRow) -> String {
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
    fn trap_ledger_dedupes_identical_rows() {
        let mut ledger = SeedLedger::default();
        assert!(ledger.insert(row(1)).expect("first row"));
        assert!(!ledger.insert(row(1)).expect("identical retry"));
        assert_eq!(ledger.rows().count(), 1);
    }

    #[test]
    fn trap_ledger_conflict_never_selects_verdict() {
        let mut ledger = SeedLedger::default();
        assert!(ledger.insert(row(1)).expect("first row"));
        let mut conflict = row(1);
        conflict.verdict = LedgerVerdict::Invariant;
        assert!(matches!(
            ledger.insert(conflict),
            Err(LedgerError::Conflict { .. })
        ));
        assert_eq!(
            ledger.rows().next().expect("retained row").verdict,
            LedgerVerdict::Ok
        );
    }

    #[test]
    fn trap_resume_skips_only_prior_ok_by_default() {
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
    fn trap_partial_final_line_is_discarded() {
        let mut ledger = SeedLedger::default();
        ledger.insert(row(1)).expect("first row");
        let mut torn = ledger.encode();
        torn.push_str("build-a\t0000000000000007");
        assert_eq!(SeedLedger::parse(&torn), Ok(ledger.clone()));
    }

    #[test]
    fn trap_malformed_interior_ledger_row_fails() {
        let mut ledger = SeedLedger::default();
        ledger.insert(row(1)).expect("first row");
        let corrupt = format!("{}bad\ntrailing-partial", ledger.encode());
        assert!(matches!(
            SeedLedger::parse(&corrupt),
            Err(LedgerError::Line { line: 4, .. })
        ));
    }

    #[test]
    fn trap_shards_partition_seed_range_exactly() {
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

    #[test]
    fn trap_wall_time_difference_cannot_create_seed_conflict() {
        let mut ledger = SeedLedger::default();
        let first = row(7);
        let later_on_another_machine = first.clone();
        assert!(ledger.insert(first).expect("first observation"));
        assert!(
            !ledger
                .insert(later_on_another_machine)
                .expect("ledger row contains no wall-time or machine field")
        );
    }

    #[test]
    fn trap_merge_rejects_missing_shard() {
        let mut ledgers = (0..4).map(|_| SeedLedger::default()).collect::<Vec<_>>();
        for seed in 0..32_u64 {
            ledgers[usize::try_from(seed % 4).expect("residue")]
                .insert(row(seed))
                .expect("shard row");
        }
        assert!(validate_sharded_coverage(&ledgers, 0, 32, 4, "build-a", 7).is_ok());
        ledgers.remove(2);
        assert!(matches!(
            validate_sharded_coverage(&ledgers, 0, 32, 4, "build-a", 7),
            Err(LedgerError::Coverage(_))
        ));
    }
}
