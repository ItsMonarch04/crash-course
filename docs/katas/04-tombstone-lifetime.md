# Kata 04: tombstone lifetime

Build and run the exact detector:

```sh
cargo test -p cc-store --features kata04 trap_kata_04_tombstone_gc_is_found_within_budget
cargo run -p cc-swarm --features kata04 -- run --profile rough --seeds 1 --jobs 1 --ledger campaigns/kata-ledger.tsv --build kata04 --export-json
```

The fixed input is config hash `23b166788e01ba12`, seeds `0x0..0x1`. The
detector must pass in one predicate evaluation above the bottom level;
`artifacts/0.json` is `synthetic=true`. The bounded campaign is a labelling
receipt and may be healthy. Budget: one unit test and one seed, with at most
10,000 events at one virtual instant.

Learning objective: explain how prematurely dropping a delete marker can
resurrect an older value from an unvisited lower level.

Hint: age relative to snapshots is necessary, but not sufficient.

## Solution

The kata ignores `reaches_bottom_for_key`. Restore that conjunct along with the
snapshot-age and unselected-overlap checks before a tombstone may be dropped.
