# Kata 02: election timer

Build and run the exact detector:

```sh
cargo test --locked -p cc-raft --features kata02 trap_kata_02_wrong_timer_reset_is_found_within_budget
cargo run --locked -p cc-swarm --features kata02 -- run --profile rough --seeds 1 --jobs 1 --ledger campaigns/kata-ledger.tsv --build kata02 --export-json
```

The fixed input is config hash `23b166788e01ba12`, seeds `0x0..0x1`. The
detector must pass after one denied vote request; `artifacts/0.json` is typed
`synthetic=true`. The campaign may be healthy because the detector, not chance
coverage, is the verdict. Budget: one unit test and one bounded seed, with at
most 10,000 events at one virtual instant.

Learning objective: distinguish valid leader/candidate contact from messages
that are not allowed to suppress a follower election.

Hint: compare the deadline before and after a vote request that must be denied.

## Solution

The kata resets the election deadline when `granted == false`. Remove that
reset; only a granted current-term vote or valid current-leader contact may
postpone the election.
