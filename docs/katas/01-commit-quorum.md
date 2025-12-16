# Kata 01: commit quorum

Build and run the exact detector:

```sh
cargo test --locked -p cc-raft --features kata01 trap_kata_01_commit_quorum_is_found_within_budget
cargo run --locked -p cc-swarm --features kata01 -- run --profile rough --seeds 1 --jobs 1 --ledger campaigns/kata-ledger.tsv --build kata01 --export-json
```

The fixed input is config hash `23b166788e01ba12`, seeds `0x0..0x1`. The
detector must pass after one four-voter construction; the campaign artifact is
`artifacts/0.json`, has `synthetic=true`, and may remain healthy because this
seed is evidence of labelling, not guaranteed reachability. Budget: one unit
test and one bounded seed, never more than 10,000 events at one virtual instant.

Learning objective: derive quorum size for odd and even voter sets and explain
why two acknowledgements cannot commit in a four-voter term.

Hint: write the strict-majority formula before reading `majority()`.

## Solution

The planted branch returns `len / 2` for even sets. The correct quorum is
`len / 2 + 1` for every nonempty voter set.
