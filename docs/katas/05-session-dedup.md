# Kata 05: session deduplication

Build and run the exact detector:

```sh
cargo test --locked -p cc-cluster --features kata05 trap_kata_05_session_dedup_is_found_within_budget
cargo run --locked -p cc-swarm --features kata05 -- run --profile rough --seeds 1 --jobs 1 --ledger campaigns/kata-ledger.tsv --build kata05 --export-json
```

The fixed input is config hash `23b166788e01ba12`, seeds `0x0..0x1`. The
detector must pass after two requests with one session sequence but different
canonical command bytes; `artifacts/0.json` is `synthetic=true`. The campaign
may remain healthy because the exact detector supplies the verdict. Budget:
two table operations and one bounded seed, with at most 10,000 events at one
virtual instant.

Learning objective: distinguish an exact reconnect retry from illegal sequence
reuse and explain why returning a cached success is unsafe for different bytes.

Hint: a session key and sequence identify a slot, not the command occupying it.

## Solution

On `sequence == max_seq`, compare the incoming canonical command with the
stored bytes. Return the cached reply only when equal; otherwise return
`SequenceConflict` without mutation.
