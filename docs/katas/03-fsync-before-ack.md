# Kata 03: fsync before acknowledgement

Build and run the exact detector:

```sh
cargo test -p cc-host --features kata03 trap_kata_03_ack_before_fsync_is_found_within_budget
cargo run -p cc-swarm --features kata03 -- run --profile rough --seeds 1 --jobs 1 --ledger campaigns/kata-ledger.tsv --build kata03 --export-json
```

The fixed input is config hash `23b166788e01ba12`, seeds `0x0..0x1`. The
detector must observe a peer reply immediately after `DiskWrite`, with no
`DiskFsync`. The campaign deterministically reaches its 10,000-events-per-
instant guard on seed zero, exits nonzero, and preserves `artifacts/0.json` as
`synthetic=true` plus a row in the typed kata ledger. That guard is the upper
budget; no wall-clock waiting is involved.

Learning objective: identify the durability continuation that separates page-
cache completion from stable-storage acknowledgement.

Hint: follow one vote reply backward through `IoDone::Written` and
`IoDone::Fsynced`.

## Solution

`Written` must allocate and emit `DiskFsync`. Only `Fsynced` may release
`NodeInput::Persisted { success: true }` and its dependent network reply.
