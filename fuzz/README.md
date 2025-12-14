# Codec fuzz corpus

`inventory.tsv` is the reviewed decoder/budget inventory. Every inventory name
owns a directory below `corpus/`; `corpus/manifest.tsv` records the checked-in
case hash, expected typed outcome, stable signature, and allocation budget.

Replay one format with
`cargo run --locked -p cc-swarm -- fuzz --format <name> --iterations <n>`.
Mutation selection is deterministic from `--seed`. CI never
passes `--update-corpus`; that flag only proposes locally reviewable cases and
regenerates the manifest. Panics and budget failures are minimized into the
ignored `fuzz-artifacts/crashes/` tree for upload. Moving a fixed case into a
`regressions/` subdirectory makes it permanent.

The panic guard is diagnostic only. Allocation safety comes from decoder
length/count preflight and the inventory budgets; an allocator abort cannot be
made safe by `catch_unwind`.
