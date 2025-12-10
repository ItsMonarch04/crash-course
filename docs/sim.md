# Simulator scenarios

Scenarios are data, not scripts. A run is a seed, profile, node count, workload,
virtual-time bound, and ordered fault plan. The browser share button encodes the
same information in `run_spec`; `cc-swarm one --export-json` writes it as an
artifact that can be replayed, shrunk, compared, or rendered.

Useful authoring commands:

```sh
cargo run -p cc-swarm -- one --seed 0x2a --profile rough --export-json
cargo run -p cc-swarm -- diff artifacts/a.json artifacts/b.json
cargo run -p cc-swarm -- sequence artifacts/42.json --output artifacts/42.svg
cargo run -p cc-swarm -- search --profile brutal --iterations 1000
```

To check a history captured outside the simulator — a real-host run, or a
trace exported from another tool — hand the TSV to the checker directly:

```sh
./scripts/check-real-history.sh --file history.tsv
```

The theater includes guided figure-8, asymmetric-election, thundering-herd,
and retained-log recovery lessons. Cluster size is part of the scenario: the
control offers 3, 5, and 7 voters and rebuilds the engine at the chosen size,
which `#nodes=7` also selects. An embeddable view uses the same live engine:
append `#embed=1&seed=0x...&profile=rough` to the theater URL. It removes the
chrome, not the simulator.

The Theater's packet-loss slider targets the displayed directed link. It
accepts an integer percentage from 0 through 100, converts it to the
simulator's `P16` drop probability with nearest-integer rounding, and displays
the inverse-rounded effective value read back from that link. The control
therefore records a real `LinkDegrade` action in the shared scenario, rather
than changing only a browser label.

The disk-latency slider targets the selected node and records a persistent
`SlowDisk` fault. Its 0–5,000 ms value configures the simulator's independent
read, write, fsync, rename, and directory-sync delay fields. For the current
Raft WAL path, write and fsync are separate scheduled completions: the node
defers its own subsequent inputs and cannot send a dependent vote, append
response, or client reply until the delayed fsync succeeds. Zero clears the
additional delay. This is distinct from the `DiskDegrade` one-shot EIO fault
used in fault profiles; N3's file-backed store will consume the remaining
operation categories.

Database-surface campaigns use dedicated profiles rather than relabelling
`rough`. `batch` activates the replicated `ATOMIC_BATCH` feature after
simulated CCHL observations, then issues multi-command batches checked as one
atomic transition. `follower-read` routes `GET` to a non-leader after a v3
ReadIndex grant. `follower-read-v2` keeps the same client mix against v2
capability observations so a missing feature returns an error instead of a
local linearizable read. `stale-read` records tagged local observations and
checks them only against the reported applied watermark after TTL filtering.

The `corruption` profile is the transport counterpart. It installs one corrupt
frame, one truncated frame, one rechecksummed-but-malformed CCRP body, and one
replay of a previously sent frame on the same directed link, so every decoder
rejection path is reached in a single run. A replayed frame carries whatever
bytes were last on the wire, so a duplicate of a corrupted frame is a modelled
drop rather than a host invariant violation.

Persistent storage faults are explicit scenario data as well: `EnospcFrom`
rejects later space-growing writes, `DiskQuota` caps the simulated allocated
bytes, and `BitRotAtRest` flips one byte immediately after its selected fsync.
The simulator keeps the pre-flip durable checksum, so the next read or restart
detects that corruption and places the node in `StorageFault`; it is never
served as a plausible value.

## Gallery submissions

A scenario contribution must include the complete `RunSpec`, the command that
replays it, and one of these honest outcomes:

- a pinned, shrunk real failure admitted through the museum schema; or
- a teaching scenario explicitly labelled as a preset, never as a discovered
  bug.

Include this attestation in the change description: “The trace was produced by
the checked-in engine and was not fabricated or hand-edited.” A clean run is a
valid scenario; do not plant a bug to make it dramatic.
