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

The theater includes guided figure-8, asymmetric-election, thundering-herd,
and snapshot-catch-up lessons. An embeddable view uses the same live engine:
append `#embed=1&seed=0x...&profile=rough` to the theater URL. It removes the
chrome, not the simulator.

## Gallery submissions

A scenario contribution must include the complete `RunSpec`, the command that
replays it, and one of these honest outcomes:

- a pinned, shrunk real failure admitted through the museum schema; or
- a teaching scenario explicitly labelled as a preset, never as a discovered
  bug.

Include this attestation in the change description: “The trace was produced by
the checked-in engine and was not fabricated or hand-edited.” A clean run is a
valid scenario; do not plant a bug to make it dramatic.
