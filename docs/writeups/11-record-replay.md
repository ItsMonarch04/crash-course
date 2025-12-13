# 11 — Record in production, replay in the simulator

`ccdb run --record` writes CCIJ: the captured boot image, each admitted input,
the exact block-read observations and service time, and the complete effect
vector produced by the shared Driver. The recorder writes a synced terminal
footer that says `complete`, `capped`, or the fatal reason; replay never
upgrades a bounded prefix into a whole-run claim.

The real-process receipt starts three TCP nodes, performs acknowledged work,
fails over, restarts a node, and records a live process. The same journal is
then consumed without ambient host configuration:

```sh
cargo test --locked -p cc-node --test real_cluster trap_real_host_effects_match_replay
cargo run --locked -p cc-swarm -- replay --file run.ccij --assert-effects
```

The observed result is `effects-match` for a complete bounded recording or
`effects-match-prefix` for an interrupted/capped one. Replay compares the
effect bytes at every transition; it does not merely compare the final key-
value state. A block request arriving in another order, a changed service
duration, or one altered effect is therefore a divergence at the boundary
where it occurred.

This is production-host *input* replay through the deterministic core, not a
claim that the simulator reproduces kernel scheduling, TCP buffering, or disk
firmware. Those omissions are measured separately rather than hidden inside
the recorder.
