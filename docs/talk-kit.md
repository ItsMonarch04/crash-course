# Fifteen-minute talk kit

## Run of show

1. **0:00–2:00 — The premise.** The database is the excuse; deterministic
   replay and evidence-backed claims are the product.
2. **2:00–5:00 — One core, modeled faults.** Open the theater, point out the
   live WASM engine, virtual time, and real trace panels.
3. **5:00–9:00 — Break it live.** Press Play, identify the leader, press
   **Kill leader**, and wait for a different leader. Call out acknowledged and
   lost-write counters.
4. **9:00–11:00 — Prove replay.** Press **Run twice** and show the matching
   trace hash. Share the URL, then reload it.
5. **11:00–13:00 — Explain a trace.** Export a sequence diagram and use
   `cc-swarm diff` to show the first semantic divergence between two specs.
6. **13:00–15:00 — Honesty boundary.** Open `LIMITATIONS.md`: model versus
   kernel truth, one group, bounded histories, and no security boundary.

## Live-demo commands

```sh
./scripts/demo.sh
cargo run -p cc-swarm -- one --seed 0x2a --profile rough --export-json
cargo run -p cc-swarm -- sequence artifacts/0x000000000000002a.json \
  --output artifacts/talk.svg
```

Keep a browser tab on the embedded theater and the generated GIF as fallbacks.
Never substitute a staged trace while describing it as live.
