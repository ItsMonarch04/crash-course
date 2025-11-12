# 08 — A flight recorder is a teaching surface

The theater is intentionally instrument-like. The persistent WASM bridge owns
the deterministic cluster state; React owns controls and inspection, while
Canvas2D renders the topology and timeline. The panels show roles, terms,
commit/applied indexes, durable bytes, live trace markers, and acked/lost
counters. Every interaction has a seed/profile/RunSpec shape that can be put in
a URL, and scrub replays fault injections from five-second virtual-time
checkpoints.

The museum is empty until a trace earns a pinned build and a written root
cause. That blank state is useful: it tells visitors that the catalogue is an
archive of evidence, not a collection of planted demos. The browser equivalence
fixture compares native and generated-WASM state for the checked scenario; it
does not turn the theater into a production monitoring surface.
