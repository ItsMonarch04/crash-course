# ADR-0001: Foundation decisions

- Status: Accepted
- Date: 2025-10-31

## Context

Crash Course is a deterministic distributed-systems laboratory. Its core must be
portable between the simulator, a real host, and WebAssembly, while its persisted
formats and public demonstrations remain replayable. The following decisions
close the highest-risk architectural choices before implementation begins.

## Decisions

1. **Sans-IO synchronous cores** — effects and inputs are values, which removes scheduler nondeterminism and makes simulation and WebAssembly use the same state machines.
2. **Datagram semantics over TCP** — the core tolerates loss, duplication, reordering, and delay, so a reliable transport cannot hide consensus assumptions.
3. **No serde in persisted formats** — hand-rolled codecs make byte layouts, bounds, versions, and corruption behavior explicit.
4. **Integer-only core** — integer arithmetic avoids native/WASM floating-point divergence in safety-critical logic.
5. **BTreeMap-only core** — ordered containers keep iteration independent of hash seeds and allocation history.
6. **xoshiro256++ with domain streams** — stable, independently derived streams keep random choices reproducible across refactors.
7. **Page-cache disk model** — writes become visible before fsync and non-durable data can vanish on crash, exposing ordering bugs.
8. **Fault plans as data** — materialized fault schedules can be inspected, shrunk, and replayed exactly.
9. **Replay is re-execution** — traces are witnesses and the seed/config/build are the replay contract, avoiding a second execution semantics.
10. **Leveled compaction parameters** — bounded, explicit levels provide predictable write-amplification experiments and deterministic scheduling.
11. **Pre-Vote and CheckQuorum enabled** — isolated nodes must not destabilize a healthy majority or advance terms unnecessarily.
12. **Pipeline window eight** — a small fixed window exercises concurrency without making message order unbounded.
13. **No-op on election** — a leader commits a current-term entry before serving read barriers, making the read precondition visible.
14. **ReadIndex now, leases later** — quorum-confirmed reads preserve safety without depending on clock bounds; leases remain a future optimization.
15. **Joint consensus with learners** — membership changes require overlapping majorities and a catch-up path that is safe to observe.
16. **Configuration on append** — the configuration becomes durable at the same log position as the change it governs.
17. **TTL via leader log-time** — expiry is replicated data, not a wall-clock observation, so replicas agree under skew.
18. **Sessions in snapshots** — exactly-once request state survives restart and snapshot installation with the key/value state.
19. **Applied index atomic with data** — readers cannot observe an applied marker without the corresponding state-machine mutation.
20. **RESP2 subset with honest NOTLEADER replies** — clients receive a small familiar protocol and an explicit routing signal rather than a fabricated success.
21. **Fsync errors are fatal** — acknowledging data after a failed durability barrier would violate the crash-safety promise.
22. **Separate metrics port** — observability cannot consume or perturb the client protocol surface.
23. **Museum pinned-build contract** — old builds and their ABI adapters remain runnable so a bug exhibit remains a reproducible artifact.
24. **No fabricated exhibits** — the museum records only bugs the harness actually found, including a seed and a replayable witness.
25. **Bench self-comparison rule** — published numbers compare the project with itself under a disclosed environment, not with incomparable systems.
26. **Single group forever in the core** — sharding would change the correctness and failure model; it is outside the teaching artifact.
27. **Machine envelope for the workflow track** — durable execution records the host assumptions needed to distinguish replayable decisions from side effects.
28. **Analytics floats allowed only outside consensus** — reports may use convenient presentation math while replicated decisions remain integer-only.
29. **Tie-sequence event ordering** — equal-time events execute by insertion sequence, making heap implementation details irrelevant.
30. **Trace binary plus JSON dual encoding** — compact bytes serve gates and files while JSON serves inspection without becoming a consensus format.
31. **AGPL-3.0-only license** — the lab remains freely shareable and every crate carries the same explicit license metadata.
32. **`preflight.sh` as the local CI mirror** — contributors can run the same essential checks without needing a remote commit or pull request.
33. **Runaway guards in the simulator** — zero-delay self-scheduling must fail loudly instead of hanging a campaign.
34. **Theater Playwright smoke with reduced motion** — browser behavior is checked at the interaction boundary while animation timing stays testable.

## Consequences

The core has a narrow dependency surface and explicit versioned codecs. Some
implementations are intentionally less convenient than their production
counterparts, especially around I/O and data structures. Those costs are part of
the laboratory's correctness and teaching goals. Any reversal requires a new,
numbered ADR and a format or protocol version bump when bytes are affected.

## Alternatives considered

General-purpose serialization, ambient clocks, live random draws, unordered
maps, async core state machines, and a production-oriented scope were rejected
because each would hide a source of nondeterminism or broaden the proof surface.
