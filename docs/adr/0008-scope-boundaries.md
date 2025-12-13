# ADR-0008: Permanent scope boundaries

- Status: Accepted; the follower-read deferral is superseded by [ADR-0017](0017-complete-replay-and-implementation-status.md)
- Date: 2025-11-15

## Context

Four capabilities were repeatedly proposed for this laboratory and are
individually reasonable. Each was left open long enough to be re-proposed, and
each carries a cost that is easy to under-count at proposal time and expensive
to reverse once merged. Leaving them nominally "open" invites a future
contributor or maintainer to spend the project's credibility implementing
one.

This record closes them so the answer is written down once rather than
re-argued.

## Decision

The following are permanent non-goals for this repository.

**A second host adapter on an async runtime.** The idea was to add a `tokio`
real host beside the thread-per-connection one and publish "the async tax,
measured." [ADR-0004](0004-dependency-policy.md) makes the standard-library-only
core a deliberate and permanent property rather than an artifact of the offline
environment it was born in. Trading that for one measurement is a bad exchange:
the dependency is permanent, the writeup is not.

**A Jepsen or Elle external audit harness.** Self-auditing is this project's
brand, and an external checker would strengthen it. But the harness cannot be
run or verified in the environment that maintains this repository, and shipping
an unrun harness is a claim without a receipt. The local real-history shape gate
in `scripts/ci/porcupine-crosscheck.sh` stands on its own, and its external leg
stays optional behind `PORCUPINE_COMMAND`.

**Keyspace notifications.** `WATCH`/subscribe would widen the public RESP
surface with new delivery-semantics promises — at-least-once from an offset, no
cross-key ordering — on a single-group lab host that has no consumer for them.
New guarantees need new verification; these would arrive with neither.

**An LSM block cache.** [ADR-0005](0005-store-format-audit.md) already defers
the store's depth work, and the real host does not use the LSM path at all. A
read cache would add cache-coherence surface to a component nothing in
production reads through.

Follower and learner read endpoints were deferred when this decision was
accepted. ADR-0017 supersedes that deferral with the negotiated follower-read
protocol; stale reads remain explicitly non-linearizable.

## Consequences

`docs/LIMITATIONS.md` states these as non-goals so a reader learns the boundary
from the product documentation rather than from this record. The dependency
graph stays as it is: standard-library-only core crates, with `wasm-bindgen` in
`cc-wasm` as the single sanctioned exception under
[ADR-0003](0003-theater-wasm-bridge.md).

A reversal is possible but must supersede this record and say what changed —
new evidence, a new environment, or a genuine user need — rather than restating
the original appeal.

## Alternatives considered

Leaving the items open in a backlog. Rejected: an unbounded backlog of
plausible-sounding work is indistinguishable from a plan, and this project's
scope discipline is one of the few things protecting its honesty guarantees.

Implementing the cheapest one to show progress. Rejected: each is cheap only in
isolation, and the cost that matters is the permanent widening of what the
repository has to keep true.

## Supersedes / superseded by

Extends [ADR-0004](0004-dependency-policy.md) by closing the async-host
question it left open. Does not modify [ADR-0002](0002-real-host-runtime.md),
which anticipated an alternative host but did not commit to one.
