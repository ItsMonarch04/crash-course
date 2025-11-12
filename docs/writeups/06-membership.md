# 06 — Membership changes are a two-quorum story

The Raft fixture represents a learner separately from voters. Promotion is
rejected until the learner's match index reaches the leader's log. A joint
configuration then requires a majority of both the old and new sets; only
after the joint entry is committed can the old set leave.

The implementation carries the configuration in the replicated log and
snapshot surface. Tests name the sharp edges: config-on-append,
configuration in a snapshot, removed-leader timing, and disruption defenses.
The real SimCluster campaign runner can select the membership profile; its
large campaign result is recorded by the fixture gate rather than implied by
these unit examples.
