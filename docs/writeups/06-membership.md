# 06 — Membership changes are a two-quorum story

Adding a learner gives a new node time to catch up without changing the voter
quorum. Promotion is rejected until the learner's match index reaches the
leader's log. A joint configuration then requires a majority of both the old
and new sets; only after the joint entry is committed can the old set leave.

The implementation carries the configuration in the replicated log and
snapshot surface. Tests name the sharp edges: config-on-append, configuration
in a snapshot, removed-leader timing, and disruption defenses. A membership
campaign is still a long gate, because a few examples cannot stand in for a
large schedule search.
