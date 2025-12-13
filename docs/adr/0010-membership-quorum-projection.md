# ADR 0010: Membership quorum projection (D01)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

Membership is replicated state. A node projects stable voters and learners, or
one joint old/new voter transition, from the surviving configuration log. Every
election, commit, ReadIndex, and CheckQuorum decision counts only eligible
voters; joint decisions require a majority of both sets. Learners and unknown
senders never contribute to a quorum.

Configuration takes effect on append and projection is rebuilt after suffix
truncation. Recovery and real-host administration now exercise the same
projection; ADR-0017 consolidates those receipts.
