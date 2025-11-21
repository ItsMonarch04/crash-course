# ADR 0010: Membership quorum projection (D01)

Status: Accepted — partially implemented

Membership is replicated state. A node projects stable voters and learners, or
one joint old/new voter transition, from the surviving configuration log. Every
election, commit, ReadIndex, and CheckQuorum decision counts only eligible
voters; joint decisions require a majority of both sets. Learners and unknown
senders never contribute to a quorum.

Configuration takes effect on append and projection is rebuilt after suffix
truncation. The remaining recovery and production-admin receipts are tracked
as implementation work; this record does not claim them complete.
