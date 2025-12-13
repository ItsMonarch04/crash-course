# ADR 0016: Replicated membership administration (D17)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

Real membership administration follows add learner, catch up, joint
promote/remove and carries replicated peer addresses. Admin requests use an
explicit operator/session sequence and return durable outcomes rather than
treating proposal acceptance as success.

Leadership transfer is a committed intent, a caught-up `TimeoutNow`, target
election, and a committed final result. Ambiguous or expired admin requests are
never silently converted into a new proposal. Local configuration is bootstrap
only.
