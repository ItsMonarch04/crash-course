# ADR 0012: Binary history receipts (D03)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

CC-HISTORY v2 is the bounded binary-safe history representation shared by
writers, readers, and the checker. It retains binary arguments, explicit
initial state, stable operation identifiers, and open operations. Whole-run
claims require one continuous history and final state evidence; a bounded
window must state its initial receipt and cannot claim an empty-model proof.

The real-host fault and replay receipts now consume this same history shape, as
recorded in ADR-0017.
