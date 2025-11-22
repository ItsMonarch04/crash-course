# ADR 0012: Binary history receipts (D03)

Status: Accepted — partially implemented

CC-HISTORY v2 is the bounded binary-safe history representation shared by
writers, readers, and the checker. It retains binary arguments, explicit
initial state, stable operation identifiers, and open operations. Whole-run
claims require one continuous history and final state evidence; a bounded
window must state its initial receipt and cannot claim an empty-model proof.

The real fault harness coverage receipt remains separately gated.
