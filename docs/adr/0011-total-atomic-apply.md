# ADR 0011: Total atomic committed apply (D02)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

Every committed entry must advance the consensus and state-machine apply
watermarks exactly once. Deterministic command errors and explicit-session
duplicates are normal replies, not infrastructure errors that skip the apply
marker. Session equality compares canonical logical command bytes and a
same-sequence different command never mutates state.

Conditional SET is one replicated transition; host adapters do not turn it
into a local read followed by a proposal. Malformed committed data and a
durability failure fail closed. The store-WAL continuation now provides the
durable atomic-store protocol recorded in ADR-0017.
