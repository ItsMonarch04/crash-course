# ADR 0011: Total atomic committed apply (D02)

Status: Accepted — partially implemented

Every committed entry must advance the consensus and state-machine apply
watermarks exactly once. Deterministic command errors and explicit-session
duplicates are normal replies, not infrastructure errors that skip the apply
marker. Session equality compares canonical logical command bytes and a
same-sequence different command never mutates state.

Conditional SET is one replicated transition; host adapters must not turn it
into a local read followed by a proposal. Malformed committed data and a
durability failure fail closed. The complete durable atomic-store protocol is
still an implementation gate.
