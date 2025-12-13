# ADR 0015: Bounds are correctness properties (D10)

Status: Implemented; current receipts are consolidated in [ADR-0017](0017-complete-replay-and-implementation-status.md)

`ClusterPolicy` owns limits and time values that can change deterministic
decode, apply, or reply behavior. Peer and durable identity fences compare the
complete canonical policy bytes; its hash is only a fast mismatch fence.
Host-local limits may affect admission, delay, and fail-stop behavior but not a
committed reply.

Every owner accounts for bytes and counts before allocation. Sessions keep
canonical commands and replies, and capacity policy does not evict live session
state. Aggregate accounting and streamed resource receipts are included in the
implemented boundary consolidated by ADR-0017.
