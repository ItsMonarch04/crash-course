# ADR 0015: Bounds are correctness properties (D10)

Status: Accepted — partially implemented

`ClusterPolicy` owns limits and time values that can change deterministic
decode, apply, or reply behavior. Peer and durable identity fences compare the
complete canonical policy bytes; its hash is only a fast mismatch fence.
Host-local limits may affect admission, delay, and fail-stop behavior but not a
committed reply.

Every owner must account for bytes and counts before allocation. Sessions keep
canonical commands and replies, and capacity policy may not evict live session
state. Aggregate session accounting and streamed resource receipts remain
implementation work.
