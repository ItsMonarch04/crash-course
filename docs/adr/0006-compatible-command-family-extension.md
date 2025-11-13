# ADR-0006: Compatible command-family extension

- Status: Accepted
- Date: 2025-11-13

## Context

The single-key RMW enhancement adds `APPEND`, `GETSET`, `GETDEL`, `EXPIREAT`,
and `TTL`. Write commands are stored inside the existing `CCKV` journal
payload, so the byte compatibility rule applies.

## Decision

Add command tags 12–16 without changing existing tags or field layouts. Keep
`CCKV` version 1 because this is a backward-compatible tagged-union extension:
new readers accept every old payload byte-for-byte, while old readers already
fail closed on an unknown tag. A version bump is reserved for a change that
reinterprets an existing byte sequence.

`APPEND` preserves an existing TTL. `GETSET` clears it like `SET`; `GETDEL`
removes both value and TTL. `TTL` returns `-2` for a missing/expired key and
`-1` for a persistent key. `EXPIREAT` carries an absolute leader timestamp.
All writes remain one replicated, session-deduplicated command; no multi-key
atomicity is introduced.

## Consequences

Codec round trips cover every new tag, and state-machine tests cover reply and
TTL semantics. Real-host RESP parsing maps directly to the same commands.
