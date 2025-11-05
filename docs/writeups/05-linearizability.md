# 05 — Histories are the user-facing proof

An operation history records invocation, response, client identity, and the
logical ordering constraints imposed by the system. The checker searches legal
linearizations with open operations allowed to take effect after a timeout;
this avoids treating a client that stopped listening as proof that a write did
not happen.

The checker is deliberately small and deterministic. When the search bound is
reached it reports an explicit inconclusive result instead of manufacturing a
pass. The theater's verdict chip is meant to point back to this report, not to
replace it.
