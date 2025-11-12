# 05 — Histories are the user-facing proof

An operation history records invocation, response, client identity, and the
logical ordering constraints imposed by the system. The checker searches legal
linearizations with open operations allowed to take effect after a timeout;
this avoids treating a client that stopped listening as proof that a write did
not happen.

The checker is deliberately small and deterministic. It memoizes remaining
operations plus model state, partitions independent keys, treats `Scan` as a
snapshot-legal operation within its call window, and reports an explicit
`undecided` result at the 10⁷-state budget instead of manufacturing a pass.
Real `cc-swarm` runs capture the histories that feed this checker; the
Porcupine export is a separate cross-validation shape. The theater's verdict
chip points back to this report, not to replace it.
