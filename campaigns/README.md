# Campaign artifacts

`ledger.tsv` and shard ledgers are local, append-only campaign evidence and
are intentionally ignored by Git. Their first two lines are the fixed
`cc-ledger-v1` header and column schema; use `cc-swarm ledger stats` or
`cc-swarm ledger merge` rather than editing them.

`summary.tsv` is the small, reviewed coverage summary that may be committed — it is
the only file here that a reader should treat as a claim.
It is not a source of truth for resuming or deduplicating runs.
