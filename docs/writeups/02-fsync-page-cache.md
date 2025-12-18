# 02 — Fsync, torn writes, and the page cache made visible

The first storage promise in Crash Course is intentionally narrow: an
acknowledged log record must survive a crash, while an unacknowledged record
may disappear. Making that promise useful requires separating three moments
that are often collapsed in a happy-path implementation.

`append` builds a checksummed physical record and places it in a segment. A
`flush` makes the bytes visible to reads through the page cache. Neither event
means the bytes will survive a process or machine crash. `commit` completes the
fsync barrier and moves the visible prefix into the durable image. Group commit
uses one barrier for every request that accumulated before it completed.

That model changes recovery. A missing or partially written final record is a
normal crash signature: the recovery scan truncates the tail and returns the
valid prefix. A checksum failure in the middle of a segment is different. If a
later valid record exists, silently stopping would erase a committed record and
turn corruption into an apparently successful recovery. The log fails closed
instead.

The tests keep those cases separate: page-cache writes vanish without fsync,
multi-record crashes recover a durable prefix, torn tails are idempotently
truncated, and `trap_midlog_corruption_failstop` rejects a damaged record with
valid data after it. The simulator's disk model applies the same distinction
across files, so a future manifest cannot accidentally depend on an ordering
that the host never promised.

The result is deliberately less magical than a production abstraction. The
caller can ask which logical sequence is durable, and the simulator can place a
crash between every physical step. That visibility is the feature: if a storage
ordering is load-bearing, it should be named in code and exercised by a test.
