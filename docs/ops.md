# Operations notes

The real host is a local demonstration of the host boundary. `ccdb init`
creates a three-node configuration, and each process stores acknowledged
writes in `commands.log`, a length-delimited journal with CRC-32C and
`sync_data` before applying the command. The leader replicates over bounded
`CCREPL1` TCP frames to a voter quorum; restart recovery can install the
leader's journal snapshot into a wiped data directory. This journal is not a
replacement for the segmented WAL in the simulator crates.

## Start and inspect

```sh
cargo run -p cc-node --bin ccdb -- init --cluster demo --nodes 3 --base-dir /tmp/ccdb-demo
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n1/ccdb.toml
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n2/ccdb.toml
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n3/ccdb.toml
cargo run -p cc-node --bin ccdb -- admin --addr 127.0.0.1:7102 status
cargo run -p cc-node --bin ccdb -- selfcheck --data-dir /tmp/ccdb-demo/n1
```

The client listener accepts bounded RESP2 frames. The peer listener accepts
bounded `CCPF` frames from `cc-env`, checks their checksum, and carries the
versioned `CCREPL1` write, acknowledgement, and snapshot messages. Admin
status follows a follower's leader address to the current leader; the bench
driver does the same for real-host workloads. `metrics.prom` is refreshed by
the host heartbeat and includes command, write, fsync, and peer-frame
counters. A data-directory identity marker prevents starting a configuration
against another node's files.

## Fault work

`scripts/demo.sh` is the three-node proof: it checks acknowledged writes,
kills the leader, measures sub-two-second failover, wipes and restarts a node,
and verifies TCP snapshot catch-up. `scripts/real-faults.sh` adds a sustained
SET workload, chunked history checking, SIGSTOP/SIGCONT pauses, and a
byte-level userspace proxy on one peer path. `--soak-hours 1` performs the
local one-hour fixture soak; the 24-hour owner/nightly soak remains a separate
gate. Page-cache loss, torn writes, partitions, and large simulator campaigns
must use the deterministic models and their recorded commands.
