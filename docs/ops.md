# Operations notes

The real host is a local demonstration of the host boundary. `ccdb init`
creates a three-node configuration, and each process stores acknowledged
writes in `commands.log`, a length-delimited journal with CRC-32C and
`sync_data` before applying the command. The leader replicates over bounded
`CCREPL1` TCP frames to a voter quorum; restart recovery can install the
leader's journal snapshot into a wiped data directory. This journal is not a
replacement for the segmented WAL in the simulator crates.

`ccdb` does not run the consensus core. Leadership here is static: the lowest
configured node id that answers a TCP probe is the leader, records are written
at term 1, and no election takes place. Raft itself lives in `cc-cluster` and
`cc-raft`, exercised by the deterministic simulator and the WebAssembly
theater. Read [limitations](LIMITATIONS.md) before reading a `role:leader`
line as a consensus outcome.

## Start and inspect

```sh
cargo run -p cc-node --bin ccdb -- init --cluster demo --nodes 3 --base-dir /tmp/ccdb-demo
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n1/ccdb.toml
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n2/ccdb.toml
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n3/ccdb.toml
cargo run -p cc-node --bin ccdb -- admin --addr 127.0.0.1:7102 status
cargo run -p cc-node --bin ccdb -- selfcheck --data-dir /tmp/ccdb-demo/n1
cargo run -p cc-node --bin ccdb -- selfcheck --deep --data-dir /tmp/ccdb-demo/n1
cargo run -p cc-node --bin ccdb -- doctor --data-dir /tmp/ccdb-demo/n1
cargo run -p cc-node --bin ccdb -- admin backup --data-dir /tmp/ccdb-demo/n1 --output /tmp/n1.ccbk
cargo run -p cc-node --bin ccdb -- admin restore --input /tmp/n1.ccbk --data-dir /tmp/ccdb-restored/n1
```

The client listener accepts bounded RESP2 frames. The peer listener accepts
bounded `CCPF` frames from `cc-env`, checks their checksum, and carries the
versioned `CCREPL1` write, acknowledgement, and snapshot messages. Admin
status follows a follower's leader address to the current leader; the bench
driver does the same for real-host workloads. `metrics.prom` is refreshed by
the host heartbeat and includes command, write, fsync, and peer-frame
counters. `listen_metrics` serves the Prometheus text at `/metrics` and a
dependency-free live dashboard at `/`. A data-directory identity marker
prevents starting a configuration against another node's files. Deep
self-check replays and CRC-checks the journal, verifies its sequence watermark,
checks identity/config agreement, rejects incomplete snapshot staging, and
validates emitted metrics; it advises but never repairs.

## Fault work

`scripts/demo.sh` is the three-node proof: it checks acknowledged writes,
kills the leader, measures sub-two-second failover, wipes and restarts a node,
and verifies TCP snapshot catch-up. `scripts/real-faults.sh` adds a sustained
SET workload, chunked history checking, SIGSTOP/SIGCONT pauses, and a
byte-level `cc-swarm proxy` on one peer path. `--duration-seconds N` and
`--soak-hours N` both set the run length; the routine local check is five
minutes and the longest soak actually performed is one hour. There is no
multi-day soak gate — running one has never been part of this project's
evidence, and the page does not imply otherwise. Page-cache loss, torn writes,
partitions, and large simulator campaigns must use the deterministic models and
their recorded commands, which reach far more states per second than wall-clock
soaking does.

For a packaged local lab, see [`deploy/README.md`](../deploy/README.md). It
contains a three-node Compose topology and a hardened-by-default systemd unit
template; neither changes the project’s local-lab security boundary.
