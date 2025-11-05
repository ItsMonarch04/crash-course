# Operations notes

The real host is a local, single-process demonstration of the host boundary.
It stores acknowledged writes in `commands.log`, a length-delimited journal
with CRC-32C and `sync_data` before applying the command. Restart recovery
replays that journal into the deterministic KV state machine. The journal is
not a replacement for the segmented WAL in the simulator crates.

## Start and inspect

```sh
cargo run -p cc-node --bin ccdb -- init --cluster demo --nodes 1 --base-dir /tmp/ccdb-demo
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n1/ccdb.toml
cargo run -p cc-node --bin ccdb -- admin --addr 127.0.0.1:7101 status
cargo run -p cc-node --bin ccdb -- selfcheck --data-dir /tmp/ccdb-demo/n1
```

The client listener accepts bounded RESP2 frames. The peer listener accepts
bounded `CCPF` frames from `cc-env`, checks their checksum, and echoes a
validated frame; it is a transport/framing seam, not a complete networked
Raft cluster. `metrics.prom` is refreshed by the host heartbeat and includes
command, write, fsync, and peer-frame counters.

## Fault work

`scripts/demo.sh` validates pipelined RESP and restart recovery. The bounded
`scripts/real-faults.sh` wrapper exercises that path and states its scope
explicitly. Page-cache loss, torn writes, partitions, and large campaigns must
use the deterministic simulator and the recorded campaign commands.
