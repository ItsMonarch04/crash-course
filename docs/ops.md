# Operations notes

The real host is a local demonstration of the shared host boundary. `ccdb
init` creates a three-node configuration; each process starts a
`cc_host::Driver` over the same `cc_cluster::Node` used by the simulator and
theater. Raft durability records are written to `raft/wal.0` as bounded,
length-prefixed `cc-log` records. Every write is followed by `sync_data` before
the driver releases any dependent peer send or client reply. Recovery accepts a
verified prefix and truncates only a torn final frame; a complete corrupt frame
refuses service.

Peer sockets exchange the checked `CCHL` hello before bounded `CCPF` frames
carrying versioned `CCRP` Raft messages. Leadership is an election result, not
a lowest-node-id probe; before an election, RESP writes and reads return an
explicit `NOTLEADER` response. The adapter still lacks durable logical
snapshots and Storage v2, so a restarted follower relies on its retained Raft
log and later leader replication rather than claiming a complete wipe/reseed
recovery protocol. Read [limitations](LIMITATIONS.md) before interpreting a
real-host result beyond that boundary.

## Listener safety

`ccdb run` accepts loopback client, peer, and metrics bind addresses by
default. Binding a listener that resolves to a non-loopback address requires
`--i-know-this-is-unauthenticated`. The protocols are not authenticated or
encrypted; see [the threat model](threat-model.md) before using that opt-in.
After binding, an opted-in unsafe listener emits a structured warning with its
actual bound address.

## Cluster identity

`ccdb init` requires a nonzero `--cluster-id` written as exactly 32 lowercase
hexadecimal characters. It records the 16-byte value, node ID, default
cluster-policy hash, reader floors, lifecycle, and CRC in `identity.ccid`.
`ccdb run` reads and validates that record before opening `raft/wal.0` or
starting a listener; a different configuration ID, altered policy fence,
unsupported reader floor, corrupt record, or `Removed` lifecycle refuses
service. The data directory and its `identity.ccid` must not be symbolic
links. The former `node.json` data directory is deliberately refused rather
than partially converted, because its static-primary journal is outside the
post-N1 recovery boundary.

## Start and inspect

```sh
cargo run -p cc-node --bin ccdb -- init --cluster demo --cluster-id 00112233445566778899aabbccddeeff --nodes 3 --base-dir /tmp/ccdb-demo
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

For a compact deterministic receipt from one real-host run, provide a new
record path and replay it through the same shared Driver:

```sh
cargo run -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n1/ccdb.toml --record /tmp/n1.ccij
cargo run -p cc-swarm -- replay --file /tmp/n1.ccij --assert-effects
```

The recorder creates the receipt once, owner-readable only on Unix, and
syncs every transition frame. Treat it as sensitive operational data: it
contains the node's retained WAL, client data, replies, and topology. It also
captures every synchronous block read in request order, including returned
bytes, result tag, and service time; replay refuses to consult local block
storage for those reads. The current WAL-only adapter has no SST read path yet,
and snapshot images are still not captured, so this does not turn wipe/reseed
recovery into a supported claim. Its default 128 MiB limit can be changed with
`--record-max-bytes N`. Before that limit would be exceeded, it fsyncs a
`capped` footer and normally continues the node unrecorded; use
`--record-required` to exit nonzero instead if the receipt must cover the run.
Replay reports a capped or interrupted result as an effects-matching prefix,
not as evidence for the entire host lifetime.

For a bounded lab run that closes its recorder normally, add
`--run-for-ms N`. Its receipt has a synced `complete` footer. An externally
interrupted process remains an `interrupted` prefix, and fatal storage/host
paths retain their distinct termination labels.

Plain RESP writes are at-least-once across a lost reply or reconnect. A caller
that owns a stable retry identity can instead issue
`CC.REQUEST <client-u64> <sequence-u64> <write-command> [args...]`; it accepts
one state-changing command and returns the cached result for an exact retry.
Do not use connection ids as the client id, and allocate a fresh identity once
the policy-defined session lifetime has elapsed.

The client listener accepts bounded RESP2 frames. The peer listener first
negotiates `CCHL`, then accepts bounded `CCPF` frames from `cc-env`, checks
their checksum, and carries versioned `CCRP` Raft messages. Admin
status follows a follower's leader address to the current leader; the bench
driver does the same for real-host workloads. `listen_metrics` serves the
Prometheus text at `/metrics` and a
dependency-free live dashboard at `/`. A data-directory identity marker
prevents starting a configuration against another node's files. Deep
self-check validates identity/config agreement and a complete, CRC-checked
Raft WAL genesis, rejects a torn tail or incomplete snapshot staging, and
advises but never repairs.

## Fault work

`scripts/demo.sh` is the three-node exercise: it checks acknowledged writes,
kills the elected leader, and restarts a retained-log node. Wiped-node recovery
uses ordinary retained-log replication rather than an out-of-band state copy;
the streamed snapshot lifecycle remains future work.
`scripts/real-faults.sh` adds a sustained
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
