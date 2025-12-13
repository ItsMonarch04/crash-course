# Operations notes

The real host is a local demonstration of the shared host boundary. `ccdb init`
creates a three-node configuration; each process starts a
`cc_host::Driver` over the same `cc_cluster::Node` used by the simulator and
theater. Raft durability records are written to `raft/wal.0` as bounded,
length-prefixed `cc-log` records. Every write is followed by `sync_data` before
the driver releases any dependent peer send or client reply. Recovery accepts a
verified prefix and truncates only a torn final frame; a complete corrupt frame
refuses service.

Peer sockets exchange the checked `CCHL` hello before bounded `CCPF` frames
carrying versioned `CCRP` Raft messages. Leadership is an election result, not
a lowest-node-id probe; before an election, RESP writes and reads return an
explicit `NOTLEADER` response. The adapter has durable, streamed logical
checkpoints tied to an fsynced Raft snapshot mark, a checksummed Storage v2
manifest/store WAL, and ordered Raft prefix reclamation after the checkpoint
mark and directory sync are durable.
Read [limitations](LIMITATIONS.md) before interpreting a real-host result
beyond that boundary.

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
cargo run --locked -p cc-node --bin ccdb -- init --cluster demo --cluster-id 00112233445566778899aabbccddeeff --nodes 3 --base-dir /tmp/ccdb-demo
cargo run --locked -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n1/ccdb.toml
cargo run --locked -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n2/ccdb.toml
cargo run --locked -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n3/ccdb.toml
cargo run --locked -p cc-node --bin ccdb -- admin --addr 127.0.0.1:7102 status
cargo run --locked -p cc-node --bin ccdb -- selfcheck --data-dir /tmp/ccdb-demo/n1
cargo run --locked -p cc-node --bin ccdb -- selfcheck --deep --data-dir /tmp/ccdb-demo/n1
cargo run --locked -p cc-node --bin ccdb -- doctor --data-dir /tmp/ccdb-demo/n1
cargo run --locked -p cc-node --bin ccdb -- admin backup --data-dir /tmp/ccdb-demo/n1 --output /tmp/n1.ccbk
cargo run --locked -p cc-node --bin ccdb -- admin restore --input /tmp/n1.ccbk --data-dir /tmp/ccdb-restored/n1 --new-cluster-id 11112233445566778899aabbccddeeff --new-node-id 1
```

`backup` exports the one CCSN file named by a durable snapshot mark, so a data
directory that has never run has nothing to export. `restore` always writes a
fresh identity — the new cluster id and node id are required, never inherited
from the archive — because this is fresh-cluster recovery, not an in-place
restore of the source cluster.

For a compact deterministic receipt from one real-host run, provide a new
record path and replay it through the same shared Driver:

```sh
cargo run --locked -p cc-node --bin ccdb -- run --config /tmp/ccdb-demo/n1/ccdb.toml --record /tmp/n1.ccij
cargo run --locked -p cc-swarm -- replay --file /tmp/n1.ccij --assert-effects
```

The recorder creates the receipt once, owner-readable only on Unix, and
syncs every transition frame. Treat it as sensitive operational data: it
contains the node's retained WAL, client data, replies, and topology. It also
captures every synchronous block read in request order, including returned
bytes, result tag, and service time; replay refuses to consult local block
storage for those reads. The boot image embeds the verified Raft and store-WAL
prefixes plus the exact logical checkpoint named by a durable snapshot mark;
it does not copy ambient SST or manifest paths. Its default 128 MiB limit can
be changed with
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

## Metrics contract

`GET /metrics` takes one bounded copy of Driver, store, membership, and peer
state and releases the Driver lock before rendering. The response is capped at
64 KiB; if the cap would be exceeded it returns only `ccdb_up` and
`ccdb_metrics_overflow`. The only dynamic labels are numeric `node_id`, the
bounded manifest `level`, and fixed `resource`/`kind` vocabularies. Keys,
client/request identifiers, file names, addresses, and error strings are never
labels.

Names ending in `_total` are monotonically increasing process-lifetime
counters and reset only when that process restarts. Byte counters count the
requested or successfully published physical bytes named by the metric;
`ccdb_block_*` is the SST block-source boundary and `ccdb_file_*` is the
host-effect file boundary. Bloom positive/negative counters record the actual
retained-filter decision. Manifest rewrites and compaction
started/completed/aborted counters advance at their respective store
transitions. Snapshot created/sent/received/aborted counters count a terminal
transfer/publication event once per peer transfer id, not chunks or retries.
Expiry proposals count replicated purge entries; expiry keys count keys only
after their store durability continuation succeeds.

The following are gauges and may move in either direction:

- `ccdb_store_files{level}` and `ccdb_store_file_bytes{level}` are current
  physical file count and bytes.
- `ccdb_footprint_bytes{resource,kind}` exports `current`, process `peak`, and
  admission `limit` for every byte-valued `NodeFootprint` resource. The
  companion `ccdb_footprint_items` values and `ccdb_driver_blocked` are current
  queue/timer state.
- `ccdb_storage_fault` is zero during healthy service and is set before the
  fatal storage path. A fatal process may exit before another scrape observes
  it; the termination receipt remains authoritative.
- Peer gauges report negotiated semantic version and feature bits, leader-side
  match index and commit lag (entries), and last successful contact age
  (seconds). An age of `-1` means this process has not observed a successful
  contact. Node-id cardinality is bounded by the configured membership policy.
- `ccdb_up`, node/leader ids, role, commit/applied index, configured peers, and
  uptime are current process gauges.

## Fault work

`scripts/demo.sh` is the three-node exercise: it checks acknowledged writes,
kills the elected leader, and restarts a retained-log node. When a follower
backs up to the log origin, the shared Driver can send a capped stop-and-wait
CCSN transfer from a durable, marked checkpoint instead of an out-of-band
state copy.
`scripts/demo-membership.sh` is the five-process membership gate. By default
it runs 20 independently port-isolated 3→5 cycles with forced checkpoint
catch-up, learner promotion, address publication, leader transfer, voter
removal, terminal-node exit, final probes, and one checked CC-HISTORY per run.
Set `CC_MEMBERSHIP_RUNS=1..20` only to shorten local diagnosis. CI runs the
same 20-cycle command from `.github/workflows/campaigns.yml`. The test-only
`CCDB_SNAPSHOT_AFTER_BYTES` environment variable lowers the checkpoint trigger
within this lab; production configuration continues to use bounded host
defaults.
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
