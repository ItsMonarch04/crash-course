# 04 — What the simulator cannot promise

The simulator models page cache, torn sectors, directional links, timer order,
and process lifecycle with excellent reproducibility. It cannot promise that a
real filesystem, scheduler, kernel, or NIC behaves identically. The host now
has a three-node restart demo, quorum replication, peer-frame checksums, and a
TCP snapshot path. `real-faults.sh` adds a sustained SET workload, chunked
history checks, SIGSTOP/SIGCONT pauses, and delay/drop injection on a peer
path; those observations are still integration evidence, not hardware
certification.

The right workflow is complementary: use a seed to shrink a semantic failure
to a short trace, then reproduce the corresponding integration shape in the
real host. A green simulator campaign is evidence about the model and the
protocol; a green real-fault soak is evidence about this host implementation,
not a production deployment guarantee.
