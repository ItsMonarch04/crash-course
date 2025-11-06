# 04 — What the simulator cannot promise

The simulator models page cache, torn sectors, directional links, timer order,
and process lifecycle with excellent reproducibility. It cannot promise that a
real filesystem, scheduler, kernel, or NIC behaves identically. That boundary
is why the host has a restart demo and a peer-frame checksum path, and why the
long real-fault harness remains a separately recorded campaign.

The right workflow is complementary: use a seed to shrink a semantic failure
to a short trace, then reproduce the corresponding integration shape in the
real host. A green simulator campaign is evidence about the model and the
protocol; it is not a hardware certification.
