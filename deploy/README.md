# Lab in a box

Start the three-node real-host lab with:

```sh
docker compose up --build
redis-cli -p 7101 SET course crash
redis-cli -p 7101 GET course
curl http://127.0.0.1:7301/
```

Election decides which node leads, so any of the three ports may be the one
that answers. Reads and writes both go to the leader by default, and a node
that is not the leader replies `NOTLEADER leader=nX addr=HOST:PORT` rather than
serving the request — that error is the redirect, and following it by hand is
the point. `redis-cli -p 7102 GET course` shows it when n2 is a follower.
Reading from a follower on purpose is a separate, negotiated opt-in described
in [consistency](../docs/consistency.md).

Each node has its own durable volume and exposes a client and metrics port. The
peer ports stay on the Compose network, and peers are addressed by Compose
service name — `ccdb` resolves `host:port` at connect time, so a container that
comes back on a new address is still reachable.

The Compose file supplies one explicit non-secret `CCDB_CLUSTER_ID` for all
three nodes. The entrypoint creates a checksummed `identity.ccid` only for an
empty volume and then refuses any cluster-id/config mismatch. Its all-network
listeners require the visible unsafe-listener opt-in in the Compose command;
the lab protocols remain unauthenticated.

This is a local teaching topology: it does not add TLS, authentication,
orchestration, or production hardening. `ccdb` runs the shared Raft driver;
leader election and client redirects are real process behavior, but the lab is
still not a production deployment — see [limitations](../docs/LIMITATIONS.md).

`ccdb@.service` is a systemd template for hosts where the binary and initialized
data directories are provisioned separately. Review its paths and user before
installing it.
