# Lab in a box

Start the three-node real-host lab with:

```sh
docker compose up --build
redis-cli -p 7101 SET course crash
redis-cli -p 7102 GET course
curl http://127.0.0.1:7301/
```

Each node has its own durable volume and exposes a client and metrics port. The
peer ports stay on the Compose network, and peers are addressed by Compose
service name — `ccdb` resolves `host:port` at connect time, so a container that
comes back on a new address is still reachable.

This is a local teaching topology: it does not add TLS, authentication,
orchestration, or production hardening, and `ccdb` replicates over a static
primary/backup path rather than running the consensus core — see
[limitations](../docs/LIMITATIONS.md). Stopping the lowest-numbered node hands
the client path to the next one, which is failover by configuration, not an
election.

`ccdb@.service` is a systemd template for hosts where the binary and initialized
data directories are provisioned separately. Review its paths and user before
installing it.
