# `rust-toolchain.toml` pins an exact compiler version, and rustup honours it
# on the first cargo invocation — downloading that toolchain if the base image
# ships a different one. Pinning a version in this tag too would be decoration:
# it would be overridden anyway, and could drift from the real pin.
FROM rust:bookworm AS build
# Multi-stage: the build toolchain never reaches the runtime image.
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p cc-node

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 ccdb
COPY --from=build /src/target/release/ccdb /usr/local/bin/ccdb
COPY deploy/container-entrypoint.sh /usr/local/bin/ccdb-entrypoint
# Create and own the data directory *before* dropping privileges. Docker
# creates a missing volume mount point as root, so without this the entrypoint's
# first `mkdir` fails with EACCES as uid 10001 and every node dies on start.
RUN mkdir -p /var/lib/ccdb && chown ccdb:ccdb /var/lib/ccdb
USER ccdb
VOLUME ["/var/lib/ccdb"]
EXPOSE 7101 7201 7301
ENTRYPOINT ["/usr/local/bin/ccdb-entrypoint"]
