FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p cc-node

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 ccdb
COPY --from=build /src/target/release/ccdb /usr/local/bin/ccdb
COPY deploy/container-entrypoint.sh /usr/local/bin/ccdb-entrypoint
USER ccdb
VOLUME ["/var/lib/ccdb"]
EXPOSE 7101 7201 7301
ENTRYPOINT ["/usr/local/bin/ccdb-entrypoint"]
