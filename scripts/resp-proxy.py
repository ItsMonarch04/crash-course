#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
"""Small userspace RESP byte proxy for local fault experiments."""

import argparse
import select
import socket
import time


def parse_endpoint(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def relay(client, upstream, drop_every, delay_ms):
    sockets = [client, upstream]
    forwarded = 0
    try:
        while True:
            ready, _, _ = select.select(sockets, [], [], 1.0)
            if not ready:
                continue
            for source in ready:
                data = source.recv(65536)
                if not data:
                    return
                target = upstream if source is client else client
                forwarded += 1
                if drop_every and forwarded % drop_every == 0:
                    continue
                if delay_ms:
                    time.sleep(delay_ms / 1000)
                target.sendall(data)
    finally:
        client.close()
        upstream.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1:7379")
    parser.add_argument("--upstream", default="127.0.0.1:7101")
    parser.add_argument("--drop-every", type=int, default=0)
    parser.add_argument("--delay-ms", type=int, default=0)
    args = parser.parse_args()
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(parse_endpoint(args.listen))
    listener.listen(16)
    print(f"resp-proxy listening={args.listen} upstream={args.upstream}", flush=True)
    while True:
        client, _ = listener.accept()
        upstream = socket.create_connection(parse_endpoint(args.upstream), timeout=2)
        relay(client, upstream, args.drop_every, args.delay_ms)


if __name__ == "__main__":
    main()
