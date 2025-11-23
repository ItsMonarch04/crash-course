#!/usr/bin/env sh
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2025 Sidakpreet Singh
set -eu

node_id="${CCDB_NODE_ID:?CCDB_NODE_ID is required}"
client_port="${CCDB_CLIENT_PORT:?CCDB_CLIENT_PORT is required}"
peer_port="${CCDB_PEER_PORT:?CCDB_PEER_PORT is required}"
metrics_port="${CCDB_METRICS_PORT:?CCDB_METRICS_PORT is required}"
peers="${CCDB_PEERS:?CCDB_PEERS is required}"
cluster_id="${CCDB_CLUSTER_ID:?CCDB_CLUSTER_ID is required (32 lowercase hexadecimal characters)}"
data_dir="/var/lib/ccdb"

if [ ! -f "$data_dir/identity.ccid" ]; then
  ccdb init --cluster compose --cluster-id "$cluster_id" --node-id "$node_id" --data-dir "$data_dir"
fi
if [ ! -f "$data_dir/ccdb.toml" ]; then
  printf '[node]\nid = %s\ncluster_id = "%s"\ndata_dir = "%s"\nlisten_client = "0.0.0.0:%s"\nlisten_peer = "0.0.0.0:%s"\nlisten_metrics = "0.0.0.0:%s"\npeer_nodes = "%s"\n\n[storage]\nfsync = "always"\n' \
    "$node_id" "$cluster_id" "$data_dir" "$client_port" "$peer_port" "$metrics_port" "$peers" > "$data_dir/ccdb.toml"
fi

exec ccdb "$@"
