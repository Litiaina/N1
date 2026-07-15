# N2 Gateway

N2 Gateway is the user-facing front door for N2 clusters.

It is not storage authority. It does not own namespace truth, object metadata,
cluster placement, repair, rebalance, capacity admission, or multipart
resumability state. Its job is to route user traffic to healthy N2 nodes and
return the selected coordinator to the client.

## Deployment Shape

```text
users / browsers / UI
        |
        v
litiaina-n2-gateway
        |
        v
N2 node A / N2 node B / N2 node C / ...
```

The gateway can run on a dedicated machine:

```text
node 1: n2-gateway
node 2: n2
node 3: n2
node 4: n2
node 5: n2
```

N2 nodes remain directly usable for operators and native tools. The gateway is
for user-facing traffic and stable public endpoints.

## Protocol Boundary

External clients use HTTPS:

```text
https://gateway.example.com/noa/v1/...
```

Gateway hot paths use N2 native upstreams where possible:

```text
n2-gateway -> n2://node
```

Fragment auth is gateway-aware: the gateway tries available N2 nodes until a
valid fragment session is minted. Hot-path bearer traffic then uses native
upstreams where the gateway has a native operation for that route.

Fragment creation is also gateway-aware. `POST /noa/v1/fragments` provisions the
same stable fragment identity on every available N2 node, so later gateway
auth, uploads, reads, and maintenance do not depend on which node receives a
request.

## Multipart Routing

Multipart state is coordinator-owned by design, and coordinator memory is
client-owned. The gateway does not persist upload routes.

```text
POST init      -> gateway selects node A and returns coordinator_node_id=node-a
PUT part       -> client sends coordinator_node_id=node-a
GET status     -> client sends coordinator_node_id=node-a
POST finalize  -> client sends coordinator_node_id=node-a
DELETE cancel  -> client sends coordinator_node_id=node-a
```

The client stores the resumability pointer:

```text
session_id -> coordinator_node_id
version_id -> coordinator_node_id
```

The gateway stores neither upload routes nor object metadata.

Do not add a gateway upload/session database. Multipart resume state belongs
to the client and selected N2 coordinator. The gateway may route by the
client-supplied `coordinator_node_id`; it must not become cluster truth.

## Maintenance Job Routing

Cluster repair and rebalance jobs are also coordinator-owned. The gateway
starts the job on one healthy N2 node and returns that node as
`coordinator_node_id`. The client keeps that value and sends it on status and
stop requests.

```text
POST repair/rebalance start  -> gateway selects node A and returns coordinator_node_id=node-a
GET status                   -> client sends coordinator_node_id=node-a
POST stop                    -> client sends coordinator_node_id=node-a
```

The gateway does not store job state. The selected coordinator owns the local
job state, and N2 owns the distributed repair/rebalance work.

## Configuration

The gateway creates `n2_gateway_config.ini` on first run if missing.

```ini
[server]
host=0.0.0.0
https_port=51002
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[routing]
health_interval_seconds=5
request_timeout_seconds=300

[node.node-a]
http_url=https://127.0.0.1:50001
native_url=n2://127.0.0.1:50002
weight=1
```

Add one `[node.<id>]` section per N2 node.

## Node Setup

Every N2 node behind the gateway must already be a working N2 server. The
gateway can provision fragments across those nodes, but it does not format
disks or manage cluster membership.

Each node needs:

- HTTPS API enabled with `/noa/v1/...`
- native protocol enabled with `n2://`
- the same cluster membership view
- a unique `node_id`
- reachable HTTP and native ports from the gateway machine

Example N2 node config shape:

```ini
[server]
host=0.0.0.0
https_port=50001
native_port=50002
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[disk_config]
storage_disks=/mnt/n2/disk-00,/mnt/n2/disk-01
fs_storage_path=/mnt/n2/control

[cluster_config]
node_id=node-a
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
cluster_layout_mode=auto
auto_rebalance=false
```

`storage_disks` are unified N2 storage devices: they can hold immutable object
shards, immutable `.ncdx` records, and assigned namespace partitions.
`fs_storage_path` is local control state for membership, upload coordination,
and jobs; it is not a gateway database and not a namespace/index tier.

For a three-node cluster, the gateway config should point to each node:

```ini
[node.node-a]
http_url=https://192.168.0.22:50001
native_url=n2://192.168.0.22:50002
weight=1

[node.node-b]
http_url=https://192.168.0.13:50001
native_url=n2://192.168.0.13:50002
weight=1

[node.node-c]
http_url=https://192.168.0.18:50001
native_url=n2://192.168.0.18:50002
weight=1
```

Ports that must be reachable from the gateway:

```text
N2 HTTPS API: 50001/tcp
N2 native:    50002/tcp
```

User traffic only needs to reach the gateway HTTPS port:

```text
Gateway HTTPS: 51002/tcp
```

## Setup Order

1. Start each N2 node with its own `node_id`, disk roots, HTTP endpoint, and
   native endpoint.
2. Confirm each node is reachable directly:

   ```bash
   curl -k https://192.168.0.22:50001/noa/v1/ping
   ```

3. Confirm the native port is open from the gateway host:

   ```bash
   nc -vz 192.168.0.22 50002
   ```

4. Add every node to `n2_gateway_config.ini`.
5. Start `litiaina-n2-gateway`.
6. Confirm gateway routing health:

   ```bash
   curl -k https://127.0.0.1:51002/gateway/health
   curl -k https://127.0.0.1:51002/gateway/nodes
   ```

`/gateway/health` reports:

```text
healthy      at least one node is confirmed online
warming      nodes are configured and not failed yet, but probes have not passed
unavailable  every configured node is offline
```

## Build And Run

```bash
cargo build -p n2_gateway --release
target/release/litiaina-n2-gateway
```

## Gateway Admin API

```text
GET /gateway/health
GET /gateway/nodes
```

## Gateway Routed N2 API

These routes have gateway-specific routing rules:

```text
POST /noa/v1/fragments                         fan out fragment creation to all available nodes
POST /noa/v1/auth/fragment                     try available nodes until auth succeeds
POST /noa/v1/upload/one-shot/{object_key}      stream through native N2 to a selected coordinator
POST /noa/v1/upload/multipart/init/{object_key} select coordinator and return coordinator_node_id
PUT  /noa/v1/upload/multipart/{version}/{part} route to client-supplied coordinator_node_id
GET  /noa/v1/upload/multipart/{version}        route to client-supplied coordinator_node_id
POST /noa/v1/upload/multipart/{version}/{session}/finalize
DELETE /noa/v1/upload/multipart/{version}
POST /noa/v1/cluster/repair/start              select coordinator and return coordinator_node_id
GET  /noa/v1/cluster/repair/status             route to client-supplied coordinator_node_id
POST /noa/v1/cluster/repair/stop               route to client-supplied coordinator_node_id
POST /noa/v1/cluster/rebalance/start           select coordinator and return coordinator_node_id
GET  /noa/v1/cluster/rebalance/status          route to client-supplied coordinator_node_id
POST /noa/v1/cluster/rebalance/stop            route to client-supplied coordinator_node_id
GET  /noa/v1/objects/download                  stream through native N2 from a selected node
```

All other `/noa/v1/...` routes are proxied to an available N2 node.

These endpoints expose gateway routing health only.

## Supported Native Hot Paths

The gateway handles these paths through N2 native upstreams:

```text
POST   /noa/v1/upload/one-shot/{object_key}
POST   /noa/v1/upload/multipart/init/{object_key}
PUT    /noa/v1/upload/multipart/{version_id}/{part_index}
GET    /noa/v1/upload/multipart/{version_id}
POST   /noa/v1/upload/multipart/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/{version_id}
GET    /noa/v1/objects/download?object_key=...
POST   /noa/v1/cluster/repair/start
GET    /noa/v1/cluster/repair/status?coordinator_node_id=...
POST   /noa/v1/cluster/repair/stop?coordinator_node_id=...
POST   /noa/v1/cluster/rebalance/start
GET    /noa/v1/cluster/rebalance/status?coordinator_node_id=...
POST   /noa/v1/cluster/rebalance/stop?coordinator_node_id=...
```

Other `/noa/v1/...` routes are transparently proxied over HTTP to a healthy N2 node.

## Failure Behavior

- New stateless requests route to another healthy node.
- Existing multipart sessions remain sticky only when the client supplies the
  coordinator node returned by multipart init.
- Existing repair/rebalance jobs remain sticky only when the client supplies
  the coordinator node returned by job start.
- If the coordinator is unavailable, the gateway returns a clear unavailable
  response instead of inventing cross-node resume or cross-node job state.
- If the gateway restarts, there is no gateway upload or job route state to
  reload.

This keeps resumability strong without pushing temporary upload state into N2
cluster consensus.
