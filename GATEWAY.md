# N2 Gateway

N2 Gateway is the user-facing front door for N2 clusters.

It is not storage authority. It does not own namespace truth, object metadata,
cluster placement, repair, rebalance, or quota enforcement. Its job is to route
user traffic to healthy N2 nodes and keep multipart upload resumability pointed
at the correct coordinator node.

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
https://gateway.example.com/n2/...
```

Gateway hot paths use N2 native upstreams where possible:

```text
n2-gateway -> n2://node
```

Auth/token routes remain HTTP passthrough because bearer tokens are minted by
the N2 HTTP API. Native N2 auth is connection-local.

## Multipart Routing

Multipart state is coordinator-owned by design.

```text
POST init      -> gateway selects node A
PUT part       -> gateway routes to node A
GET status     -> gateway routes to node A
POST finalize  -> gateway routes to node A
DELETE cancel  -> gateway routes to node A
```

The gateway stores only routing state:

```text
session_id -> coordinator_node_id
version_id -> coordinator_node_id
expires_at -> cleanup boundary
```

It does not store object metadata.

## Configuration

The gateway creates `n2_gateway_config.ini` on first run if missing.

```ini
[server]
host=0.0.0.0
https_port=51002
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[state]
upload_routes_path=./n2_gateway_upload_routes.json

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
gateway does not create fragments, format disks, or manage cluster membership.

Each node needs:

- HTTPS API enabled with `/n2/...`
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
index_store_disks=/mnt/n2/index
blob_store_disks=/mnt/n2/blob
fs_storage_path=/mnt/n2/fs

[cluster_config]
node_id=node-a
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
cluster_layout_mode=auto
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled
```

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
   curl -k https://192.168.0.22:50001/n2/ping
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
GET /gateway/routes/uploads
GET /gateway/routes/uploads/{session_id}
```

These endpoints expose gateway routing health only.

## Supported Native Hot Paths

The gateway handles these paths through N2 native upstreams:

```text
POST   /n2/upload/one-shot/{object_key}
POST   /n2/upload/multipart/init/{object_key}
PUT    /n2/upload/multipart/{version_id}/{part_index}
GET    /n2/upload/multipart/{version_id}
POST   /n2/upload/multipart/{version_id}/{session_id}/finalize
DELETE /n2/upload/multipart/{version_id}
GET    /n2/objects/download?object_key=...
```

Other `/n2/...` routes are transparently proxied over HTTP to a healthy N2 node.

## Failure Behavior

- New stateless requests route to another healthy node.
- Existing multipart sessions remain sticky to their coordinator.
- If the coordinator is unavailable, the gateway returns a clear conflict or
  unavailable response instead of inventing cross-node resume.
- If the gateway restarts, it reloads active upload routes from
  `n2_gateway_upload_routes.json`.

This keeps resumability strong without pushing temporary upload state into N2
cluster consensus.
