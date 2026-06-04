# N2 Documentation

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

This document is the user, operator, and API guide for N2. For the deeper
storage layout and consistency model, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Status

N2 `1.0.0` is the architecture-final line for the distributed object storage
system. That means the core shape is set: `/n2/...` HTTP API, `n2://` native
protocol, distributed object placement, namespace coordination, cluster
monitoring, repair, rebalance, and N2 console/shell flows.

This is not an LTS claim yet. Long soak, larger cluster proof, node-loss
re-add proof, and wider UI viewport proof remain validation work.

## What N2 Is

N2 stores objects under namespace paths and distributes immutable shard/parity
payloads across cluster nodes. Every committed object version records the
layout that wrote it, so N2 can read, inspect, repair, and rebalance objects
without guessing from the current cluster shape.

N2 is built around these rules:

- Any healthy node can coordinate a new operation.
- Namespace mutations are quorum-applied.
- Object payloads and object-version records are immutable after commit.
- New objects use the current cluster layout.
- Existing objects keep their recorded layout until explicit rebalance.
- Multipart upload session state is coordinator-owned until finalize.
- Quota is monitoring-only inside N2. Enforcement belongs in an external
  business layer.

## Core Concepts

### Fragment

A fragment is the access and namespace boundary. In UI language it behaves like
a bucket or storage space. A fragment has a human name, a hashed alias, and a
stable fragment identity.

Fragment names can change. The stable identity is the storage truth.

### Object Key

An object key is the namespace path clients use:

```text
reports/2026/q1.pdf
videos/mhw.mp4
```

N2 supports object-style operations and POSIX-style namespace operations. Both
address the same namespace truth.

### Object Version

Each upload creates an immutable object version. The namespace head points to
the latest live version. Older versions can be listed, inspected, recovered, or
deleted by version where the API supports it.

### Cluster Layout

N2 treats active nodes as the cluster-level erasure layout.

`cluster_layout_mode=auto` derives the layout from writable node count:

```text
1 writable node  -> 1 + 0
2 writable nodes -> 1 + 1
3 writable nodes -> 2 + 1
4 writable nodes -> 3 + 1
```

`cluster_layout_mode=fixed` pins the layout to explicit `data + parity` counts.
Fixed layout is useful when operators want predictable durability even while
temporarily running with extra nodes.

### Coordinator-Owned Multipart

Multipart resumability is intentionally local to the coordinator node.

```text
new multipart init -> any healthy node
same multipart session -> same coordinator node until finalize
finalized object -> readable from any node
```

This keeps resumability strong without forcing every temporary part receipt
through cluster-wide consensus. The part data itself is still distributed
across the cluster during upload.

## Install And Build

Build the N2 server:

```bash
cargo build -p n2 --release
```

Build the N2 native shell:

```bash
cargo build -p n2_shell --release
```

Build the N2 Gateway:

```bash
cargo build -p n2_gateway --release
```

Server binary:

```text
target/release/litiaina-n2
```

Shell binary:

```text
target/release/litiaina-n2-shell
```

Gateway binary:

```text
target/release/litiaina-n2-gateway
```

Run locally:

```bash
cargo run -p n2 --release
```

N2 creates missing local runtime files on first run. Production should use
explicit config and environment values.

## Environment

N2 requires these environment values:

```bash
JWT_SECRET='replace-with-long-random-value'
AUTH_KEY='server-auth-key'
MASTER_KEY='cluster-master-key'
ADMIN_KEY='admin-key'
ACCESS_TOKEN_DEFAULT_TTL_SECS=900
ACCESS_TOKEN_MAX_TTL_SECS=3600
REFRESH_TOKEN_TTL_SECS=604800
```

Meaning:

- `JWT_SECRET`: signs access tokens.
- `AUTH_KEY`: server-level auth key for server-wide auth paths.
- `MASTER_KEY`: fragment creation, fragment rename, quota edit, and fragment
  delete authority.
- `ADMIN_KEY`: retained admin secret for admin surfaces.
- `ACCESS_TOKEN_*`: token lifetime controls.
- `REFRESH_TOKEN_TTL_SECS`: refresh-token lifetime.

## Configuration

Minimal `n2_config.ini`:

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
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled

[index_config]
partition_count=64

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=2592000

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting=true
auth_limit_per_second=2
auth_limit_burst_size=5
view_limit_per_second=30
view_limit_burst_size=100
upload_limit_per_second=300
upload_limit_burst_size=3000
global_limit_per_second=200
global_limit_burst_size=300

[upload_config]
max_concurrent_io=auto
adaptive_concurrency_enabled=true
adaptive_concurrency_interval_ms=auto
writer_buffer_size=auto
upload_memory_budget_bytes=auto
upload_part_strict_durability_default=false
upload_io_gate_enabled=true
stream_max_concurrency=auto
stream_chunk_size=auto
stream_readahead_bytes=auto
stream_locality_bias_enabled=true
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size=52428800
data_shards=1
parity_shards=0
stripe_block_size=262144
```

Notes:

- `index_store_disks` and `blob_store_disks` are comma-separated paths.
- `fs_storage_path` stores runtime control state such as membership and jobs.
- `membership_file` is relative to `fs_storage_path` unless absolute.
- `cluster_layout_mode=auto` is recommended for normal N2 deployments.
- `auto_rebalance=false` means new objects adapt to current layout, while old
  objects stay in their recorded layout until manual rebalance.
- N2 rejects fragment quota during creation. Use cluster usage monitoring.

## Membership File

N2 creates a local membership file when missing. For multi-node clusters, the
membership file should contain every node:

```json
{
  "cluster_id": "litiaina-n2-prod",
  "epoch": 1,
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "https://192.168.0.22:50001",
      "native_endpoint": "n2://192.168.0.22:50002",
      "state": "online",
      "weight": 1,
      "index_roots": ["/mnt/n2/index"],
      "blob_roots": ["/mnt/n2/blob"]
    },
    {
      "id": "node-b",
      "endpoint": "https://192.168.0.13:50001",
      "native_endpoint": "n2://192.168.0.13:50002",
      "state": "online",
      "weight": 1,
      "index_roots": ["/mnt/n2/index"],
      "blob_roots": ["/mnt/n2/blob"]
    },
    {
      "id": "node-c",
      "endpoint": "https://192.168.0.18:50001",
      "native_endpoint": "n2://192.168.0.18:50002",
      "state": "online",
      "weight": 1,
      "index_roots": ["/mnt/n2/index"],
      "blob_roots": ["/mnt/n2/blob"]
    }
  ]
}
```

Node states:

- `online`: readable and writable.
- `degraded`: readable and writable, but flagged for attention.
- `draining`: readable, not writable for new placement.
- `maintenance`: not used for active writes.
- `offline`: not used for reads or writes.

Membership reload is intentionally disabled at runtime. Restart N2 after editing
membership so the epoch transition is explicit and clean.

## Full Installation With Gateway

This is the normal production shape when N2 is exposed through one stable user
endpoint:

```text
users / UI / clients
        |
        v
litiaina-n2-gateway
        |
        v
N2 node A / N2 node B / N2 node C
```

The gateway is routing authority only. N2 nodes remain the storage authority.

### 1. Prepare Every N2 Node

On each N2 node:

```bash
mkdir -p /mnt/n2/index /mnt/n2/blob /mnt/n2/fs
```

Install the same N2 build on every node:

```bash
cargo build -p n2 --release
cargo build -p n2_shell --release
```

Each node must have:

- a unique `node_id`;
- reachable HTTPS API port;
- reachable native `n2://` port;
- local index/blob/runtime roots;
- the same membership file content and epoch.

Example per-node identity:

```text
node-a -> https://192.168.0.22:50001 / n2://192.168.0.22:50002
node-b -> https://192.168.0.13:50001 / n2://192.168.0.13:50002
node-c -> https://192.168.0.18:50001 / n2://192.168.0.18:50002
```

### 2. Write `n2_config.ini` Per Node

Each node uses the same config shape, but different `node_id`,
`node_endpoint`, `native_endpoint`, and disk roots when needed.

Node A:

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
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled
```

Node B and Node C use the same file with their own IDs and addresses.

### 3. Write The Membership File

Place the same membership file on every node under `fs_storage_path`, or use an
absolute `membership_file` path.

```json
{
  "cluster_id": "litiaina-n2-prod",
  "epoch": 1,
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "https://192.168.0.22:50001",
      "native_endpoint": "n2://192.168.0.22:50002",
      "state": "online",
      "weight": 1,
      "index_roots": ["/mnt/n2/index"],
      "blob_roots": ["/mnt/n2/blob"]
    },
    {
      "id": "node-b",
      "endpoint": "https://192.168.0.13:50001",
      "native_endpoint": "n2://192.168.0.13:50002",
      "state": "online",
      "weight": 1,
      "index_roots": ["/mnt/n2/index"],
      "blob_roots": ["/mnt/n2/blob"]
    },
    {
      "id": "node-c",
      "endpoint": "https://192.168.0.18:50001",
      "native_endpoint": "n2://192.168.0.18:50002",
      "state": "online",
      "weight": 1,
      "index_roots": ["/mnt/n2/index"],
      "blob_roots": ["/mnt/n2/blob"]
    }
  ]
}
```

### 4. Start And Validate N2 Nodes

Start `litiaina-n2` on every node.

Validate process liveness:

```bash
curl -k https://192.168.0.22:50001/n2/ping
curl -k https://192.168.0.13:50001/n2/ping
curl -k https://192.168.0.18:50001/n2/ping
```

Validate native ports from the gateway host:

```bash
nc -vz 192.168.0.22 50002
nc -vz 192.168.0.13 50002
nc -vz 192.168.0.18 50002
```

After authenticating, validate cluster health from any node:

```bash
curl -k https://192.168.0.22:50001/n2/cluster/health \
  -H "Authorization: Bearer $N2_TOKEN"
```

### 5. Configure The Gateway

Build the gateway on the gateway machine:

```bash
cargo build -p n2_gateway --release
```

Create or edit `n2_gateway_config.ini`:

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

Start:

```bash
target/release/litiaina-n2-gateway
```

Validate:

```bash
curl -k https://127.0.0.1:51002/gateway/health
curl -k https://127.0.0.1:51002/gateway/nodes
```

### 6. Use The Gateway Endpoint

Create fragments, authenticate, upload, list, download, repair, and rebalance
through the gateway endpoint:

```bash
curl -k https://gateway.example.com:51002/n2/ping
curl -k https://gateway.example.com:51002/gateway/health
```

Multipart uploads through the gateway are coordinator-sticky by client-owned
coordinator tracking:

```text
init -> selected N2 node, response includes coordinator_node_id
part/status/finalize/cancel -> client sends coordinator_node_id
finalized object -> readable from any N2 node
```

This is the preferred user-facing deployment because clients only need one
stable URL while N2 keeps its distributed node semantics internally. The
gateway does not store resumability routes.

## Authentication

Most routes require:

```text
Authorization: Bearer <access_token>
```

Create a fragment access token:

```http
POST /n2/auth/fragment
Content-Type: application/json
```

Body:

```json
{
  "fragment": "n2-test",
  "secret": "fragment-key",
  "read": true,
  "write": true,
  "modify": true,
  "delete": true,
  "expires_in": 900
}
```

Fields:

- `fragment`: fragment name hash or stable fragment identifier accepted by N2.
- `secret`: fragment key or `MASTER_KEY`.
- `read`: allow list, read, status, health.
- `write`: allow upload, mkdir, trash, recover.
- `modify`: allow rename, repair, rebalance, maintenance mutations.
- `delete`: allow permanent delete, purge, fragment delete.
- `expires_in`: optional access-token TTL in seconds, capped by env config.

Response:

```json
{
  "access_token": "...",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_token": "...",
  "refresh_expires_in": 604800
}
```

Refresh:

```http
POST /n2/auth/refresh
Content-Type: application/json
```

```json
{
  "refresh_token": "..."
}
```

Logout refresh token:

```http
POST /n2/auth/logout
Content-Type: application/json
```

```json
{
  "refresh_token": "..."
}
```

## Common Headers

Protected route:

```bash
AUTH_HEADER="Authorization: Bearer $N2_TOKEN"
```

Binary upload:

```text
Content-Type: application/octet-stream
Content-Length: <bytes>
```

Binary download routes return object bytes. JSON routes return JSON.

## Public Health

### Ping

```http
GET /n2/ping
```

Use this as the simplest process-level liveness check. Cluster-level readiness
uses `/n2/cluster/health`.

## Fragment API

### Create Fragment

```http
POST /n2/fragments
Content-Type: application/json
```

Body:

```json
{
  "name": "n2-test",
  "key": "fragment-key",
  "secret": "cluster-master-key"
}
```

Optional:

```json
{
  "stable_fragment_id": "optional-stable-id"
}
```

Do not pass `quota_bytes`. N2 rejects per-fragment quota at creation because
quota is monitoring-only in the distributed layer.

Response:

```json
{
  "fragment_hash": "ba121429ad16a87dadb2bcd989aa6159f11c9b754b3ff7cd72fb53204031982f",
  "stable_fragment_id": "01KS...",
  "name": "n2-test"
}
```

### List Fragments

```http
GET /n2/fragments
Authorization: Bearer <token>
```

Requires `read`.

### Update Fragment

```http
PATCH /n2/fragments/{fragment}
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "new_name": "renamed-fragment",
  "new_key": "new-fragment-key",
  "secret": "cluster-master-key"
}
```

Rules:

- Rename requires `MASTER_KEY`.
- Key update requires master authority or a token with `modify`.
- Fragment rename changes the alias, not the stable storage identity.

### Delete Fragment

```http
DELETE /n2/fragments/{fragment}
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "secret": "cluster-master-key",
  "confirm_fragment": "{fragment}"
}
```

Requires token `delete` and `MASTER_KEY`.

### Fragment Check And Hash

```http
GET /n2/fragments/check
GET /n2/fragments/hash
```

Body:

```json
{
  "fragment": "n2-test"
}
```

These routes are compatibility helpers. Normal clients should create a
fragment, authenticate it, then use the returned token.

## Object Upload API

### One-Shot Upload

Use for smaller files and simple clients.

```http
POST /n2/upload/one-shot/{object_key}?durability=auto
Authorization: Bearer <token>
Content-Type: video/mp4
Content-Length: <bytes>
```

Body is raw object bytes.

Query fields:

- `checksum`: optional checksum string for client tracking.
- `durability`: optional durability mode.
- `strict_durability`: optional boolean.
- `io_gate`: optional boolean.

Example:

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n2/upload/one-shot/videos/sample.mp4" \
  -H "$AUTH_HEADER" \
  -H "Content-Type: video/mp4" \
  --data-binary @sample.mp4
```

Response is object metadata:

```json
{
  "object_key": "videos/sample.mp4",
  "fragment": "01KS...",
  "version_id": "01KS...",
  "upload_session_id": "01KS...",
  "content_type": "video/mp4",
  "total_size_bytes": 1048576,
  "created_at": 1780000000,
  "parts": [],
  "is_delete_marker": false,
  "stripe_size_bytes": 262144
}
```

### Multipart Init

Use for large files and resumable uploads.

```http
POST /n2/upload/multipart/init/{object_key}
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "content_type": "video/mp4",
  "total_size_bytes": 17179869184,
  "expected_parts": 328,
  "part_size_bytes": 52428800
}
```

Response:

```json
{
  "session_id": "01KS...",
  "version_id": "01KS...",
  "coordinator_node_id": "node-a"
}
```

Keep the same coordinator node for every request in this multipart session.
When using N2 Gateway, include `coordinator_node_id` as a query field or
`x-n2-coordinator-node` header on part/status/finalize/cancel requests.

### Multipart Put Part

```http
PUT /n2/upload/multipart/{version_id}/{part_index}
Authorization: Bearer <token>
Content-Length: <part-bytes>
```

Body is raw part bytes.

Query fields:

- `checksum`: optional checksum string.
- `coordinator_node_id`: required when this request goes through N2 Gateway.
- `durability`: optional durability mode.
- `strict_durability`: optional boolean.
- `io_gate`: optional boolean.

Example:

```bash
curl -k -X PUT \
  "https://node-a:50001/n2/upload/multipart/$VERSION_ID/0" \
  -H "$AUTH_HEADER" \
  --data-binary @part-000
```

Returns `200 OK` when accepted.

### Multipart Status

```http
GET /n2/upload/multipart/{version_id}?coordinator_node_id=node-a
Authorization: Bearer <token>
```

Response:

```json
{
  "uploaded_parts": [0, 1, 2],
  "committed_parts": 3,
  "committed_bytes": 157286400,
  "expected_parts": 328,
  "total_size_bytes": 17179869184,
  "session_id": "01KS...",
  "version_id": "01KS...",
  "object_key": "videos/mhw.mp4"
}
```

### List Active Uploads

```http
GET /n2/upload/multipart?limit=100
Authorization: Bearer <token>
```

### Multipart Finalize

```http
POST /n2/upload/multipart/{version_id}/{session_id}/finalize?object_key=videos/mhw.mp4&coordinator_node_id=node-a
Authorization: Bearer <token>
Content-Type: application/json
```

Body repeats the upload shape:

```json
{
  "content_type": "video/mp4",
  "total_size_bytes": 17179869184,
  "expected_parts": 328,
  "part_size_bytes": 52428800
}
```

If parts are missing, N2 returns `409 Conflict`:

```json
{
  "uploaded_parts": [0, 1],
  "missing_parts": [2, 3]
}
```

If complete, N2 returns committed object metadata and clears the pending upload
binding.

### Multipart Cancel

```http
DELETE /n2/upload/multipart/{version_id}?object_key=videos/mhw.mp4&coordinator_node_id=node-a
Authorization: Bearer <token>
```

Cancels the coordinator-owned upload session and clears the pending namespace
binding when possible.

## Object Read API

### List Objects

```http
GET /n2/objects?prefix=videos/&delimiter=/&limit=200&continuation_token=...
Authorization: Bearer <token>
```

Query fields:

- `prefix`: optional object key prefix.
- `delimiter`: optional delimiter for grouped object listing.
- `limit`: page size.
- `continuation_token`: next page token.

### List Trash

```http
GET /n2/trash?prefix=videos/&limit=200
Authorization: Bearer <token>
```

### List Versions

```http
GET /n2/objects/versions/{object_key}
Authorization: Bearer <token>
```

Returns all live versions for the object key. The namespace head is the latest
live version by ULID/version order.

### Metadata

```http
GET /n2/objects/meta/{object_key}
Authorization: Bearer <token>
```

### Object Bytes

```http
GET /n2/objects/get/{object_key}
GET /n2/objects/stream/{object_key}
Authorization: Bearer <token>
```

Range reads use normal HTTP `Range` headers where supported by the route.

### Download

```http
GET /n2/objects/download?object_key=videos/mhw.mp4&filename=mhw.mp4
Authorization: Bearer <token>
```

Query fields:

- `object_key`: required logical object key.
- `version_id`: optional explicit version.
- `filename`: optional download filename.

### Download Chunk

```http
GET /n2/objects/download/chunk?object_key=videos/mhw.mp4&offset=0&chunk_size=8388608
Authorization: Bearer <token>
```

Query fields:

- `object_key`: required.
- `offset`: byte offset.
- `chunk_size`: requested chunk size.

Large clients should prefer streaming/range download over excessive chunk
parallelism.

### Signed Read

Mint signed URL:

```http
POST /n2/objects/signed-url
Authorization: Bearer <token>
```

Read signed URL:

```http
GET /n2/objects/signed-read?token=...
```

Signed read is for temporary read access without exposing a fragment token.

## Namespace API

N2 exposes POSIX-style namespace commands under `/n2/posix/...`. This is object
namespace control, not an operating-system filesystem.

### List Directory

```http
GET /n2/posix/list?path=videos&limit=200&trash=false&continuation_token=...
Authorization: Bearer <token>
```

Response:

```json
{
  "path": "videos",
  "scan_trash": false,
  "entries": [
    {
      "name": "mhw.mp4",
      "path": "videos/mhw.mp4",
      "kind": "file",
      "size_bytes": 17179869184,
      "modified_at_unix": 1780000000,
      "version_id": "01KS..."
    }
  ],
  "continuation_token": null
}
```

### Stat Path

```http
GET /n2/posix/stat?path=videos/mhw.mp4&trash=false
Authorization: Bearer <token>
```

### Make Directory

```http
POST /n2/posix/mkdir
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "path": "videos"
}
```

### Rename Or Move

```http
POST /n2/posix/rename
POST /n2/posix/move
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "source_path": "videos/mhw.mp4",
  "destination_path": "archive/mhw.mp4"
}
```

Rename and move are namespace mutations. They do not rewrite object payloads.

### Trash

```http
POST /n2/posix/trash
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "path": "archive/mhw.mp4"
}
```

### Recover

```http
POST /n2/posix/recover
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "path": "archive/mhw.mp4"
}
```

### Purge

```http
POST /n2/posix/purge
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "path": "archive/mhw.mp4"
}
```

Purge removes the namespace trash entry. Payload reclamation is handled by GC.

## Object Mutation API

These routes are object-oriented aliases around the same namespace truth.

### Rename Object

```http
POST /n2/objects/rename
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "source_fragment": "stable-fragment-id",
  "source_key": "videos/mhw.mp4",
  "destination_fragment": "stable-fragment-id",
  "destination_key": "archive/mhw.mp4"
}
```

`source_fragment` and `destination_fragment` must match the authenticated
fragment.

### Trash Object

```http
DELETE /n2/objects/{object_key}
Authorization: Bearer <token>
```

### Recover Object

```http
POST /n2/objects/recover/{object_key}
Authorization: Bearer <token>
```

### Permanent Delete Object

```http
DELETE /n2/objects/permanent/{object_key}
Authorization: Bearer <token>
```

### Delete Version

```http
DELETE /n2/objects/version/{version_id}/{object_key}
Authorization: Bearer <token>
```

### Recover Version

```http
POST /n2/objects/recover/version/{version_id}/{object_key}
Authorization: Bearer <token>
```

### Bulk Delete, Recover, Purge

```http
POST /n2/objects/bulk-delete
POST /n2/objects/bulk-recover
POST /n2/objects/permanent/bulk-delete
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "object_keys": [
    "videos/a.mp4",
    "videos/b.mp4"
  ]
}
```

Response:

```json
{
  "requested": 2,
  "unique": 2,
  "succeeded": 2,
  "not_found": 0,
  "failed": []
}
```

### Empty Trash

```http
DELETE /n2/trash/empty
POST /n2/trash/empty
Authorization: Bearer <token>
```

### Recover All Trash

```http
POST /n2/trash/recover-all
Authorization: Bearer <token>
```

## Object Health And Repair

### Object Status

```http
GET /n2/objects/status/{object_key}?mode=availability&version_id=01KS...
Authorization: Bearer <token>
```

Modes:

- `availability`: fast availability check.
- `checksum`: checksum-aware check.
- `strict`: deeper verification with read-only maintenance behavior.
- `scrub`: strictest repair/scrub behavior.

`version_id` is optional. When omitted, N2 reports the current namespace head.

Response:

```json
{
  "object_key": "videos/mhw.mp4",
  "version_id": "01KS...",
  "status": "healthy",
  "total_parts": 328,
  "total_shards": 984,
  "healthy_shards": 984,
  "missing_shards": 0,
  "corrupt_shards": 0,
  "repairable": true,
  "repaired_shards": 0,
  "failed_repairs": 0,
  "layout": {
    "recorded_membership_epoch": 7,
    "current_membership_epoch": 8,
    "recorded_data_shards": 2,
    "recorded_parity_shards": 1,
    "current_data_shards": 3,
    "current_parity_shards": 1,
    "recorded_total_shards": 3,
    "current_total_shards": 4,
    "active_write_nodes": 4,
    "layout_matches_current": false,
    "placement_matches_current": false,
    "state": "older_recorded_layout",
    "redistributable": true,
    "mismatch_reasons": [
      "membership_epoch recorded=7 current=8",
      "ec_layout recorded=2+1 current=3+1"
    ]
  }
}
```

An object can be `healthy` while its layout state is `older_recorded_layout`.
That means the object is valid under the topology that wrote it. It is not a
failure; it is eligible for operator-controlled redistribution.

### Object Repair

```http
POST /n2/objects/repair/{object_key}?mode=scrub
Authorization: Bearer <token>
```

Requires `write` and `modify`.

### Object Redistribution

```http
POST /n2/objects/redistribute/{object_key}?version_id=01KS...&dry_run=false&force=false
Authorization: Bearer <token>
```

Requires `write` and `modify`.

Object redistribution rewrites one object/version into the current cluster
topology. When `version_id` is omitted, N2 targets the current namespace head.
Already-current objects are skipped unless `force=true`.

Response:

```json
{
  "object_key": "videos/mhw.mp4",
  "physical_object_id": "9f2d...",
  "requested_version_id": "01KS...",
  "dry_run": false,
  "force": false,
  "rewritten": true,
  "skipped": false,
  "reason": null,
  "before": {
    "status": "healthy",
    "layout": {
      "state": "older_recorded_layout",
      "redistributable": true
    }
  },
  "after": {
    "status": "healthy",
    "layout": {
      "state": "current_layout",
      "redistributable": false
    }
  }
}
```

## Archives

### Selected Archive

```http
POST /n2/objects/archive
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "keys": ["videos/a.mp4", "docs/report.pdf"],
  "prefix": "exports/",
  "prefixes": ["photos/", "reports/"],
  "format": "tar",
  "filename": "export.tar",
  "export_all": false,
  "include_internal": false
}
```

### Manifest Archive

```http
POST /n2/objects/archive/manifest?format=tar&filename=export.tar
Authorization: Bearer <token>
Content-Type: application/json
```

Body is a manifest accepted by the archive implementation.

## Capacity And Usage

### Node Capacity Usage

```http
GET /n2/capacity/usage
Authorization: Bearer <token>
```

Returns local capacity usage.

### Fragment Quota Usage

```http
GET /n2/capacity/quota
Authorization: Bearer <token>
```

Returns fragment usage, but N2 does not enforce per-fragment quota.

### Resize Fragment Quota

```http
PATCH /n2/fragments/{fragment}/quota
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "quota_bytes": 0,
  "secret": "cluster-master-key"
}
```

This records monitoring metadata only in N2. It does not block uploads.

## Cluster API

### Membership

```http
GET /n2/cluster/membership
Authorization: Bearer <token>
```

### Layout

```http
GET /n2/cluster/layout
Authorization: Bearer <token>
```

Response includes:

- `membership_epoch`
- layout policy
- active writable node count
- derived or fixed `data_shards` and `parity_shards`
- `adaptive_without_rebalance`
- `automatic_rebalance`

### Health

```http
GET /n2/cluster/health
Authorization: Bearer <token>
```

Cluster health summarizes membership, layout, node state, and cluster readiness.

### Detailed Cluster Monitor

```http
GET /n2/cluster/monitor?fragment=n2-test&object_limit=96
Authorization: Bearer <token>
```

This is the preferred console/operator polling endpoint. It combines the
high-signal cluster state into one read-only snapshot without starting repair,
rebalance, GC, or disk usage scans.

Query fields:

- `fragment`: optional fragment to include object, namespace, and upload
  context. If omitted, the authenticated fragment is used.
- `prefix`: optional namespace prefix for the object-layout sample.
- `object_limit`: number of live namespace objects to sample. Default is `64`,
  maximum is `512`.
- `probe_objects`: optional full availability probe for sampled objects.
  Default is `false` because shard probing is heavier than layout inspection.
- `refresh`: bypass local cached health where supported.
- `deep_scan`: request deeper local node health sampling. Use manually, not as
  an aggressive UI poll.

Response sections:

- `overview`: cluster state, epoch, local node, writable/readable nodes, and
  current `data + parity` layout.
- `nodes`: configured node list with reachability, HTTP/native endpoints,
  index/blob roots, local health status, upload counters, storage plane, disk
  status, and maintenance state.
- `topology`: current layout policy, current shard counts, object-level erasure
  layout flag, old-layout validity, and manual redistribution policy.
- `object_health`: sampled live object versions with current-layout count,
  older-recorded-layout count, placement mismatch count, redistributable count,
  optional availability probe counts, and representative samples.
- `redistribution`: targeted rebalance capability and current rebalance job.
  Already-current objects are skipped by design.
- `jobs`: cluster repair, cluster rebalance, and local GC status.
- `namespace_consensus`: quorum size, voter count, local ledger summary, and
  namespace-consensus notes.
- `uploads`: aggregate active/stale multipart activity reported by nodes.

Normal console polling should use `/n2/cluster/monitor`. Use
`/n2/cluster/usage` only when an operator explicitly requests disk/fragment
usage totals.

### Usage

```http
GET /n2/cluster/usage?fragment=n2-test
Authorization: Bearer <token>
```

This asks every readable node for usage and aggregates the result.

Response includes:

- `quota_enforced: false`
- `monitoring_only: true`
- node-level usage results
- cluster total/used/available bytes
- fragment live/trash/physical bytes

Do not poll this aggressively. It can require node and disk scans.

### Placement Preview

```http
GET /n2/cluster/placement/preview?fragment=n2-test&object_key=videos/mhw.mp4&version_id=preview
Authorization: Bearer <token>
```

Shows which nodes would receive data/parity shards under the current membership
and layout.

### Membership Reload

```http
POST /n2/cluster/membership/reload
Authorization: Bearer <token>
```

Runtime reload is intentionally disabled until safe live epoch transition is
implemented. Restart N2 after membership edits.

## Maintenance API

### System Health

```http
GET /n2/maintenance/health
Authorization: Bearer <token>
```

### Fragment Storage Init And Cleanup

```http
POST /n2/maintenance/fragment/init
POST /n2/maintenance/fragment/cleanup
Authorization: Bearer <token>
```

Use these for operator-controlled fragment storage initialization or metadata
cleanup. Normal object upload does not require calling them manually when the
fragment is already provisioned correctly.

### Fragment Health

```http
GET /n2/maintenance/fragments/{fragment}/health?mode=availability
Authorization: Bearer <token>
```

### GC

```http
POST /n2/maintenance/gc/start?mode=availability&fast_mode=true
POST /n2/maintenance/fragments/{fragment}/gc/start?mode=scrub&fast_mode=true
POST /n2/maintenance/gc/stop
GET  /n2/maintenance/gc/status
Authorization: Bearer <token>
```

GC reclaims headless object metadata and blobs after namespace/trash lifecycle
rules allow it.

### Repair

```http
POST /n2/maintenance/repair/start?mode=availability&fast_mode=true
POST /n2/maintenance/fragments/{fragment}/repair/start?mode=scrub&fast_mode=false
POST /n2/maintenance/repair/stop
GET  /n2/maintenance/repair/status
Authorization: Bearer <token>
```

Useful query fields:

- `mode`: `availability`, `checksum`, `strict`, or `scrub`.
- `fast_mode`: `true` or `false`.
- `dry_run`: `true` or `false`.
- `fragment`: whole-cluster repair can be scoped by fragment.
- `read_only_fast`: optional read-only behavior in fast mode.

### Redistribution

```http
POST /n2/maintenance/redistribution/start?mode=availability&fast_mode=true
POST /n2/maintenance/fragments/{fragment}/redistribution/start?mode=strict
POST /n2/maintenance/redistribution/stop
GET  /n2/maintenance/redistribution/status
Authorization: Bearer <token>
```

Redistribution rewrites objects onto the current local disk layout where
applicable. Cluster-level rebalance is covered below.

### Trash Job Status

```http
GET /n2/maintenance/trash/recover/status
GET /n2/maintenance/trash/empty/status
Authorization: Bearer <token>
```

## Cluster Repair And Rebalance

Cluster repair and rebalance are distributed rewrite jobs.

### Cluster Repair Plan

```http
GET /n2/cluster/repair/plan
Authorization: Bearer <token>
```

### Start Cluster Repair

```http
POST /n2/cluster/repair/start
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "fragment": "n2-test",
  "prefix": "videos/",
  "limit": 1000,
  "dry_run": false
}
```

Fields:

- `fragment`: required.
- `prefix`: optional object key prefix.
- `limit`: optional maximum objects for this run.
- `dry_run`: plan without rewrite.

### Cluster Repair Status

```http
GET /n2/cluster/repair/status
Authorization: Bearer <token>
```

### Cluster Rebalance Plan

```http
GET /n2/cluster/rebalance/plan
Authorization: Bearer <token>
```

### Start Cluster Rebalance

```http
POST /n2/cluster/rebalance/start
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "fragment": "n2-test",
  "prefix": "videos/",
  "limit": 1000,
  "dry_run": false,
  "mismatched_only": true
}
```

`mismatched_only` defaults to `true`. In that mode N2 scans object metadata and
only rewrites versions whose recorded epoch, EC shape, or deterministic node
placement differs from the current topology. Already-current objects are
skipped completely.

### Cluster Rebalance Status

```http
GET /n2/cluster/rebalance/status
Authorization: Bearer <token>
```

Job status shape:

```json
{
  "running": false,
  "job": "cluster_rebalance",
  "fragment": "n2-test",
  "dry_run": false,
  "mismatched_only": true,
  "resumable": true,
  "scanned": 100,
  "rewritten": 20,
  "skipped": 80,
  "failed": 0,
  "started_at_unix": 1780000000,
  "finished_at_unix": 1780000100,
  "last_object": "videos/mhw.mp4",
  "continuation_token": null,
  "last_error": null
}
```

## Native Protocol

N2 native protocol is available through:

```text
n2://host:port
```

The native protocol is for low-latency N2-native clients and operators. It is
not a wrapper around a browser UI. It carries authenticated operation frames
and supports streamed upload/download operations.

Native shell examples:

```bash
target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 ping

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  auth --fragment n2-test --secret fragment-key

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  health --fragment n2-test --secret fragment-key

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  membership --fragment n2-test --secret fragment-key

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  layout --fragment n2-test --secret fragment-key

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  usage --fragment n2-test --secret fragment-key --target-fragment n2-test

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  list --fragment n2-test --secret fragment-key --path videos --limit 200

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  upload --fragment n2-test --secret fragment-key \
  --object-key videos/sample.mp4 --file ./sample.mp4 --content-type video/mp4

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  object redistribute --fragment n2-test --secret fragment-key \
  --object-key videos/sample.mp4 --dry-run
```

The shell supports raw native operation calls:

```bash
target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  raw cluster.health --fragment n2-test --secret fragment-key --payload '{}'
```

Common native operation names:

```text
namespace.list
namespace.stat
namespace.mkdir
namespace.rename
namespace.trash
namespace.recover
namespace.purge
object.list
object.meta
object.versions
object.status
object.repair
object.redistribute
object.delete
object.recover
object.purge
upload.one_shot
multipart.init
multipart.put_part
multipart.finalize
multipart.cancel
multipart.status
uploads.list
cluster.membership
cluster.layout
cluster.health
cluster.monitor
cluster.usage
cluster.repair.status
cluster.rebalance.plan
cluster.rebalance.status
cluster.rebalance.start
maintenance.gc.start
maintenance.gc.status
maintenance.repair.start
maintenance.repair.status
maintenance.redistribution.start
maintenance.redistribution.status
fragment.create
fragment.list
fragment.update
fragment.delete
capacity.usage
```

## Peer Routes

Peer routes are internal node-to-node APIs. They are not public client APIs.

Examples:

```text
/n2/peer/usage
/n2/peer/shards/...
/n2/peer/namespace/apply
```

Set `N2_CLUSTER_SECRET` in deployments that require peer-route shared-secret
protection.

## WebSocket Events

The WebSocket endpoint is:

```text
/ws
```

It is used by consoles to receive storage and namespace events. HTTP APIs are
the source of truth; WebSocket messages are notifications.

## Load Balancing

N2 does not require a storage-aware load balancer.

Recommended rules:

- Send new operations to any healthy node.
- Keep one multipart session sticky to the node that accepted `init`.
- Do not route based on shard placement.
- Do not make the load balancer responsible for N2 object ownership.

This preserves N2's bidirectional design while keeping temporary multipart
state simple and robust.

## Typical Workflows

### Create And Use A Fragment

```bash
curl -k -X POST https://node-a:50001/n2/fragments \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "n2-test",
    "key": "123123",
    "secret": "'"$MASTER_KEY"'"
  }'

TOKEN=$(curl -sk https://node-a:50001/n2/auth/fragment \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment": "n2-test",
    "secret": "123123",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }' | jq -r .access_token)
```

### Upload A Large File With Multipart

```bash
curl -k -X POST "https://node-a:50001/n2/upload/multipart/init/videos/mhw.mp4" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "content_type": "video/mp4",
    "total_size_bytes": 17179869184,
    "expected_parts": 328,
    "part_size_bytes": 52428800
  }'
```

Send all part/status/finalize calls for that session back to `node-a`.

### Read From Another Node After Finalize

```bash
curl -k "https://node-b:50001/n2/posix/list?path=videos" \
  -H "Authorization: Bearer $TOKEN"

curl -k "https://node-c:50001/n2/objects/download?object_key=videos/mhw.mp4" \
  -H "Authorization: Bearer $TOKEN" \
  -o mhw.mp4
```

### Monitor Cluster Usage

```bash
curl -k "https://node-a:50001/n2/cluster/usage?fragment=n2-test" \
  -H "Authorization: Bearer $TOKEN"
```

Run this on operator request. Do not poll it every second.

## Error Model

Common statuses:

- `200 OK`: operation completed.
- `201 Created`: fragment or upload created.
- `202 Accepted`: background maintenance accepted.
- `204 No Content`: logout or empty response.
- `400 Bad Request`: invalid fields or unsupported request.
- `401 Unauthorized`: missing or wrong secret/token.
- `403 Forbidden`: token lacks permission.
- `404 Not Found`: fragment, object, version, or path not found.
- `409 Conflict`: multipart finalize is incomplete or resource conflict.
- `429 Too Many Requests`: rate limited.
- `503 Service Unavailable`: saturated maintenance gate, peer unavailable, or
  cluster route cannot complete.

Errors are returned as HTTP errors with operator-readable text or JSON,
depending on the route.

## Troubleshooting

### Multipart Upload Resumes Fail On A Different Node

This is expected if the client does not preserve the coordinator. Multipart
temporary state is coordinator-owned. Direct-node clients should keep using the
node that returned the `session_id` and `version_id`; gateway clients should
store and send the returned `coordinator_node_id`.

### Fragment Quota Does Not Stop Uploads

Correct. N2 quota is monitoring-only. Use an external business layer for user
quota enforcement.

### Old Objects Do Not Move After Adding A Node

Correct. New objects use the current layout. Old objects keep their recorded
layout until manual rebalance.

### Cluster Usage Is Slow

Cluster usage asks readable nodes for usage and aggregates the result. It is an
operator monitoring action, not a high-frequency UI polling route.

### Namespace Rename Is Fast

Correct. Rename and move are namespace mutations. N2 does not rewrite object
payloads just because a path changed.

### Unexpected Internal Entries Appear In Listings

N2 user-facing listings should expose namespace entries only. Object metadata
under `.objects`, uploads, trash internals, locks, repair state, GC state, and
other control directories must not appear as user files. If a console shows an
internal control path, treat that as a UI/API filtering bug.

## Validation Commands

```bash
cargo check -p n2 -p n2_shell -p n2_native_client -p n2_gateway
cargo test -p n2 --lib
cargo test -p n2_shell
cargo test -p n2_native_client
cargo test -p n2_gateway
```

Native hardening proof:

```bash
scripts/bench/run_n2_hardening_native_proof.sh
```

For 1.0.0 readiness, prove at minimum:

- all nodes start with the same membership epoch;
- create fragment and authenticate;
- one-shot upload through each node;
- multipart upload through each node with coordinator stickiness;
- download from every node after finalize;
- object status from every node;
- namespace list, mkdir, rename, trash, recover, purge;
- cluster health, layout, usage;
- cluster repair plan/status;
- cluster rebalance plan/status;
- Shell ping, auth, health, membership, layout, list, usage, upload, download,
  object status/repair, cluster repair, and cluster rebalance.
