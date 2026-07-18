# N2 Documentation

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

This document is the user, operator, and API guide for N2. For the deeper
storage layout and consistency model, see [ARCHITECTURE.md](ARCHITECTURE.md).
For production service setup, systemd templates, and validation order, see
[INSTALLATION.md](INSTALLATION.md).

## Status

N2 `2.0.0` is an in-development engine boundary. It separates an independently
expandable namespace-node collection from an independently expandable blob-node
collection. The namespace uses stable directory inodes, exact replica layouts,
quorum-certified immutable generations, and bounded cross-inode transactions.
The blob plane retains immutable `.ncdx` records and object-level erasure sets.
Earlier development layouts are intentionally not opened by the 2.0 engine.

The July 17, 2026 independent-host `2 namespace + 2 blob` architectural proof
covered NOA CRUD, nested directories, same-parent rename, cross-parent move,
object-level `1 + 1` EC placement, exact node/root locators, byte-identical
reads, strict namespace and blob fail-closed behavior, recover-on-read, explicit
auto-healing, trash/restore invariants, and TTL reclamation. This is bounded
architectural evidence, not an LTS, long-soak, or general production-readiness
claim. The current evidence and remaining release gates are recorded in
`N2_2_0_VERIFICATION.md`.

## What N2 Is

N2 stores POSIX-like paths in the namespace pool and distributes immutable
shard/parity payloads through the blob pool. Every inode and object version
records the exact node and disk-root layout that committed it, so N2 never
guesses historical truth from current membership.

N2 is built around these rules:

- Any healthy node can coordinate a new operation.
- Namespace mutations touch only the directly affected recorded inode groups.
- Namespace and blob membership expand independently.
- Object payloads and object-version records are immutable after commit.
- New objects use the current cluster layout.
- Existing objects keep their recorded layout until explicit rebalance.
- Multipart upload session state is coordinator-owned until finalize.
- N2 exposes capacity monitoring and full-disk admission. Business-level
  per-user or per-fragment limits belong above N2.

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

The POSIX-style API is intentionally namespace control, not filesystem
emulation. Path operations stay bounded because they mutate or read stable
directory inodes; immutable object metadata and shard/parity payloads remain in
the blob plane and are not rewritten for list, rename, move, trash, or recover.

### Object Version

Each upload creates an immutable object version. The namespace head points to
the latest live version. Older versions can be listed, inspected, recovered, or
deleted by version where the API supports it.

### Namespace And Blob Layouts

Namespace redundancy and object EC width are separate settings. New directory
inodes select a fixed-width replica group from live namespace nodes. New object
versions select a bounded erasure set from live blob nodes. Both layouts record
exact node and root identities and remain unchanged when membership expands.

The absolute 2.0 minimum is two namespace nodes plus two blob nodes. With
namespace redundancy two, both namespace replicas are required for writes;
losing one namespace node makes affected groups read-only rather than risking a
split brain. A wider namespace redundancy can preserve write availability
through a majority.

N2 treats active blob nodes as the candidate pool for object-level erasure
placement. Each object uses a bounded erasure set selected from that pool.

`cluster_layout_mode=auto` derives a bounded layout from writable node count.
The default bound is `8 data + 3 parity`:

```text
1 writable node    -> 1 + 0
2 writable nodes   -> 1 + 1
3 writable nodes   -> 2 + 1
4 writable nodes   -> 3 + 1
10 writable nodes  -> 8 + 2
11+ writable nodes -> 8 + 3
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

Save an operator shell profile:

```bash
target/release/litiaina-n2-shell \
  --native-url n2://192.168.0.22:50002 \
  --fragment n2-test \
  --secret fragment-key \
  profile save --name lab --store-secret
```

Use the profile:

```bash
target/release/litiaina-n2-shell --profile lab dashboard
target/release/litiaina-n2-shell --profile lab dashboard --probe-objects
target/release/litiaina-n2-shell --profile lab repair watch
target/release/litiaina-n2-shell --profile lab rebalance watch
```

Profile storage is local to the operator account. Secrets are not saved unless
`--store-secret` is explicitly passed.

Run locally:

```bash
cargo run -p n2 --release
```

N2 creates missing local runtime files on first run. Production should use
explicit config and environment values.

Systemd service templates live under `deploy/systemd/`. They are templates, not
runtime config. Keep node-local `n2.env`, `n2_config.ini`, gateway config,
certificates, membership files, and disk roots on the machine that owns them.

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
- `MASTER_KEY`: fragment creation, fragment rename, and fragment delete
  authority.
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
storage_roots=ns-a-root=/mnt/n2/namespace
runtime_path=/mnt/n2/control

[cluster_config]
node_id=ns-a
node_role=namespace
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
namespace_redundancy=2
object_data_shards=1
object_parity_shards=1
auto_rebalance=false

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=2592000

[auto_healing]
enabled=true
initial_delay_seconds=30
scan_interval_seconds=900
coordinator_nodes=auto
max_parallel_fragments=2
max_objects_per_fragment_cycle=256
max_duration_seconds=30
lease_seconds=90
checksum_scan_every_cycles=24

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
max_part_size=536870912
data_shards=1
parity_shards=0
stripe_block_size=4194304
```

Notes:

- `storage_roots` is a comma-separated `ROOT_ID=/absolute/path` list owned by this logical
  node. Every root has a stable ID in membership and is bound to the node's
  single `namespace` or `blob` role.
- Namespace-node roots hold inode/catalog replicas. Blob-node roots hold
  immutable `.ncdx`, shard, and parity data. A logical node does not mix roles.
- `runtime_path` stores process-local membership, upload coordination, and
  maintenance job state. It is not namespace truth.
- `membership_file` is relative to `runtime_path` unless absolute.
- N2 2.0 production layouts should use fixed object EC. The membership policy
  is authoritative and never shrinks EC to fit an unsafe live topology.
- `auto_max_data_shards` and `auto_max_parity_shards` bound each object's
  erasure set so large clusters do not place one object across every node.
- `auto_rebalance=false` means new objects adapt to the current bounded
  layout, while old objects stay in their recorded layout until manual
  rebalance.
- `auto_rebalance=true` is rejected. Redistribution is always an explicit
  operation because older recorded layouts remain valid object truth.
- `[auto_healing]` controls bounded, restart-resumable object scanning.
  `coordinator_nodes=auto` selects half of the reachable nodes as an HA healer
  pool; one quorum lease owns each stable fragment at a time. Directory inodes
  are visited one at a time; very large namespace-walk continuation remains a
  2.0 release gate.
- Availability cycles detect missing/unreadable shards cheaply. Every
  `checksum_scan_every_cycles` cycle performs full checksum verification.
- `lease_seconds` is raised at runtime when necessary so it is longer than
  twice the configured object-work budget.
- N2 uses cluster capacity monitoring and physical free-space admission.
  Per-fragment business limits belong above N2 when needed.

### Role-Separated Storage Layout

Configure every device owned by this logical node:

```ini
[disk_config]
storage_roots=ns-a-root=/mnt/n2/namespace
runtime_path=/mnt/n2/control
```

The node role in membership decides what these roots may contain. Namespace
and blob collections expand independently, one node or disk at a time, without
remapping recorded layouts.

Each committed version is stored as:

```text
.objects/aa/bb/cc/dd/@o_<object-id>/
  <version-ulid>.<size>.<unix-nanos>.<packed-ext>.ncdx
  <version-ulid>/
    part_0_shard_0.shard
    part_0_shard_1.parity
```

The logical path exists only as an inode dentry targeting `@o_<object-id>`.
`.ncdx` stores immutable object identity and recorded cluster placement, not
the logical path. The maximum valid ULID filename is the head version.

Each configured root contains either namespace truth or blob truth according to
the node role. `runtime_path` is process-local control state for membership,
upload coordination, and maintenance jobs; it is neither plane's storage
truth. Older development layouts are not an in-place migration target.

## Membership File

N2 creates a local membership file when missing. For multi-node clusters, the
membership file should contain every node:

```json
{
  "format_version": 2,
  "cluster_id": "litiaina-n2-2-prod",
  "epoch": 1,
  "policy": {
    "namespace_redundancy": 2,
    "object_data_shards": 1,
    "object_parity_shards": 1
  },
  "namespace_catalog": {
    "generation": 1,
    "replicas": [
      { "node_id": "ns-a", "root_id": "ns-a-root" },
      { "node_id": "ns-b", "root_id": "ns-b-root" }
    ]
  },
  "nodes": [
    {
      "id": "ns-a",
      "role": "namespace",
      "endpoint": "https://192.168.0.22:50001",
      "native_endpoint": "n2://192.168.0.22:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [
        { "id": "ns-a-root", "path": "/mnt/namespace-a/n2" }
      ],
      "joined_epoch": 1
    },
    {
      "id": "ns-b",
      "role": "namespace",
      "endpoint": "https://192.168.0.13:50001",
      "native_endpoint": "n2://192.168.0.13:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [
        { "id": "ns-b-root", "path": "/mnt/namespace-b/n2" }
      ],
      "joined_epoch": 1
    },
    {
      "id": "blob-a",
      "role": "blob",
      "endpoint": "https://192.168.0.18:50001",
      "native_endpoint": "n2://192.168.0.18:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [
        { "id": "blob-a-root", "path": "/mnt/blob-a/n2" }
      ],
      "joined_epoch": 1
    },
    {
      "id": "blob-b",
      "role": "blob",
      "endpoint": "https://192.168.0.19:50001",
      "native_endpoint": "n2://192.168.0.19:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [
        { "id": "blob-b-root", "path": "/mnt/blob-b/n2" }
      ],
      "joined_epoch": 1
    }
  ]
}
```

All nodes use the identical membership document. Each node's local config names
its own `node_id` and only the roots assigned to that node. Root IDs are stable
identities; changing a mount path does not authorize rebinding a root identity
to another node or role.

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
mkdir -p /mnt/n2/storage /mnt/n2/control
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
- local storage/control roots;
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
storage_roots=ns-a-root=/mnt/n2/storage
runtime_path=/mnt/n2/control

[cluster_config]
node_id=ns-a
node_role=namespace
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
namespace_redundancy=2
object_data_shards=1
object_parity_shards=1
auto_rebalance=false
```

Node B and Node C use the same file with their own IDs and addresses.

### 3. Write The Membership File

Place the same membership file on every node under `runtime_path`, or use an
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
      "storage_roots": ["/mnt/n2/storage"]
    },
    {
      "id": "node-b",
      "endpoint": "https://192.168.0.13:50001",
      "native_endpoint": "n2://192.168.0.13:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": ["/mnt/n2/storage"]
    },
    {
      "id": "node-c",
      "endpoint": "https://192.168.0.18:50001",
      "native_endpoint": "n2://192.168.0.18:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": ["/mnt/n2/storage"]
    }
  ]
}
```

### 4. Start And Validate N2 Nodes

Start `litiaina-n2` on every node.

Validate process liveness:

```bash
curl -k https://192.168.0.22:50001/noa/v1/ping
curl -k https://192.168.0.13:50001/noa/v1/ping
curl -k https://192.168.0.18:50001/noa/v1/ping
```

Validate native ports from the gateway host:

```bash
nc -vz 192.168.0.22 50002
nc -vz 192.168.0.13 50002
nc -vz 192.168.0.18 50002
```

After authenticating, validate cluster health from any node:

```bash
curl -k https://192.168.0.22:50001/noa/v1/cluster/health \
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
curl -k https://gateway.example.com:51002/noa/v1/ping
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

Repair and rebalance through the gateway follow the same coordinator rule:

```text
start -> selected N2 node, response includes coordinator_node_id
status/stop -> client sends coordinator_node_id
```

The gateway is route-only. It does not store repair, rebalance, upload,
namespace, object, or cluster truth.

## Authentication

Most routes require:

```text
Authorization: Bearer <access_token>
```

Create a fragment access token:

```http
POST /noa/v1/auth/fragment
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
- `delete`: allow soft delete and fragment administration. Object reclamation
  remains TTL lifecycle-only.
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
POST /noa/v1/auth/refresh
Content-Type: application/json
```

```json
{
  "refresh_token": "..."
}
```

Logout refresh token:

```http
POST /noa/v1/auth/logout
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
GET /noa/v1/ping
```

Use this as the simplest process-level liveness check. Cluster-level readiness
uses `/noa/v1/cluster/health`.

## Fragment API

### Create Fragment

```http
POST /noa/v1/fragments
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

The stable fragment identity is generated by N2. Clients and gateways cannot
select or replace it.

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
GET /noa/v1/fragments
Authorization: Bearer <token>
```

Requires `read`.

### Update Fragment

```http
PATCH /noa/v1/fragments/{fragment}
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
DELETE /noa/v1/fragments/{fragment}
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
GET /noa/v1/fragments/check
GET /noa/v1/fragments/hash
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
POST /noa/v1/upload/one-shot/{object_key}?durability=auto
Authorization: Bearer <token>
Content-Type: video/mp4
Content-Length: <bytes>
```

Body is raw object bytes.

Query fields:

- `checksum`: optional CRC32 validation value. Accepts `deadbeef` or
  `crc32:deadbeef`. Headers `x-litiaina-checksum`, `x-n2-checksum-crc32`, and
  base64 `x-amz-checksum-crc32` are also accepted.
- `durability`: optional durability mode.
- `strict_durability`: optional boolean.
- `io_gate`: optional boolean.

Example:

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/noa/v1/upload/one-shot/videos/sample.mp4" \
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
POST /noa/v1/upload/multipart/init/{object_key}
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
PUT /noa/v1/upload/multipart/{version_id}/{part_index}
Authorization: Bearer <token>
Content-Length: <part-bytes>
```

Body is raw part bytes.

Query fields:

- `checksum`: optional CRC32 validation value. Accepts `deadbeef` or
  `crc32:deadbeef`. Headers `x-litiaina-checksum`, `x-n2-checksum-crc32`, and
  base64 `x-amz-checksum-crc32` are also accepted.
- `coordinator_node_id`: required when this request goes through N2 Gateway.
- `durability`: optional durability mode.
- `strict_durability`: optional boolean.
- `io_gate`: optional boolean.

Example:

```bash
curl -k -X PUT \
  "https://node-a:50001/noa/v1/upload/multipart/$VERSION_ID/0" \
  -H "$AUTH_HEADER" \
  --data-binary @part-000
```

Returns `200 OK` when accepted.

### Multipart Status

```http
GET /noa/v1/upload/multipart/{version_id}?coordinator_node_id=node-a
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
GET /noa/v1/upload/multipart?limit=100
Authorization: Bearer <token>
```

### Multipart Finalize

```http
POST /noa/v1/upload/multipart/{version_id}/{session_id}/finalize?object_key=videos/mhw.mp4&coordinator_node_id=node-a
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
DELETE /noa/v1/upload/multipart/{version_id}?object_key=videos/mhw.mp4&coordinator_node_id=node-a
Authorization: Bearer <token>
```

Cancels the coordinator-owned upload session and clears the pending namespace
binding when possible.

## Object Read API

### List Objects

```http
GET /noa/v1/objects?prefix=videos/&delimiter=/&limit=200&continuation_token=...
Authorization: Bearer <token>
```

Query fields:

- `prefix`: optional object key prefix.
- `delimiter`: optional delimiter for grouped object listing.
- `limit`: page size.
- `continuation_token`: next page token.

### List Trash

```http
GET /noa/v1/trash?prefix=videos/&limit=200
Authorization: Bearer <token>
```

### List Versions

```http
GET /noa/v1/objects/versions/{object_key}
Authorization: Bearer <token>
```

Returns all live versions for the object key. The namespace head is the latest
live version by ULID/version order.

### Metadata

```http
GET /noa/v1/objects/meta/{object_key}
Authorization: Bearer <token>
```

### Object Bytes

```http
GET /noa/v1/objects/get/{object_key}
GET /noa/v1/objects/stream/{object_key}
Authorization: Bearer <token>
```

Range reads use normal HTTP `Range` headers where supported by the route.

### Download

```http
GET /noa/v1/objects/download?object_key=videos/mhw.mp4&filename=mhw.mp4
Authorization: Bearer <token>
```

Query fields:

- `object_key`: required logical object key.
- `version_id`: optional explicit version.
- `filename`: optional download filename.

### Download Chunk

```http
GET /noa/v1/objects/download/chunk?object_key=videos/mhw.mp4&offset=0&chunk_size=8388608
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
POST /noa/v1/objects/signed-url
Authorization: Bearer <token>
```

Read signed URL:

```http
GET /noa/v1/objects/signed-read?token=...
```

Signed read is for temporary read access without exposing a fragment token.

## Namespace API

N2 exposes POSIX-style namespace commands under `/noa/v1/posix/...`. This is object
namespace control, not an operating-system filesystem.

### List Directory

```http
GET /noa/v1/posix/list?path=videos&limit=200&trash=false&continuation_token=...
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
GET /noa/v1/posix/stat?path=videos/mhw.mp4&trash=false
Authorization: Bearer <token>
```

### Make Directory

```http
POST /noa/v1/posix/mkdir
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
POST /noa/v1/posix/rename
POST /noa/v1/posix/move
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
POST /noa/v1/posix/trash
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
POST /noa/v1/posix/recover
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "path": "archive/mhw.mp4"
}
```

### Trash Lifecycle

N2 1.0.0 does not expose manual namespace purge or empty-trash semantics.
Trash is lifecycle-managed. Delete moves the namespace entry into reserved
trash state, recover moves it back while the retention period is valid, and GC
reclaims immutable object material only after the configured TTL proves no live
or trash namespace reference remains.

## Object Mutation API

These routes are object-oriented aliases around the same namespace truth.

### Rename Object

```http
POST /noa/v1/objects/rename
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
DELETE /noa/v1/objects/{object_key}
Authorization: Bearer <token>
```

### Recover Object

```http
POST /noa/v1/objects/recover/{object_key}
Authorization: Bearer <token>
```

### Bulk Delete And Recover

```http
POST /noa/v1/objects/bulk-delete
POST /noa/v1/objects/bulk-recover
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

### Trash Lifecycle Boundary

N2 1.0.0 intentionally has no empty-trash or recover-all namespace mutation.
Recover explicit entries before their retention period expires. Expired trash
is removed by lifecycle/GC after namespace reachability checks.

## Object Health And Repair

### Object Status

```http
GET /noa/v1/objects/status/{object_key}?mode=availability&version_id=01KS...
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
POST /noa/v1/objects/repair/{object_key}?mode=scrub
Authorization: Bearer <token>
```

Requires `write` and `modify`.

### Object Redistribution

```http
POST /noa/v1/maintenance/objects/redistribution/start/{object_key}?version_id=01KS...&dry_run=false&force=false
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
POST /noa/v1/objects/archive
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
POST /noa/v1/objects/archive/manifest?format=tar&filename=export.tar
Authorization: Bearer <token>
Content-Type: application/json
```

Body is a manifest accepted by the archive implementation.

## Capacity And Usage

### Node Capacity Usage

```http
GET /noa/v1/capacity/usage
Authorization: Bearer <token>
```

Returns local capacity usage.

Per-fragment usage is exposed through cluster usage aggregation, not through a
limit-management API.

This records monitoring metadata only in N2. It does not block uploads.

## Cluster API

### Membership

```http
GET /noa/v1/cluster/membership
Authorization: Bearer <token>
```

### Layout

```http
GET /noa/v1/cluster/layout
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
GET /noa/v1/cluster/health
Authorization: Bearer <token>
```

Cluster health summarizes membership, layout, node state, and cluster readiness.

### Detailed Cluster Monitor

```http
GET /noa/v1/cluster/monitor?fragment=n2-test&object_limit=96
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
  unified storage devices, local health status, upload counters, storage plane, disk
  status, and maintenance state.
- `topology`: current layout policy, current shard counts, object-level erasure
  layout flag, old-layout validity, and manual redistribution policy.
- `object_health`: sampled live object versions with current-layout count,
  older-recorded-layout count, placement mismatch count, redistributable count,
  optional availability probe counts, and representative samples.
- `redistribution`: targeted rebalance capability and current rebalance job.
  Already-current objects are skipped by design.
- `io_control`: peer read/write pressure, stalled peer count, active
  maintenance jobs, usage-cache state, p50/p95/p99 IO latency samples, recent
  slow-path events, process RSS/fd/thread snapshot, and read-on-recover
  counters.
- `io_control.scrub`: explicit scrub route, native operation, and recommended
  scheduling interval. N2 does not surprise weak nodes with automatic scrub IO.
- `jobs`: cluster repair, cluster rebalance, and local GC status.
- `namespace_consensus`: quorum size, voter count, local ledger summary, and
  namespace-consensus notes.
- `uploads`: aggregate active/stale multipart activity reported by nodes.

Normal console polling should use `/noa/v1/cluster/monitor`. Use
`/noa/v1/cluster/usage` only when an operator explicitly requests disk/fragment
usage totals.

### Usage

```http
GET /noa/v1/cluster/usage?fragment=n2-test
Authorization: Bearer <token>
```

This asks every readable node for usage and aggregates the result. N2 keeps a
short bounded local usage cache so explicit monitoring does not repeatedly walk
the same disks. Use `refresh=true` when the operator needs a fresh scan.

```http
GET /noa/v1/cluster/usage?fragment=n2-test&refresh=true
Authorization: Bearer <token>
```

Response includes:

- `capacity_admission_enabled: true`
- node-level usage results
- cluster total/used/available bytes
- fragment live/trash/physical bytes

Do not poll this aggressively. It can require node and disk scans when the
cache is cold or refresh is forced.

### Placement Preview

```http
GET /noa/v1/cluster/placement/preview?fragment=n2-test&object_key=videos/mhw.mp4&version_id=preview
Authorization: Bearer <token>
```

Shows which nodes would receive data/parity shards under the current membership
and layout.

### Membership Reload

```http
POST /noa/v1/cluster/membership/reload
Authorization: Bearer <token>
```

Runtime reload is intentionally disabled until safe live epoch transition is
implemented. Restart N2 after membership edits.

## Maintenance API

### System Health

```http
GET /noa/v1/maintenance/health
Authorization: Bearer <token>
```

### Fragment Storage Init And Cleanup

```http
POST /noa/v1/maintenance/fragment/init
POST /noa/v1/maintenance/fragment/cleanup
Authorization: Bearer <token>
```

Use these for operator-controlled fragment storage initialization or metadata
cleanup. Normal object upload does not require calling them manually when the
fragment is already provisioned correctly.

### Fragment Health

```http
GET /noa/v1/maintenance/fragments/{fragment}/health?mode=availability
Authorization: Bearer <token>
```

### GC

```http
POST /noa/v1/maintenance/gc/start?mode=availability&fast_mode=true
POST /noa/v1/maintenance/fragments/{fragment}/gc/start?mode=scrub&fast_mode=true
POST /noa/v1/maintenance/gc/stop
GET  /noa/v1/maintenance/gc/status
Authorization: Bearer <token>
```

GC reclaims headless object metadata and blobs after namespace/trash lifecycle
rules allow it.

### Repair

```http
POST /noa/v1/maintenance/repair/start?mode=availability&fast_mode=true
POST /noa/v1/maintenance/fragments/{fragment}/repair/start?mode=scrub&fast_mode=false
POST /noa/v1/maintenance/repair/stop
GET  /noa/v1/maintenance/repair/status
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
POST /noa/v1/maintenance/redistribution/start?mode=availability&fast_mode=true
POST /noa/v1/maintenance/fragments/{fragment}/redistribution/start?mode=strict
POST /noa/v1/maintenance/redistribution/stop
GET  /noa/v1/maintenance/redistribution/status
Authorization: Bearer <token>
```

Redistribution rewrites objects onto the current local disk layout where
applicable. Cluster-level rebalance is covered below.

## Cluster Repair And Rebalance

Cluster repair and rebalance are deliberately different. Repair reconstructs
missing or corrupt shards into the immutable placement recorded by the object;
it does not change EC shape, placement epoch, object version, or namespace.
Rebalance is an explicit distributed rewrite that commits a new immutable
version under the current layout.

### Autonomous Healing

```http
GET  /noa/v1/cluster/healing/status
POST /noa/v1/cluster/healing/cycle
Authorization: Bearer <token>
```

Every node runs the scheduler, but only nodes in the deterministic bounded HA
pool can own work. A stable fragment maps to one pool member, and that member
must obtain a lease from the fragment catalog's namespace replica quorum before
scanning. The object cursor is persisted under
`runtime_path/.n2/auto-healing`, so bounded object work resumes after restart.
Independent fragments run in bounded parallel. Directory inodes are visited
one at a time; bounded continuation or lease renewal for a namespace walk that
outlives its lease remains an explicit release gate.

If a degraded object still has its recorded data-shard threshold, healing
restores it to the exact recorded holders. The same cycle walks stable
directory inodes, repairs missing or stale exact replicas from a certified
complete snapshot, and verifies every repair before recording success.

Every degraded inode or object produces an immutable JSON report under
`runtime_path/.reports/auto-heal/<fragment>/`. Report files are local,
disposable operational evidence: they are never replicated and are never used
as storage authority. Reports identify the fragment, inode or object, known
path, exact layout observations, repair attempt, and verified outcome.

An unrepairable inode or object, a dangling namespace pointer, divergent
immutable metadata, or an impossible recorded topology activates the durable
cluster safety read-only latch. User mutations, GC, and rebalance stop;
reconstructable reads and exact-layout repair remain available. A headless
published object is reported and preserved without tripping the latch because
blob publication intentionally precedes namespace publication and failed
finalize can leave safe orphan residue. Incomplete namespace traversal or blob
inventory defers headless classification instead of guessing.

The safety latch survives restart and is intentionally sticky. N2 2.0 does not
expose an unsafe delete-marker or force-clear API; a verified cluster-wide
proof-and-clear procedure remains a release gate.

### Cluster Repair Plan

```http
GET /noa/v1/cluster/repair/plan
Authorization: Bearer <token>
```

### Start Cluster Repair

```http
POST /noa/v1/cluster/repair/start
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
  "max_duration_seconds": 1800,
  "max_objects": 1000,
  "max_directories": 10000,
  "max_bytes": 107374182400,
  "continuation_token": null,
  "priority": "low"
}
```

Fields:

- `fragment`: required.
- `prefix`: optional object key prefix.
- `limit`: optional maximum objects for this run.
- `dry_run`: plan without rewrite.
- `max_duration_seconds`: optional wall-clock budget.
- `max_objects`: optional object budget.
- `max_bytes`: optional rewrite byte budget.
- `priority`: optional operator label, normally `low` for maintenance.

When this request is sent through N2 Gateway, the response includes
`coordinator_node_id`. Keep that value for repair status and stop.

### Stop Cluster Repair

```http
POST /noa/v1/cluster/repair/stop?coordinator_node_id=node-a
Authorization: Bearer <token>
```

`coordinator_node_id` is required when this request goes through N2 Gateway.

### Cluster Repair Status

```http
GET /noa/v1/cluster/repair/status?coordinator_node_id=node-a
Authorization: Bearer <token>
```

`coordinator_node_id` is required when this request goes through N2 Gateway.

### Cluster Rebalance Plan

```http
GET /noa/v1/cluster/rebalance/plan
Authorization: Bearer <token>
```

### Start Cluster Rebalance

```http
POST /noa/v1/cluster/rebalance/start
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
  "mismatched_only": true,
  "max_duration_seconds": 1800,
  "max_objects": 1000,
  "max_bytes": 107374182400,
  "priority": "low"
}
```

`mismatched_only` defaults to `true`. In that mode N2 scans object metadata and
only rewrites versions whose recorded epoch, EC shape, or deterministic node
placement differs from the current topology. Already-current objects are
skipped completely.

`max_duration_seconds`, `max_objects`, `max_directories`, and `max_bytes` are
optional budgets. They make long repair/rebalance runs controllable on weak
hardware and unstable networks without making rebalance automatic.

`continuation_token` resumes from a previous status token when an operator
wants to continue a stopped or budget-exhausted rewrite job.

When this request is sent through N2 Gateway, the response includes
`coordinator_node_id`. Keep that value for rebalance status and stop.

### Stop Cluster Rebalance

```http
POST /noa/v1/cluster/rebalance/stop?coordinator_node_id=node-a
Authorization: Bearer <token>
```

`coordinator_node_id` is required when this request goes through N2 Gateway.

### Cluster Rebalance Status

```http
GET /noa/v1/cluster/rebalance/status?coordinator_node_id=node-a
Authorization: Bearer <token>
```

`coordinator_node_id` is required when this request goes through N2 Gateway.

Job status shape:

```json
{
  "running": false,
  "job": "cluster_rebalance",
  "fragment": "n2-test",
  "dry_run": false,
  "mismatched_only": true,
  "resumable": true,
  "priority": "low",
  "cancel_requested": false,
  "budget_exhausted": false,
  "budget_reason": null,
  "budget": {
    "elapsed_ms": 5000,
    "objects": 100,
    "bytes": 1073741824,
    "directories": 0,
    "exhausted": false,
    "exhaustion_reason": null
  },
  "scanned": 100,
  "rewritten": 20,
  "skipped": 80,
  "skipped_current_layout": 80,
  "skipped_healthy": 0,
  "failed": 0,
  "bytes_processed": 1073741824,
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
  monitor --fragment n2-test --secret fragment-key \
  --object-limit 96 --probe-objects

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  usage --fragment n2-test --secret fragment-key --refresh

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  list --fragment n2-test --secret fragment-key --path videos --limit 200

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  upload --fragment n2-test --secret fragment-key \
  --object-key videos/sample.mp4 --file ./sample.mp4 --content-type video/mp4 \
  --crc32

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  object redistribute --fragment n2-test --secret fragment-key \
  --object-key videos/sample.mp4 --dry-run

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  rebalance start --fragment n2-test --secret fragment-key \
  --dry-run --mismatched-only --max-objects 100 --priority low

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  rebalance stop --fragment n2-test --secret fragment-key

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  gc status --fragment n2-test --secret fragment-key
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
object.list
object.meta
object.versions
object.status
object.repair
object.redistribute
object.delete
object.recover
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
cluster.repair.plan
cluster.repair.status
cluster.repair.start
cluster.repair.stop
cluster.rebalance.plan
cluster.rebalance.status
cluster.rebalance.start
cluster.rebalance.stop
maintenance.gc.start
maintenance.gc.stop
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
/noa/v1/ws
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
FRAGMENT_HASH=$(curl -sk -X POST https://node-a:50001/noa/v1/fragments \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "n2-test",
    "key": "123123",
    "secret": "'"$MASTER_KEY"'"
  }' | jq -r .fragment_hash)

TOKEN=$(curl -sk https://node-a:50001/noa/v1/auth/fragment \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment": "'"$FRAGMENT_HASH"'",
    "secret": "123123",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }' | jq -r .access_token)
```

### Upload A Large File With Multipart

```bash
curl -k -X POST "https://node-a:50001/noa/v1/upload/multipart/init/videos/mhw.mp4" \
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
curl -k "https://node-b:50001/noa/v1/posix/list?path=videos" \
  -H "Authorization: Bearer $TOKEN"

curl -k "https://node-c:50001/noa/v1/objects/download?object_key=videos/mhw.mp4" \
  -H "Authorization: Bearer $TOKEN" \
  -o mhw.mp4
```

### Monitor Cluster Usage

```bash
curl -k "https://node-a:50001/noa/v1/cluster/usage?fragment=n2-test" \
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

### Business Limits Do Not Belong In N2

Correct. N2 enforces physical capacity admission only. User or tenant business
limits belong in the service layer above N2.

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
N2_PROOF_STORAGE_LAYOUT=separated \
  scripts/bench/run_n2_hardening_native_proof.sh

N2_PROOF_STORAGE_LAYOUT=shared \
  scripts/bench/run_n2_hardening_native_proof.sh
```

Installer proof:

```bash
scripts/proof/run_n2_installer_proof.sh
```

Benchmark matrix:

```bash
FRAGMENT=n2-test SECRET=fragment-key \
  DIRECT_BASE=https://127.0.0.1:50001 \
  GATEWAY_BASE=https://127.0.0.1:51002 \
  NATIVE_URL=n2://127.0.0.1:50002 \
  scripts/bench/run_n2_benchmark_matrix.sh
```

Capacity expansion proof:

```bash
scripts/bench/run_n2_capacity_expansion_proof.sh
```

The hardening proof covers multipart resume on the coordinator, download from
each node, object status/repair, trash/recover, namespace stress, small-object
scale, cancel, canonical disk-layout checks, byte-identical `.ncdx` participant
copies, native placement locator checks, read-on-recover after deleting a
shard, node-loss read/list/rename behavior, targeted rebalance of a real
old-layout object, final cluster health, and RSS summaries. Unified-device mode
proves that each configured device can hold assigned namespace partitions and
immutable object material without a separate index role. The harness
only kills unrelated N2 processes when `N2_PROOF_ALLOW_PKILL=1` is explicitly
set.

For 1.0.0 readiness, prove at minimum:

- all nodes start with the same membership epoch;
- create fragment and authenticate;
- one-shot upload through each node;
- multipart upload through each node with coordinator stickiness;
- download from every node after finalize;
- object status from every node;
- namespace list, mkdir, rename, trash, TTL expiry, and restore-before-expiry;
- cluster health, layout, usage;
- cluster repair plan/status;
- cluster rebalance plan/status;
- distributed fragment/global repair and rebalance across real nodes;
- namespace race hardening for mkdir, rename, collision, trash, and recover;
- Shell ping, auth, health, membership, layout, list, usage, upload, download,
  object status/repair, cluster repair, cluster rebalance, dashboard, profile,
  and job watch flows.
