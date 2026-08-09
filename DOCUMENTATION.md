# N2 Documentation

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

This is the complete user, operator, and API guide. For the storage layout
and invariants, see [ARCHITECTURE.md](ARCHITECTURE.md). For production
systemd installation, see [INSTALLATION.md](INSTALLATION.md). For the
versioned public contract, see [NOA.md](NOA.md).

## Status

N2 `3.4.1` uses the clean N2 3.x engine and disk-format generation. N2 3.0
replaced the 2.x distributed-inode namespace with a flat replicated NCNX key
plane. N2 3.1 adds collision-free per-mutation trash generation directories.
N2 3.2 makes SHA-256 the only engine integrity checksum, adds SHA-256
integrity to NCNX and object NCDX, derives exact placements from immutable
server-pool membership, and adds replica-backed degraded listing. It retains
immutable NCDX object metadata, immutable shards and parity, exact recorded
per-object EC layouts, bidirectional coordination, strict publication,
explicit redistribution, and autonomous healing.

N2 3.3 adds autonomous Reserve Set replacement. Format-v5 membership separates
stable logical pool slots from physical instances, records a distinct binding
epoch, keeps reserve instances outside placement, and fences retired
instances. A promoted reserve preserves the exact pool, EC, slot, logical
root, and old object layouts while bounded healing restores historical NCNX,
NCDX, shard, and parity placement.

N2 3.3.1 hardens autonomous maintenance under node churn. A temporarily
unreachable NCNX or NCDX participant is recorded as incomplete verification
and retried by a later healing cycle; it is not treated as proof of immutable
loss. Confirmed object-local missing, divergent, corrupt, or unrepairable state
produces an exact report without globally latching unrelated keys read-only;
systemic identity and membership failures retain the safety latch.

The survival write path retains the configured layout and can acknowledge at
the EC reconstruction threshold: `K` EC pieces, `K` NCDX copies, and one of
`M + 1` NCNX replicas. Missing exact slots remain deterministic healing work;
writes reject below `K`. The upload existence check follows that same
reachable-replica boundary. Normal reads and destination-first rename, move,
trash, and recover retain their stricter namespace rules.

N2 3.4.1 makes this fixed-layout survival behavior the current release
contract. It is not quorum consensus: the configured server-pool generation,
logical slots, shard roles, NCDX holders, and NCNX repair targets remain
unchanged while degraded writes acknowledge only at the reconstruction
threshold.

N2 3.4 intentionally does not open or migrate N2 1.x, 2.x, or earlier
experimental 3.x roots. Static server-pool enrollment, root identity, and
format-v5 membership prevent old layouts from being silently reinterpreted.

N2 3.4 deliberately fails closed on retired NCNX, NCDX, and trash layouts
instead of maintaining multiple integrity and lifecycle formats.

The current qualification is bounded evidence, not an LTS declaration or an
exabyte deployment claim. Exact passed, failed, and unexercised boundaries
are recorded in [TESTING.md](TESTING.md). Performance measurements are
recorded separately in [BENCHMARK.md](BENCHMARK.md).

## What N2 Is

N2 distributes immutable erasure-coded object versions through unified static
server pools and publishes each user key as a small immutable NCNX record.
Every committed object records the exact pool generation, node IDs, root IDs,
shard roles, sizes, and checksums that wrote it. N2 never guesses historical
truth from current membership.

N2 is built around these rules:

- Any healthy node can coordinate a new operation.
- Every enrolled node root can hold NCNX, NCDX, shards, and parity.
- A pool's `K + M`, node set, and root-ID set are static after enrollment.
- NCNX replica count is `R = M + 1` in the key's home pool.
- Object payloads, NCDX, and NCNX records are immutable after publication.
- Exact-key mutation is destination-first copy/delete of NCNX only.
- Prefix operations are bounded per-key operations, not one global lock.
- New objects use the newest complete writable pool.
- Existing objects retain their recorded layout until explicit redistribution.
- Repair restores a recorded layout and never changes EC implicitly.
- Multipart state is coordinator-owned only until finalize.
- Unsafe namespace or object publication fails closed.

## Core Concepts

### Fragment

A fragment is the authentication, lifecycle, listing, policy, and maintenance
boundary. In UI language it behaves like a bucket or virtual object
partition. A fragment has a human alias and a stable fragment identity.

Fragment names can change. The stable identity is storage truth. A fragment
is not a server pool: its objects may retain layouts from several pool
generations as the cluster expands.

### Object Key

An object key is the logical name used by clients:

```text
reports/2026/q1.pdf
videos/mhw.mp4
```

On an NCNX holder those keys are physical records:

```text
.namespace/reports/2026/q1.pdf.ncnx
.namespace/videos/mhw.mp4.ncnx
```

The physical directories are prefix partitioning only. They are not inodes,
locks, replica groups, or mutable parent records. Mutating
`videos/mhw.mp4` never changes shared state for another key under `videos/`.

N2 exposes object operations and POSIX-like path operations over the same
flat namespace truth. Empty directories are not stored. Prefix/folder
operations enumerate matching keys and apply exact-key copy/delete with
bounded concurrency.

### Object Version

Every upload creates an immutable version. NCDX records its exact layout and
the object ID remains stable across namespace rename, move, trash, and
recover. The newest live version is selected by version ULID ordering rather
than a mutable latest-pointer file.

### Namespace And Blob Layouts

N2 3.4 uses unified server-pool roots rather than namespace-only and
blob-only node roles. The namespace and payload remain separate logical
planes even though they share enrolled storage roots:

- NCNX maps a logical key to a stable object ID.
- NCDX records an immutable object version and exact EC layout.
- shard and parity files contain immutable encoded bytes.

A static `K + M` server pool requires exactly `K + M` distinct node failure
domains for safe new publication. N2 counts nodes, not disks. NCNX uses
`M + 1` deterministic holders in its home pool. The smallest supported pool
is `1 + 1`; production sizing must reflect the operator's required failure
domain and current qualification uses `2 + 2`.

Within one node, a recorded root ID is a deterministic preference rather than
a permanent disk binding. Reads search the node's enrolled roots. If one disk
is unavailable or full, writes and exact-layout healing fall back to another
identity-verified writable root on the same node. The cluster reports degraded
disk health without discarding that node's EC role; the node stops accepting
writes only when none of its enrolled roots can safely accept them.

Capacity expansion adds another complete pool generation. It does not add a
node into an enrolled pool, remap existing NCNX, or rewrite old objects.

### Coordinator-Owned Multipart

N2 exposes two coordinator-owned multipart contracts:

- Standard multipart is the default high-throughput path. Its compact session
  and completed-part map live only in coordinator memory. Parts are independent
  and may be processed concurrently. Restart loses unfinished resumability.
- Durable multipart uses the same EC data path while retaining the existing
  restart-proof manifest, pending binding, and per-part receipts.

Both contracts remain sticky to one coordinator until finalize:

```text
new multipart init  -> any healthy node
same upload session -> same coordinator until finalize
finalized object    -> readable through any healthy node
```

Standard multipart writes no temporary receipt replicas and defers shard sync
until finalize. Finalize synchronizes the exact shard holders before publishing
NCDX, then publishes NCNX, converting temporary parts into cluster object
truth. Durable multipart keeps per-part durability for clients that explicitly
prefer restart-proof progress over maximum ingest speed. Neither path changes
the object's immutable EC layout or publication order.

Standard batch upload is stateless at the batch level. It amortizes HTTP and
authentication overhead for many small objects while sending every entry
through the ordinary distributed one-shot commit. There is no batch manifest,
shared EC set, shared NCNX transaction, or gateway route record.

## Install And Build

Build the complete workspace:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --lib --bins --tests
cargo build --workspace --release
```

Binaries:

```text
target/release/litiaina-n2
target/release/litiaina-n2-shell
target/release/litiaina-n2-gateway
```

Run a development node from the directory containing `n2_config.ini`:

```bash
cargo run -p n2 --release
```

Production installs should use [INSTALLATION.md](INSTALLATION.md), reviewed
format-v5 membership, stable device mounts and root IDs, and systemd.

## Environment

Every node in one cluster must share authentication and peer secrets:

```dotenv
JWT_SECRET=<long-random-cluster-value>
N2_CLUSTER_SECRET=<different-long-random-cluster-value>
MASTER_KEY=<operator-master-key>
AUTH_KEY=<operator-master-key>
ADMIN_KEY=<operator-master-key>
ACCESS_TOKEN_DEFAULT_TTL_SECS=900
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_RUNTIME_WORKER_THREADS=
N2_RUNTIME_MAX_BLOCKING_THREADS=
N2_RUNTIME_THREAD_STACK_BYTES=
```

Keep the file outside source control with mode `0600`. `JWT_SECRET` signs
client tokens. `N2_CLUSTER_SECRET` authenticates peer traffic. Operator keys
authorize fragment administration and maintenance. Do not put secrets in
membership JSON.

## Configuration

Every node has a local `n2_config.ini`. Identity, endpoints, and local roots
differ per node; pool identity, generation, and EC must match membership.

```ini
[server]
host=0.0.0.0
https_port=50001
native_port=50002
cert_path=/var/lib/litiaina/n2/certificates/cert.pem
key_path=/var/lib/litiaina/n2/certificates/key.pem

[disk_config]
storage_roots=a-disk-1=/mnt/NDISK001/n2,a-disk-2=/mnt/NDISK002/n2
runtime_path=/var/lib/litiaina/n2/state

[cluster_config]
node_id=node-a
pool_id=pool-1
pool_generation=1
node_endpoint=https://10.0.0.11:50001
native_endpoint=n2://10.0.0.11:50002
membership_file=/var/lib/litiaina/n2/state/n2_membership.json
object_data_shards=2
object_parity_shards=2
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

[reserve_set]
enabled=true
automatic_promotion=true
initial_delay_seconds=30
probe_interval_seconds=5
failure_threshold=3
promotion_timeout_seconds=30

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
data_shards=2
parity_shards=2
stripe_block_size=262144
```

Configuration laws:

- `storage_roots` uses stable `ROOT_ID=/absolute/path` entries.
- A root ID must never be rebound to different media.
- Each server pool contains exactly `K + M` distinct nodes.
- Upload `data_shards` and `parity_shards` must equal the static pool policy.
- `node_role`, dynamic EC, and in-place pool expansion are removed in 3.0.
- `auto_rebalance=false` preserves explicit object-level redistribution.
- `runtime_path` is control state and must not be treated as object authority.
- Reserve detection requires at least two consecutive failed observations;
  the shipped default is three after a 30-second startup grace period.
- A reserve must have the same number of physical roots as the logical slot it
  may replace and enough declared capacity for that slot's recorded usage.

## Membership File

All nodes use the same complete format-v5 membership. This example defines
one four-node static `2 + 2` pool:

```json
{
  "format_version": 5,
  "cluster_id": "cluster-a",
  "epoch": 1,
  "binding_epoch": 1,
  "pools": [{
    "id": "pool-1",
    "generation": 1,
    "policy": {
      "object_data_shards": 2,
      "object_parity_shards": 2
    },
    "state": "writable",
    "slot_ids": ["node-a", "node-b", "node-c", "node-d"]
  }],
  "slot_bindings": [
    {
      "slot_id": "node-a",
      "instance_id": "node-a",
      "binding_epoch": 1,
      "endpoint": "https://10.0.0.11:50001",
      "native_endpoint": "n2://10.0.0.11:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"a-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    },
    {
      "slot_id": "node-b",
      "instance_id": "node-b",
      "binding_epoch": 1,
      "endpoint": "https://10.0.0.12:50001",
      "native_endpoint": "n2://10.0.0.12:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"b-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    },
    {
      "slot_id": "node-c",
      "instance_id": "node-c",
      "binding_epoch": 1,
      "endpoint": "https://10.0.0.13:50001",
      "native_endpoint": "n2://10.0.0.13:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"c-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    },
    {
      "slot_id": "node-d",
      "instance_id": "node-d",
      "binding_epoch": 1,
      "endpoint": "https://10.0.0.14:50001",
      "native_endpoint": "n2://10.0.0.14:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"d-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    }
  ],
  "reserve_nodes": [{
    "instance_id": "reserve-r1",
    "endpoint": "https://10.0.0.21:50001",
    "native_endpoint": "n2://10.0.0.21:50002",
    "state": "online",
    "storage_roots": [{"id":"r1-disk-1","path":"/mnt/NDISK001/n2"}]
  }],
  "retired_instances": []
}
```

Enrollment laws:

- Pool ID, generation, policy, logical slot IDs, and each slot's logical
  root-ID set are immutable after enrollment.
- Every active and reserve instance must receive the same complete file,
  topology epoch, and binding epoch.
- New capacity is a new complete pool generation; never append a node to an
  enrolled static pool.
- Old pools remain online for reads and healing. They may be sealed against
  new writes without invalidating recorded objects.
- A reserve is outside placement until it replaces one failed logical slot.
  Promotion increments only `binding_epoch`, preserves the topology `epoch`,
  and writes a retired-instance fence for the former physical binding.

## Full Installation With Gateway

The gateway is optional. Every N2 node already accepts and coordinates NOA.
The gateway selects a healthy node and preserves multipart coordinator
routing; it is not namespace, object, upload, repair, or cluster authority.

### 1. Prepare Every N2 Node

Use stable mounts, synchronized time, TLS certificates, and one or more
stable root IDs per node. Create the runtime and storage directories and
ensure the service user owns only the required paths.

### 2. Write `n2_config.ini` Per Node

Start from the configuration above. Change only the node's identity,
endpoints, and local root paths. Confirm its pool ID, generation, and EC
exactly match format-v5 membership.

### 3. Write The Membership File

Copy the same reviewed membership JSON to every node. Do not generate
membership independently on each host. Verify stable root IDs before the
first start because root enrollment becomes part of the pool contract.

### 4. Start And Validate N2 Nodes

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now litiaina-n2
sudo systemctl status litiaina-n2 --no-pager
curl --cacert /path/to/ca.pem https://10.0.0.11:50001/noa/v1/ping
```

Check every node directly. After authentication, verify cluster health shows
the expected pool, `K + M`, `M + 1` NCNX replicas, and writable state.

### 5. Configure The Gateway

Configure each node's HTTPS and native endpoint in
`n2_gateway_config.ini`, then install the gateway systemd unit. Do not place
storage or namespace state on the gateway.

### 6. Use The Gateway Endpoint

Normal NOA requests may enter the gateway. Multipart init returns
`coordinator_node_id`; the client or gateway must preserve that identity for
part, status, finalize, and cancel. Direct node endpoints remain fully valid.

Before admitting data, prove one-shot upload/download checksum, multipart
resume, cross-node read, namespace copy/delete, trash/recover, node-loss
read, unsafe-write rejection, healing, TTL GC, and full restart persistence.
See [TESTING.md](TESTING.md).

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
- `write`: allow upload, trash, and recover. The `mkdir` route is retained by NOA but N2 3.4 rejects empty-directory persistence.
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

- `checksum`: optional SHA-256 validation value. Accepts 64 lowercase or
  uppercase hexadecimal characters, optionally prefixed with `sha256:`.
  Headers `x-litiaina-checksum`, `x-noa-checksum-sha256`, and
  `x-n2-checksum-sha256` are also accepted. CRC32 and S3 checksum headers are
  not N2 3.4 contracts.
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

### Standard Small-Object Batch

Use one request to ingest up to 256 independent objects of at most 1 MiB each:

```http
POST /noa/v1/upload/batch?concurrency=8
Authorization: Bearer <token>
Content-Type: multipart/form-data; boundary=...
```

Every file field must be named `object`. Set its multipart filename to the
percent-encoded logical key:

```bash
curl -k -X POST "https://127.0.0.1:50001/noa/v1/upload/batch?concurrency=8" \
  -H "$AUTH_HEADER" \
  -F 'object=@scan-001.png;filename=images%2Fscan-001.png;type=image/png' \
  -F 'object=@scan-002.png;filename=images%2Fscan-002.png;type=image/png'
```

The optional concurrency value is clamped to `1..128` and also constrained by
the coordinator-wide batch gate. Each object independently calculates its
server-pool layout, writes its EC pieces, publishes NCDX, then publishes NCNX.
The response is `201 Created` when all entries commit or `207 Multi-Status`
when only some commit:

```json
{
  "requested": 2,
  "succeeded": 1,
  "failed": 1,
  "results": [
    {"object_key":"images/scan-001.png","status":201,"version_id":"01K...","size_bytes":8192},
    {"object_key":"images/scan-002.png","status":503,"size_bytes":8192,"error":"Service unavailable: fewer than K participants are writable"}
  ]
}
```

A failed entry cannot publish incomplete NCNX state and does not cancel an
unrelated successful entry. Durable multipart remains the contract for
restart-resumable unfinished uploads.

### Standard Multipart Init

Use for large files and maximum ingest throughput. Progress is resumable while
the selected coordinator process remains alive; a coordinator restart requires
a new standard session.

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

- `checksum`: optional SHA-256 validation value. Accepts 64 hexadecimal
  characters, optionally prefixed with `sha256:`. Headers
  `x-litiaina-checksum`, `x-noa-checksum-sha256`, and
  `x-n2-checksum-sha256` are also accepted.
- `coordinator_node_id`: required when this request goes through N2 Gateway.
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
POST /noa/v1/upload/multipart/{version_id}/{session_id}/finalize?coordinator_node_id=node-a
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

If complete, N2 synchronizes every exact shard holder used by the upload,
publishes NCDX, publishes NCNX, returns committed object metadata, and removes
the coordinator-memory session.

### Multipart Cancel

```http
DELETE /noa/v1/upload/multipart/{version_id}?coordinator_node_id=node-a
Authorization: Bearer <token>
```

Cancels the coordinator-memory session. Already-written, unpublished immutable
parts remain headless and are never mistaken for a live object.

### Durable Multipart

Use the durable family when unfinished upload progress must survive a
coordinator restart. Request and response bodies are the same as standard
multipart; insert `/durable` after `/multipart` in every route:

```text
POST   /noa/v1/upload/multipart/durable/init/{object_key}
GET    /noa/v1/upload/multipart/durable
GET    /noa/v1/upload/multipart/durable/{version_id}
PUT    /noa/v1/upload/multipart/durable/{version_id}/{part_index}
POST   /noa/v1/upload/multipart/durable/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/durable/{version_id}
```

Durable init persists the upload manifest and pending logical binding. Each
accepted part persists its compact receipt. Finalize keeps the same immutable
commit boundary as standard multipart: NCDX first, NCNX last. Gateway clients
must preserve `coordinator_node_id` for this family as well.

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

Mint a signed URL:

```http
POST /noa/v1/objects/signed-url
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "object_key": "videos/mhw.mp4",
  "disposition": "download",
  "expires_in": 3600,
  "version_id": null,
  "filename": "mhw.mp4"
}
```

`disposition` accepts `view`/`inline`, `stream`, or
`download`/`attachment`. `expires_in` is clamped to 1 second through 6
hours. The response includes `url`, `path`, fragment, object key, normalized
disposition, absolute `expires_at`, and optional version/filename.

Use the returned path without a Bearer token:

```http
GET /noa/v1/objects/signed-read?fragment=...&object_key=...&expires=...&disposition=...&signature=...
```

Optional signed query fields are `version_id` and `filename`. The HMAC covers
all signed fields, so tampering or expiry returns unauthorized.

## Namespace API

N2 exposes POSIX-like path commands under `/noa/v1/posix/...`. This is flat
object namespace control, not an operating-system filesystem. A returned
directory is a derived common prefix; it is not a persistent inode.

### List Directory

```http
GET /noa/v1/posix/list?path=videos&limit=200&trash=false&continuation_token=...
Authorization: Bearer <token>
```

Fields:

- `path`: normalized logical prefix to list.
- `limit`: page size from 1 through 1000; default 200.
- `trash`: when true, list trash NCNX records instead of live records.
- `continuation_token`: opaque token returned by the previous page.

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
      "object_id": "9f2d...",
      "size_bytes": 17179869184,
      "modified_at_unix": 1780000000,
      "version_id": "01KS..."
    }
  ],
  "continuation_token": null
}
```

Local enumeration is a prefix-pruned lexical walk. Nodes emit canonical
deterministic replicas and the coordinator performs a bounded merge. Listing
does not decode NCNX, open NCDX, or touch object payloads.

### Stat Path

```http
GET /noa/v1/posix/stat?path=videos/mhw.mp4&trash=false
Authorization: Bearer <token>
```

Stat resolves the exact live or trash NCNX. For a file it may read the object
head only when cached namespace fields do not already provide the requested
metadata. Directory results are derived prefixes.

### Make Directory

```http
POST /noa/v1/posix/mkdir
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{ "path": "videos" }
```

The route is retained as part of the NOA v1 surface, but N2 3.4 returns
`400 Bad Request`: the flat namespace does not persist empty directories.
Uploading `videos/a.mp4` makes the `videos/` prefix visible automatically.

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

For one key N2 proves source/destination state, strictly publishes a new
destination NCNX generation, then removes only the expected source bytes.
A retry cannot remove a newer record. A crash can temporarily leave both
names but never intentionally removes the source before the destination is
durable.

When `source_path` is a prefix, N2 enumerates matching exact keys and applies
the same protocol with bounded concurrency. The operation is restart-safe
per key and intentionally not atomic across an unbounded prefix. NCDX and
object payloads are not opened or rewritten.

### Trash

```http
POST /noa/v1/posix/trash
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{ "path": "archive/mhw.mp4" }
```

N2 publishes a uniquely named trash NCNX before removing only the expected
live NCNX. Prefix trash uses one mutation group for the exact keys discovered
by that operation.

### Recover

```http
POST /noa/v1/posix/recover
Authorization: Bearer <token>
Content-Type: application/json
```

Recover by logical path:

```json
{ "path": "archive/mhw.mp4" }
```

Recover a precise trash generation when multiple generations exist:

```json
{
  "path": "archive/mhw.mp4",
  "trash_entry_id": "01KS..."
}
```

Recover publishes live NCNX first, then removes the expected trash NCNX.
Folder recovery restores the selected mutation group. If any selected logical
key already exists live, recovery returns `409 Conflict` and preserves the
trash record.

### Trash Lifecycle

N2 3.4 exposes no empty-trash or hard-delete namespace operation. Each delete
uses one `.trash/<unix-nanos>.<mutation-ulid>/` generation across all affected
logical keys and deterministic NCNX replicas. Normal GC reads TTL from that
directory name, scans only `.trash`, proves live/unexpired reachability,
removes object holders first, and removes the final expired trash pointer
last. Live and unexpired trash always win over expired trash. Headless
immutable objects are reported and preserved rather than guessed disposable.

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

N2 3.4 intentionally has no empty-trash or recover-all namespace mutation.
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

N2 uses physical capacity admission to reject writes that cannot be placed
safely. This is distinct from business-level user or fragment quotas.

## Cluster API

### Membership

```http
GET /noa/v1/cluster/membership
Authorization: Bearer <token>
```

Returns `local_node_id`, the active membership path, and the complete
format-v5 membership including static pools, logical slots, physical
instances, endpoints, logical/physical root identities, topology and binding
epochs, reserves, and retired-instance fences.

### Reserve Set

```http
GET /noa/v1/cluster/reserve
Authorization: Bearer <token>
```

Requires read permission. It returns whether automatic promotion is enabled or
running, topology and binding epochs, active and reserve counts, suspected and
recovering slots, and the last promotion/error state.

```http
POST /noa/v1/cluster/reserve/promote
Authorization: Bearer <token>
Content-Type: application/json

{
  "slot_id": "node-d",
  "reserve_instance_id": "reserve-r1"
}
```

Requires modify permission. Manual promotion uses the same compatibility,
multi-node failure observation, fencing vote, atomic membership commit, and
historical healing path as autonomous promotion. It cannot bypass the exact
pool contract.

### Layout

```http
GET /noa/v1/cluster/layout
Authorization: Bearer <token>
```

Response fields include:

- `membership_epoch`;
- static layout `policy` and `configured_layout`;
- reachable `active_write_nodes` and effective `layout`;
- `static_server_pool_ec: true`;
- `survival_writable`, true while at least `K` configured pool nodes can
  accept writes;
- `fully_protected`, true only at the complete `K + M` width;
- `degraded_write_layout`, true when survival writes are enabled below full
  width;
- `bounded_object_erasure_set: true`;
- `old_object_layouts_remain_valid: true`;
- `automatic_rebalance`, which should remain false for explicit movement.

N2 never returns a smaller write layout merely because a pool node failed.

### Health

```http
GET /noa/v1/cluster/health
Authorization: Bearer <token>
```

Health separates data readability, survival-write capability, full protection,
NCNX replication, and the durable safety latch. Important fields are:

- `state`: `healthy`, `degraded`, or `critical`;
- `data_plane_readable`, `data_plane_writable`, and
  `data_plane_fully_protected`;
- `ncnx_replicas`, `ncnx_replicas_required`, and
  `estimated_ncnx_replicas_reachable`;
- `ncnx_survival_writable`, `ncnx_strictly_writable`, and
  `effective_client_writable`;
- `safety_read_only`;
- `manual_rebalance_available` and `automatic_rebalance`;
- per-node read/write/reachability state and explanatory `notes`.

At exactly `K` writable pool nodes the state is degraded, new objects remain
reconstructable, and `effective_client_writable` remains true. The write keeps
the configured `K + M` target layout; it does not shrink or substitute nodes.
Below `K`, new writes are rejected.

### Detailed Cluster Monitor

```http
GET /noa/v1/cluster/monitor?fragment=n2-test&object_limit=96
Authorization: Bearer <token>
```

This is the preferred console/operator polling endpoint. It combines
high-signal read-only state without starting repair, redistribution, GC, or
a forced disk scan.

Query fields:

- `fragment`: optional fragment context; defaults to the authenticated one.
- `prefix`: optional flat namespace prefix for the object sample.
- `object_limit`: live keys to inspect, default 64 and maximum 512.
- `probe_objects`: perform heavier shard-availability probes.
- `refresh`: bypass supported local health caches.
- `deep_scan`: request deeper local node sampling; use manually.

Response sections:

- `overview`: cluster ID, state, epoch, local node, current static EC,
  readability, and effective writability.
- `nodes`: endpoints, reachability, unified roots, local system/storage/IO
  information, uploads, and maintenance state.
- `topology`: static/current shard counts, NCNX replication, object-level EC,
  old-layout validity, and manual-only redistribution.
- `object_health`: sampled layout and optional shard health.
- `redistribution`: plan and current explicit redistribution job.
- `io_control`: pressure, jobs, latency samples, process resources, and
  recover-on-read counters.
- `jobs`: cluster repair, rebalance, and local GC status.
- `namespace_replication`: flat NCNX replication and holder health.
- `uploads`: aggregated active/stale multipart activity.

### Usage

```http
GET /noa/v1/cluster/usage?fragment=n2-test
GET /noa/v1/cluster/usage?fragment=n2-test&refresh=true
Authorization: Bearer <token>
```

Returns node results, total/used/available bytes, and fragment live/trash/
physical bytes. Cold or forced usage can walk disks; do not poll it as an
aggressive dashboard heartbeat.

### Placement Preview

```http
GET /noa/v1/cluster/placement/preview?fragment=n2-test&object_key=videos/mhw.mp4&version_id=preview
Authorization: Bearer <token>
```

Shows the exact static pool, nodes, roots, shard indexes, and data/parity
roles that a new object would use.

### Membership Reload

```http
POST /noa/v1/cluster/membership/reload
Authorization: Bearer <token>
```

Live operator reload is intentionally disabled. Review a format-v5 topology
change and restart N2 through systemd. Reserve promotion is the only live
membership transition: it advances the binding epoch while preserving the
enrolled logical pool. Capacity expansion still adds a complete new pool
generation.

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

Normal GC scans flat `.trash` records, proves live and unexpired references, and
reclaims expired object holders before deleting the final trash NCNX. It does
not use a routine whole-object orphan scan. GC is lockless and idempotent across
coordinators: a branch concurrently pruned by another completed cleanup is not
an error. Temporary topology unavailability defers the run without reclaiming
data; corrupt, conflicting, or malformed records still fail the run. The status
response distinguishes `Deferred` from `Failed` and exposes the latest job's
deferred count separately from real errors.

### Repair

```http
POST /noa/v1/maintenance/repair/start?mode=availability&fast_mode=true
POST /noa/v1/maintenance/fragments/{fragment}/repair/start?mode=scrub&fast_mode=false
POST /noa/v1/maintenance/repair/stop
GET  /noa/v1/maintenance/repair/status
Authorization: Bearer <token>
```

Manual, replacement, and scheduled autonomous healing share one node-local
single-run gate. If the scheduler finds another cycle active, it skips that
interval and leaves the running job's status untouched.

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

Redistribution explicitly re-encodes selected objects into the newest complete
writable server-pool layout. It is never triggered merely by pool expansion.

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

Every node runs the scheduler, but deterministic bounded ownership assigns a
fragment to one coordinator for each time-bounded cycle. Independent
fragments may heal in parallel. Progress is persisted under
`runtime_path/.n2/auto-healing` so a bounded scan can resume after restart.

NCNX healing reads the exact deterministic `M + 1` holders, validates
immutable bytes and checksums, and restores a missing or corrupt replica only
from identical valid authority. Divergent valid bytes or unprovable absence
fail closed. Before any critical report, the scanner revalidates the exact
live/trash state so a concurrent TTL GC cannot produce a stale failure or
resurrect expired trash.

Object healing reads NCDX, verifies the exact recorded holders and shard
checksums, reconstructs a missing/corrupt shard when at least `K` valid
shards survive, and republishes only that missing immutable component. It
never changes pool generation, EC width, or object version.

Each degraded, repaired, or unrepairable item writes an independent JSON
report under `runtime_path/.reports/auto-heal/<fragment>/`. Reports include
the logical filename when known, object/version identity, component,
observations, action, and outcome. They are local disposable evidence, never
replicated and never used as storage authority.

Object-local NCNX, NCDX, shard, or parity loss is reported and remains scoped
to that object; it does not stop unrelated reconstructable service.
Systemic membership or binding-authority disagreement can activate the durable
safety read-only latch. Headless immutable objects are reported and preserved
because missing namespace authority is not proof that payload bytes are
disposable.

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
  --sha256

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

Large shell uploads use standard multipart by default. Add `--durable` when
unfinished multipart progress must survive a coordinator restart.

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
upload.multipart.standard.init
upload.multipart.standard.put_part
upload.multipart.standard.finalize
upload.multipart.standard.cancel
uploads.standard.status
uploads.standard.list
upload.multipart.durable.init
upload.multipart.durable.put_part
upload.multipart.durable.finalize
upload.multipart.durable.cancel
uploads.durable.status
uploads.durable.list
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

The older `multipart.*` and `uploads.*` native aliases remain mapped to the
durable contract for existing native clients.

## Peer Routes

Peer routes are authenticated N2 node-to-node APIs. They are not public NOA
client contracts:

```text
/n2/peer/usage
/n2/peer/monitor
/n2/peer/ncnx-v3
/n2/peer/objects/publish
/n2/peer/objects/resolve
/n2/peer/objects/resolve-batch
/n2/peer/objects/inventory
/n2/peer/shards/{fragment}/{object_id}/{version_id}/{part_index}/{shard_index}
/n2/peer/fragments/control
/n2/peer/fragments/snapshot
/n2/peer/healing/lease
/n2/peer/healing/read-only
/n2/peer/lifecycle/purge-object
/n2/peer/maintenance/start
/n2/peer/maintenance/status/{job_id}
```

Every node must share `N2_CLUSTER_SECRET`. Peer handlers also validate exact
pool, node, root, object, and mutation identities where applicable. Clients
must use `/noa/v1/...` or `n2://`, not these internal routes.

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

### Old Objects Do Not Move After Adding A Pool

Correct. A complete new pool accepts future objects. Old objects keep their
exact recorded pool layout until explicit redistribution.

### Cluster Usage Is Slow

Cluster usage asks readable nodes for usage and aggregates the result. It is an
operator monitoring action, not a high-frequency UI polling route.

### Namespace Rename Does Not Rewrite Object Data

Correct. Exact rename/move touches only source and destination NCNX replicas.
A prefix rename enumerates its matching keys and therefore scales with the
number of keys; it still performs no NCDX or payload rewrite.

### Unexpected Internal Entries Appear In Listings

N2 user-facing listings should expose namespace entries only. Object metadata
under `.objects`, uploads, trash internals, locks, repair state, GC state, and
other control directories must not appear as user files. If a console shows an
internal control path, treat that as a UI/API filtering bug.

## Validation Commands

Repository gates:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --lib --bins --tests
cargo build --workspace --release
```

Installer proof:

```bash
scripts/proof/run_n2_installer_proof.sh
```

Finite N2 3.4 multi-node proof (retained proof-family script name):

```bash
REMOTE_PASS='operator-supplied' \
  scripts/bench/run_n2_3_0_release_proof.sh
```

Inspect lifecycle/chaos options:

```bash
scripts/bench/run_n2_3_0_soak_chaos.sh --help
```

For N2 3.4 qualification, prove at minimum:

- identical format-v5 membership and static pool contracts on every node;
- fragment creation and authentication;
- one-shot and multipart upload with coordinator stickiness;
- byte-identical download through every node after finalize;
- zero-decode LIST pagination including exact-key/same-prefix ordering;
- `/posix/mkdir` rejection and derived-prefix behavior;
- exact and prefix rename, trash, recovery, TTL expiry, and live-wins rules;
- strict `K + M` and `M + 1` publication;
- one-node and parity-count loss reads plus unsafe-write rejection;
- one-slot autonomous reserve promotion with topology preservation,
  binding-epoch advance, retired-instance rejection, healing, and restart;
- NCNX, NCDX, shard, and parity healing to exact recorded holders;
- explicit redistribution without changing untouched objects;
- full restart persistence;
- concurrent lifecycle/healing race coverage;
- shell and gateway flows where those optional components are deployed.

Current results and uncompleted gates are in [TESTING.md](TESTING.md).
Performance reproduction belongs in [BENCHMARK.md](BENCHMARK.md).
