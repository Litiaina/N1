# N2 Documentation

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

This document covers operator setup, runtime concepts, and the public API
surface. For the storage design, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Current Status

N2 `1.0.0` is the structure-final distributed object storage line. It is not an
LTS claim yet. Long soak, larger cluster proof, and full node-loss/re-add proof
remain release-validation work.

What exists now:

- `litiaina-n2` server binary
- `litiaina-n2-cli` native operator CLI
- `n2_native_client` reusable Rust client
- Registry business layer retained for user/account/policy workflows
- indexdb retained as Registry's local metadata engine
- `/n2/...` HTTP API
- `n2://` native protocol identity
- stable fragment identity through `.fragment/<alias_hash> -> @f_<stable_fragment_id>`
- namespace, object record, blob, repair, GC, and maintenance code
- cluster membership file creation/update
- auto and fixed node-level layout policy
- deterministic placement preview
- peer-to-peer shard transport
- distributed one-shot and multipart upload
- coordinator-owned multipart resumability
- distributed download and parity reconstruction
- cluster health, repair plan, and rebalance plan APIs
- repair and rebalance executors
- hidden `.cluster` control metadata
- quota usage monitoring without fragment quota enforcement

What remains before LTS:

- long multi-node soak
- node loss, re-add, and split-brain hardening proof
- deeper console proof across mobile and desktop viewports

## Configuration

N2 uses explicit local roots and cluster membership.

```ini
[server]
host=0.0.0.0
https_port=53401
native_port=53402
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[disk_config]
index_store_disks=/mnt/disk0/n2/index
blob_store_disks=/mnt/disk0/n2/blob
fs_storage_path=/mnt/disk0/n2/fs

[cluster_config]
node_id=node-a
node_endpoint=https://192.168.0.22:53401
native_endpoint=n2://192.168.0.22:53402
membership_file=/mnt/disk0/n2/fs/n2_membership.json
cluster_layout_mode=auto
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled
```

`membership_file` is resolved under `fs_storage_path` when it is relative. If
it does not exist, N2 creates it with the local node.

## Layout Policy

`cluster_layout_mode=auto` derives the EC shape from writable node count:

```text
1 node  -> 1 + 0
2 nodes -> 1 + 1
3 nodes -> 2 + 1
4 nodes -> 3 + 1
```

`cluster_layout_mode=fixed` lets operators pin data and parity shard counts.

N2 does not redistribute existing objects automatically when membership
changes. New objects use the current membership and layout. Existing objects
stay valid under their recorded layout until an operator runs rebalance.

## Public APIs

All public N2 routes use `/n2/...`.

### Auth And Fragments

```text
POST /n2/auth/fragment
GET  /n2/fragments
POST /n2/fragments
```

Fragment quota values are accepted only as monitoring metadata. N2 does not
enforce per-fragment quota because cluster storage usage is distributed.

### Object Upload

```text
POST /n2/upload/one-shot/{object_key}
POST /n2/upload/multipart/init/{object_key}
PUT  /n2/upload/multipart/{version_id}/{part_index}
POST /n2/upload/multipart/{version_id}/{session_id}/finalize
GET  /n2/upload/multipart/{version_id}/status
```

Large browser uploads should use multipart. One-shot upload is intentionally
bounded so a client cannot push huge files through the least resumable path.

Multipart sessions are coordinator-owned by design. The node that accepts
`/n2/upload/multipart/init/...` owns the temporary upload session until
finalize. Clients, consoles, and load balancers should send all `put_part`,
`status`, and `finalize` calls for that session back to the same coordinator.

That does not make multipart single-node storage. The coordinator streams every
part through the distributed N2 shard/parity placement path. After finalize, the
object metadata, namespace entry, and placement record are cluster-visible, and
any node can list, download, or inspect the object.

`init` responses identify the session through `version_id` and `session_id`.
Deployments should keep those requests sticky to the node that returned them.
This keeps resumability strong without forcing every in-flight part receipt
through cluster-wide consensus.

Operational rule:

```text
new upload session -> any healthy node
same multipart session -> same coordinator node until finalize
finalized object -> readable from any node
```

### Object Read

```text
GET /n2/objects
GET /n2/objects/get/{object_key}
GET /n2/objects/download/chunk/{object_key}
GET /n2/objects/status/{object_key}
```

Reads use the recorded object layout, not the newest cluster layout.

### Namespace

```text
GET  /n2/posix/list
POST /n2/posix/mkdir
POST /n2/posix/rename
POST /n2/posix/trash
POST /n2/posix/recover
```

The namespace is the object-key face. Folder and file mutations are namespace
mutations, while object records and payloads remain immutable.

### Cluster Monitoring

```text
GET /n2/cluster/membership
GET /n2/cluster/layout
GET /n2/cluster/health
GET /n2/cluster/usage
GET /n2/cluster/placement/preview
```

Cluster usage is operator-triggered monitoring. It should not be polled
aggressively by the UI because it can require node and disk scans.

## Load Balancing

N2 does not require a storage-aware load balancer. The load balancer should only
perform normal health-based routing and multipart session affinity.

Recommended behavior:

- send new operations to any healthy N2 node
- keep all requests for one multipart session on the coordinator that accepted
  `init`
- do not route based on object shard placement; N2 handles placement internally

This keeps load balancing simple while allowing every N2 node to become a
coordinator for independent operations.

### Maintenance

```text
GET  /n2/cluster/repair/plan
POST /n2/cluster/repair/start
GET  /n2/cluster/repair/status
GET  /n2/cluster/rebalance/plan
POST /n2/cluster/rebalance/start
GET  /n2/cluster/rebalance/status
POST /n2/maintenance/gc/start
```

Repair fixes missing or degraded recorded layouts. Rebalance is explicit
operator work to rewrite selected objects onto a newer layout.

## Peer APIs

Peer routes are internal node-to-node routes and are protected by
`N2_CLUSTER_SECRET` when set.

```text
PUT  /n2/peer/shards/{fragment}/{version_id}/{part_index}/{shard_index}
GET  /n2/peer/shards/{fragment}/{version_id}/{part_index}/{shard_index}
POST /n2/peer/namespace/apply
```

These are not public client APIs.

## Native CLI

Build the CLI:

```bash
cargo build -p n2_cli --release
```

Basic native operations:

```bash
target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 ping

target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 \
  health --fragment <fragment> --secret <key>

target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 \
  membership --fragment <fragment> --secret <key>

target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 \
  list --fragment <fragment> --secret <key> --path videos
```

The CLI uses `n2_native_client` and talks `n2://` directly.

## Registry

Registry remains in this repository because it is the N2-facing business layer
for users, accounts, storage sessions, shares, and policy. Registry uses
indexdb as its local metadata engine.
