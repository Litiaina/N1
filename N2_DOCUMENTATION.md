# N2 Documentation

N2 is Litiaina's standalone distributed object storage system. It owns a real
cluster namespace, immutable cluster object identity, node-level shard
placement, peer transport, repair, rebalance, and health monitoring.

N1 and N2 are independent systems. They share Litiaina storage principles, but
N2 is not described or operated as a distributed copy of N1.

For the deeper design, see [N2_ARCHITECTURE.md](N2_ARCHITECTURE.md).

## Current Status

N2 `0.11.0` is the native-peer hardening line after the destructive reset.

What exists now:

- `litiaina-n2` server binary
- `n2_config.ini`
- `n2.env`
- `/n2/...` HTTP route prefix
- `n2://` native protocol identity
- N2 namespace, object record, blob, repair, GC, quota, and maintenance code
- membership file creation/update
- auto/fixed node-level layout policy
- deterministic placement preview
- namespace mutation ledger foundation
- cluster health, repair-plan, and rebalance-plan APIs
- peer-to-peer shard transport under `/n2/peer/...`
- cluster EC upload streaming for one-shot and multipart writes
- distributed range/chunk reads with parity reconstruction
- namespace quorum apply for upload commits and POSIX/object mutations
- cluster repair executor status/start
- cluster rebalance executor status/start
- immutable `.nmtd` cluster placement records
- native `n2://` peer shard transport proof across two nodes

What is still before 1.0:

- long two-node soak with real node loss/re-add
- native `n2://` client/CLI once server semantics settle
- tighter membership reload and split-brain handling
- N2 CLI after the server core stabilizes

## Configuration

N2 uses explicit index roots, blob roots, and a filesystem control root:

```ini
[disk_config]
index_store_disks=/mnt/disk0,/mnt/disk1
blob_store_disks=/mnt/disk0,/mnt/disk1
fs_storage_path=/srv/n2/fs
```

N2 adds cluster configuration:

```ini
[cluster_config]
node_id=node-1
node_endpoint=https://127.0.0.1:50001
native_endpoint=n2://127.0.0.1:50002
membership_file=n2_membership.json
cluster_layout_mode=auto
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false
```

`membership_file` is resolved under `fs_storage_path` when it is relative. If it does not exist, N2 creates it with the local node.

## Adaptable Layout

`cluster_layout_mode=auto` derives the EC shape from active writable nodes:

```text
1 node  -> 1 + 0
2 nodes -> 1 + 1
3 nodes -> 2 + 1
4 nodes -> 3 + 1
```

`cluster_layout_mode=fixed` lets operators pin data/parity shard counts.

N2 does not redistribute existing objects automatically when membership changes. New objects use the latest membership/layout. Old objects stay on their recorded layout until an operator runs rebalance.

## Cluster APIs

Protected cluster routes:

```text
GET  /n2/cluster/membership
GET  /n2/cluster/layout
GET  /n2/cluster/health
GET  /n2/cluster/placement/preview?fragment=<fragment>&object_key=<key>&version_id=<version>
GET  /n2/cluster/rebalance/plan
POST /n2/cluster/rebalance/start
GET  /n2/cluster/rebalance/status
GET  /n2/cluster/repair/plan
POST /n2/cluster/repair/start
GET  /n2/cluster/repair/status
POST /n2/cluster/membership/reload
```

`membership/reload` currently returns `503` intentionally. Until namespace consensus is active, membership is reloaded by restart after editing the membership file. That prevents mid-flight split-brain behavior.

## Object APIs

Object and POSIX routes live under `/n2/...`:

```text
POST   /n2/auth/fragment
GET    /n2/fragments
GET    /n2/objects
GET    /n2/objects/get/{object_key}
POST   /n2/upload/one-shot/{object_key}
POST   /n2/upload/multipart/init/{object_key}
PUT    /n2/upload/multipart/{version_id}/{part_index}
POST   /n2/upload/multipart/{version_id}/{session_id}/finalize
GET    /n2/posix/list
POST   /n2/posix/mkdir
POST   /n2/posix/rename
POST   /n2/maintenance/repair/start
GET    /n2/maintenance/health
```

Upload and read routes now use the N2 cluster path when membership contains multiple active nodes. Remote shard holders receive streamed `.shard` / `.parity` payloads through peer routes; reads use the recorded object layout and fetch bounded shard ranges from peers when needed.

## Storage Layout

N2 stores namespace, cluster object records, and blob payloads in explicit
planes:

```text
<index_root>/<fragment_hash>/
  namespace/
  .objects/
  .cluster/objects/<fanout>/@o_<object_id>/<version_id>.nmtd

<blob_root>/<fragment_hash>/
  .blobs/<version_id>/part_<part>_shard_<shard>.shard
  .blobs/<version_id>/part_<part>_shard_<shard>.parity
```

`.cluster` is for N2 cluster control records such as immutable `.nmtd`
placement metadata. Payload truth stays immutable in object records and blobs.

Peer-only routes are not public APIs. They are protected by `N2_CLUSTER_SECRET` when set:

```text
PUT /n2/peer/shards/{fragment}/{version_id}/{part_index}/{shard_index}
GET /n2/peer/shards/{fragment}/{version_id}/{part_index}/{shard_index}
POST /n2/peer/namespace/apply
```
