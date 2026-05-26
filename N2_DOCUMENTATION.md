# N2 0.7.1 API Documentation

`N2` is Litiaina's distributed object storage layer. It owns the cluster namespace, shard placement, distributed object metadata, peer transport, reconcile, and cluster repair while each node uses `N1` as its local storage kernel.

For the disk layout, write/read path, placement model, transport planes, and failure model, see [N2_ARCHITECTURE.md](N2_ARCHITECTURE.md).

This document focuses on:

- the public and operator-facing `N2` APIs
- the native `n2://` protocol surface
- the request and response fields that matter to integrators
- the difference between public routes and node-to-node internal routes

## What N2 Owns

`N2` adds the cluster plane:

- cluster membership
- cluster view and governance state
- rendezvous placement of cluster shards
- multi-node object ingest and read
- real-prefix N2 namespace directories with symlink object entries
- immutable filesystem `.nmtd` object-record persistence and reconcile
- internal peer transport between `N2` nodes

`N2` does not use N1's rich namespace for user object keys. N2 owns the distributed namespace and stores physical shard objects into the local N1 storage kernel using internal shard identity. Distributed EC is performed once at the N2 layer; local N1 shard storage is a single local shard object (`kd=1`, `md=0`) by design.

## N2 0.7 Rewrite Structure

N2 0.7 is being shaped as a clean Litiaina cluster layer over the N1 kernel, not as a carry-over of the old pre-N1-kernel N2 runtime.

The active code boundaries are:

- LOA HTTP routes for cluster/object APIs under `/n2/...`
- native N2 protocol operations over `n2://`
- peer routes for node-to-node shard, object-record, namespace, and governance traffic under `/n2/internal/...`
- `.nmtd` metadata primitives for filesystem object-record paths and names
- N1 as the local storage kernel dependency

This keeps LOA as the product API, `n2://` as the low-level N2 protocol, peer transport as a private cluster face, and `.nmtd` as the N2 metadata identity. External compatibility adapters are outside the N2 core.

## N2 0.7 Storage Layout

N2 uses filesystem metadata, not an external database.

The mutable N2 namespace is a real directory tree. Object names are symlinks to immutable object-record entries:

```text
<index_root>/<fragment_hash>/n2/namespace/videos/demo.mp4
  -> <index_root>/<fragment_hash>/n2/objects/<hh>/<hh>/<hh>/<hh>/@o_<object_id>
```

Object records are immutable `.nmtd` files under each configured index root:

```text
<index_root>/<fragment_hash>/n2/objects/<hh>/<hh>/<hh>/<hh>/@o_<object_id>/<version_id>.nmtd
```

There is no mutable latest pointer file. Latest is the max ULID `.nmtd` filename inside the stable object-id directory. Rename moves the namespace symlink only; it does not rewrite existing `.nmtd` records or shard payload bytes.

Normal prefix listing reads the real namespace directory first. A delimiter listing such as `prefix=videos/&delimiter=/` opens the `videos/` namespace directory directly and returns direct children/common prefixes without walking the whole cluster object-record tree.

Local cluster shards are stored under each configured blob root:

```text
<blob_root>/<fragment_hash>/n2/shards/<version_id>/cshard_<cluster_shard_index>/
  disk_shard_0.bin
  .nmtd
```

`disk_shard_0.bin` is the exact cluster shard payload. N2 0.7 does not run a second local erasure-code pass under each node. Node/disk survivability is handled by the N2 cluster EC plan and by the local N1 storage kernel around the shard object.

Active N2 object truth is limited to:

- namespace symlinks under `n2/namespace`
- immutable `<version_id>.nmtd` records under `n2/objects`
- local cluster shard metadata `.nmtd` beside shard payloads
- local shard storage with `kd=1`, `md=0`

The local `N1` kernel may still own its own runtime files and fragment metadata. N2's distributed object truth is `.nmtd`.

## Base URL

Typical base URL:

```text
https://127.0.0.1:50001
```

`N2` LOA HTTP routes live under `/n2/...`.

`N2` native protocol listens on `n2://<host>:<native_port>` when `native_protocol_enabled=true`.

The `/n1/...` routes remain available for fragment creation/auth and local N1-kernel operations. New clustered object traffic should use `/n2/...` or `n2://`.

## Operator Configuration

Important `[n2]` keys:

- `node_id`, `node_endpoint`, `cluster_membership_file`
- `kc`, `mc` for cluster EC layout
- `kd=1`, `md=0` for local shard storage
- `cluster_commit_mode`
- `auto_maintenance_mode`, `auto_maintenance_interval_seconds`
- `repair_global_concurrency`, `repair_per_fragment_concurrency`

`repair_global_concurrency` bounds total repair/reconcile work on one node.
`repair_per_fragment_concurrency` prevents one fragment from consuming every
repair slot. Both are backpressure controls; saturation returns `503`.

## Authentication Model

Protected `N2` routes use the same bearer-token middleware as the shared storage runtime.

In practice, the runtime usually mints those fragment-scoped tokens through the shared storage auth route on the same server:

`POST /n1/auth/fragment`

Typical header:

```text
Authorization: Bearer <access_token>
```

Practical permission model:

- read routes require `read`
- write or mutate routes require `write`, `modify`, or `delete` depending on the route
- membership and governance mutation routes are operator/admin surfaces

Example auth request:

```json
{
  "fragment": "fragment-a",
  "secret": "fragment-or-master-key",
  "read": true,
  "write": true,
  "modify": true,
  "delete": true
}
```

## Native `n2://` Protocol

`n2://` is the low-level N2 protocol. It uses the same length-prefixed MessagePack control-frame shape as the N1 native protocol, with JSON payload strings and optional byte bodies. It is intended for N2 tools, N2-to-N2 operators, future desktop clients, and high-performance internal integrations.

Core operations:

- `ping`
- `auth.fragment`
- `auth.bearer`
- `auth.clear`
- `session.info`
- `cluster.view.public`
- `cluster.view`
- `governance.state`
- `membership.get`
- `membership.put`
- `membership.rollback`
- `placement.resolve`
- `object.put`
- `object.get`
- `object.download`
- `object.list`
- `object.versions`
- `object.delete`
- `object.rename`
- `reconcile.fragment`

`object.get` and `object.download` stream the response body after the native response control frame when the object response has a `Content-Length`. `object.put` accepts a native request body and commits through the same N2 object plane as HTTP `/n2/object/...`.

## Core Data Shapes

### Node Descriptor

```json
{
  "node_id": "node-a",
  "endpoint": "https://10.0.0.1:50001",
  "epoch": 12,
  "active": true
}
```

### Cluster Membership

```json
{
  "epoch": 12,
  "nodes": [
    {
      "node_id": "node-a",
      "endpoint": "https://10.0.0.1:50001",
      "epoch": 12,
      "active": true
    },
    {
      "node_id": "node-b",
      "endpoint": "https://10.0.0.2:50001",
      "epoch": 12,
      "active": true
    }
  ]
}
```

### Placement Identity

Placement and object records are keyed by:

```json
{
  "uid": "tenant-or-owner-id",
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W"
}
```

### N2 Object Record

The committed cluster object record contains:

- `epoch`
- `object`
- `params`:
  - `kc`
  - `mc`
  - `kd` (`1` in N2 0.7)
  - `md` (`0` in N2 0.7)
- `stripe_block_size`
- `total_size_bytes`
- `content_type`
- `write_quorum`
- `created_at`
- `committed_at`
- `shards`

Representative response:

```json
{
  "epoch": 12,
  "object": {
    "uid": "tenant-a",
    "fragment": "fragment-a",
    "object_key": "videos/demo.mp4",
    "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W"
  },
  "params": {
    "kc": 2,
    "mc": 1,
    "kd": 1,
    "md": 0
  },
  "stripe_block_size": 1048576,
  "total_size_bytes": 10485760,
  "content_type": "video/mp4",
  "write_quorum": 2,
  "created_at": 1775901000,
  "committed_at": 1775901001,
  "shards": [
    {
      "cluster_shard_index": 0,
      "node_id": "node-a",
      "endpoint": "https://10.0.0.1:50001",
      "size_bytes": 5242880,
      "checksum": "sha256...",
      "disk_shards": []
    }
  ]
}
```

## Public And Protected Cluster Routes

### `GET /n2/cluster/view/public`

No auth. Public diagnostics for cluster view.

Query parameters:

- `expected_epoch` optional

Response:

```json
{
  "node_id": "node-a",
  "node_endpoint": "https://10.0.0.1:50001",
  "membership_epoch": 12,
  "cluster_fingerprint": "sha256...",
  "expected_epoch": 12,
  "total_nodes": 3,
  "active_nodes": 3,
  "local_node_present": true,
  "local_node_active": true,
  "governance_term": 8,
  "governance_commit_index": 12,
  "governance_leader_id": "node-a",
  "governance_leader_endpoint": "https://10.0.0.1:50001",
  "governance_pending_index": null,
  "governance_healthy": true,
  "write_ready": true,
  "reasons": []
}
```

### `GET /n2/cluster/view`

Same shape as the public route, but protected.

### `GET /n2/health/node`

Protected local node health. This reports N2 process RSS plus index/blob root readiness and capacity.

Use this for per-node operator monitoring:

```bash
curl -k \
  "https://127.0.0.1:50001/n2/health/node" \
  -H "Authorization: Bearer <token>"
```

### `GET /n2/health/cluster`

Protected cluster health. Query parameters:

- `expected_epoch`
- `deep=true|false`
- `max_fragments`
- `max_objects_per_fragment`

The response classifies the cluster as `healthy`, `degraded`, `critical`, or `lost`, reports reachable active nodes, storage-ready nodes, write readiness, governance alignment, node health, and optional deep fragment health.

`deep=true` performs bounded fragment/object health scans. Keep `max_fragments` and
`max_objects_per_fragment` explicit for production dashboards.

### `GET /n2/health/fragment/{fragment}`

Protected fragment health. Query parameters:

- `max_objects`

This scans bounded object-record health for one fragment and reports healthy/degraded/critical/lost object counts.

### `GET /n2/health/object/{fragment}/{object_key}`

Protected object health. Query parameters:

- `version_id`

This checks one object/version, verifies the current object record, probes local/remote shard availability, detects stale placement after topology changes, and reports whether the object is readable, repairable, degraded, critical, or lost.

Object health is metadata and shard-presence oriented. It does not download full
payload shards for healthy checks.

### `GET /n2/governance/state`

Protected governance snapshot.

Response:

```json
{
  "current_term": 8,
  "commit_index": 12,
  "last_applied_membership_epoch": 12,
  "leader_id": "node-a",
  "leader_endpoint": "https://10.0.0.1:50001",
  "pending_index": null,
  "is_leader": true,
  "leader_lease_valid": true
}
```

### `GET /n2/membership`

Return current cluster membership.

### `PUT /n2/membership`

Replace or upsert cluster membership.

Request body:

```json
{
  "epoch": 12,
  "nodes": [
    {
      "node_id": "node-a",
      "endpoint": "https://10.0.0.1:50001",
      "epoch": 12,
      "active": true
    },
    {
      "node_id": "node-b",
      "endpoint": "https://10.0.0.2:50001",
      "epoch": 12,
      "active": true
    }
  ]
}
```

Response:

- `ClusterMembership`

### `POST /n2/membership/rollback/{epoch}`

Rollback membership to a saved snapshot for the target epoch.

Path parameters:

- `epoch`

Response:

- `ClusterMembership`

### `GET /n2/placement/{fragment}/{*object_key}`

Resolve where a cluster object would be placed.

Path parameters:

- `fragment`
- `object_key`

Query parameters:

- `uid` optional
- `version_id` optional
- `kc` optional
- `mc` optional

Response:

```json
{
  "epoch": 12,
  "object": {
    "uid": "tenant-a",
    "fragment": "fragment-a",
    "object_key": "videos/demo.mp4",
    "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W"
  },
  "kc": 2,
  "mc": 1,
  "nc": 3,
  "write_quorum": 2,
  "targets": [
    {
      "node_id": "node-a",
      "endpoint": "https://10.0.0.1:50001",
      "epoch": 12,
      "active": true
    }
  ]
}
```

## Object API

### `POST /n2/object/{fragment}/{*object_key}`

Put an object into the cluster.

Path parameters:

- `fragment`
- `object_key`

Query parameters:

- `uid` optional
- `version_id` optional
- `expected_epoch` optional
- `kc` optional
- `mc` optional
- `kd` optional, must be `1` if supplied
- `md` optional, must be `0` if supplied
- `stripe_block_size` optional
- `content_type` optional

Body:

- raw object bytes

Response:

```json
{
  "epoch": 12,
  "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W",
  "total_size_bytes": 10485760,
  "commit_mode": "quorum",
  "planned_shards": 3,
  "committed_shards": 3,
  "required_quorum": 2,
  "required_commit_shards": 2,
  "object": {
    "...": "N2ObjectRecord"
  }
}
```

### `GET /n2/object/{fragment}/{*object_key}`

Read an object from the cluster.

Query parameters:

- `version_id` optional

Supports range requests through the standard `Range` header.

### `GET /n2/object/list/{fragment}`

List cluster objects by latest visible head.

Query parameters:

- `limit` optional
- `continuation` optional

Response:

```json
{
  "fragment": "fragment-a",
  "limit": 200,
  "continuation": "videos/demo.mp4",
  "has_more": true,
  "objects": [
    {
      "object_key": "videos/demo.mp4",
      "latest_version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W",
      "epoch": 12,
      "total_size_bytes": 10485760,
      "content_type": "video/mp4",
      "committed_at": 1775901001,
      "shard_count": 3
    }
  ]
}
```

### `GET /n2/object/versions/{fragment}/{*object_key}`

List versions for one logical key.

Query parameters:

- `limit` optional

Response:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "versions": [
    {
      "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W",
      "epoch": 12,
      "total_size_bytes": 10485760,
      "content_type": "video/mp4",
      "committed_at": 1775901001,
      "shard_count": 3
    }
  ]
}
```

### `DELETE /n2/object/{fragment}/{*object_key}`

Delete one object or one specific version.

Query parameters:

- `version_id` optional
- `expected_epoch` optional
- `purge_shards` optional boolean

Response:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "deleted_versions": 1,
  "deleted_metadata_files": 1,
  "attempted_shard_deletes": 3,
  "deleted_shard_paths": 3,
  "shard_delete_failures": 0
}
```

### `POST /n2/object/rename`

Rename a logical cluster object key without changing its physical version ids.

Request body:

```json
{
  "fragment": "fragment-a",
  "source_object_key": "videos/demo.mp4",
  "destination_object_key": "videos/archive/demo.mp4",
  "expected_epoch": 12
}
```

Response:

```json
{
  "fragment": "fragment-a",
  "source_object_key": "videos/demo.mp4",
  "destination_object_key": "videos/archive/demo.mp4",
  "moved_versions": 2,
  "metadata_records_rewritten": 2
}
```

## Reconcile API

### `POST /n2/reconcile/{fragment}`

Re-evaluate placement and rebuild or relocate shards as needed.

Path parameters:

- `fragment`

Query parameters:

- `max_objects` optional, default `128`, clamped `1..10000`
- `dry_run` optional boolean
- `expected_epoch` optional
- `cleanup_orphans` optional boolean, default `true`

Response:

```json
{
  "fragment": "fragment-a",
  "dry_run": false,
  "scanned_objects": 1,
  "updated_objects": 1,
  "failed_objects": 0,
  "results": [
    {
      "object_key": "videos/demo.mp4",
      "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W",
      "old_epoch": 11,
      "target_epoch": 12,
      "planned_shards": 1,
      "repaired_shards": 1,
      "cleaned_orphans": 1,
      "cleanup_failures": 0,
      "status": "reconciled",
      "error": null
    }
  ]
}
```

### `POST /n2/repair/object/{fragment}/{object_key}`

Protected object-scoped repair. Query parameters:

- `version_id`
- `expected_epoch`
- `dry_run`
- `cleanup_orphans`

This repairs or redistributes one object/version only. It does not lock or scan the whole cluster.
N2 0.7.1 keeps this path bounded with a global repair gate, a per-fragment
repair gate, and an object-scoped repair gate. When the gate is saturated, N2
returns `503` and the caller should retry shortly.

Repair writes rebuilt shards through streamed temporary files. It does not hold
whole cluster shards in memory for the common `kc=1` recovery path, and wider
cluster EC layouts rebuild missing targets stripe-by-stripe.

## Internal Peer API

Everything below `/n2/internal/...` is node-to-node traffic. These routes are not intended as public product-facing APIs.

Still, if you are operating or debugging `N2`, this is the shape.

### Internal Session Upload

#### `POST /n2/internal/session/begin/{fragment}/{version_id}/{cluster_shard_index}`

Query parameters:

- `uid` optional
- `object_key` optional
- `kd` required, must be `1`
- `md` required, must be `0`
- `stripe_block_size` required

Response:

- internal session record including `session_id`

#### `PUT /n2/internal/session/chunk/{session_id}`

Body:

- binary chunk bytes for one cluster shard stream

#### `POST /n2/internal/session/commit/{session_id}`

Finalize the shard session and persist the shard on the target node.

#### `POST /n2/internal/session/abort/{session_id}`

Abort the session and drop temporary state.

### Direct Internal Shard Store / Read / Delete

#### `POST /n2/internal/store/{fragment}/{version_id}/{cluster_shard_index}`

Directly store a full cluster shard on the target node.

#### `GET /n2/internal/read/{fragment}/{version_id}/{cluster_shard_index}`

Stream a stored cluster shard.

#### `DELETE /n2/internal/delete/{fragment}/{version_id}/{cluster_shard_index}`

Delete a stored cluster shard.

### Internal Object-Record Replication

#### `POST /n2/internal/object-record/upsert`

Body:

- `N2ObjectRecord`

The replicated body is persisted as `.nmtd` on receiving peers.

#### `GET /n2/internal/object-record/{fragment}`

Read fragment-local object-record state.

#### `DELETE /n2/internal/object-record/{fragment}`

Delete object-record state for a fragment.

#### `GET /n2/internal/health/node`

Internal token only. Returns local node health for peer cluster monitoring.

#### `GET /n2/internal/health/shard/{fragment}/{version_id}/{cluster_shard_index}`

Internal token only. Returns local presence/size health for one cluster shard without downloading the shard payload.

### Internal Governance Replication

#### `POST /n2/internal/governance/request-vote`

Body:

```json
{
  "term": 8,
  "candidate_id": "node-b",
  "candidate_endpoint": "https://10.0.0.2:50001",
  "last_commit_index": 12,
  "last_applied_membership_epoch": 12
}
```

#### `POST /n2/internal/governance/append`

Body:

```json
{
  "term": 8,
  "leader_id": "node-a",
  "leader_endpoint": "https://10.0.0.1:50001",
  "prev_commit_index": 11,
  "leader_commit_index": 12,
  "entry": {
    "index": 12,
    "term": 8,
    "membership": {
      "epoch": 12,
      "nodes": []
    }
  }
}
```

#### `POST /n2/internal/governance/abort`

Body:

```json
{
  "term": 8,
  "leader_id": "node-a",
  "pending_index": 13
}
```

## Typical Operator Flows

### Placement Inspection

```bash
curl -k \
  "https://127.0.0.1:50001/n2/placement/fragment-a/videos/demo.mp4?kc=2&mc=1" \
  -H "Authorization: Bearer <token>"
```

### Cluster Upload

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n2/object/fragment-a/videos/demo.mp4?kc=2&mc=1&content_type=video/mp4" \
  -H "Authorization: Bearer <token>" \
  --data-binary @demo.mp4
```

### Cluster Read

```bash
curl -k \
  "https://127.0.0.1:50001/n2/object/fragment-a/videos/demo.mp4" \
  -H "Authorization: Bearer <token>" \
  -o demo.mp4
```

### Reconcile After Membership Change

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n2/reconcile/fragment-a?cleanup_orphans=true" \
  -H "Authorization: Bearer <token>"
```

## Operational Notes

- `N2` write success is governed by cluster commit mode and available placement targets, not just by whether the local node is healthy.
- The public and protected cluster-view responses are the first place to look when a cluster appears writable but ingest is being rejected.
- Membership changes are operator workflows. Treat them like control-plane operations, not end-user object operations.
- Internal peer routes are intentionally low-level and should not be used as an application integration surface.
- `Registry` remains the right layer for user-facing logical namespace, quota, share, and account behavior above `N2`.
- `N2 0.7.1` uses `N1` locally and persists distributed object truth as immutable `.nmtd` files under stable object-id directories. Retired N1 indexdb files are not active N2 metadata inputs.
- `N2 0.7.1` performs one cluster EC pass. Local shard storage is `kd=1`, `md=0`, so uploads do not pay a second per-node EC encode.
- `N2 0.7.1` keeps health checks metadata/shard-presence oriented and keeps repair bounded through explicit gates and streamed shard rebuild.
- Prefix listing uses the N2 namespace tree first. Delimiter listing is direct-directory IO, not a whole object-record tree scan.
- Two-node `N2 0.7` health/failure proof was run on real devices on 2026-05-26 with one node on `192.168.0.22` and one node on `192.168.0.13`, `kc=1`, `mc=1`, `kd=1`, `md=0`, and a 304 MiB real video object. Upload, download hash match, shard-loss degradation, read while degraded, automatic repair, node outage, and node rejoin all passed. Longer soak and larger multi-object proof are still separate release-gate work.
- The source tree now keeps N2 faces separate: LOA HTTP API routes, private peer API routes, and native `n2://` protocol code are separate modules.
