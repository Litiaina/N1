# N2 API Documentation

`N2` is the multi-node clustering and placement layer that sits on top of the shared `N1` storage kernel. Use it when you want object storage across multiple nodes with cluster-managed placement, cluster shard maps, and reconcile behavior.

This document focuses on:

- the public and operator-facing `N2` APIs
- the request and response fields that matter to integrators
- the difference between public routes and node-to-node internal routes

## What N2 Owns

`N2` adds the cluster plane:

- cluster membership
- cluster view and governance state
- rendezvous placement of cluster shards
- multi-node object ingest and read
- shard-map persistence and reconcile
- internal peer transport between `N2` nodes

`N2` does not replace `N1`'s storage kernel responsibilities. It reuses that core underneath.

## Base URL

Typical base URL:

```text
https://127.0.0.1:50001
```

`N2` routes live under `/v4/n2/...`.

That route namespace is intentional. `N1` moved its stable standalone surface to `/n1/...`, but the current `N2` cluster API still lives under `/v4/n2/...`.

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

Placement and object maps are keyed by:

```json
{
  "uid": "tenant-or-owner-id",
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "version_id": "01KNYQ9TJFK2H6E3TQ8FQ1WQ9W"
}
```

### N2 Object Map

The committed cluster object map contains:

- `epoch`
- `object`
- `params`:
  - `kc`
  - `mc`
  - `kd`
  - `md`
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
    "kd": 2,
    "md": 1
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

### `GET /v4/n2/cluster/view/public`

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

### `GET /v4/n2/cluster/view`

Same shape as the public route, but protected.

### `GET /v4/n2/governance/state`

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

### `GET /v4/n2/membership`

Return current cluster membership.

### `PUT /v4/n2/membership`

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

### `POST /v4/n2/membership/rollback/{epoch}`

Rollback membership to a saved snapshot for the target epoch.

Path parameters:

- `epoch`

Response:

- `ClusterMembership`

### `GET /v4/n2/placement/{fragment}/{*object_key}`

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

### `POST /v4/n2/object/{fragment}/{*object_key}`

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
- `kd` optional
- `md` optional
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
    "...": "N2ObjectMap"
  }
}
```

### `GET /v4/n2/object/{fragment}/{*object_key}`

Read an object from the cluster.

Query parameters:

- `version_id` optional

Supports range requests through the standard `Range` header.

### `GET /v4/n2/object/list/{fragment}`

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

### `GET /v4/n2/object/versions/{fragment}/{*object_key}`

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

### `DELETE /v4/n2/object/{fragment}/{*object_key}`

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
  "deleted_map_files": 1,
  "attempted_shard_deletes": 3,
  "deleted_shard_paths": 3,
  "shard_delete_failures": 0
}
```

### `POST /v4/n2/object/rename`

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
  "deleted_old_map_files": 2
}
```

## Reconcile API

### `POST /v4/n2/reconcile/{fragment}`

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

## Internal Peer API

Everything below `/v4/n2/internal/...` is node-to-node traffic. These routes are not intended as public product-facing APIs.

Still, if you are operating or debugging `N2`, this is the shape.

### Internal Session Upload

#### `POST /v4/n2/internal/session/begin/{fragment}/{version_id}/{cluster_shard_index}`

Query parameters:

- `uid` optional
- `object_key` optional
- `kd` required
- `md` required
- `stripe_block_size` required

Response:

- internal session record including `session_id`

#### `PUT /v4/n2/internal/session/chunk/{session_id}`

Body:

- binary chunk bytes for one cluster shard stream

#### `POST /v4/n2/internal/session/commit/{session_id}`

Finalize the shard session and persist the shard on the target node.

#### `POST /v4/n2/internal/session/abort/{session_id}`

Abort the session and drop temporary state.

### Direct Internal Shard Store / Read / Delete

#### `POST /v4/n2/internal/store/{fragment}/{version_id}/{cluster_shard_index}`

Directly store a full cluster shard on the target node.

#### `GET /v4/n2/internal/read/{fragment}/{version_id}/{cluster_shard_index}`

Stream a stored cluster shard.

#### `DELETE /v4/n2/internal/delete/{fragment}/{version_id}/{cluster_shard_index}`

Delete a stored cluster shard.

### Internal Map Replication

#### `POST /v4/n2/internal/map/upsert`

Body:

- `N2ObjectMap`

#### `GET /v4/n2/internal/map/{fragment}`

Read fragment-local map state.

#### `DELETE /v4/n2/internal/map/{fragment}`

Delete map state for a fragment.

### Internal Governance Replication

#### `POST /v4/n2/internal/governance/request-vote`

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

#### `POST /v4/n2/internal/governance/append`

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

#### `POST /v4/n2/internal/governance/abort`

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
  "https://127.0.0.1:50001/v4/n2/placement/fragment-a/videos/demo.mp4?kc=2&mc=1" \
  -H "Authorization: Bearer <token>"
```

### Cluster Upload

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/v4/n2/object/fragment-a/videos/demo.mp4?kc=2&mc=1&kd=2&md=1&content_type=video/mp4" \
  -H "Authorization: Bearer <token>" \
  --data-binary @demo.mp4
```

### Cluster Read

```bash
curl -k \
  "https://127.0.0.1:50001/v4/n2/object/fragment-a/videos/demo.mp4" \
  -H "Authorization: Bearer <token>" \
  -o demo.mp4
```

### Reconcile After Membership Change

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/v4/n2/reconcile/fragment-a?cleanup_orphans=true" \
  -H "Authorization: Bearer <token>"
```

## Operational Notes

- `N2` write success is governed by cluster commit mode and available placement targets, not just by whether the local node is healthy.
- The public and protected cluster-view responses are the first place to look when a cluster appears writable but ingest is being rejected.
- Membership changes are operator workflows. Treat them like control-plane operations, not end-user object operations.
- Internal peer routes are intentionally low-level and should not be used as an application integration surface.
- `Registry` remains the right layer for user-facing logical namespace, quota, share, and account behavior above `N2`.
- `N2` reuses the `N1 3.0` storage core. Per-fragment metadata authority is still `<index_root>/<fragment_hash>/indexdb/index.ndxdb`; old N1 namespace catalog or redb index files are not active runtime inputs.
