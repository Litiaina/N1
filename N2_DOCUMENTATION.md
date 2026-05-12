# N2 0.4 API Documentation

`N2` is the multi-node clustering and placement layer for the `N1` storage kernel. It is a rewrite of the active N2 storage path: N2 owns distributed placement, peer transport, reconcile, and cluster object metadata while each node uses `N1` as its local storage kernel.

This document focuses on:

- the public and operator-facing `N2` APIs
- the S3-compatible path-style object surface
- the request and response fields that matter to integrators
- the difference between public routes and node-to-node internal routes

## What N2 Owns

`N2` adds the cluster plane:

- cluster membership
- cluster view and governance state
- rendezvous placement of cluster shards
- multi-node object ingest and read
- real-prefix N2 namespace directories with symlink object entries
- immutable filesystem `.nmtd` object-map persistence and reconcile
- internal peer transport between `N2` nodes

`N2` does not use N1's rich namespace for user object keys. N2 owns the distributed namespace and stores physical shard objects into the local N1 storage kernel using internal shard identity. Distributed EC is performed once at the N2 layer; local N1 shard storage is a single local shard object (`kd=1`, `md=0`) by design.

## N2 0.4 Rewrite Structure

N2 0.4 is being shaped as a clean Litiaina cluster layer over the N1 kernel, not as a carry-over of the old pre-N1-kernel N2 runtime.

The active code boundaries are:

- native N2 routes for cluster/object APIs under `/v4/n2/...`
- peer routes for node-to-node shard, map, and governance traffic under `/v4/n2/internal/...`
- S3 route face under `/s3/...`
- `.nmtd` metadata primitives for filesystem object-map paths and names
- N1 as the local storage kernel dependency

This keeps S3 compatibility as a product face, peer transport as a private cluster face, and `.nmtd` as the N2 metadata identity.

## N2 0.4 Storage Layout

N2 uses filesystem metadata, not an external database.

The mutable N2 namespace is a real directory tree. Object names are symlinks to immutable object-map records:

```text
<index_root>/<fragment_hash>/n2/namespace/videos/demo.mp4
  -> <index_root>/<fragment_hash>/n2/objects/<hh>/<hh>/<hh>/<hh>/<object_key_hash>
```

Object maps are immutable `.nmtd` files under each configured index root:

```text
<index_root>/<fragment_hash>/n2/objects/<hh>/<hh>/<hh>/<hh>/<object_key_hash>/<version_id>/.nmtd
<index_root>/<fragment_hash>/n2/objects/<hh>/<hh>/<hh>/<hh>/<object_key_hash>/.nlatest
```

Normal prefix listing reads the real namespace directory first. A delimiter listing such as `prefix=videos/&delimiter=/` opens the `videos/` namespace directory directly and returns direct children/common prefixes without walking the whole cluster map tree.

Local cluster shards are stored under each configured blob root:

```text
<blob_root>/<fragment_hash>/n2/shards/<version_id>/cshard_<cluster_shard_index>/
  disk_shard_0.bin
  .nmtd
```

`disk_shard_0.bin` is the exact cluster shard payload. N2 0.4 does not run a second local erasure-code pass under each node. Node/disk survivability is handled by the N2 cluster EC plan and by the local N1 storage kernel around the shard object.

Forbidden legacy active metadata for N2 0.4:

- `.n2map`
- `.n2latest`
- `cshard.meta`
- local second-layer `kd>1` or `md>0` EC for N2 shard storage
- any retired N1 indexdb metadata as the active N2 map authority

The local `N1` kernel may still own its own runtime files and fragment metadata. N2's distributed object truth is `.nmtd`.

## Base URL

Typical base URL:

```text
https://127.0.0.1:50001
```

`N2` native routes live under `/v4/n2/...`.

S3-compatible path-style routes live under `/s3/...`.

The `/n1/...` routes remain available for fragment creation/auth and local N1-kernel operations. New clustered object traffic should use `/v4/n2/...` or `/s3/...`.

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

## S3-Compatible Path-Style Object Surface

N2 exposes a path-style object surface for S3-oriented clients and gateways. The route prefix is `/s3`, and the bucket name is the fragment hash.

Authentication currently uses the same N2 bearer token as the native API:

```text
Authorization: Bearer <access_token>
```

Implemented object operations:

- `GET /s3` list accessible buckets for the current token
- `HEAD /s3/{bucket}` check bucket access
- `GET /s3/{bucket}?list-type=2&prefix=...&delimiter=...&max-keys=...&continuation-token=...`
- `PUT /s3/{bucket}/{*key}` upload an object
- `HEAD /s3/{bucket}/{*key}` object metadata
- `GET /s3/{bucket}/{*key}` read an object
- `DELETE /s3/{bucket}/{*key}` delete an object
- `POST /s3/{bucket}/{*key}?uploads` initiate a multipart upload
- `PUT /s3/{bucket}/{*key}?partNumber=<n>&uploadId=<id>` upload a multipart part
- `GET /s3/{bucket}/{*key}?uploadId=<id>` list uploaded parts
- `POST /s3/{bucket}/{*key}?uploadId=<id>` complete multipart upload
- `DELETE /s3/{bucket}/{*key}?uploadId=<id>` abort multipart upload

S3 multipart upload state is staged outside committed object truth under the local N2 runtime storage area. Parts are not visible as objects. Completion streams the ordered parts into the N2 object plane and commits one `.nmtd` object map. Aborting removes the staging directory. This preserves the N2 rule that committed distributed object truth is `.nmtd`.

Examples:

```bash
curl -k -X PUT \
  "https://127.0.0.1:50001/s3/<fragment_hash>/videos/demo.mp4" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: video/mp4" \
  --data-binary @demo.mp4

curl -k \
  "https://127.0.0.1:50001/s3/<fragment_hash>?list-type=2&prefix=videos/" \
  -H "Authorization: Bearer <token>"

curl -k \
  "https://127.0.0.1:50001/s3/<fragment_hash>/videos/demo.mp4" \
  -H "Authorization: Bearer <token>" \
  -o demo.mp4
```

Multipart example:

```bash
UPLOAD_ID=$(curl -ks -X POST \
  "https://127.0.0.1:50001/s3/<fragment_hash>/videos/big.bin?uploads" \
  -H "Authorization: Bearer <token>" | sed -n 's:.*<UploadId>\\(.*\\)</UploadId>.*:\\1:p')

curl -k -X PUT \
  "https://127.0.0.1:50001/s3/<fragment_hash>/videos/big.bin?partNumber=1&uploadId=${UPLOAD_ID}" \
  -H "Authorization: Bearer <token>" \
  --data-binary @part-001.bin

curl -k -X POST \
  "https://127.0.0.1:50001/s3/<fragment_hash>/videos/big.bin?uploadId=${UPLOAD_ID}" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/xml" \
  --data '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>'
```

Responses use S3-style XML for bucket/list operations and S3 object headers such as `ETag` and `x-amz-version-id`.

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
  - `kd` (`1` in N2 0.4)
  - `md` (`0` in N2 0.4)
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
- `kd` required, must be `1`
- `md` required, must be `0`
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

The replicated body is persisted as `.nmtd` on receiving peers.

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
  "https://127.0.0.1:50001/v4/n2/object/fragment-a/videos/demo.mp4?kc=2&mc=1&content_type=video/mp4" \
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
- `N2 0.4` uses `N1` locally and persists distributed object truth as `.nmtd`. Retired N1 indexdb files are not active N2 metadata inputs.
- `N2 0.4` performs one cluster EC pass. Local shard storage is `kd=1`, `md=0`, so uploads do not pay a second per-node EC encode.
- Prefix listing uses the N2 namespace tree first. Delimiter listing is direct-directory IO, not a whole-map scan.
- The older `N2 0.3` proof remains historical evidence for the N1-kernel rewrite. `N2 0.4` requires a fresh multi-node proof before a 1.0 release decision.
- The source tree now keeps N2 faces separate: native API routes, private peer API routes, and S3 routes are separate modules. S3 multipart staging is also isolated under the S3 face instead of being mixed into the core data plane.
