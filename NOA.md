# Node Object API (NOA)

NOA v1 is N1's canonical HTTPS/WebSocket interface under /noa/v1. It was
introduced in N1 3.2.0; its version is separate from the package/storage format.
The native n1:// transport is N1-specific and is not NOA.

Use Authorization: Bearer <access-token> on protected calls. Fragment permissions
remain independent: read, write, modify and delete. Signed URLs carry narrowly
scoped download authority instead of a reusable fragment token.

N1 4 is development: keys are flat, prefixes virtual, empty mkdir rejected and
prefix mutations non-atomic with per-child results. Old /n1 routes are not
aliases. This inventory names routes; field-level requests, responses, examples
and retry rules are in [DOCUMENTATION.md](DOCUMENTATION.md).

## Connectivity And Authentication

```text
GET  /noa/v1/ping
POST /noa/v1/auth/fragment
POST /noa/v1/auth/refresh
POST /noa/v1/auth/logout
GET  /noa/v1/ws?token=<access-token>
```

## Fragments And Capacity

```text
POST   /noa/v1/fragments
GET    /noa/v1/fragments
GET    /noa/v1/fragments/check
GET    /noa/v1/fragments/hash
PATCH  /noa/v1/fragments/{fragment}
DELETE /noa/v1/fragments/{fragment}
GET    /noa/v1/capacity/usage
GET    /noa/v1/capacity/fragment
```

## POSIX-Like Namespace

```text
GET  /noa/v1/posix/list
GET  /noa/v1/posix/stat
POST /noa/v1/posix/mkdir
POST /noa/v1/posix/rename
POST /noa/v1/posix/move
POST /noa/v1/posix/trash
POST /noa/v1/posix/recover
```

## Objects And Trash

```text
GET    /noa/v1/objects
GET    /noa/v1/namespace/snapshot
GET    /noa/v1/namespace/search
GET    /noa/v1/trash
GET    /noa/v1/objects/meta/{object_key}
GET    /noa/v1/objects/versions/{object_key}
GET    /noa/v1/objects/get/{object_key}
GET    /noa/v1/objects/stream/{object_key}
GET    /noa/v1/objects/download
GET    /noa/v1/objects/download/chunk
POST   /noa/v1/objects/signed-url
GET    /noa/v1/objects/signed-read
DELETE /noa/v1/objects/{object_key}
POST   /noa/v1/objects/recover/{object_key}
POST   /noa/v1/objects/rename
POST   /noa/v1/objects/bulk-delete
POST   /noa/v1/objects/bulk-recover
POST   /noa/v1/trash/recover-all
POST   /noa/v1/objects/archive
POST   /noa/v1/objects/archive/manifest
POST   /noa/v1/exports
GET    /noa/v1/exports/{export_id}
GET    /noa/v1/exports/{export_id}/download
POST   /noa/v1/exports/{export_id}/signed-url
GET    /noa/v1/signed-exports/download
DELETE /noa/v1/exports/{export_id}
```

Deletion remains soft-only. TTL lifecycle owns final reclamation; NOA v1 does
not add empty-trash, hard-delete, or per-version-delete semantics.

Snapshot is a read-authorized NDJSON projection feed with a completion marker.
Search proxies to optional Accel. Optional `N1_ACCEL_READ_URL` also accelerates
version-pinned object bytes after N1 authorization/resolution, including existing
Console media URLs. Pre-header failure falls back to N1; mid-stream failure
terminates the response. Accel never becomes namespace or durability authority.
Exports pin immutable versions, build durable ZIP/TAR artifacts and support
stable length/ETag, range resume and scoped signed downloads. Cancellation and
artifact retention do not delete source objects.

## Uploads

```text
POST   /noa/v1/upload/one-shot/{object_key}
POST   /noa/v1/upload/batch
POST   /noa/v1/upload/multipart/init/{object_key}
GET    /noa/v1/upload/multipart
GET    /noa/v1/upload/multipart/{version_id}
PUT    /noa/v1/upload/multipart/{version_id}/{part_index}
POST   /noa/v1/upload/multipart/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/{version_id}

POST   /noa/v1/upload/multipart/durable/init/{object_key}
GET    /noa/v1/upload/multipart/durable
GET    /noa/v1/upload/multipart/durable/{version_id}
PUT    /noa/v1/upload/multipart/durable/{version_id}/{part_index}
POST   /noa/v1/upload/multipart/durable/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/durable/{version_id}
```

Standard multipart is nonpersistent until finalize; durable multipart retains
restart-resumable progress. Both publish immutable objects after finalization.
Batch accepts independent small objects, returning 201 for all-success or 207
for mixed results. There is no durable batch route.

## Maintenance And Object Health

```text
GET  /noa/v1/maintenance/health
GET  /noa/v1/maintenance/pools/health
GET  /noa/v1/maintenance/fragments/{fragment}/health
GET  /noa/v1/objects/status/{object_key}
POST /noa/v1/objects/repair/{object_key}

POST /noa/v1/maintenance/fragment/init
POST /noa/v1/maintenance/fragment/cleanup

POST /noa/v1/maintenance/repair/start
POST /noa/v1/maintenance/repair/stop
GET  /noa/v1/maintenance/repair/status
POST /noa/v1/maintenance/fragments/{fragment}/repair/start

POST /noa/v1/maintenance/gc/start
POST /noa/v1/maintenance/gc/stop
GET  /noa/v1/maintenance/gc/status
POST /noa/v1/maintenance/fragments/{fragment}/gc/start

POST /noa/v1/maintenance/redistribution/start
POST /noa/v1/maintenance/redistribution/stop
GET  /noa/v1/maintenance/redistribution/status
POST /noa/v1/maintenance/fragments/{fragment}/redistribution/start
POST /noa/v1/maintenance/objects/redistribution/start/{object_key}
```

## Conformance

Maintenance starts require write/modify/delete. Fragment tokens are scoped to
their own fragment; explicit all-fragment work, restrictive modes and global
stops require master-key operator authentication. Pool content diagnostics stay
read-authorized and fragment-scoped. See the maintenance reference for details.

Conformance requires matching request meaning, authentication, responses,
errors and retry behavior, not just route names. N1's workspace tests and the
release runner's --noa-api-proof check the canonical surface. Test evidence and
limits are in [TESTING.md](TESTING.md).
