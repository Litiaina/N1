# Litiaina Node Object API (NOA)

NOA is the versioned HTTPS and WebSocket contract shared by Litiaina object
storage engines. N2 3.4 implements NOA v1 over unified static-EC server pools,
flat replicated NCNX keys, and immutable object-level EC records. NOA meanings
do not expose either engine's disk layout.

## Version Boundary

- API name: `NOA`
- API version: `v1`
- HTTPS prefix: `/noa/v1`
- Realtime endpoint: `/noa/v1/ws`
- N2 implementation: `3.4.0`

The NOA version is independent of the N2 package and disk-format versions. An
incompatible public API requires a new prefix such as `/noa/v2`; package
updates can continue implementing NOA v1.

N2 3.4 retains the NOA v1 public `/noa/v1/...` namespace. It does not
retain aliases that would turn the retired routes into a second compatibility
contract.

These N2 identities are intentionally unchanged:

- `n2://` is the trusted N2-native operator and data protocol;
- `/n2/peer/...` is authenticated node-to-node transport;
- `/gateway/...` is N2 Gateway control state;
- `.ncnx`, `.ncdx`, exact EC placement, pool generations, and disk
  paths are engine internals rather than NOA resources.

## Common NOA v1 Surface

N2 uses the common NOA meanings for connectivity, authentication, fragments,
objects, uploads, POSIX-like namespace operations, lifecycle, and maintenance.
Protected calls use:

```http
Authorization: Bearer <access-token>
```

### Connectivity And Authentication

```text
GET  /noa/v1/ping
POST /noa/v1/auth/fragment
POST /noa/v1/auth/refresh
POST /noa/v1/auth/logout
GET  /noa/v1/ws?token=<access-token>
```

### Fragments, Objects, And Trash

```text
POST   /noa/v1/fragments
GET    /noa/v1/fragments
GET    /noa/v1/fragments/check
GET    /noa/v1/fragments/hash
PATCH  /noa/v1/fragments/{fragment}
DELETE /noa/v1/fragments/{fragment}
GET    /noa/v1/capacity/usage

GET    /noa/v1/objects
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
POST   /noa/v1/objects/archive
POST   /noa/v1/objects/archive/manifest
```

Deletion is soft. TTL lifecycle owns physical reclamation; NOA v1 does not
define empty-trash or hard-delete operations.

### Uploads

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

The standard multipart family is speed-first: temporary progress is held in
coordinator memory and independent parts may arrive concurrently. The durable
family persists restart-proof manifests and part receipts. Both publish NCDX
and then NCNX only at finalize. Multipart responses expose N2's coordinator
identity. That is an N2 extension to the shared upload result, not a different
upload meaning.

Standard batch upload is a transport envelope for up to 256 independent
objects of at most 1 MiB each. Every multipart field is named `object`; its
filename is the percent-encoded logical object key. Each entry uses the normal
one-shot EC/NCDX/NCNX commit path and returns its own result. HTTP `201` means
every object committed; `207 Multi-Status` reports mixed independent results.
Batch upload has no shared object transaction, session, or durable-resume
record.

### POSIX-Like Namespace

```text
GET  /noa/v1/posix/list
GET  /noa/v1/posix/stat
POST /noa/v1/posix/mkdir
POST /noa/v1/posix/rename
POST /noa/v1/posix/move
POST /noa/v1/posix/trash
POST /noa/v1/posix/recover
GET  /noa/v1/namespace/operations/{operation_id}
DELETE /noa/v1/namespace/operations/{operation_id}
```

N2 implements these operations over independent flat NCNX key records.
Directories are derived common prefixes, so N2 rejects `mkdir` rather than
pretending an empty directory was persisted. Exact-key rename/move is
destination-first copy/delete. Folder
operations enumerate the prefix and apply that same restart-safe protocol per
key with bounded concurrency; they never rewrite NCDX or object data. Exact-key
mutations return synchronously. Prefix mutations return `202 Accepted` with an
operation ULID and coordinator node, then expose authoritative planning and
per-NCNX execution progress through the operation endpoint. Cancellation is
best-effort and never rolls back keys already committed by copy/delete.

### Health And Maintenance

```text
GET  /noa/v1/objects/status/{object_key}
POST /noa/v1/objects/repair/{object_key}
POST /noa/v1/maintenance/objects/redistribution/start/{object_key}

GET  /noa/v1/maintenance/health
GET  /noa/v1/maintenance/fragments/{fragment}/health
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
```

Repair and redistribution are different contracts. Repair reconstructs an
object's recorded immutable layout in place. Redistribution is an explicit
re-encode that commits a new immutable version under the current layout.
Topology growth never invokes redistribution automatically.

## N2 Cluster Extensions

NOA permits engine-specific capabilities under a versioned extension family.
N2 exposes its distributed control plane under `/noa/v1/cluster/...`:

```text
GET  /noa/v1/cluster/membership
POST /noa/v1/cluster/membership/reload
GET  /noa/v1/cluster/layout
GET  /noa/v1/cluster/health
GET  /noa/v1/cluster/monitor
GET  /noa/v1/cluster/usage
GET  /noa/v1/cluster/placement/preview
GET  /noa/v1/cluster/reserve
POST /noa/v1/cluster/reserve/promote

GET  /noa/v1/cluster/healing/status
POST /noa/v1/cluster/healing/cycle

GET  /noa/v1/cluster/repair/plan
POST /noa/v1/cluster/repair/start
POST /noa/v1/cluster/repair/stop
GET  /noa/v1/cluster/repair/status

GET  /noa/v1/cluster/rebalance/plan
POST /noa/v1/cluster/rebalance/start
POST /noa/v1/cluster/rebalance/stop
GET  /noa/v1/cluster/rebalance/status

POST /noa/v1/cluster/maintenance/distributed/start
GET  /noa/v1/cluster/maintenance/distributed/status
```

The healing cycle uses deterministic fragment ownership within a bounded HA
coordinator pool and a time-bounded fragment lease. Object scans resume from a
persisted continuation token after restart. Independent fragments can run in
parallel, while each repair follows the exact recorded NCNX or NCDX layout and
remains bounded and idempotent. Healing reports are disposable JSON evidence,
never namespace or object authority.

## Conformance Boundary

Matching route names is not sufficient. A NOA implementation must preserve the
same user-visible operation meaning, authentication rule, success response,
error class, streaming behavior, and retry behavior for the shared surface.
Engine-specific placement and namespace algorithms remain private.

N2 route tests reject the retired public prefix, gateway tests verify NOA
forwarding and WebSocket upgrade paths, and the bounded four-node proof
exercises the public API against the actual N2 engine.
