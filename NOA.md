# Litiaina Node Object API (NOA)

NOA is the versioned HTTPS and WebSocket contract shared by Litiaina object
storage engines. N1 3.2.0 is the first reference implementation of NOA v1.

NOA standardizes the public transport contract. It does not standardize or
replace an engine's namespace representation, object metadata, erasure-code
placement, repair algorithms, lifecycle implementation, or disk topology.

## Version Boundary

- API name: `NOA`
- API version: `v1`
- HTTPS prefix: `/noa/v1`
- Realtime endpoint: `/noa/v1/ws`
- First N1 implementation: `3.2.0`

The NOA version is independent of the N1 package version and N1 engine format.
A future N1 package can continue implementing NOA v1 without changing the URL.
An incompatible public API requires a new NOA prefix such as `/noa/v2`.

N1 3.2.0 deliberately replaces the former `/n1/...` HTTPS namespace. It does
not expose permanent aliases for the old paths. This prevents two public API
names from becoming separate compatibility contracts.

The native `n1://` protocol remains an N1-specific trusted transport. It calls
the same engine operations but is not renamed or presented as part of NOA.

## Compatibility Rule

NOA v1 began from the N1 3.1.1 HTTP methods, request bodies, query parameters,
response bodies, status codes, authentication behavior, and streaming
behavior. The contract change in N1 3.2.0 was the public route namespace and
API identity. Later N1 3.2.x bug fixes continue to use the same NOA v1 route
families; internal namespace, lifecycle, EC, and healing implementations are
not part of the NOA standard.

Clients authenticate protected calls with:

```http
Authorization: Bearer <access-token>
```

The complete field-level request and response contract is documented in
`DOCUMENTATION.md`. The route families below are the canonical NOA v1 surface
implemented by N1.

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
```

Deletion remains soft-only. TTL lifecycle owns final reclamation; NOA v1 does
not add empty-trash, hard-delete, or per-version-delete semantics.

## Uploads

```text
POST   /noa/v1/upload/one-shot/{object_key}
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

The standard multipart family is process-resumable and optimized for concurrent
ingest: initialization/status/part completion are memory-resident and finalize
is the only durability/publication boundary. The durable family retains
restart-resumable manifests and part receipts. Both publish the same immutable
N1 object and namespace representation after successful finalize.

## Maintenance And Object Health

```text
GET  /noa/v1/maintenance/health
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

NOA exposes the observable operation and status. N1 continues deciding how the
operation reaches its object-level EC set and local roots. A future N2 NOA v1
implementation can use voters, partitions, and distributed placement without
exposing those internals through ordinary object calls.

## Conformance Boundary

An implementation conforms to NOA v1 only when the same client request has the
same externally visible meaning, authentication rules, success response, error
class, and retry behavior. Matching route names alone is insufficient.

N1's route tests, Shell tests, signed-read tests, and release-proof client use
the canonical prefix so API drift fails the normal workspace verification.
The bounded real-process conformance gate is available through the release
proof runner's `--noa-api-proof` mode.
