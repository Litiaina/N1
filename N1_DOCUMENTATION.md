<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1 Documentation</h1>
  <p>Stable <code>1.0.0</code> reference for the pure N1 fragment-scoped storage engine.</p>
</div>

> Status (2026-04-01):
> - `n1` (`litiaina-n1`) is stable at `1.0.0`.
> - N1 is an authenticated fragment-scoped storage engine.
> - N1 owns bytes, object metadata, upload state, repair, redistribution, GC, and health.
> - N1 does not own public sharing, thumbnails, or legacy FS routes in `1.0.0`.
> - Registry, N2, and `n1-arc` are versioned independently and documented separately.

## 1. Scope

N1 `1.0.0` freezes the engine contract under `/n1/*`.

Stable identity:
- authenticated fragment-scoped object storage
- exact-key reads and flat/prefix listing
- multipart and one-shot upload
- metadata-first trash / recover / permanent delete
- repair / redistribution / GC / health
- websocket telemetry for authenticated engine events

Not part of the N1 `1.0.0` HTTP contract:
- legacy `/dir/*`
- `/authenticate/fs`
- public share creation or public share serving
- thumbnail generation
- old N1 `/v4/*` routes

## 2. Base URL And Auth

Base URL example:
- `https://127.0.0.1:50001`

Ping:
- `GET /n1/ping` -> `pong`

Protected engine routes require:
- `Authorization: Bearer <fragment_access_token>`

Fragment access tokens carry:
- `fragment`
- `read`
- `write`
- `modify`
- `delete`
- `exp`

## 3. Authentication

### 3.1 `POST /n1/auth/fragment`

Issues a fragment-scoped access token and refresh token pair.

Request:

```json
{
  "fragment": "<fragment_hash>",
  "secret": "<fragment_key_or_master_key>",
  "read": true,
  "write": true,
  "modify": true,
  "delete": true,
  "expires_in": 900
}
```

Response:

```json
{
  "access_token": "<jwt>",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_token": "<opaque>",
  "refresh_expires_in": 604800
}
```

### 3.2 `POST /n1/auth/refresh`

Rotates a refresh token and returns a new access token pair.

Request:

```json
{
  "refresh_token": "<opaque>"
}
```

### 3.3 `POST /n1/auth/logout`

Revokes a refresh token.

Request:

```json
{
  "refresh_token": "<opaque>"
}
```

## 4. Unprotected Engine Routes

### 4.1 `GET /n1/ping`

Returns `pong`.

### 4.2 `POST /n1/fragments`

Creates a fragment.

Request:

```json
{
  "name": "sample-fragment",
  "secret": "<MASTER_KEY>",
  "key": "<fragment_key>"
}
```

### 4.3 `GET /n1/fragments/check`

Checks whether a fragment exists.

Expected query parameters:
- `fragment=<fragment_hash>`

### 4.4 `GET /n1/fragments/hash`

Resolves the fragment hash for a fragment name.

Expected query parameters:
- `name=<fragment_name>`

## 5. Protected Read Routes

All routes in this section require a fragment bearer token.

### 5.1 Capacity And Listing

- `GET /n1/capacity/usage`
- `GET /n1/fragments`
- `GET /n1/objects`
- `GET /n1/trash`
- `GET /n1/objects/versions/{*object_key}`

Listing notes:
- `GET /n1/objects` returns live objects only.
- `GET /n1/trash` returns trashed objects only.
- prefix/delimiter pagination is supported through query parameters.
- internal synthetic keys stay hidden unless explicitly requested.

Common list query parameters:
- `prefix`
- `delimiter`
- `limit`
- `continuation_token`
- `include_internal`

### 5.2 Object Reads

- `GET /n1/objects/get/{*object_key}`
- `GET /n1/objects/stream/{*object_key}`
- `GET /n1/objects/meta/{*object_key}`
- `GET /n1/objects/download`
- `GET /n1/objects/download/chunk`

Usage guidance:
- `get` is the direct object read.
- `stream` is the streaming read optimized for larger objects.
- `meta` returns current object metadata only.
- `download` and `download/chunk` are query-driven helpers for direct download and chunk-range flows.

### 5.3 Archive Export

N1 keeps the low-level archive primitive for exact object sets.

- `POST /n1/objects/archive`
- `POST /n1/objects/archive/manifest`

Typical archive request:

```json
{
  "object_keys": [
    "a.txt",
    "nested/path/b.txt"
  ],
  "filename": "export",
  "format": "tar"
}
```

## 6. Protected Mutation Routes

### 6.1 Fragment Maintenance

- `DELETE /n1/fragments/{fragment}`
- `PATCH /n1/fragments/{fragment}`

### 6.2 Trash And Object Lifecycle

- `DELETE /n1/trash/empty`
- `POST /n1/trash/recover-all`
- `DELETE /n1/objects/{*object_key}`
- `DELETE /n1/objects/version/{version_id}/{*object_key}`
- `DELETE /n1/objects/permanent/{*object_key}`
- `POST /n1/objects/recover/{*object_key}`
- `POST /n1/objects/recover/version/{version_id}/{*object_key}`
- `POST /n1/objects/rename`
- `POST /n1/objects/bulk-delete`
- `POST /n1/objects/permanent/bulk-delete`
- `POST /n1/objects/bulk-recover`

Rename request:

```json
{
  "source_key": "folder/a.txt",
  "destination_key": "folder/b.txt"
}
```

Bulk request shape:

```json
{
  "object_keys": [
    "a.txt",
    "nested/path/b.txt"
  ]
}
```

Lifecycle model:
- soft delete is metadata-first
- recover is metadata-first
- permanent delete destroys the object version data
- reads may reconstruct bytes on the fly, but they do not silently rebuild a wiped disk subtree

## 7. Upload Routes

N1 `1.0.0` exposes explicit sibling upload modes:
- one-shot
- multipart

### 7.1 One-Shot

- `POST /n1/upload/one-shot/{*object_key}`

Use for small single-request objects when session/finalize overhead is unnecessary.

### 7.2 Multipart

- `POST /n1/upload/multipart/init/{*object_key}`
- `GET /n1/upload/multipart`
- `GET /n1/upload/multipart/{version_id}`
- `PUT /n1/upload/multipart/{version_id}/{part_index}`
- `POST /n1/upload/multipart/{version_id}/{session_id}/finalize`
- `DELETE /n1/upload/multipart/{version_id}`

Multipart flow:
1. init
2. upload parts
3. inspect status if needed
4. finalize
5. or cancel

Typical init request:

```json
{
  "expected_size_bytes": 10485760,
  "content_type": "video/mp4"
}
```

Typical init response fields:
- `version_id`
- `upload_session_id`
- `stripe_size_bytes`
- `part_size_bytes`

## 8. Maintenance Routes

### 8.1 Fragment Storage Maintenance

- `POST /n1/maintenance/fragment/cleanup`
- `POST /n1/maintenance/fragment/init`

### 8.2 Repair

- `POST /n1/maintenance/repair/start`
- `POST /n1/maintenance/repair/stop`
- `GET /n1/maintenance/repair/status`

Repair modes are selected by query parameters, including:
- `mode=availability`
- `mode=strict`
- `mode=checksum`
- `mode=scrub`

Maintenance access model:
- `availability` keeps normal CRUD available
- `strict` moves repair toward read-only isolation
- `scrub` is the heaviest verification mode

### 8.3 Garbage Collection

- `POST /n1/maintenance/gc/start`
- `POST /n1/maintenance/gc/stop`
- `GET /n1/maintenance/gc/status`

### 8.4 Trash Job Status

- `GET /n1/maintenance/trash/recover/status`
- `GET /n1/maintenance/trash/empty/status`

### 8.5 Redistribution

- `POST /n1/maintenance/redistribution/start`
- `POST /n1/maintenance/redistribution/stop`
- `GET /n1/maintenance/redistribution/status`

Redistribution notes:
- adjusts placement for changed disk/root topology and EC layout
- handles downgrade and re-expand flows such as `2 roots -> 1 root -> 2 roots`
- works with the same fragment-scoped metadata authority as CRUD and repair

### 8.6 Health

- `GET /n1/maintenance/health`

Health reports:
- disk/root status
- system resource usage
- upload runtime stats
- index runtime stats
- maintenance status
- degraded / healthy summary

## 9. WebSocket Telemetry

### 9.1 `GET /ws?token=<fragment_access_token>`

WebSocket stays in N1 for authenticated engine telemetry only.

Typical event families:
- repair updates
- redistribution updates
- monitoring / health updates
- storage event payloads scoped to the authenticated fragment

## 10. Permission Model

Fragment tokens are capability-scoped:
- `read`
- `write`
- `modify`
- `delete`

Typical mapping:
- read/list/stream/meta/download/archive -> `read`
- upload init/part/finalize/one-shot -> `write`
- rename / fragment modify -> `modify`
- delete / recover / trash-empty / permanent delete -> `delete`

The fragment in the token is authoritative. Stable N1 routes do not accept a fragment path for
normal object operations because the claim already scopes the request.

## 11. Stable Contract Notes

N1 `1.0.0` freezes these engine decisions:
- route prefix is `/n1/*`
- exact-key authority lives in fragment-level `indexdb/index.ndxdb`
- logical hash bucketing stays internal to the metadata plane
- soft delete and recover are metadata-first lifecycle operations
- disk-loss / wipe rebuild is owned by repair and redistribution, not by reads
- reads may reconstruct bytes on the fly, but they do not silently repopulate a lost disk subtree

## 12. Quick Smoke Example

Create fragment:

```bash
curl -k -X POST "https://127.0.0.1:50001/n1/fragments" \
  -H "Content-Type: application/json" \
  -d '{"name":"sample","secret":"<MASTER_KEY>","key":"fragkey"}'
```

Authenticate:

```bash
curl -k -X POST "https://127.0.0.1:50001/n1/auth/fragment" \
  -H "Content-Type: application/json" \
  -d '{"fragment":"<fragment_hash>","secret":"fragkey","read":true,"write":true,"modify":true,"delete":true}'
```

One-shot upload:

```bash
curl -k -X POST "https://127.0.0.1:50001/n1/upload/one-shot/hello.txt" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain" \
  --data-binary 'hello world'
```

List objects:

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/n1/objects"
```

Get object:

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/n1/objects/get/hello.txt"
```

Health:

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/n1/maintenance/health"
```

## 13. Related Docs

- [DOCUMENTATION.md](/home/administrator/Development/N1/DOCUMENTATION.md) for the workspace docs index
- [README.md](/home/administrator/Development/N1/README.md) for product/runtime overview
- [ARCHITECTURE.md](/home/administrator/Development/N1/ARCHITECTURE.md) for the storage-engine model
- [DEPLOYMENT.md](/home/administrator/Development/N1/DEPLOYMENT.md) for operator setup
- [DEPLOYMENT_PROFILES.md](/home/administrator/Development/N1/DEPLOYMENT_PROFILES.md) for deployment profile split
- [N2_DOCUMENTATION.md](/home/administrator/Development/N1/N2_DOCUMENTATION.md) for multi-node N2 only
- [REGISTRY_DOCUMENTATION.md](/home/administrator/Development/N1/REGISTRY_DOCUMENTATION.md) for the Registry layer
