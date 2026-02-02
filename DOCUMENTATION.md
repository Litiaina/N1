<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1 Storage API (v4) — Complete Reference</h1>
  <p>Routes and behavior aligned to <code>routes.rs</code> (fragment‑centric N1).</p>
</div>

> This document reflects the **current v4 router layout**: fragment-centric object storage (CAS + EC), separate auth endpoints, maintenance jobs, public sharing, and WebSocket events.

---

## Table of Contents

- [Base URL & Versioning](#base-url--versioning)
- [Authentication](#authentication)
  - [Fragment auth (object storage)](#fragment-auth-object-storage)
  - [FS auth (legacy fs routes)](#fs-auth-legacy-fs-routes)
- [Permissions & Common Rules](#permissions--common-rules)
- [Error Responses](#error-responses)
- [Core Data Models](#core-data-models)
- [Object Storage Read APIs](#object-storage-read-apis)
- [Object Storage Write APIs](#object-storage-write-apis)
- [Upload APIs (multipart / resumable)](#upload-apis-multipart--resumable)
- [Sharing APIs](#sharing-apis)
- [Maintenance APIs](#maintenance-apis)
- [WebSocket APIs](#websocket-apis)
- [Public + Bootstrapping APIs](#public--bootstrapping-apis)
- [Quick Endpoint Index](#quick-endpoint-index)

---

## Base URL & Versioning

All endpoints here are under **v4** unless stated otherwise.

- Example base URL: `https://storage.example.com`
- Example endpoint: `GET https://storage.example.com/v4/fragment/list`

---

## Authentication

N1 has **separate auth entry points** for:
1) **Object Storage v4** (fragment-scoped JWT)  
2) **Legacy FS routes** (`/dir/...`), which are still present for compatibility

### Fragment auth (object storage)

**POST** `/authenticate/fragment`

Purpose:
- Obtain a **fragment-scoped JWT** used for all `/v4/...` protected object-storage routes.
- The token typically encodes: `fragment`, and permissions booleans: `read/write/modify/delete`.

Typical request body (shape may vary by your implementation):

```json
{
  "fragment": "my-fragment",
  "secret": "fragment-secret",
  "read": true,
  "write": true,
  "modify": true,
  "delete": true
}
```

Typical response:

```json
{
  "access_token": "<JWT>"
}
```

Use it on protected v4 routes:

```
Authorization: Bearer <JWT>
```

### FS auth (legacy fs routes)

**POST** `/authenticate/fs`

Purpose:
- Authenticate the legacy FS API under `/dir/...`.

Request/response shape depends on your implementation (often email+password or user+password).

---

## Permissions & Common Rules

### Permission flags (Claims)
The v4 object-storage routes are wrapped with `validate_access`, and handlers commonly enforce:

- `read`: list, get, stream, metadata, download, status endpoints
- `write`: upload, delete, recover, create signed URL (varies), etc.
- `modify`: fragment modify, rename operations
- `delete`: destructive operations (delete/permanent/trash empty, stop/start jobs depending)

If a token lacks the required permission, the server returns **403 Forbidden**.

### Fragment parameter
Most v4 routes are **fragment-scoped**:
- the JWT must match the fragment path parameter
- the server rejects cross-fragment access

### Object keys
Routes using `{*object_key}` capture the rest of the path. Clients should:
- keep slashes as path separators
- percent-encode special characters (`#`, `%`, spaces, etc.)

### Range support
Streaming/download endpoints generally support:

```
Range: bytes=start-end
```

### Upload parts
`PUT /v4/upload/chunk/...` requires:
- `Content-Length` header
- part indices are 0-based unless your client chooses otherwise (server expects consistent indexing)

---

## Error Responses

Errors are mapped from your `AppError` to HTTP status codes. Typical patterns:

- `400 Bad Request` — invalid input, reserved path, bad signature
- `401 Unauthorized` — missing/invalid JWT
- `403 Forbidden` — permission denied or fragment mismatch
- `404 Not Found` — object/version missing, expired share link
- `409 Conflict` — invalid state (e.g., missing parts on finalize, job already running)
- `416 Range Not Satisfiable` — invalid range
- `500 Internal Server Error` — unexpected failures / storage backend issues

Payload shape depends on your error handler; common JSON format:

```json
{ "error": "message" }
```

---

## Core Data Models

> Model shapes below reflect the **v4 object storage** handlers (upload.rs, retrieve.rs, etc.).  
> Some admin/fragment manager models may differ if you changed request/response structs.

### InitiateUploadParams

Used by `POST /v4/upload/init/...` and `POST /v4/upload/finalize/...` (some implementations reuse the same struct).

```json
{
  "content_type": "application/octet-stream",
  "total_size_bytes": 123456,
  "expected_parts": 4
}
```

### InitiateUploadResponse

```json
{
  "session_id": "01J...",
  "version_id": "01J..."
}
```

### UploadStatusResponse

```json
{
  "uploaded_parts": [0, 1, 2]
}
```

### FinalizeIncompleteResponse

Returned when finalize is attempted but parts are missing:

```json
{
  "uploaded_parts": [0, 1],
  "missing_parts": [2, 3],
  "expected_parts": 4
}
```

### ListUploadsResponse

```json
{
  "sessions": [
    {
      "session_id": "01J...",
      "version_id": "01J...",
      "object_key": "path/to/file.bin",
      "created_at": 1730000000,
      "reserved_size": 123456,
      "total_size_bytes": 123456,
      "expected_parts": 4
    }
  ],
  "total": 1
}
```

### ObjectMetadata (high level)

The server returns object metadata for reads and finalize responses. The exact schema depends on your internal struct,
but the important stable fields are:

- `fragment`, `object_key`, `version_id`
- `content_type`, `total_size_bytes`
- `created_at`
- `parts[]` (each part has shard list, checksums, etc.)
- `is_delete_marker`

---

## Object Storage Read APIs

All routes below require `Authorization: Bearer <JWT>` and `read=true` unless stated otherwise.

### Get capacity usage (per fragment)
**GET** `/v4/capacity/usage/{fragment}`

Returns fragment usage (implementation-defined payload), commonly:

```json
{ "capacity_used": 123456789 }
```

---

### List fragments (private / authenticated)
**GET** `/v4/fragment/list`

Returns fragments visible to this credential (implementation-defined):

```json
["fragmentA", "fragmentB"]
```

---

### List objects in fragment
**GET** `/v4/fragment/objects/{fragment}`

Typical query params (if implemented):
- `limit`
- `prefix`
- `continuation_token`

Returns list + pagination token.

---

### List trash objects in fragment
**GET** `/v4/fragment/trash/{fragment}`

Same listing semantics as normal list, but for `.trash` namespace.

---

### List object versions
**GET** `/v4/fragment/versions/{fragment}/{*object_key}`

Returns versions newest-first (implementation-defined array of metadata/summary).

---

### Get object bytes (inline)
**GET** `/v4/fragment/get/{fragment}/{*object_key}`

- `Content-Disposition: inline`
- Supports `Range`

---

### Stream object bytes
**GET** `/v4/fragment/stream/{fragment}/{*object_key}`

- Streaming-friendly variant
- Supports `Range`

---

### Get object metadata
**GET** `/v4/fragment/meta/{fragment}/{*object_key}`

Returns full `ObjectMetadata`.

---

### Download object
**GET** `/v4/fragment/download/{fragment}`

Typically uses query parameters like:
- `object_key` (required)
- `version_id` (optional)

Returns:
- `Content-Disposition: attachment`

---

### Download chunk (assembled)
**GET** `/v4/fragment/download/chunk/{fragment}`

Typically uses query parameters like:
- `object_key` (required)
- `offset` (required)
- `chunk_size` (required)

Reads metadata and reconstructs contiguous bytes from shards if needed.

---

### Download archive of selected objects
**POST** `/v4/fragment/archive/{fragment}`

Body example (implementation-defined; common pattern):

```json
{
  "keys": ["a/b.txt", "c/d.png"],
  "format": "zip",
  "filename": "export"
}
```

---

## Object Storage Write APIs

All routes below require `Authorization: Bearer <JWT>` and `write=true` unless stated otherwise.

### Delete fragment
**DELETE** `/v4/fragment/delete/{fragment}`

Requires `delete=true` (and sometimes `modify=true` depending on your policy).

---

### Modify fragment (metadata/config)
**PATCH** `/v4/fragment/modify/{fragment}`

Requires `modify=true`.

---

### Empty trash
**DELETE** `/v4/fragment/trash/empty/{fragment}`

Requires `delete=true`.

---

### Rename object
**PATCH** `/v4/fragment/object/rename`

Requires `modify=true`.

Body example:

```json
{
  "fragment": "my-fragment",
  "source_key": "a/b.txt",
  "destination_key": "a/c.txt"
}
```

---

### Soft delete object (move to trash)
**DELETE** `/v4/fragment/object/delete/{fragment}/{*object_key}`

Requires `delete=true`.

---

### Delete a specific version
**DELETE** `/v4/fragment/version/delete/{fragment}/{version_id}/{*object_key}`

Requires `delete=true`.

---

### Permanent delete object
**DELETE** `/v4/fragment/permanent/delete/{fragment}/{*object_key}`

Requires `delete=true`. Destroys blobs + metadata.

---

### Recover a specific version from trash
**POST** `/v4/fragment/recover/version/{fragment}/{version_id}/{*object_key}`

Requires `write=true` (and often `modify=true` depending on your policy).

---

### Recover latest object from trash
**POST** `/v4/fragment/recover/object/{fragment}/{*object_key}`

---

### Recover ALL trash (bulk restore)
**POST** `/v4/fragment/recover/trash/all/{fragment}`

---

## Upload APIs (multipart / resumable)

Upload flow:
1) `init` → returns `(version_id, session_id)`
2) `chunk` uploads (parts)
3) `status` can be polled for resumability
4) `finalize` publishes metadata (object becomes visible)
5) `cancel` tears down session + quota reservation

### Initiate upload
**POST** `/v4/upload/init/{fragment}/{*object_key}`

Body: `InitiateUploadParams`  
Response: `InitiateUploadResponse`

---

### Upload status (for resumable upload)
**GET** `/v4/upload/status/{fragment}/{version_id}`

Response: `UploadStatusResponse`

---

### List active uploads for fragment
**GET** `/v4/upload/status/list/{fragment}`

Query params (typical):
- `limit`
- `offset`

Response: `ListUploadsResponse`

---

### Upload one part (chunk)
**PUT** `/v4/upload/chunk/{fragment}/{version_id}/{part_index}`

Query params (if implemented):
- `enable_checksum=true|false`
- `checksum=<hex>` (optional)

Headers:
- `Content-Length: <bytes>`

Body:
- raw bytes for that part

---

### Finalize upload
**POST** `/v4/upload/finalize/{fragment}/{version_id}/{session_id}`

Body: `InitiateUploadParams` (often reused for content_type / expected_parts / total_size_bytes)

Returns:
- `200 OK` with object metadata on success
- `409 Conflict` + `FinalizeIncompleteResponse` if missing parts

---

### Cancel upload session
**DELETE** `/v4/upload/cancel/{fragment}/{version_id}`

Cancels an upload that has not been finalized.

---

## Sharing APIs

### Create signed URL (protected)
**POST** `/v4/share/create`

Requires `read=true`.

Body example (implementation-defined):

```json
{
  "fragment": "my-fragment",
  "object_key": "a/b.txt",
  "expires_in": 3600,
  "disposition": "download"
}
```

Response:

```json
{
  "url": "https://storage.example.com/v4/public/share?...",
  "expires_at": "2026-02-02T12:34:56Z"
}
```

### Public share (no auth)
**GET** `/v4/public/share`

Query params depend on your signing format (token, signature, expiry, etc.).  
Returns bytes with disposition controlled by the signed URL.

---

## Maintenance APIs

Maintenance routes require `Authorization: Bearer <JWT>` and generally require `write/modify/delete` depending on risk.

### Sync global quota/health
**POST** `/v4/maintenance/sync/global`

---

### Sync fragment quota
**POST** `/v4/maintenance/sync/fragment`

---

### Cleanup fragment metadata
**POST** `/v4/maintenance/cleanup/fragment`

---

### Initialize fragment storage
**POST** `/v4/maintenance/init/fragment`

Used to bootstrap required on-disk roots for a fragment.

---

### Repair job control
- **POST** `/v4/maintenance/repair/start`
- **POST** `/v4/maintenance/repair/stop`
- **GET**  `/v4/maintenance/repair/status`

Start query params (as implemented in your repair manager):
- `dry_run=true|false`
- `mode=availability|scrub|strict|checksum`
- `meta_concurrency=<n>`
- `repair_concurrency=<n>`
- `batch=<n>`

---

### Garbage collector
**POST** `/v4/maintenance/gc/start`

Starts GC / lifecycle cleanup.

---

### Redistribution job control
- **POST**  `/v4/maintenance/redistribution/start`
- **PATCH** `/v4/maintenance/redistribution/stop`
- **GET**   `/v4/maintenance/redistribution/monitor`

---

### System health
**GET** `/v4/maintenance/health`

Returns disk / index mirror / latency / errors summary (implementation-defined).

---

## WebSocket APIs

### WebSocket upgrade
**GET** `/ws`

Used for real-time events (uploads, repair/redistribution updates, system broadcasts).

### System broadcast (authenticated)
**POST** `/v4/system/broadcast`

Protected by `validate_access`.

---

## Public + Bootstrapping APIs

### List public fragments (no auth)
**GET** `/v4/fragment/list/public`

### Create fragment (auth layer)
**POST** `/v4/fragment/create`

This route is wrapped by `layer_auth` (not the fragment JWT middleware).
Used to **create/initialize fragment identity** (implementation-defined).

Common request shape (from older versions):

```json
{
  "name": "my-fragment",
  "quota": 1099511627776,
  "secret": "fragment-secret"
}
```

### Ping
**GET** `/ping` → `"pong"`

---

## Quick Endpoint Index

### Object storage (protected: validate_access)

**Read**
- `GET  /v4/capacity/usage/{fragment}`
- `GET  /v4/fragment/list`
- `GET  /v4/fragment/objects/{fragment}`
- `GET  /v4/fragment/trash/{fragment}`
- `GET  /v4/fragment/versions/{fragment}/{*object_key}`
- `GET  /v4/fragment/get/{fragment}/{*object_key}`
- `GET  /v4/fragment/stream/{fragment}/{*object_key}`
- `GET  /v4/fragment/meta/{fragment}/{*object_key}`
- `GET  /v4/fragment/download/{fragment}`
- `GET  /v4/fragment/download/chunk/{fragment}`
- `POST /v4/fragment/archive/{fragment}`
- `POST /v4/share/create`

**Write**
- `DELETE /v4/fragment/delete/{fragment}`
- `PATCH  /v4/fragment/modify/{fragment}`
- `DELETE /v4/fragment/trash/empty/{fragment}`
- `PATCH  /v4/fragment/object/rename`
- `DELETE /v4/fragment/object/delete/{fragment}/{*object_key}`
- `DELETE /v4/fragment/version/delete/{fragment}/{version_id}/{*object_key}`
- `DELETE /v4/fragment/permanent/delete/{fragment}/{*object_key}`
- `POST   /v4/fragment/recover/version/{fragment}/{version_id}/{*object_key}`
- `POST   /v4/fragment/recover/object/{fragment}/{*object_key}`
- `POST   /v4/fragment/recover/trash/all/{fragment}`

**Upload**
- `POST   /v4/upload/init/{fragment}/{*object_key}`
- `GET    /v4/upload/status/{fragment}/{version_id}`
- `GET    /v4/upload/status/list/{fragment}`
- `PUT    /v4/upload/chunk/{fragment}/{version_id}/{part_index}`
- `POST   /v4/upload/finalize/{fragment}/{version_id}/{session_id}`
- `DELETE /v4/upload/cancel/{fragment}/{version_id}`

**Maintenance**
- `POST  /v4/maintenance/sync/global`
- `POST  /v4/maintenance/sync/fragment`
- `POST  /v4/maintenance/cleanup/fragment`
- `POST  /v4/maintenance/init/fragment`
- `POST  /v4/maintenance/repair/start`
- `POST  /v4/maintenance/repair/stop`
- `GET   /v4/maintenance/repair/status`
- `POST  /v4/maintenance/gc/start`
- `POST  /v4/maintenance/redistribution/start`
- `PATCH /v4/maintenance/redistribution/stop`
- `GET   /v4/maintenance/redistribution/monitor`
- `GET   /v4/maintenance/health`

### Public
- `GET /v4/public/share`
- `GET /v4/fragment/list/public`
- `GET /ping`

### Auth
- `POST /authenticate/fragment`
- `POST /authenticate/fs`

### WebSocket
- `GET  /ws`
- `POST /v4/system/broadcast`
