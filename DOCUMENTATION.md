<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1 Storage API (v4) — Complete Reference</h1>
</div>

This README documents the **v4 HTTP + WebSocket APIs** exposed by the Litiaina N1 storage service based on the routes and handlers shown in the latest updates.

> **Key concepts**
> - **Index store**: metadata mirrors (`*.bson`) for objects/versions, organized by SHA-256 hash prefix (`/aa/bb/<hash>#<version>.bson`).
> - **Blob store**: erasure-coded shards for each uploaded part, distributed round-robin across configured disks.
> - **Directory shadow index (“structure”)**: an auxiliary prefix index used to accelerate prefix listings; it is **not** the authoritative object index.
> - **Trash**: soft deletes move objects/versions into the fragment’s `.trash` namespace.
> - **Repair / Redistribution**: system maintenance jobs that heal index/blob mirrors (repair) or rebalance shards (redistribute).
> - **GC**: strict garbage collection. When it deletes, it deletes **fully** (and quickly), not “maybe later”.

---

## Table of Contents

- [Base URL & Versioning](#base-url--versioning)
- [Authentication](#authentication)
- [Common Rules](#common-rules)
- [Error Responses](#error-responses)
- [Data Models](#data-models)
- [Read APIs](#read-apis)
- [Write APIs](#write-apis)
- [Sharing APIs](#sharing-apis)
- [Maintenance APIs](#maintenance-apis)
- [Garbage Collector API](#garbage-collector-api)
- [WebSocket APIs](#websocket-apis)
- [Compatibility Notes](#compatibility-notes)

---

## Base URL & Versioning

All endpoints in this doc are under **v4** unless explicitly stated.

- Example base URL:
  - `https://storage.example.com`
- Example full endpoint:
  - `GET https://storage.example.com/v4/fragment/metadata`

---

## Authentication

### 1) Obtain a JWT

**POST** `/authenticate`

Body (form or JSON depending on your implementation; typical is JSON):

```json
{
  "email": "user@example.com",
  "password": "plain-text-password"
}
```

Response:

```json
{
  "token": "<JWT>"
}
```

### 2) Use the JWT

Send the token on all protected routes:

```
Authorization: Bearer <JWT>
```

Protected groups:
- All **Read APIs** except `/v4/public/share`
- All **Write APIs**
- All **Maintenance APIs**
- GC endpoint
- System broadcast endpoint

---

## Common Rules

### Object Keys
- The service normalizes keys by stripping a leading slash:
  - `"/a/b.txt"` → `"a/b.txt"`
- Object keys can contain slashes to represent “folders”.
- Endpoints using `{*object_key}` capture the rest of the path; clients should:
  - keep path slashes as path separators, and
  - percent-encode special characters (spaces, `#`, `%`, etc).

### Reserved Namespace
`.trash` is reserved and cannot be used directly as an object key or prefix.

If a client uses:
- `".trash"` or
- `".trash/anything"`

the server rejects the request with `400 Bad Request`.

### Versioning
- Objects are stored as versions.
- Latest version is determined by the metadata selection logic (commonly version id ordering / created_at).

### Range Reads & Streaming
Endpoints that return object bytes support:
- `Range: bytes=start-end`
- `Accept-Ranges: bytes`

### Checksums
- For single-part objects, a checksum may be exposed in:
  - `x-litiaina-checksum: <checksum>`

### Browser Cache
Some endpoints add browser cache control:
- `Cache-Control: public, max-age=<BROWSER_CACHE_MAX_AGE>`

---

## Error Responses

The server uses an internal `AppError` mapping to HTTP status codes.

Typical patterns:
- `400 Bad Request` — invalid parameters, reserved paths, bad signature
- `401 Unauthorized` — missing/invalid JWT
- `403 Forbidden` — not allowed
- `404 Not Found` — object/version missing, expired share link
- `409 Conflict` — invalid state (e.g., job already running)
- `416 Range Not Satisfiable` — invalid range
- `500 Internal Server Error` — unexpected failures

Payload shape depends on your error handler; if you return JSON, a common format is:

```json
{
  "error": "message"
}
```

---

## Data Models

These are the primary request/response models referenced by the endpoints.

### ObjectSummary
Used for listing, lightweight metadata stored in index files.

```json
{
  "object_key": "path/to/file.bin",
  "fragment": "My Files",
  "version_id": "01H...",
  "upload_session_id": "01H...",
  "owner_uid": "28af29a1-cff5-...",
  "content_type": "application/octet-stream",
  "total_size_bytes": 123456,
  "created_at": 1730000000,
  "is_delete_marker": false,
  "stripe_size_bytes": 262144
}
```

### ObjectMetadata
Full metadata including parts and shards.

```json
{
  "object_key": "path/to/file.bin",
  "fragment": "My Files",
  "version_id": "01H...",
  "upload_session_id": "01H...",
  "owner_uid": "28af29a1-cff5-...",
  "content_type": "application/octet-stream",
  "total_size_bytes": 123456,
  "created_at": 1730000000,
  "parts": [
    {
      "part_index": 0,
      "size_bytes": 123456,
      "created_at": 1730000000,
      "shards": [
        {
          "shard_index": 0,
          "file_name": "shard_0.bin",
          "absolute_path": "/mnt/disk1/.../shard_0.bin",
          "size_bytes": 65536,
          "checksum": "hex...",
          "is_parity": false
        }
      ],
      "checksum": "optional-crc-or-hash"
    }
  ],
  "is_delete_marker": false,
  "stripe_size_bytes": 262144
}
```

### ListObjectsResult
Used by list endpoints.

```json
{
  "objects": [ /* array of ObjectMetadata (often summary-like) */ ],
  "continuation_token": "base64(s1/s2/filename)" 
}
```

If `continuation_token` is `null`, you reached the end.

### CapacityUsageResponse
```json
{ "capacity_used": 123456789 }
```

### Upload API Models

**InitiateUploadParams**
```json
{
  "content_type": "application/octet-stream",
  "total_size_bytes": 123456,
  "expected_parts": 3
}
```

**InitiateUploadResponse**
```json
{
  "session_id": "01H...",
  "version_id": "01H..."
}
```

**UploadStatusResponse**
```json
{
  "uploaded_parts": [0, 1, 2]
}
```

**UploadPartQuery**
```json
{
  "enable_checksum": true,
  "checksum": "optional"
}
```

### RenameObjectRequest
```json
{
  "source_fragment": "My Files",
  "source_key": "a/b.txt",
  "destination_fragment": "My Files",
  "destination_key": "a/c.txt"
}
```

### Signed URL Models

**CreateSignedUrlRequest**
```json
{
  "fragment": "My Files",
  "object_key": "a/b.txt",
  "expires_in": 3600,
  "disposition": "view"
}
```

`disposition` values:
- `view` (default)
- `stream`
- `download`

**SignedUrlResponse**
```json
{
  "url": "https://storage.example.com/v4/public/share?owner_uid=...&fragment=...&object_key=...&expires=...&signature=...&disposition=view",
  "expires_at": "2026-01-23T12:34:56Z"
}
```

---

## Read APIs

All read APIs below require `Authorization: Bearer <JWT>` unless marked **Public**.

### Get capacity usage
**GET** `/v4/capacity/usage`

Response `200`:
```json
{ "capacity_used": 123456789 }
```

---

### List fragments
**GET** `/v4/fragment/metadata`

Response `200`:
```json
["My Files", "Photos", "Backups"]
```

---

### List objects in a fragment
**GET** `/v4/fragment/object/metadata/{fragment}`

Query params:
- `limit` (optional, default `100`, max `1000`)
- `prefix` (optional) — returns objects whose keys start with prefix
- `continuation_token` (optional) — for pagination

Response `200`:
```json
{
  "objects": [ /* ObjectMetadata */ ],
  "continuation_token": "..."
}
```

Notes:
- If `prefix` is provided, the service attempts **DirectoryIndexManager** (shadow index) first for speed.
- If the shadow index yields nothing, it falls back to scanning the authoritative hashed index layout.
- `.trash` prefix is rejected (`400`).

Example:
```bash
curl -H "Authorization: Bearer $JWT" \
  "https://storage.example.com/v4/fragment/object/metadata/My%20Files?limit=200&prefix=photos/2026/"
```

---

### List object versions
**GET** `/v4/fragment/object/versions/{fragment}/{*object_key}`

Response `200`:
```json
[
  { "object_key":"a/b.txt", "version_id":"...", "...":"..." },
  { "object_key":"a/b.txt", "version_id":"...", "...":"..." }
]
```

Sorted newest-first by `created_at`.

---

### Get object bytes (inline)
**GET** `/v4/fragment/object/get/{fragment}/{*object_key}`

- Returns bytes with `Content-Disposition: inline`
- Adds browser cache control

Supports `Range`.

---

### Stream object bytes (inline)
**GET** `/v4/fragment/object/stream/{fragment}/{*object_key}`

- Returns bytes with `Content-Disposition: inline`
- Designed for streaming clients

Supports `Range`.

---

### Get object metadata
**GET** `/v4/fragment/object/get/metadata/{fragment}/{*object_key}`

Response `200`:
```json
{ /* ObjectMetadata */ }
```

---

### Download object (attachment)
**GET** `/v4/fragment/object/download/{fragment}`

Query params:
- `object_key` (required)
- `version_id` (optional)

Returns:
- `Content-Disposition: attachment; filename="..."`

Example:
```bash
curl -L -H "Authorization: Bearer $JWT" \
  "https://storage.example.com/v4/fragment/object/download/My%20Files?object_key=a/b.txt" \
  -o b.txt
```

---

### Download object chunk (byte window, assembled)
**GET** `/v4/fragment/object/download/chunk/{fragment}`

Query params:
- `object_key` (required)
- `offset` (required, u64)
- `chunk_size` (required, usize)

Behavior:
- Reads the object metadata then assembles a contiguous chunk from the erasure-coded layout by calling `read_or_recover_chunk`.
- If `offset >= total_size_bytes`, returns `204 No Content`.

Response:
- `200 OK` with `application/octet-stream`
- Optional `x-litiaina-checksum` when the request aligns exactly with a full part that has a checksum.

---

### Download selected objects as an archive
**POST** `/v4/fragment/object/download/archive/{fragment}`

Body:
```json
{
  "keys": ["a/b.txt", "c/d.png"],
  "format": "zip",
  "filename": "export"
}
```

Rules:
- `keys` must not be empty
- `keys.len()` must be <= `10000`

Response:
- `200 OK`
- `Content-Type: application/zip` or `application/x-tar`
- `Content-Disposition: attachment; filename="export.zip"` (or `.tar`)

---

### List deleted objects (trash)
**GET** `/v4/fragment/trash/object/metadata/{fragment}`

Query params:
- `limit`, `prefix`, `continuation_token` (same behavior as normal list)
- `.trash` is still reserved as a direct key/prefix input

Response is `ListObjectsResult`.

---

### List deleted object versions (trash)
**GET** `/v4/fragment/trash/object/versions/{fragment}/{*object_key}`

Response is an array of `ObjectMetadata` versions present in trash.

---

### Get deleted object metadata (trash)
**GET** `/v4/fragment/trash/object/get/metadata/{fragment}/{*object_key}`

Response `200`:
```json
{ /* ObjectMetadata */ }
```

---

## Write APIs

All write APIs require `Authorization: Bearer <JWT>`.

### Rename object
**PATCH** `/v4/fragment/object/rename`

Body: `RenameObjectRequest`

Response:
- `200 OK` on success (payload depends on handler)

---

### Delete fragment
**DELETE** `/v4/fragment/delete/{fragment_name}`

Deletes an entire fragment (implementation-defined safety rules).

---

### Soft-delete object (move to trash)
**DELETE** `/v4/fragment/object/delete/{fragment}/{*object_key}`

Moves object to `.trash` in that fragment (trash index).

---

### Delete a specific version
**DELETE** `/v4/fragment/object/delete/version/{fragment}/{version_id}/{*object_key}`

Soft-deletes only that version.

---

### Permanently delete object
**DELETE** `/v4/fragment/object/delete/permanent/{fragment}/{*object_key}`

Hard deletes object data and index entries.

> Note: There is also a trash permanent delete route; ensure your router maps to the intended handler.

---

### Restore object from trash
**POST** `/v4/fragment/trash/object/restore/{fragment}/{*object_key}`

Restores the latest trashed version to the active namespace.

---

### Restore specific version from trash
**POST** `/v4/fragment/trash/object/restore/version/{fragment}/{version_id}/{*object_key}`

Restores a specific trashed version.

---

### Permanently delete from trash
**DELETE** `/v4/fragment/trash/object/delete/permanent/{fragment}/{*object_key}`

Hard deletes trashed entries (and associated blobs).

---

## Upload APIs

All upload APIs require `Authorization: Bearer <JWT>`.

### Initiate upload
**POST** `/v4/fragment/object/upload/init/{fragment}/{*object_key}`

Body: `InitiateUploadParams`

Response `200`:
```json
{
  "session_id": "01H...",
  "version_id": "01H..."
}
```

---

### Upload status (resume support)
**GET** `/v4/fragment/object/upload/status/{fragment}/{version_id}`

Response `200`:
```json
{ "uploaded_parts": [0, 1, 2] }
```

Use this to resume uploads without re-selecting files on the client.

---

### Upload part (chunk)
**PUT** `/v4/fragment/object/upload/chunk/{fragment}/{version_id}/{part_index}`

Query:
- `enable_checksum` (optional)
- `checksum` (optional)

Body:
- raw bytes for this part

Response:
- `200 OK` (implementation-defined), or
- appropriate error on mismatch/corruption.

---

### Finalize upload
**POST** `/v4/fragment/object/upload/finalize/{fragment}/{version_id}/{session_id}`

Completes an upload and writes final object metadata.

Response:
- `200 OK` on success
- If incomplete, implementations often return a “missing parts” payload.

---

### Cancel upload session
**DELETE** `/v4/fragment/object/upload/cancel/{fragment}/{version_id}`

Cancels an active upload session; server should clean temporary state.

---

## Sharing APIs

### Create signed URL (protected)
**POST** `/v4/fragment/object/share`

Auth: required

Body: `CreateSignedUrlRequest`

Rules:
- `expires_in` cannot exceed **7 days** (`604800` seconds)

Response `200`: `SignedUrlResponse`

---

### Get shared object (public)
**GET** `/v4/public/share`

Auth: **not required** (public)

Query params:
- `owner_uid` (required)
- `fragment` (required)
- `object_key` (required)
- `expires` (required, unix timestamp seconds)
- `signature` (required, hex HMAC)
- `disposition` (optional: `view|stream|download`)

Behavior:
- Rejects if current time > `expires` (returns `404 Not Found`)
- Recomputes signature using `HMAC-SHA256` over:
  - `owner_uid:fragment:object_key:expires:disposition`
- If signature matches, streams bytes using `Content-Disposition`:
  - `inline` for `view|stream`
  - `attachment` for `download`

Example:
```bash
curl -L "https://storage.example.com/v4/public/share?owner_uid=...&fragment=...&object_key=a%2Fb.txt&expires=...&signature=...&disposition=download" -o b.txt
```

---

## Maintenance APIs

All maintenance APIs require `Authorization: Bearer <JWT>`.

> These endpoints start long-running jobs in the background and return quickly.

### Trigger redistribution job
**POST** `/v4/system/maintenance/redistribute`

Starts shard redistribution / rebalance job.

---

### Stop redistribution job
**PATCH** `/v4/system/maintenance/redistribute/stop`

Stops the active redistribution job.

---

### Sync quota for all users
**POST** `/v4/system/maintenance/sync/all`

Scans users in the DB, ensures:
- quota file exists
- blob directories exist
- stale `.gc` files are swept
- calculated usage is aligned (max across index mirrors)

Returns `202 Accepted` (job started).

---

### Trigger repair job
**POST** `/v4/system/maintenance/repair`

Query params:
- `dry_run=true|false` (default false)
- `mode=availability|scrub|strict|checksum` (default `availability`)
- `meta_concurrency` (default 8, clamp 1..64)
- `repair_concurrency` (default 2, clamp 1..16)
- `batch` (default 256, clamp 32..4096)

Behavior:
- **Availability mode**: checks shard presence + size
- **Scrub/strict/checksum mode**: verifies shard integrity using stored checksums
- Repairs:
  - metadata mirror divergence (index replicas)
  - missing/corrupt shards via EC reconstruction (`repair_entire_shard`)
- Sends progress over WebSocket broadcast as `RepairUpdate`

Returns:
- `202 Accepted` when started
- `409 Conflict` if a job is already running

---

### Stop repair job
**POST** `/v4/system/maintenance/repair/stop`

Signals the repair manager to stop.
Returns `200 OK`.

---

### System health
**GET** `/v4/system/maintenance/health`

Returns a system health payload (implementation-defined).

---

## Garbage Collector API

All GC APIs require `Authorization: Bearer <JWT>`.

### Run GC (manual)
**POST** `/v4/storage/gc/run`

Behavior (strict GC):
- Performs a full GC scan.
- Deletes expired versions / stale uploads based on retention and TTL rules.
- When it deletes, it does so **fully and efficiently** (no leftovers).

Returns `202 Accepted` (GC started in background).

---

## WebSocket APIs

### WebSocket upgrade
**GET** `/ws`

Upgrades to WebSocket. Used for real-time events:
- repair progress broadcasts
- system broadcast messages
- other storage events

### System broadcast (authenticated)
**POST** `/v4/system/broadcast`

Sends a system-wide message/event to connected clients (payload depends on handler).

---

## Compatibility Notes

- **Shadow directory index (`structure/`)** is designed for **fast prefix listing** only.
  - It should be updated asynchronously on write operations (insert/remove).
  - It is **not** a replacement for repairing the authoritative index/blobs.
- **Repair manager** targets:
  - authoritative index mirror divergence
  - shard availability/integrity
  - quota mirror alignment
- **Quota sync** endpoints target:
  - aligning quota used vs actual metadata
  - removing orphan upload sessions/shards
  - ensuring required directory structure exists

---

## Quick Endpoint Index

### Protected Read
- `GET  /v4/capacity/usage`
- `GET  /v4/fragment/metadata`
- `GET  /v4/fragment/object/metadata/{fragment}`
- `GET  /v4/fragment/object/versions/{fragment}/{*object_key}`
- `GET  /v4/fragment/object/get/{fragment}/{*object_key}`
- `GET  /v4/fragment/object/stream/{fragment}/{*object_key}`
- `GET  /v4/fragment/object/get/metadata/{fragment}/{*object_key}`
- `GET  /v4/fragment/object/download/{fragment}`
- `GET  /v4/fragment/object/download/chunk/{fragment}`
- `POST /v4/fragment/object/download/archive/{fragment}`
- `POST /v4/fragment/object/share`
- `GET  /v4/fragment/trash/object/metadata/{fragment}`
- `GET  /v4/fragment/trash/object/versions/{fragment}/{*object_key}`
- `GET  /v4/fragment/trash/object/get/metadata/{fragment}/{*object_key}`

### Protected Write
- `PATCH  /v4/fragment/object/rename`
- `DELETE /v4/fragment/delete/{fragment_name}`
- `DELETE /v4/fragment/object/delete/{fragment}/{*object_key}`
- `DELETE /v4/fragment/object/delete/version/{fragment}/{version_id}/{*object_key}`
- `DELETE /v4/fragment/object/delete/permanent/{fragment}/{*object_key}`
- `POST   /v4/fragment/trash/object/restore/{fragment}/{*object_key}`
- `POST   /v4/fragment/trash/object/restore/version/{fragment}/{version_id}/{*object_key}`
- `DELETE /v4/fragment/trash/object/delete/permanent/{fragment}/{*object_key}`
- `POST   /v4/fragment/object/upload/init/{fragment}/{*object_key}`
- `GET    /v4/fragment/object/upload/status/{fragment}/{version_id}`
- `PUT    /v4/fragment/object/upload/chunk/{fragment}/{version_id}/{part_index}`
- `POST   /v4/fragment/object/upload/finalize/{fragment}/{version_id}/{session_id}`
- `DELETE /v4/fragment/object/upload/cancel/{fragment}/{version_id}`
- `POST   /v4/maintenance/sync/user`
- `POST   /v4/storage/sync/user`
- `POST   /v4/storage/sync/user/initialize`

### Public
- `GET /v4/public/share`

### System Maintenance
- `POST  /v4/system/maintenance/redistribute`
- `PATCH /v4/system/maintenance/redistribute/stop`
- `POST  /v4/system/maintenance/sync/all`
- `POST  /v4/system/maintenance/repair`
- `POST  /v4/system/maintenance/repair/stop`
- `GET   /v4/system/maintenance/health`

### GC
- `POST /v4/storage/gc/run`

### WebSocket
- `GET /ws`
- `POST /v4/system/broadcast`
