<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1 API Documentation</h1>
  <p>Complete route reference and usage guide (current implementation).</p>
</div>

> Source of truth for routing: `server/src/routes/storage_routes.rs`.
> Source of truth for request/response behavior: the handler files under `server/src/n1_core` and `server/src/fs`.

> Storage backend note:
> - `index_store_disks` must be mounted filesystem paths.
> - `blob_store_disks` are raw, unformatted, unmounted device paths managed directly by N1.
> - Blob payload I/O uses `O_DIRECT`; metadata and control files stay filesystem-backed.

> Metadata architecture note:
> - `index` is the authoritative object, trash, and upload-session metadata plane.
> - Each object or session hashes to one metadata partition with one owner root plus one replica root when multiple index roots exist.
> - The owner root is the synchronous ACK boundary for object/session writes.
> - Replica roots are replica-behind durability copies and may lag briefly until repair/replay catches them up.
> - Prefix and delimiter listing are served from a derived namespace index with inline list payload and are projected from `index` WAL changes into projected namespace snapshots.
> - Legacy mirrored object/session trees are no longer the hot-path authority.
> - Same-key upload recency comes from the upload session's `created_at`; finalize does not get to reorder versions.
> - If two versions share the same second-level timestamp, ULID `version_id` order breaks the tie.
> - Exact-key reads resolve the latest marker and then rehydrate the referenced immutable version record before serving metadata or bytes.
> - Prefix-serving state may lag, but it must never override or invent visibility that `index` does not contain.
> - Multipart uploads do not rely on in-memory part state for resumability. Part truth is durable part metadata on disk, and a pending-part receipt covers the raw-allocation crash window until that metadata exists.

> Release status note:
> - N1 `0.9.1` freezes the storage architecture, not the public API surface.
> - This document describes the current implementation and current routes.
> - Request/response details and endpoint layout may still change before `1.0.0` even though the underlying storage model is now fixed.

## 1. Base URL And Conventions
- Base URL example: `https://127.0.0.1:50001`
- Ping: `GET /ping` -> `pong`
- Protected v4 APIs require: `Authorization: Bearer <fragment_jwt>`
- Most fragment-scoped routes enforce `claims.fragment == {fragment}`
- Object key path captures use `{*object_key}` and support slash-separated keys
- Range requests use standard `Range: bytes=start-end`

## 2. Auth APIs

### 2.1 `POST /authenticate/fragment`
Purpose:
- Issues fragment-scoped JWT for `/v4/*` protected APIs.
- Supports two auth modes:
  - Fragment mode: `secret=<fragment_secret>` and `fragment=<fragment_hash>`
  - Admin mode: `fragment=<ADMIN_KEY>` and `secret=<MASTER_KEY>` (admin fragment bypasses fragment existence checks)

Typical usage:
```bash
curl -k -X POST "https://127.0.0.1:50001/authenticate/fragment" \
  -H "Content-Type: application/json" \
  -d '{
    "fragment": "<fragment_hash>",
    "secret": "<fragment_secret>",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }'
```

Admin maintenance usage:
```bash
curl -k -X POST "https://127.0.0.1:50001/authenticate/fragment" \
  -H "Content-Type: application/json" \
  -d '{
    "fragment": "admin",
    "secret": "<MASTER_KEY>",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }'
```

Notes:
- Admin mode requires exact admin fragment value from `ADMIN_KEY`; any other fragment value follows normal fragment existence validation.
- For maintenance watches (`ops gc/repair/redistribute/prefix-rebuild`), CLI now prints `Last Error` when status polling fails or times out.

### 2.2 `POST /authenticate/fs`
Purpose:
- Issues legacy FS auth context for `/dir/*` routes.

```bash
curl -k -X POST "https://127.0.0.1:50001/authenticate/fs" \
  -H "Content-Type: application/json" \
  -d '{"username":"...","password":"..."}'
```

## 3. Permission Model (Protected v4)
- `read`: listing, metadata, get, stream, download, status endpoints
- `write`: upload, recover, start maintenance jobs
- `modify`: rename/fragment updates and maintenance controls
- `delete`: destructive endpoints and some stop/start controls

Common rejections:
- `401` invalid/missing JWT
- `403` permission denied or fragment mismatch
- `404` object/version not found
- `409` conflicting state (job already running, missing parts, etc.)

## 4. Common Request/Response Schemas

### 4.1 Initiate Upload
Request (`InitiateUploadParams`):
```json
{
  "content_type": "application/octet-stream",
  "total_size_bytes": 1048576,
  "expected_parts": 4
}
```

Response (`InitiateUploadResponse`):
```json
{
  "session_id": "01J...",
  "version_id": "01J..."
}
```

### 4.2 Upload Status
```json
{
  "uploaded_parts": [0, 1, 2]
}
```

### 4.3 Archive Request (`DownloadArchiveRequest`)
```json
{
  "keys": ["reports/a.pdf", "videos/b.mp4"],
  "prefix": "videos/2026",
  "delimiter": "/",
  "max_keys": 10000,
  "continuation_token": "<token>",
  "format": "tar",
  "filename": "export"
}
```
Notes:
- `keys` can be explicit list.
- Or resolve by `prefix` with optional `delimiter` (`/` supported).
- For full prefix recovery at scale, omit `max_keys` to stream the entire prefix in one archive response (server forces `tar`).
- Large archive responses can return header `x-n1-archive-next-token`.

### 4.4 Signed URL Request
```json
{
  "fragment": "<fragment_hash>",
  "object_key": "videos/clip.mp4",
  "expires_in": 3600,
  "disposition": "view"
}
```
`disposition`: `view | stream | download`.

## 5. Public And Global APIs

### 5.1 `GET /ping`
```bash
curl -k "https://127.0.0.1:50001/ping"
```

### 5.2 `GET /v4/fragment/list/public`
Returns public fragment hashes.
```bash
curl -k "https://127.0.0.1:50001/v4/fragment/list/public"
```

### 5.3 `GET /v4/fragment/check`
Implementation expects JSON body with fragment hash.
```bash
curl -k -X GET "https://127.0.0.1:50001/v4/fragment/check" \
  -H "Content-Type: application/json" \
  -d '{"fragment":"<fragment_hash>"}'
```

### 5.4 `GET /v4/fragment/get/hash`
Implementation expects JSON body with fragment name.
```bash
curl -k -X GET "https://127.0.0.1:50001/v4/fragment/get/hash" \
  -H "Content-Type: application/json" \
  -d '{"fragment":"tenant-a"}'
```

### 5.5 `POST /v4/fragment/create`
Requires master secret.
```bash
curl -k -X POST "https://127.0.0.1:50001/v4/fragment/create" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"tenant-a",
    "quota":1099511627776,
    "secret":"<master_key>",
    "key":"<fragment_key>"
  }'
```

### 5.6 `GET /v4/public/share`
No bearer token required. Query is signature-based.
Required query params:
- `fragment`
- `object_key`
- `expires`
- `signature`
Optional:
- `disposition=view|stream|download`

```bash
curl -k "https://127.0.0.1:50001/v4/public/share?fragment=<f>&object_key=<k>&expires=<ts>&signature=<sig>&disposition=view"
```

## 6. Protected v4 Read APIs
All endpoints in this section require a valid fragment JWT with `read=true`.

### 6.1 `GET /v4/capacity/usage/{fragment}`
Returns fragment capacity usage.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/capacity/usage/<fragment_hash>"
```

### 6.2 `GET /v4/fragment/list`
Lists fragment usage objects visible to claims.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/list"
```

### 6.3 `GET /v4/fragment/objects/{fragment}`
Query:
- `limit` default `100`, max `1000`
- `continuation_token` optional
- `prefix` optional
- `delimiter` optional (`/`)
- `include_internal` optional (`true|false`, default `false`)

Notes:
- Internal marker keys (for example `.n1fsd_dir`, `.n1-trash`) are hidden unless `include_internal=true`.
- Normal serving path uses the prefix namespace index.
- Prefix projection runs asynchronously and coalesces repeated mutations per fragment.
- If prefix projection is still pending, or if prefix-serving state is missing or insufficient, the server falls back to authoritative `index` scans for the requested prefix, not a legacy mirrored metadata tree.
- Prefix rows carry inline list payload, so successful prefix hits avoid reopening object metadata for every returned row.

Response shape (`ListObjectsResult`):
- `objects`: array of `ObjectMetadata`
- `common_prefixes`: array (with delimiter listing)
- `continuation_token`: optional

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/objects/<fragment_hash>?prefix=videos/2026&delimiter=/&limit=200"
```

### 6.4 `GET /v4/fragment/trash/{fragment}`
Trash listing, same pagination style.
- Supports the same `include_internal` query behavior as object listing.
- Trash visibility comes from `index` trash state.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/trash/<fragment_hash>?limit=100"
```

### 6.5 `GET /v4/fragment/versions/{fragment}/{*object_key}`
Lists versions for one object key.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/versions/<fragment_hash>/videos/a.mp4"
```

### 6.6 `GET /v4/fragment/get/{fragment}/{*object_key}`
Inline object response. Supports range.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  -H "Range: bytes=0-1048575" \
  "https://127.0.0.1:50001/v4/fragment/get/<fragment_hash>/videos/a.mp4" \
  -o part.bin
```

### 6.7 `GET /v4/fragment/stream/{fragment}/{*object_key}`
Streaming variant. Supports range.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  -H "Range: bytes=0-1048575" \
  "https://127.0.0.1:50001/v4/fragment/stream/<fragment_hash>/videos/a.mp4" \
  -o stream.bin
```

### 6.8 `GET /v4/fragment/meta/{fragment}/{*object_key}`
Returns full object metadata.
- Metadata is resolved from authoritative `index` state.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/meta/<fragment_hash>/videos/a.mp4"
```

### 6.9 `GET /v4/fragment/download/{fragment}`
Query:
- `object_key` required
- `version_id` optional

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/download/<fragment_hash>?object_key=videos/a.mp4" \
  -o a.mp4
```

### 6.10 `GET /v4/fragment/download/chunk/{fragment}`
Query:
- `object_key` required
- `offset` required
- `chunk_size` required

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/download/chunk/<fragment_hash>?object_key=videos/a.mp4&offset=0&chunk_size=1048576" \
  -o chunk.bin
```

### 6.11 `POST /v4/fragment/archive/{fragment}`
Purpose:
- Streams selected objects as `tar` or `zip`.
- Memory bounded; does not materialize whole archive in RAM.

Behavior:
- If selection is very large (>1000 keys), server forces `tar`.
- If `prefix` is used without `max_keys`, server streams all matching keys (unbounded prefix walk, memory bounded) and forces `tar`.
- Response may include `x-n1-archive-next-token` for continuation.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/archive/<fragment_hash>" \
  -d '{
    "prefix":"videos/2026",
    "delimiter":"/",
    "max_keys":5000,
    "format":"tar",
    "filename":"videos-export"
  }' \
  -o videos-export.tar
```

### 6.12 `POST /v4/share/create`
Creates signed public URL for an object.
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/share/create" \
  -d '{
    "fragment":"<fragment_hash>",
    "object_key":"videos/a.mp4",
    "expires_in":3600,
    "disposition":"view"
  }'
```

## 7. Protected v4 Write/Modify/Delete APIs

### 7.1 `DELETE /v4/fragment/delete/{fragment}`
Deletes fragment data on index disks and blob storage backends.
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/delete/<fragment_hash>"
```

### 7.2 `PATCH /v4/fragment/modify/{fragment}`
Body fields:
- `new_name` optional
- `new_quota` optional
- `new_key` optional
- `secret` optional (master secret for admin-level actions)

```bash
curl -k -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/modify/<fragment_hash>" \
  -d '{"new_quota":2199023255552}'
```

### 7.3 `DELETE /v4/fragment/trash/empty/{fragment}`
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/trash/empty/<fragment_hash>"
```

### 7.4 `PATCH /v4/fragment/object/rename`
Body (`RenameObjectRequest`):
```json
{
  "source_fragment": "<fragment_hash>",
  "source_key": "old/name.bin",
  "destination_fragment": "<fragment_hash>",
  "destination_key": "new/name.bin"
}
```

```bash
curl -k -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/object/rename" \
  -d '{"source_fragment":"<f>","source_key":"a.bin","destination_fragment":"<f>","destination_key":"b.bin"}'
```

### 7.5 `DELETE /v4/fragment/object/delete/{fragment}/{*object_key}`
Soft delete (trash marker flow).
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/object/delete/<fragment_hash>/videos/a.mp4"
```

### 7.6 `DELETE /v4/fragment/version/delete/{fragment}/{version_id}/{*object_key}`
Delete specific version.
- If the version is live, it is soft-deleted into trash.
- If the same version is already in trash, the delete permanently removes only that trashed version and leaves newer live versions untouched.
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/version/delete/<fragment_hash>/<version_id>/videos/a.mp4"
```

### 7.7 `DELETE /v4/fragment/permanent/delete/{fragment}/{*object_key}`
Permanent delete.
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/permanent/delete/<fragment_hash>/videos/a.mp4"
```

### 7.8 `POST /v4/fragment/recover/version/{fragment}/{version_id}/{*object_key}`
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/recover/version/<fragment_hash>/<version_id>/videos/a.mp4"
```

### 7.9 `POST /v4/fragment/recover/object/{fragment}/{*object_key}`
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/recover/object/<fragment_hash>/videos/a.mp4"
```

### 7.10 `POST /v4/fragment/recover/trash/all/{fragment}`
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/recover/trash/all/<fragment_hash>"
```

## 8. Protected v4 Upload APIs (Resumable Object Storage Upload)

### Upload flow
1. Initiate session
2. Upload parts (`part_index`)
3. Poll status
4. Finalize
5. Optional cancel

### 8.1 `POST /v4/upload/init/{fragment}/{*object_key}`
Body:
- `content_type` optional
- `total_size_bytes` required
- `expected_parts` required
- `part_size_bytes` optional: freezes deterministic part geometry for fixed-size multipart uploads
- `part_sizes_bytes` optional: freezes deterministic per-part geometry explicitly

Behavior:
- Opens the authoritative multipart session in `index`.
- If geometry is supplied at init, the node pre-reserves deterministic raw shard slots up front so part uploads can reuse them directly.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/upload/init/<fragment_hash>/videos/a.mp4" \
  -d '{"content_type":"video/mp4","total_size_bytes":1073741824,"expected_parts":256,"part_size_bytes":4194304}'
```

### 8.2 `PUT /v4/upload/chunk/{fragment}/{version_id}/{part_index}`
Requirements:
- `Content-Length` header required
- query `checksum` optional
- query `durability` optional: `strict|sync|durable|batched|async|fast|relaxed`
- query `strict_durability` optional bool fallback (`true|false`)
- query `io_gate` optional bool (`true|false`) to force-enable/disable upload I/O gate for that request

Durability default:
- If query flags are omitted, upload-part durability follows `[upload_config].upload_part_strict_durability_default` (`false` by default).
- Upload I/O gating follows `[upload_config].upload_io_gate_enabled` (`true` by default) unless overridden by `io_gate`.

Notes:
- Part upload writes a durable pending-part receipt, commits raw shard payload, then writes durable part metadata on disk.
- The upload hot path does not rely on in-memory part state for resumability.
- If init provided geometry, the part path reuses pre-reserved raw shard slots instead of allocating new extents on the hot path.
- Multipart part commits are unordered across distinct `part_index` values.
- Client coordination should keep one logical writer per `(version_id, part_index)` and use upload-status checks for stateless resume.
- A successful part upload does not publish object visibility by itself.

```bash
curl -k -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Length: 4194304" \
  "https://127.0.0.1:50001/v4/upload/chunk/<fragment_hash>/<version_id>/0?checksum=<optional_crc_or_hash>&durability=batched&io_gate=false" \
  --data-binary @part-000.bin
```

### 8.3 `GET /v4/upload/status/{fragment}/{version_id}`
Behavior:
- Reads the routed upload manifest from `index` and the committed part receipts from disk.
- Returns committed part indexes plus committed byte/part counters for resumability checks.

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/upload/status/<fragment_hash>/<version_id>"
```

### 8.4 `GET /v4/upload/status/list/{fragment}`
Query:
- `limit` default 50, max 100
- `offset` default 0

Behavior:
- Lists active routed multipart sessions from `index` and derives committed progress from durable part receipts on disk.
- Includes committed part/byte progress per session.

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/upload/status/list/<fragment_hash>?limit=50&offset=0"
```

### 8.5 `POST /v4/upload/finalize/{fragment}/{version_id}/{session_id}`
Behavior:
- Loads the authoritative upload session from `index`.
- Verifies the committed parts and publishes the immutable object version into the object's `index` partition.
- Exact-key visibility is gated by this publish step.
- Prefix listing is serving state layered on top of the publish and may be rebuilt from `index`.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/upload/finalize/<fragment_hash>/<version_id>/<session_id>" \
  -d '{"content_type":"video/mp4","total_size_bytes":1073741824,"expected_parts":256}'
```

### 8.6 `DELETE /v4/upload/cancel/{fragment}/{version_id}`
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/upload/cancel/<fragment_hash>/<version_id>"
```

## 9. Protected v4 Maintenance APIs

### 9.1 Quota/metadata sync endpoints
- `POST /v4/maintenance/sync/global`
- `POST /v4/maintenance/sync/fragment`
- `POST /v4/maintenance/cleanup/fragment`
- `POST /v4/maintenance/init/fragment`

Notes:
- Sync and cleanup recompute live and trash usage from authoritative `index` state.
- Quota-file mirrors remain persisted state, but they are no longer the hot-path authority for object truth.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/sync/global"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/sync/fragment"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/cleanup/fragment"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/init/fragment"
```

### 9.2 Repair job endpoints
- `POST /v4/maintenance/repair/start`
- `POST /v4/maintenance/repair/stop`
- `GET /v4/maintenance/repair/status`

Start query params:
- `dry_run=true|false`
- `mode=availability|scrub|strict|checksum`
- `fast_mode=true|false` (enables read-only fast maintenance mode)
- `read_only_fast=true|false` (alias of `fast_mode`)
- `meta_concurrency=1..N` (RAM-adaptive cap; low-RAM nodes clamp lower than high-RAM nodes)
- `repair_concurrency=1..N` (RAM-adaptive cap)
- `batch=32..N` (RAM-adaptive cap)

Notes:
- Repair reconciles partition replicas and blob availability from `index`.
- Full mirrored equality across every index root is no longer the invariant; owner/replica survivability for each partition is.
- `mode=availability` keeps CRUD online and intentionally caps repair pressure lower so the node stays latency-friendly.
- `mode=strict` makes the node read-only and raises repair aggressiveness to use more of the available I/O width.
- `mode=scrub` quiesces reads and writes and runs the same integrity-first repair engine at the highest practical aggressiveness.
- Runtime concurrency is normalized against live blob disks and live index roots, so auto-tuning follows actual reachable hardware instead of static CPU counts alone.
- Fragment object-version scans are streamed incrementally from partition roots, so repair no longer materializes a whole fragment-wide metadata vector before work begins.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/repair/start?dry_run=false&mode=availability&fast_mode=true&meta_concurrency=32&repair_concurrency=8&batch=1024"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/repair/status"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/repair/stop"
```

### 9.3 Garbage collector endpoints
- `POST /v4/maintenance/gc/start`
- `GET /v4/maintenance/gc/status`

Notes:
- GC deletes trashed versions and abandoned upload sessions through `index`.
- Raw payload without authoritative session or object state is cleanup-candidate data.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/gc/start"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/gc/status"
```

### 9.4 Scrub endpoints
- `POST /v4/maintenance/scrub/start`
- `POST /v4/maintenance/scrub/stop`
- `GET /v4/maintenance/scrub/status`

Scrub runs the repair engine in checksum-verification mode against committed raw shards.

Notes:
- Scrub is the quiesced/offline execution mode of the repair engine.
- It blocks reads and mutations so integrity verification and repair can consume more of the available I/O path without serving traffic.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/scrub/start"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/scrub/status"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/scrub/stop"
```

### 9.5 Redistribution endpoints
- `POST /v4/maintenance/redistribution/start`
- `PATCH /v4/maintenance/redistribution/stop`
- `GET /v4/maintenance/redistribution/monitor`

Start query params:
- `dry_run=true|false`
- `mode=availability|strict|scrub`
- `fast_mode=true|false` (enables read-only fast maintenance mode)
- `read_only_fast=true|false` (alias of `fast_mode`)
- `durability=auto|strict|batched` (aliases: `sync|durable|async|fast|relaxed`)
- `durability_mode=auto|strict|batched` (alternate key)
- `strict_durability=true|false` (boolean fallback for durability)
- `object_concurrency=1..N` (RAM-adaptive cap; low-RAM nodes clamp lower)
- `status_batch=1..N` (RAM-adaptive cap)

Notes:
- Redistribution rewrites payload placement first, then republishes updated shard addresses through authoritative `index` metadata.
- `mode=availability` keeps CRUD online and intentionally throttles object concurrency lower so redistribution does not consume the full device bandwidth.
- `mode=strict` makes the node read-only and drives redistribution harder across the live blob/index layout.
- `mode=scrub` quiesces reads and writes and uses the widest redistribution concurrency that the live layout and RAM caps allow.
- Execution `mode` controls service access and scheduler aggressiveness; `durability` still separately controls strict-vs-batched write sync behavior.
- Fragment object-version scans are streamed incrementally from partition roots, so redistribution can start moving data without first loading an entire fragment into memory.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/start?dry_run=false&fast_mode=true&durability=batched&object_concurrency=32&status_batch=128"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/monitor"

curl -k -X PATCH -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/stop"
```

### 9.6 Raw blob status endpoint
`GET /v4/maintenance/blob/raw/status`

Reports one row per configured blob device:

- path
- device id
- status
- mounted flag
- total bytes
- used bytes
- free bytes
- used block count
- journal cursor
- detected foreign signature

```bash
curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/blob/raw/status"
```

### 9.7 Prefix rebuild endpoints
- `POST /v4/maintenance/prefix/rebuild/start`
- `POST /v4/maintenance/prefix/rebuild/stop`
- `GET /v4/maintenance/prefix/rebuild/status`

Notes:
- Prefix rebuild scans authoritative `index` object state and regenerates namespace-serving segments.
- Normal prefix maintenance is driven asynchronously from `index` WAL changes with coalesced per-fragment projection; rebuild is the full repair path when the serving plane needs a full regeneration.
- Prefix state is rebuildable acceleration, not object truth.

Start query params:
- `fragment=<fragment_hash>` optional

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/prefix/rebuild/start"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/prefix/rebuild/start?fragment=<fragment_hash>"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/prefix/rebuild/status"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/prefix/rebuild/stop"
```

### 9.8 System health endpoint
`GET /v4/maintenance/health`

Query params:
- `deep_scan=true` force immediate deep consistency scan
- `refresh=true` bypass cache and force fresh snapshot

Notes:
- Default request serves cached health snapshot produced by monitor loop.
- Background monitor interval is config-driven.
- Fast probes are read-oriented and avoid temp-file write/delete churn on every monitor pass.
- Deep scan reflects the partitioned `index` model; differing per-root object counts are expected outside a partition's owner/replica placements.
- Health includes system metrics, process RSS/open-FD pressure, active upload/runtime counters, and raw/blob storage totals.

```bash
curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/health"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/health?refresh=true"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/health?deep_scan=true"
```

## 10. WebSocket APIs

### 10.1 `GET /ws?token=<jwt>`
- Token is passed via query parameter `token`.
- On connect, server sends `HELLO`.
- Client heartbeat: send `{"op":"PING"}` and receive `{"op":"PONG"}`.
- Server pushes event envelopes with `{"op":"EVENT","d":...}`.

Example (`wscat`):
```bash
wscat -c "wss://127.0.0.1:50001/ws?token=$TOKEN"
```

### 10.2 `POST /v4/system/broadcast`
Broadcasts system message event.
Body (`BroadcastReq`):
```json
{
  "message": "Maintenance window starts in 10 minutes",
  "severity": "warning"
}
```

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/system/broadcast" \
  -d '{"message":"Maintenance starts","severity":"info"}'
```

## 11. Legacy FS API (`/dir/*`)
These routes are protected by legacy FS auth middleware.

### 11.1 Directory listing and inspection

#### `GET /dir/` and `GET /dir/{*path}`
Lists files recursively under scope.
```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" "https://127.0.0.1:50001/dir/"
curl -k -H "Authorization: Bearer $FS_TOKEN" "https://127.0.0.1:50001/dir/media"
```

#### `GET /dir/capacity`
Query:
- `uid` required

```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/capacity?uid=<uid_path>"
```

#### `GET /dir/file/metadata/` and `GET /dir/file/metadata/{*path}`
Query:
- `file_name` required

```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/metadata/?file_name=movie.mp4"

curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/metadata/media?file_name=movie.mp4"
```

### 11.2 File view/stream/download

#### `GET /dir/file/view/{*path}`
Reads full file response.
```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/view/media/movie.mp4" -o movie.mp4
```

#### `GET /dir/file/stream/{*path}`
Supports optional range.
```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  -H "Range: bytes=0-1048575" \
  "https://127.0.0.1:50001/dir/file/stream/media/movie.mp4" -o part.bin
```

#### `GET /dir/file/stream/v2/{*path}`
Range-optimized partial content behavior.
```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  -H "Range: bytes=0-2097151" \
  "https://127.0.0.1:50001/dir/file/stream/v2/media/movie.mp4" -o part2.bin
```

#### `GET /dir/file/transcode/stream/hls/{*path}`
Only `.m3u8` and `.ts` allowed.
```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/transcode/stream/hls/hls/index.m3u8"
```

#### `GET /dir/file/download/` and `GET /dir/file/download/{*path}`
Query:
- `file_name` required
- `offset` required
- `chunk_size` required

Returns a file chunk per request.
```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/download/media?file_name=movie.mp4&offset=0&chunk_size=1048576" \
  -o movie.part0
```

### 11.3 Directory and file mutations

#### `POST /dir/create/{*path}`
```bash
curl -k -X POST -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/create/media/new-folder"
```

#### `DELETE /dir/delete/{*path}`
```bash
curl -k -X DELETE -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/delete/media/old-folder"
```

#### `PUT /dir/file/rename/` and `PUT /dir/file/rename/{*path}`
Query params:
- `file_to_modify`
- `new_file_name`

```bash
curl -k -X PUT -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/rename/media?file_to_modify=old.mp4&new_file_name=new"
```

#### `DELETE /dir/file/delete/` and `DELETE /dir/file/delete/{*path}`
Query params:
- `file_to_delete`

```bash
curl -k -X DELETE -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/delete/media?file_to_delete=movie.mp4"
```

### 11.4 Legacy upload APIs

#### Direct multipart upload
- `POST /dir/file/upload/`
- `POST /dir/file/upload/{*path}`

```bash
curl -k -X POST -H "Authorization: Bearer $FS_TOKEN" \
  -F "file=@movie.mp4" \
  "https://127.0.0.1:50001/dir/file/upload/media"
```

#### Fragmented upload v2
- `POST /dir/file/upload/v2/`
- `POST /dir/file/upload/v2/{*path}`

Query params (`UploadParams`):
- `file_id`
- `total_chunks`
- `chunk_index`
- `original_filename`
- `mime_type` optional
- `checksum` optional

```bash
curl -k -X POST -H "Authorization: Bearer $FS_TOKEN" \
  -F "file=@chunk_0.bin" \
  "https://127.0.0.1:50001/dir/file/upload/v2/media?file_id=myupload1&total_chunks=10&chunk_index=0&original_filename=movie.mp4&checksum=<hex>"
```

#### Check uploaded chunks (v2)
`GET /dir/file/upload/check/{*path}`
Query:
- `file_id`

```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/upload/check/media?file_id=myupload1"
```

### 11.5 Stateless upload v3

#### Initiate
- `POST /dir/file/upload/v3/initiate/`
- `POST /dir/file/upload/v3/initiate/{*path}`

Body:
```json
{
  "original_filename": "movie.mp4",
  "total_size_bytes": 1073741824,
  "total_chunks": 256,
  "mime_type": "video/mp4"
}
```

```bash
curl -k -X POST -H "Authorization: Bearer $FS_TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/dir/file/upload/v3/initiate/media" \
  -d '{"original_filename":"movie.mp4","total_size_bytes":1073741824,"total_chunks":256,"mime_type":"video/mp4"}'
```

#### Upload chunk (binary)
`PUT /dir/file/upload/v3/{file_id}/{chunk_index}`
Headers:
- `Digest: sha-256=<base64_sha256>` required
- `Buffer: memory-size=<bytes>` optional

```bash
curl -k -X PUT -H "Authorization: Bearer $FS_TOKEN" \
  -H "Digest: sha-256=<base64_digest>" \
  -H "Buffer: memory-size=262144" \
  --data-binary @chunk_0.bin \
  "https://127.0.0.1:50001/dir/file/upload/v3/<file_id>/0"
```

#### Upload chunk (multipart)
`PUT /dir/file/upload/multipart/v3/{file_id}/{chunk_index}`
Query:
- `checksum=<hex_sha256>` optional

```bash
curl -k -X PUT -H "Authorization: Bearer $FS_TOKEN" \
  -F "file=@chunk_0.bin" \
  "https://127.0.0.1:50001/dir/file/upload/multipart/v3/<file_id>/0?checksum=<hex>"
```

#### Status and finalize
- `GET /dir/file/upload/v3/{file_id}/status`
- `POST /dir/file/upload/v3/{file_id}/finalize`

```bash
curl -k -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/upload/v3/<file_id>/status"

curl -k -X POST -H "Authorization: Bearer $FS_TOKEN" \
  "https://127.0.0.1:50001/dir/file/upload/v3/<file_id>/finalize"
```

## 12. Full Endpoint Index

### Auth
- `POST /authenticate/fragment`
- `POST /authenticate/fs`

## 13. Streaming And Upload Tuning (Config)
`[upload_config]` keys used by streaming read and upload-part paths:
- `upload_memory_budget_bytes`: caps upload-part in-flight working set by reducing effective `max_concurrent_io` at startup (`auto` derives from RAM, override for strict memory ceilings).
- `stream_chunk_size`: maximum emitted stream chunk per read step (4 KiB..4 MiB after clamp/alignment).
- `stream_readahead_bytes`: sequential read-ahead window per stream step (64 KiB..8 MiB after clamp/alignment).
- `stream_locality_bias_enabled`: prefer shard directory inferred from shard absolute path for sequential streaming.
- `stream_max_concurrency`: global stream semaphore capacity.
- `upload_part_strict_durability_default`: default durability for `PUT /v4/upload/chunk` when query override is absent (`true` = strict fsync/dir-sync, `false` = batched, default `false`).
- `upload_io_gate_enabled`: default for upload I/O gate in `PUT /v4/upload/chunk` when `io_gate` query override is absent.

`auto` preset behavior:
- `auto` values are hardware-aware (CPU cores + detected RAM + disk count + EC shard layout).
- Upload and stream auto concurrency are memory-capped so defaults stay safe on low-end nodes while scaling on high-end nodes.
- Auto disk-count tuning is physical-device aware: if multiple configured blob roots resolve to the same underlying disk, `auto` treats them as one effective disk and clamps upload/stream concurrency and upload buffer/budget more conservatively.

Recommended starting points:
- Low hardware: `stream_chunk_size=131072`, `stream_readahead_bytes=262144`
- High hardware: `stream_chunk_size=1048576`, `stream_readahead_bytes=2097152`

### Public/global
- `GET /ping`
- `GET /v4/fragment/list/public`
- `GET /v4/fragment/check`
- `GET /v4/fragment/get/hash`
- `POST /v4/fragment/create`
- `GET /v4/public/share`

### Protected v4 read
- `GET /v4/capacity/usage/{fragment}`
- `GET /v4/fragment/list`
- `GET /v4/fragment/objects/{fragment}`
- `GET /v4/fragment/trash/{fragment}`
- `GET /v4/fragment/versions/{fragment}/{*object_key}`
- `GET /v4/fragment/get/{fragment}/{*object_key}`
- `GET /v4/fragment/stream/{fragment}/{*object_key}`
- `GET /v4/fragment/meta/{fragment}/{*object_key}`
- `GET /v4/fragment/download/{fragment}`
- `GET /v4/fragment/download/chunk/{fragment}`
- `GET /v4/smb/progress/{fragment}`
- `POST /v4/fragment/archive/{fragment}`
- `POST /v4/share/create`

### Protected v4 write/upload
- `DELETE /v4/fragment/delete/{fragment}`
- `PATCH /v4/fragment/modify/{fragment}`
- `DELETE /v4/fragment/trash/empty/{fragment}`
- `PATCH /v4/fragment/object/rename`
- `DELETE /v4/fragment/object/delete/{fragment}/{*object_key}`
- `DELETE /v4/fragment/version/delete/{fragment}/{version_id}/{*object_key}`
- `DELETE /v4/fragment/permanent/delete/{fragment}/{*object_key}`
- `POST /v4/fragment/recover/version/{fragment}/{version_id}/{*object_key}`
- `POST /v4/fragment/recover/object/{fragment}/{*object_key}`
- `POST /v4/fragment/recover/trash/all/{fragment}`
- `POST /v4/upload/init/{fragment}/{*object_key}`
- `GET /v4/upload/status/{fragment}/{version_id}`
- `GET /v4/upload/status/list/{fragment}`
- `PUT /v4/upload/chunk/{fragment}/{version_id}/{part_index}`
- `POST /v4/upload/finalize/{fragment}/{version_id}/{session_id}`
- `DELETE /v4/upload/cancel/{fragment}/{version_id}`

### Protected v4 maintenance
- `POST /v4/maintenance/sync/global`
- `POST /v4/maintenance/sync/fragment`
- `POST /v4/maintenance/cleanup/fragment`
- `POST /v4/maintenance/init/fragment`
- `POST /v4/maintenance/repair/start`
- `POST /v4/maintenance/repair/stop`
- `GET /v4/maintenance/repair/status`
- `POST /v4/maintenance/gc/start`
- `GET /v4/maintenance/gc/status`
- `POST /v4/maintenance/redistribution/start`
- `PATCH /v4/maintenance/redistribution/stop`
- `GET /v4/maintenance/redistribution/monitor`
- `POST /v4/maintenance/prefix/rebuild/start`
- `POST /v4/maintenance/prefix/rebuild/stop`
- `GET /v4/maintenance/prefix/rebuild/status`
- `GET /v4/maintenance/health`

### Websocket/broadcast
- `GET /ws`
- `POST /v4/system/broadcast`

### Legacy FS
- `POST /dir/file/upload/`
- `POST /dir/file/upload/{*path}`
- `POST /dir/file/upload/v2/`
- `POST /dir/file/upload/v2/{*path}`
- `POST /dir/file/upload/v3/initiate/`
- `POST /dir/file/upload/v3/initiate/{*path}`
- `PUT /dir/file/upload/multipart/v3/{file_id}/{chunk_index}`
- `PUT /dir/file/upload/v3/{file_id}/{chunk_index}`
- `GET /dir/file/upload/v3/{file_id}/status`
- `POST /dir/file/upload/v3/{file_id}/finalize`
- `POST /dir/create/{*path}`
- `DELETE /dir/delete/{*path}`
- `PUT /dir/file/rename/`
- `PUT /dir/file/rename/{*path}`
- `DELETE /dir/file/delete/`
- `DELETE /dir/file/delete/{*path}`
- `GET /dir/`
- `GET /dir/{*path}`
- `GET /dir/capacity`
- `GET /dir/file/upload/check/{*path}`
- `GET /dir/file/view/{*path}`
- `GET /dir/file/stream/{*path}`
- `GET /dir/file/stream/v2/{*path}`
- `GET /dir/file/transcode/stream/hls/{*path}`
- `GET /dir/file/metadata/`
- `GET /dir/file/metadata/{*path}`
- `GET /dir/file/download/`
- `GET /dir/file/download/{*path}`

## 14. SMB Network Drive (`smb`)

`smb` is N1's native SMB-lite gateway integrated in the server process. It is not part of `/v4/*` HTTP routing.

### 14.1 Config keys (`[smb]`)
- `enabled=true|false`
- `bind_addr=<ip_or_host>`
- `smb_port=<u16>`
- `max_sessions=<64..65536>`
- `max_frame_bytes=<4096..67108864>`
- `async_close_commit=true|false` (default `false`; retained for compatibility, ignored in read-only SMB mode)
- `async_close_threshold_bytes=<u64>` (default `67108864`; retained for compatibility, ignored in read-only SMB mode)
- `auth_username=<string>` optional
- `auth_password=<string>` optional
- `auth_domain=<string>` optional
- `require_signing=true|false`
- `query_directory_page_limit=<16..1000>` (default `256`)
- `query_directory_max_buffered_entries=<32..8192>` (default `512`)
- `query_directory_max_seeded_entries=<8..2048>` (default `128`, capped by `query_directory_max_buffered_entries`)

Behavior:
- Preferred auth mode is fragment-scoped:
- SMB `username` is fragment identifier (`<fragment_name>` or `<fragment_hash>`).
- SMB `password` is fragment key.
- Share `N1` maps to the authenticated fragment for the session.
- SMB filesystem capacity reported to clients (for example Windows "Properties") is derived from that fragment's quota (`allocated_storage` / `storage_used`), not a global fixed size.
- SMB is read-only: browse/list/open/read/query operations are supported, while write/create/rename/delete/truncate and other mutating requests are denied with `STATUS_ACCESS_DENIED`.
- Optional static mode: if `auth_username` and `auth_password` are configured, that static account is also accepted.
- If `require_signing=true`, SMB signing is required.
- Quota mutations are applied to the in-memory quota cache first and flushed asynchronously to mirrored `quota.msgpack` files.
- If one quota mirror lags during disk saturation, the background flusher keeps retrying until mirrors converge; warnings are reserved for full flush failure, not transient partial lag.

### 14.2 SMB Transfer Progress API
`GET /v4/smb/progress/{fragment}`

Query:
- `include_completed` optional (`true|false`, default `false`)
- `limit` optional (`1..1000`, default `100`)

Auth:
- Protected v4 route; requires valid fragment JWT (`read=true`) and matching fragment scope.

Response:
- Array of `SmbTransferProgressPayload`.
- In the current read-only SMB mode this endpoint normally returns no active rows, because SMB uploads are disabled.

### 14.3 Environment overrides
- `N1_SMB_ENABLED`
- `N1_SMB_BIND_ADDR`
- `N1_SMB_PORT`
- `N1_SMB_MAX_SESSIONS`
- `N1_SMB_MAX_FRAME_BYTES`
- `N1_SMB_ASYNC_CLOSE_COMMIT`
- `N1_SMB_ASYNC_CLOSE_THRESHOLD_BYTES`
- `N1_SMB_AUTH_USERNAME`
- `N1_SMB_AUTH_PASSWORD`
- `N1_SMB_AUTH_DOMAIN`
- `N1_SMB_REQUIRE_SIGNING`
- `N1_SMB_QUERY_DIRECTORY_PAGE_LIMIT`
- `N1_SMB_QUERY_DIRECTORY_MAX_BUFFERED_ENTRIES`
- `N1_SMB_QUERY_DIRECTORY_MAX_SEEDED_ENTRIES`

### 14.4 WebSocket Event
- `SMB_TRANSFER_PROGRESS` with payload shape `SmbTransferProgressPayload`.

### 14.5 SMB-lite scope
Implemented:
- auth + signing
- `list/read/open`
- query-directory/query-info behavior
- non-mutating `SET_INFO` classes needed for client compatibility
- conservative share-mode conflict checks
- conservative lock ownership handling

Not implemented:
- write/create/rename/delete/mkdir/rmdir/truncate/delete-on-close mutation flows
- oplocks / leases
- durable or persistent handles
- multichannel and other enterprise SMB semantics

Deployment guide:
- `NETWORK_DRIVE.md`

### 14.4 Mount examples
Linux (`cifs`):
```bash
sudo mount -t cifs //127.0.0.1/N1 /mnt/n1 \
  -o username=<fragment_name>,password='<fragment_key>',domain=N1,sec=ntlmssp,vers=2.1,ro,noperm
```

GNOME Files:
- `smb://127.0.0.1/N1`
- Username = fragment name/hash
- Password = fragment key
