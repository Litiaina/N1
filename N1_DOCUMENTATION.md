# N1 API Documentation

`N1` is the standalone storage kernel in this repository. In `2.1.1`, it exposes:

- fragment provisioning
- fragment-scoped authentication
- object upload, read, list, version, trash, recover, and archive flows
- a standalone mutable namespace layer
- lifecycle, garbage collection, repair, redistribution, and health endpoints
- SMB Lite and POSIX-style namespace access on top of the same storage truth

This document is written as an operator and integrator reference. It is meant to answer:

- which endpoint to call
- what headers, query parameters, and JSON body it expects
- what shape comes back
- how to combine the endpoints into a real upload, read, or lifecycle flow

## Base URL

Typical standalone `N1` base URL:

```text
https://127.0.0.1:50001
```

## Authentication Model

Most `N1` APIs are fragment-scoped.

1. Create a fragment with `POST /n1/fragments`
2. Mint a fragment token with `POST /n1/auth/fragment`
3. Use `Authorization: Bearer <access_token>` on protected routes

Stable `N1 2.0` API routes and operator routes live under `/n1/...`.

Operational note:

- `namespace_v2` is replicated across configured index roots
- if the owner namespace disk is lost but another namespace replica survives, `N1` reseeds the owner catalog from the surviving replica before serving logical namespace reads

### Permission Bits

Fragment tokens carry four booleans:

- `read`
- `write`
- `modify`
- `delete`

The route handlers enforce them directly. For example:

- reads require `read`
- upload init / write paths require `write`
- rename / move requires `modify`
- trash / empty trash / destructive maintenance paths require `delete`

### Fragment Auth Request

`POST /n1/auth/fragment`

```json
{
  "fragment": "b77adaff371716fc0de411a88c924c21c6e4e68ab9addfa536fe07ec774aa1fd",
  "secret": "fragment-or-master-key",
  "read": true,
  "write": true,
  "modify": true,
  "delete": true,
  "expires_in": 3600
}
```

Response:

```json
{
  "access_token": "<jwt>",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "<opaque-refresh-token>",
  "refresh_expires_in": 604800
}
```

### Refresh And Logout

`POST /n1/auth/refresh`

```json
{
  "refresh_token": "<opaque-refresh-token>"
}
```

`POST /n1/auth/logout`

```json
{
  "refresh_token": "<opaque-refresh-token>"
}
```

## Operator CLI

[`n1_cli`](/home/administrator/Development/N1/n1_cli) is the operator-facing companion to the stable `N1 2.0` API.

Current CLI scope:

- fragment create
- health snapshots and watch mode
- active multipart upload list/status
- GC start/status/stop/watch
- repair start/status/stop/watch
- redistribution start/status/stop/watch

Representative commands:

```bash
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  fragment create --name <fragment_name> --key <fragment_key>

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  health --deep-scan

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  gc status
```

Protected CLI commands authenticate through `POST /n1/auth/fragment`, so pass `--fragment <name-or-hash>` or `--auth-fragment <name-or-hash>` when operating a real server.

Complete examples:

```bash
# Health
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  health

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  health --watch --deep-scan

# Multipart uploads
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  uploads list --limit 100 --offset 0

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  uploads status --version-id <version_id>

# GC
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  gc status

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  gc start

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  gc watch

# Repair
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  repair start --dry-run --mode availability

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  repair status

# Redistribution
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  redistribute start --dry-run --mode availability --durability auto --scope all

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  redistribute status

# JSON output
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  --json repair status

# Different auth fragment
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <target_fragment> \
  --auth-fragment <auth_fragment> \
  health
```

## Common Response Shapes

### Object Metadata

Many object routes return the same core shape:

```json
{
  "object_key": "videos/demo.mp4",
  "fragment": "b77adaff371716fc0de411a88c924c21c6e4e68ab9addfa536fe07ec774aa1fd",
  "version_id": "01KNYMZC0M0J1E4A74M0J2P8F8",
  "upload_session_id": "",
  "owner_uid": "b77adaff371716fc0de411a88c924c21c6e4e68ab9addfa536fe07ec774aa1fd",
  "content_type": "video/mp4",
  "total_size_bytes": 10485760,
  "created_at": 1775900000,
  "parts": [],
  "is_delete_marker": false,
  "stripe_size_bytes": 1048576
}
```

Notes:

- In standalone `N1 2.0`, `object_key` is the logical path the caller uses.
- The immutable physical storage identity is managed under the namespace layer and does not need to be supplied by normal clients.

### Object Listing

`GET /n1/objects` and `GET /n1/trash` return:

```json
{
  "objects": [
    {
      "object_key": "videos/demo.mp4",
      "fragment": "fragment-hash",
      "version_id": "01KNYMZC0M0J1E4A74M0J2P8F8",
      "upload_session_id": "",
      "owner_uid": "fragment-hash",
      "content_type": "video/mp4",
      "total_size_bytes": 10485760,
      "created_at": 1775900000,
      "parts": [],
      "is_delete_marker": false,
      "stripe_size_bytes": 1048576
    }
  ],
  "common_prefixes": [
    "videos/"
  ],
  "continuation_token": "videos/demo.mp4"
}
```

### POSIX List

`GET /n1/posix/list`

```json
{
  "path": "videos",
  "scan_trash": false,
  "entries": [
    {
      "name": "demo.mp4",
      "path": "videos/demo.mp4",
      "kind": "file",
      "modified_at_unix": 1775900000
    },
    {
      "name": "clips",
      "path": "videos/clips",
      "kind": "directory",
      "modified_at_unix": 1775900100
    }
  ],
  "continuation_token": null
}
```

### POSIX Mutation

`POST /n1/posix/mkdir`, `rename`, `trash`, and `recover` all return:

```json
{
  "operation": "rename",
  "kind": "file",
  "affected": 1,
  "source_path": "videos/demo.mp4",
  "destination_path": "videos/demo-final.mp4"
}
```

### Upload Status

`GET /n1/upload/multipart/{version_id}`

```json
{
  "uploaded_parts": [0, 1, 2],
  "committed_parts": 3,
  "committed_bytes": 6291456,
  "expected_parts": 5,
  "total_size_bytes": 10485760,
  "created_at": 1775900200,
  "session_id": "01KNYN0Q4FC5B7NAT0S6Z2TQ4Q",
  "version_id": "01KNYN0Q4E4YQ5RDNM5AC7BQ7S",
  "object_key": "videos/demo.mp4",
  "physical_object_key": "a309986ab5..."
}
```

`object_key` is the logical path for the caller. `physical_object_key` is included only when the underlying storage object id differs and is mainly useful for debugging.

## Public And Helper Endpoints

### `GET /n1/ping`

Simple liveness check.

Response:

```text
pong
```

### `GET /n1/fragments/check`

Legacy helper route. It currently expects a JSON body even though the method is `GET`.

Request body:

```json
{
  "fragment": "fragment-hash"
}
```

Response on success:

```json
{
  "fragment": "fragment-hash"
}
```

### `GET /n1/fragments/hash`

Legacy helper route. It currently expects a JSON body even though the method is `GET`.

Request body:

```json
{
  "fragment": "mainframe"
}
```

Response:

```json
{
  "fragment": "mainframe",
  "fragment_hash": "b77adaff371716fc0de411a88c924c21c6e4e68ab9addfa536fe07ec774aa1fd"
}
```

### `GET /n1/objects/signed-read`

Public signed-read route. No bearer token is required. The URL is normally minted by `POST /n1/objects/signed-url`.

Query parameters:

- `fragment`
- `object_key`
- `expires`
- `signature`
- `disposition` optional: `view`, `stream`, or `download`
- `version_id` optional
- `filename` optional

## Fragment Provisioning API

### `POST /n1/fragments`

Create a fragment.

Request body:

```json
{
  "name": "mainframe",
  "secret": "<MASTER_KEY>",
  "key": "fragment-local-secret"
}
```

Fields:

- `name`: human input used to derive the deterministic fragment hash
- `secret`: must match `MASTER_KEY`
- `key`: fragment secret used later for fragment auth

Response:

```json
{
  "fragment_hash": "b77adaff371716fc0de411a88c924c21c6e4e68ab9addfa536fe07ec774aa1fd",
  "name": "mainframe"
}
```

### `GET /n1/fragments`

List fragment hashes visible from configured index roots and include storage totals.

Response:

```json
[
  {
    "fragment": "b77adaff371716fc0de411a88c924c21c6e4e68ab9addfa536fe07ec774aa1fd",
    "total": 4000787030016,
    "used": 120449024
  }
]
```

### `PATCH /n1/fragments/{fragment}`

Update fragment metadata or rotate its fragment key.

Request body:

```json
{
  "new_name": "mainframe-renamed",
  "new_key": "new-fragment-secret",
  "secret": "<MASTER_KEY>"
}
```

Notes:

- `new_name` requires the master secret because it changes the fragment hash.
- `new_key` rotates the fragment secret used by `POST /n1/auth/fragment`.

### `DELETE /n1/fragments/{fragment}`

Delete the fragment control plane and storage directories for that fragment.

Protected by fragment token permissions and admin/master checks inside the handler.

Success response:

```json
{
  "status": "success",
  "message": "Fragment deleted successfully"
}
```

### `GET /n1/capacity/usage`

Return aggregate storage capacity from configured storage roots.

Response:

```json
{
  "capacity_used": 120449024,
  "capacity_allocated": 4000787030016,
  "capacity_available": 3880338004992,
  "filesystem_count": 4
}
```

## Object Listing And Read API

### `GET /n1/objects`

List live objects from the standalone namespace layer.

Query parameters:

- `limit` optional, default `100`, max `1000`
- `prefix` optional
- `delimiter` optional, usually `/`
- `continuation_token` optional

### `GET /n1/trash`

Same query contract as `GET /n1/objects`, but lists trashed objects.

### `GET /n1/objects/versions/{*object_key}`

List versions for a logical object path.

Path parameters:

- `object_key`: logical path such as `videos/demo.mp4`

Response:

- JSON array of `ObjectMetadata`

### `GET /n1/objects/get/{*object_key}`

Read the full object body.

Supports logical paths in standalone `N1 2.0`.

### `GET /n1/objects/stream/{*object_key}`

Stream the object body. This is the better route for large objects or browser playback.

### `GET /n1/objects/meta/{*object_key}`

Return object metadata for the current live head.

### `GET /n1/objects/download`

Download by query instead of path.

Query parameters:

- `object_key` required
- `version_id` optional
- `filename` optional

If `filename` is omitted and the logical path differs from the underlying storage object id, `N1 2.0` will derive a browser-friendly download filename from the logical path.

### `GET /n1/objects/download/chunk`

Read a specific byte window.

Query parameters:

- `object_key` required
- `offset` required
- `chunk_size` required

### `POST /n1/objects/signed-url`

Mint a time-limited public signed read URL.

Request body:

```json
{
  "object_key": "videos/demo.mp4",
  "disposition": "download",
  "expires_in": 3600,
  "version_id": "01KNYMZC0M0J1E4A74M0J2P8F8",
  "filename": "demo.mp4"
}
```

Response:

```json
{
  "url": "https://host/n1/objects/signed-read?...",
  "path": "/n1/objects/signed-read",
  "fragment": "fragment-hash",
  "object_key": "videos/demo.mp4",
  "disposition": "download",
  "expires_at": 1775903600,
  "version_id": "01KNYMZC0M0J1E4A74M0J2P8F8",
  "filename": "demo.mp4"
}
```

### `POST /n1/objects/archive`

Download multiple objects as an archive.

Request body:

```json
{
  "keys": ["videos/demo.mp4", "docs/readme.txt"],
  "prefixes": ["videos/2025/"],
  "format": "tar",
  "filename": "bundle.tar",
  "export_all": false,
  "include_internal": false
}
```

Fields:

- `keys`: logical paths to include directly
- `prefix`: optional single logical folder prefix
- `prefixes`: optional logical folder prefixes for folder/archive export
- `format`: `tar` for folder, mixed, manifest, and fragment-scale export. `zip` is only valid for explicit selected file keys without folder prefixes or `export_all=true`
- `filename`: optional download filename
- `export_all`: if `true`, archive all visible objects
- `include_internal`: include internal metadata entries when supported

Rules:

- selected folder archive requests are resolved through the logical namespace first, then mapped to physical object ids for streaming
- `prefix` or `prefixes` can be combined with `keys`
- `tar` is the safe/default format for large exports and folder exports

### `GET /n1/objects/archive/manifest`

Stream a manifest-driven archive build.

Query parameters:

- `format` optional, default `tar`
- `filename` optional

The request body is a manifest payload consumed by the archive builder.

## Upload API

`N1 2.0` supports both one-shot upload and resumable multipart upload.

### Common Upload Parameters

Multipart init and finalize use:

```json
{
  "content_type": "video/mp4",
  "total_size_bytes": 10485760,
  "expected_parts": 5,
  "part_size_bytes": 2097152
}
```

Fields:

- `content_type` optional
- `total_size_bytes` required
- `expected_parts` required
- `part_size_bytes` optional but strongly recommended for consistent resume behavior

### `POST /n1/upload/one-shot/{*object_key}`

Upload a complete object in one request.

Path parameters:

- `object_key`: logical path, for example `videos/demo.mp4`

Query parameters:

- `checksum` optional
- `durability` optional
- `strict_durability` optional boolean
- `io_gate` optional boolean

Body:

- raw object bytes

Response:

- `201 Created`
- `ObjectMetadata`

### `POST /n1/upload/multipart/init/{*object_key}`

Start or reuse a multipart upload session for a logical path.

Body:

- `InitiateUploadParams`

Response:

```json
{
  "session_id": "01KNYN0Q4FC5B7NAT0S6Z2TQ4Q",
  "version_id": "01KNYN0Q4E4YQ5RDNM5AC7BQ7S"
}
```

If an interrupted multipart session already exists for the same logical path and compatible upload manifest, `N1 2.0` reuses it instead of creating a new one.

### `GET /n1/upload/multipart`

List active multipart sessions.

Query parameters:

- `limit` optional, default `50`, max `100`
- `offset` optional, default `0`

Response:

```json
{
  "sessions": [
    {
      "session_id": "01KNYN0Q4FC5B7NAT0S6Z2TQ4Q",
      "version_id": "01KNYN0Q4E4YQ5RDNM5AC7BQ7S",
      "object_key": "videos/demo.mp4",
      "physical_object_key": "a309986ab5...",
      "created_at": 1775900200,
      "reserved_size": 10485760,
      "total_size_bytes": 10485760,
      "expected_parts": 5,
      "committed_parts": 3,
      "committed_bytes": 6291456
    }
  ],
  "total": 1
}
```

### `GET /n1/upload/multipart/{version_id}`

Return upload progress for one multipart session.

### `PUT /n1/upload/multipart/{version_id}/{part_index}`

Upload one part.

Path parameters:

- `version_id`
- `part_index`

Query parameters:

- `checksum` optional
- `durability` optional
- `strict_durability` optional
- `io_gate` optional

Body:

- raw bytes for this part

### `POST /n1/upload/multipart/{version_id}/{session_id}/finalize`

Commit the multipart upload.

Query parameters:

- `object_key` required
- `version_id` optional
- `filename` optional but unused by finalize

Body:

- same `InitiateUploadParams` shape used for init

### `DELETE /n1/upload/multipart/{version_id}`

Cancel the multipart session.

Query parameters:

- `object_key` required

## Namespace And POSIX-Style API

These routes operate on the standalone mutable namespace layer in `N1 2.0`.

### `GET /n1/posix/list`

Query parameters:

- `path` optional, default root
- `limit` optional, default `200`, max `1000`
- `continuation_token` optional
- `trash` optional boolean, default `false`

Behavior:

- `trash=false`: live namespace browsing
- `trash=true`: trash browsing
- when `path` is omitted or empty and `trash=true`, the response is an item-based trash inventory:
  deleted files are listed by their real logical file path, and deleted directories are listed as directories

### `GET /n1/posix/stat`

Query parameters:

- `path` optional, default root
- `trash` optional boolean

Response:

```json
{
  "path": "videos/demo.mp4",
  "scan_trash": false,
  "kind": "file",
  "is_virtual": false,
  "size_bytes": 10485760,
  "modified_at_unix": 1775900000,
  "content_type": "video/mp4",
  "version_id": "01KNYMZC0M0J1E4A74M0J2P8F8"
}
```

### `POST /n1/posix/mkdir`

Request body:

```json
{
  "path": "videos/clips"
}
```

### `POST /n1/posix/rename`

Request body:

```json
{
  "source_path": "videos/demo.mp4",
  "destination_path": "videos/demo-final.mp4"
}
```

### `POST /n1/posix/move`

Alias of `POST /n1/posix/rename`. Same request and response.

### `POST /n1/posix/trash`

Request body:

```json
{
  "path": "videos/demo.mp4"
}
```

### `POST /n1/posix/recover`

Request body:

```json
{
  "path": "videos/demo.mp4"
}
```

Important `2.1.1` behavior:

- root trash listing returns recoverable trashed items directly
- recovering a listed file uses that exact file path
- recovering a listed directory restores the deleted directory subtree

### `POST /n1/posix/purge`

Remove a single trashed file or trashed directory subtree from the trash lifecycle.

Request body:

```json
{
  "path": "videos/demo.mp4"
}
```

Behavior:

- the item stops being recoverable immediately
- underlying immutable storage is still reclaimed later by garbage collection
- deleting a trashed directory path removes the entire deleted subtree represented by that directory

## Lifecycle API

### `POST /n1/objects/rename`

Request body:

```json
{
  "source_fragment": "fragment-hash",
  "source_key": "videos/demo.mp4",
  "destination_fragment": "fragment-hash",
  "destination_key": "videos/demo-final.mp4"
}
```

For standalone `N1 2.0`, source and destination fragment should normally be the same fragment.

### `DELETE /n1/objects/{*object_key}`

Trash a logical object path.

Response:

```json
{
  "operation": "trash",
  "affected_paths": 1,
  "affected_objects": 1
}
```

### `DELETE /n1/objects/version/{version_id}/{*object_key}`

Delete a specific version from the physical version chain.

### `POST /n1/objects/recover/{*object_key}`

Recover a logical object path from trash.

### `POST /n1/objects/recover/version/{version_id}/{*object_key}`

Recover a specific version.

### `POST /n1/objects/bulk-delete`

Request body:

```json
{
  "object_keys": [
    "videos/demo-1.mp4",
    "videos/demo-2.mp4"
  ]
}
```

Response:

```json
{
  "requested": 2,
  "unique": 2,
  "succeeded": 2,
  "not_found": 0,
  "failed": []
}
```

### `POST /n1/objects/bulk-recover`

Same request shape as bulk delete.

### `POST /n1/trash/recover-all`

Recover every currently trashed object in the fragment.

No request body.

### `DELETE /n1/trash/empty`

Retire all trashed namespace entries immediately. Physical bytes are reclaimed later by GC.

No request body.

### Permanent Delete Note

`N1 2.0` is lifecycle-first.

- normal delete means move to trash
- `empty trash` retires namespace visibility
- GC later reclaims unreachable object versions and blobs

Routes that try to expose immediate permanent delete behavior are no longer the preferred `2.0` contract.

## Maintenance API

### `POST /n1/maintenance/fragment/init`

Ensure blob directory layout exists for the fragment.

Response:

- `202 Accepted`

### `POST /n1/maintenance/fragment/cleanup`

Clean fragment orphan state and return usage.

Response:

```json
{
  "fragment": "fragment-hash",
  "usage_bytes": 10485760,
  "orphan_blob_dirs_removed": 0,
  "message": "Fragment metadata cleanup complete."
}
```

### Repair

Routes:

- `POST /n1/maintenance/repair/start`
- `POST /n1/maintenance/repair/stop`
- `GET /n1/maintenance/repair/status`

`repair/start` query parameters:

- `mode` or `check_mode` optional: `availability`, `checksum`, `strict`, `scrub`
- `access_mode` optional
- `fast_mode` optional boolean
- `read_only_fast` optional boolean
- `dry_run` optional boolean
- `fragment` optional fragment filter
- `meta_concurrency` optional
- `repair_concurrency` optional
- `batch` optional

Status shape:

```json
{
  "is_running": true,
  "phase": "Scanning",
  "fast_mode": false,
  "total_scanned": 1024,
  "total_repaired": 3,
  "corrupt_found": 3,
  "missing_meta_repaired": 0,
  "divergent_meta_repaired": 1,
  "last_disagreement": null,
  "current_fragment": "fragment-hash",
  "last_repaired": "videos/demo.mp4",
  "last_error": null,
  "start_timestamp": 1775900400,
  "duration_seconds": 12,
  "scan_rate_per_sec": 85.3
}
```

### Garbage Collection

Routes:

- `POST /n1/maintenance/gc/start`
- `POST /n1/maintenance/gc/stop`
- `GET /n1/maintenance/gc/status`

The GC status route returns:

```json
{
  "is_running": false,
  "phase": "Completed",
  "is_manual_run": true,
  "total_fragments": 1,
  "total_fragments_scanned": 1,
  "total_live_versions_seen": 0,
  "total_trash_versions_expired": 0,
  "total_sessions_deleted": 0,
  "total_orphan_blob_dirs_deleted": 4,
  "total_refund_bytes": 21290008,
  "total_errors": 0,
  "current_fragment": "",
  "last_error": null,
  "start_timestamp": 1775900500,
  "duration_seconds": 2
}
```

### Redistribution

Routes:

- `POST /n1/maintenance/redistribution/start`
- `POST /n1/maintenance/redistribution/stop`
- `GET /n1/maintenance/redistribution/status`

`redistribution/start` query parameters:

- `dry_run` optional boolean
- `fast_mode` optional boolean
- `read_only_fast` optional boolean
- `mode` or execution mode optional: `availability`, `strict`, `scrub`
- `scope` optional: `all`, `blob`, `index`
- `fragment` optional
- `durability_mode` optional
- `object_concurrency` optional
- `status_batch` optional

Status shape:

```json
{
  "is_running": true,
  "phase": "Scanning",
  "fast_mode": false,
  "durability_mode": "auto",
  "scope": "all",
  "total_fragments_scanned": 1,
  "total_objects_scanned": 128,
  "total_objects_need_redistribution": 2,
  "total_objects_migrated": 2,
  "total_errors": 0,
  "current_fragment": "fragment-hash",
  "last_object": "videos/demo.mp4",
  "last_error": null,
  "start_timestamp": 1775900600,
  "duration_seconds": 6,
  "scan_rate_per_sec": 21.3
}
```

### Trash Job Status

Routes:

- `GET /n1/maintenance/trash/recover/status`
- `GET /n1/maintenance/trash/empty/status`

These expose background recover-all and empty-trash job progress when persistent trash jobs are in flight.

### `GET /n1/maintenance/health`

System-wide health report combining:

- disk status
- consistency scan results
- system metrics
- upload runtime stats
- index runtime stats
- storage plane stats
- maintenance job state

Response shape:

```json
{
  "sampled_at": 1775900700,
  "status": "healthy",
  "disks": [
    {
      "path": "/mnt/DISK0",
      "role": "blob",
      "status": "online",
      "total_bytes": 1000000000000,
      "used_bytes": 120449024,
      "available_bytes": 999879550976,
      "usage_percent": 0.01,
      "latency_ms": 2
    }
  ],
  "consistency": {
    "total_partition_dbs_scanned": 16,
    "index_partition_health": "healthy"
  },
  "system": {
    "cpu_usage_percent": 5.2,
    "memory_used_bytes": 123456789
  },
  "uploads": {
    "active_fragments": 0,
    "active_sessions": 0,
    "reserved_bytes": 0,
    "committed_parts": 0,
    "committed_bytes": 0,
    "stale_sessions": 0,
    "stale_reserved_bytes": 0
  },
  "index_runtime": {
    "loaded_partition_runtimes": 8,
    "wal_appenders": 8,
    "prefix_projection_fragments_pending": 0
  },
  "storage": {
    "index_roots_total": 2,
    "index_roots_online": 2,
    "blob_devices_total": 4,
    "blob_devices_online": 4,
    "raw_total_bytes": 4000000000000,
    "raw_used_bytes": 120449024,
    "raw_available_bytes": 3999879550976,
    "heterogeneous_blob_capacity": false
  },
  "maintenance": {
    "access_mode": "availability",
    "repair_running": false,
    "repair_phase": "Completed",
    "redistribution_running": false,
    "redistribution_phase": "Completed",
    "gc_running": false,
    "gc_phase": "Completed",
    "pending_redistribution_intents": 0,
    "failed_redistribution_intents": 0,
    "fragments_with_pending_redistribution": 0,
    "trash_recover_running": false,
    "trash_recover_phase": "",
    "trash_recover_fragment": "",
    "trash_recover_total_objects": 0,
    "trash_recover_processed_objects": 0,
    "trash_empty_running": false,
    "trash_empty_phase": "",
    "trash_empty_fragment": "",
    "trash_empty_total_objects": 0,
    "trash_empty_processed_objects": 0,
    "trash_empty_total_roots": 0,
    "trash_empty_processed_roots": 0
  },
  "summary": "All disks online; no maintenance debt."
}
```

## Recommended Standalone Flow

### 1. Create A Fragment

```bash
curl -k https://127.0.0.1:50001/n1/fragments \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"mainframe",
    "secret":"<MASTER_KEY>",
    "key":"fragment-local-secret"
  }'
```

### 2. Mint A Fragment Token

```bash
curl -k https://127.0.0.1:50001/n1/auth/fragment \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment":"<fragment-hash>",
    "secret":"fragment-local-secret",
    "read":true,
    "write":true,
    "modify":true,
    "delete":true,
    "expires_in":3600
  }'
```

### 3. Upload A File

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n1/upload/one-shot/videos/demo.mp4" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: video/mp4" \
  --data-binary @demo.mp4
```

### 4. Browse With POSIX Or Object Listing

```bash
curl -k \
  "https://127.0.0.1:50001/n1/posix/list?path=videos&limit=200" \
  -H "Authorization: Bearer <token>"
```

```bash
curl -k \
  "https://127.0.0.1:50001/n1/objects?prefix=videos/&delimiter=/&limit=100" \
  -H "Authorization: Bearer <token>"
```

### 5. Trash, Recover, And Reclaim

```bash
curl -k -X POST https://127.0.0.1:50001/n1/posix/trash \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"path":"videos/demo.mp4"}'
```

```bash
curl -k -X POST https://127.0.0.1:50001/n1/posix/recover \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"path":"videos/demo.mp4"}'
```

```bash
curl -k -X DELETE https://127.0.0.1:50001/n1/trash/empty \
  -H "Authorization: Bearer <token>"
curl -k -X POST https://127.0.0.1:50001/n1/maintenance/gc/start \
  -H "Authorization: Bearer <token>"
```

## Operational Notes

- `N1 2.0` is lifecycle-first. Clients should think in terms of `trash`, `recover`, and `GC`, not immediate physical delete.
- Standalone namespace routes and raw object routes are both available. New integrations should prefer the namespace-aware `2.0` surfaces unless they specifically need lower-level object semantics.
- Some helper routes remain legacy-shaped. The most visible example is `GET /n1/fragments/check` and `GET /n1/fragments/hash`, which currently deserialize JSON bodies.
- Signed-read URLs are the preferred way to expose public view, stream, or download without leaking bearer tokens.
- SMB Lite and POSIX routes are backed by the same namespace/runtime model, so rename, mkdir, trash, and recover are meant to stay consistent across surfaces.
