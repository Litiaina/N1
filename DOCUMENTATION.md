# N1 programmer and operator documentation

This guide describes N1 1.0.0-RC.1 and its API and operational tools. The formal
storage-format specification is 1.0.0-RC.1, retaining encoding identifier 4;
unsupported older development roots are not migrated automatically. For internals, see
[ARCHITECTURE.md](ARCHITECTURE.md); for routes, [NOA.md](NOA.md); for actual test
results and limitations, [TESTING.md](TESTING.md).

Jump to [authentication](#authentication-reference), [objects](#object-api),
[uploads](#upload-api), [exports](#archive-download), [native](#native-protocol),
[backups](#backup-and-restore) or [setup](#operator-setup).

## Release versioning and compatibility

N1's formal release sequence starts at `1.0.0-RC.1` (October 18, 2026).
Earlier 3.x/4.x versions were private development identifiers, not previous
stable releases. Engine, Shell, native client and bundled tools inherit the
same version from the workspace manifest.

- Further candidates are `1.0.0-RC.2`, `1.0.0-RC.3`, etc.; the first stable
  release is `1.0.0`. RC status is not stable or LTS support. LTS is a separate
  support declaration, not something inferred from a version number.
- After stable, patch versions fix compatible behavior, minor versions add
  compatible capabilities, and major versions declare breaking public changes.
- The storage specification independently starts at `1.0.0-RC.1`. Existing
  encoding integer `4`, `N1NCX4` magic, member descriptors and backup format
  `n1-backup-v1` remain unchanged. They are encoding identifiers, not product
  release numbers. A product-only update does not bump the storage specification.
- NOA remains `/noa/v1`; native framing remains `n1/0.5`. Incompatible storage
  or wire changes require an explicit contract revision and documented migration
  or compatibility procedure, not silent reuse of the current identifier.
- The private `4.0.0` to formal `1.0.0-RC.1` transition is a numbering reset,
  not an on-disk downgrade. Do not use numeric ordering against those private
  labels to infer update safety. Automatic updates are not introduced by RC.1.

Before updating storage deployments, keep a verified independent recovery point,
check release/format compatibility, and use the matching Engine/Shell pair.
Unsupported formats must not be forced by editing stored version markers.
Check the engine without starting it using `litiaina-n1 --version`; it reports
the product, storage specification, encoding and protocol versions. Git release
tags use `v1.0.0-RC.1`; published candidate tags must not be moved to new code.

## API conventions

- HTTPS base: https://storage.example:50001/noa/v1. Trust the server certificate;
  use --cacert for a private CA, not disabled verification.
- JSON requests use Content-Type: application/json. Raw upload/download bodies
  are bytes; batch upload uses multipart/form-data.
- Protected requests use a fragment bearer token. read, write, modify and delete
  permissions are independent; master/fragment secrets stay out of browser URLs.
- Object keys are logical paths, not host filesystem paths. Encode path/query
  values correctly and reject parent traversal. A key and its prefix can coexist.
- Prefix mutations may partially succeed. Inspect per-child conflicts/failures;
  an interrupted request is not proof that no work committed. Copy/delete retry
  accepts an identical destination but never overwrites an unrelated object.
- GET selects the latest committed version unless a version is specified.
  Latest means maximum valid ULID, not a mutable head. Pin versions for stable
  reads/export and preserve opaque continuation tokens.
- Empty mkdir is rejected. Deletion is soft-only; TTL/GC owns reclamation.
- Public authentication/fragment-create JSON is limited to 64 KiB; protected
  JSON controls to 4 MiB. Streaming upload bodies have separate limits.
- Trash/delete requires `delete`; rename/move/recover requires `modify`.
  Protected object responses use `Cache-Control: private, no-store`.

### HTTP rate limiting

In `[rate_limiter_config]`, `auth`, `view`, `upload` and `global` settings named
`*_limit_per_second` are sustained **requests per second**, not refill seconds.
`*_limit_burst_size` is the independent initial/maximum burst. For example,
`view_limit_per_second=30` and `view_limit_burst_size=100` allow a burst of 100,
then replenish about one request every 33.33 ms, capped at 100 available tokens.

Auth/global budgets are IP-scoped; view/upload budgets are fragment-scoped and
shared by clients using that fragment. These are process-local HTTP limits,
not native-protocol or distributed quotas. Enabled rates must be 1–1,000,000,000
and bursts nonzero; invalid numeric bounds refuse startup. Use
`enable_rate_limiting=false` to disable the layers, not zero-valued quotas.
Settings are loaded at startup; restart after changing them.

## Quick start

Examples use Bash and placeholders; replace secrets locally and do not commit
them. The hostname must match the TLS certificate. Create and authenticate a
fragment before uploading:

```bash
BASE='https://storage.example:50001'
curl --fail-with-body "$BASE/noa/v1/ping"
curl --fail-with-body "$BASE/noa/v1/fragments" \
  -H 'Content-Type: application/json' \
  -d '{"name":"example","secret":"<MASTER_KEY>","key":"<FRAGMENT_SECRET>"}'
curl --fail-with-body "$BASE/noa/v1/auth/fragment" \
  -H 'Content-Type: application/json' \
  -d '{"fragment":"<FRAGMENT_HASH>","secret":"<FRAGMENT_SECRET>","read":true,"write":true,"modify":true,"delete":true}'

TOKEN='<returned-access-token>'
AUTH=(-H "Authorization: Bearer $TOKEN")
curl --fail-with-body "$BASE/noa/v1/upload/one-shot/docs/example.txt" "${AUTH[@]}" \
  -H 'Content-Type: text/plain' --data-binary @example.txt
curl --fail-with-body "$BASE/noa/v1/posix/list?path=docs&limit=200" "${AUTH[@]}"
curl --fail-with-body "$BASE/noa/v1/objects/download?object_key=docs/example.txt" \
  "${AUTH[@]}" --output downloaded.txt
```

Uploading docs/example.txt creates the virtual docs/ prefix automatically.
For signed browser downloads, multipart and exports, use the contracts below.

## Configuration

N1 reads `n1_config.ini` and `n1.env` from the server working directory.
Keep the generated configuration sections. The `auto_healing` section is
required; invalid values fail startup. This example shows selected settings,
not a replacement for the complete generated configuration.

Read paths validate the existing shard checksum before serving that shard and
reconstruct within parity tolerance when validation fails. Validation/file
handles are request-local, not an object cache; a new request validates again.
The unchanged format has whole-shard checksums, so first-byte/range latency can
include reading the selected shard for verification. This is not block-level
checksum framing and does not claim zero verification overhead.

Full availability repair and checksum walks run through explicit repair
requests, with an optional calendar schedule for online checksum repair.
Legacy `auto_healing` scan switches/intervals alone do not launch these
walks. Known-member identity/writability probes and startup safety validation
remain; they do not enumerate objects. Deep health inspection is explicit,
not a periodic engine job. The legacy health `auto_healing.enabled` field
describes whether calendar scheduling is enabled; the nested `scheduled_scrub`
field reports its actual phase, errors and last successful completion.

Use `refresh=true` for a newly collected health snapshot. Plain health requests
may return the last cached summary; `sampled_at` identifies its age. No background
health worker refreshes that summary. Deep content inspection additionally needs
`deep_scan=true`.

Scheduled GC runs the same conservative collector as manual GC. It expires
trash using `trash_retention_seconds` and abandoned upload data using
`file_ttl_seconds`. This includes ordinary-upload pieces left after a restart
and unpublished versions beneath otherwise live objects. Active uploads and
committed historical versions are preserved. New unfinished payload is owned by
`.uploads/<version>/@o_<object-id>/payload`; those candidates are reclaimed before
the NCX reachability scan. Committed orphan discovery still needs
namespace-reference and object-directory traversal. GC shares BackgroundGc I/O admission and yields between object/session
batches; a low interval is not a free operation at large object counts.
If member identity, namespace or export protection cannot be established, GC
preserves data and reports the error through `/maintenance/gc/status`.

Minimal two-member storage pool (1+1):

```ini
[server]
host=127.0.0.1
https_port=50001
native_port=50002
native_max_connections=32
cert_path=cert.pem
key_path=key.pem

[disk_config]
fs_storage_path=/var/lib/litiaina/n1-control

[storage_pool.A]
generation=1
roots=/mnt/storage-00,/mnt/storage-01
data_shards=1
parity_shards=1
sealed=false

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
trash_retention_seconds=2592000

[auto_healing]
enabled=false
scrub_schedule_utc=
scheduled_scrub_pause_millis=25
topology_check_interval_seconds=30
topology_recovery_stability_seconds=10
initial_scan_delay_seconds=60
availability_scan_interval_seconds=3600
checksum_scrub_interval_seconds=604800
object_scan_concurrency=1
shard_repair_concurrency=1
metadata_batch_size=128

[upload_config]
max_part_size=52428800
stripe_block_size=262144
max_concurrent_io=auto
upload_memory_budget_bytes=auto
adaptive_concurrency_enabled=true
upload_io_gate_enabled=true
upload_part_strict_durability_default=true

[io_control]
foreground_wait_timeout_ms=30000
stall_threshold_ms=20000
slow_io_threshold_ms=1000
latency_samples_per_class=1024
```

`n1.env` must provide the secrets used by N1:

```dotenv
JWT_SECRET=<independent-random-secret>
AUTH_KEY=<independent-random-secret>
MASTER_KEY=<independent-random-secret>
ADMIN_KEY=<independent-random-secret>
SIGNED_READ_URL_SECRET=<independent-random-secret>
```

`N1_IO_BACKEND=auto` (default) selects the built-in io_uring backend when kernel
support and process permissions allow it. `N1_IO_BACKEND=uring` requires it and
fails startup if unavailable; `N1_IO_BACKEND=blocking` explicitly selects the
compatibility backend. Invalid values fail startup. Set the variable in the
service environment or `n1.env`, then restart N1. Automatic fallback occurs only
during backend initialization, is logged, and does not mask later storage errors.
No extra daemon or package is required. The backend does not weaken configured
durability or change pool/storage formats; throughput gains require measurement.
The selection governs asynchronous data/read/sync operations. Synchronous
metadata publication uses direct POSIX writes and syncs in its existing workers;
health reports this fixed policy as `publication_backend: posix` alongside the
selected asynchronous backend and its counters. It is not a retry or fallback
after a native operation fails. A native completion count is not a count of all
filesystem operations.

`N1_ACCEL_SEARCH_URL` enables the optional authenticated namespace-search
proxy. The browser continues to authenticate to N1 with its fragment token;
N1 forwards only the search request and authority to the configured local
Accel backend. Accel can therefore remain bound to loopback and is never a
second public authentication surface. If the variable is absent or Accel is
unavailable, ordinary namespace listing and every authoritative N1 operation
continue to work; only accelerated search returns `503`.

Capacity reports deduplicate physical devices. Uploads do not scan free space
synchronously; actual kernel writes are authoritative and capacity exhaustion
returns 507. Pool membership/geometry is immutable: append a new complete pool
for expansion, not index/blob roots. Protect generated secrets and keep them
separate from application credentials. Optional Accel setup is below.

The example explicitly enables strict upload durability for production use.
Generated configs retain `upload_part_strict_durability_default=false`: relaxed
acknowledgements are not a power-loss guarantee. Per-request overrides apply to
one-shot, batch and multipart parts. This does not change NCX copy/delete.

## Native protocol

Use the canonical n1_native_client Rust crate; do not invent another framing
layer. The n1/0.5 listener is plain TCP on n1://host:50002, so keep it on a
trusted network or a protected tunnel. Native authentication does not encrypt
the connection. HTTPS/NOA remains the public TLS interface.

### Frame Format

Every frame uses a fixed length prefix followed by a MessagePack payload:

```text
u32_be frame_length
MessagePack payload
```

Unauthenticated control frames are limited to 64 KiB; authenticated frames to
8 MiB. Use streaming/range/multipart for large
objects rather than embedding them in a frame.

`[server] native_max_connections` defaults to 32 (valid range 1–4096). Excess
connections close without admission; size it for the host/client connection
pool. This limits sockets, not writers. Control reads time out after 15 seconds
before authentication and 120 seconds afterward. Raw uploads require prior
write authority, allow at most 1 GiB per native body and use a 30-second inactivity
timeout. An invalid declaration closes the connection without reading its body.

Client request:

```json
{
  "id": "request-id",
  "op": "namespace.stat",
  "payload": "{\"path\":\"videos/clip.mp4\"}",
  "body": null
}
```

Operation response:

```json
{
  "id": "request-id",
  "ok": true,
  "status": 200,
  "payload": null,
  "body": "MessagePack-encoded operation result bytes",
  "body_len": 12345,
  "error": null
}
```

Important details:

- `id` is echoed by the server so clients can correlate requests.
- `op` selects the native operation.
- request `payload` is UTF-8 JSON text stored inside the MessagePack frame.
- operation response `body` is MessagePack-encoded typed data for normal operations. Native does not duplicate the same data as JSON in the envelope.
- streaming response operations set `body_len`, omit the large response body from the MessagePack frame, and then write exactly that many raw bytes after the response frame.
- `ok=false` means the native transport or typed operation failed.
- `status` is present for typed operations that return API-style status bodies.
- `error.code` and `error.message` are present for native-layer failures.

Native sockets use `TCP_NODELAY` to avoid delayed-ACK/Nagle latency on small framed request/response workloads.

### Authentication

Native connections hold session state. Authenticate once, then reuse the connection:

```json
{
  "id": "auth-1",
  "op": "auth.fragment",
  "payload": "{\"fragment\":\"<fragment_hash>\",\"secret\":\"fragment-local-secret\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":3600}",
  "body": null
}
```

The server calls the fragment authorization core directly and stores the returned bearer token in that native connection session. Future native operations on the same connection automatically use that bearer.

Other auth/session operations:

- `auth.bearer`: install an existing JWT bearer token into the connection session.
- `auth.clear` or `logout`: clear native session state.
- `session.info`: inspect the current native session permissions.

### Operations and tools

| Family | Native operations |
| --- | --- |
| Session | auth.fragment, auth.bearer, auth.clear/logout, session.info |
| Namespace | namespace.list, namespace.stat, namespace.mkdir (rejected), namespace.rename, namespace.trash, namespace.recover |
| Object | object.list, object.status, object.repair, object.delete, object.recover |
| Trash | trash.list, trash.recover_all |
| Upload progress | uploads.list, uploads.status, multipart.status, multipart.list |
| Maintenance | maintenance.health; maintenance.repair/gc/redistribution.start, .status, .stop |
| Fragment/capacity | fragment.create, fragment.list, fragment.hash, fragment.hash.public, fragment.usage, capacity.usage |

Reuse authenticated connections. Complete raw streaming responses before sending
another frame. Native multipart is durable/restart-resumable. The canonical
client and release-proof runner are the source for upload/stream operation
payloads; [native_client](native_client) is the reusable integration crate.

Rust client example, using `n1_native_client` and `serde_json`:

```rust
use n1_native_client::{N1NativeClient, NativeClientResult};
use serde_json::{json, Value};

async fn list_docs(fragment: &str, secret: &str) -> NativeClientResult<Value> {
    let mut client = N1NativeClient::connect("n1://127.0.0.1:50002").await?;
    client.auth_fragment(fragment, secret).await?;
    client.operation_json(
        "namespace.list",
        &json!({"path": "docs", "limit": 200}),
        None,
    ).await
}
```

The convenience `auth_fragment` requests all four permissions. For a restricted
session, send explicit permissions with `auth.fragment` or install an existing
restricted token using `auth.bearer`. Use `operation_json` for typed MessagePack
results; `raw_json` is for JSON-envelope operations, not namespace responses.

The operator shell also supports trusted native connections:

```bash
litiaina-n1-shell --native-url n1://127.0.0.1:50002
litiaina-n1-shell --url https://storage.example:50001
```

Use help inside the shell for commands and flags. Browser setup is covered in
[operator setup](#operator-setup).

## Authentication reference

### `POST /noa/v1/auth/fragment`

Body:

```json
{
  "fragment": "<fragment_hash>",
  "secret": "fragment-local-secret",
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
  "refresh_token": "<refresh-token>",
  "refresh_expires_in": 604800
}
```

Use returned expiry values; server configuration bounds requested token life.

Authentication with `MASTER_KEY` grants signed `operator=true`; a fragment
secret never does, even with all four permissions. Clients cannot grant this
capability in request JSON. Native `session.info` reports it. Reauthenticate with
the master key for whole-server maintenance after upgrading older tokens.
Ordinary object reads/writes remain scoped to the authenticated fragment.

New fragment credentials use Argon2 only. New/rotated credentials do not generate
NT hashes; old optional NT fields remain readable until rotation removes them.

### `POST /noa/v1/auth/refresh`

Body:

```json
{ "refresh_token": "<refresh-token>" }
```

Refresh sessions are bound to the verified fragment credential (or master key
for an operator). Rotating that credential rejects old refresh tokens; legacy
sessions without this binding require fresh login. Already issued access tokens
retain normal expiry, not immediate revocation. No per-object revocation lookup
or new namespace record is introduced.

### `POST /noa/v1/auth/logout`

Body:

```json
{ "refresh_token": "<refresh-token>" }
```

## Fragment API

### `POST /noa/v1/fragments`

Creates a fragment. This route uses the master secret and does not use a fragment token.

Body:

```json
{
  "name": "mainframe",
  "secret": "<MASTER_KEY>",
  "key": "fragment-local-secret"
}
```

Response:

```json
{
  "fragment_hash": "<fragment_hash>",
  "name": "mainframe"
}
```

### `GET /noa/v1/fragments`

Lists fragments visible to the authenticated caller.

### `GET /noa/v1/fragments/check`

Checks whether a fragment hash exists.

Body:

```json
{ "fragment": "<fragment_hash>" }
```

### `GET /noa/v1/fragments/hash`

Returns the fragment hash for a fragment name when that fragment exists.

Body:

```json
{ "fragment": "mainframe" }
```

### `PATCH /noa/v1/fragments/{fragment}`

Updates fragment name or fragment key.

Body:

```json
{
  "new_name": "mainframe-renamed",
  "new_key": "new-fragment-secret",
  "secret": "<MASTER_KEY>"
}
```

### `DELETE /noa/v1/fragments/{fragment}`

Deletes a fragment. Requires all of the following:

- a token scoped to the same fragment
- `delete: true` permission
- the N1 master key
- an exact fragment-hash confirmation in the body

Body:

```json
{
  "secret": "<MASTER_KEY>",
  "confirm_fragment": "<fragment_hash>"
}
```

N1 rejects the request if `confirm_fragment` does not exactly match `{fragment}`.

### `GET /noa/v1/capacity/usage`

Returns system-level configured storage capacity and availability.

### `GET /noa/v1/capacity/fragment`

Returns storage usage for the authenticated fragment, plus current server
capacity totals:

```json
{
  "fragment": "<fragment_hash>",
  "state": "active",
  "committed_bytes": 104857600,
  "reserved_bytes": 0,
  "trash_bytes": 0,
  "used_bytes": 104857600,
  "capacity_used": 1073741824,
  "capacity_allocated": 1099511627776,
  "capacity_available": 1098437885952,
  "filesystem_count": 2,
  "capacity_usage_percent": 0.0976,
  "created_at_unix": 1778000000,
  "updated_at_unix": 1778000001
}
```

### `POST /noa/v1/maintenance/fragment/init`

Initializes the storage structure for the authenticated fragment. This is normally handled during fragment creation, but the route is useful after manual disk preparation.

### `POST /noa/v1/maintenance/fragment/cleanup`

Cleans stale fragment metadata and old orphaned blob directories for the authenticated fragment.

Response:

```json
{
  "fragment": "<fragment_hash>",
  "usage_bytes": 104857600,
  "orphan_blob_dirs_removed": 2,
  "message": "Fragment metadata cleanup complete."
}
```

## POSIX Namespace API

These routes operate on the user-facing namespace.

### `GET /noa/v1/posix/list`

Query parameters:

- `path`: virtual prefix, default `""`
- `limit`: page size, default `200`, max `1000`
- `continuation_token`: optional cursor
- `trash`: `true` to list trash

Example:

```bash
curl --fail-with-body "$BASE/noa/v1/posix/list?path=videos&limit=200" "${AUTH[@]}"
```

Response:

```json
{
  "path": "videos",
  "scan_trash": false,
  "entries": [
    {
      "name": "clip.mp4",
      "path": "videos/clip.mp4",
      "kind": "file",
      "modified_at_unix": 0
    }
  ],
  "continuation_token": null
}
```

Live LIST is names-only: optional size/version/content fields are not object
metadata guarantees. A zero modified_at_unix is not a business timestamp.
Use stat/meta when metadata is needed. Follow continuation_token unchanged;
pages are not a namespace snapshot under concurrent changes.

### `GET /noa/v1/posix/stat`

Query parameters:

- `path`
- `trash=true` for trash lookup

### `POST /noa/v1/posix/mkdir`

Format 4 rejects this operation with `400 Bad Request`: empty folders are not objects.

Body:

```json
{ "path": "videos/2026" }
```

### `POST /noa/v1/posix/rename`

Also available as `POST /noa/v1/posix/move`.

Body:

```json
{
  "source_path": "videos/clip.mp4",
  "destination_path": "archive/clip.mp4"
}
```

Use a trailing slash for prefix-only work: source_path: "videos/" selects
children even if a separate videos object exists. Without it, an exact key
takes precedence. Prefix rename/move returns affected plus any conflicted_paths
and failed_paths; inspect these even on a successful HTTP response. Do not
assume an atomic folder transaction or overwrite of an unrelated destination.

### `POST /noa/v1/posix/trash`

Soft deletes a key or processes a virtual prefix per key. Prefix operations are
not atomic and report `conflicted_paths` / `failed_paths` alongside counts.

Body:

```json
{ "path": "archive/clip.mp4" }
```

### `POST /noa/v1/posix/recover`

Copies the selected NCX back before deleting its trash replicas. Send the path
and, when available, trash_entry_id returned by trash listing. Path-only recovery
selects the newest retained deletion; use the ID to recover a specific one.
An unrelated live destination conflicts; an identical destination permits retry.

Body:

```json
{ "path": "archive/clip.mp4", "trash_entry_id": "<ID_FROM_TRASH_LIST>" }
```

## Object API

These routes accept namespace paths. N1 resolves them to stable object identities internally.

### `GET /noa/v1/objects`

Lists live objects. Query parameters:

- `prefix`
- `limit`
- `continuation_token`
- `delimiter`

Example:

```bash
curl --fail-with-body "$BASE/noa/v1/objects?prefix=videos/&limit=200" "${AUTH[@]}"
```

### `GET /noa/v1/namespace/snapshot`

Streams every live user-facing namespace file exactly once as
`application/x-ndjson`. Each `entry` frame contains the logical `object_key`,
immutable `version_id`, content type, byte size, and creation time. A successful
stream always ends with one `complete` frame containing the exact entry count.

For long inventories, request `?progress=true` to also receive `progress` frames
with increasing `scanned_entries` and the `entry_count` emitted so far. These
report actual traversal, including skipped replicas, not successful completion.
They keep advancing scans distinguishable from stalled connections without
increasing an idle timeout. The default stream remains `entry`/`complete` only.

This endpoint is intended for disposable search/index projections such as
Litiaina Accel. It performs one bounded-memory namespace traversal, does not
read object payloads, does not expose physical object identities, and requires
fragment-scoped read authority. Consumers must reject a stream without its
completion frame rather than publishing a partial index.

### `GET /noa/v1/namespace/search`

Authenticated browser-safe proxy for the configured Litiaina Accel namespace
index. It accepts Accel's search query contract (`q`, `scope`, `field`,
`match`, `limit`, and `continuation_token`) and forwards the caller's fragment
read authority. N1 fixes the backend id from server configuration; clients
cannot select an arbitrary origin or turn this route into a general proxy.

Search is a disposable projection only. Results contain logical paths and
immutable public version summaries. N1 remains authoritative for lookup,
listing, reads, and mutations. Exact and prefix matches use the ordered index;
contains matching scans only Accel's bounded in-memory projection, never N1
object bytes.

### `GET /noa/v1/trash`

Lists trash entries. Supports the same paging style as object listing.

### `GET /noa/v1/objects/meta/{object_key}`

Returns public object metadata for the latest version.

### `GET /noa/v1/objects/versions/{object_key}`

Returns committed versions for a file path, newest first by version ULID. Internal shard layout is stripped from the public response.

### `GET /noa/v1/objects/get/{object_key}`

Returns the object payload response.

### `GET /noa/v1/objects/stream/{object_key}`

Streams an object. Supports HTTP `Range`.

### `GET /noa/v1/objects/download`

Query parameters:

- `object_key`
- `version_id` optional
- `filename` optional

Example:

```bash
curl --fail-with-body "$BASE/noa/v1/objects/download?object_key=videos/clip.mp4" \
  "${AUTH[@]}" \
  -o clip.mp4
```

### `GET /noa/v1/objects/download/chunk`

Query parameters:

- `object_key`
- `offset`
- `chunk_size`

Server-side limits:

- `chunk_size` must be greater than `0`.
- `chunk_size` must be `<= 8388608` bytes (`8 MiB`).
- Use `/noa/v1/objects/download` or `/noa/v1/objects/stream/{object_key}` for normal large downloads. The chunk route exists for bounded browser save flows and must not be used with huge per-request buffers.

Responses:

- `200 OK`: body contains the requested bytes.
- `204 No Content`: `offset` is at or beyond object size.
- `400 Bad Request`: invalid or overflowing range.
- `413 Payload Too Large`: requested chunk exceeds the server maximum.

### `POST /noa/v1/objects/signed-url`

Creates a temporary signed read URL.

Body:

```json
{
  "object_key": "videos/clip.mp4",
  "disposition": "download",
  "expires_in": 3600,
  "version_id": null,
  "filename": "clip.mp4"
}
```

### `GET /noa/v1/objects/signed-read`

Public signed read endpoint. The signed URL generator returns the full URL.

### `DELETE /noa/v1/objects/{object_key}`

Soft deletes an exact key or the keys under a virtual prefix.

### `POST /noa/v1/objects/recover/{object_key}`

Recovers a trash path.

### `POST /noa/v1/objects/rename`

Body:

```json
{
  "source_fragment": "<fragment_hash>",
  "source_key": "videos/clip.mp4",
  "destination_fragment": "<fragment_hash>",
  "destination_key": "archive/clip.mp4"
}
```

Cross-fragment rename is rejected with 400. For partial prefix reports, use
posix/rename or posix/move. The object rename endpoint returns an empty 200 response.

### `POST /noa/v1/objects/bulk-delete`

Body:

```json
{ "object_keys": ["a.txt", "b.txt"] }
```

### `POST /noa/v1/objects/bulk-recover`

Body:

```json
{ "object_keys": ["a.txt", "b.txt"] }
```

## Upload API

Use one-shot for small files and standard multipart for large ingest. Use
durable multipart when unfinished progress must survive an engine restart.

Multipart upload is pipelined inside N1's foreground write lane. Different parts can upload concurrently, and each part writes/flushes/publishes its shard set in parallel across the configured disks. N1 intentionally serializes retries for the same `(version_id, part_index)` so a duplicate client retry cannot race the same receipt or stable shard files.

### One-shot Upload

```http
POST /noa/v1/upload/one-shot/{object_key}
```

Query parameters:

- `checksum` optional
- `durability` optional
- `strict_durability` optional boolean
- `io_gate` optional boolean

Request body is raw bytes.

Example:

```bash
curl --fail-with-body "$BASE/noa/v1/upload/one-shot/docs/readme.txt" \
  "${AUTH[@]}" \
  -H 'Content-Type: text/plain' \
  --data-binary @readme.txt
```

### Standard Small-Object Batch Upload

Use the standard batch route when one client must ingest many independent
small files. It removes one HTTP authentication/request cycle per object while
preserving a separate object id, EC layout, NDX, namespace entry, result, and
failure boundary for every file.

```http
POST /noa/v1/upload/batch?concurrency=32
Content-Type: multipart/form-data
```

Each file field must be named `object`, and its multipart filename must be the
percent-encoded object key. Each object is limited to one MiB; use ordinary
one-shot or multipart for larger files.
Requests accept at most 4,096 fields. The server consumes the multipart body
incrementally. Strict uploads to enrolled pools acquire the existing shared
I/O permit before buffering each group (at most 64 objects and 8 MiB). Groups
use at most 16 concurrent payload writers. `concurrency` is optional, clamped
to 1–128; neither path bypasses global or per-fragment I/O admission. Relaxed
uploads retain the independent one-shot path.

```bash
curl --fail-with-body "$BASE/noa/v1/upload/batch?concurrency=32" \
  "${AUTH[@]}" \
  -F 'object=@a.txt;filename=docs%2Fa.txt;type=text/plain' \
  -F 'object=@icon.png;filename=images%2Ficon.png;type=image/png'
```

All-success responses use `201 Created`. A mixed batch uses `207 Multi-Status`
and reports `status`, `version_id`, `size_bytes`, and any error separately for
each object. Batch upload is standard-only: it creates no durable session,
manifest, binding, or receipt and cannot resume after process restart.
It accepts the same `durability=strict|batched` and `strict_durability` options
as one-shot upload, falling back to the configured upload default. Strict
durability and restart-resumable multipart are different features. A disconnect
may leave complete, unacknowledged objects; list or retry to reconcile.

Strict groups keep ordinary separate shards, immutable NDX and NCX files. The
server writes payload and private metadata, synchronizes the participating
filesystems, publishes NDX, synchronizes, then publishes NCX and synchronizes
before reporting success. Existing healthy NCX is not rewritten for a new
version. There is no packing, new disk format or cross-request commit queue.
Linux `syncfs` covers the whole participating filesystem, including unrelated
writes on it. A filesystem synchronization failure prevents a strict ACK;
already-published complete versions are preserved. Batches are not all-or-nothing
transactions. Repeated keys start a new group and retain separate version results.

N1 performs no synchronous free-space scan before one-shot, batch, standard
multipart, or durable multipart writes. Actual writes to the selected K+P
roots are authoritative. Kernel capacity exhaustion returns `507`, publishes
no namespace entry, and leaves only cleanup/TTL-reclaimable unpublished data.
Capacity reporting remains available asynchronously through monitoring.

### Multipart Initiate

The standard multipart API is the high-throughput path. Initiation and part
completion state live only in process memory. Parts may be uploaded concurrently;
each part is streamed once through EC to its fixed K+P placement. N1 writes no
session manifest, namespace pending record, or per-part receipt on this path.
Finalize synchronizes the completed shard set and then publishes the immutable
object index and namespace entry once according to the parts' effective
durability. An unfinished standard session does not
survive process restart; its unreferenced shards remain invisible and are
reclaimed from `.uploads` by the normal upload TTL scan. A legacy durable
manifest without the staging flag continues to use its original `.objects`
placement. NDX always records committed addresses, not staging addresses.

Both multipart modes allow at most 131,072 parts. Standard initiation additionally
caps pending sessions at 1,024 and part-slot allocations at 16 MiB globally;
saturation returns 503. This bounds temporary slots, not total engine RSS.
Complete/cancel sessions to free capacity; durable uploads keep separate on-disk
resume state.

```http
POST /noa/v1/upload/multipart/init/{object_key}
```

Initiation accepts optional `fresh=true` to allocate an independent session
instead of adopting a same-sized pending upload. The default retains existing
resume discovery. Editors should use `fresh=true` for a new save and retain the
returned version/session IDs for retries. This applies to durable initiation
too; it is not an atomic expected-version/conflict check.

Body:

```json
{
  "content_type": "video/mp4",
  "total_size_bytes": 21474836480,
  "expected_parts": 410,
  "part_size_bytes": 52428800
}
```

Response:

```json
{
  "session_id": "<session_id>",
  "version_id": "01KR..."
}
```

### Multipart Upload Part

```http
PUT /noa/v1/upload/multipart/{version_id}/{part_index}
```

Query parameters:

- `checksum` optional
- `durability` optional (`strict` or `batched`)
- `strict_durability` optional boolean
- `io_gate` optional boolean

Request body is the raw bytes for that part. Distinct part indexes are accepted
concurrently. Repeating a completed part index in the same live process is
idempotent; a concurrent duplicate of an in-flight index returns conflict.

All completed parts must have been acknowledged strict for strict NDX and NCX
finalization. A strict retry of a previously relaxed completed part returns 409;
it cannot upgrade earlier durability without rewriting bytes. Start a new upload
if strict durability is required. Mixed/relaxed parts finalize relaxed. Directory
sync failures on the strict path propagate rather than reporting success.

Clients should use bounded in-flight parts for large files instead of increasing the one-shot size. Start with `2-4` concurrent parts and tune by disk count and disk quality. Very small multipart jobs may not benefit from part concurrency; large files with many parts usually do. N1 will apply upload backpressure when the disk scheduler is saturated.

Example:

```bash
curl --fail-with-body "$BASE/noa/v1/upload/multipart/$VERSION_ID/0?io_gate=true" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @part-000
```

### Multipart Status

```http
GET /noa/v1/upload/multipart/{version_id}
```

Response:

```json
{
  "uploaded_parts": [0, 1, 2],
  "committed_parts": 3,
  "committed_bytes": 157286400,
  "expected_parts": 410,
  "total_size_bytes": 21474836480,
  "session_id": "<session_id>",
  "version_id": "01KR...",
  "object_key": "videos/large.mp4"
}
```

### List Active Multipart Uploads

```http
GET /noa/v1/upload/multipart?limit=100&offset=0
```

### Multipart Finalize

```http
POST /noa/v1/upload/multipart/{version_id}/{session_id}/finalize?object_key=videos/large.mp4
```

Body should match the initiate sizing:

```json
{
  "content_type": "video/mp4",
  "total_size_bytes": 21474836480,
  "expected_parts": 410,
  "part_size_bytes": 52428800
}
```

If any part is missing, N1 returns the uploaded and missing part indexes.

### Multipart Cancel

```http
DELETE /noa/v1/upload/multipart/{version_id}?object_key=videos/large.mp4
```

Cancel removes the pending upload session. GC can later reclaim orphaned partial payloads.

### Durable Multipart Upload

Use the durable variant when unfinished-upload restart recovery matters more
than maximum ingest throughput. Its existing replicated manifest, pending
namespace binding and compact part receipts preserve restart progress.
The request and response bodies are identical to standard multipart; insert
`/durable` after `/multipart`:

```text
POST   /noa/v1/upload/multipart/durable/init/{object_key}
GET    /noa/v1/upload/multipart/durable
GET    /noa/v1/upload/multipart/durable/{version_id}
PUT    /noa/v1/upload/multipart/durable/{version_id}/{part_index}
POST   /noa/v1/upload/multipart/durable/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/durable/{version_id}
```

Durable part upload continues to accept the `durability`,
`strict_durability`, and `io_gate` query controls documented for the previous
multipart contract. Native n1:// multipart retains durable restart-resumable
behavior. These upload controls do not define namespace-mutation durability modes.

## Archive Download

### Restart-safe bulk export

Use an asynchronous export job for large or restart-resumable prefix downloads. The job captures one immutable version for every selected
logical path, builds the complete archive before publication, survives an N1
restart, and exposes a stable browser-resumable download.

Create an export:

```http
POST /noa/v1/exports
Authorization: Bearer <fragment-token>
Content-Type: application/json
```

```json
{
  "keys": ["videos/clip.mp4"],
  "prefixes": ["photos", "reports/2026"],
  "format": "zip",
  "filename": "account-export.zip",
  "export_all": false,
  "include_internal": false
}
```

`format` is `zip` or `tar`. ZIP uses stored entries and ZIP64 so browsers can
open archives larger than the classic ZIP limits without spending CPU on
recompressing already-compressed media. TAR avoids a central directory and is
preferred for extremely high entry counts. `include_internal=true` is rejected;
an export can contain only the authenticated fragment's live user namespace.
The create body is bounded to 4 MiB, at most 10,000 explicit keys, and at most
1,024 prefixes. Use a prefix instead of enumerating millions of keys.

An authenticated business layer whose user-visible path differs from its N1
storage key may instead submit mapped entries:

```json
{
  "entries": [
    {
      "object_key": "01K...opaque-storage-object",
      "archive_path": "patients/2026/scan.dcm",
      "version_id": "01K...immutable-version",
      "offset_bytes": 0,
      "length_bytes": 8388608
    }
  ],
  "format": "zip",
  "filename": "patient-records.zip"
}
```

Mapped entries are mutually exclusive with `keys`, prefixes, and `export_all`.
N1 validates every source path, exact immutable version, archive path, and byte
range before publishing the manifest. Multiple entries may safely reference
different byte ranges of one immutable source object, which lets Space export
packed small objects without unpacking them into temporary files or exposing
opaque storage identities as filenames.

The create call returns `202 Accepted`:

```json
{
  "export_id": "01K...",
  "phase": "queued",
  "status_url": "/noa/v1/exports/01K...",
  "download_url": "/noa/v1/exports/01K.../download",
  "expires_at_unix": 1788240000
}
```

Read progress:

```http
GET /noa/v1/exports/{export_id}
Authorization: Bearer <fragment-token>
```

Phases are `queued`, `capturing_manifest`, `building`, `ready`, `failed`, and
`cancelled`. Progress reports selected/completed object counts and source byte
counts. A ready status additionally returns `artifact_size_bytes`, a quoted
SHA-256 `etag`, and `download_url`. A failed export contains a safe public
`error`; exact failure context remains in N1 logs.

Download when ready:

```http
GET /noa/v1/exports/{export_id}/download
Authorization: Bearer <fragment-token>
Range: bytes=33554432-
If-Range: "<etag>"
```

The response has an exact `Content-Length`, `ETag`, `Accept-Ranges: bytes`, and
`Content-Disposition`. A valid range returns `206 Partial Content` with
`Content-Range`; an unsatisfiable or multi-range request returns `416`. Standard
browser download managers and `curl -C -` can therefore resume the same
artifact instead of restarting its object scan. `If-None-Match` returns `304`
for an unchanged retained artifact.

Browser applications which cannot attach an authorization header to the
browser's native download manager can mint a short-lived URL after the export
is ready:

```http
POST /noa/v1/exports/{export_id}/signed-url
Authorization: Bearer <fragment-token>
Content-Type: application/json

{"expires_in":21600}
```

The response URL targets `GET /noa/v1/signed-exports/download`. Its HMAC binds
the export id, fragment authority, and expiry. The expiry is clamped to the
retained export lifetime. This global download route accepts the same single
byte-range and conditional request headers as the authenticated route, so the
browser owns save location, pause, continuation, and restart-resume without
exposing the fragment token in the URL.

Cancel and remove a retained artifact:

```http
DELETE /noa/v1/exports/{export_id}
Authorization: Bearer <fragment-token>
```

Cancellation returns `202 Accepted`. N1 never exposes an archive until every
selected entry has been validated and every artifact chunk is durable. A crash
during manifest capture repeats capture; a crash during build preserves the
completed manifest, removes partial artifact chunks, and rebuilds that exact
version set.

The optional `[export]` configuration section is:

```ini
[export]
retention_seconds=86400
cleanup_interval_seconds=60
max_concurrent_jobs=2
max_queued_jobs=128
manifest_validation_concurrency=8
object_read_concurrency=8
artifact_chunk_size_bytes=16777216
artifact_write_concurrency=4
max_buffered_bytes_per_job=134217728
max_buffered_bytes_global=268435456
max_concurrent_downloads=32
max_retained_bytes=0
```

Manifest validation, immutable-object read ahead, and artifact writes are
independently concurrent. The ZIP/TAR serializer remains ordered. The two
buffer limits are authoritative byte budgets: N1 reduces the effective reader
or writer concurrency and artifact chunk size when their configured product
would exceed the per-job budget, while the global budget limits aggregate
active export builders. Download admission is separately bounded and returns a
retryable service-unavailable response rather than allocating beyond
`max_concurrent_downloads`.

`max_retained_bytes=0` leaves actual kernel writes authoritative. A nonzero
value bounds ready artifacts plus admitted in-progress reservations. Export
retention is independent of object-trash retention and never deletes source
objects. Garbage collection conservatively defers a fragment while one of its
durable exports is active; this protection is bounded by the export job count
rather than the number of objects in a potentially million-entry manifest.
Expired or explicitly cancelled artifacts are not removed from underneath an
active HTTP download.

### Compatibility streaming archive

#### `POST /noa/v1/objects/archive`

Streams selected files or folders directly to the caller. This compatibility
path has no stable length or restart-resume identity; use `/noa/v1/exports` for
large bulk download, unreliable networks, or browser retry support.

Body:

```json
{
  "keys": ["videos/clip.mp4"],
  "prefix": "docs/",
  "prefixes": ["photos/", "reports/"],
  "format": "tar",
  "filename": "export.tar",
  "export_all": false,
  "include_internal": false
}
```

Use `tar` for very large folder downloads on this live-streaming path. `zip` is
available but is not ideal for very large compatibility streams.

#### `POST /noa/v1/objects/archive/manifest`

Streams an archive from a request manifest.

Query parameters:

- `format`: `tar` or `zip`, default `tar`
- `filename`: optional response filename

## Trash and recovery

Soft delete copies unchanged NCX to every trash holder before removing live NCX;
recovery reverses that sequence. Neither reads nor rewrites NDX/payload. Trash
entry IDs distinguish repeated deletes of the same key; retain the selected ID
when recovering a specific deletion. Prefix work is bounded and non-atomic.

TTL compares trash timestamps with trash_retention_seconds and reclaims only
safely unreferenced immutable data. Active uploads and export protections remain
in force. N1 exposes no empty-trash, per-entry purge, permanent-delete or
version-delete operation. Manual GC does not shorten retention. Retired
destructive HTTP routes return 410; removed native operations are unsupported.

```bash
curl --fail-with-body -X POST "$BASE/noa/v1/trash/recover-all" "${AUTH[@]}"
```

System health reports trash_lifecycle_running, trash_lifecycle_phase,
trash_retention_seconds and trash_versions_expired under maintenance.

## Health and maintenance

System health includes `io_control.backend`: selected `backend`, `requested`
mode, optional `fallback_reason`, bounded `queue_capacity`, current `queued`
and `in_flight` work, and cumulative `submitted`/`completed` operations. Counts
describe native backend requests, including errors and empty operations, not
objects or a durability certificate. The blocking backend leaves native counts
at zero. Ordinary cached health responses are not live queue telemetry.

```http
GET /noa/v1/maintenance/health?deep_scan=true&refresh=true
GET /noa/v1/maintenance/fragments/{fragment}/health?deep_scan=true&refresh=true
GET /noa/v1/maintenance/pools/health?pool=A&deep_scan=false
GET /noa/v1/objects/status/{object_key}?mode=availability
POST /noa/v1/objects/repair/{object_key}?mode=strict
```

Pool diagnostics return `fragment`, `allocation_policy` and `pools`. Omit `pool`
to inspect every configured pool. Each entry reports ID/generation/K+P, P+1 NCX
replicas, sealed/current-write-pool flags, member IDs/slots/current paths,
readiness, fragment-metadata coverage and estimated data space (minimum member
free space × K, not reserved or guaranteed upload capacity). `ready` describes
identity/access readiness, not full content integrity.

Members expose `total_inodes`, `available_inodes` and `capacity_warning`. Inode
counts may be null when unavailable. Warnings flag zero capacity, below 5% free
bytes or fewer than 1,024 free inodes; they do not reserve space or change pool
selection. Old pools still need NCX headroom after expansion. Shell and Console
display these warnings independently of integrity status.

`deep_scan=true` explicitly verifies the authenticated fragment's NCX, committed
NDX, durable manifests/receipts and recorded piece CRCs using the shared verifier.
`integrity.status` is `not_scanned`, `passed`, `failed` or `unavailable`.
Counters report versions and physical NCX replicas checked, not global logical
object totals. Missing members/fragment metadata cannot produce a complete pass.
This is a read-only **live scan**, not a consistent snapshot: concurrent
publication/mutation/reclaim can invalidate it. Quiesce writers and use offline
`pool verify` for a conclusive maintenance check. Only one explicit pool scan
runs at a time; overlapping deep requests return 503. No new data cache is used.
Read permission is required; content scope is the token's fragment. A different
`fragment` parameter is rejected with 403. Native operation:
`maintenance.pools.health` with `pool`/`deep_scan` payload parameters.
The current-write-pool flag identifies allocation selection; existing engine
safety gates still decide whether mutations are permitted. NCX may remain in an
older pool while newer versions of its stable object allocate in another pool;
reference checks recognize that history, but a selected pool scan does not
certify another pool's payload bytes.

```bash
litiaina-n1-shell --url n1://storage.example:50002 --fragment mainframe \
  --secret '<fragment-secret>' pools
litiaina-n1-shell --url n1://storage.example:50002 --fragment mainframe \
  --secret '<fragment-secret>' pools --pool A --deep-scan
```

Console → Resource Monitor → Health exposes pool selection and an explicit
Verify Fragment Contents action. Ordinary refreshes never initiate that deeper
scan. Its offline Shell workflow covers verification, cloning, replacement and
encrypted backups; the browser does not execute host operations or stop N1.

Health reports topology, read-only reason, controller phase, missing/corrupt
replicas, last scans/errors and IO pressure. N1 initially validates enrolled
identities and stabilizes topology before permitting writes. Missing/replaced
members block unsafe mutations; parity-tolerant reads do not authorize writes
to an incomplete namespace-holder set.

Write readiness requires `maintenance.access_mode == "availability"`,
`maintenance.auto_healing.write_safe == true` and no `read_only_reason`.
The healer status includes the live safety gate, not just the last topology
probe. Returning members must pass identity/writability checks and the configured
stability interval before the topology gate clears. That does not certify their
contents; use explicit verification/repair for that. Health is an observation,
not a write reservation; clients must still handle a subsequent `503` if
availability changes.

Object status includes version_id, status, total_parts/shards, healthy_shards,
missing_shards, corrupt_shards, repairable, repaired_shards, failed_repairs and
recorded/current layout information. Status and repair have bounded admission;
saturation returns 503. Repair preserves recorded pool geometry and membership.
It does not move healthy old-pool objects to a new expansion pool.

Modes are availability (online placement checks), checksum (checksum checks),
strict (foreground mutations blocked) and scrub (quiesced work). A read-only
maintenance mode restricts clients; it does not mean repair writes no data.
Read-triggered repair does not schedule full availability/checksum cycles.
Start those jobs explicitly or opt into a calendar checksum schedule. Unsafe topology keeps writes blocked; do not
override identity checks to force service startup.

For an optional weekly online checksum scan, set `enabled=true` and
`scrub_schedule_utc=Sun 02:00` in `[auto_healing]`, then restart N1. Times are UTC;
`daily 02:00` is also accepted. Empty/absent schedules are disabled even when
legacy `enabled` and interval settings are present. Invalid schedule text is
reported as `scheduled_scrub.phase=invalid_schedule` without starting a scan.
`initial_scan_delay_seconds` applies after startup; no missed slot is replayed.

Scheduled checks use the existing repair engine in online availability access
mode, checksum verification, `object_scan_concurrency`,
`shard_repair_concurrency`, and `metadata_batch_size`. The pause between objects
defaults to 25 ms (`scheduled_scrub_pause_millis`); foreground I/O can add up to
one second of backoff before the next object. Manual and scheduled repair share
one running-job guard. Busy slots are skipped until the next scheduled time.
This keeps requests available but does not promise unchanged latency or zero
extra disk I/O. A large in-flight object is not paused halfway through repair.

Poll `/maintenance/repair/status` for scan progress and refreshed health for
`maintenance.auto_healing.scheduled_scrub`. A stopped or failed scan reports
`incomplete`; a restart during a scan reports interrupted coverage. The last
successful completion is retained separately. These outcomes do not assert
storage failure or make N1 read-only. No whole-scan timeout is imposed. Existing
per-operation I/O errors and topology safety checks still apply. The existing
operator repair-stop endpoint can stop a scheduled run. There is no durable
mid-scan cursor or ZFS-style pause/resume; subsequent runs scan afresh.

### Start, stop and status

Replace {job} with repair, gc or redistribution:

```http
POST /noa/v1/maintenance/{job}/start?fragment={fragment}&mode=strict
POST /noa/v1/maintenance/fragments/{fragment}/{job}/start?mode=strict
POST /noa/v1/maintenance/{job}/stop
GET /noa/v1/maintenance/{job}/status
POST /noa/v1/maintenance/objects/redistribution/start/{object_key}
```

Starts require `write`, `modify` and `delete`. Ordinary tokens can select only
their own fragment; omitted scope defaults to that fragment. Operator tokens
may select another fragment or omit scope for all. Explicit all-fragment
requests use `all_fragments=true`; Shell `--none` and Console global controls set
it, so insufficient authority is rejected rather than silently changing scope.
Restrictive/read-only/quiesced modes and all global stops require operator
authority because their gates/stop signals are server-wide. Authenticate with
`MASTER_KEY`, not a full-permission fragment secret, for those operations.

| Job | Query controls |
| --- | --- |
| Repair | mode=availability/checksum/strict/scrub, dry_run, fast_mode, read_only_fast, meta_concurrency, repair_concurrency, batch |
| GC | mode=availability/strict/scrub |
| Redistribution | mode=availability/strict/scrub, scope=all/index/blob, durability=auto/strict/batched, fast_mode, object_concurrency, status_batch |

Poll status until completion and inspect errors, not just admission. Legacy
index/blob scope names remain API parameters, not separately configured pool
roles. Healthy format-4 placements are not redistribution debt. This API is
not an automatic pool migration/recode command. Expansion requires no rebalance.
Maintenance may inspect/change immutable placements under its own safeguards;
ordinary rename/trash/recover does not.

Foreground upload, repair, GC and export share bounded disk IO admission.
Health exposes disk_io foreground/background lanes, waits, stalls, permits,
bytes, latency and admission failures. Saturation can reject requests instead
of allocating unbounded work. This scheduler is not a content/namespace cache.

## WebSocket events

Connect using wss://storage.example:50001/noa/v1/ws?token=<access-token>.
Treat the URL token as a secret. Events use this envelope:

```json
{
  "op": "EVENT",
  "d": {
    "timestamp_ms": 1778000000000,
    "type": "NAMESPACE_CHANGED",
    "data": {
      "fragment": "<fragment>",
      "operation": "rename",
      "kind": "File",
      "source_path": "docs/example.txt",
      "destination_path": "archive/example.txt",
      "affected_paths": 1,
      "affected_objects": 1
    }
  }
}
```

Common operations: upsert_file, rename, trash, recover, recover_all,
bulk_delete and bulk_recover. Apply deltas without reloading the entire tree
after each event. On reconnect, re-list to reconcile missed changes; events
are not a durable client transaction log. Inspect partial mutation responses
as well as notifications.

WebSocket upgrade requires read permission. The connection closes at token
expiry even when heartbeats remain active; reconnect with refreshed authority.

## Errors and retries

Most failures return status plus plain error text. Preserve the status and
message in diagnostics; do not assume every error body is JSON.

| Status | Meaning |
| --- | --- |
| 400 | Invalid request/path, rejected empty mkdir or cross-fragment rename |
| 401 / 403 | Invalid/missing authentication / insufficient permission |
| 404 | Missing object, fragment, version or completed/absent session |
| 409 | Destination/session conflict or already-running maintenance |
| 410 | Retired destructive route |
| 413 | Request/body exceeds a limit |
| 416 | Unsatisfiable range |
| 503 | Unsafe topology, unavailable accelerator or admission saturation |
| 507 | Kernel storage/quota exhaustion |

202 accepts asynchronous export work; poll its status. 207 batch upload contains
per-object outcomes. Prefix mutations may return successful HTTP status with
conflicted_paths/failed_paths. Retry only the unfinished work, with backoff for
saturation. Reconcile after connection loss; do not blindly repeat an upload
and assume it cannot create another immutable version.

## Accel integration

Accel is optional and rebuildable. Configure N1 with:

```dotenv
N1_ACCEL_SEARCH_URL=http://127.0.0.1:53001
N1_ACCEL_SEARCH_BACKEND_ID=n1-primary
```

Configure a matching Accel backend ID, N1 origin/trusted TLS and jwt_secret_env
referencing N1's signing secret, not a fragment secret. Keep Accel private.
Clients call N1's authenticated namespace/search; N1 forwards to this fixed
backend. Snapshot ingestion is projection building, never namespace authority.

Search supports q, scope, field, match, limit, cursor and refresh controls of
the configured Accel implementation. Refresh-based results can be stale; N1
LIST/lookup remains authoritative. Without Accel, only search returns 503.
Ordinary reads/writes/listing still work. Search alone does not enable byte caching.
To accelerate existing Console and API image/video/download URLs, opt in with:

```dotenv
N1_ACCEL_READ_URL=http://127.0.0.1:53001
N1_ACCEL_READ_BACKEND_ID=n1-primary
```

Restart N1 after changing these values. Configure the matching private Accel
backend with `cache_private_responses=true`. N1 still authorizes every incoming
read and resolves its namespace/version before proxying immutable bytes. The
internal read-only token is never returned to browsers; its six-hour lifetime
covers long streams. Cache-origin requests bypass only the proxy, not N1 auth.
Console requires no alternate URL or exposed Accel administrative credential.
Invalid configuration, unavailable Accel, rejected responses and pre-header
timeouts fall back to N1. A mid-stream failure terminates the response; clients
may resume with a fresh authorized request. Browser `private, no-store` policy
is preserved. Accel remains optional and is not a backup or namespace authority.

## Offline pool maintenance

Stop N1 and quiesce application writers before every command below. Use the
matching `litiaina-n1-shell` and `litiaina-n1` binaries in the same directory
(or use `pool --engine-binary PATH verify --config CONFIG`). These commands are local administrative
operations, not HTTP APIs or automatic enrollment exceptions.

```bash
litiaina-n1-shell pool verify --config /etc/litiaina/n1/n1_config.ini
litiaina-n1-shell pool verify --config /etc/litiaina/n1/n1_config.ini \
  --pool A --fragment <STABLE_FRAGMENT_ULID>
litiaina-n1-shell pool clone --config /etc/litiaina/n1/n1_config.ini \
  --pool A --member 0 --copy-root /mnt/replacement/n1
```

Offline `verify --pool A` leases/scans only that pool plus the control workspace;
unselected members are not inspected for content or required to be online.
Omit `--fragment` for complete pool verification before activation. A fragment
filter uses the stable ULID, not its public name/hash, and is not a whole-pool
activation certificate. Pool membership/catalog validation still applies.

Member numbers are zero-based logical shard slots. Existing path-bound v4
deployments must first start the updated engine at their original locations to
upgrade the small pool descriptors; committed NDX and payload are not rewritten.
Afterward `roots` lists candidate locations. Descriptors, not list order or
hardware serials, identify members. Keep pool ID/generation/K+P unchanged.
Clone requires an empty non-overlapping target; for hardware migration, choose
a different filesystem independent of every remaining pool member. It performs
a byte-exact copy plus two-way inventory/SHA-256 verification and leaves the source intact.
It does not recode EC or rewrite NCX/NDX/payload. Keep the engine stopped, mount
the verified copy at the same or a new root, update only the corresponding
candidate location in `roots`, run `pool verify`, then start
N1 and validate listing, version history, full hashes and new writes. Keep the
source until validation is complete. This also works for a healthy 1+0 member.
Once writes resume, the old member alone is a stale rollback image. Do not
remount it as current storage without verifying the resulting complete pool.

For a manual copy, run `pool prepare-copy` with the same selection/target first,
copy the entire member including hidden files and relative fragment symlinks,
then run `pool verify-copy`. Preserve the target's safety fence; copying tools
must not overwrite it. A partial or same-size corrupt copy fails verification.
Never copy a live member and treat it as a consistent snapshot.

To replace **one lost member**, prepare a blank independent filesystem at its
existing or a new location; update its candidate location in `roots`, and retain
the original control workspace and all other members. The missing slot is never
renumbered. `--member` selects the logical slot, not its discovery-list position:

```bash
litiaina-n1-shell pool replace --config /etc/litiaina/n1/n1_config.ini \
  --pool A --member 0 --authorize
```

All surviving members must verify. The command copies surviving NCX and metadata,
reconstructs only the selected slot's finalized and acknowledged durable-upload
pieces, verifies the complete result, and only then restores its enrolled root
identity. A lost 1+0 member cannot be reconstructed: restore a verified backup.
Multiple simultaneous member replacements are not supported by this command.

After an interrupted clone/replacement, repeat its exact command with `--resume`.
An unfinished target stays fenced; do not remove markers to force startup.
Conflicting or unexpected files fail closed instead of being silently overwritten.
The offline verifier caps one metadata file at 64 MiB and one durable upload's
receipt inventory at 131,072 records; exceeding these limits is an explicit
verification failure, never a health pass. Reports include elapsed time and peak
process RSS. Ordinary NCX mutations retain their existing lightweight semantics.

Root discovery performs bounded descriptor checks, not a full checksum scrub.
Accepted identity alone cannot prove freshness/completeness: finish copy/rebuild
verification before activation. Locations stay fixed while N1 is running;
relocation is offline. Wrong-pool/slot identities, duplicate members and pending
maintenance are refused. Missing members retain existing degraded read/write
rules; existing health checks and background integrity verification remain active.
New shard/session addresses are logical member-relative addresses. Existing
absolute addresses resolve through their original slot prefixes without changing
committed NDX/payload during relocation. Backup/restore below is also supported
and never requires payload EC recoding when logical pool geometry is unchanged.

## Backup and restore

`litiaina-n1-shell backup` creates encrypted recovery points, not
live replication or a writable standby. It delegates to the matching engine;
the legacy `litiaina-n1-backup` helper remains available for local jobs.
Both use the same implementation and repository format. Remote transfer uses
N1-owned HTTP(S) or native N1(S); no SSH client/daemon is used.
Offline capture requires all configured roots, initialized leases and a stopped engine
for the entire scan; quiesce application writers first. Source leases reject an
active updated engine, but cannot protect against unrelated filesystem writes.
Do not remount/edit roots or overlap maintenance during capture.

Offline capture includes pool/fragment identities and credentials, live/trash NCX,
NDX/payload, durable multipart progress, directories/symlinks and timestamps.
It excludes runtime leases/physical-root identity, control workspace, config/env,
TLS/service files, refresh-token sessions, exports, Accel and application DBs.
Back up these separately. Standard unfinished uploads do not survive a stop.

AES-256-GCM/HKDF-SHA256 protects snapshot contents. Repository block names expose
equality/size, not plaintext. Protect repository/key permissions, and store an
offsite key copy: losing the key loses recovery. Restore does not recreate Unix
owners, ACLs or xattrs. Key rotation is not implemented.

### Capture and verify

```bash
litiaina-n1-shell backup keygen --key-file /secure/n1-backup.key
litiaina-n1-shell backup init \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key
sudo systemctl stop litiaina-n1.service
litiaina-n1-shell backup create \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --config /opt/litiaina/n1/n1_config.ini --workers 4
sudo systemctl start litiaina-n1.service
litiaina-n1-shell backup list \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key
litiaina-n1-shell backup verify \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID
```

Manual create does not stop/restart N1; restart it even when capture fails.
Use an account with source/repository access. Incremental capture reuses blocks
but still reads/hashes the tree; downtime scales with dataset and storage.
Workers are bounded at 1–32, default four, with streamed 4 MiB blocks.

### Online committed-state capture

Online capture is coordinated by the **running N1 engine**, not an unguarded
filesystem scan from Shell. Configure private repository aliases, a dedicated
operator token and `engine_config` using
[backup-operator.json.example](deploy/backup-operator.json.example), then set
`N1_BACKUP_OPERATOR_CONFIG=/etc/litiaina/n1/backup-operator.json` in N1's service
environment and restart once to enable it. Initialize the repository/key first.
N1 uses its existing HTTPS/native listeners; no extra engine listener is needed.

```bash
litiaina-n1-shell backup create-online \
  --source-url https://storage-host:21001 --repository primary \
  --token-file /secure/operator.token --ca-file /secure/storage-ca.pem
```

Native `n1://` control is also supported with explicit `--allow-plaintext` on a
trusted network. TLS verification is not disabled. The engine endpoint accepts
`create-online`, `capabilities` and `operations` only, using the dedicated token;
fragment/master JWTs alone cannot run backup jobs. Paths come from private
configuration, never API callers.

The engine briefly queues mutations while it pins the exact committed namespace
and version set. `capture_boundary_seconds` defaults to 30 (1–300), bounding
admission and traversal; timeout abandons the view rather than publishing a
partial point. Payload verification, hashing, encryption and copying occur after
mutation admission reopens. The completed result reports `boundary_ms` and
`capture_mode: "online_committed"`. Hard links retain selected files across later
rename, deletion and GC; no payload copy happens under the capture boundary.
The deadline is checked between filesystem calls; it cannot forcibly cancel a
kernel-blocked device call. Complete redistribution and manual filesystem/mount
maintenance before capture.

Scope: committed objects/all committed versions, trash, pool/fragment identities
and credentials. **No unfinished upload sessions, receipts or temporary pieces.**
Full durable-upload continuation is the separate offline mode. Application DBs,
configuration/env, keys, TLS and runtime workspaces still need separate protection.
A consistent N1 point is not an application database transaction snapshot.
Backup requires a complete healthy pool; missing/corrupt files fail closed.
Keep byte/inode headroom for scratch links and repository blocks. The view is
not an independent backup until the encrypted repository snapshot is complete.

Shell's separate operator config can set `online_source` to a `Destination`
(`url`, source repository alias, private `token_file`, optional `ca_file`,
`allow_plaintext`). It forwards capture to N1, waits for real completion, and
continues to provide repository verification/replication/restore while N1 runs.
An automated job uses the same optional `online_source` field: no engine stop,
restart intent or service-state transition is made in online mode. Omit it for
full offline capture. Retention follows verification and required replication.

For online mode, place this object in the Shell operator and job configuration
(replace the source alias/CA/token with your configured values):

```json
"online_source": {
  "url": "https://127.0.0.1:21001",
  "repository": "primary",
  "token_file": "/etc/litiaina/n1/operator.token",
  "ca_file": "/etc/litiaina/n1/source-ca.pem",
  "allow_plaintext": false
}
```

The running engine's preset needs the repository alias and dedicated token;
it captures locally and does not forward `create-online` to itself. The standalone
Shell endpoint uses `online_source` to reach that engine. Keep source and Shell
repository configuration aligned. Offline supervision requires service-management
permissions; online-only jobs do not need permission to stop/start N1.

### Replicate recovery points

Initialize the destination with a protected copy of the same key. Put it on an
independent storage/host/site failure domain. Replication can run after N1
restarts because it reads a committed snapshot, not live engine files.

```bash
litiaina-n1-shell backup replicate \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID \
  --target-repository /independent-backup/n1-replica \
  --target-key-file /secure/n1-backup.key

litiaina-n1-shell backup replicate \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID --target-url https://backup-host:50902 \
  --target-repository replica --token-file /secure/replica-operator.token \
  --ca-file /secure/backup-ca.pem
```

Run the matching Engine/Shell pair's own operator endpoint on the destination
(see below). The remote repository is a configured **alias**, not a caller-supplied
path. The backup encryption key stays on each host; only the dedicated operator
token authenticates the connection. HTTPS validates the hostname/certificate;
use a trusted CA file for private certificates. Native `n1s://backup-host:50903`
uses the same operations and built-in TLS (requires `--ca-file`). Explicit trusted
network testing can use `http://` or `n1://` with `--allow-plaintext`; these expose
credentials/data in transit and must not be used on an untrusted network.
HTTP transfer is a bounded binary WebSocket stream upgraded on the N1-owned
endpoint. Native transfer starts with an N1 MessagePack operation frame. Both
carry the existing snapshot/block verification protocol, not live root copying.
Interrupted transfer is retried with the same snapshot; verified blocks are
reused. A partial transfer is not a committed recovery point. Replication does
not propagate source deletion or retention; choose destination retention separately.

For planned server migration, pre-copy a verified online recovery point to the
new server over the same owned protocol. At cutover, pause application writes,
capture and replicate the final point, verify it, then restore into empty stopped
target roots with matching pool geometry/member slots. Keep the old authority
stopped before enabling writes on the new server. Online pre-copy shortens the
handoff; it is not live mirroring, automatic failover or a zero-downtime restore.

### Shell-owned operator API and Console

The endpoint runs independently of the storage engine so offline capture/restore
does not disconnect Console. It uses the existing backup implementation; it
does not create another namespace, live replica, or storage coordinator.

```bash
litiaina-n1-shell backup operator-keygen --token-file /secure/operator.token
# Adapt deploy/backup-operator.json.example; protect it with chmod 600.
litiaina-n1-shell backup serve --config /etc/litiaina/n1/backup-operator.json
```

Configure fixed repository aliases/key files, source engine config, optional
replica aliases, empty restore-target configs and supervised-job configs. Only
the configured paths/operations are accepted; no terminal/remote commands,
formatting or arbitrary filesystem paths are accepted from clients. Keep the
operator token separate from the backup encryption key and fragment credentials.
TLS is required unless plaintext is explicitly enabled. The same certificate
secures HTTP and native listeners. Exact Console origins must be listed in
`allowed_origins`; tokens never belong in URLs or browser storage.

`POST /noa/v1/maintenance/backups/operation` on this endpoint accepts JSON
`operation` plus configured `repository`, `snapshot`, `target` or `job` aliases.
Operations: `capabilities`, `list`, `operations`, `init`, `create`, `create-online`, `verify`,
`replicate`, `restore`, `forget`, `gc`, `prune`, `run-job`, `job-status`.
Mutating/offline operations require `confirm=true`. Restore resumes only with
`resume=true` and matching existing fences. Long jobs return `operation_id`;
query `operations` for actual completion/failure. Acceptance is not success.
Native operation `backup.operation` uses the same payload plus the operator
`token`; `backup.receive` opens the snapshot transfer stream after authentication.
API job history is bounded and process-local; after an endpoint restart inspect
repository verification, restore fences and durable supervised-job status rather
than assuming an interrupted request succeeded. Repository leases/pins/fences,
not this UI history, enforce recovery safety.

Console → Maintenance → Backups & snapshot replication connects to this endpoint
and executes capture/verify/replicate/restore and retention operations. `create`
remains full offline capture; `create-online` forwards to the configured running
N1 source and excludes unfinished uploads. Target restore remains offline.
A configured `run-job` follows its online/offline source setting, then verifies
and replicates; it does not quiesce application databases. Scheduling remains the
optional configured backup timer, not a browser timer. The supplied optional
`litiaina-n1-operator.service` starts the Shell-owned endpoint; it is not an
external software dependency. An ordinary static Console cannot host operations
on another machine without that machine's N1-owned endpoint.
Install its matching `litiaina-n1-operator-recover.service` too: ExecStopPost and
independent failure/boot recovery restore recorded engine restart intent after
an interrupted supervised job. Endpoint startup also checks that intent. When
running without a supervisor, after the killed process and children have exited,
run `backup recover-operator --config backup-operator.json` before further work.
Recovery does not label an interrupted backup successful. Do not remove its
job state or restore fences to force readiness. Disabled jobs cannot be launched
through the API; configure and approve their service/settings first.
Repository admission failure is reported before transfer acceptance (HTTP 409
or native `repository_unavailable`); wait/retry without bypassing the lease.

### Portable restore

Keep the destination engine stopped and use fresh empty roots, matching pool
IDs/generations/geometry and corresponding independent member devices. Root
remapping matches pool ID, generation, K+P and zero-based member slot, independent
of mount-name sorting; review the source/target mapping. This is not format migration or EC
recoding to a different count/layout. Use a fresh fs_storage_path; do not copy
the source physical identity registry onto replacement devices.

```bash
litiaina-n1-shell backup restore \
  --repository /backup/n1-replica --key-file /secure/n1-backup.key \
  --snapshot SNAPSHOT_ID --target-config /etc/litiaina/n1-restored.ini
```

Restore authenticates blocks, writes startup fences, remaps paths and synchronizes
files/directories before removing fences. The engine refuses incomplete restore.
After interruption, repeat the matching command with --resume. Never remove
fences to force startup; unexpected destination files fail rather than being
deleted. Configure secrets, TLS and ownership separately before starting.

Validate fragment auth, namespace/history/trash, full download hashes, health,
new writes and any required durable continuation. Restore a matching application
DB recovery point before resuming writes. Restore does not reset expiration ages;
review cleanup policy before first startup of an old snapshot. Keep the backup
repository intact while validating recovery.

### Retention

```bash
litiaina-n1-shell backup forget \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key \
  --snapshot OLD_SNAPSHOT_ID
litiaina-n1-shell backup gc \
  --repository /backup/n1-primary --key-file /secure/n1-backup.key
```

forget refuses the last completed point. Repository GC protects authenticated
completed snapshots and incomplete-job pins. Add --discard-incomplete only when
deliberately abandoning failed-job pins with no active job. Repository leases
prevent concurrent destructive cleanup. This is not WORM storage; OS-level
administrators can still erase it.

### Optional supervised backup jobs

[deploy/backup-job.json.example](deploy/backup-job.json.example) is disabled by
default. Initialize repositories/key first, configure exact paths/service,
private state outside roots/repositories, timeouts and independent retention,
then set enabled=true for an approved maintenance window. Protect the config,
state, binaries and destination token/CA settings from untrusted writes. The source engine must
have initialized its root leases before capture.

Install the supplied units in /etc/systemd/system and the matching Engine/Shell
pair at their configured executable paths. Remote replicas set `url`,
`repository` (destination alias), `token_file` and optional `ca_file`; local
replicas retain repository/key paths. Old SSH fields/CLI flags are rejected.

```bash
sudo install -m 644 deploy/systemd/litiaina-n1-backup.service \
  deploy/systemd/litiaina-n1-backup.timer \
  deploy/systemd/litiaina-n1-backup-recover.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start litiaina-n1-backup.service
sudo /opt/litiaina/n1/litiaina-n1-shell backup job-status \
  --config /etc/litiaina/n1/backup-job.json
# Enable only after a successful supervised acceptance/recovery drill:
sudo systemctl enable --now litiaina-n1-backup.timer
```

In offline mode the supervisor preserves the original N1 service state, records restart intent
before stopping, restarts before verify/replication, and applies retention only
after required success. Pending replication retries reuse the snapshot without
another capture. ExecStopPost plus separate-cgroup OnFailure/boot recovery handle
interrupted restart intent. This supervises N1 only, not application DB quiescing.

The supplied timer is Sunday 02:00 host time, Persistent=false: missed windows
are skipped. The timer also enables the recovery unit. job-status emits JSON
and exits nonzero for enabled jobs that are failed, interrupted, overdue or not
yet complete; connect it to monitoring. Building does not enable scheduling.

Disable the timer and enabled setting for future runs, but retain recovery
state/unit while restart intent is pending. To interrupt, stop the backup
service and verify N1 recovered. Unsupervised run-job cannot clean up its own
SIGKILL. After its job and children exit, manual recovery is:

```bash
litiaina-n1-shell backup recover-job --state-dir /var/lib/litiaina/n1-backup
```

This restores service intention, not a successful backup. Live replication,
standby promotion, automatic key rotation and zero-RPO recovery are not features
of this offline helper. See [TESTING.md](TESTING.md) for drilled cases and limits.

## Operator setup

For normal startup/systemd installation see [README.md](README.md#build-and-start).
The browser-terminal shell adds guarded provisioning, not filesystem semantics
or a required always-running management service. On Debian-family systems it
needs xfsprogs, util-linux and fdisk; create the intended N1 service account and
prepare a trusted certificate/key first.

```bash
cargo build --release -p n1 --bins -p n1_shell
sudo litiaina-n1-shell serve --bind 127.0.0.1:50901 \
  --install-dir /opt/litiaina/n1 \
  --engine-binary /opt/litiaina/n1/litiaina-n1 \
  --mount-root /mnt --run-user n1 --run-group n1
```

Use the temporary setup token shown by serve. Keep this privileged setup
listener on loopback or a protected SSH tunnel; it is not public TLS. Shut it
down after setup. The N1 systemd service runs independently.

### Browser-terminal sequence

```text
prerequisites
lsblk
disk inspect <device>
n1 init
config pool A 3 1 /mnt/NDISK001,/mnt/NDISK002,/mnt/NDISK003,/mnt/NDISK004
config endpoint 127.0.0.1 50001 50002
config tls /opt/litiaina/n1/cert.pem /opt/litiaina/n1/key.pem
config show
config validate
n1 start
n1 status
n1 logs
n1 enable
```

n1 init creates config/secrets and installs a disabled service; it does not
make unconfigured storage safe. Secrets output is private. Ensure all four
mounted roots are provisioned before config pool/start. Configure pools while
the engine is stopped; enrolled topology is immutable.

Only when explicitly provisioning empty devices, disk format <device> NDISK001
performs destructive GPT/XFS formatting. It requires current label/serial
confirmation and rejects protected root/boot/swap, mounted and read-only devices.
Review disk inspect output; never guess a device name. Formatting creates a
persistent UUID mount and service ownership; it is not secure erasure. Repeat
with distinct devices/labels only as required. Existing storage should be
mounted/validated, not formatted again after a startup error.

Use disk mount <label> for prepared mounts, and n1 stop/restart/disable for
service control. Use help for command details. Missing member/identity errors
need inspection and a tested replacement/restore procedure, not config editing
to bypass safety. Pool expansion adds a separate complete pool.
