# N1 Documentation

N1 1.0.0 is Litiaina's single-node object storage system. It exposes object-storage semantics through a POSIX-like namespace, immutable object versions, shard/parity blob storage, and a native `n1://` protocol for internal tools. It is designed to exploit one Linux server: multiple disks, bounded memory, durable upload resumability, repair, redistribution, garbage collection, trash/recover, archive download, and realtime namespace events.

N1 is not a filesystem. It is an object storage system with its own namespace rules: folders are namespace directories, files are stable object identities, and each committed write creates an immutable object version. Large data movement stays in the blob plane; namespace operations stay lightweight.

For implementation details, see [ARCHITECTURE.md](ARCHITECTURE.md). For release validation notes, see [VERIFICATION.md](VERIFICATION.md).

## Core Concepts

- **Fragment:** A quota-bound virtual storage partition. Clients mount/authenticate a fragment before using the API. The public fragment hash is a mutable namespace alias; N1 stores data under an immutable `@f_<stable_fragment_id>` physical fragment root.
- **Namespace:** The user-facing tree of folders and files. This is what `/n1/posix/*` and the UI show.
- **Object:** The stable identity behind a file. Renaming a file changes the namespace name, not the object identity.
- **Version:** One committed immutable object version. Uploading to an existing file path creates a new version under the same object identity.
- **Trash:** Soft delete moves namespace entries into `.trash`. Trash listing reads `.trash`. Recover moves paths back to live namespace. Hard delete and empty trash remove `.trash` paths first; GC later reclaims blob bytes that no remaining namespace metadata references.
- **Repair:** Rebuilds missing or corrupt shard/parity/index replicas when enough data remains.
- **Redistribution:** Rebalances index/blob placement after disk topology changes.

## Installation

Build N1:

```bash
cargo build -p n1 --release
```

Check the exact N1 binary version without starting the server:

```bash
./target/release/litiaina-n1 --version
```

Run directly for development:

```bash
sudo ./target/release/litiaina-n1
```

## Runtime Files

N1 does not keep machine runtime files in the repository. When N1 starts inside
a fresh deployment directory, it creates the local files it needs for that
machine:

- `n1.env` - local secret material.
- `n1_config.ini` - local machine configuration: ports, disk roots, fragment
  quota backend, erasure layout, and deployment settings.

These files are intentionally ignored by git. Start N1 once in the deployment
directory to generate them, stop the process, then fill the real machine values
before installing the service.

Production disks should already be mounted before N1 starts. If XFS quota is
enabled, mount the disks with project quota support.

## Production Systemd Install

Build the release binary, place it in the deployment directory, and keep
`n1.env` plus `n1_config.ini` in that same directory. N1 reads those files from
its working directory by design.

Example deployment directory:

```text
/home/administrator/SERVERS/N1-SERVER/
  litiaina-n1
  n1.env
  n1_config.ini
  cert.pem
  key.pem
```

Install N1 as a systemd service:

```bash
sudo scripts/install-n1-systemd.sh \
  --install-dir /home/administrator/SERVERS/N1-SERVER \
  --enable \
  --start
```

The production default runs as root because strict XFS project quota needs
filesystem quota privileges. Advanced non-root deployments can use
`--privilege cap-sys-admin --user <user> --group <group>`, but root mode is the
simple operational path.

Check the service:

```bash
sudo systemctl status litiaina-n1.service --no-pager --full
sudo journalctl -u litiaina-n1.service -f
```

Installer proof commands:

```bash
bash -n scripts/install-n1-systemd.sh
sudo scripts/install-n1-systemd.sh \
  --install-dir /home/administrator/SERVERS/N1-SERVER \
  --dry-run \
  --skip-quota-preflight
```

`--dry-run` renders the exact unit without installing it. A real install also
preflights XFS project quota unless `fragment_quota_backend=disabled` or
`--skip-quota-preflight` is supplied.

## Configuration

N1 reads `n1_config.ini` and `n1.env` from the server working directory.

Minimal two-disk configuration where both disks house index and blob data:

```ini
[server]
host=127.0.0.1
https_port=50001
native_port=50002
cert_path=cert.pem
key_path=key.pem

[disk_config]
index_store_disks=/mnt/disk-00,/mnt/disk-01
blob_store_disks=/mnt/disk-00,/mnt/disk-01
fs_storage_path=/home/administrator/Server Deploy Test/fs-disk

[quota_config]
fragment_quota_backend=xfs_project

[upload_config]
data_shards=1
parity_shards=1
max_part_size=52428800
stripe_block_size=262144
max_concurrent_io=auto
upload_memory_budget_bytes=auto
adaptive_concurrency_enabled=true
upload_io_gate_enabled=true
```

`n1.env` must provide the secrets used by N1:

```bash
JWT_SECRET=change-this
MASTER_KEY=change-this-master-key
SIGNED_READ_URL_SECRET=change-this-signed-url-secret
```

### Disk And Quota Notes

N1 recommends XFS with project quota enabled for production fragment quotas:

```bash
sudo mount -o prjquota /dev/<device> /mnt/disk-00
```

Fragment quota is a storage partition limit. Creating a fragment requires `quota_bytes`. N1 refuses fragment creation when the requested quota exceeds available allocatable capacity.

### Fragment Identity Layout

N1 1.0.0 separates the mutable fragment name/hash from the physical fragment root:

```text
<root>/
  .fragment/
    <fragment_hash> -> ../@f_<stable_fragment_id>

  @f_<stable_fragment_id>/
    .fragment.ndx
    .objects/
    .uploads/
    .trash/
    .blobs/
    <namespace directories>
```

The `.fragment/<fragment_hash>` symlink is the mount/auth namespace. The `@f_<stable_fragment_id>` directory is immutable. Renaming a fragment changes the alias and `.fragment.ndx`; it does not move blob payloads or rewrite immutable object `.ndx` records. This keeps absolute shard/parity paths valid after fragment rename.

## Quick Start

Set a base URL:

```bash
BASE='https://127.0.0.1:50001'
MASTER='<MASTER_KEY>'
```

Check server health:

```bash
curl -k "$BASE/n1/ping"
```

Create a fragment:

```bash
curl -k "$BASE/n1/fragments" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "mainframe",
    "secret": "'"$MASTER"'",
    "key": "fragment-local-secret",
    "quota_bytes": 322122547200
  }'
```

Authenticate the fragment:

```bash
curl -k "$BASE/n1/auth/fragment" \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment": "<FRAGMENT_HASH>",
    "secret": "fragment-local-secret",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }'
```

Use the returned token:

```bash
TOKEN='<ACCESS_TOKEN>'
AUTH=(-H "Authorization: Bearer $TOKEN")
```

Create a folder:

```bash
curl -k "$BASE/n1/posix/mkdir" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"path":"videos"}'
```

Upload a small file with one-shot upload:

```bash
curl -k "$BASE/n1/upload/one-shot/videos/clip.mp4" "${AUTH[@]}" \
  -H 'Content-Type: video/mp4' \
  --data-binary @clip.mp4
```

List files:

```bash
curl -k "$BASE/n1/posix/list?path=videos&limit=200" "${AUTH[@]}"
```

Download a file:

```bash
curl -k "$BASE/n1/objects/download?object_key=videos/clip.mp4" \
  "${AUTH[@]}" \
  -o clip.mp4
```

Soft delete and recover:

```bash
curl -k "$BASE/n1/posix/trash" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"path":"videos/clip.mp4"}'

curl -k "$BASE/n1/posix/recover" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"path":"videos/clip.mp4"}'
```

## Authentication

Protected routes require:

```http
Authorization: Bearer <access_token>
```

Fragment tokens carry independent permissions:

- `read`
- `write`
- `modify`
- `delete`

## API Conventions

- `object_key`, `path`, `source_path`, and `destination_path` are namespace paths such as `videos/clip.mp4`. URL-encode spaces and special characters when they appear in a path or query string.
- Request bodies are JSON unless the route says the body is raw bytes.
- HTTPS is the universal API face for consumers and broad compatibility. `n1://` is the native N1 protocol face for N1 Console, N1 CLI, NS, N2, and heavy internal clients.
- Large files should use multipart upload. One-shot upload is intended for smaller objects.
- `version_id` is the persistent upload/object-version id. Multipart part upload and status use `version_id`; finalize also requires the `session_id` returned by init.
- Empty trash and hard delete remove namespace references first. Blob bytes are reclaimed after GC proves no live or trash metadata references them.
- Object status/repair is object-scoped. A repair or scrub for one object does not lock the whole fragment.
- When an operation returns `503 Service Unavailable`, retry later. This can mean maintenance is running, object maintenance gates are saturated, or quota/disk provisioning prevents safe progress.

## Native N1 Protocol

N1 1.0.0 includes the native N1 protocol:

```text
n1://127.0.0.1:50002
protocol: n1/0.4
```

HTTPS remains the universal consumer API. The native protocol is the low-level N1 execution face for N1 Console, N1 CLI, NS, N2, and trusted services that need lower latency than HTTPS for namespace, control-plane, and data-plane calls.

The native protocol does not create another storage engine, another namespace, another metadata authority, or another code path for object truth. Native requests use typed `n1://` operations that call N1 core/service functions directly. They do not emulate HTTP requests and they do not dispatch through the Axum route graph.

Rust clients should use the workspace `n1_native_client` crate instead of reimplementing frames. The crate owns native URL parsing, MessagePack frame IO, fragment authentication, typed operation calls, raw upload bodies, and raw streaming response reads.

`n1/0.4` supports the native data plane:

- `upload.one_shot`
- `multipart.init`
- `multipart.put_part`
- `multipart.finalize`
- `multipart.cancel`
- `uploads.list`
- `uploads.status`
- `object.meta`
- `object.versions`
- `object.status`
- `object.repair`
- `object.rename`
- `object.delete`
- `object.recover`
- `object.purge`
- `object.version.recover`
- `object.bulk_delete`
- `object.bulk_recover`
- `object.bulk_purge`
- `object.download_chunk`
- `object.download`
- `object.download_stream`
- `object.view_stream`

Upload bodies are sent as raw bytes after a small MessagePack control frame. The server streams those bytes through a bounded internal pipe into the N1 upload core, so large native one-shot uploads and multipart parts do not become whole in-memory protocol frames. Streaming download/view responses send a small MessagePack response frame with `body_len`, then stream raw object bytes directly on the same socket.

### Configuration

```ini
[server]
host=127.0.0.1
https_port=50001
native_port=50002
```

The native listener is part of N1, not an optional sidecar. Configure `native_port` to choose where the low-level protocol binds. N1 fails startup if this required listener cannot bind. Bind `host` carefully. `n1://` should be treated as an internal/trusted-network protocol unless a deployment adds an external secure tunnel or equivalent network protection.

### Frame Format

Every frame uses a fixed length prefix followed by a MessagePack payload:

```text
u32_be frame_length
MessagePack payload
```

The maximum frame size is currently `8 MiB`. This keeps native control frames bounded and prevents one client from forcing unbounded server buffers. Large object transfer should use streaming/range/multipart semantics instead of attempting to place huge files in one native frame.

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

### Core Operations

Native operations are typed N1 operations, not HTTP bridge calls. Native has no HTTP bridge operation; clients should use the operation names below.

Namespace/POSIX:

- `namespace.list`
- `namespace.stat`
- `namespace.mkdir`
- `namespace.rename`
- `namespace.trash`
- `namespace.recover`
- `namespace.purge`

Object:

- `object.list`
- `object.status`
- `object.repair`
- `object.delete`
- `object.recover`
- `object.purge`

Trash:

- `trash.list`
- `trash.empty`
- `trash.recover_all`

Multipart:

- `uploads.list`
- `uploads.status`
- `multipart.status`
- `multipart.list`

Maintenance:

- `maintenance.health`
- `maintenance.repair.start`
- `maintenance.repair.status`
- `maintenance.repair.stop`
- `maintenance.gc.start`
- `maintenance.gc.status`
- `maintenance.gc.stop`
- `maintenance.redistribution.start`
- `maintenance.redistribution.status`
- `maintenance.redistribution.stop`

Fragment:

- `fragment.create`
- `fragment.list`
- `fragment.hash`
- `fragment.hash.public`
- `fragment.quota`
- `fragment.resize_quota`

Typed stat example:

```json
{
  "id": "stat-1",
  "op": "namespace.stat",
  "payload": "{\"path\":\"videos/clip.mp4\"}",
  "body": null
}
```

Typed rename example:

```json
{
  "id": "rename-1",
  "op": "namespace.rename",
  "payload": "{\"source_path\":\"videos/clip.mp4\",\"destination_path\":\"videos/final.mp4\"}",
  "body": null
}
```

### Performance Profile

Native is designed to remove HTTP/TLS overhead and JSON response duplication for trusted internal clients. N1 1.0.0 operation responses send the result once as MessagePack body bytes instead of JSON body plus a second JSON copy in the response envelope. Upload operations send payload bytes outside the MessagePack envelope as raw native body bytes, and streaming download/view responses send raw object bytes after the control response frame.

Current release-build loopback proof on 1,000 measured iterations showed:

- HTTPS `posix stat`: about `3.4k ops/sec`, p50 about `262 us`.
- Native `namespace.stat`: about `11.8k ops/sec`, p50 about `76 us`.
- HTTPS `posix list`: about `576 ops/sec`, p50 about `1387 us`.
- Native `namespace.list`: about `804 ops/sec`, p50 about `1189 us`.

Listing is less transport-bound because namespace scanning and response body size dominate, but the native MessagePack body path avoids JSON duplication and now stays ahead of HTTPS on the POSIX list proof.

### Tooling Policy

N1-owned tools should use `n1://` when they are running on trusted infrastructure. HTTPS stays available for browsers, external consumers, curl workflows, and compatibility. `n1_cli` uses native transport by default and only uses HTTPS when the operator explicitly passes `--url <https-url>`.

`n1_cli` uses the shared `n1_native_client` crate. It does not keep a private native frame/auth implementation.

For normal maintenance, use direct commands. The CLI accepts either a fragment name or a fragment hash. `--none` means whole-N1 scope:

```bash
cargo run -p n1_cli -- --secret <secret> --fragment <auth-fragment> repair --none --strict --fast-mode
cargo run -p n1_cli -- --secret <secret> --fragment <fragment-name-or-hash> repair --dry-run --availability
cargo run -p n1_cli -- --secret <secret> --fragment <auth-fragment> gc --none --scrub --fast-mode
cargo run -p n1_cli -- --secret <secret> --fragment <auth-fragment> redistribute --none --strict --durability strict
```

Status, stop, and watch are flags, not nested subcommands:

```bash
cargo run -p n1_cli -- --secret <secret> --fragment <auth-fragment> repair --status
cargo run -p n1_cli -- --secret <secret> --fragment <auth-fragment> gc --watch
cargo run -p n1_cli -- --secret <secret> --fragment <auth-fragment> redistribute --stop
```

`n1_cli` is stateful when opened as a console:

```bash
cargo run -p n1_cli -- --secret <secret> --fragment <fragment> console
```

The console keeps one native session open and supports read/maintenance operations:

- `health`
- `quota`
- `fragments`
- `list [path]`
- `stat <path>`
- `uploads`
- `upload <version_id>`
- `gc status|start`
- `repair status|start`
- `redistribute status|start`
- `object status <object_key>`
- `object repair <object_key>`
- `session`

The CLI intentionally keeps destructive namespace mutations out of the operator console. Rename, delete, update, upload, and application CRUD still belong to the HTTP or native API surfaces used by clients that explicitly own those workflows.

### Stability Boundary

Native `n1://` does not bypass N1 validation, authorization, repair, GC, redistribution, upload accounting, or namespace rules. It reaches N1 core/service functions directly instead of masking HTTP. Its stability risks are protocol risks: framing, session handling, socket behavior, and client implementation. The object storage system behavior stays unchanged.

For LTS release proof, native should remain in the Rust release proof matrix for:

- auth/session
- list/stat
- CRUD namespace operations
- trash/recover/purge
- object status/repair
- maintenance status/start
- multipart control paths
- benchmark regression detection

### `POST /n1/auth/fragment`

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
  "refresh_expires_in": 2592000
}
```

### `POST /n1/auth/refresh`

Body:

```json
{ "refresh_token": "<refresh-token>" }
```

### `POST /n1/auth/logout`

Body:

```json
{ "refresh_token": "<refresh-token>" }
```

## Fragment API

### `POST /n1/fragments`

Creates a quota-bound fragment. This route uses the master secret and does not use a fragment token.

Body:

```json
{
  "name": "mainframe",
  "secret": "<MASTER_KEY>",
  "key": "fragment-local-secret",
  "quota_bytes": 322122547200
}
```

Response:

```json
{
  "fragment_hash": "<fragment_hash>",
  "name": "mainframe"
}
```

### `GET /n1/fragments`

Lists fragments visible to the authenticated caller.

### `GET /n1/fragments/check`

Checks whether a fragment hash exists.

Body:

```json
{ "fragment": "<fragment_hash>" }
```

### `GET /n1/fragments/hash`

Returns the fragment hash for a fragment name when that fragment exists.

Body:

```json
{ "fragment": "mainframe" }
```

### `PATCH /n1/fragments/{fragment}`

Updates fragment name or fragment key.

Body:

```json
{
  "new_name": "mainframe-renamed",
  "new_key": "new-fragment-secret",
  "secret": "<MASTER_KEY>"
}
```

### `DELETE /n1/fragments/{fragment}`

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

### `GET /n1/capacity/usage`

Returns system-level configured storage capacity and availability.

### `GET /n1/capacity/quota`

Returns quota usage for the authenticated fragment:

```json
{
  "fragment": "<fragment_hash>",
  "quota_bytes": 322122547200,
  "used_bytes": 104857600,
  "available_bytes": 322017689600,
  "usage_percent": 0.0325,
  "quota_enforced": true,
  "quota_backend": "xfs_project"
}
```

### `PATCH /n1/fragments/{fragment}/quota`

Admin-driven quota resize.

Body:

```json
{
  "quota_bytes": 536870912000,
  "secret": "<MASTER_KEY>"
}
```

### `POST /n1/maintenance/fragment/init`

Initializes the storage structure for the authenticated fragment. This is normally handled during fragment creation, but the route is useful after manual disk preparation.

### `POST /n1/maintenance/fragment/cleanup`

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

### `GET /n1/posix/list`

Query parameters:

- `path`: directory path, default `""`
- `limit`: page size, default `200`, max `1000`
- `continuation_token`: optional cursor
- `trash`: `true` to list trash

Example:

```bash
curl -k "$BASE/n1/posix/list?path=videos&limit=200" "${AUTH[@]}"
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
      "size_bytes": 157230177,
      "modified_at_unix": 1778045325,
      "version_id": "01KQXW54GPFX8STY6NGC8J11PA"
    }
  ],
  "continuation_token": null
}
```

### `GET /n1/posix/stat`

Query parameters:

- `path`
- `trash=true` for trash lookup

### `POST /n1/posix/mkdir`

Body:

```json
{ "path": "videos/2026" }
```

### `POST /n1/posix/rename`

Also available as `POST /n1/posix/move`.

Body:

```json
{
  "source_path": "videos/clip.mp4",
  "destination_path": "archive/clip.mp4"
}
```

### `POST /n1/posix/trash`

Soft deletes a file or folder.

Body:

```json
{ "path": "archive/clip.mp4" }
```

### `POST /n1/posix/recover`

Recovers a trash path by moving it from `.trash` back to the live namespace. Clients should send the `path` returned by trash listing.

Body:

```json
{ "path": "archive/clip.mp4" }
```

### `POST /n1/posix/purge`

Hard deletes a trash path by removing it from `.trash`. GC reclaims blob bytes later after no live or trash namespace path references the object metadata.

Body:

```json
{ "path": "archive/clip.mp4" }
```

## Object API

These routes accept namespace paths. N1 resolves them to stable object identities internally.

### `GET /n1/objects`

Lists live objects. Query parameters:

- `prefix`
- `limit`
- `continuation_token`
- `delimiter`

Example:

```bash
curl -k "$BASE/n1/objects?prefix=videos/&limit=200" "${AUTH[@]}"
```

### `GET /n1/trash`

Lists trash entries. Supports the same paging style as object listing.

### `GET /n1/objects/meta/{object_key}`

Returns public object metadata for the latest version.

### `GET /n1/objects/versions/{object_key}`

Returns committed versions for a file path, newest first by version ULID. Internal shard layout is stripped from the public response.

### `GET /n1/objects/get/{object_key}`

Returns the object payload response.

### `GET /n1/objects/stream/{object_key}`

Streams an object. Supports HTTP `Range`.

### `GET /n1/objects/download`

Query parameters:

- `object_key`
- `version_id` optional
- `filename` optional

Example:

```bash
curl -k "$BASE/n1/objects/download?object_key=videos/clip.mp4" \
  "${AUTH[@]}" \
  -o clip.mp4
```

### `GET /n1/objects/download/chunk`

Query parameters:

- `object_key`
- `offset`
- `chunk_size`

Server-side limits:

- `chunk_size` must be greater than `0`.
- `chunk_size` must be `<= 8388608` bytes (`8 MiB`).
- Use `/n1/objects/download` or `/n1/objects/stream/{object_key}` for normal large downloads. The chunk route exists for bounded browser save flows and must not be used with huge per-request buffers.

Responses:

- `200 OK`: body contains the requested bytes.
- `204 No Content`: `offset` is at or beyond object size.
- `400 Bad Request`: invalid or overflowing range.
- `413 Payload Too Large`: requested chunk exceeds the server maximum.

### `POST /n1/objects/signed-url`

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

### `GET /n1/objects/signed-read`

Public signed read endpoint. The signed URL generator returns the full URL.

### `DELETE /n1/objects/{object_key}`

Soft deletes a file or folder.

### `DELETE /n1/objects/permanent/{object_key}`

Hard deletes a trash path.

### `POST /n1/objects/recover/{object_key}`

Recovers a trash path.

### `DELETE /n1/objects/version/{version_id}/{object_key}`

Deletes one object version.

### `POST /n1/objects/recover/version/{version_id}/{object_key}`

Recovers one object version when available.

### `POST /n1/objects/rename`

Body:

```json
{
  "source_fragment": "<fragment_hash>",
  "source_key": "videos/clip.mp4",
  "destination_fragment": "<fragment_hash>",
  "destination_key": "archive/clip.mp4"
}
```

### `POST /n1/objects/bulk-delete`

Body:

```json
{ "object_keys": ["a.txt", "b.txt"] }
```

### `POST /n1/objects/bulk-recover`

Body:

```json
{ "object_keys": ["a.txt", "b.txt"] }
```

### `POST /n1/objects/permanent/bulk-delete`

Body:

```json
{ "object_keys": ["a.txt", "b.txt"] }
```

## Upload API

Use one-shot upload for small files. Use multipart for large files and resumability.

Multipart upload is pipelined inside N1's foreground write lane. Different parts can upload concurrently, and each part writes/flushes/publishes its shard set in parallel across the configured disks. N1 intentionally serializes retries for the same `(version_id, part_index)` so a duplicate client retry cannot race the same receipt or stable shard files.

### One-shot Upload

```http
POST /n1/upload/one-shot/{object_key}
```

Query parameters:

- `checksum` optional
- `durability` optional
- `strict_durability` optional boolean
- `io_gate` optional boolean

Request body is raw bytes.

Example:

```bash
curl -k "$BASE/n1/upload/one-shot/docs/readme.txt" \
  "${AUTH[@]}" \
  -H 'Content-Type: text/plain' \
  --data-binary @readme.txt
```

### Multipart Initiate

```http
POST /n1/upload/multipart/init/{object_key}
```

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
PUT /n1/upload/multipart/{version_id}/{part_index}
```

Query parameters:

- `checksum` optional
- `durability` optional
- `strict_durability` optional boolean
- `io_gate` optional boolean

Request body is the raw bytes for that part. Parts may be retried. Uploaded part receipts are persistent and survive server restart.

Clients should use bounded in-flight parts for large files instead of increasing the one-shot size. Start with `2-4` concurrent parts and tune by disk count and disk quality. Very small multipart jobs may not benefit from part concurrency; large files with many parts usually do. N1 will apply upload backpressure when the disk scheduler is saturated.

Example:

```bash
curl -k "$BASE/n1/upload/multipart/$VERSION_ID/0?io_gate=true" \
  "${AUTH[@]}" \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @part-000
```

### Multipart Status

```http
GET /n1/upload/multipart/{version_id}
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
GET /n1/upload/multipart?limit=100&offset=0
```

### Multipart Finalize

```http
POST /n1/upload/multipart/{version_id}/{session_id}/finalize?object_key=videos/large.mp4
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
DELETE /n1/upload/multipart/{version_id}?object_key=videos/large.mp4
```

Cancel removes the pending upload session. GC can later reclaim orphaned partial payloads.

## Archive Download

### `POST /n1/objects/archive`

Downloads selected files or folders as an archive.

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

Use `tar` for very large folder downloads. `zip` is available but is not ideal for very large archives.

### `POST /n1/objects/archive/manifest`

Streams an archive from a request manifest.

Query parameters:

- `format`: `tar` or `zip`, default `tar`
- `filename`: optional response filename

## Trash And Recovery

Soft delete moves a file or folder into `.trash`. Live listing no longer shows it. Trash listing is derived from the `.trash` namespace itself: a deleted file is listed as that file, and a deleted folder is listed as that folder, without a separate trash metadata plane.

Hard delete removes trash namespace entries. Bytes are returned after GC determines no live or trash namespace path references the blob versions.

N1 1.0.0 keeps trash state single-sourced in the namespace. There is no separate persisted trash metadata plane. This avoids split-brain behavior where secondary delete-state metadata says one thing while the namespace says another.

Empty trash:

```bash
curl -k -X DELETE "$BASE/n1/trash/empty" "${AUTH[@]}"
```

Recover all trash:

```bash
curl -k -X POST "$BASE/n1/trash/recover-all" "${AUTH[@]}"
```

Status routes:

```http
GET /n1/maintenance/trash/empty/status
GET /n1/maintenance/trash/recover/status
```

## Health, Repair, GC, And Redistribution

### Disk IO Scheduler

N1 includes a shared disk IO scheduler for disk-level fairness. Foreground uploads and chunk reads, object status/repair, GC, fragment repair, and redistribution acquire scheduler lanes before doing disk-heavy work. Multipart upload also pipelines shard writes, shard flushes, and shard publication inside the foreground write lane.

The scheduler separates:

- foreground read/write work
- background repair
- background GC
- background redistribution
- per-fragment pressure

This prevents one hot fragment or one background job from consuming the whole disk path. It does not change object metadata, blob layout, durability semantics, or namespace behavior.

The health response includes an `io_scheduler` object with configured permits, active foreground/background counts, and currently available lanes.

### Health

System health:

```http
GET /n1/maintenance/health?deep_scan=true&refresh=true
```

Fragment health:

```http
GET /n1/maintenance/fragments/{fragment}/health?deep_scan=true&refresh=true
```

### Object Status And Repair

Object status:

```http
GET /n1/objects/status/{object_key}?mode=availability
```

Response:

```json
{
  "object_key": "videos/clip.mp4",
  "version_id": "01KR...",
  "status": "healthy",
  "total_parts": 1,
  "total_shards": 2,
  "healthy_shards": 2,
  "missing_shards": 0,
  "corrupt_shards": 0,
  "repairable": true,
  "repaired_shards": 0,
  "failed_repairs": 0
}
```

Object repair:

```http
POST /n1/objects/repair/{object_key}?mode=strict
```

Modes:

- `availability`: fast placement/existence check
- `checksum`: verify checksum where needed
- `strict`: read-only full integrity check
- `scrub`: quiesced/offline full integrity check

Object status and repair are bounded by global and per-fragment gates. If too many checks or repairs are already running, N1 returns `503 Service Unavailable` with a retry-shortly message. Repair and scrub also take an object-scoped lock so two repair/scrub tasks cannot mutate the same object at the same time.

### Fragment Or Global Repair

```http
POST /n1/maintenance/fragments/{fragment}/repair/start?mode=strict
POST /n1/maintenance/repair/start?fragment={fragment}&mode=strict
POST /n1/maintenance/repair/stop
GET  /n1/maintenance/repair/status
```

Useful query parameters:

- `mode`: `availability`, `checksum`, `strict`, `scrub`
- `dry_run`: boolean
- `fast_mode`: boolean
- `read_only_fast`: boolean
- `meta_concurrency`
- `repair_concurrency`
- `batch`

### Garbage Collection

```http
POST /n1/maintenance/fragments/{fragment}/gc/start?mode=availability
POST /n1/maintenance/gc/start?fragment={fragment}&mode=availability
POST /n1/maintenance/gc/stop
GET  /n1/maintenance/gc/status
```

Modes:

- `availability`: conservative online GC
- `strict`: read-only stronger check
- `scrub`: quiesced strongest check

### Redistribution

```http
POST /n1/maintenance/fragments/{fragment}/redistribution/start?mode=strict
POST /n1/maintenance/redistribution/start?fragment={fragment}&mode=strict
POST /n1/maintenance/redistribution/stop
GET  /n1/maintenance/redistribution/status
```

Useful query parameters:

- `mode`: `availability`, `strict`, `scrub`
- `scope`: `all`, `index`, or `blob`
- `durability`: `auto`, `strict`, or `batched`
- `fast_mode`: boolean
- `object_concurrency`
- `status_batch`

When filesystem project quota is enabled, redistribution preflights per-root fragment quota headroom before moving blob payloads. If a fragment is too close to its quota share, N1 fails fast with:

```text
fragment quota headroom insufficient for redistribution
```

That error means the operator should increase fragment quota, run GC if trash/orphans are expected, or disable strict quota only for controlled stress tests. N1 does this before migration to avoid repeated low-level `No space left on device` shard-write failures.

## Realtime Namespace Websocket

Connect with a fragment token:

```text
wss://127.0.0.1:50001/ws?token=<access_token>
```

The websocket emits namespace deltas for CRUD and POSIX changes. It does not ask clients to reload the whole tree after each operation.

Example event:

```json
{
  "op": "EVENT",
  "d": {
    "timestamp_ms": 1778000000000,
    "type": "NAMESPACE_CHANGED",
    "data": {
      "fragment": "<fragment_hash>",
      "operation": "rename",
      "kind": "Directory",
      "source_path": "videos",
      "destination_path": "archive/videos",
      "affected_paths": 1,
      "affected_objects": 1200
    }
  }
}
```

Common operations:

- `upsert_file`
- `mkdir`
- `rename`
- `trash`
- `recover`
- `purge_trash`
- `empty_trash`
- `recover_all`
- `delete_version`
- `bulk_delete`
- `bulk_recover`
- `bulk_delete_permanent`

## Error Handling

N1 returns HTTP status codes with plain error text for most failures.

Common mappings:

- `400 Bad Request`: invalid path, invalid body, missing required field
- `401 Unauthorized`: missing or invalid token/secret
- `403 Forbidden`: token lacks required permission
- `404 Not Found`: object, fragment, or path not found
- `409 Conflict`: existing fragment or running maintenance job
- `413 Payload Too Large`: request exceeds configured size
- `416 Range Not Satisfiable`: invalid range request
- `503 Service Unavailable`: maintenance mode, saturated upload path, disk/quota provisioning issue
- `507 Insufficient Storage`: quota/capacity limit reached

## Troubleshooting

### Fragment creation fails with quota errors

Check that every fragment disk is mounted with project quota support and that N1 has permission to apply project quotas.

```bash
findmnt -no OPTIONS /mnt/disk-00
sudo xfs_quota -x -c 'state' /mnt/disk-00
```

### Upload returns `Upload path is saturated`

The upload IO gate is protecting the server. Retry with backoff or lower client concurrency. On slow mixed disks, this is a backpressure signal, not a data-loss event.

### Empty trash does not immediately free bytes

This is expected. Empty trash retires the `.trash` namespace quickly and recreates an empty `.trash`. GC later reclaims unreferenced object metadata and blob payloads.

### Redistribution fails with quota headroom insufficient

Redistribution can need temporary write headroom while it rebuilds shard placement. With filesystem project quota enabled, N1 checks that headroom before starting blob movement. Increase the fragment quota, run GC, or use a quota-disabled test profile when the goal is stress testing disk behavior instead of quota enforcement.

### Health shows degraded after disk loss

Run fragment repair first, then refresh health:

```bash
curl -k -X POST "$BASE/n1/maintenance/fragments/$FRAGMENT/repair/start?mode=strict" "${AUTH[@]}"
curl -k "$BASE/n1/maintenance/fragments/$FRAGMENT/health?deep_scan=true&refresh=true" "${AUTH[@]}"
```

### Trash recovery path

Trash recovery is path based. Send the path shown by trash listing.

```json
{ "path": "videos" }
```

## Operational Tradeoffs

N1 is optimized for durable large-object storage, POSIX movement, repairability, and bounded memory. It can store many small objects, but tiny-object storms pay per-object namespace commit, receipt, and shard/parity overhead. For very high counts of tiny files, archiving or batching before upload is usually more efficient.

N1 is strongest when it can stream bytes, keep namespace movement lightweight, and keep durable resumability on disk rather than depending on RAM-only state.
