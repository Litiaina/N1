# N1 Documentation

N1 3.4.0 is Litiaina's single-node object storage system. It exposes object-storage semantics through a POSIX-like namespace, immutable object versions, shard/parity blob storage, and a native `n1://` protocol for internal tools. It is designed to exploit one Linux server: multiple disks, bounded memory, high-throughput and durable upload modes, autonomous repair, explicit redistribution, targeted TTL trash lifecycle, archive download, and realtime namespace events.

N1 is not a filesystem. It is an object storage system with its own namespace rules: folders are namespace directories, files are stable object identities, and each committed write creates an immutable object version. Large data movement stays in the blob plane; namespace operations stay lightweight.

For implementation details, see [ARCHITECTURE.md](ARCHITECTURE.md). The canonical
HTTPS/WebSocket transport contract is [NOA v1](NOA.md). For release validation
notes, see [VERIFICATION.md](VERIFICATION.md).

## Version And Release Status

N1 `3.4.0` is the current development engine/package version. It is on the
development release channel and is not a declaration that this engine line has
reached its stable product milestone. Storage compatibility is tracked
separately as engine format `3`; package updates within this development line
must continue to honor the format-3 architecture unless an explicitly new
engine format is introduced.

Historical verification directories retain the version of the binary that
produced them. A `3.0.0` or `3.0.1` evidence path is therefore not renamed when
the current package version advances to `3.2.0`.

## Core Concepts

- **Fragment:** An authenticated namespace and object-placement scope. Clients mount/authenticate a fragment before using the API. The public fragment hash is a mutable namespace alias; N1 stores data under an immutable `@f_<stable_fragment_id>` physical fragment root.
- **Namespace:** The user-facing tree of folders and files. This is what `/noa/v1/posix/*` and the UI show.
- **Object:** The stable identity behind a file. Renaming a file changes the namespace name, not the object identity.
- **Version:** One committed immutable object version. Uploading to an existing file path creates a new version under the same object identity.
- **Trash:** Every soft delete reserves one timestamp generation and renames the live file or directory to `.trash/<unix-nanos>.d<path-depth>/<same-logical-path>`. Repeated deletion of the same path therefore cannot collide. Recovery moves one selected generation back to its original path and returns `409 Conflict` if that live destination exists. Configured TTL expiry removes the generation and reclaims only safely unreferenced immutable objects.
- **Repair:** Rebuilds missing namespace/control replicas, immutable object metadata placements, and shard/parity payload when enough data remains.
- **Redistribution:** Explicitly rewrites selected object versions into the current blob/EC topology. Existing objects remain valid under their recorded topology until redistributed.

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
- `n1_config.ini` - local machine configuration: ports, disk roots, erasure
  layout, and deployment settings.

These files are intentionally ignored by git. Start N1 once in the deployment
directory to generate them, stop the process, then fill the real machine values
before installing the service.

Production disks should already be mounted before N1 starts. N1 monitors disk
capacity and placement availability directly; it does not require special
filesystem accounting support.

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

The installer writes into `/etc/systemd/system`, so installation normally uses
`sudo`. The service itself defaults to the original sudo user and can be pinned
explicitly with `--user <user> --group <group>`.

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
  --dry-run
```

`--dry-run` renders the exact unit without installing it.

## Configuration

N1 reads `n1_config.ini` and `n1.env` from the server working directory.
N1 3.2.0 does not accept an older configuration contract: the complete
`[auto_healing]` section below is required, and malformed values fail startup.

Minimal two-disk configuration where both disks house index and blob roles:

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

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
trash_retention_seconds=2592000

[auto_healing]
enabled=true
topology_check_interval_seconds=30
topology_recovery_stability_seconds=10
initial_scan_delay_seconds=60
availability_scan_interval_seconds=3600
checksum_scrub_interval_seconds=604800
object_scan_concurrency=1
shard_repair_concurrency=1
metadata_batch_size=128

[upload_config]
data_shards=1
parity_shards=1
max_part_size=52428800
stripe_block_size=262144
max_concurrent_io=auto
upload_memory_budget_bytes=auto
adaptive_concurrency_enabled=true
upload_io_gate_enabled=true

[io_control]
foreground_wait_timeout_ms=30000
stall_threshold_ms=20000
slow_io_threshold_ms=1000
latency_samples_per_class=1024
```

`n1.env` must provide the secrets used by N1:

```bash
JWT_SECRET=change-this
MASTER_KEY=change-this-master-key
SIGNED_READ_URL_SECRET=change-this-signed-url-secret
```

### Disk And Capacity Notes

N1 is Linux-only and works with normal mounted disks. It uses `statvfs`-based
capacity checks plus per-placement upload preflight so aggregate capacity and
the placement-limiting disk are both considered before writes proceed.

On first N1 3.2.0 startup, every configured index/blob root receives a persistent
`.n1-root-id`, and N1 atomically records the expected identities under
`<fs_storage_path>/.n1/topology-roots.json`. Mount all production disks before
the first startup. Once a path is enrolled, N1 never silently adopts a missing
mount's underlying directory or a replacement disk at the same path. A root
identity mismatch is reported as offline and keeps unsafe mutations read-only.

To replace a disk at the same configured path, stop N1, remove that path from
`index_store_disks`/`blob_store_disks`, and start N1 once so the path is
explicitly decommissioned. Stop N1 again, mount the replacement, restore the
path in the configuration, and start N1 to enroll its new identity. Never
delete or edit the topology registry while N1 is running.

### Fragment Identity Layout

N1 3.2.0 separates the mutable fragment alias, namespace/control plane, and immutable object plane:

```text
<index_root>/
  .fragment/
    <fragment_hash> -> ../@f_<stable_fragment_id>

  @f_<stable_fragment_id>/
    .fragment.ndx
    .uploads/
    .trash/
      <unix-nanos>.d<path-depth>/<original-logical-path>
    <namespace directories>

<blob_root>/
  @f_<stable_fragment_id>/
    .objects/
      ab/
        cd/
          @o_<object-id>/
            <version>.<size>.<time>.<extension>.ndx
            <version>/
              <shard-or-parity-files>
```

The `.fragment/<fragment_hash>` symlink is the mount/auth alias. The `@f_<stable_fragment_id>` identity is immutable. Renaming a fragment changes only index-plane alias/control state; it does not move namespace contents, blob payload, or immutable object `.ndx` records.

Object `.ndx` metadata is stored only on blob roots that carry a shard or parity file for that version. The metadata and EC payload therefore share one durability topology. Index-only roots do not carry object metadata capacity.

N1 3.2.0 uses engine format 3 and does not open, adopt, or migrate earlier or unversioned N1 fragments. Initialize fresh engine-format-3 fragments; an incompatible fragment fails closed.

## Quick Start

Set a base URL:

```bash
BASE='https://127.0.0.1:50001'
MASTER='<MASTER_KEY>'
```

Check server health:

```bash
curl -k "$BASE/noa/v1/ping"
```

Create a fragment:

```bash
curl -k "$BASE/noa/v1/fragments" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "mainframe",
    "secret": "'"$MASTER"'",
    "key": "fragment-local-secret"
  }'
```

Authenticate the fragment:

```bash
curl -k "$BASE/noa/v1/auth/fragment" \
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
curl -k "$BASE/noa/v1/posix/mkdir" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"path":"videos"}'
```

Upload a small file with one-shot upload:

```bash
curl -k "$BASE/noa/v1/upload/one-shot/videos/clip.mp4" "${AUTH[@]}" \
  -H 'Content-Type: video/mp4' \
  --data-binary @clip.mp4
```

List files:

```bash
curl -k "$BASE/noa/v1/posix/list?path=videos&limit=200" "${AUTH[@]}"
```

Download a file:

```bash
curl -k "$BASE/noa/v1/objects/download?object_key=videos/clip.mp4" \
  "${AUTH[@]}" \
  -o clip.mp4
```

Soft delete and recover:

```bash
curl -k "$BASE/noa/v1/posix/trash" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"path":"videos/clip.mp4"}'

curl -k "$BASE/noa/v1/posix/recover" "${AUTH[@]}" \
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

- The universal HTTPS/WebSocket surface is the Litiaina Node Object API (NOA)
  v1 under `/noa/v1`. N1 3.2.0 preserves the preceding request, response, and
  storage semantics while replacing the engine-branded route namespace.
- `object_key`, `path`, `source_path`, and `destination_path` are namespace paths such as `videos/clip.mp4`. URL-encode spaces and special characters when they appear in a path or query string.
- Request bodies are JSON unless the route says the body is raw bytes.
- HTTPS is the universal API face for consumers and broad compatibility. `n1://` is the native N1 protocol face for N1 Console, N1 Shell, NS, N2, and heavy internal clients.
- Large files should use multipart upload. One-shot upload is intended for smaller objects.
- `version_id` is the persistent upload/object-version id. Multipart part upload and status use `version_id`; finalize also requires the `session_id` returned by init.
- Delete is always a soft delete. Trash remains recoverable until `trash_retention_seconds` expires; there is no manual empty-trash or hard-delete API.
- Normal trash reclamation scans only `.trash`, removes expired namespace entries, verifies reference safety, and directly reclaims the exact unreferenced object ids.
- Object status/repair is object-scoped. A repair or scrub for one object does not lock the whole fragment.
- When an operation returns `503 Service Unavailable`, retry later. This can mean maintenance is running, object maintenance gates are saturated, or disk provisioning prevents safe progress.

## Native N1 Protocol

N1 3.2.0 includes the native N1 protocol:

```text
n1://127.0.0.1:50002
protocol: n1/0.5
```

HTTPS remains the universal consumer API. The native protocol is the low-level N1 execution face for N1 Console, N1 Shell, NS, N2, and trusted services that need lower latency than HTTPS for namespace, control-plane, and data-plane calls.

The native protocol does not create another storage engine, another namespace, another metadata authority, or another code path for object truth. Native requests use typed `n1://` operations that call N1 core/service functions directly. They do not emulate HTTP requests and they do not dispatch through the Axum route graph.

Rust clients should use the workspace `n1_native_client` crate instead of reimplementing frames. The crate owns native URL parsing, MessagePack frame IO, fragment authentication, typed operation calls, raw upload bodies, and raw streaming response reads.

`n1/0.5` supports the native data plane. It removes manual purge, empty-trash,
and version-delete operations as part of the N1 3.2.0 lifecycle contract:

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
- `object.bulk_delete`
- `object.bulk_recover`
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

Object:

- `object.list`
- `object.status`
- `object.repair`
- `object.delete`
- `object.recover`

Trash:

- `trash.list`
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
- `fragment.usage`
- `capacity.usage`

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

Native is designed to remove HTTP/TLS overhead and JSON response duplication for trusted internal clients. N1 3.2.0 operation responses send the result once as MessagePack body bytes instead of JSON body plus a second JSON copy in the response envelope. Upload operations send payload bytes outside the MessagePack envelope as raw native body bytes, and streaming download/view responses send raw object bytes after the control response frame.

Current release-build loopback proof on 1,000 measured iterations showed:

- HTTPS `posix stat`: about `3.4k ops/sec`, p50 about `262 us`.
- Native `namespace.stat`: about `11.8k ops/sec`, p50 about `76 us`.
- HTTPS `posix list`: about `576 ops/sec`, p50 about `1387 us`.
- Native `namespace.list`: about `804 ops/sec`, p50 about `1189 us`.

Listing is less transport-bound because namespace scanning and response body size dominate, but the native MessagePack body path avoids JSON duplication and now stays ahead of HTTPS on the POSIX list proof.

### Tooling Policy

N1-owned tools should use `n1://` when they are running on trusted infrastructure. HTTPS stays available for browsers, external consumers, curl workflows, and compatibility. `n1_shell` uses native transport by default and only uses HTTPS when the operator explicitly passes `--url <https-url>`.

`n1_shell` uses the shared `n1_native_client` crate. It does not keep a private native frame/auth implementation.

For normal maintenance, use direct commands. The shell accepts either a fragment name or a fragment hash. `--none` means whole-N1 scope:

```bash
cargo run -p n1_shell -- --secret <secret> --fragment <auth-fragment> repair --none --strict --fast-mode
cargo run -p n1_shell -- --secret <secret> --fragment <fragment-name-or-hash> repair --dry-run --availability
cargo run -p n1_shell -- --secret <secret> --fragment <auth-fragment> gc --none --scrub --fast-mode
cargo run -p n1_shell -- --secret <secret> --fragment <auth-fragment> redistribute --none --strict --durability strict
```

Status, stop, and watch are flags, not nested subcommands:

```bash
cargo run -p n1_shell -- --secret <secret> --fragment <auth-fragment> repair --status
cargo run -p n1_shell -- --secret <secret> --fragment <auth-fragment> gc --watch
cargo run -p n1_shell -- --secret <secret> --fragment <auth-fragment> redistribute --stop
```

`n1_shell` is stateful when opened as a shell:

```bash
cargo run -p n1_shell -- --secret <secret> --fragment <fragment> shell
```

The shell keeps one native session open and supports read/maintenance operations:

- `health`
- `usage`
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

The shell intentionally keeps destructive namespace mutations out of the operator prompt. Rename, delete, update, upload, and application CRUD still belong to the HTTP or native API surfaces used by clients that explicitly own those workflows.

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
  "refresh_expires_in": 2592000
}
```

### `POST /noa/v1/auth/refresh`

Body:

```json
{ "refresh_token": "<refresh-token>" }
```

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

- `path`: directory path, default `""`
- `limit`: page size, default `200`, max `1000`
- `continuation_token`: optional cursor
- `trash`: `true` to list trash

Example:

```bash
curl -k "$BASE/noa/v1/posix/list?path=videos&limit=200" "${AUTH[@]}"
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

### `GET /noa/v1/posix/stat`

Query parameters:

- `path`
- `trash=true` for trash lookup

### `POST /noa/v1/posix/mkdir`

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

### `POST /noa/v1/posix/trash`

Soft deletes a file or folder.

Body:

```json
{ "path": "archive/clip.mp4" }
```

### `POST /noa/v1/posix/recover`

Recovers a trash path by moving it from `.trash` back to the live namespace. Clients should send the `path` returned by trash listing.

Body:

```json
{ "path": "archive/clip.mp4" }
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
curl -k "$BASE/noa/v1/objects?prefix=videos/&limit=200" "${AUTH[@]}"
```

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
curl -k "$BASE/noa/v1/objects/download?object_key=videos/clip.mp4" \
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

Soft deletes a file or folder.

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

Use one-shot upload for small files. Use multipart for large files and resumability.

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
curl -k "$BASE/noa/v1/upload/one-shot/docs/readme.txt" \
  "${AUTH[@]}" \
  -H 'Content-Type: text/plain' \
  --data-binary @readme.txt
```

### Multipart Initiate

The standard multipart API is the high-throughput path. Initiation and part
completion state live only in process memory. Parts may be uploaded concurrently;
each part is streamed once through EC to its fixed K+M placement. N1 writes no
session manifest, namespace pending record, or per-part receipt on this path.
Finalize synchronizes the completed shard set and then publishes the immutable
object index and namespace entry once. An unfinished standard session does not
survive process restart; its unreferenced shards remain invisible and are
reclaimed by the normal orphan TTL scan.

```http
POST /noa/v1/upload/multipart/init/{object_key}
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
PUT /noa/v1/upload/multipart/{version_id}/{part_index}
```

Query parameters:

- `checksum` optional
- `io_gate` optional boolean

Request body is the raw bytes for that part. Distinct part indexes are accepted
concurrently. Repeating a completed part index in the same live process is
idempotent; a concurrent duplicate of an in-flight index returns conflict.

Clients should use bounded in-flight parts for large files instead of increasing the one-shot size. Start with `2-4` concurrent parts and tune by disk count and disk quality. Very small multipart jobs may not benefit from part concurrency; large files with many parts usually do. N1 will apply upload backpressure when the disk scheduler is saturated.

Example:

```bash
curl -k "$BASE/noa/v1/upload/multipart/$VERSION_ID/0?io_gate=true" \
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
namespace binding, and compact part-receipt contract is unchanged in N1 3.4.0.
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
multipart contract. Existing native `n1://` multipart operations retain this
durable restart-resumable behavior; N1 3.4.0 does not silently change their
established contract.

## Archive Download

### `POST /noa/v1/objects/archive`

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

### `POST /noa/v1/objects/archive/manifest`

Streams an archive from a request manifest.

Query parameters:

- `format`: `tar` or `zip`, default `tar`
- `filename`: optional response filename

## Trash And Recovery

Soft delete reserves one filesystem directory named `<unix-nanos>.d<path-depth>` and renames a file or folder from its live path to `.trash/<generation>/<same-logical-path>` on every index root. Live listing no longer shows it. The generation name supplies deletion time and the retained path supplies the exact recovery destination. There is no lifecycle database, record file, receipt, or temporary state.

Each delete has a distinct `trash_entry_id`, so the same logical path may be deleted repeatedly without overwriting older recoverable data. Recovery selects that id, performs the reverse rename, and returns `409 Conflict` without changing trash if the live destination already exists. Folder soft delete and recovery remain directory renames; N1 does not scan descendants until a retained folder reaches expiry.

The background lifecycle scans only timestamp generations below `.trash`, not the full blob plane. It compares each generation timestamp with `trash_retention_seconds`. For expired entries it removes the namespace reference, verifies all candidate object ids once against the remaining namespace, and directly reclaims only unreferenced immutable object directories. A crash can leave only safe immutable orphan data; explicit deep GC reclaims that leak without risking reachable objects.

N1 3.2.0 exposes no empty-trash, per-entry purge, permanent-delete, or version-delete operation over HTTP, POSIX, native protocol, shell, or console. Manual GC cannot shorten the retention window.

Legacy HTTP clients receive `410 Gone` from the retired destructive routes.
This explicit tombstone prevents a former `/objects/permanent/...` URL from
falling through to the ordinary object-key catch-all. Removed native operation
names fail as unsupported requests.

Recover all trash:

```bash
curl -k -X POST "$BASE/noa/v1/trash/recover-all" "${AUTH[@]}"
```

Lifecycle state is reported in the system health `maintenance` object through `trash_lifecycle_running`, `trash_lifecycle_phase`, `trash_retention_seconds`, and `trash_versions_expired`.

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

N1 IO control adds bounded foreground admission without imposing a total
operation timeout. A foreground request that cannot enter the scheduler within
`foreground_wait_timeout_ms` receives `503 Service Unavailable`; an admitted
stream remains active as long as it continues making progress. Background
maintenance waits for its reserved lane and remains governed by its existing
stop/cancel authority.

The health response includes:

- `io_scheduler`: configured lanes, adaptive foreground target, queue depth,
  pressure state, per-class operation/byte/failure counters, and active
  operation progress/stall state
- `io_control`: configured thresholds, bounded p50/p95/p99 latency samples,
  recent slow paths, process RSS, thread count, and open file descriptors

Upload and download counters report logical payload bytes. GC reports reclaimed
logical bytes. Repair and redistribution use progress heartbeats while scanning
and rewriting so long-running healthy work is not mislabeled as stalled.

The pressure states are:

- `idle`: no disk-heavy operation is active
- `normal`: work is active without scheduler queue pressure
- `pressured`: lanes are busy or work is queued
- `saturated`: foreground work is queued while all scheduler lanes are occupied
- `stalled`: at least one active operation has made no progress for
  `stall_threshold_ms`

Adaptive concurrency does not reduce throughput merely because disks are fully
utilized. It blocks upward growth under pressure and cuts concurrency only when
an actual no-progress stall or system CPU/RAM/load pressure is observed.

### Health

System health:

```http
GET /noa/v1/maintenance/health?deep_scan=true&refresh=true
```

Fragment health:

```http
GET /noa/v1/maintenance/fragments/{fragment}/health?deep_scan=true&refresh=true
```

The system health `maintenance.auto_healing` object reports the current
controller phase, registered/writable roots, distinct writable devices, EC
width, read-only reason, last topology check, last availability scan, last
checksum scrub, and last error.

N1 starts read-only, validates the registered storage identities and writable
failure domains, and enables mutations only after the topology has remained
safe for `topology_recovery_stability_seconds`. The current configured `D + P`
must fit on distinct healthy devices. Extra configured disks may be offline
without degrading healthy recorded objects as long as the current EC width can
still be satisfied.

### Object Status And Repair

Object status:

```http
GET /noa/v1/objects/status/{object_key}?mode=availability
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
POST /noa/v1/objects/repair/{object_key}?mode=strict
```

Modes:

- `availability`: fast placement/existence check
- `checksum`: verify checksum where needed
- `strict`: read-only full integrity check
- `scrub`: quiesced/offline full integrity check

Object status and repair are bounded by global and per-fragment gates. If too many checks or repairs are already running, N1 returns `503 Service Unavailable` with a retry-shortly message. Repair and scrub also take an object-scoped lock so two repair/scrub tasks cannot mutate the same object at the same time.

### Fragment Or Global Repair

The auto-healing controller normally owns routine global maintenance. Its
availability cycle checks recorded paths and sizes; its checksum cycle verifies
payload checksums. Fragments are processed serially, while
`object_scan_concurrency` bounds objects inside the current fragment and
`shard_repair_concurrency` bounds repair IO. The conservative defaults are one
object and one shard repair at a time.

Both healthy scheduled cycles remain in `availability` mode, so foreground
reads and mutations continue. N1 re-reads each immutable version while holding
its object-operation lock before repairing or atomically publishing a higher
placement generation. A manually requested `checksum` remains read-only
by default; `strict` is read-only and `scrub` is quiesced. The autonomous
controller enters persistent read-only only when topology or root identity is
unsafe, or when a scan finishes with unresolved repair debt.

When a recorded shard root is gone but enough data and another distinct healthy
root exist, the healer reconstructs the shard on that replacement root and
atomically publishes a higher object placement generation. Stale metadata at a
lower generation cannot win. If any object cannot be repaired, the scan is
reported as failed and N1 keeps user mutations read-only until a later scan
completes without unresolved repair debt.

Manual repair remains available for diagnosis or an immediate operator-driven
cycle:

```http
POST /noa/v1/maintenance/fragments/{fragment}/repair/start?mode=strict
POST /noa/v1/maintenance/repair/start?fragment={fragment}&mode=strict
POST /noa/v1/maintenance/repair/stop
GET  /noa/v1/maintenance/repair/status
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
POST /noa/v1/maintenance/fragments/{fragment}/gc/start?mode=availability
POST /noa/v1/maintenance/gc/start?fragment={fragment}&mode=availability
POST /noa/v1/maintenance/gc/stop
GET  /noa/v1/maintenance/gc/status
```

Modes:

- `availability`: conservative online GC
- `strict`: read-only stronger check
- `scrub`: quiesced strongest check

### Redistribution

```http
POST /noa/v1/maintenance/fragments/{fragment}/redistribution/start?mode=strict
POST /noa/v1/maintenance/redistribution/start?fragment={fragment}&mode=strict
POST /noa/v1/maintenance/redistribution/stop
GET  /noa/v1/maintenance/redistribution/status
```

Useful query parameters:

- `mode`: `availability`, `strict`, `scrub`
- `scope`: `all`, `index`, or `blob`
- `durability`: `auto`, `strict`, or `batched`
- `fast_mode`: boolean
- `object_concurrency`
- `status_batch`

Redistribution rewrites selected object versions into the current blob/EC
topology. Upload and rewrite paths use the same disk-capacity and placement
preflight model; if a disk is too full, N1 fails before unsafe shard writes.

Redistribution is never triggered by disk addition or auto-healing. Each object
version remains governed by its recorded `D + P`, stripe size, and root set.
Changing the configured EC width affects new versions only. Added disks enter
the deterministic placement pool for new objects and as eligible repair
targets; healthy existing objects are neither degraded nor rewritten.

Namespace maintenance also reconciles missing or incomplete index replicas.
Ordinary namespace mutations are direct filesystem operations on every
accepted index root and write no generation record. Root-identity and topology
safety prevent mutations while a configured root is absent or replaced, so a
returning root cannot silently miss foreground writes. Replica inspection uses
a deterministic digest of sorted namespace/control content; destructive repair
fails closed when equally complete replicas reference different immutable
objects.

## Realtime Namespace Websocket

Connect with a fragment token:

```text
wss://127.0.0.1:50001/noa/v1/ws?token=<access_token>
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
- `recover_all`
- `bulk_delete`
- `bulk_recover`

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
- `503 Service Unavailable`: maintenance mode, saturated upload path, or disk provisioning issue
- `507 Insufficient Storage`: capacity limit reached

## Troubleshooting

### Fragment creation fails with disk errors

Check that every configured index and blob root exists, is mounted, and is
writable by the N1 service user. Also inspect `maintenance.auto_healing` for a
root-identity mismatch. N1 deliberately does not overwrite an enrolled
identity when a disk is replaced or a mount is absent.

### Upload returns `Upload path is saturated`

The upload IO gate is protecting the server. Retry with backoff or lower client concurrency. On slow mixed disks, this is a backpressure signal, not a data-loss event.

### Deleted data has not been reclaimed yet

This is expected while the configured `trash_retention_seconds` has not elapsed. N1 deliberately preserves the entry for recovery. After expiry, the targeted lifecycle scan removes the `.trash` entry and directly reclaims object data only when no other namespace reference remains. Manual GC does not shorten this window.

### Redistribution fails with insufficient storage

Redistribution can need temporary write headroom while it rebuilds shard
placement. Free capacity, run GC if trash/orphans are expected, or add blob
capacity before retrying.

### Health shows degraded after disk loss

Wait for the configured automatic availability cycle or start an immediate
manual repair, then refresh health:

```bash
curl -k -X POST "$BASE/noa/v1/maintenance/fragments/$FRAGMENT/repair/start?mode=strict" "${AUTH[@]}"
curl -k "$BASE/noa/v1/maintenance/fragments/$FRAGMENT/health?deep_scan=true&refresh=true" "${AUTH[@]}"
```

### Trash recovery path

Trash listing returns both the original path and `trash_entry_id`. Send the id to recover one exact deletion generation. Path-only recovery selects the newest retained generation for that path. Recovery remains strict and returns `409 Conflict` when the original live path is occupied.

```json
{ "path": "videos" }
```

## Operational Tradeoffs

N1 is optimized for durable large-object storage, POSIX movement, repairability, and bounded memory. It can store many small objects, but tiny-object storms pay per-object namespace commit, receipt, and shard/parity overhead. For very high counts of tiny files, archiving or batching before upload is usually more efficient.

N1 is strongest when it can stream bytes, keep namespace movement lightweight, and keep durable resumability on disk rather than depending on RAM-only state.
