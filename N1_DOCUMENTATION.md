# N1 API Documentation

Current version: `3.0.0`

`N1` is the standalone storage kernel in this repository. In `3.0.0`, it exposes:

- fragment provisioning
- fragment-scoped authentication
- object upload, read, list, version, trash, recover, and archive flows
- a POSIX/object-key namespace facade backed by the per-fragment sharded `indexdb`
- lifecycle, garbage collection, repair, redistribution, and health endpoints
- POSIX-style namespace access on top of the same storage truth

This document describes **N1 Standard**, the secondary sharded `indexdb` engine line. For the primary N1 face for new deployments, use **N1 Edge** and see `N1_EDGE_DOCUMENTATION.md`.

SMB/NAS serving has moved to standalone `Litiaina NS`. `N1` no longer embeds the SMB server; keep `N1` focused on storage-kernel durability, namespace truth, repair, redistribution, and lifecycle.

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

## N1 3.0 Breaking Metadata Rewrite

`N1 3.0` is not runtime-compatible with the `N1 2.x` active metadata layout.

The only active metadata authority for a fragment is its per-fragment sharded `indexdb`:

```text
<index_root>/<fragment_hash>/indexdb/shard_0000.ndxdb
<index_root>/<fragment_hash>/indexdb/shard_0001.ndxdb
...
<index_root>/<fragment_hash>/indexdb/shard_XXXX.ndxdb
```

All shard files together form the fragment metadata authority. The shard count is recorded in fragment metadata at creation from `index_config.partition_count` and must match the running server configuration before the fragment is opened.

Inside the per-fragment indexdb shard set, `N1` owns:

- `indexdb_meta`
- `fragment_meta`
- `namespace_entries`
- `object_heads`
- `object_versions`
- `object_parts`
- `part_shards`
- `object_version_states`
- `upload_sessions`
- `upload_parts`
- `pending_upload_bindings`
- `trash_entries`
- `quota_reservations`
- `maintenance_state`
- `replica_meta`

The following files are not active runtime authorities in `N1 3.0`:

- `namespace_v2/catalog.ndxdb`
- `catalog.ndxdb`
- `namespace.ndxdb`
- `namespace.sqlite`
- any separate namespace database
- any separate catalog database
- any redb-derived active index database
- any serialized `ObjectMetadata` blob as the main persisted object record

Old files may be read only by explicit one-time migration/import tooling. After import, runtime must use only `indexdb/shard_*.ndxdb`; old files should be ignored or renamed aside.

The blob layout is unchanged:

```text
<blob_root>/<fragment_hash>/blob/<version_id>/<shard-or-parity-file>
```

Rename, trash, and recover do not move blobs. The N1 metadata database stores metadata and references only; GC later removes unreachable blob files.

## Storage Filesystem Recommendation

N1 recommends `XFS` for production blob disks, with project quotas enabled when strict fragment quota enforcement is required.

For release confidence and real restart/listing proof, see `N1_TESTING.md`. The production signal is not only `cargo test`; N1 must also pass the real HTTP restart proof on two real disk roots before a risky deployment.

Fragment quota is a storage limit, not an indexdb reservation ledger. N1 stores the configured fragment quota in `fragment_meta`, while the byte truth is the blob filesystem. Hard delete and empty trash remove namespace reachability first; bytes are returned only after GC physically removes the unreachable blob files.

N1 does not implement a hot upload-path quota ledger in indexdb. That avoids turning a hot fragment into a serialized byte-counter workload. Strict quota enforcement is delegated to the filesystem.

Production quota configuration:

```ini
[quota_config]
fragment_quota_backend=xfs_project
```

With `xfs_project`, N1 requires every configured blob root to exist on XFS with project quota enabled. If a blob root is mounted with `noquota`, is not XFS, or the `xfs_quota` tool cannot apply project quotas, N1 refuses startup or fragment create/resize. This is intentional: quota enforcement is either real or disabled, never silently fake.

Disk preparation is one-time per disk/server, not per fragment:

```bash
# New/empty disk example only. Do not run mkfs on a disk with data.
mkfs.xfs /dev/<disk>
mkdir -p /mnt/disk-00
mount -o prjquota /dev/<disk> /mnt/disk-00
```

Persist the mount option in `/etc/fstab`:

```text
/dev/<disk> /mnt/disk-00 xfs defaults,prjquota 0 0
```

For an existing XFS disk, do not reformat just to enable quota. Enable `prjquota` in the mount configuration and remount/reboot according to your operating procedure. N1 should then be restarted and will validate the root before serving.

Validate each blob mount before starting N1:

```bash
findmnt -no TARGET,FSTYPE,OPTIONS /mnt/disk-00
xfs_quota -x -c 'state -p' /mnt/disk-00
```

The N1 process must be allowed to run `xfs_quota` in expert mode and set project quota limits. `xfs_quota -x -c 'state -p'` only proves that project quota accounting exists; it does not prove that the N1 service can change limits. In strict mode, N1 performs a small startup permission probe and fails closed if `limit -p ...` returns `Operation not permitted`.

For a locked-down service deployment, run N1 with the needed filesystem quota privileges or set `N1_XFS_QUOTA_BIN` to a root-owned helper that only permits the required `xfs_quota -x -c ... <mount>` operations. A systemd service running as an unprivileged `User=` without the required capability will not be able to create quota-enforced fragments.

Fragment creation remains dynamic. When `POST /n1/fragments` receives `quota_bytes`, N1 creates the fragment directories, assigns a stable XFS project ID to the fragment root under each blob root, and applies the quota automatically.

Use simple blob mount paths such as `/mnt/disk-00` and `/mnt/disk-01`. In strict quota mode, N1 rejects blob paths containing whitespace or quoting characters because `xfs_quota -c` does not reliably parse quoted project paths.

Important multi-disk detail: XFS project quotas are enforced per filesystem. N1 splits a fragment quota across the unique configured blob filesystems so the total enforced footprint remains bounded without a database byte ledger. This preserves upload performance, but operators should keep blob roots balanced and run redistribution after changing disk count/layout.

N1 owns project ID `9999` for startup permission probing and owns high-numbered project IDs for fragments on its blob filesystems. Do not manually reuse N1-assigned project IDs for other directories on the same XFS mounts, because XFS enforces quota per project ID per filesystem.

Development/report-only mode:

```ini
[quota_config]
fragment_quota_backend=disabled
```

In disabled mode, N1 reports usage from blob files and rejects quota shrink below current blob usage, but there is no hard per-fragment byte enforcement. Use this only for development or deliberately unmanaged deployments.

## Production Systemd Installation

N1 Standard should be installed as a systemd service when operating the Standard engine line in production. N1 Edge is the primary N1 face for new deployments, but Standard still requires stable startup ordering, journal logs, high file-descriptor limits, and permission to set XFS project quota limits when it is used.

The repo provides:

```text
deploy/systemd/litiaina-n1-standard.service.in
deploy/systemd/n1.env.example
scripts/install-n1-systemd.sh
```

Prepare a server directory that contains:

```text
<install-dir>/litiaina-n1
<install-dir>/n1.env
<install-dir>/n1_config.ini
<install-dir>/cert.pem
<install-dir>/key.pem
```

Copy `deploy/systemd/n1.env.example` to `<install-dir>/n1.env` and replace every secret before installing. The service working directory is the install directory, so `n1.env` and `n1_config.ini` must live there unless explicit paths are passed to the installer.

Production install with strict XFS project quota:

```bash
sudo scripts/install-n1-systemd.sh \
  --install-dir /home/administrator/SERVERS/N1-SERVER \
  --enable \
  --start
```

The default installer mode is:

```text
--service-name litiaina-n1-standard
--user root
--group root
--privilege root
```

That is intentional. A service running as an ordinary user can read/write normal files, but it cannot apply XFS project quota limits. If `fragment_quota_backend=xfs_project`, N1 must be able to run `xfs_quota -x -c 'limit -p ...' <mount>`.

Advanced non-root install:

```bash
sudo scripts/install-n1-systemd.sh \
  --install-dir /home/administrator/SERVERS/N1-SERVER \
  --privilege cap-sys-admin \
  --user administrator \
  --group administrator \
  --enable \
  --start
```

This grants ambient `CAP_SYS_ADMIN` to the N1 process. It is still a broad filesystem administration capability, so root mode is simpler to operate and debug. Use non-root capability mode only after validating that the service can create a fragment with strict quota enabled.

The installer preflights strict quota by checking each blob root for:

- mounted filesystem exists
- filesystem type is XFS
- `prjquota` or `pquota` is enabled
- `noquota` is absent
- `xfs_quota -x -c 'state -p' <mount>` succeeds
- `xfs_quota -x -c 'limit -p bhard=1k 9999' <mount>` succeeds

If this preflight fails, fix the mount or service privilege before starting N1. Do not bypass this in production. `--skip-quota-preflight` exists only for controlled packaging or image-build flows where the final runtime host performs the validation.

Useful commands:

```bash
sudo systemctl status litiaina-n1-standard
sudo journalctl -u litiaina-n1-standard -f
sudo systemctl restart litiaina-n1-standard
```

## Namespace And Object Metadata Model

Namespace is the POSIX/object-key front tier inside `indexdb`. The namespace module remains an API facade, but it does not own persistence and does not open a separate catalog file. Namespace persistence is `indexdb.namespace_entries`.

Object metadata remains shaped as `ObjectMetadata { parts: Vec<PartMetadata> }` at the Rust/API boundary, but persisted storage is split across `object_versions`, `object_parts`, and `part_shards`.

Listing APIs use summary rows and must not load `object_parts`, `part_shards`, or deserialize full part maps. Full `ObjectMetadata.parts` is reconstructed only for download, repair, verify, GC, explicit full metadata requests, or recovery paths that need shard maps.

## Memory Tuning

`N1 3.0` is designed to be IO-bound and bounded-memory. Metadata calls are routed through blocking work, listing uses cursor queries, and cold DB handles are evicted LRU instead of keeping a full namespace catalog resident in memory. Metadata writes are partitioned across per-fragment indexdb shards so a hot fragment is not forced through one metadata file.

```ini
[index_config]
partition_count=64
per_db_cache_bytes=auto
max_cached_dbs=auto

[namespace_config]
max_loaded_facades=auto
max_loaded_dbs=auto
```

Defaults are adaptive but intentionally lean. `auto` favors bounded RSS over maximum cache residency even on larger hosts: indexdb DB handles, namespace facades, upload buffers, and stream buffers are capped conservatively so N1 remains closer to an IO-bound storage kernel.

`partition_count` controls how many `shard_XXXX.ndxdb` files each new fragment uses. Supported values are `32`, `64`, and `256`. Fragment creation does not accept a shard count in the API; it records the server's configured value. Existing fragments must match the running server's configured `partition_count` until an explicit index rebalance/migration exists.

Increase these only when deliberately running a high-performance profile on machines with enough RAM and heavy namespace/index scan workloads.

Upload writer note: N1 uses the direct writer by default and routes blocking writes through worker threads. `N1_IO_URING_ENABLED=1` opts into the current experimental ring writer; leave it unset for the stable direct-write path unless specifically benchmarking that implementation.

## Authentication Model

Most `N1` APIs are fragment-scoped.

1. Create a fragment with `POST /n1/fragments`
2. Mint a fragment token with `POST /n1/auth/fragment`
3. Use `Authorization: Bearer <access_token>` on protected routes

Stable `N1 3.0` API routes and operator routes live under `/n1/...`.

Operational note:

- per-fragment indexdb shard files are replicated across configured index roots
- if the owner index disk is lost but another shard replica survives, `N1` reseeds the owner shard from the surviving replica before serving metadata reads
- health reports per-fragment index replica debt
- repair reconciles missing, divergent, or corrupt shard replicas across reachable index roots

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

[`n1_cli`](n1_cli) is the operator-facing companion to the stable `N1 3.0` API.

Current CLI scope:

- fragment create
- fragment quota resize
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
  fragment create --name <fragment_name> --key <fragment_key> --quota-bytes <bytes>

cargo run -p n1_cli -- \
  --url <url> \
  --secret <fragment_key> \
  --fragment <fragment_name_or_hash> \
  fragment resize-quota --quota-bytes <bytes> --master-secret <MASTER_KEY>

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

- In standalone `N1 3.0`, `object_key` is the logical path the caller uses.
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
  "key": "fragment-local-secret",
  "quota_bytes": 536870912000
}
```

Fields:

- `name`: human input used to derive the deterministic fragment hash
- `secret`: must match `MASTER_KEY`
- `key`: fragment secret used later for fragment auth
- `quota_bytes`: required non-zero fragment quota in bytes. A fragment is a quota-bound virtual partition; creation is rejected if active fragment quotas plus the requested quota exceed usable configured storage capacity.

The standalone Storage console uses this same contract and requires a quota amount before submitting fragment creation.

When `fragment_quota_backend=xfs_project`, fragment creation also applies the filesystem project quota. If the blob roots are not mounted with XFS project quota support, creation fails with `503 Service Unavailable`.

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

### `PATCH /n1/fragments/{fragment}/quota`

Resize a fragment quota as an explicit operator/admin action.

Request body:

```json
{
  "quota_bytes": 1073741824000,
  "secret": "<MASTER_KEY>"
}
```

Rules:

- Requires a valid fragment session plus the N1 master secret.
- `quota_bytes` must be greater than zero.
- Increasing quota is accepted only when the new active fragment quota total fits configured physical capacity.
- Decreasing quota is accepted only when current blob filesystem usage fits inside the new `quota_bytes`.
- Shrinking below current used bytes returns `409 Conflict`; growing beyond physical capacity returns `507 Insufficient Storage`.

Response:

```json
{
  "fragment": "fragment-hash",
  "previous_quota_bytes": 536870912000,
  "quota_bytes": 1073741824000,
  "committed_bytes": 4294967296,
  "reserved_bytes": 0,
  "trash_bytes": 0,
  "used_bytes": 4294967296,
  "available_bytes": 1069446856704,
  "usage_percent": 0.4,
  "active_quota_bytes": 1073741824000,
  "capacity_allocated": 4000787030016,
  "quota_enforced": true,
  "quota_backend": "filesystem_project",
  "changed": true,
  "direction": "increase"
}
```

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

### `GET /n1/capacity/quota`

Return quota usage for the authenticated fragment.

Usage is reported from the fragment blob files. `reserved_bytes` is `0` in the N1 3.0 active upload path because multipart resumability uses compact persistent receipts, not a durable quota reservation ledger.

Response:

```json
{
  "fragment": "fragment-hash",
  "state": "active",
  "quota_bytes": 536870912000,
  "committed_bytes": 4294967296,
  "reserved_bytes": 0,
  "trash_bytes": 0,
  "used_bytes": 4294967296,
  "available_bytes": 532575944704,
  "usage_percent": 0.8,
  "quota_enforced": true,
  "quota_backend": "filesystem_project",
  "created_at_unix": 1777440000,
  "updated_at_unix": 1777443600
}
```

`used_bytes` is the blob filesystem footprint currently present for the fragment. Because trash and recover do not move blobs, hard-deleted data may remain included until GC reclaims the unreachable blob files.

## Object Listing And Read API

### `GET /n1/objects`

List live objects from the authenticated fragment namespace in indexdb.

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

Supports logical paths in standalone `N1 3.0`.

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

If `filename` is omitted and the logical path differs from the underlying storage object id, `N1 3.0` will derive a browser-friendly download filename from the logical path.

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

Stream selected logical paths or folders as an archive.

Request body:

```json
{
  "keys": ["videos/demo.mp4", "docs/readme.txt"],
  "prefixes": ["videos/2025/"],
  "format": "tar",
  "filename": "bundle",
  "export_all": false,
  "include_internal": false
}
```

Fields:

- `keys`: logical paths to include directly
- `prefix`: optional single logical folder prefix
- `prefixes`: optional logical folder prefixes for folder/archive export
- `format`: `tar` for folder, mixed, manifest, and fragment-scale export. `zip` is only valid for explicit selected file keys without folder prefixes or `export_all=true`
- `filename`: optional download filename; `.tar` or `.zip` is appended when omitted
- `export_all`: if `true`, archive all visible objects
- `include_internal`: include internal metadata entries when supported

Rules:

- selected folder archive requests walk namespace pages and stream each file by physical object id without prebuilding the full folder in memory
- `prefix` or `prefixes` can be combined with `keys`
- `tar` is the safe/default format for large exports and folder exports

### `GET /n1/objects/archive/manifest`

Stream a manifest-driven archive build.

Query parameters:

- `format` optional, default `tar`
- `filename` optional

The request body is a manifest payload consumed by the archive builder.

## Upload API

`N1 3.0` supports both one-shot upload and resumable multipart upload.

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

### Upload Stripe Selection

N1 stores the stripe block size with each committed object version. The configured stripe size remains the default ceiling for large objects, while small uploads may use a smaller per-object stripe to avoid excessive physical padding on blob disks.

Current N1 3.0 selection:

- uploads up to `64 KiB`: `4 KiB` stripe block
- uploads above `64 KiB` and up to `1 MiB`: `16 KiB` stripe block
- larger uploads: configured stripe block

One-shot upload chooses the stripe from the object size. Multipart upload chooses the stripe once from the planned part sizes during session creation and persists it in the upload manifest. Every retry, finalize, read, archive, repair, and redistribution operation must honor that persisted object stripe.

This keeps multipart resumability durable across server restarts while reducing the quota/usage amplification of deeply nested folder uploads with many small files. Multipart remains strict to the session manifest after creation; changing server EC settings does not rewrite an active session.

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

If an interrupted multipart session already exists for the same logical path and compatible upload manifest, `N1 3.0` reuses it instead of creating a new one.

Multipart resumability is persistent. Each uploaded part writes raw part bytes to the blob plane, then records a compact receipt in the fragment indexdb. The full session row is not rewritten for every part; activity is flushed periodically while each part receipt remains durable. If a retry finds the part bytes already present, N1 treats the part as uploaded and refreshes the missing receipt when needed.

The pending-upload lookup is part of namespace/indexdb state, not blob state. It lets resumability survive process restart while preserving logical POSIX paths separately from immutable object metadata.

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

These routes operate on the namespace facade backed by `indexdb.namespace_entries` in `N1 3.0`.

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

Trash listing behavior:

- root trash listing returns recoverable trashed items directly
- recovering a listed file uses that exact file path
- recovering a listed directory restores the deleted directory subtree

Trash generation behavior:

- each trash root has a stable `trash_entry_id`
- repeated deletes of the same visible path produce separate trash generations
- recover and purge should send `trash_entry_id` when it is present in list/stat responses
- path-only recover or purge remains available for legacy callers, but ambiguous same-path trash generations must be resolved by `trash_entry_id`

Request body for an exact trash generation:

```json
{
  "path": "videos/demo.mp4",
  "trash_entry_id": "01K..."
}
```

### `POST /n1/posix/purge`

Remove a single trashed file or trashed directory subtree from the trash lifecycle.

Request body:

```json
{
  "path": "videos/demo.mp4",
  "trash_entry_id": "01K..."
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

For standalone `N1 3.0`, source and destination fragment should normally be the same fragment.

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

`N1 3.0` is lifecycle-first.

- normal delete means move to trash
- `empty trash` retires namespace visibility
- GC later reclaims unreachable object versions and blobs

Routes that try to expose immediate permanent delete behavior are no longer the preferred `3.0` contract.

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

Maintenance mode controls how much the operation protects foreground CRUD:

- `availability` keeps the node writable and uses conservative pacing/concurrency.
- `strict` enters read-only maintenance while repair runs and allows wider metadata/blob concurrency.
- `scrub` enters quiesced maintenance for the strongest offline-style repair/scrub pass.
- `checksum` behaves like strict integrity checking without requiring every operator workflow to use the word scrub.

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

`gc/start` query parameters:

- `mode` optional: `availability`, `strict`, `scrub`
- `fast_mode` optional boolean
- `fragment` optional fragment filter

GC mode controls reclaim aggressiveness:

- `availability` keeps CRUD available and preserves a longer grace window for newly committed namespace/object bindings.
- `strict` enters read-only maintenance and shortens GC grace windows for intentional operator reclaim.
- `scrub` enters quiesced maintenance and performs the most aggressive reachability cleanup.

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

Redistribution mode controls the placement mover:

- `availability` keeps foreground CRUD available and favors lower concurrency.
- `strict` enters read-only maintenance and widens object move concurrency for faster convergence.
- `scrub` enters quiesced maintenance and uses the widest maintenance lane.

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
    "key":"fragment-local-secret",
    "quota_bytes":536870912000
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

- `N1 3.0` is lifecycle-first. Clients should think in terms of `trash`, `recover`, and `GC`, not immediate physical delete.
- Standalone namespace routes and raw object routes are both available. New integrations should prefer the namespace-aware `3.0` surfaces unless they specifically need lower-level object semantics.
- Some helper routes remain legacy-shaped. The most visible example is `GET /n1/fragments/check` and `GET /n1/fragments/hash`, which currently deserialize JSON bodies.
- Signed-read URLs are the preferred way to expose public view, stream, or download without leaking bearer tokens.
- SMB/NAS workflows are handled by standalone `Litiaina NS`. `N1` remains the storage kernel and exposes POSIX-style namespace routes directly.
