<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N2 API Documentation</h1>
  <p>Unified route reference for the N2 platform. N1 remains the shared node-core engine.</p>
</div>

> Deployment profile status (2026-03-20):
> - `N2` is the product identity of this workspace.
> - `N1` is the shared storage engine used by both deployment shapes.
> - `n1` is the stable single-node deployment path.
> - `n1` is released at `0.9.0`.
> - `n2` (`litiaina-n2`) is the multi-node N2 deployment path in this workspace and is released at `0.2.0`.
> - `registry` (`registry-server`) is the SQLite-backed Registry layer for user/business metadata and is versioned independently at `0.4.0`.
> - `n1-arc` is experimental for raw-device deployments.
> - See `DEPLOYMENT_PROFILES.md`.
>
> Source of truth for routing (`n1`, single-node): `n1/src/routes/storage_routes.rs`.
> Source of truth for routing (`n2`, multi-node): `n2/src/routes/storage_routes.rs`.
> N1-Arc route path: `n1-arc/src/routes/storage_routes.rs`.
> Source of truth for request/response behavior is profile-specific (`n1/src/n1_core` as the shared N1 engine for both `n1` and `n2`, and `n1-arc/src/n1_core` for `n1-arc`).
> - unless explicitly labeled otherwise, the route descriptions below document the stable `n1` runtime first
> Runtime split note:
> - Single-node N2 deployment runs `n1` (`litiaina-n1`).
> - Multi-node N2 deployment runs `n2` (`litiaina-n2`).
> - `n2` adds `/v4/n2/*` cluster routes and `/v4/n2/internal/*` peer routes on top of the same shared stable N1 node core used by `n1`.
> - For the multi-node operator runbook, see `N2_DEPLOYMENT.md`.
> N1-Arc route scope note:
> - `n1-arc` serves v4 object/maintenance APIs and does not expose legacy `/dir/*` routes.
> - `n1-arc` also does not expose `/authenticate/fs`.

> Storage backend note:
> - `n1` (stable): `index_store_disks` and `blob_store_disks` are mounted filesystem paths (directory roots).
> - `n1-arc` (experimental raw-device): `index_store_disks` and `blob_store_disks` are raw, unformatted, unmounted device paths managed directly by N1.
> - `n1` prefers Linux `io_uring` on the blob write path when available and falls back to `O_DIRECT` + blocking writes when it is not.
> - Payload substrate still differs by profile (filesystem files in `n1`, raw extents in `n1-arc`).

> Platform identity note:
> - N2 is the operator-facing platform and API identity.
> - N1 is the node-local storage engine inside N2.
> - Single-node N2 serves the shared v4 object/maintenance surface directly.
> - Multi-node N2 serves that same node surface plus the `/v4/n2/*` distributed control/data-plane routes.
>
> Multi-node N2 deployment note:
> - `n2` is operational on a tested 2-node filesystem-backed cluster.
> - In an isolated `litiaina-n2` working directory, the runtime now auto-creates `n1.env` and `n1_config.ini` compatibility aliases beside `n2.env` and `n2_config.ini` when needed.
> - Default `[n2].cluster_commit_mode=strict_full_placement` now requires the planned `kc+mc` target set before `PUT /v4/n2/object/...` succeeds.
> - Use `committed_shards` as the immediate placement truth and `required_commit_shards` as the actual success threshold reported by the runtime.
> - Use `cluster_commit_mode=quorum` only when you explicitly want degraded writes to succeed with less than full planned placement.
> - Use `POST /v4/n2/reconcile/{fragment}` after degraded periods or peer recovery to restore missing copies.
> - `auto_maintenance_mode=off|observe|repair_and_redistribute` lets operators choose whether topology changes stay fully manual, debt-reporting only, or automatically reconciled.
> - N2 governance is now consensus-scoped only to cluster state:
>   - membership apply and rollback go through majority governance commit
>   - object payload traffic does not pass through a governance leader
>   - `GET /v4/n2/governance/state` exposes the governance control-plane state
>   - committed membership epochs are snapshotted as `n2_cluster_membership.epoch-<N>.json` for rollback
> - Internal N2 peer traffic now validates the current membership epoch and cluster fingerprint, so stale or drifted nodes reject object/session/map participation instead of silently staying in the path.
> - Under `strict_full_placement`, writes with too few active nodes are rejected before ingest with `400 insufficient active nodes: need <kc+mc>, have <active>`.
> - On a low-latency LAN, a lower `remote_request_timeout_ms` is usually a better degraded-mode default than the conservative wide-area timeout.
> - Startup now rejects operator-dangerous multi-node layouts such as placeholder internal tokens, duplicate active endpoints, missing membership files, and loopback-only peer endpoints.

> Engine profile note:
> - `n1` is the stable/default profile for general-purpose filesystem-backed deployments.
> - `n1-arc` is the experimental raw-device profile for absolute device control and maximum performance ceiling.
> - Both variants are intended to keep the same object/API semantics; the difference is backend execution model and operations envelope.

> Metadata architecture note:
> - `index` is the authoritative object, trash, and upload-session metadata plane.
> - Each object or session hashes to one metadata partition with one owner root plus one replica root when multiple index roots exist.
> - The owner root is the synchronous ACK boundary for object/session writes.
> - Replica roots are replica-behind durability copies and may lag briefly until repair/replay catches them up.
> - Engine-side object and trash listing are flat scans over authoritative latest-object state.
> - Legacy mirrored object/session trees are no longer the hot-path authority.
> - Same-key upload recency comes from the upload session's `created_at`; finalize does not get to reorder versions.
> - If two versions share the same second-level timestamp, ULID `version_id` order breaks the tie.
> - Exact-key reads resolve the authoritative object head and then rehydrate the referenced immutable version record before serving metadata or bytes.
> - Multipart uploads do not rely on in-memory part state for resumability. Part commits are durably recorded in the `index` session state through compact per-part receipts, and finalize assembles the authoritative object metadata from those receipts.

> Release status note:
> - N1 `n1` is stable at `0.9.0`; `n1-arc` is experimental.
> - This document describes the current implementation and current routes.
> - Request/response details, endpoint layout, and metadata layout may still change before `1.0.0` even though the underlying storage model is now fixed.
> - Backward compatibility is not guaranteed on the `0.x` line; destructive redesigns are allowed until `1.0.0`.

## 0. Profile Distinction And Disk Layout

| Dimension | N1 (`litiaina-n1`) | N1-Arc (`litiaina-n1-arc`) |
|---|---|---|
| Status | Stable / default (`0.9.0`) | Experimental / raw-device profile |
| API source of truth | Yes (`n1/src/routes/storage_routes.rs`) | Yes (`n1-arc/src/routes/storage_routes.rs`) |
| `index_store_disks` | Filesystem directories | Raw block devices / loopback files |
| `blob_store_disks` | Filesystem directories | Raw devices / loopback files |
| Index metadata layout | Partitioned `indexdb` metadata (`.ndxdb`) | Raw index catalog on block-manager media |
| Blob payload layout | Shard files on blob roots | Raw extents via `block_manager` |

Standard disk layout:

```text
{INDEX_ROOT}/{fragment}/indexdb/partitions/{partition_id}.ndxdb
{BLOB_ROOT}/{fragment}/blob/{version_id}/part_{p}_shard_{i}.shard|.parity
{BLOB_ROOT}/{fragment}/.trash/blob/{version_id}/...
```

N1-Arc disk layout:

```text
{RAW_INDEX_DEVICE} -> block_manager allocator/journal metadata + raw index catalog records
{RAW_BLOB_DEVICE} -> block_manager superblock/journals/allocation groups
payload address: raw://{device_id}/{allocation_id}
```

N1-Arc index note:
- `n1-arc` index and blob are both raw block-manager media.
- `n1-arc` is experimental.

## 1. Base URL And Conventions
- Base URL example: `https://127.0.0.1:50001`
- Ping: `GET /ping` -> `pong`
- Protected v4 APIs require: `Authorization: Bearer <fragment_jwt>`
- Most fragment-scoped routes enforce `claims.fragment == {fragment}`
- Object key path captures use `{*object_key}` and support slash-separated keys
- Range requests use standard `Range: bytes=start-end`

## 1.1 Registry Runtime (`registry-server`)

Registry base URL example:
- `https://127.0.0.1:51001`

Startup note:
- if `registry.env` is missing in the current working directory, the Registry creates a template automatically and then stops on the first required unset value

Registry routes available in `0.4.0`:
- `GET /health`
- `GET /v1/ws`
- `POST /v1/admin/bootstrap`
- `POST /v1/admin/auth/login`
- `POST /v1/admin/users`
- `GET /v1/admin/users`
- `GET /v1/admin/stats`
- `POST /v1/admin/users/{uid}/quota`
- `POST /v1/admin/users/{uid}/credentials`
- `POST /v1/admin/users/{uid}/2fa/setup`
- `POST /v1/admin/users/{uid}/2fa/disable`
- `DELETE /v1/admin/users/{uid}`
- `POST /v1/auth/signup`
- `POST /v1/auth/login`
- `POST /v1/auth/refresh`
- `POST /v1/auth/logout`
- `GET /v1/me`
- `POST /v1/account/email`
- `POST /v1/account/password`
- `POST /v1/account/2fa/setup`
- `POST /v1/account/2fa/enable`
- `POST /v1/account/2fa/disable`
- `GET /v1/fragments`
- `POST /v1/fragments/link`
- `POST /v1/storage/session`
- `POST /v1/objects/register`
- `POST /v1/objects/finalize`
- `POST /v1/objects/archive`
- `GET /v1/objects/suggest`
- `POST /v1/objects/rename`
- `POST /v1/objects/trash`
- `POST /v1/objects/recover`
- `POST /v1/objects/recover-all`
- `POST /v1/objects/trash/empty`
- `DELETE /v1/objects`
- `GET /v1/objects`
- `GET /v1/objects/stats`
- `GET /v1/objects/resolve`

Registry responsibilities:
- first-administrator bootstrap guarded by `REGISTRY_MASTER_KEY`
- administrator identity, user administration, and quota control
- administrator-created user/account provisioning plus normal user login/signup
- rotating browser/client refresh-session management
- owner-scoped and admin-wide realtime websocket events for Registry-backed clients and admin surfaces
- self-service account changes (email, password, optional 2FA)
- registry-wide statistics aggregation for the admin control plane
- user quota accounting
- ownership metadata
- logical listing/filtering
- prefix/delimiter browsing for Registry-backed clients
- filtered object statistics for Registry-backed clients and admin-facing insights
- prefix/search archive selection resolved into exact-key manifests for the storage engine
- fragment binding to a `single_node` or `multi_node` storage backend
- brokering short-lived storage sessions for Registry-backed clients and future GUI/web surfaces
- exact-key logical resolution before talking to the storage engine

Current Registry backing store:
- SQLite in both single-node and multi-node deployments

Current product surface:
- CLI + HTTP API are the stable shipped interfaces in this repository today
- bundled web/admin surfaces exist for development, but they are still under development and are not part of the stable N2 release contract yet

Realtime websocket contract:
- route: `GET /v1/ws?token=<registry_access_token>`
- auth: same Registry JWT access token issued by login/signup/refresh/bootstrap
- envelope:
  - `HELLO`
  - `PONG`
  - `EVENT`
- current event families:
  - user/account/admin mutations
  - fragment link/invalidation events
  - object register/finalize/rename/trash/recover/delete events
  - state invalidation events for bulk or lagged consumers

### 1.1.1 `POST /v1/admin/bootstrap`
Purpose:
- creates the first Registry administrator
- only works while the Registry has zero administrator accounts
- uses `REGISTRY_MASTER_KEY` as the trust anchor for first-admin creation

Request shape:
```json
{
  "email": "admin@example.com",
  "password": "password123",
  "master_key": "<REGISTRY_MASTER_KEY>"
}
```

Response shape:
- same token/user envelope as normal login, including refresh token rotation support

Notes:
- once an administrator already exists, this route returns conflict
- bootstrap also provisions the caller's default managed fragment binding

### 1.1.2 `POST /v1/admin/auth/login`
Purpose:
- signs an administrator into the Registry control plane

Request shape:
```json
{
  "email": "admin@example.com",
  "password": "password123",
  "otp": "123456"
}
```

Notes:
- `otp` is only required when the account has 2FA enabled
- successful login returns both access and refresh tokens

### 1.1.3 Registry session lifecycle
Routes:
- `POST /v1/auth/login`
- `POST /v1/auth/signup`
- `POST /v1/auth/refresh`
- `POST /v1/auth/logout`

Auth response shape:
```json
{
  "access_token": "<jwt>",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_token": "<opaque_refresh_token>",
  "refresh_expires_in": 604800,
  "user": {
    "uid": "<uid>",
    "email": "user@example.com",
    "is_admin": false,
    "two_factor_enabled": false,
    "quota_limit_bytes": 26843545600,
    "quota_used_bytes": 0,
    "created_at": "<timestamp>",
    "updated_at": "<timestamp>"
  }
}
```

Notes:
- refresh tokens are server-tracked and rotate on every successful refresh
- `POST /v1/auth/logout` revokes the current refresh chain
- user and admin login both accept `otp` when 2FA is enabled
- `POST /v1/auth/signup` only accepts `email` and `password`; quota is always server-assigned and cannot be chosen by the caller

### 1.1.4 Self-service account routes
Routes:
- `POST /v1/account/email`
- `POST /v1/account/password`
- `POST /v1/account/2fa/setup`
- `POST /v1/account/2fa/enable`
- `POST /v1/account/2fa/disable`

Important behavior:
- email change requires current password and, when enabled, the current OTP
- password change requires current password and, when enabled, the current OTP
- email change will also rename the Registry-managed default fragment when the binding matches a managed naming scheme
- disabling 2FA requires the current password and, when already enabled, the current OTP

TOTP profile:
- algorithm: `HMAC-SHA1`
- digits: `6`
- step: `30 seconds`
- verification window: `±1` step

### 1.1.5 Admin user-management routes
Routes:
- `POST /v1/admin/users`
- `GET /v1/admin/users`
- `GET /v1/admin/stats`
- `POST /v1/admin/users/{uid}/quota`
- `POST /v1/admin/users/{uid}/credentials`
- `POST /v1/admin/users/{uid}/2fa/setup`
- `POST /v1/admin/users/{uid}/2fa/disable`
- `DELETE /v1/admin/users/{uid}`

Notes:
- these routes require an administrator bearer token
- `POST /v1/admin/users` can create either normal users or additional admins, and can optionally override the default quota for the new account
- admin-created accounts automatically get the normal Registry-managed default fragment/bootstrap path
- `GET /v1/admin/stats` returns:
  - global quota and usage totals
  - file-type distribution
  - largest ready objects
  - currently active users and sessions
  - top storage-consuming users
- admin 2FA setup returns a secret, otpauth URI, and QR PNG data URL for the target user
- deleting a user removes Registry state and deletes linked managed fragments through the storage engine before the account row is removed

Example `POST /v1/admin/users` request:
```json
{
  "email": "staff@example.com",
  "password": "temporary-password",
  "quota_limit_bytes": 53687091200,
  "is_admin": false
}
```

Example `GET /v1/admin/stats` response shape:
```json
{
  "generated_at": "2026-03-21T12:34:56Z",
  "totals": {
    "total_users": 42,
    "admin_users": 2,
    "two_factor_enabled_users": 5,
    "users_using_storage": 17,
    "total_quota_bytes": 1099511627776,
    "total_used_bytes": 268435456000,
    "total_fragments": 42,
    "ready_objects": 12844,
    "trashed_objects": 103,
    "pending_uploads": 2,
    "active_sessions": 7,
    "active_users": 4
  },
  "file_types": [],
  "largest_objects": [],
  "active_users": [],
  "top_storage_users": []
}
```

### 1.1.5.1 `GET /v1/objects/stats`
Purpose:
- returns full Registry-side object totals for the caller's current logical scope
- intended for Registry-backed client insights panels without relying on the currently paginated page only

Query parameters:
- `fragment`
- `prefix`
- `search`
- `include_pending`
- `include_trashed`

Response shape:
```json
{
  "total_objects": 1204,
  "total_size_bytes": 987654321,
  "extension_stats": [
    {
      "extension": "dcm",
      "object_count": 903,
      "total_size_bytes": 812345678
    }
  ]
}
```

Notes:
- this route is authenticated with the caller's normal Registry bearer token
- totals are computed across the full filtered Registry scope, not just the current UI page

### 1.1.6 `GET /v1/fragments`
Purpose:
- lists the caller's known fragments
- returns any saved fragment backend bindings for Registry-backed clients

### 1.1.7 `POST /v1/fragments/link`
Purpose:
- binds a fragment to the storage backend a Registry-backed client should use

Request shape:
```json
{
  "fragment": "<fragment_hash>",
  "deployment": "single_node",
  "engine_url_override": "https://optional-backend.example"
}
```

Notes:
- `deployment` must be either `single_node` or `multi_node`
- `engine_url_override` is optional; if omitted, the Registry uses its configured default backend for that deployment type

### 1.1.8 `POST /v1/storage/session`
Purpose:
- asks the Registry to mint a short-lived fragment access session against the linked storage backend
- used by Registry-backed clients for upload/download/rename/delete/recover without exposing fragment secrets in the client

Response shape:
```json
{
  "fragment": "<fragment_hash>",
  "deployment": "single_node",
  "engine_url": "https://storage.example",
  "access_token": "<fragment_jwt>",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_token": "<engine_refresh_token>",
  "refresh_expires_in": 604800
}
```

Notes:
- engine refresh fields are optional and only appear when the backend returns them

## 2. Auth APIs

Auth response shape for both fragment and FS login now includes:

```json
{
  "access_token": "<jwt>",
  "token_type": "Bearer",
  "expires_in": 900,
  "refresh_token": "<opaque_refresh_token>",
  "refresh_expires_in": 604800
}
```

Notes:
- `access_token` is still a bearer JWT used on protected routes.
- `refresh_token` is server-tracked, rotates on every successful refresh, and can be revoked explicitly.
- Current default refresh TTL is `7 days` (`REFRESH_TOKEN_TTL_SECS`).
- Current default access-token TTL is `15 minutes` (`ACCESS_TOKEN_DEFAULT_TTL_SECS=900`).
- Requested `expires_in` is capped at `1 hour` by default (`ACCESS_TOKEN_MAX_TTL_SECS=3600`).

### 2.1 `POST /authenticate/fragment`
Purpose:
- Issues fragment-scoped JWT for `/v4/*` protected APIs.
- Supports two auth modes:
  - Fragment mode: `secret=<fragment_secret>` and `fragment=<fragment_hash>`
  - Master mode: `secret=<MASTER_KEY>` with any non-empty `fragment` auth context

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
    "fragment": "maintenance-context",
    "secret": "<MASTER_KEY>",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }'
```

Notes:
- Master mode no longer requires `ADMIN_KEY`; `MASTER_KEY` alone is the privileged path.
- `litiaina-n1-cli` is the dedicated n1 admin CLI; `litiaina-n1` now runs the server only.
- `litiaina-n2-cli` is the dedicated n2 admin CLI; `litiaina-n2` now runs the server only.
- CLI semantics are split: `--auth-fragment` controls auth context, while `--fragment` is only passed to maintenance start endpoints that support explicit fragment targeting.
- N1 CLI watch mode is line-oriented and low-overhead: health prints one compact line per poll, and job watches only print when status changes.
- Health responses now include per-disk total, used, available, usage percentage, and current disk state for each configured index/blob path.
- Fragment auth now also mints a refresh token for session continuation.
- `litiaina-n1-cli` and `litiaina-n2-cli` automatically use that refresh token in-memory, so long-running watch/poll sessions can keep operating without manual re-authentication.
- If `expires_in` is omitted, the server issues a `15 minute` access token by default.
- If `expires_in` is larger than the configured maximum, the server clamps it before minting the token.

### 2.2 `POST /authenticate/fs` (n1 only)
Purpose:
- Issues legacy FS auth context for `/dir/*` routes.

```bash
curl -k -X POST "https://127.0.0.1:50001/authenticate/fs" \
  -H "Content-Type: application/json" \
  -d '{
    "path":"/",
    "secret":"<AUTH_KEY>",
    "read":true,
    "write":false,
    "modify":false,
    "delete":false
  }'
```

Notes:
- FS auth now also returns a refresh token with the same response shape.
- The same access-token default/max TTL rules apply to FS auth.

### 2.3 `POST /authenticate/refresh`
Purpose:
- Rotates a refresh token and returns a new access token plus a new refresh token.
- Works for both fragment-scoped auth and FS auth sessions.

```bash
curl -k -X POST "https://127.0.0.1:50001/authenticate/refresh" \
  -H "Content-Type: application/json" \
  -d '{
    "refresh_token":"<refresh_token>"
  }'
```

Notes:
- Refresh tokens are one-time rotating tokens.
- Reuse of an already-rotated refresh token revokes the session.

### 2.4 `POST /authenticate/logout`
Purpose:
- Revokes the current refresh session.
- Returns `204 No Content` on success.

```bash
curl -k -X POST "https://127.0.0.1:50001/authenticate/logout" \
  -H "Content-Type: application/json" \
  -d '{
    "refresh_token":"<refresh_token>"
  }'
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
  "format": "tar",
  "filename": "export",
  "export_all": false,
  "include_internal": false
}
```
Notes:
- `keys` can be an explicit list of exact object keys.
- Or set `export_all=true` to stream the entire fragment in flat engine order.
- Registry-backed archive flows can still resolve exact keys first, but the N1 engine now also
  supports authoritative `prefix` and `delimiter=/` listing directly from partition head rows.

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
    "secret":"<master_key>",
    "key":"<fragment_key>"
  }'
```

Notes:
- Fragment creation only requires the fragment name, fragment key, and master secret.
- Live storage monitoring and `/v4/capacity/usage` report real filesystem totals for the current node.

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
For claim-scoped object APIs below, the fragment comes from the JWT claim and is not passed in the path.

### 6.1 `GET /v4/capacity/usage`
Returns live filesystem capacity totals visible to the current `n1` node.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/capacity/usage"
```

Response shape:
```json
{
  "capacity_used": 123456789,
  "capacity_allocated": 999999999,
  "capacity_available": 876543210,
  "filesystem_count": 2
}
```

### 6.2 `GET /v4/fragment/list`
Lists fragment usage objects visible to claims.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/list"
```

### 6.3 `GET /v4/fragment/objects`
Query:
- `limit` default `100`, max `1000`
- `continuation_token` optional
- `prefix` optional
- `delimiter` optional (`/`)
- `include_internal` optional (`true|false`, default `false`)

Notes:
- Internal marker keys (for example `.n1fsd_dir`, `.n1-trash`) are hidden unless `include_internal=true`.
- Listing reads authoritative `object_heads` rows from routed `indexdb` partition files.
- `prefix` is supported directly by the engine.
- `delimiter=/` is supported directly by the engine and returns folded folder-style entries in `common_prefixes`.
- `continuation_token` is opaque and must be passed through unchanged.
- Registry still owns higher-level business filtering and owner-scoped logical listing above storage truth.

Response shape (`ListObjectsResult`):
- `objects`: array of `ObjectMetadata`
- `common_prefixes`: optional folded prefixes when `delimiter=/`
- `continuation_token`: optional

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/objects?limit=200"
```

### 6.4 `GET /v4/fragment/trash`
Trash listing, same pagination style.
- Supports the same `include_internal` query behavior as object listing.
- Trash visibility comes from authoritative `object_heads` trash state in `indexdb`.
- Supports the same `prefix`, `delimiter=/`, and `continuation_token` behavior as live listing.
- `continuation_token` is opaque and must be passed through unchanged.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/trash?limit=100"
```

### 6.5 `GET /v4/fragment/versions/{*object_key}`
Lists versions for one object key.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/versions/videos/a.mp4"
```

### 6.6 `GET /v4/fragment/get/{*object_key}`
Inline object response. Supports range.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  -H "Range: bytes=0-1048575" \
  "https://127.0.0.1:50001/v4/fragment/get/videos/a.mp4" \
  -o part.bin
```

### 6.7 `GET /v4/fragment/stream/{*object_key}`
Streaming variant. Supports range.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  -H "Range: bytes=0-1048575" \
  "https://127.0.0.1:50001/v4/fragment/stream/videos/a.mp4" \
  -o stream.bin
```

### 6.8 `GET /v4/fragment/meta/{*object_key}`
Returns full object metadata.
- Metadata is resolved from authoritative `index` state.
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/meta/videos/a.mp4"
```

### 6.9 `GET /v4/fragment/thumb/{*object_key}`
Returns a derived image thumbnail.

Query:
- `w` optional, default `256`, max `2048`
- `h` optional, default `w`
- `fit` optional: `contain|cover`, default `contain`
- `format` optional: `jpeg|png`, default `jpeg`
- `version_id` optional: generate thumbnail for a specific immutable object version

Behavior:
- Validates fragment scope and resolves authoritative object metadata.
- Only supports image objects.
- Uses a disk-backed derived cache keyed by `fragment + version_id + thumbnail profile`.
- Does not create or publish a separate user object.

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/thumb/images/a.png?w=256&h=256&fit=contain&format=jpeg" \
  -o a-thumb.jpg
```

### 6.10 `GET /v4/fragment/download`
Query:
- `object_key` required
- `version_id` optional

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/download?object_key=videos/a.mp4" \
  -o a.mp4
```

### 6.11 `GET /v4/fragment/download/chunk`
Query:
- `object_key` required
- `offset` required
- `chunk_size` required

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/download/chunk?object_key=videos/a.mp4&offset=0&chunk_size=1048576" \
  -o chunk.bin
```

### 6.11 `POST /v4/fragment/archive`
Purpose:
- Streams selected objects as `tar` or `zip`.
- Memory bounded; does not materialize whole archive in RAM.

Behavior:
- If selection is very large (>1000 keys), server forces `tar`.
- If `export_all=true`, server scans the fragment in flat engine order and forces `tar`.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/archive" \
  -d '{
    "keys":["videos/2026/a.mp4","videos/2026/b.mp4"],
    "format":"tar",
    "filename":"videos-export"
  }' \
  -o videos-export.tar
```

### 6.11.1 `POST /v4/fragment/archive/manifest`
Purpose:
- Streams a tar archive from a newline-delimited manifest of exact object keys.
- Used by the Registry to stream exact-key archive selections from its own logical catalog, while N1 also supports authoritative `prefix` / `delimiter=/` listing directly from `indexdb` partition heads.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  --data-binary @keys.lst \
  "https://127.0.0.1:50001/v4/fragment/archive/manifest?format=tar&filename=videos-export" \
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
- `new_key` optional
- `secret` optional (master secret for admin-level actions)

```bash
curl -k -X PATCH -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/modify/<fragment_hash>" \
  -d '{"new_key":"<new_fragment_key>"}'
```

### 7.3 `DELETE /v4/fragment/trash/empty`
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/trash/empty"
```
Notes:
- Empty-trash clears partition trash metadata and removes `.trash` roots directly (`remove_dir_all`) for O(1)-style namespace purge on filesystem-backed `n1`.
- Live storage reporting is derived from current filesystem totals on the node.

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

### 7.5 `DELETE /v4/fragment/object/delete/{*object_key}`
Soft delete (move to trash flow).
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/object/delete/videos/a.mp4"
```

### 7.6 `POST /v4/fragment/object/bulk-delete`
Bulk soft delete for many objects in one request. This is the preferred path for UI or operator actions that need to delete hundreds or thousands of keys at once.

Body:
```json
{
  "object_keys": [
    "videos/a.mp4",
    "videos/b.mp4"
  ]
}
```

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/object/bulk-delete" \
  -d '{"object_keys":["videos/a.mp4","videos/b.mp4"]}'
```

Response:
- `requested`
- `unique`
- `succeeded`
- `not_found`
- `failed[]`

### 7.7 `DELETE /v4/fragment/version/delete/{version_id}/{*object_key}`
Delete specific version.
- If the version is live, it is soft-deleted into trash.
- If the same version is already in trash, the delete permanently removes only that trashed version and leaves newer live versions untouched.
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/version/delete/<version_id>/videos/a.mp4"
```

### 7.8 `DELETE /v4/fragment/permanent/delete/{*object_key}`
Permanent delete.
```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/permanent/delete/videos/a.mp4"
```

### 7.9 `POST /v4/fragment/recover/version/{version_id}/{*object_key}`
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/recover/version/<version_id>/videos/a.mp4"
```

### 7.10 `POST /v4/fragment/recover/object/{*object_key}`
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/recover/object/videos/a.mp4"
```

### 7.11 `POST /v4/fragment/object/bulk-recover`
Bulk restore for many objects in one request.

Body:
```json
{
  "object_keys": [
    "videos/a.mp4",
    "videos/b.mp4"
  ]
}
```

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/fragment/object/bulk-recover" \
  -d '{"object_keys":["videos/a.mp4","videos/b.mp4"]}'
```

Response:
- `requested`
- `unique`
- `succeeded`
- `not_found`
- `failed[]`

### 7.12 `POST /v4/fragment/recover/trash/all`
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/fragment/recover/trash/all"
```
Notes:
- Recover-all restores from authoritative partition trash metadata and moves `.trash/blob/{version}` back to live blob dirs per version.
- Recovery follows direct move semantics and does not perform any separate capacity bookkeeping.

## 8. Protected v4 Upload APIs (Resumable Object Storage Upload)

### Upload flow
1. Initiate session
2. Upload parts (`part_index`)
3. Poll status
4. Finalize
5. Optional cancel

### 8.1 `POST /v4/upload/init/{*object_key}`
Body:
- `content_type` optional
- `total_size_bytes` required
- `expected_parts` required
- `part_size_bytes` optional: freezes deterministic part geometry for fixed-size multipart uploads
- `part_sizes_bytes` optional: freezes deterministic per-part geometry explicitly

Behavior:
- Opens the authoritative multipart session in `index`.
- Persists upload manifest/session state on index roots.
- Explicit geometry (`part_size_bytes` / `part_sizes_bytes`) is accepted when provided by the client.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/upload/init/videos/a.mp4" \
  -d '{"content_type":"video/mp4","total_size_bytes":1073741824,"expected_parts":256,"part_size_bytes":4194304}'
```

### 8.2 `POST /v4/upload/one-shot/{*object_key}` (`n1`)
Direct single-request upload for small objects. This path skips upload session creation, part tracking, and finalize.

Headers:
- `Content-Type` optional
- `Content-Length` required

Query:
- `checksum` optional
- `durability` optional: `strict|sync|durable|batched|async|fast|relaxed`
- `strict_durability` optional bool fallback (`true|false`)
- `io_gate` optional bool (`true|false`)

Behavior:
- Validates fragment scope and request size.
- Writes shard payload once.
- Publishes final metadata and latest immediately.
- Leaves no active upload session behind.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/octet-stream" \
  -H "Content-Length: 8192" \
  "https://127.0.0.1:50001/v4/upload/one-shot/tiny.bin?durability=batched&io_gate=false" \
  --data-binary @tiny.bin
```

### 8.3 `PUT /v4/upload/chunk/{version_id}/{part_index}`
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
- Part upload commits shard payload, persists part metadata on disk, then records committed-part state in `index` session state.
- If upload I/O gating is enabled and the node cannot admit more upload work within the gate timeout, the request is rejected with `503 Service Unavailable` instead of proceeding ungated.
- The upload hot path does not rely on in-memory part state for resumability.
- In `n1`, shards are written as files under blob root directories.
- In `n1-arc`, shards are committed to raw extents.
- Multipart part commits are unordered across distinct `part_index` values.
- Client coordination should keep one logical writer per `(version_id, part_index)` and use upload-status checks for stateless resume.
- A successful part upload does not publish object visibility by itself.

```bash
curl -k -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Length: 4194304" \
  "https://127.0.0.1:50001/v4/upload/chunk/<version_id>/0?checksum=<optional_crc_or_hash>&durability=batched&io_gate=false" \
  --data-binary @part-000.bin
```

### 8.4 `GET /v4/upload/status/{version_id}`
Behavior:
- Reads the routed upload manifest and committed part set from `index` session state (with disk metadata fallback when needed).
- Returns committed part indexes plus committed byte/part counters for resumability checks.

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/upload/status/<version_id>"
```

### 8.5 `GET /v4/upload/status/list`
Query:
- `limit` default 50, max 100
- `offset` default 0

Behavior:
- Lists active routed multipart sessions from `index` and reports committed progress from session state.
- Includes committed part/byte progress per session.
- Sessions that are cancelled through `DELETE /v4/upload/cancel/{version_id}` disappear from this listing immediately after the cancel completes.

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/upload/status/list?limit=50&offset=0"
```

### 8.6 `POST /v4/upload/finalize/{version_id}/{session_id}`
Behavior:
- Loads the authoritative upload session from `index`.
- Verifies committed parts from session state (with disk metadata fallback for legacy sessions) and publishes the immutable object version into the object's `index` partition.
- Exact-key visibility is gated by this publish step.
- Prefix listing is serving state layered on top of the publish and may be rebuilt from `index`.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://127.0.0.1:50001/v4/upload/finalize/<version_id>/<session_id>" \
  -d '{"content_type":"video/mp4","total_size_bytes":1073741824,"expected_parts":256}'
```

### 8.7 `DELETE /v4/upload/cancel/{version_id}`
Response:
- `204 No Content` on successful cancel.
- If already finalized, returns conflict.

Behavior:
- Aborts the active multipart session for that `version_id`.
- Removes the session from `GET /v4/upload/status/list`.
- Frees the reserved upload budget tracked by the engine.
- Bundled upload-recovery UIs now use this route for interrupted upload cancellation.
- In the Registry-backed Drive surface, cancellation also triggers Registry-side pending reservation release before the recovery card is cleared.

```bash
curl -k -X DELETE -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/upload/cancel/<version_id>"
```

## 9. Protected v4 Maintenance APIs

### 9.1 Fragment bootstrap / cleanup endpoints
- `POST /v4/maintenance/cleanup/fragment`
- `POST /v4/maintenance/init/fragment`

Notes:
- `cleanup/fragment` removes orphaned blob directories and reconciles fragment metadata shape.
- `cleanup/fragment` now returns a structured result that includes the fragment id, current usage bytes, orphan blob directories removed, and a human-readable message.
- `init/fragment` ensures fragment blob directories exist and clears stale `.gc` files.
- Live storage monitoring and `/v4/capacity/usage` use current filesystem totals.

```bash
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
- `check_mode=availability|scrub|strict|checksum` (alias of `mode`)
- `execution_mode=availability|scrub|strict|checksum` (alias of `mode`)
- `access_mode=availability|readonly|read_only|quiesced` (accepted fallback when `mode` is omitted)
- `fast_mode=true|false` (scheduler tuning)
- `read_only_fast=true|false` (when set with availability mode, upgrades access behavior to read-only/strict)
- `meta_concurrency=1..N` (RAM-adaptive cap; low-RAM nodes clamp lower than high-RAM nodes)
- `repair_concurrency=1..N` (RAM-adaptive cap)
- `batch=32..N` (RAM-adaptive cap)

Notes:
- API compatibility: endpoint path/method/response shape did not change; this update only broadened accepted query aliases and tightened mode-to-access enforcement.
- Repair reconciles partition replicas and blob availability from `index`.
- Full mirrored equality across every index root is no longer the invariant; owner/replica survivability for each partition is.
- `mode=availability` keeps CRUD online and intentionally caps repair pressure lower so the node stays latency-friendly.
- Degraded reads stay online-first: missing/corrupt shard blocks are reconstructed on-the-fly for the response, while full-shard rebuild is queued in bounded background work (no foreground read blocking on full repair).
- `mode=strict` makes the node read-only and raises repair aggressiveness to use more of the available I/O width.
- `mode=scrub` quiesces reads and writes and runs the same integrity-first repair engine at the highest practical aggressiveness.
- Runtime concurrency is normalized against live blob disks and live index roots, so auto-tuning follows actual reachable hardware instead of static CPU counts alone.
- Fragment object-version scans are streamed incrementally from routed `indexdb` partition rows, so repair no longer depends on any legacy per-object metadata files.
- Log severity policy for repair/recovery paths: expected retry/remap steps emit `DEBUG`, successful per-object summaries emit `INFO`, degraded-but-continued outcomes emit `WARN`, and unrecoverable read failures emit `ERROR`.
- Repair/scrub dry-run no longer aborts when `.raw_gc` markers reference a decommissioned device id; those markers are kept and treated as deferred cleanup state.

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
- `POST /v4/maintenance/gc/stop`
- `GET /v4/maintenance/gc/status`

Notes:
- GC deletes trashed versions and abandoned upload sessions through `index`.
- GC is partition-aware for both live/trash metadata and session cleanup (no dependency on legacy `index/sessions` or `{hash}#{version}` naming).
- GC stop is cooperative: the current scan loop exits at safe checkpoints and reports `phase=Stopped by User`.
- Raw payload without authoritative session or object state is cleanup-candidate data.
- If raw-GC markers reference a device id that is no longer configured (for example after disk decommission), GC defers those extents and logs a single summary `INFO`; markers are retained for later cleanup if the device is reintroduced.
- Raw extent cleanup updates allocator metadata in batch journal commits; payload zeroing is not the authority for free state.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/gc/start"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/gc/status"

curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/gc/stop"
```

### 9.4 Scrub endpoints
- `POST /v4/maintenance/scrub/start`
- `POST /v4/maintenance/scrub/stop`
- `GET /v4/maintenance/scrub/status`

Scrub runs the repair engine in checksum-verification mode against committed shards.

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
- `check_mode=availability|strict|scrub` (alias of `mode`)
- `execution_mode=availability|strict|scrub` (alias of `mode`)
- `access_mode=availability|readonly|read_only|quiesced` (accepted fallback when `mode` is omitted)
- `fast_mode=true|false` (scheduler tuning)
- `read_only_fast=true|false` (when set with availability mode, upgrades execution/access behavior to strict/read-only)
- `scope=all|blob|index`
- `durability=auto|strict|batched` (aliases: `sync|durable|async|fast|relaxed`)
- `durability_mode=auto|strict|batched` (alternate key)
- `strict_durability=true|false` (boolean fallback for durability)
- `object_concurrency=1..N` (RAM-adaptive cap; low-RAM nodes clamp lower)
- `status_batch=1..N` (RAM-adaptive cap)

Notes:
- API compatibility: endpoint path/method/response shape did not change; this update only broadened accepted query aliases and tightened execution-mode parsing.
- Redistribution rewrites payload placement first, then republishes updated shard addresses through authoritative `index` metadata.
- `scope=all` performs blob+index updates in one run.
- `scope=blob` rewrites payload placement and republishes touched metadata, but does not run index-only rebalancing for untouched objects.
- `scope=index` re-publishes metadata to the current routed index layout only; blob payload is not rewritten.
- `mode=availability` keeps CRUD online and intentionally throttles object concurrency lower so redistribution does not consume the full device bandwidth.
- `mode=strict` makes the node read-only and drives redistribution harder across the live blob/index layout.
- `mode=scrub` quiesces reads and writes and uses the widest redistribution concurrency that the live layout and RAM caps allow.
- Execution `mode` controls service access and scheduler aggressiveness; `durability` still separately controls strict-vs-batched write sync behavior.
- Fragment object-version scans are streamed incrementally from partition metadata paths first, with legacy scan paths used only as compatibility fallback when partition layout is absent.
- Redistribution now follows the same log-severity contract as repair/recovery (`DEBUG` expected retries, `INFO` successful migration summaries, `WARN` degraded-but-continued failures, `ERROR` unrecoverable read failures).
- Cancellation is cooperative and crash-safe at object publish boundary: object tasks returning `Stopped` are treated as stop outcomes, not hard migration failures.
- Object-level redistribution intent markers are persisted on index roots; pending intents are replayed first on the next redistribution run so interrupted large-object migrations can resume safely.

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/start?dry_run=false&scope=all&fast_mode=true&durability=batched&object_concurrency=32&status_batch=128"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/monitor"

curl -k -X PATCH -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/stop"
```

### 9.6 Raw blob status endpoint (n1-arc only)
`GET /v4/maintenance/blob/raw/status`

Notes:
- This endpoint is exposed in `n1-arc`.
- `n1` route set does not expose `/v4/maintenance/blob/raw/status`.

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

Status semantics:
- `managed` or `unformatted`: usable by runtime.
- `unavailable`: configured path cannot currently be opened (for example hot-unplug/stale by-id path).
- `misconfigured`: path exists but violates raw-disk requirements (for example mounted directory or foreign/dirty media state).

Degraded-path behavior:
- The endpoint returns per-disk status rows even when one path is unavailable; it no longer fails the whole request because of a single missing disk path.
- Runtime disk resolution falls forward to the next usable configured blob disk when the selected disk slot is unavailable.

```bash
curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/blob/raw/status"
```

### 9.7 System health endpoint
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
- Health now also exposes:
  - maintenance access mode (`availability`, `read_only`, `quiesced`)
  - stale multipart session count
  - stale reserved bytes
  - pending/failed redistribution intent debt

```bash
curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/health"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/health?refresh=true"

curl -k -X GET -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/health?deep_scan=true"
```

### 9.7.1 Filesystem-backed 2-disk real-hardware validation
Validated on March 26, 2026 against the IT server `192.168.0.18` using an isolated N1 harness on real `/mnt/DISK2` and `/mnt/DISK3` media with `1D+1P`.

Validated outcomes:
- CRUD + versioning: one object was written twice under the same key and `GET` served the latest bytes; version listing returned `2`.
- Degraded read path: with one test disk root forced offline, both `GET` and ranged `STREAM` still returned the expected SHA256 from the surviving copy.
- Degraded maintenance behavior: availability-mode redistribution and repair completed without fatal errors while one disk root was offline.
- Policy-cycle redistribution: changing policy from `1D+1P` to `1D+0P` and back completed cleanly; the proof object stayed readable before and after both redistributions.
- Soft delete lifecycle: trashed object appeared in `/v4/fragment/trash`, then manual GC expired it, removed orphan blob dirs, refunded bytes, and subsequent `GET` returned `404`.
- Interrupted upload cancel: active multipart session disappeared from `/v4/upload/status/list` immediately after `DELETE /v4/upload/cancel/{version_id}`.
- Abandoned upload lifecycle: an unfinalized multipart session aged past `file_ttl_seconds`, then GC deleted the session, refunded bytes, and removed all remaining blob dirs.

Reference timings from that run:
- create/write proof object: `683.06 ms`
- redistribution while one disk root was offline: `1010.81 ms`
- repair after restore: `1005.92 ms`
- redistribution `2-disk -> 1-disk`: `1378.93 ms`
- redistribution `1-disk -> 2-disk`: `1386.49 ms`
- soft-delete GC: `4017.16 ms`
- abandoned-upload GC: `4027.15 ms`

Artifact:
- `/home/administrator/DEPLOY_TEST/n1-realtest-20260326-154051-summary.json`

### 9.9 Hotplug HA Validation and Deploy Checklist (arc raw-blob validation)
Validated on March 5, 2026 (real hardware, raw blob devices, hot-unplug and reattach cycle).

Validated outcomes:
- Degraded raw status behavior: when one blob disk was unplugged, `GET /v4/maintenance/blob/raw/status` remained `200` and returned per-disk rows with one disk marked `unavailable` instead of failing the endpoint.
- Degraded CRUD path: upload init/chunk/finalize/read succeeded while one blob disk was unavailable.
- Pre-failure object recovery proof: object written while both disks were healthy remained readable while one disk was unplugged, and both `GET` and `STREAM` checksums matched expected SHA256.
- Degraded maintenance behavior: repair and redistribution in availability mode completed without fatal errors (`last_error=null`).
- Failback behavior: after reattach, both disks returned to `managed`; redistribution (`dry_run=false`) and repair completed with `total_errors=0`/`last_error=null`, and proof-object checksum stayed unchanged.

Deploy smoke test (dev server):
1. Verify both disks are visible and managed:
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/blob/raw/status"
```
2. Run redistribution real pass:
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/start?dry_run=false&mode=availability&fast_mode=true"
```
3. Wait for completion and verify no fatal error:
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/monitor"
```
4. Run repair pass:
```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/repair/start?dry_run=false&mode=availability&fast_mode=true"
```
5. Verify repair status:
```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  "https://127.0.0.1:50001/v4/maintenance/repair/status"
```
6. Confirm read path integrity for one known object using both endpoints:
- `GET /v4/fragment/get/{object_key}`
- `GET /v4/fragment/stream/{object_key}`
- Compare client-side SHA256 with expected.

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

## 11. Legacy FS API (`/dir/*`, n1 only)
These routes are protected by legacy FS auth middleware and are not exposed by `n1-arc`.

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
- `POST /authenticate/fs` (n1 only)
- `POST /authenticate/refresh`
- `POST /authenticate/logout`

## 13. Streaming And Upload Tuning (Config)
`[upload_config]` keys used by streaming read and upload-part paths:
- `upload_memory_budget_bytes`: caps upload-part in-flight working set by reducing effective `max_concurrent_io` at startup (`auto` derives from RAM, override for strict memory ceilings).
- `stream_chunk_size`: maximum emitted stream chunk per read step (4 KiB..4 MiB after clamp/alignment).
- `stream_readahead_bytes`: sequential read-ahead window per stream step (64 KiB..8 MiB after clamp/alignment).
- `stream_locality_bias_enabled`: prefer shard directory inferred from shard absolute path for sequential streaming.
- `stream_max_concurrency`: global stream semaphore capacity.
- `upload_part_strict_durability_default`: default durability for `PUT /v4/upload/chunk` when query override is absent (`true` = strict fsync/dir-sync, `false` = batched, default `false`).
- `upload_io_gate_enabled`: default for upload I/O gate in `PUT /v4/upload/chunk` when `io_gate` query override is absent. When enabled, uploads that cannot acquire gate capacity within the timeout are rejected with `503` to preserve backpressure.

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
- `GET /v4/capacity/usage`
- `GET /v4/fragment/list`
- `GET /v4/fragment/objects`
- `GET /v4/fragment/trash`
- `GET /v4/fragment/versions/{*object_key}`
- `GET /v4/fragment/get/{*object_key}`
- `GET /v4/fragment/stream/{*object_key}`
- `GET /v4/fragment/meta/{*object_key}`
- `GET /v4/fragment/thumb/{*object_key}`
- `GET /v4/fragment/download`
- `GET /v4/fragment/download/chunk`
- `GET /v4/smb/progress`
- `POST /v4/fragment/archive`
- `POST /v4/fragment/archive/manifest`
- `POST /v4/share/create`

### Protected v4 write/upload
- `DELETE /v4/fragment/delete/{fragment}`
- `PATCH /v4/fragment/modify/{fragment}`
- `DELETE /v4/fragment/trash/empty`
- `PATCH /v4/fragment/object/rename`
- `DELETE /v4/fragment/object/delete/{*object_key}`
- `POST /v4/fragment/object/bulk-delete`
- `DELETE /v4/fragment/version/delete/{version_id}/{*object_key}`
- `DELETE /v4/fragment/permanent/delete/{*object_key}`
- `POST /v4/fragment/recover/version/{version_id}/{*object_key}`
- `POST /v4/fragment/recover/object/{*object_key}`
- `POST /v4/fragment/object/bulk-recover`
- `POST /v4/fragment/recover/trash/all`
- `POST /v4/upload/init/{*object_key}`
- `POST /v4/upload/one-shot/{*object_key}`
- `GET /v4/upload/status/{version_id}`
- `GET /v4/upload/status/list`
- `PUT /v4/upload/chunk/{version_id}/{part_index}`
- `POST /v4/upload/finalize/{version_id}/{session_id}`
- `DELETE /v4/upload/cancel/{version_id}`

### Protected v4 maintenance
- `POST /v4/maintenance/cleanup/fragment`
- `POST /v4/maintenance/init/fragment`
- `POST /v4/maintenance/repair/start`
- `POST /v4/maintenance/repair/stop`
- `GET /v4/maintenance/repair/status`
- `POST /v4/maintenance/gc/start`
- `POST /v4/maintenance/gc/stop`
- `GET /v4/maintenance/gc/status`
- `POST /v4/maintenance/redistribution/start`
- `PATCH /v4/maintenance/redistribution/stop`
- `GET /v4/maintenance/redistribution/monitor`
- `GET /v4/maintenance/health`

### Websocket/broadcast
- `GET /ws`
- `POST /v4/system/broadcast`

### Legacy FS (n1 only)
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
- `async_close_commit=true|false` (default `false`; retained for compatibility, ignored in SMB-lite transport mode)
- `async_close_threshold_bytes=<u64>` (default `67108864`; retained for compatibility, ignored in SMB-lite transport mode)
- `auth_username=<string>` optional
- `auth_password=<string>` optional
- `auth_domain=<string>` optional
- `require_signing=true|false`
- `query_directory_page_limit=<16..1000>` (default `1000`)
- `query_directory_max_buffered_entries=<32..8192>` (default `4096`)
- `query_directory_max_seeded_entries=<8..2048>` (default `512`, capped by `query_directory_max_buffered_entries`)

Behavior:
- Preferred auth mode is fragment-scoped:
- SMB `username` is fragment identifier (`<fragment_name>` or `<fragment_hash>`).
- SMB `password` is fragment key.
- Share `N1` maps to the authenticated fragment for the session.
- SMB filesystem capacity reported to clients (for example Windows "Properties") is derived from live filesystem totals on the current node.
- SMB is read-only: browse/list/open/read/query only.
- Optional static mode: if `auth_username` and `auth_password` are configured, that static account is also accepted.
- If `require_signing=true`, SMB signing is required.
- Live monitoring and SMB capacity reporting use current filesystem totals.

### 14.2 SMB Transfer Progress API
`GET /v4/smb/progress`

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
