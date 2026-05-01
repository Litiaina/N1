# Registry API Documentation

`Registry` is the user-facing logical metadata and policy layer that sits above `N1` or `N2`.

Use `Registry` when you need:

- user accounts
- admin bootstrap and quota management
- user-facing policy around an `N1` or `N2` logical object namespace
- storage session brokering into `N1` or `N2`
- trash, recover, rename, and prefix operations at the logical metadata layer
- public shares
- audit/activity
- user metadata overlays such as favorites, tags, labels, and comments

`Registry` is also the correct home for user quotas and account policy. Standalone `N1` is not the user/account quota control plane.

Current `0.9.1` behavior that matters operationally:

- for fragment-bound object listing and stats, Registry reads logical namespace truth from the bound `N1` engine instead of trusting stale Registry projection alone
- for `N1 3.0`, that engine namespace truth is backed by the fragment's sharded `indexdb`, not a separate namespace catalog
- Registry object APIs for bound fragments use the engine namespace as the file-tree authority; Registry keeps a durable business/policy projection
- pending quota reservations are stored durably in `object_uploads` and reconciled against committed object usage during finalize/cancel flows
- shares, audit events, favorites, tags, labels, and comments remain Registry-owned business metadata keyed by logical `object_key`
- Drive/RAD ARC should treat `/v1/objects` results as the DICOM scan source for N1-bound fragments; direct `.dcm` objects and nested `common_prefixes` are both valid folder contents.

This document is written for engineers integrating with `Registry` directly.

## Base URL

Typical base URL:

```text
https://127.0.0.1:7001
```

## Auth Model

Most routes require:

```text
Authorization: Bearer <registry_access_token>
```

Bootstrap is separate and uses the `master_key` field in its request body.

## Core Data Shapes

### User Record

```json
{
  "uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "email": "user@example.com",
  "is_admin": false,
  "two_factor_enabled": false,
  "quota_limit_bytes": 10737418240,
  "quota_used_bytes": 1048576,
  "created_at": "2026-04-12T00:00:00Z",
  "updated_at": "2026-04-12T00:00:00Z"
}
```

### Object Record

```json
{
  "owner_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "storage_object_id": "videos/demo.mp4",
  "version_id": "01KNYR8AB4VYG4JCM5TX9GWV8M",
  "status": "ready",
  "object_size_bytes": 10485760,
  "content_type": "video/mp4",
  "created_at": "2026-04-12T00:00:00Z",
  "updated_at": "2026-04-12T00:00:02Z"
}
```

Important distinction:

- `object_key` is the logical user-facing path
- `storage_object_id` is retained for Registry compatibility and non-engine projections; for `N1` 3.0-bound fragments, Registry addresses N1 by logical `object_key`

### Audit Event Record

```json
{
  "event_id": "2c7b4f5d-2f2f-4c2e-88d8-5346f4a3a4e1",
  "owner_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "actor_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "event_type": "object_overlay_updated",
  "fragment": "fragment-a",
  "object_key": "docs/report.pdf",
  "details_json": "{\"favorite\":true}",
  "created_at": "2026-04-21T00:00:00Z"
}
```

### Object Overlay Record

```json
{
  "owner_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "fragment": "fragment-a",
  "object_key": "docs/report.pdf",
  "favorite": true,
  "tags": ["finance", "review"],
  "labels": ["important"],
  "comment_count": 2,
  "updated_at": "2026-04-21T00:00:00Z"
}
```

### Fragment Binding Record

```json
{
  "owner_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "fragment": "fragment-a",
  "deployment": "single_node",
  "engine_url_override": "https://127.0.0.1:50001",
  "created_at": "2026-04-12T00:00:00Z",
  "updated_at": "2026-04-12T00:00:00Z"
}
```

### Storage Session Response

This is the bridge from `Registry` to `N1` or `N2`.

```json
{
  "fragment": "fragment-a",
  "deployment": "single_node",
  "engine_url": "https://127.0.0.1:50001",
  "access_token": "<fragment-access-token>",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "<optional-fragment-refresh-token>",
  "refresh_expires_in": 604800
}
```

Operational note:

- for standalone `N1 3.0`, `Registry` currently brokers fragment access against the backend engine’s `/n1/auth/fragment` route
- clients should treat the storage session response as the engine-facing contract and should not re-derive engine auth on their own for normal product flows

### Share Record

```json
{
  "share_id": "01KNYRA3T7D7T2D6J9RZ4H7A1D",
  "owner_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
  "fragment": "fragment-a",
  "object_key": "docs/report.pdf",
  "disposition": "download",
  "expires_at": "2026-04-12T06:00:00Z",
  "created_at": "2026-04-12T00:00:00Z",
  "revoked_at": null
}
```

## Health And Bootstrap

### `GET /health`

Response:

```json
{
  "status": "ok",
  "sqlite_path": "/path/to/registry.sqlite3",
  "users": 3,
  "objects": 1024,
  "ready_objects": 1001
}
```

### `POST /v1/admin/bootstrap`

Create the initial admin user.

Request body:

```json
{
  "email": "admin@example.com",
  "password": "strong-password",
  "master_key": "<REGISTRY_MASTER_KEY>"
}
```

Response:

- same token envelope shape as login/signup

## Admin API

### `POST /v1/admin/auth/login`

Admin login.

Request body:

```json
{
  "email": "admin@example.com",
  "password": "strong-password",
  "otp": "123456"
}
```

Response:

```json
{
  "access_token": "<jwt>",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "<refresh>",
  "refresh_expires_in": 604800,
  "user": {
    "...": "UserRecord"
  }
}
```

### `POST /v1/admin/users`

Create a user.

Request body:

```json
{
  "email": "user@example.com",
  "password": "strong-password",
  "quota_limit_bytes": 10737418240,
  "is_admin": false
}
```

Response:

```json
{
  "user": {
    "...": "UserRecord"
  }
}
```

### `GET /v1/admin/users`

List users with admin summaries.

Response:

```json
{
  "users": [
    {
      "uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
      "email": "user@example.com",
      "is_admin": false,
      "two_factor_enabled": false,
      "quota_limit_bytes": 10737418240,
      "quota_used_bytes": 1048576,
      "fragment_count": 1,
      "fragments": ["fragment-a"],
      "created_at": "2026-04-12T00:00:00Z",
      "updated_at": "2026-04-12T00:00:00Z"
    }
  ]
}
```

### `GET /v1/admin/stats`

Aggregated admin statistics.

Includes:

- totals
- file type distribution
- largest objects
- active users
- top storage users

For bound fragments, object counts, largest objects, and file type distribution are reconciled from the N1 namespace when the engine is reachable. Registry still owns user, quota, session, and share statistics.

### `GET /v1/admin/activity`

List Registry audit/activity events across users.

Query parameters:

- `owner_uid` optional
- `fragment` optional
- `object_key` optional
- `event_type` optional
- `limit` optional, default `100`, max `1000`
- `offset` optional, default `0`

Response:

```json
{
  "events": [
    {
      "...": "AuditEventRecord"
    }
  ],
  "limit": 100,
  "offset": 0
}
```

### `POST /v1/admin/users/{uid}/quota`

Request body:

```json
{
  "quota_limit_bytes": 21474836480
}
```

### `POST /v1/admin/users/{uid}/credentials`

Request body:

```json
{
  "email": "new-email@example.com",
  "password": "new-password",
  "is_admin": false
}
```

### `POST /v1/admin/users/{uid}/2fa/setup`

Admin-initiated 2FA setup for another user.

### `POST /v1/admin/users/{uid}/2fa/disable`

Disable another user's 2FA.

### `DELETE /v1/admin/users/{uid}`

Delete a user and remove their registry-managed fragment associations.

Response:

```json
{
  "removed": true,
  "deleted_fragments": ["fragment-a"]
}
```

## User Auth And Account API

### `POST /v1/auth/signup`

Request body:

```json
{
  "email": "user@example.com",
  "password": "strong-password"
}
```

Response:

- same token envelope as login

### `POST /v1/auth/login`

Request body:

```json
{
  "email": "user@example.com",
  "password": "strong-password",
  "otp": "123456"
}
```

### `POST /v1/auth/refresh`

```json
{
  "refresh_token": "<refresh>"
}
```

### `POST /v1/auth/logout`

```json
{
  "refresh_token": "<refresh>"
}
```

### `GET /v1/me`

Return the authenticated `UserRecord`.

### `POST /v1/account/email`

```json
{
  "current_password": "current-password",
  "new_email": "new@example.com",
  "otp": "123456"
}
```

### `POST /v1/account/password`

```json
{
  "current_password": "current-password",
  "new_password": "new-password",
  "otp": "123456"
}
```

### `POST /v1/account/2fa/setup`

```json
{
  "password": "current-password"
}
```

Response:

```json
{
  "secret": "BASE32SECRET",
  "otpauth_uri": "otpauth://...",
  "qr_data_url": "data:image/png;base64,...",
  "pending": true
}
```

### `POST /v1/account/2fa/enable`

```json
{
  "password": "current-password",
  "otp": "123456"
}
```

### `POST /v1/account/2fa/disable`

```json
{
  "password": "current-password",
  "otp": "123456"
}
```

### `GET /v1/ws`

Registry realtime websocket endpoint.

## Fragment Binding And Storage Session API

### `GET /v1/fragments`

Return linked fragments for the current user.

Response:

```json
{
  "fragments": ["fragment-a"],
  "bindings": [
    {
      "...": "FragmentBindingRecord"
    }
  ]
}
```

### `POST /v1/fragments/link`

Link a fragment to a storage backend deployment.

Request body:

```json
{
  "fragment": "fragment-a",
  "deployment": "single_node",
  "engine_url_override": "https://127.0.0.1:50001"
}
```

`deployment` values:

- `single_node`
- `multi_node`

Response:

```json
{
  "binding": {
    "...": "FragmentBindingRecord"
  }
}
```

### `POST /v1/storage/session`

Mint a storage session for a linked fragment.

Request body:

```json
{
  "fragment": "fragment-a",
  "read": true,
  "write": true,
  "modify": true,
  "delete": true
}
```

Behavior:

- if all four booleans are omitted or false, `Registry` defaults to read access
- response contains the correct engine URL plus an engine access token

## Share API

### `POST /v1/shares`

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "docs/report.pdf",
  "expires_in": 3600,
  "disposition": "download"
}
```

`disposition` values:

- `view`
- `stream`
- `download`

Response:

```json
{
  "share": {
    "...": "ShareRecord"
  },
  "url": "https://cloud.example.test/v1/public/shares/<token>",
  "expires_at": "2026-04-12T06:00:00Z"
}
```

### `GET /v1/shares`

Query parameters:

- `fragment` optional
- `include_expired` optional boolean
- `include_revoked` optional boolean

Response:

```json
{
  "shares": [
    {
      "...": "ShareRecord"
    }
  ]
}
```

### `DELETE /v1/shares/{share_id}`

Response:

```json
{
  "revoked": true
}
```

### `GET /v1/public/shares/{token}`

Public read-through endpoint. No bearer token required.

Behavior:

- validates token and expiry
- resolves the logical object
- streams the object from the linked `N1` or `N2` backend
- forces download filename to the logical basename for download shares

## Activity API

### `GET /v1/activity`

List the current user’s Registry activity events.

Query parameters:

- `fragment` optional
- `object_key` optional
- `event_type` optional
- `limit` optional, default `100`, max `500`
- `offset` optional, default `0`

Response:

```json
{
  "events": [
    {
      "...": "AuditEventRecord"
    }
  ],
  "limit": 100,
  "offset": 0
}
```

Typical event types include:

- `fragment_linked`
- `share_created`
- `share_revoked`
- `object_registered`
- `object_finalized`
- `object_renamed`
- `object_trashed`
- `object_recovered`
- `object_purged`
- `object_overlay_updated`
- `object_comment_added`
- `object_comment_deleted`

## Object API

Registry is intentionally two-phase for writes:

1. register the logical object
2. upload to storage using the issued storage session
3. finalize back into Registry

### `POST /v1/objects/register`

Register a pending logical object.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "expected_size_bytes": 10485760,
  "content_type": "video/mp4",
  "version_id": null,
  "session_id": null
}
```

Response:

```json
{
  "object": {
    "...": "ObjectRecord"
  },
  "quota_limit_bytes": 10737418240,
  "quota_used_bytes": 10485760
}
```

### `POST /v1/objects/finalize`

Finalize the registered object after the backend upload succeeded.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "version_id": "01KNYR8AB4VYG4JCM5TX9GWV8M",
  "object_size_bytes": 10485760,
  "content_type": "video/mp4"
}
```

Behavior:

- `Registry` reconciles the logical row with live engine state when possible
- final object status becomes `ready`

### `DELETE /v1/objects/pending`

Cancel a pending registered object.

Query parameters:

- `fragment` required
- `object_key` required
- `version_id` optional
- `session_id` optional

### `GET /v1/objects`

List logical objects.

Query parameters:

- `fragment` optional
- `prefix` optional
- `delimiter` optional
- `search` optional
- `limit` optional, default `200`, max `5000`
- `offset` optional, default `0`
- `include_pending` optional boolean
- `include_trashed` optional boolean

Response:

```json
{
  "objects": [
    {
      "...": "ObjectRecord"
    }
  ],
  "common_prefixes": ["videos/"],
  "limit": 200,
  "offset": 0,
  "continuation_token": "200"
}
```

Operational note:

- when `fragment` is provided and bound to an engine, committed object listing is sourced from the live `N1` namespace and then decorated with Registry metadata
- Registry policy storage remains a business/policy projection, not the authoritative file-tree source for bound-fragment committed listings

### `GET /v1/objects/suggest`

Autocomplete/suggestion route.

Query parameters:

- `fragment` optional
- `prefix` optional
- `q` required
- `limit` optional, default `8`, max `100`
- `include_trashed` optional boolean

Response:

```json
{
  "suggestions": [
    "videos/demo.mp4",
    "videos/demo-final.mp4"
  ]
}
```

### `GET /v1/objects/stats`

Query parameters:

- `fragment` optional
- `prefix` optional
- `search` optional
- `include_pending` optional boolean
- `include_trashed` optional boolean

Response:

```json
{
  "total_objects": 42,
  "total_size_bytes": 987654321,
  "extension_stats": [
    {
      "extension": "mp4",
      "object_count": 10,
      "total_size_bytes": 876543210
    }
  ]
}
```

Operational note:

- fragment-bound stats are computed from the engine namespace view, not from stale Registry-only projection rows

### `GET /v1/objects/resolve`

Resolve one live logical object.

Query parameters:

- `fragment` required
- `object_key` required

Response:

- `ObjectRecord`

### `GET /v1/objects/overlay`

Read Registry-owned business metadata for a live object.

Query parameters:

- `fragment` required
- `object_key` required

Response:

```json
{
  "overlay": {
    "...": "ObjectOverlayRecord"
  }
}
```

For bound fragments, Registry validates the object against the live N1 namespace before returning overlay metadata. A stale Registry projection row is not enough.

### `POST /v1/objects/overlay`

Update Registry-owned business metadata for a live object.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "docs/report.pdf",
  "favorite": true,
  "tags": ["finance", "review"],
  "labels": ["important"]
}
```

Fields are patch-like:

- `favorite` optional boolean
- `tags` optional full replacement array
- `labels` optional full replacement array

Tags and labels are trimmed, deduplicated, sorted, and capped to 100 values each.

### `GET /v1/objects/comments`

List Registry-owned comments for a live object.

Query parameters:

- `fragment` required
- `object_key` required
- `include_deleted` optional boolean

Response:

```json
{
  "comments": [
    {
      "comment_id": "9e1b5f7e-37f9-48a7-bf98-b0793be42df0",
      "owner_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
      "actor_uid": "01KNYR7QPPQSMW3P5M4N0S6K2P",
      "fragment": "fragment-a",
      "object_key": "docs/report.pdf",
      "body": "Needs review",
      "created_at": "2026-04-21T00:00:00Z",
      "updated_at": "2026-04-21T00:00:00Z",
      "deleted_at": null
    }
  ]
}
```

### `POST /v1/objects/comments`

Add a comment.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "docs/report.pdf",
  "body": "Needs review"
}
```

Response:

- status `201`
- `ObjectCommentsResponse` containing the created comment

### `DELETE /v1/objects/comments/{comment_id}`

Soft-delete one comment owned by the current user.

Response:

```json
{
  "removed": true
}
```

### `POST /v1/objects/archive`

Archive a selected set of logical objects.

Request body:

```json
{
  "fragment": "fragment-a",
  "keys": ["docs/report.pdf", "videos/demo.mp4"],
  "prefix": null,
  "search": null,
  "filename": "export.tar",
  "format": "tar",
  "include_trashed": false
}
```

Rules:

- use either `keys` or `prefix/search`, not both
- current Registry archive surface supports `tar`

Quota note:

- Registry keeps committed usage separate from pending reservations
- pending uploads reserve quota durably before finalize
- cancel/finalize paths release or convert those reservations so quota accounting stays atomic at the Registry layer

## Rename, Trash, Recover, And Delete

### `POST /v1/objects/rename`

Request body:

```json
{
  "fragment": "fragment-a",
  "source_key": "videos/demo.mp4",
  "destination_key": "videos/archive/demo.mp4"
}
```

Response:

- `ObjectMutationResponse`

### `POST /v1/objects/rename-prefix`

Request body:

```json
{
  "fragment": "fragment-a",
  "source_prefix": "videos/2025/",
  "destination_prefix": "videos/archive/2025/"
}
```

Response:

```json
{
  "affected": 120,
  "quota_limit_bytes": 10737418240,
  "quota_used_bytes": 10485760
}
```

### `POST /v1/objects/trash`

Trash one logical object.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4"
}
```

### `POST /v1/objects/trash/bulk`

Trash multiple logical objects.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_keys": [
    "videos/demo-1.mp4",
    "videos/demo-2.mp4"
  ]
}
```

### `POST /v1/objects/trash-prefix`

Trash a logical folder/prefix. For fragments bound to N1, Registry delegates the folder lifecycle operation to N1 POSIX namespace trash (`/n1/posix/trash`) and only uses Registry rows as a quota/business projection. This is intentionally namespace-based, so empty N1 directories can be trashed even when Registry has no object rows for the folder.

Request body:

```json
{
  "fragment": "fragment-a",
  "prefix": "videos/2025/"
}
```

### `POST /v1/objects/recover`

Recover one logical object from trash.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4"
}
```

### `POST /v1/objects/recover/bulk`

Recover multiple logical objects.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_keys": [
    "videos/demo-1.mp4",
    "videos/demo-2.mp4"
  ]
}
```

### `POST /v1/objects/recover-prefix`

Recover everything under a prefix.

Request body:

```json
{
  "fragment": "fragment-a",
  "prefix": "videos/2025/"
}
```

### `POST /v1/objects/recover-all`

Recover all trashed objects in the fragment.

Request body:

```json
{
  "fragment": "fragment-a"
}
```

### `POST /v1/objects/trash/empty`

Purge all trashed objects in the fragment from the Registry logical plane. For N1-bound fragments, Registry first calls N1 `/n1/trash/empty`; Registry state is purged only after N1 accepts the lifecycle operation.

Request body:

```json
{
  "fragment": "fragment-a"
}
```

### `DELETE /v1/objects`

Purge one object immediately from Registry state.

Query parameters:

- `fragment` required
- `object_key` required
- `version_id` optional
- `session_id` optional

Response:

```json
{
  "removed": true,
  "quota_limit_bytes": 10737418240,
  "quota_used_bytes": 0
}
```

### `POST /v1/objects/permanent-prefix`

Purge every object under a prefix.

Request body:

```json
{
  "fragment": "fragment-a",
  "prefix": "videos/old/"
}
```

### `POST /v1/objects/version/delete`

Delete one version from a logical object.

Request body:

```json
{
  "fragment": "fragment-a",
  "object_key": "videos/demo.mp4",
  "version_id": "01KNYR8AB4VYG4JCM5TX9GWV8M"
}
```

Response:

```json
{
  "removed": true,
  "object_removed": false,
  "object": {
    "...": "ObjectRecord"
  },
  "quota_limit_bytes": 10737418240,
  "quota_used_bytes": 10485760
}
```

## Practical End-To-End Flow

### 1. Sign Up Or Log In

```bash
curl -k https://127.0.0.1:7001/v1/auth/signup \
  -H 'Content-Type: application/json' \
  -d '{
    "email":"user@example.com",
    "password":"strong-password"
  }'
```

### 2. Link A Fragment

```bash
curl -k https://127.0.0.1:7001/v1/fragments/link \
  -H "Authorization: Bearer <registry-token>" \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment":"fragment-a",
    "deployment":"single_node",
    "engine_url_override":"https://127.0.0.1:50001"
  }'
```

### 3. Issue A Storage Session

```bash
curl -k https://127.0.0.1:7001/v1/storage/session \
  -H "Authorization: Bearer <registry-token>" \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment":"fragment-a",
    "read":true,
    "write":true,
    "modify":true,
    "delete":true
  }'
```

### 4. Register The Logical Object

```bash
curl -k https://127.0.0.1:7001/v1/objects/register \
  -H "Authorization: Bearer <registry-token>" \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment":"fragment-a",
    "object_key":"videos/demo.mp4",
    "expected_size_bytes":10485760,
    "content_type":"video/mp4"
  }'
```

### 5. Upload To N1 Or N2

Use the `engine_url` and `access_token` from `POST /v1/storage/session`.

### 6. Finalize Back Into Registry

```bash
curl -k https://127.0.0.1:7001/v1/objects/finalize \
  -H "Authorization: Bearer <registry-token>" \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment":"fragment-a",
    "object_key":"videos/demo.mp4",
    "version_id":"01KNYR8AB4VYG4JCM5TX9GWV8M",
    "object_size_bytes":10485760,
    "content_type":"video/mp4"
  }'
```

## Operational Notes

- `Registry` is the correct place for user ownership, quotas, shares, audit/activity, and product metadata overlays.
- For `N1` 3.0-bound fragments, object listing, stats, suggestions, archive selection, and object existence checks are sourced from N1 namespace truth.
- Registry never opens or imports old N1 `namespace_v2` catalogs or redb-era index files as authority.
- `Registry` does not own repair, redistribution, GC, or recover-on-read. Those remain backend responsibilities in `N1` or `N2`.
- `storage_object_id` remains in the Registry schema for compatibility, but N1-bound object operations should use logical `object_key`.
- Prefix trash, prefix recover, and prefix rename must delegate to the bound engine when the fragment is engine-backed.
- Registry audit/overlay/comment rows are business metadata. They must not be treated as proof that an object exists if the bound N1 namespace says it does not.
- For user-facing applications, `Registry` should usually be your entry point. `N1` and `N2` are the storage backends, not the business metadata plane.
