# Registry Documentation

Current `0.7.0` reference for the Registry user-owned metadata and POSIX facade.

Status:
- `registry-server` is the SQLite-backed control plane above `N1` and `N2`.
- Registry owns users, auth, quotas, logical namespace, shares, archive selection, and user-facing lifecycle.
- `N1` and `N2` own physical storage, durability, repair, redistribution, and GC.
- `POST /v1/admin/bootstrap` is a pure Registry admin bootstrap route. It does not create a storage fragment.
- New Registry-backed objects are stored in the engine by stable flat `storage_object_id`, not by logical path.

## 1. Identity

Registry is the place where users and applications see filesystem-like meaning:
- logical `object_key`
- prefix and delimiter listing
- folder illusion
- rename and prefix rename
- trash, recover, purge semantics
- share creation and public share validation

Registry is not the physical storage engine.

That split is deliberate:
- Registry owns logical meaning
- N1/N2 own physical bytes

## 2. Contract Boundary

Registry owns:
- super-admin bootstrap and administration
- user signup, login, refresh, logout, and optional TOTP 2FA
- quota ownership and registry-wide stats
- fragment-to-backend bindings
- logical object metadata
- stable `storage_object_id` mapping for each logical object
- share tokens and public share serving
- archive selection and logical archive path naming
- logical lifecycle states
- websocket realtime for owners and admins

Registry does not own:
- shard placement
- fragment durability
- repair
- redistribution
- engine garbage collection
- raw physical object storage

## 3. Base URL And Auth

Base URL example:
- `https://127.0.0.1:7001`

Health:
- `GET /health`

Protected Registry routes require:
- `Authorization: Bearer <registry_access_token>`

Registry access tokens represent users and administrators, not fragments.

## 4. Admin Bootstrap And Admin Control Plane

### 4.1 Bootstrap

- `POST /v1/admin/bootstrap`

Bootstrap is for the first Registry administrator only.

Required input:
- `email`
- `password`
- `master_key`

Important current behavior:
- bootstrap is guarded by `REGISTRY_MASTER_KEY`
- bootstrap is only allowed while no admin exists
- bootstrap creates the first super admin directly
- bootstrap does not require a storage backend to be reachable
- bootstrap does not auto-provision a fragment

That makes bootstrap a pure Registry control-plane action.

### 4.2 Admin Auth

- `POST /v1/admin/auth/login`

### 4.3 Admin Management Routes

- `POST /v1/admin/users`
- `GET /v1/admin/users`
- `GET /v1/admin/stats`
- `POST /v1/admin/users/{uid}/quota`
- `POST /v1/admin/users/{uid}/credentials`
- `POST /v1/admin/users/{uid}/2fa/setup`
- `POST /v1/admin/users/{uid}/2fa/disable`
- `DELETE /v1/admin/users/{uid}`

Typical admin responsibilities:
- create users
- set or change quota
- rotate credentials
- inspect global usage
- reset or disable 2FA

## 5. User Auth And Account Routes

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

Current behavior:
- public signup never accepts client-chosen quota
- new public accounts start on the server default quota unless an admin changes it later
- normal non-admin signup can auto-provision a default fragment when a storage backend is configured
- admin bootstrap and admin login do not depend on that storage provisioning path

## 6. Fragments And Storage Sessions

- `GET /v1/fragments`
- `POST /v1/fragments/link`
- `POST /v1/storage/session`

Registry owns the user-facing fragment binding.

Backends today:
- `single_node` -> `N1`
- `multi_node` -> `N2`

Storage sessions are short-lived backend bearer tokens that let a Registry-backed client talk directly to the engine for byte transfer.

Important boundary:
- Registry decides which backend and fragment a user may use
- N1/N2 still enforce fragment-scoped storage permissions

## 7. Object Identity Model

Registry tracks two different identities for a Registry-backed object:

- logical key in Registry:
  - `Projects/Alpha/spec.txt`
- physical key in N1/N2:
  - `storage_object_id`
  - for example `2a6af218d2821335ed39d0071abd69086284947d7ec17621ee7446caae7bcc4c`

Rules:
- users and apps see `object_key`
- engines store and read by `storage_object_id`
- rename changes the logical key
- rename does not change the physical key
- folder move is a prefix rename in Registry, not a physical engine rename

For Registry-backed objects, the engine field named `object_key` is expected to hold the physical flat ID, not the user path.

Legacy note:
- older rows may still have `storage_object_id == object_key`
- new Registry-backed writes are expected to use a generated flat `storage_object_id`

## 8. Lifecycle Model

Registry lifecycle states are:
- `ready`
- `trashed`
- `purged`

Meaning:
- `ready` -> visible in normal listings
- `trashed` -> hidden from normal listings and visible in recycle-bin style views
- `purged` -> hidden from normal user listings and pending or following background engine cleanup

Current lifecycle behavior:
- trash is Registry metadata only
- recover from trash is Registry metadata only
- permanent delete and empty trash move objects to `purged`
- after purge, Registry schedules background engine soft-delete by `storage_object_id`
- physical reclaim in N1/N2 can lag behind logical removal
- a purged object may still be recoverable until the engine lifecycle removes it

This is intentional:
- Registry gives fast user-facing lifecycle transitions
- N1/N2 handle physical cleanup asynchronously

## 9. Logical Object Routes

### 9.1 Upload Registration And Finalization

- `POST /v1/objects/register`
- `POST /v1/objects/finalize`
- `DELETE /v1/objects/pending`

Registry-backed upload flow:
1. call `POST /v1/objects/register`
2. receive logical metadata plus stable `storage_object_id`
3. request a storage session
4. upload bytes directly to N1/N2 using `storage_object_id`
5. call `POST /v1/objects/finalize`

This is the core invariant:
- Registry owns the logical name
- N1/N2 receive only the flat physical key

### 9.2 Listing And Lookup

- `GET /v1/objects`
- `GET /v1/objects/suggest`
- `GET /v1/objects/stats`
- `GET /v1/objects/resolve`

Registry listing is logical and user-facing.

Supported patterns:
- prefix listing
- delimiter grouping
- filtered stats
- search suggestions
- include pending
- include trashed

### 9.3 Rename, Folder Move, And Lifecycle

- `POST /v1/objects/rename`
- `POST /v1/objects/rename-prefix`
- `POST /v1/objects/trash`
- `POST /v1/objects/trash/bulk`
- `POST /v1/objects/trash-prefix`
- `POST /v1/objects/recover`
- `POST /v1/objects/recover/bulk`
- `POST /v1/objects/recover-prefix`
- `POST /v1/objects/recover-all`
- `POST /v1/objects/permanent-prefix`
- `POST /v1/objects/trash/empty`
- `DELETE /v1/objects`
- `POST /v1/objects/version/delete`

Current semantics:
- single rename updates only Registry metadata
- folder move is `rename-prefix`
- trash/recover do not rewrite engine paths
- purge routes operate on Registry lifecycle first
- engine cleanup happens afterward by `storage_object_id`

## 10. Shares

- `POST /v1/shares`
- `GET /v1/shares`
- `DELETE /v1/shares/{share_id}`
- `GET /v1/public/shares/{token}`

Registry owns:
- share metadata
- token validation
- expiry and revoke state

The engine read still happens through the storage broker using the physical `storage_object_id`.

Important consequence:
- share URLs remain Registry concerns
- N1 does not own public sharing policy

## 11. Archive

- `POST /v1/objects/archive`

Registry owns:
- which objects are selected
- logical prefix and filter behavior
- logical path names inside the archive

N1/N2 own:
- the physical object reads behind that archive

Current archive model:
- Registry selects objects logically
- Registry reads engine bytes by `storage_object_id`
- archive entries are emitted with logical names like `Archive/Alpha-Moved/spec.txt`

## 12. WebSocket Realtime

- `GET /v1/ws`

Registry realtime includes:
- owner-scoped invalidation events
- admin-wide mutation visibility
- rename / delete / upload lifecycle notices
- session and 2FA notices

## 13. Operator Guide

### 13.1 First-Time Bootstrap

1. Start `registry-server`.
2. Call `POST /v1/admin/bootstrap` with:
   - `email`
   - `password`
   - `master_key`
3. Use the returned Registry bearer token for admin routes.

Bootstrap is now safe to perform before a storage backend is online.

### 13.2 Create Or Provision Users

Admin options:
- create users directly with admin routes
- assign quota explicitly
- reset credentials or 2FA later

Public path:
- users can sign up through `/v1/auth/signup`
- Registry may auto-provision their default fragment when backend storage is configured

### 13.3 Registry-Backed Upload

1. register the object with `/v1/objects/register`
2. obtain a storage session with `/v1/storage/session`
3. upload to N1/N2 using the returned `storage_object_id`
4. finalize through `/v1/objects/finalize`

Expected result:
- Registry resolves by logical `object_key`
- the engine stores by flat physical ID

### 13.4 Folder Move

Folder move is a prefix rename:
- source prefix -> destination prefix
- Registry metadata updates
- engine storage IDs stay unchanged

### 13.5 Trash, Remove, Recover

User-facing behavior:
- trash hides from normal listings
- recover restores from trash quickly
- remove from recycle bin / empty trash hides immediately
- physical engine cleanup continues in the background

Operational implication:
- logical state can move faster than physical reclaim
- this is by design

## 14. Client Surface Truth

Current UI split in this workspace:
- `drive/index.html` is the Registry-backed user surface
- `storage/index.html` is the direct N1 operator surface

That distinction matters:
- Drive is the reference surface for Registry logical namespace behavior
- Storage is the reference surface for direct engine/operator behavior

So if direct N1 listing in the Storage console shows path-backed objects, that does not by itself disprove the Registry flat-key model. It means those objects came from direct N1/operator flows or legacy path-backed data.

## 15. Verified Current Truth

Current release validation on `2026-04-07` confirmed:
- bootstrap works without a storage backend
- Registry-backed CRUD works
- single rename keeps the same `storage_object_id`
- folder move is `rename-prefix`
- N1 listing by logical prefix stays empty for Registry-backed objects
- N1 metadata lookup by the physical ID returns the physical ID as the engine key
- archive uses logical paths
- trash, recover, purge, and recover-from-purged work end to end

## 16. Related Docs

- [DOCUMENTATION.md](/home/administrator/Development/N1/DOCUMENTATION.md)
- [README.md](/home/administrator/Development/N1/README.md)
- [N1_DOCUMENTATION.md](/home/administrator/Development/N1/N1_DOCUMENTATION.md)
- [N2_DOCUMENTATION.md](/home/administrator/Development/N1/N2_DOCUMENTATION.md)
