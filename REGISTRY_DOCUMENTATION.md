# Registry Documentation

Current `0.5.0` reference for the Registry business and control plane.

Status:
- Registry is the business metadata layer above storage.
- Registry owns users, auth, quotas, logical object metadata, shares, archive selection, and storage-session brokering.
- Registry does not replace N1 storage truth. It coordinates with N1 or N2 backends through the storage broker.

## 1. Scope

Registry owns:
- account signup and login
- refresh-session lifecycle
- optional TOTP 2FA
- admin bootstrap and admin user management
- fragment-to-backend bindings
- logical object metadata
- stable physical `storage_object_id` mapping for flat engine storage
- share creation and public share serving
- logical trash and recover state
- logical prefix rename, prefix trash, prefix recover, and prefix permanent delete
- archive selection and proxy streaming
- websocket realtime for owners and admins

Registry does not own:
- physical shard placement
- EC repair / redistribution
- fragment-level storage durability

## 2. Base URL And Auth

Base URL example:
- `https://127.0.0.1:7001`

Health:
- `GET /health`

Protected Registry routes require:
- `Authorization: Bearer <registry_access_token>`

Registry bearer tokens represent users, not fragments.

## 3. Admin Bootstrap And Admin Routes

### 3.1 Bootstrap And Admin Auth

- `POST /v1/admin/bootstrap`
- `POST /v1/admin/auth/login`

Bootstrap is intended for the first administrator only and is protected by `REGISTRY_MASTER_KEY`.

### 3.2 Admin User Management

- `POST /v1/admin/users`
- `GET /v1/admin/users`
- `GET /v1/admin/stats`
- `POST /v1/admin/users/{uid}/quota`
- `POST /v1/admin/users/{uid}/credentials`
- `POST /v1/admin/users/{uid}/2fa/setup`
- `POST /v1/admin/users/{uid}/2fa/disable`
- `DELETE /v1/admin/users/{uid}`

Typical admin capabilities:
- create users
- set quota
- rotate credentials
- reset or disable 2FA
- inspect system-wide usage and activity

## 4. User Auth And Account Routes

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

Key request models:
- `SignupRequest`
- `LoginRequest`
- `RefreshRequest`
- `UpdateEmailRequest`
- `UpdatePasswordRequest`
- `SetupTwoFactorRequest`
- `EnableTwoFactorRequest`
- `DisableTwoFactorRequest`

## 5. Fragments And Storage Sessions

- `GET /v1/fragments`
- `POST /v1/fragments/link`
- `POST /v1/storage/session`

Purpose:
- list a user’s linked fragments
- attach a logical fragment binding to a backend deployment
- issue a short-lived backend storage session for direct N1/N2 use

Important boundary:
- Registry owns the user-facing fragment binding
- N1 still enforces fragment-scoped storage permissions with its own bearer token

## 6. Share Routes

- `POST /v1/shares`
- `GET /v1/shares`
- `DELETE /v1/shares/{share_id}`
- `GET /v1/public/shares/{token}`

Current behavior:
- Registry stores share metadata and expiry
- Registry validates public tokens
- Registry proxies the actual read through the storage broker

Key models:
- `CreateShareRequest`
- `CreateShareResponse`
- `ListSharesQuery`
- `ListSharesResponse`
- `RevokeShareResponse`
- `ShareDisposition`

## 7. Logical Object Routes

### 7.1 Upload Registration And Finalization

- `POST /v1/objects/register`
- `POST /v1/objects/finalize`
- `DELETE /v1/objects/pending`

Purpose:
- reserve logical object metadata
- finalize it after backend upload succeeds
- clean up pending reservations

Key models:
- `RegisterObjectRequest`
- `FinalizeObjectRequest`
- `ObjectMutationResponse`
- `ObjectDeleteResponse`

### 7.2 Listing And Lookup

- `GET /v1/objects`
- `GET /v1/objects/suggest`
- `GET /v1/objects/stats`
- `GET /v1/objects/resolve`

Registry listing is logical and user-facing:
- prefix
- delimiter
- search
- include pending
- include trashed

Key models:
- `ListObjectsQuery`
- `ListObjectsResponse`
- `SuggestObjectsQuery`
- `SuggestObjectsResponse`
- `ObjectStatsQuery`
- `ObjectStatsResponse`
- `ResolveObjectQuery`

### 7.3 Rename And Lifecycle

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

Current model:
- Registry owns logical object metadata and state
- Registry owns namespace-heavy operations like folder-style rename and prefix trash/recover without rewriting engine paths
- Registry coordinates with the storage backend only for physical object lifecycle operations keyed by stable `storage_object_id`

Key models:
- `RenameRegistryObjectRequest`
- `RenamePrefixObjectsRequest`
- `ObjectStateRequest`
- `FragmentStateRequest`
- `BulkObjectKeysRequest`
- `PrefixObjectsRequest`
- `DeleteObjectQuery`
- `DeleteObjectVersionRequest`

## 8. Archive Route

- `POST /v1/objects/archive`

Registry owns:
- object selection
- prefix/search filtering
- logical path naming inside the archive
- proxying the archive stream from backend object reads keyed by `storage_object_id`

N1 still owns:
- the underlying physical object reads and durability for the selected object set

Key model:
- `ArchiveObjectsRequest`

## 9. WebSocket Realtime

- `GET /v1/ws`

Registry realtime is used for:
- owner-scoped invalidation events
- admin events
- file uploaded / deleted / renamed notices
- session and 2FA notices

## 10. Data Model Notes

Current Registry object metadata is keyed by:
- `owner_uid`
- `fragment`
- `object_key`

Important current behavior:
- Registry owns the logical namespace
- Registry keeps a stable physical `storage_object_id` for each logical object so rename does not require a physical engine rename
- Storage sessions are brokered into N1/N2 for actual bytes
- Registry logical object state may be reconciled against engine state when needed

## 11. Related Docs

- [DOCUMENTATION.md](/home/administrator/Development/N1/DOCUMENTATION.md)
- [N1_DOCUMENTATION.md](/home/administrator/Development/N1/N1_DOCUMENTATION.md)
- [N2_DOCUMENTATION.md](/home/administrator/Development/N1/N2_DOCUMENTATION.md)
- [README.md](/home/administrator/Development/N1/README.md)
