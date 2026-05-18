<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N2</h1>
  <p>
    Unified storage platform built around <code>N1</code>, <code>N2</code>,
    <code>NS</code>, and <code>Registry</code>: single-node storage through
    <code>n1</code>, multi-node placement through <code>n2</code>,
    standalone NAS/SMB access through <code>ns</code>, and user-facing namespace
    and policy through <code>registry</code>.
  </p>
</div>

---

This repository contains the storage platform stack built around four runtime components:

> - `N1` (`1.1.1`)
  Primary N1 storage kernel with an N1 namespace, two-level fanned immutable `.objects/<hh>/<hh>/@o_<object-id>` records, zero-decode listing, object-level repair/status, and the N1 blob/shard/parity plane. See `N1_DOCUMENTATION.md`.
> - `N2` (`0.4.0`)
  Multi-node clustering and placement layer built on top of the `N1` storage kernel, with real-prefix namespace directories, symlink object entries, immutable filesystem `.nmtd` cluster metadata, one cluster EC pass, and an S3-compatible path-style object surface.
> - `NS` (`1.0.0`)
  Standalone NAS Lite / SMB service with fragment-backed auth, configurable share roots, backing-filesystem capacity, and optional cold-sync backup to `N1`.
> - `Registry` (`0.9.1`)
  User-facing logical metadata and policy layer for accounts, quotas, logical namespace, shares, and storage-session brokering.

If you are new to the codebase, start here. This README explains what each component does, how the platform is intended to be used, and how to stand up a practical deployment.

## Project Overview

The platform is designed to separate storage durability from user-facing metadata concerns:

- `N1` owns physical storage truth and maintenance.
- `N2` adds cluster coordination when storage is distributed across nodes.
- `NS` owns NAS/SMB live file-serving without coupling that hot path to `N1`.
- `Registry` owns user/business metadata and product-facing policies.

Current route namespaces are intentionally split:

- `N1` stable runtime and operator routes live under `/n1/...`
- `N2` cluster routes live under `/v4/n2/...`
- `NS` exposes SMB on its configured TCP bind address and port.
- `Registry` product-facing routes live under `/v1/...`

The current N1 line retires the old Standard/indexdb engine from the active workspace. Active N1 metadata truth is the N1 namespace, immutable `.ndx` object records, and the `.blobs` payload plane:

- namespace state is real directories and symlink object pointers
- object metadata is immutable `.ndx` records under `.objects/<hh>/<hh>/@o_<object-id>/`
- upload sessions and receipts live under `.uploads/`
- trash is namespace movement under `.trash/`
- repair, redistribution, GC, and recover-on-read operate on explicit storage truth and reachability
- SMB/NAS serving has moved out of `N1` into standalone `NS`, keeping `N1` focused on storage-kernel responsibilities.
- `NS 1.0.0` keeps the live SMB path local to disk, uses dynamic fragment authentication, supports recursive folder deletion semantics expected by Windows clients, and cold-syncs large files to `N1` through multipart upload.
- Per-fragment N1 index replicas are copied across configured index roots, monitored by health, and repaired when a rebuilt index disk is missing or stale.
- Trash entries are generation-addressed with `trash_entry_id`, so repeated deletes of the same visible path remain individually recoverable or purgeable without path ambiguity.
- fragment-bound Registry listing and stats now prefer `N1` namespace truth instead of stale Registry projection.
- Registry folder trash/recover delegates to N1 POSIX namespace lifecycle, including empty folders, instead of emulating directory delete with object-key prefix loops.
- Registry quota reservations are tracked as explicit durable pending reservations and reconciled against committed object usage on finalize and cancel paths.
- Registry now keeps product metadata separate from object truth: shares, audit/activity, favorites, tags, labels, and comments live in Registry while file existence/listing comes from N1 for bound fragments.
- Drive/RAD ARC folder scans consume the Registry/N1 namespace response directly and tolerate both direct DICOM files and nested `common_prefixes` under the selected folder.

## Core Concepts

### N1

`N1 1.1.1` is the primary N1 storage kernel and the default N1 face for new deployments.

It keeps the N1 object/blob plane, repair model, redistribution model, GC model, and public route shape, with active storage authority in explicit N1 planes:

```text
index_root/<fragment_hash>/
  .blobs/<version-id>/
    <shard-or-parity-file>
  .objects/<hh>/<hh>/@o_<object-id>/
    <ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
  docs/video.mp4 -> @o_<object-id>
```

Folders are real directories. File names are namespace symlinks whose payload is a stable object id. Object records live in two-level fanned `.objects/<hh>/<hh>/@o_<object-id>/` directories, each committed version is an immutable `.ndx` file, and normal listing reads direct directory children plus `.ndx` filenames only. Payload shards live under the explicit internal `.blobs/` directory so user namespace names such as `blob/` remain valid. Normal listing does not decode `.ndx` bodies.

Use `N1` when the goal is the Linux-first, IO-bound N1 storage kernel.

### N2

`N2 0.4.0` is the multi-node layer for N1.

It extends `N1` with:

- cluster membership
- governance state
- shard placement
- peer transport
- reconcile behavior
- real-prefix namespace directories with symlink object entries
- filesystem `.nmtd` cluster object metadata
- one cluster-level EC pass; local N1 shard storage is `kd=1`, `md=0`
- S3-compatible path-style object operations under `/s3/...`

Use `N2` when storage needs to span multiple nodes with cluster-managed placement and reconcile behavior. Use standalone `N1` when you want a single-node engine.

### NS

`NS` is the standalone NAS Lite service.

It owns:

- SMB-compatible file serving
- configurable local share roots
- fragment-backed authentication such as `\\server\my-fragment` with username `my-fragment`
- backing-filesystem capacity instead of logical quota enforcement
- optional cold-sync backup to `N1`

`NS` deliberately uses its own directories and metadata store. Do not point `NS` share roots into `N1` index or blob directories.

### Registry

`Registry` is the product-facing logical metadata and policy layer.

It owns:

- users and admin bootstrap
- login, refresh, logout, 2FA
- quotas and stats
- policy around the backend logical object namespace
- shares and public links
- storage-session issuance
- audit/activity
- favorites, tags, labels, and comments

Registry keeps user/business metadata around logical paths owned by `N1` or `N2`. It is the right layer for user ownership, quotas, shares, product policy, and UI overlays. It is not the layer that should own the authoritative file tree, repair shards, rebalance disks, or reclaim orphaned physical payloads.

## Architecture Overview

At a high level, the platform looks like this:

```text
Clients / UI / CLI
        |
        +--> Registry (optional) --> N1 or N2 --> index roots + blob roots
        |
        +--> NS (SMB/NAS) --------> local share roots
                                    optional cold sync to N1
```

For standalone `N1 1.1`, the internal model is:

```text
index_root/<fragment_hash>/
  .fragment.ndx
  .objects/<hh>/<hh>/@o_<object-id>/
    <ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
  .uploads/<version-id>/
    manifest.ndx
    part_p<part-number>.<crc32>.receipt
  .trash/
  docs/video.mp4 -> @o_<object-id>

blob_root/<fragment_hash>/.blobs/<version_id>/
  - payload shards / parity
  - placement
```

This split is why current `N1` is materially different from the older Standard/indexdb design:

- path changes do not rewrite immutable object or blob truth
- namespace is the POSIX/object-key face and is expressed as real directories plus symlink object pointers
- delete becomes lifecycle-first
- GC is driven by reachability
- repair and redistribution stay storage-focused
- object listing parses filenames and decodes `.ndx` only when full metadata is needed

## Features And Capabilities

### Storage And Durability

- one-shot upload
- multipart upload and resume
- versioned objects
- shard/parity layouts
- recover-on-read
- manual and continuous repair
- redistribution across configured blob roots
- garbage collection driven by lifecycle and reachability

### Namespace And Access

- explicit directories
- POSIX-style browse, stat, mkdir, rename, move
- trash and recover for files and directories
- signed reads and downloads
- standalone `NS` SMB/NAS access for normal file-manager flows

### Product And Control Plane

- Registry users, quotas, and shares
- Registry logical namespace independent from storage identity
- single-node `N1` backend or multi-node `N2` backend

## Intended Use Cases

This workspace is meant for building storage systems where:

- physical durability and maintenance matter
- namespace and lifecycle semantics matter
- you want a clean separation between storage truth and user/business metadata

Typical deployments:

- standalone `N1` for a single-node storage appliance or service
- standalone `NS` when a lightweight NAS/SMB appliance is needed
- `Registry + N1` for user-facing single-node product deployments
- `Registry + N2` for clustered deployments with centralized logical metadata

## Repository Layout

- [`n1`](n1)
  `N1` server and storage kernel
- [`n2`](n2)
  `N2` cluster layer
- [`ns`](ns)
  Litiaina NS standalone NAS Lite / SMB service
- [`registry`](registry)
  Registry service
- [`n1_cli`](n1_cli)
  Operator CLI for `N1` maintenance, health, fragment create, object status/repair, and multipart status
- [`n2_cli`](n2_cli)
  CLI helpers for `N2`
- [`indexdb`](indexdb)
  Internal per-fragment metadata crate used by `N1`
- [`simd-erasure-core`](simd-erasure-core)
  Erasure and shard primitives

Additional docs:

- [`DOCUMENTATION.md`](DOCUMENTATION.md)
- [`N1_DOCUMENTATION.md`](N1_DOCUMENTATION.md)
- [`NS_DOCUMENTATION.md`](NS_DOCUMENTATION.md)
- [`N2_DOCUMENTATION.md`](N2_DOCUMENTATION.md)
- [`REGISTRY_DOCUMENTATION.md`](REGISTRY_DOCUMENTATION.md)

## Deployment Example

### Standalone N1

1. Prepare a config file.

`N1` creates `n1_config.ini` with defaults when the runtime config does not already exist. Use that generated file as the local template.

Key sections:

- `[server]`
  bind host, HTTPS port, TLS cert/key
- `[disk_config]`
  `index_store_disks`, `blob_store_disks`, optional `fs_storage_path`
- `[upload_config]`
  durability and throughput knobs

2. Create or edit an env file for secrets.

Required `N1` environment values:

- `JWT_SECRET`
- `AUTH_KEY`
- `MASTER_KEY`
- `ADMIN_KEY`

3. Start the server.

```bash
cargo run -p n1
```

4. Create a fragment.

```bash
curl -k https://127.0.0.1:50001/n1/fragments \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "mainframe",
    "secret": "<MASTER_KEY>",
    "key": "fragment-local-secret",
    "quota_bytes": 536870912000
  }'
```

5. Mint a fragment access token.

```bash
curl -k https://127.0.0.1:50001/n1/auth/fragment \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment":"<fragment-hash>",
    "secret":"<MASTER_KEY>",
    "read":true,
    "write":true,
    "modify":true,
    "delete":true
  }'
```

6. Use the token for uploads, reads, or POSIX routes.

### N1 Operator CLI

`n1_cli` is aligned with the stable `/n1/...` surface. It is an operator CLI, not a general file-transfer client.

Protected CLI commands authenticate through `/n1/auth/fragment`, so pass `--fragment <name-or-hash>` or `--auth-fragment <name-or-hash>` when talking to a real server.

Examples:

```bash
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  health --deep-scan

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  uploads status --version-id <version_id>

cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  repair status
```

Common commands:

```bash
# Create a fragment
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  fragment create --name <fragment_name> --key <fragment_key> --quota-bytes <bytes>

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

# Repair dry-run
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  repair start --dry-run --mode availability

# Redistribution dry-run
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  redistribute start --dry-run --mode availability --durability auto --scope all

# JSON output
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <fragment> \
  --json repair status

# Use a different fragment only for auth
cargo run -p n1_cli -- \
  --url <url> \
  --secret <secret> \
  --fragment <target_fragment> \
  --auth-fragment <auth_fragment> \
  health
```

### Registry + N1

1. Start `N1`.
2. Configure Registry env values:

- `REGISTRY_HOST`
- `REGISTRY_HTTPS_PORT`
- `REGISTRY_CERT_PATH`
- `REGISTRY_KEY_PATH`
- `REGISTRY_SQLITE_PATH`
- `REGISTRY_JWT_SECRET`
- `REGISTRY_MASTER_KEY`
- `REGISTRY_SINGLE_NODE_URL`
- `REGISTRY_SINGLE_NODE_MASTER_KEY`

3. Start Registry.

```bash
cargo run -p registry
```

4. Bootstrap admin and users through the Registry API.

### N2 Cluster

Use `N2` when you need cluster-managed placement instead of a single storage node. `N2 0.4.0` uses the `N1` storage kernel locally, stores distributed object maps as `.nmtd`, uses real-prefix namespace directories for listing, and adds cluster membership, governance, placement, peer transport, S3-compatible path-style object routes, and reconcile flows. Multi-node `N2` must not be configured as loopback-only.

## Configuration Overview

### N1 / N2 Config

Standalone `N1` uses `n1_config.ini`.

`N2` uses `n2_config.ini` and `n2.env`. At runtime it also materializes N1-kernel aliases for the embedded `N1` storage kernel.

Important settings:

- `server.host`, `server.https_port`
- `server.cert_path`, `server.key_path`
- `disk_config.index_store_disks`
- `disk_config.blob_store_disks`
- `garbage_collector.retention_seconds`
- `upload_config.data_shards`, `upload_config.parity_shards`
- `[n2].node_id`, `[n2].node_endpoint`, `[n2].cluster_membership_file`
- `[n2].kc`, `[n2].mc`
- `[n2].kd=1`, `[n2].md=0` for N2 0.4 local shard storage
- `[n2].cluster_commit_mode`

Notes:

- `index_store_disks` and `blob_store_disks` can point to different directories on the same device, but separate devices are operationally stronger.
- fragment names hash deterministically, so recreating the same fragment name yields the same fragment hash.

### Secrets

`N1`:

- `JWT_SECRET` signs storage access tokens
- `AUTH_KEY` is used for storage auth flows
- `MASTER_KEY` is used for privileged fragment operations
- `ADMIN_KEY` is used for admin-level routes

`Registry`:

- `REGISTRY_JWT_SECRET`
- `REGISTRY_MASTER_KEY`
- backend master key settings for the chosen storage target

## Usage Examples

### One-Shot Upload

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n1/upload/one-shot/path/to/file.txt" \
  -H "Authorization: Bearer <fragment-token>" \
  --data-binary @file.txt
```

### POSIX List

```bash
curl -k \
  "https://127.0.0.1:50001/n1/posix/list?path=docs&limit=200" \
  -H "Authorization: Bearer <fragment-token>"
```

### POSIX Mkdir

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n1/posix/mkdir" \
  -H "Authorization: Bearer <fragment-token>" \
  -H 'Content-Type: application/json' \
  -d '{"path":"docs"}'
```

### Rename Or Move

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n1/posix/rename" \
  -H "Authorization: Bearer <fragment-token>" \
  -H 'Content-Type: application/json' \
  -d '{"from":"docs/old.txt","to":"docs/new.txt"}'
```

### Trash And Recover

```bash
curl -k -X POST \
  "https://127.0.0.1:50001/n1/posix/trash" \
  -H "Authorization: Bearer <fragment-token>" \
  -H 'Content-Type: application/json' \
  -d '{"path":"docs/new.txt"}'

curl -k -X POST \
  "https://127.0.0.1:50001/n1/posix/recover" \
  -H "Authorization: Bearer <fragment-token>" \
  -H 'Content-Type: application/json' \
  -d '{"path":"docs/new.txt"}'
```

### Signed Download

```bash
curl -k \
  "https://127.0.0.1:50001/n1/objects/signed-read?fragment=<fragment>&object_key=docs/new.txt&secret=<MASTER_KEY>"
```

### Standalone NS

Use `NS` when SMB/NAS access is required. `NS` has its own `ns_config.ini`, local share roots, metadata store, backing-filesystem capacity model, and fragment-backed SMB auth that is verified locally from N1 fragment metadata instead of extending `N1`'s stable API surface.

```bash
cargo run -p ns --bin litiaina-ns
```

See [`NS_DOCUMENTATION.md`](NS_DOCUMENTATION.md) for deployment, disk-layout, SMB port, auth, capacity behavior, and optional cold-sync configuration.

## Operational Flows

### Normal File Lifecycle

1. create fragment
2. mint fragment token
3. upload object
4. browse through object or POSIX routes
5. trash object or directory
6. recover if needed
7. empty trash when logically retired
8. run GC to reclaim unreachable physical bytes

### Repair And Redistribution

- repair fixes missing or damaged shard/index state
- redistribution rebalances data placement
- recover-on-read reconstructs when a readable layout still exists

These are storage maintenance flows. They should not depend on namespace mutations.

### Registry Flow

1. user logs in to Registry
2. Registry resolves logical namespace and quotas
3. Registry issues a storage session for `N1` or `N2`
4. storage engine handles upload/read/lifecycle
5. Registry finalizes logical metadata after storage commit

Quota and end-user policy live in `Registry`, not in standalone `N1`.

## Troubleshooting And Notes

### Empty Trash Does Not Immediately Free Disk Space

That is expected.

`N1` is lifecycle-first:

- trash hides objects from the live namespace
- empty trash retires namespace visibility
- GC later reclaims unreachable payloads

If you need immediate space reclamation, empty trash and then run GC.

### Fragment Already Exists After Reusing The Same Name

Fragment names hash deterministically. Reusing the same name recreates the same fragment hash. If you want a brand-new fragment identity, use a different name. If a fragment was partially initialized, current N1 fragment create logic heals partial state on retry instead of false-conflicting.

### Active Metadata Files

For current `N1`, the active per-fragment metadata authority is the N1 storage index:

```text
<index_root>/<fragment_hash>/
  .fragment.ndx
  .objects/<hh>/<hh>/@o_<object-id>/*.ndx
  .uploads/
  .trash/
  <namespace directories and symlink object pointers>
```

Do not restore or point runtime code at retired Standard `indexdb`, `namespace_v2/catalog.ndxdb`, `catalog.ndxdb`, `namespace.ndxdb`, `namespace.sqlite`, redb index files, or serialized `ObjectMetadata` blobs as authority. Those belong only to older releases or explicit import tooling.

### Listing Or Trash Views Look Wrong

The N1 namespace routes are the source of truth for standalone namespace behavior:

- `GET /n1/posix/list`
- `POST /n1/posix/mkdir`
- `POST /n1/posix/rename`
- `POST /n1/posix/trash`
- `POST /n1/posix/recover`

For trash browsing, the root trash list now returns actual trashed item paths. If a deleted item is a directory, it appears as a directory entry. If a deleted item is a nested file such as `videos/video.mp4`, the trash list returns that full file path directly.

### Self-Signed TLS

Many local deployments use self-signed or locally issued certificates. Use `curl -k` for local testing or configure trusted certificates in your client environment.

### NS Scope

SMB/NAS workflows are handled by standalone `NS`, not embedded in `N1`. Keep `NS` share roots separate from `N1` index/blob roots so live NAS file serving cannot destabilize storage-kernel state.

## Development Notes

Useful workspace commands:

```bash
cargo check -p n1
cargo test -p n1 -- --test-threads=1
cargo test -p ns
cargo test -p registry -- --test-threads=1
```

High-signal release tests used for `N1` include:

- CRUD upload/read/delete/recover flows
- GC reclaim after empty trash
- recover-on-read with disk loss
- repair rebuilding missing blob/index state
- redistribution with disk loss
- POSIX pagination, mkdir, rename, trash, recover
- NS SMB read/write/mkdir/rename/delete flows

High-signal proof used for the N2 rewrite includes:

- native `/v4/n2` upload/download over two real nodes
- `.nmtd` object-map replication on local and remote index roots
- S3 path-style put/head/get/list/delete under `/s3`
- remote-node-loss read from the surviving node
- verification that legacy `.n2map` and `cshard.meta` files are not created

`N2 0.4.0` adds the one-EC local shard path and N2 namespace symlink mirror. It needs a fresh two-node proof before being considered for the 1.0 release gate.

If you are touching a subsystem boundary, prefer adding a focused regression test in that subsystem plus a workflow test when the behavior spans storage, namespace, and maintenance.
