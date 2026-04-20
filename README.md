<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N2</h1>
  <p>
    Unified storage platform built around <code>N1</code>, <code>N2</code>, and
    <code>Registry</code>: single-node storage through <code>n1</code>,
    multi-node placement through <code>n2</code>, and user-facing namespace and
    policy through <code>registry</code>.
  </p>
</div>

---

This repository contains the storage platform stack built around three runtime components:

> - `N1` (`2.1.1`)
  Single-node storage kernel with an object-storage core, standalone namespace layer, lifecycle, repair, redistribution, GC, recover-on-read, POSIX-style routes, and SMB Lite.
> - `N2` (`0.2.0`)
  Multi-node clustering and placement layer built on top of the shared `N1` storage core.
> - `Registry` (`0.9.0`)
  User-facing logical metadata and policy layer for accounts, quotas, logical namespace, shares, and storage-session brokering.

If you are new to the codebase, start here. This README explains what each component does, how the platform is intended to be used, and how to stand up a practical deployment.

## Project Overview

The platform is designed to separate storage durability from user-facing metadata concerns:

- `N1` owns physical storage truth and maintenance.
- `N2` adds cluster coordination when storage is distributed across nodes.
- `Registry` owns user/business metadata and product-facing policies.

Current route namespaces are intentionally split:

- `N1` stable runtime and operator routes live under `/n1/...`
- `N2` cluster routes live under `/v4/n2/...`
- `Registry` product-facing routes live under `/v1/...`

The major `2.0` change in `N1` is architectural:

- the namespace is now a separate mutable database
- object metadata is immutable
- blob payloads are immutable

That means file and folder semantics such as `mkdir`, rename, move, trash, recover, and browse are handled in the namespace layer, while repair, redistribution, GC, and recover-on-read operate on storage truth and reachability.

Current release notes for the validated `N1 2.1.1` and `Registry 0.9.0` line:

- `namespace_v2` is replicated across configured index roots, and `N1` can reseed the owner namespace catalog from a surviving replica after owner-disk loss.
- fragment-bound Registry listing and stats now prefer `N1` namespace truth instead of stale SQLite-only projection.
- Registry quota reservations are tracked as explicit durable pending reservations and reconciled against committed object usage on finalize and cancel paths.

## Core Concepts

### N1

`N1` is the storage kernel.

It is responsible for:

- fragments and storage tenancy boundaries
- immutable object/version metadata
- immutable blob payload placement
- one-shot and multipart upload
- logical reads and downloads
- lifecycle-first delete, trash, and recover
- garbage collection
- redistribution
- repair and recover-on-read
- standalone namespace routes
- SMB Lite namespace access

`N1 2.0` is best thought of as:

`object storage core + mutable namespace database`

It is not the user/account control plane and it is not the business metadata layer.

### N2

`N2` is the multi-node layer.

It extends the shared storage core with:

- cluster membership
- governance state
- shard placement
- peer transport
- reconcile behavior

Use `N2` when storage needs to span multiple nodes with cluster-managed placement and reconcile behavior. Use standalone `N1` when you want a single-node engine.

### Registry

`Registry` is the product-facing logical metadata and policy layer.

It owns:

- users and admin bootstrap
- login, refresh, logout, 2FA
- quotas and stats
- logical object namespace
- shares and public links
- storage-session issuance

Registry maps user-visible logical paths to stable storage identities in `N1` or `N2`. It is the right layer for user ownership, quotas, and product policy. It is not the layer that should repair shards, rebalance disks, or reclaim orphaned physical payloads.

## Architecture Overview

At a high level, the platform looks like this:

```text
Clients / UI / CLI
        |
        v
   Registry (optional)
        |
        v
     N1 or N2
        |
        v
  index roots + blob roots
```

For standalone `N1 2.0`, the internal model is:

```text
namespace DB (mutable)
  - paths
  - directories
  - rename / move
  - trash / recover

object metadata (immutable)
  - object_id
  - version lineage
  - size / hash / content metadata

blob storage (immutable)
  - payload shards / parity
  - placement
```

This split is why `N1 2.0` is materially different from the older path-as-identity design:

- path changes do not rewrite immutable object or blob truth
- delete becomes lifecycle-first
- GC is driven by reachability
- repair and redistribution stay storage-focused

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
- SMB Lite namespace access for normal file-manager flows

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
- `Registry + N1` for user-facing single-node product deployments
- `Registry + N2` for clustered deployments with centralized logical metadata

## Repository Layout

- [`n1`](/home/administrator/Development/N1/n1)
  `N1` server and storage kernel
- [`n2`](/home/administrator/Development/N1/n2)
  `N2` cluster layer
- [`registry`](/home/administrator/Development/N1/registry)
  Registry service
- [`n1_cli`](/home/administrator/Development/N1/n1_cli)
  Operator CLI for `N1 2.0` maintenance, health, fragment create, and multipart status
- [`n2_cli`](/home/administrator/Development/N1/n2_cli)
  CLI helpers for `N2`
- [`indexdb`](/home/administrator/Development/N1/indexdb)
  Index database crate used by the storage stack
- [`simd-erasure-core`](/home/administrator/Development/N1/simd-erasure-core)
  Erasure and shard primitives

Additional docs:

- [`DOCUMENTATION.md`](/home/administrator/Development/N1/DOCUMENTATION.md)
- [`N1_DOCUMENTATION.md`](/home/administrator/Development/N1/N1_DOCUMENTATION.md)
- [`N2_DOCUMENTATION.md`](/home/administrator/Development/N1/N2_DOCUMENTATION.md)
- [`REGISTRY_DOCUMENTATION.md`](/home/administrator/Development/N1/REGISTRY_DOCUMENTATION.md)

## Deployment Example

### Standalone N1

1. Prepare a config file.

Use [`standard/n1_config.ini`](/home/administrator/Development/N1/standard/n1_config.ini) or [`n1_config.ini`](/home/administrator/Development/N1/n1_config.ini) as the template.

Key sections:

- `[server]`
  bind host, HTTPS port, TLS cert/key
- `[disk_config]`
  `index_store_disks`, `blob_store_disks`, optional `fs_storage_path`
- `[upload_config]`
  durability and throughput knobs
- `[smb]`
  SMB Lite listener settings

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
    "key": "fragment-local-secret"
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

6. Use the token for uploads, reads, POSIX routes, or SMB Lite.

### N1 Operator CLI

`n1_cli` is now aligned with the stable `N1 2.0` surface. It is an operator CLI, not a general file-transfer client.

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
  fragment create --name <fragment_name> --key <fragment_key>

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

Use `N2` when you need cluster-managed placement instead of a single storage node. `N2` reuses the shared storage core and auth model, but adds cluster membership, governance, placement, and reconcile flows. Multi-node `N2` must not be configured as loopback-only.

## Configuration Overview

### N1 Config

The main runtime file is [`n1_config.ini`](/home/administrator/Development/N1/n1_config.ini).

Important settings:

- `server.host`, `server.https_port`
- `server.cert_path`, `server.key_path`
- `disk_config.index_store_disks`
- `disk_config.blob_store_disks`
- `garbage_collector.retention_seconds`
- `upload_config.data_shards`, `upload_config.parity_shards`
- `smb.enabled`, `smb.smb_port`

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

### SMB Lite

Enable SMB in `n1_config.ini`:

```ini
[smb]
enabled=true
bind_addr=0.0.0.0
smb_port=445
```

SMB Lite is intended for normal namespace operations:

- open/read/write
- mkdir
- rename/move
- delete to trash
- directory traversal

It is not a full Unix filesystem implementation with symlinks, hard links, xattrs, or full POSIX locking semantics.

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

`N1 2.0` is lifecycle-first:

- trash hides objects from the live namespace
- empty trash retires namespace visibility
- GC later reclaims unreachable payloads

If you need immediate space reclamation, empty trash and then run GC.

### Fragment Already Exists After Reusing The Same Name

Fragment names hash deterministically. Reusing the same name recreates the same fragment hash. If you want a brand-new fragment identity, use a different name. If a fragment was partially initialized, current `2.0` fragment create logic heals partial state on retry instead of false-conflicting.

### Listing Or Trash Views Look Wrong

The `2.0` namespace routes are the source of truth for standalone namespace behavior:

- `GET /n1/posix/list`
- `POST /n1/posix/mkdir`
- `POST /n1/posix/rename`
- `POST /n1/posix/trash`
- `POST /n1/posix/recover`

For trash browsing, the root trash list now returns actual trashed item paths. If a deleted item is a directory, it appears as a directory entry. If a deleted item is a nested file such as `videos/video.mp4`, the trash list returns that full file path directly.

### Self-Signed TLS

Many local deployments use self-signed or locally issued certificates. Use `curl -k` for local testing or configure trusted certificates in your client environment.

### SMB Lite Scope

SMB Lite now supports normal namespace operations, but it is intentionally narrower than a general-purpose Unix filesystem server.

## Development Notes

Useful workspace commands:

```bash
cargo check -p n1
cargo test -p n1 -- --test-threads=1
cargo test -p registry -- --test-threads=1
```

High-signal release tests used for `N1 2.0` include:

- CRUD upload/read/delete/recover flows
- GC reclaim after empty trash
- recover-on-read with disk loss
- repair rebuilding missing blob/index state
- redistribution with disk loss
- POSIX pagination, mkdir, rename, trash, recover
- SMB Lite read/write/mkdir/rename/trash/copy flows

If you are touching a subsystem boundary, prefer adding a focused regression test in that subsystem plus a workflow test when the behavior spans storage, namespace, and maintenance.
