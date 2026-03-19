<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N2</h1>
  <p>
    Unified object storage platform built on the <code>N1</code> core engine:
    single-node deployment through <code>standard</code>, multi-node deployment through
    <code>distribution-runtime</code>, and an experimental raw-device engine profile
    through <code>arc</code>.
  </p>
</div>

---

> Deployment profile status (2026-03-19):
> - `N2` is the product identity of this workspace.
> - `N1` is the shared node-local storage engine used by both deployments.
> - `standard` (`litiaina-n1-standard`) is the single-node N2 deployment path and the stable default.
> - `standard` is released at `0.6.1`.
> - `distribution-runtime` (`litiaina-n2`) is the multi-node N2 deployment path.
> - `registry-runtime` (`registry-server`) is the SQLite-backed business metadata layer for both deployment shapes.
> - `arc` (`litiaina-n1-arc`) is the experimental raw-device profile and is not the production default.
> - Prefer a non-root service user for `litiaina-n1-standard`, `litiaina-n2`, and `registry-server`. Keep `root` for `litiaina-n1-arc` raw-device operations only.
> - See `DEPLOYMENT_PROFILES.md` for the profile split and architecture guidance.
> - Operator shortcut: choose the deployment shape in `DEPLOYMENT.md` first, then ignore Arc unless you are explicitly doing raw-device testing.

## Table of Contents

- [What is N2 (platform architecture)](#what-is-n2-platform-architecture)
- [Profile Distinction And Disk Layout (Authoritative)](#profile-distinction-and-disk-layout-authoritative)
- [What N1 Is Inside N2](#what-n1-is-inside-n2)
- [Design principles](#design-principles)
- [Core concepts](#core-concepts)
- [Architecture (Authority vs Namespace vs Blob)](#architecture-authority-vs-namespace-vs-blob)
- [On-disk layout](#on-disk-layout)
  - [`index` partitions](#index-partitions)
  - [Blob storage](#blob-storage)
- [Write path (multipart upload)](#write-path-multipart-upload)
  - [Initiate upload](#1-initiate-upload)
  - [Upload part (payload first, metadata second)](#2-upload-part-payload-first-metadata-second)
  - [Finalize upload (publish into `index`)](#3-finalize-upload-publish-into-index)
  - [Crash matrix (what can exist on disk)](#crash-matrix-what-can-exist-on-disk)
- [Read path (exact key and list)](#read-path-exact-key-and-list)
- [Durability model](#durability-model)
  - [Write quorum](#write-quorum)
  - [Visibility and list behavior](#visibility-and-list-behavior)
- [Maintenance jobs](#maintenance-jobs)
  - [Repair (authoritative metadata + blob)](#repair-authoritative-metadata--blob)
  - [Recovery runtime (read and heal)](#recovery-runtime-read-and-heal)
  - [Redistribution](#redistribution)
  - [Garbage collection and trash](#garbage-collection-and-trash)
  - [Raw blob operations](#raw-blob-operations-Arc-only)
- [Why `index` is future-proof](#why-index-is-future-proof)
- [Operational notes](#operational-notes)
  - [Linux-first + direct I/O](#linux-first--direct-io)
  - [Preallocation](#preallocation)
  - [Directory sync](#directory-sync)
- [Admin CLIs](#admin-clis)
  - [How to run the CLIs](#how-to-run-the-clis)
  - [N1 Standard CLI](#n1-standard-cli)
  - [N2 Runtime CLI](#n2-runtime-cli)
- [Invariants (must always be true)](#invariants-must-always-be-true)
- [Glossary](#glossary)

---

For SMB network-drive deployment, see `NETWORK_DRIVE.md`.
The single-node N2 deployment ships with native SMB-lite via `fsgwd`; the old `fsd_mount + Samba` bridge is now retired.
For deployment and operator steps, see `DEPLOYMENT.md`.
For deployment profile guidance, see `DEPLOYMENT_PROFILES.md`.

N2 deployment split in this workspace:
- Single-node deployment: run `litiaina-n1-standard` from `standard`.
- Multi-node deployment: run `litiaina-n2` from `distribution-runtime`.
- Registry deployment: run `registry-server` from `registry-runtime`.
- If `registry.env` is missing in the current working directory, `registry-server` creates a template automatically and tells you which required value is still missing.
- Raw-device testing remains `litiaina-n1-arc` from `arc`.
- `standard` and `distribution-runtime` now share the same stable `standard/src/n1_core` implementation; `distribution-runtime` only adds cluster-specific `n2_core` and route layers.
- `registry-runtime` owns business logic and user convenience metadata: users, ownership, quotas, logical listing, and logical object lookup.
- `registry-runtime` also brokers short-lived storage sessions for Registry-backed clients and future GUI/web surfaces that are still under development.
- Product identity:
  - `N2` is the operator-facing platform identity.
  - `N1` is the shared storage engine underneath both deployment shapes.
- Stable operator surface today:
  - CLI + HTTP API first
  - web surfaces are still under development and are not part of the stable N2 release contract yet
- Security defaults:
  - access token default TTL: `15 minutes`
  - access token max TTL: `1 hour`
  - refresh token default TTL: `7 days`

Single-node N2 on stable/default `standard` (production):
- `index_store_disks` and `blob_store_disks` are directory paths.
- Do not run raw disk format commands.
- Auth now supports rotating refresh tokens through:
  - `POST /authenticate/refresh`
  - `POST /authenticate/logout`

```ini
[disk_config]
index_store_disks=/srv/n1/index0,/srv/n1/index1
blob_store_disks=/srv/n1/blob0,/srv/n1/blob1
```

`arc` raw media initialization:
- `format-blob-device` and `format-index-device` are Arc-only commands.
- Use the arc binary for the two-step safety flow.

```bash
sudo target/release/litiaina-n1-arc format-blob-device \
  --path /dev/disk/by-id/... \
  --prepare

sudo target/release/litiaina-n1-arc format-blob-device \
  --path /dev/disk/by-id/... \
  --nonce <nonce-from-prepare> \
  --fingerprint <fingerprint-from-prepare>
```

For raw index media pre-formatting (kernel-track), use the matching flow:

```bash
sudo target/release/litiaina-n1-arc format-index-device \
  --path /dev/disk/by-id/... \
  --prepare

sudo target/release/litiaina-n1-arc format-index-device \
  --path /dev/disk/by-id/... \
  --nonce <nonce-from-prepare> \
  --fingerprint <fingerprint-from-prepare>
```

If Arc uses one physical raw device for both `index_store_disks` and `blob_store_disks`, format that device once only.

Profile note:
- `profiles/n1/high-hw.ini` is for multi-disk, high-parallelism deployments.
- `profiles/n1/single-raw-nvme.ini` is arc-oriented and is the better starting point for one raw Linux-owned NVMe blob device.

## Release Status

- `standard` is the stable/default deployment profile at `0.6.1`.
- `registry-runtime` is available in `0.6.1` as the SQLite-backed Registry layer.
- `arc` is the experimental raw-device profile.
- Stable architecture baseline (`standard`):
  - filesystem-backed authoritative `index`
  - filesystem-backed `blob_store_disks` shard layout (directory roots)
  - partitioned + replicated metadata
  - fragment-scoped exact-key object serving
- Registry architecture baseline (`registry-runtime`):
  - user management and login
  - ownership and quota accounting
  - logical listing and filtering
  - registry-owned object metadata lookup
  - fragment-to-backend binding for `single_node` and `multi_node` storage targets
  - storage-session brokering for Registry-backed clients and future GUI/web integrations
- `standard` also exposes `POST /v4/upload/one-shot/{object_key}` for direct small-object upload without session/finalize overhead. The fragment comes from the authenticated claim.
- `standard` also exposes claim-scoped bulk mutation routes for high-count object workflows:
  - `POST /v4/fragment/object/bulk-delete`
  - `POST /v4/fragment/object/bulk-recover`
- `standard` also exposes `GET /v4/fragment/thumb/{*object_key}` for disk-backed derived image thumbnails. Thumbnails are keyed by object `version_id`, are not listed as normal objects, and are regenerated on demand.
- On Linux, `standard` prefers `io_uring` on the blob data path when available and falls back to `O_DIRECT` + blocking writes when it is not.
- Arc is the experimental raw-device profile with raw index/blob media managed by `block_manager`.
- The HTTP/API surface is **not** frozen yet. Route shapes, request fields, and operational endpoints may still change before `1.0.0`.

## Profile Distinction And Disk Layout (Authoritative)

| Dimension | Standard (`litiaina-n1-standard`) | Arc (`litiaina-n1-arc`) |
|---|---|---|
| Status | Stable / production default (`0.6.1`) | Experimental / raw-device profile |
| `index_store_disks` | Filesystem directories | Raw block devices / loopback files |
| `blob_store_disks` | Filesystem directories | Raw block devices / loopback files |
| Index metadata backend | Partitioned filesystem metadata | Raw index catalog on block-manager media |
| Blob payload backend | Shard files on blob roots | Raw extent allocator (`block_manager`) |
| Raw format commands | Not used | Required for explicit raw media prep |

### Standard Disk Layout (Current)

```text
{INDEX_ROOT}/{fragment}/index/partitions/{partition_id}/
  wal/manifest.msgpack
  wal/segments/{segment_id}.wal
  checkpoints/latest.msgpack
  sessions/{version_id}.session.msgpack
  objects/{hh}/{object_key_sha256}/{version_id}.msgpack
  objects/{hh}/{object_key_sha256}/latest.latest.msgpack
  trash/{hh}/{object_key_sha256}/{version_id}.msgpack
  trash/{hh}/{object_key_sha256}/latest.latest.msgpack

{BLOB_ROOT}/{fragment}/blob/{version_id}/
  part_{p}_shard_{i}.shard
  part_{p}_shard_{i}.parity

{BLOB_ROOT}/{fragment}/.trash/blob/{version_id}/...
```

### Arc Disk Layout (Current)

```text
{RAW_INDEX_DEVICE}
  block-manager superblock + journals + allocation groups
  raw index catalog allocation (authoritative sessions/versions/fragment metadata)

{RAW_BLOB_DEVICE}
  block-manager superblock + journals + allocation groups
  extents addressed as raw://{device_id}/{allocation_id}
```

Arc note:
- Index and blob are both raw block-manager media in Arc.
- Arc is experimental and remains under active performance and control-plane hardening.

## What is N2 (platform architecture)

**Litiaina N2** is the storage platform delivered by this repository.

It has two deployment shapes:

- **Single-node N2**
  - runs `standard` (`litiaina-n1-standard`)
  - uses the N1 engine directly on one node
- **Multi-node N2**
  - runs `distribution-runtime` (`litiaina-n2`)
  - uses the same N1 engine on every node, then adds cluster membership, placement, and peer data movement

Under both shapes, the node-local storage engine is still **N1**. N1 provides:

- a partitioned metadata authority in `index` (filesystem-backed in `standard`, raw in `arc`)
- fragment-scoped object storage with exact-key retrieval
- a profile-specific blob backend:
  - `standard`: filesystem-backed shard files under blob roots
  - `arc`: raw-device/direct-I/O blob plane with raw index authority

In the `0.6.1` baseline, user convenience behavior moves out of the engine and into the Registry layer:

- user management
- ownership tracking
- quota enforcement
- logical listing and filtering
- folder/prefix simulation for clients

That Registry layer runs as `registry-server` and uses SQLite in both single-node and multi-node deployments.

Registry-backed client flow now looks like this:

- user logs in to the Registry with email/password
- a Registry-backed client links a fragment to either:
  - `single_node`
  - `multi_node`
- the client asks the Registry for a short-lived storage session
- Registry authenticates against the selected engine backend and returns:
  - engine URL
  - short-lived fragment access token
- the client sends object bytes directly to the storage engine
- Registry remains the business/control plane

This keeps the data path direct while removing fragment-secret handling from Registry-backed clients.

Current shipped surface:

- CLI + HTTP API are the stable operator interfaces in this repository today
- web/admin surfaces remain under development and should not be treated as the stable N2 product surface yet

This is the architectural shift from the legacy node: object and session truth are no longer stored as a whole-node mirrored tree on every index root. Instead, metadata authority is partitioned by hash onto a small selected set of roots, and N2 composes that node-local engine into either a single-node or multi-node deployment.

N2 is designed for environments where you want:

- **NVMe / JBOD speed** without cluster coordination tax
- **video-grade streaming** with bounded memory
- **crash safety** and recoverability on unstable disks
- storage that remains **inspectable and movable on disk** without a database hot path

## What N1 Is Inside N2

N1 is the **fragment-scoped core storage engine** inside N2. It provides partitioned metadata authority and a profile-specific blob backend for node-local storage where:

- **high availability** during maintenance matters
- **durability** matters across index-root and blob-device failures
- **bounded resource usage** matters
- **no external database hot path** is required

Operationally, N1 means:

- fragments are the isolation boundary
- object versions are immutable
- `index` is the authoritative source for live objects, trash, and upload sessions
- object listing is flat and exact-key oriented
- maintenance jobs reconcile from the same authoritative metadata plane

## Engine Identity And Why Two Variants Exist

N1's core identity is a specialized object-storage engine: authoritative on-disk metadata truth, immutable version publish, crash-first recovery semantics, and lock-lean multipart data paths.

Both variants exist by design so operators can choose deployment style without changing object semantics:

- `standard`: filesystem-backed N1 for general-purpose deployments where operational simplicity and broad hardware compatibility are the priority.
- `arc`: raw-device/block-manager-backed N1 for operators that want absolute device control and the highest performance ceiling.

Both keep the same core behavior contracts (init/chunk/finalize semantics, immutable versions, repair/redistribution/GC guarantees). The choice is execution substrate and operational envelope, not storage correctness model.

### Fragment-centric engine (no multitenant UID layout)

N1 is **not** "user multi-tenant storage" anymore. The isolation boundary is a **Fragment**:

- **Fragment = virtual disk / namespace / tenant boundary**
- **Capacity is enforced from live filesystem totals on the node**
- **Auth scopes to Fragment**
- **Maintenance jobs operate per Fragment**
- N1 is the **shared node-local engine**: partitioned metadata authority, strict publish discipline, and repairable blob durability

---

## Design principles

### 1) Partitioned metadata authority, profile-specific blob plane

N1 keeps authoritative metadata on profile-specific media: mounted index filesystems in `standard`, raw index catalog media in `arc`. Blob payload backend depends on profile:
- `standard`: shard files under configured blob root directories.
- `arc`: raw, unformatted, unmounted devices managed directly by N1.

**Result:** the node keeps an inspectable on-disk layout while bounding metadata write fanout.

### 2) Exact-key truth without namespace coupling

Exact object visibility comes from `index`, and engine-side listing is a flat scan over authoritative latest-object state.

**Result:** `GET`, `HEAD`, and flat `LIST` all resolve from the same storage authority.

### 3) Immutability as correctness

A finalized object version is immutable:

- shards are never edited in place after commit
- new writes create a new `version_id`
- delete is represented safely through trash state
- rename is metadata-only

**Result:** crash safety is a publish-discipline problem, not a lock-ownership problem.

### 4) Crash-first engineering

N1 assumes failures are normal:

- process stops mid-upload
- one index root disappears
- blob devices return corrupt or missing data
- namespace-serving state lags behind object truth

N1 stays correct by making `index` the only object authority and treating everything else as repairable or rebuildable around it.

---

## Core concepts

### Fragment

A Fragment is the unit of isolation and operational scope:

- virtual disk boundary
- auth boundary
- maintenance boundary
- namespace boundary

### Object

An object is identified by:

- `fragment`
- `object_key`

### Version

A version is immutable and identified by `version_id`. Publish makes a version visible.

### Session

A multipart upload session is routed by `fragment + version_id` onto one owner index root plus one replica root when available.

That routed session state holds:

- upload manifest
- optional reservation receipt
- optional upload geometry hints (arc may pre-reserve raw slots)
- finalized or aborted state

Committed multipart progress is not kept as shared in-memory/session-WAL part state. Each committed part is represented by its own durable part metadata receipt on the routed session roots.

### Partition

A partition is the placement unit for authoritative metadata:

- object partitions hash from `fragment + object_key`
- session partitions hash from `fragment + version_id`
- each partition selects one owner root plus one replica root when more than one index root exists
- the owner root is the synchronous ACK boundary for object/session state
- replica writes are replica-behind by design and reconcile after the owner-acknowledged write path

### Shard

Each multipart part is encoded into data and parity shards whose payloads are committed to the active blob substrate (filesystem files in `standard`, raw extents in `arc`).

---

## Architecture (Authority vs Namespace vs Blob)

### `index` authority plane

`index` stores:

- live object versions
- latest markers
- trash versions
- upload sessions and committed parts
- WAL records for mutation history

This is the authoritative plane used by exact-key reads, finalize, delete/recover, sync, repair, GC, redistribution, and flat listing.

### Blob plane

The blob plane stores payload shards on the profile-selected substrate:

- `standard`: filesystem-backed shard files under blob roots
- `arc`: raw devices and extent allocation
- RS-encoded data + parity shards
- deterministic placement
- degraded reads via reconstruction
- background repair and redistribution

---

## On-disk layout

### `index` partitions (standard)

The authoritative metadata shape is:

```text
{INDEX_ROOT}/{fragment}/index/partitions/{partition_id}/
  wal/manifest.msgpack
  wal/segments/{segment_id}.wal
  checkpoints/latest.msgpack
  sessions/
    {version_id}.session.msgpack
  objects/{hh}/{object_key_sha256}/
    latest.latest.msgpack
    {version_id}.msgpack
  trash/{hh}/{object_key_sha256}/
    latest.latest.msgpack
    {version_id}.msgpack
```

Notes:

- `partition_id` comes from a stable hash route
- object and session state are partitioned independently
- writes target the partition's selected roots, not every index root in the node
- success requires at least one selected root to persist the update

### Blob storage (`standard`)

- `blob_store_disks` point at mounted directory paths (for example `/srv/n1/blob0`)
- payload shards are files: `part_{p}_shard_{i}.shard|.parity`
- trash payloads move under `{BLOB_ROOT}/{fragment}/.trash/blob/{version_id}/...`

### Blob storage (`arc`)

- `blob_store_disks` point at raw device paths (for example `/dev/disk/by-id/...`)
- shard metadata stores raw extent addresses (`raw://<device_id>/<allocation_id>`)
- arc startup rejects mounted blob devices and warns on unstable names like `/dev/sdb`

**Deterministic placement rule (typical):**

- choose disk for each shard as `(part_index + shard_index) % num_disks`
- require at least `data_shards` successful writers for the part to be accepted

## Write path (multipart upload)

### 1) Initiate upload

Initiate does:

- validate fragment scope
- check available storage headroom on the current backing filesystems or raw media
- create `version_id` and `session_id`
- optionally freeze upload geometry when the client provides `part_size_bytes` or `part_sizes_bytes`
- open authoritative session state in `index`

At this point no object version is visible.

### 1a) One-shot upload (`standard`)

For small single-request objects, `standard` also exposes `POST /v4/upload/one-shot/{*object_key}`. The fragment is claim-scoped and is not passed in the path.

That path:

- validates fragment scope and request size
- checks available filesystem capacity
- writes the blob once
- publishes final metadata + latest immediately
- returns without creating an upload session or requiring finalize

For large file-management actions on many keys, `standard` also exposes claim-scoped bulk object mutation routes:

- `POST /v4/fragment/object/bulk-delete`
- `POST /v4/fragment/object/bulk-recover`

These accept a JSON body of the form:

```json
{
  "object_keys": [
    "xray/2025/02-02-2026/doctor/files/images/ppacs/file-001.dcm",
    "xray/2025/02-02-2026/doctor/files/images/ppacs/file-002.dcm"
  ]
}
```

They are intended for UI or operator flows that need to mutate hundreds or thousands of objects without paying one HTTPS request per object.

### 1b) Derived thumbnails (`standard`)

For image-heavy browsing, `standard` also exposes `GET /v4/fragment/thumb/{*object_key}`.

That path:

- validates fragment scope and resolves the current or requested object version
- loads the original image once
- generates a bounded thumbnail on demand
- stores the derived thumbnail on disk under a hidden cache path keyed by `fragment + version_id + profile`
- serves later requests from that disk cache instead of reloading the full original image

This is a derived cache, not a second user object:

- thumbnails do not appear in normal object listings
- thumbnails do not create new object versions
- cache keys are version-aware, so a new upload of the same object key produces a fresh thumbnail path

### 2) Upload part (payload first, metadata second)

Part upload remains strict and payload-first:

1. stream request bytes through the RS encoder
2. place shard payloads using the active backend (`standard` file paths, `arc` raw extents)
3. write and verify shard payloads
4. build `PartMetadata`
5. atomically write the independent part metadata receipt on the routed session roots

Upload status, resume checks, and finalize read the committed part receipts from disk. The session manifest remains the authoritative upload manifest; per-part truth lives in durable receipt files, not in a shared multipart coordinator.

### 3) Finalize upload (publish into `index`)

Finalization is the publish step:

- load the authoritative session state
- verify all expected parts exist
- build immutable `ObjectMetadata`
- write the object version and latest marker to the selected object-partition roots
- append the partition WAL record

After this, exact-key reads are visible. Prefix listing is serving state layered on top of that publish.

---

## Crash matrix (what can exist on disk)

This is the mental model N1 is built around now.

### Crash during part upload (before session part commit)

You may see:

- partial shard payload files/extents
- no committed part metadata yet

The part is not complete because committed part metadata does not claim it yet. GC or cancel can reclaim orphaned payload artifacts.

### Crash after session part commit but before finalize

You may see:

- committed shard payload artifacts
- committed part metadata on disk
- no published object version

This is safe and resumable. Finalize can be retried.

### Crash after version publish but before prefix state catches up

You may see:

- published object state in `index`
- stale or missing prefix rows

`GET` and `HEAD` are still correct. `LIST` can use existing namespace rows or fall back to authoritative `index` state and rebuild.

---

## Read path (exact key and list)

Exact-key reads are driven by `index`:

1. resolve the latest or requested version from the object's partition
2. map the requested range onto parts and shard locations
3. read shard chunks from the active backend (files in `standard`, raw devices in `arc`)
4. reconstruct missing data when parity allows

Engine-side listing is flat and paginated from authoritative `index` state. Namespace browsing, folder simulation, and prefix filtering belong in the Registry layer.

---

## Durability model

### Write quorum

A part upload still requires at minimum:

- `data_shards` successful payload writers

If fewer than `data_shards` shards can be written and committed, the part upload fails.

For authoritative metadata:

- `index` writes target the selected owner/replica roots for that partition
- the owner root is the synchronous ACK boundary
- replica roots are durability copies behind the ACK path and may lag briefly until repair/replay catches up

### Visibility and list behavior

A version becomes exact-key visible only after:

- shard payload commit succeeds
- session part state is committed
- `index` publishes the object version

Prefix visibility is intentionally softer:

- prefix rows are serving state
- they may lag or be rebuilt
- they must never invent visibility that `index` does not contain

### Authority rules

- Upload recency is defined by the session manifest's `created_at`, not by when finalize returns.
- If two versions land in the same second-level timestamp bucket, ULID `version_id` order breaks the tie.
- Publish only advances the latest marker when the candidate is actually newer; a stale finalize must not demote the visible version.
- Exact-key reads resolve the latest marker and then rehydrate the referenced immutable version record before serving metadata or bytes.
- Prefix rows are derived serving state only; if they are pending, missing, or stale, `LIST` falls back to authoritative `index` scans for the requested prefix.

---

## Maintenance jobs

### Repair (authoritative metadata + blob)

Repair now reconciles from `index`:

- heal missing or stale partition replicas
- verify shard availability or checksum
- reconstruct missing or corrupt payloads
- republish repaired metadata through authoritative state

Execution modes:

- `availability`: keep CRUD online and hold repair concurrency to a steadier, bandwidth-friendlier level
- `strict`: keep reads online, block mutations, and run repair harder
- `scrub`: quiesce reads and writes and use the widest safe repair concurrency
- fragment scans are streamed incrementally from partition roots instead of being materialized fragment-wide first
- API compatibility: no maintenance route/path changes; mode parsing now also accepts `check_mode`/`execution_mode` and `access_mode` aliases.

### Recovery runtime (read and heal)

Reads can degrade gracefully. When a shard is missing or corrupt:

- serve bytes by reconstructing them
- repair the missing shard immediately when this read owns the repair token
- commit repaired payload to a new backend location (file path in `standard`, raw extent in `arc`)
- preserve authoritative metadata through `index`

### Redistribution

Redistribution moves payloads to new blob placement without interrupting CRUD:

- read from current placement
- write to new backend placement
- republish shard addresses through `index`
- leave old payload artifacts for later GC

Redistribution scopes:

- `all` (default): blob+index update in one pass
- `blob`: move/re-encode blob shards and republish touched metadata
- `index`: rebalance metadata replicas only (no blob rewrite)

Execution modes:

- `availability`: keep CRUD online and intentionally cap redistribution pressure lower
- `strict`: make the node read-only and redistribute faster across the live layout
- `scrub`: quiesce reads and writes and use the widest safe redistribution concurrency
- fragment scans are streamed incrementally from partition roots so large fragments do not require full metadata vectors up front
- API compatibility: no redistribution route/path changes; mode parsing now also accepts `check_mode`/`execution_mode` and `access_mode` aliases.
- cancellation semantics are cooperative: job phase becomes `Stopped by User`; in-flight object tasks that return `Stopped` are no longer counted as hard migration failures.
- each object migration writes a redistribution intent marker on index roots; interrupted objects are replayed on the next redistribution run before fresh scan work.

### Garbage collection and trash

N1 still favors safety:

- delete moves versions into trash state
- destructive deletion is handled by GC
- abandoned sessions are cleaned through `index`
- payload artifacts without authoritative metadata are cleanup candidate data

### Raw blob operations (`arc` only)

- `GET /v4/maintenance/blob/raw/status` reports device and allocator state
- scrub verifies committed raw payload
- raw blob maintenance remains orthogonal to metadata authority

---

## Why `index` is future-proof

The legacy mirrored tree was safe, but it scaled badly because every publish and every maintenance path wanted to touch the same whole-node metadata surface. `index` is more future-proof because:

1. It bounds write fanout.
   Metadata writes hit the owning partition roots, not every index disk.

2. It separates engine storage from business namespace behavior.
   Exact-key truth lives in `index`; user-facing browsing belongs in the Registry layer.

3. It gives maintenance one source of truth.
   Repair, GC, sync, and redistribution all converge on the same authority.

4. It provides a clean evolution path.
   The current segmented WAL plus partition checkpoints already provide a clean base for stronger replay, compaction, and replica-repair behavior without another semantic rewrite.

5. It composes into N2 cleanly.
   N2 can remain distributed at the cluster layer while inheriting a cheaper and clearer node-local authority model.

---

## Operational notes

### Linux-first + direct I/O

N1 is Linux-first and uses direct I/O on the blob data path for predictable latency and reduced page-cache pressure.

This applies to:

- filesystem-backed blob/staging writes in `standard`, which prefer Linux `io_uring` when available and fall back to `O_DIRECT` + blocking writes when not
- raw blob devices managed directly by `block_manager` in `arc`

Metadata and serving state are profile-specific:
- `standard`: metadata/index/prefix state are filesystem-backed and buffered; capacity monitoring uses live filesystem totals.
- `arc`: metadata and payload are both raw-device backed via `block_manager`.

### Preallocation

Shard payloads are often preallocated to:

- reduce fragmentation
- make failure modes clearer
- enforce deterministic shard-size expectations

### Directory sync

Durable metadata publish still relies on atomic file replace plus directory sync:

- `fsync(file)`
- `rename(tmp, stable)`
- `fsync(parent_dir)`

That discipline is now applied to partition state rather than a whole mirrored metadata tree.

## Admin CLIs

This workspace now has two dedicated admin shells:

- `litiaina-n1-cli` for `N1 Standard`
- `litiaina-n2-cli` for `distribution-runtime`

Runtime binaries are server-only:

- `litiaina-n1-standard`
- `litiaina-n2`

### How to run the CLIs

Use the dedicated Standard CLI:

```bash
cargo run -p standard_cli -- <command> [flags]
```

Use the dedicated N2 CLI:

```bash
cargo run -p distribution_cli -- <command> [flags]
```

Built binaries:

```bash
./target/release/litiaina-n1-cli <command> [flags]
./target/release/litiaina-n2-cli <command> [flags]
```

Server runtime:

```bash
cargo run -p standard
```

Or run the built server directly:

```bash
./target/release/litiaina-n1-standard
```

`litiaina-n1-standard` and `litiaina-n2` no longer double as admin CLIs. If you pass command arguments to them, they exit and tell you to use the dedicated CLI binaries.

### N1 Standard CLI

### Global CLI flags

Available on all commands:

- `-s, --secret <SECRET>`: secret input for auth
- `-f, --fragment <FRAGMENT>`: optional maintenance target fragment
- `--auth-fragment <AUTH_FRAGMENT>`: auth context fragment used during `/authenticate/fragment`
- `--url <URL>`: explicit N1 API URL
- `--insecure <bool>`: TLS cert validation toggle (default `true`)
- `--poll-ms <N>`: watch polling interval in milliseconds (default `1000`)
- `--json <bool>`: machine-readable output mode (default `false`)

Auth behavior for CLI:
- Authentication context (`--auth-fragment`) is independent from operation target (`--fragment`).
- Maintenance/admin flows use `MASTER_KEY` as `--secret`; GC does not require a target fragment.
- Repair/redistribution accept optional `--fragment` to run single-fragment scope.
- Upload status/listing and health are claim-scoped; they do not require a separate target fragment in the CLI.
- `litiaina-n1-cli` and `litiaina-n2-cli` now keep refresh tokens in-memory and automatically rotate access tokens during long-running watch/poll commands.

### Top-level commands

#### `fragment create`
Creates a new fragment.

Flags:
- `--name <NAME>` required
- `--key <KEY>` required

```bash
cargo run -p standard_cli -- fragment create --name imaging-a --key supersecretfragmentkey
```

#### `health`
Gets health once, or watches continuously.

Flags:
- `--watch`
- `--deep-scan`

Health reports:
- system CPU/RAM plus process RSS/open-FD pressure
- active upload sessions, reserved bytes, and committed multipart progress
- index runtime counters and raw/blob storage totals
- per-disk health plus total/used/free capacity on every configured index and blob path

```bash
cargo run -p standard_cli -- health --watch --deep-scan
```

#### `uploads`
Inspect active multipart sessions and resumable upload progress.

```bash
cargo run -p standard_cli -- uploads list
cargo run -p standard_cli -- uploads status --version-id <version_id>
```

#### `gc`
Lifecycle control for garbage collection.

```bash
cargo run -p standard_cli -- gc start
cargo run -p standard_cli -- gc status
cargo run -p standard_cli -- gc watch
cargo run -p standard_cli -- gc stop
```

#### `repair`
Lifecycle control for repair.

Flags:
- `--dry-run` optional
- `--mode availability|checksum|strict|scrub`
- `--fast-mode`
- `--read-only-fast`
- `--meta-concurrency <N>`
- `--repair-concurrency <N>`
- `--batch <N>`

```bash
cargo run -p standard_cli -- repair start --fragment <fragment_hash> --dry-run
cargo run -p standard_cli -- repair status
cargo run -p standard_cli -- repair watch
```

#### `redistribute`
Lifecycle control for redistribution.

Flags:
- `--dry-run` optional
- `--mode availability|strict|scrub`
- `--fast-mode`
- `--read-only-fast`
- `--durability auto|strict|batched`
- `--object-concurrency <N>`
- `--status-batch <N>`

```bash
cargo run -p standard_cli -- redistribute start --fragment <fragment_hash>
cargo run -p standard_cli -- redistribute status
cargo run -p standard_cli -- redistribute stop
```

### Watch behavior

The Standard CLI watch path is intentionally lightweight now:

- health watch prints one compact line per poll
- maintenance job watch prints only when the status actually changes
- no full-screen redraw loop
- no ANSI screen clearing or terminal state management

### N2 Runtime CLI

`distribution-runtime` has its own admin CLI:

- crate: `distribution_cli`
- binary: `litiaina-n2-cli`

It includes the shared maintenance/admin surface plus N2-specific cluster commands:

- `cluster-view [--public] [--expected-epoch <N>]`
- `membership get`
- `membership apply --file membership.json`
- `membership rollback --epoch <N>`
- `placement --fragment <fragment> --object-key <key> [--uid ... --version-id ... --kc ... --mc ...]`

Examples:

```bash
cargo run -p distribution_cli -- cluster-view
cargo run -p distribution_cli -- membership get
cargo run -p distribution_cli -- membership rollback --epoch 12
cargo run -p distribution_cli -- placement --fragment imaging-a --object-key scans/ct/0001.dcm
```

Sequential stream probe (readahead/locality):
```bash
N1_STREAM_PROBE_OBJECT_BYTES=$((64*1024*1024)) \
N1_STREAM_PROBE_PART_BYTES=$((4*1024*1024)) \
N1_STREAM_PROBE_PASSES=2 \
cargo test -p standard matrix_stream_sequential_readahead_locality_probe -- --ignored --nocapture
```

### General Testing

```bash
# Core server test suite
cargo test -p standard

# Dedicated Standard CLI
cargo check -p standard_cli

# Dedicated N2 CLI
cargo check -p distribution_cli

# Ignored/manual suites
cargo test -p standard -- --ignored --nocapture
```

---

## Invariants (must always be true)

These are the non-negotiables for correctness:

1. **No temp filenames in committed metadata**
   - `PartMetadata` must reference stable shard names only
2. **Stable shard commit before session part state commit**
3. **`index` publish is the only exact-key visibility gate**
4. **Never mutate committed shards in place**
5. **Every commit uses rename + directory sync**
6. **Write quorum must hold**
   - fail the part if healthy writers < `data_shards`
7. **Repair writes are also immutable**
   - repair via temp → rename, never patch

---

## Glossary

- **fragment**: virtual disk / tenant boundary
- **object_key**: object path/name within a fragment
- **version_id**: immutable identifier of a committed object version
- **session_id**: upload session identifier for resumability
- **part**: multipart chunk with shard map
- **shard**: one data/parity extent on blob storage
- **data_shards / parity_shards**: Reed-Solomon parameters
- **stripe_size_bytes**: block size used per shard per stripe

---

## Chaos benchmark harness

N1 includes a dependency-free HA chaos runner at:

`scripts/bench/run_ha_chaos.sh`

It repeatedly runs the ignored test `matrix_ha_chaos_profile_nightly` and writes:

- per-run logs (`run_N.log`)
- machine-readable CSV summary (`summary.csv`)
- pass/fail summary (`summary.txt`)

### Quick start

```bash
scripts/bench/run_ha_chaos.sh
```

### Custom stress profile

```bash
RUNS=10 \
N1_SOAK_ROUNDS=20 \
N1_SOAK_OPS_PER_ROUND=32 \
N1_SOAK_CLIENT_CONCURRENCY=12 \
N1_SOAK_DISK_DOWN=1 \
scripts/bench/run_ha_chaos.sh
```

### CI-style gating

Fail if any run fails and average runtime exceeds 600s:

```bash
RUNS=5 \
MAX_DURATION_SECS=600 \
REQUIRE_ALL_PASS=1 \
scripts/bench/run_ha_chaos.sh
```

All outputs are written under `.n1-bench/ha-chaos-<timestamp>/` by default.

## Release drill harness

For pre-release hardening on target hardware, use:

`scripts/bench/run_release_drills.sh`

It runs the release-critical storage/repair workflows:

- active multipart upload + resume while one replica index disk is down
- blob CRUD + delete/recover/permanent delete
- live capacity reporting and storage-path reconciliation
- blob redistribution + repair + recovered read + GC marker cleanup
- index mirror repair convergence
- HA repair reconvergence with an index disk down

It also optionally chains into the nightly HA chaos harness.

### Quick start

```bash
scripts/bench/run_release_drills.sh
```

### Harder gate on real hardware

```bash
RUNS=3 \
RUN_CHAOS=1 \
CHAOS_RUNS=5 \
CHAOS_MAX_DURATION_SECS=900 \
scripts/bench/run_release_drills.sh
```

Optional owner-first multipart soak:

```bash
RUN_INDEX_OWNER_RESUME_SOAK=1 \
INDEX_OWNER_RESUME_SOAK_ROUNDS=10 \
scripts/bench/run_release_drills.sh
```

Outputs are written under `.n1-bench/release-drills-<timestamp>/`.

## Arc allocator certification harness

For Arc allocator/formatter closure gates, use:

`scripts/bench/run_arc_allocator_gate.sh`

Quick gate:

```bash
MODE=quick scripts/bench/run_arc_allocator_gate.sh
```

Full gate (release-candidate):

```bash
MODE=full \
RELEASE_RUNS=1 \
CHAOS_RUNS=3 \
INDEX_SOAK=1 \
scripts/bench/run_arc_allocator_gate.sh
```

Outputs are written under `.n1-bench/arc-allocator-gate-<timestamp>/`.

Current certification snapshot:
- `ARC_ALLOCATOR_CERT_REPORT.md`

## Performance probes

N1 also ships a wrapper for the ignored performance probes:

`scripts/bench/run_perf_probes.sh`

This runs:

- `matrix_crud_cpu_ram_probe`
- `matrix_upload_single_nvme_saturation_probe`
- `matrix_stream_sequential_readahead_locality_probe`

### Quick start

```bash
scripts/bench/run_perf_probes.sh
```

### Example tuned run

```bash
N1_CRUD_PROBE_ROUNDS=2 \
N1_CRUD_PROBE_OBJECTS=1024 \
N1_CRUD_PROBE_CONCURRENCY=24 \
N1_UPLOAD_PROBE_CONCURRENCY=8 \
N1_UPLOAD_PROBE_OBJECTS_PER_WORKER=4 \
N1_STREAM_PROBE_PASSES=3 \
scripts/bench/run_perf_probes.sh
```

Outputs are written under `.n1-bench/perf-probes-<timestamp>/`.

---

© Litiaina --- N2
