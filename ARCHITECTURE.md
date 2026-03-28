# N2 Architecture

> Deployment profile status (2026-03-20):
> - `N2` is the product identity of this workspace.
> - `N1` is the shared storage engine used underneath both deployment shapes.
> - `n1` is the stable single-node deployment profile.
> - `n1` is released at `0.9.1`.
> - `n2` is the multi-node N2 deployment profile and is released at `0.2.0`.
> - `registry` is the SQLite-backed Registry/business layer and is versioned independently at `0.4.0`.
> - `n1-arc` is an experimental raw-device deployment profile.
> - See `DEPLOYMENT_PROFILES.md`.
> - For the multi-node operator runbook, see `N2_DEPLOYMENT.md`.

N2 has three runtime pieces in this workspace:

- single-node N2:
  - run `n1`
  - use the N1 engine directly on one node
- multi-node N2:
  - run `n2`
  - use that same N1 engine on each node, then add cluster governance, placement, and peer traffic
  - governance is consensus-scoped only to cluster state
  - committed membership epochs are snapshotted for rollback
  - object payload traffic remains direct and bidirectional
- registry layer:
  - run `registry`
  - keep user/business metadata out of the storage engine
  - use SQLite for both single-node and multi-node deployments

At the node layer, N1 is a single-node object storage engine with two distinct storage planes:

- `index` is the authoritative object, session, and trash metadata plane.
- The blob plane is profile-specific:
  - `n1`: filesystem-backed shard files under blob roots.
  - `n1-arc`: raw-device/direct-I/O payload plane with raw index authority.

At the platform layer, `registry` owns the business plane:

- first-admin bootstrap guarded by `REGISTRY_MASTER_KEY`
- administrator account control
- users
- rotating refresh sessions
- realtime websocket feed (`GET /v1/ws`)
- self-service account management
- optional TOTP 2FA
- ownership
- quotas
- logical listing/filtering
- folder/prefix simulation for clients
- prefix/search archive selection resolved into exact-key manifests for the engine
- fragment backend bindings
- storage-session brokering for Registry-backed clients

This replaces the legacy "full mirrored metadata tree on every index root" model. The node is still single-node, but its metadata authority is now partitioned instead of whole-node mirrored.

## Freeze Status

The current production architecture baseline is `n1 0.9.1`, `n2 0.2.0`, and `registry 0.4.0`.

That baseline applies to:

- filesystem-backed authoritative `index`
- partitioned + replicated metadata ownership
- registry-owned namespace/business serving above `index`
- stateless resumability through authoritative session state
- profile-specific payload substrate (`n1` filesystem-backed blob roots, `n1-arc` raw blob devices)

It does **not** freeze the HTTP/API surface yet. API, operational route details, and metadata layout can still change before `1.0.0` while the core storage architecture stays fixed.
Backward compatibility is not guaranteed on the `0.x` line; destructive redesigns remain allowed until the `1.0.0` contract is declared stable.

## N1 Core Identity And Variant Choice

N1 is intentionally a specialized object-storage engine, not a general filesystem:

- authoritative object/session/trash truth on disk
- immutable version publish discipline
- crash-first repairability
- low-overhead multipart payload paths

At the N1 core layer, two engine variants exist so operators can choose the execution substrate:

- `n1`: filesystem-backed N1 for general-purpose use and broad hardware compatibility.
- `n1-arc`: raw-device/direct-I/O N1 for absolute device control and maximum throughput ceiling, currently treated as experimental.

They are expected to keep the same storage semantics. The difference is backend execution model and operational profile, not object correctness guarantees.

## Profile Distinction (Current Runtime Truth)

| Dimension | N1 | N1-Arc |
|---|---|---|
| Status | Stable/default (`0.9.1`) | Experimental raw-device profile |
| `index_store_disks` | Filesystem directories | Raw devices |
| `blob_store_disks` | Filesystem directories | Raw devices |
| Index backend | Partitioned `indexdb` metadata (`.ndxdb`) | Raw index catalog (`block_manager`) |
| Blob backend | Filesystem shard files | Raw extent allocator (`block_manager`) |

N1-Arc currently runs raw index and raw blob on managed block-manager media.

### N1-Arc Raw-Index Direction

N1-Arc development is now targeting a raw-only storage-kernel model (`index` + `blob` both on raw devices). The staged structure and migration gates are defined in [`ARC_RAW_KERNEL_PLAN.md`](ARC_RAW_KERNEL_PLAN.md).

## Platform Truth

- Exact-key reads, finalize, delete, recover, GC, repair, redistribution, and flat listing all resolve against `index`.
- Each object key hashes to one metadata partition. Each partition selects one owning index root and one replica root when more than one root exists.
- Session state is partitioned separately by `fragment + version_id`, which keeps resumable upload state bounded to the session's selected roots.
- Multipart part truth stays on disk: part uploads write durable part metadata, and a pending-part receipt covers the crash window before that metadata exists.
- Blob payload durability (file shards in `n1`, raw extents in `n1-arc`) does not make an object visible until `indexdb` publishes the head.
- Health/monitoring now treats maintenance debt as part of engine truth: maintenance access mode, stale upload debt, and redistribution intent debt are surfaced directly from the engine.

## Why The Legacy Mirrored Index Had To Go

The legacy layout made every node-level metadata mutation expensive:

- final publish fanned object metadata out across mirrored roots
- session and part state lived in a separate mirrored tree
- namespace-serving logic shared the same metadata surface as exact-key reads
- repair and GC had to reconcile multiple partially authoritative trees

That model was durable, but once the blob plane became fast enough, the node ceiling moved to the metadata mirrors. The old design improved safety, not write scalability.

## Storage Domains

### 1. `index` authority plane

`index` stores the canonical object/session/trash state under hashed partitions:

- immutable version files
- latest markers for exact-key reads
- resumable upload session state with committed parts
- WAL records for mutation history and recovery tooling

Writes target the partition's selected roots, not every index root in the node.

### 2. Registry plane

The Registry plane is where namespace and business logic now belongs:

- `LIST` by logical owner/query
- folder/prefix simulation
- metadata filtering
- quota/accounting
- admin-created accounts and quota overrides
- registry-wide statistics aggregation
- exact-key logical lookup before engine access
- binding a fragment to either a `single_node` or `multi_node` storage backend
- issuing short-lived storage sessions to client applications

In the `0.9.1` / `0.2.0` / `0.4.0` baseline, the Registry is a dedicated binary. The engine contract is flat, exact-key-oriented object storage, while client-facing namespace behavior moves into the Registry layer.

Registry identity/session notes:
- the first administrator is created through a master-key bootstrap flow
- later browser/client sessions use rotating refresh tokens rather than long-lived bearer tokens
- 2FA is TOTP-based and enforced at the Registry identity layer, not in the storage engine
- public signup never sets its own quota; quota is server-default or admin-assigned
- account deletion cascades through Registry state and removes linked managed fragments through the engine before the user row is deleted

Registry-backed client flow:

1. user logs in to the Registry with email/password
2. a Registry-backed client selects or links a fragment to a storage deployment:
   - `single_node`
   - `multi_node`
3. the client requests a short-lived storage session from the Registry
4. Registry authenticates against the linked engine backend using its configured master access
5. Registry returns:
   - engine URL
   - short-lived fragment access token
6. the client sends object bytes directly to the engine

That keeps the Registry as the control plane and the engine as the direct data plane.

### 3. Blob plane

Blob payload substrate depends on profile:

- `n1`: multipart parts are RS-encoded into shard files under configured blob root directories.
- `n1-arc`: multipart parts are RS-encoded and committed to raw extents on managed blob devices.
- In both profiles, repaired or redistributed payload placement is published through authoritative metadata updates.

## On-Disk Layout

### `index` partitions

The authoritative metadata shape is:

```text
{INDEX_ROOT}/{fragment}/indexdb/partitions/{partition_id}.ndxdb
  object_heads
  object_versions
  sessions
```

Notes:

- object partitions are chosen from `hash("object", fragment, object_key)`
- session partitions are chosen from `hash("session", fragment, version_id)`
- when multiple index roots exist, a partition writes to one owner root plus one replica root
- the owner root is the synchronous ACK boundary for object/session state
- replica writes are intentionally replica-behind and may lag briefly until repair/replay catches up
- `object_heads` are the current truth for one object key
- `object_versions` are immutable history rows keyed by object key + version id
- `sessions` hold authoritative multipart upload state

### Engine Listing

Engine-side listing reads authoritative `object_heads` rows. It supports:

- flat pagination
- `prefix`
- `delimiter=/`
- opaque continuation tokens

Registry still owns business filtering, owner-scoped logical listing, and archive selection above
that storage truth.

### Blob storage

The payload plane remains:

- `n1`: filesystem-backed blob roots with shard files
- `n1-arc`: raw blob devices managed by `block_manager`
- shard locations are recorded in metadata using the active backend representation
- direct-I/O writes and reads are applied on the blob path where supported by the active backend

Standard blob layout:

```text
{BLOB_ROOT}/{fragment}/blob/{version_id}/
  part_{p}_shard_{i}.shard
  part_{p}_shard_{i}.parity

{BLOB_ROOT}/{fragment}/.trash/blob/{version_id}/...
{BLOB_ROOT}/{fragment}/.thumbs/{version_id}/thumb-{profile}.{ext}
```

Thumbnail note:
- `n1` thumbnails are derived cache files, not user objects.
- They are keyed by immutable `version_id` plus thumbnail profile (`width`, `height`, `fit`, `format`).
- They do not appear in object listings and can be regenerated from the original object version at any time.

N1-Arc blob layout:

```text
{RAW_BLOB_DEVICE}
  block-manager superblock + journals + allocation groups
  shard payload addressed as raw://{device_id}/{allocation_id}
```

N1-Arc index note:
- Index and blob are both raw in `n1-arc`.

Allocator design note:
- The target allocator architecture and rollout plan are defined in [`BLOCK_ALLOCATOR_V2.md`](BLOCK_ALLOCATOR_V2.md).
- Current allocator journals are txid-stamped and replay is idempotent.
- Large free workloads are deferred and applied by a bounded background worker to keep foreground paths predictable.

## Write Path

### 1. Initiate upload

Initiate does:

- validate fragment scope
- check available backing-store capacity
- generate `version_id` and `session_id`
- open `index` session state on the session partition

At this stage no object is visible.

### 1a. Standard one-shot upload

`n1` also exposes a direct single-request path:

- `POST /v4/upload/one-shot/{*object_key}`

That path skips session state and finalize for small objects:

1. validate fragment scope and request size
2. check live filesystem capacity
3. write blob payload once
4. publish immutable metadata + the authoritative head immediately
5. return success

### 2. Upload part

Part upload is still payload-first:

1. stream bytes through RS encode
2. write bytes to the active payload backend (`n1` shard files, `n1-arc` raw extents)
3. commit part metadata and session part state
4. build `PartMetadata`
5. update the session's `index` state with the committed part

This keeps metadata small and bounded: blob payload durability and part-session durability are separate from object visibility.

Upload geometry behavior:
- Explicit geometry (`part_size_bytes` / `part_sizes_bytes`) is strict.
- If geometry is not supplied, init derives deterministic geometry from `total_size_bytes + expected_parts` for bounded shard writes.

### 3. Finalize upload

Finalize is now an authoritative publish into `index`:

1. load session state from the session partition
2. verify that all required parts exist
3. build immutable `ObjectMetadata`
4. commit the new immutable version row into the object partition DB
5. advance the authoritative object head if the new version wins by recency
6. mark the session finalized and return success

Exact-key visibility depends on `indexdb` head publish, not on legacy mirrored metadata files.
For the current index model, the owner root is the synchronous publish boundary. Replica roots are
durability replicas behind that boundary, not part of the foreground ACK path.

### Crash matrix

- Crash before part state commit: partial payload artifacts may exist, but no session part state means the part is not complete.
- Crash after part state commit but before finalize: the upload is resumable and still not visible.
- Crash after head publish but before replica catch-up: `GET`, `HEAD`, and `LIST` are correct from the committed owner partition DB.

## Read And List Path

### Exact-key reads

`GET`, `HEAD`, metadata lookup, version lookup, delete, recover, and rename resolve through `index`:

- readers probe the routed partition DB copies for the partition
- current state resolves through `object_heads`
- immutable history resolves through `object_versions`
- blob addresses come from the published `ObjectMetadata`

### Derived thumbnails

`n1` also exposes a derived image preview path:

- `GET /v4/fragment/thumb/{*object_key}`

That path resolves the authoritative object version first, then:

- validates that the target is an image object
- checks for a disk-backed thumbnail cache entry under `.thumbs/{version_id}/...`
- generates the thumbnail lazily on first request when the cache entry is missing
- serves later requests from that derived file

This keeps image-heavy gallery browsing efficient without changing the core object model:

- no thumbnail rows in `index`
- no thumbnail entries in normal object listing
- no extra user-visible object versions

### Flat engine listing

`LIST` in the engine now pages over authoritative head rows:

- object responses come from `object_heads`
- `prefix` filters are supported directly by the engine
- `delimiter=/` folds child ranges into `common_prefixes`
- registry-backed products still own namespace semantics above the storage truth

That keeps exact-key correctness and engine listing on the same authority source: `index`.

## Maintenance Path

All major maintenance flows now share the same authority source:

- cleanup removes orphaned blob state and repairs fragment-local metadata shape
- repair reconciles partition replicas and blob availability from `index`
- redistribution republishes moved shard addresses through `index`
- GC expires trash versions and abandoned sessions through `index`
- health reporting reflects partitioned authority instead of expecting full-root mirrored equality

## Durability Invariants

- A version is visible only after its owner-root `index` publish succeeds.
- Raw extents without live session or object metadata are cleanup candidates, not visible data.
- Replica divergence is repairable, but the owner-acknowledged `index` state wins until replicas reconverge.
- Full-root byte-for-byte metadata equality is no longer an invariant. Partition ownership and replica survivability are the invariant.

## Current Authority Rules

- Upload recency is defined by session creation time. Finalize publishes that version; it does not get to redefine which same-key upload is newer.
- When second-level `created_at` timestamps tie, ULID `version_id` order is the tiebreaker for latest selection.
- Publish may write an immutable version file for an older version, but it must not advance the `latest` marker unless that version actually wins the recency comparison.
- Exact-key reads treat the `latest` marker as a routing hint and rehydrate the referenced immutable version record before serving metadata or bytes.
## Why This Design Is More Future-Proof

This architecture gives N1 a better long-term shape than the legacy mirrored tree for five reasons:

1. Metadata write fanout is bounded.
   Publish and session writes hit the owning partition roots, not every index root on the node.

2. Exact-key truth and engine listing share one authority source.
   `GET`, `HEAD`, and flat `LIST` all resolve from authoritative `index` state.

3. Maintenance has one authority source.
   Repair, GC, and sync no longer need to guess between mirrored object trees and session trees.

4. The model can evolve without another semantic rewrite.
   The current segmented WAL plus partition checkpoints already give a clean path to richer replay, compaction, and replica-repair behavior without changing API-visible behavior.

5. The same node-core model is what N2 runs in both deployment shapes.
   Single-node N2 exposes it directly through `n1`; multi-node N2 composes it with the distributed control plane in `n2`.

## HA Model

- Losing one selected partition root is survivable if the other selected root is still readable.
- Reads prefer the freshest available partition replica.
- Repair restores missing owner/replica copies from surviving authoritative state.
