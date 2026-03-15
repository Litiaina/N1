# N2 Architecture

> Deployment profile status (2026-03-13):
> - `N2` is the product identity of this workspace.
> - `N1` is the shared storage engine used underneath both deployment shapes.
> - `standard` is the stable single-node deployment profile.
> - `standard` is released at `0.5.2`.
> - `distribution-runtime` is the multi-node N2 deployment profile.
> - `arc` is an experimental raw-device deployment profile.
> - See `DEPLOYMENT_PROFILES.md`.

N2 has two deployment shapes:

- single-node N2:
  - run `standard`
  - use the N1 engine directly on one node
- multi-node N2:
  - run `distribution-runtime`
  - use that same N1 engine on each node, then add cluster membership, placement, and peer traffic

At the node layer, N1 is a single-node object storage engine with three distinct planes:

- `index` is the authoritative object, session, and trash metadata plane.
- The prefix namespace index is a derived serving accelerator for `LIST prefix` and `delimiter=/`.
- The blob plane is profile-specific:
  - `standard`: filesystem-backed shard files under blob roots.
  - `arc`: raw-device/direct-I/O payload plane with raw index authority.

This replaces the legacy "full mirrored metadata tree on every index root" model. The node is still single-node, but its metadata authority is now partitioned instead of whole-node mirrored.

## Freeze Status

The current production architecture baseline is `standard 0.5.2`.

That baseline applies to:

- filesystem-backed authoritative `index`
- partitioned + replicated metadata ownership
- projected namespace serving derived from `index`
- stateless resumability through authoritative session state
- profile-specific payload substrate (`standard` filesystem-backed blob roots, `arc` raw blob devices)

It does **not** freeze the HTTP/API surface yet. API and operational route details can still change before `1.0.0` while the core storage architecture stays fixed.

## N1 Core Identity And Variant Choice

N1 is intentionally a specialized object-storage engine, not a general filesystem:

- authoritative object/session/trash truth on disk
- immutable version publish discipline
- crash-first repairability
- low-overhead multipart payload paths

At the N1 core layer, two engine variants exist so operators can choose the execution substrate:

- `standard`: filesystem-backed N1 for general-purpose use and broad hardware compatibility.
- `arc`: raw-device/direct-I/O N1 for absolute device control and maximum throughput ceiling, currently treated as experimental.

They are expected to keep the same storage semantics. The difference is backend execution model and operational profile, not object correctness guarantees.

## Profile Distinction (Current Runtime Truth)

| Dimension | Standard | Arc |
|---|---|---|
| Status | Stable/default (`0.5.2`) | Experimental raw-device profile |
| `index_store_disks` | Filesystem directories | Raw devices |
| `blob_store_disks` | Filesystem directories | Raw devices |
| Index backend | Partitioned filesystem metadata | Raw index catalog (`block_manager`) |
| Blob backend | Filesystem shard files | Raw extent allocator (`block_manager`) |

Arc currently runs raw index and raw blob on managed block-manager media.

### Arc Raw-Index Direction

Arc development is now targeting a raw-only storage-kernel model (`index` + `blob` both on raw devices). The staged structure and migration gates are defined in [`ARC_RAW_KERNEL_PLAN.md`](ARC_RAW_KERNEL_PLAN.md).

## Platform Truth

- Exact-key reads, finalize, delete, recover, GC, repair, and prefix rebuild all resolve against `index`.
- Each object key hashes to one metadata partition. Each partition selects one owning index root and one replica root when more than one root exists.
- Session state is partitioned separately by `fragment + version_id`, which keeps resumable upload state bounded to the session's selected roots.
- Multipart part truth stays on disk: part uploads write durable part metadata, and a pending-part receipt covers the crash window before that metadata exists.
- The prefix index is not the source of truth. It is a rebuildable namespace-serving structure with inline list payload.
- Blob payload durability (file shards in standard, raw extents in arc) does not make an object visible until `index` publishes the version.

## Why The Legacy Mirrored Index Had To Go

The legacy layout made every node-level metadata mutation expensive:

- final publish fanned object metadata out across mirrored roots
- session and part state lived in a separate mirrored tree
- prefix listing shared the same metadata surface as exact-key reads
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

### 2. Prefix namespace plane

The prefix subsystem remains filesystem-backed, but it is no longer authoritative. It exists to serve:

- `LIST prefix`
- `LIST prefix delimiter=/`
- archive selection by prefix

Prefix rows now carry inline list metadata, and `standard` serves them from a root full-key accelerator. Mutations update only full-key rows; delimiter/common-prefix views are derived during the read path instead of being maintained as ancestor-prefix state. A prefix hit can serve list results without reopening object metadata per row. If the accelerator is missing, the node rebuilds that fragment from authoritative `index` metadata and then serves the request from the rebuilt accelerator.

### 3. Blob plane

Blob payload substrate depends on profile:

- `standard`: multipart parts are RS-encoded into shard files under configured blob root directories.
- `arc`: multipart parts are RS-encoded and committed to raw extents on managed blob devices.
- In both profiles, repaired or redistributed payload placement is published through authoritative metadata updates.

## On-Disk Layout

### `index` partitions

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

- object partitions are chosen from `hash("object", fragment, object_key)`
- session partitions are chosen from `hash("session", fragment, version_id)`
- when multiple index roots exist, a partition writes to one owner root plus one replica root
- the owner root is the synchronous ACK boundary for object/session state
- replica writes are intentionally replica-behind and may lag briefly until repair/replay catches up

### Prefix namespace index

The serving index remains under the prefix tree on index disks, but `standard` now treats it as a
root full-key accelerator instead of one eagerly-maintained bucket per ancestor prefix:

```text
{INDEX_ROOT}/{fragment}/index/__p__/v1/pfx/{p1}/{p2}/{root_prefix_hash}/
  compacted.msgpack
  seg_<id>.msgpack
```

This plane stores serving snapshots only. Live and trash listing each use one root accelerator bucket, and the node can rebuild that bucket from authoritative `index` whenever serving state needs regeneration.

### Blob storage

The payload plane remains:

- `standard`: filesystem-backed blob roots with shard files
- `arc`: raw blob devices managed by `block_manager`
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
- `standard` thumbnails are derived cache files, not user objects.
- They are keyed by immutable `version_id` plus thumbnail profile (`width`, `height`, `fit`, `format`).
- They do not appear in object listings and can be regenerated from the original object version at any time.

Arc blob layout:

```text
{RAW_BLOB_DEVICE}
  block-manager superblock + journals + allocation groups
  shard payload addressed as raw://{device_id}/{allocation_id}
```

Arc index note:
- Index and blob are both raw in Arc.

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

`standard` also exposes a direct single-request path:

- `POST /v4/upload/one-shot/{*object_key}`

That path skips session state and finalize for small objects:

1. validate fragment scope and request size
2. check live filesystem capacity
3. write blob payload once
4. publish immutable metadata + latest marker immediately
5. return success

### 2. Upload part

Part upload is still payload-first:

1. stream bytes through RS encode
2. write bytes to the active payload backend (`standard` shard files, `arc` raw extents)
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
4. write the version file and latest marker on the object partition's selected roots
5. append the partition WAL record
6. return success

Exact-key visibility depends on `index` publish, not on legacy mirrored metadata files.
For the current index model, the owner root is the synchronous publish boundary. Replica roots are durability replicas behind that boundary, not part of the foreground ACK path.

### Crash matrix

- Crash before part state commit: partial payload artifacts may exist, but no session part state means the part is not complete.
- Crash after part state commit but before finalize: the upload is resumable and still not visible.
- Crash after version publish but before prefix maintenance catches up: `GET` and `HEAD` are correct through `index`; `LIST` can serve from existing prefix rows or rebuild from authoritative metadata.

## Read And List Path

### Exact-key reads

`GET`, `HEAD`, metadata lookup, version lookup, delete, recover, and rename resolve through `index`:

- readers probe the selected roots for the partition
- the freshest readable replica is preferred
- blob addresses come from the published `ObjectMetadata`

### Derived thumbnails

`standard` also exposes a derived image preview path:

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

### Prefix and delimiter listing

`LIST` is intentionally separated from exact-key truth:

- normal serving path is the root full-key accelerator
- rows include inline list metadata
- delimiter rows materialize the folder illusion without real directories
- if the accelerator is missing, the node rebuilds it from authoritative `index` metadata before serving the request

That keeps exact-key correctness and namespace serving performance independent.

## Maintenance Path

All major maintenance flows now share the same authority source:

- cleanup removes orphaned blob state and repairs fragment-local metadata shape
- repair reconciles partition replicas and blob availability from `index`
- redistribution republishes moved shard addresses through `index`
- GC expires trash versions and abandoned sessions through `index`
- prefix rebuild regenerates namespace segments from `index`
- health reporting reflects partitioned authority instead of expecting full-root mirrored equality

## Durability Invariants

- A version is visible only after its owner-root `index` publish succeeds.
- Prefix rows must never invent visibility that `index` does not contain.
- Raw extents without live session or object metadata are cleanup candidates, not visible data.
- Replica divergence is repairable, but the owner-acknowledged `index` state wins until replicas reconverge.
- Full-root byte-for-byte metadata equality is no longer an invariant. Partition ownership and replica survivability are the invariant.

## Current Authority Rules

- Upload recency is defined by session creation time. Finalize publishes that version; it does not get to redefine which same-key upload is newer.
- When second-level `created_at` timestamps tie, ULID `version_id` order is the tiebreaker for latest selection.
- Publish may write an immutable version file for an older version, but it must not advance the `latest` marker unless that version actually wins the recency comparison.
- Exact-key reads treat the `latest` marker as a routing hint and rehydrate the referenced immutable version record before serving metadata or bytes.
- Prefix and delimiter rows remain derived serving state. They can lag `index`, but they must never override or invent exact-key truth.

## Why This Design Is More Future-Proof

This architecture gives N1 a better long-term shape than the legacy mirrored tree for five reasons:

1. Metadata write fanout is bounded.
   Publish and session writes hit the owning partition roots, not every index root on the node.

2. Exact-key truth is separated from namespace serving.
   `GET` and `HEAD` no longer depend on the same structure that powers prefix listing.

3. Maintenance has one authority source.
   Repair, GC, sync, and rebuild no longer need to guess between mirrored object trees and session trees.

4. The model can evolve without another semantic rewrite.
   The current segmented WAL plus partition checkpoints already give a clean path to richer replay, compaction, and replica-repair behavior without changing API-visible behavior.

5. The same node-core model is what N2 runs in both deployment shapes.
   Single-node N2 exposes it directly through `standard`; multi-node N2 composes it with the distributed control plane in `distribution-runtime`.

## HA Model

- Losing one selected partition root is survivable if the other selected root is still readable.
- Reads prefer the freshest available partition replica.
- Repair restores missing owner/replica copies from surviving authoritative state.
- Prefix state can lag or be rebuilt without making exact-key reads incorrect.
