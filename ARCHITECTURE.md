# N1 Architecture

N1 is a single-node object storage engine with three distinct planes:

- `index_v2` is the authoritative object, session, and trash metadata plane.
- The prefix namespace index is a derived serving accelerator for `LIST prefix` and `delimiter=/`.
- The blob plane stores payload shards on raw devices through direct I/O.

This replaces the legacy "full mirrored metadata tree on every index root" model. The node is still single-node, but its metadata authority is now partitioned instead of whole-node mirrored.

## Platform Truth

- Exact-key reads, finalize, delete, recover, GC, repair, and prefix rebuild all resolve against `index_v2`.
- Each object key hashes to one metadata partition. Each partition selects one owning index root and one replica root when more than one root exists.
- Session state is partitioned separately by `fragment + version_id`, which keeps resumable upload state bounded to the session's selected roots.
- The prefix index is not the source of truth. It is a rebuildable namespace-serving structure with inline list payload.
- Raw blob extents are payload durability only. They do not make an object visible until `index_v2` publishes the version.

## Why The Legacy Mirrored Index Had To Go

The legacy layout made every node-level metadata mutation expensive:

- final publish fanned object metadata out across mirrored roots
- session and part state lived in a separate mirrored tree
- prefix listing shared the same metadata surface as exact-key reads
- repair and GC had to reconcile multiple partially authoritative trees

That model was durable, but once the blob plane became fast enough, the node ceiling moved to the metadata mirrors. The old design improved safety, not write scalability.

## Storage Domains

### 1. `index_v2` authority plane

`index_v2` stores the canonical object/session/trash state under hashed partitions:

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

Prefix rows now carry inline list metadata, and the namespace plane is projected from `index_v2` WAL changes into compacted prefix snapshots. Projection is asynchronous and coalesced per fragment, so repeated writes do not re-run namespace maintenance in the foreground. A prefix hit can serve list results without reopening object metadata per row. If prefix state is pending, missing, or stale, the node falls back to authoritative `index_v2` scans for the requested prefix and can rebuild the namespace plane from there.

### 3. Blob plane

Blob payloads live on raw, unformatted blob devices managed by the node:

- multipart parts are RS-encoded into data and parity shards
- shard payloads are committed to raw extents
- repaired or redistributed payloads publish new raw extent addresses through authoritative metadata updates

## On-Disk Layout

### `index_v2` partitions

The authoritative metadata shape is:

```text
{INDEX_ROOT}/{fragment}/index_v2/partitions/{partition_id}/
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
- success requires at least one selected root to persist the state

### Prefix namespace index

The serving index remains under the `index/prefix/...` tree:

```text
{INDEX_ROOT}/{fragment}/index/prefix/<version>/<namespace>/{p1}/{p2}/{prefix_hash}/
  compacted...
  seg_...
  delimiter...
```

This plane stores compacted and recent namespace segments only. It can be compacted, repaired, or rebuilt from `index_v2`.

### Blob storage

The payload plane remains:

- raw blob devices managed by `block_manager`
- shard addresses recorded as `raw://<device_id>/<allocation_id>`
- direct-I/O writes and reads on the blob path

## Write Path

### 1. Initiate upload

Initiate does:

- validate fragment scope
- reserve quota
- generate `version_id` and `session_id`
- open `index_v2` session state on the session partition

At this stage no object is visible.

### 2. Upload part

Part upload is still payload-first:

1. stream bytes through RS encode
2. write staged shard data
3. commit staged shard data into raw blob extents
4. build `PartMetadata`
5. update the session's `index_v2` state with the committed part

This keeps metadata small and bounded: blob payload durability and part-session durability are separate from object visibility.

### 3. Finalize upload

Finalize is now an authoritative publish into `index_v2`:

1. load session state from the session partition
2. verify that all required parts exist
3. build immutable `ObjectMetadata`
4. write the version file and latest marker on the object partition's selected roots
5. append the partition WAL record
6. return success

Exact-key visibility depends on `index_v2` publish, not on legacy mirrored metadata files.

### Crash matrix

- Crash before part state commit: raw extents may exist, but no session part state means the part is not complete.
- Crash after part state commit but before finalize: the upload is resumable and still not visible.
- Crash after version publish but before prefix maintenance catches up: `GET` and `HEAD` are correct through `index_v2`; `LIST` can serve from existing prefix rows or rebuild from authoritative metadata.

## Read And List Path

### Exact-key reads

`GET`, `HEAD`, metadata lookup, version lookup, delete, recover, and rename resolve through `index_v2`:

- readers probe the selected roots for the partition
- the freshest readable replica is preferred
- blob addresses come from the published `ObjectMetadata`

### Prefix and delimiter listing

`LIST` is intentionally separated from exact-key truth:

- normal serving path is the prefix namespace index
- rows include inline list metadata
- delimiter rows materialize the folder illusion without real directories
- fallback scans authoritative `index_v2`, not a legacy mirrored object tree

That keeps exact-key correctness and namespace serving performance independent.

## Maintenance Path

All major maintenance flows now share the same authority source:

- sync and cleanup recompute fragment usage from `index_v2` live and trash state
- repair reconciles partition replicas and blob availability from `index_v2`
- redistribution republishes moved shard addresses through `index_v2`
- GC expires trash versions and abandoned sessions through `index_v2`
- prefix rebuild regenerates namespace segments from `index_v2`
- health reporting reflects partitioned authority instead of expecting full-root mirrored equality

## Durability Invariants

- A version is visible only after its `index_v2` publish succeeds on at least one selected root.
- Prefix rows must never invent visibility that `index_v2` does not contain.
- Raw extents without live session or object metadata are cleanup candidates, not visible data.
- Replica divergence is repairable, but the newest committed `index_v2` state wins.
- Full-root byte-for-byte metadata equality is no longer an invariant. Partition ownership and replica survivability are the invariant.

## Current Authority Rules

- Upload recency is defined by session creation time. Finalize publishes that version; it does not get to redefine which same-key upload is newer.
- When second-level `created_at` timestamps tie, ULID `version_id` order is the tiebreaker for latest selection.
- Publish may write an immutable version file for an older version, but it must not advance the `latest` marker unless that version actually wins the recency comparison.
- Exact-key reads treat the `latest` marker as a routing hint and rehydrate the referenced immutable version record before serving metadata or bytes.
- Prefix and delimiter rows remain derived serving state. They can lag `index_v2`, but they must never override or invent exact-key truth.

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

5. The same node-core model composes cleanly into N2.
   N2 can keep its distributed control plane while inheriting a cheaper, clearer node-local authority model.

## HA Model

- Losing one selected partition root is survivable if the other selected root is still readable.
- Reads prefer the freshest available partition replica.
- Repair restores missing owner/replica copies from surviving authoritative state.
- Prefix state can lag or be rebuilt without making exact-key reads incorrect.
