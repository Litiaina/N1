<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>N1 Architecture (Current)</h1>
</div>

This document reflects the current N1 storage architecture as implemented in `server/src/n1_core`.

## 1. Architecture Truth

N1 is a **fragment-scoped object storage engine with a filesystem-backed metadata plane and a direct-I/O blob plane**:

- no database in the object hot path
- metadata authority lives on disk (mirrored index roots)
- data/parity shards live on blob disks
- upload visibility is controlled by atomic metadata publish
- repair/redistribution/prefix rebuild are first-class maintenance pipelines

## 2. Storage Domains

N1 uses two disk pools:

- **Index roots** (`INDEX_STORE_DISKS`): metadata, sessions, quota, fragment metadata
- **Blob disks** (`BLOB_STORE_DISKS`): direct-I/O shard storage. Raw, unformatted devices are the
  preferred deployment model; mounted directories remain supported as a compatibility backend.

Fragment is the isolation boundary for auth, quota, maintenance, and layout.

## 3. Canonical On-Disk Layout

### 3.1 Index root layout

```text
{INDEX_ROOT}/{fragment_hash}/
├── quota.msgpack
├── fragment.metadata.msgpack
├── index/
│   ├── {h1}/{h2}/{h3}/{h4}/{sha256(object_key)}#{version_id}.msgpack
│   ├── sessions/{version_id}/
│   │   ├── session.manifest.msgpack
│   │   ├── reservation.msgpack
│   │   └── part_{part_index}.meta.msgpack
│   └── __p__/v1/pfx/{p1}/{p2}/{prefix_hash}/
│       ├── compacted.msgpack
│       └── seg_{segment_id}.msgpack
└── .trash/
    └── index/
        ├── {h1}/{h2}/{h3}/{h4}/{sha256(object_key)}#{version_id}.msgpack
        └── sessions/{version_id}/...
```

Notes:
- Object metadata index is **4-level sharded** from SHA256(object_key).
- Current authoritative object metadata is under `index/{h1}/{h2}/{h3}/{h4}/...` (no extra `objects/` level).
- Prefix index namespace is under `index/__p__/v1/pfx/...`.
- Fragment quota state remains mirrored on index roots, with cache-backed updates flushed asynchronously to `quota.msgpack` and retried until mirrors converge.

### 3.2 Blob root layout

Compatibility filesystem backend:

```text
{BLOB_ROOT}/{fragment_hash}/
├── blob/{version_id}/
│   ├── part_{p}_shard_{i}.shard
│   └── part_{p}_shard_{i}.parity
└── .trash/
    └── blob/{version_id}/...
```

Notes:
- Blob directory name is currently `blob`.
- Trash mirrors this under `.trash/blob/...`.
- In raw blob mode, payloads live in `block_manager` extents on the blob device rather than under
  mounted blob directories; metadata carries `raw_blob_address` values and debug paths like
  `raw://<device_id>/<allocation_id>`.

### 3.3 Direct-I/O split

- Blob payload writes and reads use `O_DIRECT`.
- Block-manager metadata (superblock/journal) is flushed explicitly but remains buffered.
- Index, quota, prefix indexes, fragment metadata, and other control files remain filesystem-backed.

## 4. Object Model

`ObjectMetadata` (msgpack) is the source of truth for a committed version:

- `fragment`, `object_key`, `version_id`
- `content_type`, `total_size_bytes`, `created_at`
- `parts[]` where each part includes shard list and checksums
- `is_delete_marker` for tombstone semantics
- `stripe_size_bytes` for streaming/recovery math

## 5. Write Path (Upload)

### 5.1 Initiate

`/v4/upload/init/{fragment}/{object_key}`:

- validates fragment-scoped write claims
- reserves quota (session receipt)
- creates `version_id` + `session_id`
- writes session manifest to session dirs on index mirrors

### 5.2 Upload part

`/v4/upload/chunk/{fragment}/{version_id}/{part_index}`:

- bounded by IO semaphore and configured limits
- encodes data to `data_shards + parity_shards`
- writes to temp attempt files, fsyncs, then atomically renames to stable shard names
- writes `part_{index}.meta.msgpack` to session mirrors

### 5.3 Finalize

`/v4/upload/finalize/{fragment}/{version_id}/{session_id}`:

- verifies all expected parts exist and are valid
- assembles final `ObjectMetadata`
- atomically writes metadata to index mirrors
- updates prefix index segments
- object becomes visible to read/list only after metadata publish

## 6. Read Path

### 6.1 Metadata resolution

Read APIs (`get`, `stream`, `download`, `meta`) resolve metadata from index mirrors.

### 6.2 Streaming/retrieval behavior

- reads are chunked and bounded by stream config
- range support is native
- recovery helpers can reconstruct unavailable shard slices when enough shards exist

### 6.3 Prefix listing

`list_objects` uses prefix index fast path when applicable:

- reads `compacted.msgpack` + delta segments
- merges latest entries per key/version
- falls back to full index scan if prefix delta fan-in exceeds configured threshold

## 7. Delete and Trash Semantics

Soft delete operations move metadata/shards/sessions into `.trash` namespaces.

- live index path -> `.trash/index/...`
- live blob path -> `.trash/blob/...`
- sessions -> `.trash/index/sessions/...`

Recovery moves entries back; permanent delete removes both index and blob artifacts.

## 8. Maintenance Architecture

### 8.1 Repair

Repair reconciles:

- fragment metadata mirrors
- session manifests/receipts/part metas across index roots
- object metadata divergence/missing replicas
- quota mirror divergence
- shard corruption/missing shard recovery (availability/scrub modes)

After meaningful repairs, prefix rebuild is triggered for affected fragments.

### 8.2 Redistribution

Redistribution realigns shard placement to current EC and disk topology, while keeping CRUD online.

### 8.3 Garbage collector

GC handles:

- expired trash versions
- orphan sessions/blob dirs
- quota refunds

### 8.4 Prefix rebuild

Rebuild reconstructs prefix index segments from canonical object metadata.

### 8.5 Monitoring

System health endpoint serves cached near-real-time snapshots, with optional deep scan and refresh.

## 9. Durability and Crash Safety Rules

Core invariants:

1. Temp shard names are never published as committed shard references.
2. Stable shard commit happens before part metadata commit.
3. Final object metadata publish is the visibility gate.
4. Committed versions are immutable; updates create new versions.
5. Rename/delete/recover are metadata-orchestrated operations with durable sync discipline.
6. Repair writes are atomic and mirror-safe.

## 10. HA Model (Single-Node Multi-Disk)

N1 HA is not multi-node consensus HA; it is **single-node operational HA**:

- service remains available during repair/redistribution
- index mirrors allow metadata survivability and reconvergence
- shard/parity model allows data recovery under disk failures
- maintenance APIs + CLI provide live control and observability

This is the practical architecture for low-hardware environments that still need continuous CRUD + durability.
