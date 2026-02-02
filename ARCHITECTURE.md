<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Storage Architecture & Disk Layout</h1>
</div>

This document describes the **current** on-disk architecture of **Litiaina N1 Core** (fragment-centric, database-free, filesystem-as-truth), including **quota-per-fragment**, **multi-root index/blob disks**, **sessions for resumable uploads**, and **strict atomic commits**.

> **Important correction:** N1’s authoritative index is **4-level sharded** (not 2-level). Each shard directory is **2 hex chars** from `SHA256(object_key)`:
>
> `h1 = hash[0..2]`, `h2 = hash[2..4]`, `h3 = hash[4..6]`, `h4 = hash[6..8]`

---

## 1) Core layout principles

### Fragment is the isolation boundary
N1 does **not** treat “users” as an on-disk root. The **fragment** (identified by `fragment_hash`) is the isolation scope for:
- authorization (JWT claims scoped to fragment)
- quota accounting
- trash namespace
- upload sessions
- object metadata and version history

### Filesystem is the database
There is no SQL/NoSQL dependency in the hot path. Correctness comes from:
- deterministic paths
- append-only/immutable commits
- atomic rename + `fsync(dir)` discipline
- repair tooling that can rebuild from disk truth

---

## 2) Disk roots and top-level hierarchy

N1 is configured with **multiple roots**:
- **Index roots**: store small metadata + session manifests (replicated / mirrored)
- **Blob roots**: store large erasure-coded shard files (striped / distributed)

A simplified conceptual view:

```text
{INDEX_ROOTS[n]}/
└── {fragment_hash}/
    ├── quota.msgpack
    └── index/
        ├── objects/      (authoritative object metadata index)
        ├── trash/        (authoritative trash metadata index)
        └── sessions/     (upload session manifests + part metas)

{BLOB_ROOTS[m]}/
└── {fragment_hash}/
    └── blobs/
        └── {version_id}/
            ├── part_{p}_shard_{i}.data
            ├── part_{p}_shard_{i}.parity
            └── ...
```

> Exact folder names can differ slightly depending on your `ObjectPathManager`, but the invariants above are what matter:
> - **Index:** mirrored across index roots
> - **Blobs:** distributed across blob roots
> - **Version:** blob placement is version-scoped and immutable after commit

---

## 3) Index layout (authoritative)

### 3.1 Object metadata index — **4 shards deep**
Object metadata is stored as immutable version records. The filename is version-suffixed so old versions are never overwritten.

```text
{INDEX_ROOT}/{fragment_hash}/index/objects/{h1}/{h2}/{h3}/{h4}/{hash}#{version_id}.msgpack
```

Where:
- `hash = sha256_hex(object_key)` (64 hex chars)
- `h1..h4` are 2-hex prefix directories (4 levels total)
- `{version_id}` is a ULID (or your version identifier)
- the file contains `ObjectMetadata` (msgpack per your build)

**Why 4-level sharding?**
- prevents pathological directory fan-in as object count grows
- keeps `readdir()` and lookup behavior stable
- avoids inode pressure and “giant directory” latency cliffs

### Example
Object key: `photos/2026/clip.mp4`  
`sha256(object_key) = a4c9d21f...`

```text
.../objects/a4/c9/d2/1f/a4c9d21f...#{version_id}.msgpack
```

### 3.2 Trash metadata index
Trash uses the same index strategy (still authoritative, still sharded):

```text
{INDEX_ROOT}/{fragment_hash}/index/trash/{h1}/{h2}/{h3}/{h4}/{hash}#{version_id}.msgpack
```

Trash is a **namespace**, not a “special object key” clients are allowed to write directly.
Server-side handlers move versions into/out of trash by manipulating metadata placement.

---

## 4) Upload sessions (resumable, strict finalize)

Uploads are tracked via a **session directory** per `{version_id}` on index roots:

```text
{INDEX_ROOT}/{fragment_hash}/index/sessions/{version_id}/
├── session_manifest.msgpack
├── reservation.msgpack
├── part_0.partmeta
├── part_1.partmeta
└── ...
```

Key properties:
- **Session manifests are mirrored** across index roots (best-effort mirror writes; correctness is tolerant to partial mirror loss).
- Each uploaded part commits a `part_N.partmeta` record via a lockless “first valid wins” strategy.
- Finalize requires **all expected parts present and valid**, then publishes final `ObjectMetadata` into the authoritative index.

---

## 5) Blob layout (erasure-coded shards)

Blob roots store the **shards** produced by Reed–Solomon encoding per uploaded part.
Shard placement is deterministic (e.g., round-robin based on part index and shard index).

Conceptually:

```text
{BLOB_ROOT}/{fragment_hash}/blobs/{version_id}/
└── part_{part_index}_shard_{shard_index}.{data|parity}
```

### Temp → stable commit rule
During `upload_part`, shards are first written as temp names (attempt-scoped), then committed to stable names only after verification:

```text
part_{p}_shard_{i}.data.tmp.{attempt_id}   (write target)
part_{p}_shard_{i}.data                   (stable committed)
```

This guarantees:
- no half-written shard is ever treated as “the real shard”
- a crash mid-upload leaves only temp files for GC/repair to clean

---

## 6) Durability discipline (atomic + sync)

N1 relies on a strict commit ordering:

1) **Write temp shard files**
2) `fsync(file)`
3) **rename temp → stable shard names**
4) `fsync(parent_dir)`
5) **commit part meta** (`part_N.partmeta`) via create-new + sync
6) On finalize: **write final object metadata** via atomic write + sync

This is why you can:
- resume safely after crash
- run repair tooling without guessing
- never “expose” partial data as a successful version

---

## 7) Operational notes

### Repair / recovery and “dir not found” failures
Repair tooling should:
- treat recorded `absolute_path` as a hint
- fall back to deterministic path reconstruction when directories move or temp suffixes exist
- always `create_dir_all(parent)` before attempting to write repaired shards

(Your recovery/repair code should also strip `.tmp.{attempt}` and `.recovering` suffixes when trying to locate stable shards.)

---

## 8) Quick glossary

- **fragment_hash**: stable namespace identifier for isolation and placement
- **object_key**: user-visible object path/key
- **version_id**: immutable version identifier (ULID)
- **part**: upload chunk index (multipart)
- **shard**: per-part data/parity file stored on blob disks
- **4-level sharding**: `/{h1}/{h2}/{h3}/{h4}/` from SHA256 prefix (2 hex chars each)
