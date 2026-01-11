<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />

  <h1>Storage Architecture & Disk Layout</h1>
  
</div>

This document details the internal storage engine of **Litiaina N1**. N1 moves away from traditional hierarchical filesystems to a **Content-Addressable Storage (CAS)** model using a decoupled **Index / Blob** architecture.

This design is engineered to saturate disk I/O, eliminate lock contention during writes, and bypass operating system limits on file counts and path lengths.

---

## 1. High-Level Disk Hierarchy

The storage root follows a strict, predictable hierarchy designed for isolation and performance.

```text

/storage_root/
├── {user_uid}/                       # 1. User Isolation Scope
│   ├── quota.bson                    #    Global User Quota (Single Source of Truth)
│   │
│   └── {fragment_name}/              # 2. Fragment (Logical Bucket)
│       │
│       ├── index/                    # 3. Metadata Store (Mutable Index)
│       │   └── {shard_1}/            #    Shard L1: First 2 hex chars of SHA256
│       │       └── {shard_2}/        #    Shard L2: Next 2 hex chars of SHA256
│       │           └── {Hash}#{Ver}.bson
│       │
│       └── blobs/                    # 4. Data Store (Immutable Content)
│           └── {VersionID}/          #    Content-Addressable ID (ULID)
│               ├── part_0.data       #    Binary Chunk 0
│               ├── part_1.data       #    Binary Chunk 1
│               └── reservation.bson  #    Temporary Upload Receipt
├──

```

---

## 2. Component Deep Dive

### Global User Quota (`quota.bson`)

- **Location:** `storage/{uid}/quota.bson`
- **Format:** BSON serialization of the `Quota` struct.
- **Role:** Single source of truth for a user's storage consumption.

**Concurrency Control**
- Managed via **sharded in-memory mutexes** (`LOCK_SHARD_COUNT = 64`)
- Operations hash `uid` to select a lock shard
- Ensures high concurrency across users with strict per-user consistency
- **Why at User Root?**
  Allows enforcing limits without scanning millions of objects across fragments.

---

### The Index (`index/`)

The Index acts as filesystem **inodes**, decoupling logical names (keys) from physical data (blobs).

**Location:** `storage/{uid}/{fragment}/index/`

#### Sharding Strategy
- **Algorithm:** 2-level hex sharding derived from SHA-256(key)
- **Structure:** `index/{h1}/{h2}/` (e.g. `index/a4/c9/`)
- **Fan-out:** `256 × 256 = 65,536` directories
- **Benefit:** Prevents inode exhaustion and maintains `O(1)` readdir performance

#### Filename Format
```

{SHA256_Hash}#{VersionID}.bson

```

- **SHA256_Hash:** 64-char hex of original key (bypasses path limits)
- **VersionID:** ULID (time-sortable, unique)

#### Metadata Contents
- `object_key` — original user key
- `total_size_bytes`
- `parts[]` — byte ranges → `part_N.data`
- `is_delete_marker` — soft delete flag

---

### The Blobs (`blobs/`)

The Blobs directory stores raw payloads. It is **content-addressable and immutable**.

**Location:** `storage/{uid}/{fragment}/blobs/{VersionID}/`

**Immutability Guarantee**
- Once finalized, a blob directory is never modified
- Renames only move index entries

#### Chunked Storage (`part_N.data`)
- Sequential binary chunks (128KB → multi-GB)
- Enables parallel uploads without lock contention
- Supports constant-memory streaming on reads

---

## 3. Critical I/O Workflows

### Write Path (Upload)

1. **Authorization & Reservation**
   - JWT authentication
   - Lock `quota.bson`, reserve space
   - Write `reservation.bson`

2. **Lockless Ingest**
   - Stream directly to `part_N.data`
   - Unique ULID → zero filesystem contention

3. **Atomic Finalization**
   - Validate parts
   - Serialize metadata
   - Atomic write to index shard
   - Delete reservation

**Result:** Object becomes instantly visible.

---

### Read Path (Streaming)

1. **Index Lookup**
   - Moka cache → shard → BSON read

2. **Zero-Copy Stitching**
   - Open only required `part_N.data`
   - Stream ranges directly to socket
   - Constant memory usage

---

### Lifecycle & Garbage Collection

**Mark-and-Sweep GC** (non-blocking):

1. **Mark**
   - Scan index shards
   - Apply retention policy

2. **Sweep**
   - Remove orphaned blob folders
   - Refund quota asynchronously

3. **Shard Cleanup**
   - Remove empty index directories

---

## 4. Architectural Advantages

| Feature | Implementation | Benefit |
|------|--------------|--------|
| Atomic Renames | Move index BSON only | `O(1)` renames, crash-safe |
| Unlimited Paths | SHA-256 hashing | Bypasses OS limits |
| Linear Scaling | 256² shard fan-out | Stable performance |
| High Throughput | ULID per upload | Saturates disk |
| Resumability | Chunked parts | Byte-accurate resume |

---

## 5. Visual Data Flow

**Scenario:** User uploads documents/resume.pdf

1.  Hash Key: SHA256("documents/resume.pdf") -> a4c9...
2.  Gen Version: 01HQ... (ULID)
3.  Write Data:
   - storage/user_1/main/blobs/01HQ.../part_0.data
5.  Write Index:
   - storage/user_1/main/index/a4/c9/a4c9...#01HQ....bson

**Result:** The heavy data sits in an immutable ID-based folder, while the lightweight index provides the directory structure view.
