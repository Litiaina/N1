<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1</h1>
  <p>
    A single-node, object storage engine optimized for lockless streaming,
    predictable latency, and durability.
  </p>
</div>

---

## What is N1?

**Litiaina N1** is a *single machine* object storage engine that treats the local filesystem as the truth source and
pushes the box to its physical limits (disk bandwidth, IOPS, and network throughput) **without turning your storage into an opaque black box**.

N1 is built for environments where:
- you want **NVMe/JBOD speed** without cluster coordination tax
- you want **video-grade streaming** and range reads with minimal CPU
- you want **crash safety** and recoverability even on unstable hardware / networks
- you want storage that stays **understandable on disk** (inspectable, recoverable, movable)

---

## N1 philosophy

### 1) Filesystem-native, not “virtualized storage”
N1 stores data as *real files and directories*. No proprietary block formats. No hidden databases on the hot path.
If the software disappears, your disks are still mountable and your data is still physically present in a sane layout.

### 2) Immutability as a correctness primitive
A finalized object version is **immutable**:
- no in-place rewrite of committed shard files
- updates create a new `version_id`
- delete is represented as a **delete marker**
- rename is **metadata-only**

Immutability is what makes N1 safe under power loss and partial failure: the engine never tries to “patch” committed data.

### 3) Lockless data path
N1’s identity is **lockless streaming**. Hot-path operations do not rely on global mutexes for correctness.
Concurrency is achieved by deterministic layout + atomic commit patterns, not by central locks.

(You may still see narrow synchronization in non-hot-path bookkeeping, but the design target is: no locks in the data path.)

### 4) Crash-first engineering
N1 assumes failures are normal:
- the server can be stopped mid-upload
- disks can disappear mid-stream
- metadata mirrors can desync
- bytes can corrupt (bitrot / manual edits)

Durability is achieved by commit discipline (atomic rename + directory sync) and by recoverability tools.

### 5) “Readable under chaos” operations
Maintenance jobs (repair / redistribution / GC) are designed to:
- be resumable
- not interrupt streaming downloads
- not interrupt uploads
- avoid destructive deletes in the hot path

---

## Architecture overview

N1 splits the world into **Index** and **Blob** storage domains.

### Index vs Blob roots
- **Index roots** hold metadata files (small, replicated across configured index disks)
- **Blob roots** hold shard data (large, striped across configured blob disks)

The engine can be configured with separate devices for index and blob, or shared devices depending on deployment.

### On-disk layout (N1)

#### Index (metadata) layout
Metadata is stored as BSON files, sharded by hash of `object_key`:

```
{INDEX_ROOT}/{storage}/{uid}/{fragment}/index/{h1}/{h2}/{hash}#{version_id}.bson
```

Where:
- `hash = sha256(object_key)`
- `h1 = hash[0..2]`, `h2 = hash[2..4]`
- the `#{version_id}` suffix enables versioning without rewriting old metadata

A delete operation creates a **delete marker** metadata record (a zero-size version with `is_delete_marker=true`).

#### Blob (data) layout
Each committed version has its own blob directory on each blob disk:

```
{BLOB_ROOT}/{storage}/{uid}/{fragment}/blobs/{version_id}/
```

Inside, each part’s shards live as normal files (data + parity). The exact filenames are recorded in metadata.

> This is why N1 remains filesystem-friendly: the “truth” is always the shard files + immutable metadata.

---

## Core data model

- **Object**: `(fragment, object_key)`
- **Version**: immutable `version_id` produced on finalize
- **Parts**: multipart uploads (each part has size, created_at, shards[])
- **Shards**: per-part data/parity files with checksums and absolute paths recorded in metadata
- **Stripe block size**: per-object `stripe_size_bytes` controls erasure stripe geometry

---

## Write path (upload)

N1 is fully streaming: it does not need to buffer whole objects in RAM.

1) **Initiate upload**
- validates quota (reserve/charge)
- creates a new `version_id` + upload session id

2) **Stream write**
- the incoming stream is encoded into data/parity shards (Reed–Solomon)
- shards are written across blob disks
- on Linux, writes can use **O_DIRECT** (aligned) to reduce page-cache pressure and improve predictability

3) **Commit discipline (durability)**
Finalization is a strict, crash-safe sequence:
- shard files are committed via **rename + fsync(dir)**
- metadata is published via **rename + fsync(dir)**
- only after metadata is durable does the version become “visible” to reads/lists

4) **No partial visibility**
If the process dies mid-upload or mid-finalize, the system never exposes a half-committed version as a successful object.

---

## Read path (download / stream)

Reads are optimized for range access and continuous streaming.

1) **Resolve latest metadata**
- N1 finds the latest visible version (or returns NotFound if latest is a delete marker)
- metadata is read from disk (no mandatory in-memory caching requirement)

2) **Range mapping**
- the requested byte range is mapped to `(part, stripe_index, shard_index, file_offset)`
- N1 reads only the required shard chunks

3) **Degraded reads + repair**
If a shard chunk is missing or corrupt:
- N1 reconstructs the requested bytes using remaining shards (RS decode)
- in the background, N1 may schedule **repair_entire_shard** on a dedicated recovery runtime
- repair writes are done as new files (e.g., `*.recovering`) then committed with rename

This is what keeps streaming stable even when disks disappear: reads degrade gracefully and repair happens out-of-band.

---

## Maintenance jobs

### Repair
A repair scan verifies shard integrity and reconstructs missing/corrupt shards.
Important properties:
- IO-bound work is isolated (dedicated runtime / bounded concurrency)
- repairs are committed via rename (no in-place patching)

### Garbage collection
N1 avoids immediate destructive deletes in critical workflows.
A common safe pattern is:
- rename old data to a `.gc` suffix
- delete later in a background pass

This keeps operations resilient under crashes and avoids “half-deleted” states.

### Redistribution
Redistribution moves data from one layout or placement to another **without interrupting CRUD**.
Correct redistribution is:
- resumable
- uses stream read (recover if needed)
- uses stream write (O_DIRECT-aligned)
- commits via rename + directory sync
- marks old shards as `.gc` (no immediate delete)

---

## Failure model

N1 is built to survive:
- **power loss / server stop mid-write**
- **disk disappears mid-stream**
- **metadata mirror desync / partial loss**
- **bitrot / manual corruption**
- **long soak** with bounded memory usage

### RFTD — Reconstruction From The Disk
A last-resort recovery philosophy: if the node is dead, disks can be mounted and reconstructed using the filesystem-visible layout + metadata.
This is possible because N1 never hides data behind opaque storage layers.

---

## Performance characteristics

N1 is optimized for **throughput and predictability**:
- lockless streaming data path
- low CPU overhead for large objects
- supports high-bitrate media workloads (range reads / seeks)
- keeps memory usage bounded (you’ve observed tens of MB under repeated browsing / video streaming)

N1 is not trying to maximize tiny-object ops/sec at any cost; it’s built to push sustained bandwidth and remain stable under failure.

---

## Where N1 fits

N1 is the **node engine**:
- standalone: a fast, durable single-node object store
- as a building block: can be used under a clustering layer (e.g., your N2)

N1 does not need a cluster to be correct. It is correct on a single machine.

---

## Quick glossary

- **fragment**: namespace / bucket-like partition
- **object_key**: object path/name within a fragment
- **version_id**: immutable identifier of a committed object version
- **part**: multipart chunk metadata (contains shard list)
- **shard**: the on-disk file segment (data or parity)
- **stripe_size_bytes**: erasure coding stripe block size
