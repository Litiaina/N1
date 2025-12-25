<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />

  <h1>Litiaina N1</h1>

  <p>
    A high-performance, single-node object storage engine engineered for
    extreme throughput, predictable latency, and filesystem-level efficiency.
  </p>
</div>

---

## 🧭 What Is N1?

**Litiaina N1** is a purpose-built storage engine that pushes a *single machine* to its physical limits.

Instead of optimizing for cluster coordination or eventual consistency, N1 is designed to:
- Saturate disk bandwidth
- Eliminate lock contention
- Stream data with minimal CPU overhead
- Preserve long-term filesystem health

N1 treats the local filesystem (RAID 10 + XFS) as a trusted, high-speed substrate and removes all distributed-system tax to deliver **wire-speed object I/O**.

---

## ⚡ Core Capabilities

### 🚀 Lockless, Linear Scalability

- Fully lockless write path for uploads
- Scales linearly with CPU cores
- No global mutexes, no upload serialization
- Stable performance under sustained concurrency

---

### 🔁 Zero-Copy Streaming I/O

- Direct streaming from network socket to disk buffers
- No intermediate buffering layers
- Constant memory usage regardless of object size
- Ideal for multi-GB to multi-TB objects

---

### 🧱 Filesystem-Aware Design

- Aggressive cleanup of temporary metadata files
- Minimal inode footprint even at massive object counts
- Optimized for XFS behavior under heavy parallel I/O
- Long-running deployments without filesystem degradation

---

### 💾 Strong Write Safety

- Atomic object finalization
- Objects become visible only after durable `sync_all()`
- Partial or failed uploads are never exposed
- Crash-safe behavior by design

---

### 📦 High-Throughput Object Ingestion

- Designed for sustained sequential writes
- Parallel part ingestion with fast finalization
- Handles raw video, AI datasets, VM images, and scientific data
- Optimized for bandwidth, not request count

---

### 🔄 Native Versioning & Lifecycle Control

- Immutable version IDs per upload
- Soft delete with instant rollback
- Background lifecycle engine enforces retention policies
- Fast garbage collection without touching hot paths

---

### 🔍 Efficient Reads & Seeking

- Metadata cached in-memory (Moka)
- Range reads open only the required chunk files
- Fast random access for media seeking and analytics
- Avoids large file scans for small reads

---

## 🧠 Internal Architecture (N1)

### Write Path

1. Incoming data streams into temporary `.data` chunk files
2. Parts are written locklessly and independently
3. Finalization writes a single immutable `object_meta.bson`
4. Temporary metadata is immediately purged

---

### Read Path

- Metadata is resolved from memory cache
- Only required chunk files are opened
- Byte-range reads avoid unnecessary disk I/O
- Optimized for high request concurrency

---

### Lifecycle Engine

- Uses filesystem timestamps to skip healthy objects
- Avoids parsing metadata unless necessary
- Enables extremely fast maintenance scans
- Designed to run continuously without performance impact

---

## 🔄 End-to-End Data Flow

- Upload begins → quota validated
- Data streams → lockless chunk writes
- Finalization → atomic commit + metadata write
- Download → zero-copy streaming of requested byte range

---

## 💡 Where N1 Excels

- 🏎️ **High-Bitrate Media Pipelines**  
  Raw 4K / 8K video ingest, post-production, and streaming

- 🧠 **AI / ML Training Storage**  
  Feeding GPUs with massive datasets at disk-speed

- 🧪 **Scientific & Research Workloads**  
  Simulation outputs, sensor data, and experimental results

- 🏢 **On-Premise Performance Storage**  
  Predictable, high-throughput object storage without cluster overhead

- ⚡ **Edge & Ingest Nodes**  
  Ultra-fast front-line storage ahead of slower distributed systems

---

## 🔮 Roadmap: N2 (Distributed Evolution)

- Multi-node scaling using per-part erasure coding
- Automatic data rebalancing across nodes
- JBOD-first design with software-defined redundancy
- Same object model, distributed execution

---

## 🚀 Why Litiaina N1?

Most storage systems are designed to survive failure across hundreds of machines.

**Litiaina N1 is designed to go as fast as physics allows on one.**

No consensus.
No coordination.
No latency inflation.

Just raw, predictable performance.
