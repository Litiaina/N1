<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />

  <h1>N1 Storage Redistribution Guide</h1>
</div>

This guide describes how **N1 safely re-stripes and realigns data** when storage topology changes occur, including:

- Adding or removing data disks
- Changing Erasure Coding (EC) ratios
- Adding Index Mirrors for metadata redundancy

Redistribution is designed to be **online, deterministic, and crash-safe**, allowing the system to evolve without downtime.

---

## Concept Overview

Redistribution in N1 is a **background migration process** that ensures the physical storage layout matches the desired state defined in `config.ini`.

The system operates in three phases:

### 1. Discovery
- Reads the **Target Configuration** from `config.ini`
- Scans the primary metadata index

### 2. Diagnosis
Each object is evaluated for alignment:

**Data Alignment**
- Do shards match the configured EC ratio (e.g., `7+3`)?
- Do shard placements match the expected disk mapping (modulo-based)?

**Metadata Alignment**
- Does the metadata file (`.bson`) exist on *all* configured Index Disks?

### 3. Migration
Depending on what is misaligned:

**Data Redistribution**
- Reads existing shards (reconstructing data if required)
- Re-encodes data using the new EC layout
- Writes shards to target disks
- Updates metadata and invalidates caches
- Deletes obsolete shards after verification

**Index Mirroring**
- Copies existing metadata files to newly added Index Disks
- Does not touch data blobs unless the blob layout also changed

---

## Scenario A: Scaling Blob Storage (Adding Disks)

**Goal:** Scale from **2 data disks (1+1 EC)** to **10 data disks (7+3 EC)**.

### 1. Preparation

Mount the new disks:

```bash
/mnt/data/disk2 ... /mnt/data/disk9
```

Verify ownership and permissions:

```bash
chown -R n1user:n1group /mnt/data
```

---

### 2. Update Configuration

Edit `config.ini` to define the **new desired state**:

```ini
[disk_config]
# Add ALL paths. Order matters.
blob_store_disks = /mnt/data/disk0,/mnt/data/disk1,...,/mnt/data/disk9

[upload_config]
data_shards = 7
parity_shards = 3
```

---

### 3. Restart N1

Restart the service to load the new configuration.

> **Note:** The API remains fully functional. Existing objects are read using absolute paths stored in metadata until redistribution completes.

---

### 4. Verify with Dry Run

Preview planned actions without moving data:

```bash
curl -X POST "http://localhost:50001/v4/maintenance/redistribute?dry_run=true" \
     -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

Logs will show entries such as:

```text
[DRY RUN] Would redistribute uid/fragment/object_key
```

---

### 5. Execute Redistribution

Start the migration:

```bash
curl -X POST "http://localhost:50001/v4/maintenance/redistribute?dry_run=false" \
     -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

- Operations run sequentially to avoid system lockup
- CPU and disk I/O usage will increase

Completion is indicated by:

```text
Redistribution Job Complete
```

---

## Scenario B: Scaling Down (Removing Disks)

**Goal:** Scale from **10 disks down to 5**.

> **CRITICAL:** Do **not** unmount or remove disks before redistribution completes.

### 1. Update Configuration

Remove disks to be decommissioned:

```ini
[disk_config]
blob_store_disks = /mnt/data/disk0,/mnt/data/disk1,/mnt/data/disk2,/mnt/data/disk3,/mnt/data/disk4

[upload_config]
data_shards = 3
parity_shards = 2
```

---

### 2. Restart and Redistribute

Restart N1 and run the redistribution job.

N1 will:
- Read shards from excluded disks using absolute metadata paths
- Re-encode and write data to remaining disks
- Automatically delete old shards as migration completes

---

### 3. Decommission Disks

After completion:
- Verify excluded disks are empty (or nearly empty)
- Safely unmount and physically remove the drives

---

## Scenario C: Adding Index Mirrors (Metadata)

**Goal:** Add a third Index Disk for triple metadata redundancy.

### 1. Update Configuration

```ini
[disk_config]
index_store_disks = /mnt/index/disk0,/mnt/index/disk1,/mnt/index/disk2
```

---

### 2. Run Redistribution

```http
POST /v4/maintenance/redistribute?dry_run=false
```

**Logic**
- Existing metadata on `disk0` and `disk1` is detected as missing on `disk2`

**Action**
- `.bson` files are copied to the new Index Disk
- Data blobs remain untouched unless blob configuration also changed

---

## Operational Notes

### Zero-Downtime Guarantee

- Streaming continues uninterrupted during redistribution
- Reads transparently switch to new locations on subsequent requests

**Atomic Transition**
1. New data is written
2. Metadata is updated on all Index Mirrors
3. In-memory caches are invalidated
4. Old data is deleted

---

### Performance Impact

- **CPU:** Increased usage due to Reed-Solomon encoding
- **I/O:** Sequential processing avoids lockups, but user throughput may temporarily decrease due to contention

---

## Summary

N1 redistribution enables **safe, online evolution of storage topology**.

By treating configuration as the desired end state and metadata as the source of truth, N1 guarantees deterministic, recoverable, and zero-downtime migration across data and metadata layouts.
