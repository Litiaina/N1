# N1 Deployment Profiles

This document is the canonical deployment split for N1.

As of 2026-03-13:

- `standard` (`litiaina-n1-standard`) is the stable deployment profile, the default for production, and released at `0.5.1`.
- `arc` (`litiaina-n1-arc`) is the experimental raw-device profile.
- If you are unsure which one to deploy, use `standard`.

## 1) Profile Status

| Profile | Binary | Status | Production default |
|---|---|---|---|
| Standard | `litiaina-n1-standard` | Stable (`0.5.1`) | Yes |
| Arc | `litiaina-n1-arc` | Experimental | No |

## 2) Architecture Split

### Standard (stable)

- Filesystem-backed authoritative `index` plane.
- Filesystem-backed blob shard plane (`blob_store_disks` are directory roots).
- Partitioned metadata authority and derived prefix serving.
- Best operator compatibility and lowest deployment risk.

Disk layout snapshot:

```text
{INDEX_ROOT}/{fragment}/index/partitions/{partition_id}/...
{BLOB_ROOT}/{fragment}/blob/{version_id}/part_{p}_shard_{i}.shard|.parity
```

### Arc (experimental raw-device profile)

- Raw partitioned `index` plane (`index_store_disks` are raw devices).
- Raw payload plane (`blob_store_disks` are raw devices).
- Managed by `block_manager` with journaling/allocator semantics.
- Supports same object/API behavior contracts as `standard`.

Disk layout snapshot:

```text
{RAW_INDEX_DEVICE} -> block_manager allocator/journal metadata + raw index catalog
{RAW_BLOB_DEVICE} -> block_manager allocator/journal metadata
payload address: raw://{device_id}/{allocation_id}
```

## 3) Recommended Production Deployment

Build and run Standard:

```bash
cargo build --release -p standard
./target/release/litiaina-n1-standard server
```

Use Standard for:

- production nodes
- integration with N2
- baseline operational runbooks (repair, redistribution, GC, SMB-lite)

## 4) Arc Deployment Guidance

Use Arc when you need:

- raw device control for both `index` and `blob`
- direct-I/O raw path operations
- storage-kernel deployment shape

Arc is not the default path when:

- broad operator compatibility is the priority
- you do not want raw-device operational procedures

## 5) N2 Alignment Policy

- N2 documentation and deployment guidance keep `standard` as the default path.
- Arc-compatible N2 deployment is supported when N1 nodes are intentionally deployed on Arc.
