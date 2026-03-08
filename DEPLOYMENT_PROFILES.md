# N1 Deployment Profiles

This document is the canonical deployment split for N1.

As of 2026-03-08:

- `standard` (`litiaina-n1-standard`) is the stable deployment profile and the default for production.
- `arc` (`litiaina-n1-arc`) is experimental and must be treated as a development track.

## 1) Profile Status

| Profile | Binary | Status | Production default |
|---|---|---|---|
| Standard | `litiaina-n1-standard` | Stable | Yes |
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

### Arc (experimental)

- Filesystem-backed partitioned `index` plane (current runtime truth).
- Raw/blob allocator-centric payload plane (`blob_store_disks` are raw devices).
- Kernel-style storage-kernel direction.
- Active hardening/certification work still in progress.
- Not the default deployment target until release gates are closed.

Disk layout snapshot:

```text
{INDEX_ROOT}/{fragment}/index/partitions/{partition_id}/...
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

## 4) Experimental Arc Track Rules

Arc is allowed for:

- allocator/kernel-track development
- isolated benchmarking
- non-production validation

Arc is not the default for:

- production rollout
- operator runbooks
- N2 default node-core expectations

## 5) N2 Alignment Policy

Until arc passes full hardening and stability gates:

- N2 documentation and deployment guidance must assume N1 Standard semantics as default.
- Arc references must be explicitly labeled experimental.
