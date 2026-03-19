# N2 Deployment Profiles

This document is the canonical deployment split for N2.

N2 is the product identity of this workspace.
N1 is the shared storage engine underneath it.

As of 2026-03-20:

- `standard` (`litiaina-n1-standard`) is the single-node N2 deployment path, the default for production, and released at `0.6.2`.
- `distribution-runtime` (`litiaina-n2`) is the multi-node N2 deployment path on top of the same N1 core.
- `registry-runtime` (`registry-server`) is the SQLite-backed Registry layer used in both deployment shapes and released independently at `0.2.0`.
- `arc` (`litiaina-n1-arc`) is the experimental raw-device profile.
- Privilege split: `standard`, `distribution-runtime`, and `registry-runtime` should normally run as non-root services, while `arc` is the only profile that should require root for raw-device workflows.
- If you are unsure which one to deploy, use `standard`.

## 1) Deployment Shapes

| Deployment shape | Binary | Uses | Status |
|---|---|---|---|
| Single-node N2 | `litiaina-n1-standard` + `registry-server` | N1 engine + Registry | Stable / default |
| Multi-node N2 | `litiaina-n2` + `registry-server` | N1 engine + N2 cluster runtime + Registry | Available in this workspace |
| Arc engine testing | `litiaina-n1-arc` | Experimental N1 engine profile | Experimental |

## 2) Engine Profile Status

| Profile | Binary | Status | Production default |
|---|---|---|---|
| Standard | `litiaina-n1-standard` | Stable (`0.6.2`) | Yes |
| Arc | `litiaina-n1-arc` | Experimental | No |

## 3) Architecture Split

### Standard (stable)

- Filesystem-backed authoritative `index` plane.
- Filesystem-backed blob shard plane (`blob_store_disks` are directory roots).
- Partitioned metadata authority and exact-key object serving.
- Best operator compatibility and lowest deployment risk.

### Registry (`0.2.0` baseline)

- SQLite-backed metadata and control plane.
- Runs in both single-node and multi-node deployments.
- Owns first-admin bootstrap through `REGISTRY_MASTER_KEY`.
- Owns administrator/user identity, rotating refresh sessions, and optional TOTP 2FA.
- Owns users, ownership, quotas, logical listing, and logical object lookup.
- Owns fragment backend binding (`single_node` vs `multi_node`) for Registry-backed clients.
- Brokers short-lived storage sessions so clients do not need fragment secrets for normal Registry-backed usage.
- Resolves prefix/search archive selections into exact-key manifests for engine-side tar streaming.
- Bundled web/admin shells can target this layer, but they are still under development and are not the stable N2 product surface yet.

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

## 4) Recommended Production Deployment

Build and run Standard:

```bash
cargo build --release -p standard
./target/release/litiaina-n1-standard
```

Use Standard for:

- production nodes
- integration with N2
- baseline operational runbooks (repair, redistribution, GC, SMB-lite)

## 5) Arc Deployment Guidance

Use Arc when you need:

- raw device control for both `index` and `blob`
- direct-I/O raw path operations
- storage-kernel deployment shape

Arc is not the default path when:

- broad operator compatibility is the priority
- you do not want raw-device operational procedures

## 6) N2 Alignment Policy

- N2 documentation and deployment guidance keep `standard` as the default single-node path.
- Multi-node N2 builds on that same Standard-backed N1 engine through `distribution-runtime`.
- Arc-compatible N2 deployment is supported when N1 nodes are intentionally deployed on Arc.
