# N2 Deployment Profiles

This document is the canonical deployment split for N2.

For the dedicated multi-node operator runbook, use [N2_DEPLOYMENT.md](/home/administrator/Development/N1/N2_DEPLOYMENT.md).

N2 is the product identity of this workspace.
N1 is the shared storage engine underneath it.

As of 2026-03-20:

- `n1` (`litiaina-n1`) is the single-node N2 deployment path, the default for production, and released at `0.9.0`.
- `litiaina-n1-cli` tracks the same release line and is also `0.9.0`.
- `n2` (`litiaina-n2`) is the multi-node N2 deployment path on top of the same N1 core and is released at `0.2.0`.
- `registry` (`registry-server`) is the SQLite-backed Registry layer used in both deployment shapes and released independently at `0.4.0`.
- `n1-arc` (`litiaina-n1-arc`) is the experimental raw-device profile.
- Privilege split: `n1`, `n2`, and `registry` should normally run as non-root services, while `n1-arc` is the only profile that should require root for raw-device workflows.
- If you are unsure which one to deploy, use `n1`.

## 1) Deployment Shapes

| Deployment shape | Binary | Uses | Status |
|---|---|---|---|
| Single-node N2 | `litiaina-n1` + `registry-server` | N1 engine + Registry | Stable / default |
| Multi-node N2 | `litiaina-n2` + `registry-server` | N1 engine + N2 cluster runtime + Registry | Available in this workspace |
| N1-Arc engine testing | `litiaina-n1-arc` | Experimental N1 engine profile | Experimental |

## 2) Engine Profile Status

| Profile | Binary | Status | Production default |
|---|---|---|---|
| N1 | `litiaina-n1` | Stable (`0.9.0`) | Yes |
| N1-Arc | `litiaina-n1-arc` | Experimental | No |

## 3) Architecture Split

### N1 (stable)

- Filesystem-backed authoritative `index` plane.
- Filesystem-backed blob shard plane (`blob_store_disks` are directory roots).
- Partitioned metadata authority and exact-key object serving.
- Best operator compatibility and lowest deployment risk.

### Registry (`0.4.0` baseline)

- SQLite-backed metadata and control plane.
- Runs in both single-node and multi-node deployments.
- Owns first-admin bootstrap through `REGISTRY_MASTER_KEY`.
- Owns administrator/user identity, administrator-created accounts, rotating refresh sessions, and optional TOTP 2FA.
- Exposes `GET /v1/ws` for owner-scoped and admin-wide realtime Registry updates.
- Owns users, ownership, quotas, logical listing, logical object lookup, and Registry-wide statistics aggregation.
- Public signup stays on default/admin-assigned quota only; quota is not caller-controlled.
- Owns fragment backend binding (`single_node` vs `multi_node`) for Registry-backed clients.
- Brokers short-lived storage sessions so clients do not need fragment secrets for normal Registry-backed usage.
- Resolves prefix/search archive selections into exact-key manifests for engine-side tar streaming.
- Bundled web/admin shells can target this layer, but they are still under development and are not the stable N2 product surface yet.

### Multi-node N2 operational note

- `n2` is operational on a tested 2-node filesystem-backed layout.
- Current deployment reality:
  - keep `n2.env` + `n2_config.ini` as the profile-owned files
  - `litiaina-n2` now auto-creates `n1.env` and `n1_config.ini` compatibility aliases beside them when missing
  - operators should still manage only the N2-owned files directly
- Current write semantics:
  - default `cluster_commit_mode` is `strict_full_placement`
  - `required_commit_shards` is the actual success threshold for a `PUT`
  - `committed_shards` in the response is the immediate placement truth
  - `cluster_commit_mode=quorum` is still available as an explicit operator override for degraded-write acceptance
  - reconcile is the supported way to restore missing peer copies after degraded periods
- Current automatic maintenance semantics:
  - `auto_maintenance_mode=off` is the safe default
  - `observe` reports redistribution/healing debt without mutating placement
  - `repair_and_redistribute` runs bounded automatic reconcile after membership epoch changes
- Current peer-governance semantics:
  - internal N2 peer calls carry the expected membership epoch and cluster fingerprint
  - stale or drifted peers reject object/session/map traffic instead of silently participating
  - membership apply and rollback now go through governance-only majority commit
  - committed membership epochs are snapshotted for rollback beside the active membership file
  - governance protects committed cluster state but does not sit in the object byte path
  - `GET /v4/n2/governance/state` is the operator view into term, leader hint, commit index, and applied membership epoch
- Current timeout tuning note:
  - conservative defaults are safe
  - low-latency LAN clusters usually want a lower `remote_request_timeout_ms` for faster degraded fallback
- Current deployment guardrails:
  - membership file must exist before startup
  - placeholder internal tokens are rejected
  - active multi-node endpoints must be valid `https://host:port` values and must not be loopback-only

Disk layout snapshot:

```text
{INDEX_ROOT}/{fragment}/indexdb/partitions/{partition_id}.ndxdb
{BLOB_ROOT}/{fragment}/blob/{version_id}/part_{p}_shard_{i}.shard|.parity
```

### N1-Arc (experimental raw-device profile)

- Raw partitioned `index` plane (`index_store_disks` are raw devices).
- Raw payload plane (`blob_store_disks` are raw devices).
- Managed by `block_manager` with journaling/allocator semantics.
- Supports same object/API behavior contracts as `n1`.

Disk layout snapshot:

```text
{RAW_INDEX_DEVICE} -> block_manager allocator/journal metadata + raw index catalog
{RAW_BLOB_DEVICE} -> block_manager allocator/journal metadata
payload address: raw://{device_id}/{allocation_id}
```

## 4) Recommended Production Deployment

Build and run N1:

```bash
cargo build --release -p n1
./target/release/litiaina-n1
```

Use N1 for:

- production nodes
- integration with N2
- baseline operational runbooks (repair, redistribution, GC, SMB-lite)

## 5) N1-Arc Deployment Guidance

Use `n1-arc` when you need:

- raw device control for both `index` and `blob`
- direct-I/O raw path operations
- storage-kernel deployment shape

`n1-arc` is not the default path when:

- broad operator compatibility is the priority
- you do not want raw-device operational procedures

## 6) N2 Alignment Policy

- N2 documentation and deployment guidance keep `n1` as the default single-node path.
- Multi-node N2 builds on that same `n1`-backed engine through `n2`.
- `n1-arc`-compatible N2 deployment is supported when N1 nodes are intentionally deployed on `n1-arc`.
