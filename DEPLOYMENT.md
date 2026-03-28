# Deployment Index

> Deployment profile status (2026-03-20):
> - `N2` is the product identity of this workspace.
> - `N1` is the shared storage engine used underneath both deployment shapes.
> - Single-node deployment: `n1` (`litiaina-n1`)
> - `n1` release: `0.9.2`
> - Multi-node deployment: `n2` (`litiaina-n2`, `0.2.0`)
> - Registry/business deployment: `registry` (`registry-server`, `0.4.0`)
> - Experimental raw-device profile: `n1-arc` (`litiaina-n1-arc`)
> - Privilege split: prefer a normal service user for `litiaina-n1`, `litiaina-n2`, and `registry-server`. Reserve `root` for `litiaina-n1-arc` raw-device operations only, and let the runtimes validate their writable state paths explicitly.
> - See `DEPLOYMENT_PROFILES.md` for profile policy.

If you are deploying multi-node N2 today, use the dedicated runbook:
- [N2_DEPLOYMENT.md](/home/administrator/Development/N1/N2_DEPLOYMENT.md)

This document now acts as the umbrella deployment index for the workspace. It keeps the single-node and shared deployment guidance, while the multi-node operator runbook lives in its own document:
- [N2 multi-node runbook](/home/administrator/Development/N1/N2_DEPLOYMENT.md)

If you are deploying one node today, use the single-node N2 path on `n1`.

Single-node/default production path:

- Single-node N2 on `n1` (stable/default):
  - `index_store_disks`: mounted filesystem paths
  - `blob_store_disks`: mounted filesystem paths (directory roots)
  - no raw format commands

`n1-arc` still exists, but only as an experimental raw-device appendix at the bottom of this document.
For deployment profile policy, see `DEPLOYMENT_PROFILES.md`.

Current runtime truth:
- `N2` is the platform identity exposed to operators and deployment docs.
- `n1`: single-node N2 deployment on the shared N1 engine with filesystem-backed partitioned `index` and blob roots.
- `n2`: multi-node N2 deployment on the same shared N1 engine, with extra `/v4/n2/*` and `/v4/n2/internal/*` APIs for cluster membership and peer data movement.
- `registry`: SQLite-backed Registry layer for users, ownership, quotas, and logical listing/filtering across both deployment shapes.
- `registry` also brokers short-lived storage sessions for Registry-backed clients and future GUI/web surfaces.
- `registry` also owns first-admin bootstrap, administrator account control, rotating refresh sessions, and optional TOTP 2FA.
- `registry` now also exposes `GET /v1/ws` for Registry-backed realtime events.
- `n1-arc`: raw partitioned `index` and raw blob devices, experimental only.
- stable operator surface today is CLI + HTTP API; web/admin surfaces are still under development
- Auth/session security defaults:
  - access token default TTL: `15 minutes`
  - access token max TTL: `1 hour`
  - refresh token default TTL: `7 days`
  - dedicated CLIs automatically refresh short-lived access tokens in-memory during long-running watch/poll sessions
  - env knobs:
    - `ACCESS_TOKEN_DEFAULT_TTL_SECS`
    - `ACCESS_TOKEN_MAX_TTL_SECS`
    - `REFRESH_TOKEN_TTL_SECS`

## 0. Choose The N2 Deployment Shape

Use the runtime that matches the deployment shape:

- Single-node deployment:
  - run `target/release/litiaina-n1`
  - run `target/release/registry-server`
  - use `target/release/litiaina-n1-cli` for Standard admin commands
  - use normal mounted filesystem paths for `index_store_disks` and `blob_store_disks`
  - this is the recommended production path for one node
  - this is still N2, just without the cluster layer
- Multi-node deployment:
  - run `target/release/litiaina-n2`
  - run `target/release/registry-server`
  - use `target/release/litiaina-n2-cli` for cluster admin commands
  - each node still uses normal mounted filesystem paths for `index_store_disks` and `blob_store_disks`
  - configure `n2.env` and `n2_config.ini`
  - use this when the cluster needs cross-node placement, replication, and peer recovery
- Raw-device engine testing:
  - run `target/release/litiaina-n1-arc`
  - arc remains experimental and is not the default deployment path

## 1. Disk Roles

## Registry Runtime

Run the Registry binary alongside either deployment shape:

- `target/release/registry-server`
- SQLite path configured with `REGISTRY_SQLITE_PATH`
- if `registry.env` is missing in the current working directory, the Registry now creates a template automatically and then stops on the first missing required value
- required env:
  - `REGISTRY_JWT_SECRET`
  - `REGISTRY_MASTER_KEY`
  - `REGISTRY_CERT_PATH`
  - `REGISTRY_KEY_PATH`

The Registry owns business metadata:

- first-administrator bootstrap and admin login
- users and administrator-created accounts
- rotating refresh sessions
- self-service account changes (email, password, optional 2FA)
- ownership
- quotas
- logical object listing/filtering
- registry-wide statistics aggregation for the admin control plane
- filtered object statistics for Registry-backed client insights
- logical object resolution by owner + key
- fragment backend bindings
- short-lived storage session brokering for Registry-backed clients

Required env:
- `REGISTRY_JWT_SECRET`
- `REGISTRY_MASTER_KEY`
- `REGISTRY_CERT_PATH`
- `REGISTRY_KEY_PATH`
- `REGISTRY_SQLITE_PATH`

Storage-broker env:
- `REGISTRY_SINGLE_NODE_URL`
- `REGISTRY_SINGLE_NODE_MASTER_KEY`
- `REGISTRY_MULTI_NODE_URL`
- `REGISTRY_MULTI_NODE_MASTER_KEY`
- `REGISTRY_STORAGE_SESSION_TTL_SECS`

Registry auth/control-plane notes:
- `REGISTRY_MASTER_KEY` is both the broker trust secret for engine-side privileged actions and the trust anchor for first-admin bootstrap.
- Browser/API sessions use rotating refresh tokens through:
  - `POST /v1/auth/refresh`
  - `POST /v1/auth/logout`
- Optional TOTP 2FA is available for both administrator and normal Registry accounts.
- Public signup does not accept caller-chosen quota; new accounts always use the server default until an administrator changes them.

Bootstrap the first Registry administrator once the server is running:

```bash
curl -k -X POST "https://127.0.0.1:51001/v1/admin/bootstrap" \
  -H "Content-Type: application/json" \
  -d '{
    "email":"admin@example.com",
    "password":"replace-this-password",
    "master_key":"<REGISTRY_MASTER_KEY>"
  }'
```

Notes:
- bootstrap only works while the Registry has zero administrator accounts
- later administrator sign-in uses `POST /v1/admin/auth/login`
- account/user administration then happens through the Registry admin APIs:
  - `POST /v1/admin/users`
  - `GET /v1/admin/users`
  - `GET /v1/admin/stats`
  - `POST /v1/admin/users/{uid}/quota`
  - `POST /v1/admin/users/{uid}/credentials`
  - `POST /v1/admin/users/{uid}/2fa/setup`
  - `POST /v1/admin/users/{uid}/2fa/disable`
  - `DELETE /v1/admin/users/{uid}`
- Registry-backed client insights can also use:
  - `GET /v1/objects/stats`

### Index disks (`n1`)

Use normal filesystems for:

- object head metadata
- immutable object version history
- upload sessions
- trash state

Example mountpoints:

```text
/srv/n1/index0
/srv/n1/index1
```

N1 index layout:

```text
{INDEX_ROOT}/{fragment}/indexdb/partitions/{partition_id}.ndxdb
  object_heads
  object_versions
  sessions
```

### Index disks (`n1-arc`)

Use dedicated raw devices for `n1-arc` index authority:

- object metadata
- session metadata
- fragment metadata
- partition WAL/checkpoint data

N1-Arc index layout:

```text
{RAW_INDEX_DEVICE}
  block_manager allocator metadata + journals
  raw index catalog records (sessions/objects/trash/fragment)
```

### Blob disks (`n1`)

Use normal filesystems for blob roots in `n1` deployment:

```text
/srv/n1/blob0
/srv/n1/blob1
```

N1 blob layout:

```text
{BLOB_ROOT}/{fragment}/blob/{version_id}/part_{p}_shard_{i}.shard|.parity
{BLOB_ROOT}/{fragment}/.trash/blob/{version_id}/...
```

### Blob disks (`n1-arc`)

Use dedicated raw devices for:

- data shards
- parity shards
- repaired shard rewrites
- redistribution output

Example device paths:

```text
/dev/disk/by-id/nvme-Samsung_SSD_1
/dev/disk/by-id/nvme-Samsung_SSD_2
/dev/disk/by-id/nvme-Samsung_SSD_3
```

N1-Arc blob layout:

```text
{RAW_BLOB_DEVICE}
  block_manager allocator metadata + journals
  payload extents addressed as raw://{device_id}/{allocation_id}
```

Do not:

- format them
- mount them
- place any filesystem on them
- share them with another service

`n1-arc` rejects startup if a configured raw blob device is mounted.

## 2. Prepare The Host

Install or verify:

- Linux host
- TLS certificates
- dedicated index filesystems
- dedicated raw blob devices (`n1-arc` only)

Recommended:

- use stable `/dev/disk/by-id/*` names
- avoid `/dev/sdX`, `/dev/vdX`, `/dev/nvmeXnY` if enumeration may drift

Verify raw blob devices are unused (`n1-arc` only):

```bash
lsblk -f
findmnt
```

Verify each blob device shows no mounted filesystem:

```bash
findmnt /dev/disk/by-id/nvme-Samsung_SSD_1 || true
```

Prepare explicit raw blob formatting with `n1-arc` (returns nonce + fingerprint):

```bash
sudo target/release/litiaina-n1-arc format-blob-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_1 \
  --prepare
```

Apply format using the returned nonce/fingerprint:

```bash
sudo target/release/litiaina-n1-arc format-blob-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_1 \
  --nonce <nonce-from-prepare> \
  --fingerprint <fingerprint-from-prepare>
```

For destructive reinitialization of a device that already contains foreign or stale data:

```bash
sudo target/release/litiaina-n1-arc format-blob-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_1 \
  --prepare \
  --force

sudo target/release/litiaina-n1-arc format-blob-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_1 \
  --nonce <nonce-from-prepare> \
  --fingerprint <fingerprint-from-prepare> \
  --force
```

This command applies the N1 raw blob format directly and uses a journal sized for current blob workloads.

For index media pre-formatting (arc command path), use the same safety handshake.

```bash
sudo target/release/litiaina-n1-arc format-index-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_INDEX_1 \
  --prepare

sudo target/release/litiaina-n1-arc format-index-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_INDEX_1 \
  --nonce <nonce-from-prepare> \
  --fingerprint <fingerprint-from-prepare>
```

## 3. Example Configuration

`n1_config.ini` (`n1`, stable/default):

```ini
[disk_config]
index_store_disks=/srv/n1/index0,/srv/n1/index1
blob_store_disks=/srv/n1/blob0,/srv/n1/blob1
fs_storage_path=/srv/n1/fs

[upload_config]
data_shards=2
parity_shards=1
stripe_block_size=4194304
```

`n2_config.ini` (multi-node `n2` / `litiaina-n2`):

```ini
[disk_config]
index_store_disks=/srv/n1/index0,/srv/n1/index1
blob_store_disks=/srv/n1/blob0,/srv/n1/blob1,/srv/n1/blob2,/srv/n1/blob3
fs_storage_path=/srv/n1/fs

[upload_config]
data_shards=3
parity_shards=1
stripe_block_size=4194304

[n2]
node_id=node-a
node_endpoint=https://10.0.0.11:50001
cluster_membership_file=/etc/litiaina/n2_cluster_membership.json
internal_token=replace-this-internal-peer-token
cluster_commit_mode=strict_full_placement
auto_maintenance_mode=off
auto_maintenance_max_fragments=64
auto_maintenance_max_objects_per_fragment=256
auto_maintenance_cleanup_orphans=true
auto_cross_params=false
kc=2
mc=1
kd=2
md=1
cluster_stripe_block_size=262144
remote_request_timeout_ms=60000
```

Notes for `n2`:

- `node_endpoint` must be the address other nodes use to reach this node.
- `cluster_membership_file` must be readable and writable by the runtime.
- governance bootstrap now materializes `n2_cluster_membership.epoch-<N>.json` snapshots so rollback has a durable baseline.
- `internal_token` protects the `/v4/n2/internal/*` peer APIs and should be rotated like any other secret.
- `cluster_commit_mode=strict_full_placement` is now the safe multi-node default and requires the planned `kc+mc` target set to commit before `PUT` succeeds.
- Set `cluster_commit_mode=quorum` only when you explicitly want degraded writes to succeed with fewer than the full planned targets.
- `auto_maintenance_mode=off|observe|repair_and_redistribute` controls whether N2 reacts automatically to topology epoch changes.
- Start with `off`, then `observe`, then `repair_and_redistribute` once you trust the cluster behavior.
- Cluster-state changes now use governance-only majority commit.
- Governance is scoped only to membership and committed cluster view.
- Object bytes still move directly peer-to-peer and do not pass through a governance leader.
- `GET /v4/n2/governance/state` exposes the current governance term, leader hint, commit index, and applied membership epoch.
- Internal N2 peer traffic now also proves the current cluster view using the expected membership epoch plus a cluster fingerprint.
- Nodes on stale or drifted cluster views reject internal object/session/map requests instead of silently joining the data path.
- Under `strict_full_placement`, degraded writes with too few active nodes are rejected before ingest with `400 insufficient active nodes: need <kc+mc>, have <active>`.
- `auto_cross_params=true` derives `kc/mc` from active node count and `kd/md` from local blob root count at startup.
- If you want explicit `kc/mc/kd/md`, set `auto_cross_params=false`.
- Startup now fails early if `kc+mc` exceeds active nodes or `kd` exceeds local blob roots.
- `data_shards/parity_shards` are still the local shard layout used by the shared N1 engine inside each node.
- `kc/mc` are the cross-node cluster layout.
- `kd/md` are the per-node local layout for each cluster shard.
- Current shared-core compatibility note:
  - `n2` owns `n2.env` and `n2_config.ini`.
  - `litiaina-n2` now auto-creates `n1.env` and `n1_config.ini` compatibility aliases beside `n2.env` and `n2_config.ini` when they are missing.
  - Operators should treat `n2.env` and `n2_config.ini` as the owned source files.
  - Keep those files in the same working directory as `litiaina-n2` until that filename coupling is removed from the shared core.
- Timeout note:
  - `remote_request_timeout_ms=60000` is conservative.
  - On a low-latency LAN, set it lower (`3000` to `10000`) if you want faster degraded-mode fallback when a peer is black-holed instead of cleanly offline.
- Cluster commit note:
  - Default `cluster_commit_mode=strict_full_placement` requires the full planned `kc+mc` target set.
  - The response now exposes both `required_quorum` and `required_commit_shards`.
  - `committed_shards` remains the immediate placement truth.
  - Use `cluster_commit_mode=quorum` only if you are intentionally trading immediate full placement for degraded-write availability.
  - Run reconcile after degraded periods to restore missing copies.

### Validated Two-Node Reference (`n2 0.2.0`)

This two-node filesystem-backed layout was exercised on real hardware on `2026-03-20`:

- 2 nodes
- 2 index roots per node
- 2 blob roots per node
- explicit cross params:
  - `kc=1`
  - `mc=1`
  - `kd=1`
  - `md=1`
- shared membership epoch file on both nodes
- epoch snapshot files written beside the membership file for rollback
- shared `internal_token` on both nodes
- HTTPS peer transport with self-signed certificates

Observed baseline from that run:

- `256 MiB` object upload through local node:
  - `10.30 s`
  - `24.86 MiB/s`
- `256 MiB` object read:
  - local node: `164.96 MiB/s`
  - remote node: `75.79 MiB/s`
- paginated listing with `limit=5`:
  - page counts: `5 / 5 / 2`
  - continuation tokens advanced correctly
- rename:
  - `402.66 ms`
- delete:
  - `236.78 ms`
- one-node-loss read of the existing `256 MiB` object:
  - `1.74 s`
  - `147.45 MiB/s`
- degraded write while remote node was offline:
  - `64 MiB`
  - `2.21 s`
  - `28.95 MiB/s`
- reconcile after remote node rejoined:
  - `14.34 s`
  - `13` objects scanned
  - `2` objects updated
  - `0` failed

Important interpretation:

- The tested `kc=1 mc=1` cluster stayed readable during one-node loss.
- Degraded writes also succeeded while the remote node was down.
- Reconcile restored the missing peer shards after rejoin.
- On that run, one healthy-cluster `PUT` still returned `committed_shards=1`.
  - Treat `committed_shards` as the immediate placement truth.
  - Do not assume a successful `PUT` means full `kc+mc` placement has already landed.
  - Use reconcile as the repair path when a peer copy is missing.

`n1_config.ini` (`n1-arc`):

```ini
[disk_config]
index_store_disks=/dev/disk/by-id/nvme-Samsung_SSD_INDEX_1,/dev/disk/by-id/nvme-Samsung_SSD_INDEX_2
blob_store_disks=/dev/disk/by-id/nvme-Samsung_SSD_1,/dev/disk/by-id/nvme-Samsung_SSD_2,/dev/disk/by-id/nvme-Samsung_SSD_3
fs_storage_path=/srv/n1/fs

[upload_config]
data_shards=2
parity_shards=1
stripe_block_size=4194304
```

Notes:

- `fs_storage_path` is still required for runtime state/staging.
- Standard stores blob payload files under mounted blob directories.
- `n1` uses `n1.env` and `n1_config.ini`.
- `n2` uses `n2.env` and `n2_config.ini`.
- `n2` still uses the same filesystem-backed disk roles as `n1`, then adds its own `[n2]` runtime section for cluster behavior.
- `n1-arc` stores blob payloads on raw devices through `block_manager`.

## 4. Startup Safeguards

At startup arc validates each raw blob device:

- mounted devices are rejected
- foreign filesystem signatures are rejected
- stable device naming is recommended
- zeroed/unformatted devices are accepted and initialized by `block_manager`

If startup fails on a raw blob device, fix the device state before retrying.

## 5. First Boot On Fresh Blob Devices

`n1-arc` fresh raw blob devices may be:

- zeroed
- empty loopback files for testing
- raw block devices with no valid signature

On first use arc writes its raw blob manager structures to the device. You can also do this explicitly
with `format-blob-device` before first server startup.

That is expected. Do not pre-format the device.

## 6. Systemd Example

`n1` systemd example:

```ini
[Unit]
Description=Litiaina N1 Standard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=administrator
Group=administrator
WorkingDirectory=/opt/n1
EnvironmentFile=/opt/n1/n1.env
ExecStart=/opt/n1/target/release/litiaina-n1
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

`n2` systemd example:

```ini
[Unit]
Description=Litiaina N2 Distribution Runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=administrator
Group=administrator
WorkingDirectory=/opt/n2
EnvironmentFile=/opt/n2/n2.env
ExecStart=/opt/n2/target/release/litiaina-n2
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

`registry` systemd example:

```ini
[Unit]
Description=Litiaina N2 Registry
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=administrator
Group=administrator
WorkingDirectory=/opt/registry
ExecStart=/opt/registry/target/release/registry-server
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Use `/opt/n1/target/release/litiaina-n1-arc` when deploying the `n1-arc` raw-kernel profile.
Prefer not to run `litiaina-n1`, `litiaina-n2`, or `registry-server` with `sudo`; if you do, make sure their writable state paths such as `.auth`, env/config files, and the Registry database stay intentional and writable for the user that will run the service later.

## 7. Health And Observability

### Raw blob status (`n1-arc` only)

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  https://127.0.0.1:50001/v4/maintenance/blob/raw/status
```

This reports:

- configured path
- device id
- total bytes
- used bytes
- free bytes
- used block count
- journal cursor
- mount state
- detected foreign signature
- online / unformatted / error status

### Scrub

Start:

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  https://127.0.0.1:50001/v4/maintenance/scrub/start
```

Status:

```bash
curl -k -H "Authorization: Bearer $TOKEN" \
  https://127.0.0.1:50001/v4/maintenance/scrub/status
```

Stop:

```bash
curl -k -X POST -H "Authorization: Bearer $TOKEN" \
  https://127.0.0.1:50001/v4/maintenance/scrub/stop
```

Scrub is the integrity-check pass for committed shards (raw shards on `n1-arc`, shard files on `n1`).

## 8. Operational Rules

- For `n1-arc`: never mount a raw blob device while N1 is configured to use it.
- For `n1-arc`: never mount a raw index device while N1 is configured to use it.
- For `n1-arc`: never point two N1 instances at the same raw blob device.
- Replace blob devices by:
  1. stopping writes or scheduling maintenance
  2. replacing the failed device
  3. restarting N1
  4. running repair / redistribution as needed
- Keep profile layout consistent: `n1` index on filesystem paths, `n1-arc` index on raw devices.

## 9. Quick Validation Checklist

- n1: index disks mounted and writable
- n1: blob root directories mounted and writable
- n1-arc: index devices raw/unformatted or already N1-managed
- n1-arc: index devices not mounted
- n1-arc: `index_store_disks` uses stable `/dev/disk/by-id/*` names
- n1-arc: blob devices unformatted or already N1-managed
- n1-arc: blob devices not mounted
- n1-arc: `blob_store_disks` uses stable `/dev/disk/by-id/*` names
- n1-arc: `GET /v4/maintenance/blob/raw/status` shows all devices healthy
- scrub can start and report status
- authoritative `index` writes ACK on the owner root first; replica roots are expected to reconcile behind that ACK path
- multipart part uploads are payload-first and disk-backed; committed part metadata remains the resumability source of truth

## 10. Release Hardening On Target Hardware

Run the release drill harness before production rollout (`n1 0.9.2` stable, `n1-arc` experimental):

```bash
scripts/bench/run_release_drills.sh
```

This executes the critical storage workflows:

- active multipart upload + resume while one replica index disk is down
- CRUD/delete/recover/permanent delete
- redistribution + repair + recovered read
- indexdb partition replica convergence
- HA repair with an index disk down

Latest filesystem-backed real-hardware reference:
- validated on March 26, 2026 on the IT server `192.168.0.18`
- isolated harness used the real `/mnt/DISK2` and `/mnt/DISK3` media
- degraded `GET` and ranged `STREAM` held while one disk root was forced offline
- availability-mode redistribution and repair completed cleanly during the degraded window
- policy-cycle redistribution `2-disk -> 1-disk -> 2-disk` completed with the proof object still readable
- soft-delete GC completed in about `4.02 s`
- abandoned-upload GC completed in about `4.03 s`
- artifact: `/home/administrator/DEPLOY_TEST/n1-realtest-20260326-154051-summary.json`

For a heavier gate:

```bash
RUNS=3 \
RUN_CHAOS=1 \
CHAOS_RUNS=5 \
CHAOS_MAX_DURATION_SECS=900 \
scripts/bench/run_release_drills.sh
```

Optional owner-first multipart soak:

```bash
RUN_INDEX_OWNER_RESUME_SOAK=1 \
INDEX_OWNER_RESUME_SOAK_ROUNDS=10 \
scripts/bench/run_release_drills.sh
```

Run the performance probes on the same hardware to establish a baseline:

```bash
scripts/bench/run_perf_probes.sh
```

The probes wrap the ignored benchmark-style tests for:

- CRUD CPU/RAM profile
- single-device upload throughput
- sequential stream locality/readahead

All outputs are written under `.n1-bench/` with timestamped subdirectories so you can compare runs over time.
