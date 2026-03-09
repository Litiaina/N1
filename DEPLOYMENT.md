# N1 Deployment Runbook (Standard + Arc)

> Deployment profile status (2026-03-09):
> - Stable/default: `standard` (`litiaina-n1-standard`)
> - Stable raw-kernel: `arc` (`litiaina-n1-arc`, `0.2.0`)
> - See `DEPLOYMENT_PROFILES.md` for profile policy.

This runbook documents both N1 deployment profiles:

- `standard` (stable/default):
  - `index_store_disks`: mounted filesystem paths
  - `blob_store_disks`: mounted filesystem paths (directory roots)
  - no raw format commands
- `arc` (stable raw-kernel):
  - `index_store_disks`: raw, unformatted, unmounted devices
  - `blob_store_disks`: raw, unformatted, unmounted devices
  - raw format commands available (`format-blob-device`, `format-index-device`)

N1 Arc owns raw index/blob device format and extent allocation through `block_manager`.
For deployment profile policy, see `DEPLOYMENT_PROFILES.md`.

Current runtime truth:
- `standard`: filesystem-backed partitioned `index` and blob roots.
- `arc`: raw partitioned `index` and raw blob devices.

## 1. Disk Roles

### Index disks (`standard`)

Use normal filesystems for:

- object metadata
- quota state
- fragment metadata
- sessions
- prefix and delimiter indexes
- trash index mirrors

Example mountpoints:

```text
/srv/n1/index0
/srv/n1/index1
```

Standard index layout:

```text
{INDEX_ROOT}/{fragment}/index/partitions/{partition_id}/
  wal/...
  checkpoints/...
  sessions/...
  objects/...
  trash/...
```

### Index disks (`arc`)

Use dedicated raw devices for Arc index authority:

- object metadata
- session metadata
- quota and fragment metadata
- partition WAL/checkpoint data

Arc index layout:

```text
{RAW_INDEX_DEVICE}
  block_manager allocator metadata + journals
  raw index catalog records (sessions/objects/trash/quota/fragment)
```

### Blob disks (`standard`)

Use normal filesystems for blob roots in standard deployment:

```text
/srv/n1/blob0
/srv/n1/blob1
```

Standard blob layout:

```text
{BLOB_ROOT}/{fragment}/blob/{version_id}/part_{p}_shard_{i}.shard|.parity
{BLOB_ROOT}/{fragment}/.trash/blob/{version_id}/...
```

### Blob disks (`arc`)

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

Arc blob layout:

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

Arc rejects startup if a configured raw blob device is mounted.

## 2. Prepare The Host

Install or verify:

- Linux host
- TLS certificates
- dedicated index filesystems
- dedicated raw blob devices (arc only)

Recommended:

- use stable `/dev/disk/by-id/*` names
- avoid `/dev/sdX`, `/dev/vdX`, `/dev/nvmeXnY` if enumeration may drift

Verify raw blob devices are unused (arc only):

```bash
lsblk -f
findmnt
```

Verify each blob device shows no mounted filesystem:

```bash
findmnt /dev/disk/by-id/nvme-Samsung_SSD_1 || true
```

Prepare explicit raw blob formatting with N1 Arc (returns nonce + fingerprint):

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

`n1_config.ini` (standard, stable/default):

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

`n1_config.ini` (arc):

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
- Arc stores blob payloads on raw devices through `block_manager`.

## 4. Startup Safeguards

At startup arc validates each raw blob device:

- mounted devices are rejected
- foreign filesystem signatures are rejected
- stable device naming is recommended
- zeroed/unformatted devices are accepted and initialized by `block_manager`

If startup fails on a raw blob device, fix the device state before retrying.

## 5. First Boot On Fresh Blob Devices

Arc fresh raw blob devices may be:

- zeroed
- empty loopback files for testing
- raw block devices with no valid signature

On first use arc writes its raw blob manager structures to the device. You can also do this explicitly
with `format-blob-device` before first server startup.

That is expected. Do not pre-format the device.

## 6. Systemd Example

```ini
[Unit]
Description=Litiaina N1
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/n1
EnvironmentFile=/opt/n1/n1.env
ExecStart=/opt/n1/target/release/litiaina-n1-standard
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Use `/opt/n1/target/release/litiaina-n1-arc` when deploying the Arc raw-kernel profile.

## 7. Health And Observability

### Raw blob status (`arc` only)

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

Scrub is the integrity-check pass for committed shards (raw shards on arc, shard files on standard).

## 8. Operational Rules

- For arc: never mount a raw blob device while N1 is configured to use it.
- For arc: never mount a raw index device while N1 is configured to use it.
- For arc: never point two N1 instances at the same raw blob device.
- Replace blob devices by:
  1. stopping writes or scheduling maintenance
  2. replacing the failed device
  3. restarting N1
  4. running repair / redistribution as needed
- Keep profile layout consistent: `standard` index on filesystem paths, `arc` index on raw devices.

## 9. Quick Validation Checklist

- standard: index disks mounted and writable
- standard: blob root directories mounted and writable
- arc: index devices raw/unformatted or already N1-managed
- arc: index devices not mounted
- arc: `index_store_disks` uses stable `/dev/disk/by-id/*` names
- arc: blob devices unformatted or already N1-managed
- arc: blob devices not mounted
- arc: `blob_store_disks` uses stable `/dev/disk/by-id/*` names
- arc: `GET /v4/maintenance/blob/raw/status` shows all devices healthy
- scrub can start and report status
- authoritative `index` writes ACK on the owner root first; replica roots are expected to reconcile behind that ACK path
- multipart part uploads are payload-first and disk-backed; committed part metadata remains the resumability source of truth

## 10. Release Hardening On Target Hardware

Run the release drill harness before production rollout (`standard 0.4.0` stable, `arc 0.2.0` stable):

```bash
scripts/bench/run_release_drills.sh
```

This executes the critical storage workflows plus the quota-drift regression:

- active multipart upload + resume while one replica index disk is down
- CRUD/delete/recover/permanent delete
- redistribution + repair + recovered read
- quota reconvergence after redistribution, trash, GC, and sync
- index mirror repair convergence
- HA repair with an index disk down

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
