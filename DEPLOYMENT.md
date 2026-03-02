# N1 Raw Blob Deployment Runbook

This runbook documents the current N1 deployment model:

- `index_store_disks`: mounted filesystem paths
- `blob_store_disks`: raw, unformatted, unmounted blob devices

N1 owns the blob device format and extent allocation through `block_manager`.

## 1. Disk Roles

### Index disks

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

### Blob disks

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

Do not:

- format them
- mount them
- place any filesystem on them
- share them with another service

N1 rejects startup if a configured blob device is mounted.

## 2. Prepare The Host

Install or verify:

- Linux host
- TLS certificates
- dedicated index filesystems
- dedicated raw blob devices

Recommended:

- use stable `/dev/disk/by-id/*` names
- avoid `/dev/sdX`, `/dev/vdX`, `/dev/nvmeXnY` if enumeration may drift

Verify raw blob devices are unused:

```bash
lsblk -f
findmnt
```

Verify each blob device shows no mounted filesystem:

```bash
findmnt /dev/disk/by-id/nvme-Samsung_SSD_1 || true
```

Format a raw blob device explicitly with N1:

```bash
sudo target/release/litiaina-n1 format-blob-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_1
```

For destructive reinitialization of a device that already contains foreign or stale data:

```bash
sudo target/release/litiaina-n1 format-blob-device \
  --path /dev/disk/by-id/nvme-Samsung_SSD_1 \
  --force
```

This command applies the N1 raw blob format directly and uses a journal sized for current blob workloads.

## 3. Example Configuration

`n1_config.ini`:

```ini
[disk_config]
index_store_disks=/srv/n1/index0,/srv/n1/index1
blob_store_disks=/dev/disk/by-id/nvme-Samsung_SSD_1,/dev/disk/by-id/nvme-Samsung_SSD_2,/dev/disk/by-id/nvme-Samsung_SSD_3
fs_storage_path=/srv/n1/fs

[upload_config]
data_shards=2
parity_shards=1
stripe_block_size=4194304
```

Notes:

- `fs_storage_path` is still required for filesystem-backed control/state and raw staging.
- Blob payloads are not stored under mounted blob directories anymore.

## 4. Startup Safeguards

At startup N1 validates each blob device:

- mounted devices are rejected
- foreign filesystem signatures are rejected
- stable device naming is recommended
- zeroed/unformatted devices are accepted and initialized by `block_manager`

If startup fails on a blob device, fix the device state before retrying.

## 5. First Boot On Fresh Blob Devices

Fresh blob devices may be:

- zeroed
- empty loopback files for testing
- raw block devices with no valid signature

On first use N1 writes its raw blob manager structures to the device. You can also do this explicitly
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
ExecStart=/opt/n1/target/release/server
Restart=always
RestartSec=2
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

## 7. Health And Observability

### Raw blob status

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

Scrub is the integrity-check pass for committed raw shards.

## 8. Operational Rules

- Never mount a blob device while N1 is configured to use it.
- Never point two N1 instances at the same blob device.
- Replace blob devices by:
  1. stopping writes or scheduling maintenance
  2. replacing the failed device
  3. restarting N1
  4. running repair / redistribution as needed
- Keep index disks on normal filesystems; they remain the authoritative metadata plane.

## 9. Quick Validation Checklist

- index disks mounted and writable
- blob devices unformatted or already N1-managed
- blob devices not mounted
- `blob_store_disks` uses stable `/dev/disk/by-id/*` names
- `GET /v4/maintenance/blob/raw/status` shows all devices healthy
- scrub can start and report status

## 10. Release Hardening On Target Hardware

Run the release drill harness before cutting a stable build:

```bash
scripts/bench/run_release_drills.sh
```

This executes the critical raw-blob workflows plus the quota-drift regression:

- raw CRUD/delete/recover/permanent delete
- raw redistribution + repair + recovered read
- raw quota reconvergence after redistribution, trash, GC, and sync
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

Run the performance probes on the same hardware to establish a baseline:

```bash
scripts/bench/run_perf_probes.sh
```

The probes wrap the ignored benchmark-style tests for:

- CRUD CPU/RAM profile
- single-device upload throughput
- sequential stream locality/readahead

All outputs are written under `.n1-bench/` with timestamped subdirectories so you can compare runs over time.
