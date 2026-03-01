# N1 Redistribution Guide

This document reflects the current redistribution engine and API behavior in N1.

Redistribution is the online maintenance job that realigns existing objects to the
current runtime storage topology:

- target EC layout (`data_shards`, `parity_shards`, `stripe_block_size`)
- target blob disk set and placement
- target index mirror set

It runs without requiring a database and is designed to keep CRUD available while it
migrates data in the background.

## Current API Surface

- `POST /v4/maintenance/redistribution/start?dry_run=true|false`
- `PATCH /v4/maintenance/redistribution/stop`
- `GET /v4/maintenance/redistribution/monitor`

Important differences from older docs:

- endpoint is `redistribution/start` (not `/v4/maintenance/redistribute`)
- status endpoint is `redistribution/monitor`
- stop is `PATCH`, not `POST`

## Authentication and Permissions

Redistribution endpoints require a valid bearer token from:

- `POST /authenticate/fragment`

Required claims:

- `start`: `write=true`, `modify=true`, `delete=true`
- `stop`: `write=true`, `modify=true`, `delete=true`
- `monitor`: `read=true`

Admin bypass behavior:

- when `fragment == ADMIN_KEY`, server validates `secret == MASTER_KEY`
- otherwise fragment must exist and secret must match that fragment key

## CLI Usage

Preferred commands:

```bash
# Start redistribution
cargo run -p server -- ops redistribute start --secret "$MASTER_KEY" --fragment "$ADMIN_KEY" --insecure

# Dry-run (plan only, no writes)
cargo run -p server -- ops redistribute start --dry-run --secret "$MASTER_KEY" --fragment "$ADMIN_KEY" --insecure

# Watch monitor
cargo run -p server -- ops redistribute watch --secret "$MASTER_KEY" --fragment "$ADMIN_KEY" --insecure

# One-shot status
cargo run -p server -- ops redistribute status --secret "$MASTER_KEY" --fragment "$ADMIN_KEY" --insecure

# Stop job (cooperative)
cargo run -p server -- ops redistribute stop --secret "$MASTER_KEY" --fragment "$ADMIN_KEY" --insecure
```

Notes:

- `ops redistribute start` only submits the job.
- `ops redistribute watch` attaches a live terminal monitor.
- monitor polling uses `--poll-ms` (global CLI flag).

## HTTP Usage

```bash
# 1) Authenticate
TOKEN="$(
  curl -k -sS -X POST https://127.0.0.1:50001/authenticate/fragment \
    -H 'Content-Type: application/json' \
    -d '{"fragment":"'"$ADMIN_KEY"'","secret":"'"$MASTER_KEY"'","read":true,"write":true,"modify":true,"delete":true}' \
  | jq -r .access_token
)"

# 2) Start full redistribution
curl -k -sS -X POST \
  "https://127.0.0.1:50001/v4/maintenance/redistribution/start?dry_run=false" \
  -H "Authorization: Bearer $TOKEN"

# 3) Monitor
curl -k -sS https://127.0.0.1:50001/v4/maintenance/redistribution/monitor \
  -H "Authorization: Bearer $TOKEN"

# 4) Stop
curl -k -sS -X PATCH https://127.0.0.1:50001/v4/maintenance/redistribution/stop \
  -H "Authorization: Bearer $TOKEN"
```

## What the Engine Actually Does

At job start:

- reads target topology from runtime config atomics
- resolves currently reachable blob/index roots
- rejects run if no reachable blob or index roots

Scan scope:

- scans both live `index/` and trash `.trash/index/`
- skips delete-marker metadata (`is_delete_marker = true`)

For each object version, redistribution is needed when any of these are true:

- stripe size differs from target
- current data/parity shard counts differ from target
- shard absolute paths do not match expected target placement
- live/trash path class is inconsistent
- metadata file is missing on any reachable target index root

Migration path (non-dry-run):

- streams old shards and re-encodes to new EC layout
- writes new shards through direct I/O writer path
- verifies shard length and checksum before publish
- atomically publishes new blob placements as raw extent addresses on raw blob devices
- atomically writes updated metadata to reachable index mirrors
- stages old shards for GC cleanup
- tracks touched fragments for post-job prefix rebuild

Post phase:

- if non-dry-run and fragments were touched, triggers prefix rebuild per touched fragment
- marks final phase as `Completed`, `Stopped by User`, or `Failed`

Dry-run behavior:

- performs full scan and need-detection
- increments migration counters for planned objects
- does not write shards or metadata

## Status Fields (`/monitor`)

The monitor payload includes:

- `is_running`
- `phase`
- `total_fragments_scanned`
- `total_objects_scanned`
- `total_objects_need_redistribution`
- `total_objects_migrated`
- `total_errors`
- `current_fragment`
- `last_object`
- `last_error`
- `start_timestamp`
- `duration_seconds`
- `scan_rate_per_sec`

## Operational Guidance

Before running:

- apply new disk/EC configuration and ensure server has loaded it
- verify index disks are mounted and writable
- verify blob devices are raw, unmounted, and visible to N1
- run dry-run first to estimate impact

During run:

- expect increased disk I/O and CPU
- CRUD remains available, but throughput can dip under load
- use `ops redistribute watch` for real-time visibility

After run:

- check `phase == Completed`
- ensure `total_errors == 0`
- run health and consistency checks
- only then decommission removed disks

## Known Constraints

- stop is cooperative, not instant; current object step may finish first
- unreachable disks are skipped from live target sets; this can limit migration coverage
- if target data shard count exceeds reachable blob disks, affected objects fail with
  `InsufficientStorage`
- rerunning redistribution is safe and expected for convergence after transient failures
