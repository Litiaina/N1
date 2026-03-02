<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N1</h1>
  <p>
    Fragment-centric object storage engine with a filesystem-backed metadata plane and
    a direct-I/O blob plane, built for lockless streaming, strict crash safety, and
    survivable single-node durability.
  </p>
</div>

---

## Table of Contents

- [What is N1 (current architecture)](#what-is-n1-current-architecture)
- [What N1 Is (Truth Statement)](#what-n1-is-truth-statement)
- [Design principles](#design-principles)
- [Core concepts](#core-concepts)
- [Architecture (Index vs Blob)](#architecture-index-vs-blob)
- [On-disk layout](#on-disk-layout)
  - [Index (metadata mirrors)](#index-metadata-mirrors)
  - [Upload sessions](#upload-sessions)
  - [Blob (data/parity shards)](#blob-dataparity-shards)
- [Write path (multipart upload)](#write-path-multipart-upload)
  - [Initiate upload](#1-initiate-upload)
  - [Upload part (strict commit discipline)](#2-upload-part-strict-commit-discipline)
  - [Finalize upload (publish a version)](#3-finalize-upload-publish-a-version)
  - [Crash matrix (what can exist on disk)](#crash-matrix-what-can-exist-on-disk)
- [Read path (stream + range reads)](#read-path-stream--range-reads)
- [Durability model](#durability-model)
  - [Write quorum](#write-quorum)
  - [Immutability and visibility](#immutability-and-visibility)
- [Maintenance jobs](#maintenance-jobs)
  - [Repair (index + blob)](#repair-index--blob)
  - [Recovery runtime (background heal)](#recovery-runtime-background-heal)
  - [Redistribution](#redistribution)
  - [Garbage collection and trash](#garbage-collection-and-trash)
- [Operational notes](#operational-notes)
  - [Linux-first + O_DIRECT](#linux-first--o_direct)
  - [Preallocation](#preallocation)
  - [Directory sync](#directory-sync)
- [N1 CLI Functions](#n1-cli-functions)
  - [How to run the CLI](#how-to-run-the-cli)
  - [Global CLI flags](#global-cli-flags)
  - [Top-level commands](#top-level-commands)
  - [Ops subcommands (recommended)](#ops-subcommands-recommended)
- [Invariants (must always be true)](#invariants-must-always-be-true)
- [Glossary](#glossary)

---

For SMB network-drive deployment, see `NETWORK_DRIVE.md`.
N1 ships with native SMB-lite via `fsgwd`, with `fsd_mount + Samba` kept as fallback.
For raw blob device setup and operator steps, see `DEPLOYMENT.md`.
For explicit raw blob initialization, use:

```bash
sudo target/release/litiaina-n1 format-blob-device --path /dev/disk/by-id/...
```

Profile note:
- `profiles/n1/high-hw.ini` is for multi-disk, high-parallelism deployments.
- `profiles/n1/single-raw-nvme.ini` is the better starting point for one raw Linux-owned NVMe blob device.

## What is N1 (current architecture)

**Litiaina N1** is a **single-machine, multi-disk** object storage engine that keeps the
metadata/index plane on mounted filesystems while driving the blob plane through direct I/O.
The on-disk metadata layout remains the source of truth.

N1 is designed for environments where you want:

- **NVMe / JBOD speed** without cluster coordination tax
- **video-grade streaming** (long sequential reads + range reads) with bounded memory
- **crash safety** (power-loss safe) and recoverability even on unstable disks/networks
- storage that remains **inspectable and movable on disk** (no opaque black-box volumes)

## What N1 Is (Truth Statement)

N1 is a **fragment-scoped object storage engine with filesystem-backed metadata and a direct-I/O blob backend** for single-machine multi-disk deployments where:

- **high availability** during maintenance matters (CRUD should continue while repair/redistribution run)
- **durability** matters (index mirrors + shard-level recovery)
- **bounded resource usage** matters (streaming-first reads/writes with explicit concurrency controls)
- **no database hot-path dependency** is required (disk layout is the source of truth)

Operationally, “true N1” means:

- fragments are the isolation boundary (quota/auth/maintenance scope)
- object versions are immutable
- index mirrors can diverge temporarily but must be repairable and reconverged
- maintenance is first-class and operator-driven through API + CLI

### Fragment-centric engine (no multitenant UID layout)

N1 is **not** “user multi-tenant storage” anymore. The isolation boundary is a **Fragment**:

- **Fragment = virtual disk / namespace / tenant boundary**
- **Quota is per Fragment**
- **Auth scopes to Fragment**
- **Maintenance jobs operate per Fragment**
- N1 is the **CORE** engine: deterministic disk layout + strict commit discipline + repairability

---

## Design principles

### 1) Filesystem-backed control plane, direct-I/O blob plane

N1 keeps metadata and control state as normal files on configured index disks. The blob plane is
separate: `blob_store_disks` are raw, unformatted, unmounted blob devices managed directly by N1
via `block_manager`. If a configured blob device is mounted, N1 rejects startup.

**Result:** if the service disappears, disks remain mountable and the layout remains readable.

### 2) Immutability as correctness

A finalized object version is immutable:

- shards are never edited in-place after commit
- new writes create a new `version_id`
- delete is represented safely (marker + trash semantics)
- rename is metadata-only

**Result:** crash safety becomes a discipline problem (atomic publish), not a “distributed lock” problem.

### 3) Lockless streaming data path

Hot-path I/O relies on:

- deterministic placement rules
- bounded concurrency (semaphores)
- atomic rename + directory sync for commits

Correctness does not depend on a global mutex.

### 4) Crash-first engineering

N1 assumes failures are normal:

- server stops mid-upload
- disks disappear mid-stream
- replicas diverge
- bitrot happens

N1 stays correct by making partial work non-visible, and recoverable via maintenance tools.

---

## Core concepts

### Fragment
A Fragment is the unit of isolation and operational scope:

- “virtual disk” boundary
- quota boundary
- auth boundary
- directory root on disk

### Object
An object is identified by:

- `fragment`
- `object_key` (path/name inside the fragment)

### Version
A version is immutable and identified by `version_id`.

Uploads produce a new version, and finalization publishes it.

### Part
N1 uses multipart uploads: each part has:

- `part_index`
- `size_bytes`
- `created_at`
- checksums
- `shards[]` (data + parity)

### Shard
Each part is encoded into:

- `data_shards` originals
- `parity_shards` recoveries

Each shard is a **real file** on disk, with size and checksum recorded.

---

## Architecture (Index vs Blob)

N1 separates the world into two storage domains:

### Index store (metadata mirrors)
- immutable metadata files (MessagePack via `rmp-serde`)
- replicated across configured index roots (mirrors)
- repaired by reconciling replicas (missing/divergent/corrupt)

### Blob store (data/parity shards)
- direct-I/O shard payload storage across configured blob disks
- placement is deterministic (no database required)
- read path can degrade and reconstruct missing data using RS decode
- background repair can regenerate missing/corrupt shards

---

## On-disk layout

> Exact constants and suffixes come from your code:
> - `METADATA_EXT` (final object metadata file extension)
> - `PART_META_SUFFIX` (per-part metadata extension)
> - `SHARD_SUFFIX` and `PARITY_SUFFIX` for shard file types
> - `SESSION_MANIFEST_FILE`, `RESERVATION_FILE` in the session dir

Your **ObjectPathManager** defines the precise directories, but the *shape* is:

### Index (metadata mirrors)

Finalized object metadata is stored as replicated files under the fragment index:

```
{INDEX_ROOT}/{fragment}/index/objects/{h1}/{h2}/{hash}#{version_id}{METADATA_EXT}
```

- `hash = sha256(object_key)`
- `h1 = hash[0..2]`, `h2 = hash[2..4]`
- multiple `{INDEX_ROOT}` contain independent replicas

### Upload sessions

Uploads create a session directory for resumability:

```
{INDEX_ROOT}/{fragment}/index/sessions/{version_id}/
  {SESSION_MANIFEST_FILE}
  {RESERVATION_FILE}
  part_{p}{PART_META_SUFFIX}
```

Per-part metadata (`PartMetadata`) is committed locklessly to one healthy index mirror
(and read across mirrors for resumability/verification).

### Blob (data/parity shards)

- `blob_store_disks` points at raw, unformatted device paths such as `/dev/nvme1n1`
- N1 formats and manages those devices through `block_manager`
- committed shard metadata stores `raw://<device_id>/<allocation_id>` style addresses instead of
  depending on shard files under mounted blob directories
- startup rejects mounted blob devices and warns on unstable names like `/dev/sdb`

**Deterministic placement rule (typical):**

- choose disk for each shard as: `(part_index + shard_index) % num_disks`
- require at least `data_shards` successful writers for the part to be accepted

---

## Write path (multipart upload)

Uploads are strict and crash-safe. A part is not considered uploaded unless it passes verification
and is committed into durable raw extents.

### 1) Initiate upload

Initiate does:

- validate auth scopes to fragment
- reserve quota for the fragment (session receipt)
- create `version_id` and `session_id`
- write session manifest to index mirrors (best-effort replication)

### 2) Upload part (strict commit discipline)

This is the critical part.

**Goal:** all final shard filenames are stable and referenced by metadata. Temp names never leak into `PartMetadata`.

#### Step A — Write to temp attempt files

For each shard (data + parity), the writer opens:

```
part_{p}_shard_{i}{ext}.tmp.<attempt>
```

- `<attempt>` is a ULID
- `ext` is `SHARD_SUFFIX` for data shards and `PARITY_SUFFIX` for parity shards

Temp files allow safe re-tries and ensure that incomplete work is never treated as committed data.

#### Step B — Stream encode + write (Reed–Solomon)

- N1 streams request bytes directly into an RS encoder
- stripes are produced at `block_size` (a.k.a. `stripe_size_bytes`)
- each stripe produces `data_shards` blocks and `parity_shards` blocks
- blocks are written to each shard writer

#### Step C — Finalize: length, padding, checksum

At end of stream:

- pad the last stripe with zeros so RS geometry is consistent
- ensure all shard outputs reach the computed `final_shard_len`
- optionally validate the overall checksum (`crc32`) against `UploadPartQuery.checksum`
- compute per-shard checksums for repair verification

#### Step D — Commit to the raw blob backend

After successful write/verify, each shard is committed to its target raw device:

```
part_{p}_shard_{i}{ext}.stage.<attempt>  -->  raw://<device_id>/<allocation_id>
```

Commit rules:

1. stage the encoded shard under the filesystem-backed raw staging area
2. allocate a raw extent on the target blob device
3. copy the staged shard into that extent through `O_DIRECT`
4. flush the raw device and store the raw extent address in metadata
5. only committed raw extent addresses may be referenced by metadata

If backend commit fails on a disk, treat that shard as failed and fail the part if you fall below write quorum.

#### Step E — Persist PartMetadata using stable filenames

Only after commit succeeds do we write `PartMetadata`.

**PartMetadata must store stable names:**
- `ShardInfo.file_name` remains the logical shard name `part_{p}_shard_{i}{ext}`
- `ShardInfo.absolute_path` stores a debug-only raw address path
- `ShardInfo.raw_blob_address` stores the authoritative raw extent address
- `ShardInfo.checksum` is recorded for integrity checks during repair/recovery

The commit pattern is:

- commit raw blob addresses
- then commit per-part metadata (`part_{p}{PART_META_SUFFIX}`)

### 3) Finalize upload (publish a version)

Finalization is a publish step:

- verify all expected parts exist by reading `PartMetadata` across index mirrors
- build `ObjectMetadata` with the full parts + shard maps
- write final object metadata to index mirrors via atomic write + rename + dir sync

Only after metadata is durable does the version become visible to reads/list.

---

## Crash matrix (what can exist on disk)

This is the mental model N1 is built around.

### Crash during part upload (before commit)
You may see:

- staged temporary raw shard files under the raw staging area

**But you must not**:
- reference these in `PartMetadata`
- treat the part as uploaded

Repair/GC can later clean these temp attempts.

### Crash after backend commit but before PartMetadata
You may see:

- committed raw extent addresses exist
- no `part_{p}{PART_META_SUFFIX}`

Finalization will not include that part, because metadata doesn’t claim it.
A re-upload can detect committed shard state and either reuse (if supported) or overwrite via a new attempt.

### Crash after PartMetadata but before final object metadata
You may see:

- committed blob addresses exist
- part metadata exists
- object metadata for the version does not exist

This is safe and resumable: finalize can be retried until publish succeeds.

### Crash after final object metadata
The version is fully committed and immutable. Reads must succeed (possibly degraded) as long as quorum exists.

---

## Read path (stream + range reads)

Reads are optimized for long sequential streaming and range access.

1. resolve metadata (latest or requested version) from index mirrors
2. map requested range → (part, stripe index, shard index, file offset)
3. read shard chunks directly from disk
4. if a shard is missing/corrupt:
   - reconstruct bytes using Reed–Solomon decode
   - optionally schedule background repair to regenerate the missing shard

---

## Durability model

### Write quorum

A part upload requires at minimum:

- `data_shards` successful writers (healthy disks)

If fewer than `data_shards` shards can be written and committed, the part upload fails.

Parity improves survivability:

- with `parity_shards > 0`, N1 can tolerate missing/corrupt shards up to `parity_shards` (for reads/repair)

### Immutability and visibility

A version becomes visible only after:

- stable shard commit (rename + dir sync)
- part metadata commit
- final object metadata publish (atomic write + rename + dir sync)

This prevents partial visibility.

---

## Maintenance jobs

### Repair (index + blob)

Repair is a metadata-and-raw-blob reconciliation job:

- **Index repair:** reconcile missing/divergent object metadata replicas across index roots
- **Shard verification:** availability checks (size) or checksum scrub mode
- **Shard repair:** reconstruct missing/corrupt shards from remaining shards (bounded concurrency)
- **Commit discipline:** repair writes to staged files, commits into raw extents, then updates metadata

### Recovery runtime (background heal)

Reads can degrade gracefully. When a shard is missing/corrupt:

- serve the requested bytes by reconstructing them
- schedule a background `repair_entire_shard()` on a dedicated runtime
- the repair writer stages a shard, writes reconstructed stripes, then commits into a new raw extent

This keeps streaming fast while repairs happen out-of-band.

### Redistribution

Redistribution moves shards to new placement rules without interrupting CRUD:

- read from existing layout (recover if needed)
- write to a new target raw extent
- publish updated metadata
- mark old raw extents for later GC

### Garbage collection and trash

N1 favors safety:

- deletes are represented safely (trash markers / soft delete)
- destructive deletion is handled by GC, not the hot path
- temp attempts (`*.tmp.<attempt>`) are safe to GC because they are never canonical

### Raw blob operations

- `GET /v4/maintenance/blob/raw/status` reports raw blob device state and allocator usage
- `POST /v4/maintenance/scrub/start`
- `POST /v4/maintenance/scrub/stop`
- `GET /v4/maintenance/scrub/status`

Scrub runs the repair engine in checksum-verification mode across committed raw shards.

---

## Operational notes

### Linux-first + O_DIRECT

N1 is Linux-first and uses `O_DIRECT` on the blob data path for predictable latency and reduced
page-cache pressure. This applies to both:

- filesystem-backed blob shard files
- raw blob devices managed directly by `block_manager`

Metadata, quota, prefix indexes, and other control files remain filesystem-backed and buffered by design.

Implications:

- writes should be aligned (e.g., 4096 bytes)
- your `AlignedBuffer` exists specifically for this
- direct I/O failures must be treated as disk health failures (writer disabled)

### Preallocation

Shard files are often preallocated via `set_len(final_shard_len)`:

- reduces fragmentation
- makes failure modes clearer
- enforces deterministic shard size expectations for availability checks

### Directory sync

Durable commit requires syncing the directory entry after rename:

- `fsync(file)`
- `rename(tmp, stable)`
- `fsync(parent_dir)` (or platform equivalent)

This is why `sync_dir()` exists. Without it, rename can be lost after power loss even if the file contents were flushed.

---

## N1 CLI Functions

N1 CLI is embedded in the `server` binary (`server/src/main.rs`) and uses `server_cli` as the command layer.

### How to run the CLI

Use:

```bash
cargo run -p server -- <command> [flags]
```

or the built binary:

```bash
./target/release/litiaina-n1 <command> [flags]
```

Server mode:

```bash
cargo run -p server -- server
```

If no command is given, server mode is the default.

### Global CLI flags

Available on all commands:

- `-s, --secret <SECRET>`: secret input for auth
- `-f, --fragment <FRAGMENT>`: target fragment (default `admin`)
- `--url <URL>`: explicit N1 API URL
- `--insecure <bool>`: TLS cert validation toggle (default `true`)
- `--poll-ms <N>`: watch polling interval in milliseconds (default `500`)
- `--json <bool>`: machine-readable output mode (default `false`)

Auth behavior for CLI:
- Maintenance/admin flows use `MASTER_KEY` as `--secret` and must target `--fragment <ADMIN_KEY>`
- Fragment-scoped data access uses fragment secret + fragment hash

### Top-level commands

#### `server`
Starts the N1 service process.

```bash
cargo run -p server -- server
```

#### `create-fragment`
Creates a new fragment.

Flags:
- `--name <NAME>` required
- `--quota <BYTES>` optional (default `10737418240`)

```bash
cargo run -p server -- create-fragment --name imaging-a --quota 1099511627776
```

#### `gc`
Starts garbage collection and follows status.

```bash
cargo run -p server -- gc --fragment <fragment_hash>
```

#### `repair`
Starts repair and follows status.

Flags:
- `--dry-run` optional

```bash
cargo run -p server -- repair --fragment <fragment_hash> --dry-run
```

#### `redistribute`
Starts redistribution and follows status.

Flags:
- `--dry-run` optional

```bash
cargo run -p server -- redistribute --fragment <fragment_hash>
```

#### `prefix-rebuild`
Starts prefix index rebuild and follows status.

```bash
cargo run -p server -- prefix-rebuild --fragment <fragment_hash>
```

#### `health`
Gets health once, or watches continuously.

Flags:
- `--watch`
- `--deep-scan`

```bash
cargo run -p server -- health --fragment <fragment_hash> --watch --deep-scan
```

### Ops subcommands (recommended)

`ops` gives explicit lifecycle control with `start|status|stop|watch`.

Pattern:

```bash
cargo run -p server -- ops <job> <action> [job-flags]
```

Jobs:

- `ops gc start|status|watch`
  - `ops gc stop` is currently not supported by server API
- `ops repair start|status|stop|watch [--dry-run]`
- `ops redistribute start|status|stop|watch [--dry-run]`
- `ops prefix-rebuild start|status|stop|watch [--fragment <fragment_hash>]`

Streaming tuning knobs (`[upload_config]`):
- `stream_chunk_size`
- `stream_readahead_bytes`
- `stream_locality_bias_enabled`
- `upload_part_strict_durability_default`
- `upload_io_gate_enabled`

Examples:

```bash
# GC lifecycle (admin context)
export ADMIN_KEY="<admin_fragment_key>"
cargo run -p server -- ops gc start --fragment "$ADMIN_KEY" --secret "<MASTER_KEY>"
cargo run -p server -- ops gc status --fragment "$ADMIN_KEY" --secret "<MASTER_KEY>"

# Repair lifecycle
cargo run -p server -- ops repair start --fragment <fragment_hash>
cargo run -p server -- ops repair status --fragment <fragment_hash>
cargo run -p server -- ops repair watch --fragment <fragment_hash> --poll-ms 250
cargo run -p server -- ops repair stop --fragment <fragment_hash>

# Redistribution lifecycle
cargo run -p server -- ops redistribute start --fragment <fragment_hash>
cargo run -p server -- ops redistribute status --fragment <fragment_hash>
cargo run -p server -- ops redistribute stop --fragment <fragment_hash>

# Prefix rebuild lifecycle
cargo run -p server -- ops prefix-rebuild start --fragment <fragment_hash>
cargo run -p server -- ops prefix-rebuild status --fragment <fragment_hash>
cargo run -p server -- ops prefix-rebuild watch --fragment <fragment_hash>
```

Sequential stream probe (readahead/locality):
```bash
N1_STREAM_PROBE_OBJECT_BYTES=$((64*1024*1024)) \
N1_STREAM_PROBE_PART_BYTES=$((4*1024*1024)) \
N1_STREAM_PROBE_PASSES=2 \
cargo test -p server matrix_stream_sequential_readahead_locality_probe -- --ignored --nocapture
```

### General Testing

```bash
# Core server test suite
cargo test -p server

# CLI test suite
cargo test -p server_cli

# Ignored/manual suites
cargo test -p server -- --ignored --nocapture
```

---

## Invariants (must always be true)

These are the non-negotiables for correctness:

1. **No temp filenames in committed metadata**
   - `PartMetadata` must reference stable shard names only
2. **Stable shard commit before part metadata commit**
3. **Final object metadata publish is the only visibility gate**
4. **Never mutate committed shards in place**
5. **Every commit uses rename + directory sync**
6. **Write quorum must hold**
   - fail the part if healthy writers < `data_shards`
7. **Repair writes are also immutable**
   - repair via temp → rename, never patch

---

## Glossary

- **fragment**: virtual disk / tenant boundary
- **object_key**: object path/name within a fragment
- **version_id**: immutable identifier of a committed object version
- **session_id**: upload session identifier for resumability
- **part**: multipart chunk with shard map
- **shard**: one data/parity file on disk
- **data_shards / parity_shards**: Reed–Solomon parameters
- **stripe_size_bytes**: block size used per shard per stripe

---

## Chaos benchmark harness

N1 includes a dependency-free HA chaos runner at:

`scripts/bench/run_ha_chaos.sh`

It repeatedly runs the ignored test `matrix_ha_chaos_profile_nightly` and writes:

- per-run logs (`run_N.log`)
- machine-readable CSV summary (`summary.csv`)
- pass/fail summary (`summary.txt`)

### Quick start

```bash
scripts/bench/run_ha_chaos.sh
```

### Custom stress profile

```bash
RUNS=10 \
N1_SOAK_ROUNDS=20 \
N1_SOAK_OPS_PER_ROUND=32 \
N1_SOAK_CLIENT_CONCURRENCY=12 \
N1_SOAK_DISK_DOWN=1 \
scripts/bench/run_ha_chaos.sh
```

### CI-style gating

Fail if any run fails and average runtime exceeds 600s:

```bash
RUNS=5 \
MAX_DURATION_SECS=600 \
REQUIRE_ALL_PASS=1 \
scripts/bench/run_ha_chaos.sh
```

All outputs are written under `.n1-bench/ha-chaos-<timestamp>/` by default.

## Release drill harness

For pre-release hardening on target hardware, use:

`scripts/bench/run_release_drills.sh`

It runs the release-critical raw/repair workflows:

- raw blob CRUD + delete/recover/permanent delete
- raw blob quota drift reconvergence across redistribution, trash, GC, and sync
- raw blob redistribution + repair + recovered read + GC marker cleanup
- index mirror repair convergence
- HA repair reconvergence with an index disk down

It also optionally chains into the nightly HA chaos harness.

### Quick start

```bash
scripts/bench/run_release_drills.sh
```

### Harder gate on real hardware

```bash
RUNS=3 \
RUN_CHAOS=1 \
CHAOS_RUNS=5 \
CHAOS_MAX_DURATION_SECS=900 \
scripts/bench/run_release_drills.sh
```

Outputs are written under `.n1-bench/release-drills-<timestamp>/`.

## Performance probes

N1 also ships a wrapper for the ignored performance probes:

`scripts/bench/run_perf_probes.sh`

This runs:

- `matrix_crud_cpu_ram_probe`
- `matrix_upload_single_nvme_saturation_probe`
- `matrix_stream_sequential_readahead_locality_probe`

### Quick start

```bash
scripts/bench/run_perf_probes.sh
```

### Example tuned run

```bash
N1_CRUD_PROBE_ROUNDS=2 \
N1_CRUD_PROBE_OBJECTS=1024 \
N1_CRUD_PROBE_CONCURRENCY=24 \
N1_UPLOAD_PROBE_CONCURRENCY=8 \
N1_UPLOAD_PROBE_OBJECTS_PER_WORKER=4 \
N1_STREAM_PROBE_PASSES=3 \
scripts/bench/run_perf_probes.sh
```

Outputs are written under `.n1-bench/perf-probes-<timestamp>/`.

---

© Litiaina --- N1 Core Engine
