# N1 Edge Documentation

N1 Edge 1.0.0 is the primary filesystem-native N1 engine face.

N1 Standard remains the sharded indexdb engine. N1 Edge is not an indexdb compatibility mode and does not use a database as the active metadata authority. Edge keeps the N1 blob/shard/parity plane, repair model, redistribution model, GC model, and public route shape, but replaces the active index authority with filesystem records.

## Identity

N1 Edge is:

- filesystem-native
- Linux-first
- IO-bound by design
- zero-decode for normal listing
- immutable for committed object version metadata
- POSIX-like for folders, rename, move, trash, recover, and listing
- backed by the existing N1 blob/shard/parity data plane

Public/operator wording should say:

- N1 Edge index
- filesystem-native index
- `.ndx` metadata record
- per-fragment Edge index
- N1 Edge storage kernel

Do not describe Edge as sqlite-backed, indexdb-backed, database-backed, or a sharded database. Those describe N1 Standard, not N1 Edge.

## Final Disk Layout

Each configured index root contains one Edge index tree per fragment:

```text
<index_root>/<fragment_hash>/
  .fragment.ndx
  .objects/
    @o_<object-id>/
      <ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
      <ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
  .uploads/
    <version_id>/
      manifest.ndx
      binding.ndx
      part_p00000000000000000000.<crc32>.receipt
      shard_p00000000000000000000_s0000.<crc32>.receipt
  .trash/
    docs/
      video.mp4 -> @o_<object-id>
  docs/
    video.mp4 -> @o_<object-id>
```

Blob layout is unchanged:

```text
<blob_root>/<fragment_hash>/blob/<version_id>/<shard-or-parity-file>
```

Both disks may house both index and blob roots. The minimum N1 Edge profile remains two configured roots, for example `/mnt/disk-00` and `/mnt/disk-01`.

## What `.fragment.ndx` Is

`.fragment.ndx` is the per-fragment Edge control record. It stores fragment-level operational state such as quota/project identity, state, and timestamps.

It is not object metadata truth. It is not a namespace catalog. It is not a database. Object truth is in `.objects/@o_<object-id>/*.ndx`, namespace truth is in real directories and symlink pointers, and payload truth remains in the blob plane.

## Namespace And Object Truth

N1 Edge has three clear planes:

- Namespace: real directories and file symlinks.
- Object index: flat `.objects/@o_<object-id>/` directories with immutable `.ndx` versions.
- Blob plane: immutable shard/parity files under `<blob_root>/<fragment>/blob/<version_id>/`.

The namespace symlink is the mutable POSIX face:

```text
docs/video.mp4 -> @o_01KQ...
```

The symlink payload is a stable object id. N1 reads it with `readlink` and resolves it to:

```text
<index_root>/<fragment>/.objects/@o_01KQ.../
```

The symlink target is intentionally not a depth-relative filesystem path. That makes folder rename and folder move safe: moving `docs/` to `archive/docs/` does not require rewriting child symlinks.

## Object Versions

Each committed version is one immutable `.ndx` file:

```text
<ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
```

The filename is the list summary. Normal listing parses this filename only.

The `.ndx` body contains full immutable object metadata for download, verify, repair, redistribution, recover-on-read, archive generation, and GC.

Head/latest is:

```text
max ULID among valid .ndx filenames under .objects/@o_<object-id>/
```

There is no `.head`, mutable head pointer, object head table, version counter, sidecar summary file, or correctness cache.

## Packed Extension

The packed extension field is a zero-allocation u64:

- byte 0 stores extension length
- bytes 1 through 7 store lowercase ASCII extension bytes
- extensions longer than 7 bytes store `0`

This keeps normal listing from decoding `.ndx` bodies just to know basic file type.

## Normal Listing

Normal listing is direct directory scanning:

```text
prefix=docs/
open <index_root>/<fragment>/docs/
read direct children only
```

For each direct child:

- normal directory -> directory entry
- symlink whose payload is `@o_<object-id>` -> file entry
- reserved internal entry -> hidden

For a file symlink, Edge:

1. reads the symlink payload
2. opens `.objects/@o_<object-id>/`
3. chooses the greatest valid ULID `.ndx` filename
4. parses size, timestamp, extension, and version id from the filename
5. returns the entry

Normal listing does not decode `.ndx` bodies.

Reserved internal names are hidden from user listing:

```text
.objects
.uploads
.trash
.tmp
.locks
.gc
.repair
.intents
.trash-jobs
blob
@o_*
```

User paths containing reserved segments are rejected.

## POSIX Semantics

File rename:

```text
rename docs/video.mp4 -> docs/new-video.mp4
```

This renames the symlink only. The object id stays the same, so version lineage is preserved.

Folder rename:

```text
rename docs -> archive/docs
```

This renames the directory tree only. Child symlinks keep pointing to the same stable object ids.

Soft delete:

```text
mv docs/video.mp4 .trash/docs/video.mp4
```

Recover:

```text
mv .trash/docs/video.mp4 docs/video.mp4
```

Hard delete removes the trash symlink or trash directory. After no live or trash symlink references an object id, Edge removes the unreferenced `.objects/@o_<object-id>/` index record so GC can reclaim headless blobs.

Rename, trash, and recover never move blobs.

## Read And Download

Read paths decode `.ndx`; listing does not.

Latest read:

1. resolve logical path through namespace symlink
2. open `.objects/@o_<object-id>/`
3. choose greatest ULID `.ndx`
4. decode `.ndx`
5. read through the existing N1 blob/shard/parity plane

Exact version read selects the requested ULID `.ndx` before decoding.

## Upload

One-shot upload keeps the existing N1 data plane:

```text
stream bytes -> erasure-coded blob/shard/parity files
build immutable object metadata with stable object id
write temp .ndx
fsync temp file
rename temp -> final .ndx
fsync parent directory
publish or reuse namespace symlink
```

If the logical path already points to an object id, the new version appends under the same `.objects/@o_<object-id>/` directory. Rename does not break version history.

## Multipart Upload

Incomplete multipart state stays outside the live namespace:

```text
<index_root>/<fragment>/.uploads/<version_id>/
```

Receipts are filename-authoritative and intentionally tiny:

```text
part_p<part-number>.<crc32>.receipt
shard_p<part-number>_s<shard-index>.<crc32>.receipt
```

Receipt bodies are not authoritative. Finalize reads the upload manifest and receipt filenames, rebuilds full object metadata, writes the final `.ndx`, and publishes the namespace symlink. Live listing never sees incomplete multipart state.

## GC

GC is reachability-driven.

GC may decode `.ndx` bodies because it needs full blob references. It protects blobs referenced by committed object records whose object id is referenced by live namespace symlinks or trash symlinks.

When trash is emptied, bytes are not promised back immediately. Blob bytes return after GC removes headless blob versions.

Fragment-scoped and global routes are supported:

```http
POST /n1/maintenance/fragments/{fragment}/gc/start
POST /n1/maintenance/gc/start
GET  /n1/maintenance/gc/status
```

## Repair And Redistribution

Repair and redistribution remain N1 operations over the existing blob/shard/parity system. Edge only changes how metadata reachability is found.

Repair must restore:

- missing Edge index records on rebuilt index roots
- missing namespace symlinks from mirrored index roots when available
- missing blob shards/parity where recoverable
- listing after disk loss/reformat
- download after repair

Redistribution may move blob placement when disk topology changes. It must preserve stable object ids, namespace symlinks, immutable `.ndx` records, and blob references.

Both operations can run per fragment or globally.

## Public API Shape

Common fragment routes:

```http
POST /n1/fragments
GET  /n1/fragments
GET  /n1/fragments/{fragment}/quota
PATCH /n1/fragments/{fragment}/quota
```

Object routes:

```http
GET    /n1/objects?prefix=docs/&limit=200
PUT    /n1/object/{*object_key}
GET    /n1/object/{*object_key}
DELETE /n1/object/{*object_key}
GET    /n1/objects/versions/{*object_key}
DELETE /n1/objects/version/{version_id}/{*object_key}
```

POSIX routes:

```http
GET  /n1/posix/list?path=docs&limit=200
POST /n1/posix/mkdir
POST /n1/posix/rename
POST /n1/posix/trash
POST /n1/posix/recover
POST /n1/posix/trash/empty
```

Multipart routes:

```http
POST /n1/upload/multipart/initiate
PUT  /n1/upload/multipart/{version_id}/{part_index}
POST /n1/upload/multipart/{version_id}/{session_id}/finalize
GET  /n1/upload/multipart/{version_id}
DELETE /n1/upload/multipart/{version_id}
```

Archive download routes use namespace truth and then decode selected `.ndx` records for payload reads.

## Operational Tradeoff

N1 Edge is optimized for durable object storage, large byte flow, POSIX movement, and bounded memory. It can store many small objects, but tiny-object storms pay filesystem commit and EC flush overhead per object. For very high counts of tiny files, archiving or batching before upload is usually more efficient.

This is an intentional storage-kernel tradeoff: Edge stays close to the filesystem and kernel instead of hiding the workload in RAM-heavy caches.

## Verification

Source-level verification currently includes:

```bash
RUSTFLAGS='-D warnings' cargo test -p n1-edge --all-targets
RUSTFLAGS='-D warnings' cargo check --workspace --all-targets
```

The non-destructive HTTP smoke proof can be run on two temporary roots:

```bash
mkdir -p /tmp/n1-edge-proof-disk-00 /tmp/n1-edge-proof-disk-01
python3 scripts/bench/n1_edge_real_proof.py \
  --binary target/debug/litiaina-n1-edge \
  --source-dir /home/administrator/Development/N2/n1-edge/src \
  --disk-roots /tmp/n1-edge-proof-disk-00,/tmp/n1-edge-proof-disk-01 \
  --out-dir /tmp/n1-edge-proof-run \
  --port 50661 \
  --concurrency 4 \
  --quota-backend disabled \
  --quota-bytes 1073741824 \
  --prefix edge-smoke/ \
  --max-files 8 \
  --skip-redistribution \
  --cleanup-roots
```

The Edge unit suite covers:

- reserved path rejection
- correct packed extension encoding
- zero-decode live listing with invalid `.ndx` bodies
- friendly POSIX pagination from symlink names
- file rename preserving object id and version lineage
- folder move preserving symlink object pointers
- soft delete moving namespace symlinks to trash
- hard delete removing unreferenced object index records
- multipart receipt filenames as zero-decode state

Real hardware qualification should still be performed before release promotion:

- CRUD
- POSIX list/rename/move/trash/recover
- one-shot upload
- multipart upload/resume/finalize
- archive download
- recover-on-read
- GC after hard delete
- repair after simulated disk loss/reformat
- redistribution 2 roots -> 1 root -> 2 roots
- concurrent multi-user upload on `/mnt/disk-00` and `/mnt/disk-01`

Latest local proof run on 2026-05-06:

- `cargo test -p n1-edge --all-targets` passed with `RUSTFLAGS=-D warnings`.
- `cargo check --workspace --all-targets` passed with `RUSTFLAGS=-D warnings`.
- Edge real proof on `/mnt/disk-00` and `/mnt/disk-01` passed CRUD, POSIX, archive, multipart, recover-on-read, simulated disk-format repair, and redistribution `2 disk -> 1 disk -> 2 disk`.
- The real proof ended with `live_ndx=12`, `namespace_symlinks=12`, and `blob_version_dirs=12` on both roots.
- Four-user storm passed on the same two disks: `12,000` one-shot objects plus `4` multipart objects of `16 MiB` each; Edge POSIX listing returned `12,004` objects exactly.
- Storm memory remained bounded: max server RSS was about `258 MiB` with `42` threads.
- Storm throughput on the bad-disk tiny-object workload was `14.343 objects/s` and `0.203 MiB/s`, confirming the expected IO/fsync-heavy small-object tradeoff rather than a RAM runaway.
