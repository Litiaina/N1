# N1 Edge Documentation

N1 Edge is a separate N1 engine face.

N1 Standard remains the sharded `indexdb` engine. N1 Edge replaces that metadata authority with a filesystem-native index plane. Do not treat Edge as an indexdb compatibility mode, a database wrapper, or a migration layer around Standard.

N1 Edge is intended to be Linux-first, filesystem-native, IO-bound, and close to the kernel:

- the filesystem is the namespace/index authority
- normal listing is zero-decode
- committed object versions are immutable `.ndx` files
- folders are real directories
- trash and recover are directory movement
- blob/shard/parity layout remains the existing N1 blob plane
- repair, redistribution, GC, and health are fragment-scoped by default, with global admin routes retained

N1 Edge is not considered release-proven until the real hardware test matrix in this document passes.

## Identity

Runtime crate:

```text
n1-edge
```

Binary:

```text
litiaina-n1-edge
```

N1 Edge public/operator language should describe the index as:

- N1 Edge index
- filesystem-native index
- `.ndx` metadata records
- per-fragment Edge index
- N1 Edge storage kernel

Do not describe N1 Edge as:

- indexdb-backed
- sqlite-backed
- database-backed
- sharded database
- namespace database

Those terms belong to N1 Standard, not Edge.

## Disk Layout

N1 Edge keeps N1's index/blob separation, but the index root is no longer an indexdb directory.

Live Edge index layout:

```text
<index_root>/<fragment_hash>/
  docs/
    reports/
      @o_<object-key-hash>/
        <ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
```

Blob layout remains the N1 blob plane:

```text
<blob_root>/<fragment_hash>/blob/<version_id>/<shard-or-parity-file>
```

Edge does not move blobs for rename, trash, recover, or folder movement. GC reclaims blob bytes only after metadata reachability says the blob is headless.

## Namespace Model

Folders are real directories.

Object records are directories:

```text
@o_<object-key-hash>/
```

The object key hash is:

```text
sha256(canonical_object_key)
```

Example:

```text
object key: docs/video.mp4
index path: <index_root>/<fragment>/docs/@o_<sha256("docs/video.mp4")>/
```

Reserved user path segments are rejected:

```text
@o_
.trash
.uploads
.tmp
.locks
.gc
.repair
```

This keeps user namespace separate from Edge's internal filesystem records.

## Version Records

Each committed object version is one immutable `.ndx` file:

```text
<ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
```

The `.ndx` filename is the zero-decode list summary.

The `.ndx` body contains the full immutable object metadata needed by download, repair, verify, recover-on-read, redistribution, and GC.

Head/latest is:

```text
max ULID among valid .ndx filenames under @o_<object-key-hash>/
```

There is no:

- `.head`
- mutable head pointer
- object head table
- version counter
- database state for latest
- cache required for correctness

## Listing

Normal LIST is zero-decode.

For a prefix such as:

```text
docs/
```

Edge opens exactly:

```text
<index_root>/<fragment>/docs/
```

Then it reads direct children:

- normal directory -> directory entry
- `@o_...` directory -> inspect `.ndx` filenames only, choose greatest ULID, parse size/time/extension from the filename

LIST must not decode `.ndx` bodies.

LIST must not recursively scan the whole fragment and filter afterward.

LIST must not synthesize folders from key prefixes as the core model. Folders are directories.

## Read And Download

GET/download is where `.ndx` body decoding is correct.

Latest open:

```text
canonicalize object key
hash object key
open @o_<hash>/
choose max ULID .ndx filename
decode selected .ndx body
read through the existing N1 blob/shard/parity plane
```

Exact version open:

```text
canonicalize object key
hash object key
open @o_<hash>/
find .ndx whose filename begins with requested ULID
decode selected .ndx body
read through the existing N1 blob/shard/parity plane
```

## Upload

One-shot upload keeps the existing N1 blob write path.

Commit point:

```text
final .ndx exists under live @o_<hash>/
```

Finalize discipline:

```text
write temp .ndx
fsync temp file
rename temp -> final .ndx
fsync parent directory
```

The final committed `.ndx` record makes the object visible in listing.

## Multipart Upload

Incomplete multipart state stays outside the live namespace:

```text
<index_root>/<fragment>/.uploads/<upload_id>/
```

Receipts are upload-session state only. Receipts are not committed object truth.

N1 Edge receipt records are intentionally tiny and filename-authoritative:

```text
shard_p<part-number>_s<shard-index>.<crc32>.receipt
part_p<part-number>.<crc32>.receipt
```

Finalize reads the upload manifest/receipts, rebuilds final object metadata, writes the final `.ndx`, and atomically publishes into live `@o_<hash>/`.

Live listing never sees incomplete multipart state.

## Trash, Recover, And Purge

Trash is filesystem movement.

Soft delete object:

```text
mv <index_root>/<fragment>/docs/@o_<hash> \
   <index_root>/<fragment>/.trash/docs/@o_<hash>
```

Recover object:

```text
mv <index_root>/<fragment>/.trash/docs/@o_<hash> \
   <index_root>/<fragment>/docs/@o_<hash>
```

Soft delete folder:

```text
mv <index_root>/<fragment>/docs \
   <index_root>/<fragment>/.trash/docs
```

Recover folder:

```text
mv <index_root>/<fragment>/.trash/docs \
   <index_root>/<fragment>/docs
```

Hard delete removes the selected trash subtree. It does not move blobs.

GC later reclaims headless blobs after no committed `.ndx` under live or trash references them.

## GC

GC is reachability-driven.

GC may decode `.ndx` bodies because it needs full blob references.

GC protects blobs referenced by committed `.ndx` records under:

```text
<index_root>/<fragment>/
<index_root>/<fragment>/.trash/
```

GC may reclaim blobs that no longer have committed metadata reachability.

GC must support:

- one fragment
- all fragments for admin/CLI

Fragment route:

```http
POST /n1/maintenance/fragments/{fragment}/gc/start
```

Global/admin route:

```http
POST /n1/maintenance/gc/start
```

## Repair

Repair is fragment-scoped by default because the fragment is the virtual partition and durability boundary.

Repair must restore:

- missing Edge index records on rebuilt index roots
- missing blob shards/parity where recoverable
- listing after disk loss/reformat
- download after repair

Fragment route:

```http
POST /n1/maintenance/fragments/{fragment}/repair/start
```

Global/admin route:

```http
POST /n1/maintenance/repair/start
```

## Redistribution

Redistribution is also fragment-scoped by default.

It may move blob placement when the physical disk set changes, but it must preserve immutable logical metadata identity.

Fragment route:

```http
POST /n1/maintenance/fragments/{fragment}/redistribution/start
```

Global/admin route:

```http
POST /n1/maintenance/redistribution/start
```

Required real-hardware redistribution proof:

```text
2 disks, 1+1 EC -> 1 disk, 1+0 EC -> 2 disks, 1+1 EC
```

After each stage:

- live listing matches expected object count
- sample downloads match source hashes
- repair status is clean
- health does not report missing index or blob replicas

## Health Monitoring

Health monitoring should be fragment-bound for normal UI/operator flows.

Fragment route:

```http
GET /n1/maintenance/fragments/{fragment}/health
```

Global/admin route:

```http
GET /n1/maintenance/health
```

Fragment health must avoid whole-server deep scans unless the caller explicitly requests global/admin health.

## Real Hardware Test Matrix

Run this before treating N1 Edge as release-ready.

Minimum hardware profile:

```text
index roots: /mnt/disk-00, /mnt/disk-01
blob roots:  /mnt/disk-00, /mnt/disk-01
```

The two disks intentionally house both index and blob roots. This is the minimum N1 Edge profile.

Required proof cases:

1. Fragment creation with quota.
2. One-shot upload of a real nested folder.
3. Multipart upload resume after restart.
4. LIST before restart.
5. LIST after restart.
6. Download sampled files and compare SHA-256.
7. POSIX mkdir, rename, folder rename, move folder to folder.
8. Soft delete file and folder, verify hidden from live listing.
9. Trash listing.
10. Recover by path and by trash entry ID where ambiguous.
11. Hard delete selected trash entries.
12. Empty trash.
13. GC after hard delete, verify only headless blobs are reclaimed.
14. Recover-on-read after wiping one shard/parity side.
15. Repair after real disk loss/reformat.
16. Redistribution 2 disk -> 1 disk -> 2 disk.
17. Bitrot detection and repair where recoverable.
18. Archive download of selected files/folders as TAR.
19. Parallel four-user small-object storm.
20. RAM, open file descriptors, and CPU remain bounded by configured runtime caps.

Expected pass evidence:

- live listing count is exact
- restart listing count is exact
- POSIX listing is exact
- trash listing is exact
- sample downloads hash-match source files
- no warning storm from repair/replica/GC paths
- GC never deletes a blob referenced by live or trash `.ndx`
- disk loss followed by repair restores both listing and reads
- health reports fragment-specific debt accurately
- global health is only used for admin/CLI aggregate views
- RSS does not grow with object count under parallel upload

## Testing Notes

Standard N1 proof scripts that inspect sharded `indexdb` files are not sufficient final evidence for Edge. Edge proof must inspect filesystem-native `.ndx` records instead of indexdb tables.

API-level storm tests can still be used against Edge because the public routes are compatible:

```bash
scripts/bench/n1_live_parallel_storm.py \
  --base-url https://127.0.0.1:50001 \
  --master-key <MASTER_KEY> \
  --users 4 \
  --objects-per-user 3000 \
  --per-user-concurrency 4 \
  --quota-bytes 21474836480 \
  --pid <N1_EDGE_PID>
```

Before tomorrow's rigorous test pass, build and run:

```bash
cargo build -p n1-edge --bin litiaina-n1-edge
RUSTFLAGS='-D warnings' cargo test -p n1-edge --all-targets
RUSTFLAGS='-D warnings' cargo check --workspace --all-targets
```

Then run real hardware tests against:

```text
/mnt/disk-00
/mnt/disk-01
```

Do not mark Edge stable from synthetic tests alone.

