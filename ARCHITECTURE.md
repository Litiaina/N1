# N1 Object Storage Architecture

This document defines the N1 3.5.1 development-engine storage contract. It is the reference for
object identity, namespace behavior, upload commit, repair, redistribution, and
garbage collection.

N1 3.5.1 uses engine format 3 and preserves N1's immutable-object and
namespace-mutation architecture while introducing the TTL trash lifecycle,
per-object EC-set authority, and autonomous repair.
It does not open, adopt, or migrate any earlier N1 engine format.

Version `3.5.1` identifies the current development package line; it does not
declare stable-release status. Engine format `3` is the independent on-disk
compatibility boundary defined by this document.

## Core Rule

N1 separates mutable namespace/control state from immutable object truth:

```text
index roots = namespace and control
blob roots  = immutable object metadata and EC payload
```

The namespace points to stable object identities. It never points to a shard,
parity file, version path, or mutable head record.

Each committed object version stores its authoritative `.ndx` record beside its
shard or parity payload on every shard-bearing blob root. Object metadata
durability therefore follows the recorded EC placement instead of consuming
the index plane.

## Storage Roles

### Index Root

An index root stores mutable fragment aliases, fragment control state, upload
receipts, trash, and the user namespace:

```text
<index_root>/
  .n1-root-id
  .fragment/
    <fragment-alias-hash> -> ../@f_<stable-fragment-id>

  @f_<stable-fragment-id>/
    .fragment.ndx
    .uploads/
      <version-id>/
        manifest.ndx
        binding.ndx
        part_p00000000000000000000.<crc32>.receipt
        shard_p00000000000000000000_s0000.<crc32>.receipt
    .trash/
      <unix-nanos>.d<path-depth>/
        docs/
          deleted-video.mp4 -> @o_<object-id>

    docs/
      video.mp4 -> @o_<object-id>
```

An index-only root must not contain `.objects` or object payload.

### Blob Root

A blob root stores immutable object records and their EC payload:

```text
<blob_root>/
  .n1-root-id
  @f_<stable-fragment-id>/
    .objects/
      ab/
        cd/
          @o_<object-id>/
            <version-ulid>.<size>.<unix-nanos>.<packed-ext>.ndx
            <version-ulid>/
              part_0_shard_0.shard
              part_0_shard_1.shard
              part_0_shard_2.parity
```

A blob-only root must not contain:

- `.fragment/`
- `.fragment.ndx`
- `.uploads/`
- `.trash/`
- user namespace paths

Only roots that hold a shard or parity file for a version hold that version's
`.ndx`. The `.ndx` and payload placement form one immutable object-version
record.

### Colocated Root

The same physical disk may be configured as both an index root and a blob root.
This is the minimum and low-disk-count deployment model:

```text
<root>/
  .fragment/
  @f_<stable-fragment-id>/
    .fragment.ndx
    .uploads/
    .trash/
    <user namespace>
    .objects/
```

Colocation combines disk roles, not ownership rules. Namespace/control code
does not own `.objects`, and object-placement code does not own namespace
state.

## Stable Fragment Identity

The public fragment alias is mutable:

```text
.fragment/<alias-hash> -> ../@f_<stable-fragment-id>
```

The `@f_<stable-fragment-id>` identity is immutable. Authentication may begin
with the alias hash, but core operations resolve it to the stable fragment
identity before touching object or namespace state.

`.fragment.ndx` is a control record stored only on index roots. It records the
stable fragment identity, state, timestamps, key
material metadata, and namespace partition count.

Fragment rename changes only the index-plane alias and mutable display
identity. It does not move or rewrite:

- object `.ndx` records
- shard or parity files
- object ids
- upload payload
- namespace contents

## Namespace

Folders are real directories. Files are symlinks to stable object ids:

```text
docs/video.mp4 -> @o_<object-id>
```

The symlink target is an identity token, not a relative payload path.

Namespace operations are therefore lightweight:

- file rename changes a symlink name
- folder rename changes a directory name
- move changes namespace placement
- soft delete moves namespace entries into `.trash`
- recover moves entries from `.trash` back to the live namespace

These operations do not rewrite object metadata or EC payload.

Each soft delete retains the same logical path below a unique timestamp
generation: `.trash/<unix-nanos>.d<path-depth>/<logical-path>`. The timestamp
is the TTL authority and the path-depth suffix identifies the renamed anchor
without a side record. Repeated deletion of one logical path creates another
generation instead of colliding or overwriting recoverable data. Folder soft
delete remains a constant-sized directory rename; descendants are inspected
only if the folder reaches lifecycle expiry.

There is no manual empty-trash or hard-delete operation in N1 3.2.0. Recovery
is allowed while the physical trash entry exists. At or after the configured
retention deadline, the next lifecycle pass may reclaim it.

Reserved internal names are rejected in user paths:

```text
.fragment
.objects
.uploads
.trash
.tmp
.locks
.gc
.repair
.intents
.trash-jobs
@o_*
@f_*
```

## Object Identity And Versions

Object directories use two-level fanout:

```text
.objects/<hh>/<hh>/@o_<object-id>/
```

Each committed version has:

```text
<ulid>.<size>.<unix-nanos>.<packed-ext>.ndx
<ulid>/
  <shard-or-parity-files>
```

The `.ndx` body stores object-version truth:

- object key at commit time
- stable fragment identity
- version and upload-session ids
- owner and content type
- logical size and timestamps
- recorded data/parity shard counts
- recorded stripe size
- placement generation
- parts
- absolute shard/parity paths
- checksums and shard roles

The object id, version id, logical bytes, checksum truth, ownership, and
content semantics are immutable. Repair or explicit redistribution may replace
only the physical topology projection (recorded shard paths and placement
generation) after equivalent shard bytes are durable. That maintenance update
does not create a content version or mutate the namespace.

There is no mutable head pointer. The head is the greatest valid version ULID
found across the object's healthy blob placements.

## Zero-Decode Summary

The `.ndx` filename is a listing summary:

```text
<ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
```

Listing resolves the namespace symlink to `@o_<object-id>`, finds the greatest
ULID filename across blob roots, and parses the filename fields. It does not
decode the `.ndx` body.

The packed extension is a `u64`:

- byte 0 stores extension length
- bytes 1 through 7 store lowercase ASCII extension bytes
- unsupported or longer extensions store `0`

Full `.ndx` decoding is reserved for reads, repair, redistribution, status,
recover-on-read, archive operations, and GC.

## Upload And Commit

N1 has two multipart contracts that share the same streaming EC writer and the
same immutable final object representation.

The standard high-throughput contract keeps its manifest and compact part
completion state in bounded process memory. Initialization performs no storage
write. Distinct parts independently claim one memory slot and stream through EC
to the fixed object placement in parallel, atomically publishing their stable
K+M shard names only after each complete part. There is no persistent binding
or receipt. Finalize verifies that every part slot is complete, publishes the
immutable `.ndx`, and then publishes the namespace entry. It deliberately
does not force every shard, metadata file, or parent directory to stable media;
that would turn concurrent finalization into a K+M-times-parts flush storm.
Restart discards unfinished sessions; their
unreferenced shard versions remain invisible and the TTL orphan scan reclaims
them. GC treats live in-memory version ids as active while the process exists.
Standard completion is atomic and immediately readable, but recently completed
standard writes may be lost by sudden power failure before the kernel flushes
them. Clients requiring power-loss acknowledgement use durable multipart or an
explicit strict one-shot upload.

The standard small-object batch endpoint amortizes authentication and HTTP
request overhead without combining object identities. Every multipart field
is named `object` and carries its percent-encoded logical key as its filename.
It consumes the request incrementally and keeps at most a globally bounded
number of one-megabyte-or-smaller objects in flight across all clients, and
sends every entry through the ordinary standard one-shot EC and namespace
commit boundary. Each result succeeds or fails
independently. There is no batch manifest, shared object, packed payload,
durable upload receipt, or batch-wide rollback contract.

The durable restart-resumable contract keeps incomplete state in the
index/control plane:

```text
.uploads/<version-id>/
  manifest.ndx
  binding.ndx
  part_p<part-index>.<crc32>.receipt
  shard_p<part-index>_s<shard-index>.<crc32>.receipt
```

Durable multipart parts stream through EC into temporary shard files. Receipts
record restart-survivable progress without retaining the object body in memory.
The established native `n1://` multipart operations continue to use this
durable contract.

Finalize:

1. validates the upload binding and part receipts
2. validates or publishes stable shard/parity files
3. builds the immutable object `.ndx`
4. writes that `.ndx` to every shard-bearing blob root
5. publishes or updates the namespace symlink
6. retires upload-session state

Neither contract makes the namespace visible before all K+M writes and
immutable object metadata are complete. The durable contract additionally
forces its data and publication boundary to stable media before acknowledgement.
Direct upload remains the bounded small-object path: one request body streams
directly through EC and performs the selected standard or strict publication
boundary.

No upload performs synchronous `statvfs` or free-space prediction. Such a
check races every concurrent writer and would add O(total roots) or O(K+M)
work to each object without guaranteeing capacity. The selected K+M writes are
the authority: a kernel `ENOSPC` or quota error aborts the unpublished attempt,
returns `507 Insufficient Storage`, and triggers immediate best-effort cleanup.
The TTL orphan collector is the crash fallback. Capacity remains asynchronously
observable through monitoring and the Console, never a foreground correctness
gate.

## Erasure Placement

N1 records EC topology per object version. For `3 + 1`, each part has three
data shards and one parity shard placed across four selected blob roots. Each
of those roots also stores the same immutable `.ndx` for that version.

The selected roots form one fixed object EC set: every part maps shard index
`i` to the same object root `i`. Selection uses deterministic rendezvous
ranking over all currently healthy registered blob roots, so adding disks
expands placement choices for new object versions without moving old ones.
N1 refuses an upload unless every `D + P` shard target opens successfully.

This provides:

- metadata survival with surviving shard placements
- self-describing blob placements
- no object-metadata capacity pressure on index-only disks
- independent recovery of missing metadata and missing payload

Absolute shard paths remain recorded because they are direct storage truth on
one machine. Stable fragment ids ensure fragment rename cannot invalidate
those paths.

Each configured storage root has a persistent `.n1-root-id`. The expected
path-to-identity registry is atomically stored under
`<fs_storage_path>/.n1/topology-roots.json`. A known configured path is never
rebound to a new identity automatically. Explicitly removing a path from the
configuration decommissions its registry entry; re-adding it on a later start
is the controlled enrollment boundary for a replacement disk. This prevents an unmounted disk's underlying
directory, or a replacement disk at the same path, from being mistaken for
the recorded failure domain.

## Topology Changes

Changing configured disks or EC values does not rewrite existing objects.

- old objects remain governed by their recorded topology
- new objects use the current configured topology
- health compares each version against its recorded layout
- redistribution is explicit

Adding a disk is not degradation and does not create redistribution debt.
Changing `D + P` is prospective: it changes new object versions only. An old
version remains healthy while its own recorded shard set is healthy. A disk
loss may cause automatic repair to relocate one missing shard to a healthy,
distinct configured root; that changes only the affected object's placement
metadata and increments its placement generation.

Object-level redistribution rewrites one selected version into the current
topology. Fragment redistribution scans that fragment and rewrites only
objects that do not already match the requested target.

Redistribution preserves object id and namespace identity while creating new
payload placement and colocated `.ndx` copies transactionally. Old payload is
retired through GC staging after the new placement is committed.

## Repair

Repair has two independent responsibilities.

Namespace/control repair restores index replicas:

- fragment aliases
- `.fragment.ndx`
- upload/control state when applicable
- live namespace
- trash namespace

Foreground namespace mutations apply the same direct filesystem operation to
every accepted index root. Root-identity and topology safety prevent writes
when a configured root is missing or has been replaced, so one configured
namespace replica cannot continue advancing alone. There is no namespace
authority generation, mutation journal, or lifecycle record in this path.

Maintenance hashes sorted namespace/control content only when it inspects or
repairs replicas. It can restore a missing or incomplete replica from the
available filesystem state. If equally complete replicas reference different
immutable objects, destructive automatic repair fails closed instead of
guessing. This maintenance work stays outside ordinary mkdir, rename, move,
trash, and recover requests.

It must never copy, delete, merge, or rewrite blob-plane `.objects`.

Object repair uses the immutable `.ndx` and surviving shards to restore:

- missing or corrupt shard/parity files
- missing `.ndx` copies on shard-bearing placements
- readable object state after disk loss

Recover-on-read uses the same recorded object topology and repair primitives.

N1 runs a lightweight topology controller and two configurable repair cycles:

- availability scans verify recorded shard existence and size
- checksum scrubs verify immutable payload integrity

Fragments are scanned serially. Object concurrency is bounded and defaults to
one; shards for one object are examined and repaired one at a time. A repaired
shard is first made durable, then the higher placement generation is published
to every new shard-bearing root. Readers select the highest generation and
fail closed if equal generations disagree.

Recover-on-read serves successfully reconstructed data first and schedules an
in-place background repair. If the recorded root is unavailable, relocation is
left to the scheduled healer because only that path may atomically update the
object's placement generation.

The controller keeps mutations read-only until the configured EC width can be
satisfied by distinct writable, correctly identified devices and at least one
writable index root. A stable healthy interval clears topology read-only mode
automatically. An unresolved automatic repair keeps N1 read-only and remains
visible in repair and health status; it is never converted into a false
successful scan.

Healthy scheduled availability and checksum cycles run online and preserve
foreground reads and mutations. This is safe because immutable object versions
are re-read and repaired under an object-operation lock, namespace mutations
remain atomic, and placement metadata is published atomically.
Operator-requested checksum remains read-only by default, strict repair is
read-only, and scrub is quiesced. Only unsafe topology, root-identity failure,
or unresolved automatic repair debt activates the persistent safety gate.

Autonomous repair does not invoke redistribution. Redistribution remains an
explicit operator request for intentionally rewriting otherwise valid objects
into the current EC or placement policy.

## Trash Lifecycle And Garbage Collection

The namespace is reachability truth. Normal deletion reclamation is a targeted
trash lifecycle operation, not a full object-plane scan:

1. scan the timestamp generation directories under each fragment's `.trash`
2. read deletion time directly from each generation name
3. retain entries younger than `trash_retention_seconds`
4. collect exact object ids only for an expired file or expired subtree
5. remove the expired namespace entry on each index replica
6. verify all candidates once against the remaining live and trash namespace
7. directly reclaim only unreferenced immutable object directories

There are no lifecycle record files, receipts, databases, or temporary states;
the generation directory and retained namespace entry are the complete state.
If the process stops after namespace reclamation but before object
reclamation, immutable data can only leak safely as an orphan; explicit deep GC
detects and reclaims it. Earlier-engine trash entries are not adopted: N1
requires freshly initialized format-3 fragments and fails closed when an
earlier or unversioned fragment marker is encountered.

Full GC remains an explicit operator maintenance tool for orphan detection,
abandoned upload state, and scrub work. A manual GC run cannot bypass trash
retention or permanently delete a still-recoverable entry.

Redistribution may temporarily stage old payload as `.gc` files with
compatibility symlinks. Those entries are object-plane retirement state, not
namespace entries.

## IO Control

N1 applies bounded admission above disk-heavy work:

- foreground reads
- foreground writes
- background repair
- background GC
- background redistribution

Foreground concurrency adapts to available memory, CPU pressure, disk count,
configured EC width, and observed no-progress stalls. Healthy full utilization
is not treated as a reason to throttle. Background lanes are smaller and
fragment-scoped so maintenance cannot consume the complete live IO path.

Scheduler admission, progress tracking, and execution remain separate:

- foreground queue admission is bounded and returns `503` when the configured
  wait expires
- admitted operations have no arbitrary whole-operation deadline
- upload/download streams record logical byte progress
- repair, GC, redistribution, and bulk-export construction heartbeat during
  scans and rewrites
- per-class latency, queue depth, failure, slow-path, and stall telemetry is
  bounded in memory

IO control is not a second maintenance coordinator. Repair, redistribution,
and GC retain their existing stop signals and durability rules. N1 does not
inherit N2 peer quorum, peer routing, or cluster job authority.

Multipart encoding remains streaming. Shard writes and final flushes run in
parallel within the admitted request budget. N1 does not buffer whole objects
for EC.

## Bulk Export

Bulk export is an asynchronous projection of committed immutable object data;
it is not a new authoritative object representation. The export manager stores
durable job state and a version-pinned manifest below the configured filesystem
state root:

```text
<fs_storage_path>/.n1-exports/<export-ulid>/
  state.json
  manifest.ndjson
```

Manifest capture traverses one healthy strict namespace replica once, validates
every selected live entry, and records its logical archive path, stable object
identity, exact version ULID, and byte size. A missing namespace or object
record fails the complete job; N1 never silently publishes a partial export.
Once the manifest is durable, restart recovery rebuilds from that same pinned
version set rather than observing a newer namespace view.

Archive bytes are generated through bounded object-read prefetch and written as
fixed-size chunks across configured blob roots:

```text
<blob_root>/.n1-export-artifacts/<export-ulid>/chunk-<index>.bin
```

Manifest capture and archive construction hold a fragment-scoped background IO
admission for the complete job and report progress through the shared disk
scheduler. They therefore remain bounded beside repair, GC, and redistribution
instead of creating an unaccounted IO path. Delivery of a ready artifact holds
foreground-read admission for the complete HTTP body stream, including resumed
range transfers, and releases it only after the requested bytes finish.

Each chunk is synchronized and atomically renamed before the job becomes
`ready`. The ready state contains the logical chunk order, exact content length,
and whole-artifact SHA-256. HTTP download maps a single byte range onto only the
required chunks and returns stable `Content-Length`, `ETag`, `Accept-Ranges`,
and `Content-Range` headers. A process interruption before readiness exposes no
downloadable partial archive. An interrupted build discards partial chunks and
is regenerated after restart.

Export chunks are retained conveniences, not EC-protected user objects. A
missing or truncated chunk fails closed and the client creates a replacement
export from authoritative N1 data. Cancellation and configured retention remove
manifest, state, and artifact chunks. Retention capacity may be bounded by
configuration; when its limit is zero, actual kernel writes remain authoritative.

## Transport Faces

N1 exposes:

- NOA v1 at `/noa/v1/...` over HTTPS for universal clients
- `n1://` for N1-owned low-level tools and trusted services

Both call the same N1 core. The native protocol does not bridge through Axum
and does not own separate storage semantics.

NOA is an external transport contract only. N1 3.2.0 changes the HTTPS and
WebSocket namespace from the former engine-branded paths to `/noa/v1` without
changing storage metadata, namespace authority, lifecycle, EC placement, or
maintenance behavior. The complete route contract is defined in `NOA.md`.

Native data operations use a small MessagePack control frame followed by raw
body bytes. Upload bodies stream into the same bounded upload core, and
downloads stream raw bytes after the response control frame.

The native face must not bypass:

- fragment authentication
- namespace rules
- upload accounting
- maintenance gates
- EC validation
- repair, redistribution, or GC invariants

## Locked Invariants

N1 3.5.1 is correct only while all of these remain true:

1. Index roots own namespace/control state only.
2. Blob roots own immutable object metadata and payload.
3. A version `.ndx` exists only on shard-bearing blob roots.
4. Namespace files point only to stable `@o_` identities.
5. Fragment aliases resolve to immutable `@f_` identities.
6. Head is max ULID; no mutable head record exists.
7. Listing remains zero-decode.
8. Object versions retain their recorded EC topology until explicit redistribution.
9. Namespace repair never mutates `.objects`.
10. Fragment rename never rewrites object metadata or payload.
11. GC derives reachability from live and trash namespace state.
12. Upload and download remain streaming and memory-bounded.
13. Delete reserves one timestamp generation and performs exactly one live-path to `.trash/<generation>/<same-logical-path>` rename per participating index root.
14. Recovery is the reverse rename for one exact `trash_entry_id`; an occupied live destination returns `409 Conflict` and leaves trash unchanged.
15. No public HTTP, native, POSIX, shell, or console operation can empty or purge trash early.
16. Normal TTL reclamation scans `.trash`, not the complete immutable object plane.
17. Namespace removal precedes physical reclamation, and a candidate object is deleted only when no live or trash reference remains.
18. Interrupted TTL reclamation is fail-safe: it may leave immutable orphan data, never delete reachable data; deep GC reclaims such leaks.
19. Ordinary namespace mutations use no fragment-wide or striped userspace mutation lock; the filesystem namespace is the mutation boundary.
20. Recovery reads no object metadata and reports success only after at least one physical trash path moved back to live.
21. Destination conflicts are checked on all participating roots before publication; ordinary live rename remains one atomic filesystem rename per participating index root.
22. User paths are normalized and reserved internal path segments or parent traversal are rejected.
23. Ordinary mkdir, rename, move, trash, and recover do not add synchronous record files, databases, journals, or parent-directory fsync barriers; XFS/POSIX namespace semantics remain the foreground durability model.
24. Recursive continuation tokens filter object entries but never prune a directory whose descendants may sort after the token.
25. Every delete owns a unique timestamp generation; repeated file and directory deletion at the same logical path remains independently recoverable.
26. Replica health compares deterministic namespace/control content in addition to record counts.
27. Namespace mutations do not write or consult an authority-generation record and cannot be blocked by generation disagreement.
28. Equal-size replicas that reference the same object identities may be reconciled from the latest filesystem namespace mutation; different object-identity sets fail closed instead of being destructively repaired.
29. Namespace inspection and repair remain maintenance work outside the foreground mutation path.
30. Namespace authority-generation files do not exist and are never read or written.
31. Folder rename cost is proportional only to configured index-root count and is independent of descendant count; it never walks or rewrites the subtree.
32. Each object version carries its own fixed EC set; configuration changes are prospective and never invalidate a healthy recorded layout.
33. Adding a blob root neither marks old objects degraded nor triggers implicit redistribution.
34. New uploads require all `D + P` targets on distinct, registered physical devices; N1 never commits an under-protected new version.
35. A configured path is writable only when its on-disk root identity matches the durable local topology registry; it is never rebound while it remains configured, and configuration removal is the explicit decommission boundary.
36. Repair scans fragments serially, bound object concurrency, and process an object's missing shards one at a time.
37. A repair relocation publishes a higher placement generation only after the reconstructed shard is durable; stale lower-generation metadata cannot override it.
38. Equal placement generations with different immutable metadata fail closed.
39. Healthy automatic availability and checksum scans preserve foreground availability; unresolved repair debt is reported as failure and keeps user mutations read-only until a successful retry.
40. Recover-on-read may repair a recorded live root in the background, but only the scheduled healer may relocate a shard and change placement metadata.
41. Automatic healing never performs or requires redistribution of a healthy recorded object layout.
42. Upload correctness relies on actual K+M kernel writes, not synchronous free-space prediction; no upload hot path scans total configured roots for capacity.
43. Kernel capacity exhaustion publishes neither immutable object metadata nor a namespace entry and is reported as `507 Insufficient Storage` over HTTP and native transports.
44. Standard batch upload preserves one independent EC set and commit boundary per object; durable multipart has no batch variant.
45. A ready bulk export is derived only from one durable manifest of exact immutable object versions; later namespace mutations cannot change it.
46. Bulk export fails as one job when any selected namespace entry, object metadata record, or payload is unavailable; silent archive omission is forbidden.
47. Partial export artifacts are never downloadable, and restart recovery removes them before rebuilding the same pinned manifest.
48. Export artifacts are retained non-authoritative projections; their loss cannot mutate object, namespace, EC, repair, or lifecycle truth.
49. A mapped export entry pins an immutable source version and validated byte range independently from its archive path; logical business-layer names never alter N1 object identity.
50. Multiple mapped entries may read disjoint ranges from one immutable packed object, while duplicate archive paths and out-of-bounds ranges fail the export before publication.
