# N1 Object Storage Architecture

This document defines the N1 3.1.0 development-engine storage contract. It is the reference for
object identity, namespace behavior, upload commit, repair, redistribution, and
garbage collection.

N1 3.1.0 uses engine format 3 and preserves N1's immutable-object and
namespace-mutation architecture while introducing the TTL trash lifecycle,
per-object EC-set authority, and autonomous repair.
It does not open, adopt, or migrate any earlier N1 engine format.

Version `3.1.0` identifies the current development package line; it does not
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
      .entries/
        <trash-entry-id> -> @o_<object-id>
      .records/
        <trash-entry-id>.ndx

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

Each soft delete receives a unique trash entry id. The lifecycle record stores
the original path, deletion timestamp, entry kind, and internal payload
location. This permits multiple retained generations of the same logical path
without overwriting an older recoverable deletion. Folder soft delete remains
a constant-sized directory rename; descendants are inspected only if the
folder reaches lifecycle expiry.

There is no manual empty-trash or hard-delete operation in N1 3.1.0. Recovery
is allowed until the configured trash retention deadline. Once that deadline
is reached, lifecycle owns the entry and recovery fails closed.

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

Incomplete upload state lives only in the index/control plane:

```text
.uploads/<version-id>/
  manifest.ndx
  binding.ndx
  part_p<part-index>.<crc32>.receipt
  shard_p<part-index>_s<shard-index>.<crc32>.receipt
```

Multipart parts stream through EC into temporary shard files. Receipts record
durable progress without retaining the object body in memory.

Finalize:

1. validates the upload binding and part receipts
2. validates or publishes stable shard/parity files
3. builds the immutable object `.ndx`
4. writes that `.ndx` to every shard-bearing blob root
5. publishes or updates the namespace symlink
6. retires upload-session state

The namespace becomes visible only after immutable object truth is durable.

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

Each accepted namespace mutation first advances a small, durable generation
record on every participating index replica. The generation is namespace-plane
authority metadata, not an object head and not part of immutable object state.
It lets `2 -> 1 -> 2` redistribution distinguish the continuously active
replica from a returning stale replica even when their link counts happen to
match or the stale replica contains more pre-deletion links. Generation reads
and writes are constant-size; namespace mutation does not hash or scan the
namespace.

Redistribution selects the highest generation as authoritative and copies only
the namespace/control plane. Replicas at the same generation must have the
same deterministic content digest; disagreement at the same generation fails
closed. Normal mutations are rejected while configured replicas expose
different generations, so no new write can erase the authority history before
redistribution reconciles it.

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

Autonomous repair does not invoke redistribution. Redistribution remains an
explicit operator request for intentionally rewriting otherwise valid objects
into the current EC or placement policy.

## Trash Lifecycle And Garbage Collection

The namespace is reachability truth. Normal deletion reclamation is a targeted
trash lifecycle operation, not a full object-plane scan:

1. scan lifecycle records under each fragment's `.trash`
2. retain entries younger than `trash_retention_seconds`
3. collect exact object ids only for an expired file or subtree
4. persist a crash-replayable reclamation receipt under `.gc/trash-lifecycle`
5. remove the expired namespace entry and its lifecycle record
6. verify candidate object ids are absent from both live and remaining trash namespace
7. directly reclaim only those unreferenced immutable object directories
8. remove the receipt after reclamation completes

An interrupted expiry retries from the durable receipt and never needs to
rediscover headless blobs through a global scan. Earlier-engine trash entries
are not adopted: N1 3.1.0 requires freshly initialized format-3 fragments and
fails closed when an earlier or unversioned fragment marker is encountered.

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
- repair, GC, and redistribution heartbeat during scans and rewrites
- per-class latency, queue depth, failure, slow-path, and stall telemetry is
  bounded in memory

IO control is not a second maintenance coordinator. Repair, redistribution,
and GC retain their existing stop signals and durability rules. N1 does not
inherit N2 peer quorum, peer routing, or cluster job authority.

Multipart encoding remains streaming. Shard writes and final flushes run in
parallel within the admitted request budget. N1 does not buffer whole objects
for EC.

## Transport Faces

N1 exposes:

- `/n1/...` over HTTPS for universal clients
- `n1://` for N1-owned low-level tools and trusted services

Both call the same N1 core. The native protocol does not bridge through Axum
and does not own separate storage semantics.

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

N1 3.1.0 is correct only while all of these remain true:

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
13. Delete is always a namespace soft delete with a unique trash entry id.
14. Recovery is permitted only before the configured trash TTL expires.
15. No public HTTP, native, POSIX, shell, or console operation can empty or purge trash early.
16. Normal TTL reclamation scans `.trash`, not the complete immutable object plane.
17. Namespace removal precedes physical reclamation, and a candidate object is deleted only when no live or trash reference remains.
18. Interrupted TTL reclamation is replayable from a durable receipt.
19. Mutating operations for one fragment are serialized across rename, trash, recovery, finalize, mkdir, and TTL sweep boundaries.
20. Recovery never removes a lifecycle record unless at least one retained payload placement was moved back into the live namespace.
21. Multi-root namespace publication and rename preflight all roots and roll back publications completed by a failed operation.
22. Every namespace mutation rejects a symlink or non-directory in its parent chain; a logical path cannot escape its physical fragment root.
23. Namespace rename durability includes syncing both source and destination parent directories after forward and rollback renames.
24. Recursive continuation tokens filter object entries but never prune a directory whose descendants may sort after the token.
25. Path-based trash lookup and recovery select the same newest generation by deletion time and trash entry id.
26. Replica health compares deterministic namespace/control content in addition to record counts. Replicas with the same authority generation but different content fail closed instead of being destructively auto-repaired.
27. Every accepted namespace mutation advances a constant-size durable generation record on all participating index replicas before changing namespace content.
28. Redistribution selects a higher namespace generation over link counts or filesystem timestamps, including when the newer replica contains fewer links because of deletion or TTL expiry.
29. Configured replicas with different namespace generations reject new namespace mutations until redistribution reconciles them.
30. Namespace generations are excluded from content hashing, and generation maintenance never scans the namespace on the mutation path.
31. Namespace redistribution and repair share the per-fragment mutation boundary; they cannot copy a replica concurrently with rename, trash, recovery, finalize, mkdir, or TTL sweep.
32. Each object version carries its own fixed EC set; configuration changes are prospective and never invalidate a healthy recorded layout.
33. Adding a blob root neither marks old objects degraded nor triggers implicit redistribution.
34. New uploads require all `D + P` targets on distinct, registered physical devices; N1 never commits an under-protected new version.
35. A configured path is writable only when its on-disk root identity matches the durable local topology registry; it is never rebound while it remains configured, and configuration removal is the explicit decommission boundary.
36. Repair scans fragments serially, bound object concurrency, and process an object's missing shards one at a time.
37. A repair relocation publishes a higher placement generation only after the reconstructed shard is durable; stale lower-generation metadata cannot override it.
38. Equal placement generations with different immutable metadata fail closed.
39. Automatic availability and checksum scans report unresolved repair debt as failure and keep user mutations read-only until a successful retry.
40. Recover-on-read may repair a recorded live root in the background, but only the scheduled healer may relocate a shard and change placement metadata.
41. Automatic healing never performs or requires redistribution of a healthy recorded object layout.
