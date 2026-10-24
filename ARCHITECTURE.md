# N1 architecture

N1 1.0.0-RC.1 is a single-node object store, not a filesystem or a distributed
cluster. Its storage-format specification is also 1.0.0-RC.1. This formal naming
reset retains the tested encoding identifier 4 and existing NCX/NDX/payload,
pool and backup representations; it is not a data migration. Unsupported older
development formats are not migrated automatically. API details and versioning
rules are in [DOCUMENTATION.md](DOCUMENTATION.md).

## Storage pools

Each named pool owns its members and immutable K+P erasure-code layout:

```text
Pool A: 3 data + 1 parity → four mounted members
Pool B: 4 data + 2 parity → six different mounted members
```

Expand by appending a complete pool. Existing objects keep their pool, geometry,
stripe size and placements; expansion creates no mandatory redistribution debt.
New allocation selects the newest complete, unsealed pool, not a free-space
balancer. Sealing stops new allocation without retiring stored objects.

Pool ID, generation, geometry and logical shard slots remain fixed after
enrollment. Each .n1-pool.json identifies its member and accepted pool. Startup
reads these small descriptors and binds configured candidate locations by slot,
not path or discovery order. Mount paths may change; member identity does not.
Members must remain independent mounted devices. Unknown/conflicting identities,
duplicate slots and unfinished maintenance are rejected. Missing slots are never
renumbered or initialized automatically.

Root identity is a software enrollment guard, not a disk serial number. Offline
cloning preserves a healthy member's bytes and logical identity for verified
cutover at the same or a new location. Explicit replacement reconstructs only
the missing member's pieces at its configured location;
full inventory/CRC verification precedes identity restoration and activation.
An interrupted operation remains fenced and requires matching explicit resume.
These administrative fences do not add journals, tombstones or synchronization
to ordinary NCX mutations. Without parity, a lost member requires a backup.

fs_storage_path holds local topology/control state, including
.n1/topology-roots.json and the accepted logical pool membership. It is not a
namespace database, object index or global object writer. NCX, NDX and payload
live together on storage-pool members.

New shard/session addresses name pool, generation, slot and a member-relative
path. One resolver uses the startup-bound roots for reads, writes and maintenance.
Pre-portability absolute addresses resolve through each slot's original prefix;
relocation does not rewrite committed NDX or payload or grow an alias history.
Locations remain fixed during a running engine; cutover is offline.
Existing path-bound v4 members first acquire descriptors at their old locations.
Identity/topology readiness is not a full content-integrity claim: health gates,
verified recover-on-read and explicit content scans have separate roles.
A valid marker alone does not prove a complete or fresh copy.

## Stored representation

Fragments are authentication scopes. Their public aliases may change; physical
fragment identity and object identity remain stable.

A fragment has no preallocated, pool-local size. On pool expansion, startup
enrolls its small identity/credential record on the new members. New object
versions use the current write pool; existing objects/versions remain in their
original pools. Listing resolves the same fragment across all pools. Adding
capacity therefore extends where that fragment can store new data, without
moving old data. Allocation is not automatic free-space balancing: switch to a
new complete pool before the selected pool fills, and retain metadata headroom
for NCX operations in old pools.

On-demand pool diagnostics are independent of the global health summary.
Readiness checks logical member descriptors, fences, independent filesystems,
fragment metadata replicas and current capacity. Explicit live integrity scans
reuse the offline NCX/NDX/receipt/piece verifier, scoped to the caller's fragment.
They do not repair or cache data and are not atomic snapshots of concurrent
mutations. A single bounded diagnostic worker does not coordinate object writes;
quiesced offline verification remains the complete maintenance gate.

```text
<pool-member>/
  .n1-pool.json
  .fragment/<alias> -> ../@f_<stable-fragment-id>
  @f_<stable-fragment-id>/
    .fragment.ndx
    .namespace/<encoded-key>.ncx
    .trash/<time>.d<depth>.<nonce>/<encoded-key>.ncx
    .uploads/
    .objects/<fanout>/@o_<object-id>/
      <version-summary>.ndx
      <version-id>/<data-and-parity-pieces>
```

- NCX is a small checksummed reference containing format, pool ID and object ID.
  Each key has P+1 deterministic holders in its owning pool.
- NDX describes an immutable version and its recorded EC placement. It is
  colocated with the K+P payload placements, not held in a separate index pool.
- Committed NDX and payload are immutable during normal namespace operations.
  Repair and explicit maintenance have separate write/reclamation authority.
- Versions are identified by ULID; latest means the maximum valid committed
  ULID. No mutable version-head pointer exists.

ECS Core performs bounded-memory encoding, decoding, checksums and recovery.
N1 owns paths, placement, synchronization, publication and metadata. Its codec
dependency remains pinned; the namespace revision does not change EC math.

## Flat keys and listing

a and a/child may coexist. Prefixes are virtual groupings, not stored empty
folders. A trailing slash selects prefix-only behavior when a key and prefix
share a name. Encoded key components prevent suffix and physical-path collisions.

Live LIST reads entry names/types without decoding NCX, NDX or payload. Page
memory is bounded by page size and traversal depth. Sorted continuation pages
can rescan matching directories: listing is not an O(1) seek or a snapshot.
Export enumeration is a separate linear walk. Its cursor visits a sorted copy
of the roots to match lexical replica suppression; this does not reorder pool
members, omit replica traversal or add a whole-namespace deduplication cache.
Maintenance reachability snapshots
are not claimed constant-memory at arbitrary object counts.

Repair, monitoring and redistribution resume an internal directory cursor across
bounded metadata batches rather than restarting sorted LIST for every batch.
Replica selection uses the existing holders; no object catalog or cache is added.
Filesystem work runs outside async workers, and a paused cursor occupies no
blocking worker. Public LIST ordering and continuation semantics are unchanged.
Payload/NDX presence probes stream the existing two-level object fanout and stop
at their first match; they do not allocate an inventory of every object directory.

## Namespace mutation

Rename, move, trash and recovery use the same per-key sequence:

1. Copy unchanged NCX to every required destination holder.
2. Complete destination publication and its durability barriers.
3. Delete source NCX replicas and complete source-directory barriers.

Local copies can link an already committed NCX inode. Cross-filesystem copies
stage and synchronize only those small NCX bytes. Neither path opens or rewrites
object-version metadata or payload. There is no namespace tombstone, deletion
generation, mutation journal or background durability queue.

A failure before destination completion preserves the source. A process crash
during deletion may leave both names; data remains reachable, repair preserves
the destination and retry removes remaining sources. This is not an atomic
multi-key transaction. Prefix work reports conflicts/failures per child and
preserves unrelated destinations. Trash IDs distinguish repeated deletions;
they are not version-head or deletion-authority records.

Same-key publication/mutation is coordinated, including blocking workers after
caller cancellation. Different keys can progress independently; there is no
global namespace writer. All holder workers are joined before source deletion
or ownership release. Holder fanout is bounded at four, independent mutation/
recovery work at sixteen. Coordination and bounded admission are not a cache.

## Publication, repair and reclamation

Successful upload publication follows completion of the recorded EC set and
NDX, then NCX. Standard multipart progress is process-local until finalize;
durable multipart persists progress for restart/resume. Kernel ENOSPC/EDQUOT
failures return 507 rather than publishing a successful partial object.

Strict upload publication propagates shard-directory synchronization failures.
Standard multipart tracks each part's acknowledged durability in its existing
temporary session; all parts must be strict for strict NDX/NCX finalization.
The relaxed default does not promise power-loss-durable acknowledgement.
This changes upload error handling, not ordinary copy/delete mutation barriers.

Topology/identity checks block unsafe mutations. Reads may reconstruct within
the recorded parity tolerance, but missing members do not authorize silently
rebinding or rewriting an enrolled pool. Repair uses the object's original
geometry; a newer pool is not an automatic repair/rebalance destination.

Reads validate the existing whole-shard checksum before exposing bytes; the
streaming path does not bypass verification on a successful filesystem read.
Validated descriptors and rejected candidates are retained only for the current
request/part. Read-triggered repair uses the existing bounded repair capacity;
reconstruction is checksum-checked before replacement and directory-sync errors
propagate. Range recovery decodes a recorded EC stripe before slicing the
requested bytes; it never treats an arbitrary byte slice as a separately encoded
shard. A peer's failed validation does not suppress repair when that peer becomes
a read target. Read repair reuses whole-fragment lifecycle admission and checks
the fragment still exists before writing. It defers to pending deletion rather
than waiting behind it or recreating a deleted fragment. Healthy committed NDX
is not rewritten by this repair path.
Full availability/checksum walks and deep health inspection are explicit.
Periodic known-member safety probes do not enumerate namespace or payload.

Whole-fragment deletion takes exclusive per-fragment admission. Uploads and
repair share admission, so different uploads still run concurrently. Each
rechecks on-disk existence after admission; stale tokens or repair snapshots
cannot recreate a deleted fragment. Started HTTP upload workers retain admission
through publication/cleanup even after disconnect. Deletion errors propagate
and aliases remain available for retry until all members are removed.
Ordinary object/NCX mutations do not acquire this guard.

Independent NDX and NCX holders publish in bounded parallel phases with all
workers joined before proceeding. Ordinary strict uploads retain their direct-I/O
path and every data/publication barrier; small relaxed writes may use buffered I/O.
Strict small-file batches use request-local groups bounded to 64 objects / 8 MiB,
not a global writer. Payloads and private metadata are written in the existing
version directories. Pinned participating filesystem handles are opened before
writing; a group synchronization makes contents durable before NDX publication,
another precedes NCX publication, and a final one precedes strict ACK. Healthy
existing NCX is reused for a new immutable version. Sorted object guards precede
the existing namespace/backup mutation boundary, avoiding lock inversion. GC's
existing version pass removes expired private staging only when no upload owns
the object. Lock waits and payload I/O remain asynchronous; only synchronous
publication occupies a blocking worker, so admitted groups cannot exhaust the
workers needed to open their own files. No packing, new receipts, version heads
or disk format are introduced.
The Linux I/O backend uses a bounded in-process io_uring completion queue when
available. Submitted buffers and descriptors remain owned until completion,
including after caller cancellation. Writes must complete before data sync;
data sync precedes publication and the existing directory-sync barriers. Queue
submission alone is not durability. Backend selection happens at startup;
automatic compatibility fallback is reported, not a retry of failed storage I/O.
This adds no service, cache, storage representation or relaxed acknowledgement.
One driver thread owns the ring, with at most 128 queued and 128 active
requests; it does not busy-poll. Asynchronous direct and buffered shard writes,
exact/chunked checksum reads and file/directory syncs use this backend.
Synchronous publication workers perform their writes and durability syncs
directly through POSIX, avoiding a second queue-and-wait handoff. Opens and
renames also retain their blocking helpers. This is a fixed operation policy,
reported separately as `publication_backend: posix`, not fallback after a failed
native request. Publication ordering and every required sync are unchanged.
Replicated NDX writes share one immutable encoded allocation, and checksum walks
reuse a bounded chunk buffer rather than allocating per chunk.
Batch buffering takes the existing shared I/O permit before allocating each
object body; request concurrency is not extra payload capacity. That permit,
lifecycle and activity protection remain held through NCX publication, including
after disconnect. This bounds the complete commit, not only payload/NDX work.
Admission failure preserves already-admitted results and reports explicit
per-object rejections. Batch result memory is bounded by a 4,096-object request
limit; these per-request bounds are not a universal process-memory guarantee.

Adaptive I/O capacity uses the existing FIFO semaphore. Reducing capacity retires
permits as admitted work finishes; increasing capacity returns those permits.
Waiters retain their queue position instead of cancelling and polling. Waiting
for foreground capacity does not occupy the shared background lane. The hard
configured concurrency bound and foreground admission deadline remain in force.

Scheduled and manual GC use the same collector. It expires trash NCX by TTL,
expires abandoned durable sessions and reclaims aged, inactive upload versions.
Trash TTL is checked before reading version NDX. Ordinary uploads have no new
cleanup receipt: restart discovery walks object/version directory names and
checks namespace references, upload activity and existing NDX authority. An
abandoned new version under a live object is eligible; committed history is not.

GC uses BackgroundGc admission, rejoining between groups of 128 objects/sessions
and fragments. The fanout cursor retains at most three open directories rather
than collecting every object directory. Trash expiration and the namespace
reference snapshot remain separate traversal phases; the reference set grows
with the namespace. This is not constant-memory or constant-time collection.
All configured members, not only the foreground-eligible subset, must establish
safe deletion authority. Unavailable identities, unreadable authority or unsafe
layout cause preservation/reporting. Existing upload/mutation fences protect
overlap; export admission is rechecked between destructive batches. Backup pins
remain protected. No payload relocation or new cleanup-record encoding is added.

Offline backup captures every pool member under exclusive leases, including
unfinished durable-upload progress. Online backup instead pins committed NCX,
version NDX and pieces inside the running engine. Shared mutation admission stays
concurrent; a bounded exclusive capture boundary freezes namespace/version
selection while hard links are made. Hashing/copying runs after that boundary.
No object-count memory cache, mutable head or namespace mutation journal is added.
GC and fragment deletion cannot remove pinned inodes; abandoned scratch views
are cleaned only after startup owns the member leases. Online recovery excludes
all unfinished uploads. Restore
maps by pool ID/generation/geometry/member slot, never mount-path sorting;
it preserves logical identities and uses startup fences until complete. Config,
secrets and application databases require separate backup. Accel can be rebuilt
from N1 and is never storage truth. See
[backup instructions](DOCUMENTATION.md#backup-and-restore) and
[measured validation](TESTING.md).

## Operational boundaries

Optional private Accel read delivery runs only after N1 authorization and
authoritative namespace/version resolution. Byte cache keys pin immutable object
identity/version; rename does not invalidate the underlying content. Cache-origin
requests bypass only the proxy, never authentication. Pre-header failure returns
to the normal N1 read path; mid-stream failure terminates that response. No cache
entries, mutable heads or new durable coordination are added to N1. Accepted HTTP
sockets use TCP_NODELAY, as the native transport already does.

Master-key authentication grants a signed operator capability for server-wide
maintenance. Fragment secrets cannot request another fragment or put the whole
server into restrictive maintenance. This is API authorization, not namespace
state, a new writer lock or distributed coordination. Key rotation invalidates
credential-bound refresh sessions; access tokens retain their normal expiry.

Native connections/control frames and multipart temporary metadata have bounded
admission. Pool warnings reuse capacity probes and report byte/inode headroom;
they add no upload free-space scan, placement reservation or object cache.
Replication copies authenticated, completed recovery points to optional
independent storage. It does not add a live second writer or distributed failover.
The receiver and operator API belong to N1's own Engine/Shell binaries. HTTP(S)
and native N1(S) carry the same bounded snapshot transfer; no SSH daemon/client
or external backup service is required. Offline operator work is separate from
the running storage engine and reuses repository leases, pins and restore fences.
