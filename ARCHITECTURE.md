# N1 architecture

N1 4.0.0 is a development single-node object store, not a filesystem or a
distributed cluster. Storage format 4 has no automatic migration from older
development formats. API details are in [DOCUMENTATION.md](DOCUMENTATION.md).

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

Pool ID, generation, geometry and canonical member order are immutable after
enrollment. Members must be independent mounted devices. Root identities and
.n1-pool.json descriptors reject missing, replaced or incompatible roots.
Removing or editing an enrolled pool is not a member-replacement procedure.

fs_storage_path holds local topology/control state, including
.n1/topology-roots.json. It is not a namespace database, object index or global
object writer. NCX, NDX and payload live together on storage-pool members.

## Stored representation

Fragments are authentication scopes. Their public aliases may change; physical
fragment identity and object identity remain stable.

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
Export enumeration is a separate linear walk. Maintenance reachability snapshots
are not claimed constant-memory at arbitrary object counts.

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

Topology/identity checks block unsafe mutations. Reads may reconstruct within
the recorded parity tolerance, but missing members do not authorize silently
rebinding or rewriting an enrolled pool. Repair uses the object's original
geometry; a newer pool is not an automatic repair/rebalance destination.

GC/TTL removes expired trash and safely unreferenced immutable versions.
Active uploads, overlapping scans and retained exports must remain protected;
uncertain authority causes preservation/reporting, not guessed deletion.

Backup captures every pool member offline under exclusive leases. Restore
preserves logical identities and uses startup fences until complete. Config,
secrets and application databases require separate backup. Accel can be rebuilt
from N1 and is never storage truth. See
[backup instructions](DOCUMENTATION.md#backup-and-restore) and
[measured validation](TESTING.md).
