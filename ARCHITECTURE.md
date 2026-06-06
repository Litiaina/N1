# N2 Architecture

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

N2 owns distributed namespace, immutable object identity, per-version erasure
layout, node placement, peer transport, repair, redistribution, rebalance, and
cluster health. Every healthy node can receive client operations; there is no
permanent object-IO leader.

## Architectural Invariants

The N2 1.0 storage model is defined by these rules:

- a user path is mutable namespace, not object storage;
- a namespace file is a direct symlink to `@o_<object-id>`;
- `@o_<object-id>` and every committed `.ncdx` version are immutable;
- each `.ncdx` is colocated with that version's shard/parity data;
- `.ncdx` contains immutable object and placement truth, never a logical path;
- every shard-bearing participant stores the same named-MessagePack `.ncdx`;
- the head version is the greatest valid ULID filename in the object directory;
- object bytes and metadata are not cluster-consensus state;
- namespace mutation is partitioned quorum state by fragment and path;
- every version records the exact membership epoch, EC shape, and shard
  placement that wrote it;
- adding nodes does not invalidate or automatically rewrite old objects;
- no `.cluster`, `.namespace`, `.blobs`, `.nmtd`, `.ndx`, or mutable latest
  pointer exists in the accepted disk format.

These boundaries are not optional implementation details. They are the N2
storage contract.

## Bidirectional Nodes

Any healthy N2 node can coordinate an operation:

```text
client A -> node-a -> upload object-a
client B -> node-b -> upload object-b
client C -> node-c -> list, download, rename, delete, recover
```

The receiving node is coordinator only for that operation. It resolves the
current layout, distributes bytes to participant nodes, publishes immutable
metadata, and submits namespace mutation to the fragment's voters.

All nodes can serve reads and mutations. A node that is not a namespace voter
routes the namespace request to the current voter group for that namespace
partition.

## Disk Roles

N2 supports two deliberate node layouts.

### Separated Roots

Fast storage holds mutable namespace and control state. Capacity storage holds
immutable object metadata and shard data.

```text
NVMe / index root
  -> namespace, trash, fragment control, coordinator upload state

HDD / blob root
  -> immutable .ncdx and shard/parity data
```

This is the preferred production layout when NVMe and HDD are available.

### Shared Root

The exact same path may be configured as both an index root and a blob root:

```ini
index_store_disks=/mnt/n2/storage
blob_store_disks=/mnt/n2/storage
```

N2 merges both roles into one physical tree. It does not create duplicate
metadata planes. This is the minimum deployment shape and is valid for a node
with one usable storage device.

Roots are defined by configuration, not by marker files. An index root is a
mutable namespace/control root. A blob root is an immutable object/shard root.
If the same path is configured for both roles, the two planes intentionally
coexist under the same stable fragment directory.

This architecture has no in-place compatibility or migration path from earlier
development layouts.

## Physical Layout

### Namespace And Control Root

```text
<index_root>/
  .fragment/
    <fragment-alias-hash> -> ../@f_<stable-fragment-id>

  @f_<stable-fragment-id>/
    .uploads/
    .trash/

    videos/
      movie.mp4 -> @o_<object-id>
    reports/
      annual.pdf -> @o_<object-id>
```

The mutable fragment alias is only an entrypoint. The stable fragment ID is
storage truth. Renaming a fragment changes its alias, not its physical identity
or existing object metadata. The index root does not own `.fragment.ncdx`;
immutable fragment metadata lives in the blob plane.

There is no `.namespace` wrapper. Namespace paths live directly below the
stable fragment root, and N2 applies distributed mutation semantics around that
namespace.

### Object And Shard Root

```text
<blob_root>/
  @f_<stable-fragment-id>/
    .fragment.ncdx
    .objects/
      ab/
        cd/
          ef/
            12/
              @o_<object-id>/
                <version-ulid>.<size>.<unix-nanos>.<packed-ext>.ncdx
                <version-ulid>/
                  part_0_shard_0.shard
                  part_0_shard_1.shard
                  part_0_shard_2.parity
```

Object fanout is four levels. The fanout distributes object directories across
large filesystems without changing object identity.

Blob roots do not own fragment aliases. They only use stable `@f_` roots. On a
shared root, the namespace/control entries and immutable `.objects` tree simply
coexist under the same stable fragment root.

## Immutable Object Record

`.ncdx` means N2 cluster data index. It is the only committed metadata record
for one immutable object version.

Its body stores:

- immutable object ID;
- stable fragment ID;
- version ID and upload session ID;
- owner and content metadata;
- total logical size and creation time;
- membership/layout epoch;
- resolved data and parity shard counts;
- stripe size;
- part metadata;
- shard locators, checksums, lengths, node IDs, and parity role.

It does not store the user-facing object path. Rename and move therefore never
rewrite `.ncdx`.

The filename provides a zero-decode summary:

```text
<version-ulid>.<size>.<unix-nanos>.<packed-ext>.ncdx
```

Listings and head selection can obtain version, size, time, and extension
without decoding the MessagePack body.

## Metadata Placement

Object metadata follows the object's erasure participants:

```text
object version uses node-a, node-b, node-c
  -> node-a stores local shard(s) + identical .ncdx
  -> node-b stores local shard(s) + identical .ncdx
  -> node-c stores local shard(s) + identical .ncdx
```

N2 does not replicate object metadata across every index disk or every cluster
node. The erasure participants already define the durable object set. Metadata
resolution queries those participants and rejects divergent immutable copies.

If a node has several blob roots, shard placement rotates over those roots.
`.ncdx` is written only where that node has a shard-bearing version directory.

## Version And Head Rules

The namespace points only to the immutable object ID:

```text
videos/movie.mp4 -> @o_<object-id>
```

It does not store a version pointer. To resolve the head, N2 enumerates valid
`.ncdx` filenames for that object and chooses the greatest ULID:

```text
01...A.ncdx
01...B.ncdx
01...C.ncdx  <- head
```

There is no `.nlatest`, head file, mutable object record, or cached head truth
on disk. A new upload appends a new immutable version.

## Partitioned Namespace Quorum

Namespace is the mutable database of N2. It is partitioned by fragment and
logical path, then each partition gets a stable voter group from the current
membership epoch. This spreads namespace write pressure without dragging
immutable object bytes or object `.ncdx` records into consensus.

```text
fragment + logical path
  -> namespace partition
  -> stable voter assignment for that partition
  -> prepare
  -> quorum acceptance
  -> commit
  -> durable request ledger
  -> periodic namespace snapshot
```

Files are materialized in their owning namespace partition. Directories are
namespace structure and may be present across roots so folder rename/move stays
fast and does not rewrite every child object.

Namespace quorum covers:

- upload publication;
- mkdir;
- rename and move;
- trash and recover;
- purge and empty trash.

Prepared mutations are durable. Recovery checks whether the requested
filesystem state was already materialized before replaying, closing the crash
window between filesystem mutation and ledger append.

Namespace snapshots contain only mutable namespace and trash state. They do not
copy `.objects`, `.ncdx`, shards, parity, or upload payload data.

The assignment, ledger, prepared records, and snapshots live under
`fs_storage_path/.n2`. They are control state, not a second user namespace.

## Multipart Ownership

Multipart sessions are coordinator-owned until finalize:

```text
init on node-a
  -> manifest, binding, receipts on node-a
  -> parts streamed and erasure-written across participant nodes
  -> status/finalize/cancel return to node-a
```

Temporary receipts are high-churn state and are not replicated through
namespace quorum. The client records `coordinator_node_id` and returns to that
node when resuming.

This preserves stateless cluster routing for new operations while avoiding
cluster-wide consensus for every part. Once finalized, the immutable object is
cluster-visible and no longer depends on its upload coordinator.

## Write Path

```text
client upload
  -> any healthy N2 node
  -> authenticate and resolve stable fragment
  -> choose/reuse immutable object ID
  -> resolve current membership epoch and EC layout
  -> stream stripes through SIMD erasure coding
  -> write shard/parity bytes to participant nodes
  -> publish byte-identical .ncdx to shard-bearing participants
  -> commit namespace object pointer through fragment quorum
  -> return committed version
```

Object publication is ordered so namespace cannot intentionally point to an
unpublished object version. Failed pre-commit uploads remain unreachable and
are reclaimed by maintenance.

## Read Path

```text
client read on any node
  -> resolve logical path through fragment namespace voter
  -> obtain immutable object ID
  -> resolve max-ULID .ncdx from shard participants
  -> use that version's recorded placement
  -> fetch sufficient shard ranges over n2://
  -> stream directly when healthy
  -> reconstruct through parity when degraded
```

Reads never infer an old object's placement from the current cluster topology.
The version record is authoritative.

## Layout Evolution

N2 supports:

```text
auto
fixed
```

Auto mode derives an EC shape from writable node count:

```text
1 node  -> 1 + 0
2 nodes -> 1 + 1
3 nodes -> 2 + 1
4 nodes -> 3 + 1
```

Fixed mode lets the operator preserve a deliberate production EC shape.

Every object version records its resolved layout. Therefore:

- adding a node lets new objects use the new topology immediately;
- old objects remain healthy under their recorded topology;
- no automatic full-cluster rebalance is required;
- operators can modernize selected objects explicitly.

## Repair And Rebalance

N2 maintenance uses immutable version metadata and recorded placement:

- object status verifies shard availability and reports layout age;
- read-on-recover reconstructs a readable result without mutating namespace;
- object repair restores missing or corrupt shards for the recorded layout;
- object redistribution rewrites one selected object/version into the current
  layout;
- targeted rebalance scans metadata and rewrites only layout-mismatched
  objects;
- already-current objects are skipped without shard IO;
- GC removes immutable object versions no longer reachable from live or trash
  namespace.

An older recorded layout is not degradation:

```text
healthy + older_recorded_layout
```

Health and topology age are separate facts.

## Failure Semantics

- A lost non-coordinator node can degrade affected objects while repairable
  reads reconstruct from remaining shards.
- Namespace operations continue while the fragment voter quorum remains
  available.
- A restored node makes its recorded shards available again without mandatory
  rebalance.
- A lost multipart coordinator loses access to that unfinished local session;
  committed objects remain unaffected.
- A disk should normally be replaced at the same operational role before
  repair. N2 can repair onto currently configured writable roots, but that may
  create temporary local imbalance.

## Gateway Boundary

N2 Gateway is optional and deliberately route-only:

```text
client -> gateway -> healthy N2 node
```

It performs node health selection and forwards streaming requests. It is not
namespace, object, placement, maintenance, quota, or resumability authority.
Multipart coordinator ownership remains a client-visible N2 rule.

## Monitoring And IO Control

`/n2/cluster/monitor` and native `cluster.monitor` report:

- cluster overview;
- node health and resource state;
- current topology/layout;
- sampled object health and layout age;
- peer IO pressure and read-on-recover counters;
- redistribution/rebalance readiness;
- repair, rebalance, and GC jobs;
- namespace voter and ledger state;
- active upload activity.

Explicit `/n2/cluster/usage` performs the more expensive disk aggregation and
should be operator-driven rather than polled continuously.

N2 protects mixed hardware through:

- bounded peer read/write concurrency;
- backpressure and quorum-aware admission;
- latency and stall observation;
- bounded capacity/usage caching;
- maintenance budgets and cancellation;
- persisted maintenance progress;
- streaming CRC32 validation;
- RSS, thread, and file-descriptor monitoring.

These controls govern pressure. They do not change object identity, placement
truth, or namespace authority.

## Protocol Surfaces

N2 exposes:

- `/n2/...` HTTPS for browsers and general clients;
- `n2://` for N2-native tools, shard transport, and node-to-node operations.

The two transports call the same storage and namespace rules. The native
protocol does not introduce a second storage path or alternate object model.
