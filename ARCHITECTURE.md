# N2 Architecture

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

N2 owns distributed namespace, immutable object identity, per-version erasure
layout, node placement, peer transport, repair, redistribution, rebalance, and
cluster health. Every healthy node can receive client operations; there is no
permanent object-IO leader.

## N2 1.0.0 Verdict

N2 `1.0.0` locks the storage architecture as a native distributed object
storage engine with filesystem-like namespace semantics. The architecture is
not a database-backed file manager and not an S3 clone. It is three storage
planes with strict ownership:

```text
immutable blob shards      actual erasure-coded object bytes
immutable .ncdx records    zero-decode object/version/shard truth
mutable namespace          path, directory, trash, rename, and recover truth
```

Only namespace mutates for ordinary user-visible filesystem operations. Blob
payloads and `.ncdx` records are immutable after commit. This is the N2 design
line for 1.0.0 and later 1.x hardening work.

N2 is scalable because object IO is bounded by object-level erasure sets while
namespace IO is partitioned by stable directory-entry identity and replicated
through bounded voter groups. Adding nodes and devices adds object capacity,
repair bandwidth, and namespace partition capacity without introducing a
central database, global namespace writer, or cluster-wide mutable object head.

The current production minimum is three storage nodes. Three nodes allow the
namespace to use a safe 2-of-3 quorum and the object plane to use a 2+1 layout.
Two-node deployments are valid for development or constrained labs, but a
2-of-2 namespace cannot keep accepting writes after either node is lost.

## Architectural Invariants

The N2 1.0 storage model is defined by these rules:

- a user path is mutable namespace, not object storage;
- namespace identity is inode-like: stable directory IDs plus independently
  sharded directory entries;
- a directory entry maps `(parent-directory-id, name)` to a stable child
  directory ID or immutable object ID;
- `@o_<object-id>` and every committed `.ncdx` version are immutable;
- each `.ncdx` is colocated with that version's shard/parity data;
- `.ncdx` contains immutable object and placement truth, never a logical path;
- every shard-bearing participant stores the same named-MessagePack `.ncdx`;
- the head version is the greatest valid ULID filename in the object directory;
- object bytes and metadata are not cluster-consensus state;
- namespace mutation is partitioned quorum state by directory entry, never by
  a complete mutable pathname;
- namespace virtual partitions use fixed-size voter groups across distinct
  nodes and storage devices;
- no global namespace writer, global namespace index, union read, or
  source-voter search exists;
- directory rename, move, trash, and restore never rewrite descendants;
- `.trash` is lifecycle-managed exclusively by configured TTL; there is no
  manual purge, empty-trash operation, or recover-all mutation;
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

## Unified Storage Devices

N2 has one configured storage-device role. Every configured device can hold
immutable object shards, immutable `.ncdx` records, and assigned namespace
virtual partitions. There is no index-disk role and no index-root setting.

```ini
storage_disks=/mnt/n2/disk-00,/mnt/n2/disk-01
```

Namespace replicas for one virtual partition must be placed on distinct nodes
and, where topology information is available, distinct failure domains. A node
with several devices distributes its local namespace assignments across them.
Removing the mandatory index tier makes node expansion identical to storage
expansion: adding a node or device contributes both object capacity and
namespace throughput.

This architecture has no in-place compatibility or migration path from earlier
development layouts.

## Physical Layout

### Unified Device Root

```text
<storage_device>/
  .n2/
    cluster/
    namespace/
      partitions/

  .fragment/
    <fragment-alias-hash> -> ../@f_<stable-fragment-id>

  @f_<stable-fragment-id>/
    .uploads/
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

The mutable namespace partition store and immutable object tree share the same
configured devices but remain separate data planes. Namespace placement is
independent of object EC placement. A device may hold an object shard without
holding that object's directory entry, or vice versa.

Object fanout is four levels. The fanout distributes object directories across
large filesystems without changing object identity.

The stable fragment ID is storage truth. Renaming a fragment changes namespace
references, not immutable object metadata.

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
object uses node-a, node-b, node-c
  -> node-a stores local shard(s) + identical .ncdx
  -> node-b stores local shard(s) + identical .ncdx
  -> node-c stores local shard(s) + identical .ncdx
```

N2 does not replicate object metadata across every index disk or every cluster
node. The erasure participants already define the durable object set. Metadata
resolution queries those participants and rejects divergent immutable copies.

Placement is stable by object identity, not by version ID. New versions of the
same immutable object ID use the same bounded participant set, while each
committed version still records its exact membership epoch, EC shape, and shard
locators. This lets N2 resolve a namespace `@o_<object-id>` head by querying a
bounded object holder set instead of broadcasting metadata reads to the whole
cluster. Older or layout-drifted objects can fall back to wider discovery.

If a node has several storage devices, shard placement rotates over those devices.
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

Namespace is N2's only mutable data plane. It uses the same identity model as a
POSIX filesystem instead of storing complete paths as identity:

```text
(fragment, parent-directory-id, entry-name)
  -> child-directory-id
  or
  -> immutable-object-id
```

Directory IDs never change when a directory is renamed, moved, trashed, or
restored. Consequently, those operations change only parent directory entries;
they never scan or rewrite descendants.

Every directory-entry key maps deterministically to one of 16 initial virtual
namespace partitions for its directory. Each partition has a bounded voter
group selected across distinct failure domains. The number of contacted voters
is independent of total cluster size. A directory records the membership epoch
that owns its partitions. Node liveness changes and later node additions cannot
silently remap existing namespace truth.

```text
(parent-id, name)
  -> virtual partition
  -> bounded voter group
  -> conditional mutation at expected generation
  -> durable quorum result
```

Disjoint entries mutate concurrently, including entries in the same logical
directory. A transaction acquires participant partitions by virtual partition
ID, then visits each partition's voters in deterministic order. These two
orders avoid cross-partition deadlock and split-lock livelock without a global
writer. Conflicting operations on one entry are serialized by its generation
and mutation ID. Cross-directory rename is a bounded atomic operation over only
the source and destination entry partitions. It never becomes a cluster-wide
transaction.

Reads resolve each path component through its directory-entry partition. A
listing reads the partitions containing that directory's entries and performs a
paginated merge. N2 never unions replicas of the same partition: it reads only
an identical quorum-confirmed generation. A quorum-certified committed payload
can replay an interrupted commit onto a stale voter; stale state is never
accepted as namespace truth.

### Trash Lifecycle

`.trash` is a reserved namespace directory. Delete atomically moves one
directory entry beneath `.trash`; restore reverses that move while the entry is
inside its retention period. A directory move carries its stable directory ID,
so all descendants remain attached without being rewritten.

There is no manual purge, empty-trash, or recover-all operation. After the
configured TTL expires, lifecycle commits removal of the trash entry, verifies
that no namespace reference remains, and then removes the immutable `.ncdx`
records and EC shards. Retries are idempotent by mutation ID.

### Namespace Scaling

Virtual partitions can be reassigned and split without changing directory or
object identities. Adding nodes and devices therefore increases namespace
capacity and aggregate throughput. A large directory does not become one
physical metadata partition because its entries are distributed by
`(parent-directory-id, entry-name)`.

Namespace recovery state contains only directory entries, generations,
committed mutation identities, and placement epochs. It never copies or mutates
`.objects`, `.ncdx`, shards, parity, or upload payload data.

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
  -> resolve max-ULID .ncdx from bounded object participants
  -> use that version's recorded placement
  -> fetch sufficient shard ranges over n2://
  -> stream directly when healthy
  -> reconstruct through parity when degraded
```

Reads never infer an old object's placement from the current cluster topology.
The version record is authoritative.

Namespace reads contact only the directory partition's recorded voter group.
An identical voter quorum is required for both an entry and an empty result.
N2 never searches non-voters, unions divergent replicas, or treats a lone
non-empty response as truth.

## Layout Evolution

N2 supports:

```text
auto
fixed
```

Auto mode derives a bounded object erasure set from writable node count. The
default bound is `8 data + 3 parity`, so small clusters grow naturally while
large clusters stop expanding one object's shard set across every node:

```text
1 node     -> 1 + 0
2 nodes    -> 1 + 1
3 nodes    -> 2 + 1
4 nodes    -> 3 + 1
10 nodes   -> 8 + 2
11+ nodes  -> 8 + 3
```

The bounds are configured with `auto_max_data_shards` and
`auto_max_parity_shards`. Fixed mode still lets the operator preserve a
deliberate production EC shape.

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

Cluster maintenance is fragment-parallel. A distributed maintenance request
first resolves the reachable write nodes, then assigns stable fragment IDs
across those nodes:

```text
fragments: F1 F2 F3 F4
live nodes: node-a node-b

node-a -> F1, F3
node-b -> F2, F4
```

Each node runs its assigned fragments locally through the same object repair,
targeted rebalance, or GC worker. The coordinator records assignment and
status; it does not become object truth, namespace truth, or gateway truth. If a
node is offline, it is excluded from the assignment set and the remaining nodes
continue.

Distributed maintenance routes:

```text
POST /n2/cluster/maintenance/distributed/start
GET  /n2/cluster/maintenance/distributed/status
```

Supported kinds:

```text
gc
repair
rebalance
```

An older recorded layout is not degradation:

```text
healthy + older_recorded_layout
```

Health and topology age are separate facts.

## Proof Status

The 1.0.0 architecture has both unit proof and live-node proof.

Unit proof covers immutable dentry transactions, generation conflicts, O(1)
directory rename, trash/restore identity, strict quorum selection,
committed-intent replay, expansion-stable namespace placement, adaptive object
layout, conservative namespace health, cluster-aware fragment repair, and
gateway replay safety.

The July 8, 2026 four-node hardware proof is recorded outside the repository
under `/home/wlzambo/DEV-TEST/n2-maintenance-proof-20260708`. The tested build
passed object repair, fragment repair, global distributed repair, fragment and
global redistribution, 4->3->4 topology movement, namespace race hardening,
trash/recover, final SHA-256 content verification, and per-fragment monitoring.

One important boundary remains intentionally enforced: a four-node cluster
reduced to two arbitrary survivors can read objects that still have enough
recorded shards, but it must not accept new namespace mutations until namespace
voter migration/reassignment exists. The current health and upload admission
report that state conservatively instead of pretending it is writable.

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
namespace, object, placement, maintenance, capacity admission, or resumability authority.
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
- namespace voter, partition, transaction, and placement-epoch state;
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
