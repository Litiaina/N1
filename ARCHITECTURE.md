# N2 3.3 Architecture

## Status

This document is the locked implementation contract for N2 3.3, the current
release of the clean N2 3.x engine generation. It does not read, migrate, or
maintain compatibility with the N2 2.0 inode namespace.

N2 3.3 is a clean format. It accepts only the generation-directory trash
layout and SHA-256 NCNX/object-NCDX/shard integrity defined here. Earlier
experimental 3.x records are rejected rather than ignored or guessed.

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

N2 3.3 combines:

- immutable, object-level erasure sets;
- flat physical-key namespace records;
- incrementally added static-EC server pools;
- deterministic bounded placement;
- copy/delete namespace mutation;
- autonomous object and namespace healing;
- optional cluster-wide reserve capacity with autonomous logical-slot
  replacement.

The architecture has no database, global namespace writer, directory inode,
cluster-wide mutation log, or implicit redistribution.

## Architectural Laws

1. Object data, parity, and `.ncdx` records are immutable after publication.
2. A committed object records its exact pool, EC width, holders, roots, and
   layout generation. Current topology never reinterprets an old object.
3. The namespace is a flat collection of independently mutable `.ncnx`
   records. Physical directories only partition logical key prefixes.
4. Each pool has a static `K + M` object EC contract.
5. A pool generation fixes its node IDs and each node's root-ID set. Expansion
   appends a new complete pool; it never remaps an enrolled pool in place.
6. Each live `.ncnx` record has exactly `R = M + 1` replicas on distinct nodes
   in its immutable namespace home pool.
7. Every node in a server pool can store namespace records, NCDX, shards,
   parity, uploads, and fragment state. There are no namespace/blob roles.
8. A namespace mutation operates only on NCNX records. It never opens or
   rewrites NCDX, shards, or parity.
9. Different keys mutate independently. The same logical key is the only
   unavoidable namespace conflict boundary.
10. Rename and move use destination-first live copy/delete. Trash and recover
   use destination-first live/trash copy/delete. Folder operations are per-key,
   resumable, and deliberately non-atomic across the full prefix.
11. LIST derives keys and prefixes from filenames and directories. It does not
    decode NCNX or open NCDX.
12. Adding a pool adds capacity for future objects. Existing objects are not
    redistributed unless an operator explicitly requests re-encoding.
13. When a pool cannot satisfy its exact object EC or NCNX durability, unsafe
    writes fail closed. Readable committed objects remain readable.
14. SHA-256 is the only N2 integrity checksum for NCNX, object NCDX, upload
    bodies, immutable shards, and verified shard blocks.
15. Placement is calculated from the immutable node membership of the selected
    server-pool generation. An unavailable selected holder is never silently
    replaced by another active pool node.
16. Placement names stable logical slots, not replaceable physical instances.
    Rebinding one failed slot advances only the binding epoch; it does not
    change the topology epoch, pool generation, EC width, or object layout.
17. Reserve instances are unassigned physical capacity. They receive no new
    shards or NCNX replicas and do not participate in LIST before promotion.
18. Promotion requires repeated failure observations, confirmation from every
    surviving pool slot and the reserve candidate, compatible roots and
    capacity, an atomic membership commit, and a durable retired-instance
    fence.

## Cluster And Server Pools

An N2 cluster is an ordered collection of server pools:

```text
N2 cluster
├── pool A, generation 1, EC 4 + 2
│   ├── A1
│   ├── A2
│   ├── A3
│   ├── A4
│   ├── A5
│   └── A6
├── pool B, generation 2, EC 4 + 2
│   ├── B1
│   ├── B2
│   ├── B3
│   ├── B4
│   ├── B5
│   └── B6
├── Reserve Set, unassigned and outside placement
│   ├── R1
│   └── R2
└── fragments
    ├── videos
    └── documents
```

A fragment is a bucket-like authentication, lifecycle, listing, and policy
boundary. It is not a server pool. A fragment may contain objects in every
pool. Adding pool B extends the cluster without migrating pool A.

Each pool records:

- stable pool ID;
- monotonically increasing pool generation;
- static data width `K`;
- static parity width `M`;
- ordered node membership and stable node identities;
- writable or sealed state.

Its membership contains exactly `K + M` distinct node failure domains. Extra
nodes form another complete server pool; they are never silently folded into
an existing pool. Separately declared reserve instances remain outside every
pool and do not alter its mathematical placement contract.

The newest writable pool that can satisfy its static EC accepts new object
versions. A sealed or full pool remains readable and healable. An object lookup
uses its recorded pool layout, never a guess based on the newest membership.

### Logical slots and the Reserve Set

A pool's ordered members are stable logical slots. Each slot has one current
physical binding:

```text
pool A slot A3
├── logical root A3D1
└── physical binding instance-a3, binding epoch 1

after confirmed loss

pool A slot A3
├── logical root A3D1
└── physical binding reserve-r1 / physical root R1D1, binding epoch 2
```

The pool ID, pool generation, slot ID, logical root ID, topology epoch, and
object placement remain unchanged. The reserve's physical root identity is
retained separately for device validation. The failed instance becomes a
durable retired-instance fence and cannot publish with its old binding epoch.

N2 changes a binding only after every surviving slot and the reserve candidate
independently observe the failed physical instance as unreachable. The reserve
commits first, remote survivors commit next, and the coordinating survivor
commits last. Membership is persisted with temporary-file write, file `fsync`,
atomic rename, and parent-directory `fsync`.

New exact-width writes may continue through the promoted degraded binding while
bounded autonomous healing restores historical layouts. The slot returns to
`online` only after healing completes with no incomplete, unrecoverable,
failed, or errored objects. Only one missing slot in a pool is eligible for
autonomous promotion. Multiple missing slots, incompatible reserve roots,
insufficient recorded capacity, observation disagreement, or an unreachable
reserve fail closed without rewriting membership.

### Reserve Set deployment policy

The Reserve Set is architecturally optional. N2 remains a valid static-EC
engine with `reserve_nodes: []`:

- constrained, low-end, development, and lab deployments may run without a
  reserve;
- without a reserve, committed objects remain readable within their recorded
  parity budget, but loss of an active slot stops new writes to that exact
  pool until the operator restores the slot or enrolls compatible replacement
  capacity;
- omitting a reserve does not reduce the EC recorded for existing objects, but
  it removes automatic physical-node replacement.

The production operational recommendation is at least one compatible reserve
instance per cluster. A production reserve must:

- be in an independent physical failure domain from the active pool nodes;
- have the same physical-root count as every logical slot it is expected to
  replace;
- have capacity no smaller than the replaced slot's recorded usage;
- run the same N2 version, format-v5 membership, TLS trust, and cluster
  secrets;
- remain outside client routing and object/namespace placement until
  promotion.

One reserve replaces one failed physical binding and is consumed by that
promotion. After recovery, operators replenish the Reserve Set. Clusters with
multiple pool shapes need at least one reserve compatible with each shape they
expect to replace automatically; clusters requiring more than one sequential
replacement without operator intervention provision additional reserves.
Reserves are availability capacity, not extra parity and not another server
pool.

## Unified Nodes And Disks

Every N2 3.3 node is bidirectional and unified. Any healthy node may receive a
client operation and coordinate it. Every pool node may also persist the data
assigned to it:

```text
node A1
├── disk A1D1
│   ├── NCNX replicas selected for A1
│   ├── NCDX replicas selected for A1
│   └── data/parity shards selected for A1
├── disk A1D2
└── N2 coordinator, JWT verifier, peer client, healer
```

Node selection always precedes disk selection. Two disks in one host never
count as two NCNX replicas or two shards of one EC stripe. A node may have one
or many roots; deterministic root selection distributes independent records
while preserving a single host failure domain.

Bidirectionality does not weaken durability. A coordinator without enough
reachable holders rejects the operation or routes it; it never invents local
authority.

## Disk Layout

Each enrolled storage root uses the same N2 3.3 format:

```text
<storage_root>/
  .n2-root.ncdx
  .fragment/
    <fragment-alias-hash> -> ../@f_<stable-fragment-id>

  @f_<stable-fragment-id>/
    .fragment.ncdx
    .namespace/
      videos/
        video.mp4.ncnx
    .trash/
      <unix-nanos>.<mutation-ulid>/
        videos/
          video.mp4.ncnx
    .uploads/
    .objects/
      ab/
        cd/
          ef/
            12/
              @o_<object-id>/
                <version>.l<layout-generation>.<size>.<unix-nanos>.<ext>.ncdx
                <version>/
                  part_0_shard_0.shard
                  part_0_shard_1.shard
                  part_0_shard_2.parity
```

The `.namespace/videos` directory is only the physical prefix representation
for keys beginning with `videos/`. It is not an inode, mutable directory
record, lock owner, durability group, or transaction participant.

Reserved internal components begin with `.n2-` or match N2's physical control
directories. User keys cannot address them.

## NCNX

NCNX means Node Cluster Namespace Index. One live NCNX maps one normalized
logical key to one stable object identity.

The key is represented by the NCNX path:

```text
.namespace/videos/video.mp4.ncnx
                         -> object @o_01...
```

NCNX contains only the truth required to validate and resolve the pointer:

```text
format_version
stable_fragment_id
object_id
mutation_id
created_at_unix_nanos
payload_sha256
```

It does not duplicate object size, content type, checksums, shard holders, or
version history. Those remain in immutable NCDX. HEAD or GET resolves NCNX and
then reads NCDX. LIST does not.

NCNX is named MessagePack, integrity checked, immutable after publication, and
small enough that namespace mutation performs bounded metadata IO only.

### NCNX placement

For a pool with EC `K + M`:

```text
NCNX replica count R = M + 1
```

For `4 + 2`, every key has three NCNX replicas. For `1 + 1`, every key has two.
The replica set is selected by deterministic rendezvous scoring over:

```text
(cluster ID, pool ID, pool generation, fragment ID, logical key, node ID)
```

One deterministic selected holder is canonical for listing. The remaining
holders are durability replicas and read/healing fallbacks. Root selection is
deterministic inside each selected node.

All `R` NCNX replicas must be durably published before the namespace mutation
reports success. N2 does not acknowledge a partially durable key.

## Static Object Placement

Object upload selects exactly `K + M` distinct nodes from one writable pool.
Each selected node contributes one root and one shard role. NCDX records the
exact result:

```text
object ID
content version
pool ID and pool generation
EC K + M
membership/layout generation
node ID, root ID, shard index, shard role
part sizes and checksums
```

NCDX is byte-identical on its selected holders. Reads and repairs follow this
recorded layout. Its named MessagePack body carries a SHA-256 integrity digest,
and every shard plus every verified range block is also SHA-256 checked.
Adding disks, nodes, or pools cannot remap an existing object.

## Key Semantics

N2 3.3 is a flat object-key namespace with prefix presentation:

```text
videos/a.mp4
videos/b.mp4
images/c.jpg
```

`videos/` is derived from the keys. Independent keys do not update shared
directory state and therefore do not collide. Empty directories are not
first-class storage objects. A directory shown by NOA or POSIX-like clients is
a common prefix.

Exact key lookup contacts the deterministic NCNX replica set for each
candidate pool. Once found, its physical home pool and `object_id` locate
immutable object metadata. Pools are checked in newest-to-oldest order; a key
may have only one live NCNX identity cluster-wide. The NCNX home pool remains
stable across later content versions; exact version placement remains NCDX
truth and may use a newer pool.

## Zero-Decode Listing

LIST scans physical `.namespace` paths, not NCNX bodies. A node returns only
keys for which it is the deterministic canonical NCNX holder. Replica copies
are skipped mathematically without opening them.

```text
coordinator
  -> request pages from all relevant pool nodes in parallel
  -> each node walks the requested physical prefix
  -> node emits only its canonical filenames/common prefixes
  -> coordinator performs a bounded ordered merge
  -> opaque cursor records the last emitted global NCNX ordering key
```

Properties:

- no NCNX decode;
- no NCDX open;
- no shard/parity IO;
- no replica deduplication table on the healthy canonical path;
- no full-cluster in-memory listing;
- bounded work proportional to the requested page and traversed prefix.

If a canonical source is unavailable, the coordinator retries the same page
against physical replica streams. It deduplicates only that bounded page and
returns a result only while each pool remains within its `M`-node NCNX
failure budget. An unavailable source is never interpreted as an empty one.

LIST returns key, name, prefix/type, and lifecycle state. Object size, content
type, checksum, and current version require HEAD because they belong to NCDX.

## Namespace Mutation

Namespace publication is per key and destination first. Every peer operation
contains a stable mutation ULID and expected record identity so retry is
idempotent and stale same-key operations fail rather than delete newer state.

### Upload commit

```text
publish K + M shards/parity
-> publish immutable NCDX
-> create and fsync all R live NCNX replicas
-> fsync their parent directories
-> acknowledge
```

If NCNX publication cannot complete, the immutable object is not live. N2
reports that headless immutable data and preserves it; automatic GC never
guesses that missing namespace authority makes an object disposable.

### Copy/move one key

```text
read and validate source NCNX
-> create a destination NCNX generation stamped with the mutation ULID
-> create and fsync all destination NCNX replicas
-> remove the exact expected live source replicas
-> fsync source parents
-> acknowledge
```

The destination becomes live before the source is removed. A crash may leave
both live names, which is safe and retryable. It must never leave neither name.

### Delete one key

```text
read and validate live NCNX
-> copy to all deterministic trash holders
-> one generation directory records deletion time and mutation ULID
-> preserve the unchanged logical NCNX path below that directory
-> fsync trash records and parents
-> remove only the exact expected live records
-> fsync live parents
```

### Recover

```text
read and validate trash NCNX
-> reject any existing live destination
-> publish all R live NCNX replicas
-> remove only the recovered trash mutation records
```

### Prefix/folder operations

Folder rename, move, delete, and recover enumerate matching keys and execute
the one-key protocol independently with bounded parallelism. They are:

- resumable;
- idempotent per key;
- independent of descendant count for each record;
- non-atomic across the whole prefix;
- free of global and physical-directory locks;
- free of object data IO.

Progress reports count completed, conflicted, and failed keys. Repeating the
same operation completes remaining keys.

## Lifecycle And Garbage Collection

Trash records use zero-decode expiry names:

```text
<unix-nanos>.<mutation-ulid>/<logical-key>.ncnx
```

GC scans `.trash` only for normal lifecycle work. It reads expiry from the
generation directory name and builds temporary live and unexpired-trash
object-ID sets from canonical NCNX records.

For each expired trash record:

1. if any live or unexpired trash NCNX points to the same object, remove only
   that expired trash record;
2. if no live or unexpired trash NCNX points to it, reclaim the exact recorded
   immutable object;
3. remove the trash NCNX last.

The invariant is **live or unexpired trash wins over expired trash**.
Copy/move can leave several live keys for one object without risking
reclamation. Repeated deletes create independent generation directories and
cannot collide. A prefix delete uses one generation directory across every
key and every deterministic NCNX replica.

Recovery is strict. If the logical key exists live, recovery returns a
conflict and preserves both records. Otherwise it publishes live NCNX first,
removes only the selected trash record, and prunes its generation directory
only when that directory becomes empty.

Autonomous healing may inventory immutable objects to report headless data, but
it preserves those objects because absence of a namespace reference does not
prove whether the cause was failed publication or catastrophic namespace loss.
Normal TTL GC never scans all objects and never guesses that a headless object
is disposable.

GC keeps no persistent reference database, `.entries`, `.records`, or global
transaction state.

## Autonomous Healing

Healing is deterministic and object scoped.

### NCNX healing

The deterministic owner for a fragment scans its canonical live and trash
NCNX records. For each record it:

1. derives the exact `R = M + 1` holders;
2. reads replica observations in parallel;
3. selects an integrity-valid identical record;
4. recreates missing or corrupt replicas on their exact roots;
5. emits one disposable JSON report when degradation or repair occurred.

The healer revalidates the exact NCNX state immediately before a critical
classification or repair. TTL-expired trash is owned by GC and is never
resurrected by a stale healing observation. A dangling object-reference report
also revalidates the exact live reference before it is emitted.

Reports are not replicated and are not storage truth.

### Object healing

For each canonical live NCNX, the healer resolves NCDX and verifies its exact
holders, checksums, shards, and parity. Recoverable loss is reconstructed only
to the recorded layout. Redistribution is a separate explicit re-encode.

### Safety state

A pool rejects new writes when:

- fewer than `K + M` distinct writable pool nodes are available;
- fewer than `M + 1` NCNX holders can be durably written;
- membership/layout identities disagree;
- healing proves that safe publication or repair is impossible.

Reads continue for objects whose recorded layouts remain decodable. A
catastrophic object emits a precise report containing fragment, logical key,
object ID, pool, expected holders, observed loss, and repair result.

Work is partitioned by fragment, pool, and canonical key ownership so multiple
nodes heal concurrently without scanning or repairing the same record.

### Reserve replacement healing

Reserve recovery is a persisted continuation of exact-layout healing, not
redistribution. The promoted slot first synchronizes fragment control records
and then participates in forced SHA-256 NCNX, NCDX, shard, and parity repair.
Existing objects keep their recorded pool, slot, and logical-root coordinates.
A restart while the binding is `degraded` deterministically resumes recovery;
it does not create a second promotion.

Reserve promotion reports are disposable independent JSON files under the
membership control directory. They are operational evidence, not replicated
storage authority.

## Fragment Operations

A fragment is a deliberate cluster-wide control boundary. Create, alias
rename, key rotation, policy change, and delete may contact all pools/nodes.
These operations are expected to be infrequent and may be `O(pools + nodes)`.

Renaming a fragment changes only its alias links/catalog. Stable fragment IDs,
NCNX relative paths, NCDX, object IDs, and shards do not move.

## Expansion And Rebalancing

Pool expansion is append-only:

```text
pool A remains exact and readable
-> add complete pool B with its own static K + M
-> increment membership generation
-> direct new writes to B
```

N2 does not force redistribution when capacity is added. Explicit rebalance is
object re-encoding into a new immutable layout. It is optional, restartable,
and never required merely to make a new pool usable.

The logical slot/root set of an enrolled pool generation is immutable because
NCNX placement depends on it. Add ordinary capacity as a new complete pool
generation; never append or remove logical roots inside an existing
generation. Reserve promotion is the narrow exception at the physical-binding
layer: it rebinds an existing logical slot/root contract and does not add
placement identities or capacity.

## Authentication

JWT authentication remains cluster-wide and stateless. Every node verifies the
same signed fragment claims locally. Server pools do not introduce separate
authentication databases or user-visible endpoints.

## Performance Contract

Every N2 3.3 release proof records:

- operation latency distributions in microseconds/milliseconds;
- CPU utilization and cycles when host permissions support hardware counters;
- context switches and peak memory;
- bytes read and written;
- observed process IO and the exact designed participant/phase count per
  operation. Syscall-level counts require an explicitly instrumented run.

Required namespace properties:

```text
GET key       = bounded NCNX replica reads + bounded NCDX holder read
PUT publish   = K+M object holders + M+1 NCNX holders
DELETE key    = M+1 NCNX holders, zero object data IO
RENAME key    = source and destination NCNX holders, zero object data IO
LIST page     = filename traversal only, zero NCNX/NCDX/shard decode
```

A test passes correctness only when the measured IO matches these boundaries.
Fast output with hidden global scans or object-data reads is a failure.

## Release Gates

N2 3.3 is not declared production-stable until the following are proven:

- exact `K + M` shard placement and exact `M + 1` NCNX replication;
- upload, GET, HEAD, LIST, copy/move, trash, recover, and prefix operations;
- zero-decode paginated listing with no duplicates or omissions;
- same-key collision safety and independent-key parallelism;
- crash/restart at every destination-first copy/delete stage;
- one- and parity-count node loss behavior;
- autonomous one-slot reserve promotion, retired-instance fencing, historical
  exact-layout healing, and restart continuation;
- fail-closed writes beyond the configured failure budget;
- NCNX and object autonomous repair;
- TTL live-wins-over-trash and orphan safety;
- explicit object redistribution without changing untouched layouts;
- append-only server-pool expansion;
- measured latency, CPU, filesystem calls, bytes, and peer fan-out;
- clean restart with no manual repair after every bounded chaos case.

Passing bounded architectural tests proves implementation conformance and
known failure behavior. It does not replace long-duration soak, hardware fault,
or independent multi-host production evidence. Current results and remaining
gates are recorded in [TESTING.md](TESTING.md); performance measurements are
kept separately in [BENCHMARK.md](BENCHMARK.md).
