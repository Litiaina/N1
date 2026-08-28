# N2 3.4 Architecture

## Status

This document is the locked implementation contract for N2 3.4, the current
release of the clean N2 3.x engine generation. It does not read, migrate, or
maintain compatibility with the N2 2.0 inode namespace.

N2 3.4 is a clean format. It accepts only the generation-directory trash
layout and SHA-256 NCNX/object-NCDX/shard integrity defined here. Earlier
experimental 3.x records are rejected rather than ignored or guessed.

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

N2 3.4 combines:

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
6. Each live `.ncnx` record has an exact target layout of `R = M + 1`
   replicas on distinct nodes in its immutable namespace home pool. A new
   client-visible upload publishes all exact replicas; healing restores any
   later loss to that same target layout.
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
13. A new object is acknowledged only after the coordinator reaches
    `A = max(K, floor((K + M) / 2) + 1)` bound pool nodes, at least `K` exact
    EC pieces and `K` immutable NCDX copies are durable, and all `M + 1` exact
    NCNX replicas for that key publish inside that authorized component. N2
    records the complete configured `K + M` shard/NCDX and `M + 1` NCNX repair
    layout even when a recoverable piece is temporarily absent.
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

New writes use every available exact holder and may commit reconstructably at
`K` while bounded autonomous healing restores missing exact slots. The slot returns to
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
  parity budget and reconstructable writes continue while at least `K` pool
  nodes remain writable; the operator must restore capacity before the pool
  drops below `K` or to regain full parity protection;
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

Every N2 3.4 node is bidirectional and unified. Any healthy node may receive a
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

The recorded root ID is a preferred local route, not a permanent disk binding.
Reads probe every enrolled root on the selected node. If the preferred disk is
unavailable or full, new writes and exact-layout repair use another verified
writable root on that same node. Health remains degraded to expose the disk
loss, but the node retains its complete EC and NCNX role while at least one
enrolled root remains writable; only loss of every writable root removes the
node from write admission.

Bidirectionality does not weaken durability. A coordinator without enough
reachable holders rejects the operation or routes it; it never invents local
authority.

## Disk Layout

Each enrolled storage root uses the same N2 3.4 format:

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

One deterministic selected holder remains the canonical ownership coordinate
for healing. Normal LIST merges bounded physical names from all replica
holders so later replica loss and repair never hide a surviving noncanonical
copy. Root selection is deterministic inside each selected node.

Upload commit requires all `R` holders after the object reaches its `K`
EC/NCDX threshold. Rename, move, trash, and recover retain the same
destination-first all-`R` copy/delete contract. A key-absence decision also
requires the complete deterministic replica observation; an unavailable
replica is never interpreted as an empty one.

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

N2 3.4 is a flat object-key namespace with prefix presentation:

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

LIST scans physical `.namespace` paths, not NCNX bodies. Every relevant pool
node returns a bounded page of physical replica names so listing remains
correct through later replica loss and repair. The coordinator merges and
deduplicates those names without opening them.

```text
coordinator
  -> request pages from all relevant pool nodes in parallel
  -> each node walks the requested physical prefix
  -> each node emits replica filenames/common prefixes
  -> coordinator performs a bounded ordered merge and deduplication
  -> opaque cursor records the last emitted global NCNX ordering key
```

Properties:

- no NCNX decode;
- no NCDX open;
- no shard/parity IO;
- no full-cluster in-memory listing;
- bounded memory proportional to the requested page and NCNX replica factor.

The coordinator returns a result only while each pool remains within its
`M`-node NCNX failure budget. An unavailable source is never interpreted as
an empty one.

LIST returns key, name, prefix/type, and lifecycle state. Object size, content
type, checksum, and current version require HEAD because they belong to NCDX.

## Namespace Mutation

Namespace publication is per key and destination first. Every peer operation
contains a stable mutation ULID and expected record identity so retry is
idempotent and stale same-key operations fail rather than delete newer state.

### Upload commit

N2 has two staging lanes before this shared commit boundary. Standard
multipart keeps only a sparse coordinator-memory part map, permits independent
parts to execute concurrently, writes no session/receipt replicas, and defers
shard sync to finalize. Durable multipart persists the existing manifest,
pending binding, and per-part receipts so progress survives restart. Standard
restart loss is an explicit API contract, not loss of a committed object.

One-shot upload and every entry in a standard batch enter this same object
commit path directly. A batch is only one bounded HTTP transport envelope;
its objects never share EC state, NCDX, NCNX, failure fate, or a transaction.
Coordinator-wide backpressure bounds aggregate batch work across concurrent
requests without adding cluster state.

```text
calculate the complete fixed K + M layout
-> prove pool authority A=max(K, strict pool majority) over the NCNX control plane
-> require every exact R=M+1 NCNX holder for this key inside that authority component
-> attempt every exact shard/parity holder in parallel
-> require at least K successfully written EC pieces
-> for standard multipart, each participating node syncs its exact shards
-> publish immutable NCDX to every available exact holder and require K
-> publish to every exact R=M+1 NCNX holder
-> fsync their parent directories
-> acknowledge
```

The NCDX retains descriptors and SHA-256 checksums for all `K + M` exact
pieces, including an unavailable target, so healing has one deterministic
destination rather than inventing a replacement layout. `R=M+1` guarantees
that every possible `K`-node survivor set intersects the NCNX replica set.
The authority rule is stricter than reconstructability but does not require a
healthy full-width pool. A coordinator must reach both `K` and a strict pool
majority, and the complete deterministic NCNX layout for the affected key
must be inside that component. This permits key-scoped degraded writes without
allowing two disjoint components to acknowledge competing namespace truth.
If exact NCNX publication cannot complete, the immutable object is not
acknowledged as live. N2 reports headless immutable data and preserves it;
automatic GC never guesses that missing namespace authority makes an object
disposable.

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
object-ID sets from bounded, deduplicated NCNX replica pages.

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

GC coordinators remain independent and lockless. A namespace branch or trash
record that disappears after it was listed is treated as an idempotently
completed concurrent cleanup, not as engine damage. If a coordinator cannot
prove the complete live and unexpired reference set because the required
topology is temporarily unavailable, it defers the entire run without
reclaiming anything and retries on the next schedule. Malformed records,
conflicting replicas, and integrity failures remain hard errors. A completed
run therefore means that reachability was proven; a deferred run means that no
destructive decision was made.

## Autonomous Healing

Healing is deterministic and object scoped.

Each node has one healing-cycle owner at a time. A scheduled cycle that
overlaps a manual or replacement cycle skips that interval; it does not mark
the active cycle failed and does not overwrite the active cycle's status.

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

### Survival and safety state

A pool is:

- fully protected when all `K + M` exact holders accept the new object;
- degraded and read-capable when at least `K` exact pieces remain
  reconstructable;
- conditionally write-capable when the coordinator reaches
  `A = max(K, strict pool majority)` and every exact NCNX holder for the
  affected key, even if as many as `M` shard/NCDX holders are absent;
- critical for an object read below its recorded `K` boundary.

`K` is the object EC reconstruction threshold, not a standalone consensus
quorum and not an adaptive EC layout. Pool authority additionally requires a
strict majority; the key's immutable NCNX layout additionally requires every
exact replica. Missing pieces remain exact object-local repair work. An
unrepairable object or namespace record emits a precise local report but does
not stop unrelated reconstructable reads. Cluster-wide read-only protection
is reserved for systemic control-authority failures such as membership or
binding identity disagreement.

Reads continue for objects whose recorded layouts remain decodable.

Work is partitioned by fragment, pool, and canonical key ownership so multiple
nodes heal concurrently without scanning or repairing the same record.

### Network-partition authority

Transport unreachability is not proof that a physical instance has stopped.
During an internode partition, every configured instance may remain alive and
may still be reachable by a gateway or direct NOA/native client. N2 must never
allow two disconnected components to acknowledge conflicting mutations of the
same NCNX key. Immutable object IDs and version ULIDs prevent byte-path
collisions, but they do not resolve two valid NCNX records that point one
logical key at different objects.

The enforced safety law is therefore:

```text
pool authority = max(K, strict majority)
key authority  = pool authority + every exact NCNX holder for that key
```

Connection refusal, timeout, packet loss, and a missing health response are
observations only; none is independently a fence. A route-only gateway is not
storage authority and cannot make an isolated node safe merely by selecting
it for a request. New object placement uses the newest writable pool;
mutation of an existing NCNX uses that record's exact historical pool,
including a pool sealed only against new placement. The coordinator first
proves `A = max(K, floor(N/2)+1)` reachable bound nodes, then requires every
deterministic `M + 1` NCNX replica for the key to commit. A component below
`A`, or one missing even one exact NCNX holder for the key, fails closed.

Authority and data availability are deliberately separate. Pool authority is
evaluated on peer HTTP/NCNX reachability; namespace truth still requires the
key's complete exact NCNX set. The native shard plane may lose up to `M` exact
holders: an admitted object may commit at `K`, record unavailable roles in its
immutable NCDX, and heal those roles when their bindings return. Busy or
failed shard transport therefore cannot manufacture namespace authority and
does not force a healthy-path object lock or namespace database.

For a configured pool `N = K + M`, `K` defines object reconstruction and the
strict majority prevents disjoint mutation owners. Exact NCNX publication
closes the remaining overlapping-view case: two asymmetric majority views
cannot both acknowledge the same key unless both can reach that one fixed
replica set. A `1 + 1` pool has no one-node write authority without an
independent witness, reserve promotion, or physical fence; it remains readable
at `K=1` when its object can be decoded.

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

Every N2 3.4 release proof records:

- operation latency distributions in microseconds/milliseconds;
- CPU utilization and cycles when host permissions support hardware counters;
- context switches and peak memory;
- bytes read and written;
- observed process IO and the exact designed participant/phase count per
  operation. Syscall-level counts require an explicitly instrumented run.

Required namespace properties:

```text
GET key       = bounded NCNX replica reads + bounded NCDX holder read
PUT attempt   = K+M object/NCDX holders + M+1 NCNX holders
PUT acknowledge = max(K, pool majority) authority + at least K EC pieces + K NCDX copies + all exact M+1 NCNX copies
DELETE key    = M+1 NCNX holders, zero object data IO
RENAME key    = source and destination NCNX holders, zero object data IO
LIST page     = bounded replica filename merge, zero NCNX/NCDX/shard decode
```

A test passes correctness only when the measured IO matches these boundaries.
Fast output with hidden global scans or object-data reads is a failure.

## Release Gates

N2 3.4 is not declared production-stable until the following are proven:

- exact `K + M` shard placement and exact `M + 1` NCNX replication;
- upload, GET, HEAD, LIST, copy/move, trash, recover, and prefix operations;
- zero-decode paginated listing with no duplicates or omissions;
- same-key collision safety and independent-key parallelism;
- crash/restart at every destination-first copy/delete stage;
- one- and parity-count node loss behavior;
- full-width protection, degraded writes at exactly `K`, rejection at `K-1`,
  and healing back to the unchanged exact layout;
- autonomous one-slot reserve promotion, retired-instance fencing, historical
  exact-layout healing, and restart continuation;
- fail-closed writes beyond the configured failure budget;
- NCNX and object autonomous repair;
- repair jobs that never skip scheduler-denied objects while reporting a
  clean completion;
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
