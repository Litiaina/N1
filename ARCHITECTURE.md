# N2 2.0 Architecture Redesign

## Status

This document defines the locked N2 2.0 architecture and the implementation
contract now being built. Phases 1 through 6 and exact-layout autonomous
healing are present in the 2.0 development tree; expansion, permanent-holder
replacement, and Phase 8 operational integration remain release gates.
Bounded evidence and the exact unclosed gates are recorded in
`N2_2_0_VERIFICATION.md`.

N2 2.0 is a clean engine boundary. Its namespace layout, node roles, disk
layout, mutation protocol, and failure rules are defined here without an
in-place compatibility contract for earlier development layouts. Any future
migration utility must be designed and verified separately; it must not add
compatibility branches to the 2.0 storage engine.

The redesign has two independently expandable storage planes:

```text
N2 cluster
├── namespace-node collection
│   └── replicated stable directory inodes and dentries
└── blob-node collection
    └── immutable per-object erasure sets, shards, parity, and .ncdx
```

The two collections are not fixed server sets. A collection is the
current set of eligible nodes for that storage plane. After its minimum is
met, one node or one disk can be added at a time.

## Design Objective

N2 2.0 combines a POSIX-like distributed inode namespace with N2's immutable,
object-level erasure-coded blob design.

The namespace plane owns names and hierarchy. The blob plane owns bytes and
immutable placement truth. Namespace operations never rewrite object bytes,
and blob topology changes never redefine namespace identity.

```text
mutable namespace
  path, directory identity, rename, move, trash, restore

immutable object plane
  object identity, versions, EC shape, holders, shards, parity, checksums
```

The scale rule is fixed-width work over expandable collections:

```text
namespace operation width = recorded inode redundancy R
object operation width    = recorded object EC width k + m

namespace collection size may grow beyond R
blob collection size may grow beyond k + m
```

Adding nodes increases aggregate capacity, placement choices, repair targets,
and concurrency. It does not increase the number of participants contacted by
one existing inode or object operation.

## Terminology

### Fragment

A fragment is a logical access and namespace boundary. It behaves like a
bucket or virtual storage space, not like a physical server pool.

Each fragment has:

- a stable fragment ID;
- a mutable human alias;
- one stable root directory inode;
- one reserved trash directory inode;
- authentication and policy state;
- namespace entries that may point to objects on any eligible blob nodes.

A fragment is never full merely because one physical node or historical
placement is full. New object placements use capacity available in the blob
node collection.

### Namespace node

A namespace node stores replicas of directory inodes and their dentries. It
does not need to store object shards. Namespace nodes form one incrementally
expandable collection.

### Blob node

A blob node stores immutable shards, parity, and byte-identical `.ncdx`
records for the objects whose layouts selected it. Blob nodes form a separate,
incrementally expandable collection.

### Directory inode

A directory inode is the stable identity of one directory. Its name exists
only as a dentry in its parent. Renaming or moving the directory therefore does
not change its identity or descendants.

### Dentry

A dentry maps one name inside a parent directory inode to one target:

```text
(parent inode ID, name)
  -> child directory inode reference
  or
  -> immutable object reference
  or
  -> symbolic-link reference
```

### Object layout

An object layout is the immutable physical truth for one object identity and
version: its EC shape, exact node and disk holders, part and stripe metadata,
checksums, and shard roles.

## Absolute Minimum And Production Topologies

### Absolute minimum: 2N + 2B

The lowest supported logical topology is:

```text
namespace nodes:       2
namespace redundancy:  2

blob nodes:             2
object EC:              1 data + 1 parity
```

```text
N2 cluster
├── namespace collection
│   ├── N1
│   └── N2
└── blob collection
    ├── B1
    └── B2
```

Namespace redundancy two is strict synchronous mirroring:

- every mutation targets both replicas;
- a mutation commits only after both replicas durably accept the same result;
- loss, disagreement, or isolation of either replica stops namespace writes;
- one surviving replica may serve only a generation carrying valid 2-of-2
  commit evidence;
- automatic membership replacement is forbidden until the missing identity is
  restored or conclusively fenced.

Object EC `1 + 1` tolerates the loss of either stored shard for existing reads.
It must not accept new durable object writes when two distinct writable blob
nodes are unavailable.

With physically dedicated roles, the absolute minimum is four physical nodes.
For architectural testing, two namespace processes may share one physical
machine and two blob processes may share another. That proves protocol and
restart behavior, not four independent physical failure domains.

### Recommended production minimum

The recommended production starting point is:

```text
namespace nodes:       3
namespace redundancy:  3

blob nodes:             at least k + m
recommended baseline:  3 nodes for object EC 2 + 1
```

With three namespace replicas, a 2-of-3 quorum can continue safe mutations
after one namespace node fails and can autonomously rebuild the missing
replica after fencing it. Production replicas and EC shards should be placed
across distinct host, rack, power, and network failure domains when those
labels are available.

## Static Width, Incremental Expansion

Namespace redundancy and object EC width are independent static policies:

```text
namespace_redundancy = R
object_ec             = k + m
```

The collection sizes are independent of those widths:

```text
namespace_node_count >= R
blob_node_count      >= k + m
```

Expansion is always incremental:

```text
namespace: N1,N2       -> add N3 -> add N4 -> add N5
blob:      B1,B2,B3    -> add B4 -> add B5 -> add B6
```

Adding a node does not silently change `R` or `k + m`. Existing inode and
object layouts remain valid. New directory inodes and new objects may use the
new node immediately.

Namespace redundancy is an immutable cluster contract. Once any N2 root is
enrolled, changing `namespace_redundancy` is rejected. Object EC policy remains
an independent object-level contract: old object layouts remain valid under
their recorded widths unless an operator explicitly requests re-encoding.

## Node Roles

N2 2.0 separates namespace and blob roles for predictable IO and failure
behavior:

```text
namespace node
  -> namespace roots only

blob node
  -> blob roots and upload staging only
```

A production node process belongs to one collection. A laboratory may run
multiple isolated logical nodes on one physical host, but every logical node
must have its own node ID, process, ports, roots, logs, and runtime state.

All client-facing N2 nodes use the same cluster authentication and NOA
contract. Namespace and blob roles are internal storage responsibilities, not
separate user authentication domains.

## Bidirectional Coordination

N2 2.0 remains bidirectional. Storage role controls local persistence; it does
not restrict operation coordination.

Every healthy namespace node and every healthy blob node runs the common N2
authentication, routing, peer-client, and operation-coordinator capabilities.
Any one of them may receive a client request:

```text
upload received by namespace node
  -> resolve destination inode
  -> select and stream to blob holders
  -> publish namespace reference

rename received by blob node
  -> resolve recorded inode groups
  -> coordinate bounded namespace mutation

download received by namespace or blob node
  -> resolve namespace object reference
  -> read exact recorded blob holders
  -> stream result to client
```

The receiving node is coordinator only for that operation. It is never a
permanent cluster leader, namespace owner, object owner, or placement source of
truth. Authority remains with the recorded inode replica groups and immutable
object layouts.

A deployment may expose only selected nodes through a load balancer or public
network, but that is an operational boundary rather than an engine-role
restriction. Internally, any reachable healthy N2 node can coordinate.

Multipart sessions may remain temporarily owned by the node that initiated
them. The session records that coordinator identity so the client or another
N2 endpoint can route resume, finalize, or cancel correctly. A finalized object
has no permanent coordinator dependency.

Bidirectionality does not permit unsafe fallback. A coordinator that cannot
reach the required namespace quorum or object durability set rejects or routes
the request; it never invents authority from its local storage role.

## Failure-Domain Placement

Placement always chooses distinct nodes before choosing disks:

```text
directory inode replica placement
  -> choose R distinct namespace nodes
  -> choose one namespace root on each selected node

object stripe placement
  -> choose k + m distinct blob nodes
  -> choose one blob root on each selected node
```

Two disks in one node never count as two namespace replicas or two shards of
one EC stripe. Adding a disk increases capacity and local parallelism; adding a
node adds another distributed failure domain.

Node IDs and storage-root IDs are stable placement identities. Mount paths are
operational configuration and are not durability truth.

## Namespace Disk Layout

A namespace node may contain many namespace disks. Different directory inodes
are distributed across those disks, but one replica of one inode lives on one
root at a time.

```text
N1
├── N1DISK001
├── N1DISK002
├── ...
└── N1DISK100

N2
├── N2DISK001
├── ...
└── N2DISK100
```

Example directory placements with redundancy three:

```text
@i_root
├── N1 / N1DISK002
├── N2 / N2DISK019
└── N3 / N3DISK071

@i_video
├── N1 / N1DISK017
├── N2 / N2DISK063
└── N3 / N3DISK042

@i_images
├── N1 / N1DISK081
├── N2 / N2DISK004
└── N3 / N3DISK095
```

The target namespace-root format is:

```text
<namespace_root>/
  .n2-root.ncdx
  .fragment/
    <fragment-alias-hash> -> ../@f_<stable-fragment-id>

  @f_<stable-fragment-id>/
    .namespace/
      @i_<directory-id>/
        .inode
        <encoded-entry-name>.nref
```

`.n2-root.ncdx` records the stable root identity and role. `.inode` stores the
directory's stable identity, fragment identity, parent identity, replica
layout generation, and committed namespace state. Each `.nref` is a dentry
record. The representation may use filesystem-safe encoded names while always
preserving the original logical name inside the record.

All dentries belonging directly to one directory inode are colocated with that
inode replica. The operating-system filesystem provides local directory
indexing and atomic local rename primitives. N2 provides cross-node agreement.

## Blob Disk Layout

Blob roots preserve N2's immutable object layout:

```text
<blob_root>/
  .n2-root.ncdx
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
                <version-ulid>.l<layout-generation>.<size>.<unix-nanos>.<packed-ext>.ncdx
                <version-ulid>/
                  part_0_shard_0.shard
                  part_0_shard_1.shard
                  part_0_shard_2.parity
```

Object fanout is derived from immutable object identity. Every shard-bearing
holder stores its shard or parity plus the same named-MessagePack `.ncdx`
record for that committed version.

The `l<layout-generation>` filename component permits holder replacement to
publish a new immutable placement record without changing the logical content
version or editing an earlier `.ncdx` in place.

## Fragment Catalog And Root Inodes

The cluster fragment catalog is small namespace state replicated with the
configured namespace redundancy. It maps a fragment alias and stable fragment
ID to a root inode reference:

```text
fragment: videos
stable_fragment_id: @f_01...
root:
  inode_id: @i_root
  layout_generation: 7
  replicas:
    - N1 / N1DISK002
    - N2 / N2DISK019
    - N3 / N3DISK071
```

The catalog is not an object database. Ordinary path traversal leaves the
catalog after resolving the fragment root and follows inode pointers from
parent to child.

Fragment rename changes the alias mapping only. It does not change the stable
fragment ID, directory inodes, object IDs, `.ncdx`, or shards.

## Distributed Inode Model

### Stable directory identity

The logical tree is:

```text
fragment root @i_root
├── "video"  -> @i_video
└── "images" -> @i_images

@i_video
└── "video.mp4" -> @o_video_mp4
```

The directory name belongs to its parent dentry. `@i_video` remains unchanged
when `video` is renamed, moved, trashed, or restored.

Every directory reference contains:

- target kind;
- stable directory inode ID;
- inode layout generation;
- exact namespace node and root holders;
- target metadata needed for validation.

Exact recorded holders eliminate current-membership guessing. Adding a
namespace node cannot silently remap an existing inode.

### Object references

A file dentry points to stable object identity and an immutable layout
reference:

```text
type: object
object_id: @o_92af
version: 01K...
layout_generation: 4
ec: 2 + 1
holders:
  - B1 / B1DISK005 / data-0
  - B2 / B2DISK041 / data-1
  - B3 / B3DISK012 / parity
```

The dentry contains enough locator information to contact the bounded object
holder set. The byte-identical `.ncdx` records on those holders remain the
authoritative immutable body and must agree with the namespace locator before
bytes are served.

N2 2.0 does not expose public hard links. One live or trash dentry owns one
physical object identity. Symbolic links remain namespace references. This
keeps object reclamation independent of global reference counting.

### Parent identity

Every directory inode records its current parent inode ID. Directory movement
updates the source parent, destination parent, and moved inode parent record in
one bounded mutation. This provides a stable guard against concurrent cycles
without rewriting descendants.

The fragment root has no parent. The reserved trash inode is rooted directly
by the fragment.

## Namespace Placement And Expansion

At directory creation, N2:

1. creates a stable directory inode ID;
2. selects `R` distinct healthy namespace nodes;
3. selects one writable namespace root on each node;
4. writes the identical empty inode replica to all selected roots;
5. verifies the required durable commit;
6. publishes the child pointer in the parent inode.

With exactly `R` namespace nodes, every inode uses all of them. With more than
`R`, different directory inodes use different combinations:

```text
R = 3

@i_music   -> N1,N2,N4
@i_users   -> N1,N3,N4
@i_reports -> N2,N3,N4
```

Adding a node does not relocate healthy existing inodes. The new node is used
for new directory placements and as a repair target. Explicit whole-inode
movement may later redistribute a hot or capacity-constrained replica, but it
must never rewrite descendants or object data.

Adding a namespace disk makes it eligible for new inode replicas on that node.
It does not create another replica failure domain.

## Namespace Consistency

Namespace is the only mutable user-visible storage plane. It uses bounded,
per-inode agreement and never a global cluster writer.

For inode redundancy `R`, the default write quorum is:

```text
Qwrite = floor(R / 2) + 1
```

For `R = 2`, `Qwrite = 2`. For `R = 3`, `Qwrite = 2`. Reads use a quorum that
intersects every write quorum and select the highest valid committed
generation.

Every mutation carries:

- stable mutation ID;
- affected inode IDs;
- expected inode/dentry generations;
- normalized operation and operands;
- target generation;
- replica-layout generations.

Every replica validates the same preconditions. A generation becomes visible
only with durable quorum commit evidence. Prepared but uncertified mutations
are not namespace truth. Repeating a mutation ID is idempotent.

This is optimistic, bounded coordination:

- no global namespace mutex;
- no global writer;
- no cluster-wide transaction log;
- unrelated inode groups mutate concurrently;
- conflicting mutations on the same inode generation cannot both commit.

### Single-directory operations

Create, unlink, same-parent rename, and dentry replacement touch one directory
inode replica group. Local replicas use atomic filesystem operations; the
replica group supplies distributed durability and generation agreement.

### Cross-directory moves

A cross-directory move touches only:

- the source parent inode group;
- the destination parent inode group;
- the moved directory inode group when the target is a directory.

Participant groups are prepared in canonical inode-ID order. One mutation ID
and one durable decision bind the groups. Commit and replay are idempotent.
There is no descendant scan and no cluster-wide transaction.

### Cycle prevention

Before a directory move, N2 validates the destination parent chain. The moved
directory's parent record and destination directory generation participate in
the same conditional mutation. Concurrent inverse or multi-directory cycle
attempts therefore overlap on guarded inode generations; at most one can
commit.

### Listing and lookup

Path lookup begins at the fragment root, reads each directory reference, and
contacts only that inode's recorded replicas. Depth is user-controlled; a
shallow namespace approaches flat-key lookup cost.

Directory listing reads a quorum-consistent committed view of one inode and is
paginated. Listing `K` entries is necessarily `O(K)` in returned work. N2 does
not broadcast a lookup or list to the entire namespace collection.

## POSIX-Like Namespace Semantics

N2 2.0 provides namespace semantics for:

- directory creation;
- object creation and replacement;
- stable directory identity;
- same-directory rename;
- cross-directory move;
- directory rename and move without descendant rewriting;
- symbolic links;
- soft delete and recovery;
- paginated listing and stat;
- atomic collision rejection and conditional mutation.

Rename and move are constant with respect to descendant count. They are
bounded by the replica groups of the directly affected inodes.

Cross-fragment movement is copy/delete rather than one atomic rename. Public
hard links are excluded from the 2.0 contract so lifecycle never requires a
cluster-wide object reference count.

## Upload And Publication

An upload is published in durability order:

```text
authenticate fragment
-> resolve destination parent inode
-> choose k + m distinct blob nodes
-> choose one blob root per node
-> stream and encode object parts
-> durably publish shards/parity
-> durably publish identical immutable .ncdx records
-> commit the object reference in the parent inode group
-> acknowledge success
```

Namespace never points to incomplete blob data. A crash before namespace
publication may leave an unreachable upload or immutable object candidate;
session cleanup reclaims it after its safety window.

Multipart coordinator state is temporary operational state, not namespace or
object truth. Finalized objects no longer depend on their upload coordinator.

## Read Path

```text
client read
-> authenticate fragment
-> resolve root inode from fragment catalog
-> traverse recorded directory inode replicas
-> resolve object reference and exact holder layout
-> validate matching immutable .ncdx
-> read required data shards concurrently
-> reconstruct from parity when required
-> schedule recover-on-read after serving reconstructable bytes
```

A healthy read fetches the required data shards first. Parity is fetched only
for validation, degraded reconstruction, or configured scrub work. One object
never broadcasts reads to the complete blob-node collection.

## Object-Level Erasure Sets

Every object independently selects `k + m` distinct blob nodes from the
current eligible collection. Another object may select a different set.

```text
blob nodes: B1,B2,B3,B4,B5
object EC:  2 + 1

object A -> B1,B2,B4
object B -> B2,B3,B5
object C -> B1,B4,B5
```

Every object records its resolved layout. The current blob-node membership is
never used to guess an old object's holders.

Adding B6 means:

- existing object layouts remain valid;
- no healthy object becomes degraded;
- no automatic redistribution or re-encoding starts;
- new objects may select B6;
- repair may use B6 when publishing a safe replacement layout.

An older recorded EC shape is not degradation. Health and topology age are
separate facts.

## Immutable `.ncdx` Contract

Each committed `.ncdx` stores:

- stable object and fragment identities;
- immutable object version;
- object owner and content attributes;
- logical size and creation time;
- data and parity counts;
- stripe and part metadata;
- exact node and root holders;
- shard roles, lengths, and checksums;
- immutable layout generation identity.

It never stores a mutable user path. Folder and file rename never rewrite
`.ncdx`.

When a permanently lost holder must be replaced, N2 publishes a new immutable
layout generation after the replacement shard and matching metadata are
durable. The prior `.ncdx` generation is never edited in place and is retained
until the namespace/object commit makes the replacement authoritative and
restart-safe.

## Autonomous Healing

Namespace and blob maintenance remain independent.

### Namespace healing

For each recorded inode layout, healing:

1. contacts only the recorded replica group;
2. selects an identical quorum snapshot or the unique highest generation with
   a valid recorded commit certificate;
3. rejects divergent uncertified state;
4. restores the complete inode header and committed dentry set to its exact
   recorded node/root;
5. verifies the rebuilt replica;
6. never changes replica width or redistributes a healthy inode.

With redundancy two, one surviving complete snapshot can restore its missing
recorded peer only when its last generation carries the original two-voter
commit certificate. It cannot silently choose a different logical holder. A
permanently replaced holder must first be fenced and re-admitted under the
recorded logical identity. If authority or the target topology cannot be
proved, the cluster fails closed in durable read-only mode.

### Object healing

Object healing reads the immutable recorded layout, verifies holder
availability and checksums, reconstructs only missing or corrupt shards, and
restores missing collocated `.ncdx` metadata replicas. It never re-encodes
healthy objects merely because nodes were added.

Every degraded directory inode or object produces one node-local immutable
JSON report containing the initial condition and repair outcome. A published
object without a live or trash namespace reference is reported as headless and
preserved. A namespace pointer whose target no longer exists is reported as
dangling and preserved. Reports are disposable operational evidence and are
never replicated.

An unrepairable inode or object, dangling namespace pointer, divergent
immutable record, or impossible recorded topology activates the durable
cluster safety latch. User mutations, GC, and redistribution stop;
reconstructable reads and healing remain allowed. A physical object without a
namespace reference is reported and preserved, but it does not by itself trip
the latch because durable blob publication intentionally precedes namespace
publication and a failed finalize can leave safe orphan residue. If namespace
traversal or blob inventory is incomplete, no headless conclusion is made.

Recover-on-read serves reconstructable bytes first and schedules bounded
repair in the background.

### Maintenance concurrency

Maintenance is bounded by plane, node, disk, fragment, inode, and object. It
must not create a cluster-wide scan storm. Independent inodes and objects may
be checked and repaired concurrently while foreground IO continues.

## Trash, TTL, And Reclamation

Every fragment has one reserved trash inode. Delete is a namespace pointer
move:

```text
live parent/name -> target
                becomes
trash/<stable-trash-name> -> target
```

Directory deletion moves only the directory pointer. Descendants remain
attached to the stable directory inode. Restore moves the pointer back after
validating the original parent and collision state.

TTL is recorded on the trash entry. GC scans only trash entries for normal
lifecycle reclamation:

1. ignore entries whose TTL has not expired;
2. for an expired object entry, reclaim its exact immutable object layout;
3. for an expired directory entry, traverse that inode subtree;
4. reclaim object shards and `.ncdx` from their recorded holders;
5. remove child dentries and directory inodes bottom-up;
6. remove the trash anchor last.

The trash anchor is the restart-proof source of truth. GC never destroys the
anchor before all reachable work is complete.

If a child was trashed before its former parent, it is already a separate
trash anchor and is not part of the later parent subtree. Each anchor keeps its
own TTL and is reclaimed independently.

Upload-session cleanup and low-frequency orphan scrub are separate maintenance
jobs. Normal TTL GC must not scan every blob root looking for headless objects.

## Expansion, Replacement, And Removal

### Adding a namespace node

1. assign a stable namespace-node ID;
2. register its namespace roots and failure-domain labels;
3. verify health and writable reserve;
4. admit it to the eligible namespace collection;
5. use it for new inode layouts and repairs.

Existing inodes do not move automatically.

### Adding a blob node

1. assign a stable blob-node ID;
2. register its blob roots and failure-domain labels;
3. verify health and writable reserve;
4. admit it to the eligible blob collection;
5. use it for new object layouts and repairs.

Existing objects do not move automatically.

### Adding a disk

A disk receives a stable root identity and inherits its node's role. It is
eligible for new placements on that node. Adding a disk does not change inode
redundancy, object EC, or failure tolerance.

### Removing a node or disk

Removal is never equivalent to deleting it from configuration. N2 must first
enumerate every recorded inode replica or object shard that depends on the
identity, create verified replacements, publish authoritative layout
generations, and then fence the old identity. Unsafe removal is rejected.

## Read-Only And Failure Semantics

Ordinary quorum or holder loss fails closed at the smallest affected scope:

- an inode group without a safe write quorum rejects mutations involving that
  inode;
- a fragment root without safe namespace truth makes that fragment read-only
  or unavailable;
- insufficient distinct blob nodes for `k + m` rejects new object writes;
- a reconstructable degraded object remains readable;
- an unreconstructable object is reported explicitly and never fabricated;
- unrelated healthy fragments, inode groups, and objects continue operating.

Auto-healing escalates a confirmed catastrophic invariant to the durable
cluster safety latch. Examples are an inode whose authority cannot be proved,
a namespace pointer with no surviving target, a headless published object, an
unrecoverable object, or an impossible recorded layout. This stronger response
prevents further user mutations and reclamation from enlarging damage while
reads and exact-layout healing remain available.

Node addition never marks anything degraded. Degradation is determined only
from the recorded layout of the inode or object being evaluated.

## Concurrency Model

N2 is fully concurrent across independent work, not broadcast-oriented:

```text
one namespace operation -> one to three bounded inode replica groups
one object operation    -> one bounded k + m object set
many operations         -> distributed across all eligible nodes and disks
```

All of these may proceed simultaneously:

- different fragments;
- different directories;
- independent inode mutations;
- different objects;
- different multipart sessions;
- different disks within each selected node;
- bounded namespace and blob healing.

Operations that conflict on the exact same inode or dentry generation must be
ordered. That is required filesystem semantics, not a global lock. No unrelated
operation waits for that conflict.

## Capacity And Operational Reserves

Each root reserves space and filesystem inodes for namespace decisions,
temporary publication, healing, GC, and recovery. Blob payload admission must
stop before consuming operational reserve. A full data root must never prevent
delete, rename, repair metadata, or safe shutdown.

Capacity selection considers:

- free bytes and filesystem inodes;
- observed IO latency and stalls;
- node and disk health;
- failure-domain diversity;
- current foreground and maintenance pressure.

Capacity scoring affects new placement only. It does not redefine existing
layout truth.

## Honest Scaling Boundaries

Aggregate namespace capacity and throughput grow as namespace nodes and disks
are added while each inode keeps fixed redundancy `R`. Aggregate blob capacity
and throughput grow as blob nodes and disks are added while each object keeps
fixed EC width `k + m`.

A single directory inode is still bounded by its recorded replica nodes and
the selected disk on each node. One directory containing an extreme number of
direct children may become hot even when the cluster has unused namespace
capacity elsewhere. N2 2.0 initially treats one directory as one placement
unit. Dynamic directory splitting must not be added until measured workloads
prove it necessary and its invariants are separately specified.

This is architectural horizontal scalability, not a claim that finite
hardware has infinite capacity or that correlated failures cannot occur.

## NOA And Client Boundary

NOA remains the public object and namespace contract. Clients authenticate to
the cluster and fragment, not separately to namespace or blob nodes.

Any healthy API endpoint may coordinate a request. The receiving node is an
operation coordinator only; it does not become permanent namespace, object,
or cluster truth.

The N2 2.0 engine must preserve common NOA meanings for:

- fragment creation and authentication;
- object upload, download, stat, versioning, and delete;
- directory list, mkdir, rename, move, trash, and recover;
- health, repair, GC, and topology reporting.

Engine-specific inode and object placement remain internal.

## Non-Goals

N2 2.0 does not introduce:

- a SQL or external metadata database;
- a global namespace writer;
- a cluster-wide mutable object head;
- complete namespace replication to every namespace node after the collection
  grows beyond the recorded inode redundancy;
- public hard links and global object reference counting;
- automatic healthy-object redistribution after node addition;
- automatic rewriting of old EC layouts;
- atomic rename across fragments;
- a compatibility layer for earlier development disk layouts.

## Implementation Plan

The redesign must be implemented in isolated stages. Blob invariants are not
to be rewritten while namespace work is incomplete.

### Phase 1: architecture types and configuration

- introduce explicit namespace-node and blob-node roles;
- define stable node and root identities;
- define namespace redundancy separately from object EC;
- enforce the `2N + 2B` absolute minimum and fail-closed rules;
- reject roots shared by different logical nodes.

### Phase 2: namespace disk store

- implement fragment catalog and root inode references;
- implement directory inode and dentry record formats;
- implement exact replica-layout references;
- implement atomic local publication using filesystem primitives;
- implement disk inventory and capacity-safe inode placement.

### Phase 3: replicated inode protocol

- implement quorum-certified generations;
- implement idempotent mutation IDs;
- implement single-directory create, replace, rename, and unlink;
- implement strict 2-of-2 behavior;
- implement majority behavior for redundancy three and above;
- implement restart replay without global locks.

### Phase 4: cross-inode POSIX operations

- implement bounded cross-parent move;
- include moved-directory parent state in the transaction;
- prevent concurrent cycles;
- implement collision and stale-generation rejection;
- prove that descendants and blob bytes remain untouched.

### Phase 5: object publication integration

- retain immutable object-level EC placement;
- publish namespace object references only after durable `.ncdx` and shards;
- implement idempotent finalize and orphan-safe failure behavior;
- verify exact node and root locators on read.

### Phase 6: trash and lifecycle

- implement direct pointer move into the reserved trash inode;
- implement reverse restore;
- implement TTL from trash-entry state;
- reclaim recorded object layouts and remove trash anchors last;
- keep session cleanup and orphan scrub separate.

### Phase 7: autonomous healing and expansion

- repair missing namespace replicas from certified complete snapshots;
- repair missing object shards and `.ncdx` replicas from immutable recorded layouts;
- emit node-local non-replicated JSON findings and fail closed on catastrophic invariants;
- add namespace and blob nodes one at a time;
- add disks one at a time;
- prove that expansion never remaps or degrades existing layouts;
- enforce safe drain and fencing before removal.

### Phase 8: API, console, and operational integration

- expose the redesign through the existing NOA meanings;
- update N2 Console topology, namespace, object, and health views;
- report namespace redundancy independently from object EC;
- report affected-scope read-only state precisely;
- remove obsolete 1.x namespace controls and terminology.

## Verification Requirements

N2 2.0 must not be called stable merely because it compiles or passes synthetic
happy-path tests. Verification must preserve evidence for every invariant.

### Deterministic namespace tests

- create and list across all namespace replica combinations;
- same-parent rename under concurrency;
- cross-parent file and directory moves;
- concurrent inverse and multi-directory cycle attempts;
- conflicting create, delete, trash, and restore;
- duplicate mutation replay;
- crash after prepare, decision, and partial apply;
- stale replica recovery;
- namespace disk and node replacement;
- strict 2-of-2 read-only behavior;
- 2-of-3 continuation and autonomous repair;
- node addition without existing-inode remap.

### Deterministic object tests

- `1 + 1`, `2 + 1`, and configured wider EC layouts;
- exact holder and disk recording;
- shard and parity corruption;
- recover-on-read;
- restart during upload and finalize;
- holder replacement through immutable layout generation;
- blob-node and disk addition without redistribution;
- object status based on recorded layout rather than current topology.

### Lifecycle tests

- object trash, restore, TTL expiry, and reclaim;
- directory trash with nested objects;
- child trashed before parent;
- interrupted subtree reclamation and restart;
- partial object-holder reclaim and retry;
- no live or restorable object reclaimed;
- orphan scrub isolated from normal TTL GC.

### Real topology tests

The minimum architectural test topology is:

```text
physical machine A
├── simulated namespace N1
└── simulated namespace N2

physical machine B
├── simulated blob B1
└── simulated blob B2
```

Each logical node must have isolated roots, processes, ports, and state. This
test proves the minimum protocol but not independent physical-node durability.

The same NOA operation matrix must be executed through every node role:

- upload through each namespace node and each blob node;
- download through each namespace node and each blob node;
- namespace mutation through each namespace node and each blob node;
- multipart resume through its recorded coordinator;
- coordinator loss before and after final publication;
- routing rejection when required namespace or blob authority is unavailable.

Production proof must use independent physical failure domains for namespace
replicas and object shards.

### Performance evidence

Every release proof records:

- operation latency in milliseconds by phase;
- namespace RPC and durable-sync counts;
- object network and disk bytes;
- CPU time and cycles where supported;
- memory, threads, descriptors, and queues;
- foreground versus maintenance IO;
- number of inode groups, nodes, and disks touched per operation;
- repair and GC work amplification;
- behavior under node, disk, and network failure.

The implementation is rejected if a namespace mutation unexpectedly scans a
subtree, contacts the complete collection, rewrites object data, or introduces
a global writer.

## Locked N2 2.0 Invariants

1. Namespace nodes and blob nodes are independently expandable collections.
2. One node or disk may be added at a time after the minimum is met.
3. Namespace redundancy and object EC width remain independent.
4. Directory identity is stable and inode-based.
5. Directory names exist only in parent dentries.
6. Directory rename and move never rewrite descendants.
7. Namespace work is bounded by directly affected inode replica groups.
8. Object work is bounded by that object's recorded `k + m` set.
9. Shards, parity, and committed `.ncdx` records are immutable.
10. Every layout records exact node and disk-root identities.
11. Adding nodes never degrades or remaps existing healthy layouts.
12. No healthy object is automatically redistributed after expansion.
13. Namespace publishes an object pointer only after blob durability.
14. Trash anchors are removed only after complete safe reclamation.
15. Public hard links and global object reference counting are excluded.
16. Conflicting inode mutations cannot both commit.
17. Unrelated inodes and objects operate concurrently without a global lock.
18. Unsafe topology fails closed; a confirmed catastrophic invariant activates
    the durable cluster safety read-only latch.
19. Existing layout truth is never inferred from current membership.
20. N2 2.0 carries no in-engine backward-compatibility path for earlier
    development layouts.
21. Every healthy namespace or blob node may coordinate any supported client
    operation without becoming permanent authority.

These invariants define the N2 2.0 engine. Implementation choices are
valid only when they preserve every invariant above.
