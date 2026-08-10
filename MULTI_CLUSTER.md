# N2 Geographic Multi-Cluster Architecture

## Status

This document records the proposed architecture for a future geographic
multi-cluster generation of N2. It is not implemented by N2 3.4, is not a
production claim, and does not change the locked single-cluster storage laws
in [ARCHITECTURE.md](ARCHITECTURE.md).

The regional design is intentionally documented before implementation so the
single-cluster engine can mature behind a stable extension boundary. The
multi-cluster design may be revised repeatedly before that boundary is locked.
Any undecided rule is marked as such; an unresolved conflict rule must never be
silently converted into implementation behavior.

The first implementation must be additive and isolated under N2's `cluster`
module. It must consume committed single-cluster objects and namespace state
through narrow internal interfaces. It must not insert geographic coordination
into ordinary PUT, GET, LIST, rename, trash, healing, placement, or server-pool
operations.

The purpose is to connect independent N2 clusters across geographic failure
domains, including a future active-active mode, while preserving the defining
properties of N2:

- immutable object versions;
- object-local, recorded EC layouts;
- flat NCNX namespace authority;
- lockless independent-key operation;
- static, append-only server pools;
- autonomous exact-layout healing;
- bounded work and explicit failure behavior.

## Design Objective

Multi-cluster N2 provides:

1. asynchronous geographic replication of committed objects and namespace
   state;
2. fragment-scoped active-active replication between explicitly configured
   writer clusters;
3. optional read-only disaster-recovery replicas that use their own local N2
   durability;
4. restart-proof replication with bounded retries and anti-entropy repair;
5. planned migration and explicitly fenced changes to fragment writer
   membership;
6. independent cluster operation during WAN loss;
7. observable recovery-point and replication-lag guarantees.

It does not create one stretched EC pool across regions. Each cluster remains
a complete N2 system and failure domain.

```text
                              Global routing layer
                    DNS / load balancer / application router
                                      |
                 +--------------------+--------------------+
                 |                    |                    |
          Philippine region     Singapore region       Japan region
             cluster PH            cluster SG           cluster JP
                 |                    |                    |
        +--------v---------+  +-------v----------+  +------v-----------+
        | local pools      |  | local pools      |  | local pools      |
        | local EC 6 + 2   |  | local EC 4 + 2   |  | local EC 3 + 2   |
        | local reserves   |  | local reserves   |  | local reserves   |
        +--------+---------+  +-------+----------+  +------+-----------+
                 |                    |                    |
                 +----- committed logical versions -------+

fragment hospital-records: PH <-> SG active-active, JP read-only DR
fragment radiology:        PH <-> SG active-active
fragment research:         SG -> JP read-only replication
fragment local-backups:    PH only
```

## Architectural Laws

1. **A cluster is the geographic failure boundary.** EC shards, NCDX copies,
   NCNX replicas, logical pool slots, and Reserve Sets never span clusters.
2. **Every fragment has an explicit regional write policy.** A fragment may be
   local-only, single-authority with read-only replicas, or active-active across
   an explicit writer set. No cluster may infer write authority from peer
   unavailability.
3. **WAN replication is asynchronous.** Loss of a remote cluster or WAN link
   does not stop safe local writes on any configured writer cluster.
4. **Replication starts only after local publication succeeds.** Temporary
   uploads and uncommitted shards are never replicated.
5. **A remote copy is a new local physical layout of the same logical object
   version.** The destination uses its own server pools and EC contract.
6. **Object, version, fragment, and mutation identities are preserved across
   clusters.** Physical holder and root identities are never preserved across
   clusters.
7. **Replication is idempotent.** Duplicate delivery, retry, restart, and
   concurrent reconciliation must converge without creating duplicate logical
   versions.
8. **Replication events are reconciliation hints, not storage authority.** The
   committed NCNX mutation set and immutable versions remain the source of
   truth. Lost hints are repaired by anti-entropy.
9. **Event arrival order and wall-clock order are not trusted.** Applying a
   hint compares stable mutation identities and the currently observed exact-key
   state. Delayed delivery cannot roll back a converged key.
10. **Namespace publication remains destination-first.** Replicated rename,
    trash, and recover operations publish the destination state before removing
    obsolete replica state.
11. **No geographic single writer is inserted into the local data path.**
    Replication workers may duplicate work because the protocol is idempotent.
12. **Writer membership changes require a durable policy generation and
    fencing.** A removed writer cannot continue publishing accepted mutations
    after reconnecting.
13. **A stale policy generation can never publish.** Reconnected clusters load
    the newer signed fragment replication manifest before accepting mutations.
14. **Local healing precedes geographic assumptions.** A cluster repairs its
    own exact layouts; geographic replication does not replace local EC,
    parity, or Reserve Sets.
15. **Foreground storage has priority.** Replication is bounded, rate-limited,
    and subordinate to client IO and critical local healing.
16. **Concurrent exact-key mutations preserve evidence.** Active-active
    conflict resolution may select one visible HEAD, but it must not destroy a
    separately committed immutable version or tombstone merely because another
    site won visibility.
17. **The same observed mutation set produces the same visible state.** HEAD,
    live, trash, and absence resolution must converge deterministically on every
    cluster without consulting connection order or local arrival order.

## Terminology

| Term | Meaning |
| --- | --- |
| Federation | A configured collection of independent N2 clusters. |
| Region or site | The operational location of one independent N2 cluster. A site never means a WAN-spanning server pool. |
| Cluster ID | Stable globally unique identity of one N2 cluster. |
| Writer cluster | A cluster explicitly allowed to accept mutations for a fragment in the current policy generation. |
| Replica cluster | A cluster holding a locally durable read-only copy of a replicated fragment. |
| Policy generation | Monotonically increasing fragment replication-policy generation used to fence stale writers and configurations. |
| Fragment replication manifest | Signed record defining a fragment's writer set, replica set, mode, and conflict-policy identity. |
| Replication rule | Source, destination, fragment selection, retention, bandwidth, and required-ack policy. |
| Mutation identity | Stable globally unique identity for one committed namespace mutation. Retries preserve it. |
| Reconcile hint | Durable or recoverable notification that an exact key or fragment control record must be compared with its replication peers. |
| Anti-entropy | Bounded comparison that discovers missing or divergent replica state without relying on the hint stream. |
| RPO | Maximum accepted age or volume of acknowledged local changes not yet durable on a required replica. |

## Decision Status

The following boundaries are architectural requirements for any future
implementation:

- clusters remain independent local N2 systems;
- EC shards, server pools, disk identities, and Reserve Sets never cross the
  WAN;
- only committed logical versions and namespace mutations replicate;
- destinations re-encode with their own local EC topology;
- replication is asynchronous and cannot block ordinary local writes because
  a peer or WAN link is unavailable;
- fragment replication is the data mechanism;
- site replication is policy that enrolls eligible fragments into that same
  mechanism;
- duplicate, delayed, reordered, and replayed mutations are safe;
- bounded anti-entropy can reconstruct correctness without a global database.

The following details remain proposals and must be locked before code:

- the exact deterministic HEAD ordering for concurrent same-key mutations;
- delete-versus-overwrite and recover-versus-overwrite precedence;
- whether the first active-active release exposes conflicting siblings or only
  exposes the deterministic winner while retaining siblings by version ID;
- cross-key operation grouping for rename and move observability;
- whether site replication includes authentication and policy configuration in
  its first release or only fragment data and namespace state;
- the exact write-routing and read-after-write behavior while regions are
  converging.

## Single-Cluster Maturity Isolation

Geographic replication must extend N2 without weakening or reinterpreting the
single-cluster engine that has already been tested. This is a hard compatibility
boundary, not merely a source-code organization preference.

When multi-cluster support is disabled:

- PUT, GET, HEAD, LIST, rename, move, trash, recover, healing,
  redistribution, Reserve Set promotion, and server-pool behavior remain
  identical to ordinary single-cluster N2;
- the engine performs no WAN lookup, remote acknowledgement, federation
  routing, or geographic conflict resolution;
- local NCNX, NCDX, EC, lifecycle, and publication formats retain their
  single-cluster meanings;
- a cluster does not need a reachable federation service to start, heal, or
  serve its local fragments.

When multi-cluster support is enabled, the extension may observe only
successfully committed local mutations and may submit only verified logical
imports through narrow engine interfaces. It may not directly mutate local
shards, NCDX, NCNX, pool membership, Reserve Set bindings, or healing state.

Single-cluster release evidence remains valid only for the unchanged local
engine. Multi-cluster replication, convergence, routing, fencing, and regional
failure behavior require their own release evidence. Passing the geographic
tests does not permit weakening a local invariant, and passing local tests
does not prove the geographic layer.

## Cluster Identity And Fragment Policy

Every cluster has a stable `cluster_id`, independent TLS identity, replication
signing identity, and local membership. Cluster identity is not a server-pool
ID and is not derived from an IP address or DNS name.

Each replicated fragment has a signed replication manifest:

```text
federation_id
fragment_id
policy_generation
mode = local | single_authority | active_active
writer_cluster_ids[]
replica_cluster_ids[]
local_cluster_mode = writer | replica_read_only | fenced
conflict_policy_id
replication_policy_id
issued_at
manifest_digest
signature
```

The manifest is small control authority. It does not contain object listings,
placement maps, shards, namespace rows, user quotas, or business metadata.

Every mutation is evaluated against the fragment's local replication manifest:

- `writer`: normal NOA and native mutations are allowed;
- `replica_read_only`: GET, HEAD, LIST, and integrity operations are allowed,
  while client writes are rejected with the permitted writer-cluster
  identities;
- `fenced`: client mutations are rejected until authority is explicitly
  resolved.

JWT claims may carry `fragment_id`, preferred cluster, and policy generation as
routing hints. The signed local fragment manifest remains the final write
admission authority; a stale token cannot override it.

## Federation Routing

A federation-aware gateway resolves a fragment before routing an operation.
It is not object or namespace authority and is not required to inspect an
object's NCDX.

```text
client
  -> federation gateway resolves fragment policy
  -> nearest permitted writer for mutations
  -> nearest sufficiently current cluster for permitted reads
```

Routing may use a redirect or proxy mode:

- **redirect mode** returns the signed cluster endpoint so large object bytes
  do not traverse a central gateway;
- **proxy mode** provides a single endpoint when client or network policy
  requires it.

Routing state is bounded by fragment replication manifests. It never contains
a global object index. LIST for one fragment is served by one selected cluster;
the gateway does not merge the same replicated fragment from multiple
clusters during an ordinary request.

If a new tenant or fragment is assigned to another cluster because existing
clusters are full, the federation publishes or updates one fragment manifest.
It does not extend an existing server pool across the WAN.

## Regional Storage Layout

Each regional cluster independently owns its physical layout:

```text
cluster PH, fragment radiology, local EC 6 + 2
├── pool PH-A
│   ├── PH1: shard 0 + local NCDX
│   ├── PH2: shard 1 + local NCDX
│   ├── PH3: shard 2 + local NCDX
│   ├── PH4: shard 3 + local NCDX
│   ├── PH5: shard 4 + local NCDX
│   ├── PH6: shard 5 + local NCDX
│   ├── PH7: parity 0 + local NCDX
│   └── PH8: parity 1 + local NCDX
└── local NCNX replicas and local Reserve Set

cluster SG, same logical fragment, local EC 4 + 2
├── pool SG-A
│   ├── SG1: shard 0 + local NCDX
│   ├── SG2: shard 1 + local NCDX
│   ├── SG3: shard 2 + local NCDX
│   ├── SG4: shard 3 + local NCDX
│   ├── SG5: parity 0 + local NCDX
│   └── SG6: parity 1 + local NCDX
└── local NCNX replicas and local Reserve Set
```

The clusters share logical identity, not physical layout:

```text
fragment_id = radiology
logical_key = patient-001/xray.dcm
object_id   = @o_<stable-object-id>
version_id  = <stable-version-ulid>
mutation_id = <stable-mutation-id>
```

Node IDs, disk roots, logical pool slots, layout generations, shard numbers,
parity numbers, and reserve bindings remain local to one cluster. A geographic
peer cannot use another cluster's NCDX as a placement map.

## Fragment Replication And Site Replication

Fragment replication is the underlying data operation:

```text
fragment radiology
  PH <-> SG
```

Site replication is a policy expansion over fragment replication:

```text
site policy PH <-> SG
├── enroll fragment hospital-records
├── enroll fragment radiology
├── enroll fragment documents
├── enroll fragment media
└── enroll future fragments matching the policy
```

Site replication must not introduce a second object-transfer engine. It
creates, removes, and observes fragment replication relationships. Every
enrolled fragment still uses the same logical-version transfer, destination
import, idempotency, and anti-entropy mechanisms.

The first implementation may intentionally limit site replication to fragment
data, namespace state, and fragment replication policy. Replication of users,
JWT authorities, quotas, or unrelated business configuration is a separate
control-plane decision and is not implied by geographic object replication.

## Replication Unit

The replication unit is a committed logical object version plus the namespace
state that makes it visible.

The source retains:

- stable fragment ID;
- normalized logical key;
- stable object ID;
- immutable version ULID;
- content size and media type;
- end-to-end SHA-256;
- committed NCNX state and mutation identity;
- source cluster ID and fragment policy generation.

The destination creates:

- a local immutable NCDX describing its own destination pool and holders;
- local `K + M` shards using the destination pool's EC contract;
- local `M + 1` NCNX target replicas;
- a replication receipt binding the local layout to the preserved logical
  origin identity.

Source NCDX and shard placement are never copied as destination placement
authority. A `6 + 2` source may replicate into a `4 + 2` destination. Both are
valid independent copies of the same logical version, and each cluster heals
its own recorded layout.

## Replication Envelope

An internal replication request carries a versioned envelope:

```text
protocol_version
federation_id
origin_cluster_id
destination_cluster_id
fragment_id
policy_generation
logical_key
object_id
version_id
mutation_id
parent_mutation_ids[]
operation = publish | trash | recover | remove
namespace_state = live | trash | absent
deleted_at_unix_nanos? 
content_length
content_sha256
content_type?
source_commit_digest
idempotency_key
```

The idempotency key is derived from stable logical identities, never from a
connection, worker, hostname, or retry number.

```text
SHA-256(
  federation_id || origin_cluster_id || fragment_id || policy_generation ||
  object_id || version_id || mutation_id || namespace_state
)
```

Receiving the same envelope again returns the existing verified receipt.
Receiving the same identity with different immutable content or checksum is a
hard conflict: the destination quarantines the request, emits a report, and
does not guess which payload is correct.

`parent_mutation_ids[]` records the exact-key mutation frontier observed by a
writer when it committed a new namespace mutation. It allows a receiver to
distinguish a normal successor from concurrent siblings without relying on
wall-clock time. The exact compact causal encoding remains a design decision,
but the final protocol must have an equivalent way to detect concurrency. A
single optional parent is not sufficient after a writer has observed and
resolved more than one concurrent sibling.

## Commit Protocol

### New object or version

```text
writer cluster publishes object locally
-> replication module emits a reconcile hint
-> destination validates fragment policy generation and identity
-> destination opens a durable internal multipart import
-> source streams the logical object bytes
-> destination computes SHA-256 while locally erasure-encoding
-> destination reaches its local N2 publication threshold
-> destination verifies the final end-to-end SHA-256
-> destination publishes local immutable NCDX
-> destination publishes local NCNX replica state
-> destination writes an idempotent replication receipt
-> source advances the peer checkpoint
```

For example:

```text
client in PH
  -> PUT radiology/patient-001/xray.dcm
  -> cluster PH commits version V1 using local EC 6 + 2
  -> cluster PH publishes local NCNX mutation M1
  -> cluster PH sends logical V1 and M1 to cluster SG
  -> cluster SG imports V1 using local EC 4 + 2
  -> cluster SG verifies V1 and publishes M1 into its local NCNX
  -> cluster SG acknowledges V1/M1
```

The client success response depends on the local N2 commit contract, not on
the WAN acknowledgement, unless a future explicitly synchronous policy is
separately designed. An asynchronous peer failure changes replication health
and RPO; it does not retroactively invalidate the local commit.

The destination must never publish NCNX before the imported object version is
locally readable according to its own N2 survival threshold.

### Existing version

If the destination already has the same version identity and verified content
digest, it does not rewrite the object. It reconciles NCNX state and returns
the existing receipt.

### Rename or move

Replication reconciles both affected keys against the complete observed
mutation set. The destination publishes the new live NCNX state first,
verifies it, and publishes the obsolete source-key tombstone second. Object
NCDX and shards are not rewritten.

A rename remains copy/delete at the namespace layer. It is not a cross-region
atomic directory transaction. A shared operation ID may group the destination
and source mutations for progress, diagnosis, and restart, but correctness is
still destination-first and exact-key based.

### Trash

The original deletion timestamp, origin cluster, and mutation identity are
preserved. The destination publishes the timestamped trash NCNX generation
before removing its live NCNX. Replication delay never resets trash retention.

### Recover

The destination validates the recovery mutation against the observed exact-key
mutation set, publishes live NCNX first, and removes the corresponding trash
state second. A concurrently published live version is preserved and resolved
through the same conflict contract as an overwrite; it is never overwritten
silently.

### Permanent absence

Absence is never inferred from a failed read, timeout, or missing replica.
Permanent removal requires a valid committed tombstone or a complete
anti-entropy comparison under the same fragment policy generation.

## Active-Active Namespace Convergence

Active-active does not create competing in-place byte writes. Object versions
are immutable, NCDX is committed once for a local layout, and ordinary
mutations operate on NCNX. Consequently, most work is independent:

```text
PH writes radiology/a.dcm
SG writes radiology/b.dcm

different exact keys -> no conflict -> replicate both
```

The true conflict boundary is concurrent mutation of the same normalized key:

```text
WAN partition

PH: report.pdf -> version PH-V2, mutation PH-M2
SG: report.pdf -> version SG-V3, mutation SG-M3

after reconnection

PH observes {PH-M2, SG-M3}
SG observes {PH-M2, SG-M3}
             |
             v
same deterministic visible HEAD on both clusters
```

Both committed versions remain addressable by version identity. Selecting one
visible HEAD does not erase the other. A delete is a first-class tombstone
mutation, so overwrite-versus-delete and recover-versus-delete use the same
convergence machinery rather than special arrival-order behavior.

The final conflict algorithm must satisfy all of these invariants:

1. the same observed mutation set produces the same HEAD and namespace state;
2. duplicate and reordered delivery does not change the result;
3. clock skew, clock rollback, equal timestamps, and restart do not create
   different winners;
4. no committed version or tombstone is silently discarded;
5. a stale fragment policy generation cannot defeat a newer fenced generation;
6. a result remains stable when an already-observed mutation is delivered
   again;
7. conflict resolution requires no WAN lock, global database, or synchronous
   quorum.

Using only `LastModified`, local arrival time, or maximum ULID timestamp is not
sufficient proof of this contract. The ordering may use stable mutation
identity, origin cluster identity, causal parent information, or another
deterministic rule, but the specific rule remains deliberately undecided until
its failure cases and tests are complete.

### Proposed active-active NCNX representation

Active-active convergence must remain part of the first-class NCNX namespace;
it must not introduce a global SQL database, a cluster-wide mutable latest
pointer, or an unrelated namespace service. The proposed extension is one
versioned NCNX payload per exact live or trash key containing:

```text
normalized_key
fragment_id
policy_generation
visible_mutation_id
visible_object_id?
visible_version_id?
frontier[]
  mutation_id
  origin_cluster_id
  parent_mutation_ids[]
  operation
  object_id?
  version_id?
  deleted_at_unix_nanos?
```

The frontier contains the mutations for that exact key that are not causally
dominated by another mutation already observed by the cluster. A mutation that
names the complete current frontier as its parents supersedes that frontier. A
mutation that does not observe another frontier member is concurrent with it,
so both remain until an explicit later mutation resolves them.

This preserves the existing fast namespace behavior:

- LIST still enumerates NCNX paths and filenames without opening NCDX or object
  payloads;
- an exact GET opens one NCNX record and follows the selected object/version;
- different keys remain independent and require no common lock;
- NCNX replication and healing continue through the local `M + 1` placement
  contract;
- NCDX and shards remain immutable and are not rewritten by namespace
  convergence.

The visible mutation is a deterministic projection of the frontier, not a
separate mutable source of truth. Every cluster holding the same frontier must
calculate the same visible mutation. Concurrent siblings remain readable by
exact version identity even when they are not the visible HEAD.

The exact NCNX wire encoding, frontier compaction rule, and maximum exceptional
conflict width are not locked. They must be proven against repeated concurrent
writes, deletes, recoveries, renames, policy changes, clock rollback, restart,
and malicious replay before implementation. Ordinary reconciliation may remove
only causally dominated entries; it must never truncate an unresolved sibling
merely to meet a size target.

### Read and LIST behavior during convergence

Within one cluster, reads and LIST use that cluster's currently committed NCNX
state and retain the existing zero-decode bounded listing path. They do not
contact every region for an ordinary request.

During a WAN partition, two active writer clusters may temporarily expose
different HEAD values or listings because each has observed a different
mutation set. After the mutation sets converge, both clusters must expose the
same result. Immediate global read-after-write consistency during a WAN
partition is not promised because providing it would require WAN coordination
in the foreground path.

An exact version-ID read remains unambiguous once that immutable version is
present locally. A future gateway may route or proxy an exact-version read to
the origin while a local replica is behind, but that is an optimization and
not namespace authority.

## Reconcile Hints And Anti-Entropy

Ordinary committed mutations create small reconcile hints under durable local
cluster runtime state. Hints contain identities, not object bodies.

Hints deliberately do not form a globally ordered transaction log. Multiple
nodes may emit the same hint, and hints may arrive late or out of order.

For each hint, the worker asks the home cluster for the current canonical state
of that exact key and immutable version set in single-authority mode. In
active-active mode, it exchanges the stable mutation and version identities
missing from either peer. Therefore an old rename or trash hint cannot roll
back a mutation that is already part of the converged set.

Anti-entropy is the correctness fallback:

1. partition work by fragment and deterministic key range;
2. compare bounded pages of NCNX names and mutation identities between the
   configured peers;
3. compare immutable version identities and content digests only for differing
   keys;
4. enqueue exact reconciliation work;
5. continue from a restart-proof cursor;
6. repeat until the configured replica converges.

Anti-entropy never performs an unbounded in-memory full-cluster listing. It
uses the existing zero-decode paginated namespace model and bounded merge
windows.

If local hint state is lost, anti-entropy reconstructs the required work. Hints
improve recovery time; they are not the only proof that replication is
complete.

## Checkpoints And Restart

Each directional replication relationship persists independent checkpoints:

```text
source cluster
destination cluster
fragment or fragment range
last successful reconciliation cursor
oldest pending mutation time
pending item count
pending byte estimate
last verified anti-entropy pass
last error
```

Checkpoints are operational state, not object authority. A missing or corrupt
checkpoint causes bounded replay or anti-entropy, never object deletion.

After restart:

1. validate peer identity and signed fragment replication manifests;
2. load verified receipts and checkpoints;
3. resume incomplete durable imports;
4. replay pending reconcile hints idempotently;
5. run anti-entropy for uncertain ranges;
6. report healthy only after backlog and configured RPO return within policy.

## Replication Topologies

### Active-active fragment replication

```text
fragment radiology

cluster PH, writer  <---- asynchronous ---->  cluster SG, writer
        |                                           |
        +-- local EC and NCNX                       +-- local EC and NCNX
```

Both clusters accept local mutations while connected or partitioned. They
exchange immutable versions and namespace mutations, retain concurrent
siblings, and deterministically converge after observing the same mutation
set.

### One-way disaster-recovery replica

```text
cluster A, single writer
  -> cluster B, read-only replica
```

This remains the simplest implementation and validation topology. It can be
built before active-active without changing the logical import protocol.

### Bidirectional federation with independent authorities

```text
fragment X: cluster A -> cluster B
fragment Y: cluster B -> cluster A
```

Traffic moves in both directions, but each fragment still has exactly one
authority. This does not create multi-writer conflict.

This mode remains useful even after active-active exists because operators may
prefer single-writer fragments with geographic read-only copies.

### One-to-many replication

```text
cluster A
  ├── cluster B
  └── cluster C
```

Each destination has an independent checkpoint, backlog, bandwidth policy,
and RPO status. One slow destination does not block another unless policy
explicitly requires its acknowledgement for lifecycle reclamation.

### Chained replication

Chaining is not part of the first release. A replica does not silently become
the origin for another replica because this obscures authority and recovery
point guarantees. It requires an explicit future contract.

### Site replication

```text
site policy PH <-> SG
  -> active-active fragment relationship for every eligible fragment
  -> independent checkpoint and RPO for every fragment and direction
```

Site replication is configuration expansion and lifecycle management. It does
not merge fragments into a global bucket, create a stretched server pool, or
change an object's local EC layout.

## WAN Failure Behavior

### Destination cluster unavailable

- permitted local writes continue;
- backlog and oldest-unreplicated age increase;
- retries use bounded exponential backoff with jitter;
- no busy retry loop or unbounded memory queue is allowed;
- anti-entropy verifies convergence after reconnection.

### WAN partition

- active-active writer clusters remain independently writable;
- read-only replicas remain readable and reject client mutations;
- no synchronous WAN quorum is attempted;
- routing reports peer staleness and last successful checkpoint;
- exact-key views may temporarily differ until mutation sets converge.

### One active-active writer unavailable

- another healthy configured writer continues serving reads and writes;
- no authority promotion is required merely because another active writer is
  unavailable;
- the unavailable writer catches up idempotently before reporting converged
  after it returns.

### Single-authority home cluster unavailable

- locally durable replicas may continue serving reads;
- writes remain disabled until an explicit authority promotion completes;
- inability to contact the home cluster alone is not permission to promote.

### Corrupt or truncated transfer

- destination publication does not occur;
- the partial durable import remains resumable or is reclaimed by local upload
  lifecycle policy;
- SHA-256 mismatch emits an exact object/version report;
- retry may use another healthy source node from the same originating
  cluster.

### Destination out of capacity

- destination rejects the import before namespace publication;
- the already committed source version remains valid and its cluster remains
  operational;
- the replication relationship reports capacity-blocked rather than healthy;
- operators add a complete destination server pool or change the replication
  policy.

### Lost reconcile hint or checkpoint

- no object is deleted;
- the affected range is marked uncertain;
- bounded anti-entropy discovers and reconstructs missing work.

## Writer Membership, Promotion, And Fencing

Active-active availability does not eliminate fencing. Fencing is required
when a cluster is removed from the writer set, when an active-active fragment
is converted to read-only at one site, or when a stale configuration could
continue accepting mutations.

```text
policy generation 12
writers = [PH, SG]

operator removes PH from the writer set
-> fence PH for fragment radiology
-> publish signed generation 13
-> writers = [SG]
-> gateways stop routing writes to PH
-> PH rejects generation-12 mutations after reconnecting
```

Loss of contact is not proof that fencing succeeded. If a removed writer cannot
be durably fenced, the federation must report the fragment as exposed to
potential split authority rather than silently claiming a clean transition.

Single-authority disaster-recovery promotion remains a supported policy:

Geographic promotion changes fragment authority, not object placement inside a
cluster.

```text
policy generation 7: cluster A writer, cluster B read-only replica
-> operator verifies replication state and fences cluster A
-> signed fragment manifest commits generation 8 with cluster B as writer
-> federation routing publishes generation 8
-> cluster B enables writes for generation 8
-> cluster A rejects generation-7 writes when it reconnects
```

Promotion requirements:

- explicit operator request in the first release;
- destination has a verified local copy within the accepted RPO;
- destination single-cluster health is write-safe;
- a durable fence prevents the old home from accepting writes;
- the new signed fragment manifest is persisted before routing changes;
- the new home revalidates fragment NCNX and immutable versions before it
  reports writable;
- all clients and gateways reject stale policy generations.

If the former home cannot be fenced, automatic promotion is forbidden. The
system prefers read-only availability over geographic split brain.

When a stale cluster reconnects:

- its older policy generation is rejected;
- divergent stale-generation mutations are quarantined and reported;
- they are never silently merged into current authority;
- it is resynchronized as a replica of the new home.

Planned migration uses the same mechanism without an outage emergency: fully
replicate, verify, fence the old home, advance the policy generation, change
routing, and reverse the replication direction.

## Adding A Region And Initial Synchronization

Adding a region never changes existing clusters' server pools or rewrites
their local object layouts:

```text
existing federation
  PH <-> SG

add cluster JP
  -> initialize JP as an independent N2 cluster
  -> configure JP local pools, EC, roots, and Reserve Set
  -> exchange and verify cluster identities
  -> publish a new fragment policy generation
  -> backfill committed logical versions into JP
  -> reconcile NCNX mutation sets in bounded pages
  -> record a verified checkpoint
  -> begin consuming live mutations
  -> report JP healthy only after the configured RPO is satisfied
```

Backfill and live replication may overlap. Stable mutation and version
identities make duplicate delivery safe. A bounded checkpoint separates
already verified ranges from uncertain ranges; anti-entropy revisits uncertain
ranges after restart.

The destination imports every object through the same verified internal import
boundary used by live replication. Bootstrap is not allowed to place source
shards directly on destination disks or manufacture NCNX before local
readability is proven.

Changing a destination's EC configuration affects new local imports according
to ordinary N2 rules. It does not require the origin clusters to redistribute
or reinterpret their layouts.

## Regional Request Flows

### Normal active-active write

```text
client
  -> nearest gateway
  -> nearest permitted writer cluster
  -> ordinary local N2 commit
  -> client success
  -> asynchronous peer transfer
  -> destination-local EC commit
  -> destination NCNX convergence
```

### Read

```text
client
  -> nearest gateway
  -> nearest sufficiently current cluster
  -> ordinary local N2 exact-key or LIST path
```

The gateway does not merge regional listings. If policy requires monotonic
read-after-write behavior, the client or gateway carries a minimum mutation or
version identity and routes to a cluster that has observed it. The precise
session-consistency contract remains a future API decision.

### Regional outage

```text
PH unavailable

SG active writer
  -> continues local reads and writes
  -> accumulates PH-directed replication backlog

PH returns
  -> validates current fragment policy generation
  -> receives missing immutable versions and mutations
  -> runs bounded anti-entropy
  -> reports converged only after verification
```

### WAN partition with both writers alive

```text
PH writer  X----- WAN -----X  SG writer
    |                            |
local commits continue      local commits continue
local views may diverge     local views may diverge
    |                            |
    +------ WAN restored --------+
                 |
       exchange mutation sets
                 |
       deterministic convergence
```

This mode chooses regional survival and eventual convergence over immediate
global linearizability. That trade-off must be explicit in the NOA contract and
operator documentation.

## Lifecycle And Geographic Retention

Local N2 trash and GC remain the physical reclamation mechanisms inside each
cluster. Replication policy adds the geographic safety barrier and converged
tombstone state.

For a required disaster-recovery replica:

- source GC must not reclaim the last transferable immutable version before
  the required destination acknowledges either the live version or its final
  trash state;
- trash expiration uses the original committed deletion timestamp;
- an offline required destination may extend physical retention on the source;
- operators can explicitly waive a failed destination only through an audited
  policy action.

For best-effort replicas, source lifecycle may proceed after local policy
expires even when a destination is behind. The resulting RPO loss is reported
and must be an explicit deployment choice.

Replica GC never infers that an object is obsolete merely because it is absent
from one scan. It requires a converged committed tombstone under the current
fragment policy generation.

For active-active fragments, a tombstone generated at one writer competes with
concurrent exact-key mutations through the same deterministic conflict rule as
all other NCNX mutations. Physical reclamation waits until retention and
required-peer safety conditions are satisfied. Replication lag cannot shorten
retention by replacing the original deletion time with arrival time.

## Security

Inter-cluster replication is not authenticated with ordinary user JWTs.

Required controls:

- mutual TLS between cluster identities;
- pinned federation and cluster IDs;
- signed fragment replication manifests;
- narrowly scoped replication credentials per peer and direction;
- envelope signature or authenticated transport binding;
- replay-safe idempotency keys;
- rejection of unknown policy generations and destination IDs;
- configurable peer address allowlists;
- complete audit reports for promotion, policy change, checksum conflict, and
  replication disablement.

A replication peer can import only fragments covered by its rule. It cannot
change local server-pool membership, Reserve Sets, unrelated fragments, master
keys, or operator policy.

## Resource And Performance Contract

Replication must never create an unbounded worker, queue, scan, or memory
structure.

Every rule configures:

- maximum concurrent object transfers;
- maximum concurrent namespace reconciliations;
- bandwidth ceiling;
- bounded retry queue and on-disk backlog;
- scan page size;
- priority relative to foreground IO and local healing;
- optional time windows;
- per-peer request timeout and backoff ceiling.

Large objects stream end to end. They are not buffered in RAM or staged as a
complete plaintext file. The destination uses durable multipart import so a
WAN interruption resumes verified parts rather than restarting the object.

Scheduling priority is:

1. local safety-critical healing and write admission;
2. foreground client reads and writes;
3. required-replica recovery within RPO;
4. ordinary replication backlog;
5. anti-entropy and optional revalidation.

Release benchmarks must measure foreground latency with replication disabled,
healthy, lagging, and recovering. A replication implementation that preserves
correctness but silently destroys foreground performance does not pass.

## Observability

Every replication relationship reports:

- source and destination cluster IDs;
- fragment scope, replication mode, and policy generation;
- state: disabled, initializing, healthy, lagging, blocked, divergent, or
  fenced;
- pending items and estimated bytes;
- oldest pending mutation age;
- last successful object and namespace reconciliation;
- last completed anti-entropy pass;
- current transfer rate and worker count;
- checksum, authentication, capacity, policy-generation, and fencing errors;
- configured and currently achieved RPO;
- convergence state, conflict count, and promotion or writer-removal readiness.

Object-level failures emit disposable independent JSON reports containing the
fragment, logical key, object ID, version ID, source cluster, destination
cluster, policy generation, checksum evidence, retry count, and exact failure.
Reports are not replicated storage authority.

Federation health never reports `healthy` when a required peer is outside its
RPO, a checkpoint range is uncertain, a checksum conflict is unresolved,
same-key conflicts have not converged, or fragment manifests disagree.

## Configuration Model

The exact syntax may evolve, but the semantic configuration is:

```text
[federation]
federation_id=<stable-id>
local_cluster_id=ph-1
state_path=<durable-runtime-path>/cluster

[peer.sg-1]
endpoint=https://n2-sg-1.example
cluster_id=sg-1
tls_identity=<peer-identity>

[replication.hospital-dr]
fragment=hospital-a
mode=active_active
writer_clusters=ph-1,sg-1
replica_clusters=jp-1
conflict_policy=<locked-policy-id>
required_peers=sg-1
max_concurrent_objects=8
max_concurrent_namespace_ops=32
bandwidth_limit_mib_per_second=200
maximum_rpo_seconds=300
```

Peer endpoints and credentials are operational configuration. Stable cluster,
fragment, policy-generation, version, and mutation identities are never
derived from configuration list order.

## Module Boundary

The implementation belongs under a separate cluster subsystem:

```text
engine/src/cluster/
├── manifest.rs        signed fragment policies, writer sets, and fencing
├── federation.rs      cluster identity and peer configuration
├── routing.rs         fragment-to-cluster resolution
├── envelope.rs        versioned replication protocol
├── outbox.rs          bounded reconcile hints and checkpoints
├── sender.rs          source-side streaming and retry
├── receiver.rs        destination validation and durable import
├── anti_entropy.rs    bounded namespace/version reconciliation
├── convergence.rs     exact-key mutation-set and HEAD resolution
├── promotion.rs       writer changes, migration, and DR promotion
├── policy.rs          fragment/site scope, RPO, retention, and bandwidth
├── monitoring.rs      health, lag, and reports
└── tests/
```

The existing object engine exposes narrow internal operations:

- read canonical committed key state;
- enumerate bounded NCNX pages;
- stream a verified immutable version;
- import a preserved logical version into a local physical layout;
- publish replica NCNX state destination-first;
- inspect local readability and checksum state.

The cluster module cannot directly rewrite NCDX, shards, NCNX files,
membership, or Reserve Set bindings. It must use verified engine boundaries.

The cluster module owns geographic retry, conflict convergence, checkpoints,
and peer policy. The existing engine remains unaware of WAN topology and must
not gain a remote-cluster branch in its ordinary shard or namespace mutation
paths.

## Implementation Phases

### Phase 0: contract and test harness

- lock protocol identities, mutation identities, and fragment policy
  generations;
- build two independent simulated clusters with different EC layouts;
- add WAN latency, loss, duplication, reordering, and partition injection;
- add deterministic replication evidence under the test-results directory.

### Phase 1: one-way read-only replication

- stable cluster identities and mutual TLS;
- one single-writer fragment and one read-only destination;
- durable object import, NCNX reconciliation, hints, checkpoints, and
  anti-entropy;
- no promotion and no reverse direction.

### Phase 2: bidirectional federation

- different fragments authoritative in different clusters;
- federation routing and authority-aware JWT hints;
- independent directional policies and observability;
- still one writer per fragment.

### Phase 3: active-active fragment replication

- explicitly configured writer set for one fragment;
- local writes continue at every writer during WAN partition;
- immutable concurrent versions and tombstones are preserved;
- deterministic same-key convergence under duplication, reordering, clock
  skew, clock rollback, equal timestamps, and restart;
- no global lock, database, or WAN quorum;
- active-active remains opt-in until its separate release gates pass.

### Phase 4: migration and disaster-recovery promotion

- signed fragment policy generations;
- explicit fencing and planned migration;
- read-only disaster operation and controlled promotion;
- stale-generation rejection and former-writer resynchronization.

### Phase 5: site replication and broader policies

- one-to-many replicas;
- site-wide enrollment of eligible fragments using the same fragment
  replication engine;
- policy-driven placement of new fragments across clusters;
- optional replica read routing;
- chained replication only under a separate locked design and proof.

## Release Gates

Geographic multi-cluster N2 is not production-ready until all of the following
pass cleanly:

- two independent clusters with different server-pool EC layouts;
- preservation of fragment, key, object, version, mutation, and checksum
  identities;
- destination-local NCDX, shard, parity, and NCNX placement correctness;
- duplicate, delayed, dropped, and reordered reconcile hints;
- source, destination, worker, and gateway restart at every import stage;
- large-object durable resume after repeated WAN interruption;
- exact rename, move, trash, recover, and TTL timestamp preservation;
- concurrent same-key overwrites from two and three writer clusters;
- overwrite-versus-delete, overwrite-versus-recover, delete-versus-recover,
  and rename-destination conflicts;
- deterministic HEAD and namespace state under equal timestamps, extreme clock
  skew, clock rollback, restart, and deliberately reordered delivery;
- preservation and exact-version readability of every committed conflict
  sibling and tombstone;
- no active-active replay loop or unbounded mutation amplification;
- independent writes to different keys proceeding without cross-key locking;
- bounded paginated anti-entropy with no omissions or false deletion;
- destination disk-full and server-pool-full behavior;
- destination node loss and autonomous local healing during replication;
- source node loss with transfer retry through another healthy source node;
- prolonged WAN partition while active-active writers independently remain
  writable;
- prolonged WAN partition in single-authority mode while replicas remain
  read-only;
- checkpoint loss followed by complete convergence through anti-entropy;
- planned migration with one fragment-policy-generation advance;
- failed promotion when the former home cannot be fenced;
- successful promotion, stale-writer rejection, and former-home resync;
- active-active writer removal with stale-generation rejection after
  reconnection;
- adding a third region through bounded backfill while live writes continue;
- fragment-level policy expansion into site replication without a second data
  transfer path;
- required-replica GC barrier and explicit best-effort RPO behavior;
- credential rotation, peer rejection, replay, and unauthorized-fragment tests;
- foreground latency and throughput within the declared replication budget;
- multi-day independent-host operation followed by full checksum comparison.

Passing single-cluster tests is a prerequisite, not evidence that these
multi-cluster gates have passed.

## Explicit Non-Goals

The initial geographic implementation will not provide:

- EC stripes split across geographic clusters;
- server pools whose members span WAN regions;
- synchronous WAN quorum on ordinary object or namespace mutations;
- a global namespace database or global object index;
- cluster-wide LIST merged across unrelated fragments;
- automatic promotion based only on a timeout;
- immediate global linearizability while writer regions are partitioned;
- silent arrival-order or wall-clock-only last-writer-wins conflict
  resolution;
- destructive conflict resolution that discards an independently committed
  immutable version or tombstone;
- automatic write permission inferred only from another site's outage;
- geographic Reserve Set substitution for a local logical pool slot;
- transparent reinterpretation of an existing object's local EC layout.

These exclusions preserve N2's single-cluster correctness and keep geographic
replication an additive operational layer rather than a replacement storage
engine.
