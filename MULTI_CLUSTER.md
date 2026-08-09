# N2 Geographic Multi-Cluster Architecture

## Status

This document is the design contract for the future geographic multi-cluster
generation of N2. It is not implemented by N2 3.4 and does not change the
locked single-cluster storage laws in [ARCHITECTURE.md](ARCHITECTURE.md).

The first implementation must be additive and isolated under N2's `cluster`
module. It must consume committed single-cluster objects and namespace state
through narrow internal interfaces. It must not insert geographic coordination
into ordinary PUT, GET, LIST, rename, trash, healing, placement, or server-pool
operations.

The purpose is to connect independent N2 clusters across geographic failure
domains while preserving the defining properties of N2:

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
2. deterministic routing of a fragment to its authoritative home cluster;
3. read-only geographic replicas that use their own local N2 durability;
4. restart-proof replication with bounded retries and anti-entropy repair;
5. planned migration and explicitly fenced disaster-recovery promotion;
6. independent cluster operation during WAN loss;
7. observable recovery-point and replication-lag guarantees.

It does not create one stretched EC pool across regions. Each cluster remains
a complete N2 system and failure domain.

```text
N2 federation
├── cluster PH-1
│   ├── local server pools
│   ├── local Reserve Set
│   ├── authoritative fragments: hospital-a, imaging-a
│   └── replicated fragments: research-b
├── cluster SG-1
│   ├── local server pools
│   ├── local Reserve Set
│   ├── authoritative fragments: research-b
│   └── replicated fragments: hospital-a
└── federation routing and signed authority manifests
```

## Architectural Laws

1. **A cluster is the geographic failure boundary.** EC shards, NCDX copies,
   NCNX replicas, logical pool slots, and Reserve Sets never span clusters.
2. **Every fragment has exactly one write-authoritative home cluster for one
   authority epoch.** Replicas reject ordinary client mutations.
3. **WAN replication is asynchronous.** Loss of a remote cluster or WAN link
   does not stop safe local writes on the authoritative cluster.
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
   authoritative cluster's committed NCNX and immutable versions remain the
   source of truth. Lost hints are repaired by anti-entropy.
9. **Event arrival order is not trusted.** Applying a hint re-reads or verifies
   the current authoritative state for that exact key.
10. **Namespace publication remains destination-first.** Replicated rename,
    trash, and recover operations publish the destination state before removing
    obsolete replica state.
11. **No geographic single writer is inserted into the local data path.**
    Replication workers may duplicate work because the protocol is idempotent.
12. **No automatic promotion occurs without a durable authority fence.** When
    the former home cluster cannot be fenced, replicas remain read-only.
13. **A stale authority epoch can never publish.** Reconnected former homes
    accept the newer signed authority manifest and become replicas.
14. **Local healing precedes geographic assumptions.** A cluster repairs its
    own exact layouts; geographic replication does not replace local EC,
    parity, or Reserve Sets.
15. **Foreground storage has priority.** Replication is bounded, rate-limited,
    and subordinate to client IO and critical local healing.

## Terminology

| Term | Meaning |
| --- | --- |
| Federation | A configured collection of independent N2 clusters. |
| Cluster ID | Stable globally unique identity of one N2 cluster. |
| Home cluster | The only cluster allowed to accept normal writes for a fragment in the current authority epoch. |
| Replica cluster | A cluster holding a locally durable read-only copy of a remote fragment. |
| Authority epoch | Monotonically increasing fragment authority generation used to fence stale writers. |
| Authority manifest | Signed record mapping a fragment and epoch to its home cluster and replica policy. |
| Replication rule | Source, destination, fragment selection, retention, bandwidth, and required-ack policy. |
| Reconcile hint | Durable or recoverable notification that an exact key or fragment control record must be compared with the home cluster. |
| Anti-entropy | Bounded comparison that discovers missing or divergent replica state without relying on the hint stream. |
| RPO | Maximum accepted age or volume of acknowledged local changes not yet durable on a required replica. |

## Cluster Identity And Authority

Every cluster has a stable `cluster_id`, independent TLS identity, replication
signing identity, and local membership. Cluster identity is not a server-pool
ID and is not derived from an IP address or DNS name.

Each replicated fragment has a signed authority manifest:

```text
federation_id
fragment_id
authority_epoch
home_cluster_id
replica_cluster_ids[]
write_mode = authoritative | replica_read_only | fenced
replication_policy_id
issued_at
manifest_digest
signature
```

The manifest is small control authority. It does not contain object listings,
placement maps, shards, namespace rows, user quotas, or business metadata.

Every mutation is evaluated against the fragment's local authority manifest:

- `authoritative`: normal NOA and native mutations are allowed;
- `replica_read_only`: GET, HEAD, LIST, and integrity operations are allowed,
  while client writes are rejected with the current home-cluster identity;
- `fenced`: client mutations are rejected until authority is explicitly
  resolved.

JWT claims may carry `fragment_id`, `home_cluster_id`, and `authority_epoch` as
routing hints. The signed local authority manifest remains the final write
admission authority; a stale token cannot override it.

## Federation Routing

A federation-aware gateway resolves a fragment before routing an operation.
It is not object or namespace authority.

```text
client
  -> federation gateway resolves fragment authority
  -> authoritative cluster for mutations
  -> nearest healthy replica or home cluster for permitted reads
```

Routing may use a redirect or proxy mode:

- **redirect mode** returns the signed cluster endpoint so large object bytes
  do not traverse a central gateway;
- **proxy mode** provides a single endpoint when client or network policy
  requires it.

Routing state is bounded by fragment authority manifests. It never contains a
global object index. LIST for one fragment is served by one selected cluster;
the gateway does not merge the same replicated fragment from multiple
clusters.

If a new tenant or fragment is assigned to another cluster because existing
clusters are full, the federation adds a new authority manifest. It does not
extend an existing server pool across the WAN.

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
- authoritative NCNX state and mutation identity;
- source authority epoch.

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
authority_epoch
logical_key
object_id
version_id
mutation_id
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
  federation_id || origin_cluster_id || fragment_id || authority_epoch ||
  object_id || version_id || mutation_id || namespace_state
)
```

Receiving the same envelope again returns the existing verified receipt.
Receiving the same identity with different immutable content or checksum is a
hard conflict: the destination quarantines the request, emits a report, and
does not guess which payload is correct.

## Commit Protocol

### New object or version

```text
home cluster publishes object locally
-> replication module emits a reconcile hint
-> destination validates authority epoch and identity
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

The destination must never publish NCNX before the imported object version is
locally readable according to its own N2 survival threshold.

### Existing version

If the destination already has the same version identity and verified content
digest, it does not rewrite the object. It reconciles NCNX state and returns
the existing receipt.

### Rename or move

Replication reconciles both affected keys against the current authoritative
state. The destination publishes the new live NCNX state first, verifies it,
and removes the obsolete live replica second. Object NCDX and shards are not
rewritten.

### Trash

The original authoritative deletion timestamp and mutation identity are
preserved. The destination publishes the timestamped trash NCNX generation
before removing its live NCNX. Replication delay never resets trash retention.

### Recover

The destination verifies that recovery is still authoritative, publishes live
NCNX first, and removes the corresponding trash state second. A conflicting
live destination is reported and never overwritten silently.

### Permanent absence

Absence is never inferred from a failed read, timeout, or missing replica.
Permanent removal requires a valid authoritative mutation state or a complete
anti-entropy comparison under the same authority epoch.

## Reconcile Hints And Anti-Entropy

Ordinary committed mutations create small reconcile hints under durable local
cluster runtime state. Hints contain identities, not object bodies.

Hints deliberately do not form a globally ordered transaction log. Multiple
nodes may emit the same hint, and hints may arrive late or out of order.

For each hint, the worker asks the home cluster for the current canonical state
of that exact key and immutable version set. Therefore an old rename or trash
hint cannot roll back a newer authoritative result.

Anti-entropy is the correctness fallback:

1. partition work by fragment and deterministic key range;
2. compare bounded pages of authoritative NCNX names and mutation identities;
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

1. validate peer identity and authority manifests;
2. load verified receipts and checkpoints;
3. resume incomplete durable imports;
4. replay pending reconcile hints idempotently;
5. run anti-entropy for uncertain ranges;
6. report healthy only after backlog and configured RPO return within policy.

## Replication Topologies

### One-way disaster-recovery replica

```text
cluster A, authoritative
  -> cluster B, read-only replica
```

This is the first implementation target and the simplest production topology.

### Bidirectional federation with independent authorities

```text
fragment X: cluster A -> cluster B
fragment Y: cluster B -> cluster A
```

Traffic moves in both directions, but each fragment still has exactly one
authority. This does not create multi-writer conflict.

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

## WAN Failure Behavior

### Destination cluster unavailable

- authoritative local writes continue;
- backlog and oldest-unreplicated age increase;
- retries use bounded exponential backoff with jitter;
- no busy retry loop or unbounded memory queue is allowed;
- anti-entropy verifies convergence after reconnection.

### WAN partition

- the home cluster remains writable;
- replicas remain readable and reject client mutations;
- no synchronous WAN quorum is attempted;
- routing reports replica staleness and last successful checkpoint.

### Home cluster unavailable

- locally durable replicas may continue serving reads;
- writes remain disabled until an explicit authority promotion completes;
- inability to contact the home cluster alone is not permission to promote.

### Corrupt or truncated transfer

- destination publication does not occur;
- the partial durable import remains resumable or is reclaimed by local upload
  lifecycle policy;
- SHA-256 mismatch emits an exact object/version report;
- retry may use another healthy source node from the same home cluster.

### Destination out of capacity

- destination rejects the import before namespace publication;
- source remains authoritative and operational;
- the replication relationship reports capacity-blocked rather than healthy;
- operators add a complete destination server pool or change the replication
  policy.

### Lost reconcile hint or checkpoint

- no object is deleted;
- the affected range is marked uncertain;
- bounded anti-entropy discovers and reconstructs missing work.

## Promotion And Fencing

Geographic promotion changes fragment authority, not object placement inside a
cluster.

```text
authority epoch 7: cluster A authoritative, cluster B replica
-> operator verifies replication state and fences cluster A
-> signed authority manifest commits epoch 8 with cluster B authoritative
-> federation routing publishes epoch 8
-> cluster B enables writes for epoch 8
-> cluster A rejects epoch 7 writes when it reconnects
```

Promotion requirements:

- explicit operator request in the first release;
- destination has a verified local copy within the accepted RPO;
- destination single-cluster health is write-safe;
- a durable fence prevents the old home from accepting writes;
- the new signed authority manifest is persisted before routing changes;
- the new home revalidates fragment NCNX and immutable versions before it
  reports writable;
- all clients and gateways reject stale authority epochs.

If the former home cannot be fenced, automatic promotion is forbidden. The
system prefers read-only availability over geographic split brain.

When a stale cluster reconnects:

- its older authority epoch is rejected;
- divergent stale-epoch mutations are quarantined and reported;
- they are never silently merged into current authority;
- it is resynchronized as a replica of the new home.

Planned migration uses the same mechanism without an outage emergency: fully
replicate, verify, fence the old home, advance the epoch, change routing, and
reverse the replication direction.

## Lifecycle And Geographic Retention

Local N2 trash and GC remain authoritative inside each cluster. Replication
policy adds only the geographic safety barrier.

For a required disaster-recovery replica:

- source GC must not reclaim the last transferable immutable version before
  the required destination acknowledges either the live version or its final
  trash state;
- trash expiration uses the authoritative deletion timestamp;
- an offline required destination may extend physical retention on the source;
- operators can explicitly waive a failed destination only through an audited
  policy action.

For best-effort replicas, source lifecycle may proceed after local policy
expires even when a destination is behind. The resulting RPO loss is reported
and must be an explicit deployment choice.

Replica GC never infers that an object is obsolete merely because it is absent
from one scan. It requires authoritative replicated state under the current
authority epoch.

## Security

Inter-cluster replication is not authenticated with ordinary user JWTs.

Required controls:

- mutual TLS between cluster identities;
- pinned federation and cluster IDs;
- signed authority manifests;
- narrowly scoped replication credentials per peer and direction;
- envelope signature or authenticated transport binding;
- replay-safe idempotency keys;
- rejection of unknown authority epochs and destination IDs;
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
- fragment scope and authority epoch;
- state: disabled, initializing, healthy, lagging, blocked, divergent, or
  fenced;
- pending items and estimated bytes;
- oldest pending mutation age;
- last successful object and namespace reconciliation;
- last completed anti-entropy pass;
- current transfer rate and worker count;
- checksum, authentication, capacity, and authority errors;
- configured and currently achieved RPO;
- promotion readiness.

Object-level failures emit disposable independent JSON reports containing the
fragment, logical key, object ID, version ID, source cluster, destination
cluster, authority epoch, checksum evidence, retry count, and exact failure.
Reports are not replicated storage authority.

Federation health never reports `healthy` when a required peer is outside its
RPO, a checkpoint range is uncertain, a checksum conflict is unresolved, or
authority manifests disagree.

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
source_cluster=ph-1
destination_cluster=sg-1
fragment=hospital-a
mode=required
max_concurrent_objects=8
max_concurrent_namespace_ops=32
bandwidth_limit_mib_per_second=200
maximum_rpo_seconds=300
```

Peer endpoints and credentials are operational configuration. Stable cluster,
fragment, and authority identities are never derived from configuration list
order.

## Module Boundary

The implementation belongs under a separate cluster subsystem:

```text
n2/src/cluster/
├── authority.rs       signed fragment authority manifests and fencing
├── federation.rs      cluster identity and peer configuration
├── routing.rs         fragment-to-cluster resolution
├── envelope.rs        versioned replication protocol
├── outbox.rs          bounded reconcile hints and checkpoints
├── sender.rs          source-side streaming and retry
├── receiver.rs        destination validation and durable import
├── anti_entropy.rs    bounded namespace/version reconciliation
├── promotion.rs       planned migration and disaster-recovery promotion
├── policy.rs          RPO, retention, bandwidth, and fragment rules
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

## Implementation Phases

### Phase 0: contract and test harness

- lock protocol identities and authority rules;
- build two independent simulated clusters with different EC layouts;
- add WAN latency, loss, duplication, reordering, and partition injection;
- add deterministic replication evidence under the test-results directory.

### Phase 1: one-way read-only replication

- stable cluster identities and mutual TLS;
- one authoritative fragment and one read-only destination;
- durable object import, NCNX reconciliation, hints, checkpoints, and
  anti-entropy;
- no promotion and no reverse direction.

### Phase 2: bidirectional federation

- different fragments authoritative in different clusters;
- federation routing and authority-aware JWT hints;
- independent directional policies and observability;
- still one writer per fragment.

### Phase 3: migration and disaster-recovery promotion

- signed authority epochs;
- explicit fencing and planned migration;
- read-only disaster operation and controlled promotion;
- stale-epoch rejection and former-home resynchronization.

### Phase 4: broader policies

- one-to-many replicas;
- policy-driven placement of new fragments across clusters;
- optional replica read routing;
- chained or active-active behavior only under a separate locked design and
  proof. Neither is implied by this document.

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
- live-wins and conflict behavior under delayed delivery;
- bounded paginated anti-entropy with no omissions or false deletion;
- destination disk-full and server-pool-full behavior;
- destination node loss and autonomous local healing during replication;
- source node loss with transfer retry through another healthy source node;
- prolonged WAN partition while the home remains writable and replicas remain
  read-only;
- checkpoint loss followed by complete convergence through anti-entropy;
- planned migration with one authority-epoch advance;
- failed promotion when the former home cannot be fenced;
- successful promotion, stale-writer rejection, and former-home resync;
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
- active-active writes to the same fragment;
- silent last-writer-wins conflict resolution;
- geographic Reserve Set substitution for a local logical pool slot;
- transparent reinterpretation of an existing object's local EC layout.

These exclusions preserve N2's single-cluster correctness and keep geographic
replication an additive operational layer rather than a replacement storage
engine.
