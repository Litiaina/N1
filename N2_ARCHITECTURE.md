# N2 Distributed Storage Architecture

This document describes how N2 stores distributed object metadata, places cluster shards, and coordinates nodes. Operators and API users should start with [N2_DOCUMENTATION.md](N2_DOCUMENTATION.md). This document is for maintainers, reviewers, and administrators who need to reason about placement, repair, reconcile, peer traffic, and failure recovery.

## Architecture Summary

N2 is the distributed object storage layer above the N1 storage kernel.

N1 is the single-node storage kernel. N2 is the cluster layer. N2 owns the distributed namespace, cluster object identity, cluster erasure coding, shard placement, membership, governance, and reconcile behavior. Each N2 node uses local N1 storage roots as the local persistence substrate, but N2 does not hand user object keys to N1's rich namespace.

The N2 rule is:

```text
client object key belongs to N2
cluster object metadata is .nmtd
cluster shards are placed across N2 nodes
local N1 stores the local shard payload and local metadata roots
```

N2 0.7 is LOA-first:

- Public product API: `/n2/...`
- Native low-level protocol: `n2://`
- Private peer API: `/n2/internal/...`
- Local storage kernel API: `/n1/...` for fragment auth and local N1 operations

External compatibility adapters are not part of the N2 core.

## Storage Planes

N2 has five active planes:

- **Cluster namespace plane:** real prefix directories and symlink object entries.
- **Object record plane:** immutable `.nmtd` records under stable object-id directories.
- **Cluster shard plane:** local persisted cluster shard payloads.
- **Membership and governance plane:** cluster view, epoch, node set, and rollback state.
- **Transport plane:** LOA HTTP, native `n2://`, and private peer traffic.

These planes are separate so listing, reads, writes, reconcile, and membership changes can be reasoned about independently.

## Cluster Namespace Plane

N2 keeps user-visible object keys in its own namespace tree:

```text
<index_root>/<fragment_hash>/n2/namespace/
  videos/
    demo.mp4 -> ../../objects/ab/cd/ef/12/@o_<object_id>
```

Folders are real directories. File names are symlinks. A prefix listing such as `videos/` opens the `videos/` directory directly and reads direct children. This is the same core idea that makes N1 listing scale, but the namespace belongs to N2.

N2 does not store `videos/demo.mp4` as an N1 user object. N2 stores the cluster shard payload under an internal N2 shard identity. This keeps N1 stable and lets N2 own distributed object semantics.

Normal N2 listing should not scan the whole object record tree. It should:

```text
open N2 namespace directory for requested prefix
read direct children
resolve file symlink to stable object id
read max-ULID .nmtd summary when needed
return objects/common prefixes
```

## Object Record Plane

The committed distributed object truth is `.nmtd`.

Object records live under a fanned object-id tree:

```text
<index_root>/<fragment_hash>/n2/objects/<hh>/<hh>/<hh>/<hh>/@o_<object_id>/
  <version_id>.nmtd
```

`.nmtd` means N2 metadata. It records:

- fragment
- committed object key at creation time
- stable object id
- object uid
- version id
- membership epoch
- `kc` and `mc`
- local shard mode `kd=1`, `md=0`
- stripe block size
- total object size
- content type
- committed cluster shard references
- checksums and shard sizes
- timestamps

Object records are immutable after commit. N2 does not keep a mutable latest pointer file. Latest is derived by scanning the object-id directory and selecting the max ULID `.nmtd` filename.

Rename is namespace-only:

```text
n2/namespace/videos/demo.mp4 -> @o_<object_id>
n2/namespace/videos/final.mp4 -> @o_<object_id>
```

The object-id directory and all committed `.nmtd` records stay in place. This is the distributed version of the N1 invariant: mutable names live in the namespace plane, object identity and versions stay immutable.

Active N2 metadata inputs:

```text
n2/namespace symlinks
n2/objects/@o_<object_id>/<version_id>.nmtd
n2/shards/<version_id>/cshard_<cluster_shard_index>/.nmtd
```

## Cluster Shard Plane

N2 performs one cluster-level erasure-code pass. The local shard payload is persisted as the exact cluster shard:

```text
<blob_root>/<fragment_hash>/n2/shards/<version_id>/cshard_<cluster_shard_index>/
  disk_shard_0.bin
  .nmtd
```

`disk_shard_0.bin` is not a second EC set. It is the cluster shard bytes for this node.

N2 0.7 enforces:

```text
kd = 1
md = 0
```

That avoids double encoding and keeps upload CPU, disk IO, and write amplification close to the object-storage path N2 needs. Cluster durability comes from `kc + mc` across nodes.

## Write Path

The N2 write path is:

```text
client
  -> /n2/object/... or n2:// object.put
  -> fragment auth and permission check
  -> cluster write-ready check
  -> placement from object identity + membership epoch
  -> open local/remote shard sessions
  -> stream body bytes
  -> encode cluster stripes into kc + mc shard blocks
  -> append shard blocks to each target
  -> commit enough shard sessions for cluster commit mode
  -> persist .nmtd object record
  -> publish namespace symlink to @o_<object_id>
```

The important invariant is that the object is not visible as committed N2 truth until shard commit and `.nmtd` persistence succeed.

If a write drops below required commit shards, N2 aborts open sessions and cleans up committed shard fragments where possible. This prevents a partial object from becoming visible as a good cluster object.

## Read Path

The N2 read path is:

```text
client
  -> /n2/object/... or n2:// object.download
  -> fragment auth and permission check
  -> load local .nmtd object record, or recover from remote object-record replica
  -> open local and remote shard sources
  -> require at least kc readable shards
  -> reconstruct stream from available shards
  -> return bytes, supporting range when requested
```

N2 can read through one missing shard in a `kc=2, mc=1` layout. More generally, reads require at least `kc` usable cluster shards.

## Placement Model

Placement is deterministic and membership-aware. Given the same:

- fragment
- stable object id
- object uid
- version id
- membership epoch
- `kc`
- `mc`

N2 resolves the same target shard set.

The placement layer should spread primaries across nodes and should avoid a single coordinator becoming a permanent data owner. Any N2 node can accept a write, coordinate placement, and commit object truth.

## Membership And Governance

N2 membership is epoch-based.

Membership contains the node descriptors:

```json
{
  "node_id": "node-a",
  "endpoint": "https://10.0.0.1:50001",
  "epoch": 12,
  "active": true
}
```

N2 uses governance state to protect membership transitions and support rollback. Object writes can include `expected_epoch` so clients and operators can reject writes against stale cluster topology.

Cluster view is the first diagnostic surface when writes are rejected. A cluster can be locally healthy but not write-ready if membership, active node count, or commit policy does not allow the requested layout.

## Private Peer Plane

Peer routes are private N2-to-N2 traffic:

```text
/n2/internal/session/begin/...
/n2/internal/session/chunk/...
/n2/internal/session/commit/...
/n2/internal/session/abort/...
/n2/internal/store/...
/n2/internal/read/...
/n2/internal/delete/...
/n2/internal/object-record/...
/n2/internal/governance/...
```

Peer routes require internal peer headers and cluster-view proof. They are not application APIs.

The public API is `/n2/...`. The low-level native product protocol is `n2://`. The private node fabric is `/n2/internal/...`.

## Native Protocol Plane

`n2://` is N2's native low-level protocol. It uses the same length-prefixed MessagePack control-frame shape as `n1://`:

```text
TCP connection
  u32_be frame length
  MessagePack NativeRequest
    id
    op
    payload: JSON text
    body_len: optional raw body byte count
  raw body bytes when body_len is present
  MessagePack NativeResponse
    id
    ok
    status
    payload
    body_len
  raw response bytes when body_len is present
```

N2 0.7 streams native object PUT bodies into the N2 data plane. Large native uploads must not be collected into one giant protocol buffer before EC. Native download streams the response body after a small response control frame.

Core native operations:

- `ping`
- `auth.fragment`
- `auth.bearer`
- `auth.clear`
- `session.info`
- `cluster.view.public`
- `cluster.view`
- `governance.state`
- `membership.get`
- `membership.put`
- `membership.rollback`
- `placement.resolve`
- `object.put`
- `object.get`
- `object.download`
- `object.list`
- `object.versions`
- `object.delete`
- `object.rename`
- `reconcile.fragment`
- `health.node`
- `health.cluster`
- `health.fragment`
- `health.object`
- `repair.object`

`n2://` must call the same core data-plane functions as `/n2/...`. It is not masked HTTP, and it is not allowed to own separate metadata truth.

## Reconcile Model

Reconcile compares object records, namespace pointers, and shard references. It can:

- repair missing local `.nmtd` from remote replicas
- detect stale shard references
- clean orphan shard payloads when requested
- recover namespace/object-id drift from committed object records
- report objects that cannot meet the requested availability policy

Reconcile is the N2 equivalent of keeping distributed filesystem truth honest. It should be scoped by fragment and bounded by max object count when used operationally.

## Health Model

N2 health monitoring is explicit and layered:

- **Node health:** process RSS, index root readiness, blob root readiness, capacity, and reachability.
- **Cluster health:** governance alignment, write readiness, reachable active nodes, storage-ready nodes, and optional deep fragment scan.
- **Fragment health:** bounded scan of object records inside one fragment.
- **Object health:** one object/version, shard presence, stale placement, readability, repairability, and loss state.

Health states are:

- `healthy`: all expected shards are present on current placement targets.
- `degraded`: the object or cluster is readable but missing redundancy, stale placement, or full write readiness.
- `critical`: there are some surviving shards/nodes but below read quorum.
- `lost`: no usable active node/shard remains for the inspected target.

Object repair is object-scoped. It reconstructs missing/stale target shards from available shards and persists a corrected `.nmtd` object record. Fragment reconcile remains the broader maintenance path for topology changes and bounded bulk repair.

Repair is explicitly gated:

- a global repair semaphore bounds total repair work per node
- a per-fragment semaphore prevents one hot fragment from consuming every repair slot
- an object-scoped semaphore prevents duplicate repair/scrub work on the same object
- saturation returns `503`, preserving backpressure instead of growing memory

Automatic maintenance uses a health preflight before reconcile. Healthy fragments
are counted as scanned without entering byte-heavy shard reconstruction. This
keeps steady-state monitoring cheap while still allowing degraded objects to be
repaired or redistributed when health reports missing or stale shard placement.

Normal full-object reads in the minimum `kc=1, mc=1` layout use a direct data-shard
streaming path when the data shard is available. N2 only enters reconstruction
when the required data shard is unavailable, a byte range needs reconstruction,
or the cluster layout requires multi-shard decode.

Real two-node proof on 2026-05-26 validated:

- node/cluster/fragment/object health endpoints
- 304 MiB real-video upload through node A
- full download through node B with SHA-256 match
- remote shard deletion reporting `degraded`
- read survival while degraded
- periodic automatic repair returning object health to `healthy`
- node B outage reporting cluster `critical`
- read survival during node outage
- node B rejoin returning cluster health to `healthy`

The same proof showed steady-state healthy monitoring and normal download staying
near low RSS. N2 0.7.1 moves repair away from whole-shard `Vec<u8>` rebuilds:
the common `kc=1` repair path stream-copies surviving shard material, and wider
EC layouts rebuild missing targets stripe-by-stripe through bounded temporary
files. Degraded reads can still allocate decode buffers for the active request,
but repair no longer needs to retain whole cluster shards in process memory.

## Failure Model

N2 is designed around node-level failures, not only disk-level failures.

Examples:

- If one node disappears in `kc=2, mc=1`, reads can continue from the remaining two shards.
- If a local `.nmtd` replica is missing, N2 can fall back to remote object-record replicas.
- If a shard is orphaned after failed writes or membership changes, reconcile can identify and remove it.
- If membership changes, expected epochs protect clients from writing against stale topology.

N2 depends on N1 for local storage-kernel strength, but N2's distributed durability comes from cluster placement and cluster EC.

## Performance Shape

N2 performance is dominated by:

- network bandwidth between nodes
- cluster EC encoding
- target node disk throughput
- remote shard session latency
- commit policy
- object size and shard count

The default N2 0.7 path avoids double EC. For large objects, that is the correct performance posture. For tiny objects, the same durable object-storage tradeoff applies: many small objects amplify metadata and commit overhead. Horizontal distribution helps, but it does not make tiny durable objects free.

## Public Identity

Public documentation should call this system N2, the N2 cluster namespace, N2 object records, N2 cluster shards, LOA `/n2`, and native `n2://`.

Implementation terms such as `.nmtd`, `n2/namespace`, `n2/objects`, `n2/shards`, and `/n2/internal` are architecture terms. They are valid for internals and operator diagnostics, but the product identity is N2.
