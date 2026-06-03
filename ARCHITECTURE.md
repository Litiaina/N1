# N2 Architecture

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

N2 owns distributed namespace, object identity, cluster layout, node placement,
peer transport, repair, rebalance, and health.

## Core Model

```text
client
  -> any N2 node
  -> namespace/object operation
  -> cluster layout decision
  -> shard/parity placement across nodes
  -> immutable object and placement records
```

Every N2 node can coordinate object IO. There is no permanent object-IO leader.

## Bidirectional Coordination

N2 is bidirectional at the node level. Any healthy node can receive a client
operation and become the coordinator for that operation.

```text
client A -> node-a -> upload object-a
client B -> node-b -> upload object-b
client C -> node-c -> list, download, rename, delete, recover
```

There is no fixed leader for normal object IO. Coordination is per operation,
not per cluster.

Multipart upload sessions are intentionally coordinator-owned until finalize.
The node that accepts `init` owns that temporary `.uploads` session state:

```text
init multipart on node-a
put part/status/finalize for that session -> node-a
```

The session coordinator records resumability state locally:

- upload manifest
- session id and version id binding
- uploaded part metadata
- part and shard receipts
- finalize readiness

This is deliberate. Temporary multipart session state is hot, high-churn, and
not final object truth. Replicating every in-flight part receipt through the
cluster would add metadata chatter and race surfaces without improving the
committed object model.

The data plane is still distributed during the upload. Each part is encoded and
written as shard/parity data across the selected cluster nodes. After finalize,
the immutable object metadata, namespace entry, and placement record become
cluster-visible and any node can list, read, download, or inspect the object.

Load balancers do not need storage intelligence. They only need to:

- route new operations to any healthy N2 node
- keep multipart part/status/finalize requests sticky to the node that returned
  the multipart session

If a coordinator dies before finalize, the in-flight multipart upload can be
restarted. Finalized objects remain cluster-visible and are not tied to the
coordinator that created them.

## Storage Planes

Each node has index roots and blob roots.

```text
<index_root>/
  .fragment/<fragment_alias_hash> -> ../@f_<stable_fragment_id>

  @f_<stable_fragment_id>/
    videos/
      demo.mp4 -> @o_<object_id>

    .objects/
      ab/
        cd/
          ef/
            12/
              @o_<object_id>/
                <version_id>.<size>.<time>.<ext>.ncdx

<blob_root>/
  .fragment/<fragment_alias_hash> -> ../@f_<stable_fragment_id>

  @f_<stable_fragment_id>/
    .blobs/
      <version_id>/
        part_0_shard_0.shard
        part_0_shard_1.parity
```

The mutable fragment alias is an entrypoint. Storage truth resolves to the
stable fragment id.

## Object Records

Object records are immutable. New content creates a new version record.

`.ncdx` means N2 cluster data index. It is the single immutable object metadata
record for a committed N2 object version.

Each object record stores:

- stable fragment id
- object key at commit time
- object id
- version id
- layout epoch
- resolved EC layout
- shard refs by node
- size and content metadata
- timestamps

The latest object version is selected from immutable `.ncdx` filenames, not
from a mutable latest pointer. N2 does not use a separate cluster object
metadata plane; the object record already owns distributed placement.

## Namespace Boundary

Namespace state is mutable. Object and blob payload state is immutable.

Namespace mutations include:

- create namespace entry
- rename namespace entry
- trash/recover namespace entry
- folder mutation
- membership and layout epoch changes

Payload bytes are not consensus state. Payloads are streamed to their holder
nodes, committed as immutable shard/parity files, and referenced by immutable
placement records.

## Layout Modes

N2 supports two layout modes:

```text
auto
fixed
```

Auto mode derives the cluster EC shape from writable node count:

```text
1 node  -> 1 + 0
2 nodes -> 1 + 1
3 nodes -> 2 + 1
4 nodes -> 3 + 1
```

Fixed mode lets the operator explicitly set data and parity shard counts.

Every object version records its resolved layout. Adding nodes does not make old
objects unhealthy. Old objects remain valid under their recorded layout; new
objects use the current layout.

## Write Path

```text
client upload
  -> any N2 node
  -> coordinator validates auth, fragment, membership, and capacity gates
  -> coordinator resolves cluster layout
  -> coordinator streams bytes through cluster EC
  -> target nodes persist .shard/.parity files
  -> coordinator commits immutable object and .ncdx records
  -> namespace mutation is applied through the cluster mutation path
```

Multipart upload is the preferred large-object path. Each part is independently
streamed, recorded, and finalized into the committed object version.

Multipart resumability is coordinator-owned. A resumed multipart upload should
return to the node that created the session. This keeps temporary upload state
simple and avoids cluster-wide consensus on every part receipt while preserving
distributed shard placement and cluster-wide final object visibility.

## Read Path

```text
client read
  -> any N2 node
  -> namespace symlink resolves object id
  -> immutable object record resolves version
  -> .ncdx resolves recorded shard placement
  -> coordinator fetches needed shard ranges from holder nodes
  -> healthy path streams data shards
  -> degraded path reconstructs from parity
```

Reads use the object version's recorded layout. They do not assume the current
cluster layout matches old objects.

## Maintenance

N2 maintenance is cluster-level:

- health checks node reachability and shard availability
- repair restores missing shards for recorded layouts
- rebalance rewrites selected objects onto a newer layout when requested
- GC removes unreachable immutable records and blobs

N2 distinguishes:

```text
healthy old layout
degraded object
critical object
lost object
rebalance available
```

An old layout is not a health failure.

## Adaptability Rule

N2 does not force redistribution just because a node was added.

```text
new layout available != existing objects are unhealthy
```

That keeps the system adaptable:

- add node: new objects can use it immediately
- remove node: affected recorded layouts degrade based on actual reachability
- re-add node: recorded layouts can become healthy again
- rebalance: explicit operator action

## Quota Rule

N2 does not enforce per-fragment quota. Fragment quota is monitoring metadata.

Cluster storage is distributed, so usage is reported through explicit monitoring
calls instead of being treated like single-node physical quota.

## Protocol Surfaces

N2 has two protocol surfaces:

- `/n2/...` HTTP API for browsers, external clients, and compatibility
- `n2://` native protocol identity for N2-native tools and node-to-node work
