# N2 Storage Architecture

N2 is Litiaina's standalone distributed object storage system.

N1 and N2 are independent systems. They share Litiaina storage DNA: real
namespaces, immutable object identity, zero-decode metadata records, explicit
blob roots, and operator-controlled maintenance. N2 applies those principles at
cluster scope, where the placement unit is a node:

```text
N2 placement unit: node
```

N2 owns its own cluster namespace, cluster object records, shard placement,
peer protocol, repair, rebalance, and health model.

## Core Rule

```text
N2 owns distributed namespace, object identity, cluster layout, node placement,
and cluster maintenance.

N2 uses Litiaina object-storage principles directly: real namespace
directories, symlink object identity, immutable version records, and immutable
.shard/.parity blobs.
```

N2 does not use the discarded local `disk_shard_0.bin` layout. N2 also does
not keep `kd/md` as a cluster concept. Local disk behavior is the node's
storage layout; cluster durability is the N2 node-level EC layout.

## Disk Layout

Each N2 node has index roots and blob roots.

```text
<index_root>/<fragment_hash>/
  namespace/
    videos/
      demo.mp4 -> @o_<object_id>

  .objects/
    ab/
      cd/
        @o_<object_id>/
          <version_id>.<size>.<time>.<ext>.ndx

  .cluster/
    objects/
      ab/
        cd/
          ef/
            12/
              @o_<object_id>/
                <version_id>.nmtd

<blob_root>/<fragment_hash>/
  .blobs/
    <version_id>/
      part_0_shard_0.shard
      part_0_shard_1.parity
```

The namespace is user-facing. The `.objects` directory is the local object
index used for namespace/list/download semantics. The `.cluster/objects`
directory stores N2 cluster placement records. The blob files are immutable
cluster shard payloads.

## `.nmtd`

`.nmtd` means N2 metadata. It is the immutable cluster-object placement record.

Each committed version record stores:

- fragment
- object key at commit time
- stable object id
- version id
- layout epoch
- cluster layout mode and resolved EC layout
- shard refs by node
- size, checksum, content type, and timestamps

There is no mutable latest file. Latest is determined by immutable version
records, not a `.latest` pointer.

## Namespace Consensus Boundary

Consensus belongs on mutable namespace and membership state.

Consensus covers:

- create namespace entry
- rename namespace entry
- delete/trash/recover namespace entry
- folder mutation
- membership and layout epoch updates

Consensus does not wrap every payload byte. Object records are immutable, and blobs are immutable.

## Cluster Layout

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

Every object version records the resolved layout. Adding nodes or disks does not force immediate rebalance. Old objects remain valid under their recorded layout; new objects use the current layout. Rebalance is explicit operator work.

## Write Path

```text
client upload
  -> any N2 node
  -> coordinator validates namespace/membership epoch
  -> coordinator derives or reads cluster layout
  -> coordinator streams object bytes through cluster EC
  -> data/parity shards are sent to target nodes
  -> target nodes persist .shard/.parity blobs
  -> coordinator commits immutable object record and .nmtd placement record
  -> namespace mutation is committed through consensus
```

Every N2 node can coordinate. There is no permanent leader for object IO.

## Read Path

```text
client read
  -> any N2 node
  -> namespace symlink resolves object id
  -> latest immutable object record gives version layout and shard refs
  -> coordinator streams required shards from target nodes
  -> healthy path streams directly when enough data shards are present
  -> degraded path reconstructs from parity
```

Read uses the recorded object layout, not the latest cluster layout.

## Maintenance

N2 maintenance operates at cluster level:

- health checks node reachability and shard availability
- repair restores missing shards for the recorded object layout
- rebalance rewrites selected objects to a newer layout only when requested
- GC removes unreachable immutable records and blobs

N2 must distinguish:

```text
healthy old layout
degraded object
critical object
lost object
rebalance available
```

An old layout is not a health failure.

## Adaptability Rule

N2 does not force redistribution just because a node or disk was added.

```text
new layout available != existing objects are unhealthy
```

Each committed object version records the membership epoch and resolved placement used at commit time. When the cluster changes, new writes use the current membership and layout policy. Existing objects continue to read from their recorded placement until an operator explicitly runs rebalance.

That keeps N2 adaptable:

- add node: new objects can use it immediately
- remove node: affected recorded layouts become degraded/critical based on reachability
- re-add node: recorded layouts can become healthy again
- rebalance: explicit operator action, never silent mandatory churn

## Current 0.12.0 State

N2 0.8.0 was the destructive reset point. N2 0.9.0 added the first distributed
control plane. N2 0.10.0 moved into distributed execution. N2 0.11.0 hardened
native peer transport, `.nmtd` placement records, repair/rebalance status, and
two-node proof coverage. N2 0.12.0 hardens namespace mutation flow so upload
commits, POSIX/object mutations, recover-all, and empty-trash pass through a
shared quorum commit path.

Current base:

- crate: `n2`
- binary: `litiaina-n2`
- config: `n2_config.ini`
- env: `n2.env`
- HTTP route prefix: `/n2/...`
- native protocol: `n2://`
- storage shape: N2 namespace/object/blob planes
- membership file: `n2_membership.json`
- cluster layout policy: `auto` or `fixed`
- deterministic node placement preview
- namespace mutation ledger foundation
- cluster health, repair-plan, and rebalance-plan routes
- peer shard transport for remote `.shard` / `.parity` holders
- distributed one-shot upload
- distributed multipart upload
- distributed range/chunk read with parity reconstruction
- namespace quorum apply for upload commits and namespace mutation operations
- per-fragment namespace commit gate for local mutation serialization
- cluster health reports adaptive layout separately from optional rebalance debt
- repair/rebalance executor start/status routes
- persisted repair/rebalance status snapshots
- native `n2://` peer transport for shard put/get
- two-node proof script for multipart resume, reconstruction, repair, rebalance,
  health, and RSS checks

The remaining hard work before 1.0 is not a new storage shape. It is proof and
hardening: longer node-loss/re-add testing, membership reload safety, long soak,
and operator CLI/client polish.
