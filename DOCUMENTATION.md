# N2 3.0 Documentation

N2 is a distributed object storage engine with immutable per-object erasure
layouts and a flat, namespace-managed logical key plane. Version 3.0 is a new
engine and disk format. It does not read or migrate N2 2.x inode state.

The authoritative invariants are in [ARCHITECTURE.md](ARCHITECTURE.md), the
public protocol is [NOA.md](NOA.md), and deployment is
[INSTALLATION.md](INSTALLATION.md).

## Engine model

N2 3.0 has three immutable record types:

- NCNX maps one logical key to one stable object ID.
- NCDX describes one immutable object version and its exact EC layout.
- Shard/parity files contain immutable encoded object bytes.

Every node in a server pool is unified: it accepts client operations and can
hold NCNX, NCDX, shards, parity, uploads, and fragment state. There are no
namespace-only or blob-only node roles.

Each writable pool has a static `K + M` contract. An upload requires all
`K + M` distinct pool nodes. N2 does not reduce parity to fit a failed node.
NCNX has `M + 1` deterministic replicas. Every committed NCDX records exact
pool generation, nodes, roots, roles, sizes, and checksums, so topology growth
cannot reinterpret existing data.

## Fragments

A fragment is N2's virtual object-partition boundary. It provides naming,
authentication, lifecycle, listing, and maintenance scope. It is not tied to a
single server pool: objects in one fragment can retain layouts from multiple
pool generations.

Fragment aliases are hashes pointing to a stable fragment ID. Renaming a
fragment changes its contract boundary and therefore coordinates across the
cluster. It is an administrative operation, not a hot object mutation path.

## Logical keys and prefixes

The namespace contains files, not directory inodes:

```text
videos/a.mp4
videos/b.mp4
images/c.jpg
```

On an NCNX holder these become physical key directories and small records:

```text
.namespace/
  videos/a.mp4.ncnx
  videos/b.mp4.ncnx
  images/c.jpg.ncnx
```

`videos/` is a physical prefix and client-visible common prefix. It has no
mutable inode, lock, generation, or shared transaction. `mkdir` therefore does
not preserve an empty directory. Adding or mutating `videos/a.mp4` never
rewrites directory state shared with `videos/b.mp4`.

LIST reads paths and filenames only. Each node emits keys for which it is the
canonical deterministic replica; replica copies are skipped mathematically.
The coordinator merges bounded pages and returns an opaque continuation token.
LIST does not decode NCNX, read NCDX, or touch object bytes.

Local enumeration is a prefix-pruned lexical directory walk. It sorts only the
direct entries of each visited physical directory, so working memory is
proportional to the widest visited directory rather than the whole namespace.
An exact key and a same-name prefix (for example `a` and `a/child`) retain true
logical order across continuation pages.

## Object write path

1. Normalize and validate the logical key.
2. Select the newest complete writable static-EC pool.
3. Generate stable object/version identities and deterministic node/root
   placement.
4. Stream-encode each part into exactly `K` shards and `M` parity shards.
5. Durably publish byte-identical immutable NCDX to the exact object holders.
6. Publish identical immutable NCNX to all `M + 1` key holders.
7. Acknowledge only after both object and namespace durability succeed.

One-shot upload is for bounded request bodies. Multipart upload pins its
coordinator and layout at initiation; part and finalize requests must continue
through that coordinator. A lost required pool node stops finalization instead
of silently changing EC.

## Read path

1. Deterministically read the NCNX replica set for the logical key.
2. Validate identical surviving NCNX bytes and obtain the object ID.
3. Read immutable NCDX by physical object ID—without resolving namespace a
   second time.
4. Fetch the required shard ranges concurrently.
5. Verify recorded checksums and reconstruct when at least `K` valid shards
   survive.
6. Stream the result to the client; recover-on-read may restore a missing local
   shard after serving valid data.

NCNX absence is accepted only when every assigned replica responds with
absence. An unreachable holder makes absence unproven, preventing a false 404.
A valid identical NCNX survivor can tolerate another corrupt replica; divergent
valid replicas fail closed.

## Namespace mutation

Exact-key rename and move are destination-first copy/delete:

1. Read and validate the source NCNX.
2. Prove that the destination is absent or already points to the same object.
3. Publish a new destination NCNX mutation generation to all exact replicas.
4. Remove only source bytes matching the expected immutable record.

Retries reapply the same expected bytes. They cannot delete a newer same-key
record. A crash may temporarily leave both names, never neither.

Folder rename and move enumerate the matching live prefix and run that same
independent copy/delete protocol with bounded concurrency. Folder trash and
recover use the lifecycle protocol below. All are restart-safe per key, but
intentionally not one atomic transaction across an unbounded prefix.

## Trash and TTL lifecycle

Delete publishes:

```text
.trash/<logical-key>.ncnx.trashed.<unix-nanos>.<mutation-ulid>
```

before removing the expected live NCNX. Recover publishes live first and
removes trash last. A folder trash/recover uses one mutation group so every
matching key can be recovered together.

Normal GC scans only `.trash`. Expiry comes from the trash filename timestamp;
there is no mutable lifecycle database. Before reclaiming object data, GC
builds the current live/unexpired reference set. Live always wins over trash,
object holders are purged before the final expired pointer, and errors preserve
the pointer for retry.

Headless immutable objects are reported but not guessed disposable. A lost
namespace pointer is an authority failure, so automatic orphan deletion would
be unsafe.

GC modes are explicit:

- availability: foreground access continues during the bounded scan;
- strict: the maintenance gate protects the scan from concurrent mutation;
- fast: reduced checking, explicitly operator selected.

## Autonomous healing

The HA coordinator set is deterministic and bounded. Each fragment has one
time-bounded owner per cycle; independent fragments can heal in parallel.
Progress is restartable and repair remains per record/object.

For each NCNX key, healing:

- reads the exact deterministic replica set;
- validates fragment, object ID, integrity, and identical immutable bytes;
- restores a missing or corrupt replica from valid authority;
- reports divergence or unrepairable absence;
- defers live repair only when an active delete tombstone contains the exact
  same NCNX generation bytes.

For each NCDX object version, healing:

- validates exact recorded holders and checksums;
- reconstructs missing/corrupt shards when at least `K` valid shards remain;
- republishes only the missing immutable layout component;
- never changes EC or redistributes an object implicitly.

Each degraded or repaired item produces an independent JSON report containing
fragment, logical filename when known, object/version identity, damaged
component, action, and result. Reports are disposable and are not replicated
or consulted as storage truth.

If namespace authority becomes ambiguous or a layout is unrepairable, N2
reports the exact failure and its safety latch can reject unsafe mutations.
Readable immutable data remains available when its recorded layout still has
enough valid shards.

## Repair versus redistribution

Repair restores an object's existing recorded layout. Redistribution is an
operator-requested re-encode into the current writable pool and commits a new
immutable layout/version. Adding a pool never starts redistribution.

This preserves the object-level EC property: old and new objects may use
different exact layouts while remaining independently readable and repairable.

## Expansion

Add capacity by defining a new pool generation with its own complete static
`K + M` nodes. New writes select the newest complete writable pool. Seal older
pools when they should receive no new data; keep them online for reads and
healing.

Do not mutate an enrolled pool's EC width, node IDs, or per-node root-ID set.
Root identity records reject accidental rebinding, static-EC changes, and
in-place root-set changes. Add a new complete pool for capacity expansion.

## Health states

Cluster health separates these questions:

- Can the newest pool satisfy exact `K + M` publication?
- Can NCNX publish all `M + 1` replicas?
- Is an existing object healthy, degraded-but-repairable, or unrepairable?
- Is a namespace record healthy, degraded, divergent, or absent?

Adding a pool does not mark old objects degraded. Losing a required current
pool node rejects new writes, while an existing object with at least `K` valid
shards may still be read.

## NOA operations

The canonical external prefix is `/noa/v1`. Major groups are:

```text
/noa/v1/auth/*
/noa/v1/fragments/*
/noa/v1/objects/*
/noa/v1/upload/one-shot/*
/noa/v1/upload/multipart/*
/noa/v1/posix/*
/noa/v1/maintenance/*
/noa/v1/cluster/*
```

NOA `posix` means familiar path/list/stat/rename/trash/recover semantics. N2 is
not a kernel POSIX filesystem and does not promise atomic folder-wide rename or
empty directory persistence.

See [NOA.md](NOA.md) for the route contract. Trusted node-to-node endpoints
remain under `/n2/peer/...`, and the native protocol remains `n2://`.

## Operational rules

- Keep membership, time, TLS trust, JWT secret, and cluster peer secret
  consistent on every node.
- Use stable device mounts and never recycle root IDs for different media.
- Treat static EC as a production contract.
- Restart through systemd after an intentional configuration change.
- Inspect healing reports and health before replacing media.
- Never delete immutable object directories to clean namespace mistakes.
- Never use `--force-config` without reviewing the exact generated diff.

## Verification boundary

The release proof is finite and repeatable. It uses four logical nodes on two
physical hosts, static 2+2 EC, NCNX R=3, isolated roots, multipart data, cross-
node checksum reads, namespace mutation, healing, TTL GC, node loss, fail-
closed writes, and full restart. It records latency, throughput, PID CPU/IO/
memory, logs, and disk layout under `/home/wlzambo/DEV-TEST`.

That evidence proves the tested architecture and failure cases. It does not
claim exabyte deployment, independent failure domains for simulated nodes, or
long-duration production soak.
