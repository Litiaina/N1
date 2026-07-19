# Litiaina N2 3.0

N2 is a high-performance distributed object storage engine built around
immutable object-level erasure sets and a flat, namespace-managed key plane.
Version 3.0 is a clean engine generation. It does not open or migrate N2 2.0
inode layouts.

The authoritative design is [ARCHITECTURE.md](ARCHITECTURE.md). The public API
contract is [NOA.md](NOA.md), deployment is [INSTALLATION.md](INSTALLATION.md),
and current proof evidence is recorded in
[N2_3_0_VERIFICATION.md](N2_3_0_VERIFICATION.md).

## N2 3.0 DNA

- Every server-pool node is unified and bidirectional. Any node can coordinate
  requests, and every node can hold NCNX, NCDX, shards, and parity.
- Each server pool has a static `K + M` EC contract. N2 never silently shrinks
  an object's durability to fit a degraded topology.
- Each object version records its exact pool generation, EC width, holders,
  roots, shard roles, sizes, and checksums in immutable NCDX.
- Each logical key is one small immutable NCNX record replicated to exactly
  `R = M + 1` deterministic holders in its namespace home pool.
- LIST reads filenames and physical prefixes only. It does not decode NCNX,
  open NCDX, or touch object data.
- Rename and move are destination-first live copy/delete. Trash and recover are
  destination-first live/trash copy/delete. All touch namespace records only.
- Adding a new pool increases future capacity without moving existing objects.
  Redistribution is always explicit object re-encoding.
- Autonomous healing verifies and repairs exact recorded NCNX, NCDX, shard,
  and parity layouts. Disposable JSON reports are never storage authority.

## Physical Layout

```text
<storage_root>/
  .n2-root.ncdx
  .fragment/
    <fragment-alias-hash> -> ../@f_<stable-fragment-id>

  @f_<stable-fragment-id>/
    .fragment.ncdx
    .namespace/
      videos/video.mp4.ncnx
    .trash/
      videos/video.mp4.ncnx.trashed.<unix-nanos>.<mutation-ulid>
    .uploads/
    .objects/
      ab/cd/ef/12/@o_<object-id>/
        <version>.l<pool-generation>.<size>.<unix-nanos>.<ext>.ncdx
        <version>/
          part_0_shard_0.shard
          part_0_shard_1.shard
          part_0_shard_2.parity
```

The physical `videos/` directory is only key-prefix partitioning. It is not an
inode, lock, transaction participant, or durability group. Empty directories
are not persisted; clients derive them from keys.

## Server Pools

```text
cluster
├── pool-a, generation 1, EC 4+2
│   └── nodes a1..a6
├── pool-b, generation 2, EC 4+2
│   └── nodes b1..b6
└── fragments
    ├── videos
    └── documents
```

Fragments are bucket-like authentication, lifecycle, and policy boundaries.
They are not server pools. One fragment can contain objects whose exact layouts
belong to several pool generations.

New writes use the newest complete writable pool. Old objects remain readable
and healable from their recorded layouts. A missing pool node stops unsafe new
writes; it does not globally label every existing object as degraded.

## Namespace Semantics

An NCNX record maps one normalized key to one stable object ID. It contains
only fragment identity, object identity, mutation identity, creation time, and
an integrity checksum. Object size, content type, versions, and EC layout stay
in NCDX.

Independent keys never update shared directory state. The unavoidable conflict
boundary is one exact logical key. Prefix operations enumerate matching keys
and execute the same one-key protocol with bounded concurrency. They are
restartable per key and intentionally non-atomic across the entire prefix.

Mkdir is not persisted because N2 3.0 has no empty-directory records. Uploading
`videos/a.mp4` makes the `videos/` prefix visible.

## Lifecycle

Delete first publishes a uniquely named trash NCNX on all exact replicas, then
removes only the expected live NCNX bytes. Recover publishes live first and
removes trash last. Repeated and interrupted requests remain safe at the
one-key boundary.

Normal GC scans `.trash`, derives expiry from its filenames, and applies the
rule **live wins over trash**. An expired pointer cannot reclaim an object that
still has a live key or another unexpired trash reference. Object holders are
purged before the final trash pointer is removed.

Headless immutable objects are reported and preserved. N2 does not guess that
missing namespace authority means the object is disposable.

## Build And Test

```bash
cargo fmt --all -- --check
cargo check -p n2
cargo test -p n2 --lib -- --test-threads=1
cargo build -p n2 --release
```

The finite release proof uses four logical nodes, static 2+2 EC, three NCNX
replicas, two physical hosts, and isolated temporary roots:

```bash
REMOTE_PASS='operator-supplied' \
  scripts/bench/run_n2_3_0_release_proof.sh
```

It proves NOA CRUD, multipart upload, cross-node reads, zero-decode listing,
folder trash/recovery, NCNX repair, shard repair, TTL GC, node-loss read/write
behavior, and full restart persistence. It also records latency distributions,
throughput, PID CPU/IO/memory samples, logs, and physical layouts under
`/home/wlzambo/DEV-TEST` by default.

This is bounded architectural evidence, not a claim of exabyte deployment or a
substitute for long-duration independent-hardware soak testing.

## Runtime Files

Every node requires:

```text
n2.env
n2_config.ini
<runtime_path>/n2_membership.json
TLS certificate and key
one or more enrolled storage roots
```

All nodes must use the same JWT and cluster peer secrets and the same complete
format-v3 membership. Each node has a unique stable node ID, endpoints, and
root IDs. Root identity prevents accidental rebinding, silent static-EC
changes, or in-place root-set changes that would remap NCNX.

Use [INSTALLATION.md](INSTALLATION.md) for the full configuration and systemd
procedure.

## API

The canonical surface is `/noa/v1`. Important groups are:

- `/noa/v1/fragments` and `/noa/v1/auth/fragment`
- `/noa/v1/objects`, `/objects/download`, `/objects/rename`
- `/noa/v1/upload/one-shot` and `/upload/multipart`
- `/noa/v1/trash` and `/objects/recover`
- `/noa/v1/posix/list`, `/stat`, `/rename`, `/trash`, `/recover`
- `/noa/v1/cluster/health`, `/monitor`, `/healing/cycle`
- `/noa/v1/maintenance/gc/*`, `/repair/*`, `/redistribution/*`

NOA standardizes the external contract. It does not weaken N2's recorded
layout, strict publication, or flat NCNX invariants.
