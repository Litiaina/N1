# N1 Monolith Raw-Kernel Plan

> Experimental track note:
> - This document is for monolith experimental development only.
> - Stable/default production deployment remains `standard`.
> - See `DEPLOYMENT_PROFILES.md`.

This document defines the target structure for `N1 monilith` as a raw-only storage kernel:

- `blob` on raw devices (already in place)
- `index` on raw devices (new target)
- no filesystem-backed legacy `fs` hot path

Scope note:
- This is for `N1 monilith` only.
- `N1 standard` remains filesystem-backed and stable.
- Do not sync this in-progress monolith work to `N2` until monolith passes stability gates.

## 1. Target Planes

## 1.1 Raw Index Plane (new authority)

Raw index stores all authoritative metadata:

- session manifests
- committed part receipts
- object latest/version metadata
- trash/version tombstone metadata
- quota state
- WAL + checkpoints for crash recovery

This replaces filesystem index directories for monolith authority.

## 1.2 Raw Blob Plane (existing authority for payload)

Raw blob remains payload plane:

- extents for data/parity shards
- direct IO read/write
- relocation and deferred free queues

## 1.3 Derived Namespace Plane (non-authoritative)

Prefix/delimiter list acceleration stays derived from index WAL/checkpoints:

- never source of truth
- rebuildable
- async projection

## 2. Module Layout (Monolith)

Target module structure under `monilith/src/n1_core`:

```text
n1_core/
  raw_index/
    mod.rs
    types.rs             # index record schemas (session/object/trash/quota)
    codec.rs             # fixed + msgpack codec for index records
    device.rs            # direct extent IO wrappers for index devices
    partition_table.rs   # partition->owner/replica mapping + epoch
    wal.rs               # append-only records + replay cursor
    checkpoint.rs        # durable snapshots per partition
    session_store.rs     # upload session state read/write
    object_store.rs      # latest/version publish + lookup
    trash_store.rs       # trash/recover metadata paths
    quota_store.rs       # quota accounting state
    gc_queue.rs          # deferred reclaim queue records
    repair.rs            # replica/index reconvergence
    iter.rs              # fragment/partition walkers (streaming)
  index.rs               # facade: delegates to raw_index backend
  blob_store.rs          # existing raw blob path (kept)
  upload.rs              # init/chunk/finalize against raw index facade
  retrieve.rs            # exact-key/list path via index facade
  delete.rs modify.rs    # metadata mutations via index facade
  garbage_collector.rs   # uses index gc_queue + blob deferred free
  repair_manager.rs      # reads raw index + blob repair surfaces
  redistribution.rs      # republish via raw index object_store
```

Design rule:
- all control-plane modules (`upload`, `retrieve`, `delete`, `modify`, `repair`, `gc`) talk to an index facade, not direct filesystem paths.

## 3. Raw Index Logical Layout

Each index raw device is formatted with an index allocator + journals.

Per partition logical contents:

- WAL segment ring
- latest checkpoint pointer
- immutable metadata payload records
- key->record pointer table

Logical keys:

- `sess:{fragment}:{version_id}`
- `part:{fragment}:{version_id}:{part_index}`
- `objv:{fragment}:{object_key_hash}:{version_id}`
- `objl:{fragment}:{object_key_hash}` (latest pointer)
- `trash:{fragment}:{object_key_hash}:{version_id}`
- `quota:{fragment}`

Durability order:

1. append WAL intent
2. write record payload
3. atomically update key pointer
4. optional checkpoint compaction

## 4. Migration Phases

## Phase 0: Abstraction boundary

- Introduce index facade trait used by control-plane code.
- Move direct index filesystem reads/writes behind facade impl.
- Keep existing behavior; no semantic change.

Exit criteria:
- all n1_core metadata paths compile through facade calls.

## Phase 1: Raw index backend (write-through shadow)

- Add `raw_index` backend implementation.
- For a transition window, dual-write:
  - existing fs index backend
  - new raw index backend
- Read path still fs-first with parity checks.

Exit criteria:
- WAL replay + checkpoint restore tested.
- fs vs raw index metadata parity test passes on workflow suite.

## Phase 2: Raw index read authority

- Switch monolith read authority to raw index.
- fs index write path disabled.
- Prefix projection sourced from raw index stream.

Exit criteria:
- CRUD + multipart + list + trash + recover pass under raw-index-only reads.

## Phase 3: Remove legacy fs hot path

- Remove legacy `fs` routes and handlers in monolith.
- Remove fs cleanup loop dependencies from monolith boot/GC.
- Keep only object APIs and fsd gateway backed by object semantics if needed.

Exit criteria:
- no `crate::fs::*` dependency in monolith runtime hot path.

## Phase 4: Maintenance convergence

- repair/redistribution/gc use raw index iterators only.
- eliminate duplicate metadata write paths.
- stabilize log severity and summary aggregation.

Exit criteria:
- hotplug, disk-loss, repair-mode matrix stable on real hardware.

## Phase 5: Hardening gates before N2 sync

- long-run soak tests
- crash/restart replay loops
- large-object parallel upload/read stress
- repair + redistribution correctness checks

Gate:
- only after Phase 5 passes do we mirror monolith core changes to `N2`.

## 5. Locking And Concurrency Policy

Target policy:

- lockless hot upload path at request level
- bounded allocator/domain locks only at partition/AG scope
- no global mutex in hot data path
- WAL append and pointer updates are short critical sections

## 6. Non-Negotiable Invariants

- object visibility only after authoritative metadata publish
- raw blob extents without live metadata are not visible
- checksum/EC integrity must hold across repair and read recovery
- strict mode: read-only while repair accelerates
- scrub mode: mutation-disabled and repair/redistribution prioritized
- availability mode: CRUD-first with bounded background repair

## 7. Immediate First Implementation Steps

1. Add index facade trait and fs-backed adapter.
2. Refactor upload/retrieve/delete/modify to use facade methods only.
3. Remove direct filesystem index path construction from those flows.
4. Add raw-index backend skeleton with WAL + session/object key spaces.
5. Enable dual-write shadow mode for validation.
