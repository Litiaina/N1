# N1 Block Allocator V2 (BM-V2) Design

> Scope note:
> - This document describes the arc allocator track.
> - Arc is stable at `0.2.0`.
> - Stable/default deployment remains `standard` (`DEPLOYMENT_PROFILES.md`).

This document defines the production target allocator design for N1 raw blob devices.

Hardening and certification work (formatter safety + crash/powerfail stability gates) is tracked in [`BLOCK_ALLOCATOR_HARDENING_PLAN.md`](BLOCK_ALLOCATOR_HARDENING_PLAN.md).

It is specialized for N1 workloads:
- immutable shard writes
- reservation-first multipart uploads
- background repair and redistribution
- single-node durability with multi-disk HA behavior

It is not a general-purpose filesystem allocator.

## 1. Design Goals

1. Keep chunk upload hot path as pure byte pipe to pre-reserved extents.
2. Keep allocator operations predictable at million-object scale.
3. Make recovery/replay fully deterministic and idempotent.
4. Make disk topology changes an expected control-plane event, not a hard-failure path.
5. Keep availability-mode service latency stable while maintenance runs in background.

## 2. Core Model

BM-V2 uses:
- AG-style allocator domains per raw device
- dual free-space indexes per AG
- WAL-first allocation metadata with transaction ids
- topology epoch and device map versioning
- deferred free and bounded background coalescing

Each raw device is independently managed. Cross-device behavior is orchestrated by N1 index/session metadata, not by shared allocator state.

## 3. On-Disk Metadata Layout (Per Raw Device)

```text
block 0            superblock_v2
block 1..J         wal area (segmented)
block J+1..K       AG descriptor table + allocator checkpoint table
block K+1..end     data region split into AGs
```

### 3.1 Superblock V2

Required fields:
- `magic = "LITIAINA-BM-V2"`
- `format_version`
- `device_uuid`
- `block_size`
- `total_blocks`
- `wal_start_block`, `wal_block_count`
- `ag_count`, `ag_desc_start_block`, `ag_desc_block_count`
- `topology_epoch`
- `checkpoint_seq`

### 3.2 AG Descriptor

Per AG:
- `ag_id`
- `start_block`, `block_count`
- `free_blocks`
- `largest_free_extent_blocks`
- `size_index_root` (free-space by size)
- `addr_index_root` (free-space by start block)
- `deferred_free_queue_head`
- `last_compact_seq`

### 3.3 Address Format (Forward-Compatible)

Keep existing fields and extend with optional BM-V2 fields:

```rust
RawBlobAddress {
  device_id: String,
  allocation_id: u64,
  offset_bytes: u64,
  size_bytes: u64,
  topology_epoch: Option<u64>,
  ag_id: Option<u32>,
  extent_gen: Option<u32>,
}
```

`extent_gen` prevents stale-address reuse ambiguity when an allocation id is recycled in future epochs.

## 4. AG-Style Allocation

### 4.1 AG Selection

Primary AG selection:
- deterministic hash of `(fragment, version_id, part_index, shard_index)` modulo `ag_count`
- plus per-device rotor for tie-break and smoothing

Fallback:
- same device, neighboring AGs by distance
- then global best candidate on the device

No global free-list lock across all AGs.

### 4.2 Concurrency Domain

Each AG has an independent lock/domain.
- allocate/free in different AGs are parallel
- allocator contention is limited to the same AG

## 5. Dual Free-Space Indexes per AG

Each AG keeps the same extents in two structures:

1. Size index:
- key: `extent_len_blocks`
- value: extent refs
- purpose: fast best-fit lookup

2. Address index:
- key: `start_block`
- value: extent records
- purpose: O(log n) neighbor lookup and merge/coalesce

Allocation complexity target:
- lookup: `O(log n)`
- split/update: `O(log n)`

Free complexity target:
- insert + coalesce: `O(log n)`

No linear scan over all free extents.

## 6. Reservation-First Uploads

At upload init:
1. derive full shard geometry for all planned parts
2. reserve extents for all parts up front
3. persist reservation plan in session manifest (`planned_parts`)
4. WAL-commit reservation transaction

At chunk upload:
1. O(1) check: part already committed?
2. open direct I/O from reserved address
3. stream bytes and write shard payload
4. persist part metadata + index part-commit record

Chunk path does not call allocate/free.

## 7. WAL-First Metadata Rules

### 7.1 Record Types

Per-device WAL records:
- `ReserveIntent`
- `ReserveCommit`
- `PartCommitAttach`
- `FreeDefer`
- `FreeApply`
- `RelocateIntent`
- `RelocateCommit`
- `EpochBump`
- `Checkpoint`

Each record includes:
- `txid`
- `seq`
- `crc`
- `recorded_at`

### 7.2 Replay Contract

Replay is idempotent:
- if `txid` already applied, skip
- only committed records mutate allocator state
- interrupted `Intent` without `Commit` is ignored/rolled forward via policy

No in-memory-only allocator truth.

## 8. Topology Epoch + Device Map

### 8.1 Epoch

Any effective blob topology change bumps a global `topology_epoch`:
- disk added/removed/replaced
- device id remap events
- explicit redistribution geometry changes

### 8.2 Device Map Table

Persist mirrored device map metadata in index plane:
- `device_id`
- `canonical_path`/identity fingerprint
- `epoch_first_seen`, `epoch_last_seen`
- state (`online`, `decommissioned`, `missing`)

### 8.3 Stale Address Handling

If address epoch/device mapping is stale:
- do not hard-fail foreground read when reconstructable
- serve via on-the-fly recovery
- enqueue relocation and metadata republish
- summarize logs at object/fragment level, not per-block spam

## 9. Background Maintenance

### 9.1 Deferred Free Queue

Foreground delete/cancel:
- append `FreeDefer`
- enqueue allocation id in AG queue

Background worker:
- batch apply frees
- coalesce with address index
- write `FreeApply`
- bounded budget per cycle

### 9.2 Coalescing/Compaction

Bounded AG sweeps:
- prioritize AGs with highest fragmentation ratio
- stop after configured I/O budget
- never monopolize device bandwidth in availability mode

### 9.3 Relocation Queue

For repair/redistribution/topology events:
- queue relocation jobs keyed by `(fragment, version, part, shard)`
- bounded per-object parallelism
- monotonic state transitions so retries are safe

## 10. Operational Modes and Budgets

Allocator workers obey N1 mode policy:
- `availability`: slow and steady; strict caps on repair/relocate bandwidth
- `strict`: read-only service; aggressive repair/relocate budget
- `scrub`: no CRUD; maximum repair budget

All modes are I/O-budgeted first, CPU/RAM bounded second.

## 11. Invariants

1. Reserved extent is never reused before `FreeApply`.
2. A committed part must map to exactly one live raw extent set.
3. Replay result is deterministic regardless of crash point.
4. Epoch mismatch cannot silently serve wrong extent mapping.
5. Metadata publish controls visibility; allocator state alone does not.

## 12. Integration Map (Current N1 Code)

Primary integration files:
- `block_manager/src/manager.rs`
- `arc/src/n1_core/blob_store.rs`
- `arc/src/n1_core/upload.rs`
- `arc/src/n1_core/recovery.rs`
- `arc/src/n1_core/redistribution.rs`
- `arc/src/n1_core/garbage_collector.rs`

Required transitions:
- replace linear free-list allocation with AG + dual indexes
- keep existing `RawBlobAddress` readable (legacy compatibility)
- keep upload API unchanged

## 13. Rollout Plan

### Phase A: Structural allocator upgrade
- Add AG table and dual indexes.
- Keep current address schema and journal format compatibility shim.
- Feature flag: `bm_v2_allocator=true`.
Status:
- Completed in N1: AG-split allocator and dual free-space indexes are active in `block_manager`.
- Extended with AG max-free index to avoid full AG scans in best-fit allocation.
- AG range lookup now uses a start-block index with binary-search selection (replacing linear AG scans for block-to-AG resolution).
- Added allocator raw-address-only allocation API (`allocate_raw_extent_address`) so reservation flows can avoid creating unused direct-I/O handles.
- Arc planned-part reservation path now uses raw-address-only allocation for lower upload-init control-plane overhead.

### Phase B: WAL tx semantics
- Introduce txid-based WAL records and idempotent replay.
- Add deferred free queue and `FreeApply` worker.
Status:
- Txid-based journal records and idempotent replay are active in N1.
- Two-phase prepare/commit WAL records are active for allocator allocate/free transactions.
- Journal checkpoint compaction is active and bumps superblock generation on compaction commits.
- Journal checkpoint compaction now includes rollback-on-failure semantics:
  - prior journal bytes, superblock copies, and in-memory journal state are restored if checkpoint rewrite/persist fails mid-flight.
- `JournalFull` now primarily indicates a live-set-to-journal-capacity mismatch, not steady churn exhaustion.
- Deferred free markers and bounded `FreeApply` background worker are active in N1 for large raw-free workloads.
- Replay now enforces txid monotonicity across all records (including prepare-only tails) and hard-fails on regressions.
- Arc raw allocation retry loops now fail fast when deferred-free pressure drain cannot reclaim space, preventing unbounded `NoSpace` retry stalls.

### Phase C: Epoch/device map
- Add topology epoch persistence and device map table.
- Implement stale-map relocation-first behavior.
Status:
- Topology epoch and mirrored raw device-map table are active in N1 (`.n1/raw_device_map.msgpack`).
- Stale-map relocation-first behavior remains in recovery/repair paths and now operates with epoch-tagged raw addresses.
- Raw addresses now carry `extent_gen`, and block-manager enforces generation match on raw read/write/free/direct-io validation.
- Raw device status now includes allocator health telemetry (generation, replay txids, journal headroom, and fragmentation per disk).
- Raw device status now includes deferred-free backlog per device (deduped across mirrored index roots).

### Phase D: Default-on hardening
- Make BM-V2 default.
- Keep BM-V1 read compatibility for legacy addresses.
Status:
- In progress (closure gating now standardized).
- BM-V2 allocator path is the active raw-blob allocator for Arc.
- Stable tag remains blocked until the full gate pack below passes in both lab and real hardware.

## 14. Validation Gate (Required Before Stable Tag)

Real hardware matrix:
1. hot-unplug during upload, read, repair, redistribution
2. crash during init/chunk/finalize and replay verification
3. long-run churn (upload/delete/recover/empty-trash) with fragmentation growth
4. strict/scrub/availability mode transitions under load
5. decommission and add-disk epoch bump scenarios

Release gate:
- zero data loss
- deterministic replay
- bounded tail latency in availability mode
- no per-shard warning storms for expected relocation paths

Automation entry point:
- quick gate: `MODE=quick scripts/bench/run_arc_allocator_gate.sh`
- full gate: `MODE=full RELEASE_RUNS=1 CHAOS_RUNS=3 INDEX_SOAK=1 scripts/bench/run_arc_allocator_gate.sh`
