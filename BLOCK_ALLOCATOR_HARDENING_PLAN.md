# N1 Block Allocator + Formatter Hardening Plan

> Scope note:
> - Hardening plan for arc allocator/formatter track.
> - Arc is experimental.
> - Stable/default deployment remains `standard` (`DEPLOYMENT_PROFILES.md`).

This plan defines how we make the allocator and formatter production-stable for arc kernel mode.

Reality check:
- No storage engine is literally indestructible.
- The practical target is fail-stop, crash-recoverable, deterministic behavior with zero silent corruption.

## 1. Hard Requirements

1. Never reuse an extent unless free is durably committed.
2. Never acknowledge metadata durability before durable write ordering is satisfied.
3. Replay must be deterministic and idempotent at every crash point.
4. Formatter must not claim unknown media without explicit destructive intent.
5. Corruption must be detected early and surfaced as hard errors (not silent continuation).

## 2. Formatter Hardening (Disk Claiming)

## 2.1 Preflight Safety Gate

- Strict device identity snapshot before format:
  - canonical path
  - size bytes
  - stable hardware id (when available)
  - current detected signature
- Reject mounted targets.
- Reject foreign signature unless `--force` is explicit.
- Reject dirty unknown media unless `--force` is explicit.

## 2.2 Destructive Intent Handshake

- Add `format_nonce` challenge/response in CLI/API:
  - first call returns nonce + device fingerprint
  - second call must return exact nonce + fingerprint to execute format
- Prevent accidental one-shot destructive calls.

## 2.3 Atomic Format Protocol

- Write superblock B (new generation, CRC) first.
- Flush metadata.
- Write superblock A.
- Flush metadata and data.
- On open: choose newest valid superblock by generation + CRC.

## 2.4 Post-Format Verification

- Immediate reopen and self-check:
  - header CRC
  - journal region sanity
  - AG table sanity
- Fail format if verification fails.

## 3. Allocator Hardening (Runtime)

## 3.1 Journal Semantics

- WAL records include:
  - txid
  - sequence
  - CRC32/XXH64
  - op type
  - payload length
- Two-phase transaction style:
  - `Prepare`
  - `Commit`
- Replay ignores prepared-only txns without commit.

## 3.2 Checkpoint Semantics

- Copy-on-write checkpoint pages with generation numbers.
- Checkpoint root pointer switch is atomic and checksummed.
- Replay:
  - load latest valid checkpoint
  - apply committed WAL suffix only
  - verify free-space/accounting invariants

## 3.3 Allocation Safety

- Extent generation counter (`extent_gen`) increments on each reuse.
- Address tuple includes generation and epoch.
- Stale address with old generation is rejected.

## 3.4 Free Path Safety

- Deferred free queue remains idempotent:
  - duplicate free of already-freed allocation is success (idempotent)
  - unknown allocation does not panic allocator state
- Bounded background free apply worker with strict per-cycle budget.

## 3.5 Corruption Containment

- On checksum mismatch/corrupt record:
  - hard error and stop writes on affected device
  - mark device degraded
  - continue service only through recoverable redundancy path
- No best-effort writes after allocator metadata corruption is detected.

## 4. Concurrency + Locking Stability

- Keep AG-scoped lock domains only.
- No global allocator lock in hot allocate/free path.
- Add lock hold-time telemetry:
  - p50/p95/p99 lock hold duration
  - queue wait duration
- Alert if tail lock hold exceeds budget.

## 5. Observability and Operational Controls

- Add allocator health endpoint fields:
  - valid_superblock_generation
  - replay_last_txid
  - journal_tail/head distance
  - deferred_free_backlog
  - largest_free_extent per AG
  - fragmentation ratio per AG
- Add formatter audit log:
  - who/when/which device/fingerprint/result
Status:
- Block manager now exposes allocator health snapshots containing:
  - superblock generation
  - replay/next txid
  - journal used/capacity/free records
  - largest free extent and fragmentation ppm (global + per-AG)
- Arc raw blob device status now surfaces these allocator health fields per managed raw disk.
- Deferred raw free backlog is now exposed per managed device in raw blob status.

## 6. Validation Matrix (Must Pass)

## 6.1 Deterministic Crash Tests

- Crash injection at each critical boundary:
  - after WAL prepare
  - after WAL commit
  - after allocation split
  - after free enqueue
  - during checkpoint switch
- Verify identical replay result across repeated runs.

## 6.2 Power-Fail Simulation

- Fault-inject fsync/flush failures.
- Simulate torn writes at block boundaries.
- Verify superblock/journal recovery chooses safe state.

## 6.3 Long-Run Churn

- Millions of ops:
  - upload/delete/recover/redistribute loops
  - mixed small/large objects
  - strict/scrub/availability mode transitions
- Success criteria:
  - no metadata drift
  - no allocator leaks
  - bounded fragmentation trend

## 6.4 Real Hardware Chaos

- Hot unplug/replug during:
  - upload
  - read
  - repair
  - redistribution
  - GC
- Must degrade predictably and recover with deterministic replay.

## 7. Release Gates for “Stable”

Mark allocator/formatter stable only when all are true:

1. Zero silent corruption in chaos + power-fail matrix.
2. Replay determinism proven with txid equality and invariant checks.
3. Formatter safety handshake enforced in CLI/API.
4. Tail latency budgets held in availability mode under maintenance load.
5. Documentation and operator runbooks updated to final behavior.

## 7.1 Certification Command Pack

Use the Arc allocator certification harness to produce reproducible gate artifacts:

- Quick gate (CI/lab smoke + core allocator correctness):
  - `MODE=quick scripts/bench/run_arc_allocator_gate.sh`
- Full gate (release-candidate pass including chaos):
  - `MODE=full RELEASE_RUNS=1 CHAOS_RUNS=3 INDEX_SOAK=1 scripts/bench/run_arc_allocator_gate.sh`

Artifacts:
- CSV summary: `.n1-bench/arc-allocator-gate-*/summary.csv`
- Human summary: `.n1-bench/arc-allocator-gate-*/summary.txt`
- Per-step logs: `.n1-bench/arc-allocator-gate-*/*.log`

Stable closure rule:
- Do not mark Arc allocator stable unless the full gate command above passes and the real-hardware matrix in section 6.4 is attached to the release evidence.

## 8. Execution Phases

## Phase H1: Formatter safety protocol

- nonce handshake
- dual-superblock commit protocol
- post-format self-verification
Status:
- Formatter preflight inspection is now active via block-manager API:
  - canonical target path
  - target size bytes
  - block-device detection
  - mounted-state detection (best-effort via mountinfo source matching)
  - signature classification (`Empty`, `BlockManager`, `Foreign`)
- Formatter now rejects foreign-signature targets by default.
- Explicit destructive override path is now available via `format_with_force`.
- Nonce challenge/response handshake is now active via:
  - `issue_format_nonce` (challenge + fingerprint snapshot + expiry)
  - `format_with_nonce` (single-use response validation + drift/expiry rejection)
- Dual-superblock write+sync ordering and open-time newest-valid selection are active.

## Phase H2: Journal and checkpoint hardening

- checksummed WAL records
- prepare/commit transaction shape
- COW checkpoint generation switch
Status:
- WAL prepare/commit transaction records are now active for allocator allocate/free operations.
- Replay now applies only committed prepare/commit pairs and ignores prepare-only tails.
- Journal checkpoint compaction now rewrites a compact committed snapshot and bumps superblock generation.
- Open now selects the newest valid superblock copy by generation + checksum.
- `JournalFull` is now limited to cases where the live allocation set itself cannot fit within configured journal capacity.

## Phase H3: Address-generation safety

- add `extent_gen` handling in allocator + metadata addresses
- reject stale-generation addresses on read/write paths
Status:
- `extent_gen` is now emitted by block-manager raw addresses and persisted through arc raw metadata.
- Read/write/direct/free raw address paths now validate generation and reject mismatches as corruption.
- Backward compatibility is retained for existing metadata via default generation value `1`.

## Phase H4: Chaos and certification

- full crash/powerfail/hotplug matrix
- freeze only after gate pass
Status:
- Added deterministic crash-window replay tests in `block_manager` for:
  - committed allocate txns persisted without runtime apply
  - committed free txns persisted without runtime apply
- Added checkpoint-switch crash-window coverage:
  - stale primary + newer valid backup generation after checkpoint compaction still reopens on newest valid copy.
- Added deterministic long-run crash-loop coverage:
  - periodic reopen boundaries during randomized allocate/free churn preserve exact allocation map and allocator invariants.
- Added torn-tail journal replay coverage:
  - bytes beyond first zero tail marker are ignored safely.
  - partial tail records with non-zero magic hard-fail on checksum mismatch (fail-stop).
- Added duplicate-prepare replay hardening:
  - duplicate prepare records for same txid/payload are idempotent.
  - duplicate prepare records with mismatched payload hard-fail as corruption.
- Added duplicate-commit and txid-order replay hardening:
  - duplicate committed records are idempotent only when payload matches exactly.
  - txid regressions hard-fail as corruption instead of being silently skipped.
  - txid monotonicity is now enforced across all journal records (including prepare-only tails).
- Added legacy replay compatibility coverage:
  - committed legacy journal records with `txn_id=0` replay deterministically via cursor-derived txid normalization.
- Added test-only I/O failpoint injection at critical metadata boundaries:
  - format superblock write/sync steps
  - journal record write
  - metadata flush sync
- Added failpoint crash-boundary tests to verify fail-stop behavior and replay stability:
  - injected format sync failure returns hard error
  - injected format backup-sync and primary-write failures return hard errors
  - injected format zero-journal failure returns hard error
  - injected allocate/free commit-write failures leave no partial state change and replay ignores prepare-only tails
  - injected flush-meta sync failure is fail-stop and recoverable on retry
  - injected checkpoint journal-zero + persist backup-sync/backup-write/primary-sync failures are fail-stop and reopen with allocator state intact
  - injected mid-checkpoint journal rewrite failure now triggers explicit rollback of pre-checkpoint journal bytes + superblock copies + in-memory state (verified before and after reopen)
- Added deterministic mixed-failpoint replay certification test:
  - fixed-seed workload with periodic reopen/crash boundaries and rotating failpoints
  - replayed allocator fingerprint is stable across repeated runs with identical schedule
- Added raw-address allocation fast path in block manager (`allocate_raw_extent_address`) so reservation flows can allocate without creating unused direct-I/O handles.
- Arc planned-part reservation path now uses raw-address-only allocation for lower allocator/control-plane overhead during upload init.
- Raw allocation retry loops in arc now fail fast on `NoSpace` when deferred-free pressure drain yields zero reclaimed extents (prevents unbounded retry stalls).
- Failpoint state is now thread-local in tests to avoid cross-test contamination under parallel execution.
- Full `cargo test -p block_manager` remains green with these replay-boundary checks (`80` tests).
