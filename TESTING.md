# N1 Testing And Release Confidence

N1 tests are split into three gates. A green in-process suite is useful, but it is not a production release signal by itself.

N1 release evidence must inspect the namespace/control plane, colocated immutable object metadata, upload receipts, and EC payload directly. The public API, native protocol, on-disk layout, and maintenance paths all need to agree before a build is considered release-worthy.

## Gate 1: In-Process Regression Suite

Run this on every code change:

```bash
RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets
```

This catches deterministic invariants, object metadata rules, namespace semantics, GC safety, repair/redistribution contracts, object-level repair/status, and route-level regressions. It is not enough for production confidence because many tests run in-process and cannot prove process restart, TLS/JWT routing, capacity/placement behavior, or real disk behavior.

### ECS Core integration gate

N1 consumes the separately versioned ECS Core library through a thin adapter.
Before changing that dependency or adapter, require all of the following:

- the ECS Core unit, streaming, hard-stress, and real-disk gates pass;
- `engine/tests/ecs_compatibility.rs` proves exact piece bytes and CRC32 values
  across supported layouts, boundary sizes, tails, and arbitrary input chunks;
- the four-disk N1 release matrix passes restart/resume, download hashing,
  recover-on-read, disk repair, redistribution, and clean-log inspection;
- the installed N1 build retains ownership of placement, file sync, atomic
  rename, directory sync, immutable NDX publication, and namespace commit.

ECS Core is only the byte-to-piece streaming boundary. A green library test
cannot replace the full N1 storage proof.

### N1 Shell Provisioning Gate

The embedded installation terminal has an additional non-destructive gate:

```bash
cargo test -p n1_shell
cargo clippy -p n1_shell --all-targets -- \
  -D warnings -A clippy::too_many_arguments
node --check shell/web/shell.js
```

The Shell unit suite uses simulated block inventories and proves:

- a root/boot disk is classified as protected and cannot reach confirmation;
- formatting requires an exact NDISK label plus the currently observed serial;
- four independently mounted disks can be assigned as index and blob roots;
- `3 + 1` validates only after four distinct physical blob disks exist;
- initialization is repeatable, preserves generated secrets, and leaves the
  service disabled;
- storage and EC configuration changes are rejected while N1 is active;
- setup tokens require exact comparison and are independently generated.

The browser transport proof must additionally show an unauthorized WebSocket
receives `401`, the valid setup token receives `101 Switching Protocols`, and
read-only `version` plus `lsblk` commands complete through the same embedded UI
protocol. This transport proof never invokes `disk format` on the development
machine.

## Gate 2: Release Drills

### Offline backup and snapshot replication

Build `litiaina-n1-backup` beside the same-revision test engine and run
`n1-release-proof --backup-proof` with dedicated test roots on distinct physical
disks. The engine must be stopped for snapshot capture. See
[BACKUP.md](BACKUP.md) for the operational boundary; this is not live replication.

The release drill must prove:

- A running source engine prevents capture; unfinished snapshots are not listed.
- Full and incremental snapshots authenticate correctly, and a second repository
  retains an older recovery point after independent source retention.
- Original shard paths become unavailable before restore, so successful readback
  cannot accidentally fall back to the source files.
- Restored fragments retain credentials, object/version identity, namespace,
  empty folders and unexpired trash. Every fixture payload is hash-checked.
- A captured durable multipart session can finish after restore. Standard
  in-memory sessions are intentionally not restart-resumable.
- An interrupted SSH transfer publishes no partial snapshot; retry and repeated
  transfer succeed, including verification after incomplete-job cleanup.
- Killing restore mid-write leaves startup fences on all roots. Engine startup
  fails, matching `--resume` succeeds, and restored payloads match the offline
  source before normal engine operations resume.
- Wrong keys, damaged blocks, ambiguous target contents, and unsafe path/root
  mappings fail closed. Existing unrelated target files are not deleted.

Record exact binary hashes, test topology, elapsed time, fixture counts, and
production service state before/after the isolated drill. Process termination is
not equivalent to a physical power cut. Large recovery sets, independent-site
failures, and application/database consistency require separate acceptance.

### GC and concurrent publication safety

The `n1-release-proof --gc-safety-proof` mode runs against a separate engine
instance and dedicated test roots. Never pass production roots directly to a
fault-injection proof. Keep the binary, configuration, logs, and fixtures in
one disposable test environment.

This proof requires these observable outcomes:

- An old object parent cannot cause a young version or unknown child to be
  recursively removed. Apply the same check to fragment cleanup and GC.
- Expired, unreferenced, inactive version directories are reclaimed.
- Pause a real one-shot HTTP body after its K+M shard files are opened. Age
  only its isolated test directories beyond the GC TTL, run GC and fragment
  cleanup, then finish the body and verify its download hash before and after
  restart. An active writer is not an expired orphan merely because it is slow.
- Repeat with an existing committed version: pause its replacement upload,
  trash the old path, wait for real TTL expiry, and require the GC status to
  confirm an expired anchor. The replacement's shard directories must survive
  physical trash reclamation too. Finish and hash-check it across restart.
  Deterministic tests separately protect idle standard and durable sessions
  until cancellation/session removal allows reclamation.
- Standard and durable multipart writers overlap with read/list/rename/trash/
  recover work and GC. Each published payload is hash-checked; standard
  finalize has a deadline so a hang is a failure.
- A ZIP export runs alongside the mixed workload and GC, and its contents are
  independently checked. At least one GC pass starts before export retention
  can defer cleanup.
- Deliberately corrupting session metadata must preserve potential live data
  and produce an explicit GC error. Removing the injected corruption must
  allow a clean retry. Expected fault logs are matched explicitly; other
  errors still fail the proof, and unreadable logs are an error.
- Restart and final namespace verification must pass.

Run `--auto-healing-proof` repeatedly on the same candidate as well. It must
allow fragment creation while the scheduler is active. The unit regression
`replica_repair_preserves_inflight_atomic_writes_and_local_workspaces` also
checks that repair neither deletes nor clones atomic-write temporary files,
that the original writer can finish its rename, and that root-local work
directories remain owned by their original root.

These are bounded workload proofs, not a guarantee of constant-memory GC at
millions of objects or an exhaustive proof of all concurrent mutations.

Additional deterministic regressions cover cancellation and publication:

- `cancelled_standard_finalize_releases_busy_state_for_retry` polls the actual
  finalizer while its publication lock is held, drops the future, and repeats.
  Cancellation must release the busy state without touching completed parts.
- `durable_finalize_retains_resume_truth_until_namespace_commit` verifies the
  manifest and logical binding after NDX publication and on retry; only a
  successful namespace commit permits removing the session.
- `durable_receipts_refresh_idle_age_without_rewriting_manifest` verifies that
  receipt-directory activity refreshes durable session idle age while manifest
  bytes remain unchanged. Session age must not remain stuck at initialization.
- `cancelled_request_keeps_namespace_publication_exclusive_until_worker_finishes`
  cancels the actual logical finalizer after its blocking publisher is queued.
  The same-path gate must stay held until the worker finishes, and its registry
  entry must then disappear. Companion cases cover 16 cancelled waiters,
  unrelated-path concurrency and worker unwind.
- `metadata_disappearing_after_ttl_unlinks_anchor_is_not_a_broken_record`
  injects a metadata read after TTL unlinks a trash anchor. Only that confirmed
  disappearance is benign. A retained anchor with missing metadata and a hard
  I/O failure still return errors. The concurrent recover/TTL regression checks
  the winning operation and resulting live visibility, not just HTTP status.
- `core::upload::activity::tests` covers uploads beginning before/after a GC
  snapshot, completed uploads against stale snapshots, clock rollback with
  zero TTL, per-object exclusion, overlapping scans, concurrent writers,
  canonical object IDs, poisoned-state preservation and cancelled callers
  whose blocking publishers are still running.

Upload activity protection is process-local, not a new durable receipt. Its
registry contains active operations, not the entire stored namespace. Each
scan remembers at most 65,536 overlapping object IDs (each validated to at
most 128 bytes); reaching that limit defers that scan's remaining orphan
reclamation and logs the deferral, without refusing uploads. Registry locks
cover bookkeeping only, never filesystem work or network streaming. This
bound does not make the existing namespace reachability scan constant-memory.

Temporary private filesystems can run these functional tests on one machine.
They do not establish independent-disk resilience, fsync power-loss durability,
or physical-disk performance. Do not substitute them for hardware acceptance.

Trash enumeration must also distinguish an in-flight empty parent chain from
an ambiguous committed generation. Run
`trash_scan_tolerates_inflight_parent_chains_but_rejects_ambiguous_generations`:
partially created/pruned parents must not break another user's recover, but
multiple competing path components must still fail closed. A committed empty
folder is a valid trash item and must remain listed.

Run the release drill harness before tagging or deploying:

```bash
scripts/bench/run_release_drills.sh
```

The drill runs high-value workflow cases repeatedly and can include the HA chaos harness. For this workspace, direct every output and Cargo target to `/home/litiaina/DEV_TEST`.

For a real large-file multipart restart proof as part of the release drill:

```bash
RUN_REAL_RESTART_PROOF=1 \
SOURCE_FILE=/home/administrator/Downloads/INCDT\ 122225.mp4 \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
scripts/bench/run_release_drills.sh
```

## Gate 3: Real HTTP Proof

This focused gate covers process restart, real disks, multipart receipt
persistence, streamed download hashing, range reads, health, and committed
namespace/object state. It is not the complete LTS matrix by itself; repair,
redistribution, GC, UI, and long chaos still need their Rust-owned gates.

```bash
SOURCE_FILE=/home/administrator/Downloads/INCDT\ 122225.mp4 \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
scripts/bench/run_n1_release_proof.sh
```

The proof starts a real `litiaina-n1` server, creates a real fragment, rejects
one-shot for files under the multipart threshold, uploads a real video through
multipart, restarts the process midway, resumes from persisted receipts,
finalizes, lists through public APIs, streams the download hash, checks range
reads, health-checks, and directly inspects committed namespace, `.ndx`, and
EC payload state on both roots.

Expected pass evidence includes:

- before-restart list count equals uploaded file count
- after-restart list count equals uploaded file count
- before/after path set hashes match
- sample downloads match source SHA-256
- after simulated disk format, the surviving root still lists all expected objects
- strict repair restores the missing index and blob planes
- after `1 disk -> 2 disk` re-expansion, the one-disk write is still listed
- every index root reports the same live namespace links
- index-only roots contain no `.objects`
- blob-only roots contain no namespace/control state
- every shard-bearing blob placement has both immutable `.ndx` and payload
- every `.ndx` placement has payload and every payload placement has `.ndx`
- server RSS remains bounded instead of growing with object count

Useful knobs:

```bash
PORT=50491
CONCURRENCY=16
MAX_FILES=0
CLEANUP_ROOTS=0
OUT_DIR=/tmp/n1-real-proof
```

Use `MAX_FILES=128` for a smoke run. Use `MAX_FILES=0` for the full source tree.

## Gate 4: Parallel Small-Object Storm

This hardening gate must be owned by a Rust runner. Python is not part of the
N1 release verification path.

Expected pass evidence includes:

- total uploaded objects equals 12,000
- every fragment lists exactly 3,000 objects after upload
- restart listing still returns exactly 3,000 objects per fragment
- N1 does not hit the process open-file ceiling
- RSS remains bounded under the storm instead of growing with object count
- logs contain no write-quorum, replica, namespace, object-index, or blob shard open failures

## Release Rule

For production confidence, require:

```bash
RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets
scripts/bench/run_release_drills.sh
SOURCE_FILE=<large-video> DISK_ROOTS=<disk-a>,<disk-b> scripts/bench/run_n1_release_proof.sh
```

Do not treat synthetic tests alone as release confidence. N1 must prove the public API, process restart, real disk capacity/placement behavior, real disk layout, object-level repair/status, and durable namespace/colocated-object state together.

## N1 3.4.0 Development Release Requirement

N1 3.4.0 must retain the engine-format-3 storage invariants and additionally prove:

- standard multipart init performs no persistent session or namespace write
- distinct standard multipart parts execute concurrently and publish nothing before finalize
- duplicate part submission is idempotent after completion and conflict-safe while in flight
- finalize synchronizes the complete K+M shard set before publishing NDX and namespace state
- unfinished standard sessions disappear on restart and their headless shards remain TTL-reclaimable
- `/upload/multipart/durable` retains restart-resumable manifest and part-receipt behavior
- local-server and LAN benchmarks record direct and multipart throughput independently

- delete is soft-only across HTTP, POSIX, native protocol, shell, and console
- each deletion receives a unique trash entry id
- repeated deletion of the same logical path remains independently recoverable
- recovery succeeds before TTL and fails closed after TTL
- manual GC cannot shorten trash retention
- normal lifecycle scans `.trash` rather than the complete blob plane
- expiry derives its deadline from the timestamp generation and removes no live namespace path
- exact object ids are reclaimed directly only after live/trash reference verification
- folder delete remains a directory rename and descendant inspection is deferred until expiry
- strict recovery returns `409 Conflict` without consuming trash when the live destination exists
- legacy 3.2 direct-path trash is migrated offline before a 3.3 binary starts; live namespace and blob data do not move

Evidence for the current release belongs under `/home/litiaina/DEV_TEST` and
must record the exact binary hash, configuration, disk topology, commands,
results, and any unproven boundaries.

## N1 3.5.0 Upload Release Requirement

N1 3.5.0 must retain every engine-format-3 and 3.4.0 upload invariant and also
prove:

- one-shot, standard multipart, and durable multipart execute without any synchronous physical-capacity probe
- a real kernel `ENOSPC`/quota failure returns `507` and publishes no NDX or namespace entry
- cleanup removes partial shards after a capacity failure, with orphan GC as the crash fallback
- standard batch upload accepts many independent one-MiB-or-smaller objects through one bounded multipart request
- a batch entry receives its own object identity, K+M layout, NDX, namespace entry, and response
- one failed or conflicting batch entry does not corrupt or cancel successful independent entries
- batch concurrency remains bounded in memory and can be selected independently of total configured disk count
- no durable batch route exists; durable multipart retains replicated restart-proof manifests and receipts
- small-object evidence records objects/second, MiB/second, CPU cycles, latency percentiles, RSS, and disk I/O
- scale analysis demonstrates no O(total configured disks) capacity work in any upload hot path

## N1 3.5.1 Bulk Export Release Requirement

N1 3.5.1 must preserve every engine-format-3 and 3.5.0 invariant and prove:

- manifest capture traverses the selected live namespace once and pins logical path, object identity, exact version ULID, and byte size
- missing or corrupt selected metadata fails the entire job instead of omitting an archive entry
- no partial archive is downloadable before the durable `ready` boundary
- process termination during manifest capture safely repeats capture after restart
- process termination during artifact build reuses the completed immutable manifest, removes partial chunks, and rebuilds the same version set
- ZIP64 and TAR artifacts reproduce the selected entry set and every entry's source SHA-256
- mapped business-layer entries preserve their archive paths and export only the requested immutable source byte ranges, including multiple logical files sharing one packed source object
- ready downloads return exact `Content-Length`, stable SHA-256 `ETag`, `Accept-Ranges: bytes`, and correct `206`/`416` behavior
- an interrupted LAN download resumes through a single byte range and the final artifact hash still equals its ETag
- artifact chunks use bounded concurrent writes and distribute across eligible blob roots without becoming authoritative object data
- cancellation leaves no published artifact, and export retention removes job state, manifest, and chunks without touching source objects
- queue, job, object-read, chunk-size, write, and optional retained-byte limits remain bounded
- server-local build/delivery and LAN-resume measurements are recorded separately
- release evidence contains topology labels and measurements but no host filesystem paths or credentials

The focused Rust proof is invoked through `n1-release-proof --export-proof`.
Use distinct physical roots for EC evidence; same-device directories are only a
functional test. A complete proof deliberately kills the optimized N1 process
while phase is `building`, restarts it, validates the recovered archive, creates
a second uninterrupted performance export, tests cancel and TTL cleanup, and
scans all engine logs with warnings/errors treated as failure.

The reusable release runner has one bounded API gate and three focused
autonomous-maintenance gates:

- `--noa-api-proof` starts an optimized real N1 process and proves canonical
  NOA ping/auth, one-shot upload, list/stat/download/range integrity, object
  status, realtime WebSocket events, POSIX file/folder rename, trash/recover,
  TTL reclamation, maintenance status, deep health, restart persistence, clean
  logs, and absence of the former `/n1/...` namespace.

- `--auto-healing-proof` injects data/parity loss, root loss, root-identity
  mismatch, unrecoverable damage and later recovery, scheduled online checksum
  with a successful concurrent mutation, and a prospective EC change.
- `--maintenance-coverage-proof` proves Deep Scan/Fresh Snapshot observation,
  multipart restart, online availability repair, manual checksum/strict
  read-only gates, scrub quiescence, and clean gate release.
- `--trash-maintenance-proof` repairs trashed objects across restart and across
  the 128-entry metadata batch boundary before recovering and hashing all 141
  objects.

Pass explicit `--data-shards`, `--parity-shards`, `--disk-roots`,
`--colocated-roots`, and `--cleanup-roots`. Every `D + P` proof root must be on
a distinct physical device; same-device directories are functional tests, not
EC hardware evidence.

Recover-on-read fault injection must remove a data shard (`.shard`) that the
read path actually consumes. Removing parity (`.parity`) exercises explicit
object or fragment repair instead; a healthy read is not required to scan for
or recreate unused parity. The proof runner keeps these two fault classes
separate so a parity-repair expectation cannot masquerade as recover-on-read
evidence.

## Historical N1 2.0.0 Evidence

N1 2.0.0 release evidence must include both supported disk-role modes:

- colocated roots, where each physical disk carries index and blob roles
- split roots, where blob-only roots are proven free of namespace/control state

The proof records live namespace-link counts separately from immutable `.ndx`
and payload-placement counts. Object-plane GC compatibility links are not
namespace entries.

Focused N1 2.0.0 evidence from June 11, 2026:

- Colocated two-disk proof passed with `212` live objects and `212` matching
  namespace links on each index replica.
- Split-root two-disk proof passed with `94` live objects and `94` matching
  namespace links on each index replica.
- Both proofs used a real `256 MiB` multipart object, restarted midway, resumed
  from receipts, finalized, and verified the downloaded SHA-256.
- Both proofs passed native protocol operations, concurrent users, POSIX CRUD,
  trash/recover/GC, recover-on-read, object repair, simulated disk-format loss,
  strict fragment repair, and `2 disks -> 1 -> 2` redistribution.
- Split blob roots contained zero aliases, `.fragment.ndx`, uploads, trash, or
  user namespace entries.
- Split index roots contained zero `.objects` directories.
- Every inspected blob placement had matching immutable `.ndx` and payload.
- Object-level redistribution passed a real `1 + 0 -> 1 + 1` topology change:
  one object scanned, one mismatch found, one migration completed, zero errors,
  followed by physical `.ndx + payload` verification on both blob roots.
- N1 3.2.0 redistribution tests must prove that a higher durable namespace
  generation wins after `2 -> 1 -> 2`, including equal-count replacement and
  newer deletion cases, while same-generation content disagreement and writes
  attempted before reconciliation still fail closed.
- Server RSS peaked at about `155 MiB` in the larger colocated matrix and about
  `123 MiB` in the split-root matrix.
- Log scans reported no warning, error, panic, critical, degraded, or failed
  lines after the expected failure/recovery operations completed.
- Release-mode upload benchmarks passed for HTTP and native one-shot,
  sequential multipart, and pipelined multipart. On this HDD pair, explicit
  multipart concurrency `2` produced `41.47 MiB/s` HTTP pipelined and
  `40.82 MiB/s` native sequential multipart averages while keeping peak RSS
  below `87 MiB`.
