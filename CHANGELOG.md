# Changelog

This is a condensed release and development history, not an LTS declaration.
Results and known failures are recorded separately in [TESTING.md](TESTING.md).

## Unreleased — bounded maintenance and upload performance

- Reduce full namespace-inventory I/O: reuse the validated NCX identity, avoid
  repeated authoritative lookups for duplicate replicas, and select the newest
  immutable NDX in one directory pass per member. Retain replica-conflict,
  corruption and maximum-ULID checks; change no storage encoding.
- Add opt-in, actual-traversal progress to namespace snapshots. Require explicit
  successful scan exhaustion before emitting completion; a failed producer is
  not an empty success. Preserve the default entry/completion stream.
- Continue failed post-upload qualifications in place with pinned source/build
  evidence, bounded probe discovery and independent results. Preserve original
  failures; do not clone member storage or repeat the million-object upload.
- Share only pending, sealed filesystem flush rounds between concurrent small
  batches. Late writers require a new flush; writeback errors fail every live
  attached group without retry. Keep all three ordered durability barriers,
  pre/post root validation and ordinary immutable storage files. Add bounded
  per-device flush timing; this does not eliminate filesystem-wide coupling.
- Pace GC at entry-count or elapsed-time boundaries. Give active and queued
  foreground work finite service windows, without disabling collection or
  weakening reference, upload, export or TTL protection.
- Separate CPU execution from I/O wait and sample counters coherently. Do not
  interpret I/O-blocked load average alone as CPU saturation; retain configured
  concurrency ceilings and actual CPU, memory and stall safeguards.
- Retain read-only GC/flush diagnostics on a refused scale-test batch, and
  support exact enrolled EC topology when independently cloning failed test
  populations for verification and matched comparisons.
- Isolate the namespace key-lock cleanup regression from unrelated parallel
  tests; preserve the failed qualification and require a fresh complete rerun.
  This changes no runtime behavior, storage format or RC version.
- Add a request-owned strict small-file batch path: at most 64 objects / 8 MiB
  per commit group, bounded payload parallelism and shared filesystem durability
  barriers before NDX, before NCX and before ACK. Keep ordinary independent
  NCX/immutable NDX/shards, existing per-key coordination and format. Do not
  rewrite healthy NCX for a new version. GC reclaims only expired private batch
  staging files in its existing version pass, fenced against active uploads.
  Correctness and measured performance qualification are separate gates.
- Add a bounded, in-process Linux io_uring backend with owned completion
  lifetimes and explicit `auto`, required `uring` and `blocking` selection.
  Expose the selected backend, initialization fallback and operation counters
  in startup logs/system health. Preserve publication order and durability
  barriers across direct/relaxed writes, checksum/recovery reads and metadata
  publication. Keep asynchronous data/read/sync work native; synchronous
  publication uses POSIX directly rather than a worker-to-ring reply bridge,
  reported as `publication_backend: posix`. Share encoded NDX between holders
  and reuse checksum buffers;
  storage formats are unchanged. Native XFS/ext4 correctness gates pass;
  performance qualification is separate.
- Visit metadata-snapshot holders in the same lexical order used for replica
  suppression, avoiding a complete non-emitting member walk before the first
  entry. Keep all-member traversal and EC member order unchanged. Report startup
  phase timings without weakening lease or publication syncs.
- Verify existing shard checksums on streaming GET/download/range reads as well
  as verified chunk reads. Reject unavailable checksum authority; validate
  reconstructed bytes before repair publication. Keep storage encoding intact.
- Make full healing/checksum walks and deep health scans explicit. Retain
  known-member safety checks and bounded, targeted read-triggered repair.
- Check trash TTL before loading version metadata. Route scheduled and manual
  GC through the same collector so scheduled runs also reclaim aged abandoned
  uploads, including unpublished versions beneath live objects. Preserve
  committed history, active uploads, overlapping namespace mutations and backups.
- Stream object fanout with bounded directory handles and rejoin BackgroundGc
  admission between object/session batches. Check every configured member before
  deletion; reject redirected layouts and uncertain export authority. Recheck
  export admission between destructive batches. No new upload receipts or format.
- Prune only known empty trash ancestors after GC expiry, preserving siblings
  and `.trash` itself. Keep ordinary recovery free of added pruning syncs.
- Reconstruct short/unaligned reads from recorded EC stripes, and repair every
  encountered corrupt data target rather than confusing peer rejection with
  an already-admitted repair. Keep reconstruction memory stripe-bounded.
- Honor existing whole-fragment deletion admission in read-triggered repair;
  defer repair during deletion and refuse publication without live identity.
- Treat unavailable process metrics as telemetry, not storage I/O failure, and
  stop the sampler before stopping a test engine. Preserve and reconcile timed-out
  writes without retrying them or relabeling an incomplete run as a scale PASS.

- Preserve and independently recheck failed HDD scale fixtures, including every
  acknowledged historical version and complete namespace/maximum-ULID inventory.
- Record bounded, in-memory admission-owner diagnostics on foreground timeout
  and separate batch payload/NDX and NCX latency classes. Durability, admission
  deadlines and storage encoding are unchanged.
- Preserve FIFO admission when adaptive concurrency shrinks: retire existing
  semaphore permits as operations finish instead of cancelling queue position
  and retrying every 2 ms. Keep waiting foreground work out of the shared
  background lane. Hard concurrency and memory limits remain unchanged.
- Allow adaptive concurrency to recover after host pressure clears even with
  queued demand. Retain configured ceilings and saturation/stall/resource guards;
  validate against unchanged strict-durability workloads before claiming gains.
- Resume one physical NCX walk across internal maintenance batches instead of
  restarting sorted LIST for every page. Keep live/trash scan state bounded;
  reuse the cursor for repair, monitoring and redistribution.
- Remove duplicate NCX repair passes and move namespace inspection off async
  workers. Namespace/payload/NDX presence probes stop at their first match;
  object fanout counting streams without a whole-fragment directory inventory.
- Keep bounded independent-object concurrency and report admitted successes
  alongside explicit per-object backpressure rejections.
- Publish independent NDX holders in bounded parallel phases, joining every
  writer before proceeding. Avoid redundant existing-parent traversal while
  retaining the existing strict direct-I/O and data/directory barriers.
- Keep one-shot and batch-upload admission through NCX publication, including detached
  commits after caller cancellation. Resolve stable fragment IDs without
  rediscovering roots and create only missing directory suffixes.
- Reuse the typed streaming commit result for HTTP one-shot uploads instead of
  serializing/parsing an internal response and rereading the just-written NDX.
  Share stream/reader shard setup, open independent members in bounded parallel,
  and omit redundant small-shard pre-sizing. Keep strict direct I/O and all
  synchronization barriers. NDX publication no longer initializes unused
  `.uploads` and `.trash` directories.
- Collect GC references from each physical NCX in cooperative batches. Preserve
  conflicting references and defer reclamation on unreadable authority; reuse
  one object-directory inventory and lazy cross-member NDX authority per object.
  Give queued foreground work a bounded service window between GC batches.
- Separate shard setup, direct-write drain, data sync and directory-sync timing.
  These changes require fresh HDD integrity and matched-population performance
  verification; the failed 300k run is not relabeled as a pass.
- Retain server-stage/scheduler telemetry at scale checkpoints and separate
  client source preparation, HTTP and acknowledgement-ledger timing. Keep the
  real source files, FULL acknowledgement database and strict workload unchanged.
- No disk-format, NCX copy/delete, immutable version, backup or cache changes.
  Performance and integrity qualification are recorded separately below.

## 1.0.0-RC.1 — October 18, 2026

- Declare the first formal release candidate. Reset private development product
  numbering to `1.0.0-RC.1` across the workspace; this is not stable or LTS.
- Formalize the storage-format specification as `1.0.0-RC.1`, retaining encoding
  identifier 4 and existing data/backups without a rewrite or format migration.
  NOA v1, native `n1/0.5` and repository `n1-backup-v1` remain unchanged.
- Expose product/storage/protocol versions through the engine's `--version`;
  align Shell, backup-helper and proof-tool release reporting. Document candidate,
  stable and future compatibility/migration rules.

- Honor configured I/O admission for small uploads. Resolve one-shot object
  identity after admission on HTTP/native paths, preserving progress tracking
  and explicit overrides. NCX publication does not consume a payload permit.
- Fix synchronous backup-boundary admission when Tokio's cooperative task
  budget is exhausted; preserve lock exclusion and capture admission timeouts.
- Reuse healthy, unchanged NCX inodes when publishing another version. Retain
  directory syncing and repair missing/corrupt copies; no version head or cache.
- Add isolated overload/crash/integrity tools and regressions. Make the
  beyond-parity healing fixture deterministic by injecting the complete cold
  fault before restart, then verifying automatic recovery while running.

### Private 4.0.0 development baseline (encoding 4)

The features below form RC.1's baseline. Their earlier private development
version labels are historical, not additional stable releases.

- Fix HTTP auth/view/upload/global limiter configuration: `*_limit_per_second`
  now means requests per second, not seconds per replenished request. Preserve
  burst sizes and key scopes; reject zero rates/bursts and unrepresentable rates.
  Add exhaustion/refill and HTTP middleware regressions.

- Add committed-state online backup through the running engine's authenticated
  HTTP/native endpoint. Pin the namespace and immutable version files at a
  bounded capture boundary, then copy with N1 serving traffic. Unfinished upload
  progress remains offline-only. Shell scheduling and Console expose both modes;
  ordinary NCX copy/delete and max-ULID version selection remain unchanged.

- Replace SSH-dependent remote snapshot replication with N1-owned HTTP(S) and
  native N1(S) transport. Shell's optional operator endpoint executes existing
  backup/restore/replication jobs for Console Maintenance, with dedicated
  authentication, configured aliases, bounded streaming and explicit confirmations.
  Backup repository format and ordinary NCX/NDX/payload semantics are unchanged.
  Old SSH job fields/flags are rejected; configure the owned destination endpoint.

- Multipart initiation accepts `fresh=true` for independent editor saves while
  retaining default pending-session discovery. Retries use their original
  session/version IDs; no storage format or namespace mutation changes.

- Optional private Accel read proxy accelerates existing Console/API images,
  video ranges and downloads after authoritative authorization/version lookup.
  Cache-origin reads prevent recursive proxying without bypassing authorization;
  pre-header failures fall back to N1. No NCX, NDX or payload format changes.
- Enabled TCP_NODELAY on HTTP accepted sockets to reduce header/early-body
  packet-coalescing delay; native sockets already used it.

- Propagated strict shard-directory sync errors; standard multipart now honors
  durability options and preserves effective part durability through NDX/NCX
  finalization. Relaxed-to-strict completed-part retries return conflict rather
  than falsely upgrading an acknowledgement. Existing default remains relaxed.
- Enforced independent delete/recover permissions on HTTP/native object and
  POSIX operations. Master-authenticated operator capability gates global,
  cross-fragment and restrictive maintenance; global stops are operator-only.
- Bound refresh sessions to verified credentials; fragment-key rotation rejects
  old refresh authority. Fresh login is required for legacy refresh sessions.
  Removed generation/backfill of weak NT hashes; Argon2 authentication remains.
- Bounded public JSON, protected JSON, native admission/control frames and
  multipart temporary allocations. Raw native uploads require prior write
  authority and stream without unauthenticated whole-body allocation.
- Required read authority and token-expiry closure for WebSockets; protected
  object responses are private/no-store. Shell TLS verification is now default.
- Added available-inode and low/exhausted headroom warnings to existing pool
  health, Shell and Console. No space reservation or new object cache.
- Added negative backup/replication verification coverage for wrong keys,
  corrupt source/replica blocks and preservation of the last valid recovery point.

- Added read-only pool-scoped health over HTTPS/native transport and Shell
  `pools`: member identity/location, independent readiness/capacity and the
  authenticated fragment's coverage. Explicit content checks reuse the existing
  verifier, distinguish live scan results from readiness, and never repair data.
  Offline `pool verify` can select a pool/stable fragment without scanning other
  pools. Console exposes these checks and offline maintenance/backup workflows.
- Corrected pool-local dangling-reference verification: an NCX binding can stay
  in its original pool while newer versions live in an expansion pool. Online,
  offline maintenance and backup verification now recognize that existing
  cross-pool history without relocating namespace bindings or committed bytes.

- Self-identifying pool members bind logical shard slots at startup independently
  of configured mount paths/discovery order. New addresses are member-relative;
  a shared resolver reads pre-portability NDX unchanged. Verified offline clones
  and single-member reconstruction support new locations; startup fences and
  existing health gates remain enforced. No object cache or namespace changes.

- Named, independently expandable storage pools with immutable K+P geometry
  and membership. New pools do not require rebalancing old objects.
- Unified pool roots replace separately configured index/blob roles.
- Flat object keys and virtual prefixes replace real-folder semantics.
  Empty-folder creation is rejected; prefix operations process individual keys.
- Checksummed NCX references use P+1 holders. Ordinary rename, move, trash
  and recovery copy unchanged NCX to every destination before deleting sources;
  they do not alter committed NDX or payload.
- Removed namespace tombstones, deletion generations and recovery-origin
  markers. Interrupted mutations may leave both names; retry finishes deletion.
- Latest version remains the maximum valid ULID, without a mutable head pointer.
- Retained exact-key coordination, upload/GC protection, root-identity checks,
  bounded workers and durable upload publication.
- Updated setup, backup, repair, exports and proof tooling for pool/flat storage.
- No automatic migration or compatibility with earlier development formats.
- Fixed automatic healing racing whole-fragment deletion: per-fragment
  exclusion and existence revalidation prevent false read-only faults and
  recreation of deleted fragments. Ordinary NCX mutations are unchanged.
- Whole-fragment deletion now reports storage errors and retains aliases for
  retry. Shared upload/repair admission rejects stale uploads after deletion
  without serializing concurrent object uploads.
- Upload NCX replica publication uses the existing bounded parallel holder
  helper. Batch acquires I/O admission before buffering, caps requests at
  4,096 objects, and follows one-shot durability configuration/options.
- Replaced the obsolete real-directory batch proof fixture and made filtered
  upload unit tests initialize their own storage settings.
- Health now reconciles automatic-healing readiness with the live safety gate.
  Failed scans retry from their persistent fault state even if topology updates
  replace the display phase; only successful repair clears the scan fault.
- Added shell-integrated offline pool verification, byte-exact healthy cloning
  and explicit verified single-member replacement with interrupted-work resume.
  Replacement includes acknowledged durable-upload pieces and activates root
  identity only after complete validation; NCX mutation semantics are unchanged.
- Backup restore maps logical pool/member slots rather than alphabetic mount
  names. New snapshots identify storage format 4; authenticated flat-v4 archives
  previously mislabeled 3 can recover membership without guessing older layouts.
- Exposed the shared backup implementation through `litiaina-n1-shell backup`;
  scheduled jobs and SSH receivers support the integrated entrypoint, while
  existing standalone helper/job entrypoints remain supported.
  Offline activation rejects missing/divergent durable manifests and receipts
  rather than treating a replacement marker as proof of health.

The October 6 copy/delete candidate passed testing and was subsequently deployed
to the main server with its matching backup helper. Objects and configuration
were preserved; Console and Accel were unchanged. Performance results include
both regressions and improvements; deployment does not declare LTS readiness.
The subsequent upload/deletion hardening above passed isolated validation and
was later deployed with its matching backup helper. Live storage-preservation,
batch/mutation smoke and stale-upload rejection checks passed.

## 3.5.1 — historical

- Restart-safe, version-pinned ZIP/TAR export jobs with resumable downloads,
  mapped entries, signed URLs, cancellation and artifact retention.
- Hardened upload finalization, cancellation, namespace repair and GC overlap.
- Encrypted offline snapshots, incremental block reuse, repository replication,
  portable/resumable restore and optional supervised backup jobs.
- Optional authenticated Accel namespace-search integration.

## 3.5.0 — historical

- Streaming erasure coding moved to ECS Core while N1 retained placement,
  metadata and durability/publication responsibilities.
- Kernel write failures became authoritative for upload capacity; ENOSPC and
  EDQUOT map to 507 without successful object publication.
- Standard small-object batch upload with independent per-object results.

## 3.4.0 — historical

- Standard multipart became nonpersistent until finalization; durable multipart
  remained the restart-resumable alternative.
- Fragment-alias self-healing with ambiguous authority rejected.

## 3.3.x / 3.2.x — historical

- Trash and namespace lifecycle revisions, replica reconciliation and recovery
  hardening. Earlier directory/generation mechanisms are not the format-4 design.
- NOA v1 introduced under /noa/v1; old /n1 routes were not retained as aliases.

## 3.1.x — historical

- Object-owned EC layout, physical root identity and autonomous maintenance.
- Availability/checksum repair scheduling and disk IO admission controls.

## 2.x / 1.x — historical

- Single-node immutable object storage, replicated namespace, streaming EC,
  repair, multipart, shell and native protocol.
- Format 2 separated namespace from colocated object metadata/payload. Both
  split-role and colocated configurations were tested; neither defines format 4.
