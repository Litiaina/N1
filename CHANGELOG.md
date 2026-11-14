# Changelog

This file records product changes. Architecture is defined in
[ARCHITECTURE.md](ARCHITECTURE.md); commands are in
[DOCUMENTATION.md](DOCUMENTATION.md); test results are in [TESTING.md](TESTING.md).

## Unreleased — 1.0.0-RC.2

RC.2 introduces N1's LINK architecture and the independent M1 metadata store. It has not
been released. This section describes implemented changes, not a declaration of
complete API conformance or qualification.

- Reduce bridged multipart conversion I/O by verifying and forwarding each
  external file together. Retain M1's durable source until N1's final durable
  publication, avoiding a second per-part flush of the converted copy. Keep
  checksum errors, bounded memory ownership and immutable native retries.
- Recover a wholly missing unused N1 allocation from an unpublished M1 upload's
  retained source with a fresh private LINK identity. Atomically switch its
  reference and expiry index while preserving the public session/version and
  cancellation/GC fences; leave the old LINK key missing.

- Extend concurrent M1 status queries to read-only staged receipts. Preserve
  acknowledged-file checks and exclude in-flight temporary files; retain
  exclusive reconciliation and lifecycle changes. Add a reproduced bridge
  lock witness and high-resolution process CPU measurements for short queries.

- Allow indexed M1 multipart status to observe committed ACKs while a part
  body or disk write is still in flight. Keep exclusive reconciliation and
  lifecycle mutation. Add real kernel I/O-delay, concurrent status and
  bounded memory-pressure/recovery qualification with N1 unchanged.

- Stream a nonempty single-part M1 upload directly to N1 when it fits the
  native part capacity, preserving client geometry, identity, ACK/GC fences
  and restart recovery. Keep the existing bridge for empty uploads, larger
  single external parts and incompatible multi-part geometry.

- Fence M1 multipart ACK records against trash, publication, cancellation and
  GC in the acknowledgement transaction. Share existing per-upload exclusion
  with indexed GC so active writers are deferred without stalling maintenance.
  Persist bridged staging directory links before strict receipts and return
  explicit 507 responses for actual storage-space/quota exhaustion.

- Extend explicit N1 object repair to completed, unpublished staging. Reconstruct
  verified shards in `.uploads/` without publication; retain read-only completed
  upload retries and require a separate finalize operation.

- Add opt-in M1 multipart size negotiation before LINK Begin, with saved-identity
  reuse across explicit and negotiated callers. Aligned chosen parts stream to
  N1 without an M1 payload spool; explicit geometry and resume remain stable.
  Preserve the measured baseline, rejected eager-forwarding prototypes and
  adverse observations in the multipart qualification report.

- Bound M1's staged multipart buffers through the existing upload-memory budget,
  coalesce short receives and enlarge staged-file forwarding reads. Preserve
  checksum verification before forwarding and existing durable ACK ordering.
  Add real XFS ENOSPC/recovery and bridge memory-pressure, disconnect, retry,
  restart and corruption checks. N1's measured LINK runtime remains unchanged.

- Replace M1's imported Procyon/Fjall database with the independently implemented
  `m1_storage` crate: ordered on-disk tree, snapshot readers, optimistic writers,
  synchronized group commits, checksummed recovery and online checkpoints.
  Preserve the existing M1 service/domain modules, NOA and operator controls;
  N1 LINK's implementation is unchanged. Requalify this storage implementation
  independently of the historical Procyon-backed population results.

- Enforce M1's configured request quotas across HTTP and named native NOA,
  sharing peer and fragment budgets across connections and token changes.
- Keep M1 limiter cleanup off request/I/O workers, use fixed-size fragment
  identities for quota lookup, and drain the cleanup task during shutdown.
- Disable Nagle buffering on M1 HTTP/HTTPS listeners and bypass optional Accel
  header preparation when no read accelerator is configured. Preserve N1's
  object path, integrity checks and durable acknowledgement barriers.
- Add controlled Rust upload regression measurements with alternating binaries
  on the same database/pool, placement audits, individual latency samples,
  CPU/RSS, complete payload readback and separate durable-stage profiling.
- Connect M1 to optional Accel search and version-pinned reads; validate cache
  response identity/geometry, preserve origin fallback before headers and fail
  truncated streams without replaying a partial response.
- Restore direct selected ZIP64/TAR and streaming-manifest TAR archives with
  bounded export admission, immutable source pins and M1 crash cleanup. Preserve
  source errors even for zero-length TAR entries; retain queued export pins
  during graceful shutdown.
- Stream direct named-native archives beyond the control-frame bound, using an
  exact length counted during immutable metadata capture. Hold the final bounded
  chunk until source verification and pin/manifest cleanup complete.
- Coalesce short native transport reads within the existing receive buffer. Keep immutable
  framing, retry validation, upload ownership and durability barriers intact;
  matched performance qualification remains required.
- Construct large-upload frames directly in reusable aligned output buffers,
  avoiding payload zero-fill and an intermediate aligned-buffer copy. Stream
  native multipart bodies in the existing activity owner without a body relay;
  retain exact body bounds, rejection draining, cancellation ownership and
  sender-stall detection. Preserve checksums, partial-page isolation, normal/
  batch adapters, pool placement and durable acknowledgement barriers.

- Make N1 LINK + M1 the mainstream system on `main`. Preserve the former
  NCX/NDX engine as **N1 Lite** in the independent `Litiaina/N1-Lite` repository,
  with separate engine, binary, configuration and service identities. This
  branch promotion is not an RC.2 release;
  outstanding API, scale, fault and performance gates remain required.
- Formalize the two systems in this repository: **N1** is the object store,
  **M1** is the metadata store, **LINK** is N1's key algorithm and storage
  architecture, and M1 owns its specialized database foundation. Keep the
  `object_engine/` and `metadata_engine/` source structure. Cargo packages are
  `n1` and `m1`; their binaries are `litiaina-n1` and `litiaina-m1`.
- Give M1 the `L I T I A I N A  M 1` banner and readable per-line startup output;
  retain N1's signature banner. M1 defaults to `m1_config.ini` / `m1.env`;
  explicit operator paths, INI controls and native NOA protocols are retained.
- Expand dense JSON/control-flow macros and embedded configuration strings for
  readability. Clarify component module maps and shared `OperatorSettings`
  naming while retaining the existing engine structure and LINK storage logic.

- Introduce **LINK — Litiaina Immutable Native Key** for deterministic object
  addressing and shard placement within immutable storage pools. Remove NCX/NDX
  lookup from object storage; keep names and user-visible versions outside it.
- Separate the workspace into an independently usable `object_engine` and an
  optional `metadata_engine` with an independent transactional storage boundary.
- Add metadata-owned fragments, named uploads, immutable version references,
  maximum-ULID selection, namespace operations, trash/recovery and exports.
  Named HTTP/WebSocket and native adapters serve the original Console contract.
- Provide direct LINK NOA v2 operations over native `n1/1`, with a secondary
  HTTP adapter. Retain NOA v1 as the metadata interface; v2 does not replace it.
- Keep N1's adaptive concurrency, I/O scheduler, io_uring, middleware, server,
  streaming download, repair, backup automation and module responsibilities.
- Retain operator INI/environment controls for listeners, TLS, pool enrollment,
  concurrency, I/O, staging cleanup and optional scheduled healing. Expand
  capacity by enrolling complete immutable pools.
- Make the total-object size cap optional in both engines. Omitted
  `[set_limits] max_file_size` or `unlimited` disables the policy cap; positive
  byte counts retain explicit limits independently of multipart part size.
- Allow a separate `[server] env_file` for each component and environment-only
  secret provisioning without a mandatory `n1.env`; retain strict file checks.
- Reuse authenticated object connections after complete native control errors,
  avoiding a fresh login per missing-status page on large multipart uploads.
- Add CRC32C frame verification with context-bound source SHA-256 for the hybrid
  shard profile; preserve immutable integrity profiles within enrolled pools.
- Restore aligned `O_DIRECT` large uploads through the retained ECS buffers and
  owned I/O backend. Preserve immutable framing, concurrent part boundaries,
  checksums, admission and acknowledgement durability; keep small writes buffered.
- Skip reads of proven sparse frame groups on large uploads. Keep ordinary
  validation for written retries and unsupported extent queries; preserve the
  shard format, integrity checks, publication ordering and durability barriers.
- Classify deferred deep-scan checks separately from confirmed corruption and
  drain owned diagnostic work during metadata shutdown.
- Use one LINK key from Begin through finalization and continuation that only
  opens existing files. Persist incomplete headers and paths with the first
  durable payload; an unused Begin allocation may disappear on power loss.
  Purge removes all
  identity files without terminal records; metadata or standalone clients retain
  unacknowledged purge intent until retry succeeds.
- Keep normal-upload data synchronization before publication and join namespace
  persistence per filesystem. Establish the immutable shard length sparsely
  before payload writes to avoid unused speculative allocation beyond EOF.
- Add conservative upload staging GC, object recovery points, replication and
  explicit member verification/reconstruction. Restoring an older recovery
  point explicitly rolls back later writes and purges.
- Add Rust physical qualification and failure auditing for direct objects,
  metadata transactions and original-console-to-metadata-to-object operations.
  Retire cached metadata transports before the object's native idle deadline.
- Verify empty named downloads against the object engine before returning
  success, so missing backing shards cannot become a successful empty response.
- Recover a disconnected metadata-to-object download with one fresh native
  connection only before any destination bytes or destination error. Keep the
  original admission/shutdown guards; never replay writes or partial downloads.
- Add exhaustive shard-loss/corruption checks, full metadata-loss restoration,
  in-flight process-crash/resume and explicit physical HDD unmount/remount tests.
  Extend encrypted-replica recovery to cover loss of both original members.

- Add metadata-owned indexed trash TTL and expired-upload reclamation, automatic
  and explicit bounded GC, durable deletion retry and export/reference pinning.
- Replace the object-only maintenance adapter with scoped metadata integrity/
  healing jobs and verified pool migration across different EC geometries.
  Keep names/version IDs stable while atomically switching private LINK keys;
  retain source and restart/stop state until destination verification completes.
- Update the existing Console maintenance cards with pool selection, effective
  metadata GC policy and truthful job status. Keep the original product layout.
- Drain queued Shell download-file writes before reporting a short native body,
  preserving partial output rather than racing process exit.

Remaining export lifecycle, full NOA conformance and release qualification are
tracked in [the completion checklist](docs/RC2-IMPLEMENTATION.md). Benchmark and
population outcomes are not release notes; see [TESTING.md](TESTING.md).

## Earlier architecture

The entries below describe **N1 Lite**, the former NCX/NDX engine, and its historical releases
or private development labels. They do not describe LINK storage or imply
compatibility with RC.2. The pre-LINK unreleased notes are preserved in Git at
`9939ec8:CHANGELOG.md`, rather than being presented as current RC.2 changes.

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
to the primary instance with its matching backup helper. Objects and configuration
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
