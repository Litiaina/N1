# Changelog

This is a condensed development history, not an LTS release declaration.
Results and known failures are recorded separately in [TESTING.md](TESTING.md).

## 4.0.0 — development, storage format 4

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
