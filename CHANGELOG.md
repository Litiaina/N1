# Changelog

This is a condensed development history, not an LTS release declaration.
Results and known failures are recorded separately in [TESTING.md](TESTING.md).

## 4.0.0 — development, storage format 4

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
