# RC.2 hardening gate

Status: **in progress; not release qualified**. Do not publish, tag, deploy, or
advertise RC.2 as qualified until every required gate has passed. The working
tree retains the existing RC.1 version identifiers during this work.

The candidate keeps encoding-4 NCX authority, immutable committed NDX and pieces,
stable fragment/object identities, and maximum-ULID version selection. Changes
under qualification are upload-owned payload staging, conservative upload GC,
and an optional paced online checksum-maintenance schedule.

The schedule is disabled by default. Legacy interval settings do not enable it.
Stopped, interrupted, or errored maintenance reports incomplete coverage rather
than asserting engine failure or setting a global read-only gate. Existing
topology/identity safety gates remain independent. Repair admission and stop
requests share one atomic state so a queued stop cannot be reset by worker
startup. Stopped paced batches drain without sleeping once for every queued
object. Durable mid-scan pause/resume is outside the implemented contract.

New binaries can resume legacy durable sessions. New staged pending sessions
require a staging-aware binary; an older binary does not understand their
payload placement. Committed objects retain their original encoding and final
addresses. A rollback must account for unfinished sessions explicitly.

Required qualification:

- Complete engine, workspace, backend, codec, version, and harness regressions.
- Stage/finalize, mixed-member promotion recovery, cancellation, duplicate body
  handling, and immutable-version collision protection.
- Scheduled GC reclamation after SIGKILL, preserving active sessions, live and
  historical versions, young/future candidates, and redirected/unknown entries.
- All three strict batch barriers under injected EIO and SIGKILL.
- Optional calendar scrub: disabled legacy defaults, cold corruption repair
  while clients read/write, singleton admission, restart without duplicate slots,
  and stopped/incomplete maintenance remaining online.
- Matched original-NCX/candidate measurements on the same hardware; retain RSS,
  upload and maintenance timings, failures, and complete hash/restart checks.
- Million-object inventory, complete historical hashes, million-mutation churn
  through 256 clients, final hashes and restart verification.
- Four-member physical qualification when MCH is available. Two local USB HDDs
  qualify the tested 1+1 geometry only; they do not substitute for four members
  or physical power-loss testing.

Local evidence belongs under
`/mnt/PROJECT-DISK-1TB/n1-rc2-hardening-20261030-01`. Builds may use RAM-backed
temporary storage with swap disabled for the build process, with witnesses
copied to the HDD. Local HDD startup I/O stalls and unavailable MCH results are
infrastructure observations, not passing N1 gates. Failed attempts are retained
and corrected runs use new evidence directories. A smaller passing run never
certifies the million-object gate.
