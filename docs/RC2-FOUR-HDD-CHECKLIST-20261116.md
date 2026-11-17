# N1 and M1 four-HDD qualification checklist — November 16, 2026

**Active checklist, updated November 17, 2026.** The operator opened the
four-drive window early, ahead of the planned November 20 start. See the
[recorded results](FOUR-DRIVE-QUALIFICATION-20261117.md) for completed checks,
remaining performance observations and original failures. Three physical
power-cut windows now pass independent verification: readback after publication,
idle services and strict multipart reception. The last preserves all 94 strict
part ACKs before recovery. Other exact transition windows keep their separate
scope. This file
is not an automated power-cut job. Use the [hardware reporting profiles](TEST-PROFILES.md).

Use the four-HDD reference system to qualify N1 and M1 recovery and begin
controlled hardware power-loss testing. Preserve the completed million-object
and previous four-drive campaigns. Repeat affected checks for the selected
candidate; do not repopulate those campaigns routinely. Remaining release gates
are in [the completion checklist](RC2-IMPLEMENTATION.md).

## 1. Hardware and test scope

- [x] Confirm the hardware profile: Intel Core i5-11400, 6 cores / 12 threads,
  approximately 11 GiB RAM, four HDDs and a separate SSD/NVMe. Record actual
  specifications, Linux version and access to kernel logs.
- [x] Identify all four HDDs by device, serial, filesystem UUID and mount path
  in the private inventory. Confirm four distinct devices/filesystems and record
  their transport in the public summary.
  Existing reference roots are `/mnt/DISK0` through `/mnt/DISK3`.
- [x] Check current drive/filesystem health, free space and kernel I/O errors
  before writing. Preserve any existing faults as evidence.
- [x] Place M1's database and staging on the designated SSD/NVMe, and N1's
  private pool members on the four HDDs. Record control/build/log paths too.
  Capacity must cover the selected payloads, staging, backups and retained logs.
- [x] Use fresh task-private directories without formatting or erasing existing
  datasets. List all services affected by loss of power to this host and arrange
  their operator-controlled maintenance window before the first cut.
- [x] Keep the client, acknowledgement journal and control connection on a
  separate powered computer. A battery-powered laptop is suitable if its journal
  is synchronized to persistent storage. Keep the network equipment powered.
- [x] Identify the exact switched power feed and confirm that it removes power
  from the test server and every tested drive, including separately powered enclosures.
  Record manual switching or the actual PDU mechanism. A reboot, reset, process
  kill or USB disconnect is a different fault and must be labelled accordingly.
- [ ] Provide a separate recovery/backup destination. Record whether a spare
  HDD is available for actual hardware replacement; a cleared existing member
  tests blank-member enrollment, not a different physical drive.

## 2. Freeze the candidate and validate the recorder

Complete system `1.0.0-RC.2` selects N1 `1.0.0-RC.2` and M1 `1.0.0-RC.1`;
verify their independent versions against [the system manifest](../deploy/system-release.json)
and [release policy](VERSIONING.md). Historical binary hashes are not relabelled
when package versions change.

The November 15 starting runtime source was `f114c30`, documented in
[the M1 commit I/O report](M1-COMMIT-IO-20261114.md). The November 17 selected
artifacts are recorded in [the current qualification](FOUR-DRIVE-QUALIFICATION-20261117.md).
Documentation edits do not change their identities. Verify the binaries actually
installed on the reference system; a rebuild needs its own hash, toolchain and
regression evidence.

### Preparation checkpoint — November 15

Source checkpoint `efdd40c` declares the independent component versions above.
Its five manifest/CLI checks and one real-service M1 → N1 interoperability
case pass, together with the targeted strict lint and formatting checks. These
six checks qualify the versioning change; they do not replace performance,
four-HDD, power-loss or complete operator acceptance.

### Four-drive checkpoint — November 17

The [current report](FOUR-DRIVE-QUALIFICATION-20261117.md) records the selected
frozen N1 and corrected M1 hashes, 3+1/2+2 recovery/operator/mixed checks,
compiler-matched controls, adverse timing and independent storage measurements.
The [external Rust recorder](../metadata_engine/examples/power_loss_witness/README.md)
passes clean N1/M1 checks and two process-interruption/reopen/recovery dry runs.
All prior successful replies remain on the independently powered client's HDD;
verification precedes recovery and retained strict parts are not resent.
The operator subsequently confirms the power feed and shared-service outage,
removes the plug and restores power after both clients drain. All six retained
journals verify before explicit missing-part-only recovery. The offline database
check and exact shard census also pass. This is one recorded physical window,
not every transition in the matrix below.

Two later physical cuts retain the extended corpus, including an interruption
during strict uploads on both paths. All ten journals verify before recovery:
34 complete objects, seven absences and 94 strict part ACKs. Missing-part-only
resume, a read-only 160-shard audit, the final offline database and exact member
census pass. These three recorded cuts do not certify the unexecuted exact
instruction windows in the matrix below. No further cut is scheduled here.

- Keep N1's selected runtime and ECS Core revision
  `c77c638bfb709368f49517f2828fa977738e8467` fixed. No dependency upgrade or
  engine redesign is part of this preparation.
- Preserve a portable copy of the source, selected executables, checksums and
  version/interoperability logs. Keep existing population journals and earlier
  evidence with their original identities; verify the copied checksums before
  starting on the reference system.
- The November 17 checkpoint completes the hardware inventory, separately
  powered recorder dry run and three operator-confirmed physical cuts. Process
  interruption and the remaining physical transition windows retain their
  separate classifications.

- [x] Record source commit, compiler, build flags, executable hashes, effective
  INI settings, durability modes and pool geometry. Hold the selected N1 runtime
  fixed during M1 comparisons. Preserve the baseline binaries separately.
- [ ] Verify io_uring initialization and the intended direct-I/O path rather than
  assuming an environment variable proves which backend is active.
- [x] Prepare a Rust client/recorder on an independently powered system that
  survives losing its connection. Existing process-fault tests do not provide a
  hardware power-loss controller or independently powered ACK journal.
- [x] Record operation IDs, LINK keys, named paths/versions, lengths, expected
  checksums, durability mode, acknowledged parts and final/delete outcomes.
  Store credentials separately from the evidence.
- [x] Synchronize the journal file and its directory when created. Synchronize
  new ACK records before using them as a power-cut trigger. Keep deterministic
  source data or enough information to reproduce and check every payload.
- [x] Dry-run the recorder against an isolated fixture: interrupt the connection,
  reopen its journal, and confirm complete records, exact expected ACKs and
  recovery without replacing them with a new population.
- [x] Distinguish failed/unknown requests from successful replies. After a cut,
  drain any complete replies already in flight and synchronize the final ACK
  set before verification. A successful durable reply must not be excluded just
  because it arrived near the cut.
- [x] Prove that the selected read-only verification can target the retained
  objects and sessions without first re-uploading or repairing their contents.

## 3. Test order and fault coverage

Run one controlled fault at a time. Record the phase actually observed; without
a demonstrated transition trigger, a cut during upload is not evidence of a
cut at one exact fsync instruction. Use existing test-only barriers for precise
process-fault coverage and identify that evidence separately.

| Stage | Required exercise | Required observation |
| --- | --- | --- |
| Preflight | Standalone N1, M1 → N1 and Console → M1 → N1 on the selected installation | Upload/read/resume/finalize and credentials work; baseline objects are fully verified and recorded externally |
| Four-member recovery | Separate immutable 3+1 and 2+2 fixtures; affected within/beyond-parity loss, corruption, replacement and explicit repair checks | Exact payloads and repaired shards where sufficient validated members remain; explicit failure beyond recoverability; no topology mutation |
| Payload reception | Cut during normal and durable multipart, including an acknowledged strict part and an incomplete request | Prior strict ACKs retained; incomplete requests never treated as acknowledged; continuation and immutable retries follow the same session contract |
| Finalization/publication | Cut while finalizing standalone N1 and while M1 publishes a named version | No partial object is published; completed durable ACKs survive; an uncertain attempt is absent/pending or complete and its permitted retry is safe |
| M1 persistence | Cut during database commit and checkpoint with concurrent readers/writers | No partial cross-table transaction or broken snapshot; every prior durable mutation remains correct after reopen |
| Repair | Cut during reconstruction of a private member | Surviving validated source data remains intact; partial repair is not accepted as a healthy shard; explicit retry completes correctly |
| GC and purge | Cut during TTL reclamation/object deletion; include a retained successful purge from before the cut | Pending reclamation remains retryable until object deletion succeeds; acknowledged purges remain absent and old continuation cannot recreate them |
| Repetition | Repeat representative power-cut windows with newly recorded operations while retaining earlier acknowledged objects | Later interruptions preserve all still-required prior ACKs; record each cut and outcome separately |

Normal intermediate part ACKs are not strict durability promises. An unused
Begin may disappear before its first durable payload. Successful finalization
durably publishes under the current N1 contract, regardless of intermediate
part mode. Test those distinctions rather than requiring normal parts to be
lost or calling a permitted loss a durability failure.

An interrupted purge without a successful reply may leave readable survivors
when quorum remains. M1 must retain its reclamation intent and retry; that is
different from an acknowledged purge leaving data behind. A completed purge
must leave no identity-specific staging or final shard files.

## 4. Procedure after every power interruption

1. Seal the independent ACK journal and record the cut time, observed workload
   phase, actual power scope and successful/uncertain requests.
2. Restore power, retain boot/kernel/filesystem and service recovery logs, and
   record startup failures. Do not recreate the database, overwrite missing
   objects or rerun the upload population to make verification pass.
3. Check every still-required acknowledged object by exact length, checksum and
   version; check durable part progress and named metadata mutations separately.
   Account for later acknowledged trash, rename and purge operations.
4. Inspect interrupted operations, publication boundaries and pending GC/repair
   work before corrective action. Verify their allowed retry/recovery and final
   outcome, retaining both pre-repair and post-repair evidence.
5. Retain offline database checks where supported and scoped metadata/physical
   censuses. Run broad scans outside throughput timing, not as a new foreground
   monitoring behavior. Stop and preserve the fixture on an integrity failure.

## 5. Performance, Console and release evidence

- [ ] Measure N1 alone and M1 → N1 independently, with the same payloads,
  topology, durability, client geometry, concurrency and build settings for
  each baseline/candidate pair. Use alternating run order and record cache/load
  conditions; do not turn a RAM-only database control into a durable result.
- [ ] Retain enough individual small-object samples for p50/p95/p99, plus
  15 GiB and 100 GiB large-upload/readback observations within available capacity.
  Keep per-part tails separate from object-level finalize tails.
- [ ] Record init, status, part transfer, finalize and repeated-finalize timing;
  throughput, errors, per-process CPU/RSS/I/O and device/queue observations.
  Capture fault-recorder overhead separately from ordinary performance runs.
- [ ] Exercise bounded sustained load with monitoring, GC and backup separately,
  then together. Check admission, queue/memory bounds, error handling, recovery
  and health classification. Set quantitative acceptance limits before timing;
  record any limit still awaiting a decision rather than declaring a pass.
- [ ] Exercise original Console upload/resume and operator workflows, including
  fragment credentials, health under upload, GC results/TTL, backup listing and
  restore, permissions and migration. Link each required NOA/Console case to its
  actual result; passing upload tests do not qualify the entire interface.
- [ ] Save raw samples, source/binary/configuration manifests, external ACKs,
  cut records, failed attempts and recovery reports outside the four-HDD reference system. Make a portable
  copy under `/srv/test-evidence/portable` before leaving the network.
- [ ] For each gate, record **pass**, **fail** or **not run**, its evidence and
  remaining work. Existing million-object results keep their original binary
  scope. No release, tag or RC.2-ready claim follows from scheduling this window.

Any loss/corruption of data required by a durable ACK, incomplete publication,
successful purge that resurrects, or unresolved acceptance failure keeps the
corresponding release gate open. A successful campaign qualifies its recorded
hardware, candidate and fault windows; it does not prove every possible failure.
