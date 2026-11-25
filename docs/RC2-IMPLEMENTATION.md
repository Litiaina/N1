# N1 and M1 RC.2 completion checklist

This checklist records RC.2 qualification. The repository contains the N1 LINK
object store and the M1 metadata store with its own specialized database.
See [ARCHITECTURE.md](../ARCHITECTURE.md).

System RC.2 targets N1 `1.0.0-RC.2` and M1 `1.0.0-RC.1`. The independent
component versions and selected pairing are defined by
[the release policy](VERSIONING.md) and
[system manifest](../deploy/system-release.json). A version declaration does
not close a qualification gate or create a release.

## Current release decision — November 25, 2026

The [API and operator qualification](RC2-API-QUALIFICATION-20261124.md#candidate-and-fixtures)
records the baseline identities. Package revision 2 pairs the qualified N1
executable with M1's maintenance status and Trash pagination fixes. Its
artifact manifest identifies the exact binaries and their source commits.
Console has a separate distribution and its own follow-up performance checks.

| Gate | Current state |
| --- | --- |
| N1 runtime | Frozen at the selected `f1a54f…` executable. Minimum-profile performance, 3+1/2+2 recovery, three whole-host cuts and standalone 2+2 physical member loss and return have passed their recorded checks. |
| M1 storage and named services | Owned-storage, retained million-campaign and service/recovery results remain scoped to their artifacts. The November 18 monitoring change passed 115 default tests, eight local service cases and twelve four-drive executions. |
| Reported Console monitoring regression | Closed by the [scan-admission/presentation qualification](M1-CONSOLE-20261117.md#monitoring-and-scan-admission--november-18-2026). Repeated starts join; explicit Stop drains; hidden panels stop scan polling. |
| Maintenance status follow-up | The previous M1 returned 403 after scheduled GC when queried with a regular fragment token. The exact updated M1 release binary passes that regression, including scope isolation and the existing start/stop permissions. Console presents GC, repair and migration results beside their controls and renders the write-pool layout from N1. |
| M1 maintenance recovery and mixed reads | Passed on the selected M1. The 3+1 and 2+2 runs completed 11,912 operations without errors, verified 1,748 identities after restart and restored four snapshots. Full and ranged reads also passed the targeted early-rejection and late-damage checks on both layouts. [Four-drive results](FOUR-DRIVE-QUALIFICATION-20261117.md#system-candidate-verification--november-24) |
| Contract and operator qualification | Passed for the selected N1, M1 and Console builds. The public NOA checks, fresh browser walkthrough, named snapshot restore and optional N1 Shell recovery workflows have recorded evidence. [API and operator matrix](RC2-API-QUALIFICATION-20261124.md) |
| Sustained resource/performance acceptance | Accepted for the tested four-HDD profile on November 25. All 1,641 durable small uploads finished under 2 seconds. The worst window measured 1.67 s p99. The five-minute HTTP/native overload passed on 3+1, including readback of all 2,267 acknowledged writes after restart. [Latency sign-off](FOUR-DRIVE-QUALIFICATION-20261117.md#mixed-load-latency-sign-off--november-25) · [Sustained admission results](FOUR-DRIVE-QUALIFICATION-20261117.md#sustained-http-and-native-admission) |
| Physical recovery | Passed. Earlier cuts covered strict reception, publication and standalone 2+2 reads during physical two-member loss. Four additional whole-host cuts passed at M1 journal commit, M1 checkpoint publication, N1 shard repair and M1 GC during N1 purge. [Targeted physical results](FOUR-DRIVE-QUALIFICATION-20261117.md#targeted-physical-interruptions--november-25) |
| Engine package revision | Linux x86-64 bundle with N1, M1, the N1 backup helper, and systemd installers. Both units rendered and verified. The revision tag `v1.0.0-RC.2+2` supersedes the withdrawn bundle and identifies the corrected package. |

The sections below retain earlier evidence and its limits. A historical open
item is not automatically a new instruction to restart a completed population.

The [November 17 M1/Console qualification](M1-CONSOLE-20261117.md) holds both
selected engines fixed: 134 default M1/storage/native-client checks, 31 explicit
SSD service cases, real two-HDD Console backup/upload/migration/GC workflows,
400 enabled browser executions and both independent-engine launcher profiles
pass. Console identifies M1 and N1 separately and retains its desktop interface.
This closes the recorded UI/launcher regressions; final release acceptance and
the exact scope of the earlier hardware campaigns remain separate.

The same report's [native search qualification](M1-CONSOLE-20261117.md#native-m1-search--november-18-2026)
adds bounded M1 search without Accel: 113 default M1 checks and ten explicit
service cases pass. N1 and the database format remain unchanged. Console
handles unknown totals and sparse continuation pages; no release is implied.

**Operator schedule:** the four-drive window opened early on **November 17,
2026**, ahead of the planned November 20 start (Asia/Manila). Continue full N1
and M1 qualification; perform coordinated physical power loss last. Release
RC.2 after its required gates pass. Target stable
**1.0.0 on February 14, 2027**, conditional on qualification and the intervening
real-deployment evidence. The [release plan](RC2-RELEASE-PLAN.md) orders the work;
target dates do not waive failures.

**Hardware preparation:** use the four-HDD reference system in the operator's confirmed window.
[The preparation checklist](RC2-FOUR-HDD-CHECKLIST-20261116.md) covers hardware,
independent ACK recording, four-drive recovery, controlled power loss and
acceptance evidence. The [November 17 qualification](FOUR-DRIVE-QUALIFICATION-20261117.md)
passes the selected 3+1/2+2 member/process-fault, public API, Console backup,
migration and mixed-maintenance checks with N1 fixed. It corrects a reproduced
M1 stale-connection read-control failure. Matched timing retains adverse tails
and an independently measured slow HDD; it does not establish universal
performance parity or complete hardware power-loss acceptance.

The same report now records the first operator-confirmed physical power cut:
all four HDDs lose power while independent clients retain ACKs. Before explicit
resume, 20 complete objects, four required absences and both pending uploads'
strict parts verify. Missing-part-only recovery and the offline metadata/physical
census pass. This closes that recorded prior-ACK window, not the remaining
active-write/transition or final release acceptance conditions.

Two subsequent physical cuts retain the expanded corpus: an idle-service cut
and a cut during strict multipart reception on both N1 and M1 paths. All 94
strict part ACKs verify before missing-part-only recovery. The final 43-identity
corpus, all 160 retained shards, the 282-record offline database and exact
member census pass with both server binaries unchanged. This closes the
recorded strict-upload power-loss window; unexecuted exact transition windows
and final system acceptance remain separately tracked below.

The [first preparation batch](M1-ACCEPTANCE-20261113.md) holds N1 fixed, reduces
M1's observed bridged-upload CPU cost and passes 418 workspace checks, the same
seven selected service cases on SSD and two HDDs, and separate object-full/
metadata-full XFS recovery cases. The bridge still forwards payload during
finalize; physical power loss, broader saturation and final contract/Console
acceptance remain open.

The [November 14 geometry qualification](M1-PART-GEOMETRY-20261114.md) adds
optional native-aligned M1 init and preserves existing explicit sessions across
negotiation. The slower eager-forwarding/spool experiment is rejected. A fault
probe independently reproduces a completed-but-unpublished N1 staging repair
gap: damaged sealed staging refuses finalize and its replay. The subsequent
[explicit-repair correction](STAGED-RECOVERY-20261114.md) passes the same physical
two-HDD witness, restores only staging, preserves retry immutability and leaves
publication to finalize. This scoped recovery case is closed; remaining host
power-loss, critical-transition and performance acceptance conditions remain.

The [M1 ACK/GC hardening](M1-GC-ACK-20261114.md) subsequently passes 424 default
workspace tests, nine selected real-process service cases, two-HDD active-writer
GC and actual staging/object/metadata ENOSPC recovery. It closes the reproduced
active-writer purge/late-ACK race without a namespace or final-shard scan. Its
matched multipart measurements retain adverse latency, CPU and throughput;
performance acceptance remains open alongside memory/stalled-I/O, broader
saturation, final contract/Console and final-candidate hardware qualification.

The subsequent [single-part streaming qualification](M1-SINGLE-PART-20261114.md)
removes unnecessary M1 payload staging for a nonempty single external part
within native capacity. All 425 default workspace checks, 11 SSD service cases,
two physical HDD witnesses and four original Console browser cases pass.
The matched small multipart run improves latency and CPU; adverse large-file
controls remain explicit performance acceptance work.

The [concurrent-status/resource qualification](M1-RESOURCE-RECOVERY-20261114.md)
then closes an indexed M1 status request waiting behind active part writers. The
corrected M1 passes an actual private kernel write-delay/restart witness, 15
selected SSD cases, four physical-HDD cases, four original Console workflows and
a short real cgroup pressure profile. All 425 default workspace tests pass. This
covers the recorded local resource profiles; broader sustained saturation,
physical power loss, final the four-HDD reference system hardware and complete
contract/performance acceptance remain required.

The [bridge-status follow-up](M1-BRIDGE-STATUS-20261114.md) then passes the
equivalent active-body query for staged receipts, retaining file-size checks
and existing ACK/finalize/GC rules. All 16 selected service cases, two HDD
witnesses, four original Console workflows and the repeated local kernel-delay
and memory-pressure profiles pass. Its measured idle tails remain explicit;
the broader final acceptance conditions above are unchanged.

The [M1 bridge I/O/recovery qualification](M1-BRIDGE-IO-20261114.md) removes
object-wide checksum/cache churn and repeated flushes of a converted copy
already backed by durable M1 source. Its selected source passes 426 default
checks, 18 selected SSD cases, four two-HDD cases and four original Console
upload workflows with N1 unchanged. Confirmed loss of an unused allocation is
recovered with a fresh private LINK identity, preserving the named session and
ACKs. The measured large-file gains and adverse controls remain scoped; final
power-loss, the four-HDD reference system, saturation and complete
operator/contract acceptance stay open.

The [latency follow-up](M1-LATENCY-20261114.md) reproduces small one-shot tails
under retained desktop load and correlates M1/N1 persistence waits with XFS
kernel stacks. Its A–B–B–A database-medium isolation identifies shared I/O and
N1's filesystem-wide publication barrier as concrete costs. The RAM control
does not qualify a durable deployment or close tail acceptance. N1 remains
unchanged; 426 default workspace tests and strict normal/diagnostic lint pass.

The [M1 commit I/O follow-up](M1-COMMIT-IO-20261114.md) reduces database append
calls while retaining the existing synchronization policy and frozen N1. It
preserves the small-object tails and mixed large-file observations; fewer
syscalls do not close persistent tail-latency, the four-HDD reference system or
power-loss acceptance.

The [November 16 standalone mixed workload](N1-MIXED-WORKLOAD-20261116.md)
then holds the same N1 binary fixed on two XFS HDDs. Five/fourteen/five concurrent
application sessions complete 2,541 measured operations without request errors,
with all 513 identities verified before and after clean restart and subsequently
purged. Small uploads/deletes and some GET/resume requests retain multi-second
tails, while large uploads slow under the busy mix. These costs occur without
M1. This closes the recorded local functional-mix exercise, not standalone
tail-performance, four-HDD or hardware power-loss acceptance.

Its subsequent [two-SATA-SSD control](N1-MIXED-WORKLOAD-20261116.md#two-sata-ssd-control)
holds both executables and workload settings fixed: 4,291 operations and 872
identities pass on SATA data/OS SSDs, without NVMe test files. Busy multipart
byte rate rises from 14.200 to 49.333 MiB/s and busy mutation/read tails shorten;
some light-after observations worsen. This demonstrates platform-dependent
performance, not a new N1 version regression or proof of enclosure failure.
N1 remains frozen; the comparison does not reopen its architecture or close
the separate hardware/fault/system release gates.

The subsequent [publication follow-up](N1-MIXED-WORKLOAD-20261116.md#publication-follow-up-on-both-minimum-storage-profiles)
tests normal, durable and mixed traffic on both minimum 1+1 storage profiles.
It rejects a single-PUT directory-sync experiment after adverse HDD results,
retaining the established single-PUT, batch, purge and scheduler behavior.
The retained correction pins multipart publication directory descriptions before
namespace mutation. No M1, LINK, codec or storage-format change is involved.
This is scoped N1 qualification, not a universal performance or RC.2 release
claim. N1 stabilization takes priority over further M1 optimization.

**Branch policy — November 7, 2026:** `main` is the mainstream N1 LINK + M1
development branch. The former NCX/NDX engine is the independent **N1 Lite**
product in `Litiaina/N1-Lite`, with its own engine and operational identities.
Promoting development to `main` does not release RC.2
or close any acceptance gate.

**Release hold:** the [November 8 full-stack million-object campaign](MILLION-OBJECTS-20261108.md)
passed on the SSD test system, with M1 on DATA_SSD and N1 on two XFS HDDs. Its complete
original-version/payload readback after killing and reopening both engines
closes the requested API population count for that checkpoint/profile.
Standalone-only and original Console population, the four-HDD reference system minimum-hardware and
remaining fault/power-loss qualification retain their separate gates. A
process-kill/recovery pass does not establish hardware power-loss resilience.
Full-stack performance and the completion gates below remain required before
releasing RC.2.

**M1 storage replacement — November 10:** the Procyon/Fjall implementation has
been replaced by the independent `m1_storage` crate. Its ordered tree, optimistic
transactions, snapshots, durable group commits and checkpoints preserve the
existing service boundary. Earlier full-stack/KV passes qualify their original
binaries, not this replacement. New-core recovery, concurrency, physical service
and matched performance qualification must be recorded before its release.
N1 LINK's implementation is unchanged. The
[owned-storage qualification](M1-STORAGE-20261110.md) records the new core's
million-entry process-crash/recovery pass, retained service checks and matched
comparisons. The final local workspace has 374 passing tests, zero failed.
After reboot, the final candidate passes a million KV entries plus 100K
mutations independently on each HDD, 8,192 fresh named two-HDD uploads with
full post-SIGKILL readback, and all 24 selected physical service cases.
Both USB HDDs subsequently go offline again during the named-service comparison;
the final failed phase uses the old Procyon-backed M1. After another operator
reboot, both retained databases and all 8,192 original objects pass verification
without re-uploading acknowledged data. The shared USB path remains unchanged;
another fresh comparison aborts on both drives' renewed UAS errors, before
completing its first old-baseline phase.
A complete healthy named-service HDD comparison, new-core million-object
named population, Console, four-drive and controlled power-loss qualification
retain their separate gates.
The final SSD campaign passes a million KV entries plus 100K mutations, 18
real-service cases and complete offline record verification. The batched database
comparisons improve, but the SSD named-service repetitions retain small-upload
latency regressions. Direct Rust stage profiling does not reproduce the HTTP
tail or settle its cause. Performance acceptance remains open for those costs.

The later [clean-HDD retest](M1-CLEAN-HDD-20261111.md) repeats both million-KV
campaigns, the 8K named campaign, all 24 service cases and a matched database
comparison successfully after recreating XFS on both disposable HDDs. USB
faults recur on both drives during the first old-M1 upload-benchmark phase;
the guard stops the campaign. Fresh filesystems do not close the transport
or end-to-end performance gate. Completed integrity passes are retained
separately from the aborted comparison.

## Planned the four-HDD reference system hardening — November 11, 2026

The operator has designated the four-HDD reference system's four-physical-HDD
machine as the minimum supported hardware reference for RC.2 qualification.
November 11, 2026 (Asia/Manila) is the planned hardening campaign; it is not a
completed test or a release deadline.

Close the implementation gaps below and qualify the final candidate through
all three paths: standalone N1, M1 → N1, and Console → M1 → N1. Cover the required
API and operator workflows, population, faults, recovery and matched performance.
Record the actual hardware, filesystems, EC layouts, database/control/member
placement, configuration, source commit and executable hashes for each run.

Every required gate needs an explicit result and retained evidence. Fix failures
and rerun the affected checks, followed by the complete candidate regression.
Verify acknowledged payloads and versions after faults; measure CPU, RSS,
latency, throughput and monitoring/maintenance overhead on the reference machine.
Keep process-crash and physical power-loss results separate. Do not omit required
features, relax integrity/durability, or silently change the deployment profile
to obtain a pass. Record the outcomes of the applicable release gates before publication.

The final campaign must retain these separate acceptance results:

| Gate | Required evidence |
| --- | --- |
| Candidate identity | Clean source checkpoint, compiler/build flags, executable hashes, independent N1/M1 configuration, exact physical member/control/database placement |
| Contract and operations | Named HTTP/native and direct LINK operations; original Console workflows; permissions, refresh/revocation, IP/ports/TLS and effective concurrency/rate/GC/healing controls |
| Current-lifecycle scale | Owned-M1 split-node population, full post-restart readback, both complete member-loss readbacks, natural-expiry GC and final metadata/physical censuses pass at one million. Original ACK hashes are unchanged; exactly two million shards, zero pending/staged/unreferenced/misplaced shards. This closes the original campaign at its frozen binaries, not a million qualification of subsequent runtime changes. Extra populations and queued successors remain stopped. Console's 128-object preflight is separate; no standalone or Console million is running or queued. See [final closure](TWO-HDD-QUALIFICATION-20261113.md). |
| Storage faults | Four-drive physical 3+1 and 2+2 layouts, failures within/beyond parity, corruption detection, unavailable members, replacement and verified operator-driven repair; geometry remains immutable |
| Metadata and transactions | Lost metadata/staging, snapshot restore, pinned history/permissions, concurrent publication/GC/migration, interrupted purge and process crashes at named/object boundaries |
| Recovery and maintenance | Same/different-node object recovery, metadata backups, coordinated whole-product recovery, cross-EC migration and retry/stop; degraded or deferred diagnostics remain distinct from verified corruption |
| Performance | Matched old-N1-Lite, standalone LINK and M1 → N1 samples; uploads, reads/ranges, resume/status/finalize, batch, namespace mutations and maintenance; per-process CPU/RSS and latency distributions, including concurrent monitoring |
| Power loss | Separately identified operator-controlled host/power-loss tests and acknowledged-data verification; SIGKILL results cannot substitute |

Use task-private paths for qualification. Repeat affected failures, then the
complete candidate regression. Keep unsupported claims, unfinished runs and
fixture/setup errors out of pass counts while retaining their logs.

## Implemented components

| Component | Current implementation |
| --- | --- |
| N1 object store (LINK) | Deterministic placement in immutable pools; normal/batch/standard/durable multipart; verified GET/ranges; stat/health/repair; zero-residue purge; staging GC; paced maintenance; object backups/replication and explicit member recovery |
| N1 runtime infrastructure | Retained module structure, adaptive concurrency, I/O scheduler, io_uring, aligned O_DIRECT large uploads, middleware, server, streaming reads, banner and backup automation |
| Operator controls | INI/environment startup; IP/ports/TLS; immutable pool enrollment/append; concurrency, memory, I/O, rate, GC and healing controls |
| M1 database (`m1_storage`) | Independently implemented ordered copy-on-write tree, optimistic transactions, synchronized group commits, cross-table snapshots, bounded cache/scans and online checkpoints; replacement qualification tracked separately |
| M1 named metadata service | Logical fragments and permissions; pending upload identity; atomic named publication; max-ULID version selection; namespace/listings; bounded native name/path/extension search; rename/trash/recovery; native byte forwarding and original multipart geometry bridge |
| Public adapters | Main named NOA v1 HTTP/WebSocket and native operations; independent direct LINK NOA v2; original Console runtime/proxy retained |
| Exports and direct archives | Retained version-pinned ZIP64/TAR writer; direct selected/manifest archives with bounded workers, cancellable capture and M1-owned crash cleanup; export job state, range and signed downloads |
| M1 request quotas | Shared physical-peer auth/global limits and canonical-fragment read/upload limits across HTTP/native; disabled policy skips admission work; forwarding headers cannot choose the peer |
| Optional Accel | Configured search and version-pinned reads after M1 authorization/resolution; origin recursion bypass, validated response geometry, pre-header fallback and terminal mid-stream failure |
| Monitoring | Independently sampled metadata/object node health, cached filesystem/member capacity, bounded logical usage counters, explicit integrity checks and original Console service/pool panels |
| Metadata recovery | Operator-requested checksummed MVCC snapshots, scoped downloads, fresh-directory restore and matching paused-writer object/metadata recovery procedure |
| Console object recovery | Retained dedicated Shell operator contract; configured repository/replica/target aliases, explicit capture/verify/replicate/restore jobs using existing LINK recovery tools |
| Metadata maintenance | Indexed trash TTL and expired-upload reclamation; automatic/manual GC with bounded resources, durable retry and reference/pin protection; scoped integrity/healing; verified cross-EC pool migration with restart/stop recovery |

Implemented is not synonymous with fully qualified. [TESTING.md](../TESTING.md)
records completed checks and separates object-only, metadata-KV and full-stack
results. Each report identifies its source checkpoint and executable hashes.
The completed local
[November 4 reliability qualification](RELIABILITY-20261104.md) covers member
loss/corruption, full metadata loss and restore, crash/resume, encrypted-replica
recovery and actual OS unmount/remount. It is separate from population and
hardware power-loss qualification.

The [key lifecycle qualification](KEY-PURGE-20261105.md) covers the current
single-key upload contract and zero-residue purge. Earlier retirement-state
results remain historical; they do not qualify the current lifecycle by themselves.
The [aligned I/O qualification](DIRECT-IO-20261105.md) records the subsequent
large-upload change, unchanged shard format and lifecycle, physical proof,
repeated reliability checks and matched performance results, including costs
from cold post-upload reads/status and the measured CPU tradeoff.

The [November 8 million-object qualification](MILLION-OBJECTS-20261108.md)
passes fresh M1 → N1 population, two-million-shard census and complete original
payload/version readback after SIGKILL/reopen of both engines. It records the
SSD database/two-HDD pool profile, exact source/binaries, batch latency and
sampled per-process CPU/RSS. It is separate from the standalone-only, Console,
the four-HDD reference system topology and physical power-loss gates.

## Work required before completion

| Area | Remaining work / acceptance condition |
| --- | --- |
| Concurrent maintenance | The [November 16 two-profile scenario](M1-MAINTENANCE-20261116.md) passes mixed M1 → N1 traffic with concurrent GC, monitoring and live metadata backups: 11,259 measured operations, full post-SIGKILL verification, four snapshot restores and marker-only cleanup. Both runtimes stay fixed; maintenance costs and stale telemetry remain recorded. Broader saturation, coordinated recovery and complete operator/alert acceptance remain separate. |
| Export lifecycle | Indexed TTL cleanup, transactional retained-byte reservations and bounded restart admission are implemented. Four new real-process lifecycle/error-isolation cases and the existing service checks pass on the four-HDD reference system (21 total). The SSD/two-HDD profile’s eight-phase same-database screen records nearly equal M1 CPU but a 13.6% higher candidate upload mean with substantial phase variation; performance acceptance remains open. See the November 11 handover and split-node report. |
| Named-service recovery | Full metadata/staging loss, verified snapshot restore, named history/permissions/resume and metadata/object/both process crashes pass on two local HDDs. The current-finalization integration exposed lost volatile completion after a partial-upload restart; M1 now performs narrowly triggered, byte/part-bounded public status reconciliation and one retry. Four real resume cases and the ten current two-HDD reliability cases pass; see [SSD/HDD qualification](TWO-HDD-QUALIFICATION-20261113.md). Preserve the documented operator pause; independent live captures are not an atomic cross-service snapshot. Qualify host power loss and additional crash points separately. |
| Metadata request quotas | Shared HTTP/native mixed overload passes on the four-HDD reference system with 16 workers, exact budget checks and every ACK verified after both processes restart; the real 60-second idle-bucket cleanup also passes. Matched on/off performance samples are retained. Final deployment defaults, proxy policy and broader saturation/maintenance impact remain separate. Do not infer coverage from object-engine middleware. |
| Optional Accel | Real integration and fault-response checks are implemented. The November 13 the four-HDD reference system witness additionally passes real private-CA HTTPS, untrusted-CA/wrong-host rejection, 64 concurrent pinned reads and Accel restart using process-local CA trust. Broader sustained load and the complete conditional/range/header matrix remain separate; preserve authoritative M1 resolution and fallback rules. |
| End-to-end population | Historical Procyon-backed API M1 → N1 passed one million objects at `8dbba32`. The original owned-M1 split-node million campaign now also passes every required phase, including final automatic GC and exact censuses. Later runtime changes, original Console and standalone-only scale qualification retain separate gates; no replacement campaigns are running or queued. |
| Broader storage qualification | Four distinct HDDs on the reference system pass 3+1 and 2+2: all 24 loss combinations through the first count beyond parity, corruption/exact repair, beyond-parity refusal and sequential replacement after one/two simultaneous losses. The final connected-blank-target rerun and encrypted backup/service-identity witness both pass. A separate SSD/HDD profile places N1 on two HDDs and M1 on another host's DATA_SSD; see `SPLIT-NODE-20261111.md`. Hardware power loss remains separate. |
| Matched performance | The current key lifecycle record links original-NCX, previous-LINK and pre-optimization key-only ABBA benchmarks, plus standalone batch/multipart/concurrent upload. Record CPU/RSS, latency, throughput, allocation and every individual large-object sample. Begin now persists with the first durable payload by operator choice; a Rust reopen/readback probe verifies speculative XFS EOF allocation is gone. Earlier regressions remain in their original reports. Mixed and focused large-object results must both be reported; no universal performance claim follows. November 8 records sustained named-pipeline population metrics, not a matched old/new comparison. Matched named-pipeline, saturation and the four-HDD reference system tail-latency acceptance remain open. |

The [November 13 SSD/HDD standalone
continuation](SSD-HDD-OPTIMIZATION-20261113.md) records the subsequent N1
copy-removal candidate, compiler-matched 15 GiB comparisons on SSD 1+0 and
physical HDD 1+1, expanded small-object/read controls and an attached 100 GiB
pass with full SHA-256 readback and zero-residue purge. Observed large-upload
CPU cost falls; HDD throughput stays near its LINK reference. Adverse small
tails remain recorded. Its exact N1 checkpoint is held fixed for M1 profiling;
it does not close the final the four-HDD reference system, saturation or
hardware power-loss gates.

The November 7 preparation screen and controlled local follow-up are recorded in
[TESTING.md](../TESTING.md). The initial ABBA screen measured 14.1% more small
upload time. The follow-up holds the running N1, metadata database and pool
constant, alternates binaries and quotas, retains each request latency, audits
placement and profiles durable calls. Upload averages reverse across campaigns;
M1 listener buffering and request-path limiter maintenance were corrected, but
neither an upload speedup nor tail-latency parity is claimed. Both the original
and follow-up measurements remain available. The subsequent November 8
full-stack population pass is recorded separately. Remaining standalone/Console
population, saturation, the four-HDD reference system hardware and power-loss
gates are not closed by an average improvement or by this population pass.

The [November 13 SSD/HDD qualification](TWO-HDD-QUALIFICATION-20261113.md)
completes the original owned-M1 million campaign and tests newer binaries
separately: 401 default workspace checks, 29 real-service cases, Console
workflows and matched performance. A lost-completion resume regression is fixed
solely in M1's native adapter. Monitoring submits zero payload I/O in quiet-node
checks; same-object warm and file-cache-advised read replays do not reproduce an
earlier mixed-workload throughput loss, but retain M1 CPU overhead and every
adverse sample. These results do not close the deferred current-candidate the
four-HDD reference system, saturation, physical power-loss or remaining
contract/scale gates.

The [November 13 the four-HDD reference system optimization
continuation](FOUR-HDD-OPTIMIZATION-20261113.md) qualifies the sparse-write
shortcut separately: 218 N1 library cases, physical direct-I/O proof, renewed
four-HDD 3+1/2+2 faults, 32 matched large uploads, 1,536 small lifecycle samples
and 32 fixed-object verified GETs. The corrected M1 native archive case, 34
selected real-service/Console cases, 407 default workspace tests, mixed
HTTP/native overload with post-crash ACK readback, idle-quota cleanup and
private-CA Accel HTTPS also pass. Matched M1 and named pipeline measurements are
complete, with adverse samples retained. A balanced HDD 1+1 confirmation
reproduces an 8.21% sparse-only upload regression. Subsequent allocation-window
and larger-buffer experiments are rejected and removed. The current
sparse/receive candidate passes 222 N1 library tests, five physical direct-I/O
cases and renewed four-HDD faults. Its separate 32-large/1,536-small matched
matrix improves aggregate upload throughput in all five groups, but retains
higher four-HDD CPU time and some adverse small-object tails. Scoped counters
show fewer cycles/instructions at lower CPU frequency; strict CPU-time/tail
acceptance remains open. The exact current candidate subsequently passes 36
selected public-service/Console cases, 411 default workspace tests and strict
workspace/all-target Clippy, with all 349 Rust source files matching the
workstation manifest. Incompatible M1 multipart geometry retains its measured
staging-copy cost. These results do not qualify the rejected experiments, repeat
the prior million population or establish physical power-loss resilience.

## Fixed boundaries

- Object storage never imports the metadata database or owns names, namespace
  history or listings. All object operations remain available to an authorized
  direct client holding its LINK keys.
- Metadata completes named N1 transactions through the public native object
  interface. It must not require changing LINK placement or reducing Console
  features to match an incomplete metadata implementation.
- Pool membership and EC profiles are immutable. New capacity is a complete new
  pool. Changing operator IP, TLS or concurrency does not change object identity.
- NOA v1 and v2 are both current contracts. Neither is a promise of old NCX/NDX
  disk-format compatibility. RC.2 is still in development.
- Success requires the prescribed integrity/durability barriers. Do not weaken
  checks, retry away corruption or hide partial failures to obtain a pass.

Historical implementation notes and exact earlier checkpoints are preserved at
`9939ec8:docs/RC2-IMPLEMENTATION.md` in Git and in the reports indexed by
[TESTING.md](../TESTING.md). They are not competing descriptions of the current
product. No release follows from a source checkpoint or an unfinished campaign.
