# N1 and M1 testing

This is the current testing guide for the **N1 LINK object store**, the
**M1 metadata store with its own specialized database**, and original Console integration.
It records evidence, not the architecture or
operator procedure. Product behavior is defined by [NOA.md](NOA.md); remaining
completion gates are in [docs/RC2-IMPLEMENTATION.md](docs/RC2-IMPLEMENTATION.md).

Current Cargo packages are `n1` (object store) and `m1` (metadata store); their
server binaries are `litiaina-n1` and `litiaina-m1`. Historical evidence retains
the names and executable hashes used when each campaign ran. Renaming does not
extend those results to a new binary or replace its regression checks.

`main` now develops N1 LINK + M1; the former NCX/NDX engine is **N1 Lite** in the
independent `Litiaina/N1-Lite` repository. This product separation does not
change earlier test outcomes or close the
RC.2 release gates. Historical mentions of a held `main` describe the policy at
the time of those campaigns.

## What each test proves

| Layer | Required checks | Does not prove |
| --- | --- | --- |
| LINK/object engine | Key encoding and placement, EC/integrity, upload/finalize idempotence, exact GET/ranges, repair, purge, GC and recovery | Names, metadata transactions or Console behavior |
| M1 database | Concurrent writers/readers, atomic cross-table updates, snapshots, crash/recovery, conflict handling | Object payload durability or the NOA service contract |
| Metadata service → object engine | Pending identity before bytes, durable object completion before named publication, version/name semantics and exact reads after restart | Every Console workflow or full retained API parity |
| Original Console → metadata → object engine | Real production UI/API requests, original upload schemes, resume, listings and downloads | An API simulated by mocked responses, or untested administrative features |
| Scale/fault campaign | Every acknowledged version retained, physical placement census, zero unexpected staged/unreferenced files, process crash/reopen and complete readback | Power-loss, cold-cache performance or topology failure modes that were not injected |

Workloads and verification are Rust. Original Console tests use its existing
Node/Playwright tooling and a real headless Chromium browser. No Python is used
in the current qualification campaigns. Keep data, logs and build output on the
selected SSD/HDD/RAM locations for each profile; never put population fixtures
on the source NVMe.

## MCH RC.2 qualification — November 11, 2026

The [current handover](docs/RC2-HANDOVER-20261111.md) records four SATA/XFS
HDDs, frozen binaries, failures and remaining gates. N1 passes **201 library
cases**, zero failed, two exclusions, plus physical 3+1/2+2 witnesses covering
24 loss combinations, corruption/exact repair, beyond-parity refusal and one/
two simultaneously connected blank replacements. The corrected encrypted
backup/service-identity witness also passes. These faults use private member
directories; they do not simulate host power loss.

The current local workspace passes **378 default tests, zero failed**, with
56 explicit opt-in/helper exclusions. Workspace strict Clippy and formatting
pass. MCH's final export candidate passes **21 real-service cases**, including
per-artifact cleanup error isolation. Retrieved on November 12, the controlled
same-database screen, 4,096-object named crash/recovery/census and 19 native
storage regressions also completed successfully. One parent-invoked storage
helper is explicitly excluded. See the [MCH continuation](docs/MCH-CONTINUATION-20261112.md).
Keep these results separate from the
[home split-node qualification](docs/SPLIT-NODE-20261111.md): N1 on two local
HDDs, M1 and its database on the main PC's DATA_SSD, with a real native TLS hop.
That profile now passes its fresh 378-test workspace regression, actual
both-process kills and complete 4,096-object readback, each member's loss,
snapshot/database-loss recovery and a fresh 256-object physical-census
preflight. Its owned-M1 million campaign was stopped by the operator on
November 12: population, full post-restart readback and full member-0-loss
readback passed; member-1-loss readback reached 720,000 reported checks before
interruption. The November 12 continuation then **passed the complete original
million-object member-1-loss readback**, 13,968,125,000 bytes, zero payload/version
mismatches in 11,757.437 seconds, with unchanged original ACK hashes. Member 1
is restored; final censuses await normal TTL cleanup. A portable
source/evidence/database checkpoint is under `/home/altear/DEV_TEST`; see the
handover for its exact path and restart constraints. The
eight-phase export-feature screen records nearly equal M1 CPU but a 13.6%
higher candidate upload mean with large per-phase variation; acceptance remains
open. See that report for raw evidence and scope.

The [November 12 continuation](docs/MCH-CONTINUATION-20261112.md) records the
preserved million-object database's complete 7,001,423-record integrity pass,
the native 4,099-object recovery and interruption/corruption resume witnesses,
and an actual original-Console 128-object crash/recovery/census pass. Extra
populations and their queued successors were stopped after the operator's scope
correction. Only the original campaign's expiry cleanup and final censuses
remain. The read-only helper has its own regression and
strict Clippy checks. The 378-test workspace count above predates that helper.

The [MCH throughput measurement](docs/MCH-THROUGHPUT-20261112.md) uses NVMe M1
and the interactive four-HDD 3+1 N1 pool, with an isolated benchmark database.
Three repetitions verify 900 small objects and nine 1 GiB objects, then reclaim
all 909 through scoped GC with zero failures/deferred work. Individual and batch
small uploads, large one-shot and durable multipart, init/status/finalize/retry,
per-process CPU/RSS and every original payload/version are recorded. This is
server-local current-build performance, with warm immediate reads; it does not
close matched-baseline, browser/LAN, saturation or million-object gates.
The follow-up standalone native N1 measurement verifies and purges three
additional 1 GiB multipart objects without M1 requests. Finalize averages
59.73 ms, versus 70.43 ms for aligned M1 and 9,317.64 ms for M1's incompatible
part-geometry bridge. The same report preserves every trial and the timing
differences; no runtime code changed for these measurements.
The [15 GiB native N1 follow-up](docs/N1-STANDALONE-15GIB-20261112.md) also
passes: 320 durable 48 MiB parts, four writers, complete SHA-256 readback,
repeated finalize and four-shard purge. Whole upload is 95.45 s / 160.92 MiB/s;
finalize is 100.67 ms. This is one large-object trial, not a fault or tail-latency
qualification.

The subsequent [finalization and media qualification](docs/FINALIZATION-20261112.md)
compares the preceding LINK binary with the bounded finalization change using
the same Rust client/compiler. It measures native N1 without M1 on DATA_SSD
1+0, HDD 1+0 and HDD 1+1, including per-stage p50/p95/p99, complete 1 GiB and
15 GiB uploads, CPU/RSS and adverse observations. The final workspace passes
396 tests with zero failures; a dedicated actual SIGKILL witness on DATA_SSD
passes cold completion recovery, exact readback and purge. These are separate
results from the original MCH measurements above. They do not extend the
million-object result to this changed runtime or prove physical power loss.

Performance acceptance remains open. Native M1 has lower mutation latency than
Procyon on the completed HDD matrix but costs 50.7% more M1 CPU at 1,000 objects.
The first export candidate's fresh-fixture screen regresses small upload sum
by 14.1% with nearly equal M1 CPU. The controlled same-database follow-up does
not reproduce a broad export-worker slowdown: upload sum is 2.3% lower, rename
effectively unchanged and trash 1.6% higher; M1 CPU is 0.720 / 0.730 s per phase.
These 100-object phases do not settle saturation or broader performance acceptance.
Neither a functional pass nor these small populations establishes a new-core
million-object or Console result. RC.2 remains unreleased.

## M1 owned-storage qualification — November 10–11, 2026

M1 now uses its independently implemented `m1_storage` database. Its final local
checkpoint passes **374 default tests, zero failed**, with 50 explicit opt-in/helper
exclusions. Strict M1/storage Clippy and workspace formatting pass. Documentation
test commands succeed with zero runnable examples for these two crates.

The final SSD database campaign passes **1,000,000 metadata entries plus 100,000
mutations**, two actual SIGKILLs, prior-ACK checks before resume writes, full
value/history verification and exact final counts. It takes **43.08 s**, with
maximum observed database child RSS **96.75 MiB**. These are metadata KV fixtures,
not a million named object uploads. All **18 selected real M1 → N1 service
checks** pass on DATA_SSD, followed by offline verification of all 2,080,000
live records in the million-entry database.

The matched old/new/new/old 100K-entry/10K-mutation database campaign averages
**17.035 → 3.920 s (4.35× faster)**, including recovery and verification.
The matched named pipeline has mixed results: 256 MiB multipart upload averages
**0.871 → 0.884 s**, while the 100-small-PUT latency sum rises **27.31%** and
M1 process CPU rises **15.30%**. Small-operation and tail-latency acceptance
remain open; the batch gain does not establish all-operation parity. These
software controls use one SSD and **1+0** geometry, with both services on it.
The [owned-storage report](docs/M1-STORAGE-20261110.md) retains every phase,
operation's distributions, resources, failures, binary hashes and reproduction.

After the operator reboot, **final candidate 10** also passes independent
**million-entry/100K-mutation campaigns on each HDD**, two SIGKILLs per drive,
and exact final counts of 2,080,000 live records per database. It passes
**8,192 fresh named M1 → N1 uploads**, census and full original-version/byte
readback after killing and reopening both engines, plus **all 24 selected
physical service cases**. These include member loss/corruption, beyond-parity
refusal, backup/restore, replacement, cross-EC migration, durable resume,
archives, healing, GC, diagnostics, Accel, TLS and quotas. The matched HDD
100K/10K database comparison averages **85.50 → 17.98 s (4.76× faster)**.

Both USB HDDs subsequently go offline again during the named-service
comparison, and XFS shuts down both filesystems. The final failing phase uses
the **old Procyon-backed M1**; the incomplete comparison is not an accepted
speedup or a new-core database regression. After a second operator reboot,
both retained databases pass complete verification again and all 8,192
original named objects pass readback: **zero new uploads and zero mismatches**.
The shared USB hub remains present; reboot alone does not resolve its recurring
transport fault. The next fresh comparison aborts on renewed UAS errors on
both drives, at 02:15:04–05 on November 11. Its guard stops the old-baseline
phase immediately; there is no accepted HDD named-service comparison.
Hardware evidence and each successful/failed phase are retained.

A repeated SSD comparison keeps large upload/GET means within 1%, but the
new M1's small-PUT sum rises **35.79%**, with recurring approximately 95 ms
tails. Direct Rust stage profiling measures about **0.736 ms** extra across
the two durable metadata stages and does not reproduce that HTTP tail.
The subsequent [clean-HDD retest](docs/M1-CLEAN-HDD-20261111.md) archives selected
old evidence, recreates both disposable XFS filesystems with their labels,
UUIDs and mount points retained, then repeats the frozen workloads. Both
million-KV campaigns, the 8,192-object named campaign and all 24 selected service
cases pass again. Its matched database comparison averages **77.740 → 18.495 s
(4.20× faster)**. USB errors recur on both drives during the first old-M1
upload-comparison phase; the guard stops the run at 03:21:13. Formatting does
not resolve the transport fault, and no overall campaign pass or named-service
speedup follows from this aborted comparison.

Small-upload performance acceptance remains open. A new-core million-object
named campaign, Console, four-drive and controlled power-loss qualification
also remain separate gates.

The report separates each binary, fixture, failure and comparison. Historical
Procyon-backed results below remain valid for their original checkpoints;
**they do not qualify the replacement database**. N1 LINK's implementation,
native client, shared operator code, Shell and Console are unchanged.

## Million-object full-stack qualification — November 8, 2026

**PASSED:** source `8dbba3201414a7b986c0cacb41c3c525b021c274` completed
**1,000,000 fresh named uploads through M1 → native N1 LINK**, followed by
actual SIGKILL of both engines, offline census, restart and full readback of
every original acknowledged version and payload. This closes the requested
API population count of at least 500,000 objects for this checkpoint/profile.

The main PC at `192.168.254.107` used an i5-12600K and approximately 31 GiB RAM.
M1's database, binaries, control and journals were on `/mnt/DATA_SSD`; the 1+1
pool used the independent XFS HDDs at `/media/altear/HDD1TB001` and
`/media/altear/HDD1TB002`. Eight writers submitted strict durable batches of up
to 256 objects; M1 admission was four and N1 admission sixteen. Required
io_uring initialized without fallback. Request quotas and scheduled scrubs
were disabled for this functional scale profile.

| Check | Result |
| --- | ---: |
| Fresh durable ACKs / named objects / immutable versions / unique LINK identities | 1,000,000 each |
| Physical shard files | 2,000,000 |
| Original versions and complete payloads verified after restart | 1,000,000 |
| Verified logical bytes | 13,968,125,000 |
| Payload / version mismatches | 0 / 0 |
| Unreferenced / misplaced shards | 0 / 0 |
| Staged files / pending publications | 0 / 0 |
| Durable population time / rate | 21,030.620 s / 47.550 objects/s |
| Offline census / full post-restart readback | 8.476 s / 4,050.595 s |
| Maximum sampled N1 / M1 process RSS | 160.87 / 441.05 MiB |

The separate preflight passed **315 library tests**, zero failed, with five
explicit ignored cases; it was not a full-workspace rerun. A separate 4,096-object
smoke passed census and complete original-version/byte verification after
killing and reopening both engines. These checks retain their separate counts.

The 3,907 batch requests measured mean **43.009 s**, p50 **36.017 s**, p95
**88.600 s**, p99 **103.618 s**, maximum **123.906 s**. Each sample covers up
to 256 objects under concurrency, not an individual-object PUT. RSS/CPU were
sampled once per second; cache was not dropped. This campaign does not establish
a matched old/new performance comparison or all-operation latency parity.

The [million-object qualification report](docs/MILLION-OBJECTS-20261108.md)
records configuration, methodology, per-process CPU/RSS, executable hashes,
reproduction and evidence paths. Terminal oracles and raw fixtures remain on
that host under `/mnt/DATA_SSD/n1-rc2-million-20261108.ZsO460`; the campaign
completed successfully and its task processes ended.

This is a full-stack API and process-crash/reopen pass. Console population,
standalone-only million-object qualification, MCH four-HDD minimum-hardware and
physical power-loss gates remain separate. **RC.2 remains unreleased.**

## Performance acceptance for feature changes

A functional pass does not close a feature change that regresses an established
workload. Preserve the optimized baseline binary before changing code, measure
the candidate with the same durability, checksums, geometry and concurrency,
and retain each operation's latency, CPU, RSS and verified output. Report N1
alone separately from the M1-to-N1 pipeline. A gain in one operation cannot
compensate for a regression in another.

For a suspected regression, alternate binaries against the same running N1,
metadata database and physical pool; retain individual request samples and audit
LINK placement. Compare the feature enabled and disabled to isolate its cost.
Profile the affected stages, fix avoidable overhead and repeat the affected
integrity/recovery tests before calling the optimization complete. HDD variation
requires repeated controlled measurements; neither a single slower run nor a
single faster run establishes the code's cost. Do not weaken durable ACKs or
verification to satisfy a performance comparison.

The Rust opt-in
`upload_regression::alternating_small_uploads_on_one_database_and_pool` runs
twelve balanced phases: committed baseline, candidate with quotas, and candidate
without quotas. It saves raw PUT/verified GET samples, CPU/RSS and each sampled
version's data-member slot. Three warmups per phase are excluded. Its final audit
requires every phase; interrupted results cannot be promoted to complete.
`service::performance::small_upload_stage_profile` separately times native Begin,
durable M1 reservation, pending lookup, durable N1 PUT, stat and durable M1
publication. That opt-in test instrumentation is absent from shipped binaries.
Explicit private roots, executable paths and fresh report paths are required.
`upload_regression::alternating_namespace_mutations_on_one_database` isolates
rename/trash on fixed names, with byte verification after each phase. Its
RAM-only mode separates software cost from HDD waits; it is not durability
qualification.

## RC.2 preparation — November 7, 2026

The initial preparation checkpoint passes **479 default tests**, with **44 explicit
opt-in/helper exclusions**, and **81 documentation tests**. Strict M1 Clippy
and workspace formatting pass. Eight isolated two-XFS-HDD service cases pass
against the optimized M1 candidate and the same optimized N1 binary.

These cover shared HTTP/native request quotas, actual Accel search/read plus
upstream fault behavior, decoded ZIP64/TAR archives, pinned historical versions,
permissions, manifest input limits, client disconnects, SIGKILL cleanup, missing
empty backend objects, and graceful export shutdown/restart with source pins.
The existing named/native restart, monitoring/metadata-backup and size-policy
cases also pass. N1, LINK, native-client/shared-CLI/shell source and the README
remain unchanged in this preparation batch; Console was not edited.

A Rust benchmark runs baseline/candidate/candidate/baseline using an independently
rebuilt committed `main` and the optimized candidate. Each run verifies 100 ×
4 KiB objects and 256 MiB of strict durable multipart data, then measures rename
and trash. CPU, peak RSS and latency distributions are retained. Results are
mixed: candidate large-upload time averages 20.1% lower, while small-upload time
averages 14.1% higher. The initial large-GET comparison used different data-member
placements. A second ABBA replay holds the same object, version and N1 process
constant: baseline 7.3104 s versus candidate 7.3340 s, 0.32% higher, with every
payload verified. This screen did not isolate small-write cost; the controlled
follow-up below investigates it. No all-operation parity or old-NCX comparison
is inferred from this screen.

Evidence, raw results, failed attempts, exact binaries, reproduction scripts and
source preservation are under
`/mnt/HDD1TB001/n1-rc2-preparation-20261107-01`; see its
[preparation report](/mnt/HDD1TB001/n1-rc2-preparation-20261107-01/reports/PREPARATION-RESULTS.md)
and [benchmark report](/mnt/HDD1TB001/n1-rc2-preparation-20261107-01/reports/BENCHMARK-RESULTS.md).
Historical absolute `/mnt/PROJECT-DISK-1TB` links below identify their original
report locations; five are unavailable on this machine and were not reverified.
Retained artifact TTL/disk-limit implementation, complete contract/Console
coverage and the scheduled MCH scale/physical-fault/power-loss gates remain open.

### Controlled performance follow-up

The follow-up removes M1's foreground limiter sweep and per-request String
limiter keys, disables Nagle buffering on HTTP/HTTPS, and skips Accel header
preparation when unconfigured. N1, LINK, the native client, ProcyonDB's storage
core, CLI/shell, README and Console remain unchanged. Durable ACKs and integrity
verification are preserved.

The workspace checkpoint passes **481 default tests**, with **48 opt-in/helper
exclusions**, followed by **81 documentation tests**. After the final quota
rounding correction, all **42 M1 unit tests** and strict all-target Clippy pass.
Nine actual-service cases pass against the optimized checkpoint; the final
binary separately repeats HTTP/native quotas, named/native restart and verified
TLS upload/full/ranged reads. A real 60-second cleanup test verifies idle
reclamation without replenishing a busy client's quota.

Two twelve-phase HDD campaigns each retain 128 measured uploads per variant.
The first optimized candidate averages 424.067 ms versus 477.834 ms baseline;
the final-binary repeat averages 375.886 ms versus 323.833 ms baseline. Both
observations are retained: upload means reverse, so no upload speedup is claimed.
Pooling the two optimized checkpoints descriptively gives 399.976 versus
400.833 ms (0.21% lower), but candidate p95/p99 are higher. This is not a
tail-latency parity pass. Verified small-read means improve reproducibly from
43.747 to 1.571 ms. A single-member RAM-only software diagnostic averages
3.098 versus 3.125 ms per upload across 512 samples per variant, with comparable
CPU; it does not qualify physical durability.

Separate actual-call profiling puts 99.54% of elapsed upload time inside durable
N1 PUT and M1 reservation/publication calls. An fsync-only diagnostic trace finds
metadata-journal waits up to 1.503 s; it does not count io_uring operations or
serve as a throughput benchmark. A same-object 256 MiB read replay averages
7.3472 versus 7.3516 s with full SHA-256/ETag verification.

The complete methods, individual samples, slower tails, exact binaries and
failed attempts are in the
[performance investigation](/mnt/HDD1TB001/n1-upload-regression-20261107-01/reports/PERFORMANCE-RESULTS.md).
The original mixed workload is repeated with the final binary; see its
[per-operation results](/mnt/HDD1TB001/n1-upload-regression-20261107-01/reports/FULL-WORKLOAD.md).
Its mean small-upload time is 12.51% lower, mean multipart-upload time 1.85%
lower, and mean small GET 44.997 to 1.658 ms. Rename averages 13.83% more time
and some other percentiles/RSS increase; they are not hidden by a faster total.
A follow-up on identical names and one database measures rename at 1.118 versus
1.130 ms in RAM, and 67.717 versus 56.192 ms on HDD. Trash remains mixed,
including a 0.190 ms higher mean in the RAM diagnostic. Raw samples, CPU and
the interpretation are retained in the investigation report. No universal
per-operation parity claim follows.
These November 7 checks did not close saturation, current-lifecycle population,
hardware power-loss or all-operation tail-latency release gates. The subsequent
full-stack million-object population pass is recorded separately above.

## N1/M1 naming verification — November 6, 2026

The formalized `n1` / `m1` workspace passed **474 default tests**, with **37
explicit opt-in/helper cases ignored**, and **81 documentation tests**. Scoped
strict Clippy and formatting checks passed. Three explicit two-XFS-HDD service
cases additionally verified M1 → N1 operations/restart, monitoring with metadata
backup/restore, and finite/unlimited size policy with isolated secrets.

The service checks assert both signature banners, M1's HTTP/native identity,
`m1_config.ini` / `m1.env` defaults and independent N1/M1 monitoring labels.
Evidence is under `/mnt/HDD1TB001/n1-m1-naming-20261106-01/reports`.
These are functional checks of the naming/readability checkpoint, not new
throughput, million-object or hardware power-loss results. The MCH qualification
and RC.2 release hold remain in the completion checklist.

## Key-only lifecycle qualification — November 5, 2026

The current upload/purge contract uses one LINK key, existing-only continuation
and zero per-identity residue after acknowledged purge. The
[key lifecycle record](docs/KEY-PURGE-20261105.md) links its source, regression,
crash/recovery and matched benchmark evidence. Earlier results below describe
their original checkpoints, including the former permanent-retirement contract.

That upload-optimization checkpoint passed **472 default workspace tests**, with
**36 explicit opt-in/helper exclusions**, **81 documentation cases**, strict
scoped Clippy and **13 selected two-HDD service/native cases**. Five persistence
cases and eleven purge cases were also repeated on PROJECT. Matched standalone
benchmarks include Begin in PUT timing and verify complete payload hashes and
zero identity residue after purge. Their CPU/RSS, allocation and timing results,
including regressions, are under
`/mnt/PROJECT-DISK-1TB/n1-upload-optimization-20261105-01/reports`.
The operator selected allocation-only Begin; an unused key may disappear on
power loss before its first durable payload. This run does not repeat 500K or
million-object population or simulate host power loss.

## Earlier metadata maintenance qualification — November 5, 2026

The [maintenance qualification](docs/MAINTENANCE-20261105.md) covers indexed
trash TTL GC, unfinished-upload expiration, explicit integrity/healing and
verified migration between immutable pools with different EC geometry.

- **456 default workspace tests**, **81 documentation tests**, and scoped
  strict Clippy passed. The default run leaves **33 opt-in/helper cases ignored**;
  ignored cases are not passes.
- **18 explicit two-HDD service tests passed**, including all six new maintenance
  cases. The original Console drove real migration and GC against both engines;
  Rust checked the resulting payloads, versions and permanent retirements.
- The Console desktop suite passed **151 tests, 6 skipped**. The focused four
  service/maintenance cases passed across all five viewport profiles (**20
  executions**, overlapping desktop coverage). All **38 Node tests** passed.
- Metadata library tests cover claim/recovery races, shared references, pins,
  transactional placement switches and reads resolved before source retirement.
  Idle GC index checks used 10,000 future trash rows; they are a bounded-query
  regression, not a matched throughput or memory comparison with old N1.

Evidence, binaries, original failure logs and browser screenshots are under
`/mnt/PROJECT-DISK-1TB/n1-maintenance-20261104-01`. Data/control and the first
member used PROJECT; the second member used TEST. Builds stayed on PROJECT and
temporary unit fixtures used RAM. No object-engine or LINK source changed.

Reproduce the physical service run using distinct directories for control and
the first member (never the same parent), an unchanged tested object binary,
the original Console build and installed Chromium:

```sh
export N1_OBJECT_TEST_ROOT=/path/on/project/control-fixtures
export N1_OBJECT_TEST_FIRST_ROOT=/path/on/project/first-members
export N1_OBJECT_TEST_SECOND_ROOT=/path/on/test/second-members
export N1_OBJECT_TEST_BINARY=/path/to/litiaina-n1
export N1_CONSOLE_REPO=/path/to/console
export N1_CONSOLE_TEST_DIST=/path/to/built-console
export N1_BROWSER_NODE=/path/to/node
export PLAYWRIGHT_BROWSERS_PATH=/path/to/installed-browsers
cargo test --release -p m1 --test noa_service --locked -- \
  --ignored --nocapture --test-threads=1 \
  --skip running_deployment_ --skip monitoring_performance:: \
  --skip real_second_hdd_unmount_degraded_reads_and_remount_keep_every_named_ack
```

The OS-unmount case was deliberately excluded while the user's independent
deployment used the same HDD. It passed in the preceding qualification below.
This maintenance run does not rerun 500K population or certify hardware power
loss. The complete physical invocation and its terminal result are in
`reports/physical06.log`; failed attempts remain separate.

## Earlier reliability qualification — November 4, 2026

The local two-HDD campaign **full03 passed at 21:19:44 Asia/Manila**, after
fixing two metadata read-path defects and repeating the complete campaign.
The [reliability record](docs/RELIABILITY-20261104.md) gives coverage,
reproduction commands, failure evidence and explicit limits.

- **447 default workspace cases and 81 documentation cases passed.**
- **18 distinct opt-in Rust tests passed in 21 executions**, including repeated
  loss/crash cases. These are separate from the default count.
- One default test checks **410 EC loss/corruption combinations**. They are not
  410 additional Rust test functions or 410 physical disk failures.
- Physical named-service tests cover both member-loss directions, corruption,
  full metadata/staging loss and snapshot restore, in-flight process crashes,
  native independence, replica recovery after both original members are lost,
  replacement, INI expansion, GC and actual OS unmount/remount of the TEST HDD.
- Each HDD independently passed a 16,384-entry metadata campaign with 4,096
  mutations and two actual SIGKILL/resume cycles. These are KV fixtures, not
  object payload counts or a replicated metadata database.

Source/build/data evidence is under
`/mnt/PROJECT-DISK-1TB/n1-reliability-20261104-01`; its
`reports/RELIABILITY-20261104.md` and `reports/full03/PASSED` identify the final
result. Metadata/control and one object member shared the PROJECT HDD; the
other member used the independent TEST HDD. Unit fixtures and the separate
Console operator control fixture used RAM. The object executable was unchanged
between full02 and full03; both runtime fixes are in metadata.

This campaign does not rerun a 500K population, claim hardware power-loss
coverage, or close every RC.2 completion gate. Earlier scale results and failed
attempts below retain their own scope and source checkpoints.

## Reproduce workspace checks

Set `CARGO_TARGET_DIR` and `TMPDIR` to explicit task directories on storage with
adequate space before invoking these commands:

```sh
export N1_LINK_TEST_ROOT="$TMPDIR/n1-link-tests"
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo test -p m1 -p m1_storage --doc --locked
cargo clippy -p n1 -p m1 -p cli_core -p n1_shell --all-targets --no-deps --locked -- -D warnings
```

Use `--offline` only when the locked dependencies are already available.
Physical tests and subprocess helpers marked ignored require their documented
environment or supervisor; an ignored case is not a pass. Inspect the test
summary and opt-in physical witness logs separately. The scoped Clippy command
above does not claim that the historical release-proof tool passes a
whole-workspace lint invocation. Earlier Procyon-backed results retain their
original source/binary scope; they do not qualify M1's replacement database.

The source contains the following Rust physical harnesses:

- `metadata_engine/examples/hdd_qualification`: concurrent metadata KV
  population, churn, SIGKILL/resume and exact cross-table verification.
- `metadata_engine/examples/noa_qualification`: real API or original Console
  uploads through both services, frozen executable/source provenance, ACK
  journals, process crashes, physical census and exact version/payload readback.
- `metadata_engine/examples/failed_batch_audit.rs`: read-only recovery audit of
  every journaled ACK and captured successful mixed-batch row from a retained
  failed attempt, using the original executables. It never retries failed rows.
- `metadata_engine/examples/qualification_observer.rs`: read-only Linux process,
  wait-channel, I/O-pressure and device samples alongside a supervised campaign.
  It neither calls N1 nor changes service or host tuning.
- `shell/tests/operator_live.rs`: opt-in real original Console backup controls,
  dedicated HTTP operator host, native object capture/replication, exact restored
  payload and permanent retirement. Rust owns the isolated services and oracle;
  the Console's `n1-link-backups.live.mjs` only drives the browser. Requires
  `N1_OPERATOR_TEST_ROOT` (owner-only RAM/HDD parent), `N1_OBJECT_TEST_BINARY`,
  `N1_CONSOLE_REPO`, `N1_BROWSER_NODE`, built Console assets and Chromium.
  Invoke `cargo test -p n1_shell --test operator_live -- --ignored --nocapture`.
- `metadata_engine/examples/qualification_summary.rs`: read-only summary of
  existing latency/resource evidence; it cannot manufacture a campaign pass.
- `metadata_engine/examples/upload_comparison.rs`: matched native LINK,
  original NCX/NDX and current named metadata uploads with the same payload,
  strict durability, part size and concurrency. Samples CPU/RSS, process I/O
  and physical disk counters. `--mode ncx` uses the original named HTTPS API
  with its unmodified engine binary. `--access-file` selects that private
  endpoint/fragment configuration; `--session-mode standard|durable` selects
  the existing upload scheme. `--full-download` checks the complete named GET
  length and source SHA-256, outside upload timing. Without that flag, three
  64 KiB range checks are explicitly sampled verification. All expected source
  hashing occurs before timers. Set the measured server PID(s); omit
  `--metadata-pid` for the original combined engine.
- `metadata_engine/examples/upload_audit.rs`: inspect a stopped metadata copy
  without printing object capabilities. `recover_upload.rs` is an explicit
  offline operator recovery for a cancelled session whose verified native parts
  survived failed cleanup. It requires the original LINK key, unexpired upload and
  matching reservation; dry-run precedes `--apply`. It cannot recreate shards
  or revive a purged object key.
- Existing object-engine physical/recovery tests and the native client's Rust
  benchmark/resume examples retain their separate purposes.

The original Console repository remains separate. Run its Node checks, static
validation and existing `tests/live-local.spec.ts` against a real metadata
service. Do not replace production Console features or change its expectations
to obtain a pass.

The `noa_service` integration target also covers optional object-size policy.
Its resume witnesses cover strict standard and durable uploads, truncated bodies,
conflicting retries, bitmap-page boundaries, SIGKILL of both engines, repeated
finalize, exact readback, staged shard loss and cancellation cleanup. The scale
witness reserves a 15 GiB identity and transfers **1 GiB** (128 x 8 MiB parts)
to the two HDDs. It measures reconstruction, indexed status and status after
restarting both processes, asserting zero native I/O submissions for indexed
queries. This is a resume-control test, not a complete 15 GiB transfer. The
131,072-part metadata limit is separately exercised without allocating payloads.

`finite_and_unlimited_policy_on_real_metadata_and_object_engines` checks finite
rejection on known-length, chunked, batch and multipart uploads, then byte-exact
transfers with omitted/explicit unlimited caps and 15 GiB/1 TiB multipart
init/retry/status/cancel. Large initialization is admission testing, not a
claim that those payload sizes were transferred. Supply `N1_OBJECT_TEST_ROOT`
for control/database evidence and `N1_OBJECT_TEST_BINARY` for the actual object
binary; optional `N1_OBJECT_TEST_FIRST_ROOT` and `N1_OBJECT_TEST_SECOND_ROOT`
put shard members on separate physical disks. Run the ignored witness explicitly.

`running_deployment_unlimited_upload_smoke` targets an already running private
test deployment only when explicitly invoked with `--ignored`. It requires
`N1_SIZE_TEST_ACCESS_FILE` (JSON with `metadata_url`, `fragment`, `fragment_key`)
and `N1_SIZE_TEST_CA`. It uploads and verifies two small payloads, cancels its
large sessions, and moves its completed fixtures to named trash. Optional
`N1_SIZE_TEST_REPORT` records the result without credentials.

The separately ignored `running_deployment_large_multipart_round_trip` requires
the same access/CA inputs plus an explicit `N1_SIZE_TEST_BYTES` above 5 GiB.
It streams deterministic, part-indexed payloads through strict durable 32 MiB
parts, repeats finalize, and compares every downloaded byte using bounded
buffers. It retains its named fixture and records its path in the optional
report. This is the actual large-payload witness; the admission test above does
not replace it.

`interrupted_upload_listing_and_resume_survive_metadata_restart` is another
explicit physical witness in `noa_service`. It checks normal and durable
session discovery behind completed/cancelled records, truthful pagination,
status after restart, completion from missing parts and exact downloaded bytes.
The original Console's `tests/live-reload-resume.spec.ts` starts a real upload,
reloads after two acknowledged parts, finds Resume in the Uploads modal and
checks that only missing parts are sent, with no cancellation request. The
production Console runtime is unchanged by this regression test.

## Earlier monitoring and recovery checkpoint — November 4, 2026

At this monitoring checkpoint, work was on `link` and `main` was held. Authoritative
evidence is on `altear@192.168.254.107` under
`/mnt/DATA_SSD/n1-local-test-20261103-01/health-monitoring-01`. Task-owned HDD
roots are under `/media/altear/PROJECT-1TB/n1-monitoring-20261104` and
`/media/altear/TEST-1TB/mx-tests/n1-monitoring-20261104`; whole drives were not
reformatted. Workstation browser output uses RAM; builds/reports use DATA_SSD.

The complete workspace passed **440 cases** (`workspace-final13.log`), and
scoped strict Clippy passed. The ordinary run leaves opt-in physical/helper
cases ignored; these are not counted as passes. Separate completed witnesses
include two-HDD service monitoring/outage/metadata restore, matched transfers,
matching named metadata/object recovery, process-crash/retirement/member
recovery, actual INI pool expansion, and explicit io_uring with no fallback.
Their raw files are `physical-final.log`, `combined-recovery-final.log`,
`object-chaos-final.log`, `ini-physical-final.log` and `uring-final.log`.

Two physical machines also passed live Console service monitoring and object
outage tests; matching hostnames were distinguished using kernel boot IDs.
Native encrypted backup transfer crossed those nodes and the destination
verified the point. A replacement on a different device preserved the logical
member identity; an opt-in scrub repaired deliberate corruption with read
repair disabled, followed by exact 8,388,627-byte GET verification. Terminal
JSON files name each scope explicitly.

The full desktop Console suite passed **150 cases**, with six opt-in cases
skipped. Monitoring/backups passed **30 checks across all five viewport
profiles**; these overlap the desktop suite rather than adding 30 new cases.
All **38 Node cases** and static validation passed. The separate Rust
`operator_live` witness drives the actual Console through its dedicated Shell
host: capture, verify, native replication, restore, exact payload and retirement,
plus truthful failures for a running offline source and the last recovery point.
Its RAM control fixture is distinct from the physical backup/replacement tests.

The eight-writer full-stack attempts **failed** on 30-second object write
admission while strict filesystem flushes held all six per-fragment slots.
Read-only audits verified every **52,480** and **56,832** respective journaled
ACKs plus each retained mixed response's **192** successful rows, with exact
versions/bytes after restart. Neither failed population is a 500K pass.
The separate default four-writer 500K API campaign also **failed**, on its
300-second HTTP request budget. A read-only restart audit verified all
**101,120 journaled ACKs / 1,412,456,800 bytes**, with zero payload/version
mismatches. No complete response was captured for the timed-out batch, so its
unacknowledged outcomes are not guessed. The gated Console phase never started.
Eight readers perform post-restart readback, not simultaneous fresh population.

The Rust client's original 30-second TCP user timeout was also diagnosed and
aligned with its existing 300-second request budget. A fresh 4,096-object
strict/restart/full-read smoke passed, but the later eight-writer server
admission failure remains recorded. An experimental per-file persistence path
was rejected after its HDD stall and the original persistence code restored.
A subsequent bounded per-file candidate also failed its 4,096-object smoke
on admission and was rejected and reverted. A read-only restart audit checked
all 128 captured successful rows / 1,787,920 bytes; there were no complete
journaled batches in that attempt. Neither experiment is deployed.
The subsequent gated profile used the original batch persistence with database,
control and ACK journals on DATA_SSD and one member on each HDD. It explicitly
sets metadata admission to four while retaining eight upload clients, the
object engine's independent admission policy and all strict barriers/deadlines.
This is a different placement/admission profile, not a retroactive pass for
the failed HDD-database campaigns. Its 4,096-object smoke passed: 8,192 shards,
no unexpected staging/pending/unreferenced files and every original version and
all 57,213,440 bytes verified after both engines restarted. At that checkpoint,
API 500K was active and Console 500K was gated on its complete pass. This
checkpoint does not establish their later terminal outcomes.
`qualification-ssd21.sh` owns the sequential headless runs;
`qualification-evidence21.sh` retains resource summaries and separately audits
ACKs if the API run fails. Both temporary user units use `Restart=no`.
No server deadline or durability policy was weakened. The full current report
is `WHOLE-N1-MONITORING-RECOVERY-20261104.md` in the remote evidence directory.

## Recorded evidence as of November 3, 2026

The earlier full-stack campaigns below use checkpoint `9939ec8` on `link`. Source/executable hashes and
exact environments in each report identify the tested snapshot. Counts from
different runs are not added together as new distinct tests.

The later local size-policy and credential-provisioning checks on
`192.168.254.107` passed: 421 workspace cases, 81 storage documentation cases,
both explicit metadata-to-object HDD witnesses, and all three original Console
live cases. A separate real **15 GiB** strict durable upload used 480 parts and
passed repeated finalize and complete byte-for-byte download verification.
The **1 TiB** checks covered initialization, retry, status and cancellation.
Custom secrets files, environment-only startup and rejection of insecure files
also passed. Raw evidence, binaries and the current operator paths are under
`/mnt/DATA_SSD/n1-local-test-20261103-01/reports/UNLIMITED-UPLOADS-20261103.md`
on that host. This does not change the completion status of the earlier
population campaigns.

| Campaign | Recorded result | Scope and evidence |
| --- | --- | --- |
| Complete local workspace | 414 passed, zero failed; nine explicit helper/physical exclusions | [Full-stack report](/mnt/PROJECT-DISK-1TB/n1-ini-restoration-20261103-01/reports/FULL-STACK-QUALIFICATION-20261103.md) |
| Post-fix metadata/native checks | 14 metadata library and three native-client cases passed; strict metadata/shared-config Clippy passed | Same report; `checkpoint-metadata104.log`, `checkpoint-object-client105.log`, `checkpoint-clippy103.log` |
| Original Console regression | Three live cases and 38 Node cases passed; static validation had zero errors/warnings | Same full-stack report; existing production Console/runtime/proxy preserved |
| Corrected local two-HDD smoke | API 1,024 objects and original Console 1,000 objects passed census and every post-SIGKILL payload/version read | Same report, separate fresh datasets |
| MCH component regression and physical API | 246 passed, zero failed; eight explicit helper/physical exclusions; physical API witness and 1,024-object API smoke passed | [MCH full-stack report](/mnt/PROJECT-DISK-1TB/n1-mch-full-stack-20261103-01/reports/MCH-FULL-STACK-QUALIFICATION-20261103.md) |
| Direct LINK 500,000-object campaign | Passed 500,000 durable ACKs, all exact full GETs after restart and a 1,000,000-shard census | [Object-only 500K report](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/HEADLESS-500K-20261102-20261103.md); a separate earlier object-only snapshot |
| Procyon metadata foundation on MCH HDDs | One million KV entries, 100K mutations, concurrent snapshots and two SIGKILL/resume cycles passed; other drives passed independent smaller campaigns | [Metadata KV report](/mnt/PROJECT-DISK-1TB/n1-mch-metadata-20261103-01/reports/MCH-METADATA-QUALIFICATION-20261103.md); not object uploads |
| API and Console 500,000-object campaigns at that checkpoint | In progress at the source checkpoint; no full-stack 500K pass recorded in that snapshot | [Headless handover](/mnt/PROJECT-DISK-1TB/n1-mch-full-stack-20261103-01/reports/HEADLESS-HANDOVER-20261103.md) and the terminal files it identifies |

Headless terminal JSON files and raw logs are authoritative for completion after
this status snapshot. A populated dataset or upload progress line is not a
terminal pass. The Console population follows only after its API campaign passes
census and complete post-restart reads. Local and MCH campaigns are independent.

## Failure retained and corrected

An earlier named API batch returned 128 successes and 128 backend errors, so
that campaign failed. Metadata had retained pooled native sockets beyond the
independent object's idle timeout. The metadata pool now discards aged idle
transports before reuse; a deterministic regression checks replacement/login.
The corrected physical API and smaller end-to-end smokes passed afterward.

The separate Rust audit reopened the exact failed snapshot/binaries and checked
all 128 captured successful payloads and original version IDs without PUT/retry:
1,787,920 verified bytes, zero payload/version mismatches. The original mixed
batch remains a failure. Raw failures and source/binary hashes remain in the
full-stack report; they are not relabelled as scale passes.

## Benchmark and release limits

The [aligned direct-I/O qualification](docs/DIRECT-IO-20261105.md) records the
current large-upload writer, physical page-cache proof, repeated fault/recovery
checks and matched buffered-LINK/NCX comparisons. It includes per-phase CPU/RSS,
latency, allocation and adverse measurements. That November 5 campaign used
`/mnt/HDD1TB001` and `/mnt/HDD1TB002`, with source/build/data evidence on HDDs.
The November 8 main-PC campaign uses DATA_SSD for M1/control/builds and the HDDs
at `/media/altear/HDD1TB001` and `/media/altear/HDD1TB002`, as recorded in its
[report](docs/MILLION-OBJECTS-20261108.md).

Compare the same workload, integrity/durability policy, hardware, concurrency,
cache state and completed operation semantics. Report upload/download latency,
throughput, CPU and sampled RSS separately. A metadata KV batch latency is not a
per-object upload latency; CDP heap is not total browser RSS; a process SIGKILL
is not a physical power cut. Physical LINK purge is not the same operation
as moving a named object to trash.

The direct old/new comparison is retained in
[NCX-VS-CRC32C-LINK-20261103.md](/mnt/HDD1TB001/n1-xfs-rebenchmark-20261105-01/evidence-before-format/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/NCX-VS-CRC32C-LINK-20261103.md).
It does not prove a win in every metric or the performance of the later metadata
stack. Full product NOA conformance and the remaining fault/topology/recovery
gates must pass before RC.2 is released.

Earlier chronological test notes are preserved at `9939ec8:TESTING.md` in Git
and in their linked HDD reports. They describe their recorded snapshots, not the
current architecture or an accumulated claim that every gate has passed.
