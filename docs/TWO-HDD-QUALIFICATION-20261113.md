# Two-HDD and SSD RC.2 qualification — November 12–13, 2026

**The original owned-M1 million-object campaign is complete. RC.2 remains
unreleased.** The final natural-expiry GC and metadata/physical censuses passed
on November 12, using its frozen executables and original dataset. Qualification
of the newer finalization runtime is recorded separately below. the four-HDD reference system was not
contacted for these SSD/HDD checks; its next hardware campaign remains deferred.

## Original campaign closure

The [split-node campaign](SPLIT-NODE-20261111.md) already passed one million
named uploads, full original payload/version readback after killing both
services, and a complete readback with each member unavailable in turn. The
last member-loss pass verified 13,968,125,000 bytes with no payload/version
mismatch. Those completed populations/readbacks were not repeated for closure.

The remaining 192 unacknowledged crash-era upload attempts expired naturally on
November 12 at 21:49:23–21:49:32 Asia/Manila. The final controller waited until
21:52:32, without changing the clock, expiries or cleanup policy. It verified
the original SSD test system database's four stopped files against the preserved
checkpoint before reopening them. The exact original SSD/HDD N1/M1 executables
then performed normal automatic GC with both original members present.

| Final witness | Result |
| --- | ---: |
| Expired attempts scanned / reclaimed | 192 / 192 |
| GC failures / deferred work | 0 / 0 |
| Named objects / immutable versions / unique LINK identities | 1,000,000 / 1,000,000 / 1,000,000 |
| Pending publications | 0 |
| Physical shards on two independent filesystems | 2,000,000 |
| Staged / unreferenced / misplaced shards | 0 / 0 / 0 |
| Final offline database integrity | 7,000,272 records verified |
| Original ACK journals | SHA-256 unchanged |

Both services stopped before the final offline checks. Metadata census took
5.680 seconds; the two-million-file physical census took 2,513.263 seconds.
That census checked references/placement, not payloads again. Its reference-list
SHA-256 is `13e3a99b5b39279218df913f9d34e668a409a02b757e7b89625aeaf280b41a69`.
The final database reports 4,755,931,220 disk bytes. Counts differ from the
earlier 7,001,423-record integrity check because GC removed expired lifecycle
state; the original acknowledged names, versions and payloads remain unchanged.

The evidence root is `/mnt/test-hdd-a/n1-rc2-ssd-hdd-qualification-20261112-02`.
Its `reports/` contains `original-gc-completed.json`,
`original-final-metadata.json`, `original-final-physical.json`,
`original-final-integrity.json`, timing, original binary hashes and
`ORIGINAL-FINAL02-EXIT=0`. `ORIGINAL-CAMPAIGN-COMPLETED` also exists in the
original continuation root. Corresponding SSD test system evidence is under
`/mnt/test-ssd/n1-rc2-ssd-hdd-qualification-20261112-02`; frozen final-stage
scripts are under `source/`. The portable checkpoint in
`/srv/test-evidence/portable` remains unchanged. Earlier stopped/failed launch
reports are retained.

This closes the original frozen-checkpoint campaign. It does not transfer a
million-object qualification to subsequently changed N1/M1 executables or
establish physical power-loss resilience.

## New M1 completion-recovery regression

The real `resume_progress` service test initially failed against the new N1
finalization runtime: after both services were killed partway through an upload,
M1 retained its durable part ACKs but N1 had lost its partial in-memory
completion prefix. Completing the missing parts then returned the explicit
409 reconciliation precondition. The failure is retained in
`remote-reports/resume-progress01.log`; it is an M1/N1 integration regression,
not a failed original million-object result.

The fix is isolated to M1's native object adapter. Healthy finalization still
sends exactly one command. Only the exact lost-completion 409 permits paginated
status reconciliation and one finalization retry on the same admitted,
authenticated connection. Missing/unavailable parts, malformed pages, purged
keys and unrelated conflicts cannot publish success. Cancellation discards the
connection; shutdown drains accepted recovery. No bytes are replayed, no new
identity is allocated and no N1 runtime, shard format, database format or
Console implementation is changed.

Status pages contain at most 4,096 parts and target at most 64 MiB of declared
payload, with at least one part per request. A single larger configured part
still requires one request. Ordinary indexed M1 resume status continues to read
metadata without submitting N1 payload I/O. Exceptional cold completion
reconciliation can read existing payload frames across the upload; it is not
an O(1) recovery claim. N1's finalization remains O(pool width).

## Current-candidate verification

| Completed check | Result |
| --- | --- |
| Complete workspace, all targets, locked/offline | **401 passed, 0 failed**, 57 explicit opt-in/helper exclusions after adding the read-replay benchmark, 50 target summaries |
| Native object-adapter transport tests | **16 passed**, including five new completion/recovery cases; included in the 401 above |
| Strict workspace all-target Clippy, workspace formatting | Passed; new read-replay fixture also passed focused strict Clippy |
| test-ssd real resume | **4 passed**: both-process crash, lost staging, interrupted listing and 1 GiB reconstruction/index recovery |
| test-ssd real API/Console | **15 passed**: permissions/quotas, native/TLS, size policy, Accel, archives, retained-export lifecycle, expired uploads, upload-versus-scrub classification and original Console operator workflows |
| Two independent HDD members, real API | **10 passed**: member loss/corruption, full metadata restore, six in-flight crash cases, indexed GC, scoped healing, cross-EC migration/restart/cancel, monitoring/snapshots, encrypted backup/replication/replacement and total payload-loss refusal |
| Frozen original Console source/build | Production build and **45 Node tests passed**; current-candidate real browser operator/backup workflow and four original live upload cases passed |
| Monitoring benchmarks | Quiet-node zero-payload-I/O/CPU/RSS witness, six matched transfers and the same-object warm/file-cache-advised replays passed; measurements and limits below |

These are **29 distinct opt-in service cases** in addition to the default
workspace checks, not 29 separate populations. Faults target only newly created
fixtures. The original million-object corpus is untouched by current-candidate
tests. The original service harness contains 41 opt-in cases; the new isolated
read-replay benchmark adds one. This table does not claim all cases, full NOA
conformance or physical power-loss coverage.

test-ssd tests use SSD test system M1/database/control and N1 1+0 fixtures on
the same SATA SSD, with native loopback. The two-HDD checks run on the two-HDD
test system with N1 1+1 and M1/control on test-hdd-a; each N1 member occupies a
distinct XFS filesystem. This latter profile tests functional co-location, not a
measured split-node SSD deployment. Original split-node/million evidence remains
separate. No drive was reformatted or physically disconnected in these checks.

Current binaries, compiled on test-ssd with Rust 1.99.0 and the unchanged
optimized workspace profile:

| Executable | SHA-256 |
| --- | --- |
| N1, unchanged from finalization candidate13 | `e69ecd5e97030f3cc67960d7a69781d3bd68c3bf65c94012bc35db07b3f8b4b7` |
| M1, final byte-bounded recovery | `caa8eb2ec3319801fbf8e1870b5c14e166ed196b68555f4a787780d15aa49474` |
| Real-service Rust harness | `aef4adee1ad42e24d4e92c951ea97a2545a81162504646540bbc665c6a96eadf` |

`SCREEN03-*`, `SERVICE05-*` and `PHYSICAL04-*` record these completed checks.
`FINAL03-*` records completed benchmarks followed by an Accel fixture failure:
its launcher omitted the explicit Accel executable. That failure remains
retained; `SERVICE05-*` supplies the existing executable and passes the test
without an engine change. The earlier actual cold-finalization regression is
retained separately in `resume-progress01.log`.

## Matched healthy-path performance

This isolates the M1 adapter change against M1 built from exact committed
`2fcfeeabc72668d6e0c9dd3d4076323e4f2ec243`, using the same compiler/profile,
running N1, database, configuration and test-ssd 1+0 pool. It is **not** an
N1-Lite-versus-LINK or old-Procyon-versus-owned-database comparison. The prior
[standalone finalization benchmarks](FINALIZATION-20261112.md) remain the N1
runtime evidence; that N1 executable did not change here.
The matched baseline M1 executable SHA-256 is
`7b4ad8f7d0644f5da79646df36cf52d75c80bd76be067ba0156bebfa7c7e02d3`.

Each run alternates 12 phases, **A B C C B A A B C C B A**: A is baseline,
B is candidate with quotas enabled, C is candidate with quotas disabled. Each
phase measures 128 requests after the harness's explicit warmups. Normal policy
limits are raised equally for A/B to avoid quota rejection; overload acceptance
is separate. All versions/GET bytes and small-upload placement are verified.
There are **512 measured samples per operation/variant/run**. Percentiles below
use nearest rank over individual concatenated samples; phase p99s are not averaged.
No cache is dropped. No compilation overlaps these timers.

All latency values are milliseconds. Run 02 tests the first adapter fix; run 03
tests the final byte-bounded revision. Both runs remain primary evidence.

| Run / operation | Baseline mean / p50 / p99 | Candidate mean / p50 / p99 |
| --- | ---: | ---: |
| 02 strict 4 KiB upload | 14.271 / 12.763 / 94.429 | 13.110 / 12.946 / 22.895 |
| 02 verified GET | 1.051 / 1.179 / 2.737 | 0.895 / 0.765 / 2.407 |
| 02 rename | 2.280 / 2.126 / 5.239 | **3.907 / 2.191 / 24.335** |
| 02 trash | 2.505 / 2.409 / 4.876 | 2.524 / 2.421 / 4.988 |
| 03 strict 4 KiB upload | 12.543 / 11.597 / 33.290 | 11.550 / 11.205 / 14.519 |
| 03 verified GET | 1.210 / 1.238 / 1.787 | 1.192 / 1.206 / 1.667 |
| 03 rename | 5.361 / 2.056 / 24.534 | 2.120 / 2.047 / 2.620 |
| 03 trash | 2.315 / 2.283 / 3.260 | 2.314 / 2.182 / 3.241 |

The first run has an adverse rename result. In the repeat, similar stalls occur
in baseline phases instead, with both candidates' medians close to baseline.
This does not establish a reproducible adapter-induced rename slowdown, nor a
universal rename speedup. The root cause of those phase stalls is unproven.
Final-candidate healthy upload mean is **7.92% lower** in run 03; this small
sequential SSD screen does not settle saturation or the four-HDD reference
system tail latency.

| Measured process resource | Run 02 baseline / candidate | Run 03 baseline / candidate |
| --- | ---: | ---: |
| M1 CPU over 512 upload requests | 1.40 / 1.49 s | 1.34 / 1.36 s |
| N1 CPU over those upload requests | 2.12 / 2.24 s | 1.94 / 1.91 s |
| Maximum M1 upload-phase RSS | 14,092 / 14,124 KiB | 14,060 / 14,036 KiB |
| M1 CPU over rename/trash/recover workload | 3.86 / 4.22 s | 3.43 / 3.63 s |
| Maximum M1 mutation-phase RSS | 14,580 / 15,400 KiB | 16,092 / 15,328 KiB |

CPU comes from process counters with 10 ms tick granularity, not a whole-host
percentage. Mutation CPU includes untimed recovery calls and warmups. Peak RSS
uses per-process high-water observations, excluding filesystem page cache.
The CPU increases remain adverse observations; this screen does not justify
claiming unchanged CPU or close broader performance acceptance.

Raw `matched-upload02/03.json`, `matched-mutations02/03.json`, each phase log,
summaries and `source/summarize-m1.jq` retain every measured request and p95/max
alongside the table above. A/B/C are all retained, including the disabled-quota
results; C is not substituted for the enabled production-policy comparison.

## Monitoring qualification

The final-candidate quiet-node witness passes six five-second phases in
**A B B A A B** order after 1,024 actual 128-byte named uploads. Four clients
poll at 100 ms intervals. Each active phase completes 197 monitoring requests;
**all six phases submit zero object payload I/O**. Active-phase M1 CPU is
6.45–6.83% of one core, versus about 0.39% while idle. N1 stays at 0–0.40% of one
core. Monitoring request p99 is 3.59–4.40 ms. Sampled M1 RSS rises from 31.9 to
33.8 MiB and stabilizes; N1 stays at 35.1 MiB. These are per-process samples,
not total host/page-cache memory or sustained production-load limits.

The two-HDD transfer witness also passes full SHA-256 checks for six 256 MiB
objects, alternating monitoring **A B B A A B**, 32 durable 8 MiB parts each.
It retains **622 / 912 / 634 monitoring requests** in the active phases.
Uploads vary substantially (9.95–24.69 MiB/s across all phases). Immediate GETs
are slower in active phases: 73.90–89.56 MiB/s versus 108.72–175.82 MiB/s in quiet
phases. The test uses a different newly uploaded object per phase, uncontrolled
page cache and co-located M1 on HDD1; it does not isolate the cause. This adverse
observation is retained in `reports/monitoring06-transfers.json` and is not
described as zero monitoring overhead. `monitoring07-quiet.json` separately
records the no-payload-I/O and CPU/RSS results on test-ssd.

The follow-up Rust replay holds the same immutable object/version, N1/M1
processes and placement constant. It performs one complete warmup GET, then
eight SHA-256-verified 256 MiB GETs in **A B B A B A A B** order, with four
100 ms monitoring clients in B. It never uploads or changes placement during
those timers. Aggregate warm-read throughput is **828.69 MiB/s quiet versus
830.24 MiB/s monitored**; all eight versions and complete byte streams match.
Across four reads per variant, M1 CPU is **0.96 / 1.12 s** and N1 CPU
**1.43 / 1.43 s**; the active phases issue 68 monitoring requests. These short
RAM/page-cache-resident reads do not reproduce a throughput loss, but do show
M1 CPU overhead and do not settle the prior post-write/cold-read observation.
Raw phases and request latencies are in `monitoring09-read-replay.json`.

The replay is isolated under
`metadata_engine/tests/noa_service/monitoring_performance/read_replay.rs`.
It also supports `N1_MONITOR_READ_EVICT_FILE_CACHE=1`, advising only its new
fixture's two immutable shard files with `POSIX_FADV_DONTNEED` before each
phase. This changes no runtime policy, drops no global caches and cannot
claim to evict device/controller caches. The ordinary warm profile remains
separately retained. The first fixture-only lint attempt rejected deprecated
digest API usage; the corrected strict build is retained as `READ09-*` and
the cache-advice addition as `READ11-*`. Neither changed runtime binaries.

The file-cache-advised profile also passes all eight full reads, advising
exactly two shard files before each phase. Throughput is **111.10 MiB/s quiet
versus 119.04 MiB/s monitored**, across four reads per variant. M1 CPU is
**1.89 / 2.20 s**, N1 CPU **3.75 / 3.78 s**, with 354 monitoring requests in
the active phases. Raw quiet rates are 97.83–122.70 MiB/s and active rates
116.14–121.30 MiB/s. No global cache drop or controller-cache eviction is
claimed. `monitoring11-evicted-read-replay.json` retains every phase/request.
The warm replay executable SHA-256 is
`697f0b05478bae79606660901e23c4d2e3f3747dbdf19b2d9a6092e3ba94e74d`;
the final cache-advice-capable harness is
`5af6c07ea54f455be499d285e041e2dfc4580c5c390d6cdd6d70d85af83df599`.

Neither controlled profile reproduces the initial throughput loss. The initial
mixed upload/read observation remains retained, without attributing its cause to
a verified implementation defect. Monitoring has measurable M1 CPU cost; these
small balanced screens establish neither zero overhead nor sustained
tail-latency acceptance on the four-HDD reference system's minimum hardware.

## Remaining release scope

The SSD/HDD checks above are complete, with task-owned services stopped and
evidence retained. the four-HDD reference system's current-candidate
four-HDD/minimum-hardware and sustained performance qualification remain
deferred by the operator. Physical power-loss qualification remains separate
from the completed SIGKILL and member-directory faults. Complete
contract/Console acceptance and current-runtime scale gates remain tracked in
[RC2-IMPLEMENTATION.md](RC2-IMPLEMENTATION.md); no redundant million-object
population is running or queued.

Final provenance checks match **353 Rust/manifests/lock files** between this
worktree and the compiled test-ssd source. N1/LINK/native-client source is
unchanged from committed `2fcfeea`; the N1 artifact hash also matches measured
candidate13. New runtime work is restricted to M1's native adapter. All frozen
source overlays, launchers, raw logs, benchmark summaries and executable hashes
remain in the task evidence root. The final workspace marker is
`WORKSPACE10-EXIT=0`; the later fixture-only cache-advice addition passes strict
service-test lint, optimized compilation and its complete physical replay.

All builds, fixtures and reports use the selected HDDs or SSD test system
test-ssd. Workloads and faults are Rust; the original Console uses
Node/Playwright and Chromium. No Python, new million-object population, the
four-HDD reference system access, commit, push, tag or release is performed by
this qualification.
