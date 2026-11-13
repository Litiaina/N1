# MCH optimization and RC.2 continuation — November 13, 2026

**This continuation's regression and measurement runs are complete. RC.2 is
unreleased; performance acceptance remains open.** The standalone comparisons,
fixed-object read replay and four-HDD fault checks pass integrity verification.
The current candidate passes **411 default workspace tests**, **36 selected
public-service/Console checks** and strict workspace Clippy. Higher HDD CPU time
and some small-object tails remain explicit concerns. This run resumes the
[portable optimization checkpoint](OPTIMIZATION-CHECKPOINT-20261113.md) and
then continues the remaining qualification gates. Completed earlier million
populations, readbacks and censuses remain their original evidence.

The subsequent [home standalone continuation](HOME-OPTIMIZATION-20261113.md)
removes further upload copies and records a new compiler-matched comparison.
Its source/executable identity and hardware profiles are separate from this
completed MCH run; its results do not replace the measurements below.

The baseline throughout this report is commit `3d6e1a3`. The **initial
sparse-only candidate** and the later **receive02 candidate** have separate
artifacts and matrices. Allocation-window and larger-buffer experiments were
rejected and removed. Their measurements remain below; they do not describe
the current working implementation.

## Isolation and provenance

The host is `itserver`, `administrator@192.168.0.12`, with an Intel i5-11400,
six cores/twelve threads and approximately 11 GiB RAM. The four independent
HDDs are mounted as XFS at `/mnt/DISK0` through `/mnt/DISK3`. The system NVMe is
a Kingston SNV2S500G with an ext4 filesystem. Existing deployed services and
their data are not test targets. No global toolchain, CPU policy, kernel setting
or installed package is changed.

Task source, dependencies, reports and binaries live under
`/mnt/DISK0/n1-optimization-20261113-01`. Its verified input capsule is the
sibling `n1-optimization-20261113-01-capsule` directory. Builds reuse the prior
task's HDD target directory, but use this run's isolated Cargo home and vendored
dependencies. There is no dependency on the main PC or DATA_SSD.

The source checkpoint is committed `3d6e1a3` plus the three-file sparse-upload
optimization and its interrupted-part regression test. Both N1 variants are
built natively with **Rust/Cargo 1.92.0**, the same lockfile and ordinary release
profile. The baseline is reconstructed by reversing the preserved patch in a
separate source copy. The candidate's three source hashes match the capsule.
The home-PC executables are retained as evidence, not used as MCH measurements.

An artifact audit caught Cargo reusing the baseline for the first candidate
because the preserved candidate source had older modification times in the
shared target. That candidate is marked `invalid-cached-candidate01` and no
benchmark used it. The three candidate files were touched without modifying
their content, checked against their SHA-256 manifest, and rebuilt. The first
driver also used the former metadata package name; the corrected driver selects
the current `m1` crate. Both attempts' logs remain retained. Neither is an engine
test failure or a performance result.

## Initial sparse-only standalone comparison

The completed matrix holds each pool's identities, paths, credentials, TLS,
runtime/admission settings and client constant while switching N1 binaries in
A/B/B/A order. It uses ordinary CPU affinity and keeps builds, fault checks and
other task workloads outside the timing windows.

| Profile | Placement | Workload |
| --- | --- | --- |
| NVMe 1+0 | New private member under `/home/administrator/n1-optimization-20261113-01` | 1 GiB and 15 GiB durable multipart; small-object standard/durable latency |
| HDD 1+1 | New private members on DISK1 and DISK2 | 1 GiB durable multipart; small-object standard/durable latency |
| HDD 3+1 | New private member on each of DISK0–3 | 1 GiB and 15 GiB durable multipart; small-object standard/durable latency |

Large uploads use the retained Rust benchmark: 48 MiB parts, four concurrent
writers, 256 KiB EC blocks, full SHA-256/length/geometry verification, repeated
finalization and verified zero-residue purge. No payload fixture file or M1
forwarding is involved. Each 1 GiB variant/profile has four objects; each
15 GiB variant/profile has two. Small-object blocks contain 64 independent
4 KiB objects per mode; each variant/profile has 128 samples per mode. Report
nearest-rank p50/p95/p99 over individual samples, never averaged block quantiles.
Large-object samples are too few for a meaningful object-level p99.

CPU, RSS, physical I/O counters, part-request latency and complete operation
timings are retained. Immediate post-upload GETs retain their uncontrolled
cache condition; the separate Rust read-replay diagnostic compares the exact
same immutable identity across binary switches and optional fixture-file cache
advice. No global cache drop or device-cache eviction is claimed. Profiled runs
are diagnostic, not substitutes for unprofiled acceptance.

The initial standalone benchmark client is frozen before any read-replay
enhancement. Those later harness changes do not alter N1 or M1 runtime behavior.
The original matrix and later diagnostic client have separate recorded hashes.

## Completed initial sparse-only measurements

There are **32 matched large uploads** and **1,536 small-object lifecycle
samples**, all fully verified, finalized again idempotently and purged with a
404 check. Upload includes init, the client's durable identity journal, parts
and finalize. Tables use total bytes divided by total wall time; CPU means
N1 process CPU seconds per object, excluding GET and the client. All samples,
including slower ones, are retained.

| Profile / bytes per object | Objects per binary | Current upload MiB/s | Optimized MiB/s | Current CPU s | Optimized CPU s | Current immediate GET MiB/s | Optimized immediate GET MiB/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NVMe 1+0 / 1 GiB | 4 | 1,278.34 | 1,692.43 | 2.890 | 1.828 | 955.01 | 982.88 |
| NVMe 1+0 / 15 GiB | 2 | 1,383.06 | 1,898.53 | 43.795 | 26.490 | 962.17 | 977.08 |
| HDD 1+1 / 1 GiB | 4 | 123.87 | 122.13 | 7.073 | 5.663 | 63.85 | 63.63 |
| HDD 3+1 / 1 GiB | 4 | 118.54 | 121.44 | 6.143 | 5.560 | 170.75 | 140.29 |
| HDD 3+1 / 15 GiB | 2 | 121.51 | 134.14 | 90.500 | 77.235 | 142.46 | 145.85 |

At 15 GiB the measured NVMe upload gain is **37.27%**, with **39.51% less N1
CPU time**; HDD 3+1 improves **10.39%**, with **14.66% less CPU time**. The
1 GiB HDD 1+1 upload is **1.41% slower**. The 1 GiB HDD 3+1 immediate readback
is **17.84% slower** and is not omitted. Different object keys rotate which of
the four heterogeneous drives supplies each shard; the unchanged reader also
sees different post-write cache state. The fixed-object results below isolate
those confounders rather than relabeling this result as an improvement.

Mean finalize latency, current → optimized, is 2.08 → 2.27 ms (1 GiB NVMe),
2.28 → 3.12 ms (15 GiB NVMe), 19.70 → 22.16 ms (1 GiB HDD 1+1),
66.45 → 75.23 ms (1 GiB HDD 3+1) and 74.73 → 80.89 ms (15 GiB HDD 3+1).
Publication code is unchanged by this optimization. These few-sample physical
latencies do not establish tail equivalence; additional balanced HDD confirmation
is tracked separately. No payload scan has been added to finalize.

The following are request latencies in milliseconds. There are 88 part samples
per 1 GiB row and 640 per 15 GiB row. Four concurrent writers share the device;
part latency is not sequential disk service time. RSS is the maximum sampled
N1 resident set during upload, in KiB; filesystem page cache is outside RSS.

| Profile / object / binary | Mean init ms | Part p50 / p95 / p99 ms | Peak RSS KiB |
| --- | ---: | ---: | ---: |
| NVMe / 1 GiB / current | 0.685 | 137.80 / 163.82 / 240.73 | 84,296 |
| NVMe / 1 GiB / optimized | 0.352 | 88.63 / 139.62 / 187.27 | 81,800 |
| NVMe / 15 GiB / current | 0.763 | 135.75 / 154.11 / 167.12 | 75,708 |
| NVMe / 15 GiB / optimized | 2.160 | 90.61 / 120.82 / 142.10 | 93,008 |
| HDD 1+1 / 1 GiB / current | 0.852 | 1409.96 / 1876.77 / 1932.07 | 119,976 |
| HDD 1+1 / 1 GiB / optimized | 13.397 | 1463.04 / 1858.84 / 1984.27 | 115,964 |
| HDD 3+1 / 1 GiB / current | 85.197 | 1448.78 / 2147.55 / 2159.31 | 259,380 |
| HDD 3+1 / 1 GiB / optimized | 0.905 | 1469.01 / 1902.97 / 2047.48 | 203,132 |
| HDD 3+1 / 15 GiB / current | 0.543 | 1575.77 / 2096.75 / 2345.52 | 307,664 |
| HDD 3+1 / 15 GiB / optimized | 0.477 | 1400.40 / 1700.66 / 1915.91 | 276,844 |

Init and RSS do not improve in every row. The optimized 15 GiB NVMe peak is
higher than current; neither row implies memory grows with object length.
All large samples and 50 ms sampling observations remain available in the raw
reports rather than being replaced by a universal low-memory claim.

Small-object figures are milliseconds for init + part + finalize, with 128
individual **4 KiB** samples per binary/mode/profile. They measure the multipart
lifecycle, not one-shot/batch throughput. Nearest-rank observed p99 is not a
rare-event service guarantee.

| Profile / mode | Current p50 / p95 / p99 ms | Optimized p50 / p95 / p99 ms |
| --- | ---: | ---: |
| NVMe / standard | 2.756 / 2.964 / 9.702 | 2.751 / 3.141 / 6.939 |
| NVMe / durable | 3.521 / 3.852 / 10.093 | 3.522 / 3.790 / 8.909 |
| HDD 1+1 / standard | 73.349 / 91.365 / 97.498 | 66.871 / 90.304 / 105.650 |
| HDD 1+1 / durable | 132.203 / 173.064 / 191.212 | 130.792 / 166.221 / 171.855 |
| HDD 3+1 / standard | 141.395 / 199.138 / 307.037 | 141.391 / 184.971 / 320.796 |
| HDD 3+1 / durable | 232.985 / 302.222 / 477.621 | 232.400 / 308.228 / 533.597 |

## Current receive02 comparison

The allocation-window and larger-buffer experiments described later in this
report are removed. The current candidate retains the original write allocation,
encoder grouping, admission budgets and concurrency. It skips old-frame reads
only within a proven sparse interval and batches native transport fragments into
the existing 128 KiB handoff buffer. Checksums, immutable retry comparison,
O_DIRECT, EC, finalization and persistence barriers are unchanged.

The fresh `receive-matrix01` comparison completes all **32 phases**, with
**32 large uploads and 1,536 small-object lifecycle samples**. Every payload
passes complete SHA-256/length verification, repeated finalize and purge/404.
This is a separate matrix from the initial sparse-only comparison. The same
baseline, profiles, settings and methodology apply; neither matrix substitutes
for an old-N1-Lite or full-stack comparison.

After the matrix and counter diagnostic, a physical census of all **seven
private benchmark member roots** finds exactly seven persistent pool markers
and **zero identity files or symlinks**. This verifies cleanup beyond the API's
404 response. `RECEIVE-CENSUS01.json` and `receive-census01-files.txt` record
the scope and exact remaining paths; existing server data are not walked.

| Profile / object size | Objects per binary | Baseline upload MiB/s | Receive02 upload MiB/s | Baseline / receive02 N1 CPU s | Baseline / receive02 immediate GET MiB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| NVMe 1+0 / 1 GiB | 4 | 1,289.58 | 1,731.00 | 2.890 / 1.763 | 959.04 / 978.45 |
| NVMe 1+0 / 15 GiB | 2 | 1,395.10 | 1,865.80 | 43.795 / 26.545 | 957.94 / 968.54 |
| HDD 1+1 / 1 GiB | 4 | 118.08 | 120.64 | 7.575 / 4.830 | 62.94 / 62.02 |
| HDD 3+1 / 1 GiB | 4 | 123.77 | 128.36 | 5.870 / 6.200 | 126.66 / 175.86 |
| HDD 3+1 / 15 GiB | 2 | 152.32 | 164.21 | 76.465 / 84.755 | 154.34 / 174.42 |

Aggregate upload throughput improves in all five groups. At 15 GiB, NVMe gains
**33.74%**, with **39.39% less N1 CPU time**. HDD 3+1 gains **7.81%**, but
uses **10.84% more CPU time**. Its 1 GiB CPU also rises **5.62%**. HDD 1+1
upload improves **2.17%**, with **36.24% less CPU**, while immediate GET falls
**1.46%**. These are retained observations, not a claim that every operation,
resource measurement or individual transfer improves.

| Profile / object / binary | Mean init ms | Mean finalize ms | Part p50 / p95 / p99 ms | Peak N1 RSS KiB |
| --- | ---: | ---: | ---: | ---: |
| NVMe / 1 GiB / baseline | 0.450 | 2.265 | 135.08 / 171.52 / 189.28 | 86,268 |
| NVMe / 1 GiB / receive02 | 0.437 | 2.278 | 87.88 / 131.78 / 202.37 | 79,568 |
| NVMe / 15 GiB / baseline | 0.635 | 1.563 | 135.78 / 148.25 / 159.57 | 75,328 |
| NVMe / 15 GiB / receive02 | 0.338 | 1.662 | 90.43 / 129.99 / 147.77 | 86,268 |
| HDD 1+1 / 1 GiB / baseline | 0.879 | 17.698 | 1,526.87 / 1,756.65 / 2,120.97 | 100,972 |
| HDD 1+1 / 1 GiB / receive02 | 1.000 | 20.681 | 1,499.96 / 1,725.63 / 1,947.60 | 107,200 |
| HDD 3+1 / 1 GiB / baseline | 1.125 | 64.476 | 1,439.46 / 1,938.92 / 2,165.14 | 236,660 |
| HDD 3+1 / 1 GiB / receive02 | 1.000 | 38.431 | 1,318.10 / 1,892.48 / 1,971.47 | 200,104 |
| HDD 3+1 / 15 GiB / baseline | 0.489 | 77.195 | 1,249.30 / 1,498.65 / 1,682.23 | 323,940 |
| HDD 3+1 / 15 GiB / receive02 | 0.567 | 63.623 | 1,156.77 / 1,406.74 / 1,666.26 | 293,016 |

There are 88 part samples per 1 GiB row and 640 per 15 GiB row. Init/finalize
have only four or two samples per group, so their observed maxima are not a
useful object-level p99. Neither finalization code nor the read path changes.
The higher NVMe 1 GiB part p99, HDD 1+1 finalize mean and some RSS peaks remain
visible. RSS is sampled process memory, excluding host filesystem cache.

Small-object figures below are milliseconds, with **128 individual 4 KiB
samples per binary/mode/profile**. All seven operation distributions, including
GET, repeated finalize and purge, remain in `RECEIVE-SUMMARY01.json` and the raw
reports. The table shows init + part + finalize:

| Profile / mode | Baseline p50 / p95 / p99 ms | Receive02 p50 / p95 / p99 ms |
| --- | ---: | ---: |
| NVMe / standard | 2.755 / 2.903 / 7.980 | 2.753 / 3.060 / 9.142 |
| NVMe / durable | 3.518 / 3.661 / 9.068 | 3.502 / 3.675 / 8.811 |
| HDD 1+1 / standard | 73.788 / 89.734 / 90.478 | 74.299 / 91.187 / 133.072 |
| HDD 1+1 / durable | 117.150 / 155.849 / 324.519 | 116.407 / 149.780 / 179.134 |
| HDD 3+1 / standard | 143.727 / 186.774 / 219.095 | 139.851 / 174.835 / 224.847 |
| HDD 3+1 / durable | 238.195 / 298.982 / 520.005 | 232.867 / 283.082 / 519.985 |

Small standard-mode mean latency is 70.58 → 71.19 ms on HDD 1+1 and
2.885 → 2.887 ms on NVMe. Both standard p99 regressions are retained. Durable
HDD 1+1 p99 improves, but its maximum still rises from 366.31 to 383.92 ms;
HDD 3+1 durable maximum rises from 559.83 to 583.05 ms. This sample does not
establish strict tail-latency equivalence or an absence of occasional stalls.

### Scoped CPU counter diagnostic

After the unprofiled matrix, four separate 1 GiB HDD 3+1 objects run A/B/B/A
with `perf stat` attached only to the task-owned N1 PID. All pass full readback
and purge. No CPU governor, affinity, global perf policy or shared service is
changed. The host retains `powersave`/`balance_performance`. The counter window
starts before client setup and stops after observing finalize, with up to 50 ms
observer delay; it can include a little post-upload work. It is diagnostic,
not an additional unprofiled acceptance result.

| Mean counter per object | Baseline | Receive02 | Change |
| --- | ---: | ---: | ---: |
| CPU cycles | 11.261 billion | 7.831 billion | −30.46% |
| Instructions | 10.010 billion | 8.347 billion | −16.61% |
| Context switches | 37,977.5 | 17,901.5 | −52.86% |
| Task clock | 5,582.54 ms | 5,568.26 ms | −0.26% |
| Cycles / task-clock effective frequency | 2.017 GHz | 1.406 GHz | Lower candidate frequency |

The candidate executes less measured work while running at lower frequency.
This supports frequency variation as an explanation for divergent CPU-time
results; it does not directly measure the cause of the separate 15 GiB result.
That **10.84% higher CPU-time observation remains a performance acceptance
concern**, together with small-object tails. The hardware counters do not turn
it into a lower-CPU-time claim. `CYCLES-SUMMARY01.json` preserves all events and
the four raw verified sample paths.

### Current-candidate qualification

The exact receive02 executable passes **222 N1 library tests**, with two
explicit helper/physical exclusions, and **five physical direct-I/O cases**
including the separately enabled page-cache witness. The four-member 3+1/2+2
witness passes all 24 selected loss combinations through the first count beyond
parity, corruption/exact repair and blank-member replacement. Repeated physical
cases are hardware/path coverage, not extra distinct library tests.

With that same N1 binary and the archive-corrected M1, the final selected
public-service invocation passes **36 tests, zero failed**, with nine explicit
exclusions. It includes original Console workflows, backup/restore, trash GC,
cross-EC migration and interruption, deep scan under saturated upload admission,
separate node monitoring, acknowledged resume after both processes are killed,
indexed large-upload status, mixed HTTP/native overload and private-CA Accel
TLS/restart. It finishes in 448.96 seconds. The final complete release workspace
invocation passes **411 tests, zero failed, 60 explicit opt-in/helper exclusions**
across 50 target summaries. Strict workspace/all-target Clippy with warnings
denied passes. All **349 Rust source files** match the workstation manifest.
The driver finishes at **06:48:30 UTC / 14:48:30 Asia/Manila**, exit zero.

## Fixed-object reads and upload I/O diagnosis

The diagnostic client replays **one identical 15 GiB NVMe identity** and
**one identical 1 GiB HDD 3+1 identity**, preserving placement, bytes and roots.
Each binary/cache/profile group has four full SHA-256-verified GETs, with
binary order A/B/B/A/B/A/A/B. Each phase restarts the service; no upload,
finalize, repair or purge occurs inside its comparison timer. The fixture is
verified and purged after both cache conditions finish.

| Same immutable object / cache condition | Current MiB/s | Optimized MiB/s | Current N1 CPU s | Optimized N1 CPU s |
| --- | ---: | ---: | ---: | ---: |
| NVMe 15 GiB / unadvised | 963.90 | 975.77 | 19.940 | 19.735 |
| NVMe 15 GiB / file-cache advised | 957.00 | 985.51 | 19.805 | 19.363 |
| HDD 3+1 1 GiB / cached | 940.76 | 967.60 | 1.200 | 1.175 |
| HDD 3+1 1 GiB / file-cache advised | 139.15 | 137.88 | 3.480 | 3.395 |

The cached HDD runs submit **zero physical reads**. Both advised HDD groups
read exactly **1,074,692,096 bytes per GET**. Per-file advice targets only the
calculated fixture shards, not a storage-pool walk or global cache drop.
Device/controller caches are not evicted. The 15 GiB fixture exceeds host RAM;
the unadvised NVMe condition is not labeled wholly cached.

The 17.84% immediate-read difference is **not reproduced by the fixed-object
comparison**: advised HDD throughput differs by −0.92%; the other three groups
are 1.23–2.98% faster. The earlier comparison did not control placement/cache;
host and disk scheduling may also contribute. These results do not establish
one definitive cause, prove every deployment's reads equivalent, or close the
earlier home SATA readback concern on different hardware.

Four additional 1 GiB diagnostic transfers query the existing I/O backend
counters before/after transfer, outside its timing window. NVMe submissions
fall **8,072 → 4,172**; HDD 3+1 **3,960 → 2,216**. In those samples NVMe
system CPU falls **1.15 → 0.39 s** and HDD system CPU **2.07 → 1.16 s**.
The latter sample's total CPU rises 5.74 → 5.88 s because user CPU rises;
its throughput also falls 128.03 → 119.68 MiB/s. These single diagnostic
samples establish fewer submissions and preserve their adverse totals; they
are not replacements for the repeated acceptance matrix.

## Completed physical correctness checks

- Native release N1 suite on DISK0: **218 passed, zero failed**, two explicit
  opt-ins/helpers excluded. The later physical invocation runs the page-cache
  proof separately; the purge helper is exercised by its parent.
- Four physical XFS direct-I/O checks: **4 passed**, including existing-only
  inode/flags validation, shared boundary/out-of-order/restart behavior, actual
  page-cache bypass, and the new interrupted-part retry/conflict regression.
- Four-member public-native fault witness: **passed 3+1 and 2+2**, all 24
  selected member-loss combinations through/beyond parity, independent shard
  corruption, exact repair and blank replacement. Faults detach only private
  member directories; they do not unmount shared filesystems or cut power.

Repeated direct-I/O cases are physical coverage, not four new distinct default
tests. These checks qualify the optimized N1 artifact, not a new million-object
campaign or host power-loss durability.

## M1 native archive completion

The previous named-native adapter buffered unknown-length direct archive output
and closed the connection above 8 MiB. A new real-service >9 MiB TAR/ZIP/manifest
regression reproduces **UnexpectedEof on the old M1 binary** and passes with the
archive change. That expected baseline failure remains in
`archive-baseline-regression.log`; it is not a failing candidate result.

M1 counts deterministic stored-ZIP64/GNU-TAR framing while it already captures
the immutable manifest. A constant-size summary provides Content-Length without
rereading payloads, rescanning the manifest, writing a second archive copy or
changing native framing. The retained writer, source validation, pins, resource
admission and cancellation remain in use. One bounded final chunk waits for
source verification and manifest/pin cleanup, so a late failure cannot masquerade
as a complete known-length response. Cleanup failures retain their durable retry
intent and fail the response.

Ten local archive/budget/lifecycle unit cases and strict all-target M1 Clippy
pass. New cases compare predicted lengths with the actual pinned archive writers
for empty, boundary-length, long and Unicode names; test large-size arithmetic,
overflow, streaming before completion and late failures. The MCH candidate passes
the new native archive case and existing archive disconnect/crash, source-loss,
permissions/history and graceful shutdown cases. Complete service results follow.

## Service, Console and admission qualification

The corrected archive M1 and optimized N1 pass **34 selected real-service
cases**, zero failed, in **437.66 s**. M1 control/database/staging fixtures use
the MCH NVMe; N1's two independent object members use DISK1 and DISK2. This is
an actual service/native hop, not an in-process store replacement. The suite
includes original Console master authorization and backup/download/restore,
four original live browser cases, Console migration/GC, metadata loss/restore,
member loss/corruption, portable backup/recovery, interrupted publication,
cross-EC migration, retained-export lifecycle, large indexed resume, token/peer
quota rules and TLS listener behavior. The original Console is the already
frozen November 12 build; no Console code changes are made.

The nine service exclusions are the shared-filesystem OS-unmount fixture,
separate explicit-binary performance/audit drivers and existing interactive
deployment probes. Shared MCH mounts and production data are not detached.
Performance drivers are measured separately rather than allowed to compete
with the functional suite. New ignored load/TLS cases are qualified separately.

The complete native release workspace passes **407 default tests, zero failed**,
with **58 explicit opt-in/helper exclusions**. This count precedes the two new
ignored admission/TLS cases; those are not folded into the default count.
It includes the HDD M1 journal byte-boundary recovery sweep and N1 library,
configuration, physical transport/backend, SDK, Shell and release-tool checks.

The ten-second mixed HTTP/native overload witness passes **417 verified read
ACKs and 84 upload ACKs** with 16 bounded workers and two tokens for the same
fragment. The configured shared budgets are 40 reads/s plus a burst of 16,
and 8 uploads/s plus a burst of 4; the observed counts fit those budgets over
10.086 s. Every worker sees 429 rejections. After killing and reopening both
services, every acknowledged upload retains its exact bytes/version; the
version census is exact, completed handles match ACKs, and no pending publication
remains. Its per-worker latency arrays mix admission rejections and accepted
requests and retain the test client's default TCP behavior; they are not a
payload-throughput or service-p99 benchmark.

The initial load harness incorrectly reused a raw-upload connection after 429,
then incorrectly expected M1's completed-idempotency table to be empty. Both
failed attempts and fixtures remain retained. The corrected client reconnects
after rejection and the census distinguishes completed handles/secondary indexes
from pending uploads. **Neither engine was changed** for these harness
corrections. Earlier queued confirmation drivers stopped without measurements
when their prerequisite failed; their exit markers are not storage failures.

The real Accel HTTPS witness also passes: an untrusted private CA and a trusted
CA with the wrong hostname are rejected; a process-local `SSL_CERT_FILE` bundle
allows actual cache/search responses, 64 concurrent pinned reads and restart.
The cache process is configuration-checked and confirmed listening in each
phase. The global trust store and certificate verification are unchanged.
Its first setup used a test administrator credential shorter than Accel's
16-byte minimum and was rejected before serving; that log is retained and
the fixture now validates configuration before launch. No M1/N1 runtime change
is required for the private-CA deployment.

The real idle-bucket lifecycle also passes in **60.08 s**: idle admission state
is reclaimed, active budgets are retained, and shutdown joins the maintenance
worker. The full workspace strict Clippy audit subsequently passes. These are
separate checks from physical power loss and saturation at maximum throughput.

## M1 archive-change performance screen

The matched screen keeps the optimized N1, pool, M1 database and client fixed.
Each of twelve phases selects A (M1 before the archive correction, quotas on),
B (archive correction, quotas on), or C (archive correction, quotas off). Each
variant has 192 individual upload/GET samples and 192 rename/trash samples.
All samples, including first phases, are retained in `M1-SUMMARY.json` and its
raw source reports. This compares the archive change, not owned M1 storage
against ProcyonDB or N1 Lite.

| Metric | A | B | C |
| --- | ---: | ---: | ---: |
| Upload mean ms | 66.494 | 66.435 | 65.653 |
| Upload p50 / p95 / p99 ms | 58.638 / 84.267 / 328.343 | 62.912 / 88.698 / 104.098 | 62.157 / 88.199 / 106.160 |
| M1 CPU, total upload seconds | 0.590 | 0.600 | 0.610 |
| M1 peak RSS during upload, KiB | 13,176 | 13,380 | 13,296 |
| Verified GET mean ms | 0.258 | 0.250 | 0.271 |
| Rename mean / p99 ms | 1.212 / 1.661 | 1.310 / 4.252 | 1.305 / 1.737 |
| Trash mean / p99 ms | 1.246 / 1.423 | 1.275 / 1.466 | 1.294 / 1.498 |
| M1 CPU, total mutation seconds | 0.190 | 0.210 | 0.230 |

The upload mean is nearly unchanged; rename averages 0.098 ms slower with B,
and its p99 is worse. The first B mutation phase contains a 6.36 ms maximum;
later B phases are faster, but the first is not discarded. Database history and
host timing vary across phases. This screen does not isolate a cause for each
tail difference or establish universal latency parity. CPU counters have 10 ms
resolution, which matters for these small totals.

## HDD confirmation and allocation investigation

A predefined B/A/A/B confirmation adds four 1 GiB objects per binary/profile
and 256 small objects per binary/mode/profile. The original sparse-only
candidate's **HDD 1+1 upload regresses 8.21%**, from 126.08 to 115.73 MiB/s,
although CPU falls from 6.500 to 5.355 s/object. HDD 3+1 improves from 129.25
to 135.98 MiB/s, with CPU falling from 5.645 to 4.873 s. Integrity passes in
every case; that does not make the 1+1 performance acceptable.

| Small 4 KiB profile / mode, 256 samples | Current p50 / p95 / p99 ms | Sparse-only p50 / p95 / p99 ms |
| --- | ---: | ---: |
| HDD 1+1 / standard | 71.713 / 91.275 / 99.500 | 74.346 / 91.023 / 140.028 |
| HDD 1+1 / durable | 119.803 / 153.777 / 163.475 | 116.465 / 149.674 / 155.720 |
| HDD 3+1 / standard | 143.156 / 194.056 / 243.178 | 141.297 / 205.645 / 384.844 |
| HDD 3+1 / durable | 232.498 / 293.874 / 549.768 | 240.693 / 308.506 / 486.990 |

A subsequent four-object instrumented B/A/A/B diagnostic measures extent
queries, per-thread CPU/frequency and the exact shard allocations before purge.
Sparse queries total only 0.51–0.59 ms per object. Both binaries produce roughly
**1,999–2,007 physical extents per 1 GiB shard**, generally in interleaved
half-MiB ranges. Instrumented uploads vary from 116.49 to 155.53 MiB/s across
the four objects, and are not substituted for the unprofiled confirmation.
The query time does not explain the regression; allocation fragmentation is a
separate demonstrated cost affecting both binaries.

The next candidate tests **8 MiB allocation windows within each active part**
using the already inode-checked descriptor and `FALLOC_FL_KEEP_SIZE`. It keeps
Begin cheap, does not reserve a whole object, preserves file length and existing
bytes, and leaves SHA-256/CRC32C checks, retry comparisons and durability intact.
Unsupported filesystems retain the original write path. The sparse proof is
captured before allocation; unwritten reservations cannot count as completed
parts. Correctness checks and a matched pilot must finish before acceptance.

The first allocation candidate fails five existing regression cases with
`Invalid("stripe outside object")`: its range calculation called the real-stripe
offset helper with an exclusive end index. The full result is **213 passed,
5 failed, 2 ignored**, retained in `allocation01-n1-regression.log`; an isolated
backup test reproduces the same error. The driver stops before its physical
fault checks or benchmark. The corrected candidate derives the exclusive byte
end from the validated start plus record count and reruns the complete affected
suite. This was an error introduced in the experiment, not a failure of the
previously qualified binaries. No failed result is counted as a pass.

The corrected allocation candidate passes **218 N1 library tests**, all four
physical direct-I/O cases, the four-member 3+1/2+2 fault witness and strict N1
Clippy. Its four-object B/A/A/B pilot then verifies and purges every object.
Candidate uploads are **100.97 and 88.15 MiB/s**, versus **104.29 and 128.12**
for current N1; CPU is **8.60/8.45 versus 9.16/5.97 s**. Candidate immediate
GETs improve to **155.49/121.30 versus 61.32/57.09 MiB/s**. Physical shard
extents fall from **2,000–2,005 to 50–104**. The read improvement does not make
the upload regression acceptable, and this candidate is not accepted.

The first pilot driver stops after its first complete upload/read because its
build PATH omits `/usr/sbin/filefrag`. The fixture and original measured sample
are preserved; continuation captures that same identity's allocation, reopens
and verifies it, then purges it before the other three phases. A first
continuation launch also uses the preceding attempt's RAM directory and exits
before starting N1. The corrected continuation uses the frozen existing binary.
Neither setup error triggers a re-upload or replacement of the slower sample.

Scoped `perf` sampling attaches only to the task's N1 PID. Both profiled objects
pass exact readback/purge. Samples show memory copying, hardware CRC32C and
hardware SHA-256 work, rather than a spinning driver. A separate Rust probe
finds no benefit from adding another SSE4.2 wrapper around the retained CRC
dependency; no checksum code is changed. No global perf/CPU policy is changed.

An eight-object runtime-control diagnostic compares both binaries with twelve
and four workers. With twelve workers, current uploads are **131.70/105.27**
and the allocation candidate **83.81/106.76 MiB/s**. With four, current is
**138.39/106.21** and candidate **83.90/108.11 MiB/s**. Reducing workers does
not resolve the regression and is not adopted as a default change. Sampled
CPU frequency varies substantially: the earlier traced jobs run near 4 GHz,
while candidate upload work in this diagnostic samples near 1.0–1.1 GHz.
The 200 ms samples cannot give exact cycle accounting under thread migration;
frequency variation is retained as a confounder, not used to discard slow runs.

The next experiment coalesces large-part preparation/writes toward **4 MiB of
source data per turn**, capped at 32 stripes. Small objects/parts keep their
existing four-stripe budget. The larger working set is charged before buffer
allocation, falls back with a smaller configured memory budget and preserves
room for four simultaneous parts wherever the original budget did. Explicit
writer-buffer settings still cap the group. The retained SHA-256/CRC32C, EC,
retry checks, native framing, publication and persistence barriers are unchanged.
The experiment must pass its new budget and interrupted-large-part regressions,
the existing suite/fault checks and matched performance before acceptance.

That experiment passes **220 library tests**, five physical direct-I/O cases,
the four-member fault witness and strict Clippy. Its ordinary-INI pilot remains
slow. Inspection establishes that the legacy shared INI `auto` sizing resolves
to **512 KiB** on this two-disk profile, capping the larger groups. A second
four-object B/A/A/B pilot sets the same explicit **4 MiB writer buffer** in both
binaries. It verifies every object and records the following adverse result:

| 1 GiB HDD 1+1 sample | Upload MiB/s | N1 CPU s | Peak RSS KiB | I/O submissions | Immediate GET MiB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Larger groups, B1 | 95.18 | 6.83 | 297,584 | 668 | 117.90 |
| Current, A1 | 134.57 | 6.33 | 134,636 | 4,144 | 108.48 |
| Current, A2 | 101.31 | 8.95 | 126,816 | 4,144 | 98.69 |
| Larger groups, B2 | 117.36 | 6.15 | 291,372 | 676 | 162.31 |

Fewer submissions do not yield an acceptable upload result, and the larger
buffers materially increase resident memory. **The coalescing/admission
experiment is removed from the working implementation.** Its artifacts, source,
tests and both pilots remain retained; it is not promoted into automatic
sizing or hidden behind an operator-tuning recommendation.

The next candidate instead changes the native receive handoff. TLS often
returns one 16 KiB record per read; the previous loop forwarded each through
the owned-task duplex channel. It now fills the **existing 128 KiB buffer**
before handing it off, including an exact shorter final chunk. Each successful
network read still resets the idle deadline. Rejected uploads still drain only
their declared body, and EOF remains an error. Ownership, memory limits, EC
grouping, checksums and durability barriers are retained. New fragmented-read,
control-framing, rejection-drain and truncated-tail regressions accompany this
candidate. Build/test progress is not a performance pass.

The first receive candidate still includes the allocation experiment. It passes
**222 library tests**, five physical direct-I/O cases, the four-member fault
witness and strict Clippy. Its verified B/A/A/B pilot remains slower:
**94.33/88.82 MiB/s**, versus **104.82/108.27 MiB/s** for current N1. These
results do not isolate the receive change from allocation.

A subsequent eight-object diagnostic varies the existing I/O limit, holding the
other settings and binaries fixed. Every object passes full readback and purge:

| I/O limit | Current upload MiB/s | Allocation + receive candidate upload MiB/s | Current / candidate N1 CPU s |
| --- | ---: | ---: | ---: |
| 16 | 131.81 / 127.84 | 104.45 / 101.95 | 5.57 / 6.28 versus 8.40 / 8.43 |
| 1 | 123.85 / 109.67 | 116.18 / 128.71 | 5.39 / 7.07 versus 5.18 / 5.10 |

The allocation candidate improves with serialized I/O and loses with concurrent
parts. This supports a locality cost from writing concurrent, separately
allocated part ranges; it does not establish exact device seek accounting.
Serializing N1 globally is not adopted. **The allocation implementation is
removed from the working source.** Its artifacts and all adverse measurements
remain preserved. The next candidate, `receive02`, retains sparse-read skipping
and the existing bounded receive buffer, with the original allocation behavior,
encoder grouping, memory budget and concurrency settings. Its regression and
matched pilot must finish before any performance acceptance.

`receive02` subsequently passes **222 library tests**, five physical direct-I/O
cases, the four-HDD fault witness and strict Clippy. Its two candidate pilot
uploads are **132.27/110.88 MiB/s**, versus **111.49/127.61 MiB/s** for current
N1. N1 upload CPU is **5.29/4.83 versus 8.37/8.07 s**. Full readback, advised
replay and purge pass for all four identities. The balanced aggregate is slightly
faster with substantially lower CPU, but two objects per variant are only a
screen. A fresh full matched matrix is running under `receive-matrix01.sh`;
its terminal marker and raw results are separate from the earlier sparse-only
matrix. No build or fault suite runs concurrently with those measurements.

## Named M1 → N1 comparison

The fixed archive-corrected M1, NVMe database and four-HDD 3+1 configuration
are held constant while N1 changes A/B/B/A. Each variant has two complete
large objects per method and 200 small objects per method. Every download
verifies bytes/version, and scoped GC reclaims each phase's fixtures. This is
the sparse-only candidate, before the allocation-window experiment.

| Method | Current upload | Sparse-only upload | Current / sparse-only N1 CPU s per object | Current / sparse-only M1 CPU s per object |
| --- | ---: | ---: | ---: | ---: |
| 1 GiB one-shot | 124.81 MiB/s | 114.20 MiB/s | 3.420 / 3.095 | 1.340 / 1.405 |
| 1 GiB durable, 8 MiB parts ×4 | 88.43 MiB/s | 107.50 MiB/s | 3.125 / 2.225 | 7.020 / 7.015 |
| 1 GiB durable, 48 MiB parts ×4 | 139.41 MiB/s | 143.79 MiB/s | 3.780 / 3.345 | 1.410 / 1.560 |
| 100 ×4 KiB, sequential | 9.10 objects/s | 8.93 objects/s | 0.01185 / 0.01165 | 0.00295 / 0.00305 |
| 100 ×4 KiB, concurrency 8 | 20.93 objects/s | 20.40 objects/s | 0.01205 / 0.01130 | 0.00270 / 0.00255 |
| 100 ×4 KiB, one batch | 124.10 objects/s | 127.48 objects/s | 0.00440 / 0.00390 | 0.00060 / 0.00040 |

Aligned multipart finalize averages **26.29 → 36.92 ms**. The 8 MiB geometry
does not divide this 3+1 pool's stripe geometry and uses the existing M1 staging
bridge: finalize averages **10,403.90 → 8,360.34 ms**, with 1 GiB of staged
payload forwarded at completion. It is not constant-time LINK publication.
This remains an explicit M1 geometry-bridge performance limitation, distinct
from direct N1 finalize; these measurements do not conceal it behind the aligned
result. Two large samples per method cannot establish a useful object-level p99.

Small sequential upload p99 is **175.36 → 183.35 ms**; concurrency-eight p99
is **678.70 → 805.18 ms**. The one-shot and small regressions are retained;
no universal stack speedup is claimed. `NAMED06-SUMMARY.json` preserves process
RSS, GET rates, individual request/part latencies and source report paths.

The first named launch stopped before payload transfer because the benchmark
tried to read `/proc/0` for an absent Console process. The corrected harness
omits an optional zero Console PID and validates distinct positive N1/M1 PIDs.
Both engines remain unchanged by that correction. The failed setup and its
empty-fragment database are preserved separately from the successful run.

## Evidence

Remote root: `/mnt/DISK0/n1-optimization-20261113-01`.
Local retrieved evidence: `/mnt/HDD1TB001/n1-mch-optimization-20261113-01`.
Portable source/dependencies remain under `/home/altear/DEV_TEST`; DATA_SSD is
not required. `reports/SUMMARY-FINAL.json` and `READ-SUMMARY.json` preserve
the initial candidate's samples. `RECEIVE-SUMMARY01.json` records the current
candidate's complete matrix; `CYCLES-SUMMARY01.json` records the separate
hardware counter diagnostic. All preserve raw source paths. Bash orchestration
and Perl arithmetic are under `evidence/`. All workload and fault logic is Rust.
No Python is used.

| Native MCH artifact | SHA-256 |
| --- | --- |
| Current N1, base `3d6e1a3` | `2809bc8063b364928e3cfb247b9df794798d2dfb233c149b811100948c75cb9b` |
| Initial sparse-only N1 | `756d64365790986787a63720f2b2007d28e00fd667b82a2c8e9f0c6ca9a97391` |
| Receive02 N1, without allocation windows | `27e0375ae84e1fff86bf958334aa01e5da6dad32f2f1202f7179f3ac588337f8` |
| Initial frozen Rust matrix client | `5f6be3ecd386a525bc724dc54cdbce98a844422ac5158635ce9136bf17f843a2` |
| Original M1 before archive correction | `4cf0c338c2cac7a69fed0256fe0b38e3ed607225a891beab827e94dcf647b9fd` |
| M1 archive correction | `5d995880f2b9b563d3f2d8f88eae905695bc617b7c4eaff1b2971f43bdcc21a7` |

Source overlays, their manifests and both attempted native-build drivers are
retained. Kernel messages report XFS inode-GC workqueue CPU warnings during the
matrix; no transport/device failure is inferred from those warnings. They remain
part of the environmental evidence, not discarded as benchmark outliers.

## Continuation status

Current-candidate build/fault/pilot completion is `RECEIVE02-EXIT=0`.
The matched matrix has `RECEIVE-MATRIX01-EXIT=0` and all 32 entries in
`RECEIVE-MATRIX-COMPLETED`. The process-scoped diagnostic has
`CYCLES-DIAGNOSTIC01-EXIT=0`. Final service/Console/workspace checks are tracked
by `RECEIVE-SERVICES-STATUS`, `RECEIVE-SERVICES01-EXIT` and their individual
logs: status **PASSED**, exit **0**. Drivers and all earlier attempts remain
under `evidence/`. No task-owned qualification process or queued million
population remains. Existing deployed services were not stopped.

The exact baseline/candidate N1, corrected M1, Rust client and tested harnesses
are preserved in `evidence/receive02-tested-artifacts.tar.gz`, with relative
SHA-256 manifests. The 26 MiB archive, current source checkpoint and reports
are also copied to `/home/altear/DEV_TEST/n1-mch-continuation-20261113-01`.
The dependency capsule remains in its sibling portable directory; neither
DATA_SSD nor a build cache is needed to recover this work. No commit, push,
tag or release has been performed.

After optimizing and validating the affected paths, resume the
[RC.2 checklist](RC2-IMPLEMENTATION.md): final-candidate service/Console and
contract checks, broader saturation and maintenance impact, the recorded
CPU-time/tail concerns, M1's incompatible-part geometry bridge and separately
controlled physical power-loss qualification. The native archive correction's
specific large-stream and cleanup checks pass; that is not a complete
field-by-field NOA conformance claim.
Process kills cannot close the physical power-loss gate, and existing deployed
services must not be interrupted implicitly. No commit, push, tag or release is
part of this run unless separately requested.
