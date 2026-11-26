# Standalone N1 mixed file-service workload — November 16, 2026

**Original frozen-binary campaigns: integrity and functional concurrency passed.**
Standalone N1
completed uploads, downloads, video-style ranges, reconnect/resume and deletes
together without M1. All 513 created identities were accounted for. The busy
profile exposes substantial write/delete tails and slower large uploads; it
does not justify a general claim of excellent mixed-workload performance.

No N1, M1, LINK, ECS Core or database runtime code changes in the original
campaigns below. They add a Rust workload client and record the existing N1
binary's behavior. The [publication follow-up](#publication-follow-up-on-both-minimum-storage-profiles)
separately records a rejected performance experiment and retained hardening.

The subsequent [two-SATA-SSD control](#two-sata-ssd-control) uses the exact same
N1 and workload executables. It substantially improves busy-phase latency and
throughput, while retaining some adverse observations. These are different
hardware profiles running one implementation, not evidence of a software
regression between N1 versions. The original HDD results remain unchanged below.

## Hardware and artifact identity

The [two-HDD test profile](TEST-PROFILES.md) has an AMD Ryzen 5 PRO 4650U,
6 cores / 12 threads and 15.0 GiB usable memory. Both 1 TB ST1000LM049 HDDs use
separate XFS filesystems and USB UAS at 5 Gbit/s, observed on separate USB root
buses. Linux is `6.12.115+deb13.1-amd64`. The pool is immutable 1+1 with 256 KiB
blocks. N1 control state, fresh members, builds and evidence are on the test
HDDs; the source NVMe holds no workload dataset or build target.

| Artifact | Identity |
| --- | --- |
| Frozen N1 `staged02`, version `1.0.0-RC.2` | SHA-256 `b08686c7e392ae2fabe30632f56faaac306caddd4ac505fda2551952341eddc5` |
| Selected Rust workload executable | SHA-256 `9c154e405bf2814bff44f131bc0f6130881a34700cff0276ab1839842010867d` |
| Repository base before adding this workload | `07fc734bcb4a7c67e3b6a79e1f881b289e5b87ac` |
| Client build | Rust 1.98.0, repository release profile, locked/offline dependencies |
| ECS Core pin, unchanged | `c77c638bfb709368f49517f2828fa977738e8467` |

This is one N1 executable throughout, not an old/new binary comparison. The
client build does not relabel the previously qualified N1 artifact's compiler
or source identity. No M1 or Console process is started or used. Native TLS is
verified against a private test CA. The backend startup log confirms io_uring
with no fallback; the frozen executable retains its previously qualified aligned
direct-I/O implementation. This stage does not repeat its syscall-level proof.

Native connections are capped at 64, admitted operations at 32 and operation
memory at 256 MiB. Adaptive concurrency is enabled with a 1,000 ms interval;
stream maximum is 128, chunk size 256 KiB and readahead 512 KiB. Request quotas
are explicitly disabled to measure storage contention. Existing desktop load,
CPU policy and caches are left unchanged.

## Application workload

The [reusable client](../native_client/examples/mixed_workload/README.md)
models a file service, rather than timing each operation in isolation. It first
uploads and verifies two 64 MiB videos, eight 8 MiB downloadable files and 128
older small files. The measured sessions then run concurrently:

| Phase | Offered window | Bulk uploaders | Video viewers | File downloaders | Small-file uploaders | Deletion sessions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Light before | 60 s | 1 | 1 | 1 | 1 | 1 |
| Busy | 120 s | 2 | 4 | 2 | 4 | 2 |
| Light after | 60 s | 1 | 1 | 1 | 1 | 1 |

Each bulk uploader starts with 1 GiB and continues with 512 MiB files, using
8 MiB parts. Normal and strict part modes alternate; the busy uploaders start
in opposite modes. Every large upload reconnects after two acknowledged parts,
checks the same LINK key's progress and sends only the remaining parts.
Finalize and repeated finalize are measured separately.

Interactive uploads cycle through 4 KiB, 64 KiB, 256 KiB and 1 MiB, using normal
and strict plans. Video clients read 1 MiB ranges with changing, unaligned
offsets; download clients read whole 8 MiB files. Deletion sessions consume older
files and subsequently eligible completed uploads. These clients have recorded
think times; all share one authorized storage principal through independent
connections. This is not a test of fourteen different authorization policies.

Think time is excluded from operation latency. Upload latency includes Begin
and completed PUT. DELETE latency includes the subsequent 404 confirmation.
Read latency includes exact byte comparison and SHA-256 generation by the
client. A video-style range client is not a browser decoder or playback-buffer
quality test.

Operations already started at a window boundary finish normally. Actual phase
walls are **61.945 / 141.589 / 80.537 seconds**; the three phases total
**284.071 seconds**, excluding seeding, pauses, full verification and cleanup.
Raw reports retain every completion, including the drain. Sustained-window rates
separately count operations completed within the original 60/120/60-second
windows. A completed large object may span the window and its drain.

The 192 MiB stable read library was verified during seeding and fits in memory.
No page-cache drop is performed. These GET results include warm-cache behavior
and must not be presented as cold random HDD performance. This is a loopback
storage-service workload, not a WAN or Console benchmark. Closed-loop clients
reduce their offered rate when requests slow; zero errors does not establish
open-loop saturation capacity or a maximum user count.

## Functional outcome

| Check | Observed result |
| --- | --- |
| Measured logical operations | **2,541**, zero recorded request or worker errors |
| Peak simultaneous operations | **5 / 14 / 5**; all five operation classes overlap in every phase |
| New measured small-file uploads | **367**: 242 normal plans, 125 strict plans |
| Large files | **8**, totaling **6 GiB**, with 448 normal and 320 strict part ACKs |
| Measured reads | 536 video ranges, 322 full-file downloads and 367 immediate small-file reads; all verified |
| Reconnect/progress checks | **8 passed**, with no acknowledged part resubmitted |
| Finalize / repeated finalize | **8 / 8 passed**, same LINK identity retained |
| Deletes during mixed load | **149**, each followed by confirmed absence |
| Full census before restart | **364 live + 149 deleted = 513 identities** |
| Full readback before clean restart | Every live byte verified: **6,726,639,616 bytes**; all deleted identities absent |
| Full readback after clean restart | Same complete verification and absence results, without re-upload |
| Final cleanup | Remaining **364** live objects purged; all 513 created identities deleted |
| Final member census | Exactly **two pool-marker files**, no symlinks; separate inspection confirms both `.uploads` directories are empty |
| Process ownership | Private N1 stops cleanly; no qualification service remains |

The restart is an orderly process shutdown/startup. It is not a SIGKILL,
hardware power interruption, member-loss or filesystem-failure result.

## Latency distributions

Each cell is **sample count; p50 / p95 / p99 milliseconds**. These distributions
include all completed operations and the drain, so late completions are not
discarded. Quantiles use nearest rank. Small cohorts, especially under 100
samples, provide limited tail evidence; a percentile equal to a maximum is not
a stable production p99 estimate.

| Operation | Light before | Busy | Light after |
| --- | ---: | ---: | ---: |
| Small upload, normal plan | 32; 357.155 / 2,161.055 / 2,374.306 | 171; 1,208.436 / 2,492.470 / 2,952.470 | 39; 301.010 / 2,149.875 / 2,458.825 |
| Small upload, strict plan | 17; 332.020 / 2,116.707 / 2,116.707 | 88; 1,295.598 / 2,752.834 / 4,163.636 | 20; 254.680 / 1,400.262 / 1,537.194 |
| Immediate small GET | 49; 1.500 / 11.748 / 766.572 | 259; 2.338 / 382.533 / 791.328 | 59; 1.506 / 12.263 / 28.030 |
| Video range, 1 MiB | 60; 5.189 / 5.893 / 18.580 | 416; 21.188 / 711.260 / 1,173.275 | 60; 5.153 / 6.063 / 10.746 |
| Full download, 8 MiB | 60; 30.214 / 36.557 / 45.134 | 202; 57.654 / 624.184 / 1,166.695 | 60; 30.098 / 47.958 / 49.760 |
| DELETE + absence | 27; 355.812 / 947.789 / 1,450.002 | 92; 677.324 / 1,716.369 / 2,393.386 | 30; 151.162 / 774.233 / 813.145 |
| Normal multipart part, 8 MiB | 128; 54.760 / 1,351.865 / 1,926.217 | 128; 702.095 / 2,125.818 / 2,437.222 | 192; 62.385 / 264.910 / 954.825 |
| Strict multipart part, 8 MiB | 64; 231.331 / 1,466.038 / 2,794.256 | 128; 1,023.317 / 2,723.600 / 3,299.278 | 128; 206.733 / 1,340.818 / 1,925.231 |

Busy maxima include **4,163.636 ms** for a small upload, **2,393.386 ms** for
DELETE/absence, **1,382.688 ms** for a video range, **1,852.543 ms** for an 8 MiB
download, and **3,967.399 ms** for a strict multipart part. The normal-part maximum
is **3,616.076 ms**. These outliers remain in the raw samples.

Small-upload size also matters. The busy phase has 88 × 4 KiB, 88 × 64 KiB,
42 × 256 KiB and 41 × 1 MiB uploads. Their median/p99 milliseconds are
**1,214.063 / 4,163.636**, **1,139.237 / 3,006.832**,
**1,366.440 / 3,438.529**, and **1,362.407 / 2,952.470**, respectively.
The delay is therefore visible for tiny payloads too. Normal/strict small-plan
cohorts have different size mixtures; this run does not isolate durability-policy
cost by comparing those two rows directly.

There are only eight large objects, so report every control observation rather
than claiming an object-level finalize p99:

| Phase | Init ms, each | Finalize ms, each | Repeated finalize ms, each | Reconnect + bounded status ms, each |
| --- | --- | --- | --- | --- |
| Light before | 0.561, 0.964 | 185.438, 25.766 | 14.741, 12.301 | 657.200, 2,785.546 |
| Busy | 7.082, 16.351 | 484.221, 16.333 | 91.950, 18.037 | 1,313.606, 1,577.141 |
| Light after | 1.559, 0.882, 0.939, 0.886 | 146.654, 24.074, 206.333, 12.020 | 40.693, 50.380, 12.504, 13.685 | 3,301.050, 315.871, 642.015, 5,795.186 |

Reconnect/status includes opening and authenticating a new connection and
checking the first four parts. It is not a status-only microbenchmark. The
**5.795-second** light-after observation remains an adverse result.

## Large uploads, CPU, memory and device observations

Per-file rates include transfer, reconnect/status, finalize and repeated finalize,
but exclude Begin and preparing the source pattern. Values below preserve every
file in completion order; normal and strict files run under changing contention
and are not a matched policy comparison.

| Phase | Each completed large upload: mode, size, seconds, MiB/s |
| --- | --- |
| Light before | normal, 1 GiB, 29.781 s, 34.384 MiB/s; strict, 512 MiB, 31.039 s, 16.495 MiB/s |
| Busy | normal, 1 GiB, 120.147 s, 8.523 MiB/s; strict, 1 GiB, 141.572 s, 7.233 MiB/s |
| Light after | normal, 1 GiB, 14.552 s, 70.371 MiB/s; strict, 512 MiB, 23.328 s, 21.948 MiB/s; normal, 512 MiB, 10.132 s, 50.533 MiB/s; strict, 512 MiB, 32.517 s, 15.745 MiB/s |

Completed part-byte rates **within the offered window**, excluding later drain
completions, are 25.067 / 14.200 / 36.133 MiB/s across both normal and strict
uploaders. Full-download plus video-range rates in those same windows are
9.000 / 16.933 / 9.000 MiB/s. These omit immediate small reads and uploads;
they must not be represented as total verified object throughput. There are no
errors, and partially received but unacknowledged parts are not counted as
completed part throughput.

Resource observations below cover each complete phase including its drain.
**100% CPU means one logical CPU**, not the whole twelve-thread processor.
RSS uses 500 ms samples; kernel process high-water marks remain in the raw data.

| Observation | Light before | Busy | Light after |
| --- | ---: | ---: | ---: |
| N1 CPU seconds | 11.83 | 24.07 | 17.96 |
| N1 average CPU, one-core percentage | 19.10% | 17.00% | 22.30% |
| N1 sampled peak RSS | 74.20 MiB | 149.68 MiB | 150.20 MiB |
| N1 process swap | 0 | 0 | 0 |
| Observed peak simultaneous operations | 5 | 14 | 5 |
| Mean device write rate, two HDDs separately | 25.04 / 25.07 MiB/s | 15.00 / 15.04 MiB/s | 32.02 / 32.04 MiB/s |
| Mean device write-request await | 26.02 / 29.41 ms | 85.88 / 56.06 ms | 15.61 / 16.77 ms |
| Mean weighted queue depth | 2.20 / 2.46 | 4.88 / 3.46 | 1.85 / 1.95 |

The raw resource series also records the client separately; its hashing and
payload verification are not charged to N1's CPU. Device rates include physical
filesystem traffic and are not logical object throughput. This short test shows
the measured memory footprint; it does not establish long-duration leak absence.

## Judgment and retained evidence

N1 demonstrably handles these operation classes concurrently and preserves the
tested data and deletion outcomes. Read medians and large-upload throughput
recover when the busy profile ends. CPU consumption remains low in this paced
workload. However, busy small uploads have roughly 1.2–1.3-second medians,
read tails exceed one second, and bulk upload rates decline substantially.
Even the lighter profiles retain multi-second mutation/resume outliers.
**This is a functional concurrency pass with measured latency limits on this
hardware.** No numerical mixed-latency acceptance threshold was defined for the
campaign; the observations must not be relabeled as failed software regression
checks. Purge latency includes durable shard removal, an intentional cost of
its contract. That latency alone does not establish an implementation defect.
The light-after phase is a recovery observation, not an optimization speedup;
the binary is identical and cache, device and competing-load state may vary.

The tails occur without M1, so M1's database cannot explain them in this run.
The larger device queues and write waits support I/O contention as a factor.
N1 also retains its filesystem-wide durable publication barrier; the test does
not isolate its cost from the filesystem, device flush behavior and scheduling.
Do not automatically blame hardware or claim a particular lock/syscall caused
each tail. Trace those waits in a separate diagnostic window before changing
the frozen implementation or weakening durability.

The new harness passes its two negative-verification/quantile checks, strict
release Clippy and formatting. Its corrected short smoke run verifies overlap,
restart readback and cleanup before the full workload. The first smoke attempt
incorrectly sent a 64 MiB PUT using an 8 MiB multipart plan; the client observed
a connection reset after the invalid request. The fixture is corrected to use
multipart for those library videos. The original failure is retained separately
and is not counted as an engine integrity failure or a performance sample.

Private evidence is mapped under
`/srv/test-evidence/n1-mixed-workload-20261116`: source overlay and hashes,
hardware/configuration inventory, `reports/full.log`, all three phase summaries,
2,541 operation records, 500 ms resource samples, pre/post-restart verification,
cleanup and `full/PASSED.json`. The output schema's `request_samples` counts
logical operation samples; uploads and delete/confirm can include multiple
native requests. It must not be read as an exact protocol-frame count.

No existing population was restarted or overwritten. No N1/M1 engine change,
dependency update, release or tag follows from this test. Four-HDD, controlled
physical power-loss, broader saturation and remaining system/Console acceptance
retain their separate release gates.

## Two-SATA-SSD control

The SSD control runs on an Intel Core i5-12600K, 10 cores / 16 threads,
approximately 31 GiB usable RAM and Linux `6.12.115+deb13.1-amd64`. The two
physical members are an Intel SSDSC2KW240H6 240 GB SATA SSD with XFS and a
Kingston SA400S37480G 480 GB SATA OS SSD with ext4. Fresh private member
directories are created on their existing filesystems. No formatting or
existing-data removal occurs. The OS SSD continues serving its ordinary system
workload. CPU policies, applications, I/O scheduling and caches are unchanged.

Both executables match the SHA-256 identities in the artifact table above.
There is no rebuild or N1 runtime, LINK, ECS Core, durability or workload-client
change. The same 1+1 geometry, 256 KiB blocks, 8 MiB parts, 60/120/60-second
windows, five/fourteen/five sessions, normal/strict plans and resource limits
apply. The effective connection/operation/memory limits, concurrency and runtime
configuration are compared against the original run and match. Native TLS
verification and required io_uring initialization remain enabled. M1 and Console
are not used. Binaries, certificates, control state and evidence reside on the
data SSD; the other member resides on the OS SSD. No NVMe holds test files,
datasets, builds or test output.

This is a storage-platform control: CPU, memory, storage media, connection type
and one filesystem differ from the HDD profile. It does not isolate an enclosure
from the HDD mechanism, or prove that an enclosure is defective. The unchanged
client's historical result-scope string says `two-HDD`; the captured physical
inventory and filesystem/device checks establish the actual two-SSD placement.

### Integrity and completed work

| Check | SSD observation |
| --- | --- |
| Measured logical operations | **4,291**, zero request/worker errors |
| Peak simultaneous operations / distinct roles | **5 / 14 / 5** operations; all five roles overlap in each phase |
| Created identities | **872**: 138 seeds, 712 measured small files, 22 large files |
| Large upload payload | **13 GiB**, with reconnect/progress and finalize/repeated-finalize checks for all 22 objects |
| State before and after clean restart | **696 live + 176 deleted**, with identical verified outcomes |
| Complete verified live bytes per readback | **14,324,854,784 bytes**, before restart and again afterward |
| Cleanup | All 872 identities purged; exactly two pool markers, no symlinks, both `.uploads` directories empty |
| Process/artifact checks | Owned N1 exits cleanly; executable hashes and CPU governors remain unchanged |

Phase walls, including drain, are **74.862 / 128.748 / 76.221 seconds**. The
offered windows remain 60/120/60 seconds. Faster closed-loop sessions complete
more work, so CPU seconds, total bytes and object counts are not a fixed-volume
old/new comparison. Host process/load inventories are retained privately.

### Busy-phase comparison

Each cell gives **sample count; p50 / p95 / p99 milliseconds**, including drain.
Quantiles are recomputed from every raw request and checked against the original
summary values. Cohorts and cache/verification semantics retain the qualifications
described above; this is one campaign per platform, not repeated-run confidence
intervals or a production p99 guarantee.

| Operation | Two USB HDDs | Two SATA SSDs |
| --- | ---: | ---: |
| Small upload, normal plan | 171; 1,208.436 / 2,492.470 / 2,952.470 | 356; 374.395 / 1,007.271 / 1,262.825 |
| Small upload, strict plan | 88; 1,295.598 / 2,752.834 / 4,163.636 | 180; 391.691 / 886.675 / 1,240.087 |
| Immediate small GET | 259; 2.338 / 382.533 / 791.328 | 536; 2.030 / 89.710 / 232.694 |
| Video range, 1 MiB | 416; 21.188 / 711.260 / 1,173.275 | 468; 7.201 / 146.120 / 376.570 |
| Full download, 8 MiB | 202; 57.654 / 624.184 / 1,166.695 | 231; 23.801 / 185.507 / 304.568 |
| DELETE + absence | 92; 677.324 / 1,716.369 / 2,393.386 | 113; 312.952 / 681.213 / 746.210 |
| Normal multipart part, 8 MiB | 128; 702.095 / 2,125.818 / 2,437.222 | 384; 247.823 / 631.177 / 1,015.126 |
| Strict multipart part, 8 MiB | 128; 1,023.317 / 2,723.600 / 3,299.278 | 448; 199.446 / 763.053 / 972.014 |

The SSD busy phase still contains small-upload maxima of **1,594.803 ms**
(normal) and **1,396.246 ms** (strict). Its 181 uploads of exactly 4 KiB have
median/p99 **386.355 / 1,151.023 ms**, so the improvement is not explained solely
by a different small-file size mixture. The remaining waits are recorded;
the SSD control does not make them disappear.

Busy SSD finalize has 11 observations, **42.033 ms median / 131.505 ms maximum**;
repeated finalize has **42.565 / 131.267 ms**. Reconnect/authentication plus
bounded status has **600.758 / 1,130.131 ms**. These small cohorts are not
object-level tail guarantees. Repeated-finalize maximum exceeds the original
busy HDD observation of 91.950 ms, with different sample counts.

### Throughput, resources and adverse controls

| Observation | Two USB HDDs | Two SATA SSDs |
| --- | ---: | ---: |
| Completed part-byte rate inside light-before window | 25.067 MiB/s | 60.000 MiB/s |
| Completed part-byte rate inside busy window | 14.200 MiB/s | **49.333 MiB/s** |
| Completed part-byte rate inside light-after window | **36.133 MiB/s** | 34.933 MiB/s |
| Busy initial normal 1 GiB file, including reconnect/finalize/retry | 120.147 s / 8.523 MiB/s | 49.305 s / 20.769 MiB/s |
| Busy initial strict 1 GiB file, same timing boundary | 141.572 s / 7.233 MiB/s | 63.937 s / 16.016 MiB/s |
| Busy N1 CPU seconds / mean one-core percentage | 24.07 s / 17.00% | 51.30 s / 39.85% |
| Busy N1 sampled peak RSS | 149.68 MiB | 188.25 MiB |
| Busy physical device write rates, separately | 15.00 / 15.04 MiB/s | 52.96 / 53.80 MiB/s |
| Busy mean write-request await, separately | 85.88 / 56.06 ms | 9.48 / 21.29 ms |
| Busy weighted device queue depth, separately | 4.88 / 3.46 | 1.42 / 4.89 |

SSD device order is data SSD, OS SSD. Counters cover physical filesystem traffic,
including other users of those filesystems. They do not isolate N1's requests.
N1 has no process swap in either run. The SSD busy phase completes 6.5 GiB of
large uploads versus 2 GiB on HDD; its additional work and different CPU prevent
interpreting the greater CPU seconds or RSS as a software regression.

The complete SSD large-object observations below include every file, in
completion order. Values are mode, size, seconds and MiB/s. Timing boundaries
match the original per-file table.

| Phase | SSD large uploads |
| --- | --- |
| Light before | normal, 1 GiB, 3.728 / 274.660; strict, 512 MiB, 2.560 / 200.007; normal, 512 MiB, 4.859 / 105.364; strict, 512 MiB, 18.694 / 27.389; normal, 512 MiB, 10.547 / 48.547; strict, 512 MiB, 18.826 / 27.197; normal, 512 MiB, 15.612 / 32.796 |
| Busy | normal, 1 GiB, 49.305 / 20.769; strict, 1 GiB, 63.937 / 16.016; strict, 512 MiB, 21.875 / 23.406; normal, 512 MiB, 14.395 / 35.568; normal, 512 MiB, 26.961 / 18.990; strict, 512 MiB, 23.115 / 22.150; strict, 512 MiB, 9.061 / 56.504; normal, 512 MiB, 10.264 / 49.881; normal, 512 MiB, 11.923 / 42.942; strict, 512 MiB, 13.334 / 38.399; strict, 512 MiB, 9.592 / 53.380 |
| Light after | normal, 1 GiB, 27.456 / 37.296; strict, 512 MiB, 19.476 / 26.289; normal, 512 MiB, 11.097 / 46.139; strict, 512 MiB, 18.181 / 28.162 |

The initial 274.660 MiB/s file is a burst observation, not sustained mixed-load
capacity. The light-after SSD part-byte rate is slightly lower than HDD, its
normal-part median is **275.498 ms versus 62.385 ms**, and immediate small-GET
p99 is **73.454 ms versus 28.030 ms**. These adverse observations prevent a
universal SSD speedup claim. All three SSD phase distributions and individual
samples remain available in the evidence.

### Interpretation and evidence

The unchanged N1 runs the same busy session profile with **3.47 times**
the acknowledged multipart byte rate and substantially shorter mutation/read
latencies on the SSD platform. This supports a substantial platform contribution
to the HDD observations. It does not identify USB transport, HDD seeks, device
flushes, filesystem ordering or CPU differences as the sole cause. In particular,
this experiment establishes neither enclosure failure nor a new N1 code regression.

Filesystem-wide publication/removal barriers remain present in both runs.
Their per-request contribution is not isolated here. Earlier integrity and
optimization evidence remains valid within its recorded scope; this control
requires no architecture or runtime change. Remaining physical power-loss,
four-drive and system/Console gates retain their existing scope. No new latency
SLA, product-wide performance failure or release is inferred from this comparison.

Evidence is mapped privately under
`/srv/test-evidence/n1-ssd-control-20261116`: the exact driver, physical inventory,
artifact hashes, 4,291 raw operation records, resource samples, full verification,
cleanup and validated cross-platform reduction. The evidence archive SHA-256 is
`2cc72f2c201c1c53f7ad1f3354761b151a3f1e901e7a6cfcc2063f2f78ba2c6d`.
Raw credential and private-key files are excluded from that archive. The
unprivileged kernel journal is inaccessible; its empty output does not prove
absence of kernel storage errors. No Python is used.

## Publication follow-up on both minimum storage profiles

This follow-up tests one immutable 1+1 pool on the two HDDs and on the two
SATA SSDs described above, without M1 or Console. LINK, pool geometry,
checksums, ECS Core, direct I/O, io_uring, scheduling and durability policies
are preserved. The one-shot publication experiment below is **rejected**;
its measured disadvantages are not shipped as an optimization.

### Measured waits and physical storage controls

A separate diagnostic window on the unchanged frozen N1 correlates syscall
and engine timing. An OS-SSD `syncfs` takes 983.341 ms; the captured foreground
queue reaches about 898 ms. The HDD diagnostic records filesystem-barrier waits
up to 1,395.902 ms and write-admission waits up to 1,534.437 ms. These are separate
maxima, not additive latency components or traces of every original outlier.
The adaptive target stays at 32; the existing per-principal foreground cap is
six on this two-member configuration. Neither limit changes in this stage.

The SSD syscall diagnostic also samples Health through authenticated shell
sessions. That introduces control-state I/O and is excluded from performance
acceptance. The harness now supports `--diagnostics` snapshots outside timed
windows, without repeated authentication refresh writes.

A private Rust control writes 4 GiB to each member concurrently through aligned
O_DIRECT files, verifies every byte by direct readback, removes its own files and
persists the removals. Buffers are bounded. One control synchronizes at completion;
the other also synchronizes each 8 MiB. Rates are 4 GiB divided by the time until
both members finish writing, rather than the sum of mirrored physical bytes.

| Paired-file write reference | Two USB HDDs | Two SATA SSDs |
| --- | ---: | ---: |
| Sync at completion | 114.523 MiB/s | 70.479 MiB/s |
| Also sync each 8 MiB | 35.921 MiB/s | 40.391 MiB/s |

The SATA members differ substantially: the data SSD writes at 128.095 MiB/s
versus 70.480 MiB/s for the OS SSD in the completion-sync control, and at
104.860 versus 40.392 MiB/s with intermediate syncs. These observations explain
why an SSD label cannot supply a throughput expectation. The control is a
sequential paired-file reference, not N1's format, mixed workload, theoretical
hardware ceiling or a power-loss test. Its rates must not become a claimed N1
device-utilization percentage.

### Rejected single-object directory-sync experiment

The experiment retains sealed-file persistence but replaces single PUT's grouped
filesystem-wide publication barrier with multipart's affected-directory fsyncs.
It reduces some SSD mutation latency, but does not improve both minimum profiles.
Single PUT is therefore restored to its existing grouped barrier. Batch and
purge never change during the experiment. No journal, queue, receipt or storage
format is added.

Both baseline and candidates are built with Rust 1.98.0 and the same repository
release/LTO settings, retaining symbols. The baseline runtime is the committed
source preceding this follow-up, SHA-256
`53a6412f1358cadd97db0474f20be3e8d5547376f996b594e8920e3d57229a63`.
It is a rebuild, not the historical frozen artifact relabeled with a different
compiler. The rejected final experiment is
`bd08305246a991638f607f53bb1bcadfb454fa3d4732b22f9ede82a224b07f9d`.

All new comparisons use the same extended Rust workload executable,
`05f9bd9a5383075b28f5aef8dbafc5ffb25759a6e0ee83481576ffb8bd27cdb1`.
They use **20/40/20-second offered windows**, unchanged full-size bulk files,
and the same 5/14/5 sessions. They are separate from the preceding 60/120/60
campaigns. Builds and correctness tests finish before each same-host timed run.
CPU policies, applications and caches remain unchanged. Every run verifies all
live bytes and deleted absences before and after clean restart, then purges its
identities and requires exactly two pool-marker files.

The HDD pair runs baseline then candidate. The SSD numbers below use the
reverse-order candidate/baseline pair; the earlier forward-order observations
remain in the evidence. Cells are milliseconds except the multipart-byte row.
Percentiles include all completions, including drain.

| Busy observation | HDD baseline | HDD experiment | SSD baseline | SSD experiment |
| --- | ---: | ---: | ---: | ---: |
| Acknowledged multipart MiB/s inside offered window | 16.2 | 11.6 | 54.2 | 42.8 |
| Normal small-upload p50 | 1,075.762 | 1,167.154 | 483.197 | 339.017 |
| Strict-plan small-upload p50 | 1,255.134 | 1,548.181 | 457.777 | 305.463 |
| Video-range p99 | 1,678.660 | 801.315 | 904.949 | 858.892 |
| Full-download p99 | 1,975.459 | 1,214.768 | 1,046.238 | 800.877 |

The HDD experiment also raises normal small-upload p95 from 1,856.333 to
2,655.748 ms, strict-plan p99 from 2,539.850 to 2,800.550 ms, and sampled busy
RSS from 119.750 to 145.086 MiB. SSD normal small-upload p99 rises from
1,139.166 to 1,183.992 ms in the reverse-order pair despite the lower median.
The earlier SSD baseline records 37.6 MiB/s, compared with 42.6 MiB/s for the
first candidate variant and 42.8 for the final experiment; baseline variation
alone prevents claiming a reproducible bulk-throughput improvement.

Upload-only controls retain the same bulk sessions and geometry while removing
the other roles. Their busy normal rates are 96.6/92.6 MiB/s on HDD and
33.4/38.0 on SSD, baseline/experiment. Strict rates are 16.6/17.0 on HDD and
34.2/37.0 on SSD. SSD strict-part p99 nevertheless worsens from 1,077.050 to
1,349.250 ms. These controls and every original outlier are preserved, not pooled
with mixed samples or used to declare every operation faster.

### Retained correction and evidence

The retained runtime correction opens multipart publication directories before
renames/removals, including an independent root open description per publication.
Previously the directory-persistence helper opened them after mutation and reused
the shared root description. The existing sealed-data and directory barriers
remain; the change preserves error observation across namespace mutations and
concurrent publications. File fsync alone does not persist directory entries,
and writeback errors must be checked by the affected owners. See
[Linux fsync semantics](https://man7.org/linux/man-pages/man2/fsync.2.html) and
[Linux error-sequence accounting](https://docs.kernel.org/core-api/errseq.html).
This correction is not advertised as a throughput optimization.

The selected N1 executable is SHA-256
`f1a54f1653c1c90ae51416fa1f53474855cc20e1ea7ef52e0561af2c8c166aab`.
Its source passes **252 default N1 all-target checks, zero failed**, retaining
eight explicit helper/opt-in exclusions. Three applicable opt-in cases also
pass explicitly: real-process restart/clone/replacement recovery on the two
physical HDDs, io_uring positioned-I/O/publication, and direct-I/O page-cache
bypass/reopen. A new subprocess check kills the writer after acknowledged
single PUT in both part modes, then verifies the bytes and idempotent retry.
Another check verifies independent publication-root open descriptions. The two
existing workload verification/quantile checks, strict release N1/client lint
and formatting pass. These are N1-scoped checks, not a rerun of M1's suite.

The selected binary completes 1,089 measured operations on HDD and 1,379 on
SSD, with zero request/worker errors and all five operation classes overlapping.
HDD verification accounts for **210 live + 48 deleted = 258 identities** and
4,530,913,280 live bytes; SSD accounts for **314 live + 62 deleted = 376 identities**
and 4,559,220,736 bytes. Every live byte and deleted absence passes before and
after clean restart. All 634 identities are subsequently purged; both private
pools contain only their two member markers. No qualification service remains.

Final same-build baseline/selected comparisons use the same 20/40/20-second
windows. The table includes adverse observations. Latencies are milliseconds;
CPU/RSS cover the busy phase including drain, and throughput counts only
acknowledged multipart bytes inside the offered busy window.

| Busy observation | HDD baseline | HDD selected | SSD baseline | SSD selected |
| --- | ---: | ---: | ---: | ---: |
| Acknowledged multipart MiB/s | 10.0 | 14.6 | 49.2 | 41.8 |
| Normal small-upload p50 / p99 | 1,422.525 / 3,725.201 | 1,431.020 / 2,658.376 | 414.004 / 1,176.485 | 533.299 / 1,076.113 |
| Strict-plan small-upload p50 / p99 | 1,456.267 / 3,097.040 | 1,516.882 / 2,201.459 | 423.374 / 1,126.335 | 491.604 / 1,192.085 |
| Video-range p50 / p99 | 86.383 / 1,308.827 | 115.370 / 883.991 | 7.463 / 633.765 | 6.928 / 167.341 |
| Full-download p99 | 1,795.944 | 1,320.405 | 551.141 | 214.376 |
| DELETE + absence p99 | 2,359.530 | 1,897.294 | 1,023.081 | 665.644 |
| Normal part p99 | 2,724.230 | 2,337.936 | 968.227 | 778.503 |
| Strict part p99 | 3,244.139 | 2,746.022 | 1,182.830 | 914.738 |
| Finalize maximum; sample count | 279.219; 2 | 360.613; 2 | 151.557; 3 | 151.702; 2 |
| N1 CPU seconds | 16.40 | 16.22 | 19.93 | 17.19 |
| N1 sampled peak RSS MiB | 135.102 | 117.641 | 176.008 | 155.848 |

The unchanged baseline itself spans 10.0–16.2 MiB/s on HDD and 37.6–54.2 MiB/s
on SSD across these short mixed controls. Selected rates lie within those
observed ranges. That is context for storage-state variation, not a statistical
parity test or proof that the directory-handle correction causes either gain or
loss. The lower selected SSD throughput, higher mutation medians, higher HDD
video median and larger finalize maxima remain explicit. Two or three finalize
samples cannot establish object-level p99. No universal speedup is claimed.

An evidence audit checks all 18 baseline/experimental/selected campaigns: exact
pre/post-restart live hashes, catalog counts, expected deleted counts, marker-only
cleanup and zero recorded workload errors. Raw request quantiles are independently
recomputed. Only the two selected campaigns above run the retained final binary;
the audit does not relabel the rejected campaigns as selected-source results.

Evidence is mapped under `/srv/test-evidence/n1-busy-stability-20261116`:
source patches, immutable baseline/rejected/selected executables, Rust control
source, syscall observations, per-operation samples, resource series, test logs,
complete readbacks and marker-only censuses. The complete ten-case SSD evidence
archive has SHA-256
`40ad8c8c3edb4a1de09751f37ff5d91971c3f79f5de5006d54d67adb1ffd00cb`.
Raw credentials and private keys are excluded. No historical million-object
campaign is restarted or relabeled as qualification of these changes.
Physical host power loss, wider physical EC and system/Console qualification
retain their existing release scope. No tag or release is created.

## Same-hardware RustFS comparison, November 26

Packaged standalone N1 1.0.0-RC.2 and RustFS 1.0.6 ran sequentially on the
same Intel Core i5-12600K host with an Intel 240 GB SATA SSD (XFS) and a
Kingston 480 GB SATA SSD (ext4). Both ran one 1+1 pool on physically separate
members. The RustFS member format and external shard sizes confirmed the
layout, and both buckets reported `strict` after restart. N1 used native TLS
and io_uring. RustFS used S3 over HTTPS. Both clients verified their private
certificates. Fixtures, builds and reports were on SATA.

One Rust client ran N1, RustFS, RustFS, then N1. Each fresh fixture ran
1,024 fixed-size 4 KiB PUTs across 16 workers, followed by a 30-second
mixed window. The mixed window used four paced 4/64/256 KiB uploaders, two
64 MiB multipart uploaders with 8 MiB strict parts, and two 1 MiB range
readers. N1 timings include Begin and PUT. RustFS timings include the
complete S3 PUT. Latency includes requests that drain past the window.
Throughput counts completions inside the offered window.

| Run order | 4 KiB PUT ops/s | 4 KiB PUT p50 / p99 ms | Mixed small PUT ops/s | Mixed small PUT p50 / p99 ms | Mixed multipart MiB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| N1 A | 190.633 | 50.798 / 541.021 | 14.933 | 91.352 / 1,563.611 | 21.333 |
| RustFS A | 372.102 | 44.512 / 72.239 | 24.700 | 38.015 / 772.745 | 21.333 |
| RustFS B | 319.271 | 48.419 / 75.013 | 26.033 | 37.679 / 851.278 | 25.600 |
| N1 B | 178.032 | 55.193 / 460.835 | 15.533 | 111.829 / 1,059.990 | 25.600 |

RustFS processes more small PUTs and records lower small PUT p50 and p99
in both comparisons. Its mixed p99 still exceeds 500 ms. Both engines
complete the same amount of multipart data in each paired mixed window.
All four runs finish successfully and verify every acknowledged object
byte after process restart. Verification covers **6,604 objects** and
**4,045,619,200 bytes** across the four independent fixtures. These results
cover the SATA workload and process restart. The harness and individual
samples are retained with the private evidence.
