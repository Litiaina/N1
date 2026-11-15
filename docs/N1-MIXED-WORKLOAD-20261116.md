# Standalone N1 mixed file-service workload — November 16, 2026

**Integrity and functional concurrency passed.** Standalone N1
completed uploads, downloads, video-style ranges, reconnect/resume and deletes
together without M1. All 513 created identities were accounted for. The busy
profile exposes substantial write/delete tails and slower large uploads; it
does not justify a general claim of excellent mixed-workload performance.

No N1, M1, LINK, ECS Core or database runtime code changed. This stage adds a
Rust workload client and records the existing N1 binary's behavior.

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
