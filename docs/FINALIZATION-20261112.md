# N1 finalization, latency and media qualification — November 12, 2026

The new finalization path performs **O(pool width)** logical work, independent
of object bytes and part count. The completed standalone benchmarks improve
1 GiB upload throughput and mean finalization on DATA_SSD 1+0, HDD 1+0 and
HDD 1+1. A 15 GiB HDD 1+1 upload finalizes in **19.87 ms** after its durable
parts are acknowledged. This is a standalone N1 qualification; M1 is outside
the measured request path. RC.2 remains unreleased.

## Implementation and durability

Completion is calculated during transfer. Normal mode folds acknowledged part
roots into bounded RAM state. Durable mode persists acknowledged payload and
staging ancestry, then records ordered completion in the **existing 96-byte
shard footer**. No upload receipt, per-object database entry, permanent deletion
record or additional on-disk sidecar is introduced.

The footer's completion flag means that every member's payload persistence
finished before the proof was written. A live successful-flush confirmation
allows finalization to reuse that proof. After restart or an uncertain footer
flush, finalization refreshes and persists the fixed-size footer before
publication. A footer visible in page cache does not prove that its own flush
succeeded. Failed persistence does not become a successful retry merely
because final filenames exist.

Finalization reads fixed-size headers/footers, renames the existing shard files,
removes empty staging directories and persists the affected namespace ancestry.
It does **not** copy payloads, iterate over part records or reread object bytes.
The member operations and directory persistence run concurrently, with all
started work joined even after cancellation or a peer failure.

Normal mode still drains unpersisted bytes when finalization requests durable
publication. Its elapsed time can therefore depend on outstanding I/O. Losing
normal-mode RAM completion returns an explicit conflict requiring paginated
upload-status reconciliation; finalization never hides an object-sized scan.
Durable completion has a fixed-size recovery proof. Already completed retries
validate immutable data without rewriting it.

Completion memory is bounded by 1,024 cached uploads and 262,144 stored roots
across them. Only 64 recent folded roots are retained per upload; out-of-order
holes share the global budget. The cache can evict acceleration state. Neither
an unbounded part vector nor object-sized memory destruction occurs at finalize.

The calculated `.uploads/<fingerprint>/shard.tmp` layout and isolated upload GC
remain intact. Finalized objects stay immutable; purge leaves no per-identity
retirement files. CRC32C/source SHA-256, EC, aligned direct I/O, io_uring,
adaptive admission and the existing operator configuration remain enabled.
M1 runtime/storage code, the Console and the native protocol are unchanged.

## Measurement method

Baseline: commit **`2d1ed2bb0aef9f9e6df7f1a24aedcfa543534468`**. Candidate:
the final runtime recorded as `candidate13` in the evidence. Both use Rust
1.99.0, the same optimized release profile and the **same Rust client binary**.
This compares the preceding LINK implementation with the finalization change;
it is not a new N1 Lite/NCX comparison.

| Placement | Host / filesystem / connection | Topology |
| --- | --- | --- |
| DATA_SSD | Main PC, i5-12600K, Intel SSDSC2KW240H6 SATA SSD, XFS | 1+0 |
| One HDD | Laptop, Ryzen 5 PRO 4650U, ST1000LM049-2GH172, XFS, USB UAS | 1+0 |
| Two HDDs | Same laptop, two independent XFS HDD filesystems over USB UAS | 1+1 |

The SSD/HDD comparison has equal 1+0 topology but different host CPUs and
SATA/USB paths. It describes these deployments rather than an isolated
media-only comparison. Clients and N1 use native TLS loopback on their
respective hosts, excluding LAN and browser overhead. Canonical main-PC
artifacts/builds use DATA_SSD; local artifacts and datasets use the HDDs.
Runtime binaries are copied into RAM. No filesystem was reformatted.

Large objects use 48 MiB parts, four concurrent writers and strict durability.
Each 1 GiB group contains four complete objects: **22 parts per object**.
Each 15 GiB object contains **320 parts**. Expected source SHA-256 preparation
is outside upload timing; upload includes init, a client-held identity-journal
fsync, parts and finalize. Repeated finalize, full verified GET and purge are
outside that timer. No M1 request is made.

Small-object runs use 256 sequential, verified/purged **4 KiB** objects per
durability mode and binary. Percentiles use nearest rank over individual
samples. Upload latency is init + part + finalize for the same object; stage
percentiles must not be added together. Separate balanced HDD confirmation
blocks alternate baseline/candidate order. The raw blockwise observations
remain retained rather than being replaced by the confirmation.
These small-object numbers measure the multipart lifecycle, not one-shot or
batch upload. The earlier MCH report measures those separate methods.

CPU is N1 process CPU time over upload, not whole-host usage. RSS is sampled
every 50 ms and excludes filesystem page cache. No global cache drop is used;
GET is immediate post-upload readback. A 256-sample observed p99 is not a
rare-event service guarantee. Four large objects do not establish object-level
p99; their mean and individual observations are reported instead.

## Large-object throughput and finalization

Throughput divides total source bytes by total upload wall time. MiB/s uses
1,048,576 bytes; one GiB is 1,073,741,824 bytes.

| Placement / object size | Baseline upload MiB/s | Candidate upload MiB/s | Baseline mean finalize | Candidate mean finalize |
| --- | ---: | ---: | ---: | ---: |
| DATA_SSD 1+0 / 1 GiB, four per binary | 134.99 | **182.35** | 39.15 ms | **6.54 ms** |
| HDD 1+0 / 1 GiB, four per binary | 103.82 | **109.25** | 62.41 ms | **22.24 ms** |
| HDD 1+1 / 1 GiB, four per binary | 64.25 | **76.47** | 65.35 ms | **19.53 ms** |
| DATA_SSD 1+0 / 15 GiB, two per binary | 100.51 | **101.25** | 121.24 ms | **23.95 ms** |
| HDD 1+1 / 15 GiB, one candidate | — | **73.84** | — | **19.87 ms** |

At 1 GiB, the candidate's measured throughput improves **35.08%** on SSD,
**5.23%** on HDD 1+0 and **19.02%** on HDD 1+1. SSD 1+0 is **1.67×** the
measured HDD 1+0 rate on these different hosts. At 15 GiB, SSD aggregate
throughput changes only **+0.74%**: the first candidate trial was slower,
while the confirmation pair reversed that order.

SSD 15 GiB finalize samples are **61.69 / 180.78 ms baseline** and
**22.81 / 25.09 ms candidate**. The first candidate and baseline 1 GiB samples
are included in every aggregate; no warm-up sample is discarded.
The 15 GiB HDD candidate's whole upload takes **208.02 s**, including client
journaling. Its part-transfer phase takes **206.60 s** and repeated finalize
takes **19.50 ms**. Its full SHA-256 GET takes approximately **347.2 s**.

The new finalize operation performs bounded work for both 22-part and 320-part
objects. This does not promise fixed wall-clock milliseconds across devices,
load, errors or normal-mode dirty-data persistence.

## Large-part latency, CPU and memory

Part samples are complete durable part requests with four writers. There are
88 samples per 1 GiB group, 640 per SSD 15 GiB group and 320 for HDD 15 GiB.
The final short part is included.

| Placement / size / binary | Part p50 | Part p95 | Part p99 | Mean N1 CPU / object | Peak N1 RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| SSD 1+0 / 1 GiB / baseline | 1,638.02 ms | 1,946.47 ms | 2,041.72 ms | 3.56 s | 78.57 MiB |
| SSD 1+0 / 1 GiB / candidate | 993.68 ms | 1,963.31 ms | 2,018.13 ms | 2.61 s | 76.96 MiB |
| HDD 1+0 / 1 GiB / baseline | 1,722.86 ms | 2,534.19 ms | 2,962.58 ms | 5.75 s | 73.71 MiB |
| HDD 1+0 / 1 GiB / candidate | 1,580.76 ms | 2,324.61 ms | 2,763.25 ms | 5.58 s | 78.32 MiB |
| HDD 1+1 / 1 GiB / baseline | 2,369.46 ms | 4,777.04 ms | 5,103.11 ms | 7.70 s | 112.30 MiB |
| HDD 1+1 / 1 GiB / candidate | 2,144.66 ms | 3,555.78 ms | 4,165.23 ms | 8.41 s | 100.82 MiB |
| SSD 1+0 / 15 GiB / baseline | 1,906.75 ms | 3,102.45 ms | 3,410.70 ms | 51.13 s | 86.80 MiB |
| SSD 1+0 / 15 GiB / candidate | 1,910.36 ms | 2,735.74 ms | 3,281.44 ms | 57.95 s | 87.80 MiB |
| HDD 1+1 / 15 GiB / candidate | 2,106.21 ms | 4,436.37 ms | 5,174.93 ms | 129.00 s | 137.00 MiB |

CPU does not improve in every workload: HDD 1+1 1 GiB uses about **9.2% more**
N1 CPU, and SSD 15 GiB about **13.3% more**. These are observed process deltas;
this run does not identify a verified cause. They must not be described as
universal CPU savings.

Immediate full verified SSD GET measures **383.65 → 390.87 MiB/s** at 1 GiB
and **355.18 → 296.61 MiB/s** at 15 GiB. HDD 1+0 measures
**42.32 → 42.26 MiB/s**. The 15 GiB SSD readback is an adverse observation,
not a read-path improvement. Cache state is uncontrolled and the read runtime
is unchanged. A physical regression check overlapped the final baseline HDD
1+1 GET, so that group's GET timing is excluded as an isolated comparison;
its upload and finalize had already completed before that overlap.

## Small-object latency

All values below are milliseconds, with **256 samples per row**. HDD 1+1 uses
the completed confirmation: four 64-object blocks per mode/binary, ordered
**ABBA / BAAB**, where A is baseline and B is candidate. Individual samples
are concatenated before computing percentiles; block p99s are never averaged.

| Placement / mode / binary | Whole upload p50 | p95 | p99 | Upload objects/s |
| --- | ---: | ---: | ---: | ---: |
| SSD 1+0 / normal / baseline | 4.02 | 7.35 | 8.71 | 214.25 |
| SSD 1+0 / normal / candidate | **3.08** | **4.42** | **8.58** | **299.69** |
| SSD 1+0 / durable / baseline | 4.64 | 8.61 | 10.64 | 186.20 |
| SSD 1+0 / durable / candidate | **3.87** | **4.67** | **5.84** | **251.79** |
| HDD 1+0 / normal / baseline | 83.61 | 708.38 | 1,003.02 | 7.15 |
| HDD 1+0 / normal / candidate | **63.66** | **270.80** | **761.51** | **10.60** |
| HDD 1+0 / durable / baseline | 108.38 | 949.11 | 1,024.28 | 5.35 |
| HDD 1+0 / durable / candidate | **82.02** | **778.64** | **986.48** | **7.50** |
| HDD 1+1 / normal / baseline | 73.28 | 1,424.95 | 2,162.13 | 2.44 |
| HDD 1+1 / normal / candidate | 89.53 | **1,199.69** | **1,412.00** | **3.78** |
| HDD 1+1 / durable / baseline | 91.89 | 1,327.32 | 2,063.96 | 2.58 |
| HDD 1+1 / durable / candidate | 119.96 | 1,357.34 | 2,146.98 | **2.75** |

Objects/s divides object count by the sum of measured upload durations. Full
verification, repeated finalize and purge are excluded from that rate but are
included in the separately retained whole-cycle resource measurements.

| Placement / mode / binary | Finalize p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| SSD 1+0 / normal / baseline | 3.21 | 5.19 | 6.64 |
| SSD 1+0 / normal / candidate | **2.43** | **3.10** | 7.57 |
| SSD 1+0 / durable / baseline | 2.40 | 4.48 | 7.75 |
| SSD 1+0 / durable / candidate | **1.00** | **1.27** | **1.81** |
| HDD 1+0 / normal / baseline | 81.22 | 705.63 | 1,001.06 |
| HDD 1+0 / normal / candidate | **61.04** | **268.21** | **758.50** |
| HDD 1+0 / durable / baseline | 63.50 | 758.51 | 949.82 |
| HDD 1+0 / durable / candidate | **25.50** | **45.98** | **534.23** |
| HDD 1+1 / normal / baseline | 69.69 | 1,355.15 | 2,062.01 |
| HDD 1+1 / normal / candidate | 85.71 | **1,196.87** | **1,408.27** |
| HDD 1+1 / durable / baseline | 49.63 | 1,007.32 | 1,345.68 |
| HDD 1+1 / durable / candidate | **28.76** | **451.77** | **1,002.45** |

This is **not an improvement in every latency metric**. In the balanced HDD
1+1 run, normal upload median rises **22.2%** while its p99 falls **34.7%**.
Durable whole-upload median rises **30.5%** and p99 rises **4.0%**, despite
faster finalization. The durable part median is **36.58 → 86.45 ms**; its last
part now includes the ordered completion footer persistence. That cost remains
in whole-upload timing. The measurement does not attribute all observed delay
to that barrier. SSD normal finalize p99 also rises **6.64 → 7.57 ms**, despite
an improved whole-upload p99.

The initial longer blockwise HDD 1+1 run had normal upload p99
**1,566.26 → 1,974.79 ms** and durable p99 **1,704.95 → 1,874.40 ms**.
Those adverse observations remain alongside the alternating confirmation.
The two runs show substantial tail variation; the confirmation does not erase
the original results or establish universally better tail latency.

Every stage has recorded p50/p95/p99 and raw samples: init, part, finalize,
repeated finalize, verified GET, purge and whole upload. The complete baseline
and candidate tables are in `reports/STAGE-LATENCIES.md`; the source values are
`reports/FINAL13-SUMMARY.json` and `reports/BALANCED13-SUMMARY.json` beneath
the evidence roots below. CPU/RSS for small-object runs cover the full
verified/purged lifecycle and are kept separate from upload-only CPU above.

## Correctness and recovery qualification

| Check | Result |
| --- | --- |
| Final complete Rust workspace, including added process-crash witness | **396 passed, 0 failed**, 56 explicit ignored/helper/hardware cases across 50 test targets |
| Release N1 suite before the test-only SIGKILL addition | **216 passed, 0 failed**, two explicit ignored cases |
| Physical XFS finalization suite for the final runtime | **9 passed, 0 failed** |
| Actual SIGKILL after durable part ACKs, DATA_SSD | **Passed**: reopen with empty RAM, finalize same key, repeat finalize, exact GET and purge |
| Strict N1 and Rust benchmark Clippy; workspace format check | **Passed** |
| Workspace documentation test compilation | **Passed**; zero runnable examples across six targets |

The dedicated SIGKILL fixture uses logical 3+1 members on DATA_SSD. It proves
process-crash recovery of that fixture, not four-device independence or host
power-cut survival. The existing direct-I/O/reopen checks passed independently
on both HDDs, and the final large-object runs exercise the aligned writer.
Every successful benchmark object passes full byte length/SHA-256 verification,
unchanged LINK identity, repeated-finalize checks, exact purge shard count and
post-purge 404.
The selected final benchmark matrix covers **4,096 small-object cycles** and
**29 large-object cycles**: 24 at 1 GiB and five at 15 GiB, totaling **99 GiB**
of large source payload fully read back. Latency-only blocks return after
their small-object checks; they do not add large-object samples.
Earlier candidate-12 objects and the recovered interrupted baseline are not
added to those counts.

New tests cover 4,101 real out-of-order parts, immutable retries, missing and
damaged completion footers, partial publication, cancellation/shutdown draining,
normal-mode explicit reconciliation, exact repair-footer preservation and
completion proof without a live successful-flush confirmation. A separate
**1 TiB geometry** case folds 21,846 synthetic roots with bounded memory;
it does not claim a physical 1 TiB upload. No new million-object population
or physical power-cut qualification was run for this change.

All task-owned benchmark services are stopped. The private SSD/HDD benchmark
members contain no remaining shard payloads or incomplete-upload identities;
their pool identity files and shared fanout directories remain. Earlier
campaign datasets and services are preserved.

## Evidence and retained failures

Local evidence: `/mnt/HDD1TB001/n1-finalize-home-20261112-01`.
Canonical main-PC copies: `/mnt/DATA_SSD/n1-finalize-home-20261112-01`.
The other HDD contains only the task's private second member in addition to
previous preserved datasets. `reports/` holds unedited phase results,
resource/disk counters and test logs. `source/` holds frozen Bash drivers,
Rust source snapshots and Perl aggregation; all workload/fault logic is Rust.
No Python is used.

Final runtime SHA-256:
`e69ecd5e97030f3cc67960d7a69781d3bd68c3bf65c94012bc35db07b3f8b4b7`.
Baseline runtime SHA-256:
`50a65e197d05f275ec09d3a8219cc9c9d3828b5f1aff80282a14aba87e42096c`.
Adding the SIGKILL test after the optimized runtime build changes test source
only; runtime source remains the recorded final candidate.

Earlier candidate-12 measurements remain historical evidence and are excluded
from the final tables. The final revision closes a footer-persistence gap:
an unconfirmed footer is refreshed before persistence/publication.

A baseline 15 GiB upload lost its idle control socket after 320 durable ACKs.
Its original key was finalized, fully read and purged with **zero reupload**.
That delayed finalize is excluded from the immediate-finalization comparison.
The client now opens a fresh control connection before starting that timer.
A running Bash-driver edit caused an orchestration exit between completed
phases; a separate frozen driver resumed only the remaining phase. Both
failures remain recorded, and no affected result is labelled a successful
uninterrupted upload.

Four existing regression failures exposed implementation mistakes during the
revision: incomplete-finalize state retention, conflicting read-only retry
state, byte-preserving repair flags and replacement footer restoration. All
were corrected without weakening the existing assertions. An additional
fixture was corrected to drop member descriptors before testing reopen;
retained descriptors had correctly triggered the exclusive member lease.

This qualification does not release RC.2, replace the pending MCH campaign,
prove physical power-loss survival or requalify million-object behavior of the
changed runtime. No commit, push or release is performed by this task.
