# Litiaina N2 Performance and Resilience Benchmark

This report publishes measured N2 performance with explicit engine-format,
hardware, topology, and methodology boundaries. Correctness and failure
qualification are kept in [TESTING.md](TESTING.md) so benchmark results are
not mistaken for a second verification record.

## N2 3.4.1 Bounded Prefix-Rename Optimization

Evidence directory:
`/home/litiaina/DEV_TEST/n2-prefix-rename-optimization-20260812`.

The controlled comparison used four isolated N2 processes as one exact EC
`3 + 1` pool, one test root on each of four physical XFS HDDs, 245 objects,
and two deterministic NCNX replicas. Both binaries used the same host,
topology, paths, object set, and benchmark procedure. The hospital N1 services
and their data were not modified.

| Binary | Four complete rename samples | Mean |
| --- | --- | ---: |
| Unchanged N2 3.4.1 baseline | `6.026`, `6.810`, `6.590`, `7.782` s | `6.802` s |
| Optimized N2 3.4.1 | `2.071`, `1.989`, `2.570`, `2.004` s | `2.158` s |

The optimized path was `3.15x` faster, a `68.27%` mean latency reduction.
Every run finished with exactly 490 source NCNX replicas and zero stale
destination replicas after the final reverse rename.

The optimization preserves the copy/delete contract. It batches at most 128
logical keys per request, publishes and syncs every immutable destination
record before exact source removal, and admits at most four prefix batches and
128 planning reads globally per process. For this single-directory proof,
peer mutation envelopes fell from 980 per-replica calls to at most 16
node-batch calls. The required 490 file-data syncs remain; parent-directory
syncs are coalesced from 980 to at most 16. The remaining measured latency is
therefore the deliberately retained physical durability work, not planning:
the four operation traces spent only `0.03` to `0.08` seconds before entering
the running phase.

An injected holder outage after the operation entered `running` produced a
truthful failed first attempt without source deletion. After the holder
returned, retry converged all 245 keys to 490 byte-identical destination
replicas; reverse rename restored all 490 source replicas. Four simultaneous,
independent 64-object prefix renames also completed with 256 successes, 512
destination replicas, and zero stale sources. A pre-existing destination
returned a conflict rather than `503` and left all 490 source replicas intact.

The current N2 3.3 measurement is listed first. Earlier N2 results remain
below as historical hardware and engine evidence; they do not qualify the
format-v5 logical-slot binding and autonomous Reserve Set contract.

## N2 3.3 Four-Slot Plus Reserve Proof

Evidence directory:
`/home/wlzambo/DEV-TEST/N2-3.3-final-proof/N2-3.0-proof-20260727-044408`.

The retained proof-script filename identifies the flat-NCNX proof family. This
run used the optimized N2 `3.3.0` binary and format-v5 membership.

| Field | Value |
| --- | --- |
| Test date | 2026-07-27 |
| Logical topology | Four active logical slots plus one reserve process |
| Physical topology | Two hosts; processes are not independent host failure domains |
| Client path | Direct N2 HTTPS |
| Reverse proxy/gateway | None |
| Static object EC | 2 data + 2 parity |
| NCNX replication | 3 deterministic copies |
| Test object | 32 MiB |
| N2 binary SHA-256 | `592392682b2064f119fdc8d612fe87a1387dfb5926484b902c15151a7147ba43` |

### Measured data path

| Operation | Result |
| --- | ---: |
| 32 MiB upload | 21.13 MiB/s |
| Download through node A | 51.99 MiB/s |
| Download through node B | 71.87 MiB/s |
| Download through node C | 47.92 MiB/s |
| Download through replacement-bound node D | 55.73 MiB/s |

Every download matched the source SHA-256. Rates are end-to-end through one
coordinator, not aggregate cluster limits.

### Namespace latency

| Operation | Samples | p50 | p95 | p99 | Mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zero-decode LIST | 30 | 11.663 ms | 15.247 ms | 15.490 ms | 11.158 ms |
| Exact stat | 30 | 49.827 ms | 50.798 ms | 51.083 ms | 46.209 ms |
| Exact copy/delete rename | 20 | 80.920 ms | 1015.456 ms | 1015.456 ms | 183.698 ms |

Against the immediately preceding 3.3 pre-Reserve baseline on the same
two-host topology, LIST p50 improved `3.06%`, stat p50 increased `0.62%`, and
rename p50 increased `0.28%`. The single 32 MiB upload sample was `18.15%`
lower (`21.13` versus `25.81 MiB/s`). One sample is not a statistical
throughput conclusion; the regression is recorded rather than hidden and
requires repeated controlled hardware measurement before any no-regression
claim.

### Process cost

The finite proof sampled all active and reserve processes:

| Metric | Result |
| --- | ---: |
| Samples | 233 |
| Mean CPU | 6.815% of one logical core |
| Peak CPU | 83.000% |
| Aggregate sampled peak RSS | 230,664 KiB |
| Sampled reads | 98,732.00 KiB |
| Sampled writes | 100,619.92 KiB |
| Voluntary context switches | 365.90 |
| Involuntary context switches | 15.00 |

Hardware CPU-cycle counters were unavailable because
`kernel.perf_event_paranoid=4`; the proof did not change host policy.

### Failure-path measurement boundary

The same run stopped one active physical instance, observed a correct
pre-promotion write rejection, promoted the reserve while preserving the
logical slot, accepted writes during historical recovery, rejected a stale
retired-instance write, survived parity-count reads, and persisted the binding
through full restart. These are bounded two-host measurements, not evidence of
five independent failure domains, WAN behavior, an exabyte deployment, or a
hardware ceiling.

## Historical N2 3.2 Four-Node Proof

Evidence directory:
`/home/wlzambo/DEV-TEST/N2-3.0-proof-20260727-013543`.

The proof-script filename is retained for operator continuity, but this run
used the optimized N2 `3.2.0` binary, format-v4 membership, and clean 3.2
storage roots.

| Field | Value |
| --- | --- |
| Test date | 2026-07-27 |
| Logical topology | Four N2 nodes on two physical hosts |
| Client path | Direct N2 HTTPS |
| Reverse proxy/gateway | None |
| Static object EC | 2 data + 2 parity |
| NCNX replication | 3 deterministic copies |
| Test object | 64 MiB |
| N2 binary SHA-256 | `40baada7c09bc7463d1380668e39605399ecb994ca2c29a444381242ea052cba` |

### Measured data path

| Operation | Result |
| --- | ---: |
| 64 MiB upload | 33.11 MiB/s |
| Download through node A | 65.84 MiB/s |
| Download through node B | 64.48 MiB/s |
| Download through node C | 55.45 MiB/s |
| Download through node D | 53.29 MiB/s |

All four downloads matched the source SHA-256 exactly. These are end-to-end
rates through one coordinator at a time, not aggregate-cluster limits.

### Namespace latency

| Operation | Samples | p50 | p95 | p99 | Mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zero-decode LIST | 30 | 10.483 ms | 15.578 ms | 15.741 ms | 11.330 ms |
| Exact stat | 30 | 49.175 ms | 51.040 ms | 51.375 ms | 46.237 ms |
| Exact copy/delete rename | 20 | 80.405 ms | 1152.171 ms | 1152.171 ms | 200.734 ms |

Eighteen of twenty measured renames completed in at most 246.159 ms; two
durable multi-host mutations were 1.152 s and 1.242 s. The tail is recorded
as measured and is not hidden by the median.

### Process cost

CPU is Linux process CPU as a percentage of one logical core. RSS is the peak
resident set sampled during the finite proof.

| Node | Samples | Average CPU | Peak CPU | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| A | 52 | 12.35% | 117.00% | 193.60 MiB |
| B | 50 | 5.32% | 33.00% | 104.41 MiB |
| C | 55 | 6.16% | 45.00% | 96.07 MiB |
| D | 51 | 16.04% | 82.00% | 135.52 MiB |

Exact hardware CPU-cycle counters were unavailable because
`kernel.perf_event_paranoid=4`; the proof did not change host policy and
therefore makes no cycle-count claim.

### Measurement boundary

This is a finite correctness/performance baseline on four logical nodes
hosted by two machines. It does not establish four-host failure isolation,
long-duration stability, WAN behavior, an exabyte deployment, or a hardware
ceiling.

## Historical N2 3.0 Four-Node Proof

Evidence directory:
`/home/wlzambo/DEV-TEST/N2-3.0-proof-20260718-220508`.

| Field | Value |
| --- | --- |
| Test date | 2026-07-18 |
| Logical topology | Four N2 nodes on two physical hosts |
| Client path | Direct N2 HTTPS |
| Reverse proxy/gateway | None |
| Static object EC | 2 data + 2 parity |
| NCNX replication | 3 deterministic copies |
| Test object | 64 MiB |

All storage rates use binary mebibytes per second (`MiB/s`). These are
end-to-end application rates for the stated two-host test, not raw disk,
network, or aggregate-cluster limits.

### Measured data path

| Operation | Result |
| --- | ---: |
| 64 MiB upload | 46.49 MiB/s |
| Download through node A | 89.30 MiB/s |
| Download through node B | 129.74 MiB/s |
| Download through node C | 77.35 MiB/s |
| Download through node D | 71.45 MiB/s |

### Namespace latency

| Operation | Samples | p50 | p95 | p99 | Mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zero-decode LIST | 30 | 11.362 ms | 17.901 ms | 18.022 ms | 11.952 ms |
| Exact stat | 30 | 49.708 ms | 52.427 ms | 52.661 ms | 46.577 ms |
| Exact copy/delete rename | 20 | 84.555 ms | 689.807 ms | 689.807 ms | 191.790 ms |

Rename created no lifecycle-trash record and did not rewrite NCDX or object
payloads. The measured rename tail is preserved as a baseline; it is not
hidden or presented as already optimal.

### Process cost

CPU is Linux process CPU as a percentage of one logical core. RSS is the peak
resident set sampled during the finite proof.

| Node | Samples | Average CPU | Peak CPU | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| A | 38 | 12.10% | 56.00% | 160.69 MiB |
| B | 36 | 5.11% | 17.00% | 71.04 MiB |
| C | 41 | 6.46% | 30.00% | 103.82 MiB |
| D | 38 | 5.42% | 24.00% | 98.62 MiB |

| Node | Average read | Average write | Voluntary switches | Involuntary switches |
| --- | ---: | ---: | ---: | ---: |
| A | 1726.53 KiB/s | 1736.00 KiB/s | 2.32/s | 0.29/s |
| B | 1822.33 KiB/s | 1833.33 KiB/s | 2.05/s | 0.00/s |
| C | 800.88 KiB/s | 808.20 KiB/s | 1.59/s | 0.05/s |
| D | 864.11 KiB/s | 1736.21 KiB/s | 0.89/s | 0.08/s |

Exact hardware CPU-cycle counters were unavailable because the host kernel
used `perf_event_paranoid=4`. The test did not change that system policy, so
no cycle count is claimed.

### Measurement boundary

This is a finite baseline on four logical nodes hosted by two machines. It
does not establish a disk ceiling, WAN behavior, four-host isolation, NVMe
saturation, or exabyte scale. The completed N2 3.4.1 24-hour correctness and
chaos soak, including its single-host scope boundary, is documented in
TESTING.md.

## N2 1.0.0 Four-Node Proof

Evidence directory:
`/home/wlzambo/DEV-TEST/n2-maintenance-proof-20260708`.

| Field | Value |
| --- | --- |
| Test date | 2026-07-08 |
| Deployment | Four physical storage nodes; coordinator/client/gateway local |
| Final topology | Epoch 5, 4 readable/write nodes |
| Final erasure layout | 3 data + 1 parity |
| Gateway | `https://127.0.0.1:57912` |
| N2 binary hash | `c7830b9fe3d06f8f70d44c6bce2448b82930e7cbc6cc698331b4c025c34eb742` |
| Gateway binary hash | `e5660692273d396f93e3aff789916738263c7644e60dbae3d78114a536459d4c` |
| Qualification | Real-node repair, redistribution, namespace, topology, and monitoring proof |

### Correctness and maintenance

| Test | Result |
| --- | --- |
| N2 library tests | 116 passed; 0 failed |
| Gateway tests | 16 passed; 0 failed |
| Object repair | Missing data shard and missing parity shard repaired byte-exactly |
| Fragment repair | Cluster-aware fragment repair restored missing shard; failed=0 |
| Distributed repair | Scanned 25, skipped 25 healthy, failed 0 |
| Fragment redistribution | Forced alpha redistribution rewrote 7 objects; SHA verified |
| Distributed rebalance | Final run scanned 24, rewritten 4, skipped 20, failed 0 |
| 4 -> 3 -> 4 topology | Passed; 3-node mode wrote successfully, 4-node restored to 3+1 |
| 4 -> 2 degraded behavior | Existing repairable video read SHA matched; writes rejected by namespace quorum |
| Namespace mkdir race | 1 success, 23 conflicts, exactly one final directory |
| Namespace rename race | Exactly one destination won; no duplicate source/destination |
| Namespace collision | Conflict returned; source and destination both preserved |
| Trash/recover | Trash 200, live stat after trash 404, recover 200, live stat after recover 200 |
| Final monitor | All fragments healthy, current layout, zero missing shards |

### Final content verification

| Object | SHA-256 |
| --- | --- |
| `corpus/video-proof.mp4` | `8713f5d526baf2524074013ed2a362bddc78d9b831ad720f8260d49d8c4a0fa5` |
| `corpus/large-128m.bin` | `82f6c69ea1c4cb23fd570422f626ff6b1bebcb65a396419922844ce851baef0f` |
| `topology/three-live.bin` | `30278e9f36956d7ada519f5167793a65595fc0a8a7b377f14343cc9360464798` |

### Known boundary

A four-node 3+1 object layout is designed for one-node object-loss tolerance.
Testing two-node-loss object readability on four nodes requires 2+2. Also,
namespace writes under two arbitrary survivors require voter
migration/reassignment. Current N2 correctly rejects that write state instead
of accepting unsafe split-brain mutations.

## Earlier Two-Node Dentry-Quorum Rewrite

Evidence directory:
`/home/wlzambo/DEV-TEST/n2-namespace-rewrite-2node`.

| Field | Value |
| --- | --- |
| Test date | 2026-07-01 |
| Deployment | Two isolated physical nodes; this workstation also acts as client |
| Erasure layout | Fixed 1 data + 1 parity |
| Namespace layout | Stable directory IDs; 16 initial buckets per directory; 2-of-2 quorum |
| Client path | Direct HTTPS to either N2 node |
| Qualification | Two-node correctness and release-performance evidence, not production certification |

All storage rates use binary mebibytes per second (`MiB/s`). Network link rates
use decimal megabits per second (`Mb/s`).

### Correctness and lifecycle

| Test | Result |
| --- | --- |
| Unit suite | 112 passed; 0 failed |
| Fresh nested mkdir | First attempt `200`; four identical dentries on both voters |
| Cross-node object read | SHA-256 and byte comparison matched |
| Directory rename | `0.167 s`; one entry affected; descendant path moved |
| Immutable plane during rename | Zero shard or `.ncdx` hash differences on either node |
| Trash and restore | Same trash ID listed by both nodes; restore by ID returned exact bytes |
| Physical `.trash` directories | Zero on both storage devices |
| Restart persistence | 13 namespace files per node; identical content hashes; zero pending locks/intents |
| Conflicting rename | One `200`, one `409`; identical winner on both nodes; no stranded intent |
| One voter unavailable | List and rename both failed closed with `503`; no partial commit |
| TTL expiry | Trash entry removed; restore returned JSON `404`; object `.ncdx` and shards removed from both holders |
| Forbidden manual empty-trash route | `404` |
| Multipart with missing parent tree | Init `201`, part `200`, finalize `200`, cross-node read `200` |

The two-node namespace has a 2-of-2 quorum and therefore intentionally becomes
unavailable when either voter is absent. A three-replica deployment requires
2-of-3 and is the topology for one-node namespace availability.

### Release data path

The release build used 256 MiB split into four durable 64 MiB multipart parts.
The payload SHA-256 was
`a6d72ac7690f53be6ae46ba88506bd97302a093f7108472bd9efc3cefda06484`.

| Test | Throughput | Result |
| --- | ---: | --- |
| Durable 1+1 upload | **86.87 MiB/s (91.09 MB/s; 694.93 Mb/s)** | Four parts accepted; finalize 55 ms |
| Local-coordinator download | **312.24 MiB/s (327.40 MB/s)** | SHA-256 match |
| Remote relay download | **52.20 MiB/s (54.74 MB/s)** | SHA-256 match |

The download rewrite removed full-shard verification amplification. Before the
fix, each 1 MiB response chunk reread and reverified an entire 64 MiB shard.
Afterward, N2 reads only the contiguous checksum blocks covering the requested
range and verifies those immutable block CRCs. The local result measures the
storage/verification path. The remote result includes a data-holder-to-remote-
coordinator hop and the coordinator-to-client hop back to this workstation.

## Historical Four-Node 3+1 Results

N2 ran on four remote rotational-disk machines. The fifth machine at
`192.168.0.37` was deliberately excluded from storage and used only as the
client, route-only gateway, console host, and test coordinator.

| Node | Address | Storage root | Direct disk write/read | Client TCP receive/send |
| --- | --- | --- | ---: | ---: |
| A | 192.168.0.13 | `/mnt/DISK0` | 175 / 167 MB/s | 933 / 867 Mb/s |
| B | 192.168.0.18 | `/mnt/DISK2` | 90.8 / 98.0 MB/s | 934 / 868 Mb/s |
| C | 192.168.0.20 | rotational system disk | 135 / 130 MB/s | 933 / 897 Mb/s |
| D | 192.168.0.22 | `/mnt/disk-00` | 122 / 122 MB/s | 932 / 868 Mb/s |

Disk baselines used 1 GiB direct I/O. TCP values are independent `iperf3`
measurements in both directions. The client-side raw link ceiling was about
932-934 Mb/s, or roughly 111 MiB/s after decimal-to-binary conversion.

### Object data path

| Test | Payload | Throughput | Result |
| --- | ---: | ---: | --- |
| Four concurrent direct uploads | 4 x 512 MiB | **92.72 MiB/s aggregate** | Four healthy 3+1 objects |
| Four concurrent cold direct downloads | 4 x 512 MiB | **96.05 MiB/s aggregate** | Four SHA-256 matches |
| Direct upload, four coordinator samples | 1 GiB each | 80.58-95.21 MiB/s | All healthy 3+1 |
| Gateway upload, median of three | 1 GiB | 75.32 MiB/s | All healthy 3+1 |
| Direct cold download, median of three | 1 GiB | **64.45 MiB/s** | SHA-256 match |
| Gateway cold download, median of three | 1 GiB | **71.95 MiB/s** | SHA-256 match |

The final post-rollout concurrent run moved 2 GiB in each direction. Upload
took 22.09 seconds and cold download took 21.32 seconds. Aggregate application
throughput reached about 83.5% and 86.5% of the independently measured raw TCP
payload ceiling. Every upload retained durable shard sync, immutable metadata
publication, and namespace quorum commit.

This historical read optimization changed validation from serial whole-shard
reads to a bounded stripe pipeline with concurrent shard fetches and immutable
per-block CRC32 checks. A missing or corrupt data block was reconstructed from
parity before delivery. Direct cold read improved from 36.85 to 64.45 MiB/s
(+74.9%); gateway cold read improved from 35.55 to 71.95 MiB/s (+102.4%).
These historical CRC32 measurements do not qualify the N2 3.3 SHA-256,
format-v5 binding, and Reserve Set contract; N2 3.3 requires its own
performance evidence.

### Namespace and failure behavior

This subsection describes the retired pre-rewrite namespace and is preserved
only to explain the historical measurements.

| Test | Result |
| --- | --- |
| 200 concurrent durable `mkdir`, concurrency 16 | 200/200 acknowledged and listed; 10.86 ops/s |
| Namespace latency | p50 1,319 ms; p95 2,842 ms; p99 4,598 ms |
| Folder rename, trash, recover, purge | All passed with complete voter-union listing |
| One node stopped | Existing 3+1 object read byte-exactly as degraded |
| New write with one node stopped | Healthy immutable 2+1 object, byte-exact download |
| Namespace with one node stopped | Quorum writes and listing remained available |
| Deleted physical shard | Degraded status detected |
| Strict object repair | One shard repaired in place; zero failures |
| Immutable version during repair | Version ID preserved; restored path and SHA-256 verified |

The final namespace implementation unions collection reads across current
voters, globally paginates the merged result, chooses a source-holding voter
for rename/trash/recover/purge, and performs independent quorum RPCs
concurrently. Namespace correctness passed; durable mutation tail latency on
rotational disks remains the main measured performance hotspot.

### Gateway and console

The route-only gateway now has a dedicated WebSocket relay instead of sending
`/noa/v1/ws` through the ordinary HTTP proxy, which strips upgrade headers. A real
Chromium smoke test mounted the live fragment and opened File Manager at
1440x900 and 390x844. Both runs showed `Online (Real-time)`, rendered live
namespace contents, and recorded zero page errors, console errors, failed
requests, or HTTP 5xx responses.

The original console upload policy was separately found to be unsuitable for
N2's fsync-heavy multipart contract. A 512 MiB browser upload used 64 durable
8 MiB parts and reached only 13.90 MiB/s, producing visible transfer/fsync
cycles. The corrected console uses 256-512 MiB parts for large objects and
part-completion progress, because Chromium's granular XHR upload progress
callbacks materially throttled the data path.

| Real Chromium console upload | Before | Final default |
| --- | ---: | ---: |
| Object size | 512 MiB | 1 GiB |
| Durable parts | 64 | 2 |
| End-to-end throughput | 13.90 MiB/s | **95.72 MiB/s (100.37 MB/s)** |
| Payload throughput shown by console | stop/start | **101.97 MiB/s** |
| Result | Healthy | Healthy 3+1; SHA-256 verified on acceptance run |

An adjacent acceptance run reached 97.66 MiB/s end-to-end, or 102.4 MB/s
decimal, through the live route-only gateway with no local tuning. The browser
build keeps granular progress for small objects and uses an indeterminate
high-speed state plus completed average throughput for large objects. Failed
large parts remain resumable at the durable part boundary.

The complete evidence is under
`/home/wlzambo/DEV-TEST/n2-4node-3p1-20260630-135358`. It contains raw network
and disk baselines, machine-readable benchmark summaries, namespace latency,
destructive shard repair, chaos records, service rollout hashes, and browser
screenshots. The test deployment uses a self-signed/expired lab certificate and
disabled upstream certificate validation; production deployment requires a
valid certificate and authenticated trust configuration.

## Historical Five-Node Gigabit Results

| Test | Payload | Time | Throughput | Result |
| --- | ---: | ---: | ---: | --- |
| Exact final-SHA direct upload, node C | 1 GiB | 10.01 s | **102.28 MiB/s** | Healthy 2+1 object |
| Direct native upload, node C median (4 runs) | 1 GiB | 10.54 s | **97.19 MiB/s** | Healthy 2+1 objects |
| Direct native upload, node C best | 1 GiB | 10.38 s | **98.68 MiB/s** | Healthy 2+1 object |
| Route-only gateway multipart upload | 1 GiB | 10.89 s | **94.03 MiB/s** | Healthy 2+1 object |
| Direct native upload, node E | 1 GiB | 15.28 s | 67.00 MiB/s | Healthy 2+1 object |
| Direct distributed download | 1 GiB | 27.31 s | 37.50 MiB/s | SHA-256 match |
| Gateway distributed download | 1 GiB | 26.32 s | 38.91 MiB/s | SHA-256 match |

All upload timings include multipart part writes, durable shard sync,
immutable metadata publication, and namespace commit. The source was 1 GiB of
random data with SHA-256
`313496d15ef3cc18fc8b91e3378b77631b5e19cc537456284e10a02cfdba8d32`.
Parts were 512 MiB and large-object EC stripes were 4 MiB. Every reported
object finished healthy, and both download paths reproduced the source hash.

Raw TCP between the client and storage nodes measured 932-934 Mb/s, or about
111 MiB/s of payload. The exact final-SHA confirmation reached about 92% of
that measured ceiling; the repeated node C median reached 87.5%, and the
gateway reached 84.6%. The gateway is therefore not the primary upload
bottleneck on this topology.

### Optimization evidence

| Build or topology | Throughput | Finding |
| --- | ---: | --- |
| Incorrect 3+2 layout across all five nodes | 54.13 MiB/s median | Unnecessary fan-out and parity for the intended fixed 2+1 object set |
| Correct 2+1, buffered shard writes | 60.22 MiB/s best | Body streamed near line rate, then deferred `fdatasync` added about 8 s |
| 2+1 with streaming writeback hints | 63.90 MiB/s | Smaller sync tail, but writeback competed with ingress |
| 2+1 with aligned direct shard I/O, 1 MiB stripe | 94.52 MiB/s | Removed page-cache and final-flush serialization |
| 2+1 with aligned direct shard I/O, 4 MiB stripe | **97.19 MiB/s median; 102.28 MiB/s final-SHA confirmation** | Measured winner |
| 2+1 with aligned direct shard I/O, 8 MiB stripe | 81.00 MiB/s | Larger batching regressed latency and concurrency |

The final implementation uses the same aligned `O_DIRECT` writer for local
and distributed peer shards. It retains the final file sync, atomic rename,
directory sync, immutable metadata quorum, and namespace quorum. The speedup
does not weaken acknowledged-write durability.

Distributed reads remain below the upload result at 37-39 MiB/s. Integrity is
correct, but this is a known performance gap and is not presented as line-rate.

## Five-Node Topology

| Node | Address | CPU | Storage baseline | Link |
| --- | --- | --- | ---: | ---: |
| A | 192.168.0.13 | Intel Core i5-10300H | 106 MB/s write | 1,000 Mb/s full duplex |
| B | 192.168.0.18 | Intel Core i5-11400 | 94.6 MB/s write | 1,000 Mb/s full duplex |
| C | 192.168.0.20 | Intel Core i5-6400 | 64.0 MB/s write | 1,000 Mb/s full duplex |
| D | 192.168.0.22 | Intel Core i7-1165G7 | 93.3 MB/s write | 1,000 Mb/s full duplex |
| E / client | 192.168.0.37 | Intel Core i7-8550U | NVMe | 1,000 Mb/s full duplex |

The complete machine-readable evidence is under
`/home/wlzambo/DEV-TEST/n2-5node-gigabit-20260630-120700`, including raw
`iperf3`, disk, process, network, upload, status, placement, and integrity
records. Shard holders in the final summary were derived from immutable object
IDs resolved through namespace `@o_` pointers, not guessed from logical paths.

## Historical Three-Node Qualification

The remaining sections preserve the 2026-06-27 three-node, 100 Mb/s client
qualification and namespace chaos evidence.

### Test Topology

N2 ran as a fixed 2+1 erasure set across three physical machines. Each node used
one rotational test disk as a shared blob, immutable metadata, index, and
namespace root. The route-only gateway ran on node C.

| Node | CPU | RAM | Test storage | Link used |
| --- | --- | ---: | --- | ---: |
| A | Intel Core i5-6400, 4 cores | 7.7 GiB | 2 TB ST2000LX001 SATA, rotational | 1,000 Mb/s full duplex |
| B | Intel Core i7-1165G7, 4 cores / 8 threads | 11 GiB | 1 TB ST1000LM035 SATA, rotational | 1,000 Mb/s full duplex |
| C / gateway | Intel Core i7-8550U, 4 cores / 8 threads | 15 GiB | 320 GB ST9320325AS USB, rotational | **100 Mb/s full duplex** |

The cluster used the `n2://` native protocol for gateway-to-node and peer shard
traffic. Client requests entered through the HTTPS gateway. Multipart parts
were 64 MiB for the 512 MiB runs.

## Baseline Limits

The following probes establish what the same hardware could deliver without
the complete N2 write path.

| Baseline | Size | Throughput |
| --- | ---: | ---: |
| Raw TCP to node A | 512 MiB | 11.17 MiB/s |
| Raw TCP to node B | 512 MiB | 11.16 MiB/s |
| Raw TCP to two destinations concurrently | 512 MiB total | **11.16 MiB/s aggregate** |
| Sequential write probe, node A disk | 512 MiB | 60.23 MiB/s |
| Sequential write probe, node B disk | 512 MiB | 121.68 MiB/s |
| Sequential write probe, node C disk | 512 MiB | 62.20 MiB/s |

The raw transport ceiling aligns with a 100 Mb/s client link. Disk probes were
5.4 to 10.9 times faster than that measured network path, so storage media was
not the limiting resource in these upload tests.

## Upload Detail

### Single 512 MiB object

Native stream buffering improved the local-coordinator 2+1 upload from
8.57 MiB/s to 9.24 MiB/s:

| Build | Time | Throughput | Change |
| --- | ---: | ---: | ---: |
| Before 1 MiB native buffering | 59.73 s | 8.57 MiB/s | baseline |
| After 1 MiB native buffering | 55.42 s | **9.24 MiB/s** | **+7.79%** |

Coordinator placement matters on a constrained client path. A remote
coordinator adds another full-payload hop. The final release-candidate build
measured 8.70 MiB/s through remote coordinator node B, compared with 8.27 MiB/s
in the earlier remote-coordinator run.

### Concurrent objects

Eight multipart sessions uploaded 32 MiB each through the gateway:

| Cluster state | Recorded object layout | Objects | Total bytes | Aggregate throughput |
| --- | ---: | ---: | ---: | ---: |
| Three nodes online | 2+1 | 8 | 256 MiB | 7.76 MiB/s |
| Node B offline | 1+1 | 8 | 256 MiB | 6.88 MiB/s |

Every object was downloaded and compared byte-for-byte. The degraded run proves
that N2 continued accepting redundant writes rather than silently falling back
to nonredundant 1+0 storage.

The gateway was also probed with 30 simultaneous multipart initializations
after resetting route state. Coordinator selection distributed exactly 10 to
node A, 10 to node B, and 10 to node C.

## Read Results

| Read path | Size | Throughput | Result |
| --- | ---: | ---: | --- |
| Final-build distributed 2+1 read | 512 MiB | 10.56 MiB/s | SHA-256 match |
| Prior warm distributed 2+1 read | 512 MiB | 20.26 MiB/s | SHA-256 match |
| Read with an injected corrupt shard | 512 MiB | 20.07 MiB/s | Reconstructed, SHA-256 match |
| Read after shard repair | 512 MiB | 20.01 MiB/s | SHA-256 match |
| 1+1 object with its originally absent node still down | 512 MiB | 169.60 MiB/s | SHA-256 match |
| Same 1+1 object after a different shard node was stopped | 512 MiB | 134.81 MiB/s | SHA-256 match |

Read results vary substantially with coordinator locality and page-cache state.
The high 1+1 and prior 2+1 figures were locality and warm-page-cache influenced.
They show that the read path avoided an unnecessary remote bottleneck, but they
must not be treated as cold-disk throughput.

## Failure And Durability Results

| Scenario | Observed result |
| --- | --- |
| One of three nodes stopped | Existing objects remained readable; new objects used immutable 1+1 |
| Two of three nodes stopped | New upload rejected with HTTP 503; no unsafe 1+0 write |
| Original missing node restored, other shard node stopped | Existing 1+1 object still read byte-exactly |
| One shard corrupted | Status became degraded; read reconstructed exact bytes; repair restored healthy status |
| One real 1+1 shard removed | Read reconstructed exact bytes; one shard repaired; zero repair failures |
| Node B stopped and started three times | A pre-existing 2+1 object remained readable during every outage |
| Node A process killed with SIGKILL | systemd restarted it with a new PID; reads remained correct |
| All three storage services restarted | Representative 2+1, 1+1, renamed, and pinned objects remained byte-exact |
| Node lost during a 64 MiB 2+1 part | Part returned HTTP 503; retry succeeded; final object was healthy and byte-exact |
| Missing or trashed download through gateway | Correct HTTP 404 returned before body streaming |
| Multipart cancellation over HTTP/2 | HTTP 204 with zero body bytes |

Service-level availability was maintained with one node absent: reads,
namespace quorum, and new 1+1 uploads continued. An individual 2+1 request that
loses a required shard target in mid-transfer must retry; N2 does not claim that
the same TCP request continues transparently.

## Namespace Performance And Chaos Results

The namespace qualification used a fresh fragment and sent writes through all
three direct node entry points. Every acknowledged operation was checked after
process failure and service restart. Complete request ledgers were then
compared across voters, rather than relying only on API responses.

### Concurrent mutation benchmark

| Metric | Result |
| --- | ---: |
| Unique `mkdir` operations | 192 |
| Client concurrency | 24 |
| Namespace partitions touched | 63 of 64 |
| Successful operations | 192 of 192 |
| Wall time | 19.779 s |
| Aggregate throughput | **9.707 ops/s** |
| Mean acknowledged latency | 2,286 ms |
| p50 acknowledged latency | 2,229 ms |
| p95 acknowledged latency | 4,075 ms |
| p99 acknowledged latency | 5,387 ms |

These are durable quorum operations on rotational disks, not in-memory metadata
updates. Each successful mutation includes durable prepare state, a synced
request-ledger append, namespace materialization, and quorum replication.

The 192 paths mapped to preferred coordinators in a near-even 69/63/60 split
across nodes A/B/C. Stable paginated-list probes on the final build measured:

| Read path | Samples | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Direct node | 40 | 9.35 ms | 17.18 ms | 103.35 ms |
| HTTPS gateway | 40 | 8.56 ms | 16.45 ms | 47.20 ms |

The first run exposed a coordinator bottleneck: 169 requests succeeded, 18
returned HTTP 503 after coordinator timeout, and five hit client transport
timeouts. Namespace partitions all preferred node A when voter positions were
tied, and derived partition manifests were synchronously rewritten multiple
times per commit. The patched build rotates the preferred coordinator by
partition, always preferring a voter with a newer committed position, and
removes derived manifest rewrites from the commit hot path. The unchanged
workload then completed 192/192 operations with zero HTTP or transport failure.

### Namespace invariants

| Scenario | Observed result |
| --- | --- |
| 32 concurrent creates of one path | 32 HTTP successes; exactly one logical create |
| Two simultaneous renames of one source | Exactly one destination committed; source absent |
| 16 simultaneous recover requests | One winner; final path live and absent from trash |
| One voter available | Write rejected with HTTP 503; no namespace entry materialized |
| Stale voter after sequential node loss | Automatic prefix catch-up; failover write returned HTTP 200 |
| Coordinator killed with `SIGKILL` | systemd restarted in about 2 s; every acknowledged mutation survived |
| All three services restarted together | 192-entry paginated listing survived on every node |
| All 63 touched partitions reconciled | Ordered ledgers byte-equal; zero pending prepares |
| One voter ledger deliberately corrupted | Corrupt voter failed closed with HTTP 500 |
| Two healthy voters plus corrupt voter | Write remained available with HTTP 200 |
| One healthy voter plus corrupt voter | Write rejected with HTTP 503 |
| Last valid prefix restored on corrupt voter | Next write caught it up; all three ledgers became equal |

An interrupted client request is still allowed to be ambiguous: it may have
committed before the connection failed. N2 guarantees that an acknowledged
mutation remains committed; clients should retry unacknowledged mutations and
reconcile by path.

An earlier degraded test also completed 24 concurrent directory creates,
four-page directory listing, nested folder rename, trash, hidden-live
verification, recovery, and a post-recovery byte-exact object read while one
namespace voter was absent.

## Resource Results

Sampled resident memory during concurrent load:

| Process | Sampled RSS |
| --- | ---: |
| Gateway | 39-41 MiB |
| Node A | about 159 MiB |
| Node B | about 54 MiB |
| Node C | 212-280 MiB |

The warmed processes did not show monotonic growth during the bounded run and
were smaller after a full restart. This is not a substitute for a multi-day
memory-leak soak test.

## Historical Verification Scope

The historical source tree used for the results below passed:

- 263 automated tests including doctests;
- 0 failures;
- 8 intentionally ignored stress/SIMD cases;
- `cargo fmt --all --check`;
- `git diff --check`.

The historical four-node measured release binaries were:

- N2 node SHA-256:
  `5051a41b207f8575b40c770c9f46ea69302e6846b085c472e00001bb5952732f`
- N2 gateway SHA-256:
  `2cf23a136da9a2602e3ed2791f7513df6bd3de0d87ab4c0e495e1368cd5b31b3`
- N2 shell SHA-256:
  `37459cdf927fa8435dcb491aa7fb56d1711b8c4b3e1aab269bcdb0c0852fbbe5`

Historical measured release binaries were:

- Earlier object-path optimization benchmark N2 node SHA-256:
  `537766745f10595034d618c6576dbc3a4f723ff0bf82c8e5b2cc03938caa5738`
- Final five-node object-path, namespace, and direct-I/O N2 node SHA-256:
  `9c526909eac1a5643775b1e6c0b47385e98d34b5ddd9097f7922c38a67bba809`
- N2 gateway SHA-256:
  `af55a3485b8501762cc965cf4bd0b6ecb072151d7a1b94d042bdfa0f639b361a`
- N2 shell SHA-256:
  `37459cdf927fa8435dcb491aa7fb56d1711b8c4b3e1aab269bcdb0c0852fbbe5`

## Reproducing The Benchmark

Run the N2 3.0 finite release benchmark with isolated roots:

```bash
REMOTE_PASS='operator-supplied' \
  scripts/bench/run_n2_3_0_release_proof.sh
```

Build release binaries for a manual matrix:

```bash
cargo build --release -p n2 -p n2_gateway -p n2_shell
```

Deploy a fragment, then run the repository benchmark matrix:

```bash
OUT=/tmp/n2-benchmark \
FRAGMENT='<fragment-hash>' \
SECRET='<fragment-secret>' \
DIRECT_BASE='https://node-a.example:50001' \
GATEWAY_BASE='https://gateway.example:51002' \
NATIVE_URL='n2://node-a.example:50002' \
LARGE_MIB=512 \
PART_MIB=64 \
./scripts/bench/run_n2_benchmark_matrix.sh
```

For meaningful disk-ceiling results, use a wired client whose measured raw
throughput exceeds the aggregate storage throughput, record cold-cache and
warm-cache reads separately, repeat every run, and publish median plus tail
latency rather than only the fastest result.

## What These Numbers Do Not Prove

These measurements do not prove exabyte scale, unlimited horizontal scaling,
WAN behavior, NVMe saturation, multi-rack fault tolerance, or long-duration
production stability. Historical inode/quorum measurements do not qualify
the N2 3.0 flat NCNX engine. Larger independent node counts, faster clients,
rack-aware partitions, simultaneous media failures, and sustained soak runs
are required before making claims beyond the explicitly measured scenarios.
