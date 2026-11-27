# N1 upload optimization and large-file qualification — November 13, 2026

**Completed for the profiles below.** The matched 15 GiB comparisons, small-object
controls, same-identity read control and requested attached 100 GiB run are
complete. All successful workload samples verify their payloads and purge their
identities. The final physical census finds only pool markers. N1's measured
checkpoint is held fixed for subsequent M1 profiling; this is not a declaration
that every operation improved or that RC.2 is release-qualified.

N1 remains the standalone LINK object engine. No M1 service, database or Console
is in the timed upload path. Existing the four-HDD reference system and
million-object results are preserved; the completed million campaign is not
restarted.

## Implementation

The baseline is the checkpointed receive02 implementation, rebuilt using the
same Rust 1.99.0 release profile as the candidates, with symbols retained for
profiling. LINK identity, pool geometry, SHA-256/CRC32C, O_DIRECT, io_uring,
admission, adaptive concurrency and durability barriers are unchanged.

The final candidate, `native04`, removes three avoidable memory operations:

- Frame construction initializes its canonical header and copies its payload,
  without first zero-filling that payload.
- The large-upload encoder constructs framed bytes directly in the aligned
  output buffers, retaining the existing two-turn pipeline and partial-page carry.
- Native multipart upload owns its socket in the activity worker and reads into
  the encoder directly, following native download's existing ownership model.
  It removes the body relay task and its two full-payload copies. Normal/batch
  body relays retain their tested path.

Immutable retry checks, partial-page isolation, checksums and persistence still
precede the same acknowledgements. The transport reauthorizes the request inside
its owner, limits reads to the exact declared body, drains rejected bodies and
measures sender stalls independently of disk waits. It adds no persistent index,
receipt, publication sidecar or background commit queue.

The earlier `copy02` intermediate passes integrity but is **not accepted on its
performance screen**. Its complete timings, including higher CPU time and slower
aggregate uploads, remain in the raw evidence. The SHA-256 backend probe finds
no useful improvement from substituting ring; no hash dependency is changed.

## Method and evidence

Local run root: `/mnt/test-hdd-a/n1-ssd-hdd-optimization-20261113-01`. SSD test
system run root: `/mnt/test-ssd/n1-ssd-hdd-optimization-20261113-01` on `SSD
test system`. Binaries, source snapshots, drivers, manifests, raw JSON, per-part
samples and logs are retained under those roots.

the two-HDD test system uses two ST1000LM049 HDDs, distinct XFS filesystems at
`/mnt/test-hdd-a` and `/mnt/test-hdd-b`, in a private 1+1 pool. The SSD test
system uses the Intel test-ssd XFS filesystem in a private 1+0 pool. Both use
256 KiB blocks, 48 MiB parts, four writers, native loopback TLS and strict
durable upload. No user fragment is created. No existing user data, service,
global governor, kernel perf policy or disk scheduler is changed.

15 GiB runs include init, the client's identity-journal flush, payload and
finalize. Full streamed SHA-256/length readback, repeated finalize and purge/404
are mandatory. Large-object p99 is not inferred from two objects; part latency
quantiles pool all individual samples. Raw process CPU, RSS and I/O counters
separate N1 from the client. Sampled RSS excludes host filesystem page cache.

The final comparison uses reference `before-a2`, two `native04` passes, and
reference `before-a3`. The unchanged `before-a2` sample also appears in the
earlier intermediate screen; it is one physical run, not two distinct tests.
The complete earlier samples remain available rather than being discarded.

SSD test system hardware counters attach only to the private N1 PID. These
profiled windows are diagnostics, distinct from unprofiled HDD acceptance
observations. Effective frequency is retained because CPU seconds can rise when
a process executes fewer cycles at a lower clock. This does not excuse an
observed regression or establish that every metric improves. Two-HDD-system
hardware counters require unavailable local sudo authentication; its process
CPU/RSS/I/O are retained without weakening the kernel's profiling policy.

During the HDD sequence, throughput changes substantially between consecutive
passes, including the unchanged reference. The results must retain that storage
state variation; a single fastest sample is not an acceptance result. Immediate
GETs also retain their actual cache condition and calculated member placement.

The benchmark's private size bound is raised to 128 GiB to cover the requested
100 GiB test. This is not an N1 object-size policy change. The large-run upload
TTL and process-local authentication lifetime are four hours, to avoid expiring
an otherwise valid transfer. The new client preserves upload measurements before
readback and records monotonic elapsed time for each readback progress report.
The 100 GiB client ran attached with visible progress throughout.

## Completed regression checks

The exact `native04` executable passes **269 N1/LINK release regression tests,
zero failed**, with eight explicit helper/physical opt-in exclusions across
14 target summaries. Strict N1/LINK Clippy and formatting pass. The separately
enabled physical XFS mincore/reopen witness also passes, verifying that complete
interior payload pages bypass the page cache.

Four new transport checks cover fragmented bodies and following control bytes,
rejection draining, caller cancellation with active shard/socket ownership, and
sender idle deadlines. Existing restart, immutable retry, direct-I/O boundaries,
purge, corruption and recovery cases remain included in the selected suite.

The corrected intermediate compilation attempts and setup failures remain in
their original logs. No failed preparation is counted as a completed workload.
No physical power-loss test, fresh million population, commit, push or release
has been performed in this continuation.

## Matched 15 GiB results

Each row pools two complete, verified and purged objects, 30 GiB of source data.
Throughput divides total bytes by total wall time; it is not the average of
individual throughput values. RSS is the highest sampled N1 RSS in that row.
CPU is N1 user plus system time during upload, excluding the client's CPU and
the later GET. These comparisons are against the preserved LINK checkpoint,
not a fresh N1 Lite/NCX comparison.

The multipart latency arrays concatenate each writer's ordered samples; they
are not a globally timestamped completion trace. This does not change pooled
quantiles. The separate per-GiB progress log supplies the cumulative transfer
timeline and window throughput.

| Physical profile / implementation | Upload MiB/s | N1 CPU s/GiB | N1 peak RSS MiB | Immediate verified GET MiB/s | Part p50 / p95 / p99 ms | Finalize ms, both objects |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| test-ssd 1+0 reference, profiled | 105.48 | 2.628 | 80.86 | 364.14 | 1,892 / 1,997 / 2,731 | 22.788 / 23.333 |
| test-ssd 1+0 native04, profiled | 108.92 | 1.911 | 73.07 | 363.47 | 1,894 / 1,984 / 2,039 | 23.207 / 1.032 |
| Two HDDs 1+1 reference | 67.66 | 6.623 | 125.13 | 43.91 | 2,523 / 4,695 / 5,510 | 23.865 / 29.669 |
| Two HDDs 1+1 native04 | 67.33 | 5.542 | 120.12 | 41.91 | 2,559 / 4,752 / 5,344 | 19.359 / 19.295 |

The SSD observations show 3.3% higher upload throughput, 27.3% less N1 CPU time
per GiB and 9.6% lower peak RSS. On the HDDs, upload throughput is essentially
unchanged (-0.5%), with 16.3% less CPU time per GiB and 4.0% lower peak RSS.
The HDD immediate GET aggregate is 4.6% lower and its part p95 is 1.2% higher;
those adverse observations remain part of the result. Individual HDD uploads
range from 59.60 to 77.36 MiB/s in the final candidate pair and from 60.46 to
76.80 MiB/s in the final reference pair. Two samples cannot establish a small
throughput difference statistically or qualify object-level p99.

The SSD counter windows record 195.99 billion versus 171.86 billion N1 cycles
for the same 30 GiB, a **12.31% reduction**. Instructions fall only 0.67%.
Effective clock rises from 2.502 to 3.028 GHz, so the full CPU-time reduction
cannot be credited to the implementation alone. The raw instruction/cycle
counts establish reduced work independently of simply counting fewer seconds.
Profiler overhead and hybrid-CPU affinity are explicit conditions of these SSD
observations, not production tuning changes.

## Same-identity read control

One verified 1 GiB identity was retained, read with reference/native04/native04/
reference binaries, then verified and purged. Each replay advises only that
identity's two shard files with `POSIX_FADV_DONTNEED`. It does not evict device
or controller caches, drop global caches, upload, finalize, repair or delete.
All four replays verify the same key, length, root and source SHA-256.

| Implementation | Combined read MiB/s | N1 CPU s/GiB | Individual MiB/s |
| --- | ---: | ---: | --- |
| Reference | 50.019 | 4.675 | 47.446 / 52.887 |
| native04 | 54.791 | 4.685 | 53.089 / 56.604 |

This control does not reproduce a reader-throughput regression. It does not
erase the slower immediate 15 GiB GETs or prove a universal read speedup; it
holds identity and physical placement constant, unlike those immediate reads.

## Small objects and tails

The first ABBA screen uses 64 objects per mode in each phase: 512 complete
lifecycles on each physical profile. Every 4 KiB source is uploaded, finalized,
finalized again, read and verified, purged and checked absent. Quantiles below
pool 128 individual uploads per implementation/mode. Upload latency is init +
part + first finalize; the other operations remain separately recorded.

| Profile / mode | Reference p50 / p95 / p99 ms | native04 p50 / p95 / p99 ms |
| --- | --- | --- |
| SSD normal | 2.887 / 3.802 / 7.249 | 2.922 / 6.909 / 7.802 |
| SSD durable | 3.632 / 4.335 / 5.368 | 3.688 / 6.929 / 9.917 |
| HDD normal | 90.561 / 1,067.112 / 1,253.445 | 91.651 / 1,099.469 / 1,363.591 |
| HDD durable | 130.401 / 1,293.365 / 1,684.467 | 123.754 / 1,471.002 / 2,048.229 |

These tails are adverse and must not be described as improved. To examine the
SSD tail with more samples, a separate ABBA control uses 1,024 objects per mode
per phase: **8,192 additional verified/purged lifecycles**, 2,048 upload samples
per implementation/mode. It runs on the separate SSD test system while the
two-HDD test system's 100 GiB readback runs, without sharing a physical disk or
service.

| SSD expanded control | Reference p50 / p95 / p99 ms | native04 p50 / p95 / p99 ms |
| --- | --- | --- |
| Normal | 2.913 / 4.100 / 7.533 | 2.926 / 4.300 / 7.091 |
| Durable | 3.661 / 5.285 / 7.959 | 3.756 / 5.880 / 7.979 |

After the 100 GiB run completes, a separate HDD ABBA uses 256 objects per mode
per phase: **2,048 additional verified/purged lifecycles**, 512 samples per
implementation/mode. Both candidate phases and both references pass. N1's code
and settings are unchanged; the earlier HDD distribution is retained separately.

| HDD expanded control | Reference p50 / p95 / p99 ms | native04 p50 / p95 / p99 ms |
| --- | --- | --- |
| Normal | 89.827 / 1,150.648 / 1,493.246 | 90.831 / 1,175.405 / 1,466.607 |
| Durable | 117.832 / 1,294.076 / 2,045.404 | 119.929 / 1,269.250 / 1,693.834 |

The expanded HDD control does not reproduce the earlier p99 regression. Normal
p99 falls 1.8% and durable p99 falls 17.2%, while medians rise 1.1% and 1.8%.
Normal p95 rises 2.2%; durable p95 falls 1.9%. Across both modes, N1 complete-
lifecycle CPU is 13.78 versus 13.88 seconds for 1,024 objects per implementation;
total lifecycle wall time is 441.872 versus 434.574 seconds. These include GET,
repeat finalize and purge, and are not upload-only throughput or CPU figures.
A second physical census after this extra control again finds only pool markers.
This larger sample improves confidence in the comparison, without proving equal
tails under every load or retroactively changing the earlier observations.

The larger sample shows nearly unchanged normal median, improved normal p99 and
similar durable p99, while durable median and p95 remain higher by 2.6% and
11.3%. N1 CPU per **complete lifecycle**, including GET and purge, rises from
1.611 to 1.689 ms for normal and from 1.724 to 1.968 ms for durable in this
unprofiled control. That is not an upload-only CPU measurement.

A further, separately labelled counter diagnostic repeats the same 8,192
lifecycles with scoped `perf stat`, without stack sampling. It records:

| N1 counter, both modes combined | Reference | native04 |
| --- | ---: | ---: |
| Verified lifecycles | 4,096 | 4,096 |
| Cycles per lifecycle | 6,524,149 | 6,362,137 |
| Instructions per lifecycle | 5,963,812 | 5,958,284 |
| Effective clock GHz | 2.882 | 3.111 |
| Total task CPU milliseconds | 9,271.99 | 8,377.30 |

The instruction count is effectively unchanged (-0.09%) and cycles fall 2.48%.
CPU time reverses relative to the unprofiled control, with a higher effective
clock. This demonstrates why CPU seconds alone are insufficient to attribute
a small-operation regression to new computational work. It does not cancel
the unprofiled tail results or establish that clock speed explains every delay.
There is no blanket small-object speedup claim.

Private benchmark request quotas are disabled for the small tests and 100 GiB
run. Production defaults, authentication and I/O admission are unchanged. The
first SSD small attempt hit its configured 429 budget and exited unsuccessfully;
its one remaining private fixture was removed offline after stopping that
private N1. The failed attempt and residue inventory remain in the evidence.
It is not included in successful sample counts.

## Attached 100 GiB result — passed

The source is **107,374,182,400 bytes**, not a decimal 100 GB fixture. The client
generates a bounded 48 MiB deterministic block and precomputes its whole-source
SHA-256 before the upload timer; it does not create a 100 GiB input file. The
two-member 1+1 pool stores redundant payloads on distinct physical HDDs.

| Upload observation | Value |
| --- | ---: |
| Parts / concurrent writers | 2,134 / 4 |
| Upload wall time, including init/journal/finalize | 1,387.401 s |
| Upload throughput | 73.807 MiB/s |
| Init | 1.162 ms |
| Client identity-journal flush | 24.633 ms |
| First finalize | 26.971 ms |
| Part p50 / p95 / p99 / maximum | 2,422.630 / 4,118.357 / 4,750.119 / 6,685.194 ms |
| N1 upload CPU user / system | 388.72 / 202.14 s |
| N1 total CPU / source GiB | 5.909 s |
| N1 average CPU, one-core scale | 42.59% |
| N1 peak sampled RSS | 121,492 KiB / 118.64 MiB |
| N1 swap before / after | 0 / 0 KiB |
| N1 process physical read bytes during upload | 1,068,486,656 |
| N1 process physical write bytes during upload | 214,843,887,616 |
| Owned I/O submissions / completions at upload boundary | 426,417 / 426,417 |
| I/O backend / fallback | io_uring / none |
| Full SHA-256/length-verified GET | 2,312.315 s / 44.285 MiB/s |
| N1 GET CPU user / system | 291.77 / 172.07 s |
| N1 GET CPU / source GiB | 4.638 s |
| N1 GET physical read / write bytes | 107,400,212,480 / 0 |
| Whole-run N1 peak sampled RSS | 121,540 KiB / 118.69 MiB |
| Repeated finalize | 31.900 ms, already finalized |
| Purge | 4,799.801 ms, two shards removed |
| Subsequent stat | 404 |

The N1 process's write accounting is about 2.001 times source bytes, including
both redundant copies, framing and filesystem activity. This is process I/O
accounting, not a measured device-internal write-amplification factor. Its CPU
and RSS do not grow in proportion to the larger source. Finalize remains a
small operation after durable payload acknowledgements; one timing does not
establish finalization p99 or prove power-loss behavior.

The streamed whole-source hash and exact length match. Repeated finalize keeps
the identity and returns already finalized. Purge removes both members' files
and subsequent stat returns 404. After the private server exits cleanly, each
HDD member root contains exactly its `.n1-arc-pool.json`, with zero shard,
staging, repair, terminal or symlink files. The separate test-ssd member likewise
contains only its pool marker. These censuses cover the private test roots,
not unrelated user data or legitimate server authentication/control files.

The attached driver runs from 19:54:20 to 20:57:01 on November 13, Asia/Manila,
including source-hash preparation, upload, readback and cleanup. The private N1
process, client and observer all exit. The observer records 3,758 interval
observations with zero swap-in/out; repeated vmstat headers and its initial
since-boot summary are excluded from that count. The N1 warning log is empty.
This is process/runtime evidence, not a host power-cut test.

| Transfer segment | Upload acknowledgement MiB/s | Verified GET MiB/s |
| --- | ---: | ---: |
| First approximately 25 GiB | 81.65 | 46.14 |
| Second approximately 25 GiB | 74.80 | 46.38 |
| Third approximately 25 GiB | 74.63 | 44.40 |
| Last approximately 25 GiB | 65.88 | 40.71 |

Upload boundaries use the actual acknowledged byte count at each threshold;
concurrent parts can overshoot a threshold slightly. GET segments are exactly
25 GiB. The 100 per-GiB upload windows range from 47.86 to 118.57 MiB/s; GET
windows range from 39.28 to 48.59 MiB/s. The slowdown across the transfer is
retained as an observation, without attributing an unmeasured cause or calling
the fastest window sustained throughput. The unrounded log and CSV are kept.

## Judgement and boundaries

The candidate removes measured upload CPU work and keeps observed HDD upload
throughput near the compiler-matched LINK reference. It retains integrity and
bounded memory on the completed checks. The read control and small
counter diagnostic prevent attributing every adverse wall-time sample to code,
but adverse tails remain visible and cannot be dismissed as noise by assertion.

These are configured four-writer workloads, not a saturation sweep proving the
maximum throughput of either device or a high-end server. Native loopback TLS
does not measure the Console, M1, a physical network or every protocol path. No
maximum-object-count, billion-object, all-topology or hardware power-loss claim
follows. The four-HDD reference system four-HDD candidate and M1's named
multipart geometry bridge retain separate acceptance work. N1 must remain fixed
during subsequent M1 profiling so improvements and costs can be attributed to
the correct engine.

The large-upload objective is supported on these SSD/HDD profiles: lower
observed CPU cost with essentially unchanged HDD upload throughput, plus
successful bounded-memory 100 GiB verification. Small-object tail concerns and
final the four-HDD reference system hardware acceptance remain explicit;
freezing a reference for M1 measurements does not mark those gates closed. No N1
runtime changes are made while M1's own transport, geometry bridge and database
costs are isolated.

The exact reference executable SHA-256 is
`fa785ff25299ff445e47d5da84a5fd37b42e19a6f9c381e99926d383c78b3915`;
native04 is
`c9cc0e6f5253723735cfbeec90a5419c6ff96ac0a2c865ef86994ba47ebe6156`.
The attached 100 GiB client is
`29fad0ec91c07be7841948154cc65b97fe7a4c8dafb6ab64d8e6d30aafd784f5`.
All 116 files under `object_engine/src` and `link/src`, and the workspace
lockfile/manifests, match the native04 build snapshot.
The source base remains `3d6e1a3e74d7c0c7a5bc7649b77a85067f379f8d` plus the
preserved uncommitted changes; these hashes identify the tested executables,
not a released or committed revision.

## Retained evidence

The run root's `reports/SUMMARY-native04.json` and `.md` retain the selected
comparison rows and their raw-file paths. `CONTROLS.json`,
`HARDWARE-COUNTERS-native04.json` and `SMALL-HARDWARE-COUNTERS.json` retain
separate controls. Every successful/failed attempt remains in its original
directory, including rejected intermediate candidates.

`reports/hdd11-100g-native04/results/PASSED.json` is the terminal 100 GiB
result; `upload-r1.json` is its earlier upload-only checkpoint, explicitly
marked readback-unverified. It is not rewritten into a later verification claim.
`progress.csv`, `window-summary.json`, the raw client/vmstat logs and process/
disk counters retain timing and resource evidence. `reports/final-census/`
records physical cleanup and the runtime source check. `checkpoint/` contains
the N1 source/build manifests; `artifacts/` retains the exact executables. A
verified 29 MiB portable source/binary/evidence bundle is preserved at
`/srv/test-evidence/portable/n1-ssd-hdd-optimization-20261113-01`, without
datasets, build caches or service secrets, so continuation does not require
test-ssd access.
