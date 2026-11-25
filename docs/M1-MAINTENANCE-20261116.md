# M1 mixed traffic with concurrent maintenance — November 16, 2026

**Passed on both minimum 1+1 profiles.** The same M1 and N1 executables complete
mixed named-object traffic with GC, monitoring and live metadata backups.
All recorded completed objects retain their bytes and versions after both
processes are killed and reopened. All recorded trashed names remain absent.
Both snapshots from each profile restore successfully. Final reclamation leaves
one pool marker per member and no shard or staged-identity files.

No N1, M1, LINK, ECS Core, database implementation, format or dependency changes
are made in this stage. The addition is an opt-in Rust integration workload,
split into orchestration, application traffic and observations under
[`metadata_engine/tests/noa_service/maintenance_load.rs`](../metadata_engine/tests/noa_service/maintenance_load.rs).
The established N1 checkpoint stays fixed.

## Hardware and artifacts

The [qualification profiles](TEST-PROFILES.md) identify the two systems:

| Profile | Host and memory | Independent members | M1 placement |
| --- | --- | --- | --- |
| Two HDDs | Ryzen 5 PRO 4650U, 6 cores / 12 threads, approximately 15 GiB usable RAM | Two ST1000LM049 1 TB HDDs; separate XFS filesystems, USB UAS | Database, staging, backups and evidence on the first HDD |
| Two SATA SSDs | Core i5-12600K, 10 cores / 16 threads, approximately 31 GiB usable RAM | Intel SSDSC2KW240H6 on XFS and Kingston SA400S37480G on ext4 | Database, staging, backups and evidence on the data SSD; the second SSD retains normal OS use |

Each N1 pool uses immutable 1+1 geometry and 256 KiB blocks. Sources remain in
the normal checkout; builds, fixtures and output use the designated test media,
with no NVMe build target or workload dataset. CPU policy, caches and ordinary
desktop activity are left unchanged. The different processors, filesystems and
competing activity make this a platform comparison, not an isolated media test.

| Artifact | SHA-256 |
| --- | --- |
| Fixed N1, `1.0.0-RC.2`, selected publication checkpoint | `f1a54f1653c1c90ae51416fa1f53474855cc20e1ea7ef52e0561af2c8c166aab` |
| Fixed M1, `1.0.0-RC.1` | `07cd00e0599f052feaf559e88f1d6f6481674e4ac371d16b66938f526a8077e7` |
| Selected Rust service workload | `633af951b985d3b4ea474759c002ec7e0f39d7af666d91e698dfff74f620e728` |

The source base is `0985721`. M1 runtime changes since its append-I/O report
are component version formalization; this stage adds no runtime change. Builds
use Rust 1.98.0, locked/offline dependencies and the repository release profile
with symbols retained. The copied executables have identical hashes on both
systems. Service logs confirm io_uring initialization with no fallback; the
previously qualified aligned direct-I/O implementation is unchanged.

## Workload and limits

One Rust scenario runs four **60-second offered windows**, in the order
**maintenance off, on, on, off**. All windows use the same foreground roles:

- Two small-file sessions cycle through 4 KiB, 64 KiB and 256 KiB strict
  completed uploads, verify each readback and rename every fourth object.
- One session uploads 128 MiB objects in 8 MiB parts, alternating standard
  `durability=batched` and durable `durability=strict`. It measures init, each
  part, finalize and repeated finalize separately.
- Two readers perform complete 8 MiB downloads and unaligned 1 MiB video-style
  ranges against the never-trashed seed library. Every byte is compared;
  complete reads also verify SHA-256 and the published version.
- A bounded deletion queue trashes half the new small objects after their
  initial readback. It does not directly delete LINK shards.

Small-file sessions pause 100 ms and the library readers pause 200 ms. The bulk
session continues while its window is open. These are paced closed-loop clients;
they establish neither maximum user count nor open-loop saturation capacity.
Traffic is loopback public NOA v1 HTTP into M1 and native NOA into N1. This is
not a browser, WAN or TLS-throughput benchmark.

During each maintenance-on phase, a separate client polls ordinary health,
pool capacity and fragment capacity every 250 ms; another runs scoped GC with
two seconds between completed jobs. One live MVCC backup is requested after
the first second. Client operation intervals confirm foreground/maintenance
overlap. Monitoring requests do not request a content/deep scan.
GC and backup samples include their start/status exchanges; operation counts
and overlap peaks are not exact HTTP/native request-frame counts.

The fixture uses 32 M1 upload slots, a 64 MiB upload-memory budget and 16 native
connections. N1 retains its fixture defaults, including 16 admitted operations,
a 256 MiB operation-memory budget and adaptive concurrency. Request quotas are
disabled in both fixtures to measure storage/maintenance contention. GC has
32-record batches, one object worker, 25 ms pacing and zero trash TTL. Automatic
GC is disabled to preserve the off/on controls. These are isolated test settings;
production defaults are unchanged. Existing automatic-TTL checks remain separate.

The client has bounded samples, a 16-item deletion queue, a 20,000-object limit
and a 20 GiB cumulative payload limit. It never buffers a complete large object.
CPU/RSS observations sample the two services separately every 250 ms. Client
hashing and byte comparisons are not charged to either server's CPU.

Started operations finish after the offered window. All completions and their
tails are retained. Multipart rates below count only acknowledged part bytes
completed within the original 60 seconds; they exclude later drain, small
uploads and read traffic. Latencies include the drain. Quantiles use nearest
rank; small cohorts do not establish a stable production p99.

## Integrity and recovery

| Result | Two HDDs | Two SATA SSDs |
| --- | ---: | ---: |
| Measured logical operations | 3,708 | 7,551 |
| Recorded request/worker errors | 0 | 0 |
| Created identities, including two seeds | 416 | 1,423 |
| Live / trashed before final cleanup | 216 / 200 | 739 / 684 |
| Completed 128 MiB objects | 19 | 57 |
| Live bytes verified after both-process SIGKILL/reopen | 2,587,373,568 | 7,742,042,112 |
| Concurrent snapshots restored and verified | 2 | 2 |
| Final physical files | Two member markers | Two member markers |
| Complete scenario wall time, including verification and cleanup | 475.90 s | 357.77 s |

The independent path/version/length/hash oracle is synchronized before the
process kills. Readback does not re-upload missing objects. The original database
passes offline verification before restarting. After recovery, every recorded
live object passes complete bytes/SHA-256/version checks, and every trashed name
returns 404.

Each live backup is restored into a different fresh directory and its database
tree is verified. M1 then serves that restored database and reads both unchanged
library versions through NOA. These are concurrent **metadata** snapshots, not
atomic whole-product snapshots: the test does not require a later-GC-purged
payload to reappear when old metadata is restored. Coordinated disaster recovery
retains its documented operator pause and separate qualification.

Finally, all remaining live names are trashed and scoped GC completes with zero
failures or deferred work. The final original database verifies and retains no
upload-to-LINK controls. A separate offline member walk requires exactly one
pool marker per member, no symlinks and no identity-specific files. These
qualification walks occur outside the traffic measurements.

The two hardware executions are repetitions of one new scenario, not two new
distinct test cases. The short smoke fixture is not pooled into these results.

## Observed performance

Small uploads below include their complete named upload, with readback measured
separately. Video requests include response consumption and byte verification.
The off/on labels describe maintenance; foreground session counts stay fixed.

| Profile / phase | Logical operations | Multipart MiB/s in offered window | Small upload p50 / p99 ms | Video range p99 ms |
| --- | ---: | ---: | ---: | ---: |
| HDD off 1 | 896 | 9.867 | 690.549 / 3,307.177 | 524.908 |
| HDD on 1 | 1,023 | 8.933 | 951.612 / 3,257.888 | 588.541 |
| HDD on 2 | 926 | 7.067 | 1,313.506 / 3,546.765 | 1,017.762 |
| HDD off 2 | 863 | 9.200 | 775.502 / 2,630.733 | 554.539 |
| SSD off 1 | 1,901 | 34.667 | 196.498 / 1,006.630 | 29.804 |
| SSD on 1 | 1,911 | 24.800 | 235.509 / 995.726 | 31.417 |
| SSD on 2 | 1,924 | 25.600 | 231.190 / 938.986 | 58.466 |
| SSD off 2 | 1,815 | 33.867 | 187.037 / 1,099.363 | 47.641 |

Maintenance consumes real service/storage capacity: bulk rates decrease in the
on windows, and some foreground latencies worsen. It does not stop foreground
progress or produce a request error in these runs. There is no binary change
between off and on, so these differences are not a new software regression.
The live database and trash backlog evolve across phases; this is an operational
off/on observation, not a statistically isolated overhead percentage.

| Service observation over traffic phases and drain | Two HDDs | Two SATA SSDs |
| --- | ---: | ---: |
| Total phase wall time | 283.204 s | 256.214 s |
| M1 CPU seconds | 23.331 | 32.978 |
| N1 CPU seconds | 44.339 | 75.493 |
| Highest recorded M1 process RSS high-water mark | 95.50 MiB | 117.36 MiB |
| Highest recorded N1 process RSS high-water mark | 134.48 MiB | 152.68 MiB |
| Peak simultaneous measured operation intervals | 8 | 7 |
| Backup completion observations | 715.680 / 662.864 ms | 105.718 / 131.462 ms |

CPU seconds come from each process's CPU clock. These are short workload
footprint observations, not a long-duration leak test. The 16 MiB seed library
fits in memory; no cache drop is performed and read rates are not cold-storage
measurements. GC timing includes all work selected by that job, including trash
from preceding off phases; it is not a per-object delete latency.

## Small-upload latency follow-up

The November 26 target is **50 ms p50 and 500 ms p99** for durable named
uploads of 4, 64 and 256 KiB through M1 to N1. The workload mixes these
uploads with multipart traffic, downloads, ranges, rename and trash. The
client now hashes the next 128 MiB multipart source on a blocking worker.
Previously, this preparation ran on an async worker and could delay the
client's observation of unrelated responses.

The packaged N1 1.0.0-RC.2 and M1 1.0.0-RC.1 executables ran four 20-second
offered windows, with maintenance enabled for the middle two. Each run uses
a 1+1 pool and keeps M1's database on one of its two drives. The SATA profile
pairs a 240 GB Intel XFS SSD with a 480 GB Kingston ext4 SSD. The HDD profile
uses two 1 TB USB UAS XFS HDDs. Shards and databases are off NVMe in both.
Percentiles use nearest rank across all small-upload completions, including
requests that finish during the drain.

| Profile | Small uploads | p50 ms | p95 ms | p99 ms | Exact restart checks |
| --- | ---: | ---: | ---: | ---: | --- |
| Two SATA SSDs | 676 | 54.472 | 344.344 | 548.407 | 695 identities, 2 restored snapshots |
| Two HDDs | 115 | 891.791 | 3,207.709 | 3,600.323 | 124 identities, 2 restored snapshots |

Both runs pass payload, version and absence verification after terminating
and restarting both engines, followed by a marker-only member census. The
SATA 4 KiB cohort meets both latency targets at 46.836 ms p50 and 457.554 ms
p99 across 228 samples. The 64 KiB cohort records 51.866 / 358.418 ms and the
256 KiB cohort records 66.642 / 673.357 ms. Neither pooled run meets both
targets. Four-drive acceptance remains open for this target.

Read-only telemetry records individual N1 filesystem flushes up to 222.999 ms
on SATA and 1,207.262 ms on HDD. The sampled foreground write-admission queue
stays below 0.1 ms in both runs. A shorter, instrumented SATA run with
15-second windows reaches 49.961 ms p50 and 408.724 ms p99 across 523
uploads. In that run, N1 PUT reaches 342.497 ms p99 and M1 reserve and
publish reach 63.637 and 76.354 ms p99, respectively. In the instrumented
HDD run, median N1 PUT is 504.947 ms and median M1 reserve and publish are
110.518 and 119.867 ms on shared storage. These measurements point to
durable storage waits without attributing each outlier to one syscall.
The benchmark correction changes neither packaged engine's runtime.

### Correlated persistence trace

The same SATA and HDD profiles ran again with M1 request stages and
selective N1/M1 syscall tracing. Both retained io_uring, the same strict
upload policy and their existing EC geometry. Traced timings are diagnostic.
The trace adds overhead and its writes share the test storage.

- On SATA, one N1 PUT took **1,147.814 ms**. After both shard renames, the
  ext4 member's `syncfs` took **1,059.452 ms** and ended 0.450 ms before N1
  returned. The XFS member's flush took **5.071 ms**. A second PUT had
  already renamed its shards while that ext4 flush was running. It waited
  for a subsequent **63.583 ms** ext4 flush and took **1,192.456 ms** overall.
- On HDD, one M1 handler took **4,316.382 ms**, with **3,203.263 ms** in N1
  PUT. After both shard renames, a member's `syncfs` took **907.819 ms** and
  ended 2.328 ms before PUT returned. M1's subsequent journal `fsync` took
  **991.110 ms** inside the named publication stage. The syscall trace does
  not separately time N1's pre-rename io_uring file synchronization.

The two traces identify post-rename filesystem flushes as a source of
small-upload tail latency. On the shared HDD, M1's separate durable
publication also contributes. Both traced runs complete post-SIGKILL
verification, two snapshot restores and the marker-only census across
639 SATA and 106 HDD identities.

### One-stripe publication experiment

The candidate keeps the sealed-shard data sync and replaces `syncfs` with
the existing pinned directory syncs for one-shot objects of at most one
stripe. It checks member bindings before acknowledgement. Larger one-shot
uploads, multipart and batch use their existing paths.

Both N1 executables are locked Rust 1.99.0 release builds from the same
source apart from the candidate change. M1 is the same packaged executable
throughout. Each case uses the corrected client and four 30-second offered
windows. SATA runs candidate first and HDD runs baseline first. These are
untraced runs on the same two 1+1 profiles above. Throughput counts multipart
bytes acknowledged inside the offered windows.

| Profile | Small uploads | p50 ms | p99 ms | Worst window p99 ms | Multipart MiB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| SATA baseline | 949 | 65.389 | 512.031 | 656.608 | 24.800 |
| SATA candidate | 1,026 | 55.542 | 507.343 | 748.919 | 25.000 |
| HDD baseline | 183 | 1,030.073 | 3,864.991 | 3,988.976 | 6.000 |
| HDD candidate | 197 | 796.438 | 3,490.224 | 3,975.543 | 6.133 |

The candidate passes 230 N1 library tests, with two opt-in tests excluded.
All four workload cases verify payloads, versions and absences after SIGKILL
and restart for **2,432 identities** in total. Each restores two snapshots
and finishes with only the member markers on disk.

The candidate lowers the measured median on both profiles. Its SATA worst
window has higher p99, and both profiles miss the latency targets. The
candidate remains an experiment.

## Monitoring observations and checks

The initial smoke fixture incorrectly required every cached health component to
remain `healthy` throughout load. It failed when observations became stale,
with no recorded data-request failure or reported database/disk error. Its log
and fixture are retained. The corrected test preserves all health responses,
checks reported storage/database errors separately and requires fresh healthy
observations after the workload settles. No engine health policy is changed.

Each full profile retains two top-level `degraded` observations: stale telemetry
followed by a pending object sample. The retained metadata/object sample ages
reach 19/18 seconds on HDD and 10/10 seconds on SSD, with a configured one-second
observation interval. Both services return to fresh `healthy` observations after
the workload. This is delayed/pending telemetry, not evidence of corrupted
payloads. The report does not claim continuous health freshness or complete
Console interpretation/alert acceptance.

The M1 and database library regression passes **58 tests, zero failed**, with
three explicit helper/opt-in exclusions; the storage child fixture is exercised
by its parent crash test. This is targeted library coverage, not a new full
workspace or all-target storage result.
The selected workload passes strict release Clippy and formatting. The existing
public HTTP/named-native/restart contract also passes against this exact pair
on the two-SSD fixture. Database/service regression logs and source/runtime
preservation checks are retained with the evidence.

## Evidence and release scope

Private evidence is mapped under
`/srv/test-evidence/m1-maintenance-20261116`: executable/source hashes, original
failed smoke, corrected smoke, all eight phase request/resource/health files,
independent ACK oracles, snapshot/restart checks, exact cleanup results and raw
logs. The two-SSD evidence archive is copied onto the local evidence HDD;
credentials, private configuration and workload datasets are excluded from it.
All workload and fault logic is Rust. No Python is used.

Core invocation, with fresh private parents on the selected media:

```sh
CARGO_TARGET_DIR=/path/on/test-media/target \
  cargo test --release -p m1 --test noa_service --no-run --offline --locked

N1_IO_BACKEND=uring \
M1_TEST_BINARY=/path/to/fixed/litiaina-m1 \
N1_OBJECT_TEST_BINARY=/path/to/fixed/litiaina-n1 \
N1_OBJECT_TEST_ROOT=/path/on/test-media/new-controls \
N1_OBJECT_TEST_FIRST_ROOT=/path/on/first-member/new-parent \
N1_OBJECT_TEST_SECOND_ROOT=/path/on/second-member/new-parent \
M1_MAINTENANCE_SECONDS=60 \
  /path/to/release/noa_service --ignored --exact \
  maintenance_load::mixed_traffic_with_gc_monitoring_and_backups_preserves_every_ack \
  --nocapture
```

This completes the recorded two-profile concurrent-maintenance/restart case.
Existing million-object results remain preserved. Physical power loss, final
four-drive qualification, broader saturation and full contract/Console/operator
acceptance retain their separate release gates. No release or tag is created.
