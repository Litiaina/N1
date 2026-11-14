# M1 profiling and resource qualification — November 13, 2026

RC.2 preparation begins from `9f0b922`, with the measured N1 runtime held fixed.
The [recommended schedule](RC2-RELEASE-PLAN.md) targets November 27 and a
November 25 go/no-go review. The measurements below qualify their stated
profiles; the remaining release conditions still apply.

## Isolation and identities

The SSD test system has an Intel i5-12600K, 16 logical CPUs. M1's database,
control, staging, binaries and build output use DATA_SSD, an XFS filesystem on
`/dev/sda1`. N1 uses a private **1+0** pool on the same SSD. This is a
co-located SSD measurement, not the split-node HDD or the four-HDD reference
system 3+1 profile. Listeners are private loopback HTTPS/native TLS, with
certificate verification. The existing interactive Console/database and original
million-object corpus are untouched. No drive is reformatted; the ENOSPC
fixtures below format only new, bounded loopback images on DATA_SSD.

| Executable | SHA-256 |
| --- | --- |
| Frozen N1 native04 | `c9cc0e6f5253723735cfbeec90a5419c6ff96ac0a2c865ef86994ba47ebe6156` |
| M1 before the buffer change | `7b106fa05541e1038dfe9e1f8357d9d5daab52009bd2e4855247b44e65376e11` |
| M1 buffer01 | `bb55ba31dbfc7b9e2104a72b393feaa0eb9e9dd6583524a540d2967f8ca41107` |
| Final selected-service/resource harness | `54708e1eb99b9f025f7eca412207af4425d80ba1800f499704bcd768c82ed9ac` |

M1 variants use Rust/Cargo 1.99.0, the unchanged lockfile and release profile,
with symbols retained and four build jobs. Their production builds both select
the binary and benchmark example. A guard caught a different binary produced
by the earlier binary-only build; that artifact is retained separately and
excluded from the matched comparison. N1 is copied from the already measured
executable; its 116 runtime files still match the frozen source manifest.

All timings exclude builds and fault campaigns. CPU affinity/governors and
kernel settings are unchanged. CPU frequency is not controlled or measured in
this matrix. Do not attribute every CPU-time difference to the source change,
or compare these 1 GiB runs directly with earlier 15 GiB affinity profiles.

## What changed in M1

The incompatible-part bridge formerly wrote each short received chunk to a
Tokio file, verified staged files with a 64 KiB buffer, then forwarded them
through `ReaderStream::new`. The locked dependency's default read capacity is
4 KiB. A 1 GiB bridged upload consequently incurred many small filesystem
worker handoffs even when staging data were already cached.

The change is confined to `uploads/staging.rs` and `uploads/coordinator.rs`:

- Coalesce received data before staged writes and use up to 256 KiB per buffer.
- Reuse one buffer across staged checksum verification, then replace it with
  an explicitly sized streaming reader for forwarding.
- Reserve room for both application and Tokio file buffers from M1's existing
  upload-memory semaphore. Busy memory returns 503 without a part ACK instead
  of waiting while holding an upload slot needed by a batch.
- Preserve staged SHA-256 verification **before** forwarding, identical retry
  checks, file/directory synchronization and durable metadata ACK ordering.

N1, LINK, the database format, namespace schema and public multipart identities
are unchanged. The bridge still forwards staged payload during finalize.
This improves a buffering cost; it does **not** make bridged M1 finalization
O(pool width), and it does not close that performance gate.

## Method

The Rust benchmark generates a bounded, deterministic repeating 1 MiB source,
preserving its logical byte offset across arbitrary external part boundaries.
Two regression tests check that reconstruction and reject duplicate/unbounded
workloads before creating a fragment. Diagnostic size bounds do not change
either product's file-size policy.

The initial baseline includes 100 strict 4 KiB named uploads at concurrency 1
and 8, plus 1 GiB one-shot/8 MiB/48 MiB multipart transfers. A later benchmark
revision uses sliced source streams to avoid allocating a whole client part.
The initial results remain separate; they are not mixed into its matched matrix.

The matched large matrix is **A B B A**, using reference `geometry-a2`,
`geometry-buffer01-b1`, `geometry-buffer01-b2`, `geometry-before-a3`. A separate
earlier reference `geometry-a1` is retained as a pilot. Case order reverses
between adjacent phases. Each binary has two 1 GiB objects per shape, four
multipart writers, strict durability and 256 KiB LINK blocks:

- 8 MiB parts: aligned, 128 parts.
- 8 MiB + 128 KiB parts: deliberately misaligned, 127 parts, requiring M1's bridge.
- 48 MiB parts: aligned, 22 parts.

This reproduces the bridge on a genuine 1+0 pool; it is not a simulated
four-member pool. Native standalone controls additionally verify/purge four
1 GiB objects against the same N1. Those controls include the standalone
client's identity-journal flush, and are not interchangeable API operations.

Every named object has full streamed SHA-256, length and version readback,
idempotent finalize, manual indexed GC and an offline database check. The final
member census contains only its pool marker. N1 quotas are disabled in this
private performance configuration; M1 quotas remain enabled with explicitly
raised capacity-test budgets. These are not proposed deployment defaults.

No global cache drop occurs. Process CPU has 10 ms tick granularity; RSS is
sampled every 50 ms and excludes host page cache. Part percentiles pool raw
individual samples. Two large objects cannot establish object-level p99.

## Matched large-object observations

Throughput divides total source bytes by total upload time. CPU is process
upload CPU per GiB; it excludes the subsequent GET and the client.

| External part bytes | Before / buffer01 MiB/s | Before / buffer01 M1 CPU s/GiB | Before / buffer01 N1 CPU s/GiB |
| --- | ---: | ---: | ---: |
| 8,388,608 | 94.55 / 94.64 | 1.465 / 1.335 | 1.845 / 1.800 |
| 8,519,680, bridged | 43.75 / 47.92 | 7.510 / 3.205 | 1.945 / 1.640 |
| 50,331,648 | 106.89 / 130.53 | 1.915 / 1.295 | 2.630 / 1.690 |

The bridge's observed M1 CPU cost falls **57.3%**, while aggregate throughput
rises **9.5%**. The unchanged aligned paths also vary across phases; clock,
allocation and cache conditions prevent assigning their entire differences
to this bridge change. All individual samples remain available.

| Operation | Before, both samples | Buffer01, both samples |
| --- | ---: | ---: |
| Bridged finalize, seconds | 10.809 / 10.979 | 8.574 / 10.883 |
| Aligned 8 MiB finalize, ms | 46.218 / 3.185 | 46.723 / 3.332 |
| Aligned 48 MiB finalize, ms | 3.340 / 47.340 | 47.337 / 47.376 |

The large bridged finalize cost remains. The candidate's second sample is close
to baseline; no near-instant bridged-finalize claim follows. The bridge's
maximum sampled M1 RSS rises from **20,776 to 27,192 KiB**, a **6.27 MiB**
increase. Its 254 individual part samples improve p95/p99 from
985.553/1,602.748 to 622.620/767.039 ms. Aligned 8 MiB part p99 instead rises
from **565.156 to 600.417 ms**. These adverse observations are retained.

## Small-object control

A separate A/B/B/A performs three repeats per phase, 100 strict 4 KiB objects
at each concurrency and one additional 4 KiB one-shot per repeat: **2,412
verified/GC-reclaimed lifecycles**, with **600 latency samples per binary and
concurrency**. The additional one-shots are excluded from the distributions.
The ordinary one-shot path does not use the modified bridge.

| Concurrency | Before upload p50 / p95 / p99 ms | Buffer01 upload p50 / p95 / p99 ms |
| --- | ---: | ---: |
| 1 | 7.790 / 10.228 / 28.381 | 7.837 / 9.944 / 28.430 |
| 8 | 18.201 / 33.905 / 841.154 | 17.909 / 65.287 / 369.552 |

Sequential aggregate throughput is **116.14 / 114.45 objects/s**; concurrent
throughput is **187.89 / 243.82 objects/s**, before/candidate. Candidate
concurrent p95 is worse despite lower p99. The raw phase variation does not
establish a universal improvement or close sustained saturation acceptance.
Per-process CPU, every read distribution and all samples remain in the summary.

## Actual ENOSPC and recovery

Each target has a fresh **512 MiB XFS filesystem in a bounded loopback image
on DATA_SSD**. The driver checks the loop backing file before formatting it.
The Rust workload refuses to fill filesystems outside 16 MiB–1 GiB and requires
its external ACK journal to occupy a different filesystem. It writes until the
kernel returns ENOSPC; this is not an injected error value or a full host disk.

Object-full puts the N1 member on that filesystem, with M1 and its ACK journal
on healthy DATA_SSD. Metadata-full puts M1 there, with the object member and
ACK journal on healthy DATA_SSD. The workload preserves the original seed and
every subsequent successful ACK, checks bounded failure responses, kills both
processes, frees pressure, reopens and verifies every acknowledged version and
payload. Unacknowledged attempts must recover wholly present or absent. A new
write/read and an offline M1 integrity check must then succeed.

| Final buffer01 test | Prior ACKs verified | Attempted writes checked | Offline records verified | Result |
| --- | ---: | ---: | ---: | --- |
| N1 object filesystem full | 10 | 10 | 101 | Passed; capacity error 507 |
| M1 filesystem full | 29 | 29 | 251 | Passed; storage failure refused publication ACK |

The baseline independently passes both cases (101 / 253 offline records).
Different recovery table counts do not imply lost acknowledged objects; each
run verifies its own exact ACK oracle and permits uncertain transactions to be
wholly present or absent. The oracle files remain external to the pressure
filesystem. Every temporary filesystem is unmounted and loop detached.
These are ENOSPC-plus-process-crash checks, not physical power-loss tests.

## Verification and retained attempts

- Complete release-profile workspace, all targets: **418 passed, zero failed**,
  63 explicit opt-in/helper exclusions across 50 target summaries. This includes
  the library and benchmark counts below; they are not additional default tests.
- Strict workspace/all-target Clippy and workspace formatting pass. All **355
  Rust source files** match the tested SSD test system worktree manifest.
- M1 release library suite: **49 passed, zero failed**, two explicit opt-in exclusions.
- Rust benchmark source/shape checks: **2 passed**; strict benchmark Clippy passed.
- Candidate service checks: **7 passed** — the existing public HTTP/native scenario,
  durable indexed resume, lost-staging reconciliation/cancel, resumable listing,
  six in-flight crash combinations, monitoring/metadata backup and the new bridge case.
- The bridge case uses the supported minimum **1 MiB** upload budget. Two stalled
  senders reserve it; a third gets 503 without an ACK. Disconnect releases the
  reservation, identical retry succeeds, conflicting retry fails, durable progress
  survives both-process restart and staged corruption yields 422 before publication.
  Replacing the staged bytes correctly then permits exact readback and repeated finalize.
- Both final-candidate ENOSPC cases pass, in addition to their baseline witnesses.

The same seven cases also pass on the two-HDD test system with **two independent physical
XFS HDDs in 1+1**, using the exact frozen N1, buffer01 M1 and service executable
listed above. M1/database/control share HDD1 in this functional profile. It is
not an SSD-metadata performance measurement. The serial indexed-resume case
takes 264.43 seconds while acknowledging 1,028 small parts across both modes;
its zero-object-I/O status assertions and complete post-crash readbacks pass.
These repeated cases add hardware coverage, not seven additional distinct tests.
The HDD driver, logs and executable hashes are under `reports/hdd-services`.
No test process remains after completion.

The first resource launch copied its executable with root-only ownership and
exited before an engine case ran. Its log is preserved. The bridge fixture
initially requested an unsupported 64 KiB budget; M1 correctly rejected startup.
Its next revision incorrectly assumed staging had only one subdirectory; the
corrected fixture identifies its unique version/part file. Both failed harness
attempts remain excluded from pass counts. The first arithmetic summary reused
an array reference for phase labels; the corrected summary preserves the proper
large/small provenance, and its earlier output is retained separately.

## Evidence and remaining work

SSD test system root: `/mnt/DATA_SSD/n1-m1-acceptance-20261113-01`. Local
evidence mirror: `/mnt/HDD1TB001/n1-m1-acceptance-20261113-01`.
`reports/SUMMARY.json` aggregates raw phases; the Bash drivers and Perl
arithmetic are under `source/`. Workload and fault logic are Rust. `resource02`
and `resource03` identify baseline/candidate ENOSPC runs; external ACK/PASSED
journals are under their corresponding `resources02/03/evidence` directories.
The local mirror additionally retains the independent two-HDD service logs.

The remaining work includes moving compatible work earlier in M1's bridged
upload lifecycle, final-candidate HDD/the four-HDD reference system coverage,
sustained mixed-load and stalled-I/O/host-memory-pressure tests, complete
contract/Console acceptance and controlled physical power interruption. The
existing million-object results retain their original scope. No new population,
release or power cut occurs here.
