# N1 testing and results

N1 4.0.0 / storage format 4 remains development, not LTS. Results below are
dated evidence, not guarantees for every server. Older versions' passes do not
certify the current namespace/pool design. Personal test paths and credentials
are intentionally omitted.

## Upload/deletion hardening — October 6, 2026

Tested engine SHA-256:
7fedadeb07086ba965c8ceeb68099a16d9f54fbf2674e29c00dd20af542851f1.
Initial validation used new isolated four-member storage. The subsequent
live rollout is recorded below.

| Gate | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 248 engine, 2 native, 6 proof-runner, 10 shell, 7 CLI; 273 total |
| Upload tests selected independently | PASS: all 12; the previous isolated run failed because storage settings were uninitialized |
| All-target check, warnings-denied engine compilation, formatting/whitespace | PASS |
| Four-member 3+1 verification, 11m 45s | PASS: 25 checks covering deletion errors/retry, stale HTTP/native uploads, upload/delete/disconnect overlap, restart readback, recover-on-read, autonomous shard/NCX healing and fail-safe retry |
| Interrupted rename/move/trash/recover | PASS: 128 objects per operation remained reachable and hash-correct; retry completed; ordinary mutations left immutable files unchanged |
| Batch contracts | PASS: mixed results, empty/oversize fields, 32 same-key versions/max ULID, disconnect, explicit strict readback and 4,096/4,097-field boundary |
| Real LAN uploads | PASS: 24 trials, two repeats, 16 KiB/1 MiB objects, one-shot/batch and up to four clients; all 1,792 uploads hash-verified |

LAN measurements used direct certificate-verified HTTPS with keep-alive, no
SSH tunnel or Accel. Readback was outside the upload timer. Engine peak RSS
was **60.2 MiB**, versus **267.1 MiB** in the earlier equivalent LAN workload.
Peak/end file descriptors were 55/15. The complete fault-test cgroup peaked at
1.31 GiB, accounting for the harness and kernel memory as well as the engine;
that is not engine RSS or an uninterrupted-process memory-soak result.

| 1 MiB batch workload | Earlier throughput | Candidate throughput |
| --- | ---: | ---: |
| One client, concurrency 8, 32 objects | 6.45–15.15 MiB/s | 6.08–19.48 MiB/s |
| One client, concurrency 32, 32 objects | 6.13–7.64 MiB/s | 6.02–11.60 MiB/s |
| One client, concurrency 128, 128 objects | 5.09–5.25 MiB/s | 5.61–6.32 MiB/s |
| Four clients, concurrency 32 each, 128 total | 4.41–5.96 MiB/s | 5.45–10.30 MiB/s |

The 32-object/concurrency-8 batch took 1.64–5.26 s for the complete request,
not per object. Results are mixed: the 16 KiB/concurrency-128 case decreased
from 0.228–0.360 to 0.086–0.200 MiB/s, and sequential 1 MiB one-shot decreased
from 1.97–3.50 to 1.33–2.71 MiB/s. Shared HDD/OS state and non-interleaved runs
limit causal comparison; parallel publication and lower buffering do not
establish a universal speedup. A prior instrumented trace attributed about
97% of its upload interval to sync calls; it was not a LAN baseline.

These are bounded process-crash and readback tests, not physical power cuts or
a new long soak. During those tests, the live engine, Console, Accel and user
data were unchanged.

### Main-server rollout — October 6, 2026, 22:47 Manila

The tested engine above and its rebuilt matching backup helper are now live.
Previous binaries were retained for rollback. Startup was write-safe on all
four independent 3+1 members, with no warning-or-higher startup journal entries.
Before/after and post-smoke checks preserved 269 live objects, their namespace
snapshots, 3,708 immutable file paths/stats, 1,192 NDX hashes and three
version-pinned readback samples. Configuration, credentials and TLS hashes
were unchanged; existing backups were not modified.

The nine-object live smoke passed one-shot/eight-object batch upload, single
and eight-key prefix rename/move/trash/recover, exact-version readback and
immutable-file hashes. Its fragment was removed; a stale upload returned 404
without recreating it. Console returned verified HTTPS 200, and its nginx
process and Accel process were unchanged. This is deployment verification,
not a new LAN benchmark, full payload inventory hash or backup/restore drill.

## Deployed copy/delete candidate — October 6, 2026

Tested engine SHA-256:
4ff20f6bba63b1c6837d44835f4b81813895191830cc2e3d864201a7454720f0.
The original candidate tests did not deploy this build; production remained on
the October 5 build during those tests. The subsequent deployment is recorded
below. Console and Accel were not replaced.

| Gate | Result |
| --- | --- |
| Warnings-denied serial workspace/all-target tests | PASS: 241 engine, 2 native, 6 proof-runner, 10 shell, 7 CLI |
| Warnings-denied check, formatting and diff whitespace | PASS |
| Four physical rotational XFS members, 3+1, 1,000 objects | PASS: 32 same-key versions, 128 concurrent batch/native objects, coexistence, conflicts, NCX repair, cold degraded restart and mutation refusal |
| Live LIST, 73-entry pages | 3.63–5.04 ms, warm server-local observations |
| Ordinary mutation immutability | NDX/payload paths and hashes unchanged; deliberately corrupt NDX did not block NCX mutation |
| Process interruption during 128-key rename | Every object retained and hash-verified; retry completed deletion |

### Main-server deployment — October 6, 2026

The engine above and its matching backup helper were deployed to the main
server. Previous binaries were retained for rollback; configuration,
credentials, certificates, user objects and backups were preserved.

| Deployment check | Result |
| --- | --- |
| Locked release build; namespace regressions | PASS: engine matches tested SHA-256; 41 namespace tests |
| Before/after live namespace and readback | PASS: 32 objects, complete NCX replicas, identical snapshots and three version-pinned download samples |
| Existing immutable files | PASS: 1,204 file paths/sizes/mtimes/inodes unchanged; all 128 NDX SHA-256 hashes unchanged |
| Single-key and eight-key prefix rename/move/trash/recover | PASS: nine isolated test objects; version-pinned bytes and NDX/payload paths, sizes, mtimes and SHA-256 unchanged during mutations |
| Readiness and logs | PASS: four independent members writable, 3+1, write-safe; no warning-or-higher journal entries after startup |
| Console and Accel | Existing processes unchanged; Console verified HTTPS login page returned 200 |

Single-key rename/move/trash/recover took 228/121/100/75 ms. Eight-key prefix
operations took 228/1,170/176/305 ms. These are one observation per operation
over server-local, certificate-verified HTTPS with new connections, not a LAN
benchmark. Sampled engine RSS/HWM was 78,112 KiB after the smoke test; no
per-mutation memory or continuous-soak claim is made.

The first smoke harness used the wrong trash-response field and failed after
its mutation assertions. Its fixture was removed, the harness was corrected,
and a fresh run passed. That fixture was also removed; the final production
baseline comparison passed. This deployment does not close the remaining LTS
gates below.

### Network latency and memory

Mutation timing used real LAN HTTPS through an SSH tunnel, verified certificates
and keep-alive, without Accel. The hardware proof runner was server-local.
Both matched comparisons are retained; the favorable repeat does not erase
the initial regression.

| Single-object operation | Initial run, before → candidate median (6 samples) | Repeat, before → candidate median (12 samples) | Repeat candidate maximum |
| --- | --- | --- | --- |
| Rename | 135 → 447 ms | 117 → 63 ms | 1,007 ms |
| Move | 162 → 493 ms | 321 → 36 ms | 863 ms |
| Soft delete | 127 → 985 ms | 142 → 64 ms | 453 ms |
| Exact-ID recovery | 59 → 454 ms | 70 → 37 ms | 828 ms |

The initial 32-key rename/move/delete also regressed. A separate fresh candidate
run with 1 KiB and 8 MiB payloads measured rename/move/delete/recover medians of
81/78/58/33 ms. There is no universal sub-100-ms guarantee.

| 128-object prefix operation | Before | Candidate |
| --- | ---: | ---: |
| Rename | 16.30 s | 9.25 s |
| Move | 21.04 s | 10.08 s |
| Soft delete | 27.12 s | 14.80 s |
| Recover | 12.81 s | 2.19 s |

All 1,288 NDX/payload files retained their paths and SHA-256 hashes across those
cycles. Shared devices, old-first ordering and unreset OS/device state limit
causal comparison. A separate diagnostic trace observed a directory sync
taking 326 ms; tracing is not a latency baseline.

Sampled candidate RSS/HWM was 24,636 KiB in the initial matched phase and
77,372 KiB after fresh tests including uploads. These are whole-process samples
from different histories, not per-mutation allocations or a continuous peak.

SIGKILL 150 ms into a 128-key rename left 123 source and 18 destination names,
including 13 duplicates. Their union contained all 128 correct objects.
Restart/retry finished the remaining sources; 1,024 immutable files stayed
unchanged. This proves a process-crash case, not power-cut/cache-loss durability.

### Earlier accepted results

| Date / candidate | Accepted evidence and limits |
| --- | --- |
| Oct 5, 2026 / initial format 4 | Six four-XFS modes passed: 10,000-object flat namespace, mixed-pool expansion, HTTP/native 2 GiB restart/resume, exports, concurrent GC and 18-check backup/restore. LIST median/p95/max 10.29/15.82/19.17 ms; sampled engine RSS 99,304 KiB. These used the earlier deletion design, not the Oct 6 candidate. |
| Sep 12, 2026 / 3.5.1 | Supervised backup recovery: 11 checks. Separate N1+Accel fixture: 12 API checks; refresh-based search and version-pinned byte-cache hashes. Browser mock tests were separate, not a live end-to-end proof. |
| Sep 11–12, 2026 / 3.5.1 | Two four-member offline backup drills: 18 checks each; interrupted SSH replication and fenced/resumable restore passed. Metadata-heavy restore improved 781.73 → 163.27 s; small-fixture observations only. |
| Sep 9, 2026 / 3.5.1 | 221 workspace tests and all 11 four-HDD stages; full matrix 37m 6s. Covered concurrent upload/GC/TTL, exports, repair, 1,000-object storm and 192 same-key versions. No unexpected log faults; 24 expected injected-fault lines. |
| Sep 1, 2026 / 3.5.1 | 128 MiB export interrupted during build, restarted, range-resumed over LAN and hash-verified. Final shared-hardware build 14.72 MiB/s; LAN resume 106.59 MiB/s. |
| Aug 29, 2026 / 3.5.0 | ECS compatibility for five EC layouts; four-HDD N1 matrix 22 gates / 3,846 objects. Separate 4 GiB ECS encode 205.74 MiB/s and recoverable missing-member cases passed. |
| Aug 10, 2026 / 3.5.0 | Kernel ENOSPC returned 507 with no namespace, NDX or shard publication. Local 1+0 small-object batch measured 549.70 vs 228.52 objects/s; not multi-device fault-tolerance evidence. |
| Aug 8, 2026 / 3.4.0 | Four-HDD standard/durable multipart contracts and large-object hashes passed; LAN standard multipart 93.98 MiB/s end-to-end. One-shot samples varied substantially. |
| Jul 15–16, 2026 / 3.2.x | NOA physical-topology proof and corrected three-HDD 20-gate matrix passed. Historical sub-ms folder operations used real-directory rename, not current per-child flat semantics. |
| Jun 11, 2026 / 2.0.0 | Colocated/split-root proofs, multipart restart, repair and redistribution passed; peak RSS 158,476/125,652 KiB. Those retired layouts do not define format 4. |

### Failures and corrections

- Oct 6: deeper upload checks reproduced two implementation bugs: fragment
  deletion returned success despite a permission failure, and an unexpired
  upload token recreated a deleted fragment. Error propagation/retry aliases
  and shared upload admission corrected both; the isolated hardware rerun
  above passed. Batch previously buffered before I/O admission and forced
  batched durability; admission now precedes buffering and durability follows
  the existing one-shot configuration/options. Flush requirements were not
  removed. The old real-directory batch proof fixture was replaced with an
  oversize-field rejection check, and readback was removed from ingest timing.
- Oct 6: the requested eight-hour soak failed after 6m 43s, with 18 checks
  completed. Automatic healing raced whole-fragment deletion and tried to
  publish a fragment descriptor into a removed directory, triggering the
  read-only fail-safe. Final baseline validation never ran; this is not a
  passed soak. The fix adds per-fragment repair/delete exclusion and stale-scan
  revalidation, without changing ordinary NCX mutations. All 270 warnings-denied
  serial workspace tests passed, including four new lifecycle regressions.
  The corrected release build passed 31 hardware check executions in 7m 34s,
  including six concurrent fragment-deletion rounds during healing and final
  baseline hash validation. The fresh one-hour run completed in 59m 26s with
  103 check executions, two fault cycles, 43,093 API requests and 132 fragment
  deletions during healing. Final baseline hashes passed. Intentional process
  restarts were part of that run; it was not an uninterrupted-PID memory soak.
  The live service was unchanged.
- Oct 6: initial matched latency regressed; repeat still had large outliers.
  The first handoff also failed because the candidate binary was absent; rerun
  began only after both builds were available. The failed handoff is not a pass.
- Oct 6: an old test required tombstone suppression. It was replaced explicitly
  with the approved duplicate-name/retry contract, not presented as unchanged.
- Oct 5: flat GC removed expired trash but failed to count reclaimed logical
  versions. A reproducing regression was added; the corrected GC proof passed.
- Sep 12: whole-job SIGKILL killed the backup service's restart subprocess.
  Separate-cgroup OnFailure recovery was added; the fresh 11-check drill passed.
- Sep 7–9: GC could reclaim young/active uploads through old parents or last
  trash anchors; session-inspection failure could remove protection. Cancellation
  could retain finalizer state or release locks before blocking workers finished.
  These failures were reproduced and corrected before the final candidate-8 pass.
- Sep 7–9: repair deleted writer-owned staging; concurrent trash/TTL recovery
  exposed disappearing metadata. Preserve/retry/fail-closed regressions followed.
  A blackout interrupted an earlier matrix; ten observed stages were not a full
  pass. A 30-minute aggregate deadline was exceeded during redistribution; the
  later 90-minute matrix is a separately labeled run, not a passed old deadline.
- Jul 2026: order-dependent replica digests and per-replica timestamps caused
  real hardware failures. Sorted hashing/shared timestamps fixed them. Another
  harness incorrectly expected healthy reads to repair unused parity; a corrected
  data-shard fault selector passed a fresh 21-gate matrix.
- Early native testing: missing TCP_NODELAY caused approximately 82 ms delayed
  ACK/Nagle latency. Later loopback control-call results improved; they are not
  current LAN mutation benchmarks.

## Reproduce validation

```bash
RUSTFLAGS='-D warnings' cargo test --workspace --all-targets -- --test-threads=1
RUSTFLAGS='-D warnings' cargo check --workspace --all-targets
cargo fmt --all -- --check
git diff --check
cargo build --release -p n1 --bins -p n1-release-proof
```

Default parallel test execution has intermittently failed backup automation's
immediate lease-reacquisition assertions. A serial pass is not a parallel pass.
A warning-free full Clippy gate is also not claimed.

Run optimized hardware proofs only in new isolated children of mounted members,
with unique output directories/unused ports. Never reuse live storage roots:

```bash
n1-release-proof --binary /proof/bin/litiaina-n1 \
  --source-file /proof/source.bin --disk-roots /mnt/A,/mnt/B,/mnt/C,/mnt/D \
  --out-dir /proof/results --port 56001 --native-port 56002 \
  --data-shards 3 --parity-shards 1 --storm-objects 10000 \
  --trash-retention-seconds 3600 --flat-namespace-proof
```

Also run --pool-expansion-proof, --multipart-resume-proof, --export-proof,
--backup-proof, --gc-safety-proof and --noa-api-proof separately. Large multipart
acceptance uses 2 GiB or larger; small fixtures do not establish throughput.
An extended --gc-workload-timeout-seconds run must not hide a failed default
300-second performance gate.

Required regressions cover immutable pool enrollment, zero-decode LIST, same-key
lineage/cancellation, NCX-only mutation, destination-before-source barriers,
joined workers, conflicts, process restart, standard/durable upload boundaries,
kernel capacity failures, original-layout repair, GC protection and portable
backup recovery. Live Console/Accel/native/NOA/WebSocket tests must also exercise
flat key/prefix behavior and partial results.

## Remaining gates and evidence

Physical power-cut/fsync-stage testing, enrolled-member replacement/restore,
repeated failure masks, capacity exhaustion under format 4, long memory/FD soaks,
larger/cold namespaces, complete live consumer integration, and format-4
automatic-backup failure recovery remain release gates. Renaming an isolated
member directory is not physical device-loss or controller-cache testing.

This document summarizes accepted results, failures and remaining gates.
Raw hardware and deployment reports are local-only and excluded from version
control.

No LTS, failover, zero-loss, constant-memory-at-any-scale or universal latency
claim follows from these bounded results.
