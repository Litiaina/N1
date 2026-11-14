# M1 concurrent status and resource recovery — November 14, 2026

An indexed M1 multipart status request no longer waits for an active part's
request body or disk write. It shares the existing per-upload read lease with
part writers and observes committed database ACKs. Reconciliation retains the
exclusive lease and reloads its plan after acquiring it. Finalize, cancellation,
GC claims, late-ACK fences and publication ordering retain their existing rules.
There is no new progress cache, payload scan or per-object bookkeeping in N1.

The kernel-delay test reproduced this M1 control-path blockage independently
of N1's object placement or metadata database throughput. The previous M1
times out on a two-second status deadline. N1's health endpoint remains
responsive and competing N1 admission correctly returns 503 during the fault.
The corrected M1 returns the prior committed ACK while its next part is still
incomplete. A status response never promotes an unacknowledged part.

## Candidate and regression results

Selected M1 `status12` SHA-256:
`c793e657f8a81d4029172b8660c417e3b72eb2c2e6a3376ba6952b2bae304458`.
Previous M1 `singlepart09`:
`c6a3d382f8a7f1720d69c80537333e76b971128be36b6d650f8cb08fd4b09042`.
N1 remains `staged02`:
`b08686c7e392ae2fabe30632f56faaac306caddd4ac505fda2551952341eddc5`.
The final Rust service harness is
`de21f6d8dfd083874a024b667784c53fde5ba3eab2dcfbba8266fb7ab6223859`.
The identical old/new kernel-delay witness uses the earlier harness
`2861880fcdb7b6e9e8469f2be04f2750a43ea6edc91b9207b0fd2136e982d0c6`.
These are release-profile test executables; RC.2 remains unreleased.

| Check | Result |
| --- | --- |
| Complete default workspace, all targets | 425 passed, zero failed; 71 explicit helper/opt-in exclusions |
| Release M1 library | 53 passed, zero failed; two explicit exclusions |
| Strict workspace Clippy, all targets | Passed, including both new service cases |
| Selected real-process SSD services | 15 passed, including indexed status, bridge/native restart, ACK/GC, damaged-resume reconciliation, public HTTP/native, crashes, diagnostics, metadata loss/restore and monitoring/backups |
| Physical two-HDD N1 1+1, M1 database on HDD | Four passed: concurrent indexed status, damaged-resume reconciliation, active-writer GC and cross-EC migration with process-kill recovery |
| Original Console on the two-HDD service fixture | All four browser workflows passed |
| Private kernel block-device delay | Previous M1 fails the status deadline; corrected M1 passes fault, retry, restart and complete payload/version verification |
| Private cgroup memory pressure | Both selected service cases pass with actual kernel high-limit events and zero OOM events |
| Workstation/remote source comparison | All 364 Rust files match |

The sixteenth initial SSD service invocation, migration, failed in fixture
setup because `N1_OBJECT_TEST_SECOND_ROOT` was missing. It did not execute a
migration and is not counted as a pass. That exact case subsequently passes
with both explicit physical-HDD roots, including destination topology changes,
interrupted migration, original versions and complete payload verification.
The failed log is retained alongside the 15 SSD passes.

The browser cases use the original Console with its previously committed API
negotiation change: rename/export, re-added multipart resume, standard batch
and page-reload resume without resending acknowledged parts. No Console
production source changed during this stage.

## Status latency and resources

The Rust case first acknowledges part zero, measures 100 idle status requests,
then holds part one's HTTP body incomplete after N1 owns the operation.
Every active query must return only part zero within two seconds. The client
then finishes the original request, finalizes twice and verifies the complete
original payload and version after killing/restarting both engines.

Four alternating previous/candidate/candidate/previous phases pin the test
process and its engine children to SSD test system performance-core logical CPUs
0–11. M1 and the N1 1+0 member use DATA_SSD. These are local plaintext HTTP
status measurements through the real M1 service, not remote TLS latency, upload
throughput or a maximum request-rate test. Each aggregate contains 200
individual successful queries per available state.

| Observation | Previous M1 | Corrected M1 |
| --- | ---: | ---: |
| Idle mean / p50, ms | 0.129 / 0.097 | 0.079 / 0.069 |
| Idle p95 / p99 / max, ms | 0.333 / 0.476 / 0.776 | 0.171 / 0.344 / 0.351 |
| Active-part mean / p50, ms | First query exceeds the 2000 ms deadline | 0.072 / 0.067 |
| Active-part p95 / p99 / max, ms | No successful active samples | 0.088 / 0.139 / 0.365 |
| Aggregate M1 CPU during 200 idle queries, ms | 20 | 10 |
| Maximum observed M1 high-water RSS, KiB | 33,260 | 33,524 |

CPU counters have 10 ms granularity; this short sample does not establish an
exact CPU reduction ratio. An earlier unpinned screen is retained: idle means
were 0.104 ms before and 0.422 ms after, with p99 0.187 and 0.782 ms respectively.
The subsequent controlled screen does not erase that adverse observation.
There is no universal speedup claim or new large-file throughput claim for
this status-only change. Earlier large-file controls and bridge costs remain
in [the single-part report](M1-SINGLE-PART-20261114.md).

## Actual kernel I/O delay

The root controller creates an owned 512 MiB loop image on DATA_SSD, verifies
its backing-file identity, formats that private device as XFS and maps it
through Linux `dm-delay`. Only the new N1 member uses this mounted filesystem;
M1's database, the controller and independent ACK evidence remain on healthy
DATA_SSD paths. Existing physical filesystems and services are not targets.

After a seed object and the first durable multipart part are acknowledged,
the controller adds 5000 ms write delay. The second part stalls in actual
kernel I/O with a complete client body. N1 uses a 1000 ms admission deadline
and 1000 ms stall threshold for this witness. The corrected candidate reports:

| Observation | Result |
| --- | ---: |
| Competing part response | 503 after 1.003 s |
| Indexed status during the delayed part | 0.844 ms; only part zero acknowledged |
| Delayed part completion, including controlled recovery | 7.197 s |
| Finalize retry and post-SIGKILL payload/version readback | Passed |
| Rejected competing upload, retried after recovery | Passed with exact bytes |
| Final offline metadata verification | 33 records checked |

N1's health samples remain responsive while its I/O scheduler reports stalled
pressure. Incomplete named GET returns 404. Restoring the private linear target
allows the original part to finish; both earlier ACKs and new acknowledged data
survive process restart. No engine durability or checksum setting is weakened.

The original probe waited for status until the part completed and then saw its
valid ACK; that was a blocked-query result, not an early ACK. The tightened
deadline makes the old M1 failure explicit. Another controller attempt restored
the target too soon: `dm-delay` suspension released queued I/O before a full
delay interval, so the test's minimum-delay assertion failed. Holding the fault
through the configured interval corrects the fixture; both failed logs remain.

## Memory-pressure envelope

A separate private Linux cgroup contains the Rust harness, both engine process
trees and a Rust helper retaining 128 MiB of touched anonymous memory. The
accepted profile uses `memory.high=192 MiB`, `memory.max=384 MiB`, zero cgroup
swap and grouped OOM policy. It runs the existing bounded-bridge and complete
metadata-loss/snapshot-restore cases on fresh private DATA_SSD fixtures.

The bridge case retains its 1 MiB upload-buffer budget, interrupted clients,
bounded 503 when that budget is occupied, acknowledged-part restart recovery,
matching/conflicting retries, staged-corruption refusal and exact final bytes.
The restoration case verifies fragments, original versions, trash, permissions
and durable resume. The pass takes about 2.35 seconds of test execution; it is
a short resource-pressure witness, not sustained saturation or an OOM test.

The group reaches **203,026,432 bytes / 193.62 MiB** peak charged memory, with
**31 `high` events**, zero `max` events and zero OOM/group-OOM kills. Both cases
pass. Charged memory includes the helper, harness, engines and kernel-accounted
cache; it is not N1's or M1's individual RSS.

The first envelope put a 128 MiB resident helper against a 128 MiB high limit.
It throttled the initial enrollment CLI before either serving engine started.
That pilot was explicitly stopped, its isolated cgroup killed and its logs
retained. It did not reach an acknowledged-data workload and is not a pass.
The accepted 192 MiB envelope is a separately identified run.

## Evidence and remaining gates

SSD test system root: `/mnt/DATA_SSD/n1-m1-acceptance-20261113-01`. The exact
controllers, source overlays and manifests are in `source/`.
`reports/status12-*` contains build, regression and latency evidence;
`reports/stalled10` through `stalled13` retain all block-delay attempts;
`reports/memory14` and `memory15` retain both memory profiles. Rust owns the
workload/assertions; Bash only controls isolated Linux fixtures. No Python was
used. Private loop devices, mapper targets, mounts, cgroups and qualification
processes are removed after the runs. Their small evidence files remain.

The workstation copy is under
`/mnt/HDD1TB001/n1-m1-acceptance-20261113-01/remote-status12`, with physical
service and browser evidence in `reports/status12-hdd` and
`reports/console-status12-live`. Portable preservation under
`/srv/test-evidence/portable` excludes service credentials, private TLS keys,
test databases, object payloads, loop images and build caches.

These checks close the reproduced indexed-status blockage and qualify the
specified delayed-I/O and short memory-pressure profiles. They do not replace
physical power-loss testing, final-candidate four-HDD the four-HDD reference
system qualification, sustained saturation with concurrent maintenance, complete
contract/operator acceptance or remaining large-file performance acceptance.
Existing million campaigns remain preserved; none was restarted for this stage.
RC.2 is unreleased.
