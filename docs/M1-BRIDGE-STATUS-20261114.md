# M1 bridge status under active uploads — November 14, 2026

The read-only status lease also covers M1's staged multipart bridge. A slow
body growing a temporary file no longer blocks queries for earlier committed
receipts. Status retains its existing receipt and acknowledged-file-size
checks; it does not count an unacknowledged temporary file. Reconciliation
which can modify the native ACK index still uses the exclusive lease. Finalize,
cancel, GC exclusion, ACK fences, staging durability and payload verification
are unchanged. No N1 runtime code or persistent format changes.

This follows the [native indexed-status correction](M1-RESOURCE-RECOVERY-20261114.md).
The previous M1 `status12` passes that native case but still blocks bridge
status. The same new Rust bridge witness fails on `status12`'s two-second
deadline and passes on the corrected `bridge16` executable.

## Exact candidate

- M1 `bridge16`: `d75fcf451b463afc346b206300ce4d67b3bf234452f0ae782f7971d1ea3717ee`.
- Previous M1 `status12`: `c793e657f8a81d4029172b8660c417e3b72eb2c2e6a3376ba6952b2bae304458`.
- Fixed N1 `staged02`: `b08686c7e392ae2fabe30632f56faaac306caddd4ac505fda2551952341eddc5`.
- Functional Rust harness `bridge16`: `41b23dfd8695412f7f42fbc2fec17a926f5cb6db2f78a6c044def05286f237cd`.
- Instrumented Rust harness `bridge19`: `65c44646e53695b65b35574c8483b3f8830ca3bce9cc16fe163467e9a5461f5d`.

The later harness changes measurement only. Its resource helper adds the
POSIX process CPU clock, including all threads and exited-thread CPU time,
instead of inferring sub-millisecond costs from 10 ms `/proc` ticks or summing
only live threads. Existing coarse CPU and RSS fields remain available. The
production M1 executable is identical across both harness campaigns.

## Verification

The bridge witness uploads two 1,000,001-byte external parts with 256 KiB
native stripes. It acknowledges the first part, holds the second HTTP body
after its temporary staging file exists, and checks 100 idle and 100 active
status responses. Each must report only the first receipt. It then finishes
the same request, finalizes twice, kills/restarts both engines and compares
the complete original payload and version.

All 425 default workspace checks pass, with zero failed and 72 explicit
helper/opt-in exclusions. Release M1 library checks pass 53 cases, with two
explicit exclusions. Strict workspace/all-target Clippy passes for both
harness versions. All 365 workstation/remote Rust source files match.

The corrected candidate additionally passes:

- All 16 selected real-process SSD service cases: both concurrent-status
  modes, explicit/negotiated part geometry, single-part boundaries, bridge
  corruption/retry/restart, ACK/GC exclusion, damaged-resume reconciliation,
  public HTTP/native operations, process crashes, diagnostics, metadata
  loss/restore and monitoring/backups.
- Both new bridge-status and retained active-writer GC cases on two independent
  physical XFS HDDs, using N1 1+1 and an HDD-backed M1 database.
- All four original Console browser workflows on the two-HDD fixture. Console
  production source remains unchanged.
- The real private kernel-delay witness again: competing admission returns
  503 after 1.007 s, native indexed status returns in 0.902 ms, the delayed
  part finishes after 7.316 s, and all original payloads/versions, retry and
  33-record offline verification pass after both engines restart.
- The separate brief 192 MiB-high/384 MiB-max cgroup envelope with a touched
  128 MiB Rust helper, both engines and the harness. Bounded bridge and full
  metadata-loss/restore pass; peak charged memory is 201,977,856 bytes
  (192.62 MiB), with six high-limit events and zero max/OOM events.

Kernel-delay targets, private mounts, loop devices and cgroups are removed
after use. These are controlled Linux fault/pressure profiles, not a hardware
power cut, firmware hang or a sustained load qualification. The preceding
report retains the failed controller/setup pilots separately.

## Matched bridge-status measurements

The SSD test system runs M1 and N1 1+0 on DATA_SSD. Four alternating
previous/candidate/ candidate/previous phases pin the harness and both child
engines to performance-core logical CPUs 0–11. Each side contributes 200 idle
samples; the corrected candidate also completes 200 active-body samples. The
previous binary's first active query exceeds two seconds in both baseline
phases. These are local plaintext HTTP control queries with warm status state,
not a TLS, cold-cache, upload-throughput or maximum request-rate benchmark.

| Instrumented observation | Previous M1 | Corrected M1 |
| --- | ---: | ---: |
| Idle mean / p50, ms | 0.132 / 0.132 | 0.134 / 0.126 |
| Idle p95 / p99 / max, ms | 0.174 / 0.200 / 0.429 | 0.171 / 0.356 / 0.432 |
| M1 CPU during 200 idle queries, ms | 24.522 | 22.365 |
| M1 CPU per idle query, microseconds | 122.612 | 111.826 |
| Active-body mean / p50, ms | No successful samples; deadline exceeded | 0.340 / 0.347 |
| Active-body p95 / p99 / max, ms | No successful samples | 0.446 / 0.503 / 0.559 |
| M1 CPU during 200 active-body queries, ms | No completed-query comparison | 59.336 |
| M1 CPU per active-body query, microseconds | No completed-query comparison | 296.680 |
| Maximum observed M1 high-water RSS, KiB | 33,664 | 33,844 |

Idle mean is nearly unchanged and observed idle CPU falls 8.8%, but idle p99
increases by 0.156 ms. Those adverse tails remain part of the result. Active
queries now complete without waiting for the writer; their cost is reported
separately rather than treating a blocked baseline as a successful zero-CPU
query. CPU-clock precision does not remove workload or scheduling variability.

The earlier same-binary screen, before the CPU-clock instrumentation, records
idle means of 0.270 to 0.222 ms and idle p99 of 0.914 to 0.427 ms; corrected
active p99 is 0.450 ms. Both sets of individual samples are retained. This
stage makes no new large-file throughput claim. Earlier adverse large-file
controls, bridge conversion costs and remaining performance acceptance remain
in [the single-part report](M1-SINGLE-PART-20261114.md).

## Evidence and release boundary

SSD test system root: `/mnt/DATA_SSD/n1-m1-acceptance-20261113-01`.
`reports/bridge16-*` holds functional/build and initial timing evidence;
`reports/bridge19-*` holds the instrumented screen and final source audit.
`reports/stalled17` and `memory18` hold the repeated physical-kernel fault and
pressure evidence. Exact overlays, drivers and manifests are in `source/`. The
workstation copy is under
`/mnt/HDD1TB001/n1-m1-acceptance-20261113-01/remote-bridge19`, with HDD/browser
logs in `reports/bridge16-hdd` and `reports/console-bridge16-live`. The portable
checkpoint under `/srv/test-evidence/portable` retains source, binaries and
evidence without credentials, private TLS keys, datasets, loop images or build
caches.

RC.2 is unreleased. Final the four-HDD reference system topology/power-loss,
broader sustained maintenance/load, final contract/operator and accepted
performance gates remain required. No million-object campaign was restarted. No
Python was used.
