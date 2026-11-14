# N1 completed-staging recovery — November 14, 2026

Explicit object repair now handles completed, unpublished shards as well as
finalized objects. It reconstructs into `.uploads/` and leaves publication to
the existing finalize operation. This closes a recovery gap without changing
LINK, the shard format, upload retry immutability or N1's ownership boundary.
It does not establish hardware power-loss resilience or release RC.2.

## Reproduced failure and correction

The physical witness uses independent N1/M1 processes, M1 on the first test HDD
and a 1+1 pool on two distinct XFS HDDs. Five externally shaped parts are
acknowledged, native completion is recorded without publication, both engines
are stopped, and one staged shard is truncated to its 128-byte header. After
restart, earlier M1 ACKs still exist and finalize correctly refuses damaged
staging. Matching retries correctly remain read-only. Previously, explicit
`object.repair` returned 404 because it looked only at final shard paths.

The same Rust witness fails at that repair call on the frozen native04 binary
and passes on staged02. The correction first tries the existing final-object
reader. Only final-object absence permits inspection of staged completion
seals. Enough valid headers and verified stripes, the existing EC codec and
the complete reconstructed object root are required. Synchronized private
repair files replace damaged staging under the existing key lifecycle lock.
Ordinary GET/stat remain restricted to published objects.

The witness confirms that repair alone exposes no final object, then kills and
restarts both processes again, finalizes twice with the same version and checks
every source byte. Core cases additionally cover insufficient verified stripes,
unchanged damaged files after refused retries, finalized-shard immutability and
404 on an old retry after zero-residue purge.

An initial experiment allowed matching retries to restore completed staging.
Although its new cases passed, it broke the existing
`completed_staged_payload_retries_are_read_only_and_cannot_replace_corrupt_data`
test. That experiment was removed. The selected implementation changes only
explicit repair; `upload/stream.rs` is unchanged from `f08d55d`.

## Executables and checks

Both production N1 builds use Rust/Cargo 1.99.0, the same lockfile and release
profile on the SSD test system. Retained symbols do not change optimization
settings. The benchmark client is identical for both servers.

| Executable | SHA-256 |
| --- | --- |
| Frozen N1 native04 | `c9cc0e6f5253723735cfbeec90a5419c6ff96ac0a2c865ef86994ba47ebe6156` |
| N1 staged02 | `b08686c7e392ae2fabe30632f56faaac306caddd4ac505fda2551952341eddc5` |
| Fixed Rust benchmark | `a8b83c5c0ba7e48a26dee87c5764847d55dc2b8d90e3bf97626168e949e0375c` |

- The complete optimized N1 library suite passes: **228 passed, 0 failed,
  2 explicit exclusions**. The original read-only retry case is included.
- All five aligned direct-I/O checks pass, including the explicit physical XFS
  page-cache bypass/reopen witness.
- The two-HDD M1 → N1 recovery witness reproduces the old failure and passes
  with staged02. These are process-crash and deliberate file-damage checks,
  not a physical power interruption.
- Strict N1/M1 all-target Clippy passes for this recovery candidate.

## Small-object comparison

DATA_SSD 1+0, loopback native TLS, ABBA, fresh private identities, quotas disabled
in the private performance configuration. Each phase includes 100 normal and
100 durable 4 KiB uploads. Every object is read back and purged. No M1 process
is involved. Raw per-operation samples and process counters are retained.

| Observation | Frozen | staged02 |
| --- | ---: | ---: |
| Normal upload mean / p99, ms; 200 samples each | 6.082 / 9.395 | 6.091 / 15.135 |
| Durable upload mean / p99, ms; 200 samples each | 7.442 / 13.902 | 7.440 / 11.447 |
| Normal verified GET mean / p99, ms | 0.739 / 1.443 | 0.706 / 1.425 |
| Durable verified GET mean / p99, ms | 0.773 / 1.400 | 0.804 / 1.293 |
| Normal N1 CPU ms per complete lifecycle | 6.500 | 6.200 |
| Durable N1 CPU ms per complete lifecycle | 7.300 | 7.550 |

The nearly equal upload means do not cancel the adverse normal-upload p99 or
durable GET mean/CPU. CPU here includes upload, GET and purge; it is not
upload-only CPU. A few large objects cannot establish object-level p99.

## Large-object controls

The uncontended, unpinned 1 GiB ABBA screen measures 265.58 versus 165.87 MiB/s
aggregate upload and 2.985 versus 4.185 N1 upload CPU seconds/GiB. That adverse
observation is retained. Source inspection does not identify additional upload
work: the receive/write implementation is unchanged, and the added path is
explicit repair. This does not make the measured regression acceptable or prove
its cause.

A separate diagnostic constrains both private N1 and the client to the main
PC's P-core logical CPUs 0–11. Its four individual 1 GiB upload times are
2.367 / 4.308 / 9.143 / 9.290 seconds in ABBA order; N1 CPU is
2.320 / 3.050 / 5.270 / 5.220 seconds. GET stays between 2.671 and 2.692 seconds.
Pinning alone does not remove the within-binary variation. Neither result is
discarded or used to claim CPU parity. Longer matched 15 GiB measurements are
recorded separately; no object-level p99 is inferred from these few objects.

The longer ABBA comparisons complete **eight 15 GiB objects**: four on DATA_SSD
1+0 and four on two physical XFS HDDs in 1+1. Each uses strict durable 48 MiB
parts, four writers, identical TLS/client/source, full SHA-256 readback,
repeated finalize and native purge/404. Each filesystem's final private-pool
census contains only its enrollment marker. M1 is not running. The SSD profile
pins only the two task-owned processes to logical CPUs 0–11; the HDD profile
leaves affinity unchanged. Neither profile drops host caches or changes CPU
policy, kernel settings or other applications.

| Observation | SSD frozen / staged02 | HDD frozen / staged02 |
| --- | ---: | ---: |
| Aggregate upload MiB/s | 105.357 / 105.298 | 68.498 / 69.354 |
| N1 upload CPU s/GiB | 5.318 / 5.095 | 5.632 / 5.506 |
| Peak sampled N1 upload RSS, KiB | 76,924 / 78,448 | 115,504 / 125,204 |
| Aggregate verified GET MiB/s | 360.571 / 363.407 | 40.914 / 40.890 |
| N1 GET CPU s/GiB | 1.770 / 1.740 | 4.818 / 4.486 |
| Part p50, ms | 1,912.915 / 1,915.719 | 2,583.728 / 2,569.895 |
| Part p95, ms | 2,023.558 / 2,050.034 | 4,244.434 / 4,366.310 |
| Part p99, ms | 2,068.913 / 2,123.951 | 5,012.337 / 4,947.142 |

Each percentile pools 640 individual part samples per implementation/profile;
it is not an object-finalize p99. Throughput divides total bytes by total time.
Sampled RSS excludes the host page cache. CPU is the indicated transfer's
process CPU, separately from the verifying client.

| Profile | Implementation | Both upload times, seconds | Both init times, ms | Both finalize times, ms | Both GET times, seconds |
| --- | --- | --- | --- | --- | --- |
| SSD 1+0 | Frozen | 144.236 / 147.345 | 0.207 / 0.796 | 21.270 / 77.836 | 42.970 / 42.228 |
| SSD 1+0 | staged02 | 145.183 / 146.561 | 0.169 / 0.249 | 21.022 / 21.325 | 42.270 / 42.263 |
| HDD 1+1 | Frozen | 243.984 / 204.494 | 99.638 / 0.927 | 60.652 / 45.055 | 374.194 / 376.656 |
| HDD 1+1 | staged02 | 191.235 / 251.713 | 0.890 / 0.992 | 732.717 / 18.709 | 375.441 / 375.838 |

The long-transfer aggregates do not reproduce the short SSD upload regression:
SSD throughput differs by -0.06%, HDD by +1.25%, and measured upload CPU falls
4.19%/2.25%. This supports retaining the scoped repair correction; it does not
explain every short-run delay or establish a universal speedup. Higher RSS,
SSD part p95/p99, HDD part p95 and the **732.717 ms HDD finalize** remain adverse
observations. HDD immediate GET is effectively equal in this profile. Full
release tail-latency/saturation acceptance remains open.

## Evidence

The run root is `/mnt/HDD1TB001/n1-m1-acceptance-20261113-01`; copied SSD test
system logs are under `remote-negotiate01/`. Original SSD test system evidence
remains under `/mnt/DATA_SSD/n1-m1-acceptance-20261113-01`.

`reports/staged02-hdd/` preserves the identical baseline/candidate failure
witness. `staged-recovery-candidate02.log`, `staged02-direct-*.log` and
`staged-recovery-clippy.log` retain regression/direct-I/O/lint output.
`reports/staged02-ssd-*` and `reports/staged02-hdd15-*` retain measurements;
`staged02-ssd15-summary.json` and `staged02-hdd15-summary.json` retain arithmetic
derived from the unedited rows. Copied SSD reports use `remote-negotiate01/`.
Reports from a separate build-overlapped attempt are explicitly excluded rather
than reused.
`rejected-upload-repair/` preserves the discarded retry experiment and its
failure. No Python, release, tag, global cache drop or drive reformat is part
of this qualification.
