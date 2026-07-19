# N2 3.0 Performance and Resilience Evidence

This document records the finite release proof for the N2 3.0 flat-namespace
engine. Historical N2 1.x and 2.x results do not qualify the N2 3.0 on-disk
format or namespace contract.

## Qualified proof

| Field | Value |
| --- | --- |
| Date | 2026-07-18 |
| Evidence | `/home/wlzambo/DEV-TEST/N2-3.0-proof-20260718-220508` |
| Result | PASS |
| Logical topology | Four N2 nodes on two physical hosts |
| Hosts | Workstation plus authorized server `192.168.0.36` |
| Static object EC | 2 data + 2 parity |
| NCNX replication | 3 |
| Test object | 64 MiB |

The proof used direct N2 HTTPS endpoints. It did not place a reverse proxy,
gateway, database, or other data-path layer in front of the engine.

## Measured data path

| Operation | Result |
| --- | ---: |
| 64 MiB upload | 46.49 MiB/s |
| Download through node A | 89.30 MiB/s |
| Download through node B | 129.74 MiB/s |
| Download through node C | 77.35 MiB/s |
| Download through node D | 71.45 MiB/s |

These are end-to-end application rates for this two-host test, not raw disk
or network benchmarks.

## Namespace latency

| Operation | Samples | p50 | p95 | p99 | Mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zero-decode list | 30 | 11.362 ms | 17.901 ms | 18.022 ms | 11.952 ms |
| Exact stat | 30 | 49.708 ms | 52.427 ms | 52.661 ms | 46.577 ms |
| Exact copy/delete rename | 20 | 84.555 ms | 689.807 ms | 689.807 ms | 191.790 ms |

Rename created zero lifecycle-trash records. Its four sequential namespace
phases are destination/source validation, destination NCNX publication, and
source NCNX removal; each replica fan-out is parallel. It performs no object
rewrite and does not open NCDX or shard payloads.

The rename tail is recorded as measured and is not hidden. This finite proof
establishes correctness and exposes a performance baseline; it is not a claim
that the p95 is already optimal on every network or device.

## Process cost during proof

CPU is Linux process CPU as a percentage of one logical core. RSS is the peak
resident set sampled by `pidstat`.

| Node | Samples | Average CPU | Peak CPU | Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| A | 38 | 12.10% | 56.00% | 160.69 MiB |
| B | 36 | 5.11% | 17.00% | 71.04 MiB |
| C | 41 | 6.46% | 30.00% | 103.82 MiB |
| D | 38 | 5.42% | 24.00% | 98.62 MiB |

The same samples reported the following average process IO and scheduler
activity:

| Node | Read | Write | Voluntary switches | Involuntary switches |
| --- | ---: | ---: | ---: | ---: |
| A | 1726.53 KiB/s | 1736.00 KiB/s | 2.32/s | 0.29/s |
| B | 1822.33 KiB/s | 1833.33 KiB/s | 2.05/s | 0.00/s |
| C | 800.88 KiB/s | 808.20 KiB/s | 1.59/s | 0.05/s |
| D | 864.11 KiB/s | 1736.21 KiB/s | 0.89/s | 0.08/s |

Exact hardware CPU-cycle counters were unavailable because the host kernel had
`perf_event_paranoid=4`. The proof did not change that system policy, so no
cycle count is claimed.

## Correctness and failure evidence

| Invariant | Result |
| --- | --- |
| Same-key overwrite | Two immutable versions; newest bytes returned |
| Independent-key concurrency | 8 concurrent writes; all 8 listed |
| Exact-key/same-prefix pagination | 5 ordered one-item pages; no omission |
| Folder trash/recover | 2 of 2 objects restored |
| NCNX healing | Deleted namespace replica recreated |
| Object healing | Deleted shard recreated |
| One-node loss | Existing object read byte-exactly |
| Two-node (parity-count) loss | Existing object read byte-exactly |
| Unsafe write during node loss | Rejected with HTTP 503 |
| TTL garbage collection | Expired trash reclaimed; later GET returned 404 |
| Restart | Object remained byte-exact after all nodes restarted |
| Suspicious log lines | 0 |

## Qualification boundary

This is a bounded architectural proof on four logical failure domains hosted
by two physical machines. It validates the exercised 2+2 loss boundaries,
flat NCNX behavior, lifecycle, and healing. It is not a soak test, an exabyte
scale proof, a four-physical-host fault-isolation proof, or a substitute for
production monitoring. N2 must continue to fail closed when its configured
static topology cannot safely publish a mutation.
