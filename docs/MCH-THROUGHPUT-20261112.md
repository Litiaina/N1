# MCH throughput and multipart timing — November 12, 2026

**Passed.** The release-profile Rust client completed three repetitions of
each workload through **HTTPS → M1 on NVMe → native TLS → N1 on four HDDs,
Pool A 3+1**. Every downloaded object passed exact byte-length, SHA-256 and
immutable-version checks. All **900 small objects and nine 1 GiB objects**
were subsequently reclaimed through scoped metadata GC, with zero failures or
deferred objects. A subsequent **native TLS → N1-only** measurement verified
and purged another three 1 GiB objects, isolating N1 finalization from M1.
The interactive Console and its M1 database remained available at the end of
this measurement. RC.2 remains
unreleased.

These tables retain the original MCH snapshot. The later
[finalization and media qualification](FINALIZATION-20261112.md) measures the
optimized runtime against its preceding LINK baseline on DATA_SSD 1+0 and
HDD 1+0/1+1, including p50/p95/p99, CPU/RSS and recovery checks. Its different
hosts and topology do not replace the MCH 3+1 measurements here.
The [subsequent home qualification](HOME-QUALIFICATION-20261113.md) records
M1 restart recovery, current-candidate service/Console checks, matched adapter
performance and monitoring overhead. No additional MCH run is included there.

## Placement and method

Host `itserver`, `192.168.0.12`: Intel Core i5-11400, six cores / twelve threads,
approximately 11 GiB RAM. M1 uses its independent `m1_storage` database.

| Component | Location / policy |
| --- | --- |
| M1 database and multipart staging | Ext4 on `/dev/nvme0n1p2`, KINGSTON SNV2S500G; isolated directory under `/home/administrator/DEV_TEST/n1-m1-rc2-20261112-01/throughput-20261112-01` |
| N1 pool A | The existing interactive pool's fresh member directories under `/mnt/DISK0` through `/mnt/DISK3`, each a distinct XFS HDD filesystem |
| HDD models | ST1000DM010-2EP1; two WDC WD10EZEX-21W drives; WDC WD10JPVX-00J |
| EC / block | Immutable 3 data + 1 parity, 256 KiB block; one logical stripe is 768 KiB |
| Object runtime | Existing release N1 process, io_uring, aligned direct I/O for large writes, adaptive concurrency enabled |
| Integrity / durability | Existing CRC32C + SHA-256 policy; strict upload acknowledgements |
| Request path | Rust client on MCH → private loopback HTTPS M1 → the same N1 process and pool used by Console |
| Isolation | A separate M1 database and benchmark-only fragment; no benchmark fragment or object is created in the interactive Console's metadata database |

The interactive M1/Console continue running; existing deployed services remain
running. The benchmark bypasses browser, LAN and Console-proxy overhead.
These named/batch phases do not measure standalone N1; the separate follow-up
below does. Neither measurement compares against N1 Lite/older binaries.
Authentication and source preparation occur before the upload timers.
The deterministic source repeats a precomputed 1 MiB SplitMix64 block; the client
streams large uploads and hashes downloads without saving a gigabyte fixture.

Each main repetition uploads 100 individual 4 KiB objects with one active
request, another 100 with eight requests, then a 1 GiB one-shot object and two
1 GiB durable multipart objects. A separate batch-only measurement adds three
100-object batches without repeating the large-object workloads. Downloads
are verified after each upload phase. Resource sampling reads process counters
and RSS every 50 ms; 100% CPU denotes one logical CPU, not the whole host.

No global cache drop or filesystem reformat was performed. These are immediate
post-upload reads; the small-object reads are warm. Host services and browser
monitoring share the machine. Three large-object samples per method describe
this workload; they do not establish a saturation or cold-read limit.

## Small objects

All objects are 4 KiB. Each row includes three runs of 100 objects. Throughput
is total acknowledged objects divided by total upload-phase wall time.

| Upload method | Mean time for 100 | Objects/s | Upload p50 | Upload p95 | Upload p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Individual, one active request | 11.278 s | 8.87 | 115.10 ms | 163.21 ms | 206.41 ms |
| Individual, eight active requests | 6.222 s | 16.07 | 468.17 ms | 798.15 ms | 896.51 ms |
| One 100-object batch request | 0.762 s | 131.27 | — | — | — |

Individual-request percentiles use all 300 samples per method. The batch's
response acknowledges 100 objects together; its wall time is batch latency,
not individual-object latency. Its three upload samples are **1.121, 0.590 and
0.575 seconds**. The first batch is retained in the average.

Verified warm GET throughput was 1,666 objects/s with one reader, 2,008 with
eight readers, and 2,221 with eight readers after batch upload. Each GET
checks bytes and version. These are small populations with cached payloads.

## 1 GiB objects

One GiB is exactly **1,073,741,824 bytes**. Upload timing includes init, part
transfer, a completion-status request and finalize when multipart is used.
Repeated finalize is measured afterward and excluded from upload throughput.
Throughput is three GiB divided by the sum of the three wall times.

| Method | Mean upload | Upload MiB/s | Mean verified GET | GET MiB/s |
| --- | ---: | ---: | ---: | ---: |
| One-shot | 9.335 s | 109.69 | 3.988 s | 256.76 |
| Durable multipart, 8 MiB parts, four concurrent | 10.459 s | 97.90 | 3.963 s | 258.41 |
| Durable multipart, 48 MiB parts, four concurrent | 8.201 s | 124.86 | 7.675 s | 133.41 |

All individual upload-throughput samples, in run order:

| Method | Run 1 | Run 2 | Run 3 |
| --- | ---: | ---: | ---: |
| One-shot | 110.09 MiB/s | 92.88 MiB/s | 133.36 MiB/s |
| Multipart, 8 MiB | 101.64 MiB/s | 87.31 MiB/s | 106.95 MiB/s |
| Multipart, 48 MiB | 110.32 MiB/s | 152.06 MiB/s | 119.25 MiB/s |

The lower post-upload GET rate after aligned multipart is retained. Read cache
state differs across these paths and no cache state was controlled; this is
not evidence of a matched read-path regression or a universal read speed.

## Multipart init and finalization

These are M1's named **durable multipart** endpoints, with strict part
durability. Every successful finalize returns the init version, and repeating
finalize returns that same version.

| Stage, mean of three runs | 8 MiB parts | 48 MiB parts |
| --- | ---: | ---: |
| Parts per 1 GiB object | 128 | 22, including a 16 MiB final part |
| Init | 9.02 ms | 6.45 ms |
| All parts, four concurrent | 1.131 s | 8.123 s |
| Finalize | **9,317.64 ms** | **70.43 ms** |
| Repeated finalize | 30.81 ms | 20.91 ms |

Finalization samples are **8,953.81 / 10,577.18 / 8,421.94 ms** for 8 MiB,
and **54.39 / 55.81 / 101.09 ms** for 48 MiB.

This difference has a concrete implementation cause. The 3+1 pool's stripe
is 768 KiB. An 8 MiB part is not a multiple of that stripe, so M1's geometry
bridge stores the parts on its configured NVMe staging path. Finalization
verifies staged parts and streams their bytes into N1 before publication.
The fast part-transfer stage therefore does not represent completed HDD
throughput. A 48 MiB part contains 64 full stripes; M1 forwards it to N1 during
the part request, leaving much less work for finalization. The final short
part is supported. The relevant implementation is
[`uploads/coordinator.rs`](../metadata_engine/src/uploads/coordinator.rs).

Both paths retain the same integrity and strict durability policy. Finalize
duration identifies where work happens; complete upload throughput includes
all stages. No engine or Console implementation changed for these measurements.

## Standalone N1 finalization

**Passed.** The same release N1 process, binary, pool and durability settings
handled three fresh 1 GiB multipart uploads directly over verified native TLS
on loopback. Each used 22 parts (48 MiB, with a 16 MiB final part) and four
authenticated writer connections. The client made **no requests to M1**.
The source bytes and SHA-256 are identical to the preceding named tests.

| Native N1 stage | Run 1 | Run 2 | Run 3 | Mean |
| --- | ---: | ---: | ---: | ---: |
| Init | 0.965 ms | 1.257 ms | 1.082 ms | **1.101 ms** |
| All durable parts | 9.016 s | 6.709 s | 8.121 s | **7.948 s** |
| Finalize | 54.785 ms | 54.613 ms | 69.786 ms | **59.728 ms** |
| Repeated finalize | 13.069 ms | 10.396 ms | 14.797 ms | **12.754 ms** |

Each first finalize returns the original LINK key and exact size; every repeated
finalize returns that key with `already_finalized=true`. Every complete GET
passed exact byte-length, SHA-256 and finalized geometry checks. Native delete
then removed **four shards per object**, and a subsequent stat returned 404.
No interactive M1 namespace was created or changed by this client.

| Path, mean of three 1 GiB trials | Init | Finalize | Whole upload MiB/s |
| --- | ---: | ---: | ---: |
| Native N1 alone, 48 MiB parts | 1.10 ms | **59.73 ms** | **127.08** |
| HTTPS M1 → native N1, aligned 48 MiB parts | 6.45 ms | **70.43 ms** | **124.86** |
| HTTPS M1 → native N1, bridged 8 MiB parts | 9.02 ms | **9,317.64 ms** | **97.90** |

This isolates the long finalize observed with incompatible client geometry to
**M1's bridge workflow**, which verifies staged files and transfers their
payload into N1 during finalize. N1's measured publication after durable parts
is approximately 60 ms. Aligned M1 already performs that payload work during
part transfers. The sequential samples do not establish an exact 10.71 ms M1
overhead or a statistically significant throughput advantage for standalone N1.

The standalone upload timer includes a client-only identity report and its
fsync between init and part transfer: mean **48.82 ms**, recorded separately
and retained in the reported 127.08 MiB/s. Finalize's timer excludes this report.
Standalone upload does not issue a full native resume-status scan before
finalize; the preceding M1 test issues a named status request from its durable
ACK index. Repeated finalize, GET and purge remain outside upload throughput.

Mean standalone verified GET is **7.153 s / 143.16 MiB/s**. Mean N1 upload CPU
is **5.48 s**; the separately running interactive M1 uses **0.02 s** over those
upload intervals without participating in the benchmark. Maximum sampled N1
RSS is **423.97 MiB**, including allocations retained by the same long-lived
process across preceding workloads. This is not incremental per-upload memory
or a matched RSS regression measurement. Sampled client peak RSS is 57.38 MiB;
GNU time reports 61,160 KiB peak, zero swaps, **46.99 s** whole-job time and exit
0. Cache state and shared-host scheduling remain uncontrolled.

N1 rejects the standalone 8 MiB init with HTTP-equivalent native status **400**,
as expected for incompatible stripe geometry; no identity is allocated by that
rejected request. The original 8 MiB named test works because M1 supplies the
bridge. This follow-up measures normal uninterrupted finalization with a warm
server process, not finalization recovery after a process or power failure.

Raw trial files, client-held identity journals and completion proof are in
`standalone-results/`; `reports/standalone-summary.json` aggregates them.
`reports/STANDALONE-*`, `reports/standalone-*.log` and the GNU-time file retain
the frozen binary hashes, successful strict Clippy/build, exit status and
controller evidence. The local evidence copy has the same `standalone-results/`
plus a `standalone-reports/` directory. Runtime code remains unchanged.

The subsequent [15 GiB standalone N1 test](N1-STANDALONE-15GIB-20261112.md)
also passes full SHA-256 readback and purge: **95.45 s / 160.92 MiB/s** upload,
**100.67 ms** finalize. It is one larger-object trial on the same N1 process,
not an additional M1 measurement.

## CPU and memory

CPU below is mean process CPU seconds per uploaded **1 GiB**, summed across
that upload's init/parts/finalize phase. Client CPU and GET CPU are recorded
separately in the raw results.

| Upload method | N1 CPU seconds | M1 CPU seconds | Combined CPU seconds |
| --- | ---: | ---: | ---: |
| One-shot | 3.76 | 1.38 | 5.14 |
| Multipart, 8 MiB | 3.02 | 7.12 | 10.14 |
| Multipart, 48 MiB | 4.48 | 1.59 | 6.07 |

Maximum sampled RSS during the main measurements was **265.55 MiB N1** and
**33.74 MiB isolated M1**. The later batch-only measurement observed N1 at
**308.72 MiB** and its restarted isolated M1 at **20.74 MiB**. These are observed
process residents, not configured limits or whole-host page-cache accounting.
N1 retained its process across all measurements. The client process's whole-job
peak RSS was **359.63 MiB** in the main run. No swaps were reported by GNU time.

## Evidence, cleanup and reproduction

MCH evidence root: **`/mnt/DISK0/n1-throughput-20261112-01`**.
The main run exits 0 after **3 min 57.52 s**, including verification and untimed
cleanup. `results/PASSED.json` records **609 reclaimed objects**; the separate
`batch-results/PASSED.json` records another **300**. Both scoped GC jobs complete
with **zero failures and zero deferred objects**. Only the private benchmark
database uses zero trash retention for cleanup; the interactive policy is
unchanged. The private benchmark M1 processes are stopped afterward.

`results/` and `batch-results/` retain each trial, methodology, all latency
samples, per-process before/after counters and completion reports. `reports/`
holds aggregate `summary.json`, build/strict-Clippy logs, GNU-time results,
configurations without secret values, hardware and binary manifests. A helper
compile error was corrected before measurement; its original log remains.
Both optimized client revisions pass strict scoped Clippy; local formatting
and diff checks pass. All workload and verification code is Rust.

| Frozen artifact | SHA-256 |
| --- | --- |
| N1 release binary | `b109b9f2307f42f3b25929ffca73a6bb313259f4529457e80b03576a0afe14d4` |
| M1 release binary | `42afd755687e5d664bb8981ff998149f0a00b0741de40d0a69c231aeea10facd` |
| Main Rust client | `2cd3029f2271cf54d080ba62bd7114ef28c6f6bda476c283d421178ef1b57cfd` |
| Batch-only Rust client | `16a674c57e22e5a71549fb8edbdc64794a3993ff8dd9eaf918102e170e7da921` |
| Standalone N1 Rust client | `d6aed438e54503d83374be750a390b6f8ef35cecd1c453292f5ddf381e29b8e5` |

The server runtime matches committed `2d1ed2b`; release builds use MCH's Rust
1.92 toolchain. Benchmark source is
[`throughput_bench.rs`](../metadata_engine/examples/throughput_bench.rs) with
separate measurement and request modules. Original source archives remain at
the continuation root under `imported/throughput-bench-20261112.tar.gz` and
`throughput-bench-batch-20261112.tar.gz`; their SHA-256 values are
`19afb2c2e3b6e8a80386dd0e05005b720aec94bc91f9aa9d0a09b06acb336693` and
`cccc2a061ebe43c705421c37d84258e22bf3a000c557c31a4fa22b31abc2db64`.

```sh
cargo clippy --release -p m1 --example throughput_bench \
  --offline --locked --no-deps -- -D warnings
cargo build --release -p m1 --example throughput_bench --offline --locked
throughput_bench --config /private/isolated-m1-client.json --output /private/new-results
throughput_bench --batch-only --config /private/isolated-m1-client.json \
  --output /private/new-batch-results
throughput_bench --standalone-n1 --config /private/standalone-client.json \
  --output /private/new-standalone-results
```

The config supplies the private loopback HTTPS URL, CA, isolated master-env
path, N1/M1/Console PIDs and repeat count. The isolated M1 must have zero trash
retention for the client's scoped cleanup. Never point this client at the
interactive metadata database. Choose new private result directories; existing
results are never overwritten. A local copy of all reports is on HDD1TB001 at
`/mnt/HDD1TB001/n1-rc2-continuation-20261112-01/reports/mch-throughput-20261112-01`.

Standalone mode uses its separate config: native loopback TLS URL, CA, N1
secret-env path, the three interactive service PIDs and repeat count. It does
not start or configure M1. The frozen standalone source archive is
`imported/standalone-throughput-bench-final-20261112.tar.gz` under the MCH
continuation root, SHA-256
`30745769b5954923309ad1d943a70a03740bd30f30e613eec5227e984e2000ac`.

This workload does not repeat or replace the preserved million-object campaign.
Its final natural-TTL cleanup and census remain scheduled separately. No commit,
push, tag or release is performed by this benchmark.
