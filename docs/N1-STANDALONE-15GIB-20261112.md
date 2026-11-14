# Standalone N1: 15 GiB upload — November 12, 2026

**Passed.** A release-profile Rust client uploaded exactly **16,106,127,360
bytes (15 GiB)** directly to N1 on the four-HDD reference system, downloaded and SHA-256-verified every
byte, checked repeated finalize, then purged the four test shards. Subsequent
stat returned 404. The driver exits 0. N1 runtime code and configuration remain
unchanged; M1 is absent from this request path.

## Method

The four-HDD reference system has an Intel Core i5-11400, six cores / twelve
threads, approximately 11 GiB RAM. This uses the same N1 release binary and
long-lived process as the [preceding 1 GiB
measurements](FOUR-HDD-THROUGHPUT-20261112.md#standalone-n1-finalization).

- Native **verified TLS over loopback → N1 → Pool A**, four SATA XFS HDDs at
  `/mnt/DISK0` through `/mnt/DISK3`.
- Immutable **3 data + 1 parity**, 256 KiB block, 768 KiB logical stripe.
- **320 parts of 48 MiB**, four independent writer connections, strict durable
  acknowledgements, existing aligned direct I/O and adaptive concurrency.
- One complete trial. The deterministic payload repeats the same precomputed
  1 MiB SplitMix64 block used by the earlier benchmark. The client holds a
  bounded 48 MiB part buffer and does not write a 15 GiB source fixture.
- Authentication, connection setup and expected-hash calculation precede upload
  timing. Upload timing includes init, the client-only identity journal/fsync,
  durable part transfers and first finalize. Repeated finalize, verified GET
  and purge have separate timing. No pre-finalize resume-status scan is issued.
- No global cache drop, process restart, filesystem format or fault injection.
  Immediate GET is not a controlled cold-read measurement. Existing services,
  M1 and Console remain running on the shared host.

## Results

| Operation | Measured result |
| --- | ---: |
| Multipart init | **1.273 ms** |
| All 320 durable parts | **95.303 s** |
| Finalize | **100.668 ms** |
| Whole upload | **95.450 s / 160.922 MiB/s** |
| Repeated finalize | **40.632 ms** |
| Full verified GET | **120.868 s / 127.081 MiB/s** |
| Purge | **145.110 ms**, four shards removed |
| Stat after purge | **404** |
| Whole client job, including preparation, verification and cleanup | **3 min 45.29 s**, exit 0 |

The client identity-journal fsync is **44.996 ms**, retained in whole-upload
time and excluded from the finalize timer. First finalize retains the original
LINK key and exact size; repeating finalize returns that same identity with
`already_finalized=true`.

Expected and observed full-object SHA-256:

```text
77272496b6d815afc39fc769226097d9aa04c4d6ab488c5a1cb180aab86d2ef7
```

Durable part-response latency across the 320 parts is **p50 1,174.58 ms,
p95 1,449.29 ms, p99 1,586.34 ms, maximum 1,614.17 ms**. These are 48 MiB
transfer/acknowledgement latencies with four concurrent parts, not object
finalization percentiles.

N1 used **71.73 CPU seconds during upload**, equivalent to 0.751 logical CPU on
average, and **56.34 CPU seconds during verified GET**. Maximum sampled N1 RSS
was **515.21 MiB**, including allocations retained by the same process across
previous workloads. Sampled client peak was **57.16 MiB**; GNU time reports
60,984 KiB lifetime peak and zero swaps. Process samples do not account for
whole-host page cache or establish incremental per-object memory costs.

## Comparison with the 1 GiB test

| Standalone N1 | 1 GiB, mean of three trials | 15 GiB, one trial |
| --- | ---: | ---: |
| Parts | 22 | 320 |
| Init | 1.10 ms | 1.27 ms |
| Finalize | 59.73 ms | **100.67 ms** |
| Whole upload MiB/s | 127.08 | **160.92** |
| Finalize share of whole upload | 0.74% | **0.105%** |

The larger payload spends its time in part transfer. Finalization stays near
one tenth of a second in this trial, consistent with N1 sealing and durably
publishing already-written shards. This is not a tail-latency guarantee under
load, a matched M1 15 GiB comparison or a proof of crash recovery. M1's separate
8 MiB geometry-bridge result must not be extrapolated to a measured 15 GiB
finalize time.

## Evidence and reproduction

the four-HDD reference system evidence root:
**`/mnt/DISK0/n1-standalone-15gib-20261112-01`**. The measured client job ran
**15:06:41–15:10:26 Asia/Manila**. `results/` preserves methodology, the
client-held LINK identity, all phase/process counters and `PASSED.json`;
`reports/` contains live progress, GNU time, summary, strict release Clippy and
build logs, status/exit and server/client hashes. `source/` and `artifacts/`
preserve the tested client. These are client evidence records, not N1 metadata.

| Artifact | SHA-256 |
| --- | --- |
| Unchanged N1 binary | `b109b9f2307f42f3b25929ffca73a6bb313259f4529457e80b03576a0afe14d4` |
| Rust client | `2023a8a5282a2db03e2963a1ef8718b24d19c0ba694de1cf17de0835cf1031b5` |
| Frozen client source archive | `f95035fe8139a58ed68bf021b4dfd43cc54d27a3122f9b1f3177d1312103a80a` |
| `results/PASSED.json` | `a9f08eb26137a78fe8c86ccd88c24a7c4d5754ff9d4616285992be04c1faff33` |

The frozen source archive is under the continuation root's
`imported/standalone-15gib-source-20261112.tar.gz`. The client uses the existing
Rust 1.92 optimized build and vendored locked/offline dependencies:

```sh
cargo clippy --release -p m1 --example throughput_bench \
  --offline --locked --no-deps -- -D warnings
cargo build --release -p m1 --example throughput_bench --offline --locked
throughput_bench --standalone-n1 --config /private/client.json \
  --output /private/new-empty-results
```

The standalone config supplies the verified native loopback URL and CA, N1's
private secret-env path, the interactive N1/M1/Console PIDs, `repeats: 1` and
`total_bytes: 16106127360`. Source and expected-hash generation are bounded by
the diagnostic client's supported sizes; they do not introduce an N1 file-size
limit. The client never provisions a metadata fragment or calls M1.

The local evidence copy is on HDD1TB001 at
`/mnt/HDD1TB001/n1-rc2-continuation-20261112-01/reports/four-hdd-n1-15gib-20261112-01`.
Its completion record matches the remote SHA-256. All original deployed
services and the interactive stack remain running. No commit, push, tag or
release was performed.
