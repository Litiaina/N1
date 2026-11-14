# M1 multipart geometry qualification — November 14, 2026

M1 can negotiate multipart geometry before Begin. This removes M1 payload
staging when the client's preferred size can be aligned with the selected N1
pool. N1's native04 executable, LINK format, durability, direct I/O and runtime
source are unchanged. Explicit arbitrary part sizes retain their existing
adapter and validation. This is a bounded M1/API improvement, not a new object
engine or a release declaration.

## Contract

`negotiate_part_size: true` on HTTP or native multipart init makes the input
part size a preference. The response supplies the chosen size and count.
Selection respects the configured maximum and 131,072-part bound, preserves a
small object's single-part shape, and raises the size when necessary for a
larger object. Existing pending init retries return their saved geometry and
identity before considering a new pool. Resume never changes geometry.

The default remains explicit geometry. A configured maximum smaller than one
native stripe retains the existing staging adapter. There is no object-sized
allocation, new object record, background payload scan or N1 format change.
Unit cases cover three-data-shard alignment, a 1 TiB shape and policy limits;
the 1 TiB case tests geometry, not a physical 1 TiB transfer.

Console's change is restricted to multipart init and validation of its returned
shape. Old identity-only responses retain the original requested shape; a
server declaring a part size must return a complete, consistent shape. Saved
sessions still reject a size/count change. No UI redesign is included.

## Matched measurement

SSD test system `ssd-test.example.invalid`, DATA_SSD XFS, N1 1+0 and M1 on the
same SSD. Compiler and release profile are matched. Baseline is buffer01
(`f1c74d6` runtime), candidate is negotiate01. Both use the **same new Rust
benchmark executable** and request negotiation. Baseline ignores the new field
and keeps the hint; candidate chooses aligned parts. The deliberately different
resulting geometry is the feature being measured, not a claim of identical
native request counts.

ABBA: baseline forward order, candidate reverse, candidate forward, baseline
reverse. Each phase uploads two separate 1 GiB deterministic sources with four
concurrent strict durable part writers, checks status, finalizes twice, and
verifies the complete GET's SHA-256 and immutable version. Indexed GC,
offline database validation and a marker-only N1 file census pass in all four
phases. Desktop activity is retained and captured; no cache drop is claimed.

| Upload observation | Baseline | Negotiated |
| --- | ---: | ---: |
| Unaligned hint, bytes | 8,519,680 | 8,519,680 |
| Actual external part size, bytes | 8,519,680 | 8,388,608 |
| Aggregate upload throughput, MiB/s | 55.86 | 99.41 |
| Individual upload seconds | 14.818 / 21.846 | 11.670 / 8.931 |
| M1 CPU seconds/GiB | 7.835 | 4.490 |
| N1 CPU seconds/GiB | 3.650 | 5.785 |
| Client CPU seconds/GiB | 2.590 | 2.590 |
| Peak sampled M1 RSS, KiB | 27,992 | 19,088 |
| Finalize, individual milliseconds | 8,639.093 / 8,501.635 | 46.164 / 5.688 |
| Part latency p50 / p95 / p99, ms | 259.700 / 562.016 / 2,507.760 | 316.253 / 563.280 / 594.205 |
| Part samples | 254 | 256 |
| M1 upload write bytes, two objects | 2,148,925,440 | 2,191,360 |

Upload throughput rises **78.0%**, M1 upload CPU falls **42.7%**, and combined
M1+N1 upload CPU falls **10.5%** for this previously bridged request. N1 now
handles 128 externally acknowledged native parts per object instead of 22
larger bridged ranges, so its measured CPU rises despite an unchanged binary.
Part p50 rises and p95 is approximately equal. The sample contains only two
objects per group: part p99 is meaningful for these transfers; object-level
p99 is not established.

The already-aligned 8 MiB control stays on the same direct path: 98.69 versus
102.58 MiB/s aggregate, M1 CPU 4.140 versus 4.320 s/GiB and N1 CPU 5.465 versus
5.655 s/GiB. Part p99 is 602.727 versus 599.493 ms. The CPU increases and phase
variation remain recorded; a universal CPU improvement is not claimed.

Immediate full readback of the formerly bridged objects takes 2.105 / 2.076 s
before and 2.750 / 2.765 s after. That adverse observation remains open for
read-path/cache characterization. The already-aligned control takes 2.759 /
2.760 s before and 2.754 / 2.762 s after. These are immediate reads after
different write geometries, not a cold-storage or fixed-object read comparison.
No read implementation changed in either engine.

After the cross-client reuse correction, the **final negotiate02 executable**
repeats the same four-phase matrix. The initial matrix above is preserved
separately; the final-candidate observations are:

| Final-candidate observation | Baseline | negotiate02 |
| --- | ---: | ---: |
| Previously unaligned upload, MiB/s | 57.95 | 107.11 |
| Individual upload seconds | 13.336 / 22.004 | 9.307 / 9.814 |
| M1 / N1 CPU seconds/GiB | 8.415 / 3.685 | 4.630 / 5.635 |
| Client CPU seconds/GiB | 2.880 | 2.670 |
| Peak sampled M1 RSS, KiB | 26,772 | 19,512 |
| Individual finalize milliseconds | 8,054.033 / 10,429.287 | 46.142 / 46.894 |
| Part p50 / p95 / p99, ms | 255.117 / 529.080 / 660.880 | 128.679 / 580.104 / 1,445.253 |
| M1 upload write bytes, two objects | 2,148,900,864 | 2,195,456 |
| Immediate GET seconds | 2.054 / 2.085 | 2.780 / 2.743 |
| Already-aligned control upload, MiB/s | 104.56 | 103.14 |
| Control M1 / N1 CPU seconds/GiB | 3.515 / 4.870 | 3.920 / 5.225 |
| Control part p99, ms | 585.232 | 1,646.550 |

The final formerly bridged group improves aggregate upload throughput **84.8%**,
M1 CPU **45.0%**, and combined server CPU **15.2%**. However, this repeat has
worse part p95/p99, higher aligned-control CPU, and the same immediate-read
cost. The implementation is functionally qualified as an **optional geometry
contract**; complete tail-latency/read-performance acceptance remains open.
Both matrices are retained; the earlier favorable tail result is not substituted
for the final candidate's adverse result.

## Completed checks

- Initial selected source: **420 passed, zero failed**, 64 explicit opt-in/helper
  exclusions in the default workspace/all-target run. N1 runtime is unchanged.
- Final reuse correction: **51 M1 release library cases passed**, two opt-in
  exclusions; strict M1/all-target Clippy passes. The affected default-profile
  library rerun is retained separately.
- Final release binary: three real-process SSD cases pass: negotiated HTTP/native
  geometry and restart identity, existing bounded bridge faults, and the full
  public HTTP/native service witness.
- Two physical XFS HDDs, 1+1: the initial existing-bridge and public-service
  witnesses pass; the corrected geometry/restart witness and complete Console
  backup/restore/original-live-upload witness pass. All four original live
  browser cases pass after the cross-client correction.
- Console: **46 Node cases passed**, Svelte check has zero errors/warnings,
  original Office assets verify, and the production build succeeds. Existing
  unrelated Console work is preserved separately.

The initial Console run caught a genuine cross-client discovery regression:
negotiated init did not find an explicit client's pending session. M1 now checks
the saved explicit hint before allocating, returns its original identity/shape,
and serializes both policies on the same hint lock. Its failed resume assertion
also left interrupted sessions visible in the following batch UI check; both
failures and the successful corrected rerun remain in the evidence directories.

Exact final M1 executable SHA-256:
`d70b1e895d6feb5708c1ca6e7b569d0a2e6d137015cab4ba6d1d742f31b20770`.
Final service harness:
`36e6c5b45e4ecd5b0d37e175e2960b0ac42a06af9d1a9f046cb8bfc8ab9b7ae7`.
Benchmark used on both sides:
`a8b83c5c0ba7e48a26dee87c5764847d55dc2b8d90e3bf97626168e949e0375c`.
Frozen N1:
`c9cc0e6f5253723735cfbeec90a5419c6ff96ac0a2c865ef86994ba47ebe6156`.

## Rejected experiments and retained failures

Eager forwarding with per-part readiness counters and a contiguous source spool
was tested before negotiation. It shortened finalize but regressed total
upload speed and increased complexity. It is removed from the accepted source.
For example, spool06's unaligned group measured 56.69 to 43.90 MiB/s and M1 CPU
8.330 to 9.365 s/GiB, despite a roughly 20 ms finalize. The earlier bridge02,
bridge03 and spool05 matrices, source patch and new files remain preserved.
Those experimental test passes do not qualify the selected implementation.

The first negotiation service test passed HTTP upload/restart/readback and then
failed because its new native assertion decoded JSON from `payload` instead of
the streamed response body. The corrected Rust assertion passes; the original
failure log is retained.

A separate fault probe also found a real existing recovery limitation: after
every staged native part is complete, truncating one sealed staging shard can
make finalize correctly refuse publication while replay is also refused as
immutable. Nothing incomplete was published. Recovery of this particular
completed-but-unpublished damaged upload was a required follow-up; the
partial-upload-loss pass is not a substitute. The later
[explicit N1 repair qualification](STAGED-RECOVERY-20261114.md) closes this
specific gap without changing upload retry immutability.

## Evidence

Private run roots:

- SSD test system: `/mnt/DATA_SSD/n1-m1-acceptance-20261113-01`.
- Workstation: `/mnt/HDD1TB001/n1-m1-acceptance-20261113-01`, with the second
  member under the corresponding HDD1TB002 directory.

The `negotiate01-before-a1`, `after-b1`, `after-b2`, `before-a2` report directories
hold raw per-request timing, process CPU/RSS/I/O, host samples, exact executable
hashes, complete readback results, database checks and final file censuses.
`rejected-eager-spool/` holds the discarded source; remote `bridge*` and `spool*`
reports retain its measurements. Workloads and fault assertions are Rust;
Console contracts/browser automation use its existing JavaScript tooling.
No Python, million-population rerun, release or tag is involved.
