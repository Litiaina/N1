# M1 single-part streaming — November 14, 2026

A nonempty upload with one external part that fits the native part capacity now
streams to N1's native part zero, including when the client's preferred part
size is not stripe-aligned. N1 uses
its existing aligned geometry and short last-part rule. M1 retains the client's
size/count, version, session and LINK identity. No payload conversion is needed
for this case, so M1 creates no staged payload file or staged-file receipt.

This change is confined to M1's existing init path. Multipart uploads with
incompatible boundaries still use the bounded bridge. A single external part
larger than the rounded-down native capacity also keeps the bridge. Empty uploads keep their
existing adapter. Existing pending sessions retain their saved plan; resuming
does not silently switch an upload between bridge/native paths. N1's executable,
LINK/shard format, EC, integrity checks and durability are unchanged.

## Guarantees retained

Strict native ACKs still precede the transactional M1 progress bit. The current
GC/owner fence remains in that transaction, active writers retain per-version
exclusion, and final named publication still follows durable N1 finalization.
Relaxed native uploads retain their existing policy without a new durable
per-part receipt. Read-only completed retries and conflicting-byte refusal are
preserved. Finalize remains idempotent with the same identity.

The Rust service witness covers 4,097-byte and 700,003-byte durable uploads,
including multiple native stripes, process kills/restarts before publication,
saved-init reuse, exact progress, matching/conflicting retries and no premature
GET. A normal batched upload is finalized before its restart check; it does not
claim durable part ACKs. The same witness performs actual native NOA multipart
init/part/finalize/GET and verifies complete payloads and zero M1 staged payload
files. The former executable fails the no-spool assertion on the same harness.

The broader existing public-service test caught an error in the first candidate:
zero was treated as aligned, producing a zero-sized native part for an empty
multipart init. The positive-size guard was corrected. That failed candidate
is retained separately and is not the selected executable. Empty multipart,
the existing bridge and other public operations remain required regressions.

## Qualification

The selected executable is `litiaina-m1-singlepart09`, SHA-256
`c6a3d382f8a7f1720d69c80537333e76b971128be36b6d650f8cb08fd4b09042`.
Its Rust service harness is
`60b4dce61d453c6b57136610b0bb5dc11cd8ba69d16c075e1c819c069ca4d179`.
N1 is held at the previously qualified `staged02` executable,
`b08686c7e392ae2fabe30632f56faaac306caddd4ac505fda2551952341eddc5`.
The M1 baseline is `gc-ack04`,
`7ab8a09e4576e6a1585dbc5687849387418d29ba757d54f5efaca3dc143554e0`.
These are release-profile qualification binaries, not a published release.

| Check | Result |
| --- | --- |
| Complete default workspace, all targets | 425 passed, zero failed; 69 explicitly ignored/helper cases |
| Release M1 library | 53 passed, zero failed; two explicit exclusions |
| Workspace Clippy, all targets, warnings denied | Passed |
| Selected real-process SSD service cases | All 11 passed |
| Both new service cases on independent XFS HDDs, N1 1+1 | Both passed |
| Original Console browser workflows against this M1/N1 on two HDDs | All four passed |
| Workstation/remote Rust source comparison | All 363 files matched before the next qualification-only test was added |

The second new service case sets `max_part_size=300001` with a native stripe
of 262144 bytes. Its 300001-byte single external part correctly retains the
bridge to two native parts; an empty object also retains the bridge. Both
survive process restart before finalize, keep their original identity and
progress, pass repeated finalize and return the original bytes. Payload staging
is removed after completion. The other nine selected cases cover public
HTTP/native contracts, negotiated geometry, the multi-part bridge, ACK/GC
exclusion, durable resume, expired-upload/trash GC, in-flight process crashes
and busy-admission versus confirmed-corruption diagnostics.

The browser cases use the original production Console source with only its
already committed negotiation API change. They cover prefix rename/export,
re-adding a multipart file, standard batch upload and reload/resume without
resending acknowledged parts. This pass makes no claim about every operator
workflow or a Console million-object population.

## Small multipart comparison

The SSD test system runs both engines and the Rust client over loopback TLS,
with M1's database and N1's 1+0 member on DATA_SSD. Four alternating
baseline/candidate/ candidate/baseline phases each upload 100 strict durable
4096-byte single-part objects with explicit geometry and no negotiation. Each
upload is finalized, read back with complete SHA-256 verification and reclaimed.
Every phase passes the offline metadata check and a physical member census
containing only the pool marker. Both variants use the identical Rust client
`gc-ack06`, SHA-256
`aaf3725d6b9bd6597dfd114dc1073ad9c9a12616241ac763b7c71d11ce68cdd8`.

These are sequential single-object multipart requests, not batch throughput.
CPU values aggregate process-counter deltas over 200 uploads per side; the
underlying counters have 10 ms granularity. Page cache is not dropped. Desktop
processes remain present; host process/vmstat samples are retained.

| Observation | Previous M1 | Single-part streaming |
| --- | ---: | ---: |
| Complete upload mean / p50, ms | 29.686 / 22.190 | 16.050 / 12.489 |
| Complete upload p95 / p99 / max, ms | 32.462 / 203.987 / 453.871 | 24.009 / 36.247 / 156.480 |
| Init mean / p99, ms | 8.478 / 48.335 | 5.138 / 26.354 |
| Part mean / p99, ms | 8.384 / 72.933 | 6.254 / 11.901 |
| Finalize mean / p99, ms | 12.064 / 81.853 | 4.212 / 7.898 |
| GET mean / p99, ms | 1.303 / 2.520 | 0.781 / 2.368 |
| M1 upload CPU, ms/object | 6.5 | 3.4 |
| N1 upload CPU, ms/object | 4.3 | 2.4 |
| Client upload CPU, ms/object | 2.2 | 1.4 |
| Maximum sampled M1 RSS, KiB | 15,832 | 15,164 |

The complete upload mean improves 45.9% and M1 CPU falls 47.7% in this run.
Individual phase upload means are 28.421 and 30.951 ms for the baseline,
16.029 and 16.072 ms for the candidate. The baseline's long tails are retained;
this scoped workload is not a universal latency or throughput guarantee.

## Large-file controls and limits

Eight additional 1 GiB uploads compare stripe-aligned 8 MiB parts and explicitly
unaligned 8 MiB + 128 KiB parts. These paths retain their previous streaming and
bridge handling respectively. Every payload, repeated finalize and purge passes.
The same older Rust large-file benchmark executable is used on both sides.

The unpinned comparison is adverse: aligned aggregate throughput is 143.024 to
106.821 MiB/s and bridged throughput is 59.739 to 48.862 MiB/s. Aligned M1 CPU
is 2.330 to 2.505 s/GiB; bridged M1 CPU is 5.285 to 6.800 s/GiB. Aligned
transfer times are 7.278/7.041 seconds before and 7.630/11.542 after; bridged
times are 12.185/22.097 before and 21.881/20.033 after. No payload-path change
explains these observations by itself. They remain adverse results requiring
controlled follow-up; a small-object win does not waive large-file acceptance.

The follow-up pins N1, M1 and the same client to performance-core logical CPUs
0–11, preserving TLS, geometry, durability and dataset sizes. All eight more
1 GiB uploads pass the same verification and reclamation checks.

| Pinned observation | Previous M1 | Single-part streaming |
| --- | ---: | ---: |
| Aligned upload, MiB/s | 125.507 | 127.220 |
| Aligned M1 / N1 upload CPU, s/GiB | 0.970 / 1.435 | 1.005 / 1.420 |
| Aligned individual transfers, s | 9.126 / 7.191 | 6.836 / 9.262 |
| Aligned part p99, ms (256 samples/side) | 612.317 | 580.535 |
| Aligned finalize, ms | 2.852 / 45.589 | 74.475 / 3.386 |
| Aligned maximum sampled M1 RSS, KiB | 27,416 | 25,888 |
| Bridged upload, MiB/s | 51.301 | 48.148 |
| Bridged M1 / N1 upload CPU, s/GiB | 2.670 / 1.315 | 2.625 / 1.330 |
| Bridged individual transfers, s | 15.725 / 24.196 | 21.799 / 20.736 |
| Bridged part p99, ms (254 samples/side) | 1956.182 | 1749.867 |
| Bridged finalize, ms | 10532.134 / 10308.633 | 10384.416 / 10189.309 |
| Bridged maximum sampled M1 RSS, KiB | 29,232 | 26,740 |

The pinned aligned path is 1.4% faster in aggregate, with M1 CPU 3.6% higher
and N1 CPU 1.0% lower. The pinned bridge still loses 6.1% throughput while M1
CPU falls 1.7%; its baseline transfer times differ by 8.47 seconds. CPU affinity
materially changes these observations, but this does not establish the sole
cause of the unpinned regression or waive the retained bridged cost. There is
no universal performance-parity claim. Native negotiation remains available
for clients which can choose compatible multi-part boundaries; explicit
incompatible sessions retain their existing contract.

Object-level p99 is not inferred from two large uploads per shape. Physical
power loss, final-candidate the four-HDD reference system topology and broader
saturation/maintenance remain separate release gates. No new million-object
campaign was started.

## Evidence

The SSD test system run root is `/mnt/DATA_SSD/n1-m1-acceptance-20261113-01`.
Its `source/` holds the exact overlays, build/service drivers, manifests and
alternating benchmark drivers; `reports/singlepart09-*` holds raw checks and
measurements. The workstation copy is under
`/mnt/HDD1TB001/n1-m1-acceptance-20261113-01/remote-singlepart09`. Physical
witnesses are under `reports/singlepart09-hdd`, and original browser evidence is
under `reports/console-singlepart09-live` on the workstation run. Earlier failed
candidates, including the empty-object failure, remain in the same private
evidence roots. No Python was used.
