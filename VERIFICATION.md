# N1 Verification Report

This document tracks release proof and hardening evidence. It is intentionally separate from the user guide.

The current package line is N1 `3.5.0` on the development channel, using
engine format `3`. Stable product status has not been declared for this engine
line. Dated evidence names below remain unchanged because they identify the
exact candidate binaries and runs that produced them.

## Required Release Gates

Before an LTS release, N1 should pass:

- `RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets`
- `RUSTFLAGS='-D warnings' cargo check --workspace --all-targets`
- large multipart upload with restart/resume
- download hash match
- range/view streaming
- CRUD and POSIX namespace operations
- generational trash, strict pre-TTL recover, TTL expiry, targeted reclamation, and removed purge surfaces
- recover-on-read after one disk is wiped
- repair after simulated disk loss or reformat
- autonomous repair after recoverable data- and parity-shard loss
- automatic read-only entry/recovery for missing and identity-mismatched roots
- automatic restoration of a missing fragment alias when a surviving alias and matching physical fragment metadata provide unambiguous authority
- healer-fault read-only persistence until an unresolved repair succeeds
- old and new per-object EC layouts remain valid across an EC configuration change
- redistribution `2 roots -> 1 root -> 2 roots`
- high object-count upload storm
- concurrent users on one fragment
- concurrent users across multiple fragments
- memory/RSS observation under sustained load
- UI-backed smoke against the real N1 API
- native `n1://` auth/list/stat/CRUD smoke and transport benchmark
- index-only roots contain no object metadata or payload
- blob-only roots contain no alias, upload, trash, or user namespace state
- every shard-bearing placement has a colocated immutable `.ndx`
- adaptive small-object stripe sizes do not create false topology drift
- standard multipart init and part commits create no manifest, pending binding, or receipt
- distinct standard multipart parts execute concurrently and remain invisible before finalize
- standard finalize synchronizes K+M shard files before NDX and namespace publication
- unfinished standard sessions vanish on restart and leave only TTL-reclaimable headless shards
- durable multipart remains restart-resumable through its existing manifest and receipt contract
- upload evidence separates server-local engine throughput from LAN client throughput
- no upload path performs synchronous topology-wide or selected-root free-space probes
- kernel `ENOSPC`/quota failures return `507` and publish no object or namespace truth
- standard small-object batch upload proves bounded concurrency, mixed-result isolation, and per-object EC/namespace identity

## Rust Release Proof Runner

The release proof runner lives under `tools/n1-release-proof`.

Shell may prepare the environment and start the Rust binary, but Python is not part of the N1 release verification path.

Example:

```bash
SOURCE_FILE="/home/administrator/Downloads/INCDT 122225.mp4" \
DISK_ROOTS="/mnt/disk-00,/mnt/disk-01" \
scripts/bench/run_n1_release_proof.sh
```

For a large multipart proof, use a source file that exceeds the configured
one-shot size. Test the standard and durable paths separately: the standard
path proves concurrent ingest and finalize publication, while the durable path
proves persisted receipts and restart/resume. Both require download hash
verification.

## Focused Proof Evidence

### N1 3.5.0 Capacity-Free Standard Batch Proof

On August 10, 2026, the N1 3.5.0 candidate passed a focused real-process
NOA proof using the release binary with SHA-256
`715d040e03a77dac6741564b80f0a37b1532ae9bb14bcfc5323c156b0a642b6d`.
The proof used an explicit local `1 + 0` functional topology so it could run
without touching the independent N2 test or pretending that two directories on
one physical device were a durable multi-disk topology.

- Strict N1 all-target tests passed `143/143` library tests plus the server
  binary target with warnings denied.
- Static call-site inspection confirmed that one-shot, standard multipart, and
  durable multipart no longer call the synchronous global or selected-root
  free-space probes. Kernel writes are authoritative; `ENOSPC` and `EDQUOT`
  map to HTTP/native status `507`, while publication remains after successful
  EC and metadata completion.
- A real 128 MiB loopback ext4 filesystem was reduced to `2,093,056` free
  bytes before an 8 MiB one-shot upload. The kernel `ENOSPC` returned `507`
  without hanging; afterward there were zero matching namespace entries,
  temporary shards, published shards, or object NDX files.
- The individual baseline uploaded `256` independent 8 KiB objects at
  `228.522 objects/s` (`1.785 MiB/s`) with client concurrency `32`.
- The standard batch route uploaded the same `256` independent 8 KiB objects
  in one request at `549.702 objects/s` (`4.295 MiB/s`) with bounded engine
  concurrency `32`, a `2.41x` object-rate improvement in this proof.
- The batch included one deliberate directory conflict. The response returned
  `207 Multi-Status`, committed all `256` valid objects, isolated the conflict
  as `409`, and did not collapse the batch into one failure domain.
- Listing, sampled SHA-256 downloads, per-object health, physical index
  inspection, restart persistence, POSIX/CRUD/lifecycle checks, and maintenance
  status checks all passed.
- Both pre-restart and post-restart server logs passed with no warning, error,
  panic, or failed lines.

Machine-readable evidence is retained at
`/home/litiaina/DEV_TEST/n1-3.5/results/noa-batch-proof-4/summary.json` and
`/home/litiaina/DEV_TEST/n1-3.5/enospc-proof/summary.json`. This focused proof
establishes the 3.5.0 upload-contract change and functional publication
invariants; its single-device `1 + 0` topology is not evidence of multi-disk EC
fault tolerance.

### N1 3.4.0 Fragment-Alias Self-Healing Proof

On August 8, 2026, an isolated two-index-root N1 instance created a real
fragment, then lost the alias symlink on one root while its physical
`@f_<stable-id>` directory and `.fragment.ndx` remained intact. The next
automatic availability scan reported `discovered=1 missing=1 repaired=1
ambiguous=0` and recreated the exact relative alias. A following scan repaired
nothing, proving idempotence. The focused unit regression independently
verifies that reconciliation does not rewrite fragment metadata or namespace
content. Conflicting or ambiguous alias authority is not guessed and remains a
fail-closed condition. Evidence is retained under
`/home/litiaina/DEV_TEST/n1-alias-self-heal`.

### N1 3.4.0 Standard Multipart Upload Proof

The final optimized N1 3.4.0 candidate was exercised on August 8, 2026 in an
isolated `3 + 1` topology backed by four distinct 1 TB HDDs. Production N1
remained online on separate paths and ports. The exact release binary SHA-256
was `48c352a794eb10b3485297e2a750b86236cebd0c1471f2ace396eacd82c117d0`.

- Strict `RUSTFLAGS='-D warnings'` N1 all-target tests passed `134/134` library
  tests plus the server binary target; strict workspace all-target compile,
  formatting, release build, and diff validation passed.
- The real `15,912,736,534` byte `mhw.mp4` source and every streamed download
  matched SHA-256
  `3bfa2816d5ee2d61506137fbabe119d1161e768983bdb924367f790b6223eeac`.
- Server-local standard multipart with 256 MiB parts and concurrency four
  ingested at `165.796 MiB/s`; the `2.927 s` finalize produced an end-to-end
  upload rate of `160.443 MiB/s`.
- LAN standard multipart ingested at `94.396 MiB/s`; the `0.557 s` finalize
  produced an end-to-end upload rate of `93.984 MiB/s`, near the payload
  ceiling of the tested 1 Gbit/s link.
- Final-binary 256 MiB one-shot samples measured `36.765 MiB/s` server-local
  and `48.479 MiB/s` over LAN. Earlier samples reached `96.803 MiB/s`, so this
  path is explicitly recorded as variable rather than represented by a single
  peak result.
- A bounded 12-second `perf stat` operation window around a 1 GiB standard
  upload observed `1,574,221,979` CPU cycles, `1,351,281,667` instructions,
  `1,612.73 ms` task-clock, and `4,204` context switches. That run ingested at
  `131.769 MiB/s` and finalized in `1.094751 s`.
- Direct physical inspection found four part shard files and zero standard
  multipart manifests, pending bindings, or receipts.
- After forced process restart, the standard session identity was absent and
  its committed-part count was zero. Fast orphan GC reclaimed every abandoned
  shard while preserving and SHA-verifying a committed object.
- The separate durable route retained its old behavior: one committed part
  survived restart, the remaining parts finalized, and the download hash
  matched.

Complete machine-readable results and the human-readable report are under
`/home/litiaina/DEV_TEST/n1-3.4`. This evidence proves the 3.4.0 upload redesign
scope; it does not by itself replace the wider N1 LTS release matrix.

Latest native protocol benchmark for N1 1.2.0:

- Command path: Rust release proof `--native-transport-benchmark`.
- Build mode: release.
- Setup: one N1 server, 64 real namespace objects, same fragment for HTTPS and native.
- Iterations: 1,000 measured requests after 100 warmups per operation.
- Native listener: `n1://127.0.0.1:<native_port>`.
- HTTPS listener: `https://127.0.0.1:<https_port>`.
- Result: `PASS`.

Measured loopback results after enabling `TCP_NODELAY`:

| Operation | HTTPS ops/sec | Native ops/sec | HTTPS p50 | Native p50 |
| --- | ---: | ---: | ---: | ---: |
| ping | 7,566 | 20,093 | 111 us | 47 us |
| posix stat | 3,588 | 6,088 typed / 6,807 api bridge | 237 us | 145 us typed / 134 us api bridge |
| posix list, 64 entries | 653 | 536 typed / 725 api bridge | 1,347 us | 1,442 us typed / 1,389 us api bridge |

Interpretation: native `n1://` is materially faster for small control-plane and metadata calls. Listing is less transport-bound because direct namespace work and response body size dominate. The benchmark also caught a real protocol weakness before release: without `TCP_NODELAY`, tiny native frames hit delayed-ACK/Nagle latency around 82 ms per operation.

Previous N1 1.x local proof:

- `cargo test -p n1 --all-targets` passed with `RUSTFLAGS=-D warnings`.
- Real proof on `/mnt/disk-00` and `/mnt/disk-01` passed CRUD, POSIX, archive, multipart, recover-on-read, simulated disk-format repair, and redistribution `2 roots -> 1 root -> 2 roots`.
- The proof ended with matching object and namespace counts on both roots after re-expansion.
- Server RSS stayed around `52 MiB` during that focused proof.
- Four-user storm passed on the same two disks: `12,000` one-shot objects plus `4` multipart objects of `16 MiB` each.
- POSIX listing returned `12,004` objects exactly after the storm.
- Storm memory remained bounded: max server RSS was about `258 MiB` with `42` threads.
- Storm throughput on the bad-disk tiny-object workload was `14.343 objects/s` and `0.203 MiB/s`, matching the expected IO/fsync-heavy small-object tradeoff.

N1 2.0.0 evidence is recorded only after the namespace-only index and
colocated object metadata/payload layout passes the full two-disk proof. N1 1.x
proof results do not establish N1 2.0.0 disk-layout correctness.

N1 2.0.0 two-disk proof completed on June 11, 2026:

- Colocated mode: `PASS`, `212` live objects, identical `212`-link namespace
  replicas, `256 MiB` restart/resume multipart, native protocol, CRUD, GC,
  recover-on-read, object repair, disk-format repair, and `2 -> 1 -> 2`
  redistribution.
- Split mode: `PASS`, `94` live objects, identical `94`-link namespace
  replicas, the same recovery matrix, zero namespace/control entries on
  blob-only roots, and zero `.objects` entries on index-only roots.
- Colocated peak RSS: `158476 KiB`.
- Split-root peak RSS: `125652 KiB`.
- Both final log scans: `PASS` with no bad lines.
- The explicit object-level redistribution drill wrote one object under the
  one-disk `1 + 0` topology, restored the two-disk `1 + 1` topology, scanned
  exactly one object, detected one mismatch, migrated one object, and reported
  zero errors. Physical inspection found its immutable `.ndx` and payload on
  both shard-bearing roots before fragment redistribution processed the
  remaining objects.
- Optimized colocated upload benchmark, three samples per mode:
  - HTTP one-shot: `32.35 MiB/s` average.
  - HTTP multipart: `37.57 MiB/s` average with conservative concurrency `1`;
    `39.25 MiB/s` average with concurrency `2`.
  - HTTP pipelined multipart: `41.47 MiB/s` average with concurrency `2`.
  - Native multipart: `37.14 MiB/s` average with concurrency `1`;
    `40.82 MiB/s` average with concurrency `2`.
  - Native pipelined multipart: `36.34 MiB/s` average with concurrency `2`.
  - Benchmark peak RSS stayed below `87 MiB`; both benchmark log scans passed
    with no bad lines.
- Disk telemetry identified the slower `/dev/sdb` path as the limiting device;
  the benchmark is a measurement of this local disk pair, not a universal N1
  throughput ceiling.

N1 2.0.0 IO-control proof completed on June 11, 2026:

- `RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets` passed with all
  `102` N1 library tests.
- `cargo test --workspace --all-targets` passed, including the `7` N1 Shell
  integration tests, `107` SIMD unit tests, `5` SIMD integration tests, and
  the SIMD benchmark targets.
- The real two-disk release proof on `/mnt/disk-00` and `/mnt/disk-01`
  completed with `N1_RELEASE_PROOF PASS`.
- The proof produced `164` live objects. Nineteen gates passed; fragment usage
  was verified through the storage usage API.
- The `256 MiB` multipart upload restarted after three parts, resumed,
  finalized in `7.220 s`, and downloaded with a matching SHA-256. Measured
  upload throughput was `35.459 MiB/s` on this local HDD pair.
- Native protocol, restart persistence, permission boundaries, namespace
  deltas, concurrent multipart, same-key contention, multi-fragment
  isolation, CRUD/POSIX/trash/recover/GC, archive, recover-on-read, object
  repair, disk-format repair, and `2 -> 1 -> 2` redistribution all passed.
- Strict disk-format repair scanned `163` objects, repaired `190` shards and
  `2` missing metadata replicas, and reported `0` corrupt records.
- Object-level redistribution scanned one mismatched object, migrated it into
  the restored topology, and reported `0` errors.
- Peak RSS was `128152 KiB` during recover-on-read/object repair. Final RSS
  after redistribution and clean health was `47996 KiB`.
- The final log scan passed with `bad_lines: []`.
- Evidence: `/tmp/n1-2.0-io-control-proof/summary.json`.

## N1 3.2.0 NOA v1 API Stabilization

N1 3.2.0 preserves engine format 3 and the complete N1 3.1.1 storage behavior.
Its intentional contract change is limited to the universal HTTPS and WebSocket
surface: the existing route structure is now the Litiaina Node Object API (NOA)
v1 under `/noa/v1`. N1 does not retain `/n1/...` aliases, because doing so would
create a second permanent public contract. The native `n1://` wire protocol is
unchanged and remains N1-specific.

The N1 Shell, release-proof client, signed-read URL generator, documentation,
and N1 Console are migrated to the canonical NOA paths. Space and Space UI are
intentionally not modified in this release task. Because no engine, metadata,
namespace, EC, lifecycle, or healing implementation changed, the 3.1.1 engine
evidence below remains the storage-behavior baseline. N1 3.2.0 still requires
focused route, client, and build verification; those bounded gates are recorded
next.

The bounded API-stabilization verification completed on July 15, 2026:

- NOA route identity and retained lifecycle guards passed `2/2`; the canonical
  `/noa/v1/ping` route resolves successfully and the former `/n1/ping` route is
  absent from the isolated public route graph.
- Signed-read URL generation passed its focused encoding regression and emits
  `/noa/v1/objects/signed-read`.
- N1 Shell HTTP/native mapping integration passed `7/7` against NOA paths.
- `cargo check --workspace --all-targets` passed, including the migrated release
  proof client.
- Freshly linked N1 and N1 Shell binaries both report package version `3.2.0`;
  the intentionally separate native wire protocol remains `n1/0.5`.
- N1 Console `npm run check` passed with zero errors and warnings, the production
  build passed, and the responsive Playwright matrix completed with `36` passed,
  `4` intentionally skipped, and `0` failed.

The optimized real-process NOA gate also passed on July 15, 2026. The final
physical-topology run used the production-class test server at `192.168.0.12`
without modifying either deployed N1 service:

- Binary SHA-256:
  `06318b048db17dcc48d32ef272435f2b1a0a59d890d0b752d5b9d9d10f5e6832`.
- Four distinct physical XFS HDDs were used at `/mnt/DISK0` through
  `/mnt/DISK3`, backed by `/dev/sda1` through `/dev/sdd1`. The proof configured
  `3 + 1` EC with colocated roots, so all four disks were both index and blob
  roots.
- Canonical NOA ping/auth, upload, list/stat, full download hash, range read,
  object status, realtime WebSocket, file/folder rename, trash/recover, TTL
  reclamation, GC status, repair status, redistribution status, deep health,
  process restart, and post-restart persistence all passed.
- The former `/n1/ping` returned `404` both before and after restart.
- Final inspection found `4` live namespace links and exactly `4` matching
  metadata/payload versions on each of the four physical roots. All roots had
  distinct Linux device identities. The expired test version was reclaimed and
  the GC reported zero errors.
- Both server log scans completed with `bad_lines: []`; the temporary process,
  ports, and proof roots were removed. Both pre-existing production N1
  processes retained their PIDs and remained active throughout the run.
- Evidence:
  `/home/wlzambo/DEV-TEST/n1-3.2.0-noa-v1-four-disk-proof-20260715-154322/REPORT.md`
  and `summary.json` in the same directory.

No long-running, fault-injection, throughput, or soak proof was rerun for this
API-only change. The bounded `3 + 1` gate is real four-HDD API, layout,
lifecycle, health, and restart evidence. It does not replace the autonomous
healing and storage-engine fault-injection evidence recorded below.

## N1 3.1.1 Development-Engine Autonomous-Maintenance Confidence Matrix

N1 3.1.1 intentionally has no metadata or upload-manifest compatibility
with an earlier engine. The package, native client, and shell versions are
aligned at `3.1.1`. This development line incorporates the earlier 3.0.x
namespace/lifecycle hardening and adds object-owned EC topology plus autonomous
repair. Historical proof names retain the version used when each run completed.
Version 3.1.1 is a bug-fix package over 3.1.0: it preserves engine format 3 and
the established architecture while correcting the internal trash-maintenance
projection so repair always consumes immutable physical object identities.

| Edge case | Locked invariant | Regression proof | Status |
| --- | --- | --- | --- |
| A disk mount disappears but its underlying directory remains writable | The path must still contain the enrolled `.n1-root-id`; a known path is never rebound automatically | `missing_or_replaced_root_never_rebinds_automatically` | PASS |
| A new configured disk is added | Its identity is enrolled without changing any existing root identity or object metadata | `new_root_enrollment_preserves_existing_root_identity` | PASS |
| An operator intentionally replaces a disk at the same path | Removing the path from configuration decommissions the old identity; re-adding it enrolls the replacement without registry editing | `explicit_config_removal_allows_controlled_disk_replacement` | PASS |
| Blob paths alias fewer physical devices than `D + P` | New writes remain read-only until enough distinct registered devices exist | `insufficient_or_non_distinct_roots_fail_closed` | PASS |
| More disks are added while EC remains `4 + 2` | Current EC width remains static and the expanded pool is write-safe | `expanded_topology_stays_write_safe_without_changing_ec_width` | PASS |
| Multipart contains multiple parts | Every part maps shard index `i` to the same fixed object root `i` | `multipart_parts_keep_one_fixed_object_ec_set` | PASS |
| Added roots expand placement choices | Deterministic per-object rendezvous placement remains bounded and spreads new versions across the larger pool | `object_ec_set_selection_is_deterministic_and_bounded`, `different_object_versions_spread_across_expanded_pool` | PASS |
| New upload loses any shard writer | The version is rejected before metadata/namespace publication; N1 never commits an under-protected new EC set | upload writer and finalize invariant suite | PASS |
| Repair relocates a shard and stale metadata returns | The relocated metadata has a higher placement generation; lower generations cannot win | `object_metadata_uses_highest_placement_generation_and_rejects_ties` | PASS |
| Equal placement generations disagree | Object metadata selection fails closed rather than guessing authority | `object_metadata_uses_highest_placement_generation_and_rejects_ties` | PASS |
| Scheduled repair cannot find a distinct replacement or cannot complete IO | The scan records unresolved debt and finishes failed, causing the autonomous controller to retain read-only mode | `unresolved_automatic_repair_debt_fails_the_scan` | PASS |
| Topology or healer read-only is enforced through HTTP and direct/native paths | Mutations receive `503 Service Unavailable`; reads remain available unless explicitly quiesced | `persistent_safety_read_only_returns_service_unavailable`, operation-gate suite | PASS |
| Disk/EC configuration changes but recorded objects remain healthy | Health treats the recorded object layout as authoritative and reports current-layout mismatch only as optional redistribution opportunity | monitoring and object-layout unit suite | PASS |
| A deleted file has a POSIX name that is not a valid physical object id | User trash listings expose the original path, but repair and redistribution retain the immutable physical object id | `trash_maintenance_scan_preserves_physical_object_id` | PASS |
| A deleted directory contains one or more immutable objects | Maintenance descends through the directory trash payload and checks every referenced object instead of treating the directory anchor as an object | `trash_maintenance_scan_descends_into_deleted_directories` | PASS |

Fast, non-soak verification commands for this implementation:

```bash
cargo fmt --all -- --check
cargo check -p n1 --all-targets
cargo test -p n1 --lib -- --test-threads=1
cargo test -p n1 --bin litiaina-n1 -- --test-threads=1
cargo check --workspace --all-targets
cargo test --workspace
```

The fast N1 library gate passed all `131` tests on July 15, 2026. No soak or
long-running proof was started, per the implementation-stage test boundary.
The ordinary workspace matrix also passed: proof runner `3/3`, N1 Shell
integration `7/7`, SIMD unit `107` passed with `6` intentional ignores, SIMD
integration `5` passed with `2` intentional ignores, and documentation tests
`4/4`. All workspace targets, including benchmark targets, compiled.
The real HDD+SSD evidence below remains valid for the immutable-object, EC,
namespace, CRUD, POSIX, recovery, repair, and redistribution primitives carried
into N1 3.1.1. The historical evidence-directory names retain the package label
used by each run; the dedicated `--auto-healing-proof` section separately
records direct failure-injection proof of the autonomous controller.

On July 15, 2026, a real Space-driven trash/recover cycle exposed and closed
an internal catalog-boundary defect: the user-facing trash projection was
passing an original POSIX path into object maintenance, which correctly
rejected it as an invalid N1 object id and held the engine read-only. The
maintenance catalog now preserves immutable physical ids and scans objects
inside directory trash records, while the user catalog continues to expose
logical names. The full workspace suite passed with `131/131` N1 library
tests. A restarted HDD+SSD `1 + 1` smoke process then completed an automatic
availability scan with `3` objects scanned and no failures, and a Space cycle
successfully deleted and recovered its test object. That smoke established
the original reproduction was closed, but was not treated as pagination or
fault-injection evidence.

A dedicated bounded real-disk proof subsequently ran against the HDD
`/mnt/DISK001` and SSD `/home/wlzambo/SERVER-TEST/disk-00`, using colocated
index/blob roots and minimum `1 + 1` EC. It placed one file and a 140-object
nested directory in trash, deliberately removed a parity shard from the
directly trashed file and from `item-139.bin`, then restarted N1 while all 141
objects remained in trash. The second fault is beyond the configured
`metadata_batch_size=128` boundary, so successful repair requires both
directory expansion and maintenance continuation. After restart, the
availability job reported `Scanned: 141, Repaired: 2, Corrupt: 0, Failed: 0,
MissingMeta: 0, DivergentMeta: 0`. Both deleted paths were recovered and all
141 downloads matched their expected SHA-256 values. Final health was
`write_safe=true`, `phase=idle`, `read_only_reason=null`, and
`last_error=null`. The machine-readable result and server logs are retained
under `/home/wlzambo/DEV-TEST/n1-3.1.0-trash-maintenance-real-20260715`.

The first invocation of this new proof was invalidated during setup because
the proof-only five-second checksum scrub entered strict maintenance while
uploads were still being created. The harness now keeps the checksum scrub
outside this bounded availability scenario; the corrected invocation above
completed with `N1_RELEASE_PROOF PASS`. This was a harness scheduling issue,
not counted as engine evidence or an engine pass.

### N1 3.1.0 Fast Real-Disk Smoke

On July 14, 2026, the autonomous-maintenance implementation carried into N1
3.1.0 ran a bounded native/CRUD smoke on the real XFS HDD `/mnt/DISK001` and
ext4 NVMe root containing
`/home/wlzambo/SERVER-TEST/disk-00`, using colocated index/blob roots and
`1 + 1` EC. The controller enrolled two root identities on two distinct Linux
devices, validated the topology, and automatically left startup read-only
mode. The harness then passed HTTP upload/download plus native authentication,
one-shot upload, multipart, namespace/POSIX movement, trash/recovery, bulk
operations, capacity/usage, object status/repair, and maintenance control
surfaces. The repair check scanned `7` objects with `0` failed repairs,
corruption, missing metadata, or divergence; EC telemetry reported `errors=0`.
The completed summary is:

`/home/wlzambo/DEV-TEST/n1-3.0.0-fast-smoke-20260714-222903/summary.json`

The server log contains no panic, fatal, failed, or nonzero error record. Test
storage roots were removed by the harness; configuration, log, summary, and
the durable topology registry remain in the evidence directory. Scheduled
auto scans were disabled for this short smoke to avoid turning it into a soak;
their scheduling/fail-safe behavior is covered by the fast regression matrix.

### N1 3.1.0 Autonomous-Healing Real-Disk Proof

On July 14, 2026, the bounded `--auto-healing-proof` release gate passed for
the autonomous-healing implementation carried into N1 3.1.0 on the real XFS
HDD `/mnt/DISK001` and the ext4 NVMe device containing
`/home/wlzambo/SERVER-TEST/disk-00`. The roots were colocated index/blob
roots on two distinct Linux devices with minimum `1 + 1` EC. This was a short
failure-injection proof, not a soak.

The gate proved all of the following against a running N1 process:

- deleting a data shard preserved a correct download and recover-on-read
  recreated the exact recorded shard path;
- deleting a parity shard was detected and repaired by the scheduled
  availability scan;
- removing one registered root changed health to degraded/read-only, kept
  reads available from the remaining `1 + 1` shard, and rejected a mutation
  with `503 Service Unavailable`;
- restoring the authoritative root returned the engine to availability after
  the configured stability interval without operator-triggered repair;
- replacing that path with an empty directory did not rebind it: the disk
  matrix reported a registered-root identity mismatch and N1 stayed
  read-only until the original root returned;
- removing every shard forced the autonomous scan to `scan_failed` and kept
  N1 read-only; restoring one reconstructable shard allowed the retry to
  rebuild the other shard, clear the healer fault, and return to availability;
- changing new-write EC from `1 + 1` to `1 + 0` preserved the old object's two
  recorded roots through an autonomous scan, while the new object used one
  root. No redistribution was requested or required.

Final health was `healthy`, the controller phase was `idle`, and maintenance
access was `availability`. The harness removed its temporary storage roots.
Expected fail-safe errors appear while damage is intentionally unrecoverable;
the logs contain no panic. Reproducible evidence is stored at:

`/home/wlzambo/DEV-TEST/n1-3.0.0-auto-healing-fast-20260714/summary.json`

The reusable command path is `n1-release-proof --auto-healing-proof`; every
state wait is bounded to 30 seconds.

### N1 3.1.1 Self-Healing And Maintenance Coverage

On July 15, 2026, a fresh optimized N1 3.1.1 release binary completed three
isolated real-device gates using the HDD `/mnt/DISK001`, SSD
`/home/wlzambo/SERVER-TEST/disk-00`, colocated index/blob roots, and minimum
`1 + 1` EC.

The dedicated `--maintenance-coverage-proof` established that Console Deep
Scan plus Fresh Snapshot is observational: access remained `availability`, the
healer stayed write-safe with no error, and an immediate mutation succeeded.
It also restarted N1 with an unfinished multipart upload, repeated the deep
scan, resumed and finalized the upload, and verified its SHA-256. Manual
availability repair permitted a concurrent write; checksum and strict repair
temporarily rejected writes with HTTP `403` while checksum preserved reads;
scrub temporarily rejected reads and writes with HTTP `503`. Every clean mode
completion returned to `availability` with zero failed repairs and no
persistent healer fault.

The optimized `--auto-healing-proof` passed recover-on-read, scheduled parity
repair, missing-root fail-safe, replacement-identity rejection, unrecoverable
object `scan_failed`, automatic recovery after one shard returned, and
prospective EC-change preservation. The optimized `--trash-maintenance-proof`
passed the 141-object directory/pagination/restart/two-shard repair and recovery
matrix. `cargo test --workspace --no-fail-fast` also passed: N1 `131/131`,
proof runner `3/3`, Shell integration `7/7`, SIMD unit `107` passed with `6`
ignored, SIMD integration `5` passed with `2` ignored, and docs `4/4`.

That first coverage pass identified an autonomous-availability mismatch. The
controller scheduled automatic checksum work using the same read-only default
as an operator-requested checksum. This explained the periodic temporary gate
seen in the interactive run; Deep Scan and Fresh Snapshot did not cause it.

The mismatch is now resolved without weakening fail-safe behavior. Repair mode
policy distinguishes the operator default from the algorithm's safe minimum:
a manual checksum remains read-only, strict remains read-only, scrub remains
quiesced, and a healthy scheduled checksum runs online. Topology failure,
root-identity failure, and unresolved automatic repair debt still activate the
persistent read-only gate. The online path is valid because repair re-reads the
current immutable version under its object-operation lock and publishes any
placement change atomically.

The correction then passed an isolated optimized `3 + 1` matrix on four
distinct XFS devices at `/mnt/DISK0` through `/mnt/DISK3`. While health reported
`running_checksum_scrub`, maintenance access remained `availability`; a real
foreground mutation returned HTTP `201`, and the following health snapshot
still reported the checksum running in `availability`. Missing-root and
unrecoverable-object injections continued to enter read-only and reject unsafe
mutations. Manual maintenance modes, 141-object trash repair/recovery, and a
170-object lifecycle/release matrix also passed. The latter covered TTL GC,
POSIX CRUD, native protocol, multipart restart/resume, concurrency, archive,
recover-on-read, object repair, simulated disk-format repair, redistribution,
physical placement inspection, and a clean log audit.

The first lifecycle wrapper result was a harness false positive: it matched the
zero counter `Failed: 0`. The scanner was patched to parse ANSI severity and
retain rejection of real warning/error/panic/degraded/nonzero-failure evidence.
Its regression test and a full lifecycle rerun passed. Current workspace totals
are N1 `132/132`, proof runner `4/4`, Shell integration `7/7`, SIMD unit `107`
passed with `6` ignored, SIMD integration `5` passed with `2` ignored, and docs
`4/4`.

The proof used uniquely named nested directories and low CPU/IO priority. Both
pre-existing production N1 services retained their original PIDs and remained
active. After copying evidence, all temporary processes, ports, OS files, and
four disk roots were removed. Pre/post top-level disk entry/inode fingerprints
matched exactly on every disk.

Evidence is retained at:

- `/home/wlzambo/DEV-TEST/n1-3.1.1-maintenance-coverage-20260715/summary.json`
- `/home/wlzambo/DEV-TEST/n1-3.1.1-auto-healing-coverage-20260715/summary.json`
- `/home/wlzambo/DEV-TEST/n1-3.1.1-trash-healing-coverage-20260715/summary.json`
- `/home/wlzambo/DEV-TEST/n1-3.1.1-self-healing-coverage-20260715/REPORT.md`
- `/home/wlzambo/DEV-TEST/n1-3.1.1-four-disk-self-healing-20260715-103538/REPORT.md`
- `/home/wlzambo/DEV-TEST/n1-3.1.1-four-disk-self-healing-20260715-103538/final-results/summaries/`

## N1 3.3.0 Generational Trash Evidence

N1 3.3.0 changes only the format-3 trash namespace. Every deletion receives a
timestamp generation directory containing the unchanged logical path. This
removes same-path delete collisions without adding `.entries`, `.records`, a
database, a journal, or a foreground subtree scan. Live namespace paths,
immutable object metadata, EC layouts, and blob payload placement are unchanged.

| Edge case | Locked invariant | Regression proof | Status |
| --- | --- | --- | --- |
| The same file path is uploaded and deleted repeatedly | Every delete has a distinct `trash_entry_id`; neither retained generation is overwritten | `n1_repeated_delete_creates_independent_generations_and_recover_stays_strict` | PASS |
| The same directory path is recreated and deleted repeatedly | Each directory remains one rename and every retained generation remains independently addressable | `n1_repeated_directory_delete_keeps_each_generation_recoverable` | PASS |
| Recovery destination already exists | Exact-id and path recovery return `409 Conflict` and leave the retained generation unchanged | repeated file and directory generation tests | PASS |
| TTL expires one generation | Lifecycle scans `.trash` generations only and reclaims exact objects only after remaining-reference verification | `n1_soft_delete_ttl_reclaims_only_after_namespace_expiry`, `n1_ttl_reclaim_preserves_an_object_still_referenced_live` | PASS |
| Recover races TTL | One filesystem rename/removal wins without a namespace lifecycle lock; reachable object data survives | `n1_recover_and_ttl_sweep_race_preserves_data_without_a_namespace_lock` | PASS |
| A 3.2 deployment already has trash entries | The offline migration validates every replica before moving trash anchors; it never touches live namespace or blob paths | `scripts/migrate-n1-3.3-trash.sh` synthetic three-root dry-run/apply proof | PASS |

The complete workspace and production-deployment results are recorded after the
bounded release test and live deployment below. Historical N1 3.2.2 evidence
remains unchanged because it describes the exact earlier binary and layout.

### N1 3.3.0 Test And Production Deployment

On July 19, 2026, warning-as-error N1 all-target tests passed `132/132`, the
complete workspace tests passed, and N1 Shell integration tests passed `7/7`.
The release binary reported `litiaina-n1 3.3.0` with SHA-256
`c77e88b0a51bc959a360231d43f133366ee86c4cb1f7962486dc628aa0843a7b`.

Production deployment on `192.168.0.36` stopped `litiaina-n1`, backed up the
3.2.2 binary, configuration, environment, and all existing trash namespace
replicas under
`/var/lib/litiaina/n1/backups/n1-3.3.0-20260719-063051`, then migrated six
legacy trash anchors across `/mnt/NDISK001`, `/mnt/NDISK002`, and
`/mnt/NDISK003`. The preflight proved every anchor type, symlink target, or
directory tree identical across the three replicas before the first move.
Before/after hashes excluding `.trash`, `.objects`, and `.uploads` were
byte-identical on every root, proving the migration changed no live namespace
path. The deployed production-only retention is `86400` seconds. The service
restarted active on `0.0.0.0:50001` and `0.0.0.0:50002`; its deployed binary
hash matched the tested release, all trash-replica hashes matched, and the
post-start journal contained no warning-or-higher entry.

## N1 3.2.2 Namespace And Lifecycle Evidence

N1 3.2.2 restores the proven 1.0.1 filesystem-native namespace mutation path
while retaining engine-format-3 immutable objects, object-level EC placement,
NOA, recover-on-read, redistribution, and autonomous healing. Trash TTL is the
only lifecycle addition: the physical `.trash/<logical-path>` entry and its
filesystem modification time are authoritative. There are no `.entries`,
`.records`, reclamation receipts, namespace generations, or foreground
directory-sync chains.

| Edge case | Locked invariant | Regression proof | Status |
| --- | --- | --- | --- |
| Direct file soft delete | Delete performs one rename to `.trash/<same-logical-path>` per index root and writes no lifecycle record | `n1_soft_delete_ttl_reclaims_only_after_namespace_expiry` | PASS |
| Folder delete and recover | Both operations rename the directory anchor without walking descendants | `n1_trash_list_shows_only_explicit_folder_delete_anchor`, `n1_resolve_any_and_recover_use_the_physical_trash_path` | PASS |
| Repeated delete at one logical path | The existing trash slot returns `Conflict`; neither live nor retained data is overwritten | `n1_trash_path_collision_is_a_conflict_and_never_overwrites` | PASS |
| Repeated recover | A completed second click returns `NotFound`, never an internal server error | `n1_repeated_recover_returns_not_found_instead_of_internal_error` | PASS |
| Recovery races TTL GC | Filesystem operations select a complete winner without a namespace mutation lock; no reachable object is reclaimed | `n1_recover_and_ttl_sweep_race_preserves_data_without_a_namespace_lock` runs 32 adversarial races | PASS |
| TTL boundary | The physical entry remains before TTL and namespace plus unreferenced immutable object data are reclaimed at TTL | `n1_soft_delete_ttl_reclaims_only_after_namespace_expiry` | PASS |
| A candidate object remains live elsewhere | Reference verification prevents immutable object reclamation | `n1_ttl_reclaim_preserves_an_object_still_referenced_live` | PASS |
| Fragment creation crosses a wall-clock second while replicating metadata | One timestamp is captured before the replica loop, so every `.fragment.ndx` publishes identical authority bytes | Isolated three-HDD proof first failed on the old per-root timestamp and passed after the shared-timestamp patch | PASS |
| Finalize encounters a later-root destination conflict | Every root is preflighted before publishing the first new namespace link | `n1_finalize_preflights_replica_path_conflicts_before_publication` | PASS |
| Recursive pagination token sorts after a parent directory but before one of its descendants | Continuation filtering never prunes directory traversal | `n1_recursive_reference_pagination_never_skips_cursor_directory_descendants` | PASS |
| Parent traversal in a logical path | Normalization rejects it before any filesystem mutation | `n1_namespace_mutations_reject_parent_traversal` | PASS |
| Two replicas have equal link counts but different paths, kinds, targets, or control bytes | Replica inspection hashes deterministic namespace/control content; destructive repair refuses an equally complete disagreement | `replica_inspection_detects_equal_count_different_namespace_content` | PASS |
| Identical replicas expose directory entries in different filesystem iteration order | Every directory level is sorted before content hashing, so the digest depends only on namespace/control content | `replica_content_digest_is_independent_of_directory_iteration_order` | PASS |
| Existing lineage, O(1) folder move, POSIX pagination, EC, healing, repair, redistribution, and NOA behavior regress | The complete N1 library suite remains green beside the 3.2.2 lifecycle cases | `cargo test -p n1 --lib -- --test-threads=1` | PASS: 131/131 local |

Focused verification commands:

```bash
cargo check -p n1 --tests
cargo test -p n1 namespace::runtime::tests -- --test-threads=1
cargo test -p n1 equal_count_different_namespace_content -- --nocapture
cargo test -p n1 replica_content_digest -- --nocapture
cargo test -p n1 --lib -- --test-threads=1
```

### N1 3.2.2 Three-HDD Proof And Production Deployment

On July 16, 2026, the exact release binary was tested against three physical
XFS HDDs at `/mnt/NDISK001`, `/mnt/NDISK002`, and `/mnt/NDISK003` with all
three roots colocated as index and blob roots and a static `2 + 1` object EC
layout. The test used isolated fragment/root names and did not modify existing
production objects.

The first broad proof correctly failed before deployment: fragment metadata
replicas could receive different second-resolution timestamps when slow HDD
initialization crossed a wall-clock second. Fragment creation now captures one
timestamp before the replica loop. The failed roots were removed and the fresh
proof was rerun from the beginning.

The corrected broad matrix passed all `20` gates: native protocol, `64 MiB`
multipart restart/resume, restart persistence, authorization boundaries,
fragment usage, namespace lineage, WebSocket deltas, concurrent and contended
uploads, multi-fragment isolation, NOA/POSIX CRUD, direct trash/recover and TTL
GC, archive, recover-on-read, object repair, disk-format repair, and
`3 -> 2 -> 3` redistribution. It finished with `216` unique live objects,
`216` matching live links per root, and `bad_lines: []`. A second focused NOA
run against the exact final binary also passed.

Direct loopback latency was then measured over one persistent TLS connection,
with no Nginx or other proxy. Each operation mutated all three XFS namespace
replicas:

| Operation | Samples | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Empty-folder rename | 250 | 0.846 ms | 0.907 ms | 0.924 ms | 0.928 ms |
| 128-descendant folder rename | 250 | 0.848 ms | 0.892 ms | 0.908 ms | 0.918 ms |
| Folder trash | 100 | 0.757 ms | 0.817 ms | 0.837 ms | 0.838 ms |
| Folder recover | 100 | 0.711 ms | 0.763 ms | 0.774 ms | 0.791 ms |

The empty and populated folder results are equivalent: folder rename does not
walk descendants. Hardware counters attached only to the N1 process during
`710` executed operations recorded `830,104,706` cycles, `591,240,349`
instructions, `3,557` context switches, `3` CPU migrations, and `5` page
faults over `0.718 s`: approximately `1,169,162` cycles and `832,733`
instructions per executed operation. These process counters include N1 runtime
work during the same bounded interval, so they are a reproducible server
baseline rather than a claim about every machine.

A separate tracing run was intentionally excluded from latency numbers because
tracing perturbs execution. Across `60` operations it observed exactly `180`
`renameat` calls (one per index root per operation), `15` `utimensat` calls
(one per root for five trash operations), no `fsync`/`fdatasync`, no subtree
directory scan, and zero blob bytes written. Physical inspection found all
`128` descendants on every replica, an empty trash directory, and zero
`.entries`, `.records`, or namespace-generation files.

After warning-denied workspace tests passed, the same binary was deployed to
the production service. Pre- and post-deployment deep health both reported
healthy: three roots online, `2 + 1` write safety, zero missing/divergent/
corrupt index replicas, and autonomous healing idle and write-safe. A rollback
copy of the previous binary and the removed empty legacy control artifacts was
retained outside active storage. No soak claim is made by this bounded proof.

Evidence is retained under
`/home/wlzambo/DEV-TEST/n1-3.2.2-three-hdd-proof-20260716`.

Historical evidence below describes earlier development candidates and is not
evidence that their retired generation/record mechanisms remain in 3.2.2.

On July 13, 2026, the warning-denied N1 gate passed all `116` N1 tests, and
the warning-denied workspace library/binary/integration gate also passed (`7`
N1 Shell integration tests, `107` SIMD unit tests, and `5` SIMD integration
tests; long SIMD stress cases remained explicitly ignored as configured).

The first real HDD+SSD preflight at
`/home/wlzambo/DEV-TEST/n1-3.0.1-namespace-extreme/extreme-20260712-200250`
correctly failed on an order-dependent replica digest. The digest was fixed to
sort every directory level, and the failure became the dedicated
`replica_content_digest_is_independent_of_directory_iteration_order`
regression test. The next detached proof was launched as
`n1-301-namespace-extreme-201043.service`, with evidence under
`/home/wlzambo/DEV-TEST/n1-3.0.1-namespace-extreme/extreme-20260712-201043`.
That run correctly failed during `2 -> 1 -> 2` redistribution because equal
record counts no longer concealed different namespace contents, while no
durable authority existed to identify the continuously active replica. The
resulting patch adds the replicated namespace generation and the three
redistribution/write-gate regressions above. This failed run remains evidence;
it is not counted as release PASS.

The generation-authority patch was frozen into the unreleased N1 3.0.1 candidate binary and
a fresh detached HDD+SSD `1 + 1` proof ran on July 13, 2026 as
`n1-301-generation-proof-112852.service`, with evidence under
`/home/wlzambo/DEV-TEST/n1-3.0.1-namespace-extreme/extreme-20260713-112852`.
The run reached the chaos cycle after the earlier release gates succeeded,
including `2 -> 1 -> 2` redistribution and a repair scan of `15,095` objects
with zero corrupt objects, missing metadata, or divergent metadata. The run
then failed because the chaos harness removed an unused parity shard and
incorrectly expected an otherwise healthy read to recreate it. N1 correctly
read the intact data shard. This is retained as failed harness evidence, not a
release PASS. The harness now uses the same data-shard-only fault selector as
the dedicated recover-on-read proof, with a regression that rejects parity as
a recover-on-read target.

The corrected harness then passed a fresh compact full release matrix on the
real HDD `/mnt/DISK001` and SSD
`/home/wlzambo/SERVER-TEST/disk-00`, with evidence under
`/home/wlzambo/DEV-TEST/n1-3.0.1-harness-fix/focused-20260713-184522`.
All `21` release gates passed with `1 + 1` EC, a real `64 MiB` multipart
object, restart persistence, native protocol, concurrent workloads, POSIX
CRUD/trash/recover/GC, corrected recover-on-read, explicit parity repair,
disk-format repair, `2 -> 1 -> 2` redistribution, and a completed chaos cycle.
The corrected chaos path physically restored the deleted data shard before
continuing through trash/recover, disk-wipe repair, GC, and final health. The
final inspection found `103` live namespace links on each root and `103`
unique objects; the log scan passed with zero bad lines. This fresh complete
matrix closes the harness defect, while the preceding run remains the
independent `15,095`-object scale evidence rather than being relabeled PASS.

That historical candidate gate also required the warning-denied workspace suite and an
unattended real HDD+SSD `1 + 1` EC proof. Its command, PID, logs, and final
summary belong under `/home/wlzambo/DEV-TEST`; a running background proof is
not recorded as PASS until its summary and log scan complete successfully.

## Extended LTS Proof Matrix

The full LTS proof should include:

- 24-hour soak on `/mnt/disk-00` and `/mnt/disk-01`
- multiple 20 GB+ multipart uploads
- restart during active multipart upload, then resume/finalize
- download hash match for every large object
- range/view streaming checks
- disk wipe, disk removal/re-add, repair, and health refresh
- redistribution after disk topology changes
- targeted TTL reclamation plus explicit orphan GC after upload cancellation
- direct-path trash/recover with filesystem-mtime TTL and one trash slot per logical path
- 10k+ object namespace storm
- four concurrent users on one fragment doing upload, mkdir, rename, move, trash, recover, and listing
- four concurrent users across four fragments
- UI-backed smoke for mount, usage, list, upload, download, trash, recover, repair/status, and health display

## Known Tradeoffs

N1 is optimized for durable large-object storage and repairability. Tiny-object storms are expected to be dominated by namespace commits, receipt writes, and shard/parity flush behavior. Batching or archiving high-volume tiny-file datasets before upload is more efficient.
