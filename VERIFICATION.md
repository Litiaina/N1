# N1 Verification Report

This document tracks release proof and hardening evidence. It is intentionally separate from the user guide.

The current package line is N1 `3.1.0` on the development channel, using
engine format `3`. Stable product status has not been declared for this engine
line. Dated `3.0.0` and `3.0.1` evidence names below remain unchanged because
they identify the exact candidate binaries and runs that produced them.

## Required Release Gates

Before an LTS release, N1 should pass:

- `RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets`
- `RUSTFLAGS='-D warnings' cargo check --workspace --all-targets`
- large multipart upload with restart/resume
- download hash match
- range/view streaming
- CRUD and POSIX namespace operations
- unique-id trash, pre-TTL recover, TTL expiry, targeted reclamation, and removed purge surfaces
- recover-on-read after one disk is wiped
- repair after simulated disk loss or reformat
- autonomous repair after recoverable data- and parity-shard loss
- automatic read-only entry/recovery for missing and identity-mismatched roots
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

## Rust Release Proof Runner

The release proof runner lives under `tools/n1-release-proof`.

Shell may prepare the environment and start the Rust binary, but Python is not part of the N1 release verification path.

Example:

```bash
SOURCE_FILE="/home/administrator/Downloads/INCDT 122225.mp4" \
DISK_ROOTS="/mnt/disk-00,/mnt/disk-01" \
scripts/bench/run_n1_release_proof.sh
```

For a large multipart proof, use a source file that exceeds the configured one-shot size. Large files must exercise multipart, persisted receipts, restart/resume, finalize, and download verification.

## Focused Proof Evidence

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

## N1 3.1.0 Development-Engine Autonomous-Maintenance Confidence Matrix

N1 3.1.0 intentionally has no metadata or upload-manifest compatibility
with an earlier engine. The package, native client, and shell versions are
aligned at `3.1.0`. This development line incorporates the earlier 3.0.x
namespace/lifecycle hardening and adds object-owned EC topology plus autonomous
repair. Historical proof names retain the version used when each run completed.

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

Fast, non-soak verification commands for this implementation:

```bash
cargo fmt --all -- --check
cargo check -p n1 --all-targets
cargo test -p n1 --lib -- --test-threads=1
cargo test -p n1 --bin litiaina-n1 -- --test-threads=1
cargo check --workspace --all-targets
cargo test --workspace
```

The fast N1 library gate passed all `129` tests on July 14, 2026. No soak or
long-running proof was started, per the implementation-stage test boundary.
The ordinary workspace matrix also passed: proof runner `3/3`, N1 Shell
integration `7/7`, SIMD unit `107` passed with `6` intentional ignores, SIMD
integration `5` passed with `2` intentional ignores, and documentation tests
`4/4`. All workspace targets, including benchmark targets, compiled.
The earlier real HDD+SSD evidence below remains valid baseline evidence for
the unchanged immutable-object, EC, namespace, CRUD, POSIX, recovery, repair,
and redistribution primitives; it is not relabeled as a dedicated autonomous-
healing topology proof.

### Reset 3.0.0 Fast Real-Disk Smoke

On July 14, 2026, a fresh debug binary ran a bounded native/CRUD smoke on the
real XFS HDD `/mnt/DISK001` and ext4 NVMe root containing
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

### Reset 3.0.0 Autonomous-Healing Real-Disk Proof

On July 14, 2026, the bounded `--auto-healing-proof` release gate passed on
the real XFS HDD `/mnt/DISK001` and the ext4 NVMe device containing
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

## Pre-Reset 3.0.1 Namespace Edge-Case Evidence

The unreleased N1 3.0.1 work was hardening on engine format 3. It did not redesign the
immutable object plane, stable object identities, EC placement, or O(1)
namespace rename. The patch closes mutation-boundary edge cases identified
after the initial N1 3.0 lifecycle implementation. All of these patches are
incorporated into the current 3.1.0 development engine; historical evidence
paths retain their original names.

| Edge case | Locked invariant | Regression proof | Status |
| --- | --- | --- | --- |
| Recovery record exists but every trash payload placement is missing | Recovery fails closed and retains lifecycle metadata; it never reports a successful headless recovery | `n1_recover_fails_closed_when_record_has_no_payload_placement` | PASS |
| Recovery races TTL ownership and physical reclamation | All namespace/lifecycle mutations for one fragment use the same bounded per-fragment lock stripe | `n1_recover_and_ttl_sweep_are_serialized_per_fragment` runs 32 adversarial races and accepts only the two complete serial outcomes | PASS |
| A later index root fails during live rename | Earlier root renames are rolled back and both parent directories are synced | `n1_live_rename_rolls_back_every_replica_after_later_root_failure` | PASS |
| Finalize encounters an invalid later-root namespace parent | Every root is preflighted before publishing the first new namespace link; newly published links are rolled back on runtime failure | `n1_finalize_preflights_every_replica_before_namespace_publication` | PASS |
| Recursive pagination token sorts after a parent directory but before one of its descendants | Continuation filtering never prunes directory traversal | `n1_recursive_reference_pagination_never_skips_cursor_directory_descendants` | PASS |
| Repeated deletes create multiple retained generations at one path | Any-object resolution and recovery both select max `(deleted_at_unix, trash_entry_id)` | `n1_resolve_any_selects_the_same_latest_trash_generation_as_recovery` | PASS |
| Corrupt namespace parent is a symlink outside the fragment root | Mutation parent chains accept directories only and reject symlinks/non-directories before creation or rename | `n1_namespace_mutations_reject_parent_symlink_escape` | PASS |
| Two replicas have equal link counts but different paths, kinds, targets, or control bytes | Replica inspection hashes deterministic namespace/control content; destructive repair refuses an equally complete disagreement | `replica_inspection_detects_equal_count_different_namespace_content` | PASS |
| Identical replicas expose directory entries in different filesystem iteration order | Every directory level is sorted before content hashing, so the digest depends only on namespace/control content | `replica_content_digest_is_independent_of_directory_iteration_order` | PASS |
| A `2 -> 1 -> 2` cycle returns a stale replica with the same link count but different targets | The continuously active replica has a higher durable namespace generation and is copied to the returning replica | `redistribution_reconciles_equal_count_stale_replica_from_higher_generation` | PASS |
| A returning stale replica has more links because the active replica deleted or expired an entry | Higher generation outranks record count, preventing stale-link resurrection | `redistribution_generation_outranks_stale_replica_with_more_links` | PASS |
| A normal mutation arrives before stale replicas are reconciled | Different generations reject the mutation before any namespace path changes | `n1_namespace_mutation_is_blocked_until_stale_generation_is_reconciled` | PASS |
| Namespace authority tracking accidentally scans the namespace or mutates object state | Mutations update one constant-size internal generation record per participating index root; the record is excluded from the content digest and `.objects` remains untouched | `n1_namespace_mutations_advance_the_replicated_authority_generation` | PASS |
| Existing 3.0 lifecycle, lineage, folder move, trash generation, POSIX pagination, and shared-reference behavior regress | All pre-existing namespace runtime tests remain green beside the new adversarial cases | `cargo test -p n1 namespace::runtime::tests -- --test-threads=1` | PASS: 19/19 |

Focused verification commands:

```bash
cargo check -p n1 --tests
cargo test -p n1 namespace::runtime::tests -- --test-threads=1
cargo test -p n1 equal_count_different_namespace_content -- --nocapture
cargo test -p n1 replica_content_digest -- --nocapture
cargo test -p n1 redistribution_reconciles_equal_count_stale_replica -- --nocapture
cargo test -p n1 redistribution_generation_outranks_stale_replica -- --nocapture
cargo test -p n1 n1_namespace_mutation_is_blocked_until_stale_generation_is_reconciled -- --nocapture
```

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

The generation-authority patch was frozen into the pre-reset N1 3.0.1 candidate binary and
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
- trash/recover by unique trash entry id with lifecycle context stored inside `.trash`
- 10k+ object namespace storm
- four concurrent users on one fragment doing upload, mkdir, rename, move, trash, recover, and listing
- four concurrent users across four fragments
- UI-backed smoke for mount, usage, list, upload, download, trash, recover, repair/status, and health display

## Known Tradeoffs

N1 is optimized for durable large-object storage and repairability. Tiny-object storms are expected to be dominated by namespace commits, receipt writes, and shard/parity flush behavior. Batching or archiving high-volume tiny-file datasets before upload is more efficient.
