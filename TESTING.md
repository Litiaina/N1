# Named service and original console qualification — November 3, 2026

The latest complete workspace regression passes **414 tests, zero failures**,
with nine explicit helper/physical opt-in exclusions. Public metadata tests run
through a real native LINK process on both HDDs. All three original console live
tests pass using its existing runtime and proxy: flat-key prefix rename and
browser TAR download, interrupted multipart resume, and standard batch upload.
The existing Node suite passes 38 tests and static console validation reports
zero errors/warnings. These checks do not replace population qualification.

The first full-stack population attempt returned a mixed batch response and
failed. The metadata native-client pool retained sockets past the object
server's idle deadline; the pool now discards old idle transports before reuse.
The added deterministic transport regression and metadata/native suite pass
17 cases, and the full two-HDD public API witness passes again. No shard
corruption has been observed; neither new 500,000-object campaign has passed yet.

The Rust `metadata_engine/examples/noa_qualification` harness drives the actual
API or original console in Chromium, durably journals each acknowledged version,
kills/reopens both engines, performs an offline metadata/shard census, then
checks every object's bytes and version via metadata. The browser driver uses
real production XHR/FormData; it does not mock routes or responses. Bulk data,
binaries and reports stay on HDDs. No Python is used in these campaigns.

Current results and exact source/binary provenance:
[FULL-STACK-QUALIFICATION-20261103.md](/mnt/PROJECT-DISK-1TB/n1-ini-restoration-20261103-01/reports/FULL-STACK-QUALIFICATION-20261103.md).
The following sections preserve earlier, separately scoped qualification.

# MCH physical metadata qualification — November 3, 2026

The metadata storage foundation now passes on MCH's four rotational SATA HDDs
(XFS). The complete metadata/Procyon regression suite passes **143 tests** on
DISK0; the **15 metadata integration cases** also pass independently on DISK1,
DISK2 and DISK3. **81 storage documentation examples** pass on MCH with Rust
1.92.0, locked/offline vendored dependencies and warnings denied.

The new Rust `hdd_qualification` supervisor completes **1,000,000 metadata entries**
on DISK0 with eight writers and four snapshot readers, a mid-population SIGKILL,
resume without rewriting prior acknowledgements, full verification, and then
**100,000 mutations** (80,000 updates / 20,000 deletions) with another SIGKILL,
resume and exact final verification. The final table census is 980,000 entries,
1,080,000 history fixtures and 20,000 reclamation fixtures. All one million
identifiers and every expected value/history/absence pass; completed resume
workers record 356,692 concurrent snapshot checks without mismatches.

Each of the other HDDs separately passes an 8,192-entry / 2,048-mutation run with
two kill/resume cycles. The full million-entry campaign takes **438.99 seconds**
including faults, churn and both full verification passes, with **106.66 MiB**
peak RSS. Transactions contain up to 256 entries; phase timings and transaction
latencies are detailed in the report and are not single-object/API benchmarks.

No engine implementation changes accompany these tests. A fresh local full
workspace run, including the new harness regression, passes **382 tests**, zero
failures, six explicit opt-in/helper exclusions. New harness strict metadata
Clippy and formatting pass. All **262 shipped Rust files** match the workstation
SHA-256 manifest. Existing MCH N1 processes remain running, and the headless test
drivers exit zero with no qualification processes left behind.

This qualifies metadata KV storage, not object payload uploads, a public
namespace service, metadata NOA conformance, power loss or metadata replication.
The imported-core lint findings in the earlier report remain separate.
Raw logs, source/binary hashes, exact commands and all scope details:
[MCH-METADATA-QUALIFICATION-20261103.md](/mnt/PROJECT-DISK-1TB/n1-mch-metadata-20261103-01/reports/MCH-METADATA-QUALIFICATION-20261103.md).

# Object/metadata workspace integration — November 3, 2026

The `engine/` directory is now `object_engine/`. All **99 tracked component
files are byte-identical** to checkpoint `19462b4`, including Cargo metadata,
the banner, infrastructure and tests. The package remains `n1`; direct object
operations have no dependency on `metadata_engine`, Procyon or `lsm-tree`.

The optional `n1_metadata_engine` component imports Procyon's storage core and
regression fixtures, applies the specific fixes recorded in
[`metadata_engine/storage/UPSTREAM.md`](metadata_engine/storage/UPSTREAM.md), and
adds durable optimistic transactions, cross-table snapshots, bounded scans,
cancellation-safe blocking ownership and offline init/check commands.
This is a storage foundation, not an implemented named-object NOA service.

Validation of the final source:

| Check | Result |
| --- | --- |
| Full workspace/all targets, offline/locked, warnings denied | **381 passed, 0 failed**, 6 opt-in/helper cases ignored by the default command |
| Included original N1 checks | All previous **238 passed**, including 185 object-engine library tests |
| Included Procyon core checks | **128 passed**, including recovery, transactions, partial flush replay and write/sync failure regressions |
| Included metadata integration checks | **15 passed**, including actual concurrent writers/readers, CAS conflicts, range predicates, resource limits, schema refusal, cancellation ownership, CLI and subprocess crash recovery |
| Procyon storage documentation examples | **81 passed, 0 failed**, warnings denied |
| Separate PROJECT XFS HDD process-kill witness | All 32 acknowledged cross-table transactions recovered exactly; uncommitted transaction absent |
| Separate TEST ext4 HDD process-kill witness | All 32 acknowledged cross-table transactions recovered exactly; uncommitted transaction absent |
| N1 production Clippy, all targets, warnings denied | Passed; excludes the historical release-proof tool and imported Procyon core from linting, not from tests |
| Formatting and whitespace | Passed |

The six default exclusions comprise the four existing N1 physical/helper cases,
one retained Procyon ingestion/compaction case, and the metadata subprocess
fixture. The metadata crash test explicitly invokes that fixture; it also ran
separately on both HDDs. No ignored object-engine physical case was rerun as
part of this directory-only change, and no million-object metadata or physical
power-cut claim follows from these checks.

An inclusive Clippy attempt reports **89 imported/core/workspace diagnostics**,
including package-metadata lints, duplicate dependency versions, `expect` usage,
documentation and style. This is retained as a failed check, not reported as a
whole-workspace lint pass. The successful N1 lint command does not add lint
suppression attributes to the imported core. Earlier dependency-resolution,
fault-fixture and old-format assertion attempts are retained separately.

All logs, source archives, exact commands and scope are in
[METADATA-FOUNDATION-20261103.md](/mnt/PROJECT-DISK-1TB/n1-metadata-foundation-20261103-01/reports/METADATA-FOUNDATION-20261103.md).
Builds and ordinary fixtures used `/tmp` tmpfs; physical test data and reports
used the HDDs. No Python tests, persistent services, commit, push or release.
The original Procyon repository remains clean at `7ee9a06`.

# Previous LINK RC.2 qualification campaign — November 2–3, 2026

The November 3 `link` branch checkpoint passes a fresh warnings-denied
`cargo test --workspace --all-targets --offline --locked`: **238 passed,
0 failed, 4 opt-in/helper cases ignored**, including 185 engine-library tests.
The total includes the resumable client harness tests added after the earlier
235-test hybrid run. Log: `reports/link-precommit-workspace213.log` under the
HDD evidence root. Formatting and staged-diff checks also pass; no engine or
harness code changed while preparing this checkpoint.

The November 3 **original NCX/NDX versus current CRC32C LINK** comparison passes
two fresh alternating four-trial native campaigns: 844 verified GETs and
408 successful removal/absence checks, with clean shutdown throughout.
No engine or client code changed for these measurements. Small-object timings
and RSS improve; full-workload large PUT is near parity. Repeated large reads
remain variable and slower in aggregate with substantial unequal storage reads.
All trials and limitations are retained in
[NCX-VS-CRC32C-LINK-20261103.md](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/NCX-VS-CRC32C-LINK-20261103.md).
This comparison does not turn the separate new-only 959 MiB/s sample into an
old/new result, or establish matched population-scale performance.

The hybrid-integrity change passes **235 Rust workspace tests** (185 engine
library tests), with zero failures in `hybrid-workspace-tests179.log` and
`hybrid-workspace-tests180.log` under the HDD report root. Strict Clippy passes
in `hybrid-clippy182.log`. Four physical/helper cases require explicit opt-in;
these default test counts do not include physical qualification.
New coverage includes independent CRC32C known answers, segmented/unaligned
input, canonical reserved bytes, source identity/position/length binding,
CRC-valid wrong-source rejection before GET/repair, unchanged captured v1 bytes,
immutable pool-profile enforcement and restart/resume for both profiles.

The Rust headless scale witness is reproducible with a release client:

```sh
rc2_bench --scale-smoke /absolute/release/litiaina-n1 \
  /mnt/PROJECT-DISK-1TB /media/altear/TEST-1TB/mx-tests
rc2_bench --scale /absolute/release/litiaina-n1 \
  /mnt/PROJECT-DISK-1TB /media/altear/TEST-1TB/mx-tests
rc2_bench --scale-test-resume /absolute/release/litiaina-n1 \
  /mnt/PROJECT-DISK-1TB /media/altear/TEST-1TB/mx-tests
rc2_bench --scale-resume /absolute/release/litiaina-n1 \
  /mnt/PROJECT-DISK-1TB/n1-scale-<existing-run>
```

The current smoke case has 4,099 objects to exercise partial batches and uneven
worker tails; the earlier nonresumable client passed a 1,027-object smoke case.
The full case has 500,000 mixed 0–64 KiB objects, four native clients,
durable batches of at most 256, required io_uring and a fresh fixed 64 KiB 1+1
pool on the two HDDs. Quotas are disabled; adaptive concurrency and admission
remain enabled. Every intent and acknowledgement is saved outside member roots.
After SIGKILL/restart every acknowledged object is verified by length/SHA-256;
the harness also verifies manifest index/order/count and exact batch identities.
Upload wall time includes init, source generation and client journal barriers.
Batch PUT latency is reported as batch latency, not individual-object latency.
`PASSED.json` is written only after all GET checks and clean final shutdown.
Data and evidence remain in new private HDD directories. Starting the command
alone does not establish a passing population result.

The resume client copies and validates journal prefixes before changing any
object. Previously acknowledged keys are never PUT/finalized again; uncertain
intents retry the same identity. A fingerprint census proves the complete
population has distinct keys. Journal persistence runs on bounded blocking
workers rather than the Tokio runtime threads. A physical 4,099-object witness
passes client/server SIGKILL, intentional acknowledged-header corruption
rejection without overwriting it, exact injected-byte restoration, repeat resume
and full verified GETs after another SIGKILL/restart. Original journals stay
unchanged (`hybrid-resume-physical197.log`); the client also passes five Rust
harness tests and strict Clippy (`hybrid-resume-tests195.log`,
`hybrid-resume-clippy198.log`).

The full two-HDD case now **passes: 500,000 distinct durable acknowledgements,
all 500,000 length/SHA-256 GETs after SIGKILL/restart, and 1,000,000 correctly
placed shard files with no unreferenced shards or leftover staging**. Final
shutdown was clean. The initial process stopped before completion; resume
validated and preserved its 397,568 acknowledgements without PUT/finalize and
completed the remaining 102,432. All eight original journal hashes still match.
The temporary user job exited successfully; no service was installed/enabled.
Initial sampled upload timings and exact resume timings remain separate;
there is no uninterrupted full-population upload latency result.

Detailed workload, phase timings, RSS/CPU/I/O, allocation and evidence:
[HEADLESS-500K-20261102-20261103.md](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/HEADLESS-500K-20261102-20261103.md).
The hybrid engine's frozen hashes and prerequisite tests remain recorded in
`reports/HYBRID-INTEGRITY-20261102.md` under the same HDD evidence root.

The standalone Rust `scale_inventory` example performs the post-shutdown
read-only census. It requires a completed 500,000-object resume checkpoint and
a new output path on the PROJECT HDD:

```sh
scale_inventory /mnt/PROJECT-DISK-1TB/n1-scale-<existing-run> \
  /mnt/PROJECT-DISK-1TB/n1-scale-<existing-run>/resume-<completed-checkpoint> \
  /mnt/PROJECT-DISK-1TB/scale-inventory-<new-report>.json
```

It checks distinct client identities, exact member/fanout placement, file
lengths, no extra/hard-linked shards and empty staging. It reports logical and
allocated data-file bytes; the latter excludes filesystem metadata. Payload
verification is established by the preceding full GET pass, not by file counts.

The engine is undergoing a complete LINK redesign. Historical RC.1 results below
belong to the former engine; they do **not** qualify LINK. The current release
candidate remains unreleased. See [the implementation ledger](docs/RC2-IMPLEMENTATION.md)
for current test logs, failures, physical witnesses and outstanding gates.

Current builds and functional fixtures use RAM; durable reports and physical
fixtures are under `/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01` and the isolated
TEST HDD run directory. Nothing in a debug or RAM-only test establishes matched
performance, million-object scale or physical power-loss durability. MCH is
unavailable. The historical release-proof executable still targets the previous
API and must be ported before running its acceptance scenarios against LINK.

The November 2 infrastructure-restoration run passes **220 Rust workspace tests**
(176 in the engine library), with zero failures. Three opt-in/helper cases are
ignored in that default command; the physical process witness was run explicitly.
The current engine/LINK/client/CLI also pass strict Clippy on all their targets.
See `restoration-replacement-tests78.log` and `restoration-final-clippy80.log`
in the HDD reports. A second full-suite run after the explicit member-lease
release also passed (`restoration-lease-repeat77.log`).

Coverage includes the original four-slot recovery runtime/admission, coalesced
read repair under a full budget, cancellation-safe operation/session writes and
lease retention, real HTTP range/conditional/HEAD downloads, and explicit offline
single-member reconstruction. Reconstruction tests exercise all logical slots,
healthy inode preservation, permanent/incomplete retirement, durable multipart
holes, partial finalization, corrupt peers, unexpected inventory and resume.
A sparse 1 TiB logical upload with a verified tail frame exercises bounded
extent traversal; it does not upload or allocate a TiB. The earlier intermittent
restart WouldBlock is retained in `restoration-replacement-tests72.log`; a
deterministic duplicate-descriptor test covers the corrected lifetime mechanism.

The Rust-only `engine/tests/headless_recovery.rs` witness runs the actual engine
with io_uring required on independent XFS/ext4 HDDs. Its final optimized-binary
run passes SIGKILL during upload, SIGTERM while an admitted upload is paused,
durable multipart resume, exact native downloads, read repair, permanent
retirement, byte-exact member cutover, authorized failed-member reconstruction
and encrypted service-identity recovery. The final report is
`/mnt/PROJECT-DISK-1TB/n1-restoration-witness-01M6CN0HSMKWTF2N6TA9HMJJBC/result.json`.
Engine SHA-256:
`6114956511eccdab5851e84e33eb92f3f900df0d218e3ff6b563ddc965f039f6`.
The report records device identities; all owned engines are stopped.
This witness is not a benchmark or physical power-loss qualification.

Reproduce with a RAM `CARGO_TARGET_DIR`/`TMPDIR` and explicitly selected,
owner-writable directories on two different test filesystems:

```sh
N1_WITNESS_PROJECT_ROOT=/mnt/PROJECT-DISK-1TB \
N1_WITNESS_TEST_ROOT=/media/altear/TEST-1TB/mx-tests \
N1_WITNESS_ENGINE_BINARY=/absolute/path/to/release/litiaina-n1 \
cargo test -p n1 --test headless_recovery --offline --locked -- \
  --ignored --exact physical_restart_clone_repair_and_service_identity_recovery --nocapture
```

The harness creates private, uniquely named directories and retains its data and
reports for inspection. It signals only child processes it owns. No Python is
used for current tests, benchmarks or fault orchestration. Historical Python
results below remain dated evidence of the former engine.

The current Rust release benchmark and full methodology are on the HDD in
`reports/RESTORATION-PERFORMANCE-20261102.md`. It measures 1 GiB, 100 individual
4 KiB objects and 100 different batched objects, verified requested-cold/warm
GETs, stat, deep health and permanent deletion, with CPU/I/O/RSS/VmHWM samples
and binary hashes. An additional 4 KiB-block pool quantifies the default
256 KiB pool's excessive tiny-object padding. These measurements do not establish
a matched NCX/LINK win or million-object qualification; serial small-object
durability tails remain material. Benchmark and physical-harness clients are
Rust, and their persistent data/reports are on HDDs.

A four-trial native NCX → LINK → LINK → NCX comparison is now retained in
`reports/NCX-VS-LINK-BENCHMARK-20261102.md` under the same HDD report root.
It uses frozen release binaries, the same two HDDs, 1+1 EC, a 1 GiB object
and 100 individual 4 KiB objects per trial, including LINK initialization in
upload time. All 808 GETs pass length/SHA-256 checks; all owned servers stop.
LINK improves small-upload time and process RSS, while warm large reads regress.
The report records original adaptive tiny-object blocks versus LINK's fixed
pool blocks, O_DIRECT versus buffered cache residency, different deletion
guarantees, full latency samples, CPU/process I/O and the two interrupted setup
attempts. It does not establish total RAM use, batch parity or scale qualification.
The Rust benchmark example's `--compare` mode reproduces that campaign; its
legacy wire adapter remains confined to benchmark tooling.

The subsequent configuration cleanup uses stable `N1-LINK` identity instead
of a release-tagged discriminator. CLI/ping report the workspace Cargo version;
LINK also inherits that workspace package version. Native/HTTP ping and config
validation regressions pass in the 221-test workspace run
(`config-version-tests96.log`); current production crates and all their targets
pass strict Clippy (`config-version-clippy97.log`).
The updated release engine also passes the physical recovery witness
(`config-version-witness99.log`, 47.92 seconds); its SHA-256 is
`f7c8283633f31175da2968c14364e665f666ea288ee2b0409b8d2713b2370701`.

## Pipeline optimization — November 2

The subsequent optimization passes **223 workspace tests, zero failures**
(179 engine library tests) and strict Clippy for the production crates and all
their targets: `optimization-tests116.log`, `optimization-clippy117.log`.
The three default ignored cases retain their explicit opt-in/helper roles.
The physical Rust witness also passes on the optimized release binary in
`optimization-witness119.log` (41.03 seconds), including crash/resume, shutdown
drain, read repair, permanent retirement, member reconstruction and encrypted
service recovery. Engine SHA-256:
`f017966129b74894ea80d5c27bde0b446e5e77533ebb9f4fdfd294768e6000ed`.

Uploads reuse bounded buffers and coalesce up to four adjacent framed stripes
per member while preparing the next group. Reads prefetch bounded windows,
verify each frame once and retain independent source SHA-256 checks. Health and
repair reuse already verified source hashes. Canonical zero padding of newly
written partial tails can be physically sparse; old retry regions are rewritten
in full. Neither the immutable geometry nor the inline encoding changes.
Regressions cover conflicts beyond two upload groups, exact final bytes,
independent corrupt frames in prefetched windows, ranges, durable sparse-tail
restart, conflicting retry, padding corruption, reconstruction and retirement.

The strict before/after LINK campaign uses the same geometry, durability and
permanent-delete guarantees on both binaries. `optimization-link-abba120.log`
records four alternating trials and **808 SHA-256/length-verified downloads**.
Warm large-read aggregate throughput improves from **185.93 to 588.78 MiB/s**;
1 GiB durable upload throughput moves from **82.03 to 85.85 MiB/s**. The mean
100-object upload phase drops from **52.27 to 40.14 seconds**. Per-process
small-upload write bytes drop from **53,248,000 to 2,457,600** per 100 objects.
Delete median increases while its aggregate wall time decreases; its protocol
is unchanged, so this does not establish a deletion optimization. Process RSS
sampling and kernel VmHWM are reported separately, including regressions.

Detailed trials, latency tails, memory, CPU/I/O counters, original-NCX comparison,
full batch/health workload, exact binary hashes and limitations are retained in
`reports/OPTIMIZATION-RESULTS-20261102.md` under the HDD report root.
Reproduce the strict LINK comparison with the Rust example:

```sh
rc2_bench --compare-link /absolute/before/litiaina-n1 /absolute/after/litiaina-n1 \
  /mnt/PROJECT-DISK-1TB /media/altear/TEST-1TB/mx-tests
```

The before binary is hash-pinned to the configuration-cleanup checkpoint above;
the harness refuses a substituted baseline. `--compare` retains the separately
pinned original-NCX comparison. Both modes create isolated directories and
terminate only owned engines. Current benchmark source is
`native_client/examples/rc2_bench.rs` and `rc2_bench/{comparison,supervisor}.rs`.
These bounded serial results do not qualify million-object scale, full-machine
RAM usage, saturated concurrency or physical power cuts. RC.2 remains unreleased.

The selected native-delivery checkpoint passes **226 Rust workspace tests**
(182 engine library), zero failures, with the same three explicit opt-in/helper
cases ignored by default (`native-selected-tests170.log`). Current production crates
and all their targets pass strict Clippy (`native-selected-clippy171.log`); the
historical release-proof executable retains its separate recorded limitations.
Regressions exercise cancellation-safe socket/shard ownership, transport progress
deadlines and the retained large-block corruption/range recovery case. Original
startup banner and local timestamps are
restored and exercised by `banner-smoke127.log`.

Native GET now streams directly from the prepared verified range to the socket;
the original adaptive admission, io_uring and cancellation/drain protections
remain active. HTTP retains its bounded body adapter. Frame and source SHA-256
verification remain intact, as explicitly requested. Rust checksum profiling
separates that byte-processing cost from fixed-topology LINK placement.
The final selected release rebuild (`native-selected-release167.log`) reproduces engine SHA-256
`252b92024f59f2d339fa4e5af14412fef25272594404ab3ec1e39dc60e4ffff2`,
which passed the physical recovery witness in `native-direct-witness135.log`
(44.94 seconds). The result is
`/mnt/PROJECT-DISK-1TB/n1-restoration-witness-01M6DHHZZDBYH5GW02K2DDRXQT/result.json`.

Detailed CPU/throughput tradeoffs, original-NCX comparisons and unsuccessful
buffer-channel/hash-backend experiments are retained in the HDD report
`reports/NATIVE-READ-PATH-20261102.md`. The Rust benchmark adds
`--compare-reads-link`, `--compare-reads-direct`, `--compare-reads-ncx` and
`--profile-checksums`; existing
full-workload modes retain their workloads. Read comparisons use eight warm
verified GETs per trial; repeated reads of one object are correlated samples,
not independent object populations or scale qualification.

The attempted large-window worker-pool dispatch passes its own functional checks
but reduces warm throughput by 63.41% and raises CPU/GiB by 156.57% in the paired
comparison. That dispatch is removed from the selected implementation. Its
source/binary/results remain on the HDD; its broader corruption regression is
retained and passes in the selected 226-test suite. No checksum policy is weakened.

The final original-NCX read comparison (`native-selected-ncx-reads172.log`)
passes all 36 GETs with zero process storage-read bytes in both warm phases.
Selected LINK delivers 606.772 versus NCX's 679.871 MiB/s, uses 1.857 versus
1.443 CPU seconds/GiB and has 32.25% lower peak sampled process RSS. The full
selected workload (`native-final-full152.log`) passes 402 downloads and 201
permanent deletions with clean shutdown. The unchanged selected binary is
identified above; unfavorable results and checksum-policy differences are
explicit in the detailed report. No million-object or release claim follows.

## Historical qualification record

# N1 testing and results

N1 1.0.0-RC.1 is the first formal release candidate, not stable or LTS. The
storage-format specification is 1.0.0-RC.1 with unchanged encoding identifier 4.
Results below are dated evidence, not guarantees for every server. Older
versions' passes do not certify the current namespace/pool design. Personal test
paths and credentials are intentionally omitted. Historical 3.x/4.x labels and
binary checksums identify the private builds actually tested; they are not
retroactively renamed or presented as newly rebuilt RC binaries.

## Pending durable-upload retirement race — October 28, 2026

An MX integration run reported `read pending upload binding: ENOENT` during
concurrent durable initialization. A session discovered by the reuse scan could
be finalized/cancelled before its binding was read. This was an upload
availability failure; the report did not establish committed-data corruption.
The original incident evidence is preserved.

The original `16dd5c93` code, with test-only filesystem-boundary hooks, fails two
deterministic regressions with the reported error and a third by recreating a
retired receipt directory. The patched code passes all **15 session-lifetime
regressions**. Five additional tests pass, covering six forced part/finalizer
interleavings, committed/corrupt NDX protection, retired logical-name handling
and idempotent cancellation. They compare immutable NDX/shard bytes and perform
explicit-version downloads; absent session state never authorizes payload
deletion. Existing manifest/part surviving-replica policy remains unchanged.

An initial verification attempt failed because the new payload fixture had not
enrolled its roots. That failed attempt is retained. The fixture now performs
real enrollment; disk-identity checks are not bypassed. The initial RED driver
also incorrectly required the receipt-recreation failure to contain the binding
error text; its preserved failure is followed by correctly classified original
code reproductions. Neither is reported as an engine correctness pass.

The warning-denied optimized candidate (`c50ec218`) passed **471/471 library
tests** in 578.24 seconds on the local HDD fixture, **205/205 Python
regressions**, four backend-selection cases and the separately required native
io_uring case on both XFS and ext4. The 20 new library tests above also passed
as focused runs before the complete suite.

The two-client HTTP/restart smoke test passed. The 100-client run then **failed**
on a deliberate already-acknowledged part replay after 297 completed versions.
An independent restart/read-only audit verified all **297/297** acknowledged
historical payloads (38,928,384 bytes) by SHA-256; original failure evidence is
unchanged. A paced HTTP reproduction proved a separate pre-existing transport
bug: the duplicate-part path returned `200 OK` after receiving only 1 KiB of a
64 KiB body, closed the connection and caused the sending client to fail.
This is not reported as a completed concurrency pass.

The replay path now drains accepted bodies in bounded chunks before success,
without rewriting committed part bytes. Its warning-denied optimized build
(`51594bdd`) and all **21 focused regressions** passed. The paced HTTP proof now
passes: no early response, all 65,536 replay bytes received, `200 OK`, a ping on
the same connection, finalization and a matching 131,072-byte object hash.
This build also passed **472/472 library tests** in 556.29 seconds, **210/210
Python regressions**, four backend-selection cases and required native io_uring
integration on both XFS and ext4.

The EC 1+0 HTTP gate passed with 100 concurrent clients: 300 published objects,
300 cancelled sessions, 602 successful identity-preserving resumptions and 301
finalized versions including the partial upload resumed after SIGKILL. Complete
acknowledged hashes and namespace listings passed before/after restart. It ran
in 140.43 seconds; sampled peak RSS was 75,412 KiB and peak FDs 169.

The same 100-client gate passed on two independent HDDs (XFS/ext4), EC 1+1:
another **301 finalized versions / 300 cancellations / 602 resumptions**, with
all acknowledged historical hashes and complete listings verified around the
restart. It ran in 208.628 seconds; sampled peak RSS was 82,900 KiB and peak FDs
194. Rate limiting remained enabled, the 30-second foreground admission deadline
was unchanged and failed writes were never retried in either run.

Scheduled GC also passed on those two filesystems in 28.304 seconds, without
manual GC requests. Controlled aging of owned fixtures exercised expired trash,
synthetic orphans and four real SIGKILL-abandoned standard-upload directories
(new object and unpublished overwrite). All reclaimable paths disappeared;
56 referenced immutable NDX/payload files remained byte/inode/mtime-identical.
Young/future/unknown candidates and active standard/durable uploads survived;
the latter finalized successfully. All 16 retained historical versions passed
hash checks after restart. No unsolicited full healing/health traversal ran.

These patch-qualification gates passed on the same frozen binary. Production
has not been replaced. This is not the original mixed MX/Office integration
rerun, a million-object rerun, physical power-loss qualification or an RC.2/stable
declaration. Storage encoding and acknowledgement durability barriers are
unchanged. The owned test engines are stopped; original failures remain intact.

## Million-object population and inventory investigation — October 27, 2026

The four-SATA/XFS EC 3+1 candidate (`6ec60476`) uploaded **1,000,000 objects /
14,591,947,264 bytes**. Upload phases totalled 14,531.761 seconds (68.81 objects/s,
including source creation and acknowledgement recording). The complete million
version-pinned downloads and SHA-256 checks passed in 2,373.029 seconds.

The overall qualification nevertheless **FAILED** in complete namespace
inventory after 43,464.231 seconds, on the unchanged 120-second socket-idle
deadline. Read-only inspection of the frozen ledger found all one million
inventory entries, with no missing/extra names or version mismatches, but no
terminal completion frame. This is not a complete inventory PASS. The million
mutation and final restart gates did not execute. Sampled peak engine RSS was
493.6 MiB and peak file descriptors 279; no OOM kill was recorded.

The inventory implementation repeated all-member NCX lookups and NDX directory
enumeration. Its final duplicate-replica walk could make progress without
emitting an entry, causing the idle timeout. This endpoint reads NDX metadata;
it is not the filename-only paginated LIST path. GC and shared-device pressure
overlapped portions of the run; GC alone is not established as the cause.

The next candidate removes redundant lookups/enumeration, retains authoritative
replica validation and offers opt-in actual-scan progress. An explicit internal
successful-end marker prevents producer failure from becoming false completion.
Qualification must reuse the existing population in place, with a new pinned
candidate and separate results. The original failure remains unchanged. Churn,
restart and complete inventory remain pending until their new results pass;
these changes do not declare RC.2.

The warning-denied candidate (`edfe1d04`) then passed **451/451 library tests**
on MCH in 271.82 seconds, 19 focused snapshot cases, four backend-selection
cases, the separately required native io_uring case and **192/192 Python
regressions**. Its isolated four-member EC 3+1 fault gate passed conflicting
non-holder pointers, corrupt/missing canonical replicas, all-replica corruption
and checksum-valid pool conflicts. Incomplete streams were rejected; restored
inventory and both historical payload hashes passed.

Preflight attempts are preserved: one was stopped to correct a missing test
directory; another incorrectly required Python to raise on incomplete HTTP EOF.
N1 had withheld completion correctly. The corrected fault test checks the real
consumer rejects incomplete inventory instead of requiring a transport exception.

The in-place headless continuation is now running with this candidate, a 48-hour
workload limit, 4 GiB/no swap and per-filesystem space/growth guards. Completed
regressions are checksum-pinned and reused, not relabelled as new executions.
No source objects are re-uploaded or member roots cloned. Only a new
acknowledgement ledger and bounded probe objects are created for the remaining
inventory, million-mutation, hash and restart gates. Their results remain pending.

## Sustained batch admission investigation — October 26, 2026

The October 24 four-SATA/XFS EC 3+1 run stopped at **713,184 acknowledged
objects / 10,406,776,320 bytes**, not one million. A batch received per-object
503 refusals after the unchanged 30-second foreground-admission deadline.
The last complete integrity checkpoint downloaded and SHA-256 checked all
500,000 acknowledged versions then present. Later acknowledgements still need
independent verification; churn and final restart gates did not run. The
original members, acknowledgement ledger and failed results are preserved.

All prerequisite correctness gates passed, including 426 library tests,
required-native I/O, strict-group fault boundaries, scheduled GC, concurrency,
native protocol and read repair. The matched fresh 2,048-object ABBA comparison
measured 12.46 objects/s for the earlier build and 331.39 objects/s for the
grouped candidate (26.60x); that is not aged-population sustained throughput.
The subsequent 100k-to-500k population interval achieved 75.97 objects/s,
including source creation and acknowledgement recording.

At 500k, the retained median payload-persistence barrier took 3.332 seconds,
versus 27.5 ms for payload/metadata staging and 7.2 ms for NCX publication.
Scheduled GC began one hour into ingestion and was still collecting namespace
references at that checkpoint. Slowdown and reduced foreground concurrency
were already observed before GC began, so GC alone is not a proven cause.
Near failure, one physical member's mean write latency was about 73 ms while
two others were about 22–25 ms. These device counters include all writers.
There were no recorded cgroup OOM events or acknowledged-data checksum failures;
absence of a full final verification is not a zero-loss qualification.

The new candidate coalesces only pending filesystem flushes, retains all
payload → NDX → NCX persistence boundaries, paces GC at safe time/count
boundaries and corrects CPU/I/O-wait accounting. New tests cover late flush
arrivals, writeback-error fanout, poisoned-state wakeup, GC pacing/reference
coverage and pressure accounting. Build, fault and performance results are
recorded only after their respective gates complete. The admission deadline,
storage encoding and release-candidate version are unchanged; this is not RC.2.

The warning-denied release build passed with unchanged engine sources throughout
the build. The frozen candidate (`6ec60476`) passed **444/444 library tests** on
MCH in 267.922 seconds, four backend-selection cases and the separately required
native io_uring case. A second isolated parallel library run passed **444/444**
in 269.15 seconds. Required-native integration also passed on the local XFS and
ext4 HDD filesystems, completing 3,679 operations on each without fallback and
with drained queues. The first ext4 attempt failed before I/O because its test
directory was not writable; it is retained as a fixture-setup failure, followed
by the successful run in an owned test directory. The combined Python harness
regressions passed **150/150**; four synchronization-shim regressions also passed.
The four-member strict-batch fault gate and scheduled-GC gate also passed.
The following concurrency gate stopped when its client could not create the
512 concurrent request threads: the supervisor allowed only 512 tasks for the
client and engine combined. All 512 requests in the preceding 256-concurrency
phase succeeded. This is a preserved harness-resource failure, not a completed
overload pass. A fresh full rerun gives that shared supervisor 2,048 tasks while
retaining its 4 GiB/no-swap limit, engine concurrency and request deadlines.
That rerun passed all 444 library tests again (261.59 seconds), the batch fault
and scheduled-GC gates, concurrency stress, native protocol and read repair.
At 512 concurrent clients, 1,024 offered one-shot uploads produced 751 accepts
and 273 explicit admission refusals. This overload result does not qualify
refusals during the eight-client scale workload. The 256 same-key publishers
and mixed fragment/mutation gates passed, followed by complete acknowledged
integrity checks and restart verification. The stress gate verified 2,127
acknowledged versions; sampled engine peak RSS was 135.2 MiB and peak FDs 563,
returning to 19 FDs in its final drain sample. These are bounded-workload values.

The fresh-population comparison then passed: four strict 2,048-object trials,
old/new/new/old on the same four SATA/XFS filesystems, EC 3+1, eight batch
clients and 64-object batches. Upload-phase rates were:

| Build | Trial rates, objects/s | Combined objects/s |
| --- | --- | --- |
| Previous grouped build (`a6415893`) | 338.12 / 299.72 | 317.77 |
| Optimized build (`6ec60476`) | 372.77 / 373.11 | 372.94 |

The observed aggregate gain was **17.36%**, with non-overlapping measured trial
ranges. Every acknowledged version was hashed before and after SIGKILL restart
in every trial. Candidate sampled peak engine RSS was 92.4–93.4 MiB, with
64 peak FDs. This is short, loopback, fresh-population evidence on shared
filesystems, not controlled cold-cache, aged-population or LAN throughput.

The optional copied-population diagnostic was operator-stopped during its first
member copy. It did not complete reconciliation or compare the builds at that
population; partial copies and original members/evidence are preserved. The
existing 500k checkpoint does not require a copy to read its results.

A fresh headless million-object run now reuses the exact candidate's completed
correctness gates and ABBA comparison, reading those evidence files directly
on MCH with pinned hashes. It does not clone an old store or repeat those gates.
The target is one million new files in one logical folder, then full integrity,
at least one million hot-set namespace mutations, and SIGKILL/restart integrity.
It keeps EC 3+1, eight batch clients, strict durability, normal scheduled GC,
the admission deadline, required io_uring and prior capacity/memory limits.
No million-object pass or RC.2 is claimed while that qualification is incomplete.

## Strict small-file groups — October 24, 2026

The initial grouped candidate passed **426 library tests**, four backend-selection
cases and the separately invoked native io_uring integration case. Subsequent
harness/shim regressions total 109 passes. Executor hardening added two-worker
pool saturation and queued-backup cases. The final warning-denied release build,
both focused executor/backup regressions and explicit native integration passed.
Its frozen 426-test executable started on MCH at **17:27 Manila** and stopped
with **425 passes / one failure**. The failing key-lock cleanup test asserted
that the entire shared registry was empty while unrelated tests could hold
locks. It passed alone, but that does not qualify the parallel suite. The failed
run is preserved; neither its comparison nor its million-object stage started.

The test now checks removal of its own canonical keys while deliberately holding
an unrelated key. This is a test-isolation correction, not an engine change.
A fresh frozen test executable started headlessly on MCH at **17:47 Manila**.
The complete parallel rerun passed **426/426 library tests** in 257.603 seconds.
Backend-selection tests passed **4/4** with two opt-in cases ignored; the
separately required native io_uring case also passed. The full-64-object
strict-batch fault gate passed on all four filesystems: three injected-error
rounds and three process-kill rounds, 106 successful ACKs and 960 historical
hash checks, with no false successful fault ACK. Scheduled GC also passed.
Concurrency, native-protocol and read-repair gates subsequently passed. The
comparison and failed population result are recorded in the October 26 section
above. These injected errors follow real synchronization and do not certify
physical power loss.

An initial two-physical-HDD EC 1+1 gate passed boundary sizes, count/byte limits,
duplicate keys, partial errors, concurrent rename and upload, unchanged NCX
inodes/old immutable files, and 960 historical hash checks. Injected errors and
process kills at each of the three observed synchronization boundaries produced
no false successful ACK. Errors were injected **after real synchronization**;
this is not a physical power-loss result.

The stronger full-64-object fault rerun initially failed **before upload**:
instance-lease synchronization took 41,323 ms on one USB HDD and exceeded the
fixture's 45-second startup timeout. Its later 15-second kill/reap limit also
expired; process absence was subsequently confirmed. Logs are preserved, not
counted as a pass. Another intermediate library run recorded 425 passes and one
isolated-fixture setup `ENOENT`; its mutable build executable was subsequently
absent. The precise deletion timing is unproven. That run is unqualified, and
the final full-suite run uses immutable copies outside the compiler output tree.
The final engine candidate (`a6415893`, full SHA-256 retained with the frozen
package) is unchanged by the test correction. The replacement used an
independent headless supervisor on four SATA/XFS members, EC 3+1.
It requires the final full regressions, full-64-object fault gates, scheduled
GC/concurrency/native/read-repair gates and a matched old/new/new/old comparison
before one million single-folder uploads, followed by one million mutations.
The supervisor stops on failures or resource/deadline limits, without automatic
write retries. It has a 50-hour controller bound, 4 GiB whole-service memory
ceiling, no swap, and per-filesystem reserve/growth limits. Production services
and earlier populations remain unchanged. No new throughput or million-object
success is claimed until the corresponding recorded gates complete. That
supervisor has now stopped at the preserved admission failure described above.

## Native Linux I/O backend — October 23–24, 2026

The first-class io_uring backend passed the following focused correctness gates.
These are not throughput, million-object or new power-loss qualifications.

| Gate | Result |
| --- | --- |
| Locked, offline build with warnings denied | PASS |
| 60 distinct focused unit regressions, including the snapshot-holder ordering regression | PASS |
| Four compatibility/selection integration cases | PASS |
| Required native backend on the XFS HDD | PASS; 3,679 backend requests completed |
| Required native backend on the ext4 HDD | PASS; 3,679 backend requests completed |

Native checks explicitly required `uring`, asserted no fallback, and drained
the queue and in-flight work to zero. Coverage includes 2,048 concurrent empty
operations, 768 positioned direct-write blocks, exact readback, short reads/EOF,
caller cancellation, strict metadata publication, directory sync and propagated
read-only/ENOSPC errors. Unit regressions cover strict/relaxed writer boundaries,
shared NDX allocation and replica placement, NCX copies across filesystems,
checksum boundaries, bitrot reconstruction and read-triggered repair. Child-only
seccomp tests deny ring setup to prove reported `auto` fallback and required
`uring` startup refusal; host security settings are unchanged.

The initial two-job compiler attempt exceeded its 4 GiB build cgroup; the final
single-job build passed. An intermediate source-manifest mismatch was not
counted as qualified evidence; the final build verified unchanged sources.
No storage test failed in this focused run.

Early build/test overlap with the old-binary 300k run is recorded and must
be excluded from isolated throughput comparisons. That run was subsequently
stopped and preserved as described below, not switched to the new backend.
The completed matched optimized-build comparison below found a regression;
focused correctness passes do not establish a speedup. Large-scale qualification
remains pending.

The native integration gate is deliberately opt-in on restricted CI:

```sh
N1_IO_BACKEND=uring cargo test --locked -p n1 --test linux_io_backend -- \
  --exact io_uring_backend_preserves_positioned_io_and_publication --ignored --nocapture
```

Set `TMPDIR` and `N1_TEST_IO_DIR` to an owned test directory on the intended
mounted test filesystem. Run separately on each filesystem to verify both.
Raw manifests and logs remain local test artifacts, not repository content.

October 24 follow-up: at the operator's request, the old upload-only run was
stopped and preserved with 66,832 acknowledged objects (975,159,808 bytes).
The harness records the injected SIGTERM as a failure; preservation records
operator interruption separately. This is not a completed 300k qualification.
All 66,832 acknowledged versions passed exact size/SHA-256 verification. The
following namespace-snapshot phase failed the auxiliary audit's 20-second
socket read deadline. The failure is retained, and no new stress gate was
started by that sequence.

Investigation found a snapshot/export first-frame inefficiency: configured
root traversal can visit a non-selected NCX replica before the lexical holder
chosen for duplicate suppression. With every key replicated in EC 1+1, that
can walk one whole member without emitting an entry. Snapshot/export resolves
NCX and authoritative NDX; it is not ordinary zero-decode LIST. A separate
original-binary instrumented
inventory diagnostic used the existing scale harness's 120-second socket
deadline and a 900-second total bound. HTTP headers arrived in 1 ms, but no
entry frame arrived before that 120-second deadline. Neither failure is relabeled.

The candidate now sorts only its metadata cursor's root copy to match the
existing lexical duplicate-holder selection. Pool-member/EC slot order, all
replica traversal and ordinary LIST are unchanged. Its regression passed. An
initial inventory emitted its first frame in 0.437 s and reached 18,944 entries
before operator interruption; that partial inventory is not a pass. The next
complete audit passed: all 66,832 keys/heads, no extra live keys, and the terminal
frame verified in 731.144 s. Its first frame took 0.004 s on the partially warmed
population; the longest inter-frame gap, including the duplicate-replica tail,
was 11.913 s. This is not an isolated cold-cache throughput benchmark.
The 20-second first-frame requirement and 120-second socket-idle deadline were
unchanged. A separate bounded 3,600-second total correctness budget accounted
for the full authoritative-metadata walk. All prior failures remain recorded.

The optimized io_uring candidate built successfully with warnings denied.
The initial headless sequences correctly stopped at the failed inventory gate.
A headless continuation first requires the complete corrected inventory, then
runs scheduled-GC/interruption, native-protocol and read-repair proofs and
compares the same optimized binary in four fresh, equal
2,048-object strict-upload trials: blocking, uring, uring, blocking. No write
retries, concurrent benchmark workloads or reduced durability are used.
The required-native scheduled-GC proof passed on both HDDs: expired trash,
synthetic orphan cases, real SIGKILL-interrupted uploads/new versions and
historical-hash verification. Its last native snapshot reported 411 submitted
and completed backend requests, no fallback and no queued/in-flight requests;
no manual GC or unsolicited full healing/health scan ran. A controller port-reuse
preflight then stopped the sequence before the next proof. That orchestration
failure is retained; a fresh sequence assigns distinct ports to its phases.

Another fresh sequence failed to reach readiness within its unchanged
45-second startup deadline. Its cause remains unconfirmed. Two separate
instrumented matrices each passed 32 fresh starts, alternating native and
blocking modes with the same 45-second deadline; the second matrix used the
current frozen candidate. The first matrix's slowest native start spent
4.95 seconds in ordinary instance-lease fsyncs, not native queue operations.
These later passes do not explain or erase the earlier timeout. Startup now
reports phase timings and says STARTING until readiness; no sync barrier was
removed.

On the corrected frozen candidate, scheduled GC passed again, including both
real interrupted-upload cases, active standard/durable protection and all
historical hashes. Its run took 33.232 s, with 50,852 KiB peak RSS and 23 peak
descriptors. Native protocol and read-repair proofs also passed; the latter
verified 64 concurrent readers across 16 corrupt objects and physical repair.
These protocol/read-repair fixtures include relaxed/batched uploads; strict
acknowledgement evidence comes from the GC and matched upload trials.

The first blocking comparison passed all 2,048 strict uploads, exact hashes and
complete namespace/head verification before and after SIGKILL/restart. Upload
phases took 579.497 s (3.534 objects/s). The first native trial also passed all
2,048 uploads, hashes and post-restart inventory: 646.455 s (3.168 objects/s),
with 52,950 native operations submitted/completed at the final population
checkpoint and no fallback or outstanding work. It was approximately 10.4%
slower than the first blocking trial. The reverse-order trials also passed:
679.831 s (3.013 objects/s) native, then 656.660 s (3.119 objects/s) blocking.
Across both trials per backend, native throughput was 3.088 objects/s versus
3.313 objects/s blocking: approximately **6.8% slower**, not a performance win.
Each trial verified every acknowledged version and the complete namespace
before and after SIGKILL; native-counter evidence passed separately. Peak RSS
was 94,540 KiB native versus 82,036 KiB blocking across these trials. This is
a bounded strict-small-object comparison on two USB-attached HDDs, not a
general claim about io_uring or a completed scale qualification.

The targeted native NCX interruption proof passed separately: a real SIGKILL
after destination links appeared while source links and the rename request
remained live. All 144 acknowledged historical versions survived across the
128 keys; all 576 committed NDX/payload files retained their paths, inodes,
timestamps, lengths and hashes. An explicit recovery retry completed the
operation. This tests process interruption, not physical power loss.

The old candidate's full 402-test library attempt was intentionally cancelled
during the 4,097-key pagination fixture, without an assertion failure. It is
not a full-suite pass. The replacement candidate must pass its complete
discovered test inventory. Its fixture retains all 4,097 keys, real replica
publication and durability barriers, and every pagination assertion; eight
bounded workers prepare the population before listing starts. Fixture setup
time is not an upload-throughput result.

The revised candidate removes the synchronous worker-to-ring-to-worker bridge:
asynchronous payload/read/sync operations remain native, while synchronous
publication uses POSIX directly with the same durability barriers. This fixed
operation policy is not fallback after an I/O error. Its locked, offline,
warnings-denied test build passed; the complete 402-test library suite passed
with native I/O required (zero failures, ignored or filtered tests; 542.30 s).
The four compatibility/selection cases and separate required-native XFS/ext4
gates also passed. Each native run completed 3,679 requests, reported POSIX
publication, and drained queued/in-flight work to zero without fallback.
The revised tests require strict metadata publication to leave native counters
unchanged, then require three explicit asynchronous syncs to advance them.
They also check POSIX write/sync errors and required-native initialization
refusal. Its optimized release build and 32 fresh starts passed with the same
45-second readiness bound. Its complete 66,832-entry namespace audit passed in
555.062 s, with first-frame and terminal-frame requirements retained. This
repeated inventory is not an isolated cold-cache speed comparison. The full
physical recovery and 2,048-object backend comparison subsequently completed
as recorded below.
Test harnesses now persist an explicit backend selection in their owned environment files
and verify the selected backend at existing health checkpoints; all 66 harness
regressions passed.

### Short before/after upload benchmark — October 24, 2026

Compared frozen optimized builds `7a37820c` (native synchronous publication
bridge) and `ad139db1` (direct POSIX synchronous publication), both requiring
io_uring for their asynchronous paths. Four fresh trials ran old/new/new/old,
128 objects each, on the two USB-SATA HDDs with EC 1+1 and loopback HTTPS.
Each trial used eight concurrent 16-object batches, server concurrency 16,
strict acknowledgement and the same deterministic 0–64 KiB size mix. No write
retries, cache flushes, durability changes or overlapping N1 workload were used.

| Metric | Previous build | Revised build |
| --- | --- | --- |
| Aggregate objects / upload time | 256 / 88.599 s | 256 / 67.092 s |
| Aggregate upload throughput | 2.889 objects/s | 3.816 objects/s |
| Mean 16-object batch HTTP request | 41.148 s | 31.528 s |
| Sampled peak engine RSS | 58.52 MiB | 58.57 MiB |
| Sampled peak descriptors / quiescent descriptors | 105 / 18 | 55 / 18 |

Throughput was 32.1% higher in this short comparison; memory was effectively
unchanged. Individual upload phases were 38.078, 30.833, 36.259 and 50.521 s,
so variability is visible rather than hidden by choosing the fastest run.
Upload timing includes client source preparation and acknowledgement logging;
batch latency is not individual-object acknowledgement latency. All 512 strict
uploads, 1,024 historical payload-hash checks and complete namespace/head
inventories before and after real SIGKILL/restart passed. The long audit
finished before the first trial, and its controller resumed afterward without
changing its deadlines. This is a small-object result, not streaming-bandwidth,
LAN, million-object, physical-power-loss or stable-release qualification.

Separate source-review finding: the existing atomic-metadata helper can leave
an unpublished hidden temporary sibling if its write or file sync fails before
rename. The previously committed target is not replaced on that path. This is
not introduced by native I/O, but temporary-file reclamation on that error path
is not certified by these backend tests.

### Completed local sequence and MCH launch — October 24, 2026

The frozen revised candidate `ad139db1` completed the local controller's
correctness sequence: full library/selection/native-filesystem gates, optimized
build, 32 fresh starts, the complete 66,832-entry namespace audit, scheduled GC,
native protocol, read repair and NCX interruption checks. No disk-format or
NCX/NDX architecture change was made for the server test.

Its longer matched 2,048-object blocking/uring/uring/blocking comparison also
passed correctness in all four trials. Aggregate upload throughput was
**3.5907 objects/s blocking versus 3.3400 objects/s native**: native was about
**7.0% slower** on the two USB HDDs. This same-binary backend comparison and
the short old/new implementation comparison above answer different questions;
neither establishes MCH throughput or million-object scale.

MCH's fresh headless sequence started on four independent physical SATA HDDs
with XFS and EC 3+1. The uploaded engine/proof/helpers were SHA-256 verified;
the engine and storage format were not rebuilt or changed on the server.
The scheduled-GC gate passed with required-native initialization on all three
owned starts. RC stress was running at the first launch check. Native protocol
and physical read-repair gates precede one million upload-first objects, then
one million hot-set mutations and final hash/inventory/restart verification.
Those later gates remain pending until their recorded results complete.

The supervisor runs independently of the SSH session, with no automatic write
retries or restart loop, a 4 GiB memory ceiling and a 49-hour overall bound.
It guards four mount/device/UUID/serial identities, preserves at least 140 GiB
or 15% free per filesystem and stops at 64 GiB growth per filesystem. Existing
production N1, Accel and Console PIDs were unchanged after launch. Their shared
hardware load means this is not a dedicated-server benchmark. No production
storage was formatted, restarted or used as the fixture. A launched workload
is not a million-object, RC.2 or stable-release PASS.

At the operator's subsequent request for an isolated large-file measurement,
this sequence was stopped and preserved at **10,000 acknowledged objects,
145,867,264 bytes**, during the 10k integrity checkpoint. The last progress
snapshot recorded 5,509 historical downloads verified. The controller records
the injected SIGTERM as a failure; the accompanying interruption record
identifies the operator stop. This is not an engine-failure diagnosis or a
completed million-object qualification. All four prerequisite gates had passed.

### On-server strict 5 GB throughput — October 24, 2026

The same optimized `ad139db1` binary ran in a separate fresh EC 3+1 fixture on
MCH's four SATA/XFS members. The scale workload was stopped first; production
services were left running. Payload traffic stayed on CA-validated loopback
HTTPS, not the LAN. Each object was exactly **5,000,000,000 bytes** (decimal
5 GB), with strict acknowledgement, no write retry and required native I/O.
Only this disposable fixture's request/file-size limit was raised to 5 GB;
the default 50 MiB one-shot part limit would otherwise reject the request.
No engine code, storage format or durability barrier changed for the benchmark.

| Measurement | Elapsed | Throughput |
| --- | --- | --- |
| Upload from a real file on member 0's HDD | 48.185 s | 103.767 MB/s; 98.960 MiB/s |
| Upload identical bytes from a bounded-memory generator | 27.182 s | 183.944 MB/s; 175.423 MiB/s |
| Historical GET + SHA-256, first object | 37.746 s | 132.463 MB/s |
| Historical GET + SHA-256, second object | 40.397 s | 123.771 MB/s |
| First object's GET + SHA-256 after SIGKILL/restart | 37.656 s | 132.782 MB/s |
| Second object's GET + SHA-256 after SIGKILL/restart | 40.267 s | 124.173 MB/s |

Upload time runs from connection establishment through the complete HTTP 201
acknowledgement, including strict commit and client-side source hashing.
Generating, hashing and fsyncing the source file took 30.013 s separately and
is excluded. Both input methods use bounded 1 MiB buffers, not a 5 GB in-memory
file. Post-body response waits were 0.369 s and 0.318 s respectively.

All **10 GB uploaded and 20 GB read back** passed exact version-specific length
and SHA-256 checks, including both objects after a real owned-process SIGKILL.
Peak engine RSS was **46.059 MiB**, peak externally observed descriptors 26,
and quiescent descriptors 20. Engine CPU time during the uploads was 12.09 s
and 10.72 s, about 25% and 39% of one core averaged over each upload. Each
upload advanced native completions by 8,488 with no fallback. Whole-device
counters showed approximately 1.667 GB written to each of the four HDDs per
upload; the file-source trial also read 4.664 GB from member 0's drive.

The generated-input trial removes source-HDD read contention; it is not an
old/new build or blocking/native comparison. Source and historical reads may
use filesystem cache; strict payload writes use direct I/O. No host cache was
dropped, and no cold-cache claim is made. Whole-drive counters also include
unchanged production services. These two uploads demonstrate this fixture's
large-object throughput and bounded memory, not repeated statistical coverage,
physical-power-loss durability, tiny-object throughput or million-object scale.

### Fresh small-object million-scale baseline — October 24, 2026

After the large-file benchmark, a new isolated headless sequence was launched
on the same four physical SATA/XFS members with the same `ad139db1` engine.
Scheduled GC passed; RC stress was running at the initial launch check. The
remaining native/read-repair gates precede **one million 0–64 KiB objects**
with eight clients, 64-object batches and server concurrency 16. The fixed
offered load, strict acknowledgement, required-native policy and no-write-retry
rule are unchanged. This is a current-build small-object baseline, not a claim
that small-file performance was fixed or that a new optimization was compared.

The scale stage retains its 48-hour bound; the controller has 50 hours overall
to accommodate prerequisite gates and cleanup. The service's 4 GiB cgroup
limit includes client, engine and charged filesystem cache/slab, rather than
being an engine-RSS measurement. Mount/physical identities and the existing
free-space/growth reserves remain guarded. The interrupted 10k population,
large-file fixtures and all production data/services were preserved. Final
million-object and churn results are pending.

## RC.2 qualification plan — October 22, 2026

Preparation is not a release declaration. The current release remains RC.1.
The next server target is MCH: four independent physical members, EC 3+1,
inside new disposable directories. Existing production roots and services must
remain untouched. Test-service isolation does not remove shared disk/CPU load;
record that interference rather than claiming a dedicated-server benchmark.
The [MCH launch runbook](tools/N1-MCH-SCALE.md) records the prepared workload,
hardware/capacity checks and exact coverage limits. Preparation does not mean
the server was contacted or its hardware passed preflight.
October 24 preparation checks passed: 28 synthetic result-validator regressions
and 45 existing scale-harness regressions. They launched no engine and are not
MCH or million-object results. The validator pins the engine hash and checks
the fixed offered load, full population/verification phase order, immutable
churn inventory, process-local native evidence and resource bounds; it does
not replace hardware preflight or independently rehash the stored objects.

First run the same population-then-churn workload locally with a 300,000-object
target on the two HDDs, EC 1+1. This is an early-defect gate, not qualification
of the four-member topology or the million-object target. Run it headlessly;
retain automatic resource samples, acknowledgements and failure evidence, with
no automatic write retries or restart-on-failure loop.

The server sequence is:

1. Verify physical member identities, mount/filesystem identities, free space,
   existing N1 roots, unused ports and descriptor limits. Freeze the tested
   engine, harness and configuration. Set a free-space reserve and growth
   budget before launch; never fill production storage to literal exhaustion.
2. Repeat scheduled-only GC and the recovery matrix at EC 3+1. Checksum/read
   repair must physically restore damaged fixture shards without changing
   committed NDX. No production-device removal, reboot or power cut is implied.
3. Upload one million distinct objects first, with strict durability and no
   concurrent namespace churn. Keep the eight clients, 64-object batches,
   server batch concurrency 16 and 300-second request deadlines explicit.
   At population milestones verify all acknowledged historical versions,
   paginated prefixes, complete namespace/max-ULID inventory and resources.
4. Only after population verification, exercise the populated store: concurrent
   rename/move/trash/recover, additional uploads, GC overlap, exports/backups,
   targeted corruption/read repair and process-interruption recovery. Distinguish
   checks performed on this population from smaller separate recovery fixtures.
5. Check all expected historical bytes and namespace state after restart, and
   hold the population through a sustained mixed-load soak and an idle period.
   Compare quiescent memory/descriptors and retained bytes before/after cleanup.

One million is the first scale gate, not a supported-object-count ceiling.
For larger capacity runs, use successive 2/5/10-million-style milestones and
extend toward the server's predeclared safe capacity. Estimate limits from
measured per-member allocated bytes, file/directory counts, available inodes,
namespace/ledger overhead and recovery reserve, not payload size alone. No
hundred-million-object claim follows from a smaller population or extrapolation.
Select the next target and duration from actual server measurements; a space,
time or resource stop is an incomplete qualification, never a PASS.

Record objects/s, bytes/s, phase duration, request latency distributions and
errors alongside engine/client CPU, RSS, descriptors, memory/I/O pressure,
disk latency/utilization and filesystem growth. Source-file generation and the
synchronous acknowledgement ledger also consume fixture I/O: end-to-end ingest
includes those costs. Loopback HTTPS is not a LAN, native-protocol or Console
benchmark. Keep those integration results separate.

Two scale questions remain explicit gates: listing currently scans the relevant
namespace again per page, so bounded page memory does not imply bounded scan
work; GC reachability preparation also uses population-sized state. Measure
both at large populations. Three small-prefix listing checks plus a full
streamed snapshot do not certify a complete flat million-key paginated listing.
Any full-population verifier must itself be disk-backed/bounded in memory.
Require a completed GC cycle at the populated workload, not merely enabled GC.

The scale runner's population-then-churn mode is `--churn-after-upload`.
`--upload-only` remains a separate no-churn workload. The recovery runner accepts
explicit `--data-shards 3 --parity-shards 1` with four member parents; the older
two-member `n1-hdd-qualify.py` wrapper must not be used unchanged for MCH.
Interrupted fixtures retain their ledgers for reconciliation; this is not an
automatic in-place resume or permission to retry ambiguous writes.

Preparation validation passed 67 Python harness checks. The engine was not
changed for this preparation: the local scale run uses the same
`5b77e3a116d5abd15360d1027fce9388af5c8b7cf19c49a87957756dd113e66d`
binary as the completed integrity gates below. A separate 64-object physical
smoke must pass before the headless 300,000-object stage starts. That stage
retains a one-million-mutation hardening target, all-version hashes, full
snapshot inventory and SIGKILL/restart verification. It reserves 128 GiB per
filesystem and stops at an 80-GiB filesystem-growth budget, including unrelated
writers. The overall workload deadline is 48 hours; per-request limits remain
300 seconds. These are predeclared limits, not passed results. Automatic GC
retains its normal settings; this scale runner does not by itself certify a
completed GC cycle, backup overlap or bitrot injection at 300,000 objects.
The physical smoke passed: 64 source objects, eight probes, 128 namespace
mutations and integrity/inventory checks after restart. The 300,000-object
stage then started under a detached service. Its live report is RUNNING, not a
completed scale PASS. An earlier launcher-only sleep-inhibitor timeout occurred
before any engine/fixture started and is retained separately in launch evidence.

RC.2 requires zero lost/corrupt acknowledged data, correct complete inventory,
successful restart/recovery and GC protection, and no unresolved engine crash,
deadlock, resource leak or unexpected timeout/refusal at the declared sustained
load. Record saturation/refusals separately in deliberately excessive overload
tests and verify recovery afterward. Performance acceptance must be set against
the recorded workload/hardware before judging the result; a slow operation is
not automatically a hardware fault. Every discovered defect follows reproduce,
minimal patch, regression, rerun; changed candidates need new evidence.

Stable is a separate decision after the planned scale, soak, backup/restore,
pool replacement, capacity-pressure and integration gates. Previously completed
power-loss tests remain dated evidence for their actual builds; SIGKILL is not
a new physical power-loss certification. No RC.2 or stable claim is made yet.

## Maintenance/upload optimization — October 21–22, 2026

### Scheduled GC integration — two-HDD integrity gates passed

Scheduled and manual GC now share the orphan/trash collector. The candidate
adds configured-member and path validation, late-export protection, unpublished
version cleanup beneath live objects and cooperative BackgroundGc admission.
The bounded integrity gates below passed. Million-object scale qualification
remains separate and was not run in this session.

The local two-HDD EC 1+1 harness passed ten safety/reporting checks. Its first attempt
against the old binary failed before any upload: engine startup exceeded the
unchanged 45-second limit, and SIGKILL cleanup exceeded its 15-second wait.
Kernel SCSI timeout counters increased on both USB-attached members during the
attempt. No kernel log was available without administrator access, so the exact
device/bridge cause is not established. The attempt does not reproduce the GC
defect or certify the corrected candidate. Logs were retained and disk-load
testing was stopped pending a stable storage connection. Cleanup-timeout reporting
now preserves the original failure and writes a failed result rather than losing
the report; both added mocked regressions pass. No million-object test or
production-server update was started.

After reconnecting the drives, a new attempt used the project HDD's XFS volume
and the separately formatted test HDD's ext4 volume. All 369 engine tests passed
in 1,252.90 seconds, with zero failures, ignored or filtered tests. All 56 harness
checks and nine proof-tool tests passed; the optimized build denied warnings.
That intermediate engine SHA-256 was
`edae7bd4065035d5a9cfb15a9034b66a181f40ff2fb0be0c2a7335bc5f920e97`.

The physical scheduled-only proof reproduced the old binary's orphan-reclaim
gap. The intermediate candidate reclaimed expired object data and six abandoned
version directories, reporting no GC errors, but failed the unchanged proof
because empty expired-trash generation directories remained. This is a failed
cleanup gate, not a completed integrity PASS. Its logs and fixtures are retained.
GC expiry now prunes only known empty trash ancestors, without adding pruning
syncs to ordinary recovery. All 11 focused namespace tests passed, including
three new expiry/retry/path-safety regressions. The corrected optimized engine
SHA-256 is
`5b77e3a116d5abd15360d1027fce9388af5c8b7cf19c49a87957756dd113e66d`.

Its scheduled-only two-HDD proof passed in 20.496 seconds, without any manual
GC request. It reclaimed two expired trash versions and their four NDX replicas,
four real SIGKILL-interrupted upload directories (including a new version under
a live object) and two synthetic expired orphan directories. Six young, future
or unknown candidates survived; active standard and durable uploads finalized.
All 16 retained historical versions hash-verified after restart; 56 committed
NDX/payload files retained identical bytes, inodes and mtimes. Sampled peak RSS
was 50,644 KiB and peak descriptors 18. No unsolicited full health/healing walk
was observed despite legacy scan settings being enabled in this fixture.

The complete corrected-candidate suite passed all 372 tests in 1,198.57 seconds,
with zero failures, ignored or filtered tests. The physical read-integrity proof
passed in 47.037 seconds: all nine HTTP/native/archive bitrot paths returned
verified bytes, physically repaired the damaged shard and left NDX unchanged.
A 64-reader burst verified and physically repaired all 16 corrupted objects in
0.466 seconds. Trash maintenance passed in 76.061 seconds, covering 140
directory objects across the 128-entry maintenance batch boundary.

The concurrent GC/export/upload proof passed in 1,159.913 seconds under the
explicit 7,200-second aggregate budget (not the earlier failed 300-second
budget). It retained four actors with 1,000 files each plus 1,000 background
uploads, standard/durable multipart, namespace mutations, archive validation
and GC overlap. The mixed actors finished in 1,067.841 seconds. An injected
corrupt durable manifest caused preservation with a reported error; removing
that fault allowed a clean reclaim pass. Restart visibility and the log scan
passed; the final engine RSS sample was 144,988 KiB. Per-request deadlines and
workload sizes were not shortened for this rerun.

Maintenance-mode coverage subsequently passed in 40.038 seconds. The final
concurrent standard-multipart/batch/mutation proof passed in 111.092 seconds,
including 716 expected live objects and restart visibility. All five physical
recovery/maintenance gates passed on the same frozen engine; total recovery
runner time was 1,434.168 seconds. Both member kernel timeout counters remained
unchanged throughout the completed run. Test engines exited and no production
deployment or million-object stage was started.

The focused read timings above are small warm-fixture correctness observations,
not a general latency or throughput guarantee. The original failed attempts are
retained; their results are not retroactively converted into passes.

### Read-integrity/maintenance follow-up (in progress)

The latest 100,000-target run failed at 19,216 source objects/19,344 acknowledged
versions with a 300-second batch response timeout. Its sampler also recorded an
unavailable process FD directory as a storage fault. These remain distinct:
the saved report does not prove that the sampler caused the request timeout.
No OOM was recorded; sampled peak engine RSS was 207,588 KiB.

An exact-binary restart against same-filesystem reflink copies independently
verified all 19,344 acknowledged historical versions. The 16 ambiguous batches
covered 1,024 source keys: 900 additional versions were committed and hashed,
124 were not published. Complete namespace/max-ULID inventory matched all 20,244
visible versions. No upload was retried; original reports/ledgers were unchanged.
This is an integrity reconciliation PASS, not a sustained-load PASS.

A new same-length byte-flip regression reproduced the unverified streaming read
and then passed after routing it through existing checksum validation. A second
regression reproduced NDX inspection before trash TTL and passed after moving
eligibility ahead of version metadata reads. A two-data/two-parity fault case
then reproduced incorrect short-range reconstruction and suppression of a second
target's repair after rejecting it as a peer. Recovery now decodes the recorded
stripe before slicing; request-local handles distinguish rejection from repair
admission. The strengthened test covers two stripes and unaligned ranges and
verifies both files are physically repaired.

All six focused recovery tests passed; 20 HDD-harness, seven RC-harness and nine
Rust proof-tool checks passed. The earlier intermediate build passed all 340
engine tests. The preceding candidate passed all 343 tests in 1,306.98 seconds, with
zero failed, ignored or filtered tests. Candidate engine SHA-256:
`d79369c29af2c9760456fac26f4fab489918c9b7196953981a3c8cd737fcd364`.
That candidate's nine public read paths passed same-length bitrot recovery, physical
repair and NDX-immutability checks. A 64-reader burst recovered and physically
repaired all 16 corrupted objects in 1.437 seconds. Native-path elapsed samples
include connection/authentication; these small, warm-fixture correctness checks
are not a controlled throughput or cold-read benchmark.

A separate harness smoke run passed 64 source uploads, eight probe objects,
352 namespace mutations, complete inventory and post-SIGKILL checks. Sampled
engine peak RSS was 50,148 KiB and peak descriptors 61. These bounded results do
not substitute for the million-object population.

That candidate's matched strict-batch gate accepted all 2,048 new objects on a
12,438-object fixture, then hashed all 14,486 acknowledged versions and verified
complete inventory. Ingest took 398.879 seconds versus the saved baseline's
581.495 seconds; foreground queue p99 was 4.173 versus 10.789 seconds. The
overload/crash gate also passed. These observations belong to the recorded
candidate and are not universal latency guarantees.

The expanded mixed GC/export/upload run failed its 300-second aggregate
deadline with roughly 5,000 files plus multipart and namespace work. Saved logs
show completed GC runs and continuing uploads, not proof of a deadlock. Its
unchanged workload subsequently passed in 1,063.778 seconds under an explicit
7,200-second aggregate completion budget. Per-request limits stayed unchanged;
this proves completion, not compliance with the original 300-second deadline.
The original failure remains recorded.

A subsequent regression reproduced read repair bypassing whole-fragment deletion
admission. The correction reuses existing shared lifecycle admission without
waiting behind deletion, and checks live fragment identity before publication.
The regression verifies deferral, subsequent physical repair and no recreation
after removing identity. The latest warnings-denied optimized build completed
and passed all 344 engine tests in 1,326.76 seconds, with zero failed, ignored
or filtered tests. Latest engine SHA-256:
`ffadcc396fc9e714348759f6057b6af2bdea3ef59fdcf204f92e9218362a8fa4`.
Its matched strict-batch gate accepted 2,048 new objects and hash-verified all
14,486 acknowledged versions. Ingest took 477.631 seconds versus the saved
581.495-second baseline; foreground queue p99 fell from 10.789 to 4.745 seconds.
The overload/crash gate and all five recovery/maintenance gates passed, including
the complete mixed GC workload above. These are bounded two-HDD results.

The subsequent extreme-overload gate failed in the load generator: its user
service inherited a 1,024-descriptor soft limit while attempting up to 2,048
concurrent connections. Client-side EMFILE errors prevented the intended load;
all 570 acknowledged versions still hash-verified after the three crash rounds.
That gate is not a PASS, and the million-object stage did not start. A fresh
headless continuation now checks descriptor capacity before creating fixtures
and runs with a 65,536-descriptor test-service limit. The preflight rejects the
original 1,024 limit. With that launcher correction, unchanged extreme overload
passed in 342.084 seconds: 1,181 acknowledged versions, zero unexpected failures
and three inflight SIGKILL rounds with hash verification. The operator then
stopped the subsequent million-object attempt during probe setup; it had not
begun source population. Earlier failure and interruption evidence is retained.

After reboot, the first restart exposed a separate load-generator bug before
any acknowledged upload: its zero-initialized authentication timestamp skipped
initial login when host uptime was under ten minutes. Two new checks reproduced
the failure; the two-condition harness correction passed all 23 harness tests,
including concurrent initial login and the unchanged refresh interval. N1's
binary and all engine sources are unchanged. The resumed mixed run was stopped
at the operator's request after 8,680 source objects, 1,024 probe versions and
112,383 namespace mutations. Termination produced connection-refused/ambiguous
batch results; those reports and ledgers are preserved, not relabeled as a
completed qualification. Its launch manifest records the changed harness files
and original completed gates.

The current priority is a separate upload-only million-object gate. It retains
the same engine, two HDDs, EC 1+1, eight strict batch clients, 64-object batches,
server batch concurrency 16 and 300-second request deadlines. No mutation probes
or churn workers run; listing, full historical hashes and resource checkpoints
run between ingestion milestones. Final inventory and SIGKILL/restart hash
verification remain required. Two additional harness checks prove upload-only
mode cannot start probe/churn work; all 25 harness tests pass. A fresh 64-object
physical-HDD smoke passed in 17.649 seconds, including all hashes before/after
restart and zero namespace mutations. At the operator's request, the subsequent
run was stopped at 32,528 acknowledged source objects. Its interruption report
is preserved, not a million-object PASS. Exact-old-binary verification on cloned
members hashed all 32,528 acknowledged versions. The 16 interrupted batches
contained 148 additional committed-and-hashed objects and 876 unpublished keys;
none were retried. Complete namespace/max-ULID inventory matched all 32,676
committed versions; original reports and ledgers remained unchanged.

Live diagnostics showed a foreground target of four against a configured six,
with eight queued operations and healthy host growth thresholds. The adaptive
growth condition rejected queued demand itself. A new regression reproduced
that behavior with the old condition (target stayed four instead of increasing
to five). The candidate permits bounded recovery under queued demand while
retaining saturation, stall and host-pressure guards. All six adaptive-controller
tests pass with the candidate, as do all 27 HDD-harness checks, including distinct
accounting for reconciled, unacknowledged commits. The full candidate suite passed
all 346 engine tests in 1,244.86 seconds, with no failed, ignored or filtered
tests. The warnings-denied release build and subsequent overload/crash gate
passed. Candidate SHA-256:
`b4dda771f981cd87280c0a8591d5b7901c47291c2a9abfb1e967b8acf862c396`.

The sequential matched comparison uploaded 2,048 strict objects in 535.168 seconds
on the old build and 535.453 seconds on the candidate: effectively unchanged
throughput. Each independently cloned population then passed hashes and complete
inventory for all 34,724 known versions. Both builds stayed at six foreground
permits throughout the sampled workload, so this comparison does not demonstrate
the recovery-from-four improvement. Strict durability and request deadlines were
unchanged. The subsequent laptop million-object attempt was stopped to relocate
testing; its ledger contains 3,048 acknowledgements. Its partial evidence is
retained, not a completed scale qualification.

Testing moved to an isolated four-HDD XFS server fixture, EC 3+1, while existing
production N1 continued running. Only disposable test directories are writable
inside the test service; production storage directories are inaccessible. A
6-GiB service memory limit, 768-GiB minimum free space per member filesystem and
80-GiB filesystem-growth budget protect the shared server. Growth includes other
writers; crossing a limit stops the test rather than qualifying it. Four new
harness checks cover topology, mounted child-directory placement and space limits;
all 31 harness tests pass. The 64-object server smoke passed in 5.06 seconds,
including all hashes/inventory before and after restarting only the test engine.
The fresh million-object upload-only run is active as a system service, independent
of the client connection. This is shared-server loopback testing, not a direct
comparison with the USB-HDD topology, a LAN/Console benchmark or a physical
power-cut test. Production services and existing data were not replaced.

Early fault-matrix attempts stopped on an invalid archive request, an incorrect
job-status observer field, or a recovery request before restart readiness. Those
harness failures were preserved and corrected without relaxing engine gates.

The separate mixed-workload design targets one million actual source objects
and at least one million namespace mutations on two XFS USB HDDs, EC 1+1.
Strict eight-client batch upload runs alongside 32 namespace clients; subsequent
churn ramps to 256 clients. Its upload rate is not an upload-only benchmark.
Every acknowledged historical version, complete quiescent namespace and
post-SIGKILL state must verify. Existing engine self-reported FD/RSS checkpoints
supplement external resource samples; unavailable telemetry is not a zero value.
Timeouts, admission refusals, missing results, resource stops and ambiguous writes
fail the sustained-load gate without write retries. Source fingerprints and
frozen binaries prevent mixing revisions within a qualification.

Whole-shard checksum encoding and ordinary copy/delete barriers are unchanged.
No million-object PASS is recorded. Normal-upload restart cleanup locators and
eliminating reclamation's existing global reference walk remain unfinished;
this follow-up must not be described as completing those changes.

Implementation uses persistent single-pass maintenance cursors, preserved upload
pipeline overlap, parallel NDX-holder publication and fewer existing-parent syscalls.
Product and storage-format versions are unchanged. This is not a new release
or a completed million-object qualification.

The final pipeline-preserving build passed the sequential short-gate matrix on
two physical USB HDDs, EC 1+1, XFS and loopback HTTPS/native transport. It is not
a LAN, browser/Console folder-drop or physical power-cut qualification.
Candidate engine SHA-256:
`4921e039edd15f65a685674e14bf2010da73ab6bb93effd420526711b19ea1f1`.

| Gate | Result |
| --- | --- |
| Regression rerun | PASS: 333 engine tests, zero failures; 19 Python harness checks |
| Strict 64-object batches, eight clients | 4.74 → 4.92 objects/s, matched before/after |
| Strict 16-object batches, eight clients | 4.16 → 4.76 objects/s, matched before/after |
| Matched-benchmark integrity | All 1,024 acknowledged versions downloaded and SHA-256 verified before and after SIGKILL |
| Partial admission results | Three committed versions and 509 explicit refusals; every committed version hashed after restart |
| 512-client overload/crash gate | PASS, 345.616 s; all 1,432 acknowledged versions hashed after restart; 5,728 immutable NDX/payload files unchanged by NCX mutations |
| Recover-on-read and automatic healing | PASS, 57.029 s, including missing/foreign roots and cold beyond-parity recovery |
| Trash traversal beyond 128 entries | PASS, 46.021 s; 141 recovered object hashes checked |
| Concurrent multipart/batch/mixed users | PASS, 85.042 s; restart visibility verified; standard batch 27.743 objects/s with the proof tool's relaxed payload default |
| End-to-end scale harness | PASS, 256.444 s: 1,024 source files, 1,032 versions, 160 NCX mutations, full inventory/max-ULID comparison and historical hashes after restart |

Combined strict-upload time fell 8.4% in this small comparison, not a general
throughput SLA. Peak sampled engine RSS was 65,468 KiB versus 67,760 KiB in the
matched benchmark, with 140 FDs in both. The overload gate peaked at 133,464 KiB
and 554 FDs. Refusals never count as successful writes, and ambiguous writes are
not blindly retried. During the prefix-copy SIGKILL, all 256 source objects
remained reachable and eight also had destination names; retry converged.

The 333-test rerun excludes only the expensive 4,097-object pagination fixture
passed earlier on unchanged listing/publication code. It is not a fresh full
334-test pass. Seven targeted maintenance checks cover cursor advancement,
replica deduplication, missing/corrupt trash copies, unchanged NDX bytes and
early-stop fanout probes. Small maintenance timings did not improve measurably:
regressions prove removal of repeated enumeration, not a measured large-scale gain.

The fresh million-object/million-mutation run FAILED after 3,226.013 seconds.
It acknowledged 12,310 source objects, plus 128 probe versions, before a batch
reported 58 explicit HTTP 503 refusals: foreground disk admission exceeded its
unchanged 30-second deadline. The last in-run full integrity checkpoint checked
all 10,128 versions at the 10,000-source milestone. Only the 512 preflight NCX
mutations completed, not the million-mutation target. Peak engine RSS was
103,832 KiB, with 128 FDs; the 4 GiB/no-swap cgroup recorded no OOM events.
This is a sustained-ingest failure, not a million-object PASS.

An independent restart of the exact failed candidate then PASSED: all 12,438
acknowledged historical versions were downloaded and SHA-256 verified, and a
complete streamed namespace matched every ledgered name and maximum ULID.
The recheck took 53.502 seconds, including 42.088 seconds for hashes and 4.467
seconds for the namespace comparison. Original reports and acknowledgement
ledgers were retained. The first verifier attempt incorrectly used the physical
fragment ID at an alias-based authentication endpoint; the driver was corrected
and alias/membership safety regressions added before the completed recheck.

The saved engine log shows the automatic availability scan completed near
startup in roughly one second, with zero repairs. It does not establish that
healing or GC caused the later admission failure. Further diagnostics capture
active permit owners at admission timeout and bounded payload/NDX versus NCX
latencies without adding storage scans or weakening durability. Maintenance
policy changes and another million-object qualification remain pending.

A separate reflink clone of the failed population passed a diagnostic baseline:
2,048 additional strict uploads at eight batch clients, followed by hashes of
all 14,486 acknowledged versions and a complete namespace/maximum-ULID comparison.
Ingestion took 581.495 seconds; the original reports and ledgers remained
byte-identical. This shorter run did not reproduce the original 30-second
refusal. Its last 1,024 admission samples nevertheless had a 10,789.319 ms p99
and 19,040.659 ms maximum, compared with a 2,966.905 ms p99 payload/NDX duration.

Inspection found that adaptive admission could surrender semaphore queue
position and retry every 2 ms. The replacement keeps the existing FIFO
semaphore, retiring permits as operations finish when capacity shrinks and
returning them when capacity grows. Nine focused scheduler tests pass, including
four new checks for FIFO order, cancellation, growth and repeated resizing.
This establishes the tested admission behavior, not that every cause of the
original sustained-ingest failure has been eliminated.

That admission change's subsequent 100,000-target qualification produced the
19,344-acknowledgement timeout and integrity reconciliation described above.
It did not complete its population/mutation target. The current qualification
instead targets the full million objects and million mutations after all
regression, matched-load and fault gates pass. Scaling remains blocked if
matched strict ingestion regresses by more than 10% or admission p99 does not
improve. Pending gates are not completed passes.

Earlier attempts remain part of the evidence. The first benchmark stopped after
512 uploads because the driver sent an unnecessary JSON body to a query-only
maintenance endpoint, breaking its next keep-alive poll. Bodyless calls passed
a fresh preflight. An initial candidate passed integrity but regressed strict
batches by 6–18%; restoring direct I/O alone still left combined time 14% slower.
Buffered strict writes and the extended payload-permit lifetime were withdrawn,
preserving the original pipeline and durability barriers. An isolated upload
test also exposed missing bootstrap settings and a subsequent thread-barrier
hang; using valid stable fragment IDs removed that test-order dependency without
changing engine behavior. None of those attempts is relabeled as the final pass.

## Two-HDD RC.1 stress campaign — October 20, 2026

Fresh isolated EC 1+1 fixtures use two physical 1 TB USB HDDs with XFS.
Test data, generated source folders, reports and build outputs are on those
HDDs; the internal NVMe is not a pool member. Existing projects are not inputs
and are not modified. Traffic is loopback, not a LAN benchmark or physical
power-cut test. The engine is the unchanged October 18 optimized RC.1 binary
identified below.

Completed gates:

| Gate | Result |
| --- | --- |
| Strict uploads, up to 512 concurrent clients, shared-key versions, fragment isolation and production rate-limit refill | PASS, 342.976 s; all 1,341 acknowledged versions hashed across forced restarts |
| NCX-only mutations and observed mid-prefix-copy SIGKILL | PASS; 5,364 immutable NDX/payload files unchanged; all 256 objects remained available, 8 temporarily had both names, retry converged |
| Extreme offered load, up to 2,048 concurrent clients, shared-key contention and three inflight SIGKILL rounds | PASS, 342.897 s; all 1,182 acknowledged versions hashed; zero unexpected errors; admission-refused unique names absent |
| Small end-to-end scale-harness check | PASS; 128 actual source files, 136 total versions, 160 namespace mutations, complete streamed namespace comparison and historical hashes after restart |
| Recover-on-read, autonomous repair, missing/foreign root fencing and cold beyond-parity fail-safe/live recovery | PASS, 41.040 s in the corrected Rust matrix |
| Trashed-object repair across the 128-entry boundary and recovery after restart | PASS, 45.041 s; 141 recovered object hashes verified |
| Concurrent multipart, 4 × 64-object standard batches, mixed users and restart visibility | PASS, 80.566 s; 716 expected live objects; standard batch 5.692 s / 44.978 objects/s, with the Rust proof's relaxed payload default |

Peak sampled engine RSS was 138,940 KiB / 548 FDs in the first gate and
292,736 KiB / 2,091 FDs in the extreme gate. Overload PASS means bounded
admission and acknowledged-data integrity, not acceptance of every request.
Strict small-object upload ramps sustained approximately 4–7 objects/s on this
fixture and produced scheduler-admission 503s at high concurrency. These results
do not establish a high-throughput or sub-100-ms upload SLA.

Harness failures are retained separately from engine failures. A queued stage
initially mistook a RUNNING report for a terminal failure; terminal-state waiting
was corrected and regression tested. The older trash-maintenance proof also
requested empty-directory creation. N1 correctly rejected it: object keys
establish prefixes. That stale setup step was removed and the optimized proof
tool rebuilt; engine namespace semantics were not changed.

Tightening churn validation also initially used internal summary field names
instead of the public POSIX response's `affected` field. It was corrected and
tested against both real responses and no-op fakes. An initial long-run probe
setup was deliberately stopped after 538 acknowledgements to strengthen that
verification; its operator-stop report is retained, not an engine failure.

The corrected small scale check passed again with normal background healing
enabled and exact affected-count assertions. Sixteen Python harness tests and
nine Rust proof-tool unit tests pass. Tests include real multipart framing,
partial-success preservation, no-op rejection, mount loss, bounded client
submission and terminal-result gating.
Fixture TLS trust also outlives the full soak deadline; certificate validation
is not disabled to accommodate a longer run. Earlier operator-stopped fixtures
and their in-flight disconnects are retained separately from completed gates.

The original million-file run failed after 9 h 15 min on a 300-second batch
response timeout. It acknowledged 111,904 source objects; the last completed
integrity checkpoint downloaded and hashed all 100,256 versions at that point.
Two timed-out batches (128 source keys) have unresolved outcomes; they were not
blindly retried. Only the 1,024 preflight namespace mutations completed, not the
million-mutation target. No million-object PASS follows from this run.

Each accepted version is recorded in an
on-disk ledger and fully downloaded/hashed at population checkpoints and after
final restart. Listing probes cover selected prefixes; a separate complete
streamed inventory checks all live names and maximum-ULID versions. This is not
a completed million-object result or a Console folder-drop qualification.

## RC.1 declaration verification — October 18, 2026

After the numbering reset, the locked, offline, warnings-denied workspace/
all-targets suite passed serially and in parallel: 329 engine, two EC, two
release-reporting, nine proof-CLI, 12 Shell unit and nine Shell integration
tests (363 distinct Rust tests). All six Python fixture-safety tests passed.
The new checks exercise real engine/backup/Shell binaries; version reporting
does not initialize storage or require network credentials. Product version,
storage specification, unchanged encoding 4 and existing wire versions are
checked separately.

The locked, offline, warnings-denied optimized workspace/binary build passed.
Engine, Shell, backup helper and proof tool all reported `1.0.0-RC.1`. A fresh
isolated USB-HDD/NVMe EC 1+1 native-protocol smoke passed on the rebuilt engine,
including object/version readback. Existing deployed processes were unchanged.
RC.1 optimized Engine SHA-256:
`2b1623ff6abd25dac592780ba41b23d2d446014e95ce11076f0d48b40eed0485`.

The declaration changes version metadata, reporting and documentation, not
NCX/NDX/payload encoding or mutation/durability rules. The hardware matrix below
retains its actual pre-reset build checksum and limitations. Its results do not
claim a fresh physical power cut or long soak of a renamed/rebuilt binary.

## RC concurrency investigation — October 17–18, 2026

The locked workspace/all-targets suite passed both serially and in parallel:
329 engine, two EC, nine proof-CLI, 12 Shell unit and eight Shell integration
tests (360 distinct tests). Six Python fixture-safety tests also passed. Four new
regressions were reproduced failing before their fixes: small-upload admission,
saturated/cancelled admission with capacity reuse,
blocking boundary admission with exhausted Tokio budget, and unnecessary NCX
inode replacement. The latter also checks missing/corrupt replica repair without
replacing the healthy copy.

Fixtures use an isolated EC 1+1 pool on a USB rotational HDD and the internal
NVMe, with loopback HTTPS/native traffic. The Python stress tools verify their
fixture CA; the older Rust proof client's lab connections disable certificate
validation. Online backup separately exercises source CA validation.
Existing services and enrolled roots are untouched. The host also ran ordinary
applications; these are not uncontended throughput benchmarks or four-drive,
LAN, physical power-cut or long-soak qualifications.

Failures retained during iteration:

- Baseline small writes bypassed configured admission. Extreme load produced
  120-second request deadlines; independent restart verification still hashed
  all 9,450 acknowledged versions and 1,015 additional unacknowledged versions.
  A timeout is not proof that a write did not commit.
- GC overlap stalled at its unchanged 120-second deadline. The synchronous
  backup-boundary adapter could not replenish Tokio's exhausted cooperative
  budget. Its focused regression failed before the fix; the unchanged GC proof
  then completed in 56.65 seconds.
- Admission-only iteration: all three mixed-load waves completed, but shared-key
  contention still produced 84 timeouts. All 2,404 acknowledged versions survived
  three inflight SIGKILL cycles and complete hash verification.
- The next iteration passed the complete 2,048-concurrent overload/crash gate:
  2,418 acknowledged versions, zero unexpected errors, paginated listing and
  absence of admission-refused unique writes verified. Shared-key latency still
  approached the 120-second client deadline.
- Holding payload permits through NCX publication reduced small-write throughput
  on this shared host; that boundary was withdrawn. Admission now precedes
  identity selection, and payload permits are released after EC/NDX work, with
  lifecycle/upload protection retained through logical publication.
- The 16-client, 64-object/client, 16 KiB batch workload exceeded its original
  60-second deadline in intermediate builds. It was progressing, not hung; the
  deadline has not been raised or the workload reduced.

Harness failures are not engine passes: a renamed binary lacked Shell's expected
sibling engine, and sequential copy/remove fault injection allowed the live
healer to reconstruct the supposedly unrecoverable object. The backup fixture
now uses the normal Engine/Shell pairing. Beyond-parity loss is injected cold,
then fail-safe entry and automatic recovery from only k restored pieces are
checked while running, with unchanged recovery deadlines.

The revised optimized candidate
(`1313b5034777749bb343b39a1684a30e79bd7c60ab908aae7639a33860e8b4f8`)
passed all eight primary hardware gates, without changing their original
workloads or acceptance deadlines:

| Gate | Result |
| --- | --- |
| Original 16-client batch, 1,024 × 16 KiB | PASS: 19.256 s, 53.177 objects/s; all requests completed in 17.275–19.240 s |
| Full concurrent upload/mutation/restart phase | PASS: 96.715 s; 1,968 expected live objects; sampled peak RSS 158,604 KiB / 149 FDs |
| GC overlap, original workload/deadlines | PASS: 56.132 s |
| Recover-on-read, healing and cold beyond-parity fail-safe/live recovery | PASS: 28.052 s |
| Native protocol operations | PASS: 5.511 s |
| HTTP/native 2 GiB interrupted multipart/resume | PASS: 183.321 s |
| Online committed-state backup | PASS: all 20 checks, 168.311 s; 132 restored objects/history/trash verified; pending uploads excluded |
| Strict upload ramps, namespace isolation and interrupted prefix copy/delete | PASS: 282.519 s; all 1,915 acknowledged versions hashed across restarts; 7,660 immutable files unchanged by NCX mutations |
| Extreme offered load and three inflight SIGKILL cycles | PASS: 321.826 s; 20,480 primary requests, up to 2,048 concurrent clients; all 2,229 acknowledged versions hashed after restart; zero unexpected errors |

Overload PASS means correct bounded admission and recovery, not acceptance of
every offered write. Normal waves returned explicit scheduler-admission 503s;
disconnects were expected only after deliberate SIGKILL. Accepted shared-key
writes reached p99 31.915 s / maximum 32.357 s, versus maximum 116.883 s in the
earlier passing iteration. Successful mixed-load read/LIST p95 was 1.701–1.822 s.
Peak sampled engine RSS across the primary matrix was 268,012 KiB (261.8 MiB),
with 2,081 FDs; post-crash quiescent listing sampled 16 FDs. Restarts mean these
numbers are not evidence of a long-duration leak-free soak.

The batch row retains the proof's original relaxed payload durability default;
NCX publication remains strict. A separate strict-default batch with the same
16 clients × 64 objects × 16 KiB passed full hash checks for all 1,024 versions
before and after SIGKILL. Its slowest batch took 117.899 s, with peak RSS
57,444 KiB / 140 FDs: it did not meet 60 seconds. This additional durability test
uses a separate 300-second client limit and does not replace or extend the
original acceptance gate. The larger strict batch (32 clients × 16 objects ×
1 MiB, 512 MiB total) also passed every version hash before and after SIGKILL;
its slowest batch took 97.515 s, with peak RSS 102,352 KiB / 140 FDs. Neither
strict case proves a sub-60-second batch SLA. Strict batch latency on this
rotational member remains a performance limitation; durability was not relaxed
to improve the result.

The corrected healing proof also passed three additional fresh-fixture repeats.
Online backup covered local HTTPS/native capture and local repository
replication/relocated restore; no new cross-host replication result is claimed.
No cache, version head, tombstone or storage-format change was introduced.

Reproduction tools: `tools/n1-rc-stress.py` and `tools/n1-rc-overload.py` accept
`--binary`, a new `--root`, and one `--member-parent` per EC member. Read `--help`
first. They bind loopback only, reject existing fixtures, enrolled ancestors and
same-filesystem members, monitor resource/free-space ceilings, and only stop
their own engine. They never connect to a deployed service. Run fixture-safety
checks with `python3 -m unittest discover -s tools -p test_n1_rc_stress.py`.

## HTTP rate-limit correction — October 15, 2026

All ten focused limiter regressions passed, followed by the full locked serial
workspace/all-targets suite: 325 engine, two EC, nine proof-CLI, 12 Shell unit
and eight Shell integration tests passed. Formatting and whitespace checks passed.
The old builder incorrectly passed
requests/second into a seconds/token API. Auth/view/upload/global now share one
reciprocal-period conversion, rounded up to nanoseconds, with unchanged burst
sizes and IP/fragment scopes. Enabled zero rates/bursts and rates exceeding the
nanosecond precision bound are rejected; explicit disabling still bypasses all
four layers.

Tests inspect the actual middleware-generated quotas, exercise default/custom
settings, advance a fake clock through burst exhaustion, just-before/exact refill
and idle saturation, and check HTTP 429 plus independent IP budgets. Earlier
large-burst hardware proofs did not qualify replenishment semantics. This fix
does not change storage, native admission or disk formats.

### MCH limiter deployment — October 15, 2026

The optimized engine was checksum-verified and deployed with a brief engine/
operator restart. An isolated fragment reproduced the old behavior: after
exhausting its 30 requests/second, 100-token view budget, all three requests after
a 167 ms wait still returned 429. On the updated process, the same exhaustion
and wait were followed by three successful 200 responses. The refill check ran
locally against the real MCH HTTPS service, not an API mock or LAN latency test.

Both object versions committed before restart retained their source SHA-256
hashes through cache-bypassed reads afterward. Existing backup points, pool
identity descriptors, configuration, credentials and certificates were unchanged;
all four members were ready. The test fragment alone was removed. Shell's
authenticated operator and scheduled-backup protection checks passed. Console
and Accel retained their PIDs. Separate-host LAN checks passed for HTTPS/native
ping, Console health and its N1 proxy, with normal CA verification.

Deployed Engine SHA-256:
`ed9b24ba787aa28de0616688fc01630afa9c6e2ce7844d9ffce6c130e3028620`.
No new physical power-loss or long-soak qualification is claimed.

## Online committed-state backup — October 14, 2026

The final locked release passed all 315 engine library tests, two EC tests,
nine proof-CLI tests, 12 Shell unit tests and eight Shell integration tests.
Formatting and whitespace checks passed. Console's production build/type checks
and ten backup UI checks across five viewports passed separately.

On MCH's four distinct SATA/XFS drives, EC 3+1, all 20 online recovery checks
passed. The engine stayed running during capture. Rename, new upload/version,
trash recovery and whole-fragment deletion after pinning did not alter the
captured point. The restored point retained committed versions and trash but
excluded acknowledged parts of both new-key and existing-key unfinished durable
uploads, plus post-selection objects. Original roots were unavailable and
restored members were relocated/reversed; 132 object checks and all hashes,
listing and history passed.

Actual engine SIGKILL during another capture exposed no completed partial point.
Startup cleaned its owned scratch view and retained the last completed snapshot.
A scheduled online capture with a deliberately nonexistent service name passed
capture, verification and replication without a service-state transition.
Separately, real systemd supervision killed both controller processes during
capture: N1's PID stayed unchanged, recovery reported interruption rather than
success, and retry completed verified replication with healthy protection status.

All eight sender/receiver SIGKILL-and-retry cases passed on HTTP, native, HTTPS
and TLS-native. Fresh replication across two physical hosts also passed on all
four protocols, including full verification, repeat transfer and wrong-token
refusal. Native backup traffic did not use SSH. HTTPS/TLS-native used explicit
CA validation, not an insecure TLS bypass.

The real production-built Console submitted online capture, verification, both
secure replication modes and four-member restore over LAN. All 808 restored
NCX/payload files matched the source inventory. No operator token was stored in
browser storage and no JavaScript execution errors were observed.

Bounded observations under concurrent test load:

| Observation | Result |
| --- | ---: |
| Captured view | 3,870 records / 97,924,820 bytes |
| First / second mutation-pinning boundary | 51 / 31 ms |
| Four mutations and fragment deletion after pinning, combined | 367 ms |
| Entire 20-check recovery drill | 279.42 s |
| Fresh LAN replication: HTTP / native / HTTPS / TLS-native | 8.41 / 14.06 / 16.78 / 16.83 s |
| Sampled engine/backup-process peak RSS / FD count | 145,344 KiB / 87 |

The workload included a 64 MiB object, 128 small objects, history, trash and
unfinished uploads. The timings are not universal latency or maximum-throughput
claims; the combined mutation time is not a single-object benchmark. No new
physical power cut or long memory/descriptor soak was performed for this change.

Failures found and fixed before the final reruns: source HTTPS CA setup could
panic without explicit rustls provider selection; native engine control replies
were streamed MessagePack rather than inline JSON; and the native-TLS bridge
needed to flush pending TLS writes before waiting for protocol acknowledgements.
The last issue reproduced on the real LAN, not only loopback. Regressions cover
the codec, completed-capture acknowledgement and flush behavior. Harness-only
failures included a malformed synthetic version filename, missing secure fixture
certificates and a controller test that launched before the native listener was
ready. These failed runs are not counted as passes.

This candidate's Engine / Shell SHA-256:

- `6e844b9fd3cd8369f405593fc4526862f85fbee9897b6bf125ffcd1dc53fa95e`
- `1bfb6205cf60f1d6a17173216b1e57d3c000496ae054180c545a5dd797c5589b`

### MCH activation

Fresh N1, Accel, Console and Shell operator services were installed after the
above gates passed. Fifteen deployment checks passed: trusted-CA engine/Console
proxy access, Office isolation headers, four-member readiness, two immutable
versions, exact historical hashes, cache cold/warm/range delivery, rename,
trash/recovery and verified scheduled online capture with unchanged engine PID.
The deployment's completed recovery point then replicated and independently
verified over HTTPS and TLS-native to another physical host. A separate LAN
browser/native smoke passed with no Console JavaScript errors.

The daily online timer keeps seven verified points in a repository on the OS
NVMe, separate from the four data HDDs. No permanent remote peer was configured;
same-host backup is not off-host/site protection. Obsolete N1 binaries, configs,
soaks and disposable test storage were removed; unrelated registries and service
data were preserved. Keys/configuration remain separately protected operator
responsibilities. Deployment acceptance is not an LTS declaration.

## MCH four-drive and real LAN verification — October 14, 2026

Fresh locked release binaries were transferred and their SHA-256 checked on
MCH. With explicit disposable-server authorization, the old N1 instances were
stopped and their identified storage data purged. The OS and unrelated service
data were not formatted. All new fixtures used dedicated directories.

- Four separate SATA/XFS drives, EC 3+1: the same 21 recovery checks passed
  independently over HTTP, native, HTTPS and TLS-native. This includes actual
  restore-process SIGKILL after a piece appears, activation-fence refusal,
  explicit resume, remapped roots, version history, listing, full downloads,
  trash, durable-upload continuation and retention/GC safety. These are 21
  overlapping checks across four transports, not 84 distinct failure modes.
- Eight actual sender/receiver process-tree SIGKILL cases passed: both endpoints
  independently on all four transports, after two committed destination blocks.
  No completed snapshot was exposed mid-transfer. Retry verified the complete
  snapshot and preserved the accepted blocks' modification times. Source and
  receiver repositories were on different physical drives.
- Separate-host LAN replication passed on all four transports, with destination
  verification, repeat transfer and wrong-token refusal. Secure transfers used
  a CA-issued leaf matching the LAN IP, without disabling TLS verification.
  The receiver used isolated NVMe storage on the other host. SSH administered
  MCH only; it carried no backup traffic and is not a product dependency.
- Thirteen pool-relocation checks passed, including a verified HDD-to-NVMe
  member clone, unavailable original locations, reversed root discovery,
  wrong-identity/incomplete-copy refusal, interrupted replacement/resume,
  durable continuation, batch/native uploads, recover-on-read and automatic
  healing. Committed NDX/payload digests were unchanged.
- The production-built Console connected directly over LAN to MCH's secured
  operator endpoint. Initialization, capture, verification, HTTPS/native-TLS
  replication and four-member restore passed without API mocks. All 812
  inventoried NCX/payload files matched the source hashes. The operator token
  was absent from browser storage. Browser trust was scoped to the test leaf;
  the N1 replication clients still validated the CA normally.
- Real systemd user-service supervision passed: SIGKILL of all three operator/
  Shell/capture processes after persisted restart intent and engine stop caused
  supervision to restart N1 and mark the job interrupted, not successful. A
  subsequent capture, owned HTTP replication, independent repository verification,
  retention and healthy protection status passed. Fixture supervisors were
  stopped before their recovered engine and all fixture unit links removed.

Bounded, shared-drive observations; restore timings include deliberate
interruption, refused activation and resumed completion:

| Transport | Capture | Incremental capture | Initial replication | Interrupted/resumed restore |
| --- | ---: | ---: | ---: | ---: |
| HTTP | 25.72 s | 4.42 s | 39.94 s | 207.21 s |
| Native | 26.01 s | 4.45 s | 40.97 s | 86.37 s |
| HTTPS | 25.03 s | 4.55 s | 74.66 s | 134.42 s |
| TLS-native | 33.65 s | 4.63 s | 75.36 s | 87.85 s |

The source fixture was 64 MiB plus 128 small objects and recovery state. The
first drill overlapped old-data deletion; later drills overlapped other isolated
fault tests. These are not controlled throughput comparisons. Sampled
engine/backup process peaks were at most 100,592 KiB RSS and 94 descriptors,
observed separately. Console capture/verify/secure replication/restore took
32.38/1.11/41.42 and 46.52/104.96 seconds. No new physical mains-power cut or
long memory/descriptor soak is claimed by this run.

Past harness failures retained: the LAN driver initially tried to parse the
CLI's text acknowledgement as JSON; transfer itself succeeded, and corrected
verification/repeat checks passed on rerun. MCH's user manager rejected an
auxiliary group-kill request. The service drill now kills the owned cgroup's
actual process tree. Its cleanup ordering was also corrected so recovery cannot
restart a test engine after that engine's unit is unlinked; N1's instance lease
correctly refused the resulting accidental second writer.

Candidate SHA-256:

- Engine: `d3027d8a9d7ec0902ac1beb53b95312e9982f1003b12acb99ce7af34f32713f7`
- Shell: `3fba0dd23b0f6d6c4bd6e9e96663b4aba7214560227a3dcbbd9138e2b8aebe24`

## Standalone backup transport and Console operations — October 14, 2026

Remote snapshot replication now uses N1-owned HTTP(S) or native N1(S).
There is no SSH process/daemon requirement. The matching Shell delegates to
the engine's built-in operator endpoint and existing repository implementation.
Backup format, ordinary NCX mutation and committed-source NDX/payload semantics
are unchanged. These results do not reuse SSH passes as native qualification.

- Final library regression: 307 passed, zero failures; the seven network tests
  used isolated directories on a real USB-attached XFS HDD. Other library
  fixtures used local temporary storage. EC compatibility: two passed.
  Shell/CLI: 12 unit and eight integration tests passed. Locked release builds
  and Console's production build/type checks passed.
- Real N1-object recovery corpus: all 21 end-to-end checks passed separately
  over HTTP, native, HTTPS and TLS-native transfer. Each run used 1+0 storage,
  a 64 MiB multipart source and 128 small objects, plus independent fragment
  credentials, versions and trash. Checks included active-engine capture refusal,
  incremental capture, repeat replication, original source paths unavailable,
  SIGKILL after actual restore-piece publication, fenced startup refusal,
  explicit resume, listing/history, full hashes, ranges, durable continuation,
  new writes/deletion and independent retention. Protocol counts overlap; they
  are not 84 unique failure modes.
- Network fault tests rejected wrong operator tokens/origins/repository aliases,
  wrong encryption keys and corrupt blocks. Truncated network streams published
  no snapshot; retries reused verified blocks without changing their mtimes.
- Final optimized binaries passed four actual process-SIGKILL cases: sender and
  receiver independently, over HTTP and native, after two committed destination
  blocks. No completed point was exposed before retry. Retry and complete
  snapshot verification passed, preserving accepted block mtimes.
- Real browser → real secured operator API: repository initialization, offline
  capture, verification, HTTPS/native-TLS replication and restore passed. All
  272 inventoried payload/NCX files matched the source hashes. Dedicated operator
  credentials were absent from browser storage. UI mock regressions separately
  passed ten checks across five viewports, plus the desktop pool-health checks.

Bounded USB observations (debug recovery matrix, variable concurrent host load):

| Transport | Whole recovery drill | Initial capture | Incremental capture | Initial replication |
| --- | ---: | ---: | ---: | ---: |
| HTTP | 200.74 s | 28.28 s | 5.67 s | 40.79 s |
| Native | 284.95 s | 37.86 s | 8.44 s | 88.77 s |
| HTTPS | 289.15 s | 37.62 s | 10.87 s | 87.57 s |
| TLS-native | 200.71 s | 32.02 s | 6.99 s | 68.18 s |

The real Console API sample took 45.49 s capture, 3.11 s verify, 95.95/91.91 s
replication and 56.61 s restore. Sampled operator peak RSS was 91,300 KiB and
33 descriptors. Secure matrix descendants peaked at 118,536 KiB and 76
descriptors across engine/backup phases; these are sampled process peaks,
not a long memory-leak soak or bulk-media bandwidth claim.

Past failures retained: a native bridge initially borrowed sockets, so failed
bridging could leave the blocking sender waiting for its inactivity timeout.
Owning/closing endpoints fixed it; the original interruption deadline and hash/
mtime assertions were retained and the full regression rerun passed. A TLS
fixture incorrectly used a CA certificate as a server leaf and was correctly
rejected; it now uses a CA-issued non-CA leaf, without disabling verification.
The hard-kill harness initially counted transient atomic-write files; its gate
now selects committed SHA-256 block filenames. Immediate retries can encounter
a receiver still unwinding fsync; admission now reports repository unavailability
before accepting a stream, and bounded retries preserve its lease instead of
bypassing it.

Scope: isolated directories on one USB/XFS device and loopback networking.
Existing user data/services were not formatted, stopped or replaced. This run
does not requalify multi-device EC failures, separate-host LAN transport, physical
power loss or the retired server. Historical results below remain historical.

## Console Office upload integration — October 13, 2026

All 300 engine library tests pass. Real USB/XFS 1+0 verification confirms that
standard and durable `fresh=true` initiation allocate independent sessions for
identical sizing, while default resume discovery remains unchanged.

The real browser Office adapter created a native N1 DOCX version and reconciled
a lost finalize acknowledgement to that exact version without another upload.
The original bytes remained readable; 42 previously committed NDX/payload files
retained their SHA-256 and modification times. Accel served a pinned-version HIT.
No storage format, mutable version head, business database or NCX mutation
semantics were added. The Console's native spreadsheet input failure is recorded
in its testing document; it is not a failure of N1's committed-byte verification.
This localhost 1+0 run does not test redundancy or physical power loss.

## Optional Accel read integration — October 13, 2026

All 299 engine unit tests pass, including private read-proxy authority, range
forwarding, response validation and pre-header fallback. The release engine and
Accel also passed an isolated USB/XFS 1+0 proof using an 84 MiB AVI and PNG:
full/range/image hashes matched, cached rename reads fetched no additional
payload, the old key returned 404, and committed object NDX/payload bytes and
modification times stayed unchanged. N1 still read the correct USB-backed range
after Accel stopped. Original drive contents were not formatted or removed.

This adds optional HTTP delivery only, not an object cache inside N1 or storage
format changes. HTTP accepted sockets enable TCP_NODELAY. Cache activation,
budgets and bounded measurements are documented in the Accel repository's
TESTING.md and [integration guide](DOCUMENTATION.md#accel-integration). The proof
used loopback networking and does not qualify redundant EC recovery or an LTS
release. Earlier candidate hashes below identify their historical builds, not
this newer binary.

## Production-hardening candidate — October 12, 2026

Engine SHA-256:
fb4f46fe47adf90c0f62b4be8f0e43cc5dd4574be9249ea5e0a29e254b8456d8.
Shell SHA-256:
2b6a6f84a98338775642aede5338323be5531a62bee6e747ca03831bf6cb52d7.

Local candidate validation: 297 engine, 2 EC integration, 9 proof-runner,
12 Shell and 8 CLI tests passed (328 total) in warning-denied parallel execution.
Before the extra proof assertion, two 327-test parallel runs also passed.
Locked release Engine/Shell/helper/proof builds, warning-denied
all-target checking, formatting and whitespace checks passed.

New checks cover strict directory-sync error propagation, standard part retry/
finalization durability, independent HTTP/native mutation permissions,
operator-only/global/cross-fragment maintenance, rotation-bound refresh,
oversized JSON/part-count/slot admission, native raw-body rejection before
allocation, WebSocket read authority and expiry, inode warnings, TLS defaults,
and corrupt backup/replica rejection without replacing the last valid snapshot.
Console pool regressions passed 10 cases across five viewports; the full desktop
suite passed 51 cases with 5 unavailable/inapplicable cases skipped.

Failures retained: an HTTP POSIX permission test initially exposed a second
handler missing its checks; both HTTP and native paths now enforce the same
permissions. A body-limit fixture initially used a nonexistent fragment and
hit the existing upload existence gate before JSON extraction; a real isolated
fragment now exercises the limit without weakening the 413 assertion.
Parallel positive backup captures exposed transient fork-inherited CLOEXEC
leases again. Their fixture-only bounded acquisition helper now covers positive
captures while active-engine/overlap rejection tests remain immediate and
production locking is unchanged. Both final parallel reruns passed.

The main-server matrix runs headlessly on isolated roots on four XFS HDDs.
Accepted passes so far: NCX-only copy/delete, root relocation/reordering,
3+1 data/parity member replacement, 2+1 replacement, 1+0 migration, encrypted
backup/replication/interrupted restore in 3+1 and 1+0 layouts, pool expansion,
and 2 GiB durable multipart restart/resume. These do not certify a deployment.

Direct workstation-to-server LAN HTTPS passed restricted mutation/global-job
denials, huge-part-count refusal, 128-key pagination, repeated NCX mutation and
all final payload hashes. The three live Console tests passed against that
server: prefix rename preserving a same-named object, TAR download, multipart
resume and standard batch routing. The initial LAN latency sample ran during
simultaneous disk-intensive restore; it is a contention result, not a quiet
baseline. Quiet performance qualification is still pending.

Three initial hardware runner failures retained: the healing case expected
reconfiguration of enrolled EC geometry; native and NOA cases expected empty
mkdir. N1 correctly rejected those pre-v4 assumptions. The runner now asserts
enrolled-geometry rejection and uses upload-backed virtual prefixes. Its typed
native-error assertions require both the exact status and `ok=false`.
The corrected healing/native reruns and the final NOA rerun subsequently passed.
The last NOA failure was another pre-v4 parent-folder STAT assumption; the test
now verifies the actual trashed child binding before recovery. All 14 selected
hardware cases have accepted passes on the same candidate Engine/Shell binaries;
the initial failures above are retained. Five-second fixture-process sampling
observed approximately 174.3 MiB peak RSS and 88 peak file descriptors across
the matrix (including offline helpers); this is not a long-soak guarantee.

### Physical power-cut recovery — October 12, 2026

PASS after retry on the same candidate binaries. The operator physically cut
power during repeated 128-key prefix moves; the independent LAN client recorded
an interrupted request. After power restoration, all four XFS members replayed
their logs and the isolated N1 instance started automatically.

| Check | Result |
| --- | --- |
| Immediate committed-object readback | All 128 moved objects and the single-object baseline were reachable with matching SHA-256 hashes |
| Strict standard multipart | Finalized 16 MiB object retained its expected hash |
| Durable multipart | Both previously acknowledged 4 MiB parts survived; remaining parts uploaded and final 16 MiB hash matched |
| Interrupted namespace | 16 objects had both names; complete, ordered pagination returned 144 bindings for 128 objects |
| First offline integrity check | REFUSED: 13 incomplete NCX replica pairs; no committed NDX replica was missing. Automatic healing was disabled in this fixture |
| Retry of interrupted move | PASS: 54 remaining source bindings moved without conflicts; all 128 destination objects hash-correct, old names absent |
| Mutation-only immutable inventory | All 11,088 files under object storage had identical inventories/SHA-256 hashes before and after retry; this baseline was taken after the power cut |
| Final complete offline pool verification | PASS, exit 0: 1,381 versions and 2,700 physical NCX replicas checked, recorded payload-piece checksums verified, zero reconstructed shards |

The initial replica failure is retained, not relabeled as an immediate healthy
pool. Copy/delete can leave duplicate names or incomplete replica pairs when
interrupted; ordinary retry completed this operation without engine changes,
tombstones or a journal. Test-only boot automation was removed afterward;
existing production services and storage were not replaced.

This is one physical power-cut recovery drill on a four-HDD 3+1 deployment,
not every fsync stage, storage controller or failure topology. Broader power-cut
coverage and a longer soak of this exact candidate remain release gates.

## Earlier pool health, Shell and Console integration — October 12, 2026

Engine SHA-256:
a183c39818399637e16a2aec704e486a0620627db7d6f7b2e36343259be926ae.
Shell SHA-256:
8608c21e150bcf76f070353511534db422b4f9b910c9bb8007ce2ddd4a15e90f.

| Gate | Result |
| --- | --- |
| Workspace/all-target tests | PASS: 276 engine, 2 EC integration, 8 proof-runner, 11 Shell, 8 CLI; 305 total, including a repeated parallel run |
| Release build, all-target check, formatting and whitespace | PASS |
| Console integration/responsive regression subset | PASS: 84 cases; 121 viewport-inapplicable cases skipped, not counted as passes |
| Complete desktop Console suite | PASS on final rerun: 51 cases, 5 unavailable/inapplicable cases skipped |
| New pool UI checks | PASS across five desktop/tablet/phone viewports; explicit selection, deep scan, member/fragment identity, failure clearing and offline workflows |
| Mixed-pool real-device expansion | PASS on isolated MCH fixtures: A 1+0 plus B 2+1; existing fragment credentials enrolled, old committed bytes retained, new allocation and restart verified |
| Existing key after expansion | PASS: newer version placed in B, NCX remained in A, history spanned pools; A's namespace check passed with its old object directory temporarily unavailable and the newer version in B |
| HTTPS/native pool diagnostics | PASS: independent pool selection, correct replica counts and current allocation pool; no authentication, no read permission and cross-fragment content requests refused |
| Failure isolation | PASS: fenced B member reported degraded/unavailable; A still verified, and the diagnostic did not clear the fence |
| Content integrity vs readiness | PASS: same-sized parity corruption left readiness ready but failed CRC verification; no automatic repair or committed-byte rewrite by the diagnostic |
| Offline Shell selection | PASS: complete A verification while a B member was fenced; full verification passed afterward (3 versions, 3 physical NCX replicas) |
| Encrypted mixed-pool backup | PASS: offline Shell capture and authenticated snapshot verification; 107 records, approximately 4 MiB source bytes |
| Backup cross-pool reference regression | PASS: NCX in A with remaining metadata/payload only in B verified without global engine initialization; corrupt pieces and dangling references refused |

Only new private fixtures on MCH's four mounted XFS HDDs were used. Existing
production services stayed active; production data/configuration/binaries were
not changed. No formatting, remounting or production deployment occurred.
Server checks used loopback HTTPS/native ports, bounded CPU/memory and idle I/O
priority; they do not establish LAN latency or full-drive throughput.

Failures retained: the first native run exposed a u128 elapsed-time field's
binary MessagePack encoding; u64 and a serialization regression fixed it before
the complete rerun passed. Inspection also found a pool-local dangling-reference
assumption; the verifier now recognizes stable-object version history across
known pools, including the offline backup catalog. Two parallel backup unit
tests encountered transient flock inheritance during other tests' fork/exec
windows. Their positive fixture-reopen helper now waits boundedly; production
locks and immediate overlapping-run refusal assertions are unchanged, and the
parallel rerun passed. The extra backup script initially read `id` instead of
the capture result's `snapshot`; capture succeeded and the corrected verification
passed. Initial development checks also caught Shell query tuple types and an
undeclared proof-only walker; both were corrected before hardware validation.
The broader desktop Console run initially hit an async evaluation-context error
in its NOA-shell test and a window-animation timing assertion. The shell test
now submits through the real command input and asserts output/path state;
the geometry test polls the same size bounds instead of sleeping a fixed delay.
No production shell/window behavior was changed; the complete desktop rerun
passed without weakening command-routing or geometry assertions.

Quick identity/access readiness is not full content integrity. Online scans are
read-only live observations, not atomic snapshots; concurrent mutations can
invalidate them. Complete offline verification remains the maintenance gate.
Console presents diagnostics and host-side Shell workflows, not browser-executed
physical migration/backup automation. This is not a new long-soak or power-cut
qualification.

## Self-identifying roots and location independence — October 12, 2026

Final workspace-build engine SHA-256:
2313745a4feac9db93992494ccafac10707475255ada3f8027b17b16e536441b.
Matching Shell SHA-256:
ce520565709bd605000c494a24060cfeecea0c043df3444e0e89b734418f3054.

MCH was used only through new private fixtures on its four existing XFS HDDs
and NVMe filesystem. No production contents/configuration/binaries were changed,
no existing service was stopped, and no device was formatted or remounted.
Tests used loopback HTTPS/native ports and temporary self-signed certificates,
with a two-core CPU quota, 1.5 GiB cgroup limit and idle I/O priority. Physical
I/O remained shared with production. These are not client-LAN latency results.

| Gate | Result |
| --- | --- |
| Workspace/all-target regression tests | PASS: 271 engine, 2 EC integration, 8 proof-runner, 11 Shell, 7 CLI; 299 total |
| Warnings-denied check, formatting and whitespace | PASS |
| Existing path-bound v4 data | PASS: seeded with the previous engine, descriptor upgrade preserved committed bytes |
| 3+1 data and parity members | PASS: verified HDD-to-NVMe cloning, different root locations, reversed discovery order, original paths unavailable |
| Cold degraded restart after relocation | PASS: readback/listing remained available within parity limits; missing directories were not recreated |
| Wrong identity and unfinished copy | Rejected; marker recognition never bypassed the activation fence |
| Replacement at a new location | PASS for data/parity: SIGKILL after actual piece publication, startup refusal, explicit resume and full verification |
| Relocated operation | PASS: recursive paginated listing, versions, trash recovery, durable upload continuation, batch/native uploads, new write/rename |
| Recover-on-read and autonomous healing | PASS on relocated 3+1 storage: removed data piece recovered on read; removed parity piece healed by the background controller |
| Healthy 1+0 relocation | PASS: verified cross-device clone and new-location operation; lost-member reconstruction correctly refused |
| Committed immutability | Complete NDX/data/parity inventories and SHA-256 hashes unchanged through upgrade, relocation and reconstruction |
| Encrypted backup/portable restore | PASS: capture, incremental capture, replication, original paths hidden, restore interruption after 3,920 files, fenced refusal/resume, hashes, credentials, versions, trash, durable continuation, ranges and retention |
| Backup after legacy-data relocation | PASS: full capture and authenticated verification of the relocated mixed legacy/logical-address corpus |

The final five-mode matrix completed in 5m 04s, exit 0. Its 329.3 MiB peak
is whole-cgroup accounting, not engine RSS. Small-fixture data/parity rebuilds
observed 4.18/4.03 s and 19.8/21.5 MiB peak backend RSS, ending with six FDs.
Clones observed approximately 0.26 s, 8.5–8.8 MiB RSS and four final FDs;
these are not full-drive bandwidth claims. Backup phases observed 25.29 s
capture, 4.25 s incremental capture, 45.85 s replication and 83.04 s interrupted
restore/refusal/resume.

Matched four-HDD upload comparison against the pre-portability build used five
samples per transport/multipart case: 1 MiB one-shot, 8 MiB multipart with 2 MiB
parts, and four concurrent 64-object batches of 16 KiB objects. Median upload
rates are shown below; batch rate covers the entire four-client request group.

| Measure | Before | Self-identifying roots |
| --- | ---: | ---: |
| HTTPS one-shot | 11.08 MiB/s | 11.15 MiB/s |
| HTTPS sequential multipart | 32.09 MiB/s | 40.69 MiB/s |
| Native one-shot | 10.71 MiB/s | 17.18 MiB/s |
| Native sequential multipart | 7.04 MiB/s | 7.37 MiB/s |
| Native pipelined multipart | 7.14 MiB/s | 8.36 MiB/s |
| Concurrent batch | 28.90 objects/s | 29.75 objects/s |
| Maximum sampled engine RSS | 80.4 MiB | 81.5 MiB |

No consistent throughput regression was observed. Shared-I/O variance is large:
the candidate's native sequential mean was 3.9% lower despite its higher median.
Neither the speedups nor the roughly 1.1 MiB sampled RSS difference establish
universal throughput or memory behavior. No object cache was added.

Failed attempts remain evidence: the first relocation invocation exposed an
obsolete proof-argument dependency; the first new-location replacement safely
refused an old path-keyed authorization lookup. That lookup was removed and the
descriptor was included in the replacement activation inventory before the final
matrix. A reused fixture directory caused another refusal; unique replacement
names fixed the harness without overwriting prior evidence. A diagnostic started
before its binary transfer finished returned ETXTBSY; it performed no repair.
An extra backup-verification script initially used an unsupported `--all` flag;
capture completed, and verification passed when given its explicit snapshot ID.
Physical power cuts, hot relocation and multiple simultaneous member replacement
are not certified by these tests. Production deployment was not performed.

## Pool portability and integrated backups — October 12, 2026

Final engine SHA-256:
c96bcf269cedcc0a5a1b8ec4c083c7660b5d86fed4815a626993d78cdbb7089e.
Matching Shell SHA-256:
ce520565709bd605000c494a24060cfeecea0c043df3444e0e89b734418f3054.

The main server was used as disposable test hardware: four independent XFS
HDDs, with an independent NVMe filesystem also available as a replacement
target. Tests used isolated roots and server-local HTTPS without Accel;
the harness accepts its temporary self-signed certificates. Physical I/O was
shared between test jobs; production services were restarted before the extra
cross-device restore drill. Existing production binaries were not replaced.

| Gate | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 263 engine, 2 EC integration, 7 proof-runner, 11 shell, 7 CLI; 290 total |
| Warnings-denied all-target check, formatting and whitespace | PASS |
| 3+1 data-member and parity-member replacement | PASS: blank replacement on a different filesystem, SIGKILL after actual piece publication, startup rejection, explicit resume and verified activation |
| 2+1 replacement onto another HDD | PASS: interrupted reconstruction/resume and full integrity checks |
| Healthy 1+0 cloning onto another HDD | PASS: interrupted copy/resume, same-path cutover, complete readback and new operations |
| Lost 1+0 reconstruction | Correctly refused; requires a verified backup, not an invented healthy member |
| Copy and replica validation | Partial copies, same-size payload corruption, divergent durable manifests and missing receipt replicas rejected |
| All four pool cases | PASS: 133 final visible objects each, recursive 17-entry pagination, version history, empty object, trash recovery, durable 64 MiB upload continuation and new upload/rename |
| Immutability | Complete committed NDX/data/parity inventories and SHA-256 hashes preserved through clone/reconstruction; healthy source copying did not recode pieces |
| Portable encrypted restore, 3+1 and 1+0 | PASS: original source paths hidden, 3+1 target member paths deliberately reversed, restore SIGKILL after piece publication, fenced startup refusal and explicit resume; all baseline hashes, versions, separate fragment credentials, trash, durable uploads, range reads, new writes and retained snapshots verified |
| 1+0 migration onto another HDD through backup | PASS: replica and restored member on an independent device, original paths unavailable, restore SIGKILL/retry and all end-to-end checks; 5m 24s overall, exit 0 |
| Integrated LAN SSH replication | PASS: sender SIGKILL after five destination blocks; no completed snapshot published; retry, authenticated verification and repeat transfer passed |
| Integrated scheduled backup | PASS: real systemd stop/capture/restart, verification, replication and source/destination retention through Shell |
| Whole backup-job interruption | PASS: whole-cgroup SIGKILL after 24 capture pins; supervised recovery restarted the test engine, cleared restart intent and recorded interruption rather than success; subsequent full pool verification passed |

Small-fixture offline measurements below include explicit resume after a
deliberate interruption. They are not full-drive throughput or online mutation
benchmarks; device choice and concurrent I/O explain why they cannot be compared
as an architecture speedup.

| Operation / target | Completion time | Peak backend RSS |
| --- | ---: | ---: |
| 3+1 data-member clone / NVMe | 0.92 s | 12.0 MiB |
| 3+1 data-member rebuild / NVMe | 2.56 s | 22.7 MiB |
| 3+1 parity-member rebuild / NVMe | 2.48 s | 24.5 MiB |
| 2+1 data-member rebuild / HDD | 35.47 s | 23.6 MiB |
| 1+0 clone / HDD, concurrent backup traffic | 108.01 s | 11.7 MiB |

Offline verify/clone processes ended with four open FDs; rebuild processes
ended with six. An earlier isolated 1+0 clone observed 10.31 s; it does not
erase the slower shared-I/O result. Process RSS is distinct from systemd's
whole-cgroup memory peak.

The 3+1 snapshot observed 54.46 s capture, 14.22 s incremental capture,
52.97 s initial replication and 243.75 s interrupted restore/rejection/resume.
The 1+0 snapshot observed 40.41/11.36/85.72/65.31 s respectively. These are
small-fixture wall times, not bulk-media bandwidth or instantaneous-backup
claims. The six-mode final hardware matrix completed in 16m 51s, exit 0;
its 1011.3 MiB peak is the whole cgroup including successive test processes
and kernel accounting, not engine RSS.
The separate cross-device 1+0 drill observed 43.35 s capture, 9.48 s incremental
capture, 121.67 s initial replication and 20.78 s interrupted restore/resume.
Its 233.1 MiB memory peak is likewise whole-cgroup accounting.

Failures were retained, not counted as passes: the first hardware harness
incorrectly compared non-recursive listing with nested keys; another omitted
Content-Length on an empty upload. Corrected reruns preserved those checks.
The first real offline reconstruction exposed uninitialized repair-reader
topology; it stayed fenced and was fixed before the complete matrix above.
The scheduled-backup fixture initially exposed a stale pre-cutover member
instead of the activated replacement. Capture refused it and restarted N1;
binding the verified replacement restored full pool integrity before retry.
An SSH fixture with non-private repository permissions was also refused before
the corrected owner-only run. The extra cross-device backup run initially
stopped at argument parsing because the spare flag still required the pool-only
mode; a parser regression now covers independent-spare backup selection.
Local regressions cover integrated job child
arguments, logical slot mapping and missing/divergent upload replicas.

These are process interruption and real cross-filesystem cutover tests, not
physical power cuts, physically unplugged members, a long memory soak or
multi-node automatic failover. Replacement currently handles one missing
member with all other peers verified; continuous writable standby replication
is not implemented.

## Headless release-gate soak — October 8, 2026

**FAIL after 6h 1m 41s; six-hour continuous-PID resource gate PASS.** The isolated
instance ran from 19:03 Manila on October 8 to 01:05 on October 9, stopping
before its eight-hour limit. Existing N1 and user storage were not replaced. Test
directories are isolated on four independent XFS members, with 3+1 EC and
loopback-only verified HTTPS; physical I/O is shared.

Engine SHA-256:
7fedadeb07086ba965c8ceeb68099a16d9f54fbf2674e29c00dd20af542851f1,
matching the deployed production engine.

The uninterrupted phase recorded 4,322 resource samples: 83.2 MiB peak engine
RSS, 36 peak/15 final quiescent FDs, +2.38 MiB warmed-to-final median RSS and
zero median FD growth. It completed 101 concurrent batch epochs, 303 mixed
four-fragment traffic/deletion epochs, 37 autonomous-healing perturbations,
37 disconnect probes and 18 complete baseline hash checks.

Recovery checks passed for listing, NCX fallback, recover-on-read, autonomous
repair, unrecoverable-object fencing/retry and root-identity rejection. After
the directory-outage masks, fragment creation returned 503 because an automatic
availability scan had failed and its live safety gate was still read-only.
The harness had accepted an older topology-status snapshot without proving a
fresh scan and successful write. The run stopped before repeated process-crash
cycles and final integrity validation; it is not an eight-hour pass.

RSS, file descriptors, CPU and I/O were sampled every five seconds. Planned
fault-phase restarts are outside the continuous-PID resource phase.

The earlier October 7 main-server run passed six uninterrupted hours: 4,322
resource samples, 81.7 MiB peak engine RSS, 35 peak/15 final quiescent FDs,
2.26 MiB warmed-to-final median RSS growth and zero median FD growth. Its
subsequent recovery phase stopped because the harness wrongly required HTTP
200 from health below the namespace quorum; N1 correctly returned 503. The
overall run is not a pass. The October 8 harness corrected that expectation
and checked below-K rejection before its continuous phase; engine code was
unchanged between those two long runs.

The MCH attempt started at 09:26 on October 8 with a 17:26 hard limit. Its final
outcome is unverified because that host is inaccessible from the current home
network; no pass or confirmed stop is claimed.

The supervisor saves failure/timeout evidence and stops the isolated test
automatically; OOM, early exit and timeout are not passes. Physical power cuts,
actual member replacement, capacity exhaustion and the other remaining gates
below are not established by these tests. No LTS declaration follows from them.

### Safety-gate recovery patch — October 9, 2026

The patch reports the live safety reason in healer health and masks write-safe
status while that gate is closed. Automatic retry scheduling now checks the
existing persistent scan fault, not a display phase that topology updates can
replace. Restoring roots cannot clear unresolved repair debt. No namespace,
NDX/payload format, mutation ordering or durability policy changed.

The isolated harness now requires a successful post-restoration scan, fresh
topology check, open maintenance gate and real fragment write/delete after each
outage mask. A separate regression uses a ten-minute periodic scan interval to
require prompt fault-driven recovery without restarting the engine.

Candidate engine SHA-256:
041185921f743a8c4e996d2d1b22a2e67aba38d3ac8c87af01af9a7f63d4303f.

| Validation | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 252 engine, 2 EC integration, 6 proof-runner, 10 shell, 7 CLI; 277 total |
| All-target check, warnings-denied engine library compilation, formatting/whitespace | PASS |
| Lost-retry reproduction on previous engine | FAIL as expected: after topology restoration, the safety gate stayed closed through the 30-second recovery deadline |
| Same long-interval fault on candidate | PASS: automatic recovery, real write/delete and complete baseline verification within 11.3 s, without restarting during recovery |
| Four-member 3+1 recovery regression | PASS: two complete fault/crash cycles in 11m 57s; supervisor exit 0 |
| All single-/two-member directory-outage masks | PASS: 20 executions, each requiring a fresh scan and actual post-restoration write |
| Interrupted rename/move/trash/recover | PASS: eight process kills, 128 objects per case, early/mid-prefix cuts, hash-correct reachability/retry and unchanged immutable files |
| Listing, NCX fallback/repair, recover-on-read, autonomous data/parity repair, unrecoverable-object fencing/retry and root identity | PASS in both cycles |
| Final integrity | PASS: 129 version-pinned baseline downloads plus complete baseline immutable inventory/size/hash comparison |

This was isolated on the main server's four independent XFS members using
verified loopback HTTPS. Production retained its original PID and engine hash;
no deployment occurred. Raw reports remain excluded from version control.
The candidate has not completed a new eight-hour soak or physical power-cut test.

## Upload/deletion hardening — October 6, 2026

Tested engine SHA-256:
7fedadeb07086ba965c8ceeb68099a16d9f54fbf2674e29c00dd20af542851f1.
Initial validation used new isolated four-member storage. The subsequent
live rollout is recorded below.

| Gate | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 248 engine, 2 native, 6 proof-runner, 10 shell, 7 CLI; 273 total |
| Upload tests selected independently | PASS: all 12; the previous isolated run failed because storage settings were uninitialized |
| All-target check, warnings-denied engine compilation, formatting/whitespace | PASS |
| Four-member 3+1 verification, 11m 45s | PASS: 25 checks covering deletion errors/retry, stale HTTP/native uploads, upload/delete/disconnect overlap, restart readback, recover-on-read, autonomous shard/NCX healing and fail-safe retry |
| Interrupted rename/move/trash/recover | PASS: 128 objects per operation remained reachable and hash-correct; retry completed; ordinary mutations left immutable files unchanged |
| Batch contracts | PASS: mixed results, empty/oversize fields, 32 same-key versions/max ULID, disconnect, explicit strict readback and 4,096/4,097-field boundary |
| Real LAN uploads | PASS: 24 trials, two repeats, 16 KiB/1 MiB objects, one-shot/batch and up to four clients; all 1,792 uploads hash-verified |

LAN measurements used direct certificate-verified HTTPS with keep-alive, no
SSH tunnel or Accel. Readback was outside the upload timer. Engine peak RSS
was **60.2 MiB**, versus **267.1 MiB** in the earlier equivalent LAN workload.
Peak/end file descriptors were 55/15. The complete fault-test cgroup peaked at
1.31 GiB, accounting for the harness and kernel memory as well as the engine;
that is not engine RSS or an uninterrupted-process memory-soak result.

| 1 MiB batch workload | Earlier throughput | Candidate throughput |
| --- | ---: | ---: |
| One client, concurrency 8, 32 objects | 6.45–15.15 MiB/s | 6.08–19.48 MiB/s |
| One client, concurrency 32, 32 objects | 6.13–7.64 MiB/s | 6.02–11.60 MiB/s |
| One client, concurrency 128, 128 objects | 5.09–5.25 MiB/s | 5.61–6.32 MiB/s |
| Four clients, concurrency 32 each, 128 total | 4.41–5.96 MiB/s | 5.45–10.30 MiB/s |

The 32-object/concurrency-8 batch took 1.64–5.26 s for the complete request,
not per object. Results are mixed: the 16 KiB/concurrency-128 case decreased
from 0.228–0.360 to 0.086–0.200 MiB/s, and sequential 1 MiB one-shot decreased
from 1.97–3.50 to 1.33–2.71 MiB/s. Shared HDD/OS state and non-interleaved runs
limit causal comparison; parallel publication and lower buffering do not
establish a universal speedup. A prior instrumented trace attributed about
97% of its upload interval to sync calls; it was not a LAN baseline.

These are bounded process-crash and readback tests, not physical power cuts or
a new long soak. During those tests, the live engine, Console, Accel and user
data were unchanged.

### Main-server rollout — October 6, 2026, 22:47 Manila

The tested engine above and its rebuilt matching backup helper are now live.
Previous binaries were retained for rollback. Startup was write-safe on all
four independent 3+1 members, with no warning-or-higher startup journal entries.
Before/after and post-smoke checks preserved 269 live objects, their namespace
snapshots, 3,708 immutable file paths/stats, 1,192 NDX hashes and three
version-pinned readback samples. Configuration, credentials and TLS hashes
were unchanged; existing backups were not modified.

The nine-object live smoke passed one-shot/eight-object batch upload, single
and eight-key prefix rename/move/trash/recover, exact-version readback and
immutable-file hashes. Its fragment was removed; a stale upload returned 404
without recreating it. Console returned verified HTTPS 200, and its nginx
process and Accel process were unchanged. This is deployment verification,
not a new LAN benchmark, full payload inventory hash or backup/restore drill.

## Deployed copy/delete candidate — October 6, 2026

Tested engine SHA-256:
4ff20f6bba63b1c6837d44835f4b81813895191830cc2e3d864201a7454720f0.
The original candidate tests did not deploy this build; production remained on
the October 5 build during those tests. The subsequent deployment is recorded
below. Console and Accel were not replaced.

| Gate | Result |
| --- | --- |
| Warnings-denied serial workspace/all-target tests | PASS: 241 engine, 2 native, 6 proof-runner, 10 shell, 7 CLI |
| Warnings-denied check, formatting and diff whitespace | PASS |
| Four physical rotational XFS members, 3+1, 1,000 objects | PASS: 32 same-key versions, 128 concurrent batch/native objects, coexistence, conflicts, NCX repair, cold degraded restart and mutation refusal |
| Live LIST, 73-entry pages | 3.63–5.04 ms, warm server-local observations |
| Ordinary mutation immutability | NDX/payload paths and hashes unchanged; deliberately corrupt NDX did not block NCX mutation |
| Process interruption during 128-key rename | Every object retained and hash-verified; retry completed deletion |

### Main-server deployment — October 6, 2026

The engine above and its matching backup helper were deployed to the main
server. Previous binaries were retained for rollback; configuration,
credentials, certificates, user objects and backups were preserved.

| Deployment check | Result |
| --- | --- |
| Locked release build; namespace regressions | PASS: engine matches tested SHA-256; 41 namespace tests |
| Before/after live namespace and readback | PASS: 32 objects, complete NCX replicas, identical snapshots and three version-pinned download samples |
| Existing immutable files | PASS: 1,204 file paths/sizes/mtimes/inodes unchanged; all 128 NDX SHA-256 hashes unchanged |
| Single-key and eight-key prefix rename/move/trash/recover | PASS: nine isolated test objects; version-pinned bytes and NDX/payload paths, sizes, mtimes and SHA-256 unchanged during mutations |
| Readiness and logs | PASS: four independent members writable, 3+1, write-safe; no warning-or-higher journal entries after startup |
| Console and Accel | Existing processes unchanged; Console verified HTTPS login page returned 200 |

Single-key rename/move/trash/recover took 228/121/100/75 ms. Eight-key prefix
operations took 228/1,170/176/305 ms. These are one observation per operation
over server-local, certificate-verified HTTPS with new connections, not a LAN
benchmark. Sampled engine RSS/HWM was 78,112 KiB after the smoke test; no
per-mutation memory or continuous-soak claim is made.

The first smoke harness used the wrong trash-response field and failed after
its mutation assertions. Its fixture was removed, the harness was corrected,
and a fresh run passed. That fixture was also removed; the final production
baseline comparison passed. This deployment does not close the remaining LTS
gates below.

### Network latency and memory

Mutation timing used real LAN HTTPS through an SSH tunnel, verified certificates
and keep-alive, without Accel. The hardware proof runner was server-local.
Both matched comparisons are retained; the favorable repeat does not erase
the initial regression.

| Single-object operation | Initial run, before → candidate median (6 samples) | Repeat, before → candidate median (12 samples) | Repeat candidate maximum |
| --- | --- | --- | --- |
| Rename | 135 → 447 ms | 117 → 63 ms | 1,007 ms |
| Move | 162 → 493 ms | 321 → 36 ms | 863 ms |
| Soft delete | 127 → 985 ms | 142 → 64 ms | 453 ms |
| Exact-ID recovery | 59 → 454 ms | 70 → 37 ms | 828 ms |

The initial 32-key rename/move/delete also regressed. A separate fresh candidate
run with 1 KiB and 8 MiB payloads measured rename/move/delete/recover medians of
81/78/58/33 ms. There is no universal sub-100-ms guarantee.

| 128-object prefix operation | Before | Candidate |
| --- | ---: | ---: |
| Rename | 16.30 s | 9.25 s |
| Move | 21.04 s | 10.08 s |
| Soft delete | 27.12 s | 14.80 s |
| Recover | 12.81 s | 2.19 s |

All 1,288 NDX/payload files retained their paths and SHA-256 hashes across those
cycles. Shared devices, old-first ordering and unreset OS/device state limit
causal comparison. A separate diagnostic trace observed a directory sync
taking 326 ms; tracing is not a latency baseline.

Sampled candidate RSS/HWM was 24,636 KiB in the initial matched phase and
77,372 KiB after fresh tests including uploads. These are whole-process samples
from different histories, not per-mutation allocations or a continuous peak.

SIGKILL 150 ms into a 128-key rename left 123 source and 18 destination names,
including 13 duplicates. Their union contained all 128 correct objects.
Restart/retry finished the remaining sources; 1,024 immutable files stayed
unchanged. This proves a process-crash case, not power-cut/cache-loss durability.

### Earlier accepted results

| Date / candidate | Accepted evidence and limits |
| --- | --- |
| Oct 5, 2026 / initial format 4 | Six four-XFS modes passed: 10,000-object flat namespace, mixed-pool expansion, HTTP/native 2 GiB restart/resume, exports, concurrent GC and 18-check backup/restore. LIST median/p95/max 10.29/15.82/19.17 ms; sampled engine RSS 99,304 KiB. These used the earlier deletion design, not the Oct 6 candidate. |
| Sep 12, 2026 / 3.5.1 | Supervised backup recovery: 11 checks. Separate N1+Accel fixture: 12 API checks; refresh-based search and version-pinned byte-cache hashes. Browser mock tests were separate, not a live end-to-end proof. |
| Sep 11–12, 2026 / 3.5.1 | Two four-member offline backup drills: 18 checks each; interrupted SSH replication and fenced/resumable restore passed. Metadata-heavy restore improved 781.73 → 163.27 s; small-fixture observations only. |
| Sep 9, 2026 / 3.5.1 | 221 workspace tests and all 11 four-HDD stages; full matrix 37m 6s. Covered concurrent upload/GC/TTL, exports, repair, 1,000-object storm and 192 same-key versions. No unexpected log faults; 24 expected injected-fault lines. |
| Sep 1, 2026 / 3.5.1 | 128 MiB export interrupted during build, restarted, range-resumed over LAN and hash-verified. Final shared-hardware build 14.72 MiB/s; LAN resume 106.59 MiB/s. |
| Aug 29, 2026 / 3.5.0 | ECS compatibility for five EC layouts; four-HDD N1 matrix 22 gates / 3,846 objects. Separate 4 GiB ECS encode 205.74 MiB/s and recoverable missing-member cases passed. |
| Aug 10, 2026 / 3.5.0 | Kernel ENOSPC returned 507 with no namespace, NDX or shard publication. Local 1+0 small-object batch measured 549.70 vs 228.52 objects/s; not multi-device fault-tolerance evidence. |
| Aug 8, 2026 / 3.4.0 | Four-HDD standard/durable multipart contracts and large-object hashes passed; LAN standard multipart 93.98 MiB/s end-to-end. One-shot samples varied substantially. |
| Jul 15–16, 2026 / 3.2.x | NOA physical-topology proof and corrected three-HDD 20-gate matrix passed. Historical sub-ms folder operations used real-directory rename, not current per-child flat semantics. |
| Jun 11, 2026 / 2.0.0 | Colocated/split-root proofs, multipart restart, repair and redistribution passed; peak RSS 158,476/125,652 KiB. Those retired layouts do not define format 4. |

### Failures and corrections

- Oct 6: deeper upload checks reproduced two implementation bugs: fragment
  deletion returned success despite a permission failure, and an unexpired
  upload token recreated a deleted fragment. Error propagation/retry aliases
  and shared upload admission corrected both; the isolated hardware rerun
  above passed. Batch previously buffered before I/O admission and forced
  batched durability; admission now precedes buffering and durability follows
  the existing one-shot configuration/options. Flush requirements were not
  removed. The old real-directory batch proof fixture was replaced with an
  oversize-field rejection check, and readback was removed from ingest timing.
- Oct 6: the requested eight-hour soak failed after 6m 43s, with 18 checks
  completed. Automatic healing raced whole-fragment deletion and tried to
  publish a fragment descriptor into a removed directory, triggering the
  read-only fail-safe. Final baseline validation never ran; this is not a
  passed soak. The fix adds per-fragment repair/delete exclusion and stale-scan
  revalidation, without changing ordinary NCX mutations. All 270 warnings-denied
  serial workspace tests passed, including four new lifecycle regressions.
  The corrected release build passed 31 hardware check executions in 7m 34s,
  including six concurrent fragment-deletion rounds during healing and final
  baseline hash validation. The fresh one-hour run completed in 59m 26s with
  103 check executions, two fault cycles, 43,093 API requests and 132 fragment
  deletions during healing. Final baseline hashes passed. Intentional process
  restarts were part of that run; it was not an uninterrupted-PID memory soak.
  The live service was unchanged.
- Oct 6: initial matched latency regressed; repeat still had large outliers.
  The first handoff also failed because the candidate binary was absent; rerun
  began only after both builds were available. The failed handoff is not a pass.
- Oct 6: an old test required tombstone suppression. It was replaced explicitly
  with the approved duplicate-name/retry contract, not presented as unchanged.
- Oct 5: flat GC removed expired trash but failed to count reclaimed logical
  versions. A reproducing regression was added; the corrected GC proof passed.
- Sep 12: whole-job SIGKILL killed the backup service's restart subprocess.
  Separate-cgroup OnFailure recovery was added; the fresh 11-check drill passed.
- Sep 7–9: GC could reclaim young/active uploads through old parents or last
  trash anchors; session-inspection failure could remove protection. Cancellation
  could retain finalizer state or release locks before blocking workers finished.
  These failures were reproduced and corrected before the final candidate-8 pass.
- Sep 7–9: repair deleted writer-owned staging; concurrent trash/TTL recovery
  exposed disappearing metadata. Preserve/retry/fail-closed regressions followed.
  A blackout interrupted an earlier matrix; ten observed stages were not a full
  pass. A 30-minute aggregate deadline was exceeded during redistribution; the
  later 90-minute matrix is a separately labeled run, not a passed old deadline.
- Jul 2026: order-dependent replica digests and per-replica timestamps caused
  real hardware failures. Sorted hashing/shared timestamps fixed them. Another
  harness incorrectly expected healthy reads to repair unused parity; a corrected
  data-shard fault selector passed a fresh 21-gate matrix.
- Early native testing: missing TCP_NODELAY caused approximately 82 ms delayed
  ACK/Nagle latency. Later loopback control-call results improved; they are not
  current LAN mutation benchmarks.

## October 23 — failed 300k HDD run and follow-up

The upload-only EC 1+1 run on two USB-attached HDDs stopped after **129,113
acknowledged objects in 10 h 23 min**. The 100k checkpoint passed its historical
hash checks. Eight timed-out batches have ambiguous outcomes; the final partial
batch also reported 29 admission rejections. This is a failed scale/performance
gate, not evidence that 300k or a million objects passed. Final acknowledged-data
verification and reconciliation of ambiguous writes remain required.

Follow-up changes retain strict durability, request deadlines, concurrency and
TTL settings. They cover admission through NCX publication, missing-directory
creation, stable-ID resolution and cooperative GC reference collection. Separate
write-drain and sync timings avoid treating direct writes as measured fsync cost.

The candidate `abc1816cba80f5ee683db6515a2ac3b5443f67fa4fc15402e2f77a72fe76cc57`
passed 382 engine tests, 72 Python harness checks, nine proof-runner tests and
the two-HDD scheduled reclamation plus five-part recovery matrix. Those are
integrity gates, not a passed scale or performance test. The admission regression
confirms that caller cancellation retains the detached commit's foreground
permit until NCX publication finishes.

The fresh local 300k attempt was operator-stopped at **24,336 acknowledged
objects after 9,002 seconds** to investigate performance. Its 1k and 10k hash
checkpoints passed; final reconciliation remains pending. Sixteen batches had
ambiguous outcomes recorded during interruption. First-1k throughput was
2.701 objects/s versus 5.048 previously; the next 9k measured 2.825 versus
4.517. The new run used a shared USB hub, whereas the earlier run recorded
separate USB controller paths. The regression also predates scheduled GC, so
it cannot be attributed solely to GC. These are not controlled code-only A/B
results. Both local populations and the interrupted comparison are preserved.

### MCH SATA follow-up — October 23, 2026

At 16:22 Manila, the same frozen candidate began an isolated **300,000-upload,
then churn** run on four directly attached SATA/XFS members, EC 3+1. The
64-object smoke passed, including hashes, mutations and restart checks. The
first 1,000 strict uploads took **70.225 seconds (14.24 objects/s)** and their
complete hash checkpoint passed. This is about 5.3 times the local USB run's
early rate, but hardware, filesystems, member count and EC geometry differ;
it does not isolate USB as the sole cause or prove large-file throughput.

The run keeps eight clients, 64-object batches, server concurrency 16, strict
durability, 300-second requests, the 30-second admission deadline and normal
GC TTL/schedule. Only fresh test directories are writable. Production services
are not restarted or replaced. A 768-GiB reserve and 80-GiB growth budget per
filesystem protect shared server capacity. This older-build run was deliberately
stopped before 300k so the newest implementation could be built and tested first.
Its interruption is preserved, not counted as a scale pass. Newer hot-path edits
were not included in that binary.

### New upload hot-path candidate — October 23, 2026

The new implementation removes the HTTP one-shot serialize/parse/NDX-reread
round trip, shares stream/reader shard setup, opens independent members in bounded
parallel and omits redundant small-shard pre-sizing. NDX publication no longer
creates unused upload/trash directories. Strict direct I/O, synchronization,
publication order, storage format and concurrency limits remain unchanged.

Focused regressions passed: **15 HTTP-route tests**, **five shard setup/data-plane
tests**, **two metadata-publication tests** and **72 harness tests**. The data-plane
gate covers 18 real strict stream/reader writes at zero, 4-KiB boundaries and
1-MiB boundaries, checking exact shard bytes, padding and checksums. Short/excess
bodies and checksum failures leave no shard files; HTTP failures publish no NCX.
These results do not establish a throughput improvement or a passed scale gate.

The warnings-denied release build passed with engine SHA-256
`3e50f648ee8b3b96ed3e0e9902d03ca7dc18643e5bce425c1e7fb9f08d9ce9d0`.
All 209 frozen source entries and package inputs were independently rechecked.
The earlier full engine-suite attempt stopped before completion, not PASS.

MCH became unavailable before this new binary could be tested there. Local
qualification therefore uses two physical USB HDDs, EC 1+1: scheduled reclamation,
strict zero/unaligned upload/hash/restart checks, then a fresh 391-test engine
suite. It uses fresh private directories without formatting or contacting another
server. Scheduled reclamation passed in **39.3 s**, preserving all 16 acknowledged
historical versions, finalizing both protected upload types and reclaiming real
SIGKILL-interrupted standard-upload orphans. The strict smoke passed in **59.3 s**,
including 64 source objects, churn, 12 zero/unaligned one-shot sizes and complete
hash/inventory verification before and after restart. Both reports identify the
new engine hash above. The fresh 391-test suite was deliberately stopped, without
a PASS, to avoid competing disk I/O during the targeted scale investigation.

These are not a four-SATA, 300k or million-object qualification or a demonstrated
throughput improvement. The prepared four-drive upload-first/churn sequence
remains deferred.

### Targeted upload-scale investigation — October 23, 2026

A fresh **300,000-object upload-only** run is active on the same two physical
USB HDDs, EC 1+1, using the new engine hash above. It keeps strict durability,
normal GC settings, eight clients, 64-object batches, server concurrency 16 and
300-second request deadlines. No churn or broad regression suite runs alongside
ingestion. Source files and durable acknowledgement evidence remain part of the
workload; existing failed populations are preserved.

The harness now separates per-batch source preparation, HTTP request and FULL
acknowledgement-ledger timing using bounded histograms. Quiescent checkpoints
retain existing shard setup/drain/sync, payload/NDX, NCX and scheduler telemetry.
No repeated health scans run during ingestion. All **76 harness regressions**
passed before freezing these measurement changes. Timers overlap across clients
and server stages nest, so their durations must not be added as serial costs.

This is a fresh population-growth test, not a controlled old/new comparison.
Its 6-GiB cgroup includes client/engine memory, filesystem cache and slab; a limit
event is not automatically an engine RSS leak. Throughput, checkpoint integrity
and final 300k qualification remain pending until the corresponding gates finish.

## Reproduce validation

```bash
RUSTFLAGS='-D warnings' cargo test --locked --workspace --all-targets -- --test-threads=1
RUSTFLAGS='-D warnings' cargo check --locked --workspace --all-targets
cargo fmt --all -- --check
git diff --check
cargo build --locked --release -p n1 --bins -p n1-release-proof
python3 -B -m unittest discover -s tools -p 'test_n1_*.py'
```

Earlier default parallel execution intermittently failed backup automation's
immediate lease-reacquisition assertions. Those failures remain part of the
history; both current RC.1 execution modes passed as recorded above. A serial
pass alone is not a parallel pass. A warning-free full Clippy gate is not claimed.

Run optimized hardware proofs only in new isolated children of mounted members,
with unique output directories/unused ports. Never reuse live storage roots:

`tools/n1-hdd-benchmark.py` compares strict small-file uploads and explicit
maintenance at two populations. Run identical parameters with before/after
binaries and fresh fixture roots. `--admission-probe` separately exercises
partial batch backpressure; refused objects never count as successful uploads.
`tools/n1-hdd-scale.py` performs larger ingest/churn runs with a durable
acknowledgement ledger, complete historical hashes and restart checks. Both
tools require two distinct mounted members; inspect `--help` before running.

```bash
n1-release-proof --binary /proof/bin/litiaina-n1 \
  --source-file /proof/source.bin --disk-roots /mnt/A,/mnt/B,/mnt/C,/mnt/D \
  --out-dir /proof/results --port 56001 --native-port 56002 \
  --data-shards 3 --parity-shards 1 --storm-objects 10000 \
  --trash-retention-seconds 3600 --flat-namespace-proof
```

Also run --pool-expansion-proof, --multipart-resume-proof, --export-proof,
--backup-proof, --gc-safety-proof and --noa-api-proof separately. Large multipart
acceptance uses 2 GiB or larger; small fixtures do not establish throughput.
An extended --gc-workload-timeout-seconds run must not hide a failed default
300-second performance gate.

For offline member replacement, run `--pool-maintenance-proof` with
`--pool-member 0` and `--spare-root /mnt/SPARE` as root with a genuinely
independent spare filesystem. Repeat data/parity slots and 2+1/1+0 layouts.
On a 1+0 fixture, `--backup-proof` with `--spare-root /mnt/SPARE` additionally
places the replica and restored member on that other device. Tests must never
bind over a live storage root.

Required regressions cover immutable pool enrollment, zero-decode LIST, same-key
lineage/cancellation, NCX-only mutation, destination-before-source barriers,
joined workers, conflicts, process restart, standard/durable upload boundaries,
kernel capacity failures, original-layout repair, GC protection and portable
backup recovery. Live Console/Accel/native/NOA/WebSocket tests must also exercise
flat key/prefix behavior and partial results.

## Remaining stable/LTS qualification and evidence

Repeated power cuts at different fsync stages, simultaneous multi-member replacement,
broader pool/failure topologies, capacity exhaustion under format 4, longer
memory/FD soaks on the patched candidate,
larger/cold namespaces, complete live consumer integration, and backup failure
cases beyond the verified interruption/retry matrix remain qualification work
for stable/LTS decisions. RC.1 is available for wider validation, not a claim
that these gates have all passed.
Cross-filesystem bind cutovers and process kills are not physical device-loss
or controller-cache tests.

This document summarizes accepted results, failures and remaining gates.
Raw hardware and deployment reports are local-only and excluded from version
control.

No LTS, failover, zero-loss, constant-memory-at-any-scale or universal latency
claim follows from these bounded results.
