# LINK RC.2 implementation ledger

## INI operations and named public service — November 3

Original operator INI and private environment controls are wired into object
startup, TLS/listen addresses, immutable pool enrollment/append, concurrency,
I/O admission, staging GC and opt-in scheduled healing. Metadata has its own
INI startup and consumes the public native object client; it does not require
member roots or import object-engine internals. The object binary remains an
independently usable key store. These operational changes supersede the earlier
directory-only preservation checkpoint below.

The metadata application now serves the named HTTP/WebSocket adapters and
original named native protocol. Public integration tests cover fragments,
permissions and refresh sessions, one-shot/grouped batch uploads, original
standard/durable multipart shapes, pending publication/restart, namespace and
history operations, signed/ranged reads, and the retained ZIP64/TAR builder.
All three original console live tests pass without changing its production
runtime or proxy. Shallow listings seek past descendant prefixes.

A complete all-target workspace regression passes **414 tests**, with nine
explicit helper/opt-in exclusions (`final-workspace73.log`). A subsequent native
pool expiry regression and its metadata/native boundary suite pass **17 cases**;
updated strict metadata Clippy and the public two-HDD API witness also pass.
These are separate runs, not 431 distinct tests.

The first new full-stack population attempt failed: a captured 256-object HTTP
batch contained 128 successes and 128 backend errors. Metadata had retained
native pooled connections beyond the independent object's 120-second idle
deadline during slow HDD persistence. The metadata pool now retires idle
connections at 90 seconds; its transport regression requires a fresh connection
and login. The object engine's protocol, placement and integrity policy are
unchanged. Fresh API and browser 500,000-object campaigns are in progress;
neither has passed yet. No release follows from preflight or smoke results.

Current evidence, exact frozen binary/source hashes, failed attempts and scope:
[FULL-STACK-QUALIFICATION-20261103.md](/mnt/PROJECT-DISK-1TB/n1-ini-restoration-20261103-01/reports/FULL-STACK-QUALIFICATION-20261103.md).
Remaining metadata lifecycle/reclamation, coordinated backup, request quotas,
optional integration and complete public contract qualification remain distinct
product work. The user has authorized a checkpoint commit and push to `link`
while the detached campaigns continue. This checkpoint does not pass unfinished
campaigns, tag a release or promote RC.2 to production.

## Object independence and metadata transactions — November 3

The LINK object component stays independent and byte-identical across all 99
tracked files. The optional metadata application consumes the existing native
client, without an object-engine implementation dependency. A build-graph test
enforces both directions of this boundary. The existing console baseline is
preserved; its features are the metadata API contract.

The metadata application now has durable pending upload records, native byte
forwarding, atomic post-finalization publication, maximum-ULID version heads,
immutable version references, exact-key rename, trash and recovery. Renaming
retains a pending upload's logical identity; trash followed by an unrelated
replacement cannot be undone by a late finalize. Database roots protect stored
LINK credentials with owner-only permissions on Unix.

A two-HDD 1+1 process witness passes against the unchanged qualified LINK binary:
direct key access before metadata exists, named writes and reads, out-of-order
durable multipart, rename while pending, physical completion before metadata
publication, object process restart, saved-session retry, history, empty objects
and direct LINK access after metadata shutdown. The full workspace run passes
398 tests; the final private-root change separately passes all 29 metadata
targets/tests and the physical witness. Scoped strict Clippy passes.

These internal APIs are not the public metadata server. NOA adapters, fragment
authorization, all retained upload schemes, prefix/list/trash projections,
exports, lifecycle/backup coordination and startup/enrollment wiring remain
required product work. Restoring the shared INI parser is not a claim that those
settings are already wired into the unchanged object startup.

Full evidence and exact validation sequence:
[COMPONENT-BOUNDARY-20261103.md](/mnt/PROJECT-DISK-1TB/n1-operational-restoration-20261103-01/reports/COMPONENT-BOUNDARY-20261103.md).
No commit, push or release accompanies this work.

## MCH metadata storage qualification — November 3

The optional metadata foundation passes the 143 metadata/Procyon regression
cases on MCH DISK0, the 15 metadata integration cases independently on all four
SATA HDDs, and 81 storage documentation examples with Rust 1.92.0. The new
Rust-only qualification supervisor additionally completes a one-million-entry
database, eight concurrent writers/four snapshot readers, SIGKILL/restart,
100,000 update/delete fixtures, a second SIGKILL/restart, full exact-value/history
verification and an exact final 2,080,000-record application-table census.
Every previously acknowledged entry is verified before resuming writes.

The full million-entry campaign takes 438.99 seconds including faults and
verification, with 106.66 MiB maximum RSS. Independent 8,192-entry concurrent
fault/churn witnesses pass on DISK1–3. Both headless drivers exit zero; existing
N1 processes are preserved and test processes end. No engine implementation
changes are required. The new harness raises the complete local workspace pass
to 382 tests, zero failures; the imported-core lint findings remain separately
documented. No NOA metadata or object-upload qualification is implied.

Evidence and exact scope:
[MCH-METADATA-QUALIFICATION-20261103.md](/mnt/PROJECT-DISK-1TB/n1-mch-metadata-20261103-01/reports/MCH-METADATA-QUALIFICATION-20261103.md).
No commit, push or release accompanies the testing instruction.

## Optional metadata component — November 3

The user authorized using ProcyonDB as N1's metadata foundation and renaming the
existing component to `object_engine/`. The directory move preserves all 99
tracked component files exactly; package `n1`, executable names, adaptive
concurrency, middleware, server, io_uring, streaming and backup remain intact.
The object dependency graph contains neither Procyon nor `n1_metadata_engine`.

`metadata_engine/` and its retained `storage/` fork are workspace members. The
foundation uses Procyon's optimistic transaction engine, durable `SyncAll`
commits, cross-table snapshots, exact fragment-scoped identifiers, bounded
pagination and mutation sizes, and a bounded async execution bridge that owns
accepted work through caller cancellation and shutdown. Offline init/check
commands are implemented. The original Procyon source repository is untouched.
Selected upstream correctness fixes and retained tests are recorded in
[`metadata_engine/storage/UPSTREAM.md`](../metadata_engine/storage/UPSTREAM.md).

Final validation: **381 workspace tests and 81 storage documentation tests pass**;
both HDD filesystems separately pass the 32-transaction process-kill/recovery
witness. N1 production Clippy passes with the imported core and historical proof
tool excluded from linting; an inclusive attempt's 89 diagnostics remain in the
evidence. All core tests are included. Formatting and whitespace checks pass.
See [the full report](/mnt/PROJECT-DISK-1TB/n1-metadata-foundation-20261103-01/reports/METADATA-FOUNDATION-20261103.md).

Namespace/history/ACL models, named-upload coordination, public NOA v1/v2
metadata adapters and coordinated metadata/object recovery are service-layer
work, not capabilities supplied by this database foundation. The agreed API and
lifecycle boundaries are in [METADATA-ENGINE.md](METADATA-ENGINE.md). No release,
commit or push is authorized by this integration instruction.

## Preserved object checkpoint and qualification

Started November 1, 2026. Baseline: `061a40a`. On November 3 the user authorized
a checkpoint commit and push to the `link` branch; `main` remains the original
baseline. Deployment, tags and release remain unauthorized. The baseline archive
and qualification evidence are on the HDD. LINK changes the object
identity/placement/format; N1's tested infrastructure and familiar module
responsibilities are being retained/adapted.
The earlier broad replacement removed useful code and is not the accepted
implementation boundary.

The final November 3 checkpoint passes a fresh warnings-denied full workspace
run: **238 passed, 0 failed**, including 185 engine-library tests; four explicit
physical/helper cases are excluded from the default command. This includes the
new resumable client harness tests. Formatting and staged-diff checks pass.
HDD log: `reports/link-precommit-workspace213.log`. Physical and population
results below retain their exact binary hashes and separately completed scope.

## Hybrid integrity and two-HDD population qualification — November 2–3

A subsequent November 3 comparison runs the preserved original NCX/NDX binary
against the current CRC32C LINK binary in two separate alternating four-trial
native campaigns. All 844 GETs and 408 removal/absence checks pass. Full-workload
small PUT/next-GET/stat times fall 51.8%/67.5%/63.5%, maximum sampled RSS falls
33.5%, and large PUT is near parity. Repeated large-read throughput varies
193.58–747.17 MiB/s for LINK and is slower in aggregate with unequal cache
residency/storage counters. Permanent deletion costs more than original trash.
No engine or harness code changed; all owned servers stop cleanly.
See [the complete current comparison](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/NCX-VS-CRC32C-LINK-20261103.md)
for every trial, CPU/RSS/I/O and scope. No across-the-board speedup is established.

New immutable pools use CRC32C for encoded frames and context-bound SHA-256 for
decoded source; identity, headers, seals and roots retain SHA-256/HMAC. Missing
pool-format fields retain the exact SHA-v1 encoding. Enrolled profiles cannot
be changed in place. The byte contract is in [LINK-FORMAT.md](LINK-FORMAT.md).
This change preserves the module boundaries, banner, README design, adaptive
concurrency, recovery ownership and required io_uring infrastructure.

| Completed check | Result | HDD evidence |
| --- | --- | --- |
| Hybrid Rust workspace | **235 passed, 0 failed**, including 185 engine-library tests; four opt-in/helper cases excluded from default counts | `hybrid-workspace-tests179.log`, `180.log` |
| Strict production-crate Clippy | Passed; historical release-proof exclusion retained | `hybrid-clippy182.log` |
| Two-HDD process/recovery witness and actual SHA-v1 binary upgrade | Both passed; old immutable bytes/inodes preserved | `hybrid-physical184.log` |
| 1 GiB + 100 individual + 100 fresh batched objects | 402 exact GET checks and 201 permanent deletions passed; detailed timings recorded separately | `hybrid-full186.log` |
| Resumable Rust harness | Five tests and strict Clippy passed; final inventory helper also lint clean | `hybrid-resume-tests195.log`, `hybrid-final-clippy204.log` |
| Two-HDD 4,099-object resume/fault witness | Passed SIGKILL, expected acknowledged-corruption rejection without rewrite, exact injected-byte restoration, repeat resume and full GET verification | `hybrid-resume-physical197.log` |
| Requested two-HDD population | **500,000 distinct durable acknowledgements and all 500,000 full length/SHA-256 GETs after SIGKILL/restart passed**, zero mismatches, clean shutdown | `hybrid-scale-resume199.log`; completed checkpoint `PASSED.json` |
| Offline physical inventory | **1,000,000 correctly placed shard files**, no unreferenced shards, duplicates or leftover staging | `SCALE-500K-INVENTORY-FINAL-20261103.json`, `hybrid-final-inventory206.log` |
| Original client journal preservation | All eight original ACK/intent SHA-256 hashes still match | `hybrid-original-journals203.log` |

Production engine SHA-256:
`82d68833b7fafcaf5cf3b9b138a447a8a8f1d4a889f8ddde8ae86db7840e91e7`.
Only the Rust harness changed during the 500,000-object run. The original
processes stopped without a terminal result; the cause is not established.
Resume preserved 397,568 acknowledged objects without PUT/finalize and completed
102,432 remaining acknowledgements. Exact remaining-upload wall time was
1,669.967 s; all-population GET verification was 3,958.695 s. These are separate
measurements, not an uninterrupted 500,000-upload benchmark or a matched NCX
comparison. All original journals, data and binaries are retained on the HDDs.

The original process interruption, corrected harness visibility error and two
inventory-helper Clippy style errors are retained in their separate logs;
the expected fault-injection rejection is documented as a negative assertion.
The temporary headless user job exited successfully, with no service installed
or enabled. No test engine remains running. No Python, commit, push or release.

Full methodology, latency percentiles, scoped RSS/CPU/I/O, physical allocation,
binary hashes and raw paths:
[HEADLESS-500K-20261102-20261103.md](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/HEADLESS-500K-20261102-20261103.md).
The completed two-member population proof does not close the broader topology,
power-loss, million-object/churn and other release gates listed below.

## Implementation inventory

Implemented and exercised during this work:

- Shared `n1_link`: canonical identity/placement, scoped object keys and upload
  tickets, strict parsers, inline shard format, permanent terminal format.
- New server startup/configuration and explicit independent-member enrollment;
  directory leases, pinned bindings and descriptor-relative no-follow paths.
- Positioned streaming EC parts, standard/durable part semantics, bounded active
  coordination/progress, restart status, duplicate/conflict checks and finalization.
- Fused normal upload and bounded batch upload with two shared filesystem
  barriers; no background commit queue or per-object sidecars.
- Verified GET/ranges, shallow/deep object health, manual/read repair with healthy
  inode preservation, permanent single/bulk retirement and interrupted-delete retry.
- Conservative staging GC; optional rate/window-limited scrub/repair; disabled
  schedules by default, operator job start/status/stop, foreground preemption.
- Direct native NOA 2 (`n1/1`), verified native TLS, fragment authorization,
  HTTP adapters calling the same service, native SDK and explicit-key CLI.
- Count/memory/control admission, cancellation ownership and shutdown draining.
  Stat, delete, cancel and service initialization also take bounded admission;
  monitoring requests do not cancel a queued read repair.
- Replaced `ARCHITECTURE.md` and `NOA.md` with the new runtime contracts;
  updated the README and operator guide and marked old testing results historical.
- Replaced encrypted recovery points: bounded content-addressed chunks and
  hierarchical manifests, committed online views, offline resumable capture,
  authenticated verification, fenced portable restore and explicit resume.
- Resumable local/native/secondary HTTP recovery-point replication; operator NOA
  capture/list/verify/replicate/prune/GC/job/stop and native CLI controls.
- Restore overlays known retirements; retention refuses to discard the last saved
  retirement witness using bounded temporary sorted runs, not a permanent index.
- Removed name/version metadata and explicitly discarded namespace/export
  operations. Infrastructure removals were audited separately and are being
  restored; a smaller engine is not evidence that its retained features survived.
  Dependency versions remain pinned to the baseline.
- Corrupt terminal records block old-payload reads, repair and ticket reuse even
  when K other valid payload shards remain; authenticated delete retry completes
  retirement and remains terminal after restart.

## Infrastructure restoration — November 2

The removal audit is preserved on the HDD at
`reports/removal-audit-20261102/AUDIT.md`. Restored original implementations and
tests include adaptive concurrency, FIFO disk scheduling, system sampling,
Linux I/O helpers, the N1 EC streaming wrapper, rate-limiter quota machinery,
operation gates and backup automation. LINK-specific adapters retain their
responsibilities. `ARCHITECTURE.md` maps the actual module boundaries.

Refresh rotation/reuse detection, scoped signed reads, optional UTC calendar
scrub, registered member binding and encrypted service-identity recovery are
wired through the service. Offline pool copy/fencing/inventory helpers are
retained with LINK inline-file verification. The original shell offline proxy
and argument-module boundary are restored. Shipped service units use the actual
offline engine entry point; these changes do not install or start host services.

The original four-slot recovery bound and separate CPU-affine runtime now govern
LINK read repair. Full admission backpressures instead of silently dropping
repair; same-fingerprint requests coalesce without losing a request at completion.
Repair owners and the optional calendar scheduler drain on shutdown. An admitted
operation retains its worker and control lease through caller cancellation,
including authentication session persistence. HTTP download tests cover exact
bytes, bounded/suffix/oversized ranges, If-Range, GET/HEAD ETag preconditions,
empty objects, cache policy and signed-range restrictions.

Explicit offline `pool replace --authorize` now adapts the retained
copy/fence/inventory/synchronization workflow to LINK. Tests cover every logical
slot, exact payload and healthy inode preservation, permanent/incomplete
retirements, durable sparse multipart progress, partial finalization, fenced
interruption/resume, corrupt peers and unexpected inventory. A sparse 1 TiB
logical fixture with a verified final stripe proves extent-aware reconstruction
does not read its unwritten gap. It allocates only headers and actual frames;
it does not claim a 1 TiB transfer or disk durability result.
Member leases release when their final Arc owner ends, so an inherited duplicate
cannot prolong a completed lease; a deterministic duplicate-descriptor test
retains exclusion until the last legitimate owner ends.

| Current check | Result | HDD report |
| --- | --- | --- |
| Entire workspace, warnings denied | **220 passed, 0 failed**; 176 engine library tests; 3 opt-in/helper cases ignored in this command | `restoration-replacement-tests78.log` |
| Strict Clippy, current engine/LINK/SDK/CLI, all targets | Passed, warnings denied | `restoration-final-clippy80.log` |
| Original streaming EC contract plus LINK EC contract | Both suites pass | `restoration-replacement-tests78.log` |
| Real native/HTTP authentication, quotas, refresh, signed reads and maintenance gates | Passed | `restoration-replacement-tests78.log` |
| Real paused upload and dynamic scheduler/stream resizing | Passed in isolated subprocess | `restoration-replacement-tests78.log` |
| Repair backpressure, coalescing, worker/session cancellation ownership and member lease lifetime | Passed | `restoration-replacement-tests78.log` |
| Backup supervisor workflow faults and executable service entry points | Passed | `restoration-replacement-tests78.log` |
| Member copy/reconstruction, interruption fence, exact inventory, retirement and multipart cutover | Passed | `restoration-replacement-tests78.log` |
| Rust-only optimized two-HDD process witness, required io_uring | **Passed**: SIGKILL upload, SIGTERM during admitted upload, durable resume, exact GETs, read repair, retirement, clone/cutover, authorized failed-member reconstruction and encrypted service-identity recovery | `restoration-final-witness81.log` |
| Retained required io_uring driver checks, PROJECT XFS | Passed: 3,679 submitted/completed, zero queued/in flight | `restoration-uring-project32.log` |
| Retained required io_uring driver checks, TEST ext4 | Passed: 3,679 submitted/completed, zero queued/in flight | `restoration-uring-test33.log` |

Final physical report:
`/mnt/PROJECT-DISK-1TB/n1-restoration-witness-01M6CN0HSMKWTF2N6TA9HMJJBC/result.json`.
It records optimized engine SHA-256
`6114956511eccdab5851e84e33eb92f3f900df0d218e3ff6b563ddc965f039f6`,
distinct device IDs and phase timings. The warning-denied repository release
build is recorded in `restoration-final-release79.log`.
All owned test servers were stopped. Reports, member data and repositories are
on the HDDs; compilation and ordinary functional fixtures are in RAM. The
witness is functional recovery evidence, not a throughput benchmark, power-cut
test or million-object qualification. It does not establish feature parity by
test-count comparison.

The 220 total includes nine historical proof-tool unit tests. Their pass does
not port that tool's old namespace API scenarios. Whole-workspace Clippy also
finds three pre-existing lints in that historical tool (`restoration-clippy26.log`);
the current-target Clippy command above excludes only that tool. No lint is
silenced. Earlier compile/fixture corrections and the first HDD-root permission
failure remain in their separate logs; the physical rerun uses the existing
owner-writable TEST HDD `mx-tests` directory without changing mount permissions.
The first separate driver invocation used a missing fixture parent and failed
before any I/O checks (`restoration-uring-project30.log`); creating that private
parent corrected the harness setup. The successful runs above retain all checks.

The Rust-only release benchmark is recorded in
`reports/RESTORATION-PERFORMANCE-20261102.md`. It measures 1 GiB, 100 individual
4 KiB objects and 100 different batched objects, SHA-verified requested-cold/warm
GETs, stat, deep health, permanent deletion, sampled RSS, kernel VmHWM and CPU/I/O
counters. A separate immutable 4 KiB-block pool exposes the padding tradeoff.
Reported binaries have exact hashes; results predate the later offline
replacement change. The deletion median improved in the matched LINK pair while
p99 worsened. Serial small-object persistence tails and 130x default tiny-object
allocation remain material. These are workstation measurements, not a matched
NCX/LINK comparison or scale qualification.

A subsequent native NCX → LINK → LINK → NCX campaign is complete in
`reports/NCX-VS-LINK-BENCHMARK-20261102.md`. Frozen optimized binaries ran
on the same two HDDs with 1+1 EC, 1 GiB and 100 individual 4 KiB objects per
trial, including LINK init in upload time. All 808 downloaded bodies passed
length/SHA-256 checks; all four servers stopped cleanly. LINK reduced small PUT
time and process RSS, but warm large GETs regressed; original adaptive small
blocks versus LINK's fixed blocks, different cache residency and different
delete guarantees are explicit in the report. This bounded serial campaign
does not establish total RAM, batch parity, aged-namespace or scale behavior.

Configuration identity is now stable `N1-LINK`, independent of the package
release. CLI/ping obtain the release from workspace Cargo metadata; the LINK
crate inherits the same workspace version. The cleanup passes 221 workspace
tests, including native/HTTP version reporting and configuration validation,
and strict Clippy for the current production crates and all their targets
(`config-version-tests96.log`, `config-version-clippy97.log`).
The optimized updated binary
`f7c8283633f31175da2968c14364e665f666ea288ee2b0409b8d2713b2370701`
also passes the physical recovery witness in `config-version-witness99.log`
(47.92 seconds), including fresh configuration, restart and encrypted service
recovery. It is retained separately from the unchanged benchmark binary.

## Pipeline optimization — November 2

The restored module structure, adaptive controller, scheduler, io_uring driver,
middleware, repair ownership and backup workflows are retained. Upload now
recycles two bounded groups of owned buffers, coalesces up to four adjacent
stripes per member and overlaps preparation with completed/joined writes. EC
framing borrows source/parity directly; codec workspaces initialize lazily.
New partial tails preserve canonical zero padding with sparse physical ranges.
Retry regions are verified and written in full, and durable acknowledgement
still waits for the required persistence barriers.

GET and health coalesce bounded adjacent records, verify frame checksums once,
retain independent source checksums and recover individual corrupt frames.
Health, repair and member reconstruction reuse the computed/verified source
hash. Publication removes duplicate directory flushes while retaining durable
payload, destination, staging and fanout ancestry barriers before success.

The final source passes 223 workspace tests (179 engine library), strict
production-target Clippy and the physical Rust recovery witness on both HDDs:
`optimization-tests116.log`, `optimization-clippy117.log`,
`optimization-release118.log`, `optimization-witness119.log`.
The witness completes in 41.03 seconds using required io_uring and release
engine `f017966129b74894ea80d5c27bde0b446e5e77533ebb9f4fdfd294768e6000ed`.
Its independent result is
`/mnt/PROJECT-DISK-1TB/n1-restoration-witness-01M6D8M3C4XXMXS2TE49PAASVZ/result.json`.

Four alternating strict LINK before/after trials pass with all 808 downloaded
bodies verified and all owned engines stopped (`optimization-link-abba120.log`).
Warm 1 GiB GET aggregate throughput rises from 185.93 to 588.78 MiB/s; upload
throughput rises from 82.03 to 85.85 MiB/s. The mean 100-individual-object upload
phase falls from 52.27 to 40.14 seconds. Small-upload process write bytes fall
95.38%, while the immutable logical encoding remains unchanged. The delete
protocol is unchanged; median latency increases in this campaign even though
its aggregate phase time decreases. The detailed performance report is
`reports/OPTIMIZATION-RESULTS-20261102.md`, including every measured operation,
trial variation, memory/CPU/I/O, original-NCX comparison and full-workload results.
This is measured pipeline optimization, not million-object or power-cut proof.

Separate unsuccessful attempts remain in their logs. `restoration-replacement-tests72.log`
caught an intermittent WouldBlock during a restart's member lease acquisition.
The duplicate-descriptor regression demonstrates a lock-lifetime mechanism
consistent with it; it does not prove which concurrent child inherited the
descriptor in that invocation. Explicit final-owner release and the subsequent
passing suite are recorded separately. Earlier new-test compilation/fixture
mistakes and Clippy diagnostics are not classified as data-integrity failures
or removed from the evidence.

## Native download delivery and startup restoration — November 2

Native GET now writes verified payload directly to its socket/TLS transport.
The prepared range retains admission and lifecycle guards, and one owned worker
retains both the socket and those guards until admitted I/O drains. Caller
cancellation cannot release shard ownership prematurely. HTTP retains its bounded
body adapter. The native stalled-receiver deadline resets on transport progress
and does not limit total download or disk-verification time.

The original boxed `L I T I A I N A  N 1` startup banner, local-time formatter,
startup fields and non-blocking tracing writer are restored. Version/help and
offline command output remain parseable. `banner-smoke127.log` records actual
startup output; its missing-config error is deliberate smoke-test input.

The selected source passes **226 workspace tests**, including 182 engine library
tests, and strict Clippy on all current production targets:
`native-selected-tests170.log`, `native-selected-clippy171.log`.
Regressions cover cancelled-caller socket/shard ownership, stalled transport
deadlines and large-block corruption/range recovery. The three default
opt-in/helper cases retain their roles;
the nine historical release-proof unit tests do not qualify its old scenarios
against LINK. The historical proof executable remains excluded from the current
production-target Clippy pass for its recorded pre-existing diagnostics.

Release engine SHA-256:
`252b92024f59f2d339fa4e5af14412fef25272594404ab3ec1e39dc60e4ffff2`.
The final selected rebuild (`native-selected-release167.log`) reproduces the exact binary
that passed the 44.94-second required-io_uring physical recovery witness in
`native-direct-witness135.log`. Its report is
`/mnt/PROJECT-DISK-1TB/n1-restoration-witness-01M6DHHZZDBYH5GW02K2DDRXQT/result.json`.

The user explicitly retains the current frame/source SHA-256 policy. The Rust
checksum CPU probe measures CRC32 and both SHA-256 passes separately; these are
payload-verification costs, not LINK placement costs. No checksum, immutable
format, placement, EC geometry or deletion guarantee changes in this checkpoint.
The current configured-pool lookup scans configured pools; shard placement never
scans stored objects. See the HDD report `NATIVE-READ-PATH-20261102.md` for full
measurements, rejected experiments and limitations. RC.2 remains unreleased.

Large-window hashing in the existing blocking pool was tested and rejected:
warm throughput fell 63.41% and CPU/GiB rose 156.57% against the direct-only
binary, with zero warm storage-read bytes for both. The dispatch code is removed;
the added large-block corruption regression is retained and passes against the
selected direct implementation. Experimental f4a8 source, binary, passing
functional checks and unfavorable measurements remain in the HDD evidence.
The final rebuild's complete hash exactly matches the selected 252b binary.

The final cache-controlled NCX → selected LINK → LINK → NCX comparison passes
36 verified GETs and four clean shutdowns (`native-selected-ncx-reads172.log`).
Both warm phases record zero storage-read bytes. Warm throughput is
679.871 versus 606.772 MiB/s; CPU/GiB is 1.443 versus 1.857 seconds. Selected
LINK has 32.25% lower sampled process RSS but remains 10.75% slower on these
warm reads. Its SHA-256 integrity policy is retained. The separate controlled
f017-to-direct comparison measures 18.74% lower CPU/GiB and 2.84% lower
throughput. The report records these distinct comparisons without combining
them into an all-metric speedup. The final source-only checkpoint is
`restoration-checkpoint-20261102-07.tar.gz` on the HDD, with a separate SHA file;
the rejected worker experiment is separately archived as checkpoint 06.

## Earlier replacement-stage evidence

The following results predate infrastructure restoration and identify those
recorded binaries; they do not substitute for the current checks above.

| Area | Result | Evidence |
| --- | --- | --- |
| Complete workspace, warnings denied | 109 passed: 72 current engine; 9 historical proof-tool unit tests; 2 opt-in/helper tests ignored | `checkpoint03.log` |
| Strict Clippy, current engine/library/client/CLI targets | Passed with warnings denied | `clippy-final02.log` |
| Formatting | Passed after formatting automatic lint fixes | `format-final02.log` |
| Identity, inline bytes, terminal corruption | 14 passed | `link-format02.log` |
| New object engine incl. GC and scrub preemption | 27 passed | `store05.log` |
| Real native/HTTP/TLS/auth/job sockets | 7 passed | `jobs02.log` |
| Shared barrier failure/late-writer checks | 8 retained new-engine cases pass | `group02.log` |
| Required io_uring on PROJECT XFS HDD | 3,679 submissions/completions; no fallback or outstanding work | `uring-project-final01.log` |
| Required io_uring on TEST ext4 HDD | 3,679 submissions/completions; no fallback or outstanding work | `uring-test-final01.log` |
| Two-HDD encrypted recovery, older-point restore and retirement overlay | Exact 2,097,165-byte live object/hash; newer deletion and its old ticket rejected; all 64 prior retired keys remain retired | `recovery-hdd-01.jsonl` |
| Two-HDD native write/restart/deep verification | 64 objects, 53,194,752 source bytes passed | `headless-hdd-01-summary.json` |
| Two-HDD permanent retirement, SIGKILL after acknowledgement, restart | All 64 keys and old tickets rejected | `headless-hdd-01-retire-after-kill.jsonl` |

The physical witness used independent HDDs, 1D+1P, 256 KiB blocks, `io_uring`, and
a **debug** binary. Its mixed-operation timings include client report/manifest
persistence on an HDD. It is not a controlled performance benchmark or a
million-object/power-loss qualification. The binary hash is recorded. RAM tests
do not qualify disk independence or power-loss behavior.

Failed attempts are preserved, including two initial compile errors in owned IO
integration, the first batch sort compile, a deletion fault fixture that failed
before publication, nullable-job test assertion, concurrent CLI fixture collision,
and one post-restart probe that started before listener readiness. November 2
adds a mixed new/old backup run with two restore lease failures and an old online
boundary timeout (`retention-guard01.log`); those lease failures did not reproduce
in the subsequent combined new-engine suite, and their exact trigger is not
established. An unnecessary lockfile refresh was stopped and the pinned versions
restored. Initial dependency pruning missed the authentication HMAC dependency;
an adapted EC test required avoiding a Debug bound on its result; and a new
terminal test initially used a multipart ticket with the single-part PUT API.
The first preemption change made health polling repeatedly cancel read repair;
`checkpoint01.log` caught that real starvation regression. It was corrected and
`checkpoint02.log` passed. Synchronous restore now explicitly unlocks at completion
so duplicated/inherited descriptors cannot prolong its lease; a dedicated Linux
flock test covers that condition. It is consistent with the earlier transient
lease failures, without establishing their exact trigger. Strict Clippy initially
reported style and large-enum issues; those attempts and a later test-module
placement warning are preserved too. The first final-format check caught
indentation left by automatic lint fixes; formatting corrected it.
Their failed logs remain intact. Corrections and successful reruns use separate log names. Do not suppress their evidence.

Required work still open:

- Extend offline replacement beyond one missing member where the configured
  parity permits it. The present command requires all other peers. Codec loss
  tests include 4D+2P, while physical process tests use two HDDs in 1D+1P;
  broader multi-drive failure cases still need end-to-end qualification.
- Add the operator workflow to enroll another complete pool into an existing
  service, and test the full expansion path. Configuration, allocation and key
  resolution already support multiple pools; initial `init` enrolls one pool.
  Expansion leaves old pool identities, EC geometry, keys and placement intact.
  No redistribution or mutable-topology mechanism is required.
- Adapt the retained installer/UI, broader diagnostics/progress and remaining
  infrastructure tests; close the baseline-to-current responsibility inventory.
- Extend the completed download semantics and bounded prefetch qualification to
  saturated concurrency and wider physical failure topologies.
- Adapt the full retained acceptance/fault/chaos tooling. Retire namespace-only
  scenarios explicitly; preserve the baseline for historical comparison.
- Extend the measured sparse-tail and IO-coalescing improvements to aged
  populations, backup/restore allocation and final profile/default selection
  before claiming billion-object efficiency. Logical fixed-block padding remains;
  sparse physical representation does not change the immutable encoding.
- Run complete relevant checks, physical fault/restart/ENOSPC/EIO cases, backup
  and replication recovery proofs and million-object inventory/churn. Extend
  the completed serial old/new campaign to batch, saturation, aged namespaces
  and total-memory accounting.
- Keep operator/README/testing/release-contract documentation aligned with
  completed work; no release until qualification passes.

Persistent evidence: `/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports`.
Builds/RAM fixtures: `/tmp/n1-link-rc2-20261101-01` and `/tmp/n1-link-tests`.
Physical members: the PROJECT HDD and TEST HDD under that run's isolated roots.
MCH is unavailable; equivalent four-member hardware qualification remains open.

The workspace total includes nine unit tests in the historical release-proof
executable; these are not successful LINK API/chaos acceptance runs. Its scenario
port remains open. The ignored IO backend test was separately exercised on both
HDDs; its internal subprocess helper is intentionally not run as a standalone
qualification. The physical IO tests are functional witnesses, not controlled
latency benchmarks or physical power-loss evidence.

The earlier November 1 HDD recovery witness used its recorded debug hash before
the later admission/lease refinements. It ran with HTTP disabled and replicated
to another HDD using a different backup key. The current optimized witness above
separately covers the restored operation/recovery ownership, shutdown and
single-member replacement paths; neither establishes physical power-cut or
million-object qualification.
