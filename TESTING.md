# N1 and M1 testing

This is the current testing guide for the **N1 LINK object store**, the
**M1 metadata store backed by ProcyonDB**, and original Console integration.
It records evidence, not the architecture or
operator procedure. Product behavior is defined by [NOA.md](NOA.md); remaining
completion gates are in [docs/RC2-IMPLEMENTATION.md](docs/RC2-IMPLEMENTATION.md).

Current Cargo packages are `n1` (object store) and `m1` (metadata store); their
server binaries are `litiaina-n1` and `litiaina-m1`. Historical evidence retains
the names and executable hashes used when each campaign ran. Renaming does not
extend those results to a new binary or replace its regression checks.

## What each test proves

| Layer | Required checks | Does not prove |
| --- | --- | --- |
| LINK/object engine | Key encoding and placement, EC/integrity, upload/finalize idempotence, exact GET/ranges, repair, purge, GC and recovery | Names, metadata transactions or Console behavior |
| Procyon/metadata storage | Concurrent writers/readers, atomic cross-table updates, snapshots, crash/recovery, conflict handling | Object payload durability or the NOA service contract |
| Metadata service → object engine | Pending identity before bytes, durable object completion before named publication, version/name semantics and exact reads after restart | Every Console workflow or full retained API parity |
| Original Console → metadata → object engine | Real production UI/API requests, original upload schemes, resume, listings and downloads | An API simulated by mocked responses, or untested administrative features |
| Scale/fault campaign | Every acknowledged version retained, physical placement census, zero unexpected staged/unreferenced files, process crash/reopen and complete readback | Power-loss, cold-cache performance or topology failure modes that were not injected |

Workloads and verification are Rust. Original Console tests use its existing
Node/Playwright tooling and a real headless Chromium browser. No Python is used
in the current qualification campaigns. Keep data, logs and build output on the
selected HDD/RAM locations; never put population fixtures on the source NVMe.

## N1/M1 naming verification — November 6, 2026

The formalized `n1` / `m1` workspace passed **474 default tests**, with **37
explicit opt-in/helper cases ignored**, and **81 documentation tests**. Scoped
strict Clippy and formatting checks passed. Three explicit two-XFS-HDD service
cases additionally verified M1 → N1 operations/restart, monitoring with metadata
backup/restore, and finite/unlimited size policy with isolated secrets.

The service checks assert both signature banners, M1's HTTP/native identity,
`m1_config.ini` / `m1.env` defaults and independent N1/M1 monitoring labels.
Evidence is under `/mnt/HDD1TB001/n1-m1-naming-20261106-01/reports`.
These are functional checks of the naming/readability checkpoint, not new
throughput, million-object or hardware power-loss results. The MCH promotion
hold remains in the completion checklist.

## Key-only lifecycle qualification — November 5, 2026

The current upload/purge contract uses one LINK key, existing-only continuation
and zero per-identity residue after acknowledged purge. The
[key lifecycle record](docs/KEY-PURGE-20261105.md) links its source, regression,
crash/recovery and matched benchmark evidence. Earlier results below describe
their original checkpoints, including the former permanent-retirement contract.

That upload-optimization checkpoint passed **472 default workspace tests**, with
**36 explicit opt-in/helper exclusions**, **81 documentation cases**, strict
scoped Clippy and **13 selected two-HDD service/native cases**. Five persistence
cases and eleven purge cases were also repeated on PROJECT. Matched standalone
benchmarks include Begin in PUT timing and verify complete payload hashes and
zero identity residue after purge. Their CPU/RSS, allocation and timing results,
including regressions, are under
`/mnt/PROJECT-DISK-1TB/n1-upload-optimization-20261105-01/reports`.
The operator selected allocation-only Begin; an unused key may disappear on
power loss before its first durable payload. This run does not repeat 500K or
million-object population or simulate host power loss.

## Earlier metadata maintenance qualification — November 5, 2026

The [maintenance qualification](docs/MAINTENANCE-20261105.md) covers indexed
trash TTL GC, unfinished-upload expiration, explicit integrity/healing and
verified migration between immutable pools with different EC geometry.

- **456 default workspace tests**, **81 documentation tests**, and scoped
  strict Clippy passed. The default run leaves **33 opt-in/helper cases ignored**;
  ignored cases are not passes.
- **18 explicit two-HDD service tests passed**, including all six new maintenance
  cases. The original Console drove real migration and GC against both engines;
  Rust checked the resulting payloads, versions and permanent retirements.
- The Console desktop suite passed **151 tests, 6 skipped**. The focused four
  service/maintenance cases passed across all five viewport profiles (**20
  executions**, overlapping desktop coverage). All **38 Node tests** passed.
- Metadata library tests cover claim/recovery races, shared references, pins,
  transactional placement switches and reads resolved before source retirement.
  Idle GC index checks used 10,000 future trash rows; they are a bounded-query
  regression, not a matched throughput or memory comparison with old N1.

Evidence, binaries, original failure logs and browser screenshots are under
`/mnt/PROJECT-DISK-1TB/n1-maintenance-20261104-01`. Data/control and the first
member used PROJECT; the second member used TEST. Builds stayed on PROJECT and
temporary unit fixtures used RAM. No object-engine or LINK source changed.

Reproduce the physical service run using distinct directories for control and
the first member (never the same parent), an unchanged tested object binary,
the original Console build and installed Chromium:

```sh
export N1_OBJECT_TEST_ROOT=/path/on/project/control-fixtures
export N1_OBJECT_TEST_FIRST_ROOT=/path/on/project/first-members
export N1_OBJECT_TEST_SECOND_ROOT=/path/on/test/second-members
export N1_OBJECT_TEST_BINARY=/path/to/litiaina-n1
export N1_CONSOLE_REPO=/path/to/console
export N1_CONSOLE_TEST_DIST=/path/to/built-console
export N1_BROWSER_NODE=/path/to/node
export PLAYWRIGHT_BROWSERS_PATH=/path/to/installed-browsers
cargo test --release -p m1 --test noa_service --locked -- \
  --ignored --nocapture --test-threads=1 \
  --skip running_deployment_ --skip monitoring_performance:: \
  --skip real_second_hdd_unmount_degraded_reads_and_remount_keep_every_named_ack
```

The OS-unmount case was deliberately excluded while the user's independent
deployment used the same HDD. It passed in the preceding qualification below.
This maintenance run does not rerun 500K population or certify hardware power
loss. The complete physical invocation and its terminal result are in
`reports/physical06.log`; failed attempts remain separate.

## Earlier reliability qualification — November 4, 2026

The local two-HDD campaign **full03 passed at 21:19:44 Asia/Manila**, after
fixing two metadata read-path defects and repeating the complete campaign.
The [reliability record](docs/RELIABILITY-20261104.md) gives coverage,
reproduction commands, failure evidence and explicit limits.

- **447 default workspace cases and 81 documentation cases passed.**
- **18 distinct opt-in Rust tests passed in 21 executions**, including repeated
  loss/crash cases. These are separate from the default count.
- One default test checks **410 EC loss/corruption combinations**. They are not
  410 additional Rust test functions or 410 physical disk failures.
- Physical named-service tests cover both member-loss directions, corruption,
  full metadata/staging loss and snapshot restore, in-flight process crashes,
  native independence, replica recovery after both original members are lost,
  replacement, INI expansion, GC and actual OS unmount/remount of the TEST HDD.
- Each HDD independently passed a 16,384-entry metadata campaign with 4,096
  mutations and two actual SIGKILL/resume cycles. These are KV fixtures, not
  object payload counts or a replicated metadata database.

Source/build/data evidence is under
`/mnt/PROJECT-DISK-1TB/n1-reliability-20261104-01`; its
`reports/RELIABILITY-20261104.md` and `reports/full03/PASSED` identify the final
result. Metadata/control and one object member shared the PROJECT HDD; the
other member used the independent TEST HDD. Unit fixtures and the separate
Console operator control fixture used RAM. The object executable was unchanged
between full02 and full03; both runtime fixes are in metadata.

This campaign does not rerun a 500K population, claim hardware power-loss
coverage, or close every RC.2 completion gate. Earlier scale results and failed
attempts below retain their own scope and source checkpoints.

## Reproduce workspace checks

Set `CARGO_TARGET_DIR` and `TMPDIR` to explicit task directories on storage with
adequate space before invoking these commands:

```sh
export N1_LINK_TEST_ROOT="$TMPDIR/n1-link-tests"
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo test -p m1 -p procyon-engine --doc --locked
cargo clippy -p n1 -p m1 -p cli_core -p n1_shell --all-targets --no-deps --locked -- -D warnings
```

Use `--offline` only when the locked dependencies are already available.
Physical tests and subprocess helpers marked ignored require their documented
environment or supervisor; an ignored case is not a pass. Inspect the test
summary and opt-in physical witness logs separately. The scoped Clippy command
above does not claim that imported Procyon or the historical release-proof tool
passes a whole-workspace lint invocation.

The source contains the following Rust physical harnesses:

- `metadata_engine/examples/hdd_qualification`: concurrent metadata KV
  population, churn, SIGKILL/resume and exact cross-table verification.
- `metadata_engine/examples/noa_qualification`: real API or original Console
  uploads through both services, frozen executable/source provenance, ACK
  journals, process crashes, physical census and exact version/payload readback.
- `metadata_engine/examples/failed_batch_audit.rs`: read-only recovery audit of
  every journaled ACK and captured successful mixed-batch row from a retained
  failed attempt, using the original executables. It never retries failed rows.
- `metadata_engine/examples/qualification_observer.rs`: read-only Linux process,
  wait-channel, I/O-pressure and device samples alongside a supervised campaign.
  It neither calls N1 nor changes service or host tuning.
- `shell/tests/operator_live.rs`: opt-in real original Console backup controls,
  dedicated HTTP operator host, native object capture/replication, exact restored
  payload and permanent retirement. Rust owns the isolated services and oracle;
  the Console's `n1-link-backups.live.mjs` only drives the browser. Requires
  `N1_OPERATOR_TEST_ROOT` (owner-only RAM/HDD parent), `N1_OBJECT_TEST_BINARY`,
  `N1_CONSOLE_REPO`, `N1_BROWSER_NODE`, built Console assets and Chromium.
  Invoke `cargo test -p n1_shell --test operator_live -- --ignored --nocapture`.
- `metadata_engine/examples/qualification_summary.rs`: read-only summary of
  existing latency/resource evidence; it cannot manufacture a campaign pass.
- `metadata_engine/examples/upload_comparison.rs`: matched native LINK,
  original NCX/NDX and current named metadata uploads with the same payload,
  strict durability, part size and concurrency. Samples CPU/RSS, process I/O
  and physical disk counters. `--mode ncx` uses the original named HTTPS API
  with its unmodified engine binary. `--access-file` selects that private
  endpoint/fragment configuration; `--session-mode standard|durable` selects
  the existing upload scheme. `--full-download` checks the complete named GET
  length and source SHA-256, outside upload timing. Without that flag, three
  64 KiB range checks are explicitly sampled verification. All expected source
  hashing occurs before timers. Set the measured server PID(s); omit
  `--metadata-pid` for the original combined engine.
- `metadata_engine/examples/upload_audit.rs`: inspect a stopped metadata copy
  without printing object capabilities. `recover_upload.rs` is an explicit
  offline operator recovery for a cancelled session whose verified native parts
  survived failed cleanup. It requires the original LINK key, unexpired upload and
  matching reservation; dry-run precedes `--apply`. It cannot recreate shards
  or revive a purged object key.
- Existing object-engine physical/recovery tests and the native client's Rust
  benchmark/resume examples retain their separate purposes.

The original Console repository remains separate. Run its Node checks, static
validation and existing `tests/live-local.spec.ts` against a real metadata
service. Do not replace production Console features or change its expectations
to obtain a pass.

The `noa_service` integration target also covers optional object-size policy.
Its resume witnesses cover strict standard and durable uploads, truncated bodies,
conflicting retries, bitmap-page boundaries, SIGKILL of both engines, repeated
finalize, exact readback, staged shard loss and cancellation cleanup. The scale
witness reserves a 15 GiB identity and transfers **1 GiB** (128 x 8 MiB parts)
to the two HDDs. It measures reconstruction, indexed status and status after
restarting both processes, asserting zero native I/O submissions for indexed
queries. This is a resume-control test, not a complete 15 GiB transfer. The
131,072-part metadata limit is separately exercised without allocating payloads.

`finite_and_unlimited_policy_on_real_metadata_and_object_engines` checks finite
rejection on known-length, chunked, batch and multipart uploads, then byte-exact
transfers with omitted/explicit unlimited caps and 15 GiB/1 TiB multipart
init/retry/status/cancel. Large initialization is admission testing, not a
claim that those payload sizes were transferred. Supply `N1_OBJECT_TEST_ROOT`
for control/database evidence and `N1_OBJECT_TEST_BINARY` for the actual object
binary; optional `N1_OBJECT_TEST_FIRST_ROOT` and `N1_OBJECT_TEST_SECOND_ROOT`
put shard members on separate physical disks. Run the ignored witness explicitly.

`running_deployment_unlimited_upload_smoke` targets an already running private
test deployment only when explicitly invoked with `--ignored`. It requires
`N1_SIZE_TEST_ACCESS_FILE` (JSON with `metadata_url`, `fragment`, `fragment_key`)
and `N1_SIZE_TEST_CA`. It uploads and verifies two small payloads, cancels its
large sessions, and moves its completed fixtures to named trash. Optional
`N1_SIZE_TEST_REPORT` records the result without credentials.

The separately ignored `running_deployment_large_multipart_round_trip` requires
the same access/CA inputs plus an explicit `N1_SIZE_TEST_BYTES` above 5 GiB.
It streams deterministic, part-indexed payloads through strict durable 32 MiB
parts, repeats finalize, and compares every downloaded byte using bounded
buffers. It retains its named fixture and records its path in the optional
report. This is the actual large-payload witness; the admission test above does
not replace it.

`interrupted_upload_listing_and_resume_survive_metadata_restart` is another
explicit physical witness in `noa_service`. It checks normal and durable
session discovery behind completed/cancelled records, truthful pagination,
status after restart, completion from missing parts and exact downloaded bytes.
The original Console's `tests/live-reload-resume.spec.ts` starts a real upload,
reloads after two acknowledged parts, finds Resume in the Uploads modal and
checks that only missing parts are sent, with no cancellation request. The
production Console runtime is unchanged by this regression test.

## Earlier monitoring and recovery checkpoint — November 4, 2026

The later monitoring work remains on `link`; `main` is held. Authoritative
evidence is on `altear@192.168.254.107` under
`/mnt/DATA_SSD/n1-local-test-20261103-01/health-monitoring-01`. Task-owned HDD
roots are under `/media/altear/PROJECT-1TB/n1-monitoring-20261104` and
`/media/altear/TEST-1TB/mx-tests/n1-monitoring-20261104`; whole drives were not
reformatted. Workstation browser output uses RAM; builds/reports use DATA_SSD.

The complete workspace passed **440 cases** (`workspace-final13.log`), and
scoped strict Clippy passed. The ordinary run leaves opt-in physical/helper
cases ignored; these are not counted as passes. Separate completed witnesses
include two-HDD service monitoring/outage/metadata restore, matched transfers,
matching named metadata/object recovery, process-crash/retirement/member
recovery, actual INI pool expansion, and explicit io_uring with no fallback.
Their raw files are `physical-final.log`, `combined-recovery-final.log`,
`object-chaos-final.log`, `ini-physical-final.log` and `uring-final.log`.

Two physical machines also passed live Console service monitoring and object
outage tests; matching hostnames were distinguished using kernel boot IDs.
Native encrypted backup transfer crossed those nodes and the destination
verified the point. A replacement on a different device preserved the logical
member identity; an opt-in scrub repaired deliberate corruption with read
repair disabled, followed by exact 8,388,627-byte GET verification. Terminal
JSON files name each scope explicitly.

The full desktop Console suite passed **150 cases**, with six opt-in cases
skipped. Monitoring/backups passed **30 checks across all five viewport
profiles**; these overlap the desktop suite rather than adding 30 new cases.
All **38 Node cases** and static validation passed. The separate Rust
`operator_live` witness drives the actual Console through its dedicated Shell
host: capture, verify, native replication, restore, exact payload and retirement,
plus truthful failures for a running offline source and the last recovery point.
Its RAM control fixture is distinct from the physical backup/replacement tests.

The eight-writer full-stack attempts **failed** on 30-second object write
admission while strict filesystem flushes held all six per-fragment slots.
Read-only audits verified every **52,480** and **56,832** respective journaled
ACKs plus each retained mixed response's **192** successful rows, with exact
versions/bytes after restart. Neither failed population is a 500K pass.
The separate default four-writer 500K API campaign also **failed**, on its
300-second HTTP request budget. A read-only restart audit verified all
**101,120 journaled ACKs / 1,412,456,800 bytes**, with zero payload/version
mismatches. No complete response was captured for the timed-out batch, so its
unacknowledged outcomes are not guessed. The gated Console phase never started.
Eight readers perform post-restart readback, not simultaneous fresh population.

The Rust client's original 30-second TCP user timeout was also diagnosed and
aligned with its existing 300-second request budget. A fresh 4,096-object
strict/restart/full-read smoke passed, but the later eight-writer server
admission failure remains recorded. An experimental per-file persistence path
was rejected after its HDD stall and the original persistence code restored.
A subsequent bounded per-file candidate also failed its 4,096-object smoke
on admission and was rejected and reverted. A read-only restart audit checked
all 128 captured successful rows / 1,787,920 bytes; there were no complete
journaled batches in that attempt. Neither experiment is deployed.
The subsequent gated profile used the original batch persistence with database,
control and ACK journals on DATA_SSD and one member on each HDD. It explicitly
sets metadata admission to four while retaining eight upload clients, the
object engine's independent admission policy and all strict barriers/deadlines.
This is a different placement/admission profile, not a retroactive pass for
the failed HDD-database campaigns. Its 4,096-object smoke passed: 8,192 shards,
no unexpected staging/pending/unreferenced files and every original version and
all 57,213,440 bytes verified after both engines restarted. At that checkpoint,
API 500K was active and Console 500K was gated on its complete pass. This
checkpoint does not establish their later terminal outcomes.
`qualification-ssd21.sh` owns the sequential headless runs;
`qualification-evidence21.sh` retains resource summaries and separately audits
ACKs if the API run fails. Both temporary user units use `Restart=no`.
No server deadline or durability policy was weakened. The full current report
is `WHOLE-N1-MONITORING-RECOVERY-20261104.md` in the remote evidence directory.

## Recorded evidence as of November 3, 2026

The earlier full-stack campaigns below use checkpoint `9939ec8` on `link`. Source/executable hashes and
exact environments in each report identify the tested snapshot. Counts from
different runs are not added together as new distinct tests.

The later local size-policy and credential-provisioning checks on
`192.168.254.107` passed: 421 workspace cases, 81 storage documentation cases,
both explicit metadata-to-object HDD witnesses, and all three original Console
live cases. A separate real **15 GiB** strict durable upload used 480 parts and
passed repeated finalize and complete byte-for-byte download verification.
The **1 TiB** checks covered initialization, retry, status and cancellation.
Custom secrets files, environment-only startup and rejection of insecure files
also passed. Raw evidence, binaries and the current operator paths are under
`/mnt/DATA_SSD/n1-local-test-20261103-01/reports/UNLIMITED-UPLOADS-20261103.md`
on that host. This does not change the completion status of the earlier
population campaigns.

| Campaign | Recorded result | Scope and evidence |
| --- | --- | --- |
| Complete local workspace | 414 passed, zero failed; nine explicit helper/physical exclusions | [Full-stack report](/mnt/PROJECT-DISK-1TB/n1-ini-restoration-20261103-01/reports/FULL-STACK-QUALIFICATION-20261103.md) |
| Post-fix metadata/native checks | 14 metadata library and three native-client cases passed; strict metadata/shared-config Clippy passed | Same report; `checkpoint-metadata104.log`, `checkpoint-object-client105.log`, `checkpoint-clippy103.log` |
| Original Console regression | Three live cases and 38 Node cases passed; static validation had zero errors/warnings | Same full-stack report; existing production Console/runtime/proxy preserved |
| Corrected local two-HDD smoke | API 1,024 objects and original Console 1,000 objects passed census and every post-SIGKILL payload/version read | Same report, separate fresh datasets |
| MCH component regression and physical API | 246 passed, zero failed; eight explicit helper/physical exclusions; physical API witness and 1,024-object API smoke passed | [MCH full-stack report](/mnt/PROJECT-DISK-1TB/n1-mch-full-stack-20261103-01/reports/MCH-FULL-STACK-QUALIFICATION-20261103.md) |
| Direct LINK 500,000-object campaign | Passed 500,000 durable ACKs, all exact full GETs after restart and a 1,000,000-shard census | [Object-only 500K report](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/HEADLESS-500K-20261102-20261103.md); a separate earlier object-only snapshot |
| Procyon metadata foundation on MCH HDDs | One million KV entries, 100K mutations, concurrent snapshots and two SIGKILL/resume cycles passed; other drives passed independent smaller campaigns | [Metadata KV report](/mnt/PROJECT-DISK-1TB/n1-mch-metadata-20261103-01/reports/MCH-METADATA-QUALIFICATION-20261103.md); not object uploads |
| API and Console 500,000-object campaigns at that checkpoint | In progress at the source checkpoint; no full-stack 500K pass recorded in that snapshot | [Headless handover](/mnt/PROJECT-DISK-1TB/n1-mch-full-stack-20261103-01/reports/HEADLESS-HANDOVER-20261103.md) and the terminal files it identifies |

Headless terminal JSON files and raw logs are authoritative for completion after
this status snapshot. A populated dataset or upload progress line is not a
terminal pass. The Console population follows only after its API campaign passes
census and complete post-restart reads. Local and MCH campaigns are independent.

## Failure retained and corrected

An earlier named API batch returned 128 successes and 128 backend errors, so
that campaign failed. Metadata had retained pooled native sockets beyond the
independent object's idle timeout. The metadata pool now discards aged idle
transports before reuse; a deterministic regression checks replacement/login.
The corrected physical API and smaller end-to-end smokes passed afterward.

The separate Rust audit reopened the exact failed snapshot/binaries and checked
all 128 captured successful payloads and original version IDs without PUT/retry:
1,787,920 verified bytes, zero payload/version mismatches. The original mixed
batch remains a failure. Raw failures and source/binary hashes remain in the
full-stack report; they are not relabelled as scale passes.

## Benchmark and release limits

The [aligned direct-I/O qualification](docs/DIRECT-IO-20261105.md) records the
current large-upload writer, physical page-cache proof, repeated fault/recovery
checks and matched buffered-LINK/NCX comparisons. It includes per-phase CPU/RSS,
latency, allocation and adverse measurements. The two current HDD mounts are
`/mnt/HDD1TB001` and `/mnt/HDD1TB002`; source/build/data evidence lives on HDDs.

Compare the same workload, integrity/durability policy, hardware, concurrency,
cache state and completed operation semantics. Report upload/download latency,
throughput, CPU and sampled RSS separately. A metadata KV batch latency is not a
per-object upload latency; CDP heap is not total browser RSS; a process SIGKILL
is not a physical power cut. Permanent LINK retirement is not the same operation
as moving a named object to trash.

The direct old/new comparison is retained in
[NCX-VS-CRC32C-LINK-20261103.md](/mnt/HDD1TB001/n1-xfs-rebenchmark-20261105-01/evidence-before-format/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/NCX-VS-CRC32C-LINK-20261103.md).
It does not prove a win in every metric or the performance of the later metadata
stack. Full product NOA conformance and the remaining fault/topology/recovery
gates must pass before RC.2 is released.

Earlier chronological test notes are preserved at `9939ec8:TESTING.md` in Git
and in their linked HDD reports. They describe their recorded snapshots, not the
current architecture or an accumulated claim that every gate has passed.
