# N1 testing

This is the current testing guide for N1's LINK object engine, metadata engine
and original Console integration. It records evidence, not the architecture or
operator procedure. Product behavior is defined by [NOA.md](NOA.md); remaining
completion gates are in [docs/RC2-IMPLEMENTATION.md](docs/RC2-IMPLEMENTATION.md).

## What each test proves

| Layer | Required checks | Does not prove |
| --- | --- | --- |
| LINK/object engine | Key encoding and placement, EC/integrity, upload/finalize idempotence, exact GET/ranges, repair, retirement, GC and recovery | Names, metadata transactions or Console behavior |
| Procyon/metadata storage | Concurrent writers/readers, atomic cross-table updates, snapshots, crash/recovery, conflict handling | Object payload durability or the NOA service contract |
| Metadata service → object engine | Pending identity before bytes, durable object completion before named publication, version/name semantics and exact reads after restart | Every Console workflow or full retained API parity |
| Original Console → metadata → object engine | Real production UI/API requests, original upload schemes, resume, listings and downloads | An API simulated by mocked responses, or untested administrative features |
| Scale/fault campaign | Every acknowledged version retained, physical placement census, zero unexpected staged/unreferenced files, process crash/reopen and complete readback | Power-loss, cold-cache performance or topology failure modes that were not injected |

Workloads and verification are Rust. Original Console tests use its existing
Node/Playwright tooling and a real headless Chromium browser. No Python is used
in the current qualification campaigns. Keep data, logs and build output on the
selected HDD/RAM locations; never put population fixtures on the source NVMe.

## Reproduce workspace checks

Set `CARGO_TARGET_DIR` and `TMPDIR` to explicit task directories on storage with
adequate space before invoking these commands:

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo test -p n1_metadata_engine -p procyon-engine --doc --locked
cargo clippy -p n1 -p n1_metadata_engine -p cli_core --all-targets --no-deps --locked -- -D warnings
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
  captured successful rows from a retained failed attempt.
- `metadata_engine/examples/qualification_summary.rs`: read-only summary of
  existing latency/resource evidence; it cannot manufacture a campaign pass.
- Existing object-engine physical/recovery tests and the native client's Rust
  benchmark/resume examples retain their separate purposes.

The original Console repository remains separate. Run its Node checks, static
validation and existing `tests/live-local.spec.ts` against a real metadata
service. Do not replace production Console features or change its expectations
to obtain a pass.

## Recorded evidence as of November 3, 2026

The service checkpoint is `9939ec8` on `link`. Source/executable hashes and
exact environments in each report identify the tested snapshot. Counts from
different runs are not added together as new distinct tests.

| Campaign | Recorded result | Scope and evidence |
| --- | --- | --- |
| Complete local workspace | 414 passed, zero failed; nine explicit helper/physical exclusions | [Full-stack report](/mnt/PROJECT-DISK-1TB/n1-ini-restoration-20261103-01/reports/FULL-STACK-QUALIFICATION-20261103.md) |
| Post-fix metadata/native checks | 14 metadata library and three native-client cases passed; strict metadata/shared-config Clippy passed | Same report; `checkpoint-metadata104.log`, `checkpoint-object-client105.log`, `checkpoint-clippy103.log` |
| Original Console regression | Three live cases and 38 Node cases passed; static validation had zero errors/warnings | Same full-stack report; existing production Console/runtime/proxy preserved |
| Corrected local two-HDD smoke | API 1,024 objects and original Console 1,000 objects passed census and every post-SIGKILL payload/version read | Same report, separate fresh datasets |
| MCH component regression and physical API | 246 passed, zero failed; eight explicit helper/physical exclusions; physical API witness and 1,024-object API smoke passed | [MCH full-stack report](/mnt/PROJECT-DISK-1TB/n1-mch-full-stack-20261103-01/reports/MCH-FULL-STACK-QUALIFICATION-20261103.md) |
| Direct LINK 500,000-object campaign | Passed 500,000 durable ACKs, all exact full GETs after restart and a 1,000,000-shard census | [Object-only 500K report](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/HEADLESS-500K-20261102-20261103.md); a separate earlier object-only snapshot |
| Procyon metadata foundation on MCH HDDs | One million KV entries, 100K mutations, concurrent snapshots and two SIGKILL/resume cycles passed; other drives passed independent smaller campaigns | [Metadata KV report](/mnt/PROJECT-DISK-1TB/n1-mch-metadata-20261103-01/reports/MCH-METADATA-QUALIFICATION-20261103.md); not object uploads |
| Current API and Console 500,000-object campaigns | In progress at the source checkpoint; no full-stack 500K pass recorded here | [Headless handover](/mnt/PROJECT-DISK-1TB/n1-mch-full-stack-20261103-01/reports/HEADLESS-HANDOVER-20261103.md) and the terminal files it identifies |

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

Compare the same workload, integrity/durability policy, hardware, concurrency,
cache state and completed operation semantics. Report upload/download latency,
throughput, CPU and sampled RSS separately. A metadata KV batch latency is not a
per-object upload latency; CDP heap is not total browser RSS; a process SIGKILL
is not a physical power cut. Permanent LINK retirement is not the same operation
as moving a named object to trash.

The direct old/new comparison is retained in
[NCX-VS-CRC32C-LINK-20261103.md](/mnt/PROJECT-DISK-1TB/n1-link-rc2-20261101-01/reports/NCX-VS-CRC32C-LINK-20261103.md).
It does not prove a win in every metric or the performance of the later metadata
stack. Full product NOA conformance and the remaining fault/topology/recovery
gates must pass before RC.2 is released.

Earlier chronological test notes are preserved at `9939ec8:TESTING.md` in Git
and in their linked HDD reports. They describe their recorded snapshots, not the
current architecture or an accumulated claim that every gate has passed.
