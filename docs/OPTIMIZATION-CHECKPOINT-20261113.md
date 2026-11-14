# N1 optimization checkpoint — November 13, 2026

Work is paused at the operator's request. The optimization is experimental:
correctness checks pass, but no throughput or CPU improvement has been measured
for the new binary. Do not promote it as a performance-qualified replacement.

**Continuation:** the operator authorized resuming on the four-HDD reference system on November 13.
See [Four-HDD reference system optimization and RC.2 continuation](FOUR-HDD-OPTIMIZATION-20261113.md) for
the active run. The paused state and SSD/HDD results below remain the historical
checkpoint; they are not the four-HDD reference system benchmark results.

## Preserved change

Base commit: `3d6e1a3e74d7c0c7a5bc7649b77a85067f379f8d`.

Only three implementation/test files differ from that base:

- `object_engine/src/core/upload/direct.rs`
- `object_engine/src/core/upload/stream.rs`
- `object_engine/src/core/tests/direct_io.rs`

The current writer can read and allocate zero-filled buffers for sparse regions
of a pre-sized staged shard before writing new frames. The experimental writer
queries the first sparse interval when opening a large part's existing direct
descriptor. Encoder groups wholly inside that interval skip the old-frame read.
The part lock excludes competing writes to the same part. Other ranges retain
the original checksum-valid frame comparison, including partially written
retries. Unsupported extent queries fall back to that reader; actual extent
I/O failures propagate. Memory is bounded by pool width, not object length.

The optimization applies to the existing large direct-I/O path. Small uploads
retain the buffered path and do not allocate a sparse-interval vector. LINK
identity, shard format, EC, SHA-256/CRC32C policy, finalization ordering, payload
durability and purge behavior are unchanged. M1 and Console are unchanged.

## Completed validation

The SSD test system compiled the candidate using the existing locked, offline
release toolchain and dependency cache, with its build target and data on
DATA_SSD.

| Check | Result |
| --- | --- |
| Workspace formatting | Passed |
| N1 library/tests strict Clippy | Passed |
| N1 release library suite on DATA_SSD | 218 passed, 0 failed, 2 explicit ignored cases |
| Direct-I/O tests on local physical XFS HDD | 4 passed, 0 failed |
| Production release binary | Built successfully |

The HDD invocation includes the opt-in test proving that an interior payload
MiB bypasses page cache and remains readable after reopen. It also exercises
shared boundary pages, concurrent out-of-order multipart, retry conflicts,
restart and existing-only direct opens. These fixtures share one physical HDD;
they do not assert independent physical member redundancy.

The new test interrupts a partially written part, reopens the store, rejects a
conflicting retry, fills the other part, resumes the original bytes, finalizes,
downloads and checks every byte. It passes in standard and durable modes.

The ignored `purge_child` is a subprocess helper exercised by its parent tests.
The other default exclusion is the physical page-cache check run separately
above. Existing successful million-object campaigns are preserved and have not
been repeated or reclassified as evidence for this experimental binary.

Candidate binary SHA-256:
`0895d43e691315e3f69dfa3bf058089e555808279dca1f7aea311f33c43eb4c2`.

## Diagnostics and setup corrections

One 15 GiB upload, verified GET and purge completed on the unchanged current
LINK binary while sampled with perf. This run is diagnostic only: stack
sampling adds substantial overhead, so its timing and CPU totals must not be
compared to unprofiled benchmarks. The trace contains zero lost samples.

The original reports' increased CPU and variable 15 GiB SSD readback remain
unresolved. The sparse-read inefficiency is established by the writer's code;
its share of those regressions has not been established. The SSD test system has
different CPU core types, and the diagnostic used CPUs 0–11. Affinity control is
a diagnostic condition, not an engine performance improvement.

An initial diagnostic driver omitted its empty output directory and failed
before uploading. That was corrected in `profile-baseline02.sh`. The first
candidate build exposed an async closure borrowing error, corrected before the
successful build. A second build driver had an incorrect root substitution;
`build03.sh` is the successful driver. Earlier logs and scripts are retained.
No failed candidate was deployed to an interactive service.

Portable perf and its libraries were extracted into the task's DATA_SSD tools
directory. No global package, kernel setting, service or CPU policy was changed.
No Python was used.

## Evidence and restart point

Primary local evidence:
`/mnt/HDD1TB001/n1-large-optimization-20261113-01`.

SSD test system evidence: `/mnt/DATA_SSD/n1-large-optimization-20261113-01`.

`remote-reports/` on the local HDD contains the SSD test system's logs and raw
perf trace. `artifacts/` contains the candidate, exact pre-change current
binary, tested Rust executable and unchanged Rust benchmark client.
`checkpoint/` contains the base commit, source patch and SHA-256 manifests. The
portable checkpoint under
`/srv/test-evidence/portable/n1-optimization-20261113-01` carries the complete
source snapshot, locked vendored dependencies (including ECS), resume notes,
patch, logs, raw profiling trace, previous benchmark evidence, profiler packages
and compressed binaries. It contains no build cache or object dataset. The
entire continuation can proceed without accessing the SSD test system or
DATA_SSD. No test, build, benchmark or profiler remains running.

### Continuing without the SSD test system

The portable folder is on this laptop and is the continuation source at work.
DATA_SSD is an additional preserved copy, not a required dependency. The folder
is approximately 104 MiB before small manifest/note updates. Full task evidence
also remains on the two local HDDs.

Verify and unpack onto a new the four-HDD reference system HDD directory,
keeping compilation and test data off the two-HDD test system’s source NVMe:

```sh
CAPSULE=/srv/test-evidence/portable/n1-optimization-20261113-01
# Copy this folder to the four-HDD reference system first, then set CAPSULE to its location on that host.
bash "$CAPSULE/unpack.sh" /mnt/DISK0/n1-optimization-20261113-01
cd /mnt/DISK0/n1-optimization-20261113-01/source
export CARGO_TARGET_DIR=/mnt/DISK0/n1-optimization-20261113-01/target
export TMPDIR=/mnt/DISK0/n1-optimization-20261113-01/tmp
mkdir -p "$TMPDIR"
cargo build --release -p n1 --bin litiaina-n1 --locked --offline
```

`unpack.sh` verifies relative checksums, refuses an existing destination and
generates a Cargo configuration pointing to the restored vendor directory.
Offline dependency resolution is verified with an empty temporary Cargo home; no
dependency on DATA_SSD's Cargo cache is required. On the four-HDD reference
system, rebuild each comparison variant from its preserved source using the same
native toolchain. The tested SSD test system binaries may require a newer glibc
than the four-HDD reference system. The copied profiler packages are also
host-specific; do not assume they match the four-HDD reference system.

The source snapshot contains the experimental candidate. To obtain its exact
pre-change comparison source, make a separate copy and reverse the preserved
`optimization.patch` there with `git apply --reverse`; keep the candidate copy
intact. Do not substitute binaries built for different hosts into a matched
performance comparison.

The SSD test system's configuration names and absolute service paths in the old
logs are historical evidence. Provision fresh isolated configuration and
certificates on the four-HDD reference system; do not reuse DATA_SSD paths or
change an existing interactive deployment.

Resume in this order:

1. Audit the sparse-range shortcut and run the preserved A/B/B/A standalone
   comparison on SSD 1+0 and the two independent HDDs in 1+1. Measure upload,
   part latency, init/finalize, CPU, RSS and verified GET. Preserve every sample.
2. Run the prepared small-object A/B/B/A blocks in both modes. Concatenate
   individual samples for p50/p95/p99; do not average block percentiles.
3. Diagnose GET with repeated reads of the same immutable object across binary
   switches, recording physical reads and separating cache conditions. Merely
   reading different newly uploaded objects does not isolate the read path.
4. Use unprofiled runs for performance acceptance. Report affinity-controlled
   diagnostics separately from the ordinary deployment configuration. If the
   candidate regresses, continue diagnosis rather than accepting the regression.
5. After the optimization is demonstrated, continue the remaining RC.2
   qualification on the four-HDD reference system's four physical disks. Do not restart completed unrelated
   populations or claim host power-loss proof from process-restart tests.

`source/compare.sh` is prepared but has **not** been launched. It expects RAM
copies named `litiaina-n1-current`, `litiaina-n1-optimized` and
`throughput_bench`, plus the existing isolated test configuration. Recheck
paths, free space and drive health on the next host before starting it. Do not
carry the old test root paths blindly to the four-HDD reference system.
