# N1 and M1 RC.2 completion checklist

**Status: development, unreleased.** This file tracks current implementation and
qualification gaps. It does not redefine the API, prescribe deployment or serve
as a chronological work log. This repository contains the N1 LINK object store and the
M1 metadata store, backed by ProcyonDB; see [ARCHITECTURE.md](../ARCHITECTURE.md).

**Branch policy — November 7, 2026:** `main` is the mainstream N1 LINK + M1
development branch. The former NCX/NDX engine is the independent **N1 Lite**
product in `Litiaina/N1-Lite`, with its own engine and operational identities.
Promoting development to `main` does not release RC.2
or close any acceptance gate.

**Release hold:** the [November 8 full-stack million-object campaign](MILLION-OBJECTS-20261108.md)
passed on the main PC, with M1 on DATA_SSD and N1 on two XFS HDDs. Its complete
original-version/payload readback after killing and reopening both engines
closes the requested API population count for that checkpoint/profile.
Standalone-only and original Console population, MCH minimum-hardware and
remaining fault/power-loss qualification retain their separate gates. A
process-kill/recovery pass does not establish hardware power-loss resilience.
Full-stack performance and the completion gates below remain required before
releasing RC.2.

## Planned MCH hardening — November 11, 2026

The operator has designated MCH's four-physical-HDD machine as the minimum
supported hardware reference for RC.2 qualification. November 11, 2026
(Asia/Manila) is the planned hardening campaign; it is not a completed test or
a release deadline.

Close the implementation gaps below and qualify the final candidate through
all three paths: standalone N1, M1 → N1, and Console → M1 → N1. Cover the required
API and operator workflows, population, faults, recovery and matched performance.
Record the actual hardware, filesystems, EC layouts, database/control/member
placement, configuration, source commit and executable hashes for each run.

Every required gate needs an explicit result and retained evidence. Fix failures
and rerun the affected checks, followed by the complete candidate regression.
Verify acknowledged payloads and versions after faults; measure CPU, RSS,
latency, throughput and monitoring/maintenance overhead on the reference machine.
Keep process-crash and physical power-loss results separate. Do not omit required
features, relax integrity/durability, or silently change the deployment profile
to obtain a pass. RC.2 remains unreleased until the required gaps and release
gates are closed; a benchmark win or an upload count alone does not close them.

The final campaign must retain these separate acceptance results:

| Gate | Required evidence |
| --- | --- |
| Candidate identity | Clean source checkpoint, compiler/build flags, executable hashes, independent N1/M1 configuration, exact physical member/control/database placement |
| Contract and operations | Named HTTP/native and direct LINK operations; original Console workflows; permissions, refresh/revocation, IP/ports/TLS and effective concurrency/rate/GC/healing controls |
| Current-lifecycle scale | Main-PC API population passed at one million objects on November 8. Qualify the final candidate on MCH, the requested original Console 500K path and separate million-object standalone path; retain physical census, complete acknowledged-payload/version readback after restart, RSS/CPU and purge-residue checks |
| Storage faults | Four-drive physical 3+1 and 2+2 layouts, failures within/beyond parity, corruption detection, unavailable members, replacement and verified operator-driven repair; geometry remains immutable |
| Metadata and transactions | Lost metadata/staging, snapshot restore, pinned history/permissions, concurrent publication/GC/migration, interrupted purge and process crashes at named/object boundaries |
| Recovery and maintenance | Same/different-node object recovery, metadata backups, coordinated whole-product recovery, cross-EC migration and retry/stop; degraded or deferred diagnostics remain distinct from verified corruption |
| Performance | Matched old-N1-Lite, standalone LINK and M1 → N1 samples; uploads, reads/ranges, resume/status/finalize, batch, namespace mutations and maintenance; per-process CPU/RSS and latency distributions, including concurrent monitoring |
| Power loss | Separately identified operator-controlled host/power-loss tests and acknowledged-data verification; SIGKILL results cannot substitute |

Use task-private paths for qualification. Repeat affected failures, then the
complete candidate regression. Keep unsupported claims, unfinished runs and
fixture/setup errors out of pass counts while retaining their logs.

## Implemented components

| Component | Current implementation |
| --- | --- |
| N1 object store (LINK) | Deterministic placement in immutable pools; normal/batch/standard/durable multipart; verified GET/ranges; stat/health/repair; zero-residue purge; staging GC; paced maintenance; object backups/replication and explicit member recovery |
| N1 runtime infrastructure | Retained module structure, adaptive concurrency, I/O scheduler, io_uring, aligned O_DIRECT large uploads, middleware, server, streaming reads, banner and backup automation |
| Operator controls | INI/environment startup; IP/ports/TLS; immutable pool enrollment/append; concurrency, memory, I/O, rate, GC and healing controls |
| M1 database (ProcyonDB) | Procyon optimistic transactions, SyncAll commits, cross-table snapshots, bounded workers and scans, crash/recovery and offline administration |
| M1 named metadata service | Logical fragments and permissions; pending upload identity; atomic named publication; max-ULID version selection; namespace/listings; rename/trash/recovery; native byte forwarding and original multipart geometry bridge |
| Public adapters | Main named NOA v1 HTTP/WebSocket and native operations; independent direct LINK NOA v2; original Console runtime/proxy retained |
| Exports and direct archives | Retained version-pinned ZIP64/TAR writer; direct selected/manifest archives with bounded workers, cancellable capture and M1-owned crash cleanup; export job state, range and signed downloads |
| M1 request quotas | Shared physical-peer auth/global limits and canonical-fragment read/upload limits across HTTP/native; disabled policy skips admission work; forwarding headers cannot choose the peer |
| Optional Accel | Configured search and version-pinned reads after M1 authorization/resolution; origin recursion bypass, validated response geometry, pre-header fallback and terminal mid-stream failure |
| Monitoring | Independently sampled metadata/object node health, cached filesystem/member capacity, bounded logical usage counters, explicit integrity checks and original Console service/pool panels |
| Metadata recovery | Operator-requested checksummed MVCC snapshots, scoped downloads, fresh-directory restore and matching paused-writer object/metadata recovery procedure |
| Console object recovery | Retained dedicated Shell operator contract; configured repository/replica/target aliases, explicit capture/verify/replicate/restore jobs using existing LINK recovery tools |
| Metadata maintenance | Indexed trash TTL and expired-upload reclamation; automatic/manual GC with bounded resources, durable retry and reference/pin protection; scoped integrity/healing; verified cross-EC pool migration with restart/stop recovery |

Implemented is not synonymous with fully qualified. [TESTING.md](../TESTING.md)
records completed checks and separates object-only, metadata-KV and full-stack
results. Each report identifies its source checkpoint and executable hashes;
current development is on `main`, with RC.2 unreleased. The completed local
[November 4 reliability qualification](RELIABILITY-20261104.md) covers member
loss/corruption, full metadata loss and restore, crash/resume, encrypted-replica
recovery and actual OS unmount/remount. It is separate from population and
hardware power-loss qualification.

The [key lifecycle qualification](KEY-PURGE-20261105.md) covers the current
single-key upload contract and zero-residue purge. Earlier retirement-state
results remain historical; they do not qualify the current lifecycle by themselves.
The [aligned I/O qualification](DIRECT-IO-20261105.md) records the subsequent
large-upload change, unchanged shard format and lifecycle, physical proof,
repeated reliability checks and matched performance results, including costs
from cold post-upload reads/status and the measured CPU tradeoff.

The [November 8 million-object qualification](MILLION-OBJECTS-20261108.md)
passes fresh M1 → N1 population, two-million-shard census and complete original
payload/version readback after SIGKILL/reopen of both engines. It records the
SSD database/two-HDD pool profile, exact source/binaries, batch latency and
sampled per-process CPU/RSS. It is separate from the standalone-only, Console,
MCH topology and physical power-loss gates.

## Work required before completion

| Area | Remaining work / acceptance condition |
| --- | --- |
| Full NOA v1 surface | Direct archive/manifest routes are implemented. Complete field-level responses, permissions, errors and retry qualification against the original standard. Known-length native artifact downloads stream; unknown-length direct archive responses retain the native 8 MiB control bound. Scoped GC/healing/migration have explicit physical and browser coverage in the maintenance qualification. Registered route names alone are insufficient. |
| Export lifecycle | Complete retained artifact TTL cleanup, retained-byte accounting and restart queue admission; qualify download/cleanup races and shutdown/restart/source pins under load. Direct archive crash cleanup and terminal source-error propagation are separate implemented checks. |
| Named-service recovery | Full metadata/staging loss, verified snapshot restore, named history/permissions/resume and metadata/object/both process crashes pass on two local HDDs. Preserve the documented operator pause; independent live captures are not an atomic cross-service snapshot. Qualify host power loss and additional crash points separately. |
| Metadata request quotas | Implemented and locally checked across HTTP/native, token changes and spoofed forwarding headers. Qualify final-candidate defaults, sustained overload, idle bucket reclamation, TLS/proxy deployment and impact on the MCH workload. Do not infer coverage from object-engine middleware. |
| Optional Accel | Real Accel integration and fault-response checks are implemented. Qualify deployment TLS/CA configuration, sustained load, restart and the complete conditional/range/header matrix; preserve authoritative M1 resolution and fallback rules. |
| Console and administration | Exercise the original product workflows and deployment/administration helpers against both engines. Passing three original live tests is scoped evidence, not complete feature parity. |
| End-to-end population | API M1 → N1 passed one million fresh objects on the main PC at `8dbba32`, with physical census, real process restart and every acknowledged version/payload verified. Complete the requested original Console 500K campaign and final-candidate MCH qualification; the separate standalone-only million-object gate remains open. Keep failed attempts and distinct machine results separate. |
| Broader storage qualification | Two-HDD member loss/corruption, beyond-parity refusal, replica recovery after loss of both originals, replacement, INI expansion and database crash/churn pass. The EC subset matrix uses logical fixtures; qualify larger physical topologies, multiple-member replacement and hardware power loss separately. |
| Matched performance | The current key lifecycle record links original-NCX, previous-LINK and pre-optimization key-only ABBA benchmarks, plus standalone batch/multipart/concurrent upload. Record CPU/RSS, latency, throughput, allocation and every individual large-object sample. Begin now persists with the first durable payload by operator choice; a Rust reopen/readback probe verifies speculative XFS EOF allocation is gone. Earlier regressions remain in their original reports. Mixed and focused large-object results must both be reported; no universal performance claim follows. November 8 records sustained named-pipeline population metrics, not a matched old/new comparison. Matched named-pipeline, saturation and MCH tail-latency acceptance remain open. |

The November 7 preparation screen and controlled local follow-up are recorded
in [TESTING.md](../TESTING.md). The initial ABBA screen measured 14.1% more small
upload time. The follow-up holds the running N1, metadata database and pool
constant, alternates binaries and quotas, retains each request latency, audits
placement and profiles durable calls. Upload averages reverse across campaigns;
M1 listener buffering and request-path limiter maintenance were corrected, but
neither an upload speedup nor tail-latency parity is claimed.
Both the original and follow-up measurements remain available. The subsequent
November 8 full-stack population pass is recorded separately. Remaining
standalone/Console population, saturation, MCH hardware and power-loss gates
are not closed by an average improvement or by this population pass.

## Fixed boundaries

- Object storage never imports the metadata database or owns names, namespace
  history or listings. All object operations remain available to an authorized
  direct client holding its LINK keys.
- Metadata completes named N1 transactions through the public native object
  interface. It must not require changing LINK placement or reducing Console
  features to match an incomplete metadata implementation.
- Pool membership and EC profiles are immutable. New capacity is a complete new
  pool. Changing operator IP, TLS or concurrency does not change object identity.
- NOA v1 and v2 are both current contracts. Neither is a promise of old NCX/NDX
  disk-format compatibility. RC.2 is still in development.
- Success requires the prescribed integrity/durability barriers. Do not weaken
  checks, retry away corruption or hide partial failures to obtain a pass.

Historical implementation notes and exact earlier checkpoints are preserved at
`9939ec8:docs/RC2-IMPLEMENTATION.md` in Git and in the reports indexed by
[TESTING.md](../TESTING.md). They are not competing descriptions of the current
product. No release follows from a source checkpoint or an unfinished campaign.
