# N1 RC.2 completion checklist

**Status: development, unreleased.** This file tracks current implementation and
qualification gaps. It does not redefine the API, prescribe deployment or serve
as a chronological work log. The product uses LINK object storage with an
optional metadata engine; see [ARCHITECTURE.md](../ARCHITECTURE.md).

**Promotion hold:** keep this checkpoint on `link`. The million-object campaign
and remaining crash/power-loss qualification are deferred until the MCH network
campaign. Existing process-kill/recovery passes remain valid for their stated
scope; they do not establish hardware power-loss resilience. Full-stack
performance remains a separate gate before promotion to `main`.

## Implemented components

| Component | Current implementation |
| --- | --- |
| LINK object engine | Deterministic placement in immutable pools; normal/batch/standard/durable multipart; verified GET/ranges; stat/health/repair; zero-residue purge; staging GC; paced maintenance; object backups/replication and explicit member recovery |
| N1 runtime infrastructure | Retained module structure, adaptive concurrency, I/O scheduler, io_uring, aligned O_DIRECT large uploads, middleware, server, streaming reads, banner and backup automation |
| Operator controls | INI/environment startup; IP/ports/TLS; immutable pool enrollment/append; concurrency, memory, I/O, rate, GC and healing controls |
| Metadata storage | Procyon optimistic transactions, SyncAll commits, cross-table snapshots, bounded workers and scans, crash/recovery and offline administration |
| Named metadata service | Logical fragments and permissions; pending upload identity; atomic named publication; max-ULID version selection; namespace/listings; rename/trash/recovery; native byte forwarding and original multipart geometry bridge |
| Public adapters | Main named NOA v1 HTTP/WebSocket and native operations; independent direct LINK NOA v2; original Console runtime/proxy retained |
| Exports | Version-pinned ZIP64/TAR construction, job state, range and signed downloads through the metadata service |
| Monitoring | Independently sampled metadata/object node health, cached filesystem/member capacity, bounded logical usage counters, explicit integrity checks and original Console service/pool panels |
| Metadata recovery | Operator-requested checksummed MVCC snapshots, scoped downloads, fresh-directory restore and matching paused-writer object/metadata recovery procedure |
| Console object recovery | Retained dedicated Shell operator contract; configured repository/replica/target aliases, explicit capture/verify/replicate/restore jobs using existing LINK recovery tools |
| Metadata maintenance | Indexed trash TTL and expired-upload reclamation; automatic/manual GC with bounded resources, durable retry and reference/pin protection; scoped integrity/healing; verified cross-EC pool migration with restart/stop recovery |

Implemented is not synonymous with fully qualified. [TESTING.md](../TESTING.md)
records completed checks and separates object-only, metadata-KV and full-stack
results. Each report identifies its source checkpoint and executable hashes;
the current work remains on `link` with `main` held. The completed local
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

## Work required before completion

| Area | Remaining work / acceptance condition |
| --- | --- |
| Full NOA v1 surface | Complete retained archive/manifest operations; verify remaining field-level responses, permissions, errors and retry meaning against the original standard. Scoped GC/healing/migration have explicit physical and browser coverage in the maintenance qualification. Registered route names alone are insufficient. |
| Export lifecycle | Complete and qualify retention/cleanup, restart behavior and resource limits for the complete named service. |
| Named-service recovery | Full metadata/staging loss, verified snapshot restore, named history/permissions/resume and metadata/object/both process crashes pass on two local HDDs. Preserve the documented operator pause; independent live captures are not an atomic cross-service snapshot. Qualify host power loss and additional crash points separately. |
| Metadata request quotas | Complete and verify the configured named-service admission/rate policy; do not infer coverage from object-engine middleware. |
| Optional Accel | Restore and qualify retained search/read integration when configured; preserve authoritative metadata resolution and direct-read fallback rules. |
| Console and administration | Exercise the original product workflows and deployment/administration helpers against both engines. Passing three original live tests is scoped evidence, not complete feature parity. |
| End-to-end population | Finish each requested 500K API and Console campaign with physical census, real process restart and every acknowledged version/payload verified. Keep failed attempts and distinct machine results separate. |
| Broader storage qualification | Two-HDD member loss/corruption, beyond-parity refusal, replica recovery after loss of both originals, replacement, INI expansion and database crash/churn pass. The EC subset matrix uses logical fixtures; qualify larger physical topologies, multiple-member replacement and hardware power loss separately. |
| Matched performance | The current key lifecycle record links original-NCX, previous-LINK and pre-optimization key-only ABBA benchmarks, plus standalone batch/multipart/concurrent upload. Record CPU/RSS, latency, throughput, allocation and every individual large-object sample. Begin now persists with the first durable payload by operator choice; a Rust reopen/readback probe verifies speculative XFS EOF allocation is gone. Earlier regressions remain in their original reports. Mixed and focused large-object results must both be reported; no universal performance claim follows. Named-pipeline, saturation and population measurements remain separate gates. |

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
