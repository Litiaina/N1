# N1 RC.2 completion checklist

**Status: development, unreleased.** This file tracks current implementation and
qualification gaps. It does not redefine the API, prescribe deployment or serve
as a chronological work log. The product uses LINK object storage with an
optional metadata engine; see [ARCHITECTURE.md](../ARCHITECTURE.md).

## Implemented components

| Component | Current implementation |
| --- | --- |
| LINK object engine | Deterministic placement in immutable pools; normal/batch/standard/durable multipart; verified GET/ranges; stat/health/repair; permanent key retirement; staging GC; paced maintenance; object backups/replication and explicit member recovery |
| N1 runtime infrastructure | Retained module structure, adaptive concurrency, I/O scheduler, io_uring, middleware, server, streaming reads, banner and backup automation |
| Operator controls | INI/environment startup; IP/ports/TLS; immutable pool enrollment/append; concurrency, memory, I/O, rate, GC and healing controls |
| Metadata storage | Procyon optimistic transactions, SyncAll commits, cross-table snapshots, bounded workers and scans, crash/recovery and offline administration |
| Named metadata service | Logical fragments and permissions; pending upload identity; atomic named publication; max-ULID version selection; namespace/listings; rename/trash/recovery; native byte forwarding and original multipart geometry bridge |
| Public adapters | Main named NOA v1 HTTP/WebSocket and native operations; independent direct LINK NOA v2; original Console runtime/proxy retained |
| Exports | Version-pinned ZIP64/TAR construction, job state, range and signed downloads through the metadata service |

Implemented is not synonymous with fully qualified. [TESTING.md](../TESTING.md)
records completed checks and separates object-only, metadata-KV and full-stack
results. The latest service source checkpoint is `9939ec8` on `link`.

## Work required before completion

| Area | Remaining work / acceptance condition |
| --- | --- |
| Full NOA v1 surface | Complete retained archive/manifest and fragment-scoped maintenance operations; verify field-level responses, permissions, errors and retry meaning against the original standard. Registered route names alone are insufficient. |
| Immutable-pool maintenance mapping | Resolve every retained maintenance operation without mutating existing pool geometry. The current redistribution rejection and generic job adapter are not full contract parity. |
| Metadata lifecycle / GC | Complete expired sessions, trash/history retention and explicit-key object reclamation with durable ownership/retry. A pending or live reference must never be discarded by guessing; LINK's staging GC cannot replace metadata reclamation. |
| Export lifecycle | Complete and qualify retention/cleanup, restart behavior and resource limits for the complete named service. |
| Named-service recovery | Coordinate a metadata database snapshot and matching LINK recovery points; test restore, reference integrity and replication. Object-only recovery points do not back up the namespace database. |
| Metadata request quotas | Complete and verify the configured named-service admission/rate policy; do not infer coverage from object-engine middleware. |
| Optional Accel | Restore and qualify retained search/read integration when configured; preserve authoritative metadata resolution and direct-read fallback rules. |
| Console and administration | Exercise the original product workflows and deployment/administration helpers against both engines. Passing three original live tests is scoped evidence, not complete feature parity. |
| End-to-end population | Finish each requested 500K API and Console campaign with physical census, real process restart and every acknowledged version/payload verified. Keep failed attempts and distinct machine results separate. |
| Broader storage qualification | Complete remaining multi-member loss/topology, expansion, crash/churn and fault scenarios. Distinguish process crashes from power loss and object-only from named-service evidence. |
| Matched performance | Measure CPU, RSS, latency and throughput on equivalent completed operations; retain cache/durability/integrity conditions and all failed trials. |

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
