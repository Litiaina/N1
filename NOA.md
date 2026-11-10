# Node Object API (NOA)

NOA defines the public operation contracts of the two systems in this repository.
**M1**, the metadata store with its own transactional database, serves **NOA v1** for named objects.
**N1**, the LINK object store, serves **NOA v2** for direct object keys. Neither
replaces the other. API and wire versions are independent of the binary version
in Cargo metadata. N1 RC.2 is still in development; full product conformance has
not yet been established.

For request/response fields, executable examples and complete client workflows:

- **[M1 API reference and examples](docs/api/M1.md):** fragment authentication,
  named uploads/resume, reads, listings, trash, archives, monitoring and backups.
- **[N1 API reference and examples](docs/api/N1.md):** native LINK client,
  upload/resume/finalize, verified reads, physical purge and operator recovery.

This document remains the shared standard and operation inventory. The guides
describe current handlers and identify implementation limits explicitly.

| Client or component | Connects to | Contract |
| --- | --- | --- |
| Console and applications using fragments, names and versions | M1 | NOA v1, HTTP(S)/WebSocket; named native wire `n1/0.5` |
| M1 forwarding object bytes | N1 | NOA v2 over native `n1/1` |
| Applications managing their own metadata and LINK keys | N1 directly | NOA v2 over native `n1/1`; optional HTTP adapter |

Native URLs are `n1://host:port` or certificate-verified `n1s://host:port`.
The selected engine determines the wire schema; `n1/0.5` and `n1/1` are distinct
protocols. The LINK SDK/shell targets the object engine. The original Console
targets metadata and retains its named-object operations.

Console's dedicated object-backup operator connection is a separate
administration boundary on the repository host. N1 Shell provides
`POST /noa/v1/maintenance/backups/operation` with its own operator token,
exact-origin allowlist and configured aliases. It delegates object capture and
replication to NOA v2 and offline recovery to the existing backup tools.
Fragment tokens do not authorize this endpoint. Metadata snapshots remain
metadata-service NOA v1 operations. See [the operator guide](DOCUMENTATION.md)
for configuration and coordinated recovery.

## NOA v1 — M1 metadata store

This is the retained named-object standard under `/noa/v1`. It is a requirement
of M1, not an old-storage compatibility mode. The inventories
below retain the original contract; missing implementation must not be hidden
by deleting operations from this document.

Use `Authorization: Bearer <access-token>` on protected HTTP calls. Fragment
permissions are independently `read`, `write`, `modify` and `delete`. Signed
URLs grant narrowly scoped download authority rather than a reusable fragment
token. Names are flat, prefixes are virtual, empty mkdir is rejected, and prefix
mutations return per-child results rather than promising one atomic tree move.
Here `object_key` means the logical name; it is not a LINK `N1A1.` key.

The named native interface uses a four-byte big-endian MessagePack control-frame
length. Requests carry a string `id`, `op`, optional JSON-string `payload`,
and either an embedded byte `body` or a `body_len` followed by raw bytes.
Responses carry the matching ID, `ok`, optional status/payload/error and body
metadata. These are the existing named frames in
[types.rs](metadata_engine/src/server/native/types.rs). They must not be decoded
using the LINK frame schema below.

For example, after obtaining a fragment access token, upload a named file:

```bash
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $M1_TOKEN" -H 'Content-Type: text/plain' \
  --data-binary @hello.txt \
  "$M1_BASE/noa/v1/upload/one-shot/docs/hello.txt?durability=strict"
```

Success is HTTP 201 with `object_key`, `object_id`, `version_id`, `content_type`,
`total_size_bytes`, `created_at` and `created_at_unix`. Follow the
[complete M1 walkthrough](docs/api/M1.md#create-a-fragment-and-log-in) to create
a fragment, obtain the token, resume multipart and use the other operations.

### Connectivity And Authentication

```text
GET  /noa/v1/ping
POST /noa/v1/auth/fragment
POST /noa/v1/auth/refresh
POST /noa/v1/auth/logout
GET  /noa/v1/ws?token=<access-token>
```

### Fragments And Capacity

```text
POST   /noa/v1/fragments
GET    /noa/v1/fragments
GET    /noa/v1/fragments/check
GET    /noa/v1/fragments/hash
PATCH  /noa/v1/fragments/{fragment}
DELETE /noa/v1/fragments/{fragment}
GET    /noa/v1/capacity/usage
GET    /noa/v1/capacity/fragment
```

Named capacity responses distinguish bounded transactional live-byte counts
from cached physical object-filesystem capacity. `measurement=not_initialized`
means unknown logical usage, never zero measured usage.

### Monitoring and metadata snapshots

`GET /noa/v1/maintenance/health` and
`GET /noa/v1/maintenance/fragments/{fragment}/health` return
`components.metadata` and `components.object`, each with its own node, process,
metrics and observation age. Fragment scoping is authorized before observing.
The existing `metadata` and `object` role keys identify M1 and N1 respectively:
`components.metadata.product` and its `database` are `M1`, and
`components.object.product` is `N1`.
`GET /noa/v1/maintenance/pools/health` reports native object-pool members and
capacity. Normal calls never scan payloads. `deep_scan=true` explicitly starts
one asynchronous integrity check; subsequent `deep_scan=false` requests read
its progress. `refresh=true` cannot bypass sampling admission. The explicit
`live_counters=true` global health query reads current in-memory object I/O
counters for diagnostics, retaining cached filesystem observations.

Metadata snapshots require master-key operator authorization:

```text
GET  /noa/v1/maintenance/metadata/backups
POST /noa/v1/maintenance/metadata/backups
GET  /noa/v1/maintenance/metadata/backups/{id}/download
POST /noa/v1/maintenance/metadata/backups/{id}/signed-url
GET  /noa/v1/maintenance/metadata/signed-backup?token=SCOPED_GRANT
```

Named native operations are `maintenance.health`, `maintenance.pools.health`,
`metadata.backups.list`, `metadata.backups.create` and
`metadata.backups.download` (`snapshot` supplies the ID). Creation returns an
accepted job, listing exposes its state, and download streams a completed
snapshot. These are metadata database snapshots; object payload backup remains
the independent object engine's recovery protocol.

### POSIX-Like Namespace

```text
GET  /noa/v1/posix/list
GET  /noa/v1/posix/stat
POST /noa/v1/posix/mkdir
POST /noa/v1/posix/rename
POST /noa/v1/posix/move
POST /noa/v1/posix/trash
POST /noa/v1/posix/recover
```

### Objects And Trash

```text
GET    /noa/v1/objects
GET    /noa/v1/namespace/snapshot
GET    /noa/v1/namespace/search
GET    /noa/v1/trash
GET    /noa/v1/objects/meta/{object_key}
GET    /noa/v1/objects/versions/{object_key}
GET    /noa/v1/objects/get/{object_key}
GET    /noa/v1/objects/stream/{object_key}
GET    /noa/v1/objects/download
GET    /noa/v1/objects/download/chunk
POST   /noa/v1/objects/signed-url
GET    /noa/v1/objects/signed-read
DELETE /noa/v1/objects/{object_key}
POST   /noa/v1/objects/recover/{object_key}
POST   /noa/v1/objects/rename
POST   /noa/v1/objects/bulk-delete
POST   /noa/v1/objects/bulk-recover
POST   /noa/v1/trash/recover-all
POST   /noa/v1/objects/archive
POST   /noa/v1/objects/archive/manifest
POST   /noa/v1/exports
GET    /noa/v1/exports/{export_id}
GET    /noa/v1/exports/{export_id}/download
POST   /noa/v1/exports/{export_id}/signed-url
GET    /noa/v1/signed-exports/download
DELETE /noa/v1/exports/{export_id}
```

Deletion remains soft-only. TTL lifecycle owns final reclamation; NOA v1 does
not add empty-trash, hard-delete, or per-version-delete semantics.

Snapshot is a read-authorized NDJSON projection feed with a completion marker.
Search proxies to optional Accel. Optional `N1_ACCEL_READ_URL` also accelerates
version-pinned object bytes after M1 authorization/resolution, including existing
Console media URLs. Pre-header failure falls back to N1; mid-stream failure
terminates the response. Accel never becomes namespace or durability authority.
Exports pin immutable versions, build durable ZIP/TAR artifacts and support
stable length/ETag, range resume and scoped signed downloads. Cancellation and
artifact retention do not delete source objects.

A successful complete named download requires a successful object-engine read,
including for an empty object. Metadata verifies a zero-length backend read
before returning empty success headers. For a nonempty streamed response, a
backend failure after headers terminates the body with an error; receiving a
200 header alone does not establish a complete download. These rules apply to
named HTTP and native byte reads.

### Uploads

```text
POST   /noa/v1/upload/one-shot/{object_key}
POST   /noa/v1/upload/batch
POST   /noa/v1/upload/multipart/init/{object_key}
GET    /noa/v1/upload/multipart
GET    /noa/v1/upload/multipart/{version_id}
PUT    /noa/v1/upload/multipart/{version_id}/{part_index}
POST   /noa/v1/upload/multipart/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/{version_id}

POST   /noa/v1/upload/multipart/durable/init/{object_key}
GET    /noa/v1/upload/multipart/durable
GET    /noa/v1/upload/multipart/durable/{version_id}
PUT    /noa/v1/upload/multipart/durable/{version_id}/{part_index}
POST   /noa/v1/upload/multipart/durable/{version_id}/{session_id}/finalize
DELETE /noa/v1/upload/multipart/durable/{version_id}
```

Both multipart routes support interruption and resume. Durable multipart always
persists accepted parts; standard multipart does so when strict durability is
selected. Relaxed standard acknowledgements do not guarantee crash persistence.
Both publish immutable objects after finalization.

For native-aligned named uploads with strict acknowledgements, the metadata
engine commits completed-part bits after the object engine confirms persistence,
before returning the part ACK. Status reads those bounded metadata pages, rather
than reading the payload. Restarting either engine retains this completion state.
An unindexed session reconciles its durable ACK records with actual shard
records. Valid bytes without a recorded durable ACK are retransmitted safely;
they do not establish that an interrupted sync succeeded. Relaxed sessions
continue to inspect actual progress.
Unaligned uploads retain their existing staged-part checks. Status describes
acknowledged completion, while native finalization and object reads enforce
payload integrity. A native finalize failure invalidates the completion index so
the next resume reconciles actual parts. Publication and cancellation remove
completion pages atomically with their metadata lifecycle transition.
Batch accepts independent small objects, returning 201 for all-success or 207
for mixed results. There is no durable batch route.

### Maintenance And Object Health

```text
GET  /noa/v1/maintenance/health
GET  /noa/v1/maintenance/pools/health
GET  /noa/v1/maintenance/fragments/{fragment}/health
GET  /noa/v1/objects/status/{object_key}
POST /noa/v1/objects/repair/{object_key}

POST /noa/v1/maintenance/fragment/init
POST /noa/v1/maintenance/fragment/cleanup

POST /noa/v1/maintenance/repair/start
POST /noa/v1/maintenance/repair/stop
GET  /noa/v1/maintenance/repair/status
POST /noa/v1/maintenance/fragments/{fragment}/repair/start

POST /noa/v1/maintenance/gc/start
POST /noa/v1/maintenance/gc/stop
GET  /noa/v1/maintenance/gc/status
POST /noa/v1/maintenance/fragments/{fragment}/gc/start

POST /noa/v1/maintenance/redistribution/start
POST /noa/v1/maintenance/redistribution/stop
GET  /noa/v1/maintenance/redistribution/status
POST /noa/v1/maintenance/fragments/{fragment}/redistribution/start
POST /noa/v1/maintenance/objects/redistribution/start/{object_key}
```

Metadata maintenance starts return a persisted job record: admission is not
completion. Poll the matching status route. Phases are running, stopping,
interrupted, completed, stopped and incomplete, with scanned/processed/failure/
deferred counters and a last error. Status includes effective metadata policy.
Running/interrupted jobs resume after process restart. Stop is cooperative and
operator-only.

An unqualified start targets the authenticated fragment. The fragment path
selects that fragment; another fragment or `all_fragments=true` requires operator
authority. Starts require write/modify/delete. Native maintenance operations
use the same domain behavior and scalar parameters as these HTTP routes.

Both automatic and explicit metadata GC enforce configured trash TTL.
`dry_run=true` observes eligible work without deleting objects. There is no TTL
bypass or empty-trash action. Expired unfinished upload cleanup protects completed
versions. Object staging GC remains an independent NOA v2 operation.

Redistribution requires distinct enrolled `source_pool` and `target_pool` IDs;
the target must be unsealed. Optional `object_key` selects one logical object's
versions in the selected fragment. Migration verifies a new destination LINK
identity before switching metadata, then purges the unreferenced source. EC
topologies may differ; logical versions/ETags do not change. This is explicit
same-object-service pool migration. `dry_run=true` selects without copying.
Retry incomplete migration with its original parameters, or stop it to clean up
unswitched destinations.

`batch`, `object_concurrency`, `pause_millis` and `bytes_per_second` are bounded
by service policy. Metadata healing uses a sequential object worker and retains
the object's repair admission. `mode=check` or `dry_run=true` checks without
repair; other retained repair modes invoke healing. Fast mode is rejected and
migration always uses strict durability.

### Conformance

Maintenance starts require write/modify/delete. Fragment tokens are scoped to
their own fragment; explicit all-fragment work, restrictive modes and global
stops require master-key operator authentication. Pool content diagnostics stay
read-authorized and fragment-scoped. The metadata implementation must preserve these authorization scopes.

Conformance requires matching request meaning, authentication, responses,
errors and retry behavior, not just route names. A registered route or a passing object-engine test does not establish metadata
conformance. Current evidence and remaining gates are in [TESTING.md](TESTING.md)
and [the completion checklist](docs/RC2-IMPLEMENTATION.md).

## NOA v2 — N1 object store with LINK

This contract performs direct object storage. Its typed operations are shared
by the object engine and native SDK in [link/src/noa.rs](link/src/noa.rs).
Native `n1/1` is primary; HTTP is a secondary adapter to the same handlers.
LINK object keys are client-held capabilities, bound to a stable
object-service fragment ID and checked permissions. This service does not open
the metadata database.

Example native control map, encoded as MessagePack after its four-byte length:

```json
{
  "id":1,
  "command":{"op":"upload.status","args":{
    "object_key":"<server-issued-LINK-key>","start":0,"limit":4096
  }},
  "body_len":0
}
```

This runs on an authenticated N1 connection. Its response contains a bounded
page with `finalized`, `finalized_shards`, `parts`, `checked_start`,
`checked_end`, `missing_parts` and `unavailable_members`. The
[N1 guide](docs/api/N1.md) includes authentication, every command's fields,
response examples and an executable native Rust upload/resume/read/purge client.

### Native framing

Each control frame is a four-byte unsigned big-endian length followed by a
MessagePack map. Requests contain `id` (u64), `command` and `body_len` (u64,
default zero). Command is `{op, args}`. Successful responses contain the same ID,
`status:200`, nullable `result`, nullable `error`, and `body_len`. Errors use a
non-200 status and no raw response body. A nullable result is also used when no
maintenance job exists.

Exactly `body_len` raw bytes follow a control frame. Payload is never base64 or
nested JSON text inside MessagePack. Control maps are bounded (64 KiB before
authentication, 8 MiB after), with aggregate native control-memory admission.
Parts and batches are limited to 1 GiB per request; normal upload streams may
exceed that within the format's calculated file-offset limits. Control and body
inactivity deadlines do not impose a fixed maximum object-transfer duration.

Connections are ordered request/response streams. Authentication establishes the
connection session. After a partial exchange, reconnect before retrying. Upload
retries reuse the same LINK key, not a newly initialized identity. The client must retain
issued keys; N1 does not provide a list from which to recover them.

### Authentication

| Operation | Arguments | Result |
| --- | --- | --- |
| `ping` | none | Product, LINK, NOA and wire versions; public |
| `auth.fragment` | `fragment`, `secret`, optional `permissions`, `ttl_seconds` | Signed token, session, rotating refresh token and refresh lifetime |
| `auth.bearer` | `token` | Verified connection session |
| `session.refresh` | `refresh_token` | Rotates refresh token and returns new access token/session |
| `auth.logout` | none | Durably revokes refresh session and clears native connection session |
| `session.info` | none | Current session |

Permissions are `read`, `write`, `delete` and `repair`. Requested permissions are
intersected with configured fragment permissions. Login lifetimes are bounded by the configured maximum
(up to 86,400 seconds) and checked for expiry. Explicitly scoped login requests do
not receive operator authority. Maintenance mutation/job operations require an
operator session. Alias or credential changes do not change fragment identity or
LINK placement. The service signing secret is not a fragment password.

HTTP uses a Bearer token on each protected request. Logout does not revoke an
already issued stateless token; stop using it or let it expire.
Refresh tokens are single-use. Reusing a rotated token durably revokes its session
family, including across engine restart. Refresh intersects current fragment
permissions and rejects credential rotation; it cannot elevate a scoped session.
Service recovery starts fresh login sessions.

Both transports share the retained configurable IP/authentication and fragment
quotas. Rejected quotas return 429. HTTP uses the actual socket peer address;
untrusted forwarded-address headers cannot change the quota identity.

### Object operations

| Operation | Arguments | Permission / body |
| --- | --- | --- |
| `upload.init` | optional `pool`, `total_bytes`, `part_bytes`, `durable`, `ttl_seconds` | write; no body |
| `upload.part` | `object_key`, byte `offset` aligned to part size | write; exact part bytes |
| `upload.put` | `object_key` for a single-part/empty object | write; exact total bytes |
| `upload.batch` | `object_keys` in body order | write; concatenated single-part bodies |
| `upload.status` | `object_key`, `start`, `limit` (1..4096) | read or write; validates a page of actual parts |
| `upload.finalize` | `object_key` | write; no body |
| `upload.cancel` | `object_key` | write; removes unfinished staging only |
| `object.stat` | `object_key` | read; size, EC representation and integrity root |
| `object.get` | `object_key`, `offset`, optional `length` | read; exact raw response bytes |
| `object.signed_read` | `object_key`, `offset`, optional `length`, `ttl_seconds` | read; client-held range/expiry-bound grant |
| `object.get_signed` | `grant` | verified grant; raw signed range, no connection login required |
| `object.health` | `object_key`, `deep` | read; object-specific integrity/availability |
| `object.repair` | `object_key` | repair; replaces damaged/missing shards |
| `object.delete` | `object_key` | delete; physical purge to absence |
| `object.bulk_delete` | explicit `object_keys` (1..256) | delete; per-key results |

Begin returns one `object_key` (`N1A1.`), counts, geometry, expiry and durability
after incomplete headers are created on all members. Begin acknowledges
allocation, not payload persistence. Before the first durable payload reply,
power loss may remove an unused allocation; its old key then returns 404.
The `durable` flag governs part acknowledgements. A durable part persists its
header, payload and complete staging ancestry on every member before replying.
Normal/batch PUT and finalize persist the header with durable publication.
There is no upload
ticket or manifest. Writes open existing incomplete shards; only a fresh Begin
creates them. A normal/batch object requires `part_bytes >= total_bytes` and EC
alignment. Empty objects have zero parts and no body. Batches contain at most
256 keys, 1 GiB of body and 1,024 calculated open shard descriptors; duplicate
identities are invalid. The LINK key stays identical at finalization.

Standard part replies do not promise persistence. Durable part replies require
persistence on every member. Finalization, normal upload and batch success always
require durable object publication. Identical retries are idempotent; conflicting
valid bytes return 409. Lost replies may leave partially or fully published
objects; resolve each identity with status/retry. Batch is not a multi-object
transaction. Expiry does not garbage-collect a readable final object.

Stat reports `LINK-SHA256-tree-v2` for new hybrid pools (CRC32C frames and
identity/position/length-bound source SHA-256), and `LINK-SHA256-tree-v1` for
existing SHA-256 pools. These roots must not be labelled as SHA-256 of the raw
complete file. GET verifies frames and reconstructed
source before emitting requested bytes. Range bounds are exact and rejected
when outside the object; successful range output has exactly the declared length.
Signed grants last at most six hours and never outlive the issuing session token.
They bind fragment, key, offset, length and expiry; changing those fields invalidates
the signature. Signed access grants no other operation or connection authority.
Current fragment read permission and object existence still apply.

Delete removes incomplete/final shards and per-identity staging directories.
`removed_shards` counts member positions with newly removed files; repeated
purge can return zero. Success requires all members and persistence barriers.
No per-identity record remains. Old write/status/finalize/GET return 404 and
cannot recreate missing data. Only Begin generates a fresh random identity.

Interrupted deletion has no success acknowledgement. Remaining shards do not
encode intent; they may still be readable with sufficient EC quorum. Metadata
keeps its durable reclamation claim/key until native deletion succeeds and
retries after restart. Direct clients must retain and retry their own pending
deletions. The object engine has no rename, namespace listing, object history
or undelete operation; those belong to metadata.

### Maintenance

| Operation | Arguments | Access |
| --- | --- | --- |
| `maintenance.health` | none | read; backend, scheduler, system/capacity and maintenance state |
| `maintenance.pools` | none | read or write; immutable geometry and available slots |
| `maintenance.gc` | none | operator; starts conservative staging GC |
| `maintenance.scrub` | `options:{repair,bytes_per_second,window_seconds}` | operator; starts a paced job |
| `maintenance.job` | none | operator; current/last bounded job state |
| `maintenance.stop` | `job_id` | operator; requests cooperative stop |

A job progresses through running/stopping to completed, stopped or incomplete.
An incomplete or stopped job does not mark N1 failed. IO already admitted is
joined before ownership is released. Foreground work can preempt background work
on its object. Optional interval schedules and startup delays live in private
operator INI configuration. Staging GC uses [garbage_collector] intervals.
Scheduled healing requires [auto_healing] enabled with an explicit UTC schedule;
its persisted attempted-slot journal prevents duplicate admission.

### HTTP adapter

| Endpoint | Encoding |
| --- | --- |
| `GET /health` | Public version response |
| `POST /noa` | JSON request/response, no raw body |
| `PUT /noa/upload/{object_key}/bytes/{offset}` | Exact Content-Length, raw part body |
| `POST /noa/stream` | Exact Content-Length; native length-prefixed MessagePack request followed by raw upload bytes; JSON response |
| `GET /noa/object/{object_key}?offset=0&length=...` | Raw verified response, exact Content-Length |
| `HEAD /noa/object/{object_key}` | Authenticated object headers without downloading payload |
| `GET /noa/signed` | Signed grant query fields; raw verified signed range |
| `GET /noa/backup/receive/{repository}` | Operator-authenticated WebSocket, binary recovery protocol |

Streaming HTTP control is bounded to 64 KiB. Authentication uses the HTTP Bearer
header. Both upload entry points call the same object handlers as native NOA.
There is no native-to-HTTP bridge.
Ordinary object GET supports a single HTTP `Range`, `ETag`, `If-Range`,
`If-None-Match` and `If-Match`, returning 206/304/412/416 as appropriate. Byte
range ends and suffix lengths extending past the object are clamped to its
available bytes. An exact strong ETag satisfies `If-Range`; a mismatch, weak
tag or date validator causes a full response. Empty objects use an empty 200
response. HEAD evaluates the same ETag preconditions without reading payload.
An explicit signed/query range cannot be combined with a second `Range` header,
including when `If-Range` mismatches. Downloads use `Cache-Control: private,
no-store`. The integrity ETag is the immutable LINK tree root.

### Recovery

All recovery operations require an operator token. Requested/scoped permission
tokens do not inherit operator privileges. `repository` and `destination` name
preconfigured aliases; callers cannot submit filesystem paths or encryption keys.

| Operation | Arguments | Result |
| --- | --- | --- |
| `backup.list` | `repository,after,limit` | Bounded page of point summaries; limit 1..256, after empty or canonical ULID |
| `backup.capture` | `repository,boundary_seconds` | Starts committed online capture; boundary 1..300 seconds |
| `backup.verify` | `repository,snapshot` | Starts authenticated point verification |
| `backup.replicate` | `repository,destination,snapshot` | Starts resumable local point replication |
| `backup.prune` | `repository,keep_last,protect` | Starts retention; keep 1..10000, retain protected point and at least one recovery point |
| `backup.gc` | `repository,discard_incomplete` | Starts block reclamation; incomplete transfers protected unless explicitly discarded |
| `backup.job` | none | Current/last bounded recovery job status |
| `backup.stop` | `job_id` | Requests cooperative stop |
| `backup.receive` | `repository` | Upgrades an authenticated native connection to `LINK-replication-v1` |

Job starts return a job record, not a claim that the work has completed. Poll
`backup.job` for completed/stopped/incomplete status and its result or error.
One recovery job runs at a time; running/stopping overlap returns 409. Stop does
not release repository leases while accepted filesystem work is still running.
Restore remains an offline operator helper operation with explicit targets.

### Status meanings

400 invalid encoding, parameters or range; 401 missing/invalid authentication;
403 denied permission; 404 absent object/upload/job; 409 conflicting immutable
bytes, expiry or active-job conflict; 413 body
limit; 422 verified format/integrity failure; 503 unavailable quorum, incomplete
purge, exhausted admission or incomplete operation; 507 ENOSPC/EDQUOT.
A storage error is not converted into success because a final filename exists.

Full product conformance also requires the named metadata contract above.
See [the completion checklist](docs/RC2-IMPLEMENTATION.md) for outstanding work.

## Current compliance boundary

The contract above describes required behavior. The current metadata service
implements the main fragment, namespace, upload, versioned-read and export
paths, with integration and original-console tests. It is **not yet a full
NOA v1 conformance pass**. Specific remaining differences include:

- Retained export artifact expiry/cleanup, disk accounting and restart admission
  still require completion and qualification. Direct ZIP64/TAR and streamed TAR
  manifest routes are implemented; their bounded-worker, disconnect, process-crash
  cleanup and source-failure tests are separate from retained artifact lifecycle.
- Named native responses without a known content length retain the 8 MiB
  control-response bound. Large native archive consumers use the export-job
  download path, whose durable artifact has a known length. Unbounded direct
  archive streaming is not implied by registration of a native operation name.
- Request/response fields, permissions, errors and retry semantics require the
  complete contract campaign. Named request quotas and configured optional Accel
  search/read now have scoped HTTP/native integration tests, not a blanket
  conformance result. Search returns 503 when Accel is absent or unavailable;
  object reads preserve their pre-header origin fallback.
- Indexed metadata TTL reclamation, scoped integrity/healing and verified pool
  migration are implemented. Their remaining physical topology, scale and fault
  gates remain in the completion checklist. Metadata snapshots and object
  recovery are separate operations; a matching whole-product point requires the
  documented operator pause. Independent live snapshots do not provide an atomic
  cross-service point.

A v2 storage pass, a metadata KV test or a successful Console upload does not
close these gaps. [The completion checklist](docs/RC2-IMPLEMENTATION.md) owns the
remaining work; [TESTING.md](TESTING.md) owns qualification evidence.
