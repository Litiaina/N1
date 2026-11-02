# N1 Operation API — NOA 2

NOA 2 is the LINK engine contract for N1 1.0.0-RC.2. It intentionally replaces
NOA 1 namespace operations. The typed definitions in `link/src/noa.rs` are shared
by the native server and SDK. Implementation/qualification remains in progress;
[the ledger](docs/RC2-IMPLEMENTATION.md) identifies retained features still open.

Native `n1://host:port` is primary. `n1s://host:port` verifies the server's TLS
certificate. Wire version: `n1/1`. HTTP is an optional adapter using the same
service handlers. Object keys and upload tickets are client-held capabilities
bound to a stable fragment ID and independently checked permissions.

## Native framing

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
retries reuse the ticket, not a newly initialized identity. The client must retain
issued keys; N1 does not provide a list from which to recover them.

## Authentication

| Operation | Arguments | Result |
| --- | --- | --- |
| `ping` | none | Product, LINK, NOA and wire versions; public |
| `auth.fragment` | `fragment`, `secret`, optional `permissions`, `ttl_seconds` | Signed token, session, rotating refresh token and refresh lifetime |
| `auth.bearer` | `token` | Verified connection session |
| `session.refresh` | `refresh_token` | Rotates refresh token and returns new access token/session |
| `auth.logout` | none | Durably revokes refresh session and clears native connection session |
| `session.info` | none | Current session |

Permissions are `read`, `write`, `delete` and `repair`. Requested permissions are
intersected with configured fragment permissions. Login tokens last at most
86,400 seconds and are checked for expiry. Explicitly scoped login requests do
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

## Object operations

| Operation | Arguments | Permission / body |
| --- | --- | --- |
| `upload.init` | optional `pool`, `total_bytes`, `part_bytes`, `durable`, `ttl_seconds` | write; no body |
| `upload.part` | `upload_key`, zero-based `part` | write; exact part bytes |
| `upload.put` | `upload_key` for a single-part/empty object | write; exact total bytes |
| `upload.batch` | `upload_keys` in body order | write; concatenated single-part bodies |
| `upload.status` | `upload_key`, `start`, `limit` (1..4096) | read or write; validates a page of actual parts |
| `upload.finalize` | `upload_key` | write; no body |
| `upload.cancel` | `upload_key` | write; removes unfinished staging only |
| `object.stat` | `object_key` | read; size, EC representation and integrity root |
| `object.get` | `object_key`, `offset`, optional `length` | read; exact raw response bytes |
| `object.signed_read` | `object_key`, `offset`, optional `length`, `ttl_seconds` | read; client-held range/expiry-bound grant |
| `object.get_signed` | `grant` | verified grant; raw signed range, no connection login required |
| `object.health` | `object_key`, `deep` | read; object-specific integrity/availability |
| `object.repair` | `object_key` | repair; replaces damaged/missing shards |
| `object.delete` | `object_key` | delete; permanent retirement |
| `object.bulk_delete` | explicit `object_keys` (1..256) | delete; per-key results |

Init returns both `upload_key` (`N1U1.`) and final `object_key` (`N1A1.`), counts,
expiry and durability. It creates no session manifest on disk. A single-part
normal/batch ticket requires `part_bytes >= total_bytes` and EC alignment. Empty
objects have zero parts and no body. Batches have at most 256 tickets, 1 GiB of
body and 1,024 calculated open shard descriptors; duplicate identities are invalid.

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
Current fragment read permission and permanent retirement still apply.

Delete writes durable terminal state at calculated shard paths. `retired_shards`
is the count newly retired by that call; repeated deletion can return zero.
Completed retirement returns 410 to reads and rejects old upload tickets. Partial
retirement or unavailable members can require retry (503). No rename, namespace
listing, object history or undelete operation exists.

## Maintenance

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
service configuration; both GC and scrub schedules default to disabled. Scrub can
alternatively use one UTC calendar schedule with a persisted attempted-slot journal.

## HTTP adapter

| Endpoint | Encoding |
| --- | --- |
| `GET /health` | Public version response |
| `POST /noa` | JSON request/response, no raw body |
| `PUT /noa/upload/{upload_key}/parts/{part}` | Exact Content-Length, raw part body |
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

## Recovery

All recovery operations require an operator token. Requested/scoped permission
tokens do not inherit operator privileges. `repository` and `destination` name
preconfigured aliases; callers cannot submit filesystem paths or encryption keys.

| Operation | Arguments | Result |
| --- | --- | --- |
| `backup.list` | `repository,after,limit` | Bounded page of point summaries; limit 1..256, after empty or canonical ULID |
| `backup.capture` | `repository,boundary_seconds` | Starts committed online capture; boundary 1..300 seconds |
| `backup.verify` | `repository,snapshot` | Starts authenticated point verification |
| `backup.replicate` | `repository,destination,snapshot` | Starts resumable local point replication |
| `backup.prune` | `repository,keep_last,protect` | Starts retention; keep 1..10000, retain protected point and retirement witnesses |
| `backup.gc` | `repository,discard_incomplete` | Starts block reclamation; incomplete transfers protected unless explicitly discarded |
| `backup.job` | none | Current/last bounded recovery job status |
| `backup.stop` | `job_id` | Requests cooperative stop |
| `backup.receive` | `repository` | Upgrades an authenticated native connection to `LINK-replication-v1` |

Job starts return a job record, not a claim that the work has completed. Poll
`backup.job` for completed/stopped/incomplete status and its result or error.
One recovery job runs at a time; running/stopping overlap returns 409. Stop does
not release repository leases while accepted filesystem work is still running.
Restore remains an offline operator helper operation with explicit targets.

## Status meanings

400 invalid encoding, parameters or range; 401 missing/invalid authentication;
403 denied permission; 404 absent object/upload/job; 409 conflicting immutable
bytes, expiry or active-job conflict; 410 permanently retired identity; 413 body
limit; 422 verified format/integrity failure; 503 unavailable quorum, incomplete
retirement, exhausted admission or incomplete operation; 507 ENOSPC/EDQUOT.
A storage error is not converted into success because a final filename exists.

Recovery automation, additional pool administration and signed download operations
remain redesign work. No RC.2 release is authorized until retained features and
qualification are complete.
