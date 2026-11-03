# Litiaina N1 — object and metadata engines

LINK replaces N1's object identity, placement and shard representation. N1 keeps
its established responsibilities and module structure: adaptive concurrency,
disk scheduling, streaming, io_uring, middleware, server, repair and backup.
Both native NOA and the optional HTTP adapter use the same object service.
The workspace now contains `object_engine/` and `metadata_engine/` as separate
main components. Product versions come from workspace Cargo metadata.

Implementation and qualification are in progress. This document describes the
new runtime and identifies unfinished retained responsibilities explicitly.
[Implementation ledger](docs/RC2-IMPLEMENTATION.md) tracks completion;
[LINK format](docs/LINK-FORMAT.md) is the exact byte-level specification.
Changing the package version does not authorize or qualify a release.

## Responsibility

N1 accepts client-held immutable keys, calculates shard locations, and performs
object I/O. Its source directory is `object_engine/`; its Cargo package remains
`n1` and its executables keep their existing names. This is a directory move,
with the tested implementation and module structure retained.

The optional `metadata_engine/` component owns the metadata development cycle:
names, virtual prefixes, user-visible history, permissions, LINK references and
listings. Its storage foundation is imported from ProcyonDB, with optimistic
transactions, consistent snapshots and durable journal commits. The current
component supplies transactional storage, a named-upload coordinator, logical
namespace transactions, named HTTP/WebSocket APIs and the original named native
protocol. The coordinator consumes the existing native object API; it has no
dependency on object-engine internals. Full product qualification and the
remaining service responsibilities are tracked separately; see
[the metadata contract and implementation boundary](docs/METADATA-ENGINE.md).

The object engine is the independently usable LINK storage engine. Metadata is
optional for direct key-based access. N1's complete named-object product consists
of the console communicating with metadata, and metadata coordinating references
and transactions with the object engine. Logical features belong in metadata;
their absence there must never be worked around by removing console features or
putting a namespace back into LINK. Object-engine startup, placement and ordinary
I/O never open a metadata database or depend on metadata availability. A custom
metadata implementation can use the same public object API.

The object engine has no public rename, move, namespace list, search,
latest-version, trash, undelete, ZIP or TAR export operation. It builds and runs
without ProcyonDB or a metadata service. Users can retain LINK keys themselves,
use their own metadata service, or use N1's optional named metadata service.
Metadata outages do not enter the direct,
authorized LINK operation path.

Fragments are stable authorization scopes. Their display aliases and credentials
belong to service configuration; object addressing uses a stable 16-byte fragment
ID for authentication. They are called fragments, never buckets.

The complete retained scope is normal and batch upload, standard and durable
multipart upload, resumable status and finalization, stat, streamed and ranged
GET, object-specific health and repair, permanent single/bulk deletion,
conservative staging GC, optional paced scrub/repair, immutable pool maintenance,
transport security, native SDK/CLI, encrypted incremental backup/restore and
recovery-point replication. Native CLI operations, backup automation and verified
member cloning and explicit failed-member reconstruction are implemented.
Installer/UI adaptation and broader qualification remain open; see the
implementation ledger.

## Code responsibilities

| Location | Responsibility |
| --- | --- |
| `link/src` | Canonical immutable identity, placement, ticket and inline format |
| `metadata_engine/src/store.rs` | Procyon-backed durable cross-table transactions and snapshots |
| `metadata_engine/src/key.rs` | Exact UTF-8 metadata identifiers scoped by stable fragment ID |
| `metadata_engine/src/executor.rs` | Bounded blocking database workers, cancellation ownership and draining |
| `metadata_engine/src/namespace` | Logical name bindings, maximum-ULID heads, immutable version references, rename, trash and recovery |
| `metadata_engine/src/service.rs` | Persist upload identity before bytes, finalize through LINK, then publish metadata atomically |
| `metadata_engine/src/object_store` | Bounded concurrent native NOA client connections; no member-root or shard-format access |
| `metadata_engine/src/server` | Named HTTP/WebSocket and native adapters, INI startup and recovery of unfinished publications |
| `metadata_engine/src/fragments`, `metadata_engine/src/middleware` | Logical fragment identities, aliases, permissions, authentication and refresh rotation |
| `metadata_engine/src/uploads` | Original multipart sessions and bounded HDD re-chunking for non-aligned client part geometry |
| `metadata_engine/src/exports` | Retained bounded ZIP64/TAR builder reading immutable versions through native object requests |
| `metadata_engine/storage` | Retained Procyon storage core, upstream fixes, recovery fixtures and tests |
| `object_engine/src/core/upload.rs` | Normal/durable multipart lifecycle and shared batch barriers |
| `object_engine/src/core/upload/activity.rs` | Owned operation lifetime, shutdown admission and control-lease retention |
| `object_engine/src/core/download.rs` | Verified streamed/ranged reads and read-repair admission |
| `object_engine/src/core/adaptive_concurrency.rs` | Original controller with cancellable service lifecycle |
| `object_engine/src/core/io_scheduler.rs` | FIFO resizing, foreground/background and per-fragment admission |
| `object_engine/src/core/object_parser.rs` | Adaptive stream admission, chunks and bounded read-ahead buffer |
| `object_engine/src/core/linux_io` | Retained io_uring driver and completion ownership |
| `object_engine/src/core/ec.rs` | Retained N1 streaming EC wrapper; LINK adapter in `ec/stripe.rs` |
| `object_engine/src/core/repair_manager` | Verified shard reconstruction and publication |
| `object_engine/src/core/recovery.rs` | Retained four-job admission and separate CPU-affine recovery runtime |
| `object_engine/src/core/maintenance` | Conservative staging GC and paced asynchronous jobs |
| `object_engine/src/core/backup` | Encrypted repository, recovery, replication and retained automation supervisor |
| `object_engine/src/core/storage_admin.rs` | Offline verification, copy fences and byte-exact member cutover |
| `object_engine/src/core/storage_admin/replacement.rs` | LINK shard reconstruction under the retained offline inventory/fence workflow |
| `object_engine/src/middleware` | Authentication, refresh rotation/reuse detection and request quotas |
| `object_engine/src/protocol/server` | Native listener, TLS, explicit enrollment and startup/shutdown |
| `object_engine/src/routes/storage_routes.rs` | Secondary HTTP adapter to shared NOA operations |
| `object_engine/src/settings/config.rs` | Private configuration and explicit immutable member binding |
| `shell/src/args.rs`, `shell/src/offline.rs` | CLI arguments and shared engine-side offline operations |

Within the object engine, the removal boundary is name/version metadata,
NCX/NDX lookup and explicitly discarded namespace/export operations. Logical
metadata belongs to the optional metadata component. Tested infrastructure is retained and
adapted where the shard representation changes; fewer source files is not an
acceptance criterion. The implementation ledger records remaining adaptations.

## Addressing and immutable pools

A pool freezes its identity, generation, member slots, K data shards, P parity
shards, block size, codec profile, shard integrity format and placement version. It has exactly K+P
logical members. Capacity expansion adds a complete pool; it does not change old
placement. A missing member remains missing in its original slot.

LINK's canonical binary identity contains a format byte, pool ID, generation and
32 random bytes. A domain-separated SHA-256 produces a fingerprint. Another
versioned calculation rotates shard indices across the fixed member slots.
Each shard's relative path follows directly from the fingerprint and index.
No scan, per-object lookup table or mutable location record is needed for GET.

An authenticated object key has prefix `N1A1.`. An authenticated upload ticket
has prefix `N1U1.` and carries the exact same identity, plus immutable upload
parameters and expiry. The ticket does not contain a different temporary object
identity. Neither authentication tags nor upload progress alter placement.

The direct client retains these keys. The object engine does not persist the
original object key or upload ticket. An optional metadata service retains the
keys it needs for its logical references and unfinished transactions in its own
database, outside object members. A shared service signing secret authenticates keys to fragment
IDs. Keep an independent protected copy. Service-created recovery points also
include configuration, that secret and any configured TLS identity as a bounded
encrypted block. Losing all copies prevents validating previously issued keys.

## Member representation

```text
<member-root>/
├── .n1-arc-pool.json
├── .uploads/
│   └── <fingerprint>/
│       ├── shard.tmp
│       ├── repair.s<i>.tmp    # only while repairing this member
│       └── retire.tmp         # only while replacing terminal state
└── <fingerprint-byte-0-hex>/
    └── <fingerprint-byte-1-hex>/
        └── <fingerprint>.s<i>.shard
```

A final shard contains an immutable inline header, fixed-position EC stripe
records with integrity checks, and a final inline seal. An empty object still
has a header and seal. There is no NDX, NCX, `.commits`, `.claims`, upload manifest
or separate part receipt. The final seal is bytes inside the shard file.

A permanently deleted identity has a small terminal record at that same final
shard path in place of payload. This durable deletion state was explicitly
chosen by the user so old upload tickets cannot recreate deleted identities.
GC never expires terminal records.
If a terminal record is corrupt, N1 blocks reads, repair and upload-ticket reuse
even when other payload shards could still reconstruct the old object. An
authenticated delete retry can complete retirement; parity repair cannot erase
this deletion fence.

Pool markers identify logical members and geometry without embedding absolute
paths. Explicit enrollment requires empty roots on independent filesystems.
Startup validates existing markers and holds exclusive directory leases; it
never enrolls a missing disk. Descendants are opened relative to pinned directory
descriptors with symlink following disabled. Root replacement is rejected.

## Upload and completion

Init creates a cryptographically random identity and returns its final key and
self-contained upload ticket. It creates no per-upload disk record.

Part numbers start at zero. Part size is a positive multiple of K times block
size; the final part may be short. Parts can arrive out of order. Positioned
inline records make holes, short bodies, torn frames and conflicting retries
detectable without a manifest. Status pages validate the actual staged bytes;
file length and the ticket alone cannot prove completion.

A standard part reply acknowledges the completed write without promising disk
persistence. A durable part reply also requires every member's file and staging
directory state to pass persistence barriers. Final object success always
requires full durable publication. A successful retry must not hide an earlier
writeback error. Private data can be rewritten after verification; committed
payload is never changed by an upload retry.

The upload pipeline reuses two sets of owned record buffers, preparing the next
group while the current writes complete. Each member gets one contiguous write
for up to four adjacent stripes, within the charged memory budget; individual
frame positions and checksums stay unchanged. Original data and codec parity feed
framing directly. Codec workspaces are initialized only when that operation
needs them. For a new partial tail, known zero padding can remain sparse; its
logical bytes, positions and checksums are identical to a fully written record.
A positioned final zero extends the frame before durable part acknowledgement,
without truncating concurrently uploaded parts. Existing retry regions are
written in full. Sparse representation never replaces frame validation.

Finalization validates completeness, writes seals, persists payload, moves staged
files to calculated final paths with no replacement, and persists directory
changes. Publication across separate filesystems is not a transaction. A lost
reply or crash between member moves can leave partial publication; retrying the
same ticket completes that identity. Repeated finalization rechecks persistence.
Enough matching finalized shards may already be readable after a lost reply.

Normal `upload.put` streams a single-part ticket through completion without an
intermediate part acknowledgement or part durability flush. Batch accepts explicit
single-part tickets and concatenated bodies. It prepares the payloads, performs
a shared filesystem barrier, publishes paths, then performs another barrier
before success. Batches are bounded by count, bytes and open shard descriptors.
They are not all-or-nothing transactions across objects; clients resolve lost
replies by retrying the same identities. There is no background commit queue.

A bounded volatile cache accelerates recently completed part roots. Actual shard
bytes remain recovery authority. Cache eviction or restart triggers validation;
it does not lose upload identity or make incomplete parts complete.

## Reads, health and repair

GET validates matching headers and seals and verifies every requested stripe's
frames and reconstructed source before emitting its bytes. It reads data shards
first and uses parity when needed. A range decodes only intersecting stripes.
The integrity root is `LINK-SHA256-tree-v2` for new hybrid pools and
`LINK-SHA256-tree-v1` for existing SHA-256 pools. Neither is SHA-256 of
concatenated user bytes.

Adjacent frames use bounded coalesced reads, with the following window submitted
while the current window is emitted. Each frame is validated once and carries
that validation with its owned shared buffer. Healthy single-data-shard reads
reuse a slice of that buffer; parity recovery still verifies reconstructed
source bytes. Individual-record fallback preserves recovery when a coalesced
read fails. Both current and prefetched work drain before ownership is released.

For a fixed immutable pool topology, LINK placement does not grow with the
number of stored objects: the key identifies the pool and calculates its shard
holders and paths. Reading and verifying the requested bytes is a separate
operation whose work grows with the requested length.

New pools use CRC32C for each encoded frame and independently verify SHA-256
of the decoded source, bound to fingerprint, stripe index and logical length.
Headers, seals and integrity roots retain SHA-256. Existing pools without a
shard-format field keep their original SHA-256 frame/source encoding unchanged;
pool markers reject changing an enrolled pool's integrity format. The former
NCX engine used CRC32 for shard payload integrity. Payload-verification policy
is separate from LINK's fixed-size placement calculation.

Native GET prepares the verified range and its admission/lifecycle guards,
then streams directly into the native socket or TLS transport. One activity-owned
worker retains both the transport and those guards through completion or error;
cancelling the connection's caller cannot abandon admitted I/O. There is no
native payload duplex/relay stage. The HTTP body adapter retains its bounded
stream interface and uses the same prepared read implementation. Socket stalls
have a progress deadline, independent of total object-transfer duration.

Shallow health establishes finalized representation; deep health verifies the
whole object and its integrity root. Unavailable holders remain distinct from
missing or corrupt holders. No diagnostic timeout declares the whole engine
failed. Insufficient verified fragments prevent returning unverified data.

Read repair is bounded and optional. Manual or scheduled repair reconstructs into
private files, verifies the original integrity root, then atomically replaces
only damaged or missing shards. Healthy shard inodes are preserved. Background
scrub and repair can be preempted by foreground work on the same object.
The original global four-recovery-job budget and separate CPU-affine Tokio
runtime are retained. A full budget backpressures read-repair admission after
foreground data/memory guards are released; it does not silently skip repair.
GET completion can wait at that admission boundary. Pending requests for the
same fingerprint coalesce under an owner that shutdown drains.

## Permanent deletion

Deletion uses two durable phases. All K+P paths must first hold terminal
`Retiring` records. Only after that barrier and staging cleanup can any member
publish a completed `Retired` certificate. A valid completed certificate proves
that the first barrier was reached everywhere before it was issued.

A completed certificate returns Gone (410) on reads. An incomplete retirement
returns 503 and requires deletion retry. No upload, finalization or repair may
recreate payload through these records. Completing deletion requires all member
bindings; a missing member produces an incomplete operation. Deletion is
idempotent. Cancellation removes only unfinished staging and does not retire an
identity; use object deletion when permanent revocation is required.

## Maintenance and resource bounds

GC scans `.uploads` through bounded physical traversal. It removes only known,
expired, unpublished staged payload after excluding active ownership, final paths
and retirement intent. Uncertain or corrupt candidates are retained and reported.
It does not infer that a final object is garbage because its client key was lost.

Scheduled GC and scrub are independently opt-in and disabled by default. A scrub
schedule supplies an interval, startup delay, IO rate, run window and explicit
repair choice. Stopping or exhausting a window leaves an incomplete/stopped job;
foreground object service remains available. Only one maintenance job runs at a
time, and job records are bounded control state rather than an object catalogue.
Scrub can instead use a UTC calendar such as `Sun 02:00` or `daily 03:15`.
Its private journal records attempted slots before admission, preventing duplicate
execution after restart or clock rollback. Restart marks an interrupted scrub
incomplete; it does not silently retry it or mark the object engine failed.

Operation count and estimated EC working memory are admitted separately. Pools
whose stripe workspace exceeds the configured budget are rejected. Native
connections and aggregate control buffers are bounded. Bodies stream through
bounded buffers; object size does not determine the per-operation allocation.
All admitted member workers are joined before ownership is released, including
when callers disconnect. Shutdown stops admission and drains active work.
The activity owner retains the control-state lease across caller cancellation,
including durable login/refresh writes. Download completion can be awaited or
dropped without exposing an abort handle for an admitted worker. Transport drain
has a deadline; admitted filesystem work and repair ownership are still awaited
before control/member leases are released.
The original adaptive controller adjusts FIFO I/O and stream targets using CPU,
memory and disk pressure. Stream chunk and buffer sizes remain bounded. Fixed
operation/memory limits are hard ceilings alongside this controller. Repair and
GC use background scheduler lanes; optional maintenance cannot monopolize the
foreground lane. `maintenance.health` exposes system, capacity, scheduler,
backend and maintenance state.

Private service state lives outside object members. The engine holds the retained
instance lease there and exclusive directory leases on members. Configuration
binds every member ID, so mutually agreeing replacement markers cannot silently
rebind an existing pool. Explicit read-only/quiesced operation gates are separate
from optional maintenance time limits.

The service configuration uses the stable format identifier `N1-LINK`, defined
once by `CONFIG_FORMAT`. It contains no binary release label. The workspace
`Cargo.toml` supplies the package version for CLI output and native/HTTP ping.
LINK key, shard and wire encoding identifiers describe their respective formats
independently of the package release.

Server startup retains the original `L I T I A I N A  N 1` boxed banner in
`object_engine/src/main.rs`: Cargo version, STARTING status, local start time, PID,
hostname, working directory and runtime thread settings. The original local
`YYYY-MM-DD HH:MM:SS` log timer and nonblocking stderr logger are retained.
The logger guard flushes during shutdown; machine-readable maintenance output
and CLI version/help do not receive the server banner.

## Native NOA and HTTP

Native NOA 2 over `n1://` is primary; `n1s://` adds certificate-verified TLS. The
wire version is `n1/1`. MessagePack control is length-prefixed and raw object
bytes follow separately. Native requests call storage directly without HTTP.
The SDK poisons a connection after an interrupted exchange so it cannot reuse
an unknown frame boundary.

HTTP is optional. JSON control, raw part/range endpoints and the streaming NOA
adapter use the same authorization and object handlers. Configured TLS also
secures the HTTP listener. [NOA.md](NOA.md) defines this new operation contract.

## Recovery and qualification

Recovery preserves pool identity, generation, member slots, exact shard bytes,
terminal state and encrypted service identity. `restore --output-config` writes
owner-only signing/TLS files, rewrites member roots and publishes configuration
last. It refuses conflicting existing files and serving member roots. Recovered
services start with fresh login sessions. Backups never create a key catalogue
inside the object engine.

Online capture briefly excludes publication, deletion and repair publication
while hard-linking a committed physical view. Upload bodies and reads continue.
The gate is released before hashing, encryption and payload copying; a configured
pin deadline fails the job if that boundary cannot complete. Offline capture
requires exclusive pool leases and additionally preserves resumable private
uploads. An online point excludes unfinished uploads.

Recovery repositories live outside active members. Authenticated encryption
protects content-addressed chunks of at most 4 MiB. Bounded hierarchical manifests
describe file chunks rather than allocating an object-sized block list. Completed
points are immutable; pending transfer pins protect incomplete work from GC.
Local replication can reencrypt under a destination key. Native replication uses
the authenticated `backup.receive` protocol; HTTP uses the same recovery protocol
over an optional WebSocket adapter. Network peers require the same repository key.
This is recovery-point replication, without live standby or failover semantics.

Restore requires explicit empty, independent targets and holds directory leases.
Durable restore fences prevent startup until verification and retirement overlay
finish. Explicit resume accepts only fences belonging to the same point and key.
Restoring an older point unions retirement state from the targets and every
available point in the repository. A deletion that never reached any surviving
backup cannot be reconstructed after all original members are lost.

Retention verifies its inputs before removal and refuses to discard the final
saved witness of a retirement. This maintenance check uses temporary sorted runs
and a bounded merge, without a persistent deletion catalogue or memory proportional
to object count. Repository GC authenticates and pins retained blocks before
reclaiming unreferenced blocks.

Configured repository aliases are the only repository selectors accepted over
NOA. Operator recovery commands start a bounded asynchronous job. Status and stop
requests do not depend on the request connection staying open; cooperative stop
retains ownership until admitted work finishes. Capture, verification, local
replication, retention and repository GC share these controls on native and HTTP.
The retained optional backup supervisor persists restart intent before service
stop, restarts before long verification/replication work, retries the same pending
point and retains failed points. It supports online jobs through native or HTTP
NOA as well as full offline captures. Shipped systemd units invoke `litiaina-n1
backup run-job/recover-job`; the standalone backup helper uses the same code.
These workflows are opt-in and never enabled implicitly.

Offline `pool clone`, `prepare-copy` and `verify-copy` preserve one logical member
identity without changing placement. Source/target leases, operation-bound
fences, complete inventory/content checks and synchronization precede activation.
An administrator changes the configured physical path after verified cutover;
ordinary startup never adopts an empty replacement. The Rust physical witness
tests this path with original keys and permanent retirements.

Explicit offline `pool replace --authorize` rebuilds one configured blank slot
from all surviving peers while retaining the immutable member map. It uses the
same control/root leases, operation-bound fence, expected inventory and final
synchronization as the retained maintenance workflow. Stripe memory is bounded
by the configured budget. Healthy peers are never rewritten. Completed payload
requires verified reconstruction of the original integrity root. Private multipart
progress is reconstructed only from verified frames; holes remain missing and
partial publication is not silently promoted. Terminal records take precedence
over payload, and incomplete retirement never becomes a completed certificate.
Temporary inventory and deduplication files use bounded directory fanout and
leave at cutover; they are not part of ordinary uploads or an object catalogue.
Filesystems supporting data/hole queries avoid scanning unwritten sparse gaps;
others use a conservative sequential scan. Reconstruction failures keep the
target inactive. Multi-member replacement and full pool expansion qualification
remain tracked separately.

Builds and unit fixtures use RAM. Persistent reports and physical test data use
the HDDs, not the source NVMe. Evidence includes parser/corruption vectors,
EC loss patterns, restart/resume, retry conflicts, cancellation, repair,
interrupted retirement, native/HTTP authorization, TLS and physical two-HDD
witnesses. Small debug witnesses do not establish production throughput,
power-loss durability, million-object scale or a win over the former engine.
