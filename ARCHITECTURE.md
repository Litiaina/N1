# N1 and M1 architecture

This repository contains two independent systems: **N1**, the single-node
object store, and **M1**, the metadata store. **LINK — Litiaina Immutable Native
Key** is N1's key algorithm and storage architecture. M1 owns its specialized
transactional database in `metadata_engine/storage/`. The public native NOA API
connects M1 to N1; either service may run on a separate machine.

The `main` branch develops N1 LINK and M1. **N1 Lite** is the former combined
NCX/NDX engine in the separate
[N1-Lite repository](https://github.com/Litiaina/N1-Lite). It is an independent
product with its own engine, binaries, configuration and services. This workspace
does not build or depend on N1 Lite.

`Console / named client → M1 → native N1 (LINK) → shards`

`Client with its own metadata → native N1 (LINK) → shards`

| Component | Owns | Does not own |
| --- | --- | --- |
| N1 — `object_engine/` | LINK keys, immutable pools/EC, upload bytes, verified reads, repair, zero-residue purge, staging GC and object recovery | Human names, logical versions, namespace lists or a metadata database |
| M1 — `metadata_engine/` | Logical fragments, names, maximum-ULID version selection, upload transactions, listings, rename, trash/recovery, permissions and exports | Shard placement, member-root access or an alternate object format |
| Console (separate repository) | Existing user interface and workflows through metadata's NOA v1 API | Storage authority or replacement metadata semantics |

N1 builds and operates independently of M1 and its database. M1 uses
`n1_native_client`, not object-engine internals. An external metadata service
can use the same LINK interface. Metadata availability is not a prerequisite for
a client holding a valid object key and object-service credentials.

## LINK addressing

A LINK object identity identifies an immutable pool, generation and random token.
Canonical, versioned encoding produces its fingerprint; the pool's fixed member
map and placement rule determine the shard locations. The key-to-path calculation
does not consult NCX, NDX, a namespace index or a per-object database record.
Looking up a shard is separate from the I/O, integrity checking and EC work
required to read its bytes. LINK does not make those costs disappear.

The immutable pool fixes member slots, EC geometry and integrity/placement
profiles. Expansion adds another complete pool; it does not redistribute objects
or reinterpret old keys. The exact algorithm, key bytes and disk encoding are
in [LINK-FORMAT.md](docs/LINK-FORMAT.md).

N1 retains its adaptive concurrency, scheduler, streaming I/O,
io_uring, middleware, native server, repair and backup infrastructure. The
object Cargo package is still `n1`; existing executable names and the original
startup banner are preserved. Versions come from Cargo, not release labels in
keys or upload directories.

N1 and M1 declare independent package versions. The current targets are
N1 `1.0.0-RC.2` and M1 `1.0.0-RC.1`; complete system `1.0.0-RC.2` selects that
pair through [the system manifest](deploy/system-release.json). These release
versions are separate from NOA, LINK and database-format versions. See
[the versioning policy](docs/VERSIONING.md).

## Repository structure and naming

Directory names describe responsibilities; Cargo packages and binaries identify
the systems. Keep the existing module structure within each engine.

| Directory | Cargo package / Rust crate | Executable | Identity |
| --- | --- | --- | --- |
| `object_engine/` | `n1` | `litiaina-n1`, `litiaina-n1-backup` | N1 object store |
| `metadata_engine/` | `m1` | `litiaina-m1` | M1 metadata store |
| `link/` | `n1_link` | Library | LINK algorithm, canonical keys, placement and shard format |
| `metadata_engine/storage/` | `m1_storage` | Library | Independently implemented ordered transactional storage inside M1 |
| `native_client/` | `n1_native_client` | Library and examples | N1 native NOA v2 client used by M1 and direct clients |
| `cli_core/` | `cli_core` | Library | Shared operator settings, monitoring and CLI utilities |
| `shell/` | `n1_shell` | `litiaina-n1-shell` | N1 native CLI and object-backup operator host |
| `tools/`, `scripts/` | Qualification and operator tooling | Various | Workloads and deployment helpers; not additional storage systems |

N1's default files are `n1_config.ini` and `n1.env`; M1's are `m1_config.ini`
and `m1.env`. Each service retains its `[server]` controls and can select an
explicit config/secrets path or use its service environment. M1's integration
section remains `[metadata_engine]`. It names a responsibility, not a third
system. The native `n1://` / `n1s://` schemes and NOA wire identifiers are
protocol names and do not change when the metadata service is called M1.

Both server banners keep the signature `L I T I A I N A` heading, followed by
`N 1` or `M 1`. Version, start time, PID and operational details appear on
separate source and output lines. Runtime versions come from Cargo metadata.

## M1 database

M1's storage crate is implemented in this repository. Its on-disk ordered tree
contains all four metadata tables under one atomic root. Values remain on disk;
a byte-budgeted, sharded node cache and streaming scans avoid a catalogue-sized
in-memory index. The service adapter in `metadata_engine/src/store.rs` translates
domain keys and errors; storage algorithms live in separate `database`, `format`,
`tree`, `transaction`, `worker` and `checkpoint` modules.

Readers hold immutable snapshot roots and do not wait for a writer's disk sync.
Writers prepare concurrently. Optimistic validation checks point reads, writes
and observed range predicates before one coordinator appends a compatible group.
The coordinator orders durable commits and root publication; it is not a lock
around request preparation, reads or object transfer. Successful acknowledgement
follows file synchronization. Append/sync errors stop later writes until reopen.

Checkpoints copy a pinned view and replay concurrent changes in a background
worker. The coordinator finishes the bounded tail and durably replaces the root
descriptor. Existing readers keep the old generation until release. No key's
deletion requires a permanent per-key file in this database. Metadata policy
still decides which histories, upload state, pins and reclamation intents remain.

M1's portable SHA-256-verified metadata snapshot format remains separate from
the database file format. Restores build and verify a fresh directory before
publication. N1 payload backup and LINK placement remain independent.

See the [storage module guide](metadata_engine/storage/README.md),
[versioned file format](metadata_engine/storage/FORMAT.md) and
[SQLite source study](metadata_engine/storage/SQLITE-NOTES.md). SQLite supplied
reliability design lessons; its implementation and database dependencies are
not incorporated. Current qualification is recorded in [TESTING.md](TESTING.md).

## Fragment ownership

Logical user fragments are managed by M1. N1 separately
authenticates its clients/service fragments; metadata's backend credential is
not a user's logical fragment password. The term is **fragment**, never bucket.

## Named upload transaction

1. Metadata authorizes the logical fragment, name and permissions.
2. LINK Begin creates incomplete shard headers and returns one immutable LINK key.
3. Metadata durably records the pending named operation before accepting bytes.
4. Bytes reach the independent object engine through native NOA v2.
5. The object engine confirms durable finalization.
6. A short metadata transaction publishes the name and immutable version reference.
7. Metadata returns named success to the client.

Object I/O does not run inside a database transaction. If the object finishes
before metadata publication, the pending operation retains its identity for
reconciliation. It is not discarded as an orphan by guessing from filenames.
An overwrite keeps the old named version visible until publication. A rename
changes metadata; it never renames or rewrites the immutable shard payload.

The native adapter's completion recovery is isolated in
`metadata_engine/src/object_store/native/finalization.rs`. Healthy finalization
sends one native command. If N1 explicitly reports lost volatile completion,
M1 validates paginated upload status and retries finalization once, without
re-uploading bytes or allocating another LINK identity. Pages contain at most
4,096 parts and target at most 64 MiB of declared payload; a single larger part
still requires one request. Missing/unavailable data cannot become success.
This exceptional cold reconciliation may read existing frames; it is separate
from N1's O(pool width) finalization and M1's zero-payload-I/O indexed resume
status. Purged keys and unrelated conflicts do not trigger reconstruction.

Metadata owns trash TTL, indexed reclamation, scoped integrity/healing jobs and
explicit pool migration. Trash is claimed transactionally after expiry; object
deletion uses the native API, and metadata ownership survives until its ACK.
Ordered deletion-time/upload-expiry and pool-reference indexes bound selection
without namespace or shard-root walks. Export and migration pins fence GC.

Migration streams a new immutable destination identity, verifies its payload,
then switches the private metadata reference without changing names, versions
or ETags. Source purge follows the switch. Persisted intents support
resume/stop and cleanup after failures; immutable pool geometry is unchanged.
Neither lifecycle policy nor named migration is implemented inside object storage.

Named reads resolve an immutable LINK reference and use the public native
download interface. Empty reads verify backend availability before success;
nonempty streaming failures remain body errors. Metadata may reconnect a
disconnected download once before any destination bytes or destination error,
while keeping its admission and shutdown guards. It never replays partial
downloads or writes. This adds no RPC to healthy reads and does not change
LINK placement or the object engine's storage path.

Optional Accel reads follow that same authorization and version-resolution
boundary. M1 requests the current named path with a pinned version; cache-origin
callbacks use the ordinary authorized read route and bypass cache recursion.
A rename racing the callback can miss, in which case M1 uses its already-resolved
LINK reference. Cache response identity/geometry is checked before publishing
headers. Mid-stream failure ends the response without a second origin stream.

Namespace search also works without Accel. M1 searches live metadata heads
through a request-scoped snapshot, with bounded batches, result limits and a
signed query/fragment-bound keyset cursor. It examines at most 4,096 metadata
entries plus one lookahead per page and performs no N1 payload I/O. Unknown
match totals remain unknown; an empty bounded page may carry a continuation.
Pages opened during concurrent mutations are live views, not a retained global
snapshot. Configured Accel can still serve indexed fragment-wide searches;
scoped searches retain M1's case-sensitive path boundary. Fresh unavailable
acceleration falls back to M1, while existing cursors stay with their provider.
No secondary records, database-format change or upload-path writes are added.

M1's request quotas share actual-peer authentication/global budgets and canonical
fragment view/upload budgets across HTTP and native clients. Native admission
precedes raw-body transfer. Fragment limiter keys use the stable 16-byte identity;
requests do not allocate string keys or sweep all limiter entries. An owned,
cancellable blocking worker periodically removes replenished idle entries.
HTTP and HTTPS listeners disable Nagle buffering, as the native listener does.
Unconfigured Accel reads bypass request-header preparation.
Direct archives share the retained export worker,
queue and buffer budgets, pin immutable versions and hold only a bounded stream
in memory. M1 owns temporary manifests and recovery cleanup intents; these add
no records to N1's shard layout. During capture, a constant-size summary counts
the retained stored ZIP64/GNU TAR framing. Direct HTTP and named-native archives
therefore stream with an exact Content-Length without materializing another
archive or reading payloads to determine its size. The final bounded chunk waits
for source verification and pin/manifest cleanup; a late failure cannot complete
a successful response merely because all declared bytes have already arrived.

Retained archives reserve their worst-case stored size through a transactional
M1 byte counter before materialization, then charge their actual size when
ready. Ordered expiry rows let cleanup query bounded pages instead of walking
objects. Active builders/downloads hold per-job leases; cleanup removes files
and manifests durably before deleting job state and releasing its charge.
Restart admission uses the configured bounded queue without requiring every
persisted job to fit at startup. The lifecycle implementation lives in
`exports/lifecycle.rs` and `exports/scheduler.rs`; the existing ZIP64/TAR writer
remains in `exports/builder.rs`.

The following sections describe the storage design. Current incomplete product
work is listed only in [the completion checklist](docs/RC2-IMPLEMENTATION.md);
verification evidence belongs in [TESTING.md](TESTING.md). This is not a release
announcement or a migration guide.

## Code responsibilities

| Location | Responsibility |
| --- | --- |
| `link/src` | Canonical immutable identity, placement, key authentication and inline format |
| `metadata_engine/src/store.rs` | Service adapter for durable cross-table transactions, snapshots and bounded scans |
| `metadata_engine/src/key.rs` | Exact UTF-8 metadata identifiers scoped by stable fragment ID |
| `metadata_engine/src/executor.rs` | Bounded blocking database workers, cancellation ownership and draining |
| `metadata_engine/src/namespace` | Logical name bindings, maximum-ULID heads, immutable version references, rename, trash and recovery |
| `metadata_engine/src/service.rs` | Persist upload identity before bytes, finalize through LINK, then publish metadata atomically |
| `metadata_engine/src/object_store` | Bounded concurrent native NOA client connections; no member-root or shard-format access |
| `metadata_engine/src/server` | Named HTTP/WebSocket and native adapters, INI startup and recovery of unfinished publications |
| `metadata_engine/src/fragments`, `metadata_engine/src/middleware` | Logical fragment identities, aliases, permissions, authentication, refresh rotation and shared HTTP/native request quotas |
| `metadata_engine/src/server/accel.rs` | Optional authorized search/read acceleration, response validation and origin fallback |
| `metadata_engine/src/uploads` | Multipart sessions, durable acknowledged-part metadata and bounded HDD re-chunking for non-aligned client part geometry |
| `metadata_engine/src/exports` | Retained bounded ZIP64/TAR builder, direct selected/manifest archive streaming, immutable source pins and M1 crash cleanup |
| `metadata_engine/storage` | M1's own ordered tree, journal/recovery, optimistic transactions, cache and online checkpoints; see its module map |
| `object_engine/src/core/upload.rs` | Normal/durable multipart lifecycle and shared batch barriers |
| `object_engine/src/core/upload/publication.rs` | Joined namespace publication shared by normal, batch and multipart uploads |
| `object_engine/src/core/store/progress.rs` | Bounded volatile part roots and incremental object-checksum prefix |
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
metadata belongs to M1. Tested infrastructure is retained and
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

An authenticated object key has prefix `N1A1.`. Begin, continuation and finalized
reads use exactly the same key. Upload parameters reside in immutable incomplete
shard headers. Progress is owned by metadata or the standalone client; neither
parameters nor progress change placement. There is no separate upload ticket.

The direct client retains these keys. The object engine does not persist the
original object key. An optional metadata service retains the
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
│       └── repair.s<i>.tmp    # only while repairing this member
└── <fingerprint-byte-0-hex>/
    └── <fingerprint-byte-1-hex>/
        └── <fingerprint>.s<i>.shard
```

A final shard contains an immutable inline header, fixed-position EC stripe
records with integrity checks, and a final inline seal. An empty object still
has a header and seal. There is no NDX, NCX, `.commits`, `.claims`, upload manifest
or separate part receipt. The final seal is bytes inside the shard file.

A completed purge leaves no file or directory for that identity. Only shared
pool markers and bounded fanout organizers remain. Continuation opens existing
files only, so an old key cannot recreate a purged upload.

Pool markers identify logical members and geometry without embedding absolute
paths. Explicit enrollment requires empty roots on independent filesystems.
Startup validates existing markers and holds exclusive directory leases; it
never enrolls a missing disk. Descendants are opened relative to pinned directory
descriptors with symlink following disabled. Root replacement is rejected.

## Upload and completion

Named uploads retain their coordination state in the metadata database. For
strict native-aligned parts, a successful native object ACK is followed by a
durable metadata bit before the named request succeeds. A 32-byte page covers
256 parts; updates merge transactionally across writers. Named resume status
reads these pages without rereading payloads. Publication/cancellation removes
them in the corresponding metadata transaction. Relaxed or unindexed uploads
require actual-part reconciliation; only still-valid durable ACKs are indexed.
Valid frames without an ACK may be retransmitted using the same immutable LINK key.
If native finalization detects lost/incomplete staging, the next status rebuilds
the index from actual shards. These records describe acknowledged completion,
not media health, and do not alter LINK keys, object placement or shard files.

M1's durable part-ACK transaction reads the pending plan, logical owner and
upload-GC claim before committing progress. Cancellation, publication, trash
or reclamation which wins that race prevents a new ACK. GC shares the existing
per-version upload lock and defers active writers through its database index;
it does not wait behind a slow client or purge its staging concurrently.
Bridged strict parts synchronize the file and directory links before recording
their receipt. The configured staging ancestor chain is established at startup;
the first strict receipt persists the new version/fragment links, and later
parts retain the version-directory barrier. These are M1 coordination records,
not N1 upload tickets or permanent object tombstones.

For bridged uploads, M1's synchronized source and receipt back the external
durable ACK. Conversion uses normal native parts and retains that source until
N1's durable finalization and M1 publication succeed. Each external file is
checksum-verified before any of its bytes are forwarded, then streamed from
the same rewound descriptor. Verification and forwarding stay adjacent rather
than rereading an object-sized working set. The blocking verifier owns its
memory reservation even if its caller is cancelled.

An unused N1 Begin may disappear on power loss before any native payload is
acknowledged. Before bridge conversion, M1 probes only its native allocation's
headers with an empty status range. Confirmed total absence permits a fresh
Begin in the same pool/generation and an atomic replacement of the private
pending reference and expiry index. The public named session/version and M1
source receipts remain unchanged. Cancellation, publication, trash and GC fence
replacement. Unavailability, corruption and partial member loss do not trigger
this recovery. The old LINK key stays missing; continuation never recreates it.

The following lifecycle describes the independent object engine:

Begin creates a fresh random identity and writes incomplete shard
headers on all N members, then returns its LINK key. This is the sole creation
boundary. This reply acknowledges allocation; an unused allocation may disappear
on power loss before a durable payload acknowledgement. The first durable part
persists the header, payload and staging ancestry together. Normal/batch PUT
and finalization persist the header with durable publication. Continuation opens
existing shards only, including after a lost unused allocation.

The byte offset selects a part; it must align to part size. Part size is a positive multiple of K times block
size; the final part may be short. Parts can arrive out of order. Positioned
inline records make holes, short bodies, torn frames and conflicting retries
detectable without a manifest. Status pages validate the actual staged bytes;
file length and the key alone cannot prove completion.

A standard part reply acknowledges the completed write without promising disk
persistence. A durable part reply also requires every member's file and staging
directory state to pass persistence barriers. Final object success always
requires full durable publication. A successful retry must not hide an earlier
writeback error. Private data can be rewritten after verification; committed
payload is never changed by an upload retry.

The upload pipeline reuses two sets of owned record buffers, preparing the next
group while the current writes complete. Each member assembles up to four
adjacent stripes, within the charged memory budget; individual
frame positions and checksums stay unchanged. Original data and codec parity feed
framing directly. Codec workspaces are initialized only when that operation
needs them.

Large-upload frames are assembled directly in the aligned output buffers.
Canonical frame headers checksum the borrowed payload before it is copied into
that output; no intermediate full-payload record or payload zero-fill is needed.
The two encoder/I/O turns stay within the existing admission budget, and each
member carries less than one page between turns. Checksums and on-disk bytes
are identical to the contiguous frame encoder used by the other paths.

For objects of at least 1 MiB, complete pages stream through an existing-only
`O_DIRECT` descriptor on the same staging inode. The writer reuses ECS's aligned
buffer and the owned I/O backend, carrying a partial page between groups. Only
the unaligned bytes at each part's two ends use the buffered descriptor. Direct
ranges contain full cache pages wholly inside that part, so concurrent parts
cannot overwrite each other's boundary bytes. This needs no format change,
read-modify-write, sidecar or extra persistence barrier. Direct-open/write errors
fail the request; there is no buffered replay after an I/O error. File-data and
publication persistence still precede the same acknowledgements.

When opening a large part's checked direct descriptor, the writer captures one
filesystem-reported sparse interval under that part's exclusive lock. Groups
wholly inside that interval have no old frames to read or compare. Written
retry regions retain checksum and byte comparisons; other parts cannot change
this part's frame bytes. Unsupported extent queries use the ordinary reader,
while actual I/O errors fail the request. The interval state is bounded by pool
width and introduces no persistent index or new file format.

Native multipart uploads keep the socket in the engine's activity-owned worker,
as native downloads do. The encoder reads the bounded request body directly;
no relay task or duplex body copy is required. Sender deadlines apply only to
pending network reads, excluding disk/admission time. A rejected part drains
exactly its remaining body before the next control frame. Caller cancellation
cannot release the shard lease while owned I/O is still active.

Normal and batch native requests retain their bounded body relay. Its receive
loop coalesces transport fragments into the existing 128 KiB handoff buffer,
preserving exact tails, rejection draining and progress-based idle deadlines.

Smaller objects retain buffered writes. For a new buffered partial tail, known zero padding can remain sparse; its
logical bytes, positions and checksums are identical to a fully written record.
A positioned final zero extends the frame before durable part acknowledgement,
without truncating concurrently uploaded parts. Existing retry regions are
written in full. Sparse representation never replaces frame validation.
At the first payload write the calculated shard length is established sparsely,
after capacity admission. This prevents speculative allocation beyond EOF;
holes remain missing frames, even though the logical length is already complete.

Finalization consumes completion state, moves staged files to calculated final
paths with no replacement, and persists the affected directories concurrently.
Directory descriptors are pinned before namespace changes; each publication
opens its own root description to retain independent writeback-error accounting.
It performs O(pool width) operations: no part traversal, payload reread, or
whole-filesystem flush. Normal mode folds completed roots in memory and persists
the sealed payload at finalize. Durable mode persists each part before recording
completion; the last writer drains overlapping writes, checks its generation,
then writes and persists an ordered completion flag/root inside the existing
shard footer. Payload persistence precedes that proof. Finalize can read this
fixed-size proof after cache eviction or restart. The seal remains under
`.uploads` until explicit publication. Completed payload retries validate bytes
without rewriting them. A surviving completion footer can restore a peer's
missing footer because its ordering covers all members' payloads.
Without a live successful-flush confirmation, finalize refreshes and persists
the staged footer before publication; page-cache visibility cannot certify its
own persistence after an earlier failed flush.
Started member publication workers and their barrier finish even if a peer fails
or the caller disappears; success requires every required persistence result.
Publication across separate filesystems is not a transaction. A lost
reply or crash between member moves can leave partial publication; retrying the
same key completes that identity. Repeated finalization rechecks persistence.
Enough matching finalized shards may already be readable after a lost reply.

Normal `upload.put` streams a single-part object through completion without an
intermediate part acknowledgement. It synchronizes sealed file data, publishes
paths and joins namespace persistence per filesystem before replying.
Batch accepts explicit
single-part keys and concatenated bodies. It prepares the payloads, performs
a shared filesystem barrier, publishes paths, then performs another barrier
before success. Batches are bounded by count, bytes and open shard descriptors.
They are not all-or-nothing transactions across objects; clients resolve lost
replies by retrying the same identities. There is no background commit queue.

A bounded volatile cache folds the contiguous prefix of completed part roots
into the object checksum during transfer. Finalization can finish from that
prefix without rereading payloads or retaining every part root, including when
the object exceeds 4,096 parts. Out-of-order roots and recently retained retry
roots share a global limit of 262,144 records, with at most 1,024 cached uploads.
Only the most recent 64 folded roots are retained per upload; their older
checksum contributions stay in fixed-size SHA state. This also bounds the cache
destruction work on successful finalize. Acknowledged-part retries validate
bytes without rewriting them or discarding the prefix. Mutating writes
invalidate affected completion state before touching payload.
After normal-mode cache loss, clients explicitly reconcile paginated upload
status or resume before finalizing. Finalize returns a conflict if no completion
proof is available; it never silently scans the object. Status validates actual
shard bytes and rebuilds the volatile prefix. Durable completed uploads use
their ordered footer directly. Incomplete parts never become complete by size.
Arbitrary out-of-order holes cannot grow memory without bound. These accelerators
introduce no disk records, changes to shard layout or additional GC scan roots.

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

Explicit repair also accepts completed, unpublished staging when sufficient
verified completion seals and source stripes survive. It reconstructs and
persists replacements only in `.uploads/`; publication still requires finalize.
Completed upload retries remain read-only, and insufficient verified sources
cannot be replaced with unchecked bytes. Ordinary GET/stat require publication.

## Purge and interrupted deletion

Purge takes the per-identity exclusive lifecycle guard, drains admitted owners,
unlinks all incomplete/final shards and repair temporaries, and removes private
staging directories. All member workers finish; persistence barriers precede a
success reply. An unavailable member or a failed removal prevents acknowledgement.
No terminal record or permanent deletion history remains. Repeating an already
completed purge returns zero removals. Old continuation and finalize return 404.
Cancellation removes only incomplete data and refuses partially finalized objects.

A crash during removals is an unacknowledged purge. A survivor is indistinguishable
from disk failure: with enough remaining shards it may still be readable and
repairable. In the metadata deployment, the durable reclamation claim and LINK
key remain until native purge succeeds; named restore/publication is fenced and
GC retries after restart. Standalone clients retain their own pending deletion
intent and retry. Object startup does not infer deletion intent from shard counts.
The exact 3-of-4 crash boundary is exercised by a SIGKILL subprocess regression.

## Maintenance and resource bounds

GC scans `.uploads` through bounded physical traversal. It removes only known,
expired, unpublished staged payload after excluding active ownership, final paths
and final publication. Uncertain or corrupt candidates are retained and reported.
It does not infer that a final object is garbage because its client key was lost.

Staging GC uses the operator INI cleanup interval (3,600 seconds in the
generated configuration). Scheduled healing requires an enabled flag and an
explicit UTC calendar. Manual scrub requests can supply a rate, run window and
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

System Monitoring observes metadata and object nodes independently through the
metadata API. Node CPU/RAM and process counters are collected where that process
runs; object capacity is obtained across native NOA, never by opening object
roots on the metadata host. Demand-driven observations use one collector and a
shared cache per component. Sample age, pending/stale state and incomplete
capacity remain visible. A Console refresh cannot bypass the configured
sampling interval or start an object census. Explicit integrity checks have
separate, single-admission asynchronous work. Logical live-byte capacity reads
transactional counter shards rather than walking the namespace.
Native pool allocation discovery reads immutable configuration and never waits
for the monitoring collector. Live readiness and capacity use the health API.

Metadata backups stream one cross-keyspace MVCC snapshot into a private,
checksummed archive and publish it only after durable completion. They require
an explicit operator request and configured metadata-host destination. Restore
verifies the archive and publishes a new database directory without replacing
an existing one. These archives contain metadata and LINK capabilities, not
object payloads; whole-product recovery requires the matching object state and
service credentials. Object recovery preserves logical pool/member identities
across new filesystems and hosts. Runtime descriptor binding fences do not bind
membership to a disk hardware serial number.

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

M1 prints its own `L I T I A I N A  M 1` boxed banner from
`metadata_engine/src/main.rs`, with Cargo version, M1 storage identity, start
time, PID, hostname, working directory and the selected native/HTTP(S) listeners.
It uses one readable source statement per output line. Its offline commands
keep machine-readable output separate from server startup diagnostics.

## Native NOA and HTTP

The object engine uses native NOA v2 over `n1://` as its primary interface;
`n1s://` adds certificate-verified TLS. The wire version is `n1/1`.
MessagePack control is length-prefixed and raw object
bytes follow separately. Native requests call storage directly without HTTP.
The SDK poisons a connection after an interrupted exchange so it cannot reuse
an unknown frame boundary.

HTTP is optional. JSON control, raw part/range endpoints and the streaming NOA
adapter use the same authorization and object handlers. Configured TLS also
secures the HTTP listener. [NOA.md](NOA.md) defines both contracts. Metadata
serves the retained named NOA v1 HTTP/WebSocket and native `n1/0.5` interfaces;
it forwards object bytes
over `n1/1`. The Console communicates with metadata.

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
Durable restore fences prevent startup until exact-point verification finishes.
Explicit resume accepts only fences belonging to the same point and key.
Restoring an older point intentionally rolls back later writes and purges.
Metadata and object recovery points must be coordinated; there is no historical
deletion overlay. Retention preserves protected points and refuses deletion of
the final point. Repository GC authenticates and pins retained blocks before
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
tests this path with original keys and purged-key absence.

Explicit offline `pool replace --authorize` rebuilds one configured blank slot
from at least K validated surviving peers while retaining the immutable member
map. Multiple losses within parity are replaced one authorized slot at a time;
other absent or unregistered roots remain unavailable until their replacements.
An offline command never enrolls another blank target implicitly. It uses the
same control/root leases, operation-bound fence, expected inventory and final
synchronization as the retained maintenance workflow. Stripe memory is bounded
by the configured budget. Healthy peers are never rewritten. Completed payload
requires verified reconstruction of the original integrity root. Private multipart
progress is reconstructed only from verified frames; holes remain missing and
partial publication is not silently promoted. Outstanding metadata reclamation
claims must finish after physical member recovery.
Temporary inventory and deduplication files use bounded directory fanout and
leave at cutover; they are not part of ordinary uploads or an object catalogue.
Filesystems supporting data/hole queries avoid scanning unwritten sparse gaps;
others use a conservative sequential scan. Reconstruction failures keep the
target inactive. Physical two-member loss/recovery, replacement and INI pool
expansion are qualified in the [November 4 reliability record](docs/RELIABILITY-20261104.md).
The [November 8 million-object qualification](docs/MILLION-OBJECTS-20261108.md)
verifies fresh M1 → N1 population, exact two-HDD shard placement and every
original payload/version after killing and reopening both engines, with M1's
database on a separate SSD.
Larger physical topologies, multiple-member replacement and hardware power-loss
qualification remain separate gates.

Builds, fixtures and raw qualification evidence are kept outside the source tree.
[TESTING.md](TESTING.md) records exact hardware, binaries, commands and scope;
object-only, metadata-KV and complete named-service results are distinct.
