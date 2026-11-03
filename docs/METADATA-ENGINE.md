# N1 metadata engine internals

The metadata engine is N1's optional named-object component. The public contract
is [NOA.md](../NOA.md), deployment is in [DOCUMENTATION.md](../DOCUMENTATION.md),
and architecture boundaries are in [ARCHITECTURE.md](../ARCHITECTURE.md).
This document describes the metadata implementation, not another API standard
or a replacement operator guide.

## Components

| Module | Responsibility |
| --- | --- |
| `storage/` | Retained ProcyonDB transactional storage core and recovery tests |
| `src/store.rs`, `src/key.rs` | Bounded cross-table transactions, snapshots, fragment-scoped keys and prefix/range scans |
| `src/executor.rs` | Bounded blocking database work; ownership survives request cancellation and drains at shutdown |
| `src/fragments`, `src/middleware` | Logical fragment identity/credentials, independent permissions, authentication and refresh sessions |
| `src/namespace` | Names, immutable version references, max-ULID selection, listings, rename, trash and recovery |
| `src/service.rs` | Pending named-operation identity, LINK completion and atomic metadata publication |
| `src/object_store` | Concurrent bounded native LINK client pool; no shard or member-root access |
| `src/uploads` | Original named multipart session semantics and bounded HDD staging/re-chunking |
| `src/exports` | Retained version-pinned ZIP64/TAR export implementation |
| `src/server` | INI startup, named HTTP/WebSocket/native adapters and pending-publication recovery |

The object Cargo package has no metadata/Procyon dependency. Metadata imports the
native client rather than object-engine internals. The component-boundary tests
check this dependency rule in both directions.

## Transactional foundation

Procyon supplies optimistic multi-writer transactions and concurrent snapshot
readers on one node. Conflicting writes must be retried or reported explicitly;
concurrency does not mean conflicting mutations can both win. Durable mutations
use `SyncAll`. Cross-table snapshots read related records at one committed view.
Long object transfers, filesystem work and network I/O do not hold a database
write transaction.

The imported source, selected upstream fixes, licenses and original regression
fixtures are documented in [storage/UPSTREAM.md](../metadata_engine/storage/UPSTREAM.md).
The original Procyon document server, wire protocols and deployment are not N1
services. The database core is the foundation for N1's own metadata application.

## Named upload lifecycle

1. Authorize the logical fragment, name and mutation preconditions.
2. Ask the object engine for its final immutable identity and upload ticket.
3. Durably save the pending operation, reference and name preconditions before
   accepting bytes or exposing the session.
4. Forward bytes through the native object API using bounded memory. If the
   named client's part boundaries do not align with LINK stripes, stage and
   hash those client parts on metadata's HDD, then re-chunk them into native
   parts. Keep the original session and resumable client part numbers stable.
5. Confirm durable object finalization.
6. Atomically publish the logical name, version/reference changes and completed
   pending-operation state. Only then return named success.

The old visible version survives until publication. A retry reuses the same
saved LINK identity; it does not initialize a replacement object. A physically
complete but unpublished object remains owned by its pending metadata operation.
Recovery must publish it or durably transfer it to explicit-key reclamation.
Object staging GC cannot determine whether a metadata reference exists.

Independent names can proceed concurrently. Small native batch groups preserve
per-object results and use bounded payload/transaction sizes. Finalize order
cannot lower a maximum-ULID version head. Rename changes the logical binding,
not immutable version contents. A late finalize cannot resurrect a trashed name
that was replaced by a different logical object.

## Reads, names and credentials

Metadata resolves a name/version under the caller's logical fragment and forwards
the stored LINK reference to the independent object engine. The private backend
service credential is distinct from the logical user's password and permissions.
A client never needs member paths to read a named object.

List operations use database snapshots and seek past common prefixes instead of
walking every descendant to find the next shallow entry. Continuation tokens
are opaque and bound to the query. User-visible versions are immutable, and
latest means the maximum valid ULID. Object-service keys remain private metadata
references unless a direct-key application manages its own records.

Database/staging directories are owner-only and protect stored backend tickets.
Native clients are bounded independently, so one mutex does not serialize all
object I/O. Cancelled/failed exchanges discard uncertain transports. Idle
transports expire before the object's idle socket deadline; token expiry is a
separate condition.

## Public service and completion

The original Console connects to metadata's named NOA v1 HTTP/WebSocket API.
Named native frames use `n1/0.5`; the backend object connection uses `n1/1`.
Adapters share domain behavior without adding an HTTP network hop between the
engines. The Console's existing production features and proxy are retained.

Full contract compliance is still a completion gate. Missing route groups,
maintenance semantics, metadata reclamation, coordinated recovery, request
quotas and optional integrations are listed in
[the completion checklist](RC2-IMPLEMENTATION.md). Tests and population reports
are indexed in [TESTING.md](../TESTING.md), with their actual scope.

The `litiaina-n1-metadata` binary serves with `--config n1_config.ini` and also
provides offline `init --data-dir` and `check --data-dir` commands. Offline
checks must not open an already serving database. Both engines retain their own
configuration, listeners and lifecycle; stopping metadata does not stop direct
LINK access.
