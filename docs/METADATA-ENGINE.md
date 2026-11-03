# N1 metadata engine

`object_engine/` and `metadata_engine/` are independent main components in the
same Cargo workspace. The object engine retains package `n1`, its executable
names, module structure and object implementation. It has no dependency on the
metadata component, Procyon or a metadata server connection.

The metadata component is the optional reference implementation for N1 logical
metadata. A custom service can consume the existing object NOA v2 interface
without adopting N1's database schema. Drop-in compatibility with N1 metadata
clients will additionally require the metadata API contract. Internal database
tables, file layout and Procyon implementation details are not public NOA APIs.

## Integrated foundation

| Location | Responsibility |
| --- | --- |
| `metadata_engine/src/store.rs` | Database ownership, schema validation, snapshots, optimistic transactions, durable commits and bounded scans |
| `metadata_engine/src/key.rs` | Fragment-scoped exact UTF-8 identifiers, with no filesystem normalization |
| `metadata_engine/src/executor.rs` | Bounded async-to-blocking execution; workers own admission through caller cancellation; shutdown drains accepted work |
| `metadata_engine/src/namespace/` | Logical bindings and immutable version references; maximum-ULID publication, exact rename, trash and recovery |
| `metadata_engine/src/service.rs` | Named upload coordination, byte forwarding through native NOA, and atomic publication after object finalization |
| `metadata_engine/src/object_store/` | Concurrent bounded native client pool, streaming, typed object results, cancellation and shutdown handling |
| `metadata_engine/src/main.rs` | INI service startup, native and HTTP(S) listeners, graceful shutdown, and offline database administration |
| `metadata_engine/src/server/` | Original named NOA adapters, strict result mapping and pending-publication recovery |
| `metadata_engine/src/fragments/`, `middleware/` | Stable logical fragment identities, aliases, independent permissions and credential-bound sessions |
| `metadata_engine/src/uploads/` | Resumable multipart plans and HDD re-chunking when client parts do not align to immutable EC geometry |
| `metadata_engine/src/exports/` | Retained original bounded ZIP64/TAR builder with immutable metadata-version/native object readers |
| `metadata_engine/storage/` | Imported Procyon storage core, compaction, journal, recovery, transactions and retained regression fixtures |
| `metadata_engine/storage/UPSTREAM.md` | Exact Procyon source, selected upstream fixes and local changes |

The four internal application keyspaces are `n1_entries`, `n1_versions`,
`n1_uploads` and `n1_reclamation`. These reserve distinct transactional record
domains. Namespace entries bind a name to a stable logical object identity and
its maximum committed ULID. Version records retain each immutable LINK reference;
reverse logical bindings let an in-flight upload follow a metadata rename.
Upload records durably retain the exact object ticket before bytes are forwarded.
Native transports are pooled independently within a configured connection bound.
Idle transports expire before the object server's idle deadline, independently
of JWT expiry; failed or cancelled exchanges are discarded. A slow disk barrier
therefore does not justify reusing another connection after its idle deadline.
Pending-publication keys are inserted with reservation and removed in the same
transaction that publishes a version. Restart recovery examines those pending
keys, validates physical completion through native NOA and completes metadata
publication. It does not walk completed objects or change LINK's storage format.
Small batches reserve and publish bounded groups using durable transactions and
the existing native object batch operation, rather than separate fsync groups
for every object. A success response follows physical and metadata persistence.
Trash records retain the removed binding for recovery and detect an unrelated
replacement at the same name. These records belong only to metadata. The private
`n1_system` keyspace records the
storage schema independently of the binary release number.

All application mutations use Procyon's `OptimisticTxDatabase`. A transaction
reads its own writes, tracks point/range predicates and either commits all
changes or reports a serialization conflict. Retrying reruns the complete
closure against a fresh transaction. Storage errors return immediately.
Transaction closures must contain no object operations or other external side
effects that a retry could duplicate.

Every successful mutation commit uses `PersistMode::SyncAll`, before publishing
its writes to readers. There is no asynchronous acknowledgement followed by an
eventual persistence job. Concurrent transaction bodies and readers are
supported; validation, journal publication and fsync still have an ordered
commit section. This does not claim lock-free commits or unlimited concurrency.

A snapshot covers every keyspace. An index read and the referenced entry read
must share it. Keyset pagination seeks after the previous key within a prefix;
it does not rescan and discard an offset. Reusing one snapshot gives a stable
view; taking another snapshot for the next page gives a new view. Snapshots must
be released promptly to let the storage core reclaim older values.

Keys encode the stable 16-byte fragment ID followed by 1–4096 exact UTF-8 bytes.
NUL is rejected; spaces, Unicode and slashes retain their exact meaning. This
is a database identifier encoding, not authorization or a finalized NOA pathname
policy. The format does not put logical names into LINK keys or shard records.

Limits are 1 MiB per value, 4096 mutation calls and 16 MiB of key/value bytes per
transaction, and 1000 records/4 MiB per scan page. A transaction that exceeds
its mutation limits cannot subsequently commit its earlier writes. The async
executor rejects excess active work as busy instead of adding an unbounded
blocking job queue. Cache, per-keyspace memtable thresholds, journal rotation
threshold and background worker count are configurable. They are resource
controls, not a claim of a hard process RSS ceiling.

Opening a populated foreign directory or foreign keyspace set is rejected.
Because application records retain LINK credentials, new metadata directories
are created owner-only on Unix. Existing roots must also be owner-only and must
be real directories; symlink roots are rejected. The wrapper never silently
changes an existing directory's permissions. Procyon's internal files remain
behind that private directory boundary.
An unsupported format is rejected without rewriting its marker. A supported
format with a missing application keyspace is rejected instead of silently
creating an empty replacement. An interrupted initial creation with only empty
expected tables can finish initialization. The storage core's process lock
remains owned until the last database/transaction/snapshot handle ends.

## API standard and service layer

The existing object service continues to implement the operations in
[`NOA.md`](../NOA.md). Its primary transport is native `n1://`/verified `n1s://`;
HTTP remains secondary. The metadata executable adds the named HTTP/WebSocket
service and the original `n1/0.5` named native protocol. It uses LINK's native
`n1/1` protocol as a client. These are distinct listeners and contracts; the
object engine does not acquire a dependency on metadata. Procyon's old document
server endpoints are not exposed.

Metadata service work must retain these separate contracts:

- **NOA v1:** preserve the original versioned HTTPS/WebSocket request meanings,
  authentication, fragment `modify` authority, virtual-prefix behavior, soft
  deletion/recovery, multipart semantics and per-child mutation results. The
  original inventory is preserved at Git commit `061a40a`, file `NOA.md`; it must
  be mapped operation by operation, including explicitly unsupported optional
  capabilities. Renaming Procyon routes is not conformance.
- **NOA v2:** preserve native framing and current LINK object operations. Metadata
  operations need explicit typed commands and capability discovery, without
  turning a metadata lookup into a prerequisite for direct object access.
- **Custom metadata services:** object interoperability only requires consuming
  the object contract. Compatibility with N1 metadata clients additionally
  requires the metadata operation semantics, not the internal Procyon schema.

The named-upload coordinator must enforce this order:

1. Authorize the logical operation and its name/revision precondition.
2. Obtain the final LINK identity and ticket from object upload init.
3. Durably save the pending operation, name precondition, LINK identity and
   ticket before forwarding bytes or returning the session to the client.
4. Stream normal/batch/multipart bytes through bounded buffers. The object
   engine remains authoritative for LINK part progress. If original client
   multipart boundaries cannot map directly onto immutable EC stripes, metadata
   durably stages and hashes those client parts on its own HDD, then re-chunks
   them into native parts. The original upload session and resumable part numbers
   remain stable. This bridge does not change LINK's object identity or format.
5. Confirm durable object completion through the object API.
6. In one short metadata transaction publish the name/reference/history changes
   and complete the pending operation. Only then acknowledge named success.

An overwrite keeps its previous visible entry until step 6 succeeds. Retrying
an uncertain operation reuses the stored LINK/ticket. Object I/O never runs
inside a database transaction. A completed but unpublished object remains
reachable through its pending operation; reconciliation must either publish it
or durably transfer ownership to an explicit-key reclamation workflow. Shard
staging GC cannot infer whether a logical metadata reference exists.

The internal coordinator and exact-key namespace transactions implement this
sequence. In particular, finalization order cannot lower a maximum-ULID head,
rename never rewrites an immutable version reference, and a pending upload cannot
resurrect an object that has been trashed and replaced at the same name.

The existing public console remains the NOA v1 compatibility contract. Its
production runtime and proxy remain intact. Public adapter tests now cover
fragments and authentication, multipart geometry/session semantics, batch
coordination, prefix operations, versioned reads, signed ranges and exports
through real metadata and object processes. All three original live console
tests pass against this service, including interrupted multipart resume and
browser TAR download. Those checks do not establish complete NOA conformance.
Metadata retention/reclamation, export retention, coordinated backup/restore,
metadata request quotas and optional Accel integration still need qualification
and completion. Direct LINK object operations remain independently usable.

Live listing uses a single database snapshot and seeks past common prefixes;
listing a root containing one large prefix does not visit every descendant.
Original object-list DTOs and opaque query-bound continuation tokens are retained.
The `noa_qualification` Rust example supervises real two-HDD API and headless
console campaigns, journals acknowledgements, kills/reopens both services,
checks physical placement offline and reads every acknowledged object back.
Population success is recorded only after these checks finish.

## Component boundary verification

`tests/component_boundaries.rs` checks that the object engine cannot acquire a
metadata/Procyon dependency, and that metadata uses the native client rather than
object-engine internals. `tests/object_client.rs` exercises simultaneous native
calls, cancellation of an exchange, status preservation and shutdown draining.
`tests/native_object_boundary.rs` runs against an explicitly selected existing
object executable and private HDD directories. It verifies standalone LINK before
metadata exists, named upload/download, out-of-order durable multipart, rename
with a pending upload, restart/resume, finalize retry, history, trash/recovery and
empty objects. After metadata shuts down, the direct object key still works.
The restart witness also finalizes the physical LINK object before publishing
its metadata version, confirms the older version remains visible, then closes
metadata and kills/restarts the object process. Reopening metadata and retrying
the saved operation publishes that same immutable object successfully.

The physical witness is explicit opt-in and preserves its datasets and evidence:

```sh
N1_OBJECT_TEST_BINARY=/path/to/tested/litiaina-n1 \
N1_OBJECT_TEST_ROOT=/first-hdd/private-test-parent \
N1_OBJECT_TEST_SECOND_ROOT=/second-hdd/private-test-parent \
cargo test -p n1_metadata_engine --test native_object_boundary -- --ignored --nocapture
```

With a second root it enrolls a fresh 1+1 pool; without that optional setting it
uses a fresh 1+0 pool. Both modes retain the normal independent-filesystem
enrollment checks. This is functional and restart evidence, not a throughput,
power-loss or complete product-acceptance qualification.

## Build and local administration

```sh
cargo build --locked -p n1_metadata_engine
litiaina-n1-metadata init --data-dir /path/to/an/empty/metadata-directory
litiaina-n1-metadata check --data-dir /path/to/an/existing/metadata-directory
cargo test --locked -p n1_metadata_engine -p procyon-engine --all-targets
```

`check` performs storage recovery and format validation under the exclusive
process lock. It is an offline consistency/open check, not a full media checksum
scrub. `init` refuses an existing nonempty directory. No listener, installation,
release or deployment is implied. Use `CARGO_TARGET_DIR` and `TMPDIR` on RAM or
appropriate test storage; keep large reports and physical data on HDDs.
