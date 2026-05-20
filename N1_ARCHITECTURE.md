# N1 Storage Kernel Architecture

This document describes how N1 stores metadata and payloads on disk. Operators should start with [N1_DOCUMENTATION.md](N1_DOCUMENTATION.md). This document is for maintainers, reviewers, and administrators who need to reason about repair, GC, redistribution, and failure recovery.

## Storage Planes

N1 has three storage planes and two transport faces:

- **Namespace plane:** real directories and file symlinks.
- **Object index plane:** fanned `.objects/<hh>/<hh>/@o_<object-id>/` directories with immutable `.ndx` version records.
- **Blob plane:** shard/parity payload files under `.blobs/<version_id>/`.
- **HTTPS/LOA face:** the universal `/n1/...` API.
- **Native `n1://` face:** a MessagePack-framed protocol for N1 Console, CLI, NS, N2, and trusted internal native clients.

The transport faces call the same N1 core/service graph. `n1://` is not a separate metadata path and does not own storage truth.

## Native Protocol Plane

N1 1.2.2 uses `n1://` protocol `n1/0.3` as the native N1 protocol plane beside HTTPS. The protocol exists so N1-owned tools can talk to the storage kernel without paying HTTP/TLS overhead or JSON response duplication. It covers namespace/control operations and the native data plane: one-shot upload, multipart init/part/finalize, upload status/list, object chunk download, object status/repair, GC, repair, redistribution, quota, and health. The native listener is required at startup; if its port cannot bind, N1 should fail startup instead of silently running without the low-level face.

The native plane is intentionally thin:

```text
TCP connection
  u32_be frame length
  MessagePack NativeRequest
    id
    op
    payload: JSON text
    body: optional binary bytes
  MessagePack NativeResponse
    id
    ok
    status
    body: MessagePack-encoded operation result bytes
```

The server stores auth state per native connection. `auth.fragment` calls the fragment authorization core directly, then the returned JWT is attached to later typed native operations. Native does not contain an HTTP route bridge and does not dispatch through Axum. Operations such as `namespace.stat`, `maintenance.health`, `object.status`, and `fragment.quota` call the same N1 core/service functions that HTTPS routes use.

Native protocol performance depends on avoiding small-frame TCP latency and avoiding binary payload inflation. N1 sets `TCP_NODELAY` on accepted native sockets. Protocol `n1/0.3` sends upload payloads as raw bytes after a small MessagePack control frame instead of embedding large bodies inside the MessagePack frame. Native upload dispatch streams those raw bytes through a bounded internal pipe into the same N1 upload core, so one-shot and multipart parts are not materialized as whole protocol buffers on the server. The Rust release proof includes native namespace and upload benchmarks so delayed-ACK/Nagle regressions and native data-plane regressions are caught.

The native plane must not:

- own namespace truth
- bypass fragment auth
- bypass object maintenance gates
- bypass upload accounting
- bypass repair, GC, redistribution, or validation logic
- implement a separate object metadata path

Native is a lower-latency core face for the same storage kernel, not a fourth storage plane and not masked HTTP.

Each configured root can house both index and blob data:

```text
<root>/<fragment_hash>/
  .fragment.ndx
  .objects/
    ab/
      cd/
        @o_<object-id>/
          <ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
  .uploads/
    <version_id>/
      manifest.ndx
      binding.ndx
      part_p00000000000000000000.<crc32>.receipt
      shard_p00000000000000000000_s0000.<crc32>.receipt
  .trash/
    docs/
      video.mp4 -> @o_<object-id>
  .blobs/
    <version_id>/
      <shard-or-parity-file>
  docs/
    video.mp4 -> @o_<object-id>
```

## Fragment Record

`.fragment.ndx` is the per-fragment control record. It stores fragment-level operational state such as quota/project identity, state, timestamps, key material metadata, and index partition count.

`.fragment.ndx` is not object truth and not namespace truth.

## Namespace Plane

Folders are real directories. Files are symlinks whose payload is a stable object id:

```text
docs/video.mp4 -> @o_<object-id>
```

The symlink target is not a relative filesystem path. Moving or renaming a folder does not require rewriting child symlinks.

File rename changes only the namespace symlink name. Folder rename changes only the directory path. Blob files and object version records are not moved by namespace operations.

Reserved internal names are rejected in user paths:

```text
.objects
.uploads
.trash
.tmp
.locks
.gc
.repair
.intents
.trash-jobs
.blobs
@o_*
```

## Object Index Plane

Object directories are fanned out by object id hash:

```text
.objects/<hh>/<hh>/@o_<object-id>/
```

Each committed version is one immutable `.ndx`:

```text
<ulid>.<size-bytes>.<unix-nanos>.<packed-ext-u64>.ndx
```

The filename is a zero-decode listing summary:

- ULID: version identity and sort order
- size bytes: list/display size
- unix nanos: timestamp
- packed extension: compact extension key

The `.ndx` body stores full immutable object metadata for reads, repair, redistribution, archive download, recover-on-read, and GC.

Head/latest is the greatest valid ULID filename in the object directory. There is no mutable head pointer.

## Packed Extension

The packed extension is a u64:

- byte 0 stores extension length
- bytes 1 through 7 store lowercase ASCII extension bytes
- extensions longer than 7 bytes store `0`

This keeps list output useful without decoding `.ndx` bodies.

## Normal Listing

Normal listing opens the requested namespace directory and reads direct children only:

```text
prefix=docs/
open <root>/<fragment>/docs/
read direct children
```

For file symlinks, N1 reads the object id from the symlink, opens the object directory, selects the greatest ULID `.ndx` filename, parses the filename summary, and returns the entry.

Normal listing does not decode `.ndx` bodies.

## Upload State

Incomplete multipart state lives outside the live namespace:

```text
.uploads/<version_id>/
  manifest.ndx
  binding.ndx
  part_p<part-index>.<crc32>.receipt
  shard_p<part-index>_s<shard-index>.<crc32>.receipt
```

`manifest.ndx` and `binding.ndx` are written during session setup. Receipt filenames are the lightweight durable progress record. Finalize reads session state, rebuilds full object metadata, writes the committed `.ndx`, and publishes the namespace symlink.

## Trash Lifecycle

Soft delete moves namespace entries into `.trash`.

Recover moves namespace entries out of `.trash`.

Hard delete removes trash namespace entries. When no live or trash symlink references an object id, N1 removes the unreferenced object index record. GC later reclaims headless blob payloads.

Trash/recover never moves blob payloads.

## GC Reachability

GC scans live and trash namespace references, resolves object ids, decodes committed `.ndx` bodies when full blob references are needed, and protects referenced payloads.

Blob payloads with no reachable committed metadata become reclaim candidates.

## Repair Model

Repair restores:

- missing namespace/index replicas from healthy roots
- missing or corrupt shard/parity payloads where enough data remains
- missing metadata replicas after disk loss or reformat
- readable objects after recover-on-read or full repair

Object-level repair scopes the operation to one object. Fragment repair scans the whole fragment.

## Redistribution Model

Redistribution adapts index and blob placement to the current configured roots. It supports shrinking and expanding disk topology, including `2 roots -> 1 root -> 2 roots`.

Redistribution must preserve namespace symlinks, stable object ids, immutable `.ndx` records, and blob references.

## Public Identity

Public documentation should call this system N1, the N1 storage kernel, the N1 namespace, the N1 object index, and the N1 blob plane. Implementation files such as `.fragment.ndx`, `.objects`, `.uploads`, and `.blobs` are valid architecture terms, but they should not be presented as a separate database product.
