# LINK v1 specification — RC.2 implementation in progress

LINK means Litiaina Immutable Native Key. This is a new format, not a migration
or compatibility layer for RC.1. A single engine exclusively owns each pool.
No RC.2 release is qualified by this specification alone.

## Identity and placement

The canonical identity is exactly 57 bytes: version `01` (one byte), opaque pool
ID (16 bytes), generation (unsigned 64-bit big-endian), random token (32 bytes).
The token is generated with the operating system CSPRNG. Generation and member
slots are never inferred from discovery order or the currently available disks.

Fingerprint = SHA256(`Litiaina/LINK/identity/v1\0` || identity). Domain strings
are their literal ASCII bytes with one final NUL byte. Hexadecimal filenames are
lowercase. The two fanout components are fingerprint bytes 0 and 1, each encoded
as two hexadecimal characters.

Every pool has exactly N=K+P slots. Rotation is the full SHA256 digest of
`Litiaina/LINK/placement/v1\0` || fingerprint, interpreted as an unsigned
big-endian integer modulo N. Shard i belongs to `(rotation+i) modulo N`.
The reverse mapping is `(slot+N-rotation) modulo N`. Placement version, codec
profile, block size, K, P and logical slots are frozen at enrollment.

Final path: `<root>/<fanout0>/<fanout1>/<fingerprint>.s<i>.shard`.
Staging path: `<root>/.uploads/<fingerprint>/shard.tmp`.
The fingerprint allows physical maintenance to derive peers without recovering
the original key. There is no key catalogue or placement record.

## Tickets, final keys and fragment authorization

Object key: `N1A1.` followed by unpadded canonical base64url of identity || tag.
The tag is the first 16 bytes of HMAC-SHA256(service signing secret,
`Litiaina/LINK/object-key/v1\0` || stable fragment ID || identity).
Fragment IDs are exactly 16 opaque bytes. Renaming a display alias or rotating a
fragment credential does not change the stable ID or the service signing secret.
The secret is service configuration, not a per-object file. Preserve it for
restore; losing it prevents key validation. It must not appear in logs.

An upload plan is identity || total_bytes:u64be || part_bytes:u64be ||
expires_at_unix_seconds:u64be || durability:u8 (0 standard, 1 durable).
Upload ticket: `N1U1.` + unpadded canonical base64url(plan || tag), authenticated
using domain `Litiaina/LINK/upload-ticket/v1\0` and the same stable fragment ID.
The tag covers every plan byte. Constant-time tag verification precedes use.
Object-key and ticket domains are distinct. Exact lengths, version, reserved
bits and canonical encoding are required. Prefixes/tags/parameters never enter
the fingerprint. Finalization copies the identity; it does not generate a token.

Part numbers are zero-based. Part size must be a positive multiple of K times
block size. The final part may be short; a zero-length object has no parts.
All offset arithmetic is checked before allocation or filesystem operations.
Authentication is separate from address inspection: clients can compute paths
without possessing the service signing secret, but cannot forge another
fragment's valid object key or alter authenticated upload parameters.

## Shard bytes and immutable integrity profiles

Newly enrolled pools use `shard_format: "crc32c-sha256-v2"`. An existing pool
with no field means exactly `sha256-v1`; this default must never change. The
profile is part of the immutable pool marker and cannot be changed in place.
LINK identity, placement, key/ticket encodings and file offsets are unchanged.
The original `sha256-v1` encoding is specified first below.

No object key, original filename, namespace, version history or path list is
stored. Each shard consists of a 128-byte immutable header, zero or more
fixed-size stripe records, and a 96-byte final seal. Staging and final files use
the same format. A seal is inside the data file; there is no commit sidecar.

Header: magic `N1LSHD01`, version:u16be=1, shard:u16be, K:u16be, P:u16be,
block_bytes:u32be, durability:u32be (0/1), total_bytes:u64be, part_bytes:u64be,
expires_at:u64be, 48 zero reserved bytes, 32-byte SHA256. The hash binds
`Litiaina/LINK/shard-header/v1\0` || fingerprint || first 96 header bytes.

Record: 96-byte frame header followed by block_bytes encoded payload bytes.
Frame header: magic `N1LFRM01`, stripe:u64be, logical source length:u32be,
shard:u16be, zero:u16be, source SHA256:32, frame SHA256:32, eight reserved zeros.
Source SHA256 covers the unpadded logical input for this EC stripe. Frame SHA256
binds `Litiaina/LINK/frame/v1\0` || fingerprint || frame bytes 0..56 || frame
bytes 88..end (including payload). An unwritten whole frame, torn record, wrong
offset, wrong shard index or foreign-object frame fails validation. Known zero
padding may occupy sparse filesystem holes: its logical record bytes, offsets,
length and checksums are unchanged. A valid frame header and both payload/source
integrity checks are still required; file length or a hole alone proves nothing.

Seal: magic `N1LEND01`, total_bytes:u64be, stripes:u64be, eight reserved zeros,
integrity root:32, seal SHA256:32. Seal SHA256 binds
`Litiaina/LINK/shard-seal/v1\0` || fingerprint || complete header || first 64
seal bytes. Finalized files have exactly the calculated length, without tails.

Part root = SHA256(`Litiaina/LINK/part-root/v1\0` || part_index:u64be || ordered
source hashes of that part's stripes). Object root = SHA256(
`Litiaina/LINK/object-root/v1\0` || total_bytes:u64be || part_bytes:u64be ||
ordered part roots). It is not SHA256 of concatenated object bytes. Native
integrity results must identify this algorithm accurately.

### Hybrid profile `crc32c-sha256-v2`

Sizes and offsets are identical to v1. Header magic is `N1LSHD02`, version is
2 and header hash domain is `Litiaina/LINK/shard-header/v2\0`. Header validation
requires the exact profile enrolled in the pool; magic is not a downgrade hint.

Frame magic is `N1LFRM02`. Bytes 24..56 carry the source SHA-256. Bytes 56..60
carry a big-endian CRC32C (Castagnoli); bytes 60..96 are required zero. CRC32C
covers `Litiaina/LINK/frame/v2\0` || fingerprint || frame[0..56] ||
frame[60..end], including reserved bytes and encoded payload. The conventional
CRC32C initial/final complements apply (`123456789` gives `e3069283`).

Source SHA-256 covers `Litiaina/LINK/source/v2\0` || fingerprint ||
stripe_index:u64be || logical_source_length:u32be || unpadded logical source.
All fragments in one stripe carry the same source digest. Decoded source must
match before bytes are returned, repaired or accepted as complete. A valid
frame CRC alone never authorizes returning source bytes.

Seal magic is `N1LEND02` and seal domain is `Litiaina/LINK/shard-seal/v2\0`.
Part/object root construction remains SHA-256 with the respective `/v2\0`
domains; the reported algorithm is `LINK-SHA256-tree-v2`. Header/seal/identity
hashes, object-key authentication and permanent retirement encoding stay strong
and independent of the frame CRC. No new sidecar or index is introduced.

## Lifecycle guarantees

Durable part acknowledgement requires completed writes and persistence on all
N members, including the staging organizers. Standard mode offers weaker
persistence guarantees on the same format. No part receipt/manifest sidecars
are written. Restart recovery validates the staged records. Memory acceleration
is bounded to active uploads and cannot become correctness authority.

Finalization validates completion, writes final seals, persists data, promotes
with no replacement and persists directory changes. Promotion is not a
cross-filesystem transaction. A lost reply can leave a readable object; retry
resolves the same identity and continues partial promotion. Reads require a
matching finalized layout and enough verified fragments for each requested
stripe; they never expose unchecked decoded bytes.

Deletion permanently retires the identity, including identities whose upload has
not started. The user explicitly chose durable deletion state: an old upload
ticket must never recreate a successfully deleted key. Normal uploads add no
sidecar or lookup index. A 96-byte terminal record replaces the payload at each
existing calculated shard path; these records are never garbage-collected.

Deletion has two durable phases. First all N paths become `Retiring` records;
only after every path is terminal and staging payload is removed may any path
become a completed `Retired` certificate. Every member worker is joined before
advancing. A valid completed certificate therefore proves the terminal barrier
was reached on all members. Any observed completed certificate returns HTTP/NOA
410. Without a completed certificate, any `Retiring` record returns 503 and
requires retrying deletion. Losing a member never restores live visibility after
acknowledged retirement. Missing members prevent completing deletion.

Terminal format: magic `N1LDEL01` (8 bytes), version:u8=1, phase:u8 (1 retiring,
2 retired), shard:u16be, four reserved zeros, operation nonce:16, timestamp:u64be,
24 reserved zeros, SHA256:32. SHA256 binds
`Litiaina/LINK/retirement/v1\0` || fingerprint || first 64 bytes.
The nonce is an operation identifier, not an object key. The same strict
fingerprint, slot and byte validation applies to terminal and live files.

Active delete, repair, GC, promotion and part retries coordinate and retain
ownership until all admitted I/O has drained after cancellation. TTL alone cannot
authorize removal of a readable final object or a terminal record. Recovery
repositories preserve terminal records alongside live shard files; restoring payload from
an older point also preserves known retirement records from the targets and all
available points in that repository. It must not revive a known deleted key.
Retirement information that never reached any surviving recovery copy cannot be
reconstructed from an older payload backup; retain current recovery points.

Backup and replication records belong to their recovery repositories, outside
the foreground object representation. Backup must preserve pool identity,
generation, geometry, slots and service configuration needed to validate keys.
Online capture must coordinate with publication/deletion/GC; raw live copying
does not establish a consistent recovery point.

## Conformance

`link/tests/fixtures/link-v1.json` contains independent Python-generated vectors
for identity bytes, fingerprint, rotation, slots and authenticated keys. Both
transports use the same implementation. Failure qualification must include
SIGKILL, torn records, sparse holes, ENOSPC/EIO, lost finalization replies,
conflicting duplicate parts, unavailable members, corruption, complete hashes,
backup restoration and million-object physical inventory/churn.
