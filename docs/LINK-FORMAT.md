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

## One LINK key and fragment authorization

The object key is `N1A1.` followed by unpadded canonical base64url of identity || tag.
The tag is the first 16 bytes of HMAC-SHA256(service signing secret,
`Litiaina/LINK/object-key/v1\0` || stable fragment ID || identity).
Fragment IDs are exactly 16 opaque bytes. Display-name and credential changes
preserve this ID. The shared signing secret is protected service configuration,
never a per-object record. Preserve it independently for recovery.

Begin generates a fresh random identity and writes a 128-byte incomplete shard
header on every member before returning this key. This acknowledges allocation;
an unused allocation may disappear on power loss. The first durable part persists
its header, payload and complete staging ancestry before acknowledgement. Normal
PUT, batch and finalization also persist the header with durable publication.
The same key addresses writes,
status, finalize, reads and purge. There is no separate upload ticket. Geometry,
size, durability and staging expiry reside in the immutable shard headers;
progress belongs to metadata or the standalone client. None enters placement.

Continuation takes the key, byte offset and bytes. Offset must align to the
immutable part size, a positive multiple of K times block size. The final part
may be short; empty objects have no parts. Continuation only opens existing
files. Only Begin can create a fresh identity; callers cannot ask Begin to
reuse an old key. All offset arithmetic is checked before allocation or I/O.
Authentication is separate from address inspection: clients can compute paths
without the signing secret, but cannot forge another fragment's valid key.

## Shard bytes and immutable integrity profiles

Newly enrolled pools use `shard_format: "crc32c-sha256-v2"`. An existing pool
with no field means exactly `sha256-v1`; this default must never change. The
profile is part of the immutable pool marker and cannot be changed in place.
LINK identity, placement, key encoding and file offsets are unchanged.
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
hashes and object-key authentication stay strong
and independent of the frame CRC. No new sidecar or index is introduced.

## Lifecycle guarantees

I/O mode does not change these bytes or offsets. Large uploads use aligned
`O_DIRECT` for complete interior pages and buffered writes only at each part's
unaligned ends. Carrying a partial page between encoder groups avoids changing
the format or overwriting neighboring parts. Small objects remain buffered.
Both descriptors refer to the same existing inode, and the same persistence
barriers govern acknowledgement.

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

Purge unlinks incomplete shards, repair temporaries and finalized shards on all
N members and removes the per-identity staging directories. Success requires
all workers to finish and filesystem persistence barriers to succeed. No
terminal file, receipt, tombstone or permanent deletion index is written.
Repeated purge succeeds with zero removals when the identity is already absent.
Old continuation, finalize, status and GET then return Not Found (404); none can
recreate the identity. Shared fanout directory count is bounded by the fixed
address fanout; their allocated blocks follow filesystem behavior.

Purge is not a cross-filesystem atomic transaction. A crash after three removals
in a 3+1 pool can leave one finalized shard. That is an unacknowledged purge;
one shard is insufficient for K=3. With K surviving verified shards the data
can still be readable or repairable: absence alone cannot distinguish an
interrupted purge from member loss. Metadata retains a durable reclamation claim
and LINK key until purge acknowledgement, fences named recovery/publication,
and retries after restart. A standalone caller must retain and retry its own
unacknowledged deletion. Object startup never guesses deletion intent from a
surviving shard. No autonomous revocation after an unacknowledged call is promised.

## Recovery

Recovery captures the exact selected point. Restoring an older point is an
explicit rollback and can restore data purged later. No terminal overlays or
permanent deletion witnesses exist. Coordinate metadata and object recovery
points operationally. Retention preserves protected points and refuses removal
of the final point, without maintaining per-object deletion history.

## Conformance

`link/tests/fixtures/link-v1.json` contains independent Python-generated vectors
for identity bytes, fingerprint, rotation, slots and authenticated keys. Both
transports use the same implementation. Failure qualification must include
SIGKILL, torn records, sparse holes, ENOSPC/EIO, lost finalization replies,
conflicting duplicate parts, unavailable members, corruption, complete hashes,
backup restoration and million-object physical inventory/churn.
