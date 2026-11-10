# M1 storage format, version 1

This is the specialized database format implemented by `m1_storage`. Its schema
version is independent of the server's Cargo version. It stores metadata, never
N1 shard payloads. All integers below are unsigned and big-endian.

## Directory and ownership

```text
<private-database-directory>/
├── FORMAT
├── LOCK
├── CURRENT
└── store-00000000000000000001.m1
```

The directory is owner-only (`0700`), and files are owner-only (`0600`). The
engine rejects symlinks, nonregular files and foreign directory entries. An
exclusive process lease on `LOCK` lasts through database, transaction, snapshot
and iterator ownership. Shutdown drains the commit/checkpoint workers before
releasing that lease. No per-object file is created by this database.

`FORMAT` is 32 bytes: `M1STORE\0` (8), schema version (4), database UUID (16),
and CRC32C of the preceding 28 bytes (4).

`CURRENT` is 52 bytes: `M1ROOT01` (8), generation, sequence, root offset,
committed end and commit offset (five 8-byte fields), then CRC32C of the
preceding 48 bytes (4). Generation zero is invalid. The initially empty root
and commit offsets are zero; its committed end is 4096.

`store-<20 decimal digits>.m1` begins with a 4096-byte header: `M1DATA01` (8),
generation (8), database UUID (16), zero reserved bytes and CRC32C in the final
4 bytes. A file from another database cannot be substituted as a generation.

## Records and keys

Records start after the generation header. Each has this 28-byte header:

| Bytes | Meaning |
| --- | --- |
| 0–3 | `M1R1` |
| 4 | Kind: 1 value, 2 tree node, 3 commit |
| 5–7 | Reserved, zero |
| 8–11 | Payload length, at most 32 MiB |
| 12–15 | CRC32C of header bytes 0–11, bytes 16–23 and payload |
| 16–23 | This record's absolute file offset |
| 24–27 | CRC32C of header bytes 0–23, checked before trusting the payload length |

A key is a 2-byte length followed by that many bytes. M1's adapter encodes
`table tag (1) | fragment identity (16) | logical key (UTF-8)`. Tags 0–3 name
the four existing M1 tables. The maximum complete storage key is 4113 bytes.

A value record contains sequence (8), encoded key and value bytes. The value
limit is 1 MiB. Reads validate the record checksum, key, sequence and expected
value length against the tree entry.

A node contains kind (1), item count (4) and strictly ordered entries. A leaf
entry contains encoded key, sequence (8), value offset (8) and value length (4).
A branch entry contains its child's minimum encoded key and child offset (8).
References point backward within the same generation. Leaves target 4 KiB and
branches target 16 KiB, each with at most 256 items; a single near-limit key may
exceed the leaf target. These are construction targets, not format changes or
fixed page sizes. Traversal depth is bounded at 64.

A commit contains sequence, root offset, previous commit offset and committed
end (four 8-byte fields), followed by a 4-byte change count. Each sorted change
contains its encoded key and a one-byte presence tag. Presence 1 adds sequence
(8), value offset (8) and value length (4); presence 0 deletes the key. Remaining
payload bytes are zero padding so the record ends at a 4096-byte boundary.

## Commit and recovery

Writers prepare bounded transactions concurrently. The coordinator validates
read/write sets and observed range predicates, then groups compatible changes.
It appends values, changed tree paths and the commit record. Successful file
synchronization precedes publishing the immutable root and replying success.
A group can contain multiple transactions; its mutations become visible
atomically. No external object I/O belongs inside a database transaction.

Recovery starts at the selected `CURRENT` checkpoint and follows complete,
checksummed records and the increasing commit chain. An incomplete final record
or complete uncommitted suffix is removed back to the last complete commit.
A complete header with an invalid header checksum is corruption even if its
advertised payload extends past EOF. Recovery must not mistake that damaged
length for an incomplete append and discard acknowledged records. A malformed
header or payload checksum mismatch in a complete record is corruption;
recovery returns an error and preserves those bytes. A lost reply may leave a
whole committed transaction, so callers reconcile their operation identity.

The 4096-byte boundary separates successive commit groups' write units. It does
not establish power-loss safety for arbitrary storage devices or controllers.

## Checkpoint publication and reclamation

A checkpoint streams a pinned committed tree into a new generation and replays
concurrent commits. Bulk copying and large tails run in the checkpoint worker;
the commit coordinator finishes the bounded final tail. Publication orders:

1. Synchronize the new generation and its directory entry.
2. Write and synchronize `.CURRENT.next`.
3. Rename it over `CURRENT` and synchronize the directory.
4. Publish the new in-memory root and mark the old generation for reclamation.

Old readers keep their old generation until release. If manifest publication
becomes uncertain, retain both generations and refuse further writes until
reopen. Startup only removes recognized unused generations after validating
the selected recovery source. An incomplete uninstalled checkpoint is retained
as evidence and skipped when choosing a subsequent generation filename.

Normal GET is a tree lookup plus a value read. Ordered scans retain a tree path
and current value, not the complete catalogue. `Database::verify` deliberately
bypasses cached nodes and checks every live value and parent/child key range;
it is an explicit integrity operation, never a monitoring poll.
