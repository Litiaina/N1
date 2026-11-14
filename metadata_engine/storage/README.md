# M1 transactional storage

`m1_storage` is M1's independently implemented, specialized ordered database.
It contains no ProcyonDB, Fjall, SurrealKV, SQLite or other database engine.
It uses Rust's filesystem/concurrency primitives, CRC32C and UUID generation.
N1 LINK has no dependency on this crate.

M1's NOA handlers, permissions, namespace, uploads, exports, GC and migration
remain in `metadata_engine/src/`. The adapter in `src/store.rs` translates
fragment/table keys; it does not implement the storage algorithms.

## Module map

| Directory | Responsibility |
| --- | --- |
| `src/database/` | Handles, root publication, process lease and shutdown |
| `src/format/` | Versioned descriptors, checksummed journal records, generation identity and recovery |
| `src/tree/` | Immutable ordered nodes, cache, lookup/scan, path rewriting and streaming tree construction |
| `src/transaction/` | Snapshot reads, bounded write sets and optimistic point/range conflict validation |
| `src/worker/` | Bounded commit admission, group synchronization and maintenance coordination |
| `src/checkpoint/` | Background live-record copying, concurrent tail replay and durable cutover |
| `src/fault.rs` | Test-only process-kill barriers; absent from production binaries |
| `tests/` | Ordered reference model, concurrency, recovery, corruption and reclamation witnesses |

## Transaction and durability rules

- A snapshot retains one committed root and its generation. Point reads and
  scans use that immutable view without holding a database lock over disk I/O.
- Writers prepare concurrently. Commit validates point reads, observed range
  predicates and write conflicts. Conflicts are explicit; M1 retries only its
  side-effect-free transaction closure.
- The commit coordinator combines compatible transactions into bounded groups.
  A large batching setting cannot combine otherwise valid transactions into an
  oversized on-disk record. An individual transaction retains its own limit.
  Values, changed tree paths and an ordered commit record are appended. A
  successful `sync_all` precedes publishing the new root and replying success.
  Consecutive small records share a buffer of at most 64 KiB per appender;
  the buffer is drained before returning a completed commit for synchronization
  or checkpoint replay. Larger records retain the exact positioned-write path.
  This combines writes within a commit, without delaying admission, combining
  acknowledged groups or changing their durability boundary.
- Tables share one root and one durability boundary. A committed change cannot
  expose a name without its related version, index or reclamation mutation.
- A failed append or synchronization prevents publication and poisons further
  writes until reopen. A lost reply can leave a whole committed transaction;
  callers reconcile their operation identity instead of assuming rollback.
- Commit groups end on 4 KiB boundaries. Appending the next group does not
  rewrite the final 4 KiB write unit of an acknowledged group. This is an
  alignment rule, not a claim about arbitrary devices or power-loss behavior.

The file format is independently versioned, with no RC release labels embedded
in stored records. `FORMAT` identifies the database. `CURRENT` names a durable
checkpoint root in `store-<generation>.m1`. Recovery follows its checksummed
commit chain and discards only an incomplete suffix after the last complete
commit. Complete malformed/checksum-invalid records fail closed without
silently rewriting corruption as an empty database.
An independent header checksum is checked before trusting a record's length;
a corrupted length cannot be discarded as an ordinary incomplete suffix.
The exact encoding and publication rules are in [FORMAT.md](FORMAT.md).

## Checkpoints and bounded memory

The node cache has a byte budget; tree lookup keeps only a path in flight.
Values are read from disk rather than retained in a catalogue-sized map. The
streaming checkpoint builder holds bounded node buffers per tree level.
Transactions and queued commit groups have fixed bounds.

Small leaf pages reduce copying for individual mutations; wider branches retain
fanout at scale. Replaced pages leave the cache on unshared single-transaction
writes. Batches and shared snapshots can reuse older cached pages. Eviction
changes neither on-disk snapshot validity nor generation ownership.

A checkpoint copies a pinned committed view into another generation while
foreground transactions continue. It replays subsequent committed changes,
then coordinates the final tail and root switch with commits. The new data
file and filename are synchronized before atomically replacing `CURRENT`;
its parent directory is synchronized before retiring the source generation.
Old readers retain their old files. Their release permits physical removal.
If root publication becomes uncertain after rename, both generations remain
and further writes stop until reopen.

There is one commit coordinator and at most one checkpoint worker per database.
The retained M1 `background_workers` setting currently validates a resource
ceiling; it does not create that many compaction threads. The existing
`memtable_bytes` setting maps to commit-group/tail memory bounds, and
`journal_bytes` controls checkpoint admission. These are not process RSS caps.

## Reference study and verification

[SQLite reference notes](SQLITE-NOTES.md) explain the recovery invariants studied
in SQLite's source and their application here. No SQLite source is incorporated
in the implementation. Reference study does not confer SQLite's maturity or
its test coverage on M1.

Run `cargo test -p m1_storage` and M1's existing transaction, backup, lifecycle,
service and physical qualification suites. Keep builds, datasets and raw
reports off the source NVMe. Historical Procyon-backed passes identify their
original binaries; they do not qualify this new engine. Current results belong
in [TESTING.md](../../TESTING.md) and the
[RC.2 checklist](../../docs/RC2-IMPLEMENTATION.md).
