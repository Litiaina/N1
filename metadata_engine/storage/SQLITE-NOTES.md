# SQLite source study — November 10, 2026

SQLite is a design reference, not an M1 storage dependency. Its SQL execution,
file format and single-writer policy are not imported into M1. The following
source was read to examine reliability mechanisms:

- [`src/pager.c`](https://github.com/sqlite/sqlite/blob/master/src/pager.c):
  pager invariants, write barriers and the persistent error state.
- [`src/wal.c`](https://github.com/sqlite/sqlite/blob/master/src/wal.c):
  frame validation, commit boundaries, reader views and checkpoint ordering.
- [`src/btree.c`](https://github.com/sqlite/sqlite/blob/master/src/btree.c):
  ordered page traversal and explicit integrity checking.

The exact reference copies are retained on the test host under
`/mnt/test-ssd/m1-owned-storage-20261110-01/reference/sqlite`:

| File | SHA-256 |
| --- | --- |
| `pager.c` | `a71d373948d21f98686e31674ad8f956dc48701daa086c8d18f7411c4960d871` |
| `wal.c` | `0be881c2ab9f2ff8edc1d450b271a2680af9544f9ab7b57fda231359fa1d5683` |
| `btree.c` | `889fa420565819ffb6b8362d9e3131216bef7bcdc23d687bdb1cc3feaae18c3c` |

## Lessons applied to M1

| Reliability question | M1 mechanism and required evidence |
| --- | --- |
| What makes a transaction committed? | Complete checksummed commit record plus successful synchronization before root visibility/ACK; test kills before/after each boundary |
| Can a later unfinished write damage acknowledged data? | Copy-on-write nodes and 4 KiB group alignment; sweep every truncation position of the final group and verify the preceding committed cross-table state |
| Can a checkpoint discard the last good recovery source? | Sync new data, persist its filename, replace root descriptor, sync directory, then retire the old generation; uncertain publication preserves both |
| Do existing readers survive checkpointing? | Snapshot roots and generation leases; test old snapshots and iterators while writes and checkpoints run |
| What happens after ENOSPC or sync failure? | Do not publish the candidate root or ACK; refuse later writes until reopen; keep prior durable data and error evidence |
| Can recovery mistake foreign or malformed bytes for a valid database? | Independent format/generation identity, bounded decoders, checksums, ordered keys and strict descriptor validation |
| Can a corrupted length masquerade as an unfinished append? | Verify the complete header independently before trusting its payload length; bit-flip every length bit and require an error with the evidence unchanged |
| Does concurrency preserve namespace decisions? | Optimistic validation of point reads, writes and observed range predicates; test lost updates, write skew, same-name claims and phantoms |
| Is corruption detection an ordinary operation? | Checksums and key/version binding on value reads; explicit complete scans/checks and backup verification exercise all live records |

SQLite's rollback-journal invariants are specific to that implementation; M1's
append/copy-on-write format requires its own recovery proof. Process-kill and
byte-truncation tests do not substitute for separately controlled hardware
power-loss qualification. No equivalence with SQLite reliability is claimed.
