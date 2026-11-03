# Physical metadata qualification

`hdd_qualification` tests the metadata storage foundation through its public Rust
API. It does not upload object payloads, provide a namespace service, or test
NOA conformance. All workload generation, concurrent readers/writers, process
faults, restart validation and exact-byte checks are implemented in Rust.

```sh
cargo test -p n1_metadata_engine --example hdd_qualification --locked
cargo build --release -p n1_metadata_engine --example hdd_qualification --locked
/path/to/target/release/examples/hdd_qualification run \
  --root /absolute/path/on/HDD/new-run-directory \
  --entries 1000000 --churn 100000 --batch 256 --writers 8 --readers 4
```

The selected run directory must not exist. Its parent must already exist. The
program never opens an existing database as its run target or deletes prior
runs. `worker` is the supervisor's internal child entry point, not an operator
command. Put `CARGO_TARGET_DIR` on RAM/appropriate build storage, and the run
directory and reports on HDDs. No machine-wide cache dropping is performed.

Each entry has deterministic 128/256/512/1024-byte values, an immutable original
history fixture, and a key containing its index. The database uses a 32 MiB cache,
1 MiB memtable rotation thresholds and 64 MiB journal rotation thresholds to
exercise flush, compaction and replay. Eight writers operate on disjoint bounded
transactions. Four readers repeatedly check entries against their history using
one snapshot per check, with a 1 ms pause between checks. This is a consistency
load, not a saturated random-read throughput benchmark.

The supervisor kills the population worker halfway through acknowledged work,
drains its acknowledgement pipe, durably saves the test journal, and restarts.
The new child first verifies all previous acknowledgements without writing them.
Uncertain, unacknowledged batches must be completely present or completely
absent; a partial cross-table or within-batch state fails without overwriting
the evidence. Complete uncertain batches are preserved. The remaining batches
are committed with the engine's normal `SyncAll` semantics.

After verifying every entry, the supervisor repeats the kill/resume sequence
during churn. Four out of five selected entries receive a new revision and a
new history fixture; the fifth is deleted with an atomic reclamation-marker
fixture. Original history bytes remain unchanged. These are storage consistency
fixtures, not implemented N1 namespace versions or an object GC service.

Final verification checks all expected entries, every original/update history
and deletion marker, paginates every application table to detect extra/missing
records, and reopens the database once more. `PASSED.json` is created only after
all phases succeed; a failed completed attempt creates `FAILED.json`. Individual
phase logs and acknowledgement journals remain alongside the database. Those
journals are test-client evidence, not additions to N1's production disk layout.

Reported latency samples measure a complete durable metadata transaction,
including contention and fsync, containing up to `--batch` entries. They are
not per-object upload latencies. Memory figures from `/proc` refer to the child
database process; a killed child's last observation can precede its final
instant. The external `/usr/bin/time -v` observation also includes the supervisor
and its verification phases. Reads can be served from the host page cache.

This qualifies process-crash recovery on the selected filesystem. It does not
simulate host power loss, hardware failure or metadata replication. Existing
services and data remain outside the freshly created run directory.
