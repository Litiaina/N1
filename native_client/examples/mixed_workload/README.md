# Standalone N1 mixed file-service workload

This Rust client starts only the supplied N1 executable, in fresh private member
directories on two different filesystems. It never starts M1 or opens an existing
pool. It uses verified native TLS, normal and strict multipart uploads, single-file
uploads, full downloads, unaligned video-style range reads, deletion and confirmed
absence. Only identities created by this run can be deleted.

The library contains two 64 MiB videos, eight 8 MiB files and 128 older small
files. Every seed is uploaded and read back before timing. Interactive files
cycle through 4 KiB, 64 KiB, 256 KiB and 1 MiB. Each bulk uploader starts with
1 GiB and continues with 512 MiB files, alternating normal and strict part ACKs.
The two busy-phase uploaders start in opposite durability modes. Each reconnects
after two acknowledged 8 MiB parts, verifies progress and sends
only the remaining parts. Finalize and repeated finalize are measured separately.

| Phase | Bulk uploaders | Video viewers | Full-file downloaders | Small-file uploaders | Deletion sessions |
| --- | ---: | ---: | ---: | ---: | ---: |
| Light before | 1 | 1 | 1 | 1 | 1 |
| Busy | 2 | 4 | 2 | 4 | 2 |
| Light after | 1 | 1 | 1 | 1 | 1 |

Sessions run concurrently. Videos request 1 MiB ranges with one second of think
time; full downloads pause 500–1,499 ms; small uploads pause 150–749 ms; deletion
pauses 1,200–2,499 ms. These are paced, closed-loop application sessions using one
authorized storage principal, not an unrestricted arrival-rate saturation test
or fourteen independent authorization policies. One session owns one connection.

All successful reads compare every byte against their deterministic source and
produce SHA-256. Small uploads include Begin and completed PUT in their recorded
latency. Deletion latency includes DELETE and the subsequent absence check.
The client keeps object identities and expected content outside N1. It verifies
every retained object and every deleted identity before and after a clean engine
restart, then purges the remaining objects and requires marker-only member roots.
Failures are retained; verification never repairs a fixture by re-uploading it.

```sh
CARGO_TARGET_DIR=/path/on/test-storage/target \
  cargo build --release --offline --locked \
  -p n1_native_client --example mixed_workload

# Both parents must already exist on separate mounted test filesystems.
# Output must not exist. The client creates fresh member-0/member-1 children.
/path/on/test-storage/target/release/examples/mixed_workload \
  /path/to/frozen/litiaina-n1 \
  /path/on/first-test-filesystem/private-run \
  /path/on/second-test-filesystem/private-run \
  /path/on/test-storage/new-results \
  60 120
```

The arguments select light and busy window lengths in seconds. Windows shorter
than ten seconds are diagnostic smoke fixtures, using 32 MiB bulk objects. They
must not be pooled with the full workload. OpenSSL is needed for private test
certificates; no certificate verification is disabled. The client requires
io_uring initialization and retains the supplied executable's I/O implementation.
Native connections are capped at 64, admitted operations at 32, and request
quotas are explicitly disabled to measure storage contention. Effective memory,
adaptive concurrency, pool and runtime settings are captured in provenance.

Append `--diagnostics` to collect N1 Health snapshots immediately before and
after each phase, outside its measured window. These snapshots expose cumulative
I/O queue, operation and filesystem-barrier timings. Their retained percentiles
are not phase-local distributions. Request reports include a Unix-microsecond
anchor for correlation with a separate syscall trace; wall-clock adjustments
can affect that alignment. Diagnostic runs remain separate from baseline
performance samples. No engine code or durability policy changes for this mode.

Append `--upload-only` for a control with only the one/two/one bulk-upload
sessions. `--normal-only` or `--durable-only` fixes the measured part-ACK policy;
omitting both preserves the alternating policy above. These flags can be combined
to measure normal and strict uploads separately before comparing them with mixed
traffic. They do not change completed-object durability, the seed library, full
verification, restart or cleanup. Controls are separate scenarios, not extra
samples to pool into the mixed workload's latency distributions.

No host CPU policy or global cache setting is changed. Requests already started
when a window ends are allowed to finish. Reports separate completed operations
inside the offered window from totals including that drain. Resource samples are
taken every 500 ms and written after timing; request samples are also buffered
within a fixed bound. Do not add different operation classes' byte rates into
one upload-throughput claim, or treat per-part p99 as object-finalize p99.

This exercises a representative storage workload on the recorded hardware. It
does not establish browser/WAN behavior, maximum user count, open-loop overload,
cold reads, power-loss durability, four-drive EC recovery or a production latency
SLA. A functional pass and measured performance acceptance are separate results.
