# N1 Testing And Release Confidence

N1 tests are split into three gates. A green in-process suite is useful, but it is not a production release signal by itself.

N1 release evidence must inspect the namespace/control plane, colocated immutable object metadata, upload receipts, and EC payload directly. The public API, native protocol, on-disk layout, and maintenance paths all need to agree before a build is considered release-worthy.

## Gate 1: In-Process Regression Suite

Run this on every code change:

```bash
RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets
```

This catches deterministic invariants, object metadata rules, namespace semantics, GC safety, repair/redistribution contracts, object-level repair/status, and route-level regressions. It is not enough for production confidence because many tests run in-process and cannot prove process restart, TLS/JWT routing, capacity/placement behavior, or real disk behavior.

## Gate 2: Release Drills

Run the release drill harness before tagging or deploying:

```bash
scripts/bench/run_release_drills.sh
```

The drill runs high-value workflow cases repeatedly and can include the HA chaos harness. For this workspace, direct every output and Cargo target to `/home/wlzambo/DEV-TEST`.

For a real large-file multipart restart proof as part of the release drill:

```bash
RUN_REAL_RESTART_PROOF=1 \
SOURCE_FILE=/home/administrator/Downloads/INCDT\ 122225.mp4 \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
scripts/bench/run_release_drills.sh
```

## Gate 3: Real HTTP Proof

This focused gate covers process restart, real disks, multipart receipt
persistence, streamed download hashing, range reads, health, and committed
namespace/object state. It is not the complete LTS matrix by itself; repair,
redistribution, GC, UI, and long chaos still need their Rust-owned gates.

```bash
SOURCE_FILE=/home/administrator/Downloads/INCDT\ 122225.mp4 \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
scripts/bench/run_n1_release_proof.sh
```

The proof starts a real `litiaina-n1` server, creates a real fragment, rejects
one-shot for files under the multipart threshold, uploads a real video through
multipart, restarts the process midway, resumes from persisted receipts,
finalizes, lists through public APIs, streams the download hash, checks range
reads, health-checks, and directly inspects committed namespace, `.ndx`, and
EC payload state on both roots.

Expected pass evidence includes:

- before-restart list count equals uploaded file count
- after-restart list count equals uploaded file count
- before/after path set hashes match
- sample downloads match source SHA-256
- after simulated disk format, the surviving root still lists all expected objects
- strict repair restores the missing index and blob planes
- after `1 disk -> 2 disk` re-expansion, the one-disk write is still listed
- every index root reports the same live namespace links
- index-only roots contain no `.objects`
- blob-only roots contain no namespace/control state
- every shard-bearing blob placement has both immutable `.ndx` and payload
- every `.ndx` placement has payload and every payload placement has `.ndx`
- server RSS remains bounded instead of growing with object count

Useful knobs:

```bash
PORT=50491
CONCURRENCY=16
MAX_FILES=0
CLEANUP_ROOTS=0
OUT_DIR=/tmp/n1-real-proof
```

Use `MAX_FILES=128` for a smoke run. Use `MAX_FILES=0` for the full source tree.

## Gate 4: Parallel Small-Object Storm

This hardening gate must be owned by a Rust runner. Python is not part of the
N1 release verification path.

Expected pass evidence includes:

- total uploaded objects equals 12,000
- every fragment lists exactly 3,000 objects after upload
- restart listing still returns exactly 3,000 objects per fragment
- N1 does not hit the process open-file ceiling
- RSS remains bounded under the storm instead of growing with object count
- logs contain no write-quorum, replica, namespace, object-index, or blob shard open failures

## Release Rule

For production confidence, require:

```bash
RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets
scripts/bench/run_release_drills.sh
SOURCE_FILE=<large-video> DISK_ROOTS=<disk-a>,<disk-b> scripts/bench/run_n1_release_proof.sh
```

Do not treat synthetic tests alone as release confidence. N1 must prove the public API, process restart, real disk capacity/placement behavior, real disk layout, object-level repair/status, and durable namespace/colocated-object state together.

## N1 3.1.0 Development Release Requirement

N1 3.1.0 must retain every N1 2.0 storage invariant and additionally prove:

- delete is soft-only across HTTP, POSIX, native protocol, shell, and console
- each deletion receives a unique trash entry id
- repeated deletion of the same logical path remains independently recoverable
- recovery succeeds before TTL and fails closed after TTL
- manual GC cannot shorten trash retention
- normal lifecycle scans `.trash` rather than the complete blob plane
- expiry writes a replayable receipt before removing namespace reachability
- exact object ids are reclaimed directly only after live/trash reference verification
- folder delete remains a directory rename and descendant inspection is deferred until expiry
- N1 2.x and unversioned fragment metadata fail closed; N1 3.1.0 performs no compatibility adoption or migration

Evidence for the current release belongs under `/home/wlzambo/DEV-TEST` and
must record the exact binary hash, configuration, disk topology, commands,
results, and any unproven boundaries.

Recover-on-read fault injection must remove a data shard (`.shard`) that the
read path actually consumes. Removing parity (`.parity`) exercises explicit
object or fragment repair instead; a healthy read is not required to scan for
or recreate unused parity. The proof runner keeps these two fault classes
separate so a parity-repair expectation cannot masquerade as recover-on-read
evidence.

## Historical N1 2.0.0 Evidence

N1 2.0.0 release evidence must include both supported disk-role modes:

- colocated roots, where each physical disk carries index and blob roles
- split roots, where blob-only roots are proven free of namespace/control state

The proof records live namespace-link counts separately from immutable `.ndx`
and payload-placement counts. Object-plane GC compatibility links are not
namespace entries.

Focused N1 2.0.0 evidence from June 11, 2026:

- Colocated two-disk proof passed with `212` live objects and `212` matching
  namespace links on each index replica.
- Split-root two-disk proof passed with `94` live objects and `94` matching
  namespace links on each index replica.
- Both proofs used a real `256 MiB` multipart object, restarted midway, resumed
  from receipts, finalized, and verified the downloaded SHA-256.
- Both proofs passed native protocol operations, concurrent users, POSIX CRUD,
  trash/recover/GC, recover-on-read, object repair, simulated disk-format loss,
  strict fragment repair, and `2 disks -> 1 -> 2` redistribution.
- Split blob roots contained zero aliases, `.fragment.ndx`, uploads, trash, or
  user namespace entries.
- Split index roots contained zero `.objects` directories.
- Every inspected blob placement had matching immutable `.ndx` and payload.
- Object-level redistribution passed a real `1 + 0 -> 1 + 1` topology change:
  one object scanned, one mismatch found, one migration completed, zero errors,
  followed by physical `.ndx + payload` verification on both blob roots.
- N1 3.1.0 redistribution tests must prove that a higher durable namespace
  generation wins after `2 -> 1 -> 2`, including equal-count replacement and
  newer deletion cases, while same-generation content disagreement and writes
  attempted before reconciliation still fail closed.
- Server RSS peaked at about `155 MiB` in the larger colocated matrix and about
  `123 MiB` in the split-root matrix.
- Log scans reported no warning, error, panic, critical, degraded, or failed
  lines after the expected failure/recovery operations completed.
- Release-mode upload benchmarks passed for HTTP and native one-shot,
  sequential multipart, and pipelined multipart. On this HDD pair, explicit
  multipart concurrency `2` produced `41.47 MiB/s` HTTP pipelined and
  `40.82 MiB/s` native sequential multipart averages while keeping peak RSS
  below `87 MiB`.
