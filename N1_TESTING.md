# N1 Testing And Release Confidence

N1 tests are split into three gates. A green in-process suite is useful, but it is not a production release signal by itself.

The active N1 architecture uses real namespace directories, symlink object pointers, immutable `.ndx` object records, and explicit `.blobs` payload roots. Release evidence must inspect those N1 planes directly. Do not use retired Standard/indexdb table inspection as current N1 release evidence.

## Gate 1: In-Process Regression Suite

Run this on every code change:

```bash
RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets
```

This catches deterministic invariants, object metadata rules, namespace semantics, GC safety, repair/redistribution contracts, object-level repair/status, and route-level regressions. It is not enough for production confidence because many tests run in-process and cannot prove process restart, TLS/JWT routing, real filesystem quota, or real disk behavior.

## Gate 2: Release Drills

Run the release drill harness before tagging or deploying:

```bash
scripts/bench/run_release_drills.sh
```

The drill runs high-value workflow cases repeatedly and can include the HA chaos harness. It writes logs under `.n1-bench/release-drills-*`.

For a real HTTP restart proof as part of the release drill:

```bash
RUN_REAL_RESTART_PROOF=1 \
SOURCE_DIR=/home/administrator/Development/litiaina-cloud-web \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
scripts/bench/run_release_drills.sh
```

## Gate 3: Real HTTP Proof

This is the confidence test for process restart, real disks, namespace persistence, repair, recover-on-read, archive download, and redistribution.

```bash
sudo -E \
SOURCE_DIR=/home/administrator/Development/litiaina-cloud-web \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
QUOTA_BACKEND=disabled \
SIMULATE_DISK_FORMAT_REPAIR=1 \
scripts/bench/run_real_restart_proof.sh
```

The proof starts a real `litiaina-n1` server, creates a real fragment, uploads a real nested folder through public HTTP routes, lists through public APIs, restarts the process, downloads sample files, simulates one disk being formatted, repairs, performs `2 disk -> 1 disk -> 2 disk` redistribution, and directly inspects committed `.ndx`, namespace symlink, and blob-version counts on both roots.

Expected pass evidence includes:

- before-restart list count equals uploaded file count
- after-restart list count equals uploaded file count
- before/after path set hashes match
- sample downloads match source SHA-256
- after simulated disk format, the surviving root still lists all expected objects
- strict repair restores the missing index and blob planes
- after `1 disk -> 2 disk` re-expansion, the one-disk write is still listed
- every index root reports matching `live_ndx` and `namespace_symlinks`
- every blob root has one version directory per committed object
- server RSS remains bounded instead of growing with object count

Useful knobs:

```bash
PORT=50491
CONCURRENCY=16
MAX_FILES=0
QUOTA_BACKEND=disabled
QUOTA_BYTES=10737418240
CLEANUP_ROOTS=0
OUT_DIR=/tmp/n1-real-proof
```

Use `MAX_FILES=128` for a smoke run. Use `MAX_FILES=0` for the full source tree.

## Gate 4: Parallel Small-Object Storm

This is the hardening test for the minimum two-disk profile. It creates four real fragments, authenticates as four independent users, and uploads 3,000 nested one-shot objects per user through the public HTTP API.

```bash
scripts/bench/n1_live_parallel_storm.py \
  --base-url https://127.0.0.1:50001 \
  --master-key <MASTER_KEY> \
  --users 4 \
  --objects-per-user 3000 \
  --per-user-concurrency 4 \
  --quota-bytes 21474836480 \
  --posix-list \
  --multipart-size-mib 16 \
  --multipart-part-mib 4 \
  --pid <N1_PID>
```

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
sudo -E SOURCE_DIR=<real-folder> DISK_ROOTS=<disk-a>,<disk-b> SIMULATE_DISK_FORMAT_REPAIR=1 scripts/bench/run_real_restart_proof.sh
scripts/bench/n1_live_parallel_storm.py --base-url <live-url> --master-key <MASTER_KEY> --users 4 --objects-per-user 3000 --per-user-concurrency 4 --pid <N1_PID>
```

Do not treat synthetic tests alone as release confidence. N1 must prove the public API, process restart, real filesystem quota behavior, real disk layout, object-level repair/status, and durable `.ndx`/namespace/blob state together.

## Latest Focused Evidence

Latest focused proof evidence from 2026-05-12:

- `cargo test -p n1 --all-targets` passed after the N1 promotion and repair-source regression fix.
- Real two-disk proof on `/mnt/disk-00` and `/mnt/disk-01` passed CRUD, POSIX, archive, multipart, recover-on-read, simulated disk-format repair, and redistribution `2 disk -> 1 disk -> 2 disk`.
- The proof verified both roots with `live_ndx=904`, `namespace_symlinks=904`, and `blob_version_dirs=904` after re-expansion.
- Server RSS stayed bounded at about `52 MiB` during that focused proof.
