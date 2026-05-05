# N1 Testing And Release Confidence

N1 tests are split into three gates. A green unit/workflow suite is useful, but it is not a production release signal by itself.

N1 Standard and N1 Edge have different metadata engines. Standard release proof inspects sharded `indexdb`; Edge release proof must inspect filesystem-native `.ndx` records. Do not use Standard indexdb inspection as final Edge evidence.

## Gate 1: In-Process Regression Suite

Run this on every code change:

```bash
cargo test -p n1
```

This catches deterministic invariants, object metadata rules, namespace semantics, GC safety, repair/redistribution contracts, and route-level regressions. It is not enough for production confidence because many tests run in-process and cannot prove process restart, TLS/JWT routing, real filesystem quota, or real disk behavior.

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

## Gate 3: Real HTTP Restart Proof

This is the confidence test for the bug class where listing is correct before restart but wrong after restart.

It starts a real N1 server binary, creates a real fragment, uploads a real nested folder through the public HTTP one-shot route, lists through the public API, restarts the process, lists again from disk-only state, downloads sample files, and directly inspects the on-disk sharded `indexdb` files.

```bash
sudo -E \
SOURCE_DIR=/home/administrator/Development/litiaina-cloud-web \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
scripts/bench/run_real_restart_proof.sh
```

The script uses dedicated subdirectories under each disk root, so startup GC does not scan unrelated live fragments. It still uses the real physical disks for both index and blob roots.

Expected pass evidence includes:

- before-restart list count equals uploaded file count
- after-restart list count equals uploaded file count
- before/after path set hashes match
- sample downloads match source SHA-256
- every index root reports `integrity_check=ok`
- every index root reports matching `namespace_file_nodes`, `object_heads`, `object_versions`, and `object_parts`
- `part_shards` equals `file_count * 2` for the default 1+1 EC proof
- every blob root has one version directory per uploaded object

Useful knobs:

```bash
PORT=50491
CONCURRENCY=16
MAX_FILES=0
QUOTA_BACKEND=xfs_project
QUOTA_BYTES=10737418240
CLEANUP_ROOTS=0
OUT_DIR=/tmp/n1-real-restart-proof
```

Use `MAX_FILES=128` for a smoke run. Use `MAX_FILES=0` for the full source tree.

To prove the repaired-disk case that matters for production, run the same proof with a simulated formatted disk:

```bash
sudo -E \
SOURCE_DIR=/home/administrator/Development/litiaina-cloud-web \
DISK_ROOTS=/mnt/disk-00,/mnt/disk-01 \
QUOTA_BACKEND=disabled \
SIMULATE_DISK_FORMAT_REPAIR=1 \
scripts/bench/run_real_restart_proof.sh
```

That mode uploads the source tree, verifies restart listing, removes one proof disk's fragment subtree for both index and blob roots, restarts N1, verifies that the remaining disk still lists all objects, runs strict repair, then verifies listing, sample downloads, every shard DB, and every blob version directory on both roots.

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
  --pid <N1_PID>
```

Expected pass evidence includes:

- total uploaded objects equals 12,000
- every fragment lists exactly 3,000 objects after upload
- restart listing still returns exactly 3,000 objects per fragment
- N1 does not hit the process open-file ceiling
- no deleted indexdb handles are retained
- RSS remains bounded under the storm instead of growing with object count
- logs contain no write-quorum, indexdb replica, namespace replica, locked database, or blob shard open failures

## Release Rule

For production confidence, require:

```bash
cargo test -p n1
scripts/bench/run_release_drills.sh
sudo -E SOURCE_DIR=<real-folder> DISK_ROOTS=<disk-a>,<disk-b> scripts/bench/run_real_restart_proof.sh
scripts/bench/n1_live_parallel_storm.py --base-url <live-url> --master-key <MASTER_KEY> --users 4 --objects-per-user 3000 --per-user-concurrency 4 --pid <N1_PID>
```

Do not treat synthetic tests alone as release confidence. N1 must prove the public API, process restart, real filesystem quota, real disk layout, and durable sharded `indexdb` state together.

## N1 Edge Release Gate

N1 Edge has its own documentation and release proof requirements in `N1_EDGE_DOCUMENTATION.md`.

Minimum preflight:

```bash
cargo build -p n1-edge --bin litiaina-n1-edge
RUSTFLAGS='-D warnings' cargo test -p n1-edge --all-targets
RUSTFLAGS='-D warnings' cargo check --workspace --all-targets
```

Real hardware Edge proof must run on the minimum two-disk profile:

```text
/mnt/disk-00
/mnt/disk-01
```

Both disks should house index and blob roots.

Edge proof must cover:

- CRUD through public HTTP routes
- latest GET and exact version GET
- POSIX list, mkdir, rename, folder rename, move, trash, recover, purge
- restart listing proof
- multipart resume after restart
- recover-on-read after a missing shard/parity side
- repair after one disk is wiped or reformatted
- redistribution 2 disk -> 1 disk -> 2 disk
- GC after hard delete and empty trash
- archive download for selected folders/files
- four-user parallel upload storm with bounded RSS and file descriptors

API-level storm testing can be reused against a live Edge server:

```bash
scripts/bench/n1_live_parallel_storm.py \
  --base-url https://127.0.0.1:50001 \
  --master-key <MASTER_KEY> \
  --users 4 \
  --objects-per-user 3000 \
  --per-user-concurrency 4 \
  --quota-bytes 21474836480 \
  --pid <N1_EDGE_PID>
```

The current Standard restart proof script includes indexdb table inspection. Before treating it as Edge release evidence, its final inspection phase must be Edge-aware and verify committed `.ndx` records, live/trash reachability, and blob version directories instead of indexdb tables.
