# N1 Testing And Release Confidence

N1 tests are split into three gates. A green unit/workflow suite is useful, but it is not a production release signal by itself.

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

It starts a real N1 server binary, creates a real fragment, uploads a real nested folder through the public HTTP one-shot route, lists through the public API, restarts the process, lists again from disk-only state, downloads sample files, and directly inspects each on-disk `index.ndxdb`.

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

## Release Rule

For production confidence, require:

```bash
cargo test -p n1
scripts/bench/run_release_drills.sh
sudo -E SOURCE_DIR=<real-folder> DISK_ROOTS=<disk-a>,<disk-b> scripts/bench/run_real_restart_proof.sh
```

Do not treat synthetic tests alone as release confidence. N1 must prove the public API, process restart, real filesystem quota, real disk layout, and durable `index.ndxdb` state together.
