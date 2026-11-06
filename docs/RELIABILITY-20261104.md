# N1 reliability qualification — November 4, 2026

**Final campaign full03: PASSED**, completed at **21:19:44 Asia/Manila**.
This record describes reliability and recovery on two local HDDs, with metadata
and one object member sharing a physical disk. It does not replace the
[operator guide](../DOCUMENTATION.md), [NOA contract](../NOA.md) or
[completion checklist](RC2-IMPLEMENTATION.md).

## Results and scope

| Check | Result |
| --- | --- |
| Default workspace, all targets | 447 passed, 0 failed, 27 ignored |
| Metadata/storage documentation | 81 passed, 0 failed |
| Strict object/metadata Clippy, all targets; formatting | Passed |
| Explicit physical/recovery Rust tests | 18 distinct tests, 21 successful executions |
| EC subset matrix | 410 combinations: 190 recoverable, 220 correctly refused |
| Independent metadata database on each HDD | 16,384 entries, 4,096 mutations, two SIGKILL/resume cycles; exact final verification |
| Source and executable provenance | 415 Rust/Cargo source files and frozen executable hashes verified |

The 27 ignored default cases include the explicit opt-in checks above. Three
subprocess helpers are exercised by their parents. Six remaining cases were not
selected: an old-binary upgrade witness, inherited ingestion/compaction, two
monitoring performance witnesses and two existing-deployment transfer helpers.
This is not an all-features/internal-whitebox test run.

The default EC test exhausts nonempty shard subsets as loss and header
corruption for five payload lengths in 1+0, 1+1, 2+1, 2+2 and 3+1 geometries.
Recoverable cases require exact bytes and byte-identical repair while preserving
healthy inodes. Over-budget reads/repairs must fail without changing evidence.
These are logical fixtures on RAM; the physical service pool is 1+1 across two
independent HDDs, not a physical four-member 2+2 pool.

The physical named-service cases verify:

- Each member absent independently, cold restart, exact original versions,
  manual corruption repair and truthful refusal beyond parity.
- Full metadata database and staging loss into evidence quarantine. Restore
  from a verified snapshot recovers 48 versions across two fragments, rename,
  trash, permissions and sparse durable-upload progress. Direct LINK reads
  continue while metadata is stopped. Corrupt/truncated archives and overwriting
  an existing restore target are refused.
- Metadata, object and both processes killed during incomplete part bodies at
  two offsets: six cases per execution. Prior acknowledged parts survive;
  incomplete parts are resumed, conflicting retries fail, repeated finalize
  preserves identity and exact old/new versions survive cold restart.
- Encrypted replica recovery with both original member directories absent,
  cross-device clones, blank-member replacement, permanent retirement, service
  identities, actual INI pool expansion and scheduled staging GC.
- Real Console backup/replication/restore through Shell and native services.
  Its separate control/UI fixture is RAM-backed; physical recovery is covered
  by the preceding service tests. Console source and workflows were retained.
- Actual TEST HDD filesystem unmount, degraded reads and remount while metadata
  stays alive on the other HDD. Object storage is stopped for the clean OS
  unmount and restarted degraded; no fallback member is created on the system
  disk. This is not a live cable pull or host power cut.

The existing standard/durable resume and bitmap-boundary witnesses also pass.
The large resume case acknowledges 1 GiB of a 15 GiB reservation; it is not a
complete 15 GiB transfer. Both database campaigns verify every prior ACK before
writing, reject partial uncertain batches and finish with exact counts:
15,564 entries, 19,660 history values and 820 reclamation fixtures per database.
The two databases are independent, not replicas.

## Defects reproduced and corrected

**Missing backend data could look like an empty successful named read.**
Metadata formerly sent zero-length success headers before its asynchronous
object read failed. It now awaits the native empty read before sending success.
The regression requires failures through named native and HTTP reads when all
backing shards are absent. Nonempty streams still report late backend failures
as body errors; a 200 header alone does not establish successful completion.

**A restarted object service invalidated metadata's cached download socket.**
The first real unmount/remount attempt reproduced a 502 while metadata stayed
alive. A download now gets at most one fresh connection after a transport
disconnect, only before destination bytes and without destination failure.
It retains its admission/shutdown guards. Six real TCP regressions cover retry
limits, partial delivery, application/destination errors, stale idle transports
and guard ownership. Writes are never replayed. Healthy reads add no RPC.

The final campaign reran all gates after both fixes. The object executable is
byte-identical between full02 and full03. Earlier full01 lint, read-boundary
and device-cycle failures remain in the evidence; they were not relabelled.

## Reproduction

Use fresh owner-only fixture parents, with enough free space on two independent
HDDs. Build output belongs on HDD, and unit temporary files may use RAM.
Set `CARGO_TARGET_DIR`, `TMPDIR` and `N1_LINK_TEST_ROOT` as described in
[TESTING.md](../TESTING.md). For the physical metadata fixtures also set:

```sh
export N1_OBJECT_TEST_ROOT=/mnt/hdd-a/n1-reliability/fixtures
export N1_OBJECT_TEST_FIRST_ROOT=/mnt/hdd-a/n1-reliability/members
export N1_OBJECT_TEST_SECOND_ROOT=/mnt/hdd-b/n1-reliability/members
mkdir -p "$N1_OBJECT_TEST_ROOT" "$N1_OBJECT_TEST_FIRST_ROOT" "$N1_OBJECT_TEST_SECOND_ROOT"
chmod 700 "$N1_OBJECT_TEST_ROOT" "$N1_OBJECT_TEST_FIRST_ROOT" "$N1_OBJECT_TEST_SECOND_ROOT"
cargo build --locked --release -p n1 -p m1 -p n1_shell --bins
export N1_OBJECT_TEST_BINARY="$CARGO_TARGET_DIR/release/litiaina-n1"
cargo test --release --locked -p m1 --test noa_service -- \
  --ignored --nocapture --test-threads=1 --skip running_deployment_ \
  --skip monitoring_performance:: \
  --skip real_second_hdd_unmount_degraded_reads_and_remount_keep_every_named_ack
```

The destructive loss tests target their own newly created fixture directories;
their oracles and backup archives stay outside the fault targets. The real OS
device-cycle test must run alone, after all other HDD tests. Set
`N1_DEVICE_TEST_BLOCK` to the explicitly selected disposable partition's stable
`/dev/disk/by-id/...` path and `N1_DEVICE_TEST_MOUNT` to its actual mount root,
then invoke that exact ignored test. It requires working noninteractive
`udisksctl` access. Its device checks reject the system filesystem or metadata
control on the detached device; a guard attempts remount on failure.

For each HDD independently, run the Rust metadata crash/churn supervisor with a
fresh `--root`:

```sh
cargo build --release --locked -p m1 --example hdd_qualification
"$CARGO_TARGET_DIR/release/examples/hdd_qualification" run \
  --root /mnt/hdd-a/n1-reliability/database \
  --entries 16384 --churn 4096 --batch 128 --writers 8 --readers 4
```

The native recovery, INI, io_uring and Console operator witnesses have separate
fixture inputs; their targets and prerequisites are indexed in
[TESTING.md](../TESTING.md). Do not run every ignored test indiscriminately.

## Evidence and limits

Local evidence root: `/mnt/PROJECT-DISK-1TB/n1-reliability-20261104-01`.
`reports/RELIABILITY-20261104.md` contains the full hardware/provenance record;
`reports/full03/` holds raw logs, terminal JSON, executable hashes and `PASSED`.
`source/full-test.sh` records the exact commands and environment. The tested
source is base `2f75d4f` plus the Rust changes accompanying this record; patches,
new test sources and the 415-file manifest are archived there. Later Markdown
corrections do not change those tested executables.

The PROJECT drive used XFS and the TEST drive ext4. Build/data/reports were on
HDDs; unit fixtures were on RAM. Rust/Cargo 1.98.0 built the tests. Rust owns
workloads, ACK oracles, fault injection and verification; the existing browser
driver is JavaScript. No Python was used.

Both volumes were mounted after the run and all qualification processes exited.
This campaign does not establish hardware power-loss safety, four-drive physical
EC qualification, a new 500K population result, maximum throughput or complete
NOA feature parity. It does not declare RC.2 released or promote `main`.
