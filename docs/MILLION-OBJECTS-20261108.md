# N1 and M1 million-object qualification — November 8, 2026

**PASSED.** One million fresh named objects traversed **HTTP → M1 → native
NOA v2 → N1 LINK**. After population, the Rust supervisor killed both engine
processes, completed an offline metadata/shard census, reopened both services
and verified every acknowledged original version and every payload byte.

This qualifies the full-stack API population and process-crash/reopen path for
source `8dbba3201414a7b986c0cacb41c3c525b021c274` and the profile below. It
satisfies the requested API population count of at least 500,000 objects. The
original Console population, separate standalone-only million-object run, the
four-HDD reference system minimum-hardware qualification and hardware power-loss
tests retain their own acceptance gates. **RC.2 remains unreleased.**

## Machine and storage

SSD test system: Intel Core i5-12600K, 10 cores / 16 threads, approximately 31
GiB RAM; Debian 13, kernel `6.12.115+deb13.1-amd64`, glibc 2.41. The tested
binaries were built on this host with Rust/Cargo **1.99.0**, using the
repository release profile: optimization level 3, LTO, one codegen unit, abort
on panic and stripped binaries. Build jobs were bounded to six; incremental
compilation was disabled. No target-native flags were added.

| Role | Physical partition | Mount | Filesystem |
| --- | --- | --- | --- |
| Source, build, binaries, temporary files, M1 database, control, ACK journals and reports | `/dev/sdb1` | `/mnt/DATA_SSD` | XFS, SSD |
| N1 pool member 0 | `/dev/sdd1` | `/mnt/test-media/HDD1TB001` | XFS, rotational HDD |
| N1 pool member 1 | `/dev/sdc1` | `/mnt/test-media/HDD1TB002` | XFS, rotational HDD |

The driver checked all three filesystem types and UUIDs before each campaign.
Their recorded UUIDs are respectively `ssd-filesystem-id`,
`hdd-0-filesystem-id` and
`hdd-1-filesystem-id`. These identify the test devices;
they do not change LINK's logical member identities.

The retained run root on that host is:

```text
/mnt/DATA_SSD/n1-rc2-million-20261108.ZsO460
```

The million-object member roots are:

```text
/mnt/test-media/HDD1TB001/n1-rc2-million-20261108.ZsO460/million-api/n1-noa-01M6VCYPYQZ491ZSB3Q8Y5CD1H/member0
/mnt/test-media/HDD1TB002/n1-rc2-million-20261108.ZsO460/million-api/n1-noa-01M6VCYPYQZ491ZSB3Q8Y5CD1H/member1
```

M1's database is under `datasets/million-api/metadata/control/metadata` in the
SSD run root. Each phase used fresh, private directories. Existing HDD files
and services were preserved; no disks were reformatted and the source NVMe did
not hold the population or build output.

## Workload and verification

The Rust [NOA qualification harness](../metadata_engine/examples/noa_qualification.rs)
owns the workload, durable ACK journals, crash injection, census and readback.
Bash orchestrates the stages; the Rust observer and summary tools collect and
interpret resource evidence. No Python was used.

| Setting | Tested value |
| --- | --- |
| Fresh named objects | 1,000,000, under `qual/api/{index:09}.bin` |
| Payload sizes, equally represented | 0, 1, 127, 1,024, 4,096, 8,193, 32,768 and 65,536 bytes |
| Logical payload bytes | 13,968,125,000 |
| Upload concurrency | Eight writers; HTTP batches of up to 256 objects |
| Readback concurrency | Eight readers after population and restart |
| M1 admission / native object connections | 4 / 32 |
| N1 admission / native connection limit | 16 / 64 |
| Immutable pool geometry | 1 data + 1 parity member; 65,536-byte stripe blocks |
| Upload memory budget | 268,435,456 bytes |
| Durability and adaptive controls | Strict durable publication; adaptive concurrency enabled |
| I/O backend | `N1_IO_BACKEND=uring`, startup confirmed io_uring without initialization fallback |
| Request quotas / scheduled scrubs | Disabled for this functional scale profile; normal read repair retained |

Every successful batch had its original version IDs recorded in a per-writer
ACK journal with flush and `sync_all`. All million ACKs were obtained in this
invocation: **zero prior acknowledged objects were reused**. No durability
barriers or server deadlines were relaxed.

After population, the supervisor sent actual SIGKILLs to M1 and N1 and waited
for their exit. With both services stopped, the census checked named heads,
versions and unique LINK identities against the configured pool, and checked
both members for exact shard counts, calculated placement, staging and
unreferenced files. The restarted services then served every original object;
the oracle compared its version ETag and complete deterministic payload with
the durable ACK record. Verification did not reupload acknowledged fixtures.

The headless transient unit used `Restart=no` and survived SSH disconnection.
It did not automatically retry a failed campaign. Its engine processes,
supervisor, owner unit and temporary sleep inhibitor all ended successfully.

## Completed checks

| Check | Result |
| --- | ---: |
| Release-mode component library regression | 315 passed, 0 failed; 5 explicit ignored cases |
| Separate 4,096-object smoke | Passed census of 8,192 shards and all 57,213,440 original bytes/versions after killing and reopening both engines |
| Million fresh durable ACKs | 1,000,000 |
| Named objects / immutable versions / unique LINK identities | 1,000,000 each |
| Physical shard files | 2,000,000 |
| Original objects fully verified after restart | 1,000,000 |
| Verified logical payload bytes | 13,968,125,000 |
| Payload / version mismatches | 0 / 0 |
| Unreferenced / misplaced shards | 0 / 0 |
| Staged files / pending publications | 0 / 0 |
| Archived source file verification | All 650 files matched the source manifest |
| Final terminal and frozen-binary audits | Passed |

The 315 library cases comprise 199 N1, 42 M1, 1 LINK and 73 Procyon storage
cases; the native-client library target had no cases. This was a library
preflight, not a new full-workspace/all-targets pass. The smoke and million
campaign are separate population checks, not additional distinct unit tests.

## Timing, CPU and memory

| Measured phase | Result |
| --- | ---: |
| Strict durable population | 21,030.620 s — 5 h 50 min 30.620 s |
| Population rate across the measured phase | 47.550 objects/s |
| Offline metadata/shard census | 8.476 s |
| Complete post-restart payload/version readback | 4,050.595 s — 1 h 7 min 30.595 s |

The 3,907 HTTP batch samples contain up to 256 objects each; the final batch
contains 64. They measure the full request under concurrency, including
admission, object durability and metadata publication. **They are not
individual-object PUT latencies.**

| Batch latency | Seconds |
| --- | ---: |
| Minimum | 8.245 |
| Mean | 43.009 |
| p50 | 36.017 |
| p95 | 88.600 |
| p99 | 103.618 |
| Maximum | 123.906 |

There are 25,047 one-second resource samples. The following are per-process
observations; population and readback use different engine processes.

| Process / phase | Maximum sampled RSS, KiB | Observed user CPU, s | Observed system CPU, s |
| --- | ---: | ---: | ---: |
| N1 population | 164,728 | 979.98 | 1,735.36 |
| M1 population | 451,640 | 483.52 | 186.00 |
| Rust supervisor, whole campaign | 220,184 | 325.57 | 126.20 |
| N1 readback | 62,288 | 413.21 | 846.56 |
| M1 readback | 251,616 | 2,211.42 | 3,057.87 |

N1 and M1's maximum observed process RSS were **160.87 MiB** and **441.05 MiB**.
The maximum combined sampled RSS for both engines and the Rust supervisor was
672,768 KiB; independent process peaks must not be added as though simultaneous.
One-second sampling may miss peaks and final CPU work; shared mappings can be
counted more than once. CPU figures are sampled cumulative counters, not GNU
time totals. Raw five-second Linux waits, disk and pressure observations are
also retained.

Host page cache was not dropped. Process restart does not establish a cold-cache
read benchmark. Required io_uring startup also does not establish that every
small-object transfer used O_DIRECT. This is one current-profile population
measurement, **not a matched old/new architecture comparison or an all-operation
performance acceptance pass**. The recorded batch tails remain part of the
result.

## Evidence and reproduction

All paths in this section are relative to the retained SSD run root on
`ssd-test.example.invalid`:

| Evidence | Location |
| --- | --- |
| Driver terminal pass, including smoke and million results | `reports/PASSED.json` |
| Rust million-object terminal oracle | `datasets/million-api/api-PASSED.json` |
| Complete phase status | `reports/status.json` (`complete`, `passed`) |
| Workload log and latency/resource summary | `reports/million-api.log`, `reports/million-api-summary.json` |
| Original ACKs and per-batch samples, eight files each | `datasets/million-api/api-acks-00.jsonl` … `07.jsonl`; `api-latency-00.jsonl` … `07.jsonl` |
| Per-process and kernel observations | `datasets/million-api/api-resources.jsonl`, `reports/million-api-kernel-observations.jsonl` |
| Build and component regression logs | `reports/build.log`, `reports/component-regression.log` |
| Machine, devices and source provenance | `reports/environment.txt`, `reports/physical-disks.txt`, `reports/source-file-verification.txt` |
| Final audits and process cleanup | `reports/final-terminal-audit.txt`, `reports/final-artifact-audit.txt`, `reports/final-units.txt` |
| Exact headless orchestration | `run-campaign.sh`, `reports/RUN-PLAN.md`, `reports/systemd-unit.txt` |

No terminal `reports/FAILED.json` was produced. The final audit independently
rechecked the terminal counts and frozen executable hashes. The source archive
`source-checkpoint.tar` has SHA-256
`184eed63c9d866716b9ba46f556ed0f08203ac19fc802f257bdaa1f3ca55dab2`.

| Frozen artifact | SHA-256 |
| --- | --- |
| `litiaina-n1` | `344cc1d036f66ed7845b995693b4abc4edb2258fe4cd05921e5ac73e28ed2866` |
| `litiaina-m1` | `a93ae2363ad416ce3b8447df8c38b7df0f3afce936b93374d75fb1d110d95d24` |
| `noa_qualification` | `ddb9d0121099062538c0823ef4c0ef0d7b262915a12baa8215cc57df3cca7b45` |
| `qualification_observer` | `e29a74eaf83a2cfcb0f624fd1dc30ba39530df52dde9f352a723ae931a24a1fc` |
| `qualification_summary` | `b1437fab102671364d7f72a6731d65ee04f02d92153592cd1ce118789637b2f7` |

The exact driver retains all environment variables, mount checks, smoke gating,
resource capture and terminal validation. Its core commands were:

```sh
cargo build --release --locked -p n1 -p m1 \
  --bin litiaina-n1 --bin litiaina-m1 \
  --example noa_qualification --example qualification_observer \
  --example qualification_summary
cargo test --release --locked -p n1 -p m1 -p n1_link \
  -p n1_native_client -p procyon-engine --lib -- --test-threads=4
```

For a separate reproduction, first choose **fresh private paths** on the SSD
and both HDDs. With the tested executables in `ARTIFACT_ROOT`, the million-object
invocation is:

```sh
N1_IO_BACKEND=uring "$ARTIFACT_ROOT/noa_qualification" \
  --root "$FRESH_SSD_ROOT" \
  --first-root "$FRESH_HDD001_ROOT" \
  --second-root "$FRESH_HDD002_ROOT" \
  --object-binary "$ARTIFACT_ROOT/litiaina-n1" \
  --metadata-binary "$ARTIFACT_ROOT/litiaina-m1" \
  --count 1000000 --mode api --writers 8 --readers 8 \
  --metadata-max-concurrent-io 4
```

The completed datasets must remain unchanged as evidence. This campaign injected
process kills **after population**, not host power loss, active-write crashes,
disk removal, corruption or interrupted purge. It does not qualify four-HDD
3+1/2+2 layouts, large-file speed, quota saturation, Console population or
maintenance overhead under load. Those results remain separate in the
[RC.2 completion checklist](RC2-IMPLEMENTATION.md).
