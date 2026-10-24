# MCH scale qualification

## Dedicated small-file batch candidate

The October 24 relaunch below was intentionally stopped and preserved at
**128,160 acknowledged versions / 1,870,110,720 bytes** before changing batch
commit implementation. It is not a million-object pass. Production services
and all existing test evidence remain untouched.

The replacement headless supervisor started **October 24 at 17:27 Manila** with
frozen candidate `a6415893` and verified package hashes. Its first gate stopped
with **425 passes / one failure**: a key-lock cleanup test required the shared
registry to be empty while other tests could hold unrelated locks. The isolated
test passed, but the failed parallel gate remains preserved. The comparison and
million-object stages never started.

The assertion now checks its own canonical keys while an unrelated key remains
held. This test-only correction leaves the engine binary unchanged. The fresh
frozen test executable started headlessly on MCH at **17:47 Manila**; all 426
library tests passed together in 257.603 seconds. Backend selection passed
four cases with two opt-in cases ignored, and the separately required native
io_uring integration passed. The full-64-object strict-batch gate passed all six
error/kill rounds across four filesystems, with 106 successful ACKs and 960
historical hash checks; no injected-fault operation falsely succeeded.
Scheduled GC passed, and RC concurrency stress is running. Later native-protocol,
read-repair, comparison and scale gates remain pending. No scale pass is claimed.
Production service PIDs were unchanged at launch.
The final warning-denied build, focused executor/backup tests, explicit native
integration and 109 harness/shim regressions passed locally. Production N1,
Accel and Console remain running unchanged.

The sequence requires, in order: final candidate tests, actual
strict-group execution, full 64-object synchronization-error/kill gates on all
four filesystems, scheduled GC/concurrency/native/read-repair gates, and a
matched 2,048-object single-folder **old/new/new/old** comparison. Only a measured improvement
and complete correctness gates allow the headless scale stage to start.

That stage uses **one million objects in one logical folder**, then churn.
Client memory remains bounded; every acknowledged historical version is hashed
and full namespace snapshots are checked against the on-disk ledger. LIST
probes cover bounded lexical prefixes, not an advertised full-million-folder
pagination latency result. Deadline/resource/capacity/interruption stops remain
incomplete qualifications. Production services must not be restarted or replaced.

## Preserved native-I/O baseline

Launched October 24, 2026 on MCH after physical-device and capacity preflight.
Four independent SATA HDDs, XFS, EC 3+1; fresh test directories only. Scheduled
GC, concurrency stress, native protocol and read-repair gates passed. The
sequence was then operator-stopped and preserved at **10,000 acknowledged
objects** to isolate a requested 5 GB throughput benchmark. The million-object
qualification is **incomplete**, not passed or still running. Existing
production service PIDs were unchanged. No formatting, reboot or production
deployment was performed. Do not use the older two-member
`n1-hdd-qualify.py` wrapper.

A **fresh headless relaunch** subsequently started on October 24, using new
fixture parents and the same qualified engine. Its scheduled-GC gate passed;
RC stress was running at the initial check. The sequence automatically proceeds
through native/read-repair gates to one million **0–64 KiB** uploads, followed
by hot-set churn. This is a small-object performance/scale baseline, not a new
engine optimization or an old/new performance comparison. Results are pending.
Both earlier populations and the completed large-file results remain preserved.

The frozen candidate is `ad139db1`; the package records its complete SHA-256.
The new headless supervisor has a 50-hour total limit, 4 GiB memory limit, no swap
and no automatic restart. Each filesystem retains at least 140 GiB or 15%
free, whichever is greater, with a separate 64 GiB growth ceiling. The scale
stage has a 48-hour limit within that overall budget. The 4 GiB limit covers
the entire test service, including client, engine and charged filesystem
cache/slab; a cgroup limit event is not automatically an engine RSS leak.
A limit stop is not a PASS.

Offline preparation checks passed: 28 synthetic evidence-checker regressions
and 45 existing scale-harness regressions. No engine or server was exercised
by those checks. The evidence checker verifies recorded consistency, not the
physical server identity or a replacement payload-hash run.

## Launch checks

When the operator is at work:

1. Require the complete local candidate validation to pass. Freeze the engine,
   proof tool, Python helpers, default configuration and their SHA-256 hashes.
   A changed or server-rebuilt binary requires its own correctness gates.
2. Inventory MCH's kernel, CPU/RAM, filesystem versions and mounts. Record
   UUID, serial, capacity and transport for **four independent physical
   devices**. Four partitions or logical volumes on one drive do not qualify.
   Confirm kernel/process permissions allow required io_uring.
3. Approve a new isolated directory on each member filesystem, outside enrolled
   production roots. Never reuse an existing test directory. Keep results,
   source files, temporary files and acknowledgement ledgers on approved data
   storage, not an accidental OS-filesystem fallback. Existing services/data
   remain untouched; shared production load must be recorded.
4. Reserve at least `max(128 GiB, 15% of capacity)` on every filesystem. Start
   with an explicit 64 GiB per-filesystem growth budget, including other
   writers; require that budget **in addition to** the reserve before launch.
   Measure actual allocation at early checkpoints before considering expansion.
   Check finite inode headroom separately: allow conservatively six million
   fixture inodes per member, extra source-file inodes on the source volume,
   and retain at least one million or 10% in reserve. XFS inode capacity is
   dynamic; do not treat its report as a fixed ext4 inode ceiling. The existing
   runtime guard checks bytes/mounts, not continuous inode availability.
5. Confirm unused test ports, descriptor limits and a headless supervisor with
   a fixed time/resource budget and no automatic restarts. Only test-owned
   processes and fixture files may be interrupted or damaged. Do not unplug
   devices, unmount shared filesystems or power-cycle MCH.

Read-only inventory commands, after selecting the four actual mount paths:

```sh
lsblk --json --bytes --output NAME,PATH,TYPE,SIZE,MODEL,SERIAL,TRAN,FSTYPE,UUID,MOUNTPOINTS
findmnt --json --output TARGET,SOURCE,FSTYPE,UUID,OPTIONS
df -hT -- "$M0" "$M1" "$M2" "$M3"
df -i -- "$M0" "$M1" "$M2" "$M3"
```

Mount/UUID/physical-device mapping must be checked on MCH, not inferred from
the laptop. Recheck it during the run. Stop rather than fall back to the OS
filesystem if a member disappears.

## Sequence

Run gates serially; stop on the first failed gate and retain its evidence.

1. Repeat required-native correctness and EC 3+1 scheduled-GC/recovery proofs
   in **separate small fixtures**. Use `n1-gc-scheduled-proof.py` and
   `n1-rc-stress.py` with four member parents, `--data-shards 3
   --parity-shards 1 --io-backend uring`. Verify actual orphan/trash reclaim,
   active-upload protection, historical hashes and interruption outcomes.
2. Run explicit read-repair/bitrot and native-protocol proofs with the frozen
   Rust proof tool and four fresh fixture roots. Require native startup logs
   and completed operations for every relevant owned PID. The older
   `n1-hdd-recovery.py` removes `N1_*` environment variables and the overload
   wrapper defaults to `auto`: exporting `N1_IO_BACKEND=uring` alone does not
   make those wrappers required-native evidence. Rust proof payloads include
   relaxed uploads; do not relabel them strict-acknowledgement qualification.
3. Populate **one million unique source objects first**, with strict durability,
   required io_uring and zero mutation/probe clients. Keep the established
   eight clients, 64-object batches and server batch concurrency 16.
4. Verify every acknowledged historical version at 1k, 10k, 100k, 500k and 1m;
   record interval ingest separately from verification. Check sampled-prefix
   paginated LIST, a complete streamed namespace/MAX-ULID inventory and
   resource checkpoints.
5. Only then create 1,000 bounded probe objects in that same populated store.
   Ramp rename/move/trash/recover through 8/32/128/256 clients, reaching at
   least one million successful mutations. Verify immutable probe NDX/payload
   files and all acknowledged bytes. SIGKILL/restart only the owned test
   engine, then repeat complete version and namespace verification.

The existing `--churn-after-upload` mode performs steps 3–5 without changing
N1's engine or format. It churns a hot set **while the million source objects
remain present**; it does not mutate every source key or add concurrent new
source uploads after reaching one million.

## Population/churn command

`ENGINE` is the hash-pinned binary. `P0`–`P3` are approved fresh test parents on
the four verified filesystems. `RUN` is an existing, isolated evidence parent
on approved data storage; `RUN/scale` must not exist. `PRIOR_PASS` is the
completed required correctness gate, not a RUNNING or partial result.
`FREE_FLOOR_GIB` is the conservative common reserve computed above.

```sh
python3 -B tools/n1-hdd-scale.py \
  --binary "${ENGINE:?}" --io-backend uring \
  --root "${RUN:?}/scale" \
  --member-parent "${P0:?}" --member-parent "${P1:?}" \
  --member-parent "${P2:?}" --member-parent "${P3:?}" \
  --data-shards 3 --parity-shards 1 \
  --objects 1000000 --churn-after-upload \
  --probes 1000 --churn-mutations 1000000 \
  --batch-size 64 --batch-clients 8 --batch-concurrency 16 \
  --port 58681 --request-timeout-seconds 300 \
  --deadline-seconds 172800 --free-floor-gib "${FREE_FLOOR_GIB:?}" \
  --max-filesystem-growth-gib 64 --wait-for "${PRIOR_PASS:?}"
```

Set `TMPDIR`, `TMP`, `TEMP` and `SQLITE_TMPDIR` to an owned directory on the
approved evidence filesystem. Use an explicit headless time/memory/task/FD
limit and disable core dumps to the OS disk. Do not disable TLS validation,
lower durability, retry failed writes or reduce the target to obtain a PASS.
The 48-hour deadline is a limit, not a promise the workload will finish.

After the runner exits successfully, validate its retained evidence:

```sh
python3 -B tools/n1-scale-result-check.py \
  --result "${RUN:?}/scale/results.json" \
  --expected-engine-sha256 "${ENGINE_SHA256:?}"
```

## Reporting and next gates

Record upload objects/s and bytes/s; batch HTTP latency separately from source
preparation/ACK logging; engine versus runner CPU/RSS/FD; queue/stall/pressure
telemetry; physical-device latency/utilization; allocated bytes/inodes; and all
errors. Keep raw acknowledgements, ambiguous outcomes, configurations, hashes
and process exit evidence. A deadline, capacity, resource or operator stop is
an incomplete qualification, not a smaller completed million-object PASS.

This first gate is **million-object residency plus hot-set churn**. It does not
certify a full flat million-key paginated LIST, completed GC at that population,
post-million mixed ingestion, fault injection into the populated store,
backup-at-scale, a long idle/memory soak, browser/Console scale or LAN throughput.
Those remain separately measured gates; small recovery fixtures cannot stand
in for them. SIGKILL is not a physical power cut.

Only after the million gate passes should a new measured budget target
2/5/10-million-style checkpoints. Stop short of the production reserve. Neither
the planned object count nor an extrapolation certifies hundred-million scale,
constant memory at arbitrary scale, RC.2 or stable release.
