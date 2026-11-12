# RC.2 qualification — November 11, 2026

**RC.2 remains unreleased. Performance gate 1 is still open.** Qualification
resumed on MCH on November 12. See the [continuation report](MCH-CONTINUATION-20261112.md)
and [home split-node report](SPLIT-NODE-20261111.md).
No commit, push, tag or release is authorized by this qualification.

The home profile has since passed 378 fresh workspace checks, both-process
SIGKILL/reopen/readback, each of the two members' loss, metadata snapshot/loss/
restore and an exact fresh census preflight. Its eight-phase screen leaves
performance acceptance open. The owned-M1 million campaign was **stopped by the
operator on November 12 for travel**. Population, full post-restart readback and
the complete member-0-loss readback passed. Member-1-loss readback reached
720,000 reported checks before interruption. On November 12, that unfinished
phase resumed against the original N1 shards and byte-verified M1 checkpoint/
ACKs on MCH, then **passed all 1,000,000 original objects / 13,968,125,000 bytes**
with zero payload/version mismatches in **11,757.437 seconds**. Only normal
expiry cleanup and final censuses remain. No completed phase was repeated;
no fresh population is authorized.

The portable checkpoint is
`/home/altear/DEV_TEST/n1-m1-rc2-preserved-20261112.PLPlQD`.
Its `README.md` explains completed results, source/binary provenance, the
verified compressed M1 database, original ACKs and how to continue on MCH.
Original N1 shards remain on the HDDs; they are not duplicated onto the NVMe.
HDD1TB002 was unmounted at stop; both original HDDs are now mounted. Member 1
has been restored after its successful loss readback. No dataset was
wiped. Extra campaigns mistakenly started on November 12 are stopped; their
partial evidence is preserved separately from this original continuation.

## Resumed on MCH

Testing resumed on November 11 on `administrator@192.168.0.12`. The private run
root is `/mnt/DISK0/n1-rc2-mch-20261111-01`; the corresponding private directories
on DISK1–3 hold test members. Existing deployed N1 services are not test targets.
Builds and datasets remain on these HDDs, outside the system NVMe.

The rejected cache prototype is now **archived, not in the runtime working
tree**. The storage implementation and its architecture descriptions were
restored to committed `29bca4b`; its complete experimental patch/source remain
in the portable files below. The shared-key candidate is isolated on MCH.

All three M1 binaries, the fixed N1 binary and common release-profile Rust
client were built on MCH using Rust 1.92. The shared-key candidate passes 19
storage tests with zero failures and one parent-invoked child-fixture exclusion.
All 24 comparison phases completed: four repetitions of three variants at 100
and 1,000 small objects, with a verified 256 MiB multipart object in every run.
This compares M1 implementations over the same N1 LINK binary, not NCX versus
LINK. Mean sums of individual request latencies and whole-workload M1 CPU are:

| Objects/run | M1 | Small PUT sum | Rename sum | Trash sum | M1 CPU | Peak M1 RSS |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 100 | Native | 11.088 s | 3.184 s | 3.438 s | 1.190 s | 47,620 KiB |
| 100 | Procyon | 12.247 s | 3.709 s | 3.787 s | 1.140 s | 47,248 KiB |
| 100 | Shared-key experiment | 13.286 s | 4.156 s | 4.120 s | 1.353 s | 48,712 KiB |
| 1,000 | Native | 112.047 s | 33.468 s | 33.995 s | 7.738 s | 49,872 KiB |
| 1,000 | Procyon | 119.054 s | 36.721 s | 37.055 s | 5.135 s | 52,532 KiB |
| 1,000 | Shared-key experiment | 128.912 s | 41.599 s | 41.913 s | 7.830 s | 51,208 KiB |

Native M1 has lower mutation latency here, but **50.7% higher M1 CPU** at 1,000
objects. Earlier SSD regressions remain relevant. Overall performance acceptance
remains open. The shared-key experiment is slower and rejected; the database
source remains committed `29bca4b`.

`reports/STATUS`, `controller.log`, per-phase JSON/logs and `BINARIES.sha256`
record the performance campaign. All **17 selected real-service checks pass**
with committed native M1 and the fixed N1: metadata loss/restore, history and
permissions, crash/durable resume, quotas, archives, indexed GC, monitoring and
metadata backup, upload-versus-scrub classification, large resume, TLS and
upload-size policy. These are scoped checks, not complete NOA conformance.
`reports/SUMMARY.md` and `summary.json` collect the completed comparisons;
a detached reporting controller refreshes them after performance measurements
and updates the summary again when reliability completes or fails. All three
controllers survive an SSH/client disconnect. Raw per-request samples remain
the primary timing evidence.

The new four-member Rust witness checks distinct filesystem devices, all member
loss combinations through the first count beyond parity, damaged payload-frame
reconstruction, exact repaired-shard hashes, beyond-parity refusal, and one/two
simultaneous blank replacements followed by process restart/readback. It targets
only new private directories; it does not disconnect shared disks or simulate
host power loss. Its local compile and strict N1-test Clippy checks pass; actual
four-HDD attempts and their logs are retained:

| Attempt | Finding |
| --- | --- |
| `four-member-matrix.log` | Non-stripe-aligned fixture part size for 3+1 correctly rejected. Fixture corrected. |
| `four-member-matrix02.log` | N1 correctly refused cold startup beyond parity. Fixture now requires this precise refusal; live API refusal is also checked using corruption. |
| `four-member-matrix03.log` | 3+1 passed. Two lost members in 2+2 exposed offline replacement's requirement that every other peer be present. |
| `four-member-matrix04.log` | **Passed 3+1 and 2+2**, all 24 member-loss combinations, exact repaired shards, beyond-parity refusal, one/two blank replacements and restart/full readback. Total witness time 16.04 s. |
| `four-member-matrix05.log` | **Passed again with all blank targets connected together**, 17.23 s. Unselected blank roots remain untouched. |

A new regression reproduced the replacement restriction. The fix permits one
operator-authorized target to be reconstructed from K validated surviving
members. Other lost or unregistered roots remain unavailable until their own
replacement command; they are never enrolled implicitly.
Logical identities, source verification, independent-filesystem checks and
maintenance fences remain. All **13 offline-maintenance unit cases pass
locally**, including absent and already-connected blank peers, refusal of
corrupt markers/insufficient quorum, unchanged surviving files,
live/empty object reads and durable multipart continuation.

The retained-export candidate adds ordered expiry rows, transactional byte
reservations, bounded restart admission and build/download leases. Cleanup
reads at most 128 expiry rows per tick, carries a cursor past leased rows,
checks disk cleanup, releases pins and atomically removes job state/accounting.
Cancellation preserves the latest stored reservation. Issued chunk writers
drain before cleanup; late workers cannot recreate removed jobs. Existing
ZIP64/TAR builders and payload verification are retained.

Five export unit cases and three new real-process lifecycle cases pass locally:
restart beyond the queue limit, exact archive/accounting recovery, expiry during
a held download, complete cleanup and capacity refusal preserving the source.
All **378 default workspace tests pass**, zero failed, with 56 explicit opt-in
or helper cases excluded. Strict workspace-wide all-target Clippy now passes:
the three inherited proof-tool findings were corrected without changing wire
behavior. Its nine argument/configuration tests pass separately. MCH has also
passed the 12 maintenance and five export unit cases from overlay 04 and all
**20 service checks**, including the three new lifecycle cases. Matched
candidate-versus-native measurements are running. Overlay 05 adds the
already-connected-blank-peer case; its full N1 library suite passes **201 cases,
zero failed, two exclusions**. Its backup witness first rejected a test root
sharing the source filesystem; corrected DISK1/DISK3 placement then passed in
11.01 s (`physical-backup-recovery08.log`). This was a fixture placement error.

The new per-artifact failure regression reproduces cleanup starvation against
the prior candidate and passes with error isolation. Failed artifacts keep
their reservation and expiry index while later jobs are reclaimed; retry after
restart releases the remaining charge once filesystem access is restored.
The latest candidate completed **21 service cases, zero failed**, before the
operator left the MCH network. `BLOCKERS08-SERVICE-COMPLETED` was recorded at
`2026-11-11T07:58:33Z`.

The completed fresh-fixture follow-up of candidate 04 fails latency acceptance: at 1,000
objects, the candidate averages **128.736 s** for uploads versus **112.810 s**
(+14.1%), **41.596 / 33.385 s** for rename and **41.557 / 34.156 s** for trash.
Whole-workload M1 CPU is nearly equal (**7.730 / 7.755 s**). These failures remain
in `bench04-*.json` and `bench04-summary.json`. The ABBA/BAAB follow-up on the
same database and running N1 was retrieved on November 12. Across four phases
per variant, each with 100 objects, mean request-latency sums are:

| Operation / resource | Baseline | Final export candidate |
| --- | ---: | ---: |
| Upload | 12.142 s | 11.863 s |
| GET | 0.222 s | 0.228 s |
| Rename | 3.536 s | 3.535 s |
| Trash | 3.450 s | 3.505 s |
| M1 CPU per phase | 0.720 s | 0.730 s |
| Maximum observed M1 peak RSS | 13,748 KiB | 13,768 KiB |

This controlled screen does not reproduce a broad export-worker slowdown.
It does not erase the failed fresh-fixture screen, the native-versus-Procyon
CPU regression or establish saturation/tail-latency acceptance.

The final 4,096-object named campaign also passed: both processes killed and
reopened, all original payloads/versions verified, exactly 8,192 placed shards,
zero staging, unreferenced shards or pending publications. Following it, native
storage regression passed **19 tests, zero failed**, with one explicitly
parent-invoked child helper excluded; documentation tests had no runnable cases.
`BLOCKERS08-COMPLETED` and `STORAGE09-COMPLETED` record actual completion at
`2026-11-11T08:01:31Z` and `2026-11-11T08:06:23Z`. The home profile has separate
datasets, binary hashes and results.

The current headless controller is `evidence/n1-mch-blockers08.sh` (PID 3393645
at launch). Read `reports/BLOCKERS08-STATUS`, `BLOCKERS08-FAILED`,
`BLOCKERS08-SERVICE-COMPLETED` and `BLOCKERS08-COMPLETED`. It runs service checks,
controlled performance and a 4,096-object named preflight. The following
`evidence/storage09.sh` controller waits for it before running the native
database crash/corruption/checkpoint suite, so it cannot distort timings.
Unfinished work is not a pass. Earlier failed markers remain historical.

| Final candidate artifact | SHA-256 |
| --- | --- |
| N1 `litiaina-n1-replacement05` | `b109b9f2307f42f3b25929ffca73a6bb313259f4529457e80b03576a0afe14d4` |
| M1 `litiaina-m1-blockers08` | `42afd755687e5d664bb8981ff998149f0a00b0741de40d0a69c231aeea10facd` |
| Rust service harness `noa_service-blockers08` | `001b27af9d1762f9520fb15470e0538958c373a4506136f16a851ad2e0fea309` |
| Rust scale harness `noa_qualification08` | `5c46154866d2f469b1bd49bd6b544f76689813bd3c7fad75351fd835bcd2305e` |

## Remaining gates

- Performance acceptance: native M1 CPU cost, the failed fresh-fixture export
  screen, saturation and tail latency. The controlled same-database follow-up
  is complete within its 100-object scope; broader performance acceptance is open.
- A fresh million **named objects with owned M1**, separate standalone N1
  million and original Console population. Neither million KV fixtures nor the
  November 8 Procyon-backed named-object pass substitutes for these results.
- Complete NOA field/error/permission/retry coverage, the remaining native
  direct-archive response limit, conditional Accel and final Console/operator
  workflows under load.
- Operator-controlled physical power loss. SIGKILL, missing directories and
  blank replacement roots do not simulate host power loss.

## Preserved experiments

Committed baseline: `29bca4b8e120bded2223c1ed911b97a9e451fe23`. Native database
source remains unchanged; the accepted/rejected distinction above concerns
M1's export service and isolated performance experiments, not LINK placement.

The rejected record-cache and shared-tree-key source, original handover,
compact raw reports and reproduction scripts remain in
`.git/rc2-work-20261111/`, with `SHA256SUMS`. The portable evidence archive's
SHA-256 is `4bee72752f2dcf6c43820b963aaac04b9e36e3a9523d6bd764a75a62d5e64976`.
Original main-PC data remain at
`/mnt/DATA_SSD/m1-owned-storage-20261110-01/optimization12`; their earlier SSD
regressions are not replaced by the MCH results. See
[M1-STORAGE-20261110.md](M1-STORAGE-20261110.md) for that historical campaign.

New MCH source overlays, exact frozen executables, orchestration and raw logs
remain under `evidence/`, `artifacts/` and `reports/` in the run root. Local
workspace/lint results and positive/negative regressions are preserved in
`evidence/local-checks08.tar.gz`. Workload and fault logic is Rust. No Python,
commit, push, tag or release is performed.
