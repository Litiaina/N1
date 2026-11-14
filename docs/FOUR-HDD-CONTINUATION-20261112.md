# Four-HDD qualification continuation — November 12, 2026

**Original campaign continuation only; RC.2 remains unreleased.** Completed
population, post-restart readback and both full member-loss readbacks now pass
within their recorded scope. The remaining natural-expiry cleanup and final
censuses **completed on November 12 on the SSD/two-HDD test systems**, using the original binaries and
byte-matched database. See [final closure and current-candidate checks](TWO-HDD-QUALIFICATION-20261113.md).
No fresh population, commit, push, tag or
release is authorized by this continuation.

## Placement and preservation

The new private four-HDD test root is `/mnt/DISK0/n1-rc2-continuation-20261112-01`.
Source, compiled helpers, imported evidence and a recovered copy of the stopped
M1 database are on its SATA XFS HDD. Rust 1.92 builds use the previous campaign's
task-local Cargo cache, target and vendored dependencies. Installed services,
global toolchains and existing campaigns are untouched.

The portable checkpoint remains unchanged at
`/srv/test-evidence/portable/n1-m1-rc2-preserved-20261112.PLPlQD`. the four-HDD
reference system received its frozen source, original ACK journals, endpoint
identity, reports and compressed M1 database. All four database files match the
original stopped-file SHA-256 manifest before opening. The original compressed
import remains intact.

Both original USB XFS HDDs were mounted by the operator on the workstation.
Their UUIDs were verified against the preserved checkpoint. N1 served the
original local shards with member 1 detached for the remaining loss readback;
the original directory is now restored. No data, marker or
LINK identity was regenerated. New local evidence lives at
`/mnt/HDD1TB001/n1-rc2-continuation-20261112-01`.

## Retrieved completed the four-HDD reference system results

The previous run, `/mnt/DISK0/n1-rc2-four-hdd-20261111-01`, completed before the
operator left its network. Its final markers and raw reports now confirm:

- 21 selected real-service checks passed, including four export lifecycle and
  cleanup-error cases.
- The 4,096-object M1 → N1 campaign passed crash/reopen, every original payload
  and version, and exact metadata/physical census: 8,192 shards, no unfinished
  publication, staging or unreferenced shard.
- Native storage regression passed 19 cases with zero failures and one
  parent-invoked child-helper exclusion. Documentation checks succeeded with
  no runnable examples.
- The eight-phase same-database/same-N1 export screen completed. Its mean
  upload sums are 12.142 s baseline and 11.863 s candidate; rename is 3.536 /
  3.535 s and trash 3.450 / 3.505 s. M1 CPU is 0.720 / 0.730 s per phase.
  Each phase contains 100 objects. Broader performance acceptance remains open;
  these results do not replace the earlier failed screens or native-M1 CPU gate.

Detailed measurements and frozen executable hashes remain in the
[November 11 handover](RC2-HANDOVER-20261111.md).

## Original million-object continuation

The recovered M1 copy passes the explicit full database integrity check:
**7,001,423 records**, with 4,673,335,380 reported disk bytes. This checks the
native database structure and records; it is separate from object readback.
Evidence is `reports/ssd-hdd-million-integrity.json` and its GNU-time report.

The first strict metadata census rejected unfinished publication intents after
finding the original million acknowledged versions. The original failure log
and generated reference list are retained; that list is marked unaccepted.
Diagnostics identify **192 incomplete, unacknowledged attempts**, all for indices
500,672–501,055 around the midpoint crash. None is completed or matches its
logical entry's acknowledged version. Their actual stored expiries are
November 12, **21:49:23–21:49:32 Asia/Manila**. A zero-pending census cannot pass
before their lifecycle completes. These expiries and the real clock must be
preserved. Final census must follow normal configured lifecycle reclamation;
no incomplete census is counted as a pass.

The remaining member-1-loss readback **passed on November 12 at approximately
13:37 Asia/Manila**: **1,000,000 objects**, **13,968,125,000 bytes**, **zero
payload mismatches** and **zero version mismatches**. Verification took
**11,757.437 seconds / 3 h 15 min 57 s**. The verifier and local loss-fixture
controller both exited 0, the original ACK hashes stayed unchanged, and the
original member directory was restored. The task-owned N1 and M1 services are
stopped before the final-stage controller waited for actual TTL expiry.

`original-member-1-only/PASSED.json` has SHA-256
`aef7111641de2e1527bbb994e2e93e3d11a8b66e4a78c67d86aeca4a3f3661c5`;
the preserved local copy matches the four-HDD reference system. This is completion of the interrupted
phase against original data, not a new population or a matched performance
comparison. Final cleanup began after **21:52:32 Asia/Manila**
(three minutes after the latest recorded intent expiry), without changing TTLs
or the clock. Normal automatic GC reclaimed all **192** pending attempts with
zero failures/deferred work. The stopped-database census then confirmed
**1,000,000 names, 1,000,000 immutable versions, 1,000,000 unique LINK identities
and zero pending publications**; the offline integrity check verified
**7,000,272 records**. The two-HDD physical census found **2,000,000 shards**,
zero staging, zero unreferenced shards and zero wrong placements. Original ACK
hashes remained unchanged. All final controllers exited 0.

The interrupted second member-loss readback ran again from zero against all
original ACKs. A read-only Rust helper reuses the existing payload/version
oracle while allowing a recorded API-address change. It checks the original
endpoint digest against the frozen campaign, refuses changes to identity or
credentials, hashes ACK journals before/after, never provisions a fragment and
never uploads an object. It records the rebuilt verifier separately instead of
editing the frozen campaign's executable binding. Its provenance regression
passes on the four-HDD reference system, as does strict release-profile Clippy.
This verifies the same original million identities. The earlier 720,000 counter
has no per-index checkpoint, so the interrupted phase started its readback at
zero. No completed readback phase is repeated.

N1 uses its exact original executable and the original two HDDs on the
workstation. M1 uses the byte-verified preserved database on the four-HDD
reference system with the same-source reference executable: the original SSD/HDD
executable requires newer glibc. The changed host, compiler, binary hash and API
address are recorded separately. N1 listens at `object-test.example.invalid`; M1
verifies its leaf against the original CA. Original credentials, ACKs, versions
and LINK identity remain unchanged.

Current evidence is `reports/ORIGINAL-ONLY-STATUS`,
`original-member-1-only.log`, `original-member-1-only.time` and
`original-member-1-only/{prepared,PASSED,FAILED}.json` on the four-HDD reference
system. A pass requires the full count, bytes and version checks, plus unchanged
ACK hashes. The first local launcher requested a descriptor limit above the
host's hard limit; no verifier ran in that attempt. Its log and the remote
startup timeout are preserved. The corrected launcher respects the existing hard
limit.

The detached local `evidence/finish-original-only.sh` preserved the verifier's
reports and returned the original member directory. The final SSD/HDD
controllers were frozen as `source/finish-original-ssd-hdd02.sh` and
`source/finish-original-metadata-ssd-hdd02.sh` under
`/mnt/HDD1TB001/n1-rc2-ssd-hdd-qualification-20261112-02`. They reused the
original stopped SSD test system database after verifying all four files against
the portable checkpoint, and the exact original N1/M1 executables. the four-HDD
reference system was not contacted for these final stages. Their reports retain
the previous stopped launchers too; `ORIGINAL-FINAL02-EXIT=0` and
`ORIGINAL-CAMPAIGN-COMPLETED` identify terminal success. No payload readback or
population was repeated for census closure. This completes the original campaign
at its frozen checkpoint, not a million qualification of the subsequently
changed finalization runtime or an RC.2 release.

## Extra campaigns stopped after scope correction

Additional populations were started in error instead of continuing only the
preserved campaign. The operator explicitly rejected this work. The extra
controllers, their queued successors and the bulk shard-copy processes are
stopped. Their partial evidence is retained; none is a completed million-object
result or a prerequisite for finishing the original campaign.

The native scale helper retains the existing bounded workers, intent/ACK
journals, deterministic bytes, SHA-256 readback and process-crash logic. Its
test-only CLI now accepts selected independent filesystems and `--objects N`,
instead of binding qualification to obsolete workstation mount paths. It
refuses the system filesystem; this test bound is not an object-engine limit.

The first 4,099-object native attempt hit `EMFILE` after 1,024 ACKs: the launcher
inherited SSH's soft descriptor limit of 1,024 instead of the packaged N1
service's 1,048,576. Its error, fixture and ACKs are retained. With the packaged
limit applied only to the task process, the same frozen engine/client resumed
that fixture, preserved all prior ACKs and verified all 4,099 original identities
after SIGKILL. The separate physical interruption/corruption witness also passed,
including refusal to overwrite damaged acknowledged data and byte-exact journal
preservation. No engine code changed to obtain these results.

The current Console source and dependencies were frozen from commit
`89fd5b9e7f41cab7df418e39048ee6b43506835c`, including its existing working
changes, and freshly built on the four-HDD reference system. No Console
implementation was edited. Node 22.23.3 and the already available Chromium
149.0.7827.55 runtime are task-scoped; no system package or deployed service
changed. The real production Console preflight passed **128 ACKs**, both-service
SIGKILL/reopen, all original payloads and versions, and exact census: **256
shards**, zero staging, unreferenced shards or pending publication. It used
1,787,920 logical bytes and one real batch request; its browser report has zero
failed/unknown/scanner results and zero retained upload items on completion.
This is a preflight, not a million pass or a physical million-entry directory
traversal.

| Extra stage | Preserved state after operator correction |
| --- | --- |
| Standalone N1 | Stopped around 120,320 ACKs. Partial population and journals retained; not a million pass. |
| Bulk original-shard copy | Stopped with partial archives. No complete-transfer claim. Original local HDDs remain the continuation target. |
| Fresh the four-HDD reference system M1 → N1 | Queued controller stopped; do not start a replacement population. |
| Original Console → M1 → N1 | 128-object preflight passed. Queued million campaign stopped. |
| Additional post-GC million readback | Queued controller stopped. Do not repeat completed readbacks. |

Historical `STANDALONE-*`, `ORIGINAL-CONTINUATION-*`, `OWNED-MILLION-*`,
`CONSOLE-CAMPAIGN-*`, `POST-GC-READBACK-*` and archived
`CURRENT.stopped-extra-campaigns.json` describe those stopped controllers.
The replacement `CURRENT.json` identifies only the original continuation and
links its live progress/status files. The scope
correction is retained in `OPERATOR-SCOPE-CORRECTION`; operator termination is
not an engine failure. Partial archives do not replace original data. Process
termination and unavailable directories do not establish physical host
power-loss resilience.
