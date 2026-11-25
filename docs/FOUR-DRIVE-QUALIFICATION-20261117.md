# N1 RC.2 and M1 RC.1 four-drive qualification — November 17, 2026

**Four-drive recovery/operator checks and three coordinated physical
power-loss windows passed. No release or tag.** This continues the current N1 checkpoint
and qualifies M1 against four physical SATA HDDs. Earlier million
populations and four-drive campaigns remain preserved with their original
artifact identities; none is restarted by this stage.

## Hardware and isolation

The reference host has a Core i5-11400, six cores / twelve threads,
approximately 11 GiB RAM, Linux 6.8.0-101-generic and glibc 2.39. N1 members use
four independent XFS filesystems on one ST1000DM010-2EP1, two WD10EZEX-21W
and one WD10JPVX-00J 1 TB SATA HDDs. M1's database, staging and snapshot restores
use the separate Kingston SNV2S500G NVMe/ext4 filesystem. Builds, sources and
primary reports use a private directory on the first HDD; portable evidence is
copied to a separate computer's evidence HDD.

No filesystem is reformatted or unmounted. Member loss detaches newly created
private directories; corruption targets only the corresponding private shards.
Directory/member faults target only the private datasets. CPU policy and
ordinary competing activity remain unchanged. The later physical-cut window
explicitly includes the whole host and all four drives, with the operator's
authorization for existing services to go offline.

N1 stays at SHA-256
`f1a54f1653c1c90ae51416fa1f53474855cc20e1ea7ef52e0561af2c8c166aab`,
version `1.0.0-RC.2`. Its previously qualified Rust 1.98.0 executable is reused
exactly; successful native startup confirms io_uring with no fallback. No N1 runtime,
LINK, ECS Core, checksum, shard format or dependency changes are made.

The source base is `d120537`. The M1 baseline and correction are built with
Rust/Cargo 1.92.0 and identical release/LTO settings with symbols retained.
The ordinary M1 build contains no request profiler. M1 remains version
`1.0.0-RC.1`; version declarations do not create releases.

| Artifact | SHA-256 |
| --- | --- |
| M1 compiler-matched baseline | `83331e88d1d39095655f06414297077c439e482b2712339475c457ec183c8a73` |
| Corrected M1 | `1b40a121e706a4dc7b71844a962b02fe8b96a5470e4f95dc72388f2be11f77f5` |
| Standalone four-member Rust witness | `d28414a9109c120cdc809240cba91265341cde86fe144ca8d32aefd3afc8299d` |
| Fixed throughput client | `800d8463962423af4dbb589341c0b5a7986717746f3cf45ebc1d77e1ed55dfc3` |
| Final four-drive service harness | `a8c14b33a33bd60559f23acf085a7505e14e48937a0e67e1d80dfe254ad79d98` |
| Independent Rust ACK recorder, Rust 1.98.0 | `385c9e3a15900945c7ef8a6da1f2c64a9528698dc876a5afa37f308022fe9f34` |
| Extended recorder and deep auditor, Rust 1.98.0 | `1ac650af50cc8e809d5e09900aa409a4ccf89b331f8efa5fd750e095b6e305d1` |

## Reproduced M1 connection failure and correction

The wider pool exposes a missing M1 recovery case. Its original crash fixture
uses 128 KiB external parts. Those are native-aligned in 1+1, but require M1's
multipart bridge with a 3+1 pool and 4 KiB blocks. When N1 alone restarts, M1's
bridge can retain an idle native socket to the stopped process. Finalize's
read-only allocation-status probe then returns HTTP 502 before conversion.
The failure repeats on three preserved baseline attempts, including an exact
request-route diagnostic. This is an M1 client-recovery failure, not a failed
N1 reconstruction or an acknowledged-data-loss result.

M1 now reconnects once for a disconnected read-only status, stat, health or
object-health control. It bypasses other idle sockets while retaining the
operation's existing admission/shutdown ownership. Healthy operations send the
same single RPC. Upload allocation, payload writes, finalization, repair and
purge are not added to this replay rule. Complete native errors, including 404
and authorization failures, remain visible; a second disconnect fails.

Four targeted Rust transport tests verify successful status recovery, the
single-retry limit, no allocation replay and no retry of a complete application
error. The existing authentication/idle/revocation tests also pass. The same
six-case real-process crash matrix then passes on both 3+1 and 2+2, retaining
every prior ACK, rejecting conflicting bytes and verifying all completed objects.

Only M1's native adapter changes at runtime. The test fixture gains explicit
four-filesystem geometry. Its optional negotiated upload path uses the returned
part size/count and preserves logical source offsets across unaligned boundaries.
A separate source-oracle test verifies that boundary handling.

Two inherited fixtures also contain two-drive geometry assumptions. The expired
native-upload case initially requests a part smaller than a 3+1 stripe and
correctly receives 400. The complete-metadata/staging-loss case initially uses
bridged parts, whose lost M1 payload files cannot be reconstructed from metadata
receipts alone. Their corrected fixtures use native-aligned parts, preserving
the original witnesses' purpose without weakening assertions or changing either
engine's data guarantees. Original failed logs remain retained.

## Completed checks

| Check | Result |
| --- | --- |
| Standalone N1 physical 3+1 / 2+2 member fault matrix | Passed: 24 loss combinations through the first count beyond parity; eight exact corruption/repair checks; beyond-parity refusal; one/two simultaneous blank replacements; final cold-process readback |
| Twelve selected M1 public-service cases on 3+1 | 12 passed, zero failed |
| Same twelve cases on 2+2 | 12 passed, zero failed; hardware repetition, not twelve additional distinct cases |
| Current Console on 3+1 | Operator authorization, backup create/list/download, restored versions/trash and all four original upload/resume workflows passed |
| Console Node checks | 46 passed, zero failed/skipped |
| M1 native adapter unit checks | Seven passed, including four new recovery/no-replay checks |
| Logical source across negotiated boundaries | Passed |
| Additional recovery cases | Five passed: complete metadata/staging loss and snapshot restore; interrupted cross-topology migration; failed migration preserving its source; Console migration/trash GC; completed-staging repair on 2+2 |
| Four-phase mixed maintenance workload | Both layouts passed: 7,898 operations / 680 identities, zero request errors; full post-SIGKILL payload/version/absence checks; four restored snapshots and eight final member markers |

The twelve service cases cover original HTTP/named-native contracts, scoped
monitoring/backups, six in-flight process-crash windows, lost unused allocations,
bridge integrity/retries, interrupted conversion, resumable listing, automatic
trash TTL/outage retry, expired uploads, selected/native archives and shared
HTTP/native quotas/permissions.

The Console runs from a fresh build of the existing product source, with its
source archive and base commit retained. No Console source is rewritten for this
stage. The browser test exercises the master-key flow through the interface,
downloads its snapshot and verifies restored immutable versions through NOA.

## Concurrent maintenance observations

Each layout runs four paced 60-second offered windows, with maintenance off,
on, on and off. Normal/strict multipart, two small-upload sessions, two readers,
rename and trash continue together; maintenance adds monitoring, indexed GC and
live metadata backup. Requests already started at a boundary drain normally.
The existing workload opts into part negotiation: 3+1 selects 7.5 MiB, 2+2
selects 8 MiB. The source oracle uses logical offsets across those boundaries.

| Layout | Operations | Accounted identities | Actual workload walls by phase, seconds | Small-upload p50 / p99 by phase, ms |
| --- | ---: | ---: | --- | --- |
| 3+1 | 4,221 | 406 | 62.823 / 66.015 / 63.336 / 66.523 | 1,083 / 4,230; 994 / 4,641; 1,097 / 6,205; 1,011 / 5,875 |
| 2+2 | 3,677 | 274 | 72.435 / 65.688 / 66.034 / 65.406 | 1,239 / 5,266; 1,745 / 8,737; 1,591 / 7,112; 1,168 / 8,737 |

Each run restores two online snapshots and verifies all required live bytes,
versions and trashed-object absences after terminating/reopening the processes.
Its final scoped collection leaves only four member markers. Each also retains
two degraded monitoring samples during the second maintenance window; the
subsequent idle-health requirement passes. Cached/pending observations are not
silently relabelled as lost data. These are paced application observations,
not maximum concurrency, open-loop saturation or low-latency guarantees.

## Matched small-object control

Corrected and baseline M1 use the same compiler/settings, pool, fixed N1 and
Rust client. Order is candidate, baseline, baseline, candidate. Each phase
uploads 100 × 4 KiB at concurrency 1 and 8, repeated three times, plus three
one-shot controls. Each binary therefore accounts for 1,206 verified/reclaimed
objects, including 600 in each concurrency cohort. Tables pool the six cohorts
per concurrency using the original individual samples and nearest-rank quantiles.
No CPU/cache policy is changed and no other qualification workload/build runs
on that host during these comparisons. Existing ordinary services remain.

| Observation | Baseline M1 | Corrected M1 |
| --- | ---: | ---: |
| Concurrency 1, combined upload seconds | 122.281 | 125.633 |
| Concurrency 1 p50 / p95 / p99 ms | 138.034 / 402.084 / 2,081.299 | 133.757 / 705.960 / 1,860.134 |
| Concurrency 8, combined upload seconds | 72.682 | 85.551 |
| Concurrency 8 p50 / p95 / p99 ms | 617.368 / 2,731.353 / 4,811.262 | 634.862 / 3,579.145 / 6,290.413 |
| M1 upload CPU seconds, both cohorts | 3.87 | 3.87 |
| N1 upload CPU seconds, both cohorts | 15.30 | 15.29 |
| Highest sampled M1 RSS KiB | 38,704 | 35,896 |

The correction preserves the healthy single-RPC path and has effectively equal
measured CPU, but this window does **not** establish latency parity: candidate
concurrency-8 tails and combined wall time are worse. Retain those results
alongside the reproduced restart failure and its passing correction. They are
not evidence that the reconnect branch ran during healthy uploads, or a reason
to change N1's frozen I/O implementation without isolating a code-related cost.

## Isolated 1 GiB observations and storage control

One standalone and one M1 → N1 run per layout use the same deterministic source,
verified loopback TLS, four writers, 48 MiB strict parts and 22 parts per object.
Each verifies complete SHA-256 readback, repeats finalize, purges its identities
and checks marker-only member roots; named runs also pass M1's offline database
check. No page-cache drop is performed. These are individual observations, not
an alternating N1-versus-M1 performance experiment or object-level p99 estimate.

| Path / layout | Upload seconds / MiB/s | Init / finalize ms | N1 / M1 upload CPU seconds | Part p99 ms, 22 samples |
| --- | ---: | ---: | ---: | ---: |
| Standalone N1, 3+1 | 79.421 / 12.893 | 1.152 / 27.980 | 5.03 / absent | 26,689.036 |
| M1 → N1, 3+1 | 69.124 / 14.814 | 5.891 / 27.307 | 4.85 / 2.62 | 17,829.573 |
| Standalone N1, 2+2 | 123.521 / 8.290 | 1.053 / 29.859 | 5.87 / absent | 35,465.395 |
| M1 → N1, 2+2 | 102.093 / 10.030 | 4.830 / 32.647 | 5.86 / 2.84 | 29,065.162 |

These low transfer rates are retained. An earlier tested N1 executable, SHA-256
`27e0375ae84e1fff86bf958334aa01e5da6dad32f2f1202f7179f3ac588337f8`,
also measures only 16.943 MiB/s on the same new 3+1 fixture, with a 23.720 ms
finalize. That executable uses Rust 1.92.0, so this diagnostic is not a
compiler-matched N1 regression result. It establishes that the low current
rate is not exclusive to the selected executable.

A separate Rust paired-file control bypasses both engines. It writes private
1 GiB aligned O_DIRECT files on two independent filesystems concurrently,
synchronizes every 8 MiB and on completion, verifies every byte and removes
the files with directory synchronization. The four individual write rates are
**62.764 / 99.779 / 82.368 / 7.680 MiB/s** in member order. The last drive
takes 133.331 seconds to persist 1 GiB outside N1. This is direct evidence of a
slow storage path on this host, not proof of a particular firmware/cable fault
or an exact N1 throughput ceiling; its request geometry differs from N1.
A follow-up removes the intermediate 8 MiB synchronizations, retaining final
durability and complete readback. The last pair then writes at 169.799 and
8.155 MiB/s. Thus frequent synchronization alone does not explain the slow
member's observed write rate. This follow-up is separate from the timed engine
comparisons and does not change their samples.

Read-only SMART inventory reports overall self-assessment passed and zero
reallocated, pending or offline-uncorrectable sectors on all four drives.
Two drives retain much older ATA error-log entries; the slow drive also retains
two lifetime interface-CRC errors. Those are not presented as new test failures.
The SMART tool is extracted into the private evidence directory, without
installing a service or running a destructive/self-test workload. A passing
self-assessment does not establish that the observed slow write path is normal
or qualify the whole media surface. No engine tuning or filesystem reformat is
performed to conceal these measurements.

## Regression accounting

The default workspace run records 367 passes and one failure before its stop;
the remaining targets subsequently add 69 passes, with 75 default helper/opt-in
exclusions across those commands. The failure is the unchanged monitoring
independence test's one-second timeout around durable object I/O while other
HDD tests run. Its exact unchanged executable then passes three isolated runs.

The test now retains its deliberately blocked collector, uses a bounded
30-second functional deadline, and explicitly asserts that the collector is
still blocked when object I/O finishes. Its focused HDD run passes in 2.24
seconds including setup. This corrects a test's implicit one-second I/O SLA;
it changes no N1 runtime, barrier, configuration, selected executable or
performance acceptance threshold. The 437 distinct default cases have passing
results across these commands; the original failed log remains, rather than
being described as one clean all-target invocation. Strict release workspace/
all-target Clippy and formatting pass. The independent recorder adds four
focused journal/source-oracle checks, reported separately below.

## Independent ACK-recorder dry run

The [Rust witness](../metadata_engine/examples/power_loss_witness/README.md)
runs on a separate computer and synchronizes each request intent and complete
successful reply to that computer's evidence HDD. It verifies native TLS for
N1 and HTTPS for M1; credentials remain separate from its journal. It never
switches power or starts/reinitializes a server itself. Its new-journal guard,
exclusive lock, sequence/configuration checks, rejection of incomplete records
and deterministic source oracle pass four focused tests and strict lint.

Each clean run creates four identities: two published/read-back objects, a sparse
strict upload acknowledging parts 0 and 2, and a completed then deleted object.
N1's deletion is physical purge; M1's is metadata trash. Both clean verifiers
observe two complete objects, one absence and two retained strict parts.

Separate interruption runs then retain two complete objects and an upload with
one strict part ACK before an owned N1 or M1 process is terminated during the
next part request. The independently running recorder retains all complete
ACKs, records the failed/uncertain in-flight request and exits without replacing
its journal. The fixture supervisor stops the other owned engine; no installed
service or host power is affected. After the same private data/services return,
every earlier clean journal also verifies successfully. Each interrupted journal
first passes read-only validation, then explicit recovery fills only missing
unacknowledged parts and verifies three complete SHA-256 objects. The clean
fixtures' sparse uploads similarly recover without resending their two strict
ACKs. Original records are retained during recovery.

The first M1 recorder attempt used an unsupported diagnostic `durability=normal`
query and correctly received 400 before an allocation was acknowledged. The
recorder now maps its normal control to the existing `batched` API value. The
failed attempt and earlier recorder executable remain retained; neither M1 nor
NOA is changed to accommodate the client. The selected recorder completes the
clean M1 case and interrupted M1 recovery above.

This closes recorder preparation for its initial upload/finalize/absence window.
It does not simulate or certify physical power loss, nor establish exact hardware
cut timing during checkpoint, repair or GC. At this preparation checkpoint, the
operator still needs to confirm the actual server/drive power feed and existing-service outage window, keeping
the recorder and network powered. The existing private 3+1 fixture, original
ACK journals and all earlier populations remain available; physical interruption
is deliberately the last coordinated stage.

## Physical power removal — first window

After the other checks, the operator physically removes the server's power plug
and confirms that the server and all four SATA HDDs are off. The client laptop,
its journal HDD and the network remain powered. Both external recorders are
already running, and keep recording complete successful replies independently
of the server. This is a physical power interruption, not a reboot or process
kill. The operator reconnects power only after both recorders have drained and
their journals are retained.

Each recorder creates six identities and saves 69 complete journal records.
Its last mutation is a successful finalize for object 5; the cut interrupts the
subsequent readback. N1 eventually reports a stalled source and M1 reports an
interrupted response body. No later mutation intent or complete reply is omitted.
This window therefore tests prior durable publication, retained strict staging
and deletion across power loss. It is **not** labelled a cut during an in-flight
payload write or at an exact publication instruction.

The host returns with a different boot ID and the same four XFS mounts, pool
markers and executable hashes. Kernel logs show starting/ending XFS log recovery
on all four filesystems. Each HDD's hardware power-cycle counter increases by
exactly one; reallocated/pending/offline-uncorrectable sector and interface-CRC
counts are unchanged. Cold M1/control files are copied outside the server
before opening either test service. The original INI files are then used for
ordinary startup; the pool and database are never reinitialized.

Read-only verification of all six retained journals succeeds before explicit
resume, including the four earlier dry-run journals:

| Still-required state after physical power loss | N1 client | M1 client | Combined |
| --- | ---: | ---: | ---: |
| New completed objects, full length/SHA-256 (M1 also version) | 4 | 4 | 8 |
| New acknowledged deletion/absence | 1 physical purge | 1 metadata trash | 2 |
| New pending upload / retained strict part ACKs | 1 / 2 | 1 / 2 | 2 / 4 |
| Earlier completed objects, full readback | 6 | 6 | 12 |
| Earlier acknowledged deletion/absence | 1 | 1 | 2 |

Thus **20 completed objects, four required absences and both incomplete uploads'
four strict part ACKs survive**. The 26 identities are the retained corpus, not
a replacement population. Explicit recovery then sends only missing parts 1
and 3 for each new incomplete upload, preserving acknowledged parts 0 and 2.
Both finalize and pass complete SHA-256/version readback. The final logical
census is **22 complete objects plus four deleted/trashed identities**.

After stopping only the private test services cleanly, M1's offline check passes
with **154 verified records / 712,788 engine-reported disk bytes**. The four
members contain exactly **96 shard files plus four pool markers**, and no files
under `.uploads/`. Two native purges remove their backing identities; the two
M1 trash entries intentionally retain backing blobs until their configured TTL
and GC. No permanent per-deletion object record is added.

The boot journal also retains an unrelated preinstalled service's address-in-use
restart loop. The preceding boot's logs show the same failure before this test;
it is not a failure of either selected private executable. The unit is not
modified. Raw boot/kernel, pre/post-service, cold-control, operator-coordination,
ACK and recovery evidence remains outside the server.

This closes the recorded physical-cut window and its prior-ACK guarantees on
the selected 3+1 installation. Active-write, exact transition, larger-EC physical
power cuts and broader repetitions retain their own scope; no universal
power-loss or RC.2-ready claim follows from this one cut.

## Extended controls and deep redundancy audit

The recorder gains an optional longer reception window: 63 × 6 MiB full parts
plus a 17-byte tail, 378 MiB per object and 64 transfer parts. It still holds
only one part in memory. The default configuration encoding remains identical,
with a targeted regression check and actual re-verification of retained ACK
journals. A second physical-cut cue is withdrawn when its timing becomes stale.
The extended writers finish before the operator subsequently removes power;
that actual interruption is recorded separately below as an idle-service cut.

The extended M1 control completes eight identities. The bare native recorder
stops during its fifth identity at 900.685 seconds after creation, consistent
with the unchanged 900-second default access-token lifetime. It had requested
the SDK's default lifetime without renewal. N1's authorization path rejects an
expired upload before accepting its body; the SDK reports broken pipe while
sending. M1's existing pooled-session handling continues through the same
window. The original failure is retained as a client-lifetime issue, not an
object-integrity failure or an N1 I/O regression.

The recorder now requests an existing allowed one-hour native session; no
server token policy changes. Before recovery, it verifies all **43** saved
native strict part ACKs and all **four** saved M1 pending-part ACKs. Every
completed object and required absence also verifies. A separate read-only check
requires unfinished objects with no finalize attempt to remain unreadable.
Real native and M1 positive controls pass; deliberately stale copies presenting
already finalized objects as unfinished are correctly rejected. Original
journals are never truncated or overwritten.

Missing-part-only recovery completes both native pending objects and both M1
pending objects, without resending their strict ACKs. All 13 extended identities
are accounted for: four live native objects plus one native purge, and six live
M1 objects plus two metadata trash entries. Combined with the retained corpus,
this yields **39 identities: 32 live and seven purged/trashed**. These extra
objects were created after the first physical cut; that earlier cut's scope is
not retroactively enlarged.

The final explicit checksum/parity audit uses the existing APIs, without repair:
all **15 live native objects** pass complete configured-shard checks, and M1's
fragment diagnostic verifies **21 referenced versions**, including four retained
trash references, with zero failed or deferred checks. Thus all **36 retained
backing objects / 144 shards** pass; the other three native identities are
physically purged. M1 verifies 3,416,260,965 logical bytes in its diagnostic.
The audit is separate from throughput timing and is not presented as a cold-read
benchmark.

The extended offline database check passes with **250 verified records /
4,902,996 engine-reported disk bytes**. Member census finds exactly **144 shard
files and four pool markers**, with no `.uploads/` files. Five recorder unit
checks, strict example lint and formatting pass. Only the external test client
changes in this follow-up; both selected server executable hashes remain fixed.

## Second physical power removal — retained extended corpus

The operator subsequently confirms removing the server's power plug with all
four HDDs off. Both upload writers have already stopped; the private services
are idle. This is an additional physical interruption of the retained extended
corpus, not an active-upload cut. The operator reconnects power after the client
confirms that no recorder remains in flight.

The host returns with a new boot ID, the same four XFS filesystem and pool
identities, and unchanged server executable hashes. All four XFS filesystems
complete log recovery. Each HDD's power-cycle counter increases by one again;
reallocated, pending, offline-uncorrectable and interface-CRC counts are unchanged.
The unopened M1/control files are copied to the independently powered client
before the private services start with their original configuration.

All eight retained journals pass read-only verification: **32 live objects**
match their complete source length/SHA-256 and M1 version where applicable,
and all **seven required absences** hold. No recovery upload or repair is sent.
An isolated copy of the cold M1 database passes the offline check with **250
records / 4,902,996 disk bytes**. The cold member census matches the preceding
148-file census exactly after normalizing the two report formats. Both original
inventories are retained alongside the normalized comparison.

## Third physical power removal — strict multipart reception

After the retained corpus verifies, two new external writers each start a
378 MiB + 17-byte normal-mode control and a strict multipart upload of the same
size. The controls finalize and pass complete readback. Both strict uploads then
have durable part ACKs saved on the independently powered client before the
fresh cut cue. The operator removes the power plug and confirms the server and
all four HDDs are off while both uploads are incomplete.

The native journal retains **48 strict part ACKs** and the M1 journal **46**.
Their outstanding requests are parts 48 and 46 respectively; both ultimately
fail without a successful reply. The recorders drain normally and retain **233
and 229 complete records**, including the uncertain outcomes. No client is killed
to manufacture the disconnect. The operator reconnects power only after both
journals are saved. This is a physical cut during strict payload reception,
without claiming an exact fsync/commit instruction as the cut point.

The next boot retains the same four filesystems, pool identities and selected
binary hashes. All four XFS filesystems complete log recovery, each HDD's power
counter advances once again, and the monitored sector/interface-error counts
are unchanged. Cold M1/control files are copied outside the server before the
private services start. The isolated cold database check passes with **276
records / 5,419,092 disk bytes**.

Read-only verification of all ten retained journals succeeds before any
recovery writes:

| Required state | Result |
| --- | --- |
| Previously completed objects, including both new controls | **34**, exact length/SHA-256 and M1 version where applicable |
| Acknowledged native purges / metadata trash absences | **7**, all still absent |
| Strict native / M1 part ACKs | **48 / 46**, all **94** retained |
| Unfinished objects with no finalize attempt | Both remain unreadable |
| Missing native / M1 parts | **16 / 18**, including each 17-byte tail |

Explicit recovery sends only those 34 missing parts, preserving both original
LINK identities, the M1 session/version and every earlier ACK. Both uploads
finalize and pass full source readback. The complete retained corpus is now
**43 identities: 36 live and seven purged/trashed**. The earlier populations
are not recreated or counted as new uploads.

The final read-only deep audit passes for **17 live native objects** and all
**23 M1-referenced versions**, including four trash references: **40 backing
objects / 160 shards**, with no missing, corrupt or unavailable shards. M1's
diagnostic verifies **4,208,984,455 logical bytes**, with zero failed or deferred
versions. No repair is requested.

After cleanly stopping only the private services, M1's offline check passes
with **282 records / 5,664,852 disk bytes**. The member census contains exactly
**160 shard files plus four pool markers**, no symlinks and no `.uploads/`
files. Both server binaries stay unchanged. The fault recorders serialize
requests and synchronize their external journals; their run times are not
ordinary upload-throughput measurements.

## Evidence and remaining work

Private evidence is mapped under `/srv/test-evidence/four-drive-20261117`.
It includes physical inventory, source/executable hashes, complete failed
attempts, fault oracles, service logs, browser results and raw measurements.
All engine workloads and fault logic are Rust; browser coverage uses the existing
Node/Playwright workflows. No Python is used.

All three operator-confirmed cuts have independent evidence: readback after
publication, an idle extended corpus and strict multipart reception. These
recorded physical windows pass; exact publication/checkpoint/repair/GC power
cut points and other unexecuted cases retain their separate scope. No further
cut is scheduled by this checkpoint. Full release acceptance remains in
[the completion checklist](RC2-IMPLEMENTATION.md).

## Standalone 2+2 with two HDDs physically powered off — November 18

The frozen N1 executable served four published objects after an operator cut
power to two SATA members. The other two HDDs, system NVMe, server and external
recorder stayed on. The operator selected the pair without disclosing it.
Direct reads identified the two failed members, and the server boot ID stayed
the same. Linux recorded two SATA detachments and shut down the affected XFS
mounts. N1 continued serving reads in the original process.

| Published data | Objects | Verified bytes while two members were off |
| --- | ---: | ---: |
| Retained baseline, normal and strict | 2 | 50,331,682 |
| New controls, normal and strict | 2 | 2,130,706,466 |
| Total | 4 | 2,181,038,148 |

Before the cut, the recorder verified the original baseline journal and the
test evicted only its eight clean shard files. Linux reported zero resident
pages in all eight. During the outage, deep inspection found exactly two valid
finalized shards and two unavailable shards per object. Each inspection checked
the full reconstructed length and integrity root. A separate native download
of each object matched its original length and SHA-256.

The external journal shows both new uploads received all 128 part ACKs and a
finalize ACK before the first observed SATA error. The kernel recorded a failed
read during the outage. The next Begin returned native status 500 with an I/O
error and issued no key. This cut exercises published readback and admission
with two missing members. The earlier whole-host campaign covers interrupted
strict multipart reception.

**Four-member recovery passed.** The server stayed on when the operator
restored drive power. Linux assigned new device names to both disks. Their
filesystem UUIDs matched the pre-cut inventory. After stopping the private N1
process to release its directory handles, we unmounted the two shut-down XFS
mounts and mounted the original filesystems using their existing mount
configuration. Both completed XFS log recovery.

Before restarting the private N1 service, all four pool marker hashes matched.
The census contained the original 16 finalized shards and four markers.
The failed Begin left zero staged files. Before any new upload, independent
readback verified all 2,181,038,148 bytes against the source hashes again.
The deep audit reported four healthy shards per object with zero missing,
corrupt or unavailable shards. Recovery required no application repair.

One fresh normal upload and one fresh strict upload then finalized and passed
readback and full shard verification. After clean shutdown, each member held
six finalized shards and its pool marker, with zero staging files. This
qualifies reads through a physical two-drive outage and service recovery after
remount on this 2+2 profile.

## System candidate verification — November 24

The November 18 mixed workload had stopped after two interrupted HTTP reads.
The November 24 rerun passed on the same four-HDD reference profile, with M1
on NVMe and four independent XFS filesystems in N1 3+1. The frozen N1
executable was `f1a54f1653c1c90ae51416fa1f53474855cc20e1ea7ef52e0561af2c8c166aab`.
M1 was `f32dd61d283b549b52587c9905ad5c793de70e772a8d5067b1128b0de157b039`,
the build that passed the [two-HDD read qualification](M1-CONSOLE-20261117.md#local-m1-read-and-maintenance-qualification).

| Sixty-second offered window | Maintenance | Completed operations including drain | Errors | Full download p95, ms | Small upload p99, ms |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Off | 1,356 | 0 | 28.0 | 1,200 |
| 2 | Monitoring, GC and live backup | 1,652 | 0 | 27.5 | 1,442 |
| 3 | Monitoring, GC and live backup | 1,734 | 0 | 28.0 | 1,088 |
| 4 | Off | 1,448 | 0 | 28.2 | 1,471 |

All 6,190 operations completed. After killing and restarting both services,
the verifier checked the expected bytes, versions and absences for all 891
identities. Both live snapshots restored and passed verification. Final GC
processed 587 entries with zero failures or deferrals. Each HDD retained only
its pool marker. The earlier recording lacks the original upstream error, so
its cause remains unknown.

The same four-window workload also passed on 2+2 with the selected binaries.
All 5,722 operations completed without errors. After the process restart, all
857 identities verified. Both backups restored, and GC reclaimed 551 entries
with zero failures or deferrals and left one marker per member. Full-download
p95 was 27.9–28.3 ms and small-upload p99 was 1.11–1.67 seconds across the four
windows. Neither layout recorded a degraded health sample during the workload.

Nineteen additional real-service executions passed. Both 3+1 and 2+2 exercised
the public HTTP/native contract, TLS downloads, admission rejection before
the first byte and damage after a full or ranged transfer started. On 3+1 the
selected Console (`d3d98e5`) exercised search, backup and version restore,
upload/resume, pool migration and retrying GC after an outage. The remaining
checks covered scan cancellation, fragment quotas, archives, migration failure,
complete metadata loss and recovery, monitoring, and scheduled trash collection.

Eleven further service cases passed with a private 1+1 pool on the first two
drives of the same server. They covered optional Accel TLS, pinned reads and
fallback, archive delivery and interrupted exports, retention and quota errors,
upload size policies, multipart resume and recovery, and N1 backup, replication
and restore. An attempted fixture setup on a separate two-HDD machine returned
an I/O error before any of these cases started there.

### Mixed-load latency sign-off — November 25

The operator accepted the selected candidate's durable small-upload latency on
the four-HDD reference system and froze the N1, M1 and Console artifacts. Two
paced clients sent 4–256 KiB one-shot uploads while bulk uploads, reads, renames
and trash operations ran concurrently. Monitoring, GC and live backups ran in
two of the four windows per layout.

| N1 pool | Small uploads | Median ms | p95 ms | Pooled p99 ms | Worst window p99 ms | Maximum ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 3+1 | 826 | 446 | 924 | 1,390 | 1,471 | 1,819 |
| 2+2 | 815 | 440 | 904 | 1,276 | 1,673 | 1,815 |

All 1,641 uploads finished under 2 seconds, including requests that completed
during the drain. The 2+2 window with the highest p99 had 191 uploads. The
sign-off covers the recorded workload and reference hardware.

The same 3+1 run recorded 63 multipart finalizations for 128 MiB objects with
explicit 8 MiB parts. Finalize took 3.32 s median and 5.38 s maximum because
M1 forwarded staged data to fit the native stripe geometry. Console requests
part-size negotiation for fresh uploads. These multipart measurements remain
part of the qualification record.

### Sustained HTTP and native admission

Sixteen clients overloaded a single fragment on 3+1 for a five-minute request
window. The fragment allowed 40 reads/s and 8 uploads/s, with bursts of 16 and
4. The test used both transports and rotated tokens. It required each worker to
observe rate limiting, at least 20 successful reads/s and 4 successful writes/s
in aggregate, and no admissions beyond the configured budgets plus bursts.
The recorder counted 11,600 successful reads, 2,267 successful writes and
170,743 quota rejections. After killing both services it checked the exact
version and completed-upload counts, restarted both, and read back every
acknowledged write with its original version.

The largest individual worker p99 was 657 ms, including admitted and rejected
requests. One-second samples recorded peak RSS of 101,136 KiB for M1 and
93,712 KiB for N1, with zero sampled process swap. Package staging also used
the metadata NVMe near the end of the window. These measurements characterize
the configured quota workload. The separate mixed-workload latency result was
accepted on November 25.

To repeat the five-minute run, set `M1_OVERLOAD_SECONDS=300` for
`admission::overload::sustained_http_native_overload_preserves_acks_and_shared_quotas`.
The default remains ten seconds. The test pages through a single recovered
database snapshot to check every version and completed handle.

### Package and physical-test preparation

The private Linux x86-64 package rehearsal used the two selected server
binaries and the matching-version N1 backup helper. After extraction, all
three executable hashes and all 5,741 Console file hashes matched. Both
installer units rendered and passed systemd verification. The Console launcher
also passed upload, restart and readback with and without optional Accel on the
reference server. The package SHA-256 was
`8683dab5365fd88f400d53cee66a0d5d547b5571453466a175f0835919bedfc1`.

The external recorder synchronized 45 journal records for each of two clients,
one using standalone N1 and one using M1. Before the private 3+1 fixture was
shut down cleanly, each client verified two published payloads, one required
absence, and two acknowledged parts in an incomplete strict upload. The
independently stored journals and the server data are available for the next
physical test.

### Publication recovery rehearsal

Two independent clients finalized on the private 3+1 pool, one through
standalone N1 and one through M1. In separate normal and strict rehearsals,
the recorder confirmed that all four parts per client had received an ACK.
A syscall tracer held the actual N1 executable after each of eight shard renames
and before its publication workers could return and join the directory syncs.
Both clients still had outstanding finalize requests when the private
processes were killed. Neither finalize had returned success.

After each restart the recorder verified the original four published payloads,
two deletions and four pending strict parts before sending a recovery request.
In the strict rehearsal it also verified the eight new strict ACKs and the
complete bytes of the two interrupted finalizations, which were readable
after service startup. M1's startup reconciliation processes pending
publication intents. The subsequent explicit finalize retries preserved both
identities and completed without resending parts. A deep integrity audit and
the offline M1 database check passed.

The selected binaries also passed the six in-flight process-crash cases on
each layout, including loss of M1, N1 and both with an incomplete request.
Bridged finalize recovery, GC with an active writer and GC after an outage
passed on 3+1 and 2+2. The sealed-staging repair test initially submitted an
8 KiB native part to a 3+1 fixture whose stripe was 12 KiB. The test now takes
its aligned part size from the configured pool. It passes on 3+1, 2+2 and
1+1 using the same server binaries. Separate unit-test process barriers for
M1 journal/checkpoint recovery, N1 durable publication and interrupted purge
also passed. These are process interruptions.

### Physical publication recovery on 3+1

The operator removed power from the host and all four HDDs twice. The selected
N1 and M1 hashes remained identical to the package rehearsal. Both boots
returned the four original XFS filesystems, which completed journal recovery.
The external recorder and its ACK journals stayed powered throughout.

| Power cut | Recorded point | Readback and recovery |
| --- | --- | --- |
| First | The pause expired and both finalized objects were acknowledged before power went off. | Ten published objects, two required absences and four pending strict parts verified after restart. The offline M1 check verified 96 records. |
| Second | The tracer held eight shard rename threads before the directory sync. The host became unreachable about 22 seconds after the first observed rename. The external journals retained four strict part ACKs per upload and zero finalize ACKs. | All ten previously published objects, both absences and all twelve pending strict part ACKs verified before client recovery, including the eight ACKs from this cut. Both interrupted uploads were unreadable at that point. |

Each interrupted finalization completed on explicit retry using the original
identity without resending any acknowledged part. Both newly published objects
passed full SHA-256 readback and deep integrity checks. The prior published
objects also passed their audits. The final offline M1 check verified 122
records. Each member retained 13 finalized shards, two incomplete staging
shards and its pool marker. The extra finalized identity is retained M1 trash.
No extra files or symlinks appeared.

This test covers acknowledged publication and interruption at N1's shard
publication barrier, through both the standalone and named paths. M1 database
commit/checkpoint, shard repair and GC power-interruption windows remain open.
