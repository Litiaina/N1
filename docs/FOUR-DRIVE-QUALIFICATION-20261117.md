# N1 RC.2 and M1 RC.1 four-drive qualification — November 17, 2026

**Four-drive recovery/operator checks and the first coordinated physical
power-loss window passed. No release or tag.** This continues the current N1 checkpoint
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
on all four filesystems. Cold M1/control files are copied outside the server
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

## Evidence and remaining work

Private evidence is mapped under `/srv/test-evidence/four-drive-20261117`.
It includes physical inventory, source/executable hashes, complete failed
attempts, fault oracles, service logs, browser results and raw measurements.
All engine workloads and fault logic are Rust; browser coverage uses the existing
Node/Playwright workflows. No Python is used.

The first operator-confirmed power cut now has independent ACK/recovery evidence.
Process/member-fault checks and this physical window retain their actual fault
classifications; broader power/transition acceptance is not inferred. Full release acceptance remains in
[the completion checklist](RC2-IMPLEMENTATION.md).
