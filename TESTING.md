# N1 testing and results

N1 4.0.0 / storage format 4 remains development, not LTS. Results below are
dated evidence, not guarantees for every server. Older versions' passes do not
certify the current namespace/pool design. Personal test paths and credentials
are intentionally omitted.

## Console Office upload integration — October 13, 2026

All 300 engine library tests pass. Real USB/XFS 1+0 verification confirms that
standard and durable `fresh=true` initiation allocate independent sessions for
identical sizing, while default resume discovery remains unchanged.

The real browser Office adapter created a native N1 DOCX version and reconciled
a lost finalize acknowledgement to that exact version without another upload.
The original bytes remained readable; 42 previously committed NDX/payload files
retained their SHA-256 and modification times. Accel served a pinned-version HIT.
No storage format, mutable version head, business database or NCX mutation
semantics were added. The Console's native spreadsheet input failure is recorded
in its testing document; it is not a failure of N1's committed-byte verification.
This localhost 1+0 run does not test redundancy or physical power loss.

## Optional Accel read integration — October 13, 2026

All 299 engine unit tests pass, including private read-proxy authority, range
forwarding, response validation and pre-header fallback. The release engine and
Accel also passed an isolated USB/XFS 1+0 proof using an 84 MiB AVI and PNG:
full/range/image hashes matched, cached rename reads fetched no additional
payload, the old key returned 404, and committed object NDX/payload bytes and
modification times stayed unchanged. N1 still read the correct USB-backed range
after Accel stopped. Original drive contents were not formatted or removed.

This adds optional HTTP delivery only, not an object cache inside N1 or storage
format changes. HTTP accepted sockets enable TCP_NODELAY. Cache activation,
budgets and bounded measurements are documented in the Accel repository's
TESTING.md and [integration guide](DOCUMENTATION.md#accel-integration). The proof
used loopback networking and does not qualify redundant EC recovery or an LTS
release. Earlier candidate hashes below identify their historical builds, not
this newer binary.

## Production-hardening candidate — October 12, 2026

Engine SHA-256:
fb4f46fe47adf90c0f62b4be8f0e43cc5dd4574be9249ea5e0a29e254b8456d8.
Shell SHA-256:
2b6a6f84a98338775642aede5338323be5531a62bee6e747ca03831bf6cb52d7.

Local candidate validation: 297 engine, 2 EC integration, 9 proof-runner,
12 Shell and 8 CLI tests passed (328 total) in warning-denied parallel execution.
Before the extra proof assertion, two 327-test parallel runs also passed.
Locked release Engine/Shell/helper/proof builds, warning-denied
all-target checking, formatting and whitespace checks passed.

New checks cover strict directory-sync error propagation, standard part retry/
finalization durability, independent HTTP/native mutation permissions,
operator-only/global/cross-fragment maintenance, rotation-bound refresh,
oversized JSON/part-count/slot admission, native raw-body rejection before
allocation, WebSocket read authority and expiry, inode warnings, TLS defaults,
and corrupt backup/replica rejection without replacing the last valid snapshot.
Console pool regressions passed 10 cases across five viewports; the full desktop
suite passed 51 cases with 5 unavailable/inapplicable cases skipped.

Failures retained: an HTTP POSIX permission test initially exposed a second
handler missing its checks; both HTTP and native paths now enforce the same
permissions. A body-limit fixture initially used a nonexistent fragment and
hit the existing upload existence gate before JSON extraction; a real isolated
fragment now exercises the limit without weakening the 413 assertion.
Parallel positive backup captures exposed transient fork-inherited CLOEXEC
leases again. Their fixture-only bounded acquisition helper now covers positive
captures while active-engine/overlap rejection tests remain immediate and
production locking is unchanged. Both final parallel reruns passed.

The main-server matrix runs headlessly on isolated roots on four XFS HDDs.
Accepted passes so far: NCX-only copy/delete, root relocation/reordering,
3+1 data/parity member replacement, 2+1 replacement, 1+0 migration, encrypted
backup/replication/interrupted restore in 3+1 and 1+0 layouts, pool expansion,
and 2 GiB durable multipart restart/resume. These do not certify a deployment.

Direct workstation-to-server LAN HTTPS passed restricted mutation/global-job
denials, huge-part-count refusal, 128-key pagination, repeated NCX mutation and
all final payload hashes. The three live Console tests passed against that
server: prefix rename preserving a same-named object, TAR download, multipart
resume and standard batch routing. The initial LAN latency sample ran during
simultaneous disk-intensive restore; it is a contention result, not a quiet
baseline. Quiet performance qualification is still pending.

Three initial hardware runner failures retained: the healing case expected
reconfiguration of enrolled EC geometry; native and NOA cases expected empty
mkdir. N1 correctly rejected those pre-v4 assumptions. The runner now asserts
enrolled-geometry rejection and uses upload-backed virtual prefixes. Its typed
native-error assertions require both the exact status and `ok=false`.
The corrected healing/native reruns and the final NOA rerun subsequently passed.
The last NOA failure was another pre-v4 parent-folder STAT assumption; the test
now verifies the actual trashed child binding before recovery. All 14 selected
hardware cases have accepted passes on the same candidate Engine/Shell binaries;
the initial failures above are retained. Five-second fixture-process sampling
observed approximately 174.3 MiB peak RSS and 88 peak file descriptors across
the matrix (including offline helpers); this is not a long-soak guarantee.

### Physical power-cut recovery — October 12, 2026

PASS after retry on the same candidate binaries. The operator physically cut
power during repeated 128-key prefix moves; the independent LAN client recorded
an interrupted request. After power restoration, all four XFS members replayed
their logs and the isolated N1 instance started automatically.

| Check | Result |
| --- | --- |
| Immediate committed-object readback | All 128 moved objects and the single-object baseline were reachable with matching SHA-256 hashes |
| Strict standard multipart | Finalized 16 MiB object retained its expected hash |
| Durable multipart | Both previously acknowledged 4 MiB parts survived; remaining parts uploaded and final 16 MiB hash matched |
| Interrupted namespace | 16 objects had both names; complete, ordered pagination returned 144 bindings for 128 objects |
| First offline integrity check | REFUSED: 13 incomplete NCX replica pairs; no committed NDX replica was missing. Automatic healing was disabled in this fixture |
| Retry of interrupted move | PASS: 54 remaining source bindings moved without conflicts; all 128 destination objects hash-correct, old names absent |
| Mutation-only immutable inventory | All 11,088 files under object storage had identical inventories/SHA-256 hashes before and after retry; this baseline was taken after the power cut |
| Final complete offline pool verification | PASS, exit 0: 1,381 versions and 2,700 physical NCX replicas checked, recorded payload-piece checksums verified, zero reconstructed shards |

The initial replica failure is retained, not relabeled as an immediate healthy
pool. Copy/delete can leave duplicate names or incomplete replica pairs when
interrupted; ordinary retry completed this operation without engine changes,
tombstones or a journal. Test-only boot automation was removed afterward;
existing production services and storage were not replaced.

This is one physical power-cut recovery drill on a four-HDD 3+1 deployment,
not every fsync stage, storage controller or failure topology. Broader power-cut
coverage and a longer soak of this exact candidate remain release gates.

## Earlier pool health, Shell and Console integration — October 12, 2026

Engine SHA-256:
a183c39818399637e16a2aec704e486a0620627db7d6f7b2e36343259be926ae.
Shell SHA-256:
8608c21e150bcf76f070353511534db422b4f9b910c9bb8007ce2ddd4a15e90f.

| Gate | Result |
| --- | --- |
| Workspace/all-target tests | PASS: 276 engine, 2 EC integration, 8 proof-runner, 11 Shell, 8 CLI; 305 total, including a repeated parallel run |
| Release build, all-target check, formatting and whitespace | PASS |
| Console integration/responsive regression subset | PASS: 84 cases; 121 viewport-inapplicable cases skipped, not counted as passes |
| Complete desktop Console suite | PASS on final rerun: 51 cases, 5 unavailable/inapplicable cases skipped |
| New pool UI checks | PASS across five desktop/tablet/phone viewports; explicit selection, deep scan, member/fragment identity, failure clearing and offline workflows |
| Mixed-pool real-device expansion | PASS on isolated MCH fixtures: A 1+0 plus B 2+1; existing fragment credentials enrolled, old committed bytes retained, new allocation and restart verified |
| Existing key after expansion | PASS: newer version placed in B, NCX remained in A, history spanned pools; A's namespace check passed with its old object directory temporarily unavailable and the newer version in B |
| HTTPS/native pool diagnostics | PASS: independent pool selection, correct replica counts and current allocation pool; no authentication, no read permission and cross-fragment content requests refused |
| Failure isolation | PASS: fenced B member reported degraded/unavailable; A still verified, and the diagnostic did not clear the fence |
| Content integrity vs readiness | PASS: same-sized parity corruption left readiness ready but failed CRC verification; no automatic repair or committed-byte rewrite by the diagnostic |
| Offline Shell selection | PASS: complete A verification while a B member was fenced; full verification passed afterward (3 versions, 3 physical NCX replicas) |
| Encrypted mixed-pool backup | PASS: offline Shell capture and authenticated snapshot verification; 107 records, approximately 4 MiB source bytes |
| Backup cross-pool reference regression | PASS: NCX in A with remaining metadata/payload only in B verified without global engine initialization; corrupt pieces and dangling references refused |

Only new private fixtures on MCH's four mounted XFS HDDs were used. Existing
production services stayed active; production data/configuration/binaries were
not changed. No formatting, remounting or production deployment occurred.
Server checks used loopback HTTPS/native ports, bounded CPU/memory and idle I/O
priority; they do not establish LAN latency or full-drive throughput.

Failures retained: the first native run exposed a u128 elapsed-time field's
binary MessagePack encoding; u64 and a serialization regression fixed it before
the complete rerun passed. Inspection also found a pool-local dangling-reference
assumption; the verifier now recognizes stable-object version history across
known pools, including the offline backup catalog. Two parallel backup unit
tests encountered transient flock inheritance during other tests' fork/exec
windows. Their positive fixture-reopen helper now waits boundedly; production
locks and immediate overlapping-run refusal assertions are unchanged, and the
parallel rerun passed. The extra backup script initially read `id` instead of
the capture result's `snapshot`; capture succeeded and the corrected verification
passed. Initial development checks also caught Shell query tuple types and an
undeclared proof-only walker; both were corrected before hardware validation.
The broader desktop Console run initially hit an async evaluation-context error
in its NOA-shell test and a window-animation timing assertion. The shell test
now submits through the real command input and asserts output/path state;
the geometry test polls the same size bounds instead of sleeping a fixed delay.
No production shell/window behavior was changed; the complete desktop rerun
passed without weakening command-routing or geometry assertions.

Quick identity/access readiness is not full content integrity. Online scans are
read-only live observations, not atomic snapshots; concurrent mutations can
invalidate them. Complete offline verification remains the maintenance gate.
Console presents diagnostics and host-side Shell workflows, not browser-executed
physical migration/backup automation. This is not a new long-soak or power-cut
qualification.

## Self-identifying roots and location independence — October 12, 2026

Final workspace-build engine SHA-256:
2313745a4feac9db93992494ccafac10707475255ada3f8027b17b16e536441b.
Matching Shell SHA-256:
ce520565709bd605000c494a24060cfeecea0c043df3444e0e89b734418f3054.

MCH was used only through new private fixtures on its four existing XFS HDDs
and NVMe filesystem. No production contents/configuration/binaries were changed,
no existing service was stopped, and no device was formatted or remounted.
Tests used loopback HTTPS/native ports and temporary self-signed certificates,
with a two-core CPU quota, 1.5 GiB cgroup limit and idle I/O priority. Physical
I/O remained shared with production. These are not client-LAN latency results.

| Gate | Result |
| --- | --- |
| Workspace/all-target regression tests | PASS: 271 engine, 2 EC integration, 8 proof-runner, 11 Shell, 7 CLI; 299 total |
| Warnings-denied check, formatting and whitespace | PASS |
| Existing path-bound v4 data | PASS: seeded with the previous engine, descriptor upgrade preserved committed bytes |
| 3+1 data and parity members | PASS: verified HDD-to-NVMe cloning, different root locations, reversed discovery order, original paths unavailable |
| Cold degraded restart after relocation | PASS: readback/listing remained available within parity limits; missing directories were not recreated |
| Wrong identity and unfinished copy | Rejected; marker recognition never bypassed the activation fence |
| Replacement at a new location | PASS for data/parity: SIGKILL after actual piece publication, startup refusal, explicit resume and full verification |
| Relocated operation | PASS: recursive paginated listing, versions, trash recovery, durable upload continuation, batch/native uploads, new write/rename |
| Recover-on-read and autonomous healing | PASS on relocated 3+1 storage: removed data piece recovered on read; removed parity piece healed by the background controller |
| Healthy 1+0 relocation | PASS: verified cross-device clone and new-location operation; lost-member reconstruction correctly refused |
| Committed immutability | Complete NDX/data/parity inventories and SHA-256 hashes unchanged through upgrade, relocation and reconstruction |
| Encrypted backup/portable restore | PASS: capture, incremental capture, replication, original paths hidden, restore interruption after 3,920 files, fenced refusal/resume, hashes, credentials, versions, trash, durable continuation, ranges and retention |
| Backup after legacy-data relocation | PASS: full capture and authenticated verification of the relocated mixed legacy/logical-address corpus |

The final five-mode matrix completed in 5m 04s, exit 0. Its 329.3 MiB peak
is whole-cgroup accounting, not engine RSS. Small-fixture data/parity rebuilds
observed 4.18/4.03 s and 19.8/21.5 MiB peak backend RSS, ending with six FDs.
Clones observed approximately 0.26 s, 8.5–8.8 MiB RSS and four final FDs;
these are not full-drive bandwidth claims. Backup phases observed 25.29 s
capture, 4.25 s incremental capture, 45.85 s replication and 83.04 s interrupted
restore/refusal/resume.

Matched four-HDD upload comparison against the pre-portability build used five
samples per transport/multipart case: 1 MiB one-shot, 8 MiB multipart with 2 MiB
parts, and four concurrent 64-object batches of 16 KiB objects. Median upload
rates are shown below; batch rate covers the entire four-client request group.

| Measure | Before | Self-identifying roots |
| --- | ---: | ---: |
| HTTPS one-shot | 11.08 MiB/s | 11.15 MiB/s |
| HTTPS sequential multipart | 32.09 MiB/s | 40.69 MiB/s |
| Native one-shot | 10.71 MiB/s | 17.18 MiB/s |
| Native sequential multipart | 7.04 MiB/s | 7.37 MiB/s |
| Native pipelined multipart | 7.14 MiB/s | 8.36 MiB/s |
| Concurrent batch | 28.90 objects/s | 29.75 objects/s |
| Maximum sampled engine RSS | 80.4 MiB | 81.5 MiB |

No consistent throughput regression was observed. Shared-I/O variance is large:
the candidate's native sequential mean was 3.9% lower despite its higher median.
Neither the speedups nor the roughly 1.1 MiB sampled RSS difference establish
universal throughput or memory behavior. No object cache was added.

Failed attempts remain evidence: the first relocation invocation exposed an
obsolete proof-argument dependency; the first new-location replacement safely
refused an old path-keyed authorization lookup. That lookup was removed and the
descriptor was included in the replacement activation inventory before the final
matrix. A reused fixture directory caused another refusal; unique replacement
names fixed the harness without overwriting prior evidence. A diagnostic started
before its binary transfer finished returned ETXTBSY; it performed no repair.
An extra backup-verification script initially used an unsupported `--all` flag;
capture completed, and verification passed when given its explicit snapshot ID.
Physical power cuts, hot relocation and multiple simultaneous member replacement
are not certified by these tests. Production deployment was not performed.

## Pool portability and integrated backups — October 12, 2026

Final engine SHA-256:
c96bcf269cedcc0a5a1b8ec4c083c7660b5d86fed4815a626993d78cdbb7089e.
Matching Shell SHA-256:
ce520565709bd605000c494a24060cfeecea0c043df3444e0e89b734418f3054.

The main server was used as disposable test hardware: four independent XFS
HDDs, with an independent NVMe filesystem also available as a replacement
target. Tests used isolated roots and server-local HTTPS without Accel;
the harness accepts its temporary self-signed certificates. Physical I/O was
shared between test jobs; production services were restarted before the extra
cross-device restore drill. Existing production binaries were not replaced.

| Gate | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 263 engine, 2 EC integration, 7 proof-runner, 11 shell, 7 CLI; 290 total |
| Warnings-denied all-target check, formatting and whitespace | PASS |
| 3+1 data-member and parity-member replacement | PASS: blank replacement on a different filesystem, SIGKILL after actual piece publication, startup rejection, explicit resume and verified activation |
| 2+1 replacement onto another HDD | PASS: interrupted reconstruction/resume and full integrity checks |
| Healthy 1+0 cloning onto another HDD | PASS: interrupted copy/resume, same-path cutover, complete readback and new operations |
| Lost 1+0 reconstruction | Correctly refused; requires a verified backup, not an invented healthy member |
| Copy and replica validation | Partial copies, same-size payload corruption, divergent durable manifests and missing receipt replicas rejected |
| All four pool cases | PASS: 133 final visible objects each, recursive 17-entry pagination, version history, empty object, trash recovery, durable 64 MiB upload continuation and new upload/rename |
| Immutability | Complete committed NDX/data/parity inventories and SHA-256 hashes preserved through clone/reconstruction; healthy source copying did not recode pieces |
| Portable encrypted restore, 3+1 and 1+0 | PASS: original source paths hidden, 3+1 target member paths deliberately reversed, restore SIGKILL after piece publication, fenced startup refusal and explicit resume; all baseline hashes, versions, separate fragment credentials, trash, durable uploads, range reads, new writes and retained snapshots verified |
| 1+0 migration onto another HDD through backup | PASS: replica and restored member on an independent device, original paths unavailable, restore SIGKILL/retry and all end-to-end checks; 5m 24s overall, exit 0 |
| Integrated LAN SSH replication | PASS: sender SIGKILL after five destination blocks; no completed snapshot published; retry, authenticated verification and repeat transfer passed |
| Integrated scheduled backup | PASS: real systemd stop/capture/restart, verification, replication and source/destination retention through Shell |
| Whole backup-job interruption | PASS: whole-cgroup SIGKILL after 24 capture pins; supervised recovery restarted the test engine, cleared restart intent and recorded interruption rather than success; subsequent full pool verification passed |

Small-fixture offline measurements below include explicit resume after a
deliberate interruption. They are not full-drive throughput or online mutation
benchmarks; device choice and concurrent I/O explain why they cannot be compared
as an architecture speedup.

| Operation / target | Completion time | Peak backend RSS |
| --- | ---: | ---: |
| 3+1 data-member clone / NVMe | 0.92 s | 12.0 MiB |
| 3+1 data-member rebuild / NVMe | 2.56 s | 22.7 MiB |
| 3+1 parity-member rebuild / NVMe | 2.48 s | 24.5 MiB |
| 2+1 data-member rebuild / HDD | 35.47 s | 23.6 MiB |
| 1+0 clone / HDD, concurrent backup traffic | 108.01 s | 11.7 MiB |

Offline verify/clone processes ended with four open FDs; rebuild processes
ended with six. An earlier isolated 1+0 clone observed 10.31 s; it does not
erase the slower shared-I/O result. Process RSS is distinct from systemd's
whole-cgroup memory peak.

The 3+1 snapshot observed 54.46 s capture, 14.22 s incremental capture,
52.97 s initial replication and 243.75 s interrupted restore/rejection/resume.
The 1+0 snapshot observed 40.41/11.36/85.72/65.31 s respectively. These are
small-fixture wall times, not bulk-media bandwidth or instantaneous-backup
claims. The six-mode final hardware matrix completed in 16m 51s, exit 0;
its 1011.3 MiB peak is the whole cgroup including successive test processes
and kernel accounting, not engine RSS.
The separate cross-device 1+0 drill observed 43.35 s capture, 9.48 s incremental
capture, 121.67 s initial replication and 20.78 s interrupted restore/resume.
Its 233.1 MiB memory peak is likewise whole-cgroup accounting.

Failures were retained, not counted as passes: the first hardware harness
incorrectly compared non-recursive listing with nested keys; another omitted
Content-Length on an empty upload. Corrected reruns preserved those checks.
The first real offline reconstruction exposed uninitialized repair-reader
topology; it stayed fenced and was fixed before the complete matrix above.
The scheduled-backup fixture initially exposed a stale pre-cutover member
instead of the activated replacement. Capture refused it and restarted N1;
binding the verified replacement restored full pool integrity before retry.
An SSH fixture with non-private repository permissions was also refused before
the corrected owner-only run. The extra cross-device backup run initially
stopped at argument parsing because the spare flag still required the pool-only
mode; a parser regression now covers independent-spare backup selection.
Local regressions cover integrated job child
arguments, logical slot mapping and missing/divergent upload replicas.

These are process interruption and real cross-filesystem cutover tests, not
physical power cuts, physically unplugged members, a long memory soak or
multi-node automatic failover. Replacement currently handles one missing
member with all other peers verified; continuous writable standby replication
is not implemented.

## Headless release-gate soak — October 8, 2026

**FAIL after 6h 1m 41s; six-hour continuous-PID resource gate PASS.** The isolated
instance ran from 19:03 Manila on October 8 to 01:05 on October 9, stopping
before its eight-hour limit. Existing N1 and user storage were not replaced. Test
directories are isolated on four independent XFS members, with 3+1 EC and
loopback-only verified HTTPS; physical I/O is shared.

Engine SHA-256:
7fedadeb07086ba965c8ceeb68099a16d9f54fbf2674e29c00dd20af542851f1,
matching the deployed production engine.

The uninterrupted phase recorded 4,322 resource samples: 83.2 MiB peak engine
RSS, 36 peak/15 final quiescent FDs, +2.38 MiB warmed-to-final median RSS and
zero median FD growth. It completed 101 concurrent batch epochs, 303 mixed
four-fragment traffic/deletion epochs, 37 autonomous-healing perturbations,
37 disconnect probes and 18 complete baseline hash checks.

Recovery checks passed for listing, NCX fallback, recover-on-read, autonomous
repair, unrecoverable-object fencing/retry and root-identity rejection. After
the directory-outage masks, fragment creation returned 503 because an automatic
availability scan had failed and its live safety gate was still read-only.
The harness had accepted an older topology-status snapshot without proving a
fresh scan and successful write. The run stopped before repeated process-crash
cycles and final integrity validation; it is not an eight-hour pass.

RSS, file descriptors, CPU and I/O were sampled every five seconds. Planned
fault-phase restarts are outside the continuous-PID resource phase.

The earlier October 7 main-server run passed six uninterrupted hours: 4,322
resource samples, 81.7 MiB peak engine RSS, 35 peak/15 final quiescent FDs,
2.26 MiB warmed-to-final median RSS growth and zero median FD growth. Its
subsequent recovery phase stopped because the harness wrongly required HTTP
200 from health below the namespace quorum; N1 correctly returned 503. The
overall run is not a pass. The October 8 harness corrected that expectation
and checked below-K rejection before its continuous phase; engine code was
unchanged between those two long runs.

The MCH attempt started at 09:26 on October 8 with a 17:26 hard limit. Its final
outcome is unverified because that host is inaccessible from the current home
network; no pass or confirmed stop is claimed.

The supervisor saves failure/timeout evidence and stops the isolated test
automatically; OOM, early exit and timeout are not passes. Physical power cuts,
actual member replacement, capacity exhaustion and the other remaining gates
below are not established by these tests. No LTS declaration follows from them.

### Safety-gate recovery patch — October 9, 2026

The patch reports the live safety reason in healer health and masks write-safe
status while that gate is closed. Automatic retry scheduling now checks the
existing persistent scan fault, not a display phase that topology updates can
replace. Restoring roots cannot clear unresolved repair debt. No namespace,
NDX/payload format, mutation ordering or durability policy changed.

The isolated harness now requires a successful post-restoration scan, fresh
topology check, open maintenance gate and real fragment write/delete after each
outage mask. A separate regression uses a ten-minute periodic scan interval to
require prompt fault-driven recovery without restarting the engine.

Candidate engine SHA-256:
041185921f743a8c4e996d2d1b22a2e67aba38d3ac8c87af01af9a7f63d4303f.

| Validation | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 252 engine, 2 EC integration, 6 proof-runner, 10 shell, 7 CLI; 277 total |
| All-target check, warnings-denied engine library compilation, formatting/whitespace | PASS |
| Lost-retry reproduction on previous engine | FAIL as expected: after topology restoration, the safety gate stayed closed through the 30-second recovery deadline |
| Same long-interval fault on candidate | PASS: automatic recovery, real write/delete and complete baseline verification within 11.3 s, without restarting during recovery |
| Four-member 3+1 recovery regression | PASS: two complete fault/crash cycles in 11m 57s; supervisor exit 0 |
| All single-/two-member directory-outage masks | PASS: 20 executions, each requiring a fresh scan and actual post-restoration write |
| Interrupted rename/move/trash/recover | PASS: eight process kills, 128 objects per case, early/mid-prefix cuts, hash-correct reachability/retry and unchanged immutable files |
| Listing, NCX fallback/repair, recover-on-read, autonomous data/parity repair, unrecoverable-object fencing/retry and root identity | PASS in both cycles |
| Final integrity | PASS: 129 version-pinned baseline downloads plus complete baseline immutable inventory/size/hash comparison |

This was isolated on the main server's four independent XFS members using
verified loopback HTTPS. Production retained its original PID and engine hash;
no deployment occurred. Raw reports remain excluded from version control.
The candidate has not completed a new eight-hour soak or physical power-cut test.

## Upload/deletion hardening — October 6, 2026

Tested engine SHA-256:
7fedadeb07086ba965c8ceeb68099a16d9f54fbf2674e29c00dd20af542851f1.
Initial validation used new isolated four-member storage. The subsequent
live rollout is recorded below.

| Gate | Result |
| --- | --- |
| Serial workspace/all-target tests | PASS: 248 engine, 2 native, 6 proof-runner, 10 shell, 7 CLI; 273 total |
| Upload tests selected independently | PASS: all 12; the previous isolated run failed because storage settings were uninitialized |
| All-target check, warnings-denied engine compilation, formatting/whitespace | PASS |
| Four-member 3+1 verification, 11m 45s | PASS: 25 checks covering deletion errors/retry, stale HTTP/native uploads, upload/delete/disconnect overlap, restart readback, recover-on-read, autonomous shard/NCX healing and fail-safe retry |
| Interrupted rename/move/trash/recover | PASS: 128 objects per operation remained reachable and hash-correct; retry completed; ordinary mutations left immutable files unchanged |
| Batch contracts | PASS: mixed results, empty/oversize fields, 32 same-key versions/max ULID, disconnect, explicit strict readback and 4,096/4,097-field boundary |
| Real LAN uploads | PASS: 24 trials, two repeats, 16 KiB/1 MiB objects, one-shot/batch and up to four clients; all 1,792 uploads hash-verified |

LAN measurements used direct certificate-verified HTTPS with keep-alive, no
SSH tunnel or Accel. Readback was outside the upload timer. Engine peak RSS
was **60.2 MiB**, versus **267.1 MiB** in the earlier equivalent LAN workload.
Peak/end file descriptors were 55/15. The complete fault-test cgroup peaked at
1.31 GiB, accounting for the harness and kernel memory as well as the engine;
that is not engine RSS or an uninterrupted-process memory-soak result.

| 1 MiB batch workload | Earlier throughput | Candidate throughput |
| --- | ---: | ---: |
| One client, concurrency 8, 32 objects | 6.45–15.15 MiB/s | 6.08–19.48 MiB/s |
| One client, concurrency 32, 32 objects | 6.13–7.64 MiB/s | 6.02–11.60 MiB/s |
| One client, concurrency 128, 128 objects | 5.09–5.25 MiB/s | 5.61–6.32 MiB/s |
| Four clients, concurrency 32 each, 128 total | 4.41–5.96 MiB/s | 5.45–10.30 MiB/s |

The 32-object/concurrency-8 batch took 1.64–5.26 s for the complete request,
not per object. Results are mixed: the 16 KiB/concurrency-128 case decreased
from 0.228–0.360 to 0.086–0.200 MiB/s, and sequential 1 MiB one-shot decreased
from 1.97–3.50 to 1.33–2.71 MiB/s. Shared HDD/OS state and non-interleaved runs
limit causal comparison; parallel publication and lower buffering do not
establish a universal speedup. A prior instrumented trace attributed about
97% of its upload interval to sync calls; it was not a LAN baseline.

These are bounded process-crash and readback tests, not physical power cuts or
a new long soak. During those tests, the live engine, Console, Accel and user
data were unchanged.

### Main-server rollout — October 6, 2026, 22:47 Manila

The tested engine above and its rebuilt matching backup helper are now live.
Previous binaries were retained for rollback. Startup was write-safe on all
four independent 3+1 members, with no warning-or-higher startup journal entries.
Before/after and post-smoke checks preserved 269 live objects, their namespace
snapshots, 3,708 immutable file paths/stats, 1,192 NDX hashes and three
version-pinned readback samples. Configuration, credentials and TLS hashes
were unchanged; existing backups were not modified.

The nine-object live smoke passed one-shot/eight-object batch upload, single
and eight-key prefix rename/move/trash/recover, exact-version readback and
immutable-file hashes. Its fragment was removed; a stale upload returned 404
without recreating it. Console returned verified HTTPS 200, and its nginx
process and Accel process were unchanged. This is deployment verification,
not a new LAN benchmark, full payload inventory hash or backup/restore drill.

## Deployed copy/delete candidate — October 6, 2026

Tested engine SHA-256:
4ff20f6bba63b1c6837d44835f4b81813895191830cc2e3d864201a7454720f0.
The original candidate tests did not deploy this build; production remained on
the October 5 build during those tests. The subsequent deployment is recorded
below. Console and Accel were not replaced.

| Gate | Result |
| --- | --- |
| Warnings-denied serial workspace/all-target tests | PASS: 241 engine, 2 native, 6 proof-runner, 10 shell, 7 CLI |
| Warnings-denied check, formatting and diff whitespace | PASS |
| Four physical rotational XFS members, 3+1, 1,000 objects | PASS: 32 same-key versions, 128 concurrent batch/native objects, coexistence, conflicts, NCX repair, cold degraded restart and mutation refusal |
| Live LIST, 73-entry pages | 3.63–5.04 ms, warm server-local observations |
| Ordinary mutation immutability | NDX/payload paths and hashes unchanged; deliberately corrupt NDX did not block NCX mutation |
| Process interruption during 128-key rename | Every object retained and hash-verified; retry completed deletion |

### Main-server deployment — October 6, 2026

The engine above and its matching backup helper were deployed to the main
server. Previous binaries were retained for rollback; configuration,
credentials, certificates, user objects and backups were preserved.

| Deployment check | Result |
| --- | --- |
| Locked release build; namespace regressions | PASS: engine matches tested SHA-256; 41 namespace tests |
| Before/after live namespace and readback | PASS: 32 objects, complete NCX replicas, identical snapshots and three version-pinned download samples |
| Existing immutable files | PASS: 1,204 file paths/sizes/mtimes/inodes unchanged; all 128 NDX SHA-256 hashes unchanged |
| Single-key and eight-key prefix rename/move/trash/recover | PASS: nine isolated test objects; version-pinned bytes and NDX/payload paths, sizes, mtimes and SHA-256 unchanged during mutations |
| Readiness and logs | PASS: four independent members writable, 3+1, write-safe; no warning-or-higher journal entries after startup |
| Console and Accel | Existing processes unchanged; Console verified HTTPS login page returned 200 |

Single-key rename/move/trash/recover took 228/121/100/75 ms. Eight-key prefix
operations took 228/1,170/176/305 ms. These are one observation per operation
over server-local, certificate-verified HTTPS with new connections, not a LAN
benchmark. Sampled engine RSS/HWM was 78,112 KiB after the smoke test; no
per-mutation memory or continuous-soak claim is made.

The first smoke harness used the wrong trash-response field and failed after
its mutation assertions. Its fixture was removed, the harness was corrected,
and a fresh run passed. That fixture was also removed; the final production
baseline comparison passed. This deployment does not close the remaining LTS
gates below.

### Network latency and memory

Mutation timing used real LAN HTTPS through an SSH tunnel, verified certificates
and keep-alive, without Accel. The hardware proof runner was server-local.
Both matched comparisons are retained; the favorable repeat does not erase
the initial regression.

| Single-object operation | Initial run, before → candidate median (6 samples) | Repeat, before → candidate median (12 samples) | Repeat candidate maximum |
| --- | --- | --- | --- |
| Rename | 135 → 447 ms | 117 → 63 ms | 1,007 ms |
| Move | 162 → 493 ms | 321 → 36 ms | 863 ms |
| Soft delete | 127 → 985 ms | 142 → 64 ms | 453 ms |
| Exact-ID recovery | 59 → 454 ms | 70 → 37 ms | 828 ms |

The initial 32-key rename/move/delete also regressed. A separate fresh candidate
run with 1 KiB and 8 MiB payloads measured rename/move/delete/recover medians of
81/78/58/33 ms. There is no universal sub-100-ms guarantee.

| 128-object prefix operation | Before | Candidate |
| --- | ---: | ---: |
| Rename | 16.30 s | 9.25 s |
| Move | 21.04 s | 10.08 s |
| Soft delete | 27.12 s | 14.80 s |
| Recover | 12.81 s | 2.19 s |

All 1,288 NDX/payload files retained their paths and SHA-256 hashes across those
cycles. Shared devices, old-first ordering and unreset OS/device state limit
causal comparison. A separate diagnostic trace observed a directory sync
taking 326 ms; tracing is not a latency baseline.

Sampled candidate RSS/HWM was 24,636 KiB in the initial matched phase and
77,372 KiB after fresh tests including uploads. These are whole-process samples
from different histories, not per-mutation allocations or a continuous peak.

SIGKILL 150 ms into a 128-key rename left 123 source and 18 destination names,
including 13 duplicates. Their union contained all 128 correct objects.
Restart/retry finished the remaining sources; 1,024 immutable files stayed
unchanged. This proves a process-crash case, not power-cut/cache-loss durability.

### Earlier accepted results

| Date / candidate | Accepted evidence and limits |
| --- | --- |
| Oct 5, 2026 / initial format 4 | Six four-XFS modes passed: 10,000-object flat namespace, mixed-pool expansion, HTTP/native 2 GiB restart/resume, exports, concurrent GC and 18-check backup/restore. LIST median/p95/max 10.29/15.82/19.17 ms; sampled engine RSS 99,304 KiB. These used the earlier deletion design, not the Oct 6 candidate. |
| Sep 12, 2026 / 3.5.1 | Supervised backup recovery: 11 checks. Separate N1+Accel fixture: 12 API checks; refresh-based search and version-pinned byte-cache hashes. Browser mock tests were separate, not a live end-to-end proof. |
| Sep 11–12, 2026 / 3.5.1 | Two four-member offline backup drills: 18 checks each; interrupted SSH replication and fenced/resumable restore passed. Metadata-heavy restore improved 781.73 → 163.27 s; small-fixture observations only. |
| Sep 9, 2026 / 3.5.1 | 221 workspace tests and all 11 four-HDD stages; full matrix 37m 6s. Covered concurrent upload/GC/TTL, exports, repair, 1,000-object storm and 192 same-key versions. No unexpected log faults; 24 expected injected-fault lines. |
| Sep 1, 2026 / 3.5.1 | 128 MiB export interrupted during build, restarted, range-resumed over LAN and hash-verified. Final shared-hardware build 14.72 MiB/s; LAN resume 106.59 MiB/s. |
| Aug 29, 2026 / 3.5.0 | ECS compatibility for five EC layouts; four-HDD N1 matrix 22 gates / 3,846 objects. Separate 4 GiB ECS encode 205.74 MiB/s and recoverable missing-member cases passed. |
| Aug 10, 2026 / 3.5.0 | Kernel ENOSPC returned 507 with no namespace, NDX or shard publication. Local 1+0 small-object batch measured 549.70 vs 228.52 objects/s; not multi-device fault-tolerance evidence. |
| Aug 8, 2026 / 3.4.0 | Four-HDD standard/durable multipart contracts and large-object hashes passed; LAN standard multipart 93.98 MiB/s end-to-end. One-shot samples varied substantially. |
| Jul 15–16, 2026 / 3.2.x | NOA physical-topology proof and corrected three-HDD 20-gate matrix passed. Historical sub-ms folder operations used real-directory rename, not current per-child flat semantics. |
| Jun 11, 2026 / 2.0.0 | Colocated/split-root proofs, multipart restart, repair and redistribution passed; peak RSS 158,476/125,652 KiB. Those retired layouts do not define format 4. |

### Failures and corrections

- Oct 6: deeper upload checks reproduced two implementation bugs: fragment
  deletion returned success despite a permission failure, and an unexpired
  upload token recreated a deleted fragment. Error propagation/retry aliases
  and shared upload admission corrected both; the isolated hardware rerun
  above passed. Batch previously buffered before I/O admission and forced
  batched durability; admission now precedes buffering and durability follows
  the existing one-shot configuration/options. Flush requirements were not
  removed. The old real-directory batch proof fixture was replaced with an
  oversize-field rejection check, and readback was removed from ingest timing.
- Oct 6: the requested eight-hour soak failed after 6m 43s, with 18 checks
  completed. Automatic healing raced whole-fragment deletion and tried to
  publish a fragment descriptor into a removed directory, triggering the
  read-only fail-safe. Final baseline validation never ran; this is not a
  passed soak. The fix adds per-fragment repair/delete exclusion and stale-scan
  revalidation, without changing ordinary NCX mutations. All 270 warnings-denied
  serial workspace tests passed, including four new lifecycle regressions.
  The corrected release build passed 31 hardware check executions in 7m 34s,
  including six concurrent fragment-deletion rounds during healing and final
  baseline hash validation. The fresh one-hour run completed in 59m 26s with
  103 check executions, two fault cycles, 43,093 API requests and 132 fragment
  deletions during healing. Final baseline hashes passed. Intentional process
  restarts were part of that run; it was not an uninterrupted-PID memory soak.
  The live service was unchanged.
- Oct 6: initial matched latency regressed; repeat still had large outliers.
  The first handoff also failed because the candidate binary was absent; rerun
  began only after both builds were available. The failed handoff is not a pass.
- Oct 6: an old test required tombstone suppression. It was replaced explicitly
  with the approved duplicate-name/retry contract, not presented as unchanged.
- Oct 5: flat GC removed expired trash but failed to count reclaimed logical
  versions. A reproducing regression was added; the corrected GC proof passed.
- Sep 12: whole-job SIGKILL killed the backup service's restart subprocess.
  Separate-cgroup OnFailure recovery was added; the fresh 11-check drill passed.
- Sep 7–9: GC could reclaim young/active uploads through old parents or last
  trash anchors; session-inspection failure could remove protection. Cancellation
  could retain finalizer state or release locks before blocking workers finished.
  These failures were reproduced and corrected before the final candidate-8 pass.
- Sep 7–9: repair deleted writer-owned staging; concurrent trash/TTL recovery
  exposed disappearing metadata. Preserve/retry/fail-closed regressions followed.
  A blackout interrupted an earlier matrix; ten observed stages were not a full
  pass. A 30-minute aggregate deadline was exceeded during redistribution; the
  later 90-minute matrix is a separately labeled run, not a passed old deadline.
- Jul 2026: order-dependent replica digests and per-replica timestamps caused
  real hardware failures. Sorted hashing/shared timestamps fixed them. Another
  harness incorrectly expected healthy reads to repair unused parity; a corrected
  data-shard fault selector passed a fresh 21-gate matrix.
- Early native testing: missing TCP_NODELAY caused approximately 82 ms delayed
  ACK/Nagle latency. Later loopback control-call results improved; they are not
  current LAN mutation benchmarks.

## Reproduce validation

```bash
RUSTFLAGS='-D warnings' cargo test --workspace --all-targets -- --test-threads=1
RUSTFLAGS='-D warnings' cargo check --workspace --all-targets
cargo fmt --all -- --check
git diff --check
cargo build --release -p n1 --bins -p n1-release-proof
```

Default parallel test execution has intermittently failed backup automation's
immediate lease-reacquisition assertions. A serial pass is not a parallel pass.
A warning-free full Clippy gate is also not claimed.

Run optimized hardware proofs only in new isolated children of mounted members,
with unique output directories/unused ports. Never reuse live storage roots:

```bash
n1-release-proof --binary /proof/bin/litiaina-n1 \
  --source-file /proof/source.bin --disk-roots /mnt/A,/mnt/B,/mnt/C,/mnt/D \
  --out-dir /proof/results --port 56001 --native-port 56002 \
  --data-shards 3 --parity-shards 1 --storm-objects 10000 \
  --trash-retention-seconds 3600 --flat-namespace-proof
```

Also run --pool-expansion-proof, --multipart-resume-proof, --export-proof,
--backup-proof, --gc-safety-proof and --noa-api-proof separately. Large multipart
acceptance uses 2 GiB or larger; small fixtures do not establish throughput.
An extended --gc-workload-timeout-seconds run must not hide a failed default
300-second performance gate.

For offline member replacement, run `--pool-maintenance-proof` with
`--pool-member 0` and `--spare-root /mnt/SPARE` as root with a genuinely
independent spare filesystem. Repeat data/parity slots and 2+1/1+0 layouts.
On a 1+0 fixture, `--backup-proof` with `--spare-root /mnt/SPARE` additionally
places the replica and restored member on that other device. Tests must never
bind over a live storage root.

Required regressions cover immutable pool enrollment, zero-decode LIST, same-key
lineage/cancellation, NCX-only mutation, destination-before-source barriers,
joined workers, conflicts, process restart, standard/durable upload boundaries,
kernel capacity failures, original-layout repair, GC protection and portable
backup recovery. Live Console/Accel/native/NOA/WebSocket tests must also exercise
flat key/prefix behavior and partial results.

## Remaining gates and evidence

Repeated power cuts at different fsync stages, simultaneous multi-member replacement,
broader pool/failure topologies, capacity exhaustion under format 4, longer
memory/FD soaks on the patched candidate,
larger/cold namespaces, complete live consumer integration, and backup failure
cases beyond the verified interruption/retry matrix remain release gates.
Cross-filesystem bind cutovers and process kills are not physical device-loss
or controller-cache tests.

This document summarizes accepted results, failures and remaining gates.
Raw hardware and deployment reports are local-only and excluded from version
control.

No LTS, failover, zero-loss, constant-memory-at-any-scale or universal latency
claim follows from these bounded results.
