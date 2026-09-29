# N2 3.5 Testing

This document is the single correctness and resilience record for the current
N2 3.5 engine. It covers test scope, procedures, results, failures, fixes, and
remaining gates. Performance numbers belong in [BENCHMARK.md](BENCHMARK.md);
the storage laws being tested belong in [ARCHITECTURE.md](ARCHITECTURE.md).

## Qualification Boundary

N2 3.5 is a clean NCNX/object-NCDX format over the flat NCNX namespace, static
server pools, immutable NCDX/object layouts, and healing model. Its lifecycle
stores one delete mutation under
`.trash/<unix-nanos>.<mutation-ulid>/<logical-key>.ncnx`. NCNX and object NCDX
use the 3.x SHA-256 format; server-pool membership and root enrollment use
format version 5. Format v5 separates stable logical placement slots from
replaceable physical instances, adds an independent binding epoch, and records
reserve and retired instances. N2 1.x, 2.x, and earlier experimental 3.x
results do not qualify the current 3.x disk format.

The finite release proof uses four logical nodes over two physical hosts plus
one unassigned reserve process. The completed release soak uses four logical
nodes and four isolated XFS HDD roots on one physical host. Together they
exercise the configured `2 + 2` EC loss boundary, autonomous replacement of
one failed physical binding, sustained mixed workload, and bounded chaos, but
they are not five independent host, power, network, or rack failure domains.

## Repository Gates

The N2 3.5.0 implementation currently has passed:

- `cargo fmt --all -- --check`;
- `cargo check --workspace --all-targets`;
- N2 engine migration suite with warnings denied: 197/197 passed;
- ECS Core normal suite: 17/17 passed;
- ECS Core hard suite: 5/5 passed, including 128 concurrent streams, 1,000
  randomized round trips, 124 decode and 184 repair loss cases, injected
  midstream/flush failures, and a 4 GiB bounded-memory stream;
- `cargo test --workspace --lib --bins --tests`: gateway 27/27,
  native client 2/2, and shell 18/18 passed;
- optimized `cargo build -p n2 --release`;
- `git diff --check`.

These bounded code gates cover logical-slot/physical-instance separation,
reserve exclusion from placement, one-binding promotion, incompatible-root and
capacity rejection, stale-target fencing, SHA-256 NCNX/object-NCDX/shard
integrity, corrupted block reconstruction, canonical-list refill,
replica-backed degraded listing, exact static placement, exact metadata
visibility, copy/delete restart semantics, and object/healing invariants.

### Durable archive export proof — PASS, 2026-09-20

The [export job API](EXPORT.md), fixed-owner gateway routing, and shared
N1/N2 Console export flow passed the following scoped checks:

- `cargo test -p n2 -p n2_gateway --offline`: engine **219/219** and gateway
  **29/29**, including overlapping download leases and streamed downloads that
  outlive the ordinary request timeout;
- `cargo build -p n2 -p n2_gateway --offline`, Rust formatting, installer shell
  syntax, and `git diff --check`;
- Console `npm run check`: zero errors and warnings;
- Console `tests/search-export.spec.ts`: **7/7** on desktop Chromium, covering
  both engines' TAR download handoff, terminal failures, cancellation that is
  not falsely acknowledged during connection failure, and the existing N1
  search regression;
- standard Clippy completed, with no diagnostics in the new export/signing
  modules or modified archive reader. Repository-wide `-D warnings` remains
  blocked by existing lint debt; it is not claimed as a clean gate here.

The executable proof is `scripts/proof/n2_export_proof.py`. Final evidence
bundle: `n2-export-proof-0wlxg1pp`. It ran freshly built debug binaries as two
isolated loopback engine processes, one enrolled directory each, EC **1+1**,
and a third gateway process. It proved:

- exact ZIP64 and TAR contents from distributed shards, SHA-256 ETags, exact
  lengths, range resume, invalid-range rejection, and signed-URL tamper rejection;
- fragment isolation for status, download, signing, and cancellation;
- a missing selection fails the whole job without publishing a partial archive;
- owner SIGKILL during a 64 MiB build, immutable-manifest reuse after restart,
  no gateway takeover, and byte-exact recovered ZIP64 output;
- a durable source pin rejects holder-side purge while the owner is down,
  including after the holder itself restarts;
- exact mapped source byte ranges;
- acknowledged cancellation persists across owner restart and blocks delivery;
- natural 120-second retention removes state and artifact chunks while leaving
  source objects intact.

All child processes were stopped and evidence retained. No deployment service,
production data, N1 storage, or cluster membership outside the fixture changed.
This one-host process-crash proof does **not** qualify independent hosts/disks,
power loss, disk-full behavior, large-scale throughput, owner takeover, or
cluster backup/restore. Those remain distinct from export API correctness.

### N2 3.5 ECS Core two-host proof — PASS

The isolated N2 3.5 proof completed on 2026-08-29 with result `PASS`.

Private evidence bundle: `n2-ecs-3.5-real-20260829-164124`.

The exact optimized binary had SHA-256
`57a693dd5d84f195206e74ad619fc0cf902e58c2dd7cc0ed848024c1df488757`.
The topology used three logical nodes across two physical hosts, two enrolled
roots per node, one exact `2 + 1` server pool, two NCNX replicas, and no
reserve. Machine identities, addresses, credentials, and storage paths are
intentionally excluded from this public record.

The run proved:

- one 128 MiB multipart object encoded through ECS Core and downloaded
  byte-exactly through every logical node;
- 96 concurrent one-shot objects, complete ordered pagination in pages of 17,
  and one tracked prefix rename affecting all 96 objects;
- physical shard/parity participation from all six enrolled roots;
- HTTP 201 survival write after one parity-role node was stopped, with
  byte-exact read through the two surviving nodes;
- byte-exact reconstruction after one physical shard was deleted;
- autonomous healing recreated that exact missing shard path;
- full three-node restart preserved the original multipart object; and
- initial and final cluster health were healthy, fully protected, writable,
  and authority-proven, with zero panic or fatal log matches.

The proof runner is
`scripts/bench/run_n2_3_5_ecs_two_host_proof.sh`. It accepts only new isolated
test roots, never formats devices, removes its temporary node processes and
storage roots on exit, and preserves a finite evidence bundle. This closes the
bounded compatibility and real multi-root integration gate for replacing the
embedded erasure implementation with ECS Core. It does not replace the wider
topology, independent-power-domain, disk-full, partition-at-publication, or
long-duration gates below.

### N2 3.5 continued disk and coordinator-crash proof — PASS

The expanded isolated proof completed on 2026-08-30 with result `PASS`.

Private evidence bundle: `n2-ecs-3.5-fault-20260830-124150`.

The exact optimized binary had SHA-256
`8cec0e031e0fadd5e60509c986c979f0ab2204ff869c7f1ed2e3fe8a6576b86b`.
It retained the same bounded topology: three logical nodes on two physical
hosts, two roots per node, one exact EC `2 + 1` server pool, two NCNX replicas,
and no reserve. Machine identities, addresses, credentials, and storage paths
are intentionally excluded.

In addition to rerunning the normal multipart, concurrent-create, pagination,
rename, degraded-write, reconstruction, healing, and restart path, this run
proved:

- one enrolled root became unreadable and unwritable while its node remained
  online; health reported the exact root fault while preserving proven write
  authority through the node's other root;
- repair relocated an immutable missing piece to the surviving root, and a
  later object version also selected that surviving root;
- durable multipart retained both completed-part receipts across a coordinator
  `SIGKILL`, finalized after restart, and downloaded byte-exactly;
- a persisted 96-object prefix rename was killed in `planning`, recovered the
  same operation ULID after restart, and completed exactly 96 paths;
- complete loss of one parity-role node still admitted an authority-proven
  HTTP 201 survival write;
- deleted-shard reconstruction and autonomous healing passed; and
- the final full-cluster restart returned healthy, fully protected, writable,
  authority-proven state with no panic or fatal log matches.

This closes the bounded local-root failover and coordinator restart-resumption
gates for this topology. It does not claim independent power-domain, wider
pool, sustained disk-full, or multi-rack qualification.

### Historical reconstruction-threshold write gate

The earlier survival-write branch passed the complete debug and optimized
release library suites: 164 passed, 0 failed in each profile. This evidence is
retained because it proves immutable-layout reconstruction and healing, but
its `K`-only mutation admission rule is superseded by the network-partition
authority gate below. Its focused invariants included:

- the configured `K + M` layout remains unchanged at full width and at `K`;
- EC streaming persists and checksums every exact piece while only `K` writers
  survive;
- the same stream rejects `K - 1`;
- `R = M + 1` NCNX placement intersects every possible `K`-node survivor set;
- health reported exactly `K` as degraded/writable and `K - 1` as critical
  under the earlier contract;
- one failed storage root does not disable a node that retains another
  readable/writable root;
- object-local healing failures remain local reports instead of global
  read-only incidents.

The updated physical root-failure proof completed successfully:

Private evidence bundle: `n2-survival-minimum-20260731-005855`.

It proved HTTP 201 plus byte-exact GET while one `1 + 1` root was inaccessible,
HTTP 201 plus byte-exact GET while that root was at the ENOSPC boundary,
degraded/writable health at exactly `K`, no reserve consumption for root
pressure, return to full protection after restoration, and another HTTP 201
write after one complete node loss before reserve promotion. It also proved
missing NCNX repair, missing shard repair, autonomous binding-epoch advance,
historical-layout repair, HTTP 409 stale-instance fencing, and restart
persistence. The retained summary reports `PASS` and zero suspicious log
lines. N1 remained active throughout; all N2 storage paths were isolated below
`.n2-validation/<run-id>`.

### Debian local-disk and concurrent-GC closure

The final bounded closure run completed on 2026-08-01 with result `PASS`.
Evidence copied off the server is retained at:

Private evidence bundle: `n2-debian-resume-20260801-171816`.

The run used the exact optimized binary with SHA-256
`6139c2b51e566a4ea9bb5f02068b7044abccd252ee60b685a41bbfa7d46b0396`
on four isolated HDD paths. It modeled one `1 + 1` pool with two active
logical nodes, two enrolled roots per active instance, NCNX replication two,
and one compatible reserve instance. It proved:

- byte-exact upload/download, ordered pagination, trash/recover, restart, NCNX
  replica repair, and shard repair;
- loss of one preferred local disk left its node degraded but fully protected
  and writable through the other enrolled disk;
- exact-layout repair relocated the missing shard onto the healthy local disk,
  a same-key overwrite succeeded while the preferred disk was absent, and both
  versions remained readable after that disk returned;
- eight synchronized two-coordinator GC rounds reclaimed 64 expired objects;
  all 16 GC jobs completed with zero errors and every reclaimed key returned
  404;
- one complete active-node loss still allowed a survival write at `K`, reserve
  promotion advanced binding epoch 1 to 2, historical layouts healed, and the
  retired instance was fenced with HTTP 409;
- the final post-restart monitor was healthy and found zero suspicious engine
  log lines.

This is single-host, multi-process fault-injection evidence. It closes the
local multi-disk fallback and concurrent-GC correctness gates; it does not
replace the independent-host, controller, network-partition, multi-pool, or
long-duration qualification gates.

The release soak additionally requires a clean-log gate: zero engine error or
panic lines, zero availability failures, and a final sequential GC completion
on every node with zero errors and zero deferred runs. Deferred GC during an
intentional topology outage is safe and observable, but the topology must be
restored and every final proof run must complete before a release result can be
`PASS`.

### N2 3.4.1 24-hour release soak — PASS

The full release soak completed on 2026-08-09 with result `PASS`. The retained
evidence copied off the production server is stored at:

Private evidence bundle: `n2-production-deploy-20260810-030409`, including its
status, summary, and metrics summary records.

The run used four logical N2 nodes, four isolated real HDD roots, one static
`2 + 2` server pool, and NCNX replication `R = 3` on one physical host. It ran
for 86,438 seconds against a requested 86,400 seconds and completed:

| Release-soak gate | Result |
| --- | --- |
| Final result | PASS |
| Workload cycles | 6,326 |
| Chaos events | 316 |
| Availability events | 0 |
| Suspicious log lines | 0 |
| Engine error lines | 0 |
| Clean-log gate | PASS |
| Final operation | `final-verification` |

The workload continuously exercised upload, download, zero-decode paginated
listing, lifecycle operations, multipart standard and durable contracts,
native operations, restart reads, writes at the `K` survival boundary, NCNX
repair, shard repair, rolling restarts, and final GC verification. It observed
101 safe GC deferrals while namespace mutations were concurrent; every
deferred run was retried, the final verification completed, and no deferral
became an engine or availability error.

This is a completed long-duration engine qualification, not a claim of
independent-host fault tolerance. Four-host power, controller, rack, and
network-partition testing remain separate production gates.

### N2 3.4.1 multipart contract proof

The same finite proof was extended on 2026-08-10 for the NOA standard batch
contract and is retained at:

Private evidence bundle: `n2-upload-modes-20260810-020903`.

On a real two-process `1 + 1` pool, 64 independent 8 KiB objects committed in
one request in 0.603790 seconds (105.997 objects/s). All 64 downloaded
byte-exactly through the other node and remained readable after coordinator
restart. A second request containing one valid object and one object above the
1 MiB limit returned `207`, committed only the valid object, and reported the
oversized object as `413`. Engine panic/internal-error log matches remained
zero.

The finite two-process `1 + 1` upload proof completed on 2026-08-08 and is
retained at:

Private evidence bundle: `n2-upload-modes-20260808-233108`.

It used the optimized release binary, one local storage root per N2 process,
eight 16 MiB parts per 128 MiB object, and four concurrent client workers. It
proved the API and commit contract rather than independent-disk or network
capacity; both simulated nodes shared this machine's physical storage stack.

| Invariant | Result |
| --- | --- |
| Standard init writes manifest, pending binding, or part receipts | PASS: `.uploads` file count did not change |
| Independent standard parts execute concurrently | PASS: 8 parts, 4 client workers |
| Object or NCDX visible before finalize | PASS: download returned 404 and version NCDX count was zero |
| Standard finalize durability order | PASS: exact shard holders synced, NCDX published, then NCNX became visible |
| Standard download after finalize | PASS: SHA-256/byte exact |
| Standard coordinator restart | PASS: only unfinished in-memory resumability was lost; no object became live |
| Durable coordinator restart | PASS: persisted part 0 was recovered and finalized byte-exactly |
| Engine panic/internal-server-error log lines | PASS: zero |

Observed on this same-host proof topology:

- standard multipart: 128 MiB in 1.775637 seconds, 72.09 MiB/s;
- durable multipart: 128 MiB in 1.951550 seconds, 65.59 MiB/s;
- combined process CPU during the two measured uploads: 12.21 user seconds and
  1.10 system seconds;
- combined kernel-accounted writes: 537,010,176 bytes;
- end-of-measurement combined RSS: 169,000,960 bytes.

These figures compare the two contracts on this host. They are not an
independent-device, multi-host, or maximum-throughput claim.

### Historical N2 3.4.0 survival soak

The independent 24-hour four-node `2 + 2` survival soak started on
2026-07-31 and is retained at:

Private evidence bundle: `n2-3.4.0-survival-soak-20260731-011941`.

It ran the exact optimized 3.4.0 server and native-shell binaries over four
isolated HDD roots. Its workload covers one-shot and multipart upload,
download verification, zero-decode paginated listing, copy/delete lifecycle,
native protocol operations, healing, rolling node loss, writes at `K`,
rejection below `K`, and rotating bounded chaos. Startup validation reached
cycle 1 with all four 3.4.0 nodes and all HTTPS/native ports active. This
historical incomplete run is not release evidence; it is superseded by the
completed N2 3.4.1 24-hour release soak above.

## Superseded Strict-Width Storage-Root Baseline

The following retained `1 + 1` evidence predates the survival-write contract.
It proved the older strict-width behavior and remains useful regression
evidence for fault injection, but it does not qualify the new `K`-threshold
publication path.

Passing evidence:

Private evidence bundle: `n2-3.3.1-full-roots-24h-20260730-215615`.

| Fault or recovery | Required and observed result |
| --- | --- |
| Node B XFS root made inaccessible | New write rejected with HTTP 503; no NCNX/NCDX publication |
| Lookup of the rejected key while a replica root is inaccessible | HTTP 503 because namespace absence is unproven |
| Existing committed object during the inaccessible-root fault | Readable and SHA-256 exact |
| Health during the inaccessible-root fault | `degraded`; data readable; client/data/NCNX write signals false |
| Node B XFS root filled to 1 MiB free | New write rejected with HTTP 503 |
| Lookup of the rejected key while the full root remains readable | HTTP 404 because all NCNX replicas prove absence |
| Health during `ENOSPC` | `degraded`; data readable; client/data write signals false; NCNX remains readable |
| Capacity restored | Health returned to `healthy`; a new object uploaded and downloaded byte-exactly |
| Reserve during root pressure | Not consumed; the physical instance remained reachable |
| Later complete node loss | Reserve promoted, stale instance fenced, historical layout healed, restart persisted |

This historical gate models a destroyed or unusable device at the N2 process boundary by
making its dedicated XFS root inaccessible. It does not claim laboratory
coverage of every kernel, controller, cable, or media-specific `EIO` mode.
Reserve promotion remains a physical-instance replacement protocol: a
capacity-full but reachable node makes its exact-width pool safely read-only;
it is not silently rebound merely to create more capacity.

The replacement qualification must proceed through minimum `1 + 1`, standard
`2 + 2`, multi-pool, and bounded chaos stages. Those stages are not recorded
as survival-write passes until their own new summaries complete.

The node-churn regression proof for the fixes released as N2 3.3.1 completed
12 workload cycles and 11 bounded chaos events against one four-node `2 + 2`
server pool. It exercised node restart, NCNX replica loss and healing, shard
loss and healing, one-node and parity-count availability boundaries, and
rolling restart. The result was `PASS` with zero availability failures, zero
suspicious log lines, and no safety read-only marker:

Private evidence bundle: `n2-3.3-restart-healing-proof2-20260729-090038`.

The format-v5 installer dry-run passed node-only, gateway-only, and combined
installation:

Private evidence bundle: `N2-3.3-installer-proof-20260727-045424`.

## N2 3.3 Finite Release Proof

Private evidence bundle: `N2-3.3-final-proof-20260727-044408`.

The retained proof-script filename identifies the flat-NCNX proof family. This
run used the optimized N2 `3.3.0` binary, format-v5 membership, four logical
slots across two physical hosts, one exact `2 + 2` server pool, NCNX
replication `R = 3`, and one unassigned reserve instance. Its `summary.json`
result is `PASS`.

| Test | Result |
| --- | --- |
| Direct 32 MiB upload | PASS; 21.13 MiB/s |
| Download through all four active slots | PASS; every source SHA-256 matched |
| Same-key overwrite and multipart finalize | PASS |
| Eight concurrent independent keys | PASS |
| Exact-key and same-prefix one-item pages | PASS; five ordered keys |
| Copy/delete rename and repeated delete generations | PASS |
| Strict recover collision | PASS; HTTP 409, live object preserved |
| Prefix trash/recover and TTL GC | PASS |
| NCNX and object healing before failure | PASS |
| One active instance stopped | Existing object remained readable |
| Write before replacement | Correctly rejected with HTTP 503 |
| Autonomous reserve promotion | PASS; binding epoch advanced exactly once |
| Logical placement slot/topology epoch | Preserved |
| Write during historical recovery | PASS at exact `2 + 2` width |
| Retired physical instance write | Rejected with HTTP 503 |
| Historical NCNX/NCDX/shards/parity | Healed before slot activation |
| Parity-count read after replacement | PASS |
| Full active cluster restart | PASS; replacement binding and bytes persisted |
| Suspicious engine log lines | 0 |

Health changed from `degraded` with
`historical_recovery_active=true` to `healthy` with the replacement bound to
the original logical slot. `safety_read_only` remained false because exact
safe width had been restored.

The proof also recorded 233 process samples across active and reserve
processes: mean CPU `6.815%` of one logical core, peak CPU `83%`, aggregate
sampled peak RSS `230,664 KiB`, and no available hardware cycle counter
because `kernel.perf_event_paranoid=4`. The proof did not change host policy.

### Defects exposed and corrected during qualification

Earlier 3.3 proof attempts were retained as failed evidence:

- startup grace was too short for the remote native listener; the proof
  profile now uses a bounded 10-second grace while the runtime default remains
  30 seconds;
- replacement healing attempted immutable NCDX repair before rebuilding the
  recorded shard directory; shard reconstruction now precedes NCDX repair;
- an immediate post-repair probe could transiently misclassify a byte-correct
  shard; successful repairs now receive three bounded verification attempts
  separated by 25 ms;
- a promoted physical root was compared as if its physical ID were the logical
  root ID after restart; enrollment now validates physical ID plus path while
  retaining the logical placement root.

The final run above passed after those corrections.
The post-run audit found no live proof process and no per-run directory on
either remote proof disk.

## Historical N2 3.2 Finite Release Proof

Authoritative evidence directory:

Private evidence bundle: `N2-3.0-proof-20260727-013543`.

The retained proof-script filename predates N2 3.2. This run used the
optimized `3.2.0` binary, format-v4 membership, two logical nodes on each of
two physical hosts, one exact four-node `2 + 2` server pool, and NCNX
replication `R = 3`. Its `summary.json` result is `PASS`.

| Test | Result |
| --- | --- |
| Direct 64 MiB upload | PASS |
| Download through all four nodes | PASS; source SHA-256 matched |
| Same-key overwrite | PASS; two immutable versions |
| Multipart upload/finalize | PASS |
| Eight concurrent independent keys | PASS |
| Exact-key and same-prefix one-item pages | PASS; five ordered keys |
| Copy/delete rename | PASS; no trash record or object rewrite |
| Repeated same-key delete generations | PASS; distinct mutation ULIDs |
| Strict recover collision | PASS; HTTP 409 and live object preserved |
| Prefix trash/recover | PASS; 2 of 2 objects restored |
| NCNX healing | PASS; deleted replica recreated |
| Object healing | PASS; deleted shard recreated |
| One-node and parity-count loss reads | PASS |
| Write below exact pool width | PASS; HTTP 503 |
| TTL GC | PASS; two records expired, zero errors, GET returned 404 |
| Full cluster restart | PASS; committed bytes persisted |
| Suspicious log lines | 0 |

The cleanup audit confirmed all four proof processes stopped. The two empty
remote per-run parent directories exposed by that audit were removed with
`rmdir`, and the proof cleanup trap was corrected to remove them on future
runs.

The N2 3.0 implementation previously passed:

- `cargo fmt --all -- --check`;
- `cargo check --workspace --all-targets` before the lifecycle regression;
- `cargo test --workspace --lib --bins --tests` before the lifecycle
  regression;
- `cargo test -p n2` after the lifecycle/healing race fix: 117 passed;
- optimized N2 release build after the race fix;
- installer and proof-script shell syntax checks;
- `git diff --check`.

Documentation-only changes do not alter these engine results.

## Historical N2 3.1 Generation Lifecycle Proof

Authoritative evidence directory:

Private evidence bundle: `N2-3.0-proof-20260722-041213`.

The retained format-v3 proof harness predates the 3.1 package label, but this
run used the N2 `3.1.0` release binary and its `summary.json` result is `PASS`.

| Test | Result |
| --- | --- |
| Same logical key deleted in two generations | PASS; distinct mutation ULIDs |
| Physical generation placement | PASS; old generation present on all 3 deterministic NCNX holders |
| Recover while a live key exists | PASS; HTTP 409 and new live bytes unchanged |
| Recover selected second generation | PASS; exact new bytes restored |
| Prefix trash generation and recovery | PASS; 2 of 2 keys restored as one group |
| TTL GC | PASS; 2 expired versions reclaimed, 0 errors, GET returned 404 |
| NCNX auto-healing | PASS; deleted replica recreated |
| Object auto-healing | PASS; deleted shard recreated |
| Full four-node restart | PASS; committed object remained byte-exact |
| Suspicious engine log lines | 0 |

## Finite Release Proof

Authoritative evidence directory:

Private evidence bundle: `N2-3.0-proof-20260718-220508`.

Its `summary.json` result is `PASS`.

| Field | Value |
| --- | --- |
| Logical topology | Four N2 nodes |
| Physical topology | Two authorized physical hosts |
| Server pool | One format-v3 static pool |
| Object EC | 2 data + 2 parity |
| NCNX replication | 3 deterministic copies |
| Client path | Direct N2 HTTPS; no proxy, gateway, or database |
| Main test object | 64 MiB |

### API and object results

| Test | Result |
| --- | --- |
| Direct one-shot upload | PASS |
| Download through every node | PASS; byte-exact |
| Same-key overwrite | PASS; two immutable versions, newest returned |
| Multipart upload and finalize | PASS |
| Eight concurrent independent keys | PASS; all eight listed |
| Exact key plus same-name prefix pagination | PASS; five ordered one-item pages, no omission |
| Twenty exact rename/move operations | PASS; source removed, destination readable |
| Rename object-plane immutability | PASS; no rename-created trash and no object rewrite |
| Folder/prefix trash and recovery | PASS; two of two restored |
| TTL expiry and GC | PASS; expired key reclaimed, later GET returned 404 |
| Full four-node restart | PASS; existing object remained byte-exact |

### Failure and healing results

| Test | Result |
| --- | --- |
| Delete one NCNX replica | Healing recreated the exact immutable replica |
| Delete one object shard | Healing recreated the exact shard |
| Stop one node | Existing object readable; unsafe new write rejected with 503 |
| Stop two nodes in 2+2 | Existing object remained byte-exact from K survivors |
| Healing outcome | 17 namespace records scanned; 1 NCNX and 1 shard repaired |
| Unrecoverable/safety transition | 0 unrecoverable; safety read-only remained false |
| Suspicious selected engine logs | 0 in the finite proof |

## Namespace Invariants

Code and release tests exercise these flat-namespace laws:

- NCNX absence is accepted only when every assigned replica proves absence.
- One missing or corrupt NCNX copy is tolerated only when surviving valid
  copies contain identical immutable bytes.
- Divergent valid NCNX bytes fail closed.
- Strict publication can retry identical immutable bytes.
- Exact rename publishes a new destination mutation generation before
  removing only the expected source bytes.
- A retry cannot remove a newer same-key generation.
- Prefix rename/trash/recover applies the exact-key protocol with bounded
  concurrency and records recoverable mutation groups.
- Listing merges physical replica names without decoding NCNX or NCDX.
- Replica-page failures are tolerated only within the pool's exact `M`-node
  namespace failure budget.
- Exact-key and same-name-prefix order survives continuation pagination.
- Root identity rejects in-place root-set changes after enrollment.
- Upload `K + M` must equal the static pool contract.
- Object placement is calculated from immutable pool membership; an
  unavailable selected holder is not replaced by an outage-dependent holder.
- NCNX, object-NCDX, upload-body, shard, and verified-block integrity use
  SHA-256 only. NCNX and object NCDX reject payload mutation independently of
  replicas.
- Metadata absence or latest-version selection is accepted only after an
  exact selected NCDX holder responds for every candidate server pool.
- Removed dynamic EC, node-role, hard-delete, and empty-trash contracts are
  rejected.

### Coordinator-Owned Prefix Operation Proof — 2026-08-11

Local loopback evidence is retained at:

Private evidence bundle: `n2-namespace-operations-final-20260811-150320`.

The proof ran two active processes as one exact EC `1 + 1` server pool plus
one reserve process. Exact-key rename remained synchronous (`200`, one NCNX
path, no operation ID). Both forward and reverse folder rename, folder trash,
and folder recovery returned `202 Accepted`, exposed coordinator-owned
operation ULIDs, and completed `4/4` exact NCNX paths with zero failures. The
same finite proof also passed cross-node readback, pagination, NCNX repair,
shard repair, restart persistence, reserve promotion and historical-layout
healing. The final result was `PASS` with zero suspicious selected engine log
lines.

### Bounded Prefix-Rename Regression — 2026-08-12

Local evidence copied from the isolated four-HDD host proof is retained at:

Private evidence bundle: `n2-prefix-rename-optimization-20260812`.

The optimized N2 3.4.1 release binary passed four complete 245-object
forward/reverse rename cycles on a four-node EC `3 + 1` pool. Exact final
replica counts were verified from disk. Additional gates passed:

- destination publication remained durable-before-source-removal;
- an unavailable NCNX holder caused a truthful failed operation, and retry
  converged both replicas to byte-identical immutable bytes;
- four simultaneous independent prefix jobs completed 256/256 entries under
  global bounded admission;
- an existing destination failed as a conflict, not service unavailable, and
  caused zero source mutation;
- the full workspace suite passed 347 tests with zero failures; eight declared
  long/random SIMD cases remained ignored.

Strict workspace Clippy remains blocked by pre-existing metadata omissions and
existing lints outside this change. No unrelated manifest, legacy module, or
SIMD implementation was modified as part of this optimization.

## Lifecycle and Healing Race Regression

The first requested 12-hour soak did not pass. It failed after 1,287 seconds
at cycle 122 and exposed a real race between autonomous healing and TTL GC.
That failure is preserved at:

Private failure artifact: `N2-3.0-soak-20260719-024217/failure.json`.

The defect was a time-of-check/time-of-use window: healing could act on an
NCNX/trash scan entry after concurrent lifecycle work had already changed or
expired it. The patch added exact state revalidation at the decision boundary:

- stale NCNX entries are revalidated before critical classification;
- TTL-expired trash is GC-owned and healing does not resurrect it;
- dangling object-reference reports revalidate the exact live reference;
- genuinely unsafe topology still fails closed.

Post-fix targeted engine tests passed 117/117.

### Post-fix regression runs

| Run | Evidence | Cycles | Chaos phases | Availability failures | Suspicious events | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Lifecycle regression | `N2-3.0-soak-lifecycle-regression-20260719-1159` | 65 | 0 | 0 | 0 | PASS |
| Six-phase chaos regression | `N2-3.0-soak-chaos-regression-20260719-1203` | 65 | 6 | 0 | 0 | PASS |

The lifecycle regression ran for 168 seconds. The chaos regression ran for
180 seconds and ended healthy with `safety_read_only=false`.

These regression runs proved the reproduced race no longer appeared in the
bounded scenarios. The later N2 3.4.1 24-hour release soak completed with a
clean `PASS` and supersedes the former pending 12-hour gate.

## Test Procedure

Run local gates:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --lib --bins --tests
cargo build --workspace --release
```

The N2 Shell commissioning gate additionally proves protected-system-disk
rejection, agent/serial-bound format confirmation, UUID-and-serial disk
pinning, exact-width pool validation, active/reserve role exclusion,
credential-independent canonical hashing, staged-content tamper rejection,
node-local service fencing, idempotent commit, draft evolution/restore, and a
real HTTP two-agent prepare/commit that leaves byte-identical membership on
both agents while every engine and gateway remains stopped. Run it with:

```bash
N2_TEST_ROOT=<private-test-root>/n2-shell-tests cargo test -p n2_shell
```

Run the finite multi-node proof with isolated roots:

```bash
REMOTE_PASS='operator-supplied' \
  scripts/bench/run_n2_3_0_release_proof.sh
```

Inspect soak options before starting a bounded or 12-hour run:

```bash
scripts/bench/run_n2_3_0_soak_chaos.sh --help
```

Every real-machine run must:

- use explicit isolated test roots and ports;
- refuse to touch pre-existing N2 or unrelated data;
- record configuration, binary hash, topology, timestamps, logs, and output;
- perform byte checksums before and after failure injection;
- distinguish expected fail-closed responses from availability failures;
- retain the first failure artifact instead of hiding it with a rerun;
- report elapsed duration honestly.

## Network-Partition Release Gate

This gate is intentionally separate from stopped-process, lost-disk, and
reserve-promotion testing. A stopped process proved that the surviving node can
read committed data, accept a `K`-boundary object, and heal it after the missing
node returns. A network partition keeps both processes alive while blocking
some or all internode communication; clients or gateways may still reach both
sides.

### Two-host evidence

The independent two-host `1 + 1` run retained at:

Private evidence bundle: `n2-two-host-1p1-20260823-030752`.

proved byte-exact one-shot, batch, standard and durable multipart, pagination,
prefix rename/trash/recovery, all configured disk-root participation, NCNX and
shard repair, local-root loss, complete process loss, `K=1` survivor write,
healing, and rolling restart. The core result is `CORE_PASS`.

The same run then kept both N2 processes alive and silently dropped only their
test-port traffic in both directions. Reads and new/same-key writes returned
`503`; reconnection exposed no rejected key, no partial publication, no NCNX
disagreement, and no changed original HEAD. This is a safety pass with an
explicit availability limit, recorded as:

```text
PASS_WITH_EXPLICIT_PARTITION_AVAILABILITY_LIMIT
```

That run was not a complete partition proof. Its silent-DROP result depended
on timeout ordering and did not cover fast failure or asymmetric links.

The targeted authority patch and real two-host validation on 2026-08-24 is
retained at:

Private evidence bundle: `n2-partition-authority-20260824`.

It used one physical node on the Debian development host with two enrolled
roots and a second physical host with four isolated HDD roots,
EC `1 + 1`, NCNX replication two, and the optimized N2 binary. Production N1
on the server remained active and its roots were not modified.

Results:

- healthy baseline upload returned HTTP 201, read back byte-exact, and exposed
  `healthy`, `write_authority_proven=true`, and
  `effective_client_writable=true`;
- bidirectional peer-HTTP rejection kept both processes alive while native
  shard traffic remained connected, rejected
  concurrent different-byte writes to the same key with HTTP 503 on both
  coordinators, and kept previously committed reads byte-exact on both sides;
- after reconnection the rejected key returned 404, the original HEAD ULID was
  unchanged, and health returned to writable/healthy;
- in each asymmetric direction, exactly the coordinator that could reach the
  complete pool returned HTTP 201 while the isolated coordinator returned
  HTTP 503; both accepted objects read back byte-exact;
- complete loss of the second process left committed reads byte-exact and
  rejected new writes with HTTP 503; restart restored healthy write authority;
- a native-only bidirectional partition retained exact NCNX authority: both
  independent survivor writes returned HTTP 201, health truthfully reported
  `degraded`, `write_authority_proven=true`,
  `effective_client_writable=true` under the then-current health contract, and
  `data_plane_fully_protected=false`, and both objects read back byte-exact
  after reconnection;
- 64 concurrent healthy uploads returned 64 HTTP 201 responses, and file
  evidence appeared on both local roots and all four remote roots;
- simultaneous different-byte creation of one absent key through both healthy
  coordinators produced one HTTP 201 winner and one HTTP 409 conflict;
- the focused distribution suite passed 87/87 before cleanup.

This closes the pre-existing bidirectional peer-HTTP fast-REJECT,
native-only survivor, asymmetric-link, stopped-process, reconnection, and
exact same-key authority cases. It does not yet claim silent-DROP timing,
crash-at-every-NCNX-stage, wider EC layouts, or independent-witness
availability.

### Three-host `2 + 1` authority and degraded-root correction

The follow-up physical proof on 2026-08-26 used one exact `2 + 1` pool over
three independent hosts. One slot had one isolated root, one slot had one
isolated root, and one slot had four isolated roots. Existing production
services remained active and no production storage root was enrolled or
modified.

Private evidence bundle: `n2-survivor-root-retest-20260826-continued`.

The first overlapping-view experiment is deliberately retained as failed
evidence. A provisional rule requiring only `K` plus a pool majority allowed
views `A+B` and `B+C` to acknowledge different bytes for the same key when
that key's exact NCNX layout was `A+C`. Reconnection exposed two valid,
divergent NCNX records. This disproved reachability-majority as a sufficient
standalone namespace rule before release.

The corrected rule retains `A = max(K, strict pool majority)` and additionally
requires every exact NCNX replica for the affected key inside the authorized
component. Repeating the same synchronized overlapping-view test produced:

- HTTP 503 from both conflicting coordinators;
- HTTP 404 for the rejected key after reconnection;
- no live NCNX publication and no arbitrary HEAD selection;
- with one node stopped rather than ambiguously partitioned, HTTP 201 only for
  a key whose complete exact two-replica NCNX layout survived, and HTTP 503
  for a key whose layout included the missing node;
- byte-exact readback of the admitted degraded object;
- a three-item prefix rename, trash, and recovery completing 3/3 while the
  exact per-key authority condition remained satisfied;
- degraded startup of the four-root node with one configured root replaced by
  a blocker file, no recreation of the missing mount path, successful write
  through its remaining roots, and healthy restart after restoration.

The same proof exposed a separate maintenance defect: the fragment repair job
held one background-repair scheduler permit for its entire run, then each
object attempted to acquire another. On a one-root node, all affected objects
were skipped as scheduler-saturated while the job incorrectly reported
`Completed`, zero repaired, and zero failed. The job-wide permit was removed,
per-object repair now waits for scheduler admission, and batch execution
errors increment the failure count instead of disappearing.

A fresh isolated three-process `2 + 1` reproduction using the corrected
optimized binary then proved the repair closure:

| Repair invariant | Result |
| --- | --- |
| State before repair | `degraded`, 2/3 healthy, 1 missing, repairable |
| Repair result | `Completed`, 1 repaired, 0 failed |
| State after repair | `healthy`, 3/3 healthy, 0 missing |
| Reads through all three nodes | SHA-256 and byte exact |

This is a bounded architectural pass for the corrected `2 + 1` cases. It does
not close the wider-topology, mid-publication crash, silent-DROP timing, or
independent power/controller gates listed below.

### Conflict that must be prevented

The critical test is not two isolated writes to different keys. It is two
acknowledged mutations of the same logical NCNX key:

```text
component A: videos/a.mp4 -> object-A / version-A
component B: videos/a.mp4 -> object-B / version-B
```

The immutable objects can coexist physically, but both NCNX records are valid
and neither is missing or corrupt. Selecting either record would hide an
acknowledged write. Autonomous healing must report disagreement rather than
guess a winner, so admission must prevent this state from being acknowledged.

### Required topology matrix

Run every partition case first on the minimum real two-host `1 + 1` pool, then
on simulated or physical `2 + 1`, `3 + 1`, `3 + 2`, `2 + 2`, and a wider
`K > M` pool. Record both configured and observed components.

| EC layout | Authority condition to prove |
| --- | --- |
| `1 + 1` without witness | `A=2`; no one-node component may mutate |
| `1 + 1` with independent witness/reserve | Exactly one fenced one-node component may receive authority after a binding/authority transition |
| `2 + 1` | `A=2`, plus all exact `R=2` NCNX holders for the key |
| `3 + 1` | `A=3`, plus all exact `R=2` NCNX holders for the key |
| `3 + 2` | `A=3`, plus all exact `R=3` NCNX holders for the key |
| `2 + 2` | `A=3`; two-node halves fail closed, plus all exact `R=3` NCNX holders |
| wider `K > M` | `A=max(K, strict majority)`, plus all exact `R=M+1` NCNX holders |

### Required fault matrix

Each fault must affect only isolated test addresses, ports, processes, and
roots. Preserve the first result before rerunning.

1. Bidirectional silent `DROP` of peer HTTP and native traffic.
2. Bidirectional immediate `REJECT`, TCP reset, and connection-refused paths.
3. One-way/asymmetric partitions in both directions.
4. Peer HTTP partition with native traffic available, then the inverse.
5. Gateway able to reach both nodes while internode traffic is partitioned.
6. Gateway able to reach only one component while another gateway or direct
   client reaches the other.
7. Process pause and event-loop stall without process exit.
8. Partition during EC streaming, NCDX publication, NCNX publication, and the
   destination-first/source-delete stages.
9. Restart of one or both components while the partition remains active.
10. Reconnection with stale membership/binding epochs and a retired physical
    instance attempting to publish.

### Required operation matrix

For every relevant fault, synchronize coordinators so the operations overlap:

- create the same absent key with different bytes and object IDs;
- overwrite the same existing key with different bytes;
- overwrite versus trash/delete;
- rename two sources onto one destination;
- rename versus trash, recover versus new live creation, and double recovery;
- standard and durable multipart finalization of the same logical key;
- batch entries containing same-key conflicts and independent keys;
- independent-key writes, to quantify availability without weakening same-key
  safety;
- GET/HEAD of an existing key and negative lookup of an absent key on both
  components;
- paginated LIST before partition, on each component, and after convergence.

### Pass criteria

A partition test passes only when all of the following hold:

- at most one conflicting mutation receives a success response;
- a rejected mutation publishes no live NCNX and no visible partial object;
- no key has two valid live NCNX records with different object identities;
- every previously committed object remains byte-exact when at least K pieces
  are reachable;
- an unauthorized or stale component is read-only for mutation traffic;
- reconnection requires no arbitrary HEAD selection and healing restores only
  missing exact NCNX/NCDX/shard placements;
- restart cannot revive expired authority or a retired binding;
- gateway retries cannot convert a rejected mutation into a second commit;
- expected fail-closed responses are distinguished from engine errors;
- logs contain no panic, corruption, checksum mismatch, or unexplained
  internal error;
- the run records request outcomes, mutation/object/version IDs, membership and
  binding epochs, component views, peer calls, latency, CPU, and IO.

### Implemented policy and future availability extensions

The current implementation uses a **strict-majority plus exact-key policy**.
Before mutation, the coordinator proves
`A = max(K, floor((K+M)/2)+1)` bound pool nodes reachable. An existing NCNX
uses its recorded historical pool; a new upload uses the newest writable pool.
The mutation then requires every exact `R=M+1` NCNX holder for that key inside
the authorized component. `K` remains data-reconstruction truth and cannot
become namespace authority merely because a peer timed out. Health reports
pool-level authority separately through `write_authority_proven` and remains
conservative when it cannot guarantee every possible key's exact NCNX set.

Authority probes use peer HTTP because NCNX is the mutation boundary. Native
shard reachability remains a separate data-plane observation: after authority
is proven, the fixed layout may commit at `K` and retain unavailable exact
roles for healing. Tests must therefore partition HTTP and native traffic
independently as well as together.

Possible future availability extensions remain release-gated:

1. **Independent witness through a compatible reserve or dedicated control
   instance.** Use it only to break a balanced `K=M` tie. The witness stores no
   object shard and must not become a gateway-owned hidden database.
2. **Operator-confirmed physical fencing.** Permit a survivor after an operator
   or hardware controller proves the other instance cannot publish. This is a
   valid low-end fallback when no independent witness exists.

The following are rejected as standalone solutions:

- increasing the outer timeout or treating fast connection failure as proof of
  node death;
- allowing both components to write and asking healing to select a winner;
- making the route-only gateway the sole authority without restart-proof
  fencing and multi-gateway semantics;
- adding a cluster-wide namespace database, per-key global lock, or healthy
  data-path quorum merely to cover degraded operation;
- importing another storage engine's quorum implementation instead of proving
  an N2-specific control boundary.

### Planned execution order

1. Complete crash injection during EC, NCDX, and each NCNX publication stage.
2. Exercise peer-HTTP-only and native-only partition variants.
3. Exercise stale membership/binding epochs and retired-instance publication
   while partitioned.
4. Repeat the matrix on `2 + 1`, `3 + 1`, `3 + 2`, `2 + 2`, and a wider pool.
5. Run restart proof, repair proof, and a focused healthy-path benchmark.
6. Only after finite proof passes, run a bounded soak with repeated partition
   and reconnection cycles.

Until those remaining cases close, the two-host result proves the implemented
fail-closed rule for existing fast/asymmetric partitions, not uninterrupted
write availability or every mid-commit partition timing.

## Remaining Release Gates

- Repeat on four independent physical nodes for independent failure domains.
- Complete the remaining mid-commit and wider-topology partition cases above;
  stopped processes alone do not qualify them.
- Exercise sustained disk-full and media replacement on isolated test roots.
- Repeat at larger namespace cardinality and with mixed multipart workloads.
- Continue tracking namespace p95/p99 latency and process CPU/IO during long
  runs.

Passing the bounded tests establishes the listed invariants. It does not prove
unlimited scale, every hardware failure, or unconditional production
stability.
