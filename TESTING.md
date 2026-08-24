# N2 3.4 Testing

This document is the single correctness and resilience record for the current
N2 3.4 engine. It covers test scope, procedures, results, failures, fixes, and
remaining gates. Performance numbers belong in [BENCHMARK.md](BENCHMARK.md);
the storage laws being tested belong in [ARCHITECTURE.md](ARCHITECTURE.md).

## Qualification Boundary

N2 3.4 is a clean NCNX/object-NCDX format over the flat NCNX namespace, static
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

The N2 3.4.1 implementation currently has passed:

- `cargo fmt --all -- --check`;
- `cargo check --workspace --all-targets`;
- `cargo test --workspace --lib --bins --tests`: N2 192/192, gateway 27/27,
  native client 2/2, and shell 18/18 passed;
- SIMD erasure core: 107 passed with 6 intentionally ignored; integration:
  5 passed with 2 intentionally ignored;
- optimized `cargo build -p n2 --release`;
- `git diff --check`.

These bounded code gates cover logical-slot/physical-instance separation,
reserve exclusion from placement, one-binding promotion, incompatible-root and
capacity rejection, stale-target fencing, SHA-256 NCNX/object-NCDX/shard
integrity, corrupted block reconstruction, canonical-list refill,
replica-backed degraded listing, exact static placement, exact metadata
visibility, copy/delete restart semantics, and object/healing invariants.

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

```text
/home/administrator/DEV-TEST/n2-survival-minimum-20260731-005855/minimum
```

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

```text
/home/litiaina/DEV-TEST/n2-debian-resume-20260801-171816/server-full-closure/results
```

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

```text
/home/litiaina/DEV_TEST/n2-production-deploy-20260810-030409/soak-status.json
/home/litiaina/DEV_TEST/n2-production-deploy-20260810-030409/soak-summary.json
/home/litiaina/DEV_TEST/n2-production-deploy-20260810-030409/soak-metrics-summary.json
```

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

```text
/home/litiaina/DEV_TEST/n2-upload-modes-20260810-020903
```

On a real two-process `1 + 1` pool, 64 independent 8 KiB objects committed in
one request in 0.603790 seconds (105.997 objects/s). All 64 downloaded
byte-exactly through the other node and remained readable after coordinator
restart. A second request containing one valid object and one object above the
1 MiB limit returned `207`, committed only the valid object, and reported the
oversized object as `413`. Engine panic/internal-error log matches remained
zero.

The finite two-process `1 + 1` upload proof completed on 2026-08-08 and is
retained at:

```text
/home/litiaina/DEV_TEST/n2-upload-modes-20260808-233108
```

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

```text
/home/administrator/DEV-TEST/n2-3.4.0-survival-soak-20260731-011941
```

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

```text
/home/administrator/DEV-TEST/n2-3.3.1-full-roots-24h-20260730-215615/minimum
```

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

```text
/home/administrator/DEV-TEST/n2-3.3-restart-healing-proof2-20260729-090038
```

The format-v5 installer dry-run passed node-only, gateway-only, and combined
installation:

```text
/home/wlzambo/DEV-TEST/N2-3.3-installer-proof-20260727-045424
```

## N2 3.3 Finite Release Proof

```text
/home/wlzambo/DEV-TEST/N2-3.3-final-proof/N2-3.0-proof-20260727-044408
```

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

```text
/home/wlzambo/DEV-TEST/N2-3.0-proof-20260727-013543
```

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

```text
/home/wlzambo/DEV-TEST/N2-3.0-proof-20260722-041213
```

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

```text
/home/wlzambo/DEV-TEST/N2-3.0-proof-20260718-220508
```

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

```text
/home/litiaina/DEV_TEST/n2-namespace-operations-final-20260811-150320/report
```

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

```text
/home/litiaina/DEV_TEST/n2-prefix-rename-optimization-20260812/remote-evidence
```

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

```text
/home/wlzambo/DEV-TEST/N2-3.0-soak-20260719-024217/failure.json
```

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
| Lifecycle regression | `/home/wlzambo/DEV-TEST/N2-3.0-soak-lifecycle-regression-20260719-1159` | 65 | 0 | 0 | 0 | PASS |
| Six-phase chaos regression | `/home/wlzambo/DEV-TEST/N2-3.0-soak-chaos-regression-20260719-1203` | 65 | 6 | 0 | 0 | PASS |

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
N2_TEST_ROOT=/home/litiaina/DEV_TEST/n2-shell-tests cargo test -p n2_shell
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

```text
/home/litiaina/dev-test/n2-two-host-1p1-20260823-030752
```

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

```text
/home/litiaina/dev-test/n2-partition-authority-20260824
```

It used one physical node on the Debian development host with two enrolled
roots and one physical node on `192.168.0.12` with four isolated HDD roots,
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
  `effective_client_writable=true`, and
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
| `1 + 1` without witness | No automatic side selection; fail closed while authority is ambiguous |
| `1 + 1` with independent witness/reserve | Exactly one one-node component may receive authority |
| `2 + 1` | Only a component with at least two exclusive voters may mutate |
| `3 + 1` | Only a component with at least three exclusive voters may mutate |
| `3 + 2` | Only a component with at least three exclusive voters may mutate |
| `2 + 2` | Two-node halves need an independent witness or must fail closed |
| wider `K > M` | Publication still requires K writable EC holders and one exclusive authority component |

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

The current implementation uses an **explicit fail-closed partition policy**.
Before mutating an existing NCNX, the coordinator concurrently proves its
complete exact recorded pool reachable; a pool sealed against new object
placement remains valid for this operation. New uploads instead prove the
complete newest writable pool. Upload publication requires every exact NCNX
replica. `K` remains data-reconstruction truth and never becomes authority
merely because a peer timed out. Health reports new-write authority separately
through `write_authority_proven`.

Authority probes use peer HTTP because NCNX is the mutation boundary. Native
shard reachability remains a separate data-plane observation: after authority
is proven, the fixed layout may commit at `K` and retain unavailable exact
roles for healing. Tests must therefore partition HTTP and native traffic
independently as well as together.

Possible future availability extensions remain release-gated:

1. **Fragment-scoped survivor authority.** Extend the epoch-bound lease/fencing
   concept already used by autonomous healing. Acquire it only on degradation,
   permit concurrent work under the authorized fragment/component, and require
   it on every degraded NCNX mutation. Healthy writes must gain no lock or
   authority round trip.
2. **Independent witness through a compatible reserve or dedicated control
   instance.** Use it only to break a balanced `K=M` tie. The witness stores no
   object shard and must not become a gateway-owned hidden database.
3. **Operator-confirmed physical fencing.** Permit a survivor after an operator
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
