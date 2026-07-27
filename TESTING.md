# N2 3.3 Testing

This document is the single correctness and resilience record for the current
N2 3.3 engine. It covers test scope, procedures, results, failures, fixes, and
remaining gates. Performance numbers belong in [BENCHMARK.md](BENCHMARK.md);
the storage laws being tested belong in [ARCHITECTURE.md](ARCHITECTURE.md).

## Qualification Boundary

N2 3.3 is a clean NCNX/object-NCDX format over the flat NCNX namespace, static
server pools, immutable NCDX/object layouts, and healing model. Its lifecycle
stores one delete mutation under
`.trash/<unix-nanos>.<mutation-ulid>/<logical-key>.ncnx`. NCNX and object NCDX
use the 3.x SHA-256 format; server-pool membership and root enrollment use
format version 5. Format v5 separates stable logical placement slots from
replaceable physical instances, adds an independent binding epoch, and records
reserve and retired instances. N2 1.x, 2.x, and earlier experimental 3.x
results do not qualify the 3.3 disk format.

The current finite release proof uses four logical nodes over two physical
hosts plus one unassigned reserve process. It exercises the configured `2 + 2`
EC loss boundary and autonomous replacement of one failed physical binding,
but those processes are not five independent host, power, network, or rack
failure domains.

## Repository Gates

The N2 3.3 implementation currently has passed:

- `cargo fmt --all -- --check`;
- `cargo check --workspace --all-targets`;
- `cargo test -p n2 --all-targets`: 140 passed;
- optimized `cargo build -p n2 --release`;
- `git diff --check`.

These bounded code gates cover logical-slot/physical-instance separation,
reserve exclusion from placement, one-binding promotion, incompatible-root and
capacity rejection, stale-target fencing, SHA-256 NCNX/object-NCDX/shard
integrity, corrupted block reconstruction, canonical-list refill,
replica-backed degraded listing, exact static placement, exact metadata
visibility, copy/delete restart semantics, and object/healing invariants.

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
- Listing emits canonical replicas and uses physical names without decoding
  NCNX or NCDX.
- A canonical listing-source failure retries physical replica streams, and
  only within the pool's exact `M`-node namespace failure budget.
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

These regression runs prove the reproduced race no longer appears in the
bounded scenarios. They do not replace a completed 12-hour post-fix soak.
A full 12-hour post-fix run remains pending and must not be reported as
passed.

## Test Procedure

Run local gates:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --lib --bins --tests
cargo build --workspace --release
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

## Remaining Release Gates

- Complete the full 12-hour post-fix soak/chaos run.
- Repeat on four independent physical nodes for independent failure domains.
- Exercise network partitions, not only stopped processes.
- Exercise sustained disk-full and media replacement on isolated test roots.
- Repeat at larger namespace cardinality and with mixed multipart workloads.
- Continue tracking namespace p95/p99 latency and process CPU/IO during long
  runs.

Passing the bounded tests establishes the listed invariants. It does not prove
unlimited scale, every hardware failure, or unconditional production
stability.
