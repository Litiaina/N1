# N2 3.0 Verification

## Scope

N2 3.0 is a new on-disk and API engine generation. It does not read or migrate
N2 1.x/2.x namespace state. This verification covers the flat NCNX namespace,
immutable object layout, static 2+2 EC pool, lifecycle, autonomous healing,
restart behavior, and direct NOA operations.

The authoritative finite proof is:

`/home/wlzambo/DEV-TEST/N2-3.0-proof-20260718-220508`

Its `summary.json` result is `PASS`.

## Architecture under test

Each enrolled node is bidirectional and can coordinate an API request. A pool
generation fixes its complete node set, each node's complete root set, object
EC topology, and NCNX replica count. Expansion creates a new complete pool
generation; it does not remap the roots of an enrolled pool or rewrite old
objects.

Within a fragment, a logical key is represented by a small immutable `.ncnx`
record replicated deterministically. The record points to the stable object ID.
NCDX records the exact immutable version, pool generation, holders, and K+M
layout. Shards and NCDX do not move during namespace-only mutation.

## Moving parts per operation

| Operation | Bounded work |
| --- | --- |
| GET/stat | Read NCNX replicas in parallel, select identical valid bytes, then read NCDX; GET reads the required shards and reconstructs if necessary |
| LIST | Page physical NCNX names from participating nodes in parallel, use a prefix-pruned lexical walk with memory proportional to the widest visited directory, emit canonical logical names, and perform a bounded merge; no NCNX or NCDX payload decode |
| Upload | Publish K+M shards, identical NCDX replicas, then strictly publish R NCNX replicas |
| Rename | Prove source and destination state, strictly publish destination NCNX, strictly remove expected source NCNX; four sequential namespace phases with parallel replica fan-out |
| Delete | Prove source, strictly publish the trash NCNX record, then strictly remove expected live NCNX |
| Recover | Read trash and prove destination state, publish live NCNX, then remove expected trash NCNX; folder members use bounded concurrency |
| GC | Build canonical live/unexpired references, scan expired `.trash`, remove object holders before the pointer; normal GC does not scan every object directory for orphans |
| Healing | One deterministic fragment owner scans; exact NCNX/NCDX/layout replicas are repaired and disposable per-event JSON reports are written independently |

No operation uses a global namespace database. Namespace publications are
immutable compare-and-publish or compare-and-remove operations; conflicting
valid bytes fail closed.

## Finite four-node proof

| Test | Result |
| --- | --- |
| Direct 64 MiB upload and download through every node | PASS; byte-exact |
| Same logical key overwritten twice | PASS; 2 immutable versions, newest returned |
| Eight concurrent independent logical keys | PASS; 8 of 8 present in zero-decode list |
| Exact key plus same-name prefix pagination | PASS; 5 ordered one-item pages with no omission |
| Twenty exact renames | PASS; source removed, destination readable, zero rename-created trash records |
| Folder trash and recovery | PASS; 2 of 2 restored |
| Delete one NCNX replica and run healing | PASS; exact replica recreated |
| Delete one object shard and run healing | PASS; exact shard recreated |
| Stop one node | Existing object readable; new unsafe write rejected with 503 |
| Stop two nodes in 2+2 | Existing object still byte-exact from parity-count survivors |
| TTL expiry and GC | One trash version expired, zero errors, later GET 404 |
| Restart all four logical nodes | Existing object remained byte-exact |
| Engine logs | Zero suspicious panic/error/corruption lines selected by the proof |

Healing evidence recorded 17 namespace records scanned, one NCNX replica
repaired, one object repaired, one shard repaired, zero failed/unrecoverable
objects, and no safety read-only transition.

## Namespace invariants exercised by code tests

- NCNX absence is accepted only after every required replica proves absence.
- One unavailable or corrupt NCNX replica is tolerated only when surviving
  valid replicas contain identical bytes.
- Divergent valid replica bytes fail closed.
- A partial strict publication can retry the same immutable bytes.
- Rename destination receives a new mutation generation.
- Folder trash recovery restores the complete mutation group.
- Root identity rejects an in-place local root-set change after enrollment.
- Default upload K+M is identical to the configured cluster K+M.
- Removed `mkdir`, manual purge, and empty-trash shell contracts cannot parse.

The final local repository gate passed `cargo fmt --all -- --check`,
`cargo check --workspace --all-targets`, and
`cargo test --workspace --lib --bins --tests`: N2 core 115/115, gateway 16/16,
native client 1/1, shell 4/4, EC library 107 passed with 6 intentional long
tests ignored, and EC integration 5 passed with 2 hardware-random tests
ignored. Installer and release-proof shell syntax and `git diff --check` also
passed.

## Performance evidence

The proof measured 46.49 MiB/s upload and 71.45-129.74 MiB/s downloads. List
latency was 11.362 ms p50 / 17.901 ms p95. Exact stat was 49.708 ms p50 /
52.427 ms p95. Exact rename was 84.555 ms p50 / 689.807 ms p95. Average node
process CPU was 5.11-12.10% of one logical core; observed peak RSS was
71.04-160.69 MiB.

Hardware CPU-cycle counters could not be collected under the host's
`perf_event_paranoid=4` policy. That policy was deliberately not changed.

## Honest release boundary

This evidence uses four logical nodes over two physical hosts. It is a finite
architectural and failure proof, not a long soak, a four-physical-host proof,
or an infinite-scale proof. It proves the listed N2 3.0 invariants and tested
failure boundaries. Unexercised hardware, workload, and long-duration failure
modes remain production-observation responsibilities.
