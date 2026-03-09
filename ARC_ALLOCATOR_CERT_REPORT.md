# N1 Arc Allocator Certification Report

Report timestamp (UTC): `2026-03-08 16:30:24Z`  
Code revision: `9660a63`

## Scope

This report records allocator/formatter gate evidence for N1 Arc (`block_manager` + Arc raw blob integration).

Primary gate harness:

`scripts/bench/run_arc_allocator_gate.sh`

## Gate Profiles

- `MODE=quick`: allocator correctness + formatter intent checks + release drill workflow set (no chaos by default).
- `MODE=full`: quick profile + HA chaos harness.

## Latest Evidence

### Quick gate (latest complete run)

Artifact root:

`/home/administrator/Development/N1/.n1-bench/arc-allocator-gate-20260308-232554`

Summary:

- `status=PASS`
- `steps_pass=4`
- `steps_fail=0`
- `wall_seconds:avg=3.715 max=13.351`

Step results:

| Step | Exit | Wall (s) | Passed |
|---|---:|---:|---:|
| `block_manager_tests` | 0 | 1.001 | 1 |
| `arc_formatter_nonce_roundtrip` | 0 | 0.266 | 1 |
| `arc_extent_gen_legacy_default` | 0 | 0.242 | 1 |
| `arc_release_drills` | 0 | 13.351 | 1 |

### Full gate (latest complete run)

Artifact root:

`/home/administrator/Development/N1/.n1-bench/arc-allocator-gate-20260309-000737`

Summary:

- `status=PASS`
- `steps_pass=5`
- `steps_fail=0`
- `wall_seconds:avg=272.161 max=1317.336`

Step results:

| Step | Exit | Wall (s) | Passed |
|---|---:|---:|---:|
| `block_manager_tests` | 0 | 1.034 | 1 |
| `arc_formatter_nonce_roundtrip` | 0 | 0.267 | 1 |
| `arc_extent_gen_legacy_default` | 0 | 0.222 | 1 |
| `arc_release_drills` | 0 | 41.946 | 1 |
| `arc_ha_chaos` | 0 | 1317.336 | 1 |

### Release-candidate profile details (from latest full run)

`arc_release_drills` sub-results:

- workflow runs: `18/18 PASS`
- owner-first multipart soak: `PASS`
- per-case wall: `avg=2.221s max=4.804s`

`arc_ha_chaos` sub-results:

- chaos runs: `3/3 PASS`
- wall: `min=410.582s avg=439.105s p95=465.429s max=465.429s`

## Stable Closure Status

- Software gate pack (quick + full harness): `PASS`
- Deterministic replay/fail-stop checks in `block_manager` tests: `PASS` (covered by `block_manager_tests`)
- Arc integration drill path: `PASS` (covered by `arc_release_drills`)
- HA chaos path: `PASS` (covered by `arc_ha_chaos`)
- Multi-run release-candidate command (`RELEASE_RUNS=3 CHAOS_RUNS=3 INDEX_SOAK=1`): `PASS` in this environment
- Operator sign-off on target production hardware topology: `PENDING`

## Release-Candidate Command (Required Before Arc Stable Tag)

```bash
MODE=full \
RELEASE_RUNS=3 \
CHAOS_RUNS=3 \
INDEX_SOAK=1 \
scripts/bench/run_arc_allocator_gate.sh
```

Arc allocator should not be marked stable until the command above passes on target hardware and the produced `.n1-bench/arc-allocator-gate-*` artifacts are attached to release notes.
