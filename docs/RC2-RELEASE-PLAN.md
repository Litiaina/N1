# RC.2 qualification plan

Recommended release target: **November 27, 2026**, with a **November 25 go/no-go
review**, in Asia/Manila time. These are planning targets, not authorization to
release or permission to waive a failed acceptance condition. Move the release
date if a required gate remains open.

The starting source checkpoint is `9f0b922`. N1's measured native04 executable
and its runtime/build manifests are retained in
[the home optimization report](HOME-OPTIMIZATION-20261113.md). Hold that N1
runtime fixed while measuring M1. Attribute costs to the client, M1 and N1
separately. Preserve all completed populations, ACK journals, fault checks and
adverse measurements; do not restart the million-object campaign routinely.

| Target window | Work | Evidence needed to close the stage |
| --- | --- | --- |
| November 13–17 | M1 baseline, profiling and necessary optimization | Matched standalone and named paths on the same N1/pool; small and large objects; normal/durable, multipart geometry, init/status/finalize; per-process CPU/RSS/I/O and individual latency samples; complete payload/version verification and affected regressions |
| November 18–20 | Full-stack resource and maintenance qualification | Bounded saturation with monitoring, GC, backup and migration; isolated disk-full, memory-pressure, stalled-I/O and connection failures; all acknowledged data retained and incomplete publications refused |
| November 21–23 | Final-candidate MCH storage and power-loss qualification | Four independent HDDs, 3+1 and 2+2, affected critical-transition faults, degraded read/repair and replacement; separately authorized physical power interruption and verification against an externally retained ACK journal |
| November 24 | Candidate freeze and contract/Console closure | Exact source/build/binary identities; complete required NOA v1/v2 and operator workflow matrix; affected scale coverage reconciled with existing evidence; final regression |
| November 25 | Go/no-go review | Every required row in the completion checklist has a result, evidence and an explicit disposition; unresolved data-integrity, durability or accepted-performance failures mean no-go |
| November 26–27 | Packaging and release preparation | Clean installation/startup/configuration, binary versions and checksums, documentation and release notes checked against the frozen candidate; publish only on operator instruction |

Start with the available home DATA_SSD and two independent HDDs. MCH and
physical power interruption require the corresponding machine/operator window.
Do not power-cycle shared services implicitly. A process kill, clean reboot or
device disconnect must retain its actual fault classification.

Performance comparisons must preserve EC layout, durability, compiler/profile,
concurrency, source bytes and cache conditions. Small-object p99 needs sufficient
individual samples; a few large objects provide throughput observations, not a
credible object-level p99. A faster average does not discard adverse tails.

The authoritative acceptance conditions remain in
[RC2-IMPLEMENTATION.md](RC2-IMPLEMENTATION.md). This schedule does not close a
gate, promise unsupported topology/scale or declare RC.2 ready.
