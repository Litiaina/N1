# RC.2 qualification plan

Recommended release target: **November 27, 2026**, with a **November 25 go/no-go
review**, in Asia/Manila time. These are planning targets, not authorization to
release or permission to waive a failed acceptance condition. Move the release
date if a required gate remains open.

**Operator schedule — November 15:** four-HDD recovery qualification and
controlled physical power-loss testing are scheduled for **Monday, November 16, 2026**, Asia/Manila.
The start time and power-cut window will be established on site. Prepare with
[the Monday checklist](RC2-FOUR-HDD-CHECKLIST-20261116.md); no automated power cut or
completed result is implied. This advances that hardware window from the
original November 21–23 plan. Final-candidate reconciliation remains necessary
if runtime changes follow the Monday campaign.

The starting source checkpoint is `9f0b922`. N1's measured native04 executable
and its runtime/build manifests are retained in [the SSD/HDD optimization
report](SSD-HDD-OPTIMIZATION-20261113.md). Hold that N1 runtime fixed while
measuring M1. Attribute costs to the client, M1 and N1 separately. Preserve all
completed populations, ACK journals, fault checks and adverse measurements; do
not restart the million-object campaign routinely.

| Target window | Work | Evidence needed to close the stage |
| --- | --- | --- |
| November 13–17 | M1 baseline, profiling and necessary optimization | Matched standalone and named paths on the same N1/pool; small and large objects; normal/durable, multipart geometry, init/status/finalize; per-process CPU/RSS/I/O and individual latency samples; complete payload/version verification and affected regressions |
| Monday, November 16 | Four-HDD recovery and power-loss window | Complete the hardware/independent-ACK-recorder preflight, record actual four-HDD and power-cut outcomes using the Monday checklist, and retain unfinished gates explicitly |
| November 18–20 | Full-stack resource and maintenance qualification | Bounded saturation with monitoring, GC, backup and migration; isolated disk-full, memory-pressure, stalled-I/O and connection failures; all acknowledged data retained and incomplete publications refused |
| November 21–23 | Remaining hardware gates and final-candidate reconciliation | Preserve November 16 evidence; close unfinished four-HDD/power-loss cases and qualify affected runtime changes against the externally retained ACK journal |
| November 24 | Candidate freeze and contract/Console closure | Exact source/build/binary identities; complete required NOA v1/v2 and operator workflow matrix; affected scale coverage reconciled with existing evidence; final regression |
| November 25 | Go/no-go review | Every required row in the completion checklist has a result, evidence and an explicit disposition; unresolved data-integrity, durability or accepted-performance failures mean no-go |
| November 26–27 | Packaging and release preparation | Clean installation/startup/configuration, binary versions and checksums, documentation and release notes checked against the frozen candidate; publish only on operator instruction |

Use the available SSD and two-HDD test systems for preparation. Four-HDD
qualification and physical power interruption require the corresponding
operator window. Do not power-cycle shared services implicitly. A
process kill, clean reboot or device disconnect must retain its actual fault
classification.

Performance comparisons must preserve EC layout, durability, compiler/profile,
concurrency, source bytes and cache conditions. Small-object p99 needs sufficient
individual samples; a few large objects provide throughput observations, not a
credible object-level p99. A faster average does not discard adverse tails.

The authoritative acceptance conditions remain in
[RC2-IMPLEMENTATION.md](RC2-IMPLEMENTATION.md). This schedule does not close a
gate, promise unsupported topology/scale or declare RC.2 ready.
