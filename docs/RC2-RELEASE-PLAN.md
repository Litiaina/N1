# RC.2 qualification plan

The planned full qualification start was around November 20, 2026. The operator
opened the four-HDD window early on **November 17, 2026**, in Asia/Manila time;
the [recorded qualification](FOUR-DRIVE-QUALIFICATION-20261117.md) continues the
selected checkpoint and retains earlier populations. Release
RC.2 when the required gates pass. The target is a go/no-go review on
**November 25, 2026** and publication on **November 27, 2026**, subject to
qualification and operator approval.

The stable **1.0.0** target is **February 14, 2027**, conditional on successful
qualification and reliability/performance evidence from real deployments during
the intervening release-candidate period. Neither target authorizes publication.

The intended combination is **N1 `1.0.0-RC.2` + M1 `1.0.0-RC.1`**, recorded in
[`deploy/system-release.json`](../deploy/system-release.json), for complete
system **`1.0.0-RC.2`**. Each component has its own release history and tags;
follow [the versioning policy](VERSIONING.md) when freezing artifact identities.

The November 24 four-drive rerun and
[physical publication recovery](FOUR-DRIVE-QUALIFICATION-20261117.md#physical-publication-recovery-on-31)
passed on the selected pair. The
[API and operator qualification](RC2-API-QUALIFICATION-20261124.md) records
the contract and Console checks on the same build. The
[completion checklist](RC2-IMPLEMENTATION.md) tracks the remaining gates.
Coordinate the remaining physical power-loss checks **after** the other
qualification checks, as the final operator-controlled stage. Prepare with
[the four-drive checklist](RC2-FOUR-HDD-CHECKLIST-20261116.md); no automated power
cut or completed result is implied. Final-candidate reconciliation remains
necessary if runtime changes follow a campaign.

Use the selected artifact identities in the
[N1 publication qualification](N1-MIXED-WORKLOAD-20261116.md#publication-follow-up-on-both-minimum-storage-profiles)
and [M1 concurrent-maintenance report](M1-MAINTENANCE-20261116.md). Hold N1 fixed
while measuring M1. Attribute costs to the client, M1 and N1 separately.
Preserve completed populations, ACK journals, fault checks and adverse
measurements; do not restart the million-object campaign routinely.

| Target window | Work | Evidence needed to close the stage |
| --- | --- | --- |
| Before November 20 | Preserve the selected N1 checkpoint and prepare M1/system checks | Keep established N1 paths fixed; retain existing populations and matched performance results; add only missing qualification coverage and correct demonstrated failures |
| From around November 20 | Full-stack resource, maintenance and operator qualification | Mixed traffic with monitoring, GC, backup and migration; isolated disk-full, memory-pressure, stalled-I/O and connection failures; all acknowledged data retained and incomplete publications refused |
| Operator hardware window | Four-HDD recovery and physical power loss | Complete hardware/independent-ACK-recorder preflight; record actual four-HDD and power-cut outcomes; qualify affected changes against retained ACKs |
| After required checks pass | Candidate freeze, contract closure and go/no-go review | Exact source/build/binary identities; required NOA v1/v2 and Console matrix; affected scale evidence reconciled; final regression; unresolved required failures mean no-go |
| After go decision | Packaging and RC.2 preparation | Clean installation/startup/configuration, binary versions, checksums, documentation and release notes match the frozen candidate; publish only on operator instruction |
| RC period through February 14, 2027 target | Real-deployment validation before stable 1.0.0 | Retain deployment failures, recovery and performance evidence; resolve release-blocking defects and qualify the final stable candidate before publication |

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
