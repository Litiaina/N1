# N2 2.0 Verification Record

## Claim Boundary

This file records bounded architectural evidence for the in-development N2
2.0 engine. It is not a long-soak, LTS, or production-readiness declaration.
Failures and unimplemented release gates remain visible rather than being
converted into a broader stability claim.

Independent-host proof: 2026-07-17 (Asia/Manila)  
Final local invariant suite: 2026-07-18 (Asia/Manila)

## Independent-Host Topology

```text
namespace pool
  ns-a  local host, USB SATA SSD root
  ns-b  local host, NVMe root

blob pool
  blob-a  independent LAN host, two isolated XFS HDD directories
  blob-b  independent LAN host, two isolated XFS HDD directories

namespace redundancy  2
object EC              fixed 1 + 1
```

The remote host's existing production services and data directories were not
used or modified. N2 used only directories whose names contained the dedicated
`N2-2.0-proof-20260717` test identity. The proof processes were temporary and
were not installed as services.

## Package Evidence

After the inode-parent, dentry-compaction, GC-order, redundant-lock removal,
and exact-layout auto-healing patches:

```text
cargo test -p n2 --lib
125 passed; 0 failed; 0 ignored
test execution        3.89 s
compile + execution  43.19 s
aggregate CPU          210%
user CPU              84.28 s
system CPU             6.65 s
peak RSS           2,055,556 KiB
```

Compiler and test-harness resource measurements are not server-runtime
measurements.

Focused inode-store coverage includes:

- invisible prepared state;
- one winner for a conflicting generation claim;
- idempotent committed generation validation;
- same-parent rename as one inode generation;
- obsolete changed-dentry generation compaction;
- abort followed by restart-safe visibility;
- cross-inode decision visibility barrier;
- moved-directory parent mutation in the certified transaction.

Exact-layout healing coverage includes:

- complete inode snapshot restore into a missing recorded replica;
- atomic replacement of a corrupt recorded inode replica;
- refusal to overwrite a newer valid inode generation;
- refusal to replace an inode with a pending immutable generation claim;
- recovery of an R2 peer from one surviving complete snapshot carrying its
  original two-voter commit certificate;
- preference for a newer certified generation over an older identical quorum;
- refusal to guess between same-generation divergent snapshots;
- immutable namespace-redundancy enforcement at enrolled roots;
- filesystem-safe local report filenames.

## Real API And Failure Evidence

The first isolated 2+2 run proved:

- fragment provisioning through the namespace catalog;
- namespace roots contained namespace state and no blob object plane;
- blob roots contained object state and no namespace inode plane;
- nested `mkdir`, list, same-parent rename, and cross-parent directory move;
- 8 MiB one-shot upload through one blob coordinator;
- exact data and parity node/root locators in immutable metadata;
- byte-identical download through a namespace coordinator;
- independent object and containing-directory trash anchors followed by both
  restores, preserving the original object/version;
- both namespace nodes unavailable: mutation failed closed with HTTP 503 and
  committed no forbidden path;
- one blob node unavailable: the existing object reconstructed from parity,
  while a new fixed-width write failed closed with HTTP 503;
- removed data shard: recover-on-read returned identical bytes and recreated
  the shard;
- removed parity shard: one explicit auto-healing cycle scanned one assigned
  object and repaired exactly one shard;
- TTL lifecycle: immediate GC retained the object, expired GC reclaimed the
  object layout, and the trash anchor disappeared last;
- no pending inode claims or cross-inode decisions after normal completion.

Measured samples from that run:

```text
fragment create                         1.081 s
nested mkdir                          262.6 ms
same-parent directory rename          117.6 ms
cross-parent directory move           230.9 ms
root list                              47.8 ms
8 MiB upload                          0.900 s   9.32 MB/s
8 MiB download                        0.806 s  10.41 MB/s
namespace-quorum fail-closed            10.6 ms
blob-width write fail-closed             8.7 ms
parity reconstruction read             2.44 s
recover-on-read                         2.18 s
20-rename p50                          85.0 ms
20-rename p95                          91.3 ms
```

The 20 rename operations consumed approximately 0.15 CPU-second across both
namespace replicas from `/proc` scheduler ticks, or about 7.5 ms of aggregate
server CPU per logical rename. Hardware cycle counters were unavailable because
the host's `perf_event_paranoid` policy was 4; the test did not change that
system policy.

## Final Optimized-Binary Regression

Final release SHA-256:

```text
b4f11dd949f6071943cc22ef725cc69d3570869b3de39cb611851d1a2994f851
```

The final persisted-state restart and regression added these results:

```text
four nodes ready                         4 / 4
persisted moved-directory stat          12.5 ms, HTTP 200
persisted 8 MiB download                 2.11 s, SHA-256 identical
one namespace replica down mutation      46.8 ms, HTTP 503
forbidden path after replica restart     49.4 ms, HTTP 404
pending claims after restart             0 on both replicas
pending decisions after restart          0 on both replicas
immediate GC                             HTTP 202, 2 metadata + 2 shard files retained
expired GC                               one version, 8,388,608-byte refund, zero errors
post-expiry trash entries                0
post-expiry object metadata/shards       0 / 0
```

Twenty alternating same-parent renames through alternating local and remote
coordinators all returned HTTP 200. Nineteen completed between 63.4 and 278.6
ms; one outlier was 1.109 s. Each namespace replica retained only the two
current dentry records at generation 21, with no historical growth and no
pending claims or decisions.

The 8 MiB upload and first read on the final topology measured 6.53 MB/s and
4.61 MB/s respectively over the real LAN, and their SHA-256 values matched.
Filesystem inspection showed zero object directories on namespace roots, zero
namespace directories on blob roots, two byte-identical `.ncdx` records, one
data shard, and one parity shard.

Two defects were found by the independent-host rerun and fixed before the final
binary:

- distributed subtree walkers reused a human alias hash after resolving a
  stable inode root, causing background reads against a non-existent physical
  alias path; traversal now normalizes once to the catalog's stable fragment
  ID;
- lifecycle GC still performed a global live-namespace reference scan even
  though N2 2.0 excludes hard links; GC now scans only the selected trash entry
  and, for a trashed directory, that directory's subtree.

No ERROR log lines appeared on either blob process after the final restart, and
the foreground namespace processes emitted no errors during the regression.
End-of-proof process samples were:

```text
ns-a    RSS 26,880 KiB  11 threads  19 FDs  0.2% sampled CPU
ns-b    RSS 25,828 KiB  11 threads  20 FDs  0.2% sampled CPU
blob-a  RSS 46,872 KiB  15 threads  19 FDs  0.0% sampled CPU
blob-b  RSS 26,424 KiB  15 threads  20 FDs  0.0% sampled CPU
```

Final static/build gates also passed:

```text
cargo fmt --all -- --check       passed
git diff --check                 passed
cargo check -p n2 --all-targets  passed in 12.17 s
```

After evidence collection, all four temporary N2 processes were stopped. The
remote binary, configs, runtime files, and four HDD test directories were
removed. Ports 52105 through 52108 had zero listeners and no named proof paths
remained. Both pre-existing hospital N1 production processes were still
running with their original process IDs.

## Locked Invariants Supported By This Evidence

- namespace and blob roles are separate;
- directory identity is stable and path names live in parent dentries;
- path mutation does not rewrite descendant or object data;
- object IO uses only that object's recorded EC set;
- inode and object layouts record exact node and root identities;
- namespace publication follows durable object publication;
- conflicting inode generations use immutable atomic claims, not a global
  writer lock;
- strict unsafe topology fails closed;
- recover-on-read and explicit object healing reconstruct recorded layouts;
- trash anchors are removed after physical reclamation.

## Remaining Release Gates

N2 2.0 must not yet be called generally stable. The following architecture
work still requires implementation or independent-host proof:

- independent-host failure injection for autonomous inode, `.ncdx`, and shard
  repair plus durable cluster safety read-only propagation;
- permanent namespace-replica replacement after explicit fencing;
- verified cluster-wide safety-latch proof-and-clear procedure;
- bounded restart continuation or lease renewal for namespace walks whose
  duration exceeds one healing lease;
- restart replay of a cross-inode decision interrupted between group commit and
  finalization;
- three-replica namespace majority continuation and repair;
- capacity-aware namespace-root admission;
- one-at-a-time namespace/blob node and disk expansion without remapping old
  layouts;
- safe drain and permanent holder fencing;
- immutable new-layout publication for permanent object-holder replacement;
- deterministic inverse-move/cycle and crash-point network tests;
- N2 Console 2.0 topology and independent plane reporting;
- a later bounded soak and resource-growth proof.

These gates are release work, not permission to weaken the locked invariants in
`ARCHITECTURE.md`.
