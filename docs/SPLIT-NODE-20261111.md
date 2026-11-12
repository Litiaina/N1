# Home split-node qualification — November 11, 2026

**Reliability preflight and original million-object campaign passed;
RC.2 remains unreleased.** Population, full post-restart readback
and both full member-loss readbacks are complete. The continued member-1 phase
passed all 1,000,000 original objects with zero payload/version mismatches;
normal expiry cleanup and final censuses subsequently passed on November 12.
See the [final closure](HOME-QUALIFICATION-20261113.md) and
[continuation report](MCH-CONTINUATION-20261112.md) for the current placement,
preserved identities and scope correction. No replacement population is running.

## Placement

| Component | Host | Private fixture storage |
| --- | --- | --- |
| N1 object engine | Workstation `192.168.254.12` | Pool A, 1+1, `/mnt/HDD1TB001/n1-split-node-20261111-01/smoke4096/member-0` and the corresponding `member-1` under `/mnt/HDD1TB002` |
| M1 metadata engine | Main PC `192.168.254.107` | Database, staging, snapshots, build output, executables and client oracles on `/mnt/DATA_SSD` |
| Native storage hop | M1 → N1 | Verified `n1s://192.168.254.12:50002`, private CA and separate server certificate with the N1 IP SAN |
| Public workload hop | Rust client on main PC → M1 | Loopback HTTP; M1 also listens on `0.0.0.0` for this private test |

Both N1 members are independent XFS filesystems. The workstation uses Wi-Fi;
this is a separate performance profile from MCH's four SATA HDDs. No cold-cache
or hardware power-loss claim follows. No filesystem was formatted and no
pre-existing dataset was removed. Credentials and key files remain owner-only.
N1 selects `io_uring`; the existing aligned direct-I/O implementation and
SHA-256/CRC32C integrity policy remain unchanged.

Local evidence root: `/mnt/HDD1TB001/n1-split-node-20261111-01`.
Main-PC evidence root: `/mnt/DATA_SSD/n1-split-node-20261111-01`.
Builds use the existing DATA_SSD target directory
`/mnt/DATA_SSD/m1-owned-storage-20261110-01/target`, Rust 1.99.0 and locked,
offline dependencies. The resulting N1 executable runs on the workstation's
compatible glibc. No build or population dataset uses the source NVMe.

## Completed checks

- Current N1, M1 and the Rust network client build successfully. Both network
  examples pass scoped strict Clippy.
- **4,096 distinct named objects durably acknowledged**, four writers,
  256-object request batches, deterministic eight-size payloads from zero to
  65,536 bytes. Population wall time: **52.402 seconds**. ACK journals contain
  the original version ULIDs and are fsynced independently of both servers.
- **Actual SIGKILL of both test services**, restart of the same N1/M1 data,
  and every original payload/version checked through M1. No acknowledged
  object is reuploaded by verification.
- Standard stripe-aligned and durable unaligned multipart sessions survive the
  process kills with their original parts `[0,2,5]` acknowledged. Cold status
  performs zero N1 payload submissions. Only missing parts are sent; repeated
  finalize returns the same publication and exact bytes/version are checked.
- Identical part retries succeed; conflicting part bytes return 409. Explicit
  cancellation of the earlier incomplete fixtures returns 200, then status 410.
- Ordinary monitoring performs zero N1 payload submissions in the isolated run.
- **Each of the two members made unavailable separately:** all 4,096 original
  bytes/version pairs pass through M1 using the remaining member. Restoring the
  original directories returns the complete pool. No replacement root is
  silently created and no physical cable/power fault is claimed.
- **Metadata snapshot/loss/restore:** an operator snapshot is downloaded and
  the old database and staging are moved aside. Restoring into a new directory
  yields **29,010 verified records**; all 4,096 original payloads/versions pass.
  Every N1 shard hash remains identical. An ordinary fragment token is denied
  operator snapshot access.
- **Fresh physical-census preflight:** 256 objects, a further actual SIGKILL of
  both services, complete readback, exact offline metadata/version/identity
  counts and **512 correctly placed physical shards**, no staging or
  unreferenced shards. An injected unexpected terminal file is rejected by the
  census and removed before the large campaign. LINK signing secrets remain on
  N1; the census transfers only canonical identity bytes, without access tags.
- **Fresh workspace regression on DATA_SSD:** **378 passed, zero failed**, 56
  explicit opt-in/helper exclusions across 48 targets. Strict workspace-wide
  all-target Clippy and formatting pass. M1/storage documentation checks pass
  with zero runnable examples. These are separate from the physical witnesses.

## Controlled performance screen

Eight phases, **ABBA BAAB**, compare committed native M1 `29bca4b` against the
retained-export candidate. Both use the same database, configuration, client,
and continuously running N1 process. Each phase uploads 100 distinct 4 KiB
objects with strict durability, verifies bytes/version, renames and trashes each
object. Four phases per variant; raw completion-order samples are retained.
No compilation or regression workload ran during the measured screen.

| Mean per 100 objects | Committed owned M1 | Export candidate |
| --- | ---: | ---: |
| Sum of upload latencies | 36.686 s | 41.662 s |
| Sum of GET latencies | 0.366 s | 0.390 s |
| Sum of rename latencies | 0.329 s | 0.333 s |
| Sum of trash latencies | 0.364 s | 0.364 s |
| Workload M1 CPU, user + system | 0.893 s | 0.880 s |
| Highest observed M1 peak RSS | 32,316 KiB | 32,576 KiB |

**Latency acceptance remains open:** the candidate's mean upload sum is 13.6%
higher. Baseline phase sums range from 28.233 to 52.687 seconds, while the
candidate ranges from 36.701 to 46.515 seconds. Four samples per variant with
this spread do not establish the cause of the difference. M1 CPU is nearly
equal and the metadata-only operations stay near 3–4 ms per operation.

The post-screen N1 counters show foreground-write execution p95 **1,159 ms**,
p99 **1,521 ms**, versus queue p95 **17 µs**. Group filesystem-sync p99 is
**828 / 782 ms** on the two filesystems. The system remains healthy, with no
failed I/O or stall events in these counters. This locates the observed tails
after admission; it does not prove the export feature caused them or close the
performance gate. This is a split-node export-feature comparison over fixed
LINK, not an NCX-versus-LINK or Procyon-versus-owned-M1 comparison.

`reports/performance-[0-7].json` on DATA_SSD holds complete distributions and
M1 CPU/RSS snapshots. `performance-summary.json` is copied to the local report
root. Local per-phase files retain N1 process counters. The first performance
controller stopped after phase 0 because an empty SSH argument was omitted;
its failed marker is preserved. The corrected controller retained that phase,
verified the same N1 PID, and completed the remaining phases on the same
database; it did not replace measurements.

## Headless million-object campaign

The large campaign is **continuing its unfinished checks**, not yet passed as
a whole. It originally populated private `million/` directories on both nodes,
separate from the preflights. The frozen Rust
population/readback client runs four writers in 256-object batches and eight
verification readers. Its client journals stay on DATA_SSD. The controller is
detached on the N1 workstation, and the M1/client processes have independent
sessions on the main PC. Closing the IDE or SSH client does not terminate them.
Both hosts and the home network must remain available.

At the November 12 stop, population had acknowledged all **1,000,000 objects**:
500,224 before the midpoint crash and 499,776 in the resumed invocation. The
second both-service SIGKILL/restart was followed by complete healthy readback:
**1,000,000 objects / 13,968,125,000 bytes**, zero payload/version mismatches,
4,851.137 seconds. The full readback with member 0 unavailable also passed:
**1,000,000 objects**, zero payload/version mismatches, 11,772.781 seconds.
Member-1-loss readback last reported **720,000** successes; its client was then
intentionally terminated (exit 143). On November 12, the interrupted phase
continued from zero against the original ACKs and **passed all 1,000,000 objects /
13,968,125,000 bytes**, zero payload/version mismatches, **11,757.437 seconds**.
The partial counter was not used to skip objects. Its verifier exited 0 and
checked unchanged ACK hashes before and after the pass. Final natural-expiry GC
and censuses subsequently passed with all original identities unchanged.

The home processes were stopped for travel. Results, source, executables, original
ACKs and a compressed M1 database checkpoint are preserved at
`/home/altear/DEV_TEST/n1-m1-rc2-preserved-20261112.PLPlQD`.
The database archive's individual file hashes match the stopped main-PC
database. Original N1 shard files remain on their HDDs. Both drives are now
mounted with their original UUIDs. The continuation serves those same shards
from the workstation, with the verified M1 checkpoint on MCH. Member 1 was
detached only for its unfinished readback and is now restored. Both task-owned
services are stopped after final cleanup. No reformat,
new enrollment or replacement population is performed.

The original campaign's sequence and current disposition are:

1. Populate one million named objects using owned M1 over native-TLS N1: complete.
2. After at least 500,000 acknowledged objects, SIGKILL both services, restart
   their original data and resume using the original ACK journals. Uncertain
   batch entries are read/reconciled before sending missing entries: complete.
3. After population, SIGKILL/restart both again and verify every original
   payload/version without reuploading it: complete.
4. Make each N1 member unavailable in turn and verify every original again,
   then return the original member directories: both full million-object passes
   complete, with the original member directories restored.
5. Stop the services for independent metadata/ACK and physical-placement
   censuses, require exactly two shards per referenced version with zero
   staging/unreferenced files, and run the offline M1 integrity check: complete.
   Normal automatic GC reclaimed 192 unacknowledged crash-era intents after
   their actual expiry, with zero failures/deferred work. Final metadata counts
   are one million names, immutable versions and unique LINK references, zero
   pending publications. The physical census found exactly two million shards
   on two filesystems, no staged/unreferenced/misplaced shards. The final database
   check verified 7,000,272 records. No population or full readback was repeated
   for this closure; original ACK journals remain byte-identical.

`reports/MILLION-STATUS`, `MILLION-LAST-ACK`, `MILLION-CONTROLLER.pid`,
`million-events.log` and `million-controller.log` are on the local report root.
`MILLION-FAILED` or `MILLION-PASSED` is created only when the corresponding
condition occurs. Raw Rust client logs, journal files and M1 resource samples
remain on DATA_SSD; N1 resource/disk counters remain on HDD1TB001. The controller
also checks available DATA_SSD space and stops on failure. It never starts a
replacement workload to overwrite a failed oracle.

This campaign covers **named API → owned M1 → N1**. The completed results remain
valid within that scope. The operator stopped extra populations started on
November 12; no standalone or Console million is running or queued. Broader
release gates remain tracked separately in the RC.2 implementation list.

## Provenance and failed attempts

| Frozen artifact | SHA-256 |
| --- | --- |
| N1 | `467ee7ff5482b4407286c399607eee16e745efc73c8ab59ed090a88c0a4a1d50` |
| M1 | `668cda1cad300d06e8e4fca87a613bed9af31c55e16e858575bf0951eb65bffe` |
| Population/readback client | `ff6d0dd1f7a5a8af7499406dd19db7b6baab77054612f97a7f7a246f5a499ffd` |
| Operation client | `35e3a5d7cf80a492ea599dfc57fc678b461da09e87c43f9b350bdfb5a0d57d17` |
| Offline census client | `9fe2eeb5d2315ec5464072a838d188fc9af951342b550459259205702dfa5551` |
| Committed owned-M1 baseline | `2cda6fda20ff37e746921c12a5ca6a30b83208fd2bf7df0b1b2576094822e2e8` |

`source/workstation-source-final.tar.gz` freezes the initial current working
tree; `source/network-operations-final.tar.gz` contains the subsequent operator
client. These qualification additions do not change N1/M1 runtime source.
`network-inventory.tar.gz` preserves the census source. The final working tree
is additionally frozen as `source/home-source-checkpoint.tar.gz` on both nodes.
`source/committed-baseline.tar.gz` is committed `29bca4b` for the matched screen.
Binary/source manifests, logs and controller scripts are retained on the
selected drives. The original MCH executables have different hashes/toolchains
and are not substituted for these artifacts.

The first certificate fixture used a CA certificate as the server leaf: M1
refused its native connection. A separate leaf signed by the test CA resolves
the fixture failure with certificate verification enabled. The first launch
wrapper also assumed GNU `time` was installed; it was absent, so no workload
ran in that attempt. Bash's timing facility is used instead.

The first fault controller stopped before injection because of Bash quoting;
its already acknowledged multipart state was preserved. The first multipart
completion client omitted the version segment from the documented finalize
URL and received 405. This is a harness failure. Its logs, original executable
and state remain; incomplete sessions were explicitly cancelled. Fresh
sessions then passed with the corrected frozen client. Failed controller
markers are retained beside later successful runs, never relabelled as passes.

Workloads, comparisons and verification use Rust; process orchestration uses
Bash. No Python, commit, push, tag or release is performed.
