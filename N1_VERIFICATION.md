# N1 Verification Report

This document tracks release proof and hardening evidence. It is intentionally separate from the user guide.

## Required Release Gates

Before an LTS release, N1 should pass:

- `RUSTFLAGS='-D warnings' cargo test -p n1 --all-targets`
- `RUSTFLAGS='-D warnings' cargo check --workspace --all-targets`
- large multipart upload with restart/resume
- download hash match
- range/view streaming
- CRUD and POSIX namespace operations
- trash/recover/hard-delete/GC lifecycle
- recover-on-read after one disk is wiped
- repair after simulated disk loss or reformat
- redistribution `2 roots -> 1 root -> 2 roots`
- high object-count upload storm
- concurrent users on one fragment
- concurrent users across multiple fragments
- memory/RSS observation under sustained load
- UI-backed smoke against the real N1 API
- native `n1://` auth/list/stat/CRUD smoke and transport benchmark

## Rust Release Proof Runner

The release proof runner lives under `tools/n1-release-proof`.

Shell may prepare the environment and start the Rust binary, but Python is not part of the N1 release verification path.

Example:

```bash
SOURCE_FILE="/home/administrator/Downloads/INCDT 122225.mp4" \
DISK_ROOTS="/mnt/disk-00,/mnt/disk-01" \
scripts/bench/run_n1_release_proof.sh
```

For a large multipart proof, use a source file that exceeds the configured one-shot size. Large files must exercise multipart, persisted receipts, restart/resume, finalize, and download verification.

## Focused Proof Evidence

Latest native protocol benchmark for N1 1.2.0:

- Command path: Rust release proof `--native-transport-benchmark`.
- Build mode: release.
- Setup: one N1 server, 64 real namespace objects, same fragment for HTTPS and native.
- Iterations: 1,000 measured requests after 100 warmups per operation.
- Native listener: `n1://127.0.0.1:<native_port>`.
- HTTPS listener: `https://127.0.0.1:<https_port>`.
- Result: `PASS`.

Measured loopback results after enabling `TCP_NODELAY`:

| Operation | HTTPS ops/sec | Native ops/sec | HTTPS p50 | Native p50 |
| --- | ---: | ---: | ---: | ---: |
| ping | 7,566 | 20,093 | 111 us | 47 us |
| posix stat | 3,588 | 6,088 typed / 6,807 api bridge | 237 us | 145 us typed / 134 us api bridge |
| posix list, 64 entries | 653 | 536 typed / 725 api bridge | 1,347 us | 1,442 us typed / 1,389 us api bridge |

Interpretation: native `n1://` is materially faster for small control-plane and metadata calls. Listing is less transport-bound because direct namespace work and response body size dominate. The benchmark also caught a real protocol weakness before release: without `TCP_NODELAY`, tiny native frames hit delayed-ACK/Nagle latency around 82 ms per operation.

Latest local proof run recorded in the previous documentation set:

- `cargo test -p n1 --all-targets` passed with `RUSTFLAGS=-D warnings`.
- Real proof on `/mnt/disk-00` and `/mnt/disk-01` passed CRUD, POSIX, archive, multipart, recover-on-read, simulated disk-format repair, and redistribution `2 roots -> 1 root -> 2 roots`.
- The proof ended with `live_ndx=904`, `namespace_symlinks=904`, and `blob_version_dirs=904` on both roots after re-expansion.
- Server RSS stayed around `52 MiB` during that focused proof.
- Four-user storm passed on the same two disks: `12,000` one-shot objects plus `4` multipart objects of `16 MiB` each.
- POSIX listing returned `12,004` objects exactly after the storm.
- Storm memory remained bounded: max server RSS was about `258 MiB` with `42` threads.
- Storm throughput on the bad-disk tiny-object workload was `14.343 objects/s` and `0.203 MiB/s`, matching the expected IO/fsync-heavy small-object tradeoff.

## Extended LTS Proof Matrix

The full LTS proof should include:

- 24-hour soak on `/mnt/disk-00` and `/mnt/disk-01`
- multiple 20 GB+ multipart uploads
- restart during active multipart upload, then resume/finalize
- download hash match for every large object
- range/view streaming checks
- disk wipe, disk removal/re-add, repair, and health refresh
- redistribution after disk topology changes
- GC after hard delete and orphaned upload cancellation
- trash/recover with path-based namespace truth and no second trash metadata plane
- 10k+ object namespace storm
- four concurrent users on one fragment doing upload, mkdir, rename, move, trash, recover, and listing
- four concurrent users across four fragments
- UI-backed smoke for mount, quota, list, upload, download, trash, recover, repair/status, and health display

## Known Tradeoffs

N1 is optimized for durable large-object storage and repairability. Tiny-object storms are expected to be dominated by namespace commits, receipt writes, and shard/parity flush behavior. Batching or archiving high-volume tiny-file datasets before upload is more efficient.
