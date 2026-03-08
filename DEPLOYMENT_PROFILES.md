# N1 Deployment Profiles

This document is the canonical deployment split for N1.

As of 2026-03-08:

- `standard` (`litiaina-n1-standard`) is the stable deployment profile and the default for production.
- `monilith` (`litiaina-n1-monilith`) is experimental and must be treated as a development track.

## 1) Profile Status

| Profile | Binary | Status | Production default |
|---|---|---|---|
| Standard | `litiaina-n1-standard` | Stable | Yes |
| Monilith | `litiaina-n1-monilith` | Experimental | No |

## 2) Architecture Split

### Standard (stable)

- Filesystem-backed authoritative `index` plane.
- Raw/direct-I/O blob plane.
- Partitioned metadata authority and derived prefix serving.
- Best operator compatibility and lowest deployment risk.

### Monilith (experimental)

- Raw/blob allocator-centric engine development track.
- Kernel-style storage-kernel direction.
- Active hardening/certification work still in progress.
- Not the default deployment target until release gates are closed.

## 3) Recommended Production Deployment

Build and run Standard:

```bash
cargo build --release -p standard
./target/release/litiaina-n1-standard server
```

Use Standard for:

- production nodes
- integration with N2
- baseline operational runbooks (repair, redistribution, GC, SMB-lite)

## 4) Experimental Monilith Track Rules

Monilith is allowed for:

- allocator/kernel-track development
- isolated benchmarking
- non-production validation

Monilith is not the default for:

- production rollout
- operator runbooks
- N2 default node-core expectations

## 5) N2 Alignment Policy

Until monilith passes full hardening and stability gates:

- N2 documentation and deployment guidance must assume N1 Standard semantics as default.
- Monilith references must be explicitly labeled experimental.

