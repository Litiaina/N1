<div align="center">

  <img
    src="./assets/litiaina_icon.png"
    alt="Litiaina"
    width="150"
    height="150"
  />

  <h1>Litiaina N1</h1>

  <p>
    <b>N1 is a high-performance single-node object store built on LINK (Litiaina Immutable Native Key). M1 is its independent metadata store, backed by ProcyonDB, for named-object management.</b>
  </p>

</div>

<p align="center">
  <strong>Development target: N1 + M1 1.0.0-RC.2 — unreleased</strong>
</p>

## Overview

**LINK — Litiaina Immutable Native Key** is N1's native object-addressing and
shard-placement architecture. The object engine calculates shard locations from
a client-held key and an immutable storage-pool layout. It does not use NCX/NDX
or a namespace database to locate object bytes.

This repository contains two systems:

- **N1 — `object_engine/`** stores and verifies immutable objects. It handles normal,
  batch and multipart uploads, durable finalization, GET/ranges, object health,
  repair, zero-residue purge, staging GC and object recovery.
- **M1 — `metadata_engine/`** provides logical fragments, names, versions, listings,
  named upload transactions, rename, trash/recovery and exports. Its database
  foundation is ProcyonDB. It coordinates storage through the public native LINK
  interface, without importing object-engine internals.

**LINK** is N1's key algorithm and storage architecture. **ProcyonDB** is the
transactional database used inside M1; it is not another object store.

`main` contains the mainstream **N1 LINK + M1** system. The former NCX/NDX
engine is a separate product, **N1 Lite**, in the
[N1-Lite repository](https://github.com/Litiaina/N1-Lite). It has its own engine,
builds, binaries, configuration and operator documentation.

The original Console connects to M1's NOA v1 interface. M1 forwards object
operations to N1 over native NOA v2. Applications that manage their own names
and LINK keys can use N1 independently, without M1 or ProcyonDB.

`Console / named client → M1 → N1 (LINK) → shards`

`Client with its own metadata → N1 (LINK) → shards`

N1 retains adaptive concurrency, the disk scheduler, io_uring, streaming,
middleware, repair and backup infrastructure. Capacity expansion adds a complete
immutable pool; existing objects keep their original placement. Operators retain
INI/environment control of addresses, ports, TLS, concurrency and maintenance.

## Build and operate

Build with Rust on Linux. Keep build output and large datasets on storage with
adequate space, outside the source tree.

```sh
cargo build --locked --release -p n1 -p m1 -p n1_shell
litiaina-n1 --help
litiaina-m1 --help
litiaina-n1-shell --help
```

Package `n1` builds `litiaina-n1`; package `m1` builds `litiaina-m1`. The source
directories remain `object_engine/` and `metadata_engine/`. Each server has its
own INI configuration and private credentials,
supplied through a selected secrets file or its service environment.
Follow the operator guide to enroll empty member roots, configure the two
services and connect Console to metadata.

## Documentation

| Read | Purpose |
| --- | --- |
| [Operator guide](DOCUMENTATION.md) | Configure, start, tune and recover N1 and M1; enroll N1 pools |
| [Architecture](ARCHITECTURE.md) | System names, repository structure, LINK placement and N1/M1 boundaries |
| [NOA standard](NOA.md) | Both the named metadata v1 contract and direct LINK v2 contract |
| [M1 API reference](docs/api/M1.md) | Named requests, responses, curl examples, multipart resume, maintenance and backups |
| [N1 API reference](docs/api/N1.md) | Native LINK requests, responses, executable Rust walkthrough and secondary HTTP examples |
| [Testing](TESTING.md) | Reproduce tests and distinguish qualified results from running campaigns |
| [Changelog](CHANGELOG.md) | Product changes; older engine history is explicitly separated |

Detailed references: [LINK byte format](docs/LINK-FORMAT.md),
[metadata internals](docs/M1.md) and
[remaining completion gates](docs/RC2-IMPLEMENTATION.md).

**Development status:** RC.2 is unreleased. Implemented features, test evidence
and remaining API/product work are different things; the completion checklist
and testing guide identify them explicitly. Earlier development storage formats
are not a compatibility or migration requirement.

## License and reports

N1 is licensed under [Apache 2.0](LICENSE). See [CONTRIBUTING.md](CONTRIBUTING.md)
for the project's contribution and reporting policy.
