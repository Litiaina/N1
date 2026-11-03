<div align="center">

  <img
    src="./assets/litiaina_icon.png"
    alt="Litiaina"
    width="150"
    height="150"
  />

  <h1>Litiaina N1</h1>

  <p>
    <b>N1 is a high-performance single-node object storage system built on LINK (Litiaina Immutable Native Key), with an independent object engine and an optional metadata engine for named-object management.</b>
  </p>

</div>

<p align="center">
  <strong>Development target: N1 1.0.0-RC.2 — unreleased</strong>
</p>

## Overview

**LINK — Litiaina Immutable Native Key** is N1's native object-addressing and
shard-placement architecture. The object engine calculates shard locations from
a client-held key and an immutable storage-pool layout. It does not use NCX/NDX
or a namespace database to locate object bytes.

N1 has two main components:

- **`object_engine/`** stores and verifies immutable objects. It handles normal,
  batch and multipart uploads, durable finalization, GET/ranges, object health,
  repair, permanent key retirement, staging GC and object recovery.
- **`metadata_engine/`** provides logical fragments, names, versions, listings,
  named upload transactions, rename, trash/recovery and exports. Its database
  foundation is ProcyonDB. It coordinates storage through the public native LINK
  interface, without importing object-engine internals.

The original Console connects to metadata's NOA v1 interface. Metadata forwards
object operations over native NOA v2. Applications that manage their own names
and LINK keys can use the object engine independently.

`Console / named client → metadata_engine → LINK object_engine → shards`

`Client with its own metadata → LINK object_engine → shards`

N1 retains adaptive concurrency, the disk scheduler, io_uring, streaming,
middleware, repair and backup infrastructure. Capacity expansion adds a complete
immutable pool; existing objects keep their original placement. Operators retain
INI/environment control of addresses, ports, TLS, concurrency and maintenance.

## Build and operate

Build with Rust on Linux. Keep build output and large datasets on storage with
adequate space, outside the source tree.

```sh
cargo build --locked --release -p n1 -p n1_metadata_engine -p n1_shell
litiaina-n1 --help
litiaina-n1-metadata --help
litiaina-n1-shell --help
```

Package `n1` builds the object engine; its directory and executable names are
independent. Each server has its own INI configuration and private credentials,
supplied through a selected secrets file or its service environment.
Follow the operator guide to enroll empty member roots, configure the two
services and connect Console to metadata.

## Documentation

| Read | Purpose |
| --- | --- |
| [Operator guide](DOCUMENTATION.md) | Configure, enroll disks, start services, tune and recover N1 |
| [Architecture](ARCHITECTURE.md) | LINK placement and the object/metadata responsibility boundary |
| [NOA standard](NOA.md) | Both the named metadata v1 contract and direct LINK v2 contract |
| [Testing](TESTING.md) | Reproduce tests and distinguish qualified results from running campaigns |
| [Changelog](CHANGELOG.md) | Product changes; older engine history is explicitly separated |

Detailed references: [LINK byte format](docs/LINK-FORMAT.md),
[metadata internals](docs/METADATA-ENGINE.md) and
[remaining completion gates](docs/RC2-IMPLEMENTATION.md).

**Development status:** RC.2 is unreleased. Implemented features, test evidence
and remaining API/product work are different things; the completion checklist
and testing guide identify them explicitly. Earlier development storage formats
are not a compatibility or migration requirement.

## License and reports

N1 is licensed under [Apache 2.0](LICENSE). See [CONTRIBUTING.md](CONTRIBUTING.md)
for the project's contribution and reporting policy.
