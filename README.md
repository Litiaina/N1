<div align="center">

  <img
    src="./assets/litiaina_icon.png"
    alt="Litiaina"
    width="150"
    height="150"
  />

  <h1>Litiaina N1</h1>

  <p>
    <b>N1 is a high-performance single-node object storage system built for durable, redundant, key-addressed, and efficient storage.</b>
  </p>

</div>

<p align="center">
  <strong>Development target: N1 1.0.0-RC.2 — unreleased</strong>
</p>

## Overview

N1 is being redesigned as a pure immutable object engine using **LINK — Litiaina
Immutable Native Key**. The development target is **1.0.0-RC.2**, with native
**NOA 2 / `n1/1`**. RC.2 is not released or fully qualified.

A client-held key identifies an immutable pool and object. N1 calculates shard
locations, streams verified object bytes and permanently retires deleted keys.
The workspace has two main components: **`object_engine/`**, the standalone LINK
object engine, and **`metadata_engine/`**, the optional ProcyonDB-based metadata
component. Metadata provides fragments, names, versions, listings and named
upload transactions through the original HTTP and native NOA interfaces.
Users can also provide their own metadata service. There is no NCX/NDX catalogue
or namespace API in the object engine.

The new implementation includes normal/batch/multipart upload, durable parts,
restart resume, GET/ranges, object health, manual/read repair, conservative upload
GC, optional paced scrub schedules, encrypted recovery points, restore and
recovery-point replication. Native `n1://` and verified TLS `n1s://` are primary;
HTTP is an optional adapter to the same service. N1 retains adaptive concurrency,
the disk scheduler, io_uring, middleware and backup automation. Verified member
cloning and explicit single-member reconstruction are implemented; installer/UI adaptation
and full scale/fault/performance qualification remain tracked work.

## Build and start

Build with Rust on Linux. Keep `CARGO_TARGET_DIR`, temporary fixtures and large
reports on storage with adequate space; do not put test data in the source tree.

```sh
cargo build --locked --release -p n1 -p n1_shell
litiaina-n1 --help
litiaina-n1-shell --help
litiaina-n1-backup --help
```

The object engine's package and executable names are preserved after the
directory rename. Build the optional metadata component independently with
`cargo build --locked -p n1_metadata_engine`. Its `litiaina-n1-metadata` binary
provides the public named-object server and offline `init` and `check` commands.
Both components accept operator INI configuration; see the operator guide for
listen addresses, TLS, immutable pool enrollment and concurrency controls.

Use `litiaina-n1 init` only with explicitly selected empty, independent member
roots. Never point enrollment or restore at existing production storage. The
operator guide explains configuration and credentials; startup does not format
or adopt missing members automatically.

## Documentation

- [Architecture](ARCHITECTURE.md): engine boundaries and invariants.
- [Metadata engine](docs/METADATA-ENGINE.md): Procyon foundation, standard and integration scope.
- [LINK format](docs/LINK-FORMAT.md): canonical identities, tickets and disk bytes.
- [NOA](NOA.md): implemented native operations and secondary HTTP adapter.
- [Operator guide](DOCUMENTATION.md): setup, native CLI and recovery tools.
- [Implementation ledger](docs/RC2-IMPLEMENTATION.md): completed work and open gates.
- [Testing](TESTING.md): current qualification context and historical evidence.
- [Changelog](CHANGELOG.md): release history.

The previous engine and its historical tooling are preserved in Git at `061a40a`;
old-format data and endpoints are not supported by the new engine.

## License and reports

N1 is licensed under [Apache 2.0](LICENSE). See [CONTRIBUTING.md](CONTRIBUTING.md)
for the project's contribution and reporting policy.
