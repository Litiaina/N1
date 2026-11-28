<div align="center">
  <img src="assets/litiaina_icon.png" alt="Litiaina" width="120" height="120" />
  <h1>Litiaina N1</h1>
</div>

**N1 is a high-performance single-node object store built on LINK for durable,
key-addressed storage. M1 adds named objects and versions through a separate
metadata service.**

LINK (Litiaina Immutable Native Key) lets N1 calculate shard locations from an
object key and the pool configuration. N1 supports multipart uploads, verified
range reads, erasure coding and shard repair. Applications that manage their
own names can store the LINK keys and use N1 directly.

M1 manages names, fragments, listings, versions, permissions and trash in its
own transactional database. It uses N1's native API for object data. Both
engines live in this repository and have independent versions. Console is
distributed separately and connects to M1.

## Get started

- [Download System 1.0.0-RC.3](https://github.com/Litiaina/N1/releases/tag/v1.0.0-RC.3)
  for Linux x86-64. This release candidate includes N1 1.0.0-RC.2,
  M1 1.0.0-RC.2, the N1 backup helper and systemd installers.
- [Run N1 locally](docs/QUICKSTART.md) with a disposable single-member pool.
  Use the [operator guide](DOCUMENTATION.md) for redundant pools, TLS and M1 setup.

To build from source, install Rust, a C toolchain, Git, `pkg-config` and the
OpenSSL development headers on Linux. Then run:

```sh
cargo build --locked --release -p n1 -p m1 -p n1_shell
```

Cargo writes the server binaries, backup helper and optional N1 Shell to
`target/release/` by default.

## Documentation

| Guide | Covers |
| --- | --- |
| [Operator guide](DOCUMENTATION.md) | Pool enrollment, configuration, maintenance, backup and recovery |
| [N1 API](docs/api/N1.md) | Direct LINK operations and a runnable Rust example |
| [M1 API](docs/api/M1.md) | Named uploads, listings, trash and administration with HTTP examples |
| [NOA](NOA.md) | Shared API specification and protocol versions |
| [Architecture](ARCHITECTURE.md) | LINK placement, storage layout and engine responsibilities |
| [Testing](TESTING.md) | How to run tests and read the qualification results |
| [Changelog](CHANGELOG.md) · [Versioning](docs/VERSIONING.md) | Release changes and component versions |

## License and project policy

N1 and M1 are available under the [Apache License 2.0](LICENSE). The maintainers
develop the project and publish the source for anyone to use, study, modify and
redistribute under the license. See the [contribution policy](CONTRIBUTING.md)
and [security reporting policy](SECURITY.md).
