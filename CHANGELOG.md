# Changelog

## System 1.0.0-RC.2

One release bundles **N1 1.0.0-RC.2** and **M1 1.0.0-RC.1** for Linux x86-64.
[Download the release and checksums](https://github.com/Litiaina/N1/releases/tag/v1.0.0-RC.2).

- **N1** uses LINK keys to locate and verify shards within immutable storage
  pools. It supports standalone multipart upload and resume, range reads,
  erasure coding, shard repair and physical purge.
- **M1** adds fragments, names, versions, search, trash, garbage collection
  and metadata backup through its own transactional database. It stores
  object bytes through N1's native API.
- **Operations** include independent configuration and systemd services,
  object backup and recovery, and API references for both engines. The bundle
  includes the N1 backup helper. Console is distributed separately.

See the [detailed changelog](docs/CHANGELOG-HISTORY.md) for the development
history and [qualification](docs/RC2-IMPLEMENTATION.md) for test evidence.
