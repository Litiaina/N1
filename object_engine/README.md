# N1 — Litiaina object store

N1 is the standalone object store built on **LINK — Litiaina Immutable Native
Key**, the key algorithm and storage architecture. Cargo package/crate `n1`
builds `litiaina-n1` and `litiaina-n1-backup`.

N1 owns immutable pools, shard I/O, upload/finalize, reads, integrity, repair,
physical purge and object recovery. **M1**, in [`metadata_engine/`](../metadata_engine),
owns names, logical versions, permissions and named-object lifecycle, using
ProcyonDB. N1 runs independently of M1 and its database.

The existing `core/`, `middleware/`, `protocol/`, `routes/`, `settings/` and
utility modules retain their responsibilities. Default operator files are
`n1_config.ini` and an optional private `n1.env`; explicit configuration paths
and service environment credentials are supported.

See the [operator guide](../DOCUMENTATION.md),
[architecture and module map](../ARCHITECTURE.md), [NOA contract](../NOA.md),
[N1 API reference and native examples](../docs/api/N1.md)
and [qualification guide](../TESTING.md).
