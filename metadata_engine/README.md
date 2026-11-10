# M1 — Litiaina metadata store

M1 is the metadata store in the N1 repository. It owns the specialized
transactional storage core in [`storage/`](storage). Cargo package/crate `m1`
builds the `litiaina-m1` binary. N1, the independent **LINK object store**, lives
in [`object_engine/`](../object_engine) and does not depend on M1 or its database.

`Console / named client → M1 → N1 (LINK) → shards`

M1's defaults are `m1_config.ini` and an optional private `m1.env`; explicit
config paths, `[server] env_file` and service environment provisioning remain
available. The integration INI section retains its `[metadata_engine]` name.

The integrated foundation provides durable optimistic transactions, consistent
snapshots, fragment-scoped record keys, bounded prefix pagination, owned blocking
workers and offline database administration. The application layer now includes
named-upload coordination through the existing native LINK API and exact-key
namespace, version, rename, trash and recovery transactions. Public HTTP and
native NOA adapters expose fragments, named uploads, listings and reads to the
original console. INI configuration preserves operator listen, TLS and resource
controls. Remaining product integration and qualification gates are tracked in
[the implementation ledger](../docs/RC2-IMPLEMENTATION.md).

See the [M1 API reference and request/response examples](../docs/api/M1.md),
[the architecture, API boundary and validation contract](../docs/M1.md)
and [the storage module map and durability rules](storage/README.md).

The [Rust physical qualification harness](examples/hdd_qualification/README.md)
exercises concurrent writers/readers, population, update/delete fixtures,
SIGKILL/resume and full verification in a new private HDD directory. Historical
results and new-core qualification are distinguished in [TESTING.md](../TESTING.md).
