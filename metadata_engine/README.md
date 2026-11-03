# N1 metadata engine

N1's optional metadata component is built on the ProcyonDB transactional storage
core in [`storage/`](storage). It is a main Cargo workspace component alongside
[`object_engine/`](../object_engine). The object engine runs independently.

The integrated foundation provides durable optimistic transactions, consistent
snapshots, fragment-scoped record keys, bounded prefix pagination, owned blocking
workers and offline database administration. The application layer now includes
named-upload coordination through the existing native LINK API and exact-key
namespace, version, rename, trash and recovery transactions. Public HTTP and
native NOA adapters expose fragments, named uploads, listings and reads to the
original console. INI configuration preserves operator listen, TLS and resource
controls. Remaining product integration and qualification gates are tracked in
[the implementation ledger](../docs/RC2-IMPLEMENTATION.md).

See [the architecture, API boundary and validation contract](../docs/METADATA-ENGINE.md)
and [the storage provenance](storage/UPSTREAM.md).

The [Rust physical qualification harness](examples/hdd_qualification/README.md)
exercises concurrent writers/readers, population, update/delete fixtures,
SIGKILL/resume and full verification in a new private HDD directory. MCH's
completed one-million-entry/four-drive evidence is recorded in [TESTING.md](../TESTING.md).
