# Procyon Engine

This directory contains ProcyonDB's vendored internal storage engine.

It is not a public product surface. ProcyonDB owns the database identity,
document model, query behavior, authentication, operations, and API contract.
The engine crate exists to provide the local log-structured key-value substrate
used by the ProcyonDB document layer.

The crate is intentionally consumed as:

```toml
procyon-engine = { path = "engine" }
```

and imported as:

```rust
use procyon_engine::{Database, KeyspaceCreateOptions, PersistMode};
```

Product documentation should describe ProcyonDB, not this internal crate.
