# ProcyonDB storage core

This directory contains the ProcyonDB transactional storage core used by **M1**,
the metadata store in this repository. **N1** is the independent LINK object
store and does not depend on this database. Source provenance, retained fixes
and licenses are recorded in [UPSTREAM.md](UPSTREAM.md).

The `procyon-engine` crate provides local transactional key-value storage,
journaling, snapshots and recovery. M1 supplies the fragment, namespace,
permissions, named-upload and NOA service layers. The original ProcyonDB
document server and its protocols are not part of M1.

The crate is intentionally consumed as:

```toml
procyon-engine = { path = "storage" }
```

and imported as:

```rust
use procyon_engine::{Database, KeyspaceCreateOptions, PersistMode};
```

Use **ProcyonDB** for the database foundation and **M1** for the metadata system.
The crate retains its `procyon-engine` / `procyon_engine` package and library names.
