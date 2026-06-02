<div align="center">
  <img
    src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true"
    alt="Litiaina Icon"
    width="150"
    height="150"
  />
  <h1>Litiaina N2</h1>
  <p>
    N2 is a high-performance distributed object storage system built for
    durable, redundant, namespace-managed, and efficient storage.
  </p>
</div>

---

N2 is Litiaina's distributed object storage system. It accepts object and
namespace operations on any node, places shards across the cluster, records the
layout used by each committed version, and keeps repair, rebalance, and health
as explicit operator-visible workflows.

N2 is the focus of this repository. The workspace contains the N2 server, N2
native client, N2 CLI, Registry business layer, indexdb metadata engine used by
Registry, and the SIMD erasure core used by N2 for shard/parity work.

## Documentation

- [DOCUMENTATION.md](DOCUMENTATION.md)
  User, operator, and API reference for N2.
- [ARCHITECTURE.md](ARCHITECTURE.md)
  Storage layout, placement model, namespace rules, and maintenance design.

## Core Identity

- Distributed object storage system
- Namespace-managed object keys
- Immutable object-version records
- Immutable `.shard` and `.parity` payloads
- `/n2/...` HTTP API for broad clients
- `n2://` native protocol identity for N2-native peers and tooling
- `n2_native_client` as the reusable Rust client for N2-native tooling
- `n2_cli` as the native operator CLI
- Auto or fixed node-level EC layout
- Bidirectional node coordination with coordinator-owned multipart sessions
- Explicit health, repair, and rebalance flows
- Registry as the N2-facing business/user policy layer
- indexdb as Registry's local metadata engine
- No S3 core surface; adapters belong outside the N2 core

## Repository Layout

```text
n2/
  src/                  N2 server, storage, namespace, distribution, protocol
n2_native_client/
  src/                  reusable Rust client for n2://
n2_cli/
  src/                  N2 native operator CLI
registry/
  src/                  account, policy, session, and business layer
indexdb/
  src/                  Registry metadata engine
n1_native_client/
  src/                  client dependency retained for Registry-backed N1 compatibility
simd-erasure-core/
  src/                  SIMD erasure implementation used by N2
scripts/bench/
  run_n2_hardening_native_proof.sh
certificates/
  local development TLS certificates
```

## Build

```bash
cargo build -p n2 --release
```

The server binary is:

```bash
target/release/litiaina-n2
```

The native CLI binary is:

```bash
target/release/litiaina-n2-cli
```

## Run Locally

N2 creates local runtime configuration when a config file is missing.

```bash
cargo run -p n2 --release
```

For production-style runs, use explicit config and environment files controlled
by the deployment host.

## Coordination Model

Any healthy N2 node can coordinate a client operation. Multipart sessions are
sticky to the node that created them until finalize; the session state stays
local, while the uploaded shard/parity data is distributed across the cluster.
After finalize, the object is cluster-visible and can be read from any node.

## Proof Script

The retained proof harness is N2-specific:

```bash
scripts/bench/run_n2_hardening_native_proof.sh
```

It is designed for multi-node proof work and writes ignored local output under
`.n2-bench/`.
