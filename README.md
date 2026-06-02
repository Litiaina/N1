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

N2 is Litiaina's distributed object storage system. It accepts storage,
namespace, and maintenance operations on any healthy node, places immutable
object shards across the cluster, and keeps namespace mutation as the
coordination boundary.

N2 is not an S3 clone and does not use S3 as its core identity. Its public
surface is LOA-style `/n2/...` HTTP API plus the `n2://` native protocol for
N2-native operators, peers, tools, and future internal systems.

## What Ships Here

- `litiaina-n2`: the N2 distributed object storage server.
- `litiaina-n2-cli`: native N2 operator CLI.
- `n2_native_client`: reusable Rust client for `n2://`.
- `registry`: business/user/account/policy layer for Litiaina storage.
- `indexdb`: Registry metadata engine.
- `simd-erasure-core`: SIMD erasure implementation used by N2.

## Documentation

- [DOCUMENTATION.md](DOCUMENTATION.md): setup, operations, API usage, request
  bodies, query fields, examples, and troubleshooting.
- [ARCHITECTURE.md](ARCHITECTURE.md): storage layout, object identity,
  cluster placement, namespace coordination, repair, and rebalance design.
- [REGISTRY_DOCUMENTATION.md](REGISTRY_DOCUMENTATION.md): Registry business
  layer documentation.

## Core Model

N2 stores object data as immutable versions. Each committed version has:

- a namespace path used by users and APIs, such as `videos/mhw.mp4`;
- an immutable object identity and version record;
- shard and parity payloads placed across active cluster nodes;
- a recorded layout, so reads use the layout that actually wrote the object;
- a namespace head that can move, rename, trash, recover, or point to a newer
  version without rewriting payload data.

The result is simple operational behavior:

- new uploads can enter through any healthy node;
- each upload coordinator distributes object payloads across the cluster;
- finalized objects can be listed, read, inspected, and repaired from any node;
- folder rename and move are namespace mutations, not object rewrites;
- old objects remain valid after membership changes until explicit rebalance.

## Coordination Model

N2 is bidirectional at the node level. Any healthy node can coordinate:

- authentication;
- fragment access;
- one-shot upload;
- multipart upload initiation;
- object read/download/status;
- namespace list/mkdir/rename/trash/recover/purge;
- cluster health, usage, repair, and rebalance flows.

Multipart sessions are intentionally coordinator-owned until finalize. The node
that accepts `multipart init` owns the temporary upload session and should keep
receiving that session's part, status, finalize, and cancel requests. The object
data is still distributed across the cluster; only the temporary resumability
state is local to the coordinator.

After finalize, the object becomes cluster-visible and can be read from any
node.

## Repository Layout

```text
n2/
  src/                  N2 server, object storage, namespace, distribution, protocol
n2_cli/
  src/                  N2 native operator CLI
n2_native_client/
  src/                  reusable Rust client for n2://
registry/
  src/                  account, session, sharing, policy, and business layer
indexdb/
  src/                  Registry metadata engine
simd-erasure-core/
  src/                  erasure coding implementation
scripts/bench/
  run_n2_hardening_native_proof.sh
certificates/
  local development TLS certificates
```

## Build

Build the server:

```bash
cargo build -p n2 --release
```

Build the native CLI:

```bash
cargo build -p n2_cli --release
```

Run the server locally:

```bash
cargo run -p n2 --release
```

The server creates missing local config files on first run, but production
deployments should use explicit `n2_config.ini` and environment values.

## Minimal Configuration

```ini
[server]
host=0.0.0.0
https_port=50001
native_port=50002
cert_path=./certificates/cert.pem
key_path=./certificates/key.pem

[disk_config]
index_store_disks=/mnt/n2/index
blob_store_disks=/mnt/n2/blob
fs_storage_path=/mnt/n2/fs

[cluster_config]
node_id=node-a
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
cluster_layout_mode=auto
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled
```

For multi-node clusters, every node uses its own `node_id`, HTTP endpoint,
native endpoint, and local disk roots. The membership file records the cluster
node list and epoch.

## API Entry Points

- HTTP API: `/n2/...`
- Native protocol: `n2://host:port`
- WebSocket event stream: `/ws`
- Peer routes: `/n2/peer/...` and namespace peer routes are internal
  node-to-node surfaces.

Basic HTTP auth:

```bash
curl -k https://127.0.0.1:50001/n2/auth/fragment \
  -H 'Content-Type: application/json' \
  -d '{
    "fragment": "n2-test",
    "secret": "fragment-key",
    "read": true,
    "write": true,
    "modify": true,
    "delete": true
  }'
```

The response contains a Bearer access token and optional refresh token. Use the
access token on protected routes:

```bash
curl -k https://127.0.0.1:50001/n2/posix/list?path=videos \
  -H "Authorization: Bearer $N2_TOKEN"
```

See [DOCUMENTATION.md](DOCUMENTATION.md) for full route bodies, query fields,
and examples.

## Native CLI

```bash
target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 ping

target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 \
  health --fragment n2-test --secret fragment-key

target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 \
  list --fragment n2-test --secret fragment-key --path videos --limit 200

target/release/litiaina-n2-cli --native-url n2://127.0.0.1:50002 \
  upload-one-shot --fragment n2-test --secret fragment-key \
  --object-key videos/sample.mp4 --file ./sample.mp4 --content-type video/mp4
```

The CLI also has a `raw` command for calling supported native operation names
directly.

## Proof And Testing

Run unit-level checks:

```bash
cargo test -p n2 --lib
cargo test -p n2_cli
cargo test -p n2_native_client
```

Run the N2 native hardening harness:

```bash
scripts/bench/run_n2_hardening_native_proof.sh
```

The proof harness is intended for multi-node correctness checks and writes
ignored local output under `.n2-bench/`.

## Operational Boundaries

- N2 `1.0.0` means the product shape and architecture are finalized enough for
  the 1.0 line.
- It is not an LTS statement yet.
- Long soak, larger cluster proof, node loss/re-add proof, and wider UI
  viewport proof remain release-validation work.
- Per-fragment quota is monitoring-only in N2. Enforcement belongs in a
  business layer such as Registry.
- S3 compatibility is not part of the N2 core. If needed, it should live as an
  adapter layer.
