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

N2 deliberately keeps POSIX-style namespace operations fast without becoming a
filesystem. Directory listing, rename, move, trash, and recover operate on a
small partitioned namespace quorum of logical pointers, while object bytes and
`.ncdx` records remain immutable in the distributed blob plane. This avoids a
common object-storage weakness where path-oriented workflows degrade into
large metadata scans or payload rewrites.

N2 is not an S3 clone and does not use S3 as its core identity. Its public
surface is LOA-style `/n2/...` HTTP API plus the `n2://` native protocol for
N2-native operators, peers, tools, and future internal systems.

## What Ships Here

- `litiaina-n2`: the N2 distributed object storage server.
- `litiaina-n2-shell`: native N2 operator shell.
- `litiaina-n2-gateway`: route-only N2 front door for user-facing traffic.
- `n2_native_client`: reusable Rust client for `n2://`.
- `simd-erasure-core`: SIMD erasure implementation used by N2.

## Documentation

- [DOCUMENTATION.md](DOCUMENTATION.md): setup, operations, API usage, request
  bodies, query fields, examples, and troubleshooting.
- [INSTALLATION.md](INSTALLATION.md): production setup, systemd templates,
  gateway deployment, and validation checklist.
- [ARCHITECTURE.md](ARCHITECTURE.md): storage layout, object identity,
  cluster placement, namespace coordination, repair, and rebalance design.
- [GATEWAY.md](GATEWAY.md): gateway deployment, route-only behavior, and
  client-owned multipart coordinator tracking.

## Core Model

N2 stores object data as immutable versions. Each committed version has:

- a namespace path used by users and APIs, such as `videos/mhw.mp4`;
- a direct namespace pointer to an immutable `@o_<object-id>`;
- an immutable `.ncdx` version record colocated with shard data;
- shard and parity payloads placed across active cluster nodes;
- a recorded layout, so reads use the layout that actually wrote the object;
- max-ULID head selection without a mutable latest pointer.

The result is simple operational behavior:

- new uploads can enter through any healthy node;
- each upload coordinator distributes object payloads across the cluster;
- finalized objects can be listed, read, inspected, and repaired from any node;
- folder rename and move are namespace mutations, not object rewrites;
- old objects remain valid after membership changes until explicit rebalance;
- object status reports layout drift, object redistribution can rewrite one
  version, and targeted rebalance skips already-current objects.
- cluster monitoring exposes overview, nodes, topology, object-layout health,
  redistribution, jobs, namespace consensus, and upload activity in one
  efficient `/n2/cluster/monitor` snapshot.
- distributed maintenance assigns fragments across reachable nodes for
  fragment-level GC, repair, and targeted rebalance.

N2 supports separated and shared disk roles. With separated roots, fast index
storage holds mutable namespace/control state while capacity storage holds
colocated `.ncdx` and shard/parity data. With a shared root, the exact same path
is configured for both roles and N2 maintains one canonical tree without a
second metadata plane.

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
that accepts `multipart init` owns the temporary upload session and returns its
`coordinator_node_id`. Clients keep that coordinator and send it on part,
status, finalize, and cancel requests. The object data is still distributed
across the cluster; only the temporary resumability state is local to the
coordinator.

After finalize, the object becomes cluster-visible and can be read from any
node.

## Repository Layout

```text
n2/
  src/                  N2 server, object storage, namespace, distribution, protocol
n2_shell/
  src/                  N2 native operator shell
n2_native_client/
  src/                  reusable Rust client for n2://
n2_gateway/
  src/                  user-facing route-only N2 gateway
deploy/systemd/
  litiaina-n2*.service.in
deploy/install.sh       production installer for node/gateway systemd setup
simd-erasure-core/
  src/                  erasure coding implementation
scripts/bench/
  run_n2_benchmark_matrix.sh
  run_n2_capacity_expansion_proof.sh
  run_n2_hardening_native_proof.sh
scripts/proof/
  run_n2_installer_proof.sh
certificates/
  local development TLS certificates
```

## Build

Build the server:

```bash
cargo build -p n2 --release
```

Build the native shell:

```bash
cargo build -p n2_shell --release
```

Build the gateway:

```bash
cargo build -p n2_gateway --release
```

Install a production node or gateway:

```bash
sudo ./deploy/install.sh --node --build --copy-certs --enable --start
sudo ./deploy/install.sh --gateway --build --copy-certs --enable --start \
  --gateway-node node-a,https://192.168.0.22:50001,n2://192.168.0.22:50002
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
auto_max_data_shards=8
auto_max_parity_shards=3
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false
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

## Native Shell

```bash
target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 ping

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  health --fragment n2-test --secret fragment-key

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  list --fragment n2-test --secret fragment-key --path videos --limit 200

target/release/litiaina-n2-shell --native-url n2://127.0.0.1:50002 \
  upload --fragment n2-test --secret fragment-key \
  --object-key videos/sample.mp4 --file ./sample.mp4 --content-type video/mp4
```

The shell also has a `raw` command for calling supported native operation names
directly.

Useful operator shell flows:

```bash
target/release/litiaina-n2-shell \
  --native-url n2://127.0.0.1:50002 \
  --fragment n2-test --secret fragment-key \
  profile save --name lab

target/release/litiaina-n2-shell --profile lab dashboard
target/release/litiaina-n2-shell --profile lab repair watch
target/release/litiaina-n2-shell --profile lab rebalance watch
```

## Proof And Testing

Run unit-level checks:

```bash
cargo test -p n2 --lib
cargo test -p n2_shell
cargo test -p n2_native_client
```

Run the N2 native hardening harness:

```bash
N2_PROOF_STORAGE_LAYOUT=separated \
  scripts/bench/run_n2_hardening_native_proof.sh

N2_PROOF_STORAGE_LAYOUT=shared \
  scripts/bench/run_n2_hardening_native_proof.sh
```

Run installer dry-run proof:

```bash
scripts/proof/run_n2_installer_proof.sh
```

Run benchmark and capacity-expansion proofs:

```bash
FRAGMENT=n2-test SECRET=fragment-key scripts/bench/run_n2_benchmark_matrix.sh
scripts/bench/run_n2_capacity_expansion_proof.sh
```

The proof harnesses are intended for multi-node correctness checks and write
ignored local output under `.n2-bench/`. The hardening harness does not kill
existing N2 services unless `N2_PROOF_ALLOW_PKILL=1` is explicitly set.

Latest three-node proof summary:

```text
N2 package tests: 111 passed
Workspace tests: N2, gateway, shell, native client, and SIMD core passed
Separated hardening proof: passed
Shared-root hardening proof: passed
Capacity expansion proof: passed
Live gateway smoke: one-shot SHA matched, cluster health healthy
```

The separated proof covered node-b loss, read-on-recover, single-shard delete
reconstruction, object repair, cluster repair, targeted rebalance, POSIX
mutation, trash/recover, and old-layout rejection checks.

Latest distributed maintenance proof:

```text
Node C unreachable: 192.168.0.18 returned Destination Host Unreachable
Live workers: node-a, node-b
Distributed rebalance: 4 fragments, scanned=7, skipped=7, failed=0
Distributed repair dry-run: 4 fragments, scanned=7, rewritten=7, failed=0
Distributed GC: 4 fragments scanned, failed=0
```

## Operational Boundaries

- N2 `1.0.0` means the product shape and architecture are finalized enough for
  the 1.0 line.
- It is not an LTS statement yet.
- Long soak, clusters larger than three nodes, and wider UI viewport proof
  remain release-validation work.
- Earlier development layouts are intentionally rejected rather than migrated;
  the 1.0 line uses the documented index/blob role split.
- N2 exposes capacity monitoring and full-disk admission only. Per-user or
  per-fragment business limits belong outside N2.
- S3 compatibility is not part of the N2 core. If needed, it should live as an
  adapter layer.
