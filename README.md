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

N2 is Litiaina's distributed object storage system. It accepts object,
namespace, and maintenance operations on any healthy node, places immutable
erasure-coded object versions across static server pools, and keeps every
user-visible mutation in a small flat namespace record.

N2 `3.4.1` uses the clean N2 3.x engine and disk-format generation. N2 3.0
replaced the N2 2.x inode namespace with replicated flat NCNX key records
while retaining
immutable NCDX metadata, immutable shards and parity, per-object recorded
layouts, strict fail-closed publication, explicit redistribution, and
autonomous healing. N2 3.1 stores each delete mutation in one
`<unix-nanos>.<mutation-ulid>` trash generation without changing server-pool
or EC placement. N2 3.2 makes SHA-256 the only N2 integrity checksum, embeds
SHA-256 integrity in NCNX and object NCDX, derives object placement from
immutable server-pool membership, and uses replica-backed exact listing.
Survival publication uses the same bounded replica merge for every LIST page
so a noncanonical surviving NCNX copy is immediately visible. N2 3.x does not open or migrate N2
1.x, 2.x, or earlier experimental 3.x roots. N2 3.3 adds an autonomous
cluster Reserve Set: an unassigned physical instance can replace one failed
logical pool slot without changing the pool generation, EC width, logical
placement identities, or immutable object layouts. A separate binding epoch
fences the retired physical instance while bounded healing restores the
recorded NCNX, NCDX, shard, and parity layouts.

N2 3.3.1 is the node-churn correctness patch for the 3.3 engine. Autonomous
healing distinguishes temporary NCNX/NCDX transport unavailability from
proven missing, divergent, or corrupt immutable state. The survival write path
retains the configured layout and can acknowledge at the EC reconstruction
threshold: `K` EC pieces, `K` NCDX copies, and one of `M + 1` NCNX replicas.
Missing exact slots remain deterministic healing work; writes reject below
`K`.

N2 3.4.1 makes that survival path the release contract. It preserves each
pool's exact static `K + M` layout, acknowledges new objects at `K`, keeps
missing exact slots as deterministic healing targets, and reports degraded
rather than falsely healthy until full protection returns. It does not add
quorum consensus or adaptive placement.

A key such as `videos/mhw.mp4` is stored as `videos/mhw.mp4.ncnx` on its
deterministic replica holders. The `videos/` path is a key prefix, not a
shared inode, transaction participant, or lock. Exact-key mutations use
destination-first copy/delete and never rewrite NCDX or object payloads.

N2 is not an S3 clone and does not use S3 as its core identity. Its public
surface is the NOA v1 `/noa/v1/...` HTTP API plus the `n2://` native protocol
for N2-native operators, peers, tools, and future internal systems.

## What Ships Here

- `engine/` - N2 distributed object storage engine; Cargo package `n2`,
  executable `litiaina-n2`.
- `shell/` - native N2 operator shell; Cargo package `n2_shell`, executable
  `litiaina-n2-shell`.
- `gateway/` - route-only N2 front door for user-facing traffic; Cargo package
  `n2_gateway`, executable `litiaina-n2-gateway`.
- `native_client/` - reusable Rust client for `n2://`; Cargo package
  `n2_native_client`.
- `simd-erasure-core/` - SIMD erasure implementation used by N2; Cargo package
  `simd-erasure-core`.

The generic directory names describe repository structure. Product-specific
Cargo packages, executables, systemd services, configuration, and protocol
identities remain unchanged so installed components are unambiguous.

## Documentation

- [DOCUMENTATION.md](DOCUMENTATION.md): complete setup, operation, and API
  reference with request bodies, query fields, responses, and examples.
- [NOA.md](NOA.md): canonical NOA v1 routes and N2 cluster extensions.
- [INSTALLATION.md](INSTALLATION.md): production setup, static server-pool
  membership, systemd deployment, gateway deployment, and validation.
- [ARCHITECTURE.md](ARCHITECTURE.md): N2 3.4 storage, NCNX namespace,
  lifecycle, healing, expansion, and failure laws.
- [MULTI_CLUSTER.md](MULTI_CLUSTER.md): proposed regional active-active and
  read-only fragment replication, site policy, convergence, fencing,
  failover, maturity isolation, and release gates.
- [GATEWAY.md](GATEWAY.md): optional route-only gateway deployment.
- [TESTING.md](TESTING.md): correctness, lifecycle, failure, restart, and
  soak procedures with the currently qualified evidence, including the clean
  N2 3.4.1 24-hour release-soak pass.
- [BENCHMARK.md](BENCHMARK.md): measured performance, topology, methodology,
  limitations, and reproduction guidance.

## Core Model

N2 stores object data as immutable versions. Each committed version has:

- a logical key such as `videos/mhw.mp4`;
- an immutable NCNX record mapping that key to a stable `@o_<object-id>`;
- an immutable NCDX record describing the version and its exact layout;
- exactly `K` data shards and `M` parity shards placed on distinct nodes in
  one recorded static server-pool generation;
- exact node IDs, root IDs, roles, sizes, and checksums recorded in NCDX;
- max-ULID version selection without a mutable latest pointer.

Each server pool has a static `K + M` contract. N2 never lowers parity to
make a failed topology writable and never reinterprets an old object using
new membership. Capacity expansion adds a complete server-pool generation.
Existing objects remain on their recorded layouts until an operator requests
redistribution.

NCNX is the namespace authority:

- one normalized key maps to one stable object ID;
- NCNX replica count is `R = M + 1` for the key's home pool;
- replica holders are deterministic from stable cluster and key inputs;
- LIST reads physical paths and filenames without decoding NCNX or NCDX;
- rename/move publishes the destination before removing the source;
- trash/recover publishes the destination state before removing the old one;
- prefix operations enumerate keys and apply the same bounded per-key
  protocol; they are restart-safe per key, not globally atomic.

Every configured storage root is unified. It can hold fragment state, NCNX,
NCDX, shards, parity, and temporary uploads. `runtime_path` is process-local
control state; it is not namespace or object authority.

The result is direct operational behavior:

- any healthy bidirectional node can coordinate a request;
- uploads distribute immutable shards across their selected static pool;
- finalized objects can be read, inspected, healed, and redistributed
  through any healthy node;
- namespace operations never open or rewrite object payloads;
- repair restores an exact recorded layout;
- redistribution explicitly re-encodes into a current layout;
- autonomous healing repairs NCNX, NCDX, shard, and parity damage and writes
  disposable per-event JSON reports;
- an object-local unrepairable condition is reported without stopping
  unrelated reconstructable objects; systemic membership or binding authority
  failure can still engage the fail-closed safety state.

## Coordination Model

N2 is bidirectional at the node level. Any healthy node can coordinate:

- authentication and fragment access;
- one-shot and multipart upload;
- standard small-object batch upload with independent per-object results;
- object read, download, metadata, status, repair, and redistribution;
- flat namespace list, stat, rename, move, trash, and recover;
- fragment administration;
- cluster health, usage, healing, repair, and rebalance.

Coordination is stateless for committed objects. A coordinator calculates
deterministic placement, contacts the exact holders, and returns the result.
There is no coordinator-owned namespace database.

Multipart sessions are intentionally coordinator-owned until finalize. The
node accepting `multipart init` returns its `coordinator_node_id`. Clients
retain that coordinator for part, status, finalize, and cancel requests.
Object shards are distributed during upload; only temporary resumability
state remains local to the coordinator.

After finalize, strict NCNX publication makes the object cluster-visible and
it can be read through any healthy node.

## Repository Layout

```text
engine/
  src/                  server, object engine, flat namespace, distribution
shell/
  src/                  N2 native operator shell
native_client/
  src/                  reusable Rust client for n2://
gateway/
  src/                  optional route-only N2 gateway
deploy/systemd/
  litiaina-n2*.service.in
deploy/install.sh       production installer
simd-erasure-core/
  src/                  erasure coding implementation
scripts/bench/
  run_n2_3_0_release_proof.sh
  run_n2_3_0_soak_chaos.sh
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
storage_roots=node-a-root=/mnt/n2/storage
runtime_path=/mnt/n2/control

[cluster_config]
node_id=node-a
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
pool_id=pool-a
pool_generation=1
object_data_shards=2
object_parity_shards=2
auto_rebalance=false

[auto_healing]
enabled=true
initial_delay_seconds=30
scan_interval_seconds=900
coordinator_nodes=auto
max_parallel_fragments=2
max_objects_per_fragment_cycle=256
max_duration_seconds=30
lease_seconds=90
checksum_scan_every_cycles=24

[reserve_set]
enabled=true
automatic_promotion=true
initial_delay_seconds=30
probe_interval_seconds=5
failure_threshold=3
promotion_timeout_seconds=30

[upload_config]
data_shards=2
parity_shards=2
```

Every node has a unique `node_id`, endpoints, and stable local root IDs.
Every member of a pool uses the same pool ID, generation, and EC contract.
The format-v5 membership file defines complete static server-pool generations,
logical slot bindings, reserve physical instances, retired-instance fences,
and every root identity. See [INSTALLATION.md](INSTALLATION.md).

## API Entry Points

- HTTP API: `/noa/v1/...`
- Native protocol: `n2://host:port`
- WebSocket event stream: `/noa/v1/ws`
- Peer routes: `/n2/peer/...` and namespace peer routes are internal
  node-to-node surfaces.

Basic HTTP auth:

```bash
curl -k https://127.0.0.1:50001/noa/v1/auth/fragment \
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
curl -k https://127.0.0.1:50001/noa/v1/posix/list?path=videos \
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

Run repository gates:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --lib --bins --tests
cargo build --workspace --release
```

Run the finite N2 3.4 release proof (the retained script filename identifies
the flat-NCNX proof family):

```bash
REMOTE_PASS='operator-supplied' \
  scripts/bench/run_n2_3_0_release_proof.sh
```

Run the bounded lifecycle/chaos harness:

```bash
scripts/bench/run_n2_3_0_soak_chaos.sh --help
```

Run installer dry-run proof:

```bash
scripts/proof/run_n2_installer_proof.sh
```

The current qualified evidence and exact limitations are in
[TESTING.md](TESTING.md). Performance numbers and methodology are kept
separately in [BENCHMARK.md](BENCHMARK.md). Evidence output belongs outside
the repository under `/home/wlzambo/DEV-TEST`.

## Operational Boundaries

- N2 `3.4.1` is a clean 3.x engine format, not an in-place migration
  from N2 1.x or 2.x.
- A server pool's `K + M`, node IDs, and root-ID set are static after
  enrollment. Expansion adds a new complete pool generation.
- A Reserve Set replaces a failed physical binding while preserving its
  logical pool slot, static EC contract, placement identity, and topology
  epoch. Reserve instances are never placement participants before promotion.
- NCNX uses `M + 1` deterministic replicas within its home pool.
- N2 fails closed when exact object or namespace participants required for
  a safe mutation are unavailable.
- Exact-key mutation is copy/delete. Prefix mutation is restart-safe per key
  and intentionally not globally atomic.
- Empty directories are not persistent. `/posix/mkdir` is rejected rather
  than claiming false persistence.
- Adding a pool never redistributes old objects automatically.
- Repair restores a recorded layout. Redistribution is explicit.
- Current verification is bounded evidence, not an exabyte deployment or
  unconditional production guarantee.
- Capacity and full-disk admission belong to N2; business quotas belong
  above it.
- S3 compatibility is not N2's core identity and belongs in an adapter if
  required.
