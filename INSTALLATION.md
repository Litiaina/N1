# N2 Installation

> N2 2.0 is still in development. The role-separated membership example in
> `DOCUMENTATION.md` and the release gates in `N2_2_0_VERIFICATION.md` are the
> current 2.0 authority. Do not treat the legacy deployment walkthrough below
> as a production-release instruction until Phase 8 is completed.

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

This guide is the production installation checklist. It intentionally does not
ship local `.env` or `.ini` files in git. Those files are runtime state and must
be created on the node that owns them.

## Install Shape

```text
users / UI / operators
        |
        v
litiaina-n2-gateway        optional public/user front door
        |
        v
litiaina-n2 node-a / node-b / node-c / ...
```

Every N2 node remains directly usable. The gateway is route-only: it selects a
healthy node and returns multipart coordinator information to the client. It is
not namespace truth, cluster truth, or resumability authority.

## Filesystem Layout

Recommended production paths:

```text
/usr/local/bin/litiaina-n2
/usr/local/bin/litiaina-n2-shell
/usr/local/bin/litiaina-n2-gateway

/etc/litiaina/n2/n2.env
/var/lib/litiaina/n2/n2_config.ini
/var/lib/litiaina/n2/n2_membership.json
/var/lib/litiaina/n2/certificates/

/var/lib/litiaina/n2-gateway/n2_gateway_config.ini
/var/lib/litiaina/n2-gateway/certificates/
```

Use real mounted storage paths. N2 2.0 assigns each node a namespace or blob
role, and every configured root on that node belongs to that plane:

```text
namespace node: /mnt/namespace-ssd/n2-storage
blob node:      /mnt/blob-hdd0/n2-storage, /mnt/blob-hdd1/n2-storage
each node:      /mnt/nvme/n2-control
```

Namespace-node roots hold inode replicas. Blob-node roots hold immutable object
data and `.ncdx` records. One logical node cannot mix both roles. N2 does not
migrate earlier development layouts in place.

`runtime_path` holds process-local membership, upload coordination, and job
state. Namespace truth lives only on namespace-node storage roots; object truth
lives only on blob-node storage roots.

### Storage Configuration

```ini
[disk_config]
storage_roots=ROOT_ID=/mnt/n2/storage
runtime_path=/mnt/n2/control
```

## Build

```bash
cargo build -p n2 -p n2_shell -p n2_gateway --release
```

## Installer

The preferred production path is `deploy/install.sh`. It installs binaries,
creates runtime directories, writes missing runtime config, renders systemd
services, and can enable/start the services.

The installer does not overwrite existing `n2.env`, `n2_config.ini`,
`n2_membership.json`, or gateway config unless `--force-config` is passed.
When it creates a missing environment file, it generates local random secrets
instead of shipping static credentials.

Preview a node install without changing the machine:

```bash
./deploy/install.sh --node --dry-run \
  --node-id node-a \
  --node-endpoint https://192.168.0.22:50001 \
  --native-endpoint n2://192.168.0.22:50002 \
  --storage-roots /mnt/DISK0/n2-storage,/mnt/DISK1/n2-storage \
  --fs-root /mnt/DISK0/n2-control
```

Install an N2 node:

```bash
sudo ./deploy/install.sh --node --build --copy-certs \
  --node-id node-a \
  --node-endpoint https://192.168.0.22:50001 \
  --native-endpoint n2://192.168.0.22:50002 \
  --storage-roots /mnt/DISK0/n2-storage,/mnt/DISK1/n2-storage \
  --fs-root /mnt/DISK0/n2-control \
  --enable --start
```

Install the route-only gateway:

```bash
sudo ./deploy/install.sh --gateway --build --copy-certs \
  --gateway-node node-a,https://192.168.0.22:50001,n2://192.168.0.22:50002 \
  --gateway-node node-b,https://192.168.0.13:50001,n2://192.168.0.13:50002 \
  --gateway-node node-c,https://192.168.0.18:50001,n2://192.168.0.18:50002 \
  --enable --start
```

Install both N2 and gateway on the same machine:

```bash
sudo ./deploy/install.sh --both --build --copy-certs \
  --node-id node-a \
  --node-endpoint https://192.168.0.22:50001 \
  --native-endpoint n2://192.168.0.22:50002 \
  --gateway-node node-a,https://192.168.0.22:50001,n2://192.168.0.22:50002 \
  --enable --start
```

Manual binary install is still valid when an operator wants to control every
file directly:

```bash
sudo install -m 0755 target/release/litiaina-n2 /usr/local/bin/litiaina-n2
sudo install -m 0755 target/release/litiaina-n2-shell /usr/local/bin/litiaina-n2-shell
sudo install -m 0755 target/release/litiaina-n2-gateway /usr/local/bin/litiaina-n2-gateway
```

## Node Runtime

Create runtime directories:

```bash
sudo mkdir -p /etc/litiaina/n2
sudo mkdir -p /var/lib/litiaina/n2/certificates
sudo mkdir -p /mnt/n2/storage /mnt/n2/control
```

Create `/etc/litiaina/n2/n2.env` on each node:

```bash
JWT_SECRET='replace-with-long-random-value'
AUTH_KEY='server-auth-key'
MASTER_KEY='cluster-master-key'
ADMIN_KEY='admin-key'
ACCESS_TOKEN_DEFAULT_TTL_SECS=900
ACCESS_TOKEN_MAX_TTL_SECS=3600
REFRESH_TOKEN_TTL_SECS=604800
N2_RUNTIME_WORKER_THREADS=
N2_RUNTIME_MAX_BLOCKING_THREADS=
N2_RUNTIME_THREAD_STACK_BYTES=
```

Create `/var/lib/litiaina/n2/n2_config.ini` on each node. Each node must use a
unique `node_id`, `node_endpoint`, and `native_endpoint`.

```ini
[server]
host=0.0.0.0
https_port=50001
native_port=50002
cert_path=/var/lib/litiaina/n2/certificates/cert.pem
key_path=/var/lib/litiaina/n2/certificates/key.pem

[disk_config]
storage_roots=ROOT_ID=/mnt/n2/storage
runtime_path=/mnt/n2/control

[cluster_config]
node_id=node-a
node_role=namespace
node_endpoint=https://192.168.0.22:50001
native_endpoint=n2://192.168.0.22:50002
membership_file=n2_membership.json
namespace_redundancy=2
object_data_shards=1
object_parity_shards=1
auto_max_parity_shards=3
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=2592000

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

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting=true
auth_limit_per_second=2
auth_limit_burst_size=5
view_limit_per_second=30
view_limit_burst_size=100
upload_limit_per_second=300
upload_limit_burst_size=3000
global_limit_per_second=200
global_limit_burst_size=300

[upload_config]
max_concurrent_io=auto
adaptive_concurrency_enabled=true
adaptive_concurrency_interval_ms=auto
writer_buffer_size=auto
upload_memory_budget_bytes=auto
upload_part_strict_durability_default=false
upload_io_gate_enabled=true
stream_max_concurrency=auto
stream_chunk_size=auto
stream_readahead_bytes=auto
stream_locality_bias_enabled=true
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size=536870912
data_shards=1
parity_shards=0
stripe_block_size=4194304
```

Create the same format-v2 `n2_membership.json` on every node under
`runtime_path`. Use the complete role-separated example in `DOCUMENTATION.md`:

```json
{
  "cluster_id": "litiaina-n2-prod",
  "epoch": 1,
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "https://192.168.0.22:50001",
      "native_endpoint": "n2://192.168.0.22:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": ["/mnt/n2/storage"]
    },
    {
      "id": "node-b",
      "endpoint": "https://192.168.0.13:50001",
      "native_endpoint": "n2://192.168.0.13:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": ["/mnt/n2/storage"]
    },
    {
      "id": "node-c",
      "endpoint": "https://192.168.0.18:50001",
      "native_endpoint": "n2://192.168.0.18:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": ["/mnt/n2/storage"]
    }
  ]
}
```

Membership changes are explicit. Edit the membership file on every node, bump
`epoch`, then restart N2.

## Systemd Service

The installer renders this service automatically. For manual installs, copy
`deploy/systemd/litiaina-n2.service.in` to a local service file and replace the
placeholders:

```text
@N2_HOME@            repository or release-doc path
@N2_USER@            service user
@N2_GROUP@           service group
@N2_RUNTIME_DIR@     directory containing n2_config.ini
@N2_ENV_FILE@        /etc/litiaina/n2/n2.env
@N2_BIN@             /usr/local/bin/litiaina-n2
@N2_LOG_FILTER@      n2=info,tower_http=info
@N2_DATA_PATHS@      writable storage/control roots, space separated
```

Example replacement values:

```text
@N2_HOME@=/opt/litiaina/n2
@N2_USER@=litiaina
@N2_GROUP@=litiaina
@N2_RUNTIME_DIR@=/var/lib/litiaina/n2
@N2_ENV_FILE@=/etc/litiaina/n2/n2.env
@N2_BIN@=/usr/local/bin/litiaina-n2
@N2_LOG_FILTER@=n2=info,tower_http=info
@N2_DATA_PATHS@=/mnt/n2/storage /mnt/n2/control
```

Install and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now litiaina-n2
sudo systemctl status litiaina-n2 --no-pager
```

## Gateway Runtime

Create gateway runtime directory:

```bash
sudo mkdir -p /var/lib/litiaina/n2-gateway/certificates
```

Create `/var/lib/litiaina/n2-gateway/n2_gateway_config.ini`:

```ini
[server]
host=0.0.0.0
https_port=51002
cert_path=/var/lib/litiaina/n2-gateway/certificates/cert.pem
key_path=/var/lib/litiaina/n2-gateway/certificates/key.pem

[routing]
health_interval_seconds=5
request_timeout_seconds=300

[node.node-a]
http_url=https://192.168.0.22:50001
native_url=n2://192.168.0.22:50002
weight=1

[node.node-b]
http_url=https://192.168.0.13:50001
native_url=n2://192.168.0.13:50002
weight=1

[node.node-c]
http_url=https://192.168.0.18:50001
native_url=n2://192.168.0.18:50002
weight=1
```

The installer renders this service automatically. For manual installs, copy
`deploy/systemd/litiaina-n2-gateway.service.in` to a local service file and
replace:

```text
@N2_HOME@
@N2_USER@
@N2_GROUP@
@N2_GATEWAY_RUNTIME_DIR@
@N2_GATEWAY_BIN@
@N2_GATEWAY_LOG_FILTER@
```

Install and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now litiaina-n2-gateway
sudo systemctl status litiaina-n2-gateway --no-pager
```

## Validation

Check each node:

```bash
curl -k https://192.168.0.22:50001/noa/v1/ping
curl -k https://192.168.0.13:50001/noa/v1/ping
curl -k https://192.168.0.18:50001/noa/v1/ping
```

Check native ports from the gateway host:

```bash
nc -vz 192.168.0.22 50002
nc -vz 192.168.0.13 50002
nc -vz 192.168.0.18 50002
```

Check gateway:

```bash
curl -k https://127.0.0.1:51002/gateway/health
curl -k https://127.0.0.1:51002/gateway/nodes
```

After fragment auth, check cluster monitor:

```bash
curl -k "https://192.168.0.22:50001/noa/v1/cluster/monitor?object_limit=96" \
  -H "Authorization: Bearer $N2_TOKEN"
```

Operator proof checklist:

- create fragment through the gateway;
- authenticate through the gateway;
- one-shot upload and download;
- multipart upload, status, finalize, cancel;
- interrupt a multipart upload, reload the console, resume with the same file;
- list, mkdir, rename, move, trash, recover;
- object status and object repair;
- targeted rebalance dry-run and real run on layout-mismatched objects;
- stop one node, verify read-on-recover for repairable objects;
- restore the node and verify cluster monitor returns healthy.

Automated installer proof:

```bash
scripts/proof/run_n2_installer_proof.sh
```

Automated hardening proof:

```bash
N2_PROOF_STORAGE_LAYOUT=separated \
  scripts/bench/run_n2_hardening_native_proof.sh

N2_PROOF_STORAGE_LAYOUT=shared \
  scripts/bench/run_n2_hardening_native_proof.sh
```

The hardening proof uses temporary data roots and test ports. It does not kill
existing N2 services unless `N2_PROOF_ALLOW_PKILL=1` is explicitly set.
It verifies canonical disk layout, absence of object data on separated index
roots, byte-identical `.ncdx` participant copies, native shell access,
coordinator multipart resume, degraded reads, missing-shard reconstruction,
repair, namespace mutation, and targeted rebalance of only mismatched objects.

Capacity expansion proof:

```bash
scripts/bench/run_n2_capacity_expansion_proof.sh
```

Benchmark matrix:

```bash
FRAGMENT=n2-test SECRET=fragment-key scripts/bench/run_n2_benchmark_matrix.sh
```
