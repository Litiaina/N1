# N2 Installation

N2 is a high-performance distributed object storage system built for durable,
redundant, namespace-managed, and efficient storage.

N2 3.0 is a clean disk-format generation. Never point it at an N2 1.x or 2.x
root. Build a new format-v3 cluster, copy the same membership and shared
cluster secrets to every node, and use stable root IDs for mounted devices.

## Install Shape

```text
users / UI / operators
        |
        v
litiaina-n2-gateway        optional route-only public endpoint
        |
        v
litiaina-n2 node-a / node-b / node-c / node-d / ...
        |
        v
static server-pool generations with unified storage roots
```

Every N2 node remains directly usable. The gateway selects a healthy node and
preserves client-supplied multipart coordinator routing. It is not namespace,
object, cluster, healing, or resumability authority.

Production prerequisites:

- Linux with stable mounts and synchronized time.
- TLS certificates valid for node endpoints.
- A unique stable node ID and stable root IDs on every host.
- One complete shared format-v3 membership document.
- Shared `JWT_SECRET` and `N2_CLUSTER_SECRET` values.
- Exactly `K + M` distinct nodes in each writable static server pool.

N2 counts nodes, not disks, as failure domains. Multiple roots on one host
increase capacity and IO parallelism but do not replace independent nodes.

## Filesystem Layout

Recommended production paths:

```text
/usr/local/bin/litiaina-n2
/usr/local/bin/litiaina-n2-shell
/usr/local/bin/litiaina-n2-gateway

/etc/litiaina/n2/n2.env
/var/lib/litiaina/n2/n2_config.ini
/var/lib/litiaina/n2/state/n2_membership.json
/var/lib/litiaina/n2/certificates/

/var/lib/litiaina/n2-gateway/n2_gateway_config.ini
/var/lib/litiaina/n2-gateway/certificates/

/mnt/NDISK001/n2
/mnt/NDISK002/n2
```

Every configured N2 3.0 root is unified and can contain:

```text
<storage_root>/
  .n2-root.ncdx
  .fragment/
  @f_<stable-fragment-id>/
    .fragment.ncdx
    .namespace/
    .trash/
    .uploads/
    .objects/
```

`runtime_path` contains process-local membership and maintenance job state.
It is not namespace or object truth.

### Storage Configuration

Use explicit stable IDs:

```ini
[disk_config]
storage_roots=a-disk-1=/mnt/NDISK001/n2,a-disk-2=/mnt/NDISK002/n2
runtime_path=/var/lib/litiaina/n2/state
```

Never recycle a root ID for different media. Do not silently add a root to an
enrolled pool node; the pool's complete root-ID set is part of the static
format-v3 contract.

## Build

```bash
cargo fmt --all -- --check
cargo test --workspace --lib --bins --tests
cargo build --workspace --release
```

Binaries:

```text
target/release/litiaina-n2
target/release/litiaina-n2-shell
target/release/litiaina-n2-gateway
```

## Installer

The preferred production path is `deploy/install.sh`. It installs binaries,
creates runtime paths, renders systemd services, and can enable/start them.
A fresh 3.0 node requires explicit environment and membership sources.

Preview without changing the host:

```bash
sudo deploy/install.sh --node --dry-run \
  --node-id node-a \
  --node-endpoint https://10.0.0.11:50001 \
  --native-endpoint n2://10.0.0.11:50002 \
  --storage-roots a-disk-1=/mnt/NDISK001/n2,a-disk-2=/mnt/NDISK002/n2 \
  --state-root /var/lib/litiaina/n2/state \
  --pool-id pool-1 --pool-generation 1 \
  --data-shards 2 --parity-shards 2 \
  --env-src ./n2.env \
  --membership-src ./n2_membership.json \
  --cert-src ./certificates
```

Install a node after reviewing the dry run:

```bash
sudo deploy/install.sh --node --build --copy-certs \
  --node-id node-a \
  --node-endpoint https://10.0.0.11:50001 \
  --native-endpoint n2://10.0.0.11:50002 \
  --storage-roots a-disk-1=/mnt/NDISK001/n2,a-disk-2=/mnt/NDISK002/n2 \
  --state-root /var/lib/litiaina/n2/state \
  --pool-id pool-1 --pool-generation 1 \
  --data-shards 2 --parity-shards 2 \
  --env-src ./n2.env \
  --membership-src ./n2_membership.json \
  --cert-src ./certificates \
  --enable --start
```

Install the optional gateway:

```bash
sudo deploy/install.sh --gateway --build --copy-certs \
  --gateway-node node-a,https://10.0.0.11:50001,n2://10.0.0.11:50002 \
  --gateway-node node-b,https://10.0.0.12:50001,n2://10.0.0.12:50002 \
  --gateway-node node-c,https://10.0.0.13:50001,n2://10.0.0.13:50002 \
  --gateway-node node-d,https://10.0.0.14:50001,n2://10.0.0.14:50002 \
  --cert-src ./certificates --enable --start
```

The installer preserves existing config unless `--force-config` is supplied.
Do not use force mode without reviewing the exact generated diff.

## Node Runtime

Create the shared root-owned environment file with mode `0600`:

```dotenv
JWT_SECRET=<long-random-cluster-value>
N2_CLUSTER_SECRET=<different-long-random-cluster-value>
MASTER_KEY=<operator-master-key>
AUTH_KEY=<operator-master-key>
ADMIN_KEY=<operator-master-key>
ACCESS_TOKEN_DEFAULT_TTL_SECS=900
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
```

Create `n2_config.ini` on each node. Only identity, endpoints, and local
roots differ:

```ini
[server]
host=0.0.0.0
https_port=50001
native_port=50002
cert_path=/var/lib/litiaina/n2/certificates/cert.pem
key_path=/var/lib/litiaina/n2/certificates/key.pem

[disk_config]
storage_roots=a-disk-1=/mnt/NDISK001/n2,a-disk-2=/mnt/NDISK002/n2
runtime_path=/var/lib/litiaina/n2/state

[cluster_config]
node_id=node-a
pool_id=pool-1
pool_generation=1
node_endpoint=https://10.0.0.11:50001
native_endpoint=n2://10.0.0.11:50002
membership_file=/var/lib/litiaina/n2/state/n2_membership.json
object_data_shards=2
object_parity_shards=2
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
max_part_size=52428800
data_shards=2
parity_shards=2
stripe_block_size=262144
```

Upload EC must equal the pool policy. N2 rejects mismatched or unsafe
configuration instead of adapting durability downward.

Create the same complete format-v3 membership on all nodes:

```json
{
  "format_version": 3,
  "cluster_id": "cluster-a",
  "epoch": 1,
  "pools": [{
    "id": "pool-1",
    "generation": 1,
    "policy": {
      "object_data_shards": 2,
      "object_parity_shards": 2
    },
    "state": "writable",
    "node_ids": ["node-a", "node-b", "node-c", "node-d"]
  }],
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "https://10.0.0.11:50001",
      "native_endpoint": "n2://10.0.0.11:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"a-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    },
    {
      "id": "node-b",
      "endpoint": "https://10.0.0.12:50001",
      "native_endpoint": "n2://10.0.0.12:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"b-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    },
    {
      "id": "node-c",
      "endpoint": "https://10.0.0.13:50001",
      "native_endpoint": "n2://10.0.0.13:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"c-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    },
    {
      "id": "node-d",
      "endpoint": "https://10.0.0.14:50001",
      "native_endpoint": "n2://10.0.0.14:50002",
      "state": "online",
      "weight": 1,
      "storage_roots": [{"id":"d-disk-1","path":"/mnt/NDISK001/n2"}],
      "joined_epoch": 1
    }
  ]
}
```

Pool ID, generation, policy, node list, and root-ID sets are immutable after
enrollment. Expansion adds a complete new pool generation; it does not
rewrite existing objects.

## Systemd Service

`deploy/install.sh` renders the service automatically. Manual installations
replace these placeholders in `deploy/systemd/litiaina-n2.service.in`:

```text
@N2_HOME@
@N2_USER@
@N2_GROUP@
@N2_RUNTIME_DIR@
@N2_ENV_FILE@
@N2_BIN@
@N2_LOG_FILTER@
@N2_DATA_PATHS@
```

The service waits for `network-online.target`, reads the protected environment
file, restarts on failure, raises the file-descriptor limit, and grants write
access only to configured runtime/data paths.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now litiaina-n2
sudo systemctl status litiaina-n2 --no-pager
journalctl -u litiaina-n2 -n 100 --no-pager
```

Use `systemctl restart litiaina-n2` after an intentional reviewed membership
change. Live membership reload is deliberately disabled.

## Gateway Runtime

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
http_url=https://10.0.0.11:50001
native_url=n2://10.0.0.11:50002
weight=1

[node.node-b]
http_url=https://10.0.0.12:50001
native_url=n2://10.0.0.12:50002
weight=1
```

Add every N2 node. Multipart and maintenance jobs return
`coordinator_node_id`; clients must preserve it. The gateway must not gain a
session, namespace, or object database.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now litiaina-n2-gateway
sudo systemctl status litiaina-n2-gateway --no-pager
```

## Validation

Check every node directly:

```bash
curl --cacert /path/to/ca.pem https://10.0.0.11:50001/noa/v1/ping
curl --cacert /path/to/ca.pem https://10.0.0.12:50001/noa/v1/ping
curl --cacert /path/to/ca.pem https://10.0.0.13:50001/noa/v1/ping
curl --cacert /path/to/ca.pem https://10.0.0.14:50001/noa/v1/ping
```

Check native reachability from the gateway/operator host:

```bash
nc -vz 10.0.0.11 50002
nc -vz 10.0.0.12 50002
nc -vz 10.0.0.13 50002
nc -vz 10.0.0.14 50002
```

Check the optional gateway:

```bash
curl --cacert /path/to/ca.pem https://127.0.0.1:51002/gateway/health
curl --cacert /path/to/ca.pem https://127.0.0.1:51002/gateway/nodes
```

After fragment authentication, verify:

- cluster membership matches format-v3 on every node;
- health reports the expected static `K + M` and `M + 1` NCNX replicas;
- one-shot and multipart upload return byte-identical downloads;
- finalized objects read through every node;
- LIST pagination, exact rename, prefix rename, trash, and recover work;
- `/posix/mkdir` is rejected because empty directories are not persistent;
- one-node loss preserves reconstructable reads and rejects unsafe writes;
- NCNX and shard damage are repaired to their exact layouts;
- TTL GC reclaims expired trash without deleting live references;
- full restart preserves object bytes and namespace visibility.

Use [TESTING.md](TESTING.md) for the complete qualification procedure. Run
the installer proof with:

```bash
scripts/proof/run_n2_installer_proof.sh
```

All proof runs must use isolated roots and must never touch existing storage.
