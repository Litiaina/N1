# N2 3.0 Installation

N2 3.0 is a clean disk-format generation. Never point it at an N2 2.x root.
Build a new format-v3 cluster, copy the same membership and cluster secrets to
every node, and use stable root IDs for every mounted device.

## Production prerequisites

- Linux with stable storage mounts and synchronized time.
- One TLS certificate/key pair per endpoint, or a certificate valid for all
  node names.
- One stable node ID and one or more stable `ROOT_ID=/absolute/path` roots per
  node.
- A complete shared format-v3 membership document.
- Shared `JWT_SECRET` and `N2_CLUSTER_SECRET` values on every node.
- Exactly `K + M` distinct nodes in each writable static-EC server pool.

N2 counts nodes, not disks, as failure domains. Two roots on one host do not
replace two pool nodes.

## Build

```bash
cargo fmt --all -- --check
cargo test --workspace --lib --bins --tests
cargo build --release --workspace
```

The node binary is `target/release/litiaina-n2`; the optional gateway and shell
are `litiaina-n2-gateway` and `litiaina-n2-shell`.

## Shared environment

Create a root-owned `n2.env` and distribute the same file to every node:

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

Use mode `0600`. Do not place secrets in membership JSON or source control.

## Format-v3 membership

All nodes must start with the same complete membership. This four-node example
defines one static 2+2 pool:

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

Pool `node_ids`, every node's root-ID set, policy, ID, and generation are
immutable after enrollment. Expansion adds a new complete pool for future
writes; it does not reinterpret old NCNX or objects.

## Node configuration

Each node has its own `n2_config.ini`. Only node identity, endpoints, and local
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

The upload EC values must equal the static pool policy. N2 rejects unsafe or
inconsistent topology instead of adapting durability downward.

## Installer

The installer requires an explicit shared environment and membership for every
fresh node:

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

Run once with `--dry-run` first. The installer preserves existing configuration
unless `--force-config` is explicitly supplied.

## Validation

```bash
systemctl status litiaina-n2 --no-pager
journalctl -u litiaina-n2 -n 100 --no-pager
curl --cacert /path/to/ca.pem https://10.0.0.11:50001/noa/v1/ping
```

After authenticating, verify `/noa/v1/cluster/health` reports the expected
pool, exact `K + M`, NCNX replica count `M + 1`, and writable state. Then run a
bounded upload/read/checksum, restart, node-loss read, and fail-closed write
test before admitting production data.

Do not use the gateway as an additional durability layer. It is an optional
client routing endpoint; every N2 node already accepts and coordinates NOA.
