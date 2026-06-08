#!/usr/bin/env bash
set -euo pipefail

REMOTE_IP="${REMOTE_IP:-192.168.0.13}"
REMOTE_PASS="${REMOTE_PASS:-1122}"
REMOTE2_IP="${REMOTE2_IP:-192.168.0.18}"
REMOTE2_PASS="${REMOTE2_PASS:-Admin@@@123}"
N2_PROOF_NODE_COUNT="${N2_PROOF_NODE_COUNT:-3}"
N2_PROOF_ALLOW_PKILL="${N2_PROOF_ALLOW_PKILL:-0}"
LOCAL_IP="${LOCAL_IP:-192.168.0.22}"
LOCAL_HTTPS_PORT="${LOCAL_HTTPS_PORT:-53401}"
LOCAL_NATIVE_PORT="${LOCAL_NATIVE_PORT:-53402}"
REMOTE_HTTPS_PORT="${REMOTE_HTTPS_PORT:-53411}"
REMOTE_NATIVE_PORT="${REMOTE_NATIVE_PORT:-53412}"
REMOTE2_HTTPS_PORT="${REMOTE2_HTTPS_PORT:-53421}"
REMOTE2_NATIVE_PORT="${REMOTE2_NATIVE_PORT:-53422}"
LOCAL_HDD_ROOT="${LOCAL_HDD_ROOT:-/mnt/disk-00}"
REMOTE_HDD_ROOT="${REMOTE_HDD_ROOT:-/mnt/DISK0}"
REMOTE2_HDD_ROOT="${REMOTE2_HDD_ROOT:-/mnt/DISK2}"
LOCAL_NVME_ROOT="${LOCAL_NVME_ROOT:-/home/administrator/.local/share/litiaina-n2-proof}"
REMOTE_NVME_ROOT="${REMOTE_NVME_ROOT:-/home/administrator/.local/share/litiaina-n2-proof}"
REMOTE2_NVME_ROOT="${REMOTE2_NVME_ROOT:-/home/administrator/.local/share/litiaina-n2-proof}"
N2_PROOF_STORAGE_LAYOUT="${N2_PROOF_STORAGE_LAYOUT:-separated}"
SOURCE_VIDEO="${SOURCE_VIDEO:-/home/administrator/Downloads/mhw.mp4}"
PROOF_MIB="${PROOF_MIB:-256}"
SMALL_OBJECTS="${SMALL_OBJECTS:-120}"
NAMESPACE_OPS="${NAMESPACE_OPS:-40}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${N2_PROOF_BIN:-$ROOT_DIR/target/release/litiaina-n2}"
CERT="$ROOT_DIR/certificates/cert.pem"
KEY="$ROOT_DIR/certificates/key.pem"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT_DIR/.n2-bench/n2-hardening-native-proof-$TS"
LROOT="/tmp/n2-hardening-native-proof-$TS/node-a"
RROOT="/tmp/n2-hardening-native-proof-$TS/node-b"
CROOT="/tmp/n2-hardening-native-proof-$TS/node-c"
case "$N2_PROOF_STORAGE_LAYOUT" in
  separated)
    L_INDEX_ROOT="$LOCAL_NVME_ROOT/$TS/node-a/index"
    R_INDEX_ROOT="$REMOTE_NVME_ROOT/$TS/node-b/index"
    C_INDEX_ROOT="$REMOTE2_NVME_ROOT/$TS/node-c/index"
    L_BLOB_ROOT="$LOCAL_HDD_ROOT/n2-hardening-native-proof-$TS/node-a/blob"
    R_BLOB_ROOT="$REMOTE_HDD_ROOT/n2-hardening-native-proof-$TS/node-b/blob"
    C_BLOB_ROOT="$REMOTE2_HDD_ROOT/n2-hardening-native-proof-$TS/node-c/blob"
    ;;
  shared)
    L_INDEX_ROOT="$LOCAL_HDD_ROOT/n2-hardening-native-proof-$TS/node-a/storage"
    R_INDEX_ROOT="$REMOTE_HDD_ROOT/n2-hardening-native-proof-$TS/node-b/storage"
    C_INDEX_ROOT="$REMOTE2_HDD_ROOT/n2-hardening-native-proof-$TS/node-c/storage"
    L_BLOB_ROOT="$L_INDEX_ROOT"
    R_BLOB_ROOT="$R_INDEX_ROOT"
    C_BLOB_ROOT="$C_INDEX_ROOT"
    ;;
  *)
    echo "N2_PROOF_STORAGE_LAYOUT must be separated or shared" >&2
    exit 1
    ;;
esac
L_FS_ROOT="$LOCAL_NVME_ROOT/$TS/node-a/fs"
R_FS_ROOT="$REMOTE_NVME_ROOT/$TS/node-b/fs"
C_FS_ROOT="$REMOTE2_NVME_ROOT/$TS/node-c/fs"
LOCAL_BASE="https://$LOCAL_IP:$LOCAL_HTTPS_PORT"
REMOTE_BASE="https://$REMOTE_IP:$REMOTE_HTTPS_PORT"
REMOTE2_BASE="https://$REMOTE2_IP:$REMOTE2_HTTPS_PORT"
LOCAL_NATIVE="n2://$LOCAL_IP:$LOCAL_NATIVE_PORT"
REMOTE_NATIVE="n2://$REMOTE_IP:$REMOTE_NATIVE_PORT"
REMOTE2_NATIVE="n2://$REMOTE2_IP:$REMOTE2_NATIVE_PORT"
MASTER="n2-proof-master-key"
FRAG_NAME="n2-hardening-$TS"
FRAG_KEY="n2-frag-key-$TS"
TOKEN=""

if [[ "$N2_PROOF_NODE_COUNT" != "2" && "$N2_PROOF_NODE_COUNT" != "3" ]]; then
  echo "N2_PROOF_NODE_COUNT must be 2 or 3" >&2
  exit 1
fi

ssh_remote() {
  sshpass -p "$REMOTE_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 administrator@"$REMOTE_IP" "$@"
}

scp_remote() {
  sshpass -p "$REMOTE_PASS" scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$@"
}

ssh_remote2() {
  sshpass -p "$REMOTE2_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 administrator@"$REMOTE2_IP" "$@"
}

scp_remote2() {
  sshpass -p "$REMOTE2_PASS" scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$@"
}

cleanup() {
  set +e
  if [[ -f "$LROOT/rss-monitor.pid" ]]; then kill "$(cat "$LROOT/rss-monitor.pid")" 2>/dev/null; fi
  if [[ -f "$LROOT/n2.pid" ]]; then kill "$(cat "$LROOT/n2.pid")" 2>/dev/null; fi
  ssh_remote "if [ -f '$RROOT/rss-monitor.pid' ]; then kill \$(cat '$RROOT/rss-monitor.pid') 2>/dev/null || true; fi; if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; fi" >/dev/null 2>&1
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    ssh_remote2 "if [ -f '$CROOT/rss-monitor.pid' ]; then kill \$(cat '$CROOT/rss-monitor.pid') 2>/dev/null || true; fi; if [ -f '$CROOT/n2.pid' ]; then kill \$(cat '$CROOT/n2.pid') 2>/dev/null || true; fi" >/dev/null 2>&1
  fi
}
trap cleanup EXIT

write_env() {
  local path="$1"
  cat > "$path" <<ENV
JWT_SECRET=n2-proof-jwt-secret
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_CLUSTER_SECRET=n2-proof-cluster-secret
ENV
}

write_membership() {
  local path="$1"
  if [[ "$N2_PROOF_NODE_COUNT" == "2" ]]; then
    cat > "$path" <<JSON
{
  "cluster_id": "n2-hardening-$TS",
  "epoch": 2,
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "$LOCAL_BASE",
      "native_endpoint": "$LOCAL_NATIVE",
      "state": "online",
      "weight": 1,
      "index_roots": ["$L_INDEX_ROOT"],
      "blob_roots": ["$L_BLOB_ROOT"]
    },
    {
      "id": "node-b",
      "endpoint": "$REMOTE_BASE",
      "native_endpoint": "$REMOTE_NATIVE",
      "state": "online",
      "weight": 1,
      "index_roots": ["$R_INDEX_ROOT"],
      "blob_roots": ["$R_BLOB_ROOT"]
    }
  ]
}
JSON
    return
  fi
  cat > "$path" <<JSON
{
  "cluster_id": "n2-hardening-$TS",
  "epoch": 2,
  "nodes": [
    {
      "id": "node-a",
      "endpoint": "$LOCAL_BASE",
      "native_endpoint": "$LOCAL_NATIVE",
      "state": "online",
      "weight": 1,
      "index_roots": ["$L_INDEX_ROOT"],
      "blob_roots": ["$L_BLOB_ROOT"]
    },
    {
      "id": "node-b",
      "endpoint": "$REMOTE_BASE",
      "native_endpoint": "$REMOTE_NATIVE",
      "state": "online",
      "weight": 1,
      "index_roots": ["$R_INDEX_ROOT"],
      "blob_roots": ["$R_BLOB_ROOT"]
    },
    {
      "id": "node-c",
      "endpoint": "$REMOTE2_BASE",
      "native_endpoint": "$REMOTE2_NATIVE",
      "state": "online",
      "weight": 1,
      "index_roots": ["$C_INDEX_ROOT"],
      "blob_roots": ["$C_BLOB_ROOT"]
    }
  ]
}
JSON
}

write_config() {
  local path="$1" https_port="$2" native_port="$3" node_id="$4" node_endpoint="$5" native_endpoint="$6" run_root="$7" index_root="$8" blob_root="$9" fs_root="${10}"
  cat > "$path" <<CFG
[server]
host=0.0.0.0
https_port=$https_port
native_port=$native_port
cert_path=$run_root/cert.pem
key_path=$run_root/key.pem

[disk_config]
index_store_disks=$index_root
blob_store_disks=$blob_root
fs_storage_path=$fs_root

[cluster_config]
node_id=$node_id
node_endpoint=$node_endpoint
native_endpoint=$native_endpoint
membership_file=$fs_root/n2_membership.json
cluster_layout_mode=auto
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=2592000

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=3600

[rate_limiter_config]
enable_rate_limiting=true
auth_limit_per_second=50
auth_limit_burst_size=100
view_limit_per_second=500
view_limit_burst_size=1000
upload_limit_per_second=500
upload_limit_burst_size=2000
global_limit_per_second=1500
global_limit_burst_size=3000

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
max_part_size=67108864
data_shards=1
parity_shards=0
stripe_block_size=262144
CFG
}

curl_json() {
  curl -k --http1.1 -fsS -H "Content-Type: application/json" "$@"
}

curl_auth() {
  curl -k --http1.1 -fsS -H "Authorization: Bearer $TOKEN" "$@"
}

metric_speed() {
  awk -v b="$1" -v ns="$2" 'BEGIN{sec=ns/1000000000; if(sec<=0)sec=0.001; printf "%.2f", (b/1048576)/sec}'
}

wait_ping() {
  local base="$1"
  for _ in $(seq 1 60); do
    if curl -k --http1.1 -fsS "$base/n2/ping" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  curl -k --http1.1 -fsS "$base/n2/ping" >/dev/null
}

start_nodes() {
  ssh_remote "cd '$RROOT'; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo \$! > n2.pid"
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    ssh_remote2 "cd '$CROOT'; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo \$! > n2.pid"
  fi
  (cd "$LROOT"; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo $! > n2.pid)
  wait_ping "$LOCAL_BASE"
  wait_ping "$REMOTE_BASE"
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    wait_ping "$REMOTE2_BASE"
  fi
  (while kill -0 "$(cat "$LROOT/n2.pid")" 2>/dev/null; do
    printf '%s,%s\n' "$(date +%s)" "$(ps -o rss= -p "$(cat "$LROOT/n2.pid")" | tr -d ' ')"
    sleep 1
  done) > "$OUT/rss-local.csv" &
  echo $! > "$LROOT/rss-monitor.pid"
  ssh_remote "pid=\$(cat '$RROOT/n2.pid'); nohup sh -c 'while kill -0 \"\$0\" 2>/dev/null; do printf \"%s,%s\\\\n\" \$(date +%s) \$(ps -o rss= -p \"\$0\" | tr -d \" \"); sleep 1; done' \"\$pid\" > '$RROOT/rss-remote.csv' 2>/dev/null & echo \$! > '$RROOT/rss-monitor.pid'"
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    ssh_remote2 "pid=\$(cat '$CROOT/n2.pid'); nohup sh -c 'while kill -0 \"\$0\" 2>/dev/null; do printf \"%s,%s\\\\n\" \$(date +%s) \$(ps -o rss= -p \"\$0\" | tr -d \" \"); sleep 1; done' \"\$pid\" > '$CROOT/rss-remote2.csv' 2>/dev/null & echo \$! > '$CROOT/rss-monitor.pid'"
  fi
}

stop_nodes() {
  if [[ -f "$LROOT/rss-monitor.pid" ]]; then kill "$(cat "$LROOT/rss-monitor.pid")" 2>/dev/null || true; rm -f "$LROOT/rss-monitor.pid"; fi
  if [[ -f "$LROOT/n2.pid" ]]; then kill "$(cat "$LROOT/n2.pid")" 2>/dev/null || true; rm -f "$LROOT/n2.pid"; fi
  ssh_remote "if [ -f '$RROOT/rss-monitor.pid' ]; then kill \$(cat '$RROOT/rss-monitor.pid') 2>/dev/null || true; rm -f '$RROOT/rss-monitor.pid'; fi; if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; rm -f '$RROOT/n2.pid'; fi" >/dev/null || true
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    ssh_remote2 "if [ -f '$CROOT/rss-monitor.pid' ]; then kill \$(cat '$CROOT/rss-monitor.pid') 2>/dev/null || true; rm -f '$CROOT/rss-monitor.pid'; fi; if [ -f '$CROOT/n2.pid' ]; then kill \$(cat '$CROOT/n2.pid') 2>/dev/null || true; rm -f '$CROOT/n2.pid'; fi" >/dev/null || true
  fi
  sleep 2
}

stop_remote_node_b() {
  ssh_remote "if [ -f '$RROOT/rss-monitor.pid' ]; then kill \$(cat '$RROOT/rss-monitor.pid') 2>/dev/null || true; rm -f '$RROOT/rss-monitor.pid'; fi; if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; rm -f '$RROOT/n2.pid'; fi" >/dev/null || true
  sleep 2
}

start_remote_node_b() {
  ssh_remote "cd '$RROOT'; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo \$! > n2.pid"
  wait_ping "$REMOTE_BASE"
  ssh_remote "pid=\$(cat '$RROOT/n2.pid'); nohup sh -c 'while kill -0 \"\$0\" 2>/dev/null; do printf \"%s,%s\\\\n\" \$(date +%s) \$(ps -o rss= -p \"\$0\" | tr -d \" \"); sleep 1; done' \"\$pid\" >> '$RROOT/rss-remote.csv' 2>/dev/null & echo \$! > '$RROOT/rss-monitor.pid'"
}

wait_job() {
  local url="$1" out="$2"
  for _ in $(seq 1 180); do
    curl_auth "$url" -o "$out"
    [[ "$(jq -r '.running' "$out")" == "false" ]] && return 0
    sleep 1
  done
  echo "job did not finish: $url" >&2
  return 1
}

rss_summary() {
  awk -F, 'NF>=2 && $2 ~ /^[0-9]+$/ {rss=$2/1024; if(n==0||rss<min)min=rss; if(rss>max)max=rss; sum+=rss; n++} END{if(n>0)printf "{\"samples\":%d,\"min_mib\":%.2f,\"avg_mib\":%.2f,\"max_mib\":%.2f}",n,min,sum/n,max; else printf "{\"samples\":0}"}' "$1"
}

echo "[n2-proof] output=$OUT"
echo "[n2-proof] storage_layout=$N2_PROOF_STORAGE_LAYOUT"
if [[ "$N2_PROOF_ALLOW_PKILL" == "1" ]]; then
  pkill -x litiaina-n2 2>/dev/null || true
  ssh_remote "pkill -x litiaina-n2 2>/dev/null || true" >/dev/null || true
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    ssh_remote2 "pkill -x litiaina-n2 2>/dev/null || true" >/dev/null || true
  fi
fi
mkdir -p "$OUT" "$LROOT" "$L_INDEX_ROOT" "$L_BLOB_ROOT" "$L_FS_ROOT"
cp "$BIN" "$CERT" "$KEY" "$LROOT/"
ssh_remote "rm -rf '$RROOT' '$R_INDEX_ROOT' '$R_BLOB_ROOT' '$R_FS_ROOT' && mkdir -p '$RROOT' '$R_INDEX_ROOT' '$R_BLOB_ROOT' '$R_FS_ROOT'"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  ssh_remote2 "rm -rf '$CROOT' '$C_INDEX_ROOT' '$C_BLOB_ROOT' '$C_FS_ROOT' && mkdir -p '$CROOT' '$C_INDEX_ROOT' '$C_BLOB_ROOT' '$C_FS_ROOT'"
fi
scp_remote "$BIN" "$CERT" "$KEY" administrator@"$REMOTE_IP":"$RROOT/" >/dev/null
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  scp_remote2 "$BIN" "$CERT" "$KEY" administrator@"$REMOTE2_IP":"$CROOT/" >/dev/null
fi
write_env "$LROOT/n2.env"
scp_remote "$LROOT/n2.env" administrator@"$REMOTE_IP":"$RROOT/n2.env" >/dev/null
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  scp_remote2 "$LROOT/n2.env" administrator@"$REMOTE2_IP":"$CROOT/n2.env" >/dev/null
fi
write_membership "$L_FS_ROOT/n2_membership.json"
scp_remote "$L_FS_ROOT/n2_membership.json" administrator@"$REMOTE_IP":"$R_FS_ROOT/n2_membership.json" >/dev/null
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  scp_remote2 "$L_FS_ROOT/n2_membership.json" administrator@"$REMOTE2_IP":"$C_FS_ROOT/n2_membership.json" >/dev/null
fi
write_config "$LROOT/n2_config.ini" "$LOCAL_HTTPS_PORT" "$LOCAL_NATIVE_PORT" node-a "$LOCAL_BASE" "$LOCAL_NATIVE" "$LROOT" "$L_INDEX_ROOT" "$L_BLOB_ROOT" "$L_FS_ROOT"
write_config "$OUT/node-b-config.ini" "$REMOTE_HTTPS_PORT" "$REMOTE_NATIVE_PORT" node-b "$REMOTE_BASE" "$REMOTE_NATIVE" "$RROOT" "$R_INDEX_ROOT" "$R_BLOB_ROOT" "$R_FS_ROOT"
scp_remote "$OUT/node-b-config.ini" administrator@"$REMOTE_IP":"$RROOT/n2_config.ini" >/dev/null
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  write_config "$OUT/node-c-config.ini" "$REMOTE2_HTTPS_PORT" "$REMOTE2_NATIVE_PORT" node-c "$REMOTE2_BASE" "$REMOTE2_NATIVE" "$CROOT" "$C_INDEX_ROOT" "$C_BLOB_ROOT" "$C_FS_ROOT"
  scp_remote2 "$OUT/node-c-config.ini" administrator@"$REMOTE2_IP":"$CROOT/n2_config.ini" >/dev/null
fi

start_nodes

curl_json -X POST "$LOCAL_BASE/n2/fragments" -d "{\"name\":\"$FRAG_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAG_KEY\"}" > "$OUT/create-local.json"
FRAGMENT="$(jq -r '.fragment_hash' "$OUT/create-local.json")"
STABLE_FRAGMENT="$(jq -r '.stable_fragment_id' "$OUT/create-local.json")"
curl_json -X POST "$REMOTE_BASE/n2/auth/fragment" -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAG_KEY\",\"read\":true}" > "$OUT/create-remote.json"
jq -e '.access_token | type == "string"' "$OUT/create-remote.json" >/dev/null
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  curl_json -X POST "$REMOTE2_BASE/n2/auth/fragment" -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAG_KEY\",\"read\":true}" > "$OUT/create-remote2.json"
  jq -e '.access_token | type == "string"' "$OUT/create-remote2.json" >/dev/null
else
  printf '{}\n' > "$OUT/create-remote2.json"
fi
curl_json -X POST "$LOCAL_BASE/n2/auth/fragment" -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAG_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" > "$OUT/auth-local.json"
TOKEN="$(jq -r '.access_token' "$OUT/auth-local.json")"
curl_auth "$LOCAL_BASE/n2/cluster/health" > "$OUT/cluster-health-initial.json"

if [[ -f "$SOURCE_VIDEO" ]]; then
  dd if="$SOURCE_VIDEO" of="$OUT/mhw-proof.bin" bs=1M count="$PROOF_MIB" status=none
else
  dd if=/dev/urandom of="$OUT/mhw-proof.bin" bs=1M count="$PROOF_MIB" status=none
fi
sha256sum "$OUT/mhw-proof.bin" > "$OUT/source.sha256"
split -b 64M -d -a 2 "$OUT/mhw-proof.bin" "$OUT/part-"

KEY_MP="videos/mhw-native-resume-$TS.mp4"
NODE_LOSS_OBJECT="node-loss/upload-$TS.bin"
SIZE="$(stat -c%s "$OUT/mhw-proof.bin")"
PART=67108864
EXPECTED_PARTS=$(( (SIZE + PART - 1) / PART ))
RESUME_SPLIT=$(( EXPECTED_PARTS / 2 ))
if (( RESUME_SPLIT < 1 )); then RESUME_SPLIT=1; fi
curl_json -X POST "$LOCAL_BASE/n2/upload/multipart/init/$KEY_MP" -H "Authorization: Bearer $TOKEN" -d "{\"content_type\":\"video/mp4\",\"total_size_bytes\":$SIZE,\"expected_parts\":$EXPECTED_PARTS,\"part_size_bytes\":$PART}" > "$OUT/multipart-init.json"
VERSION="$(jq -r '.version_id' "$OUT/multipart-init.json")"
SESSION="$(jq -r '.session_id' "$OUT/multipart-init.json")"
mp_start="$(date +%s%N)"
for i in $(seq 0 $((RESUME_SPLIT - 1))); do
  part_file="$OUT/part-$(printf '%02d' "$i")"
  curl_auth -X PUT "$LOCAL_BASE/n2/upload/multipart/$VERSION/$i" --data-binary @"$part_file" -o "$OUT/part-$i.json"
done
curl_auth "$LOCAL_BASE/n2/upload/multipart/$VERSION" > "$OUT/multipart-status-before-restart.json"
stop_nodes
start_nodes
for i in $(seq "$RESUME_SPLIT" $((EXPECTED_PARTS - 1))); do
  part_file="$OUT/part-$(printf '%02d' "$i")"
  curl_auth -X PUT "$LOCAL_BASE/n2/upload/multipart/$VERSION/$i" --data-binary @"$part_file" -o "$OUT/part-$i.json"
done
curl_json -X POST "$LOCAL_BASE/n2/upload/multipart/$VERSION/$SESSION/finalize?object_key=$KEY_MP" -H "Authorization: Bearer $TOKEN" -d "{\"content_type\":\"video/mp4\",\"total_size_bytes\":$SIZE,\"expected_parts\":$EXPECTED_PARTS,\"part_size_bytes\":$PART}" > "$OUT/multipart-finalize.json"
mp_ns=$(( "$(date +%s%N)" - mp_start ))
printf '{"bytes":%s,"seconds":%.3f,"mib_per_sec":%s}\n' "$SIZE" "$(awk -v ns="$mp_ns" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(metric_speed "$SIZE" "$mp_ns")" > "$OUT/multipart-resume-upload-metrics.json"

SIDES=(local remote)
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  SIDES+=(remote2)
fi
for side in "${SIDES[@]}"; do
  if [[ "$side" == "local" ]]; then
    base="$LOCAL_BASE"
  elif [[ "$side" == "remote" ]]; then
    base="$REMOTE_BASE"
  else
    base="$REMOTE2_BASE"
  fi
  start="$(date +%s%N)"
  curl_auth -G "$base/n2/objects/download" --data-urlencode "object_key=$KEY_MP" -o "$OUT/download-$side.bin"
  ns=$(( "$(date +%s%N)" - start ))
  sha256sum "$OUT/download-$side.bin" > "$OUT/download-$side.sha256"
  printf '{"bytes":%s,"seconds":%.3f,"mib_per_sec":%s}\n' "$SIZE" "$(awk -v ns="$ns" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(metric_speed "$SIZE" "$ns")" > "$OUT/download-$side-metrics.json"
done
cmp -s "$OUT/mhw-proof.bin" "$OUT/download-local.bin"
cmp -s "$OUT/mhw-proof.bin" "$OUT/download-remote.bin"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  cmp -s "$OUT/mhw-proof.bin" "$OUT/download-remote2.bin"
else
  printf '{}\n' > "$OUT/download-remote2-metrics.json"
fi

if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  stop_remote_node_b
  curl_auth "$LOCAL_BASE/n2/cluster/health" > "$OUT/node-loss-health-down.json"
  start="$(date +%s%N)"
  curl_auth -G "$LOCAL_BASE/n2/objects/download" --data-urlencode "object_key=$KEY_MP" -o "$OUT/node-loss-download.bin"
  loss_download_ns=$(( "$(date +%s%N)" - start ))
  cmp -s "$OUT/mhw-proof.bin" "$OUT/node-loss-download.bin"
  printf '{"bytes":%s,"seconds":%.3f,"mib_per_sec":%s,"lost_node":"node-b"}\n' "$SIZE" "$(awk -v ns="$loss_download_ns" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(metric_speed "$SIZE" "$loss_download_ns")" > "$OUT/node-loss-download-metrics.json"
  curl_auth -G "$LOCAL_BASE/n2/posix/list" --data-urlencode 'path=videos' --data-urlencode 'limit=100' > "$OUT/node-loss-list.json"
  NODE_LOSS_RENAMED="videos/node-loss-rename-$TS.mp4"
  curl_json -X POST "$LOCAL_BASE/n2/posix/rename" -H "Authorization: Bearer $TOKEN" -d "{\"source_path\":\"$KEY_MP\",\"destination_path\":\"$NODE_LOSS_RENAMED\"}" > "$OUT/node-loss-rename-forward.json"
  curl_json -X POST "$LOCAL_BASE/n2/posix/rename" -H "Authorization: Bearer $TOKEN" -d "{\"source_path\":\"$NODE_LOSS_RENAMED\",\"destination_path\":\"$KEY_MP\"}" > "$OUT/node-loss-rename-back.json"
  dd if=/dev/urandom of="$OUT/node-loss-small.bin" bs=4096 count=1 status=none
  start="$(date +%s%N)"
  node_loss_upload_status="$(curl -k --http1.1 -m 20 -sS -o "$OUT/node-loss-upload-response.json" -w '%{http_code}' -X POST "$LOCAL_BASE/n2/upload/one-shot/$NODE_LOSS_OBJECT" -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/node-loss-small.bin" || true)"
  node_loss_upload_ns=$(( "$(date +%s%N)" - start ))
  printf '{"status":"%s","seconds":%.3f,"bounded":true}\n' "$node_loss_upload_status" "$(awk -v ns="$node_loss_upload_ns" 'BEGIN{printf "%.3f", ns/1000000000}')" > "$OUT/node-loss-upload-bounded.json"
  start_remote_node_b
  curl_auth "$LOCAL_BASE/n2/cluster/health" > "$OUT/node-loss-health-restored.json"
else
  printf '{}\n' > "$OUT/node-loss-health-down.json"
  printf '{}\n' > "$OUT/node-loss-download-metrics.json"
  printf '{}\n' > "$OUT/node-loss-list.json"
  printf '{}\n' > "$OUT/node-loss-rename-forward.json"
  printf '{}\n' > "$OUT/node-loss-rename-back.json"
  printf '{}\n' > "$OUT/node-loss-upload-bounded.json"
  printf '{}\n' > "$OUT/node-loss-health-restored.json"
fi

curl_auth -G "$LOCAL_BASE/n2/objects/status/$KEY_MP" --data-urlencode 'mode=availability' > "$OUT/object-status-before-repair.json"
jq -e '.status == "healthy" and .repairable == true' "$OUT/object-status-before-repair.json" >/dev/null

find "$L_BLOB_ROOT" -path '*/@o_*/*' -name "$VERSION*.ncdx" -print > "$OUT/main-version-ncdx-local.txt"
ssh_remote "find '$R_BLOB_ROOT' -path '*/@o_*/*' -name '$VERSION*.ncdx' -print" > "$OUT/main-version-ncdx-remote.txt"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  ssh_remote2 "find '$C_BLOB_ROOT' -path '*/@o_*/*' -name '$VERSION*.ncdx' -print" > "$OUT/main-version-ncdx-remote2.txt"
else
  : > "$OUT/main-version-ncdx-remote2.txt"
fi
for file in "$OUT/main-version-ncdx-local.txt" "$OUT/main-version-ncdx-remote.txt"; do
  [[ "$(wc -l < "$file")" -eq 1 ]] || {
    echo "expected one colocated metadata record for $VERSION in $file" >&2
    exit 1
  }
done
if [[ "$N2_PROOF_NODE_COUNT" == "3" && "$(wc -l < "$OUT/main-version-ncdx-remote2.txt")" -ne 1 ]]; then
  echo "expected one colocated metadata record for $VERSION on node-c" >&2
  exit 1
fi
LOCAL_NCDX="$(cat "$OUT/main-version-ncdx-local.txt")"
REMOTE_NCDX="$(cat "$OUT/main-version-ncdx-remote.txt")"
LOCAL_NCDX_SHA="$(sha256sum "$LOCAL_NCDX" | awk '{print $1}')"
REMOTE_NCDX_SHA="$(ssh_remote "sha256sum '$REMOTE_NCDX' | cut -d ' ' -f 1")"
[[ "$LOCAL_NCDX_SHA" == "$REMOTE_NCDX_SHA" ]] || {
  echo "immutable .ncdx copies diverged between node-a and node-b" >&2
  exit 1
}
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  REMOTE2_NCDX="$(cat "$OUT/main-version-ncdx-remote2.txt")"
  REMOTE2_NCDX_SHA="$(ssh_remote2 "sha256sum '$REMOTE2_NCDX' | cut -d ' ' -f 1")"
  [[ "$LOCAL_NCDX_SHA" == "$REMOTE2_NCDX_SHA" ]] || {
    echo "immutable .ncdx copies diverged between node-a and node-c" >&2
    exit 1
  }
fi
printf '%s\n' "$LOCAL_NCDX_SHA" > "$OUT/main-version-ncdx.sha256"

curl_auth -X POST -G "$LOCAL_BASE/n2/objects/repair/$KEY_MP" --data-urlencode 'mode=scrub' > "$OUT/object-repair.json"
curl_auth -G "$REMOTE_BASE/n2/objects/status/$KEY_MP" --data-urlencode 'mode=availability' > "$OUT/object-status-after-repair-remote.json"
jq -e '.status == "healthy"' "$OUT/object-status-after-repair-remote.json" >/dev/null

curl_auth -X DELETE "$REMOTE_BASE/n2/objects/$KEY_MP" > "$OUT/trash-object-remote.json"
curl_auth -G "$LOCAL_BASE/n2/trash" --data-urlencode 'prefix=videos/' --data-urlencode 'limit=100' > "$OUT/trash-list-local.json"
jq -e --arg key "$KEY_MP" '.objects[]? | select(.object_key == $key)' "$OUT/trash-list-local.json" >/dev/null
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  RECOVER_BASE="$REMOTE2_BASE"
else
  RECOVER_BASE="$LOCAL_BASE"
fi
curl_auth -X POST "$RECOVER_BASE/n2/objects/recover/$KEY_MP" > "$OUT/recover-object.json"
curl_auth -G "$REMOTE_BASE/n2/objects/download" --data-urlencode "object_key=$KEY_MP" -o "$OUT/read-after-recover.bin"
cmp -s "$OUT/mhw-proof.bin" "$OUT/read-after-recover.bin"

ns_start="$(date +%s%N)"
for i in $(seq -w 1 "$NAMESPACE_OPS"); do
  curl_json -X POST "$LOCAL_BASE/n2/posix/mkdir" -H "Authorization: Bearer $TOKEN" -d "{\"path\":\"stress/dir-$i\"}" >/dev/null
  curl_json -X POST "$LOCAL_BASE/n2/posix/rename" -H "Authorization: Bearer $TOKEN" -d "{\"source_path\":\"stress/dir-$i\",\"destination_path\":\"stress/renamed-$i\"}" >/dev/null
done
ns_total=$(( NAMESPACE_OPS * 2 ))
ns_elapsed=$(( "$(date +%s%N)" - ns_start ))
printf '{"operations":%s,"seconds":%.3f,"ops_per_sec":%.2f}\n' "$ns_total" "$(awk -v ns="$ns_elapsed" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(awk -v ops="$ns_total" -v ns="$ns_elapsed" 'BEGIN{sec=ns/1000000000; printf "%.2f", ops/sec}')" > "$OUT/namespace-stress-metrics.json"
curl_auth -G "$REMOTE_BASE/n2/posix/list" --data-urlencode 'path=stress' --data-urlencode 'limit=1000' > "$OUT/namespace-stress-remote-list.json"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  curl_auth -G "$REMOTE2_BASE/n2/posix/list" --data-urlencode 'path=stress' --data-urlencode 'limit=1000' > "$OUT/namespace-stress-remote2-list.json"
else
  printf '{}\n' > "$OUT/namespace-stress-remote2-list.json"
fi

SMALL="$OUT/small.bin"
dd if=/dev/urandom of="$SMALL" bs=4096 count=1 status=none
scale_start="$(date +%s%N)"
scale_pids=()
for i in $(seq -w 1 "$SMALL_OBJECTS"); do
  curl_auth -X POST "$LOCAL_BASE/n2/upload/one-shot/scale/obj-$i.bin" --data-binary @"$SMALL" -o /dev/null &
  scale_pids+=("$!")
  if (( 10#$i % 12 == 0 )); then
    for pid in "${scale_pids[@]}"; do wait "$pid"; done
    scale_pids=()
  fi
done
for pid in "${scale_pids[@]}"; do wait "$pid"; done
scale_elapsed=$(( "$(date +%s%N)" - scale_start ))
printf '{"objects":%s,"bytes_per_object":4096,"seconds":%.3f,"objects_per_sec":%.2f}\n' "$SMALL_OBJECTS" "$(awk -v ns="$scale_elapsed" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(awk -v objects="$SMALL_OBJECTS" -v ns="$scale_elapsed" 'BEGIN{sec=ns/1000000000; printf "%.2f", objects/sec}')" > "$OUT/scale-upload-metrics.json"
curl_auth -G "$REMOTE_BASE/n2/posix/list" --data-urlencode 'path=scale' --data-urlencode 'limit=1000' > "$OUT/scale-remote-list.json"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  curl_auth -G "$REMOTE2_BASE/n2/posix/list" --data-urlencode 'path=scale' --data-urlencode 'limit=1000' > "$OUT/scale-remote2-list.json"
else
  printf '{}\n' > "$OUT/scale-remote2-list.json"
fi

curl_json -X POST "$LOCAL_BASE/n2/upload/multipart/init/tmp/cancel-$TS.bin" -H "Authorization: Bearer $TOKEN" -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":134217728,\"expected_parts\":2,\"part_size_bytes\":67108864}" > "$OUT/cancel-init.json"
CVERSION="$(jq -r '.version_id' "$OUT/cancel-init.json")"
curl_auth -X DELETE -G "$LOCAL_BASE/n2/upload/multipart/$CVERSION" --data-urlencode "object_key=tmp/cancel-$TS.bin" -o "$OUT/cancel-response.json"
curl_auth "$LOCAL_BASE/n2/upload/multipart/$CVERSION" > "$OUT/cancel-status.json" || true

find "$L_INDEX_ROOT" "$L_BLOB_ROOT" \( -path '*/.cluster/*' -o -path '*/.namespace/*' -o -path '*/.intents/*' -o -path '*/.blobs/*' -o -name '*.nmtd' -o -name '*.ndx' \) -print | sort -u > "$OUT/forbidden-layout-local.txt"
ssh_remote "find '$R_INDEX_ROOT' '$R_BLOB_ROOT' \( -path '*/.cluster/*' -o -path '*/.namespace/*' -o -path '*/.intents/*' -o -path '*/.blobs/*' -o -name '*.nmtd' -o -name '*.ndx' \) -print | sort -u" > "$OUT/forbidden-layout-remote.txt"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  ssh_remote2 "find '$C_INDEX_ROOT' '$C_BLOB_ROOT' \( -path '*/.cluster/*' -o -path '*/.namespace/*' -o -path '*/.intents/*' -o -path '*/.blobs/*' -o -name '*.nmtd' -o -name '*.ndx' \) -print | sort -u" > "$OUT/forbidden-layout-remote2.txt"
else
  : > "$OUT/forbidden-layout-remote2.txt"
fi
if [[ -s "$OUT/forbidden-layout-local.txt" || -s "$OUT/forbidden-layout-remote.txt" || -s "$OUT/forbidden-layout-remote2.txt" ]]; then
  echo "forbidden old N2 layout paths found" >&2
  cat "$OUT"/forbidden-layout-*.txt >&2
  exit 1
fi

if [[ "$N2_PROOF_STORAGE_LAYOUT" == "separated" ]]; then
  find "$L_INDEX_ROOT" \( \( -path '*/.objects/*' -name '*.ncdx' \) -o -name '*.shard' -o -name '*.parity' \) -print > "$OUT/object-data-on-index-local.txt"
  ssh_remote "find '$R_INDEX_ROOT' \( \( -path '*/.objects/*' -name '*.ncdx' \) -o -name '*.shard' -o -name '*.parity' \) -print" > "$OUT/object-data-on-index-remote.txt"
  if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
    ssh_remote2 "find '$C_INDEX_ROOT' \( \( -path '*/.objects/*' -name '*.ncdx' \) -o -name '*.shard' -o -name '*.parity' \) -print" > "$OUT/object-data-on-index-remote2.txt"
  else
    : > "$OUT/object-data-on-index-remote2.txt"
  fi
  if [[ -s "$OUT/object-data-on-index-local.txt" || -s "$OUT/object-data-on-index-remote.txt" || -s "$OUT/object-data-on-index-remote2.txt" ]]; then
    echo "immutable object data leaked onto a separated namespace/index root" >&2
    cat "$OUT"/object-data-on-index-*.txt >&2
    exit 1
  fi
fi

find "$L_BLOB_ROOT" -name '*.ncdx' -print > "$OUT/ncdx-local.txt"
ssh_remote "find '$R_BLOB_ROOT' -name '*.ncdx' -print" > "$OUT/ncdx-remote.txt"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  ssh_remote2 "find '$C_BLOB_ROOT' -name '*.ncdx' -print" > "$OUT/ncdx-remote2.txt"
else
  : > "$OUT/ncdx-remote2.txt"
fi
NCDX_TOTAL=$(( $(wc -l < "$OUT/ncdx-local.txt") + $(wc -l < "$OUT/ncdx-remote.txt") + $(wc -l < "$OUT/ncdx-remote2.txt") ))
if (( NCDX_TOTAL == 0 )); then
  echo "no canonical .ncdx metadata found on shard-bearing disks" >&2
  exit 1
fi

find "$L_BLOB_ROOT" -name '*.ncdx' -exec strings -a {} + 2>/dev/null | grep -E 'n2-peer|n2%3A' > "$OUT/native-peer-locator-strings.txt" || true
ssh_remote "find '$R_BLOB_ROOT' -name '*.ncdx' -exec strings -a {} + 2>/dev/null | grep -E 'n2-peer|n2%3A' || true" >> "$OUT/native-peer-locator-strings.txt"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  ssh_remote2 "find '$C_BLOB_ROOT' -name '*.ncdx' -exec strings -a {} + 2>/dev/null | grep -E 'n2-peer|n2%3A' || true" >> "$OUT/native-peer-locator-strings.txt"
fi
NATIVE_LOCATORS="$(wc -l < "$OUT/native-peer-locator-strings.txt" | tr -d ' ')"

find "$L_BLOB_ROOT" -path '*/.objects/*/@o_*/*/*' -type f \( -name '*.shard' -o -name '*.parity' \) | sort > "$OUT/local-shard-candidates.txt"
head -1 "$OUT/local-shard-candidates.txt" > "$OUT/deleted-shard.txt"
DELETED="$(cat "$OUT/deleted-shard.txt")"
if [[ -z "$DELETED" ]]; then
  echo "no local cluster shard found for deletion proof" >&2
  exit 1
fi
rm -f "$DELETED"
start="$(date +%s%N)"
curl_auth -G "$LOCAL_BASE/n2/objects/download" --data-urlencode "object_key=$KEY_MP" -o "$OUT/reconstruct-after-delete.bin"
recon_elapsed=$(( "$(date +%s%N)" - start ))
cmp -s "$OUT/mhw-proof.bin" "$OUT/reconstruct-after-delete.bin"
printf '{"bytes":%s,"seconds":%.3f,"mib_per_sec":%s,"deleted_shard":"%s"}\n' "$SIZE" "$(awk -v ns="$recon_elapsed" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(metric_speed "$SIZE" "$recon_elapsed")" "$DELETED" > "$OUT/reconstruct-after-delete-metrics.json"

curl_json -X POST "$LOCAL_BASE/n2/cluster/repair/start" -H "Authorization: Bearer $TOKEN" -d "{\"fragment\":\"$FRAGMENT\",\"prefix\":\"videos/\",\"dry_run\":false}" > "$OUT/cluster-repair-start.json"
wait_job "$LOCAL_BASE/n2/cluster/repair/status" "$OUT/cluster-repair-status.json"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  curl_auth -G "$LOCAL_BASE/n2/objects/status/$NODE_LOSS_OBJECT" --data-urlencode 'mode=availability' > "$OUT/targeted-rebalance-before.json"
  jq -e '.layout.state == "older_recorded_layout"' "$OUT/targeted-rebalance-before.json" >/dev/null
fi
curl_json -X POST "$LOCAL_BASE/n2/cluster/rebalance/start" -H "Authorization: Bearer $TOKEN" -d "{\"fragment\":\"$FRAGMENT\",\"prefix\":\"node-loss/\",\"dry_run\":false}" > "$OUT/cluster-rebalance-start.json"
wait_job "$LOCAL_BASE/n2/cluster/rebalance/status" "$OUT/cluster-rebalance-status.json"
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  curl_auth -G "$LOCAL_BASE/n2/objects/status/$NODE_LOSS_OBJECT" --data-urlencode 'mode=availability' > "$OUT/targeted-rebalance-after.json"
  jq -e '.status == "healthy" and .layout.state == "current_layout"' "$OUT/targeted-rebalance-after.json" >/dev/null
  jq -e '.rewritten == 1 and .failed == 0' "$OUT/cluster-rebalance-status.json" >/dev/null
else
  printf '{}\n' > "$OUT/targeted-rebalance-before.json"
  printf '{}\n' > "$OUT/targeted-rebalance-after.json"
fi
curl_auth "$LOCAL_BASE/n2/cluster/health" > "$OUT/cluster-health-final.json"

scp_remote administrator@"$REMOTE_IP":"$RROOT/rss-remote.csv" "$OUT/rss-remote.csv" >/dev/null || true
if [[ "$N2_PROOF_NODE_COUNT" == "3" ]]; then
  scp_remote2 administrator@"$REMOTE2_IP":"$CROOT/rss-remote2.csv" "$OUT/rss-remote2.csv" >/dev/null || true
else
  : > "$OUT/rss-remote2.csv"
fi
rss_summary "$OUT/rss-local.csv" > "$OUT/rss-local-summary.json"
rss_summary "$OUT/rss-remote.csv" > "$OUT/rss-remote-summary.json"
rss_summary "$OUT/rss-remote2.csv" > "$OUT/rss-remote2-summary.json"

jq -n \
  --arg out "$OUT" \
  --arg storage_layout "$N2_PROOF_STORAGE_LAYOUT" \
  --arg fragment "$FRAGMENT" \
  --arg stable_fragment "$STABLE_FRAGMENT" \
  --arg local_index_root "$L_INDEX_ROOT" \
  --arg local_blob_root "$L_BLOB_ROOT" \
  --arg remote_index_root "$R_INDEX_ROOT" \
  --arg remote_blob_root "$R_BLOB_ROOT" \
  --arg remote2_index_root "$C_INDEX_ROOT" \
  --arg remote2_blob_root "$C_BLOB_ROOT" \
  --argjson ncdx_records "$NCDX_TOTAL" \
  --argjson native_locators "$NATIVE_LOCATORS" \
  --slurpfile upload "$OUT/multipart-resume-upload-metrics.json" \
  --slurpfile dl "$OUT/download-local-metrics.json" \
  --slurpfile dr "$OUT/download-remote-metrics.json" \
  --slurpfile dr2 "$OUT/download-remote2-metrics.json" \
  --slurpfile ns "$OUT/namespace-stress-metrics.json" \
  --slurpfile scale "$OUT/scale-upload-metrics.json" \
  --slurpfile object_status "$OUT/object-status-before-repair.json" \
  --slurpfile object_repair "$OUT/object-repair.json" \
  --slurpfile recon "$OUT/reconstruct-after-delete-metrics.json" \
  --slurpfile node_loss_health "$OUT/node-loss-health-down.json" \
  --slurpfile node_loss_download "$OUT/node-loss-download-metrics.json" \
  --slurpfile node_loss_upload "$OUT/node-loss-upload-bounded.json" \
  --slurpfile node_loss_restored "$OUT/node-loss-health-restored.json" \
  --slurpfile trash "$OUT/trash-object-remote.json" \
  --slurpfile recover "$OUT/recover-object.json" \
  --slurpfile repair "$OUT/cluster-repair-status.json" \
  --slurpfile rebalance "$OUT/cluster-rebalance-status.json" \
  --slurpfile rebalance_before "$OUT/targeted-rebalance-before.json" \
  --slurpfile rebalance_after "$OUT/targeted-rebalance-after.json" \
  --slurpfile health "$OUT/cluster-health-final.json" \
  --slurpfile rssl "$OUT/rss-local-summary.json" \
  --slurpfile rssr "$OUT/rss-remote-summary.json" \
  --slurpfile rssr2 "$OUT/rss-remote2-summary.json" \
  '{out:$out, storage_layout:$storage_layout, roots:{local:{index:$local_index_root,blob:$local_blob_root},remote:{index:$remote_index_root,blob:$remote_blob_root},remote2:{index:$remote2_index_root,blob:$remote2_blob_root}}, canonical_ncdx_records:$ncdx_records, fragment:$fragment, stable_fragment_id:$stable_fragment, native_peer_locator_strings:$native_locators, multipart_resume_upload:$upload[0], download_local:$dl[0], download_remote:$dr[0], download_remote2:$dr2[0], namespace_stress:$ns[0], scale_upload:$scale[0], object_status:$object_status[0], object_repair:$object_repair[0], node_loss:{health_down:$node_loss_health[0], download:$node_loss_download[0], bounded_upload_attempt:$node_loss_upload[0], health_restored:$node_loss_restored[0]}, trash:$trash[0], recover:$recover[0], reconstruct_after_delete:$recon[0], repair:$repair[0], targeted_rebalance:{before:$rebalance_before[0],job:$rebalance[0],after:$rebalance_after[0]}, final_health:$health[0], rss_local:$rssl[0], rss_remote:$rssr[0], rss_remote2:$rssr2[0]}' \
  | tee "$OUT/summary.json"

echo "N2_HARDENING_PROOF_OUT=$OUT"
