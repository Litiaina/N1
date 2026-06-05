#!/usr/bin/env bash
set -Eeuo pipefail

REMOTE_IP="${REMOTE_IP:-192.168.0.13}"
REMOTE_PASS="${REMOTE_PASS:-1122}"
REMOTE2_IP="${REMOTE2_IP:-192.168.0.18}"
REMOTE2_PASS="${REMOTE2_PASS:-Admin@@@123}"
LOCAL_IP="${LOCAL_IP:-192.168.0.22}"
LOCAL_HTTPS_PORT="${LOCAL_HTTPS_PORT:-54401}"
LOCAL_NATIVE_PORT="${LOCAL_NATIVE_PORT:-54402}"
REMOTE_HTTPS_PORT="${REMOTE_HTTPS_PORT:-54411}"
REMOTE_NATIVE_PORT="${REMOTE_NATIVE_PORT:-54412}"
REMOTE2_HTTPS_PORT="${REMOTE2_HTTPS_PORT:-54421}"
REMOTE2_NATIVE_PORT="${REMOTE2_NATIVE_PORT:-54422}"
LOCAL_HDD_ROOT="${LOCAL_HDD_ROOT:-/mnt/disk-00}"
REMOTE_HDD_ROOT="${REMOTE_HDD_ROOT:-/mnt/DISK0}"
REMOTE2_HDD_ROOT="${REMOTE2_HDD_ROOT:-/mnt/DISK2}"
PROOF_MIB="${PROOF_MIB:-32}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT_DIR/target/release/litiaina-n2"
CERT="$ROOT_DIR/certificates/cert.pem"
KEY="$ROOT_DIR/certificates/key.pem"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT_DIR/.n2-bench/capacity-expansion-proof-$TS"
LROOT="$LOCAL_HDD_ROOT/n2-capacity-expansion-$TS/node-a"
RROOT="$REMOTE_HDD_ROOT/n2-capacity-expansion-$TS/node-b"
CROOT="$REMOTE2_HDD_ROOT/n2-capacity-expansion-$TS/node-c"
LOCAL_BASE="https://$LOCAL_IP:$LOCAL_HTTPS_PORT"
REMOTE_BASE="https://$REMOTE_IP:$REMOTE_HTTPS_PORT"
REMOTE2_BASE="https://$REMOTE2_IP:$REMOTE2_HTTPS_PORT"
LOCAL_NATIVE="n2://$LOCAL_IP:$LOCAL_NATIVE_PORT"
REMOTE_NATIVE="n2://$REMOTE_IP:$REMOTE_NATIVE_PORT"
REMOTE2_NATIVE="n2://$REMOTE2_IP:$REMOTE2_NATIVE_PORT"
MASTER="n2-capacity-master-$TS"
FRAG_NAME="capacity-$TS"
FRAG_KEY="capacity-key-$TS"
TOKEN=""

ssh_b() { sshpass -p "$REMOTE_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 administrator@"$REMOTE_IP" "$@"; }
scp_b() { sshpass -p "$REMOTE_PASS" scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$@"; }
ssh_c() { sshpass -p "$REMOTE2_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 administrator@"$REMOTE2_IP" "$@"; }
scp_c() { sshpass -p "$REMOTE2_PASS" scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 "$@"; }

cleanup() {
  set +e
  [[ -f "$LROOT/n2.pid" ]] && kill "$(cat "$LROOT/n2.pid")" 2>/dev/null || true
  ssh_b "if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; fi" >/dev/null 2>&1
  ssh_c "if [ -f '$CROOT/n2.pid' ]; then kill \$(cat '$CROOT/n2.pid') 2>/dev/null || true; fi" >/dev/null 2>&1
}
trap cleanup EXIT

write_env() {
  local path="$1"
  cat >"$path" <<ENV
JWT_SECRET=n2-capacity-jwt-secret
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
ENV
}

write_membership() {
  local path="$1" epoch="$2" include_c="$3"
  if [[ "$include_c" == true ]]; then
    cat >"$path" <<JSON
{
  "cluster_id": "n2-capacity-$TS",
  "epoch": $epoch,
  "nodes": [
    {"id":"node-a","endpoint":"$LOCAL_BASE","native_endpoint":"$LOCAL_NATIVE","state":"online","weight":1,"index_roots":["$LROOT/index"],"blob_roots":["$LROOT/blob"]},
    {"id":"node-b","endpoint":"$REMOTE_BASE","native_endpoint":"$REMOTE_NATIVE","state":"online","weight":1,"index_roots":["$RROOT/index"],"blob_roots":["$RROOT/blob"]},
    {"id":"node-c","endpoint":"$REMOTE2_BASE","native_endpoint":"$REMOTE2_NATIVE","state":"online","weight":1,"index_roots":["$CROOT/index"],"blob_roots":["$CROOT/blob"]}
  ]
}
JSON
  else
    cat >"$path" <<JSON
{
  "cluster_id": "n2-capacity-$TS",
  "epoch": $epoch,
  "nodes": [
    {"id":"node-a","endpoint":"$LOCAL_BASE","native_endpoint":"$LOCAL_NATIVE","state":"online","weight":1,"index_roots":["$LROOT/index"],"blob_roots":["$LROOT/blob"]},
    {"id":"node-b","endpoint":"$REMOTE_BASE","native_endpoint":"$REMOTE_NATIVE","state":"online","weight":1,"index_roots":["$RROOT/index"],"blob_roots":["$RROOT/blob"]}
  ]
}
JSON
  fi
}

write_config() {
  local path="$1" https_port="$2" native_port="$3" node_id="$4" node_endpoint="$5" native_endpoint="$6" root="$7"
  cat >"$path" <<CFG
[server]
host=0.0.0.0
https_port=$https_port
native_port=$native_port
cert_path=$root/cert.pem
key_path=$root/key.pem

[disk_config]
index_store_disks=$root/index
blob_store_disks=$root/blob
fs_storage_path=$root/fs

[cluster_config]
node_id=$node_id
node_endpoint=$node_endpoint
native_endpoint=$native_endpoint
membership_file=$root/fs/n2_membership.json
cluster_layout_mode=auto
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled

[index_config]
partition_count=64

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
max_part_size=67108864
data_shards=1
parity_shards=0
stripe_block_size=262144
CFG
}

wait_ping() {
  local base="$1"
  for _ in $(seq 1 80); do
    curl -sk --http1.1 -fsS "$base/n2/ping" >/dev/null 2>&1 && return 0
    sleep 1
  done
  curl -sk --http1.1 -fsS "$base/n2/ping" >/dev/null
}

start_a() { (cd "$LROOT"; setsid ./litiaina-n2 </dev/null >server.log 2>&1 & echo $! >n2.pid); wait_ping "$LOCAL_BASE"; }
start_b() { ssh_b "cd '$RROOT'; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo \$! > n2.pid"; wait_ping "$REMOTE_BASE"; }
start_c() { ssh_c "cd '$CROOT'; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo \$! > n2.pid"; wait_ping "$REMOTE2_BASE"; }
stop_a() { [[ -f "$LROOT/n2.pid" ]] && kill "$(cat "$LROOT/n2.pid")" 2>/dev/null || true; rm -f "$LROOT/n2.pid"; }
stop_b() { ssh_b "if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; rm -f '$RROOT/n2.pid'; fi"; }
stop_c() { ssh_c "if [ -f '$CROOT/n2.pid' ]; then kill \$(cat '$CROOT/n2.pid') 2>/dev/null || true; rm -f '$CROOT/n2.pid'; fi"; }

auth() {
  curl -sk --http1.1 -fsS -X POST "$LOCAL_BASE/n2/auth/fragment" \
    -H 'Content-Type: application/json' \
    -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAG_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" \
    >"$OUT/auth.json"
  TOKEN="$(jq -r '.access_token' "$OUT/auth.json")"
}

curl_auth() { curl -sk --http1.1 -fsS -H "Authorization: Bearer $TOKEN" "$@"; }

upload_one() {
  local key="$1" file="$2" out="$3"
  curl_auth -X POST "$LOCAL_BASE/n2/upload/one-shot/$key" --data-binary @"$file" >"$out"
}

echo "[capacity-proof] out=$OUT"
mkdir -p "$OUT" "$LROOT/index" "$LROOT/blob" "$LROOT/fs"
cp "$BIN" "$CERT" "$KEY" "$LROOT/"
ssh_b "rm -rf '$RROOT' && mkdir -p '$RROOT/index' '$RROOT/blob' '$RROOT/fs'"
ssh_c "rm -rf '$CROOT' && mkdir -p '$CROOT/index' '$CROOT/blob' '$CROOT/fs'"
scp_b "$BIN" "$CERT" "$KEY" administrator@"$REMOTE_IP":"$RROOT/" >/dev/null
scp_c "$BIN" "$CERT" "$KEY" administrator@"$REMOTE2_IP":"$CROOT/" >/dev/null
write_env "$LROOT/n2.env"
scp_b "$LROOT/n2.env" administrator@"$REMOTE_IP":"$RROOT/n2.env" >/dev/null
scp_c "$LROOT/n2.env" administrator@"$REMOTE2_IP":"$CROOT/n2.env" >/dev/null
write_config "$LROOT/n2_config.ini" "$LOCAL_HTTPS_PORT" "$LOCAL_NATIVE_PORT" node-a "$LOCAL_BASE" "$LOCAL_NATIVE" "$LROOT"
write_config "$OUT/node-b-config.ini" "$REMOTE_HTTPS_PORT" "$REMOTE_NATIVE_PORT" node-b "$REMOTE_BASE" "$REMOTE_NATIVE" "$RROOT"
write_config "$OUT/node-c-config.ini" "$REMOTE2_HTTPS_PORT" "$REMOTE2_NATIVE_PORT" node-c "$REMOTE2_BASE" "$REMOTE2_NATIVE" "$CROOT"
scp_b "$OUT/node-b-config.ini" administrator@"$REMOTE_IP":"$RROOT/n2_config.ini" >/dev/null
scp_c "$OUT/node-c-config.ini" administrator@"$REMOTE2_IP":"$CROOT/n2_config.ini" >/dev/null

write_membership "$LROOT/fs/n2_membership.json" 1 false
scp_b "$LROOT/fs/n2_membership.json" administrator@"$REMOTE_IP":"$RROOT/fs/n2_membership.json" >/dev/null
start_b
start_a

curl -sk --http1.1 -fsS -X POST "$LOCAL_BASE/n2/fragments" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$FRAG_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAG_KEY\"}" >"$OUT/create-a.json"
FRAGMENT="$(jq -r '.fragment_hash' "$OUT/create-a.json")"
STABLE="$(jq -r '.stable_fragment_id' "$OUT/create-a.json")"
curl -sk --http1.1 -fsS -X POST "$REMOTE_BASE/n2/fragments" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$FRAG_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAG_KEY\",\"stable_fragment_id\":\"$STABLE\"}" >"$OUT/create-b.json"
auth

dd if=/dev/urandom of="$OUT/old-layout.bin" bs=1M count="$PROOF_MIB" status=none
upload_one "capacity/old-layout.bin" "$OUT/old-layout.bin" "$OUT/old-upload.json"
curl_auth -G "$LOCAL_BASE/n2/objects/status/capacity/old-layout.bin" --data-urlencode mode=availability >"$OUT/old-status-before-expand.json"

stop_a
stop_b
write_membership "$LROOT/fs/n2_membership.json" 2 true
scp_b "$LROOT/fs/n2_membership.json" administrator@"$REMOTE_IP":"$RROOT/fs/n2_membership.json" >/dev/null
scp_c "$LROOT/fs/n2_membership.json" administrator@"$REMOTE2_IP":"$CROOT/fs/n2_membership.json" >/dev/null
start_c
start_b
start_a
curl -sk --http1.1 -fsS -X POST "$REMOTE2_BASE/n2/fragments" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$FRAG_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAG_KEY\",\"stable_fragment_id\":\"$STABLE\"}" >"$OUT/create-c.json"
auth

dd if=/dev/urandom of="$OUT/new-layout.bin" bs=1M count="$PROOF_MIB" status=none
upload_one "capacity/new-layout.bin" "$OUT/new-layout.bin" "$OUT/new-upload.json"
curl_auth -G "$LOCAL_BASE/n2/objects/status/capacity/old-layout.bin" --data-urlencode mode=availability >"$OUT/old-status-after-expand.json"
curl_auth -G "$LOCAL_BASE/n2/objects/status/capacity/new-layout.bin" --data-urlencode mode=availability >"$OUT/new-status-after-expand.json"
curl_auth -G "$REMOTE2_BASE/n2/objects/download" --data-urlencode object_key=capacity/old-layout.bin -o "$OUT/old-download-after-expand.bin"
curl_auth -G "$REMOTE_BASE/n2/objects/download" --data-urlencode object_key=capacity/new-layout.bin -o "$OUT/new-download-after-expand.bin"
cmp -s "$OUT/old-layout.bin" "$OUT/old-download-after-expand.bin"
cmp -s "$OUT/new-layout.bin" "$OUT/new-download-after-expand.bin"

curl -sk --http1.1 -fsS -X POST "$LOCAL_BASE/n2/cluster/rebalance/start" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"fragment\":\"$FRAGMENT\",\"prefix\":\"capacity/\",\"dry_run\":true,\"mismatched_only\":true}" >"$OUT/targeted-rebalance-start.json"
for _ in $(seq 1 120); do
  curl_auth "$LOCAL_BASE/n2/cluster/rebalance/status" >"$OUT/targeted-rebalance-status.json"
  [[ "$(jq -r '.running' "$OUT/targeted-rebalance-status.json")" == "false" ]] && break
  sleep 1
done

jq -n \
  --arg out "$OUT" \
  --arg fragment "$FRAGMENT" \
  --arg stable_fragment_id "$STABLE" \
  --slurpfile old_before "$OUT/old-status-before-expand.json" \
  --slurpfile old_after "$OUT/old-status-after-expand.json" \
  --slurpfile new_after "$OUT/new-status-after-expand.json" \
  --slurpfile rebalance "$OUT/targeted-rebalance-status.json" \
  '{out:$out,fragment:$fragment,stable_fragment_id:$stable_fragment_id,old_before_expand:$old_before[0],old_after_expand:$old_after[0],new_after_expand:$new_after[0],targeted_rebalance:$rebalance[0],downloads_sha_match:true}' \
  | tee "$OUT/summary.json"

echo "N2_CAPACITY_EXPANSION_PROOF_OUT=$OUT"
