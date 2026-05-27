#!/usr/bin/env bash
set -euo pipefail

REMOTE_IP="${REMOTE_IP:-192.168.0.13}"
REMOTE_PASS="${REMOTE_PASS:-1122}"
LOCAL_IP="${LOCAL_IP:-192.168.0.22}"
LOCAL_HTTPS_PORT="${LOCAL_HTTPS_PORT:-53401}"
LOCAL_NATIVE_PORT="${LOCAL_NATIVE_PORT:-53402}"
REMOTE_HTTPS_PORT="${REMOTE_HTTPS_PORT:-53411}"
REMOTE_NATIVE_PORT="${REMOTE_NATIVE_PORT:-53412}"
LOCAL_HDD_ROOT="${LOCAL_HDD_ROOT:-/mnt/disk-00}"
REMOTE_HDD_ROOT="${REMOTE_HDD_ROOT:-/mnt/DISK0}"
SOURCE_VIDEO="${SOURCE_VIDEO:-/home/administrator/Downloads/mhw.mp4}"
PROOF_MIB="${PROOF_MIB:-256}"
SMALL_OBJECTS="${SMALL_OBJECTS:-120}"
NAMESPACE_OPS="${NAMESPACE_OPS:-40}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT_DIR/target/release/litiaina-n2"
CERT="$ROOT_DIR/certificates/cert.pem"
KEY="$ROOT_DIR/certificates/key.pem"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT_DIR/.n2-bench/n2-hardening-native-proof-$TS"
LROOT="$LOCAL_HDD_ROOT/n2-hardening-native-proof-$TS/node-a"
RROOT="$REMOTE_HDD_ROOT/n2-hardening-native-proof-$TS/node-b"
LOCAL_BASE="https://$LOCAL_IP:$LOCAL_HTTPS_PORT"
REMOTE_BASE="https://$REMOTE_IP:$REMOTE_HTTPS_PORT"
LOCAL_NATIVE="n2://$LOCAL_IP:$LOCAL_NATIVE_PORT"
REMOTE_NATIVE="n2://$REMOTE_IP:$REMOTE_NATIVE_PORT"
MASTER="n2-proof-master-key"
FRAG_NAME="n2-hardening-$TS"
FRAG_KEY="n2-frag-key-$TS"
TOKEN=""

ssh_remote() {
  sshpass -p "$REMOTE_PASS" ssh -o StrictHostKeyChecking=no administrator@"$REMOTE_IP" "$@"
}

scp_remote() {
  sshpass -p "$REMOTE_PASS" scp -o StrictHostKeyChecking=no "$@"
}

cleanup() {
  set +e
  if [[ -f "$LROOT/rss-monitor.pid" ]]; then kill "$(cat "$LROOT/rss-monitor.pid")" 2>/dev/null; fi
  if [[ -f "$LROOT/n2.pid" ]]; then kill "$(cat "$LROOT/n2.pid")" 2>/dev/null; fi
  ssh_remote "if [ -f '$RROOT/rss-monitor.pid' ]; then kill \$(cat '$RROOT/rss-monitor.pid') 2>/dev/null || true; fi; if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; fi" >/dev/null 2>&1
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
      "index_roots": ["$LROOT/index"],
      "blob_roots": ["$LROOT/blob"]
    },
    {
      "id": "node-b",
      "endpoint": "$REMOTE_BASE",
      "native_endpoint": "$REMOTE_NATIVE",
      "state": "online",
      "weight": 1,
      "index_roots": ["$RROOT/index"],
      "blob_roots": ["$RROOT/blob"]
    }
  ]
}
JSON
}

write_config() {
  local path="$1" https_port="$2" native_port="$3" node_id="$4" node_endpoint="$5" native_endpoint="$6" root="$7"
  cat > "$path" <<CFG
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
  (cd "$LROOT"; setsid ./litiaina-n2 </dev/null > server.log 2>&1 & echo $! > n2.pid)
  wait_ping "$LOCAL_BASE"
  wait_ping "$REMOTE_BASE"
  (while kill -0 "$(cat "$LROOT/n2.pid")" 2>/dev/null; do
    printf '%s,%s\n' "$(date +%s)" "$(ps -o rss= -p "$(cat "$LROOT/n2.pid")" | tr -d ' ')"
    sleep 1
  done) > "$OUT/rss-local.csv" &
  echo $! > "$LROOT/rss-monitor.pid"
  ssh_remote "pid=\$(cat '$RROOT/n2.pid'); nohup sh -c 'while kill -0 \"\$0\" 2>/dev/null; do printf \"%s,%s\\\\n\" \$(date +%s) \$(ps -o rss= -p \"\$0\" | tr -d \" \"); sleep 1; done' \"\$pid\" > '$RROOT/rss-remote.csv' 2>/dev/null & echo \$! > '$RROOT/rss-monitor.pid'"
}

stop_nodes() {
  if [[ -f "$LROOT/rss-monitor.pid" ]]; then kill "$(cat "$LROOT/rss-monitor.pid")" 2>/dev/null || true; rm -f "$LROOT/rss-monitor.pid"; fi
  if [[ -f "$LROOT/n2.pid" ]]; then kill "$(cat "$LROOT/n2.pid")" 2>/dev/null || true; rm -f "$LROOT/n2.pid"; fi
  ssh_remote "if [ -f '$RROOT/rss-monitor.pid' ]; then kill \$(cat '$RROOT/rss-monitor.pid') 2>/dev/null || true; rm -f '$RROOT/rss-monitor.pid'; fi; if [ -f '$RROOT/n2.pid' ]; then kill \$(cat '$RROOT/n2.pid') 2>/dev/null || true; rm -f '$RROOT/n2.pid'; fi" >/dev/null || true
  sleep 2
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
pkill -x litiaina-n2 2>/dev/null || true
ssh_remote "pkill -x litiaina-n2 2>/dev/null || true" >/dev/null || true
mkdir -p "$OUT" "$LROOT/index" "$LROOT/blob" "$LROOT/fs"
cp "$BIN" "$CERT" "$KEY" "$LROOT/"
ssh_remote "rm -rf '$RROOT' && mkdir -p '$RROOT/index' '$RROOT/blob' '$RROOT/fs'"
scp_remote "$BIN" "$CERT" "$KEY" administrator@"$REMOTE_IP":"$RROOT/" >/dev/null
write_env "$LROOT/n2.env"
scp_remote "$LROOT/n2.env" administrator@"$REMOTE_IP":"$RROOT/n2.env" >/dev/null
write_membership "$LROOT/fs/n2_membership.json"
scp_remote "$LROOT/fs/n2_membership.json" administrator@"$REMOTE_IP":"$RROOT/fs/n2_membership.json" >/dev/null
write_config "$LROOT/n2_config.ini" "$LOCAL_HTTPS_PORT" "$LOCAL_NATIVE_PORT" node-a "$LOCAL_BASE" "$LOCAL_NATIVE" "$LROOT"
write_config "$OUT/node-b-config.ini" "$REMOTE_HTTPS_PORT" "$REMOTE_NATIVE_PORT" node-b "$REMOTE_BASE" "$REMOTE_NATIVE" "$RROOT"
scp_remote "$OUT/node-b-config.ini" administrator@"$REMOTE_IP":"$RROOT/n2_config.ini" >/dev/null

start_nodes

curl_json -X POST "$LOCAL_BASE/n2/fragments" -d "{\"name\":\"$FRAG_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAG_KEY\",\"quota_bytes\":5368709120}" > "$OUT/create-local.json"
curl_json -X POST "$REMOTE_BASE/n2/fragments" -d "{\"name\":\"$FRAG_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAG_KEY\",\"quota_bytes\":5368709120}" > "$OUT/create-remote.json"
FRAGMENT="$(jq -r '.fragment_hash' "$OUT/create-local.json")"
curl_json -X POST "$LOCAL_BASE/n2/auth/fragment" -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAG_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" > "$OUT/auth-local.json"
TOKEN="$(jq -r '.access_token' "$OUT/auth-local.json")"
curl_auth "$LOCAL_BASE/n2/cluster/health" > "$OUT/cluster-health-initial.json"

dd if="$SOURCE_VIDEO" of="$OUT/mhw-proof.bin" bs=1M count="$PROOF_MIB" status=none
sha256sum "$OUT/mhw-proof.bin" > "$OUT/source.sha256"
split -b 64M -d -a 2 "$OUT/mhw-proof.bin" "$OUT/part-"

KEY_MP="videos/mhw-native-resume-$TS.mp4"
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

for side in local remote; do
  if [[ "$side" == "local" ]]; then base="$LOCAL_BASE"; else base="$REMOTE_BASE"; fi
  start="$(date +%s%N)"
  curl_auth -G "$base/n2/objects/download" --data-urlencode "object_key=$KEY_MP" -o "$OUT/download-$side.bin"
  ns=$(( "$(date +%s%N)" - start ))
  sha256sum "$OUT/download-$side.bin" > "$OUT/download-$side.sha256"
  printf '{"bytes":%s,"seconds":%.3f,"mib_per_sec":%s}\n' "$SIZE" "$(awk -v ns="$ns" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(metric_speed "$SIZE" "$ns")" > "$OUT/download-$side-metrics.json"
done
cmp -s "$OUT/mhw-proof.bin" "$OUT/download-local.bin"
cmp -s "$OUT/mhw-proof.bin" "$OUT/download-remote.bin"

ns_start="$(date +%s%N)"
for i in $(seq -w 1 "$NAMESPACE_OPS"); do
  curl_json -X POST "$LOCAL_BASE/n2/posix/mkdir" -H "Authorization: Bearer $TOKEN" -d "{\"path\":\"stress/dir-$i\"}" >/dev/null
  curl_json -X POST "$LOCAL_BASE/n2/posix/rename" -H "Authorization: Bearer $TOKEN" -d "{\"source_path\":\"stress/dir-$i\",\"destination_path\":\"stress/renamed-$i\"}" >/dev/null
done
ns_total=$(( NAMESPACE_OPS * 2 ))
ns_elapsed=$(( "$(date +%s%N)" - ns_start ))
printf '{"operations":%s,"seconds":%.3f,"ops_per_sec":%.2f}\n' "$ns_total" "$(awk -v ns="$ns_elapsed" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(awk -v ops="$ns_total" -v ns="$ns_elapsed" 'BEGIN{sec=ns/1000000000; printf "%.2f", ops/sec}')" > "$OUT/namespace-stress-metrics.json"
curl_auth -G "$REMOTE_BASE/n2/posix/list" --data-urlencode 'path=stress' --data-urlencode 'limit=1000' > "$OUT/namespace-stress-remote-list.json"

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

curl_json -X POST "$LOCAL_BASE/n2/upload/multipart/init/tmp/cancel-$TS.bin" -H "Authorization: Bearer $TOKEN" -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":134217728,\"expected_parts\":2,\"part_size_bytes\":67108864}" > "$OUT/cancel-init.json"
CVERSION="$(jq -r '.version_id' "$OUT/cancel-init.json")"
curl_auth -X DELETE -G "$LOCAL_BASE/n2/upload/multipart/$CVERSION" --data-urlencode "object_key=tmp/cancel-$TS.bin" -o "$OUT/cancel-response.json"
curl_auth "$LOCAL_BASE/n2/upload/multipart/$CVERSION" > "$OUT/cancel-status.json" || true

find "$LROOT/index" \( -name '*.nmtd' -o -name '*.ndx' \) -print0 | xargs -0 strings -a | rg 'n2-peer|n2%3A' > "$OUT/native-peer-locator-strings.txt" || true
NATIVE_LOCATORS="$(wc -l < "$OUT/native-peer-locator-strings.txt" | tr -d ' ')"

ssh_remote "find '$RROOT/blob/$FRAGMENT/.blobs' -type f | sort | head -1" > "$OUT/deleted-shard.txt"
DELETED="$(cat "$OUT/deleted-shard.txt")"
ssh_remote "rm -f '$DELETED'"
start="$(date +%s%N)"
curl_auth -G "$LOCAL_BASE/n2/objects/download" --data-urlencode "object_key=$KEY_MP" -o "$OUT/reconstruct-after-delete.bin"
recon_elapsed=$(( "$(date +%s%N)" - start ))
cmp -s "$OUT/mhw-proof.bin" "$OUT/reconstruct-after-delete.bin"
printf '{"bytes":%s,"seconds":%.3f,"mib_per_sec":%s,"deleted_shard":"%s"}\n' "$SIZE" "$(awk -v ns="$recon_elapsed" 'BEGIN{printf "%.3f", ns/1000000000}')" "$(metric_speed "$SIZE" "$recon_elapsed")" "$DELETED" > "$OUT/reconstruct-after-delete-metrics.json"

curl_json -X POST "$LOCAL_BASE/n2/cluster/repair/start" -H "Authorization: Bearer $TOKEN" -d "{\"fragment\":\"$FRAGMENT\",\"prefix\":\"videos/\",\"dry_run\":false}" > "$OUT/cluster-repair-start.json"
wait_job "$LOCAL_BASE/n2/cluster/repair/status" "$OUT/cluster-repair-status.json"
curl_json -X POST "$LOCAL_BASE/n2/cluster/rebalance/start" -H "Authorization: Bearer $TOKEN" -d "{\"fragment\":\"$FRAGMENT\",\"prefix\":\"videos/\",\"dry_run\":false}" > "$OUT/cluster-rebalance-start.json"
wait_job "$LOCAL_BASE/n2/cluster/rebalance/status" "$OUT/cluster-rebalance-status.json"
curl_auth "$LOCAL_BASE/n2/cluster/health" > "$OUT/cluster-health-final.json"

scp_remote administrator@"$REMOTE_IP":"$RROOT/rss-remote.csv" "$OUT/rss-remote.csv" >/dev/null || true
rss_summary "$OUT/rss-local.csv" > "$OUT/rss-local-summary.json"
rss_summary "$OUT/rss-remote.csv" > "$OUT/rss-remote-summary.json"

jq -n \
  --arg out "$OUT" \
  --arg fragment "$FRAGMENT" \
  --argjson native_locators "$NATIVE_LOCATORS" \
  --slurpfile upload "$OUT/multipart-resume-upload-metrics.json" \
  --slurpfile dl "$OUT/download-local-metrics.json" \
  --slurpfile dr "$OUT/download-remote-metrics.json" \
  --slurpfile ns "$OUT/namespace-stress-metrics.json" \
  --slurpfile scale "$OUT/scale-upload-metrics.json" \
  --slurpfile recon "$OUT/reconstruct-after-delete-metrics.json" \
  --slurpfile repair "$OUT/cluster-repair-status.json" \
  --slurpfile rebalance "$OUT/cluster-rebalance-status.json" \
  --slurpfile health "$OUT/cluster-health-final.json" \
  --slurpfile rssl "$OUT/rss-local-summary.json" \
  --slurpfile rssr "$OUT/rss-remote-summary.json" \
  '{out:$out, fragment:$fragment, native_peer_locator_strings:$native_locators, multipart_resume_upload:$upload[0], download_local:$dl[0], download_remote:$dr[0], namespace_stress:$ns[0], scale_upload:$scale[0], reconstruct_after_delete:$recon[0], repair:$repair[0], rebalance:$rebalance[0], final_health:$health[0], rss_local:$rssl[0], rss_remote:$rssr[0]}' \
  | tee "$OUT/summary.json"

echo "N2_HARDENING_PROOF_OUT=$OUT"
