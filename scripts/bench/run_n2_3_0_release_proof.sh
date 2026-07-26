#!/usr/bin/env bash
set -euo pipefail

# Finite N2 3.2 architectural proof; the filename is retained for the
# flat-NCNX proof family. This never installs a service, never uses production
# ports, and only removes directories containing this run's unique timestamp.
# REMOTE_PASS is intentionally supplied by the operator at runtime.

: "${REMOTE_PASS:?set REMOTE_PASS for the authorized proof host}"
REMOTE_HOST="${REMOTE_HOST:-192.168.0.36}"
REMOTE_USER="${REMOTE_USER:-administrator}"
LOCAL_IP="${LOCAL_IP:-192.168.0.1}"
REMOTE_IP="${REMOTE_IP:-$REMOTE_HOST}"
PROOF_MIB="${PROOF_MIB:-64}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${N2_PROOF_BIN:-$ROOT_DIR/target/release/litiaina-n2}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_ROOT="${N2_PROOF_OUT_ROOT:-/home/wlzambo/DEV-TEST}"
OUT="$OUT_ROOT/N2-3.0-proof-$TS"
LOCAL_RUN="$OUT/runtime"
LOCAL_A="$LOCAL_RUN/node-a"
LOCAL_B="$LOCAL_RUN/node-b"
LOCAL_A_STORE="$OUT/storage/node-a"
LOCAL_B_STORE="$OUT/storage/node-b"
REMOTE_RUN_BASE="/home/$REMOTE_USER/.cache/N2-3.0-proof-$TS"
REMOTE_C="$REMOTE_RUN_BASE/node-c"
REMOTE_D="$REMOTE_RUN_BASE/node-d"
REMOTE_STORE_BASE=".n2-3.0-proof-$TS"
REMOTE_C_STORE="/mnt/NDISK002/$REMOTE_STORE_BASE/node-c"
REMOTE_D_STORE="/mnt/NDISK003/$REMOTE_STORE_BASE/node-d"

A_HTTPS=56001; A_NATIVE=56002
B_HTTPS=56011; B_NATIVE=56012
C_HTTPS=56021; C_NATIVE=56022
D_HTTPS=56031; D_NATIVE=56032
A_BASE="https://$LOCAL_IP:$A_HTTPS"
B_BASE="https://$LOCAL_IP:$B_HTTPS"
C_BASE="https://$REMOTE_IP:$C_HTTPS"
D_BASE="https://$REMOTE_IP:$D_HTTPS"
POOL_ID="pool-proof-$TS"
CLUSTER_ID="cluster-proof-$TS"
MASTER="n2-3.0-proof-master-$TS"
FRAGMENT_NAME="n2-3.0-proof-$TS"
FRAGMENT_KEY="n2-3.0-fragment-$TS"
TOKEN=""

export SSHPASS="$REMOTE_PASS"
SSH=(sshpass -e ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 "$REMOTE_USER@$REMOTE_HOST")
SCP=(sshpass -e scp -o StrictHostKeyChecking=no -o ConnectTimeout=8)

for tool in jq curl openssl sshpass pidstat sha256sum awk; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done
[[ -x "$BIN" ]] || { echo "release binary is missing: $BIN" >&2; exit 1; }

remote() { "${SSH[@]}" "$@"; }
copy_remote() { "${SCP[@]}" "$@"; }

kill_pidfile() {
  local pidfile="$1"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile")"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$pidfile"
  fi
}

cleanup() {
  set +e
  kill_pidfile "$LOCAL_A/pidstat.pid"
  kill_pidfile "$LOCAL_B/pidstat.pid"
  kill_pidfile "$LOCAL_A/n2.pid"
  kill_pidfile "$LOCAL_B/n2.pid"
  remote "for n in '$REMOTE_C' '$REMOTE_D'; do if [ -f \"\$n/pidstat.pid\" ]; then kill \$(cat \"\$n/pidstat.pid\") 2>/dev/null || true; fi; if [ -f \"\$n/n2.pid\" ]; then kill \$(cat \"\$n/n2.pid\") 2>/dev/null || true; fi; done" >/dev/null 2>&1
  remote "rm -rf '$REMOTE_RUN_BASE' '$REMOTE_C_STORE' '$REMOTE_D_STORE'; rmdir '/mnt/NDISK002/$REMOTE_STORE_BASE' '/mnt/NDISK003/$REMOTE_STORE_BASE' 2>/dev/null || true" >/dev/null 2>&1
}
trap cleanup EXIT

curl_json() { curl -k --http1.1 -fsS -H 'Content-Type: application/json' "$@"; }
curl_auth() { curl -k --http1.1 -fsS -H "Authorization: Bearer $TOKEN" "$@"; }

wait_ping() {
  local base="$1"
  for _ in $(seq 1 80); do
    curl -k --http1.1 -fsS "$base/noa/v1/ping" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  echo "node did not become ready: $base" >&2
  return 1
}

wait_gc() {
  local out="$1"
  for _ in $(seq 1 120); do
    curl_auth "$A_BASE/noa/v1/maintenance/gc/status" > "$out"
    [[ "$(jq -r '.is_running' "$out")" == "false" ]] && return 0
    sleep 0.25
  done
  echo "GC did not complete in the finite wait window" >&2
  return 1
}

write_env() {
  local path="$1"
  cat > "$path" <<EOF
JWT_SECRET=n2-3.0-proof-jwt-$TS
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_CLUSTER_SECRET=n2-3.0-proof-cluster-secret-$TS
EOF
}

write_config() {
  local path="$1" node="$2" https_port="$3" native_port="$4" endpoint_ip="$5" store="$6" runtime="$7" root_id="$8"
  cat > "$path" <<EOF
[server]
host=0.0.0.0
https_port=$https_port
native_port=$native_port
cert_path=$runtime/cert.pem
key_path=$runtime/key.pem

[disk_config]
storage_roots=$root_id=$store
runtime_path=$runtime/state

[cluster_config]
node_id=$node
pool_id=$POOL_ID
pool_generation=1
node_endpoint=https://$endpoint_ip:$https_port
native_endpoint=n2://$endpoint_ip:$native_port
membership_file=$runtime/state/n2_membership.json
object_data_shards=2
object_parity_shards=2
auto_rebalance=false

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=1
retention_seconds=2

[auto_healing]
enabled=true
initial_delay_seconds=3600
scan_interval_seconds=3600
coordinator_nodes=auto
max_parallel_fragments=2
max_objects_per_fragment_cycle=256
max_duration_seconds=60
lease_seconds=90
checksum_scan_every_cycles=1

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=3600

[rate_limiter_config]
enable_rate_limiting=false
auth_limit_per_second=100
auth_limit_burst_size=200
view_limit_per_second=1000
view_limit_burst_size=2000
upload_limit_per_second=1000
upload_limit_burst_size=4000
global_limit_per_second=2000
global_limit_burst_size=4000

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
monitor_interval_seconds=3600
monitor_deep_scan_interval_seconds=3600
max_part_size=67108864
data_shards=2
parity_shards=2
stripe_block_size=1048576
EOF
}

percentiles() {
  local input="$1" output="$2"
  sort -n "$input" | awk '
    {v[NR]=$1}
    END {
      if (NR == 0) {print "{}"; exit}
      p50=v[int((NR-1)*0.50)+1]*1000
      p95=v[int((NR-1)*0.95)+1]*1000
      p99=v[int((NR-1)*0.99)+1]*1000
      sum=0; for(i=1;i<=NR;i++)sum+=v[i]
      printf "{\"samples\":%d,\"p50_ms\":%.3f,\"p95_ms\":%.3f,\"p99_ms\":%.3f,\"mean_ms\":%.3f}\n", NR,p50,p95,p99,(sum/NR)*1000
    }' > "$output"
}

start_local_node() {
  local run="$1"
  (cd "$run"; setsid ./litiaina-n2 </dev/null >>server.log 2>&1 & echo $! >n2.pid)
  local pid
  pid="$(cat "$run/n2.pid")"
  pidstat -h -u -r -d -w -p "$pid" 1 >> "$run/pidstat.log" 2>&1 &
  echo $! > "$run/pidstat.pid"
}

start_remote_node() {
  local run="$1"
  remote "cd '$run'; setsid ./litiaina-n2 </dev/null >>server.log 2>&1 & echo \$! >n2.pid; pid=\$(cat n2.pid); pidstat -h -u -r -d -w -p \"\$pid\" 1 >>pidstat.log 2>&1 & echo \$! >pidstat.pid"
}

stop_remote_node() {
  local run="$1"
  remote "if [ -f '$run/pidstat.pid' ]; then kill \$(cat '$run/pidstat.pid') 2>/dev/null || true; rm -f '$run/pidstat.pid'; fi; if [ -f '$run/n2.pid' ]; then kill \$(cat '$run/n2.pid') 2>/dev/null || true; rm -f '$run/n2.pid'; fi"
}

mkdir -p "$OUT" "$LOCAL_A" "$LOCAL_B" "$LOCAL_A_STORE" "$LOCAL_B_STORE" "$LOCAL_A/state" "$LOCAL_B/state"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=n2-3.0-proof' \
  -keyout "$OUT/key.pem" -out "$OUT/cert.pem" >/dev/null 2>&1
cp "$BIN" "$LOCAL_A/litiaina-n2"
cp "$BIN" "$LOCAL_B/litiaina-n2"
cp "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_A/"
cp "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_B/"
write_env "$LOCAL_A/n2.env"
cp "$LOCAL_A/n2.env" "$LOCAL_B/n2.env"

remote "mkdir -p '$REMOTE_C' '$REMOTE_D' '$REMOTE_C/state' '$REMOTE_D/state' '$REMOTE_C_STORE' '$REMOTE_D_STORE'"
copy_remote "$BIN" "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_A/n2.env" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/" >/dev/null
copy_remote "$BIN" "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_A/n2.env" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/" >/dev/null
remote "chmod +x '$REMOTE_C/litiaina-n2' '$REMOTE_D/litiaina-n2'"

cat > "$OUT/n2_membership.json" <<EOF
{
  "format_version": 4,
  "cluster_id": "$CLUSTER_ID",
  "epoch": 1,
  "pools": [{
    "id": "$POOL_ID",
    "generation": 1,
    "policy": {"object_data_shards": 2, "object_parity_shards": 2},
    "state": "writable",
    "node_ids": ["node-a", "node-b", "node-c", "node-d"]
  }],
  "nodes": [
    {"id":"node-a","endpoint":"$A_BASE","native_endpoint":"n2://$LOCAL_IP:$A_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-a","path":"$LOCAL_A_STORE"}],"joined_epoch":1},
    {"id":"node-b","endpoint":"$B_BASE","native_endpoint":"n2://$LOCAL_IP:$B_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-b","path":"$LOCAL_B_STORE"}],"joined_epoch":1},
    {"id":"node-c","endpoint":"$C_BASE","native_endpoint":"n2://$REMOTE_IP:$C_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-c","path":"$REMOTE_C_STORE"}],"joined_epoch":1},
    {"id":"node-d","endpoint":"$D_BASE","native_endpoint":"n2://$REMOTE_IP:$D_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-d","path":"$REMOTE_D_STORE"}],"joined_epoch":1}
  ]
}
EOF
cp "$OUT/n2_membership.json" "$LOCAL_A/state/"
cp "$OUT/n2_membership.json" "$LOCAL_B/state/"
copy_remote "$OUT/n2_membership.json" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/state/" >/dev/null
copy_remote "$OUT/n2_membership.json" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/state/" >/dev/null

write_config "$LOCAL_A/n2_config.ini" node-a "$A_HTTPS" "$A_NATIVE" "$LOCAL_IP" "$LOCAL_A_STORE" "$LOCAL_A" root-a
write_config "$LOCAL_B/n2_config.ini" node-b "$B_HTTPS" "$B_NATIVE" "$LOCAL_IP" "$LOCAL_B_STORE" "$LOCAL_B" root-b
write_config "$OUT/node-c.ini" node-c "$C_HTTPS" "$C_NATIVE" "$REMOTE_IP" "$REMOTE_C_STORE" "$REMOTE_C" root-c
write_config "$OUT/node-d.ini" node-d "$D_HTTPS" "$D_NATIVE" "$REMOTE_IP" "$REMOTE_D_STORE" "$REMOTE_D" root-d
copy_remote "$OUT/node-c.ini" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/n2_config.ini" >/dev/null
copy_remote "$OUT/node-d.ini" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/n2_config.ini" >/dev/null

start_remote_node "$REMOTE_C"
start_remote_node "$REMOTE_D"
start_local_node "$LOCAL_A"
start_local_node "$LOCAL_B"
wait_ping "$A_BASE"; wait_ping "$B_BASE"; wait_ping "$C_BASE"; wait_ping "$D_BASE"

curl_json -X POST "$A_BASE/noa/v1/fragments" \
  -d "{\"name\":\"$FRAGMENT_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAGMENT_KEY\"}" > "$OUT/fragment-create.json"
FRAGMENT="$(jq -er '.fragment_hash' "$OUT/fragment-create.json")"
STABLE_FRAGMENT="$(jq -er '.stable_fragment_id' "$OUT/fragment-create.json")"
curl_json -X POST "$B_BASE/noa/v1/auth/fragment" \
  -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAGMENT_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" > "$OUT/auth.json"
TOKEN="$(jq -er '.access_token' "$OUT/auth.json")"

curl_auth "$A_BASE/noa/v1/cluster/health" > "$OUT/health-initial.json"
jq -e '.data_shards == 2 and .parity_shards == 2 and .ncnx_replicas == 3 and .effective_client_writable == true' "$OUT/health-initial.json" >/dev/null

dd if=/dev/urandom of="$OUT/source.bin" bs=1M count="$PROOF_MIB" status=none
SOURCE_SHA="$(sha256sum "$OUT/source.bin" | awk '{print $1}')"
SIZE="$(stat -c%s "$OUT/source.bin")"
UPLOAD_STARTED="$(date +%s%N)"
curl_json -X POST "$A_BASE/noa/v1/upload/multipart/init/videos/main.bin" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$SIZE,\"expected_parts\":1,\"part_size_bytes\":$SIZE}" > "$OUT/upload-init.json"
VERSION="$(jq -er '.version_id' "$OUT/upload-init.json")"
SESSION="$(jq -er '.session_id' "$OUT/upload-init.json")"
curl_auth -X PUT "$A_BASE/noa/v1/upload/multipart/$VERSION/0" \
  --data-binary @"$OUT/source.bin" > "$OUT/upload-part.json"
curl_json -X POST "$A_BASE/noa/v1/upload/multipart/$VERSION/$SESSION/finalize?object_key=videos/main.bin" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$SIZE,\"expected_parts\":1,\"part_size_bytes\":$SIZE}" > "$OUT/upload-main.json"
UPLOAD_TIME="$(awk -v ns="$(( $(date +%s%N) - UPLOAD_STARTED ))" 'BEGIN {printf "%.6f", ns/1000000000}')"
jq -n --argjson bytes "$SIZE" --arg seconds "$UPLOAD_TIME" \
  '{bytes:$bytes,seconds:($seconds|tonumber),mib_per_second:(($bytes/1048576)/($seconds|tonumber))}' > "$OUT/upload-throughput.json"

for spec in "a:$A_BASE" "b:$B_BASE" "c:$C_BASE" "d:$D_BASE"; do
  name="${spec%%:*}"; base="${spec#*:}"
  seconds="$(curl -k --http1.1 -fsS -o "$OUT/download-$name.bin" -w '%{time_total}' \
    -G "$base/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'object_key=videos/main.bin')"
  [[ "$(sha256sum "$OUT/download-$name.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
  jq -n --argjson bytes "$SIZE" --arg seconds "$seconds" \
    '{bytes:$bytes,seconds:($seconds|tonumber),mib_per_second:(($bytes/1048576)/($seconds|tonumber))}' > "$OUT/download-$name-throughput.json"
done

printf 'small-a' > "$OUT/small-a.bin"; printf 'small-b' > "$OUT/small-b.bin"
curl_auth -X POST "$C_BASE/noa/v1/upload/one-shot/group/a.txt" --data-binary @"$OUT/small-a.bin" > "$OUT/upload-a.json"
curl_auth -X POST "$D_BASE/noa/v1/upload/one-shot/group/b.txt" --data-binary @"$OUT/small-b.bin" > "$OUT/upload-b.json"
curl_json -X POST "$B_BASE/noa/v1/posix/trash" -H "Authorization: Bearer $TOKEN" \
  -d '{"path":"group"}' > "$OUT/group-trash.json"
TRASH_ID="$(jq -er '.trash_entry_id' "$OUT/group-trash.json")"
jq -e '.affected == 2' "$OUT/group-trash.json" >/dev/null
curl_json -X POST "$C_BASE/noa/v1/posix/recover" -H "Authorization: Bearer $TOKEN" \
  -d "{\"path\":\"group\",\"trash_entry_id\":\"$TRASH_ID\"}" > "$OUT/group-recover.json"
jq -e '.affected == 2 and .kind == "directory"' "$OUT/group-recover.json" >/dev/null

# N2 3.1 generational trash: a new live object makes recovery conflict, the
# old trash generation remains intact, and deleting the same key again creates
# another generation without a physical-name collision.
printf 'collision-old' > "$OUT/collision-old.bin"
printf 'collision-new' > "$OUT/collision-new.bin"
curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/collision/recover.txt" \
  --data-binary @"$OUT/collision-old.bin" > "$OUT/collision-old-upload.json"
curl_json -X POST "$B_BASE/noa/v1/posix/trash" -H "Authorization: Bearer $TOKEN" \
  -d '{"path":"collision/recover.txt"}' > "$OUT/collision-first-trash.json"
COLLISION_FIRST_ID="$(jq -er '.trash_entry_id' "$OUT/collision-first-trash.json")"
curl_auth -X POST "$C_BASE/noa/v1/upload/one-shot/collision/recover.txt" \
  --data-binary @"$OUT/collision-new.bin" > "$OUT/collision-new-upload.json"
COLLISION_RECOVER_STATUS="$(curl -k --http1.1 -sS -o "$OUT/collision-recover-conflict.json" -w '%{http_code}' \
  -X POST "$D_BASE/noa/v1/posix/recover" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"path\":\"collision/recover.txt\",\"trash_entry_id\":\"$COLLISION_FIRST_ID\"}")"
[[ "$COLLISION_RECOVER_STATUS" == "409" ]]
curl_auth -G "$A_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=collision/recover.txt' -o "$OUT/collision-live-after-conflict.bin"
cmp -s "$OUT/collision-new.bin" "$OUT/collision-live-after-conflict.bin"
curl_auth -X DELETE "$B_BASE/noa/v1/objects/collision/recover.txt" > "$OUT/collision-second-trash.json"
COLLISION_SECOND_ID="$(jq -er '.trash_entry_id' "$OUT/collision-second-trash.json")"
[[ "$COLLISION_FIRST_ID" != "$COLLISION_SECOND_ID" ]]
curl_json -X POST "$C_BASE/noa/v1/posix/recover" -H "Authorization: Bearer $TOKEN" \
  -d "{\"path\":\"collision/recover.txt\",\"trash_entry_id\":\"$COLLISION_SECOND_ID\"}" \
  > "$OUT/collision-second-recover.json"
curl_auth -G "$D_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=collision/recover.txt' -o "$OUT/collision-live-after-recover.bin"
cmp -s "$OUT/collision-new.bin" "$OUT/collision-live-after-recover.bin"
find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -path '*/.trash/*/collision/recover.txt.ncnx' -type f \
  > "$OUT/collision-trash-local.txt"
remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -path '*/.trash/*/collision/recover.txt.ncnx' -type f" \
  > "$OUT/collision-trash-remote.txt"
COLLISION_TRASH_COUNT=$(( $(wc -l < "$OUT/collision-trash-local.txt") + $(wc -l < "$OUT/collision-trash-remote.txt") ))
[[ "$COLLISION_TRASH_COUNT" -eq 3 ]]

printf 'overwrite-v1' > "$OUT/overwrite-v1.bin"
printf 'overwrite-v2' > "$OUT/overwrite-v2.bin"
curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/overwrite.txt" --data-binary @"$OUT/overwrite-v1.bin" > "$OUT/overwrite-v1.json"
curl_auth -X POST "$B_BASE/noa/v1/upload/one-shot/overwrite.txt" --data-binary @"$OUT/overwrite-v2.bin" > "$OUT/overwrite-v2.json"
[[ "$(jq -r '.version_id' "$OUT/overwrite-v1.json")" != "$(jq -r '.version_id' "$OUT/overwrite-v2.json")" ]]
curl_auth "$C_BASE/noa/v1/objects/versions/overwrite.txt" > "$OUT/overwrite-versions.json"
jq -e 'length == 2' "$OUT/overwrite-versions.json" >/dev/null
curl_auth -G "$D_BASE/noa/v1/objects/download" --data-urlencode 'object_key=overwrite.txt' -o "$OUT/overwrite-current.bin"
cmp -s "$OUT/overwrite-v2.bin" "$OUT/overwrite-current.bin"

parallel_pids=()
for i in $(seq 1 8); do
  printf 'parallel-%s' "$i" > "$OUT/parallel-$i.bin"
  case $((i % 4)) in
    0) base="$A_BASE" ;;
    1) base="$B_BASE" ;;
    2) base="$C_BASE" ;;
    *) base="$D_BASE" ;;
  esac
  curl_auth -X POST "$base/noa/v1/upload/one-shot/concurrent/key-$i.txt" \
    --data-binary @"$OUT/parallel-$i.bin" > "$OUT/parallel-$i.json" &
  parallel_pids+=("$!")
done
for pid in "${parallel_pids[@]}"; do wait "$pid"; done
curl_auth -G "$A_BASE/noa/v1/objects" --data-urlencode 'prefix=concurrent/' --data-urlencode 'limit=20' > "$OUT/parallel-list.json"
jq -e '.entries | length == 8' "$OUT/parallel-list.json" >/dev/null

# Prove global logical ordering when an exact key and a same-name physical
# prefix coexist. A filesystem depth-first walk is not sufficient evidence for
# this case, so exercise one-item continuation pages through the public API.
for key in lex/a lex/a-previous lex/a/child lex/a/child/deep lex/a0; do
  curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/$key" \
    --data-binary @"$OUT/small-a.bin" > "$OUT/lex-$(printf '%s' "$key" | tr '/' '_').json"
done
LEX_TOKEN=""
: > "$OUT/lex-pages.txt"
for expected in lex/a lex/a-previous lex/a/child lex/a/child/deep lex/a0; do
  if [[ -n "$LEX_TOKEN" ]]; then
    curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=lex/' \
      --data-urlencode 'limit=1' --data-urlencode "continuation_token=$LEX_TOKEN" > "$OUT/lex-page.json"
  else
    curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=lex/' \
      --data-urlencode 'limit=1' > "$OUT/lex-page.json"
  fi
  [[ "$(jq -er '.entries[0].record' "$OUT/lex-page.json")" == "$expected" ]]
  printf '%s\n' "$expected" >> "$OUT/lex-pages.txt"
  LEX_TOKEN="$(jq -r '.continuation_token // empty' "$OUT/lex-page.json")"
done

: > "$OUT/list-latency.txt"; : > "$OUT/stat-latency.txt"; : > "$OUT/rename-latency.txt"
for _ in $(seq 1 30); do
  curl -k --http1.1 -fsS -o /dev/null -w '%{time_total}\n' \
    -G "$B_BASE/noa/v1/objects" -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'prefix=videos/' --data-urlencode 'limit=100' >> "$OUT/list-latency.txt"
  curl -k --http1.1 -fsS -o /dev/null -w '%{time_total}\n' \
    -G "$C_BASE/noa/v1/posix/stat" -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'path=videos/main.bin' >> "$OUT/stat-latency.txt"
done
for i in $(seq 1 20); do
  curl -k --http1.1 -fsS -o /dev/null -w '%{time_total}\n' -X POST \
    "$A_BASE/noa/v1/posix/rename" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"source_path\":\"videos/main.bin\",\"destination_path\":\"videos/main-$i.bin\"}" >> "$OUT/rename-latency.txt"
  curl_json -X POST "$A_BASE/noa/v1/posix/rename" -H "Authorization: Bearer $TOKEN" \
    -d "{\"source_path\":\"videos/main-$i.bin\",\"destination_path\":\"videos/main.bin\"}" >/dev/null
done
percentiles "$OUT/list-latency.txt" "$OUT/list-latency.json"
percentiles "$OUT/stat-latency.txt" "$OUT/stat-latency.json"
percentiles "$OUT/rename-latency.txt" "$OUT/rename-latency.json"

find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -path '*/.trash/*/videos/main*.ncnx' -type f > "$OUT/rename-trash-local.txt"
remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -path '*/.trash/*/videos/main*.ncnx' -type f" > "$OUT/rename-trash-remote.txt"
[[ ! -s "$OUT/rename-trash-local.txt" && ! -s "$OUT/rename-trash-remote.txt" ]]

find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -path '*/.namespace/videos/main.bin.ncnx' -type f > "$OUT/ncnx-local.txt"
remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -path '*/.namespace/videos/main.bin.ncnx' -type f" > "$OUT/ncnx-remote.txt"
NCNX_COUNT=$(( $(wc -l < "$OUT/ncnx-local.txt") + $(wc -l < "$OUT/ncnx-remote.txt") ))
[[ "$NCNX_COUNT" -eq 3 ]]

if [[ -s "$OUT/ncnx-local.txt" ]]; then
  DELETED_NCNX_HOST=local
  DELETED_NCNX="$(head -1 "$OUT/ncnx-local.txt")"
  rm -f "$DELETED_NCNX"
else
  DELETED_NCNX_HOST=remote
  DELETED_NCNX="$(head -1 "$OUT/ncnx-remote.txt")"
  remote "rm -f '$DELETED_NCNX'"
fi
for base in "$A_BASE" "$B_BASE" "$C_BASE" "$D_BASE"; do
  curl -k --http1.1 -sS -o "$OUT/healing-$(echo "$base" | tr ':/' '_').json" \
    -X POST "$base/noa/v1/cluster/healing/cycle" -H "Authorization: Bearer $TOKEN" || true
done
if [[ "$DELETED_NCNX_HOST" == local ]]; then [[ -f "$DELETED_NCNX" ]]; else remote "test -f '$DELETED_NCNX'"; fi

find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -path "*/$VERSION/part_0_*" -type f > "$OUT/shards-local.txt"
remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -path '*/$VERSION/part_0_*' -type f" > "$OUT/shards-remote.txt"
if [[ -s "$OUT/shards-local.txt" ]]; then
  DELETED_SHARD_HOST=local
  DELETED_SHARD="$(head -1 "$OUT/shards-local.txt")"
  rm -f "$DELETED_SHARD"
else
  DELETED_SHARD_HOST=remote
  DELETED_SHARD="$(head -1 "$OUT/shards-remote.txt")"
  remote "rm -f '$DELETED_SHARD'"
fi
for base in "$A_BASE" "$B_BASE" "$C_BASE" "$D_BASE"; do
  curl -k --http1.1 -sS -o "$OUT/object-healing-$(echo "$base" | tr ':/' '_').json" \
    -X POST "$base/noa/v1/cluster/healing/cycle" -H "Authorization: Bearer $TOKEN" || true
done
if [[ "$DELETED_SHARD_HOST" == local ]]; then [[ -f "$DELETED_SHARD" ]]; else remote "test -f '$DELETED_SHARD'"; fi
curl_auth -G "$D_BASE/noa/v1/objects/download" --data-urlencode 'object_key=videos/main.bin' -o "$OUT/download-after-heal.bin"
[[ "$(sha256sum "$OUT/download-after-heal.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]

printf 'ttl-object' > "$OUT/ttl.bin"
curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/ttl/expire.txt" --data-binary @"$OUT/ttl.bin" > "$OUT/ttl-upload.json"
curl_auth -X DELETE "$B_BASE/noa/v1/objects/ttl/expire.txt" > "$OUT/ttl-trash.json"
sleep 3
curl_auth -X POST "$A_BASE/noa/v1/maintenance/fragments/$FRAGMENT/gc/start?mode=availability" > "$OUT/gc-start.txt"
wait_gc "$OUT/gc-status.json"
jq -e '.total_trash_versions_expired >= 1 and .total_errors == 0' "$OUT/gc-status.json" >/dev/null
TTL_STATUS="$(curl -k --http1.1 -sS -o "$OUT/ttl-after-gc.json" -w '%{http_code}' \
  -G "$C_BASE/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" --data-urlencode 'object_key=ttl/expire.txt')"
[[ "$TTL_STATUS" == "404" ]]

stop_remote_node "$REMOTE_D"
curl_auth -G "$A_BASE/noa/v1/objects/download" --data-urlencode 'object_key=videos/main.bin' -o "$OUT/download-node-loss.bin"
[[ "$(sha256sum "$OUT/download-node-loss.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
printf 'must-fail' > "$OUT/node-loss.bin"
NODE_LOSS_STATUS="$(curl -k --http1.1 -sS -o "$OUT/node-loss-write.json" -w '%{http_code}' \
  -X POST "$A_BASE/noa/v1/upload/one-shot/fail-closed.bin" \
  -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/node-loss.bin")"
[[ "$NODE_LOSS_STATUS" == "503" ]]

kill_pidfile "$LOCAL_B/pidstat.pid"
kill_pidfile "$LOCAL_B/n2.pid"
curl_auth -G "$A_BASE/noa/v1/objects/download" --data-urlencode 'object_key=videos/main.bin' -o "$OUT/download-parity-count-loss.bin"
[[ "$(sha256sum "$OUT/download-parity-count-loss.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
start_local_node "$LOCAL_B"; wait_ping "$B_BASE"
start_remote_node "$REMOTE_D"; wait_ping "$D_BASE"
curl_auth -X POST "$D_BASE/noa/v1/upload/one-shot/restarted.bin" --data-binary @"$OUT/node-loss.bin" > "$OUT/write-after-node-return.json"

kill_pidfile "$LOCAL_A/n2.pid"; kill_pidfile "$LOCAL_B/n2.pid"
stop_remote_node "$REMOTE_C"; stop_remote_node "$REMOTE_D"
start_remote_node "$REMOTE_C"; start_remote_node "$REMOTE_D"; start_local_node "$LOCAL_A"; start_local_node "$LOCAL_B"
wait_ping "$A_BASE"; wait_ping "$B_BASE"; wait_ping "$C_BASE"; wait_ping "$D_BASE"
curl_auth -G "$C_BASE/noa/v1/objects/download" --data-urlencode 'object_key=videos/main.bin' -o "$OUT/download-after-restart.bin"
[[ "$(sha256sum "$OUT/download-after-restart.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
curl_auth "$A_BASE/noa/v1/cluster/monitor?deep_scan=true" > "$OUT/monitor-final.json"

cp "$LOCAL_A/server.log" "$OUT/node-a.log"; cp "$LOCAL_B/server.log" "$OUT/node-b.log"
cp "$LOCAL_A/pidstat.log" "$OUT/node-a-pidstat.log" 2>/dev/null || true
cp "$LOCAL_B/pidstat.log" "$OUT/node-b-pidstat.log" 2>/dev/null || true
copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/server.log" "$OUT/node-c.log" >/dev/null
copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/server.log" "$OUT/node-d.log" >/dev/null
copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/pidstat.log" "$OUT/node-c-pidstat.log" >/dev/null || true
copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/pidstat.log" "$OUT/node-d-pidstat.log" >/dev/null || true

find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -maxdepth 8 -printf '%P\n' | sort > "$OUT/layout-local.txt"
remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -maxdepth 8 -printf '%p\n' | sort" > "$OUT/layout-remote.txt"
ERROR_LINES="$({ grep -Eih 'panic|fatal|internal server error|corrupt' "$OUT"/node-*.log 2>/dev/null || true; } | wc -l | tr -d ' ')"
jq -n \
  --arg result PASS --arg cluster "$CLUSTER_ID" --arg pool "$POOL_ID" \
  --arg fragment "$FRAGMENT" --arg stable_fragment "$STABLE_FRAGMENT" \
  --arg topology 'four logical nodes on two physical hosts; static EC 2+2; NCNX R=3' \
  --arg node_loss_status "$NODE_LOSS_STATUS" --arg ttl_status "$TTL_STATUS" \
  --arg collision_recover_status "$COLLISION_RECOVER_STATUS" \
  --arg collision_first_id "$COLLISION_FIRST_ID" --arg collision_second_id "$COLLISION_SECOND_ID" \
  --argjson ncnx_replicas "$NCNX_COUNT" --argjson suspicious_log_lines "$ERROR_LINES" \
  --argjson collision_trash_replicas "$COLLISION_TRASH_COUNT" \
  --slurpfile upload "$OUT/upload-throughput.json" \
  --slurpfile list "$OUT/list-latency.json" --slurpfile stat "$OUT/stat-latency.json" \
  --slurpfile rename "$OUT/rename-latency.json" --slurpfile gc "$OUT/gc-status.json" \
  --slurpfile monitor "$OUT/monitor-final.json" \
  '{result:$result,cluster_id:$cluster,pool_id:$pool,fragment:$fragment,stable_fragment_id:$stable_fragment,topology:$topology,ec:"2+2",ncnx_replicas:$ncnx_replicas,upload:$upload[0],latency:{zero_decode_list:$list[0],stat:$stat[0],exact_rename:$rename[0]},rename_lifecycle_trash_records:0,trash_generation:{strict_recover_http_status:($collision_recover_status|tonumber),first_mutation_id:$collision_first_id,second_mutation_id:$collision_second_id,ids_are_distinct:($collision_first_id != $collision_second_id),surviving_old_generation_replicas:$collision_trash_replicas},overwrite_versions:2,parallel_independent_key_writes:8,hierarchical_pagination_keys:5,folder_trash_recovery:"2 of 2",ncnx_auto_heal:"recreated deleted replica",object_auto_heal:"recreated deleted shard",node_loss:{one_node_read_survived:true,parity_count_read_survived:true,new_write_http_status:($node_loss_status|tonumber)},ttl_gc:{expired:true,read_after_gc_http_status:($ttl_status|tonumber),status:$gc[0]},restart_persistence:true,suspicious_log_lines:$suspicious_log_lines,monitor:$monitor[0]}' \
  | tee "$OUT/summary.json"

echo "N2_3_0_PROOF_OUT=$OUT"
