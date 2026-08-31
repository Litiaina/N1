#!/usr/bin/env bash
set -Eeuo pipefail

# Finite N2 3.5 proof for one real 2+1 server pool across two physical hosts.
# Node A owns two local roots. Nodes B and C each own two remote roots. The
# caller must provide six new, isolated directories; this script never formats
# a device or reuses an existing N2 root. In addition to the normal data path,
# the proof denies one root while its node remains online, crashes coordinators
# with SIGKILL, and verifies restart-resumable durable/namespace operations.

: "${N2_ECS_BIN:?set N2_ECS_BIN to the tested litiaina-n2 release binary}"
: "${N2_ECS_OUT:?set N2_ECS_OUT to a new evidence directory}"
: "${N2_ECS_LOCAL_ROOT_1:?set N2_ECS_LOCAL_ROOT_1}"
: "${N2_ECS_LOCAL_ROOT_2:?set N2_ECS_LOCAL_ROOT_2}"
: "${N2_ECS_REMOTE_ROOT_1:?set N2_ECS_REMOTE_ROOT_1}"
: "${N2_ECS_REMOTE_ROOT_2:?set N2_ECS_REMOTE_ROOT_2}"
: "${N2_ECS_REMOTE_ROOT_3:?set N2_ECS_REMOTE_ROOT_3}"
: "${N2_ECS_REMOTE_ROOT_4:?set N2_ECS_REMOTE_ROOT_4}"

BIN="$N2_ECS_BIN"
OUT="$N2_ECS_OUT"
LOCAL_ROOT_1="$N2_ECS_LOCAL_ROOT_1"
LOCAL_ROOT_2="$N2_ECS_LOCAL_ROOT_2"
REMOTE_ROOT_1="$N2_ECS_REMOTE_ROOT_1"
REMOTE_ROOT_2="$N2_ECS_REMOTE_ROOT_2"
REMOTE_ROOT_3="$N2_ECS_REMOTE_ROOT_3"
REMOTE_ROOT_4="$N2_ECS_REMOTE_ROOT_4"
LOCAL_IP="${N2_ECS_LOCAL_IP:-192.168.254.11}"
REMOTE_IP="${N2_ECS_REMOTE_IP:-192.168.254.10}"
REMOTE_USER="${N2_ECS_REMOTE_USER:-administrator}"
SSH_CONTROL="${N2_ECS_SSH_CONTROL:-}"
RUN_ID="${N2_ECS_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
REMOTE_RUN="${N2_ECS_REMOTE_RUN:-/home/$REMOTE_USER/n2-ecs-3.5-$RUN_ID}"

A_HTTPS="${N2_ECS_A_HTTPS:-54101}"
A_NATIVE="${N2_ECS_A_NATIVE:-54102}"
B_HTTPS="${N2_ECS_B_HTTPS:-54111}"
B_NATIVE="${N2_ECS_B_NATIVE:-54112}"
C_HTTPS="${N2_ECS_C_HTTPS:-54121}"
C_NATIVE="${N2_ECS_C_NATIVE:-54122}"
A_BASE="https://$LOCAL_IP:$A_HTTPS"
B_BASE="https://$REMOTE_IP:$B_HTTPS"
C_BASE="https://$REMOTE_IP:$C_HTTPS"

LOCAL_RUN="$OUT/runtime/node-a"
REMOTE_B="$REMOTE_RUN/node-b"
REMOTE_C="$REMOTE_RUN/node-c"
CLUSTER_ID="n2-ecs-cluster-$RUN_ID"
POOL_ID="n2-ecs-pool-$RUN_ID"
MASTER="n2-ecs-master-$RUN_ID"
FRAGMENT_KEY="n2-ecs-fragment-$RUN_ID"
TOKEN=""
RESULT="FAIL"
ROOT_FALLBACK_KEY=""
ROOT_FALLBACK_REPAIR_VERSION=""
ROOT_FALLBACK_WRITE_VERSION=""
DURABLE_VERSION=""
PREFIX_OPERATION_ID=""

SSH=(ssh)
SCP=(scp)
if [[ -n "$SSH_CONTROL" ]]; then
  SSH+=( -o "ControlPath=$SSH_CONTROL" )
  SCP+=( -o "ControlPath=$SSH_CONTROL" )
fi
SSH+=( -o BatchMode=yes -o ConnectTimeout=10 "$REMOTE_USER@$REMOTE_IP" )

for tool in curl jq openssl sha256sum stat dd find awk sort; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done
[[ -x "$BIN" ]] || {
  echo "N2 binary is missing or not executable: $BIN" >&2
  exit 1
}

assert_local_root() {
  local path="$1"
  [[ "$path" == /home/litiaina/dev-test/* || "$path" == /media/litiaina/HDD0011/.n2-validation/* ]] || {
    echo "refusing unsafe local proof root: $path" >&2
    exit 1
  }
}

assert_remote_root() {
  local path="$1"
  [[ "$path" == /mnt/NDISK00[1-4]/.n2-validation/n2-ecs-3.5-* ]] || {
    echo "refusing unsafe remote proof root: $path" >&2
    exit 1
  }
}

assert_local_root "$LOCAL_ROOT_1"
assert_local_root "$LOCAL_ROOT_2"
for root in "$REMOTE_ROOT_1" "$REMOTE_ROOT_2" "$REMOTE_ROOT_3" "$REMOTE_ROOT_4"; do
  assert_remote_root "$root"
done

remote() {
  "${SSH[@]}" "$@"
}

copy_remote() {
  "${SCP[@]}" "$@"
}

kill_local() {
  if [[ -f "$LOCAL_RUN/n2.pid" ]]; then
    kill "$(<"$LOCAL_RUN/n2.pid")" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "$(<"$LOCAL_RUN/n2.pid")" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$(<"$LOCAL_RUN/n2.pid")" 2>/dev/null || true
    rm -f "$LOCAL_RUN/n2.pid"
  fi
}

stop_remote_node() {
  local run="$1"
  remote "if [ -f '$run/n2.pid' ]; then pid=\$(cat '$run/n2.pid'); kill \"\$pid\" 2>/dev/null || true; for i in \$(seq 1 50); do kill -0 \"\$pid\" 2>/dev/null || break; sleep 0.1; done; kill -9 \"\$pid\" 2>/dev/null || true; rm -f '$run/n2.pid'; fi"
}

crash_remote_node() {
  local run="$1"
  remote "if [ -f '$run/n2.pid' ]; then pid=\$(cat '$run/n2.pid'); kill -9 \"\$pid\" 2>/dev/null || true; rm -f '$run/n2.pid'; fi"
}

collect_remote_logs() {
  copy_remote "$REMOTE_USER@$REMOTE_IP:$REMOTE_B/server.log" "$OUT/node-b.log" >/dev/null 2>&1 || true
  copy_remote "$REMOTE_USER@$REMOTE_IP:$REMOTE_C/server.log" "$OUT/node-c.log" >/dev/null 2>&1 || true
}

cleanup() {
  set +e
  chmod 0755 "$LOCAL_ROOT_1" "$LOCAL_ROOT_2" >/dev/null 2>&1 || true
  remote "chmod 0755 '$REMOTE_ROOT_1' '$REMOTE_ROOT_2' '$REMOTE_ROOT_3' '$REMOTE_ROOT_4'" >/dev/null 2>&1 || true
  kill_local
  stop_remote_node "$REMOTE_B" >/dev/null 2>&1 || true
  stop_remote_node "$REMOTE_C" >/dev/null 2>&1 || true
  collect_remote_logs
  remote "rm -rf '$REMOTE_RUN' '$REMOTE_ROOT_1' '$REMOTE_ROOT_2' '$REMOTE_ROOT_3' '$REMOTE_ROOT_4'" >/dev/null 2>&1 || true
  remote "rmdir '$(dirname "$REMOTE_ROOT_1")' '$(dirname "$REMOTE_ROOT_2")' '$(dirname "$REMOTE_ROOT_3")' '$(dirname "$REMOTE_ROOT_4")'" >/dev/null 2>&1 || true
  rm -rf "$LOCAL_ROOT_1" "$LOCAL_ROOT_2"
  rmdir "$(dirname "$LOCAL_ROOT_2")" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'status=$?; echo "N2 3.5 ECS proof failed: line=$LINENO status=$status command=$BASH_COMMAND" >&2' ERR

write_env() {
  local path="$1"
  cat >"$path" <<EOF
JWT_SECRET=n2-ecs-jwt-$RUN_ID
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_CLUSTER_SECRET=n2-ecs-cluster-secret-$RUN_ID
EOF
}

write_config() {
  local path="$1" node="$2" host="$3" https_port="$4" native_port="$5" roots="$6" run="$7"
  cat >"$path" <<EOF
[server]
host=0.0.0.0
https_port=$https_port
native_port=$native_port
cert_path=$run/cert.pem
key_path=$run/key.pem

[disk_config]
storage_roots=$roots
runtime_path=$run/state

[cluster_config]
node_id=$node
pool_id=$POOL_ID
pool_generation=1
node_endpoint=https://$host:$https_port
native_endpoint=n2://$host:$native_port
membership_file=$run/state/n2_membership.json
object_data_shards=2
object_parity_shards=1
auto_rebalance=false

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=60
retention_seconds=60

[auto_healing]
enabled=true
initial_delay_seconds=2
scan_interval_seconds=5
coordinator_nodes=auto
max_parallel_fragments=3
max_objects_per_fragment_cycle=4096
max_duration_seconds=120
lease_seconds=180
checksum_scan_every_cycles=1

[reserve_set]
enabled=false
automatic_promotion=false
initial_delay_seconds=3600
probe_interval_seconds=30
failure_threshold=3
promotion_timeout_seconds=120

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
max_part_size=536870912
data_shards=2
parity_shards=1
stripe_block_size=1048576
EOF
}

start_local() {
  (
    cd "$LOCAL_RUN"
    setsid ./litiaina-n2 </dev/null >>server.log 2>&1 &
    echo $! >n2.pid
  )
}

start_remote() {
  local run="$1"
  remote "cd '$run'; setsid ./litiaina-n2 </dev/null >>server.log 2>&1 & echo \$! >n2.pid"
}

wait_ping() {
  local base="$1"
  for _ in $(seq 1 160); do
    curl -k --http1.1 -fsS "$base/noa/v1/ping" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  echo "node failed readiness: $base" >&2
  return 1
}

curl_json() {
  curl -k --http1.1 -fsS -H 'Content-Type: application/json' "$@"
}

curl_auth() {
  curl -k --http1.1 -fsS -H "Authorization: Bearer $TOKEN" "$@"
}

await_namespace_operation() {
  local base="$1" response="$2"
  local operation endpoint state
  operation="$(jq -r '.operation_id // empty' "$response")"
  [[ -n "$operation" ]] || return 0
  endpoint="$(jq -er '.status_endpoint' "$response")"
  for _ in $(seq 1 240); do
    curl_auth "$base$endpoint" >"$response.poll"
    state="$(jq -r '.state' "$response.poll")"
    case "$state" in
      completed) cp "$response.poll" "$response"; return 0 ;;
      failed|cancelled) cat "$response.poll" >&2; return 1 ;;
    esac
    sleep 0.25
  done
  echo "namespace operation timed out: $operation" >&2
  return 1
}

mkdir -p "$OUT" "$LOCAL_RUN/state" "$LOCAL_ROOT_1" "$LOCAL_ROOT_2"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=n2-3.5-ecs-proof' \
  -keyout "$OUT/key.pem" -out "$OUT/cert.pem" >/dev/null 2>&1
install -m 0755 "$BIN" "$LOCAL_RUN/litiaina-n2"
install -m 0644 "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_RUN/"
write_env "$LOCAL_RUN/n2.env"

remote "mkdir -p '$REMOTE_B/state' '$REMOTE_C/state' '$REMOTE_ROOT_1' '$REMOTE_ROOT_2' '$REMOTE_ROOT_3' '$REMOTE_ROOT_4'"
copy_remote "$BIN" "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_RUN/n2.env" "$REMOTE_USER@$REMOTE_IP:$REMOTE_B/"
copy_remote "$BIN" "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_RUN/n2.env" "$REMOTE_USER@$REMOTE_IP:$REMOTE_C/"
remote "chmod 0755 '$REMOTE_B/litiaina-n2' '$REMOTE_C/litiaina-n2'"

cat >"$OUT/n2_membership.json" <<EOF
{
  "format_version": 5,
  "cluster_id": "$CLUSTER_ID",
  "epoch": 1,
  "binding_epoch": 1,
  "pools": [{
    "id": "$POOL_ID",
    "generation": 1,
    "policy": {"object_data_shards": 2, "object_parity_shards": 1},
    "state": "writable",
    "slot_ids": ["node-a", "node-b", "node-c"]
  }],
  "slot_bindings": [
    {"slot_id":"node-a","instance_id":"node-a","binding_epoch":1,"endpoint":"$A_BASE","native_endpoint":"n2://$LOCAL_IP:$A_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-a1","path":"$LOCAL_ROOT_1"},{"id":"root-a2","path":"$LOCAL_ROOT_2"}],"joined_epoch":1},
    {"slot_id":"node-b","instance_id":"node-b","binding_epoch":1,"endpoint":"$B_BASE","native_endpoint":"n2://$REMOTE_IP:$B_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-b1","path":"$REMOTE_ROOT_1"},{"id":"root-b2","path":"$REMOTE_ROOT_2"}],"joined_epoch":1},
    {"slot_id":"node-c","instance_id":"node-c","binding_epoch":1,"endpoint":"$C_BASE","native_endpoint":"n2://$REMOTE_IP:$C_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-c1","path":"$REMOTE_ROOT_3"},{"id":"root-c2","path":"$REMOTE_ROOT_4"}],"joined_epoch":1}
  ],
  "reserve_nodes": [],
  "retired_instances": []
}
EOF
cp "$OUT/n2_membership.json" "$LOCAL_RUN/state/"
copy_remote "$OUT/n2_membership.json" "$REMOTE_USER@$REMOTE_IP:$REMOTE_B/state/"
copy_remote "$OUT/n2_membership.json" "$REMOTE_USER@$REMOTE_IP:$REMOTE_C/state/"

write_config "$LOCAL_RUN/n2_config.ini" node-a "$LOCAL_IP" "$A_HTTPS" "$A_NATIVE" "root-a1=$LOCAL_ROOT_1,root-a2=$LOCAL_ROOT_2" "$LOCAL_RUN"
write_config "$OUT/node-b.ini" node-b "$REMOTE_IP" "$B_HTTPS" "$B_NATIVE" "root-b1=$REMOTE_ROOT_1,root-b2=$REMOTE_ROOT_2" "$REMOTE_B"
write_config "$OUT/node-c.ini" node-c "$REMOTE_IP" "$C_HTTPS" "$C_NATIVE" "root-c1=$REMOTE_ROOT_3,root-c2=$REMOTE_ROOT_4" "$REMOTE_C"
copy_remote "$OUT/node-b.ini" "$REMOTE_USER@$REMOTE_IP:$REMOTE_B/n2_config.ini"
copy_remote "$OUT/node-c.ini" "$REMOTE_USER@$REMOTE_IP:$REMOTE_C/n2_config.ini"

start_remote "$REMOTE_B"
start_remote "$REMOTE_C"
start_local
wait_ping "$A_BASE"
wait_ping "$B_BASE"
wait_ping "$C_BASE"

curl_json -X POST "$A_BASE/noa/v1/fragments" \
  -d "{\"name\":\"ecs-proof\",\"secret\":\"$MASTER\",\"key\":\"$FRAGMENT_KEY\"}" >"$OUT/fragment.json"
FRAGMENT="$(jq -er '.fragment_hash' "$OUT/fragment.json")"
curl_json -X POST "$B_BASE/noa/v1/auth/fragment" \
  -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAGMENT_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" >"$OUT/auth.json"
TOKEN="$(jq -er '.access_token' "$OUT/auth.json")"
curl_auth "$A_BASE/noa/v1/cluster/health" >"$OUT/health-initial.json"
jq -e '.data_shards == 2 and .parity_shards == 1 and .ncnx_replicas == 2 and .effective_client_writable == true' "$OUT/health-initial.json" >/dev/null

dd if=/dev/urandom of="$OUT/source.bin" bs=1M count=128 status=none
SOURCE_BYTES="$(stat -c%s "$OUT/source.bin")"
SOURCE_SHA="$(sha256sum "$OUT/source.bin" | awk '{print $1}')"
started="$(date +%s%N)"
curl_json -X POST "$A_BASE/noa/v1/upload/multipart/init/ecs/large.bin" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$SOURCE_BYTES,\"expected_parts\":1,\"part_size_bytes\":$SOURCE_BYTES}" >"$OUT/upload-init.json"
VERSION="$(jq -er '.version_id' "$OUT/upload-init.json")"
SESSION="$(jq -er '.session_id' "$OUT/upload-init.json")"
curl_auth -X PUT "$A_BASE/noa/v1/upload/multipart/$VERSION/0" --data-binary @"$OUT/source.bin" >"$OUT/upload-part.json"
curl_json -X POST "$A_BASE/noa/v1/upload/multipart/$VERSION/$SESSION/finalize?object_key=ecs/large.bin" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$SOURCE_BYTES,\"expected_parts\":1,\"part_size_bytes\":$SOURCE_BYTES}" >"$OUT/upload-finalize.json"
upload_seconds="$(awk -v ns="$(( $(date +%s%N) - started ))" 'BEGIN {printf "%.6f", ns/1000000000}')"

for spec in "a:$A_BASE" "b:$B_BASE" "c:$C_BASE"; do
  name="${spec%%:*}"
  base="${spec#*:}"
  seconds="$(curl -k --http1.1 -fsS -o "$OUT/download-$name.bin" -w '%{time_total}' \
    -G "$base/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'object_key=ecs/large.bin')"
  [[ "$(sha256sum "$OUT/download-$name.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
  jq -n --argjson bytes "$SOURCE_BYTES" --arg seconds "$seconds" \
    '{bytes:$bytes,seconds:($seconds|tonumber),mib_per_second:(($bytes/1048576)/($seconds|tonumber))}' >"$OUT/download-$name-throughput.json"
done
jq -n --argjson bytes "$SOURCE_BYTES" --arg seconds "$upload_seconds" \
  '{bytes:$bytes,seconds:($seconds|tonumber),mib_per_second:(($bytes/1048576)/($seconds|tonumber))}' >"$OUT/upload-throughput.json"

for index in $(seq 1 96); do
  printf 'ecs-small-%06d' "$index" >"$OUT/small.bin"
  case $((index % 3)) in
    0) base="$A_BASE" ;;
    1) base="$B_BASE" ;;
    *) base="$C_BASE" ;;
  esac
  curl_auth -X POST "$base/noa/v1/upload/one-shot/small/object-$index.txt" \
    --data-binary @"$OUT/small.bin" >"$OUT/small-$index.json" &
  if (( index % 12 == 0 )); then wait; fi
done
wait

continuation=""
: >"$OUT/listed-keys.txt"
while :; do
  if [[ -n "$continuation" ]]; then
    curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=small/' \
      --data-urlencode 'limit=17' --data-urlencode "continuation_token=$continuation" >"$OUT/list-page.json"
  else
    curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=small/' \
      --data-urlencode 'limit=17' >"$OUT/list-page.json"
  fi
  jq -r '.entries[].record' "$OUT/list-page.json" >>"$OUT/listed-keys.txt"
  continuation="$(jq -r '.continuation_token // empty' "$OUT/list-page.json")"
  [[ -n "$continuation" ]] || break
done
[[ "$(sort -u "$OUT/listed-keys.txt" | wc -l)" -eq 96 ]]

curl_json -X POST "$C_BASE/noa/v1/posix/rename" -H "Authorization: Bearer $TOKEN" \
  -d '{"source_path":"small","destination_path":"renamed"}' >"$OUT/rename.json"
await_namespace_operation "$C_BASE" "$OUT/rename.json"
curl_auth -G "$A_BASE/noa/v1/objects" --data-urlencode 'prefix=renamed/' --data-urlencode 'limit=128' >"$OUT/renamed-list.json"
jq -e '.entries | length == 96' "$OUT/renamed-list.json" >/dev/null

for root in "$LOCAL_ROOT_1" "$LOCAL_ROOT_2"; do
  count="$(find "$root" -type f \( -name '*.shard' -o -name '*.parity' \) | wc -l)"
  [[ "$count" -gt 0 ]]
  printf '%s %s\n' "$root" "$count" >>"$OUT/local-root-distribution.txt"
done
remote "for root in '$REMOTE_ROOT_1' '$REMOTE_ROOT_2' '$REMOTE_ROOT_3' '$REMOTE_ROOT_4'; do count=\$(find \"\$root\" -type f \( -name '*.shard' -o -name '*.parity' \) | wc -l); test \"\$count\" -gt 0; printf '%s %s\\n' \"\$root\" \"\$count\"; done" >"$OUT/remote-root-distribution.txt"

# Prove that an N2 node is the EC failure domain, not one permanently bound
# disk. Select an object whose node-A piece resides on root-a1, deny that root,
# then require repair and a new version to use root-a2 while node A stays live.
for candidate in $(seq 1 64); do
  ROOT_FALLBACK_KEY="disk-fallback/candidate-$candidate.bin"
  curl_auth -X POST "$B_BASE/noa/v1/upload/one-shot/$ROOT_FALLBACK_KEY" \
    --data-binary @"$OUT/small.bin" >"$OUT/root-fallback-candidate-$candidate.json"
  ROOT_FALLBACK_REPAIR_VERSION="$(jq -er '.version_id' "$OUT/root-fallback-candidate-$candidate.json")"
  if find "$LOCAL_ROOT_1" -path "*/$ROOT_FALLBACK_REPAIR_VERSION/part_0_*" -type f -print -quit | grep -q .; then
    break
  fi
  ROOT_FALLBACK_KEY=""
  ROOT_FALLBACK_REPAIR_VERSION=""
done
[[ -n "$ROOT_FALLBACK_KEY" && -n "$ROOT_FALLBACK_REPAIR_VERSION" ]]
if find "$LOCAL_ROOT_2" -path "*/$ROOT_FALLBACK_REPAIR_VERSION/part_0_*" -type f -print -quit | grep -q .; then
  echo "selected root-fallback piece already exists on both local roots" >&2
  exit 1
fi

chmod 0000 "$LOCAL_ROOT_1"
sleep 1
curl_auth "$B_BASE/noa/v1/cluster/health" >"$OUT/health-local-root-lost.json"
jq -e '
  .effective_client_writable == true
  and .write_authority_proven == true
  and any(.nodes[];
    .node_id == "node-a"
    and .accepts_writes == true
    and .storage_readable == true
    and .storage_writable == true
    and (.storage_issues | length) > 0)
' "$OUT/health-local-root-lost.json" >/dev/null

for base in "$A_BASE" "$B_BASE" "$C_BASE"; do
  curl -k --http1.1 -sS -X POST "$base/noa/v1/cluster/healing/cycle" \
    -H "Authorization: Bearer $TOKEN" >"$OUT/root-fallback-heal-$(echo "$base" | tr ':/' '_').json" || true
done
for _ in $(seq 1 240); do
  if find "$LOCAL_ROOT_2" -path "*/$ROOT_FALLBACK_REPAIR_VERSION/part_0_*" -type f -print -quit | grep -q .; then
    break
  fi
  sleep 0.25
done
find "$LOCAL_ROOT_2" -path "*/$ROOT_FALLBACK_REPAIR_VERSION/part_0_*" -type f -print -quit | grep -q .

curl_auth -X POST "$C_BASE/noa/v1/upload/one-shot/$ROOT_FALLBACK_KEY" \
  --data-binary @"$OUT/small.bin" >"$OUT/root-fallback-write.json"
ROOT_FALLBACK_WRITE_VERSION="$(jq -er '.version_id' "$OUT/root-fallback-write.json")"
find "$LOCAL_ROOT_2" -path "*/$ROOT_FALLBACK_WRITE_VERSION/part_0_*" -type f -print -quit | grep -q .
curl_auth -G "$A_BASE/noa/v1/objects/download" \
  --data-urlencode "object_key=$ROOT_FALLBACK_KEY" -o "$OUT/root-fallback-download.bin"
cmp -s "$OUT/small.bin" "$OUT/root-fallback-download.bin"

chmod 0755 "$LOCAL_ROOT_1"
curl_auth "$C_BASE/noa/v1/cluster/health" >"$OUT/health-local-root-restored.json"
jq -e '
  .effective_client_writable == true
  and any(.nodes[];
    .node_id == "node-a"
    and .storage_readable == true
    and .storage_writable == true
    and (.storage_issues | length) == 0)
' "$OUT/health-local-root-restored.json" >/dev/null

# Durable multipart must recover its compact receipts after the owning
# coordinator is killed. No standard in-memory upload guarantee is inferred.
dd if="$OUT/source.bin" of="$OUT/durable-source.bin" bs=1M count=32 status=none
dd if="$OUT/durable-source.bin" of="$OUT/durable-part-0.bin" bs=1M count=16 status=none
dd if="$OUT/durable-source.bin" of="$OUT/durable-part-1.bin" bs=1M skip=16 count=16 status=none
DURABLE_BYTES="$(stat -c%s "$OUT/durable-source.bin")"
DURABLE_PART_BYTES="$(stat -c%s "$OUT/durable-part-0.bin")"
DURABLE_SHA="$(sha256sum "$OUT/durable-source.bin" | awk '{print $1}')"
curl_json -X POST "$B_BASE/noa/v1/upload/multipart/durable/init/crash/durable.bin" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$DURABLE_BYTES,\"expected_parts\":2,\"part_size_bytes\":$DURABLE_PART_BYTES}" >"$OUT/durable-init.json"
DURABLE_VERSION="$(jq -er '.version_id' "$OUT/durable-init.json")"
DURABLE_SESSION="$(jq -er '.session_id' "$OUT/durable-init.json")"
# This proof initializes directly against node B, so node B owns the session.
# coordinator_node_id is gateway routing metadata and is not part of the
# direct-engine initialization response.
curl_auth -X PUT "$B_BASE/noa/v1/upload/multipart/durable/$DURABLE_VERSION/0" \
  --data-binary @"$OUT/durable-part-0.bin" >"$OUT/durable-part-0.json"
curl_auth -X PUT "$B_BASE/noa/v1/upload/multipart/durable/$DURABLE_VERSION/1" \
  --data-binary @"$OUT/durable-part-1.bin" >"$OUT/durable-part-1.json"
crash_remote_node "$REMOTE_B"
sleep 1
start_remote "$REMOTE_B"
wait_ping "$B_BASE"
curl_auth "$B_BASE/noa/v1/upload/multipart/durable/$DURABLE_VERSION" >"$OUT/durable-status-after-crash.json"
jq -e '(.uploaded_parts | sort) == [0,1]' "$OUT/durable-status-after-crash.json" >/dev/null
curl_json -X POST "$B_BASE/noa/v1/upload/multipart/durable/$DURABLE_VERSION/$DURABLE_SESSION/finalize?object_key=crash/durable.bin" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$DURABLE_BYTES,\"expected_parts\":2,\"part_size_bytes\":$DURABLE_PART_BYTES}" >"$OUT/durable-finalize.json"
curl_auth -G "$C_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=crash/durable.bin' -o "$OUT/durable-download.bin"
[[ "$(sha256sum "$OUT/durable-download.bin" | awk '{print $1}')" == "$DURABLE_SHA" ]]

# Prefix mutation is persisted before execution. Kill its coordinator after
# acceptance, require a nonterminal record on disk, then let restart recovery
# resume the same mutation ULID to completion without duplicate publication.
curl_json -X POST "$C_BASE/noa/v1/posix/rename" -H "Authorization: Bearer $TOKEN" \
  -d '{"source_path":"renamed","destination_path":"restart-renamed"}' >"$OUT/restart-prefix-rename.json"
PREFIX_OPERATION_ID="$(jq -er '.operation_id' "$OUT/restart-prefix-rename.json")"
crash_remote_node "$REMOTE_C"
PREFIX_RECORD="$REMOTE_C/state/.n2/namespace-operations/$PREFIX_OPERATION_ID.json"
remote "test -f '$PREFIX_RECORD'"
[[ -n "$PREFIX_RECORD" ]]
PREFIX_STATE_AFTER_CRASH="$(remote "jq -r '.state' '$PREFIX_RECORD'")"
[[ "$PREFIX_STATE_AFTER_CRASH" == "planning" || "$PREFIX_STATE_AFTER_CRASH" == "running" ]]
start_remote "$REMOTE_C"
wait_ping "$C_BASE"
await_namespace_operation "$C_BASE" "$OUT/restart-prefix-rename.json"
jq -e '.state == "completed" and .affected_paths == 96' "$OUT/restart-prefix-rename.json" >/dev/null
curl_auth -G "$A_BASE/noa/v1/objects" \
  --data-urlencode 'prefix=restart-renamed/' --data-urlencode 'limit=128' >"$OUT/restart-renamed-list.json"
jq -e '.entries | length == 96' "$OUT/restart-renamed-list.json" >/dev/null

stop_remote_node "$REMOTE_C"
sleep 2
DEGRADED_STATUS="$(curl -k --http1.1 -sS -o "$OUT/degraded-upload.json" -w '%{http_code}' \
  -X POST "$A_BASE/noa/v1/upload/one-shot/degraded/survivor.txt" \
  -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/small.bin")"
[[ "$DEGRADED_STATUS" == 2* ]]
curl_auth -G "$B_BASE/noa/v1/objects/download" --data-urlencode 'object_key=degraded/survivor.txt' -o "$OUT/degraded-download.bin"
cmp -s "$OUT/small.bin" "$OUT/degraded-download.bin"
start_remote "$REMOTE_C"
wait_ping "$C_BASE"

SHARD_TO_REMOVE="$(remote "find '$REMOTE_ROOT_3' '$REMOTE_ROOT_4' -path '*/$VERSION/part_0_*' -type f | head -1")"
[[ -n "$SHARD_TO_REMOVE" ]]
remote "rm -f '$SHARD_TO_REMOVE'"
curl_auth -G "$A_BASE/noa/v1/objects/download" --data-urlencode 'object_key=ecs/large.bin' -o "$OUT/download-reconstructed.bin"
[[ "$(sha256sum "$OUT/download-reconstructed.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
for base in "$A_BASE" "$B_BASE" "$C_BASE"; do
  curl -k --http1.1 -sS -X POST "$base/noa/v1/cluster/healing/cycle" \
    -H "Authorization: Bearer $TOKEN" >"$OUT/heal-$(echo "$base" | tr ':/' '_').json" || true
done
for _ in $(seq 1 240); do
  remote "test -f '$SHARD_TO_REMOVE'" && break
  sleep 0.25
done
remote "test -f '$SHARD_TO_REMOVE'"

kill_local
stop_remote_node "$REMOTE_B"
stop_remote_node "$REMOTE_C"
start_remote "$REMOTE_B"
start_remote "$REMOTE_C"
start_local
wait_ping "$A_BASE"
wait_ping "$B_BASE"
wait_ping "$C_BASE"
curl_auth -G "$C_BASE/noa/v1/objects/download" --data-urlencode 'object_key=ecs/large.bin' -o "$OUT/download-after-restart.bin"
[[ "$(sha256sum "$OUT/download-after-restart.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
curl_auth "$A_BASE/noa/v1/cluster/health" >"$OUT/health-final.json"
jq -e '.effective_client_writable == true and (.state == "healthy" or .state == "degraded")' "$OUT/health-final.json" >/dev/null

collect_remote_logs
cp "$LOCAL_RUN/server.log" "$OUT/node-a.log"
PANIC_LINES="$(cat "$OUT"/node-*.log | grep -Eic 'panicked|panic|fatal|segmentation fault' || true)"
[[ "$PANIC_LINES" -eq 0 ]]

BIN_SHA="$(sha256sum "$BIN" | awk '{print $1}')"
jq -n \
  --arg result PASS \
  --arg run_id "$RUN_ID" \
  --arg binary_sha256 "$BIN_SHA" \
  --arg fragment "$FRAGMENT" \
  --arg degraded_status "$DEGRADED_STATUS" \
  --arg root_fallback_repair_version "$ROOT_FALLBACK_REPAIR_VERSION" \
  --arg root_fallback_write_version "$ROOT_FALLBACK_WRITE_VERSION" \
  --arg durable_version "$DURABLE_VERSION" \
  --arg prefix_operation_id "$PREFIX_OPERATION_ID" \
  --arg prefix_state_after_crash "$PREFIX_STATE_AFTER_CRASH" \
  --slurpfile upload "$OUT/upload-throughput.json" \
  --slurpfile download_a "$OUT/download-a-throughput.json" \
  --slurpfile download_b "$OUT/download-b-throughput.json" \
  --slurpfile download_c "$OUT/download-c-throughput.json" \
  --slurpfile initial "$OUT/health-initial.json" \
  --slurpfile final "$OUT/health-final.json" \
  '{result:$result,run_id:$run_id,binary_sha256:$binary_sha256,topology:{physical_hosts:2,logical_nodes:3,roots_per_node:2,ec:"2+1",ncnx_replicas:2},fragment:$fragment,byte_exact_downloads:["node-a","node-b","node-c","root-fallback","durable-after-coordinator-crash","reconstructed","post-restart"],small_objects:96,paginated_listing:true,prefix_rename_objects:96,every_root_received_pieces:true,local_root_loss:{node_remained_writable:true,missing_piece_relocated:true,write_used_surviving_root:true,repair_version:$root_fallback_repair_version,write_version:$root_fallback_write_version},durable_multipart_coordinator_crash:{receipts_recovered:true,finalized_byte_exactly:true,version:$durable_version},prefix_operation_coordinator_crash:{state_after_crash:$prefix_state_after_crash,resumed_same_operation:true,operation_id:$prefix_operation_id,affected_paths:96},degraded_write_http_status:($degraded_status|tonumber),missing_shard_reconstructed_and_healed:true,restart_persistence:true,throughput:{upload:$upload[0],download_node_a:$download_a[0],download_node_b:$download_b[0],download_node_c:$download_c[0]},health:{initial:$initial[0],final:$final[0]}}' >"$OUT/summary.json"

RESULT="PASS"
echo "N2 3.5 ECS two-host proof: PASS"
cat "$OUT/summary.json"
