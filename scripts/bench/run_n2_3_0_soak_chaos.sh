#!/usr/bin/env bash
set -Eeuo pipefail

# N2 3.3 soak and bounded-chaos qualification; the filename is retained for
# the flat-NCNX proof family. The harness uses four logical nodes, static 2+2
# EC, and NCNX R=3. It never installs a service or uses a pre-existing storage
# directory.

ALL_LOCAL="${ALL_LOCAL:-false}"
REMOTE_HOST="${REMOTE_HOST:-192.168.0.36}"
REMOTE_USER="${REMOTE_USER:-administrator}"
if [[ "$ALL_LOCAL" == "true" ]]; then
  LOCAL_IP="${LOCAL_IP:-127.0.0.1}"
  REMOTE_IP="${REMOTE_IP:-127.0.0.1}"
  SSH_IDENTITY_FILE="${SSH_IDENTITY_FILE:-}"
  TOPOLOGY_LABEL="four logical nodes on one physical host and four isolated HDD roots; static EC 2+2; NCNX R=3"
else
  : "${SSH_IDENTITY_FILE:?set SSH_IDENTITY_FILE to an authorized temporary key}"
  LOCAL_IP="${LOCAL_IP:-192.168.0.1}"
  REMOTE_IP="${REMOTE_IP:-$REMOTE_HOST}"
  TOPOLOGY_LABEL="four logical nodes on two physical hosts; static EC 2+2; NCNX R=3"
fi
DURATION_SECONDS="${DURATION_SECONDS:-43200}"
MAX_CYCLES="${MAX_CYCLES:-0}"
CYCLE_PAUSE_SECONDS="${CYCLE_PAUSE_SECONDS:-8}"
CHAOS_EVERY_CYCLES="${CHAOS_EVERY_CYCLES:-20}"
ROLLING_LIVE_OBJECTS="${ROLLING_LIVE_OBJECTS:-100}"
PAYLOAD_KIB="${PAYLOAD_KIB:-256}"
ANCHOR_MIB="${ANCHOR_MIB:-8}"
MULTIPART_PART_MIB="${MULTIPART_PART_MIB:-512}"
STRIPE_BLOCK_KIB="${STRIPE_BLOCK_KIB:-4096}"
FAULT_DWELL_SECONDS="${FAULT_DWELL_SECONDS:-2}"
NORMAL_UPLOAD_ATTEMPTS="${NORMAL_UPLOAD_ATTEMPTS:-3}"
HEAL_INITIAL_DELAY_SECONDS="${HEAL_INITIAL_DELAY_SECONDS:-10}"
HEAL_SCAN_INTERVAL_SECONDS="${HEAL_SCAN_INTERVAL_SECONDS:-60}"
HEAL_VERIFY_TIMEOUT_SECONDS="${HEAL_VERIFY_TIMEOUT_SECONDS:-360}"
HEAL_STATUS_POLL_SECONDS="${HEAL_STATUS_POLL_SECONDS:-1}"
HEAL_TRIGGER_REQUEST_TIMEOUT_SECONDS="${HEAL_TRIGGER_REQUEST_TIMEOUT_SECONDS:-180}"
CHAOS_START_PHASE="${CHAOS_START_PHASE:-1}"
AUTHORIZED_KEY_TAG="${AUTHORIZED_KEY_TAG:-}"
REMOVE_IDENTITY_ON_EXIT="${REMOVE_IDENTITY_ON_EXIT:-false}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${N2_SOAK_BIN:-$ROOT_DIR/target/release/litiaina-n2}"
TS="${SOAK_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
OUT_ROOT="${N2_SOAK_OUT_ROOT:-/home/wlzambo/DEV-TEST}"
OUT="$OUT_ROOT/N2-3.0-soak-$TS"
LOCAL_RUN="$OUT/runtime"
LOCAL_A="$LOCAL_RUN/node-a"
LOCAL_B="$LOCAL_RUN/node-b"
LOCAL_A_STORE="${N2_SOAK_A_STORE:-$OUT/storage/node-a}"
LOCAL_B_STORE="${N2_SOAK_B_STORE:-$OUT/storage/node-b}"
REMOTE_RUN_BASE="/home/$REMOTE_USER/.cache/N2-3.0-soak-$TS"
REMOTE_C="$REMOTE_RUN_BASE/node-c"
REMOTE_D="$REMOTE_RUN_BASE/node-d"
REMOTE_STORE_BASE=".n2-3.0-soak-$TS"
REMOTE_C_STORE="${N2_SOAK_C_STORE:-/mnt/NDISK002/$REMOTE_STORE_BASE/node-c}"
REMOTE_D_STORE="${N2_SOAK_D_STORE:-/mnt/NDISK003/$REMOTE_STORE_BASE/node-d}"

A_HTTPS=24001; A_NATIVE=24002
B_HTTPS=24011; B_NATIVE=24012
C_HTTPS=24021; C_NATIVE=24022
D_HTTPS=24031; D_NATIVE=24032
A_BASE="https://$LOCAL_IP:$A_HTTPS"
B_BASE="https://$LOCAL_IP:$B_HTTPS"
C_BASE="https://$REMOTE_IP:$C_HTTPS"
D_BASE="https://$REMOTE_IP:$D_HTTPS"
BASES=("$A_BASE" "$B_BASE" "$C_BASE" "$D_BASE")
POOL_ID="pool-soak-$TS"
CLUSTER_ID="cluster-soak-$TS"
MASTER="n2-3.0-soak-master-$TS"
FRAGMENT_NAME="n2-3.0-soak-$TS"
FRAGMENT_KEY="n2-3.0-fragment-$TS"
TOKEN=""
FRAGMENT=""
STABLE_FRAGMENT=""
ANCHOR_VERSION=""
ANCHOR_SHA=""
START_EPOCH="$(date +%s)"
DEADLINE=$((START_EPOCH + DURATION_SECONDS))
CYCLE=0
CHAOS_EVENTS=0
RESULT="RUNNING"
LAST_OPERATION="initializing"
FAILURE_CLASS="unclassified"
FAILURE_DETAIL=""

if [[ "$ALL_LOCAL" != "true" ]]; then
  SSH=(ssh -i "$SSH_IDENTITY_FILE" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8 "$REMOTE_USER@$REMOTE_HOST")
  SCP=(scp -i "$SSH_IDENTITY_FILE" -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8)
fi

for tool in jq curl openssl pidstat sha256sum awk dd; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done
if [[ "$ALL_LOCAL" != "true" ]]; then
  for tool in ssh scp; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
  done
fi
[[ -x "$BIN" ]] || { echo "release binary is missing: $BIN" >&2; exit 1; }
[[ "$DURATION_SECONDS" =~ ^[0-9]+$ && "$DURATION_SECONDS" -gt 0 ]]
[[ "$MAX_CYCLES" =~ ^[0-9]+$ ]]
[[ "$CHAOS_EVERY_CYCLES" =~ ^[0-9]+$ && "$CHAOS_EVERY_CYCLES" -gt 0 ]]
[[ "$NORMAL_UPLOAD_ATTEMPTS" =~ ^[0-9]+$ && "$NORMAL_UPLOAD_ATTEMPTS" -gt 0 ]]
[[ "$MULTIPART_PART_MIB" =~ ^[0-9]+$ && "$MULTIPART_PART_MIB" -gt 0 ]]
[[ "$STRIPE_BLOCK_KIB" =~ ^[0-9]+$ && "$STRIPE_BLOCK_KIB" -gt 0 ]]
(( STRIPE_BLOCK_KIB % 4 == 0 ))
[[ "$HEAL_INITIAL_DELAY_SECONDS" =~ ^[0-9]+$ && "$HEAL_INITIAL_DELAY_SECONDS" -gt 0 ]]
[[ "$HEAL_SCAN_INTERVAL_SECONDS" =~ ^[0-9]+$ && "$HEAL_SCAN_INTERVAL_SECONDS" -gt 0 ]]
[[ "$HEAL_VERIFY_TIMEOUT_SECONDS" =~ ^[0-9]+$ && "$HEAL_VERIFY_TIMEOUT_SECONDS" -ge 180 ]]
[[ "$HEAL_STATUS_POLL_SECONDS" =~ ^[0-9]+$ && "$HEAL_STATUS_POLL_SECONDS" -gt 0 ]]
[[ "$HEAL_TRIGGER_REQUEST_TIMEOUT_SECONDS" =~ ^[0-9]+$ && "$HEAL_TRIGGER_REQUEST_TIMEOUT_SECONDS" -ge 120 ]]
[[ "$CHAOS_START_PHASE" =~ ^[1-6]$ ]]

assert_ports_available() {
  local port
  command -v ss >/dev/null || {
    echo "missing required tool: ss" >&2
    exit 1
  }
  for port in "$@"; do
    if [[ -n "$(ss -H -ltn "sport = :$port")" ]]; then
      echo "required N2 soak port is already in use: $port" >&2
      exit 1
    fi
  done
}
assert_ports_available \
  "$A_HTTPS" "$A_NATIVE" "$B_HTTPS" "$B_NATIVE" \
  "$C_HTTPS" "$C_NATIVE" "$D_HTTPS" "$D_NATIVE"

mkdir -p "$OUT" "$OUT/metrics" "$OUT/events" "$OUT/payloads"
: > "$OUT/metrics/operations.jsonl"
: > "$OUT/events/chaos.jsonl"
: > "$OUT/events/health.jsonl"
: > "$OUT/events/lifecycle.jsonl"
: > "$OUT/events/availability.jsonl"

remote() {
  if [[ "$ALL_LOCAL" == "true" ]]; then
    bash -lc "$*"
  else
    "${SSH[@]}" "$@"
  fi
}

copy_remote() {
  if [[ "$ALL_LOCAL" != "true" ]]; then
    "${SCP[@]}" "$@"
    return
  fi

  local args=("$@")
  local last_index=$(( ${#args[@]} - 1 ))
  local destination="${args[$last_index]}"
  if [[ "${args[0]}" == *:* ]]; then
    cp "${args[0]#*:}" "$destination"
    return
  fi
  destination="${destination#*:}"
  cp "${args[@]:0:$last_index}" "$destination"
}

write_status() {
  local state="$1" message="$2" now elapsed temporary
  now="$(date +%s)"
  elapsed=$((now - START_EPOCH))
  temporary="$OUT/status.json.tmp"
  jq -n \
    --arg state "$state" --arg message "$message" --arg last_operation "$LAST_OPERATION" \
    --argjson cycle "$CYCLE" --argjson chaos_events "$CHAOS_EVENTS" \
    --argjson started_at "$START_EPOCH" --argjson updated_at "$now" \
    --argjson elapsed_seconds "$elapsed" --argjson requested_seconds "$DURATION_SECONDS" \
    '{state:$state,message:$message,cycle:$cycle,chaos_events:$chaos_events,last_operation:$last_operation,started_at_unix:$started_at,updated_at_unix:$updated_at,elapsed_seconds:$elapsed_seconds,requested_seconds:$requested_seconds}' \
    > "$temporary"
  mv "$temporary" "$OUT/status.json"
}

event() {
  local kind="$1" state="$2" detail="$3"
  jq -nc --arg kind "$kind" --arg state "$state" --arg detail "$detail" \
    --argjson cycle "$CYCLE" --argjson at "$(date +%s)" \
    '{at_unix:$at,cycle:$cycle,kind:$kind,state:$state,detail:$detail}' >> "$OUT/events/chaos.jsonl"
}

metric() {
  local operation="$1" node="$2" seconds="$3" bytes="$4"
  jq -nc --arg operation "$operation" --arg node "$node" \
    --argjson cycle "$CYCLE" --argjson at "$(date +%s)" \
    --argjson seconds "$seconds" --argjson bytes "$bytes" \
    '{at_unix:$at,cycle:$cycle,operation:$operation,node:$node,seconds:$seconds,milliseconds:($seconds*1000),bytes:$bytes}' \
    >> "$OUT/metrics/operations.jsonl"
}

upload_with_retry() {
  local operation="$1" base="$2" key="$3" file="$4" response="$5"
  local attempt measurement status seconds curl_code=0 bytes detail
  bytes="$(stat -c%s "$file")"
  for attempt in $(seq 1 "$NORMAL_UPLOAD_ATTEMPTS"); do
    curl_code=0
    measurement="$(curl -k --http1.1 --connect-timeout 5 --max-time 180 -sS \
      -o "$response" -w '%{http_code} %{time_total}' -X POST \
      "$base/noa/v1/upload/one-shot/$key" -H "Authorization: Bearer $TOKEN" \
      --data-binary @"$file")" || curl_code=$?
    status="${measurement%% *}"
    seconds="${measurement#* }"
    if [[ "$curl_code" -eq 0 && "$status" =~ ^2[0-9][0-9]$ ]]; then
      metric "$operation" "$base" "$seconds" "$bytes"
      return 0
    fi
    detail="$(tr '\r\n' '  ' < "$response" 2>/dev/null | head -c 500 || true)"
    jq -nc --arg operation "$operation" --arg node "$base" --arg key "$key" \
      --arg status "$status" --arg detail "$detail" --argjson curl_code "$curl_code" \
      --argjson attempt "$attempt" --argjson cycle "$CYCLE" --argjson at "$(date +%s)" \
      '{at_unix:$at,cycle:$cycle,operation:$operation,node:$node,key:$key,attempt:$attempt,http_status:$status,curl_code:$curl_code,detail:$detail}' \
      >> "$OUT/events/availability.jsonl"
    sleep "$attempt"
  done
  echo "normal upload failed after $NORMAL_UPLOAD_ATTEMPTS attempts: $key" >&2
  return 1
}

multipart_upload() {
  local operation="$1" base="$2" key="$3" file="$4" response="$5"
  local bytes part_size expected_parts version session part part_file part_offset part_bytes
  local started_ns finished_ns seconds
  bytes="$(stat -c%s "$file")"
  part_size=$((MULTIPART_PART_MIB * 1024 * 1024))
  expected_parts=$(( (bytes + part_size - 1) / part_size ))
  part_file="$OUT/payloads/multipart-current.part"
  started_ns="$(date +%s%N)"

  curl_json -X POST "$base/noa/v1/upload/multipart/init/$key" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$bytes,\"expected_parts\":$expected_parts,\"part_size_bytes\":$part_size}" \
    >"$OUT/events/$operation-init.json"
  version="$(jq -er '.version_id' "$OUT/events/$operation-init.json")"
  session="$(jq -er '.session_id' "$OUT/events/$operation-init.json")"

  for ((part = 0; part < expected_parts; part++)); do
    if (( expected_parts == 1 )); then
      part_file="$file"
    else
      part_offset=$((part * part_size))
      part_bytes=$((bytes - part_offset))
      (( part_bytes > part_size )) && part_bytes="$part_size"
      dd if="$file" of="$part_file" bs=4M iflag=skip_bytes,count_bytes \
        skip="$part_offset" count="$part_bytes" status=none
    fi
    curl_auth -X PUT "$base/noa/v1/upload/multipart/$version/$part" \
      --data-binary @"$part_file" >"$OUT/events/$operation-part-$part.json"
  done
  (( expected_parts == 1 )) || rm -f "$part_file"

  curl_json -X POST \
    "$base/noa/v1/upload/multipart/$version/$session/finalize?object_key=$key" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$bytes,\"expected_parts\":$expected_parts,\"part_size_bytes\":$part_size}" \
    >"$response"
  jq -e --arg version "$version" '.version_id == $version' "$response" >/dev/null

  finished_ns="$(date +%s%N)"
  seconds="$(awk -v start="$started_ns" -v finish="$finished_ns" \
    'BEGIN { printf "%.9f", (finish - start) / 1000000000 }')"
  metric "$operation" "$base" "$seconds" "$bytes"
}

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

stop_remote_node() {
  local run="$1"
  remote "if [ -f '$run/pidstat.pid' ]; then kill \$(cat '$run/pidstat.pid') 2>/dev/null || true; rm -f '$run/pidstat.pid'; fi; if [ -f '$run/n2.pid' ]; then kill \$(cat '$run/n2.pid') 2>/dev/null || true; rm -f '$run/n2.pid'; fi"
}

collect_remote_logs() {
  set +e
  copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/server.log" "$OUT/node-c.log" >/dev/null 2>&1
  copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/server.log" "$OUT/node-d.log" >/dev/null 2>&1
  copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/pidstat.log" "$OUT/node-c-pidstat.log" >/dev/null 2>&1
  copy_remote "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/pidstat.log" "$OUT/node-d-pidstat.log" >/dev/null 2>&1
  set -e
}

remove_remote_authorized_key() {
  [[ "$ALL_LOCAL" != "true" ]] || return 0
  [[ -n "$AUTHORIZED_KEY_TAG" ]] || return 0
  remote "set -e; file=\"\$HOME/.ssh/authorized_keys\"; tmp=\"\$file.n2-soak-$TS\"; if [ -f \"\$file\" ]; then grep -vF '$AUTHORIZED_KEY_TAG' \"\$file\" > \"\$tmp\" || true; chmod 600 \"\$tmp\"; mv \"\$tmp\" \"\$file\"; fi" >/dev/null 2>&1 || true
}

cleanup() {
  set +e
  write_status "$RESULT" "test process exited" 2>/dev/null
  cp "$LOCAL_A/server.log" "$OUT/node-a.log" 2>/dev/null
  cp "$LOCAL_B/server.log" "$OUT/node-b.log" 2>/dev/null
  cp "$LOCAL_A/pidstat.log" "$OUT/node-a-pidstat.log" 2>/dev/null
  cp "$LOCAL_B/pidstat.log" "$OUT/node-b-pidstat.log" 2>/dev/null
  collect_remote_logs
  kill_pidfile "$LOCAL_A/pidstat.pid"
  kill_pidfile "$LOCAL_B/pidstat.pid"
  kill_pidfile "$LOCAL_A/n2.pid"
  kill_pidfile "$LOCAL_B/n2.pid"
  stop_remote_node "$REMOTE_C" >/dev/null 2>&1
  stop_remote_node "$REMOTE_D" >/dev/null 2>&1
  if [[ "$RESULT" == "PASS" ]]; then
    rm -rf -- "$LOCAL_A_STORE" "$LOCAL_B_STORE"
    rmdir "$(dirname "$LOCAL_A_STORE")" "$(dirname "$(dirname "$LOCAL_A_STORE")")" \
      "$(dirname "$LOCAL_B_STORE")" "$(dirname "$(dirname "$LOCAL_B_STORE")")" \
      2>/dev/null || true
    remote "rm -rf '$REMOTE_RUN_BASE' '$REMOTE_C_STORE' '$REMOTE_D_STORE'; rmdir '/mnt/NDISK002/$REMOTE_STORE_BASE' '/mnt/NDISK003/$REMOTE_STORE_BASE' 2>/dev/null || true" >/dev/null 2>&1
    remote "rmdir \
      '$(dirname "$REMOTE_C_STORE")' '$(dirname "$(dirname "$REMOTE_C_STORE")")' \
      '$(dirname "$REMOTE_D_STORE")' '$(dirname "$(dirname "$REMOTE_D_STORE")")' \
      2>/dev/null || true" >/dev/null 2>&1
  else
    jq -n \
      --arg result "$RESULT" \
      --arg failure_class "$FAILURE_CLASS" \
      --arg failure_detail "$FAILURE_DETAIL" \
      --arg local_a "$LOCAL_A_STORE" \
      --arg local_b "$LOCAL_B_STORE" \
      --arg remote_c "$REMOTE_C_STORE" \
      --arg remote_d "$REMOTE_D_STORE" \
      --arg remote_runtime "$REMOTE_RUN_BASE" \
      '{
        result:$result,
        failure_class:$failure_class,
        failure_detail:$failure_detail,
        evidence_preserved:true,
        storage_roots:[$local_a,$local_b,$remote_c,$remote_d],
        remote_runtime:$remote_runtime
      }' >"$OUT/evidence-preserved.json" 2>/dev/null
  fi
  remove_remote_authorized_key
  if [[ "$ALL_LOCAL" != "true" && "$REMOVE_IDENTITY_ON_EXIT" == "true" ]]; then
    rm -f "$SSH_IDENTITY_FILE" "$SSH_IDENTITY_FILE.pub"
  fi
}

on_error() {
  local code="$1" line="$2" command="$3"
  trap - ERR
  RESULT="FAIL"
  LAST_OPERATION="failed"
  jq -n --argjson exit_code "$code" --argjson line "$line" \
    --arg command "$command" --arg failure_class "$FAILURE_CLASS" \
    --arg failure_detail "$FAILURE_DETAIL" \
    --argjson cycle "$CYCLE" --argjson at "$(date +%s)" \
    '{
      result:"FAIL",
      at_unix:$at,
      cycle:$cycle,
      exit_code:$exit_code,
      line:$line,
      command:$command,
      failure_class:$failure_class,
      failure_detail:$failure_detail
    }' \
    > "$OUT/failure.json"
  write_status FAIL "invariant or harness command failed"
  exit "$code"
}

on_signal() {
  RESULT="ABORTED"
  LAST_OPERATION="signal"
  write_status ABORTED "terminated by signal"
  exit 143
}

trap 'on_error $? $LINENO "$BASH_COMMAND"' ERR
trap on_signal INT TERM
trap cleanup EXIT

curl_json() { curl -k --http1.1 --connect-timeout 5 --max-time 120 -fsS -H 'Content-Type: application/json' "$@"; }
curl_auth() { curl -k --http1.1 --connect-timeout 5 --max-time 120 -fsS -H "Authorization: Bearer $TOKEN" "$@"; }

wait_ping() {
  local base="$1"
  for _ in $(seq 1 100); do
    curl -k --http1.1 --connect-timeout 2 --max-time 3 -fsS "$base/noa/v1/ping" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  echo "node did not become ready: $base" >&2
  return 1
}

wait_all_nodes() {
  wait_ping "$A_BASE"; wait_ping "$B_BASE"; wait_ping "$C_BASE"; wait_ping "$D_BASE"
}

wait_gc() {
  local output="$1"
  for _ in $(seq 1 240); do
    curl_auth "$A_BASE/noa/v1/maintenance/gc/status" > "$output"
    [[ "$(jq -r '.is_running' "$output")" == "false" ]] && return 0
    sleep 0.25
  done
  echo "GC did not complete in the finite wait window" >&2
  return 1
}

start_or_join_gc() {
  local output="$1" temporary="$1.tmp" status curl_code=0 detail
  status="$(curl -k --http1.1 --connect-timeout 5 --max-time 120 -sS \
    -o "$temporary" -w '%{http_code}' -X POST \
    "$A_BASE/noa/v1/maintenance/fragments/$FRAGMENT/gc/start?mode=availability" \
    -H "Authorization: Bearer $TOKEN")" || curl_code=$?
  mv "$temporary" "$output" 2>/dev/null || true
  detail="$(tr '\r\n' '  ' < "$output" 2>/dev/null | head -c 500 || true)"

  if [[ "$curl_code" -ne 0 ]]; then
    FAILURE_CLASS="gc_start_transport_failure"
    FAILURE_DETAIL="GC start transport failed with curl code $curl_code and HTTP status $status: $detail"
    return 1
  fi
  if [[ "$status" =~ ^2[0-9][0-9]$ ]]; then
    event gc started "manual availability GC accepted with HTTP $status"
    return 0
  fi
  if [[ "$status" == "409" && "${detail,,}" == *"gc is already running"* ]]; then
    event gc joined-existing "autonomous or earlier GC already owns the single-run gate"
    return 0
  fi

  FAILURE_CLASS="gc_start_rejected"
  FAILURE_DETAIL="GC start returned unexpected HTTP status $status: $detail"
  return 1
}

write_env() {
  local path="$1"
  cat > "$path" <<EOF
JWT_SECRET=n2-3.0-soak-jwt-$TS
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=86400
ACCESS_TOKEN_MAX_TTL_SECS=172800
REFRESH_TOKEN_TTL_SECS=259200
N2_CLUSTER_SECRET=n2-3.0-soak-cluster-secret-$TS
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
cleanup_interval_seconds=300
file_ttl_seconds=10
retention_seconds=10

[auto_healing]
enabled=true
initial_delay_seconds=$HEAL_INITIAL_DELAY_SECONDS
scan_interval_seconds=$HEAL_SCAN_INTERVAL_SECONDS
coordinator_nodes=auto
max_parallel_fragments=2
max_objects_per_fragment_cycle=512
max_duration_seconds=120
lease_seconds=45
checksum_scan_every_cycles=1

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=86400

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
monitor_interval_seconds=60
monitor_deep_scan_interval_seconds=300
max_part_size=$((MULTIPART_PART_MIB * 1024 * 1024))
data_shards=2
parity_shards=2
stripe_block_size=$((STRIPE_BLOCK_KIB * 1024))
EOF
}

start_local_node() {
  local run="$1"
  (cd "$run"; setsid ./litiaina-n2 </dev/null >>server.log 2>&1 & echo $! >n2.pid)
  local pid
  pid="$(cat "$run/n2.pid")"
  pidstat -h -u -r -d -w -p "$pid" 5 >> "$run/pidstat.log" 2>&1 &
  echo $! > "$run/pidstat.pid"
}

start_remote_node() {
  local run="$1"
  remote "cd '$run'; setsid ./litiaina-n2 </dev/null >>server.log 2>&1 & echo \$! >n2.pid; pid=\$(cat n2.pid); pidstat -h -u -r -d -w -p \"\$pid\" 5 >>pidstat.log 2>&1 & echo \$! >pidstat.pid"
}

restart_local_node() {
  local run="$1" base="$2"
  kill_pidfile "$run/pidstat.pid"
  kill_pidfile "$run/n2.pid"
  start_local_node "$run"
  wait_ping "$base"
}

restart_remote_node() {
  local run="$1" base="$2"
  stop_remote_node "$run"
  start_remote_node "$run"
  wait_ping "$base"
}

assert_writable() {
  local output="$OUT/events/health-current.json"
  curl_auth "$A_BASE/noa/v1/cluster/health" > "$output"
  jq -e '.data_shards == 2 and .parity_shards == 2 and .ncnx_replicas == 3 and .effective_client_writable == true' "$output" >/dev/null
  jq -c --argjson cycle "$CYCLE" --argjson at "$(date +%s)" '. + {cycle:$cycle,at_unix:$at}' "$output" >> "$OUT/events/health.jsonl"
}

download_verify() {
  local base="$1" key="$2" expected="$3" label="$4" output seconds bytes
  output="$OUT/payloads/download.bin"
  seconds="$(curl -k --http1.1 --connect-timeout 5 --max-time 120 -fsS -o "$output" -w '%{time_total}' \
    -G "$base/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" --data-urlencode "object_key=$key")"
  [[ "$(sha256sum "$output" | awk '{print $1}')" == "$expected" ]]
  bytes="$(stat -c%s "$output")"
  metric "$label" "$base" "$seconds" "$bytes"
}

rename_key() {
  local base="$1" source="$2" destination="$3" seconds
  seconds="$(curl -k --http1.1 --connect-timeout 5 --max-time 120 -fsS -o "$OUT/events/rename-current.json" -w '%{time_total}' \
    -X POST "$base/noa/v1/posix/rename" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"source_path\":\"$source\",\"destination_path\":\"$destination\"}")"
  metric rename "$base" "$seconds" 0
}

base_slug() {
  printf '%s' "$1" | tr ':/' '_'
}

trigger_healing_node() {
  local base="$1" slug body temporary http curl_code=0
  slug="$(base_slug "$base")"
  body="$OUT/events/healing-$slug.json"
  temporary="$body.tmp"
  http="$(curl -k --http1.1 --connect-timeout 5 \
    --max-time "$HEAL_TRIGGER_REQUEST_TIMEOUT_SECONDS" -sS \
    -o "$temporary" -w '%{http_code}' \
    -X POST "$base/noa/v1/cluster/healing/cycle" \
    -H "Authorization: Bearer $TOKEN")" || curl_code=$?
  mv "$temporary" "$body" 2>/dev/null || true
  printf '%s\n' "$http" >"$OUT/events/healing-$slug.http"
  [[ "$curl_code" -eq 0 ]] || return 1
  [[ "$http" =~ ^2[0-9][0-9]$ || "$http" == "409" ]]
}

trigger_healing_round() {
  local base pid failed=0
  local pids=()
  for base in "${BASES[@]}"; do
    trigger_healing_node "$base" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "$pid" || failed=1
  done
  if (( failed != 0 )); then
    FAILURE_CLASS="healing_trigger_transport_failure"
    FAILURE_DETAIL="one or more nodes did not return HTTP 2xx or 409 from the healing trigger"
    return 1
  fi
}

snapshot_healing_status() {
  local base slug output temporary
  for base in "${BASES[@]}"; do
    slug="$(base_slug "$base")"
    output="$OUT/events/healing-status-$slug.json"
    temporary="$output.tmp"
    curl_auth "$base/noa/v1/cluster/healing/status" >"$temporary"
    mv "$temporary" "$output"
  done
}

healing_running_count() {
  local base slug count=0
  for base in "${BASES[@]}"; do
    slug="$(base_slug "$base")"
    if jq -e '.running == true' "$OUT/events/healing-status-$slug.json" >/dev/null; then
      count=$((count + 1))
    fi
  done
  printf '%s\n' "$count"
}

wait_for_healing_idle() {
  local deadline="$1" running
  while (( $(date +%s) < deadline )); do
    snapshot_healing_status
    running="$(healing_running_count)"
    (( running == 0 )) && return 0
    sleep "$HEAL_STATUS_POLL_SECONDS"
  done
  FAILURE_CLASS="healing_cycle_timeout"
  FAILURE_DETAIL="an accepted or already-running healing cycle exceeded the verification deadline"
  return 1
}

ncnx_paths() {
  find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -path '*/.namespace/soak/anchor.bin.ncnx' -type f 2>/dev/null
  remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -path '*/.namespace/soak/anchor.bin.ncnx' -type f" 2>/dev/null
}

shard_paths() {
  find "$LOCAL_A_STORE" "$LOCAL_B_STORE" -path "*/$ANCHOR_VERSION/part_0_*" -type f 2>/dev/null
  remote "find '$REMOTE_C_STORE' '$REMOTE_D_STORE' -path '*/$ANCHOR_VERSION/part_0_*' -type f" 2>/dev/null
}

distributed_path_count() {
  local kind="$1"
  if [[ "$kind" == "ncnx" ]]; then
    ncnx_paths | wc -l | tr -d ' '
  else
    shard_paths | wc -l | tr -d ' '
  fi
}

wait_for_path_count() {
  local kind="$1" expected="$2" timeout="${3:-30}" deadline count
  deadline=$(( $(date +%s) + timeout ))
  while (( $(date +%s) < deadline )); do
    count="$(distributed_path_count "$kind")"
    [[ "$count" -eq "$expected" ]] && return 0
    sleep 0.5
  done
  FAILURE_CLASS="chaos_precondition_timeout"
  FAILURE_DETAIL="$kind path count did not reach precondition $expected"
  echo "$FAILURE_DETAIL" >&2
  return 1
}

wait_for_healing_repair() {
  local kind="$1" expected="$2" deadline count round=0
  deadline=$(( $(date +%s) + HEAL_VERIFY_TIMEOUT_SECONDS ))
  while (( $(date +%s) < deadline )); do
    count="$(distributed_path_count "$kind")"
    [[ "$count" -eq "$expected" ]] && return 0

    wait_for_healing_idle "$deadline" || return 1
    count="$(distributed_path_count "$kind")"
    [[ "$count" -eq "$expected" ]] && return 0

    round=$((round + 1))
    event "$kind-healing" "round-$round" "triggering synchronized cluster healing"
    trigger_healing_round || return 1
    wait_for_healing_idle "$deadline" || return 1
  done

  count="$(distributed_path_count "$kind")"
  FAILURE_CLASS="healing_verification_timeout"
  FAILURE_DETAIL="$kind repair remained at $count/$expected paths after ${HEAL_VERIFY_TIMEOUT_SECONDS}s"
  snapshot_healing_status 2>/dev/null || true
  jq -s \
    --arg kind "$kind" \
    --arg detail "$FAILURE_DETAIL" \
    --argjson expected "$expected" \
    --argjson observed "$count" \
    --argjson timeout_seconds "$HEAL_VERIFY_TIMEOUT_SECONDS" \
    '{
      failure_class:"healing_verification_timeout",
      kind:$kind,
      detail:$detail,
      expected_paths:$expected,
      observed_paths:$observed,
      timeout_seconds:$timeout_seconds,
      node_statuses:.
    }' "$OUT"/events/healing-status-*.json >"$OUT/healing-timeout.json" 2>/dev/null || true
  echo "$FAILURE_DETAIL" >&2
  return 1
}

delete_one_distributed_path() {
  local kind="$1" paths target
  if [[ "$kind" == "ncnx" ]]; then
    paths="$(ncnx_paths)"
  else
    paths="$(shard_paths)"
  fi
  target="$(printf '%s\n' "$paths" | sed -n '1p')"
  [[ -n "$target" ]]
  if [[ "$target" == "$LOCAL_A_STORE"/* || "$target" == "$LOCAL_B_STORE"/* ]]; then
    rm -f "$target"
  else
    remote "rm -f '$target'"
  fi
  event "$kind" injected "$target"
}

run_chaos() {
  local phase=$(( ((CHAOS_START_PHASE - 1 + CHAOS_EVENTS) % 6) + 1 )) status
  CHAOS_EVENTS=$((CHAOS_EVENTS + 1))
  LAST_OPERATION="chaos-$phase"
  event "phase-$phase" started "bounded failure injection"
  case "$phase" in
    1)
      stop_remote_node "$REMOTE_D"
      sleep "$FAULT_DWELL_SECONDS"
      download_verify "$A_BASE" soak/anchor.bin "$ANCHOR_SHA" chaos-one-node-read
      status="$(curl -k --http1.1 --connect-timeout 5 --max-time 30 -sS -o "$OUT/events/fail-closed-current.json" -w '%{http_code}' \
        -X POST "$A_BASE/noa/v1/upload/one-shot/soak/must-fail-$CYCLE.bin" \
        -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/payloads/small.bin")"
      [[ "$status" == "503" ]]
      start_remote_node "$REMOTE_D"; wait_ping "$D_BASE"
      ;;
    2)
      stop_remote_node "$REMOTE_D"
      kill_pidfile "$LOCAL_B/pidstat.pid"; kill_pidfile "$LOCAL_B/n2.pid"
      sleep "$FAULT_DWELL_SECONDS"
      download_verify "$A_BASE" soak/anchor.bin "$ANCHOR_SHA" chaos-parity-count-read
      start_local_node "$LOCAL_B"; wait_ping "$B_BASE"
      start_remote_node "$REMOTE_D"; wait_ping "$D_BASE"
      ;;
    3)
      case $((CHAOS_EVENTS % 4)) in
        0) restart_local_node "$LOCAL_A" "$A_BASE" ;;
        1) restart_local_node "$LOCAL_B" "$B_BASE" ;;
        2) restart_remote_node "$REMOTE_C" "$C_BASE" ;;
        *) restart_remote_node "$REMOTE_D" "$D_BASE" ;;
      esac
      download_verify "$C_BASE" soak/anchor.bin "$ANCHOR_SHA" chaos-restart-read
      ;;
    4)
      wait_for_path_count ncnx 3
      delete_one_distributed_path ncnx
      wait_for_healing_repair ncnx 3
      download_verify "$D_BASE" soak/anchor.bin "$ANCHOR_SHA" chaos-ncnx-heal-read
      ;;
    5)
      wait_for_path_count shard 4
      delete_one_distributed_path shard
      wait_for_healing_repair shard 4
      download_verify "$B_BASE" soak/anchor.bin "$ANCHOR_SHA" chaos-shard-heal-read
      ;;
    6)
      restart_remote_node "$REMOTE_C" "$C_BASE"
      restart_local_node "$LOCAL_A" "$A_BASE"
      restart_remote_node "$REMOTE_D" "$D_BASE"
      restart_local_node "$LOCAL_B" "$B_BASE"
      wait_all_nodes
      download_verify "$A_BASE" soak/anchor.bin "$ANCHOR_SHA" chaos-rolling-restart-read
      ;;
  esac
  assert_writable
  event "phase-$phase" passed "all post-fault invariants passed"
  write_status RUNNING "chaos phase passed"
}

setup_cluster() {
  LAST_OPERATION="cluster-setup"
  remote true
  mkdir -p "$LOCAL_A" "$LOCAL_B" "$LOCAL_A_STORE" "$LOCAL_B_STORE" "$LOCAL_A/state" "$LOCAL_B/state"
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=n2-3.0-soak' \
    -keyout "$OUT/key.pem" -out "$OUT/cert.pem" >/dev/null 2>&1
  cp "$BIN" "$LOCAL_A/litiaina-n2"; cp "$BIN" "$LOCAL_B/litiaina-n2"
  cp "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_A/"
  cp "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_B/"
  write_env "$LOCAL_A/n2.env"; cp "$LOCAL_A/n2.env" "$LOCAL_B/n2.env"

  remote "mkdir -p '$REMOTE_C' '$REMOTE_D' '$REMOTE_C/state' '$REMOTE_D/state' '$REMOTE_C_STORE' '$REMOTE_D_STORE'"
  copy_remote "$BIN" "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_A/n2.env" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/" >/dev/null
  copy_remote "$BIN" "$OUT/cert.pem" "$OUT/key.pem" "$LOCAL_A/n2.env" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/" >/dev/null
  remote "chmod +x '$REMOTE_C/litiaina-n2' '$REMOTE_D/litiaina-n2'"

  cat > "$OUT/n2_membership.json" <<EOF
{
  "format_version": 5,
  "cluster_id": "$CLUSTER_ID",
  "epoch": 1,
  "binding_epoch": 1,
  "pools": [{
    "id": "$POOL_ID",
    "generation": 1,
    "policy": {"object_data_shards": 2, "object_parity_shards": 2},
    "state": "writable",
    "slot_ids": ["node-a", "node-b", "node-c", "node-d"]
  }],
  "slot_bindings": [
    {"slot_id":"node-a","instance_id":"node-a","binding_epoch":1,"endpoint":"$A_BASE","native_endpoint":"n2://$LOCAL_IP:$A_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-a","path":"$LOCAL_A_STORE"}],"joined_epoch":1},
    {"slot_id":"node-b","instance_id":"node-b","binding_epoch":1,"endpoint":"$B_BASE","native_endpoint":"n2://$LOCAL_IP:$B_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-b","path":"$LOCAL_B_STORE"}],"joined_epoch":1},
    {"slot_id":"node-c","instance_id":"node-c","binding_epoch":1,"endpoint":"$C_BASE","native_endpoint":"n2://$REMOTE_IP:$C_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-c","path":"$REMOTE_C_STORE"}],"joined_epoch":1},
    {"slot_id":"node-d","instance_id":"node-d","binding_epoch":1,"endpoint":"$D_BASE","native_endpoint":"n2://$REMOTE_IP:$D_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-d","path":"$REMOTE_D_STORE"}],"joined_epoch":1}
  ],
  "reserve_nodes": [],
  "retired_instances": []
}
EOF
  cp "$OUT/n2_membership.json" "$LOCAL_A/state/"; cp "$OUT/n2_membership.json" "$LOCAL_B/state/"
  copy_remote "$OUT/n2_membership.json" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/state/" >/dev/null
  copy_remote "$OUT/n2_membership.json" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/state/" >/dev/null

  write_config "$LOCAL_A/n2_config.ini" node-a "$A_HTTPS" "$A_NATIVE" "$LOCAL_IP" "$LOCAL_A_STORE" "$LOCAL_A" root-a
  write_config "$LOCAL_B/n2_config.ini" node-b "$B_HTTPS" "$B_NATIVE" "$LOCAL_IP" "$LOCAL_B_STORE" "$LOCAL_B" root-b
  write_config "$OUT/node-c.ini" node-c "$C_HTTPS" "$C_NATIVE" "$REMOTE_IP" "$REMOTE_C_STORE" "$REMOTE_C" root-c
  write_config "$OUT/node-d.ini" node-d "$D_HTTPS" "$D_NATIVE" "$REMOTE_IP" "$REMOTE_D_STORE" "$REMOTE_D" root-d
  copy_remote "$OUT/node-c.ini" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_C/n2_config.ini" >/dev/null
  copy_remote "$OUT/node-d.ini" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_D/n2_config.ini" >/dev/null

  start_remote_node "$REMOTE_C"; start_remote_node "$REMOTE_D"
  start_local_node "$LOCAL_A"; start_local_node "$LOCAL_B"
  wait_all_nodes

  curl_json -X POST "$A_BASE/noa/v1/fragments" \
    -d "{\"name\":\"$FRAGMENT_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAGMENT_KEY\"}" > "$OUT/fragment-create.json"
  FRAGMENT="$(jq -er '.fragment_hash' "$OUT/fragment-create.json")"
  STABLE_FRAGMENT="$(jq -er '.stable_fragment_id' "$OUT/fragment-create.json")"
  curl_json -X POST "$B_BASE/noa/v1/auth/fragment" \
    -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAGMENT_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":86400}" > "$OUT/auth.json"
  TOKEN="$(jq -er '.access_token' "$OUT/auth.json")"
  assert_writable

  dd if=/dev/urandom of="$OUT/payloads/anchor.bin" bs=1M count="$ANCHOR_MIB" status=none
  dd if=/dev/urandom of="$OUT/payloads/current.bin" bs=1K count="$PAYLOAD_KIB" status=none
  dd if=/dev/urandom of="$OUT/payloads/small.bin" bs=4K count=1 status=none
  ANCHOR_SHA="$(sha256sum "$OUT/payloads/anchor.bin" | awk '{print $1}')"
  multipart_upload anchor-upload "$A_BASE" soak/anchor.bin \
    "$OUT/payloads/anchor.bin" "$OUT/anchor-upload.json"
  ANCHOR_VERSION="$(jq -er '.version_id' "$OUT/anchor-upload.json")"
  download_verify "$D_BASE" soak/anchor.bin "$ANCHOR_SHA" anchor-download

  for key in soak/lex/a soak/lex/a-previous soak/lex/a/child soak/lex/a/child/deep soak/lex/a0; do
    upload_with_retry setup-upload "$A_BASE" "$key" "$OUT/payloads/small.bin" \
      "$OUT/events/lex-$(printf '%s' "$key" | tr '/' '_').json"
  done
}

verify_pagination() {
  local token="" expected output="$OUT/events/lex-page.json"
  for expected in soak/lex/a soak/lex/a-previous soak/lex/a/child soak/lex/a/child/deep soak/lex/a0; do
    if [[ -n "$token" ]]; then
      curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=soak/lex/' \
        --data-urlencode 'limit=1' --data-urlencode "continuation_token=$token" > "$output"
    else
      curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=soak/lex/' \
        --data-urlencode 'limit=1' > "$output"
    fi
    [[ "$(jq -er '.entries[0].record' "$output")" == "$expected" ]]
    token="$(jq -r '.continuation_token // empty' "$output")"
  done
}

run_cycle() {
  local index base key moved old old_key current_sha seconds trash_id
  index=$((CYCLE % 4))
  base="${BASES[$index]}"
  key="soak/live/object-$(printf '%08d' "$CYCLE").bin"
  moved="$key.moved"
  LAST_OPERATION="cycle-upload"
  dd if=/dev/urandom of="$OUT/payloads/current.bin" bs=1K count="$PAYLOAD_KIB" status=none
  current_sha="$(sha256sum "$OUT/payloads/current.bin" | awk '{print $1}')"
  upload_with_retry upload "$base" "$key" "$OUT/payloads/current.bin" "$OUT/events/upload-current.json"
  download_verify "${BASES[$(((index + 1) % 4))]}" "$key" "$current_sha" download

  LAST_OPERATION="cycle-namespace"
  rename_key "${BASES[$(((index + 2) % 4))]}" "$key" "$moved"
  download_verify "${BASES[$(((index + 3) % 4))]}" "$moved" "$current_sha" renamed-download
  rename_key "$base" "$moved" "$key"

  seconds="$(curl -k --http1.1 --connect-timeout 5 --max-time 60 -fsS -o "$OUT/events/list-current.json" -w '%{time_total}' \
    -G "$base/noa/v1/objects" -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'prefix=soak/live/' --data-urlencode 'limit=25')"
  metric list "$base" "$seconds" 0
  seconds="$(curl -k --http1.1 --connect-timeout 5 --max-time 60 -fsS -o "$OUT/events/stat-current.json" -w '%{time_total}' \
    -G "$base/noa/v1/posix/stat" -H "Authorization: Bearer $TOKEN" --data-urlencode "path=$key")"
  metric stat "$base" "$seconds" 0

  if (( CYCLE % 10 == 0 )); then
    LAST_OPERATION="cycle-trash-recover"
    curl_json -X POST "$base/noa/v1/posix/trash" -H "Authorization: Bearer $TOKEN" \
      -d "{\"path\":\"$key\"}" > "$OUT/events/trash-current.json"
    trash_id="$(jq -er '.trash_entry_id' "$OUT/events/trash-current.json")"
    curl_json -X POST "$base/noa/v1/posix/recover" -H "Authorization: Bearer $TOKEN" \
      -d "{\"path\":\"$key\",\"trash_entry_id\":\"$trash_id\"}" > "$OUT/events/recover-current.json"
    download_verify "$base" "$key" "$current_sha" recovered-download
    jq -nc --arg key "$key" --arg trash_id "$trash_id" --argjson cycle "$CYCLE" \
      --argjson at "$(date +%s)" '{at_unix:$at,cycle:$cycle,key:$key,trash_id:$trash_id,result:"recovered"}' \
      >> "$OUT/events/lifecycle.jsonl"
  fi

  if (( CYCLE % 5 == 0 )); then
    LAST_OPERATION="cycle-concurrent-writes"
    local pids=() j concurrent_base
    for j in 1 2 3 4; do
      concurrent_base="${BASES[$(((index + j) % 4))]}"
      upload_with_retry concurrent-upload "$concurrent_base" "soak/concurrent/cycle-$CYCLE-$j.bin" \
        "$OUT/payloads/small.bin" "$OUT/events/concurrent-$j.json" &
      pids+=("$!")
    done
    for j in "${pids[@]}"; do wait "$j"; done
  fi

  if (( CYCLE % 20 == 0 )); then
    LAST_OPERATION="cycle-pagination"
    verify_pagination
  fi

  if (( CYCLE > ROLLING_LIVE_OBJECTS )); then
    old=$((CYCLE - ROLLING_LIVE_OBJECTS))
    old_key="soak/live/object-$(printf '%08d' "$old").bin"
    curl_auth -X DELETE "$A_BASE/noa/v1/objects/$old_key" > "$OUT/events/retire-current.json"
  fi

  if (( CYCLE % 20 == 0 )); then
    LAST_OPERATION="cycle-gc"
    start_or_join_gc "$OUT/events/gc-start-current.json"
    wait_gc "$OUT/events/gc-current.json"
    jq -e '.total_errors == 0' "$OUT/events/gc-current.json" >/dev/null
  fi

  download_verify "$base" soak/anchor.bin "$ANCHOR_SHA" anchor-continuity
  if (( CYCLE % 30 == 0 )); then
    curl_auth "$base/noa/v1/cluster/monitor?deep_scan=false" > "$OUT/events/monitor-current.json"
  fi
}

summarize_metrics() {
  jq -s '
    def pct($p): sort | if length == 0 then null else .[((length - 1) * $p | floor)] end;
    group_by(.operation) | map({
      operation: .[0].operation,
      samples: length,
      bytes: (map(.bytes) | add),
      mean_ms: (map(.milliseconds) | add / length),
      p50_ms: (map(.milliseconds) | pct(0.50)),
      p95_ms: (map(.milliseconds) | pct(0.95)),
      p99_ms: (map(.milliseconds) | pct(0.99)),
      max_ms: (map(.milliseconds) | max)
    })' "$OUT/metrics/operations.jsonl" > "$OUT/metrics/summary.json"

  : > "$OUT/metrics/process-summary.tsv"
  local file
  for file in "$OUT"/node-*-pidstat.log; do
    [[ -f "$file" ]] || continue
    awk '$NF=="litiaina-n2"{n++;cpu+=$8;if($8>maxcpu)maxcpu=$8;if($13>maxrss)maxrss=$13;rd+=$15;wr+=$16;cs+=$19;ncs+=$20} END{if(n) printf "%s\tsamples=%d\tavg_cpu=%.2f\tpeak_cpu=%.2f\tpeak_rss_mib=%.2f\tavg_read_kib_s=%.2f\tavg_write_kib_s=%.2f\tavg_csw_s=%.2f\tavg_nvcsw_s=%.2f\n",FILENAME,n,cpu/n,maxcpu,maxrss/1024,rd/n,wr/n,cs/n,ncs/n}' "$file" \
      >> "$OUT/metrics/process-summary.tsv"
  done
}

finish_success() {
  LAST_OPERATION="final-verification"
  wait_all_nodes
  assert_writable
  download_verify "$C_BASE" soak/anchor.bin "$ANCHOR_SHA" final-anchor-download
  curl_auth "$A_BASE/noa/v1/cluster/monitor?deep_scan=true" > "$OUT/final-monitor.json"
  jq -e '.overview.state == "healthy" and .jobs.healing.safety_read_only == false' "$OUT/final-monitor.json" >/dev/null
  collect_remote_logs
  cp "$LOCAL_A/server.log" "$OUT/node-a.log"
  cp "$LOCAL_B/server.log" "$OUT/node-b.log"
  cp "$LOCAL_A/pidstat.log" "$OUT/node-a-pidstat.log" 2>/dev/null || true
  cp "$LOCAL_B/pidstat.log" "$OUT/node-b-pidstat.log" 2>/dev/null || true
  summarize_metrics
  local end elapsed suspicious availability_events
  end="$(date +%s)"; elapsed=$((end - START_EPOCH))
  suspicious="$({ grep -Eih 'panic|fatal|internal server error' "$OUT"/node-*.log 2>/dev/null || true; } | wc -l | tr -d ' ')"
  availability_events="$(wc -l < "$OUT/events/availability.jsonl" | tr -d ' ')"
  [[ "$suspicious" -eq 0 ]]
  jq -n \
    --arg result PASS --arg cluster_id "$CLUSTER_ID" --arg pool_id "$POOL_ID" \
    --arg fragment "$FRAGMENT" --arg stable_fragment_id "$STABLE_FRAGMENT" \
    --arg topology "$TOPOLOGY_LABEL" \
    --arg cycle_counter_semantics 'cycle zero is included' \
    --argjson started_at "$START_EPOCH" --argjson finished_at "$end" \
    --argjson requested_seconds "$DURATION_SECONDS" --argjson elapsed_seconds "$elapsed" \
    --argjson final_cycle "$CYCLE" --argjson chaos_events "$CHAOS_EVENTS" --argjson availability_events "$availability_events" \
    --argjson chaos_start_phase "$CHAOS_START_PHASE" \
    --argjson healing_verification_timeout_seconds "$HEAL_VERIFY_TIMEOUT_SECONDS" \
    --argjson suspicious_log_lines "$suspicious" --slurpfile operations "$OUT/metrics/summary.json" \
    --slurpfile monitor "$OUT/final-monitor.json" \
    '{
      result:$result,
      cluster_id:$cluster_id,
      pool_id:$pool_id,
      fragment:$fragment,
      stable_fragment_id:$stable_fragment_id,
      topology:$topology,
      started_at_unix:$started_at,
      finished_at_unix:$finished_at,
      requested_seconds:$requested_seconds,
      elapsed_seconds:$elapsed_seconds,
      final_cycle:$final_cycle,
      cycle_counter_semantics:$cycle_counter_semantics,
      chaos_events:$chaos_events,
      chaos_start_phase:$chaos_start_phase,
      healing_verification_timeout_seconds:$healing_verification_timeout_seconds,
      availability_events:$availability_events,
      suspicious_log_lines:$suspicious_log_lines,
      operations:$operations[0],
      final_monitor:$monitor[0],
      hardware_cycle_counters:"unavailable when perf_event_paranoid blocks unprivileged counters"
    }' \
    > "$OUT/summary.json"
  RESULT="PASS"
  write_status PASS "requested soak and chaos invariants passed"
}

write_status RUNNING "initializing isolated four-node cluster"
setup_cluster
write_status RUNNING "cluster initialized; workload active"

while (( $(date +%s) < DEADLINE )); do
  if (( MAX_CYCLES > 0 && CYCLE >= MAX_CYCLES )); then
    break
  fi
  run_cycle
  if (( CYCLE > 0 && CYCLE % CHAOS_EVERY_CYCLES == 0 )); then
    run_chaos
  fi
  LAST_OPERATION="cycle-complete"
  write_status RUNNING "workload cycle passed"
  CYCLE=$((CYCLE + 1))
  sleep "$CYCLE_PAUSE_SECONDS"
done

finish_success
printf 'N2_3_0_SOAK_OUT=%s\n' "$OUT"
