#!/usr/bin/env bash
set -Eeuo pipefail

# Single-host append-only server-pool expansion proof. Pool A is established
# first at EC 2+2. Pool B is then appended at EC 4+2 without rewriting Pool A.
# Two global reserves are subsequently consumed by one replacement in each
# pool. All stores are constrained to caller-provided .n2-validation roots.

: "${N2_MULTI_BIN:?set N2_MULTI_BIN}"
: "${N2_MULTI_OUT:?set N2_MULTI_OUT}"
: "${N2_MULTI_D1:?set N2_MULTI_D1}"
: "${N2_MULTI_D2:?set N2_MULTI_D2}"
: "${N2_MULTI_D3:?set N2_MULTI_D3}"
: "${N2_MULTI_D4:?set N2_MULTI_D4}"

BIN="$N2_MULTI_BIN"
OUT="$N2_MULTI_OUT"
TS="${N2_MULTI_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
RUN="$OUT/runtime"
POOL_A="pool-a-$TS"
POOL_B="pool-b-$TS"
CLUSTER="multi-cluster-$TS"
MASTER="multi-master-$TS"
FRAGMENT_KEY="multi-fragment-key-$TS"
TOKEN=""
RESULT="FAIL"

declare -a ACTIVE_A=(a1 a2 a3 a4)
declare -a ACTIVE_B=(b1 b2 b3 b4 b5 b6)
declare -a RESERVES=(reserve-a reserve-b)
declare -a ALL_NODES=("${ACTIVE_A[@]}" "${ACTIVE_B[@]}" "${RESERVES[@]}")
declare -A HTTPS NATIVE STORE ROOT POOL GEN K M

HTTPS[a1]=23001; NATIVE[a1]=23002
HTTPS[a2]=23011; NATIVE[a2]=23012
HTTPS[a3]=23021; NATIVE[a3]=23022
HTTPS[a4]=23031; NATIVE[a4]=23032
HTTPS[b1]=23101; NATIVE[b1]=23102
HTTPS[b2]=23111; NATIVE[b2]=23112
HTTPS[b3]=23121; NATIVE[b3]=23122
HTTPS[b4]=23131; NATIVE[b4]=23132
HTTPS[b5]=23141; NATIVE[b5]=23142
HTTPS[b6]=23151; NATIVE[b6]=23152
HTTPS[reserve-a]=23201; NATIVE[reserve-a]=23202
HTTPS[reserve-b]=23211; NATIVE[reserve-b]=23212

STORE[a1]="$N2_MULTI_D1/a1"; STORE[a2]="$N2_MULTI_D2/a2"
STORE[a3]="$N2_MULTI_D3/a3"; STORE[a4]="$N2_MULTI_D4/a4"
STORE[b1]="$N2_MULTI_D1/b1"; STORE[b2]="$N2_MULTI_D2/b2"
STORE[b3]="$N2_MULTI_D3/b3"; STORE[b4]="$N2_MULTI_D4/b4"
STORE[b5]="$N2_MULTI_D1/b5"; STORE[b6]="$N2_MULTI_D2/b6"
STORE[reserve-a]="$N2_MULTI_D3/reserve-a"
STORE[reserve-b]="$N2_MULTI_D4/reserve-b"

for node in "${ACTIVE_A[@]}" reserve-a; do
  POOL[$node]="$POOL_A"; GEN[$node]=1; K[$node]=2; M[$node]=2
done
for node in "${ACTIVE_B[@]}" reserve-b; do
  POOL[$node]="$POOL_B"; GEN[$node]=2; K[$node]=4; M[$node]=2
done
for node in "${ALL_NODES[@]}"; do
  ROOT[$node]="root-$node"
done

for tool in jq curl openssl pidstat sha256sum awk dd find stat; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done
[[ -x "$BIN" ]]

assert_isolated() {
  local path="$1"
  [[ "$path" == /mnt/NDISK*/.n2-validation/* ]] || {
    echo "refusing unsafe multi-pool store: $path" >&2
    exit 1
  }
  [[ "$path" != *"/../"* && "$path" != *"/./"* ]]
}
for node in "${ALL_NODES[@]}"; do
  assert_isolated "${STORE[$node]}"
done

assert_ports_available() {
  local node port
  command -v ss >/dev/null || {
    echo "missing required tool: ss" >&2
    exit 1
  }
  for node in "${ALL_NODES[@]}"; do
    for port in "${HTTPS[$node]}" "${NATIVE[$node]}"; do
      if [[ -n "$(ss -H -ltn "sport = :$port")" ]]; then
        echo "required N2 proof port is already in use: $port" >&2
        exit 1
      fi
    done
  done
}
assert_ports_available

base() {
  echo "https://127.0.0.1:${HTTPS[$1]}"
}

run_dir() {
  echo "$RUN/$1"
}

kill_pidfile() {
  local pidfile="$1"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(<"$pidfile")"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$pidfile"
  fi
}

stop_node() {
  local node="$1" run
  run="$(run_dir "$node")"
  kill_pidfile "$run/pidstat.pid"
  kill_pidfile "$run/n2.pid"
}

stop_all() {
  for node in "${ALL_NODES[@]}"; do
    stop_node "$node"
  done
}

cleanup() {
  set +e
  stop_all
  if [[ "$RESULT" == "PASS" ]]; then
    for node in "${ALL_NODES[@]}"; do
      rm -rf "${STORE[$node]}"
    done
  fi
}
trap cleanup EXIT

write_env() {
  local path="$1"
  cat >"$path" <<EOF
JWT_SECRET=multi-jwt-$TS
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_CLUSTER_SECRET=multi-cluster-secret-$TS
EOF
}

write_config() {
  local node="$1" run
  run="$(run_dir "$node")"
  cat >"$run/n2_config.ini" <<EOF
[server]
host=127.0.0.1
https_port=${HTTPS[$node]}
native_port=${NATIVE[$node]}
cert_path=$run/cert.pem
key_path=$run/key.pem

[disk_config]
storage_roots=${ROOT[$node]}=${STORE[$node]}
runtime_path=$run/state

[cluster_config]
node_id=$node
pool_id=${POOL[$node]}
pool_generation=${GEN[$node]}
node_endpoint=https://127.0.0.1:${HTTPS[$node]}
native_endpoint=n2://127.0.0.1:${NATIVE[$node]}
membership_file=$run/state/n2_membership.json
object_data_shards=${K[$node]}
object_parity_shards=${M[$node]}
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
max_duration_seconds=90
lease_seconds=120
checksum_scan_every_cycles=1

[reserve_set]
enabled=true
automatic_promotion=true
initial_delay_seconds=3
probe_interval_seconds=1
failure_threshold=2
promotion_timeout_seconds=45

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
data_shards=${K[$node]}
parity_shards=${M[$node]}
stripe_block_size=1048576
EOF
}

start_node() {
  local node="$1" run pid
  run="$(run_dir "$node")"
  (
    cd "$run"
    setsid ./litiaina-n2 </dev/null >>server.log 2>&1 &
    echo $! >n2.pid
  )
  pid="$(<"$run/n2.pid")"
  pidstat -h -u -r -d -w -p "$pid" 1 >>"$run/pidstat.log" 2>&1 &
  echo $! >"$run/pidstat.pid"
}

wait_ping() {
  local node="$1" endpoint
  endpoint="$(base "$node")"
  for _ in $(seq 1 120); do
    curl -k --http1.1 -fsS "$endpoint/noa/v1/ping" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  echo "node did not become ready: $node" >&2
  return 1
}

start_and_wait() {
  for node in "$@"; do start_node "$node"; done
  for node in "$@"; do wait_ping "$node"; done
}

curl_json() {
  curl -k --http1.1 -fsS -H 'Content-Type: application/json' "$@"
}

curl_auth() {
  curl -k --http1.1 -fsS -H "Authorization: Bearer $TOKEN" "$@"
}

multipart_upload() {
  local node="$1" object_key="$2" source="$3" label="$4"
  local endpoint bytes version session
  endpoint="$(base "$node")"
  bytes="$(stat -c%s "$source")"

  curl_json -X POST "$endpoint/noa/v1/upload/multipart/init/$object_key" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$bytes,\"expected_parts\":1,\"part_size_bytes\":$bytes}" \
    >"$OUT/$label-init.json"
  version="$(jq -er '.version_id' "$OUT/$label-init.json")"
  session="$(jq -er '.session_id' "$OUT/$label-init.json")"

  curl_auth -X PUT "$endpoint/noa/v1/upload/multipart/$version/0" \
    --data-binary @"$source" >"$OUT/$label-part-0.json"
  curl_json -X POST "$endpoint/noa/v1/upload/multipart/$version/$session/finalize?object_key=$object_key" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$bytes,\"expected_parts\":1,\"part_size_bytes\":$bytes}" \
    >"$OUT/$label-finalize.json"

  jq -e --arg version "$version" '.version_id == $version' \
    "$OUT/$label-finalize.json" >/dev/null
  printf '%s\n' "$version"
}

install_membership() {
  local source="$1"
  for node in "${ALL_NODES[@]}"; do
    cp "$source" "$(run_dir "$node")/state/n2_membership.json"
  done
}

slot_binding() {
  local slot="$1"
  jq -n \
    --arg slot "$slot" \
    --arg instance "$slot" \
    --arg endpoint "$(base "$slot")" \
    --arg native "n2://127.0.0.1:${NATIVE[$slot]}" \
    --arg root "${ROOT[$slot]}" \
    --arg path "${STORE[$slot]}" \
    '{
      slot_id:$slot,
      instance_id:$instance,
      binding_epoch:1,
      endpoint:$endpoint,
      native_endpoint:$native,
      state:"online",
      weight:1,
      storage_roots:[{id:$root,path:$path}],
      joined_epoch:1
    }'
}

reserve_record() {
  local node="$1"
  jq -n \
    --arg instance "$node" \
    --arg endpoint "$(base "$node")" \
    --arg native "n2://127.0.0.1:${NATIVE[$node]}" \
    --arg root "${ROOT[$node]}" \
    --arg path "${STORE[$node]}" \
    '{
      instance_id:$instance,
      endpoint:$endpoint,
      native_endpoint:$native,
      state:"online",
      storage_roots:[{id:$root,path:$path}]
    }'
}

build_membership_one() {
  local bindings reserves
  bindings="$(printf '%s\n' "${ACTIVE_A[@]}" | while read -r node; do slot_binding "$node"; done | jq -s .)"
  reserves="$(reserve_record reserve-a | jq -s .)"
  jq -n \
    --arg cluster "$CLUSTER" \
    --arg pool_a "$POOL_A" \
    --argjson bindings "$bindings" \
    --argjson reserves "$reserves" \
    '{
      format_version:5,
      cluster_id:$cluster,
      epoch:1,
      binding_epoch:1,
      pools:[{
        id:$pool_a,
        generation:1,
        policy:{object_data_shards:2,object_parity_shards:2},
        state:"writable",
        slot_ids:["a1","a2","a3","a4"]
      }],
      slot_bindings:$bindings,
      reserve_nodes:$reserves,
      retired_instances:[]
    }' >"$OUT/membership-one-pool.json"
}

build_membership_two() {
  local bindings reserves
  bindings="$(
    printf '%s\n' "${ACTIVE_A[@]}" "${ACTIVE_B[@]}" |
      while read -r node; do slot_binding "$node"; done |
      jq -s .
  )"
  reserves="$(
    printf '%s\n' "${RESERVES[@]}" |
      while read -r node; do reserve_record "$node"; done |
      jq -s .
  )"
  jq -n \
    --arg cluster "$CLUSTER" \
    --arg pool_a "$POOL_A" \
    --arg pool_b "$POOL_B" \
    --argjson bindings "$bindings" \
    --argjson reserves "$reserves" \
    '{
      format_version:5,
      cluster_id:$cluster,
      epoch:2,
      binding_epoch:1,
      pools:[
        {
          id:$pool_a,
          generation:1,
          policy:{object_data_shards:2,object_parity_shards:2},
          state:"sealed",
          slot_ids:["a1","a2","a3","a4"]
        },
        {
          id:$pool_b,
          generation:2,
          policy:{object_data_shards:4,object_parity_shards:2},
          state:"writable",
          slot_ids:["b1","b2","b3","b4","b5","b6"]
        }
      ],
      slot_bindings:$bindings,
      reserve_nodes:$reserves,
      retired_instances:[]
    }' >"$OUT/membership-two-pools.json"
}

wait_slot_replacement() {
  local coordinator="$1" epoch="$2" slot="$3" instance="$4" output="$5"
  local endpoint
  endpoint="$(base "$coordinator")"
  for _ in $(seq 1 720); do
    if curl_auth "$endpoint/noa/v1/cluster/health" >"$output" 2>/dev/null \
      && jq -e \
        --argjson epoch "$epoch" \
        --arg slot "$slot" \
        --arg instance "$instance" \
        '.binding_epoch == $epoch
         and .historical_recovery_active == false
         and any(.nodes[];
           .node_id == $slot
           and .instance_id == $instance
           and .state == "online")' "$output" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "replacement $instance for $slot did not converge" >&2
  return 1
}

wait_cluster_converged() {
  local coordinator="$1" binding_epoch="$2" output="$3"
  local endpoint
  endpoint="$(base "$coordinator")"
  for _ in $(seq 1 240); do
    if curl_auth "$endpoint/noa/v1/cluster/health" >"$output" 2>/dev/null \
      && jq -e \
        --argjson binding_epoch "$binding_epoch" \
        '.state == "healthy"
         and .binding_epoch == $binding_epoch
         and .data_plane_readable == true
         and .data_plane_writable == true
         and .ncnx_strictly_writable == true
         and .effective_client_writable == true
         and .historical_recovery_active == false' "$output" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "cluster did not converge after full restart" >&2
  return 1
}

download_with_retry() {
  local node="$1" object_key="$2" output="$3"
  local endpoint status temporary
  endpoint="$(base "$node")"
  temporary="$output.tmp"
  for _ in $(seq 1 120); do
    status="$(
      curl -k --http1.1 -sS -o "$temporary" -w '%{http_code}' \
        -G "$endpoint/noa/v1/objects/download" \
        -H "Authorization: Bearer $TOKEN" \
        --data-urlencode "object_key=$object_key"
    )"
    if [[ "$status" == "200" ]]; then
      mv "$temporary" "$output"
      return 0
    fi
    sleep 0.25
  done
  rm -f "$temporary"
  echo "object remained unavailable after cluster convergence: $object_key ($status)" >&2
  return 1
}

mkdir -p "$OUT"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=n2-multi-pool-proof' \
  -keyout "$OUT/key.pem" -out "$OUT/cert.pem" >/dev/null 2>&1
for node in "${ALL_NODES[@]}"; do
  run="$(run_dir "$node")"
  mkdir -p "$run/state" "${STORE[$node]}"
  cp "$BIN" "$run/litiaina-n2"
  cp "$OUT/cert.pem" "$OUT/key.pem" "$run/"
  write_env "$run/n2.env"
  write_config "$node"
done

build_membership_one
install_membership "$OUT/membership-one-pool.json"
start_and_wait "${ACTIVE_A[@]}" reserve-a

curl_json -X POST "$(base a1)/noa/v1/fragments" \
  -d "{\"name\":\"multi-$TS\",\"secret\":\"$MASTER\",\"key\":\"$FRAGMENT_KEY\"}" \
  >"$OUT/fragment.json"
FRAGMENT="$(jq -er '.fragment_hash' "$OUT/fragment.json")"
curl_json -X POST "$(base a2)/noa/v1/auth/fragment" \
  -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAGMENT_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" \
  >"$OUT/auth.json"
TOKEN="$(jq -er '.access_token' "$OUT/auth.json")"

dd if=/dev/urandom of="$OUT/pool-a.bin" bs=1M count=32 status=none
SHA_A="$(sha256sum "$OUT/pool-a.bin" | awk '{print $1}')"
VERSION_A="$(multipart_upload a1 pools/from-a.bin "$OUT/pool-a.bin" upload-pool-a)"

stop_all
build_membership_two
install_membership "$OUT/membership-two-pools.json"
start_and_wait "${ACTIVE_A[@]}" "${ACTIVE_B[@]}" "${RESERVES[@]}"

curl_auth "$(base b1)/noa/v1/cluster/health" >"$OUT/health-two-pools.json"
jq -e '
  .membership_epoch == 2
  and .data_shards == 4
  and .parity_shards == 2
  and .ncnx_replicas == 3
  and .effective_client_writable == true
  and (.reserve_nodes | length) == 2
' "$OUT/health-two-pools.json" >/dev/null

dd if=/dev/urandom of="$OUT/pool-b.bin" bs=1M count=48 status=none
SHA_B="$(sha256sum "$OUT/pool-b.bin" | awk '{print $1}')"
VERSION_B="$(multipart_upload b1 pools/from-b.bin "$OUT/pool-b.bin" upload-pool-b)"

curl_auth -G "$(base b3)/noa/v1/objects/download" \
  --data-urlencode 'object_key=pools/from-a.bin' -o "$OUT/download-a-through-b.bin"
curl_auth -G "$(base a3)/noa/v1/objects/download" \
  --data-urlencode 'object_key=pools/from-b.bin' -o "$OUT/download-b-through-a.bin"
[[ "$(sha256sum "$OUT/download-a-through-b.bin" | awk '{print $1}')" == "$SHA_A" ]]
[[ "$(sha256sum "$OUT/download-b-through-a.bin" | awk '{print $1}')" == "$SHA_B" ]]

curl_auth -G "$(base a1)/noa/v1/objects" \
  --data-urlencode 'prefix=pools/' --data-urlencode 'limit=1' >"$OUT/list-page-1.json"
CURSOR="$(jq -er '.continuation_token' "$OUT/list-page-1.json")"
curl_auth -G "$(base b2)/noa/v1/objects" \
  --data-urlencode 'prefix=pools/' --data-urlencode 'limit=1' \
  --data-urlencode "continuation_token=$CURSOR" >"$OUT/list-page-2.json"
jq -s -e '
  ([.[].entries[].record] | sort) == ["pools/from-a.bin","pools/from-b.bin"]
' "$OUT/list-page-1.json" "$OUT/list-page-2.json" >/dev/null

NCNX_A="$(find "${STORE[a1]}" "${STORE[a2]}" "${STORE[a3]}" "${STORE[a4]}" \
  -path '*/.namespace/pools/from-a.bin.ncnx' -type f | wc -l | tr -d ' ')"
NCNX_B="$(find "${STORE[b1]}" "${STORE[b2]}" "${STORE[b3]}" "${STORE[b4]}" "${STORE[b5]}" "${STORE[b6]}" \
  -path '*/.namespace/pools/from-b.bin.ncnx' -type f | wc -l | tr -d ' ')"
NCDX_A="$(find "${STORE[a1]}" "${STORE[a2]}" "${STORE[a3]}" "${STORE[a4]}" \
  -path "*/@o_*/$VERSION_A.*.ncdx" -type f | wc -l | tr -d ' ')"
NCDX_B="$(find "${STORE[b1]}" "${STORE[b2]}" "${STORE[b3]}" "${STORE[b4]}" "${STORE[b5]}" "${STORE[b6]}" \
  -path "*/@o_*/$VERSION_B.*.ncdx" -type f | wc -l | tr -d ' ')"
[[ "$NCNX_A" -eq 3 && "$NCNX_B" -eq 3 ]]
[[ "$NCDX_A" -eq 4 && "$NCDX_B" -eq 6 ]]

stop_node a4
wait_slot_replacement a1 2 a4 reserve-a "$OUT/health-after-pool-a-promotion.json"
curl_auth -G "$(base b4)/noa/v1/objects/download" \
  --data-urlencode 'object_key=pools/from-a.bin' -o "$OUT/download-a-after-promotion.bin"
[[ "$(sha256sum "$OUT/download-a-after-promotion.bin" | awk '{print $1}')" == "$SHA_A" ]]

stop_node b6
wait_slot_replacement b1 3 b6 reserve-b "$OUT/health-after-pool-b-promotion.json"
curl_auth -G "$(base reserve-b)/noa/v1/objects/download" \
  --data-urlencode 'object_key=pools/from-b.bin' -o "$OUT/download-b-after-promotion.bin"
[[ "$(sha256sum "$OUT/download-b-after-promotion.bin" | awk '{print $1}')" == "$SHA_B" ]]

sed -i 's/^automatic_promotion=true$/automatic_promotion=false/' "$(run_dir a4)/n2_config.ini"
sed -i 's/^automatic_promotion=true$/automatic_promotion=false/' "$(run_dir b6)/n2_config.ini"
start_and_wait a4 b6
dd if=/dev/urandom of="$OUT/stale-write.bin" bs=1K count=1 status=none
STALE_A="$(
  curl -k --http1.1 -sS -o "$OUT/stale-a.json" -w '%{http_code}' \
    -X POST "$(base a4)/noa/v1/upload/one-shot/pools/stale-a.bin" \
    -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/stale-write.bin"
)"
STALE_B="$(
  curl -k --http1.1 -sS -o "$OUT/stale-b.json" -w '%{http_code}' \
    -X POST "$(base b6)/noa/v1/upload/one-shot/pools/stale-b.bin" \
    -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/stale-write.bin"
)"
[[ "$STALE_A" == "503" && "$STALE_B" == "503" ]]
stop_node a4
stop_node b6

for node in "${ACTIVE_A[@]:0:3}" "${ACTIVE_B[@]:0:5}" "${RESERVES[@]}"; do
  stop_node "$node"
done
start_and_wait "${ACTIVE_A[@]:0:3}" "${ACTIVE_B[@]:0:5}" "${RESERVES[@]}"
wait_cluster_converged reserve-a 3 "$OUT/health-after-full-restart.json"
download_with_retry reserve-a pools/from-a.bin "$OUT/final-a.bin"
download_with_retry reserve-b pools/from-b.bin "$OUT/final-b.bin"
[[ "$(sha256sum "$OUT/final-a.bin" | awk '{print $1}')" == "$SHA_A" ]]
[[ "$(sha256sum "$OUT/final-b.bin" | awk '{print $1}')" == "$SHA_B" ]]
curl_auth "$(base a1)/noa/v1/cluster/monitor?deep_scan=true" >"$OUT/monitor-final.json"

for node in "${ALL_NODES[@]}"; do
  cp "$(run_dir "$node")/server.log" "$OUT/$node.log" 2>/dev/null || true
done
ERROR_LINES="$(
  {
    grep -Eih 'panic|fatal|internal server error|corrupt' "$OUT"/*.log || true
  } | wc -l | tr -d ' '
)"

jq -n \
  --arg result PASS \
  --arg stale_a "$STALE_A" \
  --arg stale_b "$STALE_B" \
  --argjson ncnx_a "$NCNX_A" \
  --argjson ncnx_b "$NCNX_B" \
  --argjson ncdx_a "$NCDX_A" \
  --argjson ncdx_b "$NCDX_B" \
  --argjson suspicious "$ERROR_LINES" \
  --slurpfile initial "$OUT/health-two-pools.json" \
  --slurpfile promoted_a "$OUT/health-after-pool-a-promotion.json" \
  --slurpfile promoted_b "$OUT/health-after-pool-b-promotion.json" \
  --slurpfile monitor "$OUT/monitor-final.json" \
  '{
    result:$result,
    topology:"Pool A EC 2+2 sealed after append-only Pool B EC 4+2; two global reserves",
    old_pool_object_survived_expansion:true,
    new_pool_object_cross_node_read:true,
    global_pagination_exact:true,
    exact_layouts:{
      pool_a:{ncnx_replicas:$ncnx_a,ncdx_replicas:$ncdx_a},
      pool_b:{ncnx_replicas:$ncnx_b,ncdx_replicas:$ncdx_b}
    },
    reserve:{
      pool_a_replacement:"reserve-a",
      pool_b_replacement:"reserve-b",
      binding_epoch:3,
      stale_a_status:($stale_a|tonumber),
      stale_b_status:($stale_b|tonumber),
      stale_instances_rejected:true,
      pool_a_health:$promoted_a[0],
      pool_b_health:$promoted_b[0]
    },
    restart_persistence:true,
    suspicious_log_lines:$suspicious,
    initial_health:$initial[0],
    final_monitor:$monitor[0]
  }' >"$OUT/summary.json"

RESULT="PASS"
echo "N2_MULTI_POOL_PROOF_OUT=$OUT"
