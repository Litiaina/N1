#!/usr/bin/env bash
set -Eeuo pipefail

# Finite, single-host proof of N2's minimum production contract:
# one exact-width EC 1+1 pool, NCNX R=2, and one autonomous reserve.
# Every destructive path must be supplied by the caller under
# /mnt/NDISK*/.n2-validation/<run-id>/.

: "${N2_MIN_BIN:?set N2_MIN_BIN to the tested litiaina-n2 binary}"
: "${N2_MIN_OUT:?set N2_MIN_OUT to the report directory}"
: "${N2_MIN_A_STORE:?set N2_MIN_A_STORE}"
: "${N2_MIN_B_STORE:?set N2_MIN_B_STORE}"
: "${N2_MIN_R_STORE:?set N2_MIN_R_STORE}"

BIN="$N2_MIN_BIN"
OUT="$N2_MIN_OUT"
A_STORE="$N2_MIN_A_STORE"
B_STORE="$N2_MIN_B_STORE"
R_STORE="$N2_MIN_R_STORE"
RUN="$OUT/runtime"
A_RUN="$RUN/node-a"
B_RUN="$RUN/node-b"
R_RUN="$RUN/reserve-r"
TS="${N2_MIN_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"

A_HTTPS="${N2_MIN_A_HTTPS:-21001}"
A_NATIVE="${N2_MIN_A_NATIVE:-21002}"
B_HTTPS="${N2_MIN_B_HTTPS:-21011}"
B_NATIVE="${N2_MIN_B_NATIVE:-21012}"
R_HTTPS="${N2_MIN_R_HTTPS:-21021}"
R_NATIVE="${N2_MIN_R_NATIVE:-21022}"
A_BASE="https://127.0.0.1:$A_HTTPS"
B_BASE="https://127.0.0.1:$B_HTTPS"
R_BASE="https://127.0.0.1:$R_HTTPS"
POOL_ID="minimum-pool-$TS"
CLUSTER_ID="minimum-cluster-$TS"
MASTER="minimum-master-$TS"
FRAGMENT_KEY="minimum-fragment-key-$TS"
TOKEN=""
RESULT="FAIL"
ROOT_FAULTS="${N2_MIN_ROOT_FAULTS:-false}"
ROOT_UNAVAILABLE_STATUS=""
ROOT_UNAVAILABLE_OBJECT_STATUS=""
ROOT_UNAVAILABLE_RESTORED_OBJECT_STATUS=""
ROOT_FULL_STATUS=""
ROOT_FULL_OBJECT_STATUS=""

for tool in jq curl openssl pidstat sha256sum awk dd find; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done
if [[ "$ROOT_FAULTS" == "true" ]]; then
  for tool in fallocate mountpoint stat df; do
    command -v "$tool" >/dev/null || {
      echo "missing root-fault proof tool: $tool" >&2
      exit 1
    }
  done
fi
[[ -x "$BIN" ]] || {
  echo "N2 binary is missing or not executable: $BIN" >&2
  exit 1
}

assert_isolated_store() {
  local path="$1"
  [[ "$path" == /mnt/NDISK*/.n2-validation/* ]] || {
    echo "refusing unsafe N2 test store: $path" >&2
    exit 1
  }
  [[ "$path" != *"/../"* && "$path" != *"/./"* ]]
}

for store in "$A_STORE" "$B_STORE" "$R_STORE"; do
  assert_isolated_store "$store"
done

assert_ports_available() {
  local port
  command -v ss >/dev/null || {
    echo "missing required tool: ss" >&2
    exit 1
  }
  for port in "$@"; do
    if [[ -n "$(ss -H -ltn "sport = :$port")" ]]; then
      echo "required N2 proof port is already in use: $port" >&2
      exit 1
    fi
  done
}
assert_ports_available \
  "$A_HTTPS" "$A_NATIVE" "$B_HTTPS" "$B_NATIVE" "$R_HTTPS" "$R_NATIVE"

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

cleanup() {
  set +e
  if [[ "$ROOT_FAULTS" == "true" ]]; then
    chmod 0755 "$B_STORE" 2>/dev/null || true
    rm -f "$B_STORE/.n2-proof-enospc" 2>/dev/null || true
  fi
  for run in "$A_RUN" "$B_RUN" "$R_RUN"; do
    kill_pidfile "$run/pidstat.pid"
    kill_pidfile "$run/n2.pid"
  done
  if [[ "$RESULT" == "PASS" ]]; then
    rm -rf "$A_STORE" "$R_STORE"
    if [[ "$ROOT_FAULTS" == "true" ]] && mountpoint -q "$B_STORE"; then
      find "$B_STORE" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    else
      rm -rf "$B_STORE"
    fi
  fi
}
trap cleanup EXIT

write_env() {
  local path="$1"
  cat > "$path" <<EOF
JWT_SECRET=minimum-jwt-$TS
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_CLUSTER_SECRET=minimum-cluster-secret-$TS
EOF
}

write_config() {
  local path="$1" instance="$2" https_port="$3" native_port="$4" store="$5" run="$6" root="$7"
  cat > "$path" <<EOF
[server]
host=127.0.0.1
https_port=$https_port
native_port=$native_port
cert_path=$run/cert.pem
key_path=$run/key.pem

[disk_config]
storage_roots=$root=$store
runtime_path=$run/state

[cluster_config]
node_id=$instance
pool_id=$POOL_ID
pool_generation=1
node_endpoint=https://127.0.0.1:$https_port
native_endpoint=n2://127.0.0.1:$native_port
membership_file=$run/state/n2_membership.json
object_data_shards=1
object_parity_shards=1
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
max_parallel_fragments=1
max_objects_per_fragment_cycle=256
max_duration_seconds=60
lease_seconds=90
checksum_scan_every_cycles=1

[reserve_set]
enabled=true
automatic_promotion=true
initial_delay_seconds=3
probe_interval_seconds=1
failure_threshold=2
promotion_timeout_seconds=30

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
data_shards=1
parity_shards=1
stripe_block_size=1048576
EOF
}

start_node() {
  local run="$1"
  (
    cd "$run"
    setsid ./litiaina-n2 </dev/null >>server.log 2>&1 &
    echo $! >n2.pid
  )
  local pid
  pid="$(<"$run/n2.pid")"
  pidstat -h -u -r -d -w -p "$pid" 1 >>"$run/pidstat.log" 2>&1 &
  echo $! >"$run/pidstat.pid"
}

stop_node() {
  local run="$1"
  kill_pidfile "$run/pidstat.pid"
  kill_pidfile "$run/n2.pid"
}

wait_ping() {
  local base="$1"
  for _ in $(seq 1 120); do
    curl -k --http1.1 -fsS "$base/noa/v1/ping" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  echo "node failed to become ready: $base" >&2
  return 1
}

curl_json() {
  curl -k --http1.1 -fsS -H 'Content-Type: application/json' "$@"
}

curl_auth() {
  curl -k --http1.1 -fsS -H "Authorization: Bearer $TOKEN" "$@"
}

wait_binding_epoch() {
  local epoch="$1" output="$2"
  for _ in $(seq 1 160); do
    if curl_auth "$A_BASE/noa/v1/cluster/health" >"$output" 2>/dev/null \
      && [[ "$(jq -r '.binding_epoch' "$output")" == "$epoch" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "binding epoch $epoch was not committed" >&2
  return 1
}

wait_replacement_online() {
  local output="$1"
  for _ in $(seq 1 480); do
    if curl_auth "$A_BASE/noa/v1/cluster/health" >"$output" 2>/dev/null \
      && jq -e '
        .historical_recovery_active == false
        and any(.nodes[];
          .node_id == "minimum-b"
          and .instance_id == "minimum-r"
          and .state == "online")
      ' "$output" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "reserve replacement did not finish healing" >&2
  return 1
}

mkdir -p "$OUT" "$A_RUN/state" "$B_RUN/state" "$R_RUN/state"
mkdir -p "$A_STORE" "$B_STORE" "$R_STORE"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=n2-minimum-proof' \
  -keyout "$OUT/key.pem" -out "$OUT/cert.pem" >/dev/null 2>&1

for run in "$A_RUN" "$B_RUN" "$R_RUN"; do
  cp "$BIN" "$run/litiaina-n2"
  cp "$OUT/cert.pem" "$OUT/key.pem" "$run/"
  write_env "$run/n2.env"
done

cat >"$OUT/n2_membership.json" <<EOF
{
  "format_version": 5,
  "cluster_id": "$CLUSTER_ID",
  "epoch": 1,
  "binding_epoch": 1,
  "pools": [{
    "id": "$POOL_ID",
    "generation": 1,
    "policy": {"object_data_shards": 1, "object_parity_shards": 1},
    "state": "writable",
    "slot_ids": ["minimum-a", "minimum-b"]
  }],
  "slot_bindings": [
    {
      "slot_id":"minimum-a",
      "instance_id":"minimum-a",
      "binding_epoch":1,
      "endpoint":"$A_BASE",
      "native_endpoint":"n2://127.0.0.1:$A_NATIVE",
      "state":"online",
      "weight":1,
      "storage_roots":[{"id":"minimum-root-a","path":"$A_STORE"}],
      "joined_epoch":1
    },
    {
      "slot_id":"minimum-b",
      "instance_id":"minimum-b",
      "binding_epoch":1,
      "endpoint":"$B_BASE",
      "native_endpoint":"n2://127.0.0.1:$B_NATIVE",
      "state":"online",
      "weight":1,
      "storage_roots":[{"id":"minimum-root-b","path":"$B_STORE"}],
      "joined_epoch":1
    }
  ],
  "reserve_nodes": [{
    "instance_id":"minimum-r",
    "endpoint":"$R_BASE",
    "native_endpoint":"n2://127.0.0.1:$R_NATIVE",
    "state":"online",
    "storage_roots":[{"id":"minimum-root-r","path":"$R_STORE"}]
  }],
  "retired_instances": []
}
EOF

for run in "$A_RUN" "$B_RUN" "$R_RUN"; do
  cp "$OUT/n2_membership.json" "$run/state/"
done
write_config "$A_RUN/n2_config.ini" minimum-a "$A_HTTPS" "$A_NATIVE" "$A_STORE" "$A_RUN" minimum-root-a
write_config "$B_RUN/n2_config.ini" minimum-b "$B_HTTPS" "$B_NATIVE" "$B_STORE" "$B_RUN" minimum-root-b
write_config "$R_RUN/n2_config.ini" minimum-r "$R_HTTPS" "$R_NATIVE" "$R_STORE" "$R_RUN" minimum-root-r

start_node "$A_RUN"
start_node "$B_RUN"
start_node "$R_RUN"
wait_ping "$A_BASE"
wait_ping "$B_BASE"
wait_ping "$R_BASE"

curl_json -X POST "$A_BASE/noa/v1/fragments" \
  -d "{\"name\":\"minimum-$TS\",\"secret\":\"$MASTER\",\"key\":\"$FRAGMENT_KEY\"}" \
  >"$OUT/fragment.json"
FRAGMENT="$(jq -er '.fragment_hash' "$OUT/fragment.json")"
curl_json -X POST "$B_BASE/noa/v1/auth/fragment" \
  -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAGMENT_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" \
  >"$OUT/auth.json"
TOKEN="$(jq -er '.access_token' "$OUT/auth.json")"

curl_auth "$A_BASE/noa/v1/cluster/health" >"$OUT/health-initial.json"
jq -e '
  .data_shards == 1
  and .parity_shards == 1
  and .ncnx_replicas == 2
  and .effective_client_writable == true
  and (.reserve_nodes | length) == 1
' "$OUT/health-initial.json" >/dev/null

dd if=/dev/urandom of="$OUT/source.bin" bs=1M count=4 status=none
SOURCE_SHA="$(sha256sum "$OUT/source.bin" | awk '{print $1}')"
curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/minimum/anchor.bin" \
  --data-binary @"$OUT/source.bin" >"$OUT/upload.json"
curl_auth -G "$B_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=minimum/anchor.bin' -o "$OUT/download.bin"
[[ "$(sha256sum "$OUT/download.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]

for key in minimum/page/a minimum/page/a-previous minimum/page/a/child minimum/page/a0; do
  curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/$key" \
    --data-binary @"$OUT/source.bin" >"$OUT/upload-$(echo "$key" | tr '/' '_').json"
done
CURSOR=""
: >"$OUT/pages.txt"
for expected in minimum/page/a minimum/page/a-previous minimum/page/a/child minimum/page/a0; do
  if [[ -n "$CURSOR" ]]; then
    curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=minimum/page/' \
      --data-urlencode 'limit=1' --data-urlencode "continuation_token=$CURSOR" >"$OUT/page.json"
  else
    curl_auth -G "$B_BASE/noa/v1/objects" --data-urlencode 'prefix=minimum/page/' \
      --data-urlencode 'limit=1' >"$OUT/page.json"
  fi
  [[ "$(jq -er '.entries[0].record' "$OUT/page.json")" == "$expected" ]]
  echo "$expected" >>"$OUT/pages.txt"
  CURSOR="$(jq -r '.continuation_token // empty' "$OUT/page.json")"
done

curl_json -X POST "$A_BASE/noa/v1/posix/trash" \
  -H "Authorization: Bearer $TOKEN" -d '{"path":"minimum/page"}' >"$OUT/trash.json"
TRASH_ID="$(jq -er '.trash_entry_id' "$OUT/trash.json")"
jq -e '.affected == 4' "$OUT/trash.json" >/dev/null
curl_json -X POST "$B_BASE/noa/v1/posix/recover" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"path\":\"minimum/page\",\"trash_entry_id\":\"$TRASH_ID\"}" >"$OUT/recover.json"
jq -e '.affected == 4' "$OUT/recover.json" >/dev/null

mapfile -t NCNX_FILES < <(
  find "$A_STORE" "$B_STORE" -path '*/.namespace/minimum/anchor.bin.ncnx' -type f | sort
)
[[ "${#NCNX_FILES[@]}" -eq 2 ]]
rm -f "${NCNX_FILES[0]}"
curl_auth -X POST "$A_BASE/noa/v1/cluster/healing/cycle" >"$OUT/heal-ncnx-a.json" || true
curl_auth -X POST "$B_BASE/noa/v1/cluster/healing/cycle" >"$OUT/heal-ncnx-b.json" || true
[[ -f "${NCNX_FILES[0]}" ]]

VERSION="$(jq -er '.version_id' "$OUT/upload.json")"
mapfile -t SHARD_FILES < <(
  find "$A_STORE" "$B_STORE" -path "*/$VERSION/part_0_*" -type f | sort
)
[[ "${#SHARD_FILES[@]}" -eq 2 ]]
rm -f "${SHARD_FILES[0]}"
curl_auth -X POST "$A_BASE/noa/v1/cluster/healing/cycle" >"$OUT/heal-object-a.json" || true
curl_auth -X POST "$B_BASE/noa/v1/cluster/healing/cycle" >"$OUT/heal-object-b.json" || true
[[ -f "${SHARD_FILES[0]}" ]]

if [[ "$ROOT_FAULTS" == "true" ]]; then
  mountpoint -q "$B_STORE" || {
    echo "refusing ENOSPC proof because node B root is not an isolated mount: $B_STORE" >&2
    exit 1
  }

  chmod 0000 "$B_STORE"
  ROOT_UNAVAILABLE_STATUS="$(
    curl -k --http1.1 -sS -o "$OUT/root-unavailable-write.json" -w '%{http_code}' \
      -X POST "$A_BASE/noa/v1/upload/one-shot/minimum/root-unavailable-survival.bin" \
      -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/source.bin"
  )"
  [[ "$ROOT_UNAVAILABLE_STATUS" == 2* ]]
  ROOT_UNAVAILABLE_OBJECT_STATUS="$(
    curl -k --http1.1 -sS -o "$OUT/root-unavailable-object.json" -w '%{http_code}' \
      -G "$A_BASE/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" \
      --data-urlencode 'object_key=minimum/root-unavailable-survival.bin'
  )"
  [[ "$ROOT_UNAVAILABLE_OBJECT_STATUS" == "200" ]]
  [[ "$(sha256sum "$OUT/root-unavailable-object.json" | awk '{print $1}')" == "$SOURCE_SHA" ]]
  curl_auth -G "$A_BASE/noa/v1/objects/download" \
    --data-urlencode 'object_key=minimum/anchor.bin' \
    -o "$OUT/download-root-unavailable.bin"
  [[ "$(sha256sum "$OUT/download-root-unavailable.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
  sleep 3
  curl_auth "$A_BASE/noa/v1/cluster/health" >"$OUT/health-root-unavailable.json"
  jq -e '
    .binding_epoch == 1
    and (.reserve_nodes | length) == 1
    and .state == "degraded"
    and .effective_client_writable == true
    and .data_plane_writable == true
    and .data_plane_fully_protected == false
    and .ncnx_survival_writable == true
    and .ncnx_strictly_writable == false
    and any(
      .nodes[];
      .node_id == "minimum-b"
      and .storage_readable == false
      and .storage_writable == false
      and .accepts_writes == false
    )
  ' \
    "$OUT/health-root-unavailable.json" >/dev/null
  chmod 0755 "$B_STORE"
  ROOT_UNAVAILABLE_RESTORED_OBJECT_STATUS="$(
    curl -k --http1.1 -sS -o "$OUT/root-unavailable-object-after-restore.json" \
      -w '%{http_code}' -G "$A_BASE/noa/v1/objects/download" \
      -H "Authorization: Bearer $TOKEN" \
      --data-urlencode 'object_key=minimum/root-unavailable-survival.bin'
  )"
  [[ "$ROOT_UNAVAILABLE_RESTORED_OBJECT_STATUS" == "200" ]]
  [[ "$(sha256sum "$OUT/root-unavailable-object-after-restore.json" | awk '{print $1}')" == "$SOURCE_SHA" ]]

  ROOT_AVAILABLE_BYTES="$(df -B1 --output=avail "$B_STORE" | tail -n 1 | tr -d ' ')"
  (( ROOT_AVAILABLE_BYTES > 16777216 ))
  fallocate -l "$((ROOT_AVAILABLE_BYTES - 1048576))" "$B_STORE/.n2-proof-enospc"
  ROOT_FULL_STATUS="$(
    curl -k --http1.1 -sS -o "$OUT/root-full-write.json" -w '%{http_code}' \
      -X POST "$A_BASE/noa/v1/upload/one-shot/minimum/root-full-survival.bin" \
      -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/source.bin"
  )"
  [[ "$ROOT_FULL_STATUS" == 2* ]]
  ROOT_FULL_OBJECT_STATUS="$(
    curl -k --http1.1 -sS -o "$OUT/root-full-object.json" -w '%{http_code}' \
      -G "$A_BASE/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" \
      --data-urlencode 'object_key=minimum/root-full-survival.bin'
  )"
  [[ "$ROOT_FULL_OBJECT_STATUS" == "200" ]]
  [[ "$(sha256sum "$OUT/root-full-object.json" | awk '{print $1}')" == "$SOURCE_SHA" ]]
  curl_auth -G "$A_BASE/noa/v1/objects/download" \
    --data-urlencode 'object_key=minimum/anchor.bin' \
    -o "$OUT/download-root-full.bin"
  [[ "$(sha256sum "$OUT/download-root-full.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
  curl_auth "$A_BASE/noa/v1/cluster/health" >"$OUT/health-root-full.json"
  jq -e '
    .binding_epoch == 1
    and (.reserve_nodes | length) == 1
    and .state == "degraded"
    and .effective_client_writable == true
    and .data_plane_writable == true
    and .data_plane_fully_protected == false
    and .ncnx_survival_writable == true
    and .ncnx_strictly_writable == false
    and any(
      .nodes[];
      .node_id == "minimum-b"
      and .storage_readable == true
      and .storage_writable == false
      and .accepts_writes == false
    )
  ' "$OUT/health-root-full.json" >/dev/null
  rm -f "$B_STORE/.n2-proof-enospc"

  for _ in $(seq 1 30); do
    curl_auth "$A_BASE/noa/v1/cluster/health" >"$OUT/health-root-restored.json"
    if jq -e '
      .state == "healthy"
      and .effective_client_writable == true
      and .data_plane_writable == true
      and .ncnx_strictly_writable == true
      and all(.nodes[]; .storage_readable == true and .storage_writable == true)
    ' "$OUT/health-root-restored.json" >/dev/null; then
      break
    fi
    sleep 1
  done
  jq -e '
    .state == "healthy"
    and .effective_client_writable == true
    and .data_plane_writable == true
    and .ncnx_strictly_writable == true
    and all(.nodes[]; .storage_readable == true and .storage_writable == true)
  ' "$OUT/health-root-restored.json" >/dev/null
  curl_auth -X POST "$B_BASE/noa/v1/upload/one-shot/minimum/root-restored.bin" \
    --data-binary @"$OUT/source.bin" >"$OUT/root-restored-write.json"
  curl_auth -G "$A_BASE/noa/v1/objects/download" \
    --data-urlencode 'object_key=minimum/root-restored.bin' \
    -o "$OUT/download-root-restored.bin"
  [[ "$(sha256sum "$OUT/download-root-restored.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
fi

stop_node "$B_RUN"
curl_auth -G "$A_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=minimum/anchor.bin' -o "$OUT/download-one-loss.bin"
[[ "$(sha256sum "$OUT/download-one-loss.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]

PRE_PROMOTION_STATUS="$(
  curl -k --http1.1 -sS -o "$OUT/pre-promotion-write.json" -w '%{http_code}' \
    -X POST "$A_BASE/noa/v1/upload/one-shot/minimum/pre-promotion.bin" \
    -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/source.bin"
)"
[[ "$PRE_PROMOTION_STATUS" == 2* ]]
curl_auth -G "$A_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=minimum/pre-promotion.bin' \
  -o "$OUT/pre-promotion-download.bin"
[[ "$(sha256sum "$OUT/pre-promotion-download.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
wait_binding_epoch 2 "$OUT/health-after-promotion.json"
jq -e '
  .effective_client_writable == true
  and (.reserve_nodes | length) == 0
  and any(.nodes[];
    .node_id == "minimum-b"
    and .instance_id == "minimum-r"
    and (.state == "degraded" or .state == "online"))
' "$OUT/health-after-promotion.json" >/dev/null

curl_auth -X POST "$A_BASE/noa/v1/upload/one-shot/minimum/during-recovery.bin" \
  --data-binary @"$OUT/source.bin" >"$OUT/write-during-recovery.json"
wait_replacement_online "$OUT/health-after-recovery.json"

sed -i 's/^automatic_promotion=true$/automatic_promotion=false/' "$B_RUN/n2_config.ini"
start_node "$B_RUN"
wait_ping "$B_BASE"
STALE_STATUS="$(
  curl -k --http1.1 -sS -o "$OUT/stale-write.json" -w '%{http_code}' \
    -X POST "$B_BASE/noa/v1/upload/one-shot/minimum/stale-must-fail.bin" \
    -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/source.bin"
)"
[[ "$STALE_STATUS" != 2* ]]
stop_node "$B_RUN"

stop_node "$A_RUN"
stop_node "$R_RUN"
start_node "$A_RUN"
start_node "$R_RUN"
wait_ping "$A_BASE"
wait_ping "$R_BASE"
curl_auth -G "$R_BASE/noa/v1/objects/download" \
  --data-urlencode 'object_key=minimum/anchor.bin' -o "$OUT/download-after-restart.bin"
[[ "$(sha256sum "$OUT/download-after-restart.bin" | awk '{print $1}')" == "$SOURCE_SHA" ]]
curl_auth "$A_BASE/noa/v1/cluster/monitor?deep_scan=true" >"$OUT/monitor-final.json"

cp "$A_RUN/server.log" "$OUT/node-a.log"
cp "$B_RUN/server.log" "$OUT/node-b.log"
cp "$R_RUN/server.log" "$OUT/reserve-r.log"
ERROR_LINES="$(
  {
    grep -Eih 'panic|fatal|internal server error|corrupt' \
      "$OUT/node-a.log" "$OUT/node-b.log" "$OUT/reserve-r.log" || true
  } | wc -l | tr -d ' '
)"

jq -n \
  --arg result PASS \
  --arg ec "1+1" \
  --arg pre_promotion_status "$PRE_PROMOTION_STATUS" \
  --arg stale_status "$STALE_STATUS" \
  --arg root_faults "$ROOT_FAULTS" \
  --arg root_unavailable_status "$ROOT_UNAVAILABLE_STATUS" \
  --arg root_unavailable_object_status "$ROOT_UNAVAILABLE_OBJECT_STATUS" \
  --arg root_unavailable_restored_object_status "$ROOT_UNAVAILABLE_RESTORED_OBJECT_STATUS" \
  --arg root_full_status "$ROOT_FULL_STATUS" \
  --arg root_full_object_status "$ROOT_FULL_OBJECT_STATUS" \
  --argjson suspicious_log_lines "$ERROR_LINES" \
  --slurpfile initial "$OUT/health-initial.json" \
  --slurpfile promoted "$OUT/health-after-promotion.json" \
  --slurpfile recovered "$OUT/health-after-recovery.json" \
  --slurpfile monitor "$OUT/monitor-final.json" \
  '{
    result:$result,
    topology:"one EC 1+1 pool; two active logical nodes; NCNX R=2; one reserve",
    ec:$ec,
    initial_health:$initial[0],
    pagination_keys:4,
    namespace_healing:"missing NCNX replica restored",
    object_healing:"missing shard restored",
    storage_root_faults:{
      enabled:($root_faults == "true"),
      inaccessible_root_write_http_status:
        (if $root_unavailable_status == "" then null else ($root_unavailable_status|tonumber) end),
      inaccessible_root_object_read_http_status:
        (if $root_unavailable_object_status == "" then null else ($root_unavailable_object_status|tonumber) end),
      inaccessible_root_object_after_restore_http_status:
        (if $root_unavailable_restored_object_status == "" then null else ($root_unavailable_restored_object_status|tonumber) end),
      full_root_write_http_status:
        (if $root_full_status == "" then null else ($root_full_status|tonumber) end),
      full_root_object_read_http_status:
        (if $root_full_object_status == "" then null else ($root_full_object_status|tonumber) end),
      existing_object_readable:true,
      reserve_not_consumed_by_capacity_pressure:true,
      write_after_root_restoration:true
    },
    reserve:{
      one_node_read_survived:true,
      pre_promotion_http_status:($pre_promotion_status|tonumber),
      binding_epoch_advanced:true,
      replacement:"minimum-r",
      historical_layout_repaired:true,
      stale_retired_write_http_status:($stale_status|tonumber),
      stale_retired_write_rejected:true,
      health_after_promotion:$promoted[0],
      health_after_recovery:$recovered[0]
    },
    restart_persistence:true,
    suspicious_log_lines:$suspicious_log_lines,
    final_monitor:$monitor[0]
  }' >"$OUT/summary.json"

RESULT="PASS"
echo "N2_MINIMUM_PROOF_OUT=$OUT"
