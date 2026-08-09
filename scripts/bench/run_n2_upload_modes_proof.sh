#!/usr/bin/env bash
set -Eeuo pipefail

# Finite, all-local N2 multipart contract proof. It exercises a real two-node
# 1+1 server pool without installing services or touching paths outside the
# configured DEV_TEST root.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_ROOT="${N2_UPLOAD_PROOF_OUT_ROOT:-/home/litiaina/DEV_TEST}"
BIN="${N2_UPLOAD_PROOF_BIN:-$ROOT_DIR/target/release/litiaina-n2}"
PROOF_MIB="${N2_UPLOAD_PROOF_MIB:-128}"
PART_MIB="${N2_UPLOAD_PROOF_PART_MIB:-16}"
PARALLEL_PARTS="${N2_UPLOAD_PROOF_PARALLEL_PARTS:-4}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT="$OUT_ROOT/n2-upload-modes-$TS"
RUN="$OUT/runtime"
A="$RUN/node-a"
B="$RUN/node-b"
A_STORE="$OUT/storage/node-a"
B_STORE="$OUT/storage/node-b"
A_HTTPS=23101
A_NATIVE=23102
B_HTTPS=23111
B_NATIVE=23112
A_BASE="https://127.0.0.1:$A_HTTPS"
B_BASE="https://127.0.0.1:$B_HTTPS"
POOL="upload-proof-$TS"
CLUSTER="upload-proof-$TS"
MASTER="upload-proof-master-$TS"
FRAGMENT_NAME="upload-proof-$TS"
FRAGMENT_KEY="upload-proof-fragment-$TS"
TOKEN=""

case "$(realpath -m "$OUT")" in
  "$(realpath -m "$OUT_ROOT")"/*) ;;
  *) echo "proof output escaped OUT_ROOT: $OUT" >&2; exit 2 ;;
esac

for tool in curl jq openssl sha256sum split cmp awk find stat ss; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done
[[ -x "$BIN" ]] || { echo "release binary is missing: $BIN" >&2; exit 2; }

for port in "$A_HTTPS" "$A_NATIVE" "$B_HTTPS" "$B_NATIVE"; do
  [[ -z "$(ss -H -ltn "sport = :$port")" ]] || {
    echo "proof port is already in use: $port" >&2
    exit 2
  }
done

stop_node() {
  local run="$1"
  if [[ -f "$run/n2.pid" ]]; then
    local pid
    pid="$(cat "$run/n2.pid")"
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$run/n2.pid"
  fi
}

cleanup() {
  set +e
  stop_node "$A"
  stop_node "$B"
}
trap cleanup EXIT

wait_ping() {
  local base="$1"
  for _ in $(seq 1 120); do
    curl -kfsS "$base/noa/v1/ping" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "node did not become ready: $base" >&2
  return 1
}

start_node() {
  local run="$1"
  (cd "$run"; setsid ./litiaina-n2 </dev/null >>server.log 2>&1 & echo $! >n2.pid)
}

write_config() {
  local run="$1" node="$2" https_port="$3" native_port="$4" store="$5" root_id="$6"
  cat > "$run/n2_config.ini" <<EOF
[server]
host=127.0.0.1
https_port=$https_port
native_port=$native_port
cert_path=$run/cert.pem
key_path=$run/key.pem

[disk_config]
storage_roots=$root_id=$store
runtime_path=$run/state

[cluster_config]
node_id=$node
pool_id=$POOL
pool_generation=1
node_endpoint=https://127.0.0.1:$https_port
native_endpoint=n2://127.0.0.1:$native_port
membership_file=$run/state/n2_membership.json
object_data_shards=1
object_parity_shards=1
auto_rebalance=false

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=60
retention_seconds=60

[auto_healing]
enabled=false
initial_delay_seconds=3600
scan_interval_seconds=3600
coordinator_nodes=auto
max_parallel_fragments=1
max_objects_per_fragment_cycle=256
max_duration_seconds=60
lease_seconds=90
checksum_scan_every_cycles=1

[reserve_set]
enabled=false
automatic_promotion=false
initial_delay_seconds=3600
probe_interval_seconds=5
failure_threshold=3
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

mkdir -p "$A/state" "$B/state" "$A_STORE" "$B_STORE"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=n2-upload-proof' \
  -keyout "$OUT/key.pem" -out "$OUT/cert.pem" >/dev/null 2>&1
for run in "$A" "$B"; do
  cp "$BIN" "$run/litiaina-n2"
  cp "$OUT/cert.pem" "$OUT/key.pem" "$run/"
  cat > "$run/n2.env" <<EOF
JWT_SECRET=upload-proof-jwt-$TS
MASTER_KEY=$MASTER
AUTH_KEY=$MASTER
ADMIN_KEY=$MASTER
ACCESS_TOKEN_DEFAULT_TTL_SECS=3600
ACCESS_TOKEN_MAX_TTL_SECS=21600
REFRESH_TOKEN_TTL_SECS=604800
N2_CLUSTER_SECRET=upload-proof-cluster-$TS
EOF
done

cat > "$OUT/n2_membership.json" <<EOF
{
  "format_version": 5,
  "cluster_id": "$CLUSTER",
  "epoch": 1,
  "binding_epoch": 1,
  "pools": [{
    "id": "$POOL",
    "generation": 1,
    "policy": {"object_data_shards": 1, "object_parity_shards": 1},
    "state": "writable",
    "slot_ids": ["node-a", "node-b"]
  }],
  "slot_bindings": [
    {"slot_id":"node-a","instance_id":"node-a","binding_epoch":1,"endpoint":"$A_BASE","native_endpoint":"n2://127.0.0.1:$A_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-a","path":"$A_STORE"}],"joined_epoch":1},
    {"slot_id":"node-b","instance_id":"node-b","binding_epoch":1,"endpoint":"$B_BASE","native_endpoint":"n2://127.0.0.1:$B_NATIVE","state":"online","weight":1,"storage_roots":[{"id":"root-b","path":"$B_STORE"}],"joined_epoch":1}
  ],
  "reserve_nodes": [],
  "retired_instances": []
}
EOF
cp "$OUT/n2_membership.json" "$A/state/"
cp "$OUT/n2_membership.json" "$B/state/"
write_config "$A" node-a "$A_HTTPS" "$A_NATIVE" "$A_STORE" root-a
write_config "$B" node-b "$B_HTTPS" "$B_NATIVE" "$B_STORE" root-b

start_node "$A"
start_node "$B"
wait_ping "$A_BASE"
wait_ping "$B_BASE"

curl -kfsS -X POST "$A_BASE/noa/v1/fragments" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$FRAGMENT_NAME\",\"secret\":\"$MASTER\",\"key\":\"$FRAGMENT_KEY\"}" \
  >"$OUT/fragment.json"
FRAGMENT="$(jq -er '.fragment_hash' "$OUT/fragment.json")"
curl -kfsS -X POST "$A_BASE/noa/v1/auth/fragment" \
  -H 'Content-Type: application/json' \
  -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$FRAGMENT_KEY\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" \
  >"$OUT/auth.json"
TOKEN="$(jq -er '.access_token' "$OUT/auth.json")"

# NOA standard batch is one transport envelope over independent one-shot
# commits. Prove all-success, mixed-result, byte-exact readback, and restart
# persistence without involving the durable multipart family.
BATCH_COUNT=64
BATCH_DIR="$OUT/batch-payloads"
mkdir -p "$BATCH_DIR"
BATCH_FORM=()
for index in $(seq 0 $((BATCH_COUNT - 1))); do
  name="item-$(printf '%03d' "$index").bin"
  dd if=/dev/urandom of="$BATCH_DIR/$name" bs=8192 count=1 status=none
  BATCH_FORM+=(
    -F "object=@$BATCH_DIR/$name;filename=proof%2Fbatch%2F$name;type=application/octet-stream"
  )
done
BATCH_STARTED="$(date +%s%N)"
BATCH_STATUS="$(curl -ksS -o "$OUT/batch-response.json" -w '%{http_code}' \
  -X POST "$A_BASE/noa/v1/upload/batch?concurrency=8" \
  -H "Authorization: Bearer $TOKEN" "${BATCH_FORM[@]}")"
BATCH_ELAPSED="$(awk -v ns="$(( $(date +%s%N) - BATCH_STARTED ))" 'BEGIN {printf "%.6f",ns/1000000000}')"
[[ "$BATCH_STATUS" == "201" ]]
jq -e --argjson expected "$BATCH_COUNT" \
  '.requested == $expected and .succeeded == $expected and .failed == 0 and (.results | length) == $expected and all(.results[]; .status == 201 and (.version_id | length) == 26)' \
  "$OUT/batch-response.json" >/dev/null
for index in 0 $((BATCH_COUNT - 1)); do
  name="item-$(printf '%03d' "$index").bin"
  curl -kfsS -G "$B_BASE/noa/v1/objects/download" \
    -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "object_key=proof/batch/$name" \
    -o "$OUT/batch-download-$name"
  cmp -s "$BATCH_DIR/$name" "$OUT/batch-download-$name"
done

dd if=/dev/zero of="$OUT/batch-too-large.bin" bs=1048577 count=1 status=none
dd if=/dev/urandom of="$OUT/batch-mixed-valid.bin" bs=4096 count=1 status=none
MIXED_STATUS="$(curl -ksS -o "$OUT/batch-mixed-response.json" -w '%{http_code}' \
  -X POST "$A_BASE/noa/v1/upload/batch?concurrency=2" \
  -H "Authorization: Bearer $TOKEN" \
  -F "object=@$OUT/batch-mixed-valid.bin;filename=proof%2Fbatch%2Fmixed-valid.bin;type=application/octet-stream" \
  -F "object=@$OUT/batch-too-large.bin;filename=proof%2Fbatch%2Ftoo-large.bin;type=application/octet-stream")"
[[ "$MIXED_STATUS" == "207" ]]
jq -e \
  '.requested == 2 and .succeeded == 1 and .failed == 1 and .results[0].status == 201 and .results[1].status == 413' \
  "$OUT/batch-mixed-response.json" >/dev/null
curl -kfsS -G "$B_BASE/noa/v1/objects/download" \
  -H "Authorization: Bearer $TOKEN" \
  --data-urlencode 'object_key=proof/batch/mixed-valid.bin' \
  -o "$OUT/batch-mixed-valid-download.bin"
cmp -s "$OUT/batch-mixed-valid.bin" "$OUT/batch-mixed-valid-download.bin"

dd if=/dev/urandom of="$OUT/source.bin" bs=1M count="$PROOF_MIB" status=none
SOURCE_BYTES="$(stat -c%s "$OUT/source.bin")"
PART_BYTES=$((PART_MIB * 1024 * 1024))
EXPECTED_PARTS=$(( (SOURCE_BYTES + PART_BYTES - 1) / PART_BYTES ))
split -b "$PART_BYTES" -d -a 5 "$OUT/source.bin" "$OUT/part-"
SOURCE_SHA="$(sha256sum "$OUT/source.bin" | awk '{print $1}')"

proc_snapshot() {
  local pid stat_line io_write
  pid="$(cat "$1/n2.pid")"
  stat_line="$(cat "/proc/$pid/stat")"
  io_write="$(awk '$1=="write_bytes:" {print $2}' "/proc/$pid/io")"
  awk -v line="$stat_line" -v io="$io_write" 'BEGIN {
    split(line,f," "); printf "%s %s %s %s\n", f[14],f[15],f[24],io
  }'
}

snapshot_cluster() {
  local a b
  a="$(proc_snapshot "$A")"
  b="$(proc_snapshot "$B")"
  awk -v a="$a" -v b="$b" 'BEGIN {
    split(a,x," "); split(b,y," ");
    printf "%d %d %d %d\n",x[1]+y[1],x[2]+y[2],x[3]+y[3],x[4]+y[4]
  }'
}

upload_mode() {
  local label="$1" route="$2" key="proof/$1.bin"
  local before_upload_files init_path version session started elapsed status_code
  before_upload_files="$(find "$A_STORE" "$B_STORE" -path '*/.uploads/*' -type f | wc -l)"
  init_path="/noa/v1/upload/multipart$route/init/$key"
  curl -kfsS -X POST "$A_BASE$init_path" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$SOURCE_BYTES,\"expected_parts\":$EXPECTED_PARTS,\"part_size_bytes\":$PART_BYTES}" \
    >"$OUT/$label-init.json"
  version="$(jq -er '.version_id' "$OUT/$label-init.json")"
  session="$(jq -er '.session_id' "$OUT/$label-init.json")"

  if [[ -z "$route" ]]; then
    [[ "$(find "$A_STORE" "$B_STORE" -path '*/.uploads/*' -type f | wc -l)" == "$before_upload_files" ]]
  else
    [[ "$(find "$A_STORE" "$B_STORE" -path '*/.uploads/*' -type f | wc -l)" -gt "$before_upload_files" ]]
  fi

  started="$(date +%s%N)"
  local running=0 part_index=0
  for part in "$OUT"/part-*; do
    curl -kfsS -X PUT "$A_BASE/noa/v1/upload/multipart$route/$version/$part_index" \
      -H "Authorization: Bearer $TOKEN" --data-binary @"$part" \
      >"$OUT/$label-part-$part_index.json" &
    running=$((running + 1))
    part_index=$((part_index + 1))
    if (( running >= PARALLEL_PARTS )); then
      wait
      running=0
    fi
  done
  wait

  curl -kfsS "$A_BASE/noa/v1/upload/multipart$route/$version" \
    -H "Authorization: Bearer $TOKEN" >"$OUT/$label-status.json"
  jq -e --argjson expected "$EXPECTED_PARTS" \
    '.committed_parts == $expected and (.uploaded_parts | length) == $expected' \
    "$OUT/$label-status.json" >/dev/null
  status_code="$(curl -ksS -o "$OUT/$label-before-finalize.json" -w '%{http_code}' \
    -G "$B_BASE/noa/v1/objects/download" -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "object_key=$key")"
  [[ "$status_code" == "404" ]]
  [[ "$(find "$A_STORE" "$B_STORE" -type f -name "*$version*.ncdx" | wc -l)" == "0" ]]

  curl -kfsS -X POST "$A_BASE/noa/v1/upload/multipart$route/$version/$session/finalize" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$SOURCE_BYTES,\"expected_parts\":$EXPECTED_PARTS,\"part_size_bytes\":$PART_BYTES}" \
    >"$OUT/$label-finalize.json"
  elapsed="$(awk -v ns="$(( $(date +%s%N) - started ))" 'BEGIN {printf "%.6f",ns/1000000000}')"
  [[ "$(find "$A_STORE" "$B_STORE" -type f -name "*$version*.ncdx" | wc -l)" -ge 1 ]]
  curl -kfsS -G "$B_BASE/noa/v1/objects/download" \
    -H "Authorization: Bearer $TOKEN" --data-urlencode "object_key=$key" \
    -o "$OUT/$label-download.bin"
  cmp -s "$OUT/source.bin" "$OUT/$label-download.bin"
  jq -n --arg label "$label" --argjson bytes "$SOURCE_BYTES" \
    --argjson seconds "$elapsed" --arg sha256 "$SOURCE_SHA" \
    '{label:$label,bytes:$bytes,seconds:$seconds,mib_per_second:(($bytes/1048576)/$seconds),sha256:$sha256,byte_exact:true}' \
    >"$OUT/$label-metric.json"
}

BEFORE="$(snapshot_cluster)"
upload_mode standard ""
upload_mode durable "/durable"
AFTER="$(snapshot_cluster)"

# Standard progress is deliberately process-local.
RESTART_BYTES="$PART_BYTES"
curl -kfsS -X POST "$A_BASE/noa/v1/upload/multipart/init/proof/standard-restart.bin" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$RESTART_BYTES,\"expected_parts\":1,\"part_size_bytes\":$RESTART_BYTES}" \
  >"$OUT/standard-restart-init.json"
STANDARD_RESTART_VERSION="$(jq -er '.version_id' "$OUT/standard-restart-init.json")"
curl -kfsS -X PUT "$A_BASE/noa/v1/upload/multipart/$STANDARD_RESTART_VERSION/0" \
  -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/part-00000" \
  >"$OUT/standard-restart-part.json"
stop_node "$A"; start_node "$A"; wait_ping "$A_BASE"
curl -kfsS "$A_BASE/noa/v1/upload/multipart/$STANDARD_RESTART_VERSION" \
  -H "Authorization: Bearer $TOKEN" >"$OUT/standard-after-restart.json"
jq -e '.committed_parts == 0 and (.session_id == null)' "$OUT/standard-after-restart.json" >/dev/null

# Durable progress is recovered from the persisted manifest and receipt.
curl -kfsS -X POST "$A_BASE/noa/v1/upload/multipart/durable/init/proof/durable-restart.bin" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$RESTART_BYTES,\"expected_parts\":1,\"part_size_bytes\":$RESTART_BYTES}" \
  >"$OUT/durable-restart-init.json"
DURABLE_RESTART_VERSION="$(jq -er '.version_id' "$OUT/durable-restart-init.json")"
DURABLE_RESTART_SESSION="$(jq -er '.session_id' "$OUT/durable-restart-init.json")"
curl -kfsS -X PUT "$A_BASE/noa/v1/upload/multipart/durable/$DURABLE_RESTART_VERSION/0" \
  -H "Authorization: Bearer $TOKEN" --data-binary @"$OUT/part-00000" \
  >"$OUT/durable-restart-part.json"
stop_node "$A"; start_node "$A"; wait_ping "$A_BASE"
curl -kfsS "$A_BASE/noa/v1/upload/multipart/durable/$DURABLE_RESTART_VERSION" \
  -H "Authorization: Bearer $TOKEN" >"$OUT/durable-after-restart.json"
jq -e '.committed_parts == 1 and (.uploaded_parts == [0])' "$OUT/durable-after-restart.json" >/dev/null
curl -kfsS -X POST "$A_BASE/noa/v1/upload/multipart/durable/$DURABLE_RESTART_VERSION/$DURABLE_RESTART_SESSION/finalize" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"content_type\":\"application/octet-stream\",\"total_size_bytes\":$RESTART_BYTES,\"expected_parts\":1,\"part_size_bytes\":$RESTART_BYTES}" \
  >"$OUT/durable-restart-finalize.json"
curl -kfsS -G "$B_BASE/noa/v1/objects/download" \
  -H "Authorization: Bearer $TOKEN" --data-urlencode 'object_key=proof/durable-restart.bin' \
  -o "$OUT/durable-restart-download.bin"
cmp -s "$OUT/part-00000" "$OUT/durable-restart-download.bin"

# The independently committed batch remains readable after coordinator
# restarts; there is no batch session or shared recovery record.
for index in 0 $((BATCH_COUNT - 1)); do
  name="item-$(printf '%03d' "$index").bin"
  curl -kfsS -G "$A_BASE/noa/v1/objects/download" \
    -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "object_key=proof/batch/$name" \
    -o "$OUT/batch-restart-download-$name"
  cmp -s "$BATCH_DIR/$name" "$OUT/batch-restart-download-$name"
done

HZ="$(getconf CLK_TCK)"
PAGESIZE="$(getconf PAGESIZE)"
awk -v before="$BEFORE" -v after="$AFTER" -v hz="$HZ" -v pagesize="$PAGESIZE" 'BEGIN {
  split(before,b," "); split(after,a," ");
  printf "{\"cpu_user_seconds\":%.6f,\"cpu_system_seconds\":%.6f,\"end_rss_bytes\":%.0f,\"kernel_write_bytes\":%.0f}\n",(a[1]-b[1])/hz,(a[2]-b[2])/hz,a[3]*pagesize,a[4]-b[4]
}' >"$OUT/process-metric.json"

SUSPICIOUS="$(grep -Eih 'panic|thread .* panicked|internal server error|fatal' "$A/server.log" "$B/server.log" | wc -l || true)"
jq -n \
  --arg result PASS --arg out "$OUT" --argjson parts "$EXPECTED_PARTS" \
  --argjson parallel "$PARALLEL_PARTS" --argjson suspicious "$SUSPICIOUS" \
  --argjson batch_count "$BATCH_COUNT" --argjson batch_seconds "$BATCH_ELAPSED" \
  --slurpfile standard "$OUT/standard-metric.json" \
  --slurpfile durable "$OUT/durable-metric.json" \
  --slurpfile process "$OUT/process-metric.json" \
  '{result:$result,out:$out,topology:"two local N2 processes, one 1+1 server pool",parts:$parts,parallel_parts:$parallel,standard_batch:{objects:$batch_count,seconds:$batch_seconds,objects_per_second:($batch_count/$batch_seconds),all_success_status:201,mixed_status:207,restart_readback:true},standard:$standard[0],durable:$durable[0],standard_restart_loses_uncommitted_progress:true,durable_restart_restores_progress:true,ncdx_before_finalize:0,ncnx_visibility_before_finalize:false,process:$process[0],suspicious_log_lines:$suspicious}' \
  >"$OUT/summary.json"

cat "$OUT/summary.json"
