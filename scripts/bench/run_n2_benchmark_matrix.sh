#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${OUT:-$ROOT_DIR/.n2-bench/benchmark-matrix-$(date +%Y%m%d-%H%M%S)}"

DIRECT_BASE="${DIRECT_BASE:-https://127.0.0.1:50001}"
GATEWAY_BASE="${GATEWAY_BASE:-https://127.0.0.1:51002}"
NATIVE_URL="${NATIVE_URL:-n2://127.0.0.1:50002}"
FRAGMENT="${FRAGMENT:-}"
SECRET="${SECRET:-}"
SMALL_MIB="${SMALL_MIB:-8}"
LARGE_MIB="${LARGE_MIB:-128}"
PART_MIB="${PART_MIB:-64}"
LATENCY_SAMPLES="${LATENCY_SAMPLES:-40}"

mkdir -p "$OUT"

if [[ -z "$FRAGMENT" || -z "$SECRET" ]]; then
  echo "FRAGMENT and SECRET are required" >&2
  exit 2
fi

auth() {
  local base="$1" out="$2"
  curl -sk --http1.1 -fsS -X POST "$base/noa/v1/auth/fragment" \
    -H 'Content-Type: application/json' \
    -d "{\"fragment\":\"$FRAGMENT\",\"secret\":\"$SECRET\",\"read\":true,\"write\":true,\"modify\":true,\"delete\":true,\"expires_in\":21600}" \
    >"$out"
}

TOKEN_DIRECT=""
TOKEN_GATEWAY=""

auth "$DIRECT_BASE" "$OUT/auth-direct.json"
TOKEN_DIRECT="$(jq -r '.access_token' "$OUT/auth-direct.json")"
auth "$GATEWAY_BASE" "$OUT/auth-gateway.json"
TOKEN_GATEWAY="$(jq -r '.access_token' "$OUT/auth-gateway.json")"

dd if=/dev/urandom of="$OUT/small.bin" bs=1M count="$SMALL_MIB" status=none
dd if=/dev/urandom of="$OUT/large.bin" bs=1M count="$LARGE_MIB" status=none
sha256sum "$OUT/small.bin" "$OUT/large.bin" >"$OUT/source.sha256"

metric() {
  local bytes="$1" seconds="$2"
  awk -v b="$bytes" -v s="$seconds" 'BEGIN{if(s<=0)s=0.001; printf "%.2f", (b/1048576)/s}'
}

curl_upload() {
  local label="$1" base="$2" token="$3" key="$4" file="$5"
  local bytes start status seconds speed
  bytes="$(stat -c%s "$file")"
  start="$(date +%s%N)"
  status="$(curl -sk --http1.1 -o "$OUT/$label-upload.json" -w '%{http_code}' \
    -X POST "$base/noa/v1/upload/one-shot/$key" \
    -H "Authorization: Bearer $token" \
    --data-binary @"$file")"
  seconds="$(awk -v ns="$(( $(date +%s%N) - start ))" 'BEGIN{printf "%.6f", ns/1000000000}')"
  speed="$(metric "$bytes" "$seconds")"
  jq -n --arg label "$label" --arg status "$status" --argjson bytes "$bytes" --argjson seconds "$seconds" --argjson mib_per_sec "$speed" \
    '{label:$label,status:$status,bytes:$bytes,seconds:$seconds,mib_per_sec:$mib_per_sec}' \
    >"$OUT/$label-upload-metric.json"
}

curl_download() {
  local label="$1" base="$2" token="$3" key="$4" expected="$5"
  local bytes start status seconds speed
  start="$(date +%s%N)"
  status="$(curl -sk --http1.1 -o "$OUT/$label-download.bin" -w '%{http_code}' \
    -G "$base/noa/v1/objects/download" \
    -H "Authorization: Bearer $token" \
    --data-urlencode "object_key=$key")"
  seconds="$(awk -v ns="$(( $(date +%s%N) - start ))" 'BEGIN{printf "%.6f", ns/1000000000}')"
  bytes="$(stat -c%s "$OUT/$label-download.bin")"
  speed="$(metric "$bytes" "$seconds")"
  cmp -s "$expected" "$OUT/$label-download.bin"
  jq -n --arg label "$label" --arg status "$status" --argjson bytes "$bytes" --argjson seconds "$seconds" --argjson mib_per_sec "$speed" \
    '{label:$label,status:$status,bytes:$bytes,seconds:$seconds,mib_per_sec:$mib_per_sec,sha_match:true}' \
    >"$OUT/$label-download-metric.json"
}

multipart_gateway() {
  local key="bench/gateway-large-$(date +%s).bin"
  local file="$OUT/large.bin"
  local bytes part_bytes expected version session coordinator start seconds speed
  bytes="$(stat -c%s "$file")"
  part_bytes=$(( PART_MIB * 1024 * 1024 ))
  expected=$(( (bytes + part_bytes - 1) / part_bytes ))
  split -b "$part_bytes" -d -a 4 "$file" "$OUT/gw-part-"
  curl -sk --http1.1 -fsS -X POST "$GATEWAY_BASE/noa/v1/upload/multipart/init/$key" \
    -H "Authorization: Bearer $TOKEN_GATEWAY" \
    -H 'Content-Type: application/json' \
    -d "{\"total_size_bytes\":$bytes,\"expected_parts\":$expected,\"part_size_bytes\":$part_bytes,\"content_type\":\"application/octet-stream\"}" \
    >"$OUT/gateway-multipart-init.json"
  version="$(jq -r '.version_id' "$OUT/gateway-multipart-init.json")"
  session="$(jq -r '.session_id' "$OUT/gateway-multipart-init.json")"
  coordinator="$(jq -r '.coordinator_node_id // .gateway.coordinator_node_id' "$OUT/gateway-multipart-init.json")"
  start="$(date +%s%N)"
  for ((i=0; i<expected; i++)); do
    curl -sk --http1.1 -fsS -X PUT "$GATEWAY_BASE/noa/v1/upload/multipart/$version/$i?coordinator_node_id=$coordinator" \
      -H "Authorization: Bearer $TOKEN_GATEWAY" \
      --data-binary @"$OUT/gw-part-$(printf '%04d' "$i")" >"$OUT/gateway-multipart-part-$i.json"
  done
  curl -sk --http1.1 -fsS -X POST "$GATEWAY_BASE/noa/v1/upload/multipart/$version/$session/finalize?object_key=$key&coordinator_node_id=$coordinator" \
    -H "Authorization: Bearer $TOKEN_GATEWAY" \
    -H 'Content-Type: application/json' \
    -d "{\"total_size_bytes\":$bytes,\"expected_parts\":$expected,\"part_size_bytes\":$part_bytes,\"content_type\":\"application/octet-stream\"}" \
    >"$OUT/gateway-multipart-finalize.json"
  seconds="$(awk -v ns="$(( $(date +%s%N) - start ))" 'BEGIN{printf "%.6f", ns/1000000000}')"
  speed="$(metric "$bytes" "$seconds")"
  jq -n --arg coordinator "$coordinator" --arg key "$key" --argjson bytes "$bytes" --argjson seconds "$seconds" --argjson mib_per_sec "$speed" \
    '{label:"gateway_multipart",key:$key,coordinator_node_id:$coordinator,bytes:$bytes,seconds:$seconds,mib_per_sec:$mib_per_sec}' \
    >"$OUT/gateway-multipart-metric.json"
  curl_download gateway-multipart "$GATEWAY_BASE" "$TOKEN_GATEWAY" "$key" "$file"
}

native_upload_download() {
  local key="bench/native-small-$(date +%s).bin"
  "$ROOT_DIR/target/release/litiaina-n2-shell" \
    --native-url "$NATIVE_URL" --fragment "$FRAGMENT" --secret "$SECRET" \
    upload --object-key "$key" --file "$OUT/small.bin" --sha256 --json \
    >"$OUT/native-upload.json"
  "$ROOT_DIR/target/release/litiaina-n2-shell" \
    --native-url "$NATIVE_URL" --fragment "$FRAGMENT" --secret "$SECRET" \
    download --object-key "$key" --file "$OUT/native-download.bin" \
    >"$OUT/native-download.log" 2>&1
  cmp -s "$OUT/small.bin" "$OUT/native-download.bin"
  jq -n --arg key "$key" '{label:"native_shell",key:$key,sha_match:true}' >"$OUT/native-metric.json"
}

latency_probe() {
  local label="$1" base="$2" token="$3"
  local out="$OUT/$label-latency.csv"
  : >"$out"
  for _ in $(seq 1 "$LATENCY_SAMPLES"); do
    curl -sk --http1.1 -o /dev/null -w '%{time_total}\n' \
      -G "$base/noa/v1/posix/list" \
      -H "Authorization: Bearer $token" \
      --data-urlencode 'path=bench' \
      --data-urlencode 'limit=20' >>"$out"
  done
  sort -n "$out" >"$OUT/$label-latency-sorted.csv"
  awk -v label="$label" '
    {a[++n]=$1}
    END {
      if(n==0){print "{}"; exit}
      p50=a[int((n*0.50)+0.999)]
      p95=a[int((n*0.95)+0.999)]
      p99=a[int((n*0.99)+0.999)]
      printf "{\"label\":\"%s\",\"samples\":%d,\"p50_seconds\":%.6f,\"p95_seconds\":%.6f,\"p99_seconds\":%.6f}\n", label,n,p50,p95,p99
    }' "$OUT/$label-latency-sorted.csv" >"$OUT/$label-latency.json"
}

curl_upload direct-one-shot "$DIRECT_BASE" "$TOKEN_DIRECT" "bench/direct-small-$(date +%s).bin" "$OUT/small.bin"
curl_upload gateway-one-shot "$GATEWAY_BASE" "$TOKEN_GATEWAY" "bench/gateway-small-$(date +%s).bin" "$OUT/small.bin"
multipart_gateway
native_upload_download
latency_probe direct "$DIRECT_BASE" "$TOKEN_DIRECT"
latency_probe gateway "$GATEWAY_BASE" "$TOKEN_GATEWAY"

jq -n \
  --arg out "$OUT" \
  --slurpfile direct_upload "$OUT/direct-one-shot-upload-metric.json" \
  --slurpfile gateway_upload "$OUT/gateway-one-shot-upload-metric.json" \
  --slurpfile gateway_multipart "$OUT/gateway-multipart-metric.json" \
  --slurpfile gateway_multipart_download "$OUT/gateway-multipart-download-metric.json" \
  --slurpfile native "$OUT/native-metric.json" \
  --slurpfile direct_latency "$OUT/direct-latency.json" \
  --slurpfile gateway_latency "$OUT/gateway-latency.json" \
  '{out:$out,direct_one_shot:$direct_upload[0],gateway_one_shot:$gateway_upload[0],gateway_multipart:$gateway_multipart[0],gateway_multipart_download:$gateway_multipart_download[0],native:$native[0],direct_latency:$direct_latency[0],gateway_latency:$gateway_latency[0]}' \
  | tee "$OUT/summary.json"

echo "N2_BENCHMARK_MATRIX_OUT=$OUT"
