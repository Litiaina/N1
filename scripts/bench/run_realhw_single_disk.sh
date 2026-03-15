#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

MOUNT_PATH="${MOUNT_PATH:-/media/administrator/e1f8ebf2-98c1-42e5-b601-d35f0f174e07}"
DEVICE_PATH="${DEVICE_PATH:-/dev/sda}"
FILESYSTEM="${FILESYSTEM:-xfs}"
PORT="${PORT:-50447}"
BASE_URL="${BASE_URL:-https://127.0.0.1:${PORT}}"
BINARY="${BINARY:-$ROOT_DIR/target/release/litiaina-n1-standard}"
MASTER_KEY="${MASTER_KEY:-realhw-master-key}"
JWT_SECRET_VALUE="${JWT_SECRET_VALUE:-realhw-jwt-secret-please-change}"
FRAGMENT_KEY="${FRAGMENT_KEY:-fragment-secret}"
OBJECTS="${OBJECTS:-128}"
CONCURRENCY="${CONCURRENCY:-4}"
OBJECT_SIZE="${OBJECT_SIZE:-65536}"
PREFIX="${PREFIX:-xray/2025/02-02-2026/doctor/files/images/ppacs}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$MOUNT_PATH/n1-http-bench-${STAMP}-realhw}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

for cmd in curl openssl python3 pidstat xargs; do
  require_cmd "$cmd"
done

if [[ ! -x "$BINARY" ]]; then
  echo "binary not found or not executable: $BINARY" >&2
  exit 1
fi

mkdir -p "$OUT_DIR/index" "$OUT_DIR/blob" "$OUT_DIR/fsroot" "$OUT_DIR/tmp"
cd "$OUT_DIR"

FRAGMENT_NAME="realhw-$(date +%s)"
SERVER_PID=""
PIDSTAT_PID=""

cleanup() {
  if [[ -n "$PIDSTAT_PID" ]] && kill -0 "$PIDSTAT_PID" >/dev/null 2>&1; then
    kill "$PIDSTAT_PID" >/dev/null 2>&1 || true
    wait "$PIDSTAT_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -keyout key.pem -out cert.pem -subj "/CN=127.0.0.1" >/dev/null 2>&1

cat > n1.env <<CFG
HOST=127.0.0.1
PORT=$PORT
MASTER_KEY=$MASTER_KEY
JWT_SECRET=$JWT_SECRET_VALUE
RUST_LOG=warn
CFG

cat > n1_config.ini <<CFG
[server]
host=127.0.0.1
https_port=$PORT
cert_path=$OUT_DIR/cert.pem
key_path=$OUT_DIR/key.pem
sharable_url=$BASE_URL

[disk_config]
index_store_disks=$OUT_DIR/index
blob_store_disks=$OUT_DIR/blob
fs_storage_path=$OUT_DIR/fsroot

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=2592000

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting=false
auth_limit_per_second=1000
auth_limit_burst_size=1000
view_limit_per_second=1000
view_limit_burst_size=1000
upload_limit_per_second=1000
upload_limit_burst_size=1000
sharing_limit_per_second=1000
sharing_limit_burst_size=1000
global_limit_per_second=1000
global_limit_burst_size=1000

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
prefix_max_delta_segments=auto
prefix_compact_trigger_segments=auto
prefix_compact_min_interval_seconds=auto
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size=52428800
data_shards=1
parity_shards=0
stripe_block_size=262144

[smb]
enabled=false
bind_addr=0.0.0.0
smb_port=445
max_sessions=4096
max_frame_bytes=16777216
async_close_commit=false
async_close_threshold_bytes=67108864
require_signing=false
query_directory_page_limit=1000
query_directory_max_buffered_entries=4096
query_directory_max_seeded_entries=512
CFG

head -c "$OBJECT_SIZE" /dev/urandom > payload_a.bin
head -c "$OBJECT_SIZE" /dev/urandom > payload_b.bin
: > keys.txt
for i in $(seq 1 "$OBJECTS"); do
  printf "%s/file-%05d.bin\n" "$PREFIX" "$i" >> keys.txt
done

"$BINARY" > server.log 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > server.pid

for _ in $(seq 1 80); do
  if curl -ksS "$BASE_URL/v4/public/config" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
curl -ksS "$BASE_URL/v4/public/config" >/dev/null

ps -p "$SERVER_PID" -o pid=,ppid=,comm=,rss=,%cpu=,cmd= > server_ps.txt
pidstat -h -r -u -p "$SERVER_PID" 1 > pidstat.log 2>&1 &
PIDSTAT_PID=$!

epoch_ms() { date +%s%3N; }

create_payload="$(python3 - <<PY
import json
print(json.dumps({"name":"$FRAGMENT_NAME","secret":"$MASTER_KEY","key":"$FRAGMENT_KEY"}))
PY
)"
create_start="$(epoch_ms)"
create_raw="$(curl -ksS -X POST "$BASE_URL/v4/fragment/create" -H 'Content-Type: application/json' -d "$create_payload")"
printf '%s' "$create_raw" > create_raw.json
fragment_hash="$(printf '%s' "$create_raw" | python3 -c 'import sys,json; print(json.load(sys.stdin)["fragment_hash"])')"
create_end="$(epoch_ms)"

auth_payload="$(python3 - <<PY
import json
print(json.dumps({
  "fragment":"$fragment_hash",
  "secret":"$FRAGMENT_KEY",
  "read": True,
  "write": True,
  "modify": True,
  "delete": True
}))
PY
)"
auth_raw="$(curl -ksS -X POST "$BASE_URL/authenticate/fragment" -H 'Content-Type: application/json' -d "$auth_payload")"
printf '%s' "$auth_raw" > auth_raw.json
access_token="$(printf '%s' "$auth_raw" | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"
AUTH_HEADER="Authorization: Bearer $access_token"

health_before="$(curl -ksS "$BASE_URL/v4/maintenance/health?deep_scan=true" -H "$AUTH_HEADER")"

upload_one() {
  local key="$1"
  curl -ksSf -X POST "$BASE_URL/v4/upload/one-shot/$key" -H "$AUTH_HEADER" --data-binary @payload_a.bin >/dev/null
}

update_one() {
  local key="$1"
  curl -ksSf -X POST "$BASE_URL/v4/upload/one-shot/$key" -H "$AUTH_HEADER" --data-binary @payload_b.bin >/dev/null
}

stream_one() {
  local key="$1"
  curl -ksSf "$BASE_URL/v4/fragment/stream/$key" -H "$AUTH_HEADER" -o /dev/null
}

export -f upload_one update_one stream_one
export BASE_URL AUTH_HEADER

upload_start="$(epoch_ms)"
xargs -a keys.txt -I{} -P "$CONCURRENCY" bash -lc 'upload_one "$@"' _ {}
upload_end="$(epoch_ms)"

list_start="$(epoch_ms)"
list_json="$(curl -ksS --get "$BASE_URL/v4/fragment/objects" -H "$AUTH_HEADER" --data-urlencode "prefix=$PREFIX/" --data-urlencode 'limit=1000')"
list_end="$(epoch_ms)"
list_count="$(printf '%s' "$list_json" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("objects", [])))')"

stream_start="$(epoch_ms)"
xargs -a keys.txt -I{} -P "$CONCURRENCY" bash -lc 'stream_one "$@"' _ {}
stream_end="$(epoch_ms)"

update_start="$(epoch_ms)"
xargs -a keys.txt -I{} -P "$CONCURRENCY" bash -lc 'update_one "$@"' _ {}
update_end="$(epoch_ms)"

sample_key="$(head -n 1 keys.txt)"
meta_start="$(epoch_ms)"
meta_json="$(curl -ksS "$BASE_URL/v4/fragment/meta/$sample_key" -H "$AUTH_HEADER")"
meta_end="$(epoch_ms)"
meta_size="$(printf '%s' "$meta_json" | python3 -c 'import sys,json; print(json.load(sys.stdin)["total_size_bytes"])')"

bulk_delete_payload="$(python3 - <<PY
import json
with open("keys.txt", "r", encoding="utf-8") as f:
    print(json.dumps({"object_keys":[line.strip() for line in f if line.strip()]}))
PY
)"

delete_start="$(epoch_ms)"
bulk_delete_json="$(curl -ksS -X POST "$BASE_URL/v4/fragment/object/bulk-delete" -H "$AUTH_HEADER" -H 'Content-Type: application/json' -d "$bulk_delete_payload")"
delete_end="$(epoch_ms)"
delete_succeeded="$(printf '%s' "$bulk_delete_json" | python3 -c 'import sys,json; print(json.load(sys.stdin)["succeeded"])')"

trash_start="$(epoch_ms)"
trash_json="$(curl -ksS --get "$BASE_URL/v4/fragment/trash" -H "$AUTH_HEADER" --data-urlencode "prefix=$PREFIX/" --data-urlencode 'limit=1000')"
trash_end="$(epoch_ms)"
trash_count="$(printf '%s' "$trash_json" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("objects", [])))')"

recover_start="$(epoch_ms)"
curl -ksSf -X POST "$BASE_URL/v4/fragment/recover/trash/all" -H "$AUTH_HEADER" >/dev/null
recover_end="$(epoch_ms)"

list2_start="$(epoch_ms)"
list2_json="$(curl -ksS --get "$BASE_URL/v4/fragment/objects" -H "$AUTH_HEADER" --data-urlencode "prefix=$PREFIX/" --data-urlencode 'limit=1000')"
list2_end="$(epoch_ms)"
list2_count="$(printf '%s' "$list2_json" | python3 -c 'import sys,json; print(len(json.load(sys.stdin).get("objects", [])))')"

delete2_start="$(epoch_ms)"
curl -ksSf -X POST "$BASE_URL/v4/fragment/object/bulk-delete" -H "$AUTH_HEADER" -H 'Content-Type: application/json' -d "$bulk_delete_payload" >/dev/null
delete2_end="$(epoch_ms)"

empty_start="$(epoch_ms)"
empty_code="$(curl -ksS -o /dev/null -w '%{http_code}' -X DELETE "$BASE_URL/v4/fragment/trash/empty" -H "$AUTH_HEADER")"
empty_end="$(epoch_ms)"

health_after="$(curl -ksS "$BASE_URL/v4/maintenance/health?deep_scan=true" -H "$AUTH_HEADER")"

kill "$PIDSTAT_PID" >/dev/null 2>&1 || true
wait "$PIDSTAT_PID" >/dev/null 2>&1 || true
PIDSTAT_PID=""

python3 - <<PY
import json, os, re

pid = int(open("server.pid", "r", encoding="utf-8").read().strip())
cpu = []
rss = []
with open("pidstat.log", "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        parts = line.split()
        if len(parts) < 14:
            continue
        if not re.match(r"^\\d{2}:\\d{2}:\\d{2}$", parts[0]):
            continue
        if not parts[2].isdigit() or int(parts[2]) != pid:
            continue
        cpu.append(float(parts[7]))
        rss.append(int(parts[12]) / 1024.0)

st = os.statvfs("$MOUNT_PATH")
total = st.f_frsize * st.f_blocks
avail = st.f_frsize * st.f_bavail
used = total - avail

summary = {
    "workdir": "$OUT_DIR",
    "base_url": "$BASE_URL",
    "fragment": "$FRAGMENT_NAME",
    "fragment_hash": "$fragment_hash",
    "objects": int("$OBJECTS"),
    "object_size_bytes": int("$OBJECT_SIZE"),
    "prefix": "$PREFIX/",
    "concurrency": int("$CONCURRENCY"),
    "operations": {
        "fragment_create_ms": int("$create_end") - int("$create_start"),
        "upload_batch_ms": int("$upload_end") - int("$upload_start"),
        "upload_avg_ms": (int("$upload_end") - int("$upload_start")) / int("$OBJECTS"),
        "list_prefix_ms": int("$list_end") - int("$list_start"),
        "list_prefix_count": int("$list_count"),
        "stream_read_batch_ms": int("$stream_end") - int("$stream_start"),
        "stream_read_avg_ms": (int("$stream_end") - int("$stream_start")) / int("$OBJECTS"),
        "update_batch_ms": int("$update_end") - int("$update_start"),
        "update_avg_ms": (int("$update_end") - int("$update_start")) / int("$OBJECTS"),
        "meta_ms": int("$meta_end") - int("$meta_start"),
        "meta_size_bytes": int("$meta_size"),
        "bulk_delete_ms": int("$delete_end") - int("$delete_start"),
        "bulk_delete_succeeded": int("$delete_succeeded"),
        "trash_list_ms": int("$trash_end") - int("$trash_start"),
        "trash_list_count": int("$trash_count"),
        "recover_all_ms": int("$recover_end") - int("$recover_start"),
        "list_after_recover_ms": int("$list2_end") - int("$list2_start"),
        "list_after_recover_count": int("$list2_count"),
        "bulk_delete_round2_ms": int("$delete2_end") - int("$delete2_start"),
        "empty_trash_ms": int("$empty_end") - int("$empty_start"),
        "empty_trash_http": "$empty_code",
    },
    "server_monitor": {
        "samples": len(cpu),
        "avg_cpu_percent": round(sum(cpu) / len(cpu), 2) if cpu else None,
        "peak_cpu_percent": round(max(cpu), 2) if cpu else None,
        "avg_rss_mib": round(sum(rss) / len(rss), 2) if rss else None,
        "peak_rss_mib": round(max(rss), 2) if rss else None,
    },
    "disk": {
        "mount": "$MOUNT_PATH",
        "device": "$DEVICE_PATH",
        "filesystem": "$FILESYSTEM",
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": avail,
        "use_percent": round((used / total) * 100, 2) if total else 0.0,
    },
    "health_before": json.loads("""$health_before"""),
    "health_after": json.loads("""$health_after"""),
}

with open("summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)

print(json.dumps(summary, indent=2))
PY
