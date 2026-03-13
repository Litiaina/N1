#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  format_arc_raw_disks.sh [options] --device /dev/disk/by-id/wwn-... [--device /dev/disk/by-id/wwn-...]
  format_arc_raw_disks.sh [options] -- /dev/disk/by-id/wwn-... /dev/disk/by-id/wwn-...

Formats one or more N1 Arc raw devices by automatically running:
  1. format-*-device --prepare
  2. format-*-device --nonce ... --fingerprint ...

Binary resolution order:
  1. --arc-bin PATH
  2. ARC_BIN environment variable
  3. litiaina-n1-arc in the same directory as this script
  4. litiaina-n1-arc from PATH

Options:
  --arc-bin PATH     Use a specific Arc binary
  --device PATH      Add a device path (repeatable)
  --blob             Use format-blob-device (default)
  --index            Use format-index-device
  --force            Pass --force to the Arc formatter
  --json             Print final per-device statuses as a JSON array
  -h, --help         Show this help

Examples:
  ./format_arc_raw_disks.sh --force \
    --device /dev/disk/by-id/wwn-0x5000c500c80a6f31 \
    --device /dev/disk/by-id/wwn-0x5000039a92685a95

  ./format_arc_raw_disks.sh --index -- /dev/disk/by-id/wwn-0x123 /dev/disk/by-id/wwn-0x456
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

resolve_arc_bin() {
  local script_dir candidate
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

  if [[ -n "${ARC_BIN:-}" ]]; then
    [[ -x "$ARC_BIN" ]] || die "Arc binary is not executable: $ARC_BIN"
    printf '%s\n' "$ARC_BIN"
    return 0
  fi

  candidate="$script_dir/litiaina-n1-arc"
  if [[ -x "$candidate" ]]; then
    printf '%s\n' "$candidate"
    return 0
  fi

  if command -v litiaina-n1-arc >/dev/null 2>&1; then
    command -v litiaina-n1-arc
    return 0
  fi

  die "could not find litiaina-n1-arc; use --arc-bin or place the binary beside this script"
}

json_get() {
  local key="$1"
  python3 - "$key" <<'PY'
import json
import sys

key = sys.argv[1]
payload = json.load(sys.stdin)
value = payload[key]
if isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

ARC_BIN="${ARC_BIN:-}"
FORMAT_COMMAND="format-blob-device"
FORCE=0
JSON_OUTPUT=0
declare -a DEVICES=()

while (($# > 0)); do
  case "$1" in
    --arc-bin)
      (($# >= 2)) || die "--arc-bin requires a value"
      ARC_BIN="$2"
      shift 2
      ;;
    --device)
      (($# >= 2)) || die "--device requires a value"
      DEVICES+=("$2")
      shift 2
      ;;
    --blob)
      FORMAT_COMMAND="format-blob-device"
      shift
      ;;
    --index)
      FORMAT_COMMAND="format-index-device"
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --json)
      JSON_OUTPUT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while (($# > 0)); do
        DEVICES+=("$1")
        shift
      done
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      DEVICES+=("$1")
      shift
      ;;
  esac
done

((${#DEVICES[@]} > 0)) || {
  usage >&2
  die "no devices provided"
}

require_cmd python3
ARC_BIN="$(resolve_arc_bin)"

declare -A SEEN=()
declare -a UNIQUE_DEVICES=()
for device in "${DEVICES[@]}"; do
  [[ -e "$device" ]] || die "device does not exist: $device"
  canonical="$(readlink -f -- "$device" 2>/dev/null || printf '%s' "$device")"
  if [[ -n "${SEEN[$canonical]:-}" ]]; then
    continue
  fi
  SEEN["$canonical"]=1
  UNIQUE_DEVICES+=("$device")
done

tmp_json="$(mktemp)"
cleanup() {
  rm -f -- "$tmp_json"
}
trap cleanup EXIT
printf '[]\n' >"$tmp_json"

force_args=()
if ((FORCE)); then
  force_args+=(--force)
fi

for device in "${UNIQUE_DEVICES[@]}"; do
  printf 'Preparing %s with %s\n' "$device" "$FORMAT_COMMAND" >&2
  prepare_json="$("$ARC_BIN" --json "$FORMAT_COMMAND" --path "$device" "${force_args[@]}" --prepare)"
  nonce="$(printf '%s\n' "$prepare_json" | json_get nonce)"
  fingerprint="$(printf '%s\n' "$prepare_json" | json_get fingerprint)"

  printf 'Formatting %s\n' "$device" >&2
  status_json="$("$ARC_BIN" --json "$FORMAT_COMMAND" --path "$device" "${force_args[@]}" --nonce "$nonce" --fingerprint "$fingerprint")"

  python3 - "$tmp_json" <<'PY' <<<"$status_json"
import json
import sys

store_path = sys.argv[1]
status = json.load(sys.stdin)
with open(store_path, "r", encoding="utf-8") as fh:
    items = json.load(fh)
items.append(status)
with open(store_path, "w", encoding="utf-8") as fh:
    json.dump(items, fh, indent=2)
PY
done

if ((JSON_OUTPUT)); then
  cat "$tmp_json"
else
  python3 - "$tmp_json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    items = json.load(fh)

print("Formatted devices:")
for item in items:
    print(f"  path: {item.get('path')}")
    print(f"  canonical_path: {item.get('canonical_path')}")
    print(f"  status: {item.get('status')}")
    print(f"  device_id: {item.get('device_id') or '<none>'}")
    print(f"  block_size_bytes: {item.get('block_size_bytes')}")
    print(f"  total_bytes: {item.get('total_bytes')}")
    print(f"  available_bytes: {item.get('available_bytes')}")
    warning = item.get("path_warning")
    if warning:
        print(f"  warning: {warning}")
PY
fi
