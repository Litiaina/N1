#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SOURCE_DIR="${SOURCE_DIR:-/home/administrator/Development/litiaina-cloud-web}"
if [[ ! -d "$SOURCE_DIR" ]]; then
  SOURCE_DIR="$ROOT_DIR"
fi

DISK_ROOTS="${DISK_ROOTS:-/mnt/disk-00,/mnt/disk-01}"
PORT="${PORT:-50491}"
CONCURRENCY="${CONCURRENCY:-16}"
PROGRESS_EVERY="${PROGRESS_EVERY:-500}"
MAX_FILES="${MAX_FILES:-0}"
PREFIX="${PREFIX:-proof/}"
QUOTA_BYTES="${QUOTA_BYTES:-10737418240}"
QUOTA_BACKEND="${QUOTA_BACKEND:-xfs_project}"
MASTER_KEY="${MASTER_KEY:-real-proof-master-key}"
FRAGMENT_KEY="${FRAGMENT_KEY:-real-proof-fragment-key}"
FRAGMENT_NAME="${FRAGMENT_NAME:-}"
SIMULATE_DISK_FORMAT_REPAIR="${SIMULATE_DISK_FORMAT_REPAIR:-0}"
REPAIR_TIMEOUT_SECONDS="${REPAIR_TIMEOUT_SECONDS:-1800}"
BUILD="${BUILD:-1}"
BINARY="${BINARY:-$ROOT_DIR/target/debug/litiaina-n1}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.n1-bench/real-restart-proof-${STAMP}}"
CLEANUP_ROOTS="${CLEANUP_ROOTS:-0}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd python3
require_cmd openssl

if [[ "$QUOTA_BACKEND" == "xfs_project" && "${EUID:-$(id -u)}" -ne 0 ]]; then
  if [[ "$BUILD" == "1" ]]; then
    require_cmd cargo
    cargo build -p n1 --bin litiaina-n1
  fi
  echo "[proof] xfs_project requires quota privileges; re-running through sudo" >&2
  exec sudo -E BUILD=0 BINARY="$BINARY" "$0" "$@"
fi

if [[ "$BUILD" == "1" ]]; then
  require_cmd cargo
  cargo build -p n1 --bin litiaina-n1
fi

cleanup_flag=()
if [[ "$CLEANUP_ROOTS" == "1" ]]; then
  cleanup_flag=(--cleanup-roots)
fi

disk_format_repair_flag=()
if [[ "$SIMULATE_DISK_FORMAT_REPAIR" == "1" ]]; then
  disk_format_repair_flag=(--simulate-disk-format-repair)
fi

fragment_name_arg=()
if [[ -n "$FRAGMENT_NAME" ]]; then
  fragment_name_arg=(--fragment-name "$FRAGMENT_NAME")
fi

  python3 "$ROOT_DIR/scripts/bench/n1_real_proof.py" \
  --binary "$BINARY" \
  --source-dir "$SOURCE_DIR" \
  --disk-roots "$DISK_ROOTS" \
  --out-dir "$OUT_DIR" \
  --port "$PORT" \
  --concurrency "$CONCURRENCY" \
  --progress-every "$PROGRESS_EVERY" \
  --max-files "$MAX_FILES" \
  --prefix "$PREFIX" \
  --quota-bytes "$QUOTA_BYTES" \
  --quota-backend "$QUOTA_BACKEND" \
  --master-key "$MASTER_KEY" \
  --fragment-key "$FRAGMENT_KEY" \
  --repair-timeout-seconds "$REPAIR_TIMEOUT_SECONDS" \
  "${fragment_name_arg[@]}" \
  "${disk_format_repair_flag[@]}" \
  "${cleanup_flag[@]}"
