#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SOURCE_FILE="${SOURCE_FILE:-/home/administrator/Downloads/INCDT 122225.mp4}"
DISK_ROOTS="${DISK_ROOTS:-/mnt/disk-00,/mnt/disk-01}"
PORT="${PORT:-50671}"
PREFIX="${PREFIX:-release-proof/}"
MAX_PART_SIZE="${MAX_PART_SIZE:-52428800}"
MASTER_KEY="${MASTER_KEY:-n1-release-proof-master}"
FRAGMENT_KEY="${FRAGMENT_KEY:-n1-release-proof-fragment}"
PROFILE="${PROFILE:-release}"
BINARY="${BINARY:-$ROOT_DIR/target/$PROFILE/litiaina-n1}"
LARGE_COPIES="${LARGE_COPIES:-2}"
STORM_OBJECTS="${STORM_OBJECTS:-10000}"
STORM_CONCURRENCY="${STORM_CONCURRENCY:-24}"
STORM_OBJECT_SIZE="${STORM_OBJECT_SIZE:-16384}"
MIXED_USERS="${MIXED_USERS:-4}"
MIXED_FILES_PER_USER="${MIXED_FILES_PER_USER:-1000}"
MIXED_BACKGROUND_UPLOADS="${MIXED_BACKGROUND_UPLOADS:-1000}"
MULTI_FRAGMENT_USERS="${MULTI_FRAGMENT_USERS:-4}"
MULTI_FRAGMENT_FILES_PER_USER="${MULTI_FRAGMENT_FILES_PER_USER:-250}"
CHAOS="${CHAOS:-0}"
CHAOS_SECONDS="${CHAOS_SECONDS:-0}"
CHAOS_CYCLES="${CHAOS_CYCLES:-0}"
CHAOS_STORM_OBJECTS="${CHAOS_STORM_OBJECTS:-1000}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-10800}"
STAMP="$(date +%Y%m%d-%H%M%S)"
PROOF_ROOT="${PROOF_ROOT:-/tmp/litiaina-n1-release-proof}"
OUT_DIR="${OUT_DIR:-$PROOF_ROOT/n1-release-proof-${STAMP}}"

command -v cargo >/dev/null 2>&1 || {
  echo "missing required command: cargo" >&2
  exit 1
}

if [[ ! -f "$SOURCE_FILE" ]]; then
  echo "source file does not exist: $SOURCE_FILE" >&2
  exit 1
fi

if [[ "$PROFILE" == "release" ]]; then
  cargo build --release -p n1 --bin litiaina-n1
  cargo_args=(run --release -p n1-release-proof --bin n1-release-proof --)
else
  cargo build -p n1 --bin litiaina-n1
  cargo_args=(run -p n1-release-proof --bin n1-release-proof --)
fi

args=(
  --binary "$BINARY" \
  --source-file "$SOURCE_FILE" \
  --disk-roots "$DISK_ROOTS" \
  --out-dir "$OUT_DIR" \
  --port "$PORT" \
  --prefix "$PREFIX" \
  --max-part-size "$MAX_PART_SIZE" \
  --master-key "$MASTER_KEY" \
  --fragment-key "$FRAGMENT_KEY" \
  --large-copies "$LARGE_COPIES" \
  --storm-objects "$STORM_OBJECTS" \
  --storm-concurrency "$STORM_CONCURRENCY" \
  --storm-object-size "$STORM_OBJECT_SIZE" \
  --mixed-users "$MIXED_USERS" \
  --mixed-files-per-user "$MIXED_FILES_PER_USER" \
  --mixed-background-uploads "$MIXED_BACKGROUND_UPLOADS" \
  --multi-fragment-users "$MULTI_FRAGMENT_USERS" \
  --multi-fragment-files-per-user "$MULTI_FRAGMENT_FILES_PER_USER" \
  --job-timeout-seconds "$JOB_TIMEOUT_SECONDS" \
  --chaos-seconds "$CHAOS_SECONDS" \
  --chaos-cycles "$CHAOS_CYCLES" \
  --chaos-storm-objects "$CHAOS_STORM_OBJECTS"
)

if [[ "$CHAOS" == "1" || "$CHAOS" == "true" ]]; then
  args+=(--chaos)
fi

cargo "${cargo_args[@]}" "${args[@]}"
