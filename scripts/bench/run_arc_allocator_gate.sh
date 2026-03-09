#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

MODE="${MODE:-quick}" # quick | full
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-.n1-bench/arc-allocator-gate-${STAMP}}"
mkdir -p "$OUT_DIR"

CSV="$OUT_DIR/summary.csv"
SUMMARY="$OUT_DIR/summary.txt"

# Optional toggles.
RUN_BLOCK_MANAGER="${RUN_BLOCK_MANAGER:-1}"
RUN_ARC_UNIT="${RUN_ARC_UNIT:-1}"
RUN_RELEASE_DRILLS="${RUN_RELEASE_DRILLS:-1}"
RUN_HA_CHAOS="${RUN_HA_CHAOS:-}"

RELEASE_RUNS="${RELEASE_RUNS:-1}"
CHAOS_RUNS="${CHAOS_RUNS:-1}"
INDEX_SOAK="${INDEX_SOAK:-0}"

if [[ -z "$RUN_HA_CHAOS" ]]; then
  if [[ "$MODE" == "full" ]]; then
    RUN_HA_CHAOS="1"
  else
    RUN_HA_CHAOS="0"
  fi
fi

if [[ "$MODE" != "quick" && "$MODE" != "full" ]]; then
  echo "invalid MODE=$MODE (expected quick|full)" >&2
  exit 2
fi

echo "step,exit_code,wall_seconds,passed" > "$CSV"

run_step() {
  local step="$1"
  shift
  local cmd="$*"
  local log="$OUT_DIR/${step}.log"

  echo "[gate] step=$step"
  echo "[gate] cmd=$cmd" > "$log"

  local start_ms end_ms wall_ms wall ec passed
  start_ms="$(date +%s%3N)"
  set +e
  bash -lc "$cmd" >>"$log" 2>&1
  ec=$?
  set -e
  end_ms="$(date +%s%3N)"
  wall_ms=$(( end_ms - start_ms ))
  wall="$(awk "BEGIN { printf \"%.3f\", ${wall_ms}/1000 }")"

  passed=0
  if [[ "$ec" -eq 0 ]]; then
    passed=1
  fi
  echo "${step},${ec},${wall},${passed}" >> "$CSV"
  echo "[gate] step=$step exit=$ec wall=${wall}s"
}

if [[ "$RUN_BLOCK_MANAGER" == "1" ]]; then
  run_step "block_manager_tests" "cargo test -p block_manager"
fi

if [[ "$RUN_ARC_UNIT" == "1" ]]; then
  run_step "arc_formatter_nonce_roundtrip" \
    "cargo test -p arc n1_core::blob_store::tests::format_raw_blob_device_intent_prepare_and_apply_roundtrip -- --nocapture"
  run_step "arc_extent_gen_legacy_default" \
    "cargo test -p arc n1_core::blob_store::tests::raw_blob_address_defaults_extent_generation_for_legacy_metadata -- --nocapture"
fi

if [[ "$RUN_RELEASE_DRILLS" == "1" ]]; then
  run_step "arc_release_drills" \
    "PACKAGE=arc RUNS=${RELEASE_RUNS} RUN_CHAOS=0 RUN_INDEX_OWNER_RESUME_SOAK=${INDEX_SOAK} scripts/bench/run_release_drills.sh"
fi

if [[ "$RUN_HA_CHAOS" == "1" ]]; then
  run_step "arc_ha_chaos" \
    "PACKAGE=arc RUNS=${CHAOS_RUNS} REQUIRE_ALL_PASS=1 scripts/bench/run_ha_chaos.sh"
fi

read -r pass_count fail_count avg_wall max_wall <<<"$(awk -F, '
  NR==1 { next }
  {
    n++
    sum += $3 + 0
    if ($4 == "1") pass++
    else fail++
    if (n == 1 || $3 + 0 > max) max = $3 + 0
  }
  END {
    if (n == 0) { print "0 0 0 0"; exit }
    printf "%d %d %.3f %.3f\n", pass + 0, fail + 0, sum / n, max + 0
  }' "$CSV")"

status="PASS"
if [[ "$fail_count" -gt 0 ]]; then
  status="FAIL"
fi

{
  echo "status=$status"
  echo "mode=$MODE"
  echo "steps_pass=$pass_count steps_fail=$fail_count"
  echo "wall_seconds:avg=$avg_wall max=$max_wall"
  echo "csv=$CSV"
  echo "logs=$OUT_DIR"
} | tee "$SUMMARY"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi

