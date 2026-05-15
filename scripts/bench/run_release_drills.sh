#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PACKAGE="${PACKAGE:-n1}"
RUNS="${RUNS:-1}"
TEST_ARGS="${TEST_ARGS:---nocapture}"
RUN_CHAOS="${RUN_CHAOS:-0}"
CHAOS_RUNS="${CHAOS_RUNS:-3}"
CHAOS_REQUIRE_ALL_PASS="${CHAOS_REQUIRE_ALL_PASS:-1}"
CHAOS_MAX_DURATION_SECS="${CHAOS_MAX_DURATION_SECS:-0}"
RUN_INDEX_OWNER_RESUME_SOAK="${RUN_INDEX_OWNER_RESUME_SOAK:-0}"
INDEX_OWNER_RESUME_SOAK_ROUNDS="${INDEX_OWNER_RESUME_SOAK_ROUNDS:-5}"
RUN_REAL_RESTART_PROOF="${RUN_REAL_RESTART_PROOF:-0}"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-.n1-bench/release-drills-${STAMP}}"
mkdir -p "$OUT_DIR"

CSV="$OUT_DIR/summary.csv"
SUMMARY="$OUT_DIR/summary.txt"

echo "test,run,exit_code,wall_seconds,passed" > "$CSV"

TESTS=(
  n1_file_rename_preserves_object_id_and_version_lineage
  n1_folder_move_keeps_symlink_object_pointers_valid
  n1_posix_pagination_uses_display_names_not_raw_object_dirs
  n1_soft_delete_moves_symlink_to_trash_and_purge_removes_unreferenced_object_index
  n1_trash_list_shows_only_explicit_file_delete_anchor
  n1_trash_list_shows_only_explicit_folder_delete_anchor
  repair_fragment_replicas_chooses_rich_index_source_and_preserves_blobs
  repair_fragment_replicas_ignores_newer_fragment_meta_on_stale_replica
  repair_fragment_replicas_prefers_richer_replica_even_when_stale_plane_is_newer
  stale_index_on_blob_only_root_is_cleaned_without_touching_blobs
)

run_case() {
  local test_name="$1"
  local run_id="$2"
  local safe_name
  safe_name="$(echo "$test_name" | tr '/ ' '__')"
  local log="$OUT_DIR/${safe_name}.run${run_id}.log"

  echo "[drill] test=${test_name} run=${run_id}/${RUNS}"
  local start_ms end_ms wall_ms wall ec passed
  start_ms="$(date +%s%3N)"
  set +e
  cargo test -p "$PACKAGE" "$test_name" -- $TEST_ARGS >"$log" 2>&1
  ec=$?
  set -e
  end_ms="$(date +%s%3N)"
  wall_ms=$(( end_ms - start_ms ))
  wall="$(awk "BEGIN { printf \"%.3f\", ${wall_ms}/1000 }")"
  passed=0
  if [[ "$ec" -eq 0 ]] && grep -q "test result: ok" "$log"; then
    passed=1
  fi
  echo "${test_name},${run_id},${ec},${wall},${passed}" >> "$CSV"
}

for test_name in "${TESTS[@]}"; do
  for run_id in $(seq 1 "$RUNS"); do
    run_case "$test_name" "$run_id"
  done
done

index_resume_soak_status="skipped"
if [[ "$RUN_INDEX_OWNER_RESUME_SOAK" == "1" ]]; then
  echo "[drill] running owner-first multipart resume soak"
  set +e
  N1_INDEX_OWNER_RESUME_SOAK_ROUNDS="$INDEX_OWNER_RESUME_SOAK_ROUNDS" \
  cargo test -p "$PACKAGE" multipart_receipts_are_zero_decode_filename_records -- --nocapture \
    >"$OUT_DIR/index-owner-resume-soak.log" 2>&1
  index_resume_soak_ec=$?
  set -e
  if [[ "$index_resume_soak_ec" -eq 0 ]]; then
    index_resume_soak_status="pass"
  else
    index_resume_soak_status="fail"
  fi
fi

chaos_status="skipped"
if [[ "$RUN_CHAOS" == "1" ]]; then
  echo "[drill] running HA chaos harness"
  set +e
  OUT_DIR="$OUT_DIR/ha-chaos" \
  RUNS="$CHAOS_RUNS" \
  REQUIRE_ALL_PASS="$CHAOS_REQUIRE_ALL_PASS" \
  MAX_DURATION_SECS="$CHAOS_MAX_DURATION_SECS" \
  scripts/bench/run_ha_chaos.sh >"$OUT_DIR/ha-chaos.stdout.log" 2>&1
  chaos_ec=$?
  set -e
  if [[ "$chaos_ec" -eq 0 ]]; then
    chaos_status="pass"
  else
    chaos_status="fail"
  fi
fi

real_restart_status="skipped"
if [[ "$RUN_REAL_RESTART_PROOF" == "1" ]]; then
  echo "[drill] running real N1 release proof"
  set +e
  OUT_DIR="$OUT_DIR/real-restart-proof" \
    scripts/bench/run_n1_release_proof.sh >"$OUT_DIR/real-restart-proof.stdout.log" 2>&1
  real_restart_ec=$?
  set -e
  if [[ "$real_restart_ec" -eq 0 ]]; then
    real_restart_status="pass"
  else
    real_restart_status="fail"
  fi
fi

read -r pass_count fail_count avg_wall max_wall <<<"$(awk -F, '
  NR==1 { next }
  {
    n++
    sum+=$4+0
    if ($5 == "1") pass++
    else fail++
    if (n == 1 || $4+0 > max) max=$4+0
  }
  END {
    if (n == 0) { print "0 0 0 0"; exit }
    printf "%d %d %.3f %.3f\n", pass+0, fail+0, sum/n, max+0
  }' "$CSV")"

status="PASS"
if [[ "$fail_count" -gt 0 || "$chaos_status" == "fail" || "$index_resume_soak_status" == "fail" || "$real_restart_status" == "fail" ]]; then
  status="FAIL"
fi

{
  echo "status=$status"
  echo "case_runs=$(( ${#TESTS[@]} * RUNS )) pass=$pass_count fail=$fail_count"
  echo "wall_seconds:avg=$avg_wall max=$max_wall"
  echo "index_owner_resume_soak=$index_resume_soak_status"
  echo "chaos=$chaos_status"
  echo "real_restart_proof=$real_restart_status"
  echo "csv=$CSV"
} | tee "$SUMMARY"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
