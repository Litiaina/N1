#!/usr/bin/env bash
set -Eeuo pipefail

# Unattended, server-only N2 3.3 qualification controller. The caller supplies
# one immutable run ID and a controller directory containing the exact N2
# binary and stage scripts. Progress is written atomically to status.json;
# no external poller is required.

: "${N2_SEQUENCE_RUN_ID:?set N2_SEQUENCE_RUN_ID}"
: "${N2_SEQUENCE_BASE:?set N2_SEQUENCE_BASE}"

RUN_ID="$N2_SEQUENCE_RUN_ID"
BASE="$N2_SEQUENCE_BASE"
CONTROLLER="$BASE/controller"
BIN="$CONTROLLER/litiaina-n2"
MINIMUM="$CONTROLLER/run_n2_3_3_minimum_proof.sh"
STANDARD="$CONTROLLER/run_n2_3_0_release_proof.sh"
MULTI="$CONTROLLER/run_n2_3_3_multi_pool_proof.sh"
SOAK="$CONTROLLER/run_n2_3_0_soak_chaos.sh"
STATUS="$BASE/status.json"
STARTED="$(date +%s)"
CURRENT_STAGE="preflight"

for file in "$BIN" "$MINIMUM" "$STANDARD" "$MULTI" "$SOAK"; do
  [[ -f "$file" ]] || {
    echo "missing controller artifact: $file" >&2
    exit 1
  }
done
chmod +x "$BIN" "$MINIMUM" "$STANDARD" "$MULTI" "$SOAK"

write_status() {
  local state="$1" message="$2" now temporary
  now="$(date +%s)"
  temporary="$STATUS.tmp"
  jq -n \
    --arg state "$state" \
    --arg stage "$CURRENT_STAGE" \
    --arg message "$message" \
    --arg run_id "$RUN_ID" \
    --argjson started_at "$STARTED" \
    --argjson updated_at "$now" \
    --argjson elapsed_seconds "$((now - STARTED))" \
    '{
      state:$state,
      stage:$stage,
      message:$message,
      run_id:$run_id,
      started_at_unix:$started_at,
      updated_at_unix:$updated_at,
      elapsed_seconds:$elapsed_seconds
    }' >"$temporary"
  mv "$temporary" "$STATUS"
}

on_error() {
  local code="$1" line="$2" command="$3"
  trap - ERR
  jq -n \
    --arg result FAIL \
    --arg stage "$CURRENT_STAGE" \
    --arg command "$command" \
    --argjson exit_code "$code" \
    --argjson line "$line" \
    --argjson failed_at "$(date +%s)" \
    '{
      result:$result,
      stage:$stage,
      command:$command,
      exit_code:$exit_code,
      line:$line,
      failed_at_unix:$failed_at
    }' >"$BASE/failure.json"
  write_status FAIL "qualification stage failed"
  exit "$code"
}
trap 'on_error $? $LINENO "$BASH_COMMAND"' ERR

assert_n1_untouched_boundary() {
  systemctl is-active --quiet litiaina-n1
  for disk in /mnt/NDISK001 /mnt/NDISK002 /mnt/NDISK003 /mnt/NDISK004; do
    mountpoint -q "$disk"
    [[ -w "$disk" ]]
    available_kib="$(df --output=avail "$disk" | tail -n 1 | tr -d ' ')"
    (( available_kib > 104857600 ))
  done
}

mkdir -p "$BASE"
sha256sum "$BIN" >"$BASE/binary.sha256"
printf 'N2 3.3.1\n' >"$BASE/binary-version.txt"
systemctl is-active litiaina-n1 >"$BASE/n1-before.txt"
df -hT /mnt/NDISK001 /mnt/NDISK002 /mnt/NDISK003 /mnt/NDISK004 >"$BASE/storage-before.txt"
assert_n1_untouched_boundary
write_status RUNNING "preflight and N1 isolation boundary passed"

CURRENT_STAGE="minimum-ec-1-plus-1"
write_status RUNNING "minimum topology proof started"
N2_MIN_BIN="$BIN" \
N2_MIN_OUT="$BASE/minimum" \
N2_MIN_RUN_ID="$RUN_ID" \
N2_MIN_A_STORE="/mnt/NDISK001/.n2-validation/$RUN_ID/minimum/a" \
N2_MIN_B_STORE="/mnt/NDISK002/.n2-validation/$RUN_ID/minimum/b" \
N2_MIN_R_STORE="/mnt/NDISK003/.n2-validation/$RUN_ID/minimum/reserve" \
  "$MINIMUM" >"$BASE/minimum-controller.log" 2>&1
jq -e '.result == "PASS"' "$BASE/minimum/summary.json" >/dev/null
assert_n1_untouched_boundary
write_status RUNNING "minimum topology proof passed"

CURRENT_STAGE="standard-ec-2-plus-2"
write_status RUNNING "standard topology proof started"
ALL_LOCAL=true \
REMOTE_USER=administrator \
N2_PROOF_BIN="$BIN" \
N2_PROOF_OUT_ROOT="$BASE/standard" \
N2_PROOF_A_STORE="/mnt/NDISK001/.n2-validation/$RUN_ID/standard/a" \
N2_PROOF_B_STORE="/mnt/NDISK002/.n2-validation/$RUN_ID/standard/b" \
N2_PROOF_C_STORE="/mnt/NDISK003/.n2-validation/$RUN_ID/standard/c" \
N2_PROOF_D_STORE="/mnt/NDISK004/.n2-validation/$RUN_ID/standard/d" \
N2_PROOF_R_STORE="/mnt/NDISK001/.n2-validation/$RUN_ID/standard/reserve" \
  "$STANDARD" >"$BASE/standard-controller.log" 2>&1
STANDARD_SUMMARY="$(find "$BASE/standard" -name summary.json -type f -print -quit)"
jq -e '.result == "PASS"' "$STANDARD_SUMMARY" >/dev/null
rm -rf \
  "/mnt/NDISK001/.n2-validation/$RUN_ID/standard" \
  "/mnt/NDISK002/.n2-validation/$RUN_ID/standard" \
  "/mnt/NDISK003/.n2-validation/$RUN_ID/standard" \
  "/mnt/NDISK004/.n2-validation/$RUN_ID/standard"
assert_n1_untouched_boundary
write_status RUNNING "standard topology proof passed"

CURRENT_STAGE="multi-pool"
write_status RUNNING "append-only multi-pool proof started"
N2_MULTI_BIN="$BIN" \
N2_MULTI_OUT="$BASE/multi-pool" \
N2_MULTI_RUN_ID="$RUN_ID" \
N2_MULTI_D1="/mnt/NDISK001/.n2-validation/$RUN_ID/multi" \
N2_MULTI_D2="/mnt/NDISK002/.n2-validation/$RUN_ID/multi" \
N2_MULTI_D3="/mnt/NDISK003/.n2-validation/$RUN_ID/multi" \
N2_MULTI_D4="/mnt/NDISK004/.n2-validation/$RUN_ID/multi" \
  "$MULTI" >"$BASE/multi-pool-controller.log" 2>&1
jq -e '.result == "PASS"' "$BASE/multi-pool/summary.json" >/dev/null
assert_n1_untouched_boundary
write_status RUNNING "multi-pool proof passed"

CURRENT_STAGE="five-day-soak-chaos"
write_status RUNNING "five-day live workload and bounded chaos started"
ALL_LOCAL=true \
REMOTE_USER=administrator \
N2_SOAK_BIN="$BIN" \
N2_SOAK_OUT_ROOT="$BASE/soak" \
SOAK_RUN_ID="$RUN_ID" \
DURATION_SECONDS=432000 \
CYCLE_PAUSE_SECONDS=2 \
CHAOS_EVERY_CYCLES=20 \
ROLLING_LIVE_OBJECTS=1000 \
PAYLOAD_KIB=1024 \
ANCHOR_MIB=256 \
FAULT_DWELL_SECONDS=2 \
HEAL_INITIAL_DELAY_SECONDS=10 \
HEAL_SCAN_INTERVAL_SECONDS=60 \
N2_SOAK_A_STORE="/mnt/NDISK001/.n2-validation/$RUN_ID/soak/a" \
N2_SOAK_B_STORE="/mnt/NDISK002/.n2-validation/$RUN_ID/soak/b" \
N2_SOAK_C_STORE="/mnt/NDISK003/.n2-validation/$RUN_ID/soak/c" \
N2_SOAK_D_STORE="/mnt/NDISK004/.n2-validation/$RUN_ID/soak/d" \
  "$SOAK" >"$BASE/soak-controller.log" 2>&1
SOAK_SUMMARY="$(find "$BASE/soak" -name summary.json -type f -print -quit)"
jq -e '.result == "PASS"' "$SOAK_SUMMARY" >/dev/null
rm -rf \
  "/mnt/NDISK001/.n2-validation/$RUN_ID/soak" \
  "/mnt/NDISK002/.n2-validation/$RUN_ID/soak" \
  "/mnt/NDISK003/.n2-validation/$RUN_ID/soak" \
  "/mnt/NDISK004/.n2-validation/$RUN_ID/soak"

CURRENT_STAGE="complete"
assert_n1_untouched_boundary
systemctl is-active litiaina-n1 >"$BASE/n1-after.txt"
df -hT /mnt/NDISK001 /mnt/NDISK002 /mnt/NDISK003 /mnt/NDISK004 >"$BASE/storage-after.txt"
jq -n \
  --arg result PASS \
  --arg run_id "$RUN_ID" \
  --arg minimum "$BASE/minimum/summary.json" \
  --arg standard "$STANDARD_SUMMARY" \
  --arg multi "$BASE/multi-pool/summary.json" \
  --arg soak "$SOAK_SUMMARY" \
  --argjson started_at "$STARTED" \
  --argjson finished_at "$(date +%s)" \
  '{
    result:$result,
    run_id:$run_id,
    stages:{
      minimum:$minimum,
      standard:$standard,
      multi_pool:$multi,
      five_day_soak:$soak
    },
    started_at_unix:$started_at,
    finished_at_unix:$finished_at
  }' >"$BASE/summary.json"
write_status PASS "all N2 3.3 hospital-readiness stages passed"
