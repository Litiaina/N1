#!/usr/bin/env bash
set -euo pipefail

# Dependency-free HA chaos harness for N1 workflow tests.
# Uses bash + coreutils + awk only.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

RUNS="${RUNS:-5}"
N1_SOAK_ROUNDS="${N1_SOAK_ROUNDS:-10}"
N1_SOAK_OPS_PER_ROUND="${N1_SOAK_OPS_PER_ROUND:-16}"
N1_SOAK_CLIENT_CONCURRENCY="${N1_SOAK_CLIENT_CONCURRENCY:-8}"
N1_SOAK_DISK_DOWN="${N1_SOAK_DISK_DOWN:-1}"

PACKAGE="${PACKAGE:-server}"
TEST_NAME="${TEST_NAME:-matrix_ha_chaos_profile_nightly}"
TEST_ARGS="${TEST_ARGS:---ignored --nocapture}"

# Optional gates.
MAX_DURATION_SECS="${MAX_DURATION_SECS:-0}"  # 0 disables gate
REQUIRE_ALL_PASS="${REQUIRE_ALL_PASS:-1}"    # 1 = fail summary if any run fails

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-.n1-bench/ha-chaos-${STAMP}}"
mkdir -p "$OUT_DIR"

CSV="$OUT_DIR/summary.csv"
LOG_SUMMARY="$OUT_DIR/summary.txt"

echo "run,exit_code,wall_seconds,reported_seconds,passed" > "$CSV"

echo "N1 HA chaos harness"
echo "output_dir=$OUT_DIR"
echo "runs=$RUNS rounds=$N1_SOAK_ROUNDS ops_per_round=$N1_SOAK_OPS_PER_ROUND client_concurrency=$N1_SOAK_CLIENT_CONCURRENCY disk_down=$N1_SOAK_DISK_DOWN"
echo

run_one() {
  local i="$1"
  local log="$OUT_DIR/run_${i}.log"

  echo "[run $i/$RUNS] starting..."
  local start_ms
  start_ms="$(date +%s%3N)"

  set +e
  N1_SOAK_ROUNDS="$N1_SOAK_ROUNDS" \
  N1_SOAK_OPS_PER_ROUND="$N1_SOAK_OPS_PER_ROUND" \
  N1_SOAK_CLIENT_CONCURRENCY="$N1_SOAK_CLIENT_CONCURRENCY" \
  N1_SOAK_DISK_DOWN="$N1_SOAK_DISK_DOWN" \
  cargo test -p "$PACKAGE" "$TEST_NAME" -- $TEST_ARGS >"$log" 2>&1
  local ec=$?
  set -e

  local end_ms
  end_ms="$(date +%s%3N)"
  local wall_ms=$(( end_ms - start_ms ))
  local wall
  wall="$(awk "BEGIN { printf \"%.3f\", ${wall_ms}/1000 }")"

  local reported
  reported="$(awk '
    /finished in [0-9]+\.[0-9]+s/ {
      for (i=1; i<=NF; i++) {
        if ($i == "in" && (i+1) <= NF) {
          v=$(i+1)
          sub(/s$/, "", v)
          print v
          exit
        }
      }
    }' "$log")"
  reported="${reported:-NA}"

  local passed="0"
  if [[ "$ec" -eq 0 ]] && grep -q "test result: ok" "$log"; then
    passed="1"
  fi

  echo "$i,$ec,$wall,$reported,$passed" >> "$CSV"

  echo "[run $i/$RUNS] exit=$ec wall=${wall}s reported=${reported}s passed=$passed"
}

for i in $(seq 1 "$RUNS"); do
  run_one "$i"
done

read -r pass_count fail_count min_wall max_wall avg_wall p95_wall <<<"$(awk -F, '
  NR==1 { next }
  {
    n++
    wall[n]=$3+0
    sum+=$3+0
    if ($5=="1") pass++
    else fail++
    if (n==1 || $3+0 < min) min=$3+0
    if (n==1 || $3+0 > max) max=$3+0
  }
  END {
    if (n==0) { print "0 0 0 0 0 0"; exit }
    for (i=1; i<=n; i++) {
      for (j=i+1; j<=n; j++) {
        if (wall[j] < wall[i]) {
          t=wall[i]; wall[i]=wall[j]; wall[j]=t
        }
      }
    }
    idx = int((95*n + 99) / 100)
    if (idx < 1) idx = 1
    if (idx > n) idx = n
    avg = sum / n
    printf "%d %d %.3f %.3f %.3f %.3f\n", pass+0, fail+0, min+0, max+0, avg+0, wall[idx]+0
  }' "$CSV")"

status="PASS"
if [[ "$REQUIRE_ALL_PASS" == "1" && "$fail_count" -gt 0 ]]; then
  status="FAIL"
fi
if [[ "$MAX_DURATION_SECS" -gt 0 ]] && awk "BEGIN { exit !($avg_wall > $MAX_DURATION_SECS) }"; then
  status="FAIL"
fi

{
  echo "status=$status"
  echo "runs=$RUNS pass=$pass_count fail=$fail_count"
  echo "wall_seconds:min=$min_wall avg=$avg_wall p95=$p95_wall max=$max_wall"
  echo "gates:require_all_pass=$REQUIRE_ALL_PASS max_duration_secs=$MAX_DURATION_SECS"
  echo "csv=$CSV"
} | tee "$LOG_SUMMARY"

if [[ "$status" != "PASS" ]]; then
  exit 1
fi
