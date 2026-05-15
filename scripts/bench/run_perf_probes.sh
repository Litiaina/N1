#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PACKAGE="${PACKAGE:-n1-arc}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-.n1-bench/perf-probes-${STAMP}}"
mkdir -p "$OUT_DIR"

CSV="$OUT_DIR/summary.csv"
SUMMARY="$OUT_DIR/summary.txt"

echo "probe,exit_code,wall_seconds,summary" > "$CSV"

run_probe() {
  local probe_name="$1"
  local summary_prefix="$2"
  local log="$OUT_DIR/${probe_name}.log"
  local start_ms end_ms wall_ms wall ec summary_line

  echo "[probe] ${probe_name}"
  start_ms="$(date +%s%3N)"
  set +e
  cargo test -p "$PACKAGE" "$probe_name" -- --ignored --nocapture >"$log" 2>&1
  ec=$?
  set -e
  end_ms="$(date +%s%3N)"
  wall_ms=$(( end_ms - start_ms ))
  wall="$(awk "BEGIN { printf \"%.3f\", ${wall_ms}/1000 }")"
  summary_line="$(grep -F "$summary_prefix" "$log" | tail -n 1 | tr ',' ';' || true)"
  printf '%s,%s,%s,%q\n' "$probe_name" "$ec" "$wall" "$summary_line" >> "$CSV"
  if [[ "$ec" -ne 0 ]]; then
    return "$ec"
  fi
}

run_probe matrix_crud_cpu_ram_probe "N1 CRUD probe summary:"
run_probe matrix_upload_single_nvme_saturation_probe "N1 upload probe:"
run_probe matrix_upload_rss_plateau_probe "N1 upload RSS probe:"
run_probe matrix_stream_sequential_readahead_locality_probe "N1 stream probe:"
run_probe matrix_empty_trash_fragment_nuke_probe "N1 empty trash probe:"

awk -F, 'NR==1 { next } { if ($2 != 0) fail++ ; total++ } END { printf "probes=%d fail=%d\n", total+0, fail+0 }' "$CSV" | tee "$SUMMARY"
