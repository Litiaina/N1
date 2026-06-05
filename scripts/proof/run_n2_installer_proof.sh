#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${OUT:-$ROOT_DIR/.n2-bench/installer-proof-$(date +%Y%m%d-%H%M%S)}"
INSTALLER="$ROOT_DIR/deploy/install.sh"

mkdir -p "$OUT"

run_case() {
  local name="$1"
  shift
  echo "[installer-proof] $name"
  "$INSTALLER" "$@" >"$OUT/$name.log" 2>&1
  tail -8 "$OUT/$name.log"
}

cd "$ROOT_DIR"

bash -n "$INSTALLER"
"$INSTALLER" --help >"$OUT/help.txt"

run_case node-dry-run \
  --node --dry-run \
  --bin-dir /tmp/n2-proof-bin \
  --home /tmp/n2-proof-home \
  --runtime-dir /tmp/n2-proof-runtime \
  --env-file /tmp/n2-proof-etc/n2.env \
  --service-dir /tmp/n2-proof-systemd \
  --node-id node-proof \
  --node-endpoint https://127.0.0.1:50001 \
  --native-endpoint n2://127.0.0.1:50002 \
  --index-roots /tmp/n2-proof-index0,/tmp/n2-proof-index1 \
  --blob-roots /tmp/n2-proof-blob0,/tmp/n2-proof-blob1 \
  --fs-root /tmp/n2-proof-fs \
  --enable --start

run_case gateway-dry-run \
  --gateway --dry-run \
  --bin-dir /tmp/n2-proof-bin \
  --home /tmp/n2-proof-home \
  --gateway-runtime-dir /tmp/n2-proof-gateway \
  --service-dir /tmp/n2-proof-systemd \
  --gateway-node node-a,https://127.0.0.1:50001,n2://127.0.0.1:50002 \
  --gateway-node node-b,https://127.0.0.1:51001,n2://127.0.0.1:51002 \
  --enable --start

run_case both-dry-run \
  --both --dry-run \
  --bin-dir /tmp/n2-proof-bin \
  --home /tmp/n2-proof-home \
  --runtime-dir /tmp/n2-proof-runtime \
  --gateway-runtime-dir /tmp/n2-proof-gateway \
  --env-file /tmp/n2-proof-etc/n2.env \
  --service-dir /tmp/n2-proof-systemd \
  --node-id node-a \
  --node-endpoint https://127.0.0.1:50001 \
  --native-endpoint n2://127.0.0.1:50002 \
  --index-roots /tmp/n2-proof-index \
  --blob-roots /tmp/n2-proof-blob \
  --fs-root /tmp/n2-proof-fs \
  --gateway-node node-a,https://127.0.0.1:50001,n2://127.0.0.1:50002

jq -n \
  --arg out "$OUT" \
  --arg status "passed" \
  '{status:$status,out:$out,cases:["node-dry-run","gateway-dry-run","both-dry-run"]}' \
  | tee "$OUT/summary.json"

echo "N2_INSTALLER_PROOF_OUT=$OUT"
