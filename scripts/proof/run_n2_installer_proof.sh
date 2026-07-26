#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${OUT:-/home/wlzambo/DEV-TEST/N2-3.0-installer-proof-$(date +%Y%m%d-%H%M%S)}"
INSTALLER="$ROOT_DIR/deploy/install.sh"
ENV_SRC="$OUT/n2.env"
MEMBERSHIP_SRC="$OUT/n2_membership.json"

mkdir -p "$OUT"
printf '%s\n' \
  'JWT_SECRET=installer-proof-jwt' \
  'N2_CLUSTER_SECRET=installer-proof-cluster' \
  'MASTER_KEY=installer-proof-master' >"$ENV_SRC"
printf '%s\n' \
  '{' \
  '  "format_version": 4,' \
  '  "cluster_id": "installer-proof",' \
  '  "epoch": 1,' \
  '  "pools": [{' \
  '    "id": "pool-1", "generation": 1, "state": "writable",' \
  '    "policy": {"object_data_shards": 1, "object_parity_shards": 1},' \
  '    "node_ids": ["node-a", "node-b"]' \
  '  }],' \
  '  "nodes": [' \
  '    {"id":"node-a","endpoint":"https://127.0.0.1:50001","native_endpoint":"n2://127.0.0.1:50002","state":"online","weight":1,"storage_roots":[{"id":"root-a","path":"/tmp/n2-proof-a"}],"joined_epoch":1},' \
  '    {"id":"node-b","endpoint":"https://127.0.0.1:51001","native_endpoint":"n2://127.0.0.1:51002","state":"online","weight":1,"storage_roots":[{"id":"root-b","path":"/tmp/n2-proof-b"}],"joined_epoch":1}' \
  '  ]' \
  '}' >"$MEMBERSHIP_SRC"

run_case() {
  local name="$1"
  shift
  "$INSTALLER" "$@" >"$OUT/$name.log" 2>&1
}

cd "$ROOT_DIR"
bash -n "$INSTALLER"
"$INSTALLER" --help >"$OUT/help.txt"

COMMON_NODE=(
  --dry-run --no-create-user --skip-systemd
  --bin-dir /tmp/n2-proof-bin
  --home /tmp/n2-proof-home
  --runtime-dir /tmp/n2-proof-runtime
  --env-file /tmp/n2-proof-etc/n2.env
  --node-id node-a
  --node-endpoint https://127.0.0.1:50001
  --native-endpoint n2://127.0.0.1:50002
  --storage-roots root-a=/tmp/n2-proof-a
  --state-root /tmp/n2-proof-runtime/state
  --pool-id pool-1 --pool-generation 1
  --data-shards 1 --parity-shards 1
  --env-src "$ENV_SRC"
  --membership-src "$MEMBERSHIP_SRC"
)

run_case node-dry-run --node "${COMMON_NODE[@]}"
run_case gateway-dry-run \
  --gateway --dry-run --no-create-user --skip-systemd \
  --bin-dir /tmp/n2-proof-bin --home /tmp/n2-proof-home \
  --gateway-runtime-dir /tmp/n2-proof-gateway \
  --gateway-node node-a,https://127.0.0.1:50001,n2://127.0.0.1:50002
run_case both-dry-run --both "${COMMON_NODE[@]}" \
  --gateway-runtime-dir /tmp/n2-proof-gateway \
  --gateway-node node-a,https://127.0.0.1:50001,n2://127.0.0.1:50002

jq -n --arg out "$OUT" \
  '{status:"passed",format_version:4,out:$out,cases:["node-dry-run","gateway-dry-run","both-dry-run"]}' \
  | tee "$OUT/summary.json"

echo "N2_INSTALLER_PROOF_OUT=$OUT"
