#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

MODE=""
BUILD=false
START=false
ENABLE=false
DRY_RUN=false
FORCE_CONFIG=false
SKIP_SYSTEMD=false
CREATE_USER=true
COPY_CERTS=false

SERVICE_USER="litiaina"
SERVICE_GROUP="litiaina"
N2_HOME="/opt/litiaina/n2"
BIN_DIR="/usr/local/bin"
SERVICE_DIR="/etc/systemd/system"
NODE_RUNTIME_DIR="/var/lib/litiaina/n2"
GATEWAY_RUNTIME_DIR="/var/lib/litiaina/n2-gateway"
NODE_ENV_FILE="/etc/litiaina/n2/n2.env"
NODE_LOG_FILTER="n2=info,tower_http=info"
GATEWAY_LOG_FILTER="n2_gateway=info,tower_http=info"

NODE_ID="node-a"
NODE_HOST="0.0.0.0"
NODE_HTTP_PORT="50001"
NODE_NATIVE_PORT="50002"
NODE_ENDPOINT="https://127.0.0.1:50001"
NODE_NATIVE_ENDPOINT="n2://127.0.0.1:50002"
INDEX_ROOTS="/mnt/n2/index"
BLOB_ROOTS="/mnt/n2/blob"
FS_ROOT="/mnt/n2/fs"
CLUSTER_ID="litiaina-n2-prod"
MEMBERSHIP_EPOCH="1"

GATEWAY_HOST="0.0.0.0"
GATEWAY_PORT="51002"
GATEWAY_NODES=()
CERT_SRC="$REPO_ROOT/certificates"

usage() {
    cat <<'EOF'
N2 installer

Usage:
  sudo deploy/install.sh --node [options]
  sudo deploy/install.sh --gateway [options]
  sudo deploy/install.sh --both [options]

Modes:
  --node                  Install litiaina-n2 and litiaina-n2-shell.
  --gateway               Install litiaina-n2-gateway.
  --both                  Install node, shell, and gateway.

Build/install:
  --build                 Run cargo build --release before installing binaries.
  --bin-dir PATH          Binary install directory. Default: /usr/local/bin
  --home PATH             Documentation/release home. Default: /opt/litiaina/n2
  --user USER             systemd service user. Default: litiaina
  --group GROUP           systemd service group. Default: litiaina
  --no-create-user        Do not create the service user/group.
  --force-config          Overwrite generated config/service files.
  --copy-certs            Copy cert.pem/key.pem from --cert-src into runtime dirs.
  --cert-src PATH         Certificate source directory. Default: ./certificates
  --skip-systemd          Install binaries/config only.
  --enable                systemctl enable installed services.
  --start                 systemctl restart installed services after install.
  --dry-run               Print commands without changing the system.

Node options:
  --runtime-dir PATH      N2 runtime dir. Default: /var/lib/litiaina/n2
  --env-file PATH         N2 environment file. Default: /etc/litiaina/n2/n2.env
  --node-id ID            This node id. Default: node-a
  --host HOST             Server bind host. Default: 0.0.0.0
  --http-port PORT        N2 HTTPS port. Default: 50001
  --native-port PORT      N2 native n2:// port. Default: 50002
  --node-endpoint URL     Public node HTTPS endpoint.
  --native-endpoint URL   Public node native endpoint.
  --index-roots LIST      Comma-separated index roots. Default: /mnt/n2/index
  --blob-roots LIST       Comma-separated blob roots. Default: /mnt/n2/blob
  --fs-root PATH          Namespace/runtime storage root. Default: /mnt/n2/fs
  --cluster-id ID         Membership cluster id. Default: litiaina-n2-prod
  --membership-epoch N    Initial membership epoch. Default: 1

Gateway options:
  --gateway-runtime-dir PATH  Gateway runtime dir. Default: /var/lib/litiaina/n2-gateway
  --gateway-host HOST         Gateway bind host. Default: 0.0.0.0
  --gateway-port PORT         Gateway HTTPS port. Default: 51002
  --gateway-node SPEC         Node route entry. Repeatable.
                             SPEC: id,https://host:port,n2://host:port[,weight]

Examples:
  sudo deploy/install.sh --node --build --copy-certs \
    --node-id node-a \
    --node-endpoint https://192.168.0.22:50001 \
    --native-endpoint n2://192.168.0.22:50002 \
    --index-roots /mnt/DISK0/n2-index,/mnt/DISK1/n2-index \
    --blob-roots /mnt/DISK0/n2-blob,/mnt/DISK1/n2-blob \
    --fs-root /mnt/DISK0/n2-fs \
    --enable --start

  sudo deploy/install.sh --gateway --copy-certs \
    --gateway-node node-a,https://192.168.0.22:50001,n2://192.168.0.22:50002 \
    --gateway-node node-b,https://192.168.0.13:50001,n2://192.168.0.13:50002 \
    --enable --start
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

log() {
    echo "==> $*"
}

run() {
    if [[ "$DRY_RUN" == true ]]; then
        printf 'DRY-RUN:'
        printf ' %q' "$@"
        printf '\n'
    else
        "$@"
    fi
}

need_root() {
    if [[ "$DRY_RUN" == true ]]; then
        return
    fi
    [[ "${EUID:-$(id -u)}" -eq 0 ]] || die "run as root, or use --dry-run"
}

csv_to_paths() {
    local raw="$1"
    raw="${raw//,/ }"
    echo "$raw"
}

first_csv_path() {
    local raw="$1"
    raw="${raw%%,*}"
    echo "$raw"
}

random_secret() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 48
    else
        tr -dc 'A-Za-z0-9' </dev/urandom | head -c 64
        echo
    fi
}

write_if_missing() {
    local path="$1"
    local mode="$2"
    local owner="${3:-root:root}"
    local body="$4"
    if [[ -e "$path" && "$FORCE_CONFIG" != true ]]; then
        log "keeping existing $path"
        return
    fi
    log "writing $path"
    if [[ "$DRY_RUN" == true ]]; then
        echo "DRY-RUN: write $path"
        return
    fi
    install -d -m 0755 "$(dirname "$path")"
    local tmp
    tmp="$(mktemp)"
    printf '%s\n' "$body" >"$tmp"
    install -m "$mode" -o "${owner%%:*}" -g "${owner##*:}" "$tmp" "$path"
    rm -f "$tmp"
}

ensure_service_user() {
    if [[ "$CREATE_USER" != true ]]; then
        return 0
    fi
    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        run groupadd --system "$SERVICE_GROUP"
    fi
    if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
        run useradd --system --gid "$SERVICE_GROUP" --home "$N2_HOME" --shell /usr/sbin/nologin "$SERVICE_USER"
    fi
}

install_docs() {
    log "installing release documentation into $N2_HOME"
    run install -d -m 0755 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$N2_HOME"
    for file in README.md DOCUMENTATION.md ARCHITECTURE.md GATEWAY.md INSTALLATION.md LICENSE; do
        [[ -f "$REPO_ROOT/$file" ]] || continue
        run install -m 0644 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$REPO_ROOT/$file" "$N2_HOME/$file"
    done
}

build_binaries() {
    if [[ "$BUILD" != true ]]; then
        return 0
    fi
    case "$MODE" in
        node)
            run cargo build -p n2 -p n2_shell --release
            ;;
        gateway)
            run cargo build -p n2_gateway --release
            ;;
        both)
            run cargo build -p n2 -p n2_shell -p n2_gateway --release
            ;;
    esac
}

install_binary() {
    local name="$1"
    local src="$REPO_ROOT/target/release/$name"
    [[ -x "$src" ]] || die "$src is missing; run with --build or build it first"
    log "installing $name into $BIN_DIR"
    run install -d -m 0755 "$BIN_DIR"
    run install -m 0755 "$src" "$BIN_DIR/$name"
}

ensure_dirs() {
    log "creating runtime directories"
    if [[ "$MODE" == node || "$MODE" == both ]]; then
        run install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$NODE_RUNTIME_DIR" "$NODE_RUNTIME_DIR/certificates"
        for path in $(csv_to_paths "$INDEX_ROOTS") $(csv_to_paths "$BLOB_ROOTS") "$FS_ROOT"; do
            [[ -n "$path" ]] || continue
            run install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$path"
        done
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        run install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$GATEWAY_RUNTIME_DIR" "$GATEWAY_RUNTIME_DIR/certificates"
    fi
}

copy_certs() {
    if [[ "$COPY_CERTS" != true ]]; then
        return 0
    fi
    [[ -f "$CERT_SRC/cert.pem" && -f "$CERT_SRC/key.pem" ]] || die "missing cert.pem/key.pem under $CERT_SRC"
    log "copying TLS certificates from $CERT_SRC"
    if [[ "$MODE" == node || "$MODE" == both ]]; then
        run install -m 0644 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CERT_SRC/cert.pem" "$NODE_RUNTIME_DIR/certificates/cert.pem"
        run install -m 0600 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CERT_SRC/key.pem" "$NODE_RUNTIME_DIR/certificates/key.pem"
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        run install -m 0644 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CERT_SRC/cert.pem" "$GATEWAY_RUNTIME_DIR/certificates/cert.pem"
        run install -m 0600 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$CERT_SRC/key.pem" "$GATEWAY_RUNTIME_DIR/certificates/key.pem"
    fi
}

write_node_env() {
    local body
    body="$(cat <<EOF
JWT_SECRET='$(random_secret)'
AUTH_KEY='$(random_secret)'
MASTER_KEY='$(random_secret)'
ADMIN_KEY='$(random_secret)'
ACCESS_TOKEN_DEFAULT_TTL_SECS=900
ACCESS_TOKEN_MAX_TTL_SECS=3600
REFRESH_TOKEN_TTL_SECS=604800
N2_RUNTIME_WORKER_THREADS=
N2_RUNTIME_MAX_BLOCKING_THREADS=
N2_RUNTIME_THREAD_STACK_BYTES=
EOF
)"
    write_if_missing "$NODE_ENV_FILE" 0600 root:root "$body"
}

write_node_config() {
    local body
    body="$(cat <<EOF
[server]
host=$NODE_HOST
https_port=$NODE_HTTP_PORT
native_port=$NODE_NATIVE_PORT
cert_path=$NODE_RUNTIME_DIR/certificates/cert.pem
key_path=$NODE_RUNTIME_DIR/certificates/key.pem

[disk_config]
index_store_disks=$INDEX_ROOTS
blob_store_disks=$BLOB_ROOTS
fs_storage_path=$FS_ROOT

[cluster_config]
node_id=$NODE_ID
node_endpoint=$NODE_ENDPOINT
native_endpoint=$NODE_NATIVE_ENDPOINT
membership_file=n2_membership.json
cluster_layout_mode=auto
fixed_data_shards=
fixed_parity_shards=
auto_rebalance=false

[quota_config]
fragment_quota_backend=disabled

[garbage_collector]
cleanup_interval_seconds=3600
file_ttl_seconds=3600
retention_seconds=2592000

[set_limits]
max_file_size=5368709120

[jwt_token_config]
login_token_expiration=900

[rate_limiter_config]
enable_rate_limiting=true
auth_limit_per_second=2
auth_limit_burst_size=5
view_limit_per_second=30
view_limit_burst_size=100
upload_limit_per_second=300
upload_limit_burst_size=3000
global_limit_per_second=200
global_limit_burst_size=300

[upload_config]
max_concurrent_io=auto
adaptive_concurrency_enabled=true
adaptive_concurrency_interval_ms=auto
writer_buffer_size=auto
upload_memory_budget_bytes=auto
upload_part_strict_durability_default=false
upload_io_gate_enabled=true
stream_max_concurrency=auto
stream_chunk_size=auto
stream_readahead_bytes=auto
stream_locality_bias_enabled=true
monitor_interval_seconds=auto
monitor_deep_scan_interval_seconds=auto
max_part_size=52428800
data_shards=1
parity_shards=0
stripe_block_size=262144
EOF
)"
    write_if_missing "$NODE_RUNTIME_DIR/n2_config.ini" 0640 "$SERVICE_USER:$SERVICE_GROUP" "$body"
}

write_membership() {
    local membership="$FS_ROOT/n2_membership.json"
    local index_json blob_json
    index_json="$(csv_json_array "$INDEX_ROOTS")"
    blob_json="$(csv_json_array "$BLOB_ROOTS")"
    local body
    body="$(cat <<EOF
{
  "cluster_id": "$CLUSTER_ID",
  "epoch": $MEMBERSHIP_EPOCH,
  "nodes": [
    {
      "id": "$NODE_ID",
      "endpoint": "$NODE_ENDPOINT",
      "native_endpoint": "$NODE_NATIVE_ENDPOINT",
      "state": "online",
      "weight": 1,
      "index_roots": $index_json,
      "blob_roots": $blob_json
    }
  ]
}
EOF
)"
    write_if_missing "$membership" 0640 "$SERVICE_USER:$SERVICE_GROUP" "$body"
}

csv_json_array() {
    local raw="$1"
    local first=true
    printf '['
    IFS=',' read -r -a values <<<"$raw"
    for value in "${values[@]}"; do
        value="$(echo "$value" | xargs)"
        [[ -n "$value" ]] || continue
        if [[ "$first" == true ]]; then
            first=false
        else
            printf ', '
        fi
        printf '"%s"' "${value//\"/\\\"}"
    done
    printf ']'
}

default_gateway_node() {
    echo "$NODE_ID,$NODE_ENDPOINT,$NODE_NATIVE_ENDPOINT,1"
}

write_gateway_config() {
    if [[ "${#GATEWAY_NODES[@]}" -eq 0 ]]; then
        GATEWAY_NODES+=("$(default_gateway_node)")
    fi

    local body
    body="$(cat <<EOF
[server]
host=$GATEWAY_HOST
https_port=$GATEWAY_PORT
cert_path=$GATEWAY_RUNTIME_DIR/certificates/cert.pem
key_path=$GATEWAY_RUNTIME_DIR/certificates/key.pem

[routing]
health_interval_seconds=5
request_timeout_seconds=300

EOF
)"
    local entry id http_url native_url weight
    for entry in "${GATEWAY_NODES[@]}"; do
        IFS=',' read -r id http_url native_url weight <<<"$entry"
        id="${id:-}"
        http_url="${http_url:-}"
        native_url="${native_url:-}"
        weight="${weight:-1}"
        [[ -n "$id" && -n "$http_url" && -n "$native_url" ]] || die "invalid --gateway-node '$entry'"
        body+="$(cat <<EOF
[node.$id]
http_url=$http_url
native_url=$native_url
weight=$weight

EOF
)"
    done

    write_if_missing "$GATEWAY_RUNTIME_DIR/n2_gateway_config.ini" 0640 "$SERVICE_USER:$SERVICE_GROUP" "$body"
}

sed_escape() {
    printf '%s' "$1" | sed -e 's/[\/&|]/\\&/g'
}

render_service() {
    local template="$1"
    local target="$2"
    local data_paths
    data_paths="$(csv_to_paths "$INDEX_ROOTS") $(csv_to_paths "$BLOB_ROOTS") $FS_ROOT"

    log "rendering $(basename "$target")"
    if [[ "$DRY_RUN" == true ]]; then
        echo "DRY-RUN: render $template -> $target"
        return
    fi

    install -d -m 0755 "$SERVICE_DIR"
    sed \
        -e "s|@N2_HOME@|$(sed_escape "$N2_HOME")|g" \
        -e "s|@N2_USER@|$(sed_escape "$SERVICE_USER")|g" \
        -e "s|@N2_GROUP@|$(sed_escape "$SERVICE_GROUP")|g" \
        -e "s|@N2_RUNTIME_DIR@|$(sed_escape "$NODE_RUNTIME_DIR")|g" \
        -e "s|@N2_ENV_FILE@|$(sed_escape "$NODE_ENV_FILE")|g" \
        -e "s|@N2_BIN@|$(sed_escape "$BIN_DIR/litiaina-n2")|g" \
        -e "s|@N2_LOG_FILTER@|$(sed_escape "$NODE_LOG_FILTER")|g" \
        -e "s|@N2_DATA_PATHS@|$(sed_escape "$data_paths")|g" \
        -e "s|@N2_GATEWAY_RUNTIME_DIR@|$(sed_escape "$GATEWAY_RUNTIME_DIR")|g" \
        -e "s|@N2_GATEWAY_BIN@|$(sed_escape "$BIN_DIR/litiaina-n2-gateway")|g" \
        -e "s|@N2_GATEWAY_LOG_FILTER@|$(sed_escape "$GATEWAY_LOG_FILTER")|g" \
        "$template" >"$target"
    chmod 0644 "$target"
}

install_services() {
    if [[ "$SKIP_SYSTEMD" != false ]]; then
        return 0
    fi
    if [[ "$MODE" == node || "$MODE" == both ]]; then
        render_service "$REPO_ROOT/deploy/systemd/litiaina-n2.service.in" "$SERVICE_DIR/litiaina-n2.service"
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        render_service "$REPO_ROOT/deploy/systemd/litiaina-n2-gateway.service.in" "$SERVICE_DIR/litiaina-n2-gateway.service"
    fi
    run systemctl daemon-reload
}

enable_or_start_services() {
    if [[ "$SKIP_SYSTEMD" != false ]]; then
        return 0
    fi
    local services=()
    if [[ "$MODE" == node || "$MODE" == both ]]; then
        services+=("litiaina-n2.service")
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        services+=("litiaina-n2-gateway.service")
    fi
    if [[ "${#services[@]}" -eq 0 ]]; then
        return 0
    fi
    if [[ "$ENABLE" == true ]]; then
        run systemctl enable "${services[@]}"
    fi
    if [[ "$START" == true ]]; then
        run systemctl restart "${services[@]}"
        run systemctl --no-pager --full status "${services[@]}"
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --help|-h) usage; exit 0 ;;
            --node) MODE="node" ;;
            --gateway) MODE="gateway" ;;
            --both) MODE="both" ;;
            --build) BUILD=true ;;
            --start) START=true ;;
            --enable) ENABLE=true ;;
            --dry-run) DRY_RUN=true ;;
            --force-config) FORCE_CONFIG=true ;;
            --skip-systemd) SKIP_SYSTEMD=true ;;
            --no-create-user) CREATE_USER=false ;;
            --copy-certs) COPY_CERTS=true ;;
            --user) SERVICE_USER="${2:?missing --user value}"; shift ;;
            --group) SERVICE_GROUP="${2:?missing --group value}"; shift ;;
            --home) N2_HOME="${2:?missing --home value}"; shift ;;
            --bin-dir) BIN_DIR="${2:?missing --bin-dir value}"; shift ;;
            --service-dir) SERVICE_DIR="${2:?missing --service-dir value}"; shift ;;
            --runtime-dir) NODE_RUNTIME_DIR="${2:?missing --runtime-dir value}"; shift ;;
            --gateway-runtime-dir) GATEWAY_RUNTIME_DIR="${2:?missing --gateway-runtime-dir value}"; shift ;;
            --env-file) NODE_ENV_FILE="${2:?missing --env-file value}"; shift ;;
            --node-id) NODE_ID="${2:?missing --node-id value}"; shift ;;
            --host) NODE_HOST="${2:?missing --host value}"; shift ;;
            --http-port) NODE_HTTP_PORT="${2:?missing --http-port value}"; shift ;;
            --native-port) NODE_NATIVE_PORT="${2:?missing --native-port value}"; shift ;;
            --node-endpoint) NODE_ENDPOINT="${2:?missing --node-endpoint value}"; shift ;;
            --native-endpoint) NODE_NATIVE_ENDPOINT="${2:?missing --native-endpoint value}"; shift ;;
            --index-roots) INDEX_ROOTS="${2:?missing --index-roots value}"; shift ;;
            --blob-roots) BLOB_ROOTS="${2:?missing --blob-roots value}"; shift ;;
            --fs-root) FS_ROOT="${2:?missing --fs-root value}"; shift ;;
            --cluster-id) CLUSTER_ID="${2:?missing --cluster-id value}"; shift ;;
            --membership-epoch) MEMBERSHIP_EPOCH="${2:?missing --membership-epoch value}"; shift ;;
            --gateway-host) GATEWAY_HOST="${2:?missing --gateway-host value}"; shift ;;
            --gateway-port) GATEWAY_PORT="${2:?missing --gateway-port value}"; shift ;;
            --gateway-node) GATEWAY_NODES+=("${2:?missing --gateway-node value}"); shift ;;
            --cert-src) CERT_SRC="${2:?missing --cert-src value}"; shift ;;
            --node-log-filter) NODE_LOG_FILTER="${2:?missing --node-log-filter value}"; shift ;;
            --gateway-log-filter) GATEWAY_LOG_FILTER="${2:?missing --gateway-log-filter value}"; shift ;;
            *) die "unknown option: $1" ;;
        esac
        shift
    done
    [[ -n "$MODE" ]] || die "choose --node, --gateway, or --both"
}

main() {
    parse_args "$@"
    need_root

    log "install mode: $MODE"
    ensure_service_user
    install_docs
    build_binaries

    if [[ "$MODE" == node || "$MODE" == both ]]; then
        install_binary litiaina-n2
        install_binary litiaina-n2-shell
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        install_binary litiaina-n2-gateway
    fi

    ensure_dirs
    copy_certs

    if [[ "$MODE" == node || "$MODE" == both ]]; then
        write_node_env
        write_node_config
        write_membership
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        write_gateway_config
    fi

    install_services
    enable_or_start_services

    log "install complete"
    if [[ "$MODE" == node || "$MODE" == both ]]; then
        echo "node config: $NODE_RUNTIME_DIR/n2_config.ini"
        echo "node env:    $NODE_ENV_FILE"
        echo "node svc:    litiaina-n2.service"
    fi
    if [[ "$MODE" == gateway || "$MODE" == both ]]; then
        echo "gateway config: $GATEWAY_RUNTIME_DIR/n2_gateway_config.ini"
        echo "gateway svc:    litiaina-n2-gateway.service"
    fi
}

main "$@"
