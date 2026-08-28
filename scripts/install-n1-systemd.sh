#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_FILE="${ROOT_DIR}/deploy/systemd/litiaina-n1.service.in"

SERVICE_NAME="litiaina-n1"
INSTALL_DIR=""
BINARY_PATH=""
ENV_FILE=""
CONFIG_FILE=""
RUN_USER=""
RUN_GROUP=""
RUST_LOG_VALUE="info"
ENABLE_SERVICE=0
START_SERVICE=0
FORCE=0
DRY_RUN=0

usage() {
  cat <<'USAGE'
Install the production systemd unit for Litiaina N1.

Usage:
  sudo scripts/install-n1-systemd.sh --install-dir /path/to/n1-server [options]

Required:
  --install-dir PATH        Directory containing litiaina-n1, n1.env, and n1_config.ini.

Common options:
  --binary PATH             N1 binary path. Default: <install-dir>/litiaina-n1
  --env-file PATH           Environment file. Default: <install-dir>/n1.env
  --config-file PATH        Config file. Default: <install-dir>/n1_config.ini
  --service-name NAME       systemd unit name without .service. Default: litiaina-n1
  --rust-log VALUE          RUST_LOG value. Default: info
  --enable                  Run systemctl enable after installing.
  --start                   Run systemctl restart after installing.
  --force                   Replace an existing unit without prompting.
  --dry-run                 Render the unit to stdout; do not install or call systemctl.
  --user USER               Service user. Default: sudo caller or current user.
  --group GROUP             Service group. Default: primary group of --user.

Examples:
  sudo scripts/install-n1-systemd.sh \
    --install-dir /home/administrator/SERVERS/N1-SERVER \
    --enable --start

  sudo scripts/install-n1-systemd.sh \
    --install-dir /home/administrator/SERVERS/N1-SERVER \
    --user administrator --group administrator \
    --enable --start
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "==> $*" >&2
}

systemd_escape_path() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value// /\\x20}"
  value="${value//$'\t'/\\t}"
  value="${value//\"/\\x22}"
  value="${value//\'/\\x27}"
  value="${value//;/\\x3b}"
  printf '%s' "$value"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --install-dir)
        INSTALL_DIR="${2:-}"
        shift 2
        ;;
      --binary)
        BINARY_PATH="${2:-}"
        shift 2
        ;;
      --env-file)
        ENV_FILE="${2:-}"
        shift 2
        ;;
      --config-file)
        CONFIG_FILE="${2:-}"
        shift 2
        ;;
      --service-name)
        SERVICE_NAME="${2:-}"
        shift 2
        ;;
      --rust-log)
        RUST_LOG_VALUE="${2:-}"
        shift 2
        ;;
      --user)
        RUN_USER="${2:-}"
        shift 2
        ;;
      --group)
        RUN_GROUP="${2:-}"
        shift 2
        ;;
      --enable)
        ENABLE_SERVICE=1
        shift
        ;;
      --start)
        START_SERVICE=1
        ENABLE_SERVICE=1
        shift
        ;;
      --force)
        FORCE=1
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

validate_paths() {
  [[ "$DRY_RUN" -eq 1 || "$(id -u)" -eq 0 ]] || die "run this installer with sudo/root"
  [[ -n "$INSTALL_DIR" ]] || die "--install-dir is required"
  [[ -d "$INSTALL_DIR" ]] || die "install dir does not exist: $INSTALL_DIR"

  BINARY_PATH="${BINARY_PATH:-${INSTALL_DIR}/litiaina-n1}"
  ENV_FILE="${ENV_FILE:-${INSTALL_DIR}/n1.env}"
  CONFIG_FILE="${CONFIG_FILE:-${INSTALL_DIR}/n1_config.ini}"
  if [[ -z "$RUN_USER" ]]; then
    RUN_USER="${SUDO_USER:-$(id -un)}"
  fi
  if [[ -z "$RUN_GROUP" ]]; then
    RUN_GROUP="$(id -gn "$RUN_USER" 2>/dev/null || printf '%s' "$RUN_USER")"
  fi

  [[ -x "$BINARY_PATH" ]] || die "binary not found or not executable: $BINARY_PATH"
  [[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE"
  [[ -f "$CONFIG_FILE" ]] || die "config file not found: $CONFIG_FILE"
  [[ -f "$TEMPLATE_FILE" ]] || die "service template not found: $TEMPLATE_FILE"
  [[ -n "$SERVICE_NAME" ]] || die "--service-name cannot be empty"
  [[ -n "$RUN_USER" ]] || die "--user cannot be empty"
  [[ -n "$RUN_GROUP" ]] || die "--group cannot be empty"
  [[ -n "$RUST_LOG_VALUE" ]] || die "--rust-log cannot be empty"

  local expected_env expected_config
  expected_env="$(cd "$INSTALL_DIR" && pwd -P)/n1.env"
  expected_config="$(cd "$INSTALL_DIR" && pwd -P)/n1_config.ini"
  [[ "$(cd "$(dirname "$ENV_FILE")" && pwd -P)/$(basename "$ENV_FILE")" == "$expected_env" ]] || {
    die "N1 reads n1.env from its working directory; place it at ${INSTALL_DIR}/n1.env"
  }
  [[ "$(cd "$(dirname "$CONFIG_FILE")" && pwd -P)/$(basename "$CONFIG_FILE")" == "$expected_config" ]] || {
    die "N1 reads n1_config.ini from its working directory; place it at ${INSTALL_DIR}/n1_config.ini"
  }
}

render_unit() {
  local unit_tmp unit_dest
  unit_tmp="$(mktemp)"
  unit_dest="/etc/systemd/system/${SERVICE_NAME}.service"

  local working_directory binary env_file config_file line requires_mounts
  working_directory="$(systemd_escape_path "$INSTALL_DIR")"
  binary="$(systemd_escape_path "$BINARY_PATH")"
  env_file="$(systemd_escape_path "$ENV_FILE")"
  config_file="$(systemd_escape_path "$CONFIG_FILE")"
  requires_mounts="$({
    awk '
      /^\[disk_config\][[:space:]]*$/ { in_disk = 1; next }
      /^\[/ { in_disk = 0 }
      in_disk && /^(fs_storage_path|index_store_disks|blob_store_disks)[[:space:]]*=/ {
        sub(/^[^=]*=/, "")
        gsub(/,/, "\n")
        print
      }
    ' "$CONFIG_FILE"
  } | while IFS= read -r mount_path; do
    mount_path="${mount_path#"${mount_path%%[![:space:]]*}"}"
    mount_path="${mount_path%"${mount_path##*[![:space:]]}"}"
    if [[ -n "$mount_path" ]]; then
      systemd_escape_path "$mount_path"
      printf '\n'
    fi
  done | sort -u | paste -sd' ' -)"
  [[ -n "$requires_mounts" ]] || die "no N1 storage mounts found in $CONFIG_FILE"

  while IFS= read -r line; do
    line="${line//__SERVICE_NAME__/$SERVICE_NAME}"
    line="${line//__RUN_USER__/$RUN_USER}"
    line="${line//__RUN_GROUP__/$RUN_GROUP}"
    line="${line//__WORKING_DIRECTORY__/$working_directory}"
    line="${line//__BINARY__/$binary}"
    line="${line//__ENV_FILE__/$env_file}"
    line="${line//__CONFIG_FILE__/$config_file}"
    line="${line//__REQUIRES_MOUNTS_FOR__/$requires_mounts}"
    line="${line//__RUST_LOG__/$RUST_LOG_VALUE}"
    printf '%s\n' "$line"
  done < "$TEMPLATE_FILE" > "$unit_tmp"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    cat "$unit_tmp"
    rm -f "$unit_tmp"
    return
  fi

  if [[ -e "$unit_dest" && "$FORCE" -ne 1 ]]; then
    rm -f "$unit_tmp"
    die "unit already exists: $unit_dest (use --force to replace)"
  fi

  install -m 0644 "$unit_tmp" "$unit_dest"
  rm -f "$unit_tmp"
  info "installed $unit_dest"
}

main() {
  parse_args "$@"
  validate_paths
  require_cmd awk
  if [[ "$DRY_RUN" -ne 1 ]]; then
    require_cmd systemctl
  fi

  render_unit
  if [[ "$DRY_RUN" -eq 1 ]]; then
    return
  fi
  systemctl daemon-reload

  if [[ "$ENABLE_SERVICE" -eq 1 ]]; then
    systemctl enable "${SERVICE_NAME}.service"
  fi
  if [[ "$START_SERVICE" -eq 1 ]]; then
    systemctl restart "${SERVICE_NAME}.service"
    systemctl --no-pager --full status "${SERVICE_NAME}.service" || true
  fi

  info "done"
  echo "Logs: journalctl -u ${SERVICE_NAME}.service -f"
}

main "$@"
