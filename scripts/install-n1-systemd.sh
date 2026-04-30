#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_FILE="${ROOT_DIR}/deploy/systemd/litiaina-n1-standard.service.in"

SERVICE_NAME="litiaina-n1-standard"
INSTALL_DIR=""
BINARY_PATH=""
ENV_FILE=""
CONFIG_FILE=""
RUN_USER="root"
RUN_GROUP="root"
PRIVILEGE_MODE="root"
RUST_LOG_VALUE="info"
XFS_QUOTA_BIN="${N1_XFS_QUOTA_BIN:-/usr/sbin/xfs_quota}"
ENABLE_SERVICE=0
START_SERVICE=0
SKIP_QUOTA_PREFLIGHT=0
FORCE=0
DRY_RUN=0

usage() {
  cat <<'USAGE'
Install the production systemd unit for Litiaina N1 Standard.

Usage:
  sudo scripts/install-n1-systemd.sh --install-dir /path/to/n1-server [options]

Required:
  --install-dir PATH        Directory containing litiaina-n1, n1.env, and n1_config.ini.

Common options:
  --binary PATH             N1 binary path. Default: <install-dir>/litiaina-n1
  --env-file PATH           Environment file. Default: <install-dir>/n1.env
  --config-file PATH        Config file. Default: <install-dir>/n1_config.ini
  --service-name NAME       systemd unit name without .service. Default: litiaina-n1-standard
  --rust-log VALUE          RUST_LOG value. Default: info
  --xfs-quota-bin PATH      xfs_quota path. Default: /usr/sbin/xfs_quota
  --enable                  Run systemctl enable after installing.
  --start                   Run systemctl restart after installing.
  --force                   Replace an existing unit without prompting.
  --dry-run                 Render the unit to stdout; do not install or call systemctl.

Privilege modes:
  --privilege root          Production default. Runs service as root for strict XFS quota.
  --privilege cap-sys-admin Advanced. Runs as --user/--group with Ambient CAP_SYS_ADMIN.
  --privilege none          No quota privilege. Only valid when fragment_quota_backend=disabled.
  --user USER               Service user. Default: root.
  --group GROUP             Service group. Default: root.

Preflight:
  --skip-quota-preflight    Skip XFS project quota mount/limit checks during install.

Examples:
  sudo scripts/install-n1-systemd.sh \
    --install-dir /home/administrator/SERVERS/N1-SERVER \
    --enable --start

  sudo scripts/install-n1-systemd.sh \
    --install-dir /home/administrator/SERVERS/N1-SERVER \
    --privilege cap-sys-admin --user administrator --group administrator \
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

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
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

ini_get() {
  local section="$1"
  local key="$2"
  local file="$3"
  awk -v want_section="$section" -v want_key="$key" '
    BEGIN { in_section = 0 }
    /^[[:space:]]*;/ || /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    /^[[:space:]]*\[[^]]+\][[:space:]]*$/ {
      line = $0
      sub(/^[[:space:]]*\[/, "", line)
      sub(/\][[:space:]]*$/, "", line)
      in_section = (line == want_section)
      next
    }
    in_section {
      pos = index($0, "=")
      if (pos == 0) next
      k = substr($0, 1, pos - 1)
      v = substr($0, pos + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", k)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
      if (k == want_key) {
        print v
        exit 0
      }
    }
  ' "$file"
}

normalize_quota_backend() {
  local value
  value="$(printf '%s' "${1:-xfs_project}" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    ""|"xfs_project"|"xfs-project"|"xfs"|"project"|"required")
      printf 'xfs_project'
      ;;
    "disabled"|"off"|"none"|"report_only"|"report-only")
      printf 'disabled'
      ;;
    *)
      die "unsupported quota_config.fragment_quota_backend value: $value"
      ;;
  esac
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
      --xfs-quota-bin)
        XFS_QUOTA_BIN="${2:-}"
        shift 2
        ;;
      --privilege)
        PRIVILEGE_MODE="${2:-}"
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
      --skip-quota-preflight)
        SKIP_QUOTA_PREFLIGHT=1
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

  [[ -x "$BINARY_PATH" ]] || die "binary not found or not executable: $BINARY_PATH"
  [[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE"
  [[ -f "$CONFIG_FILE" ]] || die "config file not found: $CONFIG_FILE"
  [[ -f "$TEMPLATE_FILE" ]] || die "service template not found: $TEMPLATE_FILE"
  [[ -n "$SERVICE_NAME" ]] || die "--service-name cannot be empty"
  [[ -n "$RUN_USER" ]] || die "--user cannot be empty"
  [[ -n "$RUN_GROUP" ]] || die "--group cannot be empty"
  [[ -n "$RUST_LOG_VALUE" ]] || die "--rust-log cannot be empty"
}

validate_privilege_mode() {
  PRIVILEGE_MODE="$(printf '%s' "$PRIVILEGE_MODE" | tr '[:upper:]' '[:lower:]')"
  case "$PRIVILEGE_MODE" in
    root)
      [[ "$RUN_USER" == "root" ]] || die "--privilege root requires --user root"
      [[ "$RUN_GROUP" == "root" ]] || die "--privilege root requires --group root"
      ;;
    cap-sys-admin|cap_sys_admin|cap)
      PRIVILEGE_MODE="cap-sys-admin"
      ;;
    none|disabled)
      PRIVILEGE_MODE="none"
      ;;
    *)
      die "unsupported --privilege value: $PRIVILEGE_MODE"
      ;;
  esac
}

strict_quota_enabled() {
  local raw backend
  raw="$(ini_get "quota_config" "fragment_quota_backend" "$CONFIG_FILE")"
  backend="$(normalize_quota_backend "$raw")"
  [[ "$backend" == "xfs_project" ]]
}

validate_quota_preflight() {
  if ! strict_quota_enabled; then
    info "fragment_quota_backend=disabled; skipping strict XFS project quota preflight"
    return
  fi

  [[ "$PRIVILEGE_MODE" != "none" ]] || die "strict quota requires --privilege root or --privilege cap-sys-admin"
  [[ -x "$XFS_QUOTA_BIN" ]] || die "xfs_quota not executable: $XFS_QUOTA_BIN"

  local blob_roots
  blob_roots="$(ini_get "disk_config" "blob_store_disks" "$CONFIG_FILE")"
  [[ -n "$blob_roots" ]] || die "missing disk_config.blob_store_disks in $CONFIG_FILE"

  IFS=',' read -r -a roots <<< "$blob_roots"
  declare -A checked_mounts=()
  for raw_root in "${roots[@]}"; do
    local root mount_info mount_point fs_type options
    root="$(trim "$raw_root")"
    [[ -n "$root" ]] || continue
    [[ "$root" != *[[:space:]]* && "$root" != *\"* && "$root" != *"'"* && "$root" != *";"* ]] || {
      die "strict quota rejects blob paths with whitespace/quotes/semicolons: $root"
    }
    [[ -d "$root" ]] || die "blob root does not exist: $root"

    mount_info="$(findmnt -T "$root" -no TARGET,FSTYPE,OPTIONS | head -n 1 || true)"
    [[ -n "$mount_info" ]] || die "blob root is not on a mounted filesystem: $root"
    mount_point="$(awk '{print $1}' <<< "$mount_info")"
    fs_type="$(awk '{print $2}' <<< "$mount_info")"
    options="$(awk '{print $3}' <<< "$mount_info")"

    [[ "$fs_type" == "xfs" ]] || die "blob root $root is on $fs_type, strict quota requires XFS"
    [[ ",$options," == *",prjquota,"* || ",$options," == *",pquota,"* ]] || {
      die "blob root $root is mounted without prjquota/pquota: $options"
    }
    [[ ",$options," != *",noquota,"* ]] || die "blob root $root is mounted with noquota"

    if [[ -z "${checked_mounts[$mount_point]:-}" ]]; then
      info "validating XFS project quota on $mount_point"
      "$XFS_QUOTA_BIN" -x -c "state -p" "$mount_point" >/dev/null
      "$XFS_QUOTA_BIN" -x -c "limit -p bhard=1k 9999" "$mount_point" >/dev/null
      checked_mounts["$mount_point"]=1
    fi
  done
}

render_unit() {
  local privilege_lines
  case "$PRIVILEGE_MODE" in
    root)
      privilege_lines="# Strict XFS project quota mode runs privileged so N1 can set kernel quota limits."
      ;;
    cap-sys-admin)
      privilege_lines=$'# Advanced non-root strict quota mode. CAP_SYS_ADMIN is broad; root mode is simpler to operate.\nAmbientCapabilities=CAP_SYS_ADMIN\nCapabilityBoundingSet=CAP_SYS_ADMIN\nNoNewPrivileges=false'
      ;;
    none)
      privilege_lines="# No filesystem quota privilege. Use only with fragment_quota_backend=disabled."
      ;;
  esac

  local unit_tmp unit_dest
  unit_tmp="$(mktemp)"
  unit_dest="/etc/systemd/system/${SERVICE_NAME}.service"

  local working_directory binary env_file config_file xfs_quota_bin line
  working_directory="$(systemd_escape_path "$INSTALL_DIR")"
  binary="$(systemd_escape_path "$BINARY_PATH")"
  env_file="$(systemd_escape_path "$ENV_FILE")"
  config_file="$(systemd_escape_path "$CONFIG_FILE")"
  xfs_quota_bin="$(systemd_escape_path "$XFS_QUOTA_BIN")"

  while IFS= read -r line; do
    if [[ "$line" == "__PRIVILEGE_LINES__" ]]; then
      printf '%s\n' "$privilege_lines"
      continue
    fi
    line="${line//__SERVICE_NAME__/$SERVICE_NAME}"
    line="${line//__RUN_USER__/$RUN_USER}"
    line="${line//__RUN_GROUP__/$RUN_GROUP}"
    line="${line//__WORKING_DIRECTORY__/$working_directory}"
    line="${line//__BINARY__/$binary}"
    line="${line//__ENV_FILE__/$env_file}"
    line="${line//__CONFIG_FILE__/$config_file}"
    line="${line//__RUST_LOG__/$RUST_LOG_VALUE}"
    line="${line//__XFS_QUOTA_BIN__/$xfs_quota_bin}"
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
  validate_privilege_mode
  require_cmd awk
  if [[ "$DRY_RUN" -ne 1 ]]; then
    require_cmd systemctl
  fi
  if [[ "$SKIP_QUOTA_PREFLIGHT" -ne 1 ]]; then
    require_cmd findmnt
  fi

  if [[ "$SKIP_QUOTA_PREFLIGHT" -ne 1 ]]; then
    validate_quota_preflight
  else
    info "skipping strict quota preflight"
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
