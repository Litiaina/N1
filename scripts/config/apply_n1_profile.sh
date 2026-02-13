#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE_NAME="${1:-}"
TARGET_CONFIG="${2:-$ROOT_DIR/server/n1_config.ini}"
PROFILE_DIR="${ROOT_DIR}/profiles/n1"

if [[ -z "${PROFILE_NAME}" ]]; then
  echo "Usage: $0 <low-hw|balanced|high-hw> [target_config_path]" >&2
  exit 1
fi

PROFILE_FILE="${PROFILE_DIR}/${PROFILE_NAME}.ini"
if [[ ! -f "${PROFILE_FILE}" ]]; then
  echo "Profile not found: ${PROFILE_FILE}" >&2
  exit 1
fi

if [[ ! -f "${TARGET_CONFIG}" ]]; then
  echo "Target config not found: ${TARGET_CONFIG}" >&2
  exit 1
fi

TMP="$(mktemp)"
BACKUP="${TARGET_CONFIG}.bak.$(date +%Y%m%d-%H%M%S)"

# Extract key=value pairs from the profile's [upload_config] section.
declare -A profile_kv=()
in_section=0
while IFS= read -r line; do
  if [[ "${line}" =~ ^[[:space:]]*\[upload_config\][[:space:]]*$ ]]; then
    in_section=1
    continue
  fi
  if [[ "${line}" =~ ^[[:space:]]*\[.*\][[:space:]]*$ ]]; then
    in_section=0
    continue
  fi
  if [[ "${in_section}" -eq 1 && "${line}" =~ ^[[:space:]]*([a-zA-Z0-9_]+)[[:space:]]*=(.*)$ ]]; then
    key="${BASH_REMATCH[1]}"
    val="${BASH_REMATCH[2]}"
    val="${val#"${val%%[![:space:]]*}"}"
    val="${val%"${val##*[![:space:]]}"}"
    profile_kv["$key"]="$val"
  fi
done < "${PROFILE_FILE}"

if [[ "${#profile_kv[@]}" -eq 0 ]]; then
  echo "No [upload_config] keys found in ${PROFILE_FILE}" >&2
  exit 1
fi

cp "${TARGET_CONFIG}" "${BACKUP}"

awk -v profile_file="${PROFILE_FILE}" '
  BEGIN {
    in_section = 0
    section_seen = 0
  }
  function ltrim(s) { sub(/^[ \t\r\n]+/, "", s); return s }
  function rtrim(s) { sub(/[ \t\r\n]+$/, "", s); return s }
  function trim(s) { return rtrim(ltrim(s)) }
  function load_profile(path,   line, k, v, pos) {
    in_prof = 0
    while ((getline line < path) > 0) {
      if (line ~ /^[ \t]*\[upload_config\][ \t]*$/) { in_prof = 1; continue }
      if (line ~ /^[ \t]*\[.*\][ \t]*$/) { in_prof = 0; continue }
      if (in_prof && line ~ /^[ \t]*[A-Za-z0-9_]+[ \t]*=/) {
        pos = index(line, "=")
        k = trim(substr(line, 1, pos - 1))
        v = trim(substr(line, pos + 1))
        prof[k] = v
        keys[++nkeys] = k
      }
    }
    close(path)
  }
  function flush_missing(   i, k) {
    for (i = 1; i <= nkeys; i++) {
      k = keys[i]
      if (!(k in seen)) {
        print k "=" prof[k]
      }
    }
  }
  BEGIN { load_profile(profile_file) }
  {
    if ($0 ~ /^[ \t]*\[upload_config\][ \t]*$/) {
      in_section = 1
      section_seen = 1
      delete seen
      print $0
      next
    }
    if (in_section && $0 ~ /^[ \t]*\[.*\][ \t]*$/) {
      flush_missing()
      in_section = 0
      print $0
      next
    }
    if (in_section && $0 ~ /^[ \t]*[A-Za-z0-9_]+[ \t]*=/) {
      split($0, a, "=")
      key = trim(a[1])
      if (key in prof) {
        print key "=" prof[key]
        seen[key] = 1
        next
      }
    }
    print $0
  }
  END {
    if (in_section) {
      flush_missing()
    } else if (!section_seen) {
      print ""
      print "[upload_config]"
      for (i = 1; i <= nkeys; i++) {
        k = keys[i]
        print k "=" prof[k]
      }
    }
  }
' "${TARGET_CONFIG}" > "${TMP}"

mv "${TMP}" "${TARGET_CONFIG}"

echo "Applied profile '${PROFILE_NAME}' to ${TARGET_CONFIG}"
echo "Backup saved at ${BACKUP}"
