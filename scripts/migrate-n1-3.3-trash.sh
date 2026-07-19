#!/usr/bin/env bash
set -euo pipefail

ROOTS_CSV=""
APPLY=0
MANIFEST=""

usage() {
  cat <<'USAGE'
Migrate N1 3.2 direct-path trash into N1 3.3 timestamp generations.

Usage:
  scripts/migrate-n1-3.3-trash.sh --roots /index/a,/index/b [--apply] [--manifest PATH]

The default is a read-only dry run. Stop N1 before using --apply. The script
validates every trash anchor across every configured index root before moving
anything. Live namespace paths and object blobs are never modified.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --roots)
      ROOTS_CSV="${2:-}"
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --manifest)
      MANIFEST="${2:-}"
      shift 2
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

[[ -n "$ROOTS_CSV" ]] || die "--roots is required"
IFS=',' read -r -a ROOTS <<< "$ROOTS_CSV"
[[ "${#ROOTS[@]}" -gt 0 ]] || die "at least one index root is required"
for root in "${ROOTS[@]}"; do
  [[ -d "$root" ]] || die "index root does not exist: $root"
done

declare -a PLAN_FRAGMENT=()
declare -a PLAN_RELATIVE=()
declare -a PLAN_GENERATION=()
declare -a PLAN_DELETED=()

path_exists_no_follow() {
  [[ -e "$1" || -L "$1" ]]
}

collect_legacy_anchors() {
  local fragment="$1"
  local legacy_dir="$2"
  local live_root="$3"
  local relative="$4"
  local child name child_relative live_path
  local -a children=()

  shopt -s nullglob dotglob
  children=("$legacy_dir"/*)
  shopt -u nullglob dotglob
  for child in "${children[@]}"; do
    name="${child##*/}"
    child_relative="${relative:+${relative}/}${name}"
    live_path="${live_root}/${child_relative}"
    if [[ -d "$child" && ! -L "$child" ]] && path_exists_no_follow "$live_path"; then
      collect_legacy_anchors "$fragment" "$child" "$live_root" "$child_relative"
    else
      PLAN_FRAGMENT+=("$fragment")
      PLAN_RELATIVE+=("$child_relative")
    fi
  done
}

PRIMARY_ROOT="${ROOTS[0]}"
shopt -s nullglob
for fragment_root in "$PRIMARY_ROOT"/@f_*; do
  [[ -d "$fragment_root" && ! -L "$fragment_root" ]] || continue
  trash_root="$fragment_root/.trash"
  [[ -d "$trash_root" ]] || continue
  fragment="${fragment_root##*/}"
  shopt -s dotglob
  top_entries=("$trash_root"/*)
  shopt -u dotglob
  for top_entry in "${top_entries[@]}"; do
    [[ -e "$top_entry" || -L "$top_entry" ]] || continue
    top_name="${top_entry##*/}"
    if [[ "$top_name" =~ ^[0-9]+\.d[1-9][0-9]*$ ]]; then
      continue
    fi
    if [[ -d "$top_entry" && ! -L "$top_entry" ]] \
      && path_exists_no_follow "$fragment_root/$top_name"; then
      collect_legacy_anchors "$fragment" "$top_entry" "$fragment_root" "$top_name"
    else
      PLAN_FRAGMENT+=("$fragment")
      PLAN_RELATIVE+=("$top_name")
    fi
  done
done
shopt -u nullglob

for ((i = 0; i < ${#PLAN_FRAGMENT[@]}; i++)); do
  fragment="${PLAN_FRAGMENT[$i]}"
  relative="${PLAN_RELATIVE[$i]}"
  primary_path="$PRIMARY_ROOT/$fragment/.trash/$relative"
  path_exists_no_follow "$primary_path" || die "primary trash anchor disappeared: $primary_path"
  deleted_seconds="$(stat -c '%Y' -- "$primary_path")"
  IFS='/' read -r -a path_parts <<< "$relative"
  depth="${#path_parts[@]}"
  unix_nanos=$((deleted_seconds * 1000000000 + i))
  generation="${unix_nanos}.d${depth}"
  while :; do
    collision=0
    for root in "${ROOTS[@]}"; do
      if path_exists_no_follow "$root/$fragment/.trash/$generation"; then
        collision=1
        break
      fi
    done
    [[ "$collision" -eq 0 ]] && break
    unix_nanos=$((unix_nanos + 1))
    generation="${unix_nanos}.d${depth}"
  done
  PLAN_GENERATION+=("$generation")
  PLAN_DELETED+=("$deleted_seconds")

  primary_type="$(stat -c '%F' -- "$primary_path")"
  primary_link=""
  primary_tree=""
  if [[ -L "$primary_path" ]]; then
    primary_link="$(readlink -- "$primary_path")"
  elif [[ -d "$primary_path" ]]; then
    primary_tree="$(find -P "$primary_path" -printf '%P\0%y\0%l\0' | LC_ALL=C sort -z | sha256sum | cut -d' ' -f1)"
  fi
  for root in "${ROOTS[@]}"; do
    replica_path="$root/$fragment/.trash/$relative"
    path_exists_no_follow "$replica_path" || die "missing trash replica: $replica_path"
    [[ "$(stat -c '%F' -- "$replica_path")" == "$primary_type" ]] \
      || die "trash replica type mismatch: $replica_path"
    if [[ -L "$primary_path" ]]; then
      [[ "$(readlink -- "$replica_path")" == "$primary_link" ]] \
        || die "trash symlink replica mismatch: $replica_path"
    elif [[ -d "$primary_path" ]]; then
      replica_tree="$(find -P "$replica_path" -printf '%P\0%y\0%l\0' | LC_ALL=C sort -z | sha256sum | cut -d' ' -f1)"
      [[ "$replica_tree" == "$primary_tree" ]] \
        || die "trash directory replica mismatch: $replica_path"
    fi
  done
done

echo "validated ${#PLAN_FRAGMENT[@]} legacy trash anchor(s) across ${#ROOTS[@]} index root(s)"
for ((i = 0; i < ${#PLAN_FRAGMENT[@]}; i++)); do
  printf '%s\t%s\t%s\t%s\n' \
    "${PLAN_FRAGMENT[$i]}" "${PLAN_RELATIVE[$i]}" \
    "${PLAN_GENERATION[$i]}" "${PLAN_DELETED[$i]}"
done

[[ "$APPLY" -eq 1 ]] || exit 0

if [[ -n "$MANIFEST" ]]; then
  mkdir -p -- "$(dirname "$MANIFEST")"
  : > "$MANIFEST"
fi

for ((i = 0; i < ${#PLAN_FRAGMENT[@]}; i++)); do
  fragment="${PLAN_FRAGMENT[$i]}"
  relative="${PLAN_RELATIVE[$i]}"
  generation="${PLAN_GENERATION[$i]}"
  for root in "${ROOTS[@]}"; do
    source="$root/$fragment/.trash/$relative"
    destination="$root/$fragment/.trash/$generation/$relative"
    mkdir -p -- "$(dirname "$destination")"
    mv -T -- "$source" "$destination"
  done
  if [[ -n "$MANIFEST" ]]; then
    printf '%s\t%s\t%s\t%s\n' \
      "$fragment" "$relative" "$generation" "${PLAN_DELETED[$i]}" >> "$MANIFEST"
  fi
done

for root in "${ROOTS[@]}"; do
  shopt -s nullglob
  for trash_root in "$root"/@f_*/.trash; do
    [[ -d "$trash_root" ]] || continue
    find "$trash_root" -mindepth 1 -depth -type d -empty -delete
  done
  shopt -u nullglob
done

echo "migration complete"
