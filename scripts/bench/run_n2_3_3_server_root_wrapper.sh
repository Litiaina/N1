#!/usr/bin/env bash
set -Eeuo pipefail

# Root-only wrapper for the isolated ENOSPC qualification root. The N2
# controller and every N2 process still run as the unprivileged administrator
# account. Only loop-image creation, mount, and unmount require root.

: "${N2_SEQUENCE_RUN_ID:?set N2_SEQUENCE_RUN_ID}"
: "${N2_SEQUENCE_BASE:?set N2_SEQUENCE_BASE}"

RUN_ID="$N2_SEQUENCE_RUN_ID"
BASE="$N2_SEQUENCE_BASE"
SEQUENCE="$BASE/controller/run_n2_3_3_server_sequence.sh"
MOUNT_ROOT="/mnt/NDISK002/.n2-validation/$RUN_ID/minimum/b"
DISK_RUN_ROOT="/mnt/NDISK002/.n2-validation/$RUN_ID"
IMAGE_DIR="/mnt/NDISK002/.n2-validation/$RUN_ID/loop"
IMAGE="$IMAGE_DIR/node-b.xfs"
RESULT="FAIL"

[[ "$(id -u)" -eq 0 ]] || {
  echo "root wrapper must run as root" >&2
  exit 1
}
[[ "$BASE" == "/home/administrator/DEV-TEST/$RUN_ID" ]]
[[ "$MOUNT_ROOT" == "/mnt/NDISK002/.n2-validation/$RUN_ID/minimum/b" ]]
[[ -x "$SEQUENCE" ]]
systemctl is-active --quiet litiaina-n1

cleanup() {
  set +e
  if mountpoint -q "$MOUNT_ROOT"; then
    umount "$MOUNT_ROOT"
  fi
  if [[ "$RESULT" == "PASS" ]]; then
    rm -f "$IMAGE"
    rmdir "$MOUNT_ROOT" "$(dirname "$MOUNT_ROOT")" "$IMAGE_DIR" 2>/dev/null || true
  fi
}
trap cleanup EXIT

mkdir -p "$IMAGE_DIR" "$MOUNT_ROOT"
chown -R administrator:administrator "$DISK_RUN_ROOT"
truncate -s 1G "$IMAGE"
mkfs.xfs -f -L N2ROOTFAULT "$IMAGE" >"$BASE/root-fault-mkfs.log"
mount -o loop,noatime "$IMAGE" "$MOUNT_ROOT"
chown administrator:administrator "$MOUNT_ROOT"
chmod 0755 "$MOUNT_ROOT"
findmnt "$MOUNT_ROOT" >"$BASE/root-fault-mount.txt"

runuser -u administrator -- env \
  N2_SEQUENCE_RUN_ID="$RUN_ID" \
  N2_SEQUENCE_BASE="$BASE" \
  "$SEQUENCE"

RESULT="PASS"
