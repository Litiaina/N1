#!/usr/bin/env bash
# Share the established installer controls without requiring an N1 executable.
set -euo pipefail
exec "$(dirname "${BASH_SOURCE[0]}")/install-n1-systemd.sh" --component m1 "$@"
