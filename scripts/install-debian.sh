#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

if [[ -r /etc/os-release ]]; then
  . /etc/os-release
  if [[ "${ID:-}" != "debian" ]]; then
    echo "WARNING: ${0##*/} is intended for Debian. Continuing with the shared installer." >&2
  fi
fi

exec "${SCRIPT_DIR}/install-almalinux.sh" "$@"
