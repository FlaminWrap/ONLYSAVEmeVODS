#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

if [[ -r /etc/os-release ]]; then
  . /etc/os-release
  case "${ID:-}" in
    almalinux|rhel|rocky|centos|fedora)
      ;;
    *)
      echo "WARNING: ${0##*/} is intended for AlmaLinux/RHEL-like systems. Continuing with the shared installer." >&2
      ;;
  esac
fi

exec "${SCRIPT_DIR}/install-systemd.sh" "$@"
