#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="onlysavemevods.service"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}"
INSTALL_DIR="${ONLYSAVEMEVODS_INSTALL_DIR:-${YTDLBOT_INSTALL_DIR:-/opt/onlysavemevods}}"
SERVICE_USER="${ONLYSAVEMEVODS_USER:-${YTDLBOT_USER:-onlysavemevods}}"

sudo systemctl stop "${SERVICE_NAME}" >/dev/null 2>&1 || true
sudo systemctl disable "${SERVICE_NAME}" >/dev/null 2>&1 || true
sudo rm -f "${UNIT_FILE}"
sudo systemctl daemon-reload

echo "Removed ${SERVICE_NAME}"
echo "Left ${INSTALL_DIR} untouched, including config, state, downloads, .venv, and .deno."
echo "Left service user ${SERVICE_USER} untouched."
