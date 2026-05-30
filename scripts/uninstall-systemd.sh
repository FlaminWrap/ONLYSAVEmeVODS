#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="ytdlbot.service"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}"
INSTALL_DIR="${YTDLBOT_INSTALL_DIR:-/opt/ytdlbot}"
SERVICE_USER="${YTDLBOT_USER:-ytdlbot}"

sudo systemctl stop "${SERVICE_NAME}" >/dev/null 2>&1 || true
sudo systemctl disable "${SERVICE_NAME}" >/dev/null 2>&1 || true
sudo rm -f "${UNIT_FILE}"
sudo systemctl daemon-reload

echo "Removed ${SERVICE_NAME}"
echo "Left ${INSTALL_DIR} untouched, including config, state, downloads, .venv, and .deno."
echo "Left service user ${SERVICE_USER} untouched."
