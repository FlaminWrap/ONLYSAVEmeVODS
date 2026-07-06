#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="onlysavemevods.service"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}"
PYTHON_UPDATE_SERVICE_NAME="onlysavemevods-python-update.service"
PYTHON_UPDATE_TIMER_NAME="onlysavemevods-python-update.timer"
PYTHON_UPDATE_SERVICE_UNIT="/etc/systemd/system/${PYTHON_UPDATE_SERVICE_NAME}"
PYTHON_UPDATE_TIMER_UNIT="/etc/systemd/system/${PYTHON_UPDATE_TIMER_NAME}"
APP_UPDATE_SERVICE_NAME="onlysavemevods-app-update.service"
APP_UPDATE_PATH_NAME="onlysavemevods-app-update.path"
APP_UPDATE_TIMER_NAME="onlysavemevods-app-update.timer"
APP_UPDATE_SERVICE_UNIT="/etc/systemd/system/${APP_UPDATE_SERVICE_NAME}"
APP_UPDATE_PATH_UNIT="/etc/systemd/system/${APP_UPDATE_PATH_NAME}"
APP_UPDATE_TIMER_UNIT="/etc/systemd/system/${APP_UPDATE_TIMER_NAME}"
INSTALL_DIR="${ONLYSAVEMEVODS_INSTALL_DIR:-/opt/onlysavemevods}"
SERVICE_USER="${ONLYSAVEMEVODS_USER:-onlysavemevods}"

sudo systemctl stop "${APP_UPDATE_TIMER_NAME}" >/dev/null 2>&1 || true
sudo systemctl disable "${APP_UPDATE_TIMER_NAME}" >/dev/null 2>&1 || true
sudo systemctl stop "${APP_UPDATE_PATH_NAME}" >/dev/null 2>&1 || true
sudo systemctl disable "${APP_UPDATE_PATH_NAME}" >/dev/null 2>&1 || true
sudo systemctl stop "${APP_UPDATE_SERVICE_NAME}" >/dev/null 2>&1 || true
sudo systemctl stop "${PYTHON_UPDATE_TIMER_NAME}" >/dev/null 2>&1 || true
sudo systemctl disable "${PYTHON_UPDATE_TIMER_NAME}" >/dev/null 2>&1 || true
sudo systemctl stop "${PYTHON_UPDATE_SERVICE_NAME}" >/dev/null 2>&1 || true
sudo systemctl stop "${SERVICE_NAME}" >/dev/null 2>&1 || true
sudo systemctl disable "${SERVICE_NAME}" >/dev/null 2>&1 || true
sudo rm -f \
  "${UNIT_FILE}" \
  "${PYTHON_UPDATE_SERVICE_UNIT}" \
  "${PYTHON_UPDATE_TIMER_UNIT}" \
  "${APP_UPDATE_SERVICE_UNIT}" \
  "${APP_UPDATE_PATH_UNIT}" \
  "${APP_UPDATE_TIMER_UNIT}"
sudo systemctl daemon-reload

echo "Removed ${SERVICE_NAME}"
echo "Removed ${PYTHON_UPDATE_TIMER_NAME} and ${PYTHON_UPDATE_SERVICE_NAME}"
echo "Removed ${APP_UPDATE_TIMER_NAME}, ${APP_UPDATE_PATH_NAME}, and ${APP_UPDATE_SERVICE_NAME}"
echo "Left ${INSTALL_DIR} untouched, including config, state, downloads, .venv, and .deno."
echo "Left service user ${SERVICE_USER} untouched."
