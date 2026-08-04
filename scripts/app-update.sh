#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${ONLYSAVEMEVODS_INSTALL_DIR:-/opt/onlysavemevods}"
APP_DIR="${ONLYSAVEMEVODS_APP_DIR:-${INSTALL_DIR}/app}"
VENV_DIR="${ONLYSAVEMEVODS_VENV_DIR:-${INSTALL_DIR}/.venv}"
CONFIG_FILE="${ONLYSAVEMEVODS_CONFIG_FILE:-${INSTALL_DIR}/config.toml}"
SERVICE_NAME="${ONLYSAVEMEVODS_SERVICE_NAME:-onlysavemevods.service}"
APP_UPDATE_STATE_DIR="${ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR:-${ONLYSAVEMEVODS_STATE_DIR:-${INSTALL_DIR}/state}}"
UPDATE_LOCK_FILE="${ONLYSAVEMEVODS_UPDATE_LOCK_FILE:-${INSTALL_DIR}/.update.lock}"
TRUSTED_REPOSITORY="${ONLYSAVEMEVODS_TRUSTED_APP_UPDATE_REPOSITORY:-FlaminWrap/ONLYSAVEmeVODS}"
TRUSTED_MODE="${ONLYSAVEMEVODS_TRUSTED_APP_UPDATE_MODE:-manual}"
TRUSTED_INCLUDE_PRERELEASES="${ONLYSAVEMEVODS_TRUSTED_APP_UPDATE_INCLUDE_PRERELEASES:-false}"
TRUSTED_TOKEN_ENV="${ONLYSAVEMEVODS_TRUSTED_APP_UPDATE_TOKEN_ENV:-GITHUB_TOKEN}"
PYTHON_BIN="${VENV_DIR}/bin/python"
STOPPED_SERVICE=0
SERVICE_RESTARTED=0

die() {
  echo "$*" >&2
  exit 1
}

cleanup() {
  local exit_code=$?
  if [[ "${STOPPED_SERVICE}" == "1" && "${SERVICE_RESTARTED}" == "0" ]]; then
    echo "Restarting ${SERVICE_NAME} after app updater exit..."
    systemctl start "${SERVICE_NAME}" || true
  fi
  exit "${exit_code}"
}

skip() {
  echo "$*"
  exit 0
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    die "This updater must run as root so it can manage ${SERVICE_NAME} and the root-owned app directory."
  fi
}

take_lock() {
  command -v flock >/dev/null 2>&1 || die "flock is required for safe updater serialization."
  install -d -m 0755 "${INSTALL_DIR}"
  exec 9>"${UPDATE_LOCK_FILE}"
  if ! flock -n 9; then
    skip "Another installer or updater is already running; skipping."
  fi
  trap cleanup EXIT
}

service_is_active() {
  systemctl is-active --quiet "${SERVICE_NAME}"
}

ensure_idle_if_service_active() {
  if ! service_is_active; then
    echo "${SERVICE_NAME} is not active; applying app update without starting it first."
    return 0
  fi

  echo "${SERVICE_NAME} is active; checking whether it is idle..."
  set +e
  "${PYTHON_BIN}" -m onlysavemevods.python_update check-idle --config "${CONFIG_FILE}"
  local idle_status=$?
  set -e

  case "${idle_status}" in
    0)
      echo "${SERVICE_NAME} is idle; stopping it for app update."
      systemctl stop "${SERVICE_NAME}"
      STOPPED_SERVICE=1
      ;;
    1)
      skip "${SERVICE_NAME} is busy; app update remains pending."
      ;;
    2)
      skip "Could not confirm ${SERVICE_NAME} is idle; app update remains pending."
      ;;
    *)
      die "Idle check failed with unexpected exit code ${idle_status}."
      ;;
  esac
}

restart_service_if_needed() {
  if [[ "${STOPPED_SERVICE}" != "1" ]]; then
    return 0
  fi
  echo "Starting ${SERVICE_NAME} after app update..."
  systemctl start "${SERVICE_NAME}"
  SERVICE_RESTARTED=1
}

require_root
[[ -x "${PYTHON_BIN}" ]] || die "Python venv not found or not executable: ${PYTHON_BIN}"
[[ -d "${APP_DIR}" ]] || die "Application directory not found: ${APP_DIR}"
[[ -f "${CONFIG_FILE}" ]] || die "Config file not found: ${CONFIG_FILE}"
take_lock

POLICY_ARGS=(
  --trusted-repository "${TRUSTED_REPOSITORY}"
  --trusted-mode "${TRUSTED_MODE}"
  --trusted-include-prereleases "${TRUSTED_INCLUDE_PRERELEASES}"
  --trusted-token-env "${TRUSTED_TOKEN_ENV}"
)

if ! "${PYTHON_BIN}" -m onlysavemevods.app_update check-trusted-auto \
  --config "${CONFIG_FILE}" \
  --state-dir "${APP_UPDATE_STATE_DIR}" \
  "${POLICY_ARGS[@]}" >/dev/null; then
  echo "Trusted update check failed; attempting any pending request independently." >&2
fi
if ! "${PYTHON_BIN}" -m onlysavemevods.app_update has-request \
  --config "${CONFIG_FILE}" \
  --state-dir "${APP_UPDATE_STATE_DIR}"; then
  skip "No pending app update request."
fi

ensure_idle_if_service_active
"${PYTHON_BIN}" -m onlysavemevods.app_update apply \
  --config "${CONFIG_FILE}" \
  --install-dir "${INSTALL_DIR}" \
  --app-dir "${APP_DIR}" \
  --venv-dir "${VENV_DIR}" \
  --state-dir "${APP_UPDATE_STATE_DIR}" \
  "${POLICY_ARGS[@]}"
restart_service_if_needed
echo "App update completed."
