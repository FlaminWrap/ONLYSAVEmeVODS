#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${ONLYSAVEMEVODS_INSTALL_DIR:-/opt/onlysavemevods}"
APP_DIR="${ONLYSAVEMEVODS_APP_DIR:-${INSTALL_DIR}/app}"
VENV_DIR="${ONLYSAVEMEVODS_VENV_DIR:-${INSTALL_DIR}/.venv}"
CONFIG_FILE="${ONLYSAVEMEVODS_CONFIG_FILE:-${INSTALL_DIR}/config.toml}"
SERVICE_NAME="${ONLYSAVEMEVODS_SERVICE_NAME:-onlysavemevods.service}"
APP_UPDATE_STATE_DIR="${ONLYSAVEMEVODS_APP_UPDATE_STATE_DIR:-${ONLYSAVEMEVODS_STATE_DIR:-${INSTALL_DIR}/state}}"
APP_UPDATE_TRIGGER_FILE="${APP_UPDATE_STATE_DIR}/app-update-trigger"
SYSTEMD_UNIT_DIR="${ONLYSAVEMEVODS_SYSTEMD_UNIT_DIR:-/etc/systemd/system}"
APP_UPDATE_SERVICE_NAME="${ONLYSAVEMEVODS_APP_UPDATE_SERVICE_NAME:-onlysavemevods-app-update.service}"
APP_UPDATE_PATH_NAME="${ONLYSAVEMEVODS_APP_UPDATE_PATH_NAME:-onlysavemevods-app-update.path}"
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

consume_update_trigger() {
  # Rename first so a trigger created during this run remains for the next start.
  local claimed_file="${APP_UPDATE_TRIGGER_FILE}.claimed.$$"
  if [[ -e "${APP_UPDATE_TRIGGER_FILE}" ]]; then
    mv -T -- "${APP_UPDATE_TRIGGER_FILE}" "${claimed_file}" || die "Could not consume app update trigger."
    rm -f -- "${claimed_file}"
  fi
}

repair_update_watcher() {
  # App-only updates replace this script, but leave root-owned systemd units
  # installed by earlier releases in place. Keep their trusted service policy;
  # migrate only the watcher to the trigger consumed above.
  local service_unit="${SYSTEMD_UNIT_DIR}/${APP_UPDATE_SERVICE_NAME}"
  local path_unit="${SYSTEMD_UNIT_DIR}/${APP_UPDATE_PATH_NAME}"
  [[ -f "${service_unit}" ]] || return 0

  local generated_unit changed=0
  generated_unit="$(mktemp "${SYSTEMD_UNIT_DIR}/.${APP_UPDATE_PATH_NAME}.XXXXXX")" || die "Could not prepare app update watcher repair."
  cat >"${generated_unit}" <<EOF
[Unit]
Description=Watch for ONLYSAVEmeVODS app update requests

[Path]
PathExists=${APP_UPDATE_TRIGGER_FILE}
Unit=${APP_UPDATE_SERVICE_NAME}

[Install]
WantedBy=multi-user.target

EOF
  if ! cmp -s -- "${generated_unit}" "${path_unit}"; then
    chmod 0644 "${generated_unit}"
    if ! mv -T -- "${generated_unit}" "${path_unit}"; then
      rm -f -- "${generated_unit}"
      die "Could not install repaired app update watcher; request remains pending."
    fi
    changed=1
  else
    rm -f -- "${generated_unit}"
  fi

  if [[ "${changed}" == "0" ]] && \
    systemctl is-active --quiet "${APP_UPDATE_PATH_NAME}" && \
    ! systemctl is-failed --quiet "${APP_UPDATE_SERVICE_NAME}"; then
    return 0
  fi

  echo "Repairing ${APP_UPDATE_PATH_NAME} to watch ${APP_UPDATE_TRIGGER_FILE}..."
  systemctl daemon-reload || die "Could not reload app update watcher; request remains pending."
  systemctl reset-failed "${APP_UPDATE_SERVICE_NAME}" "${APP_UPDATE_PATH_NAME}" || die "Could not reset app updater units; request remains pending."
  systemctl enable "${APP_UPDATE_PATH_NAME}" || die "Could not enable app update watcher; request remains pending."
  systemctl restart "${APP_UPDATE_PATH_NAME}" || die "Could not start app update watcher; request remains pending."
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
consume_update_trigger
[[ -x "${PYTHON_BIN}" ]] || die "Python venv not found or not executable: ${PYTHON_BIN}"
[[ -d "${APP_DIR}" ]] || die "Application directory not found: ${APP_DIR}"
[[ -f "${CONFIG_FILE}" ]] || die "Config file not found: ${CONFIG_FILE}"
take_lock
repair_update_watcher

POLICY_ARGS=(
  --trusted-repository "${TRUSTED_REPOSITORY}"
  --trusted-mode "${TRUSTED_MODE}"
  --trusted-include-prereleases "${TRUSTED_INCLUDE_PRERELEASES}"
  --trusted-token-env "${TRUSTED_TOKEN_ENV}"
)

has_pending_request() {
  "${PYTHON_BIN}" -m onlysavemevods.app_update has-request \
    --config "${CONFIG_FILE}" \
    --state-dir "${APP_UPDATE_STATE_DIR}"
}

if ! has_pending_request; then
  if ! "${PYTHON_BIN}" -m onlysavemevods.app_update check-trusted-auto \
    --config "${CONFIG_FILE}" \
    --state-dir "${APP_UPDATE_STATE_DIR}" \
    "${POLICY_ARGS[@]}" >/dev/null; then
    echo "Trusted update check failed; attempting any pending request independently." >&2
  fi
  if ! has_pending_request; then
    skip "No pending app update request."
  fi
fi

if ! REQUEST_INTENT="$("${PYTHON_BIN}" -m onlysavemevods.app_update request-intent \
  --config "${CONFIG_FILE}" \
  --state-dir "${APP_UPDATE_STATE_DIR}" \
  "${POLICY_ARGS[@]}")"; then
  die "Could not validate pending app update request intent; refusing to stop ${SERVICE_NAME}."
fi
case "${REQUEST_INTENT}" in
  normal)
    ensure_idle_if_service_active
    ;;
  force)
    if service_is_active; then
      echo "Force install requested; stopping ${SERVICE_NAME} even if it is recording. Active recordings will be interrupted." >&2
      STOPPED_SERVICE=1
      systemctl stop "${SERVICE_NAME}"
    else
      echo "${SERVICE_NAME} is not active; applying forced app update."
    fi
    ;;
  *)
    die "Invalid pending app update request intent: ${REQUEST_INTENT}"
    ;;
esac
"${PYTHON_BIN}" -m onlysavemevods.app_update apply \
  --config "${CONFIG_FILE}" \
  --install-dir "${INSTALL_DIR}" \
  --app-dir "${APP_DIR}" \
  --venv-dir "${VENV_DIR}" \
  --state-dir "${APP_UPDATE_STATE_DIR}" \
  "${POLICY_ARGS[@]}"
restart_service_if_needed
echo "App update completed."
