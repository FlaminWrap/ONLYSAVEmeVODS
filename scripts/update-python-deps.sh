#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${ONLYSAVEMEVODS_INSTALL_DIR:-/opt/onlysavemevods}"
APP_DIR="${ONLYSAVEMEVODS_APP_DIR:-${INSTALL_DIR}/app}"
VENV_DIR="${ONLYSAVEMEVODS_VENV_DIR:-${INSTALL_DIR}/.venv}"
CONFIG_FILE="${ONLYSAVEMEVODS_CONFIG_FILE:-${INSTALL_DIR}/config.toml}"
SERVICE_NAME="${ONLYSAVEMEVODS_SERVICE_NAME:-onlysavemevods.service}"
LOCK_DIR="${ONLYSAVEMEVODS_PYTHON_UPDATE_LOCK_DIR:-${INSTALL_DIR}/.python-update.lock}"
PYTHON_BIN="${VENV_DIR}/bin/python"
YTDLP_BIN="${VENV_DIR}/bin/yt-dlp"
WHISPERX_BIN="${VENV_DIR}/bin/whisperx"
STOPPED_SERVICE=0
SERVICE_RESTARTED=0

die() {
  echo "$*" >&2
  exit 1
}

cleanup() {
  local exit_code=$?
  if [[ "${STOPPED_SERVICE}" == "1" && "${SERVICE_RESTARTED}" == "0" ]]; then
    echo "Restarting ${SERVICE_NAME} after updater exit..."
    systemctl start "${SERVICE_NAME}" || true
  fi
  rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true
  exit "${exit_code}"
}

skip() {
  echo "$*"
  exit 0
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    die "This updater must run as root so it can manage ${SERVICE_NAME} and the root-owned venv."
  fi
}

take_lock() {
  if ! mkdir "${LOCK_DIR}" >/dev/null 2>&1; then
    skip "Another Python dependency update is already running; skipping."
  fi
  trap cleanup EXIT
}

service_is_active() {
  systemctl is-active --quiet "${SERVICE_NAME}"
}

ensure_idle_if_service_active() {
  if ! service_is_active; then
    echo "${SERVICE_NAME} is not active; updating Python dependencies without starting it."
    return 0
  fi

  echo "${SERVICE_NAME} is active; checking whether it is idle..."
  set +e
  "${PYTHON_BIN}" -m onlysavemevods.python_update check-idle --config "${CONFIG_FILE}"
  local idle_status=$?
  set -e

  case "${idle_status}" in
    0)
      echo "${SERVICE_NAME} is idle; stopping it for dependency update."
      systemctl stop "${SERVICE_NAME}"
      STOPPED_SERVICE=1
      ;;
    1)
      skip "${SERVICE_NAME} is busy; skipping Python dependency update until the next timer run."
      ;;
    2)
      skip "Could not confirm ${SERVICE_NAME} is idle; skipping Python dependency update."
      ;;
    *)
      die "Idle check failed with unexpected exit code ${idle_status}."
      ;;
  esac
}

refresh_console_script_if_stale() {
  local package_spec="$1"
  local script="$2"
  local label="$3"
  local shebang
  local interpreter

  if [[ ! -f "${script}" ]]; then
    return 0
  fi
  IFS= read -r shebang < "${script}" || return 0
  if [[ "${shebang}" != '#!'* ]]; then
    return 0
  fi
  interpreter="${shebang#\#!}"
  interpreter="${interpreter%% *}"
  if [[ -z "${interpreter}" || "${interpreter}" == "/usr/bin/env" || -e "${interpreter}" ]]; then
    return 0
  fi

  echo "Refreshing ${label} console script after venv path migration..."
  "${PYTHON_BIN}" -m pip install --upgrade --force-reinstall --no-deps "${package_spec}"
}

config_enables_transcription() {
  set +e
  "${PYTHON_BIN}" -m onlysavemevods.python_update config-enables-transcription --config "${CONFIG_FILE}"
  local transcription_status=$?
  set -e
  return "${transcription_status}"
}

config_enables_voice_match() {
  set +e
  "${PYTHON_BIN}" -c '
from onlysavemevods.config import ConfigError, load_config, post_stream_setting_enabled_anywhere
import sys
try:
    enabled = post_stream_setting_enabled_anywhere(load_config(sys.argv[1]), "voice_match_enabled")
except ConfigError as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(2)
raise SystemExit(0 if enabled else 1)
' "${CONFIG_FILE}"
  local voice_match_status=$?
  set -e
  return "${voice_match_status}"
}

config_enables_stream_events() {
  set +e
  "${PYTHON_BIN}" -c '
from onlysavemevods.config import ConfigError, load_config, post_stream_setting_enabled_anywhere
import sys
try:
    enabled = post_stream_setting_enabled_anywhere(load_config(sys.argv[1]), "stream_event_detection_enabled")
except ConfigError as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(2)
raise SystemExit(0 if enabled else 1)
' "${CONFIG_FILE}"
  local stream_event_status=$?
  set -e
  return "${stream_event_status}"
}

voice_match_dependency_installed() {
  set +e
  "${PYTHON_BIN}" -c '
from onlysavemevods.voice_match import voice_matcher_status
raise SystemExit(0 if voice_matcher_status().get("available") else 1)
'
  local voice_match_installed_status=$?
  set -e
  return "${voice_match_installed_status}"
}

stream_events_dependency_installed() {
  set +e
  "${PYTHON_BIN}" -c '
from onlysavemevods.content_events import content_event_detector_status
raise SystemExit(0 if content_event_detector_status().get("available") else 1)
'
  local stream_events_installed_status=$?
  set -e
  return "${stream_events_installed_status}"
}

verify_python_dependencies() {
  echo "Checking Python dependency compatibility..."
  "${PYTHON_BIN}" -m pip check
}

update_python_dependencies() {
  cd "${APP_DIR}"

  echo "Upgrading Python packaging tools..."
  "${PYTHON_BIN}" -m pip install --upgrade pip "setuptools<82" wheel

  echo "Upgrading project dependencies with eager dependency resolution..."
  "${PYTHON_BIN}" -m pip install --upgrade --upgrade-strategy eager --editable "${APP_DIR}"

  echo "Upgrading yt-dlp..."
  "${PYTHON_BIN}" -m pip install --upgrade --upgrade-strategy eager "yt-dlp[default]"
  refresh_console_script_if_stale "yt-dlp[default]" "${YTDLP_BIN}" "yt-dlp"

  if [[ -x "${WHISPERX_BIN}" ]]; then
    echo "WhisperX is installed; upgrading it..."
    "${PYTHON_BIN}" -m pip install --upgrade --upgrade-strategy eager whisperx
    refresh_console_script_if_stale "whisperx" "${WHISPERX_BIN}" "WhisperX"
  elif config_enables_transcription; then
    echo "Transcription is enabled; installing/upgrading WhisperX..."
    "${PYTHON_BIN}" -m pip install --upgrade --upgrade-strategy eager whisperx
    refresh_console_script_if_stale "whisperx" "${WHISPERX_BIN}" "WhisperX"
  else
    local transcription_status=$?
    if [[ "${transcription_status}" == "2" ]]; then
      echo "Could not read transcription setting from ${CONFIG_FILE}; skipping WhisperX."
    else
      echo "WhisperX is not installed and transcription is disabled; skipping WhisperX."
    fi
  fi

  if voice_match_dependency_installed; then
    echo "Voice-match dependencies are installed; upgrading them..."
    "${PYTHON_BIN}" -m pip install --upgrade --editable "${APP_DIR}[voice-match]"
  elif config_enables_voice_match; then
    echo "Voice matching is enabled; installing/upgrading voice-match dependencies..."
    "${PYTHON_BIN}" -m pip install --upgrade --editable "${APP_DIR}[voice-match]"
  else
    local voice_match_status=$?
    if [[ "${voice_match_status}" == "2" ]]; then
      echo "Could not read voice-match setting from ${CONFIG_FILE}; skipping voice-match dependencies."
    else
      echo "Voice-match dependencies are not installed and voice matching is disabled; skipping them."
    fi
  fi

  if stream_events_dependency_installed; then
    echo "Stream-events dependencies are installed; upgrading them..."
    "${PYTHON_BIN}" -m pip install --upgrade --editable "${APP_DIR}[stream-events]"
  elif config_enables_stream_events; then
    echo "Content event detection is enabled; installing/upgrading stream-events dependencies..."
    "${PYTHON_BIN}" -m pip install --upgrade --editable "${APP_DIR}[stream-events]"
  else
    local stream_event_status=$?
    if [[ "${stream_event_status}" == "2" ]]; then
      echo "Could not read stream event setting from ${CONFIG_FILE}; skipping stream-events dependencies."
    else
      echo "Stream-events dependencies are not installed and content event detection is disabled; skipping them."
    fi
  fi

  verify_python_dependencies

  chown -R root:root "${VENV_DIR}"
  chmod -R a+rX "${VENV_DIR}"

  "${YTDLP_BIN}" --version >/dev/null
  "${PYTHON_BIN}" -m onlysavemevods --version >/dev/null
}

restart_service_if_needed() {
  if [[ "${STOPPED_SERVICE}" != "1" ]]; then
    return 0
  fi
  echo "Starting ${SERVICE_NAME} after Python dependency update..."
  systemctl start "${SERVICE_NAME}"
  SERVICE_RESTARTED=1
}

require_root
[[ -x "${PYTHON_BIN}" ]] || die "Python venv not found or not executable: ${PYTHON_BIN}"
[[ -d "${APP_DIR}" ]] || die "Application directory not found: ${APP_DIR}"
[[ -f "${CONFIG_FILE}" ]] || die "Config file not found: ${CONFIG_FILE}"
take_lock
ensure_idle_if_service_active
update_python_dependencies
restart_service_if_needed
echo "Python dependency update completed."
