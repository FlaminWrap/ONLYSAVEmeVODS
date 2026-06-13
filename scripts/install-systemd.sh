#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
STAGED_ROOT_DIR=""
DEFAULT_INSTALL_DIR="/opt/onlysavemevods"
INSTALL_DIR="${ONLYSAVEMEVODS_INSTALL_DIR:-${DEFAULT_INSTALL_DIR}}"

APP_DIR="${INSTALL_DIR}/app"
VENV_DIR="${INSTALL_DIR}/.venv"
DENO_INSTALL_DIR="${INSTALL_DIR}/.deno"
CACHE_DIR="${INSTALL_DIR}/.cache"
DOWNLOAD_DIR="${INSTALL_DIR}/downloads"
STATE_DIR="${INSTALL_DIR}/state"
CONFIG_FILE="${INSTALL_DIR}/config.toml"
SECRETS_FILE="${INSTALL_DIR}/secrets.env"
SERVICE_NAME="onlysavemevods.service"
UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}"
SERVICE_USER="${ONLYSAVEMEVODS_USER:-onlysavemevods}"
PYTHON_BIN="${PYTHON:-}"
SKIP_OS_DEPS="${ONLYSAVEMEVODS_SKIP_OS_DEPS:-0}"
SKIP_DENO="${ONLYSAVEMEVODS_SKIP_DENO:-0}"
SKIP_NVIDIA_DEPS="${ONLYSAVEMEVODS_SKIP_NVIDIA_DEPS:-0}"
INSTALL_WHISPERX="${ONLYSAVEMEVODS_INSTALL_WHISPERX:-auto}"
APT_UPDATED=0

die() {
  echo "$*" >&2
  exit 1
}

cleanup_staged_source() {
  if [[ -n "${STAGED_ROOT_DIR}" && -d "${STAGED_ROOT_DIR}" ]]; then
    rm -rf "${STAGED_ROOT_DIR}"
  fi
}

stage_source_if_inside_install_tree() {
  case "${ROOT_DIR}/" in
    "${INSTALL_DIR}/"*)
      STAGED_ROOT_DIR="$(mktemp -d)"
      echo "Staging installer source from ${ROOT_DIR} to ${STAGED_ROOT_DIR}"
      cp -a "${ROOT_DIR}/." "${STAGED_ROOT_DIR}/"
      ROOT_DIR="${STAGED_ROOT_DIR}"
      SCRIPT_DIR="${ROOT_DIR}/scripts"
      trap cleanup_staged_source EXIT
      ;;
  esac
}

script_has_missing_shebang_interpreter() {
  local script="$1"
  local shebang
  local interpreter

  if [[ ! -f "${script}" ]]; then
    return 1
  fi

  IFS= read -r shebang < "${script}" || return 1
  if [[ "${shebang}" != '#!'* ]]; then
    return 1
  fi

  interpreter="${shebang#\#!}"
  interpreter="${interpreter%% *}"
  if [[ "${interpreter}" == "/usr/bin/env" ]]; then
    return 1
  fi
  [[ -n "${interpreter}" && ! -e "${interpreter}" ]]
}

refresh_console_script_if_stale() {
  local package_spec="$1"
  local script="$2"
  local label="$3"

  if ! script_has_missing_shebang_interpreter "${script}"; then
    return 0
  fi

  echo "Refreshing ${label} console script after venv path migration..."
  sudo "${VENV_DIR}/bin/python" -m pip install --upgrade --force-reinstall --no-deps "${package_spec}"
}

python_is_supported() {
  "$1" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1
}

python_has_venv() {
  "$1" -m venv --help >/dev/null 2>&1
}

deno_is_supported() {
  "$1" eval 'const version = Deno.version.deno.split(".").map(Number); Deno.exit(version[0] >= 2 ? 0 : 1)' >/dev/null 2>&1
}

find_python() {
  local candidate
  if [[ -n "${PYTHON_BIN}" ]]; then
    command -v "${PYTHON_BIN}" >/dev/null 2>&1 && python_is_supported "${PYTHON_BIN}" && return 0
    return 1
  fi

  for candidate in python3.13 python3.12 python3.11 python3; do
    if command -v "${candidate}" >/dev/null 2>&1 && python_is_supported "${candidate}"; then
      PYTHON_BIN="${candidate}"
      return 0
    fi
  done
  return 1
}

find_deno() {
  if [[ -x "${DENO_INSTALL_DIR}/bin/deno" ]] && deno_is_supported "${DENO_INSTALL_DIR}/bin/deno"; then
    return 0
  fi

  if command -v deno >/dev/null 2>&1 && deno_is_supported "$(command -v deno)"; then
    return 0
  fi

  return 1
}

ensure_service_user() {
  if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    return 0
  fi

  local nologin_shell="/usr/sbin/nologin"
  if [[ ! -x "${nologin_shell}" ]]; then
    nologin_shell="/sbin/nologin"
  fi

  echo "Creating system user ${SERVICE_USER}..."
  sudo useradd \
    --system \
    --home-dir "${INSTALL_DIR}" \
    --shell "${nologin_shell}" \
    --comment "ONLYSAVEmeVODS service user" \
    "${SERVICE_USER}"
}

install_application_files() {
  local service_group
  service_group="$(id -gn "${SERVICE_USER}")"

  echo "Installing application files to ${APP_DIR}..."
  sudo install -d -m 0755 "${INSTALL_DIR}" "${APP_DIR}"
  sudo install -d -m 0750 -o "${SERVICE_USER}" -g "${service_group}" \
    "${CACHE_DIR}" \
    "${CACHE_DIR}/matplotlib" \
    "${CACHE_DIR}/nltk_data" \
    "${DOWNLOAD_DIR}" \
    "${STATE_DIR}"

  sudo rm -rf \
    "${APP_DIR}/src" \
    "${APP_DIR}/scripts" \
    "${APP_DIR}/tests" \
    "${APP_DIR}/pyproject.toml" \
    "${APP_DIR}/README.md" \
    "${APP_DIR}/config.example.toml"

  sudo cp -a \
    "${ROOT_DIR}/pyproject.toml" \
    "${ROOT_DIR}/README.md" \
    "${ROOT_DIR}/config.example.toml" \
    "${ROOT_DIR}/src" \
    "${ROOT_DIR}/scripts" \
    "${ROOT_DIR}/tests" \
    "${APP_DIR}/"

  sudo chown -R root:root "${APP_DIR}"
  sudo chmod -R go-w "${APP_DIR}"
}

install_or_preserve_config() {
  if [[ -f "${CONFIG_FILE}" ]]; then
    echo "Keeping existing ${CONFIG_FILE}"
    return 0
  fi

  if [[ -f "${ROOT_DIR}/config.toml" ]]; then
    sudo install -m 0644 -o root -g root "${ROOT_DIR}/config.toml" "${CONFIG_FILE}"
    echo "Copied existing config to ${CONFIG_FILE}"
  else
    sudo install -m 0644 -o root -g root "${APP_DIR}/config.example.toml" "${CONFIG_FILE}"
    echo "Created ${CONFIG_FILE}; edit channels before expecting downloads."
  fi
}

generate_watermark_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 48
    return 0
  fi

  "${PYTHON_BIN}" -c 'import secrets; print(secrets.token_urlsafe(48))'
}

install_or_preserve_secrets_file() {
  if [[ -f "${SECRETS_FILE}" ]]; then
    echo "Keeping existing ${SECRETS_FILE}"
    return 0
  fi

  local service_group
  local secret
  service_group="$(id -gn "${SERVICE_USER}")"
  secret="$(generate_watermark_secret)"

  sudo install -m 0640 -o root -g "${service_group}" /dev/null "${SECRETS_FILE}"
  sudo tee "${SECRETS_FILE}" >/dev/null <<EOF
# ONLYSAVEmeVODS service secrets. Back this file up with config.toml and state/.
# Losing ONLYSAVEMEVODS_WATERMARK_SECRET prevents detecting old watermark copies.
ONLYSAVEMEVODS_WATERMARK_SECRET=${secret}
EOF
  sudo chown root:"${service_group}" "${SECRETS_FILE}"
  sudo chmod 0640 "${SECRETS_FILE}"
  echo "Created ${SECRETS_FILE}; back it up with ${CONFIG_FILE} and ${STATE_DIR}/"
}

dnf_install() {
  sudo dnf install -y "$@"
}

try_dnf_install() {
  sudo dnf install -y "$@" >/dev/null 2>&1
}

apt_update_once() {
  if [[ "${APT_UPDATED}" == "1" ]]; then
    return 0
  fi
  sudo apt-get update
  APT_UPDATED=1
}

apt_install() {
  apt_update_once
  sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

try_apt_install() {
  apt_update_once
  sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "$@" >/dev/null 2>&1
}

is_ubuntu() {
  [[ -r /etc/os-release ]] && . /etc/os-release && [[ "${ID:-}" == "ubuntu" ]]
}

enable_ubuntu_universe_if_available() {
  if ! is_ubuntu; then
    return 0
  fi

  echo "Ensuring Ubuntu universe repository is enabled for FFmpeg..."
  apt_install software-properties-common
  sudo add-apt-repository -y universe >/dev/null 2>&1 || true
  APT_UPDATED=0
  apt_update_once
}

enable_almalinux_extra_repos() {
  local include_nonfree="${1:-0}"
  if ! command -v dnf >/dev/null 2>&1; then
    return 0
  fi

  echo "Enabling AlmaLinux/RPM Fusion repositories..."
  dnf_install dnf-plugins-core epel-release distribution-gpg-keys
  sudo dnf config-manager --set-enabled crb >/dev/null 2>&1 || \
    sudo dnf config-manager --set-enabled powertools >/dev/null 2>&1 || true

  local rhel_version
  rhel_version="$(rpm -E %rhel)"
  if [[ -r "/usr/share/distribution-gpg-keys/rpmfusion/RPM-GPG-KEY-rpmfusion-free-el-${rhel_version}" ]]; then
    sudo rpmkeys --import "/usr/share/distribution-gpg-keys/rpmfusion/RPM-GPG-KEY-rpmfusion-free-el-${rhel_version}"
  fi
  if [[ "${include_nonfree}" == "1" ]] && \
    [[ -r "/usr/share/distribution-gpg-keys/rpmfusion/RPM-GPG-KEY-rpmfusion-nonfree-el-${rhel_version}" ]]; then
    sudo rpmkeys --import "/usr/share/distribution-gpg-keys/rpmfusion/RPM-GPG-KEY-rpmfusion-nonfree-el-${rhel_version}"
  fi

  sudo dnf --setopt=localpkg_gpgcheck=1 install -y \
    "https://mirrors.rpmfusion.org/free/el/rpmfusion-free-release-${rhel_version}.noarch.rpm"
  if [[ "${include_nonfree}" == "1" ]]; then
    sudo dnf --setopt=localpkg_gpgcheck=1 install -y \
      "https://mirrors.rpmfusion.org/nonfree/el/rpmfusion-nonfree-release-${rhel_version}.noarch.rpm"
  fi
}

enable_almalinux_extra_repos_for_ffmpeg() {
  echo "Enabling AlmaLinux/RPM Fusion repositories needed for FFmpeg..."
  enable_almalinux_extra_repos 0
}

enable_almalinux_extra_repos_for_nvidia() {
  echo "Enabling AlmaLinux/RPM Fusion repositories needed for NVIDIA/NVENC..."
  enable_almalinux_extra_repos 1
}

has_nvidia_pci_device() {
  local vendor
  for vendor in /sys/bus/pci/devices/*/vendor; do
    if [[ -r "${vendor}" ]] && [[ "$(tr '[:upper:]' '[:lower:]' < "${vendor}")" == "0x10de" ]]; then
      return 0
    fi
  done
  return 1
}

ffmpeg_has_h264_nvenc() {
  command -v ffmpeg >/dev/null 2>&1 && \
    ffmpeg -hide_banner -encoders 2>/dev/null | grep -q 'h264_nvenc'
}

ffmpeg_has_any_nvenc() {
  command -v ffmpeg >/dev/null 2>&1 && \
    ffmpeg -hide_banner -encoders 2>/dev/null | grep -qi 'nvenc'
}

nvidia_smi_reports_gpu() {
  command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L >/dev/null 2>&1
}

log_ffmpeg_nvenc_diagnostics() {
  if command -v ffmpeg >/dev/null 2>&1; then
    echo "FFmpeg binary: $(command -v ffmpeg)" >&2
    ffmpeg -hide_banner -version 2>/dev/null | head -n 2 >&2 || true
  fi
  if command -v rpm >/dev/null 2>&1; then
    rpm -q ffmpeg ffmpeg-free 2>/dev/null >&2 || true
  fi
}

install_or_refresh_ffmpeg_for_nvenc() {
  if ! command -v dnf >/dev/null 2>&1; then
    echo "dnf not found; cannot refresh FFmpeg for NVENC automatically"
    return 0
  fi

  enable_almalinux_extra_repos_for_nvidia
  if command -v rpm >/dev/null 2>&1 && rpm -q ffmpeg-free >/dev/null 2>&1; then
    echo "Replacing ffmpeg-free with full RPM Fusion FFmpeg for NVENC support..."
    sudo dnf swap -y ffmpeg-free ffmpeg || dnf_install ffmpeg
  else
    dnf_install ffmpeg
  fi
  if ffmpeg_has_h264_nvenc; then
    echo "FFmpeg h264_nvenc support is available"
  else
    echo "WARNING: FFmpeg still does not advertise h264_nvenc after refresh" >&2
    log_ffmpeg_nvenc_diagnostics
  fi
}

install_nvidia_dependencies_if_detected() {
  if [[ "${SKIP_NVIDIA_DEPS}" == "1" ]]; then
    echo "Skipping NVIDIA dependency installation because ONLYSAVEMEVODS_SKIP_NVIDIA_DEPS=1"
    return 0
  fi

  if ! has_nvidia_pci_device; then
    echo "No NVIDIA PCI devices detected; skipping NVIDIA/NVENC dependencies"
    return 0
  fi

  echo "NVIDIA PCI device detected; checking NVIDIA/NVENC dependencies..."
  if ffmpeg_has_any_nvenc; then
    echo "FFmpeg already advertises NVENC encoders; leaving NVIDIA driver packages unchanged"
    if ! ffmpeg_has_h264_nvenc; then
      echo "WARNING: FFmpeg advertises NVENC, but not h264_nvenc required by chat rendering" >&2
    fi
    return 0
  fi

  if command -v nvidia-smi >/dev/null 2>&1; then
    if nvidia_smi_reports_gpu; then
      echo "nvidia-smi is installed and reports GPUs; leaving NVIDIA driver packages unchanged"
      if ffmpeg_has_h264_nvenc; then
        echo "NVIDIA driver and FFmpeg h264_nvenc support already available"
      else
        install_or_refresh_ffmpeg_for_nvenc
      fi
    else
      echo "WARNING: nvidia-smi is installed but is not reporting GPUs; leaving NVIDIA driver packages unchanged" >&2
    fi
    return 0
  fi

  if ! command -v dnf >/dev/null 2>&1; then
    echo "dnf not found; cannot install NVIDIA/NVENC dependencies automatically"
    return 0
  fi

  enable_almalinux_extra_repos_for_nvidia
  echo "Installing NVIDIA driver/CUDA runtime packages for NVENC..."
  if sudo dnf install -y akmod-nvidia xorg-x11-drv-nvidia-cuda; then
    sudo dnf install -y ffmpeg
  else
    echo "WARNING: NVIDIA dependency installation failed; continuing without NVENC setup" >&2
    return 0
  fi

  if ffmpeg_has_h264_nvenc; then
    echo "FFmpeg h264_nvenc support is available"
  else
    echo "WARNING: FFmpeg still does not advertise h264_nvenc after NVIDIA setup" >&2
  fi

  if ! nvidia_smi_reports_gpu; then
    echo "WARNING: nvidia-smi is not reporting GPUs yet; a reboot may be required after driver installation" >&2
  fi
}

install_nvidia_dependencies_if_detected_apt() {
  if [[ "${SKIP_NVIDIA_DEPS}" == "1" ]]; then
    echo "Skipping NVIDIA dependency checks because ONLYSAVEMEVODS_SKIP_NVIDIA_DEPS=1"
    return 0
  fi

  if ! has_nvidia_pci_device; then
    echo "No NVIDIA PCI devices detected; skipping NVIDIA/NVENC dependency notes"
    return 0
  fi

  echo "NVIDIA PCI device detected."
  if ffmpeg_has_any_nvenc; then
    echo "FFmpeg already advertises NVENC encoders; leaving NVIDIA driver packages unchanged"
    if ! ffmpeg_has_h264_nvenc; then
      echo "WARNING: FFmpeg advertises NVENC, but not h264_nvenc required by chat rendering" >&2
    fi
    return 0
  fi

  echo "WARNING: apt-based installer does not install NVIDIA drivers automatically." >&2
  echo "Install the distro-recommended NVIDIA driver/CUDA encode packages if you want NVENC chat rendering." >&2
}

install_dnf_os_dependencies() {
  if ! command -v dnf >/dev/null 2>&1; then
    return 0
  fi

  echo "Installing OS dependencies with dnf..."
  sudo dnf install -y systemd curl ca-certificates unzip dejavu-sans-fonts

  if ! find_python; then
    if try_dnf_install python3.13 python3.13-pip; then
      PYTHON_BIN="python3.13"
    elif try_dnf_install python3.12 python3.12-pip; then
      PYTHON_BIN="python3.12"
    elif try_dnf_install python3.11 python3.11-pip; then
      PYTHON_BIN="python3.11"
    else
      dnf_install python3 python3-pip
      PYTHON_BIN="python3"
    fi
  fi

  if ! command -v ffmpeg >/dev/null 2>&1; then
    enable_almalinux_extra_repos_for_ffmpeg
    dnf_install ffmpeg
  fi

  install_nvidia_dependencies_if_detected
}

ensure_apt_python_with_venv() {
  if find_python && python_has_venv "${PYTHON_BIN}"; then
    return 0
  fi

  local version
  for version in 3.13 3.12 3.11; do
    if try_apt_install "python${version}" "python${version}-venv"; then
      if command -v "python${version}" >/dev/null 2>&1 && \
        python_is_supported "python${version}" && \
        python_has_venv "python${version}"; then
        PYTHON_BIN="python${version}"
        return 0
      fi
    fi
  done

  if [[ -n "${PYTHON_BIN}" ]]; then
    local python_name
    python_name="${PYTHON_BIN##*/}"
    case "${python_name}" in
      python3.13|python3.12|python3.11)
        try_apt_install "${python_name}-venv" || true
        ;;
      python3)
        try_apt_install python3-venv || true
        ;;
    esac
    if python_is_supported "${PYTHON_BIN}" && python_has_venv "${PYTHON_BIN}"; then
      return 0
    fi
  fi

  apt_install python3 python3-venv python3-pip
  if command -v python3 >/dev/null 2>&1 && python_is_supported python3 && python_has_venv python3; then
    PYTHON_BIN="python3"
    return 0
  fi

  return 1
}

install_apt_os_dependencies() {
  if ! command -v apt-get >/dev/null 2>&1; then
    return 0
  fi

  echo "Installing OS dependencies with apt..."
  apt_install systemd curl ca-certificates unzip fonts-dejavu-core

  if ! ensure_apt_python_with_venv; then
    echo "WARNING: Could not install or find Python 3.11+ with venv support via apt" >&2
  fi

  if ! command -v ffmpeg >/dev/null 2>&1; then
    enable_ubuntu_universe_if_available
    apt_install ffmpeg
  fi

  install_nvidia_dependencies_if_detected_apt
}

install_os_dependencies() {
  if [[ "${SKIP_OS_DEPS}" == "1" ]]; then
    echo "Skipping OS dependency installation because ONLYSAVEMEVODS_SKIP_OS_DEPS=1"
    return 0
  fi

  if command -v dnf >/dev/null 2>&1; then
    install_dnf_os_dependencies
  elif command -v apt-get >/dev/null 2>&1; then
    install_apt_os_dependencies
  else
    echo "No supported OS package manager found; skipping OS dependency installation"
  fi
}

install_deno_runtime() {
  if [[ "${SKIP_DENO}" == "1" ]]; then
    echo "Skipping Deno installation because ONLYSAVEMEVODS_SKIP_DENO=1"
    return 0
  fi

  if find_deno; then
    return 0
  fi

  if ! command -v curl >/dev/null 2>&1; then
    die "curl is required to install Deno. Rerun without ONLYSAVEMEVODS_SKIP_OS_DEPS=1 or install curl first."
  fi

  echo "Installing Deno runtime for yt-dlp EJS support..."
  local installer
  installer="$(mktemp)"
  curl -fsSL https://deno.land/install.sh -o "${installer}"
  sudo install -d -m 0755 "${DENO_INSTALL_DIR}"
  sudo env DENO_INSTALL="${DENO_INSTALL_DIR}" sh "${installer}"
  rm -f "${installer}"
  sudo chown -R root:root "${DENO_INSTALL_DIR}"
  sudo chmod -R a+rX "${DENO_INSTALL_DIR}"

  if ! find_deno; then
    die "Deno install completed but no supported deno executable was found"
  fi
}

config_enables_transcription() {
  sudo "${VENV_DIR}/bin/python" -c '
from onlysavemevods.config import load_config
config = load_config("'"${CONFIG_FILE}"'")
raise SystemExit(0 if config.transcribe_subtitles else 1)
'
}

config_whisperx_path() {
  sudo "${VENV_DIR}/bin/python" -c '
from onlysavemevods.config import load_config
print(load_config("'"${CONFIG_FILE}"'").whisperx_path)
'
}

should_install_whisperx() {
  case "${INSTALL_WHISPERX}" in
    1|true|yes|always)
      return 0
      ;;
    0|false|no|never)
      return 1
      ;;
    auto|"")
      config_enables_transcription
      return $?
      ;;
    *)
      die "ONLYSAVEMEVODS_INSTALL_WHISPERX must be auto, 1, or 0"
      ;;
  esac
}

install_whisperx_if_needed() {
  if ! should_install_whisperx; then
    if [[ "${INSTALL_WHISPERX}" == "auto" || -z "${INSTALL_WHISPERX}" ]]; then
      echo "Transcription disabled; skipping WhisperX installation"
    else
      echo "Skipping WhisperX installation because ONLYSAVEMEVODS_INSTALL_WHISPERX=${INSTALL_WHISPERX}"
    fi
    return 0
  fi

  echo "Installing WhisperX into ${VENV_DIR}..."
  sudo "${VENV_DIR}/bin/python" -m pip install --upgrade whisperx
  refresh_console_script_if_stale "whisperx" "${VENV_DIR}/bin/whisperx" "WhisperX"
  sudo chown -R root:root "${VENV_DIR}"
  sudo chmod -R a+rX "${VENV_DIR}"

  local configured_path
  configured_path="$(config_whisperx_path)"
  if [[ "${configured_path}" != "whisperx" && "${configured_path}" != "${VENV_DIR}/bin/whisperx" ]]; then
    echo "WARNING: whisperx_path is configured as ${configured_path}; the installer installed ${VENV_DIR}/bin/whisperx" >&2
  fi
  if [[ ! -x "${VENV_DIR}/bin/whisperx" ]]; then
    die "WhisperX install completed but ${VENV_DIR}/bin/whisperx was not found"
  fi
}

stage_source_if_inside_install_tree
install_os_dependencies

if ! find_python; then
  die "Python 3.11+ is required. Rerun without ONLYSAVEMEVODS_SKIP_OS_DEPS=1 so the installer can install Python with dnf/apt, or install Python 3.11+ with venv support manually."
fi

if ! command -v ffmpeg >/dev/null 2>&1; then
  die "ffmpeg is required but was not found. Rerun without ONLYSAVEMEVODS_SKIP_OS_DEPS=1 so the installer can install it with dnf/apt, or install FFmpeg manually."
fi

install_deno_runtime
ensure_service_user
install_application_files

cd "${APP_DIR}"
sudo "${PYTHON_BIN}" -m venv "${VENV_DIR}"

sudo "${VENV_DIR}/bin/python" -m pip install --upgrade pip setuptools wheel
sudo "${VENV_DIR}/bin/python" -m pip install --upgrade "yt-dlp[default]"
refresh_console_script_if_stale "yt-dlp[default]" "${VENV_DIR}/bin/yt-dlp" "yt-dlp"

if ! sudo "${VENV_DIR}/bin/python" -m pip install --editable "${APP_DIR}"; then
  echo "pip editable install failed; falling back to local .pth wrapper install" >&2
  SITE_PACKAGES="$("${VENV_DIR}/bin/python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
  printf '%s\n' "${APP_DIR}/src" | sudo tee "${SITE_PACKAGES}/onlysavemevods-local.pth" >/dev/null
  sudo tee "${VENV_DIR}/bin/onlysavemevods" >/dev/null <<EOF
#!/usr/bin/env bash
PYTHONPATH="${APP_DIR}/src\${PYTHONPATH:+:\${PYTHONPATH}}" exec "${VENV_DIR}/bin/python" -m onlysavemevods "\$@"
EOF
  sudo chmod +x "${VENV_DIR}/bin/onlysavemevods"
fi
sudo chown -R root:root "${VENV_DIR}"
sudo chmod -R a+rX "${VENV_DIR}"

"${VENV_DIR}/bin/yt-dlp" --version >/dev/null
ffmpeg -version >/dev/null
if ! find_deno; then
  die "Deno 2.0+ is required for yt-dlp EJS support. Remove ONLYSAVEMEVODS_SKIP_DENO=1 or install Deno 2.0+ on PATH."
fi

install_or_preserve_config
install_or_preserve_secrets_file
sudo "${VENV_DIR}/bin/python" -m onlysavemevods update-config \
  --config "${CONFIG_FILE}" \
  --defaults "${APP_DIR}/config.example.toml"
install_whisperx_if_needed

sudo tee "${UNIT_FILE}" >/dev/null <<EOF
[Unit]
Description=ONLYSAVEmeVODS YouTube live stream downloader
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
Environment=HOME=${INSTALL_DIR}
Environment=XDG_CACHE_HOME=${CACHE_DIR}
Environment=DENO_DIR=${CACHE_DIR}/deno
Environment=MPLCONFIGDIR=${CACHE_DIR}/matplotlib
Environment=NLTK_DATA=${CACHE_DIR}/nltk_data
Environment=PATH=${VENV_DIR}/bin:${DENO_INSTALL_DIR}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin
EnvironmentFile=${SECRETS_FILE}
ExecStart=${VENV_DIR}/bin/onlysavemevods run --config ${CONFIG_FILE}
Restart=always
RestartSec=15
KillSignal=SIGINT
TimeoutStopSec=45
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${CACHE_DIR} ${DOWNLOAD_DIR} ${STATE_DIR}

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "${SERVICE_NAME}" --now
sudo systemctl restart "${SERVICE_NAME}"

echo "Installed and restarted ${SERVICE_NAME}"
echo "Install dir: ${INSTALL_DIR}"
echo "Service user: ${SERVICE_USER}"
echo "Status: sudo systemctl status ${SERVICE_NAME}"
echo "Logs:   journalctl -u ${SERVICE_NAME} -f"
