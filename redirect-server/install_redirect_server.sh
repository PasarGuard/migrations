#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="PasarGuard"
REPO_NAME="migrations"
SERVICE_NAME="redirect-server"
SERVICE_DESCRIPTION="Subscription URL Redirect Server"
SERVICE_USER="redirectsrv"
INSTALL_DIR="/opt/${SERVICE_NAME}"
CONFIG_DIR="/etc/${SERVICE_NAME}"
BIN_PATH="/usr/local/bin/${SERVICE_NAME}"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

VERSION="${1:-latest}"
ASSET_NAME_OVERRIDE="${ASSET_NAME_OVERRIDE:-${ASSET_NAME:-}}"

require_root() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "This installer must be run as root." >&2
    exit 1
  fi
}

PACKAGE_MANAGER=""
APT_UPDATED=0

detect_package_manager() {
  if [[ -n "${PACKAGE_MANAGER}" ]]; then
    return
  fi
  if command -v apt-get >/dev/null 2>&1; then
    PACKAGE_MANAGER="apt"
  elif command -v dnf >/dev/null 2>&1; then
    PACKAGE_MANAGER="dnf"
  elif command -v yum >/dev/null 2>&1; then
    PACKAGE_MANAGER="yum"
  elif command -v pacman >/dev/null 2>&1; then
    PACKAGE_MANAGER="pacman"
  elif command -v zypper >/dev/null 2>&1; then
    PACKAGE_MANAGER="zypper"
  elif command -v apk >/dev/null 2>&1; then
    PACKAGE_MANAGER="apk"
  else
    PACKAGE_MANAGER="unknown"
  fi
}

install_package() {
  local pkg="$1"
  detect_package_manager
  case "${PACKAGE_MANAGER}" in
    apt)
      if [[ "${APT_UPDATED}" -eq 0 ]]; then
        apt-get update
        APT_UPDATED=1
      fi
      apt-get install -y "${pkg}"
      ;;
    dnf)
      dnf install -y "${pkg}"
      ;;
    yum)
      yum install -y "${pkg}"
      ;;
    pacman)
      pacman -Sy --noconfirm "${pkg}"
      ;;
    zypper)
      zypper install -y "${pkg}"
      ;;
    apk)
      apk add --no-cache "${pkg}"
      ;;
    *)
      echo "Missing command '${pkg}' and no supported package manager found. Please install it manually." >&2
      exit 1
      ;;
  esac
}

ensure_cmd() {
  local cmd="$1"
  local pkg="${2:-$1}"
  if command -v "${cmd}" >/dev/null 2>&1; then
    return
  fi
  echo "Command '${cmd}' not found. Attempting to install '${pkg}'..."
  install_package "${pkg}"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Failed to install command '${cmd}'. Please install it manually and rerun the installer." >&2
    exit 1
  fi
}

detect_target_os() {
  case "$(uname -s)" in
    Linux)  echo "linux" ;;
    *)      echo "Unsupported OS for this installer (requires Linux with systemd)." >&2; exit 1 ;;
  esac
}

detect_target_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    armv7l|armv7hf) echo "armv7" ;;
    *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
  esac
}

fetch_release_metadata() {
  local api_url
  if [[ "${VERSION}" == "latest" ]]; then
    api_url="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/latest"
  else
    api_url="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/tags/${VERSION}"
  fi
  curl -fsSL "${api_url}"
}

select_asset() {
  local override="${ASSET_NAME_OVERRIDE}"
  if [[ -n "${override}" ]]; then
    if ! jq -er --arg name "${override}" '
        .assets
        | map(select(.name == $name))
        | .[0]
        | "\(.browser_download_url)\n\(.name)"
      '; then
      echo "Could not find asset named '${override}' in the release." >&2
      return 1
    fi
    return 0
  fi

  if ! jq -er \
      --arg os "${TARGET_OS}" \
      --arg arch_pattern "${ASSET_ARCH_PATTERN}" '
        .assets
        | map(select((.name | ascii_downcase) | contains($os)))
        | map(select((.name | ascii_downcase) | test($arch_pattern)))
        | .[0]
        | "\(.browser_download_url)\n\(.name)"
      '; then
    echo "Could not find an asset matching OS '${TARGET_OS}' and arch '${TARGET_ARCH}'." >&2
    return 1
  fi
}

create_system_user() {
  if id -u "${SERVICE_USER}" >/dev/null 2>&1; then
    return
  fi

  useradd \
    --system \
    --home "${INSTALL_DIR}" \
    --shell /usr/sbin/nologin \
    --comment "${SERVICE_DESCRIPTION}" \
    "${SERVICE_USER}"
}

install_service_file() {
  cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=${SERVICE_DESCRIPTION}
After=network.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
ExecStart=${BIN_PATH} --config ${CONFIG_DIR}/config.json --map ${CONFIG_DIR}/subscription_url_mapping.json
Restart=on-failure
RestartSec=5s
Environment=CONFIG_PATH=${CONFIG_DIR}/config.json
Environment=MAP_PATH=${CONFIG_DIR}/subscription_url_mapping.json

[Install]
WantedBy=multi-user.target
EOF
}

ensure_default_configs() {
  if [[ ! -f "${CONFIG_DIR}/config.json" ]]; then
    cat > "${CONFIG_DIR}/config.json" <<'EOF'
{
  "host": "0.0.0.0",
  "port": 8080,
  "redirect_domain": "",
  "ssl": {
    "enabled": false,
    "cert": "",
    "key": ""
  }
}
EOF
  fi

  if [[ ! -f "${CONFIG_DIR}/subscription_url_mapping.json" ]]; then
    cat > "${CONFIG_DIR}/subscription_url_mapping.json" <<'EOF'
{
  "generated_at": "",
  "total_users": 0,
  "mapped_users": 0,
  "not_found_users": 0,
  "url_formats": {
    "old_format": "",
    "new_format": ""
  },
  "mappings": {}
}
EOF
  fi
}

reload_and_start_service() {
  systemctl daemon-reload
  systemctl enable --now "${SERVICE_NAME}.service"
  systemctl status "${SERVICE_NAME}.service" --no-pager
}

main() {
  require_root
  ensure_cmd curl
  ensure_cmd jq
  ensure_cmd tar
  ensure_cmd readlink coreutils

  if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl is required but not found. This installer targets systemd-based systems." >&2
    exit 1
  fi

  local target_os target_arch arch_pattern release_json asset_info asset_url asset_name tmp_dir bin_source

  target_os="$(detect_target_os)"
  target_arch="$(detect_target_arch)"
  arch_pattern="${target_arch}"
  case "${target_arch}" in
    amd64) arch_pattern="amd64|x86_64" ;;
    arm64) arch_pattern="arm64|aarch64" ;;
    armv7) arch_pattern="armv7|armhf|armv7l" ;;
  esac
  export TARGET_OS="${target_os}"
  export TARGET_ARCH="${target_arch}"
  export ASSET_ARCH_PATTERN="${arch_pattern}"
  export ASSET_NAME_OVERRIDE

  release_json="$(fetch_release_metadata)"
  mapfile -t asset_info < <(printf '%s' "${release_json}" | select_asset)

  asset_url="${asset_info[0]:-}"
  asset_name="${asset_info[1]:-}"
  if [[ -z "${asset_url}" || -z "${asset_name}" ]]; then
    echo "Unable to resolve a download URL for the release asset." >&2
    exit 1
  fi

  echo "Downloading ${asset_name} from GitHub releases..."

  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "${tmp_dir}"' EXIT

  curl -fsSLo "${tmp_dir}/${asset_name}" "${asset_url}"

  pushd "${tmp_dir}" >/dev/null
  bin_source=""
  case "${asset_name}" in
    *.tar.gz|*.tgz)
      tar -xzf "${asset_name}"
      ;;
    *.tar.xz|*.txz)
      tar -xJf "${asset_name}"
      ;;
    *.tar)
      tar -xf "${asset_name}"
      ;;
    *.zip)
      ensure_cmd unzip
      unzip -q "${asset_name}"
      ;;
    *)
      chmod +x "${asset_name}" 2>/dev/null || true
      if [[ -x "${asset_name}" ]]; then
        bin_source="$(pwd)/${asset_name}"
      else
        echo "Unsupported archive format for ${asset_name}. Expected a tar/zip archive or raw executable." >&2
        exit 1
      fi
      ;;
  esac

  if [[ -z "${bin_source}" ]]; then
    bin_source="$(find . -maxdepth 3 -type f -perm -111 -name "${SERVICE_NAME}" | head -n 1)"
    if [[ -z "${bin_source}" ]]; then
      echo "Failed to locate the '${SERVICE_NAME}' binary inside the downloaded archive." >&2
      exit 1
    fi
  fi
  bin_source="$(readlink -f "${bin_source}")"
  popd >/dev/null

  install -d -m 0755 "${INSTALL_DIR}"
  install -d -m 0750 "${CONFIG_DIR}"
  install -D -m 0755 "${bin_source}" "${BIN_PATH}"

  create_system_user
  chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}" "${CONFIG_DIR}"

  ensure_default_configs
  chown "${SERVICE_USER}:${SERVICE_USER}" "${CONFIG_DIR}/config.json" "${CONFIG_DIR}/subscription_url_mapping.json"
  chmod 0640 "${CONFIG_DIR}/config.json" "${CONFIG_DIR}/subscription_url_mapping.json"

  install_service_file
  chmod 0644 "${SERVICE_FILE}"

  reload_and_start_service

  echo "Installation complete."
  echo
  echo "Update ${CONFIG_DIR}/config.json and ${CONFIG_DIR}/subscription_url_mapping.json as needed, then run:"
  echo "  sudo systemctl restart ${SERVICE_NAME}"
}

main "$@"
