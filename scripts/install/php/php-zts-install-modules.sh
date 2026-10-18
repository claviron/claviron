#!/usr/bin/env bash
set -euo pipefail

# PECL extensions for the PHP ZTS runtime `php-zts-install.sh` provisions.
# Linux and FreeBSD servers only: the runtime is root-owned under
# /opt/php-zts-embed and services load it read-only. PHP_OWNER, PHP_GROUP and
# PHP_INSTALL_DIR override the defaults.
if [[ "$(uname -s)" == "Darwin" ]]; then
  echo "ERROR: this script is for Linux/FreeBSD servers only." >&2
  exit 2
fi
PHP_OWNER="${PHP_OWNER:-root}"
PHP_GROUP="${PHP_GROUP:-root}"

PHP_DIR="${PHP_INSTALL_DIR:-/opt/php-zts-embed}"

SUDO_READY=0

PHP="$PHP_DIR/bin/php"
PHPIZE="$PHP_DIR/bin/phpize"
PHP_CONFIG="$PHP_DIR/bin/php-config"
PECL="$PHP_DIR/bin/pecl"
PHP_INI="${PHP_INI:-$PHP_DIR/etc/php.ini}"
PHP_SCAN_ROOT="${PHP_SCAN_ROOT:-$PHP_DIR/etc/conf.d}"
PHP_AVAILABLE_SCAN_DIR="${PHP_AVAILABLE_SCAN_DIR:-$PHP_SCAN_ROOT/available}"
PHP_EMBED_SCAN_DIR="${PHP_EMBED_SCAN_DIR:-$PHP_SCAN_ROOT/embed}"
PHP_CLI_SCAN_DIR="${PHP_CLI_SCAN_DIR:-$PHP_SCAN_ROOT/cli}"
#PHP_SCAN_DIR="$PHP_EMBED_SCAN_DIR"
PHP_EXT_DIR="${PHP_EXT_DIR:-}"
SKIP_APT="${SKIP_APT:-0}"
PECL_CHANNEL_REFRESHED=0

LOCAL_BIN="${LOCAL_BIN:-$HOME/.local/bin}"

PATH="$LOCAL_BIN:$PHP_DIR/bin:$PATH"
export PHPRC="$PHP_INI"
export PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR"
export PHP_PEAR_PHP_BIN="$PHP"
export PHP_PEAR_INSTALL_DIR="$PHP_DIR/lib/php"

REQUIRED_COMMANDS=(
  php
  phpize
  php-config
  pecl
  gcc
  autoconf
  pkg-config
)

# Request runtime extensions. These are enabled for both the embedded SAPI
# and CLI because PHP requests may use them directly under Claviron.
EXTENSIONS=(
  redis
  igbinary
)

# Development tooling for the standalone PHP CLI. Claviron tasks, daemons,
# applications, and request scripts all execute through libphp's embed SAPI;
# runtime capabilities therefore do not belong in this list.
#protobuf removed - 19.09.2026
#grpc removed - 19.09.2026
CLI_ONLY_EXTENSIONS=(
  parallel
)
# sync not 8.5 yet

# Installed only by its explicit command, never by install or install-optional.
MANUAL_CLI_ONLY_EXTENSIONS=(
  xdebug
)

# Explicitly opted-in standalone CLI capabilities. Claviron does not depend on
# these for async execution: scheduling, wake-up, admission, and fleet
# coordination remain owned by Rust and Orbit.
OPTIONAL_CLI_ONLY_EXTENSIONS=(
  event
  uv
  msgpack
  zstd
  # parallel
)

DEBIAN_PACKAGES=(
  build-essential
  autoconf
  pkg-config
  re2c
  libpcre2-dev
  libzstd-dev
  libevent-dev
  libuv1-dev
)

ALMALINUX_PACKAGES=(
  gcc
  gcc-c++
  make
  autoconf
  pkgconf-pkg-config
  re2c
  pcre2-devel
  libzstd-devel
  libevent-devel
  libuv-devel
)

FREEBSD_PACKAGES=(
  gmake
  autoconf
  pkgconf
  re2c
  pcre2
  zstd
  libevent
  libuv
)

log() { printf '%s\n' "INFO: $*"; }
ok() { printf '%s\n' "OK: $*"; }
warn() { printf '%s\n' "WARN: $*" >&2; }
die() {
  printf '%s\n' "ERROR: $*" >&2
  exit 1
}

if [[ -t 1 ]]; then
  GREEN=$'\033[32m'
  RED=$'\033[31m'
  YELLOW=$'\033[33m'
  RESET=$'\033[0m'
else
  GREEN=""
  RED=""
  YELLOW=""
  RESET=""
fi

ensure_sudo() {
  if [[ "$SUDO_READY" == "1" ]]; then
    return 0
  fi

  command -v sudo >/dev/null 2>&1 || die "this step needs root privileges; run as root or as a user with sudo"
  sudo -v || die "sudo authentication failed; run as root or use a user with sudo"
  SUDO_READY=1
}

root_cmd() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  else
    ensure_sudo
    sudo "$@"
  fi
}

detect_os() {
  case "$(uname -s)" in
  Linux) echo linux ;;
  FreeBSD) echo freebsd ;;
  *) die "unsupported OS: $(uname -s)" ;;
  esac
}

detect_linux_family() {
  if [[ -r /etc/os-release ]]; then
    . /etc/os-release
    case " ${ID:-} ${ID_LIKE:-} " in
    *" debian "* | *" ubuntu "*) echo debian ;;
    *" rhel "* | *" fedora "* | *" centos "* | *" almalinux "* | *" rocky "*) echo rhel ;;
    *) echo unknown ;;
    esac
  else
    echo unknown
  fi
}

require_executable() {
  local path="$1"
  local label="$2"
  [[ -x "$path" ]] || die "$label not found or not executable: $path"
}

scan_dir_for_mode() {
  case "${1:-embed}" in
  embed) echo "$PHP_EMBED_SCAN_DIR" ;;
  cli) echo "$PHP_CLI_SCAN_DIR" ;;
  *) die "unknown scan mode: $1" ;;
  esac
}

is_loaded() {
  local ext="$1"
  local mode="${2:-embed}"
  local scan_dir

  if [[ "$mode" == "both" ]]; then
    is_loaded "$ext" embed && is_loaded "$ext" cli
    return
  fi

  scan_dir="$(scan_dir_for_mode "$mode")"

  PHPRC="$PHP_INI" PHP_INI_SCAN_DIR="$scan_dir" "$PHP" -m | tr -d '\r' | awk -v ext="$ext" '
    BEGIN { IGNORECASE=1 }
    $0 == ext { found=1 }
    END { exit(found ? 0 : 1) }
  '
}

ini_path_for() {
  local ext="$1"
  local scan_dir="$2"

  printf '%s/50-%s.ini\n' "$scan_dir" "$ext"
}

ini_enabled() {
  local ext="$1"
  local scan_dir="$2"
  local ini_path

  ini_path="$(ini_path_for "$ext" "$scan_dir")"
  [[ -e "$ini_path" ]]
}

ini_absent() {
  local ext="$1"
  local scan_dir="$2"
  local ini_path

  ini_path="$(ini_path_for "$ext" "$scan_dir")"
  [[ ! -e "$ini_path" && ! -L "$ini_path" ]]
}

extension_so_exists() {
  local ext="$1"

  [[ -f "$PHP_EXT_DIR/${ext}.so" ]]
}

resolve_php_ext_dir() {
  if [[ -z "$PHP_EXT_DIR" ]]; then
    PHP_EXT_DIR="$("$PHP_CONFIG" --extension-dir)"
  fi
}

php_no_extensions() {
  PHPRC="$PHP_INI" PHP_INI_SCAN_DIR= "$PHP" -n "$@"
}

check_php_build_contract() {
  local php_vernum
  local config_vernum
  local zts
  local sapis

  php_vernum="$(php_no_extensions -r 'echo PHP_VERSION_ID;')"
  config_vernum="$("$PHP_CONFIG" --vernum)"
  zts="$(php_no_extensions -r 'echo (defined("PHP_ZTS") && PHP_ZTS) ? "1" : "0";')"
  sapis="$("$PHP_CONFIG" --php-sapis)"

  [[ "$php_vernum" == "$config_vernum" ]] || die "php/php-config version mismatch: php=$php_vernum php-config=$config_vernum"
  [[ "$zts" == "1" ]] || die "PHP is not a ZTS build: $PHP"

  case " $sapis " in
  *" cli "* | *" cli"*)
    ;;
  *) die "PHP build is missing cli SAPI: $sapis" ;;
  esac

  case " $sapis " in
  *" embed "* | *" embed"*)
    ;;
  *) die "PHP build is missing embed SAPI: $sapis" ;;
  esac
}

check_environment() {
  require_executable "$PHP" "php"
  require_executable "$PHPIZE" "phpize"
  require_executable "$PHP_CONFIG" "php-config"
  require_executable "$PECL" "pecl"
  check_php_build_contract
  resolve_php_ext_dir

  for cmd in "${REQUIRED_COMMANDS[@]}"; do
    command -v "$cmd" >/dev/null 2>&1 || die "missing command: $cmd"
  done

  if ! command -v make >/dev/null 2>&1 && ! command -v gmake >/dev/null 2>&1; then
    die "missing command: make or gmake"
  fi

  [[ -n "$PHP_EXT_DIR" ]] || die "cannot determine extension dir"
}

repair_pear_paths() {
  local pear="$PHP_DIR/bin/pear"

  export PHP_PEAR_PHP_BIN="$PHP"
  export PHP_PEAR_INSTALL_DIR="$PHP_DIR/lib/php"

  if [[ -x "$pear" ]]; then
    root_cmd env PHPRC="$PHP_INI" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" \
      PHP_PEAR_PHP_BIN="$PHP" PHP_PEAR_INSTALL_DIR="$PHP_DIR/lib/php" \
      "$pear" config-set php_bin "$PHP" system >/dev/null || true
    root_cmd env PHPRC="$PHP_INI" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" \
      PHP_PEAR_PHP_BIN="$PHP" PHP_PEAR_INSTALL_DIR="$PHP_DIR/lib/php" \
      "$pear" config-set php_dir "$PHP_DIR/lib/php" system >/dev/null || true
  fi
}

ensure_extension_dir() {
  if [[ -d "$PHP_EXT_DIR" ]]; then
    return 0
  fi

  root_cmd mkdir -p "$PHP_EXT_DIR"
}

configure_freebsd_ca() {
  local system_ca="/etc/ssl/cert.pem"
  local private_ca="${PHP_PRIVATE_CA:-/etc/certstore/cacert.pem}"
  local ca_bundle="$PHP_DIR/etc/ca-bundle.pem"
  local ca_file="$system_ca"
  local ca_ini="$PHP_AVAILABLE_SCAN_DIR/10-ca.ini"

  [[ "$(detect_os)" == "freebsd" ]] || return 0
  [[ -r "$system_ca" ]] || die "FreeBSD CA bundle not found: $system_ca"

  root_cmd mkdir -p "$PHP_AVAILABLE_SCAN_DIR" "$PHP_EMBED_SCAN_DIR" "$PHP_CLI_SCAN_DIR"
  if [[ -r "$private_ca" ]]; then
    log "adding Roma authority to the PHP CA bundle"
    cat "$system_ca" "$private_ca" | root_cmd tee "$ca_bundle" >/dev/null
    ca_file="$ca_bundle"
  fi

  printf 'openssl.cafile=%s\n' "$ca_file" | root_cmd tee "$ca_ini" >/dev/null
  link_ini "$ca_ini" "$PHP_EMBED_SCAN_DIR"
  link_ini "$ca_ini" "$PHP_CLI_SCAN_DIR"
}

reset_scan_dirs() {
  root_cmd mkdir -p "$PHP_AVAILABLE_SCAN_DIR" "$PHP_EMBED_SCAN_DIR" "$PHP_CLI_SCAN_DIR"
}

extension_is_listed() {
  local wanted="$1"
  shift
  local ext

  for ext in "$@"; do
    [[ "$ext" == "$wanted" ]] && return 0
  done
  return 1
}

reconcile_scan_dir() {
  local scan_dir="$1"
  shift
  local ini_path
  local ini_name
  local ext

  while IFS= read -r -d '' ini_path; do
    ini_name="$(basename "$ini_path")"
    ext="${ini_name#50-}"
    ext="${ext%.ini}"
    if ! extension_is_listed "$ext" "$@"; then
      root_cmd rm -f "$ini_path"
    fi
  done < <(find "$scan_dir" -maxdepth 1 -type l -name '50-*.ini' -print0)
}

reconcile_available_extensions() {
  local ini_path
  local ini_name
  local ext

  while IFS= read -r -d '' ini_path; do
    ini_name="$(basename "$ini_path")"
    ext="${ini_name#50-}"
    ext="${ext%.ini}"
    if [[ ! -f "$PHP_EXT_DIR/${ext}.so" ]]; then
      root_cmd rm -f \
        "$ini_path" \
        "$PHP_EMBED_SCAN_DIR/$ini_name" \
        "$PHP_CLI_SCAN_DIR/$ini_name"
    fi
  done < <(find "$PHP_AVAILABLE_SCAN_DIR" -maxdepth 1 -type f -name '50-*.ini' -print0)
}

reconcile_scan_profiles() {
  reset_scan_dirs
  reconcile_available_extensions
  reconcile_scan_dir "$PHP_EMBED_SCAN_DIR" "${EXTENSIONS[@]}"
  reconcile_scan_dir "$PHP_CLI_SCAN_DIR" \
    "${EXTENSIONS[@]}" \
    "${CLI_ONLY_EXTENSIONS[@]}" \
    "${OPTIONAL_CLI_ONLY_EXTENSIONS[@]}" \
    "${MANUAL_CLI_ONLY_EXTENSIONS[@]}"
}

link_ini() {
  local ini_path="$1"
  local scan_dir="$2"
  local ini_name

  ini_name="$(basename "$ini_path")"
  root_cmd mkdir -p "$scan_dir"
  root_cmd rm -f "$scan_dir/$ini_name"
  root_cmd ln -s "../available/$ini_name" "$scan_dir/$ini_name"
}

unlink_ini() {
  local ini_path="$1"
  local scan_dir="$2"
  local ini_name

  ini_name="$(basename "$ini_path")"
  root_cmd rm -f "$scan_dir/$ini_name"
}

enable_extension() {
  local module_name="$1"
  local so_path="$PHP_EXT_DIR/${module_name}.so"
  local mode="${2:-both}"
  local ini_path="$PHP_AVAILABLE_SCAN_DIR/50-${module_name}.ini"

  [[ -f "$so_path" ]] || return 0

  reset_scan_dirs
  if [[ "$module_name" == "opcache" || "$module_name" == "xdebug" ]]; then
    printf 'zend_extension=%s.so\n' "$module_name" | root_cmd tee "$ini_path" >/dev/null
  else
    printf 'extension=%s.so\n' "$module_name" | root_cmd tee "$ini_path" >/dev/null
  fi

  case "$mode" in
  both)
    link_ini "$ini_path" "$PHP_EMBED_SCAN_DIR"
    link_ini "$ini_path" "$PHP_CLI_SCAN_DIR"
    ;;
  cli)
    unlink_ini "$ini_path" "$PHP_EMBED_SCAN_DIR"
    link_ini "$ini_path" "$PHP_CLI_SCAN_DIR"
    ;;
  embed)
    link_ini "$ini_path" "$PHP_EMBED_SCAN_DIR"
    unlink_ini "$ini_path" "$PHP_CLI_SCAN_DIR"
    ;;
  *) die "unknown extension enable mode: $mode" ;;
  esac
}

print_debian_deps() {
  cat <<EOF
Install Debian/Ubuntu build dependencies first if needed:
  apt-get update
  apt-get install -y ${DEBIAN_PACKAGES[*]}
EOF
}

print_almalinux_deps() {
  cat <<EOF
Install AlmaLinux/RHEL build dependencies first if needed:
  dnf install -y ${ALMALINUX_PACKAGES[*]}
EOF
}

print_freebsd_deps() {
  cat <<EOF
Install FreeBSD build dependencies first if needed:
  pkg install -y ${FREEBSD_PACKAGES[*]}
EOF
}

install_debian_deps() {
  if [[ "$SKIP_APT" == "1" ]]; then
    log "SKIP_APT=1, skipping apt dependency install"
    return 0
  fi

  command -v apt-get >/dev/null 2>&1 || return 0

  log "installing Debian PECL build dependencies"
  root_cmd apt-get update
  root_cmd apt-get install -y "${DEBIAN_PACKAGES[@]}"
}

install_almalinux_deps() {
  if [[ "$SKIP_APT" == "1" ]]; then
    log "SKIP_APT=1, skipping package dependency install"
    return 0
  fi

  command -v dnf >/dev/null 2>&1 || command -v yum >/dev/null 2>&1 || return 0

  local installer="dnf"
  command -v dnf >/dev/null 2>&1 || installer="yum"

  log "installing AlmaLinux/RHEL PECL build dependencies"
  root_cmd "$installer" install -y "${ALMALINUX_PACKAGES[@]}"
}

install_freebsd_deps() {
  if [[ "$SKIP_APT" == "1" ]]; then
    log "SKIP_APT=1, skipping package dependency install"
    return 0
  fi

  command -v pkg >/dev/null 2>&1 || return 0

  log "installing FreeBSD PECL build dependencies"
  root_cmd pkg install -y "${FREEBSD_PACKAGES[@]}"
}

install_os_deps() {
  case "$(detect_os)" in
  linux)
    case "$(detect_linux_family)" in
    debian) install_debian_deps ;;
    rhel) install_almalinux_deps ;;
    *) log "unknown Linux family; skipping automatic package install" ;;
    esac
    ;;
  freebsd) install_freebsd_deps ;;
  esac
}

clean_pecl_tmp() {
  local name="$1"
  rm -rf "/tmp/${name}" "/tmp/pear" "/tmp/pear-build-"* 2>/dev/null || true
}

refresh_pecl_channel() {
  if [[ "$PECL_CHANNEL_REFRESHED" == "1" ]]; then
    return 0
  fi

  log "updating PECL channel metadata"
  if ! root_cmd env PHPRC="$PHP_INI" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" \
    PHP_PEAR_PHP_BIN="$PHP" PHP_PEAR_INSTALL_DIR="$PHP_DIR/lib/php" \
    "$PECL" channel-update pecl.php.net; then
    warn "PECL channel update failed; continuing with the cached metadata"
  fi
  PECL_CHANNEL_REFRESHED=1
}

pecl_install_one() {
  local package="$1"
  local module_name="${2:-$1}"
  local mode="${3:-both}"
  local configure_options="${4:-}"
  local so_path="$PHP_EXT_DIR/${module_name}.so"
  local pecl_env=(
    "PHPRC=$PHP_INI"
    "PHP_INI_SCAN_DIR=$PHP_EMBED_SCAN_DIR"
    "PHP_PEAR_PHP_BIN=$PHP"
    "PHP_PEAR_INSTALL_DIR=$PHP_DIR/lib/php"
  )

  if [[ -f "$so_path" ]]; then
    log "extension already installed, enabling links: $module_name ($mode)"
    enable_extension "$module_name" "$mode"
    if is_loaded "$module_name" "$mode"; then
      ok "loaded: $module_name ($mode)"
    else
      warn "installed but not loaded: $module_name ($mode)"
    fi
    return 0
  fi

  if is_loaded "$module_name" "$mode"; then
    ok "already loaded: $module_name ($mode)"
    return 0
  fi

  refresh_pecl_channel
  log "pecl install: $package"
  clean_pecl_tmp "$module_name"

  local pecl_args=(install -f)
  if [[ -n "$configure_options" ]]; then
    pecl_args+=(-D "$configure_options")
  fi
  pecl_args+=("$package")

  # `yes` receives SIGPIPE after PECL consumes its prompts; judge the PECL
  # process, not the prompt feeder, under the script-wide pipefail setting.
  if (
    set +o pipefail
    yes '' | root_cmd env "${pecl_env[@]}" "$PECL" "${pecl_args[@]}"
  ); then
    :
  else
    warn "pecl returned non-zero for $package; verifying load state"
  fi

  enable_extension "$module_name" "$mode"

  if is_loaded "$module_name" "$mode"; then
    ok "loaded: $module_name ($mode)"
  else
    warn "not loaded: $module_name ($mode)"
  fi
}

event_configure_options() {
  printf '%s\n' 'with-event-pthreads="yes"'
}

install_optional_extensions() {
  local ext

  for ext in "${OPTIONAL_CLI_ONLY_EXTENSIONS[@]}"; do
    case "$ext" in
    event)
      pecl_install_one event event cli "$(event_configure_options)"
      ;;
    uv)
      pecl_install_one "channel://pecl.php.net/uv-0.3.0" uv cli
      ;;
    *)
      pecl_install_one "$ext" "$ext" cli
      ;;
    esac
  done
}

finish_runtime_install() {
  if getent passwd "$PHP_OWNER" >/dev/null 2>&1 && getent group "$PHP_GROUP" >/dev/null 2>&1; then
    root_cmd chown -R "$PHP_OWNER:$PHP_GROUP" "$PHP_DIR"
  fi

  case "$(detect_os)" in
  freebsd)
    # FreeBSD ldconfig rejects directories not owned by root.
    root_cmd chown root:wheel "$PHP_DIR/lib"
    root_cmd ldconfig -m "$PHP_DIR/lib"
    ;;
  linux)
    printf '%s\n' "$PHP_DIR/lib" | root_cmd tee /etc/ld.so.conf.d/php-zts-embed.conf >/dev/null
    root_cmd ldconfig
    ;;
  esac
}

prepare_install() {
  check_environment
  configure_freebsd_ca
  repair_pear_paths
  ensure_extension_dir
  install_os_deps
  reconcile_scan_profiles
}

install_all() {
  local ext

  prepare_install

  for ext in "${EXTENSIONS[@]}"; do
    pecl_install_one "$ext"
  done

  for ext in "${CLI_ONLY_EXTENSIONS[@]}"; do
    pecl_install_one "$ext" "$ext" cli
  done

  if [[ "${INSTALL_OPTIONAL:-0}" == "1" ]]; then
    install_optional_extensions
  else
    log "optional extensions skipped; run install-optional or set INSTALL_OPTIONAL=1"
  fi

  finish_runtime_install
}

install_embed() {
  local ext

  prepare_install

  for ext in "${EXTENSIONS[@]}"; do
    pecl_install_one "$ext"
  done

  finish_runtime_install
}

install_optional() {
  prepare_install
  install_optional_extensions
  finish_runtime_install
}

install_xdebug() {
  prepare_install
  pecl_install_one xdebug xdebug cli
  finish_runtime_install
}

verify() {
  local missing=()
  local ext
  local so_state

  check_environment
  echo "php is $PHP"
  echo "pecl is $PECL"
  echo "phpize is $PHPIZE"
  echo "php-config is $PHP_CONFIG"
  echo "extension dir is $PHP_EXT_DIR"
  echo "embed scan dir is $PHP_EMBED_SCAN_DIR"
  echo "cli scan dir is $PHP_CLI_SCAN_DIR"
  echo

  printf '%s\n' "Embed + CLI extensions:"
  for ext in "${EXTENSIONS[@]}"; do
    so_state="${YELLOW}missing .so${RESET}"
    extension_so_exists "$ext" && so_state="${GREEN}.so ok${RESET}"

    if extension_so_exists "$ext" && ini_enabled "$ext" "$PHP_EMBED_SCAN_DIR" && ini_enabled "$ext" "$PHP_CLI_SCAN_DIR"; then
      printf '%bOK%b      %-12s embed+cli  %b\n' "$GREEN" "$RESET" "$ext" "$so_state"
    else
      printf '%bMISSING%b %-12s embed+cli  %b\n' "$RED" "$RESET" "$ext" "$so_state"
      missing+=("$ext")
    fi
  done

  echo
  printf '%s\n' "CLI-only extensions:"
  for ext in "${CLI_ONLY_EXTENSIONS[@]}"; do
    so_state="${YELLOW}missing .so${RESET}"
    extension_so_exists "$ext" && so_state="${GREEN}.so ok${RESET}"

    if extension_so_exists "$ext" && ini_enabled "$ext" "$PHP_CLI_SCAN_DIR"; then
      printf '%bOK%b      %-12s cli        %b\n' "$GREEN" "$RESET" "$ext" "$so_state"
    else
      printf '%bMISSING%b %-12s cli        %b\n' "$RED" "$RESET" "$ext" "$so_state"
      missing+=("$ext")
    fi
  done

  echo
  printf '%s\n' "Embed exclusion checks:"
  for ext in "${CLI_ONLY_EXTENSIONS[@]}" "${OPTIONAL_CLI_ONLY_EXTENSIONS[@]}" "${MANUAL_CLI_ONLY_EXTENSIONS[@]}"; do
    if ini_absent "$ext" "$PHP_EMBED_SCAN_DIR"; then
      printf '%bOK%b      %-12s embed      absent\n' "$GREEN" "$RESET" "$ext"
    else
      printf '%bUNEXPECTED%b %-12s embed      should be cli-only\n' "$RED" "$RESET" "$ext"
      missing+=("$ext-in-embed")
    fi
  done

  echo
  printf '%s\n' "Opt-in CLI-only extensions:"
  for ext in "${OPTIONAL_CLI_ONLY_EXTENSIONS[@]}" "${MANUAL_CLI_ONLY_EXTENSIONS[@]}"; do
    so_state="${YELLOW}missing .so${RESET}"
    extension_so_exists "$ext" && so_state="${GREEN}.so ok${RESET}"

    if extension_so_exists "$ext" && ini_enabled "$ext" "$PHP_CLI_SCAN_DIR"; then
      printf '%bOK%b      %-12s cli        %b\n' "$GREEN" "$RESET" "$ext" "$so_state"
    else
      printf '%bSKIP%b    %-12s cli        %b\n' "$YELLOW" "$RESET" "$ext" "$so_state"
    fi
  done

  echo
  if [[ "${#missing[@]}" -eq 0 ]]; then
    printf '%bAll requested PECL extensions are available.%b\n' "$GREEN" "$RESET"
  else
    printf '%bMissing extensions:%b\n' "$RED" "$RESET"
    printf ' - %s\n' "${missing[@]}"
    return 1
  fi
}

info() {
  check_environment
  "$PHP" -v
  echo "-------------------------"
  "$PHP_CONFIG" --php-sapis
  echo "-------------------------"
  "$PHP" --ini || true
}

usage() {
  cat <<'EOF'
Usage: scripts/php-zts-install-modules.sh [install|install-embed|install-xdebug|install-optional|verify|info|deps]

Commands:
  install           Install required PECL extensions only
  install-embed     Install embed runtime PECL extensions only, skipping CLI-only tooling
  install-xdebug    Install and enable Xdebug for CLI only
  install-optional  Install optional PECL extensions such as event and uv
  verify            Verify .so files and embed/cli ini links without loading modules
  info              Show PHP version, SAPIs, and ini paths
  deps              Print package dependencies for this OS

Environment:
  INSTALL_OPTIONAL=1  Also attempt optional CLI-only extensions during install
EOF
}

main() {
  local cmd="${1:-}"

  case "$cmd" in
  install) install_all ;;
  install-embed) install_embed ;;
  install-xdebug) install_xdebug ;;
  install-optional) install_optional ;;
  verify) verify ;;
  info) info ;;
  deps)
    case "$(detect_os)" in
    linux)
      case "$(detect_linux_family)" in
      rhel) print_almalinux_deps ;;
      *) print_debian_deps ;;
      esac
      ;;
    freebsd) print_freebsd_deps ;;
    esac
    ;;
  *)
    usage
    exit 1
    ;;
  esac
}

main "$@"
