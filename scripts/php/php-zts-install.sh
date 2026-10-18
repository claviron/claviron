#!/usr/bin/env bash
set -euo pipefail

PHP_VERSION="${PHP_VERSION:-8.5.11}"

# This script provisions the ZTS PHP build for Linux and FreeBSD servers:
# the developer builds, and the result is a root-owned runtime under
# /opt/php-zts-embed that services load read-only. PHP_OWNER, PHP_GROUP and
# PHP_INSTALL_DIR override the defaults.
#
# Usage:
#   scripts/php-zts-install.sh                # full build + install
#   scripts/php-zts-install.sh deps           # print package deps only
#   scripts/php-zts-install.sh dry-run        # show resolved paths
#   scripts/php-zts-install.sh check-source   # prepare php-src release tag only
#   scripts/php-zts-install.sh verify         # smoke-test installed php
if [[ "$(uname -s)" == "Darwin" ]]; then
  echo "ERROR: this script is for Linux/FreeBSD servers only." >&2
  exit 2
fi

# Who owns the installed runtime. Root by default: a service that loads
# libphp must not be able to rewrite it.
PHP_OWNER="${PHP_OWNER:-root}"
PHP_GROUP="${PHP_GROUP:-root}"
PHP_SRC_DIR="${PHP_SRC_DIR:-$HOME/src/php-src}"
PHP_DIR="${PHP_INSTALL_DIR:-/opt/php-zts-embed}"
PHP_CONFIG_DIR="${PHP_CONFIG_DIR:-$PHP_DIR/etc}"
PHP_SCAN_ROOT="${PHP_SCAN_ROOT:-$PHP_CONFIG_DIR/conf.d}"
PHP_EMBED_SCAN_DIR="${PHP_EMBED_SCAN_DIR:-$PHP_SCAN_ROOT/embed}"
PHP_CLI_SCAN_DIR="${PHP_CLI_SCAN_DIR:-$PHP_SCAN_ROOT/cli}"
PHP_AVAILABLE_SCAN_DIR="${PHP_AVAILABLE_SCAN_DIR:-$PHP_SCAN_ROOT/available}"
PHP_SCAN_DIR="${PHP_SCAN_DIR:-$PHP_EMBED_SCAN_DIR}"
LOCAL_BIN="${LOCAL_BIN:-$HOME/.local/bin}"
MAKE_JOBS="${MAKE_JOBS:-}"
PHP_ENABLE_FPM="${PHP_ENABLE_FPM:-1}"
SKIP_APT="${SKIP_APT:-0}"
SUDO_READY=0

DEBIAN_PACKAGES=(
  git
  build-essential
  autoconf
  automake
  bison
  re2c
  pkg-config
  libtool
  libxml2-dev
  libssl-dev
  libsqlite3-dev
  zlib1g-dev
  libcurl4-openssl-dev
  libbz2-dev
  libzip-dev
  libicu-dev
  libonig-dev
  libpng-dev
  libjpeg-dev
  libwebp-dev
  libfreetype-dev
  libpq-dev
  libreadline-dev
  libsodium-dev
  libargon2-dev
  libxslt1-dev
  libgmp-dev
)

ALMALINUX_PACKAGES=(
  git
  curl
  gcc
  gcc-c++
  clang
  clang-devel
  llvm-devel
  make
  autoconf
  automake
  bison
  pkgconf-pkg-config
  libtool
  libxml2-devel
  openssl-devel
  sqlite-devel
  zlib-devel
  libcurl-devel
  bzip2-devel
  libzip-devel
  libicu-devel
  oniguruma-devel
  libpng-devel
  libjpeg-turbo-devel
  libwebp-devel
  freetype-devel
  postgresql-devel
  readline-devel
  libsodium-devel
  libargon2-devel
  libxslt-devel
  gmp-devel
  xz
)

FREEBSD_PACKAGES=(
  git
  gmake
  autoconf
  automake
  bison
  re2c
  pkgconf
  libtool
  libiconv
  libxml2
  openssl
  sqlite3
  curl
  bzip2
  libzip
  icu
  oniguruma
  png
  jpeg-turbo
  webp
  freetype2
  postgresql18-client
  readline
  libsodium
  libargon2
  libxslt
  gmp
)

log() { printf '%s\n' "INFO: $*"; }
die() {
  printf '%s\n' "ERROR: $*" >&2
  exit 1
}

ensure_sudo() {
  if [[ "$SUDO_READY" == "1" ]]; then
    return 0
  fi

  command -v sudo >/dev/null 2>&1 || die "this step needs root privileges; run as root or as a user with sudo"
  sudo -v || die "sudo authentication failed; run as root or use a user with sudo"
  SUDO_READY=1
}

sudo_cmd() {
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

jobs_count() {
  if [[ -n "$MAKE_JOBS" ]]; then
    echo "$MAKE_JOBS"
  elif command -v nproc >/dev/null 2>&1; then
    nproc
  elif command -v sysctl >/dev/null 2>&1; then
    sysctl -n hw.ncpu
  else
    echo 2
  fi
}

version_ge() {
  awk -v version="$1" -v minimum="$2" '
    BEGIN {
      split(version, v, ".")
      split(minimum, m, ".")
      for (i = 1; i <= 3; i++) {
        vi = v[i] + 0
        mi = m[i] + 0
        if (vi > mi) exit 0
        if (vi < mi) exit 1
      }
      exit 0
    }
  '
}

re2c_version() {
  command -v re2c >/dev/null 2>&1 || return 1
  re2c --version 2>/dev/null | awk 'NR == 1 {print $2}'
}

print_debian_deps() {
  cat <<EOF
Debian/Ubuntu dependencies:
  apt-get update
  apt-get install -y ${DEBIAN_PACKAGES[*]}
EOF
}

print_almalinux_deps() {
  cat <<EOF
AlmaLinux/RHEL dependencies:
  dnf install -y ${ALMALINUX_PACKAGES[*]}
EOF
}

print_freebsd_deps() {
  cat <<EOF
FreeBSD dependencies:
  pkg install -y ${FREEBSD_PACKAGES[*]}
EOF
}

install_debian_deps() {
  if [[ "$SKIP_APT" == "1" ]]; then
    log "SKIP_APT=1, skipping apt dependency install"
    return 0
  fi

  command -v apt-get >/dev/null 2>&1 || return 0

  log "installing Debian build dependencies"
  sudo_cmd apt-get update
  sudo_cmd apt-get install -y "${DEBIAN_PACKAGES[@]}"
}

install_almalinux_deps() {
  if [[ "$SKIP_APT" == "1" ]]; then
    log "SKIP_APT=1, skipping package dependency install"
    return 0
  fi

  command -v dnf >/dev/null 2>&1 || command -v yum >/dev/null 2>&1 || return 0

  local installer="dnf"
  command -v dnf >/dev/null 2>&1 || installer="yum"

  log "installing AlmaLinux/RHEL build dependencies"
  sudo_cmd "$installer" install -y "${ALMALINUX_PACKAGES[@]}"
}

install_freebsd_deps() {
  if [[ "$SKIP_APT" == "1" ]]; then
    log "SKIP_APT=1, skipping package dependency install"
    return 0
  fi

  command -v pkg >/dev/null 2>&1 || return 0

  log "installing FreeBSD build dependencies"
  sudo_cmd pkg install -y "${FREEBSD_PACKAGES[@]}"
}

configure_linux_env() {
  case "$(detect_linux_family)" in
  debian) install_debian_deps ;;
  rhel) install_almalinux_deps ;;
  *) log "unknown Linux family; skipping automatic package install" ;;
  esac
  export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-}"
}

configure_freebsd_env() {
  install_freebsd_deps
  MAKE="${MAKE:-gmake}"
  CC="${CC:-/usr/bin/cc}"
  CXX="${CXX:-/usr/bin/c++}"
  export MAKE CC CXX

  local ssl_dir="/usr/local"
  CPPFLAGS="-I${ssl_dir}/include${CPPFLAGS:+ $CPPFLAGS}"
  LDFLAGS="-L${ssl_dir}/lib${LDFLAGS:+ $LDFLAGS}"
  PKG_CONFIG_PATH="/usr/local/libdata/pkgconfig:/usr/local/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
  export CPPFLAGS LDFLAGS PKG_CONFIG_PATH
}

prepare_source() {
  local release_tag="php-$PHP_VERSION"
  local local_tag_sha
  local remote_tag_sha

  mkdir -p "$(dirname "$PHP_SRC_DIR")"
  sudo_cmd mkdir -p "$(dirname "$PHP_DIR")"

  if [[ ! -d "$PHP_SRC_DIR/.git" ]]; then
    log "php-src not found, cloning upstream → $PHP_SRC_DIR"
    git clone https://github.com/php/php-src.git "$PHP_SRC_DIR"
  fi

  cd "$PHP_SRC_DIR"

  if ! git rev-parse --verify --quiet "refs/tags/$release_tag" >/dev/null; then
    git fetch --no-tags origin "refs/tags/$release_tag:refs/tags/$release_tag" ||
      die "PHP release tag $release_tag not found; unsupported PHP_VERSION=$PHP_VERSION"
  fi

  local_tag_sha="$(git rev-parse "refs/tags/$release_tag")"
  remote_tag_sha="$(git ls-remote --tags origin "refs/tags/$release_tag" | awk '{print $1}')"
  if [[ -z "$remote_tag_sha" ]]; then
    die "PHP release tag $release_tag not found on origin; unsupported PHP_VERSION=$PHP_VERSION"
  fi
  if [[ "$local_tag_sha" != "$remote_tag_sha" ]]; then
    die "local PHP release tag $release_tag differs from origin; delete the local tag and retry"
  fi

  log "switching php-src to release tag $release_tag"
  git switch --detach "refs/tags/$release_tag"
}

configure_build_env() {
  local os="$1"

  export PHP_DIR PHP_CONFIG_DIR PHP_SCAN_ROOT PHP_EMBED_SCAN_DIR PHP_CLI_SCAN_DIR PHP_AVAILABLE_SCAN_DIR PHP_SCAN_DIR
  export PHP="$PHP_DIR/bin/php"
  export PHPIZE="$PHP_DIR/bin/phpize"
  export PHP_CONFIG="$PHP_DIR/bin/php-config"
  export PATH="$LOCAL_BIN:$PHP_DIR/bin:$PATH"

  case "$os" in
  linux)
    configure_linux_env
    ;;
  freebsd) configure_freebsd_env ;;
  esac

  export LDFLAGS="${LDFLAGS:-}"
  export CPPFLAGS="${CPPFLAGS:-}"
  export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-}"
}

verify_re2c() {
  local min_version="1.0.3"
  local found

  found="$(re2c_version || true)"
  if [[ -n "$found" ]] && version_ge "$found" "$min_version"; then
    log "re2c $found is available"
    return 0
  fi

  if [[ -n "$found" ]]; then
    printf 'ERROR: re2c %s is too old; PHP requires >= %s\n' "$found" "$min_version" >&2
  else
    printf 'ERROR: re2c is missing; PHP requires >= %s\n' "$min_version" >&2
  fi

  if [[ "$(detect_os)" == "linux" && "$(detect_linux_family)" == "rhel" ]]; then
    cat >&2 <<EOF

On AlmaLinux/RHEL, install a modern developer-local re2c first:
  scripts/re2c-install-almalinux.sh
  export PATH="$LOCAL_BIN:\$PATH"

EOF
  fi
  exit 1
}

verify_pkg_config() {
  local missing=()
  local packages=(
    libxml-2.0
    openssl
    sqlite3
    zlib
    libpng
    libjpeg
    libwebp
    freetype2
    libcurl
    libzip
    icu-i18n
    icu-io
    icu-uc
    libpq
  )

  for package in "${packages[@]}"; do
    if ! pkg-config --exists "$package"; then
      missing+=("$package")
    fi
  done

  if [[ "${#missing[@]}" -gt 0 ]]; then
    printf 'Missing pkg-config packages:\n' >&2
    printf ' - %s\n' "${missing[@]}" >&2
    case "$(detect_os)" in
    linux)
      case "$(detect_linux_family)" in
      rhel) print_almalinux_deps >&2 ;;
      *) print_debian_deps >&2 ;;
      esac
      ;;
    freebsd) print_freebsd_deps >&2 ;;
    *) print_debian_deps >&2 ;;
    esac
    exit 1
  fi
}

clean_tree() {
  if [[ -f Makefile ]]; then
    make clean || true
    make distclean || true
  fi
}

build_php() {
  local configure_args=()
  local iconv_configure_arg="--with-iconv"

  if [[ "$(detect_os)" == "freebsd" ]]; then
    # FreeBSD provides iconv in libc, while ports install GNU libiconv under
    # /usr/local. The ports include path exposes GNU headers, so select the
    # matching library explicitly instead of mixing them with libc iconv.
    iconv_configure_arg="--with-iconv=/usr/local"
  fi

  sudo_cmd mkdir -p "$PHP_CONFIG_DIR" "$PHP_SCAN_ROOT" "$PHP_AVAILABLE_SCAN_DIR" "$PHP_EMBED_SCAN_DIR" "$PHP_CLI_SCAN_DIR"
  sudo_cmd chown -R "$(id -u):$(id -g)" "$PHP_DIR"

  ./buildconf --force

  configure_args=(
    --prefix="$PHP_DIR"
    --with-config-file-path="$PHP_CONFIG_DIR"
    --with-config-file-scan-dir="$PHP_SCAN_DIR"
    --enable-embed=shared
    --disable-zend-signals
    --enable-zts
    --enable-cli
    --with-pear
    --enable-intl
    --enable-calendar
    --enable-mbstring
    --enable-bcmath
    --enable-exif
    --enable-fileinfo
    --enable-filter
    --enable-session
    --enable-tokenizer
    --enable-dom
    --enable-simplexml
    --enable-xml
    --enable-xmlreader
    --enable-xmlwriter
    --enable-phar
    --enable-pcntl
    --enable-posix
    --enable-shmop
    --enable-sysvmsg
    --enable-sysvsem
    --enable-sysvshm
    --enable-sockets
    --enable-gd
    --with-jpeg
    --with-webp
    --with-freetype
    --with-zlib
    --with-bz2
    --with-zip
    --with-curl
    "$iconv_configure_arg"
    --with-openssl
    --with-password-argon2
    --with-sodium
    --with-libxml
    --with-xsl
    --with-mysqli=mysqlnd
    --with-pdo-mysql=mysqlnd
    --with-pdo-pgsql
    --with-pgsql
    --with-pdo-sqlite
    --with-sqlite3
    --with-gmp
  )

  if [[ "$PHP_ENABLE_FPM" == "1" ]]; then
    configure_args+=(--enable-fpm)
  fi

  ./configure "${configure_args[@]}"

  "${MAKE:-make}" -j"$(jobs_count)"
  "${MAKE:-make}" install
}

write_default_ini() {
  local ini="$PHP_CONFIG_DIR/php.ini"
  local ca_file="/etc/ssl/cert.pem"
  local private_ca="${PHP_PRIVATE_CA:-/etc/certstore/cacert.pem}"
  local ca_bundle="$PHP_CONFIG_DIR/ca-bundle.pem"
  local ca_ini="$PHP_AVAILABLE_SCAN_DIR/10-ca.ini"

  if [[ ! -f "$ini" ]]; then
    cp "$PHP_SRC_DIR/php.ini-production" "$ini"
  fi

  mkdir -p "$PHP_SCAN_ROOT" "$PHP_AVAILABLE_SCAN_DIR" "$PHP_EMBED_SCAN_DIR" "$PHP_CLI_SCAN_DIR"

  # Ports OpenSSL has a different compiled CA path from FreeBSD base.
  # Keep a guest-local bundle so PHP also trusts the Roma authority without
  # depending on the p9 mount after installation.
  if [[ "$(detect_os)" == "freebsd" && -r "$ca_file" ]]; then
    if [[ -r "$private_ca" ]]; then
      cat "$ca_file" "$private_ca" >"$ca_bundle"
      ca_file="$ca_bundle"
    fi
    printf 'openssl.cafile=%s\n' "$ca_file" >"$ca_ini"
    ln -sf "../available/$(basename "$ca_ini")" "$PHP_EMBED_SCAN_DIR/$(basename "$ca_ini")"
    ln -sf "../available/$(basename "$ca_ini")" "$PHP_CLI_SCAN_DIR/$(basename "$ca_ini")"
  fi
}

fix_runtime_permissions() {
  if getent passwd "$PHP_OWNER" >/dev/null 2>&1 && getent group "$PHP_GROUP" >/dev/null 2>&1; then
    log "setting runtime ownership to $PHP_OWNER:$PHP_GROUP"
    sudo_cmd chown -R "$PHP_OWNER:$PHP_GROUP" "$PHP_DIR"
  else
    log "owner $PHP_OWNER:$PHP_GROUP not found; leaving current ownership on $PHP_DIR"
  fi

  sudo_cmd find "$PHP_DIR" -type d -exec chmod 0755 {} +
  sudo_cmd find "$PHP_DIR" -type f -exec chmod u=rwX,go=rX {} +
  sudo_cmd find "$PHP_DIR/bin" -type f -exec chmod 0755 {} +
  sudo_cmd find "$PHP_DIR/sbin" -type f -exec chmod 0755 {} + 2>/dev/null || true
  [[ -f "$PHP_DIR/lib/libphp.so" ]] && sudo_cmd chmod 0755 "$PHP_DIR/lib/libphp.so"
}

register_runtime_library() {
  local conf="/etc/ld.so.conf.d/php-zts-embed.conf"

  if [[ "$(detect_os)" == "freebsd" ]]; then
    # FreeBSD ldconfig at boot scans /usr/local/libdata/ldconfig/* (per
    # default rc.conf `ldconfig_local_dirs`). Write the path here so it
    # survives reboots; `ldconfig -m` only updates the running cache
    # (volatile) and would otherwise be lost on next boot.
    local fbsd_conf="/usr/local/libdata/ldconfig/php-zts-embed"
    log "registering $PHP_DIR/lib in $fbsd_conf (persistent across reboots)"
    sudo_cmd mkdir -p /usr/local/libdata/ldconfig
    sudo_cmd chown root:wheel "$PHP_DIR/lib"
    printf '%s\n' "$PHP_DIR/lib" | sudo_cmd tee "$fbsd_conf" >/dev/null
    sudo_cmd ldconfig -m "$PHP_DIR/lib"
    return 0
  fi

  log "registering libphp.so path in $conf"
  printf '%s\n' "$PHP_DIR/lib" | sudo_cmd tee "$conf" >/dev/null
  sudo_cmd ldconfig
}

verify_install() {
  local cli_version
  local config_version

  [[ -x "$PHP_DIR/bin/php" ]] || die "php binary missing: $PHP_DIR/bin/php"
  [[ -x "$PHP_DIR/bin/php-config" ]] || die "php-config missing: $PHP_DIR/bin/php-config"
  [[ -f "$PHP_DIR/lib/libphp.so" ]] || die "libphp.so missing: $PHP_DIR/lib/libphp.so"

  PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" -v
  "$PHP_DIR/bin/php-config" --php-sapis
  PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" --ini
  cli_version="$(PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" -r 'echo PHP_VERSION;')"
  config_version="$("$PHP_DIR/bin/php-config" --version)"
  if [[ "$cli_version" != "$PHP_VERSION" || "$config_version" != "$PHP_VERSION" ]]; then
    printf 'REJECT: expected PHP %s, got cli=%s php-config=%s\n' "$PHP_VERSION" "$cli_version" "$config_version" >&2
    exit 1
  fi
  printf 'OK: PHP release version verified (%s)\n' "$PHP_VERSION"

  PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" -r '
    $gd = function_exists("gd_info") ? gd_info() : [];
    foreach (["JPEG Support", "PNG Support", "WebP Support", "FreeType Support"] as $key) {
        if (empty($gd[$key])) {
            fwrite(STDERR, "REJECT: PHP GD missing required capability: {$key}\n");
            exit(1);
        }
    }
    echo "OK: PHP GD JPEG/PNG/WebP/FreeType support verified\n";
  '

  if [[ -x "$(dirname "$0")/php-zts-modules.sh" ]]; then
    "$(dirname "$0")/php-zts-modules.sh" embed
    "$(dirname "$0")/php-zts-modules.sh" cli
  fi
  log "libphp.so registered through ldconfig: $PHP_DIR/lib"
}

dry_run() {
  echo "OS: $(detect_os)"
  if [[ "$(detect_os)" == "linux" ]]; then
    echo "Linux family: $(detect_linux_family)"
  fi
  echo "php version: $PHP_VERSION"
  echo "php-src: $PHP_SRC_DIR"
  echo "prefix: $PHP_DIR"
  echo "local bin: $LOCAL_BIN"
  echo "config dir: $PHP_CONFIG_DIR"
  echo "available scan dir: $PHP_AVAILABLE_SCAN_DIR"
  echo "embed scan dir: $PHP_EMBED_SCAN_DIR"
  echo "cli scan dir: $PHP_CLI_SCAN_DIR"
  echo "make jobs: $(jobs_count)"
  echo "skip package install: $SKIP_APT"
  echo "dependencies:"
  case "$(detect_os)" in
  linux)
    case "$(detect_linux_family)" in
    rhel) print_almalinux_deps ;;
    *) print_debian_deps ;;
    esac
    ;;
  freebsd) print_freebsd_deps ;;
  esac
}

main() {
  local os
  os="$(detect_os)"

  case "${1:-install}" in
  deps)
    case "$os" in
    linux)
      case "$(detect_linux_family)" in
      rhel) print_almalinux_deps ;;
      *) print_debian_deps ;;
      esac
      ;;
    freebsd) print_freebsd_deps ;;
    esac
    exit 0
    ;;
  dry-run)
    dry_run
    exit 0
    ;;
  check-source)
    prepare_source
    exit 0
    ;;
  verify)
    verify_install
    exit 0
    ;;
  install) ;;
  *) die "usage: $0 [install|verify|dry-run|check-source|deps]" ;;
  esac

  log "building PHP ZTS embed SAPI"
  log "php-src: $PHP_SRC_DIR"
  log "prefix: $PHP_DIR"

  prepare_source
  configure_build_env "$os"
  verify_re2c
  verify_pkg_config
  clean_tree
  build_php
  write_default_ini
  fix_runtime_permissions
  register_runtime_library
  verify_install
}

main "$@"
