#!/usr/bin/env bash
set -euo pipefail

# Resolve companion files before source preparation changes the working directory.
PHP_INSTALL_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
source "$PHP_INSTALL_SCRIPT_DIR/build-profile.sh"
php_zts_profile_init

PHP_VERSION="${PHP_VERSION:-8.5.12}"
RECONFIGURE=0
PHP_SOURCE_CHANGED=0

# This script provisions the ZTS PHP build for Linux and FreeBSD servers:
# the developer builds, and the result is a root-owned runtime under
# /opt/php-zts-embed that services load read-only. PHP_OWNER, PHP_GROUP and
# PHP_INSTALL_DIR override the defaults.
#
# Usage:
#   scripts/install/php/php-zts-install.sh            # show help
#   scripts/install/php/php-zts-install.sh install    # build + install without cleaning
#   scripts/install/php/php-zts-install.sh clean      # clean the source build tree
#   scripts/install/php/php-zts-install.sh deps           # print package deps only
#   scripts/install/php/php-zts-install.sh dry-run        # show resolved paths
#   scripts/install/php/php-zts-install.sh check-source   # prepare php-src release tag only
#   scripts/install/php/php-zts-install.sh verify         # smoke-test installed php
if [[ "$(uname -s)" == "Darwin" ]]; then
  echo "ERROR: this script is for Linux/FreeBSD servers only." >&2
  exit 2
fi

# Who owns the installed runtime. Root by default: a service that loads
# libphp must not be able to rewrite it.
PHP_OWNER="${PHP_OWNER:-root}"
PHP_GROUP="${PHP_GROUP:-root}"
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

# Reuse the common source-built libzip instead of installing a repository copy.
if [[ -r /usr/local/libzip/lib/pkgconfig/libzip.pc ]]; then
  export PKG_CONFIG_PATH="/usr/local/libzip/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
  local_zip_packages=()
  for build_package in "${ALMALINUX_PACKAGES[@]}"; do
    if [[ $build_package != libzip-devel ]]; then local_zip_packages+=("$build_package"); fi
  done
  ALMALINUX_PACKAGES=("${local_zip_packages[@]}")
  unset local_zip_packages build_package
fi

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

FAILED() { printf '❌ \033[1;31m[FAILED]\033[0m %s\n' "$*" >&2; }
WARNING() { printf '\033[1;33m[WARNING]\033[0m %s\n' "$*" >&2; }
INFO() { printf '\033[1;34m[INFO]\033[0m %s\n' "$*" >&2; }
SUCCESS() { printf '\033[1;32m[✔]\033[0m %s\n' "$*" >&2; }

log() { INFO "$*"; }
die() {
  FAILED "$*"
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

  if [[ "$(git rev-parse HEAD)" != "$(git rev-parse "refs/tags/$release_tag^{commit}")" ]]; then
    PHP_SOURCE_CHANGED=1
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
  php_zts_compiler_flags
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

clean_tree() (
  [[ -d "$PHP_SRC_DIR" ]] || die "PHP source directory missing: $PHP_SRC_DIR"
  cd "$PHP_SRC_DIR"
  if [[ -f Makefile ]]; then
    local make_command="${MAKE:-make}"
    if [[ "$(detect_os)" == freebsd ]]; then make_command="${MAKE:-gmake}"; fi
    "$make_command" clean
    "$make_command" distclean
    rm -f .claviron-php-build
  else
    log "No Makefile in $PHP_SRC_DIR; nothing to clean."
  fi
)

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

  configure_args=(
    --prefix="$PHP_DIR"
    "$PHP_DEBUG_OPTION"
    "$PHP_ASSERTIONS_OPTION"
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

  local configured_version="" fingerprint previous=""
  local stamp_file=.claviron-php-build
  command -v "${CC:-cc}" >/dev/null || die 'C compiler not found; set CC.'
  command -v "${CXX:-c++}" >/dev/null || die 'C++ compiler not found; set CXX.'
  fingerprint="$(
    printf '%s\n' "$PHP_BUILD_PROFILE" "$PHP_VERSION" "${configure_args[@]}" \
      "$CFLAGS" "$CXXFLAGS" "$CPPFLAGS" "$LDFLAGS" "$PKG_CONFIG_PATH" \
      "$(command -v "${CC:-cc}")" "$("${CC:-cc}" --version)" \
      "$(command -v "${CXX:-c++}")" "$("${CXX:-c++}" --version)" | cksum
  )"
  if [[ -f "$stamp_file" ]]; then previous="$(cat "$stamp_file")"; fi
  if [[ -f config.status ]]; then
    configured_version="$(sh ./config.status --version 2>/dev/null | sed -n '1p')"
  fi
  if [[ "$RECONFIGURE" == 1 || "$PHP_SOURCE_CHANGED" == 1 ||
        "$configured_version" != "PHP config.status $PHP_VERSION" ||
        "$previous" != "$fingerprint" ||
        ! -f Makefile || ! -f main/php_config.h || ! -f main/build-defs.h ]]; then
    # A failed configure must not look reusable on the next install attempt.
    rm -f config.status Makefile "$stamp_file"
    if ! ./buildconf --force || ! ./configure "${configure_args[@]}"; then
      rm -f config.status Makefile "$stamp_file"
      return 1
    fi
    # Use the new Makefile even when the old one was lost but objects survived.
    # Make does not track flags; no object from a previous profile may survive.
    "${MAKE:-make}" clean
    printf '%s\n' "$fingerprint" > "$stamp_file"
  else
    log "Reusing PHP $PHP_VERSION configuration; use install --configure to change build options."
  fi

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
  # Keep an operator-supplied private authority in the installation's CA bundle.
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
  if [[ "$PHP_BUILD_PROFILE" == debug ]]; then
    log "Debug library stays private; select $PHP_DIR/lib explicitly when linking."
    return 0
  fi
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
  php_zts_require_profile
  [[ "$("$PHP_DIR/bin/php" -n -r 'echo (int) PHP_ZTS;')" == 1 ]] || die 'Expected a ZTS PHP build.'

  PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" -v
  "$PHP_DIR/bin/php-config" --php-sapis
  PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" --ini
  cli_version="$(PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" -r 'echo PHP_VERSION;')"
  config_version="$("$PHP_DIR/bin/php-config" --version)"
  if [[ "$cli_version" != "$PHP_VERSION" || "$config_version" != "$PHP_VERSION" ]]; then
    FAILED "Expected PHP $PHP_VERSION, got cli=$cli_version php-config=$config_version"
    exit 1
  fi
  SUCCESS "PHP release version verified ($PHP_VERSION)"

  PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" -r '
    $gd = function_exists("gd_info") ? gd_info() : [];
    foreach (["JPEG Support", "PNG Support", "WebP Support", "FreeType Support"] as $key) {
        if (empty($gd[$key])) {
            fwrite(STDERR, "❌ \033[1;31m[FAILED]\033[0m PHP GD missing required capability: {$key}\n");
            exit(1);
        }
    }
    fwrite(STDERR, "\033[1;32m[✔]\033[0m PHP GD JPEG/PNG/WebP/FreeType support verified\n");
  '

  local config_report="$PHP_INSTALL_SCRIPT_DIR/php-zts-verify-config.php"
  local config_failed=0
  if ! PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_EMBED_SCAN_DIR" "$PHP_DIR/bin/php" "$config_report" embed; then
    config_failed=1
  fi
  if [[ "$PHP_CLI_SCAN_DIR" != "$PHP_EMBED_SCAN_DIR" ]]; then
    if ! PHPRC="$PHP_CONFIG_DIR/php.ini" PHP_INI_SCAN_DIR="$PHP_CLI_SCAN_DIR" "$PHP_DIR/bin/php" "$config_report" cli; then
      config_failed=1
    fi
  fi
  if [[ "$config_failed" == 1 ]]; then
    FAILED "Installed PHP configuration verification failed; review the settings reported above."
    return 1
  fi
  SUCCESS "Installed PHP configuration verification passed."
  log "Verified PHP build profile: $PHP_BUILD_PROFILE"
}

dry_run() {
  echo "OS: $(detect_os)"
  if [[ "$(detect_os)" == "linux" ]]; then
    echo "Linux family: $(detect_linux_family)"
  fi
  echo "php version: $PHP_VERSION"
  echo "build profile: $PHP_BUILD_PROFILE"
  echo "C compiler: ${CC:-cc}"
  echo "CFLAGS: ${CFLAGS:+$CFLAGS }$PHP_PROFILE_FLAGS"
  echo "CXXFLAGS: ${CXXFLAGS:+$CXXFLAGS }$PHP_PROFILE_FLAGS"
  echo "debug option: $PHP_DEBUG_OPTION"
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

usage() {
  cat <<EOF
Usage: $0 [help|install [--configure]|clean|verify|dry-run|check-source|deps]

  help          Show this help (default; also -h or --help).
  install       Build and install using the existing configuration.
  install --configure
                Run buildconf --force and configure before building and installing.
                Use when changing configure options, compiler or build environment.
                Discards objects compiled with the previous settings.
  clean         Run make clean and make distclean in the PHP source directory.
  verify        Check the installed PHP without rebuilding.
  dry-run       Show resolved installation paths and dependencies.
  check-source  Prepare the PHP source release tag only.
  deps          Print package dependencies only.

First installation, a source revision change, a configured PHP version mismatch,
or missing configuration outputs automatically runs buildconf and configure.
Compiler, flag, profile and configure-option changes also trigger a clean
reconfiguration. Retry a failed make with unchanged settings to reuse completed
objects. Use install --configure to force a clean reconfiguration.

PHP_BUILD_PROFILE=release (default): -O2 -g0, PHP debug and debug assertions off.
PHP_BUILD_PROFILE=debug: -O0 -g, PHP debug enabled, separate source and prefix.
Debug defaults to /opt/php-zts-embed-debug and is not registered with ldconfig.
CC and CXX select the compiler; inherited include/link flags are preserved.
Xdebug is an independent, explicitly installed CLI extension.

Installation must be explicitly requested. For a clean installation build:
  $0 clean && $0 install

To check an existing installation without rebuilding:
  $0 verify
EOF
}

main() {
  case "${1:-help}" in
    help|-h|--help) usage; return 0 ;;
  esac

  if [[ "${1:-}" == install && "${2:-}" == --configure && $# == 2 ]]; then
    RECONFIGURE=1
  elif [[ $# -gt 1 ]]; then
    die "usage: $0 install [--configure]; other commands take no arguments"
  fi

  local os
  os="$(detect_os)"

  case "${1:-help}" in
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
  clean)
    clean_tree
    return 0
    ;;
  install) ;;
  *) usage >&2; die "Unknown command: $1" ;;
  esac

  log "building PHP ZTS embed SAPI"
  log "php-src: $PHP_SRC_DIR"
  log "prefix: $PHP_DIR"
  log "build profile: $PHP_BUILD_PROFILE ($PHP_PROFILE_FLAGS, $PHP_DEBUG_OPTION)"
  php_zts_require_profile

  prepare_source
  configure_build_env "$os"
  verify_re2c
  verify_pkg_config
  build_php
  write_default_ini
  fix_runtime_permissions
  register_runtime_library
  verify_install
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
