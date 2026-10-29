#!/usr/bin/env bash

# Shared by the PHP and extension installers; sourcing this file performs no I/O.
php_zts_profile_init() {
  PHP_BUILD_PROFILE="${PHP_BUILD_PROFILE:-release}"
  case "$PHP_BUILD_PROFILE" in
    release)
      PHP_PROFILE_FLAGS='-O2 -g0'
      PHP_DEBUG_OPTION=--disable-debug
      PHP_ASSERTIONS_OPTION=--disable-debug-assertions
      PHP_DEFAULT_PREFIX=/opt/php-zts-embed
      PHP_DEFAULT_SOURCE="$HOME/src/php-src"
      ;;
    debug)
      PHP_PROFILE_FLAGS='-O0 -g'
      PHP_DEBUG_OPTION=--enable-debug
      PHP_ASSERTIONS_OPTION=--enable-debug-assertions
      PHP_DEFAULT_PREFIX=/opt/php-zts-embed-debug
      PHP_DEFAULT_SOURCE="$HOME/src/php-src-debug"
      ;;
    *) printf 'ERROR: PHP_BUILD_PROFILE must be release or debug.\n' >&2; return 1 ;;
  esac
  PHP_DIR="${PHP_INSTALL_DIR:-$PHP_DEFAULT_PREFIX}"
  PHP_SRC_DIR="${PHP_SRC_DIR:-$PHP_DEFAULT_SOURCE}"
  if [[ "$PHP_BUILD_PROFILE" == debug &&
        ( "$PHP_DIR" == /opt/php-zts-embed || "$PHP_SRC_DIR" == "$HOME/src/php-src" ) ]]; then
    printf 'ERROR: Debug builds require a separate PHP_INSTALL_DIR and PHP_SRC_DIR.\n' >&2
    return 1
  fi
  export PHP_BUILD_PROFILE
}

php_zts_compiler_flags() {
  # Append the policy so inherited include-only or -O0 flags cannot disable it.
  CFLAGS="${CFLAGS:+$CFLAGS }$PHP_PROFILE_FLAGS"
  CXXFLAGS="${CXXFLAGS:+$CXXFLAGS }$PHP_PROFILE_FLAGS"
  export CFLAGS CXXFLAGS
}

php_zts_require_profile() {
  [[ -x "$PHP_DIR/bin/php" ]] || return 0
  local actual
  actual="$("$PHP_DIR/bin/php" -n -r 'echo PHP_DEBUG ? "debug" : "release";')" || return 1
  if [[ "$actual" != "$PHP_BUILD_PROFILE" ]]; then
    printf 'ERROR: PHP at %s is %s; select a separate prefix for %s.\n' \
      "$PHP_DIR" "$actual" "$PHP_BUILD_PROFILE" >&2
    return 1
  fi
}
