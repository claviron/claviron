#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == module ]]; then source "$MODULE_INSTALLER"; else source "$INSTALLER"; fi
sudo_cmd() { :; }
case "$1" in
  flags)
    php_zts_compiler_flags
    printf '%s\n' "$PHP_DIR" "$PHP_SRC_DIR" "$CFLAGS" "$CXXFLAGS" "$PHP_DEBUG_OPTION"
    ;;
  profile) php_zts_require_profile ;;
  clean) clean_tree ;;
  register)
    sudo_cmd() { printf 'unexpected loader operation\n' >&2; return 1; }
    register_runtime_library
    ;;
  module)
    root_cmd() { "$@"; }
    is_loaded() { [[ -f "$FIXTURE_MARKER" ]]; }
    enable_extension() { :; }
    PECL_CHANNEL_REFRESHED=1
    PHP_EXT_DIR="$PHP_DIR/extensions"
    PECL="$FIXTURE_PECL"
    pecl_install_one fixture fixture cli
    ;;
  build)
    php_zts_compiler_flags
    cd "$PHP_SRC_DIR"
    RECONFIGURE="${FIXTURE_RECONFIGURE:-0}"
    build_php
    ;;
esac
