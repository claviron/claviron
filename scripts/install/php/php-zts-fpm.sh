#!/usr/bin/env bash
set -euo pipefail

: "${PHP_INSTALL_DIR:?Set PHP_INSTALL_DIR in the service environment}"
: "${PHP_VERSION:?Set the expected PHP_VERSION in the service environment}"
: "${PHP_CONFIG_DIR:?Set PHP_CONFIG_DIR in the service environment}"
export PHPRC="$PHP_CONFIG_DIR/php.ini"
export PHP_INI_SCAN_DIR="${PHP_INI_SCAN_DIR:-$PHP_CONFIG_DIR/conf.d/fpm}"

actual_version="$("$PHP_INSTALL_DIR/bin/php" -n -r 'echo PHP_VERSION;')"
if [[ "$actual_version" != "$PHP_VERSION" ]]; then
  printf 'Expected PHP %s, found %s in %s\n' "$PHP_VERSION" "$actual_version" "$PHP_INSTALL_DIR" >&2
  exit 1
fi
[[ -r "$PHPRC" && -r "$PHP_CONFIG_DIR/php-fpm.conf" && -d "$PHP_INI_SCAN_DIR" ]] || {
  printf 'PHP configuration is incomplete: %s\n' "$PHP_CONFIG_DIR" >&2
  exit 1
}

case "${1:-start}" in
  check) mode=--test ;;
  start) mode=--nodaemonize ;;
  *) printf 'Usage: php-zts-fpm [check|start]\n' >&2; exit 2 ;;
esac
exec "$PHP_INSTALL_DIR/sbin/php-fpm" "$mode" \
  -c "$PHPRC" --fpm-config "$PHP_CONFIG_DIR/php-fpm.conf"
