#!/usr/bin/env bash
set -euo pipefail

# This root-owned file is shared with the systemd service.
source /etc/sysconfig/php-zts-fpm
: "${PHP_INSTALL_DIR:?Missing PHP_INSTALL_DIR}"
: "${PHP_CONFIG_DIR:?Missing PHP_CONFIG_DIR}"
export PHPRC="$PHP_CONFIG_DIR/php.ini"
export PHP_INI_SCAN_DIR="${PHP_INI_SCAN_DIR:-$PHP_CONFIG_DIR/conf.d/fpm}"
exec "$PHP_INSTALL_DIR/bin/php" "$@"
