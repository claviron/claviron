#!/usr/bin/env bash
set -euo pipefail

case "${1:-help}" in
help|-h|--help)
  cat <<'USAGE'
Usage: sudo env PHP_VERSION=VERSION [PHP_INSTALL_DIR=/opt/php-zts-embed] \
  [PHP_CONFIG_DIR=PREFIX/etc] [PHP_INI_SCAN_DIR=PREFIX/etc/conf.d/fpm] \
  scripts/install/php/php-zts-fpm-install.sh install

Validate the selected runtime and existing configuration, then install the
systemd unit, launchers and /etc/sysconfig/php-zts-fpm. Does not stop services,
remove RPM packages, alter configuration, enable the unit or start FPM.
The CLI launcher is installed as /usr/local/bin/php; it uses the same INI files.
USAGE
  exit 0
  ;;
install) [[ $# == 1 ]] || { printf 'Use --help for usage.\n' >&2; exit 2; } ;;
*) printf 'Use --help for usage.\n' >&2; exit 2 ;;
esac
[[ "$(uname -s)" == Linux ]] || {
  printf 'This installer requires Linux/systemd; use php-zts-fpm-install-freebsd.sh on FreeBSD.\n' >&2
  exit 1
}
[[ $EUID == 0 ]] || { printf 'Run this installer as root.\n' >&2; exit 1; }
: "${PHP_VERSION:?Set PHP_VERSION explicitly}"
export PHP_INSTALL_DIR="${PHP_INSTALL_DIR:-/opt/php-zts-embed}"
export PHP_CONFIG_DIR="${PHP_CONFIG_DIR:-$PHP_INSTALL_DIR/etc}"
export PHP_INI_SCAN_DIR="${PHP_INI_SCAN_DIR:-$PHP_CONFIG_DIR/conf.d/fpm}"
export PHP_VERSION PHP_CONFIG_DIR
for value in "$PHP_VERSION" "$PHP_INSTALL_DIR" "$PHP_CONFIG_DIR" "$PHP_INI_SCAN_DIR"; do
  [[ "$value" =~ ^[a-zA-Z0-9/._-]+$ ]] || {
    printf 'Environment values must contain only letters, digits, /, ., _ and -.\n' >&2
    exit 1
  }
done
for value in "$PHP_INSTALL_DIR" "$PHP_CONFIG_DIR" "$PHP_INI_SCAN_DIR"; do
  [[ "$value" == /* ]] || { printf 'Configuration paths must be absolute.\n' >&2; exit 1; }
done

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
bash "$source_dir/php-zts-fpm.sh" check
install -d /usr/local/libexec /usr/local/bin /etc/sysconfig
install -m 0755 "$source_dir/php-zts-fpm.sh" /usr/local/libexec/php-zts-fpm
install -m 0755 "$source_dir/php-zts-cli.sh" /usr/local/bin/php
if [[ ! -e /usr/bin/php && ! -L /usr/bin/php ]]; then
  ln -s /usr/local/bin/php /usr/bin/php
fi
install -m 0644 "$source_dir/php-zts-fpm.service" /etc/systemd/system/php-zts-fpm.service
# The previous RPM's logrotate entry targets the same files; retire it first.
if [[ ! -e /etc/logrotate.d/php-fpm ]]; then
  install -m 0644 "$source_dir/php-zts-fpm.logrotate" /etc/logrotate.d/php-zts-fpm
fi
printf 'PHP_VERSION=%s\nPHP_INSTALL_DIR=%s\nPHP_CONFIG_DIR=%s\nPHP_INI_SCAN_DIR=%s\n' \
  "$PHP_VERSION" "$PHP_INSTALL_DIR" "$PHP_CONFIG_DIR" "$PHP_INI_SCAN_DIR" \
  >/etc/sysconfig/php-zts-fpm
chmod 0644 /etc/sysconfig/php-zts-fpm
systemctl daemon-reload
printf 'Installed php-zts-fpm.service; enable/start it only after stopping the previous FPM.\n'
