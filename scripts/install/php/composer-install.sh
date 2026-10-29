#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/install/php/composer-install.sh install
       scripts/install/php/composer-install.sh [help|--help|-h]

Download Composer's installer and current SHA-384 signature over HTTPS into a
private temporary directory, verify the installer, then install composer into
/usr/local/bin. Temporary files are removed on exit. PHP is not rebuilt.

Environment:
  PHP                   PHP executable (default: /opt/php-zts-embed/bin/php)
  COMPOSER_INSTALL_DIR  Destination directory (default: /usr/local/bin)
  TMPDIR                Temporary directory parent (default: /tmp)

Only copying the verified PHAR to a protected destination uses sudo when needed.
USAGE
}
case "${1:-help}" in
  help|-h|--help) usage; exit 0 ;;
  install) [[ $# == 1 ]] || { usage >&2; exit 2; } ;;
  *) usage >&2; exit 2 ;;
esac

php_bin="${PHP:-/opt/php-zts-embed/bin/php}"
install_dir="${COMPOSER_INSTALL_DIR:-/usr/local/bin}"
[[ "$install_dir" == /* ]] || { printf 'COMPOSER_INSTALL_DIR must be absolute.\n' >&2; exit 1; }
command -v "$php_bin" >/dev/null
command -v curl >/dev/null
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/composer-install.XXXXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT
cd "$work_dir"

curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
  https://composer.github.io/installer.sig -o installer.sig
curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
  https://getcomposer.org/installer -o composer-setup.php
"$php_bin" -r '
$expected = trim(file_get_contents("installer.sig"));
$actual = hash_file("sha384", "composer-setup.php");
if (!preg_match("/\\A[0-9a-f]{96}\\z/", $expected) || !hash_equals($expected, $actual)) {
    fwrite(STDERR, "Composer installer signature verification failed.\n");
    exit(1);
}
echo "Composer installer verified.\n";
'
"$php_bin" composer-setup.php --filename=composer.phar
"$php_bin" composer.phar --version --no-ansi
if [[ $EUID == 0 || ( -d "$install_dir" && -w "$install_dir" ) ]]; then
  install -d "$install_dir"
  install -m 0755 composer.phar "$install_dir/composer"
else
  sudo install -d "$install_dir"
  sudo install -m 0755 composer.phar "$install_dir/composer"
fi
printf 'Installed %s/composer\n' "$install_dir"
