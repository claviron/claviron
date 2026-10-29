#!/bin/sh
set -eu

usage() {
  cat <<'USAGE'
Usage: sudo env PHP_VERSION=VERSION scripts/install/php/php-zts-fpm-install-freebsd.sh install
       scripts/install/php/php-zts-fpm-install-freebsd.sh [help|--help|-h]

FreeBSD only. Read php_zts_fpm settings through rc.conf and rc.conf.d, validate
that runtime and its existing FPM/INI configuration, then install the embedded
rc.d script only if /usr/local/etc/rc.d/php_zts_fpm does not already exist.
Existing service files and rc settings are preserved. No enable, start, stop,
restart, PHP build or configuration rewrite is performed.

Settings use the existing php_zts_fpm_prefix, php_zts_fpm_config,
php_zts_fpm_ini and php_zts_fpm_scan_dir variables. Default prefix:
/opt/php-zts-embed. PHP_VERSION is the expected installed PHP version.

Example for PHP 8.5.12 (replace the version with the release you expect):
  sudo env PHP_VERSION=8.5.12 ./scripts/install/php/php-zts-fpm-install-freebsd.sh install

To inspect the installed PHP version before choosing the expected version:
  /opt/php-zts-embed/bin/php -v

PHP_VERSION is checked before root privileges; missing values stop installation.
Running without arguments only prints this help. With install, an existing
rc.d service is preserved and only validated; a missing service file is created.
The running FPM service is never restarted by this installer.
USAGE
}
case "${1:-help}" in
  help|--help|-h) usage; exit 0 ;;
  install) [ "$#" -eq 1 ] || { usage >&2; exit 2; } ;;
  *) usage >&2; exit 2 ;;
esac
[ "$(uname -s)" = FreeBSD ] || { echo 'This installer requires FreeBSD.' >&2; exit 1; }
if [ -z "${PHP_VERSION:-}" ]; then
  printf 'PHP_VERSION is required; no installation was performed.\n\n' >&2
  usage >&2
  exit 1
fi
case "$PHP_VERSION" in
  *[!0-9A-Za-z._-]*)
    echo 'PHP_VERSION contains invalid characters; no installation was performed.' >&2
    exit 1
    ;;
esac
[ "$(id -u)" -eq 0 ] || {
  printf 'Environment checked. Run with sudo: sudo env PHP_VERSION=%s %s install\n' "$PHP_VERSION" "$0" >&2
  exit 1
}

# Use FreeBSD's native configuration loading, including per-service overrides.
# rc.subr is a system shell library and is not written for nounset mode.
set +u
. /etc/rc.subr
name=php_zts_fpm
load_rc_config "$name"
set -u
: "${php_zts_fpm_prefix:=/opt/php-zts-embed}"
: "${php_zts_fpm_config:=${php_zts_fpm_prefix}/etc/php-fpm.conf}"
: "${php_zts_fpm_ini:=${php_zts_fpm_prefix}/etc/php.ini}"
: "${php_zts_fpm_scan_dir:=${php_zts_fpm_prefix}/etc/conf.d/fpm}"

[ -r "$php_zts_fpm_config" ] && [ -r "$php_zts_fpm_ini" ] &&
  [ -d "$php_zts_fpm_scan_dir" ] || {
  echo 'Existing PHP/FPM configuration is incomplete.' >&2; exit 1;
}
actual_version="$("$php_zts_fpm_prefix/bin/php" -n -r 'echo PHP_VERSION;')"
[ "$actual_version" = "$PHP_VERSION" ] || {
  printf 'Expected PHP %s, found %s.\n' "$PHP_VERSION" "$actual_version" >&2
  exit 1
}
export PHPRC="$php_zts_fpm_ini" PHP_INI_SCAN_DIR="$php_zts_fpm_scan_dir"
"$php_zts_fpm_prefix/sbin/php-fpm" -t -y "$php_zts_fpm_config" -c "$php_zts_fpm_ini"
service_file=/usr/local/etc/rc.d/php_zts_fpm
if [ -e "$service_file" ] || [ -L "$service_file" ]; then
  printf 'Validated PHP %s; preserving existing %s and rc settings.\n' "$actual_version" "$service_file"
  exit 0
fi
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/php-zts-fpm-rc.XXXXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT
cat > "$work_dir/php_zts_fpm" <<'RC_SCRIPT'
#!/bin/sh

# PROVIDE: php_zts_fpm
# REQUIRE: NETWORKING
# KEYWORD: shutdown

. /etc/rc.subr

name="php_zts_fpm"
rcvar="php_zts_fpm_enable"

load_rc_config "$name"

: ${php_zts_fpm_enable:="NO"}
: ${php_zts_fpm_prefix:="/opt/php-zts-embed"}
: ${php_zts_fpm_config:="${php_zts_fpm_prefix}/etc/php-fpm.conf"}
: ${php_zts_fpm_ini:="${php_zts_fpm_prefix}/etc/php.ini"}
: ${php_zts_fpm_scan_dir:="${php_zts_fpm_prefix}/etc/conf.d/fpm"}
: ${php_zts_fpm_log_dir:="/var/log/php-zts-fpm"}
: ${php_zts_fpm_log_owner:="root"}
: ${php_zts_fpm_log_group:="wheel"}

command="${php_zts_fpm_prefix}/sbin/php-fpm"
procname="${command}"
pidfile="/var/run/php-zts-fpm.pid"
required_files="${php_zts_fpm_config} ${php_zts_fpm_ini}"
command_args="-y ${php_zts_fpm_config} -c ${php_zts_fpm_ini}"
start_precmd="${name}_prestart"
sig_stop=QUIT
sig_reload=USR2
extra_commands=reload

php_zts_fpm_prestart()
{
	install -d -o "$php_zts_fpm_log_owner" -g "$php_zts_fpm_log_group" -m 0755 \
		"$php_zts_fpm_log_dir"
	export PHPRC="${php_zts_fpm_ini}"
	export PHP_INI_SCAN_DIR="${php_zts_fpm_scan_dir}"
}

run_rc_command "$1"
RC_SCRIPT
sh -n "$work_dir/php_zts_fpm"
install -d /usr/local/etc/rc.d
install -m 0755 "$work_dir/php_zts_fpm" "$service_file"
printf 'Installed %s; service enablement and running processes are unchanged.\n' "$service_file"
