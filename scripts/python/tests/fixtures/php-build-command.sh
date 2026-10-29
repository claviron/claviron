#!/usr/bin/env bash
set -euo pipefail
command_name="$(basename "$0")"
printf '%s\0' "$command_name" "${CFLAGS:-}" "${CXXFLAGS:-}" "$@" >> "$CHECK_LOG"
printf '\n' >> "$CHECK_LOG"
case "$command_name" in
  cc|c++) printf 'fixture compiler %s\n' "${COMPILER_REVISION:-1}" ;;
  uname) printf '%s\n' "${FIXTURE_OS:-Linux}" ;;
  php) printf '%s\n' "${FIXTURE_PHP_PROFILE:-release}" ;;
  config.status) printf 'PHP config.status %s\n' "$PHP_VERSION" ;;
  pecl)
    printf '%s\n' "$TMPDIR" > "$PECL_TEMP_RECORD"
    touch "$FIXTURE_MARKER"
    ;;
  configure)
    [[ "${FAIL_CONFIGURE:-0}" == 0 ]] || exit 1
    touch Makefile
    mkdir -p main
    touch main/php_config.h main/build-defs.h
    cp "$COMMAND_FIXTURE" config.status
    ;;
  make|gmake)
    if [[ "${1:-}" == clean ]]; then rm -f old.o; fi
    if [[ "${FAIL_MAKE:-0}" == 1 && "${1:-}" == -j1 ]]; then exit 1; fi
    ;;
esac
