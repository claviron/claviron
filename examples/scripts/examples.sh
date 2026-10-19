#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  echo 'Usage: examples/scripts/examples.sh <start|stop|status> <all|fastapi|octane|orbit> [launcher options...]'
  echo 'Names with -app are also accepted. Edge is always included.'
}

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  usage
  exit 0
fi
if [[ $# -lt 2 ]]; then
  usage >&2
  exit 2
fi

action="$1"
target="$2"
shift 2
case "$action" in
start | stop | status) ;;
*)
  usage >&2
  exit 2
  ;;
esac

case "$target" in
all) apps=(fastapi-app octane-app orbit-app) ;;
fastapi | fastapi-app) apps=(fastapi-app) ;;
octane | octane-app) apps=(octane-app) ;;
orbit | orbit-app) apps=(orbit-app) ;;
*)
  usage >&2
  exit 2
  ;;
esac

run() {
  local app="$1" result
  shift
  if [[ "$action" == start ]]; then
    if bash "$SCRIPT_DIR/$app.sh" status >/dev/null 2>&1; then
      printf '%s: already running.\n' "$app"
      return 0
    else
      result=$?
      [[ "$result" == 3 ]] || return "$result"
    fi
  fi
  bash "$SCRIPT_DIR/$app.sh" "$action" "$@"
}

result=0
if [[ "$action" != stop ]]; then
  run edge "$@" || {
    result=$?
    [[ "$action" != start ]] || exit "$result"
  }
fi
for app in "${apps[@]}"; do
  run "$app" "$@" || {
    result=$?
    [[ "$action" != start ]] || exit "$result"
  }
done
if [[ "$action" == stop ]]; then
  run edge "$@" || result=$?
fi
exit "$result"
