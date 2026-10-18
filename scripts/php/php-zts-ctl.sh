#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="php-zts-fpm"
PLIST_SRC_DEFAULT="$SCRIPT_DIR/${SERVICE_NAME}.plist"
PLIST_DEST="$HOME/Library/LaunchAgents/${SERVICE_NAME}.plist"
PLIST_SRC="${PLIST_SRC:-$PLIST_SRC_DEFAULT}"
LAUNCHCTL_TARGET="gui/$(id -u)/${SERVICE_NAME}"
LAUNCHCTL_DOMAIN="gui/$(id -u)"

usage() {
  cat <<EOF
Usage: $(basename "$0") <install|start|stop|restart|reload|status|watch|uninstall>

Commands:
  install    Copy plist into LaunchAgents and bootstrap the service
  start      Start the service
  stop       Stop the service
  restart    Restart the service
  reload     Restart the loaded service with launchctl kickstart -k
  status     Show whether the service is loaded and running
  watch      Tail the php-fpm log file
  uninstall  Stop service and remove plist from LaunchAgents
EOF
}

require_plist_source() {
  if [ ! -f "$PLIST_SRC" ]; then
    echo "Missing plist source: $PLIST_SRC" >&2
    exit 1
  fi
}

service_exists() {
  launchctl print "$LAUNCHCTL_TARGET" >/dev/null 2>&1
}

install_service() {
  require_plist_source
  mkdir -p "$HOME/Library/LaunchAgents"
  plutil -lint "$PLIST_SRC" >/dev/null
  cp "$PLIST_SRC" "$PLIST_DEST"
  launchctl bootout "$LAUNCHCTL_DOMAIN" "$PLIST_DEST" 2>/dev/null || true
  launchctl bootstrap "$LAUNCHCTL_DOMAIN" "$PLIST_DEST"
  launchctl enable "$LAUNCHCTL_TARGET" 2>/dev/null || true
  launchctl kickstart -k "$LAUNCHCTL_TARGET"
  echo "Installed and started: $SERVICE_NAME"
}

start_service() {
  if ! [ -f "$PLIST_DEST" ]; then
    echo "Missing installed plist: $PLIST_DEST" >&2
    echo "Run: $(basename "$0") install" >&2
    exit 1
  fi

  if service_exists; then
    launchctl kickstart -k "$LAUNCHCTL_TARGET"
  else
    launchctl bootstrap "$LAUNCHCTL_DOMAIN" "$PLIST_DEST"
    launchctl enable "$LAUNCHCTL_TARGET" 2>/dev/null || true
    launchctl kickstart -k "$LAUNCHCTL_TARGET"
  fi

  echo "Started: $SERVICE_NAME"
}

stop_service() {
  if service_exists; then
    launchctl bootout "$LAUNCHCTL_DOMAIN" "$PLIST_DEST"
    echo "Stopped: $SERVICE_NAME"
  else
    echo "Service not loaded: $SERVICE_NAME"
  fi
}

restart_service() {
  stop_service || true
  start_service
}

reload_service() {
  launchctl kickstart -k "$LAUNCHCTL_TARGET"
  echo "Reloaded: $SERVICE_NAME"
}

status_service() {
  if ! service_exists; then
    echo "Status: not loaded"
    exit 3
  fi

  local state pid last_exit_status
  state="$(launchctl print "$LAUNCHCTL_TARGET" 2>/dev/null | awk -F ' = ' '/state = / { print $2; exit }')"
  pid="$(launchctl print "$LAUNCHCTL_TARGET" 2>/dev/null | awk -F ' = ' '/pid = / { print $2; exit }')"
  last_exit_status="$(launchctl print "$LAUNCHCTL_TARGET" 2>/dev/null | awk -F ' = ' '/last exit code = / { print $2; exit }')"

  if [ -n "$pid" ]; then
    echo "Status: running"
    echo "PID: $pid"
    [ -n "$state" ] && echo "State: $state"
    exit 0
  fi

  echo "Status: loaded but not running"
  [ -n "$state" ] && echo "State: $state"
  [ -n "$last_exit_status" ] && echo "Last exit code: $last_exit_status"
  exit 1
}

watch_service() {
  tail -f "$HOME/dev/php-zts-embed/var/log/php-fpm.log"
}

uninstall_service() {
  launchctl bootout "$LAUNCHCTL_DOMAIN" "$PLIST_DEST" 2>/dev/null || true
  rm -f "$PLIST_DEST"
  echo "Uninstalled: $SERVICE_NAME"
}

main() {
  local cmd="${1:-}"

  case "$cmd" in
  install) install_service ;;
  start) start_service ;;
  stop) stop_service ;;
  restart) restart_service ;;
  reload) reload_service ;;
  status) status_service ;;
  watch) watch_service ;;
  uninstall) uninstall_service ;;
  -h | --help) usage ;;
  *)
    usage
    exit 1
    ;;
  esac
}

main "$@"
