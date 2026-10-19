#!/usr/bin/env bash
# Shared local lifecycle for the independent applications in this repository.

application_stamp() {
  LC_ALL=C ps -p "$1" -o lstart= 2>/dev/null |
    sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

application_read_pid() {
  [[ -f "$APP_PID_FILE" ]] || return 1
  {
    IFS= read -r APP_PID
    IFS= read -r APP_STAMP
    IFS= read -r APP_EXECUTABLE
    IFS= read -r APP_OUTPUT
    IFS= read -r APP_CONFIG_INFO
    IFS= read -r APP_FLEET_INFO
  } <"$APP_PID_FILE" || return 1
  [[ "$APP_PID" =~ ^[0-9]+$ && "$APP_PID" -gt 1 && -n "$APP_STAMP" && "$APP_EXECUTABLE" = /* ]]
}

application_identity() {
  application_read_pid || return 1
  kill -0 "$APP_PID" 2>/dev/null || return 1
  [[ "$(application_stamp "$APP_PID")" == "$APP_STAMP" ]] || return 1
  local executable owner state
  executable="$(ps -p "$APP_PID" -o comm= 2>/dev/null | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
  owner="$(ps -p "$APP_PID" -o uid= 2>/dev/null | tr -d '[:space:]')"
  state="$(ps -p "$APP_PID" -o stat= 2>/dev/null | tr -d '[:space:]')"
  [[ "$executable" == "$APP_EXECUTABLE" && "$owner" == "$(id -u)" && "$state" != Z* ]]
}

application_status() {
  printf 'Application: %s\nDirectory: %s\nPID file: %s\n' "$APP_NAME" "$APP_ROOT" "$APP_PID_FILE"
  if ! application_identity; then
    printf 'State: stopped or not tracked (no matching saved process)\n'
    return 3
  fi
  printf 'State: running\nPID: %s\nExecutable: %s\nOutput: %s\n' \
    "$APP_PID" "$APP_EXECUTABLE" "$APP_OUTPUT"
  printf 'Config: %s\nFleet: %s\n' "$APP_CONFIG_INFO" "$APP_FLEET_INFO"
  ps -p "$APP_PID" -o pid=,ppid=,etime=,comm=
  local children pids
  children="$(ps -axo pid=,ppid= | awk -v parent="$APP_PID" '$2 == parent { print $1 }' | paste -sd, -)"
  pids="$APP_PID${children:+,$children}"
  if command -v lsof >/dev/null 2>&1; then
    printf 'TCP listeners (master and workers; Orbit needs no TCP listener):\n'
    lsof -nP -a -p "$pids" -iTCP -sTCP:LISTEN 2>/dev/null || true
  fi
}

application_stop() {
  if ! application_identity; then
    printf '%s: no matching saved process; no signal sent.\n' "$APP_NAME"
    return 0
  fi
  # The supervisor owns worker drain. Never signal a process group or escalate.
  kill -TERM "$APP_PID"
  printf '%s: graceful shutdown requested for PID %s. Use status to confirm exit.\n' "$APP_NAME" "$APP_PID"
}

application_init() {
  APP_ROOT="$1"
  APP_NAME="$2"
  APP_LOG_DIR="$3"
  shift 3
  APP_ACTION=run
  case "${1:-}" in
  start | run | status | stop)
    APP_ACTION="$1"
    shift
    ;;
  esac
  # The entrypoint restores these arguments after lifecycle dispatch.
  # shellcheck disable=SC2034
  APP_ARGS=("$@")
  APP_CONFIG_INFO='not specified'
  APP_FLEET_INFO='from application configuration'
  APP_PID_FILE="$APP_LOG_DIR/$APP_NAME.pid"
  APP_LOG_FILE="$APP_LOG_DIR/$APP_NAME.console.log"
  APP_LOCK_DIR="$APP_LOG_DIR/$APP_NAME.lock"
  case "$APP_ACTION" in
  status)
    application_status
    exit $?
    ;;
  stop)
    application_stop
    exit $?
    ;;
  esac
}

application_unlock() {
  if [[ "${APP_LOCK_HELD:-false}" == true ]]; then
    rmdir "$APP_LOCK_DIR"
    APP_LOCK_HELD=false
  fi
}

application_guard() {
  local argument
  for argument in "${APP_ARGS[@]}"; do
    case "$argument" in --help | -h) return 0 ;; esac
  done
  mkdir -p "$APP_LOG_DIR"
  if ! mkdir "$APP_LOCK_DIR" 2>/dev/null; then
    printf '%s: lifecycle command already in progress; inspect %s.\n' "$APP_NAME" "$APP_LOCK_DIR" >&2
    exit 1
  fi
  APP_LOCK_HELD=true
  trap application_unlock EXIT
  if application_read_pid && kill -0 "$APP_PID" 2>/dev/null &&
    [[ "$(application_stamp "$APP_PID")" == "$APP_STAMP" ]]; then
    printf '%s: recorded PID %s is still alive; use status before starting again.\n' "$APP_NAME" "$APP_PID" >&2
    exit 1
  fi
}

application_record() {
  local temporary
  temporary="$(mktemp "$APP_LOG_DIR/pid.XXXXXX")"
  printf '%s\n%s\n%s\n%s\n%s\n%s\n' \
    "$APP_PID" "$APP_STAMP" "$APP_EXECUTABLE" "$APP_OUTPUT" \
    "$APP_CONFIG_INFO" "$APP_FLEET_INFO" >"$temporary"
  mv "$temporary" "$APP_PID_FILE"
}

application_exec() {
  local command_path argument previous=''
  command_path="$(command -v "$1")" || {
    printf '%s: executable not found: %s\n' "$APP_NAME" "$1" >&2
    return 1
  }
  APP_EXECUTABLE="$(realpath "$command_path")"
  shift
  # CLI help does not start or claim an application process.
  for argument in "$@"; do
    case "$argument" in --help | -h)
      application_unlock
      exec "$APP_EXECUTABLE" "$@"
      ;;
    esac
    case "$previous" in
    --config | --config-dir | --config-path) APP_CONFIG_INFO="$argument" ;;
    --fleet | --fleet-name) APP_FLEET_INFO="$argument" ;;
    esac
    case "$argument" in
    --config=* | --config-dir=* | --config-path=*) APP_CONFIG_INFO="${argument#*=}" ;;
    --fleet=* | --fleet-name=*) APP_FLEET_INFO="${argument#*=}" ;;
    esac
    previous="$argument"
  done
  if [[ "$APP_ACTION" == start ]]; then
    APP_OUTPUT="$APP_LOG_FILE"
    # Detach the job from the launcher's terminal/process group before exit.
    set -m
    nohup "$APP_EXECUTABLE" "$@" </dev/null >>"$APP_LOG_FILE" 2>&1 &
    APP_PID=$!
    disown "$APP_PID"
    set +m
    APP_STAMP="$(application_stamp "$APP_PID")"
    if [[ -z "$APP_STAMP" ]]; then
      printf '%s: process exited during launch; inspect %s.\n' "$APP_NAME" "$APP_LOG_FILE" >&2
      return 1
    fi
    application_record
    application_unlock
    printf '%s: launched PID %s. Boot is asynchronous; use status and inspect %s.\n' "$APP_NAME" "$APP_PID" "$APP_LOG_FILE"
  else
    APP_PID=$$
    APP_STAMP="$(application_stamp "$APP_PID")"
    APP_OUTPUT='foreground terminal (application logs remain in its configured locations)'
    application_record
    application_unlock
    exec "$APP_EXECUTABLE" "$@"
  fi
}
