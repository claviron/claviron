#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="$REPO_ROOT/examples/orbit-app"
ORBIT_ROOT="${ORBIT_ROOT:-$(cd -- "$REPO_ROOT/.." && pwd)/orbit-js}"
action=run
case "${1:-}" in start|run|status|stop) action="$1"; shift ;; esac
runtime=deno
case "${1:-}" in node|bun|deno) runtime="$1"; shift ;; esac
cd "$ROOT"
source "$REPO_ROOT/examples/scripts/lib/application.sh"
source "$REPO_ROOT/examples/scripts/lib/build.sh"
application_init "$ROOT" "orbit-app-$runtime" "$ROOT/logs" "$action" "$@"
set -- "${APP_ARGS[@]}"
skip_build=false
enable_native=false
sync=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build) skip_build=true ;;
    --native) enable_native=true ;;
    --sync) sync=true ;;
    --release) ;; # Native addons are release builds by default.
    --bench) export EXAMPLES_BENCH=1 ;;
    --help|-h)
      echo 'Usage: examples/scripts/orbit-app.sh [start|run|status|stop] [node|bun|deno] [--sync] [--release] [--native] [--bench] [--no-build]'
      echo '--native builds the Rust addon for this CPU; the JavaScript runtime is unchanged.'
      echo 'ORBIT_WORKERS (or WORKERS) selects child processes; default 8. ORBIT_FLEET and ORBIT_APP select the Link target.'
      echo 'Each runtime has a separate PID/log record. Stop forwards SIGTERM only to its owned children.'
      exit 0 ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
  shift
done
application_guard
if [[ "$sync" == true ]]; then npm ci; fi
if [[ "$skip_build" == false ]]; then example_build "$enable_native" "$ORBIT_ROOT/scripts/build-native.sh" release; fi
export RUST_LOG="${RUST_LOG:-warn}"
APP_CONFIG_INFO="$ROOT/.env (benchmark mode uses the supplied environment)"
APP_FLEET_INFO="${ORBIT_FLEET:-orj}"
application_exec "$(command -v node)" "$REPO_ROOT/examples/scripts/lib/javascript.mjs" \
  "$runtime" "$ROOT" "${ORBIT_WORKERS:-${WORKERS:-8}}"
