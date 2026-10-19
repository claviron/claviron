#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="$REPO_ROOT/examples/orbit-app"
cd "$ROOT"
source "$REPO_ROOT/examples/scripts/lib/application.sh"
application_init "$ROOT" orbit-app "$ROOT/logs" "$@"
set -- "${APP_ARGS[@]}"

case "${1:-deno}" in
deno) runtime=(deno run --allow-ffi --allow-read --allow-env --allow-net --allow-sys --env-file=.env server.js) ;;
node) runtime=(node --env-file-if-exists=.env server.js) ;;
bun) runtime=(bun server.js) ;;
--help | -h)
  printf '%s\n' \
    'Usage: examples/scripts/orbit-app.sh [start|run|status|stop] [deno|node|bun]' \
    'Run the JavaScript Orbit example; defaults to Deno.' \
    'start runs in the background; run (the default) stays in the foreground.' \
    'status reports the recorded process and logs; stop requests graceful shutdown.' \
    'Install dependencies, build the frontend and prepare .env first; see its README.'
  exit 0
  ;;
*)
  printf 'Unknown runtime: %s\n' "$1" >&2
  exit 2
  ;;
esac

application_guard
APP_CONFIG_INFO="$ROOT/.env"
APP_FLEET_INFO="${ORBIT_FLEET:-from .env (ORBIT_FLEET)}"
application_exec "${runtime[@]}"
