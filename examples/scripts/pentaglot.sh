#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
APP="$ROOT/examples/pentaglot"
cd "$APP"
source "$ROOT/examples/scripts/lib/application.sh"
source "$ROOT/examples/scripts/lib/build.sh"
application_init "$APP" pentaglot "$APP/logs" "$@"
set -- "${APP_ARGS[@]}"

profile=debug
native=false
skip_build=false
sync=false
build_args=()
server_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) profile=release; build_args+=(--release) ;;
    --native) native=true ;;
    --no-build) skip_build=true ;;
    --sync) sync=true ;;
    --help|-h)
      cat <<'HELP'
Usage: examples/scripts/pentaglot.sh [start|run|status|stop] [--sync] [--release] [--native] [--no-build] [-- server options]

Run Rust, Lua, Deno, PHP and Gufi-ASGI applications in one Claviron Edge.
Claviron reads PENTAGLOT_LISTEN from .env or the inherited environment.
CONFIG_DIR defaults to config/.
FLEET defaults to penta; WORKERS defaults to 0 (one standalone serving process).
PHP/PHP_CONFIG select the ZTS embed build; PYO3_PYTHON selects CPython 3.15.
--sync installs the locked Symfony dependencies using the selected PHP binary.
HELP
      exit 0 ;;
    --) shift; server_args+=("$@"); break ;;
    *) server_args+=("$1") ;;
  esac
  shift
done

application_guard
if [[ "$sync" == true ]]; then
  [[ -n "${PHP:-}" && -x "$PHP" ]] || {
    echo 'Set PHP to the selected ZTS PHP executable before using --sync.' >&2
    exit 1
  }
  "$PHP" "$(command -v composer)" install --working-dir="$APP/apps/php" \
    --no-dev --no-interaction --no-progress --no-plugins --no-scripts
fi
if [[ ! -f "$APP/apps/php/vendor/autoload.php" ]]; then
  echo 'PHP dependencies missing: use --sync with PHP set to the selected ZTS executable.' >&2
  exit 1
fi
if [[ "$skip_build" == false ]]; then
  example_build "$native" cargo build --workspace --all-features --all-targets "${build_args[@]}"
fi

export RUST_LOG="${RUST_LOG:-info}"
config_args=(--config "${CONFIG_DIR:-$APP/config}")
fleet_args=(--fleet "${FLEET:-penta}")
worker_args=(--workers "${WORKERS:-0}")
for argument in "${server_args[@]}"; do
  case "$argument" in --config|--config=*) config_args=() ;; esac
  case "$argument" in --fleet|--fleet=*) fleet_args=() ;; esac
  case "$argument" in --workers|--workers=*) worker_args=() ;; esac
done
application_exec "${CARGO_TARGET_DIR:-$APP/target}/$profile/pentaglot" \
  "${config_args[@]}" "${fleet_args[@]}" "${worker_args[@]}" "${server_args[@]}"
