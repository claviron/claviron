#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
EXAMPLE_ROOT="$ROOT/examples/bench-app"
cd "$EXAMPLE_ROOT"
source "$ROOT/examples/scripts/lib/application.sh"
application_init "$EXAMPLE_ROOT" bench-app "$EXAMPLE_ROOT/logs" "$@"
set -- "${APP_ARGS[@]}"

profile=debug
build_args=()
skip_build=false
args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) profile=release; build_args+=(--release) ;;
    --no-build) skip_build=true ;;
    --help|-h)
      echo 'Usage: examples/scripts/bench-app.sh [start|run|status|stop] [--release] [--no-build] [runner options...]'
      exit 0 ;;
    *) args+=("$1") ;;
  esac
  shift
done
application_guard
APP_CONFIG_INFO="$EXAMPLE_ROOT/bench-app.yaml"
APP_FLEET_INFO='benchapp (unless overridden by runner options/config)'
if [[ "$skip_build" == false ]]; then
  cargo build --workspace --all-features --all-targets "${build_args[@]}"
fi
application_exec "${CARGO_TARGET_DIR:-$EXAMPLE_ROOT/target}/$profile/bench-app" "${args[@]}"
