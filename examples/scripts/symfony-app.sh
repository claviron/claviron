#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKSPACE_ROOT="$(cd -- "$REPO_ROOT/.." && pwd)"
APP_ROOT="${SYMFONY_ROOT:-$WORKSPACE_ROOT/web/symfony-app}"
SKENE_ROOT="${SKENE_ROOT:-$WORKSPACE_ROOT/skene}"
cd "$APP_ROOT"
source "$REPO_ROOT/examples/scripts/lib/application.sh"
application_init "$APP_ROOT" symfony-app "$APP_ROOT/var/log/runner" "$@"
set -- "${APP_ARGS[@]}"

profile=debug
build_args=()
skip_build=false
server_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) profile=release; build_args=(--release) ;;
    --no-build) skip_build=true ;;
    --help|-h)
      echo 'Usage: examples/scripts/symfony-app.sh [start|run|status|stop] [--release] [--no-build] [Skene options...]'
      echo 'SYMFONY_ROOT selects the PHP application; SKENE_ROOT selects its native runner.'
      echo 'Builds Skene with its default features: native bench interception stays disabled.'
      exit 0 ;;
    *) server_args+=("$1") ;;
  esac
  shift
done
application_guard
if [[ "$skip_build" == false ]]; then
  cargo build --manifest-path "$SKENE_ROOT/Cargo.toml" --bin skene "${build_args[@]}"
fi
worker_args=()
[[ -z "${WORKERS:-}" ]] || worker_args=(--workers "$WORKERS")
for arg in "${server_args[@]}"; do
  case "$arg" in --workers|--workers=*) worker_args=() ;; esac
done
application_exec "${CARGO_TARGET_DIR:-$SKENE_ROOT/target}/$profile/skene" \
  --root "$APP_ROOT" --config "${CONFIG_PATH:-skene.yaml}" --production \
  "${worker_args[@]}" "${server_args[@]}"
