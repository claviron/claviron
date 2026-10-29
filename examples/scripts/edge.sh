#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
EXAMPLE_ROOT="$ROOT/examples/edge-server"
cd "$EXAMPLE_ROOT"
source "$ROOT/examples/scripts/lib/application.sh"
source "$ROOT/examples/scripts/lib/build.sh"
source "$ROOT/examples/scripts/lib/mimalloc.sh"
mimalloc_init
application_init "$EXAMPLE_ROOT" edge-server "$EXAMPLE_ROOT/logs" "$@"
set -- "${APP_ARGS[@]}"

usage() {
  cat <<'HELP'
Usage: examples/scripts/edge.sh [start|run|status|stop] [--bench] [--release] [--native] [--sudo] [--no-build] [server options...]

Build the edge-server debug binary, then run it in the foreground as the current user.
--release builds and runs the release binary instead.
--native builds with target-cpu=native for this machine's CPU.
--bench enables the optional native BenchService during the build.
--sudo elevates only the server launch; the build still runs as your user.
--no-build skips the build. Use -- to pass remaining options unchanged.
Ctrl-C stops the server through its normal shutdown handling.
start runs in the background; run (the default) stays in the foreground.
status reports the recorded process and logs; stop requests graceful shutdown.
Background lifecycle commands run as the current user, without --sudo.

Environment:
  RUST_LOG    Log filter (default: info)
  WORKERS     Worker count (default: 2)
  CONFIG_DIR  Runtime config directory (default: runtime)
  FLEET       Orbit fleet name (default: edge)
  PHP         ZTS embed PHP binary used during the build
  PHP_CONFIG  Matching php-config binary used during the build
  CARGO_TARGET_DIR  Build output directory (otherwise this application's target/)

Examples:
  examples/scripts/edge.sh
  examples/scripts/edge.sh start --no-build
  examples/scripts/edge.sh status
  examples/scripts/edge.sh stop
  examples/scripts/edge.sh --sudo
  examples/scripts/edge.sh --release
  examples/scripts/edge.sh --release --native
  examples/scripts/edge.sh --bench --release
  examples/scripts/edge.sh --release --no-build
  examples/scripts/edge.sh --no-build --sudo
  RUST_LOG=edge=debug,claviron=info examples/scripts/edge.sh
  WORKERS=0 examples/scripts/edge.sh --no-build
HELP
  mimalloc_help
}

use_sudo=false
skip_build=false
enable_bench=false
enable_native=false
build_profile=debug
build_args=()
server_args=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
  --mimalloc)
    [[ $# -gt 1 ]] || { echo '--mimalloc requires default, release, aggressive or retain.' >&2; exit 2; }
    shift; mimalloc_mode="$1" ;;
  --mimalloc=*) mimalloc_mode="${1#*=}" ;;
  --mimalloc-purge-delay-ms)
    [[ $# -gt 1 ]] || { echo '--mimalloc-purge-delay-ms requires -1 or a non-negative integer.' >&2; exit 2; }
    shift; mimalloc_delay="$1"; mimalloc_delay_set=true ;;
  --mimalloc-purge-delay-ms=*) mimalloc_delay="${1#*=}"; mimalloc_delay_set=true ;;
  --sudo) use_sudo=true ;;
  --no-build) skip_build=true ;;
  --bench) enable_bench=true ;;
  --native) enable_native=true ;;
  --release)
    build_profile=release
    build_args=(--release)
    ;;
  --help | -h)
    usage
    exit 0
    ;;
  --)
    shift
    server_args+=("$@")
    break
    ;;
  *) server_args+=("$1") ;;
  esac
  shift
done

mimalloc_validate

if [[ "$enable_bench" == true ]]; then
  build_args+=(--features claviron/bench)
fi

if [[ "$EUID" -eq 0 ]]; then
  echo "Run as your development user; use --sudo to elevate only the server launch." >&2
  exit 1
fi

if [[ "$use_sudo" == true && "$APP_ACTION" == start ]]; then
  echo '--sudo requires foreground run mode.' >&2
  exit 2
fi
application_guard

if [[ "$skip_build" == false ]]; then
  example_build "$enable_native" cargo build --manifest-path "$EXAMPLE_ROOT/Cargo.toml" \
    --bin edge-server "${build_args[@]}"
fi

HOSTNAME=$(hostname -s)
worker_args=(--workers "${WORKERS:-2}")
fleet_args=(--fleet "${FLEET:-edge}")
config_args=(--config "${CONFIG_DIR:-$EXAMPLE_ROOT/config}")
for argument in "${server_args[@]}"; do
  case "$argument" in --workers|--workers=*) worker_args=() ;; esac
  case "$argument" in --fleet|--fleet=*|--fleet-name|--fleet-name=*) fleet_args=() ;; esac
  case "$argument" in --config|--config=*|--config-dir|--config-dir=*) config_args=() ;; esac
done

launcher=()
if [[ "$use_sudo" == true ]]; then
  launcher=(sudo)
fi

runtime_env=("RUST_LOG=${RUST_LOG:-info}")
mimalloc_apply
allocator_unset_args=()
if [[ "$mimalloc_mode" == release ]]; then
  # Also clear sudo env_keep values when the profile delegates to native defaults.
  for allocator_key in MIMALLOC_PURGE_DELAY MIMALLOC_PURGE_DECOMMITS MIMALLOC_RESET_DELAY MIMALLOC_RESET_DECOMMITS; do
    [[ -n "${!allocator_key+x}" ]] || allocator_unset_args+=(-u "$allocator_key")
  done
fi
# Forward allocator settings explicitly through sudo, including inherited options.
for allocator_key in "${!MIMALLOC_@}"; do
  runtime_env+=("$allocator_key=${!allocator_key}")
done

export "${runtime_env[@]}"
if [[ "$use_sudo" == true ]]; then
  application_unlock
  exec "${launcher[@]}" env "${allocator_unset_args[@]}" "${runtime_env[@]}" \
    "${CARGO_TARGET_DIR:-$EXAMPLE_ROOT/target}/$build_profile/edge-server" \
    "${config_args[@]}" --environment "${HOSTNAME}" \
    --name edge-server "${fleet_args[@]}" "${worker_args[@]}" "${server_args[@]}"
fi
application_exec \
  "${CARGO_TARGET_DIR:-$EXAMPLE_ROOT/target}/$build_profile/edge-server" \
  "${config_args[@]}" \
  --environment "${HOSTNAME}" \
  --name edge-server \
  "${fleet_args[@]}" \
  "${worker_args[@]}" "${server_args[@]}"
