#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
EXAMPLE_ROOT="$ROOT/examples/edge-server"
cd "$EXAMPLE_ROOT"
source "$ROOT/examples/scripts/lib/application.sh"
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
  build_env=()
  if [[ "$enable_native" == true ]]; then
    # Cargo gives encoded flags precedence; preserve the caller's active flags.
    if [[ -n "${CARGO_ENCODED_RUSTFLAGS+x}" ]]; then
      build_env+=("CARGO_ENCODED_RUSTFLAGS=${CARGO_ENCODED_RUSTFLAGS:+${CARGO_ENCODED_RUSTFLAGS}$'\x1f'}-C"$'\x1f'"target-cpu=native")
    else
      build_env+=("RUSTFLAGS=${RUSTFLAGS:+$RUSTFLAGS }-C target-cpu=native")
    fi
  fi
  env "${build_env[@]}" cargo build --manifest-path "$EXAMPLE_ROOT/Cargo.toml" \
    --bin edge-server "${build_args[@]}"
fi

HOSTNAME=$(hostname -s)

launcher=()
if [[ "$use_sudo" == true ]]; then
  launcher=(sudo)
fi

runtime_env=("RUST_LOG=${RUST_LOG:-info}")
if [[ -n "${MIMALLOC_PURGE_DELAY+x}" ]]; then
  # Preserve an explicit allocator setting even when sudo sanitizes the environment.
  runtime_env+=("MIMALLOC_PURGE_DELAY=$MIMALLOC_PURGE_DELAY")
fi

export "${runtime_env[@]}"
if [[ "$use_sudo" == true ]]; then
  application_unlock
  exec "${launcher[@]}" env "${runtime_env[@]}" \
    "${CARGO_TARGET_DIR:-$EXAMPLE_ROOT/target}/$build_profile/edge-server" \
    --config "${CONFIG_DIR:-$EXAMPLE_ROOT/config}" --environment "${HOSTNAME}" \
    --name edge-server --fleet edge --workers "${WORKERS:-2}" "${server_args[@]}"
fi
application_exec \
  "${CARGO_TARGET_DIR:-$EXAMPLE_ROOT/target}/$build_profile/edge-server" \
  --config "${CONFIG_DIR:-$EXAMPLE_ROOT/config}" \
  --environment "${HOSTNAME}" \
  --name edge-server \
  --fleet edge \
  --workers "${WORKERS:-2}" "${server_args[@]}"
