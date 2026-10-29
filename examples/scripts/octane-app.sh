#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="$REPO_ROOT/examples/octane-app"
SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"
BIN="${BIN:-${SCRIPT_NAME%.sh}}"
FLEET="${FLEET:-octane}"
cd "$ROOT"
source "$REPO_ROOT/examples/scripts/lib/application.sh"
source "$REPO_ROOT/examples/scripts/lib/build.sh"
source "$REPO_ROOT/examples/scripts/lib/mimalloc.sh"
mimalloc_init
application_init "$ROOT" octane-app "$ROOT/storage/logs" "$@"
set -- "${APP_ARGS[@]}"

usage() {
  cat <<HELP
Usage: examples/scripts/$SCRIPT_NAME [start|run|status|stop] [--bench] [--release] [--native] [--sudo] [--no-build] [server options...]

Build examples/octane-app, then run it in the foreground as the current user.
--release builds and runs the release binary instead.
--native builds the Rust runner with target-cpu=native for this machine.
--bench enables shared native baselines and Laravel workload routes.
--sudo elevates only the server launch; the build still runs as your user.
--no-build skips the build. Use -- to pass remaining options unchanged.
Ctrl-C stops the server through its normal shutdown handling.
start runs in the background; run (the default) stays in the foreground.
status reports the recorded process and logs; stop requests graceful shutdown.
Background lifecycle commands run as the current user, without --sudo.

Environment:
  RUST_LOG    Log filter (default: info)
  WORKERS     Worker count (default: 2)
  BIN         Binary and application name (default: script basename without .sh)
  FLEET       Orbit fleet name (default: octane)
  CONFIG_PATH Runner configuration file (default: lumenize.yaml)
  EXAMPLES_BENCH  1 enables shared Laravel benchmarks (set by --bench)
  PHP         ZTS embed PHP binary used during the build
  PHP_CONFIG  Matching php-config binary used during the build
  CARGO_TARGET_DIR  Build output directory (otherwise this application's target/)

Examples:
  examples/scripts/$SCRIPT_NAME
  examples/scripts/$SCRIPT_NAME start --no-build
  examples/scripts/$SCRIPT_NAME status
  examples/scripts/$SCRIPT_NAME stop
  examples/scripts/$SCRIPT_NAME --sudo
  examples/scripts/$SCRIPT_NAME --release
  examples/scripts/$SCRIPT_NAME --bench --release
  examples/scripts/$SCRIPT_NAME --release --no-build
  examples/scripts/$SCRIPT_NAME --no-build --sudo
  RUST_LOG=lumenize=debug,claviron=info examples/scripts/$SCRIPT_NAME
  WORKERS=0 examples/scripts/$SCRIPT_NAME --no-build
HELP
  mimalloc_help
}

use_sudo=false
skip_build=false
enable_native=false
enable_bench=false
enable_deno=false
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
  --native) enable_native=true ;;
  --bench) enable_bench=true ;;
  --deno) enable_deno=true ;;
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
  export EXAMPLES_BENCH=1
  build_args+=(--features lumenize/bench)
fi

if [[ "$enable_deno" == true ]]; then
  build_args+=(--features deno)
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
  # The workspace's macOS ZTS embed toolchain; never select Homebrew PHP.
  example_build "$enable_native" cargo build --bin "$BIN" "${build_args[@]}"
fi

HOSTNAME=$(hostname -s)

launcher=()
if [[ "$use_sudo" == true ]]; then
  launcher=(sudo)
fi

runtime_env=("RUST_LOG=${RUST_LOG:-info}" "EXAMPLES_BENCH=${EXAMPLES_BENCH:-0}")
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
application_url="${APP_URL:-$(sed -n 's/^APP_URL=//p' .env)}"
if [[ -n "$application_url" ]]; then
  printf 'Application URL (through Edge): %s\n' "$application_url"
fi
if [[ "$use_sudo" == true ]]; then
  application_unlock
  exec "${launcher[@]}" env "${allocator_unset_args[@]}" "${runtime_env[@]}" \
    "${CARGO_TARGET_DIR:-$ROOT/target}/$build_profile/$BIN" \
    --config "${CONFIG_PATH:-lumenize.yaml}" --environment "${HOSTNAME}" \
    --name "$BIN" --fleet "$FLEET" --workers "${WORKERS:-2}" "${server_args[@]}"
fi
application_exec \
  "${CARGO_TARGET_DIR:-$ROOT/target}/$build_profile/$BIN" \
  --config "${CONFIG_PATH:-lumenize.yaml}" \
  --environment "${HOSTNAME}" \
  --name "$BIN" \
  --fleet "$FLEET" \
  --workers "${WORKERS:-2}" "${server_args[@]}"
