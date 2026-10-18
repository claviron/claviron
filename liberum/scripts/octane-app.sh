#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="$REPO_ROOT/liberum/octane-app"
SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"
BIN="${BIN:-${SCRIPT_NAME%.sh}}"
FLEET="${FLEET:-octane}"
cd "$ROOT"

usage() {
    cat <<HELP
Usage: liberum/scripts/$SCRIPT_NAME [--bench] [--release] [--sudo] [--no-build] [server options...]

Build liberum/octane-app, then run it in the foreground as the current user.
--release builds and runs the release binary instead.
--bench enables the shared Laravel workload routes at application boot.
--sudo elevates only the server launch; the build still runs as your user.
--no-build skips the build. Use -- to pass remaining options unchanged.
Ctrl-C stops the server through its normal shutdown handling.

Environment:
  RUST_LOG    Log filter (default: info)
  WORKERS     Worker count (default: 2)
  BIN         Binary and application name (default: script basename without .sh)
  FLEET       Orbit fleet name (default: octane)
  CONFIG_PATH Runner configuration file (default: lumenize.yaml)
  LIBERUM_BENCH  1 enables shared Laravel benchmarks (set by --bench)
  FUNDIST_TEST_MODE  1 enables vendor tests in debug-assertions builds only (default: 0)
  PHP         ZTS embed PHP binary used during the build
  PHP_CONFIG  Matching php-config binary used during the build
  CARGO_TARGET_DIR  Build output directory; set_cargo_target is used if available

Examples:
  liberum/scripts/$SCRIPT_NAME
  liberum/scripts/$SCRIPT_NAME --sudo
  liberum/scripts/$SCRIPT_NAME --release
  liberum/scripts/$SCRIPT_NAME --bench --release
  liberum/scripts/$SCRIPT_NAME --release --no-build
  liberum/scripts/$SCRIPT_NAME --no-build --sudo
  RUST_LOG=lumenize=debug,claviron=info liberum/scripts/$SCRIPT_NAME
  WORKERS=0 liberum/scripts/$SCRIPT_NAME --no-build
HELP
}

use_sudo=false
skip_build=false
enable_bench=false
enable_deno=false
build_profile=debug
build_args=()
server_args=()
while [[ "$#" -gt 0 ]]; do
    case "$1" in
    --sudo) use_sudo=true ;;
    --no-build) skip_build=true ;;
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

if [[ "$enable_bench" == true ]]; then
    export LIBERUM_BENCH=1
fi

if [[ "$enable_deno" == true ]]; then
    build_args+=(--features deno)
fi

if [[ "$EUID" -eq 0 ]]; then
    echo "Run as your development user; use --sudo to elevate only the server launch." >&2
    exit 1
fi

# Load the shared helper when the parent shell cannot export its function.
if ! command -v set_cargo_target >/dev/null 2>&1 &&
    [[ -r "$HOME/.config/ai/set-cargo-target.sh" ]]; then
    source "$HOME/.config/ai/set-cargo-target.sh"
fi
if command -v set_cargo_target >/dev/null 2>&1; then
    set_cargo_target
fi

if [[ "$skip_build" == false ]]; then
    # The workspace's macOS ZTS embed toolchain; never select Homebrew PHP.
    cargo build --bin "$BIN" "${build_args[@]}"
fi

HOSTNAME=$(hostname -s)

launcher=()
if [[ "$use_sudo" == true ]]; then
    launcher=(sudo)
fi

runtime_env=("RUST_LOG=${RUST_LOG:-info}" "FUNDIST_TEST_MODE=${FUNDIST_TEST_MODE:-0}" "LIBERUM_BENCH=${LIBERUM_BENCH:-0}")
if [[ -n "${MIMALLOC_PURGE_DELAY+x}" ]]; then
    # Preserve an explicit allocator setting even when sudo sanitizes the environment.
    runtime_env+=("MIMALLOC_PURGE_DELAY=$MIMALLOC_PURGE_DELAY")
fi

exec "${launcher[@]}" env "${runtime_env[@]}" \
    "${CARGO_TARGET_DIR:-$ROOT/target}/$build_profile/$BIN" \
    --config "${CONFIG_PATH:-lumenize.yaml}" \
    --environment "${HOSTNAME}" \
    --name "$BIN" \
    --fleet "$FLEET" \
    --workers "${WORKERS:-2}" "${server_args[@]}"
