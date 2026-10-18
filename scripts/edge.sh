#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLE_ROOT="$ROOT/examples/edge-server"
cd "$EXAMPLE_ROOT"

usage() {
  cat <<'HELP'
Usage: scripts/edge.sh [--bench] [--release] [--native] [--sudo] [--no-build] [server options...]

Build the edge-server debug binary, then run it in the foreground as the current user.
--release builds and runs the release binary instead.
--native builds with target-cpu=native for this machine's CPU.
--bench enables the optional native BenchService during the build.
--sudo elevates only the server launch; the build still runs as your user.
--no-build skips the build. Use -- to pass remaining options unchanged.
Ctrl-C stops the server through its normal shutdown handling.

Environment:
  RUST_LOG    Log filter (default: info)
  WORKERS     Worker count (default: 2)
  CONFIG_DIR  Runtime config directory (default: runtime)
  FUNDIST_TEST_MODE  1 enables vendor tests in debug-assertions builds only (default: 0)
  PHP         ZTS embed PHP binary used during the build
  PHP_CONFIG  Matching php-config binary used during the build
  CARGO_TARGET_DIR  Build output directory; set_cargo_target is used if available

Examples:
  scripts/edge.sh
  scripts/edge.sh --sudo
  scripts/edge.sh --release
  scripts/edge.sh --release --native
  scripts/edge.sh --bench --release
  scripts/edge.sh --release --no-build
  scripts/edge.sh --no-build --sudo
  RUST_LOG=edge=debug,claviron=info scripts/edge.sh
  WORKERS=0 scripts/edge.sh --no-build
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
    --release) build_profile=release; build_args=(--release) ;;
    --help|-h) usage; exit 0 ;;
    --) shift; server_args+=("$@"); break ;;
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

# Load the shared helper when the parent shell cannot export its function.
if ! command -v set_cargo_target >/dev/null 2>&1 && \
    [[ -r "$HOME/.config/ai/set-cargo-target.sh" ]]; then
  source "$HOME/.config/ai/set-cargo-target.sh"
fi
if command -v set_cargo_target >/dev/null 2>&1; then
  set_cargo_target
fi

if [[ "$skip_build" == false ]]; then
  # The workspace's macOS ZTS embed toolchain; never select Homebrew PHP.
  if [[ "$(uname -s)" == Darwin ]]; then
    export PHP="${PHP:-$HOME/dev/php-zts-embed/bin/php}"
    export PHP_CONFIG="${PHP_CONFIG:-$HOME/dev/php-zts-embed/bin/php-config}"
  fi
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

exec "${launcher[@]}" env "${runtime_env[@]}" \
  "${CARGO_TARGET_DIR:-$EXAMPLE_ROOT/target}/$build_profile/edge-server" \
  --config "${CONFIG_DIR:-$EXAMPLE_ROOT/config}" \
  --environment "${HOSTNAME}" \
  --name edge-server \
  --fleet edge \
  --workers "${WORKERS:-2}" "${server_args[@]}"
