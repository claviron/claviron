#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/examples/scripts/lib/build.sh"
APP="$ROOT/examples/bench-app"
EDGE="$ROOT/examples/edge-server"
if [[ -n "${CARGO_TARGET_DIR:-}" && "${CARGO_TARGET_DIR:-}" != /* ]]; then
  export CARGO_TARGET_DIR="$PWD/$CARGO_TARGET_DIR"
fi

usage() {
  echo 'Usage: examples/scripts/bench.sh start|status|stop [--release] [--native] [--no-build]'
  echo 'Validate both configs, then start the native Rust app and benchmark Edge.'
  echo '--native builds both binaries with target-cpu=native for this machine.'
}

action="${1:-}"
case "$action" in
  start|status|stop) shift ;;
  --help|-h) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac
profile=debug
build_args=()
launch_args=(--no-build)
skip_build=false
enable_native=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) profile=release; build_args=(--release); launch_args+=(--release) ;;
    --no-build) skip_build=true ;;
    --native) enable_native=true ;;
    *) usage >&2; exit 2 ;;
  esac
  shift
done

export BENCH_UDS="$APP/logs/bench.sock"
export CONFIG_DIR="$EDGE/config-bench"
export WORKERS="${WORKERS:-0}"
export RUST_LOG="${RUST_LOG:-info}"

benchmark_edge_recorded() {
  [[ -f "$EDGE/logs/edge-server.pid" ]] &&
    [[ "$(sed -n '5p' "$EDGE/logs/edge-server.pid")" == "$CONFIG_DIR" ]]
}

case "$action" in
  status)
    result=0
    "$ROOT/examples/scripts/bench-app.sh" status || result=$?
    if benchmark_edge_recorded; then
      "$ROOT/examples/scripts/edge.sh" status || result=$?
    else
      echo 'Benchmark Edge: not tracked (another Edge instance is left alone).'
      result=3
    fi
    exit "$result" ;;
  stop)
    if benchmark_edge_recorded; then
      "$ROOT/examples/scripts/edge.sh" stop
    else
      echo 'No benchmark Edge recorded; no Edge signal sent.'
    fi
    "$ROOT/examples/scripts/bench-app.sh" stop
    exit 0 ;;
esac

if "$ROOT/examples/scripts/edge.sh" status >/dev/null 2>&1 ||
   "$ROOT/examples/scripts/bench-app.sh" status >/dev/null 2>&1; then
  echo 'An Edge or bench-app process is already tracked; inspect status before starting.' >&2
  exit 1
fi

if [[ "$skip_build" == false ]]; then
  example_build "$enable_native" cargo build --manifest-path "$APP/Cargo.toml" --workspace --all-features --all-targets "${build_args[@]}"
  example_build "$enable_native" cargo build --manifest-path "$EDGE/Cargo.toml" --workspace --all-features --all-targets "${build_args[@]}"
fi

# Validation is foreground-only and publishes no lifecycle PID file. Neither
# application is started if either configuration is invalid.
(
  cd "$APP"
  "${CARGO_TARGET_DIR:-$APP/target}/$profile/bench-app" --check-config
)
(
  cd "$EDGE"
  "${CARGO_TARGET_DIR:-$EDGE/target}/$profile/edge-server" \
    --check-config --config "$CONFIG_DIR" --name edge-server \
    --fleet edge --workers "$WORKERS" --environment "$(hostname -s)"
)
echo 'Configuration valid: bench-app and benchmark Edge.'

"$ROOT/examples/scripts/bench-app.sh" start "${launch_args[@]}"
if ! "$ROOT/examples/scripts/edge.sh" start "${launch_args[@]}"; then
  "$ROOT/examples/scripts/bench-app.sh" stop
  exit 1
fi
echo 'Benchmark pair launched. Use bench.sh status to inspect asynchronous boot.'
