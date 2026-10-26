#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
ROOT="$REPO_ROOT/examples/fastapi-app"
SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"
BIN="${BIN:-${SCRIPT_NAME%.sh}}"
FLEET="${FLEET:-fastapi}"
cd "$ROOT"
source "$REPO_ROOT/examples/scripts/lib/application.sh"
source "$REPO_ROOT/examples/scripts/lib/build.sh"
application_init "$ROOT" fastapi-app "$ROOT/storage/logs" "$@"
set -- "${APP_ARGS[@]}"

usage() {
  cat <<HELP
Usage: examples/scripts/$SCRIPT_NAME [start|run|status|stop] [--sync] [--bench] [--release] [--native] [--no-build] [--venv PATH] [server options...]

Build examples/fastapi-app and run it from its application root.
The example edge-server routes Host: orbit.bench to this application's
fastapi fleet over Orbit shared memory; no TCP/UDS listener is needed.
--release builds and runs the release binary instead.
--native builds the Rust runner with target-cpu=native for this machine.
--bench enables shared native baselines and FastAPI workload routes.
--sync installs the locked Python dependencies into the selected environment first.
--no-build skips the Rust build. Use -- to forward remaining server options.
The default environment is UV_PROJECT_ENVIRONMENT or .venv. A shared VM checkout
uses its OS-specific environment; --venv selects application dependencies explicitly.
The build interpreter comes from the shared Cargo PYO3_PYTHON setting.
UV_PYTHON selects the base interpreter when creating an environment with --sync.
start runs in the background; run (the default) stays in the foreground.
status reports the recorded process and logs; stop requests graceful shutdown.

Environment:
  RUST_LOG          Log filter (default: info)
  WORKERS           Worker count override; otherwise read gufi.yaml
  BIN               Binary name (default: script basename without .sh)
  FLEET             Orbit fleet name (default: fastapi)
  CONFIG_PATH       Runner configuration (default: gufi.yaml)
  UV_PYTHON         Base interpreter for --sync
  EXAMPLES_BENCH     1 enables shared FastAPI benchmarks (set by --bench)
  CARGO_TARGET_DIR  Build output directory (otherwise this application's target/)

Examples:
  examples/scripts/$SCRIPT_NAME --sync
  examples/scripts/$SCRIPT_NAME start --no-build
  examples/scripts/$SCRIPT_NAME status
  examples/scripts/$SCRIPT_NAME stop
  examples/scripts/$SCRIPT_NAME --release
  examples/scripts/$SCRIPT_NAME --bench --release
  examples/scripts/$SCRIPT_NAME --release --no-build
  WORKERS=4 examples/scripts/$SCRIPT_NAME --no-build
  examples/scripts/$SCRIPT_NAME --no-build --workers 4
  examples/scripts/$SCRIPT_NAME --no-build -- --help
HELP
}

venv="${UV_PROJECT_ENVIRONMENT:-$ROOT/.venv}"
if [[ -z "${UV_PROJECT_ENVIRONMENT:-}" && "$(uname -s)" == Linux ]] && command -v findmnt >/dev/null 2>&1; then
  case "$(findmnt -n -o FSTYPE -T "$ROOT")" in
    9p|virtiofs) venv="$ROOT/.venv-linux" ;;
  esac
fi

skip_build=false
enable_native=false
sync=false
enable_bench=false
build_profile=debug
build_args=()
server_args=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
  --sync) sync=true ;;
  --no-build) skip_build=true ;;
  --native) enable_native=true ;;
  --bench) enable_bench=true ;;
  --release)
    build_profile=release
    build_args=(--release)
    ;;
  --venv)
    [[ "$#" -ge 2 && -n "$2" ]] || {
      echo '--venv needs a path' >&2
      exit 2
    }
    venv="$2"
    shift
    ;;
  --venv=*) venv="${1#*=}" ;;
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
  export EXAMPLES_BENCH=1
  build_args+=(--features gufi/bench)
fi
[[ -n "$venv" ]] || {
  echo '--venv needs a path' >&2
  exit 2
}
[[ "$venv" = /* ]] || venv="$ROOT/$venv"

application_guard

if [[ "$sync" == true ]]; then
  sync_args=(--locked)
  if [[ "${EXAMPLES_BENCH:-0}" == 1 ]]; then
    sync_args+=(--group bench)
  fi
  UV_PROJECT_ENVIRONMENT="$venv" uv sync "${sync_args[@]}"
fi
if [[ ! -x "$venv/bin/python" ]]; then
  printf 'Python environment missing: %s\nRun scripts/%s --sync first.\n' "$venv" "$SCRIPT_NAME" >&2
  exit 1
fi
if [[ "${EXAMPLES_BENCH:-0}" == 1 ]] &&
  ! "$venv/bin/python" -c 'import claviron_bench'; then
  printf 'Shared bench package missing. Run scripts/%s --sync --bench first.\n' "$SCRIPT_NAME" >&2
  exit 1
fi
# Application dependencies do not select the libpython linked by Cargo.
# Keep the shared build interpreter stable across terminal and IDE builds.
"$venv/bin/python" -c 'import sys; assert sys.version_info[:2] == (3, 15), "FastAPI example requires CPython 3.15; recreate the environment with --sync"'
if [[ "$skip_build" == false ]]; then
  example_build "$enable_native" cargo build --bin "$BIN" "${build_args[@]}"
fi

binary="${CARGO_TARGET_DIR:-$ROOT/target}/$build_profile/$BIN"
if [[ ! -x "$binary" ]]; then
  printf 'Executable not found: %s\nRun scripts/%s to build it first.\n' "$binary" "$SCRIPT_NAME" >&2
  exit 1
fi
# Keep the file's worker count unless the operator overrides it. Explicit
# runner arguments take precedence over the environment, as in Octane-app.
worker_args=()
if [[ -n "${WORKERS:-}" ]]; then
  worker_args=(--workers "$WORKERS")
fi
for arg in "${server_args[@]}"; do
  case "$arg" in
  --workers | --workers=*)
    worker_args=()
    break
    ;;
  esac
done

fleet_args=(--fleet "$FLEET")
for arg in "${server_args[@]}"; do
  case "$arg" in
  --fleet | --fleet=* | --fleet-name | --fleet-name=*)
    fleet_args=()
    break
    ;;
  esac
done

export RUST_LOG="${RUST_LOG:-info}"
application_exec "$binary" \
  --config-path "${CONFIG_PATH:-$ROOT/gufi.yaml}" --root "$ROOT" --venv "$venv" \
  "${worker_args[@]}" "${fleet_args[@]}" "${server_args[@]}"
