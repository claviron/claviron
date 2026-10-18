#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$REPO_ROOT/examples/fastapi-app"
SCRIPT_NAME="$(basename -- "${BASH_SOURCE[0]}")"
BIN="${BIN:-${SCRIPT_NAME%.sh}}"
FLEET="${FLEET:-fastapi}"
cd "$ROOT"

usage() {
  cat <<HELP
Usage: scripts/$SCRIPT_NAME [--sync] [--bench] [--release] [--no-build] [--venv PATH] [server options...]

Build examples/fastapi-app and run it from its application root.
The example edge-server routes Host: orbit.bench to this application's
fastapi fleet over Orbit shared memory; no TCP/UDS listener is needed.
--release builds and runs the release binary instead.
--bench enables the shared FastAPI workload routes at application boot.
--sync installs the locked Python dependencies into the selected environment first.
--no-build skips the Rust build. Use -- to forward remaining server options.
The default environment is .venv-freebsd on FreeBSD, .venv-linux on Linux,
and .venv on macOS. --venv selects application dependencies at runtime.
The build interpreter comes from the shared Cargo PYO3_PYTHON setting.
UV_PYTHON selects the base interpreter when creating an environment with --sync.

Environment:
  RUST_LOG          Log filter (default: info)
  WORKERS           Worker count override; otherwise read gufi.yaml
  BIN               Binary name (default: script basename without .sh)
  FLEET             Orbit fleet name (default: fastapi)
  CONFIG_PATH       Runner configuration (default: gufi.yaml)
  UV_PYTHON         Base interpreter for --sync
  LIBERUM_BENCH     1 enables shared FastAPI benchmarks (set by --bench)
  CARGO_TARGET_DIR  Build output directory; guest set_cargo_target if available

Examples:
  scripts/$SCRIPT_NAME --sync
  scripts/$SCRIPT_NAME --release
  scripts/$SCRIPT_NAME --bench --release
  scripts/$SCRIPT_NAME --release --no-build
  WORKERS=4 scripts/$SCRIPT_NAME --no-build
  scripts/$SCRIPT_NAME --no-build --workers 4
  scripts/$SCRIPT_NAME --no-build -- --help
HELP
}

host_os="$(uname -s)"
case "$host_os" in
  FreeBSD) venv="$ROOT/.venv-freebsd" ;;
  Linux) venv="$ROOT/.venv-linux" ;;
  Darwin) venv="$ROOT/.venv" ;;
  *) printf 'Unsupported operating system: %s\n' "$host_os" >&2; exit 2 ;;
esac
skip_build=false
sync=false
enable_bench=false
build_profile=debug
build_args=()
server_args=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --sync) sync=true ;;
    --no-build) skip_build=true ;;
    --bench) enable_bench=true ;;
    --release) build_profile=release; build_args=(--release) ;;
    --venv)
      [[ "$#" -ge 2 && -n "$2" ]] || { echo '--venv needs a path' >&2; exit 2; }
      venv="$2"; shift ;;
    --venv=*) venv="${1#*=}" ;;
    --help|-h) usage; exit 0 ;;
    --) shift; server_args+=("$@"); break ;;
    *) server_args+=("$1") ;;
  esac
  shift
done
if [[ "$enable_bench" == true ]]; then
  export LIBERUM_BENCH=1
fi
[[ -n "$venv" ]] || { echo '--venv needs a path' >&2; exit 2; }
[[ "$venv" = /* ]] || venv="$ROOT/$venv"

# Keep build output off the shared mount on VM guests, not on production hosts.
shared_guest=false
case "$host_os" in
  Linux)
    if awk '$2 == "/Liberum" && $3 ~ /^(9p|virtiofs|virtfs)$/ { found = 1 } END { exit !found }' /proc/mounts; then
      shared_guest=true
    fi ;;
  FreeBSD)
    if mount -p | awk '$2 == "/Liberum" && $3 ~ /^(p9fs|9p|virtfs)$/ { found = 1 } END { exit !found }'; then
      shared_guest=true
    fi ;;
esac
if [[ "$shared_guest" == true ]]; then
  if ! command -v set_cargo_target >/dev/null 2>&1 && \
      [[ -r "$HOME/.config/ai/set-cargo-target.sh" ]]; then
    source "$HOME/.config/ai/set-cargo-target.sh"
  fi
  if command -v set_cargo_target >/dev/null 2>&1; then
    set_cargo_target
  fi
fi

if [[ "$sync" == true ]]; then
  sync_args=(--locked)
  if [[ "${LIBERUM_BENCH:-0}" == 1 ]]; then
    sync_args+=(--group bench)
  fi
  UV_PROJECT_ENVIRONMENT="$venv" uv sync "${sync_args[@]}"
fi
if [[ ! -x "$venv/bin/python" ]]; then
  printf 'Python environment missing: %s\nRun scripts/%s --sync first.\n' "$venv" "$SCRIPT_NAME" >&2
  exit 1
fi
if [[ "${LIBERUM_BENCH:-0}" == 1 ]] && \
    ! "$venv/bin/python" -c 'import liberum_bench'; then
  printf 'Shared bench package missing. Run scripts/%s --sync --bench first.\n' "$SCRIPT_NAME" >&2
  exit 1
fi
# Application dependencies do not select the libpython linked by Cargo.
# Keep the shared build interpreter stable across terminal and IDE builds.
"$venv/bin/python" -c 'import sys; assert sys.version_info[:2] == (3, 15), "FastAPI example requires CPython 3.15; recreate the environment with --sync"'
if [[ "$skip_build" == false ]]; then
  cargo build --bin "$BIN" "${build_args[@]}"
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
    --workers|--workers=*) worker_args=(); break ;;
  esac
done

fleet_args=(--fleet "$FLEET")
for arg in "${server_args[@]}"; do
  case "$arg" in
    --fleet|--fleet=*|--fleet-name|--fleet-name=*) fleet_args=(); break ;;
  esac
done

exec env RUST_LOG="${RUST_LOG:-info}" "$binary" \
  --config-path "${CONFIG_PATH:-$ROOT/gufi.yaml}" --root "$ROOT" --venv "$venv" \
  "${worker_args[@]}" "${fleet_args[@]}" "${server_args[@]}"
