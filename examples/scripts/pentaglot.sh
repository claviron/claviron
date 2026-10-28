#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
APP="$ROOT/examples/pentaglot"
cd "$APP"
source "$ROOT/examples/scripts/lib/application.sh"
source "$ROOT/examples/scripts/lib/build.sh"
application_init "$APP" pentaglot "$APP/logs" "$@"
set -- "${APP_ARGS[@]}"

profile=debug
native=false
skip_build=false
sync=false
optimize=false
mimalloc_mode=default
mimalloc_delay_set=false
mimalloc_delay=''
workers="${WORKERS:-0}"
worker_mode=''
build_args=()
server_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) profile=release; build_args+=(--release) ;;
    --native) native=true ;;
    --no-build) skip_build=true ;;
    --sync) sync=true ;;
    --optimize) optimize=true ;;
    --mimalloc)
      [[ $# -gt 1 ]] || { echo '--mimalloc requires default, release or retain.' >&2; exit 2; }
      shift; mimalloc_mode="$1" ;;
    --mimalloc=*) mimalloc_mode="${1#*=}" ;;
    --mimalloc-purge-delay-ms)
      [[ $# -gt 1 ]] || { echo '--mimalloc-purge-delay-ms requires -1 or a non-negative integer.' >&2; exit 2; }
      shift; mimalloc_delay="$1"; mimalloc_delay_set=true ;;
    --mimalloc-purge-delay-ms=*) mimalloc_delay="${1#*=}"; mimalloc_delay_set=true ;;
    --standalone)
      [[ "$worker_mode" != workers ]] || { echo 'Choose --standalone or --workers.' >&2; exit 2; }
      worker_mode=standalone; workers=0 ;;
    --workers|--workers=*)
      [[ "$worker_mode" != standalone ]] || { echo 'Choose --standalone or --workers.' >&2; exit 2; }
      worker_mode=workers
      if [[ "$1" == --workers ]]; then
        [[ $# -gt 1 ]] || { echo '--workers requires a count.' >&2; exit 2; }
        shift; workers="$1"
      else
        workers="${1#*=}"
      fi
      [[ "$workers" =~ ^[1-9][0-9]*$ && "$workers" -le 255 ]] || {
        echo '--workers requires a count from 1 to 255; use --standalone for one process.' >&2
        exit 2
      } ;;
    --help|-h)
      cat <<'HELP'
Usage: examples/scripts/pentaglot.sh [start|run|status|stop] [--standalone|--workers N] [--sync] [--optimize] [--release] [--native] [--no-build]
                                  [--mimalloc default|release|retain] [--mimalloc-purge-delay-ms N] [-- server options]

Run Rust, Lua, Deno, PHP and Gufi-ASGI applications in one Claviron Edge.
Claviron resolves PENTAGLOT_LISTEN from its environment snapshot; .env overrides inherited values.
CONFIG_DIR defaults to config/.
FLEET defaults to penta; WORKERS defaults to 0 (one standalone serving process).
--standalone selects one process; --workers N selects a master and N serving workers.
--production explicitly selects Claviron's production environment.
PHP/PHP_CONFIG select the ZTS embed build; PYO3_PYTHON selects CPython 3.15.
--sync installs the locked Symfony dependencies using the selected PHP binary.
--optimize prepares an authoritative Composer classmap and Symfony production caches.
--mimalloc default preserves inherited allocator settings (the default policy).
--mimalloc release purges unused pages immediately with decommit enabled.
--mimalloc retain disables unused-page purging for reuse.
--mimalloc-purge-delay-ms N overrides the policy: -1 disables purging, 0 purges immediately.
Policies apply at process startup; they do not release live allocations or runtime caches.
HELP
      exit 0 ;;
    --) shift; server_args+=("$@"); break ;;
    *) server_args+=("$1") ;;
  esac
  shift
done

case "$mimalloc_mode" in
  default|release|retain) ;;
  *) echo '--mimalloc must be default, release or retain.' >&2; exit 2 ;;
esac
if [[ "$mimalloc_delay_set" == true && ! "$mimalloc_delay" =~ ^(-1|[0-9]+)$ ]]; then
  echo '--mimalloc-purge-delay-ms requires -1 or a non-negative integer.' >&2
  exit 2
fi

application_guard
if [[ "$sync" == true ]]; then
  [[ -n "${PHP:-}" && -x "$PHP" ]] || {
    echo 'Set PHP to the selected ZTS PHP executable before using --sync.' >&2
    exit 1
  }
  "$PHP" "$(command -v composer)" install --working-dir="$APP/apps/php" \
    --no-dev --no-interaction --no-progress --no-plugins --no-scripts
fi
if [[ ! -f "$APP/apps/php/vendor/autoload.php" ]]; then
  echo 'PHP dependencies missing: use --sync with PHP set to the selected ZTS executable.' >&2
  exit 1
fi
if [[ "$optimize" == true ]]; then
  [[ -n "${PHP:-}" && -x "$PHP" ]] || {
    echo 'Set PHP to the selected ZTS PHP executable before using --optimize.' >&2
    exit 1
  }
  PHP_INI_SCAN_DIR="$APP/config/php.d" "$PHP" "$(command -v composer)" dump-autoload \
    --working-dir="$APP/apps/php" --no-dev --classmap-authoritative \
    --no-interaction --no-plugins --no-scripts
  PHP_INI_SCAN_DIR="$APP/config/php.d" "$PHP" "$APP/apps/php/optimize.php"
fi
if [[ "$skip_build" == false ]]; then
  example_build "$native" cargo build --workspace --all-features --all-targets "${build_args[@]}"
fi

export RUST_LOG="${RUST_LOG:-info}"
config_args=(--config "${CONFIG_DIR:-$APP/config}")
fleet_args=(--fleet "${FLEET:-penta}")
worker_args=(--workers "$workers")
for argument in "${server_args[@]}"; do
  case "$argument" in --config|--config=*) config_args=() ;; esac
  case "$argument" in --fleet|--fleet=*) fleet_args=() ;; esac
  case "$argument" in --workers|--workers=*) worker_args=() ;; esac
done
case "$mimalloc_mode" in
  release) export MIMALLOC_PURGE_DELAY=0 MIMALLOC_PURGE_DECOMMITS=1 ;;
  retain) export MIMALLOC_PURGE_DELAY=-1 ;;
esac
if [[ "$mimalloc_delay_set" == true ]]; then
  export MIMALLOC_PURGE_DELAY="$mimalloc_delay"
fi
printf 'Mimalloc: policy=%s, purge_delay_ms=%s, purge_decommits=%s\n' \
  "$mimalloc_mode" "${MIMALLOC_PURGE_DELAY-allocator-default}" \
  "${MIMALLOC_PURGE_DECOMMITS-allocator-default}" >&2
application_exec "${CARGO_TARGET_DIR:-$APP/target}/$profile/pentaglot" \
  "${config_args[@]}" "${fleet_args[@]}" "${worker_args[@]}" "${server_args[@]}"
