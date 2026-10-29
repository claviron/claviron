#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
APP="$ROOT/examples/pentaglot"
cd "$APP"
source "$ROOT/examples/scripts/lib/application.sh"
source "$ROOT/examples/scripts/lib/build.sh"
source "$ROOT/examples/scripts/lib/mimalloc.sh"
mimalloc_init
application_init "$APP" pentaglot "${PENTAGLOT_LOG_DIR:-$APP/logs}" "$@"
set -- "${APP_ARGS[@]}"

profile=debug
native=false
skip_build=false
sync=false
optimize=false
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
      [[ $# -gt 1 ]] || { echo '--mimalloc requires default, release, aggressive or retain.' >&2; exit 2; }
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
                                  [--mimalloc default|release|aggressive|retain] [--mimalloc-purge-delay-ms N]
                                  [-- server options]

Run Rust, Lua, Deno, PHP and Gufi-ASGI applications in one Claviron Edge.
Builds use the Claviron source checkout containing this launcher.
Claviron resolves PENTAGLOT_LISTEN from its environment snapshot; .env overrides inherited values.
CONFIG_DIR defaults to config/.
FLEET defaults to penta; WORKERS defaults to 0 (one standalone serving process).
--standalone selects one process; --workers N selects a master and N serving workers.
--production explicitly selects Claviron's production environment.
PENTAGLOT_LOG_DIR selects isolated launcher logs and pid files (default: logs/).
PHP/PHP_CONFIG select the ZTS embed build; PYO3_PYTHON selects CPython 3.15.
--sync installs the locked Symfony dependencies using the selected PHP binary.
--optimize prepares an authoritative Composer classmap and Symfony production caches.
--release selects Cargo's optimized build; it does not select an allocator profile.
--mimalloc default preserves inherited allocator settings (the default policy).
--mimalloc release disables allocator THP and clears purge-delay/decommit overrides,
  allowing the linked MiMalloc version to use its own purge/decommit defaults.
--mimalloc aggressive disables allocator THP, sets purge delay to 0 and enables decommit.
--mimalloc retain disables unused-page purging for reuse.
--mimalloc-purge-delay-ms N overrides the policy: -1 disables purging, 0 purges immediately.
Allocator profiles apply at process startup; they do not invoke mi_collect, clear
live allocations or reset runtime caches.
HELP
      exit 0 ;;
    --) shift; server_args+=("$@"); break ;;
    *) server_args+=("$1") ;;
  esac
  shift
done

mimalloc_validate

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
  local_sources="$APP/.cache/local-sources.toml"
  local_helper="$APP/scripts/local.py"
  local_version="$(python3 "$local_helper" --root "$ROOT" --config "$local_sources")"
  if python3 "$local_helper" --root "$ROOT" --config "$local_sources" --verify "$APP/Cargo.toml"; then
    :
  else
    source_status=$?
    [[ "$source_status" == 1 ]] || exit "$source_status"
    cargo --config "$local_sources" update --manifest-path "$APP/Cargo.toml" \
      --package claviron --precise "$local_version"
    python3 "$local_helper" --root "$ROOT" --config "$local_sources" --verify "$APP/Cargo.toml"
  fi
  printf 'Claviron sources: %s\n' "$ROOT" >&2
  example_build "$native" cargo --config "$local_sources" build \
    --workspace --all-features --all-targets "${build_args[@]}"
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
mimalloc_apply
printf 'MiMalloc launcher profile: %s; purge_delay_ms=%s; purge_decommits=%s; allow_thp=%s\n' \
  "$mimalloc_mode" "${MIMALLOC_PURGE_DELAY-allocator-default}" \
  "${MIMALLOC_PURGE_DECOMMITS-allocator-default}" "${MIMALLOC_ALLOW_THP-allocator-default}" >&2
application_exec "${CARGO_TARGET_DIR:-$APP/target}/$profile/pentaglot" \
  "${config_args[@]}" "${fleet_args[@]}" "${worker_args[@]}" "${server_args[@]}"
