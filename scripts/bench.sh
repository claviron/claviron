#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"


if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  cat <<'HELP'
Usage: scripts/bench.sh [edge.sh options...]

Launch the release benchmark server through edge.sh.
Use --no-build for an existing release binary built with the bench feature.

Environment:
  OPEN_FILES           Soft file-descriptor limit; otherwise use the finite
                       hard limit, or macOS kern.maxfilesperproc.
  MIMALLOC_PURGE_DELAY  Unused-page purge delay in milliseconds (default: 0).

Other environment variables and options are handled by edge.sh.
This starts the server; it does not start a load generator.
HELP
  exit 0
fi

open_files="${OPEN_FILES:-}"
if [[ -z "$open_files" ]]; then
  open_files="$(ulimit -H -n)"
  if [[ "$open_files" == unlimited ]]; then
    if [[ "$(uname -s)" == Darwin ]]; then
      open_files="$(sysctl -n kern.maxfilesperproc)"
    else
      open_files="$(ulimit -S -n)"
    fi
  fi
fi

if [[ "$open_files" != unlimited && ! "$open_files" =~ ^[1-9][0-9]*$ ]]; then
  echo "OPEN_FILES must be a positive integer or unlimited." >&2
  exit 1
fi
if ! ulimit -S -n "$open_files"; then
  echo "Cannot set open-file limit to $open_files; check the inherited hard limit." >&2
  exit 1
fi

# Only benchmark launches opt into immediate purging. Ordinary runs are unchanged.
export MIMALLOC_PURGE_DELAY="${MIMALLOC_PURGE_DELAY:-0}"
printf 'Bench: open files=%s, mimalloc purge delay=%s ms\n' \
  "$(ulimit -S -n)" "$MIMALLOC_PURGE_DELAY"

exec "$ROOT/scripts/edge.sh" --bench --release "$@"
