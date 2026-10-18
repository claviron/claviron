#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT/liberum/orbit-app"

case "${1:-deno}" in
  deno) command_name=serve ;;
  node) command_name=serve:node ;;
  bun) command_name=serve:bun ;;
  --help|-h)
    printf '%s\n' \
      'Usage: liberum/scripts/orbit-app.sh [deno|node|bun]' \
      'Run the JavaScript Orbit example; defaults to Deno.' \
      'Install dependencies, build the frontend and prepare .env first; see its README.'
    exit 0 ;;
  *) printf 'Unknown runtime: %s\n' "$1" >&2; exit 2 ;;
esac

exec npm run "$command_name"
