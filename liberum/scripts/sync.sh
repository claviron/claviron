#!/usr/bin/env bash
# Put this site on the server, where portico serves it from /sites/claviron.
#
#   ./scripts/sync.sh             check, copy, restart the Lua application if lua/ changed
#   ./scripts/sync.sh --dry-run   show what would change; copy and restart nothing
#
# Checks first: every Lua file must compile (luac -p, Lua 5.5 like the
# server's). Only the folders in FOLDERS are copied, as the `claviron` user
# and readable by its group, which portico is in; everything else on the
# server (storage/ above all) is its own. Nothing is deleted there: a file removed
# here stays until removed by hand.
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE"

TARGET="claviron@de02:/sites/claviron/"
# What goes to the server; add a folder here when the site grows one.
FOLDERS=(public lua form)
DRY=false
case "${1:-}" in
  --dry-run) DRY=true ;;
  "") ;;
  -h|--help) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  *) echo "unknown option: $1" >&2; exit 1 ;;
esac

find lua -name '*.lua' -print0 | xargs -0 -n1 luac -p

flags=(-ai --chmod=D750,F640 --exclude .DS_Store)
$DRY && flags+=(--dry-run)

changes="$(rsync "${flags[@]}" "${FOLDERS[@]}" "$TARGET")"
[ -n "$changes" ] && echo "$changes" || echo "  nothing changed"

# A Lua application is loaded once per worker; a changed module is read again
# only when the application is restarted.
if grep -qE '^[<>ch.][^ ]* lua/' <<<"$changes"; then
  if $DRY; then
    echo "  lua/ would change: would restart the lua application demo"
  else
    claviron-client restart-application lua demo
  fi
fi
