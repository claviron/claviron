#!/usr/bin/env bash
# Format and lint through scripts/check.sh, which owns the Cargo selection.
# Kept for editor tasks and run configurations; see scripts/check.sh --help.
exec "$(dirname -- "${BASH_SOURCE[0]}")/check.sh" --no-test "$@"
