#!/usr/bin/env bash
# Test through scripts/check.sh, which owns the Cargo selection.
# Kept for editor tasks and run configurations; see scripts/check.sh --help.
exec "$(dirname -- "${BASH_SOURCE[0]}")/check.sh" --test-only "$@"
