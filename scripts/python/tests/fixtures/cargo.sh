#!/usr/bin/env bash
# Record the script contract without invoking a compiler or changing the tree.
set -eo pipefail
{
    printf '%s\0' "$PWD" "$SQLX_OFFLINE" "$PATH" "${RUSTFLAGS:-}" "${CARGO_TARGET_DIR:-}" "$@"
    printf '\n'
} >> "$CHECK_LOG"
if [[ "$1" == "${FAIL_COMMAND:-}" ]]; then
    exit 7
fi
