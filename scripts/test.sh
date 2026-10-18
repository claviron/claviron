#!/usr/bin/env bash
# Bash 3.2+; preserve Cargo's normal configuration and test parallelism.
set -eo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/test.sh [FILTER] [-- TEST_OPTIONS...]

Run cargo test --workspace --all-features --no-fail-fast.
  FILTER            Optional test-name substring; does not change Cargo features.
  -- TEST_OPTIONS   Forward options to the Rust test harness unchanged.
  -h, --help        Show this help.

Examples:
  scripts/test.sh
  scripts/test.sh host_binding
  scripts/test.sh host_binding -- --nocapture

Tests use normal parallelism; the script does not serialize them or run ignored
tests automatically. Use isolated fixtures, not a running deployment.
SQLX_OFFLINE defaults to true; set it to false for live SQLx validation.
Uses the installed Cargo; installs nothing and does not format or lint sources.
EOF
}

fail() { echo "error: $*" >&2; exit 2; }

filter=()
test_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h | --help) usage; exit 0 ;;
        --)
            shift
            test_args=(-- "$@")
            break ;;
        -*) fail "unknown argument: $1 (see scripts/test.sh --help)" ;;
        *)
            [[ ${#filter[@]} -eq 0 ]] || fail "only one test-name filter is accepted"
            filter=("$1")
            shift ;;
    esac
done
command -v cargo >/dev/null 2>&1 || fail "Cargo is required; install Rust first"

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$project_dir"
export SQLX_OFFLINE="${SQLX_OFFLINE:-true}"

echo 'Running workspace tests (all features, normal test parallelism)...'
cargo test --workspace --all-features --no-fail-fast "${filter[@]}" "${test_args[@]}"
echo 'Tests passed.'
