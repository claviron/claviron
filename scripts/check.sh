#!/usr/bin/env bash
# Bash 3.2+; the one place this repository chooses a Cargo selection and its
# environment, so every caller reuses the same build artifacts.
set -eo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/check.sh [OPTIONS] [FILTER] [-- TEST_OPTIONS...]

Format, lint and test, in that order, with one Cargo selection.
  -p, --package NAME   Check only this crate (repeatable): format it, lint it
                       with all features and targets, test it with all features.
                       Without -p the selection is the whole workspace.
  --check              Check formatting without changing files (for CI).
  --fix                Run cargo fix on the selection before formatting.
  --no-test            Format and lint only.
  --test-only          Test only; no formatting or linting.
  FILTER               Test-name substring; does not change the selection.
  -- TEST_OPTIONS      Forward options to the Rust test harness unchanged.
  -h, --help           Show this help.

Examples:
  scripts/check.sh -p claviron-cache
  scripts/check.sh -p claviron-cache --test-only reload -- --nocapture
  scripts/check.sh --check

Clippy treats warnings as errors and never applies fixes. --fix may modify
staged and unstaged sources; it cannot be combined with --check. Tests keep
normal parallelism and do not enable ignored tests.
SQLX_OFFLINE defaults to true; set it to false for live SQLx validation.
Uses the installed Cargo, rustfmt and Clippy; installs nothing.
EOF
}

fail() { echo "error: $*" >&2; exit 2; }

packages=()
filter=()
test_args=()
check=0
fix=0
lint=1
test=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        -p | --package)
            [[ -n "${2:-}" && "$2" != -* ]] || fail "$1 requires a package name"
            packages+=(--package "$2")
            shift 2 ;;
        --check) check=1; shift ;;
        --fix) fix=1; shift ;;
        --no-test) test=0; shift ;;
        --test-only) lint=0; shift ;;
        -h | --help) usage; exit 0 ;;
        --)
            shift
            test_args=(-- "$@")
            break ;;
        -*) fail "unknown argument: $1 (see scripts/check.sh --help)" ;;
        *)
            [[ ${#filter[@]} -eq 0 ]] || fail "only one test-name filter is accepted"
            filter=("$1")
            shift ;;
    esac
done
[[ "$check" == 0 || "$fix" == 0 ]] || fail "--check and --fix cannot be combined"
[[ "$lint" == 1 || "$test" == 1 ]] || fail "--no-test and --test-only cannot be combined"
[[ "$lint" == 1 || ( "$check" == 0 && "$fix" == 0 ) ]] \
    || fail "--test-only cannot be combined with --check or --fix"
[[ "$test" == 1 || ( ${#filter[@]} -eq 0 && ${#test_args[@]} -eq 0 ) ]] \
    || fail "a test filter or test options need tests; drop --no-test"
command -v cargo >/dev/null 2>&1 || fail "Cargo is required; install Rust with rustfmt and Clippy"

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$project_dir"
export SQLX_OFFLINE="${SQLX_OFFLINE:-true}"

selection=(--workspace)
fmt_scope=(--all)
scope_name='workspace'
if [[ ${#packages[@]} -gt 0 ]]; then
    selection=("${packages[@]}")
    fmt_scope=("${packages[@]}")
    scope_name='selected packages'
fi

if [[ "$lint" == 1 ]]; then
    if [[ "$fix" == 1 ]]; then
        echo "Running cargo fix ($scope_name, all features and targets)..."
        cargo fix "${selection[@]}" --all-features --all-targets --allow-dirty --allow-staged
    fi

    fmt_args=()
    [[ "$check" == 0 ]] || fmt_args=(-- --check)
    echo "Running rustfmt ($scope_name)..."
    cargo fmt "${fmt_scope[@]}" "${fmt_args[@]}"

    echo "Running Clippy ($scope_name, all features and targets; warnings are errors)..."
    cargo clippy "${selection[@]}" --all-features --all-targets -- -D warnings
fi

if [[ "$test" == 1 ]]; then
    echo "Running tests ($scope_name, all features, normal test parallelism)..."
    cargo test "${selection[@]}" --all-features --no-fail-fast "${filter[@]}" "${test_args[@]}"
fi
echo 'Checks passed.'
