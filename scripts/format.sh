#!/usr/bin/env bash
# Bash 3.2+; use the caller's Cargo environment without rewriting it.
set -eo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/format.sh [OPTIONS]

Format Rust sources, then lint the workspace with all features and targets.
  --check              Check formatting without changing files (for CI).
  --fix                Run cargo fix before formatting and linting.
  -p, --package NAME   Format only this package (repeatable); linting stays workspace-wide.
  -h, --help           Show this help.

With no options, formatting may change files; Clippy never applies fixes.
--check and --fix cannot be combined. --fix may modify staged/unstaged sources.
SQLX_OFFLINE defaults to true; set it to false for live SQLx validation.
Uses the installed Cargo, rustfmt and Clippy; installs nothing and runs no tests.
EOF
}

fail() { echo "error: $*" >&2; exit 2; }

packages=()
check=0
fix=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) check=1; shift ;;
        --fix) fix=1; shift ;;
        -p | --package)
            [[ -n "${2:-}" && "$2" != -* ]] || fail "$1 requires a package name"
            packages+=(--package "$2")
            shift 2 ;;
        -h | --help) usage; exit 0 ;;
        *) fail "unknown argument: $1 (see scripts/format.sh --help)" ;;
    esac
done
[[ "$check" == 0 || "$fix" == 0 ]] || fail "--check and --fix cannot be combined"
command -v cargo >/dev/null 2>&1 || fail "Cargo is required; install Rust with rustfmt and Clippy"

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$project_dir"
export SQLX_OFFLINE="${SQLX_OFFLINE:-true}"

if [[ "$fix" == 1 ]]; then
    echo 'Running cargo fix (workspace, all features and targets)...'
    cargo fix --workspace --all-features --all-targets --allow-dirty --allow-staged
fi

scope=(--all)
[[ ${#packages[@]} -eq 0 ]] || scope=("${packages[@]}")
fmt_args=()
[[ "$check" == 0 ]] || fmt_args=(-- --check)
echo 'Running rustfmt...'
cargo fmt "${scope[@]}" "${fmt_args[@]}"

echo 'Running Clippy (workspace, all features and targets; warnings are errors)...'
cargo clippy --workspace --all-features --all-targets -- -D warnings
echo 'Formatting and linting passed.'
