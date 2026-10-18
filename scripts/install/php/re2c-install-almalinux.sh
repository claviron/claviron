#!/usr/bin/env bash
set -euo pipefail

# Build a modern re2c for AlmaLinux/RHEL hosts whose distro package is too
# old for PHP lexer generation. This is a developer build tool, not a
# claviron runtime dependency, so it installs under the developer user's
# local prefix by default.

RE2C_VERSION="${RE2C_VERSION:-3.1}"
RE2C_PREFIX="${RE2C_PREFIX:-$HOME/.local}"
RE2C_SRC_ROOT="${RE2C_SRC_ROOT:-$HOME/src}"

usage() {
  cat <<EOF
Usage: $(basename "$0")

Build and install re2c ${RE2C_VERSION} under ${RE2C_PREFIX}.

Environment:
  RE2C_VERSION   re2c release version   (default: 3.1)
  RE2C_PREFIX    install prefix         (default: \$HOME/.local)
  RE2C_SRC_ROOT  source/cache root      (default: \$HOME/src)

After install:
  export PATH="${RE2C_PREFIX}/bin:\$PATH"
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  "")
    ;;
  *)
    echo "ERROR: unknown option: $1" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "ERROR: this helper is for AlmaLinux/RHEL-style Linux hosts." >&2
  exit 2
fi

if [[ -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  case " ${ID:-} ${ID_LIKE:-} " in
    *" rhel "*|*" fedora "*|*" centos "*|*" almalinux "*|*" rocky "*) ;;
    *)
      echo "ERROR: unsupported Linux family: ${ID:-unknown} ${ID_LIKE:-}" >&2
      exit 2
      ;;
  esac
fi

sudo dnf install -y curl xz gcc gcc-c++ make autoconf automake libtool

mkdir -p "$RE2C_SRC_ROOT" "$RE2C_PREFIX"
cd "$RE2C_SRC_ROOT"

archive="re2c-${RE2C_VERSION}.tar.xz"
src_dir="re2c-${RE2C_VERSION}"
url="https://github.com/skvadrik/re2c/releases/download/${RE2C_VERSION}/${archive}"

if [[ ! -f "$archive" ]]; then
  curl -fL -o "$archive" "$url"
fi

rm -rf "$src_dir"
tar -xf "$archive"
cd "$src_dir"

./configure --prefix="$RE2C_PREFIX"
make -j"$(nproc)"
make install

"$RE2C_PREFIX/bin/re2c" --version
echo
echo "Add this to your shell before building PHP:"
echo "  export PATH=\"${RE2C_PREFIX}/bin:\$PATH\""
