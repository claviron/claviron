#!/usr/bin/env bash
# Apply CPU-specific flags only to the requested build command.

example_build() {
  local native="$1"
  shift
  local build_env=()
  if [[ "$native" == true ]]; then
    # Cargo gives encoded flags precedence; preserve the caller's active flags.
    if [[ -n "${CARGO_ENCODED_RUSTFLAGS+x}" ]]; then
      build_env+=("CARGO_ENCODED_RUSTFLAGS=${CARGO_ENCODED_RUSTFLAGS:+${CARGO_ENCODED_RUSTFLAGS}$'\x1f'}-C"$'\x1f'"target-cpu=native")
    else
      build_env+=("RUSTFLAGS=${RUSTFLAGS:+$RUSTFLAGS }-C target-cpu=native")
    fi
  fi
  env "${build_env[@]}" "$@"
}
