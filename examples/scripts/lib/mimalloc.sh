#!/usr/bin/env bash
# Shared startup policies for launchers whose Rust binary uses MiMalloc.

mimalloc_init() {
  mimalloc_mode=default
  mimalloc_delay_set=false
  mimalloc_delay=''
}

mimalloc_validate() {
  case "$mimalloc_mode" in
    default|release|aggressive|retain) ;;
    *) echo '--mimalloc must be default, release, aggressive or retain.' >&2; exit 2 ;;
  esac
  if [[ "$mimalloc_delay_set" == true && ! "$mimalloc_delay" =~ ^(-1|[0-9]+)$ ]]; then
    echo '--mimalloc-purge-delay-ms requires -1 or a non-negative integer.' >&2
    exit 2
  fi
}

mimalloc_apply() {
  case "$mimalloc_mode" in
    release)
      export MIMALLOC_ALLOW_THP=0
      unset MIMALLOC_PURGE_DELAY MIMALLOC_PURGE_DECOMMITS \
        MIMALLOC_RESET_DELAY MIMALLOC_RESET_DECOMMITS ;;
    aggressive)
      export MIMALLOC_ALLOW_THP=0 MIMALLOC_PURGE_DELAY=0 MIMALLOC_PURGE_DECOMMITS=1 ;;
    retain) export MIMALLOC_PURGE_DELAY=-1 ;;
  esac
  if [[ "$mimalloc_delay_set" == true ]]; then
    export MIMALLOC_PURGE_DELAY="$mimalloc_delay"
  fi
}

mimalloc_help() {
  cat <<'HELP'
Allocator options: --mimalloc default|release|aggressive|retain [--mimalloc-purge-delay-ms N]
--release selects Cargo's optimized build; it does not select an allocator profile.
--mimalloc default preserves inherited allocator settings (the default policy).
--mimalloc release disables allocator THP and clears purge-delay/decommit overrides,
  allowing the linked MiMalloc version to use its own purge/decommit defaults.
--mimalloc aggressive disables allocator THP, sets purge delay to 0 and enables decommit.
--mimalloc retain disables unused-page purging for reuse.
--mimalloc-purge-delay-ms N overrides the policy: -1 disables purging, 0 purges immediately.
Allocator profiles apply at process startup; they do not invoke mi_collect, clear
live allocations or reset runtime caches.
HELP
}
