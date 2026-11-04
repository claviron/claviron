//! Heap allocation counts for the serving process.
//!
//! Counting runs only between [`begin`] and [`end`], around the warm-up load.
//! CPU windows leave it disabled: an allocation then costs one relaxed load of
//! a flag nothing writes, so a target that allocates more is not charged for
//! being counted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

/// The system allocator, with allocation calls counted while enabled.
/// `alloc`, `alloc_zeroed` and `realloc` each count once; frees do not.
pub struct Counting;

fn record(size: usize) {
    if ENABLED.load(Ordering::Relaxed) {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}

// SAFETY: every call is forwarded unchanged to the system allocator; the
// counters never touch the allocation or its layout.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc::alloc` for this layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(
        &self,
        layout: Layout,
    ) -> *mut u8 {
        record(layout.size());
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
    ) {
        // SAFETY: the pointer came from this allocator with this layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: Layout,
        size: usize,
    ) -> *mut u8 {
        record(size);
        // SAFETY: the caller upholds `GlobalAlloc::realloc`.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

/// Allocation calls and requested bytes inside one counted window.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct Allocations {
    pub(super) calls: u64,
    pub(super) bytes: u64,
}

/// Start counting from zero. `false` when a window is already open.
pub(super) fn begin() -> bool {
    if ENABLED.load(Ordering::Acquire) {
        return false;
    }
    CALLS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Release);
    true
}

/// Stop counting and read the window. `None` when no window is open.
pub(super) fn end() -> Option<Allocations> {
    if !ENABLED.swap(false, Ordering::AcqRel) {
        return None;
    }
    Some(Allocations {
        calls: CALLS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
    })
}
