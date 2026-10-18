//! RAII guards for `EMBED_METRICS`. Bumps the per-request counter at
//! enter, decrements on drop. Panics, early returns, cancellation —
//! all paths covered through `Drop`.

use super::PHP_EMBED_METRICS;

/// Wraps a single PHP embed dispatch. Constructed at the top of the
/// blocking task that runs `php_request_startup` → `Embed::run_script`
/// → `php_request_shutdown`.
pub struct EmbedRequestGuard;

impl EmbedRequestGuard {
    pub fn enter() -> Self {
        PHP_EMBED_METRICS.request_start();
        EmbedRequestGuard
    }
}

impl Drop for EmbedRequestGuard {
    fn drop(&mut self) {
        PHP_EMBED_METRICS.request_end();
    }
}

/// Hand-bumpable counters for events that don't fit a guard shape.
#[inline]
pub fn record_bailout() {
    PHP_EMBED_METRICS.bailout();
}

#[inline]
pub fn record_request_bytes(n: u64) {
    PHP_EMBED_METRICS.add_request_bytes(n);
}

#[inline]
pub fn record_response_bytes(n: u64) {
    PHP_EMBED_METRICS.add_response_bytes(n);
}

/// Bumps `threads_spawned_total` (counter) + `threads_active` (gauge).
/// Called from `SapiThreadPool::spawn` per worker fork.
#[inline]
pub fn record_thread_spawned() {
    PHP_EMBED_METRICS.thread_spawned();
}

/// Decrements `threads_active`. Called when a worker thread leaves
/// its `blocking_recv` loop (channel close at drain, panic).
#[inline]
pub fn record_thread_exited() {
    PHP_EMBED_METRICS.thread_exited();
}

/// Engine-level failure (gateway 5xx that we generated, not PHP's).
#[inline]
pub fn record_engine_error() {
    PHP_EMBED_METRICS.engine_error();
}
