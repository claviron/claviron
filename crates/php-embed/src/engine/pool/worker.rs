//! A request-pool slot and its per-job execution loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use tokio::sync::{Notify, mpsc};
use tracing::{debug, error};

use super::{PHP_JOB_CHANNEL_CAPACITY, monotonic_now_ms, update_max};
use crate::engine::thread::{PhpThreadContext, bind_php_thread};
use crate::engine::{PhpJob, ensure_sapi, run_php_on_thread};

pub(super) struct SapiThread {
    pub(super) id: u8,
    pub(super) tx: mpsc::Sender<PhpJob>,
    /// True from successful dispatch reservation until the worker
    /// finishes that job. Dispatch never queues behind a busy PHP
    /// thread; long-lived requests (SSE, uploads, slow DB work) must
    /// not create head-of-line blocking for unrelated requests.
    pub(super) busy: Arc<AtomicBool>,
    /// Lifetime count of requests this thread has served. Read by
    /// the rotation manager (when [`crate::RequestPool::max_requests`]
    /// enables rotation) to decide whether to recycle this thread. Always
    /// incremented by the worker loop so metrics stay accurate even
    /// when rotation is off — the cost is one Relaxed atomic per
    /// completed request, ~1ns.
    pub(super) request_count: Arc<AtomicU64>,
    /// Monotonic-ms timestamp of the most recent
    /// `InFlightGuard::Drop` (= when this thread last finished a
    /// request). Read by the idle culler to decide whether the
    /// thread can be trimmed. Initialised at spawn time so threads
    /// that never serve a request still age normally and become
    /// eligible after `thread_idle_timeout`.
    pub(super) last_used_ms: Arc<AtomicU64>,
    /// Monotonic-ms timestamp captured when this OS/PHP thread was
    /// spawned. Used by `thread_max_life` to renew old idle floor slots
    /// even when the pool has already returned to `threads_min`.
    pub(super) spawned_at_ms: u64,
    /// Owned OS-thread handle. Retirement and drain retain it until PHP/TSRM
    /// teardown finishes, then join it before releasing the slot's resources.
    pub(super) joiner: std::thread::JoinHandle<()>,
    /// Set as the thread's last act, after PHP/TSRM teardown, just before
    /// it wakes the drain. `JoinHandle::is_finished` turns true only once
    /// the closure has returned, a moment after any signal the thread can
    /// send, so the drain reads this instead: set means the join is a return
    /// already under way.
    pub(super) exited: Arc<AtomicBool>,
}

/// RAII counter for the pool's `in_flight` gauge. Constructed at the
/// top of each worker iteration after a job has been popped off the
/// receiver; the matching `Drop` decrements unconditionally. Using a
/// guard rather than manual `+=`/`-=` ensures a panic in PHP execution
/// can't leak the counter — `drain` would block forever otherwise.
struct InFlightGuard {
    in_flight: Arc<AtomicUsize>,
    busy: Arc<AtomicBool>,
    busy_count: Arc<AtomicUsize>,
    /// Per-thread "last used" timestamp. Updated to
    /// `monotonic_now_ms()` in `Drop` so the elastic culler can
    /// see how long this thread has been idle.
    last_used: Arc<AtomicU64>,
    /// Slot-freed notify clone, pinged in `Drop` after the busy
    /// flag is released so a parked dispatch waiter can wake and
    /// retry. Order matters — busy.store(false) BEFORE notify_one
    /// so the woken waiter's CAS observes `false` instead of
    /// spinning.
    slot_freed: Arc<Notify>,
    /// Woken with every request that ends, so a drain waiting for the last
    /// one needs no timer.
    drain_wake: Arc<Notify>,
}

impl InFlightGuard {
    fn enter(
        counter: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
        busy: Arc<AtomicBool>,
        busy_count: Arc<AtomicUsize>,
        last_used: Arc<AtomicU64>,
        slot_freed: Arc<Notify>,
        drain_wake: Arc<Notify>,
    ) -> Self {
        let current = counter.fetch_add(1, Ordering::AcqRel) + 1;
        update_max(&max_in_flight, current);
        Self {
            in_flight: counter,
            busy,
            busy_count,
            last_used,
            slot_freed,
            drain_wake,
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.busy_count.fetch_sub(1, Ordering::AcqRel);
        self.busy.store(false, Ordering::Release);
        // Stamp last activity AFTER busy release so the culler's
        // candidate scan, which checks busy first, sees a
        // consistent "idle thread, freshly stamped" pair.
        self.last_used.store(monotonic_now_ms(), Ordering::Release);
        self.slot_freed.notify_one();
        self.drain_wake.notify_waiters();
    }
}

/// Spawn a single SAPI worker thread — used by both the pool's
/// initial fill and the rotation manager's slot replacements.
/// `slot` is what becomes `SapiThread.id` and is exposed to PHP via
/// `$_SERVER['CLAVIRON_THREAD_ID']`; preserving it across rotations
/// keeps that identifier stable per slot (only the underlying OS
/// thread / TSRM slot changes). `slot_freed` is shared across all
/// threads in the pool — every `InFlightGuard::Drop` pings it so
/// queued dispatchers wake on any thread releasing.
pub(super) fn spawn_sapi_thread(
    slot: u8,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
    busy_count: Arc<AtomicUsize>,
    slot_freed: Arc<Notify>,
    drain_wake: Arc<Notify>,
    owner: crate::engine::interrupt::ExecutionOwner,
) -> SapiThread {
    let (tx, rx) = mpsc::channel::<PhpJob>(PHP_JOB_CHANNEL_CAPACITY);
    let busy = Arc::new(AtomicBool::new(false));
    let busy_for_thread = Arc::clone(&busy);
    let request_count = Arc::new(AtomicU64::new(0));
    let request_count_for_thread = Arc::clone(&request_count);
    // Threads that never serve a request still age — initialise
    // last_used at spawn time so the idle culler eventually
    // reaps them after `thread_idle_timeout`.
    let spawned_at_ms = monotonic_now_ms();
    let last_used_ms = Arc::new(AtomicU64::new(spawned_at_ms));
    let last_used_for_thread = Arc::clone(&last_used_ms);
    let exited = Arc::new(AtomicBool::new(false));
    let exited_for_thread = Arc::clone(&exited);
    let joiner = std::thread::Builder::new()
        .name(format!("claviron-php-{slot}"))
        .spawn(move || {
            php_thread_main(
                slot,
                rx,
                in_flight,
                max_in_flight,
                busy_count,
                busy_for_thread,
                request_count_for_thread,
                last_used_for_thread,
                slot_freed,
                Arc::clone(&drain_wake),
                owner,
            );
            exited_for_thread.store(true, Ordering::Release);
            drain_wake.notify_waiters();
        })
        .expect("spawn claviron-php thread");
    debug!("🧵 spawned claviron-sapi thread {slot}");
    #[cfg(feature = "metrics")]
    crate::metrics::record_thread_spawned();
    SapiThread {
        id: slot,
        tx,
        busy,
        request_count,
        last_used_ms,
        spawned_at_ms,
        joiner,
        exited,
    }
}

#[allow(clippy::too_many_arguments)]
fn php_thread_main(
    thread_id: u8,
    mut rx: mpsc::Receiver<PhpJob>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
    busy_count: Arc<AtomicUsize>,
    busy: Arc<AtomicBool>,
    request_count: Arc<AtomicU64>,
    last_used: Arc<AtomicU64>,
    slot_freed: Arc<Notify>,
    drain_wake: Arc<Notify>,
    owner: crate::engine::interrupt::ExecutionOwner,
) {
    if let Err(err) = ensure_sapi() {
        error!(thread_id, "php SAPI startup: {err}");
        return;
    }
    let _thread_context = PhpThreadContext;
    if let Err(err) = bind_php_thread(thread_id, owner) {
        error!(thread_id, "php thread: {err}");
        return;
    }

    while let Some(job) = rx.blocking_recv() {
        let _guard = InFlightGuard::enter(
            Arc::clone(&in_flight),
            Arc::clone(&max_in_flight),
            Arc::clone(&busy),
            Arc::clone(&busy_count),
            Arc::clone(&last_used),
            Arc::clone(&slot_freed),
            Arc::clone(&drain_wake),
        );
        // One request is one execution: active while it runs, and only then
        // may this pool's drain interrupt it.
        let _active = crate::engine::interrupt::Active::enter();
        let result = run_php_on_thread(
            job.runtime,
            job.sapi_req,
            job.parts,
            job.conn_ctx,
            job.request_body,
            job.body_sink,
        );
        let _ = job.reply.send(result);
        // Per-thread served counter — read by the optional rotation
        // manager to recycle slots that have served `max_requests`
        // jobs. Always incremented so process-level metrics still see
        // the count even when rotation is disabled.
        request_count.fetch_add(1, Ordering::Relaxed);
    }
    debug!(thread_id, "claviron-php thread: channel closed, exiting");
    #[cfg(feature = "metrics")]
    crate::metrics::record_thread_exited();
}
