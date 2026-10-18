//! SAPI thread pool — N dedicated PHP execution threads, each owning
//! its own TSRM slot and per-thread mpsc receiver.
//!
//! The pool serves as the dispatch primitive between async tasks
//! (anywhere in the Tokio runtime) and synchronous PHP execution: the
//! per-thread mpsc serialises work onto a single thread (so each
//! thread's TSRM slot stays bound and bailout aftermath is contained),
//! while round-robin dispatch + busy-CAS reservation gives cross-thread
//! parallelism without head-of-line blocking.
//!
//! Elastic mode (`threads_max > threads_min`) grows the pool on
//! demand under burst pressure and trims idle threads back to
//! `threads_max_idle` after the burst. A separate optional
//! `thread_max_life` keeps the cold floor fresh: excess idle threads
//! are culled down to `threads_min`, while idle floor slots are
//! replaced in-place. Fixed mode
//! (`threads_max == threads_min`) collapses to the legacy fixed-pool
//! shape with zero background overhead.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use super::PhpJob;
mod worker;
use worker::{SapiThread, spawn_sapi_thread};

use crate::error::SapiGatewayError;

// ---------------------------------------------------------------------
// Monotonic clock — anchored at first use, encoded as ms-since-anchor
// in a single AtomicU64 for cheap cross-thread reads. Used by the
// elastic pool's per-thread `last_used_ms` tracker.
// ---------------------------------------------------------------------

/// Monotonic clock anchor — set the first time any code asks for a
/// monotonic-ms timestamp. Subsequent reads return
/// `BOOT_INSTANT.elapsed().as_millis() as u64`, encoding wall-clock
/// in a single `AtomicU64` cheap to load/store across threads.
/// Used by the elastic pool's `last_used_ms` per-thread tracker
/// and the idle culler's "how long since last activity" check.
static BOOT_INSTANT: OnceLock<Instant> = OnceLock::new();

/// Returns ms since process start (or first call). Cheap (~ns
/// for the `Instant::elapsed` math). Granularity ms is plenty
/// for idle-cull decisions that fire at 5s ticks.
pub(crate) fn monotonic_now_ms() -> u64 {
    BOOT_INSTANT
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

// ---------------------------------------------------------------------
// Channel depth between any tokio task and a dedicated PHP thread.
// Sized for typical HTTP request burstiness; queue full means the
// PHP thread is sustained-saturated and clients should wait.
// ---------------------------------------------------------------------

const PHP_JOB_CHANNEL_CAPACITY: usize = 256;

// ---------------------------------------------------------------------
// SapiThreadPool / SapiThread — the heart of the elastic pool.
// ---------------------------------------------------------------------

/// Pool of dedicated PHP execution threads. One mpsc receiver per
/// thread — mpsc serves as the per-thread serialisation primitive,
/// the pool itself does cross-thread dispatch (round-robin).
pub(crate) struct SapiThreadPool {
    /// Identity of this pool's executions. Its drain interrupts these and no
    /// others — not a task, not an application, not another pool.
    owner: crate::engine::interrupt::ExecutionOwner,
    /// Worker threads. `RwLock` so the optional rotation manager can
    /// swap an exhausted slot in place (write-lock during the swap)
    /// while dispatch (read-lock during the round-robin scan) keeps
    /// flowing.
    ///
    /// When `max_requests` is `None` the lock is
    /// only ever taken in shared mode, so the cost is just an
    /// uncontended atomic load — ~10–20 ns/dispatch.
    threads: RwLock<Vec<SapiThread>>,
    /// Closed slots retain ownership until their OS thread, including TSRM
    /// teardown, has completed. Also covers rotation and idle culling.
    retired: Mutex<Vec<(std::thread::JoinHandle<()>, Arc<AtomicBool>)>>,
    /// Round-robin dispatch cursor. Increments unconditionally; modulo
    /// is applied at dispatch site. `Ordering::Relaxed` is sufficient —
    /// no ordering relationship with any other atomic to preserve.
    next: AtomicUsize,
    /// In-flight request counter. Incremented inside the worker loop
    /// the moment a job is taken off the receiver, decremented on
    /// scope-exit (success, error, panic — see [`worker::InFlightGuard`]).
    /// Exposed as the executing subset of the accepted work counted by
    /// `busy_count`. Shared `Arc` so the worker threads can mutate it without
    /// holding a reference back to the pool.
    in_flight: Arc<AtomicUsize>,
    /// Threads reserved by dispatch and not yet released by the PHP
    /// worker. Mirrors the busy flags as a cheap aggregate gauge.
    busy_count: Arc<AtomicUsize>,
    /// Dispatch calls that began before or raced with drain. Drain waits for
    /// these calls to either reject or reserve a concrete worker before it
    /// can report the generation stopped.
    dispatching: AtomicUsize,
    /// Highest live thread count observed since this pool was created.
    max_threads_current: Arc<AtomicUsize>,
    /// Total SAPI threads created by this pool since worker boot.
    /// Unlike `max_threads_current`, this is not a concurrency gauge:
    /// it includes initial floor threads, elastic burst growth, and
    /// replacement threads created by rotation / max-life renewal.
    threads_created: Arc<AtomicU64>,
    /// Highest busy thread count observed since this pool was created.
    max_busy_threads: Arc<AtomicUsize>,
    /// Highest in-flight request count observed since this pool was created.
    max_in_flight: Arc<AtomicUsize>,
    /// Accept-new-work flag. Starts `true`; flipped to `false` when
    /// [`SapiThreadPool::drain`] is called. New `dispatch` calls after
    /// that point return `SapiGatewayError::ServiceUnavailable` → 503 rather
    /// than enqueueing onto a doomed receiver. In-flight requests are
    /// NOT cancelled — they finish normally so transaction safety
    /// (`WalletCashier::save()` and friends) is preserved.
    accepting: Arc<AtomicBool>,
    /// Per-thread request count threshold for soft rotation. `None`
    /// (default) → rotation manager never spawns; pool runs the
    /// classic fixed-thread shape. `Some(n)` → background tick scans
    /// thread counters, recycles slots that hit the threshold while
    /// idle. This is owned by the PHP engine because request-count
    /// rotation is domain policy, not supervisor process policy.
    max_requests: Option<u64>,
    /// Slot-freed signal. Pinged from `InFlightGuard::Drop` whenever
    /// a worker thread releases its busy flag. `dispatch` waiters
    /// `.notified().await` on this to wake on the first available
    /// slot — turns the pool's busy CAS into a fair-ish acquire
    /// queue under bursty load.
    ///
    /// `Notify::notify_one()` stores a permit if no waiter is
    /// parked, so bursty `Drop`s don't lose signals — the next
    /// `.notified()` consumes the stored permit and retries
    /// dispatch immediately.
    slot_freed: Arc<Notify>,
    /// Woken whenever something a drain waits for happens: a request ends,
    /// a dispatch leaves, a thread finishes its teardown. The drain parks on
    /// it instead of looking every few milliseconds.
    drain_wake: Arc<Notify>,
    /// Serialises the rare elastic grow path. Without this, two
    /// dispatchers can both observe the same `threads.len()` and
    /// spawn two OS/PHP threads with the same exposed slot id before
    /// either gets pushed into the pool.
    grow_lock: AsyncMutex<()>,
    /// Elastic-mode floor: minimum threads kept alive at all
    /// times. Boot spawns this many; the idle culler never trims
    /// below it. When `threads_max == threads_min`, pool is fixed
    /// (no spawn-on-demand, no culler), configured with
    /// [`crate::RequestPool::fixed`].
    threads_min: usize,
    /// Elastic-mode ceiling: absolute upper bound on the spawn-
    /// on-demand path. When `dispatch` finds the pool full and
    /// `len < threads_max`, it spawns a new thread; at the
    /// ceiling, falls back to the queue / 503 path. `usize` so
    /// it composes cleanly with `Vec::len`.
    threads_max: usize,
    /// Elastic-mode trim target: when the culler runs, it
    /// reduces the pool back toward this size (but never below
    /// `threads_min`). Larger than `threads_min` keeps a warm
    /// idle reserve to absorb repeat bursts without the
    /// per-spawn cold-opcache cost.
    threads_max_idle: usize,
    /// Elastic-mode idle window: how long a thread above
    /// `threads_min` must stay idle (no `InFlightGuard` activity)
    /// before the culler considers it for removal. Short window
    /// = aggressive memory reclaim; long window = warm reserve
    /// for spiky traffic.
    thread_idle_timeout: Duration,
    /// Optional cold-pool lifetime. Once an idle thread reaches this
    /// age, the culler must not leave that OS/PHP thread alive: excess
    /// idle slots are removed down to `threads_min`, and floor slots
    /// are renewed in-place. Busy threads are never interrupted; they
    /// become eligible on the first later idle pass.
    thread_max_life: Option<Duration>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SapiThreadPoolSnapshot {
    pub accepting: bool,
    pub elastic: bool,
    pub threads_min: usize,
    pub threads_max: usize,
    pub threads_max_idle: usize,
    pub thread_idle_timeout_ms: u64,
    pub thread_max_life_ms: Option<u64>,
    pub in_flight: usize,
    pub max_in_flight: usize,
    pub threads_current: usize,
    pub max_threads_current: usize,
    pub threads_created_total: u64,
    pub busy_threads: usize,
    pub max_busy_threads: usize,
    pub idle_threads: usize,
    pub request_count_total: u64,
    pub slots: Vec<SapiThreadSlotSnapshot>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SapiThreadSlotSnapshot {
    pub id: u8,
    pub busy: bool,
    pub request_count: u64,
    pub idle_for_ms: u64,
    pub age_ms: u64,
}

struct DispatchGuard<'a>(&'a AtomicUsize, &'a Notify);

impl<'a> DispatchGuard<'a> {
    fn enter(
        counter: &'a AtomicUsize,
        drain_wake: &'a Notify,
    ) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter, drain_wake)
    }
}

impl Drop for DispatchGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
        self.1.notify_waiters();
    }
}

impl SapiThreadPool {
    /// Create the floor from [`crate::RequestPool::min_idle`]. Public
    /// options are validated before this internal primitive starts threads;
    /// its defensive lower clamp keeps a request pool executable.
    ///
    /// `max_requests` plumbed through so the rotation manager
    /// can be conditionally activated — `None` keeps the pool's hot
    /// path effectively zero-overhead vs the pre-rotation behaviour.
    pub(crate) fn spawn(
        max_requests: Option<u64>,
        threads_min: usize,
        threads_max: usize,
        threads_max_idle: usize,
        thread_idle_timeout: Duration,
        thread_max_life: Option<Duration>,
    ) -> Self {
        // Boot fills the pool to `threads_min`. Elastic growth
        // happens lazily on the dispatch path. `max(1)` because
        // a zero-thread pool can never serve a request.
        let initial = threads_min.max(1);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let busy_count = Arc::new(AtomicUsize::new(0));
        let max_threads_current = Arc::new(AtomicUsize::new(initial));
        let threads_created = Arc::new(AtomicU64::new(initial as u64));
        let max_busy_threads = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let accepting = Arc::new(AtomicBool::new(true));
        let slot_freed = Arc::new(Notify::new());
        let drain_wake = Arc::new(Notify::new());
        let owner = crate::engine::interrupt::ExecutionOwner::next();
        let mut threads: Vec<SapiThread> = Vec::with_capacity(initial);
        for id in 0..initial.min(u8::MAX as usize) as u8 {
            threads.push(spawn_sapi_thread(
                id,
                Arc::clone(&in_flight),
                Arc::clone(&max_in_flight),
                Arc::clone(&busy_count),
                Arc::clone(&slot_freed),
                Arc::clone(&drain_wake),
                owner,
            ));
        }
        Self {
            owner,
            threads: RwLock::new(threads),
            retired: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
            in_flight,
            busy_count,
            dispatching: AtomicUsize::new(0),
            max_threads_current,
            threads_created,
            max_busy_threads,
            max_in_flight,
            accepting,
            max_requests,
            slot_freed,
            drain_wake,
            grow_lock: AsyncMutex::new(()),
            threads_min: initial,
            threads_max: threads_max.max(initial),
            threads_max_idle: threads_max_idle.max(initial),
            thread_idle_timeout,
            thread_max_life,
        }
    }

    /// True when elastic mode is engaged (i.e. the pool may grow
    /// past `threads_min`). Cheap check — doesn't touch the lock.
    /// Used by `dispatch` to skip the spawn-on-demand path entirely
    /// when the pool is configured as fixed-size.
    fn is_elastic(&self) -> bool {
        self.threads_max > self.threads_min
    }

    fn ensure_accepting(&self) -> Result<(), SapiGatewayError> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(SapiGatewayError::ServiceUnavailable(
                "php request pool is not accepting requests".to_string(),
            ))
        }
    }

    /// Re-open a fully drained process-owned pool for a new service
    /// generation. A bounded drain that returned while PHP was still
    /// executing is deliberately not restartable until those accepted jobs
    /// finish naturally.
    pub(crate) fn resume(&self) -> Result<(), usize> {
        self.ensure_drained()?;
        super::SAPI_SHUTTING_DOWN.store(false, Ordering::Release);
        super::renew_sapi_shutdown_signal();
        let mut threads = self.threads.write().expect("php pool lock poisoned");
        if threads.is_empty() {
            for id in 0..self.threads_min {
                threads.push(spawn_sapi_thread(
                    id as u8,
                    Arc::clone(&self.in_flight),
                    Arc::clone(&self.max_in_flight),
                    Arc::clone(&self.busy_count),
                    Arc::clone(&self.slot_freed),
                    Arc::clone(&self.drain_wake),
                    self.owner,
                ));
                self.threads_created.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.accepting.store(true, Ordering::Release);
        Ok(())
    }

    /// Check the same accepted-work boundary used by drain, without opening
    /// admission. A queued reservation counts even before PHP starts it.
    pub(crate) fn ensure_drained(&self) -> Result<(), usize> {
        let dispatching = self.dispatching.load(Ordering::Acquire);
        let accepted = self.busy_count.load(Ordering::Acquire);
        if !self.accepting.load(Ordering::Acquire)
            && dispatching == 0
            && accepted == 0
        {
            let threads = std::mem::take(
                &mut *self.threads.write().expect("php pool lock poisoned"),
            );
            for thread in threads {
                self.retire(thread);
            }
        }
        self.reap_retired();
        let retired = self
            .retired
            .lock()
            .expect("php retired threads lock poisoned")
            .len();
        if dispatching == 0 && accepted == 0 && retired == 0 {
            Ok(())
        } else {
            Err(dispatching.saturating_add(accepted).saturating_add(retired))
        }
    }

    fn retire(
        &self,
        thread: SapiThread,
    ) {
        let SapiThread { tx, joiner, exited, .. } = thread;
        drop(tx);
        self.retired
            .lock()
            .expect("php retired threads lock poisoned")
            .push((joiner, exited));
        self.reap_retired();
    }

    fn reap_retired(&self) {
        let mut retired =
            self.retired.lock().expect("php retired threads lock poisoned");
        let mut index = 0;
        while index < retired.len() {
            // `exited` is the thread's last act before returning, so the
            // join waits only for a return already under way.
            if retired[index].1.load(Ordering::Acquire) {
                if retired.swap_remove(index).0.join().is_err() {
                    error!("retired PHP thread panicked");
                }
            } else {
                index += 1;
            }
        }
    }

    pub(crate) fn snapshot(&self) -> SapiThreadPoolSnapshot {
        let now = monotonic_now_ms();
        let threads = self.threads.read().expect("php pool lock poisoned");
        let slots = threads
            .iter()
            .map(|thread| {
                let busy = thread.busy.load(Ordering::Acquire);
                let last_used = thread.last_used_ms.load(Ordering::Acquire);
                SapiThreadSlotSnapshot {
                    id: thread.id,
                    busy,
                    request_count: thread.request_count.load(Ordering::Relaxed),
                    idle_for_ms: now.saturating_sub(last_used),
                    age_ms: now.saturating_sub(thread.spawned_at_ms),
                }
            })
            .collect::<Vec<_>>();
        let busy_threads = slots.iter().filter(|slot| slot.busy).count();
        let request_count_total = slots
            .iter()
            .map(|slot| slot.request_count)
            .fold(0u64, u64::saturating_add);
        let threads_current = slots.len();

        SapiThreadPoolSnapshot {
            accepting: self.accepting.load(Ordering::Acquire),
            elastic: self.is_elastic(),
            threads_min: self.threads_min,
            threads_max: self.threads_max,
            threads_max_idle: self.threads_max_idle,
            thread_idle_timeout_ms: self
                .thread_idle_timeout
                .as_millis()
                .min(u128::from(u64::MAX))
                as u64,
            thread_max_life_ms: self
                .thread_max_life
                .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64),
            in_flight: self.in_flight.load(Ordering::Acquire),
            max_in_flight: self.max_in_flight.load(Ordering::Acquire),
            threads_current,
            max_threads_current: self
                .max_threads_current
                .load(Ordering::Acquire),
            threads_created_total: self.threads_created.load(Ordering::Acquire),
            busy_threads,
            max_busy_threads: self.max_busy_threads.load(Ordering::Acquire),
            idle_threads: threads_current.saturating_sub(busy_threads),
            request_count_total,
            slots,
        }
    }

    #[allow(clippy::result_large_err)]
    /// Try to grow the pool by spawning one new thread, then
    /// dispatch the job onto it. Returns `Ok(())` on success,
    /// `Err((reason, job))` if growth is unavailable (already at
    /// `threads_max`) or the post-spawn dispatch raced and lost.
    /// Spawn cost (~5ms) is paid here on the hot path; rare under
    /// healthy traffic, fine under bursts.
    async fn try_grow_and_dispatch(
        &self,
        job: PhpJob,
    ) -> Result<(), (SapiGatewayError, PhpJob)> {
        if let Err(error) = self.ensure_accepting() {
            return Err((error, job));
        }
        if !self.is_elastic() {
            return Err((
                SapiGatewayError::ServiceUnavailable(
                    "php pool has no idle thread".to_string(),
                ),
                job,
            ));
        }
        let _grow_guard = self.grow_lock.lock().await;
        if let Err(error) = self.ensure_accepting() {
            return Err((error, job));
        }
        // Capacity check under read-lock. Cheap.
        {
            let Ok(threads) = self.threads.read() else {
                return Err((
                    SapiGatewayError::Internal(
                        "php pool lock poisoned".to_string(),
                    ),
                    job,
                ));
            };
            if threads.len() >= self.threads_max {
                return Err((
                    SapiGatewayError::ServiceUnavailable(
                        "php pool at threads_max ceiling".to_string(),
                    ),
                    job,
                ));
            }
        }
        // Spawn outside the lock — the new thread's std::thread::spawn
        // + ext_php_rs_sapi_per_thread_init costs ~5ms. Holding the
        // write-lock that long would starve other dispatchers.
        let next_slot_id = {
            let Ok(threads) = self.threads.read() else {
                return Err((
                    SapiGatewayError::Internal(
                        "php pool lock poisoned".to_string(),
                    ),
                    job,
                ));
            };
            // Slot id wraps if we exceed u8::MAX, but threads_max is
            // a u8 in config so this is bounded by the operator.
            (threads.len() % (u8::MAX as usize + 1)) as u8
        };
        let new_thread = spawn_sapi_thread(
            next_slot_id,
            Arc::clone(&self.in_flight),
            Arc::clone(&self.max_in_flight),
            Arc::clone(&self.busy_count),
            Arc::clone(&self.slot_freed),
            Arc::clone(&self.drain_wake),
            self.owner,
        );
        // Write-lock briefly to push and re-check the ceiling — a
        // racing grower could have pushed first.
        let push_idx = {
            let Ok(mut threads) = self.threads.write() else {
                self.retire(new_thread);
                return Err((
                    SapiGatewayError::Internal(
                        "php pool lock poisoned".to_string(),
                    ),
                    job,
                ));
            };
            if threads.len() >= self.threads_max {
                drop(threads);
                self.retire(new_thread);
                return Err((
                    SapiGatewayError::ServiceUnavailable(
                        "php pool at threads_max ceiling".to_string(),
                    ),
                    job,
                ));
            }
            threads.push(new_thread);
            self.threads_created.fetch_add(1, Ordering::Relaxed);
            update_max(&self.max_threads_current, threads.len());
            threads.len() - 1
        };
        debug!(
            slot = push_idx,
            "🌱 SAPI elastic pool grew: spawned new thread on dispatch demand"
        );
        // Dispatch directly to the just-pushed thread. Re-acquire
        // read-lock to access by index.
        let Ok(threads) = self.threads.read() else {
            return Err((
                SapiGatewayError::Internal(
                    "php pool lock poisoned".to_string(),
                ),
                job,
            ));
        };
        let Some(thread) = threads.get(push_idx) else {
            // Highly unlikely (culler + grow race). Fall back.
            return Err((
                SapiGatewayError::BadGateway(
                    "php pool grew but slot vanished mid-dispatch".to_string(),
                ),
                job,
            ));
        };
        // CAS busy false→true. New thread is guaranteed idle (just
        // spawned, blocking on first recv); CAS should succeed.
        if thread
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Some other dispatcher beat us to the new thread —
            // very rare under read-lock contention.
            return Err((
                SapiGatewayError::BadGateway(
                    "php pool spawned thread already claimed".to_string(),
                ),
                job,
            ));
        }
        self.record_busy_reserved();
        match thread.tx.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(returned))
            | Err(mpsc::error::TrySendError::Closed(returned)) => {
                thread.busy.store(false, Ordering::Release);
                self.busy_count.fetch_sub(1, Ordering::AcqRel);
                self.drain_wake.notify_waiters();
                Err((
                    SapiGatewayError::BadGateway(
                        "php pool spawned thread channel error".to_string(),
                    ),
                    returned,
                ))
            }
        }
    }

    /// Replace the thread occupying `slot` with a freshly-spawned one.
    /// Caller has already verified `count >= threshold && !busy` for
    /// this slot; we re-check under the write lock to defend against
    /// the race where `dispatch` reserved this thread between our
    /// scan and the swap. `id` is preserved across the swap so traces
    /// / `$_SERVER['CLAVIRON_THREAD_ID']` stay stable per slot.
    ///
    /// Close the old sender and retain its handle for nonblocking reaping.
    /// Generation drain also waits for every retired thread's teardown.
    fn rotate_slot(
        &self,
        slot: usize,
        threshold: u64,
    ) -> bool {
        let new_thread = spawn_sapi_thread(
            slot as u8,
            Arc::clone(&self.in_flight),
            Arc::clone(&self.max_in_flight),
            Arc::clone(&self.busy_count),
            Arc::clone(&self.slot_freed),
            Arc::clone(&self.drain_wake),
            self.owner,
        );
        let old_thread = {
            let Ok(mut threads) = self.threads.write() else {
                self.retire(new_thread);
                return false;
            };
            let Some(existing) = threads.get(slot) else {
                self.retire(new_thread);
                return false;
            };
            // Re-check: if dispatch claimed this thread between our scan
            // and the lock acquisition, skip — we'll catch it next pass.
            if existing.request_count.load(Ordering::Relaxed) < threshold
                || existing.busy.load(Ordering::Acquire)
            {
                self.retire(new_thread);
                return false;
            }
            self.threads_created.fetch_add(1, Ordering::Relaxed);
            std::mem::replace(&mut threads[slot], new_thread)
        };
        debug!(
            slot = slot,
            served = old_thread.request_count.load(Ordering::Relaxed),
            "♻️  rotated SAPI thread (per-thread soft reset)"
        );
        self.retire(old_thread);
        true
    }

    /// One pass over the pool checking which slots want rotation.
    /// Two phases — collect candidates under read-lock, then perform
    /// each swap under its own short write-lock. Avoids holding the
    /// write-lock across a thread spawn (~ms) and keeps dispatch
    /// happy for the duration. Manager runs at most one rotation
    /// per slot per pass; bursty rotations queue across ticks.
    fn rotation_pass(
        &self,
        threshold: u64,
    ) {
        let candidates: Vec<usize> = {
            let Ok(threads) = self.threads.read() else { return };
            threads
                .iter()
                .enumerate()
                .filter(|(_, t)| {
                    t.request_count.load(Ordering::Relaxed) >= threshold
                        && !t.busy.load(Ordering::Acquire)
                })
                .map(|(i, _)| i)
                .collect()
        };
        for slot in candidates {
            self.rotate_slot(slot, threshold);
        }
    }

    /// Background idle culler. Periodically scans the pool in two
    /// stages:
    ///
    /// 1. warm burst recovery: idle threads above `threads_max_idle`
    ///    are removed after `thread_idle_timeout`;
    /// 2. optional cold hygiene: if `thread_max_life` is set, old idle
    ///    threads are removed down to `threads_min`, then floor slots
    ///    are renewed in-place so no idle thread remains older than the
    ///    configured lifetime.
    ///
    /// Only spawned when elastic mode is engaged
    /// (`threads_max > threads_min`); fixed pools pay zero
    /// background overhead. Cooperates with shutdown via the
    /// `accepting` flag, mirroring `spawn_rotation_manager`.
    pub(crate) fn spawn_idle_culler(
        self: Arc<Self>,
        cancellation: CancellationToken,
    ) -> Option<JoinHandle<()>> {
        if !self.is_elastic() {
            return None;
        }
        debug!(
            min = self.threads_min,
            max = self.threads_max,
            max_idle = self.threads_max_idle,
            // idle_timeout = ?self.thread_idle_timeout,
            // max_life = ?self.thread_max_life,
            "🌱 SAPI elastic pool culler starting"
        );
        Some(tokio::spawn(async move {
            // Tick every 5s — coarser than rotation. Idle culling
            // is not latency-sensitive; a few seconds of "extra
            // thread alive" after timeout is operationally
            // indistinguishable from immediate trimming.
            let tick = Duration::from_secs(5);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(tick) => {}
                }
                if !self.accepting.load(Ordering::Acquire) {
                    debug!("🍂 idle culler: drain in progress, exiting");
                    return;
                }
                self.cull_pass();
            }
        }))
    }

    /// Single culler pass. Two-phase to keep the write-lock
    /// window minimal: scan candidates under read-lock, then
    /// remove under a brief write-lock. Iterates in REVERSE so
    /// removal indices remain valid as we shrink the Vec.
    fn cull_pass(&self) {
        let now = monotonic_now_ms();
        self.cull_idle_to_max_idle(now);
        self.cull_or_renew_max_life(now);
    }

    /// Warm burst recovery: once a burst has ended, trim idle slots
    /// above `threads_max_idle` after `thread_idle_timeout`.
    fn cull_idle_to_max_idle(
        &self,
        now: u64,
    ) {
        let idle_threshold_ms =
            self.thread_idle_timeout.as_millis().min(u128::from(u64::MAX))
                as u64;
        let target_len = self.threads_max_idle.max(self.threads_min);
        let to_remove: Vec<usize> = {
            let Ok(threads) = self.threads.read() else { return };
            let len = threads.len();
            if len <= target_len {
                return;
            }
            let mut budget = len - target_len;
            let mut marked = Vec::with_capacity(budget);
            // Reverse iteration → indices stable during write-lock removal.
            for (i, t) in threads.iter().enumerate().rev() {
                if budget == 0 {
                    break;
                }
                if t.busy.load(Ordering::Acquire) {
                    continue;
                }
                let last = t.last_used_ms.load(Ordering::Acquire);
                if now.saturating_sub(last) >= idle_threshold_ms {
                    marked.push(i);
                    budget -= 1;
                }
            }
            marked
        };
        if to_remove.is_empty() {
            return;
        }
        let Ok(mut threads) = self.threads.write() else { return };
        for idx in to_remove {
            // Floor protection: never trim below `threads_min`.
            // (Race-safe re-check under write-lock.)
            if threads.len() <= self.threads_min {
                break;
            }
            // Re-check eligibility under write-lock — a dispatcher
            // may have claimed this slot between scan and lock.
            if threads.get(idx).is_none_or(|t| t.busy.load(Ordering::Acquire)) {
                continue;
            }
            let removed = threads.remove(idx);
            debug!(
                slot = removed.id,
                served = removed.request_count.load(Ordering::Relaxed),
                "🍂 culled idle SAPI thread"
            );
            self.retire(removed);
        }
    }

    /// Cold hygiene: once a thread has lived past `thread_max_life`,
    /// do not leave it around while idle. Tail slots above `threads_min`
    /// are removed. Lower slots are renewed in-place so the Vec index ↔
    /// exposed slot id invariant is preserved.
    fn cull_or_renew_max_life(
        &self,
        now: u64,
    ) {
        let Some(max_life_ms) = self
            .thread_max_life
            .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        else {
            return;
        };
        let candidates: Vec<usize> = {
            let Ok(threads) = self.threads.read() else { return };
            threads
                .iter()
                .enumerate()
                .rev()
                .filter(|(_, t)| {
                    !t.busy.load(Ordering::Acquire)
                        && now.saturating_sub(t.spawned_at_ms) >= max_life_ms
                })
                .map(|(idx, _)| idx)
                .collect()
        };
        if candidates.is_empty() {
            return;
        }

        let mut renew = Vec::new();
        {
            let Ok(mut threads) = self.threads.write() else { return };
            for idx in candidates {
                let Some(thread) = threads.get(idx) else { continue };
                if thread.busy.load(Ordering::Acquire)
                    || now.saturating_sub(thread.spawned_at_ms) < max_life_ms
                {
                    continue;
                }
                if threads.len() > self.threads_min && idx + 1 == threads.len()
                {
                    let removed = threads.remove(idx);
                    debug!(
                        slot = removed.id,
                        served = removed.request_count.load(Ordering::Relaxed),
                        age_ms = now.saturating_sub(removed.spawned_at_ms),
                        "🍂 culled max-life idle SAPI thread"
                    );
                    self.retire(removed);
                } else {
                    renew.push(idx);
                }
            }
        }

        for idx in renew {
            self.renew_idle_slot_if_old(idx, now, max_life_ms);
        }
    }

    /// Replace one old idle slot. This is the `threads_min` counterpart
    /// to culling: the pool keeps its warm floor, but no idle OS/PHP
    /// thread is allowed to outlive `thread_max_life`.
    fn renew_idle_slot_if_old(
        &self,
        slot: usize,
        now: u64,
        max_life_ms: u64,
    ) -> bool {
        let new_thread = spawn_sapi_thread(
            slot as u8,
            Arc::clone(&self.in_flight),
            Arc::clone(&self.max_in_flight),
            Arc::clone(&self.busy_count),
            Arc::clone(&self.slot_freed),
            Arc::clone(&self.drain_wake),
            self.owner,
        );
        let old_thread = {
            let Ok(mut threads) = self.threads.write() else {
                self.retire(new_thread);
                return false;
            };
            let Some(existing) = threads.get(slot) else {
                self.retire(new_thread);
                return false;
            };
            if existing.busy.load(Ordering::Acquire)
                || now.saturating_sub(existing.spawned_at_ms) < max_life_ms
            {
                self.retire(new_thread);
                return false;
            }
            self.threads_created.fetch_add(1, Ordering::Relaxed);
            std::mem::replace(&mut threads[slot], new_thread)
        };
        debug!(
            slot = slot,
            served = old_thread.request_count.load(Ordering::Relaxed),
            age_ms = now.saturating_sub(old_thread.spawned_at_ms),
            "♻️  renewed max-life idle SAPI floor thread"
        );
        self.retire(old_thread);
        true
    }

    /// Background rotation manager. Spawned at most once per pool
    /// (only when `max_requests` is `Some`). Cooperates
    /// with shutdown via the `accepting` flag — once drain begins
    /// the manager exits without doing further rotations.
    pub(crate) fn spawn_rotation_manager(
        self: Arc<Self>,
        cancellation: CancellationToken,
    ) -> Option<JoinHandle<()>> {
        let threshold = match self.max_requests {
            Some(t) if t > 0 => t,
            _ => return None,
        };
        debug!(
            threshold,
            "♻️  SAPI per-thread rotation manager starting (threshold per thread)"
        );
        Some(tokio::spawn(async move {
            // Tick cadence: 1s is fine — rotation isn't latency-sensitive,
            // and a coarser scan keeps overhead invisible. Worker
            // counters increment monotonically; 1s of "above threshold"
            // before recycling is operationally indistinguishable from
            // immediate recycling.
            let tick = Duration::from_secs(1);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = tokio::time::sleep(tick) => {}
                }
                if !self.accepting.load(Ordering::Acquire) {
                    debug!("♻️  rotation manager: drain in progress, exiting");
                    return;
                }
                self.rotation_pass(threshold);
            }
        }))
    }

    #[allow(clippy::result_large_err)]
    /// One dispatch attempt — round-robin over threads, claim the
    /// first idle one via busy CAS, `try_send` the job to its mpsc.
    /// Returns `Ok(())` on success, or `Err((reason, job))` on
    /// failure so [`Self::dispatch`] can park on `slot_freed` and
    /// retry without rebuilding the job.
    ///
    /// Does NOT increment `engine_errors_total` — that's the
    /// caller's job, only after the queue deadline expires (a
    /// transient miss that recovers via the queue is normal flow,
    /// not an engine error).
    async fn try_dispatch_once(
        &self,
        mut job: PhpJob,
    ) -> Result<(), (SapiGatewayError, PhpJob)> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err((
                SapiGatewayError::ServiceUnavailable(
                    "php request pool is not accepting requests".to_string(),
                ),
                job,
            ));
        }
        // Read-lock for the whole round-robin scan. `try_send` is
        // non-blocking, so we hold the lock for at most a handful of
        // atomic ops — rotation manager's write-lock waits the same
        // sub-microsecond window. With `max_requests=None`
        // (default) the manager never runs, so the lock is purely
        // uncontended ~10–20 ns/dispatch overhead.
        let threads = match self.threads.read() {
            Ok(g) => g,
            Err(_) => {
                return Err((
                    SapiGatewayError::Internal(
                        "php pool lock poisoned".to_string(),
                    ),
                    job,
                ));
            }
        };
        let n = threads.len();
        if n == 0 {
            return Err((
                SapiGatewayError::ServiceUnavailable(
                    "php pool has no threads".to_string(),
                ),
                job,
            ));
        }
        for _ in 0..n {
            let idx = self.next.fetch_add(1, Ordering::Relaxed) % n;
            let thread = &threads[idx];
            if thread
                .busy
                .compare_exchange(
                    false,
                    true,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            self.record_busy_reserved();
            match thread.tx.try_send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(returned))
                | Err(mpsc::error::TrySendError::Closed(returned)) => {
                    thread.busy.store(false, Ordering::Release);
                    self.busy_count.fetch_sub(1, Ordering::AcqRel);
                    self.drain_wake.notify_waiters();
                    job = returned;
                }
            }
        }
        Err((
            SapiGatewayError::ServiceUnavailable(
                "php pool has no idle thread".to_string(),
            ),
            job,
        ))
    }

    /// Dispatch with bounded queueing. Tries [`Self::try_dispatch_once`]
    /// first; on failure, parks on the `slot_freed` notify (pinged by
    /// every `InFlightGuard::Drop`) and retries until either:
    ///   * a slot frees → request gets dispatched, normal flow
    ///   * `queue_timeout` elapses → 503 surfaces to the caller
    ///
    /// `queue_timeout = Duration::ZERO`
    /// means "no queue, instant 503 on exhaustion" — same behaviour
    /// as before the queue landed. Every request passes the
    /// configured timeout: admission is about waiting for a thread,
    /// not about how the reply is sent.
    pub(crate) async fn dispatch(
        &self,
        job: PhpJob,
        queue_timeout: Option<Duration>,
    ) -> Result<(), SapiGatewayError> {
        let _dispatch =
            DispatchGuard::enter(&self.dispatching, &self.drain_wake);
        self.ensure_accepting()?;
        // First attempt. The vast majority of dispatches succeed
        // here — the queue + spawn paths are reserved for bursts.
        let mut job = match self.try_dispatch_once(job).await {
            Ok(()) => return Ok(()),
            Err((_, returned)) => returned,
        };
        self.ensure_accepting()?;
        // Elastic grow: spawn-on-demand beats queueing because the
        // new thread becomes a permanent capacity bump (until the
        // culler trims it), benefiting all subsequent waiters too.
        // Skipped for fixed pools (zero overhead in the
        // `threads_max == threads_min` case).
        if self.is_elastic() {
            match self.try_grow_and_dispatch(job).await {
                Ok(()) => return Ok(()),
                Err((_, returned)) => job = returned,
            }
        }
        self.ensure_accepting()?;
        if queue_timeout.is_some_and(|timeout| timeout.is_zero()) {
            self.dump_pool_state_on_exhaustion();
            #[cfg(feature = "metrics")]
            crate::metrics::record_engine_error();
            return Err(SapiGatewayError::ServiceUnavailable(
                "php pool has no idle thread".to_string(),
            ));
        }
        // Queue path. Wake on slot_freed, retry. Bounded by
        // queue_timeout — past that, surface ServiceUnavailable exactly
        // like the no-queue path so frontend retry semantics match.
        // Unset waits until a thread frees; the drain ends that wait too, so
        // a queued request never holds a stop open.
        let deadline =
            queue_timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        let shutdown = super::sapi_shutdown_signal();
        loop {
            tokio::select! {
                _ = self.slot_freed.notified() => {}
                _ = shutdown.cancelled() => {}
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await
                    }
                } => {
                    self.dump_pool_state_on_exhaustion();
                    #[cfg(feature = "metrics")]
                    crate::metrics::record_engine_error();
                    return Err(SapiGatewayError::ServiceUnavailable(
                        "php pool queue timeout".to_string(),
                    ));
                }
            }
            self.ensure_accepting()?;
            // After a slot frees, prefer existing idle thread;
            // re-attempt grow only if the retry races and loses.
            job = match self.try_dispatch_once(job).await {
                Ok(()) => return Ok(()),
                Err((_, returned)) => returned,
            };
            self.ensure_accepting()?;
            if self.is_elastic() {
                match self.try_grow_and_dispatch(job).await {
                    Ok(()) => return Ok(()),
                    Err((_, returned)) => job = returned,
                }
            }
            self.ensure_accepting()?;
        }
    }

    /// Operational dump when the pool reports exhausted — a
    /// recurring `busy=true count=N` on every slot is the
    /// signature of permit-leak regressions (e.g.
    /// `InFlightGuard::Drop` not firing or `php_request_shutdown`
    /// hanging mid-cleanup). Cheap enough to keep at warn level
    /// since 503s are by definition rare.
    fn dump_pool_state_on_exhaustion(&self) {
        let Ok(threads) = self.threads.read() else { return };
        let snapshot: Vec<String> = threads
            .iter()
            .map(|t| {
                format!(
                    "id={} busy={} count={}",
                    t.id,
                    t.busy.load(Ordering::Acquire),
                    t.request_count.load(Ordering::Relaxed)
                )
            })
            .collect();
        warn!(threads = ?snapshot, "php pool has no idle thread");
    }

    fn record_busy_reserved(&self) {
        let current = self.busy_count.fetch_add(1, Ordering::AcqRel) + 1;
        update_max(&self.max_busy_threads, current);
    }

    /// Stop accepting new work and wait for in-flight requests to
    /// drain. The dedicated PHP thread cannot be cancelled mid-script
    /// without risking corrupted state (half-flushed transactions,
    /// abandoned locks); the contract is "claviron waits, PHP code
    /// runs to its natural completion".
    ///
    /// Sequence:
    ///   1. Flip the global `SAPI_SHUTTING_DOWN` flag so the
    ///      `claviron_is_cancelled()` PHP function starts returning
    ///      `true`. Polling code can voluntarily shed load (skip
    ///      starting new transactions, return queued items, etc.).
    ///   2. Flip `accepting` to false — new HTTP requests get
    ///      `ServiceUnavailable("php pool draining for shutdown")` instead of
    ///      sitting on a channel about to be dropped.
    ///   3. Wait for dispatch calls racing with the gate to settle, then poll
    ///      `busy_count` until zero, covering both queued and executing
    ///      accepted jobs. The wait is capped by `grace`. `None`
    ///      grace = wait forever (correct default for transactional
    ///      workloads where partial commits are catastrophic). A
    ///      `Some(d)` grace bounds the wait — we log a warning and
    ///      return regardless, leaving the supervisor's hard kill
    ///      timer to deliver SIGKILL if it has one.
    ///
    /// Once accepted work finishes, close all slot channels and join the OS
    /// threads, including retired slots and TSRM teardown. An expired bound
    /// retains unfinished handles; it never detaches them. The pool object
    /// and telemetry remain, but a successful stop leaves no live capacity.
    /// Resume creates fresh floor threads while retaining the process SAPI.
    /// How long an interrupted execution is given to unwind before the drain
    /// gives up on it. The VM acts on the interrupt at its next check, which is
    /// immediate for a script that is running bytecode; this is slack for the
    /// unwinding itself, not for the script to notice.
    const INTERRUPT_GRACE: Duration = Duration::from_secs(1);

    pub(crate) async fn drain(
        &self,
        grace: Option<Duration>,
    ) -> bool {
        super::SAPI_SHUTTING_DOWN.store(true, Ordering::Release);
        super::cancel_sapi_shutdown_signal();
        self.accepting.store(false, Ordering::Release);

        let started = Instant::now();
        let mut interrupted = false;
        loop {
            // Registered before the state is read, so a request ending or a
            // thread exiting between the read and the wait still wakes it.
            let woken = self.drain_wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let dispatching = self.dispatching.load(Ordering::Acquire);
            let accepted = self.busy_count.load(Ordering::Acquire);
            if dispatching == 0 && accepted == 0 {
                let threads = std::mem::take(
                    &mut *self.threads.write().expect("php pool lock poisoned"),
                );
                for thread in threads {
                    self.retire(thread);
                }
                self.reap_retired();
                if self
                    .retired
                    .lock()
                    .expect("php retired threads lock poisoned")
                    .is_empty()
                {
                    debug!(
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "php drain complete — requests finished and OS threads joined"
                    );
                    return true;
                }
            }
            if let Some(d) = grace
                && started.elapsed() >= d
            {
                // The grace is the cooperative window: a request that is going
                // to finish has had it. What is left is a script that will not
                // end on its own, and walking away from it is what turns a stop
                // into a kill — so it is interrupted, and then given a short
                // window to unwind before the drain reports failure.
                if !interrupted {
                    let signalled =
                        crate::engine::interrupt::interrupt_owned_by(
                            self.owner,
                        );
                    warn!(
                        dispatching,
                        accepted,
                        signalled,
                        grace_ms = d.as_millis() as u64,
                        "🚨 php drain grace exceeded; interrupting in-flight executions"
                    );
                    interrupted = true;
                    continue;
                }

                if started.elapsed() >= d + Self::INTERRUPT_GRACE {
                    warn!(
                        dispatching,
                        accepted,
                        in_flight = self.in_flight.load(Ordering::Acquire),
                        "🚨 php drain could not end its in-flight executions; \
                         leaving them to the kill timer"
                    );
                    return false;
                }
            }
            // Woken by what it waits for; the grace, when there is one, is
            // the only timer, and it is the operator's.
            match grace {
                Some(d) => {
                    let limit =
                        if interrupted { d + Self::INTERRUPT_GRACE } else { d };
                    tokio::select! {
                        _ = &mut woken => {}
                        _ = tokio::time::sleep_until((started + limit).into()) => {}
                    }
                }
                None => woken.await,
            }
        }
    }
}

fn update_max(
    target: &AtomicUsize,
    value: usize,
) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(
            current,
            value,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}
