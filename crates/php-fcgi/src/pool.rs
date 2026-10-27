use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant as TokioInstant, timeout_at};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

use crate::config::ReplenishConfig;
use crate::connection::{FcgiConnection, FcgiEndpoint};
use crate::error::ConnectError;
use crate::events::PoolEvents;
use crate::ping::ping_keepalive;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolOpts {
    /// This process's connection ceiling for the entry.
    /// FPM analog: `pm.max_children`.
    pub max_live: usize,
    /// Warm minimum. The replenisher proactively keeps `live` at
    /// `min_live` so cold-acquire latency stays bounded; replenish only
    /// fires when `live < min_live`, so leased conns count toward
    /// satisfying the floor — no churn on every acquire.
    /// FPM analog: `pm.start_servers` / `pm.min_spare_servers`.
    pub min_live: usize,
    /// Idle queue cap. On lease drop a connection returns to the idle
    /// queue only while `idle_count < max_idle`; beyond that it closes
    /// and the live slot is released. Long-lived burst conns (SSE,
    /// streaming, chunked uploads, request spikes) thus drain back to
    /// `max_idle` once load eases — without spamming the replenisher,
    /// since `live` stays ≥ `min_live`.
    /// FPM analog: `pm.max_spare_servers`.
    pub max_idle: usize,
    /// TCP/Unix dial deadline, so a dead FPM fails fast even when the
    /// pool is queueing patiently. It is a cap on the dial and not an
    /// extension of the caller's wait: an acquire-time dial gets whatever
    /// of this is left inside `acquire_timeout` (§3.1).
    pub connect_timeout: Duration,
    /// Max wait for a leasable pool slot when at `max_live`, and the whole
    /// wait an `acquire()` can impose — the dial it may do at the end of
    /// that wait is bounded by what remains of it.
    pub acquire_timeout: Duration,
    /// Total cap on the FCGI request/response cycle once a slot is leased.
    /// Baked at pool construction so the request hot path doesn't have to
    /// re-resolve per call.
    pub exec_timeout: Duration,
    /// Cap on the wait for FPM's response headers. The streaming path's
    /// only bound, since the body after them is deliberately uncapped.
    pub head_timeout: Duration,
    pub max_lifetime: Option<Duration>,
    pub max_requests: Option<u32>,
    pub idle_timeout: Option<Duration>,
    /// Optional FPM `ping.path` (typically `/ping`). When set, every
    /// connection the replenisher opens is validated with a single FCGI
    /// ping before it joins the idle queue. A successful `pong` confirms
    /// the FPM worker is live and the keep-alive socket can complete a
    /// full request cycle — turning "TCP connect succeeded" into "FPM
    /// end is actually responsive". When unset, only the TCP/Unix
    /// connect is performed; the first real request is the canary
    /// (operational lazy-mode fallback).
    pub ping_path: Option<String>,
    pub replenish: Option<ReplenishConfig>,
}

/// Pool capacity as an observer sees it. `live` is **this worker's**
/// census — idle plus leased — and drives balancer ranking; `idle` and
/// `replenishing` exist for the metrics provider, so a build without
/// `metrics` reads only the first field.
impl PoolOpts {
    /// Settle the three local tiers against each other.
    ///
    /// Done here and only here, and idempotent, so that a caller asking
    /// "would this configuration produce the pool I am holding?" can
    /// compare like with like — which is what reload does.
    pub fn reconciled(self) -> Self {
        let max_live = self.max_live.max(1);
        let min_live = self.min_live.min(max_live);
        let max_idle = self.max_idle.clamp(min_live, max_live);
        Self { max_live, min_live, max_idle, ..self }
    }
}

#[derive(Debug)]
#[cfg_attr(not(feature = "metrics"), allow(dead_code))]
pub struct PoolStats {
    pub live: usize,
    pub idle: usize,
    pub replenishing: bool,
}

/// What a broken keep-alive said about the connections beside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleKeepalive {
    /// One connection broke and the queue has not been contradicted. The
    /// connection is retired; nothing else is touched.
    Suspected,
    /// A second reused connection broke with none succeeding in between.
    /// The queue went with the FPM behind it, and is dropped.
    Confirmed { purged: usize },
}

#[derive(thiserror::Error, Debug)]
pub enum PoolError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error("acquire timeout")]
    Timeout,
    #[error("pool cancelled")]
    Cancelled,
}

/// The answer to "may I dial one more".
enum Admission {
    Granted,
    WorkerFull,
}

/// What one replenisher dial did.
enum CreateOutcome {
    Created,
    WorkerFull,
    Failed(PoolError),
}

struct Inner {
    endpoint: FcgiEndpoint,
    opts: PoolOpts,
    idle: StdMutex<VecDeque<FcgiConnection>>,
    idle_count: AtomicUsize,
    /// Connections this worker holds: idle plus leased. Local by
    /// definition — the replenisher floor and the idle cap are per-worker
    /// policy and always were, and the metrics provider reports this
    /// number per process.
    ///
    /// Distinct from admission, which is what `try_admit` decides. The
    /// two coincide today because the only ceiling is this worker's own.
    live: AtomicUsize,
    /// Stale-keepalive breaks on reused connections since the last one
    /// worked. A break is the failing connection's own evidence; it
    /// becomes the *queue's* only when a second reused connection breaks
    /// with none in between having succeeded, which is the difference
    /// between one FPM child recycling and the FPM behind the socket
    /// having gone away.
    breaks: AtomicUsize,
    /// What this pool has done, as against what it currently holds. See
    /// `events.rs` — gauges cannot answer any question about the time
    /// between two samples, which is where every one of these lives.
    events: PoolEvents,
    replenishing: AtomicBool,
    replenish_tasks: TaskTracker,
    replenish_start: StdMutex<()>,
    /// Pool-scoped monotonic id source for `FcgiConnection.id`. Each
    /// `FcgiConnection::open()` consumes the next value via a single
    /// `fetch_add` (relaxed); reads are lock-free since the conn owns its
    /// id afterwards. Cost is one cache-line atomic increment per fresh
    /// dial — only fires on the replenisher and acquire-time fresh-dial
    /// paths, never on the request hot path.
    next_conn_id: AtomicU64,
    notify: Notify,
    cancel: CancellationToken,
}

#[derive(Clone)]
pub struct FcgiPool {
    inner: Arc<Inner>,
}

pub struct PoolLease {
    inner: Arc<Inner>,
    conn: Option<FcgiConnection>,
    reusable: bool,
    /// Whether this connection was dialled for this lease rather than
    /// taken from the idle queue.
    ///
    /// The two failures look identical at the socket and mean opposite
    /// things: a connection out of the idle queue that breaks says the
    /// queue is stale, a connection dialled a moment ago that breaks says
    /// FPM is. Only the first is evidence about the *other* connections
    /// in the queue.
    fresh: bool,
}

/// A dial owns admission before it owns a socket. Cancellation or error must
/// return that capacity even when the dial future never reaches its match arm.
struct DialSlot<'a> {
    inner: &'a Inner,
    retained: bool,
}

impl Drop for DialSlot<'_> {
    fn drop(&mut self) {
        if !self.retained {
            self.inner.release_slot();
        }
    }
}

fn lk<T>(m: &StdMutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl Inner {
    /// Take a slot for a connection this worker is about to dial.
    ///
    /// Admission is process-local: reserve one live slot before dialing.
    fn try_admit(&self) -> Admission {
        if !self.take_local_slot() {
            return Admission::WorkerFull;
        }
        Admission::Granted
    }

    /// Raise this worker's census if it is below its own ceiling.
    fn take_local_slot(&self) -> bool {
        let max = self.opts.max_live;
        let mut cur = self.live.load(Ordering::Relaxed);
        loop {
            if cur >= max {
                return false;
            }
            match self.live.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(v) => cur = v,
            }
        }
    }

    /// Give back one slot taken by `try_admit`, and wake one waiter for
    /// the capacity that just freed.
    fn release_slot(&self) {
        self.live.fetch_sub(1, Ordering::Release);
        self.notify.notify_one();
    }

    /// Give back `n` slots at once.
    ///
    /// Only `purge_idle` releases in bulk, and it is a different act from
    /// a connection ending its own life: one suspicion drops a whole idle
    /// queue. Keeping it a named operation rather than a raw decrement is
    /// what lets it become a fence bump later without hunting for the
    /// arithmetic.
    fn release_slots(
        &self,
        n: usize,
    ) {
        if n == 0 {
            return;
        }
        self.live.fetch_sub(n, Ordering::Release);
        for _ in 0..n {
            self.notify.notify_one();
        }
    }
}

impl FcgiPool {
    pub fn new(
        endpoint: FcgiEndpoint,
        opts: PoolOpts,
        cancel: CancellationToken,
    ) -> Self {
        let opts = opts.reconciled();
        Self {
            inner: Arc::new(Inner {
                endpoint,
                opts,
                idle: StdMutex::new(VecDeque::new()),
                idle_count: AtomicUsize::new(0),
                live: AtomicUsize::new(0),
                breaks: AtomicUsize::new(0),
                events: PoolEvents::default(),
                replenishing: AtomicBool::new(false),
                replenish_tasks: TaskTracker::new(),
                replenish_start: StdMutex::new(()),
                next_conn_id: AtomicU64::new(1),
                notify: Notify::new(),
                cancel: cancel.child_token(),
            }),
        }
    }

    pub fn opts(&self) -> &PoolOpts {
        &self.inner.opts
    }

    pub fn endpoint(&self) -> &FcgiEndpoint {
        &self.inner.endpoint
    }

    /// A buffered request went out a second time. Counted by the caller
    /// because the pool cannot see a replay: it hands out a lease and is
    /// never told what the lease was for.
    pub fn count_replay(&self) {
        self.inner.events.replayed();
    }

    /// What this pool has done since it was built.
    #[cfg(feature = "metrics")]
    pub fn events(&self) -> crate::events::PoolEventCounts {
        self.inner.events.snapshot()
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            live: self.inner.live.load(Ordering::Relaxed),
            idle: self.inner.idle_count.load(Ordering::Relaxed),
            replenishing: self.inner.replenishing.load(Ordering::Relaxed),
        }
    }

    /// Source-of-truth kill sweep. Rotates the idle queue once: each
    /// conn is popped, `is_stale` checked, then dropped (live slot
    /// released) or pushed back. After the sweep, `maybe_spawn_replenisher`
    /// fires so the floor is restored if kill drove `live` below
    /// `min_live`.
    ///
    /// Designed to be called on a periodic ticker (see
    /// `housekeeping::housekeeper_loop`), but the algorithm lives here so
    /// the pool stays the single source of truth — the loop is a thin
    /// trigger.
    ///
    /// Race-safe under load: per-iter work is pure CPU + atomics
    /// (microsecond bound), no await/I/O held across mutex; `try_pop_idle`
    /// shares the same primitive as `acquire`, so concurrent acquires
    /// just see one fewer / one more conn. When the pool is fully leased
    /// (`idle_count == 0`) the sweep is a no-op.
    ///
    /// The `fcgi_housekeep` task is its only caller, so a build without
    /// the `tasks` feature compiles the algorithm and never triggers it.
    #[cfg_attr(not(feature = "tasks"), allow(dead_code))]
    pub fn housekeeping(&self) {
        let opts = &self.inner.opts;
        let count = self.inner.idle_count.load(Ordering::Acquire);

        for _ in 0..count {
            let Some(conn) = self.try_pop_idle() else { break };

            if let Some(reason) = conn.expiry(
                opts.max_lifetime,
                opts.max_requests,
                opts.idle_timeout,
            ) {
                drop(conn);
                self.inner.release_slot();
                self.inner.events.expired(reason);
                continue;
            }

            self.push_idle(conn);
        }

        self.maybe_spawn_replenisher();
    }

    /// Drop all idle connections, and say how many. Used when the pool is
    /// known poisoned — the FPM behind the socket went away and took every
    /// pooled child with it. Leased connections are untouched; when those
    /// drop, Drop will decide whether to push them back based on their own
    /// state.
    ///
    /// Callers reporting a broken keep-alive want
    /// [`FcgiPool::report_stale_keepalive`] instead: it is the same act
    /// once the evidence supports it.
    pub fn purge_idle(&self) -> usize {
        let drained: Vec<FcgiConnection> = {
            let mut q = lk(&self.inner.idle);
            q.drain(..).collect()
        };
        let n = drained.len();
        drop(drained);
        if n > 0 {
            self.inner.idle_count.fetch_sub(n, Ordering::Release);
            self.inner.release_slots(n);
            self.inner.events.purged(n);
        }
        n
    }

    /// A connection out of the idle queue broke on arrival. Answer whether
    /// the rest of the queue broke with it.
    ///
    /// The two causes are indistinguishable in the one failure. An FPM
    /// restart kills every child at once, so every pooled connection is
    /// dead and the next one breaks too. A single child reaching its own
    /// `pm.max_requests` kills exactly one, and the next pooled connection
    /// serves normally. Purging on the first break treats the second case
    /// as the first and throws away a queue of healthy connections over a
    /// rotation that was always going to happen.
    ///
    /// So the queue is purged on the second break with no reused
    /// connection having succeeded in between — `PoolLease`'s drop clears
    /// the count when one does. Waiting for that second opinion costs one
    /// request a retry, which §5.2 was already doing on a fresh connection
    /// before the client ever sees it.
    pub fn report_stale_keepalive(&self) -> StaleKeepalive {
        // Two, because one break is the connection's own evidence and two
        // is the smallest number that can be the queue's. A higher
        // threshold would not add information — every pooled connection is
        // dead in the case this is looking for — it would only make more
        // requests pay a retry to learn what the second already said.
        const CORROBORATION: usize = 2;

        if self.inner.breaks.fetch_add(1, Ordering::AcqRel) + 1 < CORROBORATION
        {
            self.inner.events.queue_suspected();
            return StaleKeepalive::Suspected;
        }
        self.inner.breaks.store(0, Ordering::Release);
        self.inner.events.queue_condemned();
        StaleKeepalive::Confirmed { purged: self.purge_idle() }
    }

    pub async fn acquire(&self) -> Result<PoolLease, PoolError> {
        self.acquire_inner(true).await
    }

    /// Acquire a connection this pool has never handed out, refusing the
    /// idle queue however warm it is.
    ///
    /// For the caller that just had a pooled connection break on it. The
    /// next connection out of the same queue is not a second opinion — it
    /// has the same provenance as the one that broke — and a retry that
    /// lands on it turns a recoverable failure into the client's error.
    /// Until the queue is condemned (§3.4) it stays where it is, so it is
    /// the retry that has to step around it.
    ///
    /// At the ceiling there is no room to dial, and the only slots are the
    /// ones the queue holds. Retiring one of those is how the room is
    /// made: they are the connections under suspicion, so the cost of
    /// being wrong is a re-dial of a connection this caller had already
    /// decided not to trust.
    pub async fn acquire_fresh(&self) -> Result<PoolLease, PoolError> {
        self.acquire_inner(false).await
    }

    async fn acquire_inner(
        &self,
        allow_idle: bool,
    ) -> Result<PoolLease, PoolError> {
        let deadline = TokioInstant::now() + self.inner.opts.acquire_timeout;
        let mut waited = false;

        loop {
            if self.inner.cancel.is_cancelled() {
                return Err(PoolError::Cancelled);
            }

            let notified = self.inner.notify.notified();

            if allow_idle && let Some(conn) = self.try_pop_idle() {
                if let Some(reason) = conn.expiry(
                    self.inner.opts.max_lifetime,
                    self.inner.opts.max_requests,
                    self.inner.opts.idle_timeout,
                ) {
                    drop(conn);
                    self.inner.release_slot();
                    self.inner.events.expired(reason);
                    continue;
                }
                self.maybe_spawn_replenisher();
                return Ok(PoolLease {
                    inner: self.inner.clone(),
                    conn: Some(conn),
                    reusable: true,
                    fresh: false,
                });
            }

            if let Admission::Granted = self.inner.try_admit() {
                let mut slot = DialSlot { inner: &self.inner, retained: false };
                // Acquire-time fresh dial: pool is dry, request is waiting.
                // We hand the freshly opened connection straight to the
                // caller — no ping. Ping only runs on the replenisher's
                // background path, where it doesn't add user-facing
                // latency. If FPM is genuinely dead on this socket the
                // first real request will surface it.
                //
                // The dial spends the caller's own budget, not a second
                // one after it: `connect_timeout` still caps it on its own
                // — a dead FPM fails fast on the replenisher's path too —
                // but it cannot run past the acquire deadline, so
                // `acquire_timeout` is the whole wait a caller can face and
                // nobody has to add two settings together to know it.
                let budget = self.inner.opts.connect_timeout;
                let remaining =
                    deadline.saturating_duration_since(TokioInstant::now());
                let dial = budget.min(remaining);
                let id =
                    self.inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
                return match FcgiConnection::open(
                    &self.inner.endpoint,
                    dial,
                    id,
                )
                .await
                {
                    Ok(conn) => {
                        slot.retained = true;
                        self.inner.events.dialled();
                        self.maybe_spawn_replenisher();
                        Ok(PoolLease {
                            inner: self.inner.clone(),
                            conn: Some(conn),
                            reusable: true,
                            fresh: true,
                        })
                    }
                    Err(e) => {
                        self.inner.events.dial_failed();
                        // A dial cut short by the acquire deadline is the
                        // caller running out of patience, not FPM refusing
                        // a connection — say so, or a healthy backend is
                        // reported as unreachable at the moment the pool is
                        // busiest. Only the dial's own timeout is rewritten;
                        // a refusal or a reset is still FPM's answer.
                        if dial < budget
                            && e.1.kind() == std::io::ErrorKind::TimedOut
                        {
                            self.inner.events.wait_expired();
                            return Err(PoolError::Timeout);
                        }
                        Err(e.into())
                    }
                };
            }

            // At the ceiling with nothing to admit against. A caller that
            // refused the queue can still make its own room in it, and the
            // connection it retires is one it had already declined to use.
            if !allow_idle && let Some(conn) = self.try_pop_idle() {
                drop(conn);
                self.inner.release_slot();
                self.inner.events.purged(1);
                continue;
            }

            // Nothing warm, nothing admissible: from here the caller is
            // queueing. Counted once per acquire that reaches the wait, not
            // once per wakeup, so it stays comparable with the request rate.
            if !waited {
                waited = true;
                self.inner.events.waited();
            }

            tokio::select! {
                res = timeout_at(deadline, notified) => {
                    if res.is_err() {
                        self.inner.events.wait_expired();
                        return Err(PoolError::Timeout);
                    }
                }
                _ = self.inner.cancel.cancelled() => {
                    return Err(PoolError::Cancelled);
                }
            }
        }
    }

    /// The replenisher's dial path: reserve a slot, dial, prove the FPM
    /// end is live, publish to the idle queue. The only path that puts a
    /// connection into the idle queue without a real request having run
    /// on it — hence the ping.
    async fn create_idle(&self) -> CreateOutcome {
        match self.inner.try_admit() {
            Admission::Granted => {}
            Admission::WorkerFull => return CreateOutcome::WorkerFull,
        };
        let mut slot = DialSlot { inner: &self.inner, retained: false };
        let id = self.inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let mut conn = match FcgiConnection::open(
            &self.inner.endpoint,
            self.inner.opts.connect_timeout,
            id,
        )
        .await
        {
            Ok(conn) => {
                self.inner.events.dialled();
                conn
            }
            Err(e) => {
                self.inner.events.dial_failed();
                return CreateOutcome::Failed(e.into());
            }
        };

        // Validate end-to-end via FPM ping.path before publishing to the
        // idle queue. If FPM is not actually live behind the socket
        // (half-open TCP, orphaned worker, restart-in-progress) the ping
        // fails here instead of biting the first real request. When
        // ping_path is unset (lazy mode), we skip this and let the first
        // real request act as canary.
        if let Some(path) = self.inner.opts.ping_path.as_deref()
            && let Err(e) = ping_keepalive(conn.client_mut(), path).await
        {
            warn!("pool replenish ping failed: {e}");
            drop(conn);
            self.inner.events.ping_failed();
            return CreateOutcome::Failed(PoolError::Connect(
                ConnectError::ping(e),
            ));
        }

        self.push_idle(conn);
        slot.retained = true;
        CreateOutcome::Created
    }

    fn try_pop_idle(&self) -> Option<FcgiConnection> {
        let conn = lk(&self.inner.idle).pop_front()?;
        self.inner.idle_count.fetch_sub(1, Ordering::Release);
        Some(conn)
    }

    fn push_idle(
        &self,
        conn: FcgiConnection,
    ) {
        lk(&self.inner.idle).push_back(conn);
        self.inner.idle_count.fetch_add(1, Ordering::Release);
        self.inner.notify.notify_one();
    }

    fn needs_replenish(&self) -> bool {
        if self.inner.opts.min_live == 0
            || self.inner.opts.replenish.is_none()
            || self.inner.cancel.is_cancelled()
        {
            return false;
        }
        // Compare against this worker's own `live` (idle + leased), NOT
        // `idle_count` and never a fleet total: `min_live` is per-worker
        // policy, so a replenisher reading the fleet would refill against
        // a number that is not its own.
        //
        // leased conn is still ours — it will return on Drop. If we
        // looked at idle_count, every acquire would temporarily drop us
        // below min_live and trigger a replenish, which combined with
        // Drop semantics (`idle_count >= max_idle` → close on return)
        // would churn one conn per request. Checking total live means
        // the pool only refills when there's a genuine shortage:
        // purge_idle, stale eviction, or unrecoverable lease drop.
        self.inner.live.load(Ordering::Acquire) < self.inner.opts.min_live
    }

    /// Retire this pool's background work without cancelling the parent runtime
    /// or interrupting an already leased response. No new repair can race drain.
    pub async fn shutdown(&self) {
        {
            let _start = lk(&self.inner.replenish_start);
            self.inner.cancel.cancel();
            self.inner.replenish_tasks.close();
        }
        self.inner.replenish_tasks.wait().await;
        self.purge_idle();
    }

    fn maybe_spawn_replenisher(&self) {
        if !self.needs_replenish() {
            return;
        }
        let _start = lk(&self.inner.replenish_start);
        if !self.needs_replenish() || self.inner.replenish_tasks.is_closed() {
            return;
        }
        if self
            .inner
            .replenishing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let pool = self.clone();
        self.inner.replenish_tasks.spawn(async move {
            {
                struct Reset<'a>(&'a AtomicBool);
                impl Drop for Reset<'_> {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::Release);
                    }
                }
                let _reset = Reset(&pool.inner.replenishing);
                let policy = pool
                    .inner
                    .opts
                    .replenish
                    .as_ref()
                    .expect("replenishment enabled");
                let mut backoff = policy.initial_backoff();
                while pool.needs_replenish() {
                    let outcome = tokio::select! {
                        biased;
                        _ = pool.inner.cancel.cancelled() => break,
                        outcome = pool.create_idle() => outcome,
                    };
                    match outcome {
                        CreateOutcome::Created => {
                            backoff = policy.initial_backoff()
                        }
                        CreateOutcome::WorkerFull => break,
                        CreateOutcome::Failed(error) => {
                            warn!("pool replenish failed: {error}");
                            if !wait_retry(&pool.inner.cancel, backoff).await {
                                break;
                            }
                            backoff = policy.next_backoff(backoff);
                        }
                    }
                }
            }
            // Close the floor-check/start-flag race without a retry poll.
            if pool.needs_replenish() {
                pool.maybe_spawn_replenisher();
            }
        });
    }
}

async fn wait_retry(
    cancel: &CancellationToken,
    delay: Duration,
) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

impl PoolLease {
    pub fn connection(&mut self) -> &mut FcgiConnection {
        self.conn.as_mut().expect("leased conn present")
    }

    pub fn mark_unrecoverable(&mut self) {
        self.reusable = false;
    }

    /// Whether this connection was dialled for this lease.
    ///
    /// A fresh connection's failure is not evidence about the idle queue,
    /// so it must not trigger a purge of it — the queue had nothing to do
    /// with a socket opened a moment ago.
    pub fn is_fresh(&self) -> bool {
        self.fresh
    }
}

impl Drop for PoolLease {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else { return };

        let stale = conn.is_stale(
            self.inner.opts.max_lifetime,
            self.inner.opts.max_requests,
            self.inner.opts.idle_timeout,
        );

        // Reusable + healthy conns return to the idle queue only while
        // `idle_count < max_idle`. Beyond that (burst zone — SSE,
        // streaming, chunked uploads, request spikes) the conn closes
        // on drop and the live slot is freed; the replenisher only
        // re-dials when total live drops below `min_live`, so post-burst
        // drains don't spam-replenish. CAS on `idle_count` so two
        // simultaneous releases can't both win the last idle slot.
        // A connection out of the idle queue that finished its request is
        // the queue speaking for itself: whatever broke earlier was not
        // the FPM behind this socket. `reusable` is false exactly when the
        // caller marked the connection unrecoverable, so this is the
        // success side of `report_stale_keepalive`.
        if self.reusable && !self.fresh {
            self.inner.breaks.store(0, Ordering::Release);
        }

        let mut conn = Some(conn);
        if self.reusable && !stale && !self.inner.cancel.is_cancelled() {
            let max_idle = self.inner.opts.max_idle;
            let mut cur = self.inner.idle_count.load(Ordering::Acquire);
            while cur < max_idle {
                match self.inner.idle_count.compare_exchange_weak(
                    cur,
                    cur + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        lk(&self.inner.idle)
                            .push_back(conn.take().expect("conn present"));
                        break;
                    }
                    Err(v) => cur = v,
                }
            }
        }

        match conn {
            // Explicit close — the burst slot returns to this worker's
            // capacity and the replenisher re-fires only if live falls
            // below `min_live`.
            Some(_) => self.inner.release_slot(),
            // The connection went back to the idle queue; wake a waiter
            // for it rather than for capacity.
            None => self.inner.notify.notify_one(),
        }
    }
}

#[cfg(test)]
#[path = "../tests/pool.rs"]
mod tests;
