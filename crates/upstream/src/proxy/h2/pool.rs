//! Per-origin H2 connections; snapshot selection and serialized cold dials.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use hyper::client::conn::http2::SendRequest;
use tokio::sync::{Mutex, Notify, Semaphore};

use super::conn::H2Connection;
use super::permit::H2Permit;
use crate::compile::profile::Profile;
use crate::origin::PoolGeneration;
use crate::proxy::body::H2Body;
use crate::proxy::failure::RequestFailure;

#[derive(Debug)]
pub struct H2TargetPool {
    profile: Arc<Profile>,
    pub(crate) breaker: Option<Arc<crate::proxy::service::breaker::State>>,
    generation: PoolGeneration,
    live: Arc<Semaphore>,
    next_conn_id: AtomicU64,
    connections: ArcSwap<Vec<Arc<H2Connection>>>,
    mutation: std::sync::Mutex<()>,
    connecting: Mutex<()>,
    pub(super) warming: AtomicBool,
    pub(super) in_flight: AtomicUsize,
    closed: AtomicBool,
    pub(super) changed: Arc<Notify>,
    slot: crate::state::SlotRef,
}
#[derive(Debug)]
pub struct H2AcquiredStream {
    pub sender: SendRequest<H2Body>,
    pub permit: H2Permit,
}
#[derive(Debug)]
pub enum H2AcquireError<E> {
    Connect(E),
    AcquireTimeout,
    Pool(RequestFailure),
}
// Wake capacity waiters even when a dial is cancelled before publication.
struct Connecting<'a> {
    pool: &'a H2TargetPool,
    _guard: Option<tokio::sync::MutexGuard<'a, ()>>,
    live: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Drop for Connecting<'_> {
    fn drop(&mut self) {
        self.live.take();
        self._guard.take();
        self.pool.changed.notify_waiters();
    }
}
impl H2TargetPool {
    pub(crate) fn new(
        profile: Arc<Profile>,
        generation: PoolGeneration,
        origin: crate::origin::OriginId,
    ) -> Self {
        Self {
            breaker: None,
            slot: profile.origins[origin.index()].slot.get().clone(),
            live: Arc::new(Semaphore::new(profile.capacity.max_live())),
            profile,
            generation,
            next_conn_id: AtomicU64::new(1),
            connections: ArcSwap::from_pointee(Vec::new()),
            mutation: std::sync::Mutex::new(()),
            connecting: Mutex::new(()),
            warming: AtomicBool::new(false),
            in_flight: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
            changed: Arc::new(Notify::new()),
        }
    }
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
    /// Capacity of the currently established pool, not a connection count.
    pub(crate) fn stream_limit(&self) -> Option<usize> {
        self.connections.load().iter().try_fold(0_usize, |sum, conn| {
            conn.capacity.limit().map(|limit| sum.saturating_add(limit))
        })
    }
    pub(crate) fn is_full(&self) -> bool {
        self.live.available_permits() == 0
            && self.connections.load().iter().all(|conn| !conn.has_room())
    }
    pub async fn acquire<F, Fut, E>(
        self: &Arc<Self>,
        acquire_timeout: Option<Duration>,
        connect: F,
    ) -> Result<H2AcquiredStream, H2AcquireError<E>>
    where
        F: Fn(u64, PoolGeneration) -> Fut,
        Fut: Future<Output = Result<H2Connection, E>>,
    {
        let mut remaining = acquire_timeout;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.closed.load(Ordering::Acquire) {
                return Err(H2AcquireError::Pool(RequestFailure::Closed));
            }
            for connection in self.connections.load().iter() {
                if let Some(permit) = connection.try_acquire() {
                    let traffic = crate::proxy::balance::least_connections::Traffic::acquire(
                        &self.slot,
                    ).map_err(H2AcquireError::Pool)?;
                    return Ok(H2AcquiredStream {
                        sender: connection.sender(),
                        permit: permit.with_traffic(traffic),
                    });
                }
            }
            if self.ensure_connection(&connect, true).await?.is_some() {
                continue;
            }
            let started = tokio::time::Instant::now();
            crate::proxy::body::timeout::within(remaining, changed)
                .await
                .map_err(|_| H2AcquireError::AcquireTimeout)?;
            remaining = remaining
                .map(|budget| budget.saturating_sub(started.elapsed()));
        }
    }
    /// Warmup reuses any connection. Demand reuses available streams first,
    /// then opens one connection within the same physical capacity budget.
    pub(super) async fn ensure_connection<F, Fut, E>(
        self: &Arc<Self>,
        connect: &F,
        demand: bool,
    ) -> Result<Option<Arc<H2Connection>>, H2AcquireError<E>>
    where
        F: Fn(u64, PoolGeneration) -> Fut,
        Fut: Future<Output = Result<H2Connection, E>>,
    {
        let Ok(guard) = self.connecting.try_lock() else {
            return Ok(None);
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(H2AcquireError::Pool(RequestFailure::Closed));
        }
        {
            let current = self.connections.load();
            if let Some(connection) =
                current.iter().find(|conn| !demand || conn.has_room())
            {
                return Ok(Some(connection.clone()));
            }
            // Let the new connection publish protocol capacity before deciding
            // that another physical connection is needed.
            if current.iter().any(|conn| conn.capacity.pending_peer()) {
                return Ok(None);
            }
        }
        let live = match self.live.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(H2AcquireError::Pool(RequestFailure::Closed));
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => return Ok(None),
        };
        let mut connecting =
            Connecting { pool: self, _guard: Some(guard), live: Some(live) };
        let budget = self
            .breaker
            .as_ref()
            .map(|state| state.connection())
            .transpose()
            .map_err(H2AcquireError::Pool)?;
        let id = self.next_conn_id.fetch_add(1, Ordering::Relaxed);
        let mut connection = connect(id, self.generation)
            .await
            .map_err(H2AcquireError::Connect)?;
        connection.breaker = budget;
        connection.live = connecting.live.take();
        let connection = Arc::new(connection);
        {
            let _mutation =
                self.mutation.lock().unwrap_or_else(|p| p.into_inner());
            if self.closed.load(Ordering::Acquire) {
                return Err(H2AcquireError::Pool(RequestFailure::Closed));
            }
            let mut next = self.connections.load_full().as_ref().clone();
            next.push(connection.clone());
            self.connections.store(Arc::new(next));
        }
        Ok(Some(connection))
    }
    pub(super) fn needs_warmup(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && self.connections.load().is_empty()
    }
    pub async fn clear_connection(
        &self,
        id: u64,
    ) {
        let _mutation = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        let current = self.connections.load();
        if !current.iter().any(|conn| conn.id == id) {
            return;
        }
        let next = current
            .iter()
            .filter(|conn| {
                if conn.id == id {
                    conn.capacity.seal();
                    false
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        self.connections.store(Arc::new(next));
        self.changed.notify_waiters();
    }
    pub async fn clear_connections(&self) {
        let _mutation = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        for connection in self.connections.load().iter() {
            connection.capacity.seal();
        }
        self.connections.store(Arc::new(Vec::new()));
        self.changed.notify_waiters();
    }
    pub(super) fn trim_idle(&self) {
        let _mutation = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        let current = self.connections.load();
        let max_idle = self.profile.capacity.max_idle();
        if current.iter().filter(|conn| conn.in_flight() == 0).count()
            <= max_idle
        {
            return;
        }
        let mut retained = 0;
        let next = current
            .iter()
            .filter(|conn| {
                if conn.in_flight() != 0 {
                    return true;
                }
                if retained < max_idle {
                    retained += 1;
                    true
                } else {
                    !conn.capacity.seal_if_idle()
                }
            })
            .cloned()
            .collect();
        self.connections.store(Arc::new(next));
    }
    /// Sealing rejects new work; accepted stream leases retain their connection.
    pub(crate) fn seal(&self) {
        let _mutation = self.mutation.lock().unwrap_or_else(|p| p.into_inner());
        self.closed.store(true, Ordering::Release);
        self.live.close();
        for connection in self.connections.load().iter() {
            connection.capacity.seal();
        }
        self.changed.notify_waiters();
    }
    pub(crate) fn close(&self) {
        self.seal();
        self.connections.store(Arc::new(Vec::new()));
    }
}

use std::future::poll_fn;
use std::task::Context as TaskContext;

use crate::health::probe::OriginReachabilityFailure;
use crate::origin::Origin;
use crate::proxy::connect::AcquireConnectionError;
use crate::proxy::service::UpstreamService;

pub(crate) struct ReadyH2Stream {
    pub(crate) sender: SendRequest<H2Body>,
    pub(crate) permit: H2Permit,
}

impl UpstreamService {
    pub(crate) async fn acquire_ready_h2_stream(
        &self,
        origin_index: usize,
        origin: &Origin,
        pool: Arc<H2TargetPool>,
        epoch: &Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
        wait: Option<Duration>,
    ) -> Result<ReadyH2Stream, AcquireConnectionError> {
        let mut retried_after_stale_connection = false;
        loop {
            let acquired = self
                .acquire_h2_stream(
                    origin_index,
                    origin,
                    pool.clone(),
                    epoch,
                    wait,
                )
                .await?;
            let mut sender = acquired.sender;
            let permit = acquired.permit;
            match self
                .wait_h2_sender_ready(&origin.source, &mut sender, wait)
                .await
            {
                Ok(()) => return Ok(ReadyH2Stream { sender, permit }),
                Err(err) => {
                    let conn_id = permit.conn_id;
                    drop(permit);
                    pool.clear_connection(conn_id).await;
                    if retried_after_stale_connection {
                        return Err(
                            AcquireConnectionError::OriginReachabilityFailure(
                                err,
                            ),
                        );
                    }
                    tracing::debug!(
                        profile = %self.inner.profile.name,
                        origin = origin.label.as_ref(),
                        cause = err.cause(),
                        "upstream_h2_stale_connection"
                    );
                    retried_after_stale_connection = true;
                }
            }
        }
    }

    async fn wait_h2_sender_ready(
        &self,
        source: &str,
        sender: &mut SendRequest<H2Body>,
        wait: Option<Duration>,
    ) -> Result<(), OriginReachabilityFailure> {
        // Waiting for a free stream slot is waiting for capacity, so it is
        // bounded by the request's `queue_timeout`, not by its execution.
        match crate::proxy::body::timeout::within(
            wait,
            poll_fn(|cx: &mut TaskContext<'_>| sender.poll_ready(cx)),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err({
                tracing::debug!(%source, error = %err, "upstream h2 sender not ready");
                OriginReachabilityFailure::h2_sender_not_ready()
            }),
            Err(_) => {
                Err(OriginReachabilityFailure::h2_stream_capacity_timeout())
            }
        }
    }
}

impl UpstreamService {
    pub(crate) async fn acquire_h2_stream(
        &self,
        _origin_index: usize,
        origin: &Origin,
        pool: Arc<H2TargetPool>,
        epoch: &Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
        wait: Option<Duration>,
    ) -> Result<crate::proxy::h2::H2AcquiredStream, AcquireConnectionError>
    {
        pool.acquire(wait, |conn_id, generation| {
            debug_assert_eq!(generation, epoch.transport(origin.id).generation);
            self.connect_h2_connection(
                Some(H2DriverEndContext { pool: Arc::downgrade(&pool) }),
                origin,
                conn_id,
                epoch,
            )
        })
        .await
        .map_err(|err| match err {
            H2AcquireError::Connect(err) => err,
            H2AcquireError::AcquireTimeout => {
                AcquireConnectionError::Pool(RequestFailure::AcquireTimeout)
            }
            H2AcquireError::Pool(err) => AcquireConnectionError::Pool(err),
        })
    }
}

use crate::proxy::h2::conn::H2DriverEndContext;

#[cfg(test)]
#[path = "../../../tests/proxy/h2/pool.rs"]
mod tests;
