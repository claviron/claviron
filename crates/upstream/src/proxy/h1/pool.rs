//! Exclusive leases and cancellation-safe live = idle + leased + connecting.
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

pub(crate) mod warm;

use tokio::sync::Notify;
use tokio::time;

use super::conn::H1Connection;
use super::lease::H1Lease;
use crate::compile::profile::Profile;
use crate::origin::{Origin, PoolGeneration};
use crate::proxy::connect::AcquireConnectionError;
use crate::proxy::failure::{DirtyReason, RequestFailure};
use crate::proxy::service::UpstreamService;

#[derive(Debug)]
pub struct TargetPool {
    profile: Arc<Profile>,
    pub(crate) breaker: Option<Arc<crate::proxy::service::breaker::State>>,
    generation: PoolGeneration,
    idle: Vec<H1Connection>,
    leased: usize,
    connecting: usize,
    warming: usize,
    replenishing: AtomicBool,
    warm_context: Option<warm::WarmContext>,
    closed: bool,
    changed: Arc<Notify>,
}
enum Take {
    Idle(H1Connection),
    Connect(Option<crate::proxy::service::breaker::Connection>),
    Wait,
}
impl TargetPool {
    pub(crate) fn new(
        profile: Arc<Profile>,
        generation: PoolGeneration,
    ) -> Self {
        Self {
            profile,
            breaker: None,
            generation,
            idle: Vec::new(),
            leased: 0,
            connecting: 0,
            warming: 0,
            replenishing: AtomicBool::new(false),
            warm_context: None,
            closed: false,
            changed: Arc::new(Notify::new()),
        }
    }
    pub fn live(&self) -> usize {
        self.idle.len() + self.leased + self.connecting
    }
    pub(crate) fn in_flight(&self) -> usize {
        self.leased + self.connecting - self.warming
    }
    fn needs_replenish(&self) -> bool {
        !self.closed && self.live() < self.profile.capacity.min_idle()
    }
    pub fn can_reuse(&self) -> bool {
        self.profile.capacity.max_idle() > 0
    }
    pub fn purge_idle(
        &mut self,
        _reason: DirtyReason,
    ) -> usize {
        let dropped = self.idle.len();
        self.idle.clear();
        self.changed.notify_one();
        dropped
    }
    fn take(&mut self) -> Result<Take, RequestFailure> {
        if self.closed {
            return Err(RequestFailure::Closed);
        }
        while let Some(mut connection) = self.idle.pop() {
            if connection.generation != self.generation
                || (self.live() >= self.profile.capacity.min_idle()
                    && connection.idle_for_at_least(
                        self.profile.connection.idle_timeout,
                    ))
            {
                continue;
            }
            connection.clear_idle();
            self.leased += 1;
            if !self.idle.is_empty()
                || self.live() < self.profile.capacity.max_live()
            {
                self.changed.notify_one();
            }
            return Ok(Take::Idle(connection));
        }
        if self.live() >= self.profile.capacity.max_live() {
            return Ok(Take::Wait);
        }
        let connection = self
            .breaker
            .as_ref()
            .map(|state| state.connection())
            .transpose()?;
        self.connecting += 1;
        if self.live() < self.profile.capacity.max_live() {
            self.changed.notify_one();
        }
        Ok(Take::Connect(connection))
    }
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.idle.clear();
        self.changed.notify_waiters();
    }
}

#[derive(Debug)]
enum Accounting {
    Connecting,
    Leased,
    Released,
}
/// Giving a request/reservation back is synchronous even when its future is cancelled.
#[derive(Debug)]
pub(crate) struct PoolAccounting {
    pool: Arc<Mutex<TargetPool>>,
    state: Accounting,
    traffic: Option<crate::proxy::balance::least_connections::Traffic>,
    warm: bool,
    connection: Option<crate::proxy::service::breaker::Connection>,
}
impl PoolAccounting {
    fn for_request(
        pool: Arc<Mutex<TargetPool>>,
        state: Accounting,
        origin: &Origin,
    ) -> Result<Self, RequestFailure> {
        let mut owned =
            Self { pool, state, traffic: None, warm: false, connection: None };
        owned.traffic =
            Some(crate::proxy::balance::least_connections::Traffic::acquire(
                origin.slot.get(),
            )?);
        Ok(owned)
    }

    fn finish_connect(
        mut self,
        mut connection: H1Connection,
        can_reuse: bool,
    ) -> H1PooledConnection {
        {
            let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
            debug_assert!(pool.connecting > 0);
            pool.connecting -= 1;
            pool.leased += 1;
        }
        connection.breaker = self.connection.take();
        self.state = Accounting::Leased;
        H1PooledConnection {
            connection,
            lease: H1Lease::new(),
            can_reuse,
            accounting: self,
        }
    }
    pub(crate) fn return_idle(
        mut self,
        mut connection: H1Connection,
        lease: &H1Lease,
    ) {
        debug_assert!(lease.is_reusable());
        let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        debug_assert!(pool.leased > 0);
        pool.leased -= 1;
        if !pool.closed
            && connection.generation == pool.generation
            && pool.idle.len() < pool.profile.capacity.max_idle()
        {
            connection.mark_idle();
            pool.idle.push(connection);
        }
        self.state = Accounting::Released;
        pool.changed.notify_one();
        let replenish = pool.needs_replenish();
        drop(pool);
        self.traffic.take();
        if replenish {
            warm::kick(&self.pool);
        }
    }
    pub(crate) fn release(mut self) {
        self.release_inner();
    }
    fn release_inner(&mut self) {
        let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        match self.state {
            Accounting::Connecting => {
                debug_assert!(pool.connecting > 0);
                pool.connecting -= 1;
                if self.warm {
                    pool.warming -= 1;
                }
            }
            Accounting::Leased => {
                debug_assert!(pool.leased > 0);
                pool.leased -= 1;
            }
            Accounting::Released => return,
        }
        self.state = Accounting::Released;
        pool.changed.notify_one();
        let replenish = pool.needs_replenish();
        drop(pool);
        self.traffic.take();
        if replenish {
            warm::kick(&self.pool);
        }
    }
}
impl Drop for PoolAccounting {
    fn drop(&mut self) {
        if !matches!(self.state, Accounting::Released) {
            self.release_inner();
        }
    }
}
#[derive(Debug)]
pub struct H1PooledConnection {
    pub connection: H1Connection,
    pub lease: H1Lease,
    pub(crate) can_reuse: bool,
    pub(crate) accounting: PoolAccounting,
}
impl UpstreamService {
    pub(crate) async fn acquire_connection(
        &self,
        origin_index: usize,
        origin: &Origin,
        pool: Arc<Mutex<TargetPool>>,
        epoch: &Arc<crate::proxy::service::lifecycle::ServiceEpoch>,
        wait: Option<std::time::Duration>,
    ) -> Result<H1PooledConnection, AcquireConnectionError> {
        // The request's `queue_timeout`: how long it waits for a connection.
        // Unset waits until one frees.
        let deadline = wait.map(|wait| time::Instant::now() + wait);
        loop {
            let (take, can_reuse, changed) = {
                let mut guard = pool.lock().unwrap_or_else(|p| p.into_inner());
                let take =
                    guard.take().map_err(AcquireConnectionError::Pool)?;
                (take, guard.can_reuse(), guard.changed.clone())
            };
            if self.inner.profile.capacity.min_idle() > 0 {
                warm::kick(&pool);
            }
            match take {
                Take::Idle(connection) => {
                    let pooled = H1PooledConnection {
                        connection,
                        lease: H1Lease::new(),
                        can_reuse,
                        accounting: PoolAccounting::for_request(
                            pool.clone(),
                            Accounting::Leased,
                            origin,
                        )
                        .map_err(AcquireConnectionError::Pool)?,
                    };
                    if let Some(pooled) = self.preflight_idle_connection(pooled)
                    {
                        return Ok(pooled);
                    }
                }
                Take::Connect(connection_budget) => {
                    let mut reserved = PoolAccounting::for_request(
                        pool,
                        Accounting::Connecting,
                        origin,
                    )
                    .map_err(AcquireConnectionError::Pool)?;
                    reserved.connection = connection_budget;
                    let connection = self
                        .connect_h1_connection(
                            Some(origin_index),
                            origin,
                            epoch,
                        )
                        .await?;
                    return Ok(reserved.finish_connect(connection, can_reuse));
                }
                Take::Wait => {
                    // notify_one stores a permit if release races waiter registration.
                    crate::proxy::body::timeout::within(
                        deadline.map(|deadline| {
                            deadline
                                .saturating_duration_since(time::Instant::now())
                        }),
                        changed.notified(),
                    )
                    .await
                    .map_err(|_| {
                        AcquireConnectionError::Pool(
                            RequestFailure::AcquireTimeout,
                        )
                    })?;
                }
            }
        }
    }
}
pub(crate) fn close_pooled_connection(
    _pool: Arc<Mutex<TargetPool>>,
    pooled: H1PooledConnection,
) {
    drop(pooled);
}
pub(crate) fn detach_pooled_upgrade(
    _pool: Arc<Mutex<TargetPool>>,
    mut pooled: H1PooledConnection,
) -> H1Connection {
    pooled.lease = pooled.lease.into_upgrade();
    pooled.accounting.release();
    pooled.connection
}
