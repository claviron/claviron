//! Event-driven H1 live floor; one cancellable replenisher per origin/epoch.
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};

use super::{Accounting, PoolAccounting, TargetPool};
use crate::origin::OriginId;
use crate::proxy::h1::conn::H1Connection;
use crate::proxy::service::lifecycle::ServiceEpoch;
use crate::proxy::service::{ProtocolPools, ServiceInner, UpstreamService};

#[derive(Clone)]
pub(crate) struct WarmContext {
    service: Weak<ServiceInner>,
    epoch: Weak<ServiceEpoch>,
    origin: OriginId,
}
impl std::fmt::Debug for WarmContext {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("WarmContext")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}
impl UpstreamService {
    /// Wiring is inert: constructors never require a reactor or start a dial.
    pub(crate) fn attach_warm_pools(&self) {
        if self.inner.profile.capacity.min_idle() == 0 {
            return;
        }
        let epoch = self.epoch();
        if let ProtocolPools::H1(pools) = &epoch.pools {
            for (origin, pool) in
                self.inner.profile.origins.iter().zip(pools.iter())
            {
                pool.lock().unwrap_or_else(|p| p.into_inner()).warm_context =
                    Some(WarmContext {
                        service: Arc::downgrade(&self.inner),
                        epoch: Arc::downgrade(&epoch),
                        origin: origin.id,
                    });
            }
        }
    }
    /// Provider boot/reload and explicit profile start prime serving-worker pools.
    pub(crate) fn warm_pools(&self) {
        if self.inner.profile.capacity.min_idle() == 0 {
            return;
        }
        if let ProtocolPools::H1(pools) = &self.epoch().pools {
            for pool in pools.iter() {
                kick(pool);
            }
        }
    }
    pub(crate) fn warm_origin(
        &self,
        index: usize,
    ) {
        if self.inner.profile.capacity.min_idle() == 0 {
            return;
        }
        if let ProtocolPools::H1(pools) = &self.epoch().pools
            && let Some(pool) = pools.get(index)
        {
            kick(pool);
        }
    }
}
struct Replenishing(Arc<Mutex<TargetPool>>);
impl Drop for Replenishing {
    fn drop(&mut self) {
        let pool = self.0.lock().unwrap_or_else(|p| p.into_inner());
        pool.replenishing.store(false, Ordering::Release);
        pool.changed.notify_waiters();
    }
}
pub(crate) fn kick(pool: &Arc<Mutex<TargetPool>>) {
    let context = {
        let guard = pool.lock().unwrap_or_else(|p| p.into_inner());
        if !guard.needs_replenish() {
            return;
        }
        guard.warm_context.clone()
    };
    let Some(context) = context else {
        return;
    };
    let (Some(inner), Some(epoch)) =
        (context.service.upgrade(), context.epoch.upgrade())
    else {
        return;
    };
    let service = UpstreamService { inner };
    if !service.inner.runtime.serving_role
        || epoch.background.is_cancelled()
        || service.inner.runtime.shutdown.is_cancelled()
        || !Arc::ptr_eq(&epoch, &service.epoch())
        || !service.origin_eligible(context.origin.index())
        || tokio::runtime::Handle::try_current().is_err()
    {
        return;
    }
    {
        let guard = pool.lock().unwrap_or_else(|p| p.into_inner());
        if !guard.needs_replenish()
            || guard
                .replenishing
                .compare_exchange(
                    false,
                    true,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return;
        }
    }
    let owned = Replenishing(pool.clone());
    let cancelled = epoch.background.clone();
    let shutdown = service.inner.runtime.shutdown.clone();
    let captured = epoch.clone();
    service.spawn_background_for(&epoch, async move {
        let completed = tokio::select! {
            biased;
            _ = cancelled.cancelled() => false,
            _ = shutdown.cancelled() => false,
            _ = replenish(&context, &captured, &owned.0) => true,
        };
        let pool = owned.0.clone();
        drop(owned);
        // Release the single-flight flag before rechecking a concurrent deficit.
        // Cancellation never revives maintenance during stop.
        if completed {
            kick(&pool);
        }
    });
}
async fn replenish(
    context: &WarmContext,
    epoch: &Arc<ServiceEpoch>,
    pool: &Arc<Mutex<TargetPool>>,
) {
    let policy = {
        let guard = pool.lock().unwrap_or_else(|p| p.into_inner());
        let crate::compile::profile::Capacity::H1 { replenish, .. } =
            &guard.profile.capacity
        else {
            return;
        };
        replenish.clone()
    };
    let mut backoff = policy.initial_backoff;
    loop {
        let Some(inner) = context.service.upgrade() else {
            return;
        };
        let service = UpstreamService { inner };
        let origin = &service.inner.profile.origins[context.origin.index()];
        if !service.origin_eligible(context.origin.index()) {
            return;
        }
        let reservation = match PoolAccounting::for_warm(pool.clone()) {
            Ok(Some(reservation)) => reservation,
            Ok(None) => return,
            Err(crate::proxy::failure::RequestFailure::BreakerOpen) => {
                let breaker = epoch
                    .breaker
                    .as_ref()
                    .expect("configured connection breaker")
                    .clone();
                drop(service);
                breaker.connection_available().await;
                continue;
            }
            Err(_) => return,
        };
        let result = service.connect_h1_connection(None, origin, epoch).await;
        match result {
            Ok(connection) => {
                reservation.finish_warm(
                    connection,
                    service.origin_eligible(context.origin.index()),
                );
                backoff = policy.initial_backoff;
            }
            Err(error) => {
                tracing::debug!(source = %origin.source, ?error, "upstream warm connection failed");
                drop(reservation);
                drop(service);
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(policy.max_backoff);
            }
        }
    }
}
impl PoolAccounting {
    fn for_warm(
        pool: Arc<Mutex<TargetPool>>
    ) -> Result<Option<Self>, crate::proxy::failure::RequestFailure> {
        let connection = {
            let mut guard = pool.lock().unwrap_or_else(|p| p.into_inner());
            if !guard.needs_replenish() {
                return Ok(None);
            }
            let connection = guard
                .breaker
                .as_ref()
                .map(|state| state.connection())
                .transpose()?;
            // Warm reservations consume live capacity without adding traffic.
            guard.connecting += 1;
            guard.warming += 1;
            connection
        };
        Ok(Some(Self {
            pool,
            state: Accounting::Connecting,
            traffic: None,
            warm: true,
            connection,
        }))
    }
    fn finish_warm(
        mut self,
        mut connection: H1Connection,
        selectable: bool,
    ) {
        connection.breaker = self.connection.take();
        let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        pool.connecting -= 1;
        pool.warming -= 1;
        if selectable
            && !pool.closed
            && connection.generation == pool.generation
            && pool.idle.len() < pool.profile.capacity.max_idle()
        {
            connection.mark_idle();
            pool.idle.push(connection);
        }
        self.state = Accounting::Released;
        // Store a permit too if finish races an acquire waiter registration.
        pool.changed.notify_one();
        pool.changed.notify_waiters();
    }
}
