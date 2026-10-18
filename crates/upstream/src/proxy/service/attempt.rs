//! Acquires an origin before request bytes are sent. This is not replay.

use std::sync::{Arc, Mutex};

use super::UpstreamService;
use crate::origin::OriginId;
use crate::proxy::connect::AcquireConnectionError;
use crate::proxy::failure::RequestFailure;
use crate::proxy::h1::pool::{H1PooledConnection, TargetPool};
use crate::proxy::h2::pool::{H2TargetPool, ReadyH2Stream};

pub(crate) trait Leg {
    type Acquired;
    fn acquire(
        &self,
        service: &UpstreamService,
        index: usize,
        epoch: &Arc<super::lifecycle::ServiceEpoch>,
        wait: Option<std::time::Duration>,
    ) -> impl Future<Output = Result<Self::Acquired, AcquireConnectionError>> + Send;
}
impl Leg for [Arc<Mutex<TargetPool>>] {
    type Acquired = (Arc<Mutex<TargetPool>>, H1PooledConnection);
    async fn acquire(
        &self,
        service: &UpstreamService,
        index: usize,
        epoch: &Arc<super::lifecycle::ServiceEpoch>,
        wait: Option<std::time::Duration>,
    ) -> Result<Self::Acquired, AcquireConnectionError> {
        // Share the selected pool with the lease and its response owner.
        let pool = self[index].clone();
        // Acquisition retains the same pool while dialing.
        let pooled = service
            .acquire_connection(
                index,
                &service.inner.profile.origins[index],
                pool.clone(),
                epoch,
                wait,
            )
            .await?;
        Ok((pool, pooled))
    }
}
impl Leg for [Arc<H2TargetPool>] {
    type Acquired = (Arc<H2TargetPool>, ReadyH2Stream);
    async fn acquire(
        &self,
        service: &UpstreamService,
        index: usize,
        epoch: &Arc<super::lifecycle::ServiceEpoch>,
        wait: Option<std::time::Duration>,
    ) -> Result<Self::Acquired, AcquireConnectionError> {
        // Share the selected multiplexed pool with this stream owner.
        let pool = self[index].clone();
        // Acquisition retains the same pool through readiness checking.
        let acquired = service
            .acquire_ready_h2_stream(
                index,
                &service.inner.profile.origins[index],
                pool.clone(),
                epoch,
                wait,
            )
            .await?;
        Ok((pool, acquired))
    }
}

pub(crate) async fn run<L: Leg + ?Sized>(
    service: &UpstreamService,
    leg: &L,
    epoch: &Arc<super::lifecycle::ServiceEpoch>,
    request: crate::proxy::balance::RequestKeyView<'_>,
    mut pending: Option<super::breaker::Admission>,
    request_body: bool,
    wait: Option<std::time::Duration>,
) -> Result<
    (
        OriginId,
        L::Acquired,
        tokio::time::Instant,
        Option<Arc<super::breaker::Admission>>,
        super::transport::OriginAdmission,
    ),
    RequestFailure,
> {
    let mut last_error = RequestFailure::NoHealthyOrigin;
    use crate::proxy::balance::Balancer;
    for id in service.inner.balancer.order(
        &request,
        crate::proxy::balance::Health { service, pools: &epoch.pools },
    ) {
        let index = id.index();
        let mut admission = match pending
            .take()
            .map(|admission| Ok(Some(admission)))
            .unwrap_or_else(|| {
                epoch
                    .breaker
                    .as_ref()
                    .map(|state| state.admit(false))
                    .transpose()
            }) {
            Ok(admission) => admission,
            Err(error) => {
                service.record_breaker_rejection(index);
                return Err(error);
            }
        };
        let origin_admission = match epoch.transports[index].admit(request_body)
        {
            Ok(admission) => admission,
            Err(error) => {
                last_error = error;
                continue;
            }
        };
        let started = tokio::time::Instant::now();
        match leg.acquire(service, index, epoch, wait).await {
            Ok(acquired) => {
                service.record_origin_success(index);
                if let Some(admission) = &mut admission {
                    admission.ready();
                }
                return Ok((
                    service.inner.profile.origins[index].id,
                    acquired,
                    started,
                    admission.map(Arc::new),
                    origin_admission,
                ));
            }
            Err(AcquireConnectionError::OriginReachabilityFailure(error)) => {
                service.record_origin_reachability_failure(index);
                last_error = error.request_failure();
            }
            Err(AcquireConnectionError::Pool(error)) => {
                if error == RequestFailure::BreakerOpen {
                    service.record_breaker_rejection(index);
                }
                last_error = error;
            }
        }
    }
    Err(last_error)
}

pub(crate) fn observe(
    service: &UpstreamService,
    origin: OriginId,
    started: tokio::time::Instant,
) {
    use crate::proxy::balance::Balancer;
    service.inner.balancer.observe(origin, started.elapsed());
}
