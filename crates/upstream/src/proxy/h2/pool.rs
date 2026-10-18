//! Stream admission with lock-free session reads and single-flight connect.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use hyper::client::conn::http2::SendRequest;
use tokio::sync::{Mutex, Semaphore};

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
    streams: Arc<Semaphore>,
    next_conn_id: AtomicU64,
    connection: ArcSwapOption<H2Connection>,
    connecting: Mutex<()>,
    pub(crate) peer_max_streams: AtomicUsize,
    admitted: AtomicUsize,
    closed: AtomicBool,
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
impl H2TargetPool {
    pub(crate) fn new(
        profile: Arc<Profile>,
        generation: PoolGeneration,
        origin: crate::origin::OriginId,
    ) -> Self {
        let configured = profile.capacity.max_streams();
        Self {
            breaker: None,
            slot: profile.origins[origin.index()].slot.get().clone(),
            streams: Arc::new(Semaphore::new(configured)),
            profile,
            generation,
            next_conn_id: AtomicU64::new(1),
            connection: ArcSwapOption::empty(),
            connecting: Mutex::new(()),
            peer_max_streams: AtomicUsize::new(usize::MAX),
            admitted: AtomicUsize::new(configured),
            closed: AtomicBool::new(false),
        }
    }
    pub(crate) fn in_flight(&self) -> usize {
        self.admitted
            .load(Ordering::Acquire)
            .saturating_sub(self.streams.available_permits())
    }
    pub(crate) fn apply_peer_limit(
        &self,
        peer: usize,
    ) {
        reconcile_permits(
            &self.streams,
            &self.admitted,
            self.profile.capacity.max_streams(),
            peer,
        );
    }
    pub async fn acquire<F, Fut, E>(
        &self,
        acquire_timeout: Option<Duration>,
        connect: F,
    ) -> Result<H2AcquiredStream, H2AcquireError<E>>
    where
        F: FnOnce(u64, PoolGeneration) -> Fut,
        Fut: Future<Output = Result<H2Connection, E>>,
    {
        let stream = crate::proxy::body::timeout::within(
            acquire_timeout,
            self.streams.clone().acquire_owned(),
        )
        .await
        .map_err(|_| H2AcquireError::AcquireTimeout)?
        .map_err(|_| H2AcquireError::Pool(RequestFailure::Closed))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(H2AcquireError::Pool(RequestFailure::Closed));
        }
        let traffic =
            crate::proxy::balance::least_connections::Traffic::acquire(
                &self.slot,
            )
            .map_err(H2AcquireError::Pool)?;
        let connection = if let Some(connection) = self.connection.load_full() {
            connection
        } else {
            let _connecting = self.connecting.lock().await;
            if let Some(connection) = self.connection.load_full() {
                connection
            } else {
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
                let connection = Arc::new(connection);
                if self.closed.load(Ordering::Acquire) {
                    return Err(H2AcquireError::Pool(RequestFailure::Closed));
                }
                self.connection.store(Some(connection.clone()));
                // Close can race the check above and publication. Recheck after
                // publishing; a later close itself clears the published slot.
                if self.closed.load(Ordering::Acquire) {
                    self.clear_connection(id).await;
                    return Err(H2AcquireError::Pool(RequestFailure::Closed));
                }
                connection
            }
        };
        debug_assert!(self.profile.capacity.max_streams() > 0);
        Ok(H2AcquiredStream {
            sender: connection.sender(),
            permit: connection.permit(stream).with_traffic(traffic),
        })
    }
    pub async fn clear_connection(
        &self,
        id: u64,
    ) {
        let current = self.connection.load_full();
        if current.as_ref().is_some_and(|connection| connection.id == id) {
            // A late driver end must never clear a replacement connection.
            self.connection.compare_and_swap(&current, None);
        }
    }
    pub async fn clear_current_connection(&self) {
        // Serialize cleanup with publication; a connecting session cannot escape Down.
        let _connecting = self.connecting.lock().await;
        self.connection.store(None);
    }
    /// Stop stream admission while accepted streams retain the shared session.
    pub(crate) fn seal(&self) {
        self.closed.store(true, Ordering::Release);
        self.streams.close();
    }
    pub(crate) fn close(&self) {
        self.seal();
        self.connection.store(None);
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

fn reconcile_permits(
    streams: &Semaphore,
    admitted: &AtomicUsize,
    configured: usize,
    peer: usize,
) {
    let target = configured.min(peer);
    let current = admitted.load(Ordering::Acquire);
    if target == current {
        return;
    }
    if target < current {
        let removed = streams.forget_permits(current - target);
        admitted.fetch_sub(removed, Ordering::AcqRel);
    } else {
        streams.add_permits(target - current);
        admitted.store(target, Ordering::Release);
    }
}
