//! Owns one H1 sender and driver, preflight and protocol handshake.

use std::fmt;
use std::time::{Duration, Instant};

use hyper::client::conn::http1::SendRequest;
use tokio::task::JoinHandle;

use crate::origin::{OriginId, PoolGeneration};
use crate::proxy::body::UpstreamBody;

pub struct H1Connection {
    pub origin: OriginId,
    pub generation: PoolGeneration,
    pub requests_served: u64,
    idle_since: Option<Instant>,
    sender: SendRequest<UpstreamBody>,
    driver: Option<JoinHandle<()>>,
    pub(crate) received: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    pub(crate) breaker: Option<crate::proxy::service::breaker::Connection>,
}

impl fmt::Debug for H1Connection {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("H1Connection")
            .field("origin", &self.origin)
            .field("generation", &self.generation)
            .field("requests_served", &self.requests_served)
            .finish_non_exhaustive()
    }
}

impl H1Connection {
    pub fn new(
        origin: OriginId,
        generation: PoolGeneration,
        sender: SendRequest<UpstreamBody>,
        driver: JoinHandle<()>,
    ) -> Self {
        Self {
            origin,
            generation,
            requests_served: 0,
            idle_since: None,
            sender,
            driver: Some(driver),
            received: None,
            breaker: None,
        }
    }

    /// Without stream-scoped Hyper evidence, previously observed peer bytes
    /// remain a conservative veto for the entire connection lifetime.
    pub(crate) async fn close_and_join(&mut self) {
        if let Some(driver) = &mut self.driver {
            driver.abort();
            let _ = driver.await;
        }
        self.driver.take();
    }
    pub(crate) fn no_response_byte(&self) -> bool {
        self.received.as_ref().is_some_and(|received| {
            !received.load(std::sync::atomic::Ordering::Acquire)
        })
    }
    pub fn sender_mut(&mut self) -> &mut SendRequest<UpstreamBody> {
        &mut self.sender
    }

    pub fn mark_request_served(&mut self) {
        self.requests_served = self.requests_served.saturating_add(1);
    }

    pub fn mark_idle(&mut self) {
        self.idle_since = Some(Instant::now());
    }

    pub fn clear_idle(&mut self) {
        self.idle_since = None;
    }

    pub fn idle_for_at_least(
        &self,
        timeout: Duration,
    ) -> bool {
        self.idle_since.is_some_and(|since| since.elapsed() >= timeout)
    }

    /// After a backend 101 the HTTP driver must finish before IO is handed off.
    /// Borrow the handle while joining so cancellation still aborts it on Drop.
    pub(crate) async fn finish_upgrade(
        &mut self
    ) -> Result<(), tokio::task::JoinError> {
        if let Some(driver) = self.driver.as_mut() {
            driver.await?;
        }
        self.driver.take();
        Ok(())
    }
}

impl Drop for H1Connection {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
    }
}

use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::health::probe::OriginReachabilityFailure;
use crate::origin::Origin;
use crate::proxy::h1::pool::H1PooledConnection;
use crate::proxy::service::UpstreamService;

#[derive(Clone)]
pub(crate) struct H1DriverEndContext;

impl UpstreamService {
    pub(crate) fn preflight_idle_connection(
        &self,
        mut pooled: H1PooledConnection,
    ) -> Option<H1PooledConnection> {
        let sender = pooled.connection.sender_mut();
        let stale_reason = if sender.is_closed() {
            Some("idle h1 sender is closed")
        } else if !sender.is_ready() {
            Some("idle h1 sender is not ready")
        } else {
            None
        };

        if let Some(_reason) = stale_reason {
            drop(pooled);
            return None;
        }

        Some(pooled)
    }
}

pub(crate) async fn handshake_connection<I>(
    io: TokioIo<I>,
    origin: &Origin,
    driver_end: Option<H1DriverEndContext>,
    epoch: &crate::proxy::service::lifecycle::ServiceEpoch,
) -> Result<H1Connection, OriginReachabilityFailure>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let transport = epoch.transport(origin.id);
    let generation = transport.generation;
    let force = transport.force.clone();
    let tasks = transport.io.clone();
    let owner = transport.clone();
    let received = (epoch.retry.is_some() && driver_end.is_some()).then(|| {
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false))
    });
    let io = TokioIo::new(crate::proxy::connect::io::Observed {
        inner: io.into_inner(),
        received: received.clone(),
    });
    let (sender, conn) = http1::handshake(io).await.map_err(|err| {
        tracing::debug!(source = %origin.source, error = %err, "upstream h1 handshake failed");
        OriginReachabilityFailure::handshake(crate::config::profile::UpstreamProtocol::H1)
    })?;
    let driver = tasks.spawn(async move {
        let result = tokio::select! { biased; _ = force.cancelled() => return, result = conn.with_upgrades() => result };
        if result.is_err()
            && driver_end.is_some()
            && let Some((service, epoch, origin_index)) = owner.projection()
        {
            tracing::debug!(
                origin = service.inner.profile.origins[origin_index].label.as_ref(),
                cause = "driver_error",
                "upstream h1 connection ended"
            );
            service.spawn_h1_driver_error_probe(origin_index, epoch);
        }
    });

    let mut connection =
        H1Connection::new(origin.id, generation, sender, driver);
    connection.received = received;
    Ok(connection)
}
