//! Owns one Hyper H2 connection, its sender and driver.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use hyper::client::conn::http2::SendRequest;
use tokio::task::JoinHandle;

use crate::origin::{OriginId, PoolGeneration};
use crate::proxy::body::H2Body;
use crate::proxy::h2::permit::H2Permit;

pub struct H2Connection {
    pub id: u64,
    pub origin: OriginId,
    pub generation: PoolGeneration,
    sender: SendRequest<H2Body>,
    driver: JoinHandle<()>,
    in_flight: Arc<AtomicUsize>,
    pub(crate) breaker: Option<crate::proxy::service::breaker::Connection>,
}

impl fmt::Debug for H2Connection {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        f.debug_struct("H2Connection")
            .field("id", &self.id)
            .field("origin", &self.origin)
            .field("generation", &self.generation)
            .field("in_flight", &self.in_flight())
            .finish_non_exhaustive()
    }
}

impl H2Connection {
    pub fn new(
        id: u64,
        origin: OriginId,
        generation: PoolGeneration,
        sender: SendRequest<H2Body>,
        driver: JoinHandle<()>,
    ) -> Self {
        Self {
            id,
            origin,
            generation,
            sender,
            driver,
            in_flight: Arc::new(AtomicUsize::new(0)),
            breaker: None,
        }
    }

    pub fn sender(&self) -> SendRequest<H2Body> {
        self.sender.clone()
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub(crate) fn permit(
        &self,
        stream: tokio::sync::OwnedSemaphorePermit,
    ) -> H2Permit {
        H2Permit::new(self.id, self.generation, stream, self.in_flight.clone())
    }
}

impl Drop for H2Connection {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

use hyper::client::conn::http2;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::profile::connection::ConnectionConfig;
use crate::health::probe::OriginReachabilityFailure;
use crate::origin::Origin;
use crate::proxy::h2::pool::H2TargetPool;

#[derive(Clone)]
pub(crate) struct H2DriverEndContext {
    pub(crate) pool: std::sync::Weak<H2TargetPool>,
}

pub(crate) async fn handshake_h2_connection<I>(
    io: TokioIo<I>,
    origin: &Origin,
    conn_id: u64,
    epoch: &crate::proxy::service::lifecycle::ServiceEpoch,
    driver_end: Option<H2DriverEndContext>,
    connection: &ConnectionConfig,
) -> Result<H2Connection, OriginReachabilityFailure>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let transport = epoch.transport(origin.id);
    let generation = transport.generation;
    let force = transport.force.clone();
    let tasks = transport.io.clone();
    let owner = transport.clone();
    let mut builder =
        http2::Builder::new(super::executor::OriginExecutor::new(transport));
    builder.timer(TokioTimer::new());
    if let Some(interval) = connection.keep_alive_interval {
        builder
            .keep_alive_interval(interval)
            .keep_alive_timeout(connection.keep_alive_timeout)
            .keep_alive_while_idle(connection.keep_alive_while_idle);
    }

    let (sender, mut conn) = builder.handshake(io).await.map_err(|err| {
        tracing::debug!(source = %origin.source, error = %err, "upstream h2 handshake failed");
        OriginReachabilityFailure::handshake(crate::config::profile::UpstreamProtocol::H2)
    })?;
    let driver = tasks.spawn(async move {
        let drive = std::future::poll_fn(|cx| {
            let result = std::pin::Pin::new(&mut conn).poll(cx);
            if let Some(pool) = driver_end.as_ref().and_then(|ctx| ctx.pool.upgrade()) {
                let peer = conn.current_max_send_streams();
                pool.peer_max_streams.store(peer, std::sync::atomic::Ordering::Relaxed);
                pool.apply_peer_limit(peer);
            }
            result
        });
        let result =
            tokio::select! { biased; _ = force.cancelled() => return, result = drive => result };
        if let Some(ctx) = driver_end
            && let Some((service, epoch, origin_index)) = owner.projection()
            && let Some(pool) = ctx.pool.upgrade()
        {
            tracing::debug!(
                origin = service.inner.profile.origins[origin_index].label.as_ref(),
                status = if result.is_ok() { "clean" } else { "driver_error" },
                "upstream h2 connection ended"
            );
            if result.is_ok() {
                service.spawn_h2_driver_clean_end(origin_index, conn_id, pool, epoch)
            } else {
                service.spawn_h2_driver_error_probe(origin_index, conn_id, pool, epoch)
            }
        }
    });

    Ok(H2Connection::new(conn_id, origin.id, generation, sender, driver))
}
