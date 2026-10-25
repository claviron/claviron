//! Reliable WebTransport message streams, each on its own bounded Link slot.
//! Exact native bindings keep priority; the selected Orbit application owns
//! acceptance and the message protocol, not the Edge's broadcast service.

use std::sync::Arc;

use async_trait::async_trait;
use claviron_http::config::routes::OrbitRouteConfig;
use claviron_orbit::transport::client::{LentRead, LentWrite};
use claviron_orbit::transport::{
    Exchange, HttpVersion, TransportService, TransportTarget,
};
use claviron_runtime::{AppState, registry};
use claviron_types::Host;
use http::StatusCode;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::HostConfig;
use crate::pipeline::{
    AnyAuthenticated, AuthContext, H3WebTransportSession, WebTransportAccept,
    WebTransportHandler, WebTransportRegistry, WebTransportReject,
    WebTransportResult,
};

pub(in crate::pipeline) fn bind(
    state: &AppState,
    host: &Host,
    config: &HostConfig,
) -> registry::Result<()> {
    if config.orbit().is_empty()
        && config.routes.iter().all(|(_, route)| route.orbit().is_empty())
    {
        return Ok(());
    }
    let registry =
        state.registry_ref().resolve::<WebTransportRegistry>().ok_or_else(
            || registry::Error::msg("Orbit requires the WebTransport registry"),
        )?;
    if state.registry_ref().resolve::<TransportService>().is_none() {
        return Err(registry::Error::msg("Orbit transport is not registered"));
    }
    let routes = config
        .orbit()
        .iter()
        .chain(config.routes.iter().flat_map(|(_, route)| route.orbit()))
        .map(|route| PreparedRoute {
            route: route.clone(),
            target: super::prepare_target(
                state,
                route.fleet_name,
                route.application.clone().into(),
            ),
        })
        .collect();
    registry.bind_fallback(host.clone(), Arc::new(Handler { routes }));
    Ok(())
}

fn route<'a>(
    host: &'a HostConfig,
    path: &str,
) -> Option<&'a OrbitRouteConfig> {
    let selected = host.routes.find(path).map(|route| &**route).unwrap_or(host);
    selected
        .orbit()
        .iter()
        .filter(|route| {
            route
                .path
                .as_ref()
                .is_none_or(|prefix| prefix.has_path_segment(path))
        })
        .max_by_key(|route| {
            route.path.as_ref().map_or(0, |prefix| prefix.as_str().len())
        })
}

struct Handler {
    routes: Vec<PreparedRoute>,
}

struct PreparedRoute {
    route: OrbitRouteConfig,
    target: Option<TransportTarget>,
}

/// Dropping a pending or interrupted session resets only this exchange's
/// input. A lent slot cannot return to the hot cache with its consumer asleep.
struct Channel {
    outgoing: LentWrite,
    incoming: LentRead,
    finished: bool,
}

impl Drop for Channel {
    fn drop(&mut self) {
        if !self.finished {
            self.outgoing.reset();
        }
    }
}

struct Accepted {
    target: TransportTarget,
    exchange: Exchange,
    first: Channel,
}

impl Handler {
    async fn open(
        target: &TransportTarget,
        exchange: Exchange,
        cancellation: impl std::future::Future<Output = ()>,
    ) -> Result<Channel, StatusCode> {
        let pending = target
            .send(exchange, true)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let (outgoing, incoming, reply) = pending.split();
        let channel = Channel { outgoing, incoming, finished: false };
        let head = tokio::select! {
            biased;
            () = cancellation => return Err(StatusCode::SERVICE_UNAVAILABLE),
            result = reply.head() => result.map_err(|_| StatusCode::BAD_GATEWAY)?,
        };
        if head.status != StatusCode::SWITCHING_PROTOCOLS.as_u16() {
            // 101 is Link's message-session acknowledgment, never the H3 wire
            // response. A normal HTTP 2xx cannot authorize a message stream.
            return Err(StatusCode::from_u16(head.status)
                .ok()
                .filter(|status| {
                    status.is_client_error() || status.is_server_error()
                })
                .unwrap_or(StatusCode::BAD_GATEWAY));
        }
        Ok(channel)
    }
}

#[async_trait]
impl WebTransportHandler for Handler {
    fn name(&self) -> &'static str {
        "orbit_webtransport"
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &AuthContext,
    ) -> WebTransportResult {
        if ctx.admission.is_none() {
            return Err(WebTransportReject::new(
                StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        let route = route(&ctx.host_config, &ctx.path)
            .ok_or_else(|| WebTransportReject::new(StatusCode::NOT_FOUND))?;
        let target = self
            .routes
            .iter()
            .find(|prepared| {
                prepared.route.fleet_name == route.fleet_name
                    && prepared.route.application == route.application
            })
            .and_then(|prepared| prepared.target.clone())
            .ok_or_else(|| {
                WebTransportReject::new(StatusCode::SERVICE_UNAVAILABLE)
            })?;
        let exchange = Exchange {
            method: http::Method::CONNECT.to_string(),
            scheme: ctx.conn.scheme().to_string(),
            version: HttpVersion::Http3,
            path: super::percent_decoded(ctx.uri.path()),
            raw_path: ctx.uri.path().as_bytes().to_vec(),
            query: ctx.uri.query().unwrap_or_default().as_bytes().to_vec(),
            headers: super::exchange_headers(&ctx.uri, &ctx.headers),
            client: Some(ctx.conn.remote),
            server: Some(std::net::SocketAddr::new(
                ctx.conn.local_ip,
                ctx.conn.local_port,
            )),
            upgrade: true,
            ..Exchange::default()
        };
        // Reserve the first application session before H3's 200 is sent.
        // Missing applications and refusals remain HTTP statuses, not a
        // successful transport that can never reach its application.
        let first =
            Self::open(&target, exchange.clone(), ctx.wait_graceful_shutdown())
                .await
                .map_err(WebTransportReject::new)?;
        Ok(WebTransportAccept::authenticated(Accepted {
            target,
            exchange,
            first,
        }))
    }

    async fn handle(
        &self,
        _state: AppState,
        ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> registry::Result<()> {
        let Accepted { target, exchange, first } = accepted
            .ok_or_else(|| {
                registry::Error::msg("Orbit WebTransport acceptance missing")
            })?
            .take::<Accepted>()?;
        let mut first = Some(first);
        let cancellation = CancellationToken::new();
        let mut streams = JoinSet::new();
        let shutdown = async {
            tokio::select! {
                () = ctx.wait_graceful_shutdown() => {},
                _ = session.closed() => {},
            }
        };
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                completed = streams.join_next(), if !streams.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::debug!(%error, "Orbit WebTransport stream ended");
                    }
                }
                stream = session.accept_bi(), if streams.len() < session.incoming_capacity().get() => {
                    let Some(stream) = stream else { break };
                    let first = first.take();
                    let cancellation = cancellation.clone();
                    let target = target.clone();
                    let exchange = exchange.clone();
                    streams.spawn(async move {
                        let channel = match first {
                            Some(channel) => Ok(channel),
                            None => Handler::open(&target, exchange, cancellation.cancelled()).await,
                        };
                        match channel {
                            Ok(channel) => relay(stream, channel, &cancellation).await,
                            Err(status) => {
                                tracing::debug!(%status, "Orbit WebTransport stream refused");
                                drop(stream);
                            }
                        }
                    });
                }
            }
        }
        cancellation.cancel();
        // Includes a reserved first slot when the peer never opened a stream.
        drop(first);
        while let Some(result) = streams.join_next().await {
            if let Err(error) = result {
                tracing::debug!(%error, "Orbit WebTransport stream drain failed");
            }
        }
        Ok(())
    }
}

async fn relay<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut channel: Channel,
    cancellation: &CancellationToken,
) {
    let result = transfer(
        stream,
        &mut channel.outgoing,
        &mut channel.incoming,
        cancellation,
    )
    .await;
    if result.is_ok() {
        channel.finished = true;
    }
    if let Err(error) = result {
        tracing::debug!(%error, "Orbit WebTransport stream stopped");
    }
}

async fn transfer<S, W, R>(
    stream: S,
    to_application: &mut W,
    from_application: &mut R,
    cancellation: &CancellationToken,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let (mut incoming, mut outgoing) = tokio::io::split(stream);
    let stop = cancellation.child_token();
    let inbound = async {
        let result = copy(&mut incoming, to_application, &stop).await;
        if result.is_err() {
            stop.cancel();
        }
        result
    };
    let outbound = async {
        let result = copy(from_application, &mut outgoing, &stop).await;
        if result.is_err() {
            stop.cancel();
        }
        result
    };
    let (inbound, outbound) = tokio::join!(inbound, outbound);
    // Preserve the actual IO error rather than the cancellation it signalled
    // to the other direction. Normal FIN half-closes only its own direction.
    match (inbound, outbound) {
        (Err(error), _) if error.kind() != std::io::ErrorKind::Interrupted => {
            Err(error)
        }
        (_, Err(error)) => Err(error),
        (result, Ok(())) => result,
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/orbit/webtransport.rs"]
mod tests;

/// One bounded buffer per direction. A shutdown stops new reads but finishes
/// the chunk already being written. FIN is forwarded only after a real EOF;
/// cancellation/errors reset the Link input through Channel's ownership.
async fn copy<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    cancellation: &CancellationToken,
) -> std::io::Result<()> {
    let mut buffer = vec![0; claviron_orbit::transport::TRANSPORT_BUFFER_BYTES];
    loop {
        let size = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(std::io::ErrorKind::Interrupted.into()),
            size = reader.read(&mut buffer) => size?,
        };
        if size == 0 {
            return writer.shutdown().await;
        }
        writer.write_all(&buffer[..size]).await?;
        writer.flush().await?;
    }
}
