//! A QUIC listener that gives every request to one handler.

use std::future::Future;
use std::sync::Arc;

use claviron_http::config::transport::EarlyDataPolicy;
use claviron_http::config::{
    ListenerConfig, ListenerSettings, QuicListenerConfig,
};
use claviron_http::ext::{ConnCtx, RequestArrival};
use claviron_http::{OtherHandler, RouteBody, RouteResponse};
use claviron_runtime::registry::{Error, Result};
use http::{Request, StatusCode};
use quinn::crypto::rustls::QuicServerConfig;
use tokio::sync::mpsc;

use super::listener::{Quic, QuicHost, QuicRequests, Refusal, SessionScope};
use super::wt::{Authorized, BiStream, NativeSession, QuicSession};

/// Every connection gets one TLS configuration and every request one handler,
/// whatever server name or authority the client names. No host is selected and
/// nothing is attached to the request.
///
/// A server name selecting a certificate is not a host being selected: the
/// configuration's own resolver may still choose among certificates by name
/// and refuse a name it has none for.
///
/// The listener's request policy is not applied here. 0-RTT requests are
/// refused and WebTransport is not offered: both are decisions about an
/// application this listener knows nothing of.
pub struct Single<H> {
    listener: Arc<ListenerConfig>,
    settings: QuicListenerConfig,
    config: Arc<quinn::ServerConfig>,
    handler: Arc<H>,
}

impl<H: OtherHandler> Single<H> {
    /// `listener` must be a QUIC listener; `tls` must offer the `h3` ALPN and
    /// a cipher suite QUIC can start with.
    pub fn new(
        listener: Arc<ListenerConfig>,
        tls: Arc<rustls::ServerConfig>,
        handler: H,
    ) -> Result<Self> {
        let ListenerSettings::Quic(settings) = &listener.settings else {
            return Err(Error::msg(format!(
                "listener '{}': not a QUIC listener",
                listener.id
            )));
        };
        if !tls.alpn_protocols.iter().any(|alpn| alpn == b"h3") {
            return Err(Error::msg(format!(
                "listener '{}': TLS configuration does not offer h3",
                listener.id
            )));
        }
        let crypto = QuicServerConfig::try_from(tls).map_err(|error| {
            Error::msg(format!("listener '{}': {error}", listener.id))
        })?;
        let config =
            super::config::server_config(Arc::new(crypto), &settings.quic)?;
        let settings = settings.clone();
        Ok(Self {
            listener,
            settings,
            config: Arc::new(config),
            handler: Arc::new(handler),
        })
    }
}

impl<H: OtherHandler> Quic for Single<H> {
    type Host = SingleHost<H>;

    fn listener(&self) -> &ListenerConfig {
        &self.listener
    }
    fn settings(&self) -> &QuicListenerConfig {
        &self.settings
    }
    fn endpoint_config(&self) -> quinn::ServerConfig {
        self.config.as_ref().clone()
    }
    fn handshake_config(&self) -> Arc<quinn::ServerConfig> {
        self.config.clone()
    }
    fn select(
        self: &Arc<Self>,
        _server_name: Option<&str>,
    ) -> std::result::Result<Self::Host, Refusal> {
        Ok(SingleHost(self.handler.clone()))
    }
}

/// The owner of every connection of a [`Single`] listener.
pub struct SingleHost<H>(Arc<H>);

impl<H: OtherHandler> QuicHost for SingleHost<H> {
    type Requests = SingleRequests<H>;

    fn early_data(&self) -> EarlyDataPolicy {
        EarlyDataPolicy::DenyAll
    }
    fn web_transport(&self) -> bool {
        false
    }
    fn retired(&self) -> impl Future<Output = ()> + Send + 'static {
        std::future::pending()
    }
    fn bind(
        self,
        _facts: ConnCtx,
    ) -> std::result::Result<Self::Requests, Refusal> {
        Ok(SingleRequests(self.0))
    }
}

/// One connection of a [`Single`] listener.
pub struct SingleRequests<H>(Arc<H>);

impl<H: OtherHandler> QuicRequests for SingleRequests<H> {
    type Session = NoSession;

    fn admit(
        &mut self,
        _head: &Request<()>,
    ) -> std::result::Result<(), StatusCode> {
        Ok(())
    }

    fn call(
        &self,
        request: Request<RouteBody>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + 'static {
        let answering = self.0.call(request);
        async move {
            match answering.await {
                Ok(response) => response,
                Err(never) => match never {},
            }
        }
    }

    fn connect(
        &self,
        _head: Request<()>,
        _scope: SessionScope,
    ) -> std::result::Result<
        impl Future<Output = std::result::Result<Authorized<NoSession>, StatusCode>>
        + Send
        + 'static,
        StatusCode,
    > {
        Err::<std::future::Pending<_>, _>(StatusCode::NOT_FOUND)
    }
}

/// A [`Single`] listener offers no sessions; this is never constructed.
pub enum NoSession {}

impl QuicSession for NoSession {
    fn name(&self) -> &'static str {
        match *self {}
    }
    async fn run(
        self,
        _session: NativeSession,
        _incoming: mpsc::Receiver<BiStream>,
    ) {
        match self {}
    }
}

#[cfg(test)]
#[path = "../../tests/quic/single.rs"]
mod tests;
