//! The single-application adapter for transport-owned listener Runnables.

use std::future::Future;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use claviron_http::config::ListenerConfig;
use claviron_http::ext::{ConnCtx, RealIpExt, ReceivedAt, RequestArrival};
use claviron_http::trust::Trust;
use claviron_http::{RouteBody, RouteResponse};
use claviron_runtime::{
    AppState, Component, RunContext, RunnableInstance, registry,
};
use claviron_transport::listener::{Adapter, Listener};
use claviron_transport::owner::Owner as TransportOwner;
use http::Request;

use crate::routing::{AppRequest, Application, RequestBody};

/// Cold declarations. Transport owns every named serving generation.
pub struct SocketIngress {
    listeners: Vec<Arc<Listener<ApplicationListener>>>,
}

impl SocketIngress {
    pub fn new(configs: Vec<Arc<ListenerConfig>>) -> Self {
        Self {
            listeners: configs
                .into_iter()
                .map(|config| {
                    Arc::new(Listener::new(ApplicationListener {
                        config,
                        #[cfg(feature = "tls")]
                        tls: None,
                    }))
                })
                .collect(),
        }
    }

    #[cfg(feature = "tls")]
    pub fn tls(
        self,
        tls: Option<crate::Tls>,
    ) -> Self {
        Self {
            listeners: self
                .listeners
                .iter()
                .map(|listener| {
                    Arc::new(Listener::new(ApplicationListener {
                        config: listener.config.clone(),
                        tls: tls.clone(),
                    }))
                })
                .collect(),
        }
    }
}

impl Component<AppState> for SocketIngress {
    type Deps = (Arc<Application>, Option<Arc<Trust>>);

    fn name(&self) -> &str {
        "SocketIngress"
    }

    fn validate(
        &self,
        state: &AppState,
    ) -> registry::Result<()> {
        for listener in &self.listeners {
            #[cfg(unix)]
            claviron_transport::unix::prepare(state, &listener.config)?;
        }
        Ok(())
    }

    fn runnables(self: Arc<Self>) -> Vec<RunnableInstance<AppState>> {
        self.listeners.iter().map(Listener::instance).collect()
    }
}

struct ApplicationListener {
    config: Arc<ListenerConfig>,
    #[cfg(feature = "tls")]
    tls: Option<crate::Tls>,
}

#[async_trait]
impl Adapter for ApplicationListener {
    type Deps = (Arc<Application>, Option<Arc<Trust>>);

    fn config(&self) -> Arc<ListenerConfig> {
        self.config.clone()
    }

    async fn serve(
        self: Arc<Self>,
        (application, trust): Self::Deps,
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let owner =
            Arc::new(Front { application, trust: trust.unwrap_or_default() });
        claviron_transport::single::serve(
            self.config.clone(),
            owner,
            #[cfg(feature = "tls")]
            self.tls.as_ref(),
            &state,
            &context,
        )
        .await
    }
}

struct Front {
    application: Arc<Application>,
    trust: Arc<Trust>,
}

impl TransportOwner for Front {
    fn call(
        &self,
        request: Request<RouteBody>,
        connection: Option<&Arc<ConnCtx>>,
        _arrival: RequestArrival,
    ) -> impl Future<Output = RouteResponse> + Send + use<> {
        self.application.call(prepare(request, connection, &self.trust))
    }
}

fn prepare(
    request: Request<RouteBody>,
    connection: Option<&Arc<ConnCtx>>,
    trust: &Trust,
) -> AppRequest {
    let peer = connection
        .filter(|connection| {
            connection.transport
                != claviron_http::listener::ListenerTransport::Unix
        })
        .map(|connection| connection.remote);
    let resolved = trust.resolve(peer, request.headers());
    let (mut parts, body) = request.into_parts();
    if let Some(host) = resolved.host.as_deref()
        && let Ok(value) = http::HeaderValue::from_str(host)
    {
        parts.headers.insert(http::header::HOST, value);
    }
    let mut request = Request::from_parts(parts, RequestBody::new(body));
    let extensions = request.extensions_mut();
    extensions.insert(ReceivedAt(SystemTime::now()));
    if let Some(client) = resolved.client {
        extensions.insert(RealIpExt(client.ip()));
    }
    if let Some(forwarded) = resolved.forwarded(peer) {
        extensions.insert(forwarded);
    }
    if let Some(connection) = connection {
        extensions.insert(connection.clone());
    }
    request
}

#[cfg(all(test, unix))]
#[path = "../tests/socket.rs"]
mod tests;
