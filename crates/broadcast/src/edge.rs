//! Native entrances to the same Broadcast session, prepared with the host.
mod rest;

use std::sync::Arc;

use crate::stream::FastWebSocketStream;
use crate::{BroadcastService, BroadcastSession};
use async_trait::async_trait;
use claviron_edge::Host;
use claviron_http::session::SessionSpawner;
use claviron_http::upgrade::H1UpgradeRoute;
use claviron_http::{RouteBody, RouteResponse, empty_body, err_status};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use http::{Request, StatusCode};

use claviron_edge::HostConfig;
use claviron_edge::pipeline::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectHandler,
    H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
};
#[cfg(feature = "webtransport")]
use claviron_edge::pipeline::{
    H3WebTransportSession, WebTransportAccept, WebTransportHandler,
    WebTransportRegistry, WebTransportReject, WebTransportResult,
};

fn append_routes(
    state: &AppState,
    canonical: &Host,
    host: &HostConfig,
    routes: &mut Vec<H1UpgradeRoute>,
) -> Result<()> {
    if !is_fleet(host) && !host.routes.iter().any(|(_, route)| is_fleet(route))
    {
        return Ok(());
    }
    let service = state
        .registry_ref()
        .resolve::<BroadcastService>()
        .ok_or_else(|| {
            Error::msg(
                "fleet Pusher target requires the registered BroadcastService",
            )
        })?;
    let h2 = state.registry_ref().resolve::<H2ConnectRegistry>().ok_or_else(
        || Error::msg("broadcast requires the H2 CONNECT registry"),
    )?;
    #[cfg(feature = "webtransport")]
    let wt =
        state.registry_ref().resolve::<WebTransportRegistry>().ok_or_else(
            || Error::msg("broadcast requires the WebTransport registry"),
        )?;
    for (name, application) in service.applications().applications() {
        let path = Arc::<str>::from(format!("/app/{}", application.key));
        if !is_fleet_route(host, &path) {
            continue;
        }
        // What the binding was prepared from, for its admission to compare.
        let source = directive(selected(host, &path)).cloned().map(Arc::new);
        let Some(source) = source else {
            continue;
        };
        routes.push(H1UpgradeRoute::handler(
            path.clone(),
            service.upgrade_handler(name.as_str()),
        )?);
        let handler = Arc::new(BroadcastConnectHandler {
            service: service.clone(),
            application: Arc::from(name.as_str()),
            path: path.clone(),
            source,
        });
        h2.bind_handler(canonical.clone(), path.clone(), handler.clone())?;
        #[cfg(feature = "webtransport")]
        wt.bind_handler(canonical.clone(), path, handler)?;
    }
    Ok(())
}

/// A scope's broadcast declaration as written. `pusher` is its other name;
/// the edge refuses a scope that declares both.
fn directive(scope: &HostConfig) -> Option<&serde_yaml::Value> {
    scope.directive("broadcast").or_else(|| scope.directive("pusher"))
}

/// The declaration decoded. The edge keeps it as written and knows nothing
/// of its shape; an absent or null declaration is none.
fn declared(
    source: Option<&serde_yaml::Value>
) -> std::result::Result<Option<claviron_pusher::PusherConfig>, serde_yaml::Error>
{
    match source {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(source) => serde_yaml::from_value(source.clone()).map(Some),
    }
}

/// The scope that owns `path`: its most specific route, or the host itself.
fn selected<'a>(
    host: &'a HostConfig,
    path: &str,
) -> &'a HostConfig {
    host.routes.find(path).map(|route| route.as_ref()).unwrap_or(host)
}

/// Whether the scope targets this fleet. A declaration that does not decode
/// selects nothing here; the backend's preparation of the same scope reports
/// it.
fn is_fleet(scope: &HostConfig) -> bool {
    declared(directive(scope)).is_ok_and(|config| {
        config.is_some_and(|config| {
            matches!(config.target, claviron_pusher::PusherTarget::Fleet)
        })
    })
}

fn is_fleet_route(
    host: &HostConfig,
    path: &str,
) -> bool {
    is_fleet(selected(host, path))
}

/// Whether the scope that owns `path` still declares exactly `source`, the
/// declaration a binding was prepared from.
fn still_declared(
    host: &HostConfig,
    path: &str,
    source: &serde_yaml::Value,
) -> bool {
    directive(selected(host, path)) == Some(source)
}

#[cfg(test)]
#[path = "../tests/edge.rs"]
mod tests;

struct BroadcastConnectHandler {
    service: Arc<BroadcastService>,
    application: Arc<str>,
    path: Arc<str>,
    /// The fleet declaration this binding was prepared from, as its scope
    /// wrote it. Admission compares against it and decodes nothing.
    source: Arc<serde_yaml::Value>,
}

impl BroadcastConnectHandler {
    fn admit(
        &self,
        ctx: &AuthContext,
    ) -> std::result::Result<BroadcastSession, StatusCode> {
        // A removed fleet binding must not be revived by a retained registry
        // entry: the scope that owns the path must still declare what this
        // binding was prepared from. An edited declaration has been prepared
        // again by then, and its binding replaced this one.
        // Public/private/presence admission remains session-owned.
        if ctx.path != self.path
            || !still_declared(&ctx.host_config, &ctx.path, &self.source)
        {
            return Err(StatusCode::NOT_FOUND);
        }
        let route = ctx.host_config.routes.find(&ctx.path);
        let cors = route
            .map(|route| &**route)
            .unwrap_or(&ctx.host_config)
            .cors
            .as_ref();
        if let Some(origin) = ctx.headers.get(http::header::ORIGIN)
            && let Some(cors) = cors
            && !origin.to_str().is_ok_and(|origin| {
                cors.enabled() && cors.allows_origin(origin)
            })
        {
            return Err(StatusCode::FORBIDDEN);
        }
        self.service
            .accept_session(self.application.clone())
            .map_err(|refusal| refusal.status())
    }
}

#[async_trait]
impl H2ConnectHandler for BroadcastConnectHandler {
    fn name(&self) -> &'static str {
        "broadcast_h2"
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &AuthContext,
    ) -> H2ConnectResult {
        match ctx
            .headers
            .get("sec-websocket-version")
            .and_then(|value| value.to_str().ok())
        {
            Some("13") => {}
            None => {
                return Err(H2ConnectReject::bad_request()
                    .with_reason("missing_websocket_version"));
            }
            Some(_) => {
                return Err(H2ConnectReject::bad_request()
                    .with_reason("unsupported_websocket_version"));
            }
        }
        let mut session = self.admit(ctx).map_err(|status| {
            H2ConnectReject::new(status)
                .with_reason("broadcast_session_admission")
        })?;
        session
            .authenticate_headers(&ctx.host.to_string(), &ctx.headers)
            .await
            .map_err(|status| {
                H2ConnectReject::new(status)
                    .with_reason("broadcast_session_authentication")
            })?;
        Ok(H2ConnectAccept::authenticated(session))
    }

    async fn handle(
        &self,
        _state: AppState,
        _ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        req: Request<RouteBody>,
    ) -> Result<RouteResponse> {
        if !req.extensions().get::<hyper::ext::Protocol>().is_some_and(
            |protocol| protocol.as_str().eq_ignore_ascii_case("websocket"),
        ) {
            return Ok(err_status(StatusCode::BAD_REQUEST));
        }
        let session = accepted
            .ok_or_else(|| Error::msg("broadcast session missing"))?
            .take::<BroadcastSession>()?;
        let sessions =
            req.extensions().get::<SessionSpawner>().cloned().ok_or_else(
                || Error::msg("broadcast CONNECT carries no session owner"),
            )?;
        let owner = sessions.clone();
        let submitted = sessions.spawn(async move {
            let upgrade = hyper::upgrade::on(req);
            let upgraded = tokio::select! {
                biased;
                _ = owner.wait_graceful_shutdown() => return,
                _ = session.wait_shutdown() => return,
                result = upgrade => match result {
                    Ok(upgraded) => upgraded,
                    Err(error) => { tracing::debug!(%error, "broadcast H2 CONNECT failed"); return; }
                }
            };
            if let Err(error) = session.run_with_shutdown(
                FastWebSocketStream::new(upgraded), owner.wait_graceful_shutdown(),
            ).await {
                tracing::debug!(%error, "broadcast H2 session stopped");
            }
        });
        if submitted.is_err() {
            return Ok(err_status(StatusCode::SERVICE_UNAVAILABLE));
        }
        Ok(http::Response::builder()
            .status(StatusCode::OK)
            .body(empty_body())
            .expect("static response"))
    }
}

#[cfg(feature = "webtransport")]
#[async_trait]
impl WebTransportHandler for BroadcastConnectHandler {
    fn name(&self) -> &'static str {
        "broadcast_webtransport"
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
        self.admit(ctx)
            .map(WebTransportAccept::authenticated)
            .map_err(WebTransportReject::new)
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        transport: H3WebTransportSession,
    ) -> Result<()> {
        let mut session = accepted
            .ok_or_else(|| Error::msg("broadcast session missing"))?
            .take::<BroadcastSession>()?;
        ctx.admission.as_ref().ok_or_else(|| {
            Error::msg("broadcast WebTransport admission missing")
        })?;
        let shutdown = async {
            tokio::select! { _ = ctx.wait_graceful_shutdown() => {}, _ = state.on_shutdown() => {} }
        };
        tokio::pin!(shutdown);
        let stream = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = session.wait_shutdown() => return Ok(()),
            stream = transport.accept_bi() => stream,
        };
        let Some(stream) = stream else { return Ok(()) };
        let mut stream = crate::stream::WebTransportMessageStream::new(stream);
        if !session
            .authenticate_stream(
                &ctx.host.to_string(),
                &mut stream,
                &mut shutdown,
            )
            .await?
        {
            return Ok(());
        }
        let result = session
            .run_with_shutdown(stream, &mut shutdown)
            .await
            .map_err(|error| Error::recoverable(error.to_string()));
        drop(transport);
        result
    }
}

/// Register host integration only when this application's Edge capability is selected.
/// Runner publication and native sessions do not select this feature.
pub fn register(state: &AppState) -> Result<()> {
    let registry = state
        .registry_ref()
        .resolve::<claviron_edge::HostAppRegistry>()
        .ok_or_else(|| {
            Error::msg("Broadcast requires the Edge host registry")
        })?;
    // The declaration has two names; a scope uses one of them.
    for name in ["broadcast", "pusher"] {
        registry.register_backend_factory(Arc::new(Factory(name)))?;
    }
    claviron_edge::register_routable(
        state,
        Arc::new(Sessions { state: state.clone() }),
    )
}

struct Factory(&'static str);
impl claviron_edge::pipeline::backend::BackendFactory for Factory {
    fn name(&self) -> &'static str {
        self.0
    }
    fn placement(&self) -> claviron_edge::pipeline::backend::BackendPlacement {
        claviron_edge::pipeline::backend::BackendPlacement::BeforeFiles
    }
    fn prepare(
        &self,
        context: &claviron_edge::pipeline::backend::BackendContext<'_>,
    ) -> Result<Vec<Arc<dyn claviron_edge::pipeline::BackendHandler>>> {
        let config =
            declared(context.route.directive(self.0)).map_err(|error| {
                Error::msg(format!(
                    "backend '{}' at '{}': {error}",
                    self.0, context.prefix
                ))
            })?;
        Ok(config
            .and_then(|config| rest::prepare(context.state, &config))
            .into_iter()
            .collect())
    }
}

struct Sessions {
    state: AppState,
}
impl claviron_runtime::registry::Component<AppState> for Sessions {
    type Deps = ();
    fn name(&self) -> &str {
        "BroadcastEdgeSessions"
    }
}
impl claviron_edge::Routable for Sessions {
    fn match_host(
        &self,
        _: &Host,
        host: &HostConfig,
    ) -> bool {
        is_fleet(host) || host.routes.iter().any(|(_, route)| is_fleet(route))
    }
    fn h1_upgrades(
        &self,
        host: &Host,
        config: Arc<HostConfig>,
    ) -> Result<Vec<H1UpgradeRoute>> {
        let mut routes = Vec::new();
        append_routes(&self.state, host, &config, &mut routes)?;
        Ok(routes)
    }
}
