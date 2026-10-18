//! Native entrances to the same Broadcast session, prepared with the host.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_broadcast::stream::FastWebSocketStream;
use claviron_broadcast::{BroadcastService, BroadcastSession};
use claviron_http::session::SessionSpawner;
use claviron_http::upgrade::H1UpgradeRoute;
use claviron_http::{RouteBody, RouteResponse, empty_body, err_status};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use http::{Request, StatusCode};

use crate::config::HostConfig;
use crate::pipeline::{
    AnyAuthenticated, AuthContext, H2ConnectAccept, H2ConnectHandler,
    H2ConnectRegistry, H2ConnectReject, H2ConnectResult,
};
#[cfg(feature = "webtransport")]
use crate::pipeline::{
    H3WebTransportSession, WebTransportAccept, WebTransportHandler,
    WebTransportRegistry, WebTransportReject, WebTransportResult,
};

pub(super) fn append_routes(
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
        routes.push(H1UpgradeRoute::handler(
            path.clone(),
            service.upgrade_handler(name.as_str()),
        )?);
        let handler = Arc::new(BroadcastConnectHandler {
            service: service.clone(),
            application: Arc::from(name.as_str()),
            path: path.clone(),
        });
        h2.bind_handler(canonical.clone(), path.clone(), handler.clone())?;
        #[cfg(feature = "webtransport")]
        wt.bind_handler(canonical.clone(), path, handler)?;
    }
    Ok(())
}

fn is_fleet(host: &HostConfig) -> bool {
    host.broadcast_cfg().is_some_and(|config| {
        matches!(config.target, claviron_pusher::PusherTarget::Fleet)
    })
}

fn is_fleet_route(
    host: &HostConfig,
    path: &str,
) -> bool {
    is_fleet(host.routes.find(path).map(|route| route.as_ref()).unwrap_or(host))
}

#[cfg(test)]
#[path = "../../../tests/pipeline/broadcast.rs"]
mod tests;

struct BroadcastConnectHandler {
    service: Arc<BroadcastService>,
    application: Arc<str>,
    path: Arc<str>,
}

impl BroadcastConnectHandler {
    fn admit(
        &self,
        ctx: &AuthContext,
    ) -> std::result::Result<BroadcastSession, StatusCode> {
        // A removed fleet binding must not be revived by a retained registry
        // entry. Public/private/presence admission remains session-owned.
        if ctx.path != self.path || !is_fleet_route(&ctx.host_config, &ctx.path)
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
        let open = async {
            let Some(resolver) = transport
                .accept_bi()
                .await
                .map_err(|error| Error::msg(error.to_string()))?
            else {
                return Ok(None);
            };
            match resolver
                .resolve()
                .await
                .map_err(|error| Error::msg(error.to_string()))?
            {
                h3_webtransport::server::AcceptedBi::BidiStream(id, stream)
                    if id == transport.session_id() =>
                {
                    Ok(Some(stream))
                }
                _ => Err(Error::msg(
                    "broadcast requires its own WebTransport bidirectional stream",
                )),
            }
        };
        let stream = tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = session.wait_shutdown() => return Ok(()),
            stream = open => stream?,
        };
        let Some(stream) = stream else { return Ok(()) };
        let mut stream =
            claviron_broadcast::stream::WebTransportMessageStream::new(stream);
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
