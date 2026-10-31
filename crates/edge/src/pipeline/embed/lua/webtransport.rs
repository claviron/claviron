//! Bind configured Lua socket endpoints to the existing native H3 registry.
use std::sync::Arc;

use async_trait::async_trait;
use claviron_lua::webtransport::SessionEnd;
use claviron_lua::{app::LuaApplication, serve::LuaApplications};
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;
use http::StatusCode;

use crate::HostConfig;
use crate::pipeline::{
    AnyAuthenticated, AuthContext, H3WebTransportSession, WebTransportAccept,
    WebTransportHandler, WebTransportRegistry, WebTransportReject,
    WebTransportResult,
};

pub(crate) fn append_routes(
    state: &AppState,
    host: &Host,
    config: &HostConfig,
) -> Result<()> {
    let Some(service) = state.registry_ref().resolve::<LuaApplications>()
    else {
        return Ok(());
    };
    let registry = state
        .registry_ref()
        .resolve::<WebTransportRegistry>()
        .ok_or_else(|| Error::msg("Lua requires the WebTransport registry"))?;
    for route in config.lua_routes() {
        for socket in route.websockets() {
            let path = super::mounted_websocket_path(
                route.path.as_ref(),
                &socket.path,
            );
            registry.bind_handler(
                host.clone(),
                path.clone(),
                Arc::new(Handler {
                    service: service.clone(),
                    application: Arc::from(route.application.as_str()),
                    path,
                }),
            )?;
        }
    }
    Ok(())
}

struct Handler {
    service: Arc<LuaApplications>,
    application: Arc<str>,
    path: Arc<str>,
}

fn configured_route<'a>(
    host: &'a HostConfig,
    path: &str,
    application: &str,
) -> Option<&'a claviron_http::config::routes::LuaRouteConfig> {
    let selected = host.routes.find(path).map(|route| &**route).unwrap_or(host);
    selected.lua().iter().find(|route| {
        route.application == application
            && route.websockets().iter().any(|socket| {
                super::mounted_websocket_path(route.path.as_ref(), &socket.path)
                    .as_ref()
                    == path
            })
    })
}

#[async_trait]
impl WebTransportHandler for Handler {
    fn name(&self) -> &'static str {
        "lua_webtransport"
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
        // Retained registry entries cannot revive removed or masked routes.
        if ctx.path != self.path {
            return Err(WebTransportReject::new(StatusCode::NOT_FOUND));
        }
        let route =
            configured_route(&ctx.host_config, &ctx.path, &self.application)
                .ok_or_else(|| {
                    WebTransportReject::new(StatusCode::NOT_FOUND)
                })?;
        // Native CONNECT and H1 upgrade share the same browser-origin boundary.
        if !super::origin::allows_origin(
            route.origin,
            &ctx.headers,
            &ctx.uri,
            &ctx.conn.scheme(),
        ) {
            return Err(WebTransportReject::forbidden());
        }
        let app = claviron_lua::webtransport::prepare(
            &self.service,
            &self.application,
        )
        .map_err(|_| {
            WebTransportReject::new(StatusCode::SERVICE_UNAVAILABLE)
        })?;
        Ok(WebTransportAccept::authenticated(app))
    }

    async fn handle(
        &self,
        state: AppState,
        ctx: AuthContext,
        accepted: Option<AnyAuthenticated>,
        session: H3WebTransportSession,
    ) -> Result<()> {
        let application = accepted
            .ok_or_else(|| Error::msg("Lua WebTransport application missing"))?
            .take::<Arc<LuaApplication>>()?;
        let shutdown = async {
            tokio::select! {
                () = ctx.wait_graceful_shutdown() => SessionEnd::ServerStopping,
                () = state.on_shutdown() => SessionEnd::ServerStopping,
                closed = session.closed() => match closed {
                    Ok(_) => SessionEnd::PeerClosed,
                    Err(error) => {
                        tracing::debug!(%error, "Lua WebTransport CONNECT ended");
                        SessionEnd::TransportLost
                    }
                },
            }
        };
        let headers = ctx
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        claviron_lua::webtransport::serve_session(
            || session.accept_bi(),
            session.incoming_capacity(),
            ctx.uri.to_string(),
            headers,
            application,
            shutdown,
        )
        .await;
        drop(session);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/embed/lua/webtransport.rs"]
mod tests;
