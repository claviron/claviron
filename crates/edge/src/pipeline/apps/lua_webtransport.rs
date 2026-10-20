//! Bind configured Lua socket endpoints to the existing native H3 registry.
use std::sync::Arc;

use async_trait::async_trait;
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

pub(super) fn append_routes(
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
    for route in config
        .lua()
        .iter()
        .chain(config.routes.iter().flat_map(|(_, route)| route.lua()))
    {
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
) -> Option<&'a HostConfig> {
    let selected = host.routes.find(path).map(|route| &**route).unwrap_or(host);
    selected
        .lua()
        .iter()
        .any(|route| {
            route.application == application
                && route.websockets().iter().any(|socket| {
                    super::mounted_websocket_path(
                        route.path.as_ref(),
                        &socket.path,
                    )
                    .as_ref()
                        == path
                })
        })
        .then_some(selected)
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
        let selected =
            configured_route(&ctx.host_config, &ctx.path, &self.application)
                .ok_or_else(|| {
                    WebTransportReject::new(StatusCode::NOT_FOUND)
                })?;
        // Native CONNECT does not enter the ordinary CORS exchange.
        if let Some(origin) = ctx.headers.get(http::header::ORIGIN) {
            let allowed = origin.to_str().is_ok_and(|origin| {
                if let Some(cors) = selected.cors.as_ref() {
                    cors.enabled() && cors.allows_origin(origin)
                } else {
                    let Ok(origin) = origin.parse::<http::Uri>() else {
                        return false;
                    };
                    origin.scheme_str() == Some("https")
                        && origin.host() == ctx.uri.host()
                        && origin.port_u16().unwrap_or(443)
                            == ctx.uri.port_u16().unwrap_or(443)
                }
            });
            if !allowed {
                return Err(WebTransportReject::forbidden());
            }
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
                () = ctx.wait_graceful_shutdown() => {},
                () = state.on_shutdown() => {},
            }
        };
        tokio::pin!(shutdown);
        let stopping = application.execution_stopping();
        let opening = async {
            let Some(resolver) = session
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
                    if id == session.session_id() =>
                {
                    Ok(Some(stream))
                }
                _ => Err(Error::msg(
                    "Lua requires its own WebTransport bidirectional stream",
                )),
            }
        };
        let stream = tokio::select! {
            biased;
            () = &mut shutdown => return Ok(()),
            () = stopping.cancelled() => return Ok(()),
            stream = opening => stream?,
        };
        if let Some(stream) = stream {
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
            claviron_lua::webtransport::serve(
                stream,
                ctx.uri.to_string(),
                headers,
                application,
                &mut shutdown,
            )
            .await;
        }
        drop(session);
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/lua_webtransport.rs"]
mod tests;
