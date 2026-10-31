//! Bind embedded Lua applications to native H1/H2 WebSocket sessions.

use std::sync::Arc;

use claviron_http::config::routes::lua::LuaSocketOriginPolicy;
use claviron_runtime::{
    AppState,
    registry::{Error, Result},
};
use claviron_types::Host;

use super::{configured_route, mounted_websocket_path, origin::allows_origin};
use crate::HostConfig;
use crate::pipeline::{H1UpgradeHandler, H1UpgradeRoute};

pub(crate) fn append_routes(
    state: &AppState,
    canonical: &Host,
    host: &HostConfig,
    routes: &mut Vec<H1UpgradeRoute>,
) -> Result<()> {
    let Some(service) =
        state.registry_ref().resolve::<claviron_lua::serve::LuaApplications>()
    else {
        return Ok(());
    };
    let h2_connect = state.registry_ref().resolve::<crate::H2ConnectRegistry>();

    for application in host.lua_routes() {
        for websocket in application.websockets() {
            let path = mounted_websocket_path(
                application.path.as_ref(),
                &websocket.path,
            );
            let handler = Arc::new(WebSocketUpgrade {
                origin: application.origin,
                service: service.clone(),
                application: Arc::from(application.application.as_str()),
            });
            routes.push(H1UpgradeRoute::handler(path.clone(), handler)?);
            if let Some(registry) = h2_connect.as_ref() {
                registry.bind_handler(
                    canonical.clone(),
                    path,
                    Arc::new(LuaWebSocketConnectHandler {
                        service: service.clone(),
                        application: Arc::from(
                            application.application.as_str(),
                        ),
                    }),
                )?;
            }
        }
    }
    Ok(())
}

/// A Lua application's WebSocket over HTTP/2 extended CONNECT (RFC 8441).
struct LuaWebSocketConnectHandler {
    service: Arc<claviron_lua::serve::LuaApplications>,
    application: Arc<str>,
}

#[async_trait::async_trait]
impl crate::H2ConnectHandler for LuaWebSocketConnectHandler {
    fn name(&self) -> &'static str {
        "lua_websocket_h2"
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &crate::AuthContext,
    ) -> crate::H2ConnectResult {
        let route = configured_route(&ctx.host_config, &ctx.path, &self.application)
            .ok_or_else(|| crate::H2ConnectReject::new(http::StatusCode::NOT_FOUND))?;
        if !allows_origin(
            route.origin,
            &ctx.headers,
            &ctx.uri,
            &ctx.conn.scheme(),
        ) {
            return Err(crate::H2ConnectReject::forbidden());
        }
        // RFC 8441 §5: 13 is the only version defined for this handshake.
        let version = ctx
            .headers
            .get("sec-websocket-version")
            .and_then(|value| value.to_str().ok());
        if version != Some("13") {
            return Err(crate::H2ConnectReject::bad_request());
        }
        let application =
            claviron_lua::websocket::prepare(&self.service, &self.application)
                .map_err(|_| {
                    crate::H2ConnectReject::new(
                        http::StatusCode::SERVICE_UNAVAILABLE,
                    )
                })?;
        Ok(crate::H2ConnectAccept::authenticated(application))
    }

    async fn handle(
        &self,
        _state: AppState,
        ctx: crate::AuthContext,
        authenticated: Option<crate::AnyAuthenticated>,
        req: http::Request<claviron_http::RouteBody>,
    ) -> Result<claviron_http::RouteResponse> {
        let is_websocket =
            req.extensions().get::<hyper::ext::Protocol>().is_some_and(
                |protocol| protocol.as_str().eq_ignore_ascii_case("websocket"),
            );
        if !is_websocket {
            return Ok(claviron_http::err_status(
                http::StatusCode::BAD_REQUEST,
            ));
        }
        let application = authenticated
            .ok_or_else(|| Error::msg("Lua WebSocket application missing"))?
            .take::<Arc<claviron_lua::app::LuaApplication>>()?;
        let sessions = req
            .extensions()
            .get::<claviron_http::session::SessionSpawner>()
            .cloned()
            .ok_or_else(|| {
                Error::msg("H2 CONNECT request carries no session spawner")
            })?;
        let authority = req
            .uri()
            .authority()
            .map(|authority| authority.to_string())
            .unwrap_or_else(|| ctx.host.to_string());
        let url = format!(
            "wss://{authority}{}",
            req.uri().path_and_query().map_or("/", |path| path.as_str())
        );
        let headers = claviron_lua::websocket::header_pairs(&ctx.headers);
        let submitted = sessions.spawn(async move {
            match hyper::upgrade::on(req).await {
                Ok(upgraded) => {
                    claviron_lua::websocket::serve_websocket(
                        upgraded,
                        url,
                        headers,
                        application,
                        "HTTP/2.0",
                    )
                    .await;
                }
                Err(error) => {
                    tracing::error!(%error, "H2 CONNECT upgrade failed")
                }
            }
        });
        if let Err(error) = submitted {
            tracing::debug!(%error, "H2 CONNECT session hand-off rejected");
            return Ok(claviron_http::err_status(
                http::StatusCode::SERVICE_UNAVAILABLE,
            ));
        }
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(claviron_http::empty_body())
            .expect("static response"))
    }
}

/// Edge-owned HTTP/1 handshake and admission for the native Lua socket engine.
struct WebSocketUpgrade {
    service: std::sync::Arc<claviron_lua::serve::LuaApplications>,
    application: std::sync::Arc<str>,
    origin: LuaSocketOriginPolicy,
}

#[async_trait::async_trait]
impl H1UpgradeHandler for WebSocketUpgrade {
    fn name(&self) -> &'static str {
        "lua_websocket"
    }

    fn is_upgrade(
        &self,
        request: &http::Request<claviron_http::RouteBody>,
    ) -> bool {
        claviron_http::upgrade::is_h1_header_upgrade(request, "websocket")
    }

    async fn accept(
        &self,
        _state: &claviron_runtime::AppState,
        ctx: &crate::pipeline::H1UpgradeContext,
    ) -> crate::pipeline::H1UpgradeResult {
        if !allows_origin(
            self.origin,
            &ctx.headers,
            &ctx.uri,
            &ctx.conn.scheme(),
        ) {
            return Err(crate::pipeline::H1UpgradeReject::new(
                http::StatusCode::FORBIDDEN,
                None,
            ));
        }
        let headers =
            claviron_http::upgrade::websocket_response_headers(ctx, None)
                .map_err(|error| {
                    crate::pipeline::H1UpgradeReject::bad_request(
                        error.to_string(),
                    )
                })?;
        let application =
            claviron_lua::websocket::prepare(&self.service, &self.application)
                .map_err(|error| {
                    crate::pipeline::H1UpgradeReject::service_unavailable(
                        error.to_string(),
                    )
                })?;
        Ok(crate::pipeline::H1UpgradeAccept::with_state(headers, application))
    }

    async fn handle_upgraded(
        &self,
        _state: claviron_runtime::AppState,
        ctx: crate::pipeline::H1UpgradeContext,
        accepted: Option<crate::pipeline::H1UpgradeState>,
        upgraded: hyper::upgrade::Upgraded,
    ) -> claviron_runtime::registry::Result<()> {
        let application = accepted
            .ok_or_else(|| {
                claviron_runtime::registry::Error::msg(
                    "Lua WebSocket application missing",
                )
            })?
            .take::<std::sync::Arc<claviron_lua::app::LuaApplication>>()?;
        let host = ctx
            .headers
            .get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| ctx.host.to_string());
        claviron_lua::websocket::serve_websocket(
            upgraded,
            format!("wss://{host}{}", ctx.uri),
            claviron_lua::websocket::header_pairs(&ctx.headers),
            application,
            "HTTP/1.1",
        )
        .await;
        Ok(())
    }
}
