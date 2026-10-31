//! Browser-origin admission shared by Lua H1/H2 WebSockets and H3 WebTransport.
//!
//! HTTP CORS cannot widen an opted-in same-origin boundary. This is not client
//! authentication: non-browser clients can supply their own Origin header.

#[cfg(feature = "websocket")]
use crate::pipeline::H1UpgradeHandler;
use claviron_http::config::routes::lua::LuaSocketOriginPolicy;
use http::uri::{Authority, Scheme};
use http::{HeaderMap, Uri, header};

pub(super) fn allows_origin(
    policy: LuaSocketOriginPolicy,
    headers: &HeaderMap,
    uri: &Uri,
    scheme: &Scheme,
) -> bool {
    match policy {
        LuaSocketOriginPolicy::Any => true,
        LuaSocketOriginPolicy::SameOrigin => same_origin(headers, uri, scheme),
    }
}

/// Require one serialized HTTP(S) Origin matching the admitted request's
/// authority and accepted transport scheme. Canonical vhost keys, listener
/// addresses and forwarding headers do not identify the browser's origin.
pub(super) fn same_origin(
    headers: &HeaderMap,
    uri: &Uri,
    scheme: &Scheme,
) -> bool {
    let default_port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => return false,
    };
    let mut origins = headers.get_all(header::ORIGIN).iter();
    let Some(origin) = origins.next().and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if origins.next().is_some() {
        return false;
    }
    let Some(authority) = origin
        .strip_prefix(scheme.as_str())
        .and_then(|origin| origin.strip_prefix("://"))
        .and_then(parse_authority)
    else {
        return false;
    };
    // HTTP/1 absolute targets retain precedence over Host; native H2/H3
    // authority has already passed the common host/authority guard.
    let target = match uri.authority() {
        Some(authority) => parse_authority(authority.as_str()),
        None => {
            let mut hosts = headers.get_all(header::HOST).iter();
            let host = hosts.next().and_then(|value| value.to_str().ok());
            if hosts.next().is_some() {
                return false;
            }
            host.and_then(parse_authority)
        }
    };
    target.is_some_and(|target| {
        same_host(authority.host(), target.host())
            && authority.port_u16().unwrap_or(default_port)
                == target.port_u16().unwrap_or(default_port)
    })
}

fn parse_authority(raw: &str) -> Option<Authority> {
    if raw.is_empty()
        || raw
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        || raw.contains(['@', '/', '\\', '?', '#', ','])
    {
        return None;
    }
    let authority = raw.parse::<Authority>().ok()?;
    let host = authority.host();
    if host.is_empty() || host.contains('*') {
        return None;
    }
    let suffix = raw.strip_prefix(host)?;
    if !suffix.is_empty() {
        let port = suffix.strip_prefix(':')?;
        if port.is_empty()
            || !port.bytes().all(|byte| byte.is_ascii_digit())
            || authority.port_u16().is_none()
        {
            return None;
        }
    }
    Some(authority)
}

fn same_host(
    left: &str,
    right: &str,
) -> bool {
    if left.eq_ignore_ascii_case(right) {
        return true;
    }
    let ipv6 = |host: &str| {
        host.strip_prefix('[')?
            .strip_suffix(']')?
            .parse::<std::net::Ipv6Addr>()
            .ok()
    };
    match (ipv6(left), ipv6(right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// Edge-owned HTTP/1 handshake and admission for the native Lua socket engine.
#[cfg(feature = "websocket")]
pub(super) struct WebSocketUpgrade {
    pub(super) service: std::sync::Arc<claviron_lua::serve::LuaApplications>,
    pub(super) application: std::sync::Arc<str>,
    pub(super) origin: LuaSocketOriginPolicy,
}

#[cfg(feature = "websocket")]
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
        let headers = claviron_http::upgrade::websocket_response_headers(ctx, None)
            .map_err(|error| crate::pipeline::H1UpgradeReject::bad_request(error.to_string()))?;
        let application = claviron_lua::websocket::prepare(&self.service, &self.application)
            .map_err(|error| crate::pipeline::H1UpgradeReject::service_unavailable(error.to_string()))?;
        Ok(crate::pipeline::H1UpgradeAccept::with_state(headers, application))
    }

    async fn handle_upgraded(
        &self,
        _state: claviron_runtime::AppState,
        ctx: crate::pipeline::H1UpgradeContext,
        accepted: Option<crate::pipeline::H1UpgradeState>,
        upgraded: hyper::upgrade::Upgraded,
    ) -> claviron_runtime::registry::Result<()> {
        let application = accepted.ok_or_else(|| {
            claviron_runtime::registry::Error::msg("Lua WebSocket application missing")
        })?.take::<std::sync::Arc<claviron_lua::app::LuaApplication>>()?;
        let host = ctx.headers.get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| ctx.host.to_string());
        claviron_lua::websocket::serve_websocket(
            upgraded, format!("wss://{host}{}", ctx.uri),
            claviron_lua::websocket::header_pairs(&ctx.headers),
            application, "HTTP/1.1",
        ).await;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/lua_socket.rs"]
mod tests;
