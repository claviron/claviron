//! Browser-origin admission shared by Lua H1/H2 WebSockets and H3 WebTransport.
//!
//! HTTP CORS cannot widen this same-origin boundary. This is not client
//! authentication: non-browser clients can supply their own Origin header.

#[cfg(feature = "websocket")]
use crate::pipeline::H1UpgradeHandler;
use http::uri::{Authority, Scheme};
use http::{HeaderMap, Uri, header};

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

/// Edge-owned admission around the engine's existing H1 transport adapter.
#[cfg(feature = "websocket")]
pub(super) struct WebSocketUpgrade {
    pub(super) inner: claviron_lua::websocket::LuaWebSocketUpgradeHandler,
}

#[cfg(feature = "websocket")]
#[async_trait::async_trait]
impl H1UpgradeHandler for WebSocketUpgrade {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn is_upgrade(
        &self,
        request: &http::Request<axum::body::Body>,
    ) -> bool {
        self.inner.is_upgrade(request)
    }

    async fn accept(
        &self,
        state: &claviron_runtime::AppState,
        ctx: &crate::pipeline::H1UpgradeContext,
    ) -> crate::pipeline::H1UpgradeResult {
        if !same_origin(&ctx.headers, &ctx.uri, &ctx.conn.scheme()) {
            return Err(crate::pipeline::H1UpgradeReject::new(
                http::StatusCode::FORBIDDEN,
                None,
            ));
        }
        self.inner.accept(state, ctx).await
    }

    async fn handle_upgraded(
        &self,
        state: claviron_runtime::AppState,
        ctx: crate::pipeline::H1UpgradeContext,
        accepted: Option<crate::pipeline::H1UpgradeState>,
        upgraded: hyper::upgrade::Upgraded,
    ) -> claviron_runtime::registry::Result<()> {
        self.inner.handle_upgraded(state, ctx, accepted, upgraded).await
    }
}

#[cfg(test)]
#[path = "../../../tests/pipeline/lua_socket.rs"]
mod tests;
